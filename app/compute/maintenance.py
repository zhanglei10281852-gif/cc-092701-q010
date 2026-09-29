"""评分组件升级用的维护窗口：预告 → 排空 → 强制处理 → 恢复，支持撤销与重叠合并。"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Iterable

from app.compute.repository import ComputeRepository
from app.compute.service import digest
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction

SCOPE_DIMENSIONS = ("template_codes", "project_codes", "class_codes")
BLOCKER_LIMIT = 50


class MaintenanceWindowService:
    """按课程/模板/班级划定维护窗口，并在窗口截止时批量处置存量任务。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 窗口管理

    def create_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        drain_at = self._normalize_time(payload["drain_at"])
        deadline_at = self._normalize_time(payload["deadline_at"])
        if drain_at < now:
            raise ValidationError("排空开始时间不能早于当前时间")
        if deadline_at <= drain_at:
            raise ValidationError("截止时间必须晚于排空开始时间")
        selector = self._normalize_selector(payload["scope"])
        policy = payload["deadline_policy"]
        with transaction(immediate=True) as connection:
            if connection.execute("SELECT 1 FROM compute_maintenance_windows WHERE code=?", (payload["code"],)).fetchone():
                raise ConflictError("维护窗口编码已存在")
            overlaps = connection.execute(
                "SELECT * FROM compute_maintenance_windows WHERE status IN ('announced','draining') AND NOT (deadline_at<=? OR drain_at>=?) ORDER BY drain_at,deadline_at,id",
                (drain_at, deadline_at),
            ).fetchall()
            window_id = connection.execute(
                "INSERT INTO compute_maintenance_windows(code,reason,scope_json,scope_digest,deadline_policy,status,announced_at,drain_at,deadline_at,created_by,updated_at) VALUES(?,?,?,?,?, 'announced',?,?,?,?,?)",
                (payload["code"], payload["reason"], json.dumps({"selectors": [selector]}, ensure_ascii=False), digest(selector), policy, now, drain_at, deadline_at, actor, now),
            ).lastrowid
            self._add_event(connection, window_id, actor, "created", {"policy": policy, "overlaps": [row["code"] for row in overlaps]}, now)
            if overlaps:
                self._merge_overlaps(connection, window_id, overlaps, actor, now)
            window = self._require_window(window_id, connection)
            if window["status"] == "merged":
                return self._progress(connection, self._require_window(window["merged_into"], connection))
            if window["status"] == "announced" and now >= window["drain_at"]:
                connection.execute("UPDATE compute_maintenance_windows SET status='draining',updated_at=?,version=version+1 WHERE id=?", (now, window_id))
                self._add_event(connection, window_id, actor, "draining_started", {"reason": "overlap_merge_advanced_schedule"}, now)
            return self._progress(connection, self._require_window(window_id, connection))

    def list_windows(self, *, include_closed: bool = False, limit: int = 100) -> dict[str, Any]:
        clause = "" if include_closed else "WHERE status IN ('announced','draining','enforced')"
        rows = self.connection.execute(
            f"SELECT * FROM compute_maintenance_windows {clause} ORDER BY id DESC LIMIT ?",
            (max(1, min(limit, 500)),),
        ).fetchall()
        return {"items": [self._window_dict(row) for row in rows]}

    def get_window(self, window_id: int) -> dict[str, Any]:
        return self._window_dict(self._require_window(window_id))

    def progress(self, window_id: int) -> dict[str, Any]:
        with transaction(immediate=False) as connection:
            window = self._require_window(window_id, connection)
            return self._progress(connection, window)

    # ------------------------------------------------------------------ 状态推进

    def advance(self, window_id: int, actor: str, action: str | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        transitions: list[str] = []
        processed: dict[str, list[int]] = {"cancelled": [], "requeued": []}
        reason = ""
        with transaction(immediate=True) as connection:
            window = self._require_window(window_id, connection)
            status = window["status"]
            if status in ("recovered", "revoked", "merged"):
                # 终态重复推进是安全空操作，不产生任何重复干预。
                result = self._progress(connection, window)
                result["transitions"] = []
                result["noop"] = True
                result["reason"] = f"窗口已经处于 {status} 状态"
                result["processed"] = {"cancelled": {"ids": [], "total": 0}, "requeued": {"ids": [], "total": 0}}
                return result
            if action not in (None, "drain", "enforce", "recover"):
                raise ValidationError(f"不支持的推进动作：{action}")

            if action in (None, "drain"):
                if status == "announced" and now >= window["drain_at"]:
                    connection.execute("UPDATE compute_maintenance_windows SET status='draining',updated_at=?,version=version+1 WHERE id=?", (now, window_id))
                    transitions.append("draining")
                    self._add_event(connection, window_id, actor, "draining_started", {}, now)
                    status = "draining"
                elif action == "drain":
                    reason = "排空开始时间尚未到达" if status == "announced" else "窗口已经进入排空阶段"

            if action in (None, "enforce"):
                if status == "draining" and now >= window["deadline_at"]:
                    summary = self._enforce(connection, window_id, actor, now)
                    processed["cancelled"] = summary["cancelled"]
                    processed["requeued"] = summary["requeued"]
                    connection.execute(
                        "UPDATE compute_maintenance_windows SET status='enforced',enforced_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, window_id),
                    )
                    transitions.append("enforced")
                    self._add_event(connection, window_id, actor, "enforce", summary["counts"], now)
                    status = "enforced"
                elif action == "enforce" and not transitions:
                    if status == "announced":
                        reason = "窗口尚未进入排空阶段"
                    elif status == "draining":
                        reason = "窗口截止时间尚未到达"
                    else:
                        reason = "窗口已经完成强制处理"

            if action == "recover":
                if status == "enforced":
                    connection.execute(
                        "UPDATE compute_maintenance_windows SET status='recovered',recovered_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, window_id),
                    )
                    transitions.append("recovered")
                    self._add_event(connection, window_id, actor, "recovered", {}, now)
                else:
                    reason = "只有强制处理完成的窗口可以恢复"

            result = self._progress(connection, self._require_window(window_id, connection))
        result["transitions"] = transitions
        result["noop"] = not transitions
        if reason:
            result["reason"] = reason
        result["processed"] = {key: {"ids": values[:BLOCKER_LIMIT], "total": len(values)} for key, values in processed.items()}
        return result

    def revoke(self, window_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            window = self._require_window(window_id, connection)
            if window["status"] == "revoked":
                result = self._progress(connection, window)
                result["noop"] = True
                return result
            if window["status"] not in ("announced", "draining"):
                raise ConflictError("只有预告或排空阶段的窗口可以撤销，强制处理后请使用恢复")
            connection.execute(
                "UPDATE compute_maintenance_windows SET status='revoked',revoked_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, window_id),
            )
            self._add_event(connection, window_id, actor, "revoked", {"reason": reason}, now)
            result = self._progress(connection, self._require_window(window_id, connection))
            result["noop"] = False
            return result

    # ------------------------------------------------------------------ 截止处置

    def _enforce(self, connection: sqlite3.Connection, window_id: int, actor: str, now: str) -> dict[str, Any]:
        window = self._require_window(window_id, connection)
        selectors = self._selectors(window)
        policy = window["deadline_policy"]
        batch_key = f"maintenance-window:{window['code']}"
        repository = ComputeRepository(connection)
        rows = connection.execute(
            "SELECT t.*,tpl.code AS template_code FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status IN ('queued','running','cancel_requested') ORDER BY t.id"
        ).fetchall()
        cancelled: list[int] = []
        requeued: list[int] = []
        for task in rows:
            if not self._matches_any(selectors, str(task["template_code"]), task["project_code"], task["class_code"] or ""):
                continue
            status = task["status"]
            before = dict(task)
            if policy == "cancel" or status == "cancel_requested":
                marker = self._insert_marker(connection, window_id, task["id"], "cancel", actor, {}, now)
                if marker is None:
                    continue
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task["id"]),
                )
                cancelled.append(int(task["id"]))
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="maintenance_cancel", reason=f"维护窗口 {window['code']} 截止取消", before=before, after=after, batch_key=batch_key, now=now)
            elif status == "queued":
                # 排队任务保持原 priority/created_at，仅登记处置锚点；恢复后自动回到原排序。
                self._insert_marker(connection, window_id, task["id"], "requeue", actor, {"note": "queued_task_kept_position"}, now)
                requeued.append(int(task["id"]))
            else:
                marker = self._insert_marker(connection, window_id, task["id"], "requeue", actor, {"from": status}, now)
                if marker is None:
                    continue
                connection.execute(
                    "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',available_at=?,finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task["id"]),
                )
                requeued.append(int(task["id"]))
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="maintenance_requeue", reason=f"维护窗口 {window['code']} 强制释放并重新排队", before=before, after=after, batch_key=batch_key, now=now)
        return {
            "cancelled": cancelled,
            "requeued": requeued,
            "counts": {"cancelled": len(cancelled), "requeued": len(requeued), "policy": policy},
        }

    # ------------------------------------------------------------------ 进度视图

    def _progress(self, connection: sqlite3.Connection, window: sqlite3.Row) -> dict[str, Any]:
        selectors = self._selectors(window)
        rows = connection.execute(
            "SELECT t.*,tpl.code AS template_code FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status IN ('queued','running','cancel_requested') ORDER BY t.priority DESC,t.created_at ASC,t.id"
        ).fetchall()
        matched = [row for row in rows if self._matches_any(selectors, str(row["template_code"]), row["project_code"], row["class_code"] or "")]
        breakdown = {"queued": 0, "running": 0, "cancel_requested": 0}
        blockers: list[dict[str, Any]] = []
        for task in matched:
            breakdown[task["status"]] += 1
            if task["status"] in ("running", "cancel_requested") and len(blockers) < BLOCKER_LIMIT:
                blockers.append(self._blocker(task))
        marker_counts = {
            row["action"]: int(row["amount"])
            for row in connection.execute("SELECT action,COUNT(*) AS amount FROM compute_maintenance_task_actions WHERE window_id=? GROUP BY action", (window["id"],)).fetchall()
        }
        events = [
            {"actor": row["actor"], "event": row["event"], "detail": json.loads(row["detail_json"]), "created_at": row["created_at"]}
            for row in connection.execute("SELECT * FROM compute_maintenance_events WHERE window_id=? ORDER BY id DESC LIMIT 20", (window["id"],)).fetchall()
        ]
        now = to_storage(self.clock.now())
        next_action, next_action_at = self._next_action(window, now)
        result = self._window_dict(window)
        result.update(
            {
                "matched_open_tasks": breakdown,
                "matched_open_total": sum(breakdown.values()),
                "processed": {
                    "cancelled": marker_counts.get("cancel", 0),
                    "requeued": marker_counts.get("requeue", 0),
                },
                "blockers": {"total": len([t for t in matched if t["status"] in ("running", "cancel_requested")]), "items": blockers},
                "next_action": next_action,
                "next_action_at": next_action_at,
                "events": events,
            }
        )
        if window["merged_into"]:
            parent = self._require_window(window["merged_into"], connection)
            result["merged_into"] = {"id": parent["id"], "code": parent["code"], "status": parent["status"]}
        return result

    @staticmethod
    def _blocker(task: sqlite3.Row) -> dict[str, Any]:
        if task["status"] == "cancel_requested":
            reason = "工作者尚未响应取消请求"
        elif task["lease_expires_at"] and task["lease_owner"]:
            reason = f"工作者 {task['lease_owner']} 仍在执行，租约至 {task['lease_expires_at']}"
        else:
            reason = "任务处于运行中但缺少有效租约"
        return {
            "task_id": int(task["id"]),
            "template_code": task["template_code"],
            "project_code": task["project_code"],
            "class_code": task["class_code"] or "",
            "worker_id": task["lease_owner"],
            "lease_expires_at": task["lease_expires_at"] or None,
            "status": task["status"],
            "reason": reason,
        }

    @staticmethod
    def _next_action(window: sqlite3.Row, now: str) -> tuple[str | None, str | None]:
        status = window["status"]
        if status == "announced":
            return ("drain", None if now >= window["drain_at"] else window["drain_at"])
        if status == "draining":
            return ("enforce", None if now >= window["deadline_at"] else window["deadline_at"])
        if status == "enforced":
            return "recover", None
        return None, None

    # ------------------------------------------------------------------ 重叠合并

    def _merge_overlaps(self, connection: sqlite3.Connection, new_id: int, overlaps: list[sqlite3.Row], actor: str, now: str) -> None:
        new_window = self._require_window(new_id, connection)
        candidates = [new_window, *overlaps]
        survivor = min(candidates, key=lambda row: (row["drain_at"], row["deadline_at"], row["id"]))
        selectors = self._canonical_selectors(selector for row in candidates for selector in self._selectors(row))
        policy = "cancel" if any(row["deadline_policy"] == "cancel" for row in candidates) else "requeue"
        drain_at = min(row["drain_at"] for row in candidates)
        deadline_at = min(row["deadline_at"] for row in candidates)
        absorbed_codes = [row["code"] for row in candidates if row["id"] != survivor["id"]]
        connection.execute(
            "UPDATE compute_maintenance_windows SET scope_json=?,scope_digest=?,deadline_policy=?,drain_at=?,deadline_at=?,updated_at=?,version=version+1 WHERE id=?",
            (json.dumps({"selectors": selectors}, ensure_ascii=False), digest(selectors), policy, drain_at, deadline_at, now, survivor["id"]),
        )
        for row in candidates:
            if row["id"] == survivor["id"]:
                continue
            connection.execute(
                "UPDATE compute_maintenance_windows SET status='merged',merged_into=?,updated_at=?,version=version+1 WHERE id=?",
                (survivor["id"], now, row["id"]),
            )
            self._add_event(connection, row["id"], actor, "merged", {"into": survivor["code"]}, now)
        self._add_event(
            connection, survivor["id"], actor, "merged",
            {"absorbed": absorbed_codes, "policy": policy, "drain_at": drain_at, "deadline_at": deadline_at},
            now,
        )

    # ------------------------------------------------------------------ 辅助方法

    def _require_window(self, window_id: int, connection: sqlite3.Connection | None = None) -> sqlite3.Row:
        connection = connection or self.connection
        row = connection.execute("SELECT * FROM compute_maintenance_windows WHERE id=?", (window_id,)).fetchone()
        if row is None:
            raise NotFoundError("维护窗口不存在")
        return row

    @staticmethod
    def _add_event(connection: sqlite3.Connection, window_id: int, actor: str, event: str, detail: dict[str, Any], now: str) -> None:
        connection.execute(
            "INSERT INTO compute_maintenance_events(window_id,actor,event,detail_json,created_at) VALUES(?,?,?,?,?)",
            (window_id, actor, event, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
        )

    @staticmethod
    def _insert_marker(connection: sqlite3.Connection, window_id: int, task_id: int, action: str, actor: str, detail: dict[str, Any], now: str) -> bool:
        cursor = connection.execute(
            "INSERT OR IGNORE INTO compute_maintenance_task_actions(window_id,task_id,action,actor,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (window_id, task_id, action, actor, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
        )
        return cursor.rowcount == 1

    @staticmethod
    def _normalize_time(value: Any) -> str:
        if isinstance(value, datetime):
            return to_storage(value)
        parsed = datetime.fromisoformat(str(value))
        return to_storage(parsed)

    @staticmethod
    def _normalize_selector(scope: dict[str, Any] | None) -> dict[str, list[str]]:
        scope = scope or {}
        selector: dict[str, list[str]] = {}
        for dimension in SCOPE_DIMENSIONS:
            values = scope.get(dimension) or []
            values = [str(item).strip() for item in values if str(item).strip()]
            if len(values) > 50:
                raise ValidationError(f"{dimension} 最多包含 50 个取值")
            if values:
                selector[dimension] = sorted(set(values))
        if not selector:
            raise ValidationError("维护窗口至少需要按模板、课程或班级中的一个维度筛选")
        return selector

    @staticmethod
    def _canonical_selectors(selectors: Iterable[dict[str, Any]]) -> list[dict[str, list[str]]]:
        unique: dict[str, dict[str, list[str]]] = {}
        for selector in selectors:
            normalized = {dimension: sorted(set(selector[dimension])) for dimension in SCOPE_DIMENSIONS if selector.get(dimension)}
            unique.setdefault(digest(normalized), normalized)
        return list(unique.values())

    @staticmethod
    def _selectors(window: sqlite3.Row) -> list[dict[str, Any]]:
        return list(json.loads(window["scope_json"]).get("selectors", []))

    @staticmethod
    def _selector_matches(selector: dict[str, Any], template_code: str, project_code: str, class_code: str) -> bool:
        values = {"template_codes": template_code, "project_codes": project_code, "class_codes": class_code}
        for dimension, actual in values.items():
            members = selector.get(dimension)
            if members and actual not in members:
                return False
        return True

    def _matches_any(self, selectors: Iterable[dict[str, Any]], template_code: str, project_code: str, class_code: str) -> bool:
        return any(
            self._selector_matches(selector, template_code, project_code, class_code)
            for selector in selectors
        )

    def _window_dict(self, window: sqlite3.Row) -> dict[str, Any]:
        data = {
            "id": window["id"],
            "code": window["code"],
            "reason": window["reason"],
            "scope": json.loads(window["scope_json"]),
            "deadline_policy": window["deadline_policy"],
            "status": window["status"],
            "announced_at": window["announced_at"],
            "drain_at": window["drain_at"],
            "deadline_at": window["deadline_at"],
            "enforced_at": window["enforced_at"],
            "recovered_at": window["recovered_at"],
            "revoked_at": window["revoked_at"],
            "created_by": window["created_by"],
            "updated_at": window["updated_at"],
            "version": window["version"],
        }
        if window["merged_into"]:
            data["merged_into_window_id"] = window["merged_into"]
        return data
