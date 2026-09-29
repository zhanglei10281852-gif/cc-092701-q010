from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from app.compute.window_repository import (
    ACTIVE_STAGES,
    WindowRepository,
    claim_freeze_clause,
    normalize_scope,
    scopes_intersect,
)
from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction

NEXT_STAGE = {"announced": "draining", "draining": "enforcing", "enforcing": "recovered"}
STAGE_RANK = {"announced": 0, "draining": 1, "enforcing": 2, "recovered": 3, "revoked": 3}
ENFORCE_STATUSES = ("queued", "running", "cancel_requested")
BLOCKING_STATUSES = ("running", "cancel_requested")


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def apply_scheduled_transitions(connection, clock: Clock, actor: str = "scheduler") -> list[int]:
    """在已有事务内把所有到点的开放窗口向前推进（预告→排空→强制→恢复）。

    领取路径每次调用，保证即使运维没有手动调接口，排空限制也会按计划生效。
    """
    repository = WindowRepository(connection)
    now_value = clock.now()
    touched: list[int] = []
    for window in repository.open_windows():
        stage = window["stage"]
        if stage == "announced" and now_value >= from_storage(window["drain_at"]):
            _enter_stage(repository, window, "draining", actor, clock)
            window = repository.window_by_id(window["id"])
            stage = window["stage"]
            touched.append(window["id"])
        if stage == "draining" and now_value >= from_storage(window["enforce_at"]):
            _enter_stage(repository, window, "enforcing", actor, clock)
            window = repository.window_by_id(window["id"])
            stage = window["stage"]
            touched.append(window["id"])
        if stage == "enforcing" and window["recover_at"] and now_value >= from_storage(window["recover_at"]):
            _enter_stage(repository, window, "recovered", actor, clock)
            touched.append(window["id"])
    return touched


def _enter_stage(repository: WindowRepository, window, target: str, actor: str, clock: Clock) -> dict[str, Any]:
    now = to_storage(clock.now())
    timestamp_column = {
        "draining": "entered_draining_at",
        "enforcing": "entered_enforcing_at",
        "recovered": "entered_recovered_at",
    }.get(target)
    fields: dict[str, Any] = {}
    if timestamp_column:
        fields[timestamp_column] = now
    repository.update_stage(window["id"], target, now=now, **fields)
    detail: dict[str, Any] = {}
    if target == "enforcing":
        detail["processed"] = _process_at_deadline(repository, window, actor, now)
    repository.add_event(window_id=window["id"], event=f"stage_{target}", actor=actor, now=now, detail=detail)
    return detail


def _process_at_deadline(repository: WindowRepository, window, actor: str, now: str) -> dict[str, Any]:
    """截止时按策略处理存量任务；逐任务落处理记录（UNIQUE 约束），重复触发不重复干预。"""
    scope = repository.parse_scope(window)
    policy = window["deadline_policy"]
    cancelled: list[int] = []
    requeued: list[int] = []
    skipped: list[dict[str, Any]] = []
    for task in repository.matched_tasks(scope, ENFORCE_STATUSES):
        task_id = int(task["id"])
        # 先抢占处理名额：重复调用（含手动+定时同时命中）时第二次只会跳过。
        if not repository.record_action(
            window_id=window["id"], task_id=task_id, phase="enforce", action="pending",
            before={}, after={}, actor=actor, now=now,
        ):
            skipped.append({"task_id": task_id, "reason": "已在本窗口处理过，跳过"})
            continue
        before = dict(task)
        status_before = task["status"]
        if policy == "cancel" or status_before == "cancel_requested":
            # cancel 策略下运行/排队任务一律强制取消；
            # requeue 策略下，已处于 cancel_requested 的任务仍尊重既有的取消请求。
            repository.connection.execute(
                "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task_id),
            )
            action = "cancelled" if status_before == "queued" else "force_cancelled"
            cancelled.append(task_id)
        else:
            if status_before == "queued":
                action = "held_queued"
            else:
                repository.connection.execute(
                    "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',available_at=?,finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task_id),
                )
                action = "force_requeued"
                requeued.append(task_id)
        after = dict(repository.task_row(task_id))
        repository.connection.execute(
            "UPDATE compute_maintenance_task_actions SET action=?,before_json=?,after_json=? WHERE window_id=? AND task_id=? AND phase='enforce'",
            (action, _json(before), _json(after), window["id"], task_id),
        )
        repository.add_intervention_reflection(
            window, task_id, actor, f"window_{action}", f"维护窗口 {window['code']} 截止处理", before, after, now,
        )
    return {"policy": policy, "cancelled": cancelled, "requeued": requeued, "skipped": skipped}


def _overlaps(repository: WindowRepository, scope: dict[str, Any], other: dict[str, Any]) -> bool:
    """重叠 = 筛选维度直接相交，或在当前任务数据上命中同一批任务。"""
    return scopes_intersect(scope, other) or repository.scopes_share_tasks(scope, other)


class MaintenanceWindowService:
    """按课程/模板/班级圈选任务的维护窗口：预告、排空、强制处理、恢复与撤销。"""

    def __init__(self, connection=None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 创建
    def create_window(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        try:
            scope = normalize_scope(payload.get("scope") or {})
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        if not scope:
            raise ValidationError("维护窗口至少需要指定一个模板、课程或班级筛选条件")
        policy = payload.get("deadline_policy", "cancel")
        if policy not in {"cancel", "requeue"}:
            raise ValidationError("截止策略必须是 cancel 或 requeue")
        now_value = self.clock.now()
        now = to_storage(now_value)
        drain_at_value = payload.get("drain_at") or now_value
        enforce_delay = int(payload.get("enforce_after_seconds", 3600))
        if enforce_delay < 0:
            raise ValidationError("排空时长不能为负数")
        enforce_at_value = payload.get("enforce_at") or (
            drain_at_value + timedelta(seconds=enforce_delay)
        )
        if enforce_at_value <= drain_at_value:
            raise ValidationError("强制处理时刻必须晚于排空开始时刻")
        recover_at_value = payload.get("recover_at")
        if recover_at_value and recover_at_value <= enforce_at_value:
            raise ValidationError("恢复时刻必须晚于强制处理时刻")
        drain_at = to_storage(drain_at_value)
        enforce_at = to_storage(enforce_at_value)
        recover_at = to_storage(recover_at_value) if recover_at_value else None
        with transaction(immediate=True) as connection:
            repository = WindowRepository(connection)
            if repository.window_by_code(payload["code"]):
                raise ConflictError("维护窗口编码已存在")
            window = repository.create_window(
                code=payload["code"], name=payload["name"], scope=scope, deadline_policy=policy,
                drain_at=drain_at, enforce_at=enforce_at, recover_at=recover_at,
                created_by=actor, now=now,
            )
            overlaps = [
                row for row in repository.open_windows()
                if row["id"] != window["id"] and _overlaps(repository, scope, repository.parse_scope(row))
            ]
            repository.add_event(
                window_id=window["id"], event="created", actor=actor, now=now,
                detail={"scope": scope, "deadline_policy": policy,
                        "overlapping_window_ids": [row["id"] for row in overlaps]},
            )
            for other in overlaps:
                repository.add_event(
                    window_id=other["id"], event="overlap_detected", actor=actor, now=now,
                    detail={"with_window_id": window["id"], "with_window_code": payload["code"]},
                )
            return self._snapshot(repository, window, now_value)

    # ------------------------------------------------------------------ 查询
    def list_windows(self, *, include_finished: bool = False) -> list[dict[str, Any]]:
        repository = WindowRepository(self.connection)
        now_value = self.clock.now()
        return [self._snapshot(repository, row, now_value) for row in repository.list_windows(include_finished=include_finished)]

    def get_window(self, code: str) -> dict[str, Any]:
        repository = self._repository()
        return self._snapshot(repository, self._require_window(repository, code), self.clock.now())

    def claim_freeze_clause(self) -> tuple[str | None, list[Any]]:
        """领取路径调用：排除所有排空/强制窗口命中的新领取（重叠取并集，即更严格限制）。"""
        return claim_freeze_clause(self._repository().active_windows())

    # ------------------------------------------------------------- 推进/撤销
    def advance(self, code: str, actor: str, *, to_stage: str | None = None) -> dict[str, Any]:
        """推进窗口到下一阶段。

        - to_stage 为下一阶段（announced→draining→enforcing→recovered）时执行一次流转；
        - 窗口已处于 to_stage 时直接返回当前进度（定时推进与人工调用并发也安全）；
        - 截止处理逐任务落 UNIQUE 记录，因此 enforce 动作重复调用绝不会重复干预。
        """
        with transaction(immediate=True) as connection:
            repository = WindowRepository(connection)
            window = self._require_window(repository, code)
            apply_scheduled_transitions(connection, self.clock)
            window = repository.window_by_id(window["id"])
            stage = window["stage"]
            if stage in {"recovered", "revoked"}:
                if to_stage is not None and to_stage != stage:
                    raise ConflictError(f"窗口已处于终态 {stage}，不能继续推进")
                return self._snapshot(repository, window, self.clock.now())
            target = to_stage or NEXT_STAGE[stage]
            if target not in STAGE_RANK or target == "revoked":
                raise ValidationError(f"未知或不可通过推进进入的阶段：{target}")
            target_rank = STAGE_RANK[target]
            if target_rank < STAGE_RANK[stage]:
                raise ConflictError(f"窗口当前处于 {stage}，不能回退到 {target}")
            if target_rank > STAGE_RANK[stage] + 1:
                raise ConflictError("维护窗口只能逐阶段推进，不能跳阶段")
            if target_rank == STAGE_RANK[stage]:
                # 同一动作重复调用：窗口已处于目标阶段，原样返回进度，不重复干预。
                return self._snapshot(repository, window, self.clock.now())
            _enter_stage(repository, window, target, actor, self.clock)
            return self._snapshot(repository, repository.window_by_id(window["id"]), self.clock.now())

    def revoke(self, code: str, actor: str, reason: str) -> dict[str, Any]:
        """撤销窗口：立即解除领取限制；若已强制取消过任务，则把仍处取消态的任务重新排队。"""
        if not reason or not reason.strip():
            raise ValidationError("撤销原因不能为空")
        with transaction(immediate=True) as connection:
            repository = WindowRepository(connection)
            window = self._require_window(repository, code)
            apply_scheduled_transitions(connection, self.clock)
            window = repository.window_by_id(window["id"])
            if window["stage"] in {"recovered", "revoked"}:
                raise ConflictError(f"窗口已处于 {window['stage']}，不能撤销")
            now = to_storage(self.clock.now())
            restored = self._undo_enforce(repository, window, actor, now) if window["stage"] == "enforcing" else []
            repository.update_stage(window["id"], "revoked", now=now, revoked_at=now, revoke_reason=reason[:1000])
            repository.add_event(
                window_id=window["id"], event="revoked", actor=actor, now=now,
                detail={"reason": reason, "restored_task_ids": restored},
            )
            return self._snapshot(repository, repository.window_by_id(window["id"]), self.clock.now())

    # ------------------------------------------------------------- 撤销补偿
    @staticmethod
    def _undo_enforce(repository: WindowRepository, window, actor: str, now: str) -> list[int]:
        """回滚本窗口强制阶段的取消：仅恢复仍处取消态、未被人工另行处理的任务。"""
        restored: list[int] = []
        for record in repository.actions(window["id"], phase="enforce"):
            if record["action"] not in {"cancelled", "force_cancelled"}:
                continue
            task_id = int(record["task_id"])
            task = repository.task_row(task_id)
            if task is None or task["status"] != "cancelled":
                continue
            if not repository.record_action(
                window_id=window["id"], task_id=task_id, phase="restore", action="restored",
                before=dict(task), after={}, actor=actor, now=now,
            ):
                continue
            repository.connection.execute(
                "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',finished_at=NULL,available_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task_id),
            )
            after = dict(repository.task_row(task_id))
            repository.connection.execute(
                "UPDATE compute_maintenance_task_actions SET after_json=? WHERE window_id=? AND task_id=? AND phase='restore'",
                (_json(after), window["id"], task_id),
            )
            repository.add_intervention_reflection(
                window, task_id, actor, "window_restored", f"维护窗口 {window['code']} 撤销恢复", dict(task), after, now,
            )
            restored.append(task_id)
        return restored

    # ------------------------------------------------------------------ 快照
    def _snapshot(self, repository: WindowRepository, window, now_value) -> dict[str, Any]:
        scope = repository.parse_scope(window)
        stage = window["stage"]
        counts = repository.matched_status_counts(scope)
        blockers: list[dict[str, Any]] = []
        if stage in ACTIVE_STAGES:
            for task in repository.matched_tasks(scope, BLOCKING_STATUSES):
                reasons = ["任务仍被工作者持有，等待自然结束"]
                lease_until = task["lease_expires_at"]
                if lease_until and now_value >= from_storage(lease_until):
                    reasons.append("租约已过期但尚未释放，可等待租约回收或进入强制处理")
                blockers.append({
                    "task_id": task["id"],
                    "status": task["status"],
                    "worker_id": task["lease_owner"],
                    "lease_expires_at": lease_until or None,
                    "started_at": task["started_at"],
                    "reasons": reasons,
                })
        enforce_records = repository.actions(window["id"], phase="enforce")
        restore_records = repository.actions(window["id"], phase="restore")
        processed_by_action: dict[str, int] = {}
        for record in enforce_records:
            processed_by_action[record["action"]] = processed_by_action.get(record["action"], 0) + 1
        overlapping = [
            {"id": row["id"], "code": row["code"], "stage": row["stage"],
             "deadline_policy": row["deadline_policy"]}
            for row in repository.open_windows()
            if row["id"] != window["id"] and _overlaps(repository, scope, repository.parse_scope(row))
        ]
        remaining = sum(counts.get(status, 0) for status in ENFORCE_STATUSES)
        return {
            "id": window["id"],
            "code": window["code"],
            "name": window["name"],
            "scope": scope,
            "stage": stage,
            "deadline_policy": window["deadline_policy"],
            "drain_at": window["drain_at"],
            "enforce_at": window["enforce_at"],
            "recover_at": window["recover_at"],
            "created_by": window["created_by"],
            "progress": {
                "matched_task_counts": counts,
                "remaining_intervenable": remaining,
                "frozen_queued_tasks": counts.get("queued", 0) if stage in ACTIVE_STAGES else 0,
                "blocked_workers": blockers,
                "processed_by_action": processed_by_action,
                "processed_total": len(enforce_records),
                "restored_total": len(restore_records),
                "overlapping_windows": overlapping,
            },
            "timeline": {
                "announced_at": window["entered_announced_at"],
                "draining_at": window["entered_draining_at"],
                "enforcing_at": window["entered_enforcing_at"],
                "recovered_at": window["entered_recovered_at"],
                "revoked_at": window["revoked_at"],
            },
            "events": [
                {"event": row["event"], "actor": row["actor"], "created_at": row["created_at"],
                 "detail": json.loads(row["detail_json"] or "{}")}
                for row in repository.events(window["id"])
            ],
        }

    def _repository(self) -> WindowRepository:
        return WindowRepository(self.connection)

    @staticmethod
    def _require_window(repository: WindowRepository, code: str):
        window = repository.window_by_code(code)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return window
