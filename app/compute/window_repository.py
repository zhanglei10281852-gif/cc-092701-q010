from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable, Sequence

SCOPE_FIELDS = ("template_codes", "project_codes", "class_codes")
ACTIVE_STAGES = ("draining", "enforcing")
TERMINAL_STAGES = ("recovered", "revoked")


def normalize_scope(scope: dict[str, Any]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for field in SCOPE_FIELDS:
        values = scope.get(field) or []
        if not isinstance(values, list):
            raise ValueError(f"{field} 必须是字符串数组")
        cleaned = sorted({str(value).strip() for value in values if str(value).strip()})
        if cleaned:
            normalized[field] = cleaned
    return normalized


def scope_sql(scope: dict[str, Any], *, task_alias: str = "t", template_alias: str = "tpl") -> tuple[str, list[Any]]:
    """返回任务行是否命中筛选范围的 SQL 片段（维度之间为 OR）。"""
    clauses: list[str] = []
    params: list[Any] = []
    templates = scope.get("template_codes") or []
    if templates:
        clauses.append(f"{template_alias}.code IN ({','.join('?' for _ in templates)})")
        params.extend(templates)
    projects = scope.get("project_codes") or []
    if projects:
        clauses.append(f"{task_alias}.project_code IN ({','.join('?' for _ in projects)})")
        params.extend(projects)
    classes = scope.get("class_codes") or []
    if classes:
        clauses.append(f"{task_alias}.class_code IN ({','.join('?' for _ in classes)})")
        params.extend(classes)
    return " OR ".join(clauses), params


def scopes_intersect(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return any(set(left.get(field) or []) & set(right.get(field) or []) for field in SCOPE_FIELDS)


def claim_freeze_clause(windows: Sequence[sqlite3.Row]) -> tuple[str | None, list[Any]]:
    """构造领取时排除所有排空/强制窗口命中任务的 SQL 片段（窗口取并集 = 更严格）。"""
    groups: list[str] = []
    params: list[Any] = []
    for window in windows:
        if window["stage"] not in ACTIVE_STAGES:
            continue
        group_sql, group_params = scope_sql(json.loads(window["scope_json"]))
        if group_sql:
            groups.append(f"({group_sql})")
            params.extend(group_params)
    if not groups:
        return None, []
    return " AND NOT (" + " OR ".join(groups) + ")", params


class WindowRepository:
    """维护窗口及其逐任务处理记录、阶段事件的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def create_window(
        self,
        *,
        code: str,
        name: str,
        scope: dict[str, list[str]],
        deadline_policy: str,
        drain_at: str,
        enforce_at: str,
        recover_at: str | None,
        created_by: str,
        now: str,
    ) -> sqlite3.Row:
        cursor = self.connection.execute(
            """
            INSERT INTO compute_maintenance_windows
                (code,name,scope_json,stage,deadline_policy,drain_at,enforce_at,recover_at,
                 entered_announced_at,created_by,created_at,updated_at)
            VALUES(?,?,?, 'announced', ?,?,?,?, ?,?,?,?)
            """,
            (code, name, json.dumps(scope, ensure_ascii=False, sort_keys=True), deadline_policy,
             drain_at, enforce_at, recover_at, now, created_by, now, now),
        )
        return self.window_by_id(cursor.lastrowid)

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE id=?", (window_id,)).fetchone()

    def window_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_maintenance_windows WHERE code=?", (code,)).fetchone()

    def list_windows(self, *, include_finished: bool) -> list[sqlite3.Row]:
        sql = "SELECT * FROM compute_maintenance_windows"
        if not include_finished:
            sql += f" WHERE stage NOT IN {TERMINAL_STAGES}"
        return list(self.connection.execute(sql + " ORDER BY id DESC").fetchall())

    def open_windows(self) -> list[sqlite3.Row]:
        return list(self.connection.execute(
            f"SELECT * FROM compute_maintenance_windows WHERE stage NOT IN {TERMINAL_STAGES} ORDER BY id"
        ).fetchall())

    def active_windows(self) -> list[sqlite3.Row]:
        return list(self.connection.execute(
            f"SELECT * FROM compute_maintenance_windows WHERE stage IN {ACTIVE_STAGES} ORDER BY id"
        ).fetchall())

    def update_stage(self, window_id: int, stage: str, *, now: str, **fields: Any) -> None:
        assignments = ["stage=?", "updated_at=?"]
        params: list[Any] = [stage, now]
        for column, value in fields.items():
            assignments.append(f"{column}=?")
            params.append(value)
        params.append(window_id)
        self.connection.execute(f"UPDATE compute_maintenance_windows SET {','.join(assignments)} WHERE id=?", params)

    def matched_tasks(self, scope: dict[str, list[str]], statuses: Iterable[str]) -> list[sqlite3.Row]:
        match_sql, match_params = scope_sql(scope)
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        return list(self.connection.execute(
            f"""
            SELECT t.*, tpl.code AS template_code, tpl.algorithm AS template_algorithm
            FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id
            WHERE t.status IN ({placeholders}) AND ({match_sql})
            ORDER BY t.id
            """,
            [*statuses, *match_params],
        ).fetchall())

    def matched_status_counts(self, scope: dict[str, list[str]]) -> dict[str, int]:
        match_sql, match_params = scope_sql(scope)
        rows = self.connection.execute(
            f"""
            SELECT t.status AS status, COUNT(*) AS amount
            FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id
            WHERE ({match_sql})
            GROUP BY t.status
            """,
            match_params,
        ).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def scopes_share_tasks(self, left: dict[str, list[str]], right: dict[str, list[str]]) -> bool:
        """两个筛选范围在当前任务数据上是否命中同一批任务（跨维度重叠，如课程×班级）。"""
        left_sql, left_params = scope_sql(left)
        right_sql, right_params = scope_sql(right)
        row = self.connection.execute(
            f"""
            SELECT 1 FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id
            WHERE ({left_sql}) AND ({right_sql}) LIMIT 1
            """,
            [*left_params, *right_params],
        ).fetchone()
        return row is not None

    def task_row(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE id=?", (task_id,)).fetchone()

    def add_intervention_reflection(
        self, window: sqlite3.Row, task_id: int, actor: str, action: str, reason: str,
        before: dict[str, Any], after: dict[str, Any], now: str,
    ) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason,
             json.dumps(before, ensure_ascii=False, sort_keys=True),
             json.dumps(after, ensure_ascii=False, sort_keys=True),
             f"window-{window['id']}", now),
        )

    def record_action(self, *, window_id: int, task_id: int, phase: str, action: str,
                      before: dict[str, Any], after: dict[str, Any], actor: str, now: str) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO compute_maintenance_task_actions
                (window_id,task_id,phase,action,before_json,after_json,actor,created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (window_id, task_id, phase, action,
             json.dumps(before, ensure_ascii=False, sort_keys=True),
             json.dumps(after, ensure_ascii=False, sort_keys=True), actor, now),
        )
        return cursor.rowcount == 1

    def actions(self, window_id: int, phase: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM compute_maintenance_task_actions WHERE window_id=?"
        params: list[Any] = [window_id]
        if phase is not None:
            sql += " AND phase=?"
            params.append(phase)
        sql += " ORDER BY id"
        return [dict(row) for row in self.connection.execute(sql, params).fetchall()]

    def add_event(self, *, window_id: int, event: str, detail: dict[str, Any], actor: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_maintenance_events(window_id,event,detail_json,actor,created_at) VALUES(?,?,?,?,?)",
            (window_id, event, json.dumps(detail, ensure_ascii=False, sort_keys=True), actor, now),
        )

    def events(self, window_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM compute_maintenance_events WHERE window_id=? ORDER BY id", (window_id,)
        ).fetchall()]

    @staticmethod
    def parse_scope(row: sqlite3.Row) -> dict[str, list[str]]:
        return json.loads(row["scope_json"])
