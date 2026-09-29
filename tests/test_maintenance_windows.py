from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.compute.service import ComputeOperationsService
from app.compute.window_service import MaintenanceWindowService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import close_connection, get_connection, transaction

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}
TEMPLATE_B = {**TEMPLATE, "code": "solver-b", "name": "模板乙", "algorithm": "solver-b"}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "windows.db"))
    close_connection()
    from app.database import init_db
    init_db()
    clock = FrozenClock(datetime(2026, 9, 29, 0, 0, tzinfo=UTC))
    tasks = ComputeOperationsService(get_connection(), clock)
    windows = MaintenanceWindowService(get_connection(), clock)
    tasks.create_template(TEMPLATE, "administrator")
    tasks.create_template(TEMPLATE_B, "administrator")
    yield tasks, windows, clock
    close_connection()


def submit(tasks, key, *, project="course-a", klass="", user="student-1", priority=50, template="solver-a"):
    return tasks.submit({
        "template_code": template, "project_code": project, "class_code": klass,
        "requested_by": user, "parameters": {"iterations": 10}, "priority": priority,
        "idempotency_key": key,
    })


def make_window(windows, code, scope, *, policy="cancel", drain_at=None, enforce_at=None, actor="ops"):
    now = windows.clock.now()
    return windows.create_window({
        "code": code, "name": f"窗口-{code}", "scope": scope,
        "deadline_policy": policy,
        "drain_at": drain_at or now,
        "enforce_at": enforce_at or (now + timedelta(hours=1)),
    }, actor)


# ---------------------------------------------------------------- 预告与排空
def test_announced_window_does_not_block_claims(env):
    tasks, windows, clock = env
    frozen_task = submit(tasks, "ann-0001", project="course-a")
    other = submit(tasks, "ann-0002", project="course-b")
    make_window(windows, "win-ann", {"project_codes": ["course-a"]},
                drain_at=clock.now() + timedelta(hours=2),
                enforce_at=clock.now() + timedelta(hours=3))
    claimed = tasks.claim("w1", ["solver-a"], 60)
    # 预告阶段不影响领取：先创建的 course-a 任务照常被领走
    assert claimed["id"] == frozen_task["id"]


def test_draining_only_freezes_matching_new_claims(env):
    tasks, windows, clock = env
    target = submit(tasks, "drain-001", project="course-a", klass="class-1")
    by_class = submit(tasks, "drain-002", project="course-x", klass="class-1")
    free = submit(tasks, "drain-003", project="course-b")
    window = make_window(windows, "win-drain", {"class_codes": ["class-1"]})
    windows.advance("win-drain", "ops", to_stage="draining")

    # 排空只影响命中任务的新领取；不命中的 course-b 正常领取
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == free["id"]
    assert tasks.claim("w1", ["solver-a"], 60) is None

    # 命中任务在排空期间仍可正常提交，只是排队不可被领取
    late = submit(tasks, "drain-004", project="course-a", klass="class-1")
    assert late["status"] == "queued"
    assert tasks.claim("w1", ["solver-a"], 60) is None

    snapshot = windows.get_window("win-drain")
    assert snapshot["stage"] == "draining"
    assert snapshot["progress"]["frozen_queued_tasks"] == 3
    assert snapshot["progress"]["matched_task_counts"]["queued"] == 3


def test_draining_reports_blocking_workers_and_reasons(env):
    tasks, windows, clock = env
    # 预告期领取、进入排空后仍持有任务的工作者是阻塞对象
    held = submit(tasks, "block-002", project="course-c")
    make_window(windows, "win-block2", {"project_codes": ["course-c"]},
                drain_at=clock.now() + timedelta(minutes=30),
                enforce_at=clock.now() + timedelta(hours=2))
    assert tasks.claim("worker-9", ["solver-a"], 60)["id"] == held["id"]
    windows.advance("win-block2", "ops", to_stage="draining")
    snapshot = windows.get_window("win-block2")
    blockers = snapshot["progress"]["blocked_workers"]
    assert len(blockers) == 1
    blocker = blockers[0]
    assert blocker["task_id"] == held["id"] and blocker["worker_id"] == "worker-9"
    assert any("仍被工作者持有" in reason for reason in blocker["reasons"])
    # 恢复后不再上报阻塞对象
    windows.advance("win-block2", "ops", to_stage="enforcing")
    windows.advance("win-block2", "ops", to_stage="recovered")
    assert windows.get_window("win-block2")["progress"]["blocked_workers"] == []


# ------------------------------------------------------------- 截止：取消策略
def test_enforce_cancel_policy_cancels_existing_tasks_once(env):
    tasks, windows, clock = env
    # 先创建课程 c 的任务，保证预告期领取时它（而非课程 a 任务）被工作者领走
    held = submit(tasks, "cancel-02", project="course-c")
    queued = submit(tasks, "cancel-01", project="course-a")
    make_window(windows, "win-cancel-c", {"project_codes": ["course-c"]},
                drain_at=clock.now() + timedelta(minutes=30),
                enforce_at=clock.now() + timedelta(hours=2))
    assert tasks.claim("worker-9", ["solver-a"], 60)["id"] == held["id"]
    make_window(windows, "win-cancel", {"project_codes": ["course-a", "course-c"]})
    windows.advance("win-cancel", "ops", to_stage="draining")
    result = windows.advance("win-cancel", "ops", to_stage="enforcing")

    processed = result["progress"]["processed_by_action"]
    assert processed["cancelled"] == 1          # 排队任务
    assert processed["force_cancelled"] == 1    # 仍占用工作者的长任务
    assert tasks.get_task(queued["id"])["status"] == "cancelled"
    assert tasks.get_task(held["id"])["status"] == "cancelled"

    # 同一动作重复调用不能重复干预
    again = windows.advance("win-cancel", "ops", to_stage="enforcing")
    assert again["progress"]["processed_total"] == 2
    interventions = [i["action"] for i in tasks.get_task(queued["id"])["interventions"]]
    assert interventions.count("window_cancelled") == 1


# ---------------------------------------------------------- 截止：重新排队策略
def test_enforce_requeue_policy_and_recovery_restores_order(env):
    tasks, windows, clock = env
    # 预告期先让长任务（课程 a，优先级 50）被工作者领走
    running = submit(tasks, "rq-01", project="course-a", priority=50)
    make_window(windows, "win-rq", {"project_codes": ["course-a"]},
                policy="requeue",
                drain_at=clock.now() + timedelta(minutes=30),
                enforce_at=clock.now() + timedelta(hours=2))
    assert tasks.claim("worker-long", ["solver-a"], 60)["id"] == running["id"]

    # 排空开始前补齐排队任务：同课程高/低优先级，以及课程外任务
    low = submit(tasks, "rq-02", project="course-a", priority=10)
    high = submit(tasks, "rq-03", project="course-a", priority=90)
    outside = submit(tasks, "rq-04", project="course-b", priority=1)
    windows.advance("win-rq", "ops", to_stage="draining")
    # 排空期：外部课程可领，命中课程冻结
    assert tasks.claim("w2", ["solver-a"], 60)["id"] == outside["id"]
    assert tasks.claim("w2", ["solver-a"], 60) is None

    windows.advance("win-rq", "ops", to_stage="enforcing")
    details = tasks.get_task(running["id"])
    assert details["status"] == "queued"
    assert [i["action"] for i in details["interventions"]][-1] == "window_force_requeued"
    # 强制重排不消耗额外尝试次数、不改写优先级
    assert details["attempt_count"] == 1 and details["priority"] == 50
    # 强制阶段依然禁止新领取
    assert tasks.claim("w2", ["solver-a"], 60) is None

    windows.advance("win-rq", "ops", to_stage="recovered")
    # 恢复后按原有 priority / created_at 排序竞争：high → running(50,最早) → low(10)
    assert tasks.claim("w3", ["solver-a"], 60)["id"] == high["id"]
    assert tasks.claim("w3", ["solver-a"], 60)["id"] == running["id"]
    assert tasks.claim("w3", ["solver-a"], 60)["id"] == low["id"]
    assert tasks.claim("w3", ["solver-a"], 60) is None
    # 被重排队任务的优先级未被窗口改写
    assert tasks.get_task(running["id"])["priority"] == 50


def test_requeue_policy_respects_existing_cancel_request(env):
    tasks, windows, clock = env
    task = submit(tasks, "cr-01", project="course-a")
    make_window(windows, "win-cr", {"project_codes": ["course-a"]},
                drain_at=clock.now() + timedelta(minutes=30),
                enforce_at=clock.now() + timedelta(hours=2))
    tasks.claim("w1", ["solver-a"], 60)
    tasks.cancel(task["id"], "teacher", "学员撤回")  # running → cancel_requested
    windows.advance("win-cr", "ops", to_stage="draining")
    result = windows.advance("win-cr", "ops", to_stage="enforcing", )
    # requeue 策略也尊重已有的取消请求
    assert result["progress"]["processed_by_action"]["force_cancelled"] == 1
    assert tasks.get_task(task["id"])["status"] == "cancelled"


# ------------------------------------------------------------------ 撤销
def test_revoke_draining_lifts_freeze(env):
    tasks, windows, clock = env
    task = submit(tasks, "rev-01", project="course-a")
    make_window(windows, "win-rev", {"project_codes": ["course-a"]})
    windows.advance("win-rev", "ops", to_stage="draining")
    assert tasks.claim("w1", ["solver-a"], 60) is None
    windows.revoke("win-rev", "ops", "升级取消，恢复业务")
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == task["id"]
    snapshot = windows.get_window("win-rev")
    assert snapshot["stage"] == "revoked" and snapshot["timeline"]["revoked_at"]
    # 撤销终态重复撤销应被拒绝
    with pytest.raises(ConflictError):
        windows.revoke("win-rev", "ops", "再次撤销")


def test_revoke_enforcing_restores_cancelled_tasks(env):
    tasks, windows, clock = env
    t1 = submit(tasks, "undo-01", project="course-a")
    t2 = submit(tasks, "undo-02", project="course-a")
    make_window(windows, "win-undo", {"project_codes": ["course-a"]})
    windows.advance("win-undo", "ops", to_stage="draining")
    windows.advance("win-undo", "ops", to_stage="enforcing")
    # 人工已把 t1 重新排队：撤销不应覆盖人工处置
    tasks.retry(t1["id"], "teacher", "人工恢复")
    snapshot = windows.revoke("win-undo", "ops", "窗口误操作，回滚")
    assert snapshot["progress"]["restored_total"] == 1
    assert tasks.get_task(t1["id"])["status"] == "queued"
    assert tasks.get_task(t2["id"])["status"] == "queued"
    # 恢复的任务立即重新参与领取竞争
    assert tasks.claim("w1", ["solver-a"], 60) is not None
    # 再次撤销/恢复动作不重复插入补偿
    assert windows.get_window("win-undo")["progress"]["restored_total"] == 1


# ------------------------------------------------------------- 重叠窗口合并
def test_overlapping_windows_merge_with_stricter_union(env):
    tasks, windows, clock = env
    in_a = submit(tasks, "ov-01", project="course-a", klass="class-9")
    in_class = submit(tasks, "ov-02", project="course-z", klass="class-9")
    free = submit(tasks, "ov-03", project="course-b")
    make_window(windows, "win-a", {"project_codes": ["course-a"]})
    make_window(windows, "win-class", {"class_codes": ["class-9"]})
    windows.advance("win-a", "ops", to_stage="draining")
    windows.advance("win-class", "ops", to_stage="draining")

    # 并集冻结：course-a 与 class-9 命中任务都不可领，只有 course-b 可领
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == free["id"]
    assert tasks.claim("w1", ["solver-a"], 60) is None

    snapshot = windows.get_window("win-a")
    overlap = snapshot["progress"]["overlapping_windows"]
    assert {item["code"] for item in overlap} == {"win-class"}
    # 创建时：先存在的 win-a 收到 overlap_detected 事件；新窗口 created 事件里携带重叠窗口
    assert any(event["event"] == "overlap_detected" for event in windows.get_window("win-a")["events"])
    created_detail = [e for e in windows.get_window("win-class")["events"] if e["event"] == "created"][0]["detail"]
    assert created_detail["overlapping_window_ids"] == [snapshot["id"]]


def test_overlapping_cancel_policy_wins_over_requeue(env):
    tasks, windows, clock = env
    t1 = submit(tasks, "strict-01", project="course-a")
    t2 = submit(tasks, "strict-02", project="course-a")
    # 宽窗口：课程级 requeue；严窗口：模板级 cancel，与课程窗口命中同一批任务
    make_window(windows, "win-soft", {"project_codes": ["course-a"]}, policy="requeue")
    make_window(windows, "win-hard", {"template_codes": ["solver-a"]}, policy="cancel",
                drain_at=clock.now(), enforce_at=clock.now() + timedelta(hours=2))
    windows.advance("win-soft", "ops", to_stage="draining")
    windows.advance("win-hard", "ops", to_stage="draining")
    windows.advance("win-soft", "ops", to_stage="enforcing")  # 先 requeue：排队任务保持 queued
    assert tasks.get_task(t1["id"])["status"] == "queued"
    windows.advance("win-hard", "ops", to_stage="enforcing")  # 更严格的 cancel 随后生效
    assert tasks.get_task(t1["id"])["status"] == "cancelled"
    assert tasks.get_task(t2["id"])["status"] == "cancelled"


# ------------------------------------------------------------- 定时自动流转
def test_scheduled_transitions_apply_on_claim(env):
    tasks, windows, clock = env
    t1 = submit(tasks, "sched-01", project="course-a")
    t2 = submit(tasks, "sched-02", project="course-b")
    start = clock.now()
    make_window(windows, "win-sched", {"project_codes": ["course-a"]},
                drain_at=start + timedelta(hours=1), enforce_at=start + timedelta(hours=2))

    # 预告期：正常领取
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == t1["id"]
    tasks.complete(t1["id"], "w1", {"value": 1}, {})

    clock.advance(hours=1)
    # 到达排空时刻：领取动作自动把窗口推进到 draining，course-a 新任务被冻结
    late = submit(tasks, "sched-03", project="course-a")
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == t2["id"]
    assert windows.get_window("win-sched")["stage"] == "draining"

    clock.advance(hours=1)
    # 到达强制时刻：领取动作自动推进并按 cancel 处理存量
    tasks.claim("w1", ["solver-a"], 60)
    assert windows.get_window("win-sched")["stage"] == "enforcing"
    assert tasks.get_task(late["id"])["status"] == "cancelled"


def test_scheduled_recover_lifts_freeze(env):
    tasks, windows, clock = env
    t1 = submit(tasks, "auto-01", project="course-a")
    start = clock.now()
    windows.create_window({
        "code": "win-auto", "name": "自动恢复窗口",
        "scope": {"project_codes": ["course-a"]}, "deadline_policy": "requeue",
        "drain_at": start, "enforce_at": start + timedelta(hours=1),
        "recover_at": start + timedelta(hours=2),
    }, "ops")
    clock.advance(hours=1)
    tasks.claim("w1", ["solver-a"], 60)  # 触发到 enforcing
    assert windows.get_window("win-auto")["stage"] == "enforcing"
    assert tasks.claim("w1", ["solver-a"], 60) is None
    clock.advance(hours=1)
    assert tasks.claim("w1", ["solver-a"], 60)["id"] == t1["id"]
    assert windows.get_window("win-auto")["stage"] == "recovered"
