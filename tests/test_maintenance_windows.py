from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.compute.maintenance import MaintenanceWindowService
from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}
TEMPLATE_B = {**TEMPLATE, "code": "solver-b", "name": "备用模板", "algorithm": "solver-b"}


def submit(service: ComputeOperationsService, key: str, *, user="student-1", project="course-1", klass="class-1", priority=50):
    return service.submit({
        "template_code": "solver-a", "project_code": project, "class_code": klass,
        "requested_by": user, "parameters": {"iterations": 10}, "priority": priority,
        "idempotency_key": key,
    })


def make_services(start: datetime):
    clock = FrozenClock(start)
    compute = ComputeOperationsService(get_connection(), clock)
    windows = MaintenanceWindowService(get_connection(), clock)
    compute.create_template(TEMPLATE, "administrator")
    compute.create_template(TEMPLATE_B, "administrator")
    return clock, compute, windows


def window_payload(**overrides):
    base = {
        "code": "grade-upgrade-1",
        "reason": "评分组件升级",
        "scope": {"project_codes": ["course-1"]},
        "drain_at": None,
        "deadline_at": None,
        "deadline_policy": "cancel",
    }
    base.update(overrides)
    return base


def test_window_lifecycle_cancel_policy_blocks_claims_and_reports_blockers(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    queued = submit(compute, "mw-001", priority=10)
    other_course = submit(compute, "mw-002", project="course-2", klass="class-2")
    running = submit(compute, "mw-003", priority=90)

    created = windows.create_window(window_payload(drain_at=start + timedelta(minutes=10), deadline_at=start + timedelta(hours=1)), "ops")
    assert created["status"] == "announced"
    assert created["matched_open_total"] == 2

    # 预告期不影响领取。
    claimed = compute.claim("w1", ["solver-a"], 60)
    assert claimed["id"] == running["id"]

    clock.advance(minutes=10)
    draining = windows.advance(created["id"], "ops")
    assert draining["status"] == "draining"

    # 排空期：命中的排队任务不可领取，未命中的可以。
    assert compute.claim("w2", ["solver-a"], 60)["id"] == other_course["id"]
    assert compute.claim("w3", ["solver-a"], 60) is None

    progress = windows.progress(created["id"])
    assert progress["matched_open_tasks"] == {"queued": 1, "running": 1, "cancel_requested": 0}
    assert progress["blockers"]["total"] == 1
    assert progress["blockers"]["items"][0]["task_id"] == running["id"]
    assert "w1" in progress["blockers"]["items"][0]["reason"]
    assert progress["next_action"] == "enforce"

    clock.advance(hours=1)
    enforced = windows.advance(created["id"], "ops")
    assert enforced["status"] == "enforced"
    assert enforced["processed"]["cancelled"]["total"] == 2
    assert set(enforced["processed"]["cancelled"]["ids"]) == {queued["id"], running["id"]}
    assert compute.get_task(queued["id"])["status"] == "cancelled"
    assert compute.get_task(running["id"])["status"] == "cancelled"

    recovered = windows.advance(created["id"], "ops", "recover")
    assert recovered["status"] == "recovered"


def test_requeue_policy_restores_running_task_to_queue_with_ordering(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    low = submit(compute, "rq-low", priority=10)
    high = submit(compute, "rq-high", priority=90)
    claimed = compute.claim("w1", ["solver-a"], 60)
    assert claimed["id"] == high["id"]

    windows.create_window(window_payload(code="rq-window", deadline_policy="requeue", drain_at=start, deadline_at=start + timedelta(hours=1)), "ops")
    draining = windows.advance(1, "ops")
    assert draining["status"] == "draining"
    # 排空期间命中任务都不能领取。
    assert compute.claim("w2", ["solver-a"], 60) is None

    clock.advance(hours=1)
    enforced = windows.advance(1, "ops")
    assert enforced["processed"]["requeued"]["total"] == 2
    high_after = compute.get_task(high["id"])
    assert high_after["status"] == "queued" and high_after["priority"] == 90
    assert high_after["lease_owner"] == ""
    assert high_after["interventions"][-1]["action"] == "maintenance_requeue"

    windows.advance(1, "ops", "recover")
    # 恢复后按原优先级排序，高优先级先被领取。
    assert compute.claim("w3", ["solver-a"], 60)["id"] == high["id"]
    assert compute.claim("w4", ["solver-a"], 60)["id"] == low["id"]


def test_duplicate_advance_is_idempotent(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    submit(compute, "idm-1")
    windows.create_window(window_payload(drain_at=start + timedelta(minutes=1), deadline_at=start + timedelta(hours=1)), "ops")
    clock.advance(minutes=1)
    first = windows.advance(1, "ops")
    assert first["transitions"] == ["draining"]
    second = windows.advance(1, "ops")
    assert second["transitions"] == [] and second["noop"] is True

    clock.advance(hours=1)
    enforced_once = windows.advance(1, "ops")
    assert enforced_once["processed"]["cancelled"]["total"] == 1
    enforced_again = windows.advance(1, "ops")
    assert enforced_again["noop"] is True
    assert enforced_again["processed"]["cancelled"]["total"] == 0
    details = compute.get_task(1)
    assert [item["action"] for item in details["interventions"]].count("maintenance_cancel") == 1


def test_revoke_during_drain_reopens_claims(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    task = submit(compute, "rev-1")
    windows.create_window(window_payload(code="rev-window", drain_at=start, deadline_at=start + timedelta(hours=1)), "ops")
    windows.advance(1, "ops")
    assert compute.claim("w1", ["solver-a"], 60) is None

    revoked = windows.revoke(1, "ops", "升级延期")
    assert revoked["status"] == "revoked"
    assert compute.claim("w2", ["solver-a"], 60)["id"] == task["id"]

    again = windows.revoke(1, "ops", "升级延期")
    assert again["noop"] is True


def test_overlapping_windows_merge_with_stricter_policy_and_earlier_schedule(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    course1_task = submit(compute, "ov-1", project="course-1")
    course2_task = submit(compute, "ov-2", project="course-2", klass="class-9")

    windows.create_window(window_payload(code="ov-a", scope={"project_codes": ["course-1"]}, deadline_policy="requeue", drain_at=start, deadline_at=start + timedelta(hours=4)), "ops")
    merged = windows.create_window(window_payload(code="ov-b", scope={"project_codes": ["course-2"]}, deadline_policy="cancel", drain_at=start + timedelta(hours=3), deadline_at=start + timedelta(hours=5)), "ops")

    # 后建窗口并入更早开始的存续窗口；合并后策略升级为更严格的 cancel。
    listing = windows.list_windows()["items"]
    active = {item["code"]: item for item in listing}
    assert set(active) == {"ov-a"}
    survivor = active["ov-a"]
    assert survivor["status"] == "draining"
    assert survivor["deadline_policy"] == "cancel"
    selectors = survivor["scope"]["selectors"]
    assert len(selectors) == 2
    assert merged["id"] == survivor["id"]

    # 两个课程的任务都被拦截。
    assert compute.claim("w1", ["solver-a", "solver-b"], 60) is None
    assert windows.get_window(2)["status"] == "merged"

    clock.advance(hours=4)
    enforced = windows.advance(survivor["id"], "ops")
    assert enforced["processed"]["cancelled"]["total"] == 2
    assert {tid for tid in enforced["processed"]["cancelled"]["ids"]} == {course1_task["id"], course2_task["id"]}


def test_template_and_class_scopes_filter_independently(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    by_template = submit(compute, "sc-1")
    by_class = submit(compute, "sc-2", project="other-course")
    untouched = submit(compute, "sc-3", project="other-course", klass="class-2")

    windows.create_window(window_payload(code="sc-window", scope={"template_codes": ["solver-a"], "class_codes": ["class-1"]}, drain_at=start, deadline_at=start + timedelta(hours=1)), "ops")
    windows.advance(1, "ops")
    assert compute.claim("w1", ["solver-a"], 60)["id"] == untouched["id"]
    assert compute.claim("w2", ["solver-a"], 60) is None
    progress = windows.progress(1)
    assert progress["matched_open_total"] == 2


def test_enforce_after_deadline_requires_time_and_recover_only_after_enforce(client):
    start = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    clock, compute, windows = make_services(start)
    submit(compute, "tm-1")
    windows.create_window(window_payload(drain_at=start, deadline_at=start + timedelta(hours=1)), "ops")
    windows.advance(1, "ops")
    enforced_early = windows.advance(1, "ops", "enforce")
    # 未到截止时间，不能强制；状态仍是排空。
    assert enforced_early["status"] == "draining" and enforced_early["transitions"] == []
    recover_early = windows.advance(1, "ops", "recover")
    assert recover_early["status"] == "draining" and recover_early["transitions"] == []
