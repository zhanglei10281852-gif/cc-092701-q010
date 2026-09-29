from __future__ import annotations

from datetime import UTC, datetime, timedelta


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def _prepare(client) -> None:
    assert client.post("/api/compute/templates?actor=administrator", json=TEMPLATE).status_code == 201


def _submit(client, key, *, project="course-a", klass="class-1"):
    return client.post("/api/compute/tasks", json={
        "template_code": "solver-a", "project_code": project, "class_code": klass,
        "requested_by": "student-1", "parameters": {"iterations": 10},
        "priority": 50, "idempotency_key": key,
    }).json()


def _create_window(client, code, scope, **extra):
    payload = {
        "code": code, "name": f"窗口-{code}", "scope": scope,
        "created_by": "ops-1", "enforce_after_seconds": 3600,
    }
    payload.update(extra)
    response = client.post("/api/compute/maintenance-windows", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def test_window_full_lifecycle_over_http(client):
    _prepare(client)
    held = _submit(client, "http-0001")
    queued = _submit(client, "http-0002", klass="class-2")

    window = _create_window(
        client, "win-http",
        {"template_codes": ["solver-a"], "project_codes": ["course-a"], "class_codes": ["class-1", "class-2"]},
        deadline_policy="cancel",
    )
    assert window["stage"] == "announced"
    assert window["scope"]["class_codes"] == ["class-1", "class-2"]

    # 预告 → 排空
    draining = client.post("/api/compute/maintenance-windows/win-http/advance?actor=ops-1&to_stage=draining")
    assert draining.status_code == 200, draining.text
    body = draining.json()
    assert body["stage"] == "draining"
    assert body["progress"]["frozen_queued_tasks"] == 2
    assert body["timeline"]["draining_at"]

    # 排空期命中任务无法被新领取
    assert client.post("/api/compute/tasks/claim",
                       json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"] is None

    # 同一动作重复调用：不重复干预，处理总数保持 0
    repeat = client.post("/api/compute/maintenance-windows/win-http/advance?actor=ops-1&to_stage=draining")
    assert repeat.status_code == 200
    assert repeat.json()["progress"]["processed_total"] == 0

    # 排空 → 强制：截止取消存量（held、queued 都在队列中且命中）
    enforcing = client.post("/api/compute/maintenance-windows/win-http/advance?actor=ops-1&to_stage=enforcing")
    assert enforcing.status_code == 200
    progress = enforcing.json()["progress"]
    assert progress["processed_total"] == 2
    assert progress["processed_by_action"] == {"cancelled": 2}

    # 强制动作重复调用不再重复处理
    enforce_again = client.post("/api/compute/maintenance-windows/win-http/advance?actor=ops-1&to_stage=enforcing")
    assert enforce_again.json()["progress"]["processed_total"] == progress["processed_total"]

    # 恢复并查询
    recovered = client.post("/api/compute/maintenance-windows/win-http/advance?actor=ops-1&to_stage=recovered")
    assert recovered.json()["stage"] == "recovered"
    listed = client.get("/api/compute/maintenance-windows?include_finished=true")
    assert any(item["code"] == "win-http" for item in listed.json()["items"])


def test_window_revoke_and_overlap_over_http(client):
    _prepare(client)
    _submit(client, "http-1001", project="course-a", klass="class-9")
    _create_window(client, "win-one", {"project_codes": ["course-a"]})
    second = _create_window(client, "win-two", {"class_codes": ["class-9"]})
    # 数据感知重叠：course-a 与 class-9 命中同一任务
    assert second["progress"]["overlapping_windows"][0]["code"] == "win-one"

    client.post("/api/compute/maintenance-windows/win-one/advance?actor=ops-1&to_stage=draining")
    assert client.post("/api/compute/tasks/claim",
                       json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"] is None
    revoked = client.post("/api/compute/maintenance-windows/win-one/revoke",
                          json={"actor": "ops-1", "reason": "升级延期，撤销窗口"})
    assert revoked.status_code == 200 and revoked.json()["stage"] == "revoked"
    # win-one 解除冻结后，仅剩 win-two 的班级冻结仍生效（任务仍命中）
    assert client.post("/api/compute/tasks/claim",
                       json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"] is None


def test_window_validation_errors(client):
    _prepare(client)
    # 空筛选范围
    bad = client.post("/api/compute/maintenance-windows", json={
        "code": "win-bad", "name": "非法窗口", "scope": {}, "created_by": "ops-1",
    })
    assert bad.status_code == 422
    # 不存在的窗口
    assert client.post("/api/compute/maintenance-windows/missing/advance?actor=ops-1").status_code == 404
