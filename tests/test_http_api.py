"""HTTP 入口与本地调用的一致性：依赖下发、查询、级联取消、环拒绝。"""
from fastapi.testclient import TestClient

from main import app, service

client = TestClient(app)


def test_http_dependency_chain_matches_local_view():
    r = client.post("/tasks", json={"task_id": "h-survey", "device_id": "robot-1"})
    assert r.status_code == 201
    r = client.post(
        "/tasks",
        json={"task_id": "h-inspect", "device_id": "robot-1",
              "depends_on": ["h-survey"]},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "SUCCEEDED"
    assert body["depends_on"] == ["h-survey"]
    assert body["dependency_status"] == [
        {"task_id": "h-survey", "status": "SUCCEEDED", "note": ""}
    ]
    # 与本地查询结果一致
    local = service.get("h-inspect")
    assert body["status"] == local.status.value
    assert body["depends_on"] == list(local.depends_on)


def test_http_waiting_state_queryable():
    client.post("/tasks", json={
        "task_id": "h-blocker", "device_id": "robot-1", "backoff_seconds": 0.0,
    })
    # 占用设备槽，使后续任务停在 QUEUED，模拟前置未完成场景改用前向引用
    r = client.post("/tasks", json={
        "task_id": "h-waiting", "device_id": "robot-1",
        "depends_on": ["h-missing"],
    })
    body = r.json()
    assert body["status"] == "WAITING_DEPS"
    assert body["dependency_status"][0]["task_id"] == "h-missing"
    assert body["dependency_status"][0]["status"] == "MISSING"
    # GET 查询同样能看到等待原因
    got = client.get("/tasks/h-waiting").json()
    assert got["status"] == "WAITING_DEPS"
    assert got["dependency_status"][0]["status"] == "MISSING"


def test_http_cycle_rejected_with_distinguishable_reason():
    client.post("/tasks", json={
        "task_id": "h-a", "device_id": "robot-1", "depends_on": ["h-b"],
    })
    r = client.post("/tasks", json={
        "task_id": "h-b", "device_id": "robot-1", "depends_on": ["h-a"],
    })
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "DEPENDENCY_CYCLE"
    # 本地入口对同一操作给出同一原因码
    from orchestrator.errors import RequestRejected
    from orchestrator.models import RejectReason, TaskRecord
    try:
        service.submit(TaskRecord(
            task_id="h-b", device_id="robot-1", depends_on=("h-a",)))
        raise AssertionError("应当拒绝")
    except RequestRejected as e:
        assert e.reason == RejectReason.DEPENDENCY_CYCLE


def test_http_cancel_with_cascade():
    client.post("/tasks", json={
        "task_id": "h-root", "device_id": "robot-1", "depends_on": ["h-never"],
    })
    client.post("/tasks", json={
        "task_id": "h-child", "device_id": "robot-1", "depends_on": ["h-root"],
    })
    r = client.post("/tasks/h-root/cancel?cascade=true")
    assert r.status_code == 200
    child = client.get("/tasks/h-child").json()
    assert child["status"] == "CANCELLED"
    assert child["failure_reason"] == "DEPENDENCY_CANCELLED"


def test_http_duplicate_submit_idempotent():
    payload = {"task_id": "h-dup", "device_id": "robot-1", "depends_on": []}
    r1 = client.post("/tasks", json=payload)
    r2 = client.post("/tasks", json=payload)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["attempts"] == r2.json()["attempts"] == 1
