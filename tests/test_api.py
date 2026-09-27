"""HTTP 入口与服务入口结果一致：幂等、错误原因码、状态视图。"""

import pytest
from fastapi.testclient import TestClient

from main import create_app
from orchestrator import Device, ExecutionOutcome, OutcomeKind, Orchestrator
from tests.conftest import ScriptedDriver, FakeClock, wait_for


@pytest.fixture
def client():
    clock = FakeClock()
    driver = ScriptedDriver()
    orch = Orchestrator(
        devices=[Device("robot-1", max_concurrency=1)],
        driver=driver,
        clock=clock,
    )
    return TestClient(create_app(orch)), orch, driver


def test_submit_get_cancel_flow(client):
    http, orch, driver = client
    gate = driver.gate("m1")
    r = http.post("/missions", json={"mission_id": "m1", "device_id": "robot-1"})
    assert r.status_code == 201
    wait_for(lambda: driver.started["m1"].is_set())
    r = http.get("/missions/m1")
    assert r.json()["status"] == "RUNNING"
    r = http.post("/missions/m1/cancel")
    assert r.json()["status"] == "CANCELED"
    gate.set()
    # 已结束任务再取消：409 + 可区分原因
    r = http.post("/missions/m1/cancel")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "MISSION_ALREADY_FINISHED"


def test_idempotent_submit_over_http(client):
    http, orch, driver = client
    body = {"mission_id": "m1", "device_id": "robot-1", "payload": "x"}
    assert http.post("/missions", json=body).status_code == 201
    wait_for(lambda: orch.get("m1").status.value == "SUCCEEDED")
    assert http.post("/missions", json=body).status_code == 201  # 幂等命中
    assert len(http.get("/missions").json()) == 1
    conflict = {**body, "payload": "y"}
    r = http.post("/missions", json=conflict)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "DUPLICATE_MISSION_CONFLICT"


def test_not_found_and_failure_reason_over_http(client):
    http, orch, driver = client
    assert http.get("/missions/ghost").status_code == 404
    driver.script("m1", [ExecutionOutcome(OutcomeKind.TIMEOUT, "slow")])
    http.post("/missions", json={"mission_id": "m1", "device_id": "robot-1"})
    wait_for(lambda: orch.get("m1").status.value == "FAILED")
    r = http.get("/missions/m1")
    assert r.json()["error_reason"] == "DEVICE_TIMEOUT"
    # 人工干预入口
    driver.script("m1", [ExecutionOutcome(OutcomeKind.SUCCESS, "ok")])
    r = http.post("/missions/m1/retry")
    assert r.status_code == 200
    wait_for(lambda: orch.get("m1").status.value == "SUCCEEDED")
    assert http.get("/missions/m1").json()["result"] == "ok"


def test_decision_log_endpoint(client):
    http, orch, driver = client
    http.post("/missions", json={"mission_id": "m1", "device_id": "robot-1"})
    wait_for(lambda: orch.get("m1").status.value == "SUCCEEDED")
    log = "\n".join(http.get("/decision-log").json())
    assert "RUNNING -> SUCCEEDED" in log
