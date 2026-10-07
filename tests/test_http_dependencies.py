"""HTTP 入口与本地调用一致性：依赖下发、等待可见、环拒绝、级联取消。"""
import importlib

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client():
    importlib.reload(main)  # 重置模块级单例，保证用例间隔离
    return TestClient(main.app)


def test_http_dependency_flow_matches_local_semantics(client):
    # 逆序下发：b 等 a（允许前置后补下发）
    r = client.post("/tasks", json={"task_id": "b", "device_id": "robot-1",
                                    "depends_on": ["a"]})
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "WAITING_DEPENDENCY"
    assert body["depends_on"] == ["a"]
    assert body["waiting_on"] == ["a(未下发)"]

    r = client.post("/tasks", json={"task_id": "a", "device_id": "robot-1"})
    assert r.status_code == 201
    assert r.json()["status"] == "SUCCEEDED"

    # a 成功后 b 被推进执行
    body = client.get("/tasks/b").json()
    assert body["status"] == "SUCCEEDED"
    assert body["waiting_on"] == []


def test_http_cycle_and_self_dependency_rejected(client):
    r = client.post("/tasks", json={"task_id": "s", "device_id": "robot-1",
                                    "depends_on": ["s"]})
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "SELF_DEPENDENCY"

    client.post("/tasks", json={"task_id": "x", "device_id": "robot-1",
                                "depends_on": ["y"]})
    r = client.post("/tasks", json={"task_id": "y", "device_id": "robot-1",
                                    "depends_on": ["x"]})
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "DEPENDENCY_CYCLE"


def test_http_cascade_cancel(client):
    client.post("/tasks", json={"task_id": "p", "device_id": "robot-1",
                                "depends_on": ["q"]})
    client.post("/tasks", json={"task_id": "q", "device_id": "robot-1",
                                "depends_on": ["r"]})
    # r 占不到槽（robot-1 并发 1，先让它排队）：直接取消 q 并级联
    r = client.post("/tasks/q/cancel", params={"cascade": "true"})
    assert r.json()["status"] == "CANCELLED"
    # p 是 q 的上游依赖方（p 等 q），级联取消带走下游 p
    assert client.get("/tasks/p").json()["status"] == "CANCELLED"
    assert client.get("/tasks/p").json()["failure_reason"] == "CANCELLED_BY_USER"


def test_http_idempotent_submit_with_dependencies(client):
    payload = {"task_id": "dup", "device_id": "robot-1", "depends_on": ["later"]}
    r1 = client.post("/tasks", json=payload)
    r2 = client.post("/tasks", json=payload)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["task_id"] == r2.json()["task_id"] == "dup"
    assert r2.json()["status"] == "WAITING_DEPENDENCY"
