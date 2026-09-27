"""取消：合法路径流转，已结束任务的取消被可区分地拒绝。"""
import pytest

from orchestrator.errors import RequestRejected
from orchestrator.models import (
    Device,
    FailureReason,
    RejectReason,
    TaskRecord,
    TaskStatus,
)
from orchestrator.service import OrchestratorService


def build(fault_plan=None):
    svc = OrchestratorService(fault_plan=fault_plan)
    svc._store.add_device(Device("robot-1", max_concurrency=1))
    return svc


def test_cancel_queued_task():
    svc = build()
    # 先占满设备：t1 等待长退避期间不占槽，改用直接构造排队场景
    t1 = TaskRecord(task_id="t1", device_id="robot-1")
    svc.submit(t1)
    assert t1.status == TaskStatus.SUCCEEDED

    # 构造一个 QUEUED 任务：设备槽被占时提交
    svc._store.get_device("robot-1").running = 1  # 模拟槽被占
    t2 = TaskRecord(task_id="t2", device_id="robot-1")
    svc.submit(t2)
    assert t2.status == TaskStatus.QUEUED

    cancelled = svc.cancel("t2")
    assert cancelled.status == TaskStatus.CANCELLED
    assert cancelled.failure_reason == FailureReason.CANCELLED_BY_USER


def test_cancel_finished_task_rejected_distinguishable():
    svc = build()
    svc.submit(TaskRecord(task_id="t1", device_id="robot-1"))
    with pytest.raises(RequestRejected) as ei:
        svc.cancel("t1")
    assert ei.value.reason == RejectReason.TASK_ALREADY_FINISHED


def test_cancel_unknown_task_rejected():
    svc = build()
    with pytest.raises(RequestRejected) as ei:
        svc.cancel("ghost")
    assert ei.value.reason == RejectReason.UNKNOWN_TASK


def test_cancel_waiting_retry_task():
    svc = build({"t1": ["timeout", "timeout", "timeout"]})
    t = svc.submit(TaskRecord(task_id="t1", device_id="robot-1", backoff_seconds=10.0))
    assert t.status == TaskStatus.WAITING_RETRY
    cancelled = svc.cancel("t1")
    assert cancelled.status == TaskStatus.CANCELLED
    # 取消后 pump 不得再推进该任务
    svc._store.get_task("t1").next_retry_at = 0
    svc.pump()
    assert svc.get("t1").status == TaskStatus.CANCELLED


def test_illegal_transition_rejected():
    svc = build()
    t = svc.submit(TaskRecord(task_id="t1", device_id="robot-1"))
    with pytest.raises(RequestRejected) as ei:
        svc._transition(t, TaskStatus.RUNNING, "非法跳转")
    assert ei.value.reason == RejectReason.ILLEGAL_TRANSITION
