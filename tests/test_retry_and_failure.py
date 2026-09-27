"""中途失败：重试受次数与退避约束，失败原因可区分，部分成功不丢结果。"""
import time

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


def make(task_id, backoff=0.0, max_attempts=3):
    return TaskRecord(
        task_id=task_id,
        device_id="robot-1",
        backoff_seconds=backoff,
        max_attempts=max_attempts,
    )


def build(fault_plan):
    svc = OrchestratorService(fault_plan=fault_plan)
    svc._store.add_device(Device("robot-1", max_concurrency=1))
    return svc


def test_retry_then_success_no_duplicate_result():
    """前两次失败第三次成功：结果保留且只记录一次最终结果。"""
    svc = build({"t1": ["reject", "timeout", "ok"]})
    t = svc.submit(make("t1"))
    assert t.status == TaskStatus.SUCCEEDED
    assert t.attempts == 3
    assert t.result == {"output": "done-by-robot-1", "attempts": 3}


def test_retries_exhausted_failed_with_reason():
    svc = build({"t1": ["reject", "reject", "reject"]})
    t = svc.submit(make("t1"))
    assert t.status == TaskStatus.FAILED
    assert t.failure_reason == FailureReason.DEVICE_REJECTED
    assert t.attempts == 3  # 不超过 max_attempts


def test_failure_reasons_distinguishable():
    svc = build({
        "t-reject": ["reject"],
        "t-timeout": ["timeout"],
        "t-error": ["error"],
    })
    reasons = {}
    for tid in ("t-reject", "t-timeout", "t-error"):
        t = svc.submit(make(tid, max_attempts=1))
        reasons[tid] = t.failure_reason
    assert reasons["t-reject"] == FailureReason.DEVICE_REJECTED
    assert reasons["t-timeout"] == FailureReason.TIMEOUT
    assert reasons["t-error"] == FailureReason.INTERNAL_ERROR


def test_backoff_defers_retry():
    """退避未到期时任务停留在 WAITING_RETRY，到期后才重新执行。"""
    svc = build({"t1": ["timeout", "ok"]})
    t = svc.submit(make("t1", backoff=10.0))  # 长退避：不会立刻重试
    assert t.status == TaskStatus.WAITING_RETRY
    assert t.attempts == 1
    # 人为把退避时间拨到过去，模拟到期
    t.next_retry_at = time.monotonic() - 1
    svc.pump()
    assert t.status == TaskStatus.SUCCEEDED
    assert t.attempts == 2


def test_manual_intervene_retry_after_failed():
    """重试耗尽失败后，人工干预 retry 可恢复并最终成功。"""
    svc = build({"t1": ["error", "error", "error", "ok"]})
    t = svc.submit(make("t1"))
    assert t.status == TaskStatus.FAILED
    t = svc.intervene("t1", "retry")
    assert t.status == TaskStatus.SUCCEEDED
    assert t.result["attempts"] == 1  # 人工重试后计数重置


def test_intervene_retry_on_non_failed_rejected():
    svc = build({})
    t = svc.submit(make("t1"))
    assert t.status == TaskStatus.SUCCEEDED
    with pytest.raises(RequestRejected) as ei:
        svc.intervene("t1", "retry")
    assert ei.value.reason == RejectReason.ILLEGAL_TRANSITION
