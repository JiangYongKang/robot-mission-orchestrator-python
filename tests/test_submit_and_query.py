"""重复提交幂等、查询与非法流转。"""
import threading

import pytest

from orchestrator.errors import RequestRejected
from orchestrator.models import RejectReason, TaskRecord, TaskStatus


def make(task_id, device="robot-1", resources=(), backoff=0.0):
    return TaskRecord(
        task_id=task_id,
        device_id=device,
        required_resources=tuple(resources),
        backoff_seconds=backoff,
    )


def test_submit_success(service):
    t = service.submit(make("t1"))
    assert t.status == TaskStatus.SUCCEEDED
    assert t.result["attempts"] == 1


def test_duplicate_submit_is_idempotent(service):
    a = service.submit(make("t1"))
    b = service.submit(make("t1"))
    assert a is b  # 返回同一条记录
    assert a.attempts == 1  # 未重复执行
    assert len([t for t in service._store.list_tasks()]) == 1


def test_concurrent_duplicate_submit_single_execution(service):
    results = []

    def worker():
        results.append(service.submit(make("t-dup")))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len({id(r) for r in results}) == 1  # 同一记录对象
    assert results[0].attempts == 1
    assert results[0].status == TaskStatus.SUCCEEDED


def test_get_unknown_task_rejected(service):
    with pytest.raises(RequestRejected) as ei:
        service.get("nope")
    assert ei.value.reason == RejectReason.UNKNOWN_TASK


def test_submit_unknown_device_rejected(service):
    with pytest.raises(RequestRejected) as ei:
        service.submit(make("t-x", device="ghost"))
    assert ei.value.reason == RejectReason.UNKNOWN_DEVICE


def test_submit_unknown_resource_rejected(service):
    with pytest.raises(RequestRejected) as ei:
        service.submit(make("t-x", resources=("ghost-res",)))
    assert ei.value.reason == RejectReason.UNKNOWN_RESOURCE
