"""并发抢用：设备并发上限与资源互斥，结果确定且可复现。"""
import threading

from orchestrator.models import Device, Resource, TaskRecord, TaskStatus
from orchestrator.scheduler import Scheduler
from orchestrator.service import OrchestratorService
from orchestrator.store import StateStore


def make(task_id, device="robot-1", resources=()):
    return TaskRecord(
        task_id=task_id, device_id=device, required_resources=tuple(resources)
    )


def build_store():
    store = StateStore()
    store.add_device(Device("robot-1", max_concurrency=1))
    store.add_resource(Resource("dock-a", capacity=1))
    return store


def test_device_concurrency_cap_fifo():
    """设备上限 1：三个任务同时争抢，只有最早提交的运行，其余按 FIFO 排队。"""
    store = build_store()
    sched = Scheduler(store)
    for i, tid in enumerate(["t1", "t2", "t3"], start=1):
        t = make(tid)
        t.created_seq = i
        store.put_task(t)

    started = sched.dispatch()
    assert [t.task_id for t in started] == ["t1"]
    assert store.get_task("t2").status == TaskStatus.QUEUED
    assert store.get_task("t3").status == TaskStatus.QUEUED

    # t1 释放后，FIFO 保证 t2 先于 t3
    store.get_device("robot-1").running = 0
    store.get_task("t1").status = TaskStatus.SUCCEEDED
    started = sched.dispatch()
    assert [t.task_id for t in started] == ["t2"]


def test_resource_mutex():
    """资源容量 1：两个任务抢同一资源，先到先得，另一任务排队。"""
    store = build_store()
    store.add_device(Device("robot-2", max_concurrency=2))
    sched = Scheduler(store)
    for i, (tid, dev) in enumerate([("t1", "robot-1"), ("t2", "robot-2")], start=1):
        t = make(tid, device=dev, resources=("dock-a",))
        t.created_seq = i
        store.put_task(t)

    started = sched.dispatch()
    assert [t.task_id for t in started] == ["t1"]
    assert store.get_task("t2").status == TaskStatus.QUEUED
    assert store.get_resource("dock-a").in_use == 1


def test_concurrent_submit_never_exceeds_cap():
    """多线程并发提交：全程不出现超额并发，最终结果一致。"""
    svc = OrchestratorService()
    svc._store.add_device(Device("robot-1", max_concurrency=1))
    peak = {"value": 0}
    orig_dispatch = svc._scheduler.dispatch

    def spy_dispatch():
        started = orig_dispatch()
        running = svc._store.get_device("robot-1").running
        peak["value"] = max(peak["value"], running)
        return started

    svc._scheduler.dispatch = spy_dispatch

    def worker(i):
        svc.submit(make(f"t-{i}"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    svc.pump()  # 收尾推进
    assert peak["value"] <= 1
    statuses = {t.task_id: t.status for t in svc._store.list_tasks()}
    assert all(s == TaskStatus.SUCCEEDED for s in statuses.values())
    assert len(statuses) == 10
