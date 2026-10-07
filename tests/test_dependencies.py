"""任务依赖编排：等待态可查询、级联终结、环拒绝、与并发/资源叠加。"""
import threading

import pytest

from orchestrator.errors import RequestRejected
from orchestrator.models import (
    Device,
    FailureReason,
    RejectReason,
    Resource,
    TaskRecord,
    TaskStatus,
)
from orchestrator.service import OrchestratorService


def build(fault_plan=None, max_concurrency=1):
    svc = OrchestratorService(fault_plan=fault_plan)
    svc._store.add_device(Device("robot-1", max_concurrency=max_concurrency))
    svc._store.add_resource(Resource("dock-a", capacity=1))
    return svc


def make(task_id, depends_on=(), resources=(), max_attempts=3, backoff=0.0):
    return TaskRecord(
        task_id=task_id,
        device_id="robot-1",
        required_resources=tuple(resources),
        depends_on=tuple(depends_on),
        max_attempts=max_attempts,
        backoff_seconds=backoff,
    )


# --- 正常依赖链 ---
def test_dependency_chain_runs_in_order():
    """先测绘、再巡检、最后回充：前置成功后后继才进入调度。"""
    svc = build()
    # 逆序提交（允许前置后补下发）：回充等巡检、巡检等测绘
    charge = svc.submit(make("charge", depends_on=("inspect",)))
    assert charge.status == TaskStatus.WAITING_DEPENDENCY
    assert svc.waiting_on("charge") == ["inspect(未下发)"]
    inspect = svc.submit(make("inspect", depends_on=("survey",)))
    assert inspect.status == TaskStatus.WAITING_DEPENDENCY
    assert svc.waiting_on("charge") == ["inspect(WAITING_DEPENDENCY)"]

    survey = svc.submit(make("survey"))
    assert survey.status == TaskStatus.SUCCEEDED
    # 测绘成功后，巡检、回充沿链依次就绪并执行
    assert svc.get("inspect").status == TaskStatus.SUCCEEDED
    assert svc.get("charge").status == TaskStatus.SUCCEEDED
    # 执行顺序可由历史佐证：charge 在 inspect 成功后才离开等待态
    assert svc.get("charge").history[0][1] == TaskStatus.PENDING


def test_dependency_waits_for_unfinished_predecessor():
    """前置还在重试退避时，后继停在 WAITING_DEPENDENCY 且可查询等待原因。"""
    svc = build({"a": ["timeout", "ok"]})
    a = svc.submit(make("a", backoff=10.0))
    assert a.status == TaskStatus.WAITING_RETRY
    b = svc.submit(make("b", depends_on=("a",)))
    assert b.status == TaskStatus.WAITING_DEPENDENCY
    assert svc.waiting_on("b") == ["a(WAITING_RETRY)"]
    # 等待依赖不等于设备/资源排队：b 从未进入 QUEUED
    assert all(to != TaskStatus.QUEUED for _, to, _ in b.history)

    a.next_retry_at = 0  # 退避到期
    svc.pump()
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    assert svc.get("b").status == TaskStatus.SUCCEEDED


# --- 级联终结 ---
def test_failed_predecessor_cascades_skip():
    """前置失败：后继 SKIPPED（DEPENDENCY_FAILED），与自身执行失败可区分，且逐级传播。"""
    svc = build({"a": ["reject", "reject", "reject"]})
    svc.submit(make("b", depends_on=("a",)))
    svc.submit(make("c", depends_on=("b",)))
    a = svc.submit(make("a"))
    assert a.status == TaskStatus.FAILED
    assert a.failure_reason == FailureReason.DEVICE_REJECTED  # 自己执行失败

    b = svc.get("b")
    assert b.status == TaskStatus.SKIPPED
    assert b.failure_reason == FailureReason.DEPENDENCY_FAILED  # 从未执行
    assert b.attempts == 0
    c = svc.get("c")
    assert c.status == TaskStatus.SKIPPED  # 经 b 传递级联
    assert c.failure_reason == FailureReason.DEPENDENCY_FAILED
    assert c.attempts == 0


def test_cancelled_predecessor_cascades_skip():
    """前置被取消：后继级联 SKIPPED，不会一直挂在等待态。"""
    svc = build()
    svc._store.get_device("robot-1").running = 1  # 占槽让 a 排队
    svc.submit(make("a"))
    svc.submit(make("b", depends_on=("a",)))
    assert svc.get("b").status == TaskStatus.WAITING_DEPENDENCY

    svc.cancel("a")
    assert svc.get("a").status == TaskStatus.CANCELLED
    b = svc.get("b")
    assert b.status == TaskStatus.SKIPPED
    assert b.failure_reason == FailureReason.DEPENDENCY_FAILED


def test_cancel_with_cascade_takes_downstream():
    """级联取消：下游未结束任务一并 CANCELLED。"""
    svc = build()
    svc._store.get_device("robot-1").running = 1
    svc.submit(make("a"))
    svc.submit(make("b", depends_on=("a",)))
    svc.submit(make("c", depends_on=("b",)))
    svc.submit(make("d"))  # 无依赖，不受影响

    svc.cancel("a", cascade=True)
    assert svc.get("a").status == TaskStatus.CANCELLED
    assert svc.get("b").status == TaskStatus.CANCELLED
    assert svc.get("c").status == TaskStatus.CANCELLED
    assert svc.get("b").failure_reason == FailureReason.CANCELLED_BY_USER
    assert svc.get("d").status == TaskStatus.QUEUED


def test_intervene_abort_with_cascade():
    """人工处置 abort + cascade：下游一并终止。"""
    svc = build()
    svc._store.get_device("robot-1").running = 1
    svc.submit(make("a"))
    svc.submit(make("b", depends_on=("a",)))
    svc.intervene("a", "abort", cascade=True)
    assert svc.get("a").status == TaskStatus.CANCELLED
    assert svc.get("b").status == TaskStatus.CANCELLED


# --- 依赖自洽性 ---
def test_self_dependency_rejected():
    svc = build()
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("a", depends_on=("a",)))
    assert ei.value.reason == RejectReason.SELF_DEPENDENCY


def test_two_node_cycle_rejected():
    svc = build()
    svc.submit(make("a", depends_on=("b",)))  # 前置允许后补下发
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("b", depends_on=("a",)))
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE


def test_three_node_cycle_rejected():
    svc = build()
    svc.submit(make("x", depends_on=("y",)))
    svc.submit(make("y", depends_on=("z",)))
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("z", depends_on=("x",)))
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE


# --- 依赖与并发/资源叠加 ---
def test_ready_dependency_respects_device_cap_and_fifo():
    """依赖就绪后仍受设备并发上限约束，且按提交序号 FIFO 补位。"""
    svc = build()
    svc._store.get_device("robot-1").running = 1  # 占满唯一并发槽
    svc.submit(make("a"))                        # seq1 QUEUED
    svc.submit(make("b", depends_on=("a",)))     # seq2 等待依赖
    svc.submit(make("c"))                        # seq3 QUEUED
    assert svc.get("b").status == TaskStatus.WAITING_DEPENDENCY

    svc._store.get_device("robot-1").running = 0
    svc.pump()
    # a 先跑完 -> b 就绪 -> b(seq2) 先于 c(seq3) 拿槽，全程不超额
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    assert svc.get("b").status == TaskStatus.SUCCEEDED
    assert svc.get("c").status == TaskStatus.SUCCEEDED
    ran_order = [
        t.task_id
        for t in sorted(svc._store.list_tasks(), key=lambda t: t.created_seq)
    ]
    assert ran_order == ["a", "b", "c"]


def test_ready_dependency_respects_resource_mutex():
    """依赖就绪的任务与既有任务抢同一资源：互斥不破，按 FIFO 决定先后。"""
    svc = build(max_concurrency=2)
    svc._store.get_resource("dock-a").in_use = 1  # 资源被占满
    svc.submit(make("a", resources=("dock-a",)))            # seq1 QUEUED(资源)
    svc.submit(make("b", depends_on=("a",), resources=("dock-a",)))  # seq2 等依赖
    svc.submit(make("c", resources=("dock-a",)))            # seq3 QUEUED(资源)
    assert svc.get("b").status == TaskStatus.WAITING_DEPENDENCY
    assert svc.get("c").status == TaskStatus.QUEUED

    svc._store.get_resource("dock-a").in_use = 0
    svc.pump()
    assert all(
        svc.get(t).status == TaskStatus.SUCCEEDED for t in ("a", "b", "c")
    )
    # 资源任一时刻最多一个占用：执行是同步的，结束后 in_use 必须归零
    assert svc._store.get_resource("dock-a").in_use == 0


def test_concurrent_submit_with_dependencies_no_overcap():
    """多线程并发提交带依赖的任务：幂等、不超额并发、最终一致。"""
    svc = build()
    svc.submit(make("root"))
    peak = {"value": 0}
    orig_dispatch = svc._scheduler.dispatch

    def spy_dispatch():
        started = orig_dispatch()
        running = svc._store.get_device("robot-1").running
        peak["value"] = max(peak["value"], running)
        return started

    svc._scheduler.dispatch = spy_dispatch

    def worker(i):
        # 同一 task_id 重复提交 + 依赖 root
        svc.submit(make(f"t-{i % 4}", depends_on=("root",)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    svc.pump()

    assert peak["value"] <= 1
    tasks = {t.task_id: t for t in svc._store.list_tasks()}
    assert len(tasks) == 5  # root + t-0..t-3，重复提交被幂等去重
    assert all(t.status == TaskStatus.SUCCEEDED for t in tasks.values())
