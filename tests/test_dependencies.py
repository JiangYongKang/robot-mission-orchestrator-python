"""任务依赖编排：依赖链、级联终结、环拒绝、与并发/资源叠加。"""
import threading
import time

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


def make(task_id, device="robot-1", resources=(), deps=(), backoff=0.0, max_attempts=3):
    return TaskRecord(
        task_id=task_id,
        device_id=device,
        required_resources=tuple(resources),
        depends_on=tuple(deps),
        backoff_seconds=backoff,
        max_attempts=max_attempts,
    )


def build(fault_plan=None, devices=(("robot-1", 1),), resources=(("dock-a", 1),)):
    svc = OrchestratorService(fault_plan=fault_plan)
    for dev_id, cap in devices:
        svc._store.add_device(Device(dev_id, max_concurrency=cap))
    for res_id, cap in resources:
        svc._store.add_resource(Resource(res_id, capacity=cap))
    return svc


# --- 正常依赖链 ---

def test_dependency_chain_runs_in_order(service):
    """先测绘、再巡检、最后回充：反序提交也按依赖顺序执行。"""
    service.submit(make("recharge", deps=("inspect",)))
    service.submit(make("inspect", deps=("survey",)))
    service.submit(make("survey"))

    for tid in ("survey", "inspect", "recharge"):
        assert service.get(tid).status == TaskStatus.SUCCEEDED

    # 等待状态可查询：历史上经历过 WAITING_DEPS，且记录了在等谁
    inspect = service.get("inspect")
    assert any(h[1] == TaskStatus.WAITING_DEPS for h in inspect.history)
    assert any("survey" in h[2] for h in inspect.history)
    # 执行顺序：survey 先成功，inspect 才开始第一次尝试
    assert service.get("survey").result["attempts"] == 1
    assert inspect.result["attempts"] == 1


def test_waiting_state_is_queryable(service):
    """前置未完成时停在 WAITING_DEPS，查询能说明在等谁、为什么。"""
    svc = build({"a": ["timeout", "ok"]})
    a = svc.submit(make("a", backoff=10.0))  # 长退避 -> WAITING_RETRY
    assert a.status == TaskStatus.WAITING_RETRY
    b = svc.submit(make("b", deps=("a",)))

    assert b.status == TaskStatus.WAITING_DEPS
    view = svc.dependency_view("b")
    assert view == [
        {"task_id": "a", "status": "WAITING_RETRY", "note": "前置未成功完成"}
    ]
    # 与设备/资源排队不混：WAITING_DEPS 不是 QUEUED
    assert b.status != TaskStatus.QUEUED

    # 前置恢复成功后，b 自动进入调度并完成
    a.next_retry_at = time.monotonic() - 1
    svc.pump()
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    assert svc.get("b").status == TaskStatus.SUCCEEDED


def test_forward_reference_waits_until_submitted(service):
    """允许引用尚未下发的前置：先停在 WAITING_DEPS，前置下发并成功后放行。"""
    svc = build()
    b = svc.submit(make("b", deps=("a",)))
    assert b.status == TaskStatus.WAITING_DEPS
    assert svc.dependency_view("b")[0]["status"] == "MISSING"

    svc.submit(make("a"))
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    assert svc.get("b").status == TaskStatus.SUCCEEDED


# --- 级联终结 ---

def test_dependency_failure_cascades_transitively(service):
    """前置失败 -> 下游级联 FAILED/DEPENDENCY_FAILED，且沿链传递、从未执行。"""
    svc = build({"a": ["reject", "reject", "reject"]})
    svc.submit(make("c", deps=("b",)))
    svc.submit(make("b", deps=("a",)))
    a = svc.submit(make("a"))

    assert a.status == TaskStatus.FAILED
    assert a.failure_reason == FailureReason.DEVICE_REJECTED  # 自己执行被拒
    for tid in ("b", "c"):
        t = svc.get(tid)
        assert t.status == TaskStatus.FAILED
        assert t.failure_reason == FailureReason.DEPENDENCY_FAILED  # 前置没成功
        assert t.attempts == 0  # 根本没执行
    # 级联原因与自身执行失败可区分
    assert svc.get("b").failure_reason != svc.get("a").failure_reason


def test_dependency_cancel_cascades_by_standing_rule(service):
    """前置被取消后，下游不会一直挂：下一次推进按既定规则级联 CANCELLED。"""
    svc = build({"a": ["timeout", "timeout", "timeout"]})
    a = svc.submit(make("a", backoff=10.0))
    assert a.status == TaskStatus.WAITING_RETRY
    svc.submit(make("b", deps=("a",)))

    svc.cancel("a")  # 不带 cascade：本次调用只取消 a
    assert svc.get("b").status == TaskStatus.WAITING_DEPS  # 尚未被带走

    svc.pump()  # 既定规则：前置已取消 -> 级联终结
    assert svc.get("b").status == TaskStatus.CANCELLED
    assert svc.get("b").failure_reason == FailureReason.DEPENDENCY_CANCELLED
    assert svc.get("b").attempts == 0


def test_cancel_with_cascade_takes_downstream_together(service):
    """cancel(cascade=True)：同一调用里把全部下游（传递）一并取消。"""
    svc = build({"a": ["timeout", "timeout", "timeout"]})
    svc.submit(make("a", backoff=10.0))
    svc.submit(make("b", deps=("a",)))
    svc.submit(make("c", deps=("b",)))

    svc.cancel("a", cascade=True)
    assert svc.get("a").failure_reason == FailureReason.CANCELLED_BY_USER
    for tid in ("b", "c"):
        t = svc.get(tid)
        assert t.status == TaskStatus.CANCELLED
        assert t.failure_reason == FailureReason.DEPENDENCY_CANCELLED


def test_intervene_abort_with_cascade(service):
    """人工处置 abort 也可按需连带下游。"""
    svc = build({"a": ["timeout", "timeout", "timeout"]})
    svc.submit(make("a", backoff=10.0))
    svc.submit(make("b", deps=("a",)))

    svc.intervene("a", "abort", cascade=True)
    assert svc.get("a").status == TaskStatus.CANCELLED
    assert svc.get("b").status == TaskStatus.CANCELLED
    assert svc.get("b").failure_reason == FailureReason.DEPENDENCY_CANCELLED


def test_intervene_retry_revives_cascade_failed_task(service):
    """前置人工重试成功后，被级联终结的下游可人工 retry 恢复执行。"""
    svc = build({"a": ["reject", "reject", "reject", "ok"]})
    svc.submit(make("b", deps=("a",)))
    svc.submit(make("a"))
    assert svc.get("b").status == TaskStatus.FAILED
    assert svc.get("b").failure_reason == FailureReason.DEPENDENCY_FAILED

    svc.intervene("a", "retry")
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    b = svc.intervene("b", "retry")  # 前置已就绪 -> 重新进入调度
    assert b.status == TaskStatus.SUCCEEDED
    assert b.result["attempts"] == 1


# --- 环与自依赖 ---

def test_self_dependency_rejected(service):
    svc = build()
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("a", deps=("a",)))
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE
    assert svc._store.get_task("a") is None  # 未落库


def test_two_task_cycle_rejected(service):
    svc = build()
    svc.submit(make("a", deps=("b",)))  # 前向引用，允许
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("b", deps=("a",)))
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE
    assert svc._store.get_task("b") is None


def test_three_task_cycle_rejected(service):
    svc = build()
    svc.submit(make("a", deps=("c",)))
    svc.submit(make("b", deps=("a",)))
    with pytest.raises(RequestRejected) as ei:
        svc.submit(make("c", deps=("b",)))
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE


# --- 依赖与并发/资源叠加 ---

def test_waiting_deps_do_not_block_device_or_fifo(service):
    """等前置的任务不占设备槽、不阻塞后续无依赖任务的 FIFO 调度。"""
    svc = build({"t1": ["timeout", "ok"]})
    t1 = svc.submit(make("t1", backoff=10.0))  # 占槽失败后退避，释放槽
    assert t1.status == TaskStatus.WAITING_RETRY
    t2 = svc.submit(make("t2", deps=("t1",)))   # 等前置，不参与排队
    t3 = svc.submit(make("t3"))                 # 无依赖，应直接运行

    assert t2.status == TaskStatus.WAITING_DEPS
    assert t3.status == TaskStatus.SUCCEEDED  # 不被 t2 挡住
    assert svc._store.get_device("robot-1").running == 0  # 无超额占用

    t1.next_retry_at = time.monotonic() - 1
    svc.pump()
    assert svc.get("t1").status == TaskStatus.SUCCEEDED
    assert svc.get("t2").status == TaskStatus.SUCCEEDED


def test_ready_tasks_still_respect_device_cap_fifo(service):
    """依赖就绪后仍受设备并发上限约束，且按提交序补位。"""
    svc = build({"head": ["timeout", "ok"]})
    head = svc.submit(make("head", backoff=10.0))
    assert head.status == TaskStatus.WAITING_RETRY
    svc.submit(make("w1", deps=("head",)))
    svc.submit(make("w2", deps=("head",)))

    head.next_retry_at = time.monotonic() - 1
    svc.pump()  # head 成功后 w1/w2 就绪，但设备上限 1：先 w1 后 w2
    assert svc.get("head").status == TaskStatus.SUCCEEDED
    assert svc.get("w1").status == TaskStatus.SUCCEEDED
    assert svc.get("w2").status == TaskStatus.SUCCEEDED
    # w1 先于 w2 进入 RUNNING（FIFO）
    w1_run = next(h for h in svc.get("w1").history if h[1] == TaskStatus.RUNNING)
    w2_run = next(h for h in svc.get("w2").history if h[1] == TaskStatus.RUNNING)
    assert svc.get("w1").created_seq < svc.get("w2").created_seq
    assert w1_run is not None and w2_run is not None


def test_dependency_with_resource_mutex(service):
    """依赖 + 资源互斥叠加：等前置的任务不参与资源争抢，就绪后仍守互斥。"""
    svc = build({"a": ["timeout", "ok"]})
    a = svc.submit(make("a", resources=("dock-a",), backoff=10.0))
    assert a.status == TaskStatus.WAITING_RETRY  # 资源已释放
    b = svc.submit(make("b", resources=("dock-a",), deps=("a",)))
    c = svc.submit(make("c", resources=("dock-a",)))

    assert b.status == TaskStatus.WAITING_DEPS  # 不与 c 抢资源
    assert c.status == TaskStatus.SUCCEEDED

    a.next_retry_at = time.monotonic() - 1
    svc.pump()
    assert svc.get("a").status == TaskStatus.SUCCEEDED
    assert svc.get("b").status == TaskStatus.SUCCEEDED
    assert svc._store.get_resource("dock-a").in_use == 0  # 全程不超额


# --- 幂等 ---

def test_duplicate_submit_with_deps_idempotent(service):
    svc = build()
    a = svc.submit(make("a", deps=("missing-dep",)))
    b = svc.submit(make("a", deps=("missing-dep",)))
    assert a is b
    assert a.status == TaskStatus.WAITING_DEPS
    assert len(svc._store.list_tasks()) == 1


def test_concurrent_submit_with_deps_single_record(service):
    svc = build()
    svc.submit(make("dep"))
    results = []

    def worker():
        results.append(svc.submit(make("t-dup", deps=("dep",))))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len({id(r) for r in results}) == 1
    assert results[0].status == TaskStatus.SUCCEEDED
    assert results[0].attempts == 1
