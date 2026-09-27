"""失败重试：次数与退避约束、原因可区分、人工干预、历史不丢失。"""

from orchestrator import ExecutionOutcome, MissionStatus, OutcomeKind
from tests.conftest import make_orchestrator, mission, wait_for


def ok(detail="ok"):
    return ExecutionOutcome(OutcomeKind.SUCCESS, detail)


def fail(kind, detail):
    return ExecutionOutcome(kind, detail)


def test_retry_with_backoff_then_success(clock, driver):
    orch = make_orchestrator(clock, driver, base_backoff=1.0)
    driver.script("m1", [fail(OutcomeKind.DEVICE_REJECTED, "busy"), ok("done")])
    orch.submit(mission("m1", max_retries=2))
    # 第一次尝试失败后重新排队，退避 1.0s（假时钟未推进，不得重跑）
    wait_for(lambda: orch.get("m1").status is MissionStatus.QUEUED)
    assert len(driver.calls) == 1
    clock.advance(0.5)
    orch.dispatch()
    assert len(driver.calls) == 1  # 退避未满，仍不重跑
    clock.advance(0.6)
    orch.dispatch()
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    m = orch.get("m1")
    assert m.result == "done"
    assert m.retries_used == 1
    # 部分失败后成功：两次尝试的历史都保留
    assert [a.outcome for a in m.attempts] == [
        OutcomeKind.DEVICE_REJECTED, OutcomeKind.SUCCESS,
    ]
    log = "\n".join(orch.decision_log())
    assert "退避 1.000s" in log


def test_retry_exhausted_marks_failed_with_reason(clock, driver):
    orch = make_orchestrator(clock, driver, base_backoff=1.0)
    driver.script("m1", [
        fail(OutcomeKind.TIMEOUT, "t1"),
        fail(OutcomeKind.TIMEOUT, "t2"),
    ])
    orch.submit(mission("m1", max_retries=1))
    wait_for(lambda: orch.get("m1").status is MissionStatus.QUEUED)
    clock.advance(1.0)
    orch.dispatch()
    wait_for(lambda: orch.get("m1").status is MissionStatus.FAILED)
    m = orch.get("m1")
    assert m.error_reason == "DEVICE_TIMEOUT"  # 超时原因可区分
    assert m.result is None
    assert len(m.attempts) == 2  # 初次 + 1 次重试，不多不少


def test_failure_reasons_are_distinguishable(clock, driver):
    orch = make_orchestrator(clock, driver)
    driver.script("m-reject", [fail(OutcomeKind.DEVICE_REJECTED, "refused")])
    driver.script("m-timeout", [fail(OutcomeKind.TIMEOUT, "too slow")])
    driver.script("m-internal", [fail(OutcomeKind.INTERNAL_ERROR, "boom")])
    for mid in ("m-reject", "m-timeout", "m-internal"):
        orch.submit(mission(mid))
        wait_for(lambda mid=mid: orch.get(mid).status is MissionStatus.FAILED)
        clock.advance(10)  # 让出可能的重试窗口（本例 max_retries=0）
    assert orch.get("m-reject").error_reason == "DEVICE_REJECTED"
    assert orch.get("m-timeout").error_reason == "DEVICE_TIMEOUT"
    assert orch.get("m-internal").error_reason == "INTERNAL_ERROR"


def test_driver_exception_counts_as_internal_error(clock, driver):
    def boom(m):
        raise RuntimeError("adapter crashed")
    orch = make_orchestrator(clock, driver)
    orch._driver = boom
    orch.submit(mission("m1"))
    wait_for(lambda: orch.get("m1").status is MissionStatus.FAILED)
    assert orch.get("m1").error_reason == "INTERNAL_ERROR"


def test_manual_retry_after_exhaustion(clock, driver):
    orch = make_orchestrator(clock, driver, base_backoff=1.0)
    driver.script("m1", [fail(OutcomeKind.DEVICE_REJECTED, "busy"), ok("recovered")])
    orch.submit(mission("m1"))  # max_retries=0，直接失败
    wait_for(lambda: orch.get("m1").status is MissionStatus.FAILED)
    assert orch.get("m1").error_reason == "DEVICE_REJECTED"
    orch.retry("m1")  # 人工干预
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    m = orch.get("m1")
    assert m.result == "recovered"
    assert m.error_reason is None
    assert len(m.attempts) == 2  # 失败历史未丢失


def test_no_duplicate_execution_after_partial_failure(clock, driver):
    """部分成功后失败：重试只执行未完成的新尝试，历史按序追加。"""
    orch = make_orchestrator(clock, driver, base_backoff=1.0)
    driver.script("m1", [
        fail(OutcomeKind.TIMEOUT, "flaky"),
        fail(OutcomeKind.TIMEOUT, "flaky again"),
        ok("final"),
    ])
    orch.submit(mission("m1", max_retries=3))
    for _ in range(3):
        wait_for(lambda: orch.get("m1").status in (
            MissionStatus.QUEUED, MissionStatus.SUCCEEDED,
        ))
        clock.advance(5.0)
        orch.dispatch()
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    assert driver.calls == ["m1", "m1", "m1"]  # 每次只跑一次，无并发重复
    assert [a.index for a in orch.get("m1").attempts] == [1, 2, 3]
