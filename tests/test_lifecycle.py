"""任务下发、幂等、查询、取消与状态流转合法性。"""

import pytest

from orchestrator import ErrorReason, MissionStatus, OrchestratorError
from tests.conftest import make_orchestrator, mission, wait_for


def test_submit_idempotent_same_content(clock, driver):
    orch = make_orchestrator(clock, driver)
    m1 = orch.submit(mission("m1", payload="a"))
    wait_for(lambda: m1.status is MissionStatus.SUCCEEDED)
    m2 = orch.submit(mission("m1", payload="a"))
    assert m2 is m1
    assert len(orch.list_missions()) == 1
    assert driver.calls == ["m1"]  # 未重复执行


def test_submit_same_id_different_content_rejected(clock, driver):
    orch = make_orchestrator(clock, driver)
    orch.submit(mission("m1", payload="a"))
    with pytest.raises(OrchestratorError) as ei:
        orch.submit(mission("m1", payload="b"))
    assert ei.value.reason is ErrorReason.DUPLICATE_MISSION_CONFLICT


def test_submit_unknown_device_rejected(clock, driver):
    orch = make_orchestrator(clock, driver)
    with pytest.raises(OrchestratorError) as ei:
        orch.submit(mission("m1", device="ghost"))
    assert ei.value.reason is ErrorReason.DEVICE_NOT_FOUND


def test_get_unknown_mission(clock, driver):
    orch = make_orchestrator(clock, driver)
    with pytest.raises(OrchestratorError) as ei:
        orch.get("nope")
    assert ei.value.reason is ErrorReason.MISSION_NOT_FOUND


def test_cancel_queued_mission(clock, driver):
    orch = make_orchestrator(clock, driver)
    gate = driver.gate("m1")  # m1 占住设备
    orch.submit(mission("m1"))
    wait_for(lambda: driver.started["m1"].is_set())
    m2 = orch.submit(mission("m2"))  # m2 排队
    assert m2.status is MissionStatus.QUEUED
    orch.cancel("m2")
    assert m2.status is MissionStatus.CANCELED
    assert m2.error_reason == "CANCELED_BY_USER"
    gate.set()
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    assert driver.calls == ["m1"]  # m2 从未执行


def test_cancel_running_mission_discards_late_result(clock, driver):
    orch = make_orchestrator(clock, driver)
    gate = driver.gate("m1")
    orch.submit(mission("m1"))
    wait_for(lambda: driver.started["m1"].is_set())
    orch.cancel("m1")
    gate.set()  # 执行线程随后返回成功，但结果应被丢弃
    wait_for(lambda: len(driver.calls) == 1)
    wait_for(lambda: any("迟到结果被丢弃" in line for line in orch.decision_log()))
    m = orch.get("m1")
    assert m.status is MissionStatus.CANCELED
    assert m.result is None


def test_cancel_finished_mission_rejected(clock, driver):
    orch = make_orchestrator(clock, driver)
    orch.submit(mission("m1"))
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    with pytest.raises(OrchestratorError) as ei:
        orch.cancel("m1")
    assert ei.value.reason is ErrorReason.MISSION_ALREADY_FINISHED


def test_manual_retry_only_on_failed(clock, driver):
    orch = make_orchestrator(clock, driver)
    orch.submit(mission("m1"))
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    with pytest.raises(OrchestratorError) as ei:
        orch.retry("m1")
    assert ei.value.reason is ErrorReason.ILLEGAL_STATE_TRANSITION


def test_decision_log_records_transitions(clock, driver):
    orch = make_orchestrator(clock, driver)
    orch.submit(mission("m1"))
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    log = "\n".join(orch.decision_log())
    assert "PENDING -> QUEUED" in log
    assert "QUEUED -> RUNNING" in log
    assert "RUNNING -> SUCCEEDED" in log
