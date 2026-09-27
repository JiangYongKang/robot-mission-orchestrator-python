"""设备并发上限与资源互斥：不得超额并发，排队顺序确定可复现。"""

from orchestrator import Device, MissionStatus
from tests.conftest import make_orchestrator, mission, wait_for


def test_device_concurrency_limit_fifo(clock, driver):
    orch = make_orchestrator(clock, driver, devices=[Device("robot-1", max_concurrency=1)])
    gate1 = driver.gate("m1")
    orch.submit(mission("m1"))
    orch.submit(mission("m2"))
    orch.submit(mission("m3"))
    wait_for(lambda: driver.started["m1"].is_set())
    # m1 独占设备，m2/m3 必须排队，设备上只有一个在跑
    assert orch.get("m1").status is MissionStatus.RUNNING
    assert orch.get("m2").status is MissionStatus.QUEUED
    assert orch.get("m3").status is MissionStatus.QUEUED
    gate1.set()
    # FIFO：m1 完成后必须先放行 m2 而不是 m3
    wait_for(lambda: orch.get("m2").status is not MissionStatus.QUEUED)
    wait_for(lambda: orch.get("m1").status is MissionStatus.SUCCEEDED)
    wait_for(lambda: orch.get("m3").status is MissionStatus.SUCCEEDED)
    assert driver.calls[0] == "m1"
    assert driver.calls.index("m2") < driver.calls.index("m3")
    log = "\n".join(orch.decision_log())
    assert "并发已满 (1/1)" in log


def test_device_allows_parallel_up_to_limit(clock, driver):
    orch = make_orchestrator(clock, driver, devices=[Device("robot-2", max_concurrency=2)])
    gate1 = driver.gate("m1")
    gate2 = driver.gate("m2")
    orch.submit(mission("m1", device="robot-2"))
    orch.submit(mission("m2", device="robot-2"))
    orch.submit(mission("m3", device="robot-2"))
    wait_for(lambda: driver.started["m1"].is_set() and driver.started["m2"].is_set())
    assert orch.get("m3").status is MissionStatus.QUEUED  # 不超 2 并发
    gate1.set()
    gate2.set()
    wait_for(lambda: all(
        orch.get(m).status is MissionStatus.SUCCEEDED for m in ("m1", "m2", "m3")
    ))


def test_resource_mutex_across_devices(clock, driver):
    orch = make_orchestrator(
        clock, driver,
        devices=[Device("robot-1"), Device("robot-2")],
    )
    gate1 = driver.gate("m1")
    # 两个任务在不同设备上，但争抢同一资源 "dock-a"
    orch.submit(mission("m1", device="robot-1", resources=["dock-a"]))
    orch.submit(mission("m2", device="robot-2", resources=["dock-a"]))
    wait_for(lambda: driver.started["m1"].is_set())
    assert orch.get("m1").status is MissionStatus.RUNNING
    assert orch.get("m2").status is MissionStatus.QUEUED  # 资源互斥阻塞
    gate1.set()
    wait_for(lambda: orch.get("m2").status is MissionStatus.SUCCEEDED)
    log = "\n".join(orch.decision_log())
    assert "资源被占用 {'dock-a': 'm1'}" in log


def test_cancel_releases_slot_for_queued(clock, driver):
    orch = make_orchestrator(clock, driver)
    gate1 = driver.gate("m1")
    orch.submit(mission("m1"))
    orch.submit(mission("m2"))
    wait_for(lambda: driver.started["m1"].is_set())
    orch.cancel("m1")  # 取消运行中的 m1，槽位应立即让给 m2
    wait_for(lambda: orch.get("m2").status is MissionStatus.RUNNING)
    gate1.set()
    wait_for(lambda: orch.get("m2").status is MissionStatus.SUCCEEDED)
