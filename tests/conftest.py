"""测试基建：假时钟 + 可脚本化设备驱动，保证结果确定可复现。"""

from __future__ import annotations

import threading

import pytest

from orchestrator import Device, ExecutionOutcome, Mission, Orchestrator, OutcomeKind


class FakeClock:
    def __init__(self) -> None:
        self._now = 1000.0
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, dt: float) -> None:
        with self._lock:
            self._now += dt


class ScriptedDriver:
    """按 mission_id 脚本化每次尝试的结果；可挂起以模拟执行中。"""

    def __init__(self) -> None:
        self._scripts: dict[str, list[ExecutionOutcome]] = {}
        self._gates: dict[str, threading.Event] = {}
        self.started: dict[str, threading.Event] = {}
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def script(self, mission_id: str, outcomes: list[ExecutionOutcome]) -> None:
        self._scripts[mission_id] = list(outcomes)

    def gate(self, mission_id: str) -> threading.Event:
        """让该任务的执行阻塞，直到测试放行。"""
        ev = threading.Event()
        self._gates[mission_id] = ev
        self.started[mission_id] = threading.Event()
        return ev

    def __call__(self, mission: Mission) -> ExecutionOutcome:
        with self._lock:
            self.calls.append(mission.mission_id)
        gate = self._gates.get(mission.mission_id)
        if gate is not None:
            self.started[mission.mission_id].set()
            gate.wait(timeout=5)
        with self._lock:
            outcomes = self._scripts.get(mission.mission_id)
            outcome = outcomes.pop(0) if outcomes else ExecutionOutcome(
                OutcomeKind.SUCCESS, "ok"
            )
        return outcome


def wait_for(pred, timeout: float = 5.0, interval: float = 0.005) -> None:
    """轮询等待条件成立，避免依赖真实时序。"""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(interval)
    raise AssertionError("等待条件超时")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def driver() -> ScriptedDriver:
    return ScriptedDriver()


def make_orchestrator(clock, driver, devices=None, base_backoff=1.0) -> Orchestrator:
    return Orchestrator(
        devices=devices if devices is not None else [Device("robot-1", max_concurrency=1)],
        driver=driver,
        clock=clock,
        base_backoff=base_backoff,
    )


def mission(mid: str, device: str = "robot-1", resources=(), payload="", max_retries=0) -> Mission:
    return Mission(
        mission_id=mid,
        device_id=device,
        resources=frozenset(resources),
        payload=payload,
        max_retries=max_retries,
    )
