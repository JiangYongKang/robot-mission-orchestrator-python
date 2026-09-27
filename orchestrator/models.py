"""领域模型：任务、设备与执行结果的类型定义。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable


class MissionStatus(str, Enum):
    """任务状态机。合法流转见 TRANSITIONS。"""

    PENDING = "PENDING"      # 已受理，等待进入调度队列
    QUEUED = "QUEUED"        # 排队等待设备/资源
    RUNNING = "RUNNING"      # 占用设备执行中
    SUCCEEDED = "SUCCEEDED"  # 终态：成功
    FAILED = "FAILED"        # 终态：失败（重试耗尽或不可重试）
    CANCELED = "CANCELED"    # 终态：被取消


# 合法状态流转表
TRANSITIONS: dict[MissionStatus, frozenset[MissionStatus]] = {
    MissionStatus.PENDING: frozenset({MissionStatus.QUEUED, MissionStatus.CANCELED}),
    MissionStatus.QUEUED: frozenset({MissionStatus.RUNNING, MissionStatus.CANCELED}),
    MissionStatus.RUNNING: frozenset({
        MissionStatus.SUCCEEDED,
        MissionStatus.FAILED,
        MissionStatus.QUEUED,   # 失败后重试重新排队
        MissionStatus.CANCELED,
    }),
    MissionStatus.SUCCEEDED: frozenset(),
    MissionStatus.FAILED: frozenset({MissionStatus.QUEUED}),  # 人工干预重新排队
    MissionStatus.CANCELED: frozenset(),
}

TERMINAL_STATES: frozenset[MissionStatus] = frozenset({
    MissionStatus.SUCCEEDED,
    MissionStatus.FAILED,
    MissionStatus.CANCELED,
})


class OutcomeKind(str, Enum):
    """单次执行尝试的结果类别。"""

    SUCCESS = "SUCCESS"
    DEVICE_REJECTED = "DEVICE_REJECTED"  # 设备拒绝执行
    TIMEOUT = "TIMEOUT"                  # 执行超时
    INTERNAL_ERROR = "INTERNAL_ERROR"    # 编排器内部错误


@dataclass(frozen=True)
class ExecutionOutcome:
    """一次执行尝试的结果。"""

    kind: OutcomeKind
    detail: str = ""


@dataclass
class Attempt:
    """一次执行尝试的记录。"""

    index: int
    outcome: OutcomeKind
    detail: str
    started_at: float
    finished_at: float


@dataclass
class Mission:
    """任务实体。mission_id 由调用方提供，作为幂等键。"""

    mission_id: str
    device_id: str
    resources: frozenset[str]
    payload: str = ""
    max_retries: int = 0
    status: MissionStatus = MissionStatus.PENDING
    attempts: list[Attempt] = field(default_factory=list)
    retries_used: int = 0            # 自动重试已用次数（人工 retry 会清零）
    not_before: float = 0.0          # 退避：该时间前不可被调度
    result: str | None = None
    error_reason: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def signature(self) -> tuple:
        """幂等校验用签名：同 id 不同内容视为冲突。"""
        return (self.device_id, self.resources, self.payload, self.max_retries)


@dataclass
class Device:
    """设备及其并发上限。"""

    device_id: str
    max_concurrency: int = 1


# 设备驱动接口：执行一次任务，返回结果。测试中用假实现脚本化行为。
DeviceDriver = Callable[[Mission], ExecutionOutcome]

# 时钟接口：测试可注入假时钟保证可复现。
Clock = Callable[[], float]


def default_clock() -> float:
    return time.monotonic()
