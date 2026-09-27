"""核心数据模型：任务、设备、资源与状态机定义。"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    """任务状态。合法流转见 ALLOWED_TRANSITIONS。"""

    PENDING = "PENDING"        # 已受理，等待调度
    QUEUED = "QUEUED"          # 已分配目标设备，等待设备并发槽/资源
    RUNNING = "RUNNING"        # 正在执行
    WAITING_RETRY = "WAITING_RETRY"  # 失败后退避等待重试
    SUCCEEDED = "SUCCEEDED"    # 终态：成功
    FAILED = "FAILED"          # 终态：失败（重试耗尽或不可重试）
    CANCELLED = "CANCELLED"    # 终态：被取消


TERMINAL_STATES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}
)

ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.WAITING_RETRY, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.WAITING_RETRY: frozenset(
        {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class FailureReason(str, enum.Enum):
    """失败原因，用于区分可解释的错误类别。"""

    DEVICE_REJECTED = "DEVICE_REJECTED"   # 设备拒绝执行
    TIMEOUT = "TIMEOUT"                   # 执行超时
    INTERNAL_ERROR = "INTERNAL_ERROR"     # 编排器内部错误
    CANCELLED_BY_USER = "CANCELLED_BY_USER"  # 人工取消


class RejectReason(str, enum.Enum):
    """请求被拒绝的原因（面向调用方，可区分）。"""

    UNKNOWN_TASK = "UNKNOWN_TASK"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    TASK_ALREADY_FINISHED = "TASK_ALREADY_FINISHED"
    UNKNOWN_DEVICE = "UNKNOWN_DEVICE"
    UNKNOWN_RESOURCE = "UNKNOWN_RESOURCE"


@dataclass
class Device:
    device_id: str
    max_concurrency: int = 1
    running: int = 0  # 当前占用并发槽数


@dataclass
class Resource:
    resource_id: str
    capacity: int = 1
    in_use: int = 0


@dataclass
class TaskRecord:
    task_id: str
    device_id: str
    required_resources: tuple[str, ...] = ()
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    backoff_seconds: float = 0.01
    failure_reason: FailureReason | None = None
    result: dict | None = None
    next_retry_at: float | None = None
    created_seq: int = 0  # 提交顺序，保证调度确定性（FIFO）
    history: list[tuple[TaskStatus, TaskStatus, str]] = field(default_factory=list)

    @property
    def finished(self) -> bool:
        return self.status in TERMINAL_STATES


def now() -> float:
    return time.monotonic()
