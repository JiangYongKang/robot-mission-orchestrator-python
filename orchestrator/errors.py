"""领域错误定义：所有拒绝与失败原因都可区分。"""

from __future__ import annotations

from enum import Enum


class ErrorReason(str, Enum):
    """可区分的错误原因码。"""

    MISSION_NOT_FOUND = "MISSION_NOT_FOUND"
    DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND"
    DUPLICATE_MISSION_CONFLICT = "DUPLICATE_MISSION_CONFLICT"
    ILLEGAL_STATE_TRANSITION = "ILLEGAL_STATE_TRANSITION"
    MISSION_ALREADY_FINISHED = "MISSION_ALREADY_FINISHED"
    DEVICE_REJECTED = "DEVICE_REJECTED"
    DEVICE_TIMEOUT = "DEVICE_TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"


class OrchestratorError(Exception):
    """领域异常，携带可区分的原因码。"""

    def __init__(self, reason: ErrorReason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
