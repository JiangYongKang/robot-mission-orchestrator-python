"""可区分的拒绝/失败异常。"""
from __future__ import annotations

from .models import RejectReason


class OrchestratorError(Exception):
    """编排器基础异常。"""


class RequestRejected(OrchestratorError):
    """请求被拒绝，携带可区分的原因码。"""

    def __init__(self, reason: RejectReason, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message
