"""执行器：本地模拟设备执行，支持失败注入、超时与重试退避。"""
from __future__ import annotations

import logging

from .models import FailureReason, TaskRecord, TaskStatus, now
from .store import StateStore

logger = logging.getLogger("orchestrator.executor")

_FAULT_TO_REASON = {
    "reject": FailureReason.DEVICE_REJECTED,
    "timeout": FailureReason.TIMEOUT,
    "error": FailureReason.INTERNAL_ERROR,
}


class Executor:
    """同步模拟执行一个任务的一次尝试，更新任务状态与结果。

    fault_plan: task_id -> 每次尝试注入的故障类型队列，
    取值 "ok" | "reject" | "timeout" | "error"；队列用尽后视为 "ok"。
    """

    def __init__(self, store: StateStore, fault_plan: dict[str, list[str]] | None = None) -> None:
        self._store = store
        # 复制一份，故障按计划逐次消费（pop），人工重试后不会重放旧故障
        self._fault_plan = {k: list(v) for k, v in (fault_plan or {}).items()}

    def _release(self, task: TaskRecord) -> None:
        device = self._store.get_device(task.device_id)
        if device is not None and device.running > 0:
            device.running -= 1
        for rid in task.required_resources:
            res = self._store.get_resource(rid)
            if res is not None and res.in_use > 0:
                res.in_use -= 1

    def _next_fault(self, task: TaskRecord) -> str:
        plan = self._fault_plan.get(task.task_id)
        if not plan:
            return "ok"
        return plan.pop(0)

    def run_attempt(self, task: TaskRecord) -> None:
        """执行一次尝试。调用前任务须处于 RUNNING。"""
        with self._store.lock:
            task.attempts += 1
            fault = self._next_fault(task)
            logger.info(
                "执行尝试: task=%s 第 %d/%d 次, 注入=%s",
                task.task_id, task.attempts, task.max_attempts, fault,
            )
            self._release(task)

            if fault == "ok":
                task.history.append((task.status, TaskStatus.SUCCEEDED, "执行成功"))
                task.status = TaskStatus.SUCCEEDED
                # 结果包含累计尝试次数，部分成功后重试不会丢失已有结果
                task.result = {"output": f"done-by-{task.device_id}", "attempts": task.attempts}
                logger.info("任务成功: task=%s attempts=%d", task.task_id, task.attempts)
                return

            reason = _FAULT_TO_REASON[fault]
            task.failure_reason = reason
            if task.attempts < task.max_attempts:
                delay = task.backoff_seconds * (2 ** (task.attempts - 1))
                task.next_retry_at = now() + delay
                task.history.append(
                    (task.status, TaskStatus.WAITING_RETRY,
                     f"{reason.value}, 退避 {delay:.3f}s 后重试")
                )
                task.status = TaskStatus.WAITING_RETRY
                logger.info(
                    "任务待重试: task=%s 原因=%s 退避=%.3fs",
                    task.task_id, reason.value, delay,
                )
            else:
                task.history.append(
                    (task.status, TaskStatus.FAILED,
                     f"{reason.value}, 重试次数耗尽 ({task.attempts}/{task.max_attempts})")
                )
                task.status = TaskStatus.FAILED
                logger.info(
                    "任务失败: task=%s 原因=%s 已用尽重试", task.task_id, reason.value
                )
