"""编排服务：任务下发（幂等）、查询、取消、重试与人工干预。"""
from __future__ import annotations

import logging
import threading

from .errors import RequestRejected
from .executor import Executor
from .models import (
    ALLOWED_TRANSITIONS,
    FailureReason,
    RejectReason,
    TaskRecord,
    TaskStatus,
    now,
)
from .scheduler import Scheduler
from .store import StateStore

logger = logging.getLogger("orchestrator.service")


class OrchestratorService:
    """对外统一入口：submit / get / cancel / intervene / pump。"""

    def __init__(
        self,
        store: StateStore | None = None,
        fault_plan: dict[str, list[str]] | None = None,
    ) -> None:
        self._store = store or StateStore()
        self._scheduler = Scheduler(self._store)
        self._executor = Executor(self._store, fault_plan)

    # --- 内部工具 ---
    def _transition(self, task: TaskRecord, to: TaskStatus, note: str) -> None:
        if to not in ALLOWED_TRANSITIONS[task.status]:
            raise RequestRejected(
                RejectReason.ILLEGAL_TRANSITION,
                f"任务 {task.task_id} 不允许 {task.status.value} -> {to.value}",
            )
        logger.info(
            "状态流转: task=%s %s -> %s (%s)",
            task.task_id, task.status.value, to.value, note,
        )
        task.history.append((task.status, to, note))
        task.status = to

    def _must_get(self, task_id: str) -> TaskRecord:
        task = self._store.get_task(task_id)
        if task is None:
            raise RequestRejected(RejectReason.UNKNOWN_TASK, f"任务 {task_id} 不存在")
        return task

    # --- 下发（幂等） ---
    def submit(self, task: TaskRecord) -> TaskRecord:
        """幂等下发：同一 task_id 重复提交返回既有记录，不产生重复执行。"""
        with self._store.lock:
            existing = self._store.get_task(task.task_id)
            if existing is not None:
                logger.info("幂等命中: task=%s 已存在, 返回既有记录", task.task_id)
                return existing
            if self._store.get_device(task.device_id) is None:
                raise RequestRejected(
                    RejectReason.UNKNOWN_DEVICE, f"设备 {task.device_id} 不存在"
                )
            for rid in task.required_resources:
                if self._store.get_resource(rid) is None:
                    raise RequestRejected(
                        RejectReason.UNKNOWN_RESOURCE, f"资源 {rid} 不存在"
                    )
            task.created_seq = self._store.next_seq()
            self._store.put_task(task)
            logger.info(
                "任务受理: task=%s device=%s seq=%d",
                task.task_id, task.device_id, task.created_seq,
            )
        self.pump()
        return task

    # --- 查询 ---
    def get(self, task_id: str) -> TaskRecord:
        return self._must_get(task_id)

    # --- 取消 ---
    def cancel(self, task_id: str) -> TaskRecord:
        """取消任务；已结束任务拒绝并给出可区分原因。"""
        with self._store.lock:
            task = self._must_get(task_id)
            if task.finished:
                raise RequestRejected(
                    RejectReason.TASK_ALREADY_FINISHED,
                    f"任务 {task_id} 已结束 ({task.status.value})，不可取消",
                )
            if task.status == TaskStatus.RUNNING:
                self._executor._release(task)
            task.failure_reason = FailureReason.CANCELLED_BY_USER
            self._transition(task, TaskStatus.CANCELLED, "人工取消")
            return task

    # --- 人工干预 ---
    def intervene(self, task_id: str, action: str) -> TaskRecord:
        """人工干预：'retry' 重置失败任务重新排队；'abort' 终止未结束任务。"""
        with self._store.lock:
            task = self._must_get(task_id)
            if action == "retry":
                if task.status != TaskStatus.FAILED:
                    raise RequestRejected(
                        RejectReason.ILLEGAL_TRANSITION,
                        f"仅 FAILED 任务可人工重试，当前 {task.status.value}",
                    )
                # 人工重试：重置计数与退避，回到 PENDING 重新走调度
                task.attempts = 0
                task.next_retry_at = None
                task.failure_reason = None
                task.history.append((task.status, TaskStatus.PENDING, "人工干预: retry"))
                task.status = TaskStatus.PENDING
                logger.info("人工干预: task=%s retry -> PENDING", task_id)
            elif action == "abort":
                if task.finished:
                    raise RequestRejected(
                        RejectReason.TASK_ALREADY_FINISHED,
                        f"任务 {task_id} 已结束，不可 abort",
                    )
                task.failure_reason = FailureReason.CANCELLED_BY_USER
                self._transition(task, TaskStatus.CANCELLED, "人工干预: abort")
            else:
                raise RequestRejected(
                    RejectReason.ILLEGAL_TRANSITION, f"未知干预动作: {action}"
                )
        self.pump()
        return task

    # --- 推进循环 ---
    def pump(self) -> None:
        """推进一轮：处理到期重试 -> 调度 -> 同步执行，循环至无新任务可运行。"""
        while True:
            with self._store.lock:
                for task in self._store.list_tasks():
                    if (
                        task.status == TaskStatus.WAITING_RETRY
                        and task.next_retry_at is not None
                        and task.next_retry_at <= now()
                    ):
                        self._transition(task, TaskStatus.QUEUED, "退避到期，重新排队")
                started = self._scheduler.dispatch()
            if not started:
                return
            for task in started:
                if task.status == TaskStatus.RUNNING:  # 可能已被并发取消
                    self._executor.run_attempt(task)
