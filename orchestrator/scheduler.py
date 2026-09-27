"""调度器：设备并发上限、资源互斥、FIFO 确定性调度。"""
from __future__ import annotations

import logging

from .models import TaskRecord, TaskStatus
from .store import StateStore

logger = logging.getLogger("orchestrator.scheduler")


class Scheduler:
    """从待调度任务中按提交顺序挑选可运行的任务并占用设备槽与资源。

    确定性保证：同一状态下，扫描顺序严格按任务提交序号 (created_seq) 升序，
    因此排队、超时与失败的先后结果可复现。
    """

    def __init__(self, store: StateStore) -> None:
        self._store = store

    def _can_allocate(self, task: TaskRecord) -> tuple[bool, str]:
        device = self._store.get_device(task.device_id)
        if device is None:
            return False, f"device {task.device_id} 不存在"
        if device.running >= device.max_concurrency:
            return False, (
                f"device {device.device_id} 并发已满 "
                f"({device.running}/{device.max_concurrency})"
            )
        for rid in task.required_resources:
            res = self._store.get_resource(rid)
            if res is None:
                return False, f"resource {rid} 不存在"
            if res.in_use >= res.capacity:
                return False, (
                    f"resource {rid} 已被占满 ({res.in_use}/{res.capacity})"
                )
        return True, "ok"

    def _allocate(self, task: TaskRecord) -> None:
        device = self._store.get_device(task.device_id)
        assert device is not None
        device.running += 1
        for rid in task.required_resources:
            res = self._store.get_resource(rid)
            assert res is not None
            res.in_use += 1

    def dispatch(self) -> list[TaskRecord]:
        """尝试调度一轮，返回本轮被推进到 RUNNING 的任务（按 FIFO 顺序）。"""
        started: list[TaskRecord] = []
        with self._store.lock:
            candidates = sorted(
                (
                    t
                    for t in self._store.list_tasks()
                    if t.status in (TaskStatus.PENDING, TaskStatus.QUEUED)
                ),
                key=lambda t: t.created_seq,
            )
            for task in candidates:
                ok, why = self._can_allocate(task)
                if not ok:
                    if task.status != TaskStatus.QUEUED:
                        task.history.append((task.status, TaskStatus.QUEUED, why))
                        task.status = TaskStatus.QUEUED
                    logger.info(
                        "调度决策: task=%s 保持 QUEUED, 原因=%s", task.task_id, why
                    )
                    continue
                self._allocate(task)
                task.history.append((task.status, TaskStatus.RUNNING, "获得设备槽与资源"))
                task.status = TaskStatus.RUNNING
                started.append(task)
                logger.info(
                    "调度决策: task=%s -> RUNNING (device=%s, seq=%d)",
                    task.task_id,
                    task.device_id,
                    task.created_seq,
                )
        return started
