"""编排服务：任务下发（幂等）、查询、取消、重试、人工干预与依赖编排。"""
from __future__ import annotations

import logging

from .dependencies import DependencyGate
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
        self._gate = DependencyGate(self._store)

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
            # 依赖声明：去重（保序）并校验无环（含自依赖），前向引用允许
            task.depends_on = tuple(dict.fromkeys(task.depends_on))
            self._gate.check_acyclic(task)
            task.created_seq = self._store.next_seq()
            if task.depends_on:
                task.status = TaskStatus.WAITING_DEPS
                task.history.append(
                    (TaskStatus.PENDING, TaskStatus.WAITING_DEPS,
                     f"等待前置任务: {', '.join(task.depends_on)}")
                )
            self._store.put_task(task)
            logger.info(
                "任务受理: task=%s device=%s seq=%d depends_on=%s",
                task.task_id, task.device_id, task.created_seq, task.depends_on,
            )
        self.pump()
        return task

    # --- 查询 ---
    def get(self, task_id: str) -> TaskRecord:
        return self._must_get(task_id)

    def dependency_view(self, task_id: str) -> list[dict]:
        """返回该任务各前置的当前状态，用于解释「在等谁、为什么还没轮到」。"""
        task = self._must_get(task_id)
        with self._store.lock:
            return self._gate.blockers(task)

    # --- 取消 ---
    def _descendants(self, task_id: str) -> list[TaskRecord]:
        """所有（传递）依赖 task_id 的未结束任务，按提交序。调用方须持有锁。"""
        result: list[TaskRecord] = []
        frontier = {task_id}
        seen: set[str] = set()
        while frontier:
            nxt: set[str] = set()
            for t in self._store.list_tasks():
                if t.task_id in seen or t.finished:
                    continue
                if any(d in frontier for d in t.depends_on):
                    nxt.add(t.task_id)
                    seen.add(t.task_id)
                    result.append(t)
            frontier = nxt
        return sorted(result, key=lambda t: t.created_seq)

    def _cascade_cancel(self, root: TaskRecord) -> None:
        """把 root 的全部下游未结束任务一并取消（随上游级联）。调用方须持有锁。"""
        for t in self._descendants(root.task_id):
            if t.status == TaskStatus.RUNNING:
                self._executor._release(t)
            t.failure_reason = FailureReason.DEPENDENCY_CANCELLED
            self._transition(
                t, TaskStatus.CANCELLED,
                f"随上游 {root.task_id} 级联取消，未执行" if t.attempts == 0
                else f"随上游 {root.task_id} 级联取消",
            )

    def cancel(self, task_id: str, cascade: bool = False) -> TaskRecord:
        """取消任务；已结束任务拒绝并给出可区分原因。

        cascade=True 时把下游（传递依赖它的）未结束任务在同一调用里一并取消；
        否则下游任务由依赖闸门在下一次推进时按既定规则级联终结。
        """
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
            if cascade:
                self._cascade_cancel(task)
            return task

    # --- 人工干预 ---
    def intervene(self, task_id: str, action: str, cascade: bool = False) -> TaskRecord:
        """人工干预：'retry' 重置失败任务重新排队；'abort' 终止未结束任务。

        cascade=True 时，abort 会把下游未结束任务一并取消；retry 若前置未全部
        成功则回到 WAITING_DEPS 而不是直接进入调度。
        """
        with self._store.lock:
            task = self._must_get(task_id)
            if action == "retry":
                if task.status != TaskStatus.FAILED:
                    raise RequestRejected(
                        RejectReason.ILLEGAL_TRANSITION,
                        f"仅 FAILED 任务可人工重试，当前 {task.status.value}",
                    )
                # 人工重试：重置计数与退避；前置未就绪则回到 WAITING_DEPS
                task.attempts = 0
                task.next_retry_at = None
                task.failure_reason = None
                ready = all(
                    (d := self._store.get_task(dep)) is not None
                    and d.status == TaskStatus.SUCCEEDED
                    for dep in task.depends_on
                )
                target = TaskStatus.PENDING if ready else TaskStatus.WAITING_DEPS
                task.history.append((task.status, target, "人工干预: retry"))
                task.status = target
                logger.info("人工干预: task=%s retry -> %s", task_id, target.value)
            elif action == "abort":
                if task.finished:
                    raise RequestRejected(
                        RejectReason.TASK_ALREADY_FINISHED,
                        f"任务 {task_id} 已结束，不可 abort",
                    )
                if task.status == TaskStatus.RUNNING:
                    self._executor._release(task)
                task.failure_reason = FailureReason.CANCELLED_BY_USER
                self._transition(task, TaskStatus.CANCELLED, "人工干预: abort")
                if cascade:
                    self._cascade_cancel(task)
            else:
                raise RequestRejected(
                    RejectReason.ILLEGAL_TRANSITION, f"未知干预动作: {action}"
                )
        self.pump()
        return task

    # --- 推进循环 ---
    def pump(self) -> None:
        """推进一轮：到期重试 -> 依赖闸门 -> 调度 -> 同步执行，循环至无新任务可运行。"""
        while True:
            with self._store.lock:
                for task in self._store.list_tasks():
                    if (
                        task.status == TaskStatus.WAITING_RETRY
                        and task.next_retry_at is not None
                        and task.next_retry_at <= now()
                    ):
                        self._transition(task, TaskStatus.QUEUED, "退避到期，重新排队")
                self._gate.evaluate()
                started = self._scheduler.dispatch()
            if not started:
                return
            for task in started:
                if task.status == TaskStatus.RUNNING:  # 可能已被并发取消
                    self._executor.run_attempt(task)
