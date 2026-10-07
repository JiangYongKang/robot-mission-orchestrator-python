"""编排服务：任务下发（幂等）、查询、取消、重试与人工干预。"""
from __future__ import annotations

import logging

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

    # --- 依赖校验（下发时） ---
    def _check_dependencies(self, task: TaskRecord) -> None:
        """拒绝自依赖与依赖环。前置允许引用尚未下发的任务（后补下发）。"""
        if task.task_id in task.depends_on:
            raise RequestRejected(
                RejectReason.SELF_DEPENDENCY,
                f"任务 {task.task_id} 不能依赖自己",
            )
        # 沿 既有任务 -> 其前置 的边走 DFS，若能回到新任务则成环
        # （新任务尚未入库，环只能经由既有任务的前置边绕回新任务）
        visiting: set[str] = set()
        visited: set[str] = set()

        def reaches_new(tid: str) -> bool:
            if tid == task.task_id:
                return True
            if tid in visited:
                return False
            if tid in visiting:
                return False  # 既有图无环，不会重复进入
            visiting.add(tid)
            node = self._store.get_task(tid)
            found = False
            if node is not None:
                found = any(reaches_new(dep) for dep in node.depends_on)
            visiting.discard(tid)
            visited.add(tid)
            return found

        for dep in task.depends_on:
            if reaches_new(dep):
                raise RequestRejected(
                    RejectReason.DEPENDENCY_CYCLE,
                    f"依赖成环: {task.task_id} -> {dep} -> ... -> {task.task_id}",
                )

    # --- 依赖判定（pump 内，持锁调用） ---
    def _dependency_blockers(self, task: TaskRecord) -> list[str]:
        """返回尚未成功的前置描述列表；空列表表示依赖就绪。"""
        blockers: list[str] = []
        for dep_id in task.depends_on:
            dep = self._store.get_task(dep_id)
            if dep is None:
                blockers.append(f"{dep_id}(未下发)")
            elif dep.status != TaskStatus.SUCCEEDED:
                blockers.append(f"{dep_id}({dep.status.value})")
        return blockers

    def _evaluate_dependencies(self) -> None:
        """推进依赖等待中的任务，循环至不动点（级联在同一轮内传播）。

        规则：任一前置处于 FAILED/CANCELLED/SKIPPED -> 本任务 SKIPPED（级联终结）；
        全部前置 SUCCEEDED -> PENDING（进入正常调度排队）。
        """
        changed = True
        while changed:
            changed = False
            for task in self._store.list_tasks():
                if task.status != TaskStatus.WAITING_DEPENDENCY:
                    continue
                dead = [
                    dep_id
                    for dep_id in task.depends_on
                    if (dep := self._store.get_task(dep_id)) is not None
                    and dep.status in (
                        TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.SKIPPED,
                    )
                ]
                if dead:
                    task.failure_reason = FailureReason.DEPENDENCY_FAILED
                    self._transition(
                        task, TaskStatus.SKIPPED,
                        f"前置未成功: {', '.join(dead)}，级联终结（未执行）",
                    )
                    changed = True
                    continue
                blockers = self._dependency_blockers(task)
                if not blockers:
                    self._transition(task, TaskStatus.PENDING, "全部前置已成功，进入调度")
                    changed = True
                else:
                    logger.info(
                        "依赖判定: task=%s 保持 WAITING_DEPENDENCY, 等待前置 %s",
                        task.task_id, ", ".join(blockers),
                    )

    def _downstream(self, task_id: str) -> list[TaskRecord]:
        """收集 task_id 的全部下游（传递依赖它的）未结束任务。"""
        result: list[TaskRecord] = []
        seen = {task_id}
        frontier = [task_id]
        while frontier:
            current = frontier.pop()
            for task in self._store.list_tasks():
                if task.task_id in seen or current not in task.depends_on:
                    continue
                seen.add(task.task_id)
                if not task.finished:
                    result.append(task)
                    frontier.append(task.task_id)
        return result

    def _cancel_locked(self, task: TaskRecord, note: str) -> None:
        if task.status == TaskStatus.RUNNING:
            self._executor._release(task)
        task.failure_reason = FailureReason.CANCELLED_BY_USER
        self._transition(task, TaskStatus.CANCELLED, note)

    # --- 下发（幂等） ---
    def submit(self, task: TaskRecord) -> TaskRecord:
        """幂等下发：同一 task_id 重复提交返回既有记录，不产生重复执行。

        带 depends_on 的任务先停在 WAITING_DEPENDENCY，待全部前置成功后才进入调度。
        """
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
            self._check_dependencies(task)
            task.created_seq = self._store.next_seq()
            if task.depends_on:
                task.status = TaskStatus.WAITING_DEPENDENCY
            self._store.put_task(task)
            logger.info(
                "任务受理: task=%s device=%s seq=%d depends_on=%s",
                task.task_id, task.device_id, task.created_seq,
                ",".join(task.depends_on) or "无",
            )
        self.pump()
        return task

    # --- 查询 ---
    def get(self, task_id: str) -> TaskRecord:
        return self._must_get(task_id)

    def waiting_on(self, task_id: str) -> list[str]:
        """查询任务当前仍在等待的前置（含未下发的前置）；非等待态返回空列表。"""
        with self._store.lock:
            task = self._must_get(task_id)
            if task.status != TaskStatus.WAITING_DEPENDENCY:
                return []
            return self._dependency_blockers(task)

    # --- 取消 ---
    def cancel(self, task_id: str, cascade: bool = False) -> TaskRecord:
        """取消任务；已结束任务拒绝并给出可区分原因。

        cascade=True 时把全部下游（传递依赖它的）未结束任务一并取消。
        """
        with self._store.lock:
            task = self._must_get(task_id)
            if task.finished:
                raise RequestRejected(
                    RejectReason.TASK_ALREADY_FINISHED,
                    f"任务 {task_id} 已结束 ({task.status.value})，不可取消",
                )
            downstream = self._downstream(task_id) if cascade else []
            for d in downstream:
                self._cancel_locked(d, f"级联取消: 上游 {task_id} 被取消")
            self._cancel_locked(task, "人工取消")
            if downstream:
                logger.info(
                    "级联取消: task=%s 连带下游 %s",
                    task_id, [d.task_id for d in downstream],
                )
        self.pump()
        return task

    # --- 人工干预 ---
    def intervene(self, task_id: str, action: str, cascade: bool = False) -> TaskRecord:
        """人工干预：'retry' 重置失败任务重新排队；'abort' 终止未结束任务。

        abort 带 cascade=True 时把全部下游未结束任务一并终止。
        """
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
                downstream = self._downstream(task_id) if cascade else []
                for d in downstream:
                    self._cancel_locked(d, f"级联终止: 上游 {task_id} 被 abort")
                self._cancel_locked(task, "人工干预: abort")
                if downstream:
                    logger.info(
                        "级联终止: task=%s 连带下游 %s",
                        task_id, [d.task_id for d in downstream],
                    )
            else:
                raise RequestRejected(
                    RejectReason.ILLEGAL_TRANSITION, f"未知干预动作: {action}"
                )
        self.pump()
        return task

    # --- 推进循环 ---
    def pump(self) -> None:
        """推进一轮：依赖判定 -> 到期重试 -> 调度 -> 同步执行，循环至无新任务可运行。"""
        while True:
            with self._store.lock:
                self._evaluate_dependencies()
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
