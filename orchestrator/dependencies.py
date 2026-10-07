"""任务依赖：下发时的环检测、依赖就绪判定与级联终结。

语义约定：
- 任务可声明 depends_on（前置任务 id 列表），允许引用尚未下发的任务（前向引用），
  未就绪期间任务停留在 WAITING_DEPS，不参与设备/资源排队。
- 全部前置 SUCCEEDED -> 依赖就绪，转入 PENDING 走既有 FIFO 调度。
- 任一前置 FAILED   -> 本任务级联 FAILED，原因 DEPENDENCY_FAILED（未执行）。
- 任一前置 CANCELLED -> 本任务级联 CANCELLED，原因 DEPENDENCY_CANCELLED（未执行）。
- 级联沿依赖链传递，直到没有新的状态变化（不动点）。
"""
from __future__ import annotations

import logging

from .errors import RequestRejected
from .models import FailureReason, RejectReason, TaskRecord, TaskStatus
from .store import StateStore

logger = logging.getLogger("orchestrator.dependencies")


class DependencyGate:
    """依赖闸门：在调度前评估 WAITING_DEPS 任务的就绪/级联终结。"""

    def __init__(self, store: StateStore) -> None:
        self._store = store

    # --- 下发校验：自依赖与环 ---
    def check_acyclic(self, task: TaskRecord) -> None:
        """若把 task 加入现有依赖图会成环（含自依赖），拒绝并给出可区分原因。"""
        if task.task_id in task.depends_on:
            raise RequestRejected(
                RejectReason.DEPENDENCY_CYCLE,
                f"任务 {task.task_id} 不能依赖自身",
            )
        # 沿 depends_on 边做 DFS，若能回到 task.task_id 则成环
        stack = list(task.depends_on)
        visited: set[str] = set()
        while stack:
            dep_id = stack.pop()
            if dep_id == task.task_id:
                raise RequestRejected(
                    RejectReason.DEPENDENCY_CYCLE,
                    f"任务 {task.task_id} 的依赖关系构成环",
                )
            if dep_id in visited:
                continue
            visited.add(dep_id)
            dep = self._store.get_task(dep_id)
            if dep is not None:
                stack.extend(dep.depends_on)
        logger.info("依赖校验: task=%s depends_on=%s 无环", task.task_id, task.depends_on)

    # --- 查询辅助：当前阻塞该任务的前置 ---
    def blockers(self, task: TaskRecord) -> list[dict]:
        """返回每个前置的当前状态，供查询视图解释「在等谁、为什么」。"""
        out = []
        for dep_id in task.depends_on:
            dep = self._store.get_task(dep_id)
            if dep is None:
                out.append({"task_id": dep_id, "status": "MISSING",
                            "note": "前置尚未下发"})
            else:
                out.append({"task_id": dep_id, "status": dep.status.value,
                            "note": "" if dep.status == TaskStatus.SUCCEEDED
                                    else "前置未成功完成"})
        return out

    # --- 推进：就绪提升 + 级联终结（调用方须持有 store.lock） ---
    def evaluate(self) -> None:
        """评估一轮依赖闸门，迭代到不动点（级联可能沿链传递）。"""
        while self._evaluate_once():
            pass

    def _evaluate_once(self) -> bool:
        changed = False
        waiting = sorted(
            (t for t in self._store.list_tasks()
             if t.status == TaskStatus.WAITING_DEPS),
            key=lambda t: t.created_seq,
        )
        for task in waiting:
            deps = [self._store.get_task(d) for d in task.depends_on]
            failed = next(
                (d for d in deps if d is not None and d.status == TaskStatus.FAILED),
                None,
            )
            if failed is not None:
                self._cascade(
                    task, TaskStatus.FAILED, FailureReason.DEPENDENCY_FAILED,
                    f"前置 {failed.task_id} 失败"
                    f"({failed.failure_reason.value if failed.failure_reason else 'FAILED'})"
                    "，级联终结，未执行",
                )
                changed = True
                continue
            cancelled = next(
                (d for d in deps if d is not None and d.status == TaskStatus.CANCELLED),
                None,
            )
            if cancelled is not None:
                self._cascade(
                    task, TaskStatus.CANCELLED, FailureReason.DEPENDENCY_CANCELLED,
                    f"前置 {cancelled.task_id} 被取消，级联终结，未执行",
                )
                changed = True
                continue
            if all(d is not None and d.status == TaskStatus.SUCCEEDED for d in deps):
                task.history.append(
                    (task.status, TaskStatus.PENDING, "依赖就绪，进入调度队列")
                )
                task.status = TaskStatus.PENDING
                logger.info("依赖判定: task=%s 全部前置成功 -> PENDING", task.task_id)
                changed = True
                continue
            pending_deps = [
                f"{d_id}({'未下发' if d is None else d.status.value})"
                for d_id, d in zip(task.depends_on, deps)
                if d is None or d.status != TaskStatus.SUCCEEDED
            ]
            logger.info(
                "依赖判定: task=%s 保持 WAITING_DEPS, 等待前置: %s",
                task.task_id, ", ".join(pending_deps),
            )
        return changed

    def _cascade(
        self,
        task: TaskRecord,
        to: TaskStatus,
        reason: FailureReason,
        note: str,
    ) -> None:
        task.failure_reason = reason
        task.history.append((task.status, to, note))
        task.status = to
        logger.info("级联终结: task=%s -> %s (%s)", task.task_id, to.value, note)
