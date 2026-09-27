"""编排服务：任务生命周期、并发调度、重试与取消的唯一事实来源。"""

from __future__ import annotations

import threading

from .errors import ErrorReason, OrchestratorError
from .models import (
    Attempt,
    Clock,
    Device,
    DeviceDriver,
    Mission,
    MissionStatus,
    OutcomeKind,
    TERMINAL_STATES,
    TRANSITIONS,
    default_clock,
)

# 失败类别 -> 任务最终错误原因
_OUTCOME_TO_REASON: dict[OutcomeKind, ErrorReason] = {
    OutcomeKind.DEVICE_REJECTED: ErrorReason.DEVICE_REJECTED,
    OutcomeKind.TIMEOUT: ErrorReason.DEVICE_TIMEOUT,
    OutcomeKind.INTERNAL_ERROR: ErrorReason.INTERNAL_ERROR,
}


class Orchestrator:
    """机器人任务编排器。

    所有状态变更都在同一把锁内完成，调度决策确定且可复现：
    按提交顺序 FIFO 扫描队列，设备并发与资源互斥同时满足才放行。
    执行在后台线程进行，完成回调重新取锁结算并再次调度。
    """

    def __init__(
        self,
        devices: list[Device],
        driver: DeviceDriver,
        clock: Clock = default_clock,
        base_backoff: float = 0.1,
    ) -> None:
        self._lock = threading.RLock()
        self._devices: dict[str, Device] = {d.device_id: d for d in devices}
        self._driver = driver
        self._clock = clock
        self._base_backoff = base_backoff
        self._missions: dict[str, Mission] = {}
        self._order: list[str] = []               # 提交顺序，保证 FIFO 可复现
        self._running: dict[str, set[str]] = {d.device_id: set() for d in devices}
        self._resource_holders: dict[str, str] = {}  # resource -> mission_id
        self._log: list[str] = []

    # ---- 日志 ----
    def _logline(self, msg: str) -> None:
        line = f"[{self._clock():.6f}] {msg}"
        self._log.append(line)
        print(line, flush=True)

    def decision_log(self) -> list[str]:
        """返回调度决策与状态流转日志（测试与排障用）。"""
        with self._lock:
            return list(self._log)

    # ---- 任务入口 ----
    def submit(self, mission: Mission) -> Mission:
        """下发任务。同 id 同内容为幂等命中；同 id 不同内容拒绝。"""
        with self._lock:
            existing = self._missions.get(mission.mission_id)
            if existing is not None:
                if existing.signature() == mission.signature():
                    self._logline(
                        f"submit {mission.mission_id}: 幂等命中，返回既有任务 "
                        f"(status={existing.status.value})"
                    )
                    return existing
                raise OrchestratorError(
                    ErrorReason.DUPLICATE_MISSION_CONFLICT,
                    f"任务 {mission.mission_id} 已存在且内容不一致",
                )
            if mission.device_id not in self._devices:
                raise OrchestratorError(
                    ErrorReason.DEVICE_NOT_FOUND,
                    f"设备 {mission.device_id} 不存在",
                )
            now = self._clock()
            mission.created_at = mission.updated_at = now
            self._missions[mission.mission_id] = mission
            self._order.append(mission.mission_id)
            self._transition(mission, MissionStatus.QUEUED, "受理入队")
            self._dispatch_locked()
            return mission

    def get(self, mission_id: str) -> Mission:
        """查询任务。"""
        with self._lock:
            return self._must_get(mission_id)

    def list_missions(self) -> list[Mission]:
        """按提交顺序列出全部任务。"""
        with self._lock:
            return [self._missions[mid] for mid in self._order]

    def cancel(self, mission_id: str) -> Mission:
        """取消任务。已结束任务的取消必须被拒绝并给出可区分原因。"""
        with self._lock:
            mission = self._must_get(mission_id)
            if mission.status in TERMINAL_STATES:
                raise OrchestratorError(
                    ErrorReason.MISSION_ALREADY_FINISHED,
                    f"任务 {mission_id} 已结束 ({mission.status.value})，不能取消",
                )
            if mission.status is MissionStatus.RUNNING:
                self._release_locked(mission)
            self._transition(mission, MissionStatus.CANCELED, "收到取消请求")
            mission.error_reason = "CANCELED_BY_USER"
            self._dispatch_locked()
            return mission

    def retry(self, mission_id: str) -> Mission:
        """人工干预：将失败任务重新排队，重置自动重试预算。"""
        with self._lock:
            mission = self._must_get(mission_id)
            if mission.status is not MissionStatus.FAILED:
                raise OrchestratorError(
                    ErrorReason.ILLEGAL_STATE_TRANSITION,
                    f"只有 FAILED 任务可人工重试，当前 {mission.status.value}",
                )
            mission.retries_used = 0
            mission.not_before = 0.0
            mission.error_reason = None
            self._transition(mission, MissionStatus.QUEUED, "人工干预重新排队")
            self._dispatch_locked()
            return mission

    # ---- 调度与执行 ----
    def dispatch(self) -> None:
        """外部触发一次调度扫描（如测试推进假时钟后）。"""
        with self._lock:
            self._dispatch_locked()

    def _dispatch_locked(self) -> None:
        """FIFO 扫描队列，设备并发与资源互斥同时满足才放行。"""
        now = self._clock()
        for mid in list(self._order):
            mission = self._missions[mid]
            if mission.status is not MissionStatus.QUEUED:
                continue
            if mission.not_before > now:
                self._logline(
                    f"dispatch {mid}: 退避中，not_before={mission.not_before:.6f} > now={now:.6f}"
                )
                continue
            device = self._devices[mission.device_id]
            running = self._running[mission.device_id]
            if len(running) >= device.max_concurrency:
                self._logline(
                    f"dispatch {mid}: 设备 {mission.device_id} 并发已满 "
                    f"({len(running)}/{device.max_concurrency})，继续排队"
                )
                continue
            held_by = {
                r: self._resource_holders.get(r) for r in mission.resources
            }
            conflicts = {r: h for r, h in held_by.items() if h is not None}
            if conflicts:
                self._logline(
                    f"dispatch {mid}: 资源被占用 {conflicts}，继续排队"
                )
                continue
            # 放行：占用设备槽位与资源
            running.add(mid)
            for r in mission.resources:
                self._resource_holders[r] = mid
            self._transition(mission, MissionStatus.RUNNING, "调度放行")
            self._logline(
                f"dispatch {mid}: 占用设备 {mission.device_id} "
                f"({len(running)}/{device.max_concurrency}) 资源 {sorted(mission.resources)}"
            )
            threading.Thread(
                target=self._execute, args=(mid,), daemon=True
            ).start()

    def _execute(self, mission_id: str) -> None:
        """后台线程：执行一次尝试并在锁内结算结果。"""
        started = self._clock()
        try:
            outcome = self._driver(self._missions[mission_id])
        except Exception as exc:  # 驱动自身异常归为内部错误
            from .models import ExecutionOutcome
            outcome = ExecutionOutcome(OutcomeKind.INTERNAL_ERROR, f"{type(exc).__name__}: {exc}")
        finished = self._clock()
        with self._lock:
            mission = self._missions[mission_id]
            if mission.status is not MissionStatus.RUNNING:
                # 执行期间已被取消等，迟到结果直接丢弃，状态以取消为准
                self._logline(
                    f"execute {mission_id}: 迟到结果被丢弃 (status={mission.status.value})"
                )
                return
            attempt = Attempt(
                index=len(mission.attempts) + 1,
                outcome=outcome.kind,
                detail=outcome.detail,
                started_at=started,
                finished_at=finished,
            )
            mission.attempts.append(attempt)
            self._release_locked(mission)
            if outcome.kind is OutcomeKind.SUCCESS:
                mission.result = outcome.detail
                mission.error_reason = None
                self._transition(mission, MissionStatus.SUCCEEDED, f"执行成功: {outcome.detail}")
            elif mission.retries_used < mission.max_retries:
                mission.retries_used += 1
                backoff = self._base_backoff * (2 ** (mission.retries_used - 1))
                mission.not_before = self._clock() + backoff
                self._transition(
                    mission, MissionStatus.QUEUED,
                    f"第 {attempt.index} 次尝试失败({outcome.kind.value}: {outcome.detail})，"
                    f"第 {mission.retries_used}/{mission.max_retries} 次重试，退避 {backoff:.3f}s",
                )
            else:
                reason = _OUTCOME_TO_REASON[outcome.kind]
                mission.error_reason = reason.value
                self._transition(
                    mission, MissionStatus.FAILED,
                    f"第 {attempt.index} 次尝试失败({outcome.kind.value}: {outcome.detail})，"
                    f"重试耗尽，原因 {reason.value}",
                )
            self._dispatch_locked()

    # ---- 内部工具 ----
    def _must_get(self, mission_id: str) -> Mission:
        mission = self._missions.get(mission_id)
        if mission is None:
            raise OrchestratorError(
                ErrorReason.MISSION_NOT_FOUND, f"任务 {mission_id} 不存在"
            )
        return mission

    def _release_locked(self, mission: Mission) -> None:
        """释放设备槽位与资源（完成或取消时调用）。"""
        self._running[mission.device_id].discard(mission.mission_id)
        for r in mission.resources:
            if self._resource_holders.get(r) == mission.mission_id:
                del self._resource_holders[r]
        self._logline(
            f"release {mission.mission_id}: 释放设备 {mission.device_id} "
            f"资源 {sorted(mission.resources)}"
        )

    def _transition(self, mission: Mission, target: MissionStatus, why: str) -> None:
        """状态流转，非法流转抛错。"""
        if target not in TRANSITIONS[mission.status]:
            raise OrchestratorError(
                ErrorReason.ILLEGAL_STATE_TRANSITION,
                f"任务 {mission.mission_id} 不允许 {mission.status.value} -> {target.value}",
            )
        self._logline(
            f"transition {mission.mission_id}: {mission.status.value} -> {target.value} ({why})"
        )
        mission.status = target
        mission.updated_at = self._clock()
