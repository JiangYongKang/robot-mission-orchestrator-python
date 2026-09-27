"""HTTP 入口：把领域操作暴露为 REST API，错误原因码映射为可区分的响应。"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from orchestrator import Device, Mission, Orchestrator, OrchestratorError
from orchestrator.errors import ErrorReason
from orchestrator.models import default_clock
from orchestrator.models import ExecutionOutcome, OutcomeKind

# 原因码 -> HTTP 状态码
_STATUS_BY_REASON = {
    ErrorReason.MISSION_NOT_FOUND: 404,
    ErrorReason.DEVICE_NOT_FOUND: 404,
    ErrorReason.DUPLICATE_MISSION_CONFLICT: 409,
    ErrorReason.ILLEGAL_STATE_TRANSITION: 409,
    ErrorReason.MISSION_ALREADY_FINISHED: 409,
}


def _default_driver(mission: Mission) -> ExecutionOutcome:
    """默认模拟驱动：直接成功。生产中替换为真实设备适配层。"""
    return ExecutionOutcome(OutcomeKind.SUCCESS, f"simulated ok: {mission.payload}")


class SubmitRequest(BaseModel):
    mission_id: str
    device_id: str
    resources: list[str] = Field(default_factory=list)
    payload: str = ""
    max_retries: int = 0


def create_app(orchestrator: Orchestrator | None = None) -> FastAPI:
    orch = orchestrator or Orchestrator(
        devices=[Device("robot-1", max_concurrency=1), Device("robot-2", max_concurrency=2)],
        driver=_default_driver,
        clock=default_clock,
    )
    app = FastAPI(title="robot-mission-orchestrator")
    app.state.orchestrator = orch

    def _view(m: Mission) -> dict:
        return {
            "mission_id": m.mission_id,
            "device_id": m.device_id,
            "resources": sorted(m.resources),
            "payload": m.payload,
            "max_retries": m.max_retries,
            "status": m.status.value,
            "result": m.result,
            "error_reason": m.error_reason,
            "retries_used": m.retries_used,
            "attempts": [
                {"index": a.index, "outcome": a.outcome.value, "detail": a.detail}
                for a in m.attempts
            ],
        }

    def _guard(fn):
        try:
            return fn()
        except OrchestratorError as e:
            raise HTTPException(
                status_code=_STATUS_BY_REASON.get(e.reason, 500),
                detail={"reason": e.reason.value, "message": str(e)},
            ) from e

    @app.post("/missions", status_code=201)
    def submit(req: SubmitRequest) -> dict:
        mission = Mission(
            mission_id=req.mission_id,
            device_id=req.device_id,
            resources=frozenset(req.resources),
            payload=req.payload,
            max_retries=req.max_retries,
        )
        return _view(_guard(lambda: orch.submit(mission)))

    @app.get("/missions")
    def list_missions() -> list[dict]:
        return [_view(m) for m in orch.list_missions()]

    @app.get("/missions/{mission_id}")
    def get_mission(mission_id: str) -> dict:
        return _view(_guard(lambda: orch.get(mission_id)))

    @app.post("/missions/{mission_id}/cancel")
    def cancel(mission_id: str) -> dict:
        return _view(_guard(lambda: orch.cancel(mission_id)))

    @app.post("/missions/{mission_id}/retry")
    def retry(mission_id: str) -> dict:
        return _view(_guard(lambda: orch.retry(mission_id)))

    @app.get("/decision-log")
    def decision_log() -> list[str]:
        return orch.decision_log()

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
