"""HTTP 入口：任务下发 / 查询 / 取消 / 人工干预。"""
from __future__ import annotations

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from orchestrator.errors import RequestRejected
from orchestrator.models import Device, RejectReason, Resource, TaskRecord
from orchestrator.service import OrchestratorService

app = FastAPI(title="robot-mission-orchestrator")

service = OrchestratorService()
service._store.add_device(Device("robot-1", max_concurrency=1))
service._store.add_device(Device("robot-2", max_concurrency=2))
service._store.add_resource(Resource("dock-a", capacity=1))

_STATUS_TO_HTTP = {
    RejectReason.UNKNOWN_TASK: 404,
    RejectReason.UNKNOWN_DEVICE: 400,
    RejectReason.UNKNOWN_RESOURCE: 400,
    RejectReason.ILLEGAL_TRANSITION: 409,
    RejectReason.TASK_ALREADY_FINISHED: 409,
}


class SubmitRequest(BaseModel):
    task_id: str
    device_id: str
    required_resources: list[str] = []
    max_attempts: int = 3
    backoff_seconds: float = 0.01


class InterveneRequest(BaseModel):
    action: str  # "retry" | "abort"


def _view(t: TaskRecord) -> dict:
    return {
        "task_id": t.task_id,
        "device_id": t.device_id,
        "status": t.status.value,
        "attempts": t.attempts,
        "failure_reason": t.failure_reason.value if t.failure_reason else None,
        "result": t.result,
        "history": [
            {"from": f.value, "to": to.value, "note": note}
            for f, to, note in t.history
        ],
    }


@app.exception_handler(RequestRejected)
def _rejected_handler(request, exc: RequestRejected):
    raise HTTPException(
        status_code=_STATUS_TO_HTTP[exc.reason],
        detail={"reason": exc.reason.value, "message": exc.message},
    )


@app.post("/tasks", status_code=201)
def submit_task(req: SubmitRequest):
    task = TaskRecord(
        task_id=req.task_id,
        device_id=req.device_id,
        required_resources=tuple(req.required_resources),
        max_attempts=req.max_attempts,
        backoff_seconds=req.backoff_seconds,
    )
    return _view(service.submit(task))


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return _view(service.get(task_id))


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    return _view(service.cancel(task_id))


@app.post("/tasks/{task_id}/intervene")
def intervene_task(task_id: str, req: InterveneRequest):
    return _view(service.intervene(task_id, req.action))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
