import logging

import pytest

from orchestrator.models import Device, Resource
from orchestrator.service import OrchestratorService


@pytest.fixture(autouse=True)
def _log_caplog(caplog):
    """单测日志中打印任务流转与调度决策依据。"""
    caplog.set_level(logging.DEBUG, logger="orchestrator")
    yield caplog


@pytest.fixture
def service():
    svc = OrchestratorService()
    svc._store.add_device(Device("robot-1", max_concurrency=1))
    svc._store.add_device(Device("robot-2", max_concurrency=2))
    svc._store.add_resource(Resource("dock-a", capacity=1))
    return svc
