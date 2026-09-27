"""本地状态存储：任务、设备、资源，带锁保证并发一致。"""
from __future__ import annotations

import threading

from .models import Device, Resource, TaskRecord


class StateStore:
    """内存状态存储，所有读写在同一把锁下完成，保证可复现。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._devices: dict[str, Device] = {}
        self._resources: dict[str, Resource] = {}
        self._seq = 0

    # --- 设备与资源注册 ---
    def add_device(self, device: Device) -> None:
        with self._lock:
            self._devices[device.device_id] = device

    def add_resource(self, resource: Resource) -> None:
        with self._lock:
            self._resources[resource.resource_id] = resource

    # --- 任务读写 ---
    def next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._tasks.get(task_id)

    def put_task(self, task: TaskRecord) -> None:
        with self._lock:
            self._tasks[task.task_id] = task

    def list_tasks(self) -> list[TaskRecord]:
        with self._lock:
            return list(self._tasks.values())

    def get_device(self, device_id: str) -> Device | None:
        with self._lock:
            return self._devices.get(device_id)

    def get_resource(self, resource_id: str) -> Resource | None:
        with self._lock:
            return self._resources.get(resource_id)

    @property
    def lock(self) -> threading.RLock:
        return self._lock
