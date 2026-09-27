# Robot Mission Orchestrator

基于本地状态模拟多设备与任务队列的机器人任务编排服务，保证并发调度、任务取消与故障重试在各入口（Python API / HTTP）下结果一致且可解释。

## 结构

- `orchestrator/models.py` — 任务/设备/资源模型与状态机定义
- `orchestrator/store.py` — 内存状态存储（单锁保证并发一致）
- `orchestrator/scheduler.py` — FIFO 确定性调度：设备并发上限 + 资源互斥
- `orchestrator/executor.py` — 本地模拟执行：故障注入、退避重试
- `orchestrator/service.py` — 统一入口：下发（幂等）/查询/取消/人工干预
- `main.py` — FastAPI HTTP 入口

## 任务状态流转

```
PENDING ──> QUEUED ──> RUNNING ──> SUCCEEDED（终态）
   │          │           ├──> WAITING_RETRY ──> QUEUED（退避到期后重新排队）
   │          │           └──> FAILED（终态，重试耗尽）
   └──> CANCELLED（终态，任意非终态可取消）
FAILED ──(人工干预 retry)──> PENDING
```

合法流转由 `ALLOWED_TRANSITIONS` 唯一约束；非法流转抛出 `ILLEGAL_TRANSITION`。

## 并发与互斥规则

- 每台设备有 `max_concurrency`，每个资源有 `capacity`，调度时原子占用，绝不超额。
- 调度扫描严格按提交序号 `created_seq` 升序（FIFO），同一状态下排队/运行/失败的先后结果确定且可复现。
- 设备槽或资源不足时任务进入 `QUEUED` 并记录原因，释放后按 FIFO 补位。

## 重试与失败

- 失败原因可区分：`DEVICE_REJECTED`（设备拒绝）、`TIMEOUT`（超时）、`INTERNAL_ERROR`（内部错误）、`CANCELLED_BY_USER`（人工取消）。
- 重试受 `max_attempts` 与指数退避（`backoff_seconds * 2^(n-1)`）约束；退避期间任务处于 `WAITING_RETRY`，到期后重新排队。
- 部分成功后失败不会造成重复执行或结果丢失：结果中记录累计尝试次数，重试耗尽进入 `FAILED`。
- 人工干预：`intervene(task_id, "retry")` 重置计数重新排队；`"abort"` 终止未结束任务。

## 幂等与可区分的拒绝

- 同一 `task_id` 重复提交返回既有记录，不重复执行（含并发提交）。
- 拒绝原因码：`UNKNOWN_TASK` / `UNKNOWN_DEVICE` / `UNKNOWN_RESOURCE` / `ILLEGAL_TRANSITION` / `TASK_ALREADY_FINISHED`（对已结束任务的取消）。
- HTTP 映射：404（未知任务）、400（未知设备/资源）、409（非法流转/已结束）。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 下发任务（幂等） |
| GET | `/tasks/{task_id}` | 查询任务（含流转历史） |
| POST | `/tasks/{task_id}/cancel` | 取消任务 |
| POST | `/tasks/{task_id}/intervene` | 人工干预 `{"action": "retry"\|"abort"}` |

## 验证方法

```bash
uv run pytest tests/ -q        # 全部测试
uv run pytest tests/ -q -s     # 查看任务流转与调度决策日志
```

测试覆盖：

- `test_concurrency.py` — 并发抢用设备/资源、FIFO 确定性、多线程下不超额并发
- `test_submit_and_query.py` — 重复提交幂等（含 8 线程并发提交）、未知对象拒绝
- `test_retry_and_failure.py` — 失败原因可区分、退避重试、重试耗尽、人工干预恢复
- `test_cancel.py` — 排队/待重试任务取消、已结束任务取消拒绝、非法流转拒绝

单测日志（`log_cli` 已开启）会打印每次状态流转与调度决策依据，例如：

```
调度决策: task=t2 保持 QUEUED, 原因=device robot-1 并发已满 (1/1)
状态流转: task=t1 WAITING_RETRY -> QUEUED (退避到期，重新排队)
```
