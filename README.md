# Robot Mission Orchestrator

基于本地状态模拟多设备与任务队列的机器人任务编排服务，保证并发调度、任务取消与故障重试在各入口（Python API / HTTP）下结果一致且可解释。支持任务间依赖编排：前置全部成功后后继才进入调度，前置失败/取消时按确定规则级联终结。

## 结构

- `orchestrator/models.py` — 任务/设备/资源模型与状态机定义
- `orchestrator/store.py` — 内存状态存储（单锁保证并发一致）
- `orchestrator/scheduler.py` — FIFO 确定性调度：设备并发上限 + 资源互斥
- `orchestrator/executor.py` — 本地模拟执行：故障注入、退避重试
- `orchestrator/service.py` — 统一入口：下发（幂等）/查询/取消/人工干预 + 依赖判定与级联
- `main.py` — FastAPI HTTP 入口

## 任务状态流转

```
                （提交时声明了 depends_on）
PENDING <── WAITING_DEPENDENCY ──> SKIPPED（终态：前置未成功，级联终结，从未执行）
   │              │
   │              └──> CANCELLED（等待期间可取消）
   │
PENDING ──> QUEUED ──> RUNNING ──> SUCCEEDED（终态）
   │          │           ├──> WAITING_RETRY ──> QUEUED（退避到期后重新排队）
   │          │           └──> FAILED（终态，重试耗尽）
   └──> CANCELLED（终态，任意非终态可取消）
FAILED ──(人工干预 retry)──> PENDING
```

合法流转由 `ALLOWED_TRANSITIONS` 唯一约束；非法流转抛出 `ILLEGAL_TRANSITION`。

## 任务依赖语义

- 下发时通过 `depends_on` 声明前置任务（`TaskRecord.depends_on` / HTTP `depends_on` 字段）。
- 带依赖的任务先停在 **`WAITING_DEPENDENCY`**：这是独立的等待态，**不占用设备并发槽与资源**，也不与设备/资源排队（`QUEUED`）混淆。
- 查询等待原因：`service.waiting_on(task_id)`（HTTP 返回体的 `waiting_on` 字段）列出尚未成功的前置及其状态，例如 `["a(WAITING_RETRY)"]`、`["inspect(未下发)"]`。
- 前置允许引用**尚未下发**的任务（后补下发），因此可以任意顺序提交一条依赖链。
- **依赖就绪**：全部前置 `SUCCEEDED` 后，任务转入 `PENDING` 进入正常调度，仍遵守设备并发上限、资源互斥与按提交序号的 FIFO 顺序。
- **级联终结**：任一前置进入 `FAILED` / `CANCELLED` / `SKIPPED`，本任务立即转为 **`SKIPPED`**（终态），`failure_reason=DEPENDENCY_FAILED`，`attempts=0`——与自身执行失败（`DEVICE_REJECTED` / `TIMEOUT` / `INTERNAL_ERROR`）明确区分。级联在同一轮推进内沿依赖链传播到不动点。
- **依赖自洽性**（下发时校验，可区分拒绝）：
  - 自己依赖自己 → `SELF_DEPENDENCY`（HTTP 400）
  - 依赖成环（含多任务绕环）→ `DEPENDENCY_CYCLE`（HTTP 400）
- **级联取消/处置**：`cancel(task_id, cascade=True)` 或 `intervene(task_id, "abort", cascade=True)` 会把全部下游（传递依赖它的）未结束任务一并 `CANCELLED`（`CANCELLED_BY_USER`）；不带 `cascade` 时行为与原来一致，剩下的等待中的下游由依赖规则级联 `SKIPPED`。

## 并发与互斥规则

- 每台设备有 `max_concurrency`，每个资源有 `capacity`，调度时原子占用，绝不超额。
- 调度扫描严格按提交序号 `created_seq` 升序（FIFO），同一状态下排队/运行/失败的先后结果确定且可复现。
- 设备槽或资源不足时任务进入 `QUEUED` 并记录原因，释放后按 FIFO 补位。
- 依赖就绪的任务与无依赖任务在同一 FIFO 序列中公平竞争设备槽与资源，加依赖不会造成超额并发或乱序。

## 重试与失败

- 失败原因可区分：`DEVICE_REJECTED`（设备拒绝）、`TIMEOUT`（超时）、`INTERNAL_ERROR`（内部错误）、`CANCELLED_BY_USER`（人工取消）、`DEPENDENCY_FAILED`（前置未成功，从未执行）。
- 重试受 `max_attempts` 与指数退避（`backoff_seconds * 2^(n-1)`）约束；退避期间任务处于 `WAITING_RETRY`，到期后重新排队。
- 部分成功后失败不会造成重复执行或结果丢失：结果中记录累计尝试次数，重试耗尽进入 `FAILED`。
- 人工干预：`intervene(task_id, "retry")` 重置计数重新排队；`"abort"` 终止未结束任务（可带 `cascade=True` 连带下游）。

## 幂等与可区分的拒绝

- 同一 `task_id` 重复提交返回既有记录，不重复执行（含并发提交）；带依赖的提交同样幂等。
- 拒绝原因码：`UNKNOWN_TASK` / `UNKNOWN_DEVICE` / `UNKNOWN_RESOURCE` / `ILLEGAL_TRANSITION` / `TASK_ALREADY_FINISHED` / `SELF_DEPENDENCY` / `DEPENDENCY_CYCLE`。
- HTTP 映射：404（未知任务）、400（未知设备/资源、自依赖、依赖环）、409（非法流转/已结束）。

## 兼容范围

不带 `depends_on` 的任务行为与之前完全一致：状态机、调度顺序、重试退避、取消与人工干预语义均未改变；`cancel` / `intervene` 的 `cascade` 参数缺省为 `False`，原有调用方式不受影响。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 下发任务（幂等），可选 `depends_on: [task_id, ...]` |
| GET | `/tasks/{task_id}` | 查询任务（含 `depends_on`、`waiting_on` 与流转历史） |
| POST | `/tasks/{task_id}/cancel?cascade=true` | 取消任务，可选级联带走下游 |
| POST | `/tasks/{task_id}/intervene` | 人工干预 `{"action": "retry"\|"abort", "cascade": false}` |

## 验证方法

```bash
uv run pytest tests/ -q        # 全部测试
uv run pytest tests/ -q -s     # 查看依赖判定、调度决策与状态流转日志
```

测试覆盖：

- `test_dependencies.py` — 依赖链按序跑通、等待态可查询、前置失败/取消后的级联 `SKIPPED`、自依赖与多节点环拒绝、依赖就绪后仍守设备并发/资源互斥与 FIFO、级联取消与 abort、多线程并发带依赖提交不超额且幂等
- `test_http_dependencies.py` — HTTP 入口与本地语义一致：依赖下发/等待可见/环拒绝 400/级联取消/幂等
- `test_concurrency.py` — 并发抢用设备/资源、FIFO 确定性、多线程下不超额并发
- `test_submit_and_query.py` — 重复提交幂等（含 8 线程并发提交）、未知对象拒绝
- `test_retry_and_failure.py` — 失败原因可区分、退避重试、重试耗尽、人工干预恢复
- `test_cancel.py` — 排队/待重试任务取消、已结束任务取消拒绝、非法流转拒绝

单测日志（`log_cli` 已开启）会打印依赖判定、状态流转与调度决策依据，例如：

```
依赖判定: task=b 保持 WAITING_DEPENDENCY, 等待前置 a(WAITING_RETRY)
状态流转: task=b WAITING_DEPENDENCY -> PENDING (全部前置已成功，进入调度)
状态流转: task=c WAITING_DEPENDENCY -> SKIPPED (前置未成功: b，级联终结（未执行）)
调度决策: task=t2 保持 QUEUED, 原因=device robot-1 并发已满 (1/1)
```
