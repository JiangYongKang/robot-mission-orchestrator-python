# Robot Mission Orchestrator

基于本地状态模拟多设备与任务队列的机器人任务编排服务，保证并发调度、任务取消与故障重试在各入口（Python API / HTTP）下结果一致且可解释。

## 结构

- `orchestrator/models.py` — 任务/设备/资源模型与状态机定义
- `orchestrator/store.py` — 内存状态存储（单锁保证并发一致）
- `orchestrator/scheduler.py` — FIFO 确定性调度：设备并发上限 + 资源互斥
- `orchestrator/dependencies.py` — 任务依赖：环检测、就绪判定、级联终结
- `orchestrator/executor.py` — 本地模拟执行：故障注入、退避重试
- `orchestrator/service.py` — 统一入口：下发（幂等）/查询/取消/人工干预
- `main.py` — FastAPI HTTP 入口

## 任务状态流转

```
PENDING ──> QUEUED ──> RUNNING ──> SUCCEEDED（终态）
   │          │           ├──> WAITING_RETRY ──> QUEUED（退避到期后重新排队）
   │          │           └──> FAILED（终态，重试耗尽）
   └──> CANCELLED（终态，任意非终态可取消）
WAITING_DEPS ──(全部前置 SUCCEEDED)──> PENDING
WAITING_DEPS ──(前置 FAILED)──> FAILED（DEPENDENCY_FAILED，未执行）
WAITING_DEPS ──(前置 CANCELLED)──> CANCELLED（DEPENDENCY_CANCELLED，未执行）
FAILED ──(人工干预 retry)──> PENDING / WAITING_DEPS（取决于前置是否就绪）
```

合法流转由 `ALLOWED_TRANSITIONS` 唯一约束；非法流转抛出 `ILLEGAL_TRANSITION`。

## 任务依赖

- 下发时通过 `depends_on` 声明前置任务 id 列表；允许引用尚未下发的任务（前向引用），该前置下发并成功前任务一直等待。
- 带依赖的任务先停在 `WAITING_DEPS`：这是独立的等待状态，**不参与**设备并发槽与资源排队，与 `QUEUED`（等设备/资源）明确区分。查询返回 `depends_on` 与 `dependency_status`（每个前置的当前状态/未下发），可解释「在等谁、为什么还没轮到」。
- 全部前置 `SUCCEEDED` 后，依赖闸门把任务提升为 `PENDING`，进入既有 FIFO 调度——设备并发上限、资源互斥与提交先后顺序完全照旧。
- 依赖关系自洽性在下发时校验：自依赖与任意长度的环都被拒绝，原因码 `DEPENDENCY_CYCLE`（HTTP 400），任务不落库。

### 级联规则

- 前置 `FAILED` → 下游任务级联 `FAILED`，原因 `DEPENDENCY_FAILED`（未执行，`attempts == 0`）。
- 前置 `CANCELLED` → 下游任务级联 `CANCELLED`，原因 `DEPENDENCY_CANCELLED`。
- 级联沿依赖链传递（A 失败 → B 级联失败 → C 随 B 级联失败），在每次推进（pump）时由依赖闸门评估到不动点。
- 「前置没成功所以没执行」（`DEPENDENCY_FAILED` / `DEPENDENCY_CANCELLED`）与「自己执行时设备拒绝/超时/内部出错」（`DEVICE_REJECTED` / `TIMEOUT` / `INTERNAL_ERROR`）是互斥且可区分的原因码。
- `cancel(task_id, cascade=True)` 与 `intervene(task_id, "abort", cascade=True)` 可在同一次调用里把全部（传递）下游未结束任务一并取消；不带 `cascade` 时只终结本任务，下游由上述既定规则在下一次推进时级联终结。
- 被级联终结的下游任务可通过 `intervene(task_id, "retry")` 恢复：前置此时仍未全部成功则回到 `WAITING_DEPS`，否则直接进入调度。

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

- 同一 `task_id` 重复提交返回既有记录，不重复执行（含并发提交、含带依赖的任务）。
- 拒绝原因码：`UNKNOWN_TASK` / `UNKNOWN_DEVICE` / `UNKNOWN_RESOURCE` / `ILLEGAL_TRANSITION` / `TASK_ALREADY_FINISHED`（对已结束任务的取消）/ `DEPENDENCY_CYCLE`（自依赖或依赖成环）。
- HTTP 映射：404（未知任务）、400（未知设备/资源、依赖成环）、409（非法流转/已结束）。
- 本地调用与 HTTP 入口走同一个 `OrchestratorService`，同一操作的返回与原因码一致。

## 兼容性

不带 `depends_on` 的任务行为与加入依赖编排之前完全一致：状态机、调度顺序、重试退避、取消与人工干预语义均未改变（原有测试套件全部不回归）。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 下发任务（幂等），可带 `depends_on: [task_id, ...]` |
| GET | `/tasks/{task_id}` | 查询任务（含 `depends_on`、`dependency_status` 与流转历史） |
| POST | `/tasks/{task_id}/cancel` | 取消任务，`?cascade=true` 连带取消下游 |
| POST | `/tasks/{task_id}/intervene` | 人工干预 `{"action": "retry"\|"abort", "cascade": false}` |

## 验证方法

```bash
uv run pytest tests/ -q        # 全部测试
uv run pytest tests/ -q -s     # 查看任务流转与调度决策日志
```

测试覆盖：

- `test_dependencies.py` — 依赖链按序执行、等待状态可查询、前向引用、前置失败/取消的级联（含传递）、级联取消、环与自依赖拒绝、依赖与设备并发/资源互斥叠加、带依赖的幂等提交
- `test_http_api.py` — HTTP 与本地入口一致性：依赖下发/查询、环拒绝原因码、级联取消、幂等
- `test_concurrency.py` — 并发抢用设备/资源、FIFO 确定性、多线程下不超额并发
- `test_submit_and_query.py` — 重复提交幂等（含 8 线程并发提交）、未知对象拒绝
- `test_retry_and_failure.py` — 失败原因可区分、退避重试、重试耗尽、人工干预恢复
- `test_cancel.py` — 排队/待重试任务取消、已结束任务取消拒绝、非法流转拒绝

单测日志（`log_cli` 已开启）会打印依赖判定、调度决策与每次状态流转的依据，例如：

```
依赖判定: task=b 保持 WAITING_DEPS, 等待前置: a(WAITING_RETRY)
依赖判定: task=b 全部前置成功 -> PENDING
级联终结: task=b -> FAILED (前置 a 失败(DEVICE_REJECTED)，级联终结，未执行)
调度决策: task=t2 保持 QUEUED, 原因=device robot-1 并发已满 (1/1)
状态流转: task=t1 WAITING_RETRY -> QUEUED (退避到期，重新排队)
```
