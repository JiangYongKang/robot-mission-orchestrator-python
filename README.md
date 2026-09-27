# robot-mission-orchestrator-python

基于 FastAPI 的机器人任务编排服务。用本地状态模拟多台设备与任务队列，
保证并发调度、任务取消与故障重试在 HTTP 入口与服务入口下结果一致且可解释。

## 结构

- `orchestrator/models.py` — 任务/设备/执行结果模型与状态机定义
- `orchestrator/errors.py` — 可区分的错误原因码与领域异常
- `orchestrator/service.py` — 编排核心：生命周期、调度、重试（唯一事实来源）
- `main.py` — HTTP 入口，把领域操作暴露为 REST API
- `tests/` — 并发抢用、重复提交、中途失败场景测试

## 任务状态流转

```
PENDING --受理--> QUEUED --调度放行--> RUNNING --成功--> SUCCEEDED (终态)
                    |                    |
                    |                    +--失败且可重试--> QUEUED (退避后重排)
                    |                    +--失败且重试耗尽--> FAILED (终态)
                    |                    +--取消--> CANCELED (终态)
                    +--取消--> CANCELED
FAILED --人工干预 retry--> QUEUED
```

- 状态只能沿上表流转，非法流转抛 `ILLEGAL_STATE_TRANSITION`。
- 终态（SUCCEEDED/FAILED/CANCELED）上的取消抛 `MISSION_ALREADY_FINISHED`，
  与非法流转可区分。
- 每次流转与调度决策都写入决策日志（`GET /decision-log` 或服务端
  `decision_log()`），含时间戳与原因。

## 幂等与错误原因

- `mission_id` 由调用方提供，作为幂等键：同 id 同内容重复提交返回既有任务，
  不重复执行；同 id 不同内容返回 `409 DUPLICATE_MISSION_CONFLICT`。
- 错误原因码：`MISSION_NOT_FOUND` / `DEVICE_NOT_FOUND` /
  `DUPLICATE_MISSION_CONFLICT` / `ILLEGAL_STATE_TRANSITION` /
  `MISSION_ALREADY_FINISHED`；任务失败原因：`DEVICE_REJECTED`（设备拒绝）/
  `DEVICE_TIMEOUT`（超时）/ `INTERNAL_ERROR`（内部错误，含驱动异常）。

## 并发与互斥规则

- 每台设备有 `max_concurrency` 上限；命名资源（如充电桩 `dock-a`）全局互斥。
- 调度器在单锁内按提交顺序 FIFO 扫描队列：设备有空闲槽位且所需资源全部
  空闲才放行，否则继续排队。决策确定、可复现，日志记录每一次放行/阻塞依据。
- 取消运行中任务会立即释放设备槽位与资源；执行线程迟到的结果被丢弃，
  以取消状态为准。

## 重试与人工干预

- 失败后若 `retries_used < max_retries`，自动重新排队，退避
  `base_backoff * 2^(retries_used-1)`（指数退避，时钟可注入）。
- 重试耗尽进入 FAILED，`error_reason` 区分设备拒绝/超时/内部错误。
- 每次尝试追加到 `attempts` 历史（只增不改），部分成功后失败不会重复
  执行或丢失历史；成功结果写入 `result`。
- FAILED 任务可经 `POST /missions/{id}/retry` 人工干预重新排队，
  自动重试预算清零。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/missions` | 下发任务（幂等），201 |
| GET | `/missions` | 按提交顺序列出任务 |
| GET | `/missions/{id}` | 查询任务，404 不存在 |
| POST | `/missions/{id}/cancel` | 取消，已结束返回 409 |
| POST | `/missions/{id}/retry` | 人工重试 FAILED 任务 |
| GET | `/decision-log` | 调度决策与流转日志 |

错误响应体为 `{"detail": {"reason": <原因码>, "message": ...}}`。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务（127.0.0.1，随机端口）
uv run pytest -q        # 运行全部测试
```

测试覆盖：设备并发上限与 FIFO 顺序、跨设备资源互斥、取消释放槽位、
重复提交幂等/冲突、取消已结束任务、退避重试与耗尽、三类失败原因区分、
人工干预恢复、部分失败历史保留。单测日志（`pytest -s` 或决策日志断言）
打印每次状态流转与调度决策依据。测试使用假时钟与可脚本化驱动，
结果确定可复现。
