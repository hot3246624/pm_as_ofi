# Oracle Lag Sniping 热路径收敛计划（消除业务上不应存在的串行瓶颈）

## Summary
当前系统的主问题不是赢家判定错误，而是**判定之后的热路径仍被验证逻辑、队列消费和伪“evidence tape”串行化**。  
本轮固定方向：

- 保留 `Chainlink RTDS` 作为唯一交易判定源
- 保留前端接口、REST、CSV 作为验证/观测源
- **把交易热路径收敛为：`final exact -> 读取最新盘口状态 -> 立即决策 -> 立即发单/hold`**
- 移除所有不该阻塞这条链路的等待和队列式旧数据消费
- 不再把“诊断证据”混进“执行价格来源”

目标是把当前 `detect->emit` 的 `p95≈408ms` 压到接近零级别的内部开销，只剩网络和 venue 本身的真实时延。

## Key Changes
### 1. 热路径拆分为 `Decision Path` 与 `Validation Path`
在 [`polymarket_v2.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/bin/polymarket_v2.rs) 将 `run_post_close_winner_hint_listener` 拆成两条职责完全分离的路径：

- `Decision Path`
  - 只负责：
    - 等 `Chainlink final exact`
    - 读取该市场当前最新盘口状态
    - 构造并发送 `WinnerHint`
  - 不允许等待：
    - `frontend_task`
    - REST polling 结果
    - 诊断日志拼装
    - 任何 tail/arbiter 排名逻辑
- `Validation Path`
  - 继续异步采集：
    - frontend open/close
    - REST 顶层盘口
    - 盘后观测表格/CSV
  - 仅做日志、CSV、核验，不得阻塞 `WinnerHint` 发送

固定要求：
- `post_close_emit_winner_hint` 的时间戳必须直接对应 `Decision Path` 完成时刻
- `frontend_*` 字段若未及时返回，允许为 `None`，后续由独立日志/CSV 补齐
- 以后不允许再出现“前端验证比交易更早级”的实现

### 2. 用“最新盘口状态”替代当前 queue-based evidence tape
当前的 `post_close_book_tx -> post_close_book_rx -> PostCloseBookEvidenceTape` 退出交易热路径。

改为新增单一共享运行态，例如：
- `PostCloseLiveBookState`
  - `yes_bid / yes_ask / yes_recv_ms`
  - `no_bid / no_ask / no_recv_ms`
  - `source`
- 写入点仍在 market WS 解析处
- 读取点在 `Chainlink final` 到达后的 hot path

固定语义：
- 热路径只读**当前最新值**
- 不再通过 `mpsc` 排队消费盘后 partial book
- 不再允许 `try_send` 静默丢包影响交易决策
- 如果需要保留“证据带”，它只能作为诊断 ring buffer，不能再当执行真相

直接结果：
- `final` 到达时使用的是“当前最新盘口”，不是“最后一次碰巧被 queue 消费到的盘口”
- `distance_to_final_ms` 从交易语义里降级为纯诊断字段

### 3. 执行价格来源对齐：统一使用 `latest live book`
当前 `WinnerHint` 已携带 `winner_bid/winner_ask_raw`，但这些值来自 listener 内部的旧证据选择。  
本轮固定改为：

- `WinnerHint` 继续携带：
  - `side`
  - `source`
  - `ref_price`
  - `observed_price`
  - `open_is_exact`
- `winner_bid / winner_ask_raw / distance_to_final_ms / book_source`
  改为诊断字段，不再作为下单执行的权威输入
- Coordinator 在收到 `WinnerHint` 后，立即基于当前共享 `live book` 重新计算：
  - `winner_bid`
  - `winner_ask`
  - `winner_ask_tradable`
  - `winner_spread_ticks`

这样统一后：
- “判定胜负”与“用哪一刻的盘口下单”不会再分裂
- 不再出现 `hint` 说看到 `0.09`，执行层却拿的是别的旧状态，或反过来

### 4. 移除热路径里的隐性串行点
固定清理以下瓶颈：

- `frontend_task.take()` 之后的 `350ms timeout` 不得出现在 emit 前
- `rest_interval.tick()` 不得再阻塞 `Chainlink final` 处理
- `post_close_round_tail_coordinator` 和任何跨市场排序逻辑，不得影响单市场 final 到达后的即时 FAK 判断
- `WinnerHint` 发送链路上，只允许保留：
  - `final exact`
  - `latest live book`
  - `coordinator immediate decision`

允许保留但必须降级为异步背景任务：
- frontend round fetch
- REST top-of-book sampling
- 证据 CSV 汇总
- tail fallback 统计
- 任何后验分析字段

### 5. 盘口诊断体系重做，但仅用于验证
保留盘后证据分析能力，但明确从交易路径剥离：

- 保留一个小型 ring buffer 记录最近 N 条盘后 partial book 更新
- CSV/日志中继续输出：
  - `slug`
  - `open_ref`
  - `final close`
  - `winner`
  - `final_detect_ms`
  - `decision_emit_ms`
  - `decision_submit_ms`
  - `latest_live_book_at_decision`
  - `diagnostic_ws_snapshot`
  - `diagnostic_rest_snapshot`
- 如果 `latest live book` 与诊断 snapshot 不一致，直接记为架构/调度问题，不允许静默吞掉

重点要求：
- 表格里必须能区分：
  - “交易用的盘口”
  - “诊断看到的最近证据”
- 不再混用一个 `distance_to_final_ms` 当真相

### 6. 运行时观测升级为阶段级 SLA
新增分阶段延迟指标：

- `final_detect_ms`
- `winner_hint_emit_ms`
- `winner_hint_recv_ms`
- `decision_book_recv_ms`
- `decision_ms`
- `submit_ms`

并输出派生指标：
- `detect_to_emit_ms`
- `emit_to_recv_ms`
- `recv_to_decision_ms`
- `decision_to_submit_ms`
- `final_to_submit_ms`
- `book_age_at_decision_ms`

固定验收口径：
- 不再只看 `latency_from_end_ms`
- 要能直接定位：到底慢在 oracle、listener、coordinator、还是 executor

### 7. Live readiness gate 重写
进入实盘前，dry run 至少满足：

- `open_exact=true` 覆盖率接近 100%（首轮冷启动例外单独标记）
- `detect_to_emit_ms`
  - `p50 <= 20ms`
  - `p95 <= 80ms`
- `book_age_at_decision_ms`
  - `p50 <= 50ms`
  - `p95 <= 150ms`
- 不再出现 `frontend` 或 `REST` 等待导致的 `emit` 延迟
- 若存在 `winner_ask <= 0.99` 的市场：
  - 必须在日志中看到该轮使用的是 `latest live book`
  - 且没有被旧证据覆盖
- unresolved 只允许来自真正的 RTDS/market silence，不允许来自本地架构串行等待

在这些条件之前，不进入实盘。

## Public Interfaces / Types
最小接口调整：

- `MarketDataMsg::WinnerHint`
  - 保留赢家判定语义
  - 原 `winner_bid / winner_ask_raw / winner_distance_to_final_ms / winner_book_source` 降级为诊断语义
- 新增运行态：
  - `PostCloseLiveBookState`
  - 可选 `PostCloseDiagnosticRing`
- 新增阶段级延迟日志字段：
  - `detect_to_emit_ms`
  - `emit_to_recv_ms`
  - `book_age_at_decision_ms`
  - `decision_to_submit_ms`

不新增新的交易参数，不改 winner 判定源，不改当前 FAK/hold 的高层交易策略。

## Test Plan
1. **Frontend 不再阻塞 emit**
- 模拟 frontend API 350ms-1500ms 慢响应
- `WinnerHint emit` 仍应在 `Chainlink final` 后立即发生
- `frontend_*` 字段可延后补，不得阻塞交易

2. **REST 不再阻塞 hot path**
- 模拟 REST 120ms 超时或连续失败
- `Chainlink final -> emit` 时延不受影响
- REST 只影响诊断数据，不影响交易决策

3. **最新盘口而非旧 queue 证据**
- 构造：
  - `0.09 ask` 在 `+40ms`
  - 后续还有多次 partial update
  - `final` 在 `+1200ms`
- 热路径必须使用当时最新 live snapshot，而不是最后一个碰巧被 queue 消费到的旧值

4. **无 silent drop**
- 高 burst partial book 更新下
- 不允许交易路径因为 `try_send` 满队列而退化成旧盘口
- 若诊断 ring 溢出，必须显式计数，不得影响交易决策状态

5. **Coordinator 决策对齐**
- `WinnerHint` 到达后，Coordinator 必须基于当前 live book 做一次即时判定
- 日志中同时输出：
  - `hint diagnostics`
  - `decision live book`
- 两者若不一致，必须可见

6. **SLA 验收**
- 连续 dry run 多轮
- 汇总：
  - `detect_to_emit_ms`
  - `book_age_at_decision_ms`
  - `final_to_submit_ms`
- 满足 readiness gate 后，才允许小额 live canary

## Assumptions
- 继续使用 `Chainlink RTDS` 作为唯一交易判定源
- frontend / REST 继续存在，但只做验证与观测
- 当前第一矛盾是热路径架构错误，不是交易方向逻辑错误
- 这轮不改 FAK 阈值、不改多市场交易策略本身，只修“final 后立即吃到当前盘口”的执行语义
- 当前结论是：**还不具备生产实盘条件，先修热路径串行瓶颈**
