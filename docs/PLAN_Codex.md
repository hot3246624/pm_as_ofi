# Oracle-Lag Cross-Market Arbiter V2

## Summary
在保留 `oracle_lag_sniping` 核心交易哲学不变的前提下，引入“跨市场单轮单选”仲裁层，但只把它作为**市场选择器**，不把它和价格阈值、单笔 all-in sizing、通用 `WinnerHint` 语义混在一起。

本版计划的目标是：
- 每轮 7 个 `*-updown-5m` 市场只允许 1 个市场进入真实执行
- 选择依据使用“可成交且新鲜”的盘口，而不是裸 `winner_ask_raw`
- 保持当前 `FAK / maker` 执行规则不变，不在同一补丁里把 `0.993` 改成 `0.990`
- 不引入 `use_max_size` 的一笔吃满语义；若后续需要扩大资金利用率，改为第二阶段的预算分块执行

## 针对 `PLAN_Claude.md` 的改进说明
- 保留“跨市场 arbiter”主方向，但**不修改 `WinnerHint`**。`WinnerHint` 继续只表达市场真相；新增单独的 `ArbiterDecision` 作为控制面消息。
- 不再按 `winner_ask_raw` 排序。改为使用 `winner_ask_eff + winner_ask_tradable + book_age_ms` 的组合评分，避免比较不同年龄、不同可成交性的书。
- `collection_window_ms=300` 继续采用，但语义改为“300ms 后对已到齐市场做裁决”，不要求 7 市场全部到齐。
- 不采纳 `use_max_size=true` 的一笔 all-in 方案。当前版本只做单轮单选，继续使用现有 `bid_size` 和已有 FAK re-entry / maker 循环；预算扩张单独二阶段实现。
- 不把 `ORACLE_LAG_NO_TAKER_ABOVE_PRICE` 从 `0.993` 改成 `0.990`。阈值和架构拆开验证，避免收益归因混乱。
- `cross_market_suppressed` 不使用裸 `bool`。所有 suppress/selection 必须带 `round_end_ts`，防止跨轮泄漏。

## Key Changes
### 1. 新增独立控制面：`ArbiterObservation` 和 `ArbiterDecision`
- 在 `src/bin/polymarket_v2.rs` 内新增内部结构：
  - `ArbiterObservation`
    - `round_end_ts`
    - `slug`
    - `winner_side`
    - `winner_bid`
    - `winner_ask_raw`
    - `winner_ask_eff`
    - `winner_ask_tradable`
    - `book_age_ms`
    - `detect_ms`
    - `hint_msg`
    - `hint_tx`
  - `ArbiterDecision`
    - `round_end_ts`
    - `slug`
    - `selected: bool`
    - `rank: u8`
    - `reason: &'static str`
- `MarketDataMsg::WinnerHint` 保持现状，不新增 `cross_market_selected/use_max_size` 字段。
- 新增一个独立 `MarketDataMsg::OracleLagSelection`，由 arbiter 发给各市场 coordinator。

### 2. 新增 `run_cross_market_hint_arbiter`
- arbiter 运行在 supervisor 层，位于各市场 `run_post_close_winner_hint_listener` 和各自 coordinator 之间。
- 输入：各市场发来的 `ArbiterObservation`
- 分批键：`round_end_ts`
- 收集规则：
  - 第一条 observation 到达后启动 `300ms` 窗口
  - 窗口结束立即裁决，不等待慢市场
  - 超窗未到的市场本轮视为 `missing_observation`
- 评分规则固定为：
  - 先过滤 `winner_ask_tradable=false` 且 `winner_ask_raw>0` 的市场，降为最低优先级
  - 可成交市场优先级：
    1. `winner_ask_eff` 越低越优
    2. 同 ask 时 `winner_bid` 越高越优
    3. 同分时 `book_age_ms` 越小越优
    4. 再同分时 `detect_ms` 越早越优
  - `winner_ask_raw=0` 的市场不直接判死，但排在所有有可成交 ask 的市场之后
  - `book_age_ms > 250` 的 observation 记为 stale；若本轮存在非 stale 候选，则 stale 候选不得当选
- 裁决结果：
  - 仅 rank=1 市场收到 `OracleLagSelection { selected=true }`
  - 其余收到 `selected=false`
  - 若本轮无合格候选，则所有市场都收到 `selected=false`

### 3. Coordinator 只读“当前轮选择结果”，不改 WinnerHint 语义
- 在 `src/polymarket/coordinator.rs` 增加轮次作用域状态：
  - `oracle_lag_selected_round_end_ts: Option<u64>`
  - `oracle_lag_is_selected: bool`
- `OracleLagSelection` 到来时：
  - 仅当 `round_end_ts >= current_round_end_ts` 时更新
  - 新 round 到来必须覆盖旧 round 选择状态
- `post_close_chainlink_winner()` 不再仅凭 `WinnerHint` 放行；还需满足：
  - 当前在 post-close window 内
  - 存在匹配当前 round 的 `WinnerHint`
  - 当前 round 的 `OracleLagSelection.selected == true`
- 未被选中的市场：
  - FAK 首发静默
  - FAK re-entry 静默
  - `post_close_hype::compute_quotes()` 返回空
- 被选中的市场：
  - 保持现有 FAK/maker 规则
  - 保持现有 `bid_size`
  - 保持现有 `0.993` taker 阈值

### 4. `post_close_hype` 只做策略报价，不承载仲裁
- `src/polymarket/strategy/post_close_hype.rs` 开头新增守卫：
  - 若当前 round 未被 arbiter 选中，直接返回空 quotes
- `no ask` 分支仍保留现有 maker 行为，但 ceiling 改为：
  - `min(0.991, 1.0 - tick)`
- `ask >= 0.993` 的 maker 分支保持现状
- 不新增新的 “best market” 逻辑到策略层，策略层只消费 coordinator 给出的选择结果

### 5. 预算与 sizing 明确分两阶段
- 本次实现范围：
  - **只做单轮单市场选择**
  - 不改 `bid_size`
  - 不加 `use_max_size`
  - 不在 coordinator/executor 间增加“free USDC 直传 sizing”
- 二阶段预留方向：
  - 若一阶段稳定，再新增 `oracle_lag_round_budget_usdc`
  - 执行方式不是单笔 all-in，而是复用现有 FAK re-entry / maker 循环做预算分块
- 理由固定：
  - 当前 executor 的余额口径在 executor 内部，直接 coordinator all-in 风险高
  - 目标地址行为也更接近“连续多笔/长挂”而不是单笔吃满

### 6. 配置
- 新增最小配置到 `OracleLagSnipingStrategyConfig`：
  - `cross_market_arbiter_enabled: bool = true`
  - `arbiter_collection_window_ms: u64 = 300`
  - `arbiter_book_max_age_ms: u64 = 250`
- 本轮不新增预算参数
- 本轮不修改：
  - `PM_POST_CLOSE_WINDOW_SECS`
  - `ORACLE_LAG_NO_TAKER_ABOVE_PRICE`
  - `PM_BID_SIZE`

## Test Plan
1. **仲裁窗口**
- 7 市场同轮 hint 在 `<=300ms` 内大多到齐时，arbiter 只选 1 个市场
- 有市场超过 `300ms` 才到时，本轮不等待，迟到市场直接 miss

2. **排序正确性**
- 多市场都有 ask 时：
  - 选择 `winner_ask_eff` 最低者
- ask 相同：
  - 选择 `winner_bid` 更高者
- stale book 与 fresh book 同时存在：
  - fresh book 优先

3. **作用域正确**
- 未被选中市场：
  - 不产生 `oracle_lag_submit_latency`
  - 不产生 `dry_taker_preview`
  - `post_close_hype` 不报单
- 被选中市场：
  - 继续按现有路径下单

4. **不改阈值回归**
- `winner_ask < 0.993`：仍走 FAK
- `winner_ask >= 0.993`：仍走 maker
- 本次改动不改变单市场既有行为，只改变“本轮谁有资格执行”

5. **跨轮安全**
- 上一轮 `selected=false` 不得泄漏到下一轮
- 新 round 的 `WinnerHint` 和 `OracleLagSelection` 必须按 `round_end_ts` 对齐

6. **Dry-run 验收**
- 连续 5 轮 dry-run：
  - 每轮最多 1 个市场出现真实下单日志
  - 其余市场出现 `oracle_lag_cross_market_skip`
  - 选中市场与该轮 CSV 中最低 `winner_ask_eff` 一致

## Assumptions
- 当前第一目标是“把 7 市场机会集中到 1 个市场”，不是先追求 all-in 资本利用率
- 当前 `0.993` 阈值、`bid_size`、FAK re-entry、maker 续挂逻辑先视为已独立可验证，不与 arbiter 混改
- `winner_ask_raw` 本身不足以当跨市场裁决真相，必须和 `tradable/freshness` 一起使用
- 若一阶段稳定，再单独进入二阶段：预算分块执行；那是收益优化，不是本轮架构修复
