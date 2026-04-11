# Pair_arb 04-10 收敛计划（采纳 `PLAN_Claude` 主结论，补上 90s 风险开窗）

## Summary
基于 `logs/polymarket.log.2026-04-10` 和 `docs/PLAN_Claude.md`，当前主矛盾已经明确为三件事：

1. `pairing` 腿存在真实的 `cross-book reject storm`，当前 `extra_ticks` 机制不足以纠正同价反复重试  
2. venue 的 `matched orders` 资金占用没有被本地 headroom 预检正确覆盖，导致 live 中直接吃 `balance/allowance` 拒绝  
3. 策略在已经完成一批配对、几乎回到 flat 后，仍会在 round 尾段重新开 `risk-increasing` 仓位，导致已锁利润被新残仓吃掉

本轮固定方向：
- 保留 `pair_arb / maker-only / buy-only / pair-cost-first`
- 保留 `working inventory` 作为策略脑
- 保留当前 `VWAP ceiling` 与 `residual-only safety margin`
- 不引入 `settled-only`、`fragile` 第二套主脑
- 不采用 `High bucket hard ban`
- 新增 **PairArb 专属 reject-price 重试** 和 **最后 90s 只允许 pairing/reducing**

## Key Changes
### 1. 执行层：`cross reject` 从“看 book”改成“看上次被拒价格”
给 `pair_arb` 的每个 BUY slot 新增最小状态：
- `last_cross_rejected_action_price: Option<f64>`

固定规则：
- 仅对 `StrategyKind::PairArb + BUY + Provide` 生效
- 当 slot 因 `order crosses book` / `cross-book-transient` 被拒时：
  - 记录本次实际提交的 `action_price`
  - 保留当前 `slot_pair_arb_cross_reject_extra_ticks += 1`
- 下一次 place/reprice 时：
  - `action_price = min(strategic_target, best_ask - base_margin - extra_ticks*tick, last_cross_rejected_action_price - tick)`
- 一旦该 slot 成功 `OrderPlaced`：
  - 清空 `last_cross_rejected_action_price`
  - 清零 `slot_pair_arb_cross_reject_extra_ticks`

固定语义：
- `strategic_target` 不变
- 修的是 `action_price`
- 不再允许同一个 slot 连续提交同一个已被证明会 cross 的价格

### 2. 执行层：余额预检改成“raw balance - 本地未释放 matched 占用”
保留当前 `cached_free_balance_usdc()`，但不再直接把它当最终 headroom。

新增本地 headroom 口径：
- `effective_free_usdc = cached_free_balance_usdc - local_unreleased_matched_notional_usdc`

其中 `local_unreleased_matched_notional_usdc` 固定来自本地 inventory/OMS 可计算的未释放买入占用：
- 已 matched 但尚未被 merge/claim/release 消化的 collateral
- 不重复计入已被 merge materialize 并结算释放的部分

固定规则：
- `PairArb` BUY 在 pre-place 时使用 `effective_free_usdc`
- 若 `effective_free_usdc + cushion < required_usdc`：
  - 不真正发单
  - 记 `pair_arb_headroom_blocked`
  - 走本地 reject/cooldown，而不是交给 venue 返回 `not enough balance / allowance`
- 不改变当前 recycler/headroom merge 触发机制，只修 pre-place 判断口径

目标：
- 不再出现 `04-10` 第一轮里那种“本地以为能下，venue 说 matched orders 已占满”的 reject

### 3. 策略层：给 `risk-increasing` 加 90s 风险开窗
不做“锁利多少就停”的 profit latch。  
采用你确认的更简洁方案：

- 对 `BTC 15m / PairArb`
- 最后 `90s`
- `risk-increasing` 候选全部 suppress
- `pairing / reducing` 候选继续允许
- 当前最后 `45s` 的 `SoftClose` 逻辑继续保留，但 `PairArb` 会更早进入“只配对”窗口

固定语义：
- 这不是 endgame 第二套状态机
- 只是 PairArb 的单条策略约束
- 作用是阻止 `04-10` 第二轮那种：已实现大量 pair edge 后，又在剩余几分钟里重新积出 `10-15` 份单腿

默认实现：
- 先固化为内部常量 `PAIR_ARB_RISK_OPEN_CUTOFF_SECS = 90`
- 本轮不新增 env 参数

### 4. 保留当前已正确的部分，不再重做
以下行为继续保留，不纳入本轮重写：
- `working inventory` 作为策略脑
- `Matched/Failed` 立即影响报价
- `merge` 继续消费 `settled + pending`，不再留下 phantom residual
- `state_key = { dominant_side, net_bucket, soft_close_active }` 仍是换挡信号
- stale target drop 继续保留
- `VWAP ceiling` 继续保留
- `pair_cost_safety_margin` 继续只在 residual 状态生效
- tier `0.70 / 0.30` 继续保留
- 不引入 `High bucket hard ban`

### 5. 清理死路径与观测
把当前已经退出主逻辑、但仍在仓库里制造噪音的壳清掉：
- 删除 `RoundSuitability` 的运行时采样/决策代码与对应测试
- 不再让日志/测试继续暗示 `SkippedImbalanced` 是 PairArb 的一部分

新增最小观测：
- `pair_arb_cross_reject_last_price`
- `pair_arb_retry_action_price`
- `pair_arb_cross_reject_repriced=true|false`
- `pair_arb_headroom_blocked`
- `pair_arb_risk_open_cutoff_blocked`

要求下一轮 live 能直接回答：
- 这次 cross reject 后是否真的按被拒价格步降
- 这次没下单是 venue 拒绝，还是本地 headroom 主动拦住
- 最后 90s 是否还有新的 `risk-increasing` 开仓

### 6. Live canary 默认参数
为了让风险收益比至少先回到可验证区间，本轮 canary 默认值固定为：
- `PM_BID_SIZE=5`
- `PM_PAIR_TARGET=0.97`
- `PM_PAIR_ARB_TIER_1_MULT=0.70`
- `PM_PAIR_ARB_TIER_2_MULT=0.30`
- `PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN=0.02`
- **`PM_MAX_NET_DIFF=10`**

理由固定：
- `PLAN_Claude` 已证明在 `max_net_diff=15` 下，当前 round 内已锁 pair edge 覆盖不了尾部残仓 worst-case
- 本轮先不改 tier，不改 pair_target，只把尾部最大暴露先压回 `10`

## Test Plan
1. **Cross reject 步降**
- 构造 `PairArb` BUY 被 `order crosses book` 连续拒绝
- 下一次 `action_price` 必须满足 `<= last_rejected_action_price - tick`
- 不允许连续重复同一 rejected 价格
- 成功 `OrderPlaced` 后清空 sticky state

2. **Headroom 预检**
- 构造 `cached_free_balance` 足够，但本地 `matched/unreleased` 占用后其实不足的场景
- 本地必须直接 block，不再把请求交给 venue 再吃 `balance/allowance`
- merge/release 后，block 自动解除

3. **90s 风险开窗**
- `T-91s`：`risk-increasing` 仍允许
- `T-90s` 之后：`risk-increasing` 必须 suppress
- `pairing/reducing` 在 `T-90s` 之后继续允许
- 不新增新的 HardClose/taker 行为

4. **merge/pending 回归**
- 保持当前 `04-10` merge 场景回归：
  - merge 后 `pending=0`
  - `working == settled`
  - 不再出现 phantom residual

5. **状态机回归**
- `state_key` 穿越 `5 / 10 / dominant_side flip / soft_close_active`
- 旧单仍然必须重评
- stale target 仍然必须被 drop
- 不恢复连续 tick-drift reprice

6. **Canary live 验收**
- 先跑 `BTC 15m` 小额 live `3` 轮，参数按上面的 canary 默认值
- 验收条件固定为：
  - 不再出现同一 slot 连续同价 `cross reject` storm
  - 最后 `90s` 没有新的 `risk-increasing` fill
  - `balance/allowance` venue reject 显著下降，优先变成本地 `headroom_blocked`
  - `median(round_end residual_qty) <= 5`
  - `median(realized_pair_locked_pnl + worst_case_outcome_pnl) >= 0`

## Assumptions
- `PLAN_Claude` 关于三点的判断成立：
  - merge/pending 在这份日志里不是主凶
  - 配对后重新开成大残仓是主凶
  - cross reject / balance reject 是真实执行伤害
- 本轮不做 profit latch；采用你确认的 `90s` 时间门
- 本轮不把 `pair_arb` 改成 settled-only
- 本轮不简化 `VWAP ceiling`；若修完后仍证明它系统性拖慢 pairing，再单独讨论
- 本轮不采用 `High bucket hard ban`
- 本轮默认 live canary 先把 `PM_MAX_NET_DIFF` 从 `15` 收到 `10`，其余主参数保持不变
