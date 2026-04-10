# Pair_arb 全链路职责收敛计划 V2

## Summary
`pair_arb` 收敛目标不是“更简单”，而是**单一策略脑 + 单一发布制度 + 明确执行约束**。

本轮固定基线：
- 策略：`pair_arb`
- 市场：`BTC 15m`
- 风险参数：沿用当前 live `.env`，`tier1/tier2 = 0.70 / 0.30`
- 不新增绝对价格地板
- 不引入 taker / HardClose / 市价去风险

最终架构分工锁死为三层：
- **策略层**：只算连续 `strategic_target`
- **状态层**：只维护离散 `PairArbStateKey`
- **执行层**：只决定 live order 是否仍代表当前状态下的目标，并负责 post-only 落地

## Key Changes
### 1. 策略层只保留一个连续目标价
`pair_arb` 继续只算 `strategic_target`，价格链固定为：
1. A-S 基础价
2. 三段 skew
3. tier avg-cost cap（当前基线 `0.70 / 0.30`）
4. OFI subordinate shaping
5. `VWAP ceiling`
6. `safe_price`

固定语义：
- `strategic_target` 是策略层唯一权威，不直接等于交易所挂单价
- 不再在策略层引入：
  - `PairProgressRegime`
  - `fragile`
  - `state improvement reanchor`
  - `opposite-slot blocked`
- `pairing / reducing` 与 `risk-increasing` 的区别，只保留在执行层发布规则里

### 2. 状态层只保留离散状态，不再派生第二套策略语义
唯一状态键固定为：
- `dominant_side = Yes | No | Flat`
- `net_bucket = Flat | Low | Mid | High`
- `soft_close_active = bool`

状态变化触发器固定为：
- `Matched`
- `Failed rollback`
- `Merge sync`
- `SoftClose entered`
- `Round reset`

固定语义：
- `|net_diff|` 到/穿越 `5 / 10 / 0`，必定导致 `state_key` 变化
- `Merge sync` 只视为 inventory-change 事件，不再衍生额外策略模式
- `settled / working / fragile`、`PairProgressState / Stalled` 继续可保留在 accounting / diagnostics，但不再进入 `pair_arb` 主报价逻辑

### 3. 发布制度改为 band-driven，而不是 raw target-driven
为避免 `fresh target` 的细小上移或下移都在下层放大成 churn，执行层不再对 raw `strategic_target` 逐 tick 追踪。

固定规则：
- 先由策略层给出连续 `strategic_target`
- 执行层只根据 **角色 + band** 判定是否 `Republish`

角色规则固定为：

- `RiskIncreasing`
  - upward：`retain`
  - downward：仅当 `strategic_target <= live_target - 2 ticks` 时 `Republish`
  - `state_key` 变化：强制重评

- `PairingOrReducing`
  - 只要 `|strategic_target - live_target| > 3 ticks` 才 `Republish`
  - `<= 3 ticks` 统一 `retain`
  - `state_key` 变化：强制重评

这条规则是**双向带宽**，不是只管 upward。
因此：
- 小幅上移不会风暴
- 小幅下移也不会风暴
- 真正跨状态或明显偏离时才重发

### 4. post-only 只在动作时生效，不再拖着 pairing 腿追跌
执行层拆成两层价格：
- `strategic_target`
- `action_price`

固定语义：
- `strategic_target`：策略真实意图价
- `action_price`：只有在真实 `place/reprice` 时，为满足 post-only 才执行 `min(strategic_target, ask - margin)`

结果：
- pairing 腿不再因为市场继续下跌，就被长期 ask-1tick 拖着往下跑
- same-side 与 pairing 的“执行价”约束不再反向篡改“策略价”

### 5. `admissible` 收缩成纯硬约束检查
`pair_arb_quote_still_admissible()` 保留，但只检查硬约束：
- inventory limit
- `SoftClose`
- OFI suppress
- tier cap
- `VWAP ceiling`
- `simulate_buy` 有效性

移出 retain 阶段的内容：
- `utility_delta`
- `open_edge_improvement`
- “改善 locked pnl 就特许保留”之类的第二套策略逻辑

这些只属于“生成新候选”的策略层，不再属于“旧单可否保留”的执行层。

### 6. 删除 `pair_arb` 常态路径中的 opposite-slot fuse
`pair_arb_opposite_slot_blocked()` 不再参与常态 `slot_quote_allowed / reject_reason`。

固定处理：
- 该机制从主决策链中移除
- 若保留，只保留为 metrics / debug 观测
- 当前库存风险继续由：
  - tier cap
  - `VWAP ceiling`
  - `state_key`
  - `SoftClose`
  控制

理由：
- 它是执行层 emergency patch，不是优雅的策略语义
- 在你当前 `0.70 / 0.30` 基线下，继续把它放在主路径只会制造职责混乱

### 7. 文档与配置统一
统一文档和运行基线：
- `STRATEGY_PAIR_ARB_ZH.md` 改成唯一可信说明
- `CONFIG_REFERENCE_ZH.md` 与 `.env.example` 同步到当前锁定基线，避免再出现 `0.80 / 0.60` 与 live `.env` 脱节
- 文档明确写死：
  - `strategic_target` 与 `action_price` 的区别
  - `Merge sync` 只是 inventory-change，不是额外策略模式
  - `pairing` 与 `same-side` 的差异只发生在发布规则，不发生在第二套状态机里

## Test Plan
1. **双向小漂移不 churn**
- `strategic_target` 小幅上/下移动 `<= band`
- 不得触发 `Republish`
- downward 也必须验证，不能只测 upward

2. **5/10/0 状态切换**
- `|net_diff|` 穿越 `5 / 10 / 0`
- 历史 live quotes 必须重评
- 不允许旧单跨 bucket 留存

3. **Pairing 腿不再追跌**
- 市场继续单边下行时
- `pairing` 腿的 `strategic_target` 保持由策略决定
- 只有真实动作时才 clamp 到 post-only safe 价格
- 不得再出现 “策略价也被 ask-1tick 持续拉低”

4. **same-side 保守语义保留**
- `risk-increasing` upward 继续 no-chase
- 但 downward 明显偏离时必须重发
- 不允许一直保留过时高价单

5. **Merge sync**
- 如果 surviving side 未清零且 VWAP 不变
- 仍只视为 inventory-change trigger
- 不允许引入额外的策略模式或特殊 reanchor

6. **移除 opposite-slot fuse 后回放**
- 高失衡场景下，风险仍由 tier cap / `VWAP ceiling` / `SoftClose` 控住
- 不得出现因为移除 fuse 就重新放大 same-side 风险的副作用

## Assumptions
- 当前 live 验证基线锁定为 `PM_PAIR_ARB_TIER_1_MULT=0.70`、`PM_PAIR_ARB_TIER_2_MULT=0.30`
- 这轮不再继续扩展 `PairProgressState`、`fragile`、`settled/working` 对 `pair_arb` 的影响
- 这轮不把 `debounce_ms`、`reprice_threshold` 当第一优先级；先把职责边界理顺
- 这轮不做更激进的库存参数调整，除非职责收敛后 live 仍证明主仓侧 build 过于激进
