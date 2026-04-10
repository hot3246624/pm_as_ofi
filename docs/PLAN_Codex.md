## Pair_arb 重发语义收敛计划（采纳 `PLAN_Claude_temp`）

### Summary
把 `pair_arb` 的发布语义彻底收敛为“离散库存事件驱动”，不再让 `pairing/reducing` 因连续价格漂移触发重发。

最终语义固定为：
- `RiskIncreasing`：状态驱动
- `PairingOrReducing`：也改为状态/事件驱动
- 两次离散触发之间，已有 live 单默认 retain
- 不再使用 `pairing up>2 / down>3` 的连续 band 重发

### Key Changes
#### 1. 统一 `pair_arb` 的重发触发源
`pair_arb` 的重评/重发只允许由以下离散事件触发：
- `Matched`
- `Failed`
- `Merge sync`
- `SoftClose entered`
- `Round reset`
- `PairArbStateKey` 变化
  - `dominant_side`
  - `net_bucket`
  - `soft_close_active`

这意味着：
- 不再因为市场在两次 fill 之间上下波动 `2/3 ticks` 就重发
- partial fill 不需要单独定义“多少才进入监控”
- 每一次 fill 事件本身就是唯一的精确重评节点

#### 2. 移除 `PairingOrReducing` 的连续 freshness reprice
在 [coordinator_execution.rs](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs)：
- `pair_arb_should_force_pairing_upward_republish(...)` 对 `PairingOrReducing` 返回 `false`
- `pair_arb_should_force_freshness_republish(...)` 对 `PairingOrReducing` 返回 `(false, "pairing_holds_between_triggers", 0.0)`

在 [coordinator_order_io.rs](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_order_io.rs)：
- 删除 `PairingOrReducing` 的 `upward > 2 ticks / downward > 3 ticks` retain-vs-republish 分支
- 改为：
  - `RiskIncreasing`：`same_side_state_driven`
  - `PairingOrReducing`：`pairing_holds_between_triggers`

#### 3. 保留当前离散库存脑，不新增第二套状态机
继续保留当前 `PairArbStateKey`：
- `dominant_side`
- `net_bucket = Flat / Low / Mid / High`
- `soft_close_active`

并继续保持：
- `|net_diff|` 到/穿越 `5 / 10 / 0` 时强制重评
- `net_diff = 0` 时恢复 flat-state 双边基线
- 旧 tier-limited 单不得跨状态残留

不新增：
- 新的 band
- 新的 brake
- 新的 fragile 主路径
- 新的对冲专用小状态机

#### 4. `VWAP ceiling / tier cap / OFI / SoftClose` 保持职责不变
保留当前价格链：
1. A-S 基础价
2. 三段 skew
3. tier avg-cost cap
4. same-side OFI subordinate shaping
5. `VWAP ceiling`
6. maker clamp
7. `safe_price`
8. `simulate_buy`

固定职责：
- `VWAP ceiling`：pair-cost 硬约束
- `tier cap`：主仓侧高失衡离散上限
- OFI：只塑形 `RiskIncreasing`
- `SoftClose`：最后 `45s` 只阻断 `same-side risk-increasing buy`

#### 5. 文档同步
更新 [STRATEGY_PAIR_ARB_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/STRATEGY_PAIR_ARB_ZH.md)：
- 明确写死：
  - `pair_arb` 的重发不是连续价格跟踪
  - `pairing/reducing` 也只由离散库存事件驱动
  - 两次 fill 之间默认 retain
- 删除任何仍暗示 `pairing up2/down3` 的描述

### Test Plan
1. `PairingOrReducing` 不再连续追价
- 同一 `state_key` 下，市场上下波动 `2-3 ticks`
- `pairing` live 单保持不动
- 不触发 freshness reprice

2. fill 事件触发唯一重评
- partial fill `3/5`
- 立刻重评一次并更新 live quote
- 后续无新 fill 时，即使 baseline 波动，也不再重发

3. `5 / 10 / 0` 状态切换
- `net_diff` 穿越 `5 / 10`
- fresh target 切到对应 tier
- 历史单必须重评
- `net_diff -> 0` 时恢复 flat-state

4. `Failed` 回滚
- `Matched` 后收到 `Failed`
- 回滚 inventory
- 若 `state_key` 变化则重评/重发
- 不引入第二套 fragile 报价脑

5. SoftClose
- 最后 `45s`
- 只允许 pairing/reducing
- 不再继续扩大单边敞口

### Assumptions
- 当前第一矛盾不是 tier 参数，而是 `pairing/reducing` 被错误地连续带宽化
- `PLAN_Claude_temp` 的核心判断成立：对 `pairing` 来说，fill 之间的 `2/3 tick` baseline 漂移主要是噪声，不应主导发布
- 本轮不新增任何新参数，不继续扩张状态机
