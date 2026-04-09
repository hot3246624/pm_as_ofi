# Pair_arb 策略说明（当前主线）

## 1. 核心目标

`pair_arb` 是 `maker-only / buy-only / pair-cost-first` 策略。

策略只做一件事：在不突破风险上限的前提下，持续用双边挂买把 `pair_cost` 压到 `pair_target` 附近或以下，并优先提升可配对仓位。

---

## 2. 运行语义（简化版）

### 2.1 单一策略状态脑

策略报价只读一个库存视图（`strategy_inventory`）：

- `Matched`：立即更新库存，立即影响下一单报价
- `Failed`：反向回滚库存，立即影响下一单报价
- `Merge sync`：做库存校正

策略不再使用 `settled / pending / fragile` 驱动报价分支。

### 2.2 阶梯逻辑（按 `|net_diff|`）

- `|net_diff| = 0`：
  - 回到 flat-state，按市场 + `pair_target` 发送双边买单
- `0 < |net_diff| < 5`：
  - 仍是轻偏置双边
- `5 <= |net_diff| < 10`：
  - 主仓侧进入 `tier1` ceiling
- `|net_diff| >= 10`：
  - 主仓侧进入 `tier2` ceiling

一旦 `|net_diff|` 到达/穿越 `5` 或 `10`，下一单就按新梯度上限计算。

### 2.3 状态变化后的旧单处理

当以下事件发生时会重算状态并重评 live 订单：

- `Matched`
- `Failed`
- `Merge sync`
- `SoftClose` 进入
- 新 round 开始

若旧单不符合新状态约束，执行 `Republish`（重报价），而不是让旧单跨 bucket 长期留存。

---

## 3. 价格链（保持 pair-cost-first）

每侧候选价格链：

1. 市场中价基础报价（含 A-S/skew）
2. 主仓侧 `tier avg-cost cap`
3. OFI 软塑形（仅 same-side risk-increasing）
4. `VWAP ceiling`（pair target 约束）
5. maker 安全夹层（`same-side` 在策略层持续 clamp；`pairing` 在执行动作时 clamp）
6. `simulate_buy` 与效用筛选

约束优先级：

- 风险和库存硬约束优先
- `tier cap` 与 `VWAP ceiling` 是同侧加仓上限
- `pairing / risk-reducing` 不受 same-side `tier cap` 误伤
- 配对腿的战略目标价不再被持续 `ask-1tick` 下拉；仅在真实 place/reprice 动作时做 post-only 安全夹层

### 执行语义（重要）

- `same-side risk-increasing buy`：
  - 继续 `no-chase`（新价更高时默认 retain）
- `pairing / risk-reducing buy`：
  - 若新价比 live 价高超过 `2 ticks`，允许 upward republish
  - `<=2 ticks` 的微小上行仍 retain，避免抖动

---

## 4. 尾段规则（15m）

最后 `45s` 进入最小 `SoftClose`：

- 阻断 `same-side risk-increasing buy`
- 继续允许 `pairing / risk-reducing buy`
- 不使用 `HardClose / taker / 市价去风险`

---

## 5. OFI 角色（从属，不主导）

OFI 继续使用当前引擎，但只做从属塑形：

- `pairing / risk-reducing buy`：忽略 OFI
- `same-side risk-increasing buy`：
  - `hot`：`-1 tick`
  - `toxic`：`-2 ticks`
  - `toxic + saturated`：suppress

OFI 不参与状态机切换，不定义 `5/10` 阶梯，不替代 `pair_target` 主目标。

---

## 6. 实盘检查点

重点观察四项：

1. `|net_diff|` 穿越 `5/10` 后，下一单是否立即切到对应 tier ceiling
2. `net_diff` 回到 `0` 后，是否恢复 flat-state 双边报价
3. 配对腿旧单是否在状态变化后及时 republish，而非长期滞留
4. 最后 `45s` 是否只出现 pairing/reducing，不再继续扩大单边
