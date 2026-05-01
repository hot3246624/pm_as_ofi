# Pair-Gated Tranche Arb V1 策略设计

生成时间：`2026-04-25`  
定位：全新 V1 策略设计文档，不修改旧 `pair_arb` 语义。  
目标：把 `xuanxuan008` 公开路径中最可复用的机制工程化：**一对一推进、预算化补腿、滚动回收**。

---

## 1. 策略摘要

`Pair-Gated Tranche Arb V1` 不是固定 `UP + Down <= pair_target` 的策略。

它的核心是：

```text
只有当当前未配对残差很小，才允许开新 first leg。
只要存在 active first leg，就进入 completion-only。
completion-only 期间禁止继续同侧开仓，只允许 opposite 补腿。
补腿价格可以变化，但必须被 surplus bank 覆盖。
成对库存达到阈值后滚动 MERGE。
```

一句话：

> 先追求高概率配对成功，再用 cohort 层预算约束平均 pair cost。

---

## 2. 非目标

V1 明确不做：

- 不把旧 `pair_arb` 直接改成最终归宿。
- 不启用默认 `SELL` 平仓路径。
- 不启用默认 `FAK` aggressive 补腿路径。
- 不以固定 `pair_target` 判定每一小对是否可交易。
- 不根据单个 round 的样本调实盘参数。
- 不在 maker/taker 未裁决前假装知道 xuan 的真实订单角色。

---

## 3. 核心状态机

### 3.1 状态

```text
FlatOrResidual
  abs(net_diff) <= residual_eps
  可以开启新 tranche

ActiveFirstLeg
  已有未覆盖 first leg
  记录 first_side / first_qty / first_vwap

CompletionOnly
  只允许 opposite side
  同侧 risk-increasing 订单全部抑制

PairCovered
  abs(tranche_residual_qty) <= residual_eps
  可以关闭 tranche 会计状态

MergeQueued
  market 内 pairable full set 达到 harvest 阈值

Closed
  tranche 已归因完成
```

### 3.2 转移

```text
FlatOrResidual -> ActiveFirstLeg
  条件：session_ok && no_active_tranche && new_open_allowed

ActiveFirstLeg -> CompletionOnly
  条件：first fill confirmed/matched into working inventory

CompletionOnly -> PairCovered
  条件：opposite cumulative qty covers first leg to residual_eps

PairCovered -> FlatOrResidual
  条件：remaining residual within cap; surplus/repair booked

PairCovered -> MergeQueued
  条件：pairable_full_set >= harvest_min_full_set

MergeQueued -> Closed
  条件：merge event observed or dry-run marks harvestable
```

### 3.3 Overshoot 规则

如果 opposite leg 超过当前 active tranche 所需数量：

```text
needed = first_qty - opposite_qty
overshoot = fill_qty - needed
```

- `needed` 部分归入当前 tranche。
- `overshoot` 部分开启下一条反向 residual tranche。
- 不允许把 overshoot 简单丢进全局均价，否则会污染 pair cost 归因。

---

## 4. 会计模型

### 4.1 Tranche 字段

V1 至少需要以下会计字段：

```rust
struct PairTranche {
    id: TrancheId,
    market_slug: String,
    opened_at: Instant,
    state: TrancheState,
    first_side: Side,
    first_qty: f64,
    first_vwap: f64,
    hedge_qty: f64,
    hedge_vwap: f64,
    residual_qty: f64,
    pair_cost: f64,
    pairable_qty: f64,
    surplus: f64,
    repair_spent: f64,
    closed_at: Option<Instant>,
}
```

### 4.2 Surplus Bank

```text
pair_surplus = pairable_qty * max(0, 1 - pair_cost)
repair_loss  = pairable_qty * max(0, pair_cost - 1)
```

每个 market 维护：

```text
surplus_bank = sum(pair_surplus) - sum(repair_loss)
```

V1 允许高成本 completion，但必须满足：

```text
projected_repair_loss <= repair_budget_available
```

### 4.3 Repair Budget

Shadow 默认：

```text
PM_PGT_REPAIR_BUDGET_FRACTION=0.50
PM_PGT_MIN_EDGE_PER_PAIR=0.005
PM_PGT_MAX_REPAIR_PAIR_COST=1.04
PM_PGT_TAIL_MAX_REPAIR_PAIR_COST=1.06
```

含义：

- 平时最多消耗已实现 surplus 的 `50%`。
- 默认要求 cohort 仍保留 `0.5%` 每 full set 安全边际。
- 平时 pair cost 超过 `1.04` 不补。
- 尾段只在 active tranche 已存在时，允许 shadow 观察 `1.06` 上限。

这些是 shadow defaults，不是实盘真值。

---

## 5. 价格规则

### 5.1 First Leg

First leg 只在 `FlatOrResidual` 状态允许。

Shadow 默认：

```text
PM_PGT_RESIDUAL_EPS=10
PM_PGT_RESIDUAL_EPS_OBSERVE=25
PM_PGT_BASE_CLIP=120
PM_PGT_MAX_CLIP=250
```

2026-05-01 BTC 5m replay 后新增 shadow profile：

```text
PM_PGT_SHADOW_PROFILE=replay_focused_v1
PM_PAIR_TARGET=0.975
PM_OPEN_PAIR_BAND=0.98
```

`replay_focused_v1` 只在 shadow/profile 显式启用时生效：

- 开盘前 75s 不开 first leg；
- seed pair cap = `0.980`；
- early completion pair cap = `0.975`；
- late completion pair cap = `0.995`；
- fixed seed clip = `57.6` shares。

Replay profile 会保留上述 seed clip 作为实际下单目标，不再套用 legacy 的 seed-size haircut（例如没有 immediate completion path 时乘 `0.60`）。这样 shadow 样本才是在验证 replay 搜索得到的参数，而不是另一个更保守的变体。

该 profile 来自 public market-side replay 参数搜索；它用于 shadow 验证真实 completion fill rate，不是 enforce 配置。

2026-05-01 xuan 最新 public trade 行为分析后新增 shadow profile：

```text
PM_PGT_SHADOW_PROFILE=xuan_ladder_v1
PM_PAIR_TARGET=0.975
PM_OPEN_PAIR_BAND=0.98
```

`xuan_ladder_v1` 不是替代 `replay_focused_v1` 的安全参数，而是专门用于验证“最新 xuan 等额 tranche ladder”能否在我们执行路径里复现：

- 开盘后 `4s` 开始允许 first leg；
- 收盘前 `25s` 停止新 first leg，保留 completion/harvest；
- seed pair cap 内部提升到 `1.040`，不受 launcher 默认 `PM_OPEN_PAIR_BAND=0.98` 压制；
- completion pair cap 改为 profit-guard 口径：默认守 `1.000`，仅极老/极晚 residual 允许 maker repair 到 `1.010`；
- taker-close pair cap 单独限制为 `1.000`，避免 shadow 因放宽 maker completion cap 而主动跨价补成负边际配对；
- 若本轮已闭合过配对且最近 pair cost 没有正盈余、repair budget 为空，则后续新 first leg 必须有可见 breakeven completion path，避免亏损 tranche 后继续用宽入口叠加负成本；
- clip 由轮内时间决定：`t+4-44s => 120`，`t+45-119s => 160`，`t+120-209s => 210`，`t+210-259s => 135`，尾部 `80` shares；
- 仍为 shadow-only，不应直接 enforce。当前 OMS 仍是一侧一个 maker slot，不能完整表达 xuan 一轮内多 tranche 同时 ladder，只能先验证时间窗口、报价位置和 completion 压力。

first leg 报价不追求强方向预测，只要求：

- market liquidity ok
- spread ok
- round 仍有足够 completion 时间
- projected inventory 不超过 cap

### 5.2 Completion Leg

Completion ceiling：

```text
completion_ceiling =
    1
  - first_vwap
  - min_edge_per_pair
  + repair_budget_per_share
  + urgency_budget_per_share
```

约束：

- `completion_ceiling` 永远不得超过 `max_repair_pair_cost - first_vwap`。
- 如果 `surplus_bank <= 0`，`repair_budget_per_share = 0`。
- `urgency_budget` 只在 close 前或 episode timeout 接近时启用。

### 5.3 为什么不用固定 pair_target

公开样本显示 pair cost p10 到 p90 大约从 `0.81` 到 `1.12`。固定阈值会带来两个问题：

- 阈值过低：大量 active tranche 补不回来，配对成功率下降。
- 阈值过高：repair 失控，盈利被吞掉。

V1 因此使用：

```text
cohort-level budget discipline
```

而不是：

```text
per-pair fixed target
```

---

## 6. 行为规则

### 6.1 New Open Gate

允许新开仓必须满足：

```text
no_active_tranche == true
abs(global_residual_qty) <= residual_eps
same_side_run_len == 0
session_ok == true
tail_freeze == false
```

如果还有 active tranche：

```text
block_same_side_risk_increasing = true
allow_opposite_completion = true
```

### 6.2 Same-Side Run Cap

Shadow 默认：

```text
PM_PGT_MAX_SAME_SIDE_RUN=1
PM_PGT_SAME_SIDE_RUN_OBSERVE=2
```

解释：

- V1 行为层按 `1` 执行 shadow。
- `2` 只用于观测 xuan 公开数据中的轻微偏离。

### 6.3 Episode Timeout

Shadow 默认：

```text
PM_PGT_EPISODE_TIMEOUT_SECS=75
PM_PGT_EPISODE_TIMEOUT_P90_TARGET_SECS=60
```

超时后：

- 停止新开仓。
- 继续 completion-only。
- 是否放宽 repair budget 只做 shadow 统计，不默认 enforce。

### 6.4 Tail Freeze

Shadow 默认：

```text
PM_PGT_TAIL_FREEZE_SECS=30
PM_PGT_TAIL_COMPLETION_ONLY_SECS=60
```

含义：

- 距 close `60s` 内不再开新 tranche，只补 active tranche。
- 距 close `30s` 内禁止 risk-increasing maker。

---

## 7. Merge / Harvest

V1 中：

```text
PairCovered != Merged
```

`PairCovered` 是账本状态。  
`MERGE` 是资金回收动作。

Shadow 默认：

```text
PM_PGT_HARVEST_MIN_FULL_SET=10
PM_PGT_HARVEST_MIN_INTERVAL_SECS=20
PM_PGT_HARVEST_MAX_PER_ROUND=3
```

MERGE 只减少 pairable full sets，不得错误关闭未覆盖 tranche。

---

## 8. Shadow 指标

V1 shadow 必须输出：

| 指标 | 目标 |
|------|------|
| `clean_closed_episode_ratio` | `>= 90%` |
| `same_side_add_qty_ratio` | `<= 10%` |
| `episode_close_delay_p50` | `<= 20s` |
| `episode_close_delay_p90` | `<= 60s` |
| `residual_before_new_open_p90` | `<= residual_eps` |
| `pair_cost_p10/p50/p90` | 只观测 |
| `discount_pair_surplus` | 必须大于 repair loss |
| `repair_pair_loss` | `<= surplus / 1.25` |
| `cohort_net_pair_pnl` | `> 0` |
| `merge_turnover` | 只观测 |
| `worst_intracycle_mtm` | 只观测 |

---

## 9. 上线门槛

### 9.1 可立即实现

- `TrancheLedger`
- episode classifier
- surplus / repair accounting
- shadow-only `PairGatedTrancheArb`
- merge/harvest observer

### 9.2 不可立即 enforce

以下内容在缺少裁决前不能实盘默认开启：

- taker completion
- FAK completion
- SELL abandon
- depth-aware clip sizing
- dynamic repair budget 放宽

### 9.3 Enforce 条件

至少满足：

- shadow 连续 `2w`
- `clean_closed_episode_ratio >= 90%`
- `same_side_add_qty_ratio <= 10%`
- `cohort_net_pair_pnl > baseline`
- repair loss 被 surplus 覆盖
- 没有明显增加 tail residual

---

## 10. 测试场景

### 10.1 状态机测试

```text
U 100 -> D 100
  expect: one clean closed tranche

U 100 -> D 95 with eps=10
  expect: closed

U 100 -> D 80 -> U 50
  expect: same_side_add_before_covered

U 100 -> D 130
  expect: first tranche closed, 30 overshoot rolled to next residual
```

### 10.2 Budget 测试

```text
pair_cost 0.92 on 100 qty
  surplus = 8

later pair_cost 1.03 on 100 qty
  repair_loss = 3
  allowed if repair_budget_available >= 3
```

### 10.3 Merge 测试

```text
two closed tranches, pairable=220
merge 100
  expect: pairable reduced by 100
  active tranche unchanged
```

---

## 11. 最终形态

V1 的目标不是复制 xuan 的所有细节，而是吸收最明确的 edge：

```text
一对一推进纪律
+ 低残差开仓
+ completion-only 修复
+ surplus-budgeted variable pair cost
+ rolling merge
```

这是一个新策略，不是旧 `pair_arb` 的参数调优。
