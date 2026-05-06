# Pair_Arb v3.5 评估与优化实现建议

生成时间：`2026-04-25`
定位：对 `PLAN_PAIR_ARB_V3_5_ZH.md` 的评估、修正与实现建议。
重要约束：本文不取代 `PLAN_PAIR_ARB_V3_5_ZH.md`，也不直接修改旧主文档。

---

## 1. 结论摘要

`v3.5` 的方向是对的：

- 保留研究 / 会计 / 行为 / 安全四层。
- 强调 tranche 会计。
- 强调 cadence 指标。
- 要求 `H-x` 裁决。
- 保留 2 周 shadow。
- 保留 `FAK / SELL / abandon` 默认关闭。

但 `v3.5` 仍缺一个核心中间层：

```text
Pair-Gated Tranche Automaton
```

也就是：

```text
不是只看 round 级最终配平，
而是看 active first leg 是否被 opposite 覆盖后，
才允许继续开启下一条 tranche。
```

这应当成为新的 `H-0`，排在原 `H-1..H-4` 前面。

---

## 2. v3.5 已经抓住的部分

### 2.1 正确方向

`v3.5` 已经明确：

- `merge != close`
- 需要 `TrancheLedger`
- 需要 `CadenceTracker`
- 不能直接把单 round 猜想推成 enforce
- 不能删除安全路径
- `pair_arb.rs` 只应保留 baseline 角色

这些都应保留。

### 2.2 当前不足

不足不在于方向错误，而是抽象层级还不够。

`v3.5` 已经看到了：

- same-side run length
- first opposite delay
- first fill cover delay
- imbalance half-life

但还没有把这些指标收束成一个执行状态机：

```text
Flat / Residual
Active First Leg
Completion Only
Pair Covered
Merge Queued
Closed
```

因此工程实现如果只加指标，仍可能退回旧问题：

```text
一边越买越多
等待另一边以后补
最后靠 pair_target 或 max_portfolio_cost 救
```

这不是 xuan 公开路径中最强的纪律。

---

## 3. 新增 H-0：Pair-Gated Tranche Automaton

### 3.1 假设

`xuanxuan008` 的核心 edge 是：

```text
只有当前 residual 足够小时，才允许开新 first leg；
first leg 未被 opposite 覆盖前，基本不继续开同侧风险。
```

### 3.2 公开数据支持

最近公开窗口约 `4000` 笔 trades：

| 指标 | `eps=10` | `eps=25` |
|------|----------|----------|
| clean closed ratio | `95.28%` | `95.92%` |
| same-side add qty ratio | `9.52%` | `3.08%` |
| episode close delay p50 | `12s` | `12s` |
| episode close delay p90 | `56s` | `55.8s` |

证据等级：`A/B`

### 3.3 裁决阈值

`H-0` 通过条件：

```text
clean_closed_episode_ratio >= 90%
same_side_add_qty_ratio <= 10%
episode_close_delay_p90 <= 60s
```

默认研究口径：

```text
residual_eps = 10
residual_eps_observe = 25
```

### 3.4 对行为层的影响

如果 `H-0` 通过，行为层可以进入 shadow：

```text
C1-pair-gated tranche_arb
```

但不能直接 enforce。

---

## 4. 新增 H-5：Repair-Budgeted Variable Pair Cost

### 4.1 假设

`xuan` 不是用固定 pair target 控制每一对，而是用 cohort 层预算：

```text
discount_pair_surplus - repair_pair_loss > 0
```

### 4.2 公开数据支持

FIFO 近似 pair cost：

| pair cost | qty | profit |
|----------|-----|--------|
| `<= 0.95` | `95167.51` | `+13890.56` |
| `0.95 - 1.00` | `57038.96` | `+1254.43` |
| `1.00 - 1.04` | `51322.02` | `-1017.88` |
| `> 1.04` | `68483.22` | `-8705.10` |

总 FIFO 估算 pair PnL：`+5422.02`

证据等级：`A/B`

### 4.3 裁决阈值

`H-5` 通过条件：

```text
discount_pair_surplus / abs(repair_pair_loss) >= 1.25
cohort_net_pair_pnl > 0
repair_pair_loss is bounded by realized surplus
```

### 4.4 对行为层的影响

如果 `H-5` 通过，V1 shadow 可以使用：

```text
completion_ceiling =
    1
  - first_vwap
  - min_edge_per_pair
  + repair_budget_per_share
  + urgency_budget_per_share
```

但 `repair_budget_per_share` 只能来自 surplus bank，不能用无限 `max_portfolio_cost` 替代。

---

## 5. 对 v3.5 四层架构的修正

### 5.1 研究层

新增研究对象：

```text
xuan_pair_episode_summary
xuan_pair_cost_budget_summary
```

保留原三对象：

```text
xuan_round_summary
xuan_tranche_ledger
xuan_round_path_features
```

研究层新增强制输出：

- `clean_closed_episode_ratio`
- `same_side_add_qty_ratio`
- `residual_before_new_open_p90`
- `episode_close_delay_p50/p90`
- `pair_cost_p10/p50/p90`
- `discount_pair_surplus`
- `repair_pair_loss`
- `cohort_net_pair_pnl`

### 5.2 会计层

`TrancheLedger` 需要从归因工具升级为状态机基础。

新增概念字段：

- `state`
- `first_side`
- `first_qty`
- `first_vwap`
- `hedge_qty`
- `hedge_vwap`
- `residual_qty`
- `pair_cost`
- `surplus`
- `repair_spent`

保留原则：

```text
merge != close
```

### 5.3 行为层

新增候选：

```text
C1-pair-gated tranche_arb
```

执行纪律：

- `FlatOrResidual` 才允许 new open。
- `ActiveFirstLeg` 只允许 opposite completion。
- active tranche 未覆盖前，禁止同侧 risk-increasing。
- high-cost completion 必须消耗 surplus bank。
- tail 阶段只允许 completion，不允许 new open。

### 5.4 安全层

保持 v3.5 原纪律：

- `FAK` 默认 off。
- `SELL` 默认 off。
- `abandon-sell` 默认 off。
- 任何放宽必须引用 `BS-x/H-x` 裁决。

新增纪律：

- repair budget 不能被 `max_portfolio_cost` 无边界替代。
- same-side add 不能因为 OFI 或 direction signal 被静默放开。

---

## 6. 发布矩阵更新建议

### 6.1 可立即推进

- `A0 TrancheLedger` 增补 episode 字段。
- `B0 CadenceTracker` 增补 episode metrics。
- `D0 Harvester` 继续 shadow / dry-run。
- 新增 `C1-pair-gated tranche_arb` skeleton，默认 shadow-only。

### 6.2 进入 shadow 的前置

`C1-pair-gated tranche_arb` shadow 前置：

- `H-0` 通过或至少在公开 xuan 数据中强支持。
- 我方能输出同口径 episode metrics。
- `repair_budget` 只做 shadow 计算，不直接改变实盘价格。

### 6.3 进入 enforce 的前置

必须满足：

- shadow 至少 `2w`
- 我方 baseline 对照完成
- `clean_closed_episode_ratio >= 90%`
- `same_side_add_qty_ratio <= 10%`
- `cohort_net_pair_pnl` 优于旧 baseline
- repair loss 被 surplus 覆盖
- `BS-1` 至少解决我方 execution truth

### 6.4 仍需裁决后才能做

- maker/taker 角色仿真
- two-sided seed enforce
- depth-aware sizing enforce
- abandon-sell 放宽
- FAK / taker completion 默认开启

---

## 7. 对旧 `pair_arb` 的评价

旧 `pair_arb` 的主要问题不是“没有 pair target”，而是：

```text
它以全局库存均价为中心，
缺少 active tranche 的闭环约束。
```

这会导致：

- 未覆盖 first leg 期间仍可能继续同侧开仓。
- 高成本补腿与低成本折扣没有精确归因。
- `max_portfolio_cost` 容易变成救火上限，而不是预算纪律。
- `merge` 容易被误读为 close。

因此建议：

```text
旧 pair_arb 保留 baseline
新逻辑进入 tranche_arb / pair_gated_tranche_arb
```

不要把新状态机缝回旧策略作为最终形态。

---

## 8. 测试与验收

### 8.1 研究验收

必须能用公开 trades 重放：

- episode open / close
- same-side add
- FIFO pair cost
- surplus / repair loss
- residual before new open

### 8.2 会计验收

测试场景：

```text
U 100 -> D 95, eps=10
  close tranche

U 100 -> D 80 -> U 20
  mark same-side add before covered

U 100 -> D 130
  close current tranche, roll 30 overshoot

merge 50
  reduce pairable only; do not close unrelated active tranche
```

### 8.3 行为验收

Shadow 期输出：

- every fill 的 tranche decision
- why new open allowed / blocked
- why completion allowed / blocked
- repair budget before / after
- pair cost projection

---

## 9. 最终建议

`v3.5` 不应被推翻。它应该被补强：

```text
v3.5 原四层纪律
+ H-0 Pair-Gated Tranche Automaton
+ H-5 Repair-Budgeted Variable Pair Cost
+ episode-level accounting
+ surplus-bank completion budget
```

工程策略上：

- `pair_arb.rs` 留作 baseline。
- 新建 `Pair-Gated Tranche Arb V1` 文档与后续实现。
- 所有新行为先 shadow。
- 不在 maker/taker 未裁决前上线攻击性补腿。

这条路线最符合当前证据，也最不容易把 xuan 的 edge 误读成一个固定 `pair_target` 参数。
