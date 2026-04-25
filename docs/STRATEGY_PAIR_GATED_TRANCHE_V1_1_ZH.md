# Pair-Gated Tranche Arb V1.1 增量设计

生成时间：`2026-04-25`  
定位：对 `STRATEGY_PAIR_GATED_TRANCHE_V1_ZH.md` 的增量补丁。  
重要约束：本文不取代 `V1`，只增加 V1 中三个隐藏假设的显式化与裁决路径。任何与 V1 冲突的项明确标注 `OVERRIDE`，否则继承 V1 默认。

---

## 1. 为什么需要 V1.1

V1 把 xuan 的公开路径工程化为 Pair-Gated Tranche Automaton，方向正确。但 V1 在三个地方做了**隐式假设**，没有给出裁决方法：

```text
A1. xuan 公开窗口 (~4000 trades / 33h / 367 markets) 的 clean_closed=95%
    可以代表"常态"。

A2. 我方能像 xuan 一样 maker 到 first leg。
    （V1 的 first_vwap 与 completion_ceiling 都是 maker 语义。）

A3. 7-market 设置下，cohort surplus bank 能正常累积。
    （xuan 的 surplus 来自 367 market 广覆盖 + 频繁 MERGE 回收。）

任一不成立，V1 shadow 数字都会假阳性。
```

V1.1 的目标：把 A1/A2/A3 变成**可测量、可裁决**的 gate，而不是"假设它成立"。

---

## 2. 六处增量（与 V1 的差异）

### 2.1 [OVERRIDE] H-0 数据门槛升级

**V1 引用的 H-0 数据**：单窗口 `~4000` trades，`clean_closed=95.28%/95.92%`，全量聚合。

**V1.1 升级要求**：

```text
样本规模     >= 20000 trades
时间跨度     >= 5 个连续日历周（含至少一个低波动周与一个高波动周）
分桶分析     按 market_slug 分桶后，p10 市场 clean_closed_ratio >= 80%
分时分析     按 UTC 4h 分桶后，最差时段 clean_closed_ratio >= 85%
方差约束     same_side_add_qty_ratio 跨市场 95th percentile <= 15%
```

任一未达标 → **V1 不允许进入 shadow**，回到研究层补样本。

**Why**：聚合 95% 可能掩盖某些市场 70%。我方只有 7 市场，正好可能落在那一档。

证据等级：要求由 `B` 升至 `A`。

---

### 2.2 [ADD] Tranche-Explicit Pair Cost 强制并行

**V1 现状**：会计字段 `pair_cost` 定义为 `first_vwap + hedge_vwap`，但 shadow 阈值（`max_repair_pair_cost=1.04` 等）来自 Doc 2 的 **FIFO 近似** pair cost 分布。

**V1.1 要求**：shadow 期同时计算两套：

```text
pair_cost_tranche = first_vwap + completion_vwap   (V1 原口径)
pair_cost_fifo    = FIFO 近似                       (Doc 2 口径)
```

研究层强制输出：

```text
pair_cost_tranche_p10/p50/p90
pair_cost_fifo_p10/p50/p90
delta_p50 = pair_cost_tranche_p50 - pair_cost_fifo_p50
```

裁决规则：

```text
abs(delta_p50) > 0.05  -> alert + 归因排查
进入 enforce 前必须以 pair_cost_tranche 为准重定 max_repair_pair_cost。
```

**Why**：FIFO 把 same-side add 折进下一对，可能系统性高估 surplus、低估 repair。tranche-explicit 才是 V1 行为层真正消费的口径。

---

### 2.3 [OVERRIDE] Same-Side Run 二级放行

**V1 默认**：

```text
PM_PGT_MAX_SAME_SIDE_RUN=1
PM_PGT_SAME_SIDE_RUN_OBSERVE=2
```

**V1.1 默认（OVERRIDE）**：

```text
PM_PGT_MAX_SAME_SIDE_RUN=2
PM_PGT_SAME_SIDE_RUN_STRICT=1
```

但放行第二笔同侧必须**全部满足**：

```text
qty_2 <= 0.5 * qty_1                       (体量缩减)
opposite_book_top_within_1_tick == true    (对侧 ladder 可见)
projected_pair_cost_after_close <= 1.04    (可补回来)
surplus_bank > qty_2 * 0.02                (兜底预算)
```

任一不满足 → 退回 strict（实际 `MAX=1`）。

**Why**：xuan 自身 `same_side_add_qty_ratio=9.52%`（eps=10），V1 的 `MAX=1` 比 xuan 自己更严，会让我方比 xuan 错过更多 first leg 机会。但盲目放到 `2` 会污染归因。条件化放行是折中。

证据等级：`B`，需要 shadow 验证。

---

### 2.4 [ADD] Capital Pressure Gate

**V1 现状**：MERGE 列为 observe-only，`harvest_min_full_set=10` 静态。没有"资本压力"概念。

**V1.1 新增 CapitalModel 层**：

```rust
struct CapitalState {
    working_capital: f64,            // USDC available
    locked_in_active_tranches: f64,  // first leg 已成交但未 covered
    locked_in_pair_covered: f64,     // 已 covered 但未 MERGE
    mergeable_full_sets: f64,
    locked_capital_ratio: f64,       // (locked_active + locked_covered) / working
}
```

新增门槛：

```text
locked_capital_ratio > 0.60
  -> block_new_open = true (即使 H-0 / 状态机允许)

locked_capital_ratio > 0.50 OR mergeable_full_sets >= harvest_min
  -> trigger_merge = true
```

`harvest_min_full_set` 改为**动态**：

```text
harvest_min_dynamic = max(10, round(locked_capital_ratio * 50))
```

含义：

- 低压力（`ratio < 0.2`）→ `harvest_min=10`，正常节奏。
- 高压力（`ratio > 0.5`）→ `harvest_min=25`，更频繁 MERGE。

shadow 输出：

```text
locked_capital_ratio_p50/p90
merges_per_hour
new_open_blocks_due_to_capital (count)
```

**Why**：xuan 在 367 market 上靠 MERGE 不断释放抵押，所以 surplus bank 才积得起来。我方只有 7 market，资金会更快锁死，必须显式建模资本周转，否则 V1 跑久了会"看起来 H-0 通过但实际开不出新仓"。

---

### 2.5 [OVERRIDE] 时间衰减 Completion Ceiling

**V1 现状**：

```text
completion_ceiling =
    1
  - first_vwap
  - min_edge_per_pair
  + repair_budget_per_share
  + urgency_budget_per_share
```

`min_edge_per_pair=0.005` 静态；`urgency_budget` 描述为"close 临近时启用"，无函数形态。

**V1.1 OVERRIDE**：

```text
t = round_close_ts - now
T = round_total_secs

min_edge_per_pair_t =
    base_edge * (1 + α * (1 - t / T))

repair_budget_per_share_t =
    repair_budget_per_share * (1 - β * (1 - t / T))

urgency_budget_per_share_t =
    if   t > 600s                        : 0
    elif 60s  < t <= 600s                : linear_ramp(0, 0.005, 600s, 60s)
    elif t <= 60s AND active_tranche     : 0.005
    elif t <= 60s AND no_active_tranche  : 0     (尾段不开新仓)
```

shadow 默认：

```text
PM_PGT_BASE_EDGE=0.005
PM_PGT_ALPHA_TIME_DECAY=0.5
PM_PGT_BETA_REPAIR_DECAY=0.5
PM_PGT_URGENCY_BUDGET_MAX=0.005
```

含义：

- 越临近 close，要求 surplus 越厚（`min_edge` 上升）。
- 越临近 close，repair 预算可用比例越低（`repair_budget` 衰减）。
- 仅在 close 前 10min 内才允许 urgency budget；最后 60s 仅用于已存在的 active tranche。

**Why**：close 临近时 opposite 流动性单调收缩、价格单调向 0/1 极化。静态 ceiling 在尾段会持续做错决定。`α/β` 是两股相反力——有意设计让 V1 自动在尾段降速。

证据等级：`C`（无尾段经验数据），shadow 必须输出尾段单独的 PnL 与 close 距离的散点。

---

### 2.6 [ADD] Maker/Taker 双路径并行 Shadow

**V1 隐式假设**：first leg 是 maker（隐含在 `first_vwap` 与 `completion_ceiling` 的取价语义里）。

**V1.1 要求**：shadow 阶段**两条路径并行**，不预先选边：

```text
path_maker_shadow:
  first leg: post limit on best bid (V1 现有设计)
  completion: post limit on completion_ceiling

path_taker_shadow:
  first leg: marketable IOC up to (best_ask + tick_buffer)
  completion: FAK up to completion_ceiling
```

两路径独立维护各自的 `TrancheLedger`、`SurplusBank`，互不混账。

并行 shadow 输出：

```text
maker.clean_closed_ratio          taker.clean_closed_ratio
maker.same_side_add_qty_ratio     taker.same_side_add_qty_ratio
maker.cohort_net_pair_pnl         taker.cohort_net_pair_pnl
maker.first_leg_fill_rate         taker.first_leg_fill_rate
maker.adverse_selection_proxy     taker.taker_fee_drag
```

裁决规则（**V1 → V1.1 enforce gate**）：

```text
进入 enforce 前必须满足：
  abs(maker.clean_closed_ratio - xuan.clean_closed_ratio) <= 5%
  OR
  abs(taker.clean_closed_ratio - xuan.clean_closed_ratio) <= 5%

被选中的那一路 cohort_net_pair_pnl 优于我方 baseline。
```

如两路都不达标 → **不 enforce**，回到研究层重新审视 maker/taker 之外的可能（例如 xuan 是混合角色或有未知预埋路径）。

**Why**：maker/taker 当前是 evidence `C`。如果选错，整个 V1 shadow 数字都会假阳性。并行 shadow 是**唯一**能在不裁决的情况下推进的方式，成本是双倍的 shadow 复杂度。

---

## 3. V1.1 风险矩阵

| 风险 | 来源 | V1.1 缓解 |
|------|------|----------|
| 公开样本 regime-dependent | A1 | §2.1 H-0 升级到 5 周 + 分桶 |
| maker/taker 角色错判 | A2 | §2.6 双路径并行 |
| 资本周转跟不上 | A3 | §2.4 CapitalModel + 动态 harvest |
| FIFO pair cost 系统性偏差 | Doc 2 方法 | §2.2 tranche-explicit 并行 |
| 尾段 repair 失控 | V1 公式静态 | §2.5 时间衰减 ceiling |
| same-side cap 比 xuan 严 | V1 默认 | §2.3 二级放行 |

---

## 4. 推进矩阵

### 4.1 V1.1 立即可做（不依赖 V1 实现细节）

- 研究层补样本（满足 §2.1 H-0 升级）。
- 研究层加 tranche-explicit pair cost 计算（§2.2）。
- 研究层加 maker/taker 双口径回测脚本（§2.6 shadow 前置）。

### 4.2 V1 工程实现时同步落入 V1.1

- `TrancheLedger` 同时记录 tranche-explicit 与 FIFO pair cost。
- 行为层默认 `MAX_SAME_SIDE_RUN=2` + 条件化降级（§2.3）。
- 新增 `CapitalState` 与 `harvest_min_dynamic`（§2.4）。
- `completion_ceiling` 实现时直接采用时间衰减形态（§2.5）。
- `PairGatedTrancheArb` strategy 启动两个 shadow runner（§2.6）。

### 4.3 V1.1 enforce 前置（在 V1 enforce 门槛之上加）

V1 enforce 门槛保留，**额外**要求：

```text
H-0 升级数据已收集且达标 (§2.1)
pair_cost_tranche vs FIFO delta_p50 < 0.05 (§2.2)
maker 或 taker 双路径中至少一路达标 (§2.6)
locked_capital_ratio_p90 < 0.6 (§2.4)
尾段 (close < 60s) PnL 不显著为负 (§2.5)
```

### 4.4 仍需未来裁决

- 是否引入预埋双边 ladder（V1.1 仍只做反应式 completion）。
- depth-aware clip sizing。
- abandon-sell 放宽。

---

## 5. 与 V1 的兼容性

| V1 章节 | V1.1 关系 |
|---------|-----------|
| §3 状态机 | 不变 |
| §4.1 Tranche 字段 | `pair_cost` 拆为 `pair_cost_tranche` / `pair_cost_fifo`；新增 `path_kind: Maker/Taker` |
| §4.2 Surplus Bank | 不变 |
| §4.3 Repair Budget | `MAX_REPAIR_PAIR_COST` 仍为 `1.04`，但**口径**改为 tranche-explicit |
| §5.1 First Leg | 不变 |
| §5.2 Completion Leg | OVERRIDE 为 §2.5 时间衰减形态 |
| §6.1 New Open Gate | 增加 `locked_capital_ratio <= 0.60`（§2.4） |
| §6.2 Same-Side Run Cap | OVERRIDE 为 §2.3 |
| §7 Merge / Harvest | `harvest_min_full_set` 改为 `harvest_min_dynamic`（§2.4） |
| §8 Shadow 指标 | 增补 §2.2 §2.4 §2.6 §2.5 输出 |
| §9 Enforce 门槛 | 在 V1 基础上叠加 §4.3 |

---

## 6. 一句话总结

V1 工程框架对，但内嵌三个未裁决假设。V1.1 不重做 V1，只是把这三个假设变成 gate：**样本要够（§2.1）、口径要双（§2.2 §2.6）、资本要建模（§2.4）**。其余两处（§2.3 §2.5）是因 V1 默认参数偏离 xuan 经验或缺时间衰减而做的小修。
