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

**V1.1 升级要求**（目标值）：

```text
样本规模     >= 20000 trades
时间跨度     >= 5 个连续日历周（含至少一个低波动周与一个高波动周）
分桶分析     按 market_slug 分桶后，p10 市场 clean_closed_ratio >= 80%
分时分析     按 UTC 4h 分桶后，最差时段 clean_closed_ratio >= 85%
方差约束     same_side_add_qty_ratio 跨市场 95th percentile <= 15%
```

**作用范围（修订）**：

```text
阻塞 enforce       是 (任一未达标 → 不允许进入实盘 enforce)
阻塞 shadow        否 (skeleton/shadow runner 可在升级数据收集期间并行运行)
阻塞 skeleton      否 (TrancheLedger / EpisodeMetrics / Replay 是数据生产工具，必须先建)
```

理由：skeleton 与 shadow tooling 本身是 H-0 升级数据的生产工具，等数据再建工具会陷入循环阻塞。我方 shadow 阶段产生的 episode metrics 也可纳入升级数据集。

**数据可获取性 caveat**：

`XUANXUAN008_STRATEGY_DEEP_DIVE_ZH.md` §1.1 已注明公开 `data-api` 历史 offset 受限，目前能稳定重建 ~4000 trades。在并入 PLAN_Codex 前必须先做一次 `data-api` probe 验证 5 周历史是否可获取。若不可获取，本节硬阈值降级为：

```text
"可获取的最大公开窗口" + "我方 shadow 持续 N 周生成的同口径 metrics"
N >= 4 周，且我方 shadow metrics 的市场分桶/分时分桶满足上述阈值。
```

**分级 enforce 候选**（避免 all-or-nothing）：

```text
top markets   (per-market clean_closed >= 90%)        允许 enforce 全 size
mid markets   (85% <= per-market clean_closed < 90%)  仅允许 maker-only / 半 size enforce
low markets   (per-market clean_closed < 85%)         仍 shadow，禁止 enforce
```

进入 enforce 时按市场逐个判定，不要求全 7 市场齐过线。

**Why**：聚合 95% 可能掩盖某些市场 70%。我方只有 7 市场，正好可能落在那一档。但 7 市场表现不齐整时不应一刀切关闭 enforce，分级 enforce 在保留纪律的同时不浪费已达标市场的机会。

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

### 2.3 [SHADOW COUNTER] Same-Side Run 条件化候选（不改默认）

**V1 默认（保留）**：

```text
PM_PGT_MAX_SAME_SIDE_RUN=1
PM_PGT_SAME_SIDE_RUN_OBSERVE=2
```

**V1.1 修订**：**默认行为不变**（仍 `MAX=1`），原四 gate 逻辑降级为 shadow counter，不进入实盘行为路径。

shadow counter 定义：

```text
conditional_second_same_side_would_allow
  在每个被 MAX=1 阻塞的"潜在第二笔同侧"决策点上，评估若 MAX=2 是否本可触发：
    qty_2 <= 0.5 * qty_1                       (体量缩减)
    opposite_book_top_within_1_tick == true    (对侧 ladder 可见)
    projected_pair_cost_after_close <= 1.04    (可补回来)
    surplus_bank > qty_2 * 0.02                (兜底预算)
  四 gate 全过 → counter +1，并记录该 fill 的预测 PnL（dry-run）。
```

**升级条件**（满足后才考虑改默认 `MAX=2`）：

```text
counter / total_blocked >= 5%
&& counter dry-run cohort_net_pair_pnl >= 0
&& 持续 4 周 shadow 不退化
```

**Why**：xuan `same_side_add_qty_ratio=9.52%` 说明确实存在条件化放行的 edge。但 V1 实现初期，四 gate 中 `projected_pair_cost_after_close` 与 `surplus_bank` 都还**没有可信测量**——指标本身不可信时放开 cap 等于无刹车提速。先做 shadow counter 观察，等指标稳定再改默认。

证据等级：`B`，需要 shadow 数据 + counter 显著性验证。

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

**作用阶段（修订）**：本节 V1.1 仅做 **telemetry 暴露**，不立即改变行为。所有"阻塞 / 触发"门槛先 shadow-only，待 telemetry 稳定后再决定是否进入行为层。

shadow 阶段 telemetry 输出：

```text
locked_capital_ratio_p50/p90
mergeable_full_sets_p50/p90
merges_per_hour
new_open_blocks_due_to_capital_dry_run (count, 仅记录假设启用 gate 后会触发多少次)
```

**Shadow-only 门槛**（仅记录，不阻塞）：

```text
locked_capital_ratio > 0.60
  -> would_block_new_open = true (dry-run, 不实际阻塞)

locked_capital_ratio > 0.50 OR mergeable_full_sets >= harvest_trigger_full_set
  -> would_trigger_merge = true (dry-run)
```

**字段拆分（修订）**：原 `harvest_min_dynamic` 同时承担"触发阈值"与"批大小"两种语义，逻辑矛盾。拆为两个独立字段：

```text
harvest_trigger_full_set        触发阈值（≥才 MERGE）
                                与 ratio 反相关：低压力 10，高压力降到 5
                                公式: max(5, round(10 * (1 - locked_capital_ratio)))

harvest_batch_full_set          批大小（一次 MERGE 多少 full set）
                                与 ratio 正相关：低压力 10，高压力升到 25
                                公式: max(10, round(locked_capital_ratio * 50))

harvest_min_interval_secs       最小间隔 = 20s (沿用 V1)，避免 MERGE 风暴
```

含义：

- 低压力（`ratio < 0.2`）→ trigger=10 / batch=10，正常节奏。
- 高压力（`ratio > 0.5`）→ trigger=5（更早触发）/ batch=25（一次清更多），共同实现"高压力 → 更频繁、更大批量 MERGE"。

**进入行为层的前置**（在 V1.1 enforce 前置之外）：

```text
shadow telemetry 至少 4 周，期间：
  locked_capital_ratio_p90 < 0.6                    资金未真的锁死
  would_trigger_merge dry-run 与实际 V1 静态 harvest 决策的覆盖率 > 80%
  将动态 harvest 接入后回测/replay 不退化 cohort_net_pair_pnl
```

满足后才把 `harvest_trigger_full_set` 与 `harvest_batch_full_set` 接入行为层。

**Why**：xuan 在 367 market 上靠 MERGE 不断释放抵押，所以 surplus bank 才积得起来。我方只有 7 market，资金会更快锁死，必须显式建模资本周转，否则 V1 跑久了会"看起来 H-0 通过但实际开不出新仓"。但行为 gate 在 telemetry 不可信时不应直接接入，先观察再启用。

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

**V1.1 OVERRIDE**（ratio-based，适配任意 round 长度）：

```text
t = round_close_ts - now
T = round_total_secs
r = t / T                                    剩余时间占比

min_edge_per_pair_t =
    base_edge * (1 + α * (1 - r))            r 越小（越临近 close）edge 越高

repair_budget_per_share_t =
    repair_budget_per_share * (1 - β * (1 - r))   r 越小可用 repair 预算越低

urgency_budget_per_share_t =
    if   r > 0.5                          : 0
    elif 0.2 < r <= 0.5                   : linear_ramp(0, urgency_max, r from 0.5 to 0.2)
    elif r <= 0.2 AND active_tranche      : urgency_max
    elif r <= 0.2 AND no_active_tranche   : 0     (尾段不开新仓)
```

**修订原因**：原公式使用绝对秒数 `600s / 60s` 阈值，但 PLAN_Codex line 58 明确"初始交易域固定为 BTC 5m"——`T=300s` 时 `t > 600s` 永远不成立，`60s < t <= 600s` 从开盘就成立，导致 urgency ramp 全 round 生效，与"close 前 10min 才启用"的解释矛盾。改 ratio-based 后：

- 任意 round 长度（5m / 1h / 24h）公式都自适应。
- 5m round 验算：`T=300s, r=0.2` → `t=60s`；`r=0.5` → `t=150s`。

shadow 默认：

```text
PM_PGT_BASE_EDGE=0.005
PM_PGT_ALPHA_TIME_DECAY=0.5
PM_PGT_BETA_REPAIR_DECAY=0.5
PM_PGT_URGENCY_BUDGET_MAX=0.005
```

含义：

- 越临近 close（`r` 越小），要求 surplus 越厚（`min_edge` 上升）。
- 越临近 close，repair 预算可用比例越低（`repair_budget` 衰减）。
- urgency budget 仅在 round 后半段（`r <= 0.5`）启用；最后 20%（`r <= 0.2`）仅用于已存在的 active tranche，不开新仓。

**Why**：close 临近时 opposite 流动性单调收缩、价格单调向 0/1 极化。静态 ceiling 在尾段会持续做错决定。`α/β` 是两股相反力——有意设计让 V1 自动在尾段降速。

证据等级：`C`（无尾段经验数据），shadow 必须输出尾段单独的 PnL 与 close 距离的散点。

---

### 2.6 [ADD] Maker live shadow + Taker replay/simulation

**V1 隐式假设**：first leg 是 maker（隐含在 `first_vwap` 与 `completion_ceiling` 的取价语义里）。

**V1.1 要求（修订）**：**只跑一条 live runner（maker），taker 仅在 simulator 中回放**。不再要求并行 live IOC/FAK。

```text
path_maker_live_shadow:
  first leg:  post limit on best bid (V1 现有设计；不下单时记 dry-run quote)
  completion: post limit on completion_ceiling
  使用真实 fill stream + 真实 book + 真实 PnL。

path_taker_simulation:
  数据源:    与 maker live shadow 共用同一份 tick / book snapshot / xuan trade tape
  first leg:  在 simulator 中假设以 (best_ask + tick_buffer) 即刻成交
  completion: 在 simulator 中假设以 completion_ceiling 即刻成交
  不下单。所有 fill 由 simulator 用同时刻盘口推断。
```

两路径独立维护各自的 `TrancheLedger`、`SurplusBank`，互不混账。simulator 必须建模 taker fee drag 与 simulated adverse selection（用同时刻 mid 移动近似）。

**修订原因**：原 V1.1 写"shadow 阶段两条路径并行"，但 `path_taker_shadow` 描述用 `marketable IOC` 与 `FAK`——这是真实下单。两条 live runner 同时跑会带来：
- 双倍资金占用与双倍 risk exposure；
- live taker 的 adverse selection 与 fee drag 与 maker 不同，混账后无法清晰归因；
- 与 PLAN_Codex line 8/26/46 的 `CompletionMode::BoundedTakerShadow`（仅统计不下单）不一致。

simulator 路径同样能完成 maker/taker 等价路径对照，且不引入 live 风险。

并行输出（来源不同，口径相同）：

```text
maker_live.clean_closed_ratio        taker_sim.clean_closed_ratio
maker_live.same_side_add_qty_ratio   taker_sim.same_side_add_qty_ratio
maker_live.cohort_net_pair_pnl       taker_sim.cohort_net_pair_pnl
maker_live.first_leg_fill_rate       taker_sim.first_leg_fill_rate
maker_live.adverse_selection_proxy   taker_sim.taker_fee_drag
```

裁决规则（**V1 → V1.1 enforce gate**）：

```text
进入 enforce 前必须满足：
  abs(maker_live.clean_closed_ratio - xuan.clean_closed_ratio) <= 5%
  OR
  abs(taker_sim.clean_closed_ratio   - xuan.clean_closed_ratio) <= 5%

被选中那一路的 cohort_net_pair_pnl 优于我方 baseline。
若 taker_sim 显著占优 -> 在 BS-1a 完成后再单独评估是否引入 BoundedTakerShadow，
不直接从 simulation 跳到 live taker。
```

如两路都不达标 → **不 enforce**，回到研究层重新审视 maker/taker 之外的可能（例如 xuan 是混合角色或有未知预埋路径）。

**Why**：maker/taker 当前是 evidence `C`。如果选错，整个 V1 shadow 数字都会假阳性。simulator 对照可以在不引入双倍 live 风险的前提下完成裁决；live taker 的引入应当在 simulator 显著占优 + BS-1a 完成后单独决策，而非默认与 maker 并跑。

---

## 3. V1.1 风险矩阵

| 风险 | 来源 | V1.1 缓解 |
|------|------|----------|
| 公开样本 regime-dependent | A1 | §2.1 H-0 升级 + 分级 enforce + 数据可获取性 caveat |
| maker/taker 角色错判 | A2 | §2.6 maker live shadow + taker simulator |
| 资本周转跟不上 | A3 | §2.4 CapitalModel telemetry（行为 gate 后置） |
| FIFO pair cost 系统性偏差 | Doc 2 方法 | §2.2 tranche-explicit 并行 |
| 尾段 repair 失控 | V1 公式静态 | §2.5 ratio-based 时间衰减 ceiling |
| same-side cap 是否过严 | V1 默认 | §2.3 shadow counter（默认仍 MAX=1） |

---

## 4. 推进矩阵

### 4.1 V1.1 立即可做（不依赖 V1 实现细节）

- 研究层补样本（满足 §2.1 H-0 升级）。
- 研究层加 tranche-explicit pair cost 计算（§2.2）。
- 研究层加 maker/taker 双口径回测脚本（§2.6 shadow 前置）。

### 4.2 V1 工程实现时同步落入 V1.1

- `TrancheLedger` 同时记录 `pair_cost_tranche` 与 `pair_cost_fifo`（§2.2）。
- 行为层默认仍 `MAX_SAME_SIDE_RUN=1`；新增 shadow counter `conditional_second_same_side_would_allow`（§2.3）。
- 新增 `CapitalState` telemetry，先暴露 `harvest_trigger_full_set` / `harvest_batch_full_set` 计算结果，**不接入行为层**（§2.4）。
- `completion_ceiling` 直接采用 ratio-based 时间衰减形态（§2.5）。
- `PairGatedTrancheArb` strategy 启动 maker live shadow runner + taker simulator（共用 tick stream）（§2.6）。

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
| §4.1 Tranche 字段 | `pair_cost` 拆为 `pair_cost_tranche` / `pair_cost_fifo`；新增 `path_kind: MakerLive/TakerSim` |
| §4.2 Surplus Bank | 不变 |
| §4.3 Repair Budget | `MAX_REPAIR_PAIR_COST` 仍为 `1.04`，但**口径**改为 tranche-explicit |
| §5.1 First Leg | 不变 |
| §5.2 Completion Leg | OVERRIDE 为 §2.5 ratio-based 时间衰减形态 |
| §6.1 New Open Gate | `locked_capital_ratio` shadow telemetry-only，不阻塞（§2.4） |
| §6.2 Same-Side Run Cap | **保留 V1 的 `MAX=1`**；新增 shadow counter（§2.3） |
| §7 Merge / Harvest | `harvest_min_full_set` 仍为 `10`；新增 `harvest_trigger_full_set` / `harvest_batch_full_set` shadow telemetry（§2.4） |
| §8 Shadow 指标 | 增补 §2.2 §2.3 §2.4 §2.5 §2.6 输出 |
| §9 Enforce 门槛 | 在 V1 基础上叠加 §4.3，且 H-0 升级 gate 仅阻塞 enforce、不阻塞 shadow（§2.1） |

---

## 6. 一句话总结

V1 工程框架对，但内嵌三个未裁决假设。V1.1 不重做 V1，只是把这三个假设变成 gate：**样本要够（§2.1，仅阻塞 enforce）、口径要双（§2.2 强制；§2.6 改为 maker live + taker simulator）、资本要建模（§2.4，先 telemetry 后 gate）**。其余两处只做不改默认的小修：§2.3 把条件化 same-side 放行降级为 shadow counter，§2.5 用 ratio-based 公式修复 5m round 下绝对秒数阈值的硬 bug。
