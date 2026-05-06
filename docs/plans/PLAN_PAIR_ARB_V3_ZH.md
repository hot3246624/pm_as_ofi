# Pair_Arb 升级计划 v3

> ⚠️ **本文档已被 `docs/plans/PLAN_PAIR_ARB_V3_1_ZH.md` 取代** — v3 抓对了研究方向，但实施上仍把新逻辑塞回旧 pair_arb + 聚合库存。v3.1 把架构改成"研究/会计/执行三层分离"，新建 `tranche_arb` 策略线，并加发布矩阵。本文档保留作为 v3 → v3.1 的演进对照。

---

# Pair_Arb 升级计划 v3 (superseded)

> **生成日期**：2026-04-25
> **取代**：v2（centered on completion hazard）。v2 的 hazard 框架方向对，但**控制变量定义错了**，且 Phase C/E 基于未证实假设。
> **证据基础**：`docs/research/xuan/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 1000 笔 trades + 95 个连续 BTC 5m markets 实证

---

## Context — 为什么需要 v3

v2 plan 把核心概念定为 `completion hazard h(τ | state) = P(F(t+τ) | C(t), state)`，即"对侧腿在 τ 内被填的概率"。这次基于 `xuanxuan008` 真实 1000 笔 trades + 95 个连续 BTC 5m markets 的实证研究证实：

1. **「整场 30s 配平」是被证伪的神话**：`first_opposite_delay` 中位 25s，但 `first_fill_cover_delay` 中位 46s / p90 162s。xuan **不是**控制"对侧腿完整填上"的概率，而是控制"运行中净差停留时间"的分布。
2. **真正控制变量**：`same-side run length`（中位 1，p90=2）+ `imbalance half-life`（中位 24s）。他靠**不让 first leg 长大**取胜，不是靠救腿。
3. **Multi-tranche per market 已被直接证据证实**：65/81 markets 出现 `TRADE → MERGE → TRADE`。Phase A cycle identity 不再是 prereq 讨论而是**结构性必需**。
4. **公开 trades 全部 BUY-only**：1000/1000 BUY，主路径是 `BUY both sides → MERGE paired → REDEEM leftover`。derisk SELL 路径在常态下几乎不应被触发。
5. **首次 MERGE 中位 180s**：远在 round 结束前，证实**滚动 harvest** 节奏。v2 Phase D 的 `harvest_min_full_set=20` 阈值过大、`harvest_min_interval_secs=60` 过严。

v3 的本质是把 v2 的"价位/时间 hazard 优化"替换成**"clip 节奏控制"**：限制单笔 size、限制同侧连续、追踪 half-life。

---

## 仍未解决的关键盲区（v3 决策前提）

下列三项**未澄清前 Phase C'/E 不应落地**：

| 盲区 | 影响 | 裁决方式 |
|------|------|---------|
| 公开 `side` 是 maker side 还是 taker side | 影响"高交替"是 xuan 策略指纹还是市场 BTC mean-reversion 表象 | 申请 authenticated CLOB `/trades`（含 `trader_side` `maker_orders` `taker_order_id`）|
| BUY-only 是 data-api 过滤还是真实 | 影响"buy-only path"作为模仿目标的合法性 | Polygonscan 对 `0xcfb103c37c0234f524c632d964ed31f117b5f694` 的 CTF token transfer 出入流水对账 |
| 双边预埋 vs 单边触发后补腿 | 决定 Phase C' 该写 `two-sided seed` 还是 `single-leg trigger + completion mode` | 拿 open order timeline；或 user-channel WebSocket book + own order events |

---

## v3 核心概念修订

### 旧（v2）
```
hazard h(τ | state) = P(F(t+τ) | C(t), state)
单位：完整对侧腿
优化目标：完整 cycle 完成时间
```

### 新（v3）
```
xuan 的实际控制变量：
1. same_side_run_length     ：连续同侧 fill 数（目标 ≤ 2）
2. imbalance_half_life      ：净差从 peak 衰减到 50% 的时间（目标 ≤ 30s）
3. per_clip_size            ：单笔 fill 大小（目标 small + multi-tranche）
4. rolling_merge_cadence    ：盘中 MERGE 触发节奏（不等到 round 结束）

派生变量（v2 错把它当主变量）：
- pair_target / dynamic target → 跟着上面 4 个变量走
- completion time → 是结果，不是控制变量
```

### 后果：实施重排

```
Phase A (cycle identity, 必做)
  ├─ Phase B' (新指标测量：run_length / half_life / clip_size 分布)
  │   └─ Phase C' (clip-size & run-length 限制器，写入开仓决策)
  │       └─ Phase D' (rolling harvest 触发器，小 size 高频)
  │
  └─ Phase E (completion push, 仅 abandon-sell 安全保底，不做 FAK 救腿)

Phase G (xuan 数据采集, 并行外部, 优先级提升至最高)
Phase F (dynamic target, 永久 deferred 或废弃)
```

---

## Phase 详细设计

### Phase A — Cycle / Tranche Identity（不变，强化必要性）

**Why now (v3 更强)**：65/81 markets 出现 `TRADE → MERGE → TRADE` 已是直接证据，cycle id 不只是 hazard 测量需要，而是**实时正确归因 net_diff** 的必需。

**改动**：
- `inventory.rs`：`FillRecord.cycle_id`、`current_open_cycle_id`、`cycle_counter`、可选 `per_cycle_ledger`
- `coordinator.rs`：订阅 cycle open/close、`active_cycle_features`
- `recorder.rs`：emit `cycle_opened` / `cycle_closed`

**新增**：cycle close 触发条件加 **rolling merge harvest**——cycle 内 paired 段被 merge 即认为该 cycle 关闭，剩余 leftover 开新 cycle。

**Risk**: medium。库存层改动；`cycle_id: Option<u64>` backward-compat 不影响其他策略路径。

### Phase B' — 新指标测量（取代 v2 Phase B）

**v3 KPI 三件套**（v2 的 `working_changed` 修复仍然采纳，但不是主指标）：

```rust
struct PairArbCadenceMetrics {
    same_side_run_length_p50: f64,
    same_side_run_length_p90: f64,
    runs_longer_than_2_count: u64,

    half_life_p50_ms: u64,
    half_life_p90_ms: u64,

    clip_size_p50: f64,
    clip_size_p90: f64,
    max_clip_in_window: f64,

    first_opposite_delay_p50_ms: u64,
    first_cover_delay_p50_ms: u64,
}
```

**算法定义**（plan 阶段必须明确，避免事后返工）：
- `run`：`fill.side` 等于上一笔 `fill.side` 即延续 run；不同即重置（merge 不计入）
- `half_life`：从 `|net_diff|` 到达局部 peak 起算，到下一次 `|net_diff|` ≤ peak/2 的时刻；若 net_diff 反向越零提前算"完成衰减"
- `clip_size`：单笔 fill 数量；merge/redeem 不计

**Cross-validation 目标值**：

| 指标 | xuan 中位 | xuan p90 | 我们当前 | 目标 |
|------|----------|----------|---------|------|
| `same_side_run_length` | 1 | 2 | 待测 | ≤ 2 |
| `imbalance_half_life` | 24s | 111s | 待测 | ≤ 30s |
| `clip_size` (首笔) | 170 | 397 | 待测 | 类似量级 |

**改动**：
- `coordinator_metrics.rs` 新 `PairArbCadenceTracker`
- 在 fill stream 处实时累积
- 30 分钟一次输出到 `📊 StrategyMetrics` 日志
- Recorder 写每 cycle snapshot

**Risk**: low（纯观测）。

### Phase C' — Clip-size & Run-length Limiter（取代 v2 Phase C two-sided seed）

**v3 关键修订**：v2 的 `should_open_two_sided_seed` 假设 xuan 双边预埋——**这个假设未证实**（盲区 3）。改为对每次开仓决策施加 clip-size 与 run-length 限制，行为表现等价于 xuan 的"高交替小 clip"。该实现**对预埋 vs 触发后补腿都成立**，因此**不依赖盲区 3 的答案**。

**API**：

```rust
struct ClipDecision {
    allowed: bool,
    max_clip_size: f64,
    cool_off_ms: Option<u64>,
    reason: ClipDecisionReason,
}

fn evaluate_clip_decision(
    coord: &StrategyCoordinator,
    inv: &InventoryState,
    cadence: &PairArbCadenceTracker,
    requested_side: Side,
    requested_size: f64,
) -> ClipDecision;
```

**Gate 逻辑**：
```
if same_side_run_length(requested_side) >= 2:
    allowed = false
    cool_off_ms = same_side_cooloff_ms (默认 5000)
elif requested_size > max_clip_size_cap:
    allowed = true
    max_clip_size = max_clip_size_cap (默认 200)
elif current_imbalance_half_life_estimate > 60s:
    max_clip_size = requested_size * 0.5
else:
    allowed = true; max_clip_size = requested_size
```

**接入点**：
- `strategy/pair_arb.rs::compute_quotes` 末段调用 `evaluate_clip_decision`，对返回 size 做 clip
- `coordinator_order_io.rs` 在 first-leg dispatch 前再检查一次 run-length

**Shadow 先跑**：`shadow_mode=true` 默认；只日志"会被限制"，2 周观察对 PnL/fill 影响后 enforce。

**Config**：
```
PM_PAIR_ARB_CLIP_LIMITER_ENABLED=0
PM_PAIR_ARB_CLIP_LIMITER_SHADOW=1
PM_PAIR_ARB_MAX_CLIP_SIZE=200
PM_PAIR_ARB_SAME_SIDE_MAX_RUN=2
PM_PAIR_ARB_SAME_SIDE_COOLOFF_MS=5000
PM_PAIR_ARB_HALF_LIFE_REGIME_THRESHOLD_MS=60000
```

**Risk**: medium。可能压制开仓节奏。缓解：shadow first；指标必须 enforce 后能落到目标值。

### Phase D' — Rolling Merge Harvest（取代 v2 Phase D）

**xuan 实证**：首次 MERGE 中位 180s（round 内），中位市场出现 ~3 次 MERGE。

**v3 修订（vs v2）**：
- `harvest_min_full_set` 从 20 → **5–10**（小 size 多次）
- `harvest_min_interval_secs` 从 60 → **15–30**
- 加入 hazard-aware 触发：cycle 内 `imbalance_half_life > threshold` 时立即 harvest 已配对部分

**改动**：
- `bin/polymarket_v2.rs` 新 `PairArbHarvester` actor，独立于 `run_capital_recycler`
- 调用 `execute_market_merge`（已支持 `dry_run`）
- **不复用** `try_recycle_merge` / `plan_merge_batch_usdc`（recycler 语义错位）

**触发条件**：
- `paired_qty ≥ 5–10 shares`
- 距上次 harvest ≥ 15s
- 非 abandon-sell 进行中
- 两侧无活跃 first-leg-pending order
- (可选) `imbalance_half_life > 60s` 优先 harvest

**Config**：
```
PM_PAIR_ARB_HARVEST_ENABLED=0
PM_PAIR_ARB_HARVEST_MIN_FULL_SET=10
PM_PAIR_ARB_HARVEST_MIN_INTERVAL_SECS=20
PM_PAIR_ARB_HARVEST_HALF_LIFE_TRIGGER_MS=60000
```

**Risk**: high（链上 tx）。缓解：dry-run 预演 → 小 set 单市场 A/B → 全量。

### Phase E — Abandon-sell Only Safety Net（v2 Phase E 大幅缩小）

**v3 修订**：
- xuan 不靠救腿 → **不实现 FAK 补腿路径**
- 只保留 abandon-sell 作为极端兜底
- 必须用 Phase A `cycle_id` 精确定位 first-leg tranche，不动其他 cycle 库存

**改动**：
- 新函数 `dispatch_cycle_tranche_sell(cycle_id, ...)`，**不复用** `dispatch_taker_derisk` 的聚合池路径
- 触发条件极保守：`cycle_age > abandon_after_ms` 且 `paired_qty == 0` 且 `cycle 内 fills ≥ N` 且 Phase C' 已限制后仍无对侧

**Config**：
```
PM_PAIR_ARB_ABANDON_SELL_ENABLED=0
PM_PAIR_ARB_ABANDON_AFTER_MS=180000
PM_PAIR_ARB_ABANDON_MAX_SLIPPAGE_TICKS=5
```

**Dependency**：**Phase A 必须**。

**Risk**: high。缓解：阈值宽 + Phase C' 已大概率使其不触发。

### Phase F — Dynamic pair_target（永久 deferred）

报告无证据支持 dynamic target；xuan 的 edge 在 cadence 不在 target。**v3 删除此 Phase**，留 placeholder：若 v3.0 全 enforce 后 PnL 仍逊于 xuan 同期，再回头评估。

### Phase G — xuan 数据采集（**v3 优先级提升至最高**）

**v3 重排理由**：盲区 1/2/3 任一未解，Phase C'/D'/E 都可能优化错对象。

#### P0（v3.0 启动前完成）
1. **CLOB authenticated `/trades`**：拿 `trader_side`、`maker_orders`、`taker_order_id` → 解决盲区 1
2. **Polygonscan CTF transfer 对账**：`0xcfb103c37c0234f524c632d964ed31f117b5f694` 出入流水 → 验证盲区 2 + 拿 split/merge tx hash
3. **更长地址级窗口**：2–4 周 trades + activity → 回答 session vs round

#### P1（v3.0 期间补）
4. **Open order timeline**：解决盲区 3，决定 Phase C' 是否需要预埋形态变体
5. **同步 book + trade tape**：复现 xuan clip-size 与盘口 depth 的关系

#### P2（v3.1 优化用）
6. **同期对照 trader 样本**：5–10 个 BTC 5m 高频 maker 跑同样指标，验证 xuan 是否有真正 alpha 还是行业基线

**Action Owner**：用户自行启动；本仓库代码不依赖。

---

## Phase 依赖 + Flag 表（v3）

| Phase | 名称 | Flag | 默认 | 依赖 | Risk |
|-------|------|------|------|------|------|
| A | Cycle identity | — 永久开启 | on | — | medium |
| B' | Cadence metrics | — 永久开启 | on | A | low |
| C' | Clip-size & run-length limiter | `PM_PAIR_ARB_CLIP_LIMITER_*` | shadow | B' ≥ 1w | medium |
| D' | Rolling merge harvest | `PM_PAIR_ARB_HARVEST_*` | off | A | high |
| E | Abandon-sell safety net | `PM_PAIR_ARB_ABANDON_SELL_ENABLED` | off | **A 必须** | high |
| F | Dynamic target | — | **deferred 或废弃** | — | — |
| G | xuan 数据采集 | 外部 | — | — | low |

---

## v3 vs v2 关键差异速查

| 维度 | v2 | v3 |
|------|----|----|
| 核心变量 | completion hazard `h(τ\|state)` | `same_side_run_length` + `imbalance_half_life` + `clip_size` |
| Phase B 指标 | per-bucket completion time p50/p95 | + run length + half life + clip size 三件套 |
| Phase C | `should_open_two_sided_seed → TwoSidedSeed` | `evaluate_clip_decision → ClipDecision`（与预埋假设无关）|
| Phase D 阈值 | full_set=20 / interval=60s | full_set=10 / interval=20s |
| Phase E | FAK 救腿 + abandon-sell 双路径 | **只**保留 abandon-sell；不做 FAK |
| Phase F | optional 后期评估 | **deferred 或废弃** |
| Phase G | 并行可选 | **最高优先级，启动前完成 P0** |
| 证据基础 | 2 次 profile 快照 | 1000 trades + 95 连续 markets 实证 |

---

## Critical Files

核心改动：
- `src/polymarket/inventory.rs`（Phase A cycle_id；记录 same_side run）
- `src/polymarket/messages.rs`（`FillRecord.cycle_id`，cycle event）
- `src/polymarket/coordinator_metrics.rs`（**v3 新**：`PairArbCadenceTracker`）
- `src/polymarket/strategy/pair_arb.rs`（**v3 新**：`evaluate_clip_decision` 接入 `compute_quotes`）
- `src/polymarket/coordinator_order_io.rs`（first-leg dispatch 前 run-length 检查；新 `dispatch_cycle_tranche_sell`）
- `src/bin/polymarket_v2.rs`（**v3 调整阈值**：`PairArbHarvester`）
- `src/polymarket/coordinator.rs`（cycle 生命周期；cadence 状态聚合）
- `src/polymarket/recorder.rs`（cycle events + cadence snapshots）

**复用现有**（v2 已识别的反模式不再复用）：
- `coordinator_order_io.rs:1268` `dispatch_taker_derisk` —— 不复用，聚合池语义错
- `polymarket_v2.rs:1340` `run_capital_recycler` —— 不复用，low-balance 触发语义错
- `executor.rs` `execute_market_merge`（含 dry_run）—— Phase D' 直接调用
- `recorder.rs` —— 已 live，extend 即可

---

## Verification

### Phase A
- 单测：并发开/关 cycle、rollback、timeout-promotion 不丢 cycle_id
- replay：recorder event 回放，cycle 数 == 实际配对组数

### Phase B'
- 跑 1 周生产数据，对比指标 vs xuan 中位/p90
- 若我们 `same_side_run_length` p90 ≫ 2 或 `half_life` 中位 ≫ 24s → 证明 Phase C' 必要

### Phase C'
- shadow 模式 1–2 周：日志对比"会被限"vs"实际开仓"
- enforce 后 7 天对比指标是否落到目标值
- 若 PnL 显著下降 → 阈值放宽

### Phase D'
- dry-run 7 天验证触发频率合理
- 单市场 A/B：A 启用 harvest，B 不启用，对比 capital turnover

### Phase E
- 单测：cycle_id tranche-sell 不影响其他 cycle inventory
- 生产监控：触发频率应接近零（若频繁 → Phase C' 失效）

### Phase G
- P0 验证：拿到 xuan trades 后，重跑报告里所有指标，看是否一致
- 若 maker/taker 比例显示 xuan 主要是 taker → Phase C' 假设需重审

---

## 开放问题（v3）

1. `PairArbCadenceTracker` 是 in-memory 还是 recorder-replay 计算？倾向 in-memory（实时 gate 需要）。
2. `same_side_run` 是否跨 round 重置？倾向**跨 round 重置**（不同 market 状态独立）。
3. `imbalance_half_life` 的 peak detection 用 EMA 还是简单局部 max？需小实验。
4. Phase G P0 完成前是否允许 Phase D' 上线？倾向**允许**（harvest 行为已强证实，与盲区无关）。
5. 我方 BUY-only 模式是否要替换部分现有 SELL derisk？需先看 Phase B' 数据中我们 SELL 的频率与场景。

---

## 已确认的 v3 设计决策

1. 核心变量：`run_length` + `half_life` + `clip_size`（不是 hazard）
2. Phase A 必做（cycle identity）
3. Phase B' 三件套指标 + 算法定义明确
4. Phase C' 用限制器形态（不依赖双边预埋假设）
5. Phase D' 阈值小、节奏快
6. Phase E 仅 abandon-sell，不做 FAK
7. Phase F deferred 或废弃
8. Phase G 优先级最高，P0 阻塞其他 Phase 部分上线
9. 证据基础：1000 trades + 95 markets（写入 plan 引用）
10. 算法定义全部前置明确（避免 v2 "settled vs working" 类的回头返工）

---

## 关联文档

- `docs/research/xuan/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 实证基础
- `docs/strategies/STRATEGY_PAIR_ARB_ZH.md` — 现有 pair_arb 实现说明
- `docs/runbooks/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md` — 上线检查清单（v3 上线前需更新）
