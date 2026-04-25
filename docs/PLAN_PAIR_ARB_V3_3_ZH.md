# Pair_Arb 升级计划 v3.3 — 三层架构 × 研究纪律 × 合并 Codex

> **生成日期**：2026-04-25
> **取代**：`v3.1` 三层架构 + `v3.2` 单 round patch + `PLAN_Codex.md` 研究纪律。本文档**自包含**，落地阶段不再需要回头读 v3.x / Codex。
> **目标**：定义"研究、会计、行为、安全"四个职责的**协作而非串行**关系，让基础设施与研究并行推进。
> **核心修订**：xuan 100% 还原**不是目标**；目标是"理解 xuan 的 edge 结构 → 量化我方差距 → 选择性吸收"。

---

## 0. 文档承袭与定位

| 文档 | 仍生效部分 | 已被 v3.3 修订部分 |
|------|----------|-------------------|
| `PLAN_PAIR_ARB_V3_1_ZH.md` | **三层架构**（会计 / 行为 / 安全）、TrancheLedger 设计、Phase A0/B0 / D0 / C1 / E1 骨架 | 发布矩阵（合并 v3.2 + Codex 后调整）、open question 清单 |
| `PLAN_PAIR_ARB_V3_2_ZH.md` | 单 round 6 大新发现（F1-F6）、Patch 1-6 设想 | Patch 1/2/3 全部**降级为 L2 待裁决假设**，shadow-only；不再算"修订" |
| `PLAN_Codex.md` | 研究问题树分层、独立证伪标准、研究库三对象、长窗 vs 单 round 二分 | Stage 0→4 串行 → 改为**与会计层并行**；100% 还原目标 → 改为"差距驱动" |
| `XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` | 全部（实证基础） | — |

---

## 1. 目标重定义

### 1.1 旧目标的问题

`PLAN_Codex.md` 默认目标为"`可观测层 100% 还原` → `机制层 100%`"。这是**战略缺陷**：
- xuan 资本规模、API 限流、资金成本与我们不同 → 100% 复制未必最优
- 复制目标隐含"xuan 是上限" → 排除我方现成的跨市场 arbiter 等独立 edge
- "100%" 是不可达 bar，会沦为 implementation gridlock 的借口

### 1.2 v3.3 新目标

**理解 xuan 的 edge 结构 → 量化我方差距 → 选择性吸收**：

1. **理解**：拆出他靠什么赚钱（pair discount / merge turnover / final-clip discount / residual / rebate）
2. **量化我方差距**：先把我方 pair_arb 现状指标（same_side_run / half_life / final_pair_cost / worst_MTM 分布）测出来
3. **选择性吸收**：差距最大的环节优先做；与我方既有 edge（cross-market arbiter、oracle-lag）正交的部分允许保留

**关键转变**：先比较，再模仿。**衡量我方现状是优先级 #1**，比研究 xuan 更直接。

---

## 2. 四职责架构（v3.1 三层 + 研究层）

```
┌─────────────────────────────────────────────────────┐
│ 研究层  Research (Codex 方法论)                      │
│   - xuan_round_summary / xuan_tranche_ledger /       │
│     xuan_round_path_features (in poly_trans_research)│
│   - L1 基础真相裁决 + L2 假设裁决                    │
└─────────────────────────────────────────────────────┘
                       ↕ (并行，不串行)
┌─────────────────────────────────────────────────────┐
│ 会计层  Accounting (v3.1，立即开干)                  │
│   - TrancheLedger        (inventory.rs)              │
│   - CadenceTracker       (coordinator_metrics.rs)    │
│   - 服务我方监控 + 服务我方 vs xuan 对比             │
└─────────────────────────────────────────────────────┘
                       ↓ (会计层稳定后)
┌─────────────────────────────────────────────────────┐
│ 行为层  Behavior                                      │
│   - pair_arb.rs       (现状，baseline)               │
│   - tranche_arb.rs    (新策略；按 L1+L2 裁决展开)    │
│   - PairArbHarvester  (D0 actor)                     │
└─────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│ 安全层  Safety (emergency-only, default off)         │
│   - FAK / SELL  保留代码，默认关闭                    │
│   - Abandon-sell (cycle_id-aware, A0 后可用)         │
└─────────────────────────────────────────────────────┘
```

**与 Codex 的关键差异**：研究层与会计层 **并行推进**。Codex 把会计层放 Stage 4 是错的——TrancheLedger 与 "xuan 是 maker 还是 taker" 完全无关，**不应被研究串行所阻塞**。

---

## 3. 会计层（立即开干，无前置裁决）

### 3.1 TrancheLedger（v3.1 原设计，保留）

```rust
// src/polymarket/inventory.rs
type TrancheId = u64;

pub struct Tranche {
    pub id: TrancheId,
    pub market_slug: String,
    pub opened_at: Instant,
    pub first_side: Side,
    pub first_clip_size: f64,

    pub up_qty: f64,        // 累计 YES BUY
    pub down_qty: f64,      // 累计 NO BUY
    pub merged_qty: f64,    // 已 merge 的 full-set 数

    pub live_gap: f64,      // (up_qty - merged_qty) - (down_qty - merged_qty)
    pub state: TrancheState,
    pub last_event_at: Instant,

    // v3.2 Patch 5 新增（用于多假设支持）
    pub mtm_history: VecDeque<(Instant, f64)>,
}

pub enum TrancheState { Open, Harvesting, Closed, Abandoned }

pub struct TrancheLedger {
    pub open: HashMap<TrancheId, Tranche>,
    pub recent_closed: VecDeque<Tranche>,
    next_id: u64,
}
```

**操作 API**：
- `open_tranche(market, side, qty) -> TrancheId`
- `add_fill(tranche_id, side, qty)`
- `apply_partial_merge(tranche_id, qty)` — merge **不**关闭 tranche
- `close_tranche(tranche_id, reason)`
- `find_active_for_market(market) -> Vec<TrancheId>`
- `record_mtm(tranche_id, mid_price)` — for L2 假设支持

**与聚合 VWAP 的关系**：`FillRecord` 新增 `tranche_id: Option<TrancheId>`，主聚合 VWAP **不动**，TrancheLedger 是 abandon-sell + harvest 的精确目标。

### 3.2 CadenceTracker（v3.1 原设计 + v3.2 Patch 5 字段扩展）

```rust
// src/polymarket/coordinator_metrics.rs
pub struct CadenceTracker {
    per_market: HashMap<String, MarketCadence>,
}

struct MarketCadence {
    fill_history: VecDeque<(Instant, Side, f64)>,
    current_run_side: Option<Side>,
    current_run_len: u32,
    half_life_samples: VecDeque<Duration>,
    last_imbalance_peak: Option<(Instant, f64)>,

    // v3.2 Patch 5（支持 L2 假设的指标累积，但暂不用于决策）
    ask_price_history: VecDeque<(Instant, Side, f64)>,
    book_depth_history: VecDeque<(Instant, Side, f64)>,
}
```

**算法定义**（落地前固化，避免事后返工）：
- `run`：`fill.side` 等于上一笔即延续；不同即重置；merge/redeem 不计
- `half_life`：从 `|net_diff|` 局部 peak 起算，到下一次 `|net_diff|` ≤ peak/2 的时刻；net_diff 反向越零提前算"完成衰减"
- `clip_size`：单笔 fill 数量

### 3.3 我方 baseline 测量（v3.3 新增，会计层落地后立即跑）

**目的**：在研究 xuan 之前先测我方差距。Codex 完全漏掉了这一步。

跑当前 pair_arb 1 周，输出：
| 指标 | 我方当前 | xuan 中位 | xuan p90 | gap |
|------|---------|----------|----------|-----|
| `same_side_run_length` | ? | 1 | 2 | 待测 |
| `imbalance_half_life` | ? | 24s | 111s | 待测 |
| `clip_size` 首笔 | ? | 170 | 397 | 待测 |
| `final_pair_cost` 中位 | ? | ? | ? | 待测 |
| `worst_intracycle_MTM` | ? | -$98 (n=1) | ? | 待测 |

**这一表格本身就是 v3.3 的 priority signal**：差距最大的指标决定行为层 patch 的优先级，**不是 Codex 假设的"按假设编号顺序裁决"**。

---

## 4. 研究层（与会计层并行，借鉴 Codex 方法论）

### 4.1 研究问题树（取自 Codex §研究问题树，保留分层）

#### L1：基础真相（任何行为层 enforce 前必须先解 ≥2 项）

| 编号 | 问题 | 数据需求 | 影响 |
|------|------|---------|------|
| BS-1 | `trades.side` = maker side or taker side | Authenticated CLOB `/trades` (`trader_side`/`maker_orders`/`taker_order_id`) | 决定我们学被 hit 的挂单几何还是 bounded taker 行为 |
| BS-2 | `BUY-only` 是真实还是 data-api 视角 | Polygonscan CTF token transfer 对账 + 长窗 trades/activity | 决定 SELL/FAK 是否仅保留为 emergency |
| BS-3 | 双边预埋 vs 单边触发后补腿 | open-order timeline；fallback：同步 book + trade tape + own order events | 决定 entry 骨架是 two-sided seed 还是 single-leg trigger |

**v3.2 已部分回答**：
- **BS-3a**（round-open 时刻）：F3 弱证据 = two-sided seed（T1+T2 同 timestamp）
- **BS-3b**（round 中段）：仍未答

#### L2：v3.2 提出的待裁决假设（每条独立证伪）

| 编号 | 假设 | 证伪标准 | 通过则 |
|------|------|---------|--------|
| H-1 | `final-clip discount capture` 是系统规律 | 多 round 里"末段大折扣"贡献 PnL > 30% 的 round 占比 ≥ 50% | 升格 `Phase H` 入行为层 |
| H-2 | `round-open two-sided seed` 是稳定起手式 | 多 round 里首两笔时间差 ≤ 1s 的 round 占比 ≥ 70% | C1-twoside (round-open) 默认开启 |
| H-3 | `clip size` 由 `best level depth` 驱动 | `clip_size ~ best_depth` 回归 R² ≥ 0.4，且 `best_depth` 系数稳定显著 | depth-aware sizing 入行为层 |
| H-4 | `abandon-sell` 应极度弱化 | 多 round 里"中段 worst MTM ≤ -$50 但终局 PnL > 0" 占比 ≥ 60% | abandon 阈值放至 240s+，且 MTM-aware |

**关键**：每个 H-x 的"证伪标准"是 v3.3 新增的硬阈值。Codex 给了证伪原则但没给数字；v3.2 给了 patch 但没给阈值。v3.3 把两者合上。

### 4.2 研究库三对象（取自 Codex，补 schema）

研究产物**全部进入 `poly_trans_research` 的统一研究库**，不再依赖一次性 HTML/JSON。

#### 4.2.1 `xuan_round_summary`（每 round 一行）

```sql
CREATE TABLE xuan_round_summary (
  round_slug TEXT PRIMARY KEY,
  round_open_ts TIMESTAMP,
  round_resolve_ts TIMESTAMP,
  outcome TEXT,                        -- 'Up' / 'Down' / 'inconclusive'
  trade_count INT,
  first_trade_offset_ms INT,           -- 距 round_open
  first_two_trades_gap_ms INT,         -- 支持 H-2 裁决
  same_side_run_length_p50 FLOAT,
  same_side_run_length_p90 FLOAT,
  half_life_median_ms INT,
  final_gap_ratio FLOAT,
  merge_count INT,
  redeem_count INT,
  final_pair_cost FLOAT,               -- 累计加权
  last_pair_sum FLOAT,                 -- 末段单 pair sum，支持 H-1
  last_pair_size FLOAT,
  worst_intracycle_mtm FLOAT,          -- 支持 H-4
  final_locked_pnl FLOAT,
  spent_total FLOAT,
  return_pct FLOAT
);
```

**支持查询**：
- H-1：`last_pair_sum < 0.95 AND last_pair_size > 0.3 * spent_total` 占比
- H-2：`first_two_trades_gap_ms <= 1000` 占比
- H-4：`worst_intracycle_mtm <= -50 AND final_locked_pnl > 0` 占比

#### 4.2.2 `xuan_tranche_ledger`（每 tranche 一行）

```sql
CREATE TABLE xuan_tranche_ledger (
  tranche_id BIGINT PRIMARY KEY,
  round_slug TEXT,
  opened_at TIMESTAMP,
  first_side TEXT,
  first_size FLOAT,
  up_qty FLOAT,
  down_qty FLOAT,
  pairable_qty FLOAT,
  merged_qty FLOAT,
  residual_qty FLOAT,
  worst_mtm FLOAT,
  final_pnl_contribution FLOAT
);
```

**注**：xuan 的 tranche 边界需要从 trade tape 推断（同 round 内多 MERGE 拆分）。我方 tranche 由 A0 直接产出。

#### 4.2.3 `xuan_round_path_features`（每 round 路径级特征）

```sql
CREATE TABLE xuan_round_path_features (
  round_slug TEXT PRIMARY KEY,
  round_open_seed_shape TEXT,          -- 'two_sided_simul' / 'single_then_chase' / 'other'
  largest_final_discount_pair_sum FLOAT,
  largest_final_discount_size FLOAT,
  clip_size_regime TEXT,               -- 'large_to_small_to_large' / 'uniform_small' / etc.
  burst_vs_tail_pattern TEXT,
  best_depth_corr_clip_size FLOAT      -- 支持 H-3
);
```

### 4.3 长窗报告 vs 单 round 深描（取自 Codex §数据与产物）

- **长窗报告**：回答"哪些现象稳定存在"，面向 H-x 升格判断
- **单 round 深描**：回答"在路径上怎么发生"，面向解释而不单独定参

**第一份单 round 深描模板**：`/Users/hot/web3Scientist/poly_trans_research/analysis_table.html` 对应的 round（v3.2 已建好），固化为 template。

---

## 5. 行为层（按 L1+L2 裁决进度展开）

### 5.1 已确定可做（不依赖 L1/L2）

#### Phase A0：TrancheLedger（会计基础设施）
依赖：—  
状态：立即开干

#### Phase B0：CadenceTracker（含 v3.2 Patch 5 字段）
依赖：A0  
状态：立即开干，纯观测

#### Phase D0：PairArbHarvester（独立 actor）
依赖：A0  
状态：A0 后立即 shadow / dry-run；阈值是 **initial defaults 待 scan**：
```
PM_PAIR_ARB_HARVEST_MIN_FULL_SET=10        # scan range [5, 50]
PM_PAIR_ARB_HARVEST_MIN_INTERVAL_SECS=20   # scan range [10, 60]
```
理由：rolling harvest 已被 1000 笔报告强证实（首次 MERGE 中位 180s + multi-tranche per market），与 L1/L2 都无关。

### 5.2 候选模块（L2 裁决前 shadow-only，**不进 enforce**）

#### Phase H：Final-clip Discount Capture
**门槛**：H-1 通过（多 round "末段大折扣贡献 PnL > 30%" 的 round 占比 ≥ 50%）  
**Shadow 形态**：在 tranche_arb stub 内识别 "best ask < recent_p10" 时机，只**记录会触发的 large clip 决策**到日志，**不下单**  
**通过后**：开 `PM_TRANCHE_FINAL_CLIP_ENABLED=1`，参数：
```
PM_TRANCHE_FINAL_CLIP_PRICE_PERCENTILE=10
PM_TRANCHE_FINAL_CLIP_LOOKBACK_SEC=120
PM_TRANCHE_FINAL_CLIP_MIN_PAIRED_QTY=200
```

#### C1-twoside (round-open 形态)
**门槛**：H-2 通过（首两笔时间差 ≤ 1s 占比 ≥ 70%）  
**Shadow 形态**：round-open 时刻同时下两侧 maker 的决策日志  
**通过后**：默认开启 round-open two-sided seed 路径

#### Depth-aware Clip Sizing
**门槛**：H-3 通过（`clip_size ~ best_depth` 回归 R² ≥ 0.4）  
**Shadow 形态**：B0 已有 depth 数据；让 strategy 计算"会用什么 size"并对比当前 size 写日志  
**通过后**：`clip_size = clamp(α * best_level_depth, min, max)`，α 初值 0.5

#### Abandon-sell 大放宽
**门槛**：H-4 通过（worst MTM ≤ -$50 但 final > 0 占比 ≥ 60%）  
**Shadow 形态**：A0 已有 mtm_history；统计我方 cycle 的 worst-MTM-to-final-PnL 关系，对比 xuan  
**通过后**：abandon 改为三条件：`tranche_age > 240s AND opp_blowout AND resolve_proximity < 30s`

### 5.3 永久 deferred

#### Phase F (dynamic pair_target)
被 Phase H 取代。删除。

---

## 6. 安全层（v3.1 原则保留，不删除任何路径）

| 路径 | 状态 | 默认 | 升级条件 |
|------|------|------|---------|
| FAK 补腿 | 保留代码 | off | BS-1 + BS-2 解决后重新评估；**永不删除** |
| SELL derisk | 保留代码 | tranche_arb 模式下 off | BS-2 解决（确认真 BUY-only）后才考虑 tranche_arb 内禁用 |
| Abandon-sell (tranche-aware) | 新增 | off | A0 + H-4 裁决后启用 |

**硬规则**：任何"删除 FAK / 完全禁用 SELL"的 PR 必须在描述里引用 BS-X 解决证据。

---

## 7. 时限与 fallback（v3.3 新增，修正 Codex 缺失）

### 7.1 Stage timeline（硬 deadline）

| Stage | 内容 | 上限工期 | 失败 fallback |
|-------|------|---------|--------------|
| S0 | 研究基线固化（`xuan_round_summary` 跑通现有 95 round） | **1 周** | 无；S0 必须达成才能进 S1 |
| S1 | 多 round 高粒度重建（覆盖 2-4 周窗口） | **2 周** | 若数据 API 受限，先用现有 1000 笔做最优可得 baseline |
| S2 | L2 假设逐项裁决（H-1..H-4） | **每条 ≤ 1 周** | 若数据不足以裁决，标记"无法裁决"并维持 shadow-only，不阻塞其他假设 |
| S3 | 我方 baseline 测量 | **1 周**，与 S1 并行 | 我方 1 周生产数据足够 |
| **会计层 A0/B0/D0** | 实现 + 上线 | **2 周**，与 S0+S1 并行 | — |

**总长**：会计层 2 周；研究层 ~5 周；最坏情况 5 周后行为层开始按裁决结果展开。

### 7.2 数据 fallback 矩阵

| 数据 | 首选 | fallback-1 | fallback-2 |
|------|------|-----------|-----------|
| Authenticated CLOB `/trades` | API key 申请通过 | data-api 公开 trades + 我方 maker 反推 | 仅用我方 fill stream，xuan side 留 unknown |
| Open-order timeline | user-channel 历史回放 | 同步 book + trade tape 反推 | 接受 BS-3 永远只有"部分回答"（BS-3a + F3） |
| 链上 split/merge/redeem | Polygonscan 全量 | RPC 节点定向查询 | 接受 BS-2 仅由 trade tape 推断 |
| 长窗 trades 2-4 周 | data-api 持续抓取 | 分批短窗拼接 | 接受 1-2 周窗口 |

**原则**：每个 H-x 的 shadow → enforce 转换允许在"次优数据 + 强 shadow 证据"路径上进行，不要求 100% 首选数据。

### 7.3 PnL 归因 bar 修订

Codex 要求"80% 盈利 round 能解释主要利润来源属于 ≤5 类之一"——可能不可达。

**v3.3 修订**：
- 把"主要利润来源"改为 **TOP-2 类合计 ≥ 70%**
- 4 类够用：`pair_discount` / `merge_turnover` / `final_clip_discount` / `residual_payout`
- rebate 因公开数据无法分离，单独标 `unattributable`，不算入 70% 分母

---

## 8. 发布矩阵（v3.3 合并版）

| 项 | 阻塞条件 | 可 shadow / dry-run | 可 enforce / 上线 |
|----|---------|--------------------|--------------------|
| **A0 TrancheLedger** | — | n/a (基础设施) | 立即（≤ 2 周内） |
| **B0 CadenceTracker** | A0 | n/a (观测) | 立即 |
| **我方 baseline 报告** | A0 + B0 | — | A0+B0 上线 1 周后输出 |
| **D0 Harvester** | A0 | A0 后立即 | dry-run 7d → 小流量 → 全量 |
| **S0/S1/S2 研究产物** | — | 外部，并行 | 见 §7.1 deadline |
| **C1-stub tranche_arb 骨架** | A0 + B0 | 立即 | n/a (空跑) |
| **C1-single tranche_arb** | A0 + B0 + BS-1 解 | 立即 | BS-1 解 + 2 周 shadow |
| **C1-twoside (round-open)** | C1-single + H-2 通过 | A0+B0 后立即 | H-2 通过 + 2 周 shadow |
| **C1-twoside (mid-round)** | C1-single + BS-3b 解 | BS-3b partial 后 | BS-3b 解 + 4 周 shadow |
| **Phase H (final-clip)** | A0 + B0 + H-1 通过 | A0+B0 后立即 | H-1 通过 + 2 周 shadow |
| **Depth-aware sizing** | A0 + B0 + H-3 通过 | A0+B0 后立即 | H-3 通过 + 2 周 shadow |
| **E1 Abandon-sell (放宽阈值)** | A0 + H-4 通过 | A0 后立即 | H-4 通过 + 4 周生产监控 |
| **FAK 路径删除** | BS-1 + BS-2 同时解 | n/a | **永不允许删代码**，仅切 default |
| **SELL 路径完全禁用 (tranche_arb 内)** | BS-2 解 | n/a | tranche_arb 模式下；pair_arb baseline 不动 |
| **Phase F (dynamic target)** | — | n/a | **删除** |

**硬规则**：
- 任何 enforce 行 若依赖 BS-X 或 H-x，**PR 描述必须引用解决证据**
- 任何 shadow 路径**至少跑 1 周**才能进 enforce 评审
- A0/B0/D0 **不被任何研究 gating 阻塞**——这是 v3.3 与 Codex 的核心分歧

---

## 9. Critical Files

### 立即开干（会计层）
- `src/polymarket/inventory.rs` — `TrancheLedger` + `Tranche` + `FillRecord.tranche_id`
- `src/polymarket/messages.rs` — tranche events, `FillRecord` 字段
- `src/polymarket/coordinator_metrics.rs` — `CadenceTracker` 全部字段（含 v3.2 Patch 5 三个新字段）
- `src/polymarket/coordinator.rs` — cycle 生命周期编排、cadence 接线
- `src/polymarket/recorder.rs` — tranche events + cadence snapshots

### 行为层（按裁决进度逐步开发）
- `src/polymarket/strategy/tranche_arb.rs` — **新文件**，C1-stub 骨架先建
- `src/bin/polymarket_v2.rs` — `PairArbHarvester` actor

### 安全层
- `src/polymarket/coordinator_order_io.rs` — 新 `dispatch_tranche_sell`（不复用 `dispatch_taker_derisk`）

### 不动
- `src/polymarket/strategy/pair_arb.rs` — baseline 保留
- `src/polymarket/inventory.rs::recompute_state_from_records` 聚合 VWAP — 不动
- `src/bin/polymarket_v2.rs::run_capital_recycler` — 不动（语义不同）
- 现有 FAK / SELL 路径 — 不动，仅 default 切换

### 研究层（在 `poly_trans_research`，不在 pm_as_ofi）
- `xuan_round_summary` / `xuan_tranche_ledger` / `xuan_round_path_features` 表
- 长窗自动重建脚本
- 单 round 深描模板（已有 `analysis_table.html`）

---

## 10. Verification

### A0 / B0
- 单测：并发开/关 tranche、partial merge 不丢账、abandon 不影响其他 tranche；算法定义边界 case
- 1 周生产数据 + 与 xuan 中位/p90 对比表
- replay：recorder 回放历史 fill stream，tranche 数与人工标注一致

### D0
- dry-run 7 天验证触发频率合理
- 参数扫描：`min_full_set ∈ [5,10,20,50]` × `interval ∈ [10,20,60]`
- 单市场 A/B：A 启用、B 不启用，对比 capital turnover

### L2 假设裁决（每个 H-x）
- 用 `xuan_round_summary` SQL 直接算占比 / 回归 R²
- 通过则把数字写入 v3.3 / commit message
- 不通过则 H-x 永久 deferred，文档明确标注

### 行为层（每个 enforce 切换）
- shadow ≥ 2 周；指标必须达到对应目标值
- enforce 后 1 周 PnL & 指标对比 baseline
- 任何指标恶化 → 自动回退 shadow

---

## 11. 开放问题（v3.3 合并清单）

### 来自 v3.1
1. `strategy_mode` 切换粒度：per-market 还是全局？倾向 per-market
2. `TrancheLedger` 持久化：进程崩溃恢复策略？倾向：active tranches 在 user-channel WS 重连时从 fill stream 重建
3. `tranche_arb` 与 `oracle_lag_sniping` 互动：oracle_lag 启动时 tranche_arb 是否暂停？倾向暂停
4. `CadenceTracker` 内存上限：fill history 多大？倾向 N=50/market

### 来自 v3.2
5. F1 末段大折扣每 round 都出现吗？→ 由 H-1 裁决
6. F3 同秒推断能扩展到全部 round 吗？→ 由 H-2 裁决
7. clip size 是与 depth 还是 inventory state 相关？→ 由 H-3 裁决
8. Phase H 与 D0 同时触发时的优先级？倾向先 final-clip（拉低 pair cost 优先于 turnover）
9. MTM-aware abandon 的 MTM 计算口径？倾向 best bid（保守）

### 来自 Codex
10. PnL 归因 bar 是否需进一步降低？v3.3 已降至 70%；上线后看可达性
11. 长窗报告自动化程度：完全 cron 还是半手动？倾向 cron + 异常告警

### v3.3 新增
12. **我方 baseline 测出来如果指标已经接近 xuan 中位**，是否需要 tranche_arb？还是只需要小修 pair_arb？倾向：差距 < 20% 不另开策略
13. **数据 fallback-2 路径下能否做出 shadow → enforce 的判断**？需 S2 阶段实证
14. **Phase H + Depth-aware sizing 同时通过时**，先做哪个？倾向 H-1 优先（PnL 贡献直接）

---

## 12. v3.3 已确认设计决策（最终）

1. **目标**：理解 xuan edge 结构 + 量化我方差距 + 选择性吸收（**不**追求 100% 还原）
2. **架构**：四职责（研究 / 会计 / 行为 / 安全），研究与会计**并行**，不串行
3. **会计层立即开干**：A0 + B0 + D0 不被任何研究 gating 阻塞
4. **行为层按 L1+L2 裁决进度展开**：H-1..H-4 每条独立证伪，shadow 至少 2 周才能 enforce
5. **安全层永不删代码**：FAK / SELL 仅 default 切换；abandon-sell 由 H-4 裁决放宽
6. **研究层借鉴 Codex 方法论**：研究问题树分层、独立证伪标准、研究库三对象、长窗 vs 单 round 二分
7. **修正 Codex 缺失**：补 deadline、补 fallback、PnL 归因 bar 降至 70%、补我方 baseline 优先
8. **TrancheLedger** 取代 cycle_id 标签；merge ≠ cycle close（partial harvest）
9. **Phase F 删除**；**Phase H 候选**待 H-1 裁决
10. **任何路径删减必须引用 BS-X 解决证据**

---

## 13. 关联文档

- `docs/PLAN_PAIR_ARB_V3_2_ZH.md` — 单 round 6 大新发现（F1-F6 实证）
- `docs/PLAN_PAIR_ARB_V3_1_ZH.md` — 三层架构基线（v3.3 仍引用其设计）
- `docs/PLAN_PAIR_ARB_V3_ZH.md` — v3 (superseded)
- `docs/PLAN_Codex.md` — 研究方法论来源（v3.3 吸收其分层与证伪原则）
- `docs/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 1000 笔统计研究
- `/Users/hot/web3Scientist/poly_trans_research/analysis_table.html` — v3.2 单 round 证据
- `/Users/hot/web3Scientist/poly_trans_research/trades.json` — v3.2 单 round 原始数据
- v3.3 上线前 待补：
  - `docs/STRATEGY_TRANCHE_ARB_ZH.md`（行为规范文档 v1，Codex Stage 3 产物）
  - `docs/GO_LIVE_TRANCHE_ARB_CHECKLIST_ZH.md`
  - `poly_trans_research` 研究库 schema migration
