# Pair_Arb 升级计划 v3.1

> ⚠️ **本文档已被 `docs/PLAN_PAIR_ARB_V3_2_ZH.md` 增量修订** — v3.2 基于单 round 高粒度交易细节加入 6 项 patch（Phase H 收尾大 clip / abandon-sell 阈值放宽 / depth-aware clip size / BS-3 部分回答 / CadenceTracker 字段扩展 / 发布矩阵更新）。本文档保留作为 v3.1 三层架构基线。

---

# Pair_Arb 升级计划 v3.1

> **生成日期**：2026-04-25
> **取代**：`docs/PLAN_PAIR_ARB_V3_ZH.md`（v3 抓对了研究方向，但实施上仍在补丁旧 pair_arb；v3.1 重写实施骨架）
> **证据基础**：`docs/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 1000 笔 trades + 95 个连续 BTC 5m markets
> **核心原则**：研究、会计、执行三层分离

---

## 0. v3 → v3.1 的 6 项结构性修订

| # | v3 错误 | v3.1 修订 |
|---|---------|----------|
| 1 | 把 `BUY-only`、`side 语义` 当设计依据 → 删 FAK / 压 SELL | 降级为**待证伪假设**；FAK/SELL 路径**保留**，仅默认关闭，emergency-only |
| 2 | 在旧 `pair_arb.rs::compute_quotes` 末端塞 `ClipDecision` | **新开 `tranche_arb` 策略线**；旧 pair_arb 保持原样作 baseline |
| 3 | `Phase A` 仅给 fill 打 `cycle_id` 标签 | **升级为 `TrancheLedger`** 显式数据结构；merge 不是 cycle close 而是 tranche partial harvest |
| 4 | Phase D' 阈值（`full_set=10`、`interval=20s`）写成"v3 修订"已知值 | 降级为 **initial defaults，待参数扫描**；上界由 tx latency 兜底 |
| 5 | A → B' → C' → D' → E（C 在 D 之前） | **重排为 A0 → B0 → D0 → (G-P0 并行) → C1 → E1**；与盲区相关性低的先做 |
| 6 | gating 散落在各 Phase 段落 | **新增发布矩阵**，明确 blocking conditions |

---

## 1. 三层架构

```
┌──────────────────────────────────────────────────────┐
│ 会计层  Accounting (foundational, strategy-agnostic) │
│   - TrancheLedger        (inventory.rs)              │
│   - CadenceTracker       (coordinator_metrics.rs)    │
│   - 共享基础设施，pair_arb / tranche_arb 都消费       │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│ 行为层  Behavior                                      │
│   - pair_arb.rs           (现状保留，baseline)        │
│   - tranche_arb.rs (新)   (天然以 tranche/clip 为核心)│
│   - PairArbHarvester (新) (独立 actor, 与策略解耦)    │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│ 兜底层  Safety (emergency-only, default off)          │
│   - FAK 补腿     (保留路径，默认关闭)                  │
│   - SELL derisk  (保留路径，默认关闭)                  │
│   - Abandon-sell (新增 cycle_id-aware 路径)           │
└──────────────────────────────────────────────────────┘
```

**关键原则**：
- 会计层是事实，所有策略共享
- 行为层是策略，互相独立可 A/B
- 兜底层是 hardware backup，不写"删除"，只写"默认关闭 + 升级条件"

---

## 2. 仍未解决的盲区（v3.1 的 release gate 直接引用）

| 编号 | 盲区 | 影响范围 | 裁决方式 |
|------|------|---------|---------|
| BS-1 | 公开 `side` 是 maker side 还是 taker side | tranche_arb 策略形态（双边预埋 vs 触发后补腿）| Authenticated CLOB `/trades` (`trader_side`/`maker_orders`/`taker_order_id`) |
| BS-2 | BUY-only 是 data-api 过滤还是真实 | 是否可以削减 SELL 执行路径 | Polygonscan CTF transfer 对账 `0xcfb103c37c0234f524c632d964ed31f117b5f694` |
| BS-3 | Open order 时序：双边预埋 vs 单边触发后补腿 | tranche_arb 内部状态机分支 | User-channel WebSocket book + own order events |

---

## 3. 会计层

### 3.1 TrancheLedger（取代 v3 的 `cycle_id`）

```rust
// src/polymarket/inventory.rs
type TrancheId = u64;

#[derive(Clone, Debug)]
pub struct Tranche {
    pub id: TrancheId,
    pub market_slug: String,
    pub opened_at: Instant,
    pub first_side: Side,
    pub first_clip_size: f64,

    pub up_qty: f64,        // 累计 YES BUY 进入本 tranche
    pub down_qty: f64,      // 累计 NO BUY
    pub merged_qty: f64,    // 本 tranche 已 merge 的 full-set 数

    pub live_gap: f64,      // (up_qty - merged_qty) - (down_qty - merged_qty)
    pub state: TrancheState, // Open / Harvesting / Closed / Abandoned
    pub last_event_at: Instant,
}

pub enum TrancheState { Open, Harvesting, Closed, Abandoned }

pub struct TrancheLedger {
    pub open: HashMap<TrancheId, Tranche>,
    pub recent_closed: VecDeque<Tranche>, // bounded, for replay/metrics
    next_id: u64,
}
```

**关键操作**：
- `open_tranche(market, side, qty) -> TrancheId`
- `add_fill(tranche_id, side, qty)`
- `apply_partial_merge(tranche_id, qty)` — merge 不关闭 tranche
- `close_tranche(tranche_id, reason)` — 仅在显式触发（leftover redeemed / abandon-sell 完成）
- `find_active_for_market(market) -> Vec<TrancheId>`

**与 `FillRecord` 的关系**：`FillRecord` 新增 `tranche_id: Option<TrancheId>`，但**主聚合 VWAP 不动**。聚合 VWAP 仍是 strategy 的 fallback；TrancheLedger 是 abandon-sell 与 harvest 的精确目标。

### 3.2 CadenceTracker

```rust
// src/polymarket/coordinator_metrics.rs
pub struct CadenceTracker {
    per_market: HashMap<String, MarketCadence>,
}

struct MarketCadence {
    fill_history: VecDeque<(Instant, Side, f64)>, // bounded N (e.g., 50)
    current_run_side: Option<Side>,
    current_run_len: u32,
    half_life_samples: VecDeque<Duration>,        // bounded
    last_imbalance_peak: Option<(Instant, f64)>,
}
```

**算法定义**（plan 阶段写死，避免歧义）：
- `run`：`fill.side` 等于上一笔即延续；不同即重置；merge/redeem 不计
- `half_life`：从 `|net_diff|` 到达局部 peak 起算，到下一次 `|net_diff|` ≤ peak/2 的时刻；net_diff 反向越零提前算"完成衰减"
- `clip_size`：单笔 fill 数量

**输出**（30 分钟一次到 `📊 StrategyMetrics`）：
- `same_side_run_length` p50 / p90 / `runs > 2` 计数
- `imbalance_half_life` p50 / p90
- `clip_size` p50 / p90 / max
- (cross-validation) `first_opposite_delay` / `first_cover_delay`

**Cross-validation 目标**（来自 xuan 报告）：
| 指标 | xuan 中位 | xuan p90 | 我们目标 |
|------|----------|----------|---------|
| `same_side_run_length` | 1 | 2 | ≤ 2 |
| `imbalance_half_life` | 24s | 111s | ≤ 30s |
| `clip_size` (首笔) | 170 | 397 | 类似量级 |

---

## 4. 行为层

### 4.1 pair_arb.rs（现状保留）

不动。继续作 baseline。enforce v3.1 后**与 tranche_arb 通过 strategy mode flag 互斥**。

### 4.2 tranche_arb.rs（新）

**设计原则**：天然以 tranche / clip / harvest cadence 为核心，不从 pair_target 反推。

**状态机**：
```
Idle
  └─ should_open_first_clip(regime, edge) → Yes → OpenedFirst
OpenedFirst (cadence: run_len=1, side=S)
  ├─ opposite_side_filled → PartialPair
  ├─ same_side_filled (run_len < cap) → still OpenedFirst (run_len++)
  └─ same_side_filled (run_len == cap) → BlockedSameSide (cool-off)
PartialPair (both sides have qty)
  ├─ paired_qty ≥ harvest_threshold → ReadyToHarvest
  ├─ live_gap 持续大 → ConsiderEscalate (BS-3 解决前不实装)
  └─ paired_qty 增长 → still PartialPair
ReadyToHarvest
  └─ harvester triggered, merged_qty++ → Idle (or PartialPair if leftover)
Abandoned
  └─ E1 触发后状态
```

**关键决策**（每个都是独立函数 + 独立 config）：
- `should_open_first_clip(coord, market, regime, edge) -> Option<TwoSidedSeed | SingleLegOpen>`
  - **形态由 BS-3 决定**；BS-3 解决前**只支持 SingleLegOpen**，two-sided 留 stub
- `should_add_same_side_clip(tranche, cadence) -> Option<f64>` — 受 run_length cap 约束
- `should_request_harvest(tranche) -> bool` — 委派给 harvester
- `should_escalate(tranche) -> EscalationDecision` — 默认 `NoEscalation`，BS-1/BS-2 解决后才扩展

**接入点**：
- `src/polymarket/strategy/tranche_arb.rs` 新文件
- `coordinator.rs` 加 `strategy_mode: { PairArb, TrancheArb }` 路由
- `tranche_arb` 直接消费 `TrancheLedger` + `CadenceTracker`，**不 reuse** `compute_quotes`

### 4.3 PairArbHarvester（独立 actor）

**修订要点**（vs v3）：
- 阈值是 **initial defaults**，不是研究证明值
- `min_full_set` 下界由 gas-equivalent overhead 决定（即使 gasless，relayer 容量也有限）；上界由 capital turnover 需求决定
- 参数扫描是 D0 的明确 deliverable

**触发条件（initial defaults）**：
- `paired_qty ≥ harvest_min_full_set` (init: 10, scan: [5, 50])
- 距上次 harvest ≥ `harvest_min_interval_secs` (init: 20, scan: [10, 60])
- 非 abandon-sell 进行中
- 两侧无活跃 first-leg-pending order
- (可选) 单 tranche `imbalance_half_life > 60s` → 优先 harvest 该 tranche

**Config**：
```
PM_PAIR_ARB_HARVEST_ENABLED=0
PM_PAIR_ARB_HARVEST_MIN_FULL_SET=10           # initial default, requires scan
PM_PAIR_ARB_HARVEST_MIN_INTERVAL_SECS=20      # initial default, requires scan
PM_PAIR_ARB_HARVEST_HALF_LIFE_TRIGGER_MS=60000
```

---

## 5. 兜底层

### 5.1 FAK 补腿路径（保留，默认关闭）
- 现有路径**不删除**
- 默认 `PM_FAK_RESCUE_ENABLED=0`
- 升级条件：BS-1 + BS-2 解决后，若数据显示 xuan 有 taker 行为 → 重新评估

### 5.2 SELL derisk 路径（保留，默认弱化）
- 现有 `dispatch_taker_derisk` 不动（被 pair_arb 等其他策略使用）
- tranche_arb 模式下默认不调用
- 升级条件：BS-2 解决（确认 BUY-only 是真实而非 data-api 过滤）后，可显式禁用 tranche_arb 下的 SELL 路径

### 5.3 Abandon-sell（新，A0 完成后可用）
- 新函数 `dispatch_tranche_sell(tranche_id, ...)` — 精确扣减指定 tranche，不动聚合池
- 触发条件极保守：`tranche_age > 180s` 且 `paired_qty == 0` 且 cadence-clip-limiter 已限制后仍无对侧
- Config：
```
PM_TRANCHE_ABANDON_SELL_ENABLED=0
PM_TRANCHE_ABANDON_AFTER_MS=180000
```

---

## 6. Phase 顺序（v3.1 重排）

```
A0  TrancheLedger                           （会计基础设施）
 │
 ├─ B0  CadenceTracker (观测 only)          （会计观测）
 │
 ├─ D0  PairArbHarvester                    （兜底 actor + 参数扫描）
 │     ├─ shadow / dry-run 阶段
 │     └─ 小流量 enforce
 │
 ├─ E1  Abandon-sell (依赖 A0 ledger)       （兜底层；默认 off）
 │
 └─ C1  tranche_arb 策略 (依赖 A0+B0+G)     （行为层；新策略线）
       ├─ shadow 阶段
       └─ G 充分后 enforce

G   xuan 数据采集（外部并行）
    ├─ G-1  Polygonscan 对账           （quick, 解 BS-2）
    ├─ G-2  Authenticated CLOB         （medium, 解 BS-1）
    └─ G-3  Open order timeline         （slow, 解 BS-3）

F   Dynamic pair_target                    （删除）
```

---

## 7. 发布矩阵（v3.1 新增）

| 项 | 阻塞条件 | 可 shadow / dry-run | 可 enforce / 上线 |
|----|---------|--------------------|--------------------|
| **A0 TrancheLedger** | — | n/a (basic infra) | 立即 |
| **B0 CadenceTracker** | A0 | n/a (观测) | 立即 |
| **D0 Harvester** | A0 | 立即 | dry-run 7d 后小流量 → 全量 |
| **G-1 Polygonscan 对账** | — | 外部任务 | quick (hours) |
| **G-2 Authenticated CLOB** | API key | 外部任务 | medium (days) |
| **G-3 Open order timeline** | G-2 | 外部任务 | slow (weeks) |
| **C1-stub tranche_arb 状态机骨架** | A0 + B0 | 立即 | n/a (空跑) |
| **C1-single tranche_arb single-leg open** | A0 + B0 + G-1 | 立即 | G-2 后 + 2w shadow |
| **C1-twoside tranche_arb two-sided seed 形态** | C1-single + G-3 | G-3 后 | G-3 后 + 4w shadow |
| **E1 Abandon-sell** | A0 + C1-single enforce | A0 后 | 极保守阈值，监控触发率 |
| **FAK 路径删除** | BS-1 + BS-2 | n/a | **不允许，永远保留代码** |
| **SELL 路径完全删除** | BS-2 | n/a | **不允许，仅在 tranche_arb 模式下默认关** |
| **F Dynamic target** | — | n/a | **删除** |

**硬规则**：
- 任何 `enforce / 上线` 列若依赖 BS-X，必须先在 plan/PR description 引用解决证据
- 兜底路径**永不删除代码**，只切 default flag

---

## 8. Critical Files

### 新增
- `src/polymarket/strategy/tranche_arb.rs`（新策略）
- `src/polymarket/inventory.rs` 内 `TrancheLedger`（会计）
- `src/polymarket/coordinator_metrics.rs` 内 `CadenceTracker`（会计）
- `src/bin/polymarket_v2.rs` 内 `PairArbHarvester` actor（兜底）

### 修改
- `src/polymarket/messages.rs` — `FillRecord.tranche_id`、tranche events
- `src/polymarket/coordinator.rs` — `strategy_mode` 路由、cadence/ledger 接线
- `src/polymarket/coordinator_order_io.rs` — 新 `dispatch_tranche_sell`（不复用 `dispatch_taker_derisk`）
- `src/polymarket/recorder.rs` — tranche events + cadence snapshots

### 不动
- `src/polymarket/strategy/pair_arb.rs` — 保持 baseline
- `src/polymarket/inventory.rs::recompute_state_from_records` 聚合 VWAP — 不动
- `src/bin/polymarket_v2.rs::run_capital_recycler` — 不动（语义不同）
- 所有 FAK / SELL 现有路径 — 不动，仅 default off

---

## 9. Verification

### A0 TrancheLedger
- 单测：并发开/关 tranche、partial merge 不丢账、abandon 不影响其他 tranche
- replay：recorder 回放 1 周生产 fill stream，tranche 数量与人工标注一致

### B0 CadenceTracker
- 单测：算法定义边界 case（peak detection、reverse-zero、merge 不计入 run）
- 跑 1 周生产数据，对比指标 vs xuan 中位/p90

### D0 Harvester
- dry-run 7 天验证触发频率合理（不要每秒 merge）
- **参数扫描**：`min_full_set ∈ [5, 10, 20, 50]` × `interval ∈ [10, 20, 60]`，按 capital turnover / merge tx 成功率打分
- 单市场 A/B（A 启用、B 不启用）

### C1 tranche_arb
- shadow 1–2 周：日志对比"会被开仓 / 会被同侧 cap"vs"实际行为"
- enforce 后 7 天：cadence 指标是否落到目标
- PnL 对比：tranche_arb 单市场 vs pair_arb 单市场（同期、相邻 round）

### E1 Abandon-sell
- 单测：tranche-sell 不动其他 tranche
- 生产监控：触发频率应接近零（频繁触发 → C1 失效）

### G
- G-1 完成后：重跑 BUY-only 检验
- G-2 完成后：用 trader_side 重跑 alternation 指标
- G-3 完成后：决定 C1-twoside 是否开发

---

## 10. 开放问题

1. **strategy_mode 切换粒度**：per-market 还是全局？倾向 per-market（允许 tranche_arb 在 BTC 5m，pair_arb 在其他题材）
2. **TrancheLedger 持久化**：进程崩溃恢复策略？倾向：active tranches 在 user-channel WS 重连时从 fill stream 重建
3. **tranche_arb 与 oracle_lag_sniping 的互动**：oracle_lag 启动时 tranche_arb 是否暂停？倾向暂停（避免双策略竞争同 market）
4. **CadenceTracker 内存上限**：fill history 多大？倾向 N=50/market，bounded VecDeque
5. **Phase G-1 谁做**：Polygonscan 对账可由用户脚本完成；建议用户在 G-1 完成后回填本 plan 引用

---

## 11. 已确认的 v3.1 设计决策

1. 三层分离：会计 / 行为 / 兜底
2. **不**在 `pair_arb.rs` 上打补丁；新建 `tranche_arb.rs`
3. **TrancheLedger** 取代 cycle_id 标签
4. Merge **不**等于 cycle close（是 partial harvest）
5. FAK / SELL 路径**不删除**，仅默认关闭
6. Phase D 阈值是 initial default，需参数扫描
7. Phase 顺序：A0 → B0 → D0 → (G 并行) → C1 → E1
8. 发布矩阵硬性 gate 各阶段
9. F 永久删除
10. 任何路径删减必须引用 BS-X 解决证据

---

## 12. 关联文档

- `docs/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 实证基础
- `docs/PLAN_PAIR_ARB_V3_ZH.md` — 前一版（v3，已被本文档取代）
- `docs/STRATEGY_PAIR_ARB_ZH.md` — 现有 pair_arb 实现说明（baseline）
- `docs/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md` — pair_arb 上线检查清单
- v3.1 上线前需新建：`docs/STRATEGY_TRANCHE_ARB_ZH.md` + `docs/GO_LIVE_TRANCHE_ARB_CHECKLIST_ZH.md`
