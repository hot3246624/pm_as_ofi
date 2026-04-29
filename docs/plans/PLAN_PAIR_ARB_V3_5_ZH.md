# Pair_Arb 升级计划 v3.5 — `v3.3` 研究纪律 × `v3.4` 操作纪律

> **生成日期**：2026-04-25  
> **取代**：`docs/plans/PLAN_PAIR_ARB_V3_3_ZH.md` 与 `v3.4` 合并评审结论  
> **定位**：`v3.5` 不是另起炉灶，而是以 `v3.3` 为主线，吸收 `v3.4` 的有效增强，并明确拒绝 `v3.4` 对目标、阈值、fallback、时间盒的回退。  
> **主旨**：研究层继续尽最大可能还原 `xuanxuan008` 的机制；工程层只上线已裁决、且对我方确有显著差距的部分。

---

## 0. 文档承袭与取舍

| 来源 | 保留 | 拒绝/修订 |
|------|------|-----------|
| `PLAN_PAIR_ARB_V3_3_ZH.md` | 四职责架构、A0/B0/D0 并行、L1/L2 裁决、H-1..H-4 硬阈值、fallback 矩阵、时间盒、PnL 归因 bar | `目标` 表述需要拆成研究北极星与工程北极星，避免被误读成“只做选择性吸收” |
| `v3.4` 评估后续版本 | baseline 前置、`永远不做` 清单、2 周 shadow、PR 必引证据、D0 降级为观测/工具 | 改回“100% 还原”作为唯一目标、稀释 H-1..H-4 的数字门槛、删除 PnL 归因 bar、删除 fallback、弱化时间盒 |
| `PLAN_PAIR_ARB_V3_2_ZH.md` | 单 round 深描作为 L2 假设来源 | 单 round patch 不得直接进 enforce |
| `PLAN_Codex.md` | 研究问题树、研究库三对象、长窗 vs 单 round 二分 | 串行 Stage 0→4；会计层不应被研究阻塞 |

---

## 1. 目标与北极星

### 1.1 研究北极星

**尽最大可能理解并还原 `xuanxuan008` 的 edge 结构与机制。**

这意味着研究阶段仍然追求：

- 弄清他靠什么赚钱
- 弄清他的 entry / sizing / inventory / merge / residual 机制
- 最大限度裁决 `maker/taker`、`two-sided seed`、`final-clip`、`depth-aware sizing` 这些关键问题

### 1.2 工程上线目标

**差距驱动、选择性吸收、只上线已裁决部分。**

这意味着工程层不以“100% 还原”作为发布门槛，而以：

1. 我方与 `xuan` 的 gap 是否显著  
2. 对应机制是否已被裁决  
3. 上线风险是否可控

作为唯一上线依据。

### 1.3 一句话原则

> 研究上尽量还原，工程上只吸收已证实、且确实值得吸收的部分。

---

## 2. 四职责架构

```text
研究层  Research
  - xuan 研究库
  - L1 基础真相裁决
  - L2 假设裁决

会计层  Accounting
  - TrancheLedger
  - CadenceTracker
  - 我方 baseline 测量

行为层  Behavior
  - pair_arb.rs 作为 baseline
  - tranche_arb.rs 作为新策略骨架
  - PairArbHarvester 作为独立 actor

安全层  Safety
  - FAK / SELL 保留代码
  - Abandon-sell 仅在 tranche-aware 条件下启用
```

### 核心关系

- **研究层与会计层并行推进**，不串行。
- **会计层不被任何外部研究阻塞**。
- **行为层只消费研究层的裁决结果**，不消费“看起来合理”的猜想。
- **安全层永不删代码**，只切默认值。

---

## 3. 会计层（立即开干，无前置裁决）

### 3.1 A0 — `TrancheLedger`

在 [`src/polymarket/inventory.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/inventory.rs) 引入显式 tranche 会计：

```rust
type TrancheId = u64;

pub struct Tranche {
    pub id: TrancheId,
    pub market_slug: String,
    pub opened_at: Instant,
    pub first_side: Side,
    pub first_clip_size: f64,
    pub up_qty: f64,
    pub down_qty: f64,
    pub merged_qty: f64,
    pub live_gap: f64,
    pub state: TrancheState,
    pub last_event_at: Instant,
    pub mtm_history: VecDeque<(Instant, f64)>,
}

pub enum TrancheState {
    Open,
    Harvesting,
    Closed,
    Abandoned,
}
```

固定原则：

- `merge != close`
- `apply_partial_merge(tranche_id, qty)` 只减少该 tranche 的 pairable 部分
- `FillRecord` 增加 `tranche_id: Option<TrancheId>`
- 聚合 `InventoryState` 与 `recompute_state_from_records()` 保持不变，只把 tranche 作为精确归因层

### 3.2 B0 — `CadenceTracker`

在 [`src/polymarket/coordinator_metrics.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_metrics.rs) 增加每市场 cadence 统计：

```rust
pub struct CadenceTracker {
    per_market: HashMap<String, MarketCadence>,
}

struct MarketCadence {
    fill_history: VecDeque<(Instant, Side, f64)>,
    current_run_side: Option<Side>,
    current_run_len: u32,
    half_life_samples: VecDeque<Duration>,
    last_imbalance_peak: Option<(Instant, f64)>,
    ask_price_history: VecDeque<(Instant, Side, f64)>,
    book_depth_history: VecDeque<(Instant, Side, f64)>,
}
```

算法定义固定：

- `run_length`：同侧 fill 连续数；`merge/redeem` 不计
- `half_life`：从 `|net_diff|` 局部 peak 到首次衰减到 `peak/2`
- `clip_size`：单笔 fill 数量
- `first_opposite_delay`：首笔后首次 opposite fill 延迟
- `first_fill_cover_delay`：首笔数量被 opposite 累计覆盖的延迟

所有 cadence 指标都基于 **working / tranche 事件** 计算，不基于 `settled_changed` 推导。

### 3.3 B1 — 我方 baseline 报告

会计层上线后，先跑 `>= 2` 周 baseline。

必须输出：

| 指标 | 我方 | xuan 中位 | xuan p90 | gap |
|------|------|----------|----------|-----|
| `same_side_run_length` | ? | 1 | 2 | ? |
| `imbalance_half_life` | ? | 24s | 111s | ? |
| `clip_size` 首笔 | ? | 170 | 397 | ? |
| `final_pair_cost` | ? | 待补 | 待补 | ? |
| `worst_intracycle_MTM` | ? | 待补 | 待补 | ? |

规则：

- `1` 周样本只用于 sanity check
- `>= 2` 周样本才可用于行为优先级排序与 `H-x` 裁决对照

### 3.4 D0 — `PairArbHarvester`

独立于 `run_capital_recycler` 的 harvest actor，先定位为：

- dry-run 工具
- turnover 观测工具
- 参数扫描工具

而**不是默认盈利主模块**。

`initial defaults`：

```text
PM_PAIR_ARB_HARVEST_MIN_FULL_SET=10
PM_PAIR_ARB_HARVEST_MIN_INTERVAL_SECS=20
```

仅用于 shadow / scan，不作为真值。

---

## 4. 研究层（与会计层并行）

### 4.1 L1 — 基础真相

任何行为层 enforce 前，至少解决下列三项中的两项：

| 编号 | 问题 | 数据需求 | 影响 |
|------|------|---------|------|
| `BS-1` | `trades.side` 是 maker 还是 taker | Authenticated CLOB `/trades`：`trader_side`、`maker_orders`、`taker_order_id` | 决定学习被 hit 的挂单几何还是 bounded taker 行为 |
| `BS-2` | `BUY-only` 是真实还是 data-api 视角 | Polygonscan CTF transfer + 长窗 trades/activity | 决定 SELL/FAK 是否仅保留为 emergency |
| `BS-3` | 双边预埋还是单边触发后补腿 | open-order timeline；fallback：同步 `book + trade tape` 反推 | 决定 entry 骨架是 `two-sided seed` 还是 `single-leg trigger` |

`BS-3` 继续拆分：

- `BS-3a`：round-open 形态
- `BS-3b`：mid-round 形态

### 4.2 L2 — 候选机制假设

#### `H-1` Final-clip discount capture

硬阈值：

- “末段大折扣贡献 `> 30%` 的 round 占比 `>= 50%`”

通过则：

- `Phase H` 可进入行为层 shadow，并在后续进入 enforce 候选

#### `H-2` Round-open two-sided seed

硬阈值：

- “首两笔时间差 `<= 1s` 的 round 占比 `>= 70%`”

通过则：

- `C1-twoside (round-open)` 可成为默认候选

#### `H-3` Depth-aware sizing

硬阈值：

- `clip_size ~ best_depth` 回归 `R² >= 0.4`

通过则：

- `depth-aware sizing` 可进入行为层 shadow

#### `H-4` Abandon-sell 极弱化

硬阈值：

- `worst MTM <= -$50` 且终局 `PnL > 0` 的 round 占比 `>= 60%`

通过则：

- `abandon-sell` 阈值可放宽到 `240s+` 且 MTM-aware

### 4.3 研究库三对象

研究产物统一进入 `poly_trans_research`：

1. `xuan_round_summary`
2. `xuan_tranche_ledger`
3. `xuan_round_path_features`

并固定两类报告：

- **长窗报告**：判断哪些现象稳定存在
- **单 round 深描**：解释路径机制，不单独定参

### 4.4 PnL 归因 bar

研究层必须满足以下退出条件：

- 至少 `70%` 的盈利 round，能把 `TOP-2` 利润来源归到：
  - `pair_discount`
  - `merge_turnover`
  - `final_clip_discount`
  - `residual_payout`

`rebate` 由于公开数据往往无法单独分离，统一记为 `unattributable`，不计入这 `70%` 分母。

---

## 5. 数据 fallback 矩阵

| 数据 | 首选 | fallback-1 | fallback-2 |
|------|------|-----------|-----------|
| Authenticated CLOB `/trades` | API key / 认证访问（已实测：我方账户可用） | `data-api` + 我方 maker/taker 反推 | `xuan side = unknown`，只做我方 truth |
| Open-order timeline | user-channel 历史回放 | 同步 `book + trade tape` 反推 | 接受 `BS-3` 只能部分回答 |
| 链上 split/merge/redeem | Polygonscan 全量 | RPC 定向查询 | 只用 trade tape 推断 |
| 长窗 `2-4` 周 trades | `data-api` 持续抓取 | 分批短窗拼接 | 接受 `1-2` 周窗口 |

原则：

- 任意 `H-x` 在首选数据缺失时，可以先走 `fallback` 做 shadow 判断
- 但进入 enforce 前，必须在 PR 描述里明确使用的是哪一档数据口径

### 5.1 鉴权数据可达性实测（2026-04-25）

已用本仓 `.env` 的真实 key 执行 [`src/bin/probe_clob_trades.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/bin/probe_clob_trades.rs)：

- 认证链路可用：`POLYMARKET_PRIVATE_KEY + POLYMARKET_FUNDER_ADDRESS` 可直接鉴权 `CLOB`.
- `/data/trades` 字段可用：`trader_side`、`maker_orders`、`taker_order_id`、`transaction_hash`.
- `after/before` 时间窗过滤可用（命中我方时间窗时返回非空，例：`count=5`）。
- 对外部地址（含 `xuan`）的 `maker/taker` 过滤在当前权限下不可用：  
  用 `xuan` 地址与 `0x000...` 地址查询，返回样本统计一致（均 `count=300`）。
- 用 `xuan` 公开 `transactionHash` 在当前鉴权流中未命中。
- `data-api` 对 `xuan` 拉取稳定可用：`trades?user=...&limit=1000` 实测返回 `1000` 条，时间覆盖到 `2026-04-25 06:09:48 UTC`。

结论（写入执行口径）：

1. 这批 key 足够支持 **我方 execution truth** 采集（可完整覆盖 `BS-1` 的“我方侧真相”）。  
2. 这批 key 目前 **不足以直接抽取 xuan 的 `trader_side` 真相**；`xuan` 侧仍走 fallback（`data-api + 反推`）。  
3. `BS-1` 对 `xuan` 的 fully-decided 条件，仍需新增权限来源（平台侧授权接口 / 对方授权数据 / 可验证替代源）。

---

## 6. 行为层发布矩阵

### 6.1 可立即推进

- `A0 TrancheLedger`
- `B0 CadenceTracker`
- `D0 Harvester` shadow / dry-run
- `C1-stub tranche_arb` 骨架

### 6.2 进入 enforce 的门槛

#### `C1-single tranche_arb`

前置：

- `A0 + B0`
- `>= 2` 周我方 baseline 报告
- `BS-1` 已解

#### `C1-twoside (round-open only)`

前置：

- `C1-single`
- `H-2` 通过

#### `Phase H final-clip`

前置：

- `H-1` 通过

#### `Depth-aware sizing`

前置：

- `H-3` 通过

#### `Abandon-sell` 放宽

前置：

- `H-4` 通过

### 6.3 永远不做

- 不恢复 `dynamic pair_target` 作为主模块
- 不把新逻辑再缝进旧 `pair_arb` 当最终归宿
- 不删除 `FAK/SELL` 代码
- 不根据单 round 证据直接改 enforce 参数

---

## 7. 时间盒

| Stage | 内容 | 上限工期 | fallback |
|------|------|---------|----------|
| `S0` | 跑通现有 `95` round 研究基线 | `1w` | 无，必须完成 |
| `S1` | `2-4` 周高粒度重建 | `2w` | 若数据受限，先用现有 `1000` 笔做最优 baseline |
| `S2-H1`..`S2-H4` | 每个假设独立裁决 | `<= 1w / 项` | 数据不足则标记 `无法裁决`，维持 shadow-only |
| `S3` | 我方 baseline 测量 | `>= 2w`，与 `S1` 并行 | 最短不可低于 `2w` |
| `A0/B0/D0` | 会计层实现与上线 | `2w`，与 `S0/S1` 并行 | — |

---

## 8. 安全层

固定规则：

- `FAK` 路径：保留代码，默认 `off`
- `SELL` 路径：保留代码，默认 `off` 于 `tranche_arb`，但 baseline 不动
- `Abandon-sell`：只有在 `A0 + H-4` 通过后才允许启用 tranche-aware 版本

硬规则：

- 任何 PR 如果修改 `FAK/SELL/abandon` 默认值，必须引用对应 `BS-x/H-x` 裁决证据

---

## 9. 验证纪律

### 9.1 Shadow 纪律

- 所有依赖 `BS-x/H-x` 的行为模块：
  - shadow 至少 `2w`
  - 才能申请 enforce

### 9.2 证据纪律

- 所有此类 PR：
  - 必须在描述中引用 `BS-x/H-x`
  - 必须说明采用的是真实数据、fallback-1 还是 fallback-2

### 9.3 回退纪律

- enforce 后 `1w` 对比 baseline
- 如指标或 PnL 明显恶化，自动回退 shadow

---

## 10. 最终决策

1. 研究北极星保留“尽最大可能还原 `xuan` 机制”
2. 工程上线目标继续采用“差距驱动、选择性吸收”
3. `v3.3` 是结构基线
4. `v3.4` 只提供操作纪律增强
5. `v3.5` 保留 `v3.3` 的硬阈值、PnL bar、fallback、时间盒
6. `v3.5` 吸收 `v3.4` 的四个增强：
   - baseline 前置
   - 永远不做清单
   - 2 周 shadow + PR 引证据
   - `D0` 降级为观测/工具

---

## 11. 关联文档

- [PLAN_PAIR_ARB_V3_3_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/plans/PLAN_PAIR_ARB_V3_3_ZH.md)
- [PLAN_PAIR_ARB_V3_2_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/plans/PLAN_PAIR_ARB_V3_2_ZH.md)
- [PLAN_PAIR_ARB_V3_1_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/plans/PLAN_PAIR_ARB_V3_1_ZH.md)
- [PLAN_Codex.md](/Users/hot/web3Scientist/pm_as_ofi/docs/plans/PLAN_Codex.md)
- [XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/research/xuan/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md)
