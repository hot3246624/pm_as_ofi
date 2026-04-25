# Pair_Arb 升级计划 v3.2 — 基于单 round 高粒度交易细节的增量修订

> **生成日期**：2026-04-25
> **基线**：`docs/PLAN_PAIR_ARB_V3_1_ZH.md`（三层分离架构）
> **新证据**：`/Users/hot/web3Scientist/poly_trans_research/analysis_table.html` + `trades.json`
>   - 单 round 14 笔 trades 全细节（`btc-updown-5m-1777059600`, 2026-04-24 15:40:36–15:43:28 ET）
>   - 含 size / price / cum 持仓 / pair cost / locked PnL 时序

> ⚠️ **样本警告**：以下全部基于**1 个 round** 的 14 笔。任何"策略"层级结论需后续多 round 验证。本文档每条新发现都明确标注是"单 round signal"还是"已与 1000 笔报告一致"。

---

## 0. 这一 round 的关键事实

### 0.1 时序与价格

| # | 时间 | 侧 | size | 价 | 累 pair cost | net diff | locked PnL |
|---|------|----|------|----|------------|---------|----------|
| 1 | 15:40:36 | NO | 190.66 | 0.6013 | 0.6013 | -190.66 | -114.64 |
| 2 | 15:40:36 | YES | 187.78 | 0.3975 | 0.9988 | -2.88 | -1.51 |
| 3 | 15:40:42 | NO | 154.25 | 0.6280 | 1.0107 | -157.13 | -98.38 |
| 4 | 15:40:46 | YES | 151.34 | 0.3729 | 0.9998 | -5.79 | -3.47 |
| 5 | 15:40:48 | YES | 152.79 | 0.5000 | 1.0350 | +147.00 | -74.08 |
| 6 | 15:41:04 | NO | 152.70 | 0.4916 | 0.9977 | -5.69 | -2.14 |
| 7 | 15:41:08 | YES | 80.43 | 0.5700 | 1.0185 | +74.74 | -42.29 |
| 8 | 15:41:10 | NO | 79.56 | 0.4239 | 0.9976 | -4.82 | -1.28 |
| 9 | 15:41:24 | NO | 43.83 | 0.5500 | 0.9972 | -48.65 | -25.38 |
| 10| 15:41:34 | YES | 43.31 | 0.3900 | 0.9935 | -5.34 | +1.04 |
| 11| 15:41:40 | YES | 43.76 | 0.5293 | 0.9995 | +38.42 | -16.79 |
| 12| 15:42:08 | NO | 43.14 | 0.3371 | 0.9854 | -4.72 | +7.09 |
| 13| 15:42:54 | YES | 190.09 | 0.5606 | 1.0113 | +185.37 | -94.76 |
| 14| 15:43:28 | NO | 185.67 | 0.2481 | **0.9474** | -0.30 | **+44.56** |

最终 round PnL：**+$44.56 / $804.96 spent ≈ 5.5% per round**。

### 0.2 六大新发现

#### F1. 累计 pair cost 从 0.99 → 0.95，但终局对单笔 pair 是 0.81
- 累计加权 pair cost 走势：0.999 → 1.011 → 1.000 → 1.035 → 1.018 → 0.998 → 0.987 → **0.947**
- **最后两笔 (T13+T14) 单 pair sum = 0.5606 + 0.2481 = 0.8087** — 这是巨幅 19c 折扣
- **结论**：他不是"维持 pair cost ≈ 0.99"。他**容忍中段累计 cost > 1**，等待**最后一笔超低价 NO** 把 round 拉回正收益
- 已与报告一致：报告 §4.2 的 `final_gap median 2.21%` 与本 round 净 diff -0.30 一致；但报告**未捕捉** "最后一笔大幅折扣" 这个**关键 PnL 来源**

#### F2. 中段 MTM 反复跌至 -$74 / -$94 / -$98，但坚决不平
- 已 mark 7 次负 MTM ≥ -$25：T1, T3, T5, T7, T9, T11, T13
- 最低点 T3：-$98.38
- 倒数第二笔 T13：-$94.76
- 然后 T14 一笔从 -$94 翻到 +$44.56
- **结论**：abandon-sell 在他这套里**几乎不存在**。risk tolerance 远高于 v3.1 的 `abandon_after_ms=180s + paired_qty=0` 设计
- v3.1 abandon-sell 阈值需要彻底重新评估

#### F3. T1 + T2 同秒（timestamp 都是 1777059636），强烈提示**双边预埋 maker**
- T1 NO 190.66 @ 0.6013
- T2 YES 187.78 @ 0.3975
- 同 second，size 接近（190 vs 188）
- **最简单解释**：他事先在两侧各挂一张大 maker，开盘瞬间被双向 takers 同时打掉
- 这是 **BS-3 的部分（弱）证据**：至少对 round-open 这一时刻，是 **two-sided seed**，不是 single-leg trigger
- caveat：1s timestamp 粒度 → 还是可能 ~500ms 内顺序触发；需 G-3 (open order timeline) 最终裁决

#### F4. Clip size 形态：LARGE → MEDIUM → SMALL → SMALL → LARGE（不是 uniform small）
- T1-2: ~190 (LARGE 双开)
- T3-4: ~152 (MEDIUM)
- T5-8: 80-152 (MIXED)
- T9-12: ~43 (SMALL × 4)
- T13-14: ~190 (LARGE 收尾)
- **结论**：v3.1 的 `MAX_CLIP_SIZE=200` 看似对，但 clip size 是**有结构的**，不是简单 cap
- **新假设**：他可能跟随**盘口 best level depth** 决定 clip size（depth 大就大 clip，depth 小就小 clip）
- 这与报告 §10.3 "clip size 不固定，原因未知" 一致

#### F5. 同侧 run length ≤ 2，且 run 内**价格朝同方向移动**
- Run YY (T4 @ 0.37 → T5 @ 0.50)：YES 涨了 13c
- Run NN (T8 @ 0.42 → T9 @ 0.55)：NO 涨了 13c
- Run YY (T10 @ 0.39 → T11 @ 0.53)：YES 涨了 14c
- **解读**：run 期间该 side 持续被 hit，价格走向不利于 maker → 强烈提示是 **passive maker 被 adverse selection**，不是 aggressive taker 主动加仓
- 这进一步支持 BS-1（他主要是 maker）

#### F6. 时间间隔分布：bursty 开局 + 长尾收尾
- 14 笔间隔（秒）：0, 6, 4, 2, 16, 4, 2, 14, 10, 6, 28, **46**, 34
- 前 8 笔间隔中位 ~4s（密集）
- 后段间隔 28s / **46s** / 34s（稀疏）
- 最长 gap 出现在 T11 → T12（46s）；T12 → T13（46s）
- **结论**：他**等价格漂移**，不是机械按时间挂单。当中段 pair cost 已 ~ 0.99 时，他**故意停下**等更好机会；T14 NO @ 0.2481 印证了这种"等"的回报
- v3.1 的 `harvest_min_interval_secs=20` 可能错位——他不是按时间 harvest，而是按**价格机会触发 large clip + 或 harvest**

---

## 1. 对 v3.1 的增量修订（v3.2 patch）

### Patch 1：恢复 Phase F，但形态完全不同 — 重命名为 "Final-clip Discount Capture"

**v3 / v3.1 现状**：Phase F (dynamic pair_target) **删除**。

**v3.2 修订**：**恢复**，但定义完全不同：
- 不是"在每笔报价时调 target"
- 而是"识别**收尾大 clip 时机**"——当一侧出现极便宜 ask（低于历史 X 分位）时，触发 **large size BUY** 把累计 pair cost 拉低
- 触发条件：
  - 当前 round 已有 paired qty ≥ N
  - 当前 ask 价 ≤ recent N 分钟 ask 历史的 P10 分位
  - 触发 BUY size = `min(remaining_book_depth, capital_budget)`

**新 Phase 名**：**Phase H — Opportunistic Final-clip**

**Config**：
```
PM_TRANCHE_FINAL_CLIP_ENABLED=0
PM_TRANCHE_FINAL_CLIP_PRICE_PERCENTILE=10   # 取 history p10 以下才触发
PM_TRANCHE_FINAL_CLIP_LOOKBACK_SEC=120
PM_TRANCHE_FINAL_CLIP_MIN_PAIRED_QTY=200    # 须已有底仓
```

**依赖**：A0 (TrancheLedger) + B0 (CadenceTracker，需扩展记录 ask 价历史)

### Patch 2：abandon-sell 阈值彻底放宽，且必须 MTM-aware

**v3.1**：`abandon_after_ms=180000` (3 min)，paired_qty=0 即触发。

**v3.2 修订**：
- xuan 在 round 中段 MTM -$94 后 30s 即翻正
- 我们若按 v3.1 触发，会 **错过 round 收尾的 +5.5% 收益**
- 修订：abandon 仅在**两个条件同时满足**触发
  1. `tranche_age > 240s`（4 min，远 > xuan round 长度 3-5 min）
  2. **且** 对侧 ask 已超出 round-open 时刻的 200% 范围（即对侧深度真的塌了）
  3. **且** 距 round resolve < 30s（紧急退出窗）

**Config**：
```
PM_TRANCHE_ABANDON_AFTER_MS=240000
PM_TRANCHE_ABANDON_OPP_ASK_BLOWOUT_RATIO=2.0
PM_TRANCHE_ABANDON_RESOLVE_PROXIMITY_MS=30000
```

### Patch 3：clip size 决策从 "max cap" 改为 "depth-aware sizing"

**v3.1**：`MAX_CLIP_SIZE=200` 简单 cap。

**v3.2 修订**：
- F4 显示 size 与盘口 depth 相关
- 引入 `clip_size = clamp(α * best_level_depth, min_clip, max_clip)`
- α 初值 0.5（initial default，需 scan）
- min/max 仍保留 cap

**Config**：
```
PM_TRANCHE_CLIP_DEPTH_RATIO=0.5
PM_TRANCHE_CLIP_MIN_SIZE=20
PM_TRANCHE_CLIP_MAX_SIZE=300
```

**依赖**：需要 OrderBook depth snapshot 接入策略层（已存在 `UnifiedBook`，验证字段可用即可）

### Patch 4：BS-3 升级为 "partially answered (round-open)"

**v3.1**：BS-3（双边预埋 vs 单边触发）完全未答。

**v3.2 修订**：
- F3 (T1+T2 同秒) 是**弱**证据指向 round-open 时刻 = two-sided seed
- 但中段是否 two-sided 仍未答（中段 fills 时间间隔 ≥ 2s）
- 拆分 BS-3：
  - **BS-3a**：round-open 时刻 → **弱证据 = two-sided seed** ✅（F3）
  - **BS-3b**：round 中段（已有库存后）→ **仍未答**

**对发布矩阵的影响**：
- C1-twoside (round-open 形态) 的 evidence gating 从 "G-3 完成" 降级为 "G-1 + G-3 部分"
- 仍需 G-3 完整数据校验，但**可以先实现 round-open 双边 seed 的 stub 代码**

### Patch 5：B0 CadenceTracker 新增三个字段

为支持 Patch 1-3，CadenceTracker 必须记录：
1. **每个 market 的 best ask 价历史**（最近 N 分钟）→ 支持 Patch 1 P10 分位计算
2. **每个 market 的 best level depth 历史** → 支持 Patch 3 depth-aware sizing
3. **每个 tranche 的 MTM 时序** → 支持 Patch 2 MTM-aware abandon

```rust
struct MarketCadence {
    fill_history: VecDeque<(Instant, Side, f64)>,
    ask_price_history: VecDeque<(Instant, Side, f64)>,    // 新增
    book_depth_history: VecDeque<(Instant, Side, f64)>,   // 新增
    // ... existing fields
}

struct Tranche {
    // ... existing fields
    mtm_history: VecDeque<(Instant, f64)>, // 新增
}
```

### Patch 6：发布矩阵更新

| 项 | v3.1 状态 | v3.2 修订 |
|----|----------|----------|
| **A0 TrancheLedger** | 立即 | 不变 |
| **B0 CadenceTracker** | 立即 | **扩展字段**（ask 价 / depth / MTM） |
| **D0 Harvester** | A0 后立即 shadow | 不变 |
| **C1-stub tranche_arb** | A0+B0 | 不变 |
| **C1-single tranche_arb** | A0+B0+G-1 | 不变 |
| **C1-twoside (round-open only)** | C1-single + G-3 | **降级**：C1-single + G-1 + F3 evidence (即可 shadow) |
| **C1-twoside (mid-round)** | 同上 | 仍需完整 G-3 |
| **H Final-clip Discount Capture** | n/a | **新 Phase**；A0 + B0 扩展后即可 shadow |
| **E1 Abandon-sell** | A0 + C1-single enforce | 阈值大幅放宽（Patch 2）|

---

## 2. 单 round 局限与 cross-validation 计划

### 2.1 不能从这一 round 推出的事

- 5.5% per round 是否典型（需 > 50 round 样本）
- "0.81 final pair cost" 是否常态（这一 round 是 outlier？需分布）
- F3 同秒成交是否每个 round 都有（需每 round 检查 round-open 双笔时间差）
- F1 中"最后一笔大折扣"是否每个 round 都出现（可能这只是这一 round 的尾部 lucky tape）

### 2.2 Cross-validation 任务（在 v3.2 落地前完成）

补一份**多 round 高粒度分析**，按以下维度展开：
1. **每 round 的 final pair cost 分布**（验证 F1 的"19c 折扣"是 outlier 还是常态）
2. **每 round 第 1-2 笔的时间差分布**（验证 F3 的 two-sided seed 推断）
3. **每 round 的最低中段 MTM 与最终 PnL 的关系**（验证 F2 的高风险容忍是否系统性）
4. **每 round 的 clip size 序列形态**（验证 F4 的 LARGE→SMALL→LARGE 模式）

**Owner**：用户脚本扩展 `poly_trans_research`，跑全部 1000 笔 trades 的 round-by-round 重建。

---

## 3. v3.2 vs v3.1 改动速查

| 维度 | v3.1 | v3.2 |
|------|----|----|
| Phase F (dynamic target) | 删除 | **恢复为 Phase H (final-clip discount capture)** |
| Abandon-sell 触发 | `180s + paired==0` | `240s + opp_blowout + resolve_proximity` 三条件 |
| Clip size | 简单 cap 200 | depth-aware: `α * best_depth` |
| BS-3 | 完全未答 | **拆分**：3a (round-open) 弱答；3b (mid-round) 未答 |
| CadenceTracker 字段 | run_length / half_life / clip_size | + ask 价历史 + depth 历史 + MTM 历史 |
| C1-twoside gating | 必须 G-3 全数据 | round-open 形态降级为 G-1 + F3 弱证据 |
| 新 Phase | — | **Phase H**: Final-clip Discount Capture |

---

## 4. 关键文件（v3.2 增量）

- `src/polymarket/coordinator_metrics.rs` — CadenceTracker 三个新字段
- `src/polymarket/strategy/tranche_arb.rs` — 新 `evaluate_final_clip_opportunity()` 入口（Phase H）
- `src/polymarket/strategy/tranche_arb.rs` — `evaluate_clip_decision` 内 `clip_size` 改为 depth-aware
- `src/polymarket/coordinator_order_io.rs` — abandon-sell 触发条件改为多条件复合判定

---

## 5. v3.2 已确认设计决策

1. **F1（最终大折扣）+ F2（高 MTM 容忍）+ H（Phase H 收尾大 clip）** 三者同源——他不是"持续平滑做市"，而是"忍受中段亏损 + 等待对侧极便宜时收割"
2. **F3 同秒证据**：弱但方向明确，足以让 C1-twoside (round-open) **早 shadow**
3. **F4 depth-aware sizing**：把 v3.1 的 cap 升级为 ratio
4. abandon-sell 必须 MTM-aware；不能简单按时间触发（否则会被 F2 "中段 -$94"杀掉收益）
5. **多 round cross-validation 是 v3.2 enforce 的硬前置**——不能基于单 round 写死参数

---

## 6. 开放问题（v3.2 新增）

1. **F1 的"最后一笔大折扣"是否每 round 都出现**？若否，Phase H 的触发条件需更严
2. **F3 同秒推断是否能扩展到全部 round**？若否，C1-twoside 形态可能仅在 round-open 适用
3. **clip size 真的与 depth 相关，还是与 inventory state 相关**？需多 round 回归
4. **Phase H 与 D0 (rolling harvest) 的优先级**：当两者同时触发时（已有大 paired qty + 出现极便宜 opposite ask），先 harvest 还是先收尾大 clip？倾向先收尾（拉低 pair cost 优先于 turnover）
5. **MTM-aware abandon 的 MTM 计算口径**：用 best bid (最坏退出价) 还是 mid (公允估值)？倾向 best bid（保守）

---

## 7. 关联文档

- `docs/PLAN_PAIR_ARB_V3_1_ZH.md` — v3.1 三层分离架构（基线）
- `docs/PLAN_PAIR_ARB_V3_ZH.md` — v3 (superseded)
- `docs/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md` — 1000 笔统计研究
- `/Users/hot/web3Scientist/poly_trans_research/analysis_table.html` — v3.2 证据 round
- `/Users/hot/web3Scientist/poly_trans_research/trades.json` — v3.2 证据原始数据
- v3.2 enforce 前 待补：多 round cross-validation 报告（owner: 用户）
