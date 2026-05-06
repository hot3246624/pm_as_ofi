# Polymarket 双边 BUY 玩家 Archetype 谱系（V2，2026-04-27 扩展）

生成时间：`2026-04-27`（首版） / `2026-04-27 V2`（扩到 6+ archetype）
研究目标：在 Polymarket 上识别「双边 BUY」（同时买 Up + Down）玩家的不同 archetype，提炼 PGT 蓝图的设计参考。

数据基础（**6 个深度研究 + 5 个 quick survey + 1 个排行榜横切**）：
- `XUANXUAN008_STRATEGY_V2_ZH.md` — xuan，单 venue 紧密 maker
- `LATE_GRAMMAR_0x7347d3_STRATEGY_DEEP_DIVE_ZH.md` — late_grammar，多 venue 不 MERGE maker
- `QUIETB27_0xb27bc_STRATEGY_DEEP_DIVE_ZH.md` — quietb27，单 venue selective directional taker
- `GABAGOOL22_GROWN_CANTALOUPE_STRATEGY_DEEP_DIVE_ZH.md` — gabagool22，多 venue tenor-adaptive maker (项目命名源)
- **`SILENT_D189_0xd189664c_STRATEGY_DEEP_DIVE_ZH.md`** — silent_d189，BTC 5m ITM sniper（**新**）
- **`CANDID_CLOSURE_0x93c22116_STRATEGY_DEEP_DIVE_ZH.md`** — candid_closure，long-tenor multi-venue maker（**新**）
- **`MAYBE_5_QUICK_SURVEY_ZH.md`** — 5 个 MAYBE 候选 quick survey（**新**，归入已知 archetype variant）
- `POLYMARKET_TOP_TRADER_LEADERBOARD_PROBE_ZH.md` — 体育/政治 directional 头部映射

证据等级：`A`/`B`/`C` 同前。

---

## 0. 谱系图（三轴分类，V2 增加 maker 注册维度）

```
                              MERGE 频率
                   高 (in-round)│
                                │
        Archetype C (xuan) ────●         ★ V1.1 PGT 蓝图位置
        BTC 5m, 91% clean       │
        未注册 maker             │ Archetype E (gabagool22)
                                ●  multi-venue 5m+15m+daily
                                │  ✅ 注册 maker
                                │
                                │ Archetype E' (candid_closure)
                                ●  multi-venue 15m+4h+daily（无 5m）
                                │  ✅ 注册 maker
                                │
                  ┌─────────────┼─────────────┐
                  │             │             │
        单 venue ←┤             │             ├→ 多 venue
                  │             │             │
        ●(quietb27)             │             ●(late_grammar)
        Archetype D             │             Archetype C'
        selective taker          │             multi-venue 不 MERGE
        未注册                   │             未注册
                                │
        ●(silent_d189)           │
        Archetype F              │
        BTC 5m ITM sniper        │
        ✅ 注册 maker (4 rebates) │
                                │
                  低 (post-close 或 0)
```

### 0.1 6+ archetype 完整列表

| Code | 代表 | universe / tenor | 注册 maker | 复刻推荐 |
|------|------|------|-----------|----------|
| **A** | leaderboard rank 1-6 | sports markets | n/a | ❌ 不同 segment |
| **B** | rank03 tripping | politics + active SELL | n/a | ❌ 不同 segment |
| **C** | xuan | BTC 5m | ❌ | ✅ V1.1 主干 |
| **C'** | late_grammar | 6 venue 5m+15m+daily | ❌ | ❌ margin-thin |
| **D** | quietb27 | BTC 5m | ❌ | ❌ taker directional |
| **E** | gabagool22 | 6 venue 5m+15m+daily | **✅** | ⚠️ 部分（V1.2+） |
| **E'** | candid_closure | 12 venue 15m+4h+daily | **✅** | ⚠️ 部分（V1.2+） |
| **F** | silent_d189 | BTC 5m + nba | **✅ (4 rebates)** | ❌ ITM sniper |

5 个 MAYBE 候选（c01/c02/c05/c08/c10）全部归入已知 archetype 的 variant，未发现新 archetype。**6 archetype 谱系已封盘**。

### 0.2 含义

- 我们 PGT V1.1 蓝图定位在 archetype C（xuan），与 6 archetype 中**唯一未注册 maker 紧密 5m 配对**位置一致
- 项目命名源 gabagool22 实际在 archetype E（注册 maker multi-venue），与 V1.1 在「注册 maker」+「multi-venue」两轴上不同
- 注册 maker 都跳过或绕开「5m 紧密 pair-gated」执行——意味着**注册 maker tier 与 archetype C 互斥**

---

## 1. 4 archetype 全维度对比表

| 维度 | **C: xuan** | **C': late_grammar** | **D: quietb27** | **E: gabagool22** |
|------|------|------|------|------|
| **基础参数** | | | | |
| Universe 数 | 1 | 6 | 1 | 6 |
| 主 venue | BTC 5m | btc/eth × 5m/15m + daily | BTC 5m | btc/eth × 5m/15m + daily |
| 30d profit rank | 未进 top 20 | 未进 top 20 | **rank 20** ($530k) | 未进 top 20 |
| 数据窗口 | 37 h | 14.6 h | 361 h (15 天) | 9 h (单日 burst) |
| Trades total | 3500 | 3500 | 3500 | 3500 |
| 平均频率 (trades/h) | 95 | 240 | 9.7 | **389** |
| **持仓结构** | | | | |
| 双侧同持率 | 0.2% | **88.8%** | 0% (snapshot) | 0% (snapshot) |
| 单侧残差侧偏好 | **100% loser** | 100% (混合 winner+loser) | 0 visible | 0 visible |
| Winner-side residual ratio | 0/538 | **73 winner / 14 loser** | n/a | n/a |
| **Clip 行为** | | | | |
| Clip p50 | 131.7 | 10.4 | 59.6 | **9.9** |
| Clip max | 600 | 633 | **9392** | **10.0**（封顶）|
| 适配性 | imb + intra-round 双重 | 部分 venue | 无 | **完全固定** |
| **MERGE 节律** | | | | |
| MERGE 数 (pro-rated 14.6h) | 196 | 0 | 4.6 | **102** |
| MERGE 时机 | **in-round (p50=278s, 距 close 22s)** | n/a | early in-round (p50=172s) | **post-close** (p50=1826s) |
| Tenor adaptive | 否（仅 5m） | n/a | 否（仅 5m） | **是（5m/15m post-close, daily mid）** |
| **REDEEM 节律** | | | | |
| REDEEM 数 (pro-rated 14.6h) | 125 | 451 | 0.3 | **115** |
| REDEEM 中位 offset (post-close) | +38s | +2.18 h | +216s | +25 min |
| **Episode 行为 (eps=10)** | | | | |
| clean_closed_ratio | **91.3%** | 33-100% (venue-dep) | 2.17% | 38.8% |
| same_side_add_ratio | 10.5% | 4-39% | 44.5% | 29.5% |
| Pure directional rounds % | ~0% | 未测 | **68.9%** | 13.0% |
| Run length max | 4 | 8 | **31** | 12 |
| **Inter-fill** | | | | |
| Cross p50 | 18s | 36s | 12s | 18s |
| Same p50 | 10s | 2s | 0s | 10s |
| Same-second cross % | 5.49% | 2.49% | 1.37% | 1.14% |
| **Maker/Taker** | Maker (B) | Maker (B) | **Taker (B)** | **Maker (A)** ⭐ |
| MAKER_REBATE event | 0 | 0 | 0 | **1** ⭐ |
| **PnL 字段** | | | | |
| realizedPnl 累计 | -$23,408 | $0 | n/a | $0 |
| cashPnl (open MTM) | -$3,142 | +$349 | n/a | $0 |
| 30d profit (Polymarket 字段) | small | small | **+$530k** | small |
| **MERGE 前 bias（仅 xuan 实测）** | | | | |
| (winner_qty − loser_qty) MERGE-前 | **median −8.4 shares (95% loser-heavier)** | n/a | n/a | n/a |

⭐ = 最强证据（A 级）

---

## 2. 关键发现总结

### 2.1 「双边 BUY」不是单一策略类型

xuan / late_grammar / quietb27 / gabagool22 都同时买 Up + Down，但**底层执行几何完全不同**——4 个 archetype，3 种不同 alpha 来源：
- xuan (C): 紧密 pair-gated MERGE 收水
- late_grammar (C'): 多 venue spread + winner-bias 微利
- quietb27 (D): round selection + directional bet (taker)
- gabagool22 (E): tenor-adaptive maker rebate

### 2.2 MAKER_REBATE event 是 maker 身份的唯一直接证据

跨 4 wallet 检查：

| Wallet | MAKER_REBATE 次数 | maker 等级 |
|--------|---------:|------|
| xuan | 0 | B（间接强证据，未注册）|
| late_grammar | 0 | B（间接证据）|
| quietb27 | 0 | B（间接 taker 证据）|
| gabagool22 | **1** | **A（直接确认 registered maker）**|

含义：仅 gabagool22 是 Polymarket 官方注册 maker。其余可能跑 maker geometry 但不在 rebate 计划内。

### 2.3 同 universe 不等于同 archetype

xuan + quietb27 + gabagool22 都做 BTC 5m，但分别在 archetype C / D / E。**universe 选择 ≠ 策略选择**。

### 2.4 xuan 的 MERGE 前 LOSER-bias（反预期发现）

V2 §0.5 修正 5b：MERGE 前 95.5% loser-heavier（median delta=−8.4 shares）。

**机理**：被动 maker 的 BUY Limit 在「下跌侧」（即未来 loser side）更容易被打中——市场 sell pressure 把价格压到 maker BUY price 时填单。winner side 价格上涨远离 maker BUY，反而少填。

**这与 late_grammar 在 BTC 5m 显示的 winner-bias 27:1 看似矛盾，但其实是**不同时间点**的不同观测**：
- xuan: MERGE 时刻 (in-round 278s) → loser-heavier
- late_grammar: settlement 时刻 (post-close, 没 MERGE) → winner-heavier

两者并不矛盾：late_grammar 在 settlement 之后才被观测，可能 winner side BUY 在 round 后段才大量被打。如果对 late_grammar 也做同样的 MERGE-time-equivalent 时点分析，可能也会显示 loser-bias 然后翻盘。

---

## 3. 对 PGT 蓝图（V1.1）的修订建议

### 3.1 V1.1 主干维持（基于 xuan 路径）

**继续走 archetype C（xuan）路线**：
- 单 venue（BTC 5m）紧密 pair-gated tranche
- in-round MERGE pulse 在 round close 前 22s
- 自适应 clip（imb + intra-round idx）
- 状态机：FlatOrResidual → ActiveFirstLeg → CompletionOnly → PairCovered → MergeQueued → Closed

理由：
1. xuan 在我们项目当前架构上**最直接可执行**（单 venue + 紧密 in-round）
2. xuan 的 91.3% clean_closed_ratio 是 4 archetype 中最干净的
3. xuan 实证 (winner_qty − loser_qty) MERGE 前 loser-bias，「无方向 alpha」假设成立

### 3.2 V1.1.1 增量（吸收 late_grammar / gabagool22 / quietb27 部分）

| 来源 | 增量项 | 优先级 |
|------|--------|------|
| gabagool22 | **Tenor-adaptive MERGE timer**（5m/15m post-close + daily mid-round） | P1 |
| gabagool22 | **极简 fixed-clip 选项**（10 股固定，shadow A/B 测试 vs adaptive） | P2 |
| gabagool22 | **MAKER_REBATE 监控**（PGT shadow 启动后跟踪自己是否拿到 rebate） | P0 |
| late_grammar | **Per-universe clip 表**（不同 venue 不同 BASE_CLIP） | P2 |
| late_grammar | **15m+daily 兼容性**（V1.1 当前仅 5m） | P2 |
| late_grammar | **Winner-bias 监控**（PGT shadow 中如果意外出现，说明执行偏离 pair-gated） | P1 |
| quietb27 | **不复刻**（taker directional 与 maker 蓝图正交）| - |
| 体育/政治榜 | **不复刻**（不同 segment）| - |

### 3.3 复刻判断汇总

| Archetype | 是否复刻 | 理由 |
|-----------|---------|------|
| C: xuan | ✅ 是 | V1.1 主干已基于此 |
| C': late_grammar | ❌ 否 | margin-thin + 依赖结构性偏差 + 不 MERGE 资金效率差 |
| D: quietb27 | ❌ 否 | round selection signal 缺失 + taker 与 maker 蓝图正交 |
| E: gabagool22 | ⚠️ 部分（V1.1.1 增量） | tenor-adaptive 设计有借鉴价值，但 multi-venue 部署需架构升级 |
| 体育/政治 (rank 1-6) | ❌ 否 | 完全不同 segment，与 PGT 无关 |

---

## 4. 项目命名与策略路线的关系

### 4.1 命名源 vs 实际蓝图

项目代码自我描述 `Gabagool22 + A-S + OFI Kill Switch + Oracle-Lag Sniping`：

- **Gabagool22**：archetype E（多 venue tenor-adaptive maker）—— 命名灵感来源
- **A-S (Avellaneda-Stoikov)**：经典 maker 定价模型
- **OFI Kill Switch**：order flow imbalance 风控信号
- **Oracle-Lag Sniping**：post-close 信息泄漏抢挂（与 xuan 的 in-round 行为不同）

**实际 V1.1 蓝图借鉴**：
- 主干：xuan（archetype C）
- 不是 gabagool22（archetype E）

### 4.2 这不是 bug 而是有意选择

V1.1 选择 xuan 路线的理由：
1. **架构兼容**：项目当前 polymarket_v2 单 venue 设计，xuan 单 venue 直接对接
2. **资金效率**：xuan 的 in-round MERGE 是 archetype 中最高资金周转率
3. **执行清晰**：xuan 91.3% clean_closed 给出最明确的"成功执行"signal
4. **数据丰富**：xuan 行为模式可观测度最高（4 archetype 中最深研究）

### 4.3 未来扩展路径

如果 V1.1 主干 shadow 验证通过，下一步可考虑：

- **V1.2**: 加 gabagool22 启发的 tenor-adaptive MERGE（5m → 15m 扩展）
- **V2.0**: 加 late_grammar 启发的 multi-venue 平铺（BTC + ETH 双 venue）
- **V3.0**: 探索 quietb27 启发的 round selection signal（需独立信号源研发）

---

## 5. 证据等级表

| 维度 | 等级 |
|------|------|
| 「双边 BUY」是 4 个 archetype 而非单一类型 | A |
| Archetype C / C' / D / E 各自定性 | A（每个有独立 deep dive 文档支撑） |
| MAKER_REBATE event 是 maker 直接证据 | A |
| 仅 gabagool22 是注册 maker | A |
| xuan MERGE 前 loser-bias | B（44/497 样本，selection bias 风险） |
| 谱系双轴（universe × MERGE 频率）合理 | B（基于 4 样本，可能有未发现 archetype） |
| V1.1 维持 xuan 路线是最优 | B（基于现有数据，未实装验证） |

---

## 6. 下一步

按价值/工时比排序：

1. **V1.1.1 增量备注**（写入 PGT 蓝图）—— 1 h
   - tenor-adaptive MERGE timer
   - MAKER_REBATE 监控
   - Winner-bias 监控
   - 不动主干

2. **补 cleaned slug 的 winner side**（V2 §11 第 7 条）—— 1-2 h
   - 通过 gamma-api 拉 outcome 字段补全 453 cleaned MERGEs
   - 重跑 pre-merge bias 看完整分布
   - 如仍 95% loser-heavier → §0.5 修正 5b 终结；如混合 → 维持当前 caveat

3. **late_grammar 同样做 MERGE-equivalent 时点分析**（用 round close 前 22s 的瞬时仓位） —— 1-2 h
   - 验证 late_grammar 的 winner-bias 是否在 "MERGE 时点等价" 也是 loser-bias，再 settlement 后翻盘
   - 解开 §2.4 的「mechanism contradiction」

4. **Recorder book-snapshot timing match**（V2 §8.2 alt 路径 #1）—— 2-3 h
   - 用我方 recorder 04-25/04-26 的 md_book_l1 + xuan trades.timestamp 比对
   - 推断 xuan 在 trade 瞬间是 maker（被打）还是 taker（吃 ask）
   - 把 maker/taker 等级从 B 升到 A

5. **gabagool22 长窗口验证**（拉之前的 9h 是否有更早数据）—— 1 h
   - data-api before-cursor 拉 ts < 1771545878 的数据
   - 验证 gabagool22 是周期性 burst 还是单次实验

---

## 附录 A：所有相关数据 / 文档清单

```
docs/
├── XUANXUAN008_STRATEGY_DEEP_DIVE_ZH.md           # V1 (2026-04-25, 历史基线)
├── XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md      # V1 (2026-04-25, 历史基线)
├── XUANXUAN008_STRATEGY_V2_ZH.md                  # V2 + §0.5 + §0.5 修正 5b（主参考）
├── LATE_GRAMMAR_0x7347d3_STRATEGY_DEEP_DIVE_ZH.md # archetype C' (2026-04-26)
├── QUIETB27_0xb27bc_STRATEGY_DEEP_DIVE_ZH.md      # archetype D (2026-04-26)
├── GABAGOOL22_GROWN_CANTALOUPE_STRATEGY_DEEP_DIVE_ZH.md  # archetype E (2026-04-26)
├── POLYMARKET_TOP_TRADER_LEADERBOARD_PROBE_ZH.md  # leaderboard archetype 横切
└── POLYMARKET_BILATERAL_BUY_ARCHETYPES_ZH.md      # 本文（综合对比）

data/
├── xuan/                      # xuan 全套
│   ├── pre_merge_bias.json/.txt   # NEW: §11 第 5 条结果
│   ├── positions_*.json/.txt
│   ├── trades_long.json
│   ├── activity_long.json
│   └── long_window_analysis.json/.txt
├── late_grammar/              # late_grammar 全套
├── quietb27/                  # quietb27 全套
├── grown_cantaloupe/          # gabagool22 全套
├── poly_leaderboard_30d_profit.json   # top 20 排行榜
└── leaderboard_quickprobe.json        # 9 wallet quick probe

scripts/
├── pull_xuan_long_window.py + analyze_xuan_*.py
├── pull_late_grammar_long_window.py + analyze_late_grammar_*.py
├── pull_quietb27_long_window.py + analyze_quietb27_*.py
├── pull_grown_cantaloupe_long_window.py + analyze_grown_cantaloupe_*.py
└── analyze_xuan_pre_merge_bias.py     # NEW: §11 第 5 条
```
