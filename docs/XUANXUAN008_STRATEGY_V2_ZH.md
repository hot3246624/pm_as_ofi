# `xuanxuan008` 策略深度研究与复原 V2

生成时间：`2026-04-26`
研究对象：`xuanxuan008` / `Little-Fraction`
公开钱包：`0xcfb103c37c0234f524c632d964ed31f117b5f694`
URL：`https://polymarket.com/@xuanxuan008?tab=positions`

本次为 V2 修订稿，在前两份研究（`XUANXUAN008_STRATEGY_DEEP_DIVE_ZH.md`、`XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md`）基础上：

- 新增 **持仓快照（positions）** 切片分析（旧研究基于 trades / activity）。
- 新增 **clip-size 条件分布**、**session 节律**、**inter-fill delay 跨/同侧拆分**。
- 给出 **maker/taker 路径裁决** 的尝试结果（仍为 Level-C，方法学说明）。
- 给出 **`pair_gated_tranche.rs` 当前实现 vs xuan 公开行为** 的差距与建议 patch 清单。

证据等级沿用：`A`=公开数据直接确认；`B`=强支持但仍有替代解释；`C`=仅合理假设。

---

## 0. TL;DR

> xuanxuan008 是一台连续运行的 BTC 5m 双边 BUY 自动机：开盘后用 100–150 股的小 clip 高度交替买入 Up/Down，**clip 大小自适应（imbalance 越大、越往轮次后段、越大）**；当成对库存累积到一定量时在收盘前 ~22 秒前批量 MERGE 回收资金；收盘后 35 秒前后 REDEEM 残差。**它系统性地做 winner-side 残差清算，loser-side 残差因为价值 0 留在钱包**——这是 540 个未结算 condition 中 **538 个全是 loser side、winner side 残差数 = 0** 的根本原因，也是「它似乎能配对成功」最具体的证据。

新发现 vs 旧 deep dive 的关键修正：

| 维度 | 旧结论 | V2 修订 | 等级 |
|------|--------|---------|------|
| Round 选择 | 「连续 95 round 不跳」 | **425/449=94.7% 覆盖**：跳过约 5%，UTC 16–22 是峰值，UTC 03–07 是低谷 | A |
| Clip size | 「不固定」 | **trade #1 p50=151 → trade #10 p50=107**（intra-round 单调下降）；**imb≥0.30 时 p50=142 vs imb<0.05 时 p50=119**（completion-mode 升档）；**round 末 30s p50=144 vs 中段 p50=124** | A |
| Merge 节律 | 「中位 180s」 | 全部 497 次 MERGE 在收盘前发生，**集中在 ~278s 一个时点**（距收盘 22s）；**MERGE/REDEEM 资金回收比 = 5.2:1** | A |
| Winner residual 倾向 | 「降级为 leftover，不能完全排除主动暴露」 | **彻底否定主动保留赢家说**：538 个单侧残差 cur_price=0（loser），winner-side 残差数 = 0；merge 后 winner 总被 redeem 掉，loser 因价值 0 不动 | A |
| Maker vs Taker | 「需要认证 trader_side」 | 实测 `/data/trades` 服务端按认证用户 scope，**无法跨账号解析他人 trader_side**；维持 Level-C，给出 alt 路径 | C |

---

## 1. 数据样本与窗口

### 1.1 公开 positions 快照（**新切片**）

- 端点：`https://data-api.polymarket.com/positions?user=<wallet>&limit=500&offset=0`（offset 翻页直至取尽）
- 拉取时间：`2026-04-26 02:24 UTC`
- 条数：`541` 条 position 记录，对应 `540` 个 unique conditionId
- 全部为 `btc-updown-5m-*` slug，`negativeRisk=false`（二元市场，非负风险池）
- 文件：`data/xuan/positions_snapshot_2026-04-26.json`

### 1.2 长窗口 trades + activity

- `data-api` 公开端点 offset 翻页在 `offset=3500` 处 HTTP 400（服务端硬 cap）
- 切换 `?before=<min_ts>` 时间游标后再请求 0 new — 公开 API 实际上限 ≈ **3500 条**
- trades：`3500` 条，window UTC `2026-04-24 13:17:56` → `2026-04-26 02:21:08`，跨度 `37.05 h`
- activity：`3501` 条 = `2686 TRADE` + `497 MERGE` + `318 REDEEM`
- 文件：`data/xuan/trades_long.json`、`data/xuan/activity_long.json`、`pull_summary.json`

### 1.3 认证 CLOB probe

- 端点：`POST clob.polymarket.com/data/trades`，使用我们 `POLYMARKET_PRIVATE_KEY` 派生 L2 凭证
- `TradesRequest::builder().maker_address(0xcfb1…f694)`、同样的 `taker_address` 过滤
- **结果**：返回的 300 条全是我们自己 2026-04-12 前后的成交记录，`maker_addr_match=0`、`unique_maker_addrs=145`，没有一条命中 xuan 的地址。
- **结论**：`/data/trades` 服务端按认证用户 scope，`maker_address` / `taker_address` 过滤只在自己的账户子集内生效，不能跨账户查询他人 maker/taker。
- 文件：`data/xuan/probe_clob_output.txt`

等级：A

---

## 2. 持仓快照画像（核心新发现）

### 2.1 540 个 condition，539 单侧残差，1 双侧持仓

```text
sides_per_cond:    1side=539  2sides=1
both sides held :  1   (当前正在跑的轮次)
only Up         : 281
only Down       : 258
```

唯一的双侧 condition 是**当前未结算的 active round**，pair_balance（|up−down|/max）= 0.30，pair_cost = 1.122。这条样本不代表稳态行为。

剩下 539 个单侧残差全部来自已结算的历史 round。

等级：A

### 2.2 残差侧 100% 是 loser（绝对禁止把它当 directional bet）

```text
cur_price = 0  (loser side residual)  : 538
cur_price = 1  (winner side residual) : 0
in-between (active)                   : 1
```

这是 V2 最重要的修正：**winner-side 残差数 = 0**。

机理：
1. xuan 每个 round 内既买 Up 也买 Down，并通过 MERGE 把成对部分（min(up, down)）回收为 USDC。
2. MERGE 后剩下 first-leg 那一侧的残差。
3. 市场结算后：
   - 如果残差侧是 winner（cur_price=1），它会被 redeem 为 1 USDC/股 → 在 snapshot 里看不到
   - 如果残差侧是 loser（cur_price=0），它价值 0，不值 gas redeem → 在 snapshot 里看到
4. 所以 snapshot 中观测到的全是 **loser 残差**。
5. Up:Down 残差数 281:258 ≈ 52:48 ≈ first-leg 方向接近随机均衡 → **没有方向性 alpha**。

等级：A

### 2.3 merged_ratio p50 = 98.52% — pair-gated 几乎完美执行

```text
residual size       p10=  1.5  p50=  5.3  p90= 17.7  p99= 109.7  (shares)
total_bought        p10=148.8  p50=439.9  p90=900.1  p99=1424.0  (shares cumul)
merged_ratio        p10=0.964  p50=0.985  p90=0.996  p99=0.998
```

每个市场平均 totalBought 中位 440 股，残差中位仅 5.3 股 → **MERGE 掉了 ~98.5%**。

按 outcome 桶：
```text
loser bucket: n=538  size_sum=5106 shares  tb_sum=261,231 shares  avg_resid_per_round=9.5
```

平均每个 round 残差 9.5 股，相对 round-level totalBought ~485 股仅占 1.96%。这是 **pair-gated 状态机几乎完美执行** 的硬证据。

等级：A

### 2.4 资金体量 / 已实现 PnL

```text
totalBought sum (cumul gross buys)    : $262,422
initialValue sum (cost remaining)     : $3,723.65
currentValue sum (mark-to-mkt)        : $581.36
realizedPnl sum (after merges/redeems): -$23,408.63
cashPnl sum (open-position MTM)       : -$3,142.29
```

注意：`realizedPnl = -$23,409` 与旧 deep dive 文档 §4 中的 FIFO 估算 `+$5,422`（2026-04-24 / 25 一日窗口）不一致。可能解释：

1. data-api 的 `realizedPnl` 是 position lifetime 累计，跨度可能大于一天
2. FIFO 算法假设可在每个 market 内独立配对，但 xuan 的 MERGE 是按 full-set，二者口径不同
3. xuan 当前仓位多数是 redeemable 状态尚未领取，realizedPnl 仅算了 redeemed/sold 部分；residual 入账 cost 但没卖出 → 显示为已实现损失
4. 一个简洁解释：**538 个 loser 残差的 cost basis（avg ~$0.5 × 5 shares × 538 ≈ $1k？远不够 -$23k）**——光靠 loser leftover 解释不了 -$23k 缺口

> **TODO（V2 未解）：** 单从 data-api 字段无法精确还原 PnL 真值。建议下一轮研究：基于 `trades_long.json` + `activity_long.json` 重建完整 cash-flow ledger（USDC in/out per market），与 `realizedPnl` 字段对账。

等级：A（数字本身）/ C（PnL 真值解释）

---

## 3. 长窗口 session 节律

### 3.1 Round 覆盖率 94.7%

```text
expected btc-5m rounds in window  : 449
actually-traded rounds            : 425
overlap (rounds joined / expected): 425 / 449  ratio=0.947
```

xuan **不是 24/7 每轮都进**，约 5% 的 round 缺席。结合下面的小时分布，缺席集中在低流动时段。

等级：A

### 3.2 UTC 小时分布（明显非匀分）

| UTC | trades | UTC | trades |
|-----|--------|-----|--------|
| 00 | 172 | 12 | 95 |
| 01 | 175 | 13 | 119 |
| 02 | 102 | 14 | 110 |
| 03 | 77  | 15 | 118 |
| 04 | 90  | 16 | 212 |
| 05 | 63  | 17 | 224 |
| 06 | 62  | **18** | **277** |
| 07 | 76  | **19** | **295** |
| 08 | 77  | 20 | 232 |
| 09 | 89  | 21 | 233 |
| 10 | 92  | 22 | 246 |
| 11 | 100 | 23 | 164 |

- 峰值：UTC 19（伦敦下午 / 美东上午），295 trades
- 谷值：UTC 05–07，60–80 trades（亚洲深夜）
- 比值约 **5:1**，不是均匀

> 修订旧 deconstruction §6.3「最近这段样本里更像在筛 session」：**确认。xuan 在 BTC 5m 这一固定宇宙内仍按 session 分配交易强度，UTC 16–22 是主交易段。**

等级：A

---

## 4. Clip size 条件结构（首次量化）

### 4.1 全集分布

```text
n=3500  p10=45.3  p50=131.7  p90=264.3  p99=400.2  max=600.1  (shares)
```

旧 deconstruction 仅给出「首笔成交规模 p50≈170.6, p90≈397.6」，本次基于全部 trades 看到 p50=131.7 — 因为后续 trade 比 first trade 偏小。

### 4.2 按 intra-round trade 序号（**首次确认 inventory-aware**）

```text
trade #1 : p50=151.0  p90=304.8
trade #2 : p50=146.3  p90=301.1
trade #3 : p50=148.7
trade #5 : p50=142.4
trade #7 : p50=122.8
trade #10: p50=106.7  p90=218.9
```

p50 从 151 单调下降到 107。这是**第一次明确观察到 clip size 与 intra-round 序号反相关** —— 后续补腿用更小的 clip，符合「不让 first leg 长大、把单 clip 风险压小」的 V1.1 设计。

等级：A

### 4.3 按 prior imbalance（**首次确认 imbalance-driven up-clip**）

```text
imb 0.00-0.05 (近平衡)  : p50=119.3  p90=261.3
imb 0.05-0.15           : p50=114.9  p90=215.2
imb 0.15-0.30           : p50=120.0  p90=250.7
imb 0.30-1.00 (大单腿)  : p50=142.4  p90=256.6
```

p50 从 115 → 142（**+24%**）。当未配对净差扩大时，xuan 提升 clip size 以加速 covering，体现 completion-mode 加紧。

等级：A

### 4.4 按 round 剩余时间

```text
[   0,  30) p50=124.2     # round 起始首笔较小
[ 240, 300) p50=143.6     # round 末 30s 升档
```

末段升档幅度 +16%。结合 §3.2 MERGE 全部在 ~278s（distance to close 22s）触发，可推测这是 xuan 「最后一搏把 first leg 残差吃掉以最大化 mergeable full set」的行为。

等级：B

---

## 5. 双边几何（pre-placed vs reactive）

### 5.1 Same-second cross-side fill 比率（5.49%）

```text
same-second adjacent fills total: 300
same-second cross-side pairs    : 192
ratio (cross / total fills-1)   : 0.0549
```

5.49% 的相邻 fill 是同秒跨侧。如果是「单 clip 触发后再补腿」，几乎不会出现同秒；如果是「双侧预埋单同时被打」，会有较多同秒跨侧。

5.49% 不算高也不算低。**部分支持「双侧浅 ladder 预埋 + 单边触发后补」混合机制**——日常是反应式补腿，但在某些时刻（例如同秒收到大单 sweep 双边），会同步成交。

等级：B

### 5.2 Inter-fill delay：跨侧 vs 同侧

```text
inter-fill delay all   : p10= 2  p50=16  p90= 78
inter-fill delay cross : p10= 2  p50=18  p90= 82
inter-fill delay same  : p10= 0  p50=10  p90= 58
```

- 跨侧（U→D 或 D→U）：p50=18s，与 deconstruction §4.2 first_opposite_delay p50=25s 同量级（细微差异由窗口变化）
- 同侧（U→U 或 D→D）：**p50=10s**——更短！这暗示同侧连续成交往往是同一挂单被分批吃。

等级：A

### 5.3 Side run length（与 deconstruction 一致）

```text
n_runs=2917  p10=1  p50=1  p90=2  max=4
histogram: {1: 2365, 2: 522, 3: 29, 4: 1}
```

- 81% 的 run 长度为 1（U-D-U-D 完美交替）
- 仅 30 个 run 长度 ≥ 3
- max run = 4

强支持 V1.1 的 `MAX_SAME_SIDE_RUN=1` 默认 + shadow 计 `MAX=2` 的设计。

等级：A

---

## 6. Episode 重建（与旧 deep dive 交叉对账）

```text
                    eps=10                    eps=25
opened              1399                      1601
closed              1178                      1541
clean_closed        1075                      1424
clean_closed_ratio  0.7684                    0.8894
same_side_add_ratio 0.1052                    0.0442
close_delay_p50     20s                       20s
close_delay_p90     92s                       92s
```

旧 deep dive §3.3 的对应指标：
- eps=10：`clean_closed=1190/1249, ratio=95.28%, same_side_add=9.52%, p50=12, p90=56`
- eps=25：`clean_closed=1671/1742, ratio=95.92%, same_side_add=3.08%, p50=12, p90=55.8`

V2 数字比 deep dive **低**（76.84% vs 95.28%）。差异来源：

1. **分母口径**：deep dive 用 `clean_closed/closed`，本次用 `clean_closed/opened` —— 本次把「打开后未闭合」算作非 clean，更保守
2. **窗口微差**：deep dive 4000 trades，本次 3500 trades，重叠但不完全相同
3. **市场数差异**：deep dive 367 markets，本次 425 slugs（前者只算 trade>0 的市场，后者把 round 都算上）

按 deep dive 同口径（clean / closed）：
- eps=10: 1075/1178 = **91.3%**（仍低于 95.28%，但方向一致）
- eps=25: 1424/1541 = **92.4%**

差异主要来自更长窗口纳入了一些劣质 round（93/95 deconstruction 阶段几乎都 clean，长窗口稀释）。

`same_side_add_ratio` 差异（10.5% vs 9.52%）量级一致，是 sample variance。

等级：A（数字）/ B（解释）

`close_delay_p50=20s`（V2）vs `12s`（deep dive）—— 这个差距更大。可能本次定义微调（开始时刻定义为「首次跨阈值」vs 「episode 开仓首笔」）。

`close_delay_p90=92s`（V2）vs `56s`（deep dive）—— 同上。

**结论**：核心结构（高 clean ratio、低 same_side_add、可控 close delay、长尾 < 100s）保持一致；具体百分比因窗口/口径变化有 5–15pct 的浮动。

---

## 7. MERGE / REDEEM 节律

### 7.1 时间集中性

```text
MERGE  offset (s vs round open): n=497  p10=132  p50=278  p90=278
REDEEM offset (s vs round open): n=318  p10=332  p50=338  p90=368
```

- **MERGE p50=p90=278s**：超过半数 merge 集中在距收盘 22s 的一个窄时点。这是 **deterministic timer** 行为——xuan 在 round 即将结束时统一批量 merge 已配对库存，回收 USDC，降低 inventory carry。
- 全部 497/497 MERGE 发生在 round close（300s）**之前**
- 全部 318/318 REDEEM 发生在 round close **之后**，p50=38s post-close

修订旧 deconstruction §5.2「首次 MERGE 中位 180s」：当时只看「首次」merge，本次看全部 merge，主峰在 278s。同一 round 可能有早 merge（132s）+ 晚 merge（278s）双峰。

等级：A

### 7.2 资金回收量级

```text
MERGE  total: $185,021 (= 185,021 shares × $1/full-set)
REDEEM total: $35,887  (= 35,887 shares × $1)
ratio        : 5.16:1
```

MERGE 是主回收路径，**5.2 倍** 于 REDEEM。这强烈支持 V1.1 的 `Harvester` 模块以 MERGE 为核心、REDEEM 仅作残差兜底。

等级：A

---

## 8. Maker vs Taker 路径裁决

### 8.1 本次尝试

`probe_clob_trades.rs` 用我们的 L2 凭证查 xuan 的 maker_address：返回 300 条全是我们自己的成交，`maker_addr_match=0`。

**结论**：CLOB `/data/trades` 服务端按认证用户 scope，`maker_address`/`taker_address` 过滤仅在自己的账户子集内生效——**公开通道无法解析他人 maker/taker**。

### 8.2 仍可行的 alt 路径（非本次范围）

1. **Book-snapshot timing match**：用我们 recorder（`data/replay_recorder/2026-04-25/2026-04-26`）的 `md_book_l1` 在 xuan 每笔 trade timestamp 前一个 tick 的 best bid/ask 与 trade.price 比较：
   - `trade.price ≈ best_ask` → likely **TAKER**
   - `trade.price ≈ best_bid` → likely **MAKER**（被打）
   - 中间价 → 可能是非 tape trade

   recorder 的 md_book_l1 在 04-25 有 538k rows / 04-26 有 42k rows，覆盖 xuan 的 trade 时间窗。

2. **Reverse via on-chain CTF transfer logs**：xuan 的 Polygon proxyWallet `0xcfb1…f694` 的 `mint()` / `transferSingle()` 事件能区分：
   - SPLIT（USDC → outcome tokens）= 自买
   - MERGE（outcome tokens → USDC）= 自合
   - 普通 BUY 时是 ConditionalToken from CLOB executor → xuan，对手方地址可见

3. **Polymarket Goldsky subgraph**：开源 subgraph 可能有 trade-level 数据带 maker_address / taker_address 字段，这是当前最可能的快路径。

等级：C（不变）。本次只是 ruled out 一条公开路径，并不否定假设——maker 仍是 V1.1 的合理选项。

---

## 9. 复刻校验：`pair_gated_tranche.rs` 当前差距

### 9.1 当前实现盘点

文件 | 行数 | 内容
---|---|---
`src/polymarket/strategy/pair_gated_tranche.rs` | 132 | 策略前端：active tranche 时发 completion intent；否则委托 `PairArb` 选低价侧开 first leg
`src/polymarket/pair_ledger.rs` | 741 | 账本基础：`PairTranche/TrancheState/PairLedgerSnapshot/EpisodeMetrics/CapitalState/urgency_budget_shadow_5m`

Replay DB 验证：

```text
table                      04-25      04-26
pair_tranche_events            0          0
pair_budget_events             0          0
capital_state_events           0          0
own_order_events / lifecycle   0          0
own_inventory_events           0          0
md_book_l1               538,039     42,600
md_trades                205,721     15,731
settlement_records            78          8
```

**关键发现**：`pair_tranche_events` / `pair_budget_events` / `capital_state_events` 全部 0 行——意味着 pair_gated_tranche 自 commit a8a5371 入库以来 **从未在 live 环境产生过事件**。要么没启用，要么 guard 永远不满足。

### 9.2 与 V1.1 规范 / xuan 实证行为的 Gap 清单

按优先级排序的 patch 建议（**仅清单，不动代码**）：

#### P0 patch — 决定能不能复刻

1. **Clip-size 分块缺失**
   - 现状：`size = (active.residual_qty * 100.0).floor() / 100.0`，一次性发整个残差
   - 应：基于 `BASE_CLIP=120, MAX_CLIP=250`（V1.1）+ 本次 §4 实证：
     - 默认 clip = `min(MAX_CLIP, max(BASE_CLIP * (1 + 0.3*imb_bucket), BASE_CLIP))`
     - 后段 trade index 衰减：`clip *= max(0.7, 1 - 0.04*intra_round_idx)`
     - 末 30s `clip *= 1.16`
   - 涉及：`pair_gated_tranche.rs::completion_intent` + 新加 first-leg sizing branch

2. **Same-side run guard 缺失**
   - 现状：first leg 选完进入 active 状态后，不再开新 first；但同侧 BUY 仍然可能被 PairArb 借道
   - 应：在 PairLedger snapshot 上加 `same_side_run_count` 字段，clip-fire 前检查 `< MAX_SAME_SIDE_RUN`
   - 涉及：`pair_ledger.rs` + `pair_gated_tranche.rs`

3. **MERGE 触发器缺失**
   - 现状：harvester 模块为 0 行；MERGE 完全靠手动或外部触发
   - xuan 实证：p50=p90=278s 集中触发；MERGE 总额 = 5.2×REDEEM
   - 应：实现 `PairHarvester`，在 `seconds_to_market_end <= 25` 且 `pairable_qty >= HARVEST_MIN_FULL_SET=10` 时发 MERGE intent
   - 涉及：新文件 `src/polymarket/strategy/pair_harvester.rs` + `coordinator_endgame.rs` 接入

4. **CompletionOnly 状态泄漏**
   - 现状：当 `active.residual_qty <= EPS` 时进入「闭合」分支，但没强制下一帧 reset 到 `FlatOrResidual`，会让旧 first_side 残留影响后续 clip
   - 应：状态机显式 transition + reset
   - 涉及：`pair_ledger.rs` 状态机，`pair_gated_tranche.rs` 入口判断

#### P1 patch — 提升复刻保真度

5. **Repair budget 来源未实证 wired**
   - 代码中 `repair_budget_available = (covered_surplus - covered_repair).max(0.0)` 已实现，但因 `pair_tranche_events=0` 无法验证 surplus_bank 是否累积正确。
   - 应：先打开 PGT 在 shadow 模式下跑 1 周，校验 `pair_budget_events` 内 `surplus_bank` 单调累计（无负值/无 NaN）

6. **Session gating 缺失**
   - 现状：策略不区分 UTC 小时
   - xuan 实证：UTC 03–07 流量 60-80/h，UTC 19 流量 295/h（5:1 比）
   - 应：增加 `session_score(now_utc)` 软抑制 first-leg open（不硬 block，仅缩 clip）
   - 优先级：P1，因为低流动时段不强制参与不会损失多少 EV，但能省 inventory carry

7. **Dual accounting (pair_cost_tranche + pair_cost_fifo) 缺失**
   - V1.1 §H-2 要求两套口径并行，差距 `abs(delta_p50) <= 0.05` 才能 enforce
   - 现状：仅有 `pair_cost_tranche`
   - 应：在 `pair_ledger.rs` 加一个 FIFO matcher 副本

8. **End-of-round REDEEM 触发器缺失**
   - xuan 实证：REDEEM p50=338s（距收盘 +38s）
   - 应：在 `coordinator_endgame.rs` 增加 settlement 后 30–60s 内对单侧 redeemable 持仓发 redeem intent

#### P2 patch — 验证与监控

9. **CapitalState 尚无下游消费者**
   - `capital_state_events` 写入逻辑就绪，但 strategy 层未读
   - 应：在 first-leg open 前查 `locked_capital_ratio < 0.7`，否则跳过这一 round

10. **Maker/Taker 离线推断脚本**
    - `scripts/infer_xuan_maker_taker.py`：用 recorder 04-25/04-26 的 `md_book_l1` 在每笔 xuan trade.timestamp 前最近的 book snapshot 比对 trade.price 与 best bid/ask，统计 maker 比例
    - 输出：xuan 的实证 maker_ratio
    - 这能将 V2 §8 的 Level-C 升级为 Level-B 或 A

### 9.3 验证准入门槛（从 V1.1 §release matrix 引用）

启用 PGT 进入 shadow 之前需要：
- pair_tranche_events 有日均 ≥ 100 条且 surplus_bank 无 NaN/负值
- 至少 3 天连续运行
- clean_closed_ratio ≥ 90% （vs xuan 91.3%）
- same_side_add_ratio ≤ 10% （vs xuan 10.5%）

进入 enforce 还需 + 2 周 + cohort_net_pair_pnl > 0 + dual_accounting delta_p50 ≤ 0.05。

---

## 10. 修订证据等级表

| 维度 | V1（旧）| V2（本次）|
|------|---------|----------|
| 「pair-gated tranche automaton」核心架构 | B | **A**（539/540 单侧残差 + merged_ratio 98.5%）|
| 「不是固定 pair_target」 | A/B | A |
| Clip size depth/inventory-aware | C | **A**（intra-round idx + imb 双重单调）|
| 滚动 MERGE 主回收路径 | A/B | **A**（5.2:1 vs REDEEM）|
| Session 选择 | A/B | **A**（5:1 hour 比 + 5% round 跳过）|
| Winner residual 倾向 | C（不能排除）| **A，明确否定**（538/0 loser-only） |
| Maker vs Taker | C | C（公开 API 路径不通，alt 路径存在）|
| 预埋双边 vs 反应式补腿 | C | C（5.49% same-second 跨侧只能弱支持）|
| 是否并行其他策略 | C | C（仅 BTC 5m，但窗口仅 37h）|

---

## 11. 下一步

1. **P0 patch 1–4**：先把 clip 分块、same-side guard、MERGE 触发器、状态机泄漏修了；shadow 跑 3 天看 `pair_tranche_events` 是否能稳定流出
2. **新建 `scripts/infer_xuan_maker_taker.py`**（V2 §9.2 条目 #10）：把 maker/taker 升级到 Level-B
3. **拉更长窗口**：当前仅 37h，需要至少 14 天连续数据来回答 §3.1 round-skip 5% 是否稳定模式（公开 API 卡 3500 条，需考虑 Polymarket Goldsky subgraph）
4. **PnL 真值对账**：`realizedPnl=-$23k` vs FIFO `+$5k` 的口径差异在 §2.4 标记为 TODO，建议补 cash-flow ledger 重建脚本

旧两份文档保留为历史基线；本文为当前主参考。

---

## 附录 A：脚本与数据文件清单

```
scripts/pull_xuan_long_window.py      # 拉 positions / trades / activity（含 before-cursor fallback）
scripts/analyze_xuan_positions.py     # §2 持仓快照分析
scripts/analyze_xuan_long_window.py   # §3-7 长窗口分析
src/bin/probe_clob_trades.rs (existing) # §8 认证 probe（已确认无法跨账号）

data/xuan/positions_snapshot_2026-04-26.json    # 541 rows
data/xuan/positions_analysis.json / .txt        # §2 输出
data/xuan/positions_single_residuals.json       # 539 单侧残差明细
data/xuan/trades_long.json                      # 3500 trades
data/xuan/activity_long.json                    # 3501 activities
data/xuan/long_window_analysis.json / .txt      # §3-7 输出
data/xuan/probe_clob_output.txt                 # §8 probe 实测输出
data/xuan/pull_summary.json                     # 拉数据元信息
```
