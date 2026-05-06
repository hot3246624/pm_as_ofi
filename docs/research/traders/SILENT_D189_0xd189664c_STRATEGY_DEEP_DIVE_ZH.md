# `0xd189664c...` (silent_d189) 策略深度研究

生成时间：`2026-04-27`
研究对象：钱包 `0xd189664c5308903476f9f079820431e4fd7d06f4`
Pseudonym：未公开（gamma-api `/public-search` 返回无 profile）
本文 codename：**silent_d189**

研究动机：用户提供的 10 个候选钱包之一，quick probe 确认它**有 MAKER_REBATE event**（注册 maker）+ btc 5m 主导 + 极少 SELL（仅 0.6%）。是 xuan/gabagool22 之外又一个 confirmed maker，值得深挖。

证据等级：`A`/`B`/`C` 同前。

---

## 0. TL;DR

**silent_d189 是新 archetype F：「Near-resolution ITM sniper / 大资金 selective directional confirmed maker」**——这是与 xuan / late_grammar / quietb27 / gabagool22 都不同的第 6 类玩法：

> **核心机制**：等价格倾向某侧到极端（≥ 0.99 或 ≤ 0.001）时，用大 clip 限价 BUY 进入 winner side。靠 「0.01 单股边际 × MAKER_REBATE × 高准确率」 累积小额稳定利润。**只参与 ~13% 的 BTC 5m round**（极强 round selection）+ **80% rounds 完全单方向交易**（不双边对冲）+ **靠 REDEEM 收水（223 次 $640k）远大于 MERGE 收水（13 次 $10k）**。

这与 xuan/gabagool22「双边 BUY 库存管理 + MERGE 收水」**完全不同**——silent_d189 不做双边，而是做「单边低风险 ITM 押注 + 注册 maker 拿 rebate」。

但与 quietb27（archetype D）也不同：
- quietb27：未注册 maker + IOC 大单 taker + 中等价位（不限于 0.99/0.001）
- silent_d189：**注册 maker (MAKER_REBATE 4 次) + 限价 maker + 集中在 0.99/0.001 极端价位**

```
新 archetype F：late-round confirmed-maker ITM sniper
  - selective rounds: 13% BTC 5m coverage（vs xuan 95%）
  - 极端价位限价 BUY：0.99 或 0.001
  - 单边 directional：80% rounds 单方向
  - 大资金：max single clip 115,000 shares (~$11.5k notional)
  - REDEEM 主导：64× MERGE
  - 注册 maker：MAKER_REBATE 4 events ⭐
```

---

## 1. 数据样本与窗口

### 1.1 数据规模（**最长窗口**）

- 拉取时间：`2026-04-27 02:14 UTC`
- positions = **8 条**（极清，全部都是待 redeem 的 winner-side 残差）
- trades = `3500`，window UTC `2026-03-13 05:04` → `2026-04-27 01:58`，跨度 **1076.89 h ≈ 44.9 天**
- activity = `3500` = `3250 TRADE` + `223 REDEEM` + `19 MERGE` + **`4 MAKER_REBATE` ⭐** + `4 YIELD`

silent_d189 的窗口比所有之前研究对象都长：
- xuan: 37h
- gabagool22: 9h
- quietb27: 361h (15 天)
- late_grammar: 14.6h
- **silent_d189: 1077h (45 天)**

平均频率 = 3500/1077 = **3.25 trades/h**（最低，但跨期最长）。

### 1.2 MAKER_REBATE 4 次（确认注册 maker）

| ts (UTC) | usdcSize ($) |
|----------|------:|
| 1777251642 (2026-04-26) | 72.86 |
| 1777165254 (2026-04-25) | 92.67 |
| 1777078806 (2026-04-24) | 50.17 |
| 1776992428 (2026-04-23) | 72.03 |

**每天一次 MAKER_REBATE**（4 天连续）= **rebate 是按日结算**，silent_d189 至少最近 4 天每天都达到 maker tier。

平均 $72/day × 365 = ~$26k/year just from rebates. 这是稳定收入。

等级：A（直接 on-chain 证据）

### 1.3 YIELD 4 次（新发现的 event 类型）

```
ts          usdcSize
1777248724  0.0093
1777162404  0.0098
1777075832  0.0087
1776989428  0.0098
```

每次约 **$0.01**，4 次共 ~$0.04。极少。可能是 Polymarket 对持仓的某种 yield 机制（USDC 借出收益？小额 promotional yield？），具体语义不重要。

等级：A（数据），C（机制解读）

### 1.4 文件

```
data/silent_d189/positions_snapshot_2026-04-27.json    # 8 rows
data/silent_d189/trades_long.json                      # 3500 rows, 45 天
data/silent_d189/activity_long.json                    # 3500 rows
data/silent_d189/positions_analysis.json/.txt
data/silent_d189/long_window_analysis.json/.txt
data/silent_d189/pull_summary.json/pull_log.txt
```

---

## 2. Universe

| Universe | trades | rounds | t/round |
|----------|-------:|-------:|--------:|
| **btc-updown-5m**   | **3113** | **1727** | 1.80 |
| nba (体育)         |  182 |  143 | 1.27 |
| btc-updown-15m     |  115 |   72 | 1.60 |
| bitcoin-daily      |   71 |   46 | 1.54 |
| eth-updown-5m      |    9 |    8 | 1.13 |
| eth-updown-15m     |    5 |    1 | 5.00 |
| us (政治)          |    4 |    2 | 2.00 |
| israel (政治)      |    1 |    1 | 1.00 |

- btc-updown-5m **绝对主导**（89% trades）
- 兼营少量 NBA + bitcoin daily + 少量政治事件（us/israel）
- **t/round 中位 1.6-1.8 trades/round** — 单 round 通常只交易 1-2 笔（相比 xuan 中位 10、gabagool22 中位 11.7）

低 t/round 是 silent_d189 archetype 的关键特征——**它一进就大单，一两笔决胜负**。

等级：A

---

## 3. Round selection — 13.4% coverage

```text
expected btc-5m rounds in 1077h: 12927
actually-traded btc-5m rounds:    1727
coverage:                         13.4%
```

仅参与 13.4% 的 BTC 5m round——**与 quietb27 的 17.9% 接近，是 xuan 95% 的 1/7**。

这是强 round selector 的典型特征。

等级：A

### 3.1 UTC 小时分布（极端集中）

```text
UTC  trades   UTC  trades
00   378      12   28
01   466      13   20
02   403      14   2
03   231      15   6
04   357      16   7
05   416      17   0
06   400      18   9
07   317      19   1
08   141      20   0
09   66       21   14
10   54       22   22
11   51       23   111
```

**UTC 0-7 是高峰段（300-470/h），UTC 14-20 几乎全空（0-9/h）**——比值 **∞:1**，比 quietb27 的 8:1 还更极端。

UTC 0-7 = 北美晚间 + 亚洲早晨——可能对应 BTC 实时价格的高波动时段。

等级：A

### 3.2 Day-of-week

```text
Mon: 499  Tue: 390  Wed: 497  Thu: 426  Fri: 471  Sat: 679  Sun: 538
```

Saturday 是峰值（679）但分布相对均匀，**没有明显工作日 vs 周末分化**。

等级：A

---

## 4. 大 clip 在极端价位（核心发现）

### 4.1 Clip 分布

```text
n=3500
p10=49.1  p50=198.6  p90=1599.8  p99=20000.0  max=115,000
```

vs xuan max=600；late_grammar max=633；quietb27 max=9392；gabagool22 max=10。

silent_d189 max=115,000 shares 是所有研究对象中**最大**——单笔 ~$57k notional（按 $0.5 价位估算；如在 $0.99 价位则 $113k notional）。

### 4.2 大 clip 的价格分布（**关键证据**）

91 笔 > 5000 shares 的大 clip 中，**几乎全部成交在 0.99 或 0.001 极端价位**：

| slug | side | size | price |
|------|------|-----:|------:|
| nba-bos-phi-2026-04-26 | Celtics | 21,940 | **0.999** |
| btc-updown-5m-1777010100 | Up | 9,993 | **0.99** |
| btc-updown-5m-1776995100 | Up | 9,993 | **0.99** |
| btc-updown-15m-1776925800 | Up | 7,425 | **0.001** |
| btc-updown-5m-1776918600 | Up | 9,783 | **0.99** |
| btc-updown-5m-1776824700 | Up | 9,993 | **0.99** |
| btc-updown-5m-1776824100 | Up | 11,391 | **0.99** |
| btc-updown-5m-1776753000 | Up | 11,991 | **0.99** |
| btc-updown-5m-1776736200 | Down | 9,993 | **0.99** |
| nba-tor-cle-2026-04-20-spread-home-8pt5 | Cavaliers | 5,797 | **0.99** |

**全部 0.99（或 0.001）** —— 这是 ITM 套利的硬证据。

机制：
- Round 进入末段，价格已倾向某侧（如 Up 显著领先 Down）
- silent_d189 在 winner side 挂 BUY at $0.99 limit
- 市场 sellers 把 winner shares dump 至 $0.99（也许他们等不及结算想立刻拿钱）
- silent_d189 的限价 BUY 被打中，吃下大量 winner shares
- Round 结算后 winner share = $1，每股净利 $0.01 + maker rebate

11,991 shares × $0.01 = $120 单笔利润。看起来很少，但每天可重复几十笔。

等级：A

### 4.3 By round-time 分布

```text
[0,30)     n=  62   p50=194.4
[30,60)    n=  59   p50=191.5
[60,120)   n= 184   p50=130.2
[120,180)  n= 261   p50=192.8
[180,240)  n= 646   p50=197.0
[240,300)  n=1718   p50=233.9   ← 49% of trades in last minute!
[300+)     n= 279   p50=299.9
```

**1718/3500 = 49% 的 trades 集中在 round 末 60 秒（240-300s）**——确认是 late-round sniper 行为。

而且这 1718 笔末段 trades 的 p90=1596 大 clip——末段是大 clip 高发段。

等级：A

---

## 5. Episode reconstruction & directional behavior

```text
                     eps=10                  eps=25
opened               2010                    1980
closed               106                     127
clean_closed         90                      106
clean_closed_ratio   4.48%                   5.35%
same_side_add_ratio  20.08%                  20.02%
close_delay_p50      42s                     44s
```

silent_d189 的 4.48% clean_ratio 接近 quietb27 的 2.17%（vs xuan 91.3%）—— **不是 pair-gated maker**。

### 5.1 Per-round directional metrics

```text
imbalance |U-D|/total median=1.000  mean=0.869
rounds with imb > 0.5:           1597 / 1854 (86.1%)
rounds with imb < 0.05:            88 / 1854 (4.7%)
rounds traded only ONE side:     1599 / 2000 (80.0%)
```

vs xuan ~0% pure directional / quietb27 68.9%。**silent_d189 80% pure directional**——比 quietb27 还更纯。

### 5.2 Run length

```text
n_runs=2486  p10=1  p50=1  p90=2  max=13
histogram: {1: 1854 (75%), 2: 433 (17%), 3: 117 (5%), ...}
```

**75% run = 1**——意外地交替性高（比 quietb27 的 40% 高得多）。

但结合 80% pure directional，这意味着：**silent_d189 大多数 round 只交易 1 笔**（一买就完结），所以 run length 自然短。run > 1 的 25% 是少数 round 内连续买入累加。

等级：A

---

## 6. MERGE / REDEEM cadence — REDEEM 64× MERGE

```text
MERGE  数量: 19   total: $10,646
REDEEM 数量: 223  total: $640,010
ratio: 1:60   (REDEEM 远大于 MERGE)
```

vs xuan 5.2:1（MERGE 主导）；late_grammar 0:∞（无 MERGE）；quietb27 17:1（MERGE 主导但量小）；gabagool22 12:1（MERGE 主导）。

silent_d189 **完全相反**——**REDEEM 主导**。机制：
- 它不双边持仓，所以无 pairable inventory 可 MERGE
- 它单边买 winner side at $0.99 → 等结算 → REDEEM 拿 $1
- $640k REDEEM total / $11k 单笔最大 ≈ 60+ 大单 redeem，加上小单累积

### 6.1 MERGE / REDEEM 时机

```text
MERGE p50=372s   p90=918s  (round close 后 1-15 min)
MERGE after round close: 12/13   (in-round before close: 1)

REDEEM p50=728s  p90=2418s (round close 后 7-40 min)
REDEEM all post-close: 203/203
```

19 次 MERGE 中 12 次都是 round close 后才 MERGE——这 19 次是少数双边持仓 round 的清理动作。

REDEEM p50 = 728s = round close 后 7 分钟。比 xuan 38s 慢，比 late_grammar 2.18h 快——中等节奏。

等级：A

---

## 7. 单侧残差结构 — winner-heavy（与 xuan 反向）

```text
6 single-side residuals:
  cur_price = 0 (loser): 0
  cur_price = 1 (winner): 5
  in-between: 1
```

vs xuan 538 loser / 0 winner。**silent_d189 全部 winner-side 残差**——确认是「未及时 redeem 的 winner profits」。

merged_ratio p50 = 0.0000 = 几乎没 MERGE 过。这些 winner residuals 直接来自原始 BUY，没经历过 MERGE。

等级：A

---

## 8. Maker / Taker 路径裁决（A 级 confirmed maker）

### 8.1 直接证据

`MAKER_REBATE` event 4 次，连续 4 天每天 1 次。**这是 Polymarket 注册 maker 的硬性证据**。

vs：
- xuan 0 events
- late_grammar 0 events
- quietb27 0 events
- gabagool22 1 event
- **silent_d189 4 events ⭐**（最多）

### 8.2 间接证据互证

- 大 clip 在 $0.99 / $0.001 极端价位 — maker 在 deep ITM/OTM 价位挂限价单
- Round close 前 1 minute 集中成交 — 限价单等到 round 末段被打
- 75% run = 1 + 80% pure directional — 单笔大单进入即完成，不连续追加

**等级：A（直接 on-chain 证据）**——silent_d189 是 **registered maker** 的概率几乎 100%。

---

## 9. 与 4 个之前研究对象的横向对比

| 维度 | xuan | late_grammar | quietb27 | gabagool22 | **silent_d189** |
|------|------|--------------|----------|------------|------|
| Universe | BTC 5m | 多 venue | BTC 5m | 多 venue | **BTC 5m 主导 + NBA + 政治** |
| 主市场 | BTC 5m | btc/eth × 5m/15m | BTC 5m | btc/eth × 5m/15m | **BTC 5m (89%)** |
| 窗口 | 37h | 14.6h | 361h | 9h | **1077h (45 天)** |
| 平均 trades/h | 95 | 240 | 9.7 | 389 | **3.25 (最低)** |
| Round coverage | 95% | 98% | 18% | 90% | **13.4%** |
| 双侧同持率 | 0.2% | 89% | 0% | 0% | **14% (1/7)** |
| Pure directional rounds % | 0% | n/a | 69% | 13% | **80%** |
| Clip p50 | 132 | 10 | 60 | 9.9 | **199** |
| Clip max | 600 | 633 | 9392 | 10 | **115,000** |
| 大 clip 价位特征 | 50/50 区间 | 50/50 | 中等价位 | 50/50 | **0.99 / 0.001 极端** |
| MERGE 数 (pro-rated 14.6h) | 196 | 0 | 4.6 | 102 | **0.26** |
| REDEEM 数 (pro-rated 14.6h) | 125 | 451 | 0.3 | 115 | **3.0** |
| MERGE/REDEEM 比 | 5.2:1 | 0:∞ | 17:1 | 12:1 | **1:60 (REDEEM 主导)** |
| Inter-fill same p50 | 10s | 2s | 0s | 10s | 18s |
| Maker/Taker | Maker (B) | Maker (B) | Taker (B) | Maker (A) | **Maker (A) ⭐** |
| MAKER_REBATE event | 0 | 0 | 0 | 1 | **4** |
| 残差侧偏好 | 100% loser | 73 winner / 14 loser | 0 visible | 0 visible | **5 winner / 0 loser** |
| Archetype | C | C' | D | E | **F (新)** |

### 9.1 silent_d189 vs xuan

**最大差异**：xuan 双边 BUY 配对玩法（91% clean_ratio），silent_d189 单边 ITM sniping（80% pure directional）。

但有一处神奇相似：**xuan 的 75% run=1 与 silent_d189 的 75% run=1 完全一致**——两者都呈现「短 run」交替 pattern，但机制不同：
- xuan：双边交替（U-D-U-D），run=1 是 pair-gated 的副产物
- silent_d189：80% rounds 只 1 笔成交，run=1 是「一击即终」的副产物

### 9.2 silent_d189 vs gabagool22

两者都是 confirmed maker。差异：
- gabagool22：multi-venue + 严格 10 股 clip + post-close MERGE
- silent_d189：BTC 5m 主导 + 大 clip (max 115k) + 极少 MERGE / REDEEM 主导

silent_d189 的 MAKER_REBATE 频率更高（4 vs 1），意味着它是更高 tier maker。

### 9.3 silent_d189 vs quietb27

两者都 selective directional：
- quietb27 17.9% round coverage / silent_d189 13.4% — 接近
- quietb27 max clip 9392 / silent_d189 max 115,000 — silent_d189 大 12×
- quietb27 中等价位 taker / silent_d189 极端价位 maker — **执行几何完全相反**

silent_d189 是「**limit-order 版的 quietb27**」。

---

## 10. Archetype 谱系更新（增加 F）

```
                              MERGE 频率
                   高 (in-round)│
                                │
              Archetype C (xuan)● 单 venue 紧密 maker  ← V1.1 蓝图位置
                                │
                                │ Archetype E (gabagool22)
                                ●  multi-venue tenor-adaptive maker
                                │  (post-close MERGE)
                                │
                  ┌─────────────┼─────────────┐
                  │             │             │
        单 venue ←┤             │             ├→ 多 venue
                  │             │             │
        ● (quietb27)              ●(late_grammar)
        Archetype D            Archetype C'
        selective directional   multi-venue 不 MERGE
        taker                   maker
                                │
                                │
                  低 (post-close 或 0)
                                │
                                │
                                │
                ● (silent_d189)
                Archetype F (新)
                BTC 5m 单边 + 极端价位 maker
                + 4 MAKER_REBATE
                + REDEEM 主导 (60× MERGE)
```

### 10.1 archetype 完整列表

- **A**：体育 directional（leaderboard rank01-02-04-05-06，BUY-only）
- **B**：政治 directional with active SELL（rank03 tripping，含 SELL）
- **C**：crypto-perpetual single-venue passive maker（**xuan**）
- **C'**：crypto-perpetual multi-venue passive maker（**late_grammar**，无 MERGE）
- **D**：crypto-perpetual single-venue selective directional taker（**quietb27**）
- **E**：crypto-perpetual multi-venue tenor-adaptive maker（**gabagool22**，confirmed registered maker）
- **F（新）**：**BTC 5m 主导 selective ITM-sniper confirmed maker**（**silent_d189**，4 MAKER_REBATE + 极端价位限价）

archetype F 是 **D 和 E 的混合**：selective 像 D，confirmed maker 像 E，但执行几何（限价 BUY 在 0.99/0.001）是独特的。

---

## 11. 复刻判断：**强烈不推荐** — alpha 来源与我们 PGT 完全正交

### 11.1 silent_d189 的 alpha 来源（推断）

```
alpha 路径:
  1. 监控 BTC perpetual 价格 + Polymarket 当前 round 价格
  2. 当价格倾向某侧足够确定时（implied prob > 99%）
  3. 在 winner side 挂大 limit BUY at $0.99
  4. 等待市场 sellers (短期 cash-out 派、loser-side bagholder) dump 到 $0.99
  5. 限价 BUY 成交 → settle 拿 $1 → 每股 $0.01 利润 + MAKER_REBATE
  6. 结算后 batch REDEEM
```

它的 edge 来源于：
- **Polymarket 价格 vs 真实概率的 mispricing**（如果价格说 99% 但实际 99.9% winning，每股 +0.009 expected）
- **MAKER_REBATE**（每天 ~$72 = ~$26k/year）
- **Round selection 信号**（13.4% coverage 暗示有强 entry filter）

### 11.2 不推荐复刻的理由

1. ✗ **Alpha 完全 directional**——需要预测 winner side，与我们 PGT 蓝图（双边 BUY）正交
2. ✗ **大资金门槛**：max single clip ~$11.5k，跑 ITM sniping 需要几十万 USDC margin
3. ✗ **Round selection signal**：13.4% coverage 的 87% 排除是某种盘外信号源（BTC perpetual / oracle lag / chainlink stream），我们没有这种信号
4. ✗ **MAKER_REBATE tier 门槛**：达到 maker tier 需要持续高 uptime + 大成交量，这是注册问题（先注册再跑），不是策略层问题
5. ✗ **风险结构**：单边 directional + 大 clip → 一旦 round 反转（从 0.99 反弹回 0.50），单笔损失可达 50%

但作为对照基线**有研究价值**：
- 证明 Polymarket 上有 directional alpha 来源
- MAKER_REBATE 注册流程 demystified
- ITM sniping 是一个完全不同的盈利模式

### 11.3 V1.1 蓝图修订建议（极小）

仅 1 条建议：
- **大 clip 极端价位 + REDEEM tail**：silent_d189 的「在 0.99 价位收 winner residual」这一行为，**与我们 PGT MERGE 主路径并不矛盾**——可作 **complementary tail strategy**（在 round 末 30s 检查盘口是否 ≥ 0.99，如有可顺手 sweep 一些 winner shares 利润 0.01）。但这是 V1.2+ 增量，不是 V1.1 主干。

---

## 12. 证据等级表

| 维度 | 等级 |
|------|------|
| 「BTC 5m 主导多 venue」 | A |
| 「13.4% round selector」 | A |
| 「大 clip 集中在 0.99/0.001 极端价位」 | A |
| 「80% rounds 单方向 directional」 | A |
| 「REDEEM 主导（60× MERGE）」 | A |
| 「Maker 身份（4 MAKER_REBATE）」 | **A（最强）** |
| 「Alpha 来自 ITM mispricing + maker rebate + round selection」 | B |
| 「Round selection signal 来源」 | C（未识别）|
| 「Archetype F 是新独立类」 | A（指标全维度独特）|
| 「不推荐复刻」 | A |

---

## 13. 下一步

1. **同样 deep dive c06 Candid-Closure**（multi-venue confirmed maker，最可能填补 archetype E + F 之间的中间地带）
2. **快速 survey c01/c02/c05/c08/c10**（5 个 MAYBE 候选）— 确认是否有更多新 archetype
3. **V2 第 4 轮修订**（基于 6 个 archetype 样本）：
   - 修正 §0.5 的「2 archetype」对比 → 改为 6+ archetype 谱系
   - 加 archetype F 的 alpha 路径作为对照
4. **MAKER_REBATE 注册流程研究**（Polymarket 文档）—— 既然 silent_d189 / gabagool22 / Candid-Closure 都是注册 maker，我们 PGT shadow 验证后是否值得申请？

---

## 附录 A：脚本与数据文件

```
scripts/pull_silent_d189_long_window.py        # WALLET swap from grown_cantaloupe template
scripts/analyze_silent_d189_positions.py
scripts/analyze_silent_d189_long_window.py

data/silent_d189/positions_snapshot_2026-04-27.json    # 8 rows
data/silent_d189/trades_long.json                      # 3500 rows, 45 天
data/silent_d189/activity_long.json                    # 3500 rows, including 4 MAKER_REBATE + 4 YIELD
data/silent_d189/positions_analysis.json/.txt
data/silent_d189/long_window_analysis.json/.txt
```
