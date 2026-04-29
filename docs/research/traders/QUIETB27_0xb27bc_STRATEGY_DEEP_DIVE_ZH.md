# `0xb27bc932...` 策略深度研究（Polymarket 30d profit rank 20）

生成时间：`2026-04-26`
研究对象：`0xB27BC932bf8110D8F78e55da7d5f0497a18b5b82`（无 pseudonym，社区无别名；本文称 **quietb27**）
排行榜定位：Polymarket 30d profit **rank 20**（pnl +$530,644，volume $48,575,774）
URL：`https://polymarket.com/profile/0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82`

研究动机：从 `POLYMARKET_TOP_TRADER_LEADERBOARD_PROBE_ZH.md` 发现 quietb27 是 30d 利润榜里**唯一与 xuan 同 universe（BTC 5m）的头部 trader**。预期它是 "another xuan with bigger capital"——**实证完全否定这一预期**：它是不同 archetype。

证据等级：`A`=直接确认；`B`=强支持；`C`=合理假设。

---

## 0. TL;DR

**预期** vs **实证**：

| 预期（基于「同 universe 同 BUY-only」） | 实证 |
|------|------|
| pair-gated tranche 同 xuan | **完全否定**：69% rounds 单方向交易 |
| pair_balance 紧凑 | 否定：mean imbalance 0.819，p50=1.00 |
| MERGE 频繁收水 | 否定：361h 仅 115 次 MERGE（vs xuan 14.6h pro-rated 196 次） |
| 与 xuan 同 archetype | **否定，是新 archetype D：crypto-perpetual selective directional** |

**核心定性**：

> quietb27 是 **「BTC 5m round selector + 大单 directional bettor」**——只挑 18% 的 round 进场，进场后大概率单侧 directional 押注（69% 单边），偶尔配对小段用 MERGE 清理。它的 alpha 来自 **round selection 命中 + 单边 directional 得 winner $1 redeem**，**与 xuan 的 "pair-gated maker 收水" 完全不同**。

但它仍然有几个与 xuan 共同的特征：
- ✅ Universe 完全锁死 BTC 5m（同 xuan）
- ✅ 全 BUY，0 SELL（同 xuan / late_grammar）
- ✅ 不主动 SELL，依赖 MERGE + REDEEM 收口（同 xuan）

所以 quietb27 与 xuan 同处「crypto perpetual round 双边 BUY 玩家」**大类**，但在子类型上是 "selective directional" 而非 "passive pair-gated"。

---

## 1. 数据样本与窗口

### 1.1 公开数据来源

- 排行榜定位：`https://polymarket.com/leaderboard` 内嵌 `__NEXT_DATA__`，rank 20 by 30d profit
- 数据 API：`data-api.polymarket.com/positions/trades/activity`
- 拉取时间：`2026-04-26 23:31 UTC`
- offset 翻页 cap 与 xuan/late_grammar 同：3500 trades + 3500 activity 上限

### 1.2 数据规模

- positions = **0 条**（snapshot 时刻无 open position；与 late_grammar 的 95 双侧持仓 + xuan 的 538 单侧残差**完全不同**）
- trades = `3500`，window UTC `2026-04-11 09:32:05` → `2026-04-26 10:48:56`，跨度 **361.28 h ≈ 15 天**
- activity = `3500` = `3378 TRADE` + `115 MERGE` + `7 REDEEM`

文件：
- `data/quietb27/positions_snapshot_2026-04-26.json`（空数组）
- `data/quietb27/trades_long.json`
- `data/quietb27/activity_long.json`

### 1.3 频率对照

| | xuan | late_grammar | quietb27 |
|---|------|--------------|----------|
| 窗口 | 37 h | 14.6 h | **361 h** |
| trades | 3500 | 3500 | 3500 |
| 平均 trades/h | 95 | 240 | **9.7** |

quietb27 频率比 xuan 低 **~10 倍**，比 late_grammar 低 ~25 倍。

等级：A

### 1.4 0 positions 的解读

snapshot 时刻 0 open positions。可能解释：
- (a) quietb27 把所有结算后的仓位都立即 redeem 干净（loser side 也 redeem 拿 $0）
- (b) data-api positions 端点对该 wallet 截断/缓存

无法直接验证，但**核心交易行为不依赖 positions 数据**——下面的分析全部基于 trades + activity。

---

## 2. Round 选择性（与 xuan 最大差异）

### 2.1 17.9% round coverage — 强 round selector

```text
窗口内可参与 BTC 5m round 数:    4339（361h × 12 round/h）
实际有 trade 的 round 数:           776
覆盖率:                          17.9%
```

vs xuan 94.7% 覆盖率（37 小时几乎不跳 round）。**quietb27 系统性地跳过 82% 的 round**。

这是非常强的 round-level filter。可能的过滤维度：
- BTC 价格波动（高 vol 时进，低 vol 时不进）
- 时间段（特定 UTC 小时）
- 上一 round 结果（streak / 反转信号）
- 外部 BTC perpetual market signal（CME / Binance 期货）

具体过滤规则需要 round 级元数据交叉分析，本文不展开。

等级：A

### 2.2 UTC 小时分布

| UTC | trades | UTC | trades |
|-----|--------|-----|--------|
| 00 | 116 | 12 | 90 |
| 01 | 152 | 13 | 77 |
| 02 | 85  | 14 | 131 |
| 03 | 76  | 15 | 161 |
| 04 | 105 | 16 | 166 |
| 05 | 91  | 17 | 105 |
| 06 | 80  | 18 | 134 |
| 07 | 119 | 19 | 239 |
| 08 | 67  | 20 | 133 |
| 09 | 168 | 21 | 84 |
| **10** | **550** | 22 | 119 |
| 11 | 254 | 23 | 198 |

**UTC 10 是巨峰** (550 trades，比第二名 UTC 11 高 116%)，UTC 8 是谷 (67)。**8:1 比** vs xuan 5:1 vs late_grammar ∞:1。

等级：A

### 2.3 Day-of-week 分布

```text
Mon: 618  Tue: 544  Wed: 439  Thu: 418  Fri: 109  Sat: 1103  Sun: 269
```

**Friday 是低谷**（109），**Saturday 是峰值**（1103，比第二大 Mon 高 78%）。BTC perpetual 周末波动较大，与 selective directional 策略匹配。

等级：A

---

## 3. 大单 Directional Bet（核心 alpha 路径）

### 3.1 单方向 round 占比 69%

按每个 round 的 side mix 拆桶：

```text
仅交易一侧 (pure directional):           535 / 776  =  68.9%
final |U-D|/total > 0.5 (highly biased):  623 / 776  =  80.3%
final |U-D|/total < 0.05 (well-paired):    16 / 776  =   2.1%
```

vs xuan eps=10 clean_closed_ratio = 91.3%（well-paired）。**quietb27 与 xuan 在这一指标上是镜像对立**。

```
imbalance |U-D|/total:
  median: 1.000
  mean:   0.819
```

**多数 round 完全单方向**（imbalance = 1 即只买一侧，0 是完全平衡）。

等级：A

### 3.2 大 clip 实例

12 笔成交 > 1000 股的样本：

| slug | side | size | price |
|------|------|-----:|------:|
| btc-updown-5m-1776851700 | Down | **3498** | **0.0291** |
| btc-updown-5m-1776992400 | Up | **2305** | **0.1709** |
| btc-updown-5m-1777099800 | Down | **1822** | **0.9862** |
| btc-updown-5m-1776924000 | Down | 1577 | 0.3031 |
| btc-updown-5m-1776744900 | Down | 1144 | 0.6002 |
| ... | ... | ... | ... |

vs xuan 全 clip max=600，p99=400。**quietb27 大单是 xuan 的 5–8×**。

价格 pattern 暗示策略：
- `0.029 / 0.17 / 0.30` 价位的大单 = 买**深度便宜**的某侧（押对面输）
- `0.99 / 0.60` 价位的大单 = 买**接近成熟 winner**的某侧（高把握短期反转）

这两种是不同的 directional bet 子模式：
- "Deep OTM 抄底"（赌价格不会强烈反向）
- "Deep ITM 跟趋势"（赌价格继续强势）

等级：A

### 3.3 单 round 交易序列样本

`btc-updown-5m-1776180300` 前 14 笔：

```text
ts=1776180403  BUY Up   sz=136.5  px=0.5000
ts=1776180417  BUY Up   sz=127.7  px=0.5900   # 同侧叠加
ts=1776180503  BUY Down sz=29.2   px=0.6400   # 反向开始
ts=1776180503  BUY Down sz=43.7   px=0.6378
ts=1776180503  BUY Down sz=101.9  px=0.6252
ts=1776180503  BUY Down sz=48.7   px=0.6270
ts=1776180503  BUY Down sz=179.1  px=0.6152
ts=1776180505  BUY Down sz=108.5  px=0.6547
ts=1776180505  BUY Down sz=50.5   px=0.6500   # 同侧 6 笔密集
ts=1776180529  BUY Up   sz=447.8  px=0.3600   # 大反向加仓
ts=1776180531  BUY Down sz=195.4  px=0.6800
ts=1776180531  BUY Down sz=75.3   px=0.6600
ts=1776180537  BUY Down sz=192.1  px=0.7100
ts=1776180537  BUY Down sz=2.5    px=0.7100
```

特征：
- **同侧密集叠加**：第 3–9 笔全部 Down，run length = 7
- **大反向加仓**：第 10 笔单笔 447 股反向（前面累计才 561 股 Down，这一笔 Up 把净差立刻翻盘）
- **Same-second 多笔成交**：`1776180503` 一秒内成交 5 笔——可能是市价单 sweep 多个 maker price level

整体形态：**激进 directional bet + 中途大幅调整方向 + 短时冲量**。

vs xuan 的「U-D-U-D 高度交替小 clip」完全相反。

等级：A

### 3.4 Run length 分布

```text
n_runs=1314  p10=1  p50=2  p90=5  max=31
histogram: {1: 529, 2: 300, 3: 187, 4: 100, 5: 68, 6: 42, 7: 31, 8: 20, ...}
```

vs xuan max=4，p90=2。**quietb27 max run = 31** —— 31 笔同侧连击。

40% 的 run 长度 ≥ 2，14% ≥ 3。**与 xuan 81% run=1 完全相反**。

等级：A

---

## 4. Episode 重建（与 xuan 极端反差）

```text
                     eps=10                     eps=25
opened               783                        767
closed                45                         86
clean_closed          17                         36
clean_closed_ratio   0.0217 (2.17%)             0.0469 (4.69%)
same_side_add_ratio  0.4454 (44.54%)            0.4288 (42.88%)
close_delay_p50      42s                         42s
close_delay_p90      186s                        138s
```

vs xuan: clean_ratio 91.3% / same_side_add 10.5%。

**quietb27 的 episode 几乎从不 clean 闭合**——开了 783 个 episode 只 closed 45 个（5.7%）。这意味着大多数情况下进入 active leg 后**继续同侧加仓**，最终不是闭合而是 round 结束（结算）。

这与 xuan 的 pair-gated 状态机是完全相反的策略。

等级：A

---

## 5. Clip size 分布

```text
n=3500  p10=10.0  p50=59.6  p90=257.6  p99=675.5  max=9392.3 (shares)
```

注意 max=9392 — **比 xuan max=600 大 15 倍**！这是 outlier，但反映 quietb27 偶尔会 fire massive directional bet（e.g. 一笔 9k 股 = ~$9k notional）。

按 intra-round trade index：

```text
trade #1 : p50=56.6   p90=281.6
trade #5 : p50=51.4   p90=217.6
trade #10: p50=70.1   p90=231.9
```

intra-round 序号上**没有 xuan 那种单调下降**——这是因为 quietb27 不是 inventory-aware maker。

按 prior imbalance：

```text
imb 0.00-0.05  p50=78.8
imb 0.30-1.00  p50=59.6
```

p50 **反而下降**（与 xuan 升档相反）——再次确认非 pair-gated。

等级：A

---

## 6. MERGE / REDEEM 节律（量级远低于 xuan）

```text
MERGE 数:   115  /  361h  =  0.32 次/h
REDEEM 数:    7  /  361h  =  0.02 次/h
MERGE total:  $32,406  shares
REDEEM total: $186      shares
```

vs xuan 14.6h pro-rated: MERGE 196, REDEEM 125。**quietb27 MERGE 频率仅 xuan 的 1/15**。

```text
MERGE offset (s vs round open): n=115  p10=56  p50=172  p90=286
REDEEM offset (s vs round open): n=7  p10=490  p50=516  p90=520
MERGE after round close:  9/115  (在收盘后 merge 极少)
REDEEM after round close: 7/7    (与 xuan 同：全部收盘后 redeem)
```

MERGE 时点比 xuan 早（172s vs xuan 278s）—— quietb27 的 MERGE 像是处理 directional bet 之前的"少量配对部分"，而不是收盘前的最后一搏。

REDEEM 仅 7 次但 quietb27 在 776 个 round 内交易过——这意味着**绝大多数 round 没有显式 REDEEM event**。两个可能：
- (a) 自动结算 + 即时 settle 机制（不需要主动 redeem）
- (b) data-api activity 端点截断（不太可能因为 MERGE 也很少且窗口仅 15 天）

等级：A（数据），B（机制解读）

---

## 7. Inter-fill delay

```text
all  : p10=0  p50=2   p90=40
cross: p10=2  p50=12  p90=116
same : p10=0  p50=0   p90=18
```

**Same-side p50=0s** ——同侧 50% 的相邻成交在同秒发生。

vs xuan same-side p50=10s。**quietb27 同侧极速连发**——典型 sweep 多 maker level 的 taker 行为。

cross-side p50=12s（与 xuan 18s 接近，因为跨侧需要 round 内信号变化）。

Same-second cross-side ratio = 1.37%（vs xuan 5.49%）—— quietb27 几乎不双边同时成交，**几乎肯定不预埋 ladder**。

等级：A

---

## 8. Maker vs Taker 路径裁决

公开数据无法直接区分（与 xuan 同），但 quietb27 的间接证据**明确指向 TAKER**：

1. Same-side p50=0s（**同侧极速 sweep**）—— 典型 IOC / FAK 吃 multiple levels
2. Same-second 多笔成交（如 §3.3 sample 的 1776180503 一秒 5 笔）—— sweep 行为
3. 大 clip max=9392 股 / p99=675 股 —— maker 不挂这种规模
4. 高 same_side_add 44.5% —— maker 不主动加同侧（会增加方向暴露）
5. 0 SELL events —— 不是经典 spread maker（不出后期对冲）

**结论：quietb27 几乎肯定是 TAKER-driven directional bettor**。等级：B（强支持）。

vs xuan 是 maker-driven（V2 §8 等级 B）—— 又一处镜像差异。

---

## 9. 与 xuan + late_grammar 横向对比

| 维度 | xuan | late_grammar | quietb27 | quietb27 vs xuan |
|------|------|--------------|----------|------|
| Universe | BTC 5m | 6 venues | BTC 5m | 同 |
| 窗口 | 37h | 14.6h | 361h | quietb27 长 10× |
| 频率 (trades/h) | 95 | 240 | 9.7 | quietb27 慢 10× |
| Round coverage | 94.7% | 98.3% | **17.9%** | quietb27 1/5 |
| 双侧持仓率 | 0.2% | 88.8% | 0% | quietb27 = 0 |
| Side mix | BUY only | BUY only | BUY only | 同 |
| MERGE / 14.6h pro-rated | 196 | 0 | **4.6** | quietb27 1/40 |
| REDEEM / 14.6h pro-rated | 125 | 451 | 0.3 | quietb27 极少 |
| clean_closed_ratio | 91.3% | 33-87% (venue-dep) | **2.17%** | quietb27 1/40 |
| same_side_add_ratio | 10.5% | 4-39% | **44.5%** | quietb27 4× |
| run length max | 4 | 8 | **31** | quietb27 8× |
| pure directional rounds % | ~0% | unknown | **68.9%** | quietb27 dominant |
| inter-fill same p50 | 10s | 2s | 0s | quietb27 极速 |
| Clip max (shares) | 600 | 633 | **9392** | quietb27 15× |
| Maker/Taker (推断) | Maker (B) | Maker (B) | **Taker (B)** | 反转 |

**结论**：xuan / late_grammar / quietb27 三者**只在「BTC universe + BUY only」表层共享**，**底层执行几何完全不同**：

- xuan = passive maker, every round, tight pair-gated, frequent MERGE
- late_grammar = passive maker, multi-venue spread, no MERGE
- quietb27 = aggressive taker, **selective rounds**, directional bets, sparse MERGE

---

## 10. Archetype 归类与对 PGT 蓝图的含义

### 10.1 quietb27 是 "Archetype D：Crypto-perpetual selective directional bettor"

新增 archetype（之前文档只识别到 A/B/C）：

- **Archetype A**：体育 directional（rank01-02-04-05-06）
- **Archetype B**：政治 directional with active SELL（rank03 tripping）
- **Archetype C**：crypto-perpetual passive maker（xuan / late_grammar）
- **Archetype D（新）**：**crypto-perpetual selective directional taker**（quietb27）

D 与 C 的本质区别：
- C 不挑 round，每 round 都进，靠 maker spread + MERGE 收水
- D 强挑 round（17.9%），进了就 directional，靠 round selection 命中

### 10.2 quietb27 不是 PGT 蓝图的复刻目标

理由同 late_grammar：
1. 它的 alpha 来自 **round selection signal**，我们无此信号源
2. 它是 taker 大单 directional，与 PGT 「pair-gated maker」蓝图正交
3. 0 positions 的「干净退出」机制依赖 directional 命中率，我们无法实现

但**它给 V2 文档带来 2 条修订建议**：

#### 修订 1：xuan 的「BTC 5m only」专精原因再次被强化

V2 §0.5 修正 3 已经指出 xuan 的 BTC 5m 专精暗示执行栈优势。quietb27 的存在进一步证明：**BTC 5m 是 crypto-perpetual segment 里被多种 archetype 共享的"主战场"**——maker (xuan) 和 taker (quietb27) 都在这里赚钱，但用完全不同的几何。

#### 修订 2：xuan 的 round coverage 94.7% 是 archetype 选择，不是「填空白」

V2 §3.1 把 xuan 的 5% round-skip 解释为 selective signal（弱）。quietb27 的 17.9% coverage 让 xuan 的 94.7% 看起来更像 "passive every-round" 选择——**xuan 选 every round 是 archetype C maker 的特征**，selective 是 archetype D 的特征。两者不是「弱 vs 强 selective」，是两种 archetype。

V2 §3.1 等级保持 A，但解读应改为：「xuan 选择 every-round = archetype C 的特征属性，而非弱 selective filter」。

### 10.3 复刻判断：**不推荐**

理由：
1. ✗ Round selection signal 缺失：quietb27 的 17.9% 命中是某种盘外信息源（BTC 期货 / vol regime / streak），我们没有
2. ✗ 大单 taker 执行：需要 IOC / FAK + 充足 USDC margin，我们当前 PGT 蓝图是 maker post-only
3. ✗ 0 positions 的干净 exit 机制：依赖 directional 命中率，不可移植
4. ✓ 但作为「同 universe 不同 archetype」的对照基线**有研究价值**

---

## 11. 证据等级表

| 维度 | 等级 |
|------|------|
| 「同 BTC 5m 同 BUY-only」表层共享 | A |
| 「不是 xuan 同类 archetype」 | A（核心指标全反转）|
| 「Round selector，~18% coverage」 | A |
| 「多数 round 单方向 directional」 | A |
| 「Taker driven 而非 maker」 | B（间接证据强）|
| 「Round selection signal 来源」 | C（未识别）|
| 「不推荐复刻」 | B |

---

## 12. 下一步

1. **Round-level 元数据交叉**（高价值）：取 quietb27 进/不进的 BTC 5m round，对比 round-open 时刻的：
   - 上一 round 结果（Up / Down / 接近 50/50）
   - BTC 实时价格波动率（CME / Binance perpetual）
   - 同时刻多 venue（ETH 5m / 15m）的 quietb27 缺位情况
   找出 selection signal —— 可能反推出 quietb27 的判断函数

2. **probe_clob_trades on quietb27**：与 V2 §8 同样不可行（authenticated CLOB scope 限制）

3. **Recorder book-snapshot timing match**：用 V2 §8.2 alt 路径 #1，比对 quietb27 trade.price 与最近 best ask 来确认 maker/taker 推断

4. **加入 4-archetype 综合对比文档**：xuan + late_grammar + quietb27 + (TBD gabagool22) → archetype 谱系图

---

## 附录 A：脚本与数据文件

```
scripts/pull_quietb27_long_window.py        # WALLET swap from xuan/late_grammar template
scripts/analyze_quietb27_positions.py
scripts/analyze_quietb27_long_window.py

data/quietb27/positions_snapshot_2026-04-26.json    # 0 rows
data/quietb27/trades_long.json                      # 3500 rows, 361h span
data/quietb27/activity_long.json                    # 3500 rows
data/quietb27/positions_analysis.json / .txt        # mostly NaN due to 0 positions
data/quietb27/long_window_analysis.json / .txt      # core findings
data/quietb27/pull_summary.json / pull_log.txt
```
