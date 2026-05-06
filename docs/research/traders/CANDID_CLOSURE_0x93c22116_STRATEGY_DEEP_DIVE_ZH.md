# `Candid-Closure` (0x93c22116...) 策略深度研究

生成时间：`2026-04-27`
研究对象：钱包 `0x93c22116e4402c9332ee6db578050e688934c072`
Pseudonym：**Candid-Closure**
本文 codename：**candid_closure**

研究动机：用户提供的 10 个候选钱包之一。Quick probe 确认它**有 MAKER_REBATE event**（注册 maker）+ multi-venue + 几乎纯 BUY（0.03% SELL）。最有可能是 gabagool22 同类的 multi-venue maker。

证据等级：`A`/`B`/`C` 同前。

---

## 0. TL;DR

**candid_closure 是 archetype E'（Archetype E 的 longer-tenor variant）**：

> **multi-venue maker，15m/4h/daily 主导，跳过 5m**——与 gabagool22 (archetype E) 同为「多 venue confirmed maker」，但**tenor 偏好显著更长**：gabagool22 主跑 5m + 15m + daily；candid_closure 跑 **15m + 4h + daily（无 5m）**。这意味着 candid_closure 是更慢节奏的版本——可能更适合执行栈不那么紧的 maker（不需要争 5m round 末段的几秒）。

核心数据：
- **281 single-side residuals**, **all loser-side (281/281 cur_price=0)** — 与 xuan 的 538/0 完全相同 ✓
- merged_ratio p50 = **81.6%**（高，但不像 xuan 98.5% 那么极端）
- **MERGE 70 + REDEEM 140 + MAKER_REBATE 1 + 0 SELL（实际 1/3500=0.03%）**
- MAKER_REBATE event $326（**单次 rebate 比 silent_d189 $72/day 大 4×**——可能是周/月结算）
- Universe: btc/eth/sol/xrp × **15m/4h/daily** + ethereum-4h（**无 5m**）
- Multi-venue 极广：12 个不同 prefix
- Big clips at 中等价位（0.47, 0.50, 0.59, 0.92）— 不是 silent_d189 的极端价位

```
Archetype E' (candid_closure):
  - multi-venue, 12 prefix
  - tenor: 15m / 4h / daily (NO 5m!)
  - confirmed maker (MAKER_REBATE)
  - BUY only (0.03% SELL noise)
  - post-close MERGE (52/52)
  - loser-only residual (281/281)
  - merged_ratio 81.6%（中等高）
```

vs gabagool22 (E)：核心机制相同，差异在 tenor 选择 + clip 严格度。

---

## 1. 数据样本与窗口

### 1.1 数据规模

- 拉取时间：`2026-04-27 06:51 UTC`
- positions = **281 条**（最多的一个研究对象，全部 single-side 残差）
- trades = `3500`，window UTC `2026-02-01 03:41` → `2026-02-01 17:20`，跨度 **13.64 h（单日 burst）**
- activity = `3500` = `3289 TRADE` + `140 REDEEM` + `70 MERGE` + **`1 MAKER_REBATE` ⭐**

### 1.2 时间分布特征

```text
weekday: 100% Sunday (Feb 1, 2026)
hour: UTC 03–13 集中（138-579 trades/h），UTC 14-23 + 00-02 几乎全空
peak: UTC 04 (579 trades), UTC 07 (470)
```

13.6 小时单日 burst，类似 gabagool22 的 9 小时 burst pattern。但 candid_closure 数据来自 2026-02-01（约 2.5 个月前）。

### 1.3 文件

```
data/candid_closure/positions_snapshot_2026-04-27.json    # 281 rows
data/candid_closure/trades_long.json                      # 3500 rows
data/candid_closure/activity_long.json                    # 3500 rows
data/candid_closure/positions_analysis.json/.txt
data/candid_closure/long_window_analysis.json/.txt
```

---

## 2. Universe — 12 个 venue 跳过 5m

### 2.1 trades 分布

| Universe | trades | rounds | t/round |
|----------|-------:|-------:|--------:|
| eth-updown-15m | 949 | 41 | 23.15 |
| btc-updown-15m | 565 | 40 | 14.12 |
| sol-updown-15m | 395 | 42 | 9.40 |
| **bitcoin-daily** | 381 | 11 | **34.64** |
| ethereum-daily | 316 | 11 | 28.73 |
| btc-updown-4h | 172 | 4 | 43.00 |
| eth-updown-4h | 158 | 4 | 39.50 |
| solana-daily | 149 | 10 | 14.90 |
| xrp-updown-15m | 144 | 39 | 3.69 |
| xrp-updown-4h | 105 | 4 | 26.25 |
| sol-updown-4h | 92 | 4 | 23.00 |
| xrp (daily) | 74 | 10 | 7.40 |

**12 个 universe 同时跑**，比 gabagool22 (6 venues) 多 1 倍。

**关键差异 vs gabagool22**：candid_closure **完全不做 5m**（btc-updown-5m=0, eth-updown-5m=0），而是包含 **4h tenor**（btc/eth/sol/xrp × 4h，每个 4 个 round）。

trades/round 中位 ~14-23，**比 xuan 的 10 高、与 gabagool22 11.7 接近、比 silent_d189 1.8 高得多**。

### 2.2 positions 分布

```text
positions by universe (281 single-side residuals):
  btc-updown-15m: 61
  eth-updown-15m: 46
  sol-updown-15m: 37
  xrp-updown-15m: 36
  solana-daily:   27
  ethereum-daily: 25
  bitcoin-daily:  23
  xrp:            19
  btc-updown-4h:   3
  xrp-updown-4h:   3
  sol-updown-4h:   1
```

15m 占大头（180/281 = 64%），daily 占 25%（71/281），4h 仅 7（2.5%）。

等级：A

---

## 3. Episode & directional behavior

### 3.1 Per-round 方向性

```text
imbalance |U-D|/total median=0.349  mean=0.457
rounds with imb > 0.5: 81 / 220 (36.8%)
rounds with imb < 0.05: 14 / 220 (6.4%)
pure directional rounds: 38 / 220 (17.3%)
```

**中等方向性**——比 xuan ~0% 高得多，但比 quietb27 69% / silent_d189 80% 低得多。这与 maker 几何一致：双边都买，但实际成交不平衡。

### 3.2 Episode reconstruction

```text
                     eps=10                  eps=25
opened               290                     313
closed               75                      118
clean_closed         32                      44
clean_closed_ratio   11.03%                  14.06%
same_side_add_ratio  44.92%                  43.77%
close_delay_p50      336s (5.6 min)          298s
close_delay_p90      2344s (39 min)          2684s
```

clean_closed_ratio **11%**——明显低于 xuan 91% / gabagool22 39% / silent_d189 4.5%。

但 close_delay 极长：p50=5.6 min, p90=39 min——这与 15m/4h/daily 的长 round 周期一致（5m round 不可能有 39 min close delay，daily round 才有空间）。

**用 round-tenor-adjusted close_delay 来比对更公平**——如果 close_delay / round_total < 10%，仍是合理的。

### 3.3 Run length

```text
n_runs=1087  p10=1  p50=2  p90=7  max=22
histogram: {1:381, 2:234, 3:144, 4:91, 5:65, 6:47, 7:32, 8:23, ...}
```

max=22 → 22 笔同侧连击。**比 gabagool22 max=12 长，比 quietb27 max=31 短**。

p50=2（不是 1）—— 同侧叠加是常态，与 xuan 81% run=1 截然不同。

等级：A

---

## 4. Clip size — 中等弹性

```text
n=3500  p10=9.0  p50=36.8  p90=180.0  p99=744.2  max=1426
```

vs xuan p50=132 / late_grammar 10 / quietb27 60 / gabagool22 9.9 / silent_d189 199。

**candid_closure 中位 36.8 介于 gabagool22 (10) 和 xuan (132) 之间**——中等弹性。

### 4.1 大 clip 价位（与 silent_d189 形成对比）

83 笔 > 500 股的大 clip 中，top 10：

| slug | side | size | price |
|------|------|-----:|------:|
| btc-updown-15m-1769942700 | Up | 1426 | **0.59** |
| btc-updown-15m-1769928300 | Down | 1419 | **0.51** |
| btc-updown-15m-1769940000 | Up | 1378 | **0.50** |
| btc-updown-15m-1769938200 | Up | 1356 | **0.47** |
| btc-updown-15m-1769929200 | Down | 1315 | **0.50** |
| btc-updown-15m-1769917500 | Down | 1190 | **0.0099** |
| btc-updown-15m-1769933700 | Up | 1173 | **0.58** |
| btc-updown-15m-1769940000 | Up | 1172 | **0.50** |
| btc-updown-15m-1769933700 | Down | 1166 | **0.92** |
| bitcoin-up-or-down-february-1-8am-et | Up | 1140 | **0.72** |

**集中在 0.50 中位价**（10 个中 6 个在 0.47-0.59 区间）——与 silent_d189 的 0.99/0.001 极端价位**完全不同**。

candid_closure 是 **mid-price maker**——在公平中位价附近双边挂单收 spread，不是 ITM sniping。

等级：A

### 4.2 By intra-round / imbalance / time

```text
trade #1 p50=36.5 → trade #10 p50=31.0  (轻微下降，类似 xuan)
imb 0.00-0.05 p50=50.0 → imb 0.30+ p50=35.8  (反向：imb 大时 clip 反而小)
```

intra-round 索引轻微下降，与 xuan 的 inventory-aware 类似。但 imbalance 维度反向（imb 大 clip 小），与 xuan 反向。这表明 candid_closure 在 imb 大时**不加速 completion**（不像 xuan），而是减速。

可能机制：当 imbalance 大时（已有大单暴露），candid_closure 倾向**降低参与度避免方向风险**——这是被动 maker 的保守行为。

等级：B（推断）

---

## 5. MERGE / REDEEM 节律

```text
MERGE 数量: 70   total: $180,345
REDEEM 数量: 140  total: $40,906
ratio: 4.4:1   (MERGE 主导，与 xuan 5.2:1 接近)
```

vs xuan 5.2:1 / gabagool22 12:1 / silent_d189 0.017:1。candid_closure 的 4.4:1 接近 xuan 的 5.2:1——**MERGE 是主回收路径**。

### 5.1 MERGE 时机

```text
MERGE offset (s vs round open):
  p10=4718    p50=17318    p90=27218
MERGE all post-close: 52/52 (按 5m round 计算 close=300s，所以全部 post-close)
```

但要注意 candid_closure 不做 5m，做 15m/4h/daily。考虑这些 tenor：
- 15m round close=900s → MERGE p50=17318s = **round close 后约 4.6 小时**
- 4h round close=14400s → MERGE p50 = **round close 后约 3.6 分钟**
- daily round close=86400s → MERGE p50 在 round 内（开始后 ~5 小时，距 close 还有 19 小时）

按 tenor 拆桶 MERGE / REDEEM：

| Universe | MERGE | REDEEM |
|----------|------:|-------:|
| btc-updown-15m | 22 | 16 |
| eth-updown-15m | 19 | 24 |
| bitcoin-daily | 8 | 6 |
| ethereum-daily | 6 | 5 |
| solana-daily | 4 | 6 |

主要 MERGE 集中在 15m + daily venue。

等级：A

### 5.2 与 gabagool22 对比

| | gabagool22 | candid_closure |
|---|------------|----------------|
| MERGE timing | 5m/15m post-close batch | 15m post-close batch + daily mid-round |
| Tenor focus | 5m + 15m + daily | 15m + 4h + daily（**无 5m**）|
| MERGE/REDEEM ratio | 12:1 | 4.4:1 |

candid_closure 的 MERGE/REDEEM 比 gabagool22 低（4.4:1 vs 12:1），意味着 candid_closure **更多依赖 REDEEM**——这与「跳过 5m」一致：长 tenor round 内成对机会少，更多 round 自然变成单边 directional → 等结算 redeem。

---

## 6. Maker / Taker 路径裁决（A 级）

### 6.1 MAKER_REBATE event 1 次

```text
ts=1769990842 (2026-02-02 02:07 UTC)
usdcSize=$326.42
```

**单次 rebate $326，比 silent_d189 $72/day 大 4×**——可能是 batch settlement（周/月汇总），意味着 candid_closure 跨多日累积 maker 成交。

**等级：A（直接 on-chain 证据）**

### 6.2 间接证据

- 0.03% SELL（1/3500）— 几乎纯 BUY
- 281 single-side residuals 全 loser-only（与 xuan 同模式）
- 大 clip 在中位价（不是 IOC sweep 极端价）
- inter-fill same p50=6s（中等连击速度）

---

## 7. Single-side residual 281/0 — 与 xuan 同模式

### 7.1 全部 loser-side

```text
281 single-side residuals:
  cur_price=0 (loser): 281 (100%)
  cur_price=1 (winner): 0
```

**与 xuan 538/0 完全一致**——证明 multi-venue maker 在长 tenor 上同样呈现「loser-only 残差」稳态。

机理（与 V2 §0.5 修正 5b 同）：
- 双边 BUY，winner side 自然 redeem（结算后 $1 自动到账）
- Loser side 价值 0，不值 gas redeem，留在钱包

### 7.2 merged_ratio 81.6%

```text
residual size       p10=24.9  p50=356.2  p90=2111.3
total_bought        p10=282.1  p50=2281.9  p90=9236.4
merged_ratio        p10=0.392  p50=0.816  p90=0.972
```

merged_ratio p50 = 81.6%——意思是平均每 round 把 81.6% 的 totalBought MERGE 掉，剩 18.4% 作为残差。

vs xuan 98.5% / late_grammar 2.4% / gabagool22 unknown (0 positions) / silent_d189 0% (单边持仓没有 pair)。

candid_closure 介于 xuan 和 late_grammar 之间——**配对效率不如 xuan，但远比 late_grammar 主动 MERGE**。

等级：A

---

## 8. 与所有 5 个 archetype 横向对比

| 维度 | xuan (C) | late_grammar (C') | quietb27 (D) | gabagool22 (E) | silent_d189 (F) | **candid_closure (E')** |
|------|------|------|------|------|------|------|
| Universe | BTC 5m | 6 mixed | BTC 5m | btc/eth × 5m/15m/daily | BTC 5m + nba | **btc/eth/sol/xrp × 15m/4h/daily（无 5m）** |
| 主 tenor | 5m | 5m + 15m + daily | 5m | 5m + 15m + daily | 5m | **15m + 4h + daily** |
| Venue 数 | 1 | 6 | 1 | 6 | mixed | **12** |
| 平均 trades/h | 95 | 240 | 9.7 | 389 | 3.25 | 257 |
| Round coverage | 95% | 98% | 18% | 90% | 13% | n/a (multi-tenor 不直接可比) |
| Clip p50 | 132 | 10 | 60 | 9.9 | 199 | **36.8** |
| Clip max | 600 | 633 | 9392 | 10 | 115k | **1426** |
| 大 clip 价位 | 50/50 | 50/50 | 中等 | 50/50 | **0.99 / 0.001** | **50/50（中位）** |
| MERGE 数 (pro-rated 14h) | 196 | 0 | 4.6 | 102 | 0.26 | **75** |
| MERGE/REDEEM 比 | 5.2:1 | 0:∞ | 17:1 | 12:1 | 1:60 | **4.4:1** |
| Pure directional rounds % | ~0% | ~ | 69% | 13% | 80% | **17%** |
| Single-side residual 偏好 | loser-only | winner-heavier | n/a | n/a | **winner-only (5/0)** | **loser-only (281/0)** |
| merged_ratio p50 | 98.5% | 2.4% | n/a | n/a | 0% | **81.6%** |
| Maker/Taker | Maker (B) | Maker (B) | Taker (B) | **Maker (A)** | **Maker (A)** | **Maker (A)** |
| MAKER_REBATE 次数 | 0 | 0 | 0 | 1 | 4 | **1（但 $326 单次）** |

---

## 9. Archetype 归类与对 PGT 蓝图的含义

### 9.1 candid_closure = Archetype E'（E 的 long-tenor variant）

candid_closure 与 gabagool22 (E) 共享：
- ✓ Multi-venue maker
- ✓ Confirmed registered maker (MAKER_REBATE)
- ✓ BUY-only
- ✓ Post-close MERGE 主导

差异：
- ✗ candid_closure **完全不做 5m**，最短 tenor 是 15m
- ✗ candid_closure 包含 4h tenor（gabagool22 没）
- ✗ candid_closure 12 venue（gabagool22 6）
- ✗ candid_closure clip p50=37（gabagool22 严格 10）

**candid_closure 是 archetype E 的「longer-tenor + wider-venue + larger-clip」variant**，归类为 **E'**。

### 9.2 复刻判断：**部分推荐**（与 gabagool22 同级别）

理由（与 gabagool22 §9.3 相同）：
- ✓ Confirmed maker，策略路径与 PGT 同向
- ✓ Loser-only residual + 81.6% merged_ratio 与 xuan 同模式但适用于长 tenor
- ✓ Tenor-adaptive MERGE 思路与 gabagool22 一致
- ✗ Multi-venue 12 个需要 cross-venue infrastructure
- ✗ 4h tenor 在 V1.1 蓝图（5m）外

### 9.3 V1.1 蓝图增量建议

candid_closure 给出**比 gabagool22 更明确的「跳过 5m」信号**——值得思考：

> **是否 5m 太难做？** 一个能拿到 maker rebate（注册 maker tier）的 trader，主动跳过 5m 而专做 15m/4h/daily——意味着 5m 在被注册 maker 的「成本/收益」框架下**不够好**。可能因为：
> 1. 5m round 末段竞争激烈（多 maker 抢 fill），rebate 不够补 spread 损失
> 2. 5m round 太短，inventory carry risk 转化为损失的速度太快
> 3. Long tenor (15m+) 给 maker 更多机会让双边自然成对

**含义**：xuan 之所以能做 5m 且 91% clean ratio，是因为它**未注册 maker**（无 rebate 但也无 maker tier 责任），可以做 archetype C 这种紧密 5m pair-gated。注册 maker 反而被 tier 制约不愿意做 5m。

这是个有意思的 inversion：**注册 maker 的优化目标 ≠ 高 clean_ratio**。注册 maker 优化的是 **rebate 收益 - inventory carry 损失** 的差，而 clean_ratio 高低不直接进 rebate 公式。

PGT 蓝图启示：
- 如果我们**不打算注册 maker**，xuan 的 5m 紧密 pair-gated 是最优
- 如果我们**打算注册 maker**，应学 candid_closure 跳过 5m，做 15m/4h/daily 更划算

---

## 10. 证据等级表

| 维度 | 等级 |
|------|------|
| 「12 venue multi-asset multi-tenor」 | A |
| 「跳过 5m，最短 tenor 15m」 | A |
| 「Maker 身份」 | **A（MAKER_REBATE event）** |
| 「loser-only residual」 | A |
| 「post-close MERGE 主导」 | A |
| 「中等价位大 clip」 | A |
| 「Archetype E'（E 的 long-tenor variant）」 | A |
| 「注册 maker 主动跳过 5m 是 rebate 优化结果」 | B（推断，待验证） |

---

## 11. 下一步

1. **快速 survey 5 个 MAYBE 候选**（c01/c02/c05/c08/c10）—— 一份合订 quick-survey doc
2. **V2 第 4 轮修订**（基于 6 个 archetype 样本）：
   - 修正 §0.5 archetype 谱系（C/C'/D/E/F + 新加 E'）
   - 新增结论：「**注册 maker 与未注册 maker 在 tenor 选择上系统性不同**」
3. **Polymarket maker rebate 注册流程研究**（API / 文档）—— 决定 PGT shadow 后是否值得申请

---

## 附录 A：脚本与数据文件

```
scripts/pull_candid_closure_long_window.py        # WALLET swap from silent_d189 template
scripts/analyze_candid_closure_positions.py
scripts/analyze_candid_closure_long_window.py

data/candid_closure/positions_snapshot_2026-04-27.json    # 281 rows (loser-only)
data/candid_closure/trades_long.json                      # 3500 rows, 13.6h burst
data/candid_closure/activity_long.json                    # 3500 rows, 1 MAKER_REBATE
data/candid_closure/positions_analysis.json/.txt
data/candid_closure/long_window_analysis.json/.txt
```
