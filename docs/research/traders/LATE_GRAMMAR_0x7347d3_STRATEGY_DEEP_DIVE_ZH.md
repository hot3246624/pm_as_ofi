# `0x7347d3` (Late-Grammar) 策略深度研究

生成时间：`2026-04-26`
研究对象：`0x7347d3` / `Late-Grammar`
公开钱包：`0x7347d3291b321d2ee8d43d24aff244e7fb8c3d35`
URL：`https://polymarket.com/@0x7347d3?tab=positions`

本文与 `XUANXUAN008_STRATEGY_V2_ZH.md` 平行结构，但研究对象是另一类双边 BUY 策略。**§9 专门做 xuan vs late_grammar 横向对比**——这是本研究的核心增量。

证据等级沿用：`A`=公开数据直接确认；`B`=强支持但仍有替代解释；`C`=仅合理假设。

---

## 0. TL;DR

> Late-Grammar 是一台**多 venue 平铺、被动小 clip、几乎不 MERGE、慢批量 REDEEM** 的双边 BUY 自动机。它**不是 xuan 的同类**——而是与 xuan **几乎完全相反**的策略形态：
>
> - xuan 单 venue (BTC 5m) 紧密 pair-gated，merged_ratio p50=98.5%，winner 残差 = 0；
> - late_grammar **6 个 venue 并发**（BTC/ETH/XRP × 5m/15m + bitcoin daily），**14.6 小时窗口里 0 次 MERGE**，winner-side 残差中位 +9.94 股（73 winner-heavier vs 14 loser-heavier）。
>
> **核心 alpha 来源（推断）**：双边小 clip BUY 在流动性高的 BTC/ETH 市场被**自然 order flow 拉向 winner side**——maker 被动暴露随价格走势倾斜——结算时 winner side overweight × $1 redeem 抵消 pair_cost > 1 的损失。XRP 上这种偏差消失（3 winner / 6 loser），说明此 edge 与市场流动性结构强相关。
>
> **复刻判断**：**不推荐复刻**。alpha 量级偏小（14.6h cashPnl +$349 on $15k base），依赖市场结构性偏差不易移植，且策略路径与我们 V1.1 PGT 蓝图正交。但**保留作为对照基线**有研究价值。

与 xuan 的核心反差速览：

| 维度 | xuan | late_grammar | 反差 |
|------|------|--------------|------|
| Universe 数 | 1 (BTC 5m) | **6** (BTC/ETH/XRP × 5m/15m + bitcoin-daily) | 大 |
| 双侧同持率 | 0.2% | **88.8%** | 反转 |
| MERGE 数 (14.6h 窗口) | ~497 (37h pro-rated ≈ 196 in 14.6h) | **0** | 反转 |
| merged_ratio p50 (单侧残差) | 98.5% | **2.4%** | 反转 |
| Winner-side residual 倾向 | 0 winner / 538 loser | **73 winner / 14 loser**（resolved 87） | 反转 |
| pair_cost p50 | 0.99 | 1.02 | 略升 |
| clip p50 | 131.7 | **10.4** (但 BTC 5m=40.5) | 小 12× |
| inter-fill cross p50 | 18s | **36s** | 慢 2× |
| REDEEM offset p50 | +38s post-close | **+7864s ≈ +2.18h** | 慢 200× |
| realizedPnl 累计 | -$23,408 | **$0** | 不同口径 |
| totalBought 累计 | $262,422 | **$15,496** | 小 17× |

---

## 1. 数据样本与窗口

### 1.1 公开数据来源

- 端点：`data-api.polymarket.com/positions/trades/activity?user=<wallet>`
- 拉取时间：`2026-04-26 10:07 UTC`
- offset 翻页 cap：与 xuan 相同，3500 条上限
- `before=<min_ts>` 时间游标确认无新数据返回——data-api 极限 ≈ 3500
- 数据：
  - positions = `202` 条（覆盖 `107` unique conditionId）
  - trades = `3500`，window UTC `2026-04-25 19:25:12` → `2026-04-26 10:00:58`，跨度 `14.60 h`
  - activity = `3500` = `3035 TRADE` + `465 REDEEM` + **`0 MERGE`**

文件：
- `data/late_grammar/positions_snapshot_2026-04-26.json`
- `data/late_grammar/trades_long.json`
- `data/late_grammar/activity_long.json`
- `data/late_grammar/pull_summary.json`

### 1.2 认证 CLOB probe（与 xuan 同结论）

`/data/trades` 服务端按认证用户 scope；用我方 L2 凭证查 `maker_address=0x7347d3...` 仍只能返回我们自己账户内的成交。**maker/taker 维持 Level-C，与 xuan 同**。

### 1.3 时间频率对照

late_grammar 14.6h 拉到 3500 trades = **240 trades/h**；xuan 37h 拉 3500 trades = **95 trades/h**。late_grammar 平均速率约 **2.5×** xuan。

等级：A

---

## 2. 持仓快照画像

### 2.1 双侧同持是常态（90% 的 condition）

```text
sides_per_cond:    1side=12   2sides=95
both sides held :  95
only Up         :   8
only Down       :   4
```

**88.8% 的 condition 同时持有 UP+DOWN**——与 xuan 0.2% 形成镜像。

更进一步：12 个单侧条件中，**10 个是 winner-side 残差（cur_price=1）**，1 个 loser，1 个 active 未结算。**与 xuan「全是 loser 残差」完全相反**。

机理猜测：late_grammar 不积极 MERGE，结算后让 winner side 自动按 $1 估值——这就是它的 alpha 收口路径。loser side 因为价值 0 也懒得 redeem，与 xuan 类似。但它比 xuan 多保留了 winner side 的 redeem，因为本来就没 MERGE 出 USDC。

等级：A

### 2.2 merged_ratio p50 = 2.4% — 几乎不 MERGE

```text
单侧残差 (12 conds):
  residual size       p10=  9.9  p50= 11.7  p90= 18.4
  total_bought        p10= 10.0  p50= 12.3  p90= 18.9
  merged_ratio        p10=0.010  p50=0.024  p90=0.032
```

xuan 的 merged_ratio p50 = 98.5%，late_grammar = 2.4%——**两个数量级反差**。

更精准的指标：14.6h 窗口内 activity 中 **MERGE = 0 次**。late_grammar **完全不 MERGE**，至少在这个窗口内。

是否「批量晚 MERGE」？需要更长窗口验证。但当前 95 个 mergeable=true 的 condition 都还挂着没合，说明即使有「晚 batch」也是几小时甚至几天级别的延迟。

等级：A

### 2.3 pair_cost 分布更宽，median 略亏

```text
n=95 (both-sided conds)
p5 =0.7634  p10=0.8652  p25=0.9698
p50=1.0181
p75=1.0628  p90=1.1319  p95=1.1683

buckets:
  <=0.85       :  8
  0.85-0.95    : 12
  0.95-1.00    : 21
  1.00-1.04    : 17
  1.04-1.10    : 23
  >1.10        : 14
```

vs xuan (FIFO)：xuan 的 pair_cost 分布 p10=0.81 / p50=0.99 / p90=1.12 — **late_grammar 中位数高 0.03**，而且大量 pair 超过 1.10（14 个）。如果只看 pair-side PnL，late_grammar 累计 pair_cost > 1 比 xuan 多得多。

这意味着：**late_grammar 的 alpha 不可能来自 pair discount**——pair 本身平均亏的。

等级：A

### 2.4 资金体量 / PnL

```text
totalBought sum (cumul gross buys)    : $15,496
initialValue sum (cost remaining)     : $7,091.98
currentValue sum (mark-to-mkt)        : $7,441.94
realizedPnl sum                       : $0.00
cashPnl sum (open-position MTM PnL)   : $349.95
```

- totalBought $15k vs xuan $262k → late_grammar 资金量小 17×
- realizedPnl = $0 → 注意这是**因为 14.6h 窗口里 0 MERGE 0 SELL**，所以没有「关闭 position 的 PnL」记录；REDEEM 不计入 realizedPnl 字段（推测）
- cashPnl +$349.95 = open-position MTM 浮盈，主要由 winner side overweight 贡献

> **注**：与 xuan -$23k realizedPnl 不能直接对比——口径不同（xuan 有大量 MERGE/REDEEM 完结的 lifetime PnL，late_grammar 全部还 open）。

PnL/资金比率粗估：
- late_grammar: $349 / $15,496 / 14.6h × 24 ≈ **3.7%/天浮盈**
- 但这仅是 MTM，还没结算 + 仍依赖 winner-bias 持续

等级：A（数值）/ B（比率解释）

---

## 3. Universe 分布与并发性

### 3.1 6 个 venue 并行

| Universe | trades | activity | positions | rounds |
|----------|--------|----------|-----------|--------|
| eth-updown-5m   | 843 | 870 | 56 | 167 |
| btc-updown-5m   | 839 | 906 | 65 | 176 |
| eth-updown-15m  | 677 | 618 | 24 |  60 |
| btc-updown-15m  | 574 | 575 | 24 |  60 |
| xrp-updown-15m  | 355 | 317 | 21 |  56 |
| bitcoin-daily   | 212 | 214 | 12 |  15 |

5m 与 15m 并跑；BTC、ETH、XRP 三个标的并跑；外加 bitcoin daily 长期方向。

### 3.2 并发度量

- 单秒最高同时活跃 round：**5 个**
- 60 秒滚动窗内不同 round 触及数最高：**8 个**

vs xuan 始终只在 1 个 BTC 5m round 上。**Late-Grammar 的并发数是 xuan 的 5–8 倍**——确认它是「平铺多 venue」而非「单 venue 深耕」。

等级：A

---

## 4. Per-universe 行为分化（关键新发现）

按 universe 拆桶做 episode reconstruction（eps=10）：

| Universe | opened | closed | clean | clean/closed | same_add% | p50 (s) | p90 (s) |
|----------|--------|--------|-------|--------------|-----------|---------|---------|
| bitcoin-daily   |  16 |  12 |  12 | **1.0000** | 4.60% | 12 | 76 |
| btc-updown-15m  |  17 |  16 |  14 | 0.8750 | 0.63% | 4  | 24 |
| eth-updown-15m  |  24 |  20 |  18 | 0.9000 | 0.38% | 10 | 32 |
| xrp-updown-15m  |  14 |  10 |   7 | 0.7000 | 1.19% | 4  | 28 |
| **eth-updown-5m**   | 357 | 209 | 150 | **0.7177** | **20.65%** | 46  | 162 |
| **btc-updown-5m**   | 191 |  15 |   6 | **0.4000** | **39.46%** | 88  | 272 |

**这是本研究最重要的发现**：

- **15m + daily venue**：clean_ratio ≥ 87%, same_side_add ≤ 5%——**接近 pair-gated 行为**，与 xuan 类似
- **5m venue（BTC 5m 尤其）**：clean_ratio 40-72%, same_side_add 20-40%——**完全不是 pair-gated**

可能解释：5m round 时长太短（300s），late_grammar 在 5m 上挂的小 clip 来不及在同一 round 内被双边覆盖；而 15m / daily 给了足够时间让自然 order flow 双向打到挂单。

**含义**：late_grammar 的策略**逻辑是单一的**（多 venue 双边小 clip BUY），但**呈现出的 episode 形态是 venue-dependent**——在快节奏 venue 上看起来像「单边追涨」，在慢节奏 venue 上看起来像「pair-gated」。**这是被动 maker 行为投射到不同时间尺度上的产物**。

等级：A（数据）/ B（解释）

---

## 5. Clip size 条件结构

### 5.1 全集分布

```text
n=3500  p10=5.0  p50=10.4  p90=44.0  p99=104.7  max=633.4 (shares)
```

vs xuan p50=131.7，**late_grammar 中位数小 12.7×**。这是 maker 被动小挂单的典型形态。

### 5.2 按 universe 拆

| Universe | clip p50 | clip p90 | 含义 |
|----------|---------|---------|------|
| **btc-updown-5m**   | **40.5** | 69.8  | 5m BTC 用更大 clip（接近 xuan 的量级） |
| eth-updown-5m   | 11.0 | 24.8  | ETH 5m 中等 |
| btc-updown-15m  |  5.4 | 11.3  | 15m 极小 |
| eth-updown-15m  |  5.4 | 11.5  | 15m 极小 |
| xrp-updown-15m  |  5.4 | 10.8  | 15m 极小 |
| bitcoin-daily   | 11.1 | 14.0  | daily 中小 |

**Late-Grammar 在不同 venue 用不同 clip size**：
- 5m BTC 大 clip (40.5) — 流动性最好的标的，敢用大单
- 15m 普遍小 clip (5.4) — 时间长可慢慢累积，clip 小利于多次成交分散

xuan 全程 p50 ≈ 130，没有 venue dimension 因为只玩 BTC 5m。

等级：A

### 5.3 按 intra-round trade index

```text
trade #1 : p50=10.4
trade #5 : p50=10.3
trade #10: p50= 7.3
```

late_grammar 的 clip 几乎不随 intra-round 序号变化（xuan 是 151 → 107 单调递减）。**没有 inventory-aware 行为**——这是被动小挂单的特征：每个 clip 都是独立挂单的最小单位，与累积仓位无关。

等级：A

### 5.4 按 prior imbalance

```text
imb 0.00-0.05  p50=10.9
imb 0.05-0.15  p50= 9.2
imb 0.15-0.30  p50= 8.4
imb 0.30-1.00  p50=10.0
```

平稳，无 imbalance-driven up-clip 行为（xuan 是 imb≥0.30 时 p50 升至 142）。**没有 completion-mode 加紧**——再次确认非 pair-gated。

等级：A

---

## 6. MERGE / REDEEM 节律

### 6.1 MERGE：14.6 小时 0 次

```text
MERGE 数量: 0
MERGE total: $0
```

vs xuan 14.6h pro-rated 约 196 次 MERGE，$73k 回收。**Late-Grammar 完全不在窗口内 MERGE**。

延伸观察：当前持仓中 95 个 condition mergeable=true（理论上可以 MERGE），但都没动。

### 6.2 REDEEM：缓慢批量

```text
REDEEM 数量: 451
REDEEM total: $27,338
REDEEM offset (vs round close):
  p5 = 972s   (~16 min)
  p10= 1856s  (~31 min)
  p25= 3964s  (~66 min)
  p50= 7864s  (~2.18 h)
  p75= 11772s (~3.27 h)
  p90= 15364s (~4.27 h)
```

vs xuan REDEEM offset p50 = 38s（即收盘 38 秒后）。**Late-Grammar 慢 200×**。

late_grammar 显然是**积累一批 redeemable 后批量统一处理**，而非每个 round close 就立即 redeem。

### 6.3 资金回收画像

```text
            late_grammar    xuan (37h pro-rated to 14.6h)
MERGE       $0              ~$73k
REDEEM      $27.3k          ~$14k
ratio       0:∞             5.2:1
```

完全相反的资金回收形态：
- xuan：**MERGE 主导**（5.2× REDEEM），盘中收水，资金高周转
- late_grammar：**REDEEM 主导**（MERGE = 0），结算后批量收，资金低周转

等级：A

---

## 7. Winner-side 残差倾向（按 universe 拆桶）

```
Universe        resolved  winnerHeavier  loserHeavier  resid_p50
bitcoin-daily          3            1            2       -5.57
btc-updown-15m        11            9            2        5.20
btc-updown-5m         28           27            1       19.17
eth-updown-15m        11           11            0        5.16
eth-updown-5m         25           22            3       10.37
xrp-updown-15m         9            3            6       -6.22
```

按已结算双侧 condition：
- **BTC 5m: 27 winner / 1 loser** — 极端 winner-bias
- **ETH 5m: 22 winner / 3 loser**
- BTC 15m: 9/2 — winner-bias
- ETH 15m: 11/0 — 全部 winner-bias
- **XRP 15m: 3 winner / 6 loser** — 反向（loser-bias）！
- bitcoin-daily: 1/2 — 反向（样本小）

**核心定性**：
- BTC/ETH（流动性高的标的）：**强 winner-bias**
- XRP / bitcoin-daily（流动性低或时间长的标的）：**反向或无 bias**

机理推断（B 级）：当价格信息逐步泄漏到盘口时，winner side 的 buy pressure 增加，late_grammar 在 winner side 的限价 buy 单更容易被打中。**这是被动 maker 几何在有方向流动性的市场上自然累积的「winner overweight」**——不是主动方向预测，而是**被动暴露于市场效率**。

xuan 没有这一现象因为：
- 单 venue 数据样本量稀疏（仅 1 个双侧条件可观测）
- xuan 紧密 MERGE 把不平衡部分及时回收，不让 winner-bias 累积

等级：A（数据）/ B（机理解释）

### 7.1 Winner / Loser side avgPrice

```text
Winner-side avgPrice  n=98  p10=0.37  p50=0.72  p90=0.89
Loser-side  avgPrice  n=89  p10=0.10  p50=0.33  p90=0.60
```

加起来 0.72 + 0.33 = **1.05 → 平均每对亏 $0.05**。

但因为 winner side **持仓比 loser side 多 ~10 股**，加上 winner side $1 redeem，整体 PnL 仍能正：
- pair part 净亏：pair_qty × 0.05
- winner overweight 净赚：extra_qty × (1 - winner_avgPrice) ≈ extra × 0.28

如果 extra/pair_qty > 0.05 / 0.28 = 17.8%，整体盈利。当前 73 winner-heavier × p50 +9.94 股 / 平均双侧总仓 ~50 股 ≈ 20% — **刚好正向**。

这是个**margin-thin 策略**——稍有市场结构变化（XRP 那种），即可翻盘成亏损。

等级：B

---

## 8. Maker vs Taker 路径裁决

### 8.1 本次结论

- 公开 `data-api` 不返回 trader_side（与 xuan 同）
- 认证 CLOB `/data/trades` 服务端 scope 限制（与 xuan 同）

但**间接证据强烈支持 late_grammar 是 maker**：

1. clip p50 = 10–11 股（多数 venue），极小，符合 passive limit order 形态
2. inter-fill 同侧 p50 = 2s（vs 跨侧 36s）——同侧极快连发说明**同一限价单被分批吃**
3. clip size 不随 imbalance 升档——maker 不主动加仓
4. clip size 不随 intra-round idx 变化——maker 不做 inventory 管理

综合等级：**B（强支持 maker）**——比 xuan（C 等级）有更多间接证据。

但仍需 V2 §8.2 提到的 alt 路径（recorder book-snapshot timing match / Goldsky subgraph）做最终裁决。

### 8.2 大 clip outliers（max=633）

虽然 p99=104.7，但有 max=633 的极端样本——**少量大 clip 推测是 taker scenario**：可能是被动挂单时机错过、临近 round 末追平、或紧急对冲。

数量极少（< 1%），不影响整体判断。

等级：B

---

## 9. 与 xuanxuan008 横向对比（核心增量）

### 9.1 完整对比表

| 维度 | xuan | late_grammar | 差异性质 |
|------|------|--------------|---------|
| **基础参数** |
| Universe 数 | 1 | 6 | 显著差异 |
| Universe 类型 | BTC 5m only | BTC/ETH/XRP × 5m/15m + bitcoin-daily | 显著差异 |
| 14.6h 窗口 trades | ~1380 (37h pro-rated) | 3500 | 量级差异 (2.5×) |
| totalBought 累计 | $262,422 | $15,496 | 显著差异 (17×) |
| **持仓结构** |
| 双侧同持率 | 0.2% (1/540) | 88.8% (95/107) | **完全反转** |
| merged_ratio p50 (单侧) | 98.5% | 2.4% | **完全反转** |
| Winner-side residual | 0 (全 loser) | 73 winner / 14 loser | **完全反转** |
| pair_balance p50 (\|up-down\|/max) | 0.30 (n=1) | 0.17 (n=95) | 类似量级 |
| pair_cost p50 | 0.99 | 1.02 | 略升 |
| pair_cost > 1.04 比例 | 100% (1/1，样本少) | 39% (37/95) | — |
| **Clip 行为** |
| Clip p50 (全集) | 131.7 | 10.4 | 显著差异 (12×) |
| Clip p50 (BTC 5m) | 131.7 | 40.5 | 显著差异 (3.3×) |
| Intra-round idx 衰减 | trade #1=151 → #10=107 | 平稳 | 显著差异 |
| Imbalance-driven 升档 | imb≥0.30 时 +24% | 无 | 显著差异 |
| Tail (round 末 30s) 升档 | +16% | 无 | 显著差异 |
| **时序** |
| Inter-fill cross p50 | 18s | 36s | 显著差异 (慢 2×) |
| Inter-fill same p50 | 10s | 2s | 显著差异 (快 5×) |
| Run length p50 / max | 1 / 4 | 1 / 8 | 部分相似 |
| Same-second cross-side ratio | 5.49% | 2.49% | 部分差异 |
| **Episode 行为（eps=10）** |
| clean_closed_ratio | 91.3% | 33–87%（依 venue） | venue-dependent 差异 |
| same_side_add_ratio | 10.5% | 4–39%（依 venue） | venue-dependent 差异 |
| close_delay p50 | 20s | 4–88s（依 venue） | venue-dependent 差异 |
| **资金回收** |
| MERGE 数 (14.6h 窗口) | ~196 | 0 | **完全反转** |
| MERGE 主峰 offset | 278s（距收盘 22s） | N/A | — |
| REDEEM offset p50 | +38s post-close | +7864s post-close | 显著差异 (200×) |
| MERGE/REDEEM 比 | 5.2:1 | 0:∞ | **完全反转** |
| **Session** |
| UTC 峰谷比 | 5:1 | ∞:1 (UTC 10–18 几乎全空) | 部分差异 |
| Round 覆盖率 (BTC 5m only) | 94.7% | 98.3% | 类似 |
| **PnL 字段** |
| realizedPnl 累计 | -$23,408 | $0 | 口径不可比 |
| cashPnl (open MTM) | -$3,142 | +$349 | 显著差异 |

### 9.2 定性总结

- **xuan**：单 venue 紧密 pair-gated tranche automaton + 频繁 MERGE 收水。Alpha 来自 pair_discount 累积 surplus 抵消 repair_loss。
- **late_grammar**：多 venue 平铺被动小 clip BUY + 完全不 MERGE + 慢批量 REDEEM。Alpha 来自 BTC/ETH 流动性结构性偏差导致的 winner-side 自然 overweight。

**两者唯一的共同点**：side=BUY only（无 SELL），双边 outcome token 都买。但实现路径几何完全不同。

### 9.3 是否同一类策略？

**不是**。late_grammar **不是 xuan 的同类**。证据：

1. MERGE 行为完全反转（98.5% vs 2.4%）
2. 残差结构反转（loser-only vs winner-heavier）
3. Universe 数量级差（1 vs 6）
4. Clip size 量级差（131 vs 10）
5. Episode 在 5m venue 上 clean_ratio < 50%（vs xuan 91.3%）

把 late_grammar 视作 xuan 的「另一种实现」是误读。它们是 Polymarket 上**两类完全不同的双边 BUY 策略 archetype**。

等级：A

---

## 10. 策略归类与复刻判断

### 10.1 Late-Grammar 策略 archetype

**「多 venue cross-asset 平铺 + 被动小 clip maker + 不 MERGE + 慢 REDEEM」**

子组件：
- **平铺**：6 个 venue 并发，单 round 资金占用低
- **被动小 clip**：5m 主流标的 ~10 股，依靠盘口自然成交
- **无 inventory 管理**：clip size 不依赖 imbalance / intra-round idx / 时间
- **无主动 MERGE**：让仓位自然结算，winner side 自然 redeem 为 USDC
- **批量晚 REDEEM**：积累若干 round 后统一处理（推测：减少 gas、便于税务/会计）

Alpha 来源：**市场流动性结构导致的 winner-side 自然 overweight**——并非主动方向预测。

### 10.2 复刻判断：**不推荐**

#### 10.2.1 不推荐的具体理由

1. **Alpha 量级偏小且 margin-thin**
   - 14.6h cashPnl +$349 / $15k base = 2.3%/14.6h ≈ 3.7%/天 浮盈（但仅 MTM）
   - pair part 平均亏 5%（pair_cost 1.05），完全靠 winner overweight 弥补
   - 任何 winner-bias 减弱（如 XRP 上的 3:6 反向）即翻盘成亏损

2. **依赖市场结构性偏差，不易移植**
   - BTC 5m: 27:1 winner / ETH 5m: 22:3 → 看似稳定
   - XRP 15m: 3:6（反向）/ bitcoin-daily: 1:2（反向）→ 不普适
   - 这种 alpha 来自**特定标的 × 特定 tenor** 的盘口微观结构，不是可设计的 mechanism

3. **与我们 V1.1 PGT 蓝图正交**
   - V1.1 是「主动 inventory 管理 + 紧密 MERGE + 状态机」
   - late_grammar 是「被动暴露 + 不 inventory 管理 + 等结算」
   - 两者无法合并，切方向意味着推翻已有积累

4. **资金效率低**
   - 95 个 mergeable=true 仓位长期不释放 → 双边各占用 USDC
   - 慢 REDEEM 进一步延长资金 carry
   - 我们的 PGT 蓝图刚好相反：通过 MERGE/REDEEM 高频回收资金

5. **执行栈复杂度被低估**
   - 「平铺多 venue」需要 cross-venue 同步挂单基础设施
   - 单笔利润极低（clip × spread 通常 < $1），需 high throughput
   - 我们当前 polymarket_v2 是单 venue 设计，扩到 6 venue 是显著工程

#### 10.2.2 仍可借鉴的点

虽然不推荐复刻整个策略，但有**3 个微观设计**可纳入 PGT V1.1+ 增量：

1. **Per-universe clip size**（V1.1 §H-1 增量）
   - late_grammar 在 BTC 5m 用 40.5 vs ETH 5m 用 11.0
   - 我们 PGT 当前是「全宇宙固定 BASE_CLIP=120」
   - 应改：clip = `BASE × universe_liquidity_multiplier(slug_prefix)`

2. **15m / daily 兼容的 episode reconstruction**
   - late_grammar 在 15m + daily venue 上 clean_ratio 87–100%
   - 这是「pair-gated 行为在足够时长下自然涌现」的有力证据
   - 我们 PGT 蓝图当前只针对 5m，扩到 15m 几乎是直接复用

3. **Winner-bias 监控指标**
   - 即使我们不复刻 late_grammar，**也应在 PGT shadow 中监控自己的 winner/loser residual bias**
   - 如果意外出现 winner-bias，说明执行偏离了 pair-gated 几何，应回检

#### 10.2.3 结论

> **不推荐把 late_grammar 当作复刻目标**，但**作为对照基线长期监测**有价值。它揭示了 Polymarket 上「被动 maker 的自然累积偏差」这一现象，有助于我们理解 PGT 在 shadow 中可能遭遇的 inventory drift 模式。

---

## 11. 证据等级表

| 维度 | 等级 | 数据基础 |
|------|------|---------|
| 「多 venue 平铺」 | A | 6 个 prefix 直接数据 |
| 「被动小 clip」 | A | clip p50=10.4，分布稳定 |
| 「不 MERGE」 | A | 14.6h 窗口 0 次 MERGE |
| 「慢批量 REDEEM」 | A | 451 REDEEM, p50=+2.18h post-close |
| 「双侧同持」 | A | 95/107 = 88.8% |
| 「Winner-side bias 在 BTC/ETH」 | A | BTC 5m 27:1, ETH 5m 22:3 |
| 「Winner-side bias 在 XRP 反向」 | A | XRP 3:6 |
| 「Maker 主导路径」 | B | 间接证据：clip 形态、inter-fill same 极快 |
| 「Alpha 来自市场结构偏差非方向预测」 | B | XRP 反向 + clip 不依赖 imbalance |
| 「与 xuan 是不同 archetype」 | A | 全维度反转 |
| 「不值得复刻」 | B | margin-thin + 依赖结构性偏差 |
| 「Per-universe clip 可借鉴入 PGT」 | B | late_grammar 行为 + 我们 PGT 设计兼容性 |

---

## 12. 下一步研究建议

1. **更长窗口验证 winner-bias 稳定性**：当前 87 个 resolved 样本，需扩到 ≥ 500 个判断 BTC/ETH winner-bias 是否长期成立（公开 API 卡 3500 条，需 Polymarket Goldsky subgraph）
2. **Cross-account winner-bias 统计**：如果 Polymarket 上「被动 maker 普遍呈 winner-bias」，那这是**整个 venue 的微观结构特征**，而非 late_grammar 的策略本身——这一发现远比复刻 late_grammar 更有价值
3. **Recorder book-snapshot timing match for late_grammar**：用 V2 §8.2 alt 路径 #1，把 maker/taker 从 B 升到 A
4. **对照 xuan 同期 winner-bias**：xuan 几乎全 MERGE 了，但能否在 MERGE 之前的瞬时仓位看到 winner-bias？需要对 xuan trades 重建时序仓位（复用 V2 cash-flow ledger 重建工作）
5. **PGT 蓝图增量**：把 §10.2.2 的 3 个微观设计（per-universe clip、15m 扩展、winner-bias 监控）写入 V1.1.1 增量备注

---

## 附录 A：脚本与数据文件清单

```
scripts/pull_late_grammar_long_window.py     # 复刻 xuan 拉取脚本（仅替换 WALLET / OUT_DIR）
scripts/analyze_late_grammar_positions.py    # §2 §7
scripts/analyze_late_grammar_long_window.py  # §3 §4 §5 §6

data/late_grammar/positions_snapshot_2026-04-26.json    # 202 rows
data/late_grammar/positions_analysis.json / .txt        # §2 §7 输出
data/late_grammar/positions_single_residuals.json       # 12 单侧残差明细
data/late_grammar/trades_long.json                      # 3500 trades
data/late_grammar/activity_long.json                    # 3500 activities
data/late_grammar/long_window_analysis.json / .txt      # §3-§6 输出
data/late_grammar/universe_split.txt                    # §4 per-universe episode 输出
data/late_grammar/pull_summary.json
data/late_grammar/pull_log.txt
```

附录 B：关键已验证假设

- H1 (cross-asset/cross-tenor maker)：**A 级确认** — 6 venue 并发，60s 滚动窗最高 8 round 并发
- H2 (不积极 MERGE)：**A 级确认** — 14.6h = 0 次 MERGE
- H3 (deep discount pair_cost)：**否定** — pair_cost p50=1.02 不算折扣；alpha 实际来自 winner-bias
- H4 (clip / session 不同)：**A 级确认** — clip 12×、inter-fill 2× / 5×、session 集中度 ∞:1
- H5（新提出）(Winner-bias 是市场结构性而非主动)：**B 级支持** — XRP 反向 + clip 不依赖 imbalance
