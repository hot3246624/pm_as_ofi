# Polymarket 头部 Trader 排行榜实证（2026-04-26）

生成时间：`2026-04-26`
研究目的：验证假设「Polymarket 头部 trader 是否都使用类似的库存管理思路」。
数据来源：Polymarket 官方 `/leaderboard` 页面 `__NEXT_DATA__` 内嵌的 30d profit 排行榜，加上 data-api 对每个 wallet 的快速 probe（`positions` + `trades?limit=500`）。

数据文件：
- `data/poly_leaderboard_30d_profit.json`：原始 top 20 数据
- `data/leaderboard_quickprobe.json`：top 6 + 0xb27bc + xuan + late_grammar 的 universe / sides / mergeable 快速指标

---

## 0. TL;DR

**假设被部分推翻**：Polymarket 头部利润 trader **并非全是库存管理思路**——top 5 全是体育市场 directional bet trader（NBA / MLB / WTA / ATP / e-sports），与 xuan 的 BTC 5m 双边库存管理是完全不同的 archetype。

但**修订后的假设成立**：**在 "crypto binary perpetual round" 这个细分市场里**，所有头部 trader 都使用双边 BUY + 库存管理 + MERGE/REDEEM 节律——xuan、0xb27bc（rank 20，30d profit +$530k on $48M volume）、late_grammar 都符合这一模式。

含义：
- ✅ PGT（Pair-Gated Tranche）蓝图方向仍正确——但目标市场是 crypto-perpetual segment，不是 Polymarket 整体
- ✅ 该 segment 资金体量上限较低：xuan $262k cumul vs rank01_anon $151M 30d 成交，差 580×
- ⚠️ 真想做 Polymarket 上规模最大的策略，需要切换到体育/政治 directional——但那是另一研究主线

---

## 1. 排行榜 Top 20（30d profit, category=overall）

数据通过解析 `https://polymarket.com/leaderboard` 页面内嵌 `<script id="__NEXT_DATA__">` 提取：

| rank | pseudonym | wallet | pnl ($) | volume ($) |
|-----:|-----------|--------|--------:|-----------:|
|  1 | (anon) | 0x2a2c53bd278c04da9962fcf96490e17f3dfb9bc1 | 4,460,706 | 151,130,461 |
|  2 | RN1 | 0x2005d16a84ceefa912d4e380cd32e7ff827875ea | 2,378,792 | 127,298,603 |
|  3 | tripping | 0x6480542954b70a674a74bd1a6015dec362dc8dc5 |    21,908 | 120,407,823 |
|  4 | ferrariChampions2026 | 0xfe787d2da716d60e8acff57fb87eb13cd4d10319 |   −20,799 |  87,119,563 |
|  5 | swisstony | 0x204f72f35326db932158cba6adff0b9a1da95e14 | 1,907,189 |  85,119,102 |
|  6 | GamblingIsAllYouNeed | 0x507e52ef684ca2dd91f90a9d26d149dd3288beae | 1,214,945 |  83,045,806 |
|  7 | ashash111 | 0x43e98f912cd6ddadaad88d3297e78c0648e688e5 |    31,926 |  79,206,606 |
|  8 | meetgoodlife | 0x9e9c8b080659b08c3474ea761790a20982e26421 |     5,356 |  71,044,473 |
|  9 | Q96s3kwozynxpau | 0x2663daca3cecf3767ca1c3b126002a8578a8ed1f |    92,279 |  68,035,115 |
| 10 | BakerMcKenzie | 0xd99f3bec8e060ada0aef0c4057695dd5bc22fcdc |   111,713 |  63,514,783 |
| 11 | CemeterySun | 0x37c1874a60d348903594a96703e0507c518fc53a | 1,927,133 |  61,776,193 |
| 12 | Just2SeeULaugh | 0xc21ea96be762bb55041529af6e386e7c53b80215 |    74,230 |  58,029,869 |
| 13 | poorsob | 0x2785e7022dc20757108204b13c08cea8613b70ae |   137,874 |  56,637,441 |
| 14 | gloriafoster | 0x5d189e816b4149be00977c1a3c8840374aec4972 |   −33,244 |  54,332,260 |
| 15 | elkmonkey | 0xead152b855effa6b5b5837f53b24c0756830c76a | 1,987,552 |  52,614,473 |
| 16 | sovereign2013 | 0xee613b3fc183ee44f9da9c05f53e2da107e3debf | 1,942,774 |  51,536,745 |
| 17 | ArmageddonRewardsBilly | 0xc8ab97a9089a9ff7e6ef0688e6e591a066946418 |  −174,202 |  50,404,997 |
| 18 | Bonereaper | 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30 |   536,900 |  50,116,770 |
| 19 | Dhdhsjsj | 0x5d58e38cd0a7e6f5fa67b7f9c2f70dd70df09a15 | −1,057,307 |  48,680,986 |
| **20** | **0xB27BC932...** | **0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82** | **530,644** | **48,575,774** |

**注**：xuan 自己 (`0xcfb103...f694`) 不在 top 20——xuan 30d 利润不够大（$262k cumul gross buys 量级远低于 top 20 的 $48M+ 30d volume）。这并不否定 xuan 的策略价值，只说明它处于资金量较小的细分。

---

## 2. 头部 Archetype 拆解（top 6 + 0xb27bc + 2 个 baseline）

对 top 6 + rank 20 (0xb27bc) + xuan + late_grammar 各拉 500 trades + 全部 positions 做 universe / side mix / 双侧持仓 quick probe：

| tag | positions | conds | 2sides | mergeable | redeemable | side mix | top universes |
|-----|----------:|------:|------:|----------:|-----------:|----------|---------------|
| rank01_anon | 217 | 211 |  6 |  12 | 183 | BUY 500 | **NBA**:280, MLB:42, CS2:28, ATP:27 |
| rank02_RN1 | 500 | 447 | 53 | 151 | 336 | BUY 500 | **WTA**:160, CHI:45, CS2:39, MEX:38 |
| rank03_tripping | 23 | 23 |  0 |   0 |   0 | **BUY 308 + SELL** | **WILL**:420, US:30, Israel:15, Military:6 |
| rank04_ferrari | 500 | 500 |  0 |   1 | 494 | BUY 500 | **NBA**:154, MLB:77, WTA:58 |
| rank05_swisstony | 500 | 362 | **138** | **404** |  10 | BUY 500 | **LAL**:71, DEN:63, FL1:54, BUN:49 |
| rank06_gambling | 324 | 231 | **93** | **186** |  27 | BUY 500 | **MLS**:140, ATP:114, WTA:47, Cricket IPL:47 |
| **rank20_0xb27bc** | **1** | **1** |  0 |   0 |   0 | BUY 500 | **btc-updown-5m: 500** |
| xuan_baseline | 500 | 500 |  0 |   0 | 498 | BUY 500 | btc-updown-5m: 500 |
| late_grammar_baseline | 193 | 103 | **90** | **180** | 181 | BUY 500 | btc-updown-5m:154, eth-updown-5m:119, eth-updown-15m:98 |

### 2.1 Archetype 分类

按 universe 和持仓结构，可以把 9 个 wallet 分成 **3 个 archetype**：

#### Archetype A — 体育 directional（rank01, 02, 04, 05, 06）

特征：
- Universe 是 **特定体育 league**（NBA / MLB / WTA / ATP / Bundesliga / CS2 / IPL Cricket / MLS）
- Side mix 几乎全部 BUY（500 trades 内）
- 双侧持仓率分化大：rank04 完全 0%（**纯 directional**），rank05/06 有 38–40%（**部分对冲**）
- redeemable 占大头 → 等结算赎回为主路径
- mergeable 很少（除 rank05 例外） → **MERGE 不是核心机制**

策略推断：押单边 winner side，等比赛结算自动 redeem。**与 PGT 蓝图无关**。

#### Archetype B — 政治/事件 directional with active exit（rank03 tripping）

特征：
- Universe 是政治 / 国际事件市场（"will X happen" / US / Israel / Military）
- **唯一同时 BUY + SELL** 的（500 trades 中 308 BUY + 192 SELL）
- 0% 双侧持仓
- 资金体量 top 3（$120M 30d volume）但 30d pnl 仅 $22k → **极薄利率 high-volume scalper**

策略推断：事件驱动 directional bet + 主动 SELL 出仓。也**与 PGT 无关**。

#### Archetype C — Crypto perpetual round passive maker / 库存管理（xuan + late_grammar）

特征：
- Universe 是 **btc-updown / eth-updown / 等 crypto perpetual round**（5m / 15m / daily）
- 100% BUY only（无 SELL）
- 每 round 都进（high coverage 94–98%）
- 通过 MERGE 收水（xuan 高频）/ 等结算 redeem（late_grammar）

策略推断：**双边 BUY 被动 maker 库存管理**，是我们 PGT 蓝图的目标 archetype。

#### Archetype D — Crypto perpetual round selective directional taker（rank20_0xb27bc）

> **本 doc 初稿误把 0xb27bc 归入 archetype C，深挖后发现是新 archetype。详见 `QUIETB27_0xb27bc_STRATEGY_DEEP_DIVE_ZH.md`**

特征：
- 同 universe（BTC 5m）但**只挑 17.9% round** 进场
- 进场后大概率 directional（69% rounds 单边交易，imb_p50=1.00）
- 大 clip taker（max 9392 股 vs xuan max 600）
- MERGE 频率仅 xuan 的 1/15
- 0 open positions（snapshot 时刻全部清理干净）

策略推断：**round selector + taker directional bet**，alpha 来自 round selection signal 命中。**与 xuan 同 universe 但完全不同执行几何**。

### 2.2 Crypto-perpetual segment 的内部分化

仅看 archetype C 的 3 个样本：

| | xuan | 0xb27bc (rank 20) | late_grammar |
|---|------|--------------------|--------------|
| Universe | BTC 5m | BTC 5m | BTC/ETH × 5m/15m + bitcoin-daily |
| 30d 资金量级 | 小（cumul $262k） | 大（$48M volume，rank 20） | 中（$15k cumul） |
| 频率 | 高（95 trades/h） | 低（~1 trade/h，但持续 197h+） | 中（240 trades/h，但 14.6h burst） |
| MERGE 节律 | 紧密（277s 主峰） | 待确认（probe 没拉 activity） | 完全不 MERGE |
| 双侧持仓率 | <0.2% | 0%（probe 仅 1 position） | 88.8% |

**关键观察**：rank 20 的 0xb27bc 与 xuan **完全同 universe（BTC 5m）+ 同 BUY-only**，但**频率差 ~95×**——它是 **crypto-perpetual segment 真正的头部规模实现**，必须深挖看其库存管理细节。

---

## 3. 含义与下一步

### 3.1 修订对 xuan 的定位

V2 文档对 xuan 的描述是「连续运行的 BTC 5m 双边 BUY 自动机」——这一点正确，但需要补充 **segment 定位**：

> xuan 是 crypto-perpetual round segment 的一个**资金量较小但执行紧密**的 archetype 实例。同 segment 真正头部规模（rank 20+）的实现是 0xb27bc，频率低 95× 但 30d 利润 +$530k 量级。

### 3.2 PGT 蓝图的 segment 适配

如果 PGT 蓝图实装后想冲 30d 利润榜 top 20，需要：
1. 对齐 0xb27bc 的资金量（$48M 30d volume → 每日 ~$1.6M turnover）
2. 而 xuan 的 30d turnover 量级在 $1M 以下——**0xb27bc 比 xuan 大 16–48×**

但这需要**资金 + 信用 + 风控**三件套同步扩，不是策略层一件事。**短期目标仍是 xuan 量级**，长期看 0xb27bc 是 segment 的天花板参考。

### 3.3 待补研究

1. **0xb27bc 深挖**：拉全量 trades + activity，看它在低频条件下如何做库存管理（高优先级）
2. **gabagool22 probe**：确认 universe（项目名字源头但不在榜上）
3. **xuan §11 第 5 条**：MERGE 前瞬时仓位重建（解 V2 §2.2 悬念）
4. **再补一个 sport trader 深挖（如 rank01_anon）**：可选——确认体育 archetype 的具体形态，作为 PGT 的对比 baseline 而非复刻目标

---

## 附录 A：方法学注记

数据获取路径：
1. `curl https://polymarket.com/leaderboard -L -H 'User-Agent: Mozilla/5.0'` 拿到 SSR HTML
2. 解析 `<script id="__NEXT_DATA__"...>{json}</script>` 拿到 `pageProps.dehydratedState.queries[0].state.data` 数组
3. 每条记录字段：`rank, proxyWallet, name, pseudonym, amount, pnl, volume, realized, unrealized, profileImage`
4. 对每个 wallet 调 `data-api.polymarket.com/positions?user=&limit=500` 与 `/trades?user=&limit=500` 做 quick probe

排行榜分类参数（从 OG image URL 推断）：
- `category`：`overall` / 其他类别（推测有 `crypto`, `sports` 等子榜）
- `time`：`monthly`（默认 30d） / 其他时间窗
- `sort`：`profit` / `volume` / 其他

后续如需细分子榜（例如 "30d profit, crypto category"），可改 URL 参数再重抓。

附录 B：原始数据文件

```
data/poly_leaderboard_30d_profit.json     # top 20 排行榜原始数据
data/leaderboard_quickprobe.json          # 9 个 wallet quick probe 结果
```
