# `xuanxuan008` 策略彻底解构（最近交易样本版）

生成时间：`2026-04-25`
研究目标：先彻底解构 `xuanxuan008` 最近公开交易行为，明确哪些已经证实，哪些只是工作假设，以及要澄清剩余疑问还缺什么数据。
本文刻意不讨论实现方案，先把研究基线立稳。

## 1. 研究范围与样本

本次研究只看 `xuanxuan008` 的**最近连续公开交易窗口**，不混入更早期样本。

- 账户：`0xcfb103c37c0234f524c632d964ed31f117b5f694`
- 交易样本：最近 `1000` 笔 `trades`
- 活动样本：最近 `1000` 条 `activity`
- 交易时间窗：
  - `UTC`: `2026-04-24 19:06:34` 到 `2026-04-25 02:55:32`
  - `Asia/Shanghai`: `2026-04-25 03:06:34` 到 `2026-04-25 10:55:32`
- 覆盖市场：`95` 个连续 `BTC 5m` 市场
- 这 `95` 个市场在时间上是**连续无缺口**的，没有跳过中间 round

数据来源：

- [Polymarket profile](https://polymarket.com/@xuanxuan008?tab=positions)
- [Profile by wallet](https://polymarket.com/profile/0xcfb103c37c0234f524c632d964ed31f117b5f694)
- [Data API: trades](https://data-api.polymarket.com/trades?user=0xcfb103c37c0234f524c632d964ed31f117b5f694&limit=1000)
- [Data API: activity](https://data-api.polymarket.com/activity?user=0xcfb103c37c0234f524c632d964ed31f117b5f694&limit=1000)
- [API Introduction](https://docs.polymarket.com/api-reference)
- [Get trades for a user or markets](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets)
- [User Channel](https://docs.polymarket.com/market-data/websocket/user-channel)
- [Gasless Transactions](https://docs.polymarket.com/trading/gasless)
- [Merge Tokens](https://docs.polymarket.com/trading/ctf/merge)
- [Using the orderbook](https://docs.polymarket.com/polymarket-learn/trading/using-the-orderbook)
- [Order Lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)

## 2. 证据分级

- `A`：直接由公开数据确认
- `B`：由公开数据强支持，但仍有替代解释
- `C`：合理假设，当前数据不足以定论

## 3. 已证实事实

### 3.1 近期宇宙极度收敛

`A`

- 最近这 `1000` 笔公开 `trades` 全部来自 `BTC 5m`
- 覆盖 `95` 个连续 round，没有中途挑市场跳过
- 没有看到别的币种、题材、体育或政治市场

这意味着：**近期样本里，他不是靠“广泛挑市场”赚钱，而是在一个固定高流动宇宙里连续运行。**

### 3.2 近期公开成交几乎是 `BUY-only`

`A`

- 最近 `1000/1000` 笔 `trades` 都是 `BUY`
- 最近 `1000` 条 `activity` 的类型分布是：
  - `TRADE`: `787`
  - `MERGE`: `138`
  - `REDEEM`: `75`
- 最近样本里没有看到公开 `SELL`

这说明至少在最近窗口中，主路径不是：

- `BUY first leg`
- `BUY/SELL second leg`
- `SELL` 平仓

更像是：

- `BUY Up`
- `BUY Down`
- `MERGE` 成对部分
- `REDEEM` 剩余部分

### 3.3 它不是“一脚开仓、一脚收尾”，而是多笔小 tranche 滚动构建

`A`

- 每个市场的公开成交笔数：
  - 中位数 `10`
  - `p10 = 4`
  - `p90 = 16`
  - 最大值 `26`

这已经排除了“单笔双边扫货”是主模式。
它更像**一轮内多次交替买入**，然后穿插 `MERGE`。

### 3.4 他经常很早参与，但不是每轮都瞬时完成整场配平

`A`

- 首笔成交相对市场开盘时间：
  - 中位数 `10s`
  - `p90 = 60s`

所以他的典型行为是：**开盘后很快进入**。
但这不等于“整场仓位 30 秒内完全配平”。

## 4. 最关键发现：他靠的是局部 clip 的快速对冲，不是整场仓位的瞬时闭合

这是本次研究最重要的结论。

### 4.1 side 序列高度交替

`A`

最近样本里，把每个市场的 side 序列展开：

- `95` 个市场里，`90` 个市场是“首笔之后的下一笔就是 opposite side”
- 只有 `5` 个市场会先出现第二笔同侧
- 所有 run length 汇总：
  - 中位数 `1`
  - `p90 = 2`
  - 最大值 `4`
  - `681` 个 run 的长度就是 `1`
  - `147` 个 run 的长度是 `2`
  - 长于 `2` 的 run 只有 `8` 个

这说明它的真实路径非常接近：

```text
U-D-U-D-U-D...
```

而不是：

```text
U-U-U-D...
```

### 4.2 首笔后 opposite 到来很快，但“完全覆盖首笔”会更慢

`A`

定义两个不同指标：

1. `first_opposite_delay`
   首笔成交之后，第一次 opposite side 成交出现的时间
2. `first_fill_cover_delay`
   opposite side 的累计成交量，第一次足以覆盖首笔数量的时间

最近样本结果：

`first_opposite_delay`

- 中位数 `25s`
- `p90 = 92s`
- `53/95` 在 `30s` 内
- `76/95` 在 `60s` 内
- `88/95` 在 `120s` 内

`first_fill_cover_delay`

- 中位数 `46s`
- `p90 = 162s`
- `29/95` 在 `30s` 内
- `56/95` 在 `60s` 内
- `73/95` 在 `120s` 内

这组数据非常重要。

它说明：

- 如果把问题定义成“首笔后多久看到 opposite flow”，答案是：**通常很快**
- 如果把问题定义成“首笔后多久把首笔数量完全覆盖”，答案是：**没有想象中那么神奇**

所以更准确的说法不是：

- “他总能在 30 秒内把整场仓位配平”

而是：

- “他总能让局部未配对风险很快被 opposite flow 接上，并通过很多小 clip 叠加成整场近乎配平”

### 4.3 未配对净差最终非常小

`A`

按每个市场的累计 `Up/Down` 数量计算：

- `final_gap / pairable_qty` 中位数约 `2.21%`
- `p90 ≈ 4.29%`
- `87/95` 个市场最终 `<= 5%`
- `93/95` 个市场最终 `<= 10%`

也就是说，虽然路径上经常会出现短时不对称，但**最终收盘前后的结果几乎总是接近平衡**。

### 4.4 它真正控制的是“不平衡半衰期”

`B`

如果沿着每个市场的成交路径，看净差从某个时点开始回落到一半以下所需时间：

- `imbalance half-life` 中位数约 `24s`
- `p90 ≈ 111s`

这比“固定 30 秒内完全配平”更像他真正优化的对象：

- 不是追求绝对零净差
- 而是把净差存在的时间压得很短

## 5. 这套策略到底怎么运作

基于最近公开样本，最可信的还原如下。

### 5.1 策略骨架

`A/B`

1. 只做 `BTC 5m`
2. 开盘后很快开始参与
3. 用**多笔小 clip** 交替 `BUY Up / BUY Down`
4. 尽量让成交序列保持高度交替
5. 当市场内已经形成足够成对库存时，穿插 `MERGE`
6. 收盘后再 `REDEEM` 剩余部分

### 5.2 它不是“单市场单 cycle”，而是“单市场多 tranche”

`A`

最近 `81` 个有 `MERGE` 的交易市场里，有 `65` 个市场出现了：

- 先 `MERGE`
- 后面又继续 `TRADE`

这直接证明它的基本单位不是：

- 一轮 market = 一个 cycle = 一次建仓到一次收尾

而是：

- 一轮 market 内可以有多个 tranche
- 每个 tranche 可以先 build
- 已经成对的一段可以先 merge
- 后面继续再 build 新 tranche

时间节奏上也支持这个判断：

- 最近 `81` 个有交易且出现 `MERGE` 的市场中，首次 `MERGE` 相对开盘时间：
  - 中位数 `180s`
  - `p90 = 278s`
- 最近 `74` 个有交易且出现 `REDEEM` 的市场中，首次 `REDEEM` 相对收盘时间：
  - 中位数 `35s`
  - `p90 = 68s`

这说明它近期更像：

- 盘中开始逐步 harvest
- 收盘后很快处理剩余可结算部分

### 5.3 代表性样本：典型轮次

`A`

`btc-updown-5m-1777081200` 的前段序列：

```text
01:40:06  BUY Up   397.61 @ 0.489
01:40:26  BUY Down 396.91 @ 0.465
01:40:34  BUY Up   271.34 @ 0.585
01:40:42  BUY Down 267.43 @ 0.391
01:40:48  MERGE    805.37
...
01:42:54  MERGE    890.15
...
01:44:38  MERGE    677.43
01:45:34  REDEEM   153.55
```

这是非常典型的：

- 交替 build
- 中途 harvest
- 再继续 build
- 再 harvest
- 收盘后 redeem 尾巴

### 5.4 代表性样本：明显失手轮次

`A`

`btc-updown-5m-1777065600`：

```text
21:20:10  BUY Down 154.10 @ 0.488
21:23:00  BUY Up   151.71 @ 0.280
21:24:30  BUY Down 233.23 @ 0.831
21:24:40  MERGE    151.71
```

这个市场最后留下了较大的净差，是最近窗口里最明显的 outlier。
所以近期策略虽然稳定，但并不是“绝对不会失手”。

这也反过来证明：我们不该把它神化成“固定时间必然配平”的系统。

## 6. 他为什么能做到“看起来几乎总能配上”

这是最重要的解释。

### 6.1 不是神秘信号先知未来，而是把风险单位切得足够小

`B`

他之所以表现出极高的配对成功率，最合理的解释不是：

- 他知道 30 秒后盘口一定怎么走

而是：

- 他把风险单位切成很多小 clip
- 每个 clip 的 opposite fill 概率都很高
- 市场级总仓位是很多“局部已快速回补”的 clip 累积结果

### 6.2 他几乎不允许连续同侧 run 扩张

`A`

这一点是公开数据直接证明的。

只要同侧连续成交次数很短，单腿风险就不会积累成难以挽回的大 first leg。
所以他的真实控制变量更像：

- `same-side run length`
- `current live imbalance`
- `imbalance half-life`

而不是单独的 `pair_target`

### 6.3 它近期不像是在“强择时”

`A/B`

最近 `95` 个市场是连续参与的，没有跳 round。
这意味着在这段会话里，它并不是：

- 只挑少数特殊时刻进

而更像：

- 在一个已经确认“值得参与”的高流动 session 里连续跑同一套执行几何

换句话说，**最近这段样本里，它更像在筛 session，而不是筛单个 round。**

### 6.4 这套系统更像“配对成功率优化器”，不是“最佳 target 优化器”

`B`

从公开路径看，他更像在优化：

```text
Pr(opposite flow arrives soon | current state)
```

而不是优化：

```text
expected pair_cost minimum
```

这也是为什么单看 `pair_target` 或 `VWAP`，很难解释他的稳定性。

## 7. 关于 “winner residual” 的重新评级

上一轮研究里，我对“残差是否主动偏向赢家腿”给得太强了。这里修正。

### 7.1 当前更稳妥的结论

`B`

近期样本里，更稳妥的解释是：

- 他的主目标是尽量完成配对
- 剩余残差首先应当视为未完全闭合的 leftover
- 不能先把它解释成主动方向暴露

### 7.2 仍不能完全排除小方向残差

`C`

公开 `activity` 里确实能看到：

- `MERGE`
- `REDEEM`

但这些还不足以证明：

- 他是故意保留赢家腿

因为同样可能是：

- 最后时段未完全配平
- merge 只处理了 full-set 部分
- 剩余单边自然在结算后 redeem

所以这一点目前必须降级为**未定论**。

## 8. 盈利来源的最可信分解

### 8.1 已证实部分

`A`

最近样本里，盈利主路径至少包含：

- 低成本双边 `BUY`
- 市场内 `MERGE`
- 收盘后 `REDEEM`

### 8.2 仍未证实部分

`C`

我们还没有足够证据分清：

1. 盈利主要来自：
   - pair discount
   - maker rebate
   - 高周转 merge
   - 还是少量方向残差
2. 最近 `BUY` 中到底多少是：
   - passive maker
   - aggressive taker

这两个问题直接决定我们后面该模仿什么。

## 9. 当前最强结论

如果把最近样本压缩成一句话：

> `xuanxuan008` 最近的核心 edge，不像是“神奇预测哪一刻能整场配平”，而像是“在固定高流动 BTC 5m 会话里，用高度交替的小 clip build，把局部未配对风险压得很小、很短，再通过滚动 merge 把成对库存回收”。`

更直接一点：

- 他不是靠大 first leg + 神奇救腿
- 他是靠**不让 first leg 长成大 first leg**

## 10. 当前仍未解开的关键疑问

这些问题如果不解开，任何实现都可能跑偏。

### 10.1 它的 `BUY` 到底主要是 maker 还是 taker？

重要性：最高

为什么重要：

- 如果主路径是 passive maker，被 hit 的几何就要完全照着重建
- 如果主路径是 bounded taker，那实现逻辑完全不同

当前证据：

- 公开 `data-api` 不返回 `trader_side`
- 只从成交顺序和价格不能最终定性

需要的数据：

- 认证 CLOB `/trades` 返回的 `trader_side`、`maker_orders`、`taker_order_id`
- 或同步的 order lifecycle / open order 视图

### 10.2 他是不是事先挂双边单，还是“看到一边成交后再补另一边”？

重要性：最高

为什么重要：

- 这决定策略到底是 `two-sided passive seed`
- 还是 `single-leg trigger -> completion mode`

当前证据：

- side 序列高度交替
- 但公开成交时间不是挂单时间

需要的数据：

- open order 时间线
- order accepted / placed 时间
- 原始 websocket `book` + own order events

### 10.3 他是按什么规则调 clip size 的？

重要性：高

当前证据：

- 首笔成交规模中位数约 `170.61`
- `p90 ≈ 397.61`
- 最近样本里 clip size 并不固定

需要的数据：

- 同步盘口快照
- clip 前后 trade tape
- 以及更长时间窗的样本

只有这样才能判断 clip size 是否依赖：

- depth
- spread
- recent flow alternation
- round 剩余时间

### 10.4 他到底是在筛 session，还是筛单个 round？

重要性：高

当前证据：

- 最近 `95` 个 market 连续参与，说明至少这段样本里更像在筛 session

但还需要更长窗口来回答：

- 他是不是只在特定时段上线
- 或者在 session 内部仍有更细 regime filter

需要的数据：

- 至少 `1-2` 周连续 trade/activity
- 最好外加同时期 market-wide BTC 5m universe 对照

### 10.5 残差到底是故意保留，还是未完成配对 leftover？

重要性：中高

当前证据：

- 最近更应当先按 leftover 解释
- 但还不能完全排除小方向暴露

需要的数据：

- 每个市场最终 outcome
- 每次 `MERGE` 前后的精确 pairable inventory
- 残差侧与最终 outcome 的长期统计关系

### 10.6 它是否还并行运行其他策略？

重要性：中

当前证据：

- 最近窗口几乎纯 `BTC 5m`
- 没有公开证据支持“近期并行跑另一套主策略”

但更长时间窗仍然要排查：

- 是否在别的会话、别的钱包、别的市场做别的事

需要的数据：

- 更长时间的地址级交易流水
- 链上 token movements

## 11. 为澄清疑问所需的数据清单

按优先级排序。

### P0：必须拿到

1. 地址级更长窗口 `trades/activity`
   - 至少最近 `2-4` 周
   - 用来判断 session 选择、并发、市场覆盖变化

2. 同步 `book + trade tape`
   - 至少覆盖我们研究的几个代表性 session
   - 用来回答 clip 前后的盘口状态和 total turnover

3. 我方 replay 级数据框架
   - 不是为了模仿 xuan 本人
   - 而是为了把“clip alternation / imbalance half-life / rolling merge”这些概念变成可验证指标

### P1：强烈需要

4. 能区分 maker/taker 的 trade 明细
   - `trader_side`
   - `maker_orders`
   - `taker_order_id`

5. 链上 `merge/redeem/split` 记录
   - 用来确认它是否偶尔走 split-based path
   - 也用来对账 `MERGE/REDEEM` 的真实规模

### P2：锦上添花

6. 更细粒度的 open-order 历史
   - 这能最终回答“双边预埋”还是“单边触发后补腿”

## 12. 研究结论与下一步

当前最稳的研究结论有三条：

1. 最近样本里，`xuanxuan008` 几乎就是一台连续运行的 `BTC 5m buy-only clip alternator`
2. 它的稳定性主要来自：
   - 小 clip
   - 高交替度
   - 低 same-side run length
   - 短 imbalance half-life
   - 滚动 merge
3. 目前还不能把它简化成：
   - “固定 30 秒必配平”
   - “固定 dynamic pair_target”
   - “明确故意保留赢家残差”

因此，后续研究顺序应该是：

1. 先补长时间窗与更高保真数据
2. 先把 `maker/taker`、`two-sided seed`、`clip sizing` 这三个核心问题搞清楚
3. 再谈实现

在这之前，任何直接把它翻译成旧 `pair_arb` 参数调优的做法，都有很大概率是在优化错对象。
