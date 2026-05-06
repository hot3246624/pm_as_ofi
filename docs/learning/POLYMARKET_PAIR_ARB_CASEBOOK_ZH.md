# Polymarket Pair Arb 策略研究案例手册

生成时间：`2026-04-29`

---

## 0. 这本手册怎么用

这不是教程，也不是路线图。它是一本案例手册。

你遇到一个策略问题时，直接按对应章节去拆：

- 这是观察事实，还是解释故事？
- 风险在哪里产生？
- 需要什么数据才能裁决？
- 当前能不能进 shadow？
- 当前能不能进 live？

这本手册围绕一个核心目标：

> 把 `xuanxuan008` 的可复刻部分拆成可研究、可证伪、可落地的策略模块。

---

## 1. 案例一：为什么“只成交一边”是 pair arb 的核心风险

### 1.1 表面问题

你设了一个 pair target，然后挂单。

结果：

- `YES` 成了
- `NO` 没成

表面看，这只是“配对还没完成”。

真实问题是：

- 你已经从一个中性机会，切换成了单边库存风险。

### 1.2 错误理解

最常见的错误处理是：

- 继续在同侧挂更低价
- 试图摊低成本
- 认为这样更容易最终配对成功

这个逻辑的问题是：

- 它把“成本变低”误认为“风险变低”

但在微观结构里，风险可能正在变高：

- 市场继续朝不利方向走
- 对侧流动性变差
- 你同侧库存变大
- repair 成本变高

### 1.3 `xuan` 给我们的启发

`xuan` 看起来更像：

- 第一腿成交后，立即进入 completion 状态
- 优先等待或争取 opposite
- 如果超时或状态恶化，倾向于更快 repair / flatten

它不是无限摊低同侧成本。

这说明它的核心哲学更像：

- 先控制 episode 风险
- 再谈 pair discount

### 1.4 判断 checklist

看到只成交一边时，先问：

- `opposite` 当前 spread 是否变宽？
- `opposite` 最近 5-15 秒是否有成交？
- 当前同侧库存是否已经超过单 episode 上限？
- 再挂同侧会不会提高 `same_side_before_opposite_ratio`？
- 如果现在 repair，最坏亏损是多少？
- 如果继续等 30 秒，最坏库存会是多少？

### 1.5 能不能进策略

可以进 shadow 的规则：

- `same-side add` 前必须输出风险解释
- 每个 episode 记录 `same_side_before_opposite`
- 超时后记录 would-repair 价格与真实后果

暂时不能直接进 live 的规则：

- 一刀切禁止所有 same-side add
- 一刀切允许继续摊低成本

原因是：

- 你还需要知道哪些状态下 same-side add 是修复，哪些状态下是扩大风险。

---

## 2. 案例二：为什么 `30s completion` 不是魔法

### 2.1 表面问题

`xuan` 很多 episode 看起来能在很短时间内补到 opposite。

容易得出的错误结论是：

- 它有某个固定 pair target
- 或它有某种神秘下单优先级

这些都可能有一部分影响，但不是第一解释。

### 2.2 更合理的解释

`30s completion` 更像是 `open gate` 的结果。

也就是说：

- 它不是先随便开，再靠技巧补
- 而是只在某些状态开

这些状态可能同时满足：

- 对侧 spread 不宽
- 对侧近期成交节奏够快
- 双边价差没有过度拉开
- 首腿价格处在有质量但不过分偏的一侧
- 当前 round 仍有足够时间完成 episode

### 2.3 当前研究中的关键张力

我们现在已经看到：

- 更偏 winner-quality 的状态，不一定更快 completion
- 更偏 completion 的状态，不一定更强 winner-bias

所以不能把目标写成单一指标。

真正的问题是：

- 哪些状态同时让 `winner-quality` 和 `completion-quality` 不差？

这就是当前 `Hybrid Frontier` 的意义。

### 2.4 判断 checklist

研究一个 `30s completion` 结论时，先问：

- 样本是否只来自少数高光 round？
- 是否只看成功 episode，漏掉没参与或失败的状态？
- `hit30` 提升时，`winner-quality` 有没有下降？
- `winner-quality` 提升时，`hit30` 有没有下降？
- 是否有 holdout 或 recent-only 检查？

### 2.5 能不能进策略

可以进 shadow 的规则：

- 对每个 first leg candidate 输出 `open gate family`
- 记录 `hit30`、`first_opposite_delay`、`first5 winner-net`
- 分 family 对比我方与 xuan

不能直接进 live 的规则：

- 只因为某个 bucket 的 `hit30` 高，就强制开仓

原因是：

- completion 只是一个目标，不是完整收益来源。

---

## 3. 案例三：`winner-bias` 为什么不能直接写成择边规则

### 3.1 表面问题

当前研究显示：

- `MERGE` 前存在显著 `winner-bias`
- 第一腿有弱 winner-side 倾向
- 前五笔会进一步放大 winner-side 净占优

很容易得出一个危险结论：

- 那我们应该预测 winner side，然后优先买它

这个跳跃太快。

### 3.2 三层必须分开

第一层是观察事实：

- `MERGE` 前 winner-heavier 比例高

第二层是解释：

- 可能来自 weak first-leg bias
- 可能来自 alternating fill geometry
- 可能来自 MERGE timing selection

第三层是实现含义：

- 是否应该择边
- 是否应该调整 first-leg side
- 是否应该改变 clip

现在我们只完成了第一层，并部分推进了第二层。

第三层还不能直接做。

### 3.3 当前最稳解释

当前更稳的解释不是：

- xuan 有强方向预测

而是：

- 弱 first-leg 偏置
- 高交替成交中的不对称放大
- 278s 附近 MERGE 兑现和压平库存

这说明：

- `winner-bias` 更像 episode geometry 的结果
- 不是简单方向信号

### 3.4 判断 checklist

看到任何 winner-bias 结论，先问：

- 它是 `MERGE` 前还是 settlement 后？
- 样本是否有完整 winner mapping？
- 是否混入了 lifetime positions snapshot？
- first-leg bias 有多强？
- first5 amplification 是否仍存在？
- 如果第一腿落在 loser side，后续是否仍能恢复 winner-heavier？

### 3.5 能不能进策略

可以进 shadow 的规则：

- 监控 first-leg side 是否落在 later winner side
- 监控 first5 winner-net
- 监控 pre-MERGE winner-heavier
- 分析 winner-quality 与 completion-quality 的 tradeoff

不能直接进 live 的规则：

- 默认优先买看起来更像 winner 的一侧
- 直接把 `0.6-0.7` 写成强制开仓价格带

原因是：

- 我们还没证明 winner-bias 的可执行来源。

---

## 4. 案例四：为什么 `maker-first` 可能被 adverse selection 反杀

### 4.1 表面问题

maker 看起来成本更低：

- 不吃 spread
- 可能拿到更好价格
- 甚至可能有 rebate

但 maker 的危险也更隐蔽：

- 别人只在你报价变差时成交你

这就是 adverse selection。

### 4.2 在 pair arb 里它怎么发生

你挂了两边或一边。

结果可能是：

- 只有风险更高的一边先成交
- 对侧没有成交
- 价格继续走偏

你以为自己“被动成交”，实际是：

- 市场选择了你最不想先暴露的库存

### 4.3 `xuan` 的关键可能不在 maker 本身

即使 `xuan` 偏 maker，也不能说明：

- maker-first 本身就是 edge

更可能是：

- 它只在 adverse selection 较低的状态挂单
- 或只在 opposite fillability 足够高时开第一腿
- 或有明确的超时 repair 边界

### 4.4 判断 checklist

评价 maker-first 时，不看单笔价格漂亮不漂亮，先看：

- first leg 成交后 `opposite <=30s` 是否高
- maker fill 是否集中发生在后续不利状态
- maker fill 后是否更容易 same-side add
- repair cost 是否被低估
- maker 成交的 round 是否本来就是高流动状态

### 4.5 能不能进策略

可以进 shadow 的规则：

- 记录每个 maker first leg 的后续 `30s` 状态
- 按 book age、spread、recent trade cadence 分桶
- 比较 maker-filled vs would-not-fill candidate 的后续路径

不能直接进 live 的规则：

- 因为 xuan 像 maker，就扩大 maker-first 默认范围

原因是：

- 我方 queue position 和 fillability 未必等于 xuan。

---

## 5. 案例五：`MERGE/REDEEM` 是库存管理，不是附属动作

### 5.1 表面问题

很多人会把 `MERGE` 看成：

- 交易结束后的整理动作

这在 xuan 研究里是错的。

`MERGE` 更像策略主循环的一部分。

### 5.2 `MERGE` 的三个作用

第一，现金化。

- 已配对的 YES/NO 可以变回资金

第二，降库存。

- pairable inventory 被压平

第三，控制尾部暴露。

- 不把过多库存带入 settlement

### 5.3 `5c` 后的新理解

在 `5c` 之前，我们容易把 loser-only residual 理解成：

- xuan 没有方向 alpha
- 只是中性 pair-merger

`5c` 之后，更合理的理解是：

- MERGE 前存在 winner-overweight
- MERGE 把 pairable 部分现金化
- winner residual 可能在 settlement 后 REDEEM
- snapshot 里留下 loser residual

所以 loser-only snapshot 不是完整策略画像。

### 5.4 判断 checklist

研究 `MERGE/REDEEM` 时，先问：

- 这是 `MERGE` 前状态，还是 settlement 后状态？
- pairable qty 有多少？
- merge 后 winner residual / loser residual 怎么变化？
- redeem cash-flow 是否进入了当前 PnL 口径？
- 是否混入了 lifetime positions 和短窗口 trades？

### 5.5 能不能进策略

可以进 shadow 的规则：

- 显式记录 pair-covered 与 merged 的区别
- 记录 mergeable full sets
- 记录 merge 后 residual side
- 分开 window cash-flow 和 lifetime snapshot

不能直接进 live 的规则：

- 用 positions snapshot 的 loser-only 结论反推开仓方向

原因是：

- snapshot 是结算后结果，不是开仓时的决策状态。

---

## 6. 案例六：一个研究结论进入策略前要过哪几关

### 6.1 四层检查

任何结论进入策略前，都必须过四层：

1. 观察事实
2. 因果解释
3. 可执行性
4. 风险边界

少任何一层，都不应该进入 live。

### 6.2 例子：`Hybrid Frontier`

观察事实：

- `0.6-0.7` 的某些 offset bucket 同时有较好 winner-quality 和 hit30

因果解释：

- 可能是更好的 side-location
- 可能是更好的 trade cadence
- 可能只是短窗口样本

可执行性：

- 我方是否能在同样状态拿到类似 first-leg fill？
- 我方 opposite 是否也能在 30s 内补到？

风险边界：

- 如果 30s 内没补到，是否强制 repair？
- clip 是否缩小？
- 是否禁止 same-side add？

当前裁决：

- 可以进 shadow
- 不应进 enforce

### 6.3 例子：`winner-bias`

观察事实：

- MERGE 前 winner-heavier 显著

因果解释：

- 来源未完全裁决

可执行性：

- 我方无法直接知道未来 winner
- 只能用状态 proxy

风险边界：

- 如果 proxy 错了，可能把策略变成方向交易

当前裁决：

- 只能做监控与研究解释
- 不能直接做择边规则

---

## 7. 失败 episode 复盘模板

每次策略失败，不要先问“参数怎么调”。

先按这个模板写：

### 7.1 Episode 基础信息

- market slug
- first leg time
- first leg side
- first leg price
- first leg size
- round relative time

### 7.2 Open Gate

- 当时为什么允许开？
- 属于哪个 gate family？
- 当时 spread / pair ask sum / recent trade cadence 如何？

### 7.3 Completion

- first opposite 是否出现？
- 出现用了多久？
- 30s 内是否完成？
- opposite 未出现前是否有 same-side add？

### 7.4 Inventory

- 最大单边库存是多少？
- pairable qty 何时出现？
- 是否 merge？
- 最终 residual 是什么？

### 7.5 Repair

- 是否触发 repair？
- 触发是否太晚？
- repair cost 是多少？
- 如果提前 10 秒 repair，结果会怎样？

### 7.6 结论

必须归因到一个主类：

- `open timing error`
- `price-location error`
- `opposite fillability error`
- `same-side risk expansion`
- `repair too late`
- `merge/redeem accounting issue`
- `data/replay ambiguity`

---

## 8. 当前最重要的训练目标

你要训练的不是背概念，而是形成 4 个反射：

1. 看到单笔交易，先问它属于哪个 episode
2. 看到漂亮统计，先问样本和口径是否可靠
3. 看到策略建议，先问它会不会扩大库存风险
4. 看到 xuan 的行为，先问它是公开状态选择，还是私有执行优势

这 4 个反射，比记住任何公式都重要。

---

## 9. 一句话总结

`pair arb` 的成败不在于找到一个漂亮的 pair target，而在于：

- 什么时候开第一腿
- 开多大
- 另一腿为什么能来
- 不来时怎么止损
- 配上后如何现金化
- 研究结论如何被证伪后再进入策略

这也是我们研究 `xuanxuan008` 真正要学的东西。
