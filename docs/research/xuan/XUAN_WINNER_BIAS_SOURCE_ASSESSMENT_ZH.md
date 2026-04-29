# `xuan` winner-bias 来源初步裁决

生成时间：`2026-04-28`

---

## 1. 研究问题

`§0.5 修正 5c` 已经把 xuan 的工作性主叙事推进到：

- `MERGE 前 observed winner-bias 75.1%`

但这还不是策略结论。真正关键的问题是：

> 这个 `winner-bias` 到底来自什么？

当前最值得区分的 3 条候选路径是：

1. `first-leg side selection`
2. `passive fill geometry / 后续成交放大`
3. `MERGE timing selection`

这份笔记只做 **来源初判**，不做 live 规则设计。

---

## 2. 使用的数据与边界

本轮只使用现有窗口内数据：

- `data/xuan/trades_long.json`
- `data/xuan/activity_long.json`
- `data/xuan/slug_winner_map_full.json`
- `data/xuan/pre_merge_bias_full.json`

窗口边界：

- `trades_long ≈ 37.05h`
- `activity_long ≈ 29.6h`

因此这里的结论全部是：

- **window-level source assessment**

而不是：

- `lifetime truth`

---

## 3. 先说结论

当前最稳的来源裁决不是“单一原因”，而是：

> `winner-bias` 更像 **弱 first-leg 偏置 + 后续 fill geometry 放大 + 278s 附近状态筛选/兑现** 的叠加结果。

换句话说：

- 它**不是**纯 `MERGE` 事后制造的偏差
- 也**不像**只靠强 directional signal 在第一笔就决定了全部结果

更合理的画像是：

1. xuan 的第一腿已经有一定 winner-side 倾向
2. 后续几笔成交把 winner-side 优势进一步放大
3. `278s` MERGE 主峰更像在 **兑现/锁定** 这个偏差，而不是凭空创造它

---

## 4. 证据一：第一腿本身就不是 50/50

在 `425` 个有交易且有 winner mapping 的 slug 上：

- 第一笔落在 eventual winner side：`255`
- 第一笔落在 eventual loser side：`170`
- `first winner ratio = 60.0%`

只看有 MERGE 的 `348` 个 slug：

- `first winner ratio = 58.9%`

这说明：

- xuan 的第一笔并非完全中性
- 但也不是压倒性的强方向选择

**含义**：

- `first-leg side selection` 有信号，但强度只是 **弱到中等**
- 单靠第一笔本身，还解释不了 `pre-MERGE 75.1% winner-heavier`

### 裁决

- `first-leg side selection`：**B**

存在，但不像是全部来源。

---

## 5. 证据二：winner 偏差在前几笔内被快速放大

在同一批 `425` 个 slug 上：

- 前 `2` 笔 winner-side 净占优比率：`60.9%`
- 前 `5` 笔 winner-side 净占优比率：`71.8%`

只看 `348` 个有 MERGE 的 slug：

- 前 `2` 笔 winner-side 净占优比率：`59.2%`
- 前 `5` 笔 winner-side 净占优比率：`72.1%`

这很关键。

如果 `winner-bias` 主要来自第一腿择边，那么：

- `first winner ratio`
- 与 `first 5 trades winner-net ratio`

应该相差不大。

但现在看到的是：

- 第一笔：约 `59-60%`
- 前五笔：约 `72%`

说明在 **第一笔之后**，winner-side 的优势还在继续被放大。

**这更像成交几何或状态依赖的追加成交效果**，而不像“第一笔决定一切”。

### 裁决

- `fill geometry / early-sequence amplification`：**A-/B+**

这是当前最强的来源候选。

---

## 6. 证据三：278s 主峰不是“制造偏差”，而是在兑现偏差

对有第一笔 MERGE 的 `348` 个 slug，按第一笔 MERGE offset 分桶：

### `[0,100)s`

- `n = 23`
- 第一笔 winner ratio：`52.2%`
- 前五笔 winner-net ratio：`73.9%`

### `[100,200)s`

- `n = 110`
- 第一笔 winner ratio：`58.2%`
- 前五笔 winner-net ratio：`63.6%`

### `[200,270)s`

- `n = 71`
- 第一笔 winner ratio：`50.7%`
- 前五笔 winner-net ratio：`61.9%`

### `[270,300)s`

- `n = 144`
- 第一笔 winner ratio：`64.6%`
- 前五笔 winner-net ratio：`83.3%`

同时，`pre_merge_bias_full` 已给出：

- `[270,300)s` 的 pre-MERGE `W:L = 223:54`

这说明什么？

1. 到 `278s` 主峰附近，winner-side 偏差在 **MERGE 之前** 就已经很强了
2. `MERGE` 不是这条偏差的制造器
3. 它更像是：
   - 对已形成 winner-overweight 的库存进行兑现/现金化
   - 同时减少带入结算尾段的方向暴露

### 裁决

- `MERGE timing manufactures winner-bias`：**否定**
- `MERGE timing realizes/harvests winner-bias`：**A-/B+**

---

## 7. 证据四：早期成交不是单边连打，而是高交替中的不对称放大

如果 `winner-bias` 主要来自强 directional conviction，那么我们更容易看到：

- 同侧连续成交
- 或第一腿打在 winner 后持续顺势加同侧

但当前早期路径更像相反的东西：**高交替、低连打**。

### 7.1 第一笔和第二笔几乎总是反侧

在 `425` 个 slug 上：

- `firstWin -> secondLose = 243`
- `firstWin -> secondWin = 12`
- `firstLose -> secondWin = 166`
- `firstLose -> secondLose = 4`

也就是：

- 第一笔如果在 winner side，第二笔几乎一定去 loser side
- 第一笔如果在 loser side，第二笔也几乎一定回 winner side

这更像：

- `pair-gated / alternating completion geometry`

而不像：

- 强 directional 策略的单边连续加仓

### 7.2 最常见的前五笔模式是交替串

出现最多的模式是：

- `WLWLW`：`84`
- `WLWL`：`48`
- `LWLWL`：`43`

这说明 xuan 的早期成交序列核心形态仍然是：

- **高交替**

而不是：

- `WWWWW`
- `LLLLL`

所以 `winner-bias` 不能简单理解成“他一直在追 winner side”。

### 7.3 winner 更常出现在奇数位，loser 更常出现在偶数位

前 5 笔逐笔看：

- 第 `1` 笔：winner ratio `60.0%`
- 第 `2` 笔：winner ratio `41.9%`
- 第 `3` 笔：winner ratio `60.2%`
- 第 `4` 笔：winner ratio `40.2%`
- 第 `5` 笔：winner ratio `64.3%`

也就是说：

- `1/3/5` 更偏 winner
- `2/4` 更偏 loser

这揭示了一个新的来源成分：

- **winner-bias 不是单边堆出来的，而是在交替结构里，通过“winner 更常占奇数位”逐步积累出来的**

### 7.4 但“奇偶位优势”不是全部答案

如果 winner-overweight 只是奇数位停止效应，那么：

- `MERGE` 前买入笔数为偶数的 round，应该更接近中性

但实际第一笔 MERGE 前买入笔数分布中，最常见的是：

- `6` 笔：`129`
- `5` 笔：`50`
- `4` 笔：`49`
- `3` 笔：`30`

并且按奇偶拆：

- `odd pre-merge buy count`：`116` 个，winner-heavier `76.7%`
- `even pre-merge buy count`：`232` 个，winner-heavier `74.6%`

这说明：

- 奇数位结构会放大 winner-side `delta magnitude`
- 但即使停在偶数位，winner-heavier 依旧显著

所以正确结论是：

- **交替结构解释了 winner-bias 的“形成方式”**
- **但不能单独解释它的“方向”**

方向本身仍需要：

- 弱 first-leg 偏置
- 或后续成交对 winner side 的额外偏好

### 裁决

- `same-side directional continuation drives winner-bias`：**否定**
- `alternating geometry with asymmetry contributes materially`：**A-/B+**

---

## 8. 当前最合理的来源分解

在现有证据下，最合理的分解顺序是：

### 7.1 第一层：弱 first-leg 偏置

第一笔就有约 `59-60%` 落在 eventual winner side。

这说明 xuan 并不是完全随机地选第一腿。

但这个偏置不够强，不能单独解释 `75.1% pre-MERGE winner-heavier`。

### 7.2 第二层：后续成交放大

到前 5 笔时，winner-side 净占优已经升到约 `72%`。

这比第一笔更关键。

这说明：

- xuan 的库存路径不是“第一笔决定，后面被动收尾”
- 而是“前几笔成交序列本身就在把 winner-side 优势做大”

当前更像：

- `first-leg selection` 给一个轻微初始偏向
- `fill geometry / state-dependent continuation` 把这个偏向放大

### 8.3 第三层：交替结构中的 winner 奇数位优势

当前更像：

- 并非“winner side 连续成交更多”
- 而是“在高交替序列里，winner 更容易占到第 1/3/5 笔等奇数位”

这会自然造成：

- 在 `1,3,5` 这种奇数截面上，winner-side 净头寸偏大
- 即便到偶数截面，也因为第一层偏置和成交不对称，winner 仍常保持小幅优势

这条机制说明：

- `winner-bias` 不是粗暴 directional 加仓
- 更像 pair-gated 几何内部的系统性偏差

### 8.4 第四层：278s 附近的时机兑现

`278s` 主峰桶中：

- 第一笔 winner ratio 已经更高
- 前五笔 winner-net ratio 更高
- pre-MERGE W:L 更强

这暗示：

- xuan 并不是在所有状态下一视同仁地 MERGE
- 它更可能在“winner-overweight 已经足够形成”的状态下触发主峰式 MERGE

但这里要谨慎：

- 这更像 `timing selection`
- 不是 `timing creation`

---

## 9. 证据五：`open gate timing` 可能影响 winner-side 质量

按第一笔开仓相对 round open 的时间分桶：

### `[0,30)s`

- `n = 306`
- `first winner ratio = 58.5%`
- `first5 winner-net ratio = 69.3%`
- `avg first price = 0.541`

### `[30,60)s`

- `n = 55`
- `first winner ratio = 65.5%`
- `first5 winner-net ratio = 81.8%`
- `avg first price = 0.588`

### `[60,120)s`

- `n = 42`
- `first winner ratio = 59.5%`
- `first5 winner-net ratio = 78.6%`
- `avg first price = 0.620`

后面 `120s+` 的桶样本很少，但也整体更偏高价、偏高 winner ratio。

这至少说明两件事：

1. xuan 并不是必须开盘瞬间进场；大量 round 的第一笔发生在 `0-30s` 内，但不是全部
2. **稍晚进入、价格更贵的第一腿，质量可能更高**

这更像：

- `等待更清晰的状态再开 first leg`

而不是：

- 无条件抢开仓

需要强调的是：

- 这还不能证明“越晚越好”
- 但它已经足以否定“xuan 只是毫无门槛地高频平铺开第一腿”

### 裁决

- `open gate timing likely contributes`：**B**

---

## 10. 证据六：当前更像“价格/侧别偏置”，不像“size 偏置”

### 10.1 第一腿价格越高，落在 eventual winner side 的概率越高

按第一笔价格分桶：

- `<0.4`：`first winner ratio = 33.3%`，但样本仅 `9`
- `0.4-0.5`：`50.0%`
- `0.5-0.6`：`60.5%`
- `0.6-0.7`：`67.3%`
- `>=0.7`：`72.7%`

这条梯度很重要。它说明：

- xuan 越是第一笔买在更“贵”的一侧
- 这笔越可能落在 eventual winner side

这更像：

- 对更强一侧的轻微倾向

而不是：

- 完全中性地按两边同价同量铺单

### 10.2 但第一腿 size 并没有表现出同样清晰的单调性

第一笔按 size 分桶时，winner ratio 基本都在：

- `58% ~ 62%`

附近，没有像价格那样的明显梯度。

这说明当前可见的 winner 偏置更像是：

- **side / price location bias**

而不是：

- **clip size bias**

### 10.3 前 1/3/5 笔中，winner-side trades 的价格持续高于 loser-side trades

逐笔对比：

- 第 `1` 笔：winner-side 平均价比 loser-side 高 `0.028`
- 第 `3` 笔：高 `0.106`
- 第 `4` 笔：高 `0.197`
- 第 `5` 笔：高 `0.143`

而 size premium 并不稳定：

- 有时 winner side 更大
- 有时 loser side 更大
- 幅度远不如价格差稳定

这说明：

- winner-overweight 更像是“买到更强的一侧”
- 而不是“同一侧买得更多、更大”

### 裁决

- `price / side-location bias`：**A-/B+**
- `size-led winner bias`：**弱 / 未见主导证据**

---

## 11. 证据七：第一腿方向很重要，但不是决定性的

只看有 MERGE 的 `348` 个 slug：

- 若第一笔就在 eventual winner side：
  - `pre-MERGE winner-heavier ratio = 82.9%`
  - `mean pre-MERGE delta ≈ 39.9`
- 若第一笔在 eventual loser side：
  - `pre-MERGE winner-heavier ratio = 64.3%`
  - `mean pre-MERGE delta ≈ 39.9`

这说明：

1. **第一腿方向确实重要**
   - 走对第一腿，更容易在 MERGE 时形成 winner-overweight
2. **但第一腿不是全部**
   - 即便第一腿走在 eventual loser side，后续路径仍有相当高概率恢复成 winner-heavier

所以：

- `first-leg selection` 是 contributor
- 但真正把结果推到 `75.1%` 的，仍然是后续成交路径

---

## 12. 证据八：winner-bias 与快速 completion 不是同一方向的 edge

这条是当前最重要的新约束之一。

如果 `winner-bias` 和 `completion edge` 完全同源，那么我们会预期：

- 第一腿越偏向 eventual winner
- opposite fill 应该越快出现

但当前窗口内观察到的结果是反过来的。

### 12.1 按第一腿是否押中 eventual winner 拆

若第一笔就在 eventual winner side：

- `n = 255`
- `first opposite <= 30s`：`57.6%`
- `first opposite <= 10s`：`30.2%`
- `first opposite delay p50 = 26s`
- `p90 = 124s`

若第一笔在 eventual loser side：

- `n = 170`
- `first opposite <= 30s`：`65.9%`
- `first opposite <= 10s`：`33.5%`
- `first opposite delay p50 = 20s`
- `p90 = 86s`

也就是说：

- **winner-first 更容易形成最终 winner-overweight**
- 但 **loser-first 反而更容易更快等到 opposite fill**

### 12.2 含义

这说明当前至少有两个不同维度的 edge：

1. `winner-side selection / stronger-side leaning`
2. `short-horizon opposite fillability`

它们并不自动同向。

更具体地说：

- 买在更强的一侧，可能更接近 eventual winner
- 但对侧流动性未必立刻跟上，所以 hedge completion 不一定更快

这与前文的结果是相容的：

- `W_first` 的 `pre-MERGE winner-heavier ratio` 更高
- 但 `W_first` 的 opposite delay 反而更慢

### 12.3 对研究方向的含义

这直接否定了一种过于简单的想法：

- “只要学会 winner-bias，就自然学会 30s completion”

当前更合理的判断是：

- `winner-bias` 和 `completion` 可能共享部分 open-gate 条件
- 但二者不是同一个最优化目标

因此后续研究必须把两者分开：

- 一条线研究 `winner-overweight`
- 一条线研究 `opposite fill arrival`

最后再看它们在哪些状态上重合。

### 裁决

- `winner-bias == completion-edge`：**否定**
- `winner-bias and completion partially overlap but diverge`：**A-/B+**

---

## 13. 当前不该下的结论

基于这些数据，以下结论仍然过头：

### 8.1 “xuan 已被证明有强 directional alpha”

不成立。

当前只能说：

- 它有 `winner-overweight`
- 但来源可能是：
  - 主动择边
  - 被动 fill geometry
  - 时间筛选

不能直接把它等价成“他会预测方向”。

### 8.2 “MERGE 是防 winner-bias 的安全阀”

这句话需要降温。

更准确的说法应是：

- `MERGE` 同时承担：
  - `cash realization`
  - `winner-overweight flattening`

但当前证据更像：

- 它是在兑现已经形成的偏差
- 不只是事前“防止偏差累积”

### 8.3 “我们已经知道该怎么写成策略”

也不成立。

因为当前还没有裁决：

- 这个偏差里到底有多少是可交易信号
- 多少只是 xuan 特定执行几何的副产物

---

## 14. 对 PGT 的研究含义

当前最合理的实现前研究含义是：

### 9.1 可以进入 shadow 监控的指标

- `first_leg eventual_winner_hit_rate`
- `first_5_trades winner_net_ratio`
- `pre_merge winner_minus_loser delta`
- `merge_offset_bucket vs winner_net_ratio`

这些指标适合做：

- `research telemetry`
- `shadow explain`

### 9.2 还不能进入 live 行为的逻辑

- winner-side 直接择边开仓
- 基于 `5c` 的 directional bias rule
- 把 `xuan` 重新定义成 directional strategy

### 9.3 现在最值得研究的问题

不是“他是不是 directional”，而是：

> **为什么 winner-side 的成交更容易在前几笔持续发生？**

这需要下一轮结合：

- L1 盘口
- first-leg price location
- opposite fill timing
- maybe maker/taker proxy

去拆。

---

## 15. 最终裁决

当前最稳的来源判断是：

> xuan 的 `winner-bias` 不是单一来源。它更像是 **弱 first-leg winner 倾向** 打底，再被 **高交替成交中的不对称 fill geometry（winner 更常占奇数位）** 放大，同时受 **open timing / 价格位置** 影响，最后由 `278s` 附近的 MERGE 节律 **兑现并压平**。而且它与“更快 opposite completion”并不自动同向。

因此，下一步研究不应再问：

- “xuan 到底是 neutral 还是 directional”

而应问：

- “这条 winner-overweight 是如何在高交替前几笔成交里，通过更优的 open timing / price location 被形成和放大的”
- “以及这条 winner-overweight 与快速 opposite completion 在哪些状态上一致、哪些状态上冲突”

在这个问题澄清之前，`winner-bias` 仍然是 **研究观察事实**，不是 **可直接编码的策略规则**。
