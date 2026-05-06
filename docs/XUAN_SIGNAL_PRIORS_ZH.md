# Xuan 当前信号先验与欠识别特征

生成时间：`2026-04-27`

本文基于：

- [docs/xuan_completion_gate_summary.json](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/xuan_completion_gate_summary.json)
- [data/xuan/xuan_completion_episodes.csv](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/data/xuan/xuan_completion_episodes.csv)

目标不是宣布“已经懂了 xuan”，而是把当前能用的弱先验、不能用的伪特征、以及对实现最有价值的限制条件写清楚。

## 1. 当前最重要的总判断

### 1.1 现有 `xuan completion gate` 仍然没有 holdout lift

当前 summary 明确显示：

- `recent_overlap_episode_count = 17`
- `holdout_lift_pct = 0.0`

这意味着：

- 这版 gate 只能当 `bootstrap / provisional shadow prior`
- 不能把它当成已经学到了 xuan 的 opening rule

### 1.2 问题不只是“样本少”，而是“很多特征当前根本欠识别”

新增的 `feature_identifiability` 很关键。它表明当前 overlap truth 下，有些特征根本没有有效变化，或者 recent window 太薄，不具备学习资格。

### 1.3 当前首要瓶颈首先是时间覆盖，不是复杂建模

`censor_stats` 已经把这个点钉死了：

- 总 episode：`1196`
- 可用 overlap：`208`
- censored：`988`
- 其中 `979` 个原因是 `no_l1_book`
- 只有 `9` 个是 `stale_book`

这说明：

- 当前研究最大的缺口不是“模型太弱”
- 而是公开 L1 capture 覆盖的天数还太短
- 只靠这两天的 overlap，很容易把 coverage 缺口误判成策略谜团

## 2. 当前明确欠识别的特征

以下特征在当前 overlap truth 中，**不应该继续被当成主学习对象**：

- `prior_imbalance_bucket`
  - full/recent 都只有一个桶：`lt_0.02`
- `same_side_run_before_open`
  - full/recent 全是 `0`
- `utc_hour_bucket`
  - full 有变化，但 recent `17/17` 全落在 `0`
- `l1_spread_ticks_first_side`
  - `198/208` 落在 `le_1.5`
- `l1_spread_ticks_opposite_side`
  - `198/208` 落在 `le_1.5`

这几条的直接含义是：

- 现阶段不要把 `prior_imbalance` 当作核心 gate 证据
- 不要假装已经从公开 truth 学到了 `same_side_run_before_open`
- 不要从当前 recent-only 样本里推导 session gating
- 不要把 “spread <= 1.5” 继续神化成有效筛选器；它更像数据常态，而不是区分信号

### 2.1 欠识别不等于没用

这里要特别小心一个误区：

- `欠识别` 是指当前不能把它当作稳定的排序特征去学 lift
- 不代表它不能作为单侧 guardrail

当前 overlap truth 已经给出两类单侧信息：

- `same_side_run_before_open = 0` 在 `208/208` overlap episode 中从未被打破
- `prior_imbalance_bucket = lt_0.02` 在 `208/208` overlap episode 中从未被打破

这更像：

- 它们适合继续保留为 `eligibility / hard guardrail candidate`
- 不适合继续做加分项或精细分桶打分

类似地：

- `l1_spread_ticks_{first,opposite}_side` 超过 `1.5` 的样本极少

所以 spread 目前更像：

- `hard ceiling candidate`
- 而不是一个值得细调 score 的核心维度

## 3. 当前可以保留的弱先验

这些不是 enforce 级规则，但已经足够指导 shadow 解释优先级。

### 3.1 `recent_opposite_trade_rate_5s` 值得保留

full overlap 下：

- `> 8.2/s` 桶：`51` 样本，`hit_rate = 43.1%`，相对 baseline `+11.9pct`
- `<= 5.4/s` 桶：`55` 样本，`hit_rate = 21.8%`，相对 baseline `-9.4pct`

recent overlap 下：

- `<= 3.0/s` 桶：`4` 样本，`hit_rate = 0%`
- `4.2-5.4/s` 附近桶：`5` 样本，`hit_rate = 60%`

这说明：

- “对侧近期成交活跃度”仍然是最值得保留的 `Completion Controller` 候选特征之一
- 但 recent 样本太少，还不能把具体阈值写死

### 3.2 `first_leg_clip` 不能走极小 clip 假说

full overlap 下：

- `<= 22.29` 桶：`53` 样本，`hit_rate = 13.2%`
- `108.07-140.72` 桶：`51` 样本，`hit_rate = 43.1%`

这至少说明一件事：

- 当前公开 truth **不支持“越小越安全、越容易 30s 配对”** 这个想象

但这也不能被误读成“clip 越大越好”，因为：

- `> 140.72` 桶又回落到接近 baseline

更稳妥的解释是：

- xuan 更像在状态允许时使用中等到偏大的 clip，而不是固定小 clip 慢慢试

### 3.3 `maker_proximity` 并不代表更高的 `30s completion`

当前很重要、也很容易被误读的结果：

full overlap 下：

- `taker_proximity`：`70` 样本，`hit_rate = 41.4%`
- `maker_proximity`：`135` 样本，`hit_rate = 25.9%`

recent overlap 下：

- `taker_proximity`：`9` 样本，`hit_rate = 44.4%`
- `maker_proximity`：`8` 样本，`hit_rate = 25.0%`

这并不推翻 “xuan 总体 maker-leaning mixed” 的画像，但它明确说明：

- `maker-like` 不等于 `30s completion` 更强
- 如果我们的目标是先逼近 `30s completion`，那 `maker` 应继续是 `nice-to-have`，不是主优化目标

### 3.4 时间位置依然值得保留，但不能过拟合成单一时段公式

full overlap 下：

- `round_open_rel_s <= 16`：`37.3%`
- `16-66`：`21.7%`
- `66-174`：`40.4%`
- `> 174`：`23.5%`

这说明：

- xuan 不是“任何时刻都一样好”
- 但也不是“只要越早越好”

更像是：

- 存在一组状态窗口，在这些窗口里 first leg 更容易在 `30s` 内看到 opposite
- 时间只是窗口代理变量，不是完整解释

## 4. 当前 gate 本身也暴露了不稳定性

`score_bucket` 的表现本身就提示这版 gate 还没有学对。

full overlap：

- `upclip`: `33.9%`
- `full_clip`: `28.7%`

recent overlap：

- `full_clip`: `50.0%`
- `upclip`: `25.0%`

也就是说：

- full 和 recent 的方向不稳定
- 当前 score 体系还不足以作为 enforce 候选

这比单纯说“样本少”更具体：

- 现有特征组合没有形成稳定排序能力

## 5. 对实现最有价值的翻译

### 5.1 现在就该保留为 shadow 优先解释项

- `recent_opposite_trade_rate_5s`
- `recent_total_trade_rate_15s`
- `round_open_rel_s`
- `first_leg_clip`
- `maker_taker_proxy` 作为 explain 项，而不是目标项

### 5.2 现在不要再作为主优化抓手

- `prior_imbalance`
- `same_side_run_before_open`
- recent-only `session gate`
- “spread <= 1.5 就说明是机会”

### 5.3 对 `completion-first-v2-shadow` 的直接启示

- 先把差距定位成 `timing / clip / fillability`
- 不要把“更 maker”误判成“更接近 xuan”
- 不要在当前 truth 下继续花时间打磨 `pair target` 式分数细节

## 6. 新真值到位后最该优先解决什么

下一批真值最该解决的不是“再多一些样本”这么空的话，而是这 4 个特征缺口：

1. 给 `same_side_run_before_open` 真正制造可观测变化
2. 拿到更多 `spread > 1.5` 的有效样本
3. 扩大 recent 窗口的 session 覆盖
4. 拿到我方 fillability 真值，用来判断为什么 `maker-like` 反而更慢

## 7. 结论

当前最重要的研究结论不是“xuan 的公式是什么”，而是：

1. 现有 gate 没有 holdout lift
2. 若干传统 gate 特征当前处于欠识别状态
3. 真正还值得继续压的，是 `trade cadence / clip regime / timing / fillability`
4. `maker-leaning` 应继续留在画像层，而不是 30s completion 的主优化目标

这会直接影响我们后续的实现顺序：先研究 `completion probability`，再研究是否能用更 maker 的方式实现，而不是反过来。
