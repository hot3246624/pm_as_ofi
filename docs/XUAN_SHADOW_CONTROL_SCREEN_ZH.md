# Xuan Shadow 控制组筛查口径

生成时间：`2026-04-27`

这份文档解决的是一个非常具体的问题：

> 当 `completion-first` 的 shadow 报表开始变好时，我们怎么知道这是不是在逼近 `xuan`，而不是在滑向别的 archetype？

答案是：先做 `control screen`，再谈 gap。

## 1. 为什么要单独做一层筛查

仅看这些指标是不够的：

- `30s_completion_hit_rate`
- `clean_closed_episode_ratio`
- `merge/redeem` 次数
- “看起来像 maker”

因为这些现象并不只属于 `xuan`。它们在：

- `late_grammar`
- `quietb27`
- `gabagool22`
- `silent_d189`

身上也会以不同组合出现。

所以控制组筛查的职责不是证明“已经接近 xuan”，而是先排除：

- 这其实是 `directional round picker`
- 这其实是 `multi-venue slow merge`
- 这其实是 `fixed-clip post-close cleanup`

## 2. 筛查输出的四种状态

`control_screening` 中的状态分两类：

对于 `must-match / nice-to-have`：

- `pass`
- `watch`
- `fail`
- `insufficient`

对于 `anti-target`：

- `clear`
- `warning`
- `insufficient`

解释固定如下：

- `pass`：当前 shadow 至少没有违背这条 xuan 不变量
- `watch`：当前结果没有直接跑偏，但仍不足以当作逼近 xuan 的证据
- `fail`：当前结果已经更像别的 archetype
- `clear`：当前没有触发这条 anti-target
- `warning`：当前已经出现明显的 anti-target 信号
- `insufficient`：现有 shadow / public truth 口径还不足以回答，必须等新真值

## 3. 当前可直接量化的 must-match

### 3.1 `single_venue_btc_5m`

判据：

- 当前 shadow rows 是否全部来自 `btc-updown-5m`

含义：

- 如果不是单 venue `BTC 5m`，就已经偏离 xuan 主战场

### 3.2 `low_directionality`

判据：

- `same_side_add_qty_ratio_p90 <= 0.10`：`pass`
- `0.10 < ratio <= 0.20`：`watch`
- `ratio > 0.20`：`fail`

含义：

- same-side drift 太高，默认更像 directional 污染，而不是 completion-first

### 3.3 `in_round_completion`

判据：

- `30s_completion_hit_rate >= 0.30`
- 且 `clean_closed_episode_ratio_median >= 0.90`

只有同时满足才算 `pass`。

含义：

- 如果 clean close 看起来还行，但 `30s completion` 太差，这更像“事后收拾得不错”，不是 xuan 的主优势

### 3.4 `state_selected_clip`

判据：

- `score_bucket_distribution` 至少有 `2` 个非零 bucket
- 且最大 bucket 占比 `< 95%`

含义：

- 如果始终只有一个 score bucket 在工作，说明 clip 更像固定档位，而不是状态感知

## 4. 当前仍然只能部分裁决的项

### 4.1 `high_round_coverage`

目前只能做弱裁决：

- 如果 `open_candidate_total = 0`，直接 `fail`
- 其余情况先标 `insufficient`

原因：

- 真正的 “高 coverage” 必须结合 `xuan` 的 recent/public round coverage 基准，当前 truth 还不够厚

### 4.2 `maker_leaning_execution`

先标 `insufficient`。

原因：

- 目前我们对 `xuan` 还是 `maker-leaning mixed` 的 `B` 级 proxy
- 对我方 shadow，也还没有足够细的 fillability 真值把 maker/taker 定性得更硬

### 4.3 `in_round_merge_capital_recycling`

先标 `insufficient`。

原因：

- merge/redeem 次数本身不能说明它是 xuan-like 还是 late_grammar-like
- 必须结合更细的 tranche/capital lifecycle

## 5. 当前可直接识别的 anti-target

### 5.1 `multi_venue_slow_merge_spread`

如果 `single_venue_btc_5m` 失败，直接 `warning`。

### 5.2 `fixed_clip_post_close_batch_merge`

同时满足以下条件时，标 `warning`：

- `state_selected_clip` 失败
- `30s_completion_hit_rate < 0.30`
- 且有 `merge/redeem` 活动

含义：

- 这类结果更像靠固定 clip 和事后清算把报表做平，而不是 round 内配对

### 5.3 `selective_directional_round_picker`

当前先做保守裁决：

- 若 `low_directionality` 失败，标 `watch`
- 若未来加入 coverage 真值，再升级为硬裁决

## 6. 使用顺序

后续统一按这个顺序解释 shadow 结果：

1. 先看 `control_screening`
2. 再看 `gap_vs_xuan`
3. 最后才看更细的 episode/market drill-down

如果 `control_screening` 已经显示明显 `anti-target`，那么即使某些表层指标变好，也不能宣称“逼近 xuan”。
