# Xuan 研究问题树与证据矩阵

生成时间：`2026-04-27`

## 1. 固定问题树

当前只回答 5 个模块，不再扩散：

1. `Open Gate`：什么状态下开第一腿
2. `Completion Controller`：为什么有些状态更容易在 `30s` 内补到 opposite
3. `First-Leg Sizing`：首腿大小如何随状态变化
4. `Re-entry / Cooldown`：为什么上一轮结束后不立即再开
5. `Abort / Repair`：什么时候停止等待并支付更贵的 completion

优先级解释：

- 前 `3` 项直接决定 `30s completion` 是否可复制。
- 后 `2` 项主要影响尾部平滑度、资金利用和真实执行成本。

## 2. 证据矩阵

| 模块 | 必答问题 | 公开侧可用数据 | 我方真值数据 | 主指标 | 证伪条件 |
|------|----------|----------------|--------------|--------|----------|
| `Open Gate` | 什么盘口/时段允许开 first leg | `md_book_l1`, `md_trades`, `xuan_trades`, `xuan_activity`, `market_meta` | `own_fill_events`, `own_inventory_events` | `open_allowed_count`, `score_bucket_distribution`, `session_bucket_distribution` | gate on cohort 与 baseline 在 `30s_completion_hit_rate` 上无提升 |
| `Completion Controller` | 为什么有些 first leg 能在 `30s` 内出现 opposite | `md_book_l1`, `md_trades`, `xuan episode truth` | `own_fill_events` | `30s_completion_hit_rate`, `median_first_opposite_delay_s`, `same_side_before_opposite_ratio` | 状态特征与 `30s` 命中率无稳定关系 |
| `First-Leg Sizing` | clip 是否随状态质量变化 | `xuan_trades`, `md_book_l1`, `md_trades` | `own_fill_events`, shadow report | `clip bucket vs completion rate`, `score_bucket_distribution` | clip 增减与命中率、延迟无方向一致的关系 |
| `Re-entry / Cooldown` | 为什么有些轮次结束后立刻再开，有些不参与 | `xuan_trades`, `xuan_activity`, `market_meta` | `own_inventory_events` | `round coverage`, `session participation`, `inter-episode gap` | 重开间隔只由随机波动解释，和状态无关 |
| `Abort / Repair` | 什么情况下停止等待被动补腿 | `xuan_activity`, `md_trades` | `own_fill_events`, `own_inventory_events` | `repair timing`, `high-cost completion share` | repair 时点与库存压力/对侧可成交性无关 |

## 3. 数据可回答边界

只靠公开数据，当前已经可以回答：

- `xuan` 是否固定 `pair target`：不能成立
- `xuan` 是否只靠尾盘公式：不能成立
- `xuan` 是否存在明显 `session gating`：可以部分回答
- `xuan` 的 `maker/taker` 是否纯单一路径：不能成立，当前只支持 `maker-leaning mixed`

必须等我方真值才能回答：

- 同类状态下，我们自己的单为什么没在 `30s` 内配上
- 我方 `fillability` 与 xuan 公开行为的差距是 timing、clip，还是 execution
- repair / cooldown / re-entry 的成本是否真的值得

即使有我方真值仍无法回答：

- `xuan` 的真实挂单/撤单时间线
- `xuan` 的 queue position
- `xuan` 的 fee tier / rebate tier
- `xuan` 的基础设施延迟优势

## 4. 默认裁决顺序

新数据到位后，默认只按这个顺序解释：

1. 为什么候选没有被放行
2. 为什么放行后没有在 `30s` 内出现 opposite
3. 为什么出现 opposite 后仍未在 `60s` 内 clean close

任何绕开这三个问题、直接讨论“大而全 PnL”或“某个神奇价差公式”的分析，默认视为降级材料。

## 5. 控制组约束

新增一条硬约束：所有关于 “更接近 xuan” 的表述，都必须先通过控制组筛查。

控制组总原则见：

- [XUAN_ARCHETYPE_INVARIANTS_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_ARCHETYPE_INVARIANTS_ZH.md)

默认解释规则：

- 如果某个现象同样常见于 `late_grammar / quietb27 / gabagool22 / silent_d189`，它只能算“大类共性”，不能算 xuan 证据。
- 如果某个现象更接近 `selective directional`、`multi-venue spread` 或 `fixed-clip post-close merge`，默认视为 `anti-target`。
- 只有同时满足 `单 venue BTC 5m + 高 coverage + 低 directional + in-round completion + 状态感知 clip`，才允许表述为“逼近 xuan”。
