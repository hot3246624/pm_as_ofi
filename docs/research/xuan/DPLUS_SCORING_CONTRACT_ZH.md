# Xuan B27 D+ Scoring Contract

更新时间：2026-05-20

本文只定义 `xuan_b27_dplus` 研究、shadow 和 promotion 前的评价口径。它不是部署许可，也不改变任何 runner 行为。

## 核心原则

D+ 的首要目标是风险调整后的净经济收益，而不是单项 residual 或单项 p90 指标。

最终经济目标应优先写成：

```text
economic_pnl =
  pair_pnl
  + residual_settle_or_mark_pnl
  - official_fee
  - stress_or_impact_budget
```

如果没有 source-of-truth residual settlement，只能使用保守代理：

```text
stress_pnl =
  pair_pnl
  - residual_cost
  - stress_or_impact_budget
```

因此，`residual_qty_rate` 或 `net_pair_cost_p90` 单项变差，不等于策略经济失败。它只说明风险形态变差，必须放回净 PnL、压力 PnL、样本和容量里一起判断。

## 三层评价

### 1. Research mechanism ranking

用途：比较一个机制是否比当前 control 更值得继续研究。

主指标：

- `fee_after_pnl`
- `stress100_worst_pnl`
- `worst_day_fee_after_pnl`
- `pair_qty`
- `seed_actions` / `candidates`
- `active_markets`

风险调整指标：

- `residual_qty_rate`
- `residual_cost_rate`
- `worst_residual_net_pnl`
- `weighted_pair_cost`
- qty-weighted 或 loss-weighted pair-cost tail，如果可得

判定口径：

- 净 PnL / stress / worst-day 为负，可以判定经济失败。
- 净 PnL / stress / worst-day 为正，但 residual 或 pair-cost tail 相对 control 恶化，应标记为 `TRADEOFF` 或 `MECHANISM_WEAKER`，不要写成策略经济失败。
- 相对 control 只带来极小 pair-cost 改善，同时 residual 恶化，可以 discard 该机制，但必须说明 discard 的是 mechanism，不是策略总体。

### 2. Shadow / canary hard gate

用途：防止把研究结果误推到 G2 canary 或 live。

no-order shadow acceptance 的硬 gate 继续保守，但主风险阈值不应再是固定 `residual_qty` 或 action-level `net_pair_cost_p90` 一票否决，而应转成归一化风险预算：

- no-order safety 必须通过。
- market scope / path safety 必须通过。
- candidates 必须足够大，当前默认 `>=100`。
- residual qty budget：`residual_qty / filled_qty`。
- residual cost budget：`residual_cost / filled_cost`。
- pair-tail budget：`max(net_pair_cost_p90 - 1, 0) * pair_qty * tail_fraction / fee_adjusted_pair_pnl`，直到 runner 有 qty-weighted pair-tail distribution 前作为保守 proxy。
- `material_residual_lots = 0`。
- pair PnL / ROI 非负。

旧的 `residual_qty <= 10`、`residual_cost <= 5`、`net_pair_cost_p90 <= 1.0` 只能保留为 legacy reference，帮助和历史 artifact 对齐；promotion/canary 结论以归一化 budget 为准。这些是 promotion blockers，不是最终经济目标函数。一个 run 因 residual 或 p90 tail 未过 hard gate，可以写 `BLOCKED_PROMOTION_RISK_BUDGET` 或 `UNKNOWN_SHADOW_RISK_BUDGET_FAIL`；如果它仍有正 PnL，不应写成经济失败。

### 3. Final economic objective

用途：决定策略是否真的值得部署或扩大。

必须同时满足：

- 正的 fee-after PnL。
- 正的 conservative residual stress PnL。
- 日级 / market 级尾部不靠单个样本侥幸抵消。
- OOS / walk-forward / source-of-truth replay 不翻负。
- 官方手续费、资金占用、冲击和真实 fill/queue/redeem truth 已被纳入。

最终 objective 可以容忍 residual 不是最低，只要 residual 风险被利润稳定覆盖；也可以拒绝高参与率配置，即使名义 PnL 高，只要尾部成本不可控。

## 指标冲突

| 指标 | 正向意义 | 常见冲突 | 处理方式 |
| --- | --- | --- | --- |
| `pair_pnl` | 直接利润来源 | 可被高残仓风险掩盖 | 必须扣 residual / fee / stress 后再判断 |
| `pair_qty` / `pair_actions` | 放大利润与样本 | 会增加库存暴露和 tail | 作为容量指标，不单独 PASS |
| `candidates` / `seed_actions` | 样本与参与率 | 放宽后通常拉高 residual/p90 | 用 sample retention + risk budget 共同评价 |
| `net_pair_cost_p90` | pair 成本尾部 | action-weighted p90 可能被小 qty 扭曲 | 优先升级为 qty-weighted tail loss |
| `residual_qty` / `residual_cost` | 未配对库存风险 | 固定阈值可能误杀正经济策略 | promotion 可硬 gate，research 应看 stress coverage |
| `market participation` | 容量与可扩展性 | 低质 market 会带来尾部 | 必须和 day/market tail attribution 绑定 |

## Label 约定

推荐后续 artifact 使用更精确标签：

- `KEEP_ECONOMIC_RESEARCH_ONLY`：经济指标和风险预算都支持继续研究，但还不是 promotion。
- `TRADEOFF_ECON_POSITIVE_RISK_WORSE`：净 PnL 为正，但 residual/p90 相对 control 恶化。
- `DISCARD_MECHANISM_WEAKER`：机制相对 control 没有足够增量，冻结该机制；不等于策略总体失败。
- `DISCARD_ECONOMIC_NEGATIVE`：fee-after / stress / worst-day 经济指标为负。
- `BLOCKED_PROMOTION_RISK_BUDGET`：经济可能为正，但 shadow/canary 风控硬 gate 未过。
- `BLOCKED_SAMPLE_THIN`：样本不足，不允许 promotion，但不评价经济性。

## Gate Manifest Contract

`scripts/xuan_b27_dplus_shadow_trading_acceptance.py` 必须同时输出两套结果：

- `research_ranking`：服务研究排序，核心字段是 `label`、`fee_adjusted_pair_pnl_proxy`、`risk_adjusted_pnl_proxy`、`sample_size_ok`、`promotion_risk_budget_ok`。正经济但风险 budget 未过时，应标为 `TRADEOFF_*`，不是经济失败。
- `promotion_gate`：服务 shadow/canary 阻断，核心字段是 `passed`、`hard_blockers`、`normalized_risk_budget` 和 `required_before_g2_canary=true`。只有 `promotion_gate.passed=true` 才允许进入 canary 讨论。

顶层 `acceptance_passed` 仍只代表 promotion gate 通过，不代表 research ranking；顶层 `status=FAIL_*` 也只代表 promotion blocker。后续自动化和 subagent 必须读这两个子对象，不能只看 `status` 推断策略经济质量。

## 当前应用

截至 2026-05-20：

- `late_repair90` 仍是当前最强 local completion research mechanism：它显著降低 residual，并保持 fee-after / stress / worst-day 为正。
- hard `public_trade_px_hi=0.55`、all-side risk-increasing public px cap、NO-side-only risk-increasing public px cap 都应冻结为 source-public-price cap family。它们不是经济净 PnL 失败，而是相对 `late_repair90` 的机制增量不足。
- 下一步不应再围绕 source public price 做 cap sweep。应寻找新的非价格库存路径机制，例如 dynamic target、pair opportunity quality、repair sequencing 或 side imbalance budget。
