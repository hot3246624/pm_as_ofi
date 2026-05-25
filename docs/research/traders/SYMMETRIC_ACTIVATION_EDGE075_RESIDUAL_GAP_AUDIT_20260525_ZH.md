# Symmetric Activation Edge0.075 Residual Gap Audit

## 结论

`symmetric_activation_edge0075_actoppseen20_no_order_review` 的 residual 风险失败不是 instrumentation failure，也不是 pair economics 全负；它是 promotion risk-budget blocker。当前 local-only audit 给出：

`KEEP_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_PROJECTED_RESIDUAL_JOIN_TARGET_READY`

这不是策略通过、不是 canary/deploy/private truth。它只说明下一步有一个合法的默认关闭诊断目标。

## 关键指标

- candidates: `233`
- pair_pnl: `+5.22675`
- fee_adjusted_pair_pnl_proxy: `+4.579513`
- net_pair_cost_p90: `0.99`
- residual_qty_share: `29.650083%`
- residual_cost_share: `26.014802%`
- pair_tail_loss_share: `0%`
- promotion_gate.passed: `false`

## Residual 分布

Residual 主要集中在 activation 维度，而不是 pair-cost tail：

- `activation_not_required`: residual qty share `54.6103%`, residual cost share `60.3528%`
- `activation_required_activation_opp_age_5_15s`: residual qty share `31.0186%`, residual cost share `28.7461%`

同时，admitted candidates 中 `projected_residual_gt_20pct` 占 `34.7639%`。这说明 residual failure 可能来自合法的 pre-action residual pressure，但当前 tail attribution summary 还不能把 realized residual 直接 join 到 projected residual bucket。

## 下一步目标

实现默认关闭 `symmetric_activation_projected_residual_tail_join_summary_v1`，只做诊断，不改变交易行为。所需字段：

- `residual_qty_sum_by_status_reason_projected_residual_bucket`
- `residual_cost_sum_by_status_reason_projected_residual_bucket`
- `residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket`
- `pair_qty_sum_by_status_reason_projected_residual_bucket`
- `source_sequence_presence_by_status_reason_projected_residual_bucket`

明确禁止：

- 不把 realized pair cost 当 live criteria
- 不加 YES/NO side cap
- 不加 offset cap
- 不加 public-price cap
- 不恢复 D+ failed micro-deficit/ledger/tiny-deficit/fill-to-balance families
- 不启动 shadow/canary/live
