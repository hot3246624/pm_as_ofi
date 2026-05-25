# Symmetric Activation Projected Residual Tail Join Summary

日期：2026-05-25

## 结论

`symmetric_activation_projected_residual_tail_join_summary_v1` 已完成默认关闭实现、fixture smoke 与 scorer 验证。

这一步只解决一个诊断缺口：把已实现的 realized residual / pair tail attribution，按下单前可见的 `projected_residual_bucket` 重新聚合，判断 edge0.075/window20 的 residual 失败是否集中在高 projected residual 风险桶里。

它不是交易规则，不改变 runner 默认行为，不是 private truth、deployable、promotion 或 canary evidence。

## 新增 runner flag

`--symmetric-activation-projected-residual-tail-join-event-lite-summary`

依赖：

- 必须同时启用 `--event-lite-summary`
- 默认关闭
- 不改变 candidate/block/fill/pairing 决策

## Summary 字段

新增 `event_lite.symmetric_activation_projected_residual_tail_join_summary`：

- `schema_version=symmetric_activation_projected_residual_tail_join_summary_v1`
- `pair_qty_sum_by_status_reason_projected_residual_bucket`
- `residual_qty_sum_by_status_reason_projected_residual_bucket`
- `residual_cost_sum_by_status_reason_projected_residual_bucket`
- `residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket`
- `source_sequence_presence_by_status_reason_projected_residual_bucket`

字段合同固定：

- `default_off=true`
- `post_action_outcome_labels_included=false`
- `realized_pair_cost_used_as_live_criteria=false`
- `trading_behavior_changed=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate_passed=false`

## 验证

新增 scorer：

- `scripts/xuan_symmetric_activation_projected_residual_tail_join_summary_scorer.py`

新增 smoke：

- `scripts/xuan_symmetric_activation_projected_residual_tail_join_summary_smoke.sh`

smoke 验证：

- 默认关闭时 summary absent
- 启用后 summary present
- pair/residual aggregate parity
- source_sequence coverage present
- CLI 依赖 fail-closed
- no behavior change

## 当前状态

上一轮真实 no-order pullback 生成时尚未启用该新 summary，因此不能用这一步反证 edge0.075/window20 已可部署。当前真实策略状态仍是：

`UNKNOWN_SYMMETRIC_ACTIVATION_EDGE0075_NO_ORDER_REVIEW_RESIDUAL_RISK_BUDGET_FAIL`

下一步只能在未来 allowlisted pullback 已包含该 summary 时评分，或在用户明确批准 exact bounded no-order diagnostic 后再获取新样本。
