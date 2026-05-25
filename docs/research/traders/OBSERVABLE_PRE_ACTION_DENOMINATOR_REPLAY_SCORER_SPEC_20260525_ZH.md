# Observable Pre-Action Denominator Replay Scorer Spec

时间：2026-05-25 19:55 UTC

## 结论

`KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_READY`

feature-join exporter 和 output scorer 已经就绪。本轮定义下一层 denominator replay scorer：
冻结一个 pre-action predicate 之后，如何用同窗口 feature-join rows 复现 no-order denominator，并生成可被
`observable_pre_action_rule_miner_input_v1` 验证的输入 manifest。

这仍是 local-only spec，不是策略证据，也不允许启动 no-order diagnostic。

## Future Scorer Target

目标：

`observable_pre_action_rule_miner_denominator_replay_scorer_v1`

输入：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `frozen_observable_pre_action_rule_manifest.json`
- `same_window_aggregate_summary.json`

输出：

- `observable_pre_action_no_order_denominator_summary.json`
- `observable_pre_action_rule_miner_input_manifest.json`

## Frozen Rule Contract

冻结规则 manifest schema：

`frozen_observable_pre_action_rule_manifest_v1`

必须包含：

- `frozen_rule_id`
- `rule_version`
- `created_from_train_manifest`
- `holdout_manifest`
- `predicate_expression`
- `predicate_feature_names`
- `allowed_live_feature_families`
- `forbidden_live_feature_families`
- `train_label_fields`
- `holdout_label_fields`
- `same_window_scope`

允许进入 live predicate 的字段族只限 pre-action observable：

- status / reason / block_reason
- source presence
- side / offset bucket
- pre-seed same/opp/open/deficit qty buckets
- candidate qty bucket
- source risk direction

禁止进入 live predicate：

- source_pair / source_residual
- realized pair cost
- settlement/redeem/future label
- private truth
- public-profile single-day profit
- static side/offset/public-price cap
- D+ failed-family marker

## Denominator Summary Contract

目标 schema：

`observable_pre_action_no_order_denominator_summary_v1`

必须包含：

- `frozen_rule_id`
- `same_window_id`
- `same_window_denominator_count`
- `rule_match_count`
- `admitted_count`
- `blocked_count`
- `source_sequence_coverage`
- `quote_intent_presence_rate`
- `source_order_presence_rate`
- `aggregate_parity`
- `field_contract`

最低门槛：

- denominator count `>=100`
- rule match count `>=20`
- source sequence coverage `>=0.95`

aggregate parity 必须证明：

- row count 等于 source summary row count
- admitted + blocked 等于 denominator count
- source presence counts 可由 rows 重建

## Smoke

真实 spec artifact：

`xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_20260525T195536Z/manifest.json`

smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_smoke_20260525T195536Z/manifest.json`

smoke 覆盖：

- good feature-join output scorer manifest 通过
- bad feature-join scorer fail closed
- private/deployable claim fail closed
- missing feature-join scorer manifest fail closed

## 状态

`current_real_input_status=UNKNOWN_DENOMINATOR_REPLAY_SCORER_NOT_IMPLEMENTED`。

该 KEEP 是 denominator replay scorer spec readiness，不是 strategy evidence。

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，
`promotion_gate.passed=false`，`private_truth_ready=false`，`deployable=false`。

## 下一步

实现 local-only `observable_pre_action_rule_miner_denominator_replay_fixture_scorer_v1`：用 synthetic frozen-rule
和 feature-join fixtures 生成 denominator summary 与 input manifest，先证明 fail-closed 机制完整；仍不得自动启动
no-order diagnostic。
