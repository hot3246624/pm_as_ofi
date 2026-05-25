# Observable Pre-Action Denominator Replay Fixture Scorer

结论：`KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY`。

本轮只实现 local-only fixture scorer，用 synthetic frozen-rule 与 feature-join fixtures 证明 denominator replay 合同可以被执行并 fail closed。它不是策略收益证据，不允许启动 no-order diagnostic，也不证明 private truth、deployable 或 promotion。

## 新增内容

- `scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py`
- `scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_smoke.sh`
- artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_20260525T202536Z/manifest.json`
- smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_smoke_20260525T202536Z/manifest.json`

## Scorer 合同

输入只允许当前 worktree 内的 materialized fixture/source files：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `frozen_observable_pre_action_rule_manifest.json`
- `same_window_aggregate_summary.json`

输出：

- `observable_pre_action_no_order_denominator_summary.json`
- `observable_pre_action_rule_miner_input_manifest.json`

冻结 predicate 只能使用 pre-action observable features，例如 `source_sequence_present`、`quote_intent_present`、`source_order_present`、`status_before_action`、`block_reason`、`side`、`offset_bucket`、pre-seed/open/deficit/candidate qty buckets 与 `source_risk_direction`。

`source_pair/source_residual`、realized pair cost、settlement/redeem/future labels、private truth、public-profile single-day profit、static side/offset/public-price cap 与 D+ failed-family marker 只能作为 forbidden live features，不能进入 frozen predicate。

## Fixture 结果

good fixture:

- denominator rows: `1200`
- admitted/blocked: `600/600`
- rule matches: `400`
- source_sequence coverage: `1.0`
- quote_intent/source_order presence rate: `0.5/0.5`
- aggregate parity: `true`

生成的 input manifest 已通过 `scripts/xuan_observable_pre_action_rule_miner_spec.py`，结果为 `KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_FIXTURE_PASS_NOT_REAL`。

## Fail-Closed 覆盖

smoke 覆盖以下 fail-closed 条件：

- 缺 `frozen_rule_id`
- predicate 使用 forbidden live feature `realized_pair_cost`
- denominator sample 低于阈值
- source_sequence coverage 低于阈值
- same-window aggregate mismatch
- frozen rule 声称 `deployable=true`

所有路径均保持 `strategy_evidence=false`、`no_order_diagnostic_allowed=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

下一步 local-only 是 `observable_pre_action_rule_miner_real_input_inventory_v1`：只盘点当前 worktree 内是否已经有非 fixture materialized source 能满足 feature-join + denominator replay + miner input contract。若没有，就保持 UNKNOWN 输入缺口；不得自动启动 no-order diagnostic。
