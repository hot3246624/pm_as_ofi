# Observable Pre-Action Rule Miner Feature-Join Runner Instrumentation

时间：2026-05-25 19:55 UTC

## 结论

`KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY`

已实现默认关闭的 materialized feature-join exporter。它只在显式启用
`--observable-pre-action-rule-miner-feature-join-output` 时输出 allowlisted 文件；默认关闭时不产生
`observable_pre_action_*` 文件，也不改变 candidate/block/fill/pair 计数。

## 实现

修改文件：

- `tools/xuan_dplus_passive_passive_shadow_runner.py`
- `scripts/xuan_observable_pre_action_rule_miner_feature_join_runner_instrumentation_smoke.sh`

新增 CLI：

`--observable-pre-action-rule-miner-feature-join-output`

依赖：

- `--event-lite-summary`
- `--source-link-transition-event-lite-summary`

开启后，每个市场先写 per-slug materialized 文件，再由 `aggregate()` 合并成顶层 allowlisted 文件：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`

这些文件来自 runner 内存里的 `record_source_link_transition(...)` pre-action transition，不读取 events JSONL。

## Row-Level 字段

每条 row 使用 schema `observable_pre_action_candidate_row_v1`，包含：

- `condition_id`、`market_slug`、`day_id`、`quote_ts_ms`
- `side`、`offset_s`、`offset_bucket`
- `status_before_action`、`reason`、`block_reason`
- `source_sequence_present`、`quote_intent_present`、`source_order_present`
- redacted id policy 字段，不导出原始 id
- `pre_seed_same_qty/pre_seed_opp_qty`
- `pre_seed_same_cost/pre_seed_opp_cost`
- `pre_seed_open_qty/pre_seed_open_cost/pre_seed_deficit_qty`
- `candidate_qty`
- open/deficit/candidate qty buckets
- `source_risk_direction`

field contract 固定：

- `post_action_outcome_labels_included=false`
- `realized_pair_cost_used_as_live_criteria=false`
- `trading_behavior_changed=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate_passed=false`

## Smoke

smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_rule_miner_feature_join_runner_instrumentation_smoke_20260525T195536Z/manifest.json`

验证结果：

- default-off 不产生 per-slug 或顶层 `observable_pre_action_*` 文件。
- enabled 产生 per-slug 文件和顶层合并文件。
- 顶层 `candidate_rows` 共 `5` 行：`2` admitted、`3` blocked。
- candidate count、block count、queue-supported fills、pair actions 与 default-off 完全一致。
- row-level source presence、status、block reason、pre-seed/open/deficit/candidate qty fields 均存在。
- CLI 缺 `--event-lite-summary` 或缺 `--source-link-transition-event-lite-summary` 均 fail closed。
- safety/field_contract 均保持 research-only。

## 状态

该 KEEP 是 instrumentation readiness，不是 strategy evidence。

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，
`promotion_gate.passed=false`，`private_truth_ready=false`，`deployable=false`。

## 下一步

实现 local-only `observable_pre_action_feature_join_output_scorer_v1`：只读 allowlisted
`observable_pre_action_candidate_rows.jsonl`、`observable_pre_action_source_link_summary.json`、
`observable_pre_action_feature_join_manifest.json`，验证 schema、row/source summary parity、source coverage、
field contract 与 default-off/behavior unchanged 口径。仍不得自动启动 no-order diagnostic。
