# Observable Pre-Action Rule Miner Feature-Join Contract

时间：2026-05-25 19:25 UTC

## 结论

`KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_READY_EXPORTER_TARGET_NAMED`

上一轮 input inventory 的 UNKNOWN 不是策略结论，而是输入结构缺口。本轮把缺口收敛成一个明确的
default-off 导出合同：先让 runner 在不改变行为的前提下输出 row-level pre-action feature rows，
再由 frozen-rule denominator scorer 生成 same-window no-order denominator。未完成这两层之前，不允许
启动新的 no-order diagnostic。

## 源码能力

当前 runner 已经有必要的观测点：

- `record_source_link_transition(status, reason, ...)` 能看到 admitted/blocked、reason、side、offset、pre-seed qty/cost。
- `block(...)` 对 blocked reason 走统一记录路径。
- admitted candidate 在 append 前已有 `quote_intent_id`、`source_order_id`、`source_sequence_id`、pre-seed state。
- 现有 event-lite/default-off pattern 已经成熟。

但当前 runner 还没有一个安全、allowlisted、非 events JSONL 的 row-level materialized export。因此现有
source-link summary 只能给 aggregate，不能直接喂给 rule miner。

## 合同目标

### Phase 1: default-off runner exporter

未来目标 flag：

`--observable-pre-action-rule-miner-feature-join-output`

依赖：

- `--event-lite-summary`
- `--source-link-transition-event-lite-summary`

允许写出的文件：

- `output/observable_pre_action_candidate_rows.jsonl`
- `output/observable_pre_action_source_link_summary.json`
- `output/observable_pre_action_feature_join_manifest.json`

必要 row-level 字段：

- `condition_id`
- `market_slug`
- `day_id`
- `quote_ts_ms`
- `side`
- `offset_s`
- `status_before_action`
- `block_reason`
- `source_sequence_present`
- `quote_intent_present`
- `source_order_present`
- `pre_seed_same_qty/pre_seed_opp_qty`
- `pre_seed_same_cost/pre_seed_opp_cost`
- `open_qty_bucket`
- `deficit_bucket`
- `candidate_qty_bucket`
- `source_risk_direction`

### Phase 2: frozen-rule denominator scorer

未来目标：

`observable_pre_action_rule_miner_denominator_replay_scorer_v1`

它必须在 miner 冻结 predicate 之后，用 Phase 1 rows 生成：

- `output/observable_pre_action_no_order_denominator_summary.json`
- `output/observable_pre_action_rule_miner_input_manifest.json`

必要 denominator 字段：

- `frozen_rule_id`
- `same_window_denominator_count`
- `admitted_count`
- `blocked_count`
- `source_sequence_coverage`
- `quote_intent_presence_rate`
- `source_order_presence_rate`
- `aggregate_parity`

## 禁止事项

不能用以下内容作为 live criteria：

- realized pair cost
- `source_pair_*`
- `source_residual_*`
- settlement/redeem/future label
- public-profile single-day profit filter
- 静态 side/offset/public-price cap
- D+ 已失败 family 的旧 marker

这些 post-action labels 只能用于离线 train/holdout 评分，不能进入冻结后的 live predicate。

## 当前状态

`current_direct_materialized_export_present=false`，所以本轮 KEEP 是合同和 exporter target ready，不是
真实输入 ready，也不是策略证据。

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，
`promotion_gate.passed=false`，`private_truth_ready=false`，`deployable=false`。

## 下一步

实现 local-only `observable_pre_action_rule_miner_feature_join_runner_instrumentation_default_off_v1`：
只加默认关闭 materialized output writer 和本地 smoke，证明默认关闭不产出、开启后产出 schema、CLI 依赖
fail closed、且 candidate/block counts 与交易行为不变。仍不得启动 no-order diagnostic。
