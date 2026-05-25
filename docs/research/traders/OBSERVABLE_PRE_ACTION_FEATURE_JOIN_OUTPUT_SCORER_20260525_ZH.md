# Observable Pre-Action Feature-Join Output Scorer

时间：2026-05-25 19:55 UTC

## 结论

`KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY`

已实现 local-only scorer，用来验证上一轮默认关闭 exporter 产出的 allowlisted feature-join 文件。它只读取：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`

不读取 events JSONL，不扫描 raw/replay/full store，也不产生策略盈利结论。

## 验证内容

scorer 固化以下 fail-closed 检查：

- row schema 必须是 `observable_pre_action_candidate_row_v1`
- source summary schema 必须是 `observable_pre_action_source_link_summary_v1`
- manifest schema 必须是 `observable_pre_action_feature_join_manifest_v1`
- row count 必须与 source summary / manifest 一致
- rows 必须同时覆盖 `admitted` 与 `blocked`
- row-level required fields 必须存在，包括 status/reason/source presence/pre-seed/open/deficit/candidate qty/source risk
- source summary 的 status/source-presence/bucket 计数必须能由 rows 重建
- field contract 必须保持 `strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate_passed=false`
- 禁止 post-action labels、realized pair-cost live criteria、behavior-changing/private/deployable/promotion claim

## 真实本地输出评分

输入来自上一轮 instrumentation smoke 的 enabled allowlisted 输出：

`xuan_research_artifacts/xuan_observable_pre_action_rule_miner_feature_join_runner_instrumentation_smoke_20260525T195536Z/enabled/`

scorer artifact：

`xuan_research_artifacts/xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/manifest.json`

结果：

- rows: `5`
- status coverage: `admitted` 与 `blocked`
- source sequence presence: admitted `2` present，blocked `3` present
- source summary row count: `5`
- manifest row count: `5`
- blockers: none

## Smoke

smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_feature_join_output_scorer_smoke_20260525T195536Z/manifest.json`

覆盖：

- good fixture KEEP
- 缺 row-level source field fail closed
- manifest/private/deployable claim fail closed
- missing source summary fail closed

## 状态

该 KEEP 是 feature-join output scorer readiness，不是 strategy evidence。

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，
`promotion_gate.passed=false`，`private_truth_ready=false`，`deployable=false`。

## 下一步

实现 local-only `observable_pre_action_rule_miner_denominator_replay_scorer_spec_v1`：定义 frozen predicate
如何消费 feature-join rows 与 same-window summary，生成
`observable_pre_action_no_order_denominator_summary.json` 和可被 miner spec 验证的 input manifest。
在 denominator gate 之前，仍不得启动 no-order diagnostic。
