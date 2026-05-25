# Observable Pre-Action Rule Miner Real Input Inventory V2

日期：2026-05-25

结论：`UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS`

本轮只做当前 worktree 的 local-only 输入盘点。未 fetch 新数据，未读取外部 worktree，未 SSH，未启动 shadow/canary/local agg/shared WS/live，未读或拉取 events JSONL，未扫描 raw/replay/full store，未修改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

## 做了什么

新增：

- `scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py`
- `scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2_smoke.sh`
- `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2_20260525T233507Z/manifest.json`
- `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2_smoke_20260525T233507Z/manifest.json`

V2 在 V1 的基础上补齐了训练 fixture scorer 之后的完整链路盘点，要求真实输入同时满足：

1. 非 fixture `observable_pre_action_candidate_rows.jsonl`、`observable_pre_action_source_link_summary.json`、`observable_pre_action_feature_join_manifest.json`
2. 非 fixture `observable_pre_action_training_bridge_joined_rows.jsonl` 与 `observable_pre_action_training_bridge_summary.json`
3. 非 fixture `frozen_observable_pre_action_rule_manifest.json`
4. 非 fixture `observable_pre_action_no_order_denominator_summary.json`
5. 非 fixture `observable_pre_action_rule_miner_input_manifest.json`

只有这些都存在且通过安全/字段/样本/分母/label separation 检查，才会给 `KEEP`。

## 真实盘点结果

当前 worktree 仍不具备真实训练输入：

- ready input manifests：`0`
- ready feature-join outputs：`0`
- ready training bridge outputs：`0`
- ready denominator summaries：`0`
- ready frozen rules：`0`

主要 blockers：

- `non_fixture_observable_pre_action_input_manifest_absent`
- `non_fixture_feature_join_outputs_absent_or_incomplete`
- `non_fixture_training_bridge_joined_rows_absent_or_incomplete`
- `non_fixture_denominator_summary_absent`
- `non_fixture_frozen_rule_manifest_absent_or_incomplete`
- `legacy_candidate_csv_not_assemble_ready`
- `allowlisted_pullbacks_lack_complete_observable_pre_action_chain`

旧 CSV `xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/candidate_seed_outcome_separator.csv` 仍只能当 offline label 来源参考：它有 `41580` rows、`15` days 和 `source_pair/source_residual` labels，但没有可信行级 `status_before_action/block_reason/source_sequence/quote_intent/source_order`，也没有同窗口 no-order denominator，因此不能直接训练真实规则。

## Smoke

Smoke 构造了一个 synthetic non-fixture complete chain，并验证：

- complete chain 会得到 `KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_READY`
- 该 input 能通过 `scripts/xuan_observable_pre_action_rule_miner_spec.py`
- fixture input fail closed
- missing bridge fail closed
- forbidden frozen rule fail closed
- default current-worktree run 不读 events JSONL

## 解释

这不是策略失败或通过，而是输入状态结论：当前真实 materialized input 还没有出现。现有新增能力是盘点/校验完整链路，防止把 fixture/smoke tooling readiness 当成 strategy evidence。

`strategy_evidence=false`

`no_order_diagnostic_allowed=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`

下一步 local-only：固化 `observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1`，明确未来需要放入当前 worktree 的非 fixture pullback 文件清单、校验命令和 fail-closed 规则；或者等待安全 feature-join pullback 出现。不得自动启动 no-order diagnostic。
