# Observable Pre-Action Rule Miner Safe Feature-Join Pullback Handoff

日期：2026-05-26

结论：`KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SPEC_READY`

本轮只做当前 worktree 的 local-only handoff/spec。未 fetch 新数据，未读取外部 worktree，未 SSH，未启动 shadow/canary/local agg/shared WS/live，未读或拉取 events JSONL，未扫描 raw/replay/full store，未修改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

## 新增内容

- `scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py`
- `scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_smoke.sh`
- `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_20260526T000507Z/manifest.json`
- `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_smoke_20260526T000507Z/manifest.json`

## Handoff 要求

未来安全 pullback 必须把以下非 fixture 文件放入当前 worktree：

- `output/observable_pre_action_candidate_rows.jsonl`
- `output/observable_pre_action_source_link_summary.json`
- `output/observable_pre_action_feature_join_manifest.json`
- `output/same_window_aggregate_summary.json`
- 当前 worktree 内 offline label CSV

本地派生/验证链必须产出或提供：

- `output/observable_pre_action_training_bridge_joined_rows.jsonl`
- `output/observable_pre_action_training_bridge_summary.json`
- `output/frozen_observable_pre_action_rule_manifest.json`
- `output/observable_pre_action_no_order_denominator_summary.json`
- `output/observable_pre_action_rule_miner_input_manifest.json`

验证顺序固定为：

1. `xuan_observable_pre_action_feature_join_output_scorer.py`
2. training bridge joined-row / bridge scorer
3. offline training scorer
4. denominator replay scorer
5. `xuan_observable_pre_action_rule_miner_spec.py`
6. `xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py`

任何 path outside current worktree、events JSONL、raw/replay/full-store、fixture/smoke path、缺 schema、label 进入 live predicate、realized pair cost live criterion、private/deployable/promotion claim 都 fail closed。

## Smoke

Smoke 覆盖：

- default spec KEEP
- complete synthetic handoff KEEP
- fixture handoff fail closed
- missing file handoff fail closed
- frozen rule 使用 `source_pair_qty` fail closed

该 KEEP 只表示 handoff/spec ready。当前真实 handoff 状态仍是 `UNKNOWN_SAFE_HANDOFF_MANIFEST_MISSING`。

`strategy_evidence=false`

`no_order_diagnostic_allowed=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`

下一步 local-only：实现或运行 `observable_pre_action_rule_miner_handoff_arrival_inventory_v1`，只检查当前 worktree 是否出现非 fixture handoff manifest；若没有则 UNKNOWN/wait，不启动 no-order diagnostic。
