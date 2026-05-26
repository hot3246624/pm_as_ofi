# Observable Pre-Action Non-Fixture Training Bridge Multiday Inventory

## 结论

`UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS`。

本轮只做 local-only inventory：扫描当前 worktree 已物化的 `observable_pre_action_training_bridge_summary.json` 与 `observable_pre_action_training_bridge_joined_rows.jsonl`，排除 smoke、fixture、postmerge 和旧 fixture scorer 输出，判断是否已经满足 training scorer 的多日 train/holdout 输入要求。

## 真实盘点

可接受的非 fixture bridge output 只有一个：

- `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/observable_pre_action_training_bridge_summary.json`
- `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/observable_pre_action_training_bridge_joined_rows.jsonl`

该 output 通过了 schema、materializer contract、manifest safety、label separation、joined row schema、source_sequence coverage、label field、label reuse 和 private/deployable/promotion checks。

## 指标

- accepted_bridge_output_count: `1`
- total_joined_rows: `4195`
- available_days: `["2026-05-26"]`
- available_day_count: `1`
- day_counts: `{"2026-05-26": 4195}`
- split_counts: `{"unused": 4195}`
- source_sequence_coverage: `1.0`
- label_row_reuse_count: `0`

## Blockers

- `train_day_diversity_below_min`
- `holdout_day_diversity_below_min`
- `train_rows_below_min`
- `holdout_rows_below_min`

这说明非 fixture bridge rows 已经存在，但还没有足够多日 train/holdout split。当前不能运行真实 training scorer 产出可用 frozen rule，也不能继续 denominator replay 或 miner input manifest。

## Smoke

新增 smoke 覆盖：

- complete synthetic multi-day bridge output: KEEP
- single-day bridge output: UNKNOWN
- fixture summary: not accepted
- missing joined rows: fail closed
- private/deployable claim: DISCARD
- private/deployable/promotion gate separation

smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory_smoke_20260526T151842Z/manifest.json`。

## 安全边界

本轮只读当前 worktree 的 materialized JSON/JSONL；未 SSH，未启动 shadow/canary/live/local agg/shared WS，未读取或拉取 events JSONL，未扫描 raw/replay/full store，未修改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

`source_pair_*` / `source_residual_*` 仍只作为 offline train/holdout labels；不能进入 live predicate，也不能把 realized pair cost 当 live criterion。`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

继续 local-only：定义 `observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_v1`，明确未来需要如何安全积累多个同合同、非 fixture same-window handoffs，并在累计到至少 3 个 train day + 2 个 holdout day 后再合并/resplit 成 training scorer 输入。不得自动启动新的 no-order diagnostic。
