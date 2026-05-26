# Observable Pre-Action Non-Fixture Training Bridge Materialization

## 结论

`UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_TRAIN_HOLDOUT_GAPS`。

本轮成功把同窗口 feature rows + offline labels handoff 物化成非 fixture training bridge rows，但还不能进入真实训练：当前输入只覆盖 `2026-05-26` 一天，无法满足 training spec 的 `train_days>=3`、`holdout_days>=2`、`train_rows>=100`、`holdout_rows>=50`。

## 输入

- handoff: `xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_driver_20260526T110803Z/remote_clean/output/observable_pre_action_same_window_offline_label_handoff.json`
- feature rows: `xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_driver_20260526T110803Z/remote_clean/output/observable_pre_action_candidate_rows.jsonl`
- offline labels: `xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_driver_20260526T110803Z/remote_clean/output/observable_pre_action_same_window_offline_labels.csv`
- aggregate summary: `xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_driver_20260526T110803Z/remote_clean/output/same_window_aggregate_summary.json`

## 结果

- joined rows: `4195`
- join coverage: `100%`
- join method: `exact_id:source_seed_candidate_row_id`
- feature candidate ids: `4195`
- label candidate ids: `4195`
- feature action ids: `217`
- label action ids: `217`
- source sequence coverage: `1.0`
- label row reuse: `0`
- materialized outputs:
  - `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/observable_pre_action_training_bridge_joined_rows.jsonl`
  - `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/observable_pre_action_training_bridge_summary.json`

## Fail-Closed Gate

materialization blockers 为 `[]`，说明 exact-id join、label reuse、provenance、source summary、feature manifest、same-window aggregate、label policy 均通过。

readiness blockers 为：

- `train_day_diversity_below_min`
- `holdout_day_diversity_below_min`
- `train_rows_below_min`
- `holdout_rows_below_min`

因此当前 bridge rows 是真实非 fixture 离线研究输入，但不能喂给 real miner 训练，也不能生成 frozen rule、denominator replay 或 `observable_pre_action_rule_miner_input_v1`。

## 安全边界

本轮只读当前 worktree allowlisted pullback 与已提交 spec/contract；未 SSH，未启动 shadow/canary/live/local agg/shared WS，未读取或拉取 events JSONL，未扫描 raw/replay/full store，未修改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

`source_pair_*` / `source_residual_*` labels 仅保留在 joined rows 中用于 offline train/holdout scoring；它们没有进入 live predicate，也没有把 realized pair cost 当作 live criterion。`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## Smoke

新增 smoke 覆盖：

- complete synthetic multi-day handoff: KEEP
- real-like single-day handoff: materialized but UNKNOWN
- missing exact id: fail closed
- duplicate label exact id: fail closed
- label allowed in live predicate: DISCARD
- private/deployable/promotion gate separation

smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materializer_smoke_20260526T142913Z/manifest.json`。

## 下一步

继续 local-only：实现或运行 `observable_pre_action_non_fixture_training_bridge_multiday_inventory_v1`，只扫描当前 worktree 中已物化的非 fixture bridge summaries/rows，判断是否已积累足够多日 train/holdout 输入。若仍只有单日 handoff，则报告 UNKNOWN/wait；不得自动启动新的 no-order diagnostic。
