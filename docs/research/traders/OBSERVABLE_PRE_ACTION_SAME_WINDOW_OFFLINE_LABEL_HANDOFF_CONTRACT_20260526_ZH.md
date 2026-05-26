# Observable Pre-Action Same-Window Offline-Label Handoff Contract

日期：2026-05-26

结论：`KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_CONTRACT_READY`

本轮完成 local-only 合同和验证器，用来约束未来同窗口 offline label handoff。它只定义和验证输入链路，不生成 training bridge rows，不训练规则，不启动 no-order diagnostic，也不构成 strategy evidence。

## 背景

上一轮 feature-join handoff 已经产出真实 row-level pre-action rows，但旧 offline-label CSV 与这些 rows 完全不同窗：

- feature rows: `4177`，全部为 `2026-05-26`
- legacy label CSV: `41580` rows，覆盖 `2026-05-02..2026-05-18`
- day/slug/condition overlap: `0`
- joined rows: `0`

因此不能用旧 CSV 训练真实规则。需要一个未来同窗口 label handoff。

## 合同要求

未来 handoff 必须在当前 worktree 内提供：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `same_window_aggregate_summary.json`
- 同窗口 offline `source_pair/source_residual` label CSV

优先 join keys：

- `source_seed_candidate_row_id`
- `source_seed_action_id`

严格 composite fallback：

- `condition_id`
- `market_slug/slug`
- `day_id/day`
- `side`
- `source_risk_direction`
- `quote_ts_ms` 对 `ts_ms` 或 `trigger_ts_ms`，容差 `250ms`
- `offset_s`，容差 `0.25s`
- pre-seed qty/cost，容差 `1e-6`
- `candidate_qty` 对 `seed_qty` 或 `trigger_size`，容差 `1e-6`

最低 gate：

- joined rows `>=100`
- join coverage `>=95%`
- day/slug/condition overlap 均非零
- ambiguous/multiple matches 为 `0`
- labels 只能用于 offline train/holdout scoring，不能进入 live predicate
- realized pair cost 不能作为 live criterion

## Smoke

Smoke 证明：

- default contract ready
- complete synthetic same-window handoff KEEP
- old/non-overlap handoff fail closed
- fixture handoff fail closed
- forbidden live-label handoff fail closed
- private/deployable/promotion gate 始终分离

## 状态

当前没有真实 same-window offline label handoff 到位。该 KEEP 是合同 readiness，不是策略证据。

`strategy_evidence=false`

`no_order_diagnostic_allowed=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`

## 下一步

继续 local-only：实现 `observable_pre_action_same_window_label_handoff_arrival_inventory_v1`，只扫描当前 worktree 是否出现符合该合同的非 fixture handoff manifest。若未出现，继续 UNKNOWN/wait；不得自动启动 no-order diagnostic。
