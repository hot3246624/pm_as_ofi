# Observable Pre-Action Multiday Same-Window Label Handoff Accumulation Contract

日期：2026-05-27（Asia/Shanghai）

## 结论

`observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_v1` 已就绪。

决策标签：`KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_READY`

该结论只表示多日同窗口 handoff 累积规则已经固定，允许后续实现本地 accumulator scorer；它不是策略收益证据，不是 private truth，不是 deployable，也不是 promotion/canary readiness。

## 当前真实状态

当前 worktree 已有的真实 non-fixture bridge rows 仍只有一天：

- accepted bridge output：`1`
- total joined rows：`4195`
- available days：`2026-05-26`
- split counts：`unused=4195`
- source sequence coverage：`1.0`
- real miner input ready：`false`

仍然存在的 readiness blockers：

- `train_day_diversity_below_min`
- `holdout_day_diversity_below_min`
- `train_rows_below_min`
- `holdout_rows_below_min`

## 合同规则

未来每个 per-day same-window handoff 必须先通过：

1. `scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py`
2. `scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py`
3. 多日 accumulator scorer
4. `scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py`
5. offline training scorer
6. denominator replay scorer
7. miner input spec

每个 handoff 必须提供：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `observable_pre_action_same_window_offline_labels.csv`
- `same_window_aggregate_summary.json`
- `observable_pre_action_same_window_offline_label_handoff.json`

累积时只接受 exact-id 路径：

- `source_seed_candidate_row_id` 必须每行存在，并且在单个 handoff 内与跨 handoff 全局唯一。
- `source_seed_action_id` 若存在，也必须全局唯一。
- 不允许 silent dedupe；重复 handoff id 或重复 exact id 必须 fail closed。

split policy 固定为 chronological：

- distinct days 升序排序。
- 最后 `2` 天为 holdout。
- 更早的天为 train，且至少 `3` 个 train days。
- train rows 至少 `100`，holdout rows 至少 `50`。
- 累积输出中不允许继续保留 `unused` rows。

label policy 固定为：

- `source_pair/source_residual` 只能作为 offline train/holdout labels。
- labels 不能进入 live predicate。
- realized pair cost 不能作为 live criterion。
- future labels 不能作为 live criterion。

## Smoke

Smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_smoke_20260526T160213Z/manifest.json`

覆盖结果：

- complete synthetic 5-day accumulation：KEEP
- single-day input：UNKNOWN
- duplicate exact id：UNKNOWN fail closed
- fixture input：DISCARD
- forbidden live-label input：DISCARD
- private/deployable/promotion claim：DISCARD

## Safety

本轮只读当前 worktree 已提交 manifests 与本地 synthetic cases：

- `events_jsonl_read=false`
- `events_jsonl_pulled=false`
- `raw_replay_or_full_store_scan=false`
- `ssh_used=false`
- `canary_or_live_started=false`
- `local_agg_or_shared_ws_started=false`
- `shared_ingress_modified=false`
- `orders_sent=false`
- `cancels_sent=false`
- `redeems_sent=false`
- `trading_behavior_changed=false`

`research_ranking` 与 `promotion_gate` 分开报告：`accumulation_contract_ready=true`，但 `real_miner_input_ready=false`、`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

实现 local-only `observable_pre_action_multiday_same_window_label_handoff_accumulator_v1` fixture scorer：输入多个已 materialized 的 non-fixture bridge summaries/rows，验证 exact-id 全局唯一，按合同 chronological resplit，输出单个 `observable_pre_action_training_bridge_joined_rows.jsonl` 和 `observable_pre_action_training_bridge_summary.json`。
