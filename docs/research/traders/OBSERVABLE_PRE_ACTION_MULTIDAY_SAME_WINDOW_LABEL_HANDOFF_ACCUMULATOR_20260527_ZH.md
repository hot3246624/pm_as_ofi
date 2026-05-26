# Observable Pre-Action Multiday Same-Window Label Handoff Accumulator

日期：2026-05-27（Asia/Shanghai）

## 结论

`observable_pre_action_multiday_same_window_label_handoff_accumulator_v1` 的 fixture scorer 已就绪。

核心 smoke 决策：`KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_SMOKE_PASS`

当前真实单日输入决策：`UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS`

该结论只表示多日 bridge rows 的本地 accumulator 机制可安全表达；它不是策略收益证据，不是 private truth，不是 deployable，也不是 promotion/canary readiness。

## 本轮实现

新增 accumulator：

`scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py`

它只消费已经 materialized 的 per-day bridge outputs：

- `observable_pre_action_training_bridge_summary.json`
- sibling `observable_pre_action_training_bridge_joined_rows.jsonl`
- sibling `manifest.json`

KEEP 时才输出：

- `observable_pre_action_training_bridge_joined_rows.jsonl`
- `observable_pre_action_training_bridge_summary.json`

## 累积规则

Accumulator 执行以下 fail-closed gate：

- 每个 source summary 必须是 `observable_pre_action_training_bridge_summary_v1`
- source rows 必须是 `observable_pre_action_training_bridge_joined_row_v1`
- 每个 source 必须有 `source_handoff_id`
- 每行必须有 `source_seed_candidate_row_id`
- `source_seed_candidate_row_id` 跨所有 source 全局唯一
- `source_seed_action_id` 若存在，也必须跨所有 source 全局唯一
- 不允许 silent dedupe
- labels 只能用于 offline train/holdout scoring
- labels、realized pair cost、future labels 不能作为 live criteria
- private/deployable/promotion claim 直接 DISCARD
- events JSONL/raw/replay/full-store/SSH/live/canary/shared WS/local agg 依赖直接 fail closed

Chronological split 固定为：

- distinct days 升序
- 最后 `2` 天 holdout
- 更早天 train
- train days `>=3`
- holdout days `>=2`
- train rows `>=100`
- holdout rows `>=50`
- source sequence coverage `>=0.95`

## Smoke 结果

Smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator_smoke_20260526T163213Z/manifest.json`

覆盖：

- complete 5-day synthetic bridge outputs：KEEP
- single-day：UNKNOWN
- duplicate handoff id：UNKNOWN
- duplicate candidate exact id：UNKNOWN
- duplicate action exact id：UNKNOWN
- fixture-as-real：DISCARD
- forbidden live-label：DISCARD
- missing rows：UNKNOWN
- provenance/count mismatch：UNKNOWN
- private claim：DISCARD

完整 synthetic case 产生 `200` rows，split 为：

- train days：`2026-05-21`、`2026-05-22`、`2026-05-23`
- holdout days：`2026-05-24`、`2026-05-25`
- train rows：`120`
- holdout rows：`80`
- join coverage：`1.0`
- source sequence coverage：`1.0`

## 当前真实输入状态

真实 artifact：

`xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator_20260526T163213Z/manifest.json`

当前真实输入仍是 UNKNOWN：

- source rows：`4195`
- only day：`2026-05-26`
- blockers：
  - `source_0:summary_source_handoff_id_missing`
  - `source_0_candidate_exact_id_missing_count:4195`
  - `train_day_diversity_below_min`
  - `holdout_day_diversity_below_min`
  - `holdout_rows_below_min`

解释：早前 real materializer 的 summary 记录了 exact-id counts，但 joined-row JSONL 没有保留 row-level `source_seed_candidate_row_id`。Accumulator 按新合同要求 row-level exact id，因此真实路径必须等待未来 handoff/materializer 输出补齐这些字段，或者后续安全地 rerun materializer 生成新 rows。

## Safety

本轮只读当前 worktree 已提交 manifests 与本地 synthetic rows：

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

`research_ranking` 与 `promotion_gate` 分开报告：fixture accumulator ready，但 `real_miner_input_ready=false`、`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

Local-only 下一跳应是 multiday handoff arrival/wait inventory：只检查当前 worktree 是否出现多个非 fixture、带 row-level exact ids 的 same-window handoff/materialized bridge outputs。若没有，则 UNKNOWN/wait；不得自动启动新的 no-order diagnostic。
