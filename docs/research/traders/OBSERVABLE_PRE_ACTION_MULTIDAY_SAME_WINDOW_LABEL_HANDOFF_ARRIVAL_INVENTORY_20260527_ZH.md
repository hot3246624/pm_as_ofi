# Observable Pre-Action Multiday Same-Window Label Handoff Arrival Inventory

日期：2026-05-27（Asia/Shanghai）

## 结论

`observable_pre_action_multiday_same_window_label_handoff_arrival_inventory_v1` 已就绪。

真实当前决策：`UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_GAPS`

Smoke 决策：`KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_SMOKE_PASS`

该结论只表示 arrival inventory 能安全地区分“已到达但不可累积”和“exact-id-ready 可累积”的 bridge outputs。它不是策略收益证据，不是 private truth，不是 deployable，也不是 promotion/canary readiness。

## 本轮实现

新增 inventory：

`scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory.py`

新增 smoke：

`scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory_smoke.sh`

它只检查当前 worktree 中的已物化 bridge outputs：

- `observable_pre_action_training_bridge_summary.json`
- sibling `observable_pre_action_training_bridge_joined_rows.jsonl`
- sibling `manifest.json`

默认扫描 `xuan_research_artifacts/**/observable_pre_action_training_bridge_summary.json`，并排除 smoke、fixture、postmerge、tmp_specs 等路径。它不会重新 join handoff/raw data，不会读 events JSONL，不会扫 raw/replay/full store，也不会启动新的 no-order diagnostic。

## Gate

每个候选 source 必须先通过 accumulator source precheck：

- summary schema 是 `observable_pre_action_training_bridge_summary_v1`
- materializer contract 是 `observable_pre_action_non_fixture_training_bridge_materializer_v1`
- materializer manifest `materialized=true`
- `source_handoff_id` 存在
- rows 文件存在且 count 与 summary 一致
- 每行存在 `source_seed_candidate_row_id`
- `source_seed_candidate_row_id` 在 source 内唯一
- `source_seed_action_id` 若存在，在 source 内唯一
- `offline_label_row_index` 不重复
- source sequence coverage `>=0.95`
- labels 只能用于 offline train/holdout scoring
- labels、realized pair cost、future labels 不能作为 live criteria
- private/deployable/promotion claim 直接 DISCARD

通过 source gate 的 candidates 再进入 multiday accumulator precheck：

- distinct days >= `5`
- train days >= `3`
- holdout days >= `2`
- train rows >= `100`
- holdout rows >= `50`
- `source_seed_candidate_row_id` 跨 handoffs 全局唯一
- `source_seed_action_id` 若存在，也跨 handoffs 全局唯一
- 不允许 duplicate handoff id
- 不允许 silent dedupe

## 当前真实输入状态

真实 artifact：

`xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory_20260526T170213Z/manifest.json`

盘点结果：

- candidate bridge outputs：`1`
- total joined rows：`4195`
- available days：`2026-05-26`
- accepted exact-id-ready bridge outputs：`0`
- candidate exact id present：`0`
- candidate exact id missing：`4195`
- distinct handoff count：`0`

当前唯一真实候选：

`xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/observable_pre_action_training_bridge_summary.json`

fail-closed blockers：

- `summary_source_handoff_id_missing`
- `candidate_exact_id_missing_count:4195`
- `source_contract_blockers_present`
- global blocker：`non_fixture_exact_id_ready_bridge_output_absent`

解释：同窗口 label handoff 已成功，且旧 materializer 物化了 4195 行 joined rows；但这些 rows 没有 row-level `source_seed_candidate_row_id`，summary 也没有 `source_handoff_id`。新 accumulator 合同要求 row-level exact id 和 source handoff provenance，因此当前真实输入不能进入多日累积。

## Smoke 结果

Smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory_smoke_20260526T170213Z/manifest.json`

覆盖：

- complete 5-day synthetic arrivals：KEEP
- single-day：UNKNOWN
- missing exact ids / missing source handoff：UNKNOWN
- duplicate handoff id：UNKNOWN
- duplicate candidate exact id：UNKNOWN
- duplicate action exact id：UNKNOWN
- fixture-as-real：DISCARD
- forbidden live-label：DISCARD
- missing rows：UNKNOWN
- provenance/count mismatch：UNKNOWN
- private claim：DISCARD

完整 synthetic 5-day case 证明 inventory 能命名可执行 accumulator command；但这只是 fixture/smoke readiness，不是实盘策略证据。

## Safety

本轮 local-only：

- `events_jsonl_read=false`
- `events_jsonl_pulled=false`
- `raw_replay_or_full_store_scan=false`
- `ssh_used=false`
- `new_no_order_diagnostic_started=false`
- `canary_or_live_started=false`
- `local_agg_or_shared_ws_started=false`
- `shared_ingress_modified=false`
- `orders_sent=false`
- `cancels_sent=false`
- `redeems_sent=false`
- `trading_behavior_changed=false`

`research_ranking` 与 `promotion_gate` 分开报告：arrival inventory ready，但真实 `accumulator_input_ready=false`、`real_miner_input_ready=false`、`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

Local-only 下一跳是等待/检查新的非 fixture same-window label handoff 或重新物化后的 bridge outputs 是否已出现在当前 worktree，且必须包含 row-level `source_handoff_id` 与 `source_seed_candidate_row_id`。

若没有新 exact-id-ready 多日输入，则继续 `UNKNOWN/wait`；不得自动启动新的 no-order diagnostic，不得用 fixture、old CSV、公网单日 profile 或静态 cap 替代。
