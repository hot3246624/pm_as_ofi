# Backtest V1 HYPE Lead Same-Window Handoff Proposal

日期：2026-05-27（Asia/Shanghai）

## 结论

`xuan_backtest_v1_hype_lead_same_window_handoff_proposal_v1` 已就绪。

真实决策：

`KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_READY_APPROVAL_REQUIRED`

Smoke 决策：

`KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_SMOKE_PASS`

该 KEEP 只表示 HYPE lead 已被转换成一个 exact bounded future handoff request。它没有启动 diagnostic，没有读取 raw/replay/events JSONL，也不是 strategy evidence、owner private truth、deployable 或 promotion/canary readiness。

## 输入

只读当前 worktree 已提交的 backtest V1 计划与其派生 validation manifests：

- `xuan_research_artifacts/xuan_backtest_v1_candidate_filter_handoff_plan_20260527T010000Z/manifest.json`
- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_validation_results_deep_v1/jobs/validate_0001_hype_30cfee296e1d/VALIDATION_MANIFEST.json`
- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_validation_results_latest/jobs/validate_0001_hype_30cfee296e1d/VALIDATION_MANIFEST.json`

未启动 no-order diagnostic，未 SSH，未读 events JSONL，未扫描 raw/replay/full store，未改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

## HYPE Lead

Lead candidate：

- asset：`HYPE`
- market prefix proposal：`hype-updown-5m`
- candidate_key：`30cfee296e1d0912f78109dd`
- audit_rank：`1`
- audit_status：`SEARCH_SAFE_READY_PRIVATE_BLOCKED`
- experiment labels：`deep_v1;formal_latest`
- best_queue_pnl：`-0.05`
- promotion blocker：`owner_private_truth_missing_for_deployable_promotion`

两个 validation manifest 的 candidate params 一致：

- `price_lo=0.5`
- `price_hi=0.55`
- `size_lo=50.0`
- `size_hi=150.0`
- `offset_lo=0.0`
- `offset_hi=30.0`
- `max_l1_pair_ask=1.1`
- `max_l1_immediate_pair=2.0`
- `side_alignment=high`
- `pnl_cost_source=pair_ask`

解释：该候选是 backtest V1 search-safe/private-blocked lead，不是正收益证明。当前 queue PnL 仍是 proxy/evidence-only，private truth 缺失。

## Future Handoff Request

提案 request name：

`xuan_research_backtest_v1_hype_30cfee296e1d_same_window_label_handoff_APPROVAL_TS`

当前状态：

- `diagnostic_allowed_now=false`
- `approval_required_in_thread=true`
- `future_run_template_status=proposal_only_not_executable_without_new_explicit_approval`

任何未来批准的生成路径必须先解析 `hype-updown-5m` market ids，并保持 no-order、安全、有界；不得 repo sync/build for canary，不得 service/systemd，不得 live/canary/order/cancel/redeem，不得 raw/replay/full-store/events JSONL。

最低输出文件：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `observable_pre_action_same_window_offline_labels.csv`
- `same_window_aggregate_summary.json`
- `observable_pre_action_same_window_offline_label_handoff.json`
- `observable_pre_action_training_bridge_joined_rows.jsonl`
- `observable_pre_action_training_bridge_summary.json`

最低 row-level 字段：

- `source_handoff_id`
- `source_seed_candidate_row_id`
- `source_seed_action_id`
- `day_id`
- `condition_id`
- `market_slug`
- `status_before_action`
- `side`
- `offset_s`
- `source_sequence_present`
- `source_pair_qty`
- `source_pair_cost`
- `source_pair_pnl`
- `source_residual_qty`
- `source_residual_cost`

最低多日 gate：

- distinct days `>=5`
- train days `>=3`
- holdout days `>=2`
- train rows `>=100`
- holdout rows `>=50`
- source sequence coverage `>=0.95`

标签策略：

- `source_pair/source_residual` labels 只能用于 offline train/holdout scoring
- labels 不能进入 live predicate
- realized pair cost 不能作为 live criterion
- future labels 不能作为 live criterion

## Validation Order

未来 handoff 到达后，命令顺序固定：

1. `python3 scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py --handoff-manifest ${same_window_handoff_manifest} --output-dir ${contract_output_dir}`
2. `python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py --handoff-manifest ${same_window_handoff_manifest} --output-dir ${materializer_output_dir}`
3. `python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory.py --only-explicit --bridge-summary ${bridge_summary_1} ... --output-dir ${arrival_inventory_output_dir}`
4. `python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py --bridge-summary ${bridge_summary_1} ... --output-dir ${accumulator_output_dir}`
5. `python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py --joined-rows ${accumulated_joined_rows} --output-dir ${training_output_dir}`
6. frozen rule manifest 存在后，才允许 denominator replay scorer；labels 不得成为 live predicate。

## Fail-Closed Blockers

以下任一情况必须 fail closed：

- `hype-updown-5m` prefix 不能安全解析
- train/holdout day diversity 不足
- row-level `source_handoff_id` 缺失
- row-level `source_seed_candidate_row_id` 缺失
- handoff id 或 exact candidate/action id 重复
- exact-id materialization join coverage 低于 `100%`
- source sequence coverage 低于 `95%`
- labels 被用作 live predicate
- realized pair cost 或 future label 被用作 live criterion
- 依赖 raw/replay/full store 或 events JSONL
- 依赖 SSH/shadow/canary/live/local agg/shared WS
- 出现 private truth、deployable 或 promotion claim

## Smoke

Smoke artifact：

`xuan_research_artifacts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal_smoke_20260527T011815Z/manifest.json`

覆盖：

- complete synthetic plan：KEEP
- plan not KEEP：UNKNOWN
- private claim：DISCARD
- raw/remote evidence：DISCARD
- inconsistent params：UNKNOWN
- wrong asset：UNKNOWN
- missing params：UNKNOWN

## Safety

`research_ranking` 与 `promotion_gate` 分开报告：

- `handoff_proposal_ready=true`
- `real_miner_input_ready=false`
- `strategy_evidence=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate.passed=false`

下一步只能等待用户对这个 exact bounded future HYPE same-window handoff request 的明确批准，或继续 local-only source/preflight review；未获批准前不得启动 no-order diagnostic。
