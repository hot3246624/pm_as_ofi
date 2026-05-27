# Backtest V1 Candidate Filter Handoff Plan

日期：2026-05-27（Asia/Shanghai）

## 结论

`xuan_backtest_v1_candidate_filter_handoff_plan_v1` 已就绪。

真实决策：

`KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_READY_PRIVATE_BLOCKED`

Smoke 决策：

`KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_SMOKE_PASS`

该 KEEP 只表示 backtest V1 的 search-safe audit pack 可以作为候选过滤器使用，并能为下一步 same-window handoff 生成明确的安全要求。它不是策略收益证据，不是 owner private truth，不是 deployable，也不是 promotion/canary readiness。

## 输入

只读派生产物：

- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_experiment_suite_deep_v1/BACKTEST_EXPERIMENT_SUITE_MANIFEST.json`
- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_candidate_audit_pack_latest/BACKTEST_CANDIDATE_AUDIT_PACK_MANIFEST.json`
- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_candidate_audit_pack_latest/backtest_candidate_audit_pack.csv`
- `/Volumes/PolyData/poly_backtest_data/derived/contract_examples/backtest_candidate_audit_pack_latest/backtest_candidate_audit_pack_evidence.csv`

未读取 raw、AWS、远端、events JSONL、replay raw store 或 owner private data。

## Suite 状态

已验证 fingerprint：

- `suite_fingerprint=1a762ac4491cdc7121b4f132`
- `readiness_fingerprint=d992580a6c8ff5b5bd184b04`
- `candidate_audit_pack_fingerprint=d1a6ff2fde436a2ee5c345b9`

Audit pack 状态：

- selected candidates：`6`
- evidence rows：`12`
- search-safe private-blocked：`6`
- private promotion ready：`0`
- forbidden result columns：空

## 当前候选排序

当前 search-safe/private-blocked candidates 分布：

- HYPE：`2`
- DOGE：`2`
- BNB：`2`

当前 audit pack 没有选出 BTC、ETH、SOL、XRP 候选；这说明 7 币种系统可跑，但本轮通过 audit pack 的 shortlist 只落在 3 个资产。

Lead candidate：

- asset：`HYPE`
- candidate_key：`30cfee296e1d0912f78109dd`
- audit_rank：`1`
- experiment labels：`deep_v1;formal_latest`
- experiment_count：`2`
- max_seen_matrix_count：`3`
- best_queue_pnl：`-0.05`
- avg_queue_pnl：`-0.05`
- audit_status：`SEARCH_SAFE_READY_PRIVATE_BLOCKED`
- promotion blocker：`owner_private_truth_missing_for_deployable_promotion`

解释：lead 是按 audit rank 选出的“最强可继续研究对象”，不是正收益策略。当前 queue PnL 仍是 proxy/evidence-only 数值，且 private truth 缺失，所以只能驱动 handoff 生成计划，不能驱动实盘或 promotion。

## Handoff 计划

对 lead candidate 的下一步，只允许生成或接收同合同 same-window handoff。最低文件要求：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- `observable_pre_action_same_window_offline_labels.csv`
- `same_window_aggregate_summary.json`
- `observable_pre_action_same_window_offline_label_handoff.json`
- `observable_pre_action_training_bridge_joined_rows.jsonl`
- `observable_pre_action_training_bridge_summary.json`

最低 row-level 字段要求：

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

- train days `>=3`
- holdout days `>=2`
- train rows `>=100`
- holdout rows `>=50`
- source sequence coverage `>=0.95`

标签策略：

- `source_pair/source_residual` 只能用于 offline train/holdout scoring
- labels 不能进入 live predicate
- realized pair cost 不能作为 live criterion
- future labels 不能作为 live criterion

验证顺序：

1. same-window handoff contract validator
2. non-fixture training bridge materializer
3. multiday same-window label handoff arrival inventory
4. multiday same-window label handoff accumulator
5. offline training scorer
6. frozen rule 后再做 denominator replay scorer

## Smoke

Smoke artifact：

`xuan_research_artifacts/xuan_backtest_v1_candidate_filter_handoff_plan_smoke_20260527T010000Z/manifest.json`

覆盖：

- complete synthetic audit pack：KEEP
- private promotion ready：DISCARD
- forbidden result columns：DISCARD
- no accepted candidates：UNKNOWN
- raw evidence dependency：DISCARD

## Safety

本轮只读 backtest V1 派生产物并写本地 manifest：

- `raw_replay_or_full_store_scan=false`
- `events_jsonl_read=false`
- `aws_or_remote_read=false`
- `new_no_order_diagnostic_started=false`
- `canary_or_live_started=false`
- `shared_ingress_modified=false`
- `orders_cancels_redeems_sent=false`
- `trading_behavior_changed=false`

`research_ranking` 与 `promotion_gate` 分开报告：candidate filter ready，但 `real_miner_input_ready=false`、`strategy_evidence=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

最激进但仍安全的下一步是：围绕 HYPE lead `30cfee296e1d0912f78109dd` 写一个 exact bounded same-window handoff generation proposal。该 proposal 只能请求未来生成 row-level exact-id handoff，不得启动 live/canary/orders，也不得把当前 backtest proxy evidence 升级为 private truth。
