# No-Order Shadow Report Handoff - 2026-05-29

Owner handoff: runner / shared WS colleague

## Reason for Handoff

Codex is stopping work on the no-order shadow report package per user instruction and returning focus to the local price aggregator lane.

The implementation did not complete. The main blocker was execution continuity: recurring heartbeat work repeatedly interrupted the local report-generation task, causing remote local-agg validation checks to interleave with this separate deliverable.

## User-Confirmed Contract

Produce a completely no-order shadow report. Do not order, cancel, redeem, import candidates, enable live, or enable canary/prod/funding.

Input files:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/research_only_import_contract.csv`
- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_shadow_evaluation_gate_spec_latest/required_shadow_report_columns.csv`
- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/source_semantics_contract.json`

Required output main CSV:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_report_latest/btc_same_window_tiny_canary_no_order_shadow_report.csv`
- Must match `required_shadow_report_columns.csv` exactly: 33 columns, no extra fields.

## Critical Clarifications from User / Colleague

- `candidate_id` in the main report must equal `deterministic_candidate_id` from the import contract.
- Do not add `source_fingerprint` to the main table.
- Source continuity must be proven by audit manifest plus import contract hash.
- Main report safety columns are already in the 33-column schema and must be `false` on every row:
  - `orders_sent`
  - `cancels_sent`
  - `redeems_sent`
  - `live_orders_allowed`
- `import_enabled` and `candidate_import_allowed` are not main-table fields. Put them in the audit manifest and make evaluator fail-closed if either is not false.
- Import contract is read-only input only; do not perform candidate import.
- Output a separate audit manifest to prove safety flags and source fingerprint continuity.
- Output a separate gate summary for thresholds.
- Summary must state this shadow report only validates public book / latency / fillability proxies, and cannot validate owner order acceptance, real fills, real fee, inventory/redeem, private truth, or promotion readiness.

## Observed Input Facts

Import contract:

- Path: `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/research_only_import_contract.csv`
- Rows: 52
- Safety flags in import contract are false:
  - `import_enabled=false`
  - `candidate_import_allowed=false`
  - `live_orders_allowed=false`
  - `deployable=false`
- `dry_run_only=true`
- Source dataset fingerprint:
  - `69d8a06c4ad86936a382c97191e46dc68246e4e6c164dd80a8befb496c4cddda`

Required columns file:

- Path: `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_shadow_evaluation_gate_spec_latest/required_shadow_report_columns.csv`
- Columns: 33
- Main report must not include helper columns.

Source semantics:

- Path: `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/source_semantics_contract.json`
- `source_semantics_contract_id=btc_v1_normalized_buy_adapter_canonical_research_canary_v1`
- `l2_top_overlay_contract_id=md_book_l2_top_aligned_l1_canonical_top_raw_l2_depth_asof_v1`
- It explicitly says historical shadow/v1 is not private truth and owner/private truth is required later.

## Existing Eval State

Current eval directory:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_eval_latest`

Current status before handoff:

- `XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_EVAL.json` reports `BLOCKED_XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_REPORT_MISSING`
- `observed_shadow_report_normalized.csv` contains only the header
- `threshold_failures.csv` contains only `shadow_report_present=missing`

The downstream eval latest directory should be updated after the main report exists so downstream consumers see:

- main report present
- 33-column schema aligned
- no-order safety columns false
- public L2 proxy gate evaluated
- promotion/private truth still blocked

## Data Sources Already Located

Action-level handoff source:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_completion_candidate_rescore_latest/xuan_completion_candidate_same_window_handoff_actions.csv`
- Filtering BTC actions by the 52 import-contract `condition_id`s yields 1042 action rows.

L2 as-of source:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/l2_top_aligned_mart_20260502_20260518_l2/l2_top_aligned_mart.duckdb`
- Table: `md_book_l2_top_aligned`
- Useful columns:
  - `condition_id`
  - `market_side`
  - `recv_ms`
  - `source_ts_ms`
  - `raw_l2_age_ms`
  - `ask1_px`
  - `ask1_sz`
  - `raw_l2_ask1_sz` ... `raw_l2_ask5_sz`

Suggested action-to-book join:

- For each action, join same `condition_id` and action `side` to latest L2 row with `recv_ms <= planned_ts_ms`.
- For pair cost proxy, also join `opposite_side` the same way.
- Use DuckDB `ASOF LEFT JOIN` against `planned_ts_ms >= recv_ms`.

Quick probe already completed:

- BTC actions from import contract: 1042
- L2 side matches from ASOF join: 1042 / 1042
- L2 `raw_l2_age_ms` p95: 3.0 ms
- Planned minus L2 `source_ts_ms` p95: about 129.8 ms
- Max planned minus source timestamp: 1228 ms

Gamma metadata:

- Public read of `https://gamma-api.polymarket.com/events?slug=btc-updown-5m-1777887300` succeeded.
- Market metadata provides:
  - `market.id`
  - `conditionId`
  - `clobTokenIds`
  - `outcomes=["Up","Down"]`
- For BTC up/down markets, map:
  - `token_id_yes` = first `clobTokenIds` entry / `Up`
  - `token_id_no` = second `clobTokenIds` entry / `Down`
- Resolve all 52 slugs similarly or fail-closed in manifest/gate summary if any cannot resolve.

## Gate Thresholds

Use:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_shadow_evaluation_gate_spec_latest/pass_thresholds.json`

Key thresholds:

- `book_action_match_rate >= 0.95`
- `book_age_p95_ms <= 1000`
- `top5_supports_seed_qty_rate >= 0.95`
- `seed_qty_over_top5_depth <= 0.05`
- `observed_residual_cost_share <= 0.05`
- fee 1.5x stress remains positive
- pair edge 50% haircut remains positive
- no-order safety columns are false
- import/candidate-import flags fail-closed false in manifest/evaluator

Baseline support files:

- Gate spec JSON:
  - `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_shadow_evaluation_gate_spec_latest/XUAN_BTC_TINY_CANARY_SHADOW_EVALUATION_GATE_SPEC.json`
- Microstructure feasibility:
  - `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/microstructure_feasibility.csv`
- Capital ledger:
  - `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest/filter_capital_ledger.csv`

Baseline gate spec already reports:

- actions: 1042
- l2_action_match_rate: 1.0
- max_market_p95_l2_age_ms: 26.2
- avg_top5_supports_seed_qty_rate: 1.0
- max_seed_qty_over_top5_depth: 0.026573
- max_residual_cost_share: 0.029461
- fee_1_50_zero_stress_after_fee_pnl: 44.408704
- pair_edge_50pct_zero_stress_after_fee_pnl: 22.431028

## Suggested Main Report Field Mapping

- `run_id`: deterministic local run label, e.g. `xuan_btc_tiny_canary_public_l2_proxy_no_order_shadow_YYYYMMDDTHHMMSSZ`
- `runner_profile_id`, `filter_name`, `filter_version`, `candidate_rank`, `asset`, `day`, `condition_id`, `slug`: from import contract/action merge
- `candidate_id`: import contract `deterministic_candidate_id`
- `market_id`, `token_id_yes`, `token_id_no`: resolved from Gamma metadata
- `planned_ts_ms`: action `ts_ms`
- `observed_ts_ms`: L2 side `recv_ms` or planned timestamp for proxy semantics; document choice in manifest
- `latency_ms`: `observed_ts_ms - planned_ts_ms` if using live/runner observation; for historical L2 proxy use non-negative as-of delta semantics and document clearly
- `book_age_ms`: preferably `planned_ts_ms - source_ts_ms` or L2 `raw_l2_age_ms`; keep definition explicit in summary
- `side`: action side
- `planned_seed_px`: action `seed_px`
- `observed_best_ask`: L2 side `ask1_px`
- `observed_top5_fillable_qty`: sum `raw_l2_ask1_sz` ... `raw_l2_ask5_sz`, falling back to `ask1_sz` for level 1 if needed
- `planned_seed_qty`: action `seed_qty`
- `seed_qty_over_top5_depth`: planned seed qty divided by top5 depth
- `would_fill_top1`: top1 ask size supports seed qty
- `would_fill_top5`: top5 depth supports seed qty
- `observed_pair_cost_proxy`: side observed ask plus opposite-side observed ask
- `observed_fee_model`: explicit proxy label, e.g. `public_l2_proxy_fee_model_from_research_gate`
- `observed_residual_qty_proxy`: max action inventory qty after seed
- `observed_residual_cost_proxy`: max action inventory cost after seed
- `orders_sent=false`
- `cancels_sent=false`
- `redeems_sent=false`
- `live_orders_allowed=false`

## Required Outputs

Main report:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_report_latest/btc_same_window_tiny_canary_no_order_shadow_report.csv`

Audit manifest:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_report_latest/NO_ORDER_SHADOW_AUDIT_MANIFEST.json`

Gate summary:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_report_latest/NO_ORDER_SHADOW_GATE_SUMMARY.json`

Eval latest should be updated:

- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_eval_latest/XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_EVAL.json`
- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_eval_latest/observed_shadow_report_normalized.csv`
- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_eval_latest/threshold_failures.csv`
- `/Users/hot/web3Scientist/poly_backtest_data/derived/contract_examples/xuan_btc_tiny_canary_no_order_shadow_eval_latest/stop_condition_events.csv`

## Final Status to Use if Proxy Gate Passes

Use a status that is explicit and non-promotional, for example:

`KEEP_XUAN_BTC_TINY_CANARY_PUBLIC_L2_PROXY_NO_ORDER_SHADOW_EVALUATED_PROMOTION_BLOCKED_OWNER_TRUTH`

Do not claim:

- live ready
- private truth ready
- canary ready
- promotion ready
- owner order acceptance validated
- real fills validated
- real fees validated
- inventory/redeem validated
