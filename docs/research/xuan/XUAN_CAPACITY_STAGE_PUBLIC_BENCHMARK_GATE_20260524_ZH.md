# Xuan Capacity Stage Public Benchmark Gate

## Status

- status: `KEEP_CAPACITY_STAGE_PUBLIC_BENCHMARK_GATE_PASS_RESEARCH_ONLY`
- stage: `cap_25`
- capacity_stage_gate_pass: `True`
- remote_runner_allowed: `False`
- deployable: `False`
- hard_blockers: `none`
- soft_warnings: `none`

## Metrics

- pair_pnl_after_fee: `0.706822`
- pair_qty_redeem_notional: `18.75`
- actual_pair_cost_after_fee: `0.962303`
- edge_on_redeem_notional_after_fee_pct: `3.7697`
- roi_on_total_cash_spend_after_fee_pct: `3.8118`
- residual_qty_share_pct: `4.5455`
- residual_cost_share_pct: `3.6832`
- residual_cost_to_pair_qty_pct: `2.6667`
- strict_rescue_closes: `9.0`
- strict_rescue_source_blocks: `0.0`
- rescue_l1_age_ms_max: `0.0`

## Public Benchmark

- benchmark_scope: `pair_quality_fee_residual_reference_not_new_window_realized_pnl`
- b55_actual_pair_cost: `0.959218`
- b55_pair_edge_pct: `4.0782`
- b55_residual_rate_pct: `14.9474`
- b55_new_position_mtm_ex_rebate: `2851.0`
- ce25_actual_pair_cost: `0.974217`
- ce25_residual_rate_pct: `8.7091`
- pair_cost_delta_vs_b55: `0.003085`
- residual_qty_share_delta_vs_b55: `-0.104019`

## Hard Gates

- pm_dry_run: `True` expected `True` -> `True`
- orders_sent: `False` expected `False` -> `True`
- deploy_or_restart: `False` expected `False` -> `True`
- shared_service_mutation: `False` expected `False` -> `True`
- remote_repo_mutation: `False` expected `False` -> `True`
- min_edge_on_redeem_notional_after_fee: `0.037697 >= 0.02` -> `True`
- min_roi_on_total_cash_spend_after_fee: `0.038118 >= 0.015` -> `True`
- min_realized_pair_pnl_after_fee: `0.706822 >= 0.0` -> `True`
- min_pair_qty: `18.75 >= 5.0` -> `True`
- max_residual_cost_share: `0.036832 <= 0.15` -> `True`
- max_residual_qty_share: `0.045455 <= 0.2` -> `True`
- max_residual_cost_to_pair_qty: `0.026667 <= 0.05` -> `True`
- max_rescue_l1_age_ms: `0.0 <= 50.0` -> `True`
- strict_rescue_source_blocks: `0.0 == 0.0` -> `True`
- public_review_actual_pair_cost_hard: `0.962303 <= 0.975` -> `True`
- public_review_residual_rate_hard: `0.045455 <= 0.2` -> `True`

## Soft Review Gates

- public_target_actual_pair_cost: `0.962303 <= 0.965` -> `True`
- public_target_residual_rate: `0.045455 <= 0.15` -> `True`

## Boundary

- This scorer is local/research-only.
- It does not authorize remote execution.
- It does not authorize live orders, deploy, restart, shared-ingress mutation, collector rebuild, or raw/replay mutation.
- Public leaderboard accounts are benchmarks for pair quality/residual shape, not private truth.
