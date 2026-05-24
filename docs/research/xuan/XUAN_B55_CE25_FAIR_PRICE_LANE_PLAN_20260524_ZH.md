# Xuan B55/Ce25 Fair-Price Lane Plan

## Status

- status: `KEEP_B55_CE25_FAIR_PRICE_LANE_PLAN_READY_LOCAL_ONLY`
- lane_ready: `True`
- remote_runner_allowed: `False`
- deployable: `False`
- hard_blockers: `none`

## Benchmark Evidence

- b55_actual_pair_cost: `0.959218`
- b55_pair_edge_pct: `4.0782`
- b55_residual_rate_pct: `14.9474`
- ce25_actual_pair_cost: `0.974217`
- ce25_residual_rate_pct: `8.7091`
- b55_preferred_entry_15m_to_1m_actual_share_pct: `66.577`
- b55_core_price_35c_to_90c_actual_share_pct: `70.8042`
- b55_mid_price_50c_to_80c_actual_share_pct: `42.3241`
- b55_btc_eth_pair_profit_share_pct: `87.9353`

## Admission Contract

- assets: `BTC, ETH`
- timeframes: `15m, 1h_or_named`
- entry_windows: `end_minus_15m_to_5m, end_minus_5m_to_1m`
- core_price_bands: `35c_to_50c, 50c_to_65c, 65c_to_80c, 80c_to_90c`
- target_actual_pair_cost_lte: `0.965`
- review_actual_pair_cost_lte: `0.975`
- initial_residual_target_lte: `0.1`
- hard_residual_lte: `0.2`
- fair_price_edge_min: `0.015`
- max_effective_fee_rate_hard: `0.0175`

## Required Fair-Price Inputs

- `market_slug`
- `asset`
- `timeframe`
- `market_close_ts`
- `seconds_to_close`
- `polymarket_best_bid`
- `polymarket_best_ask`
- `venue_mid_binance`
- `venue_mid_coinbase`
- `venue_mid_okx`
- `venue_mid_bybit`
- `venue_sample_ts`
- `venue_latency_ms`
- `venue_outlier_flags`
- `aggregate_fair_probability`
- `fair_probability_method`

## Research Phases

- phase_0_public_benchmark_pin: keep b55/ce25 corrected PnL caveats and pair-quality targets attached to every review packet
- phase_1_local_fair_price_replay_design: build local-only BTC/ETH 15m/1h admission rows with fair_probability, no shared-service mutation
- phase_2_source_truth_validation: prove fair-price rows link to trade/L1/L2 source ids before any runtime claim
- phase_3_no_order_shadow_probe: run bounded PM_DRY_RUN=1 no-order probes only after explicit authorization
- phase_4_capacity_ladder_merge: compare cap stages against b55/ce25 pair-cost and residual gates before moving above cap_25

## Kill Criteria

- fee_included_actual_pair_cost_above_0.975_on_review_window
- residual_qty_share_above_0.20_or_residual_cost_share_above_0.15
- effective_fee_rate_above_0.0175_without_offsetting_pair_cost_improvement
- public_pnl_or_runtime_pnl_depends_on_old_inventory_redeem_without_explicit_attribution
- source_linkage_missing_for_trade_l1_l2_or_fair_price_inputs
- positive_pnl_requires_directional_residual_not_labeled_as_directional_lane
- any_live_order_deploy_restart_or_shared_service_mutation_request

## Boundary

- This is local/research-only planning.
- It does not mutate localagg, shared ingress, raw/replay, production executors, or collector outputs.
- It does not authorize remote execution or live orders.
- b55 and ce25 are public benchmarks, not private truth.
