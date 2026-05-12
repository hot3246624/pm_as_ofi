# Xuan m0001 Shadow Smoke Plan - 2026-05-12

Research status: `c6_e022/m0001` is a strong event-store candidate, not deploy proof.
Do not use cache/store metrics as official source-of-truth verification.

## Scope Guard

- Do not touch production services, `pm-shared-ingress-broker.service`, shared ingress env/root/protocol/market universe, oracle lag, local agg, raw, or replay.
- Use only dry-run/shadow execution with `PM_PGT_SHADOW_PROFILE=xuan_m0001`.
- Keep `xuan_m0001` quote emission dry-run-only until commit, merge, owner approval, and official verifier queue are complete.
- Treat strict V2 taker-buy cache and `completion_unwind_event_store_v2` as research inputs only.

## Pre-Shadow Review

- Confirm the branch contains the Rust dry-run-only profile guard for `xuan_m0001`.
- Confirm public trade triggers retain only recent taker SELL ticks.
- Confirm offline runner diagnostics reject explicit non-SELL public trade rows when `taker_side`/equivalent columns are present.
- Confirm runner parity for material residual lockout: `residual_qty > 6` or `residual_cost > 6`.
- Confirm runner parity for staged completion caps: `0.990` base, `0.995` when remaining time is `<=60s`, and `1.010` when remaining time is `<=30s`.
- Confirm aged blocked-lot skip: age `>=120s` and `first_vwap + completion_px > 1.001`.
- Confirm active blocked-residual probe remains narrow: only aged, non-material residuals before the cycle cap may continue with a fresh public-trade seed.
- Confirm dry-run partial trade support is explicitly set for shadow:
  `PM_DRY_RUN_MARKET_TOUCH_TRADE_PARTIAL_FILLS=1`
  `PM_DRY_RUN_MARKET_TOUCH_TRADE_FILL_FRACTION=0.25`
- For offline runner diagnostics, `--queue-share` sizes the seed quote as a fraction of the public trade and `--queue-haircut` discounts observed support. Default runner seed sizing now matches the Rust `xuan_m0001` 25% public-trade quote fraction. If copied rows include remaining-time columns, the runner applies the Rust staged completion caps via `--late-completion-pair-cap`, `--final-completion-pair-cap`, `--late-remaining-s`, and `--final-remaining-s`.

## Local Verification Commands

```bash
python3 -m unittest scripts/tests/test_run_xuan_m0001_maker_shadow.py
PYTHONPYCACHEPREFIX=/private/tmp/xuan_pycache python3 -m py_compile scripts/run_xuan_m0001_maker_shadow.py
rustfmt --edition 2021 --check src/polymarket/coordinator.rs src/polymarket/coordinator_order_io.rs src/polymarket/coordinator_execution.rs src/polymarket/executor.rs src/polymarket/pair_ledger.rs src/polymarket/strategy/pair_gated_tranche.rs src/polymarket/coordinator_tests.rs
cargo test xuan_m0001 --lib
cargo test public_trade_snapshot_retains_latest_sell_after_buy_tick --lib
cargo test dry_run_market_trade_partial_fill_fraction_haircuts_trade_support --lib
cargo build --bin polymarket_v2
```

## Smoke Acceptance

- No live orders or production service changes.
- Shadow logs show public SELL tick seed attempts, not BUY-tick triggers.
- Offline runner events show `seed_skipped_non_sell_public_trade` for explicit BUY-tick rows when copied diagnostics include taker-side fields.
- Dry-run fills are partial-trade supported, with the configured 25% fraction visible in startup logs.
- Material residual lockouts occur only above the strict quantity/cost thresholds.
- Blocked-lot skip diagnostics appear for aged over-margin lots instead of forcing expensive completion.
- Active blocked-residual probe does not fire for fresh residuals, material residuals, final-60s rounds, or markets already at the cycle cap.
- `fill_source` summaries distinguish `dry_run_trade_sell_touch` from book-touch, immediate, taker, and authenticated user-WS fills.
- Early smoke should prioritize residual/stress, participation/cycles, pair cost, then ROI/PnL.

## Follow-Up Verification Queue

Queue official source-of-truth verification separately before any deployment claim. Required holdout coverage:

- `xuan_public_execution_truth_v1` currently confirmed only through `20260502_20260508`.
- Event-store extension `20260509..20260511` is research evidence only.
- Special focus: explain and verify the `20260509` stress100 negative day before promotion.
