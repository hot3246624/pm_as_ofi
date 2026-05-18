# D+ (two-sided passive BUY from public SELL) — Rust dry-run mapping plan

Date (UTC): 2026-05-14

Scope: **local-only** mapping plan for implementing the current D+ frontier candidate as a **Rust dry-run-only** profile, grounded in the existing `PairGatedTrancheArb` / xuan-maker-like plumbing.

## Latest event-store check

Remote research artifacts:

- `05-13` holdout: `/home/ubuntu/xuan_frontier_runs/d_branch_minorder_fillhaircut_oos_0513_20260514_1105`
- `05-02..05-13` fh020 full window: `/home/ubuntu/xuan_frontier_runs/d_branch_minorder_fh020_full_0502_0513_20260514_1110`

`05-13` did not invalidate the D+ min-order direction. The best holdout stress row was `fh020 / imb6 / sv0960`:

- `active_markets=288`
- `pair_actions=2986`
- `net_pair_cost_wavg=0.922410`
- `rounds_per_market=10.3681`
- `qty_residual_rate=3.4398%`
- `actual_settle_pnl=+345.25`
- `actual_settle_roi=7.1615%`
- `stress100_worst_pnl=+175.70`

On the updated `2026-05-02..2026-05-13` fh020 full window, the strongest stress row remains `imb8 / sv0950`:

- `active_markets=3442`
- `pair_actions=38960`
- `net_pair_cost_wavg=0.922802`
- `pair_delay_wavg_s=26.7388`
- `rounds_per_market=11.3190`
- `qty_residual_rate=3.8242%`
- `actual_settle_pnl=+4985.76`
- `actual_settle_roi=6.9520%`
- `stress100_worst_pnl=+2410.78`

Risk-balanced sibling `imb6 / sv0950` remains attractive if residual rate is weighted above raw PnL:

- `net_pair_cost_wavg=0.921827`
- `rounds_per_market=11.0421`
- `qty_residual_rate=3.3784%`
- `actual_settle_pnl=+4567.88`
- `actual_settle_roi=7.4240%`
- `stress100_worst_pnl=+2325.56`

## Local inputs snapshot (for context)

- strict V2 cache labels discovered locally under `/tmp/poly-cache-local/taker_buy_signal_core_v2_strict_l1`:
  - `20260502_20260507`, `20260508`, `20260509`, `20260510`, `20260511`
- `completion_unwind_event_store_v2`: none found locally under `/tmp/poly-verification-local/completion_unwind_event_store_v2`

## Target behavior (D+ summary)

- Trigger: **recent public SELL** ticks (maker-resting buys that front-run public sells).
- Action: place **passive BUY** quotes (YES_BUY and/or NO_BUY) with strict pair-cost ceilings and min-order-scaled sizing (the “minorder + fill_haircut” research axis).
- Inventory discipline: prefer “completion/inventory lifecycle” semantics compatible with the existing tranche ledger; do not introduce new capital flows or production execution paths in the first pass.

## Local implementation state (2026-05-14T11:18Z)

Stage A is now present locally:

- `StrategyCoordinator` stores `last_public_trade_by_side: [Option<PublicTradeSnapshot>; 2]`.
- `handle_market_data` updates the per-side slot for public SELL ticks.
- `recent_public_trade_for(side, max_age)` is covered by `public_trade_snapshot_tracks_latest_sell_per_side`.

Stage B is also present as a dry-run-only profile:

- `PgtShadowProfile::DPlusMinOrderV1` maps `PM_PGT_SHADOW_PROFILE=dplus_minorder_v1`.
- The profile uses public SELL price minus `0.040`, `[0.10, 0.990]` price band, `0.990` open pair cap, `0.20` fill haircut, `10.0` target qty, `8x` imbalance room, and D+ no-seed diagnostics.

Open readiness decision before shadow:

- The first D+ shadow should intentionally test simultaneous YES/NO passive BUY seeds when both sides pass D+ admission.
- Reason: the validated D+/B27 edge is a two-sided passive inventory effect; forcing a deterministic single side before shadow would likely destroy the cycles/market and pair-cost advantage that made the candidate interesting.
- Safety is owned by dry-run-only execution, min-order target sizing, fill haircut, imbalance cap, material-residual guard, and shadow acceptance metrics rather than by collapsing the strategy into a different single-side family.
- D+ now routes candidate pairs through `select_flat_seed_plans` for geometry rejection while explicitly preserving dual seed selection for valid two-sided candidates.

Patch proposal if the conservative path is chosen:

- If shadow shows the current tranche lifecycle cannot represent true two-sided passive inventory safely, fall back to a conservative single-side D+ variant and keep the current dual-side profile as Stage C.
- Keep all D+ profiles dry-run-only until live-equivalent shadow confirms fill realism, pair cost, cycles, and residual gates.

## Staged implementation plan (dry-run first)

### Stage A — Per-side public trade cache (implemented)

- Change `StrategyCoordinator` state to store last public SELL trade **per market side**:
  - Replace `last_public_trade: Option<PublicTradeSnapshot>` with `last_public_trade_by_side: [Option<PublicTradeSnapshot>; 2]` (index by `Side`).
  - Update `handle_market_data` in `src/polymarket/coordinator_order_io.rs` to write into the per-side slot on each SELL tick.
- Add APIs:
  - `recent_public_trade_for(side: Side, max_age: Duration) -> Option<PublicTradeSnapshot>`
  - Keep `recent_public_trade(max_age)` as a best-effort fallback (optional) or deprecate it once callers migrate.

Files:
- `src/polymarket/coordinator.rs`
- `src/polymarket/coordinator_order_io.rs`
- `src/polymarket/coordinator_tests.rs` (extend coverage)

### Stage B — Add a D+ dry-run profile inside `PairGatedTrancheArb` (implemented, pending dual-seed policy decision)

Goal: implement D+ as a **new `PgtShadowProfile` variant** so it reuses tranche lifecycle + completion logic, but has D+-specific seed selection and gates.

Proposed additions (names are placeholders):
- `PgtShadowProfile::DPlusMinOrderV1`
- `PgtShadowProfile::DPlusMinOrderImb6V1` (risk-balanced sibling)

Seed logic (high level):
- For each `side in [YES, NO]`:
  - Read `recent_public_trade_for(side, trade_fresh)` and require `taker_side == Sell`.
  - Compute a maker BUY quote derived from public trade price (similar to `xuan_m0001_public_trade_seed_intent`), then clamp by:
    - pair cap target (e.g., `sv0950` family)
    - explicit price band (the `px010_990` family)
    - min-order-scaled sizing with `fill_haircut` (the `fh010/fh020` family)
  - Enforce a hard imbalance gate (the `imb6/imb8` family) using `input.inv.net_diff` vs `cfg.max_net_diff`.
- Allow both sides to emit seed intents in the same tick **only if** the tranche model can represent it safely and shadow evidence confirms the lifecycle:
  - First pass (recommended): pick *one* side using a deterministic preference score (e.g., current `preference_score` shape) until tranche model supports multiple concurrent opens.
  - Second pass: enable true dual seed once multi-tranche support exists (Stage C).

Files:
- `src/polymarket/strategy/pair_gated_tranche.rs`
- `src/polymarket/strategy.rs` (if wiring a distinct `StrategyKind` becomes necessary; otherwise keep it as a PGT profile)

Tests:
- Add unit tests in `src/polymarket/strategy/pair_gated_tranche.rs` for:
  - per-side recent trade selection doesn’t cross-contaminate YES/NO
  - `fill_haircut` impacts size deterministically (quantize + min order)
  - `imb6/imb8` hard-block behaves as expected

### Stage C (optional) — True two-sided inventory (multi-tranche)

If “two-sided” must mean simultaneous YES_BUY and NO_BUY resting, the current single `active_tranche` model will eventually need:
- either multiple concurrent tranches, or
- a “dual-open tranche” representation with two first legs tracked independently.

This is intentionally deferred until Stage A+B are stable in dry-run.

## Dry-run safety gate

Keep D+ profile behind **dry-run-only** checks (same pattern used by xuan maker-like), and do not alter any collector/replay/raw/production control paths.
