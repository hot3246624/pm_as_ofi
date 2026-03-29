# Stabilize ReferenceController and Drift Mechanics

## Goal Description
Resolve the core issue where `ReferenceHealth` frequently enters `Blocked` state and triggers continuous `StaleData` cancellations, severely reducing active trading time. 

The structural churn is caused by `ReferenceHealth` strictly evaluating `drift_ticks`, while the internal model restricts its `basis_prob` adaptation via an artificially low ceiling (`GLFT_BASIS_LIVE_SOFT_CAP_ABS = 0.18`). During strong directional up-trends or down-trends in 5m updown markets, the price relative to Binance naturally exceeds this ceiling. 

Because `basis_prob` is capped mathematically, the `drift_ticks` distance inevitably grows uncontrolled. Once `drift_ticks` exceeds `12.0` (which is `0.12` probabilistic price distance), `ReferenceHealth` locks into `Blocked` and clears all orders. It's unable to recover until market momentum reverses back down below the ceiling limit, forming a structural freeze.

## Proposed Changes

### 1. Relax Basis Boundaries
#### [MODIFY] src/polymarket/glft.rs
- **`GLFT_BASIS_LIVE_SOFT_CAP_ABS`**: Increase from `0.18` to `0.45`. This prevents artificial "clamping" against the market and allows `modeled_mid` to gracefully catch up to real market trends up to 45% away from the Binance anchor, eliminating structural churn.

### 2. Widen Drift Mode Hysteresis
To stop the hyper-frequent toggle between Normal, Damped, and Frozen (35/18 times per round):
#### [MODIFY] src/polymarket/glft.rs
- `GLFT_DRIFT_DAMPED_ENTER_TICKS`: 6.0 -> 8.0
- `GLFT_DRIFT_DAMPED_EXIT_TICKS`: 5.0 -> 6.0
- `GLFT_DRIFT_FROZEN_ENTER_TICKS`: 10.0 -> 14.0
- `GLFT_DRIFT_FROZEN_EXIT_TICKS`: 8.0 -> 11.0

### 3. De-Sensitize Reference Health Guardrails
Ensure that `Blocked` is truly a last-resort safety net for extreme anomalies, not heavily triggered during momentum trends.
#### [MODIFY] src/polymarket/glft.rs
- `GLFT_REFERENCE_GUARDED_ENTER_TICKS`: 6.0 -> 10.0
- `GLFT_REFERENCE_GUARDED_EXIT_TICKS`: 5.5 -> 7.0
- `GLFT_REFERENCE_BLOCKED_ENTER_TICKS`: 12.0 -> 20.0
- `GLFT_REFERENCE_BLOCKED_EXIT_TICKS`: 10.0 -> 15.0

> [!IMPORTANT]
> Because `ReferenceHealth` strictly defines orderbook readiness, 20 ticks (20%) is a large safety net that ensures we stay `Live` and handle market friction using dynamic sizing & hedging, rather than clearing everything and going dark.

## Open Questions
- Is a 0.45 cap safe enough for your 5m updown pairs, considering the price naturally spans from `0.05` to `0.95`?

## Verification Plan
1. Run `cargo test` to ensure hysteresis tests remain sound.
2. Launch a dry-run and confirm `ref.blocked_ms` shrinks dramatically (to ideally ~0s) and `StaleData` cancels subside to <= 2.
