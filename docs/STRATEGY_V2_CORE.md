# Strategy Core (Current Mainline)

This repository now has one recommended live path:
- `PM_STRATEGY=glft_mm`
- `btc/eth/xrp/sol` `*-updown-5m` only
- real two-sided maker quoting through four execution slots

## Architecture

The system is split into three layers:
- strategy layer: emits slot intents
- execution layer: keeps, reprices, cancels, reconciles
- risk layer: net-diff gate, OFI, stale, endgame, recycle, claim

## `glft_mm`

`glft_mm` combines:
- Binance `aggTrade` anchor
- Polymarket trade-flow OFI
- GLFT intensity fitting (`10s refit / 30s window`)
- slot-keyed maker execution (`YesBuy/YesSell/NoBuy/NoSell`)

Key properties:
- no fallback to pure-Poly pricing when Binance is stale
- OFI affects both reservation-price shift and spread widening
- slot-level kill-switch suppresses the most dangerous side/direction
- quote governor limits one-tick-per-update movement to reduce churn

## Hard Risk Boundaries

Routine session risk is still anchored on:
- `PM_MAX_NET_DIFF`
- outcome floor on new buys
- stale / OFI kill-switch
- shared endgame safety valve

## Strategy Status

| Strategy | Status |
| --- | --- |
| `glft_mm` | recommended live mainline |
| `gabagool_grid` | parked baseline |
| `gabagool_corridor` | research |
| `pair_arb` | soft-deprecated |
| `dip_buy` | experimental |
| `phase_builder` | experimental |
