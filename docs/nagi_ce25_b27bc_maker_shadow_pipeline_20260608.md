# NAGI + CE25 + B27BC Maker Shadow Pipeline - 2026-06-08

## Assessment

The recommended direction is sound, with one important boundary: it is not a
copy-trading strategy. It is a research pipeline that combines four separate
lessons:

- NAGI: maker/no-fee queue edge is the alpha hypothesis.
- CE25: coverage gates decide which BTC 5m windows and price bands deserve
  observation.
- B27BC: residual closure discipline is the risk-control template.
- Xuan: fee-inclusive taker paths are the negative guardrail.

The CE25 taker/seed-px route remains blocked. Any CE25-derived candidate must
be either execution-price-first or maker-shadow-telemetry-first before it can
enter a strategy candidate set. CE25 is retained only as coverage/gating.

## Implemented Local Research Tool

Script:

```text
scripts/run_nagi_ce25_b27bc_maker_shadow.py
```

Scope:

- Reads local CSV only.
- Writes local artifacts only.
- Uses no network, no private key, no API credential, no order, no cancel, no
  redeem, no import, no canary, no live/deploy/funding pointer.
- Treats public SELL touch and visible bid depth as queue proxy evidence only.
- Never claims maker fill truth or private execution truth.

Outputs:

- `nagi_ce25_b27bc_maker_shadow_events.jsonl`
- `rolling_24h_summary.csv`
- `decision_register.json`

Input discovery:

- `scripts/materialize_nagi_ce25_b27bc_maker_shadow_input.py`
- `scripts/inventory_nagi_ce25_b27bc_maker_shadow_inputs.py`
- Converts bounded local event JSONL to CSV when non-smoke/non-fixture events
  are present.
- Scans bounded local CSV samples.
- Reports whether files are pipeline-compatible and whether sampled rows contain
  CE25-gated, public-touch, >=5-share maker-shadow opportunities.

## Gate Semantics

CE25 coverage gates:

- BTC 5m only.
- `last60 35-50`: primary.
- `last60 50-65`: primary.
- `last60 20-35`: overlay.
- `1-5m 35-50`: negative bucket, rejected.
- SOL is excluded.

NAGI maker queue proxy:

- Scores maker bid opportunities at local observed bid prices.
- Requires public SELL touch or an explicitly supplied touch proxy.
- Uses visible depth and configurable queue conversion/haircut.
- Requires at least 5 shares for any post-only/limit maker shadow opportunity.
- Uses `fee_rate = 0` for scoring, but labels all results as public proxy.

B27BC residual closer:

- Tracks active residual lots per market.
- Closes only when opposite-side maker proxy satisfies pair-cost cap.
- Emits residual discount events after a configurable age.
- Finalizes hard-timeout residuals conservatively.
- Reports residual rate and bad pair-cost share against fixed targets.

Xuan fee/taker guard:

- Any higher status requires own maker telemetry.
- Public-only and taker/ambiguous evidence remain `research_proxy`.
- Tracks the market-order minimum as 1 USDC, but this maker-only pipeline does
  not use market orders.
- The decision register keeps readiness/private-truth/live/canary claims false.

## Order Minimums

The current operational order-size guard is:

- Market orders: minimum 1 USDC.
- Limit orders: minimum 5 shares.

This pipeline is post-only maker-shadow research, so the binding rule is the
5-share limit-order minimum. A public SELL touch or visible depth sample below
5 shares is recorded as insufficient depth and cannot open a queue-proxy lot.
The 1 USDC market-order floor is kept in the decision register for future
taker/market-order guardrails, but market orders remain outside this tool's
allowed scope.

## Current Decision Boundary

The highest allowed status from this tool is:

```text
KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_RESEARCH_PROXY_PRIVATE_TELEMETRY_REQUIRED_NOT_READY
```

Promotion requires own authenticated maker-shadow telemetry, including maker fee
confirmation, zero counted taker/ambiguous fills, realized queue conversion,
pair-cost distribution, residual control, and stable rolling-window behavior.
