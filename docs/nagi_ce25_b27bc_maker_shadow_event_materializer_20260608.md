# NAGI + CE25 + B27BC Maker Shadow Event Materializer - 2026-06-08

## Purpose

The maker-shadow pipeline consumes CSV, but current local artifacts often arrive
as event JSONL. The event materializer converts bounded local `*.events.jsonl`
files into a maker-shadow input CSV:

```text
scripts/materialize_nagi_ce25_b27bc_maker_shadow_input.py
```

## Safety

The script is local-only and no-order. It does not use network, WebSocket,
private keys, API credentials, order placement, cancel, redeem, canary, live,
deploy, or funding paths.

By default it excludes any path containing `smoke` or `fixture`, so synthetic
test artifacts cannot become strategy evidence accidentally.

## Semantics

The materializer reads public-trade candidate events with fields such as:

- `slug`
- `ts_ms`
- `offset_s`
- `side`
- `price` / `seed_px`
- `public_trade_px`
- `public_trade_size`

It writes:

- `nagi_ce25_b27bc_maker_shadow_input.csv`
- `manifest.json`

The output includes `visible_depth_qty`, but that value is explicitly sourced
from `public_trade_size` and labeled:

```text
public_trade_size_proxy_not_l2_depth
```

This means the output is a queue-proxy input, not L2 depth truth and not maker
fill truth. Downstream results remain `research_proxy` and require private
maker telemetry before any higher status.

## Automation Use

Heartbeat should run this materializer before input inventory. If it produces
rows, the inventory step decides whether sampled rows satisfy CE25 timing/price
gates, public-touch proxy, and the 5-share limit-order floor. Only then may the
maker-shadow pipeline run on the generated CSV.
