# TWAP boundary shadow lane

This lane is a bounded, public-data, no-submit collector for the post-Chainlink-TWAP 5-minute crypto markets. It is intentionally separate from the legacy `oracle_lag_sniping` runtime and does not change strategy decisions, order code, credentials, or shared services.

## Inputs

- Gamma event metadata queried by generated `asset-updown-5m-<round_start>` slugs.
- Polymarket RTDS `crypto_prices_twap_thirty` and `crypto_prices_twap_sixty` updates.
- Polymarket CLOB market WebSocket book and price-change events.

For RTDS, `payload.timestamp` is the Chainlink observation time, the outer
`timestamp` is the RTDS publisher time, and `payload.value` /
`full_accuracy_value` must remain exact decimal/integer data. The 30s/60s
values are lookback windows, not publication cadence. RTDS has no history or
replay after a disconnect, so gaps are evidence gaps rather than carried-forward
prices. See the [official TWAP contract](https://docs.polymarket.com/market-data/chainlink-twap).

An open RTDS socket is not sufficient coverage evidence. The collector tracks
freshness per configured asset/window stream; a stream silent beyond
`--rtds-stale-ms` is recorded as a gap and forces reconnect/resubscribe. At a
round boundary, both selected start and end observations must be within
`--boundary-tick-max-distance-ms` or the candidate is `unknown`.

Gamma descriptions that explicitly identify a stream such as
`btc-usd-twap-30s-streams` are treated as the symbol/window mapping. The
collector never infers the window from update frequency; if no explicit
30s/60s mapping exists, it records `candidate_side=unknown` and does not score
that boundary.

For CLOB latency work, `price_change` and optional `best_bid_ask` events are
the public quote-transition tape; `receive_ms` is recorded locally and is the
timestamp used for the public-reprice lead measurement. See the [official
Market Stream contract](https://docs.polymarket.com/market-data/realtime-data#market-stream).

The collector uses Node 22 built-in `fetch` and `WebSocket`; it does not install packages or load credentials.

## Safety boundary

Every run writes `mode=no-submit`, `live_orders_submitted=0`, `credentials_loaded=false`, `open_runs=[]` at exit, and lists order/sign/redeem/funding/service mutations as forbidden. The `candidate_side` field is only a diagnostic comparison of observed TWAP ticks. `settlement_observations` use public Gamma outcome prices and must not be treated as private execution or settlement authority.

## EC2 run shape

Run from the designated EC2 staging area, with a unique run directory:

```text
node collect_twap_boundary_shadow.mjs \
  --out-dir /home/ubuntu/b_strategy_staging/pm_as_ofi/<run_tag> \
  --duration-seconds 600 \
  --poll-seconds 10 \
  --assets BTC,ETH,SOL,XRP,DOGE,BNB,HYPE \
  --windows 30,60 \
  --book-emit-min-interval-ms 0 \
  --rtds-stale-ms 10000 \
  --boundary-tick-max-distance-ms 5000 \
  --source-commit <hash> \
  --no-submit
```

The durable handoff is `STARTED.json`, periodic `CHECKPOINT.json`, `EXIT.json`, `manifest.json`, and `summary.json`, with JSONL raw observations alongside them. A bounded smoke run validates connectivity; it grants no alpha, PnL, capacity, or live-readiness claim. A longer prospective capture requires a separate frozen research decision after the engineering gate.

After `EXIT.json` exists, run the read-only terminal verifier against the same
directory. It checks the no-submit terminal contract, manifest hashes/line
counts, slug-derived round boundaries, exact TWAP fields, freshness-qualified
public Gamma label diagnostics, RTDS per-stream tail coverage, CLOB timing,
boundary L2 depth, reconnect gaps, and
the missing local-candidate causal join fields:

```text
node verify_twap_boundary_shadow_capture.mjs \
  --run-dir /home/ubuntu/b_strategy_staging/pm_as_ofi/<run_tag> \
  --expect-source-commit <hash> \
  --expect-code-sha256 <collector_sha256>
```

For a completed run whose older collector missed an explicit Gamma stream
window, the public-label comparison can be reproduced without reading the
large CLOB tape:

```text
node audit_twap_boundary_shadow_candidate_labels.mjs \
  --run-dir /home/ubuntu/b_strategy_staging/pm_as_ofi/<run_tag>
```

Only rows whose selected start and end observations are within the configured
boundary-distance gate are scored. `diagnostic_candidate_side` is retained for
debugging but must not be reported as accuracy when coverage is incomplete.

To test the actual taker mechanism, map Gamma's posthoc winner label to its
token and stream the raw CLOB tape once:

```text
node audit_twap_boundary_winner_ask_window.mjs \
  --run-dir /home/ubuntu/b_strategy_staging/pm_as_ofi/<run_tag> \
  --horizon-ms 120000 \
  --thresholds 0.90,0.95,0.98,0.99,1.00
```

This reports the winner-token best ask at the boundary and any later public
ask below each threshold. It does not prove order acceptance or fill, but a
reliable result with no winning ask below the frozen limit is sufficient to
reject the stale-ask mechanism for that sample. Aggregate first reprice across
both tokens must not be used as a substitute.

The verifier's `CONDITIONAL_RESEARCH_INSUFFICIENT_EVIDENCE` result is expected
when the collector capture is not joined to an external-source tape and a
local `local_ready_ms` candidate. It must never be upgraded to PnL, execution,
or live authority from public Gamma/CLOB observations alone.

If a bounded run has complete raw JSONL and `collector_exit` but lacks terminal
artifacts because a summary reader failed, use the recovery finalizer once:

```text
node finalize_twap_boundary_shadow_capture.mjs \
  --run-dir /home/ubuntu/b_strategy_staging/pm_as_ofi/<run_tag> \
  --collector-path /home/ubuntu/b_strategy_staging/pm_as_ofi/<collector_path>
```

The finalizer preserves the raw files, records the exact source error in
`manifest.json`, and emits `EXIT.reason=duration_elapsed_recovered_after_summary_failure`.
It must not be used to turn an open or partial run into a pass.
