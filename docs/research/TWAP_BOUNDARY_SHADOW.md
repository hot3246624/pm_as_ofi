# TWAP boundary shadow lane

This lane is a bounded, public-data, no-submit collector for the post-Chainlink-TWAP 5-minute crypto markets. It is intentionally separate from the legacy `oracle_lag_sniping` runtime and does not change strategy decisions, order code, credentials, or shared services.

## Inputs

- Gamma event metadata queried by generated `asset-updown-5m-<round_start>` slugs.
- Polymarket RTDS `crypto_prices_twap_thirty` and `crypto_prices_twap_sixty` updates.
- Polymarket CLOB market WebSocket book and price-change events.

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
  --source-commit <hash> \
  --no-submit
```

The durable handoff is `STARTED.json`, periodic `CHECKPOINT.json`, `EXIT.json`, `manifest.json`, and `summary.json`, with JSONL raw observations alongside them. A bounded smoke run validates connectivity; it grants no alpha, PnL, capacity, or live-readiness claim. A longer prospective capture requires a separate frozen research decision after the engineering gate.
