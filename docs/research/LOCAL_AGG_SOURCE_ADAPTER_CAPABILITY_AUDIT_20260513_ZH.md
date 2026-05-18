# Local Agg Source Adapter Capability Audit

更新时间：2026-05-13 21:16Z

本文是只读代码审计结果。它不改变 runtime，不重启服务，不部署，也不改变任何 source selection、gate 或 threshold。

## Runtime Safety Snapshot

21:16Z EC2 check:

- service: `pm-local-agg-challenger.service` active/running
- server HEAD: `08f6dc6`
- restarts: 0
- run id: `20260513_045906`
- accepted=252, gated=314
- accepted_side_errors=0
- accepted_max_bps=6.893443
- accepted_p95_bps=3.750916
- latency_p95_ms=50.0, latency_max_ms=279.0

Official accepted max is still the DOGE 6.893443bps row. A BNB 5.166422bps row appeared in row-level diagnostics, but final `gate_status=gated` with `gate_reason=round_max_margin_too_wide`; it is useful research evidence, not an official accepted hard-tail.

## Audit Result

Current local price adapter schema is fundamentally price-tick only. It can support source-lag/window research, but cannot yet support true microstructure filtering or microprice/depth models.

Core bottlenecks:

- `LocalPriceHub` broadcasts only `(price, ts_ms, source)` and stores `VecDeque<(price, ts_ms, source)>`.
- shared ingress wire for `LocalPriceSnapshot` / `LocalPriceTick` carries only `symbol`, `price`, `ts_ms`, `source`.
- boundary tape stores only `(ts_ms, price)`.
- `LocalBoundaryTickProbe` emits only `ts_ms`, `offset_ms`, `price`.

Code references:

- `src/bin/polymarket_v2.rs:4796` local price shared-ingress wire schema.
- `src/bin/polymarket_v2.rs:5175` `LocalPriceHub` in-memory schema.
- `src/bin/polymarket_v2.rs:11035` boundary tape schema.
- `src/bin/polymarket_v2.rs:11100` boundary tape collection.

## Source Capability Table

| Source | Current subscription | Current parsed fields | Current instrument read | Microstructure available in runtime? | Notes |
|---|---|---|---|---|---|
| Binance | `wss://stream.binance.com:9443/stream?...@trade` | trade price `p`, trade time `T` or event time `E` | spot trade / last trade | No | Raw payload includes more than used, but current parser returns only `(symbol, price, ts_ms)`. |
| Bybit | `wss://stream.bybit.com/v5/public/spot`, `publicTrade.*` | trade price `p`, time `T` or `ts` | spot public trade / last trade | No | Side/size are present in examples/tests but ignored by parser. |
| OKX | `wss://ws.okx.com:8443/ws/v5/public`, `trades` | trade price `px`, time `ts` | spot trade / last trade | No | Size/side are present in payload class but ignored by parser. |
| Coinbase | `wss://ws-feed.exchange.coinbase.com`, `ticker` | ticker `price`, RFC3339 `time` | ticker last/price | No | Current parser ignores any bid/ask/size fields even if the feed provides them. |
| Hyperliquid | `wss://api.hyperliquid.xyz/ws`, `allMids` | all-mids price, `data.time` | mid price, HYPE only | Partial | It is already a mid-like price, but still no bid/ask/depth/size. |

## Consequences For BNB

BNB tails currently use Binance/Bybit/OKX/Coinbase visible prices, but for BNB the adapter only records trade/ticker price and timestamp. Therefore we cannot yet answer:

- whether a high-error row came from a thin last trade rather than a stable top-of-book mid;
- whether Binance/Bybit spread was wide or one side was stale;
- whether microprice would have moved toward RTDS close;
- whether BNB-specific source disagreement came from market depth or only from lag;
- whether mark/index would have been closer than spot last trade.

The gated BNB 5.166422bps row reinforces this: final gate contained it, but its row-level shape shows the current tape can surface BNB stress without explaining whether the problem is stale trade, price-kind mismatch, or target alignment.

## Minimal Diagnostic Upgrade Path

The first implementation should be diagnostic-only and behind a flag such as `PM_LOCAL_AGG_MICROSTRUCTURE_DIAG=1`.

### Step 1: Add a richer internal tick type

Replace or parallel `broadcast::Sender<(f64, u64, LocalPriceSource)>` with a diagnostics-capable struct:

```rust
struct LocalPriceTick {
    symbol: String,
    source: LocalPriceSource,
    price: f64,
    ts_ms: u64,
    price_kind: LocalPriceKind,
    bid: Option<f64>,
    ask: Option<f64>,
    bid_size: Option<f64>,
    ask_size: Option<f64>,
    spread_bps: Option<f64>,
    microprice: Option<f64>,
    mark_price: Option<f64>,
    index_price: Option<f64>,
    exchange_ts_ms: Option<u64>,
    received_ts_ms: u64,
}
```

Keep `price` as the existing behavioral field. All new fields are diagnostics-only.

### Step 2: Extend shared ingress schema compatibly

Add optional fields to `LocalPriceSnapshot` and `LocalPriceTick`; clients that do not use them keep working.

Do not change `MarketBookTick`; that is Polymarket L1, not the CEX/local source book.

### Step 3: Add source-specific enrichers

Do not rewrite all sources at once. Recommended order:

1. Coinbase ticker: easiest enrichment candidate because the existing subscription is already `ticker`; audit payload fields first, then capture optional bid/ask/size if present.
2. Binance: add a parallel best-bid/ask or book-ticker diagnostic stream for watched symbols, not full depth.
3. Bybit and OKX: add top-of-book/ticker diagnostics after Binance/Coinbase are stable.
4. Hyperliquid: keep `allMids` as `price_kind=mid`; add L2 only if HYPE tails remain unresolved after deterministic HYPE selector.

### Step 4: Extend boundary tape without changing selection

Add optional `microstructure` to boundary tick probes, but keep current `price` and `ts_ms` fields unchanged.

The evaluator should first answer whether BNB historical/current tails improve under:

- top-of-book mid;
- microprice;
- mark/index;
- stale-by-exchange-ts filter;
- spread/depth bucket attribution.

## Safety Guardrails

- No runtime selection reads the new fields in the first phase.
- No thresholds or gates change.
- No source is added or removed from the aggregation model.
- No shared ingress restart unless explicitly approved and required for a diagnostic deployment.
- Log size must be measured before enabling broadly; top-of-book summaries only, not full order book dumps.

## Decision

Recommended next research action: keep Option B1 deterministic HYPE+DOGE-only as the runtime proposal awaiting explicit approval, while treating BNB as a diagnostics/schema problem. The next unattended step can be a source payload capability table or a diagnostic design patch, but no Rust behavior change should be made without approval.
