# Local Agg Source Payload Capability Table

更新时间：2026-05-13 21:46Z

本文是只读研究记录，目的是确认 Coinbase ticker 与 Binance top-of-book 是否能支持 BNB microstructure enrichment。没有 Rust 代码改动，没有 runtime 行为改动，没有重启。

## Official Sources Checked

- Binance Spot WebSocket Streams: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- Coinbase Exchange WebSocket Channels: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

## Current Runtime Versus Available Payload

### Binance

Current code:

- Subscription: `<symbol>@trade`
- Code path: `src/bin/polymarket_v2.rs:4104` and `src/bin/polymarket_v2.rs:5327`
- Parsed fields: symbol, trade price `p`, trade time `T` or event time `E`
- Ignored fields already in trade payload: quantity `q`, maker flag `m`, trade id

Official payload options:

- `<symbol>@trade` gives raw trade price, quantity, trade time, event time, and maker flag.
- `<symbol>@bookTicker` gives best bid price `b`, best bid quantity `B`, best ask price `a`, best ask quantity `A`, update id, and symbol.
- `<symbol>@depth5` / `depth10` / `depth20` gives partial top book levels and quantities.

Diagnostic implication:

- For BNB, Binance can supply exactly the first diagnostic target we need: top-of-book bid/ask and bid/ask size.
- The lowest-risk addition is a parallel diagnostic `<symbol>@bookTicker` subscription for watched local-agg symbols.
- We should not replace current trade price behavior initially. Capture `bookTicker` fields as optional diagnostics and compute `mid`, `spread_bps`, and `microprice` only in logs/replay.

Recommended fields to capture:

- `binance_trade_price`
- `binance_trade_qty`
- `binance_trade_ts_ms`
- `binance_book_bid`
- `binance_book_bid_qty`
- `binance_book_ask`
- `binance_book_ask_qty`
- `binance_book_mid`
- `binance_book_microprice`
- `binance_book_spread_bps`
- `binance_book_update_id`
- `received_ts_ms`

### Coinbase Exchange

Current code:

- Subscription: `ticker`
- Code path: `src/bin/polymarket_v2.rs:4273` and `src/bin/polymarket_v2.rs:5481`
- Parsed fields: product id, ticker `price`, RFC3339 `time`
- Ignored fields available in ticker payload: best bid/ask, best bid/ask size, last size, side, trade id, 24h/30d stats

Official payload options:

- `ticker` provides match-driven price updates and includes best bid, best ask, their sizes, side, trade id, last size, and time.
- `ticker_batch` has the same schema but emits less frequently.
- `level2` / `level2_batch` can provide full book snapshots and updates; this is richer but higher complexity and not needed for the first diagnostic pass.

Diagnostic implication:

- Coinbase is the fastest enrichment win because the current feed already subscribes to ticker. We only need to parse additional fields already present in the same message.
- It can immediately provide `mid`, `spread_bps`, `microprice`, and last trade size without adding a second connection.
- For BNB, Coinbase is not always in the runtime selected local source set, but its diagnostic BBO can help decide whether excluded Coinbase was a better-quality reference.

Recommended fields to capture:

- `coinbase_ticker_price`
- `coinbase_last_size`
- `coinbase_side`
- `coinbase_best_bid`
- `coinbase_best_bid_size`
- `coinbase_best_ask`
- `coinbase_best_ask_size`
- `coinbase_mid`
- `coinbase_microprice`
- `coinbase_spread_bps`
- `coinbase_trade_id`
- `exchange_time_ms`
- `received_ts_ms`

## Runtime Design Consequence

Current `LocalPriceHub` cannot transport these fields because its core type is only:

```rust
(price, ts_ms, LocalPriceSource)
```

and shared ingress local-price wire messages carry only:

```rust
symbol, price, ts_ms, source
```

Therefore an enrichment patch should not be a parser-only edit. It needs a diagnostic tick struct that preserves the existing behavioral `price` while carrying optional metadata.

## Minimal Safe Enrichment Order

1. Coinbase ticker parser expansion:
   - no new subscription;
   - no new upstream load;
   - optional fields only.
2. Binance `bookTicker` diagnostic stream:
   - add one compact top-of-book stream per watched symbol;
   - no full depth;
   - cap and measure log volume before enabling broadly.
3. Add boundary-tape optional microstructure fields:
   - preserve existing `price`/`ts_ms` behavior;
   - no selector/gate reads the new fields.
4. Build BNB replay:
   - compare current trade/ticker price vs Coinbase/Binance mid, microprice, spread, and book age;
   - only then consider whether BNB needs source eligibility/model changes.

## BNB Research Hypothesis

BNB residual tails may come from a mismatch between last-trade/ticker price and a book-quality reference at the boundary. If this is true, BNB errors should cluster when:

- trade price diverges from top-of-book mid or microprice;
- spread widens;
- bid/ask size imbalance is large;
- exchange timestamp and received timestamp diverge;
- selected source set excludes a better-quality Coinbase/Binance BBO.

If these buckets do not explain the residual max, the likely next explanation is target alignment or an unavailable oracle-internal source.

## Safety Position

This is not a new model and not a deploy request.

Any implementation that changes `LocalPriceHub`, shared ingress wire schema, or diagnostics emitted by `pm-local-agg-challenger.service` still requires explicit approval if it needs a service restart. It must remain dry-run, must not touch live trading, and must not restart `pm-shared-ingress-broker.service` without separate proof that shared ingress itself is broken.
