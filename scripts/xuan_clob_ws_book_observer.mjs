#!/usr/bin/env node
import fs from "node:fs";

const DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

function parseArgs(argv) {
  const out = {
    wsUrl: DEFAULT_WS_URL,
    durationMs: 30000,
    targetSnapshots: 100,
    maxBookAgeMs: 60000,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--markets-json") {
      out.marketsJson = next;
      i += 1;
    } else if (arg === "--out-jsonl") {
      out.outJsonl = next;
      i += 1;
    } else if (arg === "--ws-url") {
      out.wsUrl = next;
      i += 1;
    } else if (arg === "--duration-ms") {
      out.durationMs = Number(next);
      i += 1;
    } else if (arg === "--target-snapshots") {
      out.targetSnapshots = Number(next);
      i += 1;
    } else if (arg === "--max-book-age-ms") {
      out.maxBookAgeMs = Number(next);
      i += 1;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!out.marketsJson) throw new Error("--markets-json is required");
  if (!out.outJsonl) throw new Error("--out-jsonl is required");
  return out;
}

function num(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function str(value) {
  return value == null ? "" : String(value);
}

function eventTimeMs(value, fallbackMs) {
  for (const key of ["timestamp", "ts", "time", "event_time_ms", "eventTimeMs"]) {
    const parsed = num(value?.[key]);
    if (parsed != null) {
      return parsed < 10_000_000_000 ? Math.round(parsed * 1000) : Math.round(parsed);
    }
  }
  return fallbackMs;
}

function normalizeLevels(raw) {
  if (!Array.isArray(raw)) return new Map();
  const out = new Map();
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const price = num(item.price);
    const size = num(item.size);
    if (price == null || size == null || price <= 0 || size < 0) continue;
    if (size === 0) out.delete(price.toFixed(6));
    else out.set(price.toFixed(6), { price, size });
  }
  return out;
}

function sortedAsks(book) {
  return Array.from(book.asks.values()).sort((a, b) => a.price - b.price);
}

function sortedBids(book) {
  return Array.from(book.bids.values()).sort((a, b) => b.price - a.price);
}

function sideMetrics(book, observedMs) {
  const asks = sortedAsks(book);
  const bids = sortedBids(book);
  const bestAsk = asks.length ? asks[0].price : null;
  const bestBid = bids.length ? bids[0].price : null;
  const top1AskQty = asks.length ? asks[0].size : 0;
  const top5AskQty = asks.slice(0, 5).reduce((acc, lvl) => acc + lvl.size, 0);
  const ageMs = observedMs - book.lastEventMs;
  return {
    asset_id: book.assetId,
    best_ask: bestAsk,
    best_bid: bestBid,
    top1_ask_qty: top1AskQty,
    top5_ask_qty: top5AskQty,
    ask_level_count: asks.length,
    bid_level_count: bids.length,
    book_ts_ms: book.lastEventMs,
    observed_ts_ms: observedMs,
    book_age_ms: ageMs,
    hash: book.hash || null,
  };
}

function normalizeTakerSide(value) {
  const side = str(value).trim().toUpperCase();
  if (side === "BUY" || side === "SELL") return side;
  return null;
}

function publicTradeFromMessage(msg, assetToMarket, observedMs) {
  const assetId = str(msg.asset_id);
  const market = assetToMarket.get(assetId);
  if (!market) return null;
  const price = num(msg.price);
  const size = num(msg.size);
  const takerSide = normalizeTakerSide(msg.side);
  if (price == null || size == null || price <= 0 || size <= 0 || takerSide == null) return null;
  const marketSide =
    assetId === str(market.yes_asset_id)
      ? "YES"
      : assetId === str(market.no_asset_id)
        ? "NO"
        : null;
  if (marketSide == null) return null;
  return {
    event_type: "last_trade_price",
    market_id: market.market_id,
    condition_id: market.condition_id,
    slug: market.slug,
    asset_id: assetId,
    market_side: marketSide,
    taker_side: takerSide,
    price,
    size,
    trade_id: str(msg.trade_id || msg.id || msg.hash) || null,
    event_ts_ms: eventTimeMs(msg, observedMs),
    observed_ts_ms: observedMs,
    recv_lag_ms: observedMs - eventTimeMs(msg, observedMs),
  };
}

function makeBook(assetId) {
  return {
    assetId,
    bids: new Map(),
    asks: new Map(),
    lastEventMs: 0,
    lastObservedMs: 0,
    hash: null,
  };
}

function applyBookEvent(book, msg, observedMs) {
  const bids = msg.bids ?? msg.buys ?? [];
  const asks = msg.asks ?? msg.sells ?? [];
  book.bids = normalizeLevels(bids);
  book.asks = normalizeLevels(asks);
  book.lastEventMs = eventTimeMs(msg, observedMs);
  book.lastObservedMs = observedMs;
  book.hash = msg.hash ?? book.hash;
}

function applyPriceChange(book, change, parent, observedMs) {
  const price = num(change.price);
  const size = num(change.size);
  const side = str(change.side).trim().toUpperCase();
  if (price == null || size == null || price <= 0 || size < 0) return false;
  const key = price.toFixed(6);
  const target =
    side === "BUY" || side === "BID"
      ? book.bids
      : side === "SELL" || side === "ASK"
        ? book.asks
        : null;
  if (!target) return false;
  if (size === 0) target.delete(key);
  else target.set(key, { price, size });
  book.lastEventMs = eventTimeMs(change, eventTimeMs(parent, observedMs));
  book.lastObservedMs = observedMs;
  return true;
}

function applyBestBidAsk(book, msg, observedMs) {
  const bid = num(msg.best_bid);
  const ask = num(msg.best_ask);
  if (bid != null && bid > 0 && !book.bids.has(bid.toFixed(6))) {
    book.bids.set(bid.toFixed(6), { price: bid, size: 0 });
  }
  if (ask != null && ask > 0 && !book.asks.has(ask.toFixed(6))) {
    book.asks.set(ask.toFixed(6), { price: ask, size: 0 });
  }
  book.lastEventMs = eventTimeMs(msg, observedMs);
  book.lastObservedMs = observedMs;
}

function loadMarkets(path) {
  const payload = JSON.parse(fs.readFileSync(path, "utf8"));
  const raw = Array.isArray(payload) ? payload : payload.resolved_markets;
  if (!Array.isArray(raw)) throw new Error("markets JSON must be a list or contain resolved_markets");
  const markets = [];
  for (const market of raw) {
    const yes = str(market.yes_asset_id);
    const no = str(market.no_asset_id);
    const marketId = str(market.market_id);
    if (!yes || !no || !marketId) {
      throw new Error(`market missing explicit ids: ${JSON.stringify(market)}`);
    }
    markets.push({
      round_offset: market.round_offset,
      slug: str(market.slug),
      market_id: marketId,
      condition_id: str(market.condition_id || market.market_id),
      yes_asset_id: yes,
      no_asset_id: no,
      endDate: market.endDate ?? null,
    });
  }
  return markets;
}

function emitSnapshot({ market, books, out, counters, maxBookAgeMs, reason, observedMs }) {
  const yes = books.get(market.yes_asset_id);
  const no = books.get(market.no_asset_id);
  if (!yes || !no || yes.lastEventMs <= 0 || no.lastEventMs <= 0) return;
  const yesMetrics = sideMetrics(yes, observedMs);
  const noMetrics = sideMetrics(no, observedMs);
  const failures = [];
  for (const [sideName, metrics] of [
    ["YES", yesMetrics],
    ["NO", noMetrics],
  ]) {
    if (metrics.best_ask == null) failures.push(`${sideName.toLowerCase()}_best_ask_missing`);
    if (metrics.book_age_ms < 0 || metrics.book_age_ms > maxBookAgeMs) {
      failures.push(`${sideName.toLowerCase()}_book_age_out_of_bounds`);
    }
    if (metrics.ask_level_count <= 0) failures.push(`${sideName.toLowerCase()}_ask_levels_missing`);
  }
  if (failures.length) {
    counters.dropped_book_event_count += 1;
    if (counters.dropped_book_events_sample.length < 25) {
      counters.dropped_book_events_sample.push({
        slug: market.slug,
        market_id: market.market_id,
        reason: failures,
        observed_ts_ms: observedMs,
      });
    }
    return;
  }
  if (str(yes.assetId) !== str(market.yes_asset_id) || str(no.assetId) !== str(market.no_asset_id)) {
    counters.book_failures.push({
      slug: market.slug,
      market_id: market.market_id,
      reason: ["token_id_mismatch"],
      observed_ts_ms: observedMs,
    });
    return;
  }
  const snapshot = {
    snapshot_index: counters.market_snapshot_count,
    event_reason: reason,
    observed_ts_ms: observedMs,
    event_ts_ms: Math.min(yes.lastEventMs, no.lastEventMs),
    latency_ms: observedMs - Math.min(yes.lastEventMs, no.lastEventMs),
    market,
    yes: yesMetrics,
    no: noMetrics,
  };
  if (counters.latest_public_buy_trade_by_market_side) {
    const yesBuy = counters.latest_public_buy_trade_by_market_side.get(`${market.market_id}:YES`);
    const noBuy = counters.latest_public_buy_trade_by_market_side.get(`${market.market_id}:NO`);
    if (yesBuy) snapshot.latest_public_buy_trade_yes = yesBuy;
    if (noBuy) snapshot.latest_public_buy_trade_no = noBuy;
  }
  out.write(`${JSON.stringify(snapshot)}\n`);
  counters.market_snapshot_count += 1;
}

async function main() {
  const args = parseArgs(process.argv);
  const markets = loadMarkets(args.marketsJson);
  const assetToMarket = new Map();
  const books = new Map();
  for (const market of markets) {
    assetToMarket.set(market.yes_asset_id, market);
    assetToMarket.set(market.no_asset_id, market);
    books.set(market.yes_asset_id, makeBook(market.yes_asset_id));
    books.set(market.no_asset_id, makeBook(market.no_asset_id));
  }
  const out = fs.createWriteStream(args.outJsonl, { flags: "a" });
  const counters = {
    ws_open_count: 0,
    ws_disconnect_count: 0,
    reconnect_count: 0,
    subscribe_asset_count: assetToMarket.size,
    subscribe_market_count: markets.length,
    message_count: 0,
    parse_error_count: 0,
    unknown_event_count: 0,
    book_event_count: 0,
    price_change_event_count: 0,
    best_bid_ask_event_count: 0,
    last_trade_price_event_count: 0,
    public_buy_trade_event_count: 0,
    public_sell_trade_event_count: 0,
    market_snapshot_count: 0,
    book_snapshot_count: 0,
    book_failures: [],
    dropped_book_event_count: 0,
    dropped_book_events_sample: [],
    latest_public_buy_trade_by_market_side: new Map(),
  };
  const started = Date.now();
  let normalClose = false;
  let settle;
  const done = new Promise((resolve) => {
    settle = resolve;
  });
  const ws = new WebSocket(args.wsUrl);
  const stop = (reason) => {
    if (normalClose) return;
    normalClose = true;
    counters.stop_reason = reason;
    try {
      ws.close(1000, reason);
    } catch {
      // ignore close races
    }
    setTimeout(() => settle(), 250);
  };
  const timeout = setTimeout(() => stop("duration_elapsed"), Math.max(1000, args.durationMs));

  ws.onopen = () => {
    counters.ws_open_count += 1;
    const assetIds = Array.from(assetToMarket.keys());
    ws.send(
      JSON.stringify({
        type: "market",
        operation: "subscribe",
        markets: [],
        assets_ids: assetIds,
        asset_ids: assetIds,
        initial_dump: true,
        custom_feature_enabled: false,
      }),
    );
  };
  ws.onerror = (event) => {
    counters.last_error = event?.message || String(event);
    stop("ws_error");
  };
  ws.onclose = () => {
    if (!normalClose) {
      counters.ws_disconnect_count += 1;
      counters.stop_reason = "unexpected_ws_close";
      normalClose = true;
    }
    settle();
  };
  ws.onmessage = (event) => {
    const observedMs = Date.now();
    const text = String(event.data ?? "").trim();
    if (!text || text.toUpperCase() === "PING" || text.toUpperCase() === "PONG") return;
    counters.message_count += 1;
    let payload;
    try {
      payload = JSON.parse(text);
    } catch (err) {
      counters.parse_error_count += 1;
      counters.last_parse_error = String(err);
      return;
    }
    const messages = Array.isArray(payload) ? payload : [payload];
    const affected = new Map();
    for (const msg of messages) {
      if (!msg || typeof msg !== "object") continue;
      const eventType = str(msg.event_type || msg.type || msg.event);
      if (eventType === "book") {
        counters.book_event_count += 1;
        counters.book_snapshot_count += 1;
        const assetId = str(msg.asset_id);
        const book = books.get(assetId);
        const market = assetToMarket.get(assetId);
        if (!book || !market) continue;
        applyBookEvent(book, msg, observedMs);
        affected.set(market.market_id, { market, reason: "book" });
      } else if (eventType === "price_change") {
        counters.price_change_event_count += 1;
        const changes = Array.isArray(msg.price_changes) ? msg.price_changes : [];
        for (const change of changes) {
          const assetId = str(change.asset_id);
          const book = books.get(assetId);
          const market = assetToMarket.get(assetId);
          if (!book || !market) continue;
          if (applyPriceChange(book, change, msg, observedMs)) {
            affected.set(market.market_id, { market, reason: "price_change" });
          }
        }
      } else if (eventType === "best_bid_ask") {
        counters.best_bid_ask_event_count += 1;
        const assetId = str(msg.asset_id);
        const book = books.get(assetId);
        const market = assetToMarket.get(assetId);
        if (!book || !market) continue;
        applyBestBidAsk(book, msg, observedMs);
        affected.set(market.market_id, { market, reason: "best_bid_ask" });
      } else if (eventType === "last_trade_price") {
        counters.last_trade_price_event_count += 1;
        const trade = publicTradeFromMessage(msg, assetToMarket, observedMs);
        if (!trade) {
          counters.unknown_event_count += 1;
          continue;
        }
        if (trade.taker_side === "BUY") {
          counters.public_buy_trade_event_count += 1;
          counters.latest_public_buy_trade_by_market_side.set(
            `${trade.market_id}:${trade.market_side}`,
            trade,
          );
        } else {
          counters.public_sell_trade_event_count += 1;
        }
        const market = assetToMarket.get(trade.asset_id);
        if (market) affected.set(market.market_id, { market, reason: "last_trade_price" });
      } else {
        counters.unknown_event_count += 1;
      }
    }
    for (const { market, reason } of affected.values()) {
      emitSnapshot({
        market,
        books,
        out,
        counters,
        maxBookAgeMs: args.maxBookAgeMs,
        reason,
        observedMs,
      });
      if (counters.market_snapshot_count >= args.targetSnapshots) {
        stop("target_snapshots_reached");
        break;
      }
    }
  };

  await done;
  clearTimeout(timeout);
  await new Promise((resolve) => out.end(resolve));
  const finished = Date.now();
  const staleOnlyObserver =
    counters.market_snapshot_count === 0 &&
    counters.dropped_book_event_count > 0 &&
    (counters.dropped_book_events_sample || []).some((item) =>
      (item.reason || []).some((reason) => String(reason).includes("book_age_out_of_bounds")),
    );
  const result = {
    schema_version: "xuan_clob_ws_book_observer_v1",
    ws_url: args.wsUrl,
    started_ts_ms: started,
    finished_ts_ms: finished,
    elapsed_ms: finished - started,
    book_ws_used: true,
    book_audit: {
      transport: "WS",
      ws_disconnect_count: counters.ws_disconnect_count,
      reconnect_count: counters.reconnect_count,
      book_snapshot_count: counters.book_snapshot_count,
      market_snapshot_count: counters.market_snapshot_count,
      book_failures: counters.book_failures,
      dropped_book_event_count: counters.dropped_book_event_count,
      dropped_book_events_sample: counters.dropped_book_events_sample,
      stale_only_observer: staleOnlyObserver,
    },
    counters,
  };
  result.counters.latest_public_buy_trade_by_market_side = Array.from(
    counters.latest_public_buy_trade_by_market_side.values(),
  );
  console.log(JSON.stringify(result));
  const failed =
    counters.ws_disconnect_count !== 0 ||
    counters.parse_error_count !== 0 ||
    (counters.book_failures || []).length !== 0 ||
    counters.market_snapshot_count === 0;
  process.exit(failed ? 2 : 0);
}

main().catch((err) => {
  console.error(JSON.stringify({ error: String(err), stack: err?.stack }));
  process.exit(2);
});
