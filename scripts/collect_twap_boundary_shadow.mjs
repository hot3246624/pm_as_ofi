#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import process from "node:process";

const RTDS_URL = "wss://ws-live-data.polymarket.com";
const CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_URL = "https://gamma-api.polymarket.com/events";
const ROUND_SECONDS = 300;
const BOOK_EMIT_MIN_INTERVAL_MS = 1000;
const DEFAULT_ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"];
const DEFAULT_WINDOWS = [30, 60];

function parseArgs(argv) {
  const args = {
    durationSeconds: 90,
    pollSeconds: 10,
    assets: DEFAULT_ASSETS,
    windows: DEFAULT_WINDOWS,
    outDir: null,
    sourceCommit: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) throw new Error(`missing value for ${arg}`);
      i += 1;
      return argv[i];
    };
    if (arg === "--duration-seconds") args.durationSeconds = Number(next());
    else if (arg === "--poll-seconds") args.pollSeconds = Number(next());
    else if (arg === "--assets") args.assets = next().split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
    else if (arg === "--windows") args.windows = next().split(",").map((x) => Number(x.trim())).filter(Boolean);
    else if (arg === "--out-dir") args.outDir = next();
    else if (arg === "--source-commit") args.sourceCommit = next();
    else if (arg === "--no-submit") continue;
    else if (arg === "--help") {
      console.log("Usage: collect_twap_boundary_shadow.mjs --out-dir DIR [--duration-seconds N] [--poll-seconds N] [--assets BTC,ETH] [--windows 30,60] [--source-commit HASH] --no-submit");
      process.exit(0);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!args.outDir) throw new Error("--out-dir is required");
  if (!Number.isFinite(args.durationSeconds) || args.durationSeconds < 10 || args.durationSeconds > 86400) {
    throw new Error("--duration-seconds must be between 10 and 86400");
  }
  if (!Number.isFinite(args.pollSeconds) || args.pollSeconds < 2 || args.pollSeconds > 60) {
    throw new Error("--poll-seconds must be between 2 and 60");
  }
  if (args.windows.some((windowS) => ![30, 60].includes(windowS))) throw new Error("--windows only supports 30 and 60");
  if (!args.assets.length || !args.windows.length) throw new Error("assets and windows must not be empty");
  return args;
}

function nowIso(ms = Date.now()) {
  return new Date(ms).toISOString();
}

function appendJsonl(outDir, name, row) {
  fs.appendFileSync(path.join(outDir, name), `${JSON.stringify(row)}\n`);
}

function writeJson(outDir, name, value) {
  fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(value, null, 2)}\n`);
}

function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

function fileLineCount(filePath) {
  if (!fs.existsSync(filePath)) return 0;
  const text = fs.readFileSync(filePath, "utf8");
  return text ? text.split("\n").filter(Boolean).length : 0;
}

function jsonMaybe(value, fallback) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return fallback;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function unixSecondsForRound(roundStart) {
  return Math.floor(roundStart / ROUND_SECONDS) * ROUND_SECONDS;
}

function parseSlug(slug) {
  const match = /^([a-z0-9]+)-updown-5m-(\d+)$/.exec(slug);
  if (!match) return null;
  return { asset: match[1].toUpperCase(), startTs: Number(match[2]), endTs: Number(match[2]) + ROUND_SECONDS };
}

function assetSymbol(asset) {
  return `${asset.toLowerCase()}/usd`;
}

function inferTwapWindow(description) {
  const text = String(description || "");
  const match = text.match(/(?:over|using|based on)[^\n]{0,80}?(30|60)[- ]second/i) || text.match(/(30|60)[- ]second[^\n]{0,80}?TWAP/i);
  return match ? Number(match[1]) : null;
}

function asNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function parseTimestampMs(value) {
  if (value == null) return null;
  const number = Number(value);
  if (Number.isFinite(number)) return number < 10_000_000_000 ? number * 1000 : number;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function compactLevels(levelMap, side, limit = 10) {
  const levels = [...levelMap.entries()]
    .map(([price, size]) => ({ price: Number(price), size: Number(size) }))
    .filter((level) => Number.isFinite(level.price) && Number.isFinite(level.size) && level.size > 0)
    .sort((a, b) => side === "BUY" ? b.price - a.price : a.price - b.price)
    .slice(0, limit);
  return levels;
}

function depthWithin(levels, best, side, distance) {
  if (best == null) return 0;
  return levels
    .filter((level) => side === "BUY" ? level.price >= best - distance : level.price <= best + distance)
    .reduce((sum, level) => sum + level.size, 0);
}

function bookSummary(book) {
  const bids = compactLevels(book.bids, "BUY");
  const asks = compactLevels(book.asks, "SELL");
  const bestBid = bids.length ? bids[0].price : null;
  const bestAsk = asks.length ? asks[0].price : null;
  const spread = bestBid != null && bestAsk != null ? bestAsk - bestBid : null;
  return {
    best_bid: bestBid,
    best_ask: bestAsk,
    spread,
    bids,
    asks,
    depth_shares_within_1c: {
      bid: depthWithin(bids, bestBid, "BUY", 0.01),
      ask: depthWithin(asks, bestAsk, "SELL", 0.01),
    },
    depth_shares_within_2c: {
      bid: depthWithin(bids, bestBid, "BUY", 0.02),
      ask: depthWithin(asks, bestAsk, "SELL", 0.02),
    },
    depth_shares_within_5c: {
      bid: depthWithin(bids, bestBid, "BUY", 0.05),
      ask: depthWithin(asks, bestAsk, "SELL", 0.05),
    },
  };
}

function settlementFromMetadata(meta) {
  const prices = meta.outcome_prices.map(asNumber);
  if (prices.length < 2 || !prices.every((x) => x != null)) return { settled: false, outcome: null, prices };
  if (prices[0] >= 0.999 && prices[1] <= 0.001) return { settled: true, outcome: "Up", prices };
  if (prices[1] >= 0.999 && prices[0] <= 0.001) return { settled: true, outcome: "Down", prices };
  return { settled: false, outcome: null, prices };
}

function safeWebSocket(url, onMessage, onOpen, onClose, onError) {
  const ws = new WebSocket(url);
  ws.addEventListener("open", onOpen);
  ws.addEventListener("message", (event) => {
    const data = event.data;
    if (typeof data === "string") onMessage(data);
    else if (data instanceof ArrayBuffer) onMessage(Buffer.from(data).toString("utf8"));
    else if (ArrayBuffer.isView(data)) onMessage(Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString("utf8"));
    else if (data && typeof data.text === "function") data.text().then(onMessage).catch(onError);
    else onMessage(String(data));
  });
  ws.addEventListener("close", onClose);
  ws.addEventListener("error", onError);
  return ws;
}

async function fetchJson(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal, headers: { accept: "application/json" } });
    if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

function createState(args, outDir) {
  return {
    args,
    outDir,
    startedAtMs: Date.now(),
    stopping: false,
    finished: false,
    metadata: new Map(),
    metadataFingerprints: new Map(),
    tokenToMarket: new Map(),
    twapTicks: [],
    books: new Map(),
    bookLastEmittedMs: new Map(),
    boundaryObserved: new Set(),
    settlementObserved: new Set(),
    gapEvents: [],
    wsStates: { rtds: "starting", clob: "starting" },
    wsReconnects: { rtds: 0, clob: 0 },
    intervals: [],
    sockets: [],
    counts: { metadata: 0, twap_ticks: 0, book_events: 0, boundaries: 0, settlements: 0 },
  };
}

function log(state, message, extra = {}) {
  const line = { ts: nowIso(), message, ...extra };
  fs.appendFileSync(path.join(state.outDir, "run.log"), `${JSON.stringify(line)}\n`);
  console.log(`[${line.ts}] ${message}`);
}

function writeCheckpoint(state) {
  writeJson(state.outDir, "CHECKPOINT.json", {
    updated_at: nowIso(),
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    counts: state.counts,
    markets: state.metadata.size,
    ws_states: state.wsStates,
    ws_reconnects: state.wsReconnects,
    gap_events: state.gapEvents,
  });
}

function normalizeMarket(event, args, fetchedAtMs) {
  const slug = String(event?.slug || "");
  const parsedSlug = parseSlug(slug);
  if (!parsedSlug || !args.assets.includes(parsedSlug.asset)) return null;
  const market = Array.isArray(event?.markets) ? event.markets[0] || {} : event || {};
  // Gamma's startDate is the market creation time, not the 5m round start.
  // The generated slug is the canonical boundary key for this market family.
  const startTs = parsedSlug.startTs;
  const endTs = parsedSlug.endTs;
  const tokenIds = jsonMaybe(market?.clobTokenIds ?? event?.clobTokenIds, []);
  const outcomes = jsonMaybe(market?.outcomes ?? event?.outcomes, ["Up", "Down"]);
  const outcomePrices = jsonMaybe(market?.outcomePrices ?? event?.outcomePrices, []);
  return {
    slug,
    asset: parsedSlug.asset,
    symbol: assetSymbol(parsedSlug.asset),
    start_ts: Math.round(startTs),
    end_ts: Math.round(endTs),
    fetched_at: nowIso(fetchedAtMs),
    closed: Boolean(event?.closed ?? market?.closed),
    closed_time: event?.closedTime || market?.closedTime || null,
    active: Boolean(event?.active ?? market?.active),
    twap_window_s: inferTwapWindow(event?.description || market?.description),
    description: String(event?.description || market?.description || ""),
    token_ids: tokenIds.map(String),
    outcomes: outcomes.map(String),
    outcome_prices: outcomePrices.map(String),
    source: "gamma_public_event_slug",
  };
}

function stableMetadataFingerprint(meta) {
  return JSON.stringify({
    slug: meta.slug,
    asset: meta.asset,
    start_ts: meta.start_ts,
    end_ts: meta.end_ts,
    closed: meta.closed,
    closed_time: meta.closed_time,
    active: meta.active,
    twap_window_s: meta.twap_window_s,
    token_ids: meta.token_ids,
    outcomes: meta.outcomes,
    outcome_prices: meta.outcome_prices,
  });
}

function upsertMetadata(state, meta) {
  const previous = state.metadata.get(meta.slug);
  state.metadata.set(meta.slug, meta);
  for (const tokenId of meta.token_ids) state.tokenToMarket.set(tokenId, meta.slug);
  const fingerprint = stableMetadataFingerprint(meta);
  if (state.metadataFingerprints.get(meta.slug) !== fingerprint) {
    state.metadataFingerprints.set(meta.slug, fingerprint);
    appendJsonl(state.outDir, "market_metadata.jsonl", meta);
    state.counts.metadata += 1;
  }
  return !previous;
}

async function pollGamma(state) {
  const nowSec = Math.floor(Date.now() / 1000);
  const roundStarts = new Set();
  for (let delta = -2; delta <= 2; delta += 1) roundStarts.add(unixSecondsForRound(nowSec) + delta * ROUND_SECONDS);
  const jobs = [];
  for (const asset of state.args.assets) {
    for (const roundStart of roundStarts) jobs.push({ asset, roundStart, slug: `${asset.toLowerCase()}-updown-5m-${roundStart}` });
  }
  let addedTokens = false;
  await Promise.all(jobs.map(async (job) => {
    try {
      const url = `${GAMMA_URL}?slug=${encodeURIComponent(job.slug)}&limit=1`;
      const response = await fetchJson(url);
      const event = Array.isArray(response) ? response[0] : response;
      const meta = normalizeMarket(event, state.args, Date.now());
      if (meta) {
        const beforeTokenCount = state.tokenToMarket.size;
        upsertMetadata(state, meta);
        if (state.tokenToMarket.size !== beforeTokenCount) addedTokens = true;
      }
    } catch (error) {
      state.gapEvents.push({ ts: nowIso(), source: "gamma", slug: job.slug, error: String(error.message || error) });
    }
  }));
  return addedTokens;
}

function sendClobSubscription(state, force = false) {
  const socket = [...state.sockets].reverse().find((candidate) => candidate.__kind === "clob");
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const ids = [...state.tokenToMarket.keys()].sort();
  const fingerprint = ids.join(",");
  if (!force && socket.__subFingerprint === fingerprint) return;
  if (!ids.length) return;
  socket.send(JSON.stringify({ assets_ids: ids, type: "market" }));
  socket.__subFingerprint = fingerprint;
  log(state, "clob_subscribed", { assets: ids.length });
}

function updateBookFromSnapshot(state, message, receiveMs) {
  const assetId = String(message.asset_id || "");
  if (!assetId) return;
  const book = { bids: new Map(), asks: new Map(), market: message.market || null, last_event_ts: message.timestamp || null, last_receive_ms: receiveMs };
  for (const level of Array.isArray(message.bids) ? message.bids : []) {
    const price = asNumber(level.price);
    const size = asNumber(level.size);
    if (price != null && size != null && size > 0) book.bids.set(price.toFixed(6), size);
  }
  for (const level of Array.isArray(message.asks) ? message.asks : []) {
    const price = asNumber(level.price);
    const size = asNumber(level.size);
    if (price != null && size != null && size > 0) book.asks.set(price.toFixed(6), size);
  }
  state.books.set(assetId, book);
  writeBookEvent(state, assetId, "book", message, receiveMs);
}

function updateBookFromPriceChange(state, message, receiveMs) {
  for (const change of Array.isArray(message.price_changes) ? message.price_changes : []) {
    const assetId = String(change.asset_id || "");
    const priceNumber = asNumber(change.price);
    const size = asNumber(change.size);
    if (!assetId || priceNumber == null || size == null) continue;
    let book = state.books.get(assetId);
    if (!book) {
      book = { bids: new Map(), asks: new Map(), market: message.market || null, last_event_ts: null, last_receive_ms: receiveMs };
      state.books.set(assetId, book);
    }
    const map = change.side === "BUY" ? book.bids : book.asks;
    const key = priceNumber.toFixed(6);
    if (size <= 0) map.delete(key); else map.set(key, size);
    book.market = message.market || book.market;
    book.last_event_ts = message.timestamp || book.last_event_ts;
    book.last_receive_ms = receiveMs;
    writeBookEvent(state, assetId, "price_change", { ...message, price_change: change }, receiveMs);
  }
}

function writeBookEvent(state, assetId, eventKind, message, receiveMs) {
  const book = state.books.get(assetId);
  if (!book) return;
  const lastEmittedMs = state.bookLastEmittedMs.get(assetId) || 0;
  if (receiveMs - lastEmittedMs < BOOK_EMIT_MIN_INTERVAL_MS && eventKind !== "book" && lastEmittedMs > 0) return;
  state.bookLastEmittedMs.set(assetId, receiveMs);
  const summary = bookSummary(book);
  appendJsonl(state.outDir, "book_events.jsonl", {
    receive_ts: nowIso(receiveMs),
    receive_ms: receiveMs,
    event_ts: message.timestamp || null,
    event_kind: eventKind,
    market_id: message.market || book.market || null,
    asset_id: assetId,
    slug: state.tokenToMarket.get(assetId) || null,
    ...summary,
  });
  state.counts.book_events += 1;
}

function handleRtdsMessage(state, raw) {
  let message;
  try { message = JSON.parse(raw); } catch { return; }
  if (message?.type !== "update" || !message.payload) return;
  const payload = message.payload;
  const symbol = String(payload.symbol || "").toLowerCase();
  const asset = symbol.split("/")[0]?.toUpperCase();
  if (!state.args.assets.includes(asset)) return;
  const windowS = asNumber(payload.window_s);
  if (!state.args.windows.includes(windowS)) return;
  const receiveMs = Date.now();
  const row = {
    receive_ts: nowIso(receiveMs),
    receive_ms: receiveMs,
    publisher_ts: message.timestamp || null,
    observation_ts: payload.timestamp || null,
    topic: message.topic || null,
    symbol,
    asset,
    window_s: windowS,
    value: asNumber(payload.value),
    full_accuracy_value: payload.full_accuracy_value == null ? null : String(payload.full_accuracy_value),
  };
  state.twapTicks.push(row);
  appendJsonl(state.outDir, "twap_ticks.jsonl", row);
  state.counts.twap_ticks += 1;
}

function handleClobMessage(state, raw) {
  let message;
  try { message = JSON.parse(raw); } catch { return; }
  const receiveMs = Date.now();
  if (message.event_type === "book") updateBookFromSnapshot(state, message, receiveMs);
  else if (message.event_type === "price_change") updateBookFromPriceChange(state, message, receiveMs);
}

function latestTick(state, asset, windowS, predicate) {
  const ticks = state.twapTicks.filter((tick) => tick.asset === asset && tick.window_s === windowS && predicate(tick));
  return ticks.length ? ticks[ticks.length - 1] : null;
}

function firstTick(state, asset, windowS, predicate) {
  return state.twapTicks.find((tick) => tick.asset === asset && tick.window_s === windowS && predicate(tick)) || null;
}

function boundaryObservation(state, meta, observedAtMs) {
  const windowS = meta.twap_window_s || 30;
  const startMs = meta.start_ts * 1000;
  const endMs = meta.end_ts * 1000;
  const startBefore = latestTick(state, meta.asset, windowS, (tick) => Number(tick.observation_ts) <= startMs);
  const startAfter = firstTick(state, meta.asset, windowS, (tick) => Number(tick.observation_ts) >= startMs);
  const endBefore = latestTick(state, meta.asset, windowS, (tick) => Number(tick.observation_ts) <= endMs);
  const endAfter = firstTick(state, meta.asset, windowS, (tick) => Number(tick.observation_ts) >= endMs);
  const startTick = startBefore || startAfter;
  const endTick = endBefore || endAfter;
  const startValue = startTick?.value ?? null;
  const endValue = endTick?.value ?? null;
  const candidateSide = startValue == null || endValue == null ? "unknown" : endValue >= startValue ? "Up" : "Down";
  const tokenSummaries = {};
  for (const tokenId of meta.token_ids) {
    const book = state.books.get(tokenId);
    tokenSummaries[tokenId] = book ? { ...bookSummary(book), book_receive_ts: nowIso(book.last_receive_ms), book_age_ms: observedAtMs - book.last_receive_ms } : null;
  }
  return {
    observed_at: nowIso(observedAtMs),
    observed_at_ms: observedAtMs,
    slug: meta.slug,
    asset: meta.asset,
    start_ts: meta.start_ts,
    end_ts: meta.end_ts,
    twap_window_s: windowS,
    start_tick: startTick,
    end_tick: endTick,
    start_tick_selection: startBefore ? "latest_at_or_before_start" : startAfter ? "first_at_or_after_start" : "missing",
    end_tick_selection: endBefore ? "latest_at_or_before_end" : endAfter ? "first_at_or_after_end" : "missing",
    candidate_start_value: startValue,
    candidate_end_value: endValue,
    candidate_side: candidateSide,
    candidate_label_kind: "diagnostic_only_not_settlement_truth",
    token_books: tokenSummaries,
    orderable_diagnostic: Object.values(tokenSummaries).some((book) => book?.best_ask != null && book?.best_bid != null),
    public_settlement_source: "gamma_outcome_prices_only",
  };
}

function recordBoundariesAndSettlements(state) {
  const nowMs = Date.now();
  for (const meta of state.metadata.values()) {
    if (meta.end_ts * 1000 <= nowMs && nowMs <= (meta.end_ts + 105) * 1000 && !state.boundaryObserved.has(meta.slug)) {
      const observation = boundaryObservation(state, meta, nowMs);
      state.boundaryObserved.add(meta.slug);
      appendJsonl(state.outDir, "boundary_observations.jsonl", observation);
      state.counts.boundaries += 1;
    }
    if (meta.end_ts * 1000 + 75_000 <= nowMs && nowMs <= (meta.end_ts + 180) * 1000 && !state.settlementObserved.has(meta.slug)) {
      const settlement = settlementFromMetadata(meta);
      if (settlement.settled) {
        const boundaryPath = path.join(state.outDir, "boundary_observations.jsonl");
        let candidateSide = null;
        if (fs.existsSync(boundaryPath)) {
          for (const line of fs.readFileSync(boundaryPath, "utf8").split("\n")) {
            if (!line) continue;
            const row = JSON.parse(line);
            if (row.slug === meta.slug) candidateSide = row.candidate_side;
          }
        }
        appendJsonl(state.outDir, "settlement_observations.jsonl", {
          observed_at: nowIso(nowMs),
          observed_at_ms: nowMs,
          slug: meta.slug,
          asset: meta.asset,
          end_ts: meta.end_ts,
          closed: meta.closed,
          closed_time: meta.closed_time,
          outcome: settlement.outcome,
          outcome_prices: settlement.prices,
          candidate_side: candidateSide,
          candidate_match: candidateSide && candidateSide !== "unknown" ? candidateSide === settlement.outcome : null,
          source: "gamma_public_outcome_prices",
          label_kind: "public_settlement_observation_not_execution_truth",
        });
        state.settlementObserved.add(meta.slug);
        state.counts.settlements += 1;
      }
    }
  }
}

function connectRtds(state) {
  const socket = safeWebSocket(
    RTDS_URL,
    (raw) => handleRtdsMessage(state, raw),
    () => {
      state.wsStates.rtds = "open";
      socket.send(JSON.stringify({
        action: "subscribe",
        subscriptions: state.args.windows.map((windowS) => ({ topic: `crypto_prices_twap_${windowS === 30 ? "thirty" : "sixty"}`, type: "update" })),
      }));
      log(state, "rtds_subscribed", { topics: state.args.windows });
    },
    () => {
      state.wsStates.rtds = "closed";
      if (!state.stopping) {
        state.gapEvents.push({ ts: nowIso(), source: "rtds", error: "socket_closed" });
        state.wsReconnects.rtds += 1;
        setTimeout(() => { if (!state.stopping) connectRtds(state); }, 2000);
      }
    },
    (error) => { state.wsStates.rtds = "error"; state.gapEvents.push({ ts: nowIso(), source: "rtds", error: String(error.message || error) }); },
  );
  socket.__kind = "rtds";
  state.sockets.push(socket);
}

function connectClob(state) {
  const socket = safeWebSocket(
    CLOB_URL,
    (raw) => handleClobMessage(state, raw),
    () => {
      state.wsStates.clob = "open";
      sendClobSubscription(state, true);
    },
    () => {
      state.wsStates.clob = "closed";
      if (!state.stopping) {
        state.gapEvents.push({ ts: nowIso(), source: "clob", error: "socket_closed" });
        state.wsReconnects.clob += 1;
        setTimeout(() => { if (!state.stopping) connectClob(state); }, 2000);
      }
    },
    (error) => { state.wsStates.clob = "error"; state.gapEvents.push({ ts: nowIso(), source: "clob", error: String(error.message || error) }); },
  );
  socket.__kind = "clob";
  state.sockets.push(socket);
}

function buildSummary(state, endedAtMs, reason) {
  const settlements = [];
  const settlementPath = path.join(state.outDir, "settlement_observations.jsonl");
  if (fs.existsSync(settlementPath)) {
    for (const line of fs.readFileSync(settlementPath, "utf8").split("\n")) if (line) settlements.push(JSON.parse(line));
  }
  const matches = settlements.filter((x) => x.candidate_match === true).length;
  const scored = settlements.filter((x) => typeof x.candidate_match === "boolean").length;
  return {
    generated_at: nowIso(endedAtMs),
    reason,
    duration_seconds: (endedAtMs - state.startedAtMs) / 1000,
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    economic_claim: "none",
    counts: state.counts,
    market_count: state.metadata.size,
    boundary_count: state.counts.boundaries,
    settlement_count: state.counts.settlements,
    scored_settlement_count: scored,
    candidate_match_count: matches,
    candidate_match_rate: scored ? matches / scored : null,
    markets_with_any_book: [...state.metadata.values()].filter((meta) => meta.token_ids.some((id) => state.books.has(id))).length,
    websocket_states: state.wsStates,
    websocket_reconnects: state.wsReconnects,
    gap_event_count: state.gapEvents.length,
    gap_events: state.gapEvents,
    caveats: [
      "Candidate side is a diagnostic comparison of observed TWAP ticks, not an exchange or settlement authority.",
      "Public CLOB books are top-of-book/depth observations and do not reveal private queue, fill, or maker truth.",
      "This bounded run grants no alpha, PnL, capacity, or live-readiness claim.",
    ],
  };
}

function buildManifest(state, started, endedAtMs, reason) {
  const outputNames = ["STARTED.json", "CHECKPOINT.json", "market_metadata.jsonl", "twap_ticks.jsonl", "book_events.jsonl", "boundary_observations.jsonl", "settlement_observations.jsonl", "summary.json", "run.log"];
  const files = {};
  for (const name of outputNames) {
    const filePath = path.join(state.outDir, name);
    if (fs.existsSync(filePath)) files[name] = { sha256: sha256File(filePath), lines: fileLineCount(filePath), bytes: fs.statSync(filePath).size };
  }
  return {
    manifest_version: 1,
    generated_at: nowIso(endedAtMs),
    started_at: started.started_at,
    ended_at: nowIso(endedAtMs),
    reason,
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    network_authority: "specified_ec2",
    source_commit: state.args.sourceCommit,
    source_urls: { gamma: GAMMA_URL, rtds: RTDS_URL, clob_market: CLOB_URL },
    args: state.args,
    counts: state.counts,
    gap_events: state.gapEvents,
    files,
  };
}

async function finish(state, started, reason) {
  if (state.finished) return;
  state.finished = true;
  state.stopping = true;
  for (const interval of state.intervals) clearInterval(interval);
  for (const socket of state.sockets) {
    try { socket.close(); } catch { /* best effort close */ }
  }
  const endedAtMs = Date.now();
  log(state, "collector_exit", { reason, counts: state.counts });
  const summary = buildSummary(state, endedAtMs, reason);
  writeJson(state.outDir, "summary.json", summary);
  writeJson(state.outDir, "CHECKPOINT.json", { ...summary, checkpoint: true });
  const manifest = buildManifest(state, started, endedAtMs, reason);
  writeJson(state.outDir, "manifest.json", manifest);
  writeJson(state.outDir, "EXIT.json", {
    exited_at: nowIso(endedAtMs),
    terminal: true,
    reason,
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    open_runs: [],
    manifest: path.join(state.outDir, "manifest.json"),
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = path.resolve(args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const state = createState(args, outDir);
  const started = {
    started_at: nowIso(state.startedAtMs),
    started_at_ms: state.startedAtMs,
    command: process.argv.join(" "),
    host: process.env.HOSTNAME || null,
    mode: "no-submit",
    live_orders_submitted: 0,
    credentials_loaded: false,
    network_authority: "specified_ec2",
    forbidden_actions: ["submit_order", "cancel_order", "sign", "redeem", "merge", "funding", "credential_import", "service_mutation"],
    source_commit: args.sourceCommit,
    source_urls: { gamma: GAMMA_URL, rtds: RTDS_URL, clob_market: CLOB_URL },
    args,
  };
  writeJson(outDir, "STARTED.json", started);
  log(state, "collector_start", { out_dir: outDir, assets: args.assets, windows: args.windows, duration_seconds: args.durationSeconds });
  connectRtds(state);
  connectClob(state);
  try { if (await pollGamma(state)) sendClobSubscription(state); } catch (error) { log(state, "initial_gamma_error", { error: String(error.message || error) }); }
  state.intervals.push(setInterval(async () => {
    try { if (await pollGamma(state)) sendClobSubscription(state); } catch (error) { state.gapEvents.push({ ts: nowIso(), source: "gamma_loop", error: String(error.message || error) }); }
    recordBoundariesAndSettlements(state);
    writeCheckpoint(state);
  }, args.pollSeconds * 1000));
  state.intervals.push(setInterval(() => recordBoundariesAndSettlements(state), 1000));
  state.intervals.push(setInterval(() => writeCheckpoint(state), 10000));
  const timer = setTimeout(() => finish(state, started, "duration_elapsed"), args.durationSeconds * 1000);
  const stop = () => { clearTimeout(timer); finish(state, started, "signal").then(() => process.exit(0)); };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

main().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
