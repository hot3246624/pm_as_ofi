#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import process from "node:process";
import { pathToFileURL } from "node:url";

const RTDS_URL = "wss://ws-live-data.polymarket.com";
const CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_URL = "https://gamma-api.polymarket.com/events";
const ROUND_SECONDS = 300;
const DEFAULT_ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"];
const DEFAULT_WINDOWS = [30, 60];

function parseArgs(argv) {
  const args = {
    durationSeconds: 90,
    pollSeconds: 10,
    assets: DEFAULT_ASSETS,
    windows: DEFAULT_WINDOWS,
    bookEmitMinIntervalMs: 0,
    rtdsStaleMs: 10_000,
    boundaryTickMaxDistanceMs: 5_000,
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
    else if (arg === "--book-emit-min-interval-ms") args.bookEmitMinIntervalMs = Number(next());
    else if (arg === "--rtds-stale-ms") args.rtdsStaleMs = Number(next());
    else if (arg === "--boundary-tick-max-distance-ms") args.boundaryTickMaxDistanceMs = Number(next());
    else if (arg === "--out-dir") args.outDir = next();
    else if (arg === "--source-commit") args.sourceCommit = next();
    else if (arg === "--no-submit") continue;
    else if (arg === "--help") {
      console.log("Usage: collect_twap_boundary_shadow.mjs --out-dir DIR [--duration-seconds N] [--poll-seconds N] [--assets BTC,ETH] [--windows 30,60] [--book-emit-min-interval-ms N] [--rtds-stale-ms N] [--boundary-tick-max-distance-ms N] [--source-commit HASH] --no-submit");
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
  if (!Number.isInteger(args.bookEmitMinIntervalMs) || args.bookEmitMinIntervalMs < 0 || args.bookEmitMinIntervalMs > 60_000) {
    throw new Error("--book-emit-min-interval-ms must be an integer between 0 and 60000");
  }
  if (!Number.isInteger(args.rtdsStaleMs) || args.rtdsStaleMs < 3_000 || args.rtdsStaleMs > 120_000) {
    throw new Error("--rtds-stale-ms must be an integer between 3000 and 120000");
  }
  if (!Number.isInteger(args.boundaryTickMaxDistanceMs) || args.boundaryTickMaxDistanceMs < 0 || args.boundaryTickMaxDistanceMs > 120_000) {
    throw new Error("--boundary-tick-max-distance-ms must be an integer between 0 and 120000");
  }
  if (args.windows.some((windowS) => ![30, 60].includes(windowS))) throw new Error("--windows only supports 30 and 60");
  if (!args.assets.length || !args.windows.length) throw new Error("assets and windows must not be empty");
  return args;
}

function nowIso(ms = Date.now()) {
  return new Date(ms).toISOString();
}

function appendJsonl(state, name, row) {
  let fd = state.jsonlFds.get(name);
  if (fd == null) {
    fd = fs.openSync(path.join(state.outDir, name), "a");
    state.jsonlFds.set(name, fd);
  }
  fs.writeSync(fd, `${JSON.stringify(row)}\n`);
}

function closeJsonlWriters(state) {
  for (const fd of state.jsonlFds.values()) {
    try { fs.fsyncSync(fd); } catch { /* best effort flush */ }
    try { fs.closeSync(fd); } catch { /* best effort close */ }
  }
  state.jsonlFds.clear();
}

function writeJson(outDir, name, value) {
  fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(value, null, 2)}\n`);
}

function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  try {
    let bytesRead = 0;
    do {
      bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (bytesRead > 0) hash.update(buffer.subarray(0, bytesRead));
    } while (bytesRead > 0);
  } finally {
    fs.closeSync(fd);
  }
  return hash.digest("hex");
}

function fileLineCount(filePath) {
  if (!fs.existsSync(filePath)) return 0;
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  let bytesRead = 0;
  let lines = 0;
  let sawAnyByte = false;
  let lastByte = null;
  try {
    do {
      bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (bytesRead > 0) {
        sawAnyByte = true;
        for (let i = 0; i < bytesRead; i += 1) {
          if (buffer[i] === 0x0a) lines += 1;
          lastByte = buffer[i];
        }
      }
    } while (bytesRead > 0);
  } finally {
    fs.closeSync(fd);
  }
  return sawAnyByte && lastByte !== 0x0a ? lines + 1 : lines;
}

function forEachJsonl(filePath, onRow) {
  if (!fs.existsSync(filePath)) return 0;
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  let carry = "";
  let bytesRead = 0;
  let rows = 0;
  const consume = (text, final = false) => {
    const parts = text.split("\n");
    if (!final) carry = parts.pop() || "";
    for (const line of parts) {
      if (!line) continue;
      onRow(JSON.parse(line));
      rows += 1;
    }
  };
  try {
    do {
      bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (bytesRead > 0) consume(carry + buffer.toString("utf8", 0, bytesRead));
    } while (bytesRead > 0);
    if (carry) consume(carry, true);
  } finally {
    fs.closeSync(fd);
  }
  return rows;
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
  // Current Gamma descriptions identify the authoritative stream as, for
  // example, `btc-usd-twap-30s-streams`. Treat that explicit metadata as the
  // window mapping; never infer a window from RTDS update cadence.
  const match = text.match(/\btwap-(30|60)s\b/i)
    || text.match(/\btwap\s*[:\-]\s*(30|60)s\b/i)
    || text.match(/\b(30|60)[- ]second\b/i);
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

function parseDecimal(value) {
  const text = String(value ?? "").trim();
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(text);
  if (!match) return null;
  const sign = match[1] === "-" ? -1 : 1;
  const fraction = match[3] || "";
  const digits = `${match[2]}${fraction}`.replace(/^0+(?=\d)/, "") || "0";
  const magnitude = BigInt(digits);
  return { sign: magnitude === 0n ? 1 : sign, magnitude, scale: fraction.length };
}

function compareDecimalStrings(left, right) {
  const leftParsed = parseDecimal(left);
  const rightParsed = parseDecimal(right);
  if (!leftParsed || !rightParsed) return null;
  if (leftParsed.sign !== rightParsed.sign) return leftParsed.sign < rightParsed.sign ? -1 : 1;
  const scale = Math.max(leftParsed.scale, rightParsed.scale);
  const leftMagnitude = leftParsed.magnitude * (10n ** BigInt(scale - leftParsed.scale));
  const rightMagnitude = rightParsed.magnitude * (10n ** BigInt(scale - rightParsed.scale));
  if (leftMagnitude === rightMagnitude) return 0;
  const magnitudeComparison = leftMagnitude < rightMagnitude ? -1 : 1;
  return leftParsed.sign === 1 ? magnitudeComparison : -magnitudeComparison;
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
    jsonlFds: new Map(),
    wsStates: { rtds: "starting", clob: "starting" },
    wsReconnects: { rtds: 0, clob: 0 },
    rtdsSocket: null,
    rtdsOpenedAtMs: null,
    lastRtdsReceiveMs: null,
    rtdsLastReceiveByStream: new Map(),
    rtdsSilenceDetected: false,
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
    rtds_last_receive_ms: state.lastRtdsReceiveMs,
    rtds_tail_silence_ms: state.lastRtdsReceiveMs == null ? null : Date.now() - state.lastRtdsReceiveMs,
    rtds_silence_detected: state.rtdsSilenceDetected,
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
  const description = String(event?.description || market?.description || "");
  const twapWindowS = inferTwapWindow(description);
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
    twap_window_s: twapWindowS,
    twap_window_source: twapWindowS == null ? null : "gamma_description",
    description,
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
    appendJsonl(state, "market_metadata.jsonl", meta);
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
  socket.send(JSON.stringify({ assets_ids: ids, type: "market", custom_feature_enabled: true }));
  socket.__subFingerprint = fingerprint;
  log(state, "clob_subscribed", { assets: ids.length });
}

function normalizeClobMessage(message) {
  if (message?.topic !== "market" || !message.payload) return message;
  const payload = message.payload;
  const timestamp = payload.timestamp ?? null;
  if (message.type === "book") {
    return {
      event_type: "book",
      market: payload.market || null,
      asset_id: payload.tokenId || payload.assetId || payload.asset_id || null,
      timestamp,
      hash: payload.hash || null,
      bids: payload.bids,
      asks: payload.asks,
    };
  }
  if (message.type === "price_change") {
    const changes = Array.isArray(payload.priceChanges) ? payload.priceChanges : payload.price_changes;
    return {
      event_type: "price_change",
      market: payload.market || null,
      timestamp,
      price_changes: (Array.isArray(changes) ? changes : []).map((change) => ({
        ...change,
        asset_id: change.asset_id || change.assetId || change.tokenId,
        best_bid: change.best_bid ?? change.bestBid ?? null,
        best_ask: change.best_ask ?? change.bestAsk ?? null,
      })),
    };
  }
  if (message.type === "best_bid_ask") {
    return {
      event_type: "best_bid_ask",
      market: payload.market || null,
      asset_id: payload.tokenId || payload.assetId || payload.asset_id || null,
      timestamp,
      best_bid: payload.best_bid ?? payload.bestBid ?? null,
      best_ask: payload.best_ask ?? payload.bestAsk ?? null,
      spread: payload.spread ?? null,
    };
  }
  return message;
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
    const side = String(change.side || "").toUpperCase();
    if (side !== "BUY" && side !== "SELL") continue;
    const map = side === "BUY" ? book.bids : book.asks;
    const key = priceNumber.toFixed(6);
    if (size <= 0) map.delete(key); else map.set(key, size);
    book.market = message.market || book.market;
    book.last_event_ts = message.timestamp || book.last_event_ts;
    book.last_receive_ms = receiveMs;
    writeBookEvent(state, assetId, "price_change", { ...message, price_change: change }, receiveMs, {
      best_bid: change.best_bid,
      best_ask: change.best_ask,
    });
  }
}

function updateBookFromBestBidAsk(state, message, receiveMs) {
  const assetId = String(message.asset_id || "");
  if (!assetId) return;
  let book = state.books.get(assetId);
  if (!book) {
    book = { bids: new Map(), asks: new Map(), market: message.market || null, last_event_ts: null, last_receive_ms: receiveMs };
    state.books.set(assetId, book);
  }
  book.market = message.market || book.market;
  book.last_event_ts = message.timestamp || book.last_event_ts;
  book.last_receive_ms = receiveMs;
  writeBookEvent(state, assetId, "best_bid_ask", message, receiveMs, {
    best_bid: message.best_bid,
    best_ask: message.best_ask,
  });
}

function writeBookEvent(state, assetId, eventKind, message, receiveMs, eventQuote = {}) {
  const book = state.books.get(assetId);
  if (!book) return;
  const lastEmittedMs = state.bookLastEmittedMs.get(assetId) || 0;
  if (receiveMs - lastEmittedMs < state.args.bookEmitMinIntervalMs && eventKind !== "book" && lastEmittedMs > 0) return;
  state.bookLastEmittedMs.set(assetId, receiveMs);
  const summary = bookSummary(book);
  const { bids, asks, ...compactSummary } = summary;
  const eventTsMs = parseTimestampMs(message.timestamp);
  appendJsonl(state, "book_events.jsonl", {
    receive_ts: nowIso(receiveMs),
    receive_ms: receiveMs,
    event_ts: message.timestamp || null,
    event_ts_ms: eventTsMs,
    event_to_receive_ms: eventTsMs == null ? null : receiveMs - eventTsMs,
    event_kind: eventKind,
    market_id: message.market || book.market || null,
    asset_id: assetId,
    slug: state.tokenToMarket.get(assetId) || null,
    event_best_bid: eventQuote.best_bid == null ? null : String(eventQuote.best_bid),
    event_best_ask: eventQuote.best_ask == null ? null : String(eventQuote.best_ask),
    ...compactSummary,
    ...(eventKind === "book" ? { bids, asks } : {}),
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
  const windowS = asNumber(payload.window_s ?? payload.windowSeconds);
  if (!state.args.windows.includes(windowS)) return;
  const receiveMs = Date.now();
  state.lastRtdsReceiveMs = receiveMs;
  state.rtdsLastReceiveByStream.set(`${asset}:${windowS}`, receiveMs);
  const row = {
    receive_ts: nowIso(receiveMs),
    receive_ms: receiveMs,
    publisher_ts: message.timestamp || null,
    observation_ts: payload.timestamp || null,
    topic: message.topic || null,
    symbol,
    asset,
    window_s: windowS,
    value_decimal: payload.value == null ? null : String(payload.value),
    value: asNumber(payload.value),
    full_accuracy_value: payload.full_accuracy_value == null ? null : String(payload.full_accuracy_value),
  };
  state.twapTicks.push(row);
  appendJsonl(state, "twap_ticks.jsonl", row);
  state.counts.twap_ticks += 1;
}

function handleClobMessage(state, raw) {
  let message;
  try { message = JSON.parse(raw); } catch { return; }
  message = normalizeClobMessage(message);
  const receiveMs = Date.now();
  if (message.event_type === "book") updateBookFromSnapshot(state, message, receiveMs);
  else if (message.event_type === "price_change") updateBookFromPriceChange(state, message, receiveMs);
  else if (message.event_type === "best_bid_ask") updateBookFromBestBidAsk(state, message, receiveMs);
}

function latestTick(state, asset, windowS, predicate) {
  const ticks = state.twapTicks.filter((tick) => tick.asset === asset && tick.window_s === windowS && predicate(tick));
  return ticks.length ? ticks[ticks.length - 1] : null;
}

function firstTick(state, asset, windowS, predicate) {
  return state.twapTicks.find((tick) => tick.asset === asset && tick.window_s === windowS && predicate(tick)) || null;
}

function isSignedInteger(value) {
  return /^-?\d+$/.test(String(value || ""));
}

function compareTwapTicks(left, right) {
  const leftExact = left?.full_accuracy_value;
  const rightExact = right?.full_accuracy_value;
  if (isSignedInteger(leftExact) && isSignedInteger(rightExact)) {
    const leftInteger = BigInt(leftExact);
    const rightInteger = BigInt(rightExact);
    return leftInteger < rightInteger ? -1 : leftInteger > rightInteger ? 1 : 0;
  }
  return compareDecimalStrings(left?.value_decimal ?? left?.value, right?.value_decimal ?? right?.value);
}

function tickTiming(tick) {
  if (!tick) return null;
  const observationMs = parseTimestampMs(tick.observation_ts);
  const publisherMs = parseTimestampMs(tick.publisher_ts);
  return {
    observation_ms: observationMs,
    publisher_ms: publisherMs,
    receive_ms: tick.receive_ms ?? null,
    observation_to_receive_ms: observationMs == null ? null : tick.receive_ms - observationMs,
    publisher_to_receive_ms: publisherMs == null ? null : tick.receive_ms - publisherMs,
    publisher_minus_observation_ms: observationMs == null || publisherMs == null ? null : publisherMs - observationMs,
  };
}

function boundaryObservation(state, meta, observedAtMs) {
  const windowS = meta.twap_window_s;
  const startMs = meta.start_ts * 1000;
  const endMs = meta.end_ts * 1000;
  const tokenSummaries = {};
  for (const tokenId of meta.token_ids) {
    const book = state.books.get(tokenId);
    tokenSummaries[tokenId] = book ? { ...bookSummary(book), book_receive_ts: nowIso(book.last_receive_ms), book_age_ms: observedAtMs - book.last_receive_ms } : null;
  }
  const base = {
    observed_at: nowIso(observedAtMs),
    observed_at_ms: observedAtMs,
    round_end_detection_lag_ms: observedAtMs - endMs,
    slug: meta.slug,
    asset: meta.asset,
    start_ts: meta.start_ts,
    end_ts: meta.end_ts,
    twap_window_s: windowS,
    twap_window_source: meta.twap_window_source,
    token_books: tokenSummaries,
    orderable_diagnostic: Object.values(tokenSummaries).some((book) => book?.best_ask != null && book?.best_bid != null),
    public_settlement_source: "gamma_outcome_prices_only",
  };
  if (![30, 60].includes(windowS)) {
    return {
      ...base,
      window_resolution: "missing_or_ambiguous_market_metadata",
      start_tick: null,
      end_tick: null,
      start_tick_selection: "missing_window_mapping",
      end_tick_selection: "missing_window_mapping",
      candidate_start_value: null,
      candidate_end_value: null,
      candidate_start_full_accuracy_value: null,
      candidate_end_full_accuracy_value: null,
      candidate_side: "unknown",
      candidate_label_kind: "diagnostic_only_not_settlement_truth",
    };
  }
  const startBefore = latestTick(state, meta.asset, windowS, (tick) => {
    const observationMs = parseTimestampMs(tick.observation_ts);
    return observationMs != null && observationMs <= startMs;
  });
  const startAfter = firstTick(state, meta.asset, windowS, (tick) => {
    const observationMs = parseTimestampMs(tick.observation_ts);
    return observationMs != null && observationMs >= startMs;
  });
  const endBefore = latestTick(state, meta.asset, windowS, (tick) => {
    const observationMs = parseTimestampMs(tick.observation_ts);
    return observationMs != null && observationMs <= endMs;
  });
  const endAfter = firstTick(state, meta.asset, windowS, (tick) => {
    const observationMs = parseTimestampMs(tick.observation_ts);
    return observationMs != null && observationMs >= endMs;
  });
  const startTick = startBefore || startAfter;
  const endTick = endBefore || endAfter;
  const startValue = startTick?.value_decimal ?? startTick?.value ?? null;
  const endValue = endTick?.value_decimal ?? endTick?.value ?? null;
  const valueComparison = startTick && endTick ? compareTwapTicks(endTick, startTick) : null;
  const startObservationMs = parseTimestampMs(startTick?.observation_ts);
  const endObservationMs = parseTimestampMs(endTick?.observation_ts);
  const startDistanceMs = startObservationMs == null ? null : Math.abs(startObservationMs - startMs);
  const endDistanceMs = endObservationMs == null ? null : Math.abs(endObservationMs - endMs);
  const boundaryCoverageComplete = startDistanceMs != null
    && endDistanceMs != null
    && startDistanceMs <= state.args.boundaryTickMaxDistanceMs
    && endDistanceMs <= state.args.boundaryTickMaxDistanceMs;
  const diagnosticCandidateSide = valueComparison == null ? "unknown" : valueComparison >= 0 ? "Up" : "Down";
  const candidateSide = boundaryCoverageComplete ? diagnosticCandidateSide : "unknown";
  return {
    ...base,
    window_resolution: "explicit_gamma_description_mapping",
    start_tick: startTick,
    end_tick: endTick,
    start_tick_selection: startBefore ? "latest_at_or_before_start" : startAfter ? "first_at_or_after_start" : "missing",
    end_tick_selection: endBefore ? "latest_at_or_before_end" : endAfter ? "first_at_or_after_end" : "missing",
    start_boundary_distance_ms: startDistanceMs,
    end_boundary_distance_ms: endDistanceMs,
    boundary_tick_max_distance_ms: state.args.boundaryTickMaxDistanceMs,
    boundary_coverage_complete: boundaryCoverageComplete,
    candidate_start_value: startValue,
    candidate_end_value: endValue,
    start_tick_timing: tickTiming(startTick),
    end_tick_timing: tickTiming(endTick),
    candidate_start_full_accuracy_value: startTick?.full_accuracy_value ?? null,
    candidate_end_full_accuracy_value: endTick?.full_accuracy_value ?? null,
    candidate_side: candidateSide,
    diagnostic_candidate_side: diagnosticCandidateSide,
    candidate_label_kind: "diagnostic_only_not_settlement_truth",
  };
}

function recordBoundariesAndSettlements(state) {
  const nowMs = Date.now();
  for (const meta of state.metadata.values()) {
    if (meta.end_ts * 1000 <= nowMs && nowMs <= (meta.end_ts + 105) * 1000 && !state.boundaryObserved.has(meta.slug)) {
      const observation = boundaryObservation(state, meta, nowMs);
      state.boundaryObserved.add(meta.slug);
      appendJsonl(state, "boundary_observations.jsonl", observation);
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
        appendJsonl(state, "settlement_observations.jsonl", {
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
      state.rtdsSocket = socket;
      state.rtdsOpenedAtMs = Date.now();
      state.lastRtdsReceiveMs = null;
      state.rtdsLastReceiveByStream.clear();
      state.rtdsSilenceDetected = false;
      const sendPing = () => {
        if (socket.readyState === WebSocket.OPEN) socket.send("PING");
      };
      socket.__pingInterval = setInterval(sendPing, 5000);
      socket.send(JSON.stringify({
        action: "subscribe",
        subscriptions: state.args.windows.map((windowS) => ({ topic: `crypto_prices_twap_${windowS === 30 ? "thirty" : "sixty"}`, type: "update" })),
      }));
      log(state, "rtds_subscribed", { topics: state.args.windows });
    },
    () => {
      state.wsStates.rtds = "closed";
      if (state.rtdsSocket === socket) state.rtdsSocket = null;
      if (socket.__pingInterval) {
        clearInterval(socket.__pingInterval);
        socket.__pingInterval = null;
      }
      if (!state.stopping) {
        state.gapEvents.push({
          ts: nowIso(),
          source: "rtds",
          error: state.rtdsSilenceDetected ? "socket_closed_after_tick_silence" : "socket_closed",
        });
        state.wsReconnects.rtds += 1;
        setTimeout(() => { if (!state.stopping) connectRtds(state); }, 2000);
      }
    },
    (error) => { state.wsStates.rtds = "error"; state.gapEvents.push({ ts: nowIso(), source: "rtds", error: String(error.message || error) }); },
  );
  socket.__kind = "rtds";
  state.sockets.push(socket);
}

function checkRtdsSilence(state) {
  const socket = state.rtdsSocket;
  if (state.stopping || state.rtdsSilenceDetected || !socket || socket.readyState !== WebSocket.OPEN) return;
  const nowMs = Date.now();
  const staleStreams = [];
  for (const asset of state.args.assets) {
    for (const windowS of state.args.windows) {
      const key = `${asset}:${windowS}`;
      const lastReceiveMs = state.rtdsLastReceiveByStream.get(key) ?? state.rtdsOpenedAtMs;
      const silenceMs = lastReceiveMs == null ? null : nowMs - lastReceiveMs;
      if (silenceMs != null && silenceMs > state.args.rtdsStaleMs) {
        staleStreams.push({ asset, window_s: windowS, last_receive_ms: state.rtdsLastReceiveByStream.get(key) ?? null, silence_ms: silenceMs });
      }
    }
  }
  if (!staleStreams.length) return;
  state.rtdsSilenceDetected = true;
  const gap = {
    ts: nowIso(nowMs),
    source: "rtds",
    error: "tick_silence",
    stale_threshold_ms: state.args.rtdsStaleMs,
    stale_streams: staleStreams,
  };
  state.gapEvents.push(gap);
  log(state, "rtds_tick_silence", gap);
  try { socket.close(); } catch { /* close handler owns reconnect */ }
}

function connectClob(state) {
  const socket = safeWebSocket(
    CLOB_URL,
    (raw) => handleClobMessage(state, raw),
    () => {
      state.wsStates.clob = "open";
      const sendPing = () => {
        if (socket.readyState === WebSocket.OPEN) socket.send("PING");
      };
      socket.__pingInterval = setInterval(sendPing, 10_000);
      sendClobSubscription(state, true);
    },
    () => {
      state.wsStates.clob = "closed";
      if (socket.__pingInterval) {
        clearInterval(socket.__pingInterval);
        socket.__pingInterval = null;
      }
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

function readJsonl(filePath) {
  const rows = [];
  try { forEachJsonl(filePath, (row) => rows.push(row)); } catch { /* ignore a partial terminal line */ }
  return rows;
}

function distribution(values) {
  const finite = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!finite.length) return { count: 0, min: null, p50: null, p90: null, p95: null, p99: null, max: null };
  const quantile = (q) => finite[Math.min(finite.length - 1, Math.floor((finite.length - 1) * q))];
  return {
    count: finite.length,
    min: finite[0],
    p50: quantile(0.5),
    p90: quantile(0.9),
    p95: quantile(0.95),
    p99: quantile(0.99),
    max: finite[finite.length - 1],
  };
}

function buildRtdsTimingSummary(state, endedAtMs = Date.now()) {
  const staleThresholdMs = state.args.rtdsStaleMs ?? 10_000;
  const observationToReceive = [];
  const publisherToReceive = [];
  const publisherMinusObservation = [];
  let missingObservation = 0;
  let missingPublisher = 0;
  const streams = new Map();
  for (const tick of state.twapTicks) {
    const observationMs = parseTimestampMs(tick.observation_ts);
    const publisherMs = parseTimestampMs(tick.publisher_ts);
    if (observationMs == null) missingObservation += 1;
    else observationToReceive.push(tick.receive_ms - observationMs);
    if (publisherMs == null) missingPublisher += 1;
    else publisherToReceive.push(tick.receive_ms - publisherMs);
    if (observationMs != null && publisherMs != null) publisherMinusObservation.push(publisherMs - observationMs);
    const key = `${tick.asset}:${tick.window_s}`;
    let stream = streams.get(key);
    if (!stream) {
      stream = { asset: tick.asset, window_s: tick.window_s, receive_ms: [], observation_ms: [] };
      streams.set(key, stream);
    }
    if (Number.isFinite(tick.receive_ms)) stream.receive_ms.push(tick.receive_ms);
    if (observationMs != null) stream.observation_ms.push(observationMs);
  }
  const streamRows = [...streams.values()].map((stream) => {
    stream.receive_ms.sort((a, b) => a - b);
    stream.observation_ms.sort((a, b) => a - b);
    const receiveGaps = stream.receive_ms.slice(1).map((value, index) => value - stream.receive_ms[index]);
    const lastReceiveMs = stream.receive_ms.at(-1) ?? null;
    return {
      asset: stream.asset,
      window_s: stream.window_s,
      tick_rows: stream.receive_ms.length,
      first_receive_ms: stream.receive_ms[0] ?? null,
      last_receive_ms: lastReceiveMs,
      last_observation_ms: stream.observation_ms.at(-1) ?? null,
      max_interarrival_ms: receiveGaps.length ? Math.max(...receiveGaps) : null,
      tail_silence_ms: lastReceiveMs == null ? null : endedAtMs - lastReceiveMs,
    };
  }).sort((a, b) => `${a.asset}:${a.window_s}`.localeCompare(`${b.asset}:${b.window_s}`));
  const firstReceiveMs = state.twapTicks.reduce((earliest, tick) => Number.isFinite(tick.receive_ms) ? Math.min(earliest, tick.receive_ms) : earliest, Infinity);
  const lastReceiveMs = state.twapTicks.reduce((latest, tick) => Number.isFinite(tick.receive_ms) ? Math.max(latest, tick.receive_ms) : latest, -Infinity);
  const tailSilenceMs = Number.isFinite(lastReceiveMs) ? endedAtMs - lastReceiveMs : null;
  return {
    tick_rows: state.twapTicks.length,
    missing_observation_timestamp_count: missingObservation,
    missing_publisher_timestamp_count: missingPublisher,
    observation_to_receive_ms: distribution(observationToReceive),
    publisher_to_receive_ms: distribution(publisherToReceive),
    publisher_minus_observation_ms: distribution(publisherMinusObservation),
    first_receive_ms: Number.isFinite(firstReceiveMs) ? firstReceiveMs : null,
    last_receive_ms: Number.isFinite(lastReceiveMs) ? lastReceiveMs : null,
    tail_silence_ms: tailSilenceMs,
    stale_threshold_ms: staleThresholdMs,
    stale_at_exit: tailSilenceMs == null || tailSilenceMs > staleThresholdMs,
    streams: streamRows,
  };
}

function eventQuote(row) {
  return {
    best_bid: row.event_best_bid ?? (row.best_bid == null ? null : String(row.best_bid)),
    best_ask: row.event_best_ask ?? (row.best_ask == null ? null : String(row.best_ask)),
  };
}

function quoteKey(quote) {
  return `${quote.best_bid ?? ""}|${quote.best_ask ?? ""}`;
}

function buildClobTimingSummary(state, boundaryRows) {
  const eventPath = path.join(state.outDir, "book_events.jsonl");
  const eventToReceive = [];
  let missingEventTimestamp = 0;
  let negativeEventLatency = 0;
  let eventRows = 0;
  const boundaryByToken = new Map();
  for (const boundary of boundaryRows) {
    const endMs = Number(boundary.end_ts) * 1000;
    for (const tokenId of Object.keys(boundary.token_books || {})) {
      boundaryByToken.set(tokenId, { endMs, before: null, firstPost: null, reprice: null });
    }
  }
  forEachJsonl(eventPath, (row) => {
    eventRows += 1;
    const eventTsMs = parseTimestampMs(row.event_ts_ms ?? row.event_ts);
    if (eventTsMs == null) missingEventTimestamp += 1;
    else {
      const latency = row.receive_ms - eventTsMs;
      if (latency < 0) negativeEventLatency += 1;
      else eventToReceive.push(latency);
    }
    const boundary = boundaryByToken.get(String(row.asset_id));
    if (!boundary || !Number.isFinite(row.receive_ms)) return;
    if (row.receive_ms <= boundary.endMs) {
      boundary.before = row;
      return;
    }
    if (!boundary.firstPost) boundary.firstPost = row;
    if (!boundary.reprice && boundary.before) {
      const baselineKey = quoteKey(eventQuote(boundary.before));
      const quote = eventQuote(row);
      if (quoteKey(quote) !== baselineKey && (quote.best_bid != null || quote.best_ask != null)) boundary.reprice = row;
    }
  });
  const repriceLags = [];
  const postQuoteLags = [];
  let baselineMissing = 0;
  let postEventMissing = 0;
  for (const boundary of boundaryByToken.values()) {
    if (!boundary.before) baselineMissing += 1;
    if (!boundary.firstPost) postEventMissing += 1;
    else postQuoteLags.push(boundary.firstPost.receive_ms - boundary.endMs);
    if (boundary.reprice) repriceLags.push(boundary.reprice.receive_ms - boundary.endMs);
  }
  return {
    event_rows: eventRows,
    event_to_receive_ms: distribution(eventToReceive),
    negative_event_latency_count: negativeEventLatency,
    missing_event_timestamp_count: missingEventTimestamp,
    boundary_token_pairs: boundaryRows.reduce((sum, row) => sum + Object.keys(row.token_books || {}).length, 0),
    baseline_missing_count: baselineMissing,
    post_event_missing_count: postEventMissing,
    first_post_quote_receive_lag_ms: distribution(postQuoteLags),
    first_quote_reprice_receive_lag_ms: distribution(repriceLags),
  };
}

function buildSummary(state, endedAtMs, reason) {
  const settlements = readJsonl(path.join(state.outDir, "settlement_observations.jsonl"));
  const boundaryRows = readJsonl(path.join(state.outDir, "boundary_observations.jsonl"));
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
    book_emit_min_interval_ms: state.args.bookEmitMinIntervalMs,
    boundary_observation_poll_interval_ms: 1000,
    counts: state.counts,
    market_count: state.metadata.size,
    boundary_count: state.counts.boundaries,
    settlement_count: state.counts.settlements,
    scored_settlement_count: scored,
    candidate_match_count: matches,
    candidate_match_rate: scored ? matches / scored : null,
    rtds_timing: buildRtdsTimingSummary(state, endedAtMs),
    clob_timing: buildClobTimingSummary(state, boundaryRows),
    markets_with_any_book: [...state.metadata.values()].filter((meta) => meta.token_ids.some((id) => state.books.has(id))).length,
    websocket_states: state.wsStates,
    websocket_reconnects: state.wsReconnects,
    gap_event_count: state.gapEvents.length,
    gap_events: state.gapEvents,
    caveats: [
      "Candidate side is a diagnostic comparison of observed TWAP ticks, not an exchange or settlement authority.",
      "Public CLOB books are top-of-book/depth observations and do not reveal private queue, fill, or maker truth.",
      "round_end_detection_lag_ms is a 1-second polling diagnostic; first_quote_reprice_receive_lag_ms mixes both tokens and is not a candidate-side tradable-window metric.",
      "This bounded run grants no alpha, PnL, capacity, or live-readiness claim.",
    ],
  };
}

function buildManifest(state, started, endedAtMs, reason, codePath = process.argv[1]) {
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
    code_sha256: sha256File(path.resolve(codePath)),
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
    if (socket.__pingInterval) clearInterval(socket.__pingInterval);
    try { socket.close(); } catch { /* best effort close */ }
  }
  const endedAtMs = Date.now();
  log(state, "collector_exit", { reason, counts: state.counts });
  closeJsonlWriters(state);
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
    code_sha256: sha256File(path.resolve(process.argv[1])),
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
  state.intervals.push(setInterval(() => checkRtdsSilence(state), 1000));
  state.intervals.push(setInterval(() => writeCheckpoint(state), 10000));
  const timer = setTimeout(() => finish(state, started, "duration_elapsed"), args.durationSeconds * 1000);
  const stop = () => { clearTimeout(timer); finish(state, started, "signal").then(() => process.exit(0)); };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

const invokedScriptUrl = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (invokedScriptUrl === import.meta.url) {
  main().catch((error) => {
    console.error(error.stack || error);
    process.exitCode = 1;
  });
}

export {
  buildManifest,
  buildSummary,
  fileLineCount,
  forEachJsonl,
  inferTwapWindow,
  readJsonl,
  sha256File,
};
