#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import process from "node:process";

const ASSET_SYMBOLS = {
  BTC: { binance: "btcusdt", okx: "BTC-USDT", bybit: "BTCUSDT", coinbase: "BTC-USD" },
  ETH: { binance: "ethusdt", okx: "ETH-USDT", bybit: "ETHUSDT", coinbase: "ETH-USD" },
  SOL: { binance: "solusdt", okx: "SOL-USDT", bybit: "SOLUSDT", coinbase: "SOL-USD" },
  XRP: { binance: "xrpusdt", okx: "XRP-USDT", bybit: "XRPUSDT", coinbase: "XRP-USD" },
  DOGE: { binance: "dogeusdt", okx: "DOGE-USDT", bybit: "DOGEUSDT", coinbase: "DOGE-USD" },
  BNB: { binance: "bnbusdt", okx: "BNB-USDT", bybit: "BNBUSDT", coinbase: "BNB-USD" },
  HYPE: { binance: "hypeusdt", okx: "HYPE-USDT", bybit: "HYPEUSDT", coinbase: "HYPE-USD" },
};

const ENDPOINTS = {
  binance: "wss://stream.binance.com:9443/stream",
  okx: "wss://ws.okx.com:8443/ws/v5/public",
  bybit: "wss://stream.bybit.com/v5/public/spot",
  coinbase: "wss://advanced-trade-ws.coinbase.com",
};

function parseArgs(argv) {
  const args = {
    outDir: null,
    durationSeconds: 900,
    assets: ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"],
    sources: ["binance", "okx", "bybit", "coinbase"],
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = () => {
      if (index + 1 >= argv.length) throw new Error(`missing value for ${arg}`);
      index += 1;
      return argv[index];
    };
    if (arg === "--out-dir") args.outDir = next();
    else if (arg === "--duration-seconds") args.durationSeconds = Number(next());
    else if (arg === "--assets") args.assets = next().split(",").map((value) => value.trim().toUpperCase()).filter(Boolean);
    else if (arg === "--sources") args.sources = next().split(",").map((value) => value.trim().toLowerCase()).filter(Boolean);
    else if (arg === "--no-submit") continue;
    else if (arg === "--help") {
      console.log("Usage: collect_external_twap_source_tape.mjs --out-dir DIR [--duration-seconds N] [--assets BTC,ETH,...] [--sources binance,okx,bybit,coinbase] --no-submit");
      process.exit(0);
    } else throw new Error(`unknown argument: ${arg}`);
  }
  if (!args.outDir) throw new Error("--out-dir is required");
  if (!Number.isInteger(args.durationSeconds) || args.durationSeconds < 60 || args.durationSeconds > 86_400) {
    throw new Error("--duration-seconds must be an integer between 60 and 86400");
  }
  if (!args.assets.length || args.assets.some((asset) => !ASSET_SYMBOLS[asset])) throw new Error("assets must be supported symbols");
  if (!args.sources.length || args.sources.some((source) => !ENDPOINTS[source])) throw new Error("sources must be supported public venues");
  return args;
}

function nowIso(ms = Date.now()) {
  return new Date(ms).toISOString();
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

function appendJsonl(state, row) {
  fs.writeSync(state.tapeFd, `${JSON.stringify(row)}\n`);
  state.counts.ticks += 1;
  const key = `${row.source}:${row.symbol}`;
  const current = state.coverage.get(key) || { source: row.source, symbol: row.symbol, rows: 0, first_event_ts_ms: null, last_event_ts_ms: null, first_receive_ms: null, last_receive_ms: null };
  current.rows += 1;
  current.first_event_ts_ms = current.first_event_ts_ms == null ? row.event_ts_ms : Math.min(current.first_event_ts_ms, row.event_ts_ms);
  current.last_event_ts_ms = current.last_event_ts_ms == null ? row.event_ts_ms : Math.max(current.last_event_ts_ms, row.event_ts_ms);
  current.first_receive_ms = current.first_receive_ms == null ? row.receive_ms : Math.min(current.first_receive_ms, row.receive_ms);
  current.last_receive_ms = current.last_receive_ms == null ? row.receive_ms : Math.max(current.last_receive_ms, row.receive_ms);
  state.coverage.set(key, current);
}

function log(state, message, extra = {}) {
  const line = { ts: nowIso(), message, ...extra };
  fs.appendFileSync(path.join(state.outDir, "run.log"), `${JSON.stringify(line)}\n`);
  console.log(`[${line.ts}] ${message}`);
}

function normalizePrice(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function emitTick(state, source, asset, eventTsMs, fields) {
  const price = normalizePrice(fields.price ?? fields.mid ?? fields.last ?? fields.mark);
  if (price == null || !Number.isFinite(eventTsMs)) return;
  const receiveMs = Date.now();
  appendJsonl(state, {
    receive_ts: nowIso(receiveMs),
    receive_ms: receiveMs,
    event_ts_ms: Number(eventTsMs),
    symbol: `${asset.toLowerCase()}/usd`,
    source: `${source}_spot`,
    source_event_type: fields.eventType || "ticker",
    price,
    bid: normalizePrice(fields.bid),
    ask: normalizePrice(fields.ask),
    mid: normalizePrice(fields.mid),
    mark: normalizePrice(fields.mark),
    last: normalizePrice(fields.last),
  });
}

function safeWebSocket(url, onMessage, onOpen, onClose, onError) {
  const socket = new WebSocket(url);
  socket.addEventListener("open", onOpen);
  socket.addEventListener("message", (event) => {
    const data = event.data;
    if (typeof data === "string") onMessage(data);
    else if (data instanceof ArrayBuffer) onMessage(Buffer.from(data).toString("utf8"));
    else if (ArrayBuffer.isView(data)) onMessage(Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString("utf8"));
    else if (data && typeof data.text === "function") data.text().then(onMessage).catch(onError);
    else onMessage(String(data));
  });
  socket.addEventListener("close", onClose);
  socket.addEventListener("error", onError);
  return socket;
}

function connectBinance(state) {
  const streams = state.args.assets.map((asset) => `${ASSET_SYMBOLS[asset].binance}@trade`);
  const url = `${ENDPOINTS.binance}?streams=${streams.join("/")}`;
  const socket = safeWebSocket(url, (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { state.counts.parse_errors += 1; return; }
    if (message?.data?.e === "trade") {
      const symbol = String(message.data.s || "").toLowerCase();
      const asset = state.args.assets.find((candidate) => ASSET_SYMBOLS[candidate].binance === symbol);
      if (asset) emitTick(state, "binance", asset, Number(message.data.E), { price: message.data.p, last: message.data.p, eventType: "trade" });
    }
  }, () => { state.sourceStates.binance = "open"; log(state, "source_open", { source: "binance_spot", assets: state.args.assets }); }, () => reconnect(state, "binance"), (error) => sourceError(state, "binance", error));
  socket.__source = "binance";
  state.sockets.push(socket);
}

function connectOkx(state) {
  const args = state.args.assets.map((asset) => ({ channel: "tickers", instId: ASSET_SYMBOLS[asset].okx }));
  const socket = safeWebSocket(ENDPOINTS.okx, (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { state.counts.parse_errors += 1; return; }
    if (message?.event === "pong") return;
    const item = message?.arg?.instId ? message?.data?.[0] : null;
    if (!item) return;
    const asset = state.args.assets.find((candidate) => ASSET_SYMBOLS[candidate].okx === message.arg.instId);
    if (asset) emitTick(state, "okx", asset, Number(item.ts), { last: item.last, bid: item.bidPx, ask: item.askPx, mid: normalizePrice(item.bidPx) != null && normalizePrice(item.askPx) != null ? (Number(item.bidPx) + Number(item.askPx)) / 2 : null, eventType: "tickers" });
  }, () => {
    state.sourceStates.okx = "open";
    socket.send(JSON.stringify({ op: "subscribe", args }));
    log(state, "source_open", { source: "okx_spot", assets: state.args.assets });
  }, () => reconnect(state, "okx"), (error) => sourceError(state, "okx", error));
  socket.__source = "okx";
  state.sockets.push(socket);
}

function connectBybit(state) {
  const args = state.args.assets.map((asset) => `tickers.${ASSET_SYMBOLS[asset].bybit}`);
  const socket = safeWebSocket(ENDPOINTS.bybit, (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { state.counts.parse_errors += 1; return; }
    if (message?.op === "pong") return;
    const topic = String(message?.topic || "");
    const symbol = topic.startsWith("tickers.") ? topic.slice("tickers.".length) : null;
    const item = message?.data;
    const asset = state.args.assets.find((candidate) => ASSET_SYMBOLS[candidate].bybit === symbol);
    if (asset && item && !Array.isArray(item)) emitTick(state, "bybit", asset, Number(message.ts || item.ts), { last: item.lastPrice, bid: item.bid1Price, ask: item.ask1Price, mid: normalizePrice(item.bid1Price) != null && normalizePrice(item.ask1Price) != null ? (Number(item.bid1Price) + Number(item.ask1Price)) / 2 : null, mark: item.markPrice, eventType: "tickers" });
  }, () => {
    state.sourceStates.bybit = "open";
    socket.send(JSON.stringify({ op: "subscribe", args }));
    log(state, "source_open", { source: "bybit_spot", assets: state.args.assets });
  }, () => reconnect(state, "bybit"), (error) => sourceError(state, "bybit", error));
  socket.__source = "bybit";
  state.sockets.push(socket);
}

function connectCoinbase(state) {
  const productIds = state.args.assets.map((asset) => ASSET_SYMBOLS[asset].coinbase);
  const socket = safeWebSocket(ENDPOINTS.coinbase, (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { state.counts.parse_errors += 1; return; }
    for (const event of message?.events || []) {
      for (const ticker of event?.tickers || []) {
        const asset = state.args.assets.find((candidate) => ASSET_SYMBOLS[candidate].coinbase === ticker.product_id);
        if (!asset) continue;
        const eventTs = Date.parse(String(ticker.time || message.timestamp || ""));
        emitTick(state, "coinbase", asset, Number.isFinite(eventTs) ? eventTs : Date.now(), { last: ticker.price, bid: ticker.best_bid, ask: ticker.best_ask, eventType: "ticker" });
      }
    }
  }, () => {
    state.sourceStates.coinbase = "open";
    socket.send(JSON.stringify({ type: "subscribe", product_ids: productIds, channel: "ticker" }));
    log(state, "source_open", { source: "coinbase_spot", assets: state.args.assets });
  }, () => reconnect(state, "coinbase"), (error) => sourceError(state, "coinbase", error));
  socket.__source = "coinbase";
  state.sockets.push(socket);
}

function sourceError(state, source, error) {
  state.counts.errors += 1;
  state.gapEvents.push({ ts: nowIso(), source: `${source}_spot`, error: String(error?.message || error) });
}

function reconnect(state, source) {
  state.sourceStates[source] = "closed";
  if (state.stopping) return;
  state.reconnects[source] = (state.reconnects[source] || 0) + 1;
  state.gapEvents.push({ ts: nowIso(), source: `${source}_spot`, error: "socket_closed" });
  setTimeout(() => { if (!state.stopping) connectSource(state, source); }, 2000);
}

function connectSource(state, source) {
  state.sourceStates[source] = "connecting";
  if (source === "binance") connectBinance(state);
  else if (source === "okx") connectOkx(state);
  else if (source === "bybit") connectBybit(state);
  else if (source === "coinbase") connectCoinbase(state);
}

function closeWriters(state) {
  try { fs.fsyncSync(state.tapeFd); } catch { /* best effort */ }
  try { fs.closeSync(state.tapeFd); } catch { /* best effort */ }
}

function finish(state, started, reason) {
  if (state.finished) return;
  state.finished = true;
  state.stopping = true;
  for (const interval of state.intervals) clearInterval(interval);
  for (const socket of state.sockets) {
    if (socket.__pingInterval) clearInterval(socket.__pingInterval);
    try { socket.close(); } catch { /* best effort */ }
  }
  const endedAtMs = Date.now();
  closeWriters(state);
  const coverage = [...state.coverage.values()].sort((left, right) => `${left.source}:${left.symbol}`.localeCompare(`${right.source}:${right.symbol}`));
  const summary = {
    generated_at: nowIso(endedAtMs),
    reason,
    duration_seconds: (endedAtMs - state.startedAtMs) / 1000,
    mode: "no-submit",
    credentials_loaded: false,
    live_orders_submitted: 0,
    counts: state.counts,
    source_states: state.sourceStates,
    reconnects: state.reconnects,
    gap_event_count: state.gapEvents.length,
    gap_events: state.gapEvents,
    coverage,
    caveats: [
      "Public exchange source ticks are a research proxy and not Chainlink settlement truth.",
      "Event timestamps and local receive timestamps are retained separately.",
      "No source-specific weighting or Chainlink custom-feed reproduction is claimed.",
    ],
  };
  fs.writeFileSync(path.join(state.outDir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
  fs.writeFileSync(path.join(state.outDir, "CHECKPOINT.json"), `${JSON.stringify({ ...summary, checkpoint: true }, null, 2)}\n`);
  const files = {};
  for (const name of ["source_ticks.jsonl", "run.log", "STARTED.json", "CHECKPOINT.json", "summary.json"]) {
    const filePath = path.join(state.outDir, name);
    if (fs.existsSync(filePath)) files[name] = { sha256: sha256File(filePath), bytes: fs.statSync(filePath).size };
  }
  const manifest = {
    schema_version: 1,
    generated_at: nowIso(endedAtMs),
    started_at: started.started_at,
    ended_at: nowIso(endedAtMs),
    reason,
    mode: "no-submit",
    credentials_loaded: false,
    live_orders_submitted: 0,
    network_authority: "specified_ec2",
    code_sha256: started.code_sha256,
    args: state.args,
    files,
  };
  fs.writeFileSync(path.join(state.outDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  fs.writeFileSync(path.join(state.outDir, "EXIT.json"), `${JSON.stringify({ exited_at: nowIso(endedAtMs), terminal: true, reason, mode: "no-submit", credentials_loaded: false, live_orders_submitted: 0, open_runs: [], manifest: path.join(state.outDir, "manifest.json") }, null, 2)}\n`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = path.resolve(args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const state = {
    args,
    outDir,
    startedAtMs: Date.now(),
    stopping: false,
    finished: false,
    tapeFd: fs.openSync(path.join(outDir, "source_ticks.jsonl"), "a"),
    sockets: [],
    intervals: [],
    coverage: new Map(),
    gapEvents: [],
    reconnects: Object.fromEntries(args.sources.map((source) => [source, 0])),
    sourceStates: Object.fromEntries(args.sources.map((source) => [source, "starting"])),
    counts: { ticks: 0, errors: 0, parse_errors: 0 },
  };
  const started = {
    started_at: nowIso(state.startedAtMs),
    started_at_ms: state.startedAtMs,
    command: process.argv.join(" "),
    host: process.env.HOSTNAME || null,
    mode: "no-submit",
    credentials_loaded: false,
    live_orders_submitted: 0,
    network_authority: "specified_ec2",
    forbidden_actions: ["submit_order", "cancel_order", "sign", "redeem", "merge", "funding", "credential_import", "service_mutation"],
    code_sha256: sha256File(path.resolve(process.argv[1])),
    args,
  };
  fs.writeFileSync(path.join(outDir, "STARTED.json"), `${JSON.stringify(started, null, 2)}\n`);
  log(state, "collector_start", { sources: args.sources, assets: args.assets, duration_seconds: args.durationSeconds });
  for (const source of args.sources) connectSource(state, source);
  state.intervals.push(setInterval(() => {
    for (const socket of state.sockets) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      if (socket.__source === "okx") socket.send("ping");
      if (socket.__source === "bybit") socket.send(JSON.stringify({ op: "ping" }));
    }
  }, 20_000));
  state.intervals.push(setInterval(() => {
    fs.writeFileSync(path.join(outDir, "CHECKPOINT.json"), `${JSON.stringify({ updated_at: nowIso(), mode: "no-submit", credentials_loaded: false, live_orders_submitted: 0, counts: state.counts, source_states: state.sourceStates, reconnects: state.reconnects, gap_events: state.gapEvents }, null, 2)}\n`);
  }, 10_000));
  const timer = setTimeout(() => finish(state, started, "duration_elapsed"), args.durationSeconds * 1000);
  const stop = () => { clearTimeout(timer); finish(state, started, "signal"); };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  try { main(); } catch (error) { console.error(error.stack || error); process.exitCode = 1; }
}
