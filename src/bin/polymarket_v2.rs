//! Polymarket V2 — Async Inventory Arbitrage Engine
//!
//! Actor-based architecture:
//!   WebSocket ──fan-out──→ OFI Engine  → (watch) → StrategyCoordinator → Executor → InventoryManager
//!
//! Lifecycle: auto-discover market from prefix → run → wall-clock expiry → CancelAll → rotate.

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

// V2 Actor modules
use mev_backrun_rs_cu::polymarket::coordinator::{CoordinatorConfig, StrategyCoordinator};
use mev_backrun_rs_cu::polymarket::executor::{init_clob_client, Executor, ExecutorConfig};
use mev_backrun_rs_cu::polymarket::inventory::{InventoryConfig, InventoryManager};
use mev_backrun_rs_cu::polymarket::messages::*;
use mev_backrun_rs_cu::polymarket::ofi::{OfiConfig, OfiEngine};
use mev_backrun_rs_cu::polymarket::types::Side;
use mev_backrun_rs_cu::polymarket::user_ws::{UserWsConfig, UserWsListener};

// ─────────────────────────────────────────────────────────
// Settings (reused from V1, simplified)
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Settings {
    market_slug: Option<String>,
    market_id: String,
    yes_asset_id: String,
    no_asset_id: String,
    ws_base_url: String,
    rest_url: String,
    private_key: Option<String>,
    funder_address: Option<String>,
    custom_feature: bool,
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            market_slug: env::var("POLYMARKET_MARKET_SLUG").ok(),
            market_id: env::var("POLYMARKET_MARKET_ID").unwrap_or_default(),
            yes_asset_id: env::var("POLYMARKET_YES_ASSET_ID").unwrap_or_default(),
            no_asset_id: env::var("POLYMARKET_NO_ASSET_ID").unwrap_or_default(),
            ws_base_url: env::var("POLYMARKET_WS_BASE_URL")
                .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws".into()),
            rest_url: env::var("POLYMARKET_REST_URL")
                .unwrap_or_else(|_| "https://clob.polymarket.com".into()),
            private_key: env::var("POLYMARKET_PRIVATE_KEY").ok(),
            funder_address: env::var("POLYMARKET_FUNDER_ADDRESS").ok(),
            custom_feature: env::var("POLYMARKET_CUSTOM_FEATURE")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(true),
        })
    }

    fn ws_url(&self, channel: &str) -> String {
        format!("{}/{}", self.ws_base_url, channel)
    }

    fn market_assets(&self) -> Vec<String> {
        vec![self.yes_asset_id.clone(), self.no_asset_id.clone()]
    }
}

// ─────────────────────────────────────────────────────────
// Market Discovery — prefix → current live slug
// ─────────────────────────────────────────────────────────

/// Check if a slug is a "prefix" (no trailing timestamp) vs a full slug.
/// Prefix examples: "btc-updown-15m", "btc-updown-5m"
/// Full slug: "btc-updown-15m-1771904700"
fn is_prefix_slug(slug: &str) -> bool {
    // If the last segment (after final '-') is NOT a pure number, it's a prefix
    slug.rsplit('-').next()
        .map(|last| last.parse::<u64>().is_err())
        .unwrap_or(true)
}

/// Detect interval from prefix: "...-5m" → 300, "...-15m" → 900.
fn detect_interval(prefix: &str) -> u64 {
    if prefix.contains("-5m") { 300 }
    else if prefix.contains("-15m") { 900 }
    else { 900 } // default 15min
}

/// Compute the slug and end-timestamp for the CURRENTLY ACTIVE market.
///
/// "btc-updown-15m" + now=03:36 UTC → ("btc-updown-15m-1771904700", 1771904700)
fn compute_current_slug(prefix: &str) -> (String, u64) {
    let interval = detect_interval(prefix);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH).unwrap().as_secs();
    let end_ts = ((now / interval) + 1) * interval;
    (format!("{}-{}", prefix, end_ts), end_ts)
}

/// Resolve a market by exact slug via Gamma API.
async fn resolve_market_by_slug(slug: &str) -> anyhow::Result<(String, String, String)> {
    info!("🔍 Resolving market: {}", slug);
    let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
    let resp: Value = reqwest::get(&url).await?.json().await?;

    if let Some(markets) = resp.as_array() {
        if let Some(market) = markets.first() {
            let market_id = market["conditionId"]
                .as_str()
                .or_else(|| market["condition_id"].as_str())
                .unwrap_or_default()
                .to_string();

            let tokens = market.get("clobTokenIds")
                .or_else(|| market.get("clob_token_ids"))
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok());

            if let Some(ids) = tokens {
                if ids.len() >= 2 {
                    info!("✅ Market resolved: {} (YES={}, NO={})",
                        market_id, &ids[0][..8.min(ids[0].len())], &ids[1][..8.min(ids[1].len())]);
                    return Ok((market_id, ids[0].clone(), ids[1].clone()));
                }
            }

            // Fallback: try tokens array
            if let Some(tokens) = market.get("tokens").and_then(|v| v.as_array()) {
                let yes = tokens.iter().find(|t| t["outcome"].as_str() == Some("Yes"));
                let no = tokens.iter().find(|t| t["outcome"].as_str() == Some("No"));
                if let (Some(y), Some(n)) = (yes, no) {
                    let yes_id = y["token_id"].as_str().unwrap_or_default().to_string();
                    let no_id = n["token_id"].as_str().unwrap_or_default().to_string();
                    info!("✅ Market resolved via tokens: {}", market_id);
                    return Ok((market_id, yes_id, no_id));
                }
            }
        }
    }
    anyhow::bail!("Failed to resolve market from slug: {}", slug);
}

// ─────────────────────────────────────────────────────────
// WS Parsing helpers
// ─────────────────────────────────────────────────────────

fn classify_side(asset_id: &str, settings: &Settings) -> Option<Side> {
    if asset_id == settings.yes_asset_id {
        Some(Side::Yes)
    } else if asset_id == settings.no_asset_id {
        Some(Side::No)
    } else {
        None
    }
}

fn parse_price_str(raw: &str) -> Option<f64> {
    raw.parse::<f64>().ok().filter(|v| *v > 0.0 && *v < 100.0)
}

fn parse_price_value(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(parse_price_str))
        .filter(|p| *p > 0.0 && *p < 100.0)
}

/// Parse a WS message into MarketDataMsg events.
fn parse_ws_message(settings: &Settings, value: &Value) -> Vec<MarketDataMsg> {
    let mut msgs = Vec::new();

    match value.get("event_type").and_then(|v| v.as_str()) {
        // ─── Book snapshot ───
        Some("book") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let side = classify_side(asset_id, settings);
                let bids = value
                    .get("bids")
                    .or_else(|| value.get("buys"))
                    .and_then(|v| v.as_array());
                let asks = value
                    .get("asks")
                    .or_else(|| value.get("sells"))
                    .and_then(|v| v.as_array());
                // P2-8: Find true best bid/ask — don't assume array is sorted
                let best_bid = bids
                    .map(|levels| {
                        levels.iter()
                            .filter_map(|lvl| lvl.get("price")
                                .and_then(parse_price_value))
                            .fold(0.0_f64, f64::max)
                    })
                    .unwrap_or(0.0);
                let best_ask = asks
                    .map(|levels| {
                        levels.iter()
                            .filter_map(|lvl| lvl.get("price")
                                .and_then(parse_price_value))
                            .fold(f64::MAX, f64::min)
                    })
                    .map(|v| if v == f64::MAX { 0.0 } else { v })
                    .unwrap_or(0.0);

                if let Some(s) = side {
                    // We'll assemble full BookTick in the caller when we have both sides
                    // For now, emit partial data as a special internal representation
                    msgs.push(MarketDataMsg::BookTick {
                        yes_bid: if s == Side::Yes { best_bid } else { 0.0 },
                        yes_ask: if s == Side::Yes { best_ask } else { 0.0 },
                        no_bid: if s == Side::No { best_bid } else { 0.0 },
                        no_ask: if s == Side::No { best_ask } else { 0.0 },
                        ts: Instant::now(),
                    });
                }
            }
        }
        // ─── Price change ───
        Some("price_change") => {
            if let Some(changes) = value.get("price_changes").and_then(|v| v.as_array()) {
                for ch in changes {
                    if let Some(asset_id) = ch.get("asset_id").and_then(|v| v.as_str()) {
                        let side = classify_side(asset_id, settings);
                        let best_bid = ch
                            .get("best_bid")
                            .and_then(parse_price_value)
                            .unwrap_or(0.0);
                        let best_ask = ch
                            .get("best_ask")
                            .and_then(parse_price_value)
                            .unwrap_or(0.0);

                        if let Some(s) = side {
                            msgs.push(MarketDataMsg::BookTick {
                                yes_bid: if s == Side::Yes { best_bid } else { 0.0 },
                                yes_ask: if s == Side::Yes { best_ask } else { 0.0 },
                                no_bid: if s == Side::No { best_bid } else { 0.0 },
                                no_ask: if s == Side::No { best_ask } else { 0.0 },
                                ts: Instant::now(),
                            });
                        }
                    }
                }
            }
        }
        // ─── Best bid/ask ───
        Some("best_bid_ask") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let side = classify_side(asset_id, settings);
                let best_bid = value
                    .get("best_bid")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);
                let best_ask = value
                    .get("best_ask")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);

                if let Some(s) = side {
                    msgs.push(MarketDataMsg::BookTick {
                        yes_bid: if s == Side::Yes { best_bid } else { 0.0 },
                        yes_ask: if s == Side::Yes { best_ask } else { 0.0 },
                        no_bid: if s == Side::No { best_bid } else { 0.0 },
                        no_ask: if s == Side::No { best_ask } else { 0.0 },
                        ts: Instant::now(),
                    });
                }
            }
        }
        // ─── Last trade price (NEW — OFI data source) ───
        Some("last_trade_price") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let price = value
                    .get("price")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);
                let size = value
                    .get("size")
                    .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok())))
                    .unwrap_or(1.0); // Default to 1 if no size

                // Determine taker side from the "side" field
                let taker_side = match value.get("side").and_then(|v| v.as_str()) {
                    Some("BUY") | Some("buy") | Some("Buy") => TakerSide::Buy,
                    Some("SELL") | Some("sell") | Some("Sell") => TakerSide::Sell,
                    _ => TakerSide::Buy, // Default assumption
                };

                // Classify which market side (YES or NO token)
                let market_side = classify_side(asset_id, settings);

                if price > 0.0 {
                    if let Some(ms) = market_side {
                        msgs.push(MarketDataMsg::TradeTick {
                            asset_id: asset_id.to_string(),
                            market_side: ms,
                            taker_side,
                            price,
                            size,
                            ts: Instant::now(),
                        });
                    }
                }
            }
        }
        _ => {}
    }

    msgs
}

// ─────────────────────────────────────────────────────────
// Book State Assembler (merges partial updates into full BookTick)
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct BookAssembler {
    yes_bid: f64,
    yes_ask: f64,
    no_bid: f64,
    no_ask: f64,
}

impl BookAssembler {
    fn update(&mut self, msg: &MarketDataMsg) -> Option<MarketDataMsg> {
        if let MarketDataMsg::BookTick {
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            ts,
        } = msg
        {
            // Merge: non-zero values update the state
            if *yes_bid > 0.0 {
                self.yes_bid = *yes_bid;
            }
            if *yes_ask > 0.0 {
                self.yes_ask = *yes_ask;
            }
            if *no_bid > 0.0 {
                self.no_bid = *no_bid;
            }
            if *no_ask > 0.0 {
                self.no_ask = *no_ask;
            }

            // Only emit a full BookTick when we have all four prices
            if self.yes_bid > 0.0 && self.yes_ask > 0.0 && self.no_bid > 0.0 && self.no_ask > 0.0
            {
                return Some(MarketDataMsg::BookTick {
                    yes_bid: self.yes_bid,
                    yes_ask: self.yes_ask,
                    no_bid: self.no_bid,
                    no_ask: self.no_ask,
                    ts: *ts,
                });
            }
        }
        None
    }
}

// ─────────────────────────────────────────────────────────
// WebSocket runner (with reconnection + wall-clock deadline)
// ─────────────────────────────────────────────────────────

/// Why the WS session ended.
#[derive(Debug)]
enum MarketEnd {
    /// Wall-clock hit the market's end timestamp.
    Expired,
    /// WS connection error (will reconnect internally unless expired).
    WsError(String),
}

async fn run_market_ws(
    settings: Settings,
    ofi_tx: mpsc::Sender<MarketDataMsg>,
    coord_tx: mpsc::Sender<MarketDataMsg>,
    end_ts: u64,
) -> MarketEnd {
    let mut book_asm = BookAssembler::default();

    // Compute wall-clock deadline
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let secs_remaining = if end_ts > now_unix { end_ts - now_unix } else { 0 };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs_remaining);
    info!("⏰ Market deadline in {}s (end_ts={})", secs_remaining, end_ts);

    loop {
        // Check if already expired before connecting
        if tokio::time::Instant::now() >= deadline {
            info!("🏁 Market expired (wall-clock)");
            return MarketEnd::Expired;
        }

        let url = settings.ws_url("market");
        info!(%url, "📡 connecting market WS");

        let connect_result = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async(&url),
        )
        .await;

        match connect_result {
            Ok(Ok((ws, response))) => {
                info!("✅ WS connected (status={:?})", response.status());
                let (mut write, mut read) = ws.split();

                // Subscribe
                let asset_ids = settings.market_assets();
                let subscribe = json!({
                    "type": "market",
                    "operation": "subscribe",
                    "markets": [],
                    "assets_ids": asset_ids,
                    "initial_dump": true,
                    "custom_feature_enabled": settings.custom_feature,
                });
                info!("📤 Subscribe: {}", subscribe);

                if let Err(err) = write.send(Message::Text(subscribe.to_string())).await {
                    warn!("WS subscribe failed: {err:?}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }

                // Ping keepalive
                tokio::spawn(async move {
                    let mut delay = tokio::time::interval(Duration::from_secs(10));
                    loop {
                        delay.tick().await;
                        if write
                            .send(Message::Text("PING".to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });

                // Read loop with wall-clock deadline
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {
                            info!("🏁 Market expired (wall-clock) — stopping WS");
                            return MarketEnd::Expired;
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                        let values = if value.is_array() {
                                            value.as_array().cloned().unwrap_or_default()
                                        } else {
                                            vec![value]
                                        };

                                        for val in &values {
                                            let parsed = parse_ws_message(&settings, val);
                                            for md_msg in parsed {
                                                match &md_msg {
                                                    MarketDataMsg::TradeTick { .. } => {
                                                        let _ = ofi_tx.send(md_msg.clone()).await;
                                                    }
                                                    MarketDataMsg::BookTick { .. } => {
                                                        if let Some(full) = book_asm.update(&md_msg) {
                                                            let _ = coord_tx.send(full).await;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("WS closed by server");
                                    break;
                                }
                                Some(Err(err)) => {
                                    warn!("WS error: {err:?}");
                                    break;
                                }
                                None => break,
                                _ => {}
                            }
                        }
                    }
                }
            }
            Ok(Err(err)) => {
                warn!("WS connect error: {err:?}");
            }
            Err(_) => {
                warn!("⏱️ WS connection timeout");
            }
        }

        // If expired during reconnect, stop
        if tokio::time::Instant::now() >= deadline {
            info!("🏁 Market expired during reconnect");
            return MarketEnd::Expired;
        }

        info!("🔄 Reconnecting in 2s...");
        sleep(Duration::from_secs(2)).await;
    }
}

// ─────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter("info").init();

    info!("═══════════════════════════════════════════════════");
    info!("  Polymarket V2 — Async Inventory Arbitrage Engine");
    info!("  Auto-Discovery + Market Rotation");
    info!("═══════════════════════════════════════════════════");

    let base_settings = Settings::from_env()?;
    let raw_slug = base_settings.market_slug.clone()
        .unwrap_or_else(|| "btc-updown-15m".to_string());
    let prefix_mode = is_prefix_slug(&raw_slug);

    if prefix_mode {
        info!("🔄 PREFIX mode: '{}' — will auto-rotate markets", raw_slug);
    } else {
        info!("📌 FIXED mode: '{}' — single market", raw_slug);
    }

    let inv_cfg = InventoryConfig::from_env();
    let ofi_cfg = OfiConfig::from_env();
    let coord_cfg = CoordinatorConfig::from_env();
    let dry_run = coord_cfg.dry_run;

    info!("📊 Config: pair={:.2} bid={:.1} tick={:.3} net={:.0} ofi_thresh={:.1} dry={}",
        coord_cfg.pair_target, coord_cfg.bid_size,
        coord_cfg.tick_size, coord_cfg.max_net_diff, ofi_cfg.toxicity_threshold, dry_run);

    // ═══ Initialize CLOB client (once, reused across rotations) ═══
    let (clob_client, signer) = if !dry_run {
        init_clob_client(
            &base_settings.rest_url,
            base_settings.private_key.as_deref(),
        )
        .await
    } else {
        info!("📝 DRY-RUN mode — no orders, no User WS");
        (None, None)
    };

    if !dry_run && (clob_client.is_none() || signer.is_none()) {
        anyhow::bail!(
            "🚨 FATAL: dry_run=false but CLOB client auth failed. \
             Set PM_DRY_RUN=true or fix private key / auth config."
        );
    }

    // ═══ Derive L2 API credentials for User WS (live mode only) ═══
    let api_creds: Option<(String, String, String)> = if !dry_run {
        // Try env vars first, then derive from private key
        let env_key = env::var("POLYMARKET_API_KEY").ok();
        let env_secret = env::var("POLYMARKET_API_SECRET").ok();
        let env_pass = env::var("POLYMARKET_API_PASSPHRASE").ok();

        if let (Some(k), Some(s), Some(p)) = (env_key, env_secret, env_pass) {
            info!("🔑 Using API credentials from environment");
            Some((k, s, p))
        } else if let Some(pk) = base_settings.private_key.as_deref() {
            match mev_backrun_rs_cu::polymarket::user_ws::derive_api_key(
                &base_settings.rest_url, pk,
            ).await {
                Ok(creds) => Some(creds),
                Err(e) => {
                    // P0-1 SAFETY: live mode REQUIRES User WS for inventory tracking.
                    // Without it, real orders go out but net_diff never updates.
                    anyhow::bail!(
                        "🚨 FATAL: dry_run=false but failed to derive API key: {:?}\n\
                         Cannot run live without User WS — inventory would never update.\n\
                         Set PM_DRY_RUN=true or fix credentials.", e
                    );
                }
            }
        } else {
            // P0-1 SAFETY: No private key in live mode = no User WS = blind trading
            anyhow::bail!(
                "🚨 FATAL: dry_run=false but no POLYMARKET_PRIVATE_KEY set.\n\
                 Cannot run live without User WS — inventory would never update.\n\
                 Set PM_DRY_RUN=true or provide credentials."
            );
        }
    } else {
        None
    };

    // ═══════════════════════════════════════════════════
    // OUTER LOOP: Market Rotation
    // ═══════════════════════════════════════════════════

    let mut round = 0u64;
    loop {
        round += 1;

        // ── Step 1: Resolve current market ──
        let (slug, end_ts) = if prefix_mode {
            compute_current_slug(&raw_slug)
        } else {
            (raw_slug.clone(), u64::MAX) // Fixed mode: no expiry
        };

        info!("═══════════════════════════════════════════════════");
        info!("  Round #{} — {}", round, slug);
        info!("═══════════════════════════════════════════════════");

        let resolved = resolve_market_by_slug(&slug).await;
        let (market_id, yes_asset_id, no_asset_id) = match resolved {
            Ok(ids) => ids,
            Err(err) => {
                warn!("❌ Failed to resolve '{}': {} — retrying in 10s", slug, err);
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        let mut settings = base_settings.clone();
        settings.market_id = market_id.clone();
        settings.yes_asset_id = yes_asset_id.clone();
        settings.no_asset_id = no_asset_id.clone();

        info!("🎯 Market: {}", market_id);
        info!("   YES: {}...", &yes_asset_id[..16.min(yes_asset_id.len())]);
        info!("   NO:  {}...", &no_asset_id[..16.min(no_asset_id.len())]);

        // P0-2: Track all session spawns for cleanup on rotation
        let mut session_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Fill fanout: UserWS → fill_tx → splitter → (InventoryManager, Executor)
        let (fill_tx, mut fill_rx) = mpsc::channel::<FillEvent>(64);
        let (inv_fill_tx, inv_fill_rx) = mpsc::channel::<FillEvent>(64);
        let (exec_fill_tx, exec_fill_rx) = mpsc::channel::<FillEvent>(64);

        // Splitter task: fan-out fills to both InventoryManager and Executor
        session_handles.push(tokio::spawn(async move {
            while let Some(fill) = fill_rx.recv().await {
                let _ = inv_fill_tx.send(fill.clone()).await;
                let _ = exec_fill_tx.send(fill).await;
            }
        }));

        let (exec_tx, exec_rx) = mpsc::channel::<ExecutionCmd>(32);
        let (result_tx, result_rx) = mpsc::channel::<OrderResult>(32);
        let (ofi_md_tx, ofi_md_rx) = mpsc::channel::<MarketDataMsg>(512);
        let (coord_md_tx, coord_md_rx) = mpsc::channel::<MarketDataMsg>(512);
        let (inv_watch_tx, inv_watch_rx) = watch::channel(InventoryState::default());
        let (ofi_watch_tx, ofi_watch_rx) = watch::channel(OfiSnapshot::default());

        let inv = InventoryManager::new(inv_cfg.clone(), inv_fill_rx, inv_watch_tx);
        session_handles.push(tokio::spawn(inv.run()));

        let ofi = OfiEngine::new(ofi_cfg.clone(), ofi_md_rx, ofi_watch_tx);
        session_handles.push(tokio::spawn(ofi.run()));

        let coord = StrategyCoordinator::new(
            coord_cfg.clone(), ofi_watch_rx, inv_watch_rx, coord_md_rx, exec_tx.clone(),
            result_rx,
        );
        session_handles.push(tokio::spawn(coord.run()));

        let executor = Executor::new(
            ExecutorConfig {
                rest_url: settings.rest_url.clone(),
                yes_asset_id: yes_asset_id.clone(),
                no_asset_id: no_asset_id.clone(),
                dry_run,
            },
            clob_client.clone(),
            signer.clone(),
            exec_rx,
            result_tx,
            exec_fill_rx,
        );
        session_handles.push(tokio::spawn(executor.run()));

        // 5. User WS Listener (live mode only — single source of truth for fills)
        if let Some((ref api_key, ref api_secret, ref api_passphrase)) = api_creds {
            let ws_base = if base_settings.ws_base_url.is_empty() {
                "wss://ws-subscriptions-clob.polymarket.com/ws".to_string()
            } else {
                base_settings.ws_base_url.clone()
            };
            let user_ws = UserWsListener::new(
                UserWsConfig {
                    ws_base_url: ws_base,
                    api_key: api_key.clone(),
                    api_secret: api_secret.clone(),
                    api_passphrase: api_passphrase.clone(),
                    market_id: market_id.clone(),
                    yes_asset_id: yes_asset_id.clone(),
                    no_asset_id: no_asset_id.clone(),
                },
                fill_tx,
            );
            session_handles.push(tokio::spawn(user_ws.run()));
            info!("👤 User WS Listener spawned (real fills only)");
        } else {
            info!("📝 DRY-RUN: No User WS — net_diff stays 0 (no fills)");
            // In DRY-RUN mode, fill_tx is unused, fill_rx sees nothing.
            // InventoryManager stays at default state → Coordinator always Balanced.
        }

        info!("🚀 Actors spawned — starting WS feed");

        // ── Step 3: Run until market expires ──
        let reason = run_market_ws(settings, ofi_md_tx, coord_md_tx, end_ts).await;
        info!("🏁 Market ended: {:?}", reason);

        // ── Step 4: Cleanup ──
        let _ = exec_tx.send(ExecutionCmd::CancelAll {
            reason: CancelReason::MarketExpired,
        }).await;
        info!("🧹 CancelAll sent — waiting for executor flush");
        sleep(Duration::from_millis(1200)).await;
        info!("🧹 Aborting session tasks");

        // P0-2: Abort all session tasks to prevent leaking
        for h in session_handles {
            h.abort();
            let _ = h.await;
        }
        // Drop channels to finalize
        drop(exec_tx);

        if !prefix_mode {
            info!("📌 Fixed mode — exiting");
            break;
        }

        // Brief pause before next round
        info!("🔄 Rotating to next market in 3s...");
        sleep(Duration::from_secs(3)).await;
    }

    Ok(())
}
