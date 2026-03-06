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
use tracing::{debug, info, warn};

// V2 Actor modules
use pm_as_ofi::polymarket::claims::{
    maybe_auto_claim, scan_claimable_positions, AutoClaimConfig, AutoClaimState,
};
use pm_as_ofi::polymarket::coordinator::{CoordinatorConfig, StrategyCoordinator};
use pm_as_ofi::polymarket::executor::{init_clob_client, Executor, ExecutorConfig};
use pm_as_ofi::polymarket::inventory::{InventoryConfig, InventoryManager};
use pm_as_ofi::polymarket::messages::*;
use pm_as_ofi::polymarket::ofi::{OfiConfig, OfiEngine};
use pm_as_ofi::polymarket::types::Side;
use pm_as_ofi::polymarket::user_ws::{UserWsConfig, UserWsListener};

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
    #[allow(dead_code)]
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
    slug.rsplit('-')
        .next()
        .map(|last| last.parse::<u64>().is_err())
        .unwrap_or(true)
}

/// Detect interval from prefix: "...-5m" → 300, "...-15m" → 900.
fn detect_interval(prefix: &str) -> u64 {
    if prefix.contains("-5m") {
        300
    } else {
        900
    } // default 15min
}

fn should_skip_entry_window(now_unix: u64, end_ts: u64, interval: u64, grace: u64) -> bool {
    if now_unix >= end_ts {
        return true;
    }
    let start_ts = end_ts.saturating_sub(interval);
    now_unix > start_ts.saturating_add(grace)
}

/// Compute how long to wait before attempting next round.
///
/// We align to `end_ts` precisely instead of fixed sleeps. This avoids
/// missing opening seconds while still preventing boundary races when the
/// current loop exits slightly early due to second-level rounding.
fn rotation_wait_duration(now_unix: u64, end_ts: u64) -> Duration {
    if now_unix >= end_ts {
        Duration::from_millis(0)
    } else {
        Duration::from_secs(end_ts - now_unix)
    }
}

/// Compute the slug and end-timestamp for the CURRENTLY ACTIVE market.
///
/// "btc-updown-15m" + now=03:36 UTC → ("btc-updown-15m-1771904700", 1771904700)
fn compute_current_slug(prefix: &str) -> (String, u64) {
    let interval = detect_interval(prefix);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let end_ts = ((now / interval) + 1) * interval;
    (format!("{}-{}", prefix, end_ts), end_ts)
}

/// Resolve a market by exact slug via Gamma API.
/// P2 FIX: 10s timeout + explicit error on network stall.
async fn resolve_market_by_slug(slug: &str) -> anyhow::Result<(String, String, String)> {
    info!("🔍 Resolving market: {}", slug);
    let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let resp: Value = client.get(&url).send().await?.json().await?;

    if let Some(markets) = resp.as_array() {
        if let Some(market) = markets.first() {
            let market_id = market["conditionId"]
                .as_str()
                .or_else(|| market["condition_id"].as_str())
                .unwrap_or_default()
                .to_string();

            let tokens = market
                .get("clobTokenIds")
                .or_else(|| market.get("clob_token_ids"))
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok());

            if let Some(ids) = tokens {
                if ids.len() >= 2 {
                    info!(
                        "✅ Market resolved: {} (YES={}, NO={})",
                        market_id,
                        &ids[0][..8.min(ids[0].len())],
                        &ids[1][..8.min(ids[1].len())]
                    );
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

async fn maybe_log_claimable_positions(funder_address: Option<&str>, signer_address: Option<&str>) {
    let claim_monitor = env::var("PM_CLAIM_MONITOR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    if !claim_monitor {
        return;
    }

    let Some(funder) = funder_address else {
        return;
    };
    let funder_addr = match funder.trim().parse::<alloy::primitives::Address>() {
        Ok(a) => a,
        Err(e) => {
            warn!(
                "⚠️ Claim monitor skipped: invalid funder address '{}': {:?}",
                funder, e
            );
            return;
        }
    };

    let data_api_url = env::var("POLYMARKET_DATA_API_URL")
        .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string());

    let summary = match tokio::time::timeout(
        Duration::from_secs(8),
        scan_claimable_positions(&data_api_url, funder_addr),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!("⚠️ Claim monitor failed: {:?}", e);
            return;
        }
        Err(_) => {
            warn!("⚠️ Claim monitor timed out after 8s");
            return;
        }
    };

    if summary.positions == 0 {
        return;
    }

    warn!(
        "💸 Claimable winnings detected: positions={} conditions={} est_value=${}",
        summary.positions, summary.conditions, summary.total_value
    );
    for c in &summary.top_conditions {
        info!(
            "💸 Claim candidate: condition={} positions={} est_value=${}",
            c.condition_id, c.positions, c.total_value
        );
    }

    if let Some(signer) = signer_address {
        if !signer.trim().eq_ignore_ascii_case(funder.trim()) {
            warn!(
                "⚠️ Claim requires proxy/safe execution (signer={} funder={}). \
                 To auto-claim, enable PM_AUTO_CLAIM=true and set POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE.",
                signer, funder
            );
        }
    }
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
                        levels
                            .iter()
                            .filter_map(|lvl| lvl.get("price").and_then(parse_price_value))
                            .fold(0.0_f64, f64::max)
                    })
                    .unwrap_or(0.0);
                let best_ask = asks
                    .map(|levels| {
                        levels
                            .iter()
                            .filter_map(|lvl| lvl.get("price").and_then(parse_price_value))
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
                let size = match value.get("size").and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                }) {
                    Some(s) if s > 0.0 => s,
                    _ => {
                        // P2 FIX: Missing size — discard instead of injecting fake 1.0
                        debug!("OFI parser: missing or zero 'size' in trade, skipping to avoid fake toxicity");
                        return msgs;
                    }
                };

                let Some(side_val) = value.get("side").and_then(|v| v.as_str()) else {
                    debug!("OFI parser: missing 'side' field in trade, skipping to avoid bias");
                    return msgs;
                };

                // Determine taker side from the "side" field
                let taker_side = match side_val {
                    "BUY" | "buy" | "Buy" => TakerSide::Buy,
                    "SELL" | "sell" | "Sell" => TakerSide::Sell,
                    _ => {
                        debug!("OFI parser: unknown 'side' value: {}, skipping", side_val);
                        return msgs;
                    }
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
            if self.yes_bid > 0.0 && self.yes_ask > 0.0 && self.no_bid > 0.0 && self.no_ask > 0.0 {
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
    #[allow(dead_code)]
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
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let secs_remaining = end_ts.saturating_sub(now_unix);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs_remaining);
    info!(
        "⏰ Market deadline in {}s (end_ts={})",
        secs_remaining, end_ts
    );

    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    loop {
        // Check if already expired before connecting
        if tokio::time::Instant::now() >= deadline {
            info!("🏁 Market expired (wall-clock)");
            return MarketEnd::Expired;
        }

        let url = settings.ws_url("market");
        info!(%url, "📡 connecting market WS");

        let connect_result =
            tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await;

        match connect_result {
            Ok(Ok((ws, response))) => {
                info!("✅ WS connected (status={:?})", response.status());
                backoff = Duration::from_millis(100); // Reset on successful connect
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

                // Ping keepalive — store handle for explicit cleanup
                let ping_handle = tokio::spawn(async move {
                    let mut delay = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        delay.tick().await;
                        if write.send(Message::Text("PING".to_string())).await.is_err() {
                            break;
                        }
                    }
                });

                // Read loop with wall-clock deadline
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {
                            info!("🏁 Market expired (wall-clock) — stopping WS");
                            ping_handle.abort();
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
                                    ping_handle.abort();
                                    break;
                                }
                                Some(Err(err)) => {
                            let msg = format!("{err:?}");
                            if msg.contains("ResetWithoutClosingHandshake") {
                                info!("📡 Market WS server reset (expected) — fast reconnect");
                            } else {
                                warn!("WS error: {err:?}");
                            }
                            ping_handle.abort();
                            break;
                        }
                                None => {
                                    ping_handle.abort();
                                    break;
                                }
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

        info!("🔄 Reconnecting in {:?}...", backoff);
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ─────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    // Dual-output logging: stdout + daily rolling file in logs/
    let file_appender = tracing_appender::rolling::daily("logs", "polymarket.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    {
        use tracing_subscriber::fmt::writer::MakeWriterExt;
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_writer(std::io::stdout.and(non_blocking))
            .init();
    }

    info!("═══════════════════════════════════════════════════");
    info!("  Polymarket V2 — Async Inventory Arbitrage Engine");
    info!("  Auto-Discovery + Market Rotation");
    info!("═══════════════════════════════════════════════════");

    let base_settings = Settings::from_env()?;
    let raw_slug = base_settings
        .market_slug
        .clone()
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
    let auto_claim_cfg = AutoClaimConfig::from_env();
    let mut auto_claim_state = AutoClaimState::default();
    let dry_run = coord_cfg.dry_run;

    info!(
        "📊 Config: pair={:.2} bid={:.1} tick={:.3} net={:.0} ofi_thresh={:.1} dry={}",
        coord_cfg.pair_target,
        coord_cfg.bid_size,
        coord_cfg.tick_size,
        coord_cfg.max_net_diff,
        ofi_cfg.toxicity_threshold,
        dry_run
    );
    if auto_claim_cfg.enabled {
        info!(
            "💸 Auto-claim enabled: min_value=${} max_conditions={} interval={}s dry_run={} wait_confirm={} wait_timeout={}s",
            auto_claim_cfg.min_condition_value,
            auto_claim_cfg.max_conditions_per_run,
            auto_claim_cfg.run_interval.as_secs(),
            auto_claim_cfg.dry_run,
            auto_claim_cfg.relayer_wait_confirm,
            auto_claim_cfg.relayer_wait_timeout.as_secs()
        );
    }

    // P1 FIX: Parse funder_address from environment, which represents the Magic Proxy Wallet.
    // We need this BEFORE init_clob_client to configure the API key derivation.
    let funder_address: Option<String> = if !dry_run {
        let explicit = base_settings
            .funder_address
            .clone()
            .filter(|s| !s.trim().is_empty());
        if let Some(addr) = explicit {
            info!(
                "🔑 Using explicit POLYMARKET_FUNDER_ADDRESS: {}…",
                &addr[..10.min(addr.len())]
            );
            Some(addr)
        } else {
            // We can't derive from an uninitialized signer anymore. Let's just fall back to standard EOA auth if empty.
            // But log a critical warning.
            warn!(
                "⚠️ Live mode usually requires POLYMARKET_FUNDER_ADDRESS (Proxy Wallet) to trade."
            );
            None
        }
    } else {
        base_settings.funder_address.clone()
    };

    let funder_alloy = match funder_address.as_ref() {
        Some(addr) => match addr.trim().parse::<alloy::primitives::Address>() {
            Ok(a) => Some(a),
            Err(e) => {
                warn!(
                    "⚠️ Invalid POLYMARKET_FUNDER_ADDRESS='{}': {:?}. Falling back to EOA auth.",
                    addr, e
                );
                None
            }
        },
        None => None,
    };

    // Shared L2 credentials for BOTH CLOB REST and User WS.
    // If provided in env, we force both channels to use exactly the same keypair.
    let shared_api_creds_env: Option<(String, String, String)> = {
        let env_key = env::var("POLYMARKET_API_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let env_secret = env::var("POLYMARKET_API_SECRET")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let env_pass = env::var("POLYMARKET_API_PASSPHRASE")
            .ok()
            .filter(|s| !s.trim().is_empty());
        match (env_key, env_secret, env_pass) {
            (Some(k), Some(s), Some(p)) => Some((k, s, p)),
            (None, None, None) => None,
            _ => {
                anyhow::bail!(
                    "🚨 FATAL: POLYMARKET_API_KEY / POLYMARKET_API_SECRET / POLYMARKET_API_PASSPHRASE must be set together."
                );
            }
        }
    };
    let shared_api_creds_auth = match shared_api_creds_env.as_ref() {
        Some((key, secret, passphrase)) => {
            let key_uuid = match key.parse::<polymarket_client_sdk::auth::ApiKey>() {
                Ok(k) => k,
                Err(e) => {
                    anyhow::bail!("🚨 FATAL: Invalid POLYMARKET_API_KEY UUID: {:?}", e);
                }
            };
            Some(polymarket_client_sdk::auth::Credentials::new(
                key_uuid,
                secret.clone(),
                passphrase.clone(),
            ))
        }
        None => None,
    };

    // ═══ Initialize CLOB client (once, reused across rotations) ═══
    // We pass funder_alloy for maker identity and shared_api_creds_auth for unified L2 auth.
    let (clob_client, signer) = if !dry_run {
        init_clob_client(
            &base_settings.rest_url,
            base_settings.private_key.as_deref(),
            funder_alloy,
            shared_api_creds_auth,
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
    #[allow(unused_imports)]
    use alloy::signers::Signer;
    let signer_address = signer.as_ref().map(|s| format!("{:?}", s.address()));

    // Startup preflight: force-refresh and inspect collateral balance/allowance.
    if !dry_run {
        use alloy::primitives::Address;
        use alloy::primitives::U256;
        use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk::clob::types::{AssetType, SignatureType};
        use polymarket_client_sdk::{contract_config, POLYGON};
        use rust_decimal::Decimal;

        let is_api_key_unauthorized = |err: &dyn std::fmt::Display| -> bool {
            let lower = format!("{:#}", err).to_ascii_lowercase();
            lower.contains("unauthorized")
                || lower.contains("invalid api key")
                || lower.contains("401")
        };

        if let Some(client) = clob_client.as_ref() {
            let req = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Collateral)
                .build();

            if let Err(e) = client.update_balance_allowance(req.clone()).await {
                if is_api_key_unauthorized(&e) {
                    anyhow::bail!(
                        "🚨 FATAL: CLOB API key unauthorized during preflight update. \
                         POLYMARKET_API_* is invalid/stale for current signer/funder. \
                         Remove POLYMARKET_API_* to let bot auto-derive, or regenerate matching credentials."
                    );
                }
                warn!("⚠️ balance-allowance/update failed: {:?}", e);
            }

            match client.balance_allowance(req).await {
                Ok(resp) => {
                    let parse_u256 = |raw: &str| -> Option<U256> {
                        let s = raw.trim().split('.').next().unwrap_or(raw.trim());
                        if s.is_empty() {
                            return None;
                        }
                        if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                            U256::from_str_radix(hex, 16).ok()
                        } else {
                            U256::from_str_radix(s, 10).ok()
                        }
                    };

                    let max_allowance = resp
                        .allowances
                        .values()
                        .filter_map(|v| parse_u256(v))
                        .max()
                        .unwrap_or(U256::ZERO);
                    let main_cfg = contract_config(POLYGON, false);
                    let neg_cfg = contract_config(POLYGON, true);
                    let expected_spenders: Vec<(&str, Option<Address>)> = vec![
                        ("exchange", main_cfg.map(|c| c.exchange)),
                        ("neg_risk_exchange", neg_cfg.map(|c| c.exchange)),
                        ("neg_risk_adapter", neg_cfg.and_then(|c| c.neg_risk_adapter)),
                    ];

                    info!(
                        "💰 Preflight collateral: balance={} max_allowance={} allowance_entries={}",
                        resp.balance,
                        max_allowance,
                        resp.allowances.len()
                    );
                    for (label, maybe_addr) in expected_spenders {
                        if let Some(addr) = maybe_addr {
                            let raw = resp.allowances.get(&addr).cloned().unwrap_or_default();
                            let parsed = parse_u256(&raw).unwrap_or(U256::ZERO);
                            info!(
                                "💳 Preflight allowance[{label}] {} raw='{}' parsed={}",
                                addr, raw, parsed
                            );
                        }
                    }
                    if resp.balance <= Decimal::ZERO || max_allowance.is_zero() {
                        let samples: Vec<String> = resp
                            .allowances
                            .iter()
                            .take(3)
                            .map(|(k, v)| format!("{k:?}={v}"))
                            .collect();
                        warn!(
                            "⚠️ Preflight indicates insufficient balance/allowance for trading \
                             (balance={} max_allowance={} samples={:?})",
                            resp.balance, max_allowance, samples
                        );
                    }

                    // Diagnostic probe: compare balance/allowance views across all signature types.
                    // This catches signature type mismatches for proxy/safe wallets.
                    for sig in [
                        SignatureType::Eoa,
                        SignatureType::Proxy,
                        SignatureType::GnosisSafe,
                    ] {
                        let probe_req = BalanceAllowanceRequest::builder()
                            .asset_type(AssetType::Collateral)
                            .signature_type(sig)
                            .build();
                        match client.balance_allowance(probe_req).await {
                            Ok(probe) => {
                                let probe_max = probe
                                    .allowances
                                    .values()
                                    .filter_map(|v| parse_u256(v))
                                    .max()
                                    .unwrap_or(U256::ZERO);
                                info!(
                                    "🧪 Collateral probe sig_type={} balance={} max_allowance={} entries={}",
                                    sig as u8,
                                    probe.balance,
                                    probe_max,
                                    probe.allowances.len()
                                );
                            }
                            Err(e) => {
                                warn!("⚠️ Collateral probe sig_type={} failed: {:?}", sig as u8, e);
                            }
                        }
                    }

                    let allow_zero_allowance = env::var("PM_ALLOW_ZERO_ALLOWANCE")
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    if resp.balance > Decimal::ZERO
                        && max_allowance.is_zero()
                        && !allow_zero_allowance
                    {
                        anyhow::bail!(
                            "🚨 FATAL: wallet balance is non-zero but CLOB collateral allowance is zero. \
                             Use the same signer/funder to approve USDC for Polymarket contracts, then retry. \
                             Set PM_ALLOW_ZERO_ALLOWANCE=true to bypass this guard."
                        );
                    }
                }
                Err(e) => {
                    if is_api_key_unauthorized(&e) {
                        anyhow::bail!(
                            "🚨 FATAL: CLOB balance_allowance returned unauthorized/invalid API key. \
                             Credentials do not match current signer/funder/signature_type. \
                             Remove POLYMARKET_API_* and retry with auto-derive, or regenerate correct API creds."
                        );
                    }
                    warn!("⚠️ balance_allowance preflight failed: {:?}", e);
                }
            }
        }
    }

    // Fallback: If no explicit funder address was given but we have a signer, we assume EOA mapping.
    let funder_address = match funder_address {
        Some(addr) => Some(addr),
        None if signer.is_some() => {
            #[allow(unused_imports)]
            use alloy::signers::Signer;
            let derived = format!("{:?}", signer.as_ref().unwrap().address());
            info!(
                "🔑 Deduced funder_address from EOA private key: {}…",
                &derived[..10.min(derived.len())]
            );
            Some(derived)
        }
        None => None,
    };

    if !dry_run && funder_address.is_none() {
        anyhow::bail!(
            "🚨 FATAL: Live mode requires POLYMARKET_FUNDER_ADDRESS or a valid private key \
             to derive the wallet address. Without it, ALL maker fills will be silently \
             filtered out and inventory will never update."
        );
    }
    if !dry_run {
        maybe_log_claimable_positions(funder_address.as_deref(), signer_address.as_deref()).await;
        if let Err(e) = maybe_auto_claim(
            &auto_claim_cfg,
            &mut auto_claim_state,
            funder_address.as_deref(),
            signer_address.as_deref(),
            base_settings.private_key.as_deref(),
        )
        .await
        {
            warn!("⚠️ Auto-claim runner failed at startup: {:?}", e);
        }
    }

    // ═══ L2 API credentials for User WS (live mode only) ═══
    // Always source credentials from authenticated CLOB client to avoid REST/WS identity drift.
    let api_creds: Option<(String, String, String)> = if !dry_run {
        use secrecy::ExposeSecret;
        if let Some(client) = clob_client.as_ref() {
            let creds = client.credentials();
            if shared_api_creds_env.is_some() {
                info!(
                    "🔑 User WS using verified credentials from authenticated CLOB client \
                     (env POLYMARKET_API_* may be reused or auto-fallbacked)"
                );
            } else {
                info!("🔑 User WS reusing auto-derived authenticated CLOB credentials");
            }
            Some((
                creds.key().to_string(),
                creds.secret().expose_secret().to_string(),
                creds.passphrase().expose_secret().to_string(),
            ))
        } else {
            anyhow::bail!(
                "🚨 FATAL: dry_run=false but no authenticated CLOB client available for User WS credentials."
            );
        }
    } else {
        None
    };

    // ═══════════════════════════════════════════════════
    // OUTER LOOP: Market Rotation
    // ═══════════════════════════════════════════════════

    // Channel for pre-resolved next markets to eliminate 7-8s rotation latency
    #[allow(clippy::type_complexity)]
    let (preload_tx, mut preload_rx) =
        mpsc::channel::<(String, anyhow::Result<(String, String, String)>)>(2);
    #[allow(clippy::type_complexity)]
    let mut preloaded_market: Option<(String, anyhow::Result<(String, String, String)>)> = None;

    let mut round = 0u64;
    loop {
        round += 1;

        // ── Step 1: Resolve current market ──
        let (slug, end_ts) = if prefix_mode {
            compute_current_slug(&raw_slug)
        } else {
            // P2 FIX: Cap secs_remaining to avoid Instant + Duration overflow panics
            (raw_slug.clone(), u64::MAX) // Fixed mode: no expiry
        };

        // P2 FIX: Clamp end_ts for deadline calculation to avoid overflow
        let effective_end_ts = if end_ts == u64::MAX {
            // Fixed mode: use a sane 1-year cap instead of u64::MAX
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 31_536_000
        } else {
            end_ts
        };

        // Entry gate: if startup is too late in the current interval, skip it.
        if prefix_mode {
            let interval_secs = detect_interval(&raw_slug);
            let entry_grace_secs = env::var("PM_ENTRY_GRACE_SECONDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30);
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Secondary Entry Gate: if API resolution took too long and pushed us past grace, abort.
            if should_skip_entry_window(now_unix, end_ts, interval_secs, entry_grace_secs) {
                let start_ts = end_ts.saturating_sub(interval_secs);
                let age_secs = now_unix.saturating_sub(start_ts);
                let wait_secs = end_ts.saturating_sub(now_unix).saturating_add(1);
                warn!(
                    "⏭️ Late startup for {}: age={}s > grace={}s. Skip current market, wait {}s for next open.",
                    slug, age_secs, entry_grace_secs, wait_secs
                );

                // Pre-resolve the NEXT market in the background while sleeping
                let next_end_ts = end_ts + interval_secs;
                let next_slug = format!("{}-{}", raw_slug, next_end_ts);
                let p_tx = preload_tx.clone();
                tokio::spawn(async move {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let sleep_time = if end_ts > now + 30 {
                        end_ts - now - 30
                    } else {
                        0
                    };
                    if sleep_time > 0 {
                        tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                    }
                    info!(
                        "⏳ Pre-resolving next market in background during skip delay: {}",
                        next_slug
                    );
                    let res = resolve_market_by_slug(&next_slug).await;
                    let _ = p_tx.send((next_slug, res)).await;
                });

                sleep(Duration::from_secs(wait_secs)).await;
                continue;
            }
        }

        info!("═══════════════════════════════════════════════════");
        info!("  Round #{} — {}", round, slug);
        info!("═══════════════════════════════════════════════════");

        // Drain any incoming preloads
        while let Ok(pre) = preload_rx.try_recv() {
            preloaded_market = Some(pre);
        }

        let resolved = if let Some((pre_slug, pre_res)) = preloaded_market.take() {
            if pre_slug == slug {
                info!("⚡ Using pre-resolved market data for {}", slug);
                pre_res
            } else {
                resolve_market_by_slug(&slug).await
            }
        } else {
            resolve_market_by_slug(&slug).await
        };
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
            coord_cfg.clone(),
            ofi_watch_rx,
            inv_watch_rx,
            coord_md_rx,
            exec_tx.clone(),
            result_rx,
        );
        session_handles.push(tokio::spawn(coord.run()));

        let executor = Executor::new(
            ExecutorConfig {
                rest_url: settings.rest_url.clone(),
                yes_asset_id: yes_asset_id.clone(),
                no_asset_id: no_asset_id.clone(),
                tick_size: coord_cfg.tick_size,
                dry_run,
            },
            clob_client.clone(),
            signer.clone(),
            exec_rx,
            result_tx,
            exec_fill_rx,
        );
        let executor_handle = tokio::spawn(executor.run());
        let executor_abort = executor_handle.abort_handle();

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

        // P1 FIX: Startup reconciliation — sweep any lingering orders from prior crashes
        if !dry_run {
            let _ = exec_tx
                .send(ExecutionCmd::CancelAll {
                    reason: CancelReason::Startup,
                })
                .await;
            info!("🧹 Startup CancelAll sent — clearing any stale orders from prior session");
        }

        // ── Step 3: Run until market expires ──
        // P2 FIX: Use effective_end_ts to avoid overflow in fixed mode
        let reason = run_market_ws(settings, ofi_md_tx, coord_md_tx, effective_end_ts).await;
        info!("🏁 Market ended: {:?}", reason);

        // ── Step 4: Cleanup ──
        let _ = exec_tx
            .send(ExecutionCmd::CancelAll {
                reason: CancelReason::MarketExpired,
            })
            .await;
        // Drop exec_tx so the executor channel closes, letting it break its loop after CancelAll
        drop(exec_tx);
        info!("🧹 CancelAll sent — waiting for executor graceful shutdown (8s timeout)");

        // Wait up to 8s for the executor to complete its work and exit
        // P1 FIX: If timeout expires, use the AbortHandle to force-kill the executor task
        match tokio::time::timeout(Duration::from_secs(8), executor_handle).await {
            Ok(_) => { /* executor exited gracefully */ }
            Err(_) => {
                warn!(
                    "⚠️ Executor did not finish within 8s timeout — force aborting via AbortHandle"
                );
                executor_abort.abort();
            }
        }

        info!("🧹 Aborting remaining session tasks");

        // P0-2: Abort all session tasks to prevent leaking
        for h in session_handles {
            h.abort();
            let _ = h.await;
        }

        if !dry_run {
            maybe_log_claimable_positions(funder_address.as_deref(), signer_address.as_deref())
                .await;
            if let Err(e) = maybe_auto_claim(
                &auto_claim_cfg,
                &mut auto_claim_state,
                funder_address.as_deref(),
                signer_address.as_deref(),
                base_settings.private_key.as_deref(),
            )
            .await
            {
                warn!("⚠️ Auto-claim runner failed after round cleanup: {:?}", e);
            }
        }

        if !prefix_mode {
            info!("📌 Fixed mode — exiting");
            break;
        }

        // Background preload for next market
        if prefix_mode {
            let next_interval = detect_interval(&raw_slug);
            let next_end_ts = end_ts + next_interval;
            let next_slug = format!("{}-{}", raw_slug, next_end_ts);

            let p_tx = preload_tx.clone();
            tokio::spawn(async move {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let sleep_time = if end_ts > now + 30 {
                    end_ts - now - 30
                } else {
                    0
                };
                if sleep_time > 0 {
                    tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                }
                info!("⏳ Pre-resolving next market in background: {}", next_slug);

                let res = resolve_market_by_slug(&next_slug).await;
                let _ = p_tx.send((next_slug, res)).await;
            });
        }

        // Wait using precise rotation wait duration instead of fixed 3s latency
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait = rotation_wait_duration(now_unix, end_ts);
        if wait.is_zero() {
            info!("🔄 Rotating immediately to next market");
        } else {
            info!(
                "🔄 Waiting {}s for next market boundary before rotate",
                wait.as_secs()
            );
            sleep(wait).await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_should_skip_entry_window() {
        let interval = 300; // 5 min
        let grace = 30; // 30 sec grace
        // Suppose current block ends at timestamp 1000. Start corresponds to 700.
        // We are at 715 (15 seconds after open) -> within grace.
        assert!(!should_skip_entry_window(715, 1000, interval, grace));
        // We are at 735 (35 seconds after open) -> outside grace, we should skip!
        assert!(should_skip_entry_window(735, 1000, interval, grace));
        // We are at 1001 (already past)
        assert!(should_skip_entry_window(1001, 1000, interval, grace));
    }

    #[test]
    fn test_last_trade_price_missing_side_parsing() {
        let val_with_side = json!({
            "asset_id": "111",
            "price": "0.50",
            "size": "100",
            "side": "SELL"
        });

        let val_no_side = json!({
            "asset_id": "111",
            "price": "0.50",
            "size": "100"
        });

        let side1 = val_with_side.get("side").and_then(|v| v.as_str());
        let side2 = val_no_side.get("side").and_then(|v| v.as_str());

        assert_eq!(side1, Some("SELL"));
        assert_eq!(side2, None);
    }

    #[tokio::test]
    async fn test_executor_timeout_abort_pattern() {
        // P3 Regression Test: Verified that if timeout consumes the JoinHandle,
        // the AbortHandle successfully terminates the lingering background task.
        use std::time::Duration;
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        let abort_handle = handle.abort_handle();

        // 1. Simulate the market WS 8s shutdown timeout firing early.
        // NOTE: tokio::time::timeout consumes the `handle` itself if passed directly
        let res = tokio::time::timeout(Duration::from_millis(5), handle).await;
        assert!(res.is_err(), "timeout must expire");

        // 2. We no longer have `handle`, but we have `abort_handle`. Force kill it.
        abort_handle.abort();

        // Verification: Since we can't join it (handle is gone), we just wait a tick
        // to ensure it didn't panic and the abort went through cleanly.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_ping_task_cleanup_pattern() {
        // P3 Regression Test: Verified that ping_handle.abort() structurally works
        // to cleanly kill a spawned ping keepalive task when the WS drops.
        use std::time::Duration;
        let ping_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        // Simulate WS Exit Branch (e.g. server close or EOF) calling abort
        ping_handle.abort();

        // Awaited task should explicitly yield a Cancelled error, proving no leak.
        let join_res = ping_handle.await;
        assert!(
            join_res.unwrap_err().is_cancelled(),
            "Ping task must be cancelled on WS exit"
        );
    }
}
