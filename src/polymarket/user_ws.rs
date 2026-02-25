//! User WebSocket Listener — Authenticated channel for real trade fill events.
//!
//! Connects to Polymarket's authenticated User WS channel to receive
//! real-time fill notifications for our orders. This is the SINGLE SOURCE
//! OF TRUTH for inventory changes.
//!
//! Architecture:
//!   User WS ──trade event──→ parse ──→ FillEvent ──→ InventoryManager
//!
//! Auth flow:
//!   1. Derive L2 API credentials from private key (via REST)
//!   2. Connect to wss://ws-subscriptions-clob.polymarket.com/ws/user
//!   3. Subscribe with API key auth + market/asset IDs
//!   4. Listen for trade events on our asset IDs

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::messages::{FillEvent, FillStatus};
use super::types::Side;

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UserWsConfig {
    /// WebSocket base URL (e.g., wss://ws-subscriptions-clob.polymarket.com/ws)
    pub ws_base_url: String,
    /// L2 API Key (from derive-api-key)
    pub api_key: String,
    /// L2 API Secret
    pub api_secret: String,
    /// L2 API Passphrase
    pub api_passphrase: String,
    /// Polymarket condition_id / market token ID (for subscribe filter)
    pub market_id: String,
    /// YES token asset ID
    pub yes_asset_id: String,
    /// NO token asset ID
    pub no_asset_id: String,
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

pub struct UserWsListener {
    cfg: UserWsConfig,
    fill_tx: mpsc::Sender<FillEvent>,
}

/// Cross-reconnect dedup cache for fill events.
///
/// We keep a bounded TTL cache instead of per-connection HashSet so replayed
/// trade events after reconnect won't be counted twice.
#[derive(Debug)]
struct DedupCache {
    seen_at: HashMap<String, Instant>,
    ttl: Duration,
    max_entries: usize,
}

impl DedupCache {
    fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            seen_at: HashMap::with_capacity(max_entries.min(4096)),
            ttl,
            max_entries,
        }
    }

    fn remember(&mut self, key: String) -> bool {
        let now = Instant::now();
        self.evict_expired(now);

        if self.seen_at.contains_key(&key) {
            return false;
        }
        self.seen_at.insert(key, now);
        self.evict_oldest_if_needed();
        true
    }

    fn evict_expired(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.ttl).unwrap_or(now);
        self.seen_at.retain(|_, ts| *ts >= cutoff);
    }

    fn evict_oldest_if_needed(&mut self) {
        while self.seen_at.len() > self.max_entries {
            let oldest = self
                .seen_at
                .iter()
                .min_by_key(|(_, ts)| *ts)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                self.seen_at.remove(&key);
            } else {
                break;
            }
        }
    }
}

impl UserWsListener {
    pub fn new(cfg: UserWsConfig, fill_tx: mpsc::Sender<FillEvent>) -> Self {
        Self { cfg, fill_tx }
    }

    /// Actor main loop. Connects to User WS with auth, listens for trades.
    /// Reconnects on disconnect. Dedup cache is kept across reconnects.
    pub async fn run(self) {
        info!(
            "👤 UserWsListener started | market={} yes={}... no={}...",
            &self.cfg.market_id[..8.min(self.cfg.market_id.len())],
            &self.cfg.yes_asset_id[..8.min(self.cfg.yes_asset_id.len())],
            &self.cfg.no_asset_id[..8.min(self.cfg.no_asset_id.len())],
        );

        // Keep dedup state across reconnects to avoid replay double-counting.
        // 15 min TTL covers typical reconnect replay windows.
        let mut dedup = DedupCache::new(Duration::from_secs(15 * 60), 50_000);

        loop {
            match self.connect_and_listen(&mut dedup).await {
                Ok(()) => {
                    info!("👤 User WS connection closed normally");
                }
                Err(e) => {
                    warn!("👤 User WS error: {:?}", e);
                }
            }

            info!("👤 Reconnecting User WS in 3s...");
            sleep(Duration::from_secs(3)).await;
        }
    }

    async fn connect_and_listen(&self, dedup: &mut DedupCache) -> anyhow::Result<()> {
        let url = format!("{}/user", self.cfg.ws_base_url);
        info!(%url, "👤 Connecting User WS (authenticated)");

        let connect_result =
            tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await;

        let (ws, response) = match connect_result {
            Ok(Ok((ws, resp))) => (ws, resp),
            Ok(Err(e)) => anyhow::bail!("WS connect error: {:?}", e),
            Err(_) => anyhow::bail!("WS connection timeout"),
        };

        info!("✅ User WS connected (status={:?})", response.status());
        let (mut write, mut read) = ws.split();

        // Subscribe with authentication + market and asset IDs
        //
        // FIX #1: Polymarket User WS requires non-empty markets[] and/or
        // assets_ids[] to receive trade events. We pass both the market
        // condition_id AND the specific asset IDs we care about.
        let subscribe = json!({
            "type": "user",
            "operation": "subscribe",
            "markets": [self.cfg.market_id],
            "assets_ids": [
                self.cfg.yes_asset_id,
                self.cfg.no_asset_id,
            ],
            "auth": {
                "apiKey": self.cfg.api_key,
                "secret": self.cfg.api_secret,
                "passphrase": self.cfg.api_passphrase,
            },
        });
        info!(
            "👤 Subscribe User WS: market={} assets=[{}..., {}...]",
            &self.cfg.market_id[..8.min(self.cfg.market_id.len())],
            &self.cfg.yes_asset_id[..8.min(self.cfg.yes_asset_id.len())],
            &self.cfg.no_asset_id[..8.min(self.cfg.no_asset_id.len())],
        );
        debug!("👤 Subscribe payload: {}", subscribe);

        write.send(Message::Text(subscribe.to_string())).await?;

        // Ping keepalive
        let write_clone = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if write.send(Message::Text("PING".to_string())).await.is_err() {
                    break;
                }
            }
        });

        // Read loop
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        // Handle arrays (batched events)
                        let values = if value.is_array() {
                            value.as_array().cloned().unwrap_or_default()
                        } else {
                            vec![value]
                        };

                        for val in &values {
                            let fills = self.parse_trade_event(val, dedup);
                            for fill in fills {
                                info!(
                                    "🔔 REAL FILL: {:?} {:.2}@{:.3} status={:?} id={}",
                                    fill.side,
                                    fill.filled_size,
                                    fill.price,
                                    fill.status,
                                    &fill.order_id[..8.min(fill.order_id.len())],
                                );
                                let _ = self.fill_tx.send(fill).await;
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    warn!("👤 User WS closed by server");
                    break;
                }
                Err(e) => {
                    warn!("👤 User WS error: {:?}", e);
                    break;
                }
                _ => {}
            }
        }

        write_clone.abort();
        Ok(())
    }

    /// Parse a Polymarket User WS trade event into FillEvent(s).
    ///
    /// **Maker-first parsing**: When we are the maker (`trader_side == "MAKER"`),
    /// the real fill data lives in `maker_orders[]`, NOT the top-level fields.
    /// Top-level `size`/`price`/`taker_order_id` belong to the taker.
    ///
    /// Returns Vec because a single taker trade can match multiple maker orders.
    ///
    /// Status lifecycle:
    ///   MATCHED → MINED → CONFIRMED (happy path)
    ///   MATCHED → FAILED (reversal)
    ///   RETRYING = transient, ignore
    fn parse_trade_event(&self, val: &Value, dedup: &mut DedupCache) -> Vec<FillEvent> {
        // P2 FIX: Case-insensitive event type check
        let event_type = val
            .get("event_type")
            .or_else(|| val.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if !event_type.eq_ignore_ascii_case("trade") {
            return vec![];
        }

        // Parse status
        let status_str = val
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");
        let status = match status_str {
            "MATCHED" => FillStatus::Matched,
            "MINED" | "CONFIRMED" => FillStatus::Confirmed,
            "FAILED" => FillStatus::Failed,
            "RETRYING" => {
                debug!("👤 Ignoring RETRYING status (transient, not a failure)");
                return vec![];
            }
            _ => {
                debug!("👤 Ignoring trade with status: {}", status_str);
                return vec![];
            }
        };

        // P1-7: Routing — check trader_side, but also try maker_orders if missing
        let trader_side = val
            .get("trader_side")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let has_maker_orders = val
            .get("maker_orders")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);

        if trader_side.eq_ignore_ascii_case("MAKER") || (trader_side.is_empty() && has_maker_orders)
        {
            // ═══ MAKER PATH: extract from maker_orders[] ═══
            // When trader_side is missing, we still try maker_orders if present
            // (owner filtering inside will catch non-ours)
            let fills = self.parse_maker_fills(val, status, dedup);
            if fills.is_empty() {
                debug!("👤 Maker path yielded no owned fills — skip top-level taker fallback");
            }
            return fills;
        }

        // ═══ TAKER/UNKNOWN PATH: fallback to top-level fields ═══
        self.parse_taker_fill(val, status, dedup)
            .into_iter()
            .collect()
    }

    /// Extract fills from maker_orders[] — called when trader_side == "MAKER".
    /// Each maker_order has its own order_id, matched_amount, price, and asset_id.
    /// P1-7: Filters by owner to only process OUR fills.
    fn parse_maker_fills(
        &self,
        val: &Value,
        status: FillStatus,
        dedup: &mut DedupCache,
    ) -> Vec<FillEvent> {
        let maker_orders = match val.get("maker_orders").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => {
                warn!("👤 trader_side=MAKER but maker_orders[] missing — skipping event");
                return vec![];
            }
        };

        let mut fills = Vec::new();
        let our_key = self.cfg.api_key.trim().to_lowercase();
        let mut owner_mismatch = false;
        let mut owner_missing = false;

        for mo in maker_orders {
            // P1-7: Owner filtering — only process our own maker fills
            let owner = mo
                .get("owner")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .trim()
                .to_lowercase();

            if owner.is_empty() {
                owner_missing = true;
                continue;
            }

            if owner != our_key {
                owner_mismatch = true;
                debug!(
                    "👤 Skipping maker_order from other owner: {}…",
                    &owner[..8.min(owner.len())],
                );
                continue;
            }

            // Map asset_id to Side — Polymarket asset_ids are large decimal numbers
            // JSON may encode them as strings or numbers, so normalize via to_string()
            let asset_str = mo
                .get("asset_id")
                .map(|v| v.to_string().trim_matches('"').to_string())
                .unwrap_or_default();

            let side = if asset_str == self.cfg.yes_asset_id {
                Side::Yes
            } else if asset_str == self.cfg.no_asset_id {
                Side::No
            } else {
                // Also try the outcome field as fallback
                let outcome = mo.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
                match outcome {
                    "Yes" | "yes" | "YES" => Side::Yes,
                    "No" | "no" | "NO" => Side::No,
                    _ => {
                        debug!("👤 Skipping maker_order with unknown asset: {}", asset_str);
                        continue;
                    }
                }
            };

            let size = parse_f64_field(mo, "matched_amount")
                .or_else(|| parse_f64_field(mo, "size"))
                .unwrap_or(0.0);

            let price = parse_f64_field(mo, "price").unwrap_or(0.0);

            if size <= 0.0 || price <= 0.0 {
                continue;
            }

            let order_id = mo
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            // Dedup using trade-level unique id + maker order_id
            let trade_id = val.get("id").and_then(|v| v.as_str()).unwrap_or_default();

            // Treat MATCHED/MINED/CONFIRMED as one successful fill bucket.
            // This prevents double-counting while still allowing recovery if
            // MATCHED was missed and CONFIRMED arrives first.
            let dedup_bucket = dedup_bucket(status);
            let dedup_key = if !trade_id.is_empty() {
                format!("tid:{}:mo:{}:{}", trade_id, order_id, dedup_bucket)
            } else {
                // No trade id: include size + event identity to avoid collapsing
                // multiple partial fills at the same price.
                let evt = event_identity(val).unwrap_or_else(|| "evt=none".to_string());
                let maker_evt = event_identity(mo).unwrap_or_else(|| "mo_evt=none".to_string());
                format!(
                    "mo:{}:{}:{:.8}:{:.8}:{}:{}",
                    order_id, dedup_bucket, price, size, evt, maker_evt
                )
            };

            if !dedup.remember(dedup_key.clone()) {
                debug!("👤 Dedup: skipping duplicate maker fill key={}", dedup_key);
                continue;
            }

            info!(
                "👤 Maker fill: {:?} {:.2}@{:.3} order={}…",
                side,
                size,
                price,
                &order_id[..8.min(order_id.len())],
            );

            fills.push(FillEvent {
                order_id,
                side,
                filled_size: size,
                price,
                status,
                ts: Instant::now(),
            });
        }

        if fills.is_empty() {
            if owner_mismatch {
                warn!(
                    "👤 maker_orders owner does not match api_key; please verify owner/api_key format"
                );
            } else if owner_missing {
                warn!("👤 maker_orders missing owner field; skipped to avoid wrong inventory");
            }
        }

        fills
    }

    /// Fallback: parse fill from top-level taker fields.
    /// Used when trader_side is missing or we are the taker.
    fn parse_taker_fill(
        &self,
        val: &Value,
        status: FillStatus,
        dedup: &mut DedupCache,
    ) -> Option<FillEvent> {
        let asset_id = val
            .get("asset_id")
            .map(|v| v.to_string().trim_matches('"').to_string())?;
        let side = if asset_id == self.cfg.yes_asset_id {
            Side::Yes
        } else if asset_id == self.cfg.no_asset_id {
            Side::No
        } else {
            debug!(
                "👤 Ignoring trade for unknown asset: {}...",
                &asset_id[..8.min(asset_id.len())]
            );
            return None;
        };

        let size = parse_f64_field(val, "size")?;
        let price = parse_f64_field(val, "price")?;

        if size <= 0.0 || price <= 0.0 {
            return None;
        }

        let order_id = val
            .get("taker_order_id")
            .or_else(|| val.get("order_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        // Dedup
        let trade_id = val.get("id").and_then(|v| v.as_str()).unwrap_or_default();

        let dedup_bucket = dedup_bucket(status);
        let dedup_key = if !trade_id.is_empty() {
            format!("tid:{}:{}", trade_id, dedup_bucket)
        } else {
            let evt = event_identity(val).unwrap_or_else(|| "evt=none".to_string());
            format!(
                "oid:{}:{}:{:.8}:{:.8}:{}",
                order_id, dedup_bucket, price, size, evt
            )
        };

        if !dedup.remember(dedup_key.clone()) {
            debug!("👤 Dedup: skipping duplicate fill key={}", dedup_key);
            return None;
        }

        Some(FillEvent {
            order_id,
            side,
            filled_size: size,
            price,
            status,
            ts: Instant::now(),
        })
    }
}

fn dedup_bucket(status: FillStatus) -> &'static str {
    match status {
        FillStatus::Matched | FillStatus::Confirmed => "SUCCESS",
        FillStatus::Failed => "FAILED",
    }
}

fn value_component(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(n) = v.as_u64() {
        return Some(n.to_string());
    }
    if let Some(n) = v.as_i64() {
        return Some(n.to_string());
    }
    if let Some(n) = v.as_f64() {
        return Some(format!("{n:.0}"));
    }
    None
}

fn event_identity(v: &Value) -> Option<String> {
    const FIELDS: [&str; 10] = [
        "id",
        "trade_id",
        "match_id",
        "tx_hash",
        "transaction_hash",
        "timestamp",
        "time",
        "created_at",
        "updated_at",
        "nonce",
    ];

    for field in FIELDS {
        if let Some(id) = v.get(field).and_then(value_component) {
            return Some(format!("{field}={id}"));
        }
    }
    None
}

/// Parse a JSON field as f64, handling both string ("0.50") and number (0.50) formats.
fn parse_f64_field(val: &Value, field: &str) -> Option<f64> {
    val.get(field).and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn listener() -> UserWsListener {
        let (fill_tx, _fill_rx) = mpsc::channel(8);
        UserWsListener::new(
            UserWsConfig {
                ws_base_url: "wss://example/ws".to_string(),
                api_key: "api-key".to_string(),
                api_secret: "secret".to_string(),
                api_passphrase: "pass".to_string(),
                market_id: "mkt".to_string(),
                yes_asset_id: "1".to_string(),
                no_asset_id: "2".to_string(),
            },
            fill_tx,
        )
    }

    #[test]
    fn test_dedup_cache_blocks_replay() {
        let mut cache = DedupCache::new(Duration::from_secs(60), 16);
        assert!(cache.remember("trade-1".to_string()));
        assert!(!cache.remember("trade-1".to_string()));
    }

    #[test]
    fn test_taker_dedup_does_not_merge_distinct_partial_fills_without_trade_id() {
        let ws = listener();
        let mut dedup = DedupCache::new(Duration::from_secs(60), 16);

        let e1 = json!({
            "event_type": "trade",
            "status": "MATCHED",
            "asset_id": "1",
            "order_id": "o-1",
            "size": "1.0",
            "price": "0.51"
        });
        let e2 = json!({
            "event_type": "trade",
            "status": "MATCHED",
            "asset_id": "1",
            "order_id": "o-1",
            "size": "0.4",
            "price": "0.51"
        });

        let f1 = ws.parse_trade_event(&e1, &mut dedup);
        let f2 = ws.parse_trade_event(&e2, &mut dedup);
        let f3 = ws.parse_trade_event(&e2, &mut dedup);

        assert_eq!(f1.len(), 1);
        assert_eq!(f2.len(), 1);
        assert_eq!(f3.len(), 0);
    }
}

// ─────────────────────────────────────────────────────────
// API Key Derivation (L1 → L2)
// ─────────────────────────────────────────────────────────

/// Derive L2 API credentials from a private key via Polymarket REST API.
///
/// Calls: GET /auth/derive-api-key with L1 EIP-712 signature.
/// Returns: (api_key, api_secret, api_passphrase)
pub async fn derive_api_key(
    rest_url: &str,
    private_key: &str,
) -> anyhow::Result<(String, String, String)> {
    use alloy::signers::local::LocalSigner;
    use alloy::signers::Signer;
    use secrecy::ExposeSecret;

    info!("🔑 Deriving L2 API credentials...");

    // Parse private key
    let signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> =
        std::str::FromStr::from_str(private_key)?;
    let signer = signer.with_chain_id(Some(137));

    // Create unauthenticated CLOB client and derive API key
    use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
    let client = ClobClient::new(rest_url, ClobConfig::default())?;
    let creds = client.derive_api_key(&signer, None).await?;

    // Extract credentials via public accessors
    let api_key = creds.key().to_string();
    let api_secret = creds.secret().expose_secret().to_string();
    let api_passphrase = creds.passphrase().expose_secret().to_string();

    info!(
        "✅ L2 API key derived: {}...",
        &api_key[..8.min(api_key.len())]
    );

    Ok((api_key, api_secret, api_passphrase))
}
