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

use std::collections::HashSet;
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

impl UserWsListener {
    pub fn new(cfg: UserWsConfig, fill_tx: mpsc::Sender<FillEvent>) -> Self {
        Self { cfg, fill_tx }
    }

    /// Actor main loop. Connects to User WS with auth, listens for trades.
    /// Reconnects on disconnect. Dedup set resets on each reconnect.
    pub async fn run(self) {
        info!(
            "👤 UserWsListener started | market={} yes={}... no={}...",
            &self.cfg.market_id[..8.min(self.cfg.market_id.len())],
            &self.cfg.yes_asset_id[..8.min(self.cfg.yes_asset_id.len())],
            &self.cfg.no_asset_id[..8.min(self.cfg.no_asset_id.len())],
        );

        loop {
            match self.connect_and_listen().await {
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

    async fn connect_and_listen(&self) -> anyhow::Result<()> {
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

        // FIX #3: Dedup set — prevents double-counting on reconnect or
        // duplicate pushes. Key = (trade_id, status). Resets per connection.
        let mut seen: HashSet<String> = HashSet::new();

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
                            let fills = self.parse_trade_event(val, &mut seen);
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
    fn parse_trade_event(&self, val: &Value, seen: &mut HashSet<String>) -> Vec<FillEvent> {
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
            let fills = self.parse_maker_fills(val, status, seen);
            if fills.is_empty() {
                debug!("👤 Maker path yielded no owned fills — skip top-level taker fallback");
            }
            return fills;
        }

        // ═══ TAKER/UNKNOWN PATH: fallback to top-level fields ═══
        self.parse_taker_fill(val, status, seen)
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
        seen: &mut HashSet<String>,
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
            let dedup_bucket = match status {
                FillStatus::Matched | FillStatus::Confirmed => "SUCCESS",
                FillStatus::Failed => "FAILED",
            };
            let dedup_key = if !trade_id.is_empty() {
                format!("tid:{}:mo:{}:{}", trade_id, order_id, dedup_bucket)
            } else {
                format!("mo:{}:{}:{}", order_id, dedup_bucket, price)
            };

            if !seen.insert(dedup_key.clone()) {
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
        seen: &mut HashSet<String>,
    ) -> Option<FillEvent> {
        let asset_id = val.get("asset_id").and_then(|v| v.as_str())?;
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

        let dedup_bucket = match status {
            FillStatus::Matched | FillStatus::Confirmed => "SUCCESS",
            FillStatus::Failed => "FAILED",
        };
        let dedup_key = if !trade_id.is_empty() {
            format!("tid:{}:{}", trade_id, dedup_bucket)
        } else {
            format!("oid:{}:{}:{}", order_id, dedup_bucket, price)
        };

        if !seen.insert(dedup_key.clone()) {
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

/// Parse a JSON field as f64, handling both string ("0.50") and number (0.50) formats.
fn parse_f64_field(val: &Value, field: &str) -> Option<f64> {
    val.get(field).and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    })
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
