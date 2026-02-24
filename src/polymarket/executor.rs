//! Executor Actor — Maker-Only order management.
//!
//! Tracks active open orders in a `HashMap<Side, Vec<String>>`.
//! All orders are Post-Only (maker limit bids).
//!
//! CRITICAL: The Executor NEVER emits FillEvents.
//! Fills come exclusively from the authenticated User WebSocket.
//!
//! On order placement failure, sends OrderResult::OrderFailed to Coordinator
//! to prevent ghost slot states.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Context;
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::messages::*;
use super::types::Side;

use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use rust_decimal::prelude::FromPrimitive;

type AuthClient = ClobClient<Authenticated<polymarket_client_sdk::auth::Normal>>;

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub rest_url: String,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub dry_run: bool,
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

pub struct Executor {
    cfg: ExecutorConfig,
    client: Option<AuthClient>,
    signer: Option<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
    cmd_rx: mpsc::Receiver<ExecutionCmd>,
    /// Send order failure notifications to Coordinator.
    result_tx: mpsc::Sender<OrderResult>,
    /// Receive fill events to clean up open_orders lifecycle.
    fill_rx: mpsc::Receiver<FillEvent>,

    /// Active open orders tracked per side: order_id → remaining_size.
    /// Enables partial fill tracking — only removes when fully filled.
    open_orders: HashMap<Side, HashMap<String, f64>>,
}

impl Executor {
    pub fn new(
        cfg: ExecutorConfig,
        client: Option<AuthClient>,
        signer: Option<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
        cmd_rx: mpsc::Receiver<ExecutionCmd>,
        result_tx: mpsc::Sender<OrderResult>,
        fill_rx: mpsc::Receiver<FillEvent>,
    ) -> Self {
        let mut open_orders = HashMap::new();
        open_orders.insert(Side::Yes, HashMap::new());
        open_orders.insert(Side::No, HashMap::new());

        Self {
            cfg,
            client,
            signer,
            cmd_rx,
            result_tx,
            fill_rx,
            open_orders,
        }
    }

    pub async fn run(mut self) {
        info!(
            "⚡ Executor started [MAKER-ONLY] | dry_run={} has_client={}",
            self.cfg.dry_run,
            self.client.is_some(),
        );

        loop {
            tokio::select! {
                // Command channel (from Coordinator)
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(ExecutionCmd::PlacePostOnlyBid { side, price, size, reason }) => {
                            self.handle_place_bid(side, price, size, reason).await;
                        }
                        Some(ExecutionCmd::CancelOrder { order_id, reason }) => {
                            let _ = self.handle_cancel_order(&order_id, reason).await;
                        }
                        Some(ExecutionCmd::CancelSide { side, reason }) => {
                            self.handle_cancel_side(side, reason).await;
                        }
                        Some(ExecutionCmd::CancelAll { reason }) => {
                            self.handle_cancel_all(reason).await;
                        }
                        None => break, // Channel closed
                    }
                }
                // FIX #4: Fill notifications — clean up open_orders lifecycle
                fill = self.fill_rx.recv() => {
                    if let Some(fill) = fill {
                        self.handle_fill_notification(&fill);
                    }
                }
            }
        }

        info!("⚡ Executor shutting down");
    }

    // ─────────────────────────────────────────────────
    // Fill Notifications (from User WS → clean up open_orders)
    // ─────────────────────────────────────────────────

    /// Handle fill notifications from User WS.
    /// MATCHED: decrement remaining_size, remove when fully filled.
    /// FAILED: order is dead/reverted — remove entirely.
    fn handle_fill_notification(&mut self, fill: &FillEvent) {
        // P1-3: FAILED = order terminated, remove entirely
        if fill.status == FillStatus::Failed {
            let orders = self.open_orders.entry(fill.side).or_default();
            if orders.remove(&fill.order_id).is_some() {
                warn!(
                    "📋 Lifecycle: {:?} order {}… FAILED — removed from tracking ({} remaining)",
                    fill.side,
                    &fill.order_id[..8.min(fill.order_id.len())],
                    orders.len(),
                );
            }
            return;
        }

        // MATCHED path: decrement remaining size
        let orders = self.open_orders.entry(fill.side).or_default();
        if let Some(remaining) = orders.get_mut(&fill.order_id) {
            *remaining -= fill.filled_size;
            if *remaining <= 0.0 {
                orders.remove(&fill.order_id);
                info!(
                    "📋 Lifecycle: {:?} order {}… fully filled — removed ({} remaining on side)",
                    fill.side,
                    &fill.order_id[..8.min(fill.order_id.len())],
                    orders.len(),
                );
            } else {
                info!(
                    "📋 Lifecycle: {:?} order {}… partial fill {:.2}, remaining={:.2}",
                    fill.side,
                    &fill.order_id[..8.min(fill.order_id.len())],
                    fill.filled_size,
                    remaining,
                );
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Place Post-Only Bid
    // ─────────────────────────────────────────────────

    async fn handle_place_bid(&mut self, side: Side, price: f64, size: f64, reason: BidReason) {
        let reason_str = match reason {
            BidReason::Provide => "PROVIDE",
            BidReason::Hedge => "HEDGE",
        };
        info!(
            "📤 {} PostOnlyBid {:?}@{:.3} size={:.1}",
            reason_str, side, price, size,
        );

        if self.cfg.dry_run || self.client.is_none() {
            info!(
                "📝 [DRY-RUN] PostOnlyBid {:?}@{:.3} size={:.1}",
                side, price, size,
            );
            // DRY-RUN: track fake order, but NO FillEvent.
            // net_diff stays 0 → always Balanced. Correct for paper trading.
            let fake_id = format!("dry-{:?}-{}", side, Instant::now().elapsed().as_nanos());
            if let Some(orders) = self.open_orders.get_mut(&side) {
                orders.insert(fake_id, size);
            }
            return;
        }

        // Safety: do not place a new bid on a side while we still track
        // uncanceled live orders on that side.
        if let Some(existing) = self.open_orders.get(&side) {
            if !existing.is_empty() {
                warn!(
                    "🚫 Refusing PlacePostOnlyBid {:?}@{:.3}: {} tracked order(s) still open on side",
                    side, price, existing.len(),
                );
                let _ = self.result_tx.send(OrderResult::OrderFailed { side }).await;
                return;
            }
        }

        match self.place_post_only_order(side, price, size).await {
            Ok(order_id) => {
                info!("✅ Order placed: {:?}@{:.3} id={}", side, price, order_id);
                if let Some(orders) = self.open_orders.get_mut(&side) {
                    orders.insert(order_id, size);
                }
                // NO FillEvent here. Fills come from User WS only.
            }
            Err(e) => {
                warn!("❌ Failed to place PostOnlyBid {:?}: {:?}", side, e);
                // FIX #4: Notify Coordinator the order failed so it can reset the slot
                let _ = self.result_tx.send(OrderResult::OrderFailed { side }).await;
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Cancel operations
    // ─────────────────────────────────────────────────

    async fn handle_cancel_order(&mut self, order_id: &str, reason: CancelReason) -> bool {
        info!("🗑️ Cancel order {} (reason={:?})", order_id, reason);

        if self.cfg.dry_run || self.client.is_none() {
            // DRY-RUN: remove from tracking immediately
            for orders in self.open_orders.values_mut() {
                orders.remove(order_id);
            }
            info!("📝 [DRY-RUN] CancelOrder {}", order_id);
            return true;
        }

        // P1-4: Call remote FIRST. Only remove from local tracking on success.
        // If remote fails, keep tracking to avoid "blind orders".
        if let Some(client) = &self.client {
            match client.cancel_order(order_id).await {
                Ok(_) => {
                    for orders in self.open_orders.values_mut() {
                        orders.remove(order_id);
                    }
                    info!("✅ Order canceled: {}", order_id);
                    return true;
                }
                Err(e) => {
                    warn!(
                        "❌ Cancel failed {}: {:?} — KEEPING in local tracking (may retry)",
                        order_id, e
                    );
                    return false;
                }
            }
        }
        false
    }

    async fn handle_cancel_side(&mut self, side: Side, reason: CancelReason) {
        let order_ids: Vec<String> = self
            .open_orders
            .get(&side)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        if order_ids.is_empty() {
            return;
        }

        info!(
            "🗑️ Cancel {} {:?} orders (reason={:?})",
            order_ids.len(),
            side,
            reason,
        );

        for id in &order_ids {
            let _ = self.handle_cancel_order(id, reason).await;
        }
    }

    async fn handle_cancel_all(&mut self, reason: CancelReason) {
        let total: usize = self.open_orders.values().map(|v| v.len()).sum();
        info!("🗑️ CancelAll: {} orders (reason={:?})", total, reason);

        if self.cfg.dry_run || self.client.is_none() {
            info!("📝 [DRY-RUN] CancelAll");
            self.open_orders.values_mut().for_each(|v| v.clear());
            return;
        }

        let client = match &self.client {
            Some(c) => c,
            None => {
                warn!("❌ CancelAll skipped: no authenticated client");
                return;
            }
        };
        let cancel_all_result = client.cancel_all_orders().await;

        match cancel_all_result {
            Ok(_) => {
                info!("✅ All orders canceled");
                self.open_orders.values_mut().for_each(|v| v.clear());
            }
            Err(e) => {
                warn!(
                    "❌ Failed to cancel all: {:?} — fallback to per-order cancel",
                    e
                );

                let mut ids = Vec::new();
                for side_orders in self.open_orders.values() {
                    ids.extend(side_orders.keys().cloned());
                }

                for id in ids {
                    let _ = self.handle_cancel_order(&id, reason).await;
                }

                let remaining: usize = self.open_orders.values().map(|v| v.len()).sum();
                if remaining > 0 {
                    warn!(
                        "⚠️ CancelAll fallback completed with {} tracked order(s) still open",
                        remaining
                    );
                } else {
                    info!("✅ CancelAll fallback canceled all tracked orders");
                }
            }
        }
    }

    // ─────────────────────────────────────────────────
    // SDK order placement
    // ─────────────────────────────────────────────────

    async fn place_post_only_order(
        &self,
        side: Side,
        price: f64,
        size: f64,
    ) -> anyhow::Result<String> {
        use polymarket_client_sdk::clob::types::{OrderStatusType, Side as SdkSide};

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No authenticated client"))?;
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No signer"))?;

        let token_id = match side {
            Side::Yes => &self.cfg.yes_asset_id,
            Side::No => &self.cfg.no_asset_id,
        };

        let price_rounded = (price * 1000.0).round() / 1000.0;
        let size_rounded = (size * 1_000_000.0).round() / 1_000_000.0;

        let price_decimal = rust_decimal::Decimal::from_f64(price_rounded)
            .ok_or_else(|| anyhow::anyhow!("Invalid price"))?;
        let size_decimal = rust_decimal::Decimal::from_f64(size_rounded)
            .ok_or_else(|| anyhow::anyhow!("Invalid size"))?;
        let token_id_uint =
            alloy::primitives::U256::from_str_radix(token_id, 10).context("Invalid token_id")?;

        // Both YES and NO are bought via BUY side on the CLOB
        // CRITICAL: post_only(true) ensures we NEVER cross the spread.
        // Without this, if our bid price >= best ask, we'd become a taker.
        let order = client
            .limit_order()
            .token_id(token_id_uint)
            .size(size_decimal)
            .price(price_decimal)
            .side(SdkSide::Buy)
            .post_only(true)
            .build()
            .await?;

        let signed = client.sign(signer, order).await?;
        let response = client.post_order(signed).await?;

        // P1-6: Validate response — don't trust order_id blindly
        if !response.success {
            anyhow::bail!(
                "post_order rejected: status={:?} error={:?}",
                response.status,
                response.error_msg.unwrap_or_default(),
            );
        }

        if !matches!(
            response.status,
            OrderStatusType::Live | OrderStatusType::Matched
        ) {
            anyhow::bail!(
                "post_order unexpected status: {:?} error={:?}",
                response.status,
                response.error_msg.unwrap_or_default(),
            );
        }

        let order_id = response.order_id;

        // NO FillEvent here.
        // Fills come exclusively from the authenticated User WebSocket.

        Ok(order_id)
    }

    /// Get count of open orders for a side.
    pub fn open_order_count(&self, side: Side) -> usize {
        self.open_orders.get(&side).map(|m| m.len()).unwrap_or(0)
    }
}

/// Initialize the authenticated CLOB client from env settings.
pub async fn init_clob_client(
    rest_url: &str,
    private_key: Option<&str>,
) -> (
    Option<AuthClient>,
    Option<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
) {
    let pk = match private_key {
        Some(pk) if !pk.is_empty() => pk,
        _ => {
            warn!("⚠️ No private key — running in DRY-RUN mode");
            return (None, None);
        }
    };

    let signer = match std::str::FromStr::from_str(pk) {
        Ok(s) => {
            let s: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> = s;
            use alloy::signers::Signer;
            s.with_chain_id(Some(137))
        }
        Err(e) => {
            warn!("⚠️ Invalid private key: {:?}", e);
            return (None, None);
        }
    };

    let client = match ClobClient::new(rest_url, ClobConfig::default()) {
        Ok(c) => c,
        Err(e) => {
            warn!("⚠️ Failed to create CLOB client: {:?}", e);
            return (None, None);
        }
    };

    match client.authentication_builder(&signer).authenticate().await {
        Ok(auth_client) => {
            info!("✅ Polymarket CLOB client authenticated");
            (Some(auth_client), Some(signer))
        }
        Err(e) => {
            warn!("⚠️ CLOB auth failed: {:?}", e);
            (None, Some(signer))
        }
    }
}
