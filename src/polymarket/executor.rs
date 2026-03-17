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
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::messages::*;
use super::types::Side;

use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

pub type AuthClient = ClobClient<Authenticated<polymarket_client_sdk::auth::Normal>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileFetchMode {
    Market,
    AssetSplit,
    LocalById,
}

impl ReconcileFetchMode {
    fn next(self) -> Option<Self> {
        match self {
            Self::Market => Some(Self::AssetSplit),
            Self::AssetSplit => Some(Self::LocalById),
            Self::LocalById => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Market => "market",
            Self::AssetSplit => "asset-split",
            Self::LocalById => "local-order-id",
        }
    }
}

#[derive(Debug)]
enum ReconcileFetchError {
    InvalidParams(polymarket_client_sdk::error::Error),
    Failed(polymarket_client_sdk::error::Error),
}

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub rest_url: String,
    pub market_id: String,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub tick_size: f64,
    pub reconcile_interval_secs: u64,
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
    /// Side channel for capital-recycle trigger events.
    capital_tx: Option<mpsc::Sender<PlacementRejectEvent>>,

    /// Cached free collateral balance (USDC) used by pre-place affordability checks.
    balance_cache_usdc: Option<f64>,
    balance_cache_ts: Instant,
    balance_cache_ttl: Duration,

    /// Active open orders tracked per side: order_id → remaining_size.
    /// Enables partial fill tracking — only removes when fully filled.
    open_orders: HashMap<Side, HashMap<String, f64>>,

    /// Runtime-selected query mode for REST reconciliation.
    reconcile_fetch_mode: ReconcileFetchMode,
    /// Learned/configured floor for marketable BUY minimum notional (USDC).
    marketable_buy_min_notional_floor: f64,
    /// Cooldown after marketable-BUY min-notional rejection (ms).
    marketable_buy_cooldown_ms: u64,
    /// Whether to auto-learn marketable BUY min-notional from exchange errors.
    marketable_buy_autodetect: bool,
    /// Guarded on-demand reconcile throttle for side-lock refusal path.
    last_guard_reconcile_ts: Instant,
}

impl Executor {
    pub fn new(
        cfg: ExecutorConfig,
        client: Option<AuthClient>,
        signer: Option<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
        cmd_rx: mpsc::Receiver<ExecutionCmd>,
        result_tx: mpsc::Sender<OrderResult>,
        fill_rx: mpsc::Receiver<FillEvent>,
        capital_tx: Option<mpsc::Sender<PlacementRejectEvent>>,
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
            capital_tx,
            balance_cache_usdc: None,
            balance_cache_ts: Instant::now() - Duration::from_secs(60),
            balance_cache_ttl: Duration::from_millis(
                std::env::var("PM_BALANCE_CACHE_TTL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(2000),
            ),
            open_orders,
            reconcile_fetch_mode: ReconcileFetchMode::Market,
            marketable_buy_min_notional_floor: std::env::var("PM_MIN_MARKETABLE_NOTIONAL_FLOOR")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| *v >= 0.0)
                .unwrap_or(0.0),
            marketable_buy_cooldown_ms: std::env::var("PM_MIN_MARKETABLE_COOLDOWN_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(10_000),
            marketable_buy_autodetect: std::env::var("PM_MIN_MARKETABLE_AUTO_DETECT")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            last_guard_reconcile_ts: Instant::now() - Duration::from_secs(60),
        }
    }

    pub async fn run(mut self) {
        info!(
            "⚡ Executor started [MAKER-ONLY] | dry_run={} has_client={}",
            self.cfg.dry_run,
            self.client.is_some(),
        );
        let reconcile_enabled =
            self.cfg.reconcile_interval_secs > 0 && !self.cfg.dry_run && self.client.is_some();
        let mut reconcile_tick =
            tokio::time::interval(Duration::from_secs(self.cfg.reconcile_interval_secs.max(1)));

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
                        Some(ExecutionCmd::ReconcileNow { reason }) => {
                            if reconcile_enabled {
                                info!("🧭 ReconcileNow: {}", reason);
                                self.reconcile_open_orders().await;
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                // FIX #4: Fill notifications — clean up open_orders lifecycle
                fill = self.fill_rx.recv() => {
                    if let Some(fill) = fill {
                        self.handle_fill_notification(&fill).await;
                    }
                }
                _ = reconcile_tick.tick(), if reconcile_enabled => {
                    self.reconcile_open_orders().await;
                }
            }
        }

        info!("⚡ Executor shutting down");
    }

    // ─────────────────────────────────────────────────
    // Reconciliation Loop (REST)
    // ─────────────────────────────────────────────────

    async fn reconcile_open_orders(&mut self) {
        let client = match self.client.as_ref() {
            Some(c) => c,
            None => return,
        };

        let market_id = match self.cfg.market_id.parse::<alloy::primitives::B256>() {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "⚠️ Reconcile: invalid market_id '{}': {:?}",
                    self.cfg.market_id, e
                );
                return;
            }
        };

        let yes_id = match alloy::primitives::U256::from_str_radix(&self.cfg.yes_asset_id, 10) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "⚠️ Reconcile: invalid YES asset_id '{}': {:?}",
                    self.cfg.yes_asset_id, e
                );
                return;
            }
        };
        let no_id = match alloy::primitives::U256::from_str_radix(&self.cfg.no_asset_id, 10) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "⚠️ Reconcile: invalid NO asset_id '{}': {:?}",
                    self.cfg.no_asset_id, e
                );
                return;
            }
        };

        let mut mode = self.reconcile_fetch_mode;
        let mut remote_by_side = loop {
            match self
                .fetch_remote_open_orders(client, mode, market_id, yes_id, no_id)
                .await
            {
                Ok(orders) => {
                    if self.reconcile_fetch_mode != mode {
                        info!("🧭 Reconcile: query mode switched to {}", mode.as_str());
                    }
                    self.reconcile_fetch_mode = mode;
                    break orders;
                }
                Err(ReconcileFetchError::InvalidParams(e)) => {
                    let Some(next_mode) = mode.next() else {
                        warn!(
                            "⚠️ Reconcile: query mode {} rejected and no fallback left: {:?} — local state preserved",
                            mode.as_str(),
                            e
                        );
                        return;
                    };
                    warn!(
                        "⚠️ Reconcile: query mode {} rejected: {:?} — fallback to {}",
                        mode.as_str(),
                        e,
                        next_mode.as_str()
                    );
                    mode = next_mode;
                }
                Err(ReconcileFetchError::Failed(e)) => {
                    warn!(
                        "⚠️ Reconcile: query mode {} failed: {:?} — local state preserved",
                        mode.as_str(),
                        e
                    );
                    return;
                }
            }
        };

        for side in [Side::Yes, Side::No] {
            let remote = remote_by_side.remove(&side).unwrap_or_default();
            let local = self.open_orders.entry(side).or_default();

            let mut removed = Vec::new();
            for id in local.keys() {
                if !remote.contains_key(id) {
                    removed.push(id.clone());
                }
            }
            for id in removed {
                local.remove(&id);
                warn!(
                    "🧭 Reconcile: {:?} order {}… missing on exchange — releasing slot",
                    side,
                    &id[..8.min(id.len())],
                );
                let _ = self.result_tx.send(OrderResult::OrderFilled { side }).await;
            }

            for (id, rem) in remote {
                if !local.contains_key(&id) {
                    warn!(
                        "🧭 Reconcile: {:?} order {}… exists on exchange but not tracked (remaining={:.4})",
                        side,
                        &id[..8.min(id.len())],
                        rem
                    );
                }
                local.insert(id, rem);
            }
        }
    }

    async fn fetch_remote_open_orders(
        &self,
        client: &AuthClient,
        mode: ReconcileFetchMode,
        market_id: alloy::primitives::B256,
        yes_id: alloy::primitives::U256,
        no_id: alloy::primitives::U256,
    ) -> Result<HashMap<Side, HashMap<String, f64>>, ReconcileFetchError> {
        use polymarket_client_sdk::clob::types::request::OrdersRequest;

        let mut remote_by_side: HashMap<Side, HashMap<String, f64>> = HashMap::new();

        match mode {
            ReconcileFetchMode::Market => {
                let req = OrdersRequest::builder().market(market_id).build();
                let orders = self.fetch_orders_pagewise(client, &req).await?;
                for order in orders {
                    Self::insert_remote_order(&mut remote_by_side, order, market_id, yes_id, no_id);
                }
            }
            ReconcileFetchMode::AssetSplit => {
                for asset_id in [yes_id, no_id] {
                    let req = OrdersRequest::builder().asset_id(asset_id).build();
                    let orders = self.fetch_orders_pagewise(client, &req).await?;
                    for order in orders {
                        Self::insert_remote_order(
                            &mut remote_by_side,
                            order,
                            market_id,
                            yes_id,
                            no_id,
                        );
                    }
                }
            }
            ReconcileFetchMode::LocalById => {
                // Last-resort mode when `/data/orders` query shapes are rejected.
                // It keeps local lifecycle healthy but cannot discover unknown remote orders.
                let mut ids = Vec::new();
                for side in [Side::Yes, Side::No] {
                    if let Some(local) = self.open_orders.get(&side) {
                        ids.extend(local.keys().cloned());
                    }
                }

                for order_id in ids {
                    let order = match client.order(&order_id).await {
                        Ok(order) => order,
                        Err(e) => {
                            if Self::is_not_found_error(&e) {
                                continue;
                            }
                            return Err(ReconcileFetchError::Failed(e));
                        }
                    };
                    Self::insert_remote_order(&mut remote_by_side, order, market_id, yes_id, no_id);
                }
            }
        }

        Ok(remote_by_side)
    }

    async fn fetch_orders_pagewise(
        &self,
        client: &AuthClient,
        req: &polymarket_client_sdk::clob::types::request::OrdersRequest,
    ) -> Result<
        Vec<polymarket_client_sdk::clob::types::response::OpenOrderResponse>,
        ReconcileFetchError,
    > {
        let mut cursor: Option<String> = None;
        let mut orders = Vec::new();

        loop {
            let page = match client.orders(req, cursor.clone()).await {
                Ok(p) => p,
                Err(e) => {
                    if Self::is_invalid_order_params_error(&e) {
                        return Err(ReconcileFetchError::InvalidParams(e));
                    }
                    return Err(ReconcileFetchError::Failed(e));
                }
            };
            orders.extend(page.data);

            if page.next_cursor.is_empty() {
                break;
            }
            cursor = Some(page.next_cursor);
        }

        Ok(orders)
    }

    fn insert_remote_order(
        remote_by_side: &mut HashMap<Side, HashMap<String, f64>>,
        ord: polymarket_client_sdk::clob::types::response::OpenOrderResponse,
        market_id: alloy::primitives::B256,
        yes_id: alloy::primitives::U256,
        no_id: alloy::primitives::U256,
    ) {
        use polymarket_client_sdk::clob::types::OrderStatusType;

        if ord.market != market_id {
            return;
        }

        let side = if ord.asset_id == yes_id {
            Side::Yes
        } else if ord.asset_id == no_id {
            Side::No
        } else {
            return;
        };

        if matches!(ord.status, OrderStatusType::Canceled) {
            return;
        }

        let orig = ord.original_size.to_f64().unwrap_or(0.0);
        let matched = ord.size_matched.to_f64().unwrap_or(0.0);
        let remaining = (orig - matched).max(0.0);
        if remaining > 1e-9 {
            remote_by_side
                .entry(side)
                .or_default()
                .insert(ord.id.clone(), remaining);
        }
    }

    fn is_invalid_order_params_error(err: &polymarket_client_sdk::error::Error) -> bool {
        use polymarket_client_sdk::error::Status;

        err.downcast_ref::<Status>().is_some_and(|status| {
            status.status_code.as_u16() == 400
                && status.path.contains("/data/orders")
                && status.message.contains("invalid order params payload")
        })
    }

    fn is_not_found_error(err: &polymarket_client_sdk::error::Error) -> bool {
        use polymarket_client_sdk::error::Status;

        err.downcast_ref::<Status>()
            .is_some_and(|status| status.status_code.as_u16() == 404)
    }

    // ─────────────────────────────────────────────────
    // Fill Notifications (from User WS → clean up open_orders)
    // ─────────────────────────────────────────────────

    /// Handle fill notifications from User WS.
    /// MATCHED: decrement remaining_size, remove when fully filled.
    /// FAILED: order is dead/reverted — remove entirely.
    /// AUDIT FIX: Sends OrderFilled back to Coordinator so it can release the slot.
    async fn handle_fill_notification(&mut self, fill: &FillEvent) {
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
                // Notify Coordinator: slot is now free
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderFilled { side: fill.side })
                    .await;
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
                // AUDIT FIX: Notify Coordinator that the slot is free for new orders
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderFilled { side: fill.side })
                    .await;
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
        if let Some(existing_count) = self
            .open_orders
            .get(&side)
            .map(|existing| existing.len())
            .filter(|count| *count > 0)
        {
            warn!(
                "🚫 Refusing PlacePostOnlyBid {:?}@{:.3}: {} tracked order(s) still open on side",
                side, price, existing_count,
            );
            if self.last_guard_reconcile_ts.elapsed() >= Duration::from_millis(1500)
                && !self.cfg.dry_run
                && self.client.is_some()
            {
                self.last_guard_reconcile_ts = Instant::now();
                self.reconcile_open_orders().await;
            }
            let _ = self
                .result_tx
                .send(OrderResult::OrderFailed {
                    side,
                    // Short backoff to avoid tight retry loops when OMS and
                    // executor are temporarily out-of-sync on live orders.
                    cooldown_ms: 2_000,
                })
                .await;
            return;
        }

        let notional = (price * size).max(0.0);
        if self.marketable_buy_min_notional_floor > 0.0
            && notional + 1e-9 < self.marketable_buy_min_notional_floor
        {
            warn!(
                "🚫 Precheck reject {:?}@{:.3} sz={:.2}: notional ${:.2} < marketable min ${:.2}",
                side, price, size, notional, self.marketable_buy_min_notional_floor
            );
            let _ = self
                .emit_reject_event(PlacementRejectEvent {
                    side,
                    reason,
                    kind: RejectKind::Validation,
                    price,
                    size,
                    ts: Instant::now(),
                })
                .await;
            let _ = self
                .result_tx
                .send(OrderResult::OrderFailed {
                    side,
                    cooldown_ms: self.marketable_buy_cooldown_ms,
                })
                .await;
            return;
        }

        // Affordability guard: avoid futile submits when free collateral is clearly insufficient.
        // This reduces exchange-side hard rejects and cancel/place storms during low-balance windows.
        if let Some(free_usdc) = self.cached_free_balance_usdc().await {
            let required_usdc = (price * size).max(0.0);
            // Keep a small cushion for transient balance/allowance lag.
            if free_usdc + 0.05 < required_usdc {
                warn!(
                    "🚫 Precheck reject {:?}@{:.3} sz={:.1}: need {:.2} USDC > free {:.2} USDC",
                    side, price, size, required_usdc, free_usdc
                );
                let _ = self
                    .emit_reject_event(PlacementRejectEvent {
                        side,
                        reason,
                        kind: RejectKind::BalanceOrAllowance,
                        price,
                        size,
                        ts: Instant::now(),
                    })
                    .await;
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderFailed {
                        side,
                        cooldown_ms: 15_000,
                    })
                    .await;
                return;
            }
        }

        match self.place_post_only_order(side, price, size).await {
            Ok(order_id) => {
                info!("✅ Order placed: {:?}@{:.3} id={}", side, price, order_id);
                if let Some(orders) = self.open_orders.get_mut(&side) {
                    orders.insert(order_id, size);
                }
                // Notify OrderManager that state can transition to Live
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderPlaced {
                        side,
                        target: DesiredTarget {
                            side,
                            price,
                            size,
                            reason,
                        },
                    })
                    .await;
                // NO FillEvent here. Fills come from User WS only.
            }
            Err(e) => {
                let mut final_err = e;

                // Runtime self-heal: if venue rejects due tick precision, learn min tick and retry once.
                if let Some(min_tick) = Self::extract_min_tick_size(&format!("{:#}", final_err)) {
                    if (0.0..1.0).contains(&min_tick)
                        && (min_tick - self.cfg.tick_size).abs() > f64::EPSILON
                    {
                        warn!(
                            "🔧 Venue min tick learned: {:.6} -> {:.6}; retrying once",
                            self.cfg.tick_size, min_tick
                        );
                        self.cfg.tick_size = min_tick;
                    }

                    match self.place_post_only_order(side, price, size).await {
                        Ok(order_id) => {
                            info!(
                                "✅ Order placed after tick-size retry: {:?}@{:.3} id={}",
                                side, price, order_id
                            );
                            if let Some(orders) = self.open_orders.get_mut(&side) {
                                orders.insert(order_id, size);
                            }
                            let _ = self
                                .result_tx
                                .send(OrderResult::OrderPlaced {
                                    side,
                                    target: DesiredTarget {
                                        side,
                                        price,
                                        size,
                                        reason,
                                    },
                                })
                                .await;
                            return;
                        }
                        Err(retry_err) => {
                            final_err = retry_err;
                        }
                    }
                }

                warn!("❌ Failed to place PostOnlyBid {:?}: {:?}", side, final_err);
                let err_text = format!("{:#}", final_err);
                if self.marketable_buy_autodetect {
                    if let Some(min_notional) = Self::extract_min_marketable_notional(&err_text) {
                        if min_notional > self.marketable_buy_min_notional_floor + 1e-9 {
                            warn!(
                                "🧭 Learned marketable BUY min notional floor: ${:.2} -> ${:.2}",
                                self.marketable_buy_min_notional_floor, min_notional
                            );
                            self.marketable_buy_min_notional_floor = min_notional;
                        }
                    }
                }
                let is_429 = Self::is_rate_limit_error(&final_err);
                let is_balance = Self::is_balance_or_allowance_error(&final_err);
                let is_marketable_min = Self::is_marketable_min_error(&final_err);
                let is_validation = Self::is_validation_error(&final_err);
                let reject_kind = if is_429 {
                    RejectKind::RateLimit
                } else if is_balance {
                    RejectKind::BalanceOrAllowance
                } else if is_validation {
                    RejectKind::Validation
                } else {
                    RejectKind::Other
                };

                let cooldown_ms = if is_429 {
                    10_000 // 10s backoff for rate limiting
                } else if is_balance {
                    30_000 // 30s for balance issues
                } else if is_marketable_min {
                    self.marketable_buy_cooldown_ms // min-$1 style hard floor reject
                } else if is_validation {
                    5_000 // 5s for validation errors (size precision, lot size, etc.)
                } else {
                    0
                };

                if cooldown_ms > 0 {
                    let reject_kind_text = if is_429 {
                        "rate limit"
                    } else if is_balance {
                        "balance/allowance"
                    } else {
                        "validation/precision"
                    };
                    warn!(
                        "⛔ Hard reject on {:?}: pausing new placements for {}s ({})",
                        side,
                        cooldown_ms / 1000,
                        reject_kind_text
                    );
                    if is_balance && matches!(reason, BidReason::Hedge) {
                        warn!(
                            "🧯 Hedge blocked on {:?}: insufficient balance/allowance; inventory may remain directional until cooldown expires",
                            side
                        );
                    }
                }
                let _ = self
                    .emit_reject_event(PlacementRejectEvent {
                        side,
                        reason,
                        kind: reject_kind,
                        price,
                        size,
                        ts: Instant::now(),
                    })
                    .await;
                // FIX #4: Notify Coordinator the order failed so it can reset the slot
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderFailed { side, cooldown_ms })
                    .await;
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
            // No tracked orders — still send CancelAck to unblock OMS state machine.
            let _ = self.result_tx.send(OrderResult::CancelAck { side }).await;
            return;
        }

        info!(
            "🗑️ Cancel {} {:?} orders (reason={:?})",
            order_ids.len(),
            side,
            reason,
        );

        // ISSUE 6 FIX: Track failures. On partial failure the failed orders remain
        // in open_orders (handle_cancel_order keeps them), which prevents Executor
        // from placing a replacement until they are resolved. CancelAck is always
        // sent to unblock OMS — the open_orders guard in handle_place_bid acts as
        // the safety net against double-placement on stuck orders.
        let mut failed_count = 0usize;
        for id in &order_ids {
            if !self.handle_cancel_order(id, reason).await {
                failed_count += 1;
            }
        }

        if failed_count > 0 {
            warn!(
                "⚠️ CancelSide {:?}: {}/{} cancel(s) failed — those orders remain tracked in open_orders (placement guard active)",
                side, failed_count, order_ids.len(),
            );
        }

        let _ = self.result_tx.send(OrderResult::CancelAck { side }).await;
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

    async fn emit_reject_event(&self, evt: PlacementRejectEvent) {
        if let Some(tx) = &self.capital_tx {
            let _ = tx.send(evt).await;
        }
    }

    async fn cached_free_balance_usdc(&mut self) -> Option<f64> {
        if self.cfg.dry_run || self.client.is_none() {
            return None;
        }
        if self.balance_cache_usdc.is_some()
            && self.balance_cache_ts.elapsed() < self.balance_cache_ttl
        {
            return self.balance_cache_usdc;
        }

        use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk::clob::types::AssetType;
        use rust_decimal::prelude::ToPrimitive;

        let client = self.client.as_ref()?;
        let req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        match client.balance_allowance(req).await {
            Ok(resp) => {
                // Polymarket returns collateral in 6 decimals (1 USDC = 1_000_000).
                let raw = resp.balance.to_f64().unwrap_or(0.0);
                let free = (raw / 1_000_000.0).max(0.0);
                self.balance_cache_usdc = Some(free);
                self.balance_cache_ts = Instant::now();
                Some(free)
            }
            Err(e) => {
                warn!("⚠️ balance precheck fetch failed: {:?}", e);
                self.balance_cache_usdc = None;
                self.balance_cache_ts = Instant::now();
                None
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

        if !(0.0..1.0).contains(&self.cfg.tick_size) {
            anyhow::bail!("invalid tick_size={}", self.cfg.tick_size);
        }
        let inv_tick = (1.0 / self.cfg.tick_size).round();
        if !inv_tick.is_finite() || inv_tick <= 0.0 {
            anyhow::bail!("invalid tick_size reciprocal={}", inv_tick);
        }
        // BUY maker bids must not round up to a more aggressive price.
        let price_rounded = (price * inv_tick).floor() / inv_tick;
        // P0 FIX: CLOB max lot size = 2 decimal places. Truncate DOWN to avoid over-sizing.
        let size_rounded = (size * 100.0).floor() / 100.0;
        if size_rounded < 0.01 {
            anyhow::bail!("size {:.6} rounds to 0 at 2dp — skipping", size);
        }

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

    fn extract_min_tick_size(err: &str) -> Option<f64> {
        let lower = err.to_ascii_lowercase();
        let marker = "minimum tick size ";
        let idx = lower.find(marker)?;
        let tail = &lower[idx + marker.len()..];
        let num: String = tail
            .chars()
            .take_while(|ch| ch.is_ascii_digit() || *ch == '.')
            .collect();
        if num.is_empty() {
            return None;
        }
        num.parse::<f64>().ok().filter(|v| *v > 0.0)
    }

    fn extract_min_marketable_notional(err: &str) -> Option<f64> {
        let lower = err.to_ascii_lowercase();
        let marker = "min size: $";
        let idx = lower.find(marker)?;
        let tail = &lower[idx + marker.len()..];
        let num: String = tail
            .chars()
            .take_while(|ch| ch.is_ascii_digit() || *ch == '.')
            .collect();
        if num.is_empty() {
            return None;
        }
        num.parse::<f64>().ok().filter(|v| *v > 0.0)
    }

    fn is_balance_or_allowance_error(err: &anyhow::Error) -> bool {
        let lower = format!("{:#}", err).to_ascii_lowercase();
        lower.contains("not enough balance") || lower.contains("allowance")
    }

    fn is_rate_limit_error(err: &anyhow::Error) -> bool {
        let lower = format!("{:#}", err).to_ascii_lowercase();
        lower.contains("429") || lower.contains("too many requests") || lower.contains("rate limit")
    }

    /// P2 FIX: Detect validation/precision errors to apply cooldown and prevent retry storms.
    fn is_validation_error(err: &anyhow::Error) -> bool {
        let lower = format!("{:#}", err).to_ascii_lowercase();
        lower.contains("decimal places")
            || lower.contains("lot size")
            || lower.contains("order crosses book")
            || lower.contains("rounds to 0")
            || Self::is_marketable_min_error(err)
    }

    fn is_marketable_min_error(err: &anyhow::Error) -> bool {
        let lower = format!("{:#}", err).to_ascii_lowercase();
        lower.contains("invalid amount for a marketable buy order") || lower.contains("min size: $")
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
    funder_address: Option<alloy::primitives::Address>,
    api_credentials: Option<polymarket_client_sdk::auth::Credentials>,
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

    #[allow(unused_imports)]
    use alloy::signers::Signer;
    let signer_addr = signer.address();
    let derived_proxy = polymarket_client_sdk::derive_proxy_wallet(signer_addr, 137);
    let derived_safe = polymarket_client_sdk::derive_safe_wallet(signer_addr, 137);
    let fmt_addr = |a: alloy::primitives::Address| -> String {
        let s = format!("{a:?}");
        if s.len() > 10 {
            format!("{}…{}", &s[..6], &s[s.len() - 4..])
        } else {
            s
        }
    };
    let s_signer = fmt_addr(signer_addr);
    let s_funder = funder_address.map_or_else(|| "None".to_string(), fmt_addr);
    let s_proxy = derived_proxy.map_or_else(|| "None".to_string(), fmt_addr);
    let s_safe = derived_safe.map_or_else(|| "None".to_string(), fmt_addr);
    info!(
        "🔍 Auth identity | signer={} configured_funder={} derived_proxy={} derived_safe={}",
        s_signer, s_funder, s_proxy, s_safe,
    );

    let explicit_api_creds = api_credentials.clone();
    use polymarket_client_sdk::clob::types::SignatureType;

    let default_sig_type = if funder_address == derived_safe {
        SignatureType::GnosisSafe
    } else if funder_address == Some(signer_addr) {
        SignatureType::Eoa
    } else {
        SignatureType::Proxy
    };

    let signature_type = match std::env::var("PM_SIGNATURE_TYPE")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
    {
        Some(2) => SignatureType::GnosisSafe,
        Some(1) => SignatureType::Proxy,
        Some(0) => SignatureType::Eoa,
        _ => default_sig_type,
    };

    if funder_address.is_some() {
        info!(
            "🔐 Auth mode | signature_type={} (0=EOA,1=Proxy,2=GnosisSafe)",
            signature_type as u8
        );
    }

    let authenticate_attempt = |creds: Option<polymarket_client_sdk::auth::Credentials>| async {
        let client = ClobClient::new(rest_url, ClobConfig::default())?;
        let mut builder = client.authentication_builder(&signer);
        if let Some(creds) = creds {
            builder = builder.credentials(creds);
        }
        if let Some(funder) = funder_address {
            builder = builder.funder(funder).signature_type(signature_type);
        }
        builder.authenticate().await
    };

    let is_invalid_api_key_err = |err: &dyn std::fmt::Display| -> bool {
        let lower = err.to_string().to_ascii_lowercase();
        lower.contains("invalid api key") || lower.contains("unauthorized") || lower.contains("401")
    };

    let initial = if let Some(creds) = api_credentials {
        info!("🔑 Auth mode | using explicit POLYMARKET_API_* credentials from environment");
        authenticate_attempt(Some(creds)).await
    } else {
        authenticate_attempt(None).await
    };

    match initial {
        Ok(auth_client) => {
            // If env API credentials were forced, verify they are truly usable.
            // Some stale creds can pass construction but fail on first authenticated call.
            if explicit_api_creds.is_some() {
                match auth_client.ok().await {
                    Ok(_) => {
                        info!("✅ Polymarket CLOB client authenticated");
                        return (Some(auth_client), Some(signer));
                    }
                    Err(e) if is_invalid_api_key_err(&e) => {
                        warn!(
                            "⚠️ Explicit POLYMARKET_API_* appears invalid ({}). \
                             Falling back to L1 create/derive flow.",
                            e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "⚠️ Auth probe failed with explicit POLYMARKET_API_*: {:?}. \
                             Falling back to L1 create/derive flow.",
                            e
                        );
                    }
                }

                match authenticate_attempt(None).await {
                    Ok(fallback_client) => {
                        info!(
                            "✅ Polymarket CLOB client authenticated via L1 fallback (auto-derived API creds)"
                        );
                        (Some(fallback_client), Some(signer))
                    }
                    Err(fallback_err) => {
                        warn!(
                            "⚠️ CLOB auth fallback failed after invalid explicit POLYMARKET_API_*: {:?}",
                            fallback_err
                        );
                        (None, Some(signer))
                    }
                }
            } else {
                info!("✅ Polymarket CLOB client authenticated");
                (Some(auth_client), Some(signer))
            }
        }
        Err(e) => {
            if explicit_api_creds.is_some() && is_invalid_api_key_err(&e) {
                warn!(
                    "⚠️ Explicit POLYMARKET_API_* rejected ({}). \
                     Retrying with L1 create/derive flow.",
                    e
                );
                match authenticate_attempt(None).await {
                    Ok(fallback_client) => {
                        info!(
                            "✅ Polymarket CLOB client authenticated via L1 fallback (auto-derived API creds)"
                        );
                        (Some(fallback_client), Some(signer))
                    }
                    Err(fallback_err) => {
                        warn!("⚠️ CLOB auth fallback failed: {:?}", fallback_err);
                        (None, Some(signer))
                    }
                }
            } else {
                warn!("⚠️ CLOB auth failed: {:?}", e);
                (None, Some(signer))
            }
        }
    }
}
