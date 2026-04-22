//! Executor Actor — order management.
//!
//! Tracks active open orders per logical order slot.
//! Default path is Post-Only maker bids, with a dedicated one-shot taker hedge
//! path for tail-risk de-risking.
//!
//! CRITICAL: The Executor NEVER emits FillEvents.
//! Fills come exclusively from the authenticated User WebSocket.
//!
//! On order placement failure, sends OrderResult::OrderFailed to OMS
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
const CROSS_BOOK_COOLDOWN_MS: u64 = 1_000;
const DUST_REMAINING_SHARES: f64 = 0.10;
const SLOT_BLOCKED_FEEDBACK_INTERVAL_MS: u64 = 1_000;
const SLOT_LOCK_RECOVERY_MIN_BLOCK_MS: u64 = 8_000;
const SLOT_LOCK_RECOVERY_RETRY_MS: u64 = 5_000;

#[allow(dead_code)]
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
    /// Side channel for Coordinator execution feedback (e.g. crossed-book reject adaptation).
    feedback_tx: Option<mpsc::Sender<ExecutionFeedback>>,

    /// Cached free collateral balance (USDC) used by pre-place affordability checks.
    balance_cache_usdc: Option<f64>,
    balance_cache_ts: Instant,
    balance_cache_ttl: Duration,

    /// Active open orders tracked per slot: order_id → remaining_size.
    /// Enables partial fill tracking — only removes when fully filled.
    open_orders: [HashMap<String, f64>; 4],
    /// Recent same-side buy fills used to identify transient sell availability lag.
    last_buy_fill_ts: [Option<Instant>; 2],
    /// Recent same-side buy placements, used as a fallback when fill events lag behind submit ACKs.
    last_buy_place_ts: [Option<Instant>; 2],

    /// Runtime-selected query mode for REST reconciliation.
    reconcile_fetch_mode: ReconcileFetchMode,
    /// Learned/configured floor for marketable BUY minimum notional (USDC).
    marketable_buy_min_notional_floor: f64,
    /// Cooldown after marketable-BUY min-notional rejection (ms).
    marketable_buy_cooldown_ms: u64,
    /// Whether to auto-learn marketable BUY min-notional from exchange errors.
    marketable_buy_autodetect: bool,
    /// Fallback lot size for small BUY+Provide retries (read from PM_BID_SIZE).
    provide_size_fallback_bid_size: f64,
    /// Guarded on-demand reconcile throttle for side-lock refusal path.
    last_guard_reconcile_ts: Instant,
    /// Continuous slot-lock window started when place attempts are refused due to tracked live orders.
    slot_blocked_since: [Option<Instant>; 4],
    /// Last time slot-lock feedback was emitted to coordinator (per-slot rate limit).
    slot_last_blocked_feedback: [Option<Instant>; 4],
    /// Last time we attempted a forced cancel recovery for a blocked slot.
    slot_last_forced_cancel_attempt: [Instant; 4],
}

impl Executor {
    fn is_residual_dust(remaining: f64) -> bool {
        remaining <= DUST_REMAINING_SHARES + 1e-9
    }

    fn prune_slot_dust_locally(&mut self, slot: OrderSlot) -> usize {
        let local = self.slot_orders_mut(slot);
        let before = local.len();
        local.retain(|_, remaining| !Self::is_residual_dust(*remaining));
        before.saturating_sub(local.len())
    }

    fn slot_blocked_duration(&mut self, slot: OrderSlot, now: Instant) -> Duration {
        let idx = slot.index();
        let since = self.slot_blocked_since[idx].get_or_insert(now);
        now.saturating_duration_since(*since)
    }

    fn reset_slot_blocked_state(&mut self, slot: OrderSlot) {
        let idx = slot.index();
        self.slot_blocked_since[idx] = None;
        self.slot_last_blocked_feedback[idx] = None;
    }

    async fn emit_slot_blocked_feedback(
        &mut self,
        slot: OrderSlot,
        tracked_orders: usize,
        blocked_for: Duration,
        now: Instant,
    ) {
        let idx = slot.index();
        let can_emit = self.slot_last_blocked_feedback[idx]
            .map(|ts| ts.elapsed() >= Duration::from_millis(SLOT_BLOCKED_FEEDBACK_INTERVAL_MS))
            .unwrap_or(true);
        if !can_emit {
            return;
        }
        self.slot_last_blocked_feedback[idx] = Some(now);
        self.emit_execution_feedback(ExecutionFeedback::SlotBlocked {
            slot,
            tracked_orders,
            blocked_for_ms: blocked_for.as_millis().min(u128::from(u64::MAX)) as u64,
            ts: now,
        })
        .await;
    }

    pub fn new(
        cfg: ExecutorConfig,
        client: Option<AuthClient>,
        signer: Option<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
        cmd_rx: mpsc::Receiver<ExecutionCmd>,
        result_tx: mpsc::Sender<OrderResult>,
        fill_rx: mpsc::Receiver<FillEvent>,
        capital_tx: Option<mpsc::Sender<PlacementRejectEvent>>,
        feedback_tx: Option<mpsc::Sender<ExecutionFeedback>>,
    ) -> Self {
        Self {
            cfg,
            client,
            signer,
            cmd_rx,
            result_tx,
            fill_rx,
            capital_tx,
            feedback_tx,
            balance_cache_usdc: None,
            balance_cache_ts: Instant::now() - Duration::from_secs(60),
            balance_cache_ttl: Duration::from_millis(
                std::env::var("PM_BALANCE_CACHE_TTL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(2000),
            ),
            open_orders: std::array::from_fn(|_| HashMap::new()),
            last_buy_fill_ts: [None, None],
            last_buy_place_ts: [None, None],
            slot_blocked_since: [None; 4],
            slot_last_blocked_feedback: [None; 4],
            slot_last_forced_cancel_attempt: std::array::from_fn(|_| {
                Instant::now() - Duration::from_secs(60)
            }),
            reconcile_fetch_mode: ReconcileFetchMode::LocalById,
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
            provide_size_fallback_bid_size: std::env::var("PM_BID_SIZE")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| *v > 0.0)
                .unwrap_or(5.0),
            last_guard_reconcile_ts: Instant::now() - Duration::from_secs(60),
        }
    }

    pub async fn run(mut self) {
        info!(
            "⚡ Executor started | dry_run={} has_client={}",
            self.cfg.dry_run,
            self.client.is_some(),
        );
        info!(
            "🧭 Reconcile mode: {} (startup CancelAll authoritative)",
            self.reconcile_fetch_mode.as_str()
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
                        Some(ExecutionCmd::ExecuteIntent { intent }) => {
                            self.handle_execute_intent(intent).await;
                        }
                        Some(ExecutionCmd::PlacePostOnlyBid { side, direction, price, size, reason }) => {
                            let purpose = match reason {
                                BidReason::Provide => TradePurpose::Provide,
                                BidReason::OracleLagProvide => TradePurpose::OracleLagSnipe,
                                BidReason::Hedge => TradePurpose::Hedge,
                            };
                            self.handle_place_bid(
                                side,
                                direction,
                                price,
                                size,
                                reason,
                                purpose,
                                0.0,
                            )
                            .await;
                        }
                        Some(ExecutionCmd::PlaceTakerHedge { side, direction, size }) => {
                            self.handle_place_taker(side, direction, size, TradePurpose::Hedge, None).await;
                        }
                        Some(ExecutionCmd::CancelOrder { order_id, reason }) => {
                            let _ = self.handle_cancel_order(&order_id, reason).await;
                        }
                        Some(ExecutionCmd::CancelSlot { slot, reason }) => {
                            self.handle_cancel_slot(slot, reason).await;
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

    fn slot_orders(&self, slot: OrderSlot) -> &HashMap<String, f64> {
        &self.open_orders[slot.index()]
    }

    fn slot_orders_mut(&mut self, slot: OrderSlot) -> &mut HashMap<String, f64> {
        &mut self.open_orders[slot.index()]
    }

    async fn cancel_remote_dust_orders_for_slot(
        &mut self,
        slot: OrderSlot,
        remote: &mut HashMap<String, f64>,
    ) {
        let dust_orders: Vec<(String, f64)> = remote
            .iter()
            .filter_map(|(id, remaining)| {
                if Self::is_residual_dust(*remaining) {
                    Some((id.clone(), *remaining))
                } else {
                    None
                }
            })
            .collect();
        if dust_orders.is_empty() {
            return;
        }

        let mut canceled_dust_ids: Vec<String> = Vec::new();
        for (id, remaining) in &dust_orders {
            warn!(
                "🧹 Reconcile: {} order {}… residual {:.4} <= dust floor {:.2}; canceling and ignoring for slot lock",
                slot.as_str(),
                &id[..8.min(id.len())],
                remaining,
                DUST_REMAINING_SHARES
            );
            let canceled = self.handle_cancel_order(id, CancelReason::Reprice).await;
            if !canceled {
                warn!(
                    "⚠️ Reconcile: dust cancel failed for {} order {}…; slot may remain guarded until next reconcile",
                    slot.as_str(),
                    &id[..8.min(id.len())],
                );
            } else {
                canceled_dust_ids.push(id.clone());
            }
        }

        for id in canceled_dust_ids {
            remote.remove(&id);
        }
    }

    fn side_order_count(&self, side: Side) -> usize {
        OrderSlot::side_slots(side)
            .into_iter()
            .map(|slot| self.slot_orders(slot).len())
            .sum()
    }

    // ─────────────────────────────────────────────────
    // Reconciliation Loop (REST)
    // ─────────────────────────────────────────────────

    async fn reconcile_open_orders(&mut self) {
        if self.client.is_none() {
            return;
        }

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
        let mut remote_by_slot = loop {
            match self
                .fetch_remote_open_orders(
                    self.client.as_ref().expect("client checked above"),
                    mode,
                    market_id,
                    yes_id,
                    no_id,
                )
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

        for slot in OrderSlot::ALL {
            if let Some(remote) = remote_by_slot.get_mut(&slot) {
                self.cancel_remote_dust_orders_for_slot(slot, remote).await;
            }

            let remote = remote_by_slot.remove(&slot).unwrap_or_default();
            let mut removed: Vec<String> = Vec::new();
            let mut newly_seen: Vec<(String, f64)> = Vec::new();
            {
                let local = self.slot_orders_mut(slot);
                for id in local.keys() {
                    if !remote.contains_key(id) {
                        removed.push(id.clone());
                    }
                }
                for id in &removed {
                    local.remove(id);
                }

                for (id, rem) in remote {
                    if !local.contains_key(&id) {
                        newly_seen.push((id.clone(), rem));
                    }
                    local.insert(id, rem);
                }
            }

            for id in removed {
                warn!(
                    "🧭 Reconcile: {} order {}… missing on exchange — releasing slot",
                    slot.as_str(),
                    &id[..8.min(id.len())],
                );
                let _ = self.result_tx.send(OrderResult::OrderFilled { slot }).await;
            }

            for (id, rem) in newly_seen {
                warn!(
                    "🧭 Reconcile: {} order {}… exists on exchange but not tracked (remaining={:.4})",
                    slot.as_str(),
                    &id[..8.min(id.len())],
                    rem
                );
            }
            if self.slot_orders(slot).is_empty() {
                self.reset_slot_blocked_state(slot);
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
    ) -> Result<HashMap<OrderSlot, HashMap<String, f64>>, ReconcileFetchError> {
        use polymarket_client_sdk::clob::types::request::OrdersRequest;

        let mut remote_by_slot: HashMap<OrderSlot, HashMap<String, f64>> = HashMap::new();

        match mode {
            ReconcileFetchMode::Market => {
                let req = OrdersRequest::builder().market(market_id).build();
                let orders = self.fetch_orders_pagewise(client, &req).await?;
                for order in orders {
                    Self::insert_remote_order(&mut remote_by_slot, order, market_id, yes_id, no_id);
                }
            }
            ReconcileFetchMode::AssetSplit => {
                for asset_id in [yes_id, no_id] {
                    let req = OrdersRequest::builder().asset_id(asset_id).build();
                    let orders = self.fetch_orders_pagewise(client, &req).await?;
                    for order in orders {
                        Self::insert_remote_order(
                            &mut remote_by_slot,
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
                let mut ids: Vec<String> = Vec::new();
                for slot in OrderSlot::ALL {
                    ids.extend(self.slot_orders(slot).keys().cloned());
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
                    Self::insert_remote_order(&mut remote_by_slot, order, market_id, yes_id, no_id);
                }
            }
        }

        Ok(remote_by_slot)
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
        remote_by_slot: &mut HashMap<OrderSlot, HashMap<String, f64>>,
        ord: polymarket_client_sdk::clob::types::response::OpenOrderResponse,
        market_id: alloy::primitives::B256,
        yes_id: alloy::primitives::U256,
        no_id: alloy::primitives::U256,
    ) {
        use polymarket_client_sdk::clob::types::{OrderStatusType, Side as SdkSide};

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
        let direction = match ord.side {
            SdkSide::Buy => TradeDirection::Buy,
            SdkSide::Sell => TradeDirection::Sell,
            _ => return,
        };
        let slot = OrderSlot::new(side, direction);

        if matches!(ord.status, OrderStatusType::Canceled) {
            return;
        }

        let orig = ord.original_size.to_f64().unwrap_or(0.0);
        let matched = ord.size_matched.to_f64().unwrap_or(0.0);
        let remaining = (orig - matched).max(0.0);
        if !Self::is_residual_dust(remaining) {
            remote_by_slot
                .entry(slot)
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
        let slot = fill.slot();
        if fill.status != FillStatus::Failed && slot.direction == TradeDirection::Buy {
            self.last_buy_fill_ts[slot.side.index()] = Some(Instant::now());
        }
        // P1-3: FAILED = order terminated, remove entirely
        if fill.status == FillStatus::Failed {
            let orders = self.slot_orders_mut(slot);
            if orders.remove(&fill.order_id).is_some() {
                warn!(
                    "📋 Lifecycle: {} order {}… FAILED — removed from tracking ({} remaining)",
                    slot.as_str(),
                    &fill.order_id[..8.min(fill.order_id.len())],
                    orders.len(),
                );
                if orders.is_empty() {
                    self.reset_slot_blocked_state(slot);
                }
                // Notify Coordinator: slot is now free
                let _ = self.result_tx.send(OrderResult::OrderFilled { slot }).await;
            }
            return;
        }

        // MATCHED path: decrement remaining size
        let orders = self.slot_orders_mut(slot);
        if let Some(remaining) = orders.get_mut(&fill.order_id) {
            *remaining -= fill.filled_size;
            if *remaining <= 0.0 || Self::is_residual_dust(*remaining) {
                orders.remove(&fill.order_id);
                info!(
                    "📋 Lifecycle: {} order {}… fully filled/dust-closed — removed ({} remaining on slot)",
                    slot.as_str(),
                    &fill.order_id[..8.min(fill.order_id.len())],
                    orders.len(),
                );
                if orders.is_empty() {
                    self.reset_slot_blocked_state(slot);
                }
                // AUDIT FIX: Notify Coordinator that the slot is free for new orders
                let _ = self.result_tx.send(OrderResult::OrderFilled { slot }).await;
            } else {
                info!(
                    "📋 Lifecycle: {} order {}… partial fill {:.2}, remaining={:.2}",
                    slot.as_str(),
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

    async fn handle_execute_intent(&mut self, intent: TradeIntent) {
        let slot = OrderSlot::new(intent.side, intent.direction);
        match intent.urgency {
            TradeUrgency::MakerPostOnly => {
                let Some(price) = intent.price else {
                    warn!(
                        "🚫 ExecuteIntent rejected: missing price for MakerPostOnly {:?} {:?}",
                        intent.side, intent.direction
                    );
                    let _ = self
                        .result_tx
                        .send(OrderResult::OrderFailed {
                            slot,
                            cooldown_ms: 0,
                        })
                        .await;
                    return;
                };
                self.handle_place_bid(
                    intent.side,
                    intent.direction,
                    price,
                    intent.size,
                    intent.purpose.as_bid_reason(),
                    intent.purpose,
                    intent.local_unreleased_matched_notional_usdc,
                )
                .await;
            }
            TradeUrgency::TakerFak => {
                self.handle_place_taker(
                    intent.side,
                    intent.direction,
                    intent.size,
                    intent.purpose,
                    intent.price,
                )
                .await;
            }
        }
    }

    async fn handle_place_bid(
        &mut self,
        side: Side,
        direction: TradeDirection,
        price: f64,
        size: f64,
        reason: BidReason,
        purpose: TradePurpose,
        local_unreleased_matched_notional_usdc: f64,
    ) {
        let mut size = size;
        let slot = OrderSlot::new(side, direction);
        let reason_str = match reason {
            BidReason::Provide => "PROVIDE",
            BidReason::OracleLagProvide => "ORACLE_LAG_PROVIDE",
            BidReason::Hedge => "HEDGE",
        };

        // Oracle-lag single-shot all-in sizing for maker fallback path.
        // Keep this explicitly scoped by purpose to avoid affecting other strategies.
        if direction == TradeDirection::Buy
            && purpose == TradePurpose::OracleLagSnipe
            && price > 0.0
        {
            if let Some(free_usdc) = self.cached_free_balance_usdc().await {
                let cushion = 0.05_f64;
                let spendable = (free_usdc - cushion).max(0.0);
                let affordable_shares = (spendable / price).max(0.0);
                let affordable_2dp = (affordable_shares * 100.0).floor() / 100.0;
                let chosen = (affordable_2dp * 100.0).floor() / 100.0;
                info!(
                    "📐 oracle_lag_order_size | mode=maker_fallback side={:?} requested={:.2} chosen={:.2} free_usdc={:.2} spendable_usdc={:.2} price={:.4}",
                    side, size, chosen, free_usdc, spendable, price
                );
                size = chosen;
            }
        }

        info!(
            "📤 {} PostOnly {:?} {:?}@{:.3} size={:.1}",
            reason_str, direction, side, price, size,
        );

        if self.cfg.dry_run || self.client.is_none() {
            info!(
                "📝 [DRY-RUN] PostOnly {:?} {:?}@{:.3} size={:.1}",
                direction, side, price, size,
            );
            // DRY-RUN: track fake order, but NO FillEvent.
            // net_diff stays 0 → always Balanced. Correct for paper trading.
            let fake_id = format!(
                "dry-{:?}-{:?}-{}",
                direction,
                side,
                Instant::now().elapsed().as_nanos()
            );
            self.slot_orders_mut(slot).insert(fake_id, size);
            return;
        }

        let dust_pruned = self.prune_slot_dust_locally(slot);
        if dust_pruned > 0 {
            warn!(
                "🧹 Pre-place guard: pruned {} dust-tracked order(s) on {} before placement",
                dust_pruned,
                slot.as_str(),
            );
            let _ = self.result_tx.send(OrderResult::OrderFilled { slot }).await;
        }

        // Slot-keyed maker model: only this exact slot is mutually exclusive.
        let existing_count = self.slot_orders(slot).len();
        if existing_count > 0 {
            let now = Instant::now();
            let blocked_for = self.slot_blocked_duration(slot, now);
            self.emit_slot_blocked_feedback(slot, existing_count, blocked_for, now)
                .await;
            warn!(
                "🚫 Refusing PlacePostOnlyBid {}@{:.3}: {} tracked order(s) still open on slot",
                slot.as_str(),
                price,
                existing_count,
            );
            if self.last_guard_reconcile_ts.elapsed() >= Duration::from_millis(1500)
                && !self.cfg.dry_run
                && self.client.is_some()
            {
                self.last_guard_reconcile_ts = Instant::now();
                self.reconcile_open_orders().await;
            }
            let idx = slot.index();
            if !self.cfg.dry_run
                && self.client.is_some()
                && blocked_for >= Duration::from_millis(SLOT_LOCK_RECOVERY_MIN_BLOCK_MS)
                && self.slot_last_forced_cancel_attempt[idx].elapsed()
                    >= Duration::from_millis(SLOT_LOCK_RECOVERY_RETRY_MS)
            {
                self.slot_last_forced_cancel_attempt[idx] = now;
                warn!(
                    "🧯 Slot lock recovery: canceling tracked orders on {} after {}ms blocked",
                    slot.as_str(),
                    blocked_for.as_millis()
                );
                self.handle_cancel_slot(slot, CancelReason::Reprice).await;
            }
            let _ = self.result_tx.send(OrderResult::SlotBusy { slot }).await;
            return;
        }
        self.reset_slot_blocked_state(slot);

        let notional = (price * size).max(0.0);
        if direction == TradeDirection::Buy
            && self.marketable_buy_min_notional_floor > 0.0
            && notional + 1e-9 < self.marketable_buy_min_notional_floor
        {
            warn!(
                "🚫 Precheck reject {:?} {:?}@{:.3} sz={:.2}: notional ${:.2} < marketable min ${:.2}",
                direction, side, price, size, notional, self.marketable_buy_min_notional_floor
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
            self.emit_execution_feedback(ExecutionFeedback::PlacementRejected {
                side,
                reason,
                kind: RejectKind::Validation,
                ts: Instant::now(),
            })
            .await;
            let _ = self
                .result_tx
                .send(OrderResult::OrderFailed {
                    slot,
                    cooldown_ms: self.marketable_buy_cooldown_ms,
                })
                .await;
            return;
        }

        // Affordability guard: avoid futile submits when free collateral is clearly insufficient.
        // This reduces exchange-side hard rejects and cancel/place storms during low-balance windows.
        if direction == TradeDirection::Buy {
            if let Some(free_usdc) = self.cached_free_balance_usdc().await {
                let required_usdc = (price * size).max(0.0);
                let effective_free_usdc =
                    (free_usdc - local_unreleased_matched_notional_usdc.max(0.0)).max(0.0);
                // Keep a small cushion for transient balance/allowance lag.
                if effective_free_usdc + 0.05 < required_usdc {
                    warn!(
                        "🚫 pair_arb_headroom_blocked=true {:?} {:?}@{:.3} sz={:.1}: need {:.2} USDC > effective_free {:.2} USDC (raw_free={:.2} local_unreleased_matched={:.2})",
                        direction,
                        side,
                        price,
                        size,
                        required_usdc,
                        effective_free_usdc,
                        free_usdc,
                        local_unreleased_matched_notional_usdc.max(0.0),
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
                    self.emit_execution_feedback(ExecutionFeedback::PlacementRejected {
                        side,
                        reason,
                        kind: RejectKind::BalanceOrAllowance,
                        ts: Instant::now(),
                    })
                    .await;
                    let _ = self
                        .result_tx
                        .send(OrderResult::OrderFailed {
                            slot,
                            cooldown_ms: 15_000,
                        })
                        .await;
                    return;
                }
            }
        }

        match self
            .place_post_only_order(side, direction, price, size)
            .await
        {
            Ok(order_id) => {
                info!(
                    "✅ Order placed: {:?} {:?}@{:.3} id={}",
                    direction, side, price, order_id
                );
                if direction == TradeDirection::Buy {
                    self.last_buy_place_ts[side.index()] = Some(Instant::now());
                }
                self.slot_orders_mut(slot).insert(order_id, size);
                self.emit_execution_feedback(ExecutionFeedback::OrderAccepted {
                    slot,
                    ts: Instant::now(),
                })
                .await;
                // Notify OrderManager that state can transition to Live
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderPlaced {
                        slot,
                        target: DesiredTarget {
                            side,
                            direction,
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

                    match self
                        .place_post_only_order(side, direction, price, size)
                        .await
                    {
                        Ok(order_id) => {
                            info!(
                                "✅ Order placed after tick-size retry: {:?} {:?}@{:.3} id={}",
                                direction, side, price, order_id
                            );
                            if direction == TradeDirection::Buy {
                                self.last_buy_place_ts[side.index()] = Some(Instant::now());
                            }
                            self.slot_orders_mut(slot).insert(order_id, size);
                            self.emit_execution_feedback(ExecutionFeedback::OrderAccepted {
                                slot,
                                ts: Instant::now(),
                            })
                            .await;
                            let _ = self
                                .result_tx
                                .send(OrderResult::OrderPlaced {
                                    slot,
                                    target: DesiredTarget {
                                        side,
                                        direction,
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

                let mut err_text = format!("{:#}", final_err);
                let mut err_text_lower = err_text.to_ascii_lowercase();
                let fallback_bid_size =
                    ((self.provide_size_fallback_bid_size * 100.0).floor() / 100.0).max(0.0);
                let should_retry_with_fallback = direction == TradeDirection::Buy
                    && reason == BidReason::Provide
                    && size + 1e-9 < fallback_bid_size
                    && fallback_bid_size >= 0.01
                    && (Self::is_marketable_min_error(&err_text_lower)
                        || Self::is_validation_error(&err_text_lower));
                if should_retry_with_fallback {
                    warn!(
                        "🧭 Pairing small-size fallback retry {:?} {:?}@{:.3}: {:.2} -> {:.2}",
                        direction, side, price, size, fallback_bid_size
                    );
                    match self
                        .place_post_only_order(side, direction, price, fallback_bid_size)
                        .await
                    {
                        Ok(order_id) => {
                            info!(
                                "✅ Order placed after small-size fallback: {:?} {:?}@{:.3} size={:.2} id={}",
                                direction, side, price, fallback_bid_size, order_id
                            );
                            if direction == TradeDirection::Buy {
                                self.last_buy_place_ts[side.index()] = Some(Instant::now());
                            }
                            self.slot_orders_mut(slot)
                                .insert(order_id, fallback_bid_size);
                            self.emit_execution_feedback(ExecutionFeedback::OrderAccepted {
                                slot,
                                ts: Instant::now(),
                            })
                            .await;
                            let _ = self
                                .result_tx
                                .send(OrderResult::OrderPlaced {
                                    slot,
                                    target: DesiredTarget {
                                        side,
                                        direction,
                                        price,
                                        size: fallback_bid_size,
                                        reason,
                                    },
                                })
                                .await;
                            return;
                        }
                        Err(retry_err) => {
                            final_err = retry_err;
                            err_text = format!("{:#}", final_err);
                            err_text_lower = err_text.to_ascii_lowercase();
                        }
                    }
                }

                warn!(
                    "❌ Failed to place PostOnly {:?} {:?}: {:?}",
                    direction, side, final_err
                );
                if self.marketable_buy_autodetect {
                    if let Some(min_notional) =
                        Self::extract_min_marketable_notional(&err_text_lower)
                    {
                        if min_notional > self.marketable_buy_min_notional_floor + 1e-9 {
                            warn!(
                                "🧭 Learned marketable BUY min notional floor: ${:.2} -> ${:.2}",
                                self.marketable_buy_min_notional_floor, min_notional
                            );
                            self.marketable_buy_min_notional_floor = min_notional;
                        }
                    }
                }
                let is_429 = Self::is_rate_limit_error(&err_text_lower);
                let is_balance = Self::is_balance_or_allowance_error(&err_text_lower);
                let is_position_lag = direction == TradeDirection::Sell
                    && is_balance
                    && self.is_recent_same_side_buy_activity(side);
                let is_marketable_min = Self::is_marketable_min_error(&err_text_lower);
                let is_cross_book = Self::is_cross_book_error(&err_text_lower);
                let is_validation = Self::is_validation_error(&err_text_lower);
                let reject_kind = if is_429 {
                    RejectKind::RateLimit
                } else if is_position_lag {
                    RejectKind::PositionUnavailableLag
                } else if is_cross_book {
                    RejectKind::CrossBookTransient
                } else if is_balance {
                    RejectKind::BalanceOrAllowance
                } else if is_validation {
                    RejectKind::Validation
                } else {
                    RejectKind::Other
                };

                let cooldown_ms = if is_429 {
                    10_000 // 10s backoff for rate limiting
                } else if is_position_lag {
                    2_000 // transient venue lag right after same-side inventory fill
                } else if is_cross_book {
                    CROSS_BOOK_COOLDOWN_MS // short backoff; let adaptive safety margin settle
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
                    } else if is_position_lag {
                        "position-unavailable-lag"
                    } else if is_cross_book {
                        "cross-book-transient"
                    } else if is_balance {
                        "balance/allowance"
                    } else if is_marketable_min {
                        "marketable-min"
                    } else {
                        "validation/precision"
                    };
                    warn!(
                        "⛔ Hard reject on {:?}: pausing new placements for {}s ({})",
                        side,
                        cooldown_ms / 1000,
                        reject_kind_text
                    );
                    if is_position_lag {
                        warn!(
                            "⏳ Same-side {:?} inventory was just filled; venue likely has not released sellable balance yet",
                            side
                        );
                    }
                    if is_balance && matches!(reason, BidReason::Hedge) {
                        warn!(
                            "🧯 Hedge blocked on {:?}: insufficient balance/allowance; inventory may remain directional until cooldown expires",
                            side
                        );
                    }
                }
                if !is_position_lag {
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
                    self.emit_execution_feedback(ExecutionFeedback::PlacementRejected {
                        side,
                        reason,
                        kind: reject_kind,
                        ts: Instant::now(),
                    })
                    .await;
                }
                if is_cross_book {
                    self.emit_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
                        slot,
                        ts: Instant::now(),
                        rejected_action_price: price,
                    })
                    .await;
                }
                // FIX #4: Notify Coordinator the order failed so it can reset the slot
                let _ = self
                    .result_tx
                    .send(OrderResult::OrderFailed { slot, cooldown_ms })
                    .await;
            }
        }
    }

    // ─────────────────────────────────────────────────
    // One-shot Taker (FAK market order)
    // ─────────────────────────────────────────────────

    async fn handle_place_taker(
        &mut self,
        side: Side,
        direction: TradeDirection,
        size: f64,
        purpose: TradePurpose,
        limit_price: Option<f64>,
    ) {
        let mut size = Self::normalize_taker_size_for_market_buy(size);

        // Oracle-lag single-shot all-in sizing:
        // for BUY taker snipes, derive executable size from real-time free USDC
        // at submit time instead of fixed PM_BID_SIZE.
        if direction == TradeDirection::Buy
            && purpose == TradePurpose::OracleLagSnipe
            && limit_price.unwrap_or(0.0) > 0.0
        {
            let px_cap = limit_price.unwrap_or(0.0);
            if let Some(free_usdc) = self.cached_free_balance_usdc().await {
                let cushion = 0.05_f64;
                let spendable = (free_usdc - cushion).max(0.0);
                let affordable_shares = (spendable / px_cap).max(0.0);
                let affordable_2dp = (affordable_shares * 100.0).floor() / 100.0;
                let chosen = Self::normalize_taker_size_for_market_buy(affordable_2dp);
                info!(
                    "📐 oracle_lag_order_size | side={:?} requested={:.2} chosen={:.2} free_usdc={:.2} spendable_usdc={:.2} limit_price={:.4}",
                    side, size, chosen, free_usdc, spendable, px_cap
                );
                size = chosen;
            }
        }

        info!(
            "📤 TAKER {:?} {:?} size={:.2} purpose={:?} limit_price={:?}",
            direction, side, size, purpose, limit_price
        );

        // CLOB lot-size floor (2dp). Skip impossible requests without retry storm.
        if size < 0.01 {
            warn!(
                "🚫 Skip taker {:?} {:?}: size {:.4} below executable lot floor 0.01",
                direction, side, size
            );
            let _ = self
                .result_tx
                .send(OrderResult::TakerHedgeDone { side })
                .await;
            return;
        }

        if self.cfg.dry_run || self.client.is_none() {
            info!(
                "📝 [DRY-RUN] Taker {:?} {:?} size={:.2} purpose={:?}",
                direction, side, size, purpose
            );
            let _ = self
                .result_tx
                .send(OrderResult::TakerHedgeDone { side })
                .await;
            return;
        }

        // OMS should cancel same-side rest orders before this command.
        // Keep a hard guard here to avoid accidental mixed maker+taker on same side.
        let existing_count = self.side_order_count(side);
        if existing_count > 0 {
            warn!(
                "🚫 Refusing Taker {:?} {:?}: {} tracked order(s) still open on side",
                direction, side, existing_count
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
                .send(OrderResult::TakerHedgeFailed {
                    side,
                    cooldown_ms: 2_000,
                })
                .await;
            return;
        }

        match self
            .place_taker_order(side, direction, size, limit_price)
            .await
        {
            Ok(order_id) => {
                info!(
                    "✅ Taker accepted: {:?} {:?} size={:.2} id={}",
                    direction, side, size, order_id
                );
                let _ = self
                    .result_tx
                    .send(OrderResult::TakerHedgeDone { side })
                    .await;
            }
            Err(e) => {
                warn!("❌ Failed taker {:?} {:?}: {:?}", direction, side, e);
                let err_text = format!("{:#}", e);
                let err_text_lower = err_text.to_ascii_lowercase();
                let is_429 = Self::is_rate_limit_error(&err_text_lower);
                let is_balance = Self::is_balance_or_allowance_error(&err_text_lower);
                let is_liquidity = err_text_lower.contains("no orders found to match")
                    || err_text_lower.contains("no opposing orders");
                let is_validation = Self::is_validation_error(&err_text_lower);
                let is_accuracy_precision = err_text_lower.contains("invalid amounts")
                    || err_text_lower.contains("max accuracy");

                let reject_kind = if is_429 {
                    RejectKind::RateLimit
                } else if is_balance {
                    RejectKind::BalanceOrAllowance
                } else if is_validation || is_liquidity {
                    RejectKind::Validation
                } else {
                    RejectKind::Other
                };
                let cooldown_ms = if is_429 {
                    10_000
                } else if is_balance {
                    30_000
                } else if is_liquidity {
                    15_000
                } else if is_accuracy_precision {
                    10_000
                } else if is_validation {
                    2_000
                } else {
                    0
                };
                let _ = self
                    .emit_reject_event(PlacementRejectEvent {
                        side,
                        reason: purpose.as_bid_reason(),
                        kind: reject_kind,
                        price: 0.0,
                        size,
                        ts: Instant::now(),
                    })
                    .await;
                self.emit_execution_feedback(ExecutionFeedback::PlacementRejected {
                    side,
                    reason: purpose.as_bid_reason(),
                    kind: reject_kind,
                    ts: Instant::now(),
                })
                .await;
                let _ = self
                    .result_tx
                    .send(OrderResult::TakerHedgeFailed { side, cooldown_ms })
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
            for orders in self.open_orders.iter_mut() {
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
                    for orders in self.open_orders.iter_mut() {
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

    async fn handle_cancel_slot(&mut self, slot: OrderSlot, reason: CancelReason) {
        let order_ids: Vec<String> = self.slot_orders(slot).keys().cloned().collect();

        if order_ids.is_empty() {
            let _ = self.result_tx.send(OrderResult::CancelAck { slot }).await;
            return;
        }

        info!(
            "🗑️ Cancel {} {} orders (reason={:?})",
            order_ids.len(),
            slot.as_str(),
            reason,
        );

        let mut failed_count = 0usize;
        for id in &order_ids {
            if !self.handle_cancel_order(id, reason).await {
                failed_count += 1;
            }
        }

        if failed_count > 0 {
            warn!(
                "⚠️ CancelSlot {}: {}/{} cancel(s) failed — those orders remain tracked in open_orders",
                slot.as_str(),
                failed_count,
                order_ids.len(),
            );
            let _ = self.result_tx.send(OrderResult::SlotBusy { slot }).await;
            return;
        }

        let _ = self.result_tx.send(OrderResult::CancelAck { slot }).await;
    }

    async fn handle_cancel_side(&mut self, side: Side, reason: CancelReason) {
        let mut order_ids: Vec<String> = Vec::new();
        for slot in OrderSlot::side_slots(side) {
            order_ids.extend(self.slot_orders(slot).keys().cloned());
        }

        if order_ids.is_empty() {
            for slot in OrderSlot::side_slots(side) {
                let _ = self.result_tx.send(OrderResult::CancelAck { slot }).await;
            }
            return;
        }

        info!(
            "🗑️ Cancel {} {:?} orders (reason={:?})",
            order_ids.len(),
            side,
            reason,
        );

        let mut failed_count = 0usize;
        for id in &order_ids {
            if !self.handle_cancel_order(id, reason).await {
                failed_count += 1;
            }
        }

        if failed_count > 0 {
            warn!(
                "⚠️ CancelSide {:?}: {}/{} cancel(s) failed — emitting SlotBusy for still-tracked slots",
                side, failed_count, order_ids.len(),
            );
        }

        for slot in OrderSlot::side_slots(side) {
            if self.slot_orders(slot).is_empty() {
                let _ = self.result_tx.send(OrderResult::CancelAck { slot }).await;
            } else {
                let _ = self.result_tx.send(OrderResult::SlotBusy { slot }).await;
            }
        }
    }

    async fn handle_cancel_all(&mut self, reason: CancelReason) {
        let total: usize = self.open_orders.iter().map(|v| v.len()).sum();
        info!("🗑️ CancelAll: {} orders (reason={:?})", total, reason);

        if self.cfg.dry_run || self.client.is_none() {
            info!("📝 [DRY-RUN] CancelAll");
            self.open_orders.iter_mut().for_each(|v| v.clear());
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
                self.open_orders.iter_mut().for_each(|v| v.clear());
            }
            Err(e) => {
                warn!(
                    "❌ Failed to cancel all: {:?} — fallback to per-order cancel",
                    e
                );

                let mut ids: Vec<String> = Vec::new();
                for side_orders in self.open_orders.iter() {
                    ids.extend(side_orders.keys().cloned());
                }

                for id in ids {
                    let _ = self.handle_cancel_order(&id, reason).await;
                }

                let remaining: usize = self.open_orders.iter().map(|v| v.len()).sum();
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

    async fn emit_execution_feedback(&self, feedback: ExecutionFeedback) {
        if let Some(tx) = &self.feedback_tx {
            let _ = tx.send(feedback).await;
        }
    }

    fn is_recent_same_side_buy_fill(&self, side: Side) -> bool {
        self.last_buy_fill_ts[side.index()]
            .map(|ts| ts.elapsed() <= Duration::from_secs(5))
            .unwrap_or(false)
    }

    fn is_recent_same_side_buy_place(&self, side: Side) -> bool {
        self.last_buy_place_ts[side.index()]
            .map(|ts| ts.elapsed() <= Duration::from_secs(5))
            .unwrap_or(false)
    }

    fn is_recent_same_side_buy_activity(&self, side: Side) -> bool {
        self.is_recent_same_side_buy_fill(side) || self.is_recent_same_side_buy_place(side)
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
        direction: TradeDirection,
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

        let sdk_side = match direction {
            TradeDirection::Buy => SdkSide::Buy,
            TradeDirection::Sell => SdkSide::Sell,
        };
        // CRITICAL: post_only(true) ensures we NEVER cross the spread.
        // Without this, if our bid price >= best ask, we'd become a taker.
        let order = client
            .limit_order()
            .token_id(token_id_uint)
            .size(size_decimal)
            .price(price_decimal)
            .side(sdk_side)
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

    async fn place_taker_order(
        &self,
        side: Side,
        direction: TradeDirection,
        size: f64,
        limit_price: Option<f64>,
    ) -> anyhow::Result<String> {
        use polymarket_client_sdk::clob::types::{
            Amount, OrderStatusType, OrderType, Side as SdkSide,
        };

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
        let token_id_uint =
            alloy::primitives::U256::from_str_radix(token_id, 10).context("Invalid token_id")?;

        // Lot size: CLOB max lot precision = 2dp.
        let size_rounded = (size * 100.0).floor() / 100.0;
        if size_rounded < 0.01 {
            anyhow::bail!("size {:.6} rounds to 0 at 2dp — skipping", size);
        }

        let sdk_side = match direction {
            TradeDirection::Buy => SdkSide::Buy,
            TradeDirection::Sell => SdkSide::Sell,
        };

        // Two FAK paths:
        //   limit_price=Some(p)  → IOC limit at p: sweeps levels ≤ p (buy) / ≥ p (sell); remainder auto-cancels.
        //   limit_price=None     → market FAK: SDK walks book depth to compute cutoff (legacy hedge path).
        let order = if let Some(p) = limit_price {
            if !(0.0..1.0).contains(&self.cfg.tick_size) {
                anyhow::bail!("invalid tick_size={}", self.cfg.tick_size);
            }
            // Align price to the strictest CLOB tick. Around 0.96/0.04 boundary it's 0.001.
            // For oracle_lag_sniping we want 0.992 → scale 3 is always acceptable.
            let price_rounded = (p * 1000.0).round() / 1000.0;
            let price_decimal = rust_decimal::Decimal::from_f64(price_rounded)
                .ok_or_else(|| anyhow::anyhow!("Invalid FAK limit price"))?;
            let size_decimal = rust_decimal::Decimal::from_f64(size_rounded)
                .ok_or_else(|| anyhow::anyhow!("Invalid FAK size"))?;
            client
                .limit_order()
                .token_id(token_id_uint)
                .price(price_decimal)
                .size(size_decimal)
                .side(sdk_side)
                .order_type(OrderType::FAK)
                .post_only(false)
                .build()
                .await?
        } else {
            let shares = rust_decimal::Decimal::from_f64(size_rounded)
                .ok_or_else(|| anyhow::anyhow!("Invalid taker size"))?;
            let amount = Amount::shares(shares).expect("Valid amount");
            client
                .market_order()
                .token_id(token_id_uint)
                .amount(amount)
                .side(sdk_side)
                .order_type(OrderType::FAK)
                .build()
                .await?
        };

        let signed = client.sign(signer, order).await?;
        let response = client.post_order(signed).await?;

        if !response.success {
            anyhow::bail!(
                "post_order rejected: status={:?} error={:?}",
                response.status,
                response.error_msg.unwrap_or_default(),
            );
        }

        if !matches!(
            response.status,
            OrderStatusType::Live | OrderStatusType::Matched | OrderStatusType::Delayed
        ) {
            anyhow::bail!(
                "taker hedge unexpected status: {:?} error={:?}",
                response.status,
                response.error_msg.unwrap_or_default(),
            );
        }

        Ok(response.order_id)
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

    fn is_balance_or_allowance_error(lower: &str) -> bool {
        lower.contains("not enough balance") || lower.contains("allowance")
    }

    fn is_rate_limit_error(lower: &str) -> bool {
        lower.contains("429") || lower.contains("too many requests") || lower.contains("rate limit")
    }

    /// P2 FIX: Detect validation/precision errors to apply cooldown and prevent retry storms.
    fn is_validation_error(lower: &str) -> bool {
        lower.contains("decimal places")
            || lower.contains("lot size")
            || Self::is_cross_book_error(lower)
            || lower.contains("rounds to 0")
            || lower.contains("invalid amounts")
            || lower.contains("max accuracy")
            || lower.contains("no orders found to match")
            || lower.contains("no opposing orders")
            || Self::is_marketable_min_error(lower)
    }

    fn is_cross_book_error(lower: &str) -> bool {
        lower.contains("order crosses book")
    }

    /// Normalize taker market-buy share size to avoid venue precision rejects.
    ///
    /// Why this exists:
    /// - We observed venue-side 400 errors with "max accuracy" on market BUYs.
    /// - For BUY-by-shares, fractional share sizes can induce overly granular
    ///   maker/taker amounts after price multiplication in the SDK builder path.
    ///
    /// Policy:
    /// - Keep 2dp lot floor.
    /// - For sizes >= 1 share, round down to whole shares (safer/robust).
    /// - Never round up (do not increase risk).
    fn normalize_taker_size_for_market_buy(size: f64) -> f64 {
        if !size.is_finite() || size <= 0.0 {
            return 0.0;
        }
        let size_2dp = (size * 100.0).floor() / 100.0;
        if size_2dp >= 1.0 {
            size_2dp.floor()
        } else {
            size_2dp
        }
    }

    fn is_marketable_min_error(lower: &str) -> bool {
        lower.contains("invalid amount for a marketable buy order")
            || (lower.contains("marketable") && lower.contains("min size"))
            || lower.contains("min size: $")
    }

    /// Get count of open orders for a side.
    pub fn open_order_count(&self, side: Side) -> usize {
        self.side_order_count(side)
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;

    use super::{ExecutionCmd, Executor, ExecutorConfig, OrderResult, ReconcileFetchMode};
    use crate::polymarket::messages::{
        CancelReason, FillEvent, FillStatus, OrderSlot, TradeDirection,
    };
    use crate::polymarket::types::Side;

    fn test_executor() -> Executor {
        let (_cmd_tx, cmd_rx) = mpsc::channel::<ExecutionCmd>(4);
        let (result_tx, _result_rx) = mpsc::channel::<OrderResult>(4);
        let (_fill_tx, fill_rx) = mpsc::channel(4);
        Executor::new(
            ExecutorConfig {
                rest_url: "https://example.invalid".to_string(),
                market_id: "0x0".to_string(),
                yes_asset_id: "1".to_string(),
                no_asset_id: "2".to_string(),
                tick_size: 0.01,
                reconcile_interval_secs: 30,
                dry_run: false,
            },
            None,
            None,
            cmd_rx,
            result_tx,
            fill_rx,
            None,
            None,
        )
    }

    #[test]
    fn taker_size_normalization_whole_share_for_size_ge_one() {
        assert!((Executor::normalize_taker_size_for_market_buy(10.01) - 10.0).abs() < 1e-9);
        assert!((Executor::normalize_taker_size_for_market_buy(10.99) - 10.0).abs() < 1e-9);
        assert!((Executor::normalize_taker_size_for_market_buy(1.00) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn taker_size_normalization_keeps_sub_share_2dp() {
        assert!((Executor::normalize_taker_size_for_market_buy(0.789) - 0.78).abs() < 1e-9);
        assert!((Executor::normalize_taker_size_for_market_buy(0.019) - 0.01).abs() < 1e-9);
    }

    #[test]
    fn validation_error_detects_accuracy_rejects() {
        assert!(Executor::is_validation_error(
            "invalid amounts, maker amount supports a max accuracy of 2 decimals"
        ));
        assert!(Executor::is_validation_error(
            "taker amount a max accuracy of 4 decimals"
        ));
        assert!(Executor::is_validation_error(
            "no orders found to match with fak order"
        ));
        assert!(Executor::is_validation_error(
            "validation: invalid: no opposing orders"
        ));
    }

    #[test]
    fn executor_defaults_reconcile_mode_to_local_by_id() {
        let exec = test_executor();
        assert_eq!(exec.reconcile_fetch_mode, ReconcileFetchMode::LocalById);
    }

    #[test]
    fn recent_same_side_buy_fill_detects_lag_window() {
        let mut exec = test_executor();
        exec.last_buy_fill_ts[Side::No.index()] = Some(Instant::now());
        assert!(exec.is_recent_same_side_buy_fill(Side::No));

        exec.last_buy_fill_ts[Side::No.index()] = Some(Instant::now() - Duration::from_secs(6));
        assert!(!exec.is_recent_same_side_buy_fill(Side::No));
    }

    #[tokio::test]
    async fn matched_partial_fill_under_dust_floor_releases_slot() {
        let (_cmd_tx, cmd_rx) = mpsc::channel::<ExecutionCmd>(4);
        let (result_tx, mut result_rx) = mpsc::channel::<OrderResult>(4);
        let (_fill_tx, fill_rx) = mpsc::channel(4);
        let mut exec = Executor::new(
            ExecutorConfig {
                rest_url: "https://example.invalid".to_string(),
                market_id: "0x0".to_string(),
                yes_asset_id: "1".to_string(),
                no_asset_id: "2".to_string(),
                tick_size: 0.01,
                reconcile_interval_secs: 30,
                dry_run: false,
            },
            None,
            None,
            cmd_rx,
            result_tx,
            fill_rx,
            None,
            None,
        );

        let slot = OrderSlot::NO_BUY;
        exec.slot_orders_mut(slot).insert("ord-1".to_string(), 0.11);
        exec.handle_fill_notification(&FillEvent {
            order_id: "ord-1".to_string(),
            side: Side::No,
            direction: TradeDirection::Buy,
            filled_size: 0.02,
            price: 0.46,
            status: FillStatus::Matched,
            ts: Instant::now(),
        })
        .await;

        assert!(exec.slot_orders(slot).is_empty());
        let evt = result_rx
            .try_recv()
            .expect("OrderFilled expected for dust-closed slot");
        assert!(matches!(evt, OrderResult::OrderFilled { slot: s } if s == slot));
    }

    #[tokio::test]
    async fn cancel_slot_without_client_returns_cancel_ack_and_clears_tracking() {
        let (_cmd_tx, cmd_rx) = mpsc::channel::<ExecutionCmd>(4);
        let (result_tx, mut result_rx) = mpsc::channel::<OrderResult>(8);
        let (_fill_tx, fill_rx) = mpsc::channel(4);
        let mut exec = Executor::new(
            ExecutorConfig {
                rest_url: "https://example.invalid".to_string(),
                market_id: "0x0".to_string(),
                yes_asset_id: "1".to_string(),
                no_asset_id: "2".to_string(),
                tick_size: 0.01,
                reconcile_interval_secs: 30,
                dry_run: false,
            },
            None,
            None,
            cmd_rx,
            result_tx,
            fill_rx,
            None,
            None,
        );

        let slot = OrderSlot::YES_BUY;
        exec.slot_orders_mut(slot)
            .insert("ord-local".to_string(), 5.0);
        exec.handle_cancel_slot(slot, CancelReason::Reprice).await;

        let first = result_rx.recv().await.expect("order result expected");
        assert!(matches!(first, OrderResult::CancelAck { slot: s } if s == slot));
        assert!(exec.slot_orders(slot).is_empty());
    }
}
