//! Channel message types for the Maker-Only Actor architecture.
//!
//! Strategy: Post passive Post-Only Bids, never take.
//! OFI serves as a Kill Switch to cancel bids under toxic flow.

use std::time::Instant;

use super::types::Side;

// ─────────────────────────────────────────────────────────
// Market Data Messages (WebSocket → OFI Engine + Coordinator)
// ─────────────────────────────────────────────────────────

/// Market data events from the WebSocket feed.
#[derive(Debug, Clone)]
pub enum MarketDataMsg {
    /// Order book top-of-book update (best bid/ask for both YES and NO).
    BookTick {
        yes_bid: f64,
        yes_ask: f64,
        no_bid: f64,
        no_ask: f64,
        ts: Instant,
    },
    /// Individual trade tick (from `last_trade_price` WS event).
    TradeTick {
        asset_id: String,
        market_side: Side,
        taker_side: TakerSide,
        price: f64,
        size: f64,
        ts: Instant,
    },
}

/// Taker aggressor direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerSide {
    Buy,
    Sell,
}

// ─────────────────────────────────────────────────────────
// OFI Engine Output (per-side toxicity)
// ─────────────────────────────────────────────────────────

/// Per-side OFI metrics.
#[derive(Debug, Clone, Copy)]
pub struct SideOfi {
    pub ofi_score: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub is_toxic: bool,
}

impl Default for SideOfi {
    fn default() -> Self {
        Self {
            ofi_score: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            is_toxic: false,
        }
    }
}

/// Real-time OFI snapshot — tracked per side.
#[derive(Debug, Clone, Copy)]
pub struct OfiSnapshot {
    pub yes: SideOfi,
    pub no: SideOfi,
    pub ts: Instant,
}

impl Default for OfiSnapshot {
    fn default() -> Self {
        Self {
            yes: SideOfi::default(),
            no: SideOfi::default(),
            ts: Instant::now(),
        }
    }
}

// ─────────────────────────────────────────────────────────
// Inventory State
// ─────────────────────────────────────────────────────────

/// Current inventory / position snapshot.
/// Broadcast via watch channel to Coordinator.
#[derive(Debug, Clone, Copy)]
pub struct InventoryState {
    pub yes_qty: f64,
    pub no_qty: f64,
    pub yes_avg_cost: f64,
    pub no_avg_cost: f64,
    pub net_diff: f64,
    pub portfolio_cost: f64,
    /// Whether inventory constraints allow opening new positions.
    /// Computed by InventoryManager from max_net_diff / max_portfolio_cost.
    pub can_open: bool,
}

impl Default for InventoryState {
    fn default() -> Self {
        Self {
            yes_qty: 0.0,
            no_qty: 0.0,
            yes_avg_cost: 0.0,
            no_avg_cost: 0.0,
            net_diff: 0.0,
            portfolio_cost: 0.0,
            can_open: true, // Default: no positions → can open
        }
    }
}

// ─────────────────────────────────────────────────────────
// Execution Commands (Coordinator → Executor)
// ─────────────────────────────────────────────────────────

/// Execution instructions — Maker-Only model.
#[derive(Debug, Clone)]
pub enum ExecutionCmd {
    /// Place a Post-Only passive bid (maker limit order).
    /// If this order would cross the spread and take, the exchange rejects it.
    PlacePostOnlyBid {
        side: Side,
        price: f64,
        size: f64,
        /// Why this bid is being placed.
        reason: BidReason,
    },
    /// Cancel a specific order by ID.
    CancelOrder {
        order_id: String,
        reason: CancelReason,
    },
    /// Cancel all orders on a specific side.
    CancelSide { side: Side, reason: CancelReason },
    /// Cancel all outstanding orders (full circuit breaker).
    CancelAll { reason: CancelReason },
}

/// Why a bid is being placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BidReason {
    /// Providing liquidity on both sides (balanced inventory).
    Provide,
    /// Hedging: placing a bid on the missing leg to complete the pair.
    Hedge,
}

/// Why an order is being canceled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// OFI Kill Switch detected toxic flow.
    ToxicFlow,
    /// Inventory imbalance — stop accumulating the excess side.
    InventoryLimit,
    /// Price moved — stale order needs repricing.
    Reprice,
    /// Shutdown / circuit breaker.
    Shutdown,
    /// Market has expired — clean up before rotating.
    MarketExpired,
}

// ─────────────────────────────────────────────────────────
// Order Results (Executor → Coordinator)
// ─────────────────────────────────────────────────────────

/// Feedback from Executor to Coordinator about order outcomes.
/// Allows Coordinator to reset ghost slots when orders fail.
#[derive(Debug, Clone)]
pub enum OrderResult {
    /// Order placement failed — Coordinator should reset the slot.
    OrderFailed { side: Side },
}

// ─────────────────────────────────────────────────────────
// Fill Events (User WS → InventoryManager)
//
// CRITICAL: Only the authenticated User WebSocket may emit FillEvents.
// The Executor MUST NEVER create FillEvents — it only sends orders.
// ─────────────────────────────────────────────────────────

/// Status of a fill as reported by the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillStatus {
    /// Order matched on the CLOB — trade exists but not yet on-chain.
    Matched,
    /// Trade confirmed on-chain.
    Confirmed,
    /// Trade failed / reverted — inventory must be reversed.
    Failed,
}

/// Real trade fill from the exchange (via authenticated User WS).
///
/// This is the **single source of truth** for inventory changes.
/// `filled_size` is the size of THIS fill (supports partial fills).
#[derive(Debug, Clone)]
pub struct FillEvent {
    /// Exchange order ID.
    pub order_id: String,
    /// Which side was filled (YES or NO).
    pub side: Side,
    /// Size filled in THIS event (may be partial).
    pub filled_size: f64,
    /// Fill price.
    pub price: f64,
    /// Fill status from the exchange.
    pub status: FillStatus,
    pub ts: Instant,
}

// ─────────────────────────────────────────────────────────
// OFI Kill Switch Commands (OFI Engine → Executor, high priority)
// ─────────────────────────────────────────────────────────

/// High-priority kill switch signal from OFI Engine directly to Executor.
/// Bypasses the Coordinator for minimum latency.
#[derive(Debug, Clone)]
pub struct KillSwitchSignal {
    /// Which side's bids to cancel.
    pub side: Side,
    pub ofi_score: f64,
    pub ts: Instant,
}
