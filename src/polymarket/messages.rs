//! Channel message types for the Actor architecture.
//!
//! Strategy defaults to maker-only (Post-Only bids), with explicit tail-risk
//! escape hatches for one-shot taker hedges near market close.
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
    /// Winner-side hint for post-close strategies.
    WinnerHint {
        side: Side,
        source: WinnerHintSource,
        ref_price: f64,
        observed_price: f64,
        /// Winner-side best bid observed by post-close evidence collector.
        /// 0.0 means "unknown".
        winner_bid: f64,
        /// Winner-side best ask observed by post-close evidence collector.
        /// 0.0 means "no ask observed".
        winner_ask_raw: f64,
        /// Evidence source used to build winner-side top-of-book.
        /// Typical values: "ws_partial" / "clob_rest" / "none".
        winner_book_source: &'static str,
        /// Absolute |evidence_recv_ms - final_detect_ms| in milliseconds.
        winner_distance_to_final_ms: u64,
        /// True iff open_ref came from the exact round_start Chainlink tick
        /// (prewarm or live RTDS), not a fallback (prev_close / frontend API).
        open_is_exact: bool,
        ts: Instant,
    },
    /// Sent by cross-market arbiter to every coordinator for a given round.
    /// Delivered via the same `winner_hint_rx` channel as `WinnerHint`.
    OracleLagSelection {
        round_end_ts: u64,
        selected: bool,
        rank: u8,
        reason: &'static str,
    },
    /// Cross-market round-tail action for oracle_lag_sniping.
    /// Dispatched once after all expected market finals are processed (or timeout).
    OracleLagTailAction {
        round_end_ts: u64,
        side: Side,
        mode: OracleLagTailMode,
        /// Limit price to submit.
        /// - Fak99: typically 0.99
        /// - MakerBidStep: `min(bid0 + step, 0.991)`
        limit_price: f64,
        /// Slug that won tail ranking.
        target_slug: String,
        /// Coordinator-readable reason for logs/diagnostics.
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleLagTailMode {
    Fak99,
    MakerBidStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WinnerHintSource {
    Chainlink,
    Gamma,
    BookInference,
}

/// Taker aggressor direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerSide {
    Buy,
    Sell,
}

/// Trade direction for strategy intents and fill accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeDirection {
    Buy,
    Sell,
}

/// Unique logical order slot inside the strategy/execution graph.
///
/// A true two-sided market maker can keep both a bid and an ask live on the
/// same outcome token, so `(side, direction)` must become the primary identity
/// instead of `side` alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderSlot {
    pub side: Side,
    pub direction: TradeDirection,
}

impl OrderSlot {
    pub const YES_BUY: Self = Self {
        side: Side::Yes,
        direction: TradeDirection::Buy,
    };
    pub const YES_SELL: Self = Self {
        side: Side::Yes,
        direction: TradeDirection::Sell,
    };
    pub const NO_BUY: Self = Self {
        side: Side::No,
        direction: TradeDirection::Buy,
    };
    pub const NO_SELL: Self = Self {
        side: Side::No,
        direction: TradeDirection::Sell,
    };
    pub const ALL: [Self; 4] = [Self::YES_BUY, Self::YES_SELL, Self::NO_BUY, Self::NO_SELL];

    pub const fn new(side: Side, direction: TradeDirection) -> Self {
        Self { side, direction }
    }

    pub fn index(self) -> usize {
        match (self.side, self.direction) {
            (Side::Yes, TradeDirection::Buy) => 0,
            (Side::Yes, TradeDirection::Sell) => 1,
            (Side::No, TradeDirection::Buy) => 2,
            (Side::No, TradeDirection::Sell) => 3,
        }
    }

    pub fn side_slots(side: Side) -> [Self; 2] {
        match side {
            Side::Yes => [Self::YES_BUY, Self::YES_SELL],
            Side::No => [Self::NO_BUY, Self::NO_SELL],
        }
    }

    pub fn as_str(self) -> &'static str {
        match (self.side, self.direction) {
            (Side::Yes, TradeDirection::Buy) => "YES_BUY",
            (Side::Yes, TradeDirection::Sell) => "YES_SELL",
            (Side::No, TradeDirection::Buy) => "NO_BUY",
            (Side::No, TradeDirection::Sell) => "NO_SELL",
        }
    }
}

/// Execution urgency class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeUrgency {
    /// Passive maker order (post-only limit).
    MakerPostOnly,
    /// Active taker order (FAK market by shares).
    TakerFak,
}

/// Semantic purpose of an intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradePurpose {
    Provide,
    Hedge,
    Reduce,
    Exit,
    /// FAK/taker snipe fired immediately on Chainlink winner hint in oracle_lag_sniping.
    OracleLagSnipe,
}

impl TradePurpose {
    pub fn as_bid_reason(self) -> BidReason {
        match self {
            Self::Provide | Self::OracleLagSnipe => BidReason::Provide,
            Self::Hedge | Self::Reduce | Self::Exit => BidReason::Hedge,
        }
    }
}

/// Unified execution intent across strategies.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub side: Side,
    pub direction: TradeDirection,
    pub urgency: TradeUrgency,
    pub size: f64,
    pub price: Option<f64>,
    pub purpose: TradePurpose,
    /// Local estimate of unresolved matched buy notional (USDC) used by executor
    /// affordability precheck. Non-pair_arb paths keep this as 0.0.
    pub local_unreleased_matched_notional_usdc: f64,
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
    pub heat_score: f64,
    pub is_hot: bool,
    pub is_toxic: bool,
    pub toxic_buy: bool,
    pub toxic_sell: bool,
    pub saturated: bool,
}

impl Default for SideOfi {
    fn default() -> Self {
        Self {
            ofi_score: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            heat_score: 0.0,
            is_hot: false,
            is_toxic: false,
            toxic_buy: false,
            toxic_sell: false,
            saturated: false,
        }
    }
}

impl SideOfi {
    pub fn blocks(self, direction: TradeDirection) -> bool {
        match direction {
            TradeDirection::Buy => self.toxic_buy,
            TradeDirection::Sell => self.toxic_sell,
        }
    }
}

/// Real-time OFI snapshot — tracked per side.
#[derive(Debug, Clone, Copy)]
pub struct OfiSnapshot {
    pub yes: SideOfi,
    pub no: SideOfi,
    pub reference_mid_yes: f64,
    pub ts: Instant,
}

impl Default for OfiSnapshot {
    fn default() -> Self {
        Self {
            yes: SideOfi::default(),
            no: SideOfi::default(),
            reference_mid_yes: 0.5,
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
        }
    }
}

/// Inventory truth model with explicit pending/provisional fills.
///
/// - `settled`: final trusted inventory used for risk relief.
/// - `working`: settled + unresolved matched fills used for conservative reactions.
/// - `fragile`: true whenever unresolved pending fills exist.
#[derive(Debug, Clone, Copy, Default)]
pub struct InventorySnapshot {
    pub settled: InventoryState,
    pub working: InventoryState,
    pub pending_yes_qty: f64,
    pub pending_no_qty: f64,
    pub fragile: bool,
}

// ─────────────────────────────────────────────────────────
// Order Management Commands (Coordinator → OMS)
// ─────────────────────────────────────────────────────────

/// A snapshot of the strategy's desired order state for a specific side.
#[derive(Debug, Clone)]
pub struct DesiredTarget {
    pub side: Side,
    pub direction: TradeDirection,
    pub price: f64,
    pub size: f64,
    /// Original intent from Coordinator (Provide vs Hedge).
    pub reason: BidReason,
}

impl PartialEq for DesiredTarget {
    fn eq(&self, other: &Self) -> bool {
        // Keep equality semantic focused on executable order shape to avoid
        // unnecessary cancel/replace when only reason tag changes.
        self.side == other.side
            && self.direction == other.direction
            && self.price == other.price
            && self.size == other.size
    }
}

impl DesiredTarget {
    pub fn slot(&self) -> OrderSlot {
        OrderSlot::new(self.side, self.direction)
    }
}

#[derive(Debug, Clone)]
pub enum OrderManagerCmd {
    /// Update the desired target for an order slot.
    SetTarget(DesiredTarget),
    /// Pair_arb-only metadata for executor affordability precheck.
    SetPairArbHeadroom {
        slot: OrderSlot,
        local_unreleased_matched_notional_usdc: f64,
    },
    /// Clear desired target for a slot and preserve cancel reason.
    ClearTarget {
        slot: OrderSlot,
        reason: CancelReason,
    },
    /// Queue a one-shot taker hedge on a side.
    /// OMS will cancel same-side resting orders first, then submit the taker order.
    /// `limit_price=Some(p)` → IOC limit-FAK at p (SDK limit_order + FAK); sweeps only levels ≤ p on buy / ≥ p on sell.
    /// `limit_price=None` → legacy market-FAK path (SDK market_order + FAK); walks book depth to calculate cutoff.
    OneShotTakerHedge {
        side: Side,
        direction: TradeDirection,
        size: f64,
        purpose: TradePurpose,
        limit_price: Option<f64>,
    },
    /// Emergency cancel all targets & orders globally.
    CancelAll,
}

// ─────────────────────────────────────────────────────────
// Execution Commands (OMS → Executor)
// ─────────────────────────────────────────────────────────

/// Execution instructions — Maker-first model with explicit taker de-risk path.
#[derive(Debug, Clone)]
pub enum ExecutionCmd {
    /// Strategy-agnostic execution intent.
    ExecuteIntent { intent: TradeIntent },
    /// Place a Post-Only passive bid (maker limit order).
    /// If this order would cross the spread and take, the exchange rejects it.
    PlacePostOnlyBid {
        side: Side,
        direction: TradeDirection,
        price: f64,
        size: f64,
        /// Why this bid is being placed.
        reason: BidReason,
    },
    /// Place a one-shot taker hedge (FAK market order by shares).
    /// Used only in endgame de-risking mode.
    PlaceTakerHedge {
        side: Side,
        direction: TradeDirection,
        size: f64,
    },
    /// Cancel a specific order by ID.
    CancelOrder {
        order_id: String,
        reason: CancelReason,
    },
    /// Cancel all outstanding orders for a specific slot.
    CancelSlot {
        slot: OrderSlot,
        reason: CancelReason,
    },
    /// Cancel all orders on a specific side.
    CancelSide { side: Side, reason: CancelReason },
    /// Cancel all outstanding orders (full circuit breaker).
    CancelAll { reason: CancelReason },
    /// Force an immediate REST reconciliation cycle (used by OMS timeout recovery).
    ReconcileNow { reason: &'static str },
}

/// Why a bid is being placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BidReason {
    /// Providing liquidity on both sides (balanced inventory).
    Provide,
    /// Oracle-lag sniping maker fallback provide.
    /// Semantically provide-like, but kept explicit to avoid strategy coupling in executor sizing.
    OracleLagProvide,
    /// Hedging: placing a bid on the missing leg to complete the pair.
    Hedge,
}

/// Why an order is being canceled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// OFI Kill Switch detected toxic flow.
    ToxicFlow,
    /// Book/feed stale guard.
    StaleData,
    /// Inventory imbalance — stop accumulating the excess side.
    InventoryLimit,
    /// Endgame risk gate blocked further risk-increasing provides.
    EndgameRiskGate,
    /// Price moved — stale order needs repricing.
    Reprice,
    /// Shutdown / circuit breaker.
    Shutdown,
    /// Market has expired — clean up before rotating.
    MarketExpired,
    /// P1 FIX: Startup reconciliation — clear any lingering orders from crashes.
    Startup,
}

// ─────────────────────────────────────────────────────────
// Order Results (Executor → Coordinator)
// ─────────────────────────────────────────────────────────

/// Feedback from Executor to Coordinator about order outcomes.
/// Allows Coordinator to reset ghost slots when orders fail or fill completely.
#[derive(Debug, Clone)]
pub enum OrderResult {
    /// Order successfully placed — OrderManager can transition to Live state.
    OrderPlaced {
        slot: OrderSlot,
        target: DesiredTarget,
    },
    /// Order placement failed — Coordinator should reset the slot.
    OrderFailed { slot: OrderSlot, cooldown_ms: u64 },
    /// Executor still sees tracked live orders on this slot.
    /// OMS should reconcile by canceling the stale live slot, not by treating this
    /// as a fresh place failure.
    SlotBusy { slot: OrderSlot },
    /// Order fully filled — Coordinator should release the slot for new orders.
    OrderFilled { slot: OrderSlot },
    /// One-shot taker hedge finished submission path (accepted by venue).
    /// OMS can release pending taker state and continue normal pumping.
    TakerHedgeDone { side: Side },
    /// One-shot taker hedge failed.
    TakerHedgeFailed { side: Side, cooldown_ms: u64 },
    /// Cancel operation for a side completed.
    CancelAck { slot: OrderSlot },
}

/// OMS / executor lifecycle signal that a slot is no longer live on exchange.
#[derive(Debug, Clone, Copy)]
pub struct SlotReleaseEvent {
    pub slot: OrderSlot,
}

/// Lightweight execution feedback channel for Coordinator-side adaptive behavior.
///
/// This is intentionally separate from `OrderResult`:
/// - `OrderResult` drives OMS lifecycle state transitions.
/// - `ExecutionFeedback` informs pricing/risk adaptation without mutating OMS state.
#[derive(Debug, Clone)]
pub enum ExecutionFeedback {
    /// Venue rejected a post-only order because it would have crossed the book.
    PostOnlyCrossed {
        slot: OrderSlot,
        ts: Instant,
        rejected_action_price: f64,
    },
    /// Venue accepted a post-only order; strategy-side sticky reject margin can clear.
    OrderAccepted { slot: OrderSlot, ts: Instant },
    /// Executor repeatedly refused placement because a slot still has tracked open orders.
    /// This is used by strategy-side risk guards (e.g. pair_arb opposite-slot fuse).
    SlotBlocked {
        slot: OrderSlot,
        tracked_orders: usize,
        blocked_for_ms: u64,
        ts: Instant,
    },
    /// Placement reject observed by executor.
    /// Used by strategy-side runtime gates (e.g. oracle_lag round halt on balance rejects).
    PlacementRejected {
        side: Side,
        reason: BidReason,
        kind: RejectKind,
        ts: Instant,
    },
}

/// Rejection class for placement failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectKind {
    RateLimit,
    BalanceOrAllowance,
    PositionUnavailableLag,
    CrossBookTransient,
    Validation,
    Other,
}

/// Placement rejection event for capital/risk side-channels.
#[derive(Debug, Clone)]
pub struct PlacementRejectEvent {
    pub side: Side,
    pub reason: BidReason,
    pub kind: RejectKind,
    pub price: f64,
    pub size: f64,
    pub ts: Instant,
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
    /// Trade direction on this token side.
    pub direction: TradeDirection,
    /// Size filled in THIS event (may be partial).
    pub filled_size: f64,
    /// Fill price.
    pub price: f64,
    /// Fill status from the exchange.
    pub status: FillStatus,
    pub ts: Instant,
}

impl FillEvent {
    pub fn slot(&self) -> OrderSlot {
        OrderSlot::new(self.side, self.direction)
    }
}

/// Inventory update stream consumed by InventoryManager.
#[derive(Debug, Clone)]
pub enum InventoryEvent {
    /// Regular fill event from authenticated User WS.
    Fill(FillEvent),
    /// Local merge synchronization: shrink both YES/NO by one full-set amount.
    /// `full_set_size` uses share units (1 share ~= $1 payout notional at resolution).
    Merge {
        full_set_size: f64,
        merge_id: String,
        ts: Instant,
    },
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
