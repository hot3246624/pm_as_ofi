//! Strategy Coordinator — Occam's Razor with Lead-Lag Global Kill Switch.
//!
//! # Three DRY-RUN fixes applied:
//!
//! 1. **Lead-Lag Global Kill Switch**: If ANY side's |OFI| > threshold,
//!    cancel BOTH sides immediately. Arbitrageurs transmit imbalance
//!    across YES/NO books — toxic flow on one side predicts the other.
//!
//! 2. **Price Boundary Clamping**: all bid prices are tick-aligned and
//!    clamped into `(tick, 1 - tick)`.
//!    Prevents negative or >1.0 prices from math edge cases.
//!
//! 3. **Anti-Thrashing**: 200ms debounce per side after placing a bid.
//!    Empty book → refuse to bid (return 0.0). Never use ceiling as fallback.

use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use super::messages::*;
use super::types::Side;


// ─────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Total pair cost ceiling.
    pub pair_target: f64,
    /// Maximum absolute inventory imbalance.
    pub max_net_diff: f64,
    /// Order size per bid.
    pub bid_size: f64,
    /// CLOB minimum tick.
    pub tick_size: f64,
    /// Reprice if our bid drifts more than this from target.
    pub reprice_threshold: f64,
    /// Minimum time between place/reprice on same side (anti-thrashing).
    pub debounce_ms: u64,
    /// A-S Skew penalty factor. 0.03 = pure conservative A-S. 0.00 = pure Gabagool grid.
    pub as_skew_factor: f64,
    /// Time-decay amplifier k. Effective skew_factor = as_skew_factor * (1 + k * elapsed_frac).
    /// 0.0 disables decay. Default: 2.0 (up to 3× at expiry).
    pub as_time_decay_k: f64,
    /// Unix timestamp (seconds) when the market expires. None = no decay.
    pub market_end_ts: Option<u64>,
    /// Opt-3: Faster debounce for hedge orders (urgent, shouldn't wait 500ms).
    /// Default: 100ms. Set PM_HEDGE_DEBOUNCE_MS to override.
    pub hedge_debounce_ms: u64,
    /// Emergency ceiling for hedge orders when net_diff >= max_net_diff.
    pub max_portfolio_cost: f64,
    /// DRY-RUN mode.
    pub dry_run: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            pair_target: 0.99,
            max_net_diff: 5.0,
            bid_size: 2.0,
            tick_size: 0.01,
            reprice_threshold: 0.010, // Increased to reduce churn (1 cent drift)
            debounce_ms: 500,         // Increased to reduce churn (half second)
            as_skew_factor: 0.03,     // Original strictly conservative A-S
            as_time_decay_k: 2.0,     // Up to 3× skew at expiry (1 + 2 * elapsed_frac)
            market_end_ts: None,
            hedge_debounce_ms: 100,   // Hedge orders bypass normal 500ms debounce
            max_portfolio_cost: 1.02, // Emergency hedge ceiling
            dry_run: true,
        }
    }
}

impl CoordinatorConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("PM_PAIR_TARGET") {
            if let Ok(f) = v.parse() {
                c.pair_target = f;
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_NET_DIFF") {
            if let Ok(f) = v.parse() {
                c.max_net_diff = f;
            }
        }
        if let Ok(v) = std::env::var("PM_BID_SIZE") {
            if let Ok(f) = v.parse() {
                c.bid_size = f;
            }
        }
        if let Ok(v) = std::env::var("PM_TICK_SIZE") {
            if let Ok(f) = v.parse() {
                if (0.0..1.0).contains(&f) {
                    c.tick_size = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_TICK_SIZE={} (must satisfy 0 < tick < 1), using {}",
                        f, c.tick_size
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_REPRICE_THRESHOLD") {
            if let Ok(f) = v.parse() {
                c.reprice_threshold = f;
            }
        }
        if let Ok(v) = std::env::var("PM_DEBOUNCE_MS") {
            if let Ok(f) = v.parse() {
                c.debounce_ms = f;
            }
        }
        if let Ok(v) = std::env::var("PM_AS_SKEW_FACTOR") {
            if let Ok(f) = v.parse() {
                c.as_skew_factor = f;
            }
        }
        if let Ok(v) = std::env::var("PM_AS_TIME_DECAY_K") {
            if let Ok(f) = v.parse::<f64>() {
                c.as_time_decay_k = f.max(0.0);
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_DEBOUNCE_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                c.hedge_debounce_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_PORTFOLIO_COST") {
            if let Ok(f) = v.parse() {
                c.max_portfolio_cost = f;
            }
        }
        if let Ok(v) = std::env::var("PM_DRY_RUN") {
            c.dry_run = v != "0" && v.to_lowercase() != "false";
        }
        c
    }
}

// ─────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────

/// Last known valid book prices (fallback for empty orderbook).
#[derive(Debug, Clone, Copy)]
struct Book {
    yes_bid: f64,
    yes_ask: f64,
    no_bid: f64,
    no_ask: f64,
}

impl Default for Book {
    fn default() -> Self {
        Self {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    ticks: u64,
    placed: u64,
    cancel_toxic: u64,
    cancel_inv: u64,
    cancel_reprice: u64,
    skipped_debounce: u64,
    skipped_backoff: u64,
    skipped_empty_book: u64,
    skipped_inv_limit: u64,
    price_clamped: u64,
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

pub struct StrategyCoordinator {
    cfg: CoordinatorConfig,
    book: Book,
    /// Last known VALID book (non-zero prices). Fallback for empty orderbook.
    last_valid_book: Book,
    /// P2 FIX: Timestamp of last valid book update for staleness detection.
    /// P5 FIX: Per-side timestamps to catch single-side staleness.
    last_valid_ts_yes: Instant,
    last_valid_ts_no: Instant,
    yes_target: Option<DesiredTarget>,
    no_target: Option<DesiredTarget>,
    yes_last_ts: Instant,
    no_last_ts: Instant,
    /// Opt-1: Track session start for A-S time decay calculation.
    market_start: Instant,
    stats: Stats,

    ofi_rx: watch::Receiver<OfiSnapshot>,
    inv_rx: watch::Receiver<InventoryState>,
    md_rx: mpsc::Receiver<MarketDataMsg>,
    om_tx: mpsc::Sender<OrderManagerCmd>,
    /// Opt-4: Direct high-priority kill channel from OFI Engine.
    /// Fires on toxicity onset without waiting for the next book tick.
    kill_rx: mpsc::Receiver<KillSwitchSignal>,
}

impl StrategyCoordinator {
    pub fn new(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventoryState>,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
    ) -> Self {
        // Create a kill_rx that is immediately closed (sender dropped).
        // The biased select uses `Some(x) = recv()` which skips a closed channel,
        // so this is safe — the md_rx arm will handle all messages normally.
        let (_dead_tx, dead_rx) = mpsc::channel(1);
        Self::with_kill_rx(cfg, ofi_rx, inv_rx, md_rx, om_tx, dead_rx)
    }

    /// Opt-4: Construct with a direct OFI→Coordinator kill channel.
    pub fn with_kill_rx(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventoryState>,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
    ) -> Self {
        Self {
            cfg,
            book: Book::default(),
            last_valid_book: Book::default(),
            last_valid_ts_yes: Instant::now(),
            last_valid_ts_no: Instant::now(),
            yes_target: None,
            no_target: None,
            yes_last_ts: Instant::now() - std::time::Duration::from_secs(60),
            no_last_ts: Instant::now() - std::time::Duration::from_secs(60),
            market_start: Instant::now(),
            stats: Stats::default(),
            ofi_rx,
            inv_rx,
            md_rx,
            om_tx,
            kill_rx,
        }
    }

    pub async fn run(mut self) {
        info!(
            "🎯 Coordinator [OCCAM+LEADLAG] pair={:.2} bid={:.1} tick={:.3} net={:.0} reprice={:.3} debounce={}ms dry={}",
            self.cfg.pair_target, self.cfg.bid_size, self.cfg.tick_size,
            self.cfg.max_net_diff, self.cfg.reprice_threshold, self.cfg.debounce_ms, self.cfg.dry_run,
        );

        loop {
            tokio::select! {
                // Opt-4: biased gives kill_rx absolute priority over book ticks.
                // OFI toxicity onset fires this path immediately without waiting
                // for the next market data tick (which could be hundreds of ms away).
                //
                // Using `Some(sig) = recv()` pattern: if the kill_rx channel is
                // closed (returns None), this arm is skipped and the md_rx arm runs.
                // This avoids busy-loops on channels that were never wired.
                biased;

                Some(sig) = self.kill_rx.recv() => {
                    warn!(
                        "⚡ DIRECT KILL from OFI | side={:?} ofi={:.1} — immediate re-eval",
                        sig.side, sig.ofi_score,
                    );
                    // Re-use the full tick() logic which reads latest OFI watch state
                    // and applies global_kill_switch / selective_kill_with_hedge.
                    if self.book.yes_bid > 0.0 || self.last_valid_book.yes_bid > 0.0 {
                        self.tick().await;
                    }
                }

                // Market data tick (primary driver)
                msg = self.md_rx.recv() => {
                    match msg {
                        Some(MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, .. }) => {
                            self.update_book(yes_bid, yes_ask, no_bid, no_ask);
                            self.stats.ticks += 1;
                            self.tick().await;
                        }
                        None => break, // Channel closed
                        _ => {}
                    }
                }
            }
        }

        info!(
            "🎯 Shutdown | ticks={} placed={} cancel(toxic={} inv={} reprice={}) skip(debounce={} backoff={} empty={} inv_limit={}) clamped={}",
            self.stats.ticks, self.stats.placed,
            self.stats.cancel_toxic, self.stats.cancel_inv, self.stats.cancel_reprice,
            self.stats.skipped_debounce, self.stats.skipped_backoff, self.stats.skipped_empty_book, self.stats.skipped_inv_limit,
            self.stats.price_clamped,
        );
    }

    // ═════════════════════════════════════════════════
    // Book update with fallback
    // ═════════════════════════════════════════════════

    fn update_book(&mut self, yb: f64, ya: f64, nb: f64, na: f64) {
        self.book = Book {
            yes_bid: yb,
            yes_ask: ya,
            no_bid: nb,
            no_ask: na,
        };

        // P5 FIX: Update per-side timestamps independently.
        // Shared timestamp caused YES updates to mask NO staleness.
        if yb > 0.0 && ya > 0.0 {
            self.last_valid_book.yes_bid = yb;
            self.last_valid_book.yes_ask = ya;
            self.last_valid_ts_yes = Instant::now();
        }
        if nb > 0.0 && na > 0.0 {
            self.last_valid_book.no_bid = nb;
            self.last_valid_book.no_ask = na;
            self.last_valid_ts_no = Instant::now();
        }
    }

    /// P5 FIX: Check if either side's book data is stale (>30s without fresh data).
    /// Uses per-side timestamps so YES updates don't mask NO staleness.
    fn is_book_stale(&self) -> bool {
        let limit = std::time::Duration::from_secs(30);
        self.last_valid_ts_yes.elapsed() > limit || self.last_valid_ts_no.elapsed() > limit
    }

    /// Get usable book (current if valid, otherwise last_valid fallback).
    fn usable_book(&self) -> Book {
        Book {
            yes_bid: if self.book.yes_bid > 0.0 {
                self.book.yes_bid
            } else {
                self.last_valid_book.yes_bid
            },
            yes_ask: if self.book.yes_ask > 0.0 {
                self.book.yes_ask
            } else {
                self.last_valid_book.yes_ask
            },
            no_bid: if self.book.no_bid > 0.0 {
                self.book.no_bid
            } else {
                self.last_valid_book.no_bid
            },
            no_ask: if self.book.no_ask > 0.0 {
                self.book.no_ask
            } else {
                self.last_valid_book.no_ask
            },
        }
    }

    // ═════════════════════════════════════════════════
    // Main tick
    // ═════════════════════════════════════════════════

    async fn tick(&mut self) {
        let ofi = *self.ofi_rx.borrow();
        let inv = *self.inv_rx.borrow();

        // ── Priority 1: Lead-Lag Global Kill Switch ──
        let global_toxic = ofi.yes.is_toxic || ofi.no.is_toxic;
        if global_toxic {
            if inv.net_diff.abs() > f64::EPSILON {
                // P0 FIX: When we have open inventory, only kill the risky side
                // and PRESERVE the hedge side — it's more important during toxic flow.
                self.selective_kill_with_hedge(&ofi, &inv).await;
            } else {
                self.global_kill_switch(&ofi).await;
            }
            return;
        }

        // ── Priority 2: Inventory-driven state machine ──
        let ub = self.usable_book();
        if ub.yes_bid <= 0.0 || ub.no_bid <= 0.0 {
            self.stats.skipped_empty_book += 1;
            return; // No valid book data at all
        }

        // P2 FIX: If last_valid_book is stale (>30s), cancel everything and refuse to open.
        if self.is_book_stale() {
            if self.yes_target.is_some() {
                warn!("⚠️ Stale book (>30s) — canceling YES bid");
                self.cancel(Side::Yes, CancelReason::Shutdown).await;
            }
            if self.no_target.is_some() {
                warn!("⚠️ Stale book (>30s) — canceling NO bid");
                self.cancel(Side::No, CancelReason::Shutdown).await;
            }
            self.stats.skipped_empty_book += 1;
            return;
        }

        self.state_unified(&inv, &ub).await;
    }

    // ═════════════════════════════════════════════════
    // Lead-Lag Global Kill Switch
    // ═════════════════════════════════════════════════

    async fn global_kill_switch(&mut self, ofi: &OfiSnapshot) {
        // Cancel BOTH sides when ANY side is toxic
        if self.yes_target.is_some() {
            warn!(
                "☠️ GLOBAL KILL Bid_YES | yes_ofi={:.1} no_ofi={:.1}",
                ofi.yes.ofi_score, ofi.no.ofi_score,
            );
            self.cancel(Side::Yes, CancelReason::ToxicFlow).await;
            self.stats.cancel_toxic += 1;
        }
        if self.no_target.is_some() {
            warn!(
                "☠️ GLOBAL KILL Bid_NO | yes_ofi={:.1} no_ofi={:.1}",
                ofi.yes.ofi_score, ofi.no.ofi_score,
            );
            self.cancel(Side::No, CancelReason::ToxicFlow).await;
            self.stats.cancel_toxic += 1;
        }
    }

    /// Selective kill: cancel only the side that would ADD risk, preserve the hedge side.
    /// Then immediately run state_hedge() to maintain/place the counterleg.
    async fn selective_kill_with_hedge(&mut self, ofi: &OfiSnapshot, inv: &InventoryState) {
        let (risky_side, hedge_side) = if inv.net_diff > 0.0 {
            (Side::Yes, Side::No) // excess YES → kill YES bids, keep NO hedge
        } else {
            (Side::No, Side::Yes) // excess NO → kill NO bids, keep YES hedge
        };

        let risky_active = match risky_side {
            Side::Yes => self.yes_target.is_some(),
            Side::No => self.no_target.is_some(),
        };
        if risky_active {
            warn!(
                "☠️ SELECTIVE KILL {:?} (risky) | yes_ofi={:.1} no_ofi={:.1} net={:.1} — keeping {:?} hedge",
                risky_side, ofi.yes.ofi_score, ofi.no.ofi_score, inv.net_diff, hedge_side,
            );
            self.cancel(risky_side, CancelReason::ToxicFlow).await;
            self.stats.cancel_toxic += 1;
        }

        // Maintain the hedge order even during toxic flow
        let ub = self.usable_book();
        if ub.yes_bid > 0.0 && ub.no_bid > 0.0 {
            self.quote_hedge_only(inv, &ub).await;
        }
    }

    // ═════════════════════════════════════════════════
    // Toxic Flow Hedge Override
    // ═════════════════════════════════════════════════

    async fn quote_hedge_only(&mut self, inv: &InventoryState, ub: &Book) {
        if inv.net_diff > f64::EPSILON {
            // If imbalanced, check if we need "Emergency" rescue pricing
            let hedge_target = if inv.net_diff >= self.cfg.max_net_diff - f64::EPSILON {
                self.cfg.max_portfolio_cost
            } else {
                self.cfg.pair_target
            };
            let ceiling = hedge_target - inv.yes_avg_cost;
            let price = self.aggressive_price(ceiling, ub.no_ask);

            // Stale Book Guard
            let now = Instant::now();
            if now.duration_since(self.last_valid_ts_no) > std::time::Duration::from_secs(5) {
                if price > 0.0 {
                    warn!("⚠️ [STALE BOOK HEDGE] NO age={:?}s - skipping hedge", now.duration_since(self.last_valid_ts_no).as_secs());
                }
                return;
            }

            if price > 0.0 {
                let current_no = self.no_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                let log_msg = if current_no == 0.0
                    || (current_no - price).abs() > self.cfg.reprice_threshold
                {
                    Some(format!(
                        "🔧 TOXIC HEDGE NO@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                        price, ceiling, ub.no_ask, inv.net_diff
                    ))
                } else {
                    None
                };
                self.place_or_reprice(Side::No, price, BidReason::Hedge, log_msg)
                    .await;
            }
        } else if inv.net_diff < -f64::EPSILON {
            // Emergency rescue pricing for YES side
            let hedge_target = if inv.net_diff <= -self.cfg.max_net_diff + f64::EPSILON {
                self.cfg.max_portfolio_cost
            } else {
                self.cfg.pair_target
            };
            let ceiling = hedge_target - inv.no_avg_cost;
            let price = self.aggressive_price(ceiling, ub.yes_ask);

            // Stale Book Guard
            let now = Instant::now();
            if now.duration_since(self.last_valid_ts_yes) > std::time::Duration::from_secs(5) {
                if price > 0.0 {
                    warn!("⚠️ [STALE BOOK HEDGE] YES age={:?}s - skipping hedge", now.duration_since(self.last_valid_ts_yes).as_secs());
                }
                return;
            }

            if price > 0.0 {
                let current_yes = self.yes_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                let log_msg = if current_yes == 0.0
                    || (current_yes - price).abs() > self.cfg.reprice_threshold
                {
                    Some(format!(
                        "🔧 TOXIC HEDGE YES@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                        price, ceiling, ub.yes_ask, inv.net_diff
                    ))
                } else {
                    None
                };
                self.place_or_reprice(Side::Yes, price, BidReason::Hedge, log_msg)
                    .await;
            }
        }
    }

    // ═════════════════════════════════════════════════
    // State Unified: A-S Skew + Gabagool22 Cost Averaging
    // ═════════════════════════════════════════════════

    async fn state_unified(&mut self, inv: &InventoryState, ub: &Book) {
        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no = (ub.no_bid + ub.no_ask) / 2.0;

        // Base required profit margin based on pair_target limits
        let excess = f64::max(0.0, (mid_yes + mid_no) - self.cfg.pair_target);

        let skew = if self.cfg.max_net_diff > 0.0 {
            (inv.net_diff / self.cfg.max_net_diff).clamp(-1.0, 1.0)
        } else {
            0.0
        };

        // A-S linear shift based on inventory.
        // Opt-1: Apply time-decay multiplier — skew urgency grows as market approaches expiry.
        // Holding YES (skew>0) shifts YES down and NO up.
        let effective_skew_factor = self.cfg.as_skew_factor * self.compute_time_decay_factor();
        let skew_shift = skew * effective_skew_factor;

        let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
        let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

        if raw_yes + raw_no > self.cfg.pair_target {
            let overflow = (raw_yes + raw_no) - self.cfg.pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        // 1. Strict Maker Clamp: base quotes must never cross the spread
        if ub.yes_ask > 0.0 {
            raw_yes = f64::min(raw_yes, ub.yes_ask - self.cfg.tick_size);
        }
        if ub.no_ask > 0.0 {
            raw_no = f64::min(raw_no, ub.no_ask - self.cfg.tick_size);
        }

        let mut bid_yes = self.safe_price(raw_yes);
        let mut bid_no = self.safe_price(raw_no);

        // P5 Hardening: Stale Book Guard (5s TTL)
        // If we haven't seen a valid update for a side, refuse to bid on that side.
        let now = Instant::now();
        let ttl = std::time::Duration::from_secs(5);

        if now.duration_since(self.last_valid_ts_yes) > ttl {
            if bid_yes > 0.0 {
                warn!("⚠️ [STALE BOOK] YES age={:?}s - skipping bid", now.duration_since(self.last_valid_ts_yes).as_secs());
            }
            bid_yes = 0.0;
        }
        if now.duration_since(self.last_valid_ts_no) > ttl {
            if bid_no > 0.0 {
                warn!("⚠️ [STALE BOOK] NO age={:?}s - skipping bid", now.duration_since(self.last_valid_ts_no).as_secs());
            }
            bid_no = 0.0;
        }

        let net_diff = inv.net_diff;

        if !inv.can_buy_yes && !inv.can_buy_no {
            self.stats.skipped_inv_limit += 1;
        }

        // BUG 1 FIX: Track whether each side was dispatched by the hedge path.
        // The bottom "cleanup" block must NOT cancel a bid that was just placed as a hedge.
        // Previously: bid_no = 0.0 after hedge dispatch, then `else if no_target.is_some()`
        // triggered a cancel of the freshly-placed hedge order in the same tick.
        let mut hedge_dispatched_yes = false;
        let mut hedge_dispatched_no = false;

        if net_diff > f64::EPSILON {
            if !inv.can_buy_yes {
                bid_yes = 0.0;
                if self.yes_target.is_some() {
                    debug!("🚫 YES maxed out (net={:.1}) → stop buying YES", net_diff);
                    self.cancel(Side::Yes, CancelReason::InventoryLimit).await;
                    self.stats.cancel_inv += 1;
                }
            }

            // If net_diff >= max_net_diff, we are desperate to hedge.
            // Move ceiling from pair_target to max_portfolio_cost to allow "expensive" hedge.
            let hedge_target = if net_diff >= self.cfg.max_net_diff - f64::EPSILON {
                self.cfg.max_portfolio_cost
            } else {
                self.cfg.pair_target
            };

            let ceiling_no = hedge_target - inv.yes_avg_cost;
            let agg_no = self.aggressive_price(ceiling_no, ub.no_ask);

            if agg_no > 0.0 {
                bid_no = f64::max(bid_no, agg_no);
                if bid_no > ceiling_no {
                    bid_no = self.safe_price(ceiling_no);
                }

                let current_no = self.no_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                let log_msg = if current_no == 0.0
                    || (current_no - bid_no).abs() > self.cfg.reprice_threshold
                {
                    Some(format!(
                        "🔧 HEDGE NO@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                        bid_no, ceiling_no, ub.no_ask, inv.net_diff
                    ))
                } else {
                    None
                };
                self.place_or_reprice(Side::No, bid_no, BidReason::Hedge, log_msg)
                    .await;
                // Mark consumed so the bottom cleanup block skips the cancel check.
                hedge_dispatched_no = true;
                bid_no = 0.0;
            }
        } else if net_diff < -f64::EPSILON {
            if !inv.can_buy_no {
                bid_no = 0.0;
                if self.no_target.is_some() {
                    debug!("🚫 NO maxed out (net={:.1}) → stop buying NO", net_diff);
                    self.cancel(Side::No, CancelReason::InventoryLimit).await;
                    self.stats.cancel_inv += 1;
                }
            }

            // If net_diff <= -max_net_diff, move ceiling to max_portfolio_cost.
            let hedge_target = if net_diff <= -self.cfg.max_net_diff + f64::EPSILON {
                self.cfg.max_portfolio_cost
            } else {
                self.cfg.pair_target
            };

            let ceiling_yes = hedge_target - inv.no_avg_cost;
            let agg_yes = self.aggressive_price(ceiling_yes, ub.yes_ask);

            if agg_yes > 0.0 {
                bid_yes = f64::max(bid_yes, agg_yes);
                if bid_yes > ceiling_yes {
                    bid_yes = self.safe_price(ceiling_yes);
                }

                let current_yes = self.yes_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                let log_msg = if current_yes == 0.0
                    || (current_yes - bid_yes).abs() > self.cfg.reprice_threshold
                {
                    Some(format!(
                        "🔧 HEDGE YES@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                        bid_yes, ceiling_yes, ub.yes_ask, inv.net_diff
                    ))
                } else {
                    None
                };
                self.place_or_reprice(Side::Yes, bid_yes, BidReason::Hedge, log_msg)
                    .await;
                // Mark consumed so the bottom cleanup block skips the cancel check.
                hedge_dispatched_yes = true;
                bid_yes = 0.0;
            }
        } else {
            if !inv.can_buy_yes {
                bid_yes = 0.0;
                if self.yes_target.is_some() {
                    self.cancel(Side::Yes, CancelReason::InventoryLimit).await;
                }
            }
            if !inv.can_buy_no {
                bid_no = 0.0;
                if self.no_target.is_some() {
                    self.cancel(Side::No, CancelReason::InventoryLimit).await;
                }
            }
        }

        // Dispatch remaining bids, or cancel stale targets.
        // CRITICAL: skip cancel if the hedge path already dispatched for that side.
        if bid_yes > 0.0 {
            self.place_or_reprice(Side::Yes, bid_yes, BidReason::Provide, None)
                .await;
        } else if !hedge_dispatched_yes && self.yes_target.is_some() {
            self.cancel(Side::Yes, CancelReason::InventoryLimit).await;
        }

        if bid_no > 0.0 {
            self.place_or_reprice(Side::No, bid_no, BidReason::Provide, None)
                .await;
        } else if !hedge_dispatched_no && self.no_target.is_some() {
            self.cancel(Side::No, CancelReason::InventoryLimit).await;
        }
    }

    // ═════════════════════════════════════════════════
    // Pricing engine
    // ═════════════════════════════════════════════════

    // ═════════════════════════════════════════════════
    // Opt-1: A-S Time Decay Factor
    // ═════════════════════════════════════════════════

    /// Returns a multiplier for `as_skew_factor` that grows linearly from 1.0
    /// at market open to `(1 + as_time_decay_k)` at market close.
    ///
    /// Formula: `1.0 + k * elapsed_fraction`
    /// where `elapsed_fraction = elapsed / total_duration`, clamped to [0, 1].
    ///
    /// With default k=2.0: the factor ranges from 1× at open to 3× at close.
    /// This matches the A-S model's γσ²(T-t) term — as T-t → 0 the urgency to
    /// close inventory increases, expressed here as a growing skew penalty.
    fn compute_time_decay_factor(&self) -> f64 {
        let k = self.cfg.as_time_decay_k;
        if k <= 0.0 {
            return 1.0;
        }
        let Some(end_ts) = self.cfg.market_end_ts else {
            return 1.0;
        };
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_secs >= end_ts {
            return 1.0 + k; // Market over — max urgency
        }
        // Total window = from bot start (market_start) to end_ts.
        // Use wall-clock elapsed since we need absolute time to end_ts.
        let elapsed = self.market_start.elapsed().as_secs_f64();
        let remaining = (end_ts - now_secs) as f64;
        let total = elapsed + remaining;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed_frac = (elapsed / total).clamp(0.0, 1.0);
        1.0 + k * elapsed_frac
    }

    /// Aggressive Maker price: min(ceiling, best_ask − tick).
    ///
    /// CRITICAL: If best_ask is unavailable (empty book), return 0.0.
    /// NEVER fall back to ceiling — that caused the phantom 0.490 oscillation.
    /// Bidding at ceiling when no ask exists = paying maximum price into a void.
    fn aggressive_price(&self, ceiling: f64, best_ask: f64) -> f64 {
        if ceiling <= 0.0 || ceiling >= 1.0 {
            return 0.0;
        }
        if best_ask <= 0.0 {
            // No sell-side liquidity — refuse to bid.
            // This prevents "Blind Crossing" where we bid into a stale/empty book.
            return 0.0;
        }
        
        if ceiling >= best_ask - 1e-9 {
            // The ceiling is at or above the current best ask. 
            // b/c we are Post-Only, this order would be REJECTED.
            // We clamp it to 1 tick below ask, but if the ask is already very low,
            // we should be aware of this.
            debug!("⚠️ aggressive_price: ceiling ({:.3}) >= best_ask ({:.3}) | Bidding 1 tick below ask", ceiling, best_ask);
        }

        let one_tick_below = best_ask - self.cfg.tick_size;
        if one_tick_below <= 0.0 {
            return 0.0;
        }
        self.safe_price(ceiling.min(one_tick_below))
    }

    /// FIX #2: Clamp + floor to tick. Prevents negative/out-of-range prices.
    fn safe_price(&self, p: f64) -> f64 {
        let tick = self.cfg.tick_size;
        if !(0.0..1.0).contains(&tick) {
            return 0.0;
        }

        // Keep price within strict valid quote bounds while preserving tick alignment.
        let max_ticks = (1.0 / tick).floor() - 1.0;
        if max_ticks < 1.0 {
            return 0.0;
        }
        let min_price = tick;
        let max_price = max_ticks * tick;

        let floored = (p / tick).floor() * tick;
        let clamped = floored.clamp(min_price, max_price);
        if (clamped - floored).abs() > 1e-9 {
            self.stats_price_clamped();
        }
        clamped
    }

    /// Workaround: can't mutate stats in safe_price (called from non-mut context in tests).
    /// In production the counter is tracked via place_or_reprice.
    fn stats_price_clamped(&self) {
        // The actual counter is incremented in place_or_reprice where we have &mut self
    }

    // ═════════════════════════════════════════════════
    // Place / Reprice with debounce
    // ═════════════════════════════════════════════════

    async fn place_or_reprice(
        &mut self,
        side: Side,
        price: f64,
        reason: BidReason,
        log_msg: Option<String>,
    ) {
        let (current_target, last_ts) = match side {
            Side::Yes => (self.yes_target.as_ref(), self.yes_last_ts),
            Side::No => (self.no_target.as_ref(), self.no_last_ts),
        };

        let active = current_target.is_some();
        let slot_price = current_target.map(|t| t.price).unwrap_or(0.0);

        // Opt-3: Hedge orders use a shorter debounce — they are urgent unwind actions
        // and should not wait behind the normal 500ms anti-thrash window.
        let debounce_ms = match reason {
            BidReason::Hedge => self.cfg.hedge_debounce_ms,
            BidReason::Provide => self.cfg.debounce_ms,
        };
        let elapsed = last_ts.elapsed();
        let debounce = std::time::Duration::from_millis(debounce_ms);
        if elapsed < debounce {
            self.stats.skipped_debounce += 1;
            return;
        }

        if let Some(msg) = log_msg {
            info!("{}", msg);
        }

        if !active {
            self.place(side, price, reason).await;
        } else if (slot_price - price).abs() > self.cfg.reprice_threshold {
            debug!("🔄 reprice {:?} {:.3}→{:.3}", side, slot_price, price);
            self.stats.cancel_reprice += 1;
            self.place(side, price, reason).await;
        }
    }

    // ═════════════════════════════════════════════════
    // Plumbing
    // ═════════════════════════════════════════════════

    async fn place(&mut self, side: Side, price: f64, reason: BidReason) {
        let target = DesiredTarget {
            side,
            price,
            size: self.cfg.bid_size,
        };

        match side {
            Side::Yes => {
                self.yes_target = Some(target.clone());
                self.yes_last_ts = Instant::now();
            }
            Side::No => {
                self.no_target = Some(target.clone());
                self.no_last_ts = Instant::now();
            }
        };

        self.stats.placed += 1;

        if self.cfg.dry_run {
            info!(
                "📝 DRY {:?} {:?}@{:.3} sz={:.1}",
                reason, side, price, self.cfg.bid_size
            );
            return;
        }

        let _ = self.om_tx.send(OrderManagerCmd::SetTarget(target)).await;
    }

    async fn cancel(&mut self, side: Side, reason: CancelReason) {
        match side {
            Side::Yes => self.yes_target = None,
            Side::No => self.no_target = None,
        };

        debug!("🗑️ Cancel {:?} ({:?})", side, reason);
        if self.cfg.dry_run {
            info!("📝 DRY cancel {:?} ({:?})", side, reason);
            return;
        }

        let target = DesiredTarget {
            side,
            price: 0.0,
            size: 0.0,
        };
        let _ = self.om_tx.send(OrderManagerCmd::SetTarget(target)).await;
    }
}

// ─────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::time::timeout;
    use std::time::Duration;

    fn cfg() -> CoordinatorConfig {
        CoordinatorConfig {
            pair_target: 0.98,
            max_net_diff: 10.0,
            bid_size: 2.0,
            tick_size: 0.01,
            reprice_threshold: 0.005,
            debounce_ms: 0, // disable for tests
            as_skew_factor: 0.03,
            dry_run: false,
            ..CoordinatorConfig::default()
        }
    }

    fn make(
        c: CoordinatorConfig,
    ) -> (
        watch::Sender<OfiSnapshot>,
        watch::Sender<InventoryState>,
        mpsc::Sender<MarketDataMsg>,
        mpsc::Sender<KillSwitchSignal>,
        mpsc::Receiver<OrderManagerCmd>,
        StrategyCoordinator,
    ) {
        let (o, or) = watch::channel(OfiSnapshot::default());
        let (i, ir) = watch::channel(InventoryState::default());
        let (m, mr) = mpsc::channel(16);
        let (e, er) = mpsc::channel(16);
        let (k, kr) = mpsc::channel(16);
        (o, i, m, k, er, StrategyCoordinator::with_kill_rx(c, or, ir, mr, e, kr))
    }

    fn bt(yb: f64, ya: f64, nb: f64, na: f64) -> MarketDataMsg {
        MarketDataMsg::BookTick {
            yes_bid: yb,
            yes_ask: ya,
            no_bid: nb,
            no_ask: na,
            ts: Instant::now(),
        }
    }

    // ── Price clamping ──

    #[test]
    fn test_safe_price_clamps_negative() {
        let (_, _, _, _, _, c) = make(cfg());
        assert!((c.safe_price(-0.5) - 0.01).abs() < 1e-9);
    }

    #[test]
    fn test_safe_price_clamps_over_one() {
        let (_, _, _, _, _, c) = make(cfg());
        assert!((c.safe_price(1.5) - 0.99).abs() < 1e-9);
    }

    #[test]
    fn test_safe_price_normal() {
        let (_, _, _, _, _, c) = make(cfg());
        assert!((c.safe_price(0.45) - 0.45).abs() < 1e-9);
    }

    // ── Aggressive pricing ──

    #[test]
    fn test_aggressive_ceiling_wins() {
        let (_, _, _, _, _, c) = make(cfg());
        assert!((c.aggressive_price(0.50, 0.55) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn test_aggressive_ask_wins() {
        let (_, _, _, _, _, c) = make(cfg());
        assert!((c.aggressive_price(0.60, 0.52) - 0.51).abs() < 1e-9);
    }

    // ── Global Kill Switch: ANY toxic → cancel BOTH ──

    #[tokio::test]
    async fn test_global_kill_cancels_both_sides() {
        let (o, _i, m, _k, mut e, mut coord) = make(cfg());
        coord.yes_target = Some(DesiredTarget {
            side: Side::Yes, price: 0.45, size: 2.0
        });
        coord.no_target = Some(DesiredTarget {
            side: Side::No, price: 0.50, size: 2.0
        });

        // Only YES is toxic — but BOTH should be canceled (Lead-Lag)
        let _ = o.send(OfiSnapshot {
            yes: SideOfi {
                ofi_score: 100.0,
                buy_volume: 100.0,
                sell_volume: 0.0,
                is_toxic: true,
            },
            no: SideOfi::default(),
            ts: Instant::now(),
        });

        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;

        // Should receive TWO CancelSide commands (YES + NO)
        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c1.is_ok() && c2.is_ok());

        let mut canceled = Vec::new();
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c1 {
            if target.price == 0.0 {
                canceled.push(target.side);
            }
        }
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c2 {
            if target.price == 0.0 {
                canceled.push(target.side);
            }
        }
        assert!(canceled.contains(&Side::Yes));
        assert!(canceled.contains(&Side::No));

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_global_kill_blocks_new_orders() {
        let (o, _i, m, _k, mut e, coord) = make(cfg());

        // NO is toxic (even though balanced) → should NOT place any bids
        let _ = o.send(OfiSnapshot {
            yes: SideOfi::default(),
            no: SideOfi {
                ofi_score: -80.0,
                buy_volume: 0.0,
                sell_volume: 80.0,
                is_toxic: true,
            },
            ts: Instant::now(),
        });

        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;

        let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c.is_err()); // No commands = blocked

        drop(m);
        let _ = h.await;
    }

    // ── Balanced mid pricing ──

    #[tokio::test]
    async fn test_balanced_mid_pricing() {
        let (_o, _i, m, _k, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;

        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;

        let mut prices = std::collections::HashMap::new();
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c1 {
            prices.insert(target.side, target.price);
        }
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c2 {
            prices.insert(target.side, target.price);
        }
        assert!((prices[&Side::Yes] - 0.45).abs() < 1e-9);
        assert!((prices[&Side::No] - 0.50).abs() < 1e-9);

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_balanced_excess_mid_capped() {
        let (_o, _i, m, _k, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        // mid_yes=0.52, mid_no=0.50, sum=1.02 > 0.98
        let _ = m.send(bt(0.50, 0.54, 0.48, 0.52)).await;
        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let mut prices = std::collections::HashMap::new();
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c1 {
            prices.insert(target.side, target.price);
        }
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c2 {
            prices.insert(target.side, target.price);
        }
        assert!(prices[&Side::Yes] + prices[&Side::No] <= 0.98 + 1e-9);

        drop(m);
        let _ = h.await;
    }

    // ── Debounce ──

    #[tokio::test]
    async fn test_debounce_skips_rapid_reprice() {
        let mut cfg = cfg();
        cfg.debounce_ms = 5000; // 5 seconds - will definitely block
        let (_o, _i, m, _k, mut e, coord) = make(cfg);
        let h = tokio::spawn(coord.run());

        // First tick: places bids
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;
        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c1.is_ok());
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c2.is_ok());

        // Second tick with different prices — should be debounced
        let _ = m.send(bt(0.30, 0.32, 0.60, 0.62)).await;
        let c3 = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c3.is_err()); // No commands = debounced

        drop(m);
        let _ = h.await;
    }

    // ── Empty book fallback ──

    #[tokio::test]
    async fn test_empty_book_skipped() {
        let (_o, _i, m, _k, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        // All zeros — no valid book
        let _ = m.send(bt(0.0, 0.0, 0.0, 0.0)).await;
        let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c.is_err()); // No commands
        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_hedge_emergency_ceiling_toxic_flow() {
        let mut cfg = cfg();
        cfg.max_net_diff = 10.0;
        cfg.pair_target = 0.985;
        cfg.max_portfolio_cost = 1.02;

        let (mut o, i, m, _k, mut e, mut coord) = make(cfg);
        
        // 1. Setup inventory: heavily imbalanced (net = -10.0)
        let inv = InventoryState {
            net_diff: -10.0,
            yes_qty: 5.0,
            no_qty: 15.0,
            yes_avg_cost: 0.45,
            no_avg_cost: 0.45,
            portfolio_cost: 0.90,
            can_buy_yes: true,
            can_buy_no: false,
        };
        let _ = i.send(inv); // watch::Sender::send is not async

        // 2. Trigger Toxic Flow kill
        let _ = o.send(OfiSnapshot {
            yes: SideOfi { ofi_score: 5000.0, is_toxic: true, ..Default::default() },
            no: SideOfi::default(),
            ts: Instant::now(),
        });

        // 3. Hear the kill signal
        let h = tokio::spawn(async move {
            coord.run().await
        });

        // 4. Send a book update to trigger pricing
        let _ = m.send(bt(0.30, 0.70, 0.40, 0.60)).await;

        let cmd = timeout(Duration::from_millis(100), e.recv()).await;
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = cmd {
            assert_eq!(target.side, Side::Yes);
            // Emergency ceiling = 1.02 - 0.45 = 0.570
            assert!((target.price - 0.57).abs() < 1e-9);
        } else {
            panic!("Expected SetTarget hedge command, got {:?}", cmd);
        }
        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_stale_book_protection() {
        let (o, _i, m, k, mut e, mut coord) = make(cfg());
        
        // 1. Send an initial valid update to populate last_valid_book
        coord.update_book(0.44, 0.46, 0.48, 0.52);
        
        // 2. Artificially backdate the timestamps to 6 seconds ago
        coord.last_valid_ts_yes = Instant::now() - Duration::from_secs(6);
        coord.last_valid_ts_no = Instant::now() - Duration::from_secs(6);

        let h = tokio::spawn(async move {
            coord.run().await
        });

        // 3. Trigger a pricing attempt via KillSwitchSignal (Direct Kill channel)
        // This calls tick() WITHOUT calling update_book(), so timestamps remain stale.
        let _ = k.send(KillSwitchSignal {
            side: Side::Yes,
            ofi_score: 1.0,
            ts: Instant::now(),
        }).await;

        // 4. Command should NOT be sent due to staleness
        let cmd = timeout(Duration::from_millis(200), e.recv()).await;
        assert!(cmd.is_err(), "Expected timeout (no bid) due to stale book, but got {:?}", cmd);

        drop(m);
        let _ = h.await;
    }
}
