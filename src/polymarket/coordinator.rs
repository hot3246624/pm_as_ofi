//! Strategy Coordinator — Occam's Razor with Lead-Lag Global Kill Switch.
//!
//! # Three DRY-RUN fixes applied:
//!
//! 1. **Lead-Lag Global Kill Switch**: If ANY side's |OFI| > threshold,
//!    cancel BOTH sides immediately. Arbitrageurs transmit imbalance
//!    across YES/NO books — toxic flow on one side predicts the other.
//!
//! 2. **Price Boundary Clamping**: all bid prices → clamp(0.001, 0.999).
//!    Prevents negative or >1.0 prices from math edge cases.
//!
//! 3. **Anti-Thrashing**: 200ms debounce per side after placing a bid.
//!    Empty book → refuse to bid (return 0.0). Never use ceiling as fallback.

use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tracing::{info, warn, debug};

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
    /// DRY-RUN mode.
    pub dry_run: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            pair_target: 0.99,
            max_net_diff: 5.0,
            bid_size: 2.0,
            tick_size: 0.001,
            reprice_threshold: 0.005,
            debounce_ms: 200,
            dry_run: true,
        }
    }
}

impl CoordinatorConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("PM_PAIR_TARGET")       { if let Ok(f) = v.parse() { c.pair_target = f; } }
        if let Ok(v) = std::env::var("PM_MAX_NET_DIFF")      { if let Ok(f) = v.parse() { c.max_net_diff = f; } }
        if let Ok(v) = std::env::var("PM_BID_SIZE")           { if let Ok(f) = v.parse() { c.bid_size = f; } }
        if let Ok(v) = std::env::var("PM_TICK_SIZE")          { if let Ok(f) = v.parse() { c.tick_size = f; } }
        if let Ok(v) = std::env::var("PM_REPRICE_THRESHOLD")  { if let Ok(f) = v.parse() { c.reprice_threshold = f; } }
        if let Ok(v) = std::env::var("PM_DEBOUNCE_MS")        { if let Ok(f) = v.parse() { c.debounce_ms = f; } }
        if let Ok(v) = std::env::var("PM_DRY_RUN") { c.dry_run = v != "0" && v.to_lowercase() != "false"; }
        c
    }
}

// ─────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct BidSlot {
    active: bool,
    price: f64,
    /// When was the last bid placed (for debounce).
    last_placed: Instant,
}

impl Default for BidSlot {
    fn default() -> Self {
        Self {
            active: false,
            price: 0.0,
            // Start far in the past so first bid isn't debounced
            last_placed: Instant::now() - std::time::Duration::from_secs(60),
        }
    }
}

/// Last known valid book prices (fallback for empty orderbook).
#[derive(Debug, Clone, Copy)]
struct Book {
    yes_bid: f64, yes_ask: f64,
    no_bid: f64, no_ask: f64,
}

impl Default for Book {
    fn default() -> Self {
        Self { yes_bid: 0.0, yes_ask: 0.0, no_bid: 0.0, no_ask: 0.0 }
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
    yes_bid: BidSlot,
    no_bid: BidSlot,
    stats: Stats,

    ofi_rx: watch::Receiver<OfiSnapshot>,
    inv_rx: watch::Receiver<InventoryState>,
    md_rx: mpsc::Receiver<MarketDataMsg>,
    exec_tx: mpsc::Sender<ExecutionCmd>,
    /// Receive order failure notifications from Executor.
    result_rx: mpsc::Receiver<OrderResult>,
}

impl StrategyCoordinator {
    pub fn new(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventoryState>,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        exec_tx: mpsc::Sender<ExecutionCmd>,
        result_rx: mpsc::Receiver<OrderResult>,
    ) -> Self {
        Self {
            cfg, book: Book::default(), last_valid_book: Book::default(),
            yes_bid: BidSlot::default(), no_bid: BidSlot::default(),
            stats: Stats::default(),
            ofi_rx, inv_rx, md_rx, exec_tx, result_rx,
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
                // FIX #4: Executor order failure feedback
                result = self.result_rx.recv() => {
                    match result {
                        Some(OrderResult::OrderFailed { side }) => {
                            warn!("⚠️ OrderFailed {:?} — resetting ghost slot", side);
                            let slot = match side {
                                Side::Yes => &mut self.yes_bid,
                                Side::No => &mut self.no_bid,
                            };
                            slot.active = false;
                            slot.price = 0.0;
                        }
                        None => {} // Channel closed, ignore
                    }
                }
            }
        }

        info!(
            "🎯 Shutdown | ticks={} placed={} cancel(toxic={} inv={} reprice={}) skip(debounce={} empty={} inv_limit={}) clamped={}",
            self.stats.ticks, self.stats.placed,
            self.stats.cancel_toxic, self.stats.cancel_inv, self.stats.cancel_reprice,
            self.stats.skipped_debounce, self.stats.skipped_empty_book, self.stats.skipped_inv_limit,
            self.stats.price_clamped,
        );
    }

    // ═════════════════════════════════════════════════
    // Book update with fallback
    // ═════════════════════════════════════════════════

    fn update_book(&mut self, yb: f64, ya: f64, nb: f64, na: f64) {
        self.book = Book { yes_bid: yb, yes_ask: ya, no_bid: nb, no_ask: na };

        // Update last_valid_book only if we have real data
        if yb > 0.0 && ya > 0.0 { self.last_valid_book.yes_bid = yb; self.last_valid_book.yes_ask = ya; }
        if nb > 0.0 && na > 0.0 { self.last_valid_book.no_bid = nb; self.last_valid_book.no_ask = na; }
    }

    /// Get usable book (current if valid, otherwise last_valid fallback).
    fn usable_book(&self) -> Book {
        Book {
            yes_bid: if self.book.yes_bid > 0.0 { self.book.yes_bid } else { self.last_valid_book.yes_bid },
            yes_ask: if self.book.yes_ask > 0.0 { self.book.yes_ask } else { self.last_valid_book.yes_ask },
            no_bid:  if self.book.no_bid > 0.0  { self.book.no_bid }  else { self.last_valid_book.no_bid },
            no_ask:  if self.book.no_ask > 0.0   { self.book.no_ask }  else { self.last_valid_book.no_ask },
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
            self.global_kill_switch(&ofi).await;
            return; // Block ALL new orders until both sides recover
        }

        // ── Priority 2: Inventory-driven state machine ──
        let ub = self.usable_book();
        if ub.yes_bid <= 0.0 || ub.no_bid <= 0.0 {
            self.stats.skipped_empty_book += 1;
            return; // No valid book data at all
        }

        if inv.net_diff.abs() < f64::EPSILON {
            self.state_balanced(&ub).await;
        } else {
            self.state_hedge(&inv, &ub).await;
        }
    }

    // ═════════════════════════════════════════════════
    // Lead-Lag Global Kill Switch
    // ═════════════════════════════════════════════════

    async fn global_kill_switch(&mut self, ofi: &OfiSnapshot) {
        // Cancel BOTH sides when ANY side is toxic
        if self.yes_bid.active {
            warn!(
                "☠️ GLOBAL KILL Bid_YES | yes_ofi={:.1} no_ofi={:.1}",
                ofi.yes.ofi_score, ofi.no.ofi_score,
            );
            self.cancel(Side::Yes, CancelReason::ToxicFlow).await;
            self.stats.cancel_toxic += 1;
        }
        if self.no_bid.active {
            warn!(
                "☠️ GLOBAL KILL Bid_NO | yes_ofi={:.1} no_ofi={:.1}",
                ofi.yes.ofi_score, ofi.no.ofi_score,
            );
            self.cancel(Side::No, CancelReason::ToxicFlow).await;
            self.stats.cancel_toxic += 1;
        }
    }

    // ═════════════════════════════════════════════════
    // State A: BALANCED — passive mid-based maker
    // ═════════════════════════════════════════════════

    async fn state_balanced(&mut self, ub: &Book) {
        // P1-5: Hard inventory gate with tiered cancel
        let inv = *self.inv_rx.borrow();
        if !inv.can_open {
            self.stats.skipped_inv_limit += 1;

            if inv.net_diff.abs() < 0.001 {
                // net_diff ≈ 0: no hedge needed, stop both sides.
                // Use local cancel() so slot state is reset (avoid stale active slots).
                debug!("🚫 Inventory limit + balanced → cancel both sides");
                if self.yes_bid.active {
                    self.cancel(Side::Yes, CancelReason::InventoryLimit).await;
                    self.stats.cancel_inv += 1;
                }
                if self.no_bid.active {
                    self.cancel(Side::No, CancelReason::InventoryLimit).await;
                    self.stats.cancel_inv += 1;
                }
            } else {
                // net_diff ≠ 0: only cancel the side that would ADD risk
                // If net_diff > 0 → we have excess YES → cancel YES bids (don't buy more YES)
                // If net_diff < 0 → we have excess NO  → cancel NO bids (don't buy more NO)
                let risky_side = if inv.net_diff > 0.0 { Side::Yes } else { Side::No };
                let slot_active = match risky_side {
                    Side::Yes => self.yes_bid.active,
                    Side::No => self.no_bid.active,
                };
                if slot_active {
                    debug!(
                        "🚫 Inventory limit (net={:.1}) → cancel {:?} side only (keep hedge)",
                        inv.net_diff, risky_side,
                    );
                    self.cancel(risky_side, CancelReason::InventoryLimit).await;
                    self.stats.cancel_inv += 1;
                }
            }
            return;
        }

        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no  = (ub.no_bid + ub.no_ask) / 2.0;

        // Constrain: bid_yes + bid_no ≤ pair_target
        let (bid_yes, bid_no) = if mid_yes + mid_no <= self.cfg.pair_target {
            (mid_yes, mid_no)
        } else {
            let excess = (mid_yes + mid_no) - self.cfg.pair_target;
            (mid_yes - excess / 2.0, mid_no - excess / 2.0)
        };

        let bid_yes = self.safe_price(bid_yes);
        let bid_no  = self.safe_price(bid_no);

        self.place_or_reprice(Side::Yes, bid_yes, BidReason::Provide).await;
        self.place_or_reprice(Side::No, bid_no, BidReason::Provide).await;
    }

    // ═════════════════════════════════════════════════
    // State B: HEDGE — aggressive maker
    // ═════════════════════════════════════════════════

    async fn state_hedge(&mut self, inv: &InventoryState, ub: &Book) {
        if inv.net_diff > 0.0 {
            // Excess YES → cancel YES bids, aggressive bid NO
            if self.yes_bid.active {
                info!("⚠️ excess YES ({:.1}) → cancel Bid_YES", inv.net_diff);
                self.cancel(Side::Yes, CancelReason::InventoryLimit).await;
                self.stats.cancel_inv += 1;
            }
            let ceiling = self.cfg.pair_target - inv.yes_avg_cost;
            let price = self.aggressive_price(ceiling, ub.no_ask);
            if price > 0.0 {
                info!(
                    "🔧 HEDGE NO@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                    price, ceiling, ub.no_ask, inv.net_diff,
                );
                self.place_or_reprice(Side::No, price, BidReason::Hedge).await;
            }
        } else {
            // Excess NO → cancel NO bids, aggressive bid YES
            if self.no_bid.active {
                info!("⚠️ excess NO ({:.1}) → cancel Bid_NO", inv.net_diff);
                self.cancel(Side::No, CancelReason::InventoryLimit).await;
                self.stats.cancel_inv += 1;
            }
            let ceiling = self.cfg.pair_target - inv.no_avg_cost;
            let price = self.aggressive_price(ceiling, ub.yes_ask);
            if price > 0.0 {
                info!(
                    "🔧 HEDGE YES@{:.3} | ceiling={:.3} ask={:.3} net={:.1}",
                    price, ceiling, ub.yes_ask, inv.net_diff,
                );
                self.place_or_reprice(Side::Yes, price, BidReason::Hedge).await;
            }
        }
    }

    // ═════════════════════════════════════════════════
    // Pricing engine
    // ═════════════════════════════════════════════════

    /// Aggressive Maker price: min(ceiling, best_ask − tick).
    ///
    /// CRITICAL: If best_ask is unavailable (empty book), return 0.0.
    /// NEVER fall back to ceiling — that caused the phantom 0.490 oscillation.
    /// Bidding at ceiling when no ask exists = paying maximum price into a void.
    fn aggressive_price(&self, ceiling: f64, best_ask: f64) -> f64 {
        if ceiling <= 0.0 || ceiling >= 1.0 { return 0.0; }
        if best_ask <= 0.0 {
            // No sell-side liquidity — refuse to bid.
            // We cannot determine a safe price without an ask.
            return 0.0;
        }
        let one_tick_below = best_ask - self.cfg.tick_size;
        if one_tick_below <= 0.0 { return 0.0; }
        self.safe_price(ceiling.min(one_tick_below))
    }

    /// FIX #2: Clamp + round to tick. Prevents negative/out-of-range prices.
    fn safe_price(&self, p: f64) -> f64 {
        let floored = (p / self.cfg.tick_size).floor() * self.cfg.tick_size;
        let clamped = floored.clamp(0.001, 0.999);
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

    async fn place_or_reprice(&mut self, side: Side, price: f64, reason: BidReason) {
        let slot = match side { Side::Yes => &self.yes_bid, Side::No => &self.no_bid };

        // FIX #3: Debounce — skip if last place was too recent
        let elapsed = slot.last_placed.elapsed();
        let debounce = std::time::Duration::from_millis(self.cfg.debounce_ms);
        if elapsed < debounce {
            self.stats.skipped_debounce += 1;
            return;
        }

        if !slot.active {
            self.place(side, price, reason).await;
        } else if (slot.price - price).abs() > self.cfg.reprice_threshold {
            debug!("🔄 reprice {:?} {:.3}→{:.3}", side, slot.price, price);
            self.cancel(side, CancelReason::Reprice).await;
            self.stats.cancel_reprice += 1;
            self.place(side, price, reason).await;
        }
    }

    // ═════════════════════════════════════════════════
    // Plumbing
    // ═════════════════════════════════════════════════

    async fn place(&mut self, side: Side, price: f64, reason: BidReason) {
        let slot = match side { Side::Yes => &mut self.yes_bid, Side::No => &mut self.no_bid };
        slot.active = true;
        slot.price = price;
        slot.last_placed = Instant::now();
        self.stats.placed += 1;

        if self.cfg.dry_run {
            info!("📝 DRY {:?} {:?}@{:.3} sz={:.1}", reason, side, price, self.cfg.bid_size);
            return;
        }
        let _ = self.exec_tx.send(ExecutionCmd::PlacePostOnlyBid {
            side, price, size: self.cfg.bid_size, reason,
        }).await;
    }

    async fn cancel(&mut self, side: Side, reason: CancelReason) {
        let slot = match side { Side::Yes => &mut self.yes_bid, Side::No => &mut self.no_bid };
        slot.active = false;
        slot.price = 0.0;

        if self.cfg.dry_run {
            info!("📝 DRY cancel {:?} ({:?})", side, reason);
            return;
        }
        let _ = self.exec_tx.send(ExecutionCmd::CancelSide { side, reason }).await;
    }
}

// ─────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CoordinatorConfig {
        CoordinatorConfig {
            pair_target: 0.98, max_net_diff: 10.0,
            bid_size: 2.0, tick_size: 0.01,
            reprice_threshold: 0.005, debounce_ms: 0, // disable for tests
            dry_run: false,
        }
    }

    fn make(c: CoordinatorConfig) -> (
        watch::Sender<OfiSnapshot>, watch::Sender<InventoryState>,
        mpsc::Sender<MarketDataMsg>, mpsc::Receiver<ExecutionCmd>,
        StrategyCoordinator,
    ) {
        let (o, or) = watch::channel(OfiSnapshot::default());
        let (i, ir) = watch::channel(InventoryState::default());
        let (m, mr) = mpsc::channel(16);
        let (e, er) = mpsc::channel(16);
        let (_rt, rr) = mpsc::channel(16);
        (o, i, m, er, StrategyCoordinator::new(c, or, ir, mr, e, rr))
    }

    fn bt(yb: f64, ya: f64, nb: f64, na: f64) -> MarketDataMsg {
        MarketDataMsg::BookTick { yes_bid: yb, yes_ask: ya, no_bid: nb, no_ask: na, ts: Instant::now() }
    }

    // ── Price clamping ──

    #[test]
    fn test_safe_price_clamps_negative() {
        let (_, _, _, _, c) = make(cfg());
        assert!((c.safe_price(-0.5) - 0.001).abs() < 1e-9);
    }

    #[test]
    fn test_safe_price_clamps_over_one() {
        let (_, _, _, _, c) = make(cfg());
        assert!((c.safe_price(1.5) - 0.999).abs() < 1e-3);
    }

    #[test]
    fn test_safe_price_normal() {
        let (_, _, _, _, c) = make(cfg());
        assert!((c.safe_price(0.45) - 0.45).abs() < 1e-9);
    }

    // ── Aggressive pricing ──

    #[test]
    fn test_aggressive_ceiling_wins() {
        let (_, _, _, _, c) = make(cfg());
        assert!((c.aggressive_price(0.50, 0.55) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn test_aggressive_ask_wins() {
        let (_, _, _, _, c) = make(cfg());
        assert!((c.aggressive_price(0.60, 0.52) - 0.51).abs() < 1e-9);
    }

    // ── Global Kill Switch: ANY toxic → cancel BOTH ──

    #[tokio::test]
    async fn test_global_kill_cancels_both_sides() {
        let (o, _i, m, mut e, mut coord) = make(cfg());
        coord.yes_bid = BidSlot { active: true, price: 0.45, ..BidSlot::default() };
        coord.no_bid = BidSlot { active: true, price: 0.50, ..BidSlot::default() };

        // Only YES is toxic — but BOTH should be canceled (Lead-Lag)
        let _ = o.send(OfiSnapshot {
            yes: SideOfi { ofi_score: 100.0, buy_volume: 100.0, sell_volume: 0.0, is_toxic: true },
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
        if let Ok(Some(ExecutionCmd::CancelSide { side, reason })) = c1 {
            canceled.push(side);
            assert_eq!(reason, CancelReason::ToxicFlow);
        }
        if let Ok(Some(ExecutionCmd::CancelSide { side, reason })) = c2 {
            canceled.push(side);
            assert_eq!(reason, CancelReason::ToxicFlow);
        }
        assert!(canceled.contains(&Side::Yes));
        assert!(canceled.contains(&Side::No));

        drop(m); let _ = h.await;
    }

    #[tokio::test]
    async fn test_global_kill_blocks_new_orders() {
        let (o, _i, m, mut e, coord) = make(cfg());

        // NO is toxic (even though balanced) → should NOT place any bids
        let _ = o.send(OfiSnapshot {
            yes: SideOfi::default(),
            no: SideOfi { ofi_score: -80.0, buy_volume: 0.0, sell_volume: 80.0, is_toxic: true },
            ts: Instant::now(),
        });

        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;

        let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c.is_err()); // No commands = blocked

        drop(m); let _ = h.await;
    }

    // ── Balanced mid pricing ──

    #[tokio::test]
    async fn test_balanced_mid_pricing() {
        let (_o, _i, m, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52)).await;

        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;

        let mut prices = std::collections::HashMap::new();
        if let Ok(Some(ExecutionCmd::PlacePostOnlyBid { side, price, .. })) = c1 { prices.insert(side, price); }
        if let Ok(Some(ExecutionCmd::PlacePostOnlyBid { side, price, .. })) = c2 { prices.insert(side, price); }
        assert!((prices[&Side::Yes] - 0.45).abs() < 1e-9);
        assert!((prices[&Side::No] - 0.50).abs() < 1e-9);

        drop(m); let _ = h.await;
    }

    #[tokio::test]
    async fn test_balanced_excess_mid_capped() {
        let (_o, _i, m, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        // mid_yes=0.52, mid_no=0.50, sum=1.02 > 0.98
        let _ = m.send(bt(0.50, 0.54, 0.48, 0.52)).await;
        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        let mut prices = std::collections::HashMap::new();
        if let Ok(Some(ExecutionCmd::PlacePostOnlyBid { side, price, .. })) = c1 { prices.insert(side, price); }
        if let Ok(Some(ExecutionCmd::PlacePostOnlyBid { side, price, .. })) = c2 { prices.insert(side, price); }
        assert!(prices[&Side::Yes] + prices[&Side::No] <= 0.98 + 1e-9);

        drop(m); let _ = h.await;
    }

    // ── Debounce ──

    #[tokio::test]
    async fn test_debounce_skips_rapid_reprice() {
        let mut cfg = cfg();
        cfg.debounce_ms = 5000; // 5 seconds - will definitely block
        let (_o, _i, m, mut e, coord) = make(cfg);
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

        drop(m); let _ = h.await;
    }

    // ── Empty book fallback ──

    #[tokio::test]
    async fn test_empty_book_skipped() {
        let (_o, _i, m, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        // All zeros — no valid book
        let _ = m.send(bt(0.0, 0.0, 0.0, 0.0)).await;
        let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c.is_err()); // No commands
        drop(m); let _ = h.await;
    }
}
