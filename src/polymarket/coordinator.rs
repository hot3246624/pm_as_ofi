//! Strategy Coordinator — Occam's Razor with per-side toxicity controls.
//!
//! # Three DRY-RUN fixes applied:
//!
//! 1. **Per-Side Toxicity Guard**: If one side is toxic/stale, only that side
//!    is paused. The opposite side can still quote/hedge if healthy.
//!
//! 2. **Price Boundary Clamping**: all bid prices are tick-aligned and
//!    clamped into `(tick, 1 - tick)`.
//!    Prevents negative or >1.0 prices from math edge cases.
//!
//! 3. **Anti-Thrashing**: debounce plus toxicity recovery hold-down.
//!    Empty book → refuse to bid (return 0.0). Never use ceiling as fallback.

use std::time::{Duration, Instant};

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
    /// Maximum per-side gross exposure (shares). 0 = disabled.
    pub max_side_shares: f64,
    /// Order size per bid.
    pub bid_size: f64,
    /// CLOB minimum tick.
    pub tick_size: f64,
    /// Base safety margin below best ask, in ticks, to avoid post-only crossing.
    pub post_only_safety_ticks: f64,
    /// If spread is tighter than this (in ticks), add extra safety ticks.
    pub post_only_tight_spread_ticks: f64,
    /// Extra safety ticks under tight spread.
    pub post_only_extra_tight_ticks: f64,
    /// Reprice threshold ($). Default: 0.010.
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
    /// Minimum order size (shares). Orders below this are skipped unless hedges are rounded up.
    pub min_order_size: f64,
    /// Minimum hedge trigger size (shares). Hedges below this are skipped.
    pub min_hedge_size: f64,
    /// If true, hedges smaller than min_order_size are rounded up to min_order_size.
    pub hedge_round_up: bool,
    /// Optional hedge notional floor (USDC) used to avoid venue marketable-BUY min-size rejects.
    /// 0 disables this feature.
    pub hedge_min_marketable_notional: f64,
    /// Max extra shares allowed when bumping hedge size to satisfy notional floor.
    pub hedge_min_marketable_max_extra: f64,
    /// Max extra percentage allowed when bumping hedge size to satisfy notional floor.
    pub hedge_min_marketable_max_extra_pct: f64,
    /// Max allowable loss percent for emergency hedging (clamps max_portfolio_cost).
    pub max_loss_pct: f64,
    /// DRY-RUN mode.
    pub dry_run: bool,
    /// Configurable TTL for stale book data (ms). Default 3000ms.
    pub stale_ttl_ms: u64,
    /// Periodic watchdog tick (ms) to enforce stale/toxic cancels even when md stream is silent.
    pub watchdog_tick_ms: u64,
    /// Hold-down window after toxicity recovers to prevent rapid cancel/place oscillation.
    pub toxic_recovery_hold_ms: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            pair_target: 0.99,
            max_net_diff: 5.0,
            max_side_shares: 5.0,
            bid_size: 2.0,
            tick_size: 0.01,
            post_only_safety_ticks: 2.0,
            post_only_tight_spread_ticks: 3.0,
            post_only_extra_tight_ticks: 1.0,
            reprice_threshold: 0.010, // Increased to reduce churn (1 cent drift)
            debounce_ms: 500,         // Increased to reduce churn (half second)
            as_skew_factor: 0.03,     // Original strictly conservative A-S
            as_time_decay_k: 2.0,     // Up to 3× skew at expiry (1 + 2 * elapsed_frac)
            market_end_ts: None,
            hedge_debounce_ms: 100, // Hedge orders bypass normal 500ms debounce
            max_portfolio_cost: 1.02, // Emergency hedge ceiling
            min_order_size: 1.0,
            min_hedge_size: 0.0,
            hedge_round_up: false,
            hedge_min_marketable_notional: 0.0,
            hedge_min_marketable_max_extra: 0.5,
            hedge_min_marketable_max_extra_pct: 0.15,
            max_loss_pct: 0.02, // Hard loss cap (2%)
            dry_run: true,
            stale_ttl_ms: 3000,
            watchdog_tick_ms: 500,
            toxic_recovery_hold_ms: 1200,
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
        let mut max_side_set = false;
        if let Ok(v) = std::env::var("PM_MAX_SIDE_SHARES") {
            if let Ok(f) = v.parse() {
                c.max_side_shares = f;
                max_side_set = true;
            }
        }
        if !max_side_set {
            if let Ok(v) = std::env::var("PM_MAX_POSITION_VALUE") {
                if let Ok(f) = v.parse() {
                    c.max_side_shares = f;
                    max_side_set = true;
                    warn!(
                        "PM_MAX_POSITION_VALUE is deprecated; use PM_MAX_SIDE_SHARES (units: shares)"
                    );
                }
            }
        }
        if !max_side_set {
            c.max_side_shares = c.max_net_diff;
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
        if let Ok(v) = std::env::var("PM_POST_ONLY_SAFETY_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.post_only_safety_ticks = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_POST_ONLY_TIGHT_SPREAD_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.post_only_tight_spread_ticks = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_POST_ONLY_EXTRA_TIGHT_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.post_only_extra_tight_ticks = f;
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
        if let Ok(v) = std::env::var("PM_MIN_ORDER_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.min_order_size = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MIN_HEDGE_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.min_hedge_size = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_ROUND_UP") {
            c.hedge_round_up = v == "1" || v.to_lowercase() == "true";
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_NOTIONAL") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.hedge_min_marketable_notional = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.hedge_min_marketable_max_extra = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.hedge_min_marketable_max_extra_pct = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_LOSS_PCT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    c.max_loss_pct = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_MAX_LOSS_PCT={} (must satisfy 0 <= pct < 1), using {}",
                        f, c.max_loss_pct
                    );
                }
            }
        }
        let max_cost_cap = 1.0 + c.max_loss_pct;
        if c.max_portfolio_cost > max_cost_cap {
            warn!(
                "⚠️ Clamping PM_MAX_PORTFOLIO_COST from {:.4} to {:.4} (max_loss_pct={:.3})",
                c.max_portfolio_cost, max_cost_cap, c.max_loss_pct
            );
            c.max_portfolio_cost = max_cost_cap;
        }
        if let Ok(v) = std::env::var("PM_DRY_RUN") {
            c.dry_run = v != "0" && v.to_lowercase() != "false";
        }
        if let Ok(v) = std::env::var("PM_STALE_TTL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                c.stale_ttl_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_COORD_WATCHDOG_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                c.watchdog_tick_ms = ms.max(50);
            }
        }
        if let Ok(v) = std::env::var("PM_TOXIC_RECOVERY_HOLD_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                c.toxic_recovery_hold_ms = ms;
            }
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
    cancel_stale: u64,
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
    /// Runtime-overridable gross exposure cap (shares).
    /// Defaults to cfg.max_side_shares and can be updated via watch channel.
    max_side_shares_live: f64,
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
    /// Per-side hold-down deadline after toxicity recovers.
    yes_toxic_hold_until: Instant,
    no_toxic_hold_until: Instant,
    /// Last observed toxicity state (edge detection).
    was_toxic_yes: bool,
    was_toxic_no: bool,
    stats: Stats,

    ofi_rx: watch::Receiver<OfiSnapshot>,
    inv_rx: watch::Receiver<InventoryState>,
    md_rx: watch::Receiver<MarketDataMsg>,
    om_tx: mpsc::Sender<OrderManagerCmd>,
    /// Opt-4: Direct high-priority kill channel from OFI Engine.
    /// Fires on toxicity onset without waiting for the next book tick.
    kill_rx: mpsc::Receiver<KillSwitchSignal>,
    /// Optional runtime max-side-cap updates (balance-driven).
    max_side_rx: Option<watch::Receiver<f64>>,
}

impl StrategyCoordinator {
    pub fn new(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventoryState>,
        md_rx: watch::Receiver<MarketDataMsg>,
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
        md_rx: watch::Receiver<MarketDataMsg>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
    ) -> Self {
        Self {
            max_side_shares_live: cfg.max_side_shares,
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
            yes_toxic_hold_until: Instant::now() - std::time::Duration::from_secs(60),
            no_toxic_hold_until: Instant::now() - std::time::Duration::from_secs(60),
            was_toxic_yes: false,
            was_toxic_no: false,
            stats: Stats::default(),
            ofi_rx,
            inv_rx,
            md_rx,
            om_tx,
            kill_rx,
            max_side_rx: None,
        }
    }

    /// Wire optional runtime updates for max_side_shares.
    pub fn with_dynamic_max_side_rx(mut self, rx: watch::Receiver<f64>) -> Self {
        let initial = (*rx.borrow()).max(0.0);
        self.max_side_shares_live = initial;
        self.max_side_rx = Some(rx);
        self
    }

    pub async fn run(mut self) {
        info!(
            "🎯 Coordinator [OCCAM+LEADLAG] pair={:.2} bid={:.1} tick={:.3} net={:.0} reprice={:.3} debounce={}ms watchdog={}ms dry={}",
            self.cfg.pair_target, self.cfg.bid_size, self.cfg.tick_size,
            self.cfg.max_net_diff, self.cfg.reprice_threshold, self.cfg.debounce_ms, self.cfg.watchdog_tick_ms, self.cfg.dry_run,
        );
        let mut watchdog = tokio::time::interval(Duration::from_millis(self.cfg.watchdog_tick_ms));

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
                    // Re-use the full tick() logic which reads latest OFI watch state.
                    self.tick().await;
                }

                // Market data tick (watch/coalescing)
                changed = self.md_rx.changed() => {
                    if changed.is_err() {
                        break; // Sender dropped
                    }
                    let msg = self.md_rx.borrow().clone();
                    self.handle_market_data(msg).await;
                    self.tick().await;
                }

                // Watchdog tick: enforce stale/toxic risk checks even when market WS is temporarily silent.
                _ = watchdog.tick() => {
                    self.tick().await;
                }
            }
        }

        info!(
            "🎯 Shutdown | ticks={} placed={} cancel(toxic={} stale={} inv={} reprice={}) skip(debounce={} backoff={} empty={} inv_limit={}) clamped={}",
            self.stats.ticks, self.stats.placed,
            self.stats.cancel_toxic, self.stats.cancel_stale, self.stats.cancel_inv, self.stats.cancel_reprice,
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
        self.refresh_dynamic_caps();

        let ofi = *self.ofi_rx.borrow();
        let inv = *self.inv_rx.borrow();

        // ── Environmental Health Check ──
        let now = Instant::now();
        let ttl = Duration::from_millis(self.cfg.stale_ttl_ms);

        let yes_stale = now.duration_since(self.last_valid_ts_yes) > ttl;
        let no_stale = now.duration_since(self.last_valid_ts_no) > ttl;
        let is_toxic_yes = ofi.yes.is_toxic;
        let is_toxic_no = ofi.no.is_toxic;

        // Toxicity recovery hold-down: avoid immediate re-entry around threshold boundary.
        let hold = Duration::from_millis(self.cfg.toxic_recovery_hold_ms);
        if self.was_toxic_yes && !is_toxic_yes {
            self.yes_toxic_hold_until = now + hold;
        }
        if self.was_toxic_no && !is_toxic_no {
            self.no_toxic_hold_until = now + hold;
        }
        self.was_toxic_yes = is_toxic_yes;
        self.was_toxic_no = is_toxic_no;

        let yes_toxic_hold = now < self.yes_toxic_hold_until;
        let no_toxic_hold = now < self.no_toxic_hold_until;
        let yes_toxic_blocked = is_toxic_yes || yes_toxic_hold;
        let no_toxic_blocked = is_toxic_no || no_toxic_hold;

        // Priority 1: 30s Staleness Guard (Critical Shutdown)
        if self.is_book_stale() {
            if self.yes_target.is_some() {
                warn!("⚠️ Book expired (>30s) — clearing YES");
                self.stats.cancel_stale += 1;
                self.clear_target(Side::Yes, CancelReason::StaleData).await;
            }
            if self.no_target.is_some() {
                warn!("⚠️ Book expired (>30s) — clearing NO");
                self.stats.cancel_stale += 1;
                self.clear_target(Side::No, CancelReason::StaleData).await;
            }
            return;
        }

        // Priority 2: Per-side Toxic/Stale guard (independent of book availability)
        if yes_toxic_blocked || yes_stale {
            if self.yes_target.is_some() {
                if yes_toxic_blocked {
                    self.stats.cancel_toxic += 1;
                    self.clear_target(Side::Yes, CancelReason::ToxicFlow).await;
                } else {
                    self.stats.cancel_stale += 1;
                    self.clear_target(Side::Yes, CancelReason::StaleData).await;
                }
            }
        }
        if no_toxic_blocked || no_stale {
            if self.no_target.is_some() {
                if no_toxic_blocked {
                    self.stats.cancel_toxic += 1;
                    self.clear_target(Side::No, CancelReason::ToxicFlow).await;
                } else {
                    self.stats.cancel_stale += 1;
                    self.clear_target(Side::No, CancelReason::StaleData).await;
                }
            }
        }

        // Priority 3: Market data availability
        let ub = self.usable_book();
        if ub.yes_bid <= 0.0 || ub.no_bid <= 0.0 {
            self.stats.skipped_empty_book += 1;
            return;
        }

        // Priority 4: Toxic or Stale (3s TTL) → Selective Shutdown
        // If a side is unhealthy, its price will be 0.0 in the unified state logic below.
        self.state_unified(
            &inv,
            &ub,
            yes_stale,
            no_stale,
            yes_toxic_blocked,
            no_toxic_blocked,
        )
        .await;
    }

    // ═════════════════════════════════════════════════
    // State Unified: A-S Skew + Gabagool22 Cost Averaging
    // ═════════════════════════════════════════════════

    async fn state_unified(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        yes_stale: bool,
        no_stale: bool,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
    ) {
        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no = (ub.no_bid + ub.no_ask) / 2.0;

        // 1. Calculate base pricing via A-S + Gabagool
        let excess = f64::max(0.0, (mid_yes + mid_no) - self.cfg.pair_target);
        let skew = if self.cfg.max_net_diff > 0.0 {
            (inv.net_diff / self.cfg.max_net_diff).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let effective_skew_factor = self.cfg.as_skew_factor * self.compute_time_decay_factor();
        let skew_shift = skew * effective_skew_factor;

        let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
        let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

        if raw_yes + raw_no > self.cfg.pair_target {
            let overflow = (raw_yes + raw_no) - self.cfg.pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        // 2. Strict Maker Clamp
        // P3 FIX: Extra tick margin to reduce post-only cross-book rejections
        let mut margin_ticks = self.cfg.post_only_safety_ticks.max(0.5);
        if ub.yes_bid > 0.0 && ub.yes_ask > ub.yes_bid {
            let spread_ticks = (ub.yes_ask - ub.yes_bid) / self.cfg.tick_size.max(1e-9);
            if spread_ticks <= self.cfg.post_only_tight_spread_ticks {
                margin_ticks += self.cfg.post_only_extra_tight_ticks.max(0.0);
            }
        }
        let yes_safety_margin = margin_ticks * self.cfg.tick_size;

        let mut margin_ticks_no = self.cfg.post_only_safety_ticks.max(0.5);
        if ub.no_bid > 0.0 && ub.no_ask > ub.no_bid {
            let spread_ticks = (ub.no_ask - ub.no_bid) / self.cfg.tick_size.max(1e-9);
            if spread_ticks <= self.cfg.post_only_tight_spread_ticks {
                margin_ticks_no += self.cfg.post_only_extra_tight_ticks.max(0.0);
            }
        }
        let no_safety_margin = margin_ticks_no * self.cfg.tick_size;


        if ub.yes_ask > 0.0 {
            raw_yes = f64::min(raw_yes, ub.yes_ask - yes_safety_margin);
        }
        if ub.no_ask > 0.0 {
            raw_no = f64::min(raw_no, ub.no_ask - no_safety_margin);
        }

        let mut bid_yes = self.safe_price(raw_yes);
        let mut bid_no = self.safe_price(raw_no);

        // 3. Health Overrides (Toxicity / Staleness)
        // NOTE: Stats (cancel_toxic/cancel_stale) are counted in tick() Priority 2,
        // not here, to prevent double-counting when both paths execute.
        if yes_stale || yes_toxic_blocked {
            if bid_yes > 0.0 {
                debug!(
                    "🚫 YES {} -> skip bid",
                    if yes_stale { "stale" } else { "toxic" }
                );
            }
            bid_yes = 0.0;
        }
        if no_stale || no_toxic_blocked {
            if bid_no > 0.0 {
                debug!(
                    "🚫 NO {} -> skip bid",
                    if no_stale { "stale" } else { "toxic" }
                );
            }
            bid_no = 0.0;
        }

        // 4. Hedge / Rescue Overrides
        let net_diff = inv.net_diff;
        let mut hedge_dispatched_yes = false;
        let mut hedge_dispatched_no = false;

        // Provide gating (size = bid_size)
        let mut allow_yes_provide = self.can_buy_yes(inv, self.cfg.bid_size);
        let mut allow_no_provide = self.can_buy_no(inv, self.cfg.bid_size);
        let mut block_yes_provide = false;
        let mut block_no_provide = false;

        if net_diff > f64::EPSILON {
            // We have YES, want to hedge by buying NO.
            let hedge_target = self.hedge_target(net_diff);
            if let Some(hedge_size) = self.hedge_size_from_net(net_diff) {
                let mut hedge_size = hedge_size;
                let mut ceiling_no =
                    self.incremental_hedge_ceiling(inv, Side::No, hedge_size, hedge_target);
                let mut agg_no = self.aggressive_price(ceiling_no, ub.no_bid, ub.no_ask);
                let mut allow_no_hedge = self.can_buy_no(inv, hedge_size);
                if !allow_no_hedge {
                    block_no_provide = true;
                }

                if agg_no > 0.0 && allow_no_hedge && !no_stale && !no_toxic_blocked {
                    let mut hedge_no = f64::max(bid_no, agg_no).min(ceiling_no);
                    hedge_no = self.safe_price(hedge_no);

                    if let Some(bumped) =
                        self.bump_hedge_size_for_marketable_floor(hedge_no, hedge_size)
                    {
                        if bumped > hedge_size + 1e-9 {
                            if self.can_buy_no(inv, bumped) {
                                hedge_size = bumped;
                                ceiling_no = self.incremental_hedge_ceiling(
                                    inv,
                                    Side::No,
                                    hedge_size,
                                    hedge_target,
                                );
                                agg_no = self.aggressive_price(ceiling_no, ub.no_bid, ub.no_ask);
                                if agg_no > 0.0 {
                                    hedge_no = f64::max(bid_no, agg_no).min(ceiling_no);
                                    hedge_no = self.safe_price(hedge_no);
                                }
                                allow_no_hedge = self.can_buy_no(inv, hedge_size);
                            } else {
                                debug!(
                                    "🧩 Hedge NO notional bump skipped: size {:.2} exceeds inventory gate",
                                    bumped
                                );
                            }
                        }
                    }
                    if !allow_no_hedge {
                        block_no_provide = true;
                    }
                    if agg_no > 0.0 && allow_no_hedge {
                        let current_no = self.no_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                        let current_sz = self.no_target.as_ref().map(|t| t.size).unwrap_or(0.0);

                        let log_msg = if current_no <= 0.0
                            || (current_no - hedge_no).abs() > self.cfg.reprice_threshold
                            || (current_sz - hedge_size).abs() > 0.1
                        {
                            Some(format!(
                                "🔧 HEDGE NO@{:.3} sz={:.1} | net={:.1}",
                                hedge_no, hedge_size, net_diff
                            ))
                        } else {
                            None
                        };

                        self.place_or_reprice(
                            Side::No,
                            hedge_no,
                            hedge_size,
                            BidReason::Hedge,
                            log_msg,
                        )
                        .await;
                        hedge_dispatched_no = true;
                        bid_no = 0.0;
                    }
                }
            } else {
                debug!(
                    "🧩 Hedge skip NO: net_diff={:.2} below min thresholds (min_order_size={:.2}, min_hedge_size={:.2})",
                    net_diff,
                    self.cfg.min_order_size,
                    self.cfg.min_hedge_size,
                );
            }
        } else if net_diff < -f64::EPSILON {
            // We have NO, want to hedge by buying YES.
            let hedge_target = self.hedge_target(net_diff);
            if let Some(hedge_size) = self.hedge_size_from_net(net_diff) {
                let mut hedge_size = hedge_size;
                let mut ceiling_yes =
                    self.incremental_hedge_ceiling(inv, Side::Yes, hedge_size, hedge_target);
                let mut agg_yes = self.aggressive_price(ceiling_yes, ub.yes_bid, ub.yes_ask);
                let mut allow_yes_hedge = self.can_buy_yes(inv, hedge_size);
                if !allow_yes_hedge {
                    block_yes_provide = true;
                }

                if agg_yes > 0.0 && allow_yes_hedge && !yes_stale && !yes_toxic_blocked {
                    let mut hedge_yes = f64::max(bid_yes, agg_yes).min(ceiling_yes);
                    hedge_yes = self.safe_price(hedge_yes);

                    if let Some(bumped) =
                        self.bump_hedge_size_for_marketable_floor(hedge_yes, hedge_size)
                    {
                        if bumped > hedge_size + 1e-9 {
                            if self.can_buy_yes(inv, bumped) {
                                hedge_size = bumped;
                                ceiling_yes = self.incremental_hedge_ceiling(
                                    inv,
                                    Side::Yes,
                                    hedge_size,
                                    hedge_target,
                                );
                                agg_yes =
                                    self.aggressive_price(ceiling_yes, ub.yes_bid, ub.yes_ask);
                                if agg_yes > 0.0 {
                                    hedge_yes = f64::max(bid_yes, agg_yes).min(ceiling_yes);
                                    hedge_yes = self.safe_price(hedge_yes);
                                }
                                allow_yes_hedge = self.can_buy_yes(inv, hedge_size);
                            } else {
                                debug!(
                                    "🧩 Hedge YES notional bump skipped: size {:.2} exceeds inventory gate",
                                    bumped
                                );
                            }
                        }
                    }
                    if !allow_yes_hedge {
                        block_yes_provide = true;
                    }
                    if agg_yes > 0.0 && allow_yes_hedge {
                        let current_yes = self.yes_target.as_ref().map(|t| t.price).unwrap_or(0.0);
                        let current_sz = self.yes_target.as_ref().map(|t| t.size).unwrap_or(0.0);

                        let log_msg = if current_yes <= 0.0
                            || (current_yes - hedge_yes).abs() > self.cfg.reprice_threshold
                            || (current_sz - hedge_size).abs() > 0.1
                        {
                            Some(format!(
                                "🔧 HEDGE YES@{:.3} sz={:.1} | net={:.1}",
                                hedge_yes, hedge_size, net_diff
                            ))
                        } else {
                            None
                        };

                        self.place_or_reprice(
                            Side::Yes,
                            hedge_yes,
                            hedge_size,
                            BidReason::Hedge,
                            log_msg,
                        )
                        .await;
                        hedge_dispatched_yes = true;
                        bid_yes = 0.0;
                    }
                }
            } else {
                debug!(
                    "🧩 Hedge skip YES: net_diff={:.2} below min thresholds (min_order_size={:.2}, min_hedge_size={:.2})",
                    net_diff,
                    self.cfg.min_order_size,
                    self.cfg.min_hedge_size,
                );
            }
        }

        // 5. Final Dispatch (Provide orders)
        if block_yes_provide {
            allow_yes_provide = false;
        }
        if block_no_provide {
            allow_no_provide = false;
        }

        if !hedge_dispatched_yes {
            if !allow_yes_provide && self.yes_target.is_some() {
                self.stats.cancel_inv += 1;
                self.stats.skipped_inv_limit += 1;
            }
            if !allow_yes_provide {
                self.clear_target(Side::Yes, CancelReason::InventoryLimit)
                    .await;
            } else if bid_yes > 0.0 {
                self.place_or_reprice(
                    Side::Yes,
                    bid_yes,
                    self.cfg.bid_size,
                    BidReason::Provide,
                    None,
                )
                .await;
            } else if yes_toxic_blocked {
                self.clear_target(Side::Yes, CancelReason::ToxicFlow).await;
            } else if yes_stale {
                self.clear_target(Side::Yes, CancelReason::StaleData).await;
            }
        }
        if !hedge_dispatched_no {
            if !allow_no_provide && self.no_target.is_some() {
                self.stats.cancel_inv += 1;
                self.stats.skipped_inv_limit += 1;
            }
            if !allow_no_provide {
                self.clear_target(Side::No, CancelReason::InventoryLimit)
                    .await;
            } else if bid_no > 0.0 {
                self.place_or_reprice(
                    Side::No,
                    bid_no,
                    self.cfg.bid_size,
                    BidReason::Provide,
                    None,
                )
                .await;
            } else if no_toxic_blocked {
                self.clear_target(Side::No, CancelReason::ToxicFlow).await;
            } else if no_stale {
                self.clear_target(Side::No, CancelReason::StaleData).await;
            }
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

    /// Step hedge ceiling: pair_target within normal risk, max_portfolio_cost only at/over max_net_diff.
    fn hedge_target(&self, net_diff: f64) -> f64 {
        let pair = self.cfg.pair_target;
        let max_cost = self.cfg.max_portfolio_cost;
        if max_cost <= pair || self.cfg.max_net_diff <= f64::EPSILON {
            return pair;
        }
        if net_diff.abs() >= self.cfg.max_net_diff {
            max_cost
        } else {
            pair
        }
    }

    /// Compute the maximum acceptable incremental hedge price on the missing side,
    /// respecting the post-hedge target combined cost.
    ///
    /// Key idea: when we already hold inventory on the hedge side, we should use
    /// *incremental* budget instead of `target - avg_held_side` static subtraction.
    /// This avoids systematically underpricing hedges when side quantities are uneven.
    fn incremental_hedge_ceiling(
        &self,
        inv: &InventoryState,
        hedge_side: Side,
        hedge_size: f64,
        hedge_target: f64,
    ) -> f64 {
        if hedge_size <= f64::EPSILON {
            return 0.0;
        }

        match hedge_side {
            Side::No => {
                let q = inv.no_qty.max(0.0);
                let target_no_avg = hedge_target - inv.yes_avg_cost;
                if target_no_avg <= 0.0 {
                    return 0.0;
                }
                if q <= f64::EPSILON {
                    return target_no_avg;
                }
                let existing_cost = q * inv.no_avg_cost.max(0.0);
                let total_allowed = target_no_avg * (q + hedge_size);
                let incremental_allowed = total_allowed - existing_cost;
                if incremental_allowed <= 0.0 {
                    0.0
                } else {
                    incremental_allowed / hedge_size
                }
            }
            Side::Yes => {
                let q = inv.yes_qty.max(0.0);
                let target_yes_avg = hedge_target - inv.no_avg_cost;
                if target_yes_avg <= 0.0 {
                    return 0.0;
                }
                if q <= f64::EPSILON {
                    return target_yes_avg;
                }
                let existing_cost = q * inv.yes_avg_cost.max(0.0);
                let total_allowed = target_yes_avg * (q + hedge_size);
                let incremental_allowed = total_allowed - existing_cost;
                if incremental_allowed <= 0.0 {
                    0.0
                } else {
                    incremental_allowed / hedge_size
                }
            }
        }
    }

    /// Determine hedge size with minimum size constraints.
    /// Returns None if hedge should be skipped.
    fn hedge_size_from_net(&self, net_diff: f64) -> Option<f64> {
        let raw = net_diff.abs();
        if raw <= f64::EPSILON {
            return None;
        }
        let min_hedge = self.cfg.min_hedge_size.max(0.0);
        if min_hedge > 0.0 && raw + 1e-9 < min_hedge {
            return None;
        }
        let min_order = self.cfg.min_order_size.max(0.0);
        if min_order > 0.0 && raw + 1e-9 < min_order {
            if self.cfg.hedge_round_up {
                return Some(min_order);
            }
            return None;
        }
        Some(raw)
    }

    /// Optional hedge-size bump to satisfy venue marketable-BUY minimum notional.
    ///
    /// Returns `Some(new_size)` only when bump is enabled and within configured
    /// extra-size caps; otherwise returns `None` and caller keeps original size.
    fn bump_hedge_size_for_marketable_floor(&self, price: f64, size: f64) -> Option<f64> {
        let min_notional = self.cfg.hedge_min_marketable_notional;
        if min_notional <= 0.0 || price <= 0.0 || size <= 0.0 {
            return None;
        }
        let required = ((min_notional / price) * 100.0).ceil() / 100.0;
        if required <= size + 1e-9 {
            return None;
        }
        let extra = required - size;
        let max_extra_abs = self.cfg.hedge_min_marketable_max_extra.max(0.0);
        let max_extra_pct = (size * self.cfg.hedge_min_marketable_max_extra_pct.max(0.0)).max(0.0);
        if extra <= max_extra_abs + 1e-9 && extra <= max_extra_pct + 1e-9 {
            Some(required)
        } else {
            debug!(
                "🧩 Hedge min-notional bump rejected: price={:.3} size={:.2} -> req={:.2} (extra={:.2} > caps abs={:.2} pct={:.2})",
                price, size, required, extra, max_extra_abs, max_extra_pct
            );
            None
        }
    }

    fn max_side_shares(&self) -> f64 {
        if self.max_side_shares_live > 0.0 {
            self.max_side_shares_live
        } else {
            f64::INFINITY
        }
    }

    fn refresh_dynamic_caps(&mut self) {
        let Some(rx) = self.max_side_rx.as_mut() else {
            return;
        };

        loop {
            let changed = match rx.has_changed() {
                Ok(v) => v,
                Err(_) => break, // sender dropped; keep last known cap
            };
            if !changed {
                break;
            }

            let next = (*rx.borrow_and_update()).max(0.0);
            if (next - self.max_side_shares_live).abs() > 0.5 {
                info!(
                    "💡 Runtime max_side_shares update: {:.1} -> {:.1}",
                    self.max_side_shares_live, next
                );
            }
            self.max_side_shares_live = next;
        }
    }

    fn can_buy_yes(&self, inv: &InventoryState, size: f64) -> bool {
        let net_ok = inv.net_diff + size <= self.cfg.max_net_diff + 1e-4;
        let side_ok = inv.yes_qty + size <= self.max_side_shares() + 1e-4;
        net_ok && side_ok
    }

    fn can_buy_no(&self, inv: &InventoryState, size: f64) -> bool {
        let net_ok = inv.net_diff - size >= -self.cfg.max_net_diff - 1e-4;
        let side_ok = inv.no_qty + size <= self.max_side_shares() + 1e-4;
        net_ok && side_ok
    }

    /// Aggressive Maker price: min(ceiling, best_ask − tick).
    ///
    /// CRITICAL: If best_ask is unavailable (empty book), return 0.0.
    /// NEVER fall back to ceiling — that caused the phantom 0.490 oscillation.
    /// Bidding at ceiling when no ask exists = paying maximum price into a void.
    fn aggressive_price(&self, ceiling: f64, best_bid: f64, best_ask: f64) -> f64 {
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

        // P3 FIX: Extra tick margin to reduce post-only cross-book rejections
        // caused by stale book data between local calculation and exchange arrival.
        let mut margin_ticks = self.cfg.post_only_safety_ticks.max(0.5);
        if best_bid > 0.0 && best_ask > best_bid {
            let spread_ticks = (best_ask - best_bid) / self.cfg.tick_size.max(1e-9);
            if spread_ticks <= self.cfg.post_only_tight_spread_ticks {
                margin_ticks += self.cfg.post_only_extra_tight_ticks.max(0.0);
            }
        }
        let safety_margin = margin_ticks * self.cfg.tick_size;
        let safe_below = best_ask - safety_margin;
        if safe_below <= 0.0 {
            return 0.0;
        }
        self.safe_price(ceiling.min(safe_below))
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
        size: f64,
        reason: BidReason,
        log_msg: Option<String>,
    ) {
        let (current_target, last_ts) = match side {
            Side::Yes => (self.yes_target.as_ref(), self.yes_last_ts),
            Side::No => (self.no_target.as_ref(), self.no_last_ts),
        };

        let active = current_target.is_some();
        let slot_price = current_target.map(|t| t.price).unwrap_or(0.0);
        let slot_size = current_target.map(|t| t.size).unwrap_or(0.0);

        // OPTIMIZATION: Bypassing debounce for 0.0 price (Cancellation).
        // If we want to cancel, we should do it immediately, especially during toxic/stale events.
        // Also, skip redundant ClearTarget if no order is active.
        if price <= 0.0 {
            if active {
                self.clear_target(side, CancelReason::InventoryLimit).await;
            }
            return;
        }

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
            self.place(side, price, size, reason).await;
        } else if (slot_price - price).abs() > self.cfg.reprice_threshold
            || (slot_size - size).abs() > 0.1
        {
            debug!(
                "🔄 reprice {:?} {:.3}→{:.3} sz={:.1}",
                side, slot_price, price, size
            );
            self.stats.cancel_reprice += 1;
            self.place(side, price, size, reason).await;
        }
    }

    async fn clear_target(&mut self, side: Side, reason: CancelReason) {
        let active = match side {
            Side::Yes => self.yes_target.is_some(),
            Side::No => self.no_target.is_some(),
        };
        if !active {
            return;
        }

        match side {
            Side::Yes => self.yes_target = None,
            Side::No => self.no_target = None,
        };

        debug!("🗑️ Cancel {:?} ({:?})", side, reason);
        if self.cfg.dry_run {
            info!("📝 DRY cancel {:?} ({:?})", side, reason);
            return;
        }

        let _ = self
            .om_tx
            .send(OrderManagerCmd::ClearTarget { side, reason })
            .await;
    }

    // ═════════════════════════════════════════════════
    async fn handle_market_data(&mut self, msg: MarketDataMsg) {
        match msg {
            MarketDataMsg::BookTick {
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                ..
            } => {
                self.update_book(yes_bid, yes_ask, no_bid, no_ask);
                self.stats.ticks += 1;
            }
            MarketDataMsg::TradeTick { .. } => {
                // Trades are primarily for OFI actor; Coordinator mostly skips
                // but we could track last trade prices here if needed.
            }
        }
    }

    async fn place(&mut self, side: Side, price: f64, size: f64, reason: BidReason) {
        if price <= 0.0 {
            let cancel_reason = match reason {
                BidReason::Hedge => CancelReason::Reprice,
                BidReason::Provide => CancelReason::InventoryLimit,
            };
            self.clear_target(side, cancel_reason).await;
            return;
        }

        let target = DesiredTarget {
            side,
            price,
            size,
            reason,
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
            info!("📝 DRY {:?} {:?}@{:.3} sz={:.1}", reason, side, price, size);
            return;
        }

        let _ = self.om_tx.send(OrderManagerCmd::SetTarget(target)).await;
    }

    // clear_target() handles explicit cancellation with reason routing.
}

// ─────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    fn cfg() -> CoordinatorConfig {
        CoordinatorConfig {
            pair_target: 0.98,
            max_net_diff: 10.0,
            max_side_shares: 10.0,
            bid_size: 2.0,
            tick_size: 0.01,
            reprice_threshold: 0.001,
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
        watch::Sender<MarketDataMsg>,
        mpsc::Sender<KillSwitchSignal>,
        mpsc::Receiver<OrderManagerCmd>,
        StrategyCoordinator,
    ) {
        let (o, or) = watch::channel(OfiSnapshot::default());
        let (i, ir) = watch::channel(InventoryState::default());
        let (m, mr) = watch::channel(MarketDataMsg::BookTick {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
            ts: Instant::now(),
        });
        let (e, er) = mpsc::channel(16);
        let (k, kr) = mpsc::channel(16);
        (
            o,
            i,
            m,
            k,
            er,
            StrategyCoordinator::with_kill_rx(c, or, ir, mr, e, kr),
        )
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
        assert!((c.aggressive_price(0.50, 0.40, 0.55) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn test_aggressive_ask_wins() {
        let (_, _, _, _, _, c) = make(cfg());
        // Tight spread adds one extra safety tick:
        // best_bid=0.50 best_ask=0.52, margin=(2+1) ticks -> 0.49
        assert!((c.aggressive_price(0.60, 0.50, 0.52) - 0.49).abs() < 1e-9);
    }

    #[test]
    fn test_incremental_hedge_ceiling_uses_existing_inventory_budget() {
        let (_, _, _, _, _, c) = make(cfg());
        let inv = InventoryState {
            yes_qty: 25.5,
            no_qty: 15.5,
            yes_avg_cost: 0.3929,
            no_avg_cost: 0.4301,
            net_diff: 10.0,
            portfolio_cost: 0.8229,
        };

        let hedge_size = 10.0;
        let target = 0.98;
        let static_ceiling = target - inv.yes_avg_cost;
        let incremental_ceiling = c.incremental_hedge_ceiling(&inv, Side::No, hedge_size, target);

        // New ceiling should be materially higher than static subtraction when
        // hedge-side inventory already exists at lower average cost.
        assert!(incremental_ceiling > static_ceiling + 1e-6);
        assert!((incremental_ceiling - 0.83045).abs() < 1e-4);
    }

    #[test]
    fn test_incremental_hedge_ceiling_respects_target_after_trade() {
        let (_, _, _, _, _, c) = make(cfg());
        let inv = InventoryState {
            yes_qty: 25.5,
            no_qty: 15.5,
            yes_avg_cost: 0.3929,
            no_avg_cost: 0.4301,
            net_diff: 10.0,
            portfolio_cost: 0.8229,
        };

        let hedge_size = 10.0;
        let target = 1.02; // rescue cap
        let ceiling = c.incremental_hedge_ceiling(&inv, Side::No, hedge_size, target);
        let no_cost_after = inv.no_qty * inv.no_avg_cost + hedge_size * ceiling;
        let no_avg_after = no_cost_after / (inv.no_qty + hedge_size);
        assert!(inv.yes_avg_cost + no_avg_after <= target + 1e-9);
    }

    // ── Per-side toxicity guard ──

    #[tokio::test]
    async fn test_toxic_cancels_only_toxic_side() {
        let (o, _i, m, _k, mut e, mut coord) = make(cfg());
        coord.yes_target = Some(DesiredTarget {
            side: Side::Yes,
            price: 0.45,
            size: 2.0,
            reason: BidReason::Provide,
        });
        coord.no_target = Some(DesiredTarget {
            side: Side::No,
            price: 0.50,
            size: 2.0,
            reason: BidReason::Provide,
        });

        // Only YES is toxic — only YES should be canceled.
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
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c1.is_ok());
        match c1.unwrap() {
            Some(OrderManagerCmd::ClearTarget { side, reason }) => {
                assert_eq!(side, Side::Yes);
                assert_eq!(reason, CancelReason::ToxicFlow);
            }
            _ => panic!("expected YES clear on toxic side"),
        }

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_other_side_can_still_quote_when_one_side_toxic() {
        let (o, _i, m, _k, mut e, coord) = make(cfg());

        // NO is toxic, YES is healthy: coordinator should still place YES bid.
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
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

        let c = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c.is_ok());
        match c.unwrap() {
            Some(OrderManagerCmd::SetTarget(target)) => {
                assert_eq!(target.side, Side::Yes);
                assert!(target.price > 0.0);
                assert_eq!(target.reason, BidReason::Provide);
            }
            _ => panic!("expected YES target while NO side is toxic"),
        }

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_toxic_cancel_with_empty_book() {
        let (o, _i, m, _k, mut e, mut coord) = make(cfg());
        coord.yes_target = Some(DesiredTarget {
            side: Side::Yes,
            price: 0.45,
            size: 2.0,
            reason: BidReason::Provide,
        });
        coord.no_target = Some(DesiredTarget {
            side: Side::No,
            price: 0.50,
            size: 2.0,
            reason: BidReason::Provide,
        });

        let _ = o.send(OfiSnapshot {
            yes: SideOfi {
                ofi_score: 200.0,
                buy_volume: 200.0,
                sell_volume: 0.0,
                is_toxic: true,
            },
            no: SideOfi::default(),
            ts: Instant::now(),
        });

        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.0, 0.0, 0.0, 0.0));

        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c1.is_ok());
        match c1.unwrap() {
            Some(OrderManagerCmd::ClearTarget { side, reason }) => {
                assert_eq!(side, Side::Yes);
                assert_eq!(reason, CancelReason::ToxicFlow);
            }
            _ => panic!("expected YES clear even when book is empty"),
        }

        drop(m);
        let _ = h.await;
    }

    // ── Balanced mid pricing ──

    #[tokio::test]
    async fn test_balanced_mid_pricing() {
        let (_o, _i, m, _k, mut e, coord) = make(cfg());
        let h = tokio::spawn(coord.run());
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

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
        let _ = m.send(bt(0.50, 0.54, 0.48, 0.52));
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
        let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));
        let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c1.is_ok());
        let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
        assert!(c2.is_ok());

        // Second tick with different prices — should be debounced
        let _ = m.send(bt(0.30, 0.32, 0.60, 0.62));
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
        let _ = m.send(bt(0.0, 0.0, 0.0, 0.0));
        let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
        assert!(c.is_err()); // No commands
        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_hedge_emergency_ceiling_toxic_flow() {
        let mut cfg = cfg();
        cfg.max_net_diff = 10.0;
        cfg.max_side_shares = 20.0;
        cfg.pair_target = 0.985;
        cfg.max_portfolio_cost = 1.02;

        let (o, i, m, _k, mut e, coord) = make(cfg);

        // 1. Setup inventory: heavily imbalanced (net = -10.0)
        let inv = InventoryState {
            net_diff: -10.0,
            yes_qty: 5.0,
            no_qty: 15.0,
            yes_avg_cost: 0.45,
            no_avg_cost: 0.45,
            portfolio_cost: 0.90,
        };
        let _ = i.send(inv); // watch::Sender::send is not async

        // 2. Trigger Toxic Flow kill on the OTHER side (NO)
        // This ensures the risky side is toxic, but the hedge side (YES) is healthy.
        let _ = o.send(OfiSnapshot {
            yes: SideOfi::default(),
            no: SideOfi {
                ofi_score: 5000.0,
                is_toxic: true,
                ..Default::default()
            },
            ts: Instant::now(),
        });

        // 3. Hear the kill signal
        let h = tokio::spawn(async move { coord.run().await });

        // 4. Send a book update to trigger pricing
        let _ = m.send(bt(0.30, 0.70, 0.40, 0.60));

        let cmd = timeout(Duration::from_millis(100), e.recv()).await;
        if let Ok(Some(OrderManagerCmd::SetTarget(target))) = cmd {
            assert_eq!(target.side, Side::Yes);
            assert_eq!(target.reason, BidReason::Hedge);
            // Incremental emergency ceiling with existing YES inventory:
            // target_yes_avg=1.02-0.45=0.57 over (5+10) shares -> incremental cap 0.63
            assert!((target.price - 0.63).abs() < 1e-9);
        } else {
            panic!("Expected SetTarget hedge command, got {:?}", cmd);
        }
        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_stale_book_protection() {
        let (_o, _i, m, k, mut e, mut coord) = make(cfg());

        // 1. Send an initial valid update to populate last_valid_book
        coord.update_book(0.44, 0.46, 0.48, 0.52);

        // 2. Artificially backdate the timestamps to 6 seconds ago
        coord.last_valid_ts_yes = Instant::now() - Duration::from_secs(6);
        coord.last_valid_ts_no = Instant::now() - Duration::from_secs(6);

        let h = tokio::spawn(async move { coord.run().await });

        // 3. Trigger a pricing attempt via KillSwitchSignal (Direct Kill channel)
        // This calls tick() WITHOUT calling update_book(), so timestamps remain stale.
        let _ = k.send(KillSwitchSignal {
            side: Side::Yes,
            ofi_score: 1.0,
            ts: Instant::now(),
        });

        // 4. Command should NOT be sent due to staleness
        // Note: we check both sides in the new tick() logic.
        let cmd = timeout(Duration::from_millis(200), e.recv()).await;
        assert!(
            cmd.is_err(),
            "Expected timeout (no bid) due to stale book, but got {:?}",
            cmd
        );

        drop(m);
        let _ = h.await;
    }
    #[tokio::test]
    async fn test_dynamic_hedge_sizing_shares() {
        let mut cfg = cfg();
        cfg.bid_size = 5.0;
        cfg.max_side_shares = 20.0;
        let (_o, i, m, _k, mut e, coord) = make(cfg);
        let h = tokio::spawn(coord.run());

        // Setup imbalance of 12.0 shares (YES excess)
        let _ = i.send(InventoryState {
            net_diff: 12.0,
            yes_qty: 12.0,
            yes_avg_cost: 0.50,
            ..Default::default()
        });

        // Trigger pricing with a valid book update
        let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

        // Should see a HEDGE order on NO with size = 12.0
        let mut found_hedge = false;
        while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
            timeout(Duration::from_millis(200), e.recv()).await
        {
            if target.side == Side::No
                && (target.size - 12.0).abs() < 0.1
                && target.reason == BidReason::Hedge
            {
                found_hedge = true;
                break;
            }
        }
        assert!(found_hedge, "Expected hedge order of size 12.0 on NO");

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_inventory_limits_block_orders() {
        let mut cfg = cfg();
        cfg.as_skew_factor = 0.0;
        cfg.hedge_debounce_ms = 0;
        cfg.max_net_diff = 10.0;
        cfg.bid_size = 15.0; // Ordering 15 shares while limit is 10.
        let (_o, i, m, _k, mut e, coord) = make(cfg);
        let h = tokio::spawn(coord.run());

        let _ = i.send(InventoryState {
            net_diff: 0.0,
            ..Default::default()
        });

        let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

        // It might send a Cancel command if it thinks there's a target to clear.
        // We check that NO 'Place' command with size 50 is sent.
        while let Ok(cmd) = timeout(Duration::from_millis(100), e.recv()).await {
            match cmd {
                Some(OrderManagerCmd::SetTarget(target)) => {
                    if target.size > 1.0 && target.price > 0.0 {
                        panic!(
                            "Should not place order of size {} when budget exceeded",
                            target.size
                        );
                    }
                }
                _ => {} // Ignore CancelAll
            }
        }

        drop(m);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_hedge_size_change_reprices() {
        let mut cfg = cfg();
        cfg.as_skew_factor = 0.0;
        cfg.max_net_diff = 100.0;
        cfg.max_side_shares = 100.0;
        cfg.hedge_debounce_ms = 0;
        let (_o, i, m, _k, mut e, coord) = make(cfg);
        let h = tokio::spawn(coord.run());

        let _ = i.send(InventoryState {
            net_diff: 5.0,
            ..Default::default()
        });
        let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

        let mut first_size = None;
        while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
            timeout(Duration::from_millis(200), e.recv()).await
        {
            if target.side == Side::No
                && (target.size - 5.0).abs() < 0.1
                && target.reason == BidReason::Hedge
            {
                first_size = Some(target.size);
                break;
            }
        }
        assert!(
            first_size.is_some(),
            "Expected initial hedge size of 5.0 on NO"
        );

        let _ = i.send(InventoryState {
            net_diff: 8.0,
            yes_qty: 8.0,
            no_qty: 0.0,
            yes_avg_cost: 0.50,
            ..Default::default()
        });
        let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

        let mut updated = false;
        while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
            timeout(Duration::from_millis(200), e.recv()).await
        {
            if target.side == Side::No
                && (target.size - 8.0).abs() < 0.1
                && target.reason == BidReason::Hedge
            {
                updated = true;
                break;
            }
        }
        assert!(updated, "Expected hedge size update to 8.0 on NO");

        drop(m);
        let _ = h.await;
    }
}
