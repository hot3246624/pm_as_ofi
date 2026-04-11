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

use super::glft::GlftSignalSnapshot;
use super::messages::*;
use super::strategy::{
    StrategyExecutionMode, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput,
};
use super::types::Side;

const GLFT_SOURCE_RECOVERY_FLAP_IGNORE_MS: u64 = 800;
const GLFT_SOURCE_RECOVERY_RESET_SHADOW_MS: u64 = 4_500;
const GLFT_SOURCE_RECOVERY_SETTLE_MIN_MS: u64 = 1_800;
const GLFT_SOURCE_RECOVERY_SETTLE_MAX_MS: u64 = 12_000;
const GLFT_SOURCE_BLOCK_RETAIN_HOLD_MS: u64 = 2_500;
const GLFT_SOURCE_BLOCK_RETAIN_HOLD_BINANCE_MS: u64 = 1_500;
// Require stale to persist briefly before clearing live targets.
// This avoids cancel/reprovide ping-pong during short WS reconnect flaps.
const BOOK_SIDE_STALE_CLEAR_HOLD_MS: u64 = 2_500;
const LIVE_OBS_MIN_PLACED_SAMPLE: u64 = 10;
const LIVE_OBS_REPLACE_RATIO_WARN: f64 = 0.45;
const LIVE_OBS_REPLACE_RATIO_ALERT: f64 = 0.65;
const LIVE_OBS_REPLACE_PER_MIN_WARN: f64 = 4.0;
const LIVE_OBS_REPLACE_PER_MIN_ALERT: f64 = 6.0;
const LIVE_OBS_REPRICE_RATIO_WARN: f64 = 0.20;
const LIVE_OBS_REPRICE_RATIO_ALERT: f64 = 0.35;
const LIVE_OBS_REF_BLOCKED_WARN_MS: u64 = 15_000;
const LIVE_OBS_REF_BLOCKED_ALERT_MS: u64 = 30_000;
const LIVE_OBS_REF_BLOCKED_SOURCE_ONLY_WARN_MS: u64 = 60_000;
const LIVE_OBS_REF_BLOCKED_SOURCE_ONLY_ALERT_MS: u64 = 120_000;
const LIVE_OBS_HEAT_EVENTS_WARN: u64 = 10;
const LIVE_OBS_HEAT_EVENTS_ALERT: u64 = 14;
const LIVE_OBS_PAIR_ARB_HEAT_EVENTS_WARN: u64 = 20;
const LIVE_OBS_PAIR_ARB_SOFTENED_ALERT_RATIO: f64 = 0.50;
const LIVE_OBS_PAIR_ARB_MIN_GATE_ATTEMPTS_FOR_HEAT: u64 = 1_000;
const PAIR_ARB_GATE_SUMMARY_SECS: u64 = 30;
#[allow(dead_code)]
const PAIR_ARB_OPPOSITE_SLOT_BLOCK_MS: u64 = 30_000;
#[allow(dead_code)]
const PAIR_ARB_OPPOSITE_SLOT_BLOCK_TTL_MS: u64 = 10_000;

#[path = "coordinator_endgame.rs"]
mod coordinator_endgame;
#[path = "coordinator_execution.rs"]
mod coordinator_execution;
#[path = "coordinator_metrics.rs"]
mod coordinator_metrics;
#[path = "coordinator_order_io.rs"]
mod coordinator_order_io;
#[path = "coordinator_pricing.rs"]
mod coordinator_pricing;

// ─────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Strategy implementation selected at runtime.
    pub strategy: StrategyKind,
    /// Total pair cost ceiling.
    pub pair_target: f64,
    /// Early-entry pair band used by unified buy-only strategies before a full pair is locked.
    pub open_pair_band: f64,
    /// Maximum absolute inventory imbalance.
    pub max_net_diff: f64,
    /// Order size per bid.
    pub bid_size: f64,
    /// Dip-buy strategy entry cap (buy only when ask <= this price).
    pub dip_buy_max_entry_price: f64,
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
    /// GLFT inventory-risk coefficient.
    pub glft_gamma: f64,
    /// GLFT xi coefficient. Default equals gamma; zero is unsupported in V1.
    pub glft_xi: f64,
    /// GLFT OFI -> alpha coefficient.
    pub glft_ofi_alpha: f64,
    /// GLFT OFI -> spread widening coefficient.
    pub glft_ofi_spread_beta: f64,
    /// GLFT minimum half spread floor (ticks per side).
    pub glft_min_half_spread_ticks: f64,
    /// Time-decay amplifier k. Effective skew_factor = as_skew_factor * (1 + k * elapsed_frac).
    /// 0.0 disables decay. Default: 2.0 (up to 3× at expiry).
    pub as_time_decay_k: f64,
    /// PairArb dominant-side avg-cost cap multiplier when 5 <= |net_diff| < 10.
    pub pair_arb_tier_1_mult: f64,
    /// PairArb dominant-side avg-cost cap multiplier when |net_diff| >= 10.
    pub pair_arb_tier_2_mult: f64,
    /// PairArb safety margin kept below pair_target when deriving VWAP ceiling.
    pub pair_arb_pair_cost_safety_margin: f64,
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
    /// Deprecated. Kept only for compatibility; no longer used for risk clamping.
    pub max_loss_pct: f64,
    /// Endgame edge-hold entry threshold (best_bid / avg_cost) at HardClose entry.
    pub endgame_edge_keep_mult: f64,
    /// Endgame edge-hold exit threshold (best_bid / avg_cost) during HardClose.
    pub endgame_edge_exit_mult: f64,
    /// DRY-RUN mode.
    pub dry_run: bool,
    /// Configurable TTL for stale book data (ms). Default 3000ms.
    pub stale_ttl_ms: u64,
    /// Periodic watchdog tick (ms) to enforce stale/toxic cancels even when md stream is silent.
    pub watchdog_tick_ms: u64,
    /// Periodic strategy metrics snapshot cadence (seconds). 0 disables.
    pub strategy_metrics_log_secs: u64,
    /// Hold-down window after toxicity recovers to prevent rapid cancel/place oscillation.
    pub toxic_recovery_hold_ms: u64,
    /// Soft-close window (seconds before market end).
    /// In this phase, provide orders that increase current directional exposure are disabled.
    pub endgame_soft_close_secs: u64,
    /// Hard-close window (seconds before market end).
    /// In this phase, all provide orders are disabled and only hedge orders are allowed.
    pub endgame_hard_close_secs: u64,
    /// Freeze window (seconds before market end).
    /// In this phase, no new risk is added (provide disabled), but de-risking
    /// hedges are still allowed.
    pub endgame_freeze_secs: u64,
    /// Minimum remaining seconds required to keep trying maker repair during HardClose.
    /// If remaining <= this threshold and keep-mode is not active, switch to taker de-risk.
    pub endgame_maker_repair_min_secs: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            strategy: StrategyKind::GabagoolGrid,
            pair_target: 0.99,
            open_pair_band: 0.99,
            max_net_diff: 5.0,
            bid_size: 2.0,
            dip_buy_max_entry_price: 0.20,
            tick_size: 0.01,
            post_only_safety_ticks: 2.0,
            post_only_tight_spread_ticks: 3.0,
            post_only_extra_tight_ticks: 1.0,
            reprice_threshold: 0.010, // Increased to reduce churn (1 cent drift)
            debounce_ms: 500,         // Increased to reduce churn (half second)
            as_skew_factor: 0.08,     // Original strictly conservative A-S
            glft_gamma: 0.10,
            glft_xi: 0.10,
            glft_ofi_alpha: 0.30,
            glft_ofi_spread_beta: 1.00,
            glft_min_half_spread_ticks: 5.0,
            as_time_decay_k: 2.0, // Up to 3× skew at expiry (1 + 2 * elapsed_frac)
            pair_arb_tier_1_mult: 0.80,
            pair_arb_tier_2_mult: 0.60,
            pair_arb_pair_cost_safety_margin: 0.02,
            market_end_ts: None,
            hedge_debounce_ms: 100, // Hedge orders bypass normal 500ms debounce
            max_portfolio_cost: 1.02, // Emergency hedge ceiling
            min_order_size: 1.0,
            min_hedge_size: 0.0,
            hedge_round_up: false,
            hedge_min_marketable_notional: 0.0,
            hedge_min_marketable_max_extra: 0.5,
            hedge_min_marketable_max_extra_pct: 0.15,
            max_loss_pct: 0.02,
            endgame_edge_keep_mult: 1.5,
            endgame_edge_exit_mult: 1.25,
            dry_run: true,
            stale_ttl_ms: 3000,
            watchdog_tick_ms: 500,
            strategy_metrics_log_secs: 15,
            toxic_recovery_hold_ms: 1200,
            endgame_soft_close_secs: 35,
            endgame_hard_close_secs: 12,
            endgame_freeze_secs: 2,
            endgame_maker_repair_min_secs: 8,
        }
    }
}

impl CoordinatorConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        c.strategy = StrategyKind::from_env_or_default(c.strategy);
        if let Ok(v) = std::env::var("PM_PAIR_TARGET") {
            if let Ok(f) = v.parse() {
                c.pair_target = f;
            }
        }
        if let Ok(v) = std::env::var("PM_OPEN_PAIR_BAND") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    c.open_pair_band = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_OPEN_PAIR_BAND={} (must satisfy 0 < p < 1), using {}",
                        f, c.open_pair_band
                    );
                }
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
        if let Ok(v) = std::env::var("PM_DIP_BUY_MAX_ENTRY_PRICE") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    c.dip_buy_max_entry_price = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_DIP_BUY_MAX_ENTRY_PRICE={} (must satisfy 0 < p < 1), using {}",
                        f, c.dip_buy_max_entry_price
                    );
                }
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
        if let Ok(v) = std::env::var("PM_GLFT_GAMMA") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.glft_gamma = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_GLFT_XI") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.glft_xi = f;
                }
            }
        } else {
            c.glft_xi = c.glft_gamma;
        }
        if c.glft_xi <= 0.0 {
            c.glft_xi = c.glft_gamma.max(1e-6);
        }
        if let Ok(v) = std::env::var("PM_GLFT_OFI_ALPHA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.glft_ofi_alpha = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_GLFT_OFI_SPREAD_BETA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    c.glft_ofi_spread_beta = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MIN_HALF_SPREAD_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.glft_min_half_spread_ticks = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_MIN_HALF_SPREAD_TICKS={} (must be > 0), using {}",
                        f, c.glft_min_half_spread_ticks
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_AS_TIME_DECAY_K") {
            if let Ok(f) = v.parse::<f64>() {
                c.as_time_decay_k = f.max(0.0);
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_TIER_1_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..=1.0).contains(&f) {
                    c.pair_arb_tier_1_mult = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_TIER_1_MULT={} (must satisfy 0 <= x <= 1), using {}",
                        f, c.pair_arb_tier_1_mult
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_TIER_2_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..=1.0).contains(&f) {
                    c.pair_arb_tier_2_mult = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_TIER_2_MULT={} (must satisfy 0 <= x <= 1), using {}",
                        f, c.pair_arb_tier_2_mult
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    c.pair_arb_pair_cost_safety_margin = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN={} (must satisfy 0 <= x < 1), using {}",
                        f, c.pair_arb_pair_cost_safety_margin
                    );
                }
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
                    warn!(
                        "⚠️ PM_MAX_LOSS_PCT is deprecated and ignored (value={:.3})",
                        c.max_loss_pct
                    );
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_MAX_LOSS_PCT={} (must satisfy 0 <= pct < 1), using {}",
                        f, c.max_loss_pct
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_EDGE_KEEP_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.endgame_edge_keep_mult = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_EDGE_EXIT_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    c.endgame_edge_exit_mult = f;
                }
            }
        }
        if c.endgame_edge_exit_mult > c.endgame_edge_keep_mult {
            warn!(
                "⚠️ Clamping PM_ENDGAME_EDGE_EXIT_MULT from {:.4} to {:.4} (must be <= keep_mult)",
                c.endgame_edge_exit_mult, c.endgame_edge_keep_mult
            );
            c.endgame_edge_exit_mult = c.endgame_edge_keep_mult;
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
        if let Ok(v) = std::env::var("PM_STRATEGY_METRICS_LOG_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                c.strategy_metrics_log_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_TOXIC_RECOVERY_HOLD_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                c.toxic_recovery_hold_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_SOFT_CLOSE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                c.endgame_soft_close_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_HARD_CLOSE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                c.endgame_hard_close_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_FREEZE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                c.endgame_freeze_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_MAKER_REPAIR_MIN_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                c.endgame_maker_repair_min_secs = secs;
            }
        }
        // Keep windows ordered: soft >= hard >= freeze.
        if c.endgame_hard_close_secs > c.endgame_soft_close_secs {
            warn!(
                "⚠️ Clamping PM_ENDGAME_HARD_CLOSE_SECS from {} to {} (must be <= soft-close)",
                c.endgame_hard_close_secs, c.endgame_soft_close_secs
            );
            c.endgame_hard_close_secs = c.endgame_soft_close_secs;
        }
        if c.endgame_freeze_secs > c.endgame_hard_close_secs {
            warn!(
                "⚠️ Clamping PM_ENDGAME_FREEZE_SECS from {} to {} (must be <= hard-close)",
                c.endgame_freeze_secs, c.endgame_hard_close_secs
            );
            c.endgame_freeze_secs = c.endgame_hard_close_secs;
        }
        if c.endgame_maker_repair_min_secs > c.endgame_hard_close_secs {
            warn!(
                "⚠️ PM_ENDGAME_MAKER_REPAIR_MIN_SECS={} exceeds hard-close window {}s; maker repair may be skipped in HardClose",
                c.endgame_maker_repair_min_secs, c.endgame_hard_close_secs
            );
        }
        c
    }
}

// ─────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────

/// Last known valid book prices (fallback for empty orderbook).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Book {
    pub(crate) yes_bid: f64,
    pub(crate) yes_ask: f64,
    pub(crate) no_bid: f64,
    pub(crate) no_ask: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EndgamePhase {
    Normal,
    SoftClose,
    HardClose,
    Freeze,
}

#[derive(Debug, Clone, Copy)]
struct EdgeHoldState {
    side: Side,
    keep_allowed: bool,
}

#[derive(Debug, Clone, Copy)]
enum HardCloseAction {
    None,
    Keep {
        side: Side,
        ratio: f64,
        reason: &'static str,
    },
    MakerRepair {
        side: Side,
        ratio: f64,
        reason: &'static str,
    },
    ForceTaker {
        side: Side,
        size: f64,
        ratio: f64,
        reason: &'static str,
    },
}

#[derive(Debug, Default)]
struct Stats {
    ticks: u64,
    placed: u64,
    publish_events: u64,
    replace_events: u64,
    cancel_events: u64,
    publish_from_initial: u64,
    publish_from_policy: u64,
    publish_from_safety: u64,
    publish_from_recovery: u64,
    policy_transition_events: u64,
    policy_noop_ticks: u64,
    cancel_toxic: u64,
    cancel_stale: u64,
    cancel_inv: u64,
    cancel_reprice: u64,
    toxic_kill_signals: u64,
    ofi_heat_events: u64,
    ofi_toxic_events: u64,
    ofi_kill_events: u64,
    ofi_blocked_ticks: u64,
    reference_blocked_ms: u64,
    blocked_due_source: u64,
    blocked_due_binance: u64,
    blocked_due_poly: u64,
    blocked_due_divergence: u64,
    skipped_debounce: u64,
    skipped_backoff: u64,
    skipped_empty_book: u64,
    skipped_inv_limit: u64,
    retain_hits: u64,
    soft_reset_count: u64,
    full_reset_count: u64,
    shadow_suppressed_updates: u64,
    publish_budget_suppressed: u64,
    forced_realign_count: u64,
    forced_realign_hard_count: u64,
    pair_arb_ofi_softened_quotes: u64,
    pair_arb_ofi_suppressed_quotes: u64,
    pair_arb_pairing_upward_reprice: u64,
    pair_arb_keep_candidates: u64,
    pair_arb_skip_inventory_gate: u64,
    pair_arb_skip_simulate_buy_none: u64,
    pair_arb_skip_utility_delta: u64,
    pair_arb_skip_open_edge_not_improved: u64,
    pair_arb_opposite_slot_blocked: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PairArbGateLogSnapshot {
    ofi_softened_quotes: u64,
    ofi_suppressed_quotes: u64,
    keep_candidates: u64,
    skip_inventory_gate: u64,
    skip_simulate_buy_none: u64,
    skip_utility_delta: u64,
    skip_open_edge_not_improved: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LiveObsLevel {
    Ok,
    Warn,
    Alert,
}

impl LiveObsLevel {
    fn as_tag(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Warn => "WARN",
            Self::Alert => "ALERT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyPublishCause {
    Initial,
    Policy,
    Safety,
    Recovery,
}

impl PolicyPublishCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Policy => "policy",
            Self::Safety => "safety",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyTransitionReason {
    Activate,
    Suppress,
    PriceBucket,
    SizeBucket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum QuotePolicySideMode {
    NormalBuy,
    SuppressedBuy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotResetScope {
    Soft,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetentionDecision {
    Retain,
    Republish,
    Clear(CancelReason, SlotResetScope),
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct QuotePolicyState {
    active: bool,
    policy_price: f64,
    action_price: f64,
    size: f64,
    price_band_tick: i64,
    size_bucket: i32,
    side_mode: QuotePolicySideMode,
}

#[derive(Debug, Clone, Copy)]
struct ExecutionState {
    intent_yes: Option<StrategyIntent>,
    intent_no: Option<StrategyIntent>,
    net_diff: f64,
    hedge_dispatched_yes: bool,
    hedge_dispatched_no: bool,
    allow_yes_provide: bool,
    allow_no_provide: bool,
    block_yes_provide: bool,
    block_no_provide: bool,
    block_reason_yes: Option<CancelReason>,
    block_reason_no: Option<CancelReason>,
    force_taker_side: Option<Side>,
    force_taker_size: f64,
    block_maker_hedge: bool,
    endgame_phase: EndgamePhase,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StrategyInventoryMetrics {
    pub(crate) paired_qty: f64,
    pub(crate) pair_cost: f64,
    pub(crate) paired_locked_pnl: f64,
    pub(crate) total_spent: f64,
    pub(crate) worst_case_outcome_pnl: f64,
    pub(crate) dominant_side: Option<Side>,
    pub(crate) residual_qty: f64,
    pub(crate) residual_inventory_value: f64,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct ProjectedBuyMetrics {
    pub(crate) projected_inventory: InventoryState,
    pub(crate) projected_yes_qty: f64,
    pub(crate) projected_no_qty: f64,
    pub(crate) projected_total_cost: f64,
    pub(crate) projected_abs_net_diff: f64,
    pub(crate) metrics: StrategyInventoryMetrics,
}

#[derive(Debug, Clone, Copy)]
enum ProvideSideAction {
    None,
    Place { intent: StrategyIntent },
    Clear { reason: CancelReason },
}

impl ExecutionState {
    fn mark_hedge_dispatched(&mut self, side: Side) {
        match side {
            Side::Yes => {
                self.hedge_dispatched_yes = true;
                self.intent_yes = None;
            }
            Side::No => {
                self.hedge_dispatched_no = true;
                self.intent_no = None;
            }
        }
    }

    fn block_provide(&mut self, side: Side, reason: CancelReason) {
        match side {
            Side::Yes => {
                self.block_yes_provide = true;
                self.block_reason_yes = Some(reason);
            }
            Side::No => {
                self.block_no_provide = true;
                self.block_reason_no = Some(reason);
            }
        }
    }

    fn disable_all_provide_with_reason(&mut self, reason: CancelReason) {
        self.allow_yes_provide = false;
        self.allow_no_provide = false;
        self.block_yes_provide = true;
        self.block_no_provide = true;
        self.block_reason_yes = Some(reason);
        self.block_reason_no = Some(reason);
    }

    fn apply_blocked_provide(&mut self) {
        if self.block_yes_provide {
            self.allow_yes_provide = false;
        }
        if self.block_no_provide {
            self.allow_no_provide = false;
        }
    }

    fn intent_for(&self, side: Side) -> Option<StrategyIntent> {
        match side {
            Side::Yes => self.intent_yes,
            Side::No => self.intent_no,
        }
    }

    fn buy_price_for(&self, side: Side) -> f64 {
        match self.intent_for(side) {
            Some(intent) if intent.direction == TradeDirection::Buy => intent.price,
            _ => 0.0,
        }
    }

    fn has_sell_intent_for(&self, side: Side) -> bool {
        matches!(
            self.intent_for(side),
            Some(intent) if intent.direction == TradeDirection::Sell
        )
    }

    fn allow_provide_for(&self, side: Side) -> bool {
        match side {
            Side::Yes => self.allow_yes_provide,
            Side::No => self.allow_no_provide,
        }
    }

    fn block_reason_for(&self, side: Side) -> Option<CancelReason> {
        match side {
            Side::Yes => self.block_reason_yes,
            Side::No => self.block_reason_no,
        }
    }

    fn hedge_dispatched_for(&self, side: Side) -> bool {
        match side {
            Side::Yes => self.hedge_dispatched_yes,
            Side::No => self.hedge_dispatched_no,
        }
    }
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
    yes_stale_since: Option<Instant>,
    no_stale_since: Option<Instant>,
    slot_targets: [Option<DesiredTarget>; 4],
    slot_last_ts: [Instant; 4],
    slot_shadow_targets: [Option<DesiredTarget>; 4],
    slot_shadow_since: [Option<Instant>; 4],
    slot_shadow_velocity_tps: [f64; 4],
    slot_shadow_last_change_ts: [Option<Instant>; 4],
    slot_policy_candidates: [Option<QuotePolicyState>; 4],
    slot_policy_candidate_since: [Option<Instant>; 4],
    slot_policy_states: [Option<QuotePolicyState>; 4],
    slot_policy_since: [Option<Instant>; 4],
    slot_last_policy_transition: [Option<PolicyTransitionReason>; 4],
    slot_last_publish_reason: [Option<PolicyPublishCause>; 4],
    slot_last_recovery_publish_at: [Option<Instant>; 4],
    slot_last_recovery_cross_seen_at: [Option<Instant>; 4],
    slot_last_regime_seen: [Option<crate::polymarket::glft::QuoteRegime>; 4],
    slot_regime_changed_at: [Instant; 4],
    slot_publish_budget: [f64; 4],
    slot_last_budget_refill: [Instant; 4],
    slot_publish_debt_accum: [f64; 4],
    slot_last_debt_refill: [Instant; 4],
    slot_absent_clear_since: [Option<Instant>; 4],
    slot_pair_arb_state_keys: [Option<PairArbStateKey>; 4],
    slot_pair_arb_intent_state_keys: [Option<PairArbStateKey>; 4],
    slot_pair_arb_fill_recheck_pending: [bool; 4],
    slot_pair_arb_cross_reject_extra_ticks: [u8; 4],
    slot_pair_arb_last_cross_rejected_action_price: [Option<f64>; 4],
    pair_arb_slot_blocked_for_ms: [u64; 4],
    pair_arb_slot_blocked_at: [Option<Instant>; 4],
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
    was_hot_yes: bool,
    was_hot_no: bool,
    was_toxic_yes: bool,
    was_toxic_no: bool,
    glft_ready_seen: bool,
    reference_blocked_since: Option<Instant>,
    glft_source_blocked_since: Option<Instant>,
    glft_source_blocked_saw_binance: bool,
    glft_source_blocked_saw_poly: bool,
    glft_republish_settle_until: Option<Instant>,
    glft_recovery_force_clear_pending: bool,
    last_metrics_log_ts: Instant,
    pair_arb_gate_last_log_ts: Instant,
    pair_arb_gate_last_snapshot: PairArbGateLogSnapshot,
    last_settled_inv_snapshot: InventoryState,
    last_working_inv_snapshot: InventoryState,
    pair_arb_progress_state: PairProgressState,
    round_realized_pair_metrics: RoundRealizedPairMetrics,
    last_endgame_phase: EndgamePhase,
    edge_hold_state: Option<EdgeHoldState>,
    yes_maker_friction: MakerFriction,
    no_maker_friction: MakerFriction,
    stats: Stats,

    ofi_rx: watch::Receiver<OfiSnapshot>,
    inv_rx: watch::Receiver<InventorySnapshot>,
    md_rx: watch::Receiver<MarketDataMsg>,
    glft_rx: watch::Receiver<GlftSignalSnapshot>,
    om_tx: mpsc::Sender<OrderManagerCmd>,
    /// Opt-4: Direct high-priority kill channel from OFI Engine.
    /// Fires on toxicity onset without waiting for the next book tick.
    kill_rx: mpsc::Receiver<KillSwitchSignal>,
    /// Execution-layer feedback channel used for adaptive maker safety.
    feedback_rx: mpsc::Receiver<ExecutionFeedback>,
    /// Slot lifecycle release events from OMS.
    slot_release_rx: mpsc::Receiver<SlotReleaseEvent>,
    /// Optional low-overhead observability snapshot channel for round validation.
    obs_tx: Option<watch::Sender<CoordinatorObsSnapshot>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MakerFriction {
    extra_safety_ticks: u8,
    last_cross_reject_ts: Option<Instant>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CoordinatorObsSnapshot {
    pub ticks: u64,
    pub placed: u64,
    pub publish_events: u64,
    pub replace_events: u64,
    pub cancel_events: u64,
    pub publish_from_initial: u64,
    pub publish_from_policy: u64,
    pub publish_from_safety: u64,
    pub publish_from_recovery: u64,
    pub policy_transition_events: u64,
    pub policy_noop_ticks: u64,
    pub cancel_reprice: u64,
    pub cancel_toxic: u64,
    pub cancel_stale: u64,
    pub cancel_inv: u64,
    pub ofi_heat_events: u64,
    pub ofi_toxic_events: u64,
    pub ofi_kill_events: u64,
    pub ofi_blocked_ticks: u64,
    pub reference_blocked_ms: u64,
    pub blocked_due_source: u64,
    pub blocked_due_divergence: u64,
    pub retain_hits: u64,
    pub soft_reset_count: u64,
    pub full_reset_count: u64,
    pub shadow_suppressed_updates: u64,
    pub publish_budget_suppressed: u64,
    pub forced_realign_count: u64,
    pub forced_realign_hard_count: u64,
    pub pair_arb_ofi_softened_quotes: u64,
    pub pair_arb_ofi_suppressed_quotes: u64,
    pub pair_arb_pairing_upward_reprice: u64,
    pub pair_arb_keep_candidates: u64,
    pub pair_arb_skip_inventory_gate: u64,
    pub pair_arb_skip_simulate_buy_none: u64,
    pub pair_arb_skip_utility_delta: u64,
    pub pair_arb_skip_open_edge_not_improved: u64,
    pub pair_arb_opposite_slot_blocked: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairArbNetBucket {
    Flat,
    Low,
    Mid,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PairArbStateKey {
    pub(crate) dominant_side: Option<Side>,
    pub(crate) net_bucket: PairArbNetBucket,
    pub(crate) soft_close_active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairProgressRegime {
    Healthy,
    Stalled,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PairProgressState {
    pub(crate) last_pair_progress_at: Option<Instant>,
    pub(crate) last_pair_progress_paired_qty: f64,
    pub(crate) last_risk_increasing_fill_price_yes: Option<f64>,
    pub(crate) last_risk_increasing_fill_price_no: Option<f64>,
    pub(crate) last_risk_fill_net_bucket_yes: Option<PairArbNetBucket>,
    pub(crate) last_risk_fill_net_bucket_no: Option<PairArbNetBucket>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RoundRealizedPairMetrics {
    pub(crate) realized_pair_qty: f64,
    pub(crate) realized_pair_locked_pnl: f64,
    pub(crate) merged_cash_released: f64,
}

impl StrategyCoordinator {
    pub fn new(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventorySnapshot>,
        md_rx: watch::Receiver<MarketDataMsg>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
    ) -> Self {
        // Create aux channels that are immediately closed (sender dropped).
        // The select loop uses `Some(x) = recv()` which skips closed channels.
        let (_dead_kill_tx, dead_kill_rx) = mpsc::channel(1);
        let (_dead_feedback_tx, dead_feedback_rx) = mpsc::channel(1);
        let (_dead_release_tx, dead_release_rx) = mpsc::channel(1);
        let (_dead_glft_tx, dead_glft_rx) = watch::channel(GlftSignalSnapshot::default());
        Self::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            dead_glft_rx,
            om_tx,
            dead_kill_rx,
            dead_feedback_rx,
            dead_release_rx,
        )
    }

    /// Opt-4: Construct with a direct OFI→Coordinator kill channel.
    pub fn with_kill_rx(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventorySnapshot>,
        md_rx: watch::Receiver<MarketDataMsg>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
    ) -> Self {
        let (_dead_feedback_tx, dead_feedback_rx) = mpsc::channel(1);
        let (_dead_release_tx, dead_release_rx) = mpsc::channel(1);
        let (_dead_glft_tx, dead_glft_rx) = watch::channel(GlftSignalSnapshot::default());
        Self::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            dead_glft_rx,
            om_tx,
            kill_rx,
            dead_feedback_rx,
            dead_release_rx,
        )
    }

    pub fn with_aux_rx(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventorySnapshot>,
        md_rx: watch::Receiver<MarketDataMsg>,
        glft_rx: watch::Receiver<GlftSignalSnapshot>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
        feedback_rx: mpsc::Receiver<ExecutionFeedback>,
        slot_release_rx: mpsc::Receiver<SlotReleaseEvent>,
    ) -> Self {
        let now = Instant::now();
        let last_metrics_log_ts = if cfg.strategy_metrics_log_secs > 0 {
            now.checked_sub(Duration::from_secs(cfg.strategy_metrics_log_secs))
                .unwrap_or(now)
        } else {
            now
        };
        Self {
            cfg,
            book: Book::default(),
            last_valid_book: Book::default(),
            last_valid_ts_yes: Instant::now(),
            last_valid_ts_no: Instant::now(),
            yes_stale_since: None,
            no_stale_since: None,
            slot_targets: std::array::from_fn(|_| None),
            slot_last_ts: std::array::from_fn(|_| {
                Instant::now() - std::time::Duration::from_secs(60)
            }),
            slot_shadow_targets: std::array::from_fn(|_| None),
            slot_shadow_since: std::array::from_fn(|_| None),
            slot_shadow_velocity_tps: [0.0; 4],
            slot_shadow_last_change_ts: std::array::from_fn(|_| None),
            slot_policy_candidates: std::array::from_fn(|_| None),
            slot_policy_candidate_since: std::array::from_fn(|_| None),
            slot_policy_states: std::array::from_fn(|_| None),
            slot_policy_since: std::array::from_fn(|_| None),
            slot_last_policy_transition: std::array::from_fn(|_| None),
            slot_last_publish_reason: std::array::from_fn(|_| None),
            slot_last_recovery_publish_at: std::array::from_fn(|_| None),
            slot_last_recovery_cross_seen_at: std::array::from_fn(|_| None),
            slot_last_regime_seen: std::array::from_fn(|_| None),
            slot_regime_changed_at: std::array::from_fn(|_| Instant::now()),
            slot_publish_budget: [2.0; 4],
            slot_last_budget_refill: std::array::from_fn(|_| Instant::now()),
            slot_publish_debt_accum: [0.0; 4],
            slot_last_debt_refill: std::array::from_fn(|_| Instant::now()),
            slot_absent_clear_since: std::array::from_fn(|_| None),
            slot_pair_arb_state_keys: std::array::from_fn(|_| None),
            slot_pair_arb_intent_state_keys: std::array::from_fn(|_| None),
            slot_pair_arb_fill_recheck_pending: [false; 4],
            slot_pair_arb_cross_reject_extra_ticks: [0; 4],
            slot_pair_arb_last_cross_rejected_action_price: std::array::from_fn(|_| None),
            pair_arb_slot_blocked_for_ms: [0; 4],
            pair_arb_slot_blocked_at: [None; 4],
            yes_target: None,
            no_target: None,
            yes_last_ts: Instant::now() - std::time::Duration::from_secs(60),
            no_last_ts: Instant::now() - std::time::Duration::from_secs(60),
            market_start: Instant::now(),
            yes_toxic_hold_until: Instant::now() - std::time::Duration::from_secs(60),
            no_toxic_hold_until: Instant::now() - std::time::Duration::from_secs(60),
            was_hot_yes: false,
            was_hot_no: false,
            was_toxic_yes: false,
            was_toxic_no: false,
            glft_ready_seen: false,
            reference_blocked_since: None,
            glft_source_blocked_since: None,
            glft_source_blocked_saw_binance: false,
            glft_source_blocked_saw_poly: false,
            glft_republish_settle_until: None,
            glft_recovery_force_clear_pending: false,
            last_metrics_log_ts,
            pair_arb_gate_last_log_ts: now,
            pair_arb_gate_last_snapshot: PairArbGateLogSnapshot::default(),
            last_settled_inv_snapshot: InventoryState::default(),
            last_working_inv_snapshot: InventoryState::default(),
            pair_arb_progress_state: PairProgressState::default(),
            round_realized_pair_metrics: RoundRealizedPairMetrics::default(),
            last_endgame_phase: EndgamePhase::Normal,
            edge_hold_state: None,
            yes_maker_friction: MakerFriction::default(),
            no_maker_friction: MakerFriction::default(),
            stats: Stats::default(),
            ofi_rx,
            inv_rx,
            md_rx,
            glft_rx,
            om_tx,
            kill_rx,
            feedback_rx,
            slot_release_rx,
            obs_tx: None,
        }
    }

    pub fn with_obs_tx(mut self, obs_tx: watch::Sender<CoordinatorObsSnapshot>) -> Self {
        self.obs_tx = Some(obs_tx);
        self
    }

    pub(crate) fn current_inventory_snapshot(&self) -> InventorySnapshot {
        *self.inv_rx.borrow()
    }

    pub(crate) fn current_working_inventory(&self) -> InventoryState {
        self.current_inventory_snapshot().working
    }

    pub(crate) fn cfg(&self) -> &CoordinatorConfig {
        &self.cfg
    }

    pub async fn run(mut self) {
        info!(
            "🎯 Coordinator [OCCAM+LEADLAG] strategy={} pair={:.2} open_pair_band={:.2} bid={:.1} dip_cap={:.2} tick={:.3} net={:.0} reprice={:.3} debounce={}ms watchdog={}ms metrics_log={}s endgame(soft/hard/freeze/maker_repair_min)={}/{}/{}/{}s edge(keep/exit)={:.2}/{:.2} dry={}",
            self.cfg.strategy.as_str(),
            self.cfg.pair_target, self.cfg.open_pair_band, self.cfg.bid_size, self.cfg.dip_buy_max_entry_price, self.cfg.tick_size,
            self.cfg.max_net_diff, self.cfg.reprice_threshold, self.cfg.debounce_ms, self.cfg.watchdog_tick_ms,
            self.cfg.strategy_metrics_log_secs,
            self.cfg.endgame_soft_close_secs, self.cfg.endgame_hard_close_secs, self.cfg.endgame_freeze_secs, self.cfg.endgame_maker_repair_min_secs,
            self.cfg.endgame_edge_keep_mult, self.cfg.endgame_edge_exit_mult,
            self.cfg.dry_run,
        );
        let mut watchdog = tokio::time::interval(Duration::from_millis(self.cfg.watchdog_tick_ms));
        self.emit_obs_snapshot();

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
                    self.stats.toxic_kill_signals = self.stats.toxic_kill_signals.saturating_add(1);
                    self.stats.ofi_kill_events = self.stats.ofi_kill_events.saturating_add(1);
                    warn!(
                        "⚡ DIRECT KILL from OFI | reason=adverse_selection side={:?} ofi={:.1} — immediate re-eval",
                        sig.side, sig.ofi_score,
                    );
                    // Re-use the full tick() logic which reads latest OFI watch state.
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                Some(feedback) = self.feedback_rx.recv() => {
                    self.handle_execution_feedback(feedback);
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                Some(release) = self.slot_release_rx.recv() => {
                    self.handle_slot_release_event(release).await;
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                // Market data tick (watch/coalescing)
                changed = self.md_rx.changed() => {
                    if changed.is_err() {
                        break; // Sender dropped
                    }
                    let msg = self.md_rx.borrow().clone();
                    self.handle_market_data(msg).await;
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                // Watchdog tick: enforce stale/toxic risk checks even when market WS is temporarily silent.
                _ = watchdog.tick() => {
                    self.tick().await;
                    self.emit_obs_snapshot();
                }
            }
        }

        let final_inv = *self.inv_rx.borrow();
        let final_metrics = self.derive_inventory_metrics(&final_inv.working);
        self.flush_reference_blocked_time(Instant::now());
        let dominant_side = match final_metrics.dominant_side {
            Some(Side::Yes) => "YES",
            Some(Side::No) => "NO",
            None => "FLAT",
        };
        info!(
            "🎯 FinalMetrics | paired_qty={:.2} pair_cost={:.4} paired_locked_pnl={:.4} worst_case_outcome_pnl={:.4} total_spent={:.4} dominant={} residual_qty={:.2} residual_value={:.4} realized_pair_qty={:.2} realized_pair_locked_pnl={:.4} merged_cash_released={:.4}",
            final_metrics.paired_qty,
            final_metrics.pair_cost,
            final_metrics.paired_locked_pnl,
            final_metrics.worst_case_outcome_pnl,
            final_metrics.total_spent,
            dominant_side,
            final_metrics.residual_qty,
            final_metrics.residual_inventory_value,
            self.round_realized_pair_metrics.realized_pair_qty,
            self.round_realized_pair_metrics.realized_pair_locked_pnl,
            self.round_realized_pair_metrics.merged_cash_released,
        );
        info!(
            "🎯 Shutdown | ticks={} placed={} publish(events={} replace={} cancel={} initial={} policy={} safety={} recovery={}) policy(transitions={} noop_ticks={}) cancel(toxic={} stale={} inv={} reprice={}) ofi(heat_events={} toxic_events={} kill_events={} blocked_ticks={} pair_arb_softened={} pair_arb_suppressed={} pairing_upward_reprice={} opposite_slot_blocked={}) pair_arb_gate(keep={} skip_inv={} skip_sim={} skip_util={} skip_edge={}) ref(blocked_ms={} source={} source_binance={} source_poly={} divergence={}) retain(hits={} soft_reset={} full_reset={}) publish(shadow_suppressed={} budget_suppressed={} forced_realign(total={} hard={})) skip(debounce={} backoff={} empty={} inv_limit={})",
            self.stats.ticks, self.stats.placed,
            self.stats.publish_events, self.stats.replace_events, self.stats.cancel_events,
            self.stats.publish_from_initial, self.stats.publish_from_policy, self.stats.publish_from_safety, self.stats.publish_from_recovery,
            self.stats.policy_transition_events, self.stats.policy_noop_ticks,
            self.stats.cancel_toxic, self.stats.cancel_stale, self.stats.cancel_inv, self.stats.cancel_reprice,
            self.stats.ofi_heat_events, self.stats.ofi_toxic_events, self.stats.ofi_kill_events, self.stats.ofi_blocked_ticks,
            self.stats.pair_arb_ofi_softened_quotes, self.stats.pair_arb_ofi_suppressed_quotes, self.stats.pair_arb_pairing_upward_reprice, self.stats.pair_arb_opposite_slot_blocked,
            self.stats.pair_arb_keep_candidates, self.stats.pair_arb_skip_inventory_gate, self.stats.pair_arb_skip_simulate_buy_none, self.stats.pair_arb_skip_utility_delta, self.stats.pair_arb_skip_open_edge_not_improved,
            self.stats.reference_blocked_ms, self.stats.blocked_due_source, self.stats.blocked_due_binance, self.stats.blocked_due_poly, self.stats.blocked_due_divergence,
            self.stats.retain_hits, self.stats.soft_reset_count, self.stats.full_reset_count,
            self.stats.shadow_suppressed_updates, self.stats.publish_budget_suppressed, self.stats.forced_realign_count, self.stats.forced_realign_hard_count,
            self.stats.skipped_debounce, self.stats.skipped_backoff, self.stats.skipped_empty_book, self.stats.skipped_inv_limit,
        );
        self.emit_obs_snapshot();
        self.emit_live_observability_tags();
    }

    fn emit_obs_snapshot(&self) {
        let Some(obs_tx) = &self.obs_tx else {
            return;
        };
        let snapshot = CoordinatorObsSnapshot {
            ticks: self.stats.ticks,
            placed: self.stats.placed,
            publish_events: self.stats.publish_events,
            replace_events: self.stats.replace_events,
            cancel_events: self.stats.cancel_events,
            publish_from_initial: self.stats.publish_from_initial,
            publish_from_policy: self.stats.publish_from_policy,
            publish_from_safety: self.stats.publish_from_safety,
            publish_from_recovery: self.stats.publish_from_recovery,
            policy_transition_events: self.stats.policy_transition_events,
            policy_noop_ticks: self.stats.policy_noop_ticks,
            cancel_reprice: self.stats.cancel_reprice,
            cancel_toxic: self.stats.cancel_toxic,
            cancel_stale: self.stats.cancel_stale,
            cancel_inv: self.stats.cancel_inv,
            ofi_heat_events: self.stats.ofi_heat_events,
            ofi_toxic_events: self.stats.ofi_toxic_events,
            ofi_kill_events: self.stats.ofi_kill_events,
            ofi_blocked_ticks: self.stats.ofi_blocked_ticks,
            reference_blocked_ms: self.stats.reference_blocked_ms,
            blocked_due_source: self.stats.blocked_due_source,
            blocked_due_divergence: self.stats.blocked_due_divergence,
            retain_hits: self.stats.retain_hits,
            soft_reset_count: self.stats.soft_reset_count,
            full_reset_count: self.stats.full_reset_count,
            shadow_suppressed_updates: self.stats.shadow_suppressed_updates,
            publish_budget_suppressed: self.stats.publish_budget_suppressed,
            forced_realign_count: self.stats.forced_realign_count,
            forced_realign_hard_count: self.stats.forced_realign_hard_count,
            pair_arb_ofi_softened_quotes: self.stats.pair_arb_ofi_softened_quotes,
            pair_arb_ofi_suppressed_quotes: self.stats.pair_arb_ofi_suppressed_quotes,
            pair_arb_pairing_upward_reprice: self.stats.pair_arb_pairing_upward_reprice,
            pair_arb_keep_candidates: self.stats.pair_arb_keep_candidates,
            pair_arb_skip_inventory_gate: self.stats.pair_arb_skip_inventory_gate,
            pair_arb_skip_simulate_buy_none: self.stats.pair_arb_skip_simulate_buy_none,
            pair_arb_skip_utility_delta: self.stats.pair_arb_skip_utility_delta,
            pair_arb_skip_open_edge_not_improved: self.stats.pair_arb_skip_open_edge_not_improved,
            pair_arb_opposite_slot_blocked: self.stats.pair_arb_opposite_slot_blocked,
        };
        let _ = obs_tx.send(snapshot);
    }

    fn record_strategy_quote_diagnostics(&mut self, quotes: &StrategyQuotes) {
        self.stats.pair_arb_ofi_softened_quotes = self
            .stats
            .pair_arb_ofi_softened_quotes
            .saturating_add(quotes.diagnostics.pair_arb_ofi_softened_quotes as u64);
        self.stats.pair_arb_ofi_suppressed_quotes = self
            .stats
            .pair_arb_ofi_suppressed_quotes
            .saturating_add(quotes.diagnostics.pair_arb_ofi_suppressed_quotes as u64);
        self.stats.pair_arb_keep_candidates = self
            .stats
            .pair_arb_keep_candidates
            .saturating_add(quotes.diagnostics.pair_arb_keep_candidates as u64);
        self.stats.pair_arb_skip_inventory_gate = self
            .stats
            .pair_arb_skip_inventory_gate
            .saturating_add(quotes.diagnostics.pair_arb_skip_inventory_gate as u64);
        self.stats.pair_arb_skip_simulate_buy_none = self
            .stats
            .pair_arb_skip_simulate_buy_none
            .saturating_add(quotes.diagnostics.pair_arb_skip_simulate_buy_none as u64);
        self.stats.pair_arb_skip_utility_delta = self
            .stats
            .pair_arb_skip_utility_delta
            .saturating_add(quotes.diagnostics.pair_arb_skip_utility_delta as u64);
        self.stats.pair_arb_skip_open_edge_not_improved = self
            .stats
            .pair_arb_skip_open_edge_not_improved
            .saturating_add(quotes.diagnostics.pair_arb_skip_open_edge_not_improved as u64);
    }

    pub(super) fn execution_toxic_block_applies(&self) -> bool {
        self.cfg.strategy != StrategyKind::PairArb
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
            self.yes_stale_since = None;
        }
        if nb > 0.0 && na > 0.0 {
            self.last_valid_book.no_bid = nb;
            self.last_valid_book.no_ask = na;
            self.last_valid_ts_no = Instant::now();
            self.no_stale_since = None;
        }
    }

    /// P5 FIX: Check if either side's book data is stale (>30s without fresh data).
    /// Uses per-side timestamps so YES updates don't mask NO staleness.
    fn is_book_stale(&self) -> bool {
        let limit = std::time::Duration::from_secs(30);
        self.last_valid_ts_yes.elapsed() > limit || self.last_valid_ts_no.elapsed() > limit
    }

    fn stale_side_actionable(
        now: Instant,
        side_stale_raw: bool,
        stale_since: &mut Option<Instant>,
    ) -> bool {
        if !side_stale_raw {
            *stale_since = None;
            return false;
        }
        let since = stale_since.get_or_insert(now);
        now.saturating_duration_since(*since)
            >= Duration::from_millis(BOOK_SIDE_STALE_CLEAR_HOLD_MS)
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

    pub(super) fn glft_is_tradeable_snapshot(
        &self,
        glft: crate::polymarket::glft::GlftSignalSnapshot,
    ) -> bool {
        if self.cfg.strategy != StrategyKind::GlftMm {
            return true;
        }
        glft.ready
            && !glft.stale
            && matches!(
                glft.signal_state,
                crate::polymarket::glft::GlftSignalState::Live
            )
            && !matches!(
                glft.quote_regime,
                crate::polymarket::glft::QuoteRegime::Blocked
            )
    }

    pub(super) fn glft_is_tradeable_now(&self) -> bool {
        self.glft_is_tradeable_snapshot(*self.glft_rx.borrow())
    }

    pub(super) fn glft_should_retain_on_short_source_block(
        &self,
        glft: crate::polymarket::glft::GlftSignalSnapshot,
        now: Instant,
    ) -> bool {
        if self.cfg.strategy != StrategyKind::GlftMm || self.glft_is_tradeable_snapshot(glft) {
            return false;
        }
        if !matches!(
            glft.quote_regime,
            crate::polymarket::glft::QuoteRegime::Blocked
        ) {
            return false;
        }
        let source_blocked =
            glft.readiness_blockers.await_binance || glft.readiness_blockers.await_poly_book;
        if !source_blocked {
            return false;
        }
        let Some(since) = self.glft_source_blocked_since else {
            return false;
        };
        let blocked_for = now.saturating_duration_since(since);
        let hold_ms = if glft.readiness_blockers.await_binance {
            GLFT_SOURCE_BLOCK_RETAIN_HOLD_BINANCE_MS
        } else {
            GLFT_SOURCE_BLOCK_RETAIN_HOLD_MS
        };
        blocked_for < Duration::from_millis(hold_ms)
    }

    pub(super) fn note_cancel_reason(&mut self, reason: CancelReason) {
        match reason {
            CancelReason::ToxicFlow => {
                self.stats.cancel_toxic = self.stats.cancel_toxic.saturating_add(1)
            }
            CancelReason::StaleData => {
                self.stats.cancel_stale = self.stats.cancel_stale.saturating_add(1)
            }
            CancelReason::InventoryLimit => {
                self.stats.cancel_inv = self.stats.cancel_inv.saturating_add(1)
            }
            CancelReason::Reprice => {
                self.stats.cancel_reprice = self.stats.cancel_reprice.saturating_add(1)
            }
            _ => {}
        }
    }

    pub(super) fn note_skipped_inv_limit(&mut self) {
        self.stats.skipped_inv_limit = self.stats.skipped_inv_limit.saturating_add(1);
    }

    fn obs_ratio(num: u64, den: u64) -> f64 {
        if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        }
    }

    fn obs_level_ge_f64(value: f64, warn: f64, alert: f64) -> LiveObsLevel {
        if value >= alert {
            LiveObsLevel::Alert
        } else if value >= warn {
            LiveObsLevel::Warn
        } else {
            LiveObsLevel::Ok
        }
    }

    fn obs_level_ge_u64(value: u64, warn: u64, alert: u64) -> LiveObsLevel {
        if value >= alert {
            LiveObsLevel::Alert
        } else if value >= warn {
            LiveObsLevel::Warn
        } else {
            LiveObsLevel::Ok
        }
    }

    fn pair_arb_obs_heat_level(
        &self,
        pair_arb_gate_attempts: u64,
        pair_arb_softened_ratio: f64,
    ) -> LiveObsLevel {
        if pair_arb_gate_attempts >= LIVE_OBS_PAIR_ARB_MIN_GATE_ATTEMPTS_FOR_HEAT
            && pair_arb_softened_ratio >= LIVE_OBS_PAIR_ARB_SOFTENED_ALERT_RATIO
        {
            LiveObsLevel::Alert
        } else if self.stats.ofi_heat_events >= LIVE_OBS_PAIR_ARB_HEAT_EVENTS_WARN {
            LiveObsLevel::Warn
        } else {
            LiveObsLevel::Ok
        }
    }

    fn emit_live_observability_tags(&self) {
        let replace_ratio = Self::obs_ratio(self.stats.replace_events, self.stats.placed);
        let reprice_ratio_raw = Self::obs_ratio(self.stats.cancel_reprice, self.stats.placed);
        let pair_arb_edge_explained_reprice = if self.cfg.strategy == StrategyKind::PairArb {
            self.stats
                .cancel_reprice
                .min(self.stats.pair_arb_skip_open_edge_not_improved)
        } else {
            0
        };
        let pair_arb_unexplained_reprice = self
            .stats
            .cancel_reprice
            .saturating_sub(pair_arb_edge_explained_reprice);
        let reprice_ratio = if self.cfg.strategy == StrategyKind::PairArb {
            // PairArb can legitimately clear+republish from strategy skip_edge (intent=None)
            // without actual chase behavior. Alert only on unexplained reprice churn.
            Self::obs_ratio(pair_arb_unexplained_reprice, self.stats.placed)
        } else {
            reprice_ratio_raw
        };
        let elapsed_secs = self.market_start.elapsed().as_secs_f64().max(1.0);
        let replace_per_min = (self.stats.replace_events as f64) * 60.0 / elapsed_secs;
        let low_sample = self.stats.placed < LIVE_OBS_MIN_PLACED_SAMPLE;

        let lvl_replace_raw = Self::obs_level_ge_f64(
            replace_ratio,
            LIVE_OBS_REPLACE_RATIO_WARN,
            LIVE_OBS_REPLACE_RATIO_ALERT,
        );
        let lvl_replace_rate = Self::obs_level_ge_f64(
            replace_per_min,
            LIVE_OBS_REPLACE_PER_MIN_WARN,
            LIVE_OBS_REPLACE_PER_MIN_ALERT,
        );
        let lvl_reprice_raw = Self::obs_level_ge_f64(
            reprice_ratio,
            LIVE_OBS_REPRICE_RATIO_WARN,
            LIVE_OBS_REPRICE_RATIO_ALERT,
        );
        // Small dry-run samples produce noisy ratio alerts; downgrade to informational.
        let lvl_replace = if low_sample {
            LiveObsLevel::Ok
        } else if lvl_replace_rate < LiveObsLevel::Warn {
            // Ratio alone is not meaningful for persistent two-slot quoting.
            // Only escalate when absolute replace cadence is also high.
            LiveObsLevel::Ok
        } else if lvl_replace_raw >= LiveObsLevel::Alert && lvl_replace_rate < LiveObsLevel::Alert {
            // High ratio with only moderate absolute cadence should not page as ALERT.
            LiveObsLevel::Warn
        } else {
            lvl_replace_raw.max(lvl_replace_rate)
        };
        let lvl_reprice = if low_sample {
            LiveObsLevel::Ok
        } else {
            lvl_reprice_raw
        };
        let source_only_blocked =
            self.stats.blocked_due_source > 0 && self.stats.blocked_due_divergence == 0;
        let (ref_block_warn_ms, ref_block_alert_ms, ref_block_scope) = if source_only_blocked {
            (
                LIVE_OBS_REF_BLOCKED_SOURCE_ONLY_WARN_MS,
                LIVE_OBS_REF_BLOCKED_SOURCE_ONLY_ALERT_MS,
                "source_only",
            )
        } else {
            (
                LIVE_OBS_REF_BLOCKED_WARN_MS,
                LIVE_OBS_REF_BLOCKED_ALERT_MS,
                "mixed_or_divergence",
            )
        };
        let lvl_ref_blocked = Self::obs_level_ge_u64(
            self.stats.reference_blocked_ms,
            ref_block_warn_ms,
            ref_block_alert_ms,
        );
        let pair_arb_gate_attempts = self
            .stats
            .pair_arb_keep_candidates
            .saturating_add(self.stats.pair_arb_skip_inventory_gate)
            .saturating_add(self.stats.pair_arb_skip_simulate_buy_none)
            .saturating_add(self.stats.pair_arb_skip_utility_delta)
            .saturating_add(self.stats.pair_arb_skip_open_edge_not_improved);
        let pair_arb_softened_ratio = Self::obs_ratio(
            self.stats.pair_arb_ofi_softened_quotes,
            pair_arb_gate_attempts,
        );
        let lvl_heat = if self.cfg.strategy == StrategyKind::PairArb {
            // PairArb OFI is subordinate shaping. Follow the 5-round PLAN_Claude
            // interpretation:
            // - ALERT: shaping impacts >= 50% of gate attempts
            // - WARN: high heat-event regime (informational)
            // Guard with minimum attempts to avoid startup/sample noise.
            self.pair_arb_obs_heat_level(pair_arb_gate_attempts, pair_arb_softened_ratio)
        } else {
            Self::obs_level_ge_u64(
                self.stats.ofi_heat_events,
                LIVE_OBS_HEAT_EVENTS_WARN,
                LIVE_OBS_HEAT_EVENTS_ALERT,
            )
        };
        let lvl_toxic = if self.stats.ofi_toxic_events > 0 || self.stats.ofi_kill_events > 0 {
            LiveObsLevel::Warn
        } else {
            LiveObsLevel::Ok
        };
        let lvl_source_block = if self.stats.blocked_due_source >= 2 {
            LiveObsLevel::Warn
        } else {
            LiveObsLevel::Ok
        };

        let round_level = [
            lvl_replace,
            lvl_reprice,
            lvl_ref_blocked,
            lvl_heat,
            lvl_toxic,
            lvl_source_block,
        ]
        .into_iter()
        .max()
        .unwrap_or(LiveObsLevel::Ok);

        let mut flags: Vec<&str> = Vec::new();
        if low_sample {
            flags.push("low_sample");
        }
        if lvl_replace >= LiveObsLevel::Warn {
            flags.push("replace_ratio");
        }
        if lvl_reprice >= LiveObsLevel::Warn {
            flags.push("reprice_ratio");
        }
        if self.cfg.strategy == StrategyKind::PairArb && pair_arb_edge_explained_reprice > 0 {
            flags.push("reprice_edge_explained");
        }
        if lvl_ref_blocked >= LiveObsLevel::Warn {
            flags.push("reference_blocked_ms");
        }
        if lvl_heat >= LiveObsLevel::Warn {
            flags.push("ofi_heat_events");
        }
        if lvl_toxic >= LiveObsLevel::Warn {
            flags.push("ofi_toxic_or_kill");
        }
        if lvl_source_block >= LiveObsLevel::Warn {
            flags.push("source_blocked");
        }
        let flag_text = if flags.is_empty() {
            "none".to_string()
        } else {
            flags.join(",")
        };

        let msg = format!(
            "🏷️ LIVE_OBS[{}] placed={} sample_floor={} replace_ratio={:.2}({}) replace_per_min={:.2}({}) reprice_ratio={:.2}({}) reprice_ratio_raw={:.2} edge_explained={} ref_blocked_ms={}({};scope={}) heat_events={}({}) pair_arb_softened_ratio={:.2} toxic_events={} kill_events={} source_blocked={}({}) flags={}",
            round_level.as_tag(),
            self.stats.placed,
            LIVE_OBS_MIN_PLACED_SAMPLE,
            replace_ratio,
            lvl_replace.as_tag(),
            replace_per_min,
            lvl_replace_rate.as_tag(),
            reprice_ratio,
            lvl_reprice.as_tag(),
            reprice_ratio_raw,
            pair_arb_edge_explained_reprice,
            self.stats.reference_blocked_ms,
            lvl_ref_blocked.as_tag(),
            ref_block_scope,
            self.stats.ofi_heat_events,
            lvl_heat.as_tag(),
            pair_arb_softened_ratio,
            self.stats.ofi_toxic_events,
            self.stats.ofi_kill_events,
            self.stats.blocked_due_source,
            lvl_source_block.as_tag(),
            flag_text,
        );

        match round_level {
            LiveObsLevel::Alert => warn!("{msg}"),
            LiveObsLevel::Warn => warn!("{msg}"),
            LiveObsLevel::Ok => info!("{msg}"),
        }
    }

    fn update_reference_blocked_time(&mut self, blocked: bool, now: Instant) {
        match (blocked, self.reference_blocked_since) {
            (true, None) => {
                self.reference_blocked_since = Some(now);
            }
            (false, Some(since)) => {
                let ms = now.saturating_duration_since(since).as_millis() as u64;
                self.stats.reference_blocked_ms =
                    self.stats.reference_blocked_ms.saturating_add(ms);
                self.reference_blocked_since = None;
            }
            _ => {}
        }
    }

    fn flush_reference_blocked_time(&mut self, now: Instant) {
        if let Some(since) = self.reference_blocked_since.take() {
            let ms = now.saturating_duration_since(since).as_millis() as u64;
            self.stats.reference_blocked_ms = self.stats.reference_blocked_ms.saturating_add(ms);
        }
    }

    fn glft_republish_settle_window(
        blocked_for: Duration,
        saw_binance: bool,
        saw_poly: bool,
    ) -> Duration {
        let blocked_ms = blocked_for.as_millis() as u64;
        let base_ms = if blocked_ms < 2_000 {
            GLFT_SOURCE_RECOVERY_SETTLE_MIN_MS
        } else if blocked_ms < 6_000 {
            2_400
        } else if blocked_ms < 15_000 {
            3_200
        } else if blocked_ms < 35_000 {
            4_500
        } else {
            6_000
        };
        // Binance stalls are generally more disruptive than transient Poly-book stalls.
        let source_penalty_ms = match (saw_binance, saw_poly) {
            (true, true) => 1_200,
            (true, false) => 900,
            (false, true) => 400,
            (false, false) => 0,
        };
        let settle_ms = (base_ms + source_penalty_ms).clamp(
            GLFT_SOURCE_RECOVERY_SETTLE_MIN_MS,
            GLFT_SOURCE_RECOVERY_SETTLE_MAX_MS,
        );
        Duration::from_millis(settle_ms)
    }

    fn glft_source_block_profile(saw_binance: bool, saw_poly: bool) -> &'static str {
        match (saw_binance, saw_poly) {
            (true, true) => "binance+poly",
            (true, false) => "binance",
            (false, true) => "poly",
            (false, false) => "unknown",
        }
    }

    pub(super) fn glft_republish_settle_remaining(&self, now: Instant) -> Option<Duration> {
        self.glft_republish_settle_until
            .and_then(|until| until.checked_duration_since(now))
    }

    fn update_glft_source_recovery_state(&mut self, snapshot: GlftSignalSnapshot, now: Instant) {
        let source_blocked = matches!(
            snapshot.quote_regime,
            crate::polymarket::glft::QuoteRegime::Blocked
        ) && (snapshot.readiness_blockers.await_binance
            || snapshot.readiness_blockers.await_poly_book);
        if source_blocked {
            if self.glft_source_blocked_since.is_none() {
                self.glft_source_blocked_since = Some(now);
                self.glft_source_blocked_saw_binance = false;
                self.glft_source_blocked_saw_poly = false;
            }
            self.glft_source_blocked_saw_binance |= snapshot.readiness_blockers.await_binance;
            self.glft_source_blocked_saw_poly |= snapshot.readiness_blockers.await_poly_book;
            return;
        }

        if let Some(since) = self.glft_source_blocked_since.take() {
            let blocked_for = now.saturating_duration_since(since);
            if blocked_for < Duration::from_millis(GLFT_SOURCE_RECOVERY_FLAP_IGNORE_MS) {
                debug!(
                    "🧭 GLFT source flap ignored | blocked_for_ms={} (<{}ms)",
                    blocked_for.as_millis(),
                    GLFT_SOURCE_RECOVERY_FLAP_IGNORE_MS
                );
                return;
            }
            let saw_binance = self.glft_source_blocked_saw_binance;
            let saw_poly = self.glft_source_blocked_saw_poly;
            let settle = Self::glft_republish_settle_window(blocked_for, saw_binance, saw_poly);
            self.glft_republish_settle_until = Some(now + settle);
            let reset_shadow_state =
                blocked_for >= Duration::from_millis(GLFT_SOURCE_RECOVERY_RESET_SHADOW_MS);
            // Source-specific recovery continuity:
            // - Poly-only short stalls: preserve policy continuity via soft reset.
            // - Binance-involved or long stalls: keep full reset semantics.
            let use_full_reset = saw_binance || reset_shadow_state;
            self.glft_recovery_force_clear_pending = reset_shadow_state;
            for slot in OrderSlot::ALL {
                self.slot_last_publish_reason[slot.index()] = None;
                self.slot_absent_clear_since[slot.index()] = None;
                self.clear_slot_recovery_publish_state(slot);
                if use_full_reset {
                    self.full_reset_slot_publish_state(slot);
                } else {
                    self.soft_reset_slot_publish_state(slot);
                }
                if reset_shadow_state {
                    self.slot_shadow_targets[slot.index()] = None;
                    self.slot_shadow_since[slot.index()] = Some(now);
                } else if use_full_reset && self.slot_shadow_targets[slot.index()].is_some() {
                    self.slot_shadow_since[slot.index()] = Some(now);
                }
            }
            self.glft_source_blocked_saw_binance = false;
            self.glft_source_blocked_saw_poly = false;
            info!(
                "🧭 GLFT source recovered | blocked_for_ms={} settle_ms={} source_profile={} reset_scope={} reset_shadow={}",
                blocked_for.as_millis(),
                settle.as_millis(),
                Self::glft_source_block_profile(saw_binance, saw_poly),
                if use_full_reset { "full" } else { "soft" },
                reset_shadow_state,
            );
        } else if self
            .glft_republish_settle_until
            .is_some_and(|until| now >= until)
        {
            self.glft_republish_settle_until = None;
            self.glft_source_blocked_saw_binance = false;
            self.glft_source_blocked_saw_poly = false;
        }
    }

    // ═════════════════════════════════════════════════
    // Main tick
    // ═════════════════════════════════════════════════

    async fn tick(&mut self) {
        let now = Instant::now();
        self.decay_maker_friction(now);
        let ofi = *self.ofi_rx.borrow();
        let inv_snapshot = self.current_inventory_snapshot();
        let working_inv = inv_snapshot.working;
        let settled_inv = inv_snapshot.settled;
        let decision_inv = working_inv;
        self.observe_pair_arb_inventory_transition(&inv_snapshot, now);
        let glft_snapshot = if self.cfg.strategy == StrategyKind::GlftMm {
            Some(*self.glft_rx.borrow())
        } else {
            None
        };
        if self.cfg.strategy == StrategyKind::GlftMm
            && glft_snapshot.is_some_and(|snapshot| self.glft_is_tradeable_snapshot(snapshot))
        {
            self.glft_ready_seen = true;
        }
        let ref_blocked = matches!(
            glft_snapshot,
            Some(snapshot) if matches!(
                snapshot.quote_regime,
                crate::polymarket::glft::QuoteRegime::Blocked
            )
        );
        let track_ref_blocked = if self.cfg.strategy == StrategyKind::GlftMm {
            self.glft_ready_seen
        } else {
            true
        };
        if let Some(snapshot) = glft_snapshot {
            if self.cfg.strategy == StrategyKind::GlftMm {
                self.update_glft_source_recovery_state(snapshot, now);
                if self.glft_recovery_force_clear_pending {
                    self.glft_recovery_force_clear_pending = false;
                    for slot in OrderSlot::ALL {
                        if self.slot_target_active(slot) {
                            self.clear_slot_target(slot, CancelReason::StaleData).await;
                        }
                    }
                }
            }
            if track_ref_blocked && ref_blocked && self.reference_blocked_since.is_none() {
                let source_blocked = snapshot.readiness_blockers.await_binance
                    || snapshot.readiness_blockers.await_poly_book;
                if source_blocked {
                    self.stats.blocked_due_source = self.stats.blocked_due_source.saturating_add(1);
                    if snapshot.readiness_blockers.await_binance {
                        self.stats.blocked_due_binance =
                            self.stats.blocked_due_binance.saturating_add(1);
                    }
                    if snapshot.readiness_blockers.await_poly_book {
                        self.stats.blocked_due_poly = self.stats.blocked_due_poly.saturating_add(1);
                    }
                } else {
                    self.stats.blocked_due_divergence =
                        self.stats.blocked_due_divergence.saturating_add(1);
                }
            }
        }
        self.update_reference_blocked_time(track_ref_blocked && ref_blocked, now);
        self.maybe_log_inventory_metrics(&working_inv, &ofi);

        // ── Environmental Health Check ──
        let ttl = Duration::from_millis(self.cfg.stale_ttl_ms);

        let yes_stale_raw = now.duration_since(self.last_valid_ts_yes) > ttl;
        let no_stale_raw = now.duration_since(self.last_valid_ts_no) > ttl;
        let yes_stale_actionable =
            Self::stale_side_actionable(now, yes_stale_raw, &mut self.yes_stale_since);
        let no_stale_actionable =
            Self::stale_side_actionable(now, no_stale_raw, &mut self.no_stale_since);
        let is_hot_yes = ofi.yes.is_hot;
        let is_hot_no = ofi.no.is_hot;
        let is_toxic_yes = ofi.yes.is_toxic;
        let is_toxic_no = ofi.no.is_toxic;

        if !self.was_hot_yes && is_hot_yes {
            self.stats.ofi_heat_events = self.stats.ofi_heat_events.saturating_add(1);
        }
        if !self.was_hot_no && is_hot_no {
            self.stats.ofi_heat_events = self.stats.ofi_heat_events.saturating_add(1);
        }
        if !self.was_toxic_yes && is_toxic_yes {
            self.stats.ofi_toxic_events = self.stats.ofi_toxic_events.saturating_add(1);
        }
        if !self.was_toxic_no && is_toxic_no {
            self.stats.ofi_toxic_events = self.stats.ofi_toxic_events.saturating_add(1);
        }

        // Toxicity recovery hold-down: avoid immediate re-entry around threshold boundary.
        let hold = Duration::from_millis(self.cfg.toxic_recovery_hold_ms);
        if self.was_toxic_yes && !is_toxic_yes {
            self.yes_toxic_hold_until = now + hold;
        }
        if self.was_toxic_no && !is_toxic_no {
            self.no_toxic_hold_until = now + hold;
        }
        self.was_hot_yes = is_hot_yes;
        self.was_hot_no = is_hot_no;
        self.was_toxic_yes = is_toxic_yes;
        self.was_toxic_no = is_toxic_no;

        let yes_toxic_hold = now < self.yes_toxic_hold_until;
        let no_toxic_hold = now < self.no_toxic_hold_until;
        let yes_toxic_blocked = is_toxic_yes || yes_toxic_hold;
        let no_toxic_blocked = is_toxic_no || no_toxic_hold;
        if yes_toxic_blocked || no_toxic_blocked {
            self.stats.ofi_blocked_ticks = self.stats.ofi_blocked_ticks.saturating_add(1);
        }

        // Priority 1: 30s Staleness Guard (Critical Shutdown)
        if self.is_book_stale() {
            if self.yes_target.is_some() {
                warn!("⚠️ Book expired (>30s) — clearing YES");
                self.clear_target(Side::Yes, CancelReason::StaleData).await;
            }
            if self.no_target.is_some() {
                warn!("⚠️ Book expired (>30s) — clearing NO");
                self.clear_target(Side::No, CancelReason::StaleData).await;
            }
            return;
        }

        // Priority 2: Per-side Toxic/Stale guard (independent of book availability)
        if yes_toxic_blocked || yes_stale_actionable {
            for slot in OrderSlot::side_slots(Side::Yes) {
                if self.slot_target_active(slot) {
                    if yes_toxic_blocked {
                        if self.cfg.strategy.execution_mode()
                            == StrategyExecutionMode::SlotMarketMaking
                        {
                            if self.slot_blocked_by_ofi(slot, &ofi) {
                                self.clear_slot_target(slot, CancelReason::ToxicFlow).await;
                            }
                        } else if self.should_clear_on_toxic(Side::Yes) {
                            self.clear_target(Side::Yes, CancelReason::ToxicFlow).await;
                        }
                    } else {
                        self.clear_slot_target(slot, CancelReason::StaleData).await;
                    }
                }
            }
        }
        if no_toxic_blocked || no_stale_actionable {
            for slot in OrderSlot::side_slots(Side::No) {
                if self.slot_target_active(slot) {
                    if no_toxic_blocked {
                        if self.cfg.strategy.execution_mode()
                            == StrategyExecutionMode::SlotMarketMaking
                        {
                            if self.slot_blocked_by_ofi(slot, &ofi) {
                                self.clear_slot_target(slot, CancelReason::ToxicFlow).await;
                            }
                        } else if self.should_clear_on_toxic(Side::No) {
                            self.clear_target(Side::No, CancelReason::ToxicFlow).await;
                        }
                    } else {
                        self.clear_slot_target(slot, CancelReason::StaleData).await;
                    }
                }
            }
        }

        // Priority 3: Market data availability
        let ub = self.usable_book();
        if ub.yes_bid <= 0.0 || ub.no_bid <= 0.0 {
            self.stats.skipped_empty_book += 1;
            return;
        }

        // Priority 4: Strategy quote + unified flow-risk overlay + execution.
        let metrics = self.derive_inventory_metrics(&decision_inv);
        let input = StrategyTickInput {
            inv: &decision_inv,
            settled_inv: &settled_inv,
            working_inv: &working_inv,
            inventory: &inv_snapshot,
            book: &ub,
            metrics: &metrics,
            ofi: Some(&ofi),
            glft: glft_snapshot.as_ref(),
        };
        let mut quotes = self.cfg.strategy.compute_quotes(self, input);
        self.record_strategy_quote_diagnostics(&quotes);
        self.apply_flow_risk(
            &working_inv,
            &mut quotes,
            yes_stale_raw,
            no_stale_raw,
            yes_toxic_blocked,
            no_toxic_blocked,
        );
        self.execute_quotes(
            &working_inv,
            &ub,
            quotes,
            yes_stale_raw,
            no_stale_raw,
            yes_toxic_blocked,
            no_toxic_blocked,
        )
        .await;
    }

    // Policy-2: Flow Risk Overlay (toxicity / staleness)
    fn apply_flow_risk(
        &self,
        inv: &InventoryState,
        quotes: &mut StrategyQuotes,
        yes_stale: bool,
        no_stale: bool,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
    ) {
        if self.cfg.strategy.execution_mode() == StrategyExecutionMode::SlotMarketMaking {
            return;
        }
        let yes_toxic_blocked = yes_toxic_blocked && self.execution_toxic_block_applies();
        let no_toxic_blocked = no_toxic_blocked && self.execution_toxic_block_applies();
        let yes_allowed = self.flow_risk_allows_intent(
            inv,
            quotes.buy_for(Side::Yes),
            yes_stale,
            yes_toxic_blocked,
        );
        let no_allowed =
            self.flow_risk_allows_intent(inv, quotes.buy_for(Side::No), no_stale, no_toxic_blocked);

        // NOTE: Cancel counters are recorded in clear_slot_target() so flow-risk
        // only prunes intents here and avoids double-counting.
        if !yes_allowed {
            if quotes.buy_for(Side::Yes).is_some() {
                debug!(
                    "🚫 YES {} -> skip bid",
                    if yes_stale {
                        "stale"
                    } else if yes_toxic_blocked {
                        "toxic"
                    } else {
                        "blocked"
                    }
                );
            }
            quotes.clear(OrderSlot::YES_BUY);
        }
        if !no_allowed {
            if quotes.buy_for(Side::No).is_some() {
                debug!(
                    "🚫 NO {} -> skip bid",
                    if no_stale {
                        "stale"
                    } else if no_toxic_blocked {
                        "toxic"
                    } else {
                        "blocked"
                    }
                );
            }
            quotes.clear(OrderSlot::NO_BUY);
        }
    }

    fn flow_risk_allows_intent(
        &self,
        inv: &InventoryState,
        intent: Option<StrategyIntent>,
        stale: bool,
        toxic_blocked: bool,
    ) -> bool {
        let Some(intent) = intent else {
            return true;
        };
        if stale {
            return false;
        }
        if !toxic_blocked {
            return true;
        }
        if self.cfg.strategy.execution_mode() != StrategyExecutionMode::UnifiedBuys {
            return false;
        }
        self.projected_abs_net_diff(inv.net_diff, intent) <= inv.net_diff.abs() + 1e-6
    }

    // Execution/endgame/pricing methods are split into:
    // - coordinator_execution.rs
    // - coordinator_endgame.rs
    // - coordinator_order_io.rs
    // - coordinator_pricing.rs

    fn handle_execution_feedback(&mut self, feedback: ExecutionFeedback) {
        match feedback {
            ExecutionFeedback::PostOnlyCrossed {
                slot,
                ts,
                rejected_action_price,
            } => {
                if self.cfg.strategy == StrategyKind::PairArb && slot.direction == TradeDirection::Buy {
                    let idx = slot.index();
                    self.slot_pair_arb_cross_reject_extra_ticks[idx] = self
                        .slot_pair_arb_cross_reject_extra_ticks[idx]
                        .saturating_add(1);
                    self.slot_pair_arb_last_cross_rejected_action_price[idx] =
                        Some(rejected_action_price);
                    debug!(
                        "🧭 pair_arb cross reject sticky margin | slot={} extra_ticks={} rejected_action_price={:.4}",
                        slot.as_str(),
                        self.slot_pair_arb_cross_reject_extra_ticks[idx],
                        rejected_action_price,
                    );
                } else {
                    let side_extra_safety_ticks = {
                        let friction = self.maker_friction_mut(slot.side);
                        friction.extra_safety_ticks =
                            friction.extra_safety_ticks.saturating_add(1).min(3);
                        friction.last_cross_reject_ts = Some(ts);
                        friction.extra_safety_ticks
                    };
                    debug!(
                        "🪵 Maker friction {:?}: crossed-book reject -> extra_safety_ticks={}",
                        slot.side, side_extra_safety_ticks
                    );
                }
            }
            ExecutionFeedback::OrderAccepted { slot, ts: _ } => {
                if self.cfg.strategy == StrategyKind::PairArb
                    && slot.direction == TradeDirection::Buy
                {
                    let idx = slot.index();
                    if self.slot_pair_arb_cross_reject_extra_ticks[idx] > 0 {
                        debug!(
                            "🧭 pair_arb cross reject sticky margin cleared | slot={} extra_ticks={}",
                            slot.as_str(),
                            self.slot_pair_arb_cross_reject_extra_ticks[idx],
                        );
                    }
                    self.slot_pair_arb_cross_reject_extra_ticks[idx] = 0;
                    self.slot_pair_arb_last_cross_rejected_action_price[idx] = None;
                }
            }
            ExecutionFeedback::SlotBlocked {
                slot,
                tracked_orders,
                blocked_for_ms,
                ts,
            } => {
                let idx = slot.index();
                self.pair_arb_slot_blocked_for_ms[idx] = blocked_for_ms;
                self.pair_arb_slot_blocked_at[idx] = Some(ts);
                debug!(
                    "🧭 slot blocked feedback | slot={} tracked_orders={} blocked_for_ms={}",
                    slot.as_str(),
                    tracked_orders,
                    blocked_for_ms
                );
            }
        }
    }

    pub(crate) fn maker_friction(&self, side: Side) -> MakerFriction {
        match side {
            Side::Yes => self.yes_maker_friction,
            Side::No => self.no_maker_friction,
        }
    }

    pub(super) fn recent_cross_reject(&self, side: Side, within: Duration) -> bool {
        self.maker_friction(side)
            .last_cross_reject_ts
            .map(|ts| ts.elapsed() <= within)
            .unwrap_or(false)
    }

    pub(super) fn should_publish_recovery_for_slot(
        &self,
        slot: OrderSlot,
        within: Duration,
        now: Instant,
    ) -> bool {
        const GLFT_RECOVERY_REPUBLISH_COOLDOWN_MS: u64 = 1_200;

        let Some(cross_ts) = self.maker_friction(slot.side).last_cross_reject_ts else {
            return false;
        };
        if now.saturating_duration_since(cross_ts) > within {
            return false;
        }
        let idx = slot.index();
        if self.slot_last_recovery_cross_seen_at[idx] == Some(cross_ts) {
            return false;
        }
        self.slot_last_recovery_publish_at[idx]
            .map(|last| {
                now.saturating_duration_since(last)
                    >= Duration::from_millis(GLFT_RECOVERY_REPUBLISH_COOLDOWN_MS)
            })
            .unwrap_or(true)
    }

    pub(super) fn note_recovery_publish_for_slot(&mut self, slot: OrderSlot, now: Instant) {
        let idx = slot.index();
        self.slot_last_recovery_publish_at[idx] = Some(now);
        self.slot_last_recovery_cross_seen_at[idx] =
            self.maker_friction(slot.side).last_cross_reject_ts;
    }

    pub(super) fn clear_slot_recovery_publish_state(&mut self, slot: OrderSlot) {
        let idx = slot.index();
        self.slot_last_recovery_publish_at[idx] = None;
        self.slot_last_recovery_cross_seen_at[idx] = None;
    }

    fn maker_friction_mut(&mut self, side: Side) -> &mut MakerFriction {
        match side {
            Side::Yes => &mut self.yes_maker_friction,
            Side::No => &mut self.no_maker_friction,
        }
    }

    fn decay_maker_friction(&mut self, now: Instant) {
        for side in [Side::Yes, Side::No] {
            let friction = self.maker_friction_mut(side);
            if friction.extra_safety_ticks == 0 {
                friction.last_cross_reject_ts = None;
                continue;
            }
            let should_decay = friction
                .last_cross_reject_ts
                .map(|ts| now.duration_since(ts) >= Duration::from_secs(3))
                .unwrap_or(true);
            if should_decay {
                friction.extra_safety_ticks -= 1;
                if friction.extra_safety_ticks == 0 {
                    friction.last_cross_reject_ts = None;
                } else {
                    // Decay at most one tick per 3s window.
                    friction.last_cross_reject_ts = Some(now);
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
