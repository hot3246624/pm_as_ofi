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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use super::glft::GlftSignalSnapshot;
use super::messages::*;
use super::recorder::{RecorderHandle, RecorderSessionMeta};
use super::strategy::{
    completion_first::{CompletionFirstGateDefaults, CompletionFirstPhase},
    PgtDPlusMinOrderNoSeedReason, PgtHighPressureNoSeedReason, PgtXuanM0001NoSeedReason,
    StrategyExecutionMode, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput,
    PGT_DPLUS_MINORDER_NO_SEED_REASON_COUNT, PGT_HIGH_PRESSURE_NO_SEED_REASON_COUNT,
    PGT_XUAN_M0001_NO_SEED_REASON_COUNT,
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
// PGT seed quotes are intentionally low-cadence. If a seed is filled or has to
// be cleared because the same-side book became unsafe, wait for the book to
// stabilize before trying the same side again.
const PGT_SAME_SIDE_RELEASE_QUARANTINE_MS: u64 = 4_000;
const PGT_FLAT_SEED_LATCH_MS: u64 = 20_000;
const PGT_FLAT_SEED_LATCH_MAX_MS: u64 = 30_000;
const PUBLIC_BUY_PRESSURE_RETENTION_SECS: u64 = 15;
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
pub(crate) const PAIR_ARB_NET_EPS: f64 = 1e-6;
/// Oracle-lag taker threshold: fire FAK only when winner ask <= 0.99.
pub(crate) const ORACLE_LAG_NO_TAKER_ABOVE_PRICE: f64 = 0.990;
/// Maker fallback hard ceiling in post-close winner-side logic.
pub(crate) const ORACLE_LAG_MAKER_MAX_PRICE: f64 = 0.991;
/// Micro-tick boundary: when winner side has only bids and top bid ≥ this, step by 0.001; otherwise 0.01.
pub(crate) const ORACLE_LAG_MICRO_TICK_BID_BOUNDARY: f64 = 0.94;
/// Follow-up upward maker reprice minimum interval in post-close window.
pub(crate) const ORACLE_LAG_MAKER_FOLLOWUP_MIN_INTERVAL_MS: u64 = 200;
/// Follow-up in-flight lock (ms): after local target update/cancel activity, suppress
/// new follow-up attempts briefly to avoid overlapping reprice waves.
pub(crate) const ORACLE_LAG_MAKER_FOLLOWUP_INFLIGHT_LOCK_MS: u64 = 900;
/// Same-candidate cooldown (ms): if follow-up keeps producing the same target price,
/// hold it for a short window instead of repeatedly resubmitting identical intents.
pub(crate) const ORACLE_LAG_MAKER_FOLLOWUP_SAME_CANDIDATE_COOLDOWN_MS: u64 = 1_500;
/// Hint-side fallback freshness guard (ms). If hint evidence is older than this,
/// do not use it for execution pricing when live winner-side book is missing.
pub(crate) const ORACLE_LAG_HINT_FALLBACK_MAX_AGE_MS: u64 = 1200;
#[allow(dead_code)]
const PAIR_ARB_OPPOSITE_SLOT_BLOCK_MS: u64 = 30_000;
#[allow(dead_code)]
const PAIR_ARB_OPPOSITE_SLOT_BLOCK_TTL_MS: u64 = 10_000;

#[path = "coordinator_completion_first.rs"]
mod coordinator_completion_first;
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
pub struct PairArbStrategyConfig {
    /// Tier cap mode:
    /// - Discrete: use step thresholds (legacy behavior)
    /// - Continuous: smooth multiplier by current abs net_diff
    pub tier_mode: PairArbTierMode,
    /// PairArb dominant-side avg-cost cap multiplier when |net_diff| is in tier-1.
    pub tier_1_mult: f64,
    /// PairArb dominant-side avg-cost cap multiplier when |net_diff| is in tier-2.
    pub tier_2_mult: f64,
    /// PairArb safety margin kept below pair_target when deriving VWAP ceiling.
    pub pair_cost_safety_margin: f64,
    /// PairArb risk-open cutoff window (seconds to market end).
    /// Remaining <= this threshold blocks new risk-increasing buys.
    pub risk_open_cutoff_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairArbTierMode {
    Disabled,
    Discrete,
    Continuous,
}

impl PairArbTierMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Discrete => "discrete",
            Self::Continuous => "continuous",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "false" => Some(Self::Disabled),
            "discrete" | "step" | "bucket" => Some(Self::Discrete),
            "continuous" | "smooth" | "non_discrete" | "nondiscrete" => Some(Self::Continuous),
            _ => None,
        }
    }

    pub fn tier_cap_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub fn tiered_inventory_curve_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub fn multi_bucket_state_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionFirstMode {
    Shadow,
    Enforce,
}

#[derive(Debug, Clone)]
pub struct CompletionFirstStrategyConfig {
    pub market_enabled: bool,
    pub mode: CompletionFirstMode,
    pub gate_defaults_path: Option<String>,
    pub gate_defaults: CompletionFirstGateDefaults,
}

#[derive(Debug, Clone)]
pub struct OracleLagSnipingStrategyConfig {
    /// Post-close strategy live window (seconds after market end).
    pub window_secs: u64,
    /// Runtime market guard for post-close strategy.
    /// This is set by main loop per-round from slug + symbol-universe gating.
    pub market_enabled: bool,
    /// Enable cross-market arbiter (inproc supervisor only). Default: false.
    pub cross_market_arbiter_enabled: bool,
    /// Milliseconds to collect observations before arbitrating. Default: 200.
    pub arbiter_collection_window_ms: u64,
    /// Book age threshold (ms) above which an observation is considered stale. Default: 250.
    pub arbiter_book_max_age_ms: u64,
    /// Optional per-order notional cap (USDC) for oracle-lag orders.
    /// 0.0 disables cap and uses `PM_BID_SIZE` as before.
    pub max_order_notional_usdc: f64,
    /// Lab mode: keep data/decision pipeline active but block all trading intents.
    pub lab_only: bool,
}

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
    /// PairArb strategy-specific runtime config.
    pub pair_arb: PairArbStrategyConfig,
    /// Completion-first strategy runtime config.
    pub completion_first: CompletionFirstStrategyConfig,
    /// Post-close HYPE strategy-specific runtime config.
    pub oracle_lag_sniping: OracleLagSnipingStrategyConfig,
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
            pair_arb: PairArbStrategyConfig {
                tier_mode: PairArbTierMode::Discrete,
                tier_1_mult: 0.80,
                tier_2_mult: 0.60,
                pair_cost_safety_margin: 0.02,
                risk_open_cutoff_secs: 180,
            },
            completion_first: CompletionFirstStrategyConfig {
                market_enabled: false,
                mode: CompletionFirstMode::Shadow,
                gate_defaults_path: None,
                gate_defaults: CompletionFirstGateDefaults::default(),
            },
            oracle_lag_sniping: OracleLagSnipingStrategyConfig {
                // ~Polymarket liquidity lifecycle post-close (all resting orders cleared by then).
                window_secs: 105,
                market_enabled: false,
                cross_market_arbiter_enabled: false,
                arbiter_collection_window_ms: 200,
                arbiter_book_max_age_ms: 250,
                max_order_notional_usdc: 0.0,
                lab_only: false,
            },
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
        let mut cfg = Self::default();
        cfg.strategy = StrategyKind::from_env_or_default(cfg.strategy);
        cfg.apply_common_env();
        cfg.apply_glft_env();
        cfg.apply_pair_arb_env();
        cfg.apply_completion_first_env();
        cfg.apply_oracle_lag_env();
        cfg.apply_hedge_env();
        cfg.apply_runtime_env();
        cfg.apply_endgame_env();
        cfg.finalize_invariants();
        cfg
    }

    fn apply_common_env(&mut self) {
        if let Ok(v) = std::env::var("PM_PAIR_TARGET") {
            if let Ok(f) = v.parse() {
                self.pair_target = f;
            }
        }
        if let Ok(v) = std::env::var("PM_OPEN_PAIR_BAND") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 && f <= 1.0 {
                    self.open_pair_band = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_OPEN_PAIR_BAND={} (must satisfy 0 < p <= 1), using {}",
                        f, self.open_pair_band
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_NET_DIFF") {
            if let Ok(f) = v.parse() {
                self.max_net_diff = f;
            }
        }
        if let Ok(v) = std::env::var("PM_BID_SIZE") {
            if let Ok(f) = v.parse() {
                self.bid_size = f;
            }
        }
        if let Ok(v) = std::env::var("PM_DIP_BUY_MAX_ENTRY_PRICE") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    self.dip_buy_max_entry_price = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_DIP_BUY_MAX_ENTRY_PRICE={} (must satisfy 0 < p < 1), using {}",
                        f, self.dip_buy_max_entry_price
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_TICK_SIZE") {
            if let Ok(f) = v.parse() {
                if (0.0..1.0).contains(&f) {
                    self.tick_size = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_TICK_SIZE={} (must satisfy 0 < tick < 1), using {}",
                        f, self.tick_size
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_POST_ONLY_SAFETY_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.post_only_safety_ticks = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_POST_ONLY_TIGHT_SPREAD_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.post_only_tight_spread_ticks = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_POST_ONLY_EXTRA_TIGHT_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.post_only_extra_tight_ticks = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_REPRICE_THRESHOLD") {
            if let Ok(f) = v.parse() {
                self.reprice_threshold = f;
            }
        }
        if let Ok(v) = std::env::var("PM_DEBOUNCE_MS") {
            if let Ok(f) = v.parse() {
                self.debounce_ms = f;
            }
        }
        if let Ok(v) = std::env::var("PM_AS_SKEW_FACTOR") {
            if let Ok(f) = v.parse() {
                self.as_skew_factor = f;
            }
        }
        if let Ok(v) = std::env::var("PM_AS_TIME_DECAY_K") {
            if let Ok(f) = v.parse::<f64>() {
                self.as_time_decay_k = f.max(0.0);
            }
        }
    }

    fn apply_glft_env(&mut self) {
        if let Ok(v) = std::env::var("PM_GLFT_GAMMA") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.glft_gamma = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_GLFT_XI") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.glft_xi = f;
                }
            }
        } else {
            self.glft_xi = self.glft_gamma;
        }
        if self.glft_xi <= 0.0 {
            self.glft_xi = self.glft_gamma.max(1e-6);
        }
        if let Ok(v) = std::env::var("PM_GLFT_OFI_ALPHA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.glft_ofi_alpha = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_GLFT_OFI_SPREAD_BETA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.glft_ofi_spread_beta = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MIN_HALF_SPREAD_TICKS") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.glft_min_half_spread_ticks = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_MIN_HALF_SPREAD_TICKS={} (must be > 0), using {}",
                        f, self.glft_min_half_spread_ticks
                    );
                }
            }
        }
    }

    fn apply_pair_arb_env(&mut self) {
        if let Ok(v) = std::env::var("PM_PAIR_ARB_TIER_1_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..=1.0).contains(&f) {
                    self.pair_arb.tier_1_mult = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_TIER_1_MULT={} (must satisfy 0 <= x <= 1), using {}",
                        f, self.pair_arb.tier_1_mult
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_TIER_2_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..=1.0).contains(&f) {
                    self.pair_arb.tier_2_mult = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_TIER_2_MULT={} (must satisfy 0 <= x <= 1), using {}",
                        f, self.pair_arb.tier_2_mult
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_TIER_MODE") {
            if let Some(mode) = PairArbTierMode::parse(&v) {
                self.pair_arb.tier_mode = mode;
            } else {
                warn!(
                    "⚠️ Ignoring invalid PM_PAIR_ARB_TIER_MODE={} (supported: disabled|discrete|continuous), using {}",
                    v,
                    self.pair_arb.tier_mode.as_str()
                );
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    self.pair_arb.pair_cost_safety_margin = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN={} (must satisfy 0 <= x < 1), using {}",
                        f, self.pair_arb.pair_cost_safety_margin
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_PAIR_ARB_RISK_OPEN_CUTOFF_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.pair_arb.risk_open_cutoff_secs = secs;
            }
        }
    }

    fn apply_completion_first_env(&mut self) {
        if let Ok(v) = std::env::var("PM_COMPLETION_FIRST_MODE") {
            let v = v.trim().to_ascii_lowercase();
            self.completion_first.mode = if v == "enforce" {
                CompletionFirstMode::Enforce
            } else {
                CompletionFirstMode::Shadow
            };
        }

        let path = std::env::var("PM_COMPLETION_FIRST_GATE_DEFAULTS")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                let default_path =
                    std::path::Path::new("configs/xuan_completion_gate_defaults.json");
                default_path
                    .exists()
                    .then(|| default_path.display().to_string())
            });
        if let Some(path) = path {
            match CompletionFirstGateDefaults::from_path(std::path::Path::new(&path)) {
                Ok(defaults) => {
                    self.completion_first.gate_defaults_path = Some(path);
                    self.completion_first.gate_defaults = defaults;
                }
                Err(err) => {
                    warn!(
                        "⚠️ Failed to load completion_first gate defaults from {}: {}",
                        path, err
                    );
                }
            }
        }
    }

    fn apply_oracle_lag_env(&mut self) {
        if let Ok(v) = std::env::var("PM_POST_CLOSE_WINDOW_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.oracle_lag_sniping.window_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED") {
            let lv = v.to_ascii_lowercase();
            self.oracle_lag_sniping.cross_market_arbiter_enabled = v == "1" || lv == "true";
        }
        if let Ok(v) = std::env::var("PM_ORACLE_LAG_ARBITER_COLLECTION_WINDOW_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.oracle_lag_sniping.arbiter_collection_window_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_ORACLE_LAG_ARBITER_BOOK_MAX_AGE_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.oracle_lag_sniping.arbiter_book_max_age_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_ORACLE_LAG_MAX_ORDER_NOTIONAL_USDC") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.oracle_lag_sniping.max_order_notional_usdc = f;
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_ORACLE_LAG_MAX_ORDER_NOTIONAL_USDC={} (must satisfy x >= 0), using {}",
                        f, self.oracle_lag_sniping.max_order_notional_usdc
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ORACLE_LAG_LAB_ONLY") {
            let lv = v.to_ascii_lowercase();
            self.oracle_lag_sniping.lab_only = v == "1" || lv == "true";
        }
    }

    fn apply_hedge_env(&mut self) {
        if let Ok(v) = std::env::var("PM_HEDGE_DEBOUNCE_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.hedge_debounce_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_PORTFOLIO_COST") {
            if let Ok(f) = v.parse() {
                self.max_portfolio_cost = f;
            }
        }
        if let Ok(v) = std::env::var("PM_MIN_ORDER_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.min_order_size = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_MIN_HEDGE_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.min_hedge_size = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_ROUND_UP") {
            self.hedge_round_up = v == "1" || v.to_lowercase() == "true";
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_NOTIONAL") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.hedge_min_marketable_notional = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.hedge_min_marketable_max_extra = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT") {
            if let Ok(f) = v.parse::<f64>() {
                if f >= 0.0 {
                    self.hedge_min_marketable_max_extra_pct = f;
                }
            }
        }
    }

    fn apply_runtime_env(&mut self) {
        if let Ok(v) = std::env::var("PM_DRY_RUN") {
            self.dry_run = v != "0" && v.to_lowercase() != "false";
        }
        if let Ok(v) = std::env::var("PM_STALE_TTL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.stale_ttl_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_COORD_WATCHDOG_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.watchdog_tick_ms = ms.max(50);
            }
        }
        if let Ok(v) = std::env::var("PM_STRATEGY_METRICS_LOG_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.strategy_metrics_log_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_TOXIC_RECOVERY_HOLD_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.toxic_recovery_hold_ms = ms;
            }
        }
    }

    fn apply_endgame_env(&mut self) {
        if let Ok(v) = std::env::var("PM_MAX_LOSS_PCT") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    self.max_loss_pct = f;
                    warn!(
                        "⚠️ PM_MAX_LOSS_PCT is deprecated and ignored (value={:.3})",
                        self.max_loss_pct
                    );
                } else {
                    warn!(
                        "⚠️ Ignoring invalid PM_MAX_LOSS_PCT={} (must satisfy 0 <= pct < 1), using {}",
                        f, self.max_loss_pct
                    );
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_EDGE_KEEP_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.endgame_edge_keep_mult = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_EDGE_EXIT_MULT") {
            if let Ok(f) = v.parse::<f64>() {
                if f > 0.0 {
                    self.endgame_edge_exit_mult = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_SOFT_CLOSE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.endgame_soft_close_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_HARD_CLOSE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.endgame_hard_close_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_FREEZE_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.endgame_freeze_secs = secs;
            }
        }
        if let Ok(v) = std::env::var("PM_ENDGAME_MAKER_REPAIR_MIN_SECS") {
            if let Ok(secs) = v.parse::<u64>() {
                self.endgame_maker_repair_min_secs = secs;
            }
        }
    }

    fn finalize_invariants(&mut self) {
        if self.endgame_edge_exit_mult > self.endgame_edge_keep_mult {
            warn!(
                "⚠️ Clamping PM_ENDGAME_EDGE_EXIT_MULT from {:.4} to {:.4} (must be <= keep_mult)",
                self.endgame_edge_exit_mult, self.endgame_edge_keep_mult
            );
            self.endgame_edge_exit_mult = self.endgame_edge_keep_mult;
        }
        // Keep windows ordered: soft >= hard >= freeze.
        if self.endgame_hard_close_secs > self.endgame_soft_close_secs {
            warn!(
                "⚠️ Clamping PM_ENDGAME_HARD_CLOSE_SECS from {} to {} (must be <= soft-close)",
                self.endgame_hard_close_secs, self.endgame_soft_close_secs
            );
            self.endgame_hard_close_secs = self.endgame_soft_close_secs;
        }
        if self.endgame_freeze_secs > self.endgame_hard_close_secs {
            warn!(
                "⚠️ Clamping PM_ENDGAME_FREEZE_SECS from {} to {} (must be <= hard-close)",
                self.endgame_freeze_secs, self.endgame_hard_close_secs
            );
            self.endgame_freeze_secs = self.endgame_hard_close_secs;
        }
        if self.endgame_maker_repair_min_secs > self.endgame_hard_close_secs {
            warn!(
                "⚠️ PM_ENDGAME_MAKER_REPAIR_MIN_SECS={} exceeds hard-close window {}s; maker repair may be skipped in HardClose",
                self.endgame_maker_repair_min_secs, self.endgame_hard_close_secs
            );
        }
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct PublicTradeSnapshot {
    pub(crate) market_side: Side,
    pub(crate) taker_side: TakerSide,
    pub(crate) price: f64,
    pub(crate) size: f64,
    pub(crate) ts: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicTradeSideAlignment {
    High,
    Low,
}

#[derive(Debug, Clone, Copy)]
struct PublicBuyPressureEvent {
    alignment: PublicTradeSideAlignment,
    size: f64,
    ts: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PublicBuyPressureSnapshot {
    pub(crate) latest_trade: PublicTradeSnapshot,
    pub(crate) high_qty: f64,
    pub(crate) low_qty: f64,
    pub(crate) pressure_ratio: f64,
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
    pgt_post_flow_quotes: u64,
    pgt_dispatch_intents: u64,
    pgt_dispatch_blocked: u64,
    pgt_dispatch_place: u64,
    pgt_dispatch_taker_open: u64,
    pgt_dispatch_taker_close: u64,
    pgt_dispatch_retain: u64,
    pgt_dispatch_clear: u64,
    pgt_stale_target_dropped: u64,
    pair_arb_opposite_slot_blocked: u64,
    pair_arb_stale_target_dropped: u64,
    pair_arb_state_forced_republish: u64,
    pgt_seed_quotes: u64,
    pgt_completion_quotes: u64,
    pgt_skip_harvest: u64,
    pgt_skip_tail_completion_only: u64,
    pgt_skip_after_rescue_close: u64,
    pgt_skip_after_closed_pair: u64,
    pgt_skip_residual_guard: u64,
    pgt_skip_capital_guard: u64,
    pgt_skip_invalid_book: u64,
    pgt_skip_no_seed: u64,
    pgt_skip_geometry_guard: u64,
    pgt_seed_reject_no_visible_breakeven_path: u64,
    pgt_single_seed_bias: u64,
    pgt_single_seed_first_side: Option<Side>,
    pgt_single_seed_last_side: Option<Side>,
    pgt_single_seed_flip_count: u64,
    pgt_dual_seed_quotes: u64,
    pgt_single_seed_released_to_dual: u64,
    pgt_single_seed_released_to_dual_recorded: bool,
    pgt_entry_pressure_sides: u64,
    pgt_entry_pressure_extra_ticks: u64,
    pgt_taker_shadow_would_open: u64,
    pgt_taker_shadow_would_close: u64,
    pgt_xuan_m0001_no_seed: [u64; PGT_XUAN_M0001_NO_SEED_REASON_COUNT],
    pgt_dplus_minorder_no_seed: [u64; PGT_DPLUS_MINORDER_NO_SEED_REASON_COUNT],
    pgt_high_pressure_no_seed: [u64; PGT_HIGH_PRESSURE_NO_SEED_REASON_COUNT],
    market_trade_ticks: u64,
    market_sell_trade_ticks: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PairArbGateLogSnapshot {
    ofi_softened_quotes: u64,
    ofi_suppressed_quotes: u64,
    keep_candidates: u64,
    skip_inventory_gate: u64,
    skip_simulate_buy_none: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PgtGateLogSnapshot {
    seed_quotes: u64,
    completion_quotes: u64,
    skip_harvest: u64,
    skip_tail_completion_only: u64,
    skip_after_rescue_close: u64,
    skip_after_closed_pair: u64,
    skip_residual_guard: u64,
    skip_capital_guard: u64,
    skip_invalid_book: u64,
    skip_no_seed: u64,
    taker_shadow_would_open: u64,
    taker_shadow_would_close: u64,
    skip_geometry_guard: u64,
    seed_reject_no_visible_breakeven_path: u64,
    single_seed_bias: u64,
    single_seed_first_side: Option<Side>,
    single_seed_last_side: Option<Side>,
    single_seed_flip_count: u64,
    dual_seed_quotes: u64,
    single_seed_released_to_dual: u64,
    entry_pressure_sides: u64,
    entry_pressure_extra_ticks: u64,
    post_flow_quotes: u64,
    dispatch_intents: u64,
    dispatch_blocked: u64,
    dispatch_place: u64,
    dispatch_taker_open: u64,
    dispatch_taker_close: u64,
    dispatch_retain: u64,
    dispatch_clear: u64,
    stale_target_dropped: u64,
    xuan_m0001_no_seed: [u64; PGT_XUAN_M0001_NO_SEED_REASON_COUNT],
    dplus_minorder_no_seed: [u64; PGT_DPLUS_MINORDER_NO_SEED_REASON_COUNT],
    high_pressure_no_seed: [u64; PGT_HIGH_PRESSURE_NO_SEED_REASON_COUNT],
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
    pgt_taker_close_limit_yes: Option<f64>,
    pgt_taker_close_limit_no: Option<f64>,
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
    Place {
        intent: StrategyIntent,
    },
    ShadowTaker {
        intent: StrategyIntent,
        limit_price: f64,
    },
    ShadowTakerClose {
        intent: StrategyIntent,
        limit_price: f64,
    },
    Clear {
        reason: CancelReason,
    },
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

    fn pgt_taker_close_limit_for(&self, side: Side) -> Option<f64> {
        match side {
            Side::Yes => self.pgt_taker_close_limit_yes,
            Side::No => self.pgt_taker_close_limit_no,
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
    last_public_trade: Option<PublicTradeSnapshot>,
    last_public_trade_by_side: [Option<PublicTradeSnapshot>; 2],
    last_public_buy_trade_by_side: [Option<PublicTradeSnapshot>; 2],
    last_high_side_public_buy_trade: Option<PublicTradeSnapshot>,
    public_buy_pressure_events: VecDeque<PublicBuyPressureEvent>,
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
    slot_pair_arb_intent_epochs: [Option<u64>; 4],
    slot_pair_arb_target_epochs: [Option<u64>; 4],
    slot_pgt_intent_epochs: [Option<u64>; 4],
    slot_pgt_target_epochs: [Option<u64>; 4],
    slot_pair_arb_fill_recheck_pending: [bool; 4],
    slot_pair_arb_cross_reject_extra_ticks: [u8; 4],
    slot_pair_arb_last_cross_rejected_action_price: [Option<f64>; 4],
    slot_pair_arb_cross_reject_reprice_pending: [bool; 4],
    slot_pair_arb_state_republish_latched: [bool; 4],
    pgt_same_side_release_quarantine_until: [Option<Instant>; 2],
    pgt_flat_seed_latched_side: Option<Side>,
    pgt_flat_seed_latched_since: Option<Instant>,
    pgt_flat_seed_latched_until: Option<Instant>,
    pgt_flat_seed_latch_exhausted: bool,
    pgt_tail_seed_force_clear_sent: bool,
    pgt_taker_close_rescue_fired: bool,
    pgt_post_close_reopen_attempted_fill_count: Option<u64>,
    pgt_shadow_taker_open_fired_epoch: Option<u64>,
    pgt_shadow_taker_close_fired_epoch: [Option<u64>; 2],
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
    pgt_gate_last_log_ts: Instant,
    pgt_gate_last_snapshot: PgtGateLogSnapshot,
    post_close_winner_side: Option<Side>,
    post_close_winner_source: Option<WinnerHintSource>,
    post_close_winner_open_is_exact: Option<bool>,
    post_close_winner_ref_price: f64,
    post_close_winner_observed_price: f64,
    post_close_winner_final_detect_unix_ms: Option<u64>,
    post_close_winner_emit_unix_ms: Option<u64>,
    post_close_winner_evidence_recv_ms: Option<u64>,
    post_close_winner_ts: Option<Instant>,
    post_close_hint_winner_bid: f64,
    post_close_hint_winner_ask_raw: f64,
    post_close_hint_book_source: &'static str,
    post_close_hint_distance_to_final_ms: u64,
    oracle_lag_first_submit_logged: bool,
    /// Timestamp of the last FAK dispatch for OracleLagSniping.
    /// Used for two purposes:
    ///   1. Suppress duplicate-hint firing (2nd source arriving within cooldown).
    ///   2. Gate re-entry: after FAK (IOC auto-cancels remainder), allow another FAK
    ///      once cooldown expires AND the price condition still holds.
    oracle_lag_fak_last_dispatch: Option<Instant>,
    /// Per-round counter of FAK dispatches (first-shot + re-entries).
    /// Naturally resets because each round spawns a fresh coordinator.
    oracle_lag_fak_shots_this_round: u8,
    /// Per-market per-hint in-flight lock to suppress duplicate WinnerHint-triggered FAK fires.
    /// key=slug, value=hint_id
    oracle_lag_fak_inflight_by_slug: HashMap<String, u64>,
    /// Round end timestamp of the last OracleLagSelection received.
    /// Guards against cross-round leakage (older selections are ignored).
    oracle_lag_selected_round_end_ts: Option<u64>,
    /// Whether this market was selected by the cross-market arbiter for the current round.
    /// Defaults to true so single-market / no-arbiter mode is unaffected.
    oracle_lag_is_selected: bool,
    /// Suppress strategy-level maker emits and wait for round-tail fallback action.
    oracle_lag_defer_to_round_tail: bool,
    /// Current round that oracle-lag tail actions belong to.
    /// Used for stale/new-round gating only (not one-shot dedup).
    oracle_lag_tail_round_done: Option<u64>,
    /// Last maker-tail state key to prevent same-state high-frequency clear/repost.
    oracle_lag_maker_state_key: Option<String>,
    /// Last timestamp we performed an upward follow-up reprice for oracle-lag maker.
    oracle_lag_maker_followup_last_ts: Option<Instant>,
    /// Last follow-up candidate price per slot for oracle-lag maker.
    oracle_lag_maker_followup_last_candidate_price: [Option<f64>; 4],
    /// Hard stop for oracle_lag_sniping current round after balance/allowance reject.
    oracle_lag_round_halted: bool,
    oracle_lag_round_halt_kind: Option<RejectKind>,
    /// Rate-limiter for post_close_book_tick snapshot logs (500ms).
    last_post_close_snapshot_ts: Option<Instant>,
    pgt_decision_epoch: u64,
    pair_arb_decision_epoch: u64,
    completion_first_decision_epoch: u64,
    completion_first_round_buy_fill_count: u64,
    completion_first_same_side_run_count: u32,
    completion_first_merge_requested_this_round: bool,
    completion_first_merge_retry_requested_this_round: bool,
    completion_first_redeem_requested_count: u8,
    completion_first_shadow_summary_emitted: bool,
    completion_first_last_phase: CompletionFirstPhase,
    pair_arb_last_risk_open_cutoff_active: bool,
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
    /// Dedicated one-shot winner-hint channel for oracle_lag_sniping.
    /// Kept separate from `md_rx` watch channel to avoid coalescing loss under high WS tick rate.
    winner_hint_rx: mpsc::Receiver<MarketDataMsg>,
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
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
    /// Optional shared winner-side cache for non-oracle strategies that need
    /// post-close winner awareness after the coordinator task has been moved.
    shared_post_close_winner_side: Option<Arc<Mutex<Option<Side>>>>,
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
    pub pair_arb_opposite_slot_blocked: u64,
    pub pair_arb_stale_target_dropped: u64,
    pub pair_arb_state_forced_republish: u64,
    pub pgt_seed_quotes: u64,
    pub pgt_completion_quotes: u64,
    pub pgt_skip_harvest: u64,
    pub pgt_skip_tail_completion_only: u64,
    pub pgt_skip_residual_guard: u64,
    pub pgt_skip_capital_guard: u64,
    pub pgt_skip_invalid_book: u64,
    pub pgt_skip_no_seed: u64,
    pub pgt_skip_geometry_guard: u64,
    pub pgt_seed_reject_no_visible_breakeven_path: u64,
    pub pgt_single_seed_bias: u64,
    pub pgt_single_seed_first_side: Option<Side>,
    pub pgt_single_seed_last_side: Option<Side>,
    pub pgt_single_seed_flip_count: u64,
    pub pgt_dual_seed_quotes: u64,
    pub pgt_single_seed_released_to_dual: u64,
    pub pgt_entry_pressure_sides: u64,
    pub pgt_entry_pressure_extra_ticks: u64,
    pub pgt_taker_shadow_would_open: u64,
    pub pgt_taker_shadow_would_close: u64,
    pub pgt_post_flow_quotes: u64,
    pub pgt_dispatch_intents: u64,
    pub pgt_dispatch_blocked: u64,
    pub pgt_dispatch_place: u64,
    pub pgt_dispatch_taker_open: u64,
    pub pgt_dispatch_taker_close: u64,
    pub pgt_dispatch_retain: u64,
    pub pgt_dispatch_clear: u64,
    pub pgt_stale_target_dropped: u64,
    pub pgt_xuan_m0001_no_seed: [u64; PGT_XUAN_M0001_NO_SEED_REASON_COUNT],
    pub pgt_dplus_minorder_no_seed: [u64; PGT_DPLUS_MINORDER_NO_SEED_REASON_COUNT],
    pub pgt_high_pressure_no_seed: [u64; PGT_HIGH_PRESSURE_NO_SEED_REASON_COUNT],
    pub market_trade_ticks: u64,
    pub market_sell_trade_ticks: u64,
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
    pub(crate) risk_open_cutoff_active: bool,
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
        let (_dead_winner_hint_tx, dead_winner_hint_rx) = mpsc::channel(1);
        let (_dead_glft_tx, dead_glft_rx) = watch::channel(GlftSignalSnapshot::default());
        Self::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            dead_winner_hint_rx,
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
        let (_dead_winner_hint_tx, dead_winner_hint_rx) = mpsc::channel(1);
        let (_dead_glft_tx, dead_glft_rx) = watch::channel(GlftSignalSnapshot::default());
        Self::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            dead_winner_hint_rx,
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
        winner_hint_rx: mpsc::Receiver<MarketDataMsg>,
        glft_rx: watch::Receiver<GlftSignalSnapshot>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
        feedback_rx: mpsc::Receiver<ExecutionFeedback>,
        slot_release_rx: mpsc::Receiver<SlotReleaseEvent>,
    ) -> Self {
        Self::with_aux_rx_and_shared_winner(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            winner_hint_rx,
            glft_rx,
            om_tx,
            kill_rx,
            feedback_rx,
            slot_release_rx,
            None,
        )
    }

    pub fn with_aux_rx_and_shared_winner(
        cfg: CoordinatorConfig,
        ofi_rx: watch::Receiver<OfiSnapshot>,
        inv_rx: watch::Receiver<InventorySnapshot>,
        md_rx: watch::Receiver<MarketDataMsg>,
        winner_hint_rx: mpsc::Receiver<MarketDataMsg>,
        glft_rx: watch::Receiver<GlftSignalSnapshot>,
        om_tx: mpsc::Sender<OrderManagerCmd>,
        kill_rx: mpsc::Receiver<KillSwitchSignal>,
        feedback_rx: mpsc::Receiver<ExecutionFeedback>,
        slot_release_rx: mpsc::Receiver<SlotReleaseEvent>,
        shared_post_close_winner_side: Option<Arc<Mutex<Option<Side>>>>,
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
            last_public_trade: None,
            last_public_trade_by_side: [None; 2],
            last_public_buy_trade_by_side: [None; 2],
            last_high_side_public_buy_trade: None,
            public_buy_pressure_events: VecDeque::new(),
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
            slot_pair_arb_intent_epochs: std::array::from_fn(|_| None),
            slot_pair_arb_target_epochs: std::array::from_fn(|_| None),
            slot_pgt_intent_epochs: std::array::from_fn(|_| None),
            slot_pgt_target_epochs: std::array::from_fn(|_| None),
            slot_pair_arb_fill_recheck_pending: [false; 4],
            slot_pair_arb_cross_reject_extra_ticks: [0; 4],
            slot_pair_arb_last_cross_rejected_action_price: std::array::from_fn(|_| None),
            slot_pair_arb_cross_reject_reprice_pending: [false; 4],
            slot_pair_arb_state_republish_latched: [false; 4],
            pgt_same_side_release_quarantine_until: [None; 2],
            pgt_flat_seed_latched_side: None,
            pgt_flat_seed_latched_since: None,
            pgt_flat_seed_latched_until: None,
            pgt_flat_seed_latch_exhausted: false,
            pgt_tail_seed_force_clear_sent: false,
            pgt_taker_close_rescue_fired: false,
            pgt_post_close_reopen_attempted_fill_count: None,
            pgt_shadow_taker_open_fired_epoch: None,
            pgt_shadow_taker_close_fired_epoch: [None, None],
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
            pgt_gate_last_log_ts: now,
            pgt_gate_last_snapshot: PgtGateLogSnapshot::default(),
            post_close_winner_side: None,
            post_close_winner_source: None,
            post_close_winner_open_is_exact: None,
            post_close_winner_ref_price: 0.0,
            post_close_winner_observed_price: 0.0,
            post_close_winner_final_detect_unix_ms: None,
            post_close_winner_emit_unix_ms: None,
            post_close_winner_evidence_recv_ms: None,
            post_close_winner_ts: None,
            post_close_hint_winner_bid: 0.0,
            post_close_hint_winner_ask_raw: 0.0,
            post_close_hint_book_source: "none",
            post_close_hint_distance_to_final_ms: u64::MAX,
            oracle_lag_first_submit_logged: false,
            oracle_lag_fak_last_dispatch: None,
            oracle_lag_fak_shots_this_round: 0,
            oracle_lag_fak_inflight_by_slug: HashMap::new(),
            oracle_lag_selected_round_end_ts: None,
            oracle_lag_is_selected: true,
            oracle_lag_defer_to_round_tail: false,
            oracle_lag_tail_round_done: None,
            oracle_lag_maker_state_key: None,
            oracle_lag_maker_followup_last_ts: None,
            oracle_lag_maker_followup_last_candidate_price: std::array::from_fn(|_| None),
            oracle_lag_round_halted: false,
            oracle_lag_round_halt_kind: None,
            last_post_close_snapshot_ts: None,
            pgt_decision_epoch: 0,
            pair_arb_decision_epoch: 0,
            completion_first_decision_epoch: 0,
            completion_first_round_buy_fill_count: 0,
            completion_first_same_side_run_count: 0,
            completion_first_merge_requested_this_round: false,
            completion_first_merge_retry_requested_this_round: false,
            completion_first_redeem_requested_count: 0,
            completion_first_shadow_summary_emitted: false,
            completion_first_last_phase: CompletionFirstPhase::FlatSeed,
            pair_arb_last_risk_open_cutoff_active: false,
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
            winner_hint_rx,
            glft_rx,
            om_tx,
            kill_rx,
            feedback_rx,
            slot_release_rx,
            obs_tx: None,
            recorder: None,
            recorder_meta: None,
            shared_post_close_winner_side,
        }
    }

    pub fn with_obs_tx(mut self, obs_tx: watch::Sender<CoordinatorObsSnapshot>) -> Self {
        self.obs_tx = Some(obs_tx);
        self
    }

    pub fn with_recorder(
        mut self,
        recorder: RecorderHandle,
        recorder_meta: RecorderSessionMeta,
    ) -> Self {
        self.recorder = Some(recorder);
        self.recorder_meta = Some(recorder_meta);
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

    pub fn post_close_winner_side(&self) -> Option<Side> {
        self.post_close_winner_side
    }

    pub(crate) fn oracle_lag_is_selected(&self) -> bool {
        self.oracle_lag_is_selected
    }

    pub(crate) fn oracle_lag_defer_to_round_tail(&self) -> bool {
        self.oracle_lag_defer_to_round_tail
    }

    fn oracle_lag_allow_fallback_open_in_dry_run(&self) -> bool {
        if !self.cfg.dry_run {
            return false;
        }
        std::env::var("PM_ORACLE_LAG_DRYRUN_ALLOW_FALLBACK_OPEN")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no" || v == "off")
            })
            .unwrap_or(true)
    }

    fn oracle_lag_allow_local_agg_hint(&self) -> bool {
        std::env::var("PM_LOCAL_PRICE_AGG_DECISION_ENABLED")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no" || v == "off")
            })
            .unwrap_or(false)
    }

    /// Oracle-lag trading should only act on fully-qualified Chainlink hints.
    /// Returns `(winner_side, open_ref_price, final_price)` when all fields are present.
    pub(crate) fn post_close_chainlink_winner(&self) -> Option<(Side, f64, f64)> {
        if !self.oracle_lag_is_selected {
            return None;
        }
        let side = self.post_close_winner_side()?;
        let allow_fallback_open = self.oracle_lag_allow_fallback_open_in_dry_run();
        let source = self.post_close_winner_source?;
        let source_allowed = match source {
            WinnerHintSource::Chainlink => true,
            WinnerHintSource::LocalAgg => self.oracle_lag_allow_local_agg_hint(),
            _ => false,
        };
        if !source_allowed && !allow_fallback_open {
            return None;
        }
        // First-round / fallback protection: skip trading when open_ref did not
        // come from the exact round_start Chainlink tick (prev_close /
        // frontend_open_fallback). These paths are semantically correct but
        // typically indicate cold-start — add an extra ~300ms of HTTP hop and
        // should not risk FAK capital.
        if source == WinnerHintSource::Chainlink
            && self.post_close_winner_open_is_exact != Some(true)
            && !allow_fallback_open
        {
            return None;
        }
        let ref_price = self.post_close_winner_ref_price;
        let final_price = self.post_close_winner_observed_price;
        if !ref_price.is_finite()
            || !final_price.is_finite()
            || ref_price <= 0.0
            || final_price <= 0.0
        {
            return None;
        }
        Some((side, ref_price, final_price))
    }

    /// True when the clock has passed market_end_ts and we are still within
    /// the oracle_lag_sniping post-close window.
    pub(crate) fn is_in_post_close_window(&self) -> bool {
        let Some(end_ts) = self.cfg.market_end_ts else {
            return false;
        };
        let window = self.cfg.oracle_lag_sniping.window_secs;
        if window == 0 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= end_ts && now < end_ts.saturating_add(window)
    }

    /// Oracle-lag order size helper.
    /// Base size comes from `PM_BID_SIZE`; optional notional cap can shrink it.
    pub(crate) fn oracle_lag_effective_order_size(&self, price: f64) -> f64 {
        let mut size = self.cfg.bid_size.max(0.0);
        let cap = self.cfg.oracle_lag_sniping.max_order_notional_usdc;
        if cap > 0.0 && price.is_finite() && price > 0.0 {
            size = size.min(cap / price);
        }
        // Keep deterministic 2dp sizing aligned with dispatcher.
        (size * 100.0).floor() / 100.0
    }

    pub async fn run(mut self) {
        info!(
            "🎯 Coordinator [OCCAM+LEADLAG] strategy={} pair={:.2} open_pair_band={:.2} bid={:.1} dip_cap={:.2} tick={:.3} net={:.0} reprice={:.3} debounce={}ms watchdog={}ms metrics_log={}s endgame(soft/hard/freeze/maker_repair_min)={}/{}/{}/{}s pair_arb(tier_mode={} risk_open_cutoff={}s) oracle_lag(lab_only={},window={}s,max_notional={:.2}) edge(keep/exit)={:.2}/{:.2} dry={}",
            self.cfg.strategy.as_str(),
            self.cfg.pair_target,
            self.cfg.open_pair_band,
            self.cfg.bid_size,
            self.cfg.dip_buy_max_entry_price,
            self.cfg.tick_size,
            self.cfg.max_net_diff,
            self.cfg.reprice_threshold,
            self.cfg.debounce_ms,
            self.cfg.watchdog_tick_ms,
            self.cfg.strategy_metrics_log_secs,
            self.cfg.endgame_soft_close_secs,
            self.cfg.endgame_hard_close_secs,
            self.cfg.endgame_freeze_secs,
            self.cfg.endgame_maker_repair_min_secs,
            self.cfg.pair_arb.tier_mode.as_str(),
            self.cfg.pair_arb.risk_open_cutoff_secs,
            self.cfg.oracle_lag_sniping.lab_only,
            self.cfg.oracle_lag_sniping.window_secs,
            self.cfg.oracle_lag_sniping.max_order_notional_usdc,
            self.cfg.endgame_edge_keep_mult,
            self.cfg.endgame_edge_exit_mult,
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

                // Oracle-lag winner hint: dedicated channel (non-watch) so one-shot hint
                // cannot be overwritten by high-frequency BookTick updates.
                Some(hint_msg) = self.winner_hint_rx.recv() => {
                    self.handle_market_data(hint_msg).await;
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                Some(feedback) = self.feedback_rx.recv() => {
                    self.handle_execution_feedback(feedback);
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                // Inventory mutations (fills / failed rollbacks / merge sync) must be able to
                // drive an immediate re-evaluation on their own. Pair-gated-tranche in
                // particular needs first-leg fills to flip FlatSeed -> CompletionOnly without
                // waiting for an unrelated market-data tick or slot-release follow-up.
                changed = self.inv_rx.changed() => {
                    if changed.is_err() {
                        break; // Sender dropped
                    }
                    self.tick().await;
                    self.emit_obs_snapshot();
                }

                Some(release) = self.slot_release_rx.recv() => {
                    self.handle_slot_release_event(release).await;
                    let defer_to_inventory_tick = self.cfg.strategy.is_pair_gated_tranche_arb()
                        && release.slot.direction == TradeDirection::Buy;
                    if defer_to_inventory_tick {
                        debug!(
                            "🧭 PGT defer slot-release tick | slot={} reason=await_inventory_change",
                            release.slot.as_str()
                        );
                    } else {
                        self.tick().await;
                    }
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
            "🎯 Shutdown | ticks={} placed={} publish(events={} replace={} cancel={} initial={} policy={} safety={} recovery={}) policy(transitions={} noop_ticks={}) cancel(toxic={} stale={} inv={} reprice={}) ofi(heat_events={} toxic_events={} kill_events={} blocked_ticks={} pair_arb_softened={} pair_arb_suppressed={} pairing_upward_reprice={} opposite_slot_blocked={} stale_target_dropped={} state_forced_republish={}) pair_arb_gate(keep={} skip_inv={} skip_sim={}) pgt(seed={} completion={} taker_shadow_would_open={} taker_shadow_would_close={} dispatch_taker_open={} dispatch_taker_close={} skip_harvest={} skip_tail={} skip_after_rescue={} skip_after_close={} skip_residual={} skip_capital={} skip_invalid_book={} skip_no_seed={} no_visible_be={} stale_target_dropped={}) ref(blocked_ms={} source={} source_binance={} source_poly={} divergence={}) retain(hits={} soft_reset={} full_reset={}) publish(shadow_suppressed={} budget_suppressed={} forced_realign(total={} hard={})) skip(debounce={} backoff={} empty={} inv_limit={})",
            self.stats.ticks,
            self.stats.placed,
            self.stats.publish_events,
            self.stats.replace_events,
            self.stats.cancel_events,
            self.stats.publish_from_initial,
            self.stats.publish_from_policy,
            self.stats.publish_from_safety,
            self.stats.publish_from_recovery,
            self.stats.policy_transition_events,
            self.stats.policy_noop_ticks,
            self.stats.cancel_toxic,
            self.stats.cancel_stale,
            self.stats.cancel_inv,
            self.stats.cancel_reprice,
            self.stats.ofi_heat_events,
            self.stats.ofi_toxic_events,
            self.stats.ofi_kill_events,
            self.stats.ofi_blocked_ticks,
            self.stats.pair_arb_ofi_softened_quotes,
            self.stats.pair_arb_ofi_suppressed_quotes,
            self.stats.pair_arb_pairing_upward_reprice,
            self.stats.pair_arb_opposite_slot_blocked,
            self.stats.pair_arb_stale_target_dropped,
            self.stats.pair_arb_state_forced_republish,
            self.stats.pair_arb_keep_candidates,
            self.stats.pair_arb_skip_inventory_gate,
            self.stats.pair_arb_skip_simulate_buy_none,
            self.stats.pgt_seed_quotes,
            self.stats.pgt_completion_quotes,
            self.stats.pgt_taker_shadow_would_open,
            self.stats.pgt_taker_shadow_would_close,
            self.stats.pgt_dispatch_taker_open,
            self.stats.pgt_dispatch_taker_close,
            self.stats.pgt_skip_harvest,
            self.stats.pgt_skip_tail_completion_only,
            self.stats.pgt_skip_after_rescue_close,
            self.stats.pgt_skip_after_closed_pair,
            self.stats.pgt_skip_residual_guard,
            self.stats.pgt_skip_capital_guard,
            self.stats.pgt_skip_invalid_book,
            self.stats.pgt_skip_no_seed,
            self.stats.pgt_seed_reject_no_visible_breakeven_path,
            self.stats.pgt_stale_target_dropped,
            self.stats.reference_blocked_ms,
            self.stats.blocked_due_source,
            self.stats.blocked_due_binance,
            self.stats.blocked_due_poly,
            self.stats.blocked_due_divergence,
            self.stats.retain_hits,
            self.stats.soft_reset_count,
            self.stats.full_reset_count,
            self.stats.shadow_suppressed_updates,
            self.stats.publish_budget_suppressed,
            self.stats.forced_realign_count,
            self.stats.forced_realign_hard_count,
            self.stats.skipped_debounce,
            self.stats.skipped_backoff,
            self.stats.skipped_empty_book,
            self.stats.skipped_inv_limit,
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
            pair_arb_opposite_slot_blocked: self.stats.pair_arb_opposite_slot_blocked,
            pair_arb_stale_target_dropped: self.stats.pair_arb_stale_target_dropped,
            pair_arb_state_forced_republish: self.stats.pair_arb_state_forced_republish,
            pgt_seed_quotes: self.stats.pgt_seed_quotes,
            pgt_completion_quotes: self.stats.pgt_completion_quotes,
            pgt_entry_pressure_sides: self.stats.pgt_entry_pressure_sides,
            pgt_entry_pressure_extra_ticks: self.stats.pgt_entry_pressure_extra_ticks,
            pgt_taker_shadow_would_open: self.stats.pgt_taker_shadow_would_open,
            pgt_taker_shadow_would_close: self.stats.pgt_taker_shadow_would_close,
            pgt_skip_harvest: self.stats.pgt_skip_harvest,
            pgt_skip_tail_completion_only: self.stats.pgt_skip_tail_completion_only,
            pgt_skip_residual_guard: self.stats.pgt_skip_residual_guard,
            pgt_skip_capital_guard: self.stats.pgt_skip_capital_guard,
            pgt_skip_invalid_book: self.stats.pgt_skip_invalid_book,
            pgt_skip_no_seed: self.stats.pgt_skip_no_seed,
            pgt_skip_geometry_guard: self.stats.pgt_skip_geometry_guard,
            pgt_seed_reject_no_visible_breakeven_path: self
                .stats
                .pgt_seed_reject_no_visible_breakeven_path,
            pgt_single_seed_bias: self.stats.pgt_single_seed_bias,
            pgt_single_seed_first_side: self.stats.pgt_single_seed_first_side,
            pgt_single_seed_last_side: self.stats.pgt_single_seed_last_side,
            pgt_single_seed_flip_count: self.stats.pgt_single_seed_flip_count,
            pgt_dual_seed_quotes: self.stats.pgt_dual_seed_quotes,
            pgt_single_seed_released_to_dual: self.stats.pgt_single_seed_released_to_dual,
            pgt_post_flow_quotes: self.stats.pgt_post_flow_quotes,
            pgt_dispatch_intents: self.stats.pgt_dispatch_intents,
            pgt_dispatch_blocked: self.stats.pgt_dispatch_blocked,
            pgt_dispatch_place: self.stats.pgt_dispatch_place,
            pgt_dispatch_taker_open: self.stats.pgt_dispatch_taker_open,
            pgt_dispatch_taker_close: self.stats.pgt_dispatch_taker_close,
            pgt_dispatch_retain: self.stats.pgt_dispatch_retain,
            pgt_dispatch_clear: self.stats.pgt_dispatch_clear,
            pgt_stale_target_dropped: self.stats.pgt_stale_target_dropped,
            pgt_xuan_m0001_no_seed: self.stats.pgt_xuan_m0001_no_seed,
            pgt_dplus_minorder_no_seed: self.stats.pgt_dplus_minorder_no_seed,
            pgt_high_pressure_no_seed: self.stats.pgt_high_pressure_no_seed,
            market_trade_ticks: self.stats.market_trade_ticks,
            market_sell_trade_ticks: self.stats.market_sell_trade_ticks,
        };
        let _ = obs_tx.send(snapshot);
    }

    fn record_strategy_quote_diagnostics(&mut self, quotes: &StrategyQuotes) {
        let had_single_seed_before = self.stats.pgt_single_seed_bias > 0;
        let pgt_single_seed_side = if quotes.diagnostics.pgt_single_seed_bias > 0 {
            match (quotes.yes_buy.is_some(), quotes.no_buy.is_some()) {
                (true, false) => Some(Side::Yes),
                (false, true) => Some(Side::No),
                _ => None,
            }
        } else {
            None
        };
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
        self.stats.pgt_seed_quotes = self
            .stats
            .pgt_seed_quotes
            .saturating_add(quotes.diagnostics.pgt_seed_quotes as u64);
        self.stats.pgt_completion_quotes = self
            .stats
            .pgt_completion_quotes
            .saturating_add(quotes.diagnostics.pgt_completion_quotes as u64);
        self.stats.pgt_skip_harvest = self
            .stats
            .pgt_skip_harvest
            .saturating_add(quotes.diagnostics.pgt_skip_harvest as u64);
        self.stats.pgt_skip_tail_completion_only = self
            .stats
            .pgt_skip_tail_completion_only
            .saturating_add(quotes.diagnostics.pgt_skip_tail_completion_only as u64);
        self.stats.pgt_skip_after_rescue_close = self
            .stats
            .pgt_skip_after_rescue_close
            .saturating_add(quotes.diagnostics.pgt_skip_after_rescue_close as u64);
        self.stats.pgt_skip_after_closed_pair = self
            .stats
            .pgt_skip_after_closed_pair
            .saturating_add(quotes.diagnostics.pgt_skip_after_closed_pair as u64);
        self.stats.pgt_skip_residual_guard = self
            .stats
            .pgt_skip_residual_guard
            .saturating_add(quotes.diagnostics.pgt_skip_residual_guard as u64);
        self.stats.pgt_skip_capital_guard = self
            .stats
            .pgt_skip_capital_guard
            .saturating_add(quotes.diagnostics.pgt_skip_capital_guard as u64);
        self.stats.pgt_skip_invalid_book = self
            .stats
            .pgt_skip_invalid_book
            .saturating_add(quotes.diagnostics.pgt_skip_invalid_book as u64);
        self.stats.pgt_skip_no_seed = self
            .stats
            .pgt_skip_no_seed
            .saturating_add(quotes.diagnostics.pgt_skip_no_seed as u64);
        for (dst, src) in self
            .stats
            .pgt_xuan_m0001_no_seed
            .iter_mut()
            .zip(quotes.diagnostics.pgt_xuan_m0001_no_seed)
        {
            *dst = dst.saturating_add(src as u64);
        }
        for (dst, src) in self
            .stats
            .pgt_dplus_minorder_no_seed
            .iter_mut()
            .zip(quotes.diagnostics.pgt_dplus_minorder_no_seed)
        {
            *dst = dst.saturating_add(src as u64);
        }
        for (dst, src) in self
            .stats
            .pgt_high_pressure_no_seed
            .iter_mut()
            .zip(quotes.diagnostics.pgt_high_pressure_no_seed)
        {
            *dst = dst.saturating_add(src as u64);
        }
        self.stats.pgt_skip_geometry_guard = self
            .stats
            .pgt_skip_geometry_guard
            .saturating_add(quotes.diagnostics.pgt_skip_geometry_guard as u64);
        self.stats.pgt_seed_reject_no_visible_breakeven_path = self
            .stats
            .pgt_seed_reject_no_visible_breakeven_path
            .saturating_add(quotes.diagnostics.pgt_seed_reject_no_visible_breakeven_path as u64);
        self.stats.pgt_single_seed_bias = self
            .stats
            .pgt_single_seed_bias
            .saturating_add(quotes.diagnostics.pgt_single_seed_bias as u64);
        if let Some(side) = pgt_single_seed_side {
            if self.stats.pgt_single_seed_first_side.is_none() {
                self.stats.pgt_single_seed_first_side = Some(side);
            }
            if let Some(prev_side) = self.stats.pgt_single_seed_last_side {
                if prev_side != side {
                    self.stats.pgt_single_seed_flip_count =
                        self.stats.pgt_single_seed_flip_count.saturating_add(1);
                }
            }
            self.stats.pgt_single_seed_last_side = Some(side);
        }
        let pgt_dual_seed = matches!(
            (&quotes.yes_buy, &quotes.no_buy),
            (Some(yes), Some(no))
                if yes.reason == BidReason::Provide
                    && no.reason == BidReason::Provide
                    && yes.direction == TradeDirection::Buy
                    && no.direction == TradeDirection::Buy
        );
        if pgt_dual_seed {
            self.stats.pgt_dual_seed_quotes = self.stats.pgt_dual_seed_quotes.saturating_add(1);
            if had_single_seed_before && !self.stats.pgt_single_seed_released_to_dual_recorded {
                self.stats.pgt_single_seed_released_to_dual = self
                    .stats
                    .pgt_single_seed_released_to_dual
                    .saturating_add(1);
                self.stats.pgt_single_seed_released_to_dual_recorded = true;
            }
        }
        self.stats.pgt_entry_pressure_sides = self
            .stats
            .pgt_entry_pressure_sides
            .saturating_add(quotes.diagnostics.pgt_entry_pressure_sides as u64);
        self.stats.pgt_entry_pressure_extra_ticks = self
            .stats
            .pgt_entry_pressure_extra_ticks
            .saturating_add(quotes.diagnostics.pgt_entry_pressure_extra_ticks as u64);
        self.stats.pgt_taker_shadow_would_open = self
            .stats
            .pgt_taker_shadow_would_open
            .saturating_add(quotes.diagnostics.pgt_taker_shadow_would_open as u64);
        self.stats.pgt_taker_shadow_would_close = self
            .stats
            .pgt_taker_shadow_would_close
            .saturating_add(quotes.diagnostics.pgt_taker_shadow_would_close as u64);
    }

    pub(super) fn execution_toxic_block_applies(&self) -> bool {
        !self.cfg.strategy.is_pair_arb()
    }

    // ═════════════════════════════════════════════════
    // Book update with fallback
    // ═════════════════════════════════════════════════

    fn update_book(&mut self, yb: f64, ya: f64, nb: f64, na: f64) {
        let pgt_bid_only_freshness = self.cfg.strategy.is_pair_gated_tranche_arb();
        if yb.is_finite() {
            self.book.yes_bid = yb.max(0.0);
        }
        if ya.is_finite() {
            self.book.yes_ask = ya.max(0.0);
        }
        if nb.is_finite() {
            self.book.no_bid = nb.max(0.0);
        }
        if na.is_finite() {
            self.book.no_ask = na.max(0.0);
        }

        // P5 FIX: Update per-side timestamps independently.
        // Shared timestamp caused YES updates to mask NO staleness.
        let yes_fresh = if pgt_bid_only_freshness {
            yb.is_finite() && self.book.yes_bid > 0.0
        } else {
            yb.is_finite() && ya.is_finite() && self.book.yes_bid > 0.0 && self.book.yes_ask > 0.0
        };
        if yes_fresh {
            self.last_valid_book.yes_bid = self.book.yes_bid;
            if self.book.yes_ask > 0.0 {
                self.last_valid_book.yes_ask = self.book.yes_ask;
            }
            self.last_valid_ts_yes = Instant::now();
            self.yes_stale_since = None;
        }
        let no_fresh = if pgt_bid_only_freshness {
            nb.is_finite() && self.book.no_bid > 0.0
        } else {
            nb.is_finite() && na.is_finite() && self.book.no_bid > 0.0 && self.book.no_ask > 0.0
        };
        if no_fresh {
            self.last_valid_book.no_bid = self.book.no_bid;
            if self.book.no_ask > 0.0 {
                self.last_valid_book.no_ask = self.book.no_ask;
            }
            self.last_valid_ts_no = Instant::now();
            self.no_stale_since = None;
        }
    }

    pub(crate) fn recent_public_trade(&self, max_age: Duration) -> Option<PublicTradeSnapshot> {
        let trade = self.last_public_trade?;
        if Instant::now().saturating_duration_since(trade.ts) <= max_age {
            Some(trade)
        } else {
            None
        }
    }

    pub(crate) fn recent_public_trade_for(
        &self,
        side: Side,
        max_age: Duration,
    ) -> Option<PublicTradeSnapshot> {
        let trade = self.last_public_trade_by_side[side.index()]?;
        if Instant::now().saturating_duration_since(trade.ts) <= max_age {
            Some(trade)
        } else {
            None
        }
    }

    pub(crate) fn recent_public_buy_trade_for(
        &self,
        side: Side,
        max_age: Duration,
    ) -> Option<PublicTradeSnapshot> {
        let trade = self.last_public_buy_trade_by_side[side.index()]?;
        if Instant::now().saturating_duration_since(trade.ts) <= max_age {
            Some(trade)
        } else {
            None
        }
    }

    pub(crate) fn recent_high_side_public_buy_pressure(
        &self,
        max_age: Duration,
        lookback: Duration,
    ) -> Option<PublicBuyPressureSnapshot> {
        let latest_trade = self.last_high_side_public_buy_trade?;
        if Instant::now().saturating_duration_since(latest_trade.ts) > max_age {
            return None;
        }
        let cutoff = latest_trade
            .ts
            .checked_sub(lookback)
            .unwrap_or(latest_trade.ts);
        let mut high_qty = 0.0;
        let mut low_qty = 0.0;
        for event in self.public_buy_pressure_events.iter() {
            if event.ts < cutoff || event.ts > latest_trade.ts {
                continue;
            }
            match event.alignment {
                PublicTradeSideAlignment::High => high_qty += event.size.max(0.0),
                PublicTradeSideAlignment::Low => low_qty += event.size.max(0.0),
            }
        }
        Some(PublicBuyPressureSnapshot {
            latest_trade,
            high_qty,
            low_qty,
            pressure_ratio: high_qty / low_qty.max(1.0),
        })
    }

    pub(crate) fn pgt_buy_slot_age(&self, side: Side) -> Duration {
        self.slot_last_ts(OrderSlot::new(side, TradeDirection::Buy))
            .elapsed()
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

    fn pgt_should_hold_buy_target_on_global_stale(&self, side: Side) -> bool {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() {
            return false;
        }
        let slot = OrderSlot::new(side, TradeDirection::Buy);
        self.slot_target_active(slot)
            && matches!(
                self.side_target_reason(side),
                Some(BidReason::Provide | BidReason::Hedge)
            )
    }

    fn public_trade_alignment_for(&self, side: Side) -> Option<PublicTradeSideAlignment> {
        let (side_bid, side_ask, opp_bid, opp_ask) = match side {
            Side::Yes => (
                self.book.yes_bid,
                self.book.yes_ask,
                self.book.no_bid,
                self.book.no_ask,
            ),
            Side::No => (
                self.book.no_bid,
                self.book.no_ask,
                self.book.yes_bid,
                self.book.yes_ask,
            ),
        };
        let side_mid = if side_bid > 0.0 && side_ask > 0.0 {
            0.5 * (side_bid + side_ask)
        } else if side_bid > 0.0 {
            side_bid
        } else {
            return None;
        };
        let opp_mid = if opp_bid > 0.0 && opp_ask > 0.0 {
            0.5 * (opp_bid + opp_ask)
        } else if opp_bid > 0.0 {
            opp_bid
        } else {
            return None;
        };
        if side_mid + 1e-9 >= opp_mid {
            Some(PublicTradeSideAlignment::High)
        } else {
            Some(PublicTradeSideAlignment::Low)
        }
    }

    fn record_public_buy_pressure(&mut self, snapshot: PublicTradeSnapshot) {
        let Some(alignment) = self.public_trade_alignment_for(snapshot.market_side) else {
            return;
        };
        self.public_buy_pressure_events
            .push_back(PublicBuyPressureEvent {
                alignment,
                size: snapshot.size.max(0.0),
                ts: snapshot.ts,
            });
        if alignment == PublicTradeSideAlignment::High {
            self.last_high_side_public_buy_trade = Some(snapshot);
        }
        let retention = Duration::from_secs(PUBLIC_BUY_PRESSURE_RETENTION_SECS);
        while self
            .public_buy_pressure_events
            .front()
            .is_some_and(|event| snapshot.ts.saturating_duration_since(event.ts) > retention)
        {
            self.public_buy_pressure_events.pop_front();
        }
    }

    pub(crate) fn pgt_flat_seed_latched_side(&self) -> Option<Side> {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() {
            return None;
        }
        let now = Instant::now();
        if !self.pgt_flat_seed_latch_exhausted {
            if let (Some(side), Some(until)) = (
                self.pgt_flat_seed_latched_side,
                self.pgt_flat_seed_latched_until,
            ) {
                if now < until {
                    return Some(side);
                }
            }
        }
        // Even after the time latch exhausts, keep an already-live flat seed on
        // its side. Releasing side selection while a maker order is live burns
        // queue priority and recreates the reprice/cancel churn this latch is
        // meant to prevent.
        let yes_active = self.slot_target_active(OrderSlot::YES_BUY)
            && matches!(self.side_target_reason(Side::Yes), Some(BidReason::Provide));
        let no_active = self.slot_target_active(OrderSlot::NO_BUY)
            && matches!(self.side_target_reason(Side::No), Some(BidReason::Provide));
        match (yes_active, no_active) {
            (true, false) => Some(Side::Yes),
            (false, true) => Some(Side::No),
            _ => None,
        }
    }

    pub(crate) fn pgt_flat_seed_latch_exhausted(&self) -> bool {
        self.cfg.strategy.is_pair_gated_tranche_arb() && self.pgt_flat_seed_latch_exhausted
    }

    pub(crate) fn pgt_blocks_new_seed_after_rescue_close(&self) -> bool {
        self.cfg.strategy.is_pair_gated_tranche_arb() && self.pgt_taker_close_rescue_fired
    }

    pub(crate) fn pgt_post_close_reopen_attempted_for_fill_count(
        &self,
        round_buy_fill_count: u64,
    ) -> bool {
        self.cfg.strategy.is_pair_gated_tranche_arb()
            && self.pgt_post_close_reopen_attempted_fill_count == Some(round_buy_fill_count)
    }

    fn update_pgt_flat_seed_latch(&mut self, quotes: &StrategyQuotes, has_active_tranche: bool) {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() || !self.cfg.dry_run {
            self.pgt_flat_seed_latched_side = None;
            self.pgt_flat_seed_latched_since = None;
            self.pgt_flat_seed_latched_until = None;
            self.pgt_flat_seed_latch_exhausted = false;
            return;
        }
        if has_active_tranche {
            self.pgt_flat_seed_latched_side = None;
            self.pgt_flat_seed_latched_since = None;
            self.pgt_flat_seed_latched_until = None;
            self.pgt_flat_seed_latch_exhausted = false;
            return;
        }
        if self.pgt_flat_seed_latch_exhausted {
            return;
        }
        let now = Instant::now();
        if quotes.diagnostics.pgt_single_seed_bias == 0 {
            return;
        }
        let side = match (quotes.yes_buy.is_some(), quotes.no_buy.is_some()) {
            (true, false) => Some(Side::Yes),
            (false, true) => Some(Side::No),
            _ => None,
        };
        if let Some(side) = side {
            if let (Some(latched_side), Some(since), Some(until)) = (
                self.pgt_flat_seed_latched_side,
                self.pgt_flat_seed_latched_since,
                self.pgt_flat_seed_latched_until,
            ) {
                if now.duration_since(since) >= Duration::from_millis(PGT_FLAT_SEED_LATCH_MAX_MS) {
                    self.pgt_flat_seed_latched_side = None;
                    self.pgt_flat_seed_latched_since = None;
                    self.pgt_flat_seed_latched_until = None;
                    self.pgt_flat_seed_latch_exhausted = true;
                    return;
                }
                if latched_side == side {
                    self.pgt_flat_seed_latched_until =
                        Some(now + Duration::from_millis(PGT_FLAT_SEED_LATCH_MS));
                    return;
                }
                if now < until {
                    return;
                }
            }
            self.pgt_flat_seed_latched_side = Some(side);
            self.pgt_flat_seed_latched_since = Some(now);
            self.pgt_flat_seed_latched_until =
                Some(now + Duration::from_millis(PGT_FLAT_SEED_LATCH_MS));
        }
    }

    /// Raw (non-fallback) ask for the given side. Zero means no current ask.
    /// Use this in OracleLagSniping compute_quotes to avoid stale last_valid_book
    /// ask triggering the FAK branch when the ask has actually disappeared.
    pub(crate) fn raw_book_ask(&self, side: crate::polymarket::types::Side) -> f64 {
        match side {
            crate::polymarket::types::Side::Yes => self.book.yes_ask,
            crate::polymarket::types::Side::No => self.book.no_ask,
        }
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
        if !self.cfg.strategy.is_glft_mm() {
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
        if !self.cfg.strategy.is_glft_mm() || self.glft_is_tradeable_snapshot(glft) {
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
        let reprice_ratio = reprice_ratio_raw;
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
            .saturating_add(self.stats.pair_arb_skip_simulate_buy_none);
        let pair_arb_softened_ratio = Self::obs_ratio(
            self.stats.pair_arb_ofi_softened_quotes,
            pair_arb_gate_attempts,
        );
        let lvl_heat = if self.cfg.strategy.is_pair_arb() {
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
            "🏷️ LIVE_OBS[{}] placed={} sample_floor={} replace_ratio={:.2}({}) replace_per_min={:.2}({}) reprice_ratio={:.2}({}) reprice_ratio_raw={:.2} ref_blocked_ms={}({};scope={}) heat_events={}({}) pair_arb_softened_ratio={:.2} toxic_events={} kill_events={} source_blocked={}({}) flags={}",
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
        self.observe_completion_first_inventory_transition(&inv_snapshot, now);
        let glft_snapshot = if self.cfg.strategy.is_glft_mm() {
            Some(*self.glft_rx.borrow())
        } else {
            None
        };
        if self.cfg.strategy.is_glft_mm()
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
        let track_ref_blocked = if self.cfg.strategy.is_glft_mm() {
            self.glft_ready_seen
        } else {
            true
        };
        if let Some(snapshot) = glft_snapshot {
            if self.cfg.strategy.is_glft_mm() {
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

        // OracleLagSniping post-close immunity: after market close the CLOB stops sending
        // book ticks (expected). Strongly-directional pre-close markets may also have
        // yes_ask=0 for 30s+ (no sellers), preventing last_valid_ts_yes from updating.
        // Both cases must be exempt from stale guards; toxic-flow cancels still apply.
        let post_close_stale_immune =
            self.cfg.strategy.is_oracle_lag_sniping() && self.is_in_post_close_window();

        // Oracle-lag maker lifecycle: once we leave post-close window, force-clear any
        // residual oracle-lag maker BUY targets so they cannot leak into the next round.
        if self.cfg.strategy.is_oracle_lag_sniping() && !self.is_in_post_close_window() {
            for slot in [OrderSlot::YES_BUY, OrderSlot::NO_BUY] {
                let should_clear = self
                    .slot_target(slot)
                    .is_some_and(|t| t.reason == BidReason::OracleLagProvide);
                if should_clear {
                    self.clear_slot_target(slot, CancelReason::Reprice).await;
                }
            }
        }

        // Priority 1: 30s Staleness Guard (Critical Shutdown)
        if self.is_book_stale() && !post_close_stale_immune {
            if self.yes_target.is_some() {
                if self.pgt_should_hold_buy_target_on_global_stale(Side::Yes) {
                    debug!("🧭 PGT stale hold — retaining YES across global book expiry");
                } else {
                    warn!("⚠️ Book expired (>30s) — clearing YES");
                    self.clear_target(Side::Yes, CancelReason::StaleData).await;
                }
            }
            if self.no_target.is_some() {
                if self.pgt_should_hold_buy_target_on_global_stale(Side::No) {
                    debug!("🧭 PGT stale hold — retaining NO across global book expiry");
                } else {
                    warn!("⚠️ Book expired (>30s) — clearing NO");
                    self.clear_target(Side::No, CancelReason::StaleData).await;
                }
            }
            return;
        }

        // Priority 2: Per-side Toxic/Stale guard (independent of book availability)

        if yes_toxic_blocked || (yes_stale_actionable && !post_close_stale_immune) {
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
                        let pgt_retain_on_stale = self.cfg.strategy.is_pair_gated_tranche_arb()
                            && slot.direction == TradeDirection::Buy
                            && self.slot_target(slot).is_some_and(|t| {
                                matches!(t.reason, BidReason::Provide | BidReason::Hedge)
                            });
                        if pgt_retain_on_stale {
                            continue;
                        }
                        self.clear_slot_target(slot, CancelReason::StaleData).await;
                    }
                }
            }
        }
        if no_toxic_blocked || (no_stale_actionable && !post_close_stale_immune) {
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
                        let pgt_retain_on_stale = self.cfg.strategy.is_pair_gated_tranche_arb()
                            && slot.direction == TradeDirection::Buy
                            && self.slot_target(slot).is_some_and(|t| {
                                matches!(t.reason, BidReason::Provide | BidReason::Hedge)
                            });
                        if pgt_retain_on_stale {
                            continue;
                        }
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

        // Post-close book snapshot: 500 ms rate-limited, OracleLagSniping only.
        // Emits per-tick book state so we can audit ask availability in the post-close window.
        if self.cfg.strategy.is_oracle_lag_sniping() && self.is_in_post_close_window() {
            let emit = match self.last_post_close_snapshot_ts {
                None => true,
                Some(prev) => prev.elapsed() >= Duration::from_millis(500),
            };
            if emit {
                self.last_post_close_snapshot_ts = Some(now);
                let raw_book = self.book;
                let winner_side = self.post_close_winner_side;
                let (winner_bid, winner_ask) = match winner_side {
                    Some(Side::Yes) => (raw_book.yes_bid, raw_book.yes_ask),
                    Some(Side::No) => (raw_book.no_bid, raw_book.no_ask),
                    None => (0.0, 0.0),
                };
                let winner_tick = if winner_bid > ORACLE_LAG_MICRO_TICK_BID_BOUNDARY
                    || winner_ask > 0.96
                    || (winner_bid > 0.0 && winner_bid < 0.04)
                {
                    0.001
                } else {
                    self.cfg.tick_size.max(1e-9)
                };
                let winner_spread_ticks = if winner_bid > 0.0 && winner_ask > 0.0 {
                    (winner_ask - winner_bid) / winner_tick
                } else {
                    0.0
                };
                let winner_ask_tradable = winner_ask > 0.0
                    && (winner_bid <= 0.0 || winner_ask > winner_bid + 0.5 * winner_tick + 1e-9);
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let market_end_ms = self
                    .cfg
                    .market_end_ts
                    .map(|ts| (ts as i64).saturating_mul(1_000));
                let t_post_close_ms = market_end_ms.map(|end| now_ms.saturating_sub(end));
                info!(
                    "📖 post_close_book_tick | t_post_close_ms={:?} yes_bid={:.4} yes_ask={:.4} no_bid={:.4} no_ask={:.4} winner_side={:?} winner_bid={:.4} winner_ask={:.4} has_winner_ask={} winner_ask_tradable={} winner_spread_ticks={:.2} stale_no={}",
                    t_post_close_ms,
                    raw_book.yes_bid,
                    raw_book.yes_ask,
                    raw_book.no_bid,
                    raw_book.no_ask,
                    winner_side,
                    winner_bid,
                    winner_ask,
                    winner_ask > 0.0,
                    winner_ask_tradable,
                    winner_spread_ticks,
                    no_stale_raw,
                );
            }
        }
        if self.cfg.strategy.is_oracle_lag_sniping() {
            self.maybe_oracle_lag_followup_upward_reprice().await;
        }

        // Priority 4: Strategy quote + unified flow-risk overlay + execution.
        let metrics = self.derive_inventory_metrics(&decision_inv);
        let input = StrategyTickInput {
            inv: &decision_inv,
            settled_inv: &settled_inv,
            working_inv: &working_inv,
            inventory: &inv_snapshot,
            pair_ledger: &inv_snapshot.pair_ledger,
            episode_metrics: &inv_snapshot.episode_metrics,
            book: &ub,
            metrics: &metrics,
            ofi: Some(&ofi),
            glft: glft_snapshot.as_ref(),
        };
        let mut quotes = self.cfg.strategy.compute_quotes(self, input);
        if self.cfg.strategy == StrategyKind::CompletionFirst {
            self.emit_completion_first_decision_events(&inv_snapshot, &quotes);
            if self.completion_first_mode() == CompletionFirstMode::Shadow {
                quotes = StrategyQuotes::default();
            }
        }
        self.record_strategy_quote_diagnostics(&quotes);
        // Raw staleness blocks new intents immediately; the actionable variant
        // only controls delayed clearing of already-live targets.
        self.apply_flow_risk(
            &working_inv,
            &mut quotes,
            yes_stale_raw && !post_close_stale_immune,
            no_stale_raw && !post_close_stale_immune,
            yes_toxic_blocked,
            no_toxic_blocked,
        );
        self.update_pgt_flat_seed_latch(&quotes, inv_snapshot.pair_ledger.active_tranche.is_some());
        if self.cfg.strategy.is_pair_gated_tranche_arb() {
            let remaining_quotes =
                u64::from(quotes.yes_buy.is_some()) + u64::from(quotes.no_buy.is_some());
            self.stats.pgt_post_flow_quotes = self
                .stats
                .pgt_post_flow_quotes
                .saturating_add(remaining_quotes);
        }
        self.execute_quotes(
            &working_inv,
            &ub,
            quotes,
            yes_stale_raw && !post_close_stale_immune,
            no_stale_raw && !post_close_stale_immune,
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
        let stale_blocks = stale && !self.cfg.strategy.is_oracle_lag_sniping();
        if stale_blocks {
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
                if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
                    let idx = slot.index();
                    self.slot_pair_arb_cross_reject_extra_ticks[idx] =
                        self.slot_pair_arb_cross_reject_extra_ticks[idx].saturating_add(1);
                    self.slot_pair_arb_last_cross_rejected_action_price[idx] =
                        Some(rejected_action_price);
                    self.slot_pair_arb_cross_reject_reprice_pending[idx] = true;
                    self.slot_pair_arb_fill_recheck_pending[idx] = true;
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
                if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
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
                    self.slot_pair_arb_cross_reject_reprice_pending[idx] = false;
                    self.slot_pair_arb_state_republish_latched[idx] = false;
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
            ExecutionFeedback::PlacementRejected {
                side,
                reason,
                kind,
                ts: _,
            } => {
                if self.cfg.strategy.is_oracle_lag_sniping()
                    && self.cfg.oracle_lag_sniping.market_enabled
                    && kind == RejectKind::BalanceOrAllowance
                {
                    // Keep immediate FAK path alive across bursty multi-market finals:
                    // allowance/headroom can transiently fail and recover within the same round.
                    // Only halt on maker fallback rejects to avoid repeated stale rest submits.
                    if reason == BidReason::OracleLagProvide {
                        if !self.oracle_lag_round_halted {
                            warn!(
                                "🛑 oracle_lag_round_halt | reason=balance_or_allowance_reject side={:?} bid_reason={:?} selected_round_end_ts={:?}",
                                side, reason, self.oracle_lag_selected_round_end_ts
                            );
                        }
                        self.oracle_lag_round_halted = true;
                        self.oracle_lag_round_halt_kind = Some(kind);
                    } else {
                        info!(
                            "⏭️ oracle_lag_round_halt_skip | reason=balance_or_allowance_reject side={:?} bid_reason={:?} keep_winner_hint_immediate=true",
                            side, reason
                        );
                    }
                }
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
