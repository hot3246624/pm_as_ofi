//! OFI Engine Actor — Order Flow Imbalance detector with per-side tracking.
//!
//! Maintains separate sliding windows for YES and NO tokens.
//! Each side has its own OFI score and toxicity flag.
//! The Coordinator uses per-side toxicity to decide whether it's safe
//! to buy a specific side (e.g., "is it safe to buy NO right now?").

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use super::messages::{
    KillSwitchSignal, MarketDataMsg, OfiSnapshot, SideOfi, TakerSide, TradeDirection,
};
use super::types::Side;

/// Tail quantiles for regime-aware OFI gating.
/// Q99 is used for toxic entry; Q95 is used as the recovery reference.
const ENTER_TAIL_QUANTILE: f64 = 0.99;
const EXIT_TAIL_QUANTILE: f64 = 0.95;
const BASELINE_TAIL_QUANTILE: f64 = 0.50;
const SATURATION_LOG_HEARTBEATS: u8 = 10;
const NORMALIZED_ENTER_MIN: f64 = 1.2;
const NORMALIZED_ENTER_MAX: f64 = 8.0;
const TOXIC_REENTER_COOLDOWN_MS: u64 = 1200;
const TOXIC_CONFIRM_MOVE_TICKS: f64 = 2.0;
const TOXIC_CONFIRM_HEARTBEATS: u8 = 2;
const TOXIC_PERSIST_TICKS: f64 = 0.5;
const HEAT_ENTER_CONFIRM_HEARTBEATS: u8 = 2;
const HEAT_EXIT_CONFIRM_HEARTBEATS: u8 = 2;
const HEAT_MIN_HOLD_MS: u64 = 800;
const HEAT_ADAPTIVE_ENTER_DWELL_MS: u64 = 1000;
const HEAT_ADAPTIVE_EXIT_DWELL_MS: u64 = 1400;
const HEAT_REENTER_COOLDOWN_MS: u64 = 1800;
const HEAT_SCORE_EMA_ALPHA: f64 = 0.35;

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

/// OFI engine configuration. All values configurable at startup.
#[derive(Debug, Clone)]
pub struct OfiConfig {
    /// Polymarket tick size for reference-mid adverse-selection confirmation.
    pub tick_size: f64,
    /// Sliding window duration. Only ticks within this window are counted.
    /// Default: 3 seconds.
    pub window_duration: Duration,

    /// Toxicity threshold on |OFI score| per side.
    /// When |buy_vol − sell_vol| > threshold, that side's flow is toxic.
    /// Default: 200.0 (calibrated from live btc-updown-5m: median spikes 100-200, peaks 1000).
    pub toxicity_threshold: f64,

    /// Heartbeat interval in milliseconds for evicting expired trades
    /// even when no new trades arrive. Default: 200.
    pub heartbeat_ms: u64,

    // ── Opt-2: Adaptive threshold ──────────────────────────────
    /// Enable regime-aware adaptive toxicity thresholds.
    /// When true, OFI uses rolling tail quantiles (Q99 enter / Q95 exit).
    /// `toxicity_threshold` only acts as the cold-start fallback.
    pub adaptive_threshold: bool,
    /// Legacy compatibility knob from the old mean+sigma model.
    /// No longer used in the current tail-quantile regime.
    pub adaptive_k: f64,
    /// Floor for the adaptive threshold. Prevents false-positives on thin markets.
    /// Default: 50.0.
    pub adaptive_min: f64,
    /// Legacy compatibility knob from the old capped-threshold model.
    /// Ignored when `adaptive_threshold=true`.
    pub adaptive_max: f64,
    /// Legacy compatibility knob from the old rise-cap model.
    /// Ignored when `adaptive_threshold=true`.
    pub adaptive_rise_cap_pct: f64,
    /// Rolling window size (number of per-heartbeat OFI observations) for statistics.
    /// Default: 200 (≈ 40 seconds at 200ms heartbeat).
    pub adaptive_window: usize,
    /// Optional imbalance-ratio gate for toxicity entry.
    /// ratio = |buy - sell| / (buy + sell + eps)
    /// 0 disables ratio gating.
    /// Default: 0.45.
    pub toxicity_ratio_enter: f64,
    /// Optional imbalance-ratio recovery threshold (hysteresis).
    /// Effective only when `toxicity_ratio_enter > 0`.
    /// Default: 0.30.
    pub toxicity_ratio_exit: f64,
    /// Exit threshold ratio for hysteresis when currently toxic.
    /// Side leaves toxic only when |OFI| <= threshold * exit_ratio.
    /// Default: 0.85.
    pub toxicity_exit_ratio: f64,
    /// Minimum toxic hold duration before allowing recovery (ms).
    /// Default: 800ms.
    pub min_toxic_ms: u64,
}

impl Default for OfiConfig {
    fn default() -> Self {
        Self {
            tick_size: 0.01,
            window_duration: Duration::from_secs(3),
            toxicity_threshold: 200.0,
            heartbeat_ms: 200,
            adaptive_threshold: false,
            adaptive_k: 3.0,
            adaptive_min: 50.0,
            adaptive_max: 1000.0,
            adaptive_rise_cap_pct: 0.20,
            adaptive_window: 200,
            toxicity_ratio_enter: 0.45,
            toxicity_ratio_exit: 0.30,
            toxicity_exit_ratio: 0.85,
            min_toxic_ms: 800,
        }
    }
}

impl OfiConfig {
    /// Load overrides from environment variables (if set).
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("PM_TICK_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                if (0.0..1.0).contains(&f) {
                    cfg.tick_size = f;
                }
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_WINDOW_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.window_duration = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_TOXICITY_THRESHOLD") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.toxicity_threshold = f;
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_HEARTBEAT_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.heartbeat_ms = ms;
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE") {
            cfg.adaptive_threshold = v != "0" && v.to_lowercase() != "false";
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE_K") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.adaptive_k = f.max(0.5);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE_MIN") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.adaptive_min = f.max(1.0);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE_MAX") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.adaptive_max = if f <= 0.0 {
                    0.0 // 0 => no hard upper cap
                } else {
                    f.max(cfg.adaptive_min)
                };
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE_RISE_CAP_PCT") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.adaptive_rise_cap_pct = f.clamp(0.0, 5.0);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_ADAPTIVE_WINDOW") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.adaptive_window = n.max(10);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_RATIO_ENTER") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.toxicity_ratio_enter = f.clamp(0.0, 1.0);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_RATIO_EXIT") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.toxicity_ratio_exit = f.clamp(0.0, 1.0);
            }
        }
        if cfg.toxicity_ratio_exit > cfg.toxicity_ratio_enter {
            cfg.toxicity_ratio_exit = cfg.toxicity_ratio_enter;
        }
        if let Ok(v) = std::env::var("PM_OFI_EXIT_RATIO") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.toxicity_exit_ratio = f.clamp(0.05, 0.99);
            }
        }
        if let Ok(v) = std::env::var("PM_OFI_MIN_TOXIC_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.min_toxic_ms = ms;
            }
        }
        cfg
    }
}

// ─────────────────────────────────────────────────────────
// Internal tick record
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct TradeTick {
    taker_side: TakerSide,
    size: f64,
    ts: Instant,
}

/// Hard cap on ticks per side window.
/// Prevents unbounded memory growth in high-frequency markets where thousands
/// of trades can arrive within the 3-second sliding window.
/// 4 096 ticks ≈ ~1 300 trades/s sustained — well above any realistic load.
const MAX_WINDOW_TICKS: usize = 4096;

/// Per-side sliding window.
#[derive(Debug)]
struct SideWindow {
    ticks: VecDeque<TradeTick>,
}

impl SideWindow {
    fn new() -> Self {
        Self {
            ticks: VecDeque::with_capacity(1024),
        }
    }

    fn push(&mut self, taker_side: TakerSide, size: f64, ts: Instant) {
        // ISSUE 9 FIX: Enforce capacity cap before inserting.
        // Previously the VecDeque could grow without bound in active markets,
        // making each heartbeat O(n) on a potentially huge collection.
        if self.ticks.len() >= MAX_WINDOW_TICKS {
            self.ticks.pop_front(); // Drop oldest tick to stay within budget.
        }
        self.ticks.push_back(TradeTick {
            taker_side,
            size,
            ts,
        });
    }

    fn evict_expired(&mut self, now: Instant, window: Duration) {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while let Some(front) = self.ticks.front() {
            if front.ts < cutoff {
                self.ticks.pop_front();
            } else {
                break;
            }
        }
    }

    fn compute(&self) -> SideOfi {
        let mut buy_volume = 0.0_f64;
        let mut sell_volume = 0.0_f64;

        for tick in &self.ticks {
            match tick.taker_side {
                TakerSide::Buy => buy_volume += tick.size,
                TakerSide::Sell => sell_volume += tick.size,
            }
        }

        let ofi_score = buy_volume - sell_volume;

        SideOfi {
            ofi_score,
            buy_volume,
            sell_volume,
            ..SideOfi::default()
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveRegime {
    baseline: f64,
    enter_score: f64,
    exit_score: f64,
    enter_abs: f64,
    exit_abs: f64,
    saturated: bool,
}

#[derive(Debug, Clone, Copy)]
struct ToxicityCandidate {
    harmful_direction: TradeDirection,
    reference_mid_yes: f64,
    adverse_heartbeats: u8,
}

#[derive(Debug, Clone, Copy, Default)]
struct FlowSideState {
    hot: bool,
    hot_since: Option<Instant>,
    heat_enter_heartbeats: u8,
    heat_exit_heartbeats: u8,
    heat_enter_candidate_since: Option<Instant>,
    heat_exit_candidate_since: Option<Instant>,
    heat_reenter_allowed_at: Option<Instant>,
    smoothed_heat_score: f64,
    pending_hot_reference_mid_yes: Option<f64>,
    toxic_direction: Option<TradeDirection>,
    toxic_since: Option<Instant>,
    toxic_reference_mid_yes: Option<f64>,
    candidate: Option<ToxicityCandidate>,
    reenter_allowed_at: Option<Instant>,
    saturated_count: u8,
    saturated_logged: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct FlowSideEval {
    side_ofi: SideOfi,
    ratio: f64,
    baseline: f64,
    norm_score: f64,
    heat_entered: bool,
    heat_recovered: bool,
    toxic_entered: bool,
    toxic_recovered: bool,
    adverse_ticks: f64,
}

fn book_mid(bid: f64, ask: f64) -> Option<f64> {
    if bid > 0.0 && ask > bid {
        Some((bid + ask) * 0.5)
    } else {
        None
    }
}

fn synthetic_mid_yes(yes_bid: f64, yes_ask: f64, no_bid: f64, no_ask: f64) -> Option<f64> {
    match (book_mid(yes_bid, yes_ask), book_mid(no_bid, no_ask)) {
        (Some(yes_mid), Some(no_mid)) => Some(((yes_mid) + (1.0 - no_mid)) * 0.5),
        (Some(yes_mid), None) => Some(yes_mid),
        (None, Some(no_mid)) => Some(1.0 - no_mid),
        (None, None) => None,
    }
}

fn harmful_direction(side: Side, ofi_score: f64) -> Option<TradeDirection> {
    if ofi_score.abs() <= f64::EPSILON {
        return None;
    }
    match side {
        Side::Yes => {
            if ofi_score < 0.0 {
                Some(TradeDirection::Buy)
            } else {
                Some(TradeDirection::Sell)
            }
        }
        Side::No => {
            if ofi_score < 0.0 {
                Some(TradeDirection::Buy)
            } else {
                Some(TradeDirection::Sell)
            }
        }
    }
}

fn adverse_move_ticks(
    side: Side,
    harmful_direction: TradeDirection,
    reference_mid_yes: f64,
    current_mid_yes: f64,
    tick: f64,
) -> f64 {
    let adverse_delta = match (side, harmful_direction) {
        (Side::Yes, TradeDirection::Buy) => reference_mid_yes - current_mid_yes,
        (Side::Yes, TradeDirection::Sell) => current_mid_yes - reference_mid_yes,
        (Side::No, TradeDirection::Buy) => current_mid_yes - reference_mid_yes,
        (Side::No, TradeDirection::Sell) => reference_mid_yes - current_mid_yes,
    };
    (adverse_delta.max(0.0)) / tick.max(1e-9)
}

fn evaluate_flow_side(
    cfg: &OfiConfig,
    side: Side,
    mut side_ofi: SideOfi,
    baseline: f64,
    enter_abs: f64,
    exit_abs: f64,
    saturated: bool,
    reference_mid_yes: f64,
    now: Instant,
    heartbeat_fired: bool,
    state: &mut FlowSideState,
) -> FlowSideEval {
    let abs = side_ofi.ofi_score.abs();
    let total = (side_ofi.buy_volume + side_ofi.sell_volume).max(1e-9);
    let ratio = abs / total;
    let norm_score = abs / baseline.max(1.0);
    state.smoothed_heat_score = if state.smoothed_heat_score <= f64::EPSILON {
        norm_score
    } else {
        state.smoothed_heat_score + HEAT_SCORE_EMA_ALPHA * (norm_score - state.smoothed_heat_score)
    };
    let ratio_gate = cfg.toxicity_ratio_enter > 0.0;
    let heat_enter_ratio_ok = !ratio_gate || ratio >= cfg.toxicity_ratio_enter;
    let heat_exit_ratio_ok = !ratio_gate || ratio <= cfg.toxicity_ratio_exit;
    let adaptive_heat_enter_ok = state.smoothed_heat_score >= cfg.toxicity_ratio_enter.max(0.0)
        && state.smoothed_heat_score >= 1.2
        && state.smoothed_heat_score >= (enter_abs / baseline.max(1.0)).max(1.0)
        && heat_enter_ratio_ok;
    let adaptive_heat_exit_ok =
        state.smoothed_heat_score <= (exit_abs / baseline.max(1.0)).max(0.5) && heat_exit_ratio_ok;
    let legacy_heat_enter_ok = abs > enter_abs && heat_enter_ratio_ok;
    let legacy_heat_exit_ok = abs <= exit_abs && heat_exit_ratio_ok;
    let heat_enter_ok = if cfg.adaptive_threshold {
        adaptive_heat_enter_ok
    } else {
        legacy_heat_enter_ok
    };
    let heat_exit_ok = if cfg.adaptive_threshold {
        adaptive_heat_exit_ok
    } else {
        legacy_heat_exit_ok
    };
    if !state.hot {
        if heat_enter_ok {
            if cfg.adaptive_threshold {
                // In adaptive mode we may dwell longer before entering heat;
                // keep candidate anchor near the latest observed mid.
                state.pending_hot_reference_mid_yes = Some(reference_mid_yes);
            } else {
                // Legacy heartbeat mode keeps the first anchor of the burst.
                state
                    .pending_hot_reference_mid_yes
                    .get_or_insert(reference_mid_yes);
            }
        } else {
            state.pending_hot_reference_mid_yes = None;
        }
    }
    let mut heat_entered = false;
    let mut heat_recovered = false;
    if heartbeat_fired {
        if cfg.adaptive_threshold {
            if state.hot {
                state.heat_enter_candidate_since = None;
                let hot_held_long_enough = state
                    .hot_since
                    .map(|since| {
                        now.saturating_duration_since(since).as_millis() >= HEAT_MIN_HOLD_MS as u128
                    })
                    .unwrap_or(true);
                if hot_held_long_enough && heat_exit_ok {
                    if state.heat_exit_candidate_since.is_none() {
                        state.heat_exit_candidate_since = Some(now);
                    }
                    let can_exit = state
                        .heat_exit_candidate_since
                        .map(|since| {
                            now.saturating_duration_since(since).as_millis()
                                >= HEAT_ADAPTIVE_EXIT_DWELL_MS as u128
                        })
                        .unwrap_or(false);
                    if can_exit {
                        state.hot = false;
                        state.hot_since = None;
                        state.heat_exit_candidate_since = None;
                        state.pending_hot_reference_mid_yes = None;
                        state.heat_reenter_allowed_at =
                            Some(now + Duration::from_millis(HEAT_REENTER_COOLDOWN_MS));
                        heat_recovered = true;
                    }
                } else {
                    state.heat_exit_candidate_since = None;
                }
                state.heat_enter_heartbeats = 0;
                state.heat_exit_heartbeats = 0;
            } else {
                state.heat_exit_candidate_since = None;
                let cooldown_active = state
                    .heat_reenter_allowed_at
                    .map(|deadline| now < deadline)
                    .unwrap_or(false);
                if !cooldown_active && heat_enter_ok {
                    if state.heat_enter_candidate_since.is_none() {
                        state.heat_enter_candidate_since = Some(now);
                    }
                    let can_enter = state
                        .heat_enter_candidate_since
                        .map(|since| {
                            now.saturating_duration_since(since).as_millis()
                                >= HEAT_ADAPTIVE_ENTER_DWELL_MS as u128
                        })
                        .unwrap_or(false);
                    if can_enter {
                        state.hot = true;
                        state.hot_since = Some(now);
                        state.heat_enter_candidate_since = None;
                        heat_entered = true;
                    }
                } else {
                    state.heat_enter_candidate_since = None;
                    if !heat_enter_ok {
                        state.pending_hot_reference_mid_yes = None;
                    }
                }
                state.heat_enter_heartbeats = 0;
                state.heat_exit_heartbeats = 0;
            }
        } else if state.hot {
            let hot_held_long_enough = state
                .hot_since
                .map(|since| {
                    now.saturating_duration_since(since).as_millis() >= HEAT_MIN_HOLD_MS as u128
                })
                .unwrap_or(true);
            if hot_held_long_enough && heat_exit_ok {
                state.heat_exit_heartbeats = state
                    .heat_exit_heartbeats
                    .saturating_add(1)
                    .min(HEAT_EXIT_CONFIRM_HEARTBEATS);
            } else {
                state.heat_exit_heartbeats = 0;
            }
            state.heat_enter_heartbeats = 0;
            state.pending_hot_reference_mid_yes = None;
            if state.heat_exit_heartbeats >= HEAT_EXIT_CONFIRM_HEARTBEATS {
                state.hot = false;
                state.hot_since = None;
                state.heat_exit_heartbeats = 0;
                state.pending_hot_reference_mid_yes = None;
                heat_recovered = true;
            }
        } else {
            if heat_enter_ok {
                state.heat_enter_heartbeats = state
                    .heat_enter_heartbeats
                    .saturating_add(1)
                    .min(HEAT_ENTER_CONFIRM_HEARTBEATS);
            } else {
                state.heat_enter_heartbeats = 0;
                state.pending_hot_reference_mid_yes = None;
            }
            state.heat_exit_heartbeats = 0;
            if state.heat_enter_heartbeats >= HEAT_ENTER_CONFIRM_HEARTBEATS {
                state.hot = true;
                state.hot_since = Some(now);
                state.heat_enter_heartbeats = 0;
                heat_entered = true;
            }
        }
    }
    let is_hot = state.hot;

    let tick = cfg.tick_size.max(1e-9);
    let current_harmful = if is_hot {
        harmful_direction(side, side_ofi.ofi_score)
    } else {
        None
    };

    let mut adverse_ticks = 0.0;
    let mut toxic_entered = false;
    let mut toxic_recovered = false;

    if let Some(toxic_direction) = state.toxic_direction {
        let reference_mid = state.toxic_reference_mid_yes.unwrap_or(reference_mid_yes);
        adverse_ticks = adverse_move_ticks(
            side,
            toxic_direction,
            reference_mid,
            reference_mid_yes,
            tick,
        );
        let still_adverse = is_hot
            && current_harmful == Some(toxic_direction)
            && adverse_ticks >= TOXIC_PERSIST_TICKS;
        if !still_adverse {
            state.toxic_direction = None;
            state.toxic_since = None;
            state.toxic_reference_mid_yes = None;
            state.candidate = None;
            state.reenter_allowed_at = Some(now + Duration::from_millis(TOXIC_REENTER_COOLDOWN_MS));
            toxic_recovered = true;
        }
    }

    if state.toxic_direction.is_none() {
        if let Some(harmful_direction) = current_harmful {
            let cooldown_active = state
                .reenter_allowed_at
                .map(|deadline| now < deadline)
                .unwrap_or(false);
            if !cooldown_active {
                let pending_hot_reference_mid_yes = if heat_entered {
                    state.pending_hot_reference_mid_yes.take()
                } else {
                    None
                };
                let mut candidate = match state.candidate {
                    Some(existing) if existing.harmful_direction == harmful_direction => existing,
                    _ => ToxicityCandidate {
                        harmful_direction,
                        reference_mid_yes: pending_hot_reference_mid_yes
                            .unwrap_or(reference_mid_yes),
                        adverse_heartbeats: 0,
                    },
                };
                adverse_ticks = adverse_move_ticks(
                    side,
                    harmful_direction,
                    candidate.reference_mid_yes,
                    reference_mid_yes,
                    tick,
                );
                if heartbeat_fired {
                    if adverse_ticks >= TOXIC_PERSIST_TICKS {
                        candidate.adverse_heartbeats = candidate
                            .adverse_heartbeats
                            .saturating_add(1)
                            .min(TOXIC_CONFIRM_HEARTBEATS);
                    } else {
                        candidate.adverse_heartbeats = 0;
                    }
                }
                if adverse_ticks >= TOXIC_CONFIRM_MOVE_TICKS
                    || candidate.adverse_heartbeats >= TOXIC_CONFIRM_HEARTBEATS
                {
                    state.toxic_direction = Some(harmful_direction);
                    state.toxic_since = Some(now);
                    state.toxic_reference_mid_yes = Some(candidate.reference_mid_yes);
                    state.candidate = None;
                    toxic_entered = true;
                } else {
                    state.candidate = Some(candidate);
                }
            } else {
                state.candidate = None;
            }
        } else {
            state.candidate = None;
        }
    }

    side_ofi.heat_score = state.smoothed_heat_score;
    side_ofi.is_hot = is_hot;
    side_ofi.saturated = saturated;
    side_ofi.toxic_buy = state.toxic_direction == Some(TradeDirection::Buy);
    side_ofi.toxic_sell = state.toxic_direction == Some(TradeDirection::Sell);
    side_ofi.is_toxic = side_ofi.toxic_buy || side_ofi.toxic_sell;

    FlowSideEval {
        side_ofi,
        ratio,
        baseline,
        norm_score,
        heat_entered,
        heat_recovered,
        toxic_entered,
        toxic_recovered,
        adverse_ticks,
    }
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

/// OFI Engine: tracks order flow imbalance separately for YES and NO tokens.
pub struct OfiEngine {
    cfg: OfiConfig,
    yes_window: SideWindow,
    no_window: SideWindow,
    md_rx: mpsc::Receiver<MarketDataMsg>,
    snapshot_tx: watch::Sender<OfiSnapshot>,
    /// Opt-2: Per-side rolling history of |ofi_score| observations.
    yes_score_history: VecDeque<f64>,
    no_score_history: VecDeque<f64>,
    /// Per-side current adaptive thresholds.
    yes_threshold: f64,
    no_threshold: f64,
    yes_exit_threshold: f64,
    no_exit_threshold: f64,
    /// Reused sort buffers for quantile estimation; keeps the adaptive path allocation-free.
    yes_quantile_scratch: Vec<f64>,
    no_quantile_scratch: Vec<f64>,
    yes_regime_baseline: f64,
    no_regime_baseline: f64,
    yes_enter_norm_score: f64,
    no_enter_norm_score: f64,
    yes_exit_norm_score: f64,
    no_exit_norm_score: f64,
    yes_threshold_saturated: bool,
    no_threshold_saturated: bool,
    reference_mid_yes: f64,
    /// Opt-4: High-priority kill channel to the Coordinator (edge-triggered on toxicity onset).
    /// None when running without kill channel wiring (backward-compatible).
    kill_tx: Option<mpsc::Sender<KillSwitchSignal>>,
}

impl OfiEngine {
    pub fn new(
        cfg: OfiConfig,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        snapshot_tx: watch::Sender<OfiSnapshot>,
    ) -> Self {
        let initial_threshold = cfg.toxicity_threshold;
        let initial_exit_threshold = (initial_threshold * cfg.toxicity_exit_ratio)
            .max(cfg.adaptive_min.min(initial_threshold));
        let initial_baseline = if cfg.adaptive_max > 0.0 {
            initial_threshold
                .max(cfg.adaptive_min)
                .min(cfg.adaptive_max.max(cfg.adaptive_min))
        } else {
            initial_threshold.max(cfg.adaptive_min)
        };
        let adaptive_window = cfg.adaptive_window;
        Self {
            cfg,
            yes_window: SideWindow::new(),
            no_window: SideWindow::new(),
            md_rx,
            snapshot_tx,
            yes_score_history: VecDeque::new(),
            no_score_history: VecDeque::new(),
            yes_threshold: initial_threshold,
            no_threshold: initial_threshold,
            yes_exit_threshold: initial_exit_threshold,
            no_exit_threshold: initial_exit_threshold,
            yes_quantile_scratch: Vec::with_capacity(adaptive_window),
            no_quantile_scratch: Vec::with_capacity(adaptive_window),
            yes_regime_baseline: initial_baseline,
            no_regime_baseline: initial_baseline,
            yes_enter_norm_score: NORMALIZED_ENTER_MIN.max(1.0),
            no_enter_norm_score: NORMALIZED_ENTER_MIN.max(1.0),
            yes_exit_norm_score: 1.0,
            no_exit_norm_score: 1.0,
            yes_threshold_saturated: false,
            no_threshold_saturated: false,
            reference_mid_yes: 0.5,
            kill_tx: None,
        }
    }

    /// Opt-4: Wire a direct kill channel to the Coordinator.
    /// When toxicity is first detected (edge-triggered), sends a KillSwitchSignal
    /// so the Coordinator can act immediately without waiting for the next book tick.
    pub fn with_kill_tx(mut self, kill_tx: mpsc::Sender<KillSwitchSignal>) -> Self {
        self.kill_tx = Some(kill_tx);
        self
    }

    /// Opt-2: Update per-side adaptive thresholds from per-side histories.
    fn update_adaptive_thresholds(&mut self, yes_score: f64, no_score: f64) {
        if !self.cfg.adaptive_threshold {
            return;
        }
        let yes_regime = Self::compute_adaptive_thresholds(
            &self.cfg,
            &mut self.yes_score_history,
            &mut self.yes_quantile_scratch,
            yes_score,
        );
        self.yes_threshold = yes_regime.enter_abs;
        self.yes_exit_threshold = yes_regime.exit_abs;
        self.yes_regime_baseline = yes_regime.baseline;
        self.yes_enter_norm_score = yes_regime.enter_score;
        self.yes_exit_norm_score = yes_regime.exit_score;
        self.yes_threshold_saturated = yes_regime.saturated;

        let no_regime = Self::compute_adaptive_thresholds(
            &self.cfg,
            &mut self.no_score_history,
            &mut self.no_quantile_scratch,
            no_score,
        );
        self.no_threshold = no_regime.enter_abs;
        self.no_exit_threshold = no_regime.exit_abs;
        self.no_regime_baseline = no_regime.baseline;
        self.no_enter_norm_score = no_regime.enter_score;
        self.no_exit_norm_score = no_regime.exit_score;
        self.no_threshold_saturated = no_regime.saturated;
    }

    fn compute_adaptive_thresholds(
        cfg: &OfiConfig,
        history: &mut VecDeque<f64>,
        scratch: &mut Vec<f64>,
        score: f64,
    ) -> AdaptiveRegime {
        let obs = score.abs();
        if history.len() >= cfg.adaptive_window {
            history.pop_front();
        }
        history.push_back(obs);

        let bootstrap_enter = cfg.toxicity_threshold.max(1.0);
        let hard_cap = if cfg.adaptive_max > 0.0 {
            Some(cfg.adaptive_max.max(cfg.adaptive_min).max(1.0))
        } else {
            None
        };
        let mut bootstrap_baseline = bootstrap_enter.max(cfg.adaptive_min).max(1.0);
        if let Some(cap) = hard_cap {
            bootstrap_baseline = bootstrap_baseline.min(cap);
        }
        let bootstrap_enter_score = (bootstrap_enter / bootstrap_baseline)
            .clamp(NORMALIZED_ENTER_MIN, NORMALIZED_ENTER_MAX);
        let bootstrap_exit_score =
            (bootstrap_enter_score * cfg.toxicity_exit_ratio).clamp(1.0, bootstrap_enter_score);
        let bootstrap_enter_abs = bootstrap_baseline * bootstrap_enter_score;
        let bootstrap_exit_abs = bootstrap_baseline * bootstrap_exit_score;
        let bootstrap_saturated = hard_cap
            .map(|cap| bootstrap_baseline >= cap - 1e-9)
            .unwrap_or(false);
        if history.len() < 10 {
            return AdaptiveRegime {
                baseline: bootstrap_baseline,
                enter_score: bootstrap_enter_score,
                exit_score: bootstrap_exit_score,
                enter_abs: bootstrap_enter_abs,
                exit_abs: bootstrap_exit_abs,
                saturated: bootstrap_saturated,
            };
        }
        let q_base = Self::history_quantile(history, scratch, BASELINE_TAIL_QUANTILE);
        let q_enter = Self::history_quantile(history, scratch, ENTER_TAIL_QUANTILE).max(1.0);
        let q_exit = Self::history_quantile(history, scratch, EXIT_TAIL_QUANTILE).max(1.0);

        let mut baseline = q_base.max(cfg.adaptive_min).max(1.0);
        let mut saturated = false;
        if let Some(cap) = hard_cap {
            // In adaptive mode, let the effective cap expand with current hot regime
            // instead of pinning the threshold at a stale static maximum.
            let effective_cap = cap.max(q_enter.max(q_base));
            baseline = baseline.min(effective_cap);
            saturated = effective_cap <= cap + 1e-9 && baseline >= cap - 1e-9;
        }

        let enter_score = (q_enter / baseline).clamp(NORMALIZED_ENTER_MIN, NORMALIZED_ENTER_MAX);
        let q_exit_score = (q_exit / baseline).clamp(1.0, enter_score);
        let hyst_exit_score = (enter_score * cfg.toxicity_exit_ratio).clamp(1.0, enter_score);
        let exit_score = q_exit_score.min(hyst_exit_score);

        AdaptiveRegime {
            baseline,
            enter_score,
            exit_score,
            enter_abs: baseline * enter_score,
            exit_abs: baseline * exit_score,
            saturated,
        }
    }

    fn history_quantile(history: &VecDeque<f64>, scratch: &mut Vec<f64>, quantile: f64) -> f64 {
        scratch.clear();
        scratch.extend(history.iter().copied());
        scratch.sort_by(f64::total_cmp);
        let last = scratch.len().saturating_sub(1);
        let idx = ((last as f64) * quantile.clamp(0.0, 1.0)).round() as usize;
        scratch[idx.min(last)]
    }

    /// Actor main loop.
    pub async fn run(mut self) {
        let adaptive_rise_cap_text = if self.cfg.adaptive_rise_cap_pct > 0.0 {
            format!("{:.0}%", self.cfg.adaptive_rise_cap_pct * 100.0)
        } else {
            "off".to_string()
        };
        info!(
            "🔬 OFI Engine started | window={}ms threshold={:.1} heartbeat={}ms adaptive={} mode={}",
            self.cfg.window_duration.as_millis(),
            self.cfg.toxicity_threshold,
            self.cfg.heartbeat_ms,
            self.cfg.adaptive_threshold,
            if self.cfg.adaptive_threshold {
                "tail-quantile"
            } else {
                "static"
            },
        );
        if self.cfg.adaptive_threshold {
            info!(
                "🔬 OFI adaptive mode uses normalized regime score (baseline q{:.0}, enter q{:.0}, exit q{:.0}); adaptive_min/max remain as guard rails",
                BASELINE_TAIL_QUANTILE * 100.0,
                ENTER_TAIL_QUANTILE * 100.0,
                EXIT_TAIL_QUANTILE * 100.0
            );
        } else {
            info!(
                "🔬 OFI static mode active | rise_cap={}",
                adaptive_rise_cap_text
            );
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(self.cfg.heartbeat_ms));
        // Align the first heartbeat to `heartbeat_ms` in the future instead of
        // consuming Tokio interval's immediate initial tick. This keeps the
        // "N-heartbeat confirmation" semantic literal.
        ticker.tick().await;
        let mut yes_state = FlowSideState::default();
        let mut no_state = FlowSideState::default();

        loop {
            let mut heartbeat_fired = false;
            tokio::select! {
                msg = self.md_rx.recv() => {
                    match msg {
                        Some(MarketDataMsg::TradeTick { market_side, taker_side, size, ts, .. }) => {
                            match market_side {
                                Side::Yes => self.yes_window.push(taker_side, size, ts),
                                Side::No => self.no_window.push(taker_side, size, ts),
                            }
                        }
                        Some(MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, .. }) => {
                            if let Some(reference_mid_yes) =
                                synthetic_mid_yes(yes_bid, yes_ask, no_bid, no_ask)
                            {
                                self.reference_mid_yes = reference_mid_yes;
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = ticker.tick() => {
                    heartbeat_fired = true;
                    // Force time-based eviction and broadcast
                }
            }

            let now = Instant::now();

            // Evict expired ticks from both windows using real-time clock
            self.yes_window.evict_expired(now, self.cfg.window_duration);
            self.no_window.evict_expired(now, self.cfg.window_duration);

            // Compute per-side snapshots using current per-side thresholds.
            let yes_ofi = self.yes_window.compute();
            let no_ofi = self.no_window.compute();

            let yes_baseline = if self.cfg.adaptive_threshold {
                self.yes_regime_baseline.max(1.0)
            } else {
                1.0
            };
            let no_baseline = if self.cfg.adaptive_threshold {
                self.no_regime_baseline.max(1.0)
            } else {
                1.0
            };
            let yes_enter_score = if self.cfg.adaptive_threshold {
                self.yes_enter_norm_score.max(1.0)
            } else {
                self.yes_threshold.max(1.0)
            };
            let no_enter_score = if self.cfg.adaptive_threshold {
                self.no_enter_norm_score.max(1.0)
            } else {
                self.no_threshold.max(1.0)
            };
            let yes_exit_score = if self.cfg.adaptive_threshold {
                self.yes_exit_norm_score.max(1.0)
            } else {
                self.yes_exit_threshold.max(1.0)
            };
            let no_exit_score = if self.cfg.adaptive_threshold {
                self.no_exit_norm_score.max(1.0)
            } else {
                self.no_exit_threshold.max(1.0)
            };
            let yes_enter_abs = if self.cfg.adaptive_threshold {
                yes_baseline * yes_enter_score
            } else {
                yes_enter_score
            };
            let no_enter_abs = if self.cfg.adaptive_threshold {
                no_baseline * no_enter_score
            } else {
                no_enter_score
            };
            let yes_exit_abs = if self.cfg.adaptive_threshold {
                yes_baseline * yes_exit_score
            } else {
                yes_exit_score
            };
            let no_exit_abs = if self.cfg.adaptive_threshold {
                no_baseline * no_exit_score
            } else {
                no_exit_score
            };
            let yes_saturated = self.cfg.adaptive_threshold && self.yes_threshold_saturated;
            let no_saturated = self.cfg.adaptive_threshold && self.no_threshold_saturated;

            let yes_eval = evaluate_flow_side(
                &self.cfg,
                Side::Yes,
                yes_ofi,
                yes_baseline,
                yes_enter_abs,
                yes_exit_abs,
                yes_saturated,
                self.reference_mid_yes,
                now,
                heartbeat_fired,
                &mut yes_state,
            );
            let no_eval = evaluate_flow_side(
                &self.cfg,
                Side::No,
                no_ofi,
                no_baseline,
                no_enter_abs,
                no_exit_abs,
                no_saturated,
                self.reference_mid_yes,
                now,
                heartbeat_fired,
                &mut no_state,
            );

            let snapshot = OfiSnapshot {
                yes: yes_eval.side_ofi,
                no: no_eval.side_ofi,
                reference_mid_yes: self.reference_mid_yes,
                ts: now,
            };

            let _ = self.snapshot_tx.send(snapshot);

            if heartbeat_fired {
                if yes_saturated {
                    yes_state.saturated_count = yes_state.saturated_count.saturating_add(1);
                    if yes_state.saturated_count >= SATURATION_LOG_HEARTBEATS
                        && !yes_state.saturated_logged
                    {
                        warn!(
                            "⚠️ OFI threshold saturated at max (YES): raw_ofi={:.1} baseline={:.1} normalized_score={:.2} adaptive_max={:.1}",
                            yes_eval.side_ofi.ofi_score,
                            yes_eval.baseline,
                            yes_eval.norm_score,
                            self.cfg.adaptive_max
                        );
                        yes_state.saturated_logged = true;
                    }
                } else {
                    yes_state.saturated_count = 0;
                    yes_state.saturated_logged = false;
                }

                if no_saturated {
                    no_state.saturated_count = no_state.saturated_count.saturating_add(1);
                    if no_state.saturated_count >= SATURATION_LOG_HEARTBEATS
                        && !no_state.saturated_logged
                    {
                        warn!(
                            "⚠️ OFI threshold saturated at max (NO): raw_ofi={:.1} baseline={:.1} normalized_score={:.2} adaptive_max={:.1}",
                            no_eval.side_ofi.ofi_score,
                            no_eval.baseline,
                            no_eval.norm_score,
                            self.cfg.adaptive_max
                        );
                        no_state.saturated_logged = true;
                    }
                } else {
                    no_state.saturated_count = 0;
                    no_state.saturated_logged = false;
                }
            }

            if yes_eval.heat_entered {
                info!(
                    "🔥 YES heat enter | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} enter_abs={:.1} exit_abs={:.1} saturated={}",
                    yes_eval.side_ofi.ofi_score,
                    yes_eval.baseline,
                    yes_eval.side_ofi.heat_score,
                    yes_eval.norm_score,
                    yes_eval.ratio,
                    yes_enter_abs,
                    yes_exit_abs,
                    yes_saturated,
                );
            } else if yes_eval.heat_recovered {
                info!(
                    "🧊 YES heat cool | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} exit_abs={:.1} saturated={}",
                    yes_eval.side_ofi.ofi_score,
                    yes_eval.baseline,
                    yes_eval.side_ofi.heat_score,
                    yes_eval.norm_score,
                    yes_eval.ratio,
                    yes_exit_abs,
                    yes_saturated,
                );
            }

            if yes_eval.toxic_entered {
                warn!(
                    "☠️ YES toxicity confirmed | kill reason=adverse_selection harmful_slot={} raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} ref_mid={:.3} current_mid={:.3} adverse_ticks={:.1} enter_abs={:.1} exit_abs={:.1} saturated={}",
                    if yes_eval.side_ofi.toxic_buy { "YES_BUY" } else { "YES_SELL" },
                    yes_eval.side_ofi.ofi_score,
                    yes_eval.baseline,
                    yes_eval.side_ofi.heat_score,
                    yes_eval.norm_score,
                    yes_eval.ratio,
                    yes_state
                        .toxic_reference_mid_yes
                        .unwrap_or(self.reference_mid_yes),
                    self.reference_mid_yes,
                    yes_eval.adverse_ticks,
                    yes_enter_abs,
                    yes_exit_abs,
                    yes_saturated,
                );
                if let Some(ref tx) = self.kill_tx {
                    let _ = tx.try_send(KillSwitchSignal {
                        side: Side::Yes,
                        ofi_score: yes_eval.side_ofi.ofi_score,
                        ts: now,
                    });
                }
            } else if yes_eval.toxic_recovered {
                info!(
                    "✅ YES toxicity recovered | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} adverse_ticks={:.1} saturated={} reenter_cooldown_ms={}",
                    yes_eval.side_ofi.ofi_score,
                    yes_eval.baseline,
                    yes_eval.side_ofi.heat_score,
                    yes_eval.norm_score,
                    yes_eval.ratio,
                    yes_eval.adverse_ticks,
                    yes_saturated,
                    TOXIC_REENTER_COOLDOWN_MS,
                );
            }

            if no_eval.heat_entered {
                info!(
                    "🔥 NO heat enter | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} enter_abs={:.1} exit_abs={:.1} saturated={}",
                    no_eval.side_ofi.ofi_score,
                    no_eval.baseline,
                    no_eval.side_ofi.heat_score,
                    no_eval.norm_score,
                    no_eval.ratio,
                    no_enter_abs,
                    no_exit_abs,
                    no_saturated,
                );
            } else if no_eval.heat_recovered {
                info!(
                    "🧊 NO heat cool | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} exit_abs={:.1} saturated={}",
                    no_eval.side_ofi.ofi_score,
                    no_eval.baseline,
                    no_eval.side_ofi.heat_score,
                    no_eval.norm_score,
                    no_eval.ratio,
                    no_exit_abs,
                    no_saturated,
                );
            }

            if no_eval.toxic_entered {
                warn!(
                    "☠️ NO toxicity confirmed | kill reason=adverse_selection harmful_slot={} raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} ref_mid={:.3} current_mid={:.3} adverse_ticks={:.1} enter_abs={:.1} exit_abs={:.1} saturated={}",
                    if no_eval.side_ofi.toxic_buy { "NO_BUY" } else { "NO_SELL" },
                    no_eval.side_ofi.ofi_score,
                    no_eval.baseline,
                    no_eval.side_ofi.heat_score,
                    no_eval.norm_score,
                    no_eval.ratio,
                    no_state
                        .toxic_reference_mid_yes
                        .unwrap_or(self.reference_mid_yes),
                    self.reference_mid_yes,
                    no_eval.adverse_ticks,
                    no_enter_abs,
                    no_exit_abs,
                    no_saturated,
                );
                if let Some(ref tx) = self.kill_tx {
                    let _ = tx.try_send(KillSwitchSignal {
                        side: Side::No,
                        ofi_score: no_eval.side_ofi.ofi_score,
                        ts: now,
                    });
                }
            } else if no_eval.toxic_recovered {
                info!(
                    "✅ NO toxicity recovered | raw_ofi={:.1} baseline={:.1} heat_score={:.2} norm_score={:.2} ratio={:.2} adverse_ticks={:.1} saturated={} reenter_cooldown_ms={}",
                    no_eval.side_ofi.ofi_score,
                    no_eval.baseline,
                    no_eval.side_ofi.heat_score,
                    no_eval.norm_score,
                    no_eval.ratio,
                    no_eval.adverse_ticks,
                    no_saturated,
                    TOXIC_REENTER_COOLDOWN_MS,
                );
            }

            // Update adaptive thresholds only on heartbeat boundaries.
            // This keeps sampling frequency stable regardless of market-data burst rate.
            if heartbeat_fired {
                // Update thresholds for the next decision cycle.
                // This avoids using a threshold recalculated from the same cycle to
                // decide entry/exit in the current cycle.
                self.update_adaptive_thresholds(
                    yes_eval.side_ofi.ofi_score,
                    no_eval.side_ofi.ofi_score,
                );
            }
        }

        info!("🔬 OFI Engine shutting down");
    }
}

// ─────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn make_engine() -> OfiEngine {
        let cfg = OfiConfig {
            window_duration: Duration::from_secs(3),
            toxicity_threshold: 10.0,
            heartbeat_ms: 200,
            ..OfiConfig::default()
        };
        let (_tx, rx) = mpsc::channel(16);
        let (snap_tx, _snap_rx) = watch::channel(OfiSnapshot::default());
        OfiEngine::new(cfg, rx, snap_tx)
    }

    #[test]
    fn test_adaptive_threshold_uses_tail_quantile_without_legacy_cap() {
        let cfg = OfiConfig {
            adaptive_threshold: true,
            adaptive_min: 50.0,
            adaptive_max: 1800.0, // remains guard rail on regime baseline
            adaptive_window: 20,
            ..OfiConfig::default()
        };

        let mut history = VecDeque::new();
        let mut scratch = Vec::new();
        let mut regime = AdaptiveRegime {
            baseline: cfg.toxicity_threshold,
            enter_score: 1.0,
            exit_score: 1.0,
            enter_abs: cfg.toxicity_threshold,
            exit_abs: cfg.toxicity_threshold,
            saturated: false,
        };
        for _ in 0..30 {
            regime =
                OfiEngine::compute_adaptive_thresholds(&cfg, &mut history, &mut scratch, 5_000.0);
        }

        assert!(regime.baseline > 1800.0);
        assert!(!regime.saturated);
        assert!(regime.enter_score >= NORMALIZED_ENTER_MIN);
        assert!(regime.exit_score <= regime.enter_score);
    }

    #[test]
    fn test_adaptive_threshold_falls_back_to_min_floor_in_quiet_regime() {
        let cfg = OfiConfig {
            adaptive_threshold: true,
            adaptive_min: 120.0,
            adaptive_window: 32,
            ..OfiConfig::default()
        };
        let mut history = VecDeque::new();
        let mut scratch = Vec::new();
        history.extend(std::iter::repeat(18.0).take(32));

        let regime = OfiEngine::compute_adaptive_thresholds(&cfg, &mut history, &mut scratch, 20.0);
        assert!((regime.baseline - 120.0).abs() < 1e-9);
        assert!(regime.enter_score >= 1.0);
        assert!(regime.exit_score >= 1.0);
        assert!(regime.enter_abs >= regime.baseline);
        assert!(regime.exit_abs <= regime.enter_abs + 1e-9);
    }

    #[test]
    fn test_per_side_tracking() {
        let mut engine = make_engine();
        let now = Instant::now();

        // YES side: heavy buying
        engine.yes_window.push(TakerSide::Buy, 15.0, now);
        engine.yes_window.push(TakerSide::Sell, 2.0, now);

        // NO side: balanced
        engine.no_window.push(TakerSide::Buy, 5.0, now);
        engine.no_window.push(TakerSide::Sell, 4.0, now);

        let yes_ofi = engine.yes_window.compute();
        let no_ofi = engine.no_window.compute();

        assert!((yes_ofi.ofi_score - 13.0).abs() < 1e-9);
        assert!((no_ofi.ofi_score - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_sell_pressure_toxic() {
        let mut engine = make_engine();
        let now = Instant::now();

        // NO side: heavy selling (panic dump)
        engine.no_window.push(TakerSide::Sell, 20.0, now);
        engine.no_window.push(TakerSide::Buy, 3.0, now);

        let no_ofi = engine.no_window.compute();
        assert!((no_ofi.ofi_score - (-17.0)).abs() < 1e-9);
    }

    #[test]
    fn test_window_eviction_per_side() {
        let mut engine = make_engine();
        let t0 = Instant::now();

        // Old tick on YES
        engine.yes_window.push(TakerSide::Buy, 100.0, t0);

        // 4 seconds later
        let t1 = t0 + Duration::from_secs(4);
        engine.yes_window.push(TakerSide::Sell, 1.0, t1);
        engine.yes_window.evict_expired(t1, Duration::from_secs(3));

        let yes_ofi = engine.yes_window.compute();
        // Old buy(100) evicted
        assert!((yes_ofi.ofi_score - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn test_empty_windows() {
        let engine = make_engine();
        let yes_ofi = engine.yes_window.compute();
        let no_ofi = engine.no_window.compute();

        assert!((yes_ofi.ofi_score - 0.0).abs() < 1e-9);
        assert!((no_ofi.ofi_score - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_independent_toxicity() {
        let mut engine = make_engine();
        let now = Instant::now();

        // YES: toxic (heavy sell = dumping)
        engine.yes_window.push(TakerSide::Sell, 50.0, now);

        // NO: safe
        engine.no_window.push(TakerSide::Buy, 3.0, now);
        engine.no_window.push(TakerSide::Sell, 2.0, now);

        let yes_ofi = engine.yes_window.compute();
        let no_ofi = engine.no_window.compute();

        // YES is toxic (someone dumping YES), NO is safe
        // This means: it's DANGEROUS to buy YES, but SAFE to buy NO
        assert!(yes_ofi.ofi_score < 0.0);
        assert!(no_ofi.ofi_score > -10.0);
    }

    #[tokio::test]
    async fn test_ratio_gate_avoids_high_volume_balanced_false_toxic() {
        let cfg = OfiConfig {
            window_duration: Duration::from_secs(1),
            toxicity_threshold: 10.0,
            heartbeat_ms: 10,
            adaptive_threshold: false,
            toxicity_ratio_enter: 0.5,
            toxicity_ratio_exit: 0.3,
            min_toxic_ms: 0,
            ..OfiConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let (snap_tx, snap_rx) = watch::channel(OfiSnapshot::default());
        let engine = OfiEngine::new(cfg, rx, snap_tx);
        let handle = tokio::spawn(engine.run());

        let now = Instant::now();
        // abs OFI = 40 (>10), but ratio = 40 / (100 + 60) = 0.25 < 0.5 => should NOT be toxic.
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Buy,
                size: 100.0,
                ts: now,
                price: 0.5,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Sell,
                size: 60.0,
                ts: now,
                price: 0.5,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        let snap = *snap_rx.borrow();
        assert!((snap.yes.ofi_score - 40.0).abs() < 1e-9);
        assert!(!snap.yes.is_toxic);

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_toxicity_requires_three_heartbeats_to_confirm() {
        let cfg = OfiConfig {
            window_duration: Duration::from_secs(1),
            toxicity_threshold: 10.0,
            heartbeat_ms: 20,
            min_toxic_ms: 0,
            ..OfiConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let (snap_tx, snap_rx) = watch::channel(OfiSnapshot::default());
        let engine = OfiEngine::new(cfg, rx, snap_tx);
        let handle = tokio::spawn(engine.run());

        let now = Instant::now();
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.49,
                yes_ask: 0.51,
                no_bid: 0.49,
                no_ask: 0.51,
                ts: now,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Sell,
                size: 50.0,
                ts: now,
                price: 0.5,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(25)).await;
        let snap1 = *snap_rx.borrow();
        assert!(
            !snap1.yes.is_toxic,
            "heat alone must not confirm toxicity without adverse selection"
        );

        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.46,
                yes_ask: 0.48,
                no_bid: 0.52,
                no_ask: 0.54,
                ts: now + Duration::from_millis(30),
            })
            .await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        let snap3 = *snap_rx.borrow();
        assert!(
            snap3.yes.is_toxic,
            "adverse price move should confirm toxicity"
        );

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_toxicity_pending_confirmation_resets_when_next_heartbeat_recovers() {
        let cfg = OfiConfig {
            window_duration: Duration::from_secs(1),
            toxicity_threshold: 10.0,
            heartbeat_ms: 10,
            min_toxic_ms: 0,
            ..OfiConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let (snap_tx, snap_rx) = watch::channel(OfiSnapshot::default());
        let engine = OfiEngine::new(cfg, rx, snap_tx);
        let handle = tokio::spawn(engine.run());

        let now = Instant::now();
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.49,
                yes_ask: 0.51,
                no_bid: 0.49,
                no_ask: 0.51,
                ts: now,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Sell,
                size: 50.0,
                ts: now,
                price: 0.5,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(!snap_rx.borrow().yes.is_toxic);

        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Buy,
                size: 60.0,
                ts: now + Duration::from_millis(1),
                price: 0.5,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(25)).await;
        let snap = *snap_rx.borrow();
        assert!(
            !snap.yes.is_toxic,
            "recovered heartbeat must reset pending confirmation"
        );

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_toxicity_timeout_recovery() {
        let cfg = OfiConfig {
            window_duration: Duration::from_millis(120),
            toxicity_threshold: 10.0,
            heartbeat_ms: 10,
            min_toxic_ms: 0,
            ..OfiConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let (snap_tx, snap_rx) = watch::channel(OfiSnapshot::default());
        let engine = OfiEngine::new(cfg, rx, snap_tx);

        let handle = tokio::spawn(engine.run());

        let t0 = Instant::now();
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.49,
                yes_ask: 0.51,
                no_bid: 0.49,
                no_ask: 0.51,
                ts: t0,
            })
            .await;
        // Send a toxic dump
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Sell,
                size: 50.0,
                ts: t0,
                price: 0.5,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.46,
                yes_ask: 0.48,
                no_bid: 0.52,
                no_ask: 0.54,
                ts: t0 + Duration::from_millis(1),
            })
            .await;

        // Wait for adverse selection confirmation + scheduling slack.
        tokio::time::sleep(Duration::from_millis(45)).await;
        let snap1 = *snap_rx.borrow(); // Copy the snapshot
        assert!(snap1.yes.is_toxic);
        assert!((snap1.yes.ofi_score - (-50.0)).abs() < 1e-9);

        // Wait for window_duration to pass, toxicity should clear without new ticks (thanks to heartbeat)
        tokio::time::sleep(Duration::from_millis(160)).await;
        let snap2 = *snap_rx.borrow(); // Copy the snapshot
        assert!(!snap2.yes.is_toxic);
        assert!((snap2.yes.ofi_score - 0.0).abs() < 1e-9);

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_toxicity_persists_while_heat_and_adverse_move_remain() {
        let cfg = OfiConfig {
            window_duration: Duration::from_millis(800),
            toxicity_threshold: 100.0,
            heartbeat_ms: 10,
            adaptive_threshold: false,
            toxicity_ratio_enter: 0.0,
            toxicity_ratio_exit: 0.0,
            toxicity_exit_ratio: 0.85,
            min_toxic_ms: 0,
            ..OfiConfig::default()
        };
        let (tx, rx) = mpsc::channel(32);
        let (snap_tx, snap_rx) = watch::channel(OfiSnapshot::default());
        let engine = OfiEngine::new(cfg, rx, snap_tx);
        let handle = tokio::spawn(engine.run());

        let now = Instant::now();
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.49,
                yes_ask: 0.51,
                no_bid: 0.49,
                no_ask: 0.51,
                ts: now,
            })
            .await;
        // Step 1: huge toxic entry.
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Buy,
                size: 5_000.0,
                ts: now,
                price: 0.5,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.54,
                yes_ask: 0.56,
                no_bid: 0.44,
                no_ask: 0.46,
                ts: now + Duration::from_millis(1),
            })
            .await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let snap1 = *snap_rx.borrow();
        assert!(snap1.yes.is_toxic);

        // Step 2: reduce net OFI to ~1_000 while keeping heat and adverse drift alive.
        let _ = tx
            .send(MarketDataMsg::TradeTick {
                asset_id: "".to_string(),
                market_side: Side::Yes,
                taker_side: TakerSide::Sell,
                size: 4_000.0,
                ts: now + Duration::from_millis(1),
                price: 0.5,
            })
            .await;
        let _ = tx
            .send(MarketDataMsg::BookTick {
                yes_bid: 0.55,
                yes_ask: 0.57,
                no_bid: 0.43,
                no_ask: 0.45,
                ts: now + Duration::from_millis(2),
            })
            .await;

        tokio::time::sleep(Duration::from_millis(220)).await;
        let snap2 = *snap_rx.borrow();
        assert!(snap2.yes.ofi_score.abs() >= 900.0);
        assert!(
            snap2.yes.is_toxic,
            "should remain toxic while heat is still high and adverse selection still persists"
        );

        // Step 3: once the OFI window expires, side should recover.
        tokio::time::sleep(Duration::from_millis(900)).await;
        let snap3 = *snap_rx.borrow();
        assert!(!snap3.yes.is_toxic);

        drop(tx);
        let _ = handle.await;
    }
}
