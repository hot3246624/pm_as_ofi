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

use super::messages::{KillSwitchSignal, MarketDataMsg, OfiSnapshot, SideOfi, TakerSide};
use super::types::Side;

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

/// OFI engine configuration. All values configurable at startup.
#[derive(Debug, Clone)]
pub struct OfiConfig {
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
    /// Enable adaptive toxicity threshold (mean + k*σ over rolling history).
    /// When true, `toxicity_threshold` serves only as the initial fallback.
    /// Default: false (static threshold for predictability).
    pub adaptive_threshold: bool,
    /// Number of standard deviations above the rolling mean for toxicity.
    /// Default: 3.0 (flags the top ~0.1% of flow imbalance events).
    pub adaptive_k: f64,
    /// Floor for the adaptive threshold. Prevents false-positives on thin markets.
    /// Default: 50.0.
    pub adaptive_min: f64,
    /// Ceiling for the adaptive threshold. Set to 0 to disable hard upper cap.
    /// Default: 1000.0.
    pub adaptive_max: f64,
    /// Per-heartbeat maximum relative increase for adaptive threshold.
    /// Example: 0.20 means threshold can rise by at most +20% each update.
    /// Set to 0 to disable rise capping.
    /// Default: 0.20.
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

    fn compute(&self, threshold: f64) -> SideOfi {
        let mut buy_volume = 0.0_f64;
        let mut sell_volume = 0.0_f64;

        for tick in &self.ticks {
            match tick.taker_side {
                TakerSide::Buy => buy_volume += tick.size,
                TakerSide::Sell => sell_volume += tick.size,
            }
        }

        let ofi_score = buy_volume - sell_volume;
        let is_toxic = ofi_score.abs() > threshold;

        SideOfi {
            ofi_score,
            buy_volume,
            sell_volume,
            is_toxic,
        }
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
    /// Opt-2: Per-side current effective thresholds.
    yes_threshold: f64,
    no_threshold: f64,
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
        self.yes_threshold = Self::compute_adaptive_threshold(
            &self.cfg,
            &mut self.yes_score_history,
            yes_score,
            self.yes_threshold,
        );
        self.no_threshold = Self::compute_adaptive_threshold(
            &self.cfg,
            &mut self.no_score_history,
            no_score,
            self.no_threshold,
        );
    }

    fn compute_adaptive_threshold(
        cfg: &OfiConfig,
        history: &mut VecDeque<f64>,
        score: f64,
        fallback: f64,
    ) -> f64 {
        let obs = score.abs();
        if history.len() >= cfg.adaptive_window {
            history.pop_front();
        }
        history.push_back(obs);

        let n = history.len();
        if n < 10 {
            return fallback.max(cfg.adaptive_min);
        }
        let mean = history.iter().sum::<f64>() / n as f64;
        let variance = history.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        let std_dev = variance.sqrt();

        let mut adaptive = (mean + cfg.adaptive_k * std_dev).max(cfg.adaptive_min);
        if cfg.adaptive_rise_cap_pct > 0.0 {
            let prev = fallback.max(cfg.adaptive_min);
            let rise_cap = prev * (1.0 + cfg.adaptive_rise_cap_pct);
            adaptive = adaptive.min(rise_cap);
        }
        if cfg.adaptive_max > 0.0 {
            adaptive = adaptive.min(cfg.adaptive_max.max(cfg.adaptive_min));
        }
        adaptive
    }

    /// Actor main loop.
    pub async fn run(mut self) {
        let adaptive_rise_cap_text = if self.cfg.adaptive_rise_cap_pct > 0.0 {
            format!("{:.0}%", self.cfg.adaptive_rise_cap_pct * 100.0)
        } else {
            "off".to_string()
        };
        info!(
            "🔬 OFI Engine started | window={}ms threshold={:.1} heartbeat={}ms adaptive={} rise_cap={}",
            self.cfg.window_duration.as_millis(),
            self.cfg.toxicity_threshold,
            self.cfg.heartbeat_ms,
            self.cfg.adaptive_threshold,
            adaptive_rise_cap_text,
        );

        let mut ticker = tokio::time::interval(Duration::from_millis(self.cfg.heartbeat_ms));
        let mut was_yes_toxic = false;
        let mut was_no_toxic = false;
        let mut yes_toxic_since: Option<Instant> = None;
        let mut no_toxic_since: Option<Instant> = None;
        let mut yes_toxic_entry_threshold: Option<f64> = None;
        let mut no_toxic_entry_threshold: Option<f64> = None;

        loop {
            tokio::select! {
                msg = self.md_rx.recv() => {
                    match msg {
                        Some(MarketDataMsg::TradeTick { market_side, taker_side, size, ts, .. }) => {
                            match market_side {
                                Side::Yes => self.yes_window.push(taker_side, size, ts),
                                Side::No => self.no_window.push(taker_side, size, ts),
                            }
                        }
                        None => break, // Channel closed
                        _ => {}
                    }
                }
                _ = ticker.tick() => {
                    // Force time-based eviction and broadcast
                }
            }

            let now = Instant::now();

            // Evict expired ticks from both windows using real-time clock
            self.yes_window.evict_expired(now, self.cfg.window_duration);
            self.no_window.evict_expired(now, self.cfg.window_duration);

            // Compute per-side snapshots using current per-side thresholds.
            let mut yes_ofi = self.yes_window.compute(self.yes_threshold);
            let mut no_ofi = self.no_window.compute(self.no_threshold);

            // Hysteresis + minimum toxic hold:
            // - Enter toxic at abs(score) > enter_threshold.
            // - Exit toxic only after min_toxic_ms AND abs(score) <= exit_threshold.
            //   exit_threshold is frozen to entry threshold to avoid moving-target recovery.
            let yes_enter_threshold = self.yes_threshold.max(1.0);
            let no_enter_threshold = self.no_threshold.max(1.0);
            let yes_exit_base_threshold = yes_toxic_entry_threshold
                .unwrap_or(yes_enter_threshold)
                .max(1.0);
            let no_exit_base_threshold = no_toxic_entry_threshold
                .unwrap_or(no_enter_threshold)
                .max(1.0);
            let yes_exit_threshold =
                (yes_exit_base_threshold * self.cfg.toxicity_exit_ratio).max(1.0);
            let no_exit_threshold =
                (no_exit_base_threshold * self.cfg.toxicity_exit_ratio).max(1.0);
            let min_toxic_hold = Duration::from_millis(self.cfg.min_toxic_ms);
            let ratio_gate = self.cfg.toxicity_ratio_enter > 0.0;

            let yes_abs = yes_ofi.ofi_score.abs();
            let yes_total = (yes_ofi.buy_volume + yes_ofi.sell_volume).max(1e-9);
            let yes_ratio = yes_abs / yes_total;
            let yes_is_toxic = if was_yes_toxic {
                let held_long_enough = yes_toxic_since
                    .map(|t| now.duration_since(t) >= min_toxic_hold)
                    .unwrap_or(true);
                let ratio_recovered = ratio_gate && yes_ratio <= self.cfg.toxicity_ratio_exit;
                let abs_recovered = yes_abs <= yes_exit_threshold;
                !(held_long_enough && (abs_recovered || ratio_recovered))
            } else {
                let ratio_enter_ok = !ratio_gate || yes_ratio >= self.cfg.toxicity_ratio_enter;
                yes_abs > yes_enter_threshold && ratio_enter_ok
            };

            let no_abs = no_ofi.ofi_score.abs();
            let no_total = (no_ofi.buy_volume + no_ofi.sell_volume).max(1e-9);
            let no_ratio = no_abs / no_total;
            let no_is_toxic = if was_no_toxic {
                let held_long_enough = no_toxic_since
                    .map(|t| now.duration_since(t) >= min_toxic_hold)
                    .unwrap_or(true);
                let ratio_recovered = ratio_gate && no_ratio <= self.cfg.toxicity_ratio_exit;
                let abs_recovered = no_abs <= no_exit_threshold;
                !(held_long_enough && (abs_recovered || ratio_recovered))
            } else {
                let ratio_enter_ok = !ratio_gate || no_ratio >= self.cfg.toxicity_ratio_enter;
                no_abs > no_enter_threshold && ratio_enter_ok
            };

            yes_ofi.is_toxic = yes_is_toxic;
            no_ofi.is_toxic = no_is_toxic;

            let snapshot = OfiSnapshot {
                yes: yes_ofi,
                no: no_ofi,
                ts: now,
            };

            let _ = self.snapshot_tx.send(snapshot);

            // Edge-triggered logging and Opt-4 kill signals on toxicity onset.
            if yes_is_toxic && !was_yes_toxic {
                yes_toxic_since = Some(now);
                yes_toxic_entry_threshold = Some(yes_enter_threshold);
                warn!(
                    "☠️ YES entered toxicity! OFI={:.1} (buy={:.1} sell={:.1} ratio={:.2}) threshold={:.1}",
                    yes_ofi.ofi_score,
                    yes_ofi.buy_volume,
                    yes_ofi.sell_volume,
                    yes_ratio,
                    yes_enter_threshold,
                );
                // Opt-4: Notify coordinator immediately without waiting for next book tick.
                if let Some(ref tx) = self.kill_tx {
                    let _ = tx.try_send(KillSwitchSignal {
                        side: Side::Yes,
                        ofi_score: yes_ofi.ofi_score,
                        ts: now,
                    });
                }
            } else if !yes_is_toxic && was_yes_toxic {
                yes_toxic_since = None;
                let entry_threshold = yes_toxic_entry_threshold.unwrap_or(yes_enter_threshold);
                yes_toxic_entry_threshold = None;
                info!(
                    "✅ YES flow recovered (OFI={:.1}, ratio={:.2}, entry_thr={:.1}, current_thr={:.1})",
                    yes_ofi.ofi_score, yes_ratio, entry_threshold, yes_enter_threshold
                );
            }
            was_yes_toxic = yes_is_toxic;

            if no_is_toxic && !was_no_toxic {
                no_toxic_since = Some(now);
                no_toxic_entry_threshold = Some(no_enter_threshold);
                warn!(
                    "☠️ NO entered toxicity! OFI={:.1} (buy={:.1} sell={:.1} ratio={:.2}) threshold={:.1}",
                    no_ofi.ofi_score,
                    no_ofi.buy_volume,
                    no_ofi.sell_volume,
                    no_ratio,
                    no_enter_threshold,
                );
                // Opt-4: Notify coordinator immediately without waiting for next book tick.
                if let Some(ref tx) = self.kill_tx {
                    let _ = tx.try_send(KillSwitchSignal {
                        side: Side::No,
                        ofi_score: no_ofi.ofi_score,
                        ts: now,
                    });
                }
            } else if !no_is_toxic && was_no_toxic {
                no_toxic_since = None;
                let entry_threshold = no_toxic_entry_threshold.unwrap_or(no_enter_threshold);
                no_toxic_entry_threshold = None;
                info!(
                    "✅ NO flow recovered (OFI={:.1}, ratio={:.2}, entry_thr={:.1}, current_thr={:.1})",
                    no_ofi.ofi_score, no_ratio, entry_threshold, no_enter_threshold
                );
            }
            was_no_toxic = no_is_toxic;

            // Update adaptive thresholds for the next decision cycle.
            // This avoids using a threshold that was recalculated from the same tick
            // to decide entry/exit in the current cycle.
            self.update_adaptive_thresholds(yes_ofi.ofi_score, no_ofi.ofi_score);
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
    fn test_adaptive_threshold_can_exceed_legacy_cap_when_max_disabled() {
        let cfg = OfiConfig {
            adaptive_threshold: true,
            adaptive_k: 1.0,
            adaptive_min: 50.0,
            adaptive_max: 0.0,          // disable hard upper cap
            adaptive_rise_cap_pct: 0.0, // disable per-step cap for this test
            adaptive_window: 20,
            ..OfiConfig::default()
        };

        let mut history = VecDeque::new();
        let mut threshold = cfg.toxicity_threshold;
        for _ in 0..30 {
            threshold =
                OfiEngine::compute_adaptive_threshold(&cfg, &mut history, 2000.0, threshold);
        }

        assert!(threshold > 1000.0);
    }

    #[test]
    fn test_adaptive_threshold_rise_cap_limits_single_step_increase() {
        let cfg = OfiConfig {
            adaptive_threshold: true,
            adaptive_k: 3.0,
            adaptive_min: 50.0,
            adaptive_max: 0.0,
            adaptive_rise_cap_pct: 0.10, // <= +10% per update
            adaptive_window: 64,
            ..OfiConfig::default()
        };
        let mut history = VecDeque::new();
        history.extend(std::iter::repeat(100.0).take(32));

        let threshold = OfiEngine::compute_adaptive_threshold(&cfg, &mut history, 5_000.0, 100.0);
        assert!(threshold <= 110.0 + 1e-9);
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

        let yes_ofi = engine.yes_window.compute(10.0);
        let no_ofi = engine.no_window.compute(10.0);

        assert!(yes_ofi.is_toxic); // |13| > 10
        assert!(!no_ofi.is_toxic); // |1| < 10
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

        let no_ofi = engine.no_window.compute(10.0);
        assert!(no_ofi.is_toxic);
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

        let yes_ofi = engine.yes_window.compute(10.0);
        // Old buy(100) evicted
        assert!((yes_ofi.ofi_score - (-1.0)).abs() < 1e-9);
        assert!(!yes_ofi.is_toxic);
    }

    #[test]
    fn test_empty_windows() {
        let engine = make_engine();
        let yes_ofi = engine.yes_window.compute(10.0);
        let no_ofi = engine.no_window.compute(10.0);

        assert!(!yes_ofi.is_toxic);
        assert!(!no_ofi.is_toxic);
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

        let yes_ofi = engine.yes_window.compute(10.0);
        let no_ofi = engine.no_window.compute(10.0);

        // YES is toxic (someone dumping YES), NO is safe
        // This means: it's DANGEROUS to buy YES, but SAFE to buy NO
        assert!(yes_ofi.is_toxic);
        assert!(!no_ofi.is_toxic);
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
    async fn test_toxicity_timeout_recovery() {
        let cfg = OfiConfig {
            window_duration: Duration::from_millis(50),
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

        // Wait for it to become toxic
        tokio::time::sleep(Duration::from_millis(20)).await;
        let snap1 = *snap_rx.borrow(); // Copy the snapshot
        assert!(snap1.yes.is_toxic);
        assert!((snap1.yes.ofi_score - (-50.0)).abs() < 1e-9);

        // Wait for window_duration to pass, toxicity should clear without new ticks (thanks to heartbeat)
        tokio::time::sleep(Duration::from_millis(100)).await;
        let snap2 = *snap_rx.borrow(); // Copy the snapshot
        assert!(!snap2.yes.is_toxic);
        assert!((snap2.yes.ofi_score - 0.0).abs() < 1e-9);

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_toxic_exit_threshold_is_frozen_to_entry_threshold() {
        let cfg = OfiConfig {
            window_duration: Duration::from_millis(800),
            toxicity_threshold: 100.0,
            heartbeat_ms: 10,
            adaptive_threshold: true,
            adaptive_k: 3.0,
            adaptive_min: 50.0,
            adaptive_max: 0.0,
            adaptive_rise_cap_pct: 0.0, // allow fast threshold jump to stress-test freeze behavior
            adaptive_window: 20,
            toxicity_ratio_enter: 0.0, // disable ratio gate; recovery must rely on abs exit only
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
        tokio::time::sleep(Duration::from_millis(40)).await;
        let snap1 = *snap_rx.borrow();
        assert!(snap1.yes.is_toxic);

        // Step 2: reduce net OFI to ~1_000 while keeping a large adaptive history footprint.
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

        tokio::time::sleep(Duration::from_millis(220)).await;
        let snap2 = *snap_rx.borrow();
        assert!(snap2.yes.ofi_score.abs() >= 900.0);
        assert!(
            snap2.yes.is_toxic,
            "should remain toxic: exit threshold must be frozen to entry threshold"
        );

        // Step 3: once the OFI window expires, side should recover.
        tokio::time::sleep(Duration::from_millis(900)).await;
        let snap3 = *snap_rx.borrow();
        assert!(!snap3.yes.is_toxic);

        drop(tx);
        let _ = handle.await;
    }
}
