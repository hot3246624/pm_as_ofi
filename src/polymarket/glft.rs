use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tracing::{debug, info, warn};

use super::messages::{MarketDataMsg, TakerSide};
use super::types::Side;

const GLFT_MAX_BUCKETS: usize = 21;
const BASIS_HALF_LIFE_SECS: f64 = 30.0;
const BASIS_COLD_BLEND_ALPHA: f64 = 0.35;
const SIGMA_HALF_LIFE_SECS: f64 = 20.0;
const SIGMA_VAR_FLOOR: f64 = 2e-7;
const BINANCE_STALE_SECS: u64 = 3;
const BINANCE_STALE_GRACE_SECS: u64 = 2;
const GLFT_MIN_READY_WAIT_SECS: u64 = 2;
const GLFT_READY_WAIT_CAP_SECS: u64 = 8;
const GLFT_REJECTED_WARM_FALLBACK_WAIT_SECS: u64 = 4;
const GLFT_BASIS_CLAMP_ABS: f64 = 0.08;
const GLFT_BASIS_LIVE_SOFT_CAP_ABS: f64 = 0.18;
const GLFT_BASIS_LIVE_MAX_STEP: f64 = 0.03;
const GLFT_BASIS_SNAPSHOT_MAX_ABS: f64 = 0.20;
const GLFT_WARM_MIN_BINANCE_TICKS: usize = 3;
const GLFT_WARM_MIN_BOOK_TICKS: usize = 3;
const GLFT_WARM_BASIS_DEVIATION_TICKS: f64 = 6.0;
const GLFT_WARM_BASIS_SOFT_ACCEPT_TICKS: f64 = 10.0;
const GLFT_WARM_BASIS_BRIDGE_ACCEPT_TICKS: f64 = 16.0;
const GLFT_WARM_FRESH_SNAPSHOT_MAX_AGE_SECS: u64 = 120;
const GLFT_DRIFT_DAMPED_ENTER_TICKS: f64 = 6.0;
const GLFT_DRIFT_DAMPED_EXIT_TICKS: f64 = 5.0;
const GLFT_DRIFT_FROZEN_ENTER_TICKS: f64 = 10.0;
const GLFT_DRIFT_FROZEN_EXIT_TICKS: f64 = 8.0;
const GLFT_DRIFT_PAUSED_ENTER_TICKS: f64 = 12.0;
const GLFT_DRIFT_PAUSED_EXIT_TICKS: f64 = 10.0;
const GLFT_DRIFT_PAUSED_FORCE_TICKS: f64 = 14.0;
const GLFT_ENDGAME_PAUSE_RELAX_SECS: u64 = 90;
const GLFT_ENDGAME_PAUSE_RELAX_STALE_MAX_SECS: f64 = 2.0;
const GLFT_ENDGAME_PAUSE_RELAX_MAX_DRIFT_TICKS: f64 = 15.0;
const GLFT_DAMPED_DOMINANT_SUPPRESS_TICKS: f64 = 8.0;
const GLFT_DRIFT_DEESCALATE_MIN_DWELL_MS: u64 = 1200;
const GLFT_DRIFT_MODE_SWITCH_MIN_INTERVAL_MS: u64 = 250;
const GLFT_DRIFT_ESCALATE_CONFIRM_SAMPLES: u8 = 3;
const GLFT_DRIFT_DEESCALATE_CONFIRM_SAMPLES: u8 = 3;
const GLFT_PAUSE_RECOVER_MIN_DWELL_MS: u64 = 1500;
const GLFT_PAUSE_RECOVER_HEALTHY_STREAK: u8 = 3;
const GLFT_WARM_SIGMA_RATIO_LIMIT: f64 = 4.0;
const GLFT_WARM_MIN_A: f64 = 0.01;
const GLFT_WARM_MAX_A: f64 = 50.0;
const GLFT_WARM_MIN_K: f64 = 0.01;
const GLFT_WARM_MAX_K: f64 = 50.0;
const GLFT_TRUSTED_MID_PROVISIONAL_CORRIDOR_TICKS: f64 = 15.0;
const GLFT_TRUSTED_MID_LIVE_CORRIDOR_TICKS: f64 = 30.0;
const BOOTSTRAP_A: f64 = 0.20;
const BOOTSTRAP_K: f64 = 0.50;
const BOOTSTRAP_SIGMA: f64 = 0.02;
const BOOTSTRAP_BASIS: f64 = 0.0;
const SNAPSHOT_TTL_SECS: u64 = 6 * 3600;
const SNAPSHOT_SAVE_READY_STREAK_MIN: u8 = 2;
const STARTUP_FAST_REFIT_MIN_INTERVAL_MS: u64 = 500;
const STARTUP_MIN_TRADE_COUNT: usize = 12;
const STARTUP_MIN_BUCKET_COUNT: usize = 2;
const STARTUP_MIN_R2: f64 = 0.45;
const STEADY_MIN_TRADE_COUNT: usize = 20;
const STEADY_MIN_BUCKET_COUNT: usize = 3;
const STEADY_MIN_R2: f64 = 0.60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FitQuality {
    #[default]
    Warm,
    Ready,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlftFitSource {
    Bootstrap,
    WarmStart,
    LastGoodFit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlftSignalState {
    Bootstrapping,
    Assimilating,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DriftMode {
    #[default]
    Normal,
    Damped,
    Frozen,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WarmStartStatus {
    #[default]
    Missing,
    Candidate,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GlftFitStatus {
    #[default]
    Bootstrap,
    Provisional,
    LiveReady,
    Invalid,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GlftReadinessBlockers {
    pub await_binance: bool,
    pub await_poly_book: bool,
    pub await_fit: bool,
    pub basis_unstable: bool,
    pub sigma_unstable: bool,
    pub min_warmup_not_elapsed: bool,
}

impl GlftReadinessBlockers {
    fn is_empty(&self) -> bool {
        !self.await_binance
            && !self.await_poly_book
            && !self.await_fit
            && !self.basis_unstable
            && !self.sigma_unstable
            && !self.min_warmup_not_elapsed
    }

    fn labels(&self) -> Vec<&'static str> {
        let mut labels = Vec::with_capacity(6);
        if self.await_binance {
            labels.push("await_binance");
        }
        if self.await_poly_book {
            labels.push("await_poly_book");
        }
        if self.await_fit {
            labels.push("await_fit");
        }
        if self.basis_unstable {
            labels.push("basis_unstable");
        }
        if self.sigma_unstable {
            labels.push("sigma_unstable");
        }
        if self.min_warmup_not_elapsed {
            labels.push("min_warmup_not_elapsed");
        }
        labels
    }

    fn describe(&self) -> String {
        let labels = self.labels();
        if labels.is_empty() {
            "none".to_string()
        } else {
            labels.join(",")
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GlftSignalSnapshot {
    pub anchor_prob: f64,
    pub basis_prob: f64,
    pub basis_raw: f64,
    pub basis_clamped: f64,
    pub basis_drift_ticks: f64,
    pub modeled_mid: f64,
    pub trusted_mid: f64,
    pub synthetic_mid_yes: f64,
    pub alpha_flow: f64,
    pub sigma_prob: f64,
    pub tau_norm: f64,
    pub tau_secs: f64,
    pub fit_a: f64,
    pub fit_k: f64,
    pub fit_quality: FitQuality,
    pub fit_source: GlftFitSource,
    pub warm_start_status: WarmStartStatus,
    pub fit_status: GlftFitStatus,
    pub readiness_blockers: GlftReadinessBlockers,
    pub ready_elapsed_ms: u64,
    pub signal_state: GlftSignalState,
    pub drift_mode: DriftMode,
    pub hard_basis_unstable: bool,
    pub ready: bool,
    pub stale: bool,
    pub stale_secs: f64,
}

impl Default for GlftSignalSnapshot {
    fn default() -> Self {
        Self {
            anchor_prob: 0.5,
            basis_prob: 0.0,
            basis_raw: 0.0,
            basis_clamped: 0.0,
            basis_drift_ticks: 0.0,
            modeled_mid: 0.5,
            trusted_mid: 0.5,
            synthetic_mid_yes: 0.5,
            alpha_flow: 0.0,
            sigma_prob: BOOTSTRAP_SIGMA,
            tau_norm: 0.0,
            tau_secs: 0.0,
            fit_a: BOOTSTRAP_A,
            fit_k: BOOTSTRAP_K,
            fit_quality: FitQuality::Warm,
            fit_source: GlftFitSource::Bootstrap,
            warm_start_status: WarmStartStatus::Missing,
            fit_status: GlftFitStatus::Bootstrap,
            readiness_blockers: GlftReadinessBlockers::default(),
            ready_elapsed_ms: 0,
            signal_state: GlftSignalState::Bootstrapping,
            drift_mode: DriftMode::Normal,
            hard_basis_unstable: false,
            ready: false,
            stale: true,
            stale_secs: f64::INFINITY,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GlftBootstrapSnapshot {
    pub fit_a: f64,
    pub fit_k: f64,
    pub sigma_prob: f64,
    pub basis_prob: f64,
    pub saved_at: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct IntensityFitSnapshot {
    pub a: f64,
    pub k: f64,
    pub quality: FitQuality,
}

#[derive(Debug, Clone, Copy)]
struct WarmStartCandidate {
    snapshot: GlftBootstrapSnapshot,
    status: WarmStartStatus,
}

#[derive(Debug, Clone, Copy)]
struct ReadinessGate {
    signal_state: GlftSignalState,
    warm_start_status: WarmStartStatus,
    fit_status: GlftFitStatus,
    fit_source: GlftFitSource,
    fit: IntensityFitSnapshot,
    basis_drift_ticks: f64,
    drift_mode: DriftMode,
    hard_basis_unstable: bool,
    blockers: GlftReadinessBlockers,
    ready: bool,
    ready_elapsed_ms: u64,
}

#[derive(Debug, Clone)]
pub struct GlftRuntimeConfig {
    pub symbol: String,
    pub horizon_key: String,
    pub market_end_ts: u64,
    pub total_round_secs: u64,
    pub tick_size: f64,
    pub intensity_window: Duration,
    pub refit_interval: Duration,
}

impl GlftRuntimeConfig {
    pub fn from_market_slug(slug: &str, market_end_ts: u64, tick_size: f64) -> Option<Self> {
        let symbol = std::env::var("PM_BINANCE_SYMBOL_OVERRIDE")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .or_else(|| infer_binance_symbol(slug))?;
        let total_round_secs = if slug.contains("-5m") {
            300
        } else if slug.contains("-15m") {
            900
        } else if slug.contains("-1h") {
            3600
        } else {
            return None;
        };
        let horizon_key = if total_round_secs == 300 {
            "5m".to_string()
        } else if total_round_secs == 900 {
            "15m".to_string()
        } else {
            "1h".to_string()
        };

        Some(Self {
            symbol,
            horizon_key,
            market_end_ts,
            total_round_secs,
            tick_size,
            intensity_window: Duration::from_secs(
                std::env::var("PM_GLFT_INTENSITY_WINDOW_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(30),
            ),
            refit_interval: Duration::from_secs(
                std::env::var("PM_GLFT_REFIT_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(10),
            ),
        })
    }

    fn snapshot_path(&self) -> PathBuf {
        let dir = std::env::temp_dir().join("pm_as_ofi_glft");
        dir.join(format!("{}_{}.json", self.symbol, self.horizon_key))
    }
}

#[derive(Debug, Clone, Copy)]
struct LocalBook {
    yes_bid: f64,
    yes_ask: f64,
    no_bid: f64,
    no_ask: f64,
}

impl Default for LocalBook {
    fn default() -> Self {
        Self {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct FlowEvent {
    ts: Instant,
    yes_buy: f64,
    yes_sell: f64,
    no_buy: f64,
    no_sell: f64,
}

#[derive(Debug, Clone)]
struct TradeImpact {
    ts: Instant,
    max_bucket: usize,
}

#[derive(Debug, Clone, Copy)]
struct BinanceTick {
    price: f64,
    ts: Instant,
}

pub struct GlftSignalEngine {
    cfg: GlftRuntimeConfig,
    md_rx: mpsc::Receiver<MarketDataMsg>,
    tx: watch::Sender<GlftSignalSnapshot>,
    started_at: Instant,
    book: LocalBook,
    flow_events: VecDeque<FlowEvent>,
    buy_impacts: VecDeque<TradeImpact>,
    sell_impacts: VecDeque<TradeImpact>,
    last_binance_tick: Option<BinanceTick>,
    binance_tick_count: usize,
    poly_book_tick_count: usize,
    round_open_binance: Option<f64>,
    last_poly_mid_prob: Option<f64>,
    last_poly_mid_ts: Option<Instant>,
    basis_raw: f64,
    sigma_prob: f64,
    basis_prob: f64,
    fit: IntensityFitSnapshot,
    warm_start_candidate: Option<WarmStartCandidate>,
    validated_warm_fit: Option<IntensityFitSnapshot>,
    last_live_fit: Option<IntensityFitSnapshot>,
    live_latched: bool,
    ready_fit_streak: u8,
    bootstrap_loaded: bool,
    bootstrap_rejected: bool,
    bootstrap_saved_at: Option<u64>,
    last_fast_refit_at: Option<Instant>,
    hard_basis_blocked: bool,
    drift_mode: DriftMode,
    drift_mode_entered_at: Instant,
    drift_mode_last_switch_at: Instant,
    drift_mode_pending: Option<DriftMode>,
    drift_mode_pending_count: u8,
    pause_entered_at: Option<Instant>,
    pause_recover_healthy_streak: u8,
}

impl GlftSignalEngine {
    pub fn new(
        cfg: GlftRuntimeConfig,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        tx: watch::Sender<GlftSignalSnapshot>,
    ) -> Self {
        let raw_bootstrap = load_bootstrap_snapshot(&cfg);
        let bootstrap = raw_bootstrap.filter(|s| s.basis_prob.abs() <= GLFT_BASIS_SNAPSHOT_MAX_ABS);
        let bootstrap_rejected = raw_bootstrap.is_some() && bootstrap.is_none();
        let fit = IntensityFitSnapshot {
            a: bootstrap.map(|s| s.fit_a).unwrap_or(BOOTSTRAP_A),
            k: bootstrap.map(|s| s.fit_k).unwrap_or(BOOTSTRAP_K),
            quality: FitQuality::Warm,
        };
        let basis_raw = bootstrap.map(|s| s.basis_prob).unwrap_or(BOOTSTRAP_BASIS);
        Self {
            cfg,
            md_rx,
            tx,
            started_at: Instant::now(),
            book: LocalBook::default(),
            flow_events: VecDeque::new(),
            buy_impacts: VecDeque::new(),
            sell_impacts: VecDeque::new(),
            last_binance_tick: None,
            binance_tick_count: 0,
            poly_book_tick_count: 0,
            round_open_binance: None,
            last_poly_mid_prob: None,
            last_poly_mid_ts: None,
            basis_raw,
            sigma_prob: bootstrap.map(|s| s.sigma_prob).unwrap_or(BOOTSTRAP_SIGMA),
            basis_prob: basis_raw.clamp(-GLFT_BASIS_CLAMP_ABS, GLFT_BASIS_CLAMP_ABS),
            fit,
            warm_start_candidate: bootstrap.map(|snapshot| WarmStartCandidate {
                snapshot,
                status: WarmStartStatus::Candidate,
            }),
            validated_warm_fit: None,
            last_live_fit: None,
            live_latched: false,
            ready_fit_streak: 0,
            bootstrap_loaded: bootstrap.is_some(),
            bootstrap_rejected,
            bootstrap_saved_at: bootstrap.map(|s| s.saved_at),
            last_fast_refit_at: None,
            hard_basis_blocked: false,
            drift_mode: DriftMode::Normal,
            drift_mode_entered_at: Instant::now(),
            drift_mode_last_switch_at: Instant::now(),
            drift_mode_pending: None,
            drift_mode_pending_count: 0,
            pause_entered_at: None,
            pause_recover_healthy_streak: 0,
        }
    }

    pub async fn run(mut self) {
        let (binance_tx, mut binance_rx) = mpsc::channel::<BinanceTick>(256);
        let symbol = self.cfg.symbol.clone();
        tokio::spawn(async move {
            run_binance_aggtrade_feed(symbol, binance_tx).await;
        });

        let mut refit_tick = tokio::time::interval(self.cfg.refit_interval);
        let bootstrap_age_secs = self
            .bootstrap_saved_at
            .map(|ts| now_unix().saturating_sub(ts));
        info!(
            "📡 GLFT cold-start guard | source={} basis_raw={:.3} basis_init={:.3} clamp=±{:.2} min_ready={}s ready_cap={}s reject_fallback_wait={}s snapshot_rejected={} snapshot_age_s={}",
            if self.bootstrap_loaded { "warm-start" } else { "bootstrap" },
            self.basis_raw,
            self.basis_prob,
            GLFT_BASIS_CLAMP_ABS,
            GLFT_MIN_READY_WAIT_SECS,
            GLFT_READY_WAIT_CAP_SECS,
            GLFT_REJECTED_WARM_FALLBACK_WAIT_SECS,
            self.bootstrap_rejected,
            bootstrap_age_secs
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
        let initial_snapshot = self.publish();
        info!(
            "📡 GLFT readiness start | state={:?} warm={:?} fit_status={:?} blockers={} elapsed_ms={} source={:?}",
            initial_snapshot.signal_state,
            initial_snapshot.warm_start_status,
            initial_snapshot.fit_status,
            initial_snapshot.readiness_blockers.describe(),
            initial_snapshot.ready_elapsed_ms,
            initial_snapshot.fit_source,
        );
        let mut ready_announced = initial_snapshot.ready;

        loop {
            tokio::select! {
                Some(md) = self.md_rx.recv() => {
                    self.handle_market_data(md);
                    let snapshot = self.publish();
                    if !ready_announced && snapshot.ready {
                        info!(
                            "✅ GLFT signal ready | state={:?} warm={:?} fit_status={:?} source={:?} ready_elapsed_ms={} A={:.3} k={:.3} sigma={:.7} basis={:.3}",
                            snapshot.signal_state,
                            snapshot.warm_start_status,
                            snapshot.fit_status,
                            snapshot.fit_source,
                            snapshot.ready_elapsed_ms,
                            snapshot.fit_a,
                            snapshot.fit_k,
                            snapshot.sigma_prob,
                            snapshot.basis_prob,
                        );
                        ready_announced = true;
                    }
                }
                Some(binance_tick) = binance_rx.recv() => {
                    self.handle_binance_tick(binance_tick);
                    let snapshot = self.publish();
                    if !ready_announced && snapshot.ready {
                        info!(
                            "✅ GLFT signal ready | state={:?} warm={:?} fit_status={:?} source={:?} ready_elapsed_ms={} A={:.3} k={:.3} sigma={:.7} basis={:.3}",
                            snapshot.signal_state,
                            snapshot.warm_start_status,
                            snapshot.fit_status,
                            snapshot.fit_source,
                            snapshot.ready_elapsed_ms,
                            snapshot.fit_a,
                            snapshot.fit_k,
                            snapshot.sigma_prob,
                            snapshot.basis_prob,
                        );
                        ready_announced = true;
                    }
                }
                _ = refit_tick.tick() => {
                    self.refit_intensity();
                    let snapshot = self.publish();
                    if !ready_announced && snapshot.ready {
                        info!(
                            "✅ GLFT signal ready | state={:?} warm={:?} fit_status={:?} source={:?} ready_elapsed_ms={} A={:.3} k={:.3} sigma={:.7} basis={:.3}",
                            snapshot.signal_state,
                            snapshot.warm_start_status,
                            snapshot.fit_status,
                            snapshot.fit_source,
                            snapshot.ready_elapsed_ms,
                            snapshot.fit_a,
                            snapshot.fit_k,
                            snapshot.sigma_prob,
                            snapshot.basis_prob,
                        );
                        ready_announced = true;
                    }
                }
                else => break,
            }
        }
    }

    fn handle_market_data(&mut self, md: MarketDataMsg) {
        match md {
            MarketDataMsg::BookTick {
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                ..
            } => {
                self.poly_book_tick_count = self.poly_book_tick_count.saturating_add(1);
                self.book = LocalBook {
                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,
                };
                if let Some(anchor_prob) = self.anchor_prob() {
                    if let Some(poly_mid) = self.poly_yes_mid() {
                        let obs = poly_mid - anchor_prob;
                        self.basis_raw = ewma_update(self.basis_raw, obs, BASIS_HALF_LIFE_SECS);
                        let clamped = self
                            .basis_raw
                            .clamp(-GLFT_BASIS_CLAMP_ABS, GLFT_BASIS_CLAMP_ABS);
                        if self.live_latched {
                            let tick = self.cfg.tick_size.max(1e-9);
                            let current_drift =
                                drift_ticks_for_basis(anchor_prob, self.basis_prob, poly_mid, tick);
                            let mode = drift_mode_with_hysteresis(self.drift_mode, current_drift);
                            let soft_target =
                                soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS);
                            self.basis_prob = match mode {
                                DriftMode::Normal | DriftMode::Damped | DriftMode::Frozen => {
                                    basis_step_with_asymmetric_limiter(
                                        self.basis_prob,
                                        soft_target,
                                        GLFT_BASIS_LIVE_MAX_STEP,
                                        mode,
                                        anchor_prob,
                                        poly_mid,
                                        tick,
                                    )
                                }
                                DriftMode::Paused => self.basis_prob,
                            };
                            let next_drift =
                                drift_ticks_for_basis(anchor_prob, self.basis_prob, poly_mid, tick);
                            let next_mode = drift_mode_with_hysteresis(mode, next_drift);
                            self.update_drift_mode(next_mode, next_drift, anchor_prob, poly_mid);
                        } else {
                            self.basis_prob = match self.signal_state_at(Instant::now()) {
                                GlftSignalState::Bootstrapping | GlftSignalState::Assimilating => {
                                    blend(self.basis_prob, clamped, BASIS_COLD_BLEND_ALPHA)
                                }
                                GlftSignalState::Live => step_towards(
                                    self.basis_prob,
                                    soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS),
                                    GLFT_BASIS_LIVE_MAX_STEP,
                                ),
                            };
                        }
                    }
                }
                self.update_sigma_from_poly_mid();
                self.maybe_validate_warm_start();
            }
            MarketDataMsg::TradeTick {
                market_side,
                taker_side,
                price,
                size,
                ts,
                ..
            } => {
                self.record_flow(market_side, taker_side, size, ts);
                self.record_trade_impact(market_side, taker_side, price, ts);
                self.prune_windows(ts);
                self.fast_refit_if_needed(ts);
            }
        }
    }

    fn handle_binance_tick(&mut self, tick: BinanceTick) {
        if self.round_open_binance.is_none() {
            self.round_open_binance = Some(tick.price);
        }
        self.binance_tick_count = self.binance_tick_count.saturating_add(1);
        self.last_binance_tick = Some(tick);
        self.maybe_validate_warm_start();
        if self.live_latched {
            if let (Some(anchor_prob), Some(poly_mid)) = (self.anchor_prob(), self.poly_yes_mid()) {
                let tick = self.cfg.tick_size.max(1e-9);
                let drift_ticks =
                    drift_ticks_for_basis(anchor_prob, self.basis_prob, poly_mid, tick);
                let next_mode = drift_mode_with_hysteresis(self.drift_mode, drift_ticks);
                self.update_drift_mode(next_mode, drift_ticks, anchor_prob, poly_mid);
            }
        }
    }

    fn warm_start_status(&self) -> WarmStartStatus {
        if let Some(candidate) = self.warm_start_candidate {
            candidate.status
        } else if self.bootstrap_rejected {
            WarmStartStatus::Rejected
        } else {
            WarmStartStatus::Missing
        }
    }

    fn fit_status(&self) -> GlftFitStatus {
        if self.last_live_fit.is_some() {
            GlftFitStatus::LiveReady
        } else if self.validated_warm_fit.is_some() {
            GlftFitStatus::Provisional
        } else if matches!(self.fit.quality, FitQuality::Invalid) {
            GlftFitStatus::Invalid
        } else {
            GlftFitStatus::Bootstrap
        }
    }

    fn current_fit_for_publish(&self) -> (IntensityFitSnapshot, GlftFitSource) {
        if let Some(fit) = self.last_live_fit {
            (fit, GlftFitSource::LastGoodFit)
        } else if let Some(fit) = self.validated_warm_fit {
            (fit, GlftFitSource::WarmStart)
        } else {
            let mut fit = self.fit;
            if matches!(fit.quality, FitQuality::Invalid) {
                fit.quality = FitQuality::Warm;
            }
            (fit, GlftFitSource::Bootstrap)
        }
    }

    fn current_basis_observation(&self) -> Option<f64> {
        let anchor_prob = self.anchor_prob()?;
        let poly_mid = self.poly_yes_mid()?;
        Some(poly_mid - anchor_prob)
    }

    fn warm_sigma_stable(&self, baseline_sigma: f64) -> bool {
        let current = self.sigma_prob.max(SIGMA_VAR_FLOOR);
        let baseline = baseline_sigma.max(SIGMA_VAR_FLOOR);
        if !(current.is_finite() && baseline.is_finite()) {
            return false;
        }
        let ratio = if current > baseline {
            current / baseline
        } else {
            baseline / current
        };
        ratio <= GLFT_WARM_SIGMA_RATIO_LIMIT
    }

    fn warm_fit_reasonable(snapshot: GlftBootstrapSnapshot) -> bool {
        snapshot.fit_a.is_finite()
            && snapshot.fit_k.is_finite()
            && snapshot.fit_a >= GLFT_WARM_MIN_A
            && snapshot.fit_a <= GLFT_WARM_MAX_A
            && snapshot.fit_k >= GLFT_WARM_MIN_K
            && snapshot.fit_k <= GLFT_WARM_MAX_K
    }

    fn maybe_validate_warm_start(&mut self) {
        let Some(mut candidate) = self.warm_start_candidate else {
            return;
        };
        if candidate.status != WarmStartStatus::Candidate {
            return;
        }
        if self.binance_tick_count < GLFT_WARM_MIN_BINANCE_TICKS
            || self.poly_book_tick_count < GLFT_WARM_MIN_BOOK_TICKS
        {
            return;
        }
        let Some(anchor_prob) = self.anchor_prob() else {
            return;
        };
        let Some(poly_mid) = self.poly_yes_mid() else {
            return;
        };
        let modeled_mid = (anchor_prob + self.basis_prob).clamp(
            self.cfg.tick_size.max(1e-9),
            1.0 - self.cfg.tick_size.max(1e-9),
        );
        let basis_delta = (poly_mid - modeled_mid).abs();
        let tick = self.cfg.tick_size.max(1e-9);
        let basis_ok = basis_delta <= GLFT_WARM_BASIS_DEVIATION_TICKS * tick;
        let basis_soft_ok = basis_delta <= GLFT_WARM_BASIS_SOFT_ACCEPT_TICKS * tick;
        let basis_bridge_ok = basis_delta <= GLFT_WARM_BASIS_BRIDGE_ACCEPT_TICKS * tick;
        let sigma_ok = self.warm_sigma_stable(candidate.snapshot.sigma_prob);
        let fit_ok = Self::warm_fit_reasonable(candidate.snapshot);
        let snapshot_age_secs = now_unix().saturating_sub(candidate.snapshot.saved_at);
        let fresh_snapshot = snapshot_age_secs <= GLFT_WARM_FRESH_SNAPSHOT_MAX_AGE_SECS;

        if basis_ok && sigma_ok && fit_ok {
            candidate.status = WarmStartStatus::Accepted;
            self.validated_warm_fit = Some(IntensityFitSnapshot {
                a: candidate.snapshot.fit_a,
                k: candidate.snapshot.fit_k,
                quality: FitQuality::Ready,
            });
            info!(
                "✅ GLFT warm-start accepted | age_s={} basis_delta={:.4} sigma={:.7} seed_sigma={:.7} A={:.3} k={:.3}",
                self.bootstrap_saved_at
                    .map(|ts| now_unix().saturating_sub(ts))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                basis_delta,
                self.sigma_prob,
                candidate.snapshot.sigma_prob,
                candidate.snapshot.fit_a,
                candidate.snapshot.fit_k,
            );
        } else if basis_soft_ok && sigma_ok && fit_ok {
            candidate.status = WarmStartStatus::Accepted;
            self.validated_warm_fit = Some(IntensityFitSnapshot {
                a: candidate.snapshot.fit_a,
                k: candidate.snapshot.fit_k,
                quality: FitQuality::Ready,
            });
            // Keep fit from snapshot, but reset basis around current observation so
            // startup can continue without inheriting stale center offset.
            self.basis_raw = self.current_basis_observation().unwrap_or(BOOTSTRAP_BASIS);
            self.basis_prob = soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS);
            warn!(
                "⚠️ GLFT warm-start soft-accepted | age_s={} basis_delta={:.4} (strict<= {:.2}t, soft<= {:.2}t) sigma={:.7} seed_sigma={:.7} A={:.3} k={:.3}",
                self.bootstrap_saved_at
                    .map(|ts| now_unix().saturating_sub(ts))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                basis_delta,
                GLFT_WARM_BASIS_DEVIATION_TICKS,
                GLFT_WARM_BASIS_SOFT_ACCEPT_TICKS,
                self.sigma_prob,
                candidate.snapshot.sigma_prob,
                candidate.snapshot.fit_a,
                candidate.snapshot.fit_k,
            );
        } else if fresh_snapshot && basis_bridge_ok && sigma_ok && fit_ok {
            candidate.status = WarmStartStatus::Accepted;
            self.validated_warm_fit = Some(IntensityFitSnapshot {
                a: candidate.snapshot.fit_a,
                k: candidate.snapshot.fit_k,
                quality: FitQuality::Ready,
            });
            // Fresh snapshot bridge:
            // keep fit from recent snapshot, but always recenter basis to current
            // observation before enabling ready path.
            self.basis_raw = self.current_basis_observation().unwrap_or(BOOTSTRAP_BASIS);
            self.basis_prob = soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS);
            warn!(
                "⚠️ GLFT warm-start bridge-accepted | age_s={} basis_delta={:.4} (bridge<= {:.2}t) sigma={:.7} seed_sigma={:.7} A={:.3} k={:.3}",
                snapshot_age_secs,
                basis_delta,
                GLFT_WARM_BASIS_BRIDGE_ACCEPT_TICKS,
                self.sigma_prob,
                candidate.snapshot.sigma_prob,
                candidate.snapshot.fit_a,
                candidate.snapshot.fit_k,
            );
        } else {
            candidate.status = WarmStartStatus::Rejected;
            self.validated_warm_fit = None;
            self.fit = IntensityFitSnapshot {
                a: BOOTSTRAP_A,
                k: BOOTSTRAP_K,
                quality: FitQuality::Warm,
            };
            self.basis_raw = self.current_basis_observation().unwrap_or(BOOTSTRAP_BASIS);
            self.basis_prob = soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS);
            if !(self.sigma_prob.is_finite() && self.sigma_prob > 0.0) {
                self.sigma_prob = BOOTSTRAP_SIGMA;
            }
            warn!(
                "⚠️ GLFT warm-start rejected | age_s={} basis_delta={:.4} sigma={:.7} seed_sigma={:.7} fit_ok={} fresh={} -> downgrade to bootstrap path",
                snapshot_age_secs,
                basis_delta,
                self.sigma_prob,
                candidate.snapshot.sigma_prob,
                fit_ok,
                fresh_snapshot,
            );
        }
        self.warm_start_candidate = Some(candidate);
    }

    fn readiness_gate(&self, now: Instant) -> ReadinessGate {
        let (fit, fit_source) = self.current_fit_for_publish();
        let mut fit_status = self.fit_status();
        let warm_start_status = self.warm_start_status();
        let tick = self.cfg.tick_size.max(1e-9);
        let ready_elapsed_ms = now.duration_since(self.started_at).as_millis() as u64;
        let remaining_secs = self.cfg.market_end_ts.saturating_sub(now_unix());
        let stale_elapsed = self
            .last_binance_tick
            .map(|tick| tick.ts.elapsed())
            .unwrap_or_else(|| {
                Duration::from_secs(BINANCE_STALE_SECS + BINANCE_STALE_GRACE_SECS + 1)
            });
        let stale =
            stale_elapsed > Duration::from_secs(BINANCE_STALE_SECS + BINANCE_STALE_GRACE_SECS);
        let has_binance = self.round_open_binance.is_some()
            && self.binance_tick_count >= GLFT_WARM_MIN_BINANCE_TICKS;
        let has_poly_book =
            self.poly_book_tick_count >= GLFT_WARM_MIN_BOOK_TICKS && self.poly_yes_mid().is_some();
        let min_warmup_elapsed =
            now.duration_since(self.started_at) >= Duration::from_secs(GLFT_MIN_READY_WAIT_SECS);
        let rejected_fallback_wait_elapsed = now.duration_since(self.started_at)
            >= Duration::from_secs(GLFT_REJECTED_WARM_FALLBACK_WAIT_SECS);
        let basis_delta = if has_binance && has_poly_book {
            let anchor = self.anchor_prob().unwrap_or(0.5);
            let poly_mid = self.poly_yes_mid().unwrap_or(0.5);
            let modeled_mid = (anchor + self.basis_prob).clamp(tick, 1.0 - tick);
            (poly_mid - modeled_mid).abs()
        } else {
            0.0
        };
        let basis_drift_ticks = basis_delta / tick;
        let drift_mode = if has_binance && has_poly_book {
            drift_mode_with_hysteresis(self.drift_mode, basis_drift_ticks)
        } else {
            self.drift_mode
        };
        let basis_stable = if has_binance && has_poly_book {
            basis_delta <= GLFT_WARM_BASIS_DEVIATION_TICKS * tick
        } else {
            false
        };
        let basis_soft_stable = if has_binance && has_poly_book {
            basis_delta <= GLFT_WARM_BASIS_SOFT_ACCEPT_TICKS * tick
        } else {
            false
        };
        let paused_now = has_binance && has_poly_book && matches!(drift_mode, DriftMode::Paused);
        let pause_recover_pending = if has_binance
            && has_poly_book
            && self.live_latched
            && !matches!(drift_mode, DriftMode::Paused)
        {
            if let Some(entered_at) = self.pause_entered_at {
                let dwell_ok = now.duration_since(entered_at)
                    >= Duration::from_millis(GLFT_PAUSE_RECOVER_MIN_DWELL_MS);
                let streak_ok =
                    self.pause_recover_healthy_streak >= GLFT_PAUSE_RECOVER_HEALTHY_STREAK;
                !(dwell_ok && streak_ok)
            } else {
                false
            }
        } else {
            false
        };
        let endgame_pause_relax = has_binance
            && has_poly_book
            && remaining_secs <= GLFT_ENDGAME_PAUSE_RELAX_SECS
            && stale_elapsed.as_secs_f64() <= GLFT_ENDGAME_PAUSE_RELAX_STALE_MAX_SECS
            && basis_drift_ticks < GLFT_ENDGAME_PAUSE_RELAX_MAX_DRIFT_TICKS;
        let basis_hard_unstable = (paused_now || pause_recover_pending) && !endgame_pause_relax;
        let sigma_stable = if has_poly_book {
            match self.warm_start_candidate {
                Some(candidate) if candidate.status == WarmStartStatus::Candidate => {
                    self.warm_sigma_stable(candidate.snapshot.sigma_prob)
                }
                _ => self.sigma_prob.is_finite() && self.sigma_prob >= SIGMA_VAR_FLOOR,
            }
        } else {
            false
        };
        // Controlled fallback for rejected warm-start:
        // allow provisional trading sooner than ready-cap, but still only when
        // feeds and soft quality gates are healthy.
        let rejected_warm_fallback_ready = matches!(warm_start_status, WarmStartStatus::Rejected)
            && rejected_fallback_wait_elapsed
            && !stale
            && has_binance
            && has_poly_book
            && basis_soft_stable
            && sigma_stable;
        if rejected_warm_fallback_ready && matches!(fit_status, GlftFitStatus::Bootstrap) {
            fit_status = GlftFitStatus::Provisional;
        }
        let trading_fit_ready = matches!(
            fit_status,
            GlftFitStatus::Provisional | GlftFitStatus::LiveReady
        );

        let raw_blockers = GlftReadinessBlockers {
            await_binance: !has_binance || stale,
            await_poly_book: !has_poly_book,
            await_fit: !trading_fit_ready,
            basis_unstable: has_binance
                && has_poly_book
                && !basis_stable
                && !(matches!(warm_start_status, WarmStartStatus::Rejected) && basis_soft_stable),
            sigma_unstable: has_poly_book && !sigma_stable,
            min_warmup_not_elapsed: !min_warmup_elapsed,
        };
        let hard_not_ready = raw_blockers.await_binance
            || raw_blockers.await_poly_book
            || raw_blockers.min_warmup_not_elapsed
            || basis_hard_unstable;
        // Once the pipeline has reached Live, only hard blockers should pause quoting.
        // Soft blockers are still used before first-ready, but should not flap runtime.
        let ready = if self.live_latched {
            !hard_not_ready
        } else {
            raw_blockers.is_empty()
        };
        let blockers = if ready {
            GlftReadinessBlockers::default()
        } else if self.live_latched {
            GlftReadinessBlockers {
                await_binance: raw_blockers.await_binance,
                await_poly_book: raw_blockers.await_poly_book,
                await_fit: false,
                basis_unstable: basis_hard_unstable,
                sigma_unstable: false,
                min_warmup_not_elapsed: raw_blockers.min_warmup_not_elapsed,
            }
        } else {
            raw_blockers
        };
        let signal_state = if ready {
            GlftSignalState::Live
        } else if has_binance || has_poly_book || warm_start_status != WarmStartStatus::Missing {
            GlftSignalState::Assimilating
        } else {
            GlftSignalState::Bootstrapping
        };

        ReadinessGate {
            signal_state,
            warm_start_status,
            fit_status,
            fit_source,
            fit,
            basis_drift_ticks,
            drift_mode,
            hard_basis_unstable: basis_hard_unstable,
            blockers,
            ready,
            ready_elapsed_ms,
        }
    }

    fn update_sigma_from_poly_mid(&mut self) {
        let Some(poly_mid) = self.poly_yes_mid() else {
            return;
        };
        let now = Instant::now();
        if let (Some(prev_mid), Some(prev_ts)) = (self.last_poly_mid_prob, self.last_poly_mid_ts) {
            let d = poly_mid - prev_mid;
            let dt = now.duration_since(prev_ts).as_secs_f64().max(1e-3);
            // sigma_prob tracks a per-second variance-rate proxy; normalize by dt so
            // high-frequency book updates do not collapse volatility toward zero.
            let var_rate = (d * d) / dt;
            let next_sigma = ewma_update(self.sigma_prob, var_rate, SIGMA_HALF_LIFE_SECS);
            self.sigma_prob = step_cap(
                next_sigma.max(SIGMA_VAR_FLOOR),
                self.sigma_prob.max(SIGMA_VAR_FLOOR),
                0.25,
            );
        }
        self.last_poly_mid_prob = Some(poly_mid);
        self.last_poly_mid_ts = Some(now);
    }

    fn record_flow(&mut self, market_side: Side, taker_side: TakerSide, size: f64, ts: Instant) {
        let mut evt = FlowEvent {
            ts,
            yes_buy: 0.0,
            yes_sell: 0.0,
            no_buy: 0.0,
            no_sell: 0.0,
        };
        match (market_side, taker_side) {
            (Side::Yes, TakerSide::Buy) => evt.yes_buy = size,
            (Side::Yes, TakerSide::Sell) => evt.yes_sell = size,
            (Side::No, TakerSide::Buy) => evt.no_buy = size,
            (Side::No, TakerSide::Sell) => evt.no_sell = size,
        }
        self.flow_events.push_back(evt);
        self.prune_windows(ts);
    }

    fn record_trade_impact(
        &mut self,
        market_side: Side,
        taker_side: TakerSide,
        price: f64,
        ts: Instant,
    ) {
        let tick = self.cfg.tick_size.max(1e-9);
        match (market_side, taker_side) {
            (Side::Yes, TakerSide::Sell) => {
                let max_bucket = max_buy_bucket(self.book.yes_bid, price, tick);
                self.buy_impacts.push_back(TradeImpact { ts, max_bucket });
            }
            (Side::No, TakerSide::Sell) => {
                let max_bucket = max_buy_bucket(self.book.no_bid, price, tick);
                self.buy_impacts.push_back(TradeImpact { ts, max_bucket });
            }
            (Side::Yes, TakerSide::Buy) => {
                let max_bucket = max_sell_bucket(self.book.yes_ask, price, tick);
                self.sell_impacts.push_back(TradeImpact { ts, max_bucket });
            }
            (Side::No, TakerSide::Buy) => {
                let max_bucket = max_sell_bucket(self.book.no_ask, price, tick);
                self.sell_impacts.push_back(TradeImpact { ts, max_bucket });
            }
        }
        self.prune_windows(ts);
    }

    fn prune_windows(&mut self, now: Instant) {
        while let Some(front) = self.flow_events.front() {
            if now.duration_since(front.ts) > self.cfg.intensity_window {
                self.flow_events.pop_front();
            } else {
                break;
            }
        }
        while let Some(front) = self.buy_impacts.front() {
            if now.duration_since(front.ts) > self.cfg.intensity_window {
                self.buy_impacts.pop_front();
            } else {
                break;
            }
        }
        while let Some(front) = self.sell_impacts.front() {
            if now.duration_since(front.ts) > self.cfg.intensity_window {
                self.sell_impacts.pop_front();
            } else {
                break;
            }
        }
    }

    fn fast_refit_if_needed(&mut self, now: Instant) {
        if self.last_live_fit.is_some() {
            return;
        }
        let min_interval = Duration::from_millis(STARTUP_FAST_REFIT_MIN_INTERVAL_MS);
        if self
            .last_fast_refit_at
            .map(|last| now.duration_since(last) < min_interval)
            .unwrap_or(false)
        {
            return;
        }
        if self.buy_impacts.len() + self.sell_impacts.len() < STARTUP_MIN_TRADE_COUNT {
            return;
        }
        self.last_fast_refit_at = Some(now);
        self.refit_intensity();
    }

    fn refit_intensity(&mut self) {
        let now = Instant::now();
        self.prune_windows(now);

        let trade_count = self.buy_impacts.len() + self.sell_impacts.len();
        let startup = self.last_live_fit.is_none();
        let min_trade_count = if startup {
            STARTUP_MIN_TRADE_COUNT
        } else {
            STEADY_MIN_TRADE_COUNT
        };
        let min_bucket_count = if startup {
            STARTUP_MIN_BUCKET_COUNT
        } else {
            STEADY_MIN_BUCKET_COUNT
        };
        let min_r2 = if startup {
            STARTUP_MIN_R2
        } else {
            STEADY_MIN_R2
        };

        if trade_count < min_trade_count {
            self.fit.quality = FitQuality::Warm;
            self.ready_fit_streak = 0;
            return;
        }

        let mut counts = [0.0_f64; GLFT_MAX_BUCKETS];
        for impact in self.buy_impacts.iter().chain(self.sell_impacts.iter()) {
            for idx in 0..=impact.max_bucket.min(GLFT_MAX_BUCKETS - 1) {
                counts[idx] += 1.0;
            }
        }

        let window_secs = self.cfg.intensity_window.as_secs_f64().max(1.0);
        let mut xs = Vec::with_capacity(GLFT_MAX_BUCKETS);
        let mut ys = Vec::with_capacity(GLFT_MAX_BUCKETS);
        for (idx, count) in counts.iter().enumerate() {
            let lambda = *count / window_secs;
            if lambda > 0.0 {
                xs.push(idx as f64);
                ys.push(lambda.ln());
            }
        }
        if xs.len() < min_bucket_count {
            self.fit.quality = FitQuality::Invalid;
            self.ready_fit_streak = 0;
            return;
        }

        let Some((a, k, r2)) = fit_exponential(&xs, &ys) else {
            self.fit.quality = FitQuality::Invalid;
            self.ready_fit_streak = 0;
            return;
        };
        if !(a.is_finite() && k.is_finite() && a > 0.0 && k > 0.0 && r2 >= min_r2) {
            self.fit.quality = FitQuality::Invalid;
            self.ready_fit_streak = 0;
            return;
        }

        let candidate = if let Some(last_good) = self.last_live_fit {
            let a_capped = step_cap(a, last_good.a, 1.0);
            let k_capped = step_cap(k, last_good.k, 0.25);
            IntensityFitSnapshot {
                a: a_capped,
                k: k_capped,
                quality: FitQuality::Ready,
            }
        } else {
            IntensityFitSnapshot {
                a,
                k,
                quality: FitQuality::Ready,
            }
        };

        self.fit = candidate;
        self.last_live_fit = Some(candidate);
        self.ready_fit_streak = self.ready_fit_streak.saturating_add(1);
        if self.ready_fit_streak >= SNAPSHOT_SAVE_READY_STREAK_MIN {
            save_bootstrap_snapshot(
                &self.cfg,
                GlftBootstrapSnapshot {
                    fit_a: candidate.a,
                    fit_k: candidate.k,
                    sigma_prob: self.sigma_prob,
                    basis_prob: self.basis_prob,
                    saved_at: now_unix(),
                },
            );
        }
    }

    fn publish(&mut self) -> GlftSignalSnapshot {
        let anchor_prob = self.anchor_prob().unwrap_or(0.5);
        let alpha_flow = self.alpha_flow();
        let remaining_secs = self.cfg.market_end_ts.saturating_sub(now_unix());
        let tau_secs = remaining_secs as f64;
        let tau_norm =
            (remaining_secs as f64 / self.cfg.total_round_secs.max(1) as f64).clamp(0.0, 1.0);
        let gate = self.readiness_gate(Instant::now());
        if gate.ready {
            self.live_latched = true;
        }
        self.drift_mode = gate.drift_mode;
        let tick = self.cfg.tick_size.max(1e-9);
        let synthetic_mid_yes = self.poly_yes_mid().unwrap_or(0.5);
        let basis_clamped = match gate.signal_state {
            GlftSignalState::Bootstrapping | GlftSignalState::Assimilating => self
                .basis_raw
                .clamp(-GLFT_BASIS_CLAMP_ABS, GLFT_BASIS_CLAMP_ABS),
            GlftSignalState::Live => soft_clip(self.basis_raw, GLFT_BASIS_LIVE_SOFT_CAP_ABS),
        };
        let modeled_mid = (anchor_prob + basis_clamped).clamp(tick, 1.0 - tick);
        let trusted_mid = trusted_mid(
            modeled_mid,
            synthetic_mid_yes,
            tick,
            match gate.fit_status {
                GlftFitStatus::LiveReady => GLFT_TRUSTED_MID_LIVE_CORRIDOR_TICKS,
                _ => GLFT_TRUSTED_MID_PROVISIONAL_CORRIDOR_TICKS,
            },
        );
        let stale_secs = self
            .last_binance_tick
            .map(|tick| tick.ts.elapsed().as_secs_f64())
            .unwrap_or(f64::INFINITY);
        let snapshot = GlftSignalSnapshot {
            anchor_prob,
            basis_prob: self.basis_prob,
            basis_raw: self.basis_raw,
            basis_clamped,
            basis_drift_ticks: gate.basis_drift_ticks,
            modeled_mid,
            trusted_mid,
            synthetic_mid_yes,
            alpha_flow,
            sigma_prob: self.sigma_prob,
            tau_norm,
            tau_secs,
            fit_a: gate.fit.a,
            fit_k: gate.fit.k,
            fit_quality: gate.fit.quality,
            fit_source: gate.fit_source,
            warm_start_status: gate.warm_start_status,
            fit_status: gate.fit_status,
            readiness_blockers: gate.blockers,
            ready_elapsed_ms: gate.ready_elapsed_ms,
            signal_state: gate.signal_state,
            drift_mode: gate.drift_mode,
            hard_basis_unstable: gate.hard_basis_unstable,
            ready: gate.ready,
            stale: gate.blockers.await_binance,
            stale_secs,
        };
        if gate.hard_basis_unstable && self.live_latched && !self.hard_basis_blocked {
            warn!(
                "⚠️ GLFT hard basis misalignment -> pause quoting | modeled_mid={:.3} synthetic_mid={:.3} basis_raw={:.3} basis_clamped={:.3} drift_ticks={:.1}",
                modeled_mid,
                synthetic_mid_yes,
                self.basis_raw,
                basis_clamped,
                gate.basis_drift_ticks,
            );
            self.hard_basis_blocked = true;
            if self.pause_entered_at.is_none() {
                self.pause_entered_at = Some(Instant::now());
            }
            self.pause_recover_healthy_streak = 0;
        } else if !gate.hard_basis_unstable && self.hard_basis_blocked && gate.ready {
            info!(
                "✅ GLFT basis alignment recovered -> resume quoting | modeled_mid={:.3} synthetic_mid={:.3} dwell_ms={} healthy_streak={}",
                modeled_mid,
                synthetic_mid_yes,
                self.pause_entered_at
                    .map(|entered_at| Instant::now().duration_since(entered_at).as_millis())
                    .unwrap_or(0),
                self.pause_recover_healthy_streak,
            );
            self.hard_basis_blocked = false;
            self.pause_entered_at = None;
            self.pause_recover_healthy_streak = 0;
        }
        let _ = self.tx.send(snapshot);
        snapshot
    }

    fn signal_state_at(&self, now: Instant) -> GlftSignalState {
        self.readiness_gate(now).signal_state
    }

    fn update_drift_mode(
        &mut self,
        next_mode: DriftMode,
        drift_ticks: f64,
        anchor_prob: f64,
        synthetic_mid_yes: f64,
    ) {
        let now = Instant::now();
        if next_mode == self.drift_mode {
            if self.pause_entered_at.is_some() {
                if matches!(next_mode, DriftMode::Paused) {
                    self.pause_recover_healthy_streak = 0;
                } else if self.pause_recover_healthy_streak > 0 {
                    self.pause_recover_healthy_streak = (self.pause_recover_healthy_streak + 1)
                        .min(GLFT_PAUSE_RECOVER_HEALTHY_STREAK);
                }
            }
            self.drift_mode_pending = None;
            self.drift_mode_pending_count = 0;
            return;
        }

        let escalating = drift_mode_rank(next_mode) > drift_mode_rank(self.drift_mode);
        let force_paused =
            matches!(next_mode, DriftMode::Paused) && drift_ticks >= GLFT_DRIFT_PAUSED_FORCE_TICKS;

        if self.live_latched
            && !escalating
            && now.duration_since(self.drift_mode_entered_at)
                < Duration::from_millis(GLFT_DRIFT_DEESCALATE_MIN_DWELL_MS)
        {
            return;
        }

        if self.live_latched
            && !force_paused
            && now.duration_since(self.drift_mode_last_switch_at)
                < Duration::from_millis(GLFT_DRIFT_MODE_SWITCH_MIN_INTERVAL_MS)
        {
            return;
        }

        if force_paused {
            self.drift_mode_pending = None;
            self.drift_mode_pending_count = 0;
            self.apply_drift_mode(next_mode, drift_ticks, anchor_prob, synthetic_mid_yes, now);
            return;
        }

        if self.drift_mode_pending == Some(next_mode) {
            self.drift_mode_pending_count = self.drift_mode_pending_count.saturating_add(1);
        } else {
            self.drift_mode_pending = Some(next_mode);
            self.drift_mode_pending_count = 1;
        }

        let required = if escalating {
            GLFT_DRIFT_ESCALATE_CONFIRM_SAMPLES
        } else {
            GLFT_DRIFT_DEESCALATE_CONFIRM_SAMPLES
        };
        if self.drift_mode_pending_count < required {
            return;
        }

        self.drift_mode_pending = None;
        self.drift_mode_pending_count = 0;
        self.apply_drift_mode(next_mode, drift_ticks, anchor_prob, synthetic_mid_yes, now);
    }

    fn apply_drift_mode(
        &mut self,
        next_mode: DriftMode,
        drift_ticks: f64,
        anchor_prob: f64,
        synthetic_mid_yes: f64,
        now: Instant,
    ) {
        let modeled_mid =
            modeled_mid_from_basis(anchor_prob, self.basis_prob, self.cfg.tick_size.max(1e-9));
        if matches!(next_mode, DriftMode::Paused) {
            warn!(
                "📡 GLFT drift mode -> {:?} | drift_ticks={:.1} modeled_mid={:.3} synthetic_mid={:.3} basis={:.3}",
                next_mode,
                drift_ticks,
                modeled_mid,
                synthetic_mid_yes,
                self.basis_prob,
            );
        } else {
            info!(
                "📡 GLFT drift mode -> {:?} | drift_ticks={:.1} modeled_mid={:.3} synthetic_mid={:.3} basis={:.3}",
                next_mode,
                drift_ticks,
                modeled_mid,
                synthetic_mid_yes,
                self.basis_prob,
            );
        }
        self.drift_mode_entered_at = now;
        self.drift_mode_last_switch_at = now;

        if matches!(next_mode, DriftMode::Paused) {
            if !matches!(self.drift_mode, DriftMode::Paused) {
                self.pause_entered_at = Some(now);
            }
            self.pause_recover_healthy_streak = 0;
        } else if self.pause_entered_at.is_some() {
            if matches!(self.drift_mode, DriftMode::Paused) {
                self.pause_recover_healthy_streak = 1.min(GLFT_PAUSE_RECOVER_HEALTHY_STREAK);
            } else if self.pause_recover_healthy_streak > 0 {
                self.pause_recover_healthy_streak =
                    (self.pause_recover_healthy_streak + 1).min(GLFT_PAUSE_RECOVER_HEALTHY_STREAK);
            }
        }
        self.drift_mode = next_mode;
    }

    fn alpha_flow(&self) -> f64 {
        let mut yes_buy = 0.0;
        let mut yes_sell = 0.0;
        let mut no_buy = 0.0;
        let mut no_sell = 0.0;
        for evt in &self.flow_events {
            yes_buy += evt.yes_buy;
            yes_sell += evt.yes_sell;
            no_buy += evt.no_buy;
            no_sell += evt.no_sell;
        }
        let yes_flow = (yes_buy - yes_sell) / (yes_buy + yes_sell + 1e-9);
        let no_flow = (no_buy - no_sell) / (no_buy + no_sell + 1e-9);
        (0.5 * (yes_flow - no_flow)).clamp(-1.0, 1.0)
    }

    fn poly_yes_mid(&self) -> Option<f64> {
        let yes_mid = mid(self.book.yes_bid, self.book.yes_ask);
        let no_mid = mid(self.book.no_bid, self.book.no_ask);
        match (yes_mid, no_mid) {
            (Some(y), Some(n)) => Some(((y) + (1.0 - n)) / 2.0),
            (Some(y), None) => Some(y),
            (None, Some(n)) => Some(1.0 - n),
            (None, None) => None,
        }
    }

    fn anchor_prob(&self) -> Option<f64> {
        let round_open = self.round_open_binance?;
        let last = self.last_binance_tick?;
        let remaining_secs = self.cfg.market_end_ts.saturating_sub(now_unix()).max(1) as f64;
        let sigma = self.sigma_prob.max(1e-8).sqrt();
        let denom = (sigma * remaining_secs.sqrt()).max(1e-6);
        let z = (last.price / round_open).ln() / denom;
        Some(norm_cdf(z).clamp(0.01, 0.99))
    }
}

pub struct OptimalOffsets {
    pub inventory_shift: f64,
    pub half_spread_base: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct GlftQuoteShape {
    pub r_yes_pre: f64,
    pub r_yes_post: f64,
    pub dominant_side: Option<Side>,
    pub dominant_buy_penalty_ticks: f64,
    pub dominant_buy_suppressed: bool,
    pub suppress_yes_buy: bool,
    pub suppress_no_buy: bool,
    pub yes_buy_ceiling: f64,
    pub yes_sell_floor: f64,
    pub no_buy_ceiling: f64,
    pub no_sell_floor: f64,
}

pub fn shape_glft_quotes(
    r_yes_pre: f64,
    half_spread: f64,
    drift_mode: DriftMode,
    drift_ticks: f64,
    tick_size: f64,
    heat_score: f64,
) -> GlftQuoteShape {
    let tick = tick_size.max(1e-9);
    let min_price = tick;
    let max_price = 1.0 - tick;
    let dominant_side = if r_yes_pre > 0.5 {
        Some(Side::Yes)
    } else if r_yes_pre < 0.5 {
        Some(Side::No)
    } else {
        None
    };
    let dominant_buy_penalty_ticks = dominant_buy_penalty_ticks(r_yes_pre, heat_score);
    let dominant_buy_penalty = dominant_buy_penalty_ticks * tick;
    let dominant_buy_suppressed = dominant_side.is_some()
        && (matches!(drift_mode, DriftMode::Frozen)
            || (matches!(drift_mode, DriftMode::Damped)
                && drift_ticks >= GLFT_DAMPED_DOMINANT_SUPPRESS_TICKS));

    let mut r_yes_post = r_yes_pre;
    let mut yes_buy_ceiling = (r_yes_pre - half_spread).clamp(min_price, max_price);
    let yes_sell_floor = (r_yes_pre + half_spread).clamp(min_price, max_price);
    let mut no_buy_ceiling = ((1.0 - r_yes_pre) - half_spread).clamp(min_price, max_price);
    let no_sell_floor = ((1.0 - r_yes_pre) + half_spread).clamp(min_price, max_price);

    let mut suppress_yes_buy = false;
    let mut suppress_no_buy = false;
    match dominant_side {
        Some(Side::Yes) => {
            yes_buy_ceiling = (yes_buy_ceiling - dominant_buy_penalty).clamp(min_price, max_price);
            r_yes_post = (r_yes_pre - dominant_buy_penalty).clamp(min_price, max_price);
            suppress_yes_buy = dominant_buy_suppressed;
        }
        Some(Side::No) => {
            no_buy_ceiling = (no_buy_ceiling - dominant_buy_penalty).clamp(min_price, max_price);
            r_yes_post = (r_yes_pre + dominant_buy_penalty).clamp(min_price, max_price);
            suppress_no_buy = dominant_buy_suppressed;
        }
        None => {}
    }

    GlftQuoteShape {
        r_yes_pre,
        r_yes_post,
        dominant_side,
        dominant_buy_penalty_ticks,
        dominant_buy_suppressed,
        suppress_yes_buy,
        suppress_no_buy,
        yes_buy_ceiling,
        yes_sell_floor,
        no_buy_ceiling,
        no_sell_floor,
    }
}

pub fn compute_optimal_offsets(
    q_norm: f64,
    sigma_prob: f64,
    tau_secs: f64,
    fit: IntensityFitSnapshot,
    gamma: f64,
    xi: f64,
    bid_size: f64,
    max_net_diff: f64,
    tick_size: f64,
) -> OptimalOffsets {
    let delta_q = (bid_size / max_net_diff.max(1e-9)).max(1e-6);
    // q_norm in [-1, 1] is convenient for strategy state, but GLFT inventory term
    // is naturally expressed in quote-size steps.
    let q_steps = q_norm.clamp(-1.0, 1.0) / delta_q;
    let sigma = (sigma_prob.max(1e-9) * tau_secs.max(1.0)).sqrt();
    let tick = tick_size.max(1e-9);
    let a = fit.a.max(1e-6);
    let k = fit.k.max(1e-6);
    let xi = xi.max(1e-6);
    let gamma = gamma.max(1e-6);
    let ratio = 1.0 + (xi * delta_q) / k;
    // k is fitted from bucketed quote distance in "ticks".
    // Convert GLFT theoretical offsets back to price-space.
    let c1 = ((1.0 / (xi * delta_q)) * ratio.ln()) * tick;
    let c2_term = (gamma / (2.0 * a * delta_q * k)) * ratio.powf(k / (xi * delta_q) + 1.0);
    // c2 stays in quote-space after fitting k in tick-buckets; multiplying by tick
    // again over-shrinks inventory response.
    let c2 = c2_term.max(1e-12).sqrt();
    OptimalOffsets {
        inventory_shift: q_steps * sigma * c2,
        half_spread_base: c1 + 0.5 * delta_q * sigma * c2,
    }
}

pub fn compute_glft_alpha_shift(
    alpha_flow: f64,
    ofi_alpha: f64,
    tick_size: f64,
    fit_status: GlftFitStatus,
) -> f64 {
    let tick = tick_size.max(1e-9);
    let cap_ticks = match fit_status {
        GlftFitStatus::LiveReady => 8.0,
        GlftFitStatus::Provisional => 6.0,
        GlftFitStatus::Bootstrap | GlftFitStatus::Invalid => 4.0,
    };
    let cap = cap_ticks * tick;
    (ofi_alpha * alpha_flow).clamp(-cap, cap)
}

fn fit_exponential(xs: &[f64], ys: &[f64]) -> Option<(f64, f64, f64)> {
    if xs.len() != ys.len() || xs.len() < 2 {
        return None;
    }
    let n = xs.len() as f64;
    let sum_x: f64 = xs.iter().sum();
    let sum_y: f64 = ys.iter().sum();
    let mean_x = sum_x / n;
    let mean_y = sum_y / n;

    let mut sxx = 0.0;
    let mut sxy = 0.0;
    let mut syy = 0.0;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        sxx += (x - mean_x) * (x - mean_x);
        sxy += (x - mean_x) * (y - mean_y);
        syy += (y - mean_y) * (y - mean_y);
    }
    if sxx <= 0.0 || syy <= 0.0 {
        return None;
    }
    let slope = sxy / sxx;
    let intercept = mean_y - slope * mean_x;
    let a = intercept.exp();
    let k = (-slope).max(0.0);

    let mut ss_res = 0.0;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        let y_hat = intercept + slope * x;
        ss_res += (y - y_hat) * (y - y_hat);
    }
    let r2 = 1.0 - (ss_res / syy);
    Some((a, k, r2))
}

fn mid(bid: f64, ask: f64) -> Option<f64> {
    if bid > 0.0 && ask > bid {
        Some((bid + ask) * 0.5)
    } else {
        None
    }
}

fn max_buy_bucket(best_bid: f64, trade_price: f64, tick: f64) -> usize {
    if !(best_bid > 0.0 && trade_price > 0.0 && tick > 0.0) {
        return 0;
    }
    (((best_bid - trade_price).max(0.0) / tick).floor() as usize).min(GLFT_MAX_BUCKETS - 1)
}

fn max_sell_bucket(best_ask: f64, trade_price: f64, tick: f64) -> usize {
    if !(best_ask > 0.0 && trade_price > 0.0 && tick > 0.0) {
        return 0;
    }
    (((trade_price - best_ask).max(0.0) / tick).floor() as usize).min(GLFT_MAX_BUCKETS - 1)
}

fn ewma_update(current: f64, observation: f64, half_life_secs: f64) -> f64 {
    let alpha = 1.0 - (-std::f64::consts::LN_2 / half_life_secs.max(1.0)).exp();
    current + alpha * (observation - current)
}

fn step_cap(next: f64, current: f64, limit: f64) -> f64 {
    if current <= 0.0 || !current.is_finite() {
        return next;
    }
    let lower = current * (1.0 - limit);
    let upper = current * (1.0 + limit);
    next.clamp(lower, upper)
}

fn step_towards(current: f64, target: f64, max_step: f64) -> f64 {
    let step = max_step.max(1e-9);
    let delta = target - current;
    if delta.abs() <= step {
        target
    } else {
        current + delta.signum() * step
    }
}

fn modeled_mid_from_basis(anchor_prob: f64, basis_prob: f64, tick: f64) -> f64 {
    (anchor_prob + basis_prob).clamp(tick.max(1e-9), 1.0 - tick.max(1e-9))
}

fn drift_ticks_for_basis(
    anchor_prob: f64,
    basis_prob: f64,
    synthetic_mid_yes: f64,
    tick: f64,
) -> f64 {
    ((modeled_mid_from_basis(anchor_prob, basis_prob, tick) - synthetic_mid_yes).abs())
        / tick.max(1e-9)
}

#[cfg(test)]
fn frozen_basis_step(
    current_basis: f64,
    soft_target: f64,
    max_step: f64,
    anchor_prob: f64,
    synthetic_mid_yes: f64,
    tick: f64,
) -> f64 {
    let current_drift = drift_ticks_for_basis(anchor_prob, current_basis, synthetic_mid_yes, tick);
    let candidate = step_towards(current_basis, soft_target, max_step);
    let candidate_drift = drift_ticks_for_basis(anchor_prob, candidate, synthetic_mid_yes, tick);
    if candidate_drift + 1e-9 < current_drift {
        candidate
    } else {
        current_basis
    }
}

fn basis_step_with_asymmetric_limiter(
    current_basis: f64,
    soft_target: f64,
    base_step: f64,
    drift_mode: DriftMode,
    anchor_prob: f64,
    synthetic_mid_yes: f64,
    tick: f64,
) -> f64 {
    if !current_basis.is_finite() || !soft_target.is_finite() {
        return current_basis;
    }
    let base = base_step.max(1e-9);
    let current_drift = drift_ticks_for_basis(anchor_prob, current_basis, synthetic_mid_yes, tick);
    let trial = step_towards(current_basis, soft_target, base);
    let trial_drift = drift_ticks_for_basis(anchor_prob, trial, synthetic_mid_yes, tick);
    let improving = trial_drift + 1e-9 < current_drift;

    let step_mult = if improving {
        basis_recovery_step_multiplier(drift_mode)
    } else {
        basis_worsening_step_multiplier(drift_mode, current_drift)
    };
    let min_step = base * 0.2;
    let max_step = base * 1.8;
    let bounded_step = (base * step_mult).clamp(min_step, max_step);
    step_towards(current_basis, soft_target, bounded_step)
}

fn basis_recovery_step_multiplier(mode: DriftMode) -> f64 {
    match mode {
        DriftMode::Normal => 1.15,
        DriftMode::Damped => 1.30,
        DriftMode::Frozen => 1.45,
        DriftMode::Paused => 0.0,
    }
}

fn basis_worsening_step_multiplier(mode: DriftMode, drift_ticks: f64) -> f64 {
    let drift = drift_ticks.max(0.0);
    match mode {
        DriftMode::Normal => 0.90 / (1.0 + drift / 10.0),
        DriftMode::Damped => 0.65 / (1.0 + drift / 8.0),
        DriftMode::Frozen => 0.45 / (1.0 + drift / 6.0),
        DriftMode::Paused => 0.0,
    }
}

fn drift_mode_with_hysteresis(current: DriftMode, drift_ticks: f64) -> DriftMode {
    match current {
        DriftMode::Normal => {
            if drift_ticks > GLFT_DRIFT_PAUSED_ENTER_TICKS {
                DriftMode::Paused
            } else if drift_ticks > GLFT_DRIFT_FROZEN_ENTER_TICKS {
                DriftMode::Frozen
            } else if drift_ticks > GLFT_DRIFT_DAMPED_ENTER_TICKS {
                DriftMode::Damped
            } else {
                DriftMode::Normal
            }
        }
        DriftMode::Damped => {
            if drift_ticks > GLFT_DRIFT_PAUSED_ENTER_TICKS {
                DriftMode::Paused
            } else if drift_ticks > GLFT_DRIFT_FROZEN_ENTER_TICKS {
                DriftMode::Frozen
            } else if drift_ticks < GLFT_DRIFT_DAMPED_EXIT_TICKS {
                DriftMode::Normal
            } else {
                DriftMode::Damped
            }
        }
        DriftMode::Frozen => {
            if drift_ticks > GLFT_DRIFT_PAUSED_ENTER_TICKS {
                DriftMode::Paused
            } else if drift_ticks < GLFT_DRIFT_FROZEN_EXIT_TICKS {
                if drift_ticks < GLFT_DRIFT_DAMPED_EXIT_TICKS {
                    DriftMode::Normal
                } else {
                    DriftMode::Damped
                }
            } else {
                DriftMode::Frozen
            }
        }
        DriftMode::Paused => {
            if drift_ticks < GLFT_DRIFT_PAUSED_EXIT_TICKS {
                if drift_ticks < GLFT_DRIFT_FROZEN_EXIT_TICKS {
                    if drift_ticks < GLFT_DRIFT_DAMPED_EXIT_TICKS {
                        DriftMode::Normal
                    } else {
                        DriftMode::Damped
                    }
                } else {
                    DriftMode::Frozen
                }
            } else {
                DriftMode::Paused
            }
        }
    }
}

fn drift_mode_rank(mode: DriftMode) -> u8 {
    match mode {
        DriftMode::Normal => 0,
        DriftMode::Damped => 1,
        DriftMode::Frozen => 2,
        DriftMode::Paused => 3,
    }
}

fn dominant_buy_penalty_ticks(r_yes_pre: f64, heat_score: f64) -> f64 {
    let dev = (r_yes_pre - 0.5).abs();
    let base = if dev <= 0.10 {
        0.0
    } else if dev <= 0.20 {
        2.0
    } else if dev <= 0.30 {
        4.0
    } else {
        6.0
    };
    let heat_add = if heat_score <= 1.0 {
        0.0
    } else if heat_score <= 2.0 {
        1.0
    } else if heat_score <= 3.0 {
        2.0
    } else {
        3.0
    };
    base + heat_add
}

fn soft_clip(value: f64, cap_abs: f64) -> f64 {
    let cap = cap_abs.max(1e-9);
    cap * (value / cap).tanh()
}

fn norm_cdf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * z);
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let erf = 1.0 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * (-z * z).exp());
    0.5 * (1.0 + sign * erf)
}

fn trusted_mid(modeled_mid: f64, synthetic_mid_yes: f64, tick: f64, corridor_ticks: f64) -> f64 {
    let min_price = tick.max(1e-9);
    let max_price = 1.0 - min_price;
    let corridor = corridor_ticks.max(1.0) * min_price;
    let lower = (synthetic_mid_yes - corridor).clamp(min_price, max_price);
    let upper = (synthetic_mid_yes + corridor).clamp(min_price, max_price);
    modeled_mid.clamp(lower, upper)
}

fn infer_binance_symbol(slug: &str) -> Option<String> {
    let lower = slug.to_ascii_lowercase();
    if !lower.contains("-updown-") {
        return None;
    }
    if lower.starts_with("btc-") {
        Some("BTCUSDT".to_string())
    } else if lower.starts_with("eth-") {
        Some("ETHUSDT".to_string())
    } else if lower.starts_with("xrp-") {
        Some("XRPUSDT".to_string())
    } else if lower.starts_with("sol-") {
        Some("SOLUSDT".to_string())
    } else {
        None
    }
}

fn load_bootstrap_snapshot(cfg: &GlftRuntimeConfig) -> Option<GlftBootstrapSnapshot> {
    let path = cfg.snapshot_path();
    let raw = fs::read_to_string(path).ok()?;
    let parsed: GlftBootstrapSnapshot = serde_json::from_str(&raw).ok()?;
    if now_unix().saturating_sub(parsed.saved_at) <= SNAPSHOT_TTL_SECS {
        Some(parsed)
    } else {
        None
    }
}

fn save_bootstrap_snapshot(cfg: &GlftRuntimeConfig, snapshot: GlftBootstrapSnapshot) {
    let path = cfg.snapshot_path();
    if let Some(dir) = path.parent() {
        let _ = fs::create_dir_all(dir);
    }
    match serde_json::to_string(&snapshot) {
        Ok(serialized) => {
            if let Err(err) = fs::write(&path, serialized) {
                debug!("GLFT bootstrap snapshot write failed: {:?}", err);
            }
        }
        Err(err) => debug!("GLFT bootstrap snapshot serialize failed: {:?}", err),
    }
}

fn blend(current: f64, target: f64, alpha: f64) -> f64 {
    let a = alpha.clamp(0.0, 1.0);
    current * (1.0 - a) + target * a
}

async fn run_binance_aggtrade_feed(symbol: String, tx: mpsc::Sender<BinanceTick>) {
    let stream = format!("{}@aggTrade", symbol.to_ascii_lowercase());
    let url = format!("wss://fstream.binance.com/ws/{}", stream);
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                info!("📡 GLFT Binance anchor connected: {}", symbol);
                backoff = Duration::from_millis(250);
                let (_write, mut read) = ws.split();
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                if let Some(price) = value
                                    .get("p")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .filter(|v| *v > 0.0)
                                {
                                    let _ = tx
                                        .send(BinanceTick {
                                            price,
                                            ts: Instant::now(),
                                        })
                                        .await;
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            warn!("⚠️ GLFT Binance anchor closed: {}", symbol);
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!("⚠️ GLFT Binance anchor read failed: {} {:?}", symbol, err);
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(
                    "⚠️ GLFT Binance anchor connect failed: {} {:?}",
                    symbol, err
                );
            }
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, watch};

    fn test_cfg(symbol: &str) -> GlftRuntimeConfig {
        GlftRuntimeConfig {
            symbol: symbol.to_string(),
            horizon_key: "test".to_string(),
            market_end_ts: now_unix() + 300,
            total_round_secs: 300,
            tick_size: 0.01,
            intensity_window: Duration::from_secs(30),
            refit_interval: Duration::from_secs(10),
        }
    }

    fn test_engine(symbol: &str, snapshot: Option<GlftBootstrapSnapshot>) -> GlftSignalEngine {
        let cfg = test_cfg(symbol);
        let path = cfg.snapshot_path();
        let _ = fs::remove_file(&path);
        if let Some(snapshot) = snapshot {
            save_bootstrap_snapshot(&cfg, snapshot);
        }
        let (_md_tx, md_rx) = mpsc::channel(4);
        let (tx, _rx) = watch::channel(GlftSignalSnapshot::default());
        let engine = GlftSignalEngine::new(cfg.clone(), md_rx, tx);
        let _ = fs::remove_file(path);
        engine
    }

    fn prime_ready_inputs(engine: &mut GlftSignalEngine) {
        engine.started_at = Instant::now() - Duration::from_secs(3);
        engine.round_open_binance = Some(100.0);
        engine.last_binance_tick = Some(BinanceTick {
            price: 100.0,
            ts: Instant::now(),
        });
        engine.binance_tick_count = GLFT_WARM_MIN_BINANCE_TICKS;
        engine.poly_book_tick_count = GLFT_WARM_MIN_BOOK_TICKS;
        engine.book = LocalBook {
            yes_bid: 0.49,
            yes_ask: 0.50,
            no_bid: 0.50,
            no_ask: 0.51,
        };
        engine.basis_raw = 0.0;
        engine.basis_prob = 0.0;
        engine.sigma_prob = BOOTSTRAP_SIGMA;
    }

    #[test]
    fn infer_symbols_from_slug() {
        assert_eq!(
            infer_binance_symbol("btc-updown-5m-1770000000").as_deref(),
            Some("BTCUSDT")
        );
        assert_eq!(
            infer_binance_symbol("eth-updown-15m-1770000000").as_deref(),
            Some("ETHUSDT")
        );
        assert!(infer_binance_symbol("election-market").is_none());
    }

    #[test]
    fn compute_offsets_returns_positive_spread() {
        let fit = IntensityFitSnapshot {
            a: 0.3,
            k: 0.4,
            quality: FitQuality::Ready,
        };
        let offsets = compute_optimal_offsets(0.2, 2e-6, 150.0, fit, 0.1, 0.1, 5.0, 15.0, 0.01);
        assert!(offsets.half_spread_base.is_finite());
        assert!(offsets.half_spread_base > 0.0);
        assert!(offsets.half_spread_base < 0.2);
    }

    #[test]
    fn compute_offsets_stays_in_price_scale_for_bootstrap_fit() {
        let fit = IntensityFitSnapshot {
            a: 0.20,
            k: 0.50,
            quality: FitQuality::Warm,
        };
        let offsets = compute_optimal_offsets(0.0, 2e-6, 300.0, fit, 0.1, 0.1, 5.0, 15.0, 0.01);
        assert!(offsets.half_spread_base.is_finite());
        assert!(
            offsets.half_spread_base < 0.08,
            "unexpectedly wide half spread: {}",
            offsets.half_spread_base
        );
    }

    #[test]
    fn live_basis_soft_clip_caps_extreme_raw_basis() {
        let clipped = soft_clip(0.35, GLFT_BASIS_LIVE_SOFT_CAP_ABS);
        assert!(clipped < GLFT_BASIS_LIVE_SOFT_CAP_ABS);
        assert!(clipped > 0.15);
    }

    #[test]
    fn basis_step_towards_limits_single_update_jump() {
        let next = step_towards(0.00, 0.20, 0.03);
        assert!((next - 0.03).abs() < 1e-9);
    }

    #[test]
    fn drift_mode_uses_hysteresis_thresholds() {
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Normal, 6.5),
            DriftMode::Damped
        );
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Damped, 4.9),
            DriftMode::Normal
        );
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Damped, 10.5),
            DriftMode::Frozen
        );
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Frozen, 7.9),
            DriftMode::Damped
        );
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Frozen, 12.5),
            DriftMode::Paused
        );
        assert_eq!(
            drift_mode_with_hysteresis(DriftMode::Paused, 9.9),
            DriftMode::Frozen
        );
    }

    #[test]
    fn frozen_basis_step_accepts_only_drift_reducing_candidate() {
        let tick = 0.01;
        let accepted = frozen_basis_step(0.12, 0.00, 0.015, 0.50, 0.56, tick);
        assert!(
            accepted < 0.12,
            "candidate should be accepted when it reduces absolute drift"
        );

        let rejected = frozen_basis_step(0.02, 0.12, 0.015, 0.50, 0.52, tick);
        assert!(
            (rejected - 0.02).abs() < 1e-9,
            "candidate should be rejected when it increases absolute drift"
        );
    }

    #[test]
    fn asymmetric_basis_limiter_moves_slower_on_worsening_than_recovery() {
        let tick = 0.01;
        let worsen = basis_step_with_asymmetric_limiter(
            0.20,
            0.40,
            0.03,
            DriftMode::Frozen,
            0.50,
            0.52,
            tick,
        );
        let recover = basis_step_with_asymmetric_limiter(
            0.20,
            0.00,
            0.03,
            DriftMode::Frozen,
            0.50,
            0.52,
            tick,
        );
        let worsen_step = (worsen - 0.20).abs();
        let recover_step = (recover - 0.20).abs();
        assert!(
            worsen_step >= 0.006 - 1e-9,
            "worsening path should keep floor step, got {:.6}",
            worsen_step
        );
        assert!(
            recover_step > worsen_step,
            "recovery step should be faster (recover={:.6}, worsen={:.6})",
            recover_step,
            worsen_step
        );
    }

    #[test]
    fn quote_shaping_penalizes_dominant_side_buy_only() {
        let shaped = shape_glft_quotes(0.62, 0.01, DriftMode::Normal, 0.0, 0.01, 0.0);
        assert_eq!(shaped.dominant_side, Some(Side::Yes));
        assert_eq!(shaped.dominant_buy_penalty_ticks, 2.0);
        assert!(shaped.yes_buy_ceiling < 0.61);
        assert!((shaped.no_buy_ceiling - 0.37).abs() < 1e-9);

        let damped_low = shape_glft_quotes(0.82, 0.01, DriftMode::Damped, 7.9, 0.01, 0.0);
        assert!(!damped_low.suppress_yes_buy);

        let damped_high = shape_glft_quotes(0.82, 0.01, DriftMode::Damped, 8.1, 0.01, 0.0);
        assert!(damped_high.suppress_yes_buy);

        let frozen = shape_glft_quotes(0.82, 0.01, DriftMode::Frozen, 9.0, 0.01, 0.0);
        assert_eq!(frozen.dominant_side, Some(Side::Yes));
        assert_eq!(frozen.dominant_buy_penalty_ticks, 6.0);
        assert!(frozen.suppress_yes_buy);
        assert!(!frozen.suppress_no_buy);
    }

    #[test]
    fn validated_warm_start_can_become_ready_without_live_fit() {
        let mut engine = test_engine(
            "TESTREADY_ACCEPT",
            Some(GlftBootstrapSnapshot {
                fit_a: 0.8,
                fit_k: 0.6,
                sigma_prob: BOOTSTRAP_SIGMA,
                basis_prob: 0.0,
                saved_at: now_unix(),
            }),
        );
        prime_ready_inputs(&mut engine);
        engine.maybe_validate_warm_start();

        let gate = engine.readiness_gate(Instant::now());
        assert_eq!(gate.warm_start_status, WarmStartStatus::Accepted);
        assert_eq!(gate.fit_status, GlftFitStatus::Provisional);
        assert!(gate.ready);
        assert_eq!(gate.signal_state, GlftSignalState::Live);
    }

    #[test]
    fn warm_start_soft_accepts_and_resets_basis_to_live_observation() {
        let mut engine = test_engine(
            "TESTREADY_SOFT_ACCEPT",
            Some(GlftBootstrapSnapshot {
                fit_a: 0.8,
                fit_k: 0.6,
                sigma_prob: BOOTSTRAP_SIGMA,
                basis_prob: 0.08,
                saved_at: now_unix(),
            }),
        );
        prime_ready_inputs(&mut engine);
        // Keep snapshot basis in place so this test actually exercises soft-accept.
        engine.basis_raw = 0.08;
        engine.basis_prob = 0.08;
        // Keep basis delta between strict (6 ticks) and soft (10 ticks) gates.
        engine.book = LocalBook {
            yes_bid: 0.49,
            yes_ask: 0.50,
            no_bid: 0.50,
            no_ask: 0.51,
        };
        engine.maybe_validate_warm_start();

        let gate = engine.readiness_gate(Instant::now());
        assert_eq!(gate.warm_start_status, WarmStartStatus::Accepted);
        assert_eq!(gate.fit_status, GlftFitStatus::Provisional);
        assert!(gate.ready);
        assert!(
            engine.basis_prob.abs() < 0.02,
            "soft accept should reset basis close to live observation, got {:.4}",
            engine.basis_prob
        );
    }

    #[test]
    fn fresh_warm_start_bridge_accepts_when_basis_gap_between_soft_and_bridge_caps() {
        let mut engine = test_engine(
            "TESTREADY_BRIDGE_ACCEPT",
            Some(GlftBootstrapSnapshot {
                fit_a: 0.9,
                fit_k: 0.7,
                sigma_prob: BOOTSTRAP_SIGMA,
                basis_prob: 0.08,
                saved_at: now_unix(),
            }),
        );
        prime_ready_inputs(&mut engine);
        // Keep snapshot basis in place so this test actually exercises bridge-accept.
        engine.basis_raw = 0.08;
        engine.basis_prob = 0.08;
        // Force basis delta above soft (10 ticks) but below bridge cap (16 ticks).
        // modeled_mid ~= 0.58, synthetic ~= 0.70 => delta ~= 0.12.
        engine.book = LocalBook {
            yes_bid: 0.69,
            yes_ask: 0.71,
            no_bid: 0.29,
            no_ask: 0.31,
        };
        engine.maybe_validate_warm_start();

        let gate = engine.readiness_gate(Instant::now());
        assert_eq!(gate.warm_start_status, WarmStartStatus::Accepted);
        assert_eq!(gate.fit_status, GlftFitStatus::Provisional);
        assert!(gate.ready);
    }

    #[test]
    fn invalid_warm_start_is_rejected_and_waits_for_live_fit() {
        let mut engine = test_engine(
            "TESTREADY_REJECT",
            Some(GlftBootstrapSnapshot {
                fit_a: 0.8,
                fit_k: 0.6,
                sigma_prob: BOOTSTRAP_SIGMA,
                basis_prob: 0.18,
                saved_at: now_unix(),
            }),
        );
        prime_ready_inputs(&mut engine);
        engine.basis_raw = 0.18;
        engine.basis_prob = 0.18;
        engine.maybe_validate_warm_start();

        let gate = engine.readiness_gate(Instant::now());
        assert_eq!(gate.warm_start_status, WarmStartStatus::Rejected);
        assert_eq!(gate.fit_status, GlftFitStatus::Bootstrap);
        assert!(!gate.ready);
        assert!(gate.blockers.await_fit);
    }

    #[test]
    fn rejected_warm_start_fallback_ready_after_wait_when_stable() {
        let mut engine = test_engine(
            "TESTREADY_REJECT_FALLBACK",
            Some(GlftBootstrapSnapshot {
                fit_a: 0.8,
                fit_k: 0.6,
                sigma_prob: BOOTSTRAP_SIGMA,
                basis_prob: 0.18,
                saved_at: now_unix(),
            }),
        );
        prime_ready_inputs(&mut engine);
        engine.basis_raw = 0.18;
        engine.basis_prob = 0.18;
        engine.maybe_validate_warm_start();
        assert_eq!(engine.warm_start_status(), WarmStartStatus::Rejected);

        // Fallback only activates after its dedicated wait and stable feeds.
        engine.started_at =
            Instant::now() - Duration::from_secs(GLFT_REJECTED_WARM_FALLBACK_WAIT_SECS + 1);
        let gate = engine.readiness_gate(Instant::now());
        assert!(gate.ready);
        assert_eq!(gate.signal_state, GlftSignalState::Live);
        assert_eq!(gate.fit_status, GlftFitStatus::Provisional);
        assert!(!gate.blockers.await_fit);
    }

    #[test]
    fn cold_start_requires_live_fit_before_ready() {
        let mut engine = test_engine("TESTREADY_COLD", None);
        prime_ready_inputs(&mut engine);

        let cold_gate = engine.readiness_gate(Instant::now());
        assert_eq!(cold_gate.warm_start_status, WarmStartStatus::Missing);
        assert_eq!(cold_gate.fit_status, GlftFitStatus::Bootstrap);
        assert!(!cold_gate.ready);
        assert!(cold_gate.blockers.await_fit);
        assert_eq!(cold_gate.signal_state, GlftSignalState::Assimilating);

        engine.last_live_fit = Some(IntensityFitSnapshot {
            a: 0.9,
            k: 0.7,
            quality: FitQuality::Ready,
        });
        let live_gate = engine.readiness_gate(Instant::now());
        assert_eq!(live_gate.fit_status, GlftFitStatus::LiveReady);
        assert!(live_gate.ready);
        assert_eq!(live_gate.signal_state, GlftSignalState::Live);
    }

    #[test]
    fn latched_live_ignores_soft_readiness_blockers() {
        let mut engine = test_engine("TESTREADY_LATCH_SOFT", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.last_live_fit = None;
        // Keep basis drift above soft threshold (6 ticks) but below hard block (12 ticks).
        engine.basis_raw = 0.10;
        engine.basis_prob = 0.10;

        let gate = engine.readiness_gate(Instant::now());
        assert!(gate.ready);
        assert_eq!(gate.signal_state, GlftSignalState::Live);
        assert!(!gate.blockers.await_fit);
        assert!(!gate.blockers.basis_unstable);
        assert!(!gate.blockers.sigma_unstable);
    }

    #[test]
    fn latched_live_still_blocks_on_stale_binance() {
        let mut engine = test_engine("TESTREADY_LATCH_HARD", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.last_binance_tick = Some(BinanceTick {
            price: 100.0,
            ts: Instant::now()
                - Duration::from_secs(BINANCE_STALE_SECS + BINANCE_STALE_GRACE_SECS + 2),
        });

        let gate = engine.readiness_gate(Instant::now());
        assert!(!gate.ready);
        assert_eq!(gate.signal_state, GlftSignalState::Assimilating);
        assert!(gate.blockers.await_binance);
    }

    #[test]
    fn latched_live_blocks_on_hard_basis_misalignment() {
        let mut engine = test_engine("TESTREADY_LATCH_BASIS_HARD", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.last_live_fit = Some(IntensityFitSnapshot {
            a: 1.0,
            k: 1.0,
            quality: FitQuality::Ready,
        });
        // Force modeled_mid to diverge beyond hard threshold (>12 ticks).
        engine.basis_raw = 0.20;
        engine.basis_prob = 0.20;

        let gate = engine.readiness_gate(Instant::now());
        assert!(!gate.ready);
        assert!(gate.hard_basis_unstable);
        assert!(gate.blockers.basis_unstable);
    }

    #[test]
    fn latched_live_endgame_relaxes_moderate_pause_drift_when_feed_fresh() {
        let mut engine = test_engine("TESTREADY_ENDGAME_RELAX", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.last_live_fit = Some(IntensityFitSnapshot {
            a: 1.0,
            k: 1.0,
            quality: FitQuality::Ready,
        });
        engine.cfg.market_end_ts = now_unix() + 40;
        engine.last_binance_tick = Some(BinanceTick {
            price: 100.0,
            ts: Instant::now(),
        });
        // ~14 ticks drift: enters paused mode, but should be tolerated near round end
        // when feed freshness is good.
        engine.basis_raw = 0.14;
        engine.basis_prob = 0.14;

        let gate = engine.readiness_gate(Instant::now());
        assert!(matches!(gate.drift_mode, DriftMode::Paused));
        assert!(gate.ready);
        assert!(!gate.hard_basis_unstable);
    }

    #[test]
    fn publish_arms_pause_recovery_state_on_hard_misalignment() {
        let mut engine = test_engine("TESTREADY_PUBLISH_ARM", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.drift_mode = DriftMode::Frozen;
        engine.hard_basis_blocked = false;
        engine.pause_entered_at = None;
        engine.pause_recover_healthy_streak = 0;
        engine.basis_raw = 0.20;
        engine.basis_prob = 0.20;

        let _ = engine.publish();
        assert!(engine.hard_basis_blocked);
        assert!(engine.pause_entered_at.is_some());
        assert_eq!(engine.pause_recover_healthy_streak, 0);
    }

    #[test]
    fn publish_does_not_resume_block_when_gate_not_ready() {
        let mut engine = test_engine("TESTREADY_PUBLISH_RESUME_GUARD", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.drift_mode = DriftMode::Normal;
        engine.hard_basis_blocked = true;
        engine.pause_entered_at =
            Some(Instant::now() - Duration::from_millis(GLFT_PAUSE_RECOVER_MIN_DWELL_MS + 50));
        engine.pause_recover_healthy_streak = GLFT_PAUSE_RECOVER_HEALTHY_STREAK;
        engine.last_live_fit = Some(IntensityFitSnapshot {
            a: 1.0,
            k: 1.0,
            quality: FitQuality::Ready,
        });
        engine.last_binance_tick = Some(BinanceTick {
            price: 100.0,
            ts: Instant::now()
                - Duration::from_secs(BINANCE_STALE_SECS + BINANCE_STALE_GRACE_SECS + 2),
        });
        engine.basis_raw = 0.0;
        engine.basis_prob = 0.0;

        let snapshot = engine.publish();
        assert!(!snapshot.ready);
        assert!(engine.hard_basis_blocked);
    }

    #[test]
    fn latched_live_pause_recovery_needs_dwell_and_streak() {
        let mut engine = test_engine("TESTREADY_LATCH_RECOVER_GATE", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.last_live_fit = Some(IntensityFitSnapshot {
            a: 1.0,
            k: 1.0,
            quality: FitQuality::Ready,
        });
        engine.drift_mode = DriftMode::Paused;
        engine.basis_raw = 0.0;
        engine.basis_prob = 0.0;
        engine.pause_entered_at = Some(Instant::now());
        engine.pause_recover_healthy_streak = GLFT_PAUSE_RECOVER_HEALTHY_STREAK;

        let gate_no_dwell = engine.readiness_gate(Instant::now());
        assert!(!gate_no_dwell.ready);
        assert!(gate_no_dwell.hard_basis_unstable);

        engine.pause_entered_at =
            Some(Instant::now() - Duration::from_millis(GLFT_PAUSE_RECOVER_MIN_DWELL_MS + 10));
        engine.pause_recover_healthy_streak = GLFT_PAUSE_RECOVER_HEALTHY_STREAK - 1;
        let gate_no_streak = engine.readiness_gate(Instant::now());
        assert!(!gate_no_streak.ready);
        assert!(gate_no_streak.hard_basis_unstable);

        engine.pause_recover_healthy_streak = GLFT_PAUSE_RECOVER_HEALTHY_STREAK;
        let gate_recovered = engine.readiness_gate(Instant::now());
        assert!(gate_recovered.ready);
        assert!(!gate_recovered.hard_basis_unstable);
    }

    #[test]
    fn pause_recovery_streak_is_capped_to_threshold() {
        let mut engine = test_engine("TESTREADY_RECOVER_STREAK_CAP", None);
        prime_ready_inputs(&mut engine);
        engine.live_latched = true;
        engine.pause_entered_at = Some(Instant::now() - Duration::from_secs(2));
        engine.pause_recover_healthy_streak = GLFT_PAUSE_RECOVER_HEALTHY_STREAK;
        engine.drift_mode = DriftMode::Damped;
        engine.update_drift_mode(DriftMode::Damped, 7.0, 0.5, 0.43);
        assert_eq!(
            engine.pause_recover_healthy_streak,
            GLFT_PAUSE_RECOVER_HEALTHY_STREAK
        );
    }

    #[test]
    fn glft_alpha_shift_is_capped_by_fit_status() {
        let shift_bootstrap = compute_glft_alpha_shift(-1.0, 0.30, 0.01, GlftFitStatus::Bootstrap);
        let shift_live = compute_glft_alpha_shift(-1.0, 0.30, 0.01, GlftFitStatus::LiveReady);
        assert!((shift_bootstrap + 0.04).abs() < 1e-9);
        assert!((shift_live + 0.08).abs() < 1e-9);
    }
}
