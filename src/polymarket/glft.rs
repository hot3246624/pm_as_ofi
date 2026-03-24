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
const SIGMA_HALF_LIFE_SECS: f64 = 20.0;
const BINANCE_STALE_SECS: u64 = 3;
const BOOTSTRAP_A: f64 = 0.20;
const BOOTSTRAP_K: f64 = 0.50;
const BOOTSTRAP_SIGMA: f64 = 0.02;
const BOOTSTRAP_BASIS: f64 = 0.0;
const SNAPSHOT_TTL_SECS: u64 = 6 * 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FitQuality {
    #[default]
    Warm,
    Ready,
    Invalid,
}

#[derive(Debug, Clone, Copy)]
pub struct GlftSignalSnapshot {
    pub anchor_prob: f64,
    pub basis_prob: f64,
    pub alpha_flow: f64,
    pub sigma_prob: f64,
    pub tau_norm: f64,
    pub fit_a: f64,
    pub fit_k: f64,
    pub fit_quality: FitQuality,
    pub ready: bool,
    pub stale: bool,
}

impl Default for GlftSignalSnapshot {
    fn default() -> Self {
        Self {
            anchor_prob: 0.5,
            basis_prob: 0.0,
            alpha_flow: 0.0,
            sigma_prob: BOOTSTRAP_SIGMA,
            tau_norm: 0.0,
            fit_a: BOOTSTRAP_A,
            fit_k: BOOTSTRAP_K,
            fit_quality: FitQuality::Warm,
            ready: false,
            stale: true,
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
    book: LocalBook,
    flow_events: VecDeque<FlowEvent>,
    buy_impacts: VecDeque<TradeImpact>,
    sell_impacts: VecDeque<TradeImpact>,
    last_binance_tick: Option<BinanceTick>,
    round_open_binance: Option<f64>,
    last_log_price: Option<f64>,
    sigma_prob: f64,
    basis_prob: f64,
    fit: IntensityFitSnapshot,
    last_good_fit: Option<IntensityFitSnapshot>,
}

impl GlftSignalEngine {
    pub fn new(
        cfg: GlftRuntimeConfig,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        tx: watch::Sender<GlftSignalSnapshot>,
    ) -> Self {
        let bootstrap = load_bootstrap_snapshot(&cfg);
        let fit = IntensityFitSnapshot {
            a: bootstrap.map(|s| s.fit_a).unwrap_or(BOOTSTRAP_A),
            k: bootstrap.map(|s| s.fit_k).unwrap_or(BOOTSTRAP_K),
            quality: FitQuality::Warm,
        };
        Self {
            cfg,
            md_rx,
            tx,
            book: LocalBook::default(),
            flow_events: VecDeque::new(),
            buy_impacts: VecDeque::new(),
            sell_impacts: VecDeque::new(),
            last_binance_tick: None,
            round_open_binance: None,
            last_log_price: None,
            sigma_prob: bootstrap.map(|s| s.sigma_prob).unwrap_or(BOOTSTRAP_SIGMA),
            basis_prob: bootstrap.map(|s| s.basis_prob).unwrap_or(BOOTSTRAP_BASIS),
            fit,
            last_good_fit: None,
        }
    }

    pub async fn run(mut self) {
        let (binance_tx, mut binance_rx) = mpsc::channel::<BinanceTick>(256);
        let symbol = self.cfg.symbol.clone();
        tokio::spawn(async move {
            run_binance_aggtrade_feed(symbol, binance_tx).await;
        });

        let mut refit_tick = tokio::time::interval(self.cfg.refit_interval);
        self.publish();

        loop {
            tokio::select! {
                Some(md) = self.md_rx.recv() => {
                    self.handle_market_data(md);
                    self.publish();
                }
                Some(binance_tick) = binance_rx.recv() => {
                    self.handle_binance_tick(binance_tick);
                    self.publish();
                }
                _ = refit_tick.tick() => {
                    self.refit_intensity();
                    self.publish();
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
                self.book = LocalBook {
                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,
                };
                if let Some(anchor_prob) = self.anchor_prob() {
                    if let Some(poly_mid) = self.poly_yes_mid() {
                        let obs = poly_mid - anchor_prob;
                        self.basis_prob = ewma_update(self.basis_prob, obs, BASIS_HALF_LIFE_SECS);
                    }
                }
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
            }
        }
    }

    fn handle_binance_tick(&mut self, tick: BinanceTick) {
        if self.round_open_binance.is_none() {
            self.round_open_binance = Some(tick.price);
        }
        if let Some(prev_log_price) = self.last_log_price {
            let log_price = tick.price.ln();
            let ret = log_price - prev_log_price;
            let next_sigma = ewma_update(self.sigma_prob, ret * ret, SIGMA_HALF_LIFE_SECS);
            self.sigma_prob = step_cap(next_sigma.max(1e-8), self.sigma_prob.max(1e-8), 0.25);
            self.last_log_price = Some(log_price);
        } else {
            self.last_log_price = Some(tick.price.ln());
        }
        self.last_binance_tick = Some(tick);
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

    fn refit_intensity(&mut self) {
        let now = Instant::now();
        self.prune_windows(now);

        let trade_count = self.buy_impacts.len() + self.sell_impacts.len();
        if trade_count < 20 {
            self.fit.quality = FitQuality::Warm;
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
        if xs.len() < 3 {
            self.fit.quality = FitQuality::Invalid;
            return;
        }

        let Some((a, k, r2)) = fit_exponential(&xs, &ys) else {
            self.fit.quality = FitQuality::Invalid;
            return;
        };
        if !(a.is_finite() && k.is_finite() && a > 0.0 && k > 0.0 && r2 >= 0.60) {
            self.fit.quality = FitQuality::Invalid;
            return;
        }

        let candidate = if let Some(last_good) = self.last_good_fit {
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
        self.last_good_fit = Some(candidate);
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

    fn publish(&self) {
        let anchor_prob = self.anchor_prob().unwrap_or(0.5);
        let alpha_flow = self.alpha_flow();
        let remaining_secs = self.cfg.market_end_ts.saturating_sub(now_unix());
        let tau_norm =
            (remaining_secs as f64 / self.cfg.total_round_secs.max(1) as f64).clamp(0.0, 1.0);
        let stale = self
            .last_binance_tick
            .map(|tick| tick.ts.elapsed() > Duration::from_secs(BINANCE_STALE_SECS))
            .unwrap_or(true);
        let mut fit = self.last_good_fit.unwrap_or(self.fit);
        if self.last_good_fit.is_none() && matches!(fit.quality, FitQuality::Invalid) {
            // No reliable fit yet: keep quoting with bootstrap/warm-start shape
            // instead of going fully silent for an entire round.
            fit.quality = FitQuality::Warm;
        }
        let ready = !stale;

        let _ = self.tx.send(GlftSignalSnapshot {
            anchor_prob,
            basis_prob: self.basis_prob,
            alpha_flow,
            sigma_prob: self.sigma_prob,
            tau_norm,
            fit_a: fit.a,
            fit_k: fit.k,
            fit_quality: fit.quality,
            ready,
            stale,
        });
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

pub fn compute_optimal_offsets(
    q_norm: f64,
    sigma_prob: f64,
    tau_norm: f64,
    fit: IntensityFitSnapshot,
    gamma: f64,
    xi: f64,
    bid_size: f64,
    max_net_diff: f64,
    tick_size: f64,
) -> OptimalOffsets {
    let delta_q = (bid_size / max_net_diff.max(1e-9)).max(1e-6);
    let sigma = (sigma_prob.max(1e-8) * tau_norm.max(1e-6)).sqrt();
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
    let c2 = c2_term.max(1e-12).sqrt() * tick;
    OptimalOffsets {
        inventory_shift: q_norm.clamp(-1.0, 1.0) * sigma * c2,
        half_spread_base: c1 + 0.5 * delta_q * sigma * c2,
    }
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
        let offsets = compute_optimal_offsets(0.2, 0.02, 0.5, fit, 0.1, 0.1, 5.0, 15.0, 0.01);
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
        let offsets = compute_optimal_offsets(0.0, 0.02, 1.0, fit, 0.1, 0.1, 5.0, 15.0, 0.01);
        assert!(offsets.half_spread_base.is_finite());
        assert!(
            offsets.half_spread_base < 0.08,
            "unexpectedly wide half spread: {}",
            offsets.half_spread_base
        );
    }
}
