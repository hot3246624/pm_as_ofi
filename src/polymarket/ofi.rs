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

use super::messages::{MarketDataMsg, OfiSnapshot, SideOfi, TakerSide};
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
    /// Default: 50.0 (placeholder — calibrate with DRY-RUN data).
    pub toxicity_threshold: f64,

    /// Heartbeat interval in milliseconds for evicting expired trades
    /// even when no new trades arrive. Default: 200.
    pub heartbeat_ms: u64,
}

impl Default for OfiConfig {
    fn default() -> Self {
        Self {
            window_duration: Duration::from_secs(3),
            toxicity_threshold: 50.0,
            heartbeat_ms: 200,
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
}

impl OfiEngine {
    pub fn new(
        cfg: OfiConfig,
        md_rx: mpsc::Receiver<MarketDataMsg>,
        snapshot_tx: watch::Sender<OfiSnapshot>,
    ) -> Self {
        Self {
            cfg,
            yes_window: SideWindow::new(),
            no_window: SideWindow::new(),
            md_rx,
            snapshot_tx,
        }
    }

    /// Actor main loop.
    pub async fn run(mut self) {
        info!(
            "🔬 OFI Engine started | window={}ms threshold={:.1} heartbeat={}ms",
            self.cfg.window_duration.as_millis(),
            self.cfg.toxicity_threshold,
            self.cfg.heartbeat_ms,
        );

        let mut ticker = tokio::time::interval(Duration::from_millis(self.cfg.heartbeat_ms));
        let mut was_yes_toxic = false;
        let mut was_no_toxic = false;

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

            // Compute per-side snapshots
            let yes_ofi = self.yes_window.compute(self.cfg.toxicity_threshold);
            let no_ofi = self.no_window.compute(self.cfg.toxicity_threshold);

            let snapshot = OfiSnapshot {
                yes: yes_ofi,
                no: no_ofi,
                ts: now,
            };

            let _ = self.snapshot_tx.send(snapshot);

            // Edge-triggered logging for toxicity (prevent spam on 200ms tick)
            if yes_ofi.is_toxic && !was_yes_toxic {
                warn!(
                    "☠️ YES entered toxicity! OFI={:.1} (buy={:.1} sell={:.1})",
                    yes_ofi.ofi_score, yes_ofi.buy_volume, yes_ofi.sell_volume,
                );
            } else if !yes_ofi.is_toxic && was_yes_toxic {
                info!("✅ YES flow recovered (OFI={:.1})", yes_ofi.ofi_score);
            }
            was_yes_toxic = yes_ofi.is_toxic;

            if no_ofi.is_toxic && !was_no_toxic {
                warn!(
                    "☠️ NO entered toxicity! OFI={:.1} (buy={:.1} sell={:.1})",
                    no_ofi.ofi_score, no_ofi.buy_volume, no_ofi.sell_volume,
                );
            } else if !no_ofi.is_toxic && was_no_toxic {
                info!("✅ NO flow recovered (OFI={:.1})", no_ofi.ofi_score);
            }
            was_no_toxic = no_ofi.is_toxic;
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

    fn make_engine() -> OfiEngine {
        let cfg = OfiConfig {
            window_duration: Duration::from_secs(3),
            toxicity_threshold: 10.0,
            heartbeat_ms: 200,
        };
        let (_tx, rx) = mpsc::channel(16);
        let (snap_tx, _snap_rx) = watch::channel(OfiSnapshot::default());
        OfiEngine::new(cfg, rx, snap_tx)
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
    async fn test_toxicity_timeout_recovery() {
        let cfg = OfiConfig {
            window_duration: Duration::from_millis(50),
            toxicity_threshold: 10.0,
            heartbeat_ms: 10,
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
}
