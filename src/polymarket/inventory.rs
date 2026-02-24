//! Inventory Manager Actor.
//!
//! Tracks real-time position state (YES/NO quantities, average costs)
//! and broadcasts snapshots via a `watch` channel for the Coordinator to read.

use tokio::sync::{mpsc, watch};
use tracing::info;

use super::messages::{FillEvent, FillStatus, InventoryState};
use super::types::Side;

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

/// Inventory constraints. All values configurable at startup.
#[derive(Debug, Clone)]
pub struct InventoryConfig {
    /// Maximum absolute net directional exposure (|YES - NO|).
    /// Default: 10 shares.
    pub max_net_diff: f64,

    /// Maximum portfolio cost (yes_avg + no_avg).
    /// Must stay < 1.0 for guaranteed profit on resolution.
    /// Default: 1.02 (2% slack for fees).
    pub max_portfolio_cost: f64,

    /// Maximum dollar value of position on a single side.
    /// Default: $5.
    pub max_position_value: f64,
}

impl Default for InventoryConfig {
    fn default() -> Self {
        Self {
            max_net_diff: 10.0,
            max_portfolio_cost: 1.02,
            max_position_value: 5.0,
        }
    }
}

impl InventoryConfig {
    /// Load overrides from environment variables (if set).
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("PM_MAX_NET_DIFF") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.max_net_diff = f;
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_PORTFOLIO_COST") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.max_portfolio_cost = f;
            }
        }
        if let Ok(v) = std::env::var("PM_MAX_POSITION_VALUE") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.max_position_value = f;
            }
        }
        cfg
    }
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

/// Inventory Manager: receives fill events, maintains position state,
/// broadcasts latest state via `watch` channel.
pub struct InventoryManager {
    cfg: InventoryConfig,
    state: InventoryState,
    fill_rx: mpsc::Receiver<FillEvent>,
    state_tx: watch::Sender<InventoryState>,
}

impl InventoryManager {
    pub fn new(
        cfg: InventoryConfig,
        fill_rx: mpsc::Receiver<FillEvent>,
        state_tx: watch::Sender<InventoryState>,
    ) -> Self {
        let state = InventoryState::default();
        Self {
            cfg,
            state,
            fill_rx,
            state_tx,
        }
    }

    /// Actor main loop. Runs until the fill channel is closed.
    pub async fn run(mut self) {
        info!(
            "📦 InventoryManager started | max_net_diff={:.0} max_cost={:.3} max_val=${:.0}",
            self.cfg.max_net_diff, self.cfg.max_portfolio_cost, self.cfg.max_position_value,
        );

        while let Some(fill) = self.fill_rx.recv().await {
            self.apply_fill(&fill);

            // Broadcast updated state (non-blocking, overwrites previous)
            let _ = self.state_tx.send(self.state);

            info!(
                "📦 Fill: {:?} {:.2}@{:.3} status={:?} id={} → YES={:.1}@{:.4} NO={:.1}@{:.4} | net={:.1} cost={:.4}",
                fill.side, fill.filled_size, fill.price, fill.status, &fill.order_id[..8.min(fill.order_id.len())],
                self.state.yes_qty, self.state.yes_avg_cost,
                self.state.no_qty, self.state.no_avg_cost,
                self.state.net_diff, self.state.portfolio_cost,
            );
        }

        info!("📦 InventoryManager shutting down (channel closed)");
    }

    /// Apply a fill to the position using VWAP for average cost.
    /// FillStatus::Failed causes inventory REVERSAL (subtraction).
    fn apply_fill(&mut self, fill: &FillEvent) {
        let multiplier = match fill.status {
            FillStatus::Matched | FillStatus::Confirmed => 1.0,
            FillStatus::Failed => -1.0,  // Reverse: subtract from inventory
        };
        let delta = fill.filled_size * multiplier;

        match fill.side {
            Side::Yes => {
                let old_q = self.state.yes_qty;
                let old_avg = self.state.yes_avg_cost;
                self.state.yes_qty = (old_q + delta).max(0.0);
                if delta > 0.0 && self.state.yes_qty > 0.0 {
                    // VWAP: blend old and new
                    self.state.yes_avg_cost =
                        (old_q * old_avg + fill.filled_size * fill.price) / self.state.yes_qty;
                }
                // On reversal, keep avg cost (or reset if qty hits 0)
                if self.state.yes_qty < f64::EPSILON {
                    self.state.yes_avg_cost = 0.0;
                }
            }
            Side::No => {
                let old_q = self.state.no_qty;
                let old_avg = self.state.no_avg_cost;
                self.state.no_qty = (old_q + delta).max(0.0);
                if delta > 0.0 && self.state.no_qty > 0.0 {
                    self.state.no_avg_cost =
                        (old_q * old_avg + fill.filled_size * fill.price) / self.state.no_qty;
                }
                if self.state.no_qty < f64::EPSILON {
                    self.state.no_avg_cost = 0.0;
                }
            }
        }

        // Recompute derived fields
        self.state.net_diff = self.state.yes_qty - self.state.no_qty;
        self.state.portfolio_cost = if self.state.yes_qty > 0.0 && self.state.no_qty > 0.0 {
            self.state.yes_avg_cost + self.state.no_avg_cost
        } else {
            0.0 // Not a complete pair yet
        };

        // FIX #5: Compute can_open and include in broadcast state
        self.state.can_open = self.can_open();
    }

    /// Check whether current inventory allows opening new positions.
    /// Checks three independent limits:
    ///   1. net_diff < max_net_diff  (imbalance limit)
    ///   2. portfolio_cost < max_portfolio_cost  (pair cost limit)
    ///   3. single-side value < max_position_value  (dollar exposure limit)
    pub fn can_open(&self) -> bool {
        let net_ok = self.state.net_diff.abs() < self.cfg.max_net_diff;
        let cost_ok = self.state.portfolio_cost < self.cfg.max_portfolio_cost
            || self.state.portfolio_cost == 0.0;
        let yes_value = self.state.yes_qty * self.state.yes_avg_cost;
        let no_value = self.state.no_qty * self.state.no_avg_cost;
        let value_ok = yes_value < self.cfg.max_position_value
            && no_value < self.cfg.max_position_value;
        net_ok && cost_ok && value_ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn make_fill(side: Side, size: f64, price: f64) -> FillEvent {
        FillEvent {
            order_id: "test-order".to_string(),
            side,
            filled_size: size,
            price,
            status: FillStatus::Matched,
            ts: Instant::now(),
        }
    }

    fn make_failed_fill(side: Side, size: f64, price: f64) -> FillEvent {
        FillEvent {
            order_id: "test-order-fail".to_string(),
            side,
            filled_size: size,
            price,
            status: FillStatus::Failed,
            ts: Instant::now(),
        }
    }

    #[test]
    fn test_single_side_fill() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        im.apply_fill(&make_fill(Side::Yes, 10.0, 0.50));
        assert!((im.state.yes_qty - 10.0).abs() < 1e-9);
        assert!((im.state.yes_avg_cost - 0.50).abs() < 1e-9);
        assert!((im.state.net_diff - 10.0).abs() < 1e-9);
        assert!((im.state.portfolio_cost - 0.0).abs() < 1e-9); // no pair yet
    }

    #[test]
    fn test_pair_fill() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        im.apply_fill(&make_fill(Side::Yes, 5.0, 0.48));
        im.apply_fill(&make_fill(Side::No, 5.0, 0.49));

        assert!((im.state.net_diff - 0.0).abs() < 1e-9);
        assert!((im.state.portfolio_cost - 0.97).abs() < 1e-9); // 0.48+0.49
        assert!(im.can_open());
    }

    #[test]
    fn test_vwap_averaging() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        im.apply_fill(&make_fill(Side::Yes, 10.0, 0.50));
        im.apply_fill(&make_fill(Side::Yes, 10.0, 0.52));

        assert!((im.state.yes_qty - 20.0).abs() < 1e-9);
        // VWAP = (10*0.50 + 10*0.52) / 20 = 0.51
        assert!((im.state.yes_avg_cost - 0.51).abs() < 1e-9);
    }

    #[test]
    fn test_inventory_constraint() {
        let cfg = InventoryConfig {
            max_net_diff: 5.0,
            ..Default::default()
        };
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(cfg, fill_rx, state_tx);

        im.apply_fill(&make_fill(Side::Yes, 6.0, 0.50));
        assert!(!im.can_open()); // net_diff=6 > max=5
    }

    #[test]
    fn test_failed_fill_reversal() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        // Fill 10 YES
        im.apply_fill(&make_fill(Side::Yes, 10.0, 0.50));
        assert!((im.state.yes_qty - 10.0).abs() < 1e-9);

        // Failed: reverse 5
        im.apply_fill(&make_failed_fill(Side::Yes, 5.0, 0.50));
        assert!((im.state.yes_qty - 5.0).abs() < 1e-9);
        assert!((im.state.net_diff - 5.0).abs() < 1e-9);
    }
}
