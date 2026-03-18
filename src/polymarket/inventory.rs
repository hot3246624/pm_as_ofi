//! Inventory Manager Actor.
//!
//! Tracks real-time position state (YES/NO quantities, average costs)
//! and broadcasts snapshots via a `watch` channel for the Coordinator to read.

use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use super::messages::{FillEvent, FillStatus, InventoryEvent, InventoryState};
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

    /// Order size to project capacity (fetched from env).
    pub bid_size: f64,
}

impl Default for InventoryConfig {
    fn default() -> Self {
        Self {
            max_net_diff: 10.0,
            max_portfolio_cost: 1.02,
            bid_size: 5.0,
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
                // Note: max_portfolio_cost in InventoryConfig is informational only.
                cfg.max_portfolio_cost = f;
            }
        }
        if let Ok(v) = std::env::var("PM_BID_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.bid_size = f;
            }
        }
        cfg
    }
}

// ─────────────────────────────────────────────────────────
// Actor
// ─────────────────────────────────────────────────────────

/// Individual fill record stored in the ledger for precise VWAP reconstruction.
#[derive(Debug, Clone)]
struct FillRecord {
    order_id: String,
    side: Side,
    size: f64,
    price: f64,
}

/// Inventory Manager: receives fill events, maintains position state,
/// broadcasts latest state via `watch` channel.
///
/// P1 FIX: Uses a fill ledger for exact VWAP reconstruction on reversals,
/// preventing avg_cost drift when Failed fills subtract quantity.
pub struct InventoryManager {
    cfg: InventoryConfig,
    state: InventoryState,
    fill_rx: mpsc::Receiver<InventoryEvent>,
    state_tx: watch::Sender<InventoryState>,
    /// P1 FIX: Ledger of all active (non-reversed) fills for exact VWAP.
    ledger: Vec<FillRecord>,
}

impl InventoryManager {
    pub fn new(
        cfg: InventoryConfig,
        fill_rx: mpsc::Receiver<InventoryEvent>,
        state_tx: watch::Sender<InventoryState>,
    ) -> Self {
        let state = InventoryState::default();
        Self {
            cfg,
            state,
            fill_rx,
            state_tx,
            ledger: Vec::new(),
        }
    }

    /// Actor main loop. Runs until the fill channel is closed.
    pub async fn run(mut self) {
        info!(
            "📦 InventoryManager started | max_net_diff={:.0} max_cost={:.3}",
            self.cfg.max_net_diff, self.cfg.max_portfolio_cost,
        );

        while let Some(event) = self.fill_rx.recv().await {
            match event {
                InventoryEvent::Fill(fill) => {
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
                InventoryEvent::Merge {
                    full_set_size,
                    merge_id,
                    ..
                } => {
                    self.apply_merge(full_set_size, &merge_id);
                    let _ = self.state_tx.send(self.state);
                    info!(
                        "📦 Merge sync: full_set={:.2} id={} → YES={:.1}@{:.4} NO={:.1}@{:.4} | net={:.1} cost={:.4}",
                        full_set_size,
                        &merge_id[..8.min(merge_id.len())],
                        self.state.yes_qty,
                        self.state.yes_avg_cost,
                        self.state.no_qty,
                        self.state.no_avg_cost,
                        self.state.net_diff,
                        self.state.portfolio_cost,
                    );
                }
            }
        }

        info!("📦 InventoryManager shutting down (channel closed)");
    }

    /// Apply a fill using ledger-based VWAP reconstruction.
    /// Matched → add to ledger. Confirmed → idempotent if Matched exists, else record.
    /// Failed → remove from ledger. Then recompute from scratch.
    fn apply_fill(&mut self, fill: &FillEvent) {
        match fill.status {
            FillStatus::Matched => {
                self.ledger.push(FillRecord {
                    order_id: fill.order_id.clone(),
                    side: fill.side,
                    size: fill.filled_size,
                    price: fill.price,
                });
            }
            FillStatus::Confirmed => {
                // Check if we already have a Matched entry for this order+side+size.
                // If yes → idempotent no-op (MATCHED→CONFIRMED normal path).
                // If no → this is a Confirmed-first scenario (e.g. after reconnect),
                //         so we must record it to avoid losing the fill entirely.
                let already_tracked = self.ledger.iter().any(|r| {
                    r.order_id == fill.order_id
                        && r.side == fill.side
                        && (r.size - fill.filled_size).abs() < 1e-6
                });
                if already_tracked {
                    info!(
                        "📦 Confirmed fill for order {}… — already tracked via Matched (no-op)",
                        &fill.order_id[..8.min(fill.order_id.len())]
                    );
                    return; // Skip recompute — nothing changed
                } else {
                    // Confirmed-first: no Matched was seen (likely reconnect replay).
                    // Record it to prevent inventory loss.
                    warn!(
                        "📦 Confirmed-first fill for order {}… — no prior Matched, recording to prevent loss",
                        &fill.order_id[..8.min(fill.order_id.len())]
                    );
                    self.ledger.push(FillRecord {
                        order_id: fill.order_id.clone(),
                        side: fill.side,
                        size: fill.filled_size,
                        price: fill.price,
                    });
                }
            }
            FillStatus::Failed => {
                // Remove the FIRST matching entry for this order_id + side + size
                if let Some(idx) = self.ledger.iter().position(|r| {
                    r.order_id == fill.order_id
                        && r.side == fill.side
                        && (r.size - fill.filled_size).abs() < 1e-6
                }) {
                    self.ledger.remove(idx);
                } else {
                    // Fallback: remove any entry with matching order_id + side
                    if let Some(idx) = self
                        .ledger
                        .iter()
                        .position(|r| r.order_id == fill.order_id && r.side == fill.side)
                    {
                        self.ledger.remove(idx);
                    }
                }
            }
        }

        // Recompute everything from the ledger (zero-drift VWAP)
        self.recompute_from_ledger();
    }

    /// Apply merge-side inventory synchronization.
    ///
    /// Merge consumes one YES and one NO per full-set unit. We model this as
    /// synthetic negative ledger entries at current side VWAP so remaining
    /// average costs stay stable.
    fn apply_merge(&mut self, full_set_size: f64, merge_id: &str) {
        let requested = full_set_size.max(0.0);
        if requested <= f64::EPSILON {
            return;
        }
        let available_full_set = self.state.yes_qty.min(self.state.no_qty).max(0.0);
        let amount = requested.min(available_full_set);
        if amount <= f64::EPSILON {
            return;
        }

        let yes_price = self.state.yes_avg_cost.max(0.0);
        let no_price = self.state.no_avg_cost.max(0.0);
        self.ledger.push(FillRecord {
            order_id: format!("merge:{}:yes", merge_id),
            side: Side::Yes,
            size: -amount,
            price: yes_price,
        });
        self.ledger.push(FillRecord {
            order_id: format!("merge:{}:no", merge_id),
            side: Side::No,
            size: -amount,
            price: no_price,
        });
        self.recompute_from_ledger();
    }

    /// Rebuild position state entirely from the fill ledger.
    /// Guarantees mathematically perfect VWAP at all times.
    fn recompute_from_ledger(&mut self) {
        let (mut yes_qty, mut yes_cost_sum) = (0.0_f64, 0.0_f64);
        let (mut no_qty, mut no_cost_sum) = (0.0_f64, 0.0_f64);

        for r in &self.ledger {
            match r.side {
                Side::Yes => {
                    yes_qty += r.size;
                    yes_cost_sum += r.size * r.price;
                }
                Side::No => {
                    no_qty += r.size;
                    no_cost_sum += r.size * r.price;
                }
            }
        }

        if yes_qty.abs() < 1e-9 {
            yes_qty = 0.0;
            yes_cost_sum = 0.0;
        }
        if no_qty.abs() < 1e-9 {
            no_qty = 0.0;
            no_cost_sum = 0.0;
        }
        if yes_qty < 0.0 {
            warn!(
                "📦 YES qty drifted negative ({:.8}) after ledger rebuild; clamping to 0",
                yes_qty
            );
            yes_qty = 0.0;
            yes_cost_sum = 0.0;
        }
        if no_qty < 0.0 {
            warn!(
                "📦 NO qty drifted negative ({:.8}) after ledger rebuild; clamping to 0",
                no_qty
            );
            no_qty = 0.0;
            no_cost_sum = 0.0;
        }

        self.state.yes_qty = yes_qty;
        self.state.no_qty = no_qty;
        self.state.yes_avg_cost = if yes_qty > f64::EPSILON {
            yes_cost_sum / yes_qty
        } else {
            0.0
        };
        self.state.no_avg_cost = if no_qty > f64::EPSILON {
            no_cost_sum / no_qty
        } else {
            0.0
        };

        // Recompute derived fields
        self.state.net_diff = self.state.yes_qty - self.state.no_qty;
        self.state.portfolio_cost = if self.state.yes_qty > 0.0 && self.state.no_qty > 0.0 {
            self.state.yes_avg_cost + self.state.no_avg_cost
        } else {
            0.0 // Not a complete pair yet
        };
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
        let cfg = InventoryConfig::default();
        let mut im = InventoryManager::new(cfg, fill_rx, state_tx);

        im.apply_fill(&make_fill(Side::Yes, 5.0, 0.48));
        im.apply_fill(&make_fill(Side::No, 5.0, 0.49));

        assert!((im.state.net_diff - 0.0).abs() < 1e-9);
        assert!((im.state.portfolio_cost - 0.97).abs() < 1e-9); // 0.48+0.49
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

        im.apply_fill(&make_fill(Side::Yes, 5.0, 0.50));
        // Gating now happens in Coordinator, not InventoryManager.
        assert!((im.state.net_diff - 5.0).abs() < 1e-9);
    }

    #[test]
    fn test_failed_fill_reversal() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        // Fill 5 YES in two separate orders
        im.apply_fill(&make_fill(Side::Yes, 5.0, 0.50));
        im.apply_fill(&FillEvent {
            order_id: "test-order-2".to_string(),
            side: Side::Yes,
            filled_size: 5.0,
            price: 0.50,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });
        assert!((im.state.yes_qty - 10.0).abs() < 1e-9);

        // Failed: reverse the second order (5 units)
        im.apply_fill(&FillEvent {
            order_id: "test-order-2".to_string(),
            side: Side::Yes,
            filled_size: 5.0,
            price: 0.50,
            status: FillStatus::Failed,
            ts: Instant::now(),
        });
        assert!((im.state.yes_qty - 5.0).abs() < 1e-9);
        assert!((im.state.net_diff - 5.0).abs() < 1e-9);
        // P1 FIX: avg_cost should still be exactly 0.50 after reversal
        assert!((im.state.yes_avg_cost - 0.50).abs() < 1e-9);
    }

    #[test]
    fn test_confirmed_first_records_fill() {
        // Simulate reconnect scenario: only CONFIRMED arrives, no prior MATCHED.
        // The fill should still be recorded to prevent silent inventory loss.
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        // Only Confirmed arrives (no prior Matched)
        im.apply_fill(&FillEvent {
            order_id: "confirmed-only".to_string(),
            side: Side::Yes,
            filled_size: 5.0,
            price: 0.48,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        });

        // Should be recorded, not silently dropped
        assert!((im.state.yes_qty - 5.0).abs() < 1e-9);
        assert!((im.state.yes_avg_cost - 0.48).abs() < 1e-9);
    }

    #[test]
    fn test_confirmed_after_matched_is_noop() {
        // Normal lifecycle: MATCHED then CONFIRMED for the same order.
        // Inventory should NOT double.
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        // Matched first
        im.apply_fill(&FillEvent {
            order_id: "duptest".to_string(),
            side: Side::No,
            filled_size: 3.0,
            price: 0.45,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });
        assert!((im.state.no_qty - 3.0).abs() < 1e-9);

        // Confirmed: should be no-op (idempotent)
        im.apply_fill(&FillEvent {
            order_id: "duptest".to_string(),
            side: Side::No,
            filled_size: 3.0,
            price: 0.45,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        });
        // Still 3.0, NOT 6.0
        assert!((im.state.no_qty - 3.0).abs() < 1e-9);
        assert!((im.state.no_avg_cost - 0.45).abs() < 1e-9);
    }

    #[test]
    fn test_merge_sync_reduces_both_sides_and_keeps_vwap() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        im.apply_fill(&FillEvent {
            order_id: "yes-1".to_string(),
            side: Side::Yes,
            filled_size: 10.0,
            price: 0.40,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });
        im.apply_fill(&FillEvent {
            order_id: "no-1".to_string(),
            side: Side::No,
            filled_size: 10.0,
            price: 0.58,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });

        im.apply_merge(4.0, "m1");

        assert!((im.state.yes_qty - 6.0).abs() < 1e-9);
        assert!((im.state.no_qty - 6.0).abs() < 1e-9);
        assert!((im.state.yes_avg_cost - 0.40).abs() < 1e-9);
        assert!((im.state.no_avg_cost - 0.58).abs() < 1e-9);
        assert!(im.state.net_diff.abs() < 1e-9);
        assert!((im.state.portfolio_cost - 0.98).abs() < 1e-9);
    }

    #[test]
    fn test_merge_sync_is_clamped_by_available_full_set() {
        let (state_tx, _state_rx) = watch::channel(InventoryState::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        let mut im = InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx);

        im.apply_fill(&FillEvent {
            order_id: "yes-2".to_string(),
            side: Side::Yes,
            filled_size: 4.0,
            price: 0.37,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });
        im.apply_fill(&FillEvent {
            order_id: "no-2".to_string(),
            side: Side::No,
            filled_size: 2.0,
            price: 0.63,
            status: FillStatus::Matched,
            ts: Instant::now(),
        });

        // Request exceeds available full set (min(4,2)=2), should clamp to 2.
        im.apply_merge(5.0, "m2");

        assert!((im.state.yes_qty - 2.0).abs() < 1e-9);
        assert!(im.state.no_qty.abs() < 1e-9);
        assert!((im.state.yes_avg_cost - 0.37).abs() < 1e-9);
        assert!(im.state.no_avg_cost.abs() < 1e-9);
        assert!((im.state.net_diff - 2.0).abs() < 1e-9);
        assert!(im.state.portfolio_cost.abs() < 1e-9);
    }
}
