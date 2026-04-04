use std::time::{Duration, Instant};

use tracing::{debug, info};

use super::*;

impl StrategyCoordinator {
    pub(super) fn outcome_floor_pnl(&self) -> f64 {
        let max_net = self.cfg.max_net_diff.max(0.0);
        if max_net <= f64::EPSILON {
            return f64::NEG_INFINITY;
        }
        let pair = self.cfg.pair_target.max(0.0);
        -max_net * pair
    }

    pub(super) fn project_buy_inventory(
        &self,
        inv: &InventoryState,
        side: Side,
        size: f64,
        price: f64,
    ) -> Option<InventoryState> {
        if size <= f64::EPSILON || price <= f64::EPSILON {
            return None;
        }
        let mut next = *inv;
        match side {
            Side::Yes => {
                let q0 = inv.yes_qty.max(0.0);
                let q1 = q0 + size;
                if q1 <= f64::EPSILON {
                    return None;
                }
                let c0 = q0 * inv.yes_avg_cost.max(0.0);
                next.yes_qty = q1;
                next.yes_avg_cost = (c0 + size * price) / q1;
            }
            Side::No => {
                let q0 = inv.no_qty.max(0.0);
                let q1 = q0 + size;
                if q1 <= f64::EPSILON {
                    return None;
                }
                let c0 = q0 * inv.no_avg_cost.max(0.0);
                next.no_qty = q1;
                next.no_avg_cost = (c0 + size * price) / q1;
            }
        }
        next.net_diff = next.yes_qty - next.no_qty;
        next.portfolio_cost = if next.yes_qty > f64::EPSILON && next.no_qty > f64::EPSILON {
            next.yes_avg_cost + next.no_avg_cost
        } else {
            0.0
        };
        Some(next)
    }

    pub(crate) fn simulate_buy(
        &self,
        inv: &InventoryState,
        side: Side,
        size: f64,
        price: f64,
    ) -> Option<ProjectedBuyMetrics> {
        let projected_inv = self.project_buy_inventory(inv, side, size, price)?;
        let metrics = self.derive_inventory_metrics(&projected_inv);
        Some(ProjectedBuyMetrics {
            projected_inventory: projected_inv,
            projected_yes_qty: projected_inv.yes_qty.max(0.0),
            projected_no_qty: projected_inv.no_qty.max(0.0),
            projected_total_cost: metrics.total_spent,
            projected_abs_net_diff: projected_inv.net_diff.abs(),
            metrics,
        })
    }

    pub(crate) fn open_edge_for_inventory(
        &self,
        inv: &InventoryState,
        metrics: &StrategyInventoryMetrics,
        book: &Book,
    ) -> f64 {
        let Some(dominant_side) = metrics.dominant_side else {
            return 0.0;
        };
        if metrics.residual_qty <= f64::EPSILON {
            return 0.0;
        }

        let (dominant_avg_cost, opposite_best_ask) = match dominant_side {
            Side::Yes => (inv.yes_avg_cost.max(0.0), book.no_ask),
            Side::No => (inv.no_avg_cost.max(0.0), book.yes_ask),
        };
        if dominant_avg_cost <= 0.0 || opposite_best_ask <= 0.0 {
            return 0.0;
        }

        metrics.residual_qty
            * f64::max(
                0.0,
                self.cfg.open_pair_band - (dominant_avg_cost + opposite_best_ask),
            )
    }

    pub(crate) fn utility_for_inventory(
        &self,
        inv: &InventoryState,
        metrics: &StrategyInventoryMetrics,
        book: &Book,
    ) -> f64 {
        let net_penalty = 0.5 * inv.net_diff.abs() * self.cfg.tick_size.max(1e-9);
        metrics.paired_locked_pnl + self.open_edge_for_inventory(inv, metrics, book) - net_penalty
    }

    pub(super) fn passes_outcome_floor_for_buy(
        &self,
        inv: &InventoryState,
        side: Side,
        size: f64,
        price: f64,
        reason: BidReason,
    ) -> bool {
        let floor = self.outcome_floor_pnl();
        if !floor.is_finite() {
            return true;
        }
        let Some(projected_inv) = self.project_buy_inventory(inv, side, size, price) else {
            return false;
        };
        let current = self.derive_inventory_metrics(inv).worst_case_outcome_pnl;
        let projected = self
            .derive_inventory_metrics(&projected_inv)
            .worst_case_outcome_pnl;

        if current >= floor - 1e-9 {
            if projected + 1e-9 < floor {
                debug!(
                    "🧱 Outcome floor block ({:?} {:?}): side={:?} px={:.3} sz={:.2} current={:.4} projected={:.4} floor={:.4}",
                    reason,
                    TradeDirection::Buy,
                    side,
                    price,
                    size,
                    current,
                    projected,
                    floor,
                );
                return false;
            }
            return true;
        }
        if projected + 1e-9 < current {
            debug!(
                "🧱 Outcome floor anti-dig block ({:?} {:?}): side={:?} px={:.3} sz={:.2} current={:.4} projected={:.4} floor={:.4}",
                reason,
                TradeDirection::Buy,
                side,
                price,
                size,
                current,
                projected,
                floor,
            );
            return false;
        }
        true
    }

    pub(super) fn passes_pair_cost_guard_for_buy(
        &self,
        inv: &InventoryState,
        side: Side,
        size: f64,
        price: f64,
        reason: BidReason,
    ) -> bool {
        let Some(projected_inv) = self.project_buy_inventory(inv, side, size, price) else {
            return false;
        };
        let current = self.derive_inventory_metrics(inv);
        let projected = self.derive_inventory_metrics(&projected_inv);
        if projected.paired_qty <= f64::EPSILON {
            return true;
        }

        let target = self.cfg.pair_target.max(0.0);
        if projected.pair_cost <= target + 1e-9 {
            return true;
        }

        // Hedge intents may pay up if they are actively reducing directional risk.
        let reduces_abs_net = projected_inv.net_diff.abs() + 1e-9 < inv.net_diff.abs();
        if reason == BidReason::Hedge && reduces_abs_net {
            debug!(
                "🛡️ Pair-cost guard bypass ({:?} {:?}): side={:?} px={:.3} sz={:.2} projected_pair_cost={:.4} > target={:.4} but net_diff improves {:.2}->{:.2}",
                reason,
                TradeDirection::Buy,
                side,
                price,
                size,
                projected.pair_cost,
                target,
                inv.net_diff.abs(),
                projected_inv.net_diff.abs(),
            );
            return true;
        }

        // If we are already above pair target, allow only strict repair moves.
        if current.paired_qty > f64::EPSILON
            && current.pair_cost > target + 1e-9
            && projected.pair_cost + 1e-9 < current.pair_cost
        {
            return true;
        }

        debug!(
            "🧱 Pair-cost guard block ({:?} {:?}): side={:?} px={:.3} sz={:.2} current_pair_cost={:.4} projected_pair_cost={:.4} target={:.4} paired_qty={:.2}->{:.2}",
            reason,
            TradeDirection::Buy,
            side,
            price,
            size,
            current.pair_cost,
            projected.pair_cost,
            target,
            current.paired_qty,
            projected.paired_qty,
        );
        false
    }

    pub(crate) fn derive_inventory_metrics(
        &self,
        inv: &InventoryState,
    ) -> StrategyInventoryMetrics {
        let yes_qty = inv.yes_qty.max(0.0);
        let no_qty = inv.no_qty.max(0.0);
        let yes_avg = inv.yes_avg_cost.max(0.0);
        let no_avg = inv.no_avg_cost.max(0.0);

        let paired_qty = yes_qty.min(no_qty);
        let pair_cost = if paired_qty > f64::EPSILON {
            yes_avg + no_avg
        } else {
            0.0
        };
        let paired_locked_pnl = paired_qty * (1.0 - pair_cost);

        let total_spent = yes_qty * yes_avg + no_qty * no_avg;
        let yes_outcome_pnl = yes_qty - total_spent;
        let no_outcome_pnl = no_qty - total_spent;
        let worst_case_outcome_pnl = yes_outcome_pnl.min(no_outcome_pnl);

        let dominant_side = if inv.net_diff > f64::EPSILON {
            Some(Side::Yes)
        } else if inv.net_diff < -f64::EPSILON {
            Some(Side::No)
        } else {
            None
        };
        let residual_qty = (yes_qty - no_qty).abs();
        let residual_avg = match dominant_side {
            Some(Side::Yes) => yes_avg,
            Some(Side::No) => no_avg,
            None => 0.0,
        };
        let residual_inventory_value = residual_qty * residual_avg;

        StrategyInventoryMetrics {
            paired_qty,
            pair_cost,
            paired_locked_pnl,
            total_spent,
            worst_case_outcome_pnl,
            dominant_side,
            residual_qty,
            residual_inventory_value,
        }
    }

    pub(super) fn maybe_log_inventory_metrics(&mut self, inv: &InventoryState, ofi: &OfiSnapshot) {
        if self.cfg.strategy_metrics_log_secs == 0 {
            return;
        }
        let interval = Duration::from_secs(self.cfg.strategy_metrics_log_secs);
        let now = Instant::now();
        if now.duration_since(self.last_metrics_log_ts) < interval {
            return;
        }
        self.last_metrics_log_ts = now;

        let m = self.derive_inventory_metrics(inv);
        let dominant_side = match m.dominant_side {
            Some(Side::Yes) => "YES",
            Some(Side::No) => "NO",
            None => "FLAT",
        };

        info!(
            "📊 StrategyMetrics | paired_qty={:.2} pair_cost={:.4} paired_locked_pnl={:.4} worst_case_outcome_pnl={:.4} total_spent={:.4} dominant={} residual_qty={:.2} residual_value={:.4} net_diff={:.2} ofi(y/n)={:.1}/{:.1} toxic(y/n)={}/{}",
            m.paired_qty,
            m.pair_cost,
            m.paired_locked_pnl,
            m.worst_case_outcome_pnl,
            m.total_spent,
            dominant_side,
            m.residual_qty,
            m.residual_inventory_value,
            inv.net_diff,
            ofi.yes.ofi_score,
            ofi.no.ofi_score,
            ofi.yes.is_toxic,
            ofi.no.is_toxic,
        );
    }
}
