use std::time::{Duration, Instant};

use tracing::{debug, info};

use super::*;

const PAIR_ARB_PROGRESS_MIN_PAIRED_QTY_DELTA_RATIO: f64 = 0.2;
const PAIR_ARB_PROGRESS_MIN_PAIRED_QTY_DELTA_FLOOR: f64 = 0.25;
const PAIR_ARB_STALLED_SECS: u64 = 60;
const FLOAT_INV_EPS: f64 = PAIR_ARB_NET_EPS;

impl StrategyCoordinator {
    fn pair_arb_progress_min_paired_qty_delta(&self) -> f64 {
        (self.cfg.bid_size * PAIR_ARB_PROGRESS_MIN_PAIRED_QTY_DELTA_RATIO)
            .max(PAIR_ARB_PROGRESS_MIN_PAIRED_QTY_DELTA_FLOOR)
    }

    pub(super) fn pair_arb_bump_decision_epoch(&mut self, reason: &'static str) {
        if self.cfg.strategy != StrategyKind::PairArb {
            return;
        }
        self.pair_arb_decision_epoch = self.pair_arb_decision_epoch.saturating_add(1);
        debug!(
            "🧭 pair_arb_decision_epoch={} reason={}",
            self.pair_arb_decision_epoch, reason
        );
    }

    pub(crate) fn pair_arb_net_bucket_for_abs(abs_net: f64) -> PairArbNetBucket {
        if abs_net <= FLOAT_INV_EPS {
            PairArbNetBucket::Flat
        } else if abs_net < 5.0 {
            PairArbNetBucket::Low
        } else if abs_net < 10.0 {
            PairArbNetBucket::Mid
        } else {
            PairArbNetBucket::High
        }
    }

    pub(crate) fn pair_arb_net_bucket_rank(bucket: PairArbNetBucket) -> u8 {
        match bucket {
            PairArbNetBucket::Flat => 0,
            PairArbNetBucket::Low => 1,
            PairArbNetBucket::Mid => 2,
            PairArbNetBucket::High => 3,
        }
    }

    pub(crate) fn pair_arb_progress_regime(
        &self,
        inv: &InventoryState,
        now: Instant,
    ) -> PairProgressRegime {
        if inv.net_diff.abs() + FLOAT_INV_EPS < 10.0 {
            return PairProgressRegime::Healthy;
        }
        let since = self
            .pair_arb_progress_state
            .last_pair_progress_at
            .unwrap_or(self.market_start);
        if now.saturating_duration_since(since) >= Duration::from_secs(PAIR_ARB_STALLED_SECS) {
            PairProgressRegime::Stalled
        } else {
            PairProgressRegime::Healthy
        }
    }

    fn pair_arb_detect_buy_fill_price(
        prev_qty: f64,
        prev_avg: f64,
        next_qty: f64,
        next_avg: f64,
    ) -> Option<f64> {
        if next_qty <= prev_qty + FLOAT_INV_EPS {
            return None;
        }
        let delta_qty = next_qty - prev_qty;
        if delta_qty <= FLOAT_INV_EPS {
            return None;
        }
        let prev_cost = prev_qty.max(0.0) * prev_avg.max(0.0);
        let next_cost = next_qty.max(0.0) * next_avg.max(0.0);
        let price = (next_cost - prev_cost) / delta_qty;
        if price.is_finite() && price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

    fn pair_arb_reset_risk_fill_anchor(&mut self, side: Side) {
        match side {
            Side::Yes => {
                self.pair_arb_progress_state
                    .last_risk_increasing_fill_price_yes = None;
                self.pair_arb_progress_state.last_risk_fill_net_bucket_yes = None;
            }
            Side::No => {
                self.pair_arb_progress_state
                    .last_risk_increasing_fill_price_no = None;
                self.pair_arb_progress_state.last_risk_fill_net_bucket_no = None;
            }
        }
    }

    pub(super) fn observe_pair_arb_inventory_transition(
        &mut self,
        snapshot: &InventorySnapshot,
        now: Instant,
    ) {
        let settled = snapshot.settled;
        let working = snapshot.working;
        if self.cfg.strategy != StrategyKind::PairArb {
            self.last_settled_inv_snapshot = settled;
            self.last_working_inv_snapshot = working;
            return;
        }

        let prev_settled = self.last_settled_inv_snapshot;
        let prev_working = self.last_working_inv_snapshot;
        let settled_changed = (prev_settled.yes_qty - settled.yes_qty).abs() > FLOAT_INV_EPS
            || (prev_settled.no_qty - settled.no_qty).abs() > FLOAT_INV_EPS
            || (prev_settled.yes_avg_cost - settled.yes_avg_cost).abs() > FLOAT_INV_EPS
            || (prev_settled.no_avg_cost - settled.no_avg_cost).abs() > FLOAT_INV_EPS;
        let prev_fragile = (prev_working.yes_qty - prev_settled.yes_qty).abs() > FLOAT_INV_EPS
            || (prev_working.no_qty - prev_settled.no_qty).abs() > FLOAT_INV_EPS;
        let working_changed = (prev_working.yes_qty - working.yes_qty).abs() > FLOAT_INV_EPS
            || (prev_working.no_qty - working.no_qty).abs() > FLOAT_INV_EPS
            || (prev_working.yes_avg_cost - working.yes_avg_cost).abs() > FLOAT_INV_EPS
            || (prev_working.no_avg_cost - working.no_avg_cost).abs() > FLOAT_INV_EPS
            || snapshot.fragile != prev_fragile;
        if !settled_changed && !working_changed {
            return;
        }

        self.pair_arb_bump_decision_epoch("inventory_transition");
        self.slot_pair_arb_fill_recheck_pending[OrderSlot::YES_BUY.index()] = true;
        self.slot_pair_arb_fill_recheck_pending[OrderSlot::NO_BUY.index()] = true;

        // Pair-progress is diagnostics-only; keep it on working inventory so
        // telemetry stays aligned with the strategy brain.
        let working_metrics = self.derive_inventory_metrics(&working);
        let progress_delta = self.pair_arb_progress_min_paired_qty_delta();
        if working_metrics.paired_qty + FLOAT_INV_EPS
            < self.pair_arb_progress_state.last_pair_progress_paired_qty
        {
            // Rebase after rollback/reconcile so progress detection cannot get stuck.
            self.pair_arb_progress_state.last_pair_progress_paired_qty = working_metrics.paired_qty;
        }
        if working_metrics.paired_qty
            >= self.pair_arb_progress_state.last_pair_progress_paired_qty + progress_delta
                - FLOAT_INV_EPS
        {
            self.pair_arb_progress_state.last_pair_progress_at = Some(now);
            self.pair_arb_progress_state.last_pair_progress_paired_qty = working_metrics.paired_qty;
        }

        if !settled_changed {
            self.last_working_inv_snapshot = working;
            return;
        }

        let prev = prev_settled;
        let inv = settled;
        let prev_bucket = Self::pair_arb_net_bucket_for_abs(prev.net_diff.abs());
        let curr_bucket = Self::pair_arb_net_bucket_for_abs(inv.net_diff.abs());
        let prev_dom = if prev.net_diff > FLOAT_INV_EPS {
            Some(Side::Yes)
        } else if prev.net_diff < -FLOAT_INV_EPS {
            Some(Side::No)
        } else {
            None
        };
        let curr_dom = if inv.net_diff > FLOAT_INV_EPS {
            Some(Side::Yes)
        } else if inv.net_diff < -FLOAT_INV_EPS {
            Some(Side::No)
        } else {
            None
        };

        let dominant_flipped = prev_dom != curr_dom;
        if dominant_flipped {
            self.pair_arb_reset_risk_fill_anchor(Side::Yes);
            self.pair_arb_reset_risk_fill_anchor(Side::No);
        } else if Self::pair_arb_net_bucket_rank(curr_bucket)
            < Self::pair_arb_net_bucket_rank(prev_bucket)
        {
            match curr_dom {
                Some(side) => self.pair_arb_reset_risk_fill_anchor(side),
                None => {
                    self.pair_arb_reset_risk_fill_anchor(Side::Yes);
                    self.pair_arb_reset_risk_fill_anchor(Side::No);
                }
            }
        }

        let prev_metrics = self.derive_inventory_metrics(&prev);

        let prev_abs = prev.net_diff.abs();
        let curr_abs = inv.net_diff.abs();
        if curr_abs > prev_abs + FLOAT_INV_EPS {
            if let Some(price) = Self::pair_arb_detect_buy_fill_price(
                prev.yes_qty,
                prev.yes_avg_cost,
                inv.yes_qty,
                inv.yes_avg_cost,
            ) {
                self.pair_arb_progress_state
                    .last_risk_increasing_fill_price_yes = Some(price);
                self.pair_arb_progress_state.last_risk_fill_net_bucket_yes = Some(curr_bucket);
            }
            if let Some(price) = Self::pair_arb_detect_buy_fill_price(
                prev.no_qty,
                prev.no_avg_cost,
                inv.no_qty,
                inv.no_avg_cost,
            ) {
                self.pair_arb_progress_state
                    .last_risk_increasing_fill_price_no = Some(price);
                self.pair_arb_progress_state.last_risk_fill_net_bucket_no = Some(curr_bucket);
            }
        }

        let merged_yes = (prev.yes_qty - inv.yes_qty).max(0.0);
        let merged_no = (prev.no_qty - inv.no_qty).max(0.0);
        let merged_full_set = merged_yes.min(merged_no);
        if merged_full_set > FLOAT_INV_EPS {
            self.round_realized_pair_metrics.realized_pair_qty += merged_full_set;
            self.round_realized_pair_metrics.realized_pair_locked_pnl +=
                merged_full_set * (1.0 - prev.yes_avg_cost.max(0.0) - prev.no_avg_cost.max(0.0));
            self.round_realized_pair_metrics.merged_cash_released += merged_full_set;
        }

        self.last_settled_inv_snapshot = settled;
        self.last_working_inv_snapshot = working;

        debug!(
            "🧭 PairArb inventory transition | settled_prev_net={:.2} settled_curr_net={:.2} working_curr_net={:.2} fragile={} prev_bucket={:?} curr_bucket={:?} prev_dom={:?} curr_dom={:?} progress_regime={:?}",
            prev.net_diff,
            inv.net_diff,
            working.net_diff,
            snapshot.fragile,
            prev_bucket,
            curr_bucket,
            prev_dom,
            curr_dom,
            self.pair_arb_progress_regime(&working, now),
        );
        let _ = prev_metrics; // retained for future debugging without recompute churn
    }

    fn pair_arb_gate_snapshot(&self) -> PairArbGateLogSnapshot {
        PairArbGateLogSnapshot {
            ofi_softened_quotes: self.stats.pair_arb_ofi_softened_quotes,
            ofi_suppressed_quotes: self.stats.pair_arb_ofi_suppressed_quotes,
            keep_candidates: self.stats.pair_arb_keep_candidates,
            skip_inventory_gate: self.stats.pair_arb_skip_inventory_gate,
            skip_simulate_buy_none: self.stats.pair_arb_skip_simulate_buy_none,
        }
    }

    fn pgt_gate_snapshot(&self) -> PgtGateLogSnapshot {
        PgtGateLogSnapshot {
            seed_quotes: self.stats.pgt_seed_quotes,
            completion_quotes: self.stats.pgt_completion_quotes,
            skip_harvest: self.stats.pgt_skip_harvest,
            skip_tail_completion_only: self.stats.pgt_skip_tail_completion_only,
            skip_residual_guard: self.stats.pgt_skip_residual_guard,
            skip_capital_guard: self.stats.pgt_skip_capital_guard,
            skip_invalid_book: self.stats.pgt_skip_invalid_book,
            skip_no_seed: self.stats.pgt_skip_no_seed,
            post_flow_quotes: self.stats.pgt_post_flow_quotes,
            dispatch_intents: self.stats.pgt_dispatch_intents,
            dispatch_blocked: self.stats.pgt_dispatch_blocked,
            dispatch_place: self.stats.pgt_dispatch_place,
            dispatch_retain: self.stats.pgt_dispatch_retain,
            dispatch_clear: self.stats.pgt_dispatch_clear,
        }
    }

    fn maybe_log_pair_arb_gate_summary(&mut self) {
        if self.cfg.strategy != StrategyKind::PairArb {
            return;
        }
        let now = Instant::now();
        let interval = Duration::from_secs(PAIR_ARB_GATE_SUMMARY_SECS);
        if now.duration_since(self.pair_arb_gate_last_log_ts) < interval {
            return;
        }
        self.pair_arb_gate_last_log_ts = now;

        let cur = self.pair_arb_gate_snapshot();
        let prev = self.pair_arb_gate_last_snapshot;
        self.pair_arb_gate_last_snapshot = cur;

        let keep_delta = cur.keep_candidates.saturating_sub(prev.keep_candidates);
        let softened_delta = cur
            .ofi_softened_quotes
            .saturating_sub(prev.ofi_softened_quotes);
        let suppressed_delta = cur
            .ofi_suppressed_quotes
            .saturating_sub(prev.ofi_suppressed_quotes);
        let skip_inv_delta = cur
            .skip_inventory_gate
            .saturating_sub(prev.skip_inventory_gate);
        let skip_sim_delta = cur
            .skip_simulate_buy_none
            .saturating_sub(prev.skip_simulate_buy_none);

        let skip_total = skip_inv_delta + skip_sim_delta;
        let attempts = keep_delta + skip_total;
        let keep_rate = if attempts > 0 {
            keep_delta as f64 / attempts as f64
        } else {
            0.0
        };

        info!(
            "🧭 PairArbGate(30s) | attempts={} keep={} keep_rate={:.1}% skip(inv/sim)={}/{} ofi(softened/suppressed)={}/{}",
            attempts,
            keep_delta,
            keep_rate * 100.0,
            skip_inv_delta,
            skip_sim_delta,
            softened_delta,
            suppressed_delta,
        );
    }

    fn maybe_log_pgt_gate_summary(&mut self) {
        if self.cfg.strategy != StrategyKind::PairGatedTrancheArb {
            return;
        }
        let now = Instant::now();
        let interval = Duration::from_secs(PAIR_ARB_GATE_SUMMARY_SECS);
        if now.duration_since(self.pgt_gate_last_log_ts) < interval {
            return;
        }
        self.pgt_gate_last_log_ts = now;

        let cur = self.pgt_gate_snapshot();
        let prev = self.pgt_gate_last_snapshot;
        self.pgt_gate_last_snapshot = cur;

        let seed_delta = cur.seed_quotes.saturating_sub(prev.seed_quotes);
        let completion_delta = cur
            .completion_quotes
            .saturating_sub(prev.completion_quotes);
        let skip_harvest_delta = cur.skip_harvest.saturating_sub(prev.skip_harvest);
        let skip_tail_delta = cur
            .skip_tail_completion_only
            .saturating_sub(prev.skip_tail_completion_only);
        let skip_residual_delta = cur
            .skip_residual_guard
            .saturating_sub(prev.skip_residual_guard);
        let skip_capital_delta = cur
            .skip_capital_guard
            .saturating_sub(prev.skip_capital_guard);
        let skip_invalid_book_delta = cur
            .skip_invalid_book
            .saturating_sub(prev.skip_invalid_book);
        let skip_no_seed_delta = cur.skip_no_seed.saturating_sub(prev.skip_no_seed);
        let post_flow_delta = cur.post_flow_quotes.saturating_sub(prev.post_flow_quotes);
        let dispatch_intents_delta = cur
            .dispatch_intents
            .saturating_sub(prev.dispatch_intents);
        let dispatch_blocked_delta = cur
            .dispatch_blocked
            .saturating_sub(prev.dispatch_blocked);
        let dispatch_place_delta = cur.dispatch_place.saturating_sub(prev.dispatch_place);
        let dispatch_retain_delta = cur.dispatch_retain.saturating_sub(prev.dispatch_retain);
        let dispatch_clear_delta = cur.dispatch_clear.saturating_sub(prev.dispatch_clear);

        info!(
            "🧭 PGTGate(30s) | quotes(seed/completion/post_flow)={}/{}/{} dispatch(intent/blocked/place/retain/clear)={}/{}/{}/{}/{} skip(harvest/tail/residual/capital/invalid/no_seed)={}/{}/{}/{}/{}/{}",
            seed_delta,
            completion_delta,
            post_flow_delta,
            dispatch_intents_delta,
            dispatch_blocked_delta,
            dispatch_place_delta,
            dispatch_retain_delta,
            dispatch_clear_delta,
            skip_harvest_delta,
            skip_tail_delta,
            skip_residual_delta,
            skip_capital_delta,
            skip_invalid_book_delta,
            skip_no_seed_delta,
        );
    }

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
        // OracleLagSniping has near-certain winner knowledge from Chainlink; the
        // pair-arb symmetric-risk floor assumes unknown outcome and does not apply.
        if matches!(
            self.cfg.strategy,
            StrategyKind::OracleLagSniping | StrategyKind::PairGatedTrancheArb
        ) {
            return true;
        }
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
            "📊 StrategyMetrics | paired_qty={:.2} pair_cost={:.4} paired_locked_pnl={:.4} worst_case_outcome_pnl={:.4} total_spent={:.4} dominant={} residual_qty={:.2} residual_value={:.4} net_diff={:.2} ofi(y/n)={:.1}/{:.1} toxic(y/n)={}/{} pair_progress_regime={:?} realized_pair_qty={:.2} realized_pair_locked_pnl={:.4} merged_cash_released={:.4}",
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
            self.pair_arb_progress_regime(inv, now),
            self.round_realized_pair_metrics.realized_pair_qty,
            self.round_realized_pair_metrics.realized_pair_locked_pnl,
            self.round_realized_pair_metrics.merged_cash_released,
        );
        self.maybe_log_pair_arb_gate_summary();
        self.maybe_log_pgt_gate_summary();
    }
}
