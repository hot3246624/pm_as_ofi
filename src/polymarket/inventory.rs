//! Inventory Manager Actor.
//!
//! Tracks real-time position state (YES/NO quantities, average costs)
//! and broadcasts snapshots via a `watch` channel for the Coordinator to read.

use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use super::messages::{
    FillEvent, FillStatus, InventoryEvent, InventorySnapshot, InventoryState, TradeDirection,
};
use super::pair_ledger::{
    build_pair_ledger, PairLedgerBuildResult, PairLedgerEvent, PairLedgerEventKind, PathKind,
    TrancheState,
};
use super::recorder::{RecorderHandle, RecorderSessionMeta};
use super::types::Side;

#[derive(Debug, Clone)]
pub struct InventoryConfig {
    pub max_net_diff: f64,
    pub max_portfolio_cost: f64,
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
        if let Ok(v) = std::env::var("PM_BID_SIZE") {
            if let Ok(f) = v.parse::<f64>() {
                cfg.bid_size = f;
            }
        }
        cfg
    }
}

#[derive(Debug, Clone)]
struct FillRecord {
    side: Side,
    direction: TradeDirection,
    size: f64,
    price: f64,
    ts: Instant,
    kind: PairLedgerEventKind,
}

#[derive(Debug, Clone)]
struct PendingFillRecord {
    order_id: String,
    side: Side,
    direction: TradeDirection,
    size: f64,
    price: f64,
    matched_at: Instant,
}

const PENDING_PROMOTION_TIMEOUT: Duration = Duration::from_secs(15);
const PENDING_PROMOTION_TICK: Duration = Duration::from_millis(250);

pub struct InventoryManager {
    cfg: InventoryConfig,
    snapshot: InventorySnapshot,
    fill_rx: mpsc::Receiver<InventoryEvent>,
    state_tx: watch::Sender<InventorySnapshot>,
    settled_ledger: Vec<FillRecord>,
    pending_fills: Vec<PendingFillRecord>,
    matched_pending_events: u64,
    confirmed_promotions: u64,
    timeout_promotions: u64,
    failed_reverts: u64,
    late_failed_after_promotion: u64,
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
}

impl InventoryManager {
    fn emit_inventory_event(&self, event: &str, payload: serde_json::Value) {
        if let (Some(recorder), Some(meta)) = (&self.recorder, &self.recorder_meta) {
            recorder.emit_own_inventory_event(meta, event, payload);
        }
    }

    pub fn new(
        cfg: InventoryConfig,
        fill_rx: mpsc::Receiver<InventoryEvent>,
        state_tx: watch::Sender<InventorySnapshot>,
        recorder: Option<RecorderHandle>,
        recorder_meta: Option<RecorderSessionMeta>,
    ) -> Self {
        Self {
            cfg,
            snapshot: InventorySnapshot::default(),
            fill_rx,
            state_tx,
            settled_ledger: Vec::new(),
            pending_fills: Vec::new(),
            matched_pending_events: 0,
            confirmed_promotions: 0,
            timeout_promotions: 0,
            failed_reverts: 0,
            late_failed_after_promotion: 0,
            recorder,
            recorder_meta,
        }
    }

    pub async fn run(mut self) {
        info!(
            "📦 InventoryManager started | max_net_diff={:.0} max_cost={:.3}",
            self.cfg.max_net_diff, self.cfg.max_portfolio_cost,
        );

        let mut promotion_tick = tokio::time::interval(PENDING_PROMOTION_TICK);

        loop {
            tokio::select! {
                maybe_event = self.fill_rx.recv() => {
                    let Some(event) = maybe_event else {
                        break;
                    };
                    match event {
                        InventoryEvent::Fill(fill) => {
                            self.apply_fill(&fill);
                            let _ = self.state_tx.send(self.snapshot);
                            self.log_fill_snapshot(&fill);
                            self.emit_inventory_event(
                                "fill_snapshot",
                                serde_json::json!({
                                    "slot": fill.slot().as_str(),
                                    "side": format!("{:?}", fill.side),
                                    "direction": format!("{:?}", fill.direction),
                                    "size": fill.filled_size,
                                    "price": fill.price,
                                    "status": format!("{:?}", fill.status),
                                    "order_id": fill.order_id,
                                    "working_net_diff": self.snapshot.working.net_diff,
                                    "working_portfolio_cost": self.snapshot.working.portfolio_cost,
                                    "pending_yes_qty": self.snapshot.pending_yes_qty,
                                    "pending_no_qty": self.snapshot.pending_no_qty,
                                    "fragile": self.snapshot.fragile,
                                }),
                            );
                        }
                        InventoryEvent::Merge {
                            full_set_size,
                            merge_id,
                            ts,
                        } => {
                            self.apply_merge(full_set_size, &merge_id, ts);
                            let _ = self.state_tx.send(self.snapshot);
                            info!(
                                "📦 Merge sync: full_set={:.2} id={} → settled net={:.1} cost={:.4} || working net={:.1} cost={:.4} pending_yes={:.2} pending_no={:.2} fragile={}",
                                full_set_size,
                                &merge_id[..8.min(merge_id.len())],
                                self.snapshot.settled.net_diff,
                                self.snapshot.settled.portfolio_cost,
                                self.snapshot.working.net_diff,
                                self.snapshot.working.portfolio_cost,
                                self.snapshot.pending_yes_qty,
                                self.snapshot.pending_no_qty,
                                self.snapshot.fragile,
                            );
                            self.emit_inventory_event(
                                "merge_sync",
                                serde_json::json!({
                                    "full_set_size": full_set_size,
                                    "merge_id": merge_id,
                                    "working_net_diff": self.snapshot.working.net_diff,
                                    "working_portfolio_cost": self.snapshot.working.portfolio_cost,
                                    "settled_net_diff": self.snapshot.settled.net_diff,
                                    "settled_portfolio_cost": self.snapshot.settled.portfolio_cost,
                                    "pending_yes_qty": self.snapshot.pending_yes_qty,
                                    "pending_no_qty": self.snapshot.pending_no_qty,
                                    "fragile": self.snapshot.fragile,
                                }),
                            );
                        }
                    }
                }
                _ = promotion_tick.tick() => {
                    if self.promote_expired_pending(Instant::now()) {
                        let _ = self.state_tx.send(self.snapshot);
                    }
                }
            }
        }

        info!(
            "📦 InventoryManager shutting down (channel closed) | finality(matched_pending={} confirmed_promotions={} timeout_promotions={} failed_reverts={} late_failed_after_promotion={})",
            self.matched_pending_events,
            self.confirmed_promotions,
            self.timeout_promotions,
            self.failed_reverts,
            self.late_failed_after_promotion,
        );
    }

    fn log_fill_snapshot(&self, fill: &FillEvent) {
        info!(
            "📦 Fill: slot={} {:?} {:.2}@{:.3} status={:?} id={} → settled YES={:.1}@{:.4} NO={:.1}@{:.4} | net={:.1} cost={:.4} || working YES={:.1}@{:.4} NO={:.1}@{:.4} | net={:.1} cost={:.4} pending_yes={:.2} pending_no={:.2} fragile={}",
            fill.slot().as_str(),
            fill.side,
            fill.filled_size,
            fill.price,
            fill.status,
            &fill.order_id[..8.min(fill.order_id.len())],
            self.snapshot.settled.yes_qty,
            self.snapshot.settled.yes_avg_cost,
            self.snapshot.settled.no_qty,
            self.snapshot.settled.no_avg_cost,
            self.snapshot.settled.net_diff,
            self.snapshot.settled.portfolio_cost,
            self.snapshot.working.yes_qty,
            self.snapshot.working.yes_avg_cost,
            self.snapshot.working.no_qty,
            self.snapshot.working.no_avg_cost,
            self.snapshot.working.net_diff,
            self.snapshot.working.portfolio_cost,
            self.snapshot.pending_yes_qty,
            self.snapshot.pending_no_qty,
            self.snapshot.fragile,
        );
    }

    fn apply_fill(&mut self, fill: &FillEvent) {
        let signed_size = match fill.direction {
            TradeDirection::Buy => fill.filled_size,
            TradeDirection::Sell => -fill.filled_size,
        };

        match fill.status {
            FillStatus::Matched => {
                self.matched_pending_events = self.matched_pending_events.saturating_add(1);
                self.pending_fills.push(PendingFillRecord {
                    order_id: fill.order_id.clone(),
                    side: fill.side,
                    direction: fill.direction,
                    size: signed_size,
                    price: fill.price,
                    matched_at: fill.ts,
                });
            }
            FillStatus::Confirmed => {
                if !self.promote_pending_fill(
                    fill.order_id.as_str(),
                    fill.side,
                    fill.direction,
                    signed_size,
                    Some(fill.price),
                    "confirmed",
                ) {
                    warn!(
                        "📦 Confirmed-first fill for order {}… — no prior Matched, recording to prevent loss",
                        &fill.order_id[..8.min(fill.order_id.len())]
                    );
                    self.settled_ledger.push(FillRecord {
                        side: fill.side,
                        direction: fill.direction,
                        size: signed_size,
                        price: fill.price,
                        ts: fill.ts,
                        kind: PairLedgerEventKind::Fill,
                    });
                }
            }
            FillStatus::Failed => {
                if !self.remove_pending_fill(
                    fill.order_id.as_str(),
                    fill.side,
                    fill.direction,
                    signed_size,
                ) {
                    self.late_failed_after_promotion =
                        self.late_failed_after_promotion.saturating_add(1);
                    warn!(
                        "📦 Late FAILED after timeout promotion or missing pending record for order {}… — keeping settled inventory intact",
                        &fill.order_id[..8.min(fill.order_id.len())]
                    );
                } else {
                    self.failed_reverts = self.failed_reverts.saturating_add(1);
                }
            }
        }

        self.recompute_snapshot();
    }

    fn apply_merge(&mut self, full_set_size: f64, _merge_id: &str, merge_ts: Instant) {
        let requested = full_set_size.max(0.0);
        if requested <= f64::EPSILON {
            return;
        }

        let materialized_pending = self.materialize_pending_for_merge();
        let current = Self::recompute_state_from_records(&self.settled_ledger);
        let available_full_set = current.yes_qty.min(current.no_qty).max(0.0);
        let amount = requested.min(available_full_set);
        if amount <= f64::EPSILON {
            if materialized_pending {
                self.recompute_snapshot();
            }
            return;
        }

        self.settled_ledger.push(FillRecord {
            side: Side::Yes,
            direction: TradeDirection::Sell,
            size: -amount,
            price: current.yes_avg_cost.max(0.0),
            ts: merge_ts,
            kind: PairLedgerEventKind::Merge,
        });
        self.settled_ledger.push(FillRecord {
            side: Side::No,
            direction: TradeDirection::Sell,
            size: -amount,
            price: current.no_avg_cost.max(0.0),
            ts: merge_ts,
            kind: PairLedgerEventKind::Merge,
        });

        self.recompute_snapshot();
    }

    fn materialize_pending_for_merge(&mut self) -> bool {
        if self.pending_fills.is_empty() {
            return false;
        }

        let mut promoted_yes = 0.0;
        let mut promoted_no = 0.0;
        for pending in self.pending_fills.drain(..) {
            match pending.side {
                Side::Yes => promoted_yes += pending.size.abs(),
                Side::No => promoted_no += pending.size.abs(),
            }
            self.settled_ledger.push(FillRecord {
                side: pending.side,
                direction: pending.direction,
                size: pending.size,
                price: pending.price,
                ts: pending.matched_at,
                kind: PairLedgerEventKind::Fill,
            });
        }

        info!(
            "📦 Merge materialized pending fills into settled | promoted_yes={:.2} promoted_no={:.2}",
            promoted_yes, promoted_no,
        );
        true
    }

    fn recompute_snapshot(&mut self) {
        let prev = self.snapshot;
        self.snapshot.settled = Self::recompute_state_from_records(&self.settled_ledger);
        let mut working_records = self.settled_ledger.clone();
        working_records.extend(self.pending_fills.iter().map(|pending| FillRecord {
            side: pending.side,
            direction: pending.direction,
            size: pending.size,
            price: pending.price,
            ts: pending.matched_at,
            kind: PairLedgerEventKind::Fill,
        }));
        self.snapshot.working = Self::recompute_state_from_records(&working_records);
        self.snapshot.pending_yes_qty =
            (self.snapshot.working.yes_qty - self.snapshot.settled.yes_qty).max(0.0);
        self.snapshot.pending_no_qty =
            (self.snapshot.working.no_qty - self.snapshot.settled.no_qty).max(0.0);
        self.snapshot.fragile =
            self.snapshot.pending_yes_qty > 1e-9 || self.snapshot.pending_no_qty > 1e-9;
        let ledger = self.build_pair_ledger_snapshot(&working_records);
        self.snapshot.pair_ledger = ledger.snapshot;
        self.snapshot.episode_metrics = ledger.episode_metrics;
        self.emit_pair_ledger_events(prev);
    }

    fn recompute_state_from_records(records: &[FillRecord]) -> InventoryState {
        let (mut yes_qty, mut yes_avg) = (0.0_f64, 0.0_f64);
        let (mut no_qty, mut no_avg) = (0.0_f64, 0.0_f64);

        for r in records {
            let fill_size = r.size.abs();
            if fill_size <= f64::EPSILON {
                continue;
            }
            match (r.side, r.direction) {
                (Side::Yes, TradeDirection::Buy) => {
                    let next_qty = yes_qty + fill_size;
                    if next_qty > f64::EPSILON {
                        yes_avg = ((yes_qty * yes_avg) + (fill_size * r.price)) / next_qty;
                        yes_qty = next_qty;
                    }
                }
                (Side::No, TradeDirection::Buy) => {
                    let next_qty = no_qty + fill_size;
                    if next_qty > f64::EPSILON {
                        no_avg = ((no_qty * no_avg) + (fill_size * r.price)) / next_qty;
                        no_qty = next_qty;
                    }
                }
                (Side::Yes, TradeDirection::Sell) => {
                    if yes_qty <= f64::EPSILON {
                        warn!(
                            "📦 YES sell fill exceeds tracked inventory (size={:.8}); clamping to flat",
                            fill_size
                        );
                        yes_qty = 0.0;
                        yes_avg = 0.0;
                        continue;
                    }
                    yes_qty = (yes_qty - fill_size).max(0.0);
                    if yes_qty <= 1e-9 {
                        yes_qty = 0.0;
                        yes_avg = 0.0;
                    }
                }
                (Side::No, TradeDirection::Sell) => {
                    if no_qty <= f64::EPSILON {
                        warn!(
                            "📦 NO sell fill exceeds tracked inventory (size={:.8}); clamping to flat",
                            fill_size
                        );
                        no_qty = 0.0;
                        no_avg = 0.0;
                        continue;
                    }
                    no_qty = (no_qty - fill_size).max(0.0);
                    if no_qty <= 1e-9 {
                        no_qty = 0.0;
                        no_avg = 0.0;
                    }
                }
            }
        }

        let mut state = InventoryState::default();
        state.yes_qty = yes_qty;
        state.no_qty = no_qty;
        state.yes_avg_cost = yes_avg.max(0.0);
        state.no_avg_cost = no_avg.max(0.0);
        state.net_diff = state.yes_qty - state.no_qty;
        state.portfolio_cost = if state.yes_qty > 0.0 && state.no_qty > 0.0 {
            state.yes_avg_cost + state.no_avg_cost
        } else {
            0.0
        };
        state
    }

    fn remove_pending_fill(
        &mut self,
        order_id: &str,
        side: Side,
        direction: TradeDirection,
        signed_size: f64,
    ) -> bool {
        if let Some(idx) = self.pending_fills.iter().position(|r| {
            r.order_id == order_id
                && r.side == side
                && r.direction == direction
                && (r.size - signed_size).abs() < 1e-6
        }) {
            self.pending_fills.remove(idx);
            true
        } else if let Some(idx) = self
            .pending_fills
            .iter()
            .position(|r| r.order_id == order_id && r.side == side && r.direction == direction)
        {
            self.pending_fills.remove(idx);
            true
        } else {
            false
        }
    }

    fn promote_pending_fill(
        &mut self,
        order_id: &str,
        side: Side,
        direction: TradeDirection,
        signed_size: f64,
        price_override: Option<f64>,
        reason: &str,
    ) -> bool {
        if let Some(idx) = self.pending_fills.iter().position(|r| {
            r.order_id == order_id
                && r.side == side
                && r.direction == direction
                && (r.size - signed_size).abs() < 1e-6
        }) {
            let pending = self.pending_fills.remove(idx);
            self.settled_ledger.push(FillRecord {
                side: pending.side,
                direction: pending.direction,
                size: pending.size,
                price: price_override.unwrap_or(pending.price),
                ts: pending.matched_at,
                kind: PairLedgerEventKind::Fill,
            });
            self.confirmed_promotions = self.confirmed_promotions.saturating_add(1);
            info!(
                "📦 Pending fill promoted to settled ({}) for order {}…",
                reason,
                &order_id[..8.min(order_id.len())]
            );
            true
        } else {
            false
        }
    }

    fn promote_expired_pending(&mut self, now: Instant) -> bool {
        let mut changed = false;
        let mut idx = 0;
        while idx < self.pending_fills.len() {
            if now.saturating_duration_since(self.pending_fills[idx].matched_at)
                >= PENDING_PROMOTION_TIMEOUT
            {
                let pending = self.pending_fills.remove(idx);
                info!(
                    "📦 Pending fill timeout-promoted to settled after {}ms for order {}…",
                    PENDING_PROMOTION_TIMEOUT.as_millis(),
                    &pending.order_id[..8.min(pending.order_id.len())]
                );
                self.settled_ledger.push(FillRecord {
                    side: pending.side,
                    direction: pending.direction,
                    size: pending.size,
                    price: pending.price,
                    ts: pending.matched_at,
                    kind: PairLedgerEventKind::Fill,
                });
                self.timeout_promotions = self.timeout_promotions.saturating_add(1);
                changed = true;
                continue;
            }
            idx += 1;
        }

        if changed {
            self.recompute_snapshot();
        }
        changed
    }

    fn build_pair_ledger_snapshot(&self, working_records: &[FillRecord]) -> PairLedgerBuildResult {
        let mut events = working_records
            .iter()
            .map(|record| PairLedgerEvent {
                side: record.side,
                direction: record.direction,
                size: record.size.abs(),
                price: record.price,
                ts: record.ts,
                kind: record.kind,
            })
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.ts);
        build_pair_ledger(&events, PathKind::MakerShadow)
    }

    fn emit_pair_ledger_events(&self, prev: InventorySnapshot) {
        let curr = self.snapshot;
        let prev_active = prev.pair_ledger.active_tranche;
        let curr_active = curr.pair_ledger.active_tranche;
        let prev_closed = prev.pair_ledger.recent_closed[0];
        let curr_closed = curr.pair_ledger.recent_closed[0];

        if prev_active.is_none() && curr_active.is_some() {
            if let Some(active) = curr_active {
                self.emit_inventory_event(
                    "tranche_opened",
                    serde_json::json!({
                        "tranche_id": active.id,
                        "first_side": active.first_side.map(|side| side.as_str()),
                        "first_qty": active.first_qty,
                        "first_vwap": active.first_vwap,
                        "state": format!("{:?}", active.state),
                        "path_kind": format!("{:?}", active.path_kind),
                    }),
                );
            }
        }

        if prev_active.map(|tranche| tranche.state) != curr_active.map(|tranche| tranche.state) {
            if let Some(active) =
                curr_active.filter(|active| active.state == TrancheState::CompletionOnly)
            {
                self.emit_inventory_event(
                    "tranche_entered_completion_only",
                    serde_json::json!({
                        "tranche_id": active.id,
                        "residual_qty": active.residual_qty,
                        "pairable_qty": active.pairable_qty,
                        "pair_cost_tranche": active.pair_cost_tranche,
                        "pair_cost_fifo_ref": active.pair_cost_fifo_ref,
                    }),
                );
            }
        }

        if prev_closed.map(|tranche| tranche.id) != curr_closed.map(|tranche| tranche.id) {
            if let Some(closed) = curr_closed {
                self.emit_inventory_event(
                    "tranche_pair_covered",
                    serde_json::json!({
                        "tranche_id": closed.id,
                        "pairable_qty": closed.pairable_qty,
                        "pair_cost_tranche": closed.pair_cost_tranche,
                        "pair_cost_fifo_ref": closed.pair_cost_fifo_ref,
                        "gross_surplus": closed.gross_surplus,
                        "spendable_surplus": closed.spendable_surplus,
                    }),
                );
            }
        }

        if let (Some(prev_active), Some(curr_active)) = (prev_active, curr_active) {
            if prev_active.id == curr_active.id
                && curr_active.first_qty > prev_active.first_qty + 1e-9
                && curr_active.hedge_qty <= prev_active.hedge_qty + 1e-9
            {
                self.emit_inventory_event(
                    "same_side_add_before_covered",
                    serde_json::json!({
                        "tranche_id": curr_active.id,
                        "prev_first_qty": prev_active.first_qty,
                        "curr_first_qty": curr_active.first_qty,
                        "same_side_add_qty_ratio": curr.episode_metrics.same_side_add_qty_ratio,
                    }),
                );
            }
        }

        if prev_active.map(|tranche| tranche.id) != curr_active.map(|tranche| tranche.id)
            && prev_closed.map(|tranche| tranche.id) != curr_closed.map(|tranche| tranche.id)
            && curr_active.is_some()
        {
            if let Some(active) = curr_active {
                self.emit_inventory_event(
                    "tranche_overshoot_rolled",
                    serde_json::json!({
                        "new_tranche_id": active.id,
                        "first_side": active.first_side.map(|side| side.as_str()),
                        "first_qty": active.first_qty,
                    }),
                );
            }
        }

        if (curr.pair_ledger.surplus_bank - prev.pair_ledger.surplus_bank).abs() > 1e-9 {
            self.emit_inventory_event(
                "surplus_bank_updated",
                serde_json::json!({
                    "surplus_bank": curr.pair_ledger.surplus_bank,
                    "repair_budget_available": curr.pair_ledger.repair_budget_available,
                }),
            );
        }

        if curr.pair_ledger.repair_budget_available + 1e-9
            < prev.pair_ledger.repair_budget_available
        {
            self.emit_inventory_event(
                "repair_budget_spent",
                serde_json::json!({
                    "before": prev.pair_ledger.repair_budget_available,
                    "after": curr.pair_ledger.repair_budget_available,
                }),
            );
        }

        if curr.pair_ledger.total_pairable_qty() + 1e-9 < prev.pair_ledger.total_pairable_qty() {
            self.emit_inventory_event(
                "merge_pairable_reduced",
                serde_json::json!({
                    "before": prev.pair_ledger.total_pairable_qty(),
                    "after": curr.pair_ledger.total_pairable_qty(),
                }),
            );
        }

        self.emit_inventory_event(
            "capital_state_snapshot",
            serde_json::json!({
                "working_capital": curr.pair_ledger.capital_state.working_capital,
                "locked_in_active_tranches": curr.pair_ledger.capital_state.locked_in_active_tranches,
                "locked_in_pair_covered": curr.pair_ledger.capital_state.locked_in_pair_covered,
                "mergeable_full_sets": curr.pair_ledger.capital_state.mergeable_full_sets,
                "locked_capital_ratio": curr.pair_ledger.capital_state.locked_capital_ratio,
                "would_block_new_open_due_to_capital": curr.pair_ledger.capital_state.would_block_new_open_due_to_capital,
                "would_trigger_merge_due_to_capital": curr.pair_ledger.capital_state.would_trigger_merge_due_to_capital,
                "capital_pressure_merge_batch_shadow": curr.pair_ledger.capital_state.capital_pressure_merge_batch_shadow,
                "clean_closed_episode_ratio": curr.episode_metrics.clean_closed_episode_ratio,
                "same_side_add_qty_ratio": curr.episode_metrics.same_side_add_qty_ratio,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::recorder::{
        RecorderConfig, RecorderHandle, RecorderMarketMode, RecorderSessionMeta,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_fill(side: Side, size: f64, price: f64) -> FillEvent {
        FillEvent {
            order_id: "test-order".to_string(),
            side,
            direction: TradeDirection::Buy,
            filled_size: size,
            price,
            status: FillStatus::Matched,
            ts: Instant::now(),
        }
    }

    fn make_manager() -> InventoryManager {
        let (state_tx, _state_rx) = watch::channel(InventorySnapshot::default());
        let (_fill_tx, fill_rx) = mpsc::channel(16);
        InventoryManager::new(InventoryConfig::default(), fill_rx, state_tx, None, None)
    }

    fn temp_root(prefix: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("pm_as_ofi_{}_{}", prefix, ts))
    }

    #[test]
    fn matched_updates_working_and_sets_fragile() {
        let mut im = make_manager();
        im.apply_fill(&make_fill(Side::Yes, 10.0, 0.50));
        assert!((im.snapshot.working.yes_qty - 10.0).abs() < 1e-9);
        assert!(im.snapshot.settled.yes_qty.abs() < 1e-9);
        assert!(im.snapshot.fragile);
        assert!((im.snapshot.pending_yes_qty - 10.0).abs() < 1e-9);
    }

    #[test]
    fn confirmed_promotes_pending_to_settled() {
        let mut im = make_manager();
        let matched = make_fill(Side::No, 3.0, 0.45);
        im.apply_fill(&matched);
        im.apply_fill(&FillEvent {
            status: FillStatus::Confirmed,
            ..matched.clone()
        });
        assert!((im.snapshot.settled.no_qty - 3.0).abs() < 1e-9);
        assert!((im.snapshot.working.no_qty - 3.0).abs() < 1e-9);
        assert!(!im.snapshot.fragile);
    }

    #[test]
    fn failed_reverts_pending_only() {
        let mut im = make_manager();
        let fill = make_fill(Side::Yes, 5.0, 0.50);
        im.apply_fill(&fill);
        im.apply_fill(&FillEvent {
            status: FillStatus::Failed,
            ..fill.clone()
        });
        assert!(im.snapshot.working.yes_qty.abs() < 1e-9);
        assert!(im.snapshot.settled.yes_qty.abs() < 1e-9);
        assert!(!im.snapshot.fragile);
    }

    #[test]
    fn timeout_promotion_moves_pending_to_settled() {
        let mut im = make_manager();
        let mut fill = make_fill(Side::Yes, 5.0, 0.48);
        fill.ts = Instant::now() - PENDING_PROMOTION_TIMEOUT - Duration::from_millis(1);
        im.apply_fill(&fill);
        assert!(im.snapshot.fragile);
        assert!(im.promote_expired_pending(Instant::now()));
        assert!((im.snapshot.settled.yes_qty - 5.0).abs() < 1e-9);
        assert!(!im.snapshot.fragile);
    }

    #[test]
    fn merge_sync_reduces_settled_inventory_and_keeps_vwap() {
        let mut im = make_manager();
        let yes = FillEvent {
            order_id: "yes-1".to_string(),
            side: Side::Yes,
            direction: TradeDirection::Buy,
            filled_size: 10.0,
            price: 0.40,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        };
        let no = FillEvent {
            order_id: "no-1".to_string(),
            side: Side::No,
            direction: TradeDirection::Buy,
            filled_size: 10.0,
            price: 0.58,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        };
        im.apply_fill(&yes);
        im.apply_fill(&no);
        im.apply_merge(4.0, "m1", Instant::now());
        assert!((im.snapshot.settled.yes_qty - 6.0).abs() < 1e-9);
        assert!((im.snapshot.settled.no_qty - 6.0).abs() < 1e-9);
        assert!((im.snapshot.settled.yes_avg_cost - 0.40).abs() < 1e-9);
        assert!((im.snapshot.settled.no_avg_cost - 0.58).abs() < 1e-9);
    }

    #[test]
    fn merge_sync_consumes_pending_without_leaving_phantom_working_residual() {
        let mut im = make_manager();
        let yes = FillEvent {
            order_id: "yes-1".to_string(),
            side: Side::Yes,
            direction: TradeDirection::Buy,
            filled_size: 15.0,
            price: 0.50,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        };
        let no = FillEvent {
            order_id: "no-1".to_string(),
            side: Side::No,
            direction: TradeDirection::Buy,
            filled_size: 10.0,
            price: 0.41,
            status: FillStatus::Confirmed,
            ts: Instant::now(),
        };
        let pending_no = FillEvent {
            order_id: "no-2".to_string(),
            side: Side::No,
            direction: TradeDirection::Buy,
            filled_size: 5.0,
            price: 0.51,
            status: FillStatus::Matched,
            ts: Instant::now(),
        };

        im.apply_fill(&yes);
        im.apply_fill(&no);
        im.apply_fill(&pending_no);
        assert!((im.snapshot.working.net_diff - 0.0).abs() < 1e-9);
        assert!((im.snapshot.pending_no_qty - 5.0).abs() < 1e-9);
        assert!(im.snapshot.fragile);

        im.apply_merge(15.0, "m2", Instant::now());
        assert!(im.snapshot.working.net_diff.abs() < 1e-9);
        assert!(im.snapshot.settled.net_diff.abs() < 1e-9);
        assert!(im.snapshot.pending_no_qty.abs() < 1e-9);
        assert!(im.snapshot.pending_yes_qty.abs() < 1e-9);
        assert!(!im.snapshot.fragile);
    }

    #[test]
    fn late_failed_after_timeout_does_not_rollback_settled() {
        let mut im = make_manager();
        let mut fill = make_fill(Side::No, 5.0, 0.63);
        fill.ts = Instant::now() - PENDING_PROMOTION_TIMEOUT - Duration::from_millis(1);
        im.apply_fill(&fill);
        assert!(im.promote_expired_pending(Instant::now()));
        im.apply_fill(&FillEvent {
            status: FillStatus::Failed,
            ..fill
        });
        assert!((im.snapshot.settled.no_qty - 5.0).abs() < 1e-9);
        assert!((im.snapshot.working.no_qty - 5.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn run_loop_fill_emits_tranche_events_to_recorder() {
        let root = temp_root("inventory_recorder");
        let recorder = RecorderHandle::from_config(&RecorderConfig {
            enabled: true,
            root: root.clone(),
            md_queue_cap: 32,
            ops_queue_cap: 32,
            flush_every: Duration::from_millis(5),
            market_mode: RecorderMarketMode::Structured,
        });
        let meta = RecorderSessionMeta {
            slug: "btc-updown-5m-test".to_string(),
            condition_id: "0xcond".to_string(),
            market_id: "0xmarket".to_string(),
            strategy: "pair_gated_tranche_arb".to_string(),
            dry_run: true,
        };

        let (event_tx, event_rx) = mpsc::channel(8);
        let (state_tx, _state_rx) = watch::channel(InventorySnapshot::default());
        let manager = InventoryManager::new(
            InventoryConfig::default(),
            event_rx,
            state_tx,
            Some(recorder),
            Some(meta),
        );
        let handle = tokio::spawn(manager.run());
        let _ = event_tx
            .send(InventoryEvent::Fill(FillEvent {
                order_id: "dry-fill-1".to_string(),
                side: Side::Yes,
                direction: TradeDirection::Buy,
                filled_size: 5.0,
                price: 0.48,
                status: FillStatus::Confirmed,
                ts: Instant::now(),
            }))
            .await;
        drop(event_tx);
        let _ = handle.await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        let date_dir = fs::read_dir(&root)
            .expect("date dir should exist")
            .flatten()
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .expect("one date dir");
        let events_path = date_dir.join("btc-updown-5m-test").join("events.jsonl");
        let text = fs::read_to_string(&events_path).expect("events.jsonl should exist");
        assert!(
            text.contains("\"event\":\"tranche_opened\""),
            "expected tranche_opened in events stream, got: {}",
            text
        );
        let _ = fs::remove_dir_all(root);
    }
}
