use std::collections::VecDeque;
use std::time::Instant;

use super::messages::TradeDirection;
use super::types::Side;

pub const PGT_RECENT_CLOSED_LIMIT: usize = 4;
pub const COMPLETION_FIRST_RECENT_CLOSED_LIMIT: usize = PGT_RECENT_CLOSED_LIMIT;
const PAIR_LEDGER_EPS: f64 = 1e-9;
const MIN_EDGE_PER_PAIR: f64 = 0.005;
const REPAIR_BUDGET_FRACTION: f64 = 0.50;
const CAPITAL_BLOCK_RATIO: f64 = 0.60;
const CAPITAL_MERGE_RATIO: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PathKind {
    #[default]
    MakerShadow,
    TakerShadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TrancheState {
    #[default]
    FlatOrResidual,
    FirstLegPending,
    CompletionOnly,
    PairCovered,
    MergeQueued,
    Closed,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PairTranche {
    pub id: u64,
    pub state: TrancheState,
    pub first_side: Option<Side>,
    pub first_qty: f64,
    pub first_vwap: f64,
    pub hedge_qty: f64,
    pub hedge_vwap: f64,
    pub residual_qty: f64,
    pub pairable_qty: f64,
    pub pair_cost_tranche: f64,
    pub pair_cost_fifo_ref: f64,
    pub gross_surplus: f64,
    pub spendable_surplus: f64,
    pub repair_spent: f64,
    pub same_side_add_count: u32,
    pub opened_at: Option<Instant>,
    pub closed_at: Option<Instant>,
    pub last_transition_at: Option<Instant>,
    pub path_kind: PathKind,
}

impl PairTranche {
    pub fn is_active(self) -> bool {
        matches!(
            self.state,
            TrancheState::FirstLegPending
                | TrancheState::CompletionOnly
                | TrancheState::FlatOrResidual
        ) && self.residual_qty > PAIR_LEDGER_EPS
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CapitalState {
    pub working_capital: f64,
    pub locked_in_active_tranches: f64,
    pub locked_in_pair_covered: f64,
    pub mergeable_full_sets: f64,
    pub locked_capital_ratio: f64,
    pub would_block_new_open_due_to_capital: bool,
    pub would_trigger_merge_due_to_capital: bool,
    pub capital_pressure_merge_batch_shadow: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct PairLedgerSnapshot {
    pub active_tranche: Option<PairTranche>,
    pub residual_side: Option<Side>,
    pub residual_qty: f64,
    pub buy_fill_count: u64,
    pub completed_pair_count: u64,
    pub surplus_bank: f64,
    pub repair_budget_available: f64,
    pub capital_state: CapitalState,
    pub recent_closed: [Option<PairTranche>; PGT_RECENT_CLOSED_LIMIT],
}

impl Default for PairLedgerSnapshot {
    fn default() -> Self {
        Self {
            active_tranche: None,
            residual_side: None,
            residual_qty: 0.0,
            buy_fill_count: 0,
            completed_pair_count: 0,
            surplus_bank: 0.0,
            repair_budget_available: 0.0,
            capital_state: CapitalState::default(),
            recent_closed: [None; PGT_RECENT_CLOSED_LIMIT],
        }
    }
}

impl PairLedgerSnapshot {
    pub fn total_pairable_qty(&self) -> f64 {
        let recent = self
            .recent_closed
            .iter()
            .flatten()
            .map(|tranche| tranche.pairable_qty.max(0.0))
            .sum::<f64>();
        recent
            + self
                .active_tranche
                .map(|tranche| tranche.pairable_qty.max(0.0))
                .unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EpisodeMetrics {
    pub clean_closed_episode_ratio: f64,
    pub same_side_add_qty_ratio: f64,
    pub residual_before_new_open_p90: f64,
    pub episode_close_delay_p50: f64,
    pub episode_close_delay_p90: f64,
    pub round_buy_fill_count: u64,
    pub completed_pair_count: u64,
    pub conditional_second_same_side_would_allow: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairLedgerEventKind {
    Fill,
    Merge,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PairLedgerEvent {
    pub(crate) side: Side,
    pub(crate) direction: TradeDirection,
    pub(crate) size: f64,
    pub(crate) price: f64,
    pub(crate) ts: Instant,
    pub(crate) kind: PairLedgerEventKind,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PairLedgerBuildResult {
    pub(crate) snapshot: PairLedgerSnapshot,
    pub(crate) episode_metrics: EpisodeMetrics,
}

#[derive(Debug, Clone, Copy)]
struct Lot {
    qty: f64,
    price: f64,
}

#[derive(Debug, Clone)]
struct WorkingTranche {
    snapshot: PairTranche,
    first_lots: VecDeque<Lot>,
    hedge_lots: VecDeque<Lot>,
    same_side_add_qty: f64,
}

impl WorkingTranche {
    fn new(id: u64, side: Side, qty: f64, price: f64, ts: Instant, path_kind: PathKind) -> Self {
        let mut tranche = Self {
            snapshot: PairTranche {
                id,
                state: TrancheState::FirstLegPending,
                first_side: Some(side),
                first_qty: 0.0,
                first_vwap: 0.0,
                hedge_qty: 0.0,
                hedge_vwap: 0.0,
                residual_qty: 0.0,
                pairable_qty: 0.0,
                pair_cost_tranche: 0.0,
                pair_cost_fifo_ref: 0.0,
                gross_surplus: 0.0,
                spendable_surplus: 0.0,
                repair_spent: 0.0,
                same_side_add_count: 0,
                opened_at: Some(ts),
                closed_at: None,
                last_transition_at: Some(ts),
                path_kind,
            },
            first_lots: VecDeque::new(),
            hedge_lots: VecDeque::new(),
            same_side_add_qty: 0.0,
        };
        tranche.add_first(qty, price, ts, false);
        tranche
    }

    fn first_side(&self) -> Option<Side> {
        self.snapshot.first_side
    }

    fn add_first(&mut self, qty: f64, price: f64, ts: Instant, mark_same_side_add: bool) {
        if qty <= PAIR_LEDGER_EPS {
            return;
        }
        self.first_lots.push_back(Lot { qty, price });
        if mark_same_side_add {
            self.same_side_add_qty += qty.max(0.0);
            self.snapshot.same_side_add_count = self.snapshot.same_side_add_count.saturating_add(1);
        }
        self.snapshot.last_transition_at = Some(ts);
        self.recompute();
    }

    fn add_hedge(&mut self, qty: f64, price: f64, ts: Instant) {
        if qty <= PAIR_LEDGER_EPS {
            return;
        }
        self.hedge_lots.push_back(Lot { qty, price });
        self.snapshot.closed_at = Some(ts);
        self.snapshot.last_transition_at = Some(ts);
        self.recompute();
    }

    fn apply_merge(&mut self, mut qty: f64, ts: Instant) -> f64 {
        if qty <= PAIR_LEDGER_EPS || self.snapshot.pairable_qty <= PAIR_LEDGER_EPS {
            return qty;
        }
        let mergeable = qty.min(self.snapshot.pairable_qty.max(0.0));
        qty -= mergeable;
        consume_lots_fifo(&mut self.first_lots, mergeable);
        consume_lots_fifo(&mut self.hedge_lots, mergeable);
        self.snapshot.closed_at = Some(ts);
        self.snapshot.last_transition_at = Some(ts);
        self.recompute();
        if self.snapshot.pairable_qty <= PAIR_LEDGER_EPS
            && self.snapshot.residual_qty <= PAIR_LEDGER_EPS
        {
            self.snapshot.state = TrancheState::Closed;
        } else if self.snapshot.pairable_qty <= PAIR_LEDGER_EPS {
            self.snapshot.state = TrancheState::CompletionOnly;
        } else if self.snapshot.residual_qty <= PAIR_LEDGER_EPS {
            self.snapshot.state = TrancheState::MergeQueued;
        } else {
            self.snapshot.state = TrancheState::CompletionOnly;
        }
        qty
    }

    fn recompute(&mut self) {
        let first_qty = sum_lots(&self.first_lots);
        let hedge_qty = sum_lots(&self.hedge_lots);
        let pairable_qty = first_qty.min(hedge_qty).max(0.0);
        let residual_qty = (first_qty - hedge_qty).abs().max(0.0);
        let first_vwap = weighted_avg(&self.first_lots);
        let hedge_vwap = weighted_avg(&self.hedge_lots);
        let pair_cost_tranche = if pairable_qty > PAIR_LEDGER_EPS {
            first_vwap + hedge_vwap
        } else {
            0.0
        };
        let pair_cost_fifo_ref = if pairable_qty > PAIR_LEDGER_EPS {
            fifo_avg_for_qty(&self.first_lots, pairable_qty)
                + fifo_avg_for_qty(&self.hedge_lots, pairable_qty)
        } else {
            0.0
        };
        let gross_surplus = pairable_qty * (1.0 - pair_cost_tranche).max(0.0);
        let spendable_surplus =
            pairable_qty * (1.0 - pair_cost_tranche - MIN_EDGE_PER_PAIR).max(0.0);
        let repair_spent = pairable_qty * (pair_cost_tranche + MIN_EDGE_PER_PAIR - 1.0).max(0.0);

        self.snapshot.first_qty = first_qty;
        self.snapshot.first_vwap = first_vwap;
        self.snapshot.hedge_qty = hedge_qty;
        self.snapshot.hedge_vwap = hedge_vwap;
        self.snapshot.residual_qty = residual_qty;
        self.snapshot.pairable_qty = pairable_qty;
        self.snapshot.pair_cost_tranche = pair_cost_tranche;
        self.snapshot.pair_cost_fifo_ref = pair_cost_fifo_ref;
        self.snapshot.gross_surplus = gross_surplus;
        self.snapshot.spendable_surplus = spendable_surplus;
        self.snapshot.repair_spent = repair_spent;
        self.snapshot.state = if first_qty <= PAIR_LEDGER_EPS && hedge_qty <= PAIR_LEDGER_EPS {
            TrancheState::Closed
        } else if pairable_qty <= PAIR_LEDGER_EPS {
            TrancheState::FirstLegPending
        } else if residual_qty <= PAIR_LEDGER_EPS {
            TrancheState::PairCovered
        } else {
            TrancheState::CompletionOnly
        };
    }
}

#[derive(Debug, Default)]
struct EpisodeStats {
    buy_fill_count: u64,
    total_open_qty: f64,
    total_closed: u64,
    clean_closed: u64,
    same_side_add_qty: f64,
    residual_before_new_open: Vec<f64>,
    close_delays_secs: Vec<f64>,
    conditional_second_same_side_would_allow: u64,
}

#[derive(Debug)]
struct PairLedgerBuilder {
    next_id: u64,
    active: Option<WorkingTranche>,
    covered: Vec<WorkingTranche>,
    archived: Vec<WorkingTranche>,
    stats: EpisodeStats,
    path_kind: PathKind,
}

impl PairLedgerBuilder {
    fn new(path_kind: PathKind) -> Self {
        Self {
            next_id: 1,
            active: None,
            covered: Vec::new(),
            archived: Vec::new(),
            stats: EpisodeStats::default(),
            path_kind,
        }
    }

    fn push_buy(&mut self, side: Side, qty: f64, price: f64, ts: Instant) {
        if qty <= PAIR_LEDGER_EPS {
            return;
        }
        self.stats.buy_fill_count = self.stats.buy_fill_count.saturating_add(1);
        if let Some(active) = self.active.as_mut() {
            if active.first_side() == Some(side) {
                active.add_first(qty, price, ts, true);
                self.stats.same_side_add_qty += qty.max(0.0);
                self.stats.conditional_second_same_side_would_allow = self
                    .stats
                    .conditional_second_same_side_would_allow
                    .saturating_add(1);
                return;
            }

            let residual_before = active.snapshot.residual_qty.max(0.0);
            let consumed = qty.min(residual_before);
            if consumed > PAIR_LEDGER_EPS {
                active.add_hedge(consumed, price, ts);
            }
            let overshoot = (qty - consumed).max(0.0);
            if active.snapshot.residual_qty <= PAIR_LEDGER_EPS {
                let mut closed = self.active.take().expect("active tranche present");
                closed.snapshot.closed_at.get_or_insert(ts);
                self.record_close(&closed.snapshot);
                self.covered.push(closed);
            }
            if overshoot > PAIR_LEDGER_EPS {
                self.stats.residual_before_new_open.push(0.0);
                self.open_new_tranche(side, overshoot, price, ts);
            }
            return;
        }

        self.stats.residual_before_new_open.push(0.0);
        self.open_new_tranche(side, qty, price, ts);
    }

    fn apply_merge(&mut self, mut qty: f64, ts: Instant) {
        if qty <= PAIR_LEDGER_EPS {
            return;
        }

        let mut idx = 0;
        while idx < self.covered.len() && qty > PAIR_LEDGER_EPS {
            qty = self.covered[idx].apply_merge(qty, ts);
            if self.covered[idx].snapshot.state == TrancheState::Closed {
                let closed = self.covered.remove(idx);
                self.archived.push(closed);
                continue;
            }
            idx += 1;
        }

        if qty > PAIR_LEDGER_EPS {
            if let Some(active) = self.active.as_mut() {
                let leftover = active.apply_merge(qty, ts);
                qty = leftover;
            }
        }

        let _ = qty;
    }

    fn open_new_tranche(&mut self, side: Side, qty: f64, price: f64, ts: Instant) {
        if qty <= PAIR_LEDGER_EPS {
            return;
        }
        self.stats.total_open_qty += qty.max(0.0);
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.active = Some(WorkingTranche::new(
            id,
            side,
            qty,
            price,
            ts,
            self.path_kind,
        ));
    }

    fn record_close(&mut self, tranche: &PairTranche) {
        self.stats.total_closed = self.stats.total_closed.saturating_add(1);
        if tranche.closed_at.is_some() && tranche.opened_at.is_some() {
            let delay = tranche
                .closed_at
                .zip(tranche.opened_at)
                .map(|(closed, opened)| closed.saturating_duration_since(opened).as_secs_f64())
                .unwrap_or_default();
            self.stats.close_delays_secs.push(delay);
        }
        if tranche.residual_qty <= PAIR_LEDGER_EPS {
            self.stats.clean_closed = self.stats.clean_closed.saturating_add(1);
        }
    }

    fn build(mut self) -> PairLedgerBuildResult {
        self.covered.sort_by_key(|tranche| tranche.snapshot.id);
        self.archived.sort_by_key(|tranche| tranche.snapshot.id);

        let recent_closed = collect_recent_closed(&self.covered, &self.archived);
        let active_snapshot = self.active.as_ref().map(|tranche| tranche.snapshot);
        let residual_side = self.active.as_ref().and_then(|tranche| {
            if tranche.snapshot.residual_qty > PAIR_LEDGER_EPS {
                tranche.snapshot.first_side
            } else {
                None
            }
        });
        let residual_qty = self
            .active
            .as_ref()
            .map(|tranche| tranche.snapshot.residual_qty.max(0.0))
            .unwrap_or(0.0);
        let covered_surplus = self
            .covered
            .iter()
            .map(|tranche| tranche.snapshot.spendable_surplus.max(0.0))
            .sum::<f64>();
        let covered_repair = self
            .covered
            .iter()
            .map(|tranche| tranche.snapshot.repair_spent.max(0.0))
            .sum::<f64>();
        let repair_budget_available =
            (covered_surplus * REPAIR_BUDGET_FRACTION - covered_repair).max(0.0);
        let surplus_bank = (covered_surplus - covered_repair).max(0.0);

        let active_locked = active_snapshot
            .map(active_locked_capital)
            .unwrap_or_default()
            .max(0.0);
        let covered_locked = self
            .covered
            .iter()
            .map(|tranche| tranche.snapshot.pairable_qty * tranche.snapshot.pair_cost_tranche)
            .sum::<f64>()
            .max(0.0);
        let working_capital = active_locked + covered_locked;
        let locked_capital_ratio = if working_capital > PAIR_LEDGER_EPS {
            active_locked / working_capital
        } else {
            0.0
        };
        let capital_state = CapitalState {
            working_capital,
            locked_in_active_tranches: active_locked,
            locked_in_pair_covered: covered_locked,
            mergeable_full_sets: self
                .covered
                .iter()
                .map(|tranche| tranche.snapshot.pairable_qty.max(0.0))
                .sum::<f64>(),
            locked_capital_ratio,
            would_block_new_open_due_to_capital: locked_capital_ratio >= CAPITAL_BLOCK_RATIO,
            would_trigger_merge_due_to_capital: locked_capital_ratio >= CAPITAL_MERGE_RATIO,
            capital_pressure_merge_batch_shadow: (locked_capital_ratio * 50.0).round().max(10.0),
        };

        let snapshot = PairLedgerSnapshot {
            active_tranche: active_snapshot,
            residual_side,
            residual_qty,
            buy_fill_count: self.stats.buy_fill_count,
            completed_pair_count: self.stats.total_closed,
            surplus_bank,
            repair_budget_available,
            capital_state,
            recent_closed,
        };
        let episode_metrics = EpisodeMetrics {
            clean_closed_episode_ratio: ratio(self.stats.clean_closed, self.stats.total_closed),
            same_side_add_qty_ratio: if self.stats.total_open_qty > PAIR_LEDGER_EPS {
                self.stats.same_side_add_qty / self.stats.total_open_qty
            } else {
                0.0
            },
            residual_before_new_open_p90: percentile(&self.stats.residual_before_new_open, 0.90),
            episode_close_delay_p50: percentile(&self.stats.close_delays_secs, 0.50),
            episode_close_delay_p90: percentile(&self.stats.close_delays_secs, 0.90),
            round_buy_fill_count: self.stats.buy_fill_count,
            completed_pair_count: self.stats.total_closed,
            conditional_second_same_side_would_allow: self
                .stats
                .conditional_second_same_side_would_allow,
        };

        PairLedgerBuildResult {
            snapshot,
            episode_metrics,
        }
    }
}

pub(crate) fn build_pair_ledger(
    events: &[PairLedgerEvent],
    path_kind: PathKind,
) -> PairLedgerBuildResult {
    let mut builder = PairLedgerBuilder::new(path_kind);
    for event in events {
        let qty = event.size.max(0.0);
        if qty <= PAIR_LEDGER_EPS {
            continue;
        }
        match (event.kind, event.direction) {
            (PairLedgerEventKind::Merge, _) => builder.apply_merge(qty, event.ts),
            (PairLedgerEventKind::Fill, TradeDirection::Buy) => {
                builder.push_buy(event.side, qty, event.price.max(0.0), event.ts)
            }
            (PairLedgerEventKind::Fill, TradeDirection::Sell) => {
                builder.apply_merge(qty, event.ts);
            }
        }
    }
    builder.build()
}

pub fn urgency_budget_shadow_5m(remaining_secs: u64, has_active_tranche: bool) -> f64 {
    if !has_active_tranche {
        return 0.0;
    }
    if remaining_secs > 120 {
        return 0.0;
    }
    if remaining_secs <= 15 {
        return 0.080;
    }
    if remaining_secs <= 30 {
        return 0.070;
    }
    if remaining_secs <= 45 {
        return 0.060;
    }
    if remaining_secs <= 60 {
        return 0.045;
    }
    if remaining_secs <= 90 {
        return 0.025;
    }
    0.010
}

fn sum_lots(lots: &VecDeque<Lot>) -> f64 {
    lots.iter()
        .map(|lot| lot.qty.max(0.0))
        .sum::<f64>()
        .max(0.0)
}

fn weighted_avg(lots: &VecDeque<Lot>) -> f64 {
    let qty = sum_lots(lots);
    if qty <= PAIR_LEDGER_EPS {
        return 0.0;
    }
    lots.iter()
        .map(|lot| lot.qty.max(0.0) * lot.price.max(0.0))
        .sum::<f64>()
        / qty
}

fn fifo_avg_for_qty(lots: &VecDeque<Lot>, target_qty: f64) -> f64 {
    if target_qty <= PAIR_LEDGER_EPS {
        return 0.0;
    }
    let mut remaining = target_qty;
    let mut total_qty = 0.0;
    let mut total_cost = 0.0;
    for lot in lots {
        if remaining <= PAIR_LEDGER_EPS {
            break;
        }
        let take = remaining.min(lot.qty.max(0.0));
        if take <= PAIR_LEDGER_EPS {
            continue;
        }
        remaining -= take;
        total_qty += take;
        total_cost += take * lot.price.max(0.0);
    }
    if total_qty <= PAIR_LEDGER_EPS {
        0.0
    } else {
        total_cost / total_qty
    }
}

fn consume_lots_fifo(lots: &mut VecDeque<Lot>, mut target_qty: f64) {
    while target_qty > PAIR_LEDGER_EPS {
        let Some(mut lot) = lots.pop_front() else {
            break;
        };
        let take = lot.qty.min(target_qty);
        lot.qty -= take;
        target_qty -= take;
        if lot.qty > PAIR_LEDGER_EPS {
            lots.push_front(lot);
            break;
        }
    }
}

fn active_locked_capital(tranche: PairTranche) -> f64 {
    let first_locked = tranche.first_qty.max(0.0) * tranche.first_vwap.max(0.0);
    let hedge_locked = tranche.hedge_qty.max(0.0) * tranche.hedge_vwap.max(0.0);
    first_locked + hedge_locked
}

fn collect_recent_closed(
    covered: &[WorkingTranche],
    archived: &[WorkingTranche],
) -> [Option<PairTranche>; PGT_RECENT_CLOSED_LIMIT] {
    let mut all = covered
        .iter()
        .chain(archived.iter())
        .map(|tranche| tranche.snapshot)
        .collect::<Vec<_>>();
    all.sort_by_key(|tranche| tranche.id);
    all.reverse();
    let mut recent = [None; PGT_RECENT_CLOSED_LIMIT];
    for (idx, tranche) in all.into_iter().take(PGT_RECENT_CLOSED_LIMIT).enumerate() {
        recent[idx] = Some(tranche);
    }
    recent
}

fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let pos = ((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize;
    sorted[pos]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::messages::TradeDirection;

    fn fill(side: Side, size: f64, price: f64, ts: Instant) -> PairLedgerEvent {
        PairLedgerEvent {
            side,
            direction: TradeDirection::Buy,
            size,
            price,
            ts,
            kind: PairLedgerEventKind::Fill,
        }
    }

    fn merge(size: f64, ts: Instant) -> PairLedgerEvent {
        PairLedgerEvent {
            side: Side::Yes,
            direction: TradeDirection::Sell,
            size,
            price: 0.0,
            ts,
            kind: PairLedgerEventKind::Merge,
        }
    }

    #[test]
    fn builds_clean_pair() {
        let now = Instant::now();
        let result = build_pair_ledger(
            &[
                fill(Side::Yes, 100.0, 0.40, now),
                fill(Side::No, 100.0, 0.51, now),
            ],
            PathKind::MakerShadow,
        );
        let closed = result.snapshot.recent_closed[0].expect("closed tranche");
        assert_eq!(closed.first_side, Some(Side::Yes));
        assert!((closed.pairable_qty - 100.0).abs() < 1e-9);
        assert!((closed.residual_qty).abs() < 1e-9);
        assert!((closed.pair_cost_tranche - 0.91).abs() < 1e-9);
        assert!((result.episode_metrics.clean_closed_episode_ratio - 1.0).abs() < 1e-9);
    }

    #[test]
    fn tracks_partial_completion_residual() {
        let now = Instant::now();
        let result = build_pair_ledger(
            &[
                fill(Side::Yes, 100.0, 0.40, now),
                fill(Side::No, 95.0, 0.55, now),
            ],
            PathKind::MakerShadow,
        );
        let active = result.snapshot.active_tranche.expect("active tranche");
        assert_eq!(active.first_side, Some(Side::Yes));
        assert!((active.pairable_qty - 95.0).abs() < 1e-9);
        assert!((active.residual_qty - 5.0).abs() < 1e-9);
        assert!((active.pair_cost_tranche - 0.95).abs() < 1e-9);
    }

    #[test]
    fn records_same_side_add_before_covered() {
        let now = Instant::now();
        let result = build_pair_ledger(
            &[
                fill(Side::Yes, 100.0, 0.40, now),
                fill(Side::No, 80.0, 0.52, now),
                fill(Side::Yes, 50.0, 0.44, now),
            ],
            PathKind::MakerShadow,
        );
        let active = result.snapshot.active_tranche.expect("active tranche");
        assert_eq!(active.first_side, Some(Side::Yes));
        assert!((active.first_qty - 150.0).abs() < 1e-9);
        assert!((active.hedge_qty - 80.0).abs() < 1e-9);
        assert!(result.episode_metrics.same_side_add_qty_ratio > 0.0);
        assert_eq!(
            result
                .episode_metrics
                .conditional_second_same_side_would_allow,
            1
        );
    }

    #[test]
    fn overshoot_rolls_into_new_active_tranche() {
        let now = Instant::now();
        let result = build_pair_ledger(
            &[
                fill(Side::Yes, 100.0, 0.40, now),
                fill(Side::No, 130.0, 0.55, now),
            ],
            PathKind::MakerShadow,
        );
        let closed = result.snapshot.recent_closed[0].expect("closed tranche");
        let active = result.snapshot.active_tranche.expect("new active tranche");
        assert_eq!(closed.first_side, Some(Side::Yes));
        assert!((closed.pairable_qty - 100.0).abs() < 1e-9);
        assert_eq!(active.first_side, Some(Side::No));
        assert!((active.first_qty - 30.0).abs() < 1e-9);
        assert!((active.residual_qty - 30.0).abs() < 1e-9);
    }

    #[test]
    fn exposes_completed_pair_count_beyond_recent_closed_limit() {
        let now = Instant::now();
        let mut events = Vec::new();
        for _ in 0..(PGT_RECENT_CLOSED_LIMIT + 2) {
            events.push(fill(Side::Yes, 10.0, 0.40, now));
            events.push(fill(Side::No, 10.0, 0.58, now));
        }

        let result = build_pair_ledger(&events, PathKind::MakerShadow);

        assert_eq!(
            result.snapshot.completed_pair_count,
            (PGT_RECENT_CLOSED_LIMIT + 2) as u64
        );
        assert_eq!(
            result.episode_metrics.completed_pair_count,
            (PGT_RECENT_CLOSED_LIMIT + 2) as u64
        );
        assert_eq!(
            result.snapshot.recent_closed.iter().flatten().count(),
            PGT_RECENT_CLOSED_LIMIT
        );
        assert!(result.snapshot.active_tranche.is_none());
    }

    #[test]
    fn merge_only_reduces_pairable_on_active_tranche() {
        let now = Instant::now();
        let result = build_pair_ledger(
            &[
                fill(Side::Yes, 100.0, 0.40, now),
                fill(Side::No, 80.0, 0.52, now),
                merge(30.0, now),
            ],
            PathKind::MakerShadow,
        );
        let active = result.snapshot.active_tranche.expect("active tranche");
        assert_eq!(active.state, TrancheState::CompletionOnly);
        assert!((active.pairable_qty - 50.0).abs() < 1e-9);
        assert!((active.residual_qty - 20.0).abs() < 1e-9);
    }

    #[test]
    fn urgency_budget_shadow_5m_steps_up_into_close() {
        assert!((urgency_budget_shadow_5m(121, true) - 0.0).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(100, true) - 0.010).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(90, true) - 0.025).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(60, true) - 0.045).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(45, true) - 0.060).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(30, true) - 0.070).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(15, true) - 0.080).abs() < 1e-9);
        assert!((urgency_budget_shadow_5m(15, false) - 0.0).abs() < 1e-9);
    }
}
