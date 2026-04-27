use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{StrategyCoordinator, PAIR_ARB_NET_EPS};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche};
use crate::polymarket::types::Side;

use super::pair_arb::PairArbStrategy;
use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const RESIDUAL_EPS: f64 = 10.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;
const TAIL_COMPLETION_ONLY_SECS: u64 = 60;
const HARVEST_WINDOW_SECS: u64 = 25;
const HARVEST_MIN_PAIRABLE_QTY: f64 = 10.0;
const BASE_CLIP_QTY: f64 = 120.0;
const MAX_CLIP_QTY: f64 = 250.0;
const MIN_CLIP_QTY: f64 = 25.0;

pub(crate) struct PairGatedTrancheStrategy;

pub(crate) static PAIR_GATED_TRANCHE_STRATEGY: PairGatedTrancheStrategy = PairGatedTrancheStrategy;

impl QuoteStrategy for PairGatedTrancheStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::PairGatedTrancheArb
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let no_new_open = remaining_secs <= TAIL_COMPLETION_ONLY_SECS;
        let harvest_window_active = self.should_shadow_harvest(input, remaining_secs);

        if let Some(active) = input
            .pair_ledger
            .active_tranche
            .filter(|tranche| tranche.first_side.is_some() && tranche.residual_qty > f64::EPSILON)
        {
            if harvest_window_active {
                quotes.note_pgt_skip_harvest();
                return quotes;
            }
            if let Some(intent) = self.completion_intent(coordinator, input, active, remaining_secs)
            {
                quotes.note_pgt_completion_quote();
                quotes.set(intent);
            } else {
                quotes.note_pgt_skip_invalid_book();
            }
            return quotes;
        }

        if harvest_window_active {
            quotes.note_pgt_skip_harvest();
            return quotes;
        }
        if no_new_open {
            quotes.note_pgt_skip_tail_completion_only();
            return quotes;
        }
        if input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
            quotes.note_pgt_skip_residual_guard();
            return quotes;
        }
        if input
            .pair_ledger
            .capital_state
            .would_block_new_open_due_to_capital
        {
            quotes.note_pgt_skip_capital_guard();
            return quotes;
        }

        let Some((raw_yes, raw_no)) = self.flat_seed_raw_prices(coordinator, input) else {
            quotes.note_pgt_skip_invalid_book();
            return quotes;
        };

        let size = self.adaptive_clip_qty(coordinator, input, None, remaining_secs);
        let size = quantize_tenth(size);
        if size <= 0.0 {
            quotes.note_pgt_skip_no_seed();
            return quotes;
        }

        if let Some(seed) =
            self.flat_seed_intent_for_side(coordinator, input, Side::Yes, raw_yes, size)
        {
            quotes.note_pgt_seed_quote();
            quotes.set(seed);
        }
        if let Some(seed) =
            self.flat_seed_intent_for_side(coordinator, input, Side::No, raw_no, size)
        {
            quotes.note_pgt_seed_quote();
            quotes.set(seed);
        }

        if quotes.yes_buy.is_none() && quotes.no_buy.is_none() {
            quotes.note_pgt_skip_no_seed();
        }

        quotes
    }
}

impl PairGatedTrancheStrategy {
    fn should_shadow_harvest(&self, input: StrategyTickInput<'_>, remaining_secs: u64) -> bool {
        remaining_secs <= HARVEST_WINDOW_SECS
            && input.pair_ledger.total_pairable_qty() >= HARVEST_MIN_PAIRABLE_QTY - 1e-9
    }

    fn flat_seed_raw_prices(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> Option<(f64, f64)> {
        let ub = input.book;
        if ub.yes_bid <= 0.0 || ub.yes_ask <= 0.0 || ub.no_bid <= 0.0 || ub.no_ask <= 0.0 {
            return None;
        }

        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no = (ub.no_bid + ub.no_ask) / 2.0;
        let excess = f64::max(0.0, (mid_yes + mid_no) - coordinator.cfg().pair_target);
        let skew = if coordinator.cfg().max_net_diff > 0.0 {
            (input.inv.net_diff / coordinator.cfg().max_net_diff).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let time_decay = coordinator.compute_time_decay_factor();
        let effective_skew_factor = PairArbStrategy::effective_skew_factor(
            coordinator.cfg().as_skew_factor,
            input.inv.net_diff.abs(),
            time_decay,
        );
        let skew_shift = skew * effective_skew_factor;

        Some((
            mid_yes - (excess / 2.0) - skew_shift,
            mid_no - (excess / 2.0) + skew_shift,
        ))
    }

    fn flat_seed_intent_for_side(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        side: Side,
        raw_price: f64,
        size: f64,
    ) -> Option<StrategyIntent> {
        if raw_price <= 0.0 || size <= 0.0 {
            return None;
        }

        let (best_bid, best_ask, opp_avg, same_qty, same_avg) = match side {
            Side::Yes => (
                input.book.yes_bid,
                input.book.yes_ask,
                input.inv.no_avg_cost,
                input.inv.yes_qty,
                input.inv.yes_avg_cost,
            ),
            Side::No => (
                input.book.no_bid,
                input.book.no_ask,
                input.inv.yes_avg_cost,
                input.inv.no_qty,
                input.inv.no_avg_cost,
            ),
        };
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }

        let risk_effect = PairArbStrategy::candidate_risk_effect(input.inv, side, size);
        let mut ceiling = raw_price;
        if let Some(tier_cap) = PairArbStrategy::tier_cap_price_for_candidate(
            input.inv,
            side,
            size,
            risk_effect,
            coordinator.cfg().pair_arb.tier_1_mult,
            coordinator.cfg().pair_arb.tier_2_mult,
        ) {
            ceiling = ceiling.min(tier_cap);
        }

        if opp_avg > 0.0 {
            let effective_pair_cost_margin = if input.inv.net_diff.abs() < PAIR_ARB_NET_EPS {
                0.0
            } else {
                coordinator.cfg().pair_arb.pair_cost_safety_margin
            };
            let vwap_ceiling = PairArbStrategy::vwap_ceiling(
                coordinator.cfg().pair_target,
                effective_pair_cost_margin,
                opp_avg,
                same_qty,
                same_avg,
                size,
            );
            ceiling = ceiling.min(vwap_ceiling);
        }

        let price =
            self.passive_seed_price(coordinator, side, ceiling, best_bid, best_ask)?;
        if price <= 0.0 {
            return None;
        }
        coordinator.simulate_buy(input.inv, side, size, price)?;

        Some(StrategyIntent {
            side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        })
    }

    fn completion_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
        remaining_secs: u64,
    ) -> Option<StrategyIntent> {
        let first_side = active.first_side?;
        let hedge_side = opposite_side(first_side);
        let (best_bid, best_ask) = match hedge_side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };
        if best_ask <= 0.0 || active.first_vwap <= 0.0 || active.residual_qty <= f64::EPSILON {
            return None;
        }

        let safety_margin = coordinator.post_only_safety_margin_for(hedge_side, best_bid, best_ask);
        let repair_budget_per_share =
            input.pair_ledger.repair_budget_available / active.residual_qty.max(1.0);
        let urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true);
        let ceiling =
            1.0 - active.first_vwap - MIN_EDGE_PER_PAIR + repair_budget_per_share + urgency_shadow;
        if ceiling <= 0.0 {
            return None;
        }

        let maker_cap = (best_ask - safety_margin).max(0.0);
        let price = coordinator.safe_price(maker_cap.min(ceiling));
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }

        let size = self
            .adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
            .min(active.residual_qty.max(0.0));
        let size = quantize_tenth(size);
        if size <= 0.0 {
            return None;
        }

        Some(StrategyIntent {
            side: hedge_side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Hedge,
        })
    }

    fn passive_seed_price(
        &self,
        coordinator: &StrategyCoordinator,
        side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> Option<f64> {
        if ceiling <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let tick = coordinator.cfg().tick_size.max(1e-9);
        let safety_margin = coordinator.post_only_safety_margin_for(side, best_bid, best_ask);
        let maker_cap = (best_ask - safety_margin).max(0.0);
        if maker_cap <= 0.0 {
            return None;
        }

        // Flat-state seed should behave like a passive maker: quote at the bid
        // or improve by a single tick when there is enough spread, and treat
        // pair-target / tier / VWAP logic strictly as ceilings rather than as a
        // reason to chase toward the ask.
        let passive_anchor = if best_ask > best_bid + (2.0 * tick) {
            best_bid + tick
        } else {
            best_bid
        };
        let price = coordinator.safe_price(passive_anchor.min(maker_cap).min(ceiling));
        if price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

    fn adaptive_clip_qty(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: Option<PairTranche>,
        remaining_secs: u64,
    ) -> f64 {
        let session_mult = session_clip_mult_utc();
        let imbalance_mult = imbalance_clip_mult(coordinator, input, active);
        let trade_index_mult = active
            .map(|tranche| (1.0 - 0.05 * f64::from(tranche.same_side_add_count)).max(0.70))
            .unwrap_or(1.0);
        let tail_mult = if remaining_secs <= 30 { 1.16 } else { 1.0 };

        (BASE_CLIP_QTY * session_mult * imbalance_mult * trade_index_mult * tail_mult)
            .clamp(MIN_CLIP_QTY, MAX_CLIP_QTY)
    }
}

fn imbalance_clip_mult(
    coordinator: &StrategyCoordinator,
    input: StrategyTickInput<'_>,
    active: Option<PairTranche>,
) -> f64 {
    let abs_imb = if let Some(tranche) = active {
        let denom = tranche
            .first_qty
            .max(tranche.hedge_qty)
            .max(tranche.residual_qty)
            .max(1.0);
        (tranche.residual_qty.max(0.0) / denom).clamp(0.0, 1.0)
    } else {
        (input.inv.net_diff.abs() / coordinator.cfg().max_net_diff.max(1.0)).clamp(0.0, 1.0)
    };

    if abs_imb < 0.05 {
        1.00
    } else if abs_imb < 0.15 {
        0.95
    } else if abs_imb < 0.30 {
        1.00
    } else {
        1.20
    }
}

fn session_clip_mult_utc() -> f64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let hour = ((now_secs / 3600) % 24) as u8;
    if (16..=22).contains(&hour) {
        1.15
    } else if (3..=12).contains(&hour) {
        0.80
    } else {
        1.00
    }
}

fn quantize_tenth(qty: f64) -> f64 {
    let rounded = (qty * 10.0).floor() / 10.0;
    if rounded >= 0.1 {
        rounded
    } else {
        0.0
    }
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}
