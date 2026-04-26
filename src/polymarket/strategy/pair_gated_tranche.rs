use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const RESIDUAL_EPS: f64 = 10.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;
const TAIL_COMPLETION_ONLY_SECS: u64 = 60;
const TAIL_RISK_FREEZE_SECS: u64 = 30;

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
        let _risk_freeze_active = remaining_secs <= TAIL_RISK_FREEZE_SECS;

        if let Some(active) = input.pair_ledger.active_tranche.filter(|tranche| {
            tranche.first_side.is_some() && tranche.residual_qty > f64::EPSILON
        }) {
            if let Some(intent) = self.completion_intent(coordinator, input, active, remaining_secs)
            {
                quotes.set(intent);
            }
            return quotes;
        }

        if no_new_open || input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
            return quotes;
        }

        let pair_arb_quotes = StrategyKind::PairArb.compute_quotes(coordinator, input);
        let yes = pair_arb_quotes.buy_for(Side::Yes);
        let no = pair_arb_quotes.buy_for(Side::No);
        let tick = coordinator.cfg().tick_size.max(1e-9);

        let selected = match (yes, no) {
            (Some(yes), Some(no)) => {
                if (yes.price - no.price).abs() < tick - 1e-9 {
                    None
                } else if yes.price < no.price {
                    Some(yes)
                } else {
                    Some(no)
                }
            }
            (Some(intent), None) | (None, Some(intent)) => Some(intent),
            (None, None) => None,
        };

        if let Some(intent) = selected {
            quotes.set(StrategyIntent {
                side: intent.side,
                direction: TradeDirection::Buy,
                price: intent.price,
                size: intent.size,
                reason: BidReason::Provide,
            });
        }

        quotes
    }
}

impl PairGatedTrancheStrategy {
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
        let _urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true);
        let ceiling = 1.0 - active.first_vwap - MIN_EDGE_PER_PAIR + repair_budget_per_share;
        if ceiling <= 0.0 {
            return None;
        }

        let maker_cap = (best_ask - safety_margin).max(0.0);
        let price = coordinator.safe_price(maker_cap.min(ceiling));
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }

        let size = (active.residual_qty * 100.0).floor() / 100.0;
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
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}
