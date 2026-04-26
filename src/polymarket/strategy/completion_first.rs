use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche, TrancheState};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const MAX_SAME_SIDE_RUN: u32 = 1;
const BASE_CLIP: f64 = 150.0;
const MAX_CLIP: f64 = 250.0;
const MIN_CLIP: f64 = 45.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;

pub(crate) struct CompletionFirstStrategy;

pub(crate) static COMPLETION_FIRST_STRATEGY: CompletionFirstStrategy = CompletionFirstStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionFirstPhase {
    #[default]
    FlatSeed,
    CompletionOnly,
    HarvestWindow,
    PostResolve,
}

impl QuoteStrategy for CompletionFirstStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::CompletionFirst
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        if !coordinator.completion_first_market_enabled() {
            return StrategyQuotes::default();
        }

        let phase = coordinator.completion_first_phase();
        let pair_arb_quotes = StrategyKind::PairArb.compute_quotes(coordinator, input);
        let mut quotes = StrategyQuotes::default();

        if phase == CompletionFirstPhase::PostResolve {
            return quotes;
        }

        let ledger = &input.inventory.pair_ledger;
        if let Some(active) = ledger.active_tranche.filter(|t| t.residual_qty > 1e-9) {
            if let Some(intent) = self.completion_intent(coordinator, input, active) {
                quotes.set(intent);
            }
            if self.same_side_add_allowed(active, phase) {
                if let Some(first_side) = active.first_side {
                    if let Some(seed_intent) = pair_arb_quotes.buy_for(first_side) {
                        quotes.set(StrategyIntent {
                            side: seed_intent.side,
                            direction: TradeDirection::Buy,
                            price: seed_intent.price,
                            size: seed_intent.size,
                            reason: BidReason::Provide,
                        });
                    }
                }
            }
            return quotes;
        }

        if phase == CompletionFirstPhase::HarvestWindow {
            return quotes;
        }

        if let Some(intent) = pair_arb_quotes.buy_for(Side::Yes) {
            quotes.set(StrategyIntent {
                side: intent.side,
                direction: TradeDirection::Buy,
                price: intent.price,
                size: intent.size,
                reason: BidReason::Provide,
            });
        }
        if let Some(intent) = pair_arb_quotes.buy_for(Side::No) {
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

impl CompletionFirstStrategy {
    pub(crate) fn clip_for(
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
    ) -> f64 {
        let hour = chrono::Utc::now().hour();
        let session_mult = if (16..=22).contains(&hour) {
            1.15
        } else if (3..=12).contains(&hour) {
            0.80
        } else {
            1.00
        };
        let abs_imb = if coordinator.cfg().max_net_diff > 0.0 {
            (input.working_inv.net_diff.abs() / coordinator.cfg().max_net_diff).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let imbalance_mult = if abs_imb < 0.05 {
            1.00
        } else if abs_imb < 0.15 {
            0.95
        } else if abs_imb < 0.30 {
            1.00
        } else {
            1.20
        };
        let n = input.inventory.pair_ledger.buy_fill_count.max(1) as f64;
        let trade_index_mult = (1.0 - 0.05 * (n - 1.0)).max(0.70);
        let tail_mult = if remaining_secs <= 30 { 1.16 } else { 1.0 };
        let clip = BASE_CLIP * session_mult * imbalance_mult * trade_index_mult * tail_mult;
        ((clip.clamp(MIN_CLIP, MAX_CLIP) * 10.0).round()) / 10.0
    }

    fn same_side_add_allowed(&self, active: PairTranche, phase: CompletionFirstPhase) -> bool {
        matches!(
            phase,
            CompletionFirstPhase::CompletionOnly | CompletionFirstPhase::FlatSeed
        ) && active.same_side_add_count < MAX_SAME_SIDE_RUN
            && active.state == TrancheState::CompletionOnly
    }

    fn completion_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
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

        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let clip = Self::clip_for(coordinator, input, remaining_secs);
        let size = active.residual_qty.min(clip).max(0.0);
        let size = ((size * 10.0).round()) / 10.0;
        if size <= 0.0 {
            return None;
        }

        let safety_margin = coordinator.post_only_safety_margin_for(hedge_side, best_bid, best_ask);
        let repair_budget_per_share =
            input.inventory.pair_ledger.repair_budget_available / active.residual_qty.max(1.0);
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

        Some(StrategyIntent {
            side: hedge_side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        })
    }
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}

use chrono::Timelike;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_side_limit_blocks_second_add() {
        let tranche = PairTranche {
            state: TrancheState::CompletionOnly,
            same_side_add_count: 1,
            ..Default::default()
        };
        assert!(!COMPLETION_FIRST_STRATEGY
            .same_side_add_allowed(tranche, CompletionFirstPhase::CompletionOnly));
    }
}
