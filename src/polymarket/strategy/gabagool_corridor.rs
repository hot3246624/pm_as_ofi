use crate::polymarket::coordinator::{Book, ProjectedBuyMetrics, StrategyCoordinator};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const FLOAT_EPS: f64 = 1e-9;
const SOFT_BAND_CLIPS: f64 = 1.0;
const HARD_BAND_CLIPS: f64 = 2.0;
const SOFT_PAIR_COST_CAP: f64 = 1.00;
const HARD_PAIR_COST_CAP: f64 = 1.03;
const SOFT_OUTCOME_FLOOR_CLIPS: f64 = 0.0;
const HARD_OUTCOME_FLOOR_CLIPS: f64 = -1.0;
const REPAIR_TARGET_PAIR_COST: f64 = 0.99;
const REPAIR_MIN_PAIR_COST_IMPROVEMENT: f64 = 0.02;

pub(crate) struct GabagoolCorridorStrategy;

pub(crate) static GABAGOOL_CORRIDOR_STRATEGY: GabagoolCorridorStrategy = GabagoolCorridorStrategy;

#[derive(Debug, Clone, Copy)]
enum CorridorState {
    BalancedBuild,
    SideBiasedBuild,
    PairingRepair { side: Side },
    RiskCompression,
}

#[derive(Debug, Clone, Copy)]
struct BuyCandidate {
    intent: StrategyIntent,
    projected: ProjectedBuyMetrics,
}

impl QuoteStrategy for GabagoolCorridorStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::GabagoolCorridor
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        let clip_size = cfg.bid_size;
        if clip_size <= FLOAT_EPS {
            return StrategyQuotes::default();
        }

        let yes = self.candidate_for(coordinator, input, Side::Yes, clip_size);
        let no = self.candidate_for(coordinator, input, Side::No, clip_size);
        let state = self.classify_state(coordinator, input, clip_size, yes, no);

        match state {
            CorridorState::BalancedBuild => self.quote_balanced(yes, no),
            CorridorState::SideBiasedBuild => self.quote_side_biased(
                input.book,
                input.metrics.dominant_side,
                yes,
                no,
                cfg.tick_size,
            ),
            CorridorState::PairingRepair { side } => self.quote_pairing_repair(side, yes, no),
            CorridorState::RiskCompression => {
                self.quote_risk_compression(input.inv.net_diff, yes, no)
            }
        }
    }
}

impl GabagoolCorridorStrategy {
    fn candidate_for(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        side: Side,
        size: f64,
    ) -> Option<BuyCandidate> {
        let (best_bid, best_ask) = side_book(input.book, side);
        let opposite_ask = opposite_best_ask(input.book, side);
        if !(best_ask > 0.0 && opposite_ask > 0.0) {
            return None;
        }

        let ceiling = coordinator.cfg().open_pair_band - opposite_ask;
        if ceiling <= 0.0 {
            return None;
        }

        let price = coordinator.aggressive_price_for(side, ceiling, best_bid, best_ask);
        if price <= 0.0 {
            return None;
        }

        let intent = StrategyIntent {
            side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        };
        if !coordinator.can_place_strategy_intent(input.inv, Some(intent)) {
            return None;
        }

        let projected = coordinator.simulate_buy(input.inv, side, size, price)?;
        Some(BuyCandidate { intent, projected })
    }

    fn classify_state(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        clip_size: f64,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
    ) -> CorridorState {
        let clip_imbalance = input.inv.net_diff.abs() / clip_size;
        let pair_cost = input.metrics.pair_cost;
        let worst_case = input.metrics.worst_case_outcome_pnl;
        let soft_outcome_floor = SOFT_OUTCOME_FLOOR_CLIPS * clip_size;
        let hard_outcome_floor = HARD_OUTCOME_FLOOR_CLIPS * clip_size;

        if clip_imbalance + FLOAT_EPS >= HARD_BAND_CLIPS
            || pair_cost > HARD_PAIR_COST_CAP + FLOAT_EPS
            || worst_case < hard_outcome_floor - FLOAT_EPS
        {
            return CorridorState::RiskCompression;
        }

        if let Some(side) = self.detect_repair_side(coordinator, input, yes, no) {
            return CorridorState::PairingRepair { side };
        }

        if clip_imbalance + FLOAT_EPS >= SOFT_BAND_CLIPS
            || pair_cost > SOFT_PAIR_COST_CAP + FLOAT_EPS
            || worst_case < soft_outcome_floor - FLOAT_EPS
        {
            return CorridorState::SideBiasedBuild;
        }

        CorridorState::BalancedBuild
    }

    fn detect_repair_side(
        &self,
        _coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
    ) -> Option<Side> {
        let dominant = input.metrics.dominant_side?;
        let current_pair_cost = input.metrics.pair_cost;
        let current_worst = input.metrics.worst_case_outcome_pnl;
        let candidate = match dominant {
            Side::Yes => no,
            Side::No => yes,
        }?;

        let projected_pair_cost = candidate.projected.metrics.pair_cost;
        let projected_worst = candidate.projected.metrics.worst_case_outcome_pnl;

        if projected_worst + FLOAT_EPS < current_worst {
            return None;
        }
        let hits_target = projected_pair_cost <= REPAIR_TARGET_PAIR_COST + FLOAT_EPS;
        let improves_enough =
            projected_pair_cost <= current_pair_cost - REPAIR_MIN_PAIR_COST_IMPROVEMENT + FLOAT_EPS;
        if hits_target || improves_enough {
            Some(candidate.intent.side)
        } else {
            None
        }
    }

    fn quote_balanced(
        &self,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        if let Some(c) = yes {
            quotes.set(c.intent);
        }
        if let Some(c) = no {
            quotes.set(c.intent);
        }
        quotes
    }

    fn quote_side_biased(
        &self,
        book: &Book,
        dominant_side: Option<Side>,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
        tick_size: f64,
    ) -> StrategyQuotes {
        let preferred = cheap_side(book, tick_size).or(dominant_side);
        let mut quotes = StrategyQuotes::default();
        match preferred {
            Some(Side::Yes) => {
                if let Some(c) = yes {
                    quotes.set(c.intent);
                }
            }
            Some(Side::No) => {
                if let Some(c) = no {
                    quotes.set(c.intent);
                }
            }
            None => {}
        }
        quotes
    }

    fn quote_pairing_repair(
        &self,
        side: Side,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        match side {
            Side::Yes => {
                if let Some(c) = yes {
                    quotes.set(c.intent);
                }
            }
            Side::No => {
                if let Some(c) = no {
                    quotes.set(c.intent);
                }
            }
        }
        quotes
    }

    fn quote_risk_compression(
        &self,
        net_diff: f64,
        yes: Option<BuyCandidate>,
        no: Option<BuyCandidate>,
    ) -> StrategyQuotes {
        let current_abs = net_diff.abs();
        let mut quotes = StrategyQuotes::default();

        if let Some(c) = yes {
            if c.projected.projected_abs_net_diff <= current_abs + FLOAT_EPS {
                quotes.set(c.intent);
            }
        }
        if let Some(c) = no {
            if c.projected.projected_abs_net_diff <= current_abs + FLOAT_EPS {
                quotes.set(c.intent);
            }
        }
        quotes
    }
}

fn side_book(book: &Book, side: Side) -> (f64, f64) {
    match side {
        Side::Yes => (book.yes_bid, book.yes_ask),
        Side::No => (book.no_bid, book.no_ask),
    }
}

fn opposite_best_ask(book: &Book, side: Side) -> f64 {
    match side {
        Side::Yes => book.no_ask,
        Side::No => book.yes_ask,
    }
}

fn cheap_side(book: &Book, tick_size: f64) -> Option<Side> {
    if !(book.yes_ask > 0.0 && book.no_ask > 0.0) {
        return None;
    }
    let gap = (book.yes_ask - book.no_ask).abs();
    if gap + FLOAT_EPS < 2.0 * tick_size {
        return None;
    }
    if book.yes_ask < book.no_ask {
        Some(Side::Yes)
    } else {
        Some(Side::No)
    }
}
