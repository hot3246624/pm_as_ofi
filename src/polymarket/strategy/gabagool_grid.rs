use crate::polymarket::coordinator::{Book, StrategyCoordinator};
use crate::polymarket::messages::{BidReason, InventoryState, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const FLOAT_EPS: f64 = 1e-9;

// Parked unified-utility implementation that was previously exposed as `gabagool`.
pub(crate) struct GabagoolGridStrategy;

pub(crate) static GABAGOOL_GRID_STRATEGY: GabagoolGridStrategy = GabagoolGridStrategy;

#[derive(Debug, Clone, Copy)]
struct BuyCandidate {
    side: Side,
    price: f64,
    size: f64,
}

impl QuoteStrategy for GabagoolGridStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::GabagoolGrid
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        for side in [Side::Yes, Side::No] {
            if let Some(candidate) = self.candidate_for(coordinator, input, side) {
                quotes.set(quote_from_candidate(candidate));
            }
        }
        quotes
    }
}

impl GabagoolGridStrategy {
    fn candidate_for(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        side: Side,
    ) -> Option<BuyCandidate> {
        let cfg = coordinator.cfg();
        let size = cfg.bid_size;
        if size <= FLOAT_EPS {
            return None;
        }

        let (best_bid, best_ask) = side_book(input.book, side);
        let opposite_best_ask = opposite_best_ask(input.book, side);
        if !(best_ask > 0.0 && opposite_best_ask > 0.0) {
            return None;
        }

        let (held_qty, avg_cost) = side_inventory(input.inv, side);
        let mut entry_ceiling = cfg.open_pair_band - opposite_best_ask;
        if held_qty > FLOAT_EPS && avg_cost > 0.0 {
            entry_ceiling = entry_ceiling.min(avg_cost - cfg.tick_size);
        }
        if entry_ceiling <= 0.0 {
            return None;
        }

        let price = coordinator.aggressive_price_for(side, entry_ceiling, best_bid, best_ask);
        if !(price > 0.0) {
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
        if held_qty > FLOAT_EPS && avg_cost > 0.0 {
            let projected_avg_cost = side_avg_cost(&projected.projected_inventory, side);
            if avg_cost - projected_avg_cost + FLOAT_EPS < cfg.tick_size {
                return None;
            }
        }

        let current_utility =
            coordinator.utility_for_inventory(input.inv, input.metrics, input.book);
        let projected_utility = coordinator.utility_for_inventory(
            &projected.projected_inventory,
            &projected.metrics,
            input.book,
        );
        let utility_delta = projected_utility - current_utility;
        let min_delta = cfg.bid_size * cfg.tick_size.max(1e-9);
        if utility_delta + FLOAT_EPS < min_delta {
            return None;
        }

        Some(BuyCandidate { side, price, size })
    }
}

fn quote_from_candidate(candidate: BuyCandidate) -> StrategyIntent {
    StrategyIntent {
        side: candidate.side,
        direction: TradeDirection::Buy,
        price: candidate.price,
        size: candidate.size,
        reason: BidReason::Provide,
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

fn side_inventory(inv: &InventoryState, side: Side) -> (f64, f64) {
    match side {
        Side::Yes => (inv.yes_qty.max(0.0), inv.yes_avg_cost.max(0.0)),
        Side::No => (inv.no_qty.max(0.0), inv.no_avg_cost.max(0.0)),
    }
}

fn side_avg_cost(inv: &InventoryState, side: Side) -> f64 {
    match side {
        Side::Yes => inv.yes_avg_cost.max(0.0),
        Side::No => inv.no_avg_cost.max(0.0),
    }
}
