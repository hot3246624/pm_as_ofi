use crate::polymarket::coordinator::{Book, StrategyCoordinator};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PhaseBuilderStrategy;

pub(crate) static PHASE_BUILDER_STRATEGY: PhaseBuilderStrategy = PhaseBuilderStrategy;

impl QuoteStrategy for PhaseBuilderStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::PhaseBuilder
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        let inv = input.inv;
        let book = input.book;
        let metrics = input.metrics;
        let entry_cap = cfg.dip_buy_max_entry_price;
        if entry_cap <= 0.0 {
            return StrategyQuotes::default();
        }

        let reset_band = cfg.bid_size / 2.0;
        if inv.net_diff.abs() <= reset_band + 1e-9 {
            return self.flat_quotes(coordinator, book, entry_cap);
        }

        let build_budget = cfg.bid_size.max(0.5 * cfg.max_net_diff);
        if metrics.residual_qty + 1e-9 >= build_budget {
            return StrategyQuotes::default();
        }

        let Some(side) = metrics.dominant_side else {
            return StrategyQuotes::default();
        };
        self.build_quotes(coordinator, inv, book, side, entry_cap)
    }
}

impl PhaseBuilderStrategy {
    fn flat_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        book: &Book,
        entry_cap: f64,
    ) -> StrategyQuotes {
        let cheap_gap_min = 2.0 * coordinator.cfg().tick_size;
        let Some((cheap_side, cheap_bid, cheap_ask, other_ask)) = cheapest_side(book) else {
            return StrategyQuotes::default();
        };
        if cheap_ask > entry_cap + 1e-9 {
            return StrategyQuotes::default();
        }
        if other_ask - cheap_ask + 1e-9 < cheap_gap_min {
            return StrategyQuotes::default();
        }
        buy_intent(coordinator, cheap_side, cheap_bid, cheap_ask, entry_cap)
    }

    fn build_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        inv: &crate::polymarket::messages::InventoryState,
        book: &Book,
        side: Side,
        entry_cap: f64,
    ) -> StrategyQuotes {
        let (best_bid, best_ask, avg_cost) = match side {
            Side::Yes => (book.yes_bid, book.yes_ask, inv.yes_avg_cost),
            Side::No => (book.no_bid, book.no_ask, inv.no_avg_cost),
        };
        let build_cap = entry_cap.min(avg_cost.max(0.0));
        if best_ask <= 0.0 || build_cap <= 0.0 || best_ask > build_cap + 1e-9 {
            return StrategyQuotes::default();
        }
        buy_intent(coordinator, side, best_bid, best_ask, build_cap)
    }
}

fn cheapest_side(book: &Book) -> Option<(Side, f64, f64, f64)> {
    if !(book.yes_ask > 0.0 && book.no_ask > 0.0) {
        return None;
    }
    if book.yes_ask <= book.no_ask {
        Some((Side::Yes, book.yes_bid, book.yes_ask, book.no_ask))
    } else {
        Some((Side::No, book.no_bid, book.no_ask, book.yes_ask))
    }
}

fn buy_intent(
    coordinator: &StrategyCoordinator,
    side: Side,
    best_bid: f64,
    best_ask: f64,
    ceiling: f64,
) -> StrategyQuotes {
    let price = coordinator.aggressive_price_for(side, ceiling, best_bid, best_ask);
    if price <= 0.0 {
        return StrategyQuotes::default();
    }

    let mut quotes = StrategyQuotes::default();
    quotes.set(StrategyIntent {
        side,
        direction: TradeDirection::Buy,
        price,
        size: coordinator.cfg().bid_size,
        reason: BidReason::Provide,
    });
    quotes
}
