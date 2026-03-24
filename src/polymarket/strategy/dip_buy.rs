use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct DipBuyStrategy;

pub(crate) static DIP_BUY_STRATEGY: DipBuyStrategy = DipBuyStrategy;

impl QuoteStrategy for DipBuyStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::DipBuy
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let ub = input.book;
        let cap = coordinator.cfg().dip_buy_max_entry_price;
        if cap <= 0.0 {
            return StrategyQuotes::default();
        }

        let yes_ok = ub.yes_ask > 0.0 && ub.yes_ask <= cap + 1e-9;
        let no_ok = ub.no_ask > 0.0 && ub.no_ask <= cap + 1e-9;
        let side = match (yes_ok, no_ok) {
            (true, false) => Some(Side::Yes),
            (false, true) => Some(Side::No),
            (true, true) => {
                if ub.yes_ask <= ub.no_ask {
                    Some(Side::Yes)
                } else {
                    Some(Side::No)
                }
            }
            _ => None,
        };

        let mut quotes = StrategyQuotes::default();
        match side {
            Some(Side::Yes) => {
                let price =
                    coordinator.aggressive_price_for(Side::Yes, cap, ub.yes_bid, ub.yes_ask);
                if price > 0.0 {
                    quotes.set(StrategyIntent {
                        side: Side::Yes,
                        direction: TradeDirection::Buy,
                        price,
                        size: coordinator.cfg().bid_size,
                        reason: BidReason::Provide,
                    });
                }
            }
            Some(Side::No) => {
                let price = coordinator.aggressive_price_for(Side::No, cap, ub.no_bid, ub.no_ask);
                if price > 0.0 {
                    quotes.set(StrategyIntent {
                        side: Side::No,
                        direction: TradeDirection::Buy,
                        price,
                        size: coordinator.cfg().bid_size,
                        reason: BidReason::Provide,
                    });
                }
            }
            None => {}
        }
        quotes
    }
}
