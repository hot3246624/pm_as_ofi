use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{
    StrategyCoordinator, ORACLE_LAG_MAKER_MAX_PRICE, ORACLE_LAG_NO_TAKER_ABOVE_PRICE,
};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PostCloseHypeStrategy;

pub(crate) static POST_CLOSE_HYPE_STRATEGY: PostCloseHypeStrategy = PostCloseHypeStrategy;

impl QuoteStrategy for PostCloseHypeStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::OracleLagSniping
    }

    /// Winner-side decision tree (maker branch only; FAK is dispatched by coordinator_order_io):
    ///   empty book (no asks, no bids)   → skip (no StrategyQuotes)
    ///   asks[0] <= 0.99                 → skip (FAK path owns this case)
    ///   asks[0] > 0.99                  → maker @ asks[0] - effective_tick
    ///   no asks                         → maker @ min(bids[0] + (1 tick / 0.1 tick), 0.991)
    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        if !cfg.oracle_lag_sniping.market_enabled || cfg.oracle_lag_sniping.window_secs == 0 {
            return StrategyQuotes::default();
        }
        if !is_in_post_close_window(cfg.market_end_ts, cfg.oracle_lag_sniping.window_secs) {
            return StrategyQuotes::default();
        }
        if !coordinator.oracle_lag_is_selected() {
            return StrategyQuotes::default();
        }

        let Some((side, _open_ref, _final_price)) = coordinator.post_close_chainlink_winner()
        else {
            return StrategyQuotes::default();
        };

        let (best_bid, best_ask) = match side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };

        let fallback_tick = cfg.tick_size.max(1e-9);
        // Use raw (non-fallback) ask for FAK/maker branch decision.
        // usable_book() fills yes_ask from last_valid_book when current ask=0, which
        // can make compute_quotes see a stale ask (e.g. 0.99) and wrongly defer to FAK
        // even when there are actually no sellers post-close.
        let raw_ask = coordinator.raw_book_ask(side);
        let effective_ask = if raw_ask > 0.0 { best_ask } else { 0.0 };
        let tick = effective_tick(best_bid, effective_ask, fallback_tick);

        let maker_price = if effective_ask > 0.0 {
            if effective_ask <= ORACLE_LAG_NO_TAKER_ABOVE_PRICE {
                // FAK branch owns this case; maker stands down.
                return StrategyQuotes::default();
            }
            (effective_ask - tick).max(0.0)
        } else if best_bid > 0.0 {
            // For 0.001-tick regime: +1 tick. For 0.01-tick regime: +0.1 tick.
            let step = if tick <= 0.001 + 1e-12 {
                tick
            } else {
                tick * 0.1
            };
            let ceiling = ORACLE_LAG_MAKER_MAX_PRICE.min(1.0 - tick);
            (best_bid + step).min(ceiling)
        } else {
            // empty book — no reference, skip.
            return StrategyQuotes::default();
        };

        if maker_price <= 0.0 {
            return StrategyQuotes::default();
        }

        let mut quotes = StrategyQuotes::default();
        quotes.set(StrategyIntent {
            side,
            direction: TradeDirection::Buy,
            price: maker_price,
            size: cfg.bid_size,
            reason: BidReason::OracleLagProvide,
        });
        quotes
    }
}

fn is_in_post_close_window(market_end_ts: Option<u64>, window_secs: u64) -> bool {
    let Some(end_ts) = market_end_ts else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs();
    now >= end_ts && now < end_ts.saturating_add(window_secs)
}

fn effective_tick(best_bid: f64, best_ask: f64, fallback: f64) -> f64 {
    // Polymarket switches to 0.001 when price > 0.96 or < 0.04.
    if best_bid > 0.96 || best_ask > 0.96 || (best_bid > 0.0 && best_bid < 0.04) {
        0.001
    } else {
        fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_tick_uses_micro_tick_near_extremes() {
        assert!((effective_tick(0.97, 0.98, 0.01) - 0.001).abs() < 1e-12);
        assert!((effective_tick(0.50, 0.51, 0.01) - 0.01).abs() < 1e-12);
    }
}
