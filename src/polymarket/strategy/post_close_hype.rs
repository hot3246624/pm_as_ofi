use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{
    ORACLE_LAG_MICRO_TICK_BID_BOUNDARY, ORACLE_LAG_NO_TAKER_ABOVE_PRICE, StrategyCoordinator,
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
    ///   asks[0] < 0.993                 → skip (FAK path owns this case)
    ///   asks[0] ≥ 0.993                 → maker @ asks[0] - effective_tick
    ///   no asks, bids[0] ≥ 0.94         → maker @ bids[0] + 0.001
    ///   no asks, bids[0] < 0.94         → maker @ bids[0] + 0.01
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

        let Some((side, _open_ref, _final_price)) = coordinator.post_close_chainlink_winner()
        else {
            return StrategyQuotes::default();
        };

        let (best_bid, best_ask) = match side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };

        let fallback_tick = cfg.tick_size.max(1e-9);
        let tick = effective_tick(best_bid, best_ask, fallback_tick);

        let maker_price = if best_ask > 0.0 {
            if best_ask < ORACLE_LAG_NO_TAKER_ABOVE_PRICE {
                // FAK branch owns this case; maker stands down.
                return StrategyQuotes::default();
            }
            (best_ask - tick).max(0.0)
        } else if best_bid > 0.0 {
            let step = if best_bid >= ORACLE_LAG_MICRO_TICK_BID_BOUNDARY {
                0.001
            } else {
                0.01
            };
            let ceiling = (1.0 - tick).max(tick);
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
            reason: BidReason::Provide,
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
