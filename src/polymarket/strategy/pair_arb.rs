use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;
use tracing::debug;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PairArbStrategy;

pub(crate) static PAIR_ARB_STRATEGY: PairArbStrategy = PairArbStrategy;

impl QuoteStrategy for PairArbStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::PairArb
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        let inv = input.inv;
        let ub = input.book;

        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no = (ub.no_bid + ub.no_ask) / 2.0;

        // 1) Base pricing via A-S + Gabagool
        let excess = f64::max(0.0, (mid_yes + mid_no) - cfg.pair_target);
        let skew = if cfg.max_net_diff > 0.0 {
            (inv.net_diff / cfg.max_net_diff).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let effective_skew_factor = cfg.as_skew_factor * coordinator.compute_time_decay_factor();
        let skew_shift = skew * effective_skew_factor;

        let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
        let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

        if raw_yes + raw_no > cfg.pair_target {
            let overflow = (raw_yes + raw_no) - cfg.pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        // 2) Inventory Cost Clamp
        let mut disable_yes_by_cost = false;
        let mut disable_no_by_cost = false;
        if inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
            let yes_ceiling = cfg.pair_target - inv.no_avg_cost;
            raw_yes = f64::min(raw_yes, yes_ceiling);
            if yes_ceiling <= cfg.tick_size + 1e-9 {
                disable_yes_by_cost = true;
                debug!(
                    "🧱 Disable YES provide by inventory clamp: ceiling={:.4} tick={:.4}",
                    yes_ceiling, cfg.tick_size
                );
            }
        }
        if inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 {
            let no_ceiling = cfg.pair_target - inv.yes_avg_cost;
            raw_no = f64::min(raw_no, no_ceiling);
            if no_ceiling <= cfg.tick_size + 1e-9 {
                disable_no_by_cost = true;
                debug!(
                    "🧱 Disable NO provide by inventory clamp: ceiling={:.4} tick={:.4}",
                    no_ceiling, cfg.tick_size
                );
            }
        }

        // 3) Strict Maker Clamp (same safety-margin logic as aggressive_price)
        let yes_safety_margin =
            coordinator.post_only_safety_margin_for(Side::Yes, ub.yes_bid, ub.yes_ask);
        let no_safety_margin =
            coordinator.post_only_safety_margin_for(Side::No, ub.no_bid, ub.no_ask);

        if ub.yes_ask > 0.0 {
            raw_yes = f64::min(raw_yes, ub.yes_ask - yes_safety_margin);
        }
        if ub.no_ask > 0.0 {
            raw_no = f64::min(raw_no, ub.no_ask - no_safety_margin);
        }

        let bid_yes = if disable_yes_by_cost {
            0.0
        } else {
            coordinator.safe_price(raw_yes)
        };
        let bid_no = if disable_no_by_cost {
            0.0
        } else {
            coordinator.safe_price(raw_no)
        };

        let mut quotes = StrategyQuotes::default();
        if bid_yes > 0.0 {
            quotes.set(StrategyIntent {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                price: bid_yes,
                size: cfg.bid_size,
                reason: BidReason::Provide,
            });
        }
        if bid_no > 0.0 {
            quotes.set(StrategyIntent {
                side: Side::No,
                direction: TradeDirection::Buy,
                price: bid_no,
                size: cfg.bid_size,
                reason: BidReason::Provide,
            });
        }

        quotes
    }
}
