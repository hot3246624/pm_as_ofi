use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::coordinator::StrategyInventoryMetrics;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;
use tracing::debug;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PairArbStrategy;

pub(crate) static PAIR_ARB_STRATEGY: PairArbStrategy = PairArbStrategy;
const FLOAT_EPS: f64 = 1e-9;
const TIER_1_NET_DIFF: f64 = 5.0;
const TIER_2_NET_DIFF: f64 = 10.0;
const TIER_1_MULT: f64 = 0.85;
const TIER_2_MULT: f64 = 0.70;
const EARLY_SKEW_MULT: f64 = 0.35;

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
        let current_metrics = input.metrics;
        let current_utility = coordinator.utility_for_inventory(inv, current_metrics, ub);
        let current_open_edge = coordinator.open_edge_for_inventory(inv, current_metrics, ub);

        let mid_yes = (ub.yes_bid + ub.yes_ask) / 2.0;
        let mid_no = (ub.no_bid + ub.no_ask) / 2.0;

        // 1) Base pricing via A-S + Gabagool
        let excess = f64::max(0.0, (mid_yes + mid_no) - cfg.pair_target);
        let skew = if cfg.max_net_diff > 0.0 {
            (inv.net_diff / cfg.max_net_diff).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let abs_net_diff = inv.net_diff.abs();
        let time_decay = coordinator.compute_time_decay_factor();
        let effective_skew_factor =
            Self::effective_skew_factor(cfg.as_skew_factor, abs_net_diff, time_decay);
        let skew_shift = skew * effective_skew_factor;

        let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
        let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

        if raw_yes + raw_no > cfg.pair_target {
            let overflow = (raw_yes + raw_no) - cfg.pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        // 1b) Tiered avg-cost cap for dominant inventory side.
        // This is an extra cap before pair-cost ceiling:
        // - Tier 1: net diff >= 5  -> dominant side bid <= avg * 0.85
        // - Tier 2: net diff >= 10 -> dominant side bid <= avg * 0.70
        (raw_yes, raw_no) = Self::apply_tier_avg_cost_cap(inv, raw_yes, raw_no);

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
        if bid_yes > 0.0
            && self.should_keep_candidate(
                coordinator,
                inv,
                ub,
                current_metrics,
                current_utility,
                current_open_edge,
                Side::Yes,
                bid_yes,
                cfg.bid_size,
            )
        {
            quotes.set(StrategyIntent {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                price: bid_yes,
                size: cfg.bid_size,
                reason: BidReason::Provide,
            });
        }
        if bid_no > 0.0
            && self.should_keep_candidate(
                coordinator,
                inv,
                ub,
                current_metrics,
                current_utility,
                current_open_edge,
                Side::No,
                bid_no,
                cfg.bid_size,
            )
        {
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

impl PairArbStrategy {
    pub(crate) fn effective_skew_factor(base: f64, abs_net_diff: f64, time_decay: f64) -> f64 {
        if abs_net_diff < TIER_1_NET_DIFF {
            return base * EARLY_SKEW_MULT;
        }
        if abs_net_diff < TIER_2_NET_DIFF {
            let ramp = (abs_net_diff - TIER_1_NET_DIFF) / (TIER_2_NET_DIFF - TIER_1_NET_DIFF);
            return base * (EARLY_SKEW_MULT + (1.0 - EARLY_SKEW_MULT) * ramp) * time_decay;
        }
        base * time_decay
    }

    fn apply_tier_avg_cost_cap(
        inv: &crate::polymarket::messages::InventoryState,
        mut raw_yes: f64,
        mut raw_no: f64,
    ) -> (f64, f64) {
        if inv.net_diff >= TIER_1_NET_DIFF && inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 {
            let mult = if inv.net_diff >= TIER_2_NET_DIFF {
                TIER_2_MULT
            } else {
                TIER_1_MULT
            };
            raw_yes = raw_yes.min(inv.yes_avg_cost * mult);
        }
        if inv.net_diff <= -TIER_1_NET_DIFF && inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
            let mult = if inv.net_diff <= -TIER_2_NET_DIFF {
                TIER_2_MULT
            } else {
                TIER_1_MULT
            };
            raw_no = raw_no.min(inv.no_avg_cost * mult);
        }
        (raw_yes, raw_no)
    }

    #[allow(clippy::too_many_arguments)]
    fn should_keep_candidate(
        &self,
        coordinator: &StrategyCoordinator,
        inv: &crate::polymarket::messages::InventoryState,
        book: &crate::polymarket::coordinator::Book,
        current_metrics: &StrategyInventoryMetrics,
        current_utility: f64,
        current_open_edge: f64,
        side: Side,
        price: f64,
        size: f64,
    ) -> bool {
        let intent = StrategyIntent {
            side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        };
        if !coordinator.can_place_strategy_intent(inv, Some(intent)) {
            return false;
        }

        let Some(projected) = coordinator.simulate_buy(inv, side, size, price) else {
            return false;
        };

        let improves_locked_pnl =
            projected.metrics.paired_locked_pnl > current_metrics.paired_locked_pnl + FLOAT_EPS;
        let improves_pair_cost = current_metrics.paired_qty > FLOAT_EPS
            && projected.metrics.paired_qty > FLOAT_EPS
            && projected.metrics.pair_cost + FLOAT_EPS < current_metrics.pair_cost;
        let reaches_target_pair = projected.metrics.paired_qty > FLOAT_EPS
            && projected.metrics.pair_cost <= coordinator.cfg().pair_target + FLOAT_EPS;
        if improves_locked_pnl || improves_pair_cost || reaches_target_pair {
            return true;
        }

        let projected_utility = coordinator.utility_for_inventory(
            &projected.projected_inventory,
            &projected.metrics,
            book,
        );
        let utility_delta = projected_utility - current_utility;
        let min_utility_delta = size * coordinator.cfg().tick_size.max(1e-9);
        if utility_delta + FLOAT_EPS < min_utility_delta {
            return false;
        }

        let risk_increasing = projected.projected_abs_net_diff > inv.net_diff.abs() + FLOAT_EPS;
        if !risk_increasing {
            return true;
        }

        let projected_open_edge = coordinator.open_edge_for_inventory(
            &projected.projected_inventory,
            &projected.metrics,
            book,
        );
        projected_open_edge > current_open_edge + FLOAT_EPS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::messages::InventoryState;

    #[test]
    fn test_effective_skew_factor_uses_tiered_curve() {
        let base = 0.06;
        let td = 1.6;

        let early = PairArbStrategy::effective_skew_factor(base, 3.0, td);
        assert!((early - (base * EARLY_SKEW_MULT)).abs() < 1e-9);

        let mid = PairArbStrategy::effective_skew_factor(base, 7.5, td);
        let expected_mid = base * (EARLY_SKEW_MULT + 0.65 * 0.5) * td;
        assert!((mid - expected_mid).abs() < 1e-9);

        let late = PairArbStrategy::effective_skew_factor(base, 12.0, td);
        assert!((late - (base * td)).abs() < 1e-9);
    }

    #[test]
    fn test_tier_avg_cost_cap_applies_on_dominant_side() {
        let inv_yes = InventoryState {
            yes_qty: 15.0,
            yes_avg_cost: 0.45,
            no_qty: 0.0,
            no_avg_cost: 0.0,
            net_diff: 10.0,
            ..Default::default()
        };
        let (yes_capped, no_unchanged) =
            PairArbStrategy::apply_tier_avg_cost_cap(&inv_yes, 0.60, 0.40);
        assert!((yes_capped - (0.45 * TIER_2_MULT)).abs() < 1e-9);
        assert!((no_unchanged - 0.40).abs() < 1e-9);

        let inv_no = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.50,
            yes_qty: 0.0,
            yes_avg_cost: 0.0,
            net_diff: -6.0,
            ..Default::default()
        };
        let (yes_unchanged, no_capped) =
            PairArbStrategy::apply_tier_avg_cost_cap(&inv_no, 0.30, 0.60);
        assert!((yes_unchanged - 0.30).abs() < 1e-9);
        assert!((no_capped - (0.50 * TIER_1_MULT)).abs() < 1e-9);
    }
}
