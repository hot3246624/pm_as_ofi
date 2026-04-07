use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::coordinator::StrategyInventoryMetrics;
use crate::polymarket::messages::{BidReason, SideOfi, TradeDirection};
use crate::polymarket::types::Side;
use tracing::{debug, trace};

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PairArbStrategy;

pub(crate) static PAIR_ARB_STRATEGY: PairArbStrategy = PairArbStrategy;
const FLOAT_EPS: f64 = 1e-9;
const TIER_1_NET_DIFF: f64 = 5.0;
const TIER_2_NET_DIFF: f64 = 10.0;
const EARLY_SKEW_MULT: f64 = 0.35;
const OFI_HOT_ADJUST_TICKS: f64 = 1.0;
const OFI_TOXIC_ADJUST_TICKS: f64 = 2.0;
const RISK_INCR_BOOTSTRAP_MIN_UTILITY_MULT: f64 = -0.5;
const RISK_INCR_LOW_NET_MIN_UTILITY_MULT: f64 = 0.5;
const HIGH_IMBALANCE_UTILITY_MULT: f64 = 2.0;
const HIGH_IMBALANCE_OPEN_EDGE_IMPROVE_MULT: f64 = 0.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairArbRiskEffect {
    PairingOrReducing,
    RiskIncreasing,
}

impl PairArbRiskEffect {
    fn as_str(self) -> &'static str {
        match self {
            Self::PairingOrReducing => "pairing",
            Self::RiskIncreasing => "risk_increasing",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PairArbOfiDecision {
    pub(crate) adjust_ticks: f64,
    pub(crate) suppress: bool,
}

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
        // This is an extra cap before pair-cost ceiling, driven by the
        // current inventory state rather than path-dependent fill history.
        (raw_yes, raw_no) = Self::apply_tier_avg_cost_cap(
            inv,
            raw_yes,
            raw_no,
            cfg.pair_arb_tier_1_mult,
            cfg.pair_arb_tier_2_mult,
        );

        let mut quotes = StrategyQuotes::default();
        let yes_risk_effect = Self::candidate_risk_effect(inv, Side::Yes, cfg.bid_size);
        let yes_ofi = Self::ofi_decision(input.ofi.map(|ofi| ofi.yes), yes_risk_effect);
        if yes_ofi.suppress {
            quotes.note_pair_arb_ofi_suppressed();
            debug!(
                "🪵 pair_arb OFI suppress | side=YES risk_effect={} ofi_heat={:.2} ofi_toxic={} ofi_saturated={} ofi_adjust_ticks={:.1} ofi_suppressed=true",
                yes_risk_effect.as_str(),
                input.ofi.map(|ofi| ofi.yes.heat_score).unwrap_or_default(),
                input.ofi.map(|ofi| ofi.yes.is_toxic).unwrap_or(false),
                input.ofi.map(|ofi| ofi.yes.saturated).unwrap_or(false),
                yes_ofi.adjust_ticks,
            );
        } else if yes_ofi.adjust_ticks > 0.0 {
            raw_yes -= yes_ofi.adjust_ticks * cfg.tick_size;
            quotes.note_pair_arb_ofi_softened();
            debug!(
                "🪵 pair_arb OFI soften | side=YES risk_effect={} ofi_heat={:.2} ofi_toxic={} ofi_saturated={} ofi_adjust_ticks={:.1} ofi_suppressed=false",
                yes_risk_effect.as_str(),
                input.ofi.map(|ofi| ofi.yes.heat_score).unwrap_or_default(),
                input.ofi.map(|ofi| ofi.yes.is_toxic).unwrap_or(false),
                input.ofi.map(|ofi| ofi.yes.saturated).unwrap_or(false),
                yes_ofi.adjust_ticks,
            );
        }

        let no_risk_effect = Self::candidate_risk_effect(inv, Side::No, cfg.bid_size);
        let no_ofi = Self::ofi_decision(input.ofi.map(|ofi| ofi.no), no_risk_effect);
        if no_ofi.suppress {
            quotes.note_pair_arb_ofi_suppressed();
            debug!(
                "🪵 pair_arb OFI suppress | side=NO risk_effect={} ofi_heat={:.2} ofi_toxic={} ofi_saturated={} ofi_adjust_ticks={:.1} ofi_suppressed=true",
                no_risk_effect.as_str(),
                input.ofi.map(|ofi| ofi.no.heat_score).unwrap_or_default(),
                input.ofi.map(|ofi| ofi.no.is_toxic).unwrap_or(false),
                input.ofi.map(|ofi| ofi.no.saturated).unwrap_or(false),
                no_ofi.adjust_ticks,
            );
        } else if no_ofi.adjust_ticks > 0.0 {
            raw_no -= no_ofi.adjust_ticks * cfg.tick_size;
            quotes.note_pair_arb_ofi_softened();
            debug!(
                "🪵 pair_arb OFI soften | side=NO risk_effect={} ofi_heat={:.2} ofi_toxic={} ofi_saturated={} ofi_adjust_ticks={:.1} ofi_suppressed=false",
                no_risk_effect.as_str(),
                input.ofi.map(|ofi| ofi.no.heat_score).unwrap_or_default(),
                input.ofi.map(|ofi| ofi.no.is_toxic).unwrap_or(false),
                input.ofi.map(|ofi| ofi.no.saturated).unwrap_or(false),
                no_ofi.adjust_ticks,
            );
        }

        // 2) Inventory Cost Clamp (VWAP ceiling)
        let mut disable_yes_by_cost = false;
        let mut disable_no_by_cost = false;
        if inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
            let yes_ceiling = Self::vwap_ceiling(
                cfg.pair_target,
                inv.no_avg_cost,
                inv.yes_qty,
                inv.yes_avg_cost,
                cfg.bid_size,
            );
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
            let no_ceiling = Self::vwap_ceiling(
                cfg.pair_target,
                inv.yes_avg_cost,
                inv.no_qty,
                inv.no_avg_cost,
                cfg.bid_size,
            );
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

        if !yes_ofi.suppress {
            let bid_yes = if disable_yes_by_cost {
                0.0
            } else {
                coordinator.safe_price(raw_yes)
            };
            if bid_yes > 0.0
                && self.should_keep_candidate(
                    coordinator,
                    &mut quotes,
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
        }

        if !no_ofi.suppress {
            let bid_no = if disable_no_by_cost {
                0.0
            } else {
                coordinator.safe_price(raw_no)
            };
            if bid_no > 0.0
                && self.should_keep_candidate(
                    coordinator,
                    &mut quotes,
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
        }

        quotes
    }
}

impl PairArbStrategy {
    pub(crate) fn vwap_ceiling(
        pair_target: f64,
        opp_avg: f64,
        same_qty: f64,
        same_avg: f64,
        bid_size: f64,
    ) -> f64 {
        let legacy = pair_target - opp_avg;
        if same_qty <= FLOAT_EPS || bid_size <= FLOAT_EPS {
            return legacy;
        }

        let numerator = (pair_target - opp_avg) * (same_qty + bid_size) - same_qty * same_avg;
        let ceiling = numerator / bid_size;
        if ceiling.is_finite() {
            ceiling
        } else {
            legacy
        }
    }

    pub(crate) fn candidate_risk_effect(
        inv: &crate::polymarket::messages::InventoryState,
        side: Side,
        size: f64,
    ) -> PairArbRiskEffect {
        let projected = match side {
            Side::Yes => (inv.net_diff + size).abs(),
            Side::No => (inv.net_diff - size).abs(),
        };
        if projected <= inv.net_diff.abs() + FLOAT_EPS {
            PairArbRiskEffect::PairingOrReducing
        } else {
            PairArbRiskEffect::RiskIncreasing
        }
    }

    pub(crate) fn ofi_decision(
        side_ofi: Option<SideOfi>,
        risk_effect: PairArbRiskEffect,
    ) -> PairArbOfiDecision {
        if risk_effect != PairArbRiskEffect::RiskIncreasing {
            return PairArbOfiDecision {
                adjust_ticks: 0.0,
                suppress: false,
            };
        }
        let Some(side_ofi) = side_ofi else {
            return PairArbOfiDecision {
                adjust_ticks: 0.0,
                suppress: false,
            };
        };
        if side_ofi.is_toxic && side_ofi.saturated {
            return PairArbOfiDecision {
                adjust_ticks: OFI_TOXIC_ADJUST_TICKS,
                suppress: true,
            };
        }
        if side_ofi.is_toxic {
            return PairArbOfiDecision {
                adjust_ticks: OFI_TOXIC_ADJUST_TICKS,
                suppress: false,
            };
        }
        if side_ofi.is_hot {
            return PairArbOfiDecision {
                adjust_ticks: OFI_HOT_ADJUST_TICKS,
                suppress: false,
            };
        }
        PairArbOfiDecision {
            adjust_ticks: 0.0,
            suppress: false,
        }
    }

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

    pub(crate) fn apply_tier_avg_cost_cap(
        inv: &crate::polymarket::messages::InventoryState,
        mut raw_yes: f64,
        mut raw_no: f64,
        tier_1_mult: f64,
        tier_2_mult: f64,
    ) -> (f64, f64) {
        if inv.net_diff >= TIER_1_NET_DIFF && inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 {
            let mult = if inv.net_diff >= TIER_2_NET_DIFF {
                tier_2_mult
            } else {
                tier_1_mult
            };
            raw_yes = raw_yes.min(inv.yes_avg_cost * mult);
        }
        if inv.net_diff <= -TIER_1_NET_DIFF && inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
            let mult = if inv.net_diff <= -TIER_2_NET_DIFF {
                tier_2_mult
            } else {
                tier_1_mult
            };
            raw_no = raw_no.min(inv.no_avg_cost * mult);
        }
        (raw_yes, raw_no)
    }

    pub(crate) fn min_utility_delta_for_risk_increasing(
        current_paired_qty: f64,
        abs_net_diff: f64,
        size: f64,
        tick_size: f64,
    ) -> f64 {
        let base = size * tick_size.max(1e-9);
        if current_paired_qty <= FLOAT_EPS && abs_net_diff <= FLOAT_EPS {
            return RISK_INCR_BOOTSTRAP_MIN_UTILITY_MULT * base;
        }
        if abs_net_diff + FLOAT_EPS < size {
            return RISK_INCR_LOW_NET_MIN_UTILITY_MULT * base;
        }
        if abs_net_diff + FLOAT_EPS >= TIER_2_NET_DIFF {
            return HIGH_IMBALANCE_UTILITY_MULT * base;
        }
        base
    }

    pub(crate) fn min_open_edge_improvement_for_risk_increasing(
        abs_net_diff: f64,
        size: f64,
        tick_size: f64,
    ) -> f64 {
        let base = size * tick_size.max(1e-9);
        if abs_net_diff + FLOAT_EPS >= TIER_2_NET_DIFF {
            HIGH_IMBALANCE_OPEN_EDGE_IMPROVE_MULT * base
        } else {
            FLOAT_EPS
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn should_keep_candidate(
        &self,
        coordinator: &StrategyCoordinator,
        quotes: &mut StrategyQuotes,
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
            quotes.note_pair_arb_skip_inventory_gate();
            trace!(
                "🧭 pair_arb skip | side={} reason=inventory_gate price={:.4} size={:.2} net_diff={:.2}",
                side.as_str(),
                price,
                size,
                inv.net_diff,
            );
            return false;
        }

        let Some(projected) = coordinator.simulate_buy(inv, side, size, price) else {
            quotes.note_pair_arb_skip_simulate_buy_none();
            trace!(
                "🧭 pair_arb skip | side={} reason=simulate_buy_none price={:.4} size={:.2}",
                side.as_str(),
                price,
                size,
            );
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
            quotes.note_pair_arb_keep_candidate();
            return true;
        }

        let risk_increasing = projected.projected_abs_net_diff > inv.net_diff.abs() + FLOAT_EPS;
        if !risk_increasing {
            quotes.note_pair_arb_keep_candidate();
            return true;
        }

        let projected_utility = coordinator.utility_for_inventory(
            &projected.projected_inventory,
            &projected.metrics,
            book,
        );
        let utility_delta = projected_utility - current_utility;
        let min_utility_delta = Self::min_utility_delta_for_risk_increasing(
            current_metrics.paired_qty,
            inv.net_diff.abs(),
            size,
            coordinator.cfg().tick_size,
        );
        if utility_delta + FLOAT_EPS < min_utility_delta {
            quotes.note_pair_arb_skip_utility_delta();
            trace!(
                "🧭 pair_arb skip | side={} candidate_role={} reason=utility_delta utility_delta={:.6} min_delta={:.6} high_imbalance_admission={} pair_cost={:.4} paired_qty={:.2}",
                side.as_str(),
                if risk_increasing { "risk_increasing" } else { "pairing" },
                utility_delta,
                min_utility_delta,
                if inv.net_diff.abs() + FLOAT_EPS >= TIER_2_NET_DIFF {
                    "blocked"
                } else {
                    "pass"
                },
                projected.metrics.pair_cost,
                projected.metrics.paired_qty,
            );
            return false;
        }

        let projected_open_edge = coordinator.open_edge_for_inventory(
            &projected.projected_inventory,
            &projected.metrics,
            book,
        );
        let min_open_edge_improvement = Self::min_open_edge_improvement_for_risk_increasing(
            inv.net_diff.abs(),
            size,
            coordinator.cfg().tick_size,
        );
        let open_edge_delta = projected_open_edge - current_open_edge;
        let keep = open_edge_delta + FLOAT_EPS >= min_open_edge_improvement;
        if !keep {
            quotes.note_pair_arb_skip_open_edge_not_improved();
            trace!(
                "🧭 pair_arb skip | side={} candidate_role={} reason=open_edge_not_improved current_open_edge={:.6} projected_open_edge={:.6} open_edge_delta={:.6} min_delta={:.6} high_imbalance_admission={} projected_abs_net_diff={:.2}",
                side.as_str(),
                if risk_increasing { "risk_increasing" } else { "pairing" },
                current_open_edge,
                projected_open_edge,
                open_edge_delta,
                min_open_edge_improvement,
                if inv.net_diff.abs() + FLOAT_EPS >= TIER_2_NET_DIFF {
                    "blocked"
                } else {
                    "pass"
                },
                projected.projected_abs_net_diff,
            );
        } else {
            quotes.note_pair_arb_keep_candidate();
        }
        keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::messages::{InventoryState, SideOfi};

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
        let tier_1_mult = 0.80;
        let tier_2_mult = 0.60;
        let inv_yes = InventoryState {
            yes_qty: 15.0,
            yes_avg_cost: 0.45,
            no_qty: 0.0,
            no_avg_cost: 0.0,
            net_diff: 10.0,
            ..Default::default()
        };
        let (yes_capped, no_unchanged) =
            PairArbStrategy::apply_tier_avg_cost_cap(&inv_yes, 0.60, 0.40, tier_1_mult, tier_2_mult);
        assert!((yes_capped - (0.45 * tier_2_mult)).abs() < 1e-9);
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
            PairArbStrategy::apply_tier_avg_cost_cap(&inv_no, 0.30, 0.60, tier_1_mult, tier_2_mult);
        assert!((yes_unchanged - 0.30).abs() < 1e-9);
        assert!((no_capped - (0.50 * tier_1_mult)).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_equals_legacy_when_same_qty_is_zero() {
        let legacy = 0.98 - 0.40;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.40, 0.0, 0.35, 5.0);
        assert!((ceiling - legacy).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_relaxes_when_holding_cheap_same_side_inventory() {
        // From PLAN_Claude example: yes_qty=5, yes_avg=0.40, no_avg=0.40
        // legacy ceiling=0.58, vwap ceiling=0.76
        let legacy = 0.98 - 0.40;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.40, 5.0, 0.40, 5.0);
        assert!(ceiling > legacy);
        assert!((ceiling - 0.76).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_relaxes_opposite_side_pairing_capacity() {
        // YES-heavy buy NO example: yes_avg=0.30, no_qty=5, no_avg=0.50
        // legacy no ceiling=0.68, vwap no ceiling=0.86
        let legacy = 0.98 - 0.30;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.30, 5.0, 0.50, 5.0);
        assert!(ceiling > legacy);
        assert!((ceiling - 0.86).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_never_exceeds_pair_target_after_projected_fill() {
        let pair_target = 0.98;
        let opp_avg = 0.40;
        let same_qty = 5.0;
        let same_avg = 0.40;
        let bid_size = 5.0;
        let ceiling = PairArbStrategy::vwap_ceiling(pair_target, opp_avg, same_qty, same_avg, bid_size);
        let projected_same_avg = (same_qty * same_avg + bid_size * ceiling) / (same_qty + bid_size);
        assert!(projected_same_avg + opp_avg <= pair_target + 1e-9);

        let safer_bid = ceiling - 0.10;
        let safer_avg = (same_qty * same_avg + bid_size * safer_bid) / (same_qty + bid_size);
        assert!(safer_avg + opp_avg <= pair_target + 1e-9);
    }

    #[test]
    fn test_tier_cap_still_dominates_when_vwap_ceiling_is_looser() {
        let inv = InventoryState {
            yes_qty: 5.0,
            yes_avg_cost: 0.80,
            no_qty: 5.0,
            no_avg_cost: 0.10,
            net_diff: 10.0,
            ..Default::default()
        };
        let (tier_capped_yes, _) =
            PairArbStrategy::apply_tier_avg_cost_cap(&inv, 0.95, 0.20, 0.80, 0.60);
        let vwap_yes_ceiling = PairArbStrategy::vwap_ceiling(0.98, inv.no_avg_cost, inv.yes_qty, inv.yes_avg_cost, 5.0);
        assert!(tier_capped_yes < vwap_yes_ceiling);
    }

    #[test]
    fn test_ofi_hot_only_softens_risk_increasing_buy() {
        let decision = PairArbStrategy::ofi_decision(
            Some(SideOfi {
                is_hot: true,
                heat_score: 2.0,
                ..Default::default()
            }),
            PairArbRiskEffect::RiskIncreasing,
        );
        assert!((decision.adjust_ticks - OFI_HOT_ADJUST_TICKS).abs() < 1e-9);
        assert!(!decision.suppress);
    }

    #[test]
    fn test_ofi_toxic_softens_or_suppresses_risk_increasing_buy() {
        let softened = PairArbStrategy::ofi_decision(
            Some(SideOfi {
                is_toxic: true,
                toxic_buy: true,
                ..Default::default()
            }),
            PairArbRiskEffect::RiskIncreasing,
        );
        assert!((softened.adjust_ticks - OFI_TOXIC_ADJUST_TICKS).abs() < 1e-9);
        assert!(!softened.suppress);

        let suppressed = PairArbStrategy::ofi_decision(
            Some(SideOfi {
                is_toxic: true,
                toxic_buy: true,
                saturated: true,
                ..Default::default()
            }),
            PairArbRiskEffect::RiskIncreasing,
        );
        assert!((suppressed.adjust_ticks - OFI_TOXIC_ADJUST_TICKS).abs() < 1e-9);
        assert!(suppressed.suppress);
    }

    #[test]
    fn test_ofi_ignored_for_pairing_or_reducing_buy() {
        let decision = PairArbStrategy::ofi_decision(
            Some(SideOfi {
                is_hot: true,
                is_toxic: true,
                saturated: true,
                toxic_buy: true,
                ..Default::default()
            }),
            PairArbRiskEffect::PairingOrReducing,
        );
        assert_eq!(decision.adjust_ticks, 0.0);
        assert!(!decision.suppress);
    }

    #[test]
    fn test_min_utility_delta_for_risk_increasing_bootstrap_and_low_net() {
        let size = 5.0;
        let tick = 0.01;
        let base = size * tick;

        let bootstrap =
            PairArbStrategy::min_utility_delta_for_risk_increasing(0.0, 0.0, size, tick);
        assert!((bootstrap - (RISK_INCR_BOOTSTRAP_MIN_UTILITY_MULT * base)).abs() < 1e-9);

        let low_net = PairArbStrategy::min_utility_delta_for_risk_increasing(5.0, 4.0, size, tick);
        assert!((low_net - (RISK_INCR_LOW_NET_MIN_UTILITY_MULT * base)).abs() < 1e-9);
    }

    #[test]
    fn test_min_utility_delta_for_risk_increasing_full_threshold_after_low_net() {
        let size = 5.0;
        let tick = 0.01;
        let base = size * tick;

        let full = PairArbStrategy::min_utility_delta_for_risk_increasing(5.0, 5.0, size, tick);
        assert!((full - base).abs() < 1e-9);

        let high_imbalance =
            PairArbStrategy::min_utility_delta_for_risk_increasing(5.0, 10.0, size, tick);
        assert!((high_imbalance - (HIGH_IMBALANCE_UTILITY_MULT * base)).abs() < 1e-9);
    }

    #[test]
    fn test_min_open_edge_improvement_tightens_in_high_imbalance() {
        let size = 5.0;
        let tick = 0.01;
        let base = size * tick;

        let normal = PairArbStrategy::min_open_edge_improvement_for_risk_increasing(5.0, size, tick);
        assert!(normal <= FLOAT_EPS * 2.0);

        let high = PairArbStrategy::min_open_edge_improvement_for_risk_increasing(10.0, size, tick);
        assert!((high - (HIGH_IMBALANCE_OPEN_EDGE_IMPROVE_MULT * base)).abs() < 1e-9);
    }
}
