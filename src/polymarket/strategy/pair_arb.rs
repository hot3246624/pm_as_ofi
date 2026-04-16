use crate::polymarket::coordinator::PairArbTierMode;
use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::coordinator::StrategyInventoryMetrics;
use crate::polymarket::coordinator::PAIR_ARB_NET_EPS;
use crate::polymarket::messages::{BidReason, SideOfi, TradeDirection};
use crate::polymarket::types::Side;
use tracing::{debug, trace};

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PairArbStrategy;

pub(crate) static PAIR_ARB_STRATEGY: PairArbStrategy = PairArbStrategy;
const TIER_1_NET_DIFF: f64 = 5.0;
const TIER_2_NET_DIFF: f64 = 10.0;
const RISK_INCR_TIER_1_NET_DIFF: f64 = 3.5;
const RISK_INCR_TIER_2_NET_DIFF: f64 = 8.0;
const EARLY_SKEW_MULT: f64 = 0.35;
const OFI_HOT_ADJUST_TICKS: f64 = 1.0;
const OFI_TOXIC_ADJUST_TICKS: f64 = 2.0;

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
        let effective_skew_factor = Self::effective_skew_factor(
            cfg.as_skew_factor,
            abs_net_diff,
            time_decay,
            cfg.pair_arb.tier_mode,
        );
        let skew_shift = skew * effective_skew_factor;

        let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
        let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

        if raw_yes + raw_no > cfg.pair_target {
            let overflow = (raw_yes + raw_no) - cfg.pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        let yes_size = Self::candidate_size_for_side(inv, Side::Yes, cfg.bid_size);
        let no_size = Self::candidate_size_for_side(inv, Side::No, cfg.bid_size);
        let yes_risk_effect = Self::candidate_risk_effect(inv, Side::Yes, yes_size);
        let no_risk_effect = Self::candidate_risk_effect(inv, Side::No, no_size);

        // 1b) Tiered avg-cost cap for same-side risk-increasing candidate.
        // Tier for risk-increasing leg enters earlier (3.5/8) than the global
        // state bucket transitions (5/10). This is a pure tightening rule.
        // When tier mode is disabled, this stage is bypassed.
        if let Some(yes_cap) = Self::tier_cap_price_for_candidate(
            inv,
            Side::Yes,
            yes_size,
            yes_risk_effect,
            cfg.pair_arb.tier_mode,
            cfg.pair_arb.tier_1_mult,
            cfg.pair_arb.tier_2_mult,
        ) {
            raw_yes = raw_yes.min(yes_cap);
        }
        if let Some(no_cap) = Self::tier_cap_price_for_candidate(
            inv,
            Side::No,
            no_size,
            no_risk_effect,
            cfg.pair_arb.tier_mode,
            cfg.pair_arb.tier_1_mult,
            cfg.pair_arb.tier_2_mult,
        ) {
            raw_no = raw_no.min(no_cap);
        }

        let mut quotes = StrategyQuotes::default();
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
        let effective_pair_cost_margin = if inv.net_diff.abs() < PAIR_ARB_NET_EPS {
            0.0
        } else {
            cfg.pair_arb.pair_cost_safety_margin
        };
        if inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
            let raw_yes_before = raw_yes;
            let yes_ceiling = Self::vwap_ceiling(
                cfg.pair_target,
                effective_pair_cost_margin,
                inv.no_avg_cost,
                inv.yes_qty,
                inv.yes_avg_cost,
                yes_size,
            );
            raw_yes = f64::min(raw_yes, yes_ceiling);
            if effective_pair_cost_margin > 1e-9 && raw_yes < raw_yes_before - 1e-9 {
                debug!(
                    "🧱 pair_cost_safety_margin_applied=true side=YES active=true margin={:.3} raw_before={:.4} raw_after={:.4} vwap_ceiling={:.4}",
                    effective_pair_cost_margin,
                    raw_yes_before,
                    raw_yes,
                    yes_ceiling,
                );
            }
            if yes_ceiling <= cfg.tick_size + 1e-9 {
                disable_yes_by_cost = true;
                debug!(
                    "🧱 Disable YES provide by inventory clamp: ceiling={:.4} tick={:.4} pair_cost_safety_margin={:.3}",
                    yes_ceiling, cfg.tick_size, effective_pair_cost_margin
                );
            }
        }
        if inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 {
            let raw_no_before = raw_no;
            let no_ceiling = Self::vwap_ceiling(
                cfg.pair_target,
                effective_pair_cost_margin,
                inv.yes_avg_cost,
                inv.no_qty,
                inv.no_avg_cost,
                no_size,
            );
            raw_no = f64::min(raw_no, no_ceiling);
            if effective_pair_cost_margin > 1e-9 && raw_no < raw_no_before - 1e-9 {
                debug!(
                    "🧱 pair_cost_safety_margin_applied=true side=NO active=true margin={:.3} raw_before={:.4} raw_after={:.4} vwap_ceiling={:.4}",
                    effective_pair_cost_margin,
                    raw_no_before,
                    raw_no,
                    no_ceiling,
                );
            }
            if no_ceiling <= cfg.tick_size + 1e-9 {
                disable_no_by_cost = true;
                debug!(
                    "🧱 Disable NO provide by inventory clamp: ceiling={:.4} tick={:.4} pair_cost_safety_margin={:.3}",
                    no_ceiling, cfg.tick_size, effective_pair_cost_margin
                );
            }
        }

        // 3) Role-aware maker clamp.
        //
        // Risk-increasing leg keeps strict maker clamp.
        // Pairing/reducing leg keeps strategic target (VWAP/pair-target driven)
        // and only applies post-only safety at action time in order IO.
        let yes_safety_margin =
            coordinator.post_only_safety_margin_for(Side::Yes, ub.yes_bid, ub.yes_ask);
        let no_safety_margin =
            coordinator.post_only_safety_margin_for(Side::No, ub.no_bid, ub.no_ask);

        if ub.yes_ask > 0.0 && yes_risk_effect == PairArbRiskEffect::RiskIncreasing {
            raw_yes = f64::min(raw_yes, ub.yes_ask - yes_safety_margin);
        }
        if ub.no_ask > 0.0 && no_risk_effect == PairArbRiskEffect::RiskIncreasing {
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
                    yes_size,
                )
            {
                quotes.set(StrategyIntent {
                    side: Side::Yes,
                    direction: TradeDirection::Buy,
                    price: bid_yes,
                    size: yes_size,
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
                    no_size,
                )
            {
                quotes.set(StrategyIntent {
                    side: Side::No,
                    direction: TradeDirection::Buy,
                    price: bid_no,
                    size: no_size,
                    reason: BidReason::Provide,
                });
            }
        }

        quotes
    }
}

impl PairArbStrategy {
    pub(crate) fn candidate_size_for_side(
        inv: &crate::polymarket::messages::InventoryState,
        side: Side,
        bid_size: f64,
    ) -> f64 {
        if bid_size <= PAIR_ARB_NET_EPS {
            return bid_size.max(0.0);
        }
        let dominant = if inv.net_diff > PAIR_ARB_NET_EPS {
            Some(Side::Yes)
        } else if inv.net_diff < -PAIR_ARB_NET_EPS {
            Some(Side::No)
        } else {
            None
        };
        let pairing_side = dominant.map(|d| match d {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        });
        if pairing_side == Some(side) {
            // Pairing leg uses current residual first; if it quantizes to an
            // unexecutable lot, fall back to base bid size.
            let d = inv.net_diff.abs().max(0.0);
            let sized = (d * 100.0).floor() / 100.0;
            if sized >= 0.01 {
                sized
            } else {
                bid_size
            }
        } else {
            bid_size
        }
    }

    pub(crate) fn vwap_ceiling(
        pair_target: f64,
        safety_margin: f64,
        opp_avg: f64,
        same_qty: f64,
        same_avg: f64,
        bid_size: f64,
    ) -> f64 {
        let guarded_target = (pair_target - safety_margin).max(0.0);
        let legacy = guarded_target - opp_avg;
        if same_qty <= PAIR_ARB_NET_EPS || bid_size <= PAIR_ARB_NET_EPS {
            return legacy;
        }

        let numerator = (guarded_target - opp_avg) * (same_qty + bid_size) - same_qty * same_avg;
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
        let projected = Self::projected_abs_net_diff(inv.net_diff, side, size);
        if projected <= inv.net_diff.abs() + PAIR_ARB_NET_EPS {
            PairArbRiskEffect::PairingOrReducing
        } else {
            PairArbRiskEffect::RiskIncreasing
        }
    }

    pub(crate) fn projected_abs_net_diff(net_diff: f64, side: Side, size: f64) -> f64 {
        match side {
            Side::Yes => (net_diff + size).abs(),
            Side::No => (net_diff - size).abs(),
        }
    }

    pub(crate) fn tier_cap_price_for_candidate(
        inv: &crate::polymarket::messages::InventoryState,
        side: Side,
        size: f64,
        risk_effect: PairArbRiskEffect,
        tier_mode: PairArbTierMode,
        tier_1_mult: f64,
        tier_2_mult: f64,
    ) -> Option<f64> {
        if risk_effect != PairArbRiskEffect::RiskIncreasing {
            return None;
        }
        if !tier_mode.tier_cap_enabled() {
            return None;
        }
        let _ = size;
        let current_abs = inv.net_diff.abs();
        let mult = match tier_mode {
            PairArbTierMode::Discrete => {
                if current_abs + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_2_NET_DIFF {
                    tier_2_mult
                } else if current_abs + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_1_NET_DIFF {
                    tier_1_mult
                } else {
                    return None;
                }
            }
            PairArbTierMode::Continuous => {
                if current_abs <= PAIR_ARB_NET_EPS {
                    return None;
                }
                if current_abs <= RISK_INCR_TIER_1_NET_DIFF {
                    let ratio = (current_abs / RISK_INCR_TIER_1_NET_DIFF).clamp(0.0, 1.0);
                    1.0 + (tier_1_mult - 1.0) * ratio
                } else if current_abs < RISK_INCR_TIER_2_NET_DIFF {
                    let ratio = ((current_abs - RISK_INCR_TIER_1_NET_DIFF)
                        / (RISK_INCR_TIER_2_NET_DIFF - RISK_INCR_TIER_1_NET_DIFF))
                        .clamp(0.0, 1.0);
                    tier_1_mult + (tier_2_mult - tier_1_mult) * ratio
                } else {
                    tier_2_mult
                }
            }
            PairArbTierMode::Disabled => unreachable!("disabled mode is returned above"),
        };
        match side {
            Side::Yes if inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 => {
                Some(inv.yes_avg_cost * mult)
            }
            Side::No if inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 => {
                Some(inv.no_avg_cost * mult)
            }
            _ => None,
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

    pub(crate) fn effective_skew_factor(
        base: f64,
        abs_net_diff: f64,
        time_decay: f64,
        tier_mode: PairArbTierMode,
    ) -> f64 {
        // Disabled mode emulates pre-tier behavior:
        // no 5/10 segmented inventory curve; only linear inventory skew remains.
        if !tier_mode.tiered_inventory_curve_enabled() {
            return base * time_decay;
        }
        if abs_net_diff < TIER_1_NET_DIFF {
            return base * EARLY_SKEW_MULT;
        }
        if abs_net_diff < TIER_2_NET_DIFF {
            let ramp = (abs_net_diff - TIER_1_NET_DIFF) / (TIER_2_NET_DIFF - TIER_1_NET_DIFF);
            return base * (EARLY_SKEW_MULT + (1.0 - EARLY_SKEW_MULT) * ramp) * time_decay;
        }
        base * time_decay
    }

    #[allow(clippy::too_many_arguments)]
    fn should_keep_candidate(
        &self,
        coordinator: &StrategyCoordinator,
        quotes: &mut StrategyQuotes,
        inv: &crate::polymarket::messages::InventoryState,
        _book: &crate::polymarket::coordinator::Book,
        _current_metrics: &StrategyInventoryMetrics,
        _current_utility: f64,
        _current_open_edge: f64,
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

        let risk_effect = Self::candidate_risk_effect(inv, side, size);
        if matches!(risk_effect, PairArbRiskEffect::RiskIncreasing)
            && coordinator.pair_arb_risk_open_cutoff_active()
        {
            debug!(
                "🧭 pair_arb_risk_open_cutoff_blocked=true side={} candidate_role={} price={:.4} size={:.2} net_diff={:.2}",
                side.as_str(),
                risk_effect.as_str(),
                price,
                size,
                inv.net_diff,
            );
            return false;
        }
        let Some(_projected) = coordinator.simulate_buy(inv, side, size, price) else {
            quotes.note_pair_arb_skip_simulate_buy_none();
            trace!(
                "🧭 pair_arb skip | side={} reason=simulate_buy_none price={:.4} size={:.2}",
                side.as_str(),
                price,
                size,
            );
            return false;
        };

        quotes.note_pair_arb_keep_candidate();
        true
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

        let early =
            PairArbStrategy::effective_skew_factor(base, 3.0, td, PairArbTierMode::Discrete);
        assert!((early - (base * EARLY_SKEW_MULT)).abs() < 1e-9);

        let mid = PairArbStrategy::effective_skew_factor(base, 7.5, td, PairArbTierMode::Discrete);
        let ramp = (7.5 - TIER_1_NET_DIFF) / (TIER_2_NET_DIFF - TIER_1_NET_DIFF);
        let expected_mid = base * (EARLY_SKEW_MULT + (1.0 - EARLY_SKEW_MULT) * ramp) * td;
        assert!((mid - expected_mid).abs() < 1e-9);

        let late =
            PairArbStrategy::effective_skew_factor(base, 12.0, td, PairArbTierMode::Discrete);
        assert!((late - (base * td)).abs() < 1e-9);
    }

    #[test]
    fn test_effective_skew_factor_disabled_uses_linear_curve() {
        let base = 0.06;
        let td = 1.6;
        let expected = base * td;
        let early =
            PairArbStrategy::effective_skew_factor(base, 2.0, td, PairArbTierMode::Disabled);
        let mid = PairArbStrategy::effective_skew_factor(base, 7.0, td, PairArbTierMode::Disabled);
        let late =
            PairArbStrategy::effective_skew_factor(base, 12.0, td, PairArbTierMode::Disabled);
        assert!((early - expected).abs() < 1e-9);
        assert!((mid - expected).abs() < 1e-9);
        assert!((late - expected).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_equals_legacy_when_same_qty_is_zero() {
        let legacy = 0.98 - 0.40;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.0, 0.40, 0.0, 0.35, 5.0);
        assert!((ceiling - legacy).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_relaxes_when_holding_cheap_same_side_inventory() {
        // From PLAN_Claude example: yes_qty=5, yes_avg=0.40, no_avg=0.40
        // legacy ceiling=0.58, vwap ceiling=0.76
        let legacy = 0.98 - 0.40;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.0, 0.40, 5.0, 0.40, 5.0);
        assert!(ceiling > legacy);
        assert!((ceiling - 0.76).abs() < 1e-9);
    }

    #[test]
    fn test_vwap_ceiling_relaxes_opposite_side_pairing_capacity() {
        // YES-heavy buy NO example: yes_avg=0.30, no_qty=5, no_avg=0.50
        // legacy no ceiling=0.68, vwap no ceiling=0.86
        let legacy = 0.98 - 0.30;
        let ceiling = PairArbStrategy::vwap_ceiling(0.98, 0.0, 0.30, 5.0, 0.50, 5.0);
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
        let ceiling =
            PairArbStrategy::vwap_ceiling(pair_target, 0.0, opp_avg, same_qty, same_avg, bid_size);
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
        let tier_capped_yes = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::Yes,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Discrete,
            0.80,
            0.60,
        )
        .expect("tier cap");
        let vwap_yes_ceiling = PairArbStrategy::vwap_ceiling(
            0.98,
            0.0,
            inv.no_avg_cost,
            inv.yes_qty,
            inv.yes_avg_cost,
            5.0,
        );
        assert!(tier_capped_yes < vwap_yes_ceiling);
    }

    #[test]
    fn test_vwap_ceiling_respects_pair_cost_safety_margin() {
        let no_margin = PairArbStrategy::vwap_ceiling(0.98, 0.0, 0.40, 0.0, 0.35, 5.0);
        let with_margin = PairArbStrategy::vwap_ceiling(0.98, 0.02, 0.40, 0.0, 0.35, 5.0);
        assert!((no_margin - with_margin - 0.02).abs() < 1e-9);
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
    fn test_tier_cap_does_not_trigger_before_current_net_crosses_3_5() {
        let inv = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.40,
            net_diff: -3.49,
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Discrete,
            0.70,
            0.30,
        );
        assert!(cap.is_none());
    }

    #[test]
    fn test_tier_cap_uses_current_bucket_so_7_99_still_tier1() {
        let inv = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.40,
            net_diff: -7.99,
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Discrete,
            0.70,
            0.30,
        );
        assert!(cap.is_some());
        assert!((cap.unwrap_or_default() - 0.28).abs() < 1e-9);
    }

    #[test]
    fn test_projected_tier_cap_ignored_for_pairing_or_reducing() {
        let inv = InventoryState {
            yes_qty: 10.0,
            yes_avg_cost: 0.52,
            net_diff: 7.99,
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::PairingOrReducing,
            PairArbTierMode::Discrete,
            0.70,
            0.30,
        );
        assert!(cap.is_none());
    }

    #[test]
    fn test_tier_cap_handles_non_integer_current_net_diff_edges() {
        let cases = [
            (-3.4, None),
            (-3.5, Some(0.28)),
            (-8.0, Some(0.12)),
            (-8.5, Some(0.12)),
        ];
        for (net_diff, expected_cap) in cases {
            let inv = InventoryState {
                no_qty: 10.0,
                no_avg_cost: 0.40,
                net_diff,
                ..Default::default()
            };
            let cap = PairArbStrategy::tier_cap_price_for_candidate(
                &inv,
                Side::No,
                5.0,
                PairArbRiskEffect::RiskIncreasing,
                PairArbTierMode::Discrete,
                0.70,
                0.30,
            );
            match expected_cap {
                Some(expected) => {
                    assert!(cap.is_some());
                    assert!((cap.unwrap_or_default() - expected).abs() < 1e-9);
                }
                None => assert!(cap.is_none()),
            }
        }
    }

    #[test]
    fn test_tier_cap_continuous_mode_scales_smoothly_from_flat() {
        let inv = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.40,
            net_diff: -1.75, // 50% of first segment (0 -> 3.5)
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Continuous,
            0.70,
            0.30,
        );
        // multiplier = 1 + (0.70 - 1.0) * 0.5 = 0.85
        assert!(cap.is_some());
        assert!((cap.unwrap_or_default() - 0.34).abs() < 1e-9);
    }

    #[test]
    fn test_tier_cap_disabled_mode_never_applies_cap() {
        let inv = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.40,
            net_diff: -12.0,
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Disabled,
            0.70,
            0.30,
        );
        assert!(cap.is_none());
    }

    #[test]
    fn test_tier_cap_continuous_mode_interpolates_between_tiers() {
        let inv = InventoryState {
            no_qty: 10.0,
            no_avg_cost: 0.40,
            net_diff: -5.75, // midpoint between 3.5 and 8.0
            ..Default::default()
        };
        let cap = PairArbStrategy::tier_cap_price_for_candidate(
            &inv,
            Side::No,
            5.0,
            PairArbRiskEffect::RiskIncreasing,
            PairArbTierMode::Continuous,
            0.70,
            0.30,
        );
        // midpoint multiplier = 0.50
        assert!(cap.is_some());
        assert!((cap.unwrap_or_default() - 0.20).abs() < 1e-9);
    }

    #[test]
    fn test_candidate_size_uses_abs_net_for_pairing_side() {
        let inv = InventoryState {
            net_diff: 10.0,
            ..Default::default()
        };
        let yes_size = PairArbStrategy::candidate_size_for_side(&inv, Side::Yes, 5.0);
        let no_size = PairArbStrategy::candidate_size_for_side(&inv, Side::No, 5.0);
        assert!((yes_size - 5.0).abs() < 1e-9);
        assert!((no_size - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_candidate_size_supports_fractional_pairing_and_lot_floor_fallback() {
        let inv = InventoryState {
            net_diff: -2.17,
            ..Default::default()
        };
        let yes_size = PairArbStrategy::candidate_size_for_side(&inv, Side::Yes, 5.0);
        let no_size = PairArbStrategy::candidate_size_for_side(&inv, Side::No, 5.0);
        assert!((yes_size - 2.17).abs() < 1e-9);
        assert!((no_size - 5.0).abs() < 1e-9);

        let tiny = InventoryState {
            net_diff: 0.004,
            ..Default::default()
        };
        let tiny_pairing = PairArbStrategy::candidate_size_for_side(&tiny, Side::No, 5.0);
        assert!((tiny_pairing - 5.0).abs() < 1e-9);
    }
}
