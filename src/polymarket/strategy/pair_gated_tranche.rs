use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{StrategyCoordinator, PAIR_ARB_NET_EPS};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche};
use crate::polymarket::types::Side;

use super::pair_arb::PairArbStrategy;
use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const RESIDUAL_EPS: f64 = 10.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;
const TAIL_COMPLETION_ONLY_SECS: u64 = 25;
const PRICE_AWARE_NO_NEW_OPEN_SECS: u64 = 180;
const HARD_NO_NEW_OPEN_SECS: u64 = 150;
const LATE_OPEN_MAX_SEED_PRICE: f64 = 0.49;
const LATE_OPEN_MIN_VISIBLE_COMPLETION_SLACK_TICKS: f64 = -1.0;
const HARVEST_WINDOW_SECS: u64 = 25;
const HARVEST_MIN_PAIRABLE_QTY: f64 = 10.0;
const BASE_CLIP_QTY: f64 = 120.0;
const MAX_CLIP_QTY: f64 = 250.0;
const MIN_CLIP_QTY: f64 = 25.0;
const SEED_NO_IMMEDIATE_COMPLETION_CLIP_MULT: f64 = 0.60;
const SEED_THIN_SLACK_CLIP_MULT_TICK_0: f64 = 0.45;
const SEED_THIN_SLACK_CLIP_MULT_TICK_1: f64 = 0.70;
const SEED_THIN_SLACK_CLIP_MULT_TICK_2: f64 = 0.85;
pub(crate) const PGT_OPEN_PAIR_BAND_WIDE_SECS: u64 = 150;
pub(crate) const PGT_OPEN_PAIR_BAND_MID_SECS: u64 = 90;
pub(crate) const PGT_OPEN_PAIR_BAND_MID_VALUE: f64 = 0.995;
const EXPENSIVE_SEED_PRICE: f64 = 0.50;
const EXPENSIVE_SEED_MIN_VISIBLE_SLACK_TICKS: f64 = 1.0;
const SEED_MIN_VISIBLE_BREAKEVEN_COMPLETION_SLACK_TICKS: f64 = 0.0;
pub(crate) const PGT_MAX_SAME_SIDE_ADD_COUNT: u32 = 0;
const SAME_SIDE_ADD_FRACTION: f64 = 0.105;
const SAME_SIDE_ADD_MAX_QTY: f64 = 25.0;
const SAME_SIDE_ADD_MIN_FIRST_QTY: f64 = 45.0;
const SAME_SIDE_ADD_MIN_RESIDUAL_QTY: f64 = 45.0;
const SAME_SIDE_ADD_MAX_COMPLETION_AGE_SECS: f64 = 45.0;
const PROFIT_FIRST_BREAKEVEN_UNLOCK_AGE_SECS: f64 = 60.0;
const PROFIT_FIRST_BREAKEVEN_UNLOCK_REMAINING_SECS: u64 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgtShadowProfile {
    Legacy,
    ReplayFocusedV1,
    ReplayLowerClipV1,
}

#[derive(Debug, Clone, Copy)]
struct PgtTuning {
    profile: PgtShadowProfile,
    seed_open_max_remaining_secs: Option<u64>,
    open_pair_band_cap: Option<f64>,
    completion_early_pair_cap: f64,
    completion_late_pair_cap: f64,
    fixed_clip_qty: Option<f64>,
    base_clip_qty: f64,
    min_clip_qty: f64,
    max_clip_qty: f64,
}

impl PgtTuning {
    fn legacy() -> Self {
        Self {
            profile: PgtShadowProfile::Legacy,
            seed_open_max_remaining_secs: None,
            open_pair_band_cap: None,
            completion_early_pair_cap: 1.0 - MIN_EDGE_PER_PAIR,
            completion_late_pair_cap: 1.0,
            fixed_clip_qty: None,
            base_clip_qty: BASE_CLIP_QTY,
            min_clip_qty: MIN_CLIP_QTY,
            max_clip_qty: MAX_CLIP_QTY,
        }
    }

    fn replay_focused_v1() -> Self {
        Self {
            profile: PgtShadowProfile::ReplayFocusedV1,
            // 5m round: entry_start=75s means only open when remaining <=225s.
            seed_open_max_remaining_secs: Some(225),
            open_pair_band_cap: Some(0.980),
            completion_early_pair_cap: 0.975,
            completion_late_pair_cap: 0.995,
            fixed_clip_qty: Some(57.6),
            base_clip_qty: 57.6,
            min_clip_qty: 57.6,
            max_clip_qty: 57.6,
        }
    }

    fn replay_lower_clip_v1() -> Self {
        Self {
            profile: PgtShadowProfile::ReplayLowerClipV1,
            // 5m round: entry_start=60s means only open when remaining <=240s.
            seed_open_max_remaining_secs: Some(240),
            open_pair_band_cap: Some(0.970),
            completion_early_pair_cap: 0.975,
            completion_late_pair_cap: 1.000,
            fixed_clip_qty: Some(30.0),
            base_clip_qty: 30.0,
            min_clip_qty: 30.0,
            max_clip_qty: 30.0,
        }
    }

    fn from_env() -> Self {
        let raw = std::env::var("PM_PGT_SHADOW_PROFILE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match raw.as_str() {
            "" | "legacy" | "default" => Self::legacy(),
            "replay_focused_v1" | "focused" | "focused_v1" => Self::replay_focused_v1(),
            "replay_lower_clip_v1" | "lower_clip" | "lower_clip_v1" => Self::replay_lower_clip_v1(),
            _ => {
                eprintln!(
                    "⚠️ unknown PM_PGT_SHADOW_PROFILE={} ; falling back to legacy PGT tuning",
                    raw
                );
                Self::legacy()
            }
        }
    }

    fn open_pair_band(self, base: f64) -> f64 {
        if let Some(cap) = self.open_pair_band_cap {
            base.min(cap)
        } else {
            base
        }
    }
}

fn pgt_tuning() -> PgtTuning {
    static TUNING: OnceLock<PgtTuning> = OnceLock::new();
    *TUNING.get_or_init(PgtTuning::from_env)
}

struct CompletionPlan {
    intent: StrategyIntent,
    taker_shadow_would_close: bool,
    taker_close_limit: Option<f64>,
}

struct SeedPlan {
    intent: StrategyIntent,
    size: f64,
    taker_shadow_would_open: bool,
    entry_pressure_extra_ticks: u8,
    visible_completion_slack_ticks: f64,
    fill_distance_ticks: f64,
    preference_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlatSeedSelection {
    None,
    Dual,
    YesOnly,
    NoOnly,
}

pub(crate) struct PairGatedTrancheStrategy;

pub(crate) static PAIR_GATED_TRANCHE_STRATEGY: PairGatedTrancheStrategy = PairGatedTrancheStrategy;

impl QuoteStrategy for PairGatedTrancheStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::PairGatedTrancheArb
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let hard_no_new_open = remaining_secs <= HARD_NO_NEW_OPEN_SECS;
        let harvest_window_active = self.should_shadow_harvest(input, remaining_secs);

        if let Some(active) = input
            .pair_ledger
            .active_tranche
            .filter(|tranche| tranche.first_side.is_some() && tranche.residual_qty > f64::EPSILON)
        {
            if harvest_window_active {
                quotes.note_pgt_skip_harvest();
                return quotes;
            }
            let same_side_add =
                self.same_side_add_intent(coordinator, input, active, remaining_secs);
            if let Some(plan) = self.completion_intent(coordinator, input, active, remaining_secs) {
                quotes.note_pgt_completion_quote();
                if plan.taker_shadow_would_close {
                    quotes.note_pgt_taker_shadow_would_close();
                }
                if let Some(limit_price) = plan.taker_close_limit {
                    quotes.set_pgt_taker_close_limit(plan.intent.side, limit_price);
                }
                quotes.set(plan.intent);
            } else {
                quotes.note_pgt_skip_invalid_book();
            }
            if let Some(intent) = same_side_add {
                quotes.set(intent);
            }
            return quotes;
        }

        if harvest_window_active {
            quotes.note_pgt_skip_harvest();
            return quotes;
        }
        if hard_no_new_open {
            quotes.note_pgt_skip_tail_completion_only();
            return quotes;
        }
        if input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
            quotes.note_pgt_skip_residual_guard();
            return quotes;
        }
        if input
            .pair_ledger
            .capital_state
            .would_block_new_open_due_to_capital
        {
            quotes.note_pgt_skip_capital_guard();
            return quotes;
        }
        if !pgt_profile_seed_open_remaining_allowed(remaining_secs) {
            quotes.note_pgt_skip_geometry_guard();
            return quotes;
        }

        let Some((raw_yes, raw_no)) = self.flat_seed_raw_prices(coordinator, input, remaining_secs)
        else {
            quotes.note_pgt_skip_invalid_book();
            return quotes;
        };

        let size = self.adaptive_clip_qty(coordinator, input, None, remaining_secs);
        let size = quantize_tenth(size);
        if size <= 0.0 {
            quotes.note_pgt_skip_no_seed();
            return quotes;
        }

        let yes_seed = self
            .flat_seed_intent_for_side(coordinator, input, Side::Yes, raw_yes, size, &mut quotes)
            .filter(|seed| pgt_seed_open_window_allowed(seed, remaining_secs));
        let no_seed = self
            .flat_seed_intent_for_side(coordinator, input, Side::No, raw_no, size, &mut quotes)
            .filter(|seed| pgt_seed_open_window_allowed(seed, remaining_secs));
        if yes_seed.is_none() && no_seed.is_none() && remaining_secs <= PRICE_AWARE_NO_NEW_OPEN_SECS
        {
            quotes.note_pgt_skip_tail_completion_only();
            return quotes;
        }
        let latched_side = if coordinator.cfg().dry_run {
            coordinator.pgt_flat_seed_latched_side()
        } else {
            None
        };
        let latch_exhausted = if coordinator.cfg().dry_run {
            coordinator.pgt_flat_seed_latch_exhausted()
        } else {
            false
        };

        match self.select_flat_seed_plans(
            yes_seed.as_ref(),
            no_seed.as_ref(),
            coordinator.cfg().dry_run,
            latched_side,
            latch_exhausted,
        ) {
            FlatSeedSelection::None => {
                quotes.note_pgt_skip_geometry_guard();
            }
            FlatSeedSelection::Dual => {
                if let Some(seed) = yes_seed {
                    quotes.note_pgt_seed_quote();
                    if seed.entry_pressure_extra_ticks > 0 {
                        quotes.note_pgt_entry_pressure(seed.entry_pressure_extra_ticks);
                    }
                    if seed.taker_shadow_would_open {
                        quotes.note_pgt_taker_shadow_would_open();
                    }
                    quotes.set(seed.intent);
                }
                if let Some(seed) = no_seed {
                    quotes.note_pgt_seed_quote();
                    if seed.entry_pressure_extra_ticks > 0 {
                        quotes.note_pgt_entry_pressure(seed.entry_pressure_extra_ticks);
                    }
                    if seed.taker_shadow_would_open {
                        quotes.note_pgt_taker_shadow_would_open();
                    }
                    quotes.set(seed.intent);
                }
            }
            FlatSeedSelection::YesOnly => {
                quotes.note_pgt_single_seed_bias();
                if let Some(seed) = yes_seed {
                    quotes.note_pgt_seed_quote();
                    if seed.entry_pressure_extra_ticks > 0 {
                        quotes.note_pgt_entry_pressure(seed.entry_pressure_extra_ticks);
                    }
                    if seed.taker_shadow_would_open {
                        quotes.note_pgt_taker_shadow_would_open();
                    }
                    quotes.set(seed.intent);
                }
            }
            FlatSeedSelection::NoOnly => {
                quotes.note_pgt_single_seed_bias();
                if let Some(seed) = no_seed {
                    quotes.note_pgt_seed_quote();
                    if seed.entry_pressure_extra_ticks > 0 {
                        quotes.note_pgt_entry_pressure(seed.entry_pressure_extra_ticks);
                    }
                    if seed.taker_shadow_would_open {
                        quotes.note_pgt_taker_shadow_would_open();
                    }
                    quotes.set(seed.intent);
                }
            }
        }

        if quotes.yes_buy.is_none() && quotes.no_buy.is_none() {
            quotes.note_pgt_skip_no_seed();
        }

        quotes
    }
}

impl PairGatedTrancheStrategy {
    fn should_shadow_harvest(&self, input: StrategyTickInput<'_>, remaining_secs: u64) -> bool {
        remaining_secs <= HARVEST_WINDOW_SECS
            && input.pair_ledger.total_pairable_qty() >= HARVEST_MIN_PAIRABLE_QTY - 1e-9
    }

    fn flat_seed_raw_prices(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
    ) -> Option<(f64, f64)> {
        let ub = input.book;
        if ub.yes_bid <= 0.0 || ub.yes_ask <= 0.0 || ub.no_bid <= 0.0 || ub.no_ask <= 0.0 {
            return None;
        }
        // Opening-leg seed is anchored by the broad open_pair_band ceiling.
        // But we also reserve room for the opposite leg to complete at
        // ask-1tick if that ask is already visible now; otherwise we enter
        // first-leg fills that only close after drifting into negative pair
        // cost. Clip haircut still handles the "no immediate completion" case,
        // but price itself must not consume the entire completion budget.
        let open_pair_band =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs);
        let tick = coordinator.cfg().tick_size.max(1e-9);
        let yes_future_completion_reserve_ticks =
            pgt_seed_future_completion_reserve_ticks(remaining_secs, ub.no_ask);
        let no_future_completion_reserve_ticks =
            pgt_seed_future_completion_reserve_ticks(remaining_secs, ub.yes_ask);
        let yes_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band, ub.no_bid)?;
        let no_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band, ub.yes_bid)?;
        let yes_completion_ref =
            (ub.no_ask - tick).max(0.0) + yes_future_completion_reserve_ticks * tick;
        let no_completion_ref =
            (ub.yes_ask - tick).max(0.0) + no_future_completion_reserve_ticks * tick;
        let yes_immediate_completion_cap = (open_pair_band - yes_completion_ref).clamp(0.0, 1.0);
        let no_immediate_completion_cap = (open_pair_band - no_completion_ref).clamp(0.0, 1.0);
        let yes_ceiling = yes_bid_cap.min(yes_immediate_completion_cap);
        let no_ceiling = no_bid_cap.min(no_immediate_completion_cap);
        Some((yes_ceiling, no_ceiling))
    }

    fn flat_seed_intent_for_side(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        side: Side,
        raw_price: f64,
        size: f64,
        quotes: &mut StrategyQuotes,
    ) -> Option<SeedPlan> {
        if raw_price <= 0.0 || size <= 0.0 {
            return None;
        }

        let (best_bid, best_ask, opp_ask, opp_avg, same_qty, same_avg) = match side {
            Side::Yes => (
                input.book.yes_bid,
                input.book.yes_ask,
                input.book.no_ask,
                input.inv.no_avg_cost,
                input.inv.yes_qty,
                input.inv.yes_avg_cost,
            ),
            Side::No => (
                input.book.no_bid,
                input.book.no_ask,
                input.book.yes_ask,
                input.inv.yes_avg_cost,
                input.inv.no_qty,
                input.inv.no_avg_cost,
            ),
        };
        if best_bid <= 0.0 || best_ask <= 0.0 || opp_ask <= 0.0 {
            return None;
        }

        let risk_effect = PairArbStrategy::candidate_risk_effect(input.inv, side, size);
        let mut ceiling = raw_price;
        if let Some(tier_cap) = PairArbStrategy::tier_cap_price_for_candidate(
            input.inv,
            side,
            size,
            risk_effect,
            coordinator.cfg().pair_arb.tier_1_mult,
            coordinator.cfg().pair_arb.tier_2_mult,
        ) {
            ceiling = ceiling.min(tier_cap);
        }

        if opp_avg > 0.0 {
            let effective_pair_cost_margin = if input.inv.net_diff.abs() < PAIR_ARB_NET_EPS {
                0.0
            } else {
                coordinator.cfg().pair_arb.pair_cost_safety_margin
            };
            let vwap_ceiling = PairArbStrategy::vwap_ceiling(
                coordinator.cfg().pair_target,
                effective_pair_cost_margin,
                opp_avg,
                same_qty,
                same_avg,
                size,
            );
            ceiling = ceiling.min(vwap_ceiling);
        }

        let tick = coordinator.cfg().tick_size.max(1e-9);
        let open_pair_band = pgt_effective_open_pair_band_value(
            coordinator.cfg().open_pair_band,
            coordinator.seconds_to_market_end().unwrap_or(u64::MAX),
        );
        let mut price = self.passive_seed_price(coordinator, side, ceiling, best_bid, best_ask)?;
        if price <= 0.0 {
            return None;
        }
        let mut taker_shadow_would_open = best_ask <= ceiling + 1e-9 && best_ask > price + 1e-9;
        let mut visible_completion_slack_ticks =
            ((open_pair_band - price - opp_ask) / tick).max(-10.0);
        let mut fill_distance_ticks = ((best_ask - price) / tick).max(0.0);
        let mut preference_score = visible_completion_slack_ticks - 0.60 * fill_distance_ticks;
        let entry_pressure_extra_ticks = pgt_shadow_entry_pressure_extra_ticks(
            coordinator.cfg().dry_run,
            coordinator.seconds_to_market_end().unwrap_or(u64::MAX),
            taker_shadow_would_open,
            visible_completion_slack_ticks,
            fill_distance_ticks,
            best_bid,
            best_ask,
            price,
            ceiling,
            tick,
        );
        if entry_pressure_extra_ticks > 0 {
            let maker_cap = (best_ask - tick).max(0.0);
            price = coordinator.safe_price(
                (price + tick * f64::from(entry_pressure_extra_ticks))
                    .min(maker_cap)
                    .min(ceiling),
            );
            taker_shadow_would_open = best_ask <= ceiling + 1e-9 && best_ask > price + 1e-9;
            visible_completion_slack_ticks = ((open_pair_band - price - opp_ask) / tick).max(-10.0);
            fill_distance_ticks = ((best_ask - price) / tick).max(0.0);
            preference_score = visible_completion_slack_ticks - 0.60 * fill_distance_ticks;
        }
        if price > EXPENSIVE_SEED_PRICE + 1e-9
            && visible_completion_slack_ticks < EXPENSIVE_SEED_MIN_VISIBLE_SLACK_TICKS
        {
            return None;
        }
        let visible_breakeven_completion_slack_ticks = ((1.0 - price - opp_ask) / tick).max(-10.0);
        if visible_breakeven_completion_slack_ticks
            < SEED_MIN_VISIBLE_BREAKEVEN_COMPLETION_SLACK_TICKS - 1e-9
        {
            quotes.note_pgt_seed_reject_no_visible_breakeven_path();
            return None;
        }
        let open_path_mult = if taker_shadow_would_open {
            1.0
        } else {
            SEED_NO_IMMEDIATE_COMPLETION_CLIP_MULT
        };
        let visible_slack_mult =
            seed_visible_completion_clip_mult(open_pair_band, price, opp_ask, tick);
        let size = quantize_tenth(size * open_path_mult.min(visible_slack_mult));
        if size <= 0.0 {
            return None;
        }
        coordinator.simulate_buy(input.inv, side, size, price)?;

        Some(SeedPlan {
            size,
            taker_shadow_would_open,
            entry_pressure_extra_ticks,
            visible_completion_slack_ticks,
            fill_distance_ticks,
            preference_score,
            intent: StrategyIntent {
                side,
                direction: TradeDirection::Buy,
                price,
                size,
                reason: BidReason::Provide,
            },
        })
    }

    fn select_flat_seed_plans(
        &self,
        yes_seed: Option<&SeedPlan>,
        no_seed: Option<&SeedPlan>,
        dry_run: bool,
        latched_side: Option<Side>,
        latch_exhausted: bool,
    ) -> FlatSeedSelection {
        match (yes_seed, no_seed) {
            (None, None) => FlatSeedSelection::None,
            (Some(_), None) => FlatSeedSelection::YesOnly,
            (None, Some(_)) => FlatSeedSelection::NoOnly,
            (Some(yes), Some(no)) => {
                let yes_reject = Self::seed_geometry_reject(yes);
                let no_reject = Self::seed_geometry_reject(no);
                match (yes_reject, no_reject) {
                    (true, true) => return FlatSeedSelection::None,
                    (false, true) => return FlatSeedSelection::YesOnly,
                    (true, false) => return FlatSeedSelection::NoOnly,
                    (false, false) => {}
                }
                if dry_run {
                    match latched_side {
                        Some(Side::Yes) => return FlatSeedSelection::YesOnly,
                        Some(Side::No) => return FlatSeedSelection::NoOnly,
                        None => {}
                    }
                    if latch_exhausted
                        && !yes.taker_shadow_would_open
                        && !no.taker_shadow_would_open
                    {
                        return FlatSeedSelection::Dual;
                    }
                }
                let score_gap = yes.preference_score - no.preference_score;
                let slack_gap =
                    yes.visible_completion_slack_ticks - no.visible_completion_slack_ticks;
                let fill_gap = no.fill_distance_ticks - yes.fill_distance_ticks;
                let size_ratio = if yes.size > 0.0 && no.size > 0.0 {
                    yes.size / no.size
                } else {
                    1.0
                };
                let shadow_bias_eligible = dry_run
                    && !yes.taker_shadow_would_open
                    && !no.taker_shadow_would_open
                    && yes.entry_pressure_extra_ticks == 0
                    && no.entry_pressure_extra_ticks == 0
                    && yes.visible_completion_slack_ticks <= 1.5
                    && no.visible_completion_slack_ticks <= 1.5;
                if yes.visible_completion_slack_ticks >= 1.5
                    && no.visible_completion_slack_ticks <= 0.5
                {
                    FlatSeedSelection::YesOnly
                } else if no.visible_completion_slack_ticks >= 1.5
                    && yes.visible_completion_slack_ticks <= 0.5
                {
                    FlatSeedSelection::NoOnly
                } else if score_gap >= 3.0 || slack_gap >= 3.0 {
                    FlatSeedSelection::YesOnly
                } else if score_gap <= -3.0 || slack_gap <= -3.0 {
                    FlatSeedSelection::NoOnly
                } else if shadow_bias_eligible {
                    match latched_side {
                        Some(Side::Yes) => FlatSeedSelection::YesOnly,
                        Some(Side::No) => FlatSeedSelection::NoOnly,
                        None => {
                            if yes.visible_completion_slack_ticks.abs() <= 0.5
                                && no.visible_completion_slack_ticks.abs() <= 0.5
                                && fill_gap.abs() <= 1.0
                                && (size_ratio - 1.0).abs() <= 0.05
                            {
                                if yes.intent.price > no.intent.price + 1e-9 {
                                    // When both legs preserve roughly the same visible pair cost, prefer
                                    // the higher bid side in shadow. It is the only side with strictly
                                    // better maker fill geometry, while the opposite visible ask still
                                    // keeps the completion path near breakeven.
                                    FlatSeedSelection::YesOnly
                                } else if no.intent.price > yes.intent.price + 1e-9 {
                                    FlatSeedSelection::NoOnly
                                } else if size_ratio >= 1.20 && slack_gap >= -1.5 {
                                    FlatSeedSelection::YesOnly
                                } else if size_ratio <= (1.0 / 1.20) && slack_gap <= 1.5 {
                                    FlatSeedSelection::NoOnly
                                } else if score_gap >= 0.75
                                    || (fill_gap >= 1.0 && slack_gap >= -1.0)
                                {
                                    FlatSeedSelection::YesOnly
                                } else if score_gap <= -0.75
                                    || (fill_gap <= -1.0 && slack_gap <= 1.0)
                                {
                                    FlatSeedSelection::NoOnly
                                } else {
                                    FlatSeedSelection::Dual
                                }
                            } else if size_ratio >= 1.20 && slack_gap >= -1.5 {
                                FlatSeedSelection::YesOnly
                            } else if size_ratio <= (1.0 / 1.20) && slack_gap <= 1.5 {
                                FlatSeedSelection::NoOnly
                            } else if score_gap >= 0.75 || (fill_gap >= 1.0 && slack_gap >= -1.0) {
                                FlatSeedSelection::YesOnly
                            } else if score_gap <= -0.75 || (fill_gap <= -1.0 && slack_gap <= 1.0) {
                                FlatSeedSelection::NoOnly
                            } else {
                                FlatSeedSelection::Dual
                            }
                        }
                    }
                } else {
                    FlatSeedSelection::Dual
                }
            }
        }
    }

    fn seed_geometry_reject(seed: &SeedPlan) -> bool {
        if seed.taker_shadow_would_open {
            return false;
        }
        seed.visible_completion_slack_ticks <= -4.0 && seed.fill_distance_ticks >= 4.0
    }

    fn completion_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
        remaining_secs: u64,
    ) -> Option<CompletionPlan> {
        let first_side = active.first_side?;
        let hedge_side = opposite_side(first_side);
        let (best_bid, best_ask) = match hedge_side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };
        if best_ask <= 0.0 || active.first_vwap <= 0.0 || active.residual_qty <= f64::EPSILON {
            return None;
        }

        let completion_age_secs = pgt_active_tranche_age_secs(active);
        let repair_budget_per_share =
            input.pair_ledger.repair_budget_available / active.residual_qty.max(1.0);
        let urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true)
            * pgt_completion_urgency_mult(active.first_vwap)
            + pgt_completion_urgency_bonus(active.first_vwap, remaining_secs, completion_age_secs);
        let tuning = pgt_tuning();
        let early_pair_cap = tuning.completion_early_pair_cap.clamp(0.0, 1.0);
        let late_pair_cap = tuning
            .completion_late_pair_cap
            .max(early_pair_cap)
            .clamp(0.0, 1.0);
        let positive_edge_ceiling = early_pair_cap - active.first_vwap + repair_budget_per_share;
        let urgency_ceiling = positive_edge_ceiling + urgency_shadow;
        // Urgency can spend remaining edge, but only realized repair budget may
        // cross the profile's late pair-cost cap.
        let funded_loss_ceiling = late_pair_cap - active.first_vwap + repair_budget_per_share;
        let breakeven_unlocked = completion_age_secs >= PROFIT_FIRST_BREAKEVEN_UNLOCK_AGE_SECS
            || remaining_secs <= PROFIT_FIRST_BREAKEVEN_UNLOCK_REMAINING_SECS;
        let ceiling = if breakeven_unlocked {
            urgency_ceiling.min(funded_loss_ceiling)
        } else {
            positive_edge_ceiling.min(funded_loss_ceiling)
        };
        if ceiling <= 0.0 {
            return None;
        }

        let Some(price) = self.passive_completion_price(
            coordinator,
            hedge_side,
            ceiling,
            best_bid,
            best_ask,
            remaining_secs,
            active.first_vwap,
            completion_age_secs,
        ) else {
            return None;
        };
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }

        let size = self
            .adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
            .min(active.residual_qty.max(0.0));
        let size = quantize_tenth(size);
        if size <= 0.0 {
            return None;
        }

        let profit_taker_would_close = best_ask <= positive_edge_ceiling + 1e-9;
        let breakeven_taker_would_close =
            breakeven_unlocked && best_ask <= funded_loss_ceiling + 1e-9;
        let taker_shadow_would_close = coordinator.cfg().dry_run
            && remaining_secs > coordinator.cfg().endgame_freeze_secs
            && (profit_taker_would_close || breakeven_taker_would_close);
        let taker_close_limit = if taker_shadow_would_close {
            Some(coordinator.safe_price(best_ask))
        } else {
            None
        };

        Some(CompletionPlan {
            taker_shadow_would_close,
            taker_close_limit,
            intent: StrategyIntent {
                side: hedge_side,
                direction: TradeDirection::Buy,
                price,
                size,
                reason: BidReason::Hedge,
            },
        })
    }

    fn same_side_add_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
        remaining_secs: u64,
    ) -> Option<StrategyIntent> {
        if remaining_secs <= TAIL_COMPLETION_ONLY_SECS {
            return None;
        }
        if remaining_secs <= PRICE_AWARE_NO_NEW_OPEN_SECS {
            return None;
        }
        if pgt_active_tranche_age_secs(active) >= SAME_SIDE_ADD_MAX_COMPLETION_AGE_SECS {
            return None;
        }
        let side = active.first_side?;
        let size = pgt_same_side_add_clip_qty(active, coordinator.cfg().min_order_size)?;
        let (best_bid, best_ask, opposite_ask) = match side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask, input.book.no_ask),
            Side::No => (input.book.no_bid, input.book.no_ask, input.book.yes_ask),
        };
        if best_bid <= 0.0 || best_ask <= 0.0 || opposite_ask <= 0.0 || active.first_vwap <= 0.0 {
            return None;
        }

        let tick = coordinator.cfg().tick_size.max(1e-9);
        let open_pair_band =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs);
        let visible_completion_ref = (opposite_ask - tick).max(0.0);
        let avg_improvement_cap = active.first_vwap - tick;
        let geometry_cap = open_pair_band - visible_completion_ref - MIN_EDGE_PER_PAIR;
        let ceiling = avg_improvement_cap.min(geometry_cap).clamp(0.0, 1.0);
        if ceiling <= 0.0 {
            return None;
        }

        let price = self.passive_seed_price(coordinator, side, ceiling, best_bid, best_ask)?;
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }
        coordinator.simulate_buy(input.inv, side, size, price)?;

        Some(StrategyIntent {
            side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        })
    }

    fn passive_seed_price(
        &self,
        coordinator: &StrategyCoordinator,
        _side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> Option<f64> {
        if ceiling <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let tick = coordinator.cfg().tick_size.max(1e-9);
        // Unlike pair_arb, PGT flat seed is low-cadence and explicitly
        // maker-first. Reusing the shared tight-spread safety margin pushes a
        // one-tick market one full tick below the actual best bid, which kills
        // fills on BTC 5m. The only hard requirement here is "remain below the
        // ask", so use ask-1tick as the maker cap.
        let maker_cap = (best_ask - tick).max(0.0);
        if maker_cap <= 0.0 {
            return None;
        }

        // Flat-state seed should behave like a passive maker: quote at the bid
        // or improve by a single tick when there is enough spread, and treat
        // pair-target / tier / VWAP logic strictly as ceilings rather than as a
        // reason to chase toward the ask.
        let passive_anchor = if best_ask > best_bid + (2.0 * tick) {
            best_bid + tick
        } else {
            best_bid
        };
        let price = coordinator.safe_price(passive_anchor.min(maker_cap).min(ceiling));
        if price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

    fn adaptive_clip_qty(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: Option<PairTranche>,
        remaining_secs: u64,
    ) -> f64 {
        let tuning = pgt_tuning();
        if let Some(fixed) = tuning.fixed_clip_qty {
            return fixed.clamp(tuning.min_clip_qty, tuning.max_clip_qty);
        }

        let session_mult = session_clip_mult_utc();
        let imbalance_mult = imbalance_clip_mult(coordinator, input, active);
        let trade_index = input.episode_metrics.round_buy_fill_count.max(1) as f64;
        let trade_index_mult = (1.0 - 0.05 * (trade_index - 1.0)).max(0.70);
        let tail_mult = if remaining_secs <= 30 { 1.16 } else { 1.0 };

        (tuning.base_clip_qty * session_mult * imbalance_mult * trade_index_mult * tail_mult)
            .clamp(tuning.min_clip_qty, tuning.max_clip_qty)
    }

    fn passive_completion_price(
        &self,
        coordinator: &StrategyCoordinator,
        _side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
        remaining_secs: u64,
        first_vwap: f64,
        completion_age_secs: f64,
    ) -> Option<f64> {
        if ceiling <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let tick = coordinator.cfg().tick_size.max(1e-9);
        // PGT completion is a dedicated close-out path, not a generic reprice
        // path like pair_arb. Shadow validation against xuan only starts to make
        // sense if completion is allowed to lean to ask-1tick while remaining
        // maker-only, instead of inheriting the broader shared post-only margin.
        let maker_cap = (best_ask - tick).max(0.0);
        if maker_cap <= 0.0 {
            return None;
        }

        let spread_ticks = ((best_ask - best_bid) / tick).max(0.0);
        let max_passive_ticks = (spread_ticks - 1.0).max(0.0).floor();
        let time_ticks = if remaining_secs <= 25 {
            // Harvest edge: stay maker-only, but move all the way to ask-1tick
            // so any remaining pairable inventory has a realistic chance to close
            // before the merge pulse.
            max_passive_ticks
        } else if remaining_secs <= 45 {
            if spread_ticks >= 5.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else {
                1.0
            }
        } else if remaining_secs <= 60 {
            if spread_ticks >= 5.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 90 {
            if spread_ticks >= 4.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 120 {
            if spread_ticks >= 4.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 180 {
            if spread_ticks >= 4.0 {
                1.0
            } else {
                0.0
            }
        } else if spread_ticks >= 5.0 {
            1.0
        } else {
            0.0
        };

        // Once a residual leg has been sitting for a while, completion should
        // progressively lean further inside the spread even outside tail mode.
        // This keeps the path maker-only, but prevents 60s+ close delays where
        // a tranche is technically closable yet we keep repricing too slowly.
        let age_ticks = if completion_age_secs >= 45.0 {
            max_passive_ticks
        } else if completion_age_secs >= 25.0 {
            if spread_ticks >= 4.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if completion_age_secs >= 12.0 {
            if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let expensive_leg_age_ticks = if first_vwap >= 0.50 {
            if completion_age_secs >= 16.0 {
                if spread_ticks >= 3.0 - 1e-9 {
                    2.0
                } else if spread_ticks >= 2.0 - 1e-9 {
                    1.0
                } else {
                    0.0
                }
            } else if completion_age_secs >= 8.0 {
                if spread_ticks >= 3.0 - 1e-9 {
                    1.0
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };

        let improve_ticks = time_ticks
            .max(age_ticks)
            .max(expensive_leg_age_ticks)
            .min(max_passive_ticks);
        let passive_anchor = best_bid + improve_ticks * tick;
        let price = coordinator.safe_price(passive_anchor.min(maker_cap).min(ceiling));
        if price > 0.0 {
            Some(price)
        } else {
            None
        }
    }
}

pub(crate) fn pgt_effective_open_pair_band_value(base: f64, remaining_secs: u64) -> f64 {
    let tuning = pgt_tuning();
    if tuning.profile != PgtShadowProfile::Legacy {
        return tuning.open_pair_band(base);
    }
    if remaining_secs == u64::MAX {
        return base;
    }
    if remaining_secs > PGT_OPEN_PAIR_BAND_WIDE_SECS {
        1.0
    } else if remaining_secs > PGT_OPEN_PAIR_BAND_MID_SECS {
        base.max(PGT_OPEN_PAIR_BAND_MID_VALUE)
    } else {
        base
    }
}

pub(crate) fn pgt_open_leg_ceiling_from_opposite_bid(
    open_pair_band: f64,
    opposite_bid: f64,
) -> Option<f64> {
    if opposite_bid <= 0.0 {
        return None;
    }
    let ceiling = (open_pair_band - opposite_bid).clamp(0.0, 1.0);
    if ceiling > 0.0 {
        Some(ceiling)
    } else {
        None
    }
}

pub(crate) fn pgt_seed_future_completion_reserve_ticks(
    remaining_secs: u64,
    opposite_ask: f64,
) -> f64 {
    if remaining_secs == u64::MAX {
        return 0.0;
    }
    let base = if remaining_secs > PGT_OPEN_PAIR_BAND_WIDE_SECS {
        1.0
    } else if remaining_secs > PGT_OPEN_PAIR_BAND_MID_SECS {
        0.5
    } else {
        0.0
    };
    let extra = if remaining_secs > 240 && opposite_ask >= 0.52 {
        1.0
    } else {
        0.0
    };
    base + extra
}

fn pgt_profile_seed_open_remaining_allowed(remaining_secs: u64) -> bool {
    let tuning = pgt_tuning();
    if let Some(max_remaining) = tuning.seed_open_max_remaining_secs {
        if remaining_secs == u64::MAX || remaining_secs > max_remaining {
            return false;
        }
    }
    true
}

fn pgt_seed_open_window_allowed(seed: &SeedPlan, remaining_secs: u64) -> bool {
    if !pgt_profile_seed_open_remaining_allowed(remaining_secs) {
        return false;
    }
    if remaining_secs > PRICE_AWARE_NO_NEW_OPEN_SECS {
        return true;
    }
    if remaining_secs <= HARD_NO_NEW_OPEN_SECS {
        return false;
    }

    seed.intent.price <= LATE_OPEN_MAX_SEED_PRICE + 1e-9
        && seed.visible_completion_slack_ticks >= LATE_OPEN_MIN_VISIBLE_COMPLETION_SLACK_TICKS
}

fn pgt_shadow_entry_pressure_extra_ticks(
    dry_run: bool,
    remaining_secs: u64,
    taker_shadow_would_open: bool,
    visible_completion_slack_ticks: f64,
    fill_distance_ticks: f64,
    best_bid: f64,
    best_ask: f64,
    price: f64,
    ceiling: f64,
    tick: f64,
) -> u8 {
    if !dry_run || remaining_secs <= PRICE_AWARE_NO_NEW_OPEN_SECS {
        return 0;
    }
    if taker_shadow_would_open || tick <= 0.0 || best_ask <= 0.0 || ceiling <= 0.0 {
        return 0;
    }
    let maker_cap = (best_ask - tick).max(0.0);
    let max_price = maker_cap.min(ceiling);
    if max_price <= price + 1e-9 {
        return 0;
    }
    let room_ticks = ((max_price - price) / tick).floor().max(0.0);
    let spread_ticks = ((best_ask - best_bid) / tick).max(0.0);
    if visible_completion_slack_ticks >= 4.0
        && fill_distance_ticks >= 4.0
        && spread_ticks >= 5.0
        && room_ticks >= 2.0
    {
        2
    } else if visible_completion_slack_ticks >= 1.0
        && fill_distance_ticks >= 1.0
        && spread_ticks >= 2.0
        && room_ticks >= 1.0
    {
        1
    } else {
        0
    }
}

fn pgt_completion_urgency_mult(first_vwap: f64) -> f64 {
    if first_vwap >= 0.50 {
        0.40
    } else {
        1.0
    }
}

fn pgt_active_tranche_age_secs(active: PairTranche) -> f64 {
    active
        .last_transition_at
        .map(|ts| ts.elapsed().as_secs_f64())
        .or_else(|| active.opened_at.map(|ts| ts.elapsed().as_secs_f64()))
        .unwrap_or(0.0)
}

pub(crate) fn pgt_same_side_add_state_eligible(active: PairTranche) -> bool {
    active.first_side.is_some()
        && active.same_side_add_count < PGT_MAX_SAME_SIDE_ADD_COUNT
        && active.hedge_qty <= 1e-9
        && active.first_qty >= SAME_SIDE_ADD_MIN_FIRST_QTY - 1e-9
        && active.residual_qty >= SAME_SIDE_ADD_MIN_RESIDUAL_QTY - 1e-9
}

pub(crate) fn pgt_same_side_add_clip_qty(active: PairTranche, min_order_size: f64) -> Option<f64> {
    if !pgt_same_side_add_state_eligible(active) {
        return None;
    }
    let raw = (active.first_qty * SAME_SIDE_ADD_FRACTION).min(SAME_SIDE_ADD_MAX_QTY);
    let qty = quantize_tenth(raw);
    if qty + 1e-9 >= min_order_size.max(0.0) {
        Some(qty)
    } else {
        None
    }
}

fn pgt_completion_urgency_bonus(
    first_vwap: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
) -> f64 {
    if remaining_secs <= 120 {
        return 0.0;
    }
    if first_vwap <= 0.48 && completion_age_secs >= 45.0 {
        // A cheap first leg can spend one extra half-tick of edge early and
        // still close at breakeven, e.g. 0.47 + 0.53 = 1.00.
        0.005
    } else if (0.49..0.50).contains(&first_vwap) && completion_age_secs >= 60.0 {
        0.005
    } else {
        0.0
    }
}

fn seed_visible_completion_clip_mult(
    open_pair_band: f64,
    seed_price: f64,
    opposite_ask: f64,
    tick: f64,
) -> f64 {
    if open_pair_band <= 0.0 || seed_price <= 0.0 || opposite_ask <= 0.0 || tick <= 0.0 {
        return SEED_THIN_SLACK_CLIP_MULT_TICK_0;
    }
    let immediate_maker_completion = (opposite_ask - tick).max(0.0);
    let slack_ticks = ((open_pair_band - seed_price - immediate_maker_completion) / tick).floor();
    if slack_ticks >= 3.0 {
        1.0
    } else if slack_ticks >= 2.0 {
        SEED_THIN_SLACK_CLIP_MULT_TICK_2
    } else if slack_ticks >= 1.0 {
        SEED_THIN_SLACK_CLIP_MULT_TICK_1
    } else {
        SEED_THIN_SLACK_CLIP_MULT_TICK_0
    }
}

fn imbalance_clip_mult(
    coordinator: &StrategyCoordinator,
    input: StrategyTickInput<'_>,
    active: Option<PairTranche>,
) -> f64 {
    let abs_imb = if let Some(tranche) = active {
        let denom = tranche
            .first_qty
            .max(tranche.hedge_qty)
            .max(tranche.residual_qty)
            .max(1.0);
        (tranche.residual_qty.max(0.0) / denom).clamp(0.0, 1.0)
    } else {
        (input.inv.net_diff.abs() / coordinator.cfg().max_net_diff.max(1.0)).clamp(0.0, 1.0)
    };

    if abs_imb < 0.05 {
        1.00
    } else if abs_imb < 0.15 {
        0.95
    } else if abs_imb < 0.30 {
        1.00
    } else {
        1.20
    }
}

fn session_clip_mult_utc() -> f64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let hour = ((now_secs / 3600) % 24) as u8;
    if (16..=22).contains(&hour) {
        1.15
    } else if (3..=12).contains(&hour) {
        0.80
    } else {
        1.00
    }
}

fn quantize_tenth(qty: f64) -> f64 {
    let rounded = (qty * 10.0).floor() / 10.0;
    if rounded >= 0.1 {
        rounded
    } else {
        0.0
    }
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn replay_focused_profile_matches_replay_search_candidate() {
        let tuning = PgtTuning::replay_focused_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::ReplayFocusedV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(225));
        assert_eq!(tuning.open_pair_band(0.99), 0.980);
        assert_eq!(tuning.completion_early_pair_cap, 0.975);
        assert_eq!(tuning.completion_late_pair_cap, 0.995);
        assert_eq!(tuning.fixed_clip_qty, Some(57.6));
    }

    #[test]
    fn replay_lower_clip_profile_matches_risk_control_candidate() {
        let tuning = PgtTuning::replay_lower_clip_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::ReplayLowerClipV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(240));
        assert_eq!(tuning.open_pair_band(0.99), 0.970);
        assert_eq!(tuning.completion_early_pair_cap, 0.975);
        assert_eq!(tuning.completion_late_pair_cap, 1.000);
        assert_eq!(tuning.fixed_clip_qty, Some(30.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_plan(side: Side, price: f64, entry_pressure_extra_ticks: u8) -> SeedPlan {
        SeedPlan {
            size: 72.0,
            taker_shadow_would_open: false,
            entry_pressure_extra_ticks,
            visible_completion_slack_ticks: 1.0,
            fill_distance_ticks: 2.0,
            preference_score: 0.0,
            intent: StrategyIntent {
                side,
                direction: TradeDirection::Buy,
                price,
                size: 72.0,
                reason: BidReason::Provide,
            },
        }
    }

    #[test]
    fn select_flat_seed_plans_respects_latched_side_even_with_entry_pressure() {
        let strategy = PairGatedTrancheStrategy;
        let yes = seed_plan(Side::Yes, 0.47, 1);
        let no = seed_plan(Side::No, 0.50, 1);

        let selection =
            strategy.select_flat_seed_plans(Some(&yes), Some(&no), true, Some(Side::No), false);

        assert_eq!(selection, FlatSeedSelection::NoOnly);
    }

    #[test]
    fn select_flat_seed_plans_returns_dual_after_latch_exhaustion() {
        let strategy = PairGatedTrancheStrategy;
        let yes = seed_plan(Side::Yes, 0.47, 0);
        let no = seed_plan(Side::No, 0.50, 0);

        let selection = strategy.select_flat_seed_plans(Some(&yes), Some(&no), true, None, true);

        assert_eq!(selection, FlatSeedSelection::Dual);
    }
}
