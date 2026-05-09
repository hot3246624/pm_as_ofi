use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{StrategyCoordinator, PAIR_ARB_NET_EPS};
use crate::polymarket::messages::{BidReason, InventoryState, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairLedgerSnapshot, PairTranche};
use crate::polymarket::types::Side;
use tracing::info;

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
const COMPLETION_FULL_RESIDUAL_REMAINING_SECS: u64 = 90;
const XUAN_LADDER_ROUND_SECS: u64 = 300;
const XUAN_LADDER_START_OFFSET_SECS: u64 = 4;
const XUAN_LADDER_STOP_BEFORE_END_SECS: u64 = 25;
const XUAN_LADDER_OPEN_PAIR_CAP: f64 = 1.040;
const XUAN_LADDER_COMPLETION_FRESH_PAIR_CAP: f64 = 0.990;
const XUAN_LADDER_COMPLETION_WARM_PAIR_CAP: f64 = 0.995;
const XUAN_LADDER_COMPLETION_STALE_PAIR_CAP: f64 = 1.000;
const XUAN_LADDER_COMPLETION_MATURE_PAIR_CAP: f64 = 1.000;
const XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP: f64 = 1.010;
const XUAN_LADDER_FUNDED_REPAIR_PAIR_CAP: f64 = 1.030;
const XUAN_LADDER_TAIL_INSURANCE_PAIR_CAP: f64 = 1.030;
const XUAN_LADDER_TAIL_INSURANCE_REMAINING_SECS: u64 = 45;
const XUAN_LADDER_LAST_CHANCE_INSURANCE_PAIR_CAP: f64 = 1.050;
const XUAN_LADDER_LAST_CHANCE_INSURANCE_REMAINING_SECS: u64 = 15;
const XUAN_LADDER_TIMEOUT_INSURANCE_MIN_AGE_SECS: f64 = 4.0;
const XUAN_LADDER_TIMEOUT_MARGINAL_INSURANCE_MIN_AGE_SECS: f64 = 1.5;
const XUAN_LADDER_TIMEOUT_FAST_FIRST_MIN_PRICE: f64 = 0.34;
const XUAN_LADDER_TIMEOUT_CHEAP_FIRST_MAX_PRICE: f64 = 0.30;
const XUAN_LADDER_TIMEOUT_MID_FIRST_MAX_PRICE: f64 = 0.35;
const XUAN_LADDER_TIMEOUT_CHEAP_PAIR_CAP: f64 = 1.010;
const XUAN_LADDER_TIMEOUT_MID_PAIR_CAP: f64 = 1.005;
const XUAN_LADDER_TIMEOUT_HIGH_PAIR_CAP: f64 = 1.000;
const XUAN_LADDER_TAKER_INSURANCE_MIN_AGE_SECS: f64 = 45.0;
const XUAN_LADDER_TAKER_INSURANCE_PAIR_CAP: f64 = 1.010;
const XUAN_LADDER_COMPLETION_FRESH_AGE_SECS: f64 = 20.0;
const XUAN_LADDER_COMPLETION_WARM_AGE_SECS: f64 = 45.0;
const XUAN_LADDER_COMPLETION_STALE_AGE_SECS: f64 = 90.0;
const XUAN_LADDER_REPAIR_BUDGET_MIN_AGE_SECS: f64 = XUAN_LADDER_COMPLETION_WARM_AGE_SECS;
const XUAN_LADDER_REPAIR_BUDGET_MAX_REMAINING_SECS: u64 = 45;
const XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS: f64 = -4.0;
const XUAN_LADDER_EXPENSIVE_SEED_MIN_SLACK_TICKS: f64 = 1.0;
const XUAN_LADDER_EXPENSIVE_SEED_DOMINANCE_TICKS: f64 = 2.0;
const XUAN_LADDER_COST_BRAKE_MIN_BUY_FILLS: u64 = 2;
const XUAN_LADDER_COST_BRAKE_PAIR_COST: f64 = 1.000;
const XUAN_LADDER_COST_BRAKE_MIN_SLACK_TICKS: f64 = 0.0;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_PAIR_COST: f64 = 0.900;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_MIN_REMAINING_SECS: u64 = 120;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_MAX_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_PAIR_COST: f64 = 1.000;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_MIN_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_MAX_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_CLIP_QTY: f64 = 60.0;
const XUAN_LADDER_REOPEN_PROJECTED_PAIR_CAP: f64 = 1.000;
const XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP: f64 = 0.995;
const XUAN_LADDER_LOW_FIRST_SEED_PRICE_HI: f64 = 0.60;
const XUAN_LADDER_LOW_FIRST_SEED_TAKER_COMPLETION_PAIR_CAP: f64 = 1.000;
const XUAN_LADDER_LOW_FIRST_RELAXED_COMPLETION_CLIP_QTY: f64 = 20.0;
const XUAN_LADDER_SEED_MAKER_COMPLETION_PAIR_CAP: f64 = 0.990;
const XUAN_LADDER_MAKER_ONLY_SEED_CLIP_QTY: f64 = 45.0;
const XUAN_LADDER_DUAL_SEED_SIZE_TOLERANCE: f64 = 0.05;
const XUAN_LADDER_LAST_CHANCE_CLOSE_REMAINING_SECS: u64 = 15;
const XUAN_LADDER_LAST_CHANCE_CLOSE_MIN_AGE_SECS: f64 = 45.0;
const XUAN_LADDER_LAST_CHANCE_CLOSE_MAX_ASK: f64 = 0.99;
const XUAN_LADDER_TAIL_DIAG_REMAINING_SECS: u64 = 60;
const XUAN_LADDER_TAIL_DIAG_INTERVAL_SECS: u64 = 5;
const XUAN_TAIL_TAKER_MIN_REMAINING_SECS: u64 = 35;
const XUAN_TAIL_TAKER_MAX_REMAINING_SECS: u64 = 60;
const XUAN_TAIL_TAKER_HARD_NO_NEW_OPEN_SECS: u64 = 25;
const XUAN_TAIL_TAKER_FIRST_MIN_ASK: f64 = 0.62;
const XUAN_TAIL_TAKER_FIRST_MAX_ASK: f64 = 0.70;
const XUAN_TAIL_TAKER_OPEN_PAIR_CAP: f64 = 1.030;
const XUAN_TAIL_TAKER_CLIP_QTY: f64 = 75.0;
const XUAN_TAIL_TAKER_COMPLETION_FRESH_PAIR_CAP: f64 = 0.900;
const XUAN_TAIL_TAKER_COMPLETION_WARM_PAIR_CAP: f64 = 0.950;
const XUAN_TAIL_TAKER_COMPLETION_RESCUE_PAIR_CAP: f64 = 1.000;
const XUAN_TAIL_TAKER_COMPLETION_FRESH_AGE_SECS: f64 = 30.0;
const XUAN_TAIL_TAKER_COMPLETION_WARM_AGE_SECS: f64 = 50.0;
const XUAN_CYCLE_MERGE_START_OFFSET_SECS: u64 = 4;
const XUAN_CYCLE_MERGE_STOP_BEFORE_END_SECS: u64 = 25;
const XUAN_CYCLE_MERGE_SEED_MAX_PRICE: f64 = 0.06;
const XUAN_CYCLE_MERGE_PAIR_CAP: f64 = 1.000;
const XUAN_CYCLE_MERGE_CLIP_QTY: f64 = 15.0;
const XUAN_CYCLE_MERGE_MIN_FILL_QTY: f64 = 10.0;
const XUAN_CYCLE_MERGE_MAX_INVENTORY_COST: f64 = 50.0;

static PGT_LAST_SEED_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);
static PGT_LAST_COMPLETION_NONE_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);
static PGT_LAST_TAIL_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgtShadowProfile {
    Legacy,
    ReplayFocusedV1,
    ReplayLowerClipV1,
    XuanLadderV1,
    XuanTailTakerV1,
    XuanCycleMergeV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgtClipProfile {
    Adaptive,
    XuanLadderV1,
}

#[derive(Debug, Clone, Copy)]
struct PgtTuning {
    profile: PgtShadowProfile,
    seed_open_max_remaining_secs: Option<u64>,
    seed_open_min_remaining_secs: Option<u64>,
    hard_no_new_open_secs: u64,
    price_aware_no_new_open_secs: u64,
    open_pair_band_cap: Option<f64>,
    completion_early_pair_cap: f64,
    completion_late_pair_cap: f64,
    taker_close_pair_cap: f64,
    fixed_clip_qty: Option<f64>,
    clip_profile: PgtClipProfile,
    preserve_seed_clip_qty: bool,
    expensive_seed_min_visible_slack_ticks: f64,
    seed_min_visible_breakeven_slack_ticks: f64,
    base_clip_qty: f64,
    min_clip_qty: f64,
    max_clip_qty: f64,
}

impl PgtTuning {
    fn legacy() -> Self {
        Self {
            profile: PgtShadowProfile::Legacy,
            seed_open_max_remaining_secs: None,
            seed_open_min_remaining_secs: None,
            hard_no_new_open_secs: HARD_NO_NEW_OPEN_SECS,
            price_aware_no_new_open_secs: PRICE_AWARE_NO_NEW_OPEN_SECS,
            open_pair_band_cap: None,
            completion_early_pair_cap: 1.0 - MIN_EDGE_PER_PAIR,
            completion_late_pair_cap: 1.0,
            taker_close_pair_cap: 1.0,
            fixed_clip_qty: None,
            clip_profile: PgtClipProfile::Adaptive,
            preserve_seed_clip_qty: false,
            expensive_seed_min_visible_slack_ticks: EXPENSIVE_SEED_MIN_VISIBLE_SLACK_TICKS,
            seed_min_visible_breakeven_slack_ticks:
                SEED_MIN_VISIBLE_BREAKEVEN_COMPLETION_SLACK_TICKS,
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
            seed_open_min_remaining_secs: Some(HARD_NO_NEW_OPEN_SECS),
            hard_no_new_open_secs: HARD_NO_NEW_OPEN_SECS,
            price_aware_no_new_open_secs: PRICE_AWARE_NO_NEW_OPEN_SECS,
            open_pair_band_cap: Some(0.980),
            completion_early_pair_cap: 0.975,
            completion_late_pair_cap: 0.995,
            taker_close_pair_cap: 0.995,
            fixed_clip_qty: Some(57.6),
            clip_profile: PgtClipProfile::Adaptive,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: EXPENSIVE_SEED_MIN_VISIBLE_SLACK_TICKS,
            seed_min_visible_breakeven_slack_ticks:
                SEED_MIN_VISIBLE_BREAKEVEN_COMPLETION_SLACK_TICKS,
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
            seed_open_min_remaining_secs: Some(HARD_NO_NEW_OPEN_SECS),
            hard_no_new_open_secs: HARD_NO_NEW_OPEN_SECS,
            price_aware_no_new_open_secs: PRICE_AWARE_NO_NEW_OPEN_SECS,
            open_pair_band_cap: Some(0.970),
            completion_early_pair_cap: 0.975,
            completion_late_pair_cap: 1.000,
            taker_close_pair_cap: 1.000,
            fixed_clip_qty: Some(30.0),
            clip_profile: PgtClipProfile::Adaptive,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: EXPENSIVE_SEED_MIN_VISIBLE_SLACK_TICKS,
            seed_min_visible_breakeven_slack_ticks:
                SEED_MIN_VISIBLE_BREAKEVEN_COMPLETION_SLACK_TICKS,
            base_clip_qty: 30.0,
            min_clip_qty: 30.0,
            max_clip_qty: 30.0,
        }
    }

    fn xuan_ladder_v1() -> Self {
        Self {
            profile: PgtShadowProfile::XuanLadderV1,
            // Recent xuan samples start as early as t+4s and keep opening until
            // late round. This profile is shadow-only; it intentionally models
            // the public ladder shape rather than the conservative replay subset.
            seed_open_max_remaining_secs: Some(
                XUAN_LADDER_ROUND_SECS - XUAN_LADDER_START_OFFSET_SECS,
            ),
            seed_open_min_remaining_secs: Some(XUAN_LADDER_STOP_BEFORE_END_SECS),
            hard_no_new_open_secs: XUAN_LADDER_STOP_BEFORE_END_SECS,
            price_aware_no_new_open_secs: XUAN_LADDER_STOP_BEFORE_END_SECS,
            open_pair_band_cap: Some(XUAN_LADDER_OPEN_PAIR_CAP),
            completion_early_pair_cap: XUAN_LADDER_COMPLETION_MATURE_PAIR_CAP,
            completion_late_pair_cap: XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP,
            taker_close_pair_cap: XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP,
            fixed_clip_qty: None,
            clip_profile: PgtClipProfile::XuanLadderV1,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: XUAN_LADDER_EXPENSIVE_SEED_MIN_SLACK_TICKS,
            seed_min_visible_breakeven_slack_ticks: XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS,
            base_clip_qty: 135.0,
            min_clip_qty: 45.0,
            max_clip_qty: 250.0,
        }
    }

    fn xuan_tail_taker_v1() -> Self {
        Self {
            profile: PgtShadowProfile::XuanTailTakerV1,
            seed_open_max_remaining_secs: Some(XUAN_TAIL_TAKER_MAX_REMAINING_SECS),
            seed_open_min_remaining_secs: Some(XUAN_TAIL_TAKER_MIN_REMAINING_SECS),
            hard_no_new_open_secs: XUAN_TAIL_TAKER_HARD_NO_NEW_OPEN_SECS,
            price_aware_no_new_open_secs: XUAN_TAIL_TAKER_HARD_NO_NEW_OPEN_SECS,
            open_pair_band_cap: Some(XUAN_TAIL_TAKER_OPEN_PAIR_CAP),
            completion_early_pair_cap: XUAN_TAIL_TAKER_COMPLETION_FRESH_PAIR_CAP,
            completion_late_pair_cap: XUAN_TAIL_TAKER_COMPLETION_RESCUE_PAIR_CAP,
            taker_close_pair_cap: XUAN_TAIL_TAKER_COMPLETION_RESCUE_PAIR_CAP,
            fixed_clip_qty: Some(XUAN_TAIL_TAKER_CLIP_QTY),
            clip_profile: PgtClipProfile::Adaptive,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS,
            seed_min_visible_breakeven_slack_ticks: XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS,
            base_clip_qty: XUAN_TAIL_TAKER_CLIP_QTY,
            min_clip_qty: XUAN_TAIL_TAKER_CLIP_QTY,
            max_clip_qty: XUAN_TAIL_TAKER_CLIP_QTY,
        }
    }

    fn xuan_cycle_merge_v1() -> Self {
        Self {
            profile: PgtShadowProfile::XuanCycleMergeV1,
            seed_open_max_remaining_secs: Some(
                XUAN_LADDER_ROUND_SECS - XUAN_CYCLE_MERGE_START_OFFSET_SECS,
            ),
            seed_open_min_remaining_secs: Some(XUAN_CYCLE_MERGE_STOP_BEFORE_END_SECS),
            hard_no_new_open_secs: XUAN_CYCLE_MERGE_STOP_BEFORE_END_SECS,
            price_aware_no_new_open_secs: XUAN_CYCLE_MERGE_STOP_BEFORE_END_SECS,
            open_pair_band_cap: Some(XUAN_CYCLE_MERGE_PAIR_CAP),
            completion_early_pair_cap: XUAN_CYCLE_MERGE_PAIR_CAP,
            completion_late_pair_cap: XUAN_CYCLE_MERGE_PAIR_CAP,
            taker_close_pair_cap: XUAN_CYCLE_MERGE_PAIR_CAP,
            fixed_clip_qty: Some(XUAN_CYCLE_MERGE_CLIP_QTY),
            clip_profile: PgtClipProfile::Adaptive,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: -100.0,
            seed_min_visible_breakeven_slack_ticks: -100.0,
            base_clip_qty: XUAN_CYCLE_MERGE_CLIP_QTY,
            min_clip_qty: XUAN_CYCLE_MERGE_CLIP_QTY,
            max_clip_qty: XUAN_CYCLE_MERGE_CLIP_QTY,
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
            "xuan_ladder_v1" | "xuan_ladder" | "xuan_latest" | "xuan" => Self::xuan_ladder_v1(),
            "xuan_tail_taker_v1" | "xuan_tail_taker" | "xuan_tail" => Self::xuan_tail_taker_v1(),
            "xuan_cycle_merge_v1" | "xuan_cycle_merge" | "xuan_cycle" => {
                Self::xuan_cycle_merge_v1()
            }
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
            if matches!(
                self.profile,
                PgtShadowProfile::XuanLadderV1
                    | PgtShadowProfile::XuanTailTakerV1
                    | PgtShadowProfile::XuanCycleMergeV1
            ) {
                base.max(cap)
            } else {
                base.min(cap)
            }
        } else {
            base
        }
    }
}

fn pgt_tuning() -> PgtTuning {
    static TUNING: OnceLock<PgtTuning> = OnceLock::new();
    *TUNING.get_or_init(PgtTuning::from_env)
}

pub(crate) fn pgt_shadow_taker_open_exec_enabled() -> bool {
    pgt_tuning().profile == PgtShadowProfile::XuanTailTakerV1
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
    visible_taker_completion_ok: bool,
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
        let tuning = pgt_tuning();
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let hard_no_new_open = remaining_secs <= tuning.hard_no_new_open_secs;
        let harvest_window_active = self.should_shadow_harvest(input, remaining_secs);

        if let Some(active) = input
            .pair_ledger
            .active_tranche
            .filter(|tranche| tranche.first_side.is_some() && tranche.residual_qty > f64::EPSILON)
        {
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
        let post_close_reopen_attempted = coordinator
            .pgt_post_close_reopen_attempted_for_fill_count(
                input.episode_metrics.round_buy_fill_count,
            );
        if coordinator.pgt_blocks_new_seed_after_rescue_close()
            && !pgt_allow_reopen_after_rescue_close(
                tuning,
                input,
                remaining_secs,
                post_close_reopen_attempted,
            )
        {
            quotes.note_pgt_skip_after_rescue_close();
            return quotes;
        }
        if pgt_blocks_reopen_after_closed_pair(tuning, input, post_close_reopen_attempted) {
            quotes.note_pgt_skip_after_closed_pair();
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

        let size = self
            .adaptive_clip_qty(coordinator, input, None, remaining_secs)
            .min(pgt_xuan_cycle_available_buy_qty(
                tuning,
                input.inv,
                XUAN_CYCLE_MERGE_SEED_MAX_PRICE,
            ));
        let size = quantize_tenth(size);
        if size < pgt_profile_min_seed_qty(tuning).max(coordinator.cfg().min_order_size) {
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
            tuning.profile,
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
        if pgt_is_xuan_cycle_merge_profile(pgt_tuning()) {
            return Some((
                XUAN_CYCLE_MERGE_SEED_MAX_PRICE,
                XUAN_CYCLE_MERGE_SEED_MAX_PRICE,
            ));
        }
        // Opening-leg seed is anchored by the broad open_pair_band ceiling.
        // But we also reserve room for the opposite leg to complete at
        // ask-1tick if that ask is already visible now; otherwise we enter
        // first-leg fills that only close after drifting into negative pair
        // cost. Legacy clip haircut still handles the "no immediate completion"
        // case, while replay profiles preserve searched seed clip size and use
        // price gates to control completion budget.
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
        let tuning = pgt_tuning();
        if pgt_tail_taker_seed_price_blocks(tuning, best_ask) {
            return None;
        }

        let risk_effect = PairArbStrategy::candidate_risk_effect(input.inv, side, size);
        let mut ceiling = raw_price;
        if let Some(tier_cap) = PairArbStrategy::tier_cap_price_for_candidate(
            input.inv,
            side,
            size,
            risk_effect,
            coordinator.cfg().pair_arb.tier_mode,
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
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let open_pair_band =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs);
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
            remaining_secs,
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
            && visible_completion_slack_ticks < tuning.expensive_seed_min_visible_slack_ticks
        {
            return None;
        }
        let visible_breakeven_completion_slack_ticks = ((1.0 - price - opp_ask) / tick).max(-10.0);
        let visible_taker_completion_ok =
            price + opp_ask <= pgt_xuan_ladder_seed_taker_completion_pair_cap(price) + 1e-9;
        let recent_pair_cost = pgt_recent_closed_pair_cost(input.pair_ledger);
        let min_visible_breakeven_slack_ticks = pgt_seed_min_visible_breakeven_slack_ticks(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            input.pair_ledger.repair_budget_available,
            recent_pair_cost,
        );
        if visible_breakeven_completion_slack_ticks < min_visible_breakeven_slack_ticks - 1e-9 {
            pgt_maybe_log_seed_admission_diag(
                coordinator.cfg().dry_run,
                tuning,
                side,
                remaining_secs,
                "no_visible_breakeven_path",
                price,
                size,
                best_bid,
                best_ask,
                opp_ask,
                open_pair_band,
                tick,
                taker_shadow_would_open,
                entry_pressure_extra_ticks,
                visible_completion_slack_ticks,
                visible_breakeven_completion_slack_ticks,
                fill_distance_ticks,
                min_visible_breakeven_slack_ticks,
            );
            quotes.note_pgt_seed_reject_no_visible_breakeven_path();
            return None;
        }
        if pgt_xuan_ladder_seed_visible_completion_guard_blocks(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            price,
            opp_ask,
            tick,
        ) {
            pgt_maybe_log_seed_admission_diag(
                coordinator.cfg().dry_run,
                tuning,
                side,
                remaining_secs,
                "blocked_visible_completion_pair_cost",
                price,
                size,
                best_bid,
                best_ask,
                opp_ask,
                open_pair_band,
                tick,
                taker_shadow_would_open,
                entry_pressure_extra_ticks,
                visible_completion_slack_ticks,
                visible_breakeven_completion_slack_ticks,
                fill_distance_ticks,
                min_visible_breakeven_slack_ticks,
            );
            quotes.note_pgt_seed_reject_no_visible_breakeven_path();
            return None;
        }
        if pgt_xuan_ladder_reopen_seed_quality_blocks(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            recent_pair_cost,
            price,
            opp_ask,
            tick,
        ) {
            pgt_maybe_log_seed_admission_diag(
                coordinator.cfg().dry_run,
                tuning,
                side,
                remaining_secs,
                "blocked_reopen_pair_cost",
                price,
                size,
                best_bid,
                best_ask,
                opp_ask,
                open_pair_band,
                tick,
                taker_shadow_would_open,
                entry_pressure_extra_ticks,
                visible_completion_slack_ticks,
                visible_breakeven_completion_slack_ticks,
                fill_distance_ticks,
                min_visible_breakeven_slack_ticks,
            );
            quotes.note_pgt_seed_reject_no_visible_breakeven_path();
            return None;
        }
        pgt_maybe_log_seed_admission_diag(
            coordinator.cfg().dry_run,
            tuning,
            side,
            remaining_secs,
            "accepted",
            price,
            size,
            best_bid,
            best_ask,
            opp_ask,
            open_pair_band,
            tick,
            taker_shadow_would_open,
            entry_pressure_extra_ticks,
            visible_completion_slack_ticks,
            visible_breakeven_completion_slack_ticks,
            fill_distance_ticks,
            min_visible_breakeven_slack_ticks,
        );
        let cycle_merge_seed = pgt_is_xuan_cycle_merge_profile(tuning);
        let preserve_seed_clip_qty =
            cycle_merge_seed || (tuning.preserve_seed_clip_qty && visible_taker_completion_ok);
        let open_path_mult = if taker_shadow_would_open || preserve_seed_clip_qty {
            1.0
        } else {
            SEED_NO_IMMEDIATE_COMPLETION_CLIP_MULT
        };
        let visible_slack_mult = if preserve_seed_clip_qty {
            1.0
        } else {
            seed_visible_completion_clip_mult(open_pair_band, price, opp_ask, tick)
        };
        let mut size = quantize_tenth(size * open_path_mult.min(visible_slack_mult));
        if cycle_merge_seed {
            size = size.min(pgt_xuan_cycle_available_buy_qty(tuning, input.inv, price));
        }
        if pgt_xuan_ladder_maker_only_seed_clip_caps(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            visible_taker_completion_ok,
        ) {
            size = size.min(XUAN_LADDER_MAKER_ONLY_SEED_CLIP_QTY);
        }
        if pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            price,
            opp_ask,
        ) {
            size = size.min(XUAN_LADDER_LOW_FIRST_RELAXED_COMPLETION_CLIP_QTY);
        }
        if pgt_xuan_ladder_reopen_seed_clip_caps(tuning, input.episode_metrics.round_buy_fill_count)
        {
            size = size.min(XUAN_LADDER_REOPEN_AFTER_CLOSED_CLIP_QTY);
        }
        if size < pgt_profile_min_seed_qty(tuning).max(coordinator.cfg().min_order_size) {
            return None;
        }
        coordinator.simulate_buy(input.inv, side, size, price)?;

        Some(SeedPlan {
            size,
            taker_shadow_would_open,
            visible_taker_completion_ok,
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
        profile: PgtShadowProfile,
        dry_run: bool,
        latched_side: Option<Side>,
        latch_exhausted: bool,
    ) -> FlatSeedSelection {
        match (yes_seed, no_seed) {
            (None, None) => FlatSeedSelection::None,
            (Some(_), None) => FlatSeedSelection::YesOnly,
            (None, Some(_)) => FlatSeedSelection::NoOnly,
            (Some(yes), Some(no)) => {
                if profile == PgtShadowProfile::XuanCycleMergeV1 {
                    return FlatSeedSelection::Dual;
                }
                let yes_reject = Self::seed_geometry_reject(yes);
                let no_reject = Self::seed_geometry_reject(no);
                match (yes_reject, no_reject) {
                    (true, true) => return FlatSeedSelection::None,
                    (false, true) => return FlatSeedSelection::YesOnly,
                    (true, false) => return FlatSeedSelection::NoOnly,
                    (false, false) => {}
                }
                if profile == PgtShadowProfile::XuanLadderV1 {
                    let yes_unsafe_dual = Self::xuan_unsafe_dual_first_leg(yes);
                    let no_unsafe_dual = Self::xuan_unsafe_dual_first_leg(no);
                    match (yes_unsafe_dual, no_unsafe_dual) {
                        (true, false) => return FlatSeedSelection::NoOnly,
                        (false, true) => return FlatSeedSelection::YesOnly,
                        (true, true) => {
                            return if yes.intent.price <= no.intent.price {
                                FlatSeedSelection::YesOnly
                            } else {
                                FlatSeedSelection::NoOnly
                            };
                        }
                        (false, false) => {}
                    }
                    if Self::xuan_expensive_seed_dominated_by_cheap_seed(yes, no) {
                        return FlatSeedSelection::NoOnly;
                    }
                    if Self::xuan_expensive_seed_dominated_by_cheap_seed(no, yes) {
                        return FlatSeedSelection::YesOnly;
                    }
                    let size_mismatch = yes.size
                        > no.size * (1.0 + XUAN_LADDER_DUAL_SEED_SIZE_TOLERANCE)
                        || no.size > yes.size * (1.0 + XUAN_LADDER_DUAL_SEED_SIZE_TOLERANCE);
                    if size_mismatch {
                        return if yes.intent.price < no.intent.price - 1e-9 {
                            FlatSeedSelection::YesOnly
                        } else if no.intent.price < yes.intent.price - 1e-9 {
                            FlatSeedSelection::NoOnly
                        } else if yes.size <= no.size {
                            FlatSeedSelection::YesOnly
                        } else {
                            FlatSeedSelection::NoOnly
                        };
                    }
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

    fn xuan_expensive_seed_dominated_by_cheap_seed(expensive: &SeedPlan, cheap: &SeedPlan) -> bool {
        if expensive.intent.price <= EXPENSIVE_SEED_PRICE + 1e-9 {
            return false;
        }
        if cheap.intent.price > EXPENSIVE_SEED_PRICE + 1e-9 {
            return false;
        }
        expensive.visible_completion_slack_ticks
            < cheap.visible_completion_slack_ticks + XUAN_LADDER_EXPENSIVE_SEED_DOMINANCE_TICKS
                - 1e-9
    }

    fn xuan_unsafe_dual_first_leg(seed: &SeedPlan) -> bool {
        !seed.visible_taker_completion_ok
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
            pgt_maybe_log_completion_none_diag(
                coordinator.cfg().dry_run,
                pgt_tuning(),
                first_side,
                hedge_side,
                "invalid_active_or_book",
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                pgt_active_tranche_age_secs(active),
                best_bid,
                best_ask,
                0.0,
                0.0,
                0.0,
                0.0,
            );
            return None;
        }

        let completion_age_secs = pgt_active_tranche_age_secs(active);
        let tuning = pgt_tuning();
        let repair_budget_per_share = pgt_effective_repair_budget_per_share(
            tuning,
            input.pair_ledger.repair_budget_available,
            active.residual_qty,
            remaining_secs,
            completion_age_secs,
        );
        let urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true)
            * pgt_completion_urgency_mult(active.first_vwap)
            + pgt_completion_urgency_bonus(active.first_vwap, remaining_secs, completion_age_secs);
        let (early_pair_cap, late_pair_cap, taker_close_pair_cap) =
            pgt_effective_completion_pair_caps(tuning, remaining_secs, completion_age_secs);
        let positive_edge_ceiling = early_pair_cap - active.first_vwap + repair_budget_per_share;
        let urgency_ceiling = positive_edge_ceiling + urgency_shadow;
        // Urgency can spend remaining edge, but only realized repair budget may
        // cross the profile's late pair-cost cap.
        let funded_loss_ceiling = late_pair_cap - active.first_vwap + repair_budget_per_share;
        let base_taker_close_ceiling =
            taker_close_pair_cap - active.first_vwap + repair_budget_per_share;
        let tail_insurance_ceiling =
            pgt_tail_insurance_completion_ceiling(tuning, active.first_vwap, remaining_secs);
        let taker_insurance_ceiling = pgt_xuan_ladder_taker_insurance_completion_ceiling(
            tuning,
            active.first_vwap,
            remaining_secs,
            completion_age_secs,
        );
        let timeout_insurance_ceiling = pgt_xuan_ladder_timeout_insurance_completion_ceiling(
            tuning,
            active.first_vwap,
            remaining_secs,
            completion_age_secs,
        );
        let taker_close_ceiling = base_taker_close_ceiling
            .max(tail_insurance_ceiling.unwrap_or(0.0))
            .max(timeout_insurance_ceiling.unwrap_or(0.0))
            .max(taker_insurance_ceiling.unwrap_or(0.0));
        let breakeven_unlocked = completion_age_secs >= PROFIT_FIRST_BREAKEVEN_UNLOCK_AGE_SECS
            || remaining_secs <= PROFIT_FIRST_BREAKEVEN_UNLOCK_REMAINING_SECS;
        let ceiling = if breakeven_unlocked {
            urgency_ceiling.min(funded_loss_ceiling)
        } else {
            positive_edge_ceiling.min(funded_loss_ceiling)
        };
        let passive_ceiling =
            pgt_effective_completion_passive_ceiling(ceiling, tail_insurance_ceiling);
        if passive_ceiling <= 0.0 {
            pgt_maybe_log_completion_none_diag(
                coordinator.cfg().dry_run,
                tuning,
                first_side,
                hedge_side,
                "passive_ceiling_non_positive",
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                completion_age_secs,
                best_bid,
                best_ask,
                positive_edge_ceiling,
                funded_loss_ceiling,
                taker_close_ceiling,
                passive_ceiling,
            );
            return None;
        }

        let Some(price) = self.passive_completion_price(
            coordinator,
            hedge_side,
            passive_ceiling,
            best_bid,
            best_ask,
            remaining_secs,
            active.first_vwap,
            completion_age_secs,
        ) else {
            pgt_maybe_log_completion_none_diag(
                coordinator.cfg().dry_run,
                tuning,
                first_side,
                hedge_side,
                "no_passive_price",
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                completion_age_secs,
                best_bid,
                best_ask,
                positive_edge_ceiling,
                funded_loss_ceiling,
                taker_close_ceiling,
                passive_ceiling,
            );
            return None;
        };
        if !pgt_completion_price_allowed(price, ceiling, tail_insurance_ceiling) {
            pgt_maybe_log_completion_none_diag(
                coordinator.cfg().dry_run,
                tuning,
                first_side,
                hedge_side,
                "completion_price_not_allowed",
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                completion_age_secs,
                best_bid,
                best_ask,
                positive_edge_ceiling,
                funded_loss_ceiling,
                taker_close_ceiling,
                passive_ceiling,
            );
            return None;
        }

        let mut raw_size = if remaining_secs <= COMPLETION_FULL_RESIDUAL_REMAINING_SECS {
            active.residual_qty.max(0.0)
        } else {
            self.adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
                .min(active.residual_qty.max(0.0))
        };
        if pgt_is_xuan_cycle_merge_profile(tuning) {
            raw_size = raw_size.min(pgt_xuan_cycle_available_buy_qty(tuning, input.inv, price));
        }
        let size = raw_size.min(active.residual_qty.max(0.0));
        let size = quantize_tenth(size);
        if size < pgt_profile_min_seed_qty(tuning).max(coordinator.cfg().min_order_size) {
            pgt_maybe_log_completion_none_diag(
                coordinator.cfg().dry_run,
                tuning,
                first_side,
                hedge_side,
                "completion_size_non_positive",
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                completion_age_secs,
                best_bid,
                best_ask,
                positive_edge_ceiling,
                funded_loss_ceiling,
                taker_close_ceiling,
                passive_ceiling,
            );
            return None;
        }

        let profit_taker_would_close =
            best_ask <= positive_edge_ceiling.min(taker_close_ceiling) + 1e-9;
        let breakeven_taker_would_close =
            breakeven_unlocked && best_ask <= funded_loss_ceiling.min(taker_close_ceiling) + 1e-9;
        let tail_insurance_taker_would_close = tail_insurance_ceiling
            .map(|ceiling| best_ask <= ceiling + 1e-9)
            .unwrap_or(false);
        let timeout_insurance_would_close = timeout_insurance_ceiling
            .map(|ceiling| best_ask <= ceiling + 1e-9)
            .unwrap_or(false);
        let taker_insurance_would_close = taker_insurance_ceiling
            .map(|ceiling| best_ask <= ceiling + 1e-9)
            .unwrap_or(false);
        let last_chance_forced_taker_close = pgt_xuan_ladder_last_chance_taker_close(
            tuning,
            active.first_vwap,
            remaining_secs,
            completion_age_secs,
            best_ask,
        );
        let taker_shadow_would_close = coordinator.cfg().dry_run
            && remaining_secs > coordinator.cfg().endgame_freeze_secs
            && (profit_taker_would_close
                || breakeven_taker_would_close
                || tail_insurance_taker_would_close
                || timeout_insurance_would_close
                || taker_insurance_would_close
                || last_chance_forced_taker_close);
        let taker_close_limit = if taker_shadow_would_close {
            Some(coordinator.safe_price(best_ask))
        } else {
            None
        };
        if !taker_shadow_would_close {
            pgt_maybe_log_tail_completion_diag(
                coordinator.cfg().dry_run,
                tuning,
                first_side,
                hedge_side,
                active.first_vwap,
                active.residual_qty,
                remaining_secs,
                completion_age_secs,
                best_bid,
                best_ask,
                price,
                positive_edge_ceiling,
                funded_loss_ceiling,
                base_taker_close_ceiling,
                tail_insurance_ceiling,
                taker_insurance_ceiling,
                taker_close_ceiling,
                passive_ceiling,
                profit_taker_would_close,
                breakeven_taker_would_close,
                tail_insurance_taker_would_close,
                taker_insurance_would_close,
            );
        }

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
        if pgt_is_xuan_cycle_merge_profile(pgt_tuning()) {
            return self.xuan_cycle_same_side_add_intent(
                coordinator,
                input,
                active,
                remaining_secs,
            );
        }
        if remaining_secs <= TAIL_COMPLETION_ONLY_SECS {
            return None;
        }
        if remaining_secs <= pgt_tuning().price_aware_no_new_open_secs {
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

    fn xuan_cycle_same_side_add_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
        remaining_secs: u64,
    ) -> Option<StrategyIntent> {
        if remaining_secs <= pgt_tuning().hard_no_new_open_secs {
            return None;
        }
        let side = active.first_side?;
        let (best_bid, best_ask) = match side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let price = self.passive_seed_price(
            coordinator,
            side,
            XUAN_CYCLE_MERGE_SEED_MAX_PRICE,
            best_bid,
            best_ask,
        )?;
        if price <= 0.0 || price > XUAN_CYCLE_MERGE_SEED_MAX_PRICE + 1e-9 {
            return None;
        }
        let min_qty = XUAN_CYCLE_MERGE_MIN_FILL_QTY.max(coordinator.cfg().min_order_size);
        let size = self
            .adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
            .min(pgt_xuan_cycle_available_buy_qty(
                pgt_tuning(),
                input.inv,
                price,
            ));
        let size = quantize_tenth(size);
        if size + 1e-9 < min_qty {
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
        if tuning.clip_profile == PgtClipProfile::XuanLadderV1 {
            return pgt_xuan_ladder_clip_qty(remaining_secs)
                .clamp(tuning.min_clip_qty, tuning.max_clip_qty);
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
    if let Some(min_remaining) = tuning.seed_open_min_remaining_secs {
        if remaining_secs < min_remaining {
            return false;
        }
    }
    true
}

fn pgt_seed_open_window_allowed(seed: &SeedPlan, remaining_secs: u64) -> bool {
    let tuning = pgt_tuning();
    if !pgt_profile_seed_open_remaining_allowed(remaining_secs) {
        return false;
    }
    if remaining_secs > tuning.price_aware_no_new_open_secs {
        return true;
    }
    if remaining_secs <= tuning.hard_no_new_open_secs {
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
    if !dry_run || remaining_secs <= pgt_tuning().price_aware_no_new_open_secs {
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

fn pgt_round_elapsed_secs(remaining_secs: u64) -> Option<u64> {
    if remaining_secs == u64::MAX || remaining_secs > XUAN_LADDER_ROUND_SECS {
        None
    } else {
        Some(XUAN_LADDER_ROUND_SECS - remaining_secs)
    }
}

fn pgt_xuan_ladder_clip_qty(remaining_secs: u64) -> f64 {
    let Some(elapsed) = pgt_round_elapsed_secs(remaining_secs) else {
        return 0.0;
    };

    match elapsed {
        0..=44 => 120.0,
        45..=119 => 160.0,
        120..=209 => 210.0,
        210..=259 => 135.0,
        _ => 80.0,
    }
}

fn pgt_effective_completion_pair_caps(
    tuning: PgtTuning,
    remaining_secs: u64,
    completion_age_secs: f64,
) -> (f64, f64, f64) {
    let default_early = tuning.completion_early_pair_cap.clamp(0.0, 1.20);
    let default_late = tuning
        .completion_late_pair_cap
        .max(default_early)
        .clamp(0.0, 1.20);
    let default_taker = tuning
        .taker_close_pair_cap
        .min(default_late)
        .clamp(0.0, 1.20);

    if tuning.profile == PgtShadowProfile::XuanTailTakerV1 {
        let cap = if completion_age_secs < XUAN_TAIL_TAKER_COMPLETION_FRESH_AGE_SECS {
            XUAN_TAIL_TAKER_COMPLETION_FRESH_PAIR_CAP
        } else if completion_age_secs < XUAN_TAIL_TAKER_COMPLETION_WARM_AGE_SECS {
            XUAN_TAIL_TAKER_COMPLETION_WARM_PAIR_CAP
        } else {
            XUAN_TAIL_TAKER_COMPLETION_RESCUE_PAIR_CAP
        };
        let cap = cap.clamp(0.0, 1.20);
        return (cap, cap, cap);
    }

    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return (default_early, default_late, default_taker);
    }

    let age_pair_cap = if completion_age_secs < XUAN_LADDER_COMPLETION_FRESH_AGE_SECS {
        XUAN_LADDER_COMPLETION_FRESH_PAIR_CAP
    } else if completion_age_secs < XUAN_LADDER_COMPLETION_WARM_AGE_SECS {
        XUAN_LADDER_COMPLETION_WARM_PAIR_CAP
    } else if completion_age_secs < XUAN_LADDER_COMPLETION_STALE_AGE_SECS {
        XUAN_LADDER_COMPLETION_STALE_PAIR_CAP
    } else {
        XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP
    };

    let tail_cap =
        if remaining_secs <= 45 && completion_age_secs >= XUAN_LADDER_COMPLETION_STALE_AGE_SECS {
            XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP
        } else if remaining_secs <= 45 {
            XUAN_LADDER_COMPLETION_MATURE_PAIR_CAP
        } else {
            age_pair_cap
        };
    let early = age_pair_cap.min(default_early).clamp(0.0, 1.20);
    let late = tail_cap.max(early).min(default_late).clamp(0.0, 1.20);
    let taker =
        if remaining_secs <= 45 || completion_age_secs >= XUAN_LADDER_COMPLETION_STALE_AGE_SECS {
            default_taker.min(late)
        } else {
            default_taker.min(early).min(late)
        }
        .clamp(0.0, 1.20);
    (early, late, taker)
}

fn pgt_tail_insurance_completion_ceiling(
    tuning: PgtTuning,
    first_vwap: f64,
    remaining_secs: u64,
) -> Option<f64> {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return None;
    }
    if remaining_secs == u64::MAX || remaining_secs > XUAN_LADDER_TAIL_INSURANCE_REMAINING_SECS {
        return None;
    }
    if first_vwap <= 0.0 {
        return None;
    }
    let pair_cap = if remaining_secs <= XUAN_LADDER_LAST_CHANCE_INSURANCE_REMAINING_SECS {
        XUAN_LADDER_LAST_CHANCE_INSURANCE_PAIR_CAP
    } else {
        XUAN_LADDER_TAIL_INSURANCE_PAIR_CAP
    };
    let ceiling = (pair_cap - first_vwap).clamp(0.0, 1.0);
    if ceiling > 0.0 {
        Some(ceiling)
    } else {
        None
    }
}

fn pgt_xuan_ladder_taker_insurance_completion_ceiling(
    tuning: PgtTuning,
    first_vwap: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
) -> Option<f64> {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return None;
    }
    if remaining_secs == u64::MAX || completion_age_secs < XUAN_LADDER_TAKER_INSURANCE_MIN_AGE_SECS
    {
        return None;
    }
    if first_vwap <= 0.0 {
        return None;
    }
    let ceiling = (XUAN_LADDER_TAKER_INSURANCE_PAIR_CAP - first_vwap).clamp(0.0, 1.0);
    if ceiling > 0.0 {
        Some(ceiling)
    } else {
        None
    }
}

fn pgt_xuan_ladder_timeout_insurance_completion_ceiling(
    tuning: PgtTuning,
    first_vwap: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
) -> Option<f64> {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return None;
    }
    let min_age_secs = pgt_xuan_ladder_timeout_insurance_min_age_secs(first_vwap);
    if remaining_secs == u64::MAX || completion_age_secs < min_age_secs {
        return None;
    }
    if first_vwap <= 0.0 {
        return None;
    }

    let pair_cap = if first_vwap <= XUAN_LADDER_TIMEOUT_CHEAP_FIRST_MAX_PRICE + 1e-9 {
        XUAN_LADDER_TIMEOUT_CHEAP_PAIR_CAP
    } else if first_vwap <= XUAN_LADDER_TIMEOUT_MID_FIRST_MAX_PRICE + 1e-9 {
        XUAN_LADDER_TIMEOUT_MID_PAIR_CAP
    } else {
        XUAN_LADDER_TIMEOUT_HIGH_PAIR_CAP
    };
    let ceiling = (pair_cap - first_vwap).clamp(0.0, 1.0);
    if ceiling > 0.0 {
        Some(ceiling)
    } else {
        None
    }
}

fn pgt_xuan_ladder_timeout_insurance_min_age_secs(first_vwap: f64) -> f64 {
    if first_vwap > XUAN_LADDER_TIMEOUT_FAST_FIRST_MIN_PRICE + 1e-9 {
        XUAN_LADDER_TIMEOUT_MARGINAL_INSURANCE_MIN_AGE_SECS
    } else {
        XUAN_LADDER_TIMEOUT_INSURANCE_MIN_AGE_SECS
    }
}

fn pgt_tail_taker_seed_price_blocks(tuning: PgtTuning, best_ask: f64) -> bool {
    tuning.profile == PgtShadowProfile::XuanTailTakerV1
        && (best_ask < XUAN_TAIL_TAKER_FIRST_MIN_ASK - 1e-9
            || best_ask > XUAN_TAIL_TAKER_FIRST_MAX_ASK + 1e-9)
}

fn pgt_is_xuan_cycle_merge_profile(tuning: PgtTuning) -> bool {
    tuning.profile == PgtShadowProfile::XuanCycleMergeV1
}

fn pgt_profile_min_seed_qty(tuning: PgtTuning) -> f64 {
    if pgt_is_xuan_cycle_merge_profile(tuning) {
        XUAN_CYCLE_MERGE_MIN_FILL_QTY
    } else {
        0.0
    }
}

fn pgt_xuan_cycle_inventory_cost(inv: &InventoryState) -> f64 {
    inv.yes_qty.max(0.0) * inv.yes_avg_cost.max(0.0)
        + inv.no_qty.max(0.0) * inv.no_avg_cost.max(0.0)
}

fn pgt_xuan_cycle_available_buy_qty(tuning: PgtTuning, inv: &InventoryState, price: f64) -> f64 {
    if !pgt_is_xuan_cycle_merge_profile(tuning) {
        return f64::INFINITY;
    }
    if price <= 0.0 {
        return 0.0;
    }
    let remaining_budget =
        (XUAN_CYCLE_MERGE_MAX_INVENTORY_COST - pgt_xuan_cycle_inventory_cost(inv)).max(0.0);
    remaining_budget / price
}

fn pgt_xuan_ladder_seed_visible_completion_guard_blocks(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    seed_price: f64,
    opposite_ask: f64,
    tick: f64,
) -> bool {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return false;
    }
    if round_buy_fill_count > 0 {
        return false;
    }
    if seed_price <= 0.0 || opposite_ask <= 0.0 || tick <= 0.0 {
        return true;
    }
    let taker_pair_cap = pgt_xuan_ladder_seed_taker_completion_pair_cap(seed_price);
    let taker_pair_cost = seed_price + opposite_ask;
    if taker_pair_cost <= taker_pair_cap + 1e-9 {
        return false;
    }
    let maker_completion_ref = (opposite_ask - tick).max(0.0);
    let maker_pair_cost = seed_price + maker_completion_ref;
    maker_pair_cost > XUAN_LADDER_SEED_MAKER_COMPLETION_PAIR_CAP + 1e-9
}

fn pgt_xuan_ladder_seed_taker_completion_pair_cap(seed_price: f64) -> f64 {
    if seed_price > 0.0 && seed_price < XUAN_LADDER_LOW_FIRST_SEED_PRICE_HI {
        XUAN_LADDER_LOW_FIRST_SEED_TAKER_COMPLETION_PAIR_CAP
    } else {
        XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP
    }
}

fn pgt_xuan_ladder_maker_only_seed_clip_caps(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    visible_taker_completion_ok: bool,
) -> bool {
    tuning.profile == PgtShadowProfile::XuanLadderV1
        && round_buy_fill_count == 0
        && !visible_taker_completion_ok
}

fn pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    seed_price: f64,
    opposite_ask: f64,
) -> bool {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 || round_buy_fill_count > 0 {
        return false;
    }
    if seed_price <= 0.0 || seed_price >= XUAN_LADDER_LOW_FIRST_SEED_PRICE_HI || opposite_ask <= 0.0
    {
        return false;
    }
    let pair_cost = seed_price + opposite_ask;
    pair_cost > XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP + 1e-9
        && pair_cost <= XUAN_LADDER_LOW_FIRST_SEED_TAKER_COMPLETION_PAIR_CAP + 1e-9
}

fn pgt_xuan_ladder_reopen_seed_clip_caps(tuning: PgtTuning, round_buy_fill_count: u64) -> bool {
    tuning.profile == PgtShadowProfile::XuanLadderV1
        && round_buy_fill_count >= XUAN_LADDER_REOPEN_AFTER_CLOSED_MIN_BUY_FILLS
        && round_buy_fill_count <= XUAN_LADDER_REOPEN_AFTER_CLOSED_MAX_BUY_FILLS
}

fn pgt_xuan_ladder_reopen_seed_quality_blocks(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    recent_pair_cost: Option<f64>,
    seed_price: f64,
    opposite_ask: f64,
    tick: f64,
) -> bool {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return false;
    }
    if round_buy_fill_count < XUAN_LADDER_REOPEN_AFTER_CLOSED_MIN_BUY_FILLS {
        return false;
    }
    if round_buy_fill_count > XUAN_LADDER_REOPEN_AFTER_CLOSED_MAX_BUY_FILLS {
        return true;
    }
    if !recent_pair_cost
        .map(|cost| cost <= XUAN_LADDER_REOPEN_AFTER_CLOSED_PAIR_COST + 1e-9)
        .unwrap_or(false)
    {
        return true;
    }
    if seed_price <= 0.0 || opposite_ask <= 0.0 || tick <= 0.0 {
        return true;
    }
    let taker_pair_cost = seed_price + opposite_ask;
    if taker_pair_cost <= XUAN_LADDER_REOPEN_PROJECTED_PAIR_CAP + 1e-9 {
        return false;
    }
    let maker_completion_ref = (opposite_ask - tick).max(0.0);
    let maker_pair_cost = seed_price + maker_completion_ref;
    maker_pair_cost > XUAN_LADDER_REOPEN_PROJECTED_PAIR_CAP + 1e-9
}

fn pgt_xuan_ladder_last_chance_taker_close(
    tuning: PgtTuning,
    first_vwap: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
    best_ask: f64,
) -> bool {
    tuning.profile == PgtShadowProfile::XuanLadderV1
        && first_vwap > 0.0
        && remaining_secs <= XUAN_LADDER_LAST_CHANCE_CLOSE_REMAINING_SECS
        && completion_age_secs >= XUAN_LADDER_LAST_CHANCE_CLOSE_MIN_AGE_SECS
        && best_ask > 0.0
        && best_ask <= XUAN_LADDER_LAST_CHANCE_CLOSE_MAX_ASK + 1e-9
        && first_vwap + best_ask <= XUAN_LADDER_LAST_CHANCE_INSURANCE_PAIR_CAP + 1e-9
}

#[allow(clippy::too_many_arguments)]
fn pgt_maybe_log_seed_admission_diag(
    dry_run: bool,
    tuning: PgtTuning,
    side: Side,
    remaining_secs: u64,
    decision: &'static str,
    price: f64,
    size: f64,
    best_bid: f64,
    best_ask: f64,
    opposite_ask: f64,
    open_pair_band: f64,
    tick: f64,
    taker_shadow_would_open: bool,
    entry_pressure_extra_ticks: u8,
    visible_completion_slack_ticks: f64,
    visible_breakeven_completion_slack_ticks: f64,
    fill_distance_ticks: f64,
    min_visible_breakeven_slack_ticks: f64,
) {
    if !dry_run || tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return;
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = PGT_LAST_SEED_DIAG_UNIX_SECS.load(Ordering::Relaxed);
    if now_secs.saturating_sub(last) < XUAN_LADDER_TAIL_DIAG_INTERVAL_SECS {
        return;
    }
    if PGT_LAST_SEED_DIAG_UNIX_SECS
        .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let visible_ask_pair_cost = price + opposite_ask;
    let maker_completion_ref = (opposite_ask - tick.max(0.0)).max(0.0);
    let visible_maker_pair_cost = price + maker_completion_ref;
    let taker_pair_cap = pgt_xuan_ladder_seed_taker_completion_pair_cap(price);
    let guard_pair_cost_gap = (visible_ask_pair_cost - taker_pair_cap).max(0.0);
    info!(
        "🧭 PGT seed admission diag | decision={} side={:?} price={:.4} size={:.1} remaining_secs={} best_bid={:.4} best_ask={:.4} opposite_ask={:.4} visible_ask_pair_cost={:.4} visible_maker_pair_cost={:.4} open_pair_band={:.4} visible_completion_slack_ticks={:.2} visible_breakeven_slack_ticks={:.2} min_breakeven_slack_ticks={:.2} fill_distance_ticks={:.2} taker_shadow_would_open={} entry_pressure_extra_ticks={} xuan_taker_pair_cap={:.4} xuan_maker_pair_cap={:.4} guard_pair_cost_gap={:.4}",
        decision,
        side,
        price,
        size,
        remaining_secs,
        best_bid,
        best_ask,
        opposite_ask,
        visible_ask_pair_cost,
        visible_maker_pair_cost,
        open_pair_band,
        visible_completion_slack_ticks,
        visible_breakeven_completion_slack_ticks,
        min_visible_breakeven_slack_ticks,
        fill_distance_ticks,
        taker_shadow_would_open,
        entry_pressure_extra_ticks,
        taker_pair_cap,
        XUAN_LADDER_SEED_MAKER_COMPLETION_PAIR_CAP,
        guard_pair_cost_gap,
    );
}

#[allow(clippy::too_many_arguments)]
fn pgt_maybe_log_completion_none_diag(
    dry_run: bool,
    tuning: PgtTuning,
    first_side: Side,
    hedge_side: Side,
    reason: &'static str,
    first_vwap: f64,
    residual_qty: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
    best_bid: f64,
    best_ask: f64,
    positive_edge_ceiling: f64,
    funded_loss_ceiling: f64,
    taker_close_ceiling: f64,
    passive_ceiling: f64,
) {
    if !dry_run || tuning.profile != PgtShadowProfile::XuanLadderV1 || residual_qty <= f64::EPSILON
    {
        return;
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = PGT_LAST_COMPLETION_NONE_DIAG_UNIX_SECS.load(Ordering::Relaxed);
    if now_secs.saturating_sub(last) < XUAN_LADDER_TAIL_DIAG_INTERVAL_SECS {
        return;
    }
    if PGT_LAST_COMPLETION_NONE_DIAG_UNIX_SECS
        .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let ask_pair_cost = if first_vwap > 0.0 && best_ask > 0.0 {
        first_vwap + best_ask
    } else {
        0.0
    };
    info!(
        "🧭 PGT completion none diag | reason={} first_side={:?} hedge_side={:?} first_vwap={:.4} residual={:.2} remaining_secs={} completion_age_secs={:.1} best_bid={:.4} best_ask={:.4} ask_pair_cost={:.4} positive_ceiling={:.4} funded_ceiling={:.4} taker_ceiling={:.4} passive_ceiling={:.4}",
        reason,
        first_side,
        hedge_side,
        first_vwap,
        residual_qty,
        remaining_secs,
        completion_age_secs,
        best_bid,
        best_ask,
        ask_pair_cost,
        positive_edge_ceiling,
        funded_loss_ceiling,
        taker_close_ceiling,
        passive_ceiling,
    );
}

#[allow(clippy::too_many_arguments)]
fn pgt_maybe_log_tail_completion_diag(
    dry_run: bool,
    tuning: PgtTuning,
    first_side: Side,
    hedge_side: Side,
    first_vwap: f64,
    residual_qty: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
    best_bid: f64,
    best_ask: f64,
    passive_price: f64,
    positive_edge_ceiling: f64,
    funded_loss_ceiling: f64,
    base_taker_close_ceiling: f64,
    tail_insurance_ceiling: Option<f64>,
    taker_insurance_ceiling: Option<f64>,
    taker_close_ceiling: f64,
    passive_ceiling: f64,
    profit_taker_would_close: bool,
    breakeven_taker_would_close: bool,
    tail_insurance_taker_would_close: bool,
    taker_insurance_would_close: bool,
) {
    if !dry_run
        || tuning.profile != PgtShadowProfile::XuanLadderV1
        || remaining_secs > XUAN_LADDER_TAIL_DIAG_REMAINING_SECS
        || best_ask <= 0.0
    {
        return;
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = PGT_LAST_TAIL_DIAG_UNIX_SECS.load(Ordering::Relaxed);
    if now_secs.saturating_sub(last) < XUAN_LADDER_TAIL_DIAG_INTERVAL_SECS {
        return;
    }
    if PGT_LAST_TAIL_DIAG_UNIX_SECS
        .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let tail_ceiling = tail_insurance_ceiling.unwrap_or(0.0);
    let insurance_ceiling = taker_insurance_ceiling.unwrap_or(0.0);
    let ask_gap_to_taker = (best_ask - taker_close_ceiling).max(0.0);
    let passive_pair_cost = first_vwap + passive_price;
    let ask_pair_cost = first_vwap + best_ask;
    info!(
        "🧭 PGT tail no-taker diag | first_side={:?} hedge_side={:?} first_vwap={:.4} residual={:.2} remaining_secs={} completion_age_secs={:.1} best_bid={:.4} best_ask={:.4} passive_price={:.4} passive_pair_cost={:.4} ask_pair_cost={:.4} positive_ceiling={:.4} funded_ceiling={:.4} base_taker_ceiling={:.4} tail_ceiling={:.4} insurance_ceiling={:.4} taker_ceiling={:.4} passive_ceiling={:.4} ask_gap_to_taker={:.4} close_flags(profit/breakeven/tail/insurance)={}/{}/{}/{}",
        first_side,
        hedge_side,
        first_vwap,
        residual_qty,
        remaining_secs,
        completion_age_secs,
        best_bid,
        best_ask,
        passive_price,
        passive_pair_cost,
        ask_pair_cost,
        positive_edge_ceiling,
        funded_loss_ceiling,
        base_taker_close_ceiling,
        tail_ceiling,
        insurance_ceiling,
        taker_close_ceiling,
        passive_ceiling,
        ask_gap_to_taker,
        profit_taker_would_close,
        breakeven_taker_would_close,
        tail_insurance_taker_would_close,
        taker_insurance_would_close,
    );
}

fn pgt_effective_completion_passive_ceiling(
    base_ceiling: f64,
    tail_insurance_ceiling: Option<f64>,
) -> f64 {
    base_ceiling.max(tail_insurance_ceiling.unwrap_or(0.0))
}

fn pgt_completion_price_allowed(
    price: f64,
    base_ceiling: f64,
    tail_insurance_ceiling: Option<f64>,
) -> bool {
    price > 0.0
        && price
            <= pgt_effective_completion_passive_ceiling(base_ceiling, tail_insurance_ceiling) + 1e-9
}

fn pgt_effective_repair_budget_per_share(
    tuning: PgtTuning,
    repair_budget_available: f64,
    residual_qty: f64,
    remaining_secs: u64,
    completion_age_secs: f64,
) -> f64 {
    if repair_budget_available <= 0.0 || residual_qty <= 0.0 {
        return 0.0;
    }
    let per_share = repair_budget_available / residual_qty.max(1.0);
    if tuning.profile == PgtShadowProfile::XuanLadderV1 {
        let repair_budget_unlocked = completion_age_secs >= XUAN_LADDER_REPAIR_BUDGET_MIN_AGE_SECS
            || remaining_secs <= XUAN_LADDER_REPAIR_BUDGET_MAX_REMAINING_SECS;
        if !repair_budget_unlocked {
            return 0.0;
        }
        let base_pair_cap = if completion_age_secs >= XUAN_LADDER_COMPLETION_STALE_AGE_SECS {
            XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP
        } else {
            XUAN_LADDER_COMPLETION_MATURE_PAIR_CAP
        };
        let max_extra = (XUAN_LADDER_FUNDED_REPAIR_PAIR_CAP - base_pair_cap).max(0.0);
        per_share.min(max_extra)
    } else {
        per_share
    }
}

fn pgt_recent_closed_pair_cost(pair_ledger: &PairLedgerSnapshot) -> Option<f64> {
    let (notional, qty) = pair_ledger
        .recent_closed
        .iter()
        .flatten()
        .filter(|tranche| tranche.pairable_qty > f64::EPSILON)
        .fold((0.0, 0.0), |(notional, qty), tranche| {
            (
                notional + tranche.pairable_qty * tranche.pair_cost_tranche,
                qty + tranche.pairable_qty,
            )
        });
    if qty > f64::EPSILON {
        Some(notional / qty)
    } else {
        None
    }
}

fn pgt_allow_reopen_after_rescue_close(
    tuning: PgtTuning,
    input: StrategyTickInput<'_>,
    remaining_secs: u64,
    reopen_attempted_for_fill_count: bool,
) -> bool {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return false;
    }
    if reopen_attempted_for_fill_count {
        return false;
    }
    if remaining_secs < XUAN_LADDER_REOPEN_AFTER_RESCUE_MIN_REMAINING_SECS {
        return false;
    }
    if input.inv.net_diff.abs() > PAIR_ARB_NET_EPS {
        return false;
    }
    if input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
        return false;
    }
    if input.episode_metrics.round_buy_fill_count > XUAN_LADDER_REOPEN_AFTER_RESCUE_MAX_BUY_FILLS {
        return false;
    }
    pgt_recent_closed_pair_cost(input.pair_ledger)
        .map(|cost| cost <= XUAN_LADDER_REOPEN_AFTER_RESCUE_PAIR_COST + 1e-9)
        .unwrap_or(false)
}

fn pgt_blocks_reopen_after_closed_pair(
    tuning: PgtTuning,
    input: StrategyTickInput<'_>,
    reopen_attempted_for_fill_count: bool,
) -> bool {
    if tuning.profile == PgtShadowProfile::XuanTailTakerV1 {
        return reopen_attempted_for_fill_count || input.episode_metrics.round_buy_fill_count >= 2;
    }
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return false;
    }
    if reopen_attempted_for_fill_count {
        return true;
    }
    if input.episode_metrics.round_buy_fill_count < XUAN_LADDER_REOPEN_AFTER_CLOSED_MIN_BUY_FILLS {
        return false;
    }
    if input.episode_metrics.round_buy_fill_count > XUAN_LADDER_REOPEN_AFTER_CLOSED_MAX_BUY_FILLS {
        return true;
    }
    if input.inv.net_diff.abs() > PAIR_ARB_NET_EPS {
        return false;
    }
    if input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
        return false;
    }
    pgt_recent_closed_pair_cost(input.pair_ledger)
        .map(|cost| cost > XUAN_LADDER_REOPEN_AFTER_CLOSED_PAIR_COST + 1e-9)
        .unwrap_or(false)
}

fn pgt_seed_min_visible_breakeven_slack_ticks(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    repair_budget_available: f64,
    recent_pair_cost: Option<f64>,
) -> f64 {
    if tuning.profile != PgtShadowProfile::XuanLadderV1 {
        return tuning.seed_min_visible_breakeven_slack_ticks;
    }

    let cost_brake_active = round_buy_fill_count >= XUAN_LADDER_COST_BRAKE_MIN_BUY_FILLS
        && repair_budget_available <= f64::EPSILON
        && recent_pair_cost
            .map(|cost| cost >= XUAN_LADDER_COST_BRAKE_PAIR_COST - 1e-9)
            .unwrap_or(false);
    if cost_brake_active {
        XUAN_LADDER_COST_BRAKE_MIN_SLACK_TICKS
    } else {
        tuning.seed_min_visible_breakeven_slack_ticks
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
    use crate::polymarket::coordinator::{Book, StrategyInventoryMetrics};
    use crate::polymarket::messages::{InventorySnapshot, InventoryState};
    use crate::polymarket::pair_ledger::EpisodeMetrics;

    #[test]
    fn replay_focused_profile_matches_replay_search_candidate() {
        let tuning = PgtTuning::replay_focused_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::ReplayFocusedV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(225));
        assert_eq!(tuning.open_pair_band(0.99), 0.980);
        assert_eq!(tuning.completion_early_pair_cap, 0.975);
        assert_eq!(tuning.completion_late_pair_cap, 0.995);
        assert_eq!(tuning.taker_close_pair_cap, 0.995);
        assert_eq!(tuning.fixed_clip_qty, Some(57.6));
        assert!(tuning.preserve_seed_clip_qty);
    }

    #[test]
    fn replay_lower_clip_profile_matches_risk_control_candidate() {
        let tuning = PgtTuning::replay_lower_clip_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::ReplayLowerClipV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(240));
        assert_eq!(
            tuning.seed_open_min_remaining_secs,
            Some(HARD_NO_NEW_OPEN_SECS)
        );
        assert_eq!(tuning.open_pair_band(0.99), 0.970);
        assert_eq!(tuning.completion_early_pair_cap, 0.975);
        assert_eq!(tuning.completion_late_pair_cap, 1.000);
        assert_eq!(tuning.taker_close_pair_cap, 1.000);
        assert_eq!(tuning.fixed_clip_qty, Some(30.0));
        assert!(tuning.preserve_seed_clip_qty);
    }

    #[test]
    fn xuan_ladder_profile_matches_recent_public_shape() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::XuanLadderV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(296));
        assert_eq!(tuning.seed_open_min_remaining_secs, Some(25));
        assert_eq!(tuning.hard_no_new_open_secs, 25);
        assert_eq!(tuning.price_aware_no_new_open_secs, 25);
        assert_eq!(tuning.open_pair_band(0.98), XUAN_LADDER_OPEN_PAIR_CAP);
        assert_eq!(
            tuning.completion_early_pair_cap,
            XUAN_LADDER_COMPLETION_MATURE_PAIR_CAP
        );
        assert_eq!(
            tuning.completion_late_pair_cap,
            XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP
        );
        assert_eq!(
            tuning.taker_close_pair_cap,
            XUAN_LADDER_COMPLETION_RESCUE_PAIR_CAP
        );
        assert_eq!(tuning.fixed_clip_qty, None);
        assert_eq!(tuning.clip_profile, PgtClipProfile::XuanLadderV1);
        assert!(tuning.preserve_seed_clip_qty);
        assert_eq!(
            tuning.seed_min_visible_breakeven_slack_ticks,
            XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS
        );
    }

    #[test]
    fn xuan_tail_taker_profile_matches_dense_l2_candidate() {
        let tuning = PgtTuning::xuan_tail_taker_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::XuanTailTakerV1);
        assert_eq!(
            tuning.seed_open_max_remaining_secs,
            Some(XUAN_TAIL_TAKER_MAX_REMAINING_SECS)
        );
        assert_eq!(
            tuning.seed_open_min_remaining_secs,
            Some(XUAN_TAIL_TAKER_MIN_REMAINING_SECS)
        );
        assert_eq!(
            tuning.hard_no_new_open_secs,
            XUAN_TAIL_TAKER_HARD_NO_NEW_OPEN_SECS
        );
        assert_eq!(
            tuning.price_aware_no_new_open_secs,
            XUAN_TAIL_TAKER_HARD_NO_NEW_OPEN_SECS
        );
        assert_eq!(tuning.open_pair_band(0.98), XUAN_TAIL_TAKER_OPEN_PAIR_CAP);
        assert_eq!(tuning.fixed_clip_qty, Some(XUAN_TAIL_TAKER_CLIP_QTY));
        assert_eq!(tuning.min_clip_qty, XUAN_TAIL_TAKER_CLIP_QTY);
        assert_eq!(tuning.max_clip_qty, XUAN_TAIL_TAKER_CLIP_QTY);
        assert!(tuning.preserve_seed_clip_qty);
    }

    #[test]
    fn xuan_cycle_merge_profile_matches_maker_cycle_oos_candidate() {
        let tuning = PgtTuning::xuan_cycle_merge_v1();
        assert_eq!(tuning.profile, PgtShadowProfile::XuanCycleMergeV1);
        assert_eq!(tuning.seed_open_max_remaining_secs, Some(296));
        assert_eq!(tuning.seed_open_min_remaining_secs, Some(25));
        assert_eq!(tuning.hard_no_new_open_secs, 25);
        assert_eq!(tuning.price_aware_no_new_open_secs, 25);
        assert_eq!(tuning.open_pair_band(0.98), XUAN_CYCLE_MERGE_PAIR_CAP);
        assert_eq!(tuning.completion_early_pair_cap, XUAN_CYCLE_MERGE_PAIR_CAP);
        assert_eq!(tuning.completion_late_pair_cap, XUAN_CYCLE_MERGE_PAIR_CAP);
        assert_eq!(tuning.taker_close_pair_cap, XUAN_CYCLE_MERGE_PAIR_CAP);
        assert_eq!(tuning.fixed_clip_qty, Some(XUAN_CYCLE_MERGE_CLIP_QTY));
        assert_eq!(tuning.min_clip_qty, XUAN_CYCLE_MERGE_CLIP_QTY);
        assert_eq!(tuning.max_clip_qty, XUAN_CYCLE_MERGE_CLIP_QTY);
        assert!(tuning.preserve_seed_clip_qty);
        assert!(tuning.seed_min_visible_breakeven_slack_ticks < -50.0);
    }

    #[test]
    fn xuan_cycle_merge_inventory_budget_caps_seed_size() {
        let tuning = PgtTuning::xuan_cycle_merge_v1();
        let light = InventoryState {
            yes_qty: 100.0,
            yes_avg_cost: 0.49,
            ..InventoryState::default()
        };
        assert!(
            pgt_xuan_cycle_available_buy_qty(tuning, &light, XUAN_CYCLE_MERGE_SEED_MAX_PRICE)
                >= XUAN_CYCLE_MERGE_CLIP_QTY
        );

        let nearly_full = InventoryState {
            yes_qty: 100.0,
            yes_avg_cost: 0.495,
            ..InventoryState::default()
        };
        assert!(
            pgt_xuan_cycle_available_buy_qty(tuning, &nearly_full, XUAN_CYCLE_MERGE_SEED_MAX_PRICE)
                < XUAN_CYCLE_MERGE_MIN_FILL_QTY
        );
    }

    #[test]
    fn xuan_tail_taker_seed_price_gate_matches_oos_band() {
        let tuning = PgtTuning::xuan_tail_taker_v1();
        assert!(pgt_tail_taker_seed_price_blocks(tuning, 0.615));
        assert!(!pgt_tail_taker_seed_price_blocks(
            tuning,
            XUAN_TAIL_TAKER_FIRST_MIN_ASK
        ));
        assert!(!pgt_tail_taker_seed_price_blocks(tuning, 0.66));
        assert!(!pgt_tail_taker_seed_price_blocks(
            tuning,
            XUAN_TAIL_TAKER_FIRST_MAX_ASK
        ));
        assert!(pgt_tail_taker_seed_price_blocks(tuning, 0.705));
    }

    #[test]
    fn xuan_tail_taker_completion_caps_match_dense_l2_schedule() {
        let tuning = PgtTuning::xuan_tail_taker_v1();
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 55, 10.0),
            (0.900, 0.900, 0.900)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 45, 30.0),
            (0.950, 0.950, 0.950)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 35, 50.0),
            (1.000, 1.000, 1.000)
        );
    }

    #[test]
    fn xuan_ladder_clip_schedule_tracks_elapsed_offsets() {
        assert_eq!(pgt_xuan_ladder_clip_qty(296), 120.0);
        assert_eq!(pgt_xuan_ladder_clip_qty(255), 160.0);
        assert_eq!(pgt_xuan_ladder_clip_qty(180), 210.0);
        assert_eq!(pgt_xuan_ladder_clip_qty(90), 135.0);
        assert_eq!(pgt_xuan_ladder_clip_qty(25), 80.0);
        assert_eq!(pgt_xuan_ladder_clip_qty(301), 0.0);
    }

    #[test]
    fn xuan_ladder_completion_caps_stage_by_residual_age() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 280, 5.0),
            (0.990, 0.990, 0.990)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 260, 30.0),
            (0.995, 0.995, 0.995)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 180, 70.0),
            (1.000, 1.000, 1.000)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 120, 95.0),
            (1.000, 1.010, 1.010)
        );
    }

    #[test]
    fn xuan_ladder_completion_caps_do_not_unlock_unfunded_tail_repair() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 8.0),
            (0.990, 1.000, 1.000)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 119.0),
            (1.000, 1.010, 1.010)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 120.0),
            (1.000, 1.010, 1.010)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 80, 8.0),
            (0.990, 0.990, 0.990)
        );
    }

    #[test]
    fn xuan_ladder_tail_insurance_allows_bounded_residual_close_only_near_end() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_tail_insurance_completion_ceiling(tuning, 0.43, 46),
            None
        );
        assert!(
            (pgt_tail_insurance_completion_ceiling(tuning, 0.43, 45).unwrap() - 0.60).abs() < 1e-9
        );
        assert!(
            (pgt_tail_insurance_completion_ceiling(tuning, 0.53, 20).unwrap() - 0.50).abs() < 1e-9
        );
        assert!(
            (pgt_tail_insurance_completion_ceiling(tuning, 0.47, 16).unwrap() - 0.56).abs() < 1e-9,
            "before last-chance mode, tail insurance stays capped at pair_cost 1.030"
        );
        assert!(
            (pgt_tail_insurance_completion_ceiling(tuning, 0.47, 15).unwrap() - 0.58).abs() < 1e-9,
            "last-chance mode can spend up to pair_cost 1.050 to avoid a full residual leg"
        );
        assert_eq!(
            pgt_tail_insurance_completion_ceiling(PgtTuning::legacy(), 0.43, 20),
            None
        );
    }

    #[test]
    fn xuan_ladder_taker_insurance_unlocks_small_loss_after_maker_window() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_xuan_ladder_taker_insurance_completion_ceiling(tuning, 0.50, 240, 44.9),
            None,
            "completion maker should get the initial queue window before taker insurance"
        );
        assert!(
            (pgt_xuan_ladder_taker_insurance_completion_ceiling(tuning, 0.50, 240, 45.0).unwrap()
                - 0.51)
                .abs()
                < 1e-9,
            "after the maker window, taker insurance may spend up to pair_cost 1.010"
        );
        assert!(
            (pgt_xuan_ladder_taker_insurance_completion_ceiling(tuning, 0.27, 240, 45.0).unwrap()
                - 0.74)
                .abs()
                < 1e-9,
            "cheap first legs get enough ceiling to close a one-cent adverse completion"
        );
        assert_eq!(
            pgt_xuan_ladder_taker_insurance_completion_ceiling(
                PgtTuning::legacy(),
                0.50,
                240,
                60.0
            ),
            None,
            "insurance is scoped to the xuan ladder shadow profile"
        );
    }

    #[test]
    fn xuan_ladder_timeout_insurance_caps_by_first_leg_price() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.29, 240, 3.9),
            None,
            "cheap first legs keep the full maker queue window before timeout insurance crosses"
        );
        assert_eq!(
            pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.35, 240, 1.4),
            None,
            "marginal first legs still wait for the OMS warmup window"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.35, 240, 1.5).unwrap()
                - 0.655)
                .abs()
                < 1e-9,
            "marginal 0.35 first legs should cross sooner instead of drifting to tail insurance"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.29, 240, 4.0).unwrap()
                - 0.72)
                .abs()
                < 1e-9,
            "cheap first legs may spend up to pair_cost 1.010"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.33, 240, 4.0).unwrap()
                - 0.675)
                .abs()
                < 1e-9,
            "mid first legs may spend up to pair_cost 1.005"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.36, 240, 4.0).unwrap()
                - 0.64)
                .abs()
                < 1e-9,
            "marginal 0.36 first legs may cross at breakeven to avoid tail residual"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.37, 240, 1.5).unwrap()
                - 0.63)
                .abs()
                < 1e-9,
            "higher first legs may also cross at breakeven once a safe completion is visible"
        );
        assert!(
            (pgt_xuan_ladder_timeout_insurance_completion_ceiling(tuning, 0.80, 240, 1.5).unwrap()
                - 0.20)
                .abs()
                < 1e-9,
            "timeout insurance is now price-cap free and bounded by breakeven pair cost"
        );
        assert_eq!(
            pgt_xuan_ladder_timeout_insurance_completion_ceiling(
                PgtTuning::legacy(),
                0.36,
                240,
                4.0
            ),
            None,
            "timeout insurance is scoped to the xuan ladder shadow profile"
        );
    }

    #[test]
    fn xuan_ladder_tail_insurance_extends_completion_price_validation() {
        assert!(
            pgt_completion_price_allowed(0.55, 0.52, Some(0.55)),
            "tail insurance must allow a completion price above the normal completion ceiling"
        );
        assert!(
            !pgt_completion_price_allowed(0.56, 0.52, Some(0.55)),
            "tail insurance remains bounded by its pair-cost cap"
        );
        assert!(
            !pgt_completion_price_allowed(0.53, 0.52, None),
            "without tail insurance, normal completion ceiling still applies"
        );
    }

    #[test]
    fn xuan_ladder_repair_budget_per_share_is_surplus_capped() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_effective_repair_budget_per_share(tuning, 0.0, 120.0, 180, 60.0),
            0.0
        );
        assert!(
            (pgt_effective_repair_budget_per_share(tuning, 0.6, 120.0, 180, 60.0) - 0.005).abs()
                < 1e-9
        );
        assert!(
            (pgt_effective_repair_budget_per_share(tuning, 10.0, 120.0, 180, 60.0) - 0.030).abs()
                < 1e-9,
            "xuan ladder repair must never spend more than three cents per residual share"
        );
        assert!(
            (pgt_effective_repair_budget_per_share(tuning, 10.0, 120.0, 180, 95.0) - 0.020).abs()
                < 1e-9,
            "stale no-budget rescue already spends one cent, so funded repair still caps total pair cost at 1.030"
        );
    }

    #[test]
    fn xuan_ladder_repair_budget_stays_locked_for_fresh_completion() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_effective_repair_budget_per_share(tuning, 10.0, 120.0, 180, 10.0),
            0.0,
            "fresh residuals should wait for true positive-edge completion instead of spending surplus"
        );
        assert!(
            (pgt_effective_repair_budget_per_share(tuning, 10.0, 120.0, 44, 10.0) - 0.030).abs()
                < 1e-9,
            "tail safety can spend capped repair budget even for a fresh residual"
        );
    }

    #[test]
    fn xuan_ladder_seed_cost_brake_only_after_unprofitable_closed_pair() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_seed_min_visible_breakeven_slack_ticks(tuning, 0, 0.0, None),
            XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS
        );
        assert_eq!(
            pgt_seed_min_visible_breakeven_slack_ticks(tuning, 2, 0.0, Some(0.990)),
            XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS
        );
        assert_eq!(
            pgt_seed_min_visible_breakeven_slack_ticks(tuning, 2, 0.0, Some(1.000)),
            XUAN_LADDER_COST_BRAKE_MIN_SLACK_TICKS
        );
        assert_eq!(
            pgt_seed_min_visible_breakeven_slack_ticks(tuning, 2, 1.0, Some(1.000)),
            XUAN_LADDER_MIN_VISIBLE_BREAKEVEN_SLACK_TICKS
        );
    }

    #[test]
    fn xuan_ladder_first_seed_accepts_cheap_visible_maker_completion() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.34, 0.65, 0.01),
            "deep-discount first leg remains allowed when opposite maker completion is already visible at 0.98"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.36, 0.63, 0.01),
            "0.36 first leg remains allowed when visible taker completion has edge"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.36, 0.64, 0.01),
            "0.36 first leg is allowed when visible maker completion has edge"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.36, 0.65, 0.01),
            "first leg is blocked when taker and maker completion both exceed the pair-cost cap"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.53, 0.01),
            "mid-price first legs are no longer hard-blocked when visible maker completion has edge"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.52, 0.01),
            "mid-price first legs are no longer hard-blocked when immediate taker completion has edge"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.54, 0.01),
            "first leg is blocked when even opposite maker completion is above the pair-cost cap"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.66, 0.34, 0.01),
            "expensive first legs are allowed when visible maker completion has edge"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.63, 0.36, 0.01),
            "expensive first leg remains allowed when current opposite ask offers a visible positive-edge completion path"
        );
    }

    #[test]
    fn xuan_ladder_maker_only_first_seed_is_capped() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            pgt_xuan_ladder_maker_only_seed_clip_caps(tuning, 0, false),
            "first seed without an immediate taker completion path must not use the full ladder clip"
        );
        assert!(
            !pgt_xuan_ladder_maker_only_seed_clip_caps(tuning, 0, true),
            "visible taker completion keeps the normal ladder clip"
        );
        assert!(
            !pgt_xuan_ladder_maker_only_seed_clip_caps(tuning, 1, false),
            "after the first fill, reopen-specific guards own the path"
        );
        assert!(
            !pgt_xuan_ladder_maker_only_seed_clip_caps(PgtTuning::legacy(), 0, false),
            "legacy/replay profiles are unaffected"
        );
    }

    #[test]
    fn xuan_ladder_low_first_relaxed_completion_seed_is_capped() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(XUAN_LADDER_LOW_FIRST_RELAXED_COMPLETION_CLIP_QTY, 20.0);
        assert!(
            pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(tuning, 0, 0.47, 0.53),
            "low first legs that only qualify through the relaxed breakeven cap should not use full ladder clip"
        );
        assert!(
            !pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(tuning, 0, 0.47, 0.52),
            "strictly profitable visible completion keeps the normal first-leg clip"
        );
        assert!(
            !pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(tuning, 0, 0.60, 0.40),
            "the relaxed low-first cap is scoped below the low-first boundary"
        );
        assert!(
            !pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(tuning, 1, 0.47, 0.53),
            "same-round reopen sizing is controlled by the reopen clip cap"
        );
        assert!(
            !pgt_xuan_ladder_low_first_relaxed_completion_clip_caps(
                PgtTuning::legacy(),
                0,
                0.47,
                0.53
            ),
            "legacy/replay profiles are unaffected"
        );
    }

    #[test]
    fn xuan_ladder_first_seed_has_no_hard_price_cap() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.37, 0.62, 0.01),
            "first-leg prices above 0.36 are allowed when visible completion quality is good"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.80, 0.19, 0.01),
            "high first-leg prices are allowed when taker completion has edge"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.80, 0.21, 0.01),
            "high first-leg prices are still blocked when completion quality is poor"
        );
    }

    #[test]
    fn xuan_ladder_low_first_seed_allows_breakeven_taker_completion() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_xuan_ladder_seed_taker_completion_pair_cap(0.59),
            XUAN_LADDER_LOW_FIRST_SEED_TAKER_COMPLETION_PAIR_CAP
        );
        assert_eq!(
            pgt_xuan_ladder_seed_taker_completion_pair_cap(0.60),
            XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.59, 0.41, 0.01),
            "OOS-supported low first legs may seed when immediate completion is breakeven"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.59, 0.42, 0.01),
            "low first legs still need at least a breakeven immediate path or maker edge"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.61, 0.395, 0.01),
            "the breakeven taker relaxation is not inherited by higher first-leg prices"
        );
    }

    #[test]
    fn xuan_ladder_seed_visible_completion_guard_is_first_leg_only() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.50, 0.53, 0.01),
            "first seed admission still blocks when neither taker nor maker completion has enough edge"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 1, 0.47, 0.54, 0.01),
            "after the first buy fill, completion and reopen guards own the risk path"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(
                PgtTuning::legacy(),
                0,
                0.47,
                0.54,
                0.01
            ),
            "legacy/replay profiles are not changed by the xuan-specific guard"
        );
    }

    #[test]
    fn xuan_ladder_last_chance_taker_close_removes_tail_residual() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            pgt_xuan_ladder_last_chance_taker_close(tuning, 0.05, 15, 60.0, 0.99),
            "last-chance tail mode should cross the spread to eliminate a mature residual"
        );
        assert!(
            !pgt_xuan_ladder_last_chance_taker_close(tuning, 0.05, 16, 60.0, 0.99),
            "before the last-chance window, normal pair-cost caps still apply"
        );
        assert!(
            !pgt_xuan_ladder_last_chance_taker_close(tuning, 0.05, 15, 44.9, 0.99),
            "fresh residuals still get the maker queue window"
        );
        assert!(
            !pgt_xuan_ladder_last_chance_taker_close(tuning, 0.05, 15, 60.0, 1.00),
            "ask must remain strictly buyable below a full-dollar completion"
        );
        assert!(
            !pgt_xuan_ladder_last_chance_taker_close(tuning, 0.42, 15, 60.0, 0.99),
            "last-chance mode must not close residuals at catastrophic pair cost"
        );
        assert!(
            !pgt_xuan_ladder_last_chance_taker_close(PgtTuning::legacy(), 0.05, 15, 60.0, 0.99),
            "last-chance crossing is scoped to the xuan ladder shadow profile"
        );
    }

    #[test]
    fn xuan_ladder_reopens_after_only_high_quality_rescue_close() {
        let inv = InventoryState::default();
        let book = Book::default();
        let strat_metrics = StrategyInventoryMetrics {
            paired_qty: 0.0,
            pair_cost: 0.0,
            paired_locked_pnl: 0.0,
            total_spent: 0.0,
            worst_case_outcome_pnl: 0.0,
            dominant_side: None,
            residual_qty: 0.0,
            residual_inventory_value: 0.0,
        };
        let mut ledger = PairLedgerSnapshot::default();
        ledger.recent_closed[0] = Some(PairTranche {
            pairable_qty: 160.0,
            pair_cost_tranche: 0.880,
            ..PairTranche::default()
        });
        let metrics = EpisodeMetrics {
            round_buy_fill_count: 2,
            ..EpisodeMetrics::default()
        };
        let inventory = InventorySnapshot {
            settled: inv,
            working: inv,
            pair_ledger: ledger,
            episode_metrics: metrics,
            ..InventorySnapshot::default()
        };
        let input = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &ledger,
            episode_metrics: &metrics,
            book: &book,
            metrics: &strat_metrics,
            ofi: None,
            glft: None,
        };
        let tuning = PgtTuning::xuan_ladder_v1();

        assert!(pgt_allow_reopen_after_rescue_close(
            tuning, input, 180, false
        ));
        assert!(!pgt_allow_reopen_after_rescue_close(
            tuning, input, 180, true
        ));

        let mut costly_ledger = ledger;
        costly_ledger.recent_closed[0] = Some(PairTranche {
            pairable_qty: 160.0,
            pair_cost_tranche: 0.940,
            ..PairTranche::default()
        });
        let costly_inventory = InventorySnapshot {
            pair_ledger: costly_ledger,
            episode_metrics: metrics,
            ..inventory
        };
        let costly_input = StrategyTickInput {
            pair_ledger: &costly_ledger,
            inventory: &costly_inventory,
            ..input
        };
        assert!(!pgt_allow_reopen_after_rescue_close(
            tuning,
            costly_input,
            180,
            false
        ));
        assert!(!pgt_allow_reopen_after_rescue_close(
            tuning, input, 90, false
        ));

        let late_metrics = EpisodeMetrics {
            round_buy_fill_count: 4,
            ..metrics
        };
        let late_inventory = InventorySnapshot {
            episode_metrics: late_metrics,
            ..inventory
        };
        let late_input = StrategyTickInput {
            inventory: &late_inventory,
            episode_metrics: &late_metrics,
            ..input
        };
        assert!(!pgt_allow_reopen_after_rescue_close(
            tuning, late_input, 180, false
        ));
    }

    #[test]
    fn xuan_ladder_blocks_low_quality_reopen_after_closed_pair() {
        let inv = InventoryState::default();
        let book = Book::default();
        let strat_metrics = StrategyInventoryMetrics {
            paired_qty: 0.0,
            pair_cost: 0.0,
            paired_locked_pnl: 0.0,
            total_spent: 0.0,
            worst_case_outcome_pnl: 0.0,
            dominant_side: None,
            residual_qty: 0.0,
            residual_inventory_value: 0.0,
        };
        let mut ledger = PairLedgerSnapshot::default();
        ledger.recent_closed[0] = Some(PairTranche {
            pairable_qty: 160.0,
            pair_cost_tranche: 1.010,
            ..PairTranche::default()
        });
        let metrics = EpisodeMetrics {
            round_buy_fill_count: 2,
            ..EpisodeMetrics::default()
        };
        let inventory = InventorySnapshot {
            settled: inv,
            working: inv,
            pair_ledger: ledger,
            episode_metrics: metrics,
            ..InventorySnapshot::default()
        };
        let input = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &ledger,
            episode_metrics: &metrics,
            book: &book,
            metrics: &strat_metrics,
            ofi: None,
            glft: None,
        };
        let tuning = PgtTuning::xuan_ladder_v1();

        assert!(pgt_blocks_reopen_after_closed_pair(tuning, input, false));

        let mut high_quality_ledger = ledger;
        high_quality_ledger.recent_closed[0] = Some(PairTranche {
            pairable_qty: 160.0,
            pair_cost_tranche: 1.000,
            ..PairTranche::default()
        });
        let high_quality_inventory = InventorySnapshot {
            pair_ledger: high_quality_ledger,
            episode_metrics: metrics,
            ..inventory
        };
        let high_quality_input = StrategyTickInput {
            pair_ledger: &high_quality_ledger,
            inventory: &high_quality_inventory,
            ..input
        };
        assert!(!pgt_blocks_reopen_after_closed_pair(
            tuning,
            high_quality_input,
            false
        ));
        assert!(pgt_blocks_reopen_after_closed_pair(
            tuning,
            high_quality_input,
            true
        ));

        let early_metrics = EpisodeMetrics {
            round_buy_fill_count: 1,
            ..metrics
        };
        let early_inventory = InventorySnapshot {
            episode_metrics: early_metrics,
            ..inventory
        };
        let early_input = StrategyTickInput {
            inventory: &early_inventory,
            episode_metrics: &early_metrics,
            ..input
        };
        assert!(!pgt_blocks_reopen_after_closed_pair(
            tuning,
            early_input,
            false
        ));

        let late_metrics = EpisodeMetrics {
            round_buy_fill_count: 4,
            ..metrics
        };
        let late_inventory = InventorySnapshot {
            episode_metrics: late_metrics,
            ..inventory
        };
        let late_input = StrategyTickInput {
            inventory: &late_inventory,
            episode_metrics: &late_metrics,
            ..high_quality_input
        };
        assert!(
            pgt_blocks_reopen_after_closed_pair(tuning, late_input, false),
            "after two filled pairs, the profile should stop opening more tranches"
        );
    }

    #[test]
    fn xuan_ladder_reopen_seed_requires_visible_low_cost_completion() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            !pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 0, None, 0.47, 0.54, 0.01),
            "first seed quality remains owned by the first-leg guard"
        );
        assert!(
            !pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 2, Some(1.000), 0.46, 0.53, 0.01),
            "second tranche can open after a clean breakeven pair when visible completion is <= 1.000"
        );
        assert!(
            pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 2, Some(1.000), 0.47, 0.55, 0.01),
            "second tranche is blocked when projected maker pair cost is above the cap"
        );
        assert!(
            pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 2, Some(1.010), 0.47, 0.52, 0.01),
            "second tranche is blocked when the just-closed pair was negative"
        );
        assert!(
            pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 4, Some(0.880), 0.47, 0.52, 0.01),
            "after two filled pairs, no further reopen should be admitted"
        );
        assert!(
            !pgt_xuan_ladder_reopen_seed_quality_blocks(
                PgtTuning::legacy(),
                2,
                Some(0.990),
                0.47,
                0.54,
                0.01
            ),
            "legacy/replay profiles are not changed by the xuan-specific reopen guard"
        );
    }

    #[test]
    fn xuan_ladder_reopen_seed_clip_is_capped() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(!pgt_xuan_ladder_reopen_seed_clip_caps(tuning, 0));
        assert!(!pgt_xuan_ladder_reopen_seed_clip_caps(tuning, 1));
        assert!(pgt_xuan_ladder_reopen_seed_clip_caps(tuning, 2));
        assert!(!pgt_xuan_ladder_reopen_seed_clip_caps(tuning, 3));
        assert!(!pgt_xuan_ladder_reopen_seed_clip_caps(
            PgtTuning::legacy(),
            2
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_plan_with_slack(
        side: Side,
        price: f64,
        entry_pressure_extra_ticks: u8,
        visible_completion_slack_ticks: f64,
    ) -> SeedPlan {
        seed_plan_with_slack_and_taker(
            side,
            price,
            entry_pressure_extra_ticks,
            visible_completion_slack_ticks,
            false,
        )
    }

    fn seed_plan_with_slack_and_taker(
        side: Side,
        price: f64,
        entry_pressure_extra_ticks: u8,
        visible_completion_slack_ticks: f64,
        visible_taker_completion_ok: bool,
    ) -> SeedPlan {
        SeedPlan {
            size: 72.0,
            taker_shadow_would_open: false,
            visible_taker_completion_ok,
            entry_pressure_extra_ticks,
            visible_completion_slack_ticks,
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
        let yes = seed_plan_with_slack_and_taker(Side::Yes, 0.47, 1, 1.0, true);
        let no = seed_plan_with_slack_and_taker(Side::No, 0.50, 1, 1.0, true);

        let selection = strategy.select_flat_seed_plans(
            Some(&yes),
            Some(&no),
            PgtShadowProfile::XuanLadderV1,
            true,
            Some(Side::No),
            false,
        );

        assert_eq!(selection, FlatSeedSelection::NoOnly);
    }

    #[test]
    fn select_flat_seed_plans_returns_dual_after_latch_exhaustion() {
        let strategy = PairGatedTrancheStrategy;
        let yes = seed_plan_with_slack_and_taker(Side::Yes, 0.47, 0, 1.0, true);
        let no = seed_plan_with_slack_and_taker(Side::No, 0.50, 0, 1.0, true);

        let selection = strategy.select_flat_seed_plans(
            Some(&yes),
            Some(&no),
            PgtShadowProfile::XuanLadderV1,
            true,
            None,
            true,
        );

        assert_eq!(selection, FlatSeedSelection::Dual);
    }

    #[test]
    fn xuan_cycle_merge_keeps_dual_low_seed_quotes_despite_latch() {
        let strategy = PairGatedTrancheStrategy;
        let yes = seed_plan_with_slack(Side::Yes, 0.06, 0, -10.0);
        let no = seed_plan_with_slack(Side::No, 0.06, 0, -10.0);

        let selection = strategy.select_flat_seed_plans(
            Some(&yes),
            Some(&no),
            PgtShadowProfile::XuanCycleMergeV1,
            true,
            Some(Side::Yes),
            false,
        );

        assert_eq!(selection, FlatSeedSelection::Dual);
    }

    #[test]
    fn xuan_ladder_selection_avoids_dominated_expensive_seed() {
        let strategy = PairGatedTrancheStrategy;
        let expensive_yes = seed_plan_with_slack(Side::Yes, 0.55, 0, 0.0);
        let cheaper_no = seed_plan_with_slack(Side::No, 0.43, 0, 2.0);

        let selection = strategy.select_flat_seed_plans(
            Some(&expensive_yes),
            Some(&cheaper_no),
            PgtShadowProfile::XuanLadderV1,
            true,
            Some(Side::Yes),
            false,
        );

        assert_eq!(selection, FlatSeedSelection::NoOnly);
    }

    #[test]
    fn xuan_ladder_selection_allows_expensive_seed_with_dominant_completion_slack() {
        let strategy = PairGatedTrancheStrategy;
        let expensive_yes = seed_plan_with_slack_and_taker(Side::Yes, 0.55, 0, 3.0, true);
        let cheaper_no = seed_plan_with_slack(Side::No, 0.43, 0, 0.5);

        let selection = strategy.select_flat_seed_plans(
            Some(&expensive_yes),
            Some(&cheaper_no),
            PgtShadowProfile::XuanLadderV1,
            true,
            None,
            false,
        );

        assert_eq!(selection, FlatSeedSelection::YesOnly);
    }

    #[test]
    fn xuan_ladder_selection_avoids_dual_seed_without_taker_completion() {
        let strategy = PairGatedTrancheStrategy;
        let cheap_yes = seed_plan_with_slack(Side::Yes, 0.27, 0, 4.0);
        let high_no = seed_plan_with_slack(Side::No, 0.67, 0, 4.0);

        let selection = strategy.select_flat_seed_plans(
            Some(&cheap_yes),
            Some(&high_no),
            PgtShadowProfile::XuanLadderV1,
            true,
            None,
            true,
        );

        assert_eq!(selection, FlatSeedSelection::YesOnly);
    }

    #[test]
    fn xuan_ladder_selection_avoids_dual_when_seed_sizes_differ() {
        let strategy = PairGatedTrancheStrategy;
        let mut cheap_yes = seed_plan_with_slack_and_taker(Side::Yes, 0.41, 0, 4.0, true);
        cheap_yes.size = 45.0;
        cheap_yes.intent.size = 45.0;
        let mut high_no = seed_plan_with_slack_and_taker(Side::No, 0.55, 0, 4.0, true);
        high_no.size = 120.0;
        high_no.intent.size = 120.0;

        let selection = strategy.select_flat_seed_plans(
            Some(&cheap_yes),
            Some(&high_no),
            PgtShadowProfile::XuanLadderV1,
            true,
            None,
            false,
        );

        assert_eq!(selection, FlatSeedSelection::YesOnly);
    }
}
