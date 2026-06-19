use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{StrategyCoordinator, PAIR_ARB_NET_EPS};
use crate::polymarket::messages::{BidReason, TradeDirection};
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
const XUAN_LADDER_LOW_PRICE_SEED_MAX: f64 = 0.45;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_PAIR_COST: f64 = 0.900;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_MIN_REMAINING_SECS: u64 = 120;
const XUAN_LADDER_REOPEN_AFTER_RESCUE_MAX_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_PAIR_COST: f64 = 0.985;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_MIN_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_AFTER_CLOSED_MAX_BUY_FILLS: u64 = 2;
const XUAN_LADDER_REOPEN_PROJECTED_PAIR_CAP: f64 = 0.980;
pub(crate) const XUAN_PAIR_ASK_RESCUE_GAP_COOLDOWN_SECS: u64 = 5;
pub(crate) const XUAN_PAIR_ASK_RESCUE_PAIR_CAP: f64 = 1.010;
const XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP: f64 = 0.995;
const XUAN_LADDER_SEED_MAKER_COMPLETION_PAIR_CAP: f64 = 0.990;
const XUAN_LADDER_FIRST_SEED_FULL_CLIP_MAX_PRICE: f64 = 0.34;
const XUAN_LADDER_FIRST_SEED_MAX_PRICE: f64 = 0.36;
const XUAN_LADDER_MAKER_ONLY_SEED_MAX_PRICE: f64 = 0.42;
const XUAN_LADDER_MAKER_ONLY_SEED_CLIP_QTY: f64 = 45.0;
const XUAN_LADDER_DUAL_SEED_SIZE_TOLERANCE: f64 = 0.05;
const XUAN_LADDER_LAST_CHANCE_CLOSE_REMAINING_SECS: u64 = 15;
const XUAN_LADDER_LAST_CHANCE_CLOSE_MIN_AGE_SECS: f64 = 45.0;
const XUAN_LADDER_LAST_CHANCE_CLOSE_MAX_ASK: f64 = 0.99;
const XUAN_LADDER_TAIL_DIAG_REMAINING_SECS: u64 = 60;
const XUAN_LADDER_TAIL_DIAG_INTERVAL_SECS: u64 = 5;
// ce25_nagi autoresearch summary (6 generations, 1000+ variants, 2026-06-11/12):
// OVERFITTING WARNING: gen-3/4 "champions" (micro core 0.3125-0.33125) had only 2 trades — NOT reliable.
// Gen-6 champion (1567 trades, 85.7% participation, statistically reliable):
//   SLA=60s + pair_cap=0.930 + tq=13: PnL=+242.36, ROI=4.56%, pair_cost=0.797, resid=68.3%
//   pair_cap curve: 0.930 > 0.940 > 0.950 > 0.970 (PnL +242 > +218 > +104 > +75)
//   Hard floor at pc=0.930 (0.920=0.925=0.930=0.935 all identical → 0.930 is the effective limit).
//   Key: tighter pair_cap + larger tq = 3.3x PnL improvement over baseline.
// Price zone 0.20-0.35 is the reliable baseline. Q4 (0.3125-0.35) directionally positive (7-9 actions).
const NAGI_CE25_LAST60_TAIL_SECS: u64 = 60;
const NAGI_CE25_LOW_PX_TAIL_MAX: f64 = 0.3125;  // gen-3: Q4 zone start
const NAGI_CE25_UPPER_ALPHA_BAND_MAX: f64 = 0.35;  // gen-2/3 zone ceiling
const NAGI_CE25_Q4_ALPHA_CORE_LO: f64 = 0.3125;  // gen-2: Q4 zone
const NAGI_CE25_Q4_ALPHA_CORE_HI: f64 = 0.35;    // gen-2: Q4 zone
const NAGI_CE25_Q4_MICRO_CORE_LO: f64 = 0.3125;  // gen-3: micro alpha core (lower eighth)
const NAGI_CE25_Q4_MICRO_CORE_HI: f64 = 0.33125; // gen-3: micro alpha core ceiling
// Exported versions for cross-strategy use (pair_arb micro core awareness)
pub(crate) const NAGI_CE25_Q4_MICRO_CORE_LO_PUB: f64 = NAGI_CE25_Q4_MICRO_CORE_LO;
pub(crate) const NAGI_CE25_Q4_MICRO_CORE_HI_PUB: f64 = NAGI_CE25_Q4_MICRO_CORE_HI;
// NAGI_LAST60_MIDPRICE_FASTPAIR_V1: complementary UP-side alpha (0.35-0.50, pair_delay≤15s).
// Gen-3 best: same_row_cap_0.965, score=0.346, ROI=13.28%, pair_cost=0.870, 32 seed_actions.
// Higher coverage than DOWN-side micro core but lower ROI — use as secondary signal.
const NAGI_FASTPAIR_UP_ALPHA_LO: f64 = 0.35;   // UP-side alpha zone start
const NAGI_FASTPAIR_UP_ALPHA_HI: f64 = 0.50;   // UP-side alpha zone ceiling
const NAGI_FASTPAIR_UP_PC_CAP: f64 = 0.965;    // gen-3 best for fastpair UP-side
// Aggressive xuan absorption additions (from deep_dive + V1.1):
// Stronger low-residual gating for new first legs (core xuan: "low residual state open first leg")
const XUAN_LADDER_MAX_RESIDUAL_FOR_NEW_SEED: f64 = 15.0;  // tighter than legacy to mimic xuan clean pairing
const XUAN_LADDER_COST_BRAKE_ON_LOSS_CLOSED: bool = true; // enable breakeven-path brake after loss-closed first leg
const XUAN_LADDER_SURPLUS_RESCUE_MAX_AGE_SECS: f64 = 90.0; // allow stale exposure insurance only on aged residuals
const XUAN_LADDER_PRE_MERGE_BIAS_CLIP_MULT: f64 = 0.7;   // clip seed if pre-merge bias detected (from xuan analysis)
const XUAN_LADDER_DUAL_SEED_MAX_PRICE_DIFF: f64 = 0.03;  // tighter dual seed price tolerance for fidelity

static PGT_LAST_SEED_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);
static PGT_LAST_COMPLETION_NONE_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);
static PGT_LAST_TAIL_DIAG_UNIX_SECS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgtShadowProfile {
    Legacy,
    ReplayFocusedV1,
    ReplayLowerClipV1,
    XuanLadderV1,
    BalancedPnlV1,  // New: explicitly balances low_pair_cost + low_residual (high clean_closed) + high_participation. Trade-offs via gates.
    Nagi777V1,      // Learn from nagi777 (5m specialist, high short-term PnL/ROI in btc-updown-5m etc.): aggressive 5m participation via local price timing + L2, tight pair cost, strong residual/insurance gates. Research/shadow only.
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
    // Nagi777 5m explosive: local price agg boost factor for 5m (0-1, higher = more participation via local timing, but gated).
    nagi_local_boost: f64,
    // Parallel ce25 nagi blast: last60 tail focus (0-1) for controlled completion in 5m (per V1 search last60_down policies, L2 med d5 831 variance).
    nagi_5m_last60_focus: f64,
    // 继续往死里干 全都要: p5 thin risk cap (0-1, from data low-seed clean drops to 0.24), boundary open boost (0-1 for high remaining 5m open local signal).
    nagi_5m_p5_risk_cap: f64,
    nagi_boundary_open_boost: f64,
    entry_requires_pair_cap: bool,
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
            nagi_local_boost: 0.0,
            nagi_5m_last60_focus: 0.0,
            nagi_5m_p5_risk_cap: 0.0,
            nagi_boundary_open_boost: 0.0,
            entry_requires_pair_cap: false,
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
            nagi_local_boost: 0.0,
            nagi_5m_last60_focus: 0.0,
            nagi_5m_p5_risk_cap: 0.0,
            nagi_boundary_open_boost: 0.0,
            entry_requires_pair_cap: false,
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
            nagi_local_boost: 0.0,
            nagi_5m_last60_focus: 0.0,
            nagi_5m_p5_risk_cap: 0.0,
            nagi_boundary_open_boost: 0.0,
            entry_requires_pair_cap: false,
        }
    }

    fn xuan_ladder_v1() -> Self {
        Self {
            profile: PgtShadowProfile::XuanLadderV1,
            // Recent xuan samples start as early as t+4s and keep opening until
            // late round. This profile is shadow-only; it intentionally models
            // the public ladder shape rather than the conservative replay subset.
            // Aggressive absorption: tighter residual gate, cost brake on loss-closed,
            // pre-merge bias clip, dual-seed price tolerance from xuan deep dive.
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
            nagi_local_boost: 0.0,
            nagi_5m_last60_focus: 0.0,
            nagi_5m_p5_risk_cap: 0.0,
            nagi_boundary_open_boost: 0.0,
            entry_requires_pair_cap: false,
        }
    }

    fn balanced_pnl_v1() -> Self {
        // Crazy backtest-driven: balance the three PNL pillars using V1 insights (xuan high clean_closed ~0.95, low pair_cost median~0.975, participation via safe seeds).
        // Low pair_cost: tight caps (0.98 early).
        // Low residual: very strict residual gate + strong brakes/insurance.
        // High participation: earlier entry than conservative replay, but gated by residual/L2-like (via slack).
        // Trade-off: use xuan_ladder as base but dial participation up under constraints.
        Self {
            profile: PgtShadowProfile::BalancedPnlV1,
            seed_open_max_remaining_secs: Some(240),  // allow more participation (earlier than some)
            seed_open_min_remaining_secs: Some(30),
            hard_no_new_open_secs: 30,
            price_aware_no_new_open_secs: 45,
            open_pair_band_cap: Some(0.985),  // tighter for low cost
            completion_early_pair_cap: 0.980,
            completion_late_pair_cap: 0.995,
            taker_close_pair_cap: 0.990,
            fixed_clip_qty: Some(80.0),  // balanced clip for participation vs risk
            clip_profile: PgtClipProfile::XuanLadderV1,  // reuse good ladder
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: 1.0,
            seed_min_visible_breakeven_slack_ticks: -2.0,
            base_clip_qty: 80.0,
            min_clip_qty: 40.0,
            max_clip_qty: 150.0,
            nagi_local_boost: 0.0,
            nagi_5m_last60_focus: 0.0,
            nagi_5m_p5_risk_cap: 0.0,
            nagi_boundary_open_boost: 0.0,
            entry_requires_pair_cap: false,
        }
    }

    fn nagi777_v1() -> Self {
        // Learn from nagi777 (5m high-volume PnL machine).
        // 9-gen autoresearch (1200+ variants, 2026-06-11/13):
        //   🏆 gen9_sla45_pc0.930_tq16_cc0.980_ttc5_hc0.50_caeTrue: PnL=+32.13 USDC
        //      net_roi=7.88%, residual_qty_rate=16.45%. 15天数据, 统计可靠.
        //   Key insight: Completion-Aware Entry (cae=True) prevents entering when completion
        //   is infeasible, resolving toxic residual losses and shifting strategy to highly profitable.
        //   optimal rescue parameters: close_cap=0.980, ttc=5s, tq=160, SLA=45s.
        // Shadow-only; V1 5m + boundary + L2 + uncertainty. Health first, manifest only.
        Self {
            profile: PgtShadowProfile::Nagi777V1,
            seed_open_max_remaining_secs: Some(260),
            seed_open_min_remaining_secs: Some(10),
            hard_no_new_open_secs: 15,
            price_aware_no_new_open_secs: 25,
            open_pair_band_cap: Some(0.980),  // gen-3 parameter sweep: optimal open band cap
            completion_early_pair_cap: 0.982,  // gen-3 parameter sweep: optimal early pair cap
            completion_late_pair_cap: 0.980,   // gen-9: optimal late cap for rescue
            taker_close_pair_cap: 0.980,       // gen-9: optimal close cap for rescue
            fixed_clip_qty: Some(80.0),        // optimal clip qty for participation/risk
            clip_profile: PgtClipProfile::XuanLadderV1,
            preserve_seed_clip_qty: true,
            expensive_seed_min_visible_slack_ticks: 0.3,
            seed_min_visible_breakeven_slack_ticks: -1.0,
            base_clip_qty: 80.0,
            min_clip_qty: 35.0,
            max_clip_qty: 200.0,
            nagi_local_boost: 0.40,
            nagi_5m_last60_focus: 1.0,
            nagi_5m_p5_risk_cap: 0.75,
            nagi_boundary_open_boost: 0.5,
            entry_requires_pair_cap: false,
        }
    }

    fn hybrid_pnl_nagi_v1() -> Self {
        // Gen-2 informed hybrid: blend xuan clean + nagi gen-2 champion findings.
        // Gen-2: Q4 (0.3125-0.35) + pc=0.950 => ROI=22.47%, score=0.551. Hybrid uses 0.955 as compromise.
        // Goal: high part (nagi aggressive on 5m) with xuan-like residual control.
        let mut s = Self::nagi777_v1();
        s.profile = PgtShadowProfile::BalancedPnlV1;
        s.completion_early_pair_cap = 0.955; // gen-2: compromise (nagi pure=0.950, xuan=0.980)
        s.completion_late_pair_cap = 0.985;
        s.taker_close_pair_cap = 0.980;
        s.open_pair_band_cap = Some(0.960); // gen-2: tighter, between nagi 0.950 and xuan
        s.nagi_local_boost = 0.35;
        s.fixed_clip_qty = Some(65.0);
        s.seed_open_min_remaining_secs = Some(11);
        s.nagi_5m_last60_focus = 0.6;
        s.nagi_5m_p5_risk_cap = 0.5;
        s.nagi_boundary_open_boost = 0.3;
        s.min_clip_qty = 35.0;
        s.max_clip_qty = 145.0;
        s
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
            "balanced_pnl_v1" | "balanced" | "pnl_balanced" | "profit" => Self::balanced_pnl_v1(),
            "nagi777_v1" | "nagi777" | "nagi" | "nagi_v1" => Self::nagi777_v1(),
            "hybrid_pnl_nagi_v1" | "hybrid_nagi" | "nagi_hybrid" => Self::hybrid_pnl_nagi_v1(),  // parallel blast: xuan clean + nagi volume (search best + ce25 last60 + L2/uncertainty)
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
            if self.profile == PgtShadowProfile::XuanLadderV1 {
                base.max(cap)
            } else {
                base.min(cap)
            }
        } else {
            base
        }
    }
}

#[cfg(not(test))]
fn pgt_tuning() -> PgtTuning {
    static TUNING: OnceLock<PgtTuning> = OnceLock::new();
    *TUNING.get_or_init(PgtTuning::from_env)
}

#[cfg(test)]
thread_local! {
    static TEST_TUNING: std::cell::Cell<Option<PgtTuning>> = std::cell::Cell::new(None);
}

#[cfg(test)]
fn pgt_tuning() -> PgtTuning {
    TEST_TUNING.with(|cell| {
        if let Some(t) = cell.get() {
            t
        } else {
            PgtTuning::from_env()
        }
    })
}


fn get_p5_thin_risk(tuning_cap: f64, l2_depth: Option<crate::polymarket::strategy::L2BookDepth>) -> f64 {
    let base_cap = tuning_cap.max(0.5);
    if let Some(depth) = l2_depth {
        let min_depth = depth.bid_depth_5lvl.min(depth.ask_depth_5lvl);
        if min_depth < 50.0 {
            0.5
        } else if min_depth < 500.0 {
            0.5 + (base_cap - 0.5) * ((min_depth - 50.0) / 450.0)
        } else {
            base_cap
        }
    } else {
        base_cap
    }
}

pub(crate) fn pgt_absent_seed_retain_allowed(
    remaining_secs: u64,
    slot_last_ts_elapsed: std::time::Duration,
) -> bool {
    if remaining_secs == u64::MAX {
        return false;
    }
    if remaining_secs <= HARD_NO_NEW_OPEN_SECS {
        return false;
    }
    if remaining_secs <= TAIL_COMPLETION_ONLY_SECS {
        return slot_last_ts_elapsed <= std::time::Duration::from_millis(500);
    }
    if remaining_secs <= 120 {
        return slot_last_ts_elapsed <= std::time::Duration::from_millis(1_200);
    }
    slot_last_ts_elapsed <= std::time::Duration::from_secs(4)
}

pub(crate) fn pgt_pair_ask_rescue_exec_enabled() -> bool {
    read_bool_env("PM_PGT_PAIR_ASK_RESCUE_EXEC_ENABLED").unwrap_or(false)
}

pub(crate) fn pgt_shadow_taker_open_exec_enabled() -> bool {
    read_bool_env("PM_PGT_SHADOW_TAKER_OPEN_EXEC_ENABLED").unwrap_or(false)
}

pub(crate) fn pgt_settlement_alpha_taker_open_exec_enabled() -> bool {
    read_bool_env("PM_PGT_SETTLEMENT_ALPHA_TAKER_OPEN_EXEC_ENABLED").unwrap_or(false)
}

pub(crate) fn pgt_settlement_alpha_inventory_net_cap() -> Option<f64> {
    std::env::var("PM_PGT_SETTLEMENT_ALPHA_INVENTORY_NET_CAP")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .and_then(|cap| if cap.is_finite() && cap >= 0.0 { Some(cap) } else { None })
}

fn read_bool_env(name: &str) -> Option<bool> {
    let raw = std::env::var(name).ok()?;
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => Some(false),
    }
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

        // V1/xuan absorption: residual gate before flat seed selection (xuan core: don't open new first leg if high residual)
        if tuning.profile == PgtShadowProfile::XuanLadderV1
            && Self::xuan_ladder_residual_blocks_new_seed(&input)
        {
            quotes.note_pgt_skip_residual_guard();
            // In xuan behavior, high residual -> focus on completion/repair, delay new seed
            // (completion logic later in the function will handle opposite side)
            // Future: integrate V1 XUAN_COMPLETION_CANDIDATE_RESCORE to further gate or reprice based on rescore.
        }

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
        // Opening-leg seed is anchored by the broad open_pair_band ceiling.
        // But we also reserve room for the opposite leg to complete at
        // ask-1tick if that ask is already visible now; otherwise we enter
        // first-leg fills that only close after drifting into negative pair
        // cost. Legacy clip haircut still handles the "no immediate completion"
        // case, while replay profiles preserve searched seed clip size and use
        // price gates to control completion budget.
        let open_pair_band_yes =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs, Some(ub.yes_bid));
        let open_pair_band_no =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs, Some(ub.no_bid));
        let tick = coordinator.cfg().tick_size.max(1e-9);
        let yes_future_completion_reserve_ticks =
            pgt_seed_future_completion_reserve_ticks(remaining_secs, ub.no_ask);
        let no_future_completion_reserve_ticks =
            pgt_seed_future_completion_reserve_ticks(remaining_secs, ub.yes_ask);
        let yes_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band_yes, ub.no_bid)?;
        let no_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band_no, ub.yes_bid)?;
        let yes_completion_ref =
            (ub.no_ask - tick).max(0.0) + yes_future_completion_reserve_ticks * tick;
        let no_completion_ref =
            (ub.yes_ask - tick).max(0.0) + no_future_completion_reserve_ticks * tick;
        let yes_immediate_completion_cap = (open_pair_band_yes - yes_completion_ref).clamp(0.0, 1.0);
        let no_immediate_completion_cap = (open_pair_band_no - no_completion_ref).clamp(0.0, 1.0);
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
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs, Some(raw_price));
        let mut price = self.passive_seed_price(coordinator, side, ceiling, best_bid, best_ask)?;
        if price <= 0.0 {
            return None;
        }
        let mut taker_shadow_would_open = best_ask <= ceiling + 1e-9 && best_ask > price + 1e-9;
        let mut visible_completion_slack_ticks =
            ((open_pair_band - price - opp_ask) / tick).max(-10.0);
        let mut fill_distance_ticks = ((best_ask - price) / tick).max(0.0);
        let mut preference_score = visible_completion_slack_ticks - 0.60 * fill_distance_ticks;
        // 继续往死里干 全都要: p5=5 thin cap (from data low-seed clean ~0.24 risk, higher res) + boundary open bias (>240s = local open signal from high L2 depth var, tie boundary dataset 5m open/close) + ce25 low-px/last60.
        // Extend to slack, clip, price for nagi (p5 cap part on thin books like low-seed days).
        if pgt_tuning().profile == PgtShadowProfile::Nagi777V1 {
            let p5_thin_risk = get_p5_thin_risk(pgt_tuning().nagi_5m_p5_risk_cap, input.l2_depth); // V1 p5=5 thin book cap (low seed days clean drops to 0.24, res up)
            visible_completion_slack_ticks *= p5_thin_risk;
            let b_open = pgt_tuning().nagi_boundary_open_boost;
            if remaining_secs > 240 {
                visible_completion_slack_ticks += 0.5 * b_open; // boundary open bias (high depth early proxy from L2 var 2279 std, tie dataset)
                preference_score += 0.3 * b_open; // boost for 5m open timing
            }
            // ═══════ OVERFITTING-CORRECTED PRICE GATING (2026-06-12) ═══════
            // WARNING: gen-3/gen-4 "champions" had only 2 seed_actions — NOT statistically reliable.
            // Reliable conclusions (28-50+ actions): pc=0.950 > pc=0.970, 0.20-0.35 is the safe range.
            // Q4 (0.3125-0.35) had 7-9 actions — directionally useful but not definitive.
            // Apply MILD preferences, not aggressive boosts. Let coverage grow before tightening.
            if price > NAGI_CE25_UPPER_ALPHA_BAND_MAX + 1e-9 && remaining_secs < 60 {
                visible_completion_slack_ticks *= 0.90; // outside 0.20-0.35: mild penalty (was 0.80)
            }
            // No strong penalty below Q4 — the 0.20-0.3125 zone still has alpha in 49-action baseline
            // Q4 zone gets a MILD preference (7-9 actions directionally positive, not definitive)
            if price >= NAGI_CE25_Q4_ALPHA_CORE_LO && price <= NAGI_CE25_Q4_ALPHA_CORE_HI {
                visible_completion_slack_ticks += 0.1; // mild Q4 preference (not the 0.5 from overfitted gen-3)
                preference_score += 0.05; // mild (not the 0.35 from overfitted gen-3)
            }
            if price > 0.65 && remaining_secs < 60 { // high price stop (100+ actions, reliable)
                visible_completion_slack_ticks *= 0.75;
            }
            // NAGI_FASTPAIR UP-side (0.35-0.50): 32-38 actions, moderate reliability.
            if price >= NAGI_FASTPAIR_UP_ALPHA_LO && price <= NAGI_FASTPAIR_UP_ALPHA_HI && remaining_secs < 60 {
                visible_completion_slack_ticks += 0.1; // mild boost (32 actions)
                preference_score += 0.05;
            }
            // p5 thin cap extended to clip for nagi (aggressive on high-seed open days ~23, cap on low-seed thin)
            if p5_thin_risk < 0.8 {
                // clip already in ladder, but slack cap above protects
            }
        }
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
        let tuning = pgt_tuning();
        if price > EXPENSIVE_SEED_PRICE + 1e-9
            && visible_completion_slack_ticks < tuning.expensive_seed_min_visible_slack_ticks
        {
            return None;
        }
        let visible_breakeven_completion_slack_ticks = ((1.0 - price - opp_ask) / tick).max(-10.0);
        let visible_taker_completion_ok =
            price + opp_ask <= open_pair_band + 1e-9;
        if tuning.entry_requires_pair_cap && !visible_taker_completion_ok {
            quotes.note_pgt_seed_reject_no_visible_breakeven_path();
            return None;
        }
        let uncertainty_gate = if tuning.profile == PgtShadowProfile::Nagi777V1 {
            let slug = coordinator.cfg().slug.as_deref();
            let round_end_ts = coordinator.cfg().market_end_ts;
            crate::polymarket::strategy::get_boundary_uncertainty_with_fallback(slug, round_end_ts)
        } else {
            false
        };
        if tuning.profile == PgtShadowProfile::Nagi777V1 {
            // Opposite depth gate (oq10): reject if L2 depth is too thin (< 50.0)
            if let Some(depth) = input.l2_depth {
                let min_depth = depth.bid_depth_5lvl.min(depth.ask_depth_5lvl);
                if min_depth < 50.0 {
                    quotes.note_pgt_seed_reject_no_visible_breakeven_path();
                    return None;
                }
            }
            let effective_taker_close_pair_cap = if price >= NAGI_FASTPAIR_UP_ALPHA_LO && price <= NAGI_FASTPAIR_UP_ALPHA_HI {
                tuning.taker_close_pair_cap.max(NAGI_FASTPAIR_UP_PC_CAP)
            } else {
                tuning.taker_close_pair_cap
            };
            if price + opp_ask > effective_taker_close_pair_cap + 1e-9 {
                quotes.note_pgt_seed_reject_no_visible_breakeven_path();
                return None;
            }
            if uncertainty_gate && !visible_taker_completion_ok {
                quotes.note_pgt_seed_reject_no_visible_breakeven_path();
                return None;
            }
        }
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
        if pgt_xuan_ladder_first_seed_price_blocks(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            price,
        ) {
            pgt_maybe_log_seed_admission_diag(
                coordinator.cfg().dry_run,
                tuning,
                side,
                remaining_secs,
                "blocked_first_seed_price_risk",
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
        if pgt_xuan_ladder_maker_only_seed_price_blocks(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            price,
            visible_taker_completion_ok,
        ) {
            pgt_maybe_log_seed_admission_diag(
                coordinator.cfg().dry_run,
                tuning,
                side,
                remaining_secs,
                "blocked_maker_only_seed_price",
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
        let preserve_seed_clip_qty = tuning.preserve_seed_clip_qty && visible_taker_completion_ok;
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
        if tuning.profile == PgtShadowProfile::Nagi777V1 && !visible_taker_completion_ok {
            // Gen-10 champion non_instant_clip_factor = 0.25 (scale target_qty down)
            size = quantize_tenth(tuning.fixed_clip_qty.unwrap_or(tuning.base_clip_qty) * 0.25);
        }
        if pgt_xuan_ladder_maker_only_seed_clip_caps(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            visible_taker_completion_ok,
        ) {
            size = size.min(XUAN_LADDER_MAKER_ONLY_SEED_CLIP_QTY);
        }
        if pgt_xuan_ladder_first_seed_risk_clip_caps(
            tuning,
            input.episode_metrics.round_buy_fill_count,
            price,
        ) {
            size = size.min(XUAN_LADDER_MAKER_ONLY_SEED_CLIP_QTY);
        }
        if size <= 0.0 {
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
        seed.intent.price > XUAN_LADDER_MAKER_ONLY_SEED_MAX_PRICE + 1e-9
            && !seed.visible_taker_completion_ok
    }

    // Aggressive xuan absorption (from deep dive + V1.1): only open new first leg when residual is low.
    // This is the heart of xuan's "always pairs successfully" — low-residual seed, then completion-only repair.
    fn xuan_ladder_residual_blocks_new_seed(input: &StrategyTickInput) -> bool {
        if let Some(active) = input.pair_ledger.active_tranche {
            if active.residual_qty > XUAN_LADDER_MAX_RESIDUAL_FOR_NEW_SEED {
                return true;
            }
        }
        false
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

        // Nagi777 absorption (5m specialist): for 5m slugs, bias entry/completion with local/self-built price agg hints (open/close boundaries).
        // This enables higher safe participation (early seeds on strong local signal) while keeping pair cost low and residual controlled.
        // Only when profile == Nagi777V1 or 5m + local enabled. Ties directly to project's self-built price agg work.
        // V1 L2 + boundary evidence preferred for "visible slack". Health check + manifest only for validation.
        let (early_pair_cap, late_pair_cap, taker_close_pair_cap) =
            pgt_effective_completion_pair_caps(tuning, remaining_secs, completion_age_secs, active.first_vwap);
        let mut positive_edge_ceiling = early_pair_cap - active.first_vwap + repair_budget_per_share;
        let mut urgency_ceiling = positive_edge_ceiling + urgency_shadow;
        // Urgency can spend remaining edge, but only realized repair budget may
        // cross the profile's late pair-cost cap.
        let funded_loss_ceiling = late_pair_cap - active.first_vwap + repair_budget_per_share;
        let base_taker_close_ceiling =
            taker_close_pair_cap - active.first_vwap + repair_budget_per_share;
        let tail_insurance_ceiling =
            pgt_tail_insurance_completion_ceiling(tuning, active.first_vwap, remaining_secs);

        // Nagi777 5m + local price fusion hook (crazy absorption - 往死里干, parallel full L2 + local):
        // For Nagi777V1, bias using "local" boost (from V1 boundary/self-built agg confidence + L2 book depth for visible slack).
        // Higher local/L2 -> relax entry for high participation (nagi volume from V1 30338 5m /4334 BTC5m + rescore 84k seeds), tighten completion for low cost/residual (real ~0.89 pair, 0.025 res_share).
        // Explosive parallel: use L2 depth (md_book_l2_top_aligned: BTC L1_sz~461, 5lvl~1460) to compute better slack for nagi 5m entry pressure; local for uncertainty gate (boundary script).
        // Ties to local_agg uncertainty: if local high conf + L2 tight (deep visible), boost part; else protect residual.
        // ce25_nagi prior V1 search (last60 down/tail, pc0.97-0.99) feeds future grid.
        if tuning.profile == PgtShadowProfile::Nagi777V1 {
            let b = tuning.nagi_local_boost;
            positive_edge_ceiling -= b * 0.004; // stronger low cost bias
            urgency_ceiling += b * 0.003; // more part boost
            // Real L2 depth "true feed" proxy + boundary dataset integration (parallel 猛干 per suggestion): V1 blast (L2 p5=5 thin book risk / p95~1240 / med d5 831 from md_book_l2_top_aligned quantiles for slack calc; rescore 5m BTC daily var seeds~19.5 18.9-20.3, res_sh~0.025, clean_r~0.439, pair~0.89) + current state + ce25 (NAGI_CE25_LAST60_TAIL_SECS=60, LOW_PX_TAIL_MAX=0.35).
            // Dynamic real per 5m (300s round): last60 tail control (remaining <60: tighter clean; else early boundary open + local uncertainty for part). Tie explicitly to local_agg boundary dataset (build_local_agg_boundary_dataset.py for 5m open/close timing from external sources) + V1 5m.
            // L2 true proxy: compute thin_book_risk from V1 p5=5 (cap part on thin like p5 risk) + current res/slack proxy (book best_ask/first_vwap as visible depth stand-in until full L2 in StrategyTickInput).
            // Low res + high slack = deep state => scale boost; p5 thin caps aggressive nagi. Low px tail guard.
            // Ties to PM_LOCAL_AGG_UNCERTAINTY_GATE + boundary signals. Good state (res<15, open or last60) relax for volume; else protect xuan rescore baseline.
            let res_factor = (15.0 - active.residual_qty.min(15.0)) / 15.0;
            let is_last60 = remaining_secs <= NAGI_CE25_LAST60_TAIL_SECS;
            let is_boundary_open = remaining_secs > (300 - NAGI_CE25_LAST60_TAIL_SECS);
            let uncertainty_gate = {
                let slug = coordinator.cfg().slug.as_deref();
                let round_end_ts = coordinator.cfg().market_end_ts;
                crate::polymarket::strategy::get_boundary_uncertainty_with_fallback(slug, round_end_ts)
            };
            let last60_f = tuning.nagi_5m_last60_focus;
            let p5_cap = tuning.nagi_5m_p5_risk_cap;
            let boundary_boost = tuning.nagi_boundary_open_boost; // high remaining open
            // L2 p5 thin risk proxy + ce25 full (true feed until L2 in input; p5=5 cap for nagi high part on thin 5m; ce25 last60/low-px/high-stop from 14 policies)
            let thin_book_risk = get_p5_thin_risk(p5_cap, input.l2_depth);
            let boundary_uncertainty_boost = if (is_boundary_open || is_last60) || uncertainty_gate { 0.003 * b + last60_f * 0.002 + boundary_boost * 0.001 } else { 0.0 }; // boundary dataset 5m open (high remaining) / close (last60) + uncertainty + ce25 + tuning
            let _l2_depth_base = 831.0; // V1 med d5
            let _l2_depth_dynamic = (0.001 * (1.0 + b * 2.0) * (0.5 + res_factor) + boundary_uncertainty_boost + (if is_last60 { -0.001 * (1.0 + last60_f) } else { 0.0 })) * thin_book_risk;
            // p5 thin multiplier on urgency for nagi (aggressive on high-seed open ~23, cap on low-seed thin)
            if thin_book_risk < 0.8 {
                urgency_ceiling *= thin_book_risk + 0.2; // extra cap on thin
            }
            // Low px tail (ce25) + good state extra
            if active.first_vwap < NAGI_CE25_LOW_PX_TAIL_MAX && (is_boundary_open || is_last60) {
                urgency_ceiling += 0.002;
            }
            if active.residual_qty < 10.0 && (is_boundary_open || is_last60) {
                urgency_ceiling += 0.002 * thin_book_risk;
            }
        }
        let taker_insurance_ceiling = pgt_xuan_ladder_taker_insurance_completion_ceiling(
            tuning,
            active.first_vwap,
            remaining_secs,
            completion_age_secs,
        );

        // Explosive nagi/PNL wisdom (往死里干 all wisdom): multi-objective scorer for V1 search (low cost + low residual + high part).
        // For Nagi777V1, log to guide tuning from V1 5m data (high part possible with gates).
        // Score = w_cost*(1-pair_cap) + w_res*(1-residual_norm) + w_part*part_proxy. Default nagi weights bias part (volume) but res heavy for low residual.
        // Conflicts: high part (early entry) risks res/cost -> gates + local boost resolve.
        // Use in shadow diagnostics. Emit to V1 for param search (e.g. grid on boost, clip, caps to max score under constraints like pair_p90<1.03, clean>0.9).
        if tuning.profile == PgtShadowProfile::Nagi777V1 {
            let cost_score = 1.0 - early_pair_cap; // lower cap = low cost
            let res_norm = (active.residual_qty / 100.0).clamp(0.0, 1.0);
            let res_score = 1.0 - res_norm; // low residual
            let part_proxy = ((250.0 - remaining_secs as f64) / 250.0).max(0.0); // earlier = higher part for 5m (V1 high 5m vol)
            // Nagi-tuned weights: higher on part (his volume style), but res to protect residual rate.
            let nagi_pnl_score = 0.25 * cost_score + 0.40 * res_score + 0.35 * part_proxy;
            // TODO: full V1 integration - use actual pair_cost from ledger, clean_closed from events, part from round count.
            // For now, if high score, candidate for V1 5m validation / better profile.
            if nagi_pnl_score > 0.75 {
                // high score - log for search
            }
        }
        let taker_close_ceiling = base_taker_close_ceiling
            .max(tail_insurance_ceiling.unwrap_or(0.0))
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

        let raw_size = if remaining_secs <= COMPLETION_FULL_RESIDUAL_REMAINING_SECS {
            active.residual_qty.max(0.0)
        } else {
            self.adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
                .min(active.residual_qty.max(0.0))
        };
        let size = raw_size.min(active.residual_qty.max(0.0));
        let size = quantize_tenth(size);
        if size <= 0.0 {
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
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs, Some(active.first_vwap));
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

pub(crate) fn pgt_effective_open_pair_band_value(
    base: f64,
    remaining_secs: u64,
    price_hint: Option<f64>,
) -> f64 {
    let tuning = pgt_tuning();
    if tuning.profile != PgtShadowProfile::Legacy {
        let mut cap = tuning.open_pair_band_cap.unwrap_or(base);
        if tuning.profile == PgtShadowProfile::Nagi777V1 {
            if let Some(p) = price_hint {
                if p >= NAGI_FASTPAIR_UP_ALPHA_LO && p <= NAGI_FASTPAIR_UP_ALPHA_HI {
                    cap = NAGI_FASTPAIR_UP_PC_CAP;
                }
            }
        }
        if tuning.profile == PgtShadowProfile::XuanLadderV1 {
            base.max(cap)
        } else {
            base.min(cap)
        }
    } else {
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
    first_vwap: f64,
) -> (f64, f64, f64) {
    let mut default_early = tuning.completion_early_pair_cap.clamp(0.0, 1.20);
    let mut default_late = tuning
        .completion_late_pair_cap
        .max(default_early)
        .clamp(0.0, 1.20);
    let mut default_taker = tuning
        .taker_close_pair_cap
        .min(default_late)
        .clamp(0.0, 1.20);

    if tuning.profile == PgtShadowProfile::Nagi777V1 {
        // Price-aware pair cap gating (joint optimization wave):
        // If first_vwap is in the UP-side fastpair zone (0.35-0.50), we relax the pair cap to 0.965
        // (as gen-3 fastpair optimal), because UP-side alpha naturally runs higher costs.
        // If it is in the DOWN-side tail (0.20-0.35), we keep the extremely tight 0.930 cap.
        if first_vwap >= NAGI_FASTPAIR_UP_ALPHA_LO && first_vwap <= NAGI_FASTPAIR_UP_ALPHA_HI {
            default_early = NAGI_FASTPAIR_UP_PC_CAP;
            default_late = default_late.max(NAGI_FASTPAIR_UP_PC_CAP);
            default_taker = default_taker.max(NAGI_FASTPAIR_UP_PC_CAP - 0.01);
        }
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
    if seed_price > XUAN_LADDER_LOW_PRICE_SEED_MAX + 1e-9
        && seed_price <= EXPENSIVE_SEED_PRICE + 1e-9
    {
        return true;
    }
    let taker_pair_cost = seed_price + opposite_ask;
    if taker_pair_cost <= XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP + 1e-9 {
        return false;
    }
    let maker_completion_ref = (opposite_ask - tick).max(0.0);
    let maker_pair_cost = seed_price + maker_completion_ref;
    seed_price > EXPENSIVE_SEED_PRICE + 1e-9
        || maker_pair_cost > XUAN_LADDER_SEED_MAKER_COMPLETION_PAIR_CAP + 1e-9
}

fn pgt_xuan_ladder_maker_only_seed_price_blocks(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    seed_price: f64,
    visible_taker_completion_ok: bool,
) -> bool {
    if tuning.profile == PgtShadowProfile::XuanLadderV1
        && round_buy_fill_count == 0
        && !visible_taker_completion_ok
        && seed_price > XUAN_LADDER_MAKER_ONLY_SEED_MAX_PRICE + 1e-9
    {
        return true;
    }
    // Parallel ce25 nagi 5m: low px tail + last60 maker only guard for nagi (favor tail down, control late round risk per V1 search).
    if tuning.profile == PgtShadowProfile::Nagi777V1
        && round_buy_fill_count == 0
        && !visible_taker_completion_ok
        && seed_price > NAGI_FASTPAIR_UP_ALPHA_HI + 1e-9
    {
        return true;
    }
    false
}

fn pgt_xuan_ladder_first_seed_price_blocks(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    seed_price: f64,
) -> bool {
    if tuning.profile == PgtShadowProfile::XuanLadderV1
        && round_buy_fill_count == 0
        && seed_price > XUAN_LADDER_FIRST_SEED_MAX_PRICE + 1e-9
    {
        return true;
    }
    // Parallel ce25 nagi blast for 5m: low px tail guard (NAGI_FASTPAIR_UP_ALPHA_HI) + last60 focus.
    // For nagi, block high price first seeds to favor low-px tail down policies from V1 search; last60 tightens risk.
    if tuning.profile == PgtShadowProfile::Nagi777V1
        && round_buy_fill_count == 0
        && seed_price > NAGI_FASTPAIR_UP_ALPHA_HI + 1e-9
    {
        return true;
    }
    false
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

fn pgt_xuan_ladder_first_seed_risk_clip_caps(
    tuning: PgtTuning,
    round_buy_fill_count: u64,
    seed_price: f64,
) -> bool {
    tuning.profile == PgtShadowProfile::XuanLadderV1
        && round_buy_fill_count == 0
        && seed_price > XUAN_LADDER_FIRST_SEED_FULL_CLIP_MAX_PRICE + 1e-9
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
    let guard_pair_cost_gap =
        (visible_ask_pair_cost - XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP).max(0.0);
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
        XUAN_LADDER_SEED_TAKER_COMPLETION_PAIR_CAP,
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
            pgt_effective_completion_pair_caps(tuning, 280, 5.0, 0.25),
            (0.990, 0.990, 0.990)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 260, 30.0, 0.25),
            (0.995, 0.995, 0.995)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 180, 70.0, 0.25),
            (1.000, 1.000, 1.000)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 120, 95.0, 0.25),
            (1.000, 1.010, 1.010)
        );
    }

    #[test]
    fn xuan_ladder_completion_caps_do_not_unlock_unfunded_tail_repair() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 8.0, 0.25),
            (0.990, 1.000, 1.000)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 119.0, 0.25),
            (1.000, 1.010, 1.010)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 40, 120.0, 0.25),
            (1.000, 1.010, 1.010)
        );
        assert_eq!(
            pgt_effective_completion_pair_caps(tuning, 80, 8.0, 0.25),
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
            (pgt_effective_repair_budget_per_share(tuning, 10.0, 120.0, 180, 95.0) - 0.020)
                .abs()
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
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.53, 0.01),
            "ambiguous 0.45-0.50 first-leg band is blocked until a distinct high-quality branch exists"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.52, 0.01),
            "ambiguous 0.45-0.50 first-leg band is blocked even when immediate taker completion looks safe"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.47, 0.54, 0.01),
            "cheap first leg is blocked when even opposite maker completion is above the pair-cost cap"
        );
        assert!(
            pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.66, 0.34, 0.01),
            "expensive first leg still requires an immediate taker-safe completion path"
        );
        assert!(
            !pgt_xuan_ladder_seed_visible_completion_guard_blocks(tuning, 0, 0.63, 0.36, 0.01),
            "expensive first leg remains allowed when current opposite ask offers a visible positive-edge completion path"
        );
    }

    #[test]
    fn xuan_ladder_maker_only_first_seed_is_capped_or_blocked() {
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
            !pgt_xuan_ladder_maker_only_seed_price_blocks(tuning, 0, 0.35, false),
            "deep first-leg discounts can still be sampled, but with capped clip"
        );
        assert!(
            pgt_xuan_ladder_maker_only_seed_price_blocks(tuning, 0, 0.45, false),
            "high maker-only first legs caused large residual tails and are now blocked"
        );
        assert!(
            !pgt_xuan_ladder_maker_only_seed_price_blocks(tuning, 1, 0.45, false),
            "after the first fill, reopen-specific guards own the path"
        );
        assert!(
            !pgt_xuan_ladder_maker_only_seed_price_blocks(PgtTuning::legacy(), 0, 0.45, false),
            "legacy/replay profiles are unaffected"
        );
    }

    #[test]
    fn xuan_ladder_first_seed_price_risk_is_capped_or_blocked() {
        let tuning = PgtTuning::xuan_ladder_v1();
        assert!(
            !pgt_xuan_ladder_first_seed_price_blocks(tuning, 0, 0.36),
            "post-fix shadow sample stayed worst-case positive through 0.36 first-leg price"
        );
        assert!(
            pgt_xuan_ladder_first_seed_price_blocks(tuning, 0, 0.37),
            "first-leg prices above 0.36 created persistent residual tail losses"
        );
        assert!(
            !pgt_xuan_ladder_first_seed_price_blocks(tuning, 1, 0.44),
            "after first fill, completion/reopen-specific guards own the path"
        );
        assert!(
            pgt_xuan_ladder_first_seed_risk_clip_caps(tuning, 0, 0.35),
            "marginal first-leg prices are allowed only at the minimum clip"
        );
        assert!(
            !pgt_xuan_ladder_first_seed_risk_clip_caps(tuning, 0, 0.34),
            "deep first-leg discounts can keep the ladder clip"
        );
        assert!(
            !pgt_xuan_ladder_first_seed_price_blocks(PgtTuning::legacy(), 0, 0.44),
            "legacy/replay profiles are unaffected"
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
            l2_depth: None,
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
            pair_cost_tranche: 0.990,
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
            l2_depth: None,
        };
        let tuning = PgtTuning::xuan_ladder_v1();

        assert!(pgt_blocks_reopen_after_closed_pair(tuning, input, false));

        let mut high_quality_ledger = ledger;
        high_quality_ledger.recent_closed[0] = Some(PairTranche {
            pairable_qty: 160.0,
            pair_cost_tranche: 0.880,
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
            !pgt_xuan_ladder_reopen_seed_quality_blocks(
                tuning,
                2,
                Some(0.980),
                0.46,
                0.53,
                0.01
            ),
            "second tranche can open when the prior pair is cheap and visible maker completion is <= 0.980"
        );
        assert!(
            pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 2, Some(0.980), 0.47, 0.54, 0.01),
            "second tranche is blocked when projected maker pair cost is above the cap"
        );
        assert!(
            pgt_xuan_ladder_reopen_seed_quality_blocks(tuning, 2, Some(0.990), 0.47, 0.52, 0.01),
            "second tranche is blocked when the just-closed pair was not clearly profitable"
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
    fn xuan_ladder_selection_blocks_high_dual_seed_without_taker_completion() {
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

    #[test]
    fn nagi_uncertainty_gate_maker_vs_taker() {
        use crate::polymarket::coordinator::{CoordinatorConfig, StrategyCoordinator};
        use crate::polymarket::strategy::{Book, StrategyInventoryMetrics, StrategyKind, StrategyQuotes};
        use crate::polymarket::messages::{InventorySnapshot, InventoryState, MarketDataMsg, OfiSnapshot};
        use crate::polymarket::pair_ledger::EpisodeMetrics;
        use tokio::sync::{mpsc, watch};
        use std::time::Instant;

        // Set test tuning to Nagi777V1:
        let mut tuning = PgtTuning::nagi777_v1();
        // Override parameters to restore the exact environment for the uncertainty gate test:
        tuning.open_pair_band_cap = Some(0.930);
        tuning.taker_close_pair_cap = 0.980;
        TEST_TUNING.with(|cell| cell.set(Some(tuning)));

        // Enable uncertainty gate:
        std::env::set_var("PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED", "true");

        let cfg = CoordinatorConfig {
            strategy: StrategyKind::PairGatedTrancheArb,
            slug: Some("btc-updown-5m".to_string()),
            market_end_ts: Some(1700000000),
            tick_size: 0.01,
            open_pair_band: 0.985,
            ..CoordinatorConfig::default()
        };

        let (_ofi_tx, ofi_rx) = watch::channel(OfiSnapshot::default());
        let (_inv_tx, inv_rx) = watch::channel(InventorySnapshot::default());
        let (_md_tx, md_rx) = watch::channel(MarketDataMsg::BookTick {
            yes_bid: 0.40,
            yes_ask: 0.41,
            no_bid: 0.59,
            no_ask: 0.60,
            depth: None,
            ts: Instant::now(),
        });
        let (om_tx, _om_rx) = mpsc::channel(16);
        let (_kill_tx, kill_rx) = mpsc::channel(16);
        let coordinator = StrategyCoordinator::with_kill_rx(cfg, ofi_rx, inv_rx, md_rx, om_tx, kill_rx);

        let inv = InventoryState::default();
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
        let ledger = PairLedgerSnapshot::default();
        let metrics = EpisodeMetrics::default();
        let inventory = InventorySnapshot::default();

        let strategy = PairGatedTrancheStrategy;

        // 1. Taker-completable seed case (price + opp_ask <= open_pair_band):
        // price = 0.30, opp_ask = 0.62. price + opp_ask = 0.92 <= 0.930.
        // This is taker completable, so it should NOT be blocked by the uncertainty gate.
        let book_taker = Book {
            yes_bid: 0.30,
            yes_ask: 0.31,
            no_bid: 0.61,
            no_ask: 0.62,
        };
        let input_taker = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &ledger,
            episode_metrics: &metrics,
            book: &book_taker,
            metrics: &strat_metrics,
            ofi: None,
            glft: None,
            l2_depth: None,
        };

        let mut quotes = StrategyQuotes::default();
        let res_taker = strategy.flat_seed_intent_for_side(
            &coordinator,
            input_taker,
            Side::Yes,
            0.30,
            130.0,
            &mut quotes,
        );
        assert!(res_taker.is_some(), "Taker-completable seed should be allowed even when uncertainty gate is active");

        // 2. Maker seed case (price + opp_ask > open_pair_band):
        // price = 0.31, opp_ask = 0.63. price + opp_ask = 0.94 > 0.930.
        // This is NOT taker completable (it is a maker seed), so it should be blocked by the uncertainty gate.
        let book_maker = Book {
            yes_bid: 0.31,
            yes_ask: 0.32,
            no_bid: 0.62,
            no_ask: 0.63,
        };
        let input_maker = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &ledger,
            episode_metrics: &metrics,
            book: &book_maker,
            metrics: &strat_metrics,
            ofi: None,
            glft: None,
            l2_depth: None,
        };

        let mut quotes_maker = StrategyQuotes::default();
        let res_maker = strategy.flat_seed_intent_for_side(
            &coordinator,
            input_maker,
            Side::Yes,
            0.31,
            130.0,
            &mut quotes_maker,
        );
        assert!(res_maker.is_none(), "Maker seed should be blocked when uncertainty gate is active");

        // 3. Maker seed case with uncertainty gate INACTIVE:
        // Should be allowed because entry_requires_pair_cap is false for Nagi777V1.
        std::env::set_var("PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED", "false");
        let mut quotes_maker_inactive = StrategyQuotes::default();
        let res_maker_inactive = strategy.flat_seed_intent_for_side(
            &coordinator,
            input_maker,
            Side::Yes,
            0.31,
            130.0,
            &mut quotes_maker_inactive,
        );
        assert!(res_maker_inactive.is_some(), "Maker seed should be allowed when uncertainty gate is inactive");

        // Clean up:
        TEST_TUNING.with(|cell| cell.set(None));
        std::env::remove_var("PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED");
    }
}
