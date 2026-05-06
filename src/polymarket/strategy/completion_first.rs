use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use chrono::Timelike;
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche, TrancheState};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const MAX_SAME_SIDE_RUN: u32 = 1;
const BASE_CLIP: f64 = 150.0;
const MAX_CLIP: f64 = 250.0;
const MIN_CLIP: f64 = 45.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;

pub(crate) struct CompletionFirstStrategy;

pub(crate) static COMPLETION_FIRST_STRATEGY: CompletionFirstStrategy = CompletionFirstStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionFirstPhase {
    #[default]
    FlatSeed,
    CompletionOnly,
    HarvestWindow,
    PostResolve,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionFirstClipMultipliers {
    pub blocked: f64,
    pub half_clip: f64,
    pub full_clip: f64,
    pub upclip: f64,
}

impl Default for CompletionFirstClipMultipliers {
    fn default() -> Self {
        Self {
            blocked: 0.0,
            half_clip: 0.5,
            full_clip: 1.0,
            upclip: 1.25,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionFirstHardBlockRules {
    pub max_same_side_run_before_open: u32,
    pub max_opposite_side_spread_ticks: f64,
    pub max_book_age_ms: u64,
    pub min_session_mult_for_open: f64,
    pub min_recent_overlap_episodes: u64,
}

impl Default for CompletionFirstHardBlockRules {
    fn default() -> Self {
        Self {
            max_same_side_run_before_open: 1,
            max_opposite_side_spread_ticks: 3.0,
            max_book_age_ms: 500,
            min_session_mult_for_open: 0.75,
            min_recent_overlap_episodes: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionFirstGateRangeBucket {
    pub label: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub score: i32,
}

impl CompletionFirstGateRangeBucket {
    fn matches(&self, value: f64) -> bool {
        if let Some(min) = self.min {
            if value < min {
                return false;
            }
        }
        if let Some(max) = self.max {
            if value > max {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompletionFirstResearchWindow {
    pub full_start_ts: Option<u64>,
    pub full_end_ts: Option<u64>,
    pub recent_start_ts: Option<u64>,
    pub recent_end_ts: Option<u64>,
    pub train_episode_count: u64,
    pub holdout_episode_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompletionFirstCoverageStats {
    pub full_episode_count: u64,
    pub full_overlap_episode_count: u64,
    pub recent_episode_count: u64,
    pub recent_overlap_episode_count: u64,
    pub holdout_gate_on_completion_rate: f64,
    pub holdout_baseline_completion_rate: f64,
    pub holdout_lift_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionFirstGateDefaults {
    pub score_threshold_block: i32,
    pub score_threshold_half_clip: i32,
    pub score_threshold_full_clip: i32,
    pub score_threshold_upclip: i32,
    pub clip_mult_by_score_bucket: CompletionFirstClipMultipliers,
    pub session_mult_by_utc_hour: Vec<f64>,
    pub hard_block_rules: CompletionFirstHardBlockRules,
    pub feature_bucket_defs: BTreeMap<String, Vec<CompletionFirstGateRangeBucket>>,
    pub research_window: CompletionFirstResearchWindow,
    pub coverage_stats: CompletionFirstCoverageStats,
    pub provisional: bool,
}

impl Default for CompletionFirstGateDefaults {
    fn default() -> Self {
        let mut feature_bucket_defs = BTreeMap::new();
        feature_bucket_defs.insert(
            "round_close_rel_s".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "le_15".to_string(),
                    min: None,
                    max: Some(15.0),
                    score: 2,
                },
                CompletionFirstGateRangeBucket {
                    label: "15_60".to_string(),
                    min: Some(15.0),
                    max: Some(60.0),
                    score: 2,
                },
                CompletionFirstGateRangeBucket {
                    label: "60_120".to_string(),
                    min: Some(60.0),
                    max: Some(120.0),
                    score: 1,
                },
                CompletionFirstGateRangeBucket {
                    label: "120_180".to_string(),
                    min: Some(120.0),
                    max: Some(180.0),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "gt_180".to_string(),
                    min: Some(180.0),
                    max: None,
                    score: -1,
                },
            ],
        );
        feature_bucket_defs.insert(
            "prior_imbalance".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "lt_0.02".to_string(),
                    min: None,
                    max: Some(0.02),
                    score: 2,
                },
                CompletionFirstGateRangeBucket {
                    label: "0.02_0.05".to_string(),
                    min: Some(0.02),
                    max: Some(0.05),
                    score: 1,
                },
                CompletionFirstGateRangeBucket {
                    label: "0.05_0.08".to_string(),
                    min: Some(0.05),
                    max: Some(0.08),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "gt_0.08".to_string(),
                    min: Some(0.08),
                    max: None,
                    score: -1,
                },
            ],
        );
        feature_bucket_defs.insert(
            "l1_spread_ticks_first_side".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "le_1.5".to_string(),
                    min: None,
                    max: Some(1.5),
                    score: 2,
                },
                CompletionFirstGateRangeBucket {
                    label: "1.5_3".to_string(),
                    min: Some(1.5),
                    max: Some(3.0),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "gt_3".to_string(),
                    min: Some(3.0),
                    max: None,
                    score: -2,
                },
            ],
        );
        feature_bucket_defs.insert(
            "l1_spread_ticks_opposite_side".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "le_1.5".to_string(),
                    min: None,
                    max: Some(1.5),
                    score: 2,
                },
                CompletionFirstGateRangeBucket {
                    label: "1.5_3".to_string(),
                    min: Some(1.5),
                    max: Some(3.0),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "gt_3".to_string(),
                    min: Some(3.0),
                    max: None,
                    score: -2,
                },
            ],
        );
        feature_bucket_defs.insert(
            "mid_skew_to_opposite".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "near_zero".to_string(),
                    min: Some(-0.02),
                    max: Some(0.02),
                    score: 1,
                },
                CompletionFirstGateRangeBucket {
                    label: "mild".to_string(),
                    min: Some(-0.05),
                    max: Some(0.05),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "extreme".to_string(),
                    min: None,
                    max: None,
                    score: -1,
                },
            ],
        );
        feature_bucket_defs.insert(
            "buy_fill_count".to_string(),
            vec![
                CompletionFirstGateRangeBucket {
                    label: "le_1".to_string(),
                    min: None,
                    max: Some(1.0),
                    score: 1,
                },
                CompletionFirstGateRangeBucket {
                    label: "2_3".to_string(),
                    min: Some(2.0),
                    max: Some(3.0),
                    score: 0,
                },
                CompletionFirstGateRangeBucket {
                    label: "gt_3".to_string(),
                    min: Some(4.0),
                    max: None,
                    score: -1,
                },
            ],
        );
        let mut session_mult_by_utc_hour = vec![1.0; 24];
        for (hour, value) in session_mult_by_utc_hour.iter_mut().enumerate() {
            *value = if (16..=22).contains(&hour) {
                1.15
            } else if (3..=12).contains(&hour) {
                0.80
            } else {
                1.00
            };
        }
        Self {
            score_threshold_block: 0,
            score_threshold_half_clip: 2,
            score_threshold_full_clip: 4,
            score_threshold_upclip: 5,
            clip_mult_by_score_bucket: CompletionFirstClipMultipliers::default(),
            session_mult_by_utc_hour,
            hard_block_rules: CompletionFirstHardBlockRules::default(),
            feature_bucket_defs,
            research_window: CompletionFirstResearchWindow::default(),
            coverage_stats: CompletionFirstCoverageStats::default(),
            provisional: true,
        }
    }
}

impl CompletionFirstGateDefaults {
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).with_context(|| {
            format!(
                "failed reading completion_first gate defaults: {}",
                path.display()
            )
        })?;
        let parsed = serde_json::from_str::<Self>(&raw).with_context(|| {
            format!(
                "failed parsing completion_first gate defaults: {}",
                path.display()
            )
        })?;
        Ok(parsed)
    }

    fn session_mult(&self, hour: u8) -> f64 {
        self.session_mult_by_utc_hour
            .get(hour as usize)
            .copied()
            .unwrap_or(1.0)
    }

    fn feature_score(&self, key: &str, value: f64) -> i32 {
        self.feature_bucket_defs
            .get(key)
            .and_then(|buckets| buckets.iter().find(|bucket| bucket.matches(value)))
            .map(|bucket| bucket.score)
            .unwrap_or_default()
    }

    fn clip_mult_for_score(&self, score: i32) -> f64 {
        if score <= self.score_threshold_block {
            self.clip_mult_by_score_bucket.blocked
        } else if score <= self.score_threshold_half_clip {
            self.clip_mult_by_score_bucket.half_clip
        } else if score <= self.score_threshold_full_clip {
            self.clip_mult_by_score_bucket.full_clip
        } else {
            self.clip_mult_by_score_bucket.upclip
        }
    }

    fn score_bucket_label(&self, score: i32) -> &'static str {
        if score <= self.score_threshold_block {
            "blocked"
        } else if score <= self.score_threshold_half_clip {
            "half_clip"
        } else if score <= self.score_threshold_full_clip {
            "full_clip"
        } else {
            "upclip"
        }
    }

    fn truth_coverage_insufficient(&self) -> bool {
        self.coverage_stats.recent_overlap_episode_count
            < self.hard_block_rules.min_recent_overlap_episodes
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompletionFirstOpenDecision {
    pub side: Side,
    pub is_candidate: bool,
    pub allowed: bool,
    pub score: i32,
    pub score_bucket: &'static str,
    pub block_reason: &'static str,
    pub clip_mult: f64,
    pub session_mult: f64,
    pub final_clip: f64,
    pub base_clip: f64,
    pub book_age_ms: u64,
    pub hour_bucket: u8,
    pub prior_imbalance: f64,
    pub same_side_run_before_open: u32,
    pub l1_spread_ticks_first_side: f64,
    pub l1_spread_ticks_opposite_side: f64,
    pub mid_skew_to_opposite: f64,
    pub gate_source: &'static str,
    pub provisional: bool,
}

impl QuoteStrategy for CompletionFirstStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::CompletionFirst
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        if !coordinator.completion_first_market_enabled() {
            return StrategyQuotes::default();
        }

        let phase = coordinator.completion_first_phase();
        let pair_arb_quotes = StrategyKind::PairArb.compute_quotes(coordinator, input);
        let mut quotes = StrategyQuotes::default();

        if phase == CompletionFirstPhase::PostResolve {
            return quotes;
        }

        let ledger = &input.inventory.pair_ledger;
        if let Some(active) = ledger.active_tranche.filter(|t| t.residual_qty > 1e-9) {
            if let Some(intent) = self.completion_intent(coordinator, input, active) {
                quotes.set(intent);
            }
            if self.same_side_add_allowed(active, phase) {
                if let Some(first_side) = active.first_side {
                    if let Some(seed_intent) = pair_arb_quotes.buy_for(first_side) {
                        if let Some(intent) =
                            self.same_side_add_intent_with_clip(coordinator, input, seed_intent)
                        {
                            quotes.set(intent);
                        }
                    }
                }
            }
            return quotes;
        }

        if phase == CompletionFirstPhase::HarvestWindow {
            return quotes;
        }

        for side in [Side::Yes, Side::No] {
            let Some(seed_intent) = pair_arb_quotes.buy_for(side) else {
                continue;
            };
            let decision = self.evaluate_open_gate(coordinator, input, seed_intent);
            quotes.note_completion_first_open_eval(decision);
            if let Some(intent) = self.open_seed_intent_with_gate(seed_intent, decision) {
                quotes.set(intent);
            }
        }
        quotes
    }
}

impl CompletionFirstStrategy {
    pub(crate) fn clip_for(
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
    ) -> f64 {
        Self::clip_for_hour(
            coordinator,
            input,
            remaining_secs,
            chrono::Utc::now().hour(),
        )
    }

    fn clip_for_hour(
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
        hour: u32,
    ) -> f64 {
        let session_mult = if (16..=22).contains(&hour) {
            1.15
        } else if (3..=12).contains(&hour) {
            0.80
        } else {
            1.00
        };
        Self::clip_with_session_mult(coordinator, input, remaining_secs, session_mult)
    }

    fn clip_with_session_mult(
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
        session_mult: f64,
    ) -> f64 {
        let abs_imb = if coordinator.cfg().max_net_diff > 0.0 {
            (input.working_inv.net_diff.abs() / coordinator.cfg().max_net_diff).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let imbalance_mult = if abs_imb < 0.05 {
            1.00
        } else if abs_imb < 0.15 {
            0.95
        } else if abs_imb < 0.30 {
            1.00
        } else {
            1.20
        };
        let n = input.inventory.pair_ledger.buy_fill_count.max(1) as f64;
        let trade_index_mult = (1.0 - 0.05 * (n - 1.0)).max(0.70);
        let tail_mult = if remaining_secs <= 30 { 1.16 } else { 1.0 };
        round_clip(BASE_CLIP * session_mult * imbalance_mult * trade_index_mult * tail_mult)
    }

    fn open_base_clip(
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
    ) -> f64 {
        Self::clip_with_session_mult(coordinator, input, remaining_secs, 1.0)
    }

    fn open_seed_intent_with_gate(
        &self,
        seed_intent: StrategyIntent,
        decision: CompletionFirstOpenDecision,
    ) -> Option<StrategyIntent> {
        if !decision.allowed || decision.final_clip <= 0.0 {
            return None;
        }
        Some(StrategyIntent {
            size: decision.final_clip,
            ..seed_intent
        })
    }

    fn same_side_add_intent_with_clip(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        seed_intent: StrategyIntent,
    ) -> Option<StrategyIntent> {
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let clip = Self::clip_for(coordinator, input, remaining_secs);
        if clip <= 0.0 {
            return None;
        }
        Some(StrategyIntent {
            size: clip,
            ..seed_intent
        })
    }

    fn evaluate_open_gate(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        seed_intent: StrategyIntent,
    ) -> CompletionFirstOpenDecision {
        let defaults = &coordinator.cfg().completion_first.gate_defaults;
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let hour = chrono::Utc::now().hour() as u8;
        let base_clip = Self::open_base_clip(coordinator, input, remaining_secs);
        let session_mult = defaults.session_mult(hour);
        let tick = coordinator.cfg().tick_size.max(0.0001);
        let (first_bid, first_ask) = side_book(input.book, seed_intent.side);
        let (opp_bid, opp_ask) = side_book(input.book, opposite_side(seed_intent.side));
        let first_spread_ticks = spread_ticks(first_bid, first_ask, tick);
        let opposite_spread_ticks = spread_ticks(opp_bid, opp_ask, tick);
        let first_mid = mid(first_bid, first_ask);
        let opposite_mid = mid(opp_bid, opp_ask);
        let prior_imbalance = (first_mid + opposite_mid - 1.0).abs();
        let mid_skew_to_opposite = if first_mid > 0.0 && opposite_mid > 0.0 {
            first_mid - (1.0 - opposite_mid)
        } else {
            0.0
        };
        let same_side_run_before_open = 0u32;
        let book_age_ms = coordinator.completion_first_book_age_ms(seed_intent.side);
        let score = defaults.feature_score("prior_imbalance", prior_imbalance)
            + defaults.feature_score("round_close_rel_s", remaining_secs as f64)
            + defaults.feature_score("l1_spread_ticks_first_side", first_spread_ticks)
            + defaults.feature_score("l1_spread_ticks_opposite_side", opposite_spread_ticks)
            + defaults.feature_score("mid_skew_to_opposite", mid_skew_to_opposite)
            + defaults.feature_score(
                "buy_fill_count",
                input.inventory.pair_ledger.buy_fill_count as f64,
            );
        let clip_mult = defaults.clip_mult_for_score(score);
        let score_bucket = defaults.score_bucket_label(score);
        let mut block_reason = "allowed";
        if same_side_run_before_open > defaults.hard_block_rules.max_same_side_run_before_open {
            block_reason = "same_side_run_hard_block";
        } else if opposite_spread_ticks > defaults.hard_block_rules.max_opposite_side_spread_ticks {
            block_reason = "opposite_spread_hard_block";
        } else if book_age_ms > defaults.hard_block_rules.max_book_age_ms {
            block_reason = "book_age_hard_block";
        } else if defaults.truth_coverage_insufficient() {
            block_reason = "truth_coverage_insufficient";
        } else if session_mult < defaults.hard_block_rules.min_session_mult_for_open {
            block_reason = "session_hard_block";
        } else if clip_mult <= 0.0 || score <= defaults.score_threshold_block {
            block_reason = "score_blocked";
        }

        let mut final_clip = round_clip(base_clip * clip_mult * session_mult);
        if final_clip <= 0.0 && block_reason == "allowed" {
            block_reason = "clip_zero";
            final_clip = 0.0;
        }
        CompletionFirstOpenDecision {
            side: seed_intent.side,
            is_candidate: true,
            allowed: block_reason == "allowed",
            score,
            score_bucket,
            block_reason,
            clip_mult,
            session_mult,
            final_clip,
            base_clip,
            book_age_ms,
            hour_bucket: hour,
            prior_imbalance,
            same_side_run_before_open,
            l1_spread_ticks_first_side: first_spread_ticks,
            l1_spread_ticks_opposite_side: opposite_spread_ticks,
            mid_skew_to_opposite,
            gate_source: if coordinator
                .cfg()
                .completion_first
                .gate_defaults_path
                .is_some()
            {
                "loaded_json"
            } else {
                "built_in_default"
            },
            provisional: defaults.provisional,
        }
    }

    fn same_side_add_allowed(&self, active: PairTranche, phase: CompletionFirstPhase) -> bool {
        matches!(
            phase,
            CompletionFirstPhase::CompletionOnly | CompletionFirstPhase::FlatSeed
        ) && active.same_side_add_count < MAX_SAME_SIDE_RUN
            && active.state == TrancheState::CompletionOnly
    }

    fn completion_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
    ) -> Option<StrategyIntent> {
        let first_side = active.first_side?;
        let hedge_side = opposite_side(first_side);
        let (best_bid, best_ask) = match hedge_side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };
        if best_ask <= 0.0 || active.first_vwap <= 0.0 || active.residual_qty <= f64::EPSILON {
            return None;
        }

        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let clip = Self::clip_for(coordinator, input, remaining_secs);
        let size = active.residual_qty.min(clip).max(0.0);
        let size = ((size * 10.0).round()) / 10.0;
        if size <= 0.0 {
            return None;
        }

        let safety_margin = coordinator.post_only_safety_margin_for(hedge_side, best_bid, best_ask);
        let repair_budget_per_share =
            input.inventory.pair_ledger.repair_budget_available / active.residual_qty.max(1.0);
        let urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true);
        let ceiling =
            1.0 - active.first_vwap - MIN_EDGE_PER_PAIR + repair_budget_per_share + urgency_shadow;
        if ceiling <= 0.0 {
            return None;
        }

        let maker_cap = (best_ask - safety_margin).max(0.0);
        let price = coordinator.safe_price(maker_cap.min(ceiling));
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }

        Some(StrategyIntent {
            side: hedge_side,
            direction: TradeDirection::Buy,
            price,
            size,
            reason: BidReason::Provide,
        })
    }
}

fn round_clip(value: f64) -> f64 {
    ((value.clamp(MIN_CLIP, MAX_CLIP) * 10.0).round()) / 10.0
}

fn side_book(book: &crate::polymarket::coordinator::Book, side: Side) -> (f64, f64) {
    match side {
        Side::Yes => (book.yes_bid, book.yes_ask),
        Side::No => (book.no_bid, book.no_ask),
    }
}

fn spread_ticks(bid: f64, ask: f64, tick: f64) -> f64 {
    if bid <= 0.0 || ask <= 0.0 || ask < bid {
        return 99.0;
    }
    ((ask - bid) / tick).max(0.0)
}

fn mid(bid: f64, ask: f64) -> f64 {
    if bid <= 0.0 || ask <= 0.0 || ask < bid {
        return 0.0;
    }
    (bid + ask) / 2.0
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::coordinator::{Book, CompletionFirstMode, CoordinatorConfig};
    use crate::polymarket::messages::{
        InventorySnapshot, InventoryState, MarketDataMsg, OfiSnapshot, SlotReleaseEvent,
    };
    use crate::polymarket::pair_ledger::PairLedgerSnapshot;
    use crate::polymarket::strategy::{StrategyKind, StrategyTickInput};

    fn make_coord(mut cfg: CoordinatorConfig) -> StrategyCoordinator {
        cfg.strategy = StrategyKind::CompletionFirst;
        cfg.completion_first.market_enabled = true;
        cfg.completion_first.mode = CompletionFirstMode::Shadow;
        let (_ofi_tx, ofi_rx) = tokio::sync::watch::channel(OfiSnapshot::default());
        let (_inv_tx, inv_rx) = tokio::sync::watch::channel(InventorySnapshot::default());
        let (_md_tx, md_rx) = tokio::sync::watch::channel(MarketDataMsg::BookTick {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
            ts: std::time::Instant::now(),
        });
        let (_glft_tx, glft_rx) =
            tokio::sync::watch::channel(crate::polymarket::glft::GlftSignalSnapshot::default());
        let (om_tx, _om_rx) = tokio::sync::mpsc::channel(1);
        let (_kill_tx, kill_rx) = tokio::sync::mpsc::channel(1);
        let (_feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(1);
        let (_release_tx, release_rx) = tokio::sync::mpsc::channel::<SlotReleaseEvent>(1);
        let (_winner_hint_tx, winner_hint_rx) = tokio::sync::mpsc::channel(1);
        StrategyCoordinator::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            winner_hint_rx,
            glft_rx,
            om_tx,
            kill_rx,
            feedback_rx,
            release_rx,
        )
    }

    #[test]
    fn same_side_limit_blocks_second_add() {
        let tranche = PairTranche {
            state: TrancheState::CompletionOnly,
            same_side_add_count: 1,
            ..Default::default()
        };
        assert!(!COMPLETION_FIRST_STRATEGY
            .same_side_add_allowed(tranche, CompletionFirstPhase::CompletionOnly));
    }

    #[test]
    fn flat_seed_emits_gated_dual_buy_when_gate_allows() {
        let mut cfg = CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 15.0;
        cfg.completion_first
            .gate_defaults
            .coverage_stats
            .recent_overlap_episode_count = 500;
        let coord = make_coord(cfg);
        let inv = InventoryState::default();
        let inventory = InventorySnapshot {
            settled: inv,
            working: inv,
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
            pair_ledger: PairLedgerSnapshot::default(),
            episode_metrics: Default::default(),
        };
        let book = Book {
            yes_bid: 0.48,
            yes_ask: 0.49,
            no_bid: 0.48,
            no_ask: 0.49,
        };
        let metrics = coord.derive_inventory_metrics(&inv);
        let input = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &inventory.pair_ledger,
            episode_metrics: &inventory.episode_metrics,
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        };

        let quotes = COMPLETION_FIRST_STRATEGY.compute_quotes(&coord, input);
        assert!(quotes.yes_buy.is_some());
        assert!(quotes.no_buy.is_some());
        let yes_eval = quotes.completion_first_open_eval(Side::Yes).unwrap();
        let no_eval = quotes.completion_first_open_eval(Side::No).unwrap();
        assert!(yes_eval.allowed);
        assert!(no_eval.allowed);
        assert!((quotes.yes_buy.unwrap().size - yes_eval.final_clip).abs() < 1e-9);
        assert!((quotes.no_buy.unwrap().size - no_eval.final_clip).abs() < 1e-9);
    }

    #[test]
    fn flat_seed_gate_blocks_when_opposite_spread_is_too_wide() {
        let mut cfg = CoordinatorConfig::default();
        cfg.max_net_diff = 15.0;
        cfg.completion_first
            .gate_defaults
            .coverage_stats
            .recent_overlap_episode_count = 500;
        let coord = make_coord(cfg);
        let inv = InventoryState::default();
        let inventory = InventorySnapshot {
            settled: inv,
            working: inv,
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
            pair_ledger: PairLedgerSnapshot::default(),
            episode_metrics: Default::default(),
        };
        let book = Book {
            yes_bid: 0.48,
            yes_ask: 0.49,
            no_bid: 0.30,
            no_ask: 0.40,
        };
        let metrics = coord.derive_inventory_metrics(&inv);
        let quotes = COMPLETION_FIRST_STRATEGY.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                settled_inv: &inv,
                working_inv: &inv,
                inventory: &inventory,
                pair_ledger: &inventory.pair_ledger,
                episode_metrics: &inventory.episode_metrics,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: None,
            },
        );

        let yes_eval = quotes.completion_first_open_eval(Side::Yes).unwrap();
        assert!(!yes_eval.allowed);
        assert_eq!(yes_eval.block_reason, "opposite_spread_hard_block");
    }

    #[test]
    fn completion_only_prefers_opposite_leg_when_same_side_is_exhausted() {
        let mut cfg = CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 15.0;
        let coord = make_coord(cfg);
        let working = InventoryState {
            yes_qty: 100.0,
            yes_avg_cost: 0.44,
            no_qty: 20.0,
            no_avg_cost: 0.48,
            net_diff: 80.0,
            ..Default::default()
        };
        let active = PairTranche {
            id: 7,
            state: TrancheState::CompletionOnly,
            first_side: Some(Side::Yes),
            first_qty: 100.0,
            first_vwap: 0.44,
            hedge_qty: 20.0,
            hedge_vwap: 0.48,
            residual_qty: 80.0,
            pairable_qty: 20.0,
            same_side_add_count: 1,
            ..Default::default()
        };
        let inventory = InventorySnapshot {
            settled: working,
            working,
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
            pair_ledger: PairLedgerSnapshot {
                active_tranche: Some(active),
                buy_fill_count: 2,
                ..Default::default()
            },
            episode_metrics: Default::default(),
        };
        let book = Book {
            yes_bid: 0.46,
            yes_ask: 0.47,
            no_bid: 0.52,
            no_ask: 0.53,
        };
        let metrics = coord.derive_inventory_metrics(&working);
        let quotes = COMPLETION_FIRST_STRATEGY.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &working,
                settled_inv: &working,
                working_inv: &working,
                inventory: &inventory,
                pair_ledger: &inventory.pair_ledger,
                episode_metrics: &inventory.episode_metrics,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: None,
            },
        );

        assert!(quotes.yes_buy.is_none(), "same-side add should be blocked");
        assert!(
            quotes.no_buy.is_some(),
            "opposite completion leg should remain"
        );
    }

    #[test]
    fn clip_tail_multiplier_increases_size() {
        let mut cfg = CoordinatorConfig::default();
        cfg.max_net_diff = 15.0;
        let coord = make_coord(cfg);
        let inv = InventoryState::default();
        let inventory = InventorySnapshot {
            settled: inv,
            working: inv,
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
            pair_ledger: PairLedgerSnapshot {
                buy_fill_count: 1,
                ..Default::default()
            },
            episode_metrics: Default::default(),
        };
        let book = Book::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let input = StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &inventory,
            pair_ledger: &inventory.pair_ledger,
            episode_metrics: &inventory.episode_metrics,
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        };

        let base = CompletionFirstStrategy::clip_for_hour(&coord, input, 31, 14);
        let tail = CompletionFirstStrategy::clip_for_hour(&coord, input, 30, 14);
        assert!(tail > base, "tail clip should be larger inside final 30s");
    }
}
