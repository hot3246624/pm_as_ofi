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
                            self.seed_intent_with_clip(coordinator, input, seed_intent)
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

        if let Some(intent) = pair_arb_quotes.buy_for(Side::Yes) {
            if let Some(intent) = self.seed_intent_with_clip(coordinator, input, intent) {
                quotes.set(intent);
            }
        }
        if let Some(intent) = pair_arb_quotes.buy_for(Side::No) {
            if let Some(intent) = self.seed_intent_with_clip(coordinator, input, intent) {
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
        let clip = BASE_CLIP * session_mult * imbalance_mult * trade_index_mult * tail_mult;
        ((clip.clamp(MIN_CLIP, MAX_CLIP) * 10.0).round()) / 10.0
    }

    fn seed_intent_with_clip(
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

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}

use chrono::Timelike;

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
    fn flat_seed_emits_dual_buy_with_completion_clip() {
        let mut cfg = CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 15.0;
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
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        };

        let quotes = COMPLETION_FIRST_STRATEGY.compute_quotes(&coord, input);
        let clip = CompletionFirstStrategy::clip_for(&coord, input, u64::MAX);
        assert!(quotes.yes_buy.is_some());
        assert!(quotes.no_buy.is_some());
        assert!((quotes.yes_buy.unwrap().size - clip).abs() < 1e-9);
        assert!((quotes.no_buy.unwrap().size - clip).abs() < 1e-9);
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
