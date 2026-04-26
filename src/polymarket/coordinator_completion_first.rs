use tracing::info;

use super::*;

const COMPLETION_FIRST_MERGE_TRIGGER_SHARES: f64 = 10.0;
const COMPLETION_FIRST_MERGE_ENTER_SECS: u64 = 25;
const COMPLETION_FIRST_MERGE_RETRY_SECS: u64 = 18;
const COMPLETION_FIRST_REDEEM_FIRST_DELAY_SECS: u64 = 35;
const COMPLETION_FIRST_REDEEM_SECOND_DELAY_SECS: u64 = 50;

impl StrategyCoordinator {
    pub(crate) fn completion_first_market_enabled(&self) -> bool {
        self.cfg.strategy == StrategyKind::CompletionFirst
            && self.cfg.completion_first.market_enabled
    }

    pub(crate) fn completion_first_phase(
        &self,
    ) -> crate::polymarket::strategy::completion_first::CompletionFirstPhase {
        use crate::polymarket::strategy::completion_first::CompletionFirstPhase;

        if !self.completion_first_market_enabled() {
            return CompletionFirstPhase::PostResolve;
        }
        if self.market_has_ended() {
            return CompletionFirstPhase::PostResolve;
        }
        let remaining = self.seconds_to_market_end().unwrap_or(u64::MAX);
        if remaining <= COMPLETION_FIRST_MERGE_ENTER_SECS {
            CompletionFirstPhase::HarvestWindow
        } else if self
            .current_inventory_snapshot()
            .pair_ledger
            .active_tranche
            .is_some_and(|t| t.residual_qty > 1e-9)
        {
            CompletionFirstPhase::CompletionOnly
        } else {
            CompletionFirstPhase::FlatSeed
        }
    }

    pub(crate) fn completion_first_mode(&self) -> CompletionFirstMode {
        self.cfg.completion_first.mode
    }

    pub(crate) fn market_has_ended(&self) -> bool {
        let Some(end_ts) = self.cfg.market_end_ts else {
            return false;
        };
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now_secs >= end_ts
    }

    pub(super) fn observe_completion_first_inventory_transition(
        &mut self,
        snapshot: &InventorySnapshot,
        now: Instant,
    ) {
        if self.cfg.strategy != StrategyKind::CompletionFirst {
            return;
        }
        let fill_count = snapshot.pair_ledger.buy_fill_count;
        let same_side_add_count = snapshot
            .pair_ledger
            .active_tranche
            .map(|tranche| tranche.same_side_add_count)
            .unwrap_or(0);
        self.completion_first_round_buy_fill_count = fill_count;
        self.completion_first_same_side_run_count = same_side_add_count;
        let phase = self.completion_first_phase();
        if phase != self.completion_first_last_phase {
            info!(
                "🧭 completion_first phase {:?} -> {:?}",
                self.completion_first_last_phase, phase
            );
            self.completion_first_last_phase = phase;
            self.completion_first_decision_epoch =
                self.completion_first_decision_epoch.saturating_add(1);
        }
        let current = snapshot.working;
        let prev = self.last_working_inv_snapshot;
        let changed = (current.yes_qty - prev.yes_qty).abs() > 1e-9
            || (current.no_qty - prev.no_qty).abs() > 1e-9
            || (current.yes_avg_cost - prev.yes_avg_cost).abs() > 1e-9
            || (current.no_avg_cost - prev.no_avg_cost).abs() > 1e-9
            || snapshot.fragile
                != ((self.last_working_inv_snapshot.yes_qty
                    - self.last_settled_inv_snapshot.yes_qty)
                    .abs()
                    > 1e-9
                    || (self.last_working_inv_snapshot.no_qty
                        - self.last_settled_inv_snapshot.no_qty)
                        .abs()
                        > 1e-9);
        if changed {
            self.completion_first_decision_epoch =
                self.completion_first_decision_epoch.saturating_add(1);
        }
        self.last_settled_inv_snapshot = snapshot.settled;
        self.last_working_inv_snapshot = snapshot.working;
        self.maybe_completion_first_shadow_lifecycle(snapshot, now);
    }

    fn maybe_completion_first_shadow_lifecycle(
        &mut self,
        snapshot: &InventorySnapshot,
        _now: Instant,
    ) {
        if self.cfg.strategy != StrategyKind::CompletionFirst {
            return;
        }
        let phase = self.completion_first_phase();
        if phase
            == crate::polymarket::strategy::completion_first::CompletionFirstPhase::HarvestWindow
        {
            let remaining = self.seconds_to_market_end().unwrap_or(u64::MAX);
            let mergeable = snapshot.pair_ledger.capital_state.mergeable_full_sets;
            if !self.completion_first_merge_requested_this_round
                && remaining <= COMPLETION_FIRST_MERGE_ENTER_SECS
                && mergeable >= COMPLETION_FIRST_MERGE_TRIGGER_SHARES
            {
                self.completion_first_merge_requested_this_round = true;
                let payload = serde_json::json!({
                    "remaining_secs": remaining,
                    "mergeable_full_sets": mergeable,
                    "attempt": 1,
                    "mode": format!("{:?}", self.completion_first_mode()),
                    "shadow": self.completion_first_mode() == CompletionFirstMode::Shadow,
                });
                self.emit_completion_first_event(
                    "completion_first_merge_requested",
                    payload.clone(),
                );
                if self.completion_first_mode() == CompletionFirstMode::Shadow {
                    self.emit_completion_first_event("completion_first_merge_executed", payload);
                }
            } else if self.completion_first_merge_requested_this_round
                && !self.completion_first_merge_retry_requested_this_round
                && remaining <= COMPLETION_FIRST_MERGE_RETRY_SECS
                && mergeable >= COMPLETION_FIRST_MERGE_TRIGGER_SHARES
            {
                self.completion_first_merge_retry_requested_this_round = true;
                let payload = serde_json::json!({
                    "remaining_secs": remaining,
                    "mergeable_full_sets": mergeable,
                    "attempt": 2,
                    "mode": format!("{:?}", self.completion_first_mode()),
                    "shadow": self.completion_first_mode() == CompletionFirstMode::Shadow,
                });
                self.emit_completion_first_event(
                    "completion_first_merge_requested",
                    payload.clone(),
                );
                if self.completion_first_mode() == CompletionFirstMode::Shadow {
                    self.emit_completion_first_event("completion_first_merge_executed", payload);
                }
            }
        }

        if phase == crate::polymarket::strategy::completion_first::CompletionFirstPhase::PostResolve
        {
            let Some(end_ts) = self.cfg.market_end_ts else {
                return;
            };
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let elapsed = now_secs.saturating_sub(end_ts);
            let winner_side = self.post_close_winner_side;
            let residual = match winner_side {
                Some(Side::Yes) => snapshot.working.yes_qty,
                Some(Side::No) => snapshot.working.no_qty,
                None => 0.0,
            };
            if !self.completion_first_shadow_summary_emitted && elapsed >= 1 {
                self.completion_first_shadow_summary_emitted = true;
                self.emit_completion_first_event(
                    "completion_first_shadow_summary",
                    serde_json::json!({
                        "elapsed_secs": elapsed,
                        "winner_side": winner_side.map(|s| s.as_str()),
                        "buy_fill_count": self.completion_first_round_buy_fill_count,
                        "same_side_run_count": self.completion_first_same_side_run_count,
                        "active_tranche_id": snapshot.pair_ledger.active_tranche.map(|t| t.id),
                        "active_state": snapshot.pair_ledger.active_tranche.map(|t| format!("{:?}", t.state)),
                        "residual_qty": snapshot.pair_ledger.residual_qty,
                        "residual_side": snapshot.pair_ledger.residual_side.map(|s| s.as_str()),
                        "pairable_qty": snapshot.pair_ledger.total_pairable_qty(),
                        "surplus_bank": snapshot.pair_ledger.surplus_bank,
                        "repair_budget_available": snapshot.pair_ledger.repair_budget_available,
                        "clean_closed_episode_ratio": snapshot.episode_metrics.clean_closed_episode_ratio,
                        "same_side_add_qty_ratio": snapshot.episode_metrics.same_side_add_qty_ratio,
                        "episode_close_delay_p50": snapshot.episode_metrics.episode_close_delay_p50,
                        "episode_close_delay_p90": snapshot.episode_metrics.episode_close_delay_p90,
                        "conditional_second_same_side_would_allow": snapshot.episode_metrics.conditional_second_same_side_would_allow,
                        "working_yes_qty": snapshot.working.yes_qty,
                        "working_no_qty": snapshot.working.no_qty,
                        "mergeable_full_sets": snapshot.pair_ledger.capital_state.mergeable_full_sets,
                        "working_capital": snapshot.pair_ledger.capital_state.working_capital,
                        "locked_capital_ratio": snapshot.pair_ledger.capital_state.locked_capital_ratio,
                        "mode": format!("{:?}", self.completion_first_mode()),
                    }),
                );
            }
            if residual <= 1e-9 {
                return;
            }
            if self.completion_first_redeem_requested_count == 0
                && elapsed >= COMPLETION_FIRST_REDEEM_FIRST_DELAY_SECS
            {
                self.completion_first_redeem_requested_count = 1;
                self.emit_completion_first_event(
                    "completion_first_redeem_requested",
                    serde_json::json!({
                        "elapsed_secs": elapsed,
                        "winner_side": winner_side.map(|s| s.as_str()),
                        "residual_qty": residual,
                        "attempt": 1,
                        "mode": format!("{:?}", self.completion_first_mode()),
                    }),
                );
            } else if self.completion_first_redeem_requested_count == 1
                && elapsed >= COMPLETION_FIRST_REDEEM_SECOND_DELAY_SECS
            {
                self.completion_first_redeem_requested_count = 2;
                self.emit_completion_first_event(
                    "completion_first_redeem_requested",
                    serde_json::json!({
                        "elapsed_secs": elapsed,
                        "winner_side": winner_side.map(|s| s.as_str()),
                        "residual_qty": residual,
                        "attempt": 2,
                        "mode": format!("{:?}", self.completion_first_mode()),
                    }),
                );
            }
        }
    }

    pub(super) fn emit_completion_first_decision_events(
        &self,
        snapshot: &InventorySnapshot,
        quotes: &StrategyQuotes,
    ) {
        if self.cfg.strategy != StrategyKind::CompletionFirst {
            return;
        }
        let phase = self.completion_first_phase();
        let active = snapshot.pair_ledger.active_tranche;
        if let Some(intent) = quotes.buy_for(Side::Yes) {
            let event = if active.is_some() && active.and_then(|t| t.first_side) != Some(Side::Yes)
            {
                "completion_first_completion_built"
            } else {
                "completion_first_seed_built"
            };
            self.emit_completion_first_event(
                event,
                serde_json::json!({
                    "phase": format!("{:?}", phase),
                    "side": "YES",
                    "price": intent.price,
                    "size": intent.size,
                    "active_tranche_id": active.map(|t| t.id),
                    "same_side_run_count": self.completion_first_same_side_run_count,
                    "buy_fill_count": self.completion_first_round_buy_fill_count,
                }),
            );
        }
        if let Some(intent) = quotes.buy_for(Side::No) {
            let event = if active.is_some() && active.and_then(|t| t.first_side) != Some(Side::No) {
                "completion_first_completion_built"
            } else {
                "completion_first_seed_built"
            };
            self.emit_completion_first_event(
                event,
                serde_json::json!({
                    "phase": format!("{:?}", phase),
                    "side": "NO",
                    "price": intent.price,
                    "size": intent.size,
                    "active_tranche_id": active.map(|t| t.id),
                    "same_side_run_count": self.completion_first_same_side_run_count,
                    "buy_fill_count": self.completion_first_round_buy_fill_count,
                }),
            );
        }
        if let Some(active) = active {
            if active.same_side_add_count >= 1 && phase
                == crate::polymarket::strategy::completion_first::CompletionFirstPhase::CompletionOnly
            {
                self.emit_completion_first_event(
                    "completion_first_same_side_add_blocked",
                    serde_json::json!({
                        "active_tranche_id": active.id,
                        "same_side_add_count": active.same_side_add_count,
                        "residual_qty": active.residual_qty,
                    }),
                );
            }
        }
    }

    fn emit_completion_first_event(&self, event: &str, payload: serde_json::Value) {
        if let (Some(recorder), Some(meta)) = (&self.recorder, &self.recorder_meta) {
            recorder.emit_own_inventory_event(meta, event, payload);
        }
    }
}
