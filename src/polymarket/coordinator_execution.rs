use tracing::{debug, info};

use super::*;
use crate::polymarket::strategy::pair_gated_tranche::{
    pgt_effective_open_pair_band_value, pgt_open_leg_ceiling_from_opposite_bid,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct PgtShadowTakerOpenCandidate {
    pub(super) side: Side,
    pub(super) limit_price: f64,
    slack: f64,
    best_ask: f64,
}

const PGT_SHADOW_TAKER_CLOSE_SECS: u64 = 90;
const PGT_TAIL_NO_NEW_OPEN_SECS: u64 = 25;
const PGT_SAME_SIDE_RELEASE_QUARANTINE_MS: u64 = 1_200;
const PGT_SHADOW_TAKER_OPEN_EXEC_ENABLED: bool = false;
const PGT_SHADOW_TAKER_CLOSE_EXEC_ENABLED: bool = true;

impl StrategyCoordinator {
    pub(super) fn pgt_buy_retain_decision(
        &self,
        reason: BidReason,
        delta_ticks: f64,
        slot_age: std::time::Duration,
        remaining_secs: u64,
    ) -> (bool, &'static str) {
        match reason {
            BidReason::Provide => {
                if delta_ticks <= 0.0 + 1e-9 {
                    return (true, "pgt_seed_no_chase_down");
                }
                let min_age = if remaining_secs <= 45 {
                    std::time::Duration::from_secs(5)
                } else if remaining_secs <= 90 {
                    std::time::Duration::from_secs(10)
                } else {
                    std::time::Duration::from_secs(20)
                };
                let min_up_ticks = if remaining_secs <= 90 { 2.0 } else { 3.0 };
                if slot_age < min_age {
                    return (true, "pgt_seed_upward_refresh_throttled");
                }
                if delta_ticks < min_up_ticks - 1e-9 {
                    return (true, "pgt_seed_upward_refresh_too_small");
                }
                (false, "pgt_seed_upward_refresh_allowed")
            }
            BidReason::Hedge => {
                if delta_ticks <= 0.0 + 1e-9 {
                    (true, "pgt_completion_no_chase_down")
                } else {
                    let time_based_max_up_ticks: f64 = if remaining_secs <= 45 {
                        0.0
                    } else if remaining_secs <= 60 {
                        1.0
                    } else if remaining_secs <= 90 {
                        1.0
                    } else if remaining_secs <= 120 {
                        2.0
                    } else {
                        3.0
                    };
                    let age_based_max_up_ticks: f64 =
                        if slot_age >= std::time::Duration::from_secs(20) {
                            0.0
                        } else if slot_age >= std::time::Duration::from_secs(10) {
                            1.0
                        } else {
                            f64::INFINITY
                        };
                    let max_up_ticks = if slot_age < std::time::Duration::from_millis(1_200) {
                        time_based_max_up_ticks.max(1.0)
                    } else {
                        time_based_max_up_ticks.min(age_based_max_up_ticks)
                    };
                    (
                        delta_ticks <= max_up_ticks + 1e-9,
                        "pgt_completion_step_up_band_hold",
                    )
                }
            }
            BidReason::OracleLagProvide => (false, "not_pgt_path"),
        }
    }

    pub(super) async fn handle_slot_release_event(&mut self, event: SlotReleaseEvent) {
        self.soft_release_slot_target(event.slot);
        let idx = event.slot.index();
        self.pair_arb_slot_blocked_for_ms[idx] = 0;
        self.pair_arb_slot_blocked_at[idx] = None;
        self.slot_pair_arb_cross_reject_reprice_pending[idx] = false;
        self.slot_pair_arb_state_republish_latched[idx] = false;
        if self.cfg.strategy.is_pair_gated_tranche_arb()
            && event.slot.direction == TradeDirection::Buy
        {
            self.pgt_same_side_release_quarantine_until[event.slot.side.index()] = Some(
                std::time::Instant::now()
                    + std::time::Duration::from_millis(PGT_SAME_SIDE_RELEASE_QUARANTINE_MS),
            );
        }
    }

    pub(super) fn pair_arb_should_force_freshness_republish(
        &self,
        inv: &InventoryState,
        slot: OrderSlot,
        current_price: f64,
        fresh_price: f64,
        size: f64,
    ) -> (bool, &'static str, f64) {
        if !self.cfg.strategy.is_pair_arb() || slot.direction != TradeDirection::Buy {
            return (false, "non_pair_arb", 0.0);
        }
        let tick = self.cfg.tick_size.max(1e-9);
        let delta_ticks = (fresh_price - current_price) / tick;
        let risk_effect =
            crate::polymarket::strategy::pair_arb::PairArbStrategy::candidate_risk_effect(
                inv, slot.side, size,
            );
        match risk_effect {
            // Same-side risk-increasing leg is state-driven:
            // no continuous freshness reprice inside the same state bucket.
            crate::polymarket::strategy::pair_arb::PairArbRiskEffect::RiskIncreasing => {
                (false, "risk_increasing_state_driven", delta_ticks.abs())
            }
            // Pairing/reducing leg is event-driven plus a slow timed upward
            // freshness check to avoid long stale quotes in drifting books.
            crate::polymarket::strategy::pair_arb::PairArbRiskEffect::PairingOrReducing => {
                let slot_age = self.slot_last_ts(slot).elapsed();
                let timed_reprice_due = slot_age >= std::time::Duration::from_secs(8);
                let upward_stale = delta_ticks >= 3.0 - 1e-9;
                if timed_reprice_due && upward_stale {
                    (true, "pairing_timed_up_reprice", delta_ticks)
                } else {
                    (false, "pairing_holds_between_triggers", delta_ticks.abs())
                }
            }
        }
    }

    pub(super) fn pair_arb_state_key(
        &self,
        inv: &InventoryState,
        _phase: EndgamePhase,
    ) -> PairArbStateKey {
        let abs_net = inv.net_diff.abs();
        let net_bucket = if abs_net <= PAIR_ARB_NET_EPS {
            PairArbNetBucket::Flat
        } else if abs_net < 5.0 {
            PairArbNetBucket::Low
        } else if abs_net < 10.0 {
            PairArbNetBucket::Mid
        } else {
            PairArbNetBucket::High
        };
        let dominant_side = if inv.net_diff > PAIR_ARB_NET_EPS {
            Some(Side::Yes)
        } else if inv.net_diff < -PAIR_ARB_NET_EPS {
            Some(Side::No)
        } else {
            None
        };
        PairArbStateKey {
            dominant_side,
            net_bucket,
            risk_open_cutoff_active: self.pair_arb_risk_open_cutoff_active(),
        }
    }

    // Policy-3: Execution (hedge/provide dispatch)
    pub(super) async fn execute_quotes(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        quotes: StrategyQuotes,
        yes_stale: bool,
        no_stale: bool,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
    ) {
        if self.cfg.strategy.is_oracle_lag_sniping() && self.oracle_lag_round_halted {
            // Round hard-stop after balance/allowance reject: no more trading attempts
            // for this round; proactively clear live targets.
            self.clear_slot_target(OrderSlot::YES_BUY, CancelReason::InventoryLimit)
                .await;
            self.clear_slot_target(OrderSlot::NO_BUY, CancelReason::InventoryLimit)
                .await;
            return;
        }

        if self.cfg.strategy.execution_mode() == StrategyExecutionMode::SlotMarketMaking {
            self.execute_slot_market_making(
                inv,
                ub,
                quotes,
                yes_stale,
                no_stale,
                yes_toxic_blocked,
                no_toxic_blocked,
            )
            .await;
            return;
        }

        let mut st = self.init_execution_state(inv, quotes);
        self.apply_endgame_controls(inv, ub, &mut st);
        self.maybe_pgt_force_clear_tail_seed_orders(&mut st).await;
        if self.should_execute_directional_hedges(&st) {
            self.execute_hedges(inv, ub, yes_stale, no_stale, &mut st)
                .await;
        }
        self.finalize_provide_dispatch(
            inv,
            ub,
            yes_stale,
            no_stale,
            yes_toxic_blocked,
            no_toxic_blocked,
            &mut st,
        )
        .await;
    }

    pub(super) async fn maybe_pgt_force_clear_tail_seed_orders(&mut self, st: &mut ExecutionState) {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() {
            return;
        }
        let remaining_secs = self.seconds_to_market_end().unwrap_or(u64::MAX);
        let freeze_active = remaining_secs <= self.cfg.endgame_freeze_secs;
        if !freeze_active && remaining_secs > PGT_TAIL_NO_NEW_OPEN_SECS {
            return;
        }
        let active_tranche_exists = self.inv_rx.borrow().pair_ledger.active_tranche.is_some();
        if active_tranche_exists && !freeze_active {
            return;
        }

        st.disable_all_provide_with_reason(CancelReason::EndgameRiskGate);
        st.intent_yes = None;
        st.intent_no = None;

        let has_local_buy_target = self.slot_target_active(OrderSlot::YES_BUY)
            || self.slot_target_active(OrderSlot::NO_BUY);
        if self.pgt_tail_seed_force_clear_sent && !has_local_buy_target {
            return;
        }
        self.pgt_tail_seed_force_clear_sent = true;

        for slot in [OrderSlot::YES_BUY, OrderSlot::NO_BUY] {
            self.stats.pgt_dispatch_clear = self.stats.pgt_dispatch_clear.saturating_add(1);
            self.force_clear_slot_target(
                slot,
                CancelReason::EndgameRiskGate,
                SlotResetScope::Full,
                if freeze_active {
                    "pgt_freeze_no_buy"
                } else {
                    "pgt_tail_no_new_open"
                },
            )
            .await;
        }
    }

    pub(super) fn should_execute_directional_hedges(&self, st: &ExecutionState) -> bool {
        if self.cfg.strategy.is_pair_arb()
            || self.cfg.strategy.is_pair_gated_tranche_arb()
            || self.cfg.strategy.is_oracle_lag_sniping()
        {
            return false;
        }
        match self.cfg.strategy.execution_mode() {
            StrategyExecutionMode::DirectionalHedgeOverlay => true,
            StrategyExecutionMode::UnifiedBuys => st.endgame_phase >= EndgamePhase::HardClose,
            StrategyExecutionMode::SlotMarketMaking => false,
        }
    }

    pub(super) fn init_execution_state(
        &self,
        inv: &InventoryState,
        quotes: StrategyQuotes,
    ) -> ExecutionState {
        ExecutionState {
            intent_yes: quotes.buy_for(Side::Yes),
            intent_no: quotes.buy_for(Side::No),
            net_diff: inv.net_diff,
            hedge_dispatched_yes: false,
            hedge_dispatched_no: false,
            allow_yes_provide: self.can_place_strategy_intent(inv, quotes.buy_for(Side::Yes)),
            allow_no_provide: self.can_place_strategy_intent(inv, quotes.buy_for(Side::No)),
            block_yes_provide: false,
            block_no_provide: false,
            block_reason_yes: None,
            block_reason_no: None,
            pgt_taker_close_limit_yes: quotes.pgt_taker_close_limit_for(Side::Yes),
            pgt_taker_close_limit_no: quotes.pgt_taker_close_limit_for(Side::No),
            force_taker_side: None,
            force_taker_size: 0.0,
            block_maker_hedge: false,
            endgame_phase: self.endgame_phase(),
        }
    }

    pub(super) fn pair_arb_quote_still_admissible(
        &self,
        inv: &InventoryState,
        _ub: &Book,
        slot: OrderSlot,
        price: f64,
        size: f64,
        phase: EndgamePhase,
    ) -> bool {
        if !self.cfg.strategy.is_pair_arb() || slot.direction != TradeDirection::Buy {
            return true;
        }

        let intent = StrategyIntent {
            side: slot.side,
            direction: slot.direction,
            price,
            size,
            reason: BidReason::Provide,
        };
        if !self.can_place_strategy_intent(inv, Some(intent)) {
            return false;
        }

        let risk_effect =
            crate::polymarket::strategy::pair_arb::PairArbStrategy::candidate_risk_effect(
                inv, slot.side, size,
            );
        let risk_increasing = matches!(
            risk_effect,
            crate::polymarket::strategy::pair_arb::PairArbRiskEffect::RiskIncreasing
        );
        if phase >= EndgamePhase::SoftClose
            && risk_increasing
            && self.pair_arb_soft_close_blocks_side(inv, slot.side)
        {
            return false;
        }
        let side_ofi = match slot.side {
            Side::Yes => self.ofi_rx.borrow().yes,
            Side::No => self.ofi_rx.borrow().no,
        };
        let ofi_decision = crate::polymarket::strategy::pair_arb::PairArbStrategy::ofi_decision(
            Some(side_ofi),
            risk_effect,
        );
        if ofi_decision.suppress {
            return false;
        }

        let eps = 1e-9;
        if let Some(cap) =
            crate::polymarket::strategy::pair_arb::PairArbStrategy::tier_cap_price_for_candidate(
                inv,
                slot.side,
                size,
                risk_effect,
                self.cfg.pair_arb.tier_1_mult,
                self.cfg.pair_arb.tier_2_mult,
            )
        {
            if price > cap + eps {
                return false;
            }
        }

        let effective_pair_cost_margin = if inv.net_diff.abs() < PAIR_ARB_NET_EPS {
            0.0
        } else {
            self.cfg.pair_arb.pair_cost_safety_margin
        };
        let ceiling = match slot.side {
            Side::Yes => crate::polymarket::strategy::pair_arb::PairArbStrategy::vwap_ceiling(
                self.cfg.pair_target,
                effective_pair_cost_margin,
                inv.no_avg_cost,
                inv.yes_qty,
                inv.yes_avg_cost,
                self.cfg.bid_size,
            ),
            Side::No => crate::polymarket::strategy::pair_arb::PairArbStrategy::vwap_ceiling(
                self.cfg.pair_target,
                effective_pair_cost_margin,
                inv.yes_avg_cost,
                inv.no_qty,
                inv.no_avg_cost,
                self.cfg.bid_size,
            ),
        };
        if ceiling <= self.cfg.tick_size + eps || price > ceiling + eps {
            return false;
        }
        self.simulate_buy(inv, slot.side, size, price).is_some()
    }

    fn pair_arb_should_retain_existing(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        slot: OrderSlot,
        intent: StrategyIntent,
        phase: EndgamePhase,
    ) -> bool {
        let Some(current) = self.slot_target(slot).cloned() else {
            return false;
        };
        let state_key = self.pair_arb_state_key(inv, phase);
        let idx = slot.index();
        let prev_state = self.slot_pair_arb_state_keys[idx];
        let state_changed = prev_state.is_some() && prev_state != Some(state_key);
        let fill_recheck = self.slot_pair_arb_fill_recheck_pending[idx];
        let cross_reject_reprice_pending = self.slot_pair_arb_cross_reject_reprice_pending[idx];
        let (force_freshness_republish, freshness_reason, freshness_delta_ticks) = self
            .pair_arb_should_force_freshness_republish(
                inv,
                slot,
                current.price,
                intent.price,
                intent.size,
            );
        if cross_reject_reprice_pending {
            debug!(
                "🧭 pair_arb cross-reject pending republish | slot={} live={:.4}@{:.2} fresh={:.4}@{:.2}",
                slot.as_str(),
                current.price,
                current.size,
                intent.price,
                intent.size,
            );
            return false;
        }
        let still_admissible = !fill_recheck
            || self.pair_arb_quote_still_admissible(
                inv,
                ub,
                slot,
                current.price,
                current.size,
                phase,
            );
        if state_changed || fill_recheck {
            debug!(
                "🧭 pair_arb retain recheck | slot={} state_key_changed={} fill_recheck_pending={} net_bucket={:?} dominant_side={:?} risk_open_cutoff_active={} still_admissible={}",
                slot.as_str(),
                state_changed,
                fill_recheck,
                state_key.net_bucket,
                state_key.dominant_side,
                state_key.risk_open_cutoff_active,
                still_admissible,
            );
        }
        if force_freshness_republish {
            debug!(
                "🧭 pair_arb freshness reprice | slot={} reason={} freshness_delta_ticks={:.2} fresh={:.4} live={:.4}",
                slot.as_str(),
                freshness_reason,
                freshness_delta_ticks,
                intent.price,
                current.price,
            );
        }
        let upward_stale = (state_changed || fill_recheck)
            && intent.price > current.price + 3.0 * self.cfg.tick_size.max(1e-9);
        if upward_stale {
            debug!(
                "🧭 pair_arb upward stale reprice | slot={} fresh={:.4} live={:.4} gap_ticks={:.2}",
                slot.as_str(),
                intent.price,
                current.price,
                (intent.price - current.price) / self.cfg.tick_size.max(1e-9),
            );
            return false;
        }
        let state_aligned = (current.direction == intent.direction)
            && (current.reason == intent.reason)
            && (current.price - intent.price).abs() <= self.cfg.tick_size.max(1e-9)
            && (current.size - intent.size).abs() <= 0.1;
        if state_changed && !state_aligned {
            if !self.slot_pair_arb_state_republish_latched[idx] {
                self.slot_pair_arb_state_republish_latched[idx] = true;
                self.stats.pair_arb_state_forced_republish =
                    self.stats.pair_arb_state_forced_republish.saturating_add(1);
                info!(
                    "🧭 pair_arb_state_forced_republish | slot={} prev_state={:?} current_state={:?} live={:.4}@{:.2} fresh={:.4}@{:.2}",
                    slot.as_str(),
                    prev_state,
                    state_key,
                    current.price,
                    current.size,
                    intent.price,
                    intent.size,
                );
            }
            return false;
        }
        self.slot_pair_arb_state_republish_latched[idx] = false;
        if !still_admissible || force_freshness_republish {
            return false;
        }

        let (best_bid, best_ask) = match slot.side {
            Side::Yes => (ub.yes_bid, ub.yes_ask),
            Side::No => (ub.no_bid, ub.no_ask),
        };
        if self.keep_existing_maker_if_safe(
            inv,
            slot.side,
            current.direction,
            current.price,
            current.size,
            current.price,
            best_bid,
            best_ask,
            current.reason,
        ) {
            self.slot_pair_arb_state_keys[idx] = Some(state_key);
            self.slot_pair_arb_fill_recheck_pending[idx] = false;
            return true;
        }
        false
    }

    pub(super) fn pgt_should_retain_existing(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        slot: OrderSlot,
        intent: StrategyIntent,
    ) -> bool {
        let Some(current) = self.slot_target(slot).cloned() else {
            return false;
        };
        if current.direction != intent.direction || current.reason != intent.reason {
            return false;
        }
        if (current.size - intent.size).abs() > 0.1 {
            return false;
        }
        let tick = self.cfg.tick_size.max(1e-9);
        let delta_ticks = (intent.price - current.price) / tick;
        let slot_age = self.slot_last_ts(slot).elapsed();
        let remaining_secs = self.seconds_to_market_end().unwrap_or(u64::MAX);
        let (retain, retain_reason) =
            self.pgt_buy_retain_decision(intent.reason, delta_ticks, slot_age, remaining_secs);
        if !retain {
            return false;
        }
        let (best_bid, best_ask) = match slot.side {
            Side::Yes => (ub.yes_bid, ub.yes_ask),
            Side::No => (ub.no_bid, ub.no_ask),
        };
        if self.keep_existing_maker_if_safe(
            inv,
            slot.side,
            intent.direction,
            intent.price,
            intent.size,
            intent.price,
            best_bid,
            best_ask,
            intent.reason,
        ) {
            debug!(
                "🔒 PGT retain {:?}: reason={} strategic_target={:.4} live={:.4} delta_ticks={:.2}",
                slot, retain_reason, intent.price, current.price, delta_ticks,
            );
            return true;
        }
        false
    }

    pub(super) async fn execute_hedges(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        yes_stale: bool,
        no_stale: bool,
        st: &mut ExecutionState,
    ) {
        if st.net_diff > f64::EPSILON {
            if st.has_sell_intent_for(Side::Yes) {
                return;
            }
            self.execute_directional_hedge(inv, ub, Side::No, no_stale, st)
                .await;
        } else if st.net_diff < -f64::EPSILON {
            if st.has_sell_intent_for(Side::No) {
                return;
            }
            self.execute_directional_hedge(inv, ub, Side::Yes, yes_stale, st)
                .await;
        }
    }

    pub(super) async fn execute_directional_hedge(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        hedge_side: Side,
        hedge_stale: bool,
        st: &mut ExecutionState,
    ) {
        let force_side = match hedge_side {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        };
        let side_label = match hedge_side {
            Side::Yes => "YES",
            Side::No => "NO",
        };

        if st.force_taker_side == Some(force_side) {
            self.dispatch_taker_derisk(force_side, st.force_taker_size, inv)
                .await;
            st.mark_hedge_dispatched(force_side);
        }
        if st.block_maker_hedge {
            let block_reason = if st.endgame_phase >= EndgamePhase::HardClose {
                CancelReason::EndgameRiskGate
            } else {
                CancelReason::InventoryLimit
            };
            st.block_provide(hedge_side, block_reason);
        }
        let hedge_target = self.hedge_target(st.net_diff);
        if st.block_maker_hedge {
            return;
        }

        let Some(mut hedge_size) = self.hedge_size_from_net(st.net_diff) else {
            debug!(
                "🧩 Hedge skip {}: net_diff={:.2} below min thresholds (min_order_size={:.2}, min_hedge_size={:.2})",
                side_label,
                st.net_diff,
                self.cfg.min_order_size,
                self.cfg.min_hedge_size,
            );
            return;
        };

        let (book_bid, book_ask) = match hedge_side {
            Side::Yes => (ub.yes_bid, ub.yes_ask),
            Side::No => (ub.no_bid, ub.no_ask),
        };

        let mut ceiling = self.incremental_hedge_ceiling(inv, hedge_side, hedge_size, hedge_target);
        let mut agg = self.aggressive_price_for(hedge_side, ceiling, book_bid, book_ask);
        let mut allow_hedge = match hedge_side {
            Side::Yes => self.can_hedge_buy_yes(inv, hedge_size),
            Side::No => self.can_hedge_buy_no(inv, hedge_size),
        };
        if !allow_hedge {
            st.block_provide(hedge_side, CancelReason::InventoryLimit);
        }
        if !(agg > 0.0 && allow_hedge && !hedge_stale) {
            return;
        }

        let side_bid = st.buy_price_for(hedge_side);
        let mut hedge_px = f64::max(side_bid, agg).min(ceiling);
        hedge_px = self.safe_price(hedge_px);

        if let Some(bumped) = self.bump_hedge_size_for_marketable_floor(hedge_px, hedge_size) {
            if bumped > hedge_size + 1e-9 {
                let can_bumped = match hedge_side {
                    Side::Yes => self.can_hedge_buy_yes(inv, bumped),
                    Side::No => self.can_hedge_buy_no(inv, bumped),
                };
                if can_bumped {
                    hedge_size = bumped;
                    ceiling =
                        self.incremental_hedge_ceiling(inv, hedge_side, hedge_size, hedge_target);
                    agg = self.aggressive_price_for(hedge_side, ceiling, book_bid, book_ask);
                    if agg > 0.0 {
                        let side_bid = st.buy_price_for(hedge_side);
                        hedge_px = f64::max(side_bid, agg).min(ceiling);
                        hedge_px = self.safe_price(hedge_px);
                    }
                    allow_hedge = match hedge_side {
                        Side::Yes => self.can_hedge_buy_yes(inv, hedge_size),
                        Side::No => self.can_hedge_buy_no(inv, hedge_size),
                    };
                } else {
                    debug!(
                        "🧩 Hedge {} notional bump skipped: size {:.2} exceeds inventory gate",
                        side_label, bumped
                    );
                }
            }
        }
        if !allow_hedge {
            st.block_provide(hedge_side, CancelReason::InventoryLimit);
        }
        if !(agg > 0.0 && allow_hedge) {
            return;
        }
        if !self.passes_outcome_floor_for_buy(
            inv,
            hedge_side,
            hedge_size,
            hedge_px,
            BidReason::Hedge,
        ) {
            st.block_provide(hedge_side, CancelReason::InventoryLimit);
            return;
        }

        let remaining_secs = self.seconds_to_market_end().unwrap_or(u64::MAX);
        if PGT_SHADOW_TAKER_CLOSE_EXEC_ENABLED {
            if let Some(limit_price) = self.pgt_shadow_taker_close_limit_for_side(
                hedge_side,
                hedge_px,
                ceiling,
                book_ask,
                remaining_secs,
            ) {
                if self.cfg.strategy.is_pair_gated_tranche_arb() {
                    self.pgt_shadow_taker_close_fired_epoch[hedge_side.index()] =
                        Some(self.pgt_decision_epoch);
                    self.stats.pgt_dispatch_taker_close =
                        self.stats.pgt_dispatch_taker_close.saturating_add(1);
                }
                info!(
                    "⚡ PGT shadow taker-close | side={:?} maker_price={:.4} size={:.2} limit={:.4} ceiling={:.4}",
                    hedge_side, hedge_px, hedge_size, limit_price, ceiling
                );
                self.dispatch_taker_intent(
                    hedge_side,
                    TradeDirection::Buy,
                    hedge_size,
                    TradePurpose::Hedge,
                    Some(limit_price),
                )
                .await;
                st.mark_hedge_dispatched(hedge_side);
                return;
            }
        }

        if self.keep_existing_maker_if_safe(
            inv,
            hedge_side,
            TradeDirection::Buy,
            hedge_px,
            hedge_size,
            ceiling,
            book_bid,
            book_ask,
            BidReason::Hedge,
        ) {
            return;
        }

        let current_target = self.side_target(hedge_side);
        let current_px = current_target.map(|t| t.price).unwrap_or(0.0);
        let current_sz = current_target.map(|t| t.size).unwrap_or(0.0);
        let log_msg = if current_px <= 0.0
            || (current_px - hedge_px).abs() > self.maker_keep_band(BidReason::Hedge)
            || (current_sz - hedge_size).abs() > 0.1
        {
            Some(format!(
                "🔧 HEDGE {}@{:.3} sz={:.1} | net={:.1}",
                side_label, hedge_px, hedge_size, st.net_diff
            ))
        } else {
            None
        };

        self.place_or_reprice(
            hedge_side,
            TradeDirection::Buy,
            hedge_px,
            hedge_size,
            BidReason::Hedge,
            log_msg,
        )
        .await;
        st.mark_hedge_dispatched(hedge_side);
    }

    pub(super) async fn finalize_provide_dispatch(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        yes_stale: bool,
        no_stale: bool,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
        st: &mut ExecutionState,
    ) {
        st.apply_blocked_provide();
        let yes_toxic_blocked = yes_toxic_blocked && self.execution_toxic_block_applies();
        let no_toxic_blocked = no_toxic_blocked && self.execution_toxic_block_applies();
        let shadow_taker_open = self.pgt_shadow_taker_open_candidate(
            inv,
            ub,
            st,
            yes_toxic_blocked,
            no_toxic_blocked,
            yes_stale,
            no_stale,
        );

        let yes_is_pgt_completion = self.cfg.strategy.is_pair_gated_tranche_arb()
            && matches!(
                st.intent_for(Side::Yes),
                Some(intent) if intent.reason == BidReason::Hedge
            );
        let no_is_pgt_completion = self.cfg.strategy.is_pair_gated_tranche_arb()
            && matches!(
                st.intent_for(Side::No),
                Some(intent) if intent.reason == BidReason::Hedge
            );
        let side_order = if no_is_pgt_completion && !yes_is_pgt_completion {
            [Side::No, Side::Yes]
        } else {
            [Side::Yes, Side::No]
        };

        for side in side_order {
            let (
                intent,
                allow_provide,
                block_reason,
                hedge_dispatched,
                toxic_blocked,
                stale,
                pgt_taker_close_limit,
            ) = match side {
                Side::Yes => (
                    st.intent_for(Side::Yes),
                    st.allow_provide_for(Side::Yes),
                    st.block_reason_for(Side::Yes),
                    st.hedge_dispatched_for(Side::Yes),
                    yes_toxic_blocked,
                    yes_stale,
                    st.pgt_taker_close_limit_for(Side::Yes),
                ),
                Side::No => (
                    st.intent_for(Side::No),
                    st.allow_provide_for(Side::No),
                    st.block_reason_for(Side::No),
                    st.hedge_dispatched_for(Side::No),
                    no_toxic_blocked,
                    no_stale,
                    st.pgt_taker_close_limit_for(Side::No),
                ),
            };
            self.dispatch_provide_side(
                inv,
                ub,
                side,
                intent,
                allow_provide,
                block_reason,
                hedge_dispatched,
                toxic_blocked,
                stale,
                shadow_taker_open
                    .filter(|candidate| candidate.side == side)
                    .map(|candidate| candidate.limit_price),
                pgt_taker_close_limit,
            )
            .await;
        }
    }

    pub(super) async fn dispatch_provide_side(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        side: Side,
        intent: Option<StrategyIntent>,
        allow_provide: bool,
        block_reason: Option<CancelReason>,
        hedge_dispatched: bool,
        toxic_blocked: bool,
        stale: bool,
        shadow_taker_limit_price: Option<f64>,
        pgt_taker_close_limit_price: Option<f64>,
    ) {
        if hedge_dispatched {
            return;
        }

        if self.cfg.strategy.is_pair_gated_tranche_arb() && intent.is_some() {
            self.stats.pgt_dispatch_intents = self.stats.pgt_dispatch_intents.saturating_add(1);
        }

        if intent.is_some() && !allow_provide {
            self.note_skipped_inv_limit();
            if self.cfg.strategy.is_pair_gated_tranche_arb() {
                self.stats.pgt_dispatch_blocked = self.stats.pgt_dispatch_blocked.saturating_add(1);
            }
        }

        let action = self.decide_provide_side_action(
            side,
            intent,
            allow_provide,
            block_reason,
            toxic_blocked,
            stale,
            shadow_taker_limit_price,
            pgt_taker_close_limit_price,
        );
        self.apply_provide_side_action(inv, ub, side, action).await;
    }

    pub(super) fn pgt_shadow_taker_open_candidate(
        &self,
        inv: &InventoryState,
        ub: &Book,
        st: &ExecutionState,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
        yes_stale: bool,
        no_stale: bool,
    ) -> Option<PgtShadowTakerOpenCandidate> {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() || !self.cfg.dry_run {
            return None;
        }
        if !PGT_SHADOW_TAKER_OPEN_EXEC_ENABLED {
            // Keep taker-open as a strategy counterfactual only. Executing the
            // first leg aggressively can strand an expensive residual that later
            // needs lossy repair, which is the opposite of PGT's maker-first
            // replication target.
            return None;
        }
        if inv.net_diff.abs() > PAIR_ARB_NET_EPS {
            return None;
        }
        if self.endgame_phase() != EndgamePhase::Normal {
            return None;
        }

        let remaining_secs = self.seconds_to_market_end().unwrap_or(u64::MAX);
        let open_pair_band =
            pgt_effective_open_pair_band_value(self.cfg.open_pair_band, remaining_secs);
        let yes = self.pgt_shadow_taker_open_candidate_for_side(
            ub,
            Side::Yes,
            st.intent_for(Side::Yes),
            st.allow_provide_for(Side::Yes),
            st.hedge_dispatched_for(Side::Yes),
            yes_toxic_blocked,
            yes_stale,
            open_pair_band,
        );
        let no = self.pgt_shadow_taker_open_candidate_for_side(
            ub,
            Side::No,
            st.intent_for(Side::No),
            st.allow_provide_for(Side::No),
            st.hedge_dispatched_for(Side::No),
            no_toxic_blocked,
            no_stale,
            open_pair_band,
        );

        match (yes, no) {
            (Some(lhs), Some(rhs)) => {
                if lhs.slack > rhs.slack + 1e-9 {
                    Some(lhs)
                } else if rhs.slack > lhs.slack + 1e-9 {
                    Some(rhs)
                } else if lhs.best_ask <= rhs.best_ask + 1e-9 {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            (Some(candidate), None) | (None, Some(candidate)) => Some(candidate),
            (None, None) => None,
        }
    }

    fn pgt_shadow_taker_open_candidate_for_side(
        &self,
        ub: &Book,
        side: Side,
        intent: Option<StrategyIntent>,
        allow_provide: bool,
        hedge_dispatched: bool,
        toxic_blocked: bool,
        stale: bool,
        open_pair_band: f64,
    ) -> Option<PgtShadowTakerOpenCandidate> {
        if hedge_dispatched || !allow_provide || toxic_blocked || stale {
            return None;
        }
        let intent = intent?;
        if intent.direction != TradeDirection::Buy || intent.reason != BidReason::Provide {
            return None;
        }
        if self.pgt_same_side_release_quarantine_until[side.index()]
            .is_some_and(|until| until > std::time::Instant::now())
        {
            return None;
        }
        if self.pgt_shadow_taker_open_fired_epoch == Some(self.pgt_decision_epoch) {
            return None;
        }
        let (best_ask, opposite_bid, opposite_ask) = match side {
            Side::Yes => (ub.yes_ask, ub.no_bid, ub.no_ask),
            Side::No => (ub.no_ask, ub.yes_bid, ub.yes_ask),
        };
        if best_ask <= 0.0 || opposite_bid <= 0.0 || opposite_ask <= 0.0 {
            return None;
        }
        // Aggressive first-leg open is only acceptable when the *current*
        // two-ask completion geometry itself still fits inside the broad
        // opening band. Otherwise we are paying taker urgency into a tranche
        // that is already economically stretched before any market movement.
        if best_ask + opposite_ask > open_pair_band + 1e-9 {
            return None;
        }
        let ceiling = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band, opposite_bid)?;
        if best_ask > ceiling + 1e-9 || best_ask <= intent.price + 1e-9 {
            return None;
        }
        if self.side_target_reason(side) == Some(BidReason::Hedge) {
            return None;
        }
        if self.recent_cross_reject(side, std::time::Duration::from_secs(1)) {
            return None;
        }
        Some(PgtShadowTakerOpenCandidate {
            side,
            limit_price: ceiling,
            slack: (ceiling - best_ask).max(0.0),
            best_ask,
        })
    }

    pub(super) fn pgt_shadow_taker_close_limit_for_side(
        &self,
        side: Side,
        maker_price: f64,
        ceiling: f64,
        best_ask: f64,
        remaining_secs: u64,
    ) -> Option<f64> {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() || !self.cfg.dry_run {
            return None;
        }
        if remaining_secs > PGT_SHADOW_TAKER_CLOSE_SECS {
            return None;
        }
        if remaining_secs <= self.cfg.endgame_freeze_secs {
            return None;
        }
        if self.pgt_shadow_taker_close_fired_epoch[side.index()] == Some(self.pgt_decision_epoch) {
            return None;
        }
        if best_ask <= 0.0 || ceiling <= 0.0 || maker_price <= 0.0 {
            return None;
        }
        if best_ask > ceiling + 1e-9 {
            return None;
        }
        Some(self.safe_price(best_ask))
    }

    pub(super) async fn execute_slot_market_making(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        quotes: StrategyQuotes,
        yes_stale: bool,
        no_stale: bool,
        yes_toxic_blocked: bool,
        no_toxic_blocked: bool,
    ) {
        if self.cfg.strategy.is_glft_mm() {
            let glft = *self.glft_rx.borrow();
            if !self.glft_is_tradeable_snapshot(glft) {
                let retain_short_source_block =
                    self.glft_should_retain_on_short_source_block(glft, std::time::Instant::now());
                for slot in OrderSlot::ALL {
                    if self.slot_target_active(slot) {
                        if retain_short_source_block {
                            self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                            self.stats.shadow_suppressed_updates =
                                self.stats.shadow_suppressed_updates.saturating_add(1);
                        } else {
                            self.clear_slot_target(slot, CancelReason::StaleData).await;
                        }
                    }
                }
                return;
            }
        }

        let phase = self.endgame_phase();
        let ofi = *self.ofi_rx.borrow();
        let remaining_secs = self.seconds_to_market_end().unwrap_or(0);

        for slot in OrderSlot::ALL {
            let intent = quotes.get(slot);
            let stale = match slot.side {
                Side::Yes => yes_stale,
                Side::No => no_stale,
            };
            let toxic = match slot.side {
                Side::Yes => yes_toxic_blocked,
                Side::No => no_toxic_blocked,
            };
            if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
                self.slot_pair_arb_intent_state_keys[slot.index()] =
                    intent.map(|_| self.pair_arb_state_key(inv, phase));
                self.slot_pair_arb_intent_epochs[slot.index()] =
                    intent.map(|_| self.pair_arb_decision_epoch);
            } else {
                self.slot_pair_arb_intent_state_keys[slot.index()] = None;
                self.slot_pair_arb_intent_epochs[slot.index()] = None;
            }
            if self.cfg.strategy.is_pair_gated_tranche_arb()
                && slot.direction == TradeDirection::Buy
            {
                self.slot_pgt_intent_epochs[slot.index()] = intent.map(|_| self.pgt_decision_epoch);
            } else {
                self.slot_pgt_intent_epochs[slot.index()] = None;
            }
            if intent.is_some() {
                self.slot_absent_clear_since[slot.index()] = None;
            }

            if intent.is_none() {
                if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
                    self.slot_pair_arb_cross_reject_reprice_pending[slot.index()] = false;
                    self.slot_pair_arb_state_republish_latched[slot.index()] = false;
                }
                if self.slot_target_active(slot) {
                    match self.evaluate_slot_retention(
                        inv,
                        ub,
                        slot,
                        None,
                        CancelReason::Reprice,
                        phase,
                    ) {
                        RetentionDecision::Retain => continue,
                        RetentionDecision::Republish => {}
                        RetentionDecision::Clear(reason, scope) => {
                            self.clear_slot_target_with_scope(slot, reason, scope).await;
                        }
                    }
                }
                continue;
            }

            let allowed = self.slot_quote_allowed(inv, slot, intent, stale, toxic, phase, &ofi);
            if !allowed {
                let reject_reason =
                    self.slot_reject_reason(inv, slot, intent, stale, toxic, phase, &ofi);
                if intent.is_some() && matches!(reject_reason, CancelReason::InventoryLimit) {
                    self.note_skipped_inv_limit();
                }
                if self.slot_target_active(slot) {
                    match self.evaluate_slot_retention(inv, ub, slot, intent, reject_reason, phase)
                    {
                        RetentionDecision::Retain => continue,
                        RetentionDecision::Republish => {}
                        RetentionDecision::Clear(reason, scope) => {
                            debug!(
                                "🧭 slot gate clear {} reason={:?} phase={:?} remaining={}s stale={} toxic={} intent_present={} net_diff={:.2}",
                                slot.as_str(),
                                reason,
                                phase,
                                remaining_secs,
                                stale,
                                toxic,
                                intent.is_some(),
                                inv.net_diff,
                            );
                            self.clear_slot_target_with_scope(slot, reason, scope).await;
                            continue;
                        }
                    }
                }
                continue;
            }

            let Some(intent) = intent else { continue };

            if self.slot_target_active(slot) {
                match self.evaluate_slot_retention(
                    inv,
                    ub,
                    slot,
                    Some(intent),
                    CancelReason::Reprice,
                    phase,
                ) {
                    RetentionDecision::Retain => continue,
                    RetentionDecision::Republish => {}
                    RetentionDecision::Clear(reason, scope) => {
                        self.clear_slot_target_with_scope(slot, reason, scope).await;
                        continue;
                    }
                }
            }

            let log_msg = match self.slot_target(slot) {
                Some(current)
                    if current.direction == intent.direction
                        && current.reason == intent.reason
                        && (current.price - intent.price).abs()
                            < (self.maker_keep_band(intent.reason) - 1e-9).max(0.0)
                        && (current.size - intent.size).abs() <= 0.1 =>
                {
                    None
                }
                _ => Some(format!(
                    "📐 SLOT {} {:?}@{:.3} sz={:.1}",
                    slot.as_str(),
                    intent.direction,
                    intent.price,
                    intent.size
                )),
            };

            self.slot_place_or_reprice(slot, intent.price, intent.size, intent.reason, log_msg)
                .await;
        }
    }

    pub(super) fn evaluate_slot_retention(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        slot: OrderSlot,
        intent: Option<StrategyIntent>,
        reject_reason: CancelReason,
        phase: EndgamePhase,
    ) -> RetentionDecision {
        let Some(current) = self.slot_target(slot).cloned() else {
            return RetentionDecision::Republish;
        };

        if let Some(intent) = intent {
            if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
                if self.pair_arb_should_retain_existing(inv, ub, slot, intent, phase) {
                    self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                    return RetentionDecision::Retain;
                }
                return RetentionDecision::Republish;
            }
            if self.keep_slot_target_if_safe(inv, ub, slot, Some(intent), phase) {
                self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                return RetentionDecision::Retain;
            }
            return RetentionDecision::Republish;
        }

        if self.cfg.strategy.is_oracle_lag_sniping()
            && current.reason == BidReason::OracleLagProvide
            && matches!(reject_reason, CancelReason::Reprice)
            && phase == EndgamePhase::Normal
        {
            let keep_same_winner_side = self.is_in_post_close_window()
                && self.oracle_lag_is_selected()
                && !self.oracle_lag_round_halted
                && self.post_close_winner_side == Some(slot.side);
            if keep_same_winner_side {
                self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                debug!(
                    "🧭 oracle_lag retain existing maker | slot={} winner_side={:?} selected={} halted={} in_post_close={}",
                    slot.as_str(),
                    self.post_close_winner_side,
                    self.oracle_lag_is_selected(),
                    self.oracle_lag_round_halted,
                    self.is_in_post_close_window(),
                );
                return RetentionDecision::Retain;
            }
            debug!(
                "🧭 oracle_lag clear maker | slot={} winner_side={:?} selected={} halted={} in_post_close={}",
                slot.as_str(),
                self.post_close_winner_side,
                self.oracle_lag_is_selected(),
                self.oracle_lag_round_halted,
                self.is_in_post_close_window(),
            );
            return RetentionDecision::Clear(
                reject_reason,
                self.default_slot_reset_scope(reject_reason),
            );
        }

        if !self.cfg.strategy.is_glft_mm() && !self.cfg.strategy.is_pair_arb()
            || !matches!(reject_reason, CancelReason::Reprice)
            || phase != EndgamePhase::Normal
        {
            return RetentionDecision::Clear(
                reject_reason,
                self.default_slot_reset_scope(reject_reason),
            );
        }

        if self.cfg.strategy.is_pair_arb() {
            let now = std::time::Instant::now();
            let idx = slot.index();
            let state_key = self.pair_arb_state_key(inv, phase);
            let prev_state = self.slot_pair_arb_state_keys[idx];
            let state_changed = prev_state.is_some() && prev_state != Some(state_key);
            let fill_recheck = self.slot_pair_arb_fill_recheck_pending[idx];
            if state_changed {
                debug!(
                    "🧭 pair_arb absent-intent state change clear | slot={} prev_state={:?} current_state={:?}",
                    slot.as_str(),
                    prev_state,
                    state_key,
                );
                self.slot_absent_clear_since[idx] = None;
                self.slot_pair_arb_state_keys[idx] = Some(state_key);
                self.slot_pair_arb_fill_recheck_pending[idx] = false;
                self.slot_pair_arb_state_republish_latched[idx] = false;
                return RetentionDecision::Clear(reject_reason, SlotResetScope::Soft);
            }
            if fill_recheck {
                let admissible = self.pair_arb_quote_still_admissible(
                    inv,
                    ub,
                    slot,
                    current.price,
                    current.size,
                    phase,
                );
                debug!(
                "🧭 pair_arb absent-intent recheck | slot={} state_key_changed={} fill_recheck_pending={} net_bucket={:?} dominant_side={:?} risk_open_cutoff_active={} state_change_republish={}",
                    slot.as_str(),
                    state_changed,
                    fill_recheck,
                    state_key.net_bucket,
                    state_key.dominant_side,
                    state_key.risk_open_cutoff_active,
                    !admissible,
                );
                if admissible && self.keep_slot_target_if_safe(inv, ub, slot, None, phase) {
                    self.slot_pair_arb_state_keys[idx] = Some(state_key);
                    self.slot_pair_arb_fill_recheck_pending[idx] = false;
                    self.slot_pair_arb_state_republish_latched[idx] = false;
                    self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                    return RetentionDecision::Retain;
                }
                self.slot_absent_clear_since[idx] = None;
                self.slot_pair_arb_fill_recheck_pending[idx] = false;
                self.slot_pair_arb_state_republish_latched[idx] = false;
                return RetentionDecision::Clear(reject_reason, SlotResetScope::Soft);
            }
            let since = self.slot_absent_clear_since[idx].get_or_insert(now);
            let elapsed = now.saturating_duration_since(*since);
            let warmup_dwell = std::time::Duration::from_millis(1_200);
            if elapsed < warmup_dwell {
                self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                return RetentionDecision::Retain;
            }

            if self.keep_slot_target_if_safe(inv, ub, slot, None, phase) {
                self.slot_pair_arb_state_keys[idx] = Some(state_key);
                self.slot_pair_arb_fill_recheck_pending[idx] = false;
                self.slot_pair_arb_state_republish_latched[idx] = false;
                self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                return RetentionDecision::Retain;
            }

            self.slot_absent_clear_since[idx] = None;
            self.slot_pair_arb_state_republish_latched[idx] = false;
            return RetentionDecision::Clear(reject_reason, SlotResetScope::Soft);
        }

        let glft = *self.glft_rx.borrow();
        if !self.glft_is_tradeable_snapshot(glft)
            || matches!(
                glft.quote_regime,
                crate::polymarket::glft::QuoteRegime::Blocked
            )
        {
            self.slot_absent_clear_since[slot.index()] = None;
            return RetentionDecision::Clear(
                reject_reason,
                self.default_slot_reset_scope(reject_reason),
            );
        }

        let now = std::time::Instant::now();
        let idx = slot.index();
        let since = self.slot_absent_clear_since[idx].get_or_insert(now);
        let elapsed = now.saturating_duration_since(*since);
        let warmup_dwell = match glft.quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => {
                std::time::Duration::from_millis(1_200)
            }
            crate::polymarket::glft::QuoteRegime::Tracking => {
                std::time::Duration::from_millis(1_800)
            }
            crate::polymarket::glft::QuoteRegime::Guarded => {
                std::time::Duration::from_millis(4_000)
            }
            crate::polymarket::glft::QuoteRegime::Blocked => std::time::Duration::ZERO,
        };
        if elapsed < warmup_dwell {
            self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
            self.stats.shadow_suppressed_updates =
                self.stats.shadow_suppressed_updates.saturating_add(1);
            return RetentionDecision::Retain;
        }

        let tick = self.cfg.tick_size.max(1e-9);
        let trusted_side_mid = match slot.side {
            Side::Yes => glft.trusted_mid,
            Side::No => 1.0 - glft.trusted_mid,
        };
        let trusted_dist_ticks = ((current.price - trusted_side_mid).abs() / tick).floor();
        let hard_stale_trusted_ticks = match glft.quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => 16.0,
            crate::polymarket::glft::QuoteRegime::Tracking => 14.0,
            crate::polymarket::glft::QuoteRegime::Guarded => 14.0,
            crate::polymarket::glft::QuoteRegime::Blocked => 0.0,
        };
        let republish_settling = self.glft_republish_settle_remaining(now).is_some();
        let trusted_soft_hold_cap =
            hard_stale_trusted_ticks + if republish_settling { 2.0 } else { 0.0 };
        if trusted_dist_ticks <= trusted_soft_hold_cap {
            self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
            self.stats.shadow_suppressed_updates =
                self.stats.shadow_suppressed_updates.saturating_add(1);
            return RetentionDecision::Retain;
        }

        self.slot_absent_clear_since[idx] = None;
        RetentionDecision::Clear(reject_reason, SlotResetScope::Soft)
    }

    fn keep_slot_target_if_safe(
        &self,
        inv: &InventoryState,
        ub: &Book,
        slot: OrderSlot,
        desired: Option<StrategyIntent>,
        phase: EndgamePhase,
    ) -> bool {
        let Some(current) = self.slot_target(slot).cloned() else {
            return false;
        };
        let (best_bid, best_ask) = match slot.side {
            Side::Yes => (ub.yes_bid, ub.yes_ask),
            Side::No => (ub.no_bid, ub.no_ask),
        };
        let desired_intent = desired.unwrap_or(StrategyIntent {
            side: slot.side,
            direction: current.direction,
            price: current.price,
            size: current.size,
            reason: current.reason,
        });
        let kept_intent = StrategyIntent {
            side: slot.side,
            direction: current.direction,
            price: current.price,
            size: current.size,
            reason: current.reason,
        };

        current.direction == desired_intent.direction
            && self.keep_existing_maker_if_safe(
                inv,
                slot.side,
                desired_intent.direction,
                desired_intent.price,
                desired_intent.size,
                desired_intent.price,
                best_bid,
                best_ask,
                desired_intent.reason,
            )
            && self.can_place_strategy_intent(inv, Some(kept_intent))
            && (phase == EndgamePhase::Normal
                || self.projected_abs_net_diff(inv.net_diff, kept_intent)
                    <= inv.net_diff.abs() + 1e-6)
    }

    pub(super) fn slot_quote_allowed(
        &mut self,
        inv: &InventoryState,
        slot: OrderSlot,
        intent: Option<StrategyIntent>,
        stale: bool,
        toxic_blocked: bool,
        phase: EndgamePhase,
        ofi: &OfiSnapshot,
    ) -> bool {
        let Some(intent) = intent else {
            return false;
        };
        if self.cfg.strategy.is_pair_gated_tranche_arb()
            && intent.direction == TradeDirection::Buy
            && self.pgt_same_side_release_quarantine_until[intent.side.index()]
                .is_some_and(|until| until > std::time::Instant::now())
        {
            return false;
        }
        // OracleLagSniping decides winner from Chainlink, not the book.
        // Post-close books are naturally stale; blocking on staleness would
        // suppress every post-close maker order for this strategy.
        let stale_blocks = stale && !self.cfg.strategy.bypasses_stale_market_gate();
        if stale_blocks {
            return false;
        }
        if toxic_blocked && self.slot_blocked_by_ofi(slot, ofi) {
            return false;
        }
        if !self.can_place_strategy_intent(inv, Some(intent)) {
            return false;
        }
        if phase == EndgamePhase::Normal {
            return true;
        }
        self.projected_abs_net_diff(inv.net_diff, intent) <= inv.net_diff.abs() + 1e-6
    }

    fn slot_reject_reason(
        &self,
        inv: &InventoryState,
        slot: OrderSlot,
        intent: Option<StrategyIntent>,
        stale: bool,
        toxic_blocked: bool,
        phase: EndgamePhase,
        ofi: &OfiSnapshot,
    ) -> CancelReason {
        let stale_blocks = stale && !self.cfg.strategy.bypasses_stale_market_gate();
        if stale_blocks {
            return CancelReason::StaleData;
        }
        if toxic_blocked && self.slot_blocked_by_ofi(slot, ofi) {
            return CancelReason::ToxicFlow;
        }
        let Some(intent) = intent else {
            return CancelReason::Reprice;
        };
        if !self.can_place_strategy_intent(inv, Some(intent)) {
            return CancelReason::InventoryLimit;
        }
        if phase != EndgamePhase::Normal
            && self.projected_abs_net_diff(inv.net_diff, intent) > inv.net_diff.abs() + 1e-6
        {
            return CancelReason::EndgameRiskGate;
        }
        CancelReason::Reprice
    }

    pub(super) fn slot_blocked_by_ofi(&self, slot: OrderSlot, ofi: &OfiSnapshot) -> bool {
        let side_ofi = match slot.side {
            Side::Yes => ofi.yes,
            Side::No => ofi.no,
        };
        side_ofi.blocks(slot.direction)
    }

    #[allow(dead_code)]
    fn pair_arb_opposite_slot_blocked(&self, side: Side) -> bool {
        if !self.cfg.strategy.is_pair_arb() {
            return false;
        }
        let opposite = match side {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        };
        let slot = OrderSlot::new(opposite, TradeDirection::Buy);
        let idx = slot.index();
        let Some(last_ts) = self.pair_arb_slot_blocked_at[idx] else {
            return false;
        };
        if last_ts.elapsed()
            > std::time::Duration::from_millis(super::PAIR_ARB_OPPOSITE_SLOT_BLOCK_TTL_MS)
        {
            return false;
        }
        self.pair_arb_slot_blocked_for_ms[idx] >= super::PAIR_ARB_OPPOSITE_SLOT_BLOCK_MS
    }

    pub(super) fn decide_provide_side_action(
        &self,
        side: Side,
        intent: Option<StrategyIntent>,
        allow_provide: bool,
        block_reason: Option<CancelReason>,
        toxic_blocked: bool,
        stale: bool,
        shadow_taker_limit_price: Option<f64>,
        pgt_taker_close_limit_price: Option<f64>,
    ) -> ProvideSideAction {
        if let Some(intent) = intent {
            if !allow_provide {
                return ProvideSideAction::Clear {
                    reason: block_reason.unwrap_or(CancelReason::InventoryLimit),
                };
            }
            if intent.price > 0.0 && intent.size > 0.0 {
                if intent.reason == BidReason::Provide {
                    if let Some(limit_price) = shadow_taker_limit_price {
                        return ProvideSideAction::ShadowTaker {
                            intent,
                            limit_price,
                        };
                    }
                }
                if intent.reason == BidReason::Hedge
                    && self.cfg.strategy.is_pair_gated_tranche_arb()
                    && self.cfg.dry_run
                {
                    if self.pgt_shadow_taker_close_fired_epoch[side.index()]
                        == Some(self.pgt_decision_epoch)
                    {
                        return ProvideSideAction::None;
                    }
                    if PGT_SHADOW_TAKER_CLOSE_EXEC_ENABLED {
                        if let Some(limit_price) = pgt_taker_close_limit_price {
                            return ProvideSideAction::ShadowTakerClose {
                                intent,
                                limit_price,
                            };
                        }
                    }
                }
                return ProvideSideAction::Place { intent };
            }
        }
        if toxic_blocked {
            return if self.should_clear_on_toxic(side) {
                ProvideSideAction::Clear {
                    reason: CancelReason::ToxicFlow,
                }
            } else {
                ProvideSideAction::None
            };
        }
        if stale {
            if self.pgt_should_retain_existing_buy_on_stale(side) {
                return ProvideSideAction::None;
            }
            return ProvideSideAction::Clear {
                reason: CancelReason::StaleData,
            };
        }
        if matches!(self.side_target_reason(side), Some(BidReason::Provide)) {
            return ProvideSideAction::Clear {
                reason: CancelReason::Reprice,
            };
        }
        ProvideSideAction::None
    }

    fn pgt_should_retain_existing_buy_on_stale(&self, side: Side) -> bool {
        if !self.cfg.strategy.is_pair_gated_tranche_arb() {
            return false;
        }
        matches!(
            self.side_target_reason(side),
            Some(BidReason::Provide | BidReason::Hedge)
        )
    }

    pub(super) async fn apply_provide_side_action(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        side: Side,
        action: ProvideSideAction,
    ) {
        match action {
            ProvideSideAction::None => {}
            ProvideSideAction::Place { intent } => {
                let slot = OrderSlot::new(side, intent.direction);
                let phase = self.endgame_phase();
                let should_retain = if self.cfg.strategy.is_pair_arb() {
                    self.pair_arb_should_retain_existing(inv, ub, slot, intent, phase)
                } else if self.cfg.strategy.is_pair_gated_tranche_arb()
                    && slot.direction == TradeDirection::Buy
                {
                    self.pgt_should_retain_existing(inv, ub, slot, intent)
                } else {
                    let (best_bid, best_ask) = match side {
                        Side::Yes => (ub.yes_bid, ub.yes_ask),
                        Side::No => (ub.no_bid, ub.no_ask),
                    };
                    self.keep_existing_maker_if_safe(
                        inv,
                        side,
                        intent.direction,
                        intent.price,
                        intent.size,
                        intent.price,
                        best_bid,
                        best_ask,
                        intent.reason,
                    )
                };
                if should_retain {
                    self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                    if self.cfg.strategy.is_pair_gated_tranche_arb() {
                        self.stats.pgt_dispatch_retain =
                            self.stats.pgt_dispatch_retain.saturating_add(1);
                    }
                    return;
                }
                if self.cfg.strategy.is_pair_gated_tranche_arb() {
                    self.stats.pgt_dispatch_place = self.stats.pgt_dispatch_place.saturating_add(1);
                }
                self.place_or_reprice(
                    side,
                    intent.direction,
                    intent.price,
                    intent.size,
                    intent.reason,
                    None,
                )
                .await;
            }
            ProvideSideAction::ShadowTaker {
                intent,
                limit_price,
            } => {
                if self.cfg.strategy.is_pair_gated_tranche_arb() {
                    self.pgt_shadow_taker_open_fired_epoch = Some(self.pgt_decision_epoch);
                    self.stats.pgt_dispatch_taker_open =
                        self.stats.pgt_dispatch_taker_open.saturating_add(1);
                }
                info!(
                    "⚡ PGT shadow taker-open | side={:?} price={:.4} size={:.2} limit={:.4}",
                    side, intent.price, intent.size, limit_price
                );
                self.dispatch_taker_intent(
                    side,
                    intent.direction,
                    intent.size,
                    TradePurpose::Provide,
                    Some(limit_price),
                )
                .await;
            }
            ProvideSideAction::ShadowTakerClose {
                intent,
                limit_price,
            } => {
                if self.cfg.strategy.is_pair_gated_tranche_arb() {
                    self.pgt_shadow_taker_close_fired_epoch[side.index()] =
                        Some(self.pgt_decision_epoch);
                    self.stats.pgt_dispatch_taker_close =
                        self.stats.pgt_dispatch_taker_close.saturating_add(1);
                }
                info!(
                    "⚡ PGT shadow taker-close | side={:?} price={:.4} size={:.2} limit={:.4}",
                    side, intent.price, intent.size, limit_price
                );
                self.dispatch_taker_intent(
                    side,
                    intent.direction,
                    intent.size,
                    TradePurpose::Hedge,
                    Some(limit_price),
                )
                .await;
            }
            ProvideSideAction::Clear { reason } => {
                if self.cfg.strategy.is_pair_gated_tranche_arb() {
                    self.stats.pgt_dispatch_clear = self.stats.pgt_dispatch_clear.saturating_add(1);
                }
                if self.cfg.strategy.is_pair_arb() {
                    let slot = OrderSlot::new(side, TradeDirection::Buy);
                    match self.evaluate_slot_retention(
                        inv,
                        ub,
                        slot,
                        None,
                        reason,
                        self.endgame_phase(),
                    ) {
                        RetentionDecision::Retain => return,
                        RetentionDecision::Republish => {}
                        RetentionDecision::Clear(clear_reason, scope) => {
                            self.clear_slot_target_with_scope(slot, clear_reason, scope)
                                .await;
                            return;
                        }
                    }
                }
                self.clear_target(side, reason).await;
            }
        }
    }
}
