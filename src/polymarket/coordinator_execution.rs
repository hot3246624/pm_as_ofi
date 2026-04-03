use tracing::debug;

use super::*;

impl StrategyCoordinator {
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

    pub(super) fn should_execute_directional_hedges(&self, st: &ExecutionState) -> bool {
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
            force_taker_side: None,
            force_taker_size: 0.0,
            block_maker_hedge: false,
            endgame_phase: self.endgame_phase(),
        }
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

        self.dispatch_provide_side(
            inv,
            ub,
            Side::Yes,
            st.intent_for(Side::Yes),
            st.allow_provide_for(Side::Yes),
            st.block_reason_for(Side::Yes),
            st.hedge_dispatched_for(Side::Yes),
            yes_toxic_blocked,
            yes_stale,
        )
        .await;
        self.dispatch_provide_side(
            inv,
            ub,
            Side::No,
            st.intent_for(Side::No),
            st.allow_provide_for(Side::No),
            st.block_reason_for(Side::No),
            st.hedge_dispatched_for(Side::No),
            no_toxic_blocked,
            no_stale,
        )
        .await;
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
    ) {
        if hedge_dispatched {
            return;
        }

        if intent.is_some() && !allow_provide {
            self.note_skipped_inv_limit();
        }

        let action = self.decide_provide_side_action(
            side,
            intent,
            allow_provide,
            block_reason,
            toxic_blocked,
            stale,
        );
        self.apply_provide_side_action(inv, ub, side, action).await;
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
        if self.cfg.strategy == StrategyKind::GlftMm {
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
            if intent.is_some() {
                self.slot_absent_clear_since[slot.index()] = None;
            }

            if intent.is_none() {
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
            if self.keep_slot_target_if_safe(inv, ub, slot, Some(intent), phase) {
                self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                return RetentionDecision::Retain;
            }
            return RetentionDecision::Republish;
        }

        if self.cfg.strategy != StrategyKind::GlftMm
            || !matches!(reject_reason, CancelReason::Reprice)
            || phase != EndgamePhase::Normal
        {
            return RetentionDecision::Clear(
                reject_reason,
                self.default_slot_reset_scope(reject_reason),
            );
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

    fn slot_quote_allowed(
        &self,
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
        if stale {
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
        if stale {
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

    pub(super) fn decide_provide_side_action(
        &self,
        side: Side,
        intent: Option<StrategyIntent>,
        allow_provide: bool,
        block_reason: Option<CancelReason>,
        toxic_blocked: bool,
        stale: bool,
    ) -> ProvideSideAction {
        if let Some(intent) = intent {
            if !allow_provide {
                return ProvideSideAction::Clear {
                    reason: block_reason.unwrap_or(CancelReason::InventoryLimit),
                };
            }
            if intent.price > 0.0 && intent.size > 0.0 {
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
                let (best_bid, best_ask) = match side {
                    Side::Yes => (ub.yes_bid, ub.yes_ask),
                    Side::No => (ub.no_bid, ub.no_ask),
                };
                if self.keep_existing_maker_if_safe(
                    inv,
                    side,
                    intent.direction,
                    intent.price,
                    intent.size,
                    intent.price,
                    best_bid,
                    best_ask,
                    intent.reason,
                ) {
                    return;
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
            ProvideSideAction::Clear { reason } => {
                self.clear_target(side, reason).await;
            }
        }
    }
}
