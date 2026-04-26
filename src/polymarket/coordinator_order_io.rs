use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::{debug, info, warn};

use crate::polymarket::glft::{
    compute_glft_alpha_shift, compute_optimal_offsets, shape_glft_quotes, GlftFitSource,
    IntensityFitSnapshot,
};

use super::*;

impl StrategyCoordinator {
    fn reset_oracle_lag_hint_book_cache(&mut self) {
        self.post_close_hint_winner_bid = 0.0;
        self.post_close_hint_winner_ask_raw = 0.0;
        self.post_close_hint_book_source = "none";
        self.post_close_hint_distance_to_final_ms = u64::MAX;
    }

    async fn clear_oracle_lag_live_maker_orders(&mut self, reason: &'static str) {
        if !self.cfg.strategy.is_oracle_lag_sniping() {
            return;
        }
        self.oracle_lag_maker_state_key = None;
        self.oracle_lag_maker_followup_last_ts = None;
        self.oracle_lag_maker_followup_last_candidate_price = std::array::from_fn(|_| None);
        let mut cleared = 0u8;
        for slot in [OrderSlot::YES_BUY, OrderSlot::NO_BUY] {
            let is_oracle_lag_maker = self
                .slot_target(slot)
                .is_some_and(|t| t.reason == BidReason::OracleLagProvide);
            if is_oracle_lag_maker {
                self.clear_slot_target(slot, CancelReason::Reprice).await;
                cleared = cleared.saturating_add(1);
            }
        }
        if cleared > 0 {
            info!(
                "🧹 oracle_lag_maker_cleanup | reason={} cleared_slots={}",
                reason, cleared
            );
        }
    }

    pub(super) async fn maybe_oracle_lag_followup_upward_reprice(&mut self) {
        if !self.cfg.strategy.is_oracle_lag_sniping()
            || !self.cfg.oracle_lag_sniping.market_enabled
            || !self.is_in_post_close_window()
            || self.oracle_lag_round_halted
        {
            return;
        }
        let Some(side) = self.post_close_winner_side else {
            return;
        };
        let slot = OrderSlot::new(side, TradeDirection::Buy);
        let Some(current) = self.slot_target(slot).cloned() else {
            return;
        };
        if current.reason != BidReason::OracleLagProvide || current.direction != TradeDirection::Buy
        {
            return;
        }
        let (winner_bid, winner_ask) = match side {
            Side::Yes => (self.book.yes_bid, self.book.yes_ask),
            Side::No => (self.book.no_bid, self.book.no_ask),
        };
        if winner_bid <= 0.0 {
            return;
        }
        let tick = if winner_bid > ORACLE_LAG_MICRO_TICK_BID_BOUNDARY
            || winner_ask > 0.96
            || (winner_bid > 0.0 && winner_bid < 0.04)
            || (winner_ask > 0.0 && winner_ask < 0.04)
        {
            0.001
        } else {
            self.cfg.tick_size.max(1e-9)
        };
        let winner_ask_tradable =
            winner_ask > 0.0 && (winner_bid <= 0.0 || winner_ask > winner_bid + 0.5 * tick + 1e-9);
        if winner_ask_tradable {
            return;
        }
        if current.price >= ORACLE_LAG_MAKER_MAX_PRICE - 1e-9 {
            return;
        }
        if self.oracle_lag_maker_followup_last_ts.is_some_and(|prev| {
            prev.elapsed() < Duration::from_millis(ORACLE_LAG_MAKER_FOLLOWUP_MIN_INTERVAL_MS)
        }) {
            return;
        }
        if self.slot_last_ts(slot).elapsed()
            < Duration::from_millis(ORACLE_LAG_MAKER_FOLLOWUP_INFLIGHT_LOCK_MS)
        {
            debug!(
                "⏭️ oracle_lag_followup_skip | slot={} side={:?} reason=inflight_lock live={:.4}",
                slot.as_str(),
                side,
                current.price
            );
            return;
        }
        let step = if tick <= 0.001 + 1e-12 {
            tick
        } else {
            tick * 0.1
        };
        let candidate = (winner_bid + step)
            .min(ORACLE_LAG_MAKER_MAX_PRICE)
            .max(step);
        if candidate <= current.price + 0.5 * tick {
            return;
        }
        if let Some(prev_candidate) =
            self.oracle_lag_maker_followup_last_candidate_price[slot.index()]
        {
            let same_candidate = (candidate - prev_candidate).abs() <= 0.5 * tick + 1e-9;
            if same_candidate
                && self.oracle_lag_maker_followup_last_ts.is_some_and(|prev| {
                    prev.elapsed()
                        < Duration::from_millis(
                            ORACLE_LAG_MAKER_FOLLOWUP_SAME_CANDIDATE_COOLDOWN_MS,
                        )
                })
            {
                debug!(
                    "⏭️ oracle_lag_followup_skip | slot={} side={:?} reason=same_candidate_cooldown live={:.4} candidate={:.4} prev_candidate={:.4}",
                    slot.as_str(),
                    side,
                    current.price,
                    candidate,
                    prev_candidate
                );
                return;
            }
        }

        self.oracle_lag_maker_followup_last_ts = Some(Instant::now());
        self.oracle_lag_maker_followup_last_candidate_price[slot.index()] = Some(candidate);
        info!(
            "🎯 oracle_lag_followup_upward | slot={} side={:?} live={:.4} bid={:.4} ask={:.4} next={:.4} tick={:.4}",
            slot.as_str(),
            side,
            current.price,
            winner_bid,
            winner_ask,
            candidate,
            tick
        );
        self.slot_place_or_reprice(
            slot,
            candidate,
            current.size,
            BidReason::OracleLagProvide,
            Some(format!(
                "oracle_lag_followup_upward | winner_bid={:.4} winner_ask={:.4}",
                winner_bid, winner_ask
            )),
        )
        .await;
    }

    // ═════════════════════════════════════════════════
    // Place / Reprice with debounce
    // ═════════════════════════════════════════════════

    pub(super) async fn place_or_reprice(
        &mut self,
        side: Side,
        direction: TradeDirection,
        price: f64,
        size: f64,
        reason: BidReason,
        log_msg: Option<String>,
    ) {
        self.slot_place_or_reprice(
            OrderSlot::new(side, direction),
            price,
            size,
            reason,
            log_msg,
        )
        .await;
    }

    pub(super) async fn slot_place_or_reprice(
        &mut self,
        slot: OrderSlot,
        mut price: f64,
        size: f64,
        reason: BidReason,
        log_msg: Option<String>,
    ) {
        if self.cfg.strategy.is_oracle_lag_sniping()
            && self.cfg.oracle_lag_sniping.lab_only
            && reason == BidReason::OracleLagProvide
        {
            info!(
                "🧪 oracle_lag_lab_skip_maker | slot={} side={:?} direction={:?} price={:.4} size={:.4} reason={:?}",
                slot.as_str(),
                slot.side,
                slot.direction,
                price,
                size,
                reason
            );
            return;
        }
        let current_target = self.slot_target(slot).cloned();
        let last_ts = self.slot_last_ts(slot);
        let active = current_target.is_some();
        let slot_price = current_target.as_ref().map(|t| t.price).unwrap_or(0.0);
        let slot_size = current_target.as_ref().map(|t| t.size).unwrap_or(0.0);
        let slot_direction = current_target.as_ref().map(|t| t.direction);
        let now = Instant::now();
        let reprice_eps = 1e-9;
        let raw_target_price = price;
        let pair_arb_state_changed = if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
            && active
        {
            let inv = self.current_working_inventory();
            let phase = self.endgame_phase();
            self.slot_pair_arb_state_keys[slot.index()].is_some()
                && self.slot_pair_arb_state_keys[slot.index()]
                    != Some(self.pair_arb_state_key(&inv, phase))
        } else {
            false
        };
        let pair_arb_fill_recheck_pending = self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
            && self.slot_pair_arb_fill_recheck_pending[slot.index()];
        let pair_arb_cross_reject_reprice_pending = self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
            && self.slot_pair_arb_cross_reject_reprice_pending[slot.index()];
        let pair_arb_expected_state = if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            self.slot_pair_arb_intent_state_keys[slot.index()]
        } else {
            None
        };
        let pair_arb_expected_epoch = if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            self.slot_pair_arb_intent_epochs[slot.index()]
        } else {
            None
        };
        let pair_arb_current_state = if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            let inv = self.current_working_inventory();
            let phase = self.endgame_phase();
            Some(self.pair_arb_state_key(&inv, phase))
        } else {
            None
        };
        let pair_arb_current_epoch = if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            Some(self.pair_arb_decision_epoch)
        } else {
            None
        };
        if let (Some(expected_epoch), Some(current_epoch)) =
            (pair_arb_expected_epoch, pair_arb_current_epoch)
        {
            if expected_epoch != current_epoch {
                self.slot_pair_arb_intent_state_keys[slot.index()] = None;
                self.slot_pair_arb_intent_epochs[slot.index()] = None;
                self.stats.pair_arb_stale_target_dropped =
                    self.stats.pair_arb_stale_target_dropped.saturating_add(1);
                info!(
                    "🧭 pair_arb_stale_target_dropped | slot={} pair_arb_target_epoch={} pair_arb_current_epoch={} target_price={:.4} target_size={:.2}",
                    slot.as_str(),
                    expected_epoch,
                    current_epoch,
                    price,
                    size,
                );
                return;
            }
        }
        if let (
            Some(expected_state),
            Some(current_state),
            Some(expected_epoch),
            Some(current_epoch),
        ) = (
            pair_arb_expected_state,
            pair_arb_current_state,
            pair_arb_expected_epoch,
            pair_arb_current_epoch,
        ) {
            self.slot_pair_arb_intent_state_keys[slot.index()] = None;
            self.slot_pair_arb_intent_epochs[slot.index()] = None;
            if expected_state != current_state {
                self.stats.pair_arb_stale_target_dropped =
                    self.stats.pair_arb_stale_target_dropped.saturating_add(1);
                info!(
                    "🧭 pair_arb_stale_target_dropped | slot={} expected_state={:?} current_state={:?} pair_arb_target_epoch={} pair_arb_current_epoch={} target_price={:.4} target_size={:.2}",
                    slot.as_str(),
                    expected_state,
                    current_state,
                    expected_epoch,
                    current_epoch,
                    price,
                    size,
                );
                return;
            }
        } else if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            self.slot_pair_arb_intent_state_keys[slot.index()] = None;
            self.slot_pair_arb_intent_epochs[slot.index()] = None;
        }
        if self.cfg.strategy.is_glft_mm() && reason == BidReason::Provide {
            price = self.glft_clamp_slot_target_price(slot, price);
        }
        let target_input_price = price;
        let normalized_target_price = if active {
            self.quantize_toward_target(slot_price, target_input_price)
        } else {
            self.safe_price(target_input_price)
        };
        let action_price: f64;

        // GLFT contract: signal not ready => no quoting.
        // This second-line guard protects against intra-tick races where strategy
        // emitted intent from a just-live snapshot but signal regressed before IO.
        if self.cfg.strategy.is_glft_mm() {
            let glft = *self.glft_rx.borrow();
            if !self.glft_is_tradeable_snapshot(glft) {
                if active && !self.glft_should_retain_on_short_source_block(glft, Instant::now()) {
                    self.clear_slot_target(slot, CancelReason::StaleData).await;
                } else if active {
                    self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                    self.stats.shadow_suppressed_updates =
                        self.stats.shadow_suppressed_updates.saturating_add(1);
                }
                return;
            }
        }

        // OPTIMIZATION: Bypassing debounce for 0.0 price (Cancellation).
        // If we want to cancel, we should do it immediately, especially during toxic/stale events.
        // Also, skip redundant ClearTarget if no order is active.
        if price <= 0.0 {
            if active {
                self.clear_slot_target(slot, CancelReason::InventoryLimit)
                    .await;
            }
            return;
        }

        let debounce_ms = match reason {
            BidReason::Hedge => self.cfg.hedge_debounce_ms,
            BidReason::Provide => self.cfg.debounce_ms,
            // Oracle-lag hot path: winner hint arrives late by nature, so placement
            // must not be throttled by generic maker debounce.
            BidReason::OracleLagProvide => 0,
        };
        let elapsed = last_ts.elapsed();
        let debounce = std::time::Duration::from_millis(debounce_ms);
        if elapsed < debounce {
            self.stats.skipped_debounce += 1;
            return;
        }
        let recent_cross_reject =
            self.recent_cross_reject(slot.side, std::time::Duration::from_secs(2));
        let recovery_publish_ready =
            self.should_publish_recovery_for_slot(slot, std::time::Duration::from_secs(2), now);

        if self.cfg.strategy.is_glft_mm() && reason == BidReason::Provide {
            self.slot_place_or_reprice_glft_policy(
                slot,
                raw_target_price,
                normalized_target_price,
                size,
                reason,
                log_msg.as_deref(),
                recovery_publish_ready,
                now,
            )
            .await;
            return;
        }

        let force_glft_drift_reprice = self.cfg.strategy.is_glft_mm() && active && {
            let tick = self.cfg.tick_size.max(1e-9);
            let drift_cap = match self.glft_rx.borrow().signal_state {
                crate::polymarket::glft::GlftSignalState::Bootstrapping
                | crate::polymarket::glft::GlftSignalState::Assimilating => tick,
                crate::polymarket::glft::GlftSignalState::Live => 2.0 * tick,
            };
            // Prevent freshly placed slots from self-canceling immediately when model center
            // jitters by a few ticks on adjacent heartbeats.
            let min_age_for_drift_force = std::time::Duration::from_millis(1_500);
            if elapsed < min_age_for_drift_force {
                false
            } else {
                match slot.direction {
                    TradeDirection::Buy => slot_price + drift_cap < target_input_price,
                    TradeDirection::Sell => slot_price > target_input_price + drift_cap,
                }
            }
        };

        if self.cfg.strategy.is_glft_mm() && active && !recent_cross_reject {
            let tick = self.cfg.tick_size.max(1e-9);
            let gap = (normalized_target_price - slot_price).abs();
            if gap > tick {
                let gap_ticks = (gap / tick).floor();
                let signal_state = self.glft_rx.borrow().signal_state;
                let step_ticks = match signal_state {
                    crate::polymarket::glft::GlftSignalState::Bootstrapping
                    | crate::polymarket::glft::GlftSignalState::Assimilating => 1.0,
                    crate::polymarket::glft::GlftSignalState::Live => {
                        self.glft_governor_step_ticks(gap_ticks)
                    }
                };
                action_price =
                    self.glft_governed_price(slot_price, normalized_target_price, step_ticks);
            } else {
                action_price = normalized_target_price;
            }
        } else {
            action_price = normalized_target_price;
        }
        price = action_price;

        let glft_snapshot = if self.cfg.strategy.is_glft_mm() {
            Some(*self.glft_rx.borrow())
        } else {
            None
        };
        let glft_quote_regime = glft_snapshot.map(|s| s.quote_regime);
        let glft_soft_stale = glft_snapshot.map(|s| s.poly_soft_stale).unwrap_or(false);
        let glft_shadow_mode = self.cfg.strategy.is_glft_mm() && reason == BidReason::Provide;
        let regime_stable_for_publish = if glft_shadow_mode {
            let regime_age = self
                .update_slot_regime_state(slot, glft_quote_regime, now)
                .unwrap_or_default();
            regime_age >= Self::glft_regime_publish_settle_dwell(glft_quote_regime)
        } else {
            true
        };
        let mut shadow_publish_dwell = Self::glft_shadow_publish_dwell(glft_quote_regime);
        if glft_shadow_mode {
            if let Some(remaining) = self.glft_republish_settle_remaining(now) {
                shadow_publish_dwell = shadow_publish_dwell.max(remaining);
            }
        }
        let shadow_velocity_tps = self.slot_shadow_velocity_tps[slot.index()];
        let mut publish_reason: Option<PolicyPublishCause> = None;
        let mut distance_to_trusted_mid_ticks: Option<f64> = None;
        let mut distance_to_action_target_ticks: Option<f64> = None;
        let mut distance_to_shadow_target_ticks: Option<f64> = None;
        let mut publish_debt: Option<f64> = None;
        let mut publish_hard_safety_exception = false;
        let mut shadow_target_price = normalized_target_price;
        if glft_shadow_mode {
            let shadow_target =
                self.update_slot_shadow_target(slot, normalized_target_price, size, reason);
            shadow_target_price = shadow_target.price;
            let shadow_age = self
                .slot_shadow_since(slot)
                .map(|ts| ts.elapsed())
                .unwrap_or_default();
            if active {
                if let Some((trusted_dist, action_target_dist, _)) =
                    self.slot_relative_misalignment(slot, slot_price, price)
                {
                    distance_to_trusted_mid_ticks = Some(trusted_dist);
                    distance_to_action_target_ticks = Some(action_target_dist);
                }
                if let Some((_, shadow_target_dist, _)) =
                    self.slot_relative_misalignment(slot, slot_price, shadow_target.price)
                {
                    distance_to_shadow_target_ticks = Some(shadow_target_dist);
                }
            } else {
                if let Some((trusted_dist, action_target_dist, _)) = self
                    .slot_relative_misalignment(slot, shadow_target.price, normalized_target_price)
                {
                    distance_to_trusted_mid_ticks = Some(trusted_dist);
                    distance_to_action_target_ticks = Some(action_target_dist);
                }
                distance_to_shadow_target_ticks = Some(0.0);
                if recovery_publish_ready {
                    publish_reason = Some(PolicyPublishCause::Recovery);
                    publish_hard_safety_exception = true;
                } else if shadow_age >= shadow_publish_dwell
                    && self.glft_initial_dwell_book_healthy(slot)
                {
                    publish_reason = Some(PolicyPublishCause::Initial);
                }
            }
        }

        // GLFT soft throttle: avoid repricing every sub-second micro-jitter.
        // Keep fast paths for large drift and crossed-book recovery.
        if self.cfg.strategy.is_glft_mm()
            && active
            && reason == BidReason::Provide
            && slot_direction == Some(slot.direction)
            && (slot_size - size).abs() <= 0.1
            && !force_glft_drift_reprice
            && !recent_cross_reject
        {
            let tick = self.cfg.tick_size.max(1e-9);
            let delta_ticks = ((slot_price - price).abs() / tick).floor();
            let target_gap_ticks = ((slot_price - price).abs() / tick).floor();
            let min_age = std::time::Duration::from_millis(1_500);
            if elapsed < min_age && delta_ticks < 4.0 && target_gap_ticks < 6.0 {
                self.stats.skipped_backoff += 1;
                return;
            }
        }

        if !active {
            if self.cfg.strategy.is_glft_mm() {
                if !self.glft_is_tradeable_now() {
                    return;
                }
            }
            if self.cfg.strategy.is_pair_arb() {
                publish_reason = Some(PolicyPublishCause::Initial);
            }
            if glft_shadow_mode && publish_reason.is_none() {
                self.stats.shadow_suppressed_updates += 1;
                return;
            }
            if glft_shadow_mode {
                if let Some(reason_kind) = publish_reason {
                    let bypass_budget = publish_hard_safety_exception
                        || Self::glft_publish_reason_is_abnormal(reason_kind);
                    let budget_cost =
                        Self::glft_publish_reason_budget_cost(reason_kind, glft_quote_regime);
                    if !bypass_budget
                        && !self.consume_slot_publish_budget(
                            slot,
                            glft_quote_regime,
                            budget_cost,
                            now,
                        )
                    {
                        self.stats.publish_budget_suppressed =
                            self.stats.publish_budget_suppressed.saturating_add(1);
                        self.stats.shadow_suppressed_updates += 1;
                        return;
                    }
                }
            }
            if glft_shadow_mode && matches!(publish_reason, Some(PolicyPublishCause::Policy)) {
                self.consume_slot_publish_debt_release(slot, glft_quote_regime);
            }
            self.record_publish_reason_stats(publish_reason);
            if matches!(publish_reason, Some(PolicyPublishCause::Recovery)) {
                self.note_recovery_publish_for_slot(slot, now);
            }
            let action_price = self.pair_arb_action_price_for_post_only(slot, price, size, reason);
            self.emit_quote_log(
                slot,
                action_price,
                size,
                reason,
                log_msg.as_deref(),
                Some(raw_target_price),
                Some(normalized_target_price),
                publish_reason,
                publish_debt,
                distance_to_trusted_mid_ticks,
                distance_to_action_target_ticks,
                distance_to_shadow_target_ticks,
            );
            self.slot_last_publish_reason[slot.index()] = publish_reason;
            self.place_slot(slot, action_price, size, reason).await;
        } else {
            if self.cfg.strategy.is_glft_mm() {
                if !self.glft_is_tradeable_now() {
                    self.clear_slot_target(slot, CancelReason::StaleData).await;
                    return;
                }
            }
            // PairArb publish policy: widen reprice band while inventory is flat/light,
            // then tighten back to base band as net exposure grows.
            let reprice_band = self.pair_arb_effective_reprice_band(reason);
            let cross_reprice_override = self.cfg.strategy.is_glft_mm()
                && recent_cross_reject
                && (slot_price - price).abs() >= self.cfg.tick_size.max(1e-9) - reprice_eps;
            let price_gap = (slot_price - price).abs();
            let price_gap_triggers_reprice = if self.cfg.strategy.is_pair_arb() {
                price_gap > (reprice_band + reprice_eps).max(0.0)
            } else {
                price_gap >= (reprice_band - reprice_eps).max(0.0)
            };
            let mut needs_reprice = force_glft_drift_reprice
                || cross_reprice_override
                || slot_direction != Some(slot.direction)
                || price_gap_triggers_reprice
                || (slot_size - size).abs() > 0.1;
            if pair_arb_cross_reject_reprice_pending {
                needs_reprice = true;
            }
            let pair_arb_risk_effect = if self.cfg.strategy.is_pair_arb()
                && reason == BidReason::Provide
                && slot.direction == TradeDirection::Buy
            {
                let inv = self.current_working_inventory();
                Some(
                    crate::polymarket::strategy::pair_arb::PairArbStrategy::candidate_risk_effect(
                        &inv, slot.side, size,
                    ),
                )
            } else {
                None
            };
            let mut pair_arb_freshness_reason = "none";
            let pair_arb_force_freshness_republish = if self.cfg.strategy.is_pair_arb()
                && slot.direction == TradeDirection::Buy
                && slot_direction == Some(slot.direction)
                && (slot_size - size).abs() <= 0.1
            {
                let inv = self.current_working_inventory();
                let (force, force_reason, _) = self
                    .pair_arb_should_force_freshness_republish(&inv, slot, slot_price, price, size);
                pair_arb_freshness_reason = force_reason;
                force
            } else {
                false
            };
            if pair_arb_force_freshness_republish {
                needs_reprice = true;
            }
            // PairArb is BUY-only and pair-cost-first:
            // both candidate roles are state/event-driven and hold between
            // discrete triggers (fill/failed/merge/phase/reset).
            if needs_reprice
                && self.cfg.strategy.is_pair_arb()
                && slot_direction == Some(slot.direction)
                && (slot_size - size).abs() <= 0.1
                && slot.direction == TradeDirection::Buy
                && !pair_arb_state_changed
                && !pair_arb_fill_recheck_pending
                && !pair_arb_cross_reject_reprice_pending
            {
                let tick = self.cfg.tick_size.max(1e-9);
                let delta_ticks = (price - slot_price) / tick;
                let (retain, retain_reason) = match pair_arb_risk_effect {
                    Some(
                        crate::polymarket::strategy::pair_arb::PairArbRiskEffect::RiskIncreasing,
                    ) => (true, "same_side_state_driven"),
                    Some(
                        crate::polymarket::strategy::pair_arb::PairArbRiskEffect::PairingOrReducing,
                    ) => (true, "pairing_holds_between_triggers"),
                    None => (true, "same_side_no_chase"),
                };
                if retain && !pair_arb_force_freshness_republish {
                    self.stats.retain_hits = self.stats.retain_hits.saturating_add(1);
                    debug!(
                        "🔒 PairArb retain {:?}: candidate_role={:?} retain_block_reason={} strategic_target={:.4} live={:.4} delta_ticks={:.2}",
                        slot,
                        pair_arb_risk_effect,
                        retain_reason,
                        price,
                        slot_price,
                        delta_ticks,
                    );
                    return;
                }
                debug!(
                    "🔁 PairArb freshness reprice {:?}: candidate_role={:?} reason={} strategic_target={:.4} live={:.4} delta_ticks={:.2}",
                    slot, pair_arb_risk_effect, pair_arb_freshness_reason, price, slot_price, delta_ticks,
                );
            }
            if glft_shadow_mode && publish_reason.is_none() && needs_reprice {
                let shadow_age = self
                    .slot_shadow_since(slot)
                    .map(|ts| ts.elapsed())
                    .unwrap_or_default();
                // In GLFT shadow mode, publish debt is defined as divergence between the
                // last published order and the latest shadow target.
                let price_gap_to_shadow = (slot_price - shadow_target_price).abs();
                let size_gap = (slot_size - size).abs();
                let (side_bid, side_ask) = match slot.side {
                    Side::Yes => (self.book.yes_bid, self.book.yes_ask),
                    Side::No => (self.book.no_bid, self.book.no_ask),
                };
                let has_valid_side_book = side_bid > 0.0 && side_ask > side_bid;
                let inv = self.current_working_inventory();
                let unsafe_depth_ticks = if has_valid_side_book {
                    self.slot_quote_unsafe_depth_ticks(
                        &inv,
                        slot,
                        slot_price,
                        slot_size.max(size),
                        reason,
                        side_bid,
                        side_ask,
                    )
                } else {
                    None
                };
                // Invalid-state fast path is reserved for truly broken quotes only.
                // Force-realign is treated as structural debt so normal publish governance
                // remains single-path and avoids emergency-like churn.
                let invalid_quote_state = unsafe_depth_ticks
                    .map(|depth| !depth.is_finite())
                    .unwrap_or(false);
                let tick = self.cfg.tick_size.max(1e-9);
                let (stale_trusted_ticks, stale_target_ticks) =
                    Self::glft_stale_quote_thresholds(glft_quote_regime);
                let target_debt_ticks = (price_gap_to_shadow / tick).floor();
                let size_debt_ticks = if size_gap > 0.1 { 4.0 } else { 0.0 };
                // Structural debt must stay reference/safety scoped; otherwise
                // shadow-target drift gets double-counted as "structural" churn.
                // Target-follow debt remains a separate axis (currently disabled).
                let reference_debt_ticks = distance_to_trusted_mid_ticks
                    .zip(distance_to_shadow_target_ticks)
                    .map(|(trusted_dist, shadow_dist)| {
                        let trusted_excess = (trusted_dist - stale_trusted_ticks).max(0.0);
                        let shadow_excess = (shadow_dist - stale_target_ticks).max(0.0);
                        if trusted_excess > 0.0 && shadow_excess > 0.0 {
                            trusted_excess.max(shadow_excess)
                        } else {
                            0.0
                        }
                    })
                    .unwrap_or(0.0);
                let safety_debt_ticks = unsafe_depth_ticks
                    .map(|depth| {
                        if depth.is_finite() {
                            depth.max(4.0)
                        } else {
                            99.0
                        }
                    })
                    .unwrap_or(0.0);
                let structural_threshold =
                    Self::glft_publish_structural_debt_threshold(glft_quote_regime);
                let target_follow_debt_ticks = target_debt_ticks.max(size_debt_ticks);
                let structural_debt_ticks = reference_debt_ticks.max(safety_debt_ticks);
                let allow_target_follow_debt =
                    Self::glft_allow_target_follow_debt(glft_quote_regime)
                        && !glft_soft_stale
                        && shadow_velocity_tps < 8.0;
                let effective_target_follow_debt_ticks = if allow_target_follow_debt {
                    target_follow_debt_ticks
                } else {
                    0.0
                };
                let total_publish_debt =
                    effective_target_follow_debt_ticks.max(structural_debt_ticks);
                let target_follow_threshold =
                    Self::glft_publish_target_debt_threshold(glft_quote_regime);
                let debt_release_threshold =
                    Self::glft_publish_debt_release_threshold(glft_quote_regime);
                let debt_dwell = Self::glft_publish_debt_dwell(glft_quote_regime);
                let structural_velocity_cap_tps =
                    Self::glft_structural_publish_velocity_cap_tps(glft_quote_regime);
                let structural_force_publish_threshold =
                    Self::glft_structural_force_publish_threshold(glft_quote_regime);
                if glft_soft_stale {
                    self.consume_slot_publish_debt_release(slot, glft_quote_regime);
                }
                // Structural debt uses published-age cadence, not shadow-age cadence.
                // In trend phases shadow target can keep moving every tick; tying structural
                // recovery to shadow_age starves publish and eventually forces hard fallback.
                let structural_dwell_ready = elapsed >= debt_dwell;
                // For pure target-following debt, require shadow stability so we do not
                // chase every micro-step from the model.
                let target_dwell_ready = shadow_age >= debt_dwell;
                let debt_accum = self.update_slot_publish_debt_accumulator(
                    slot,
                    glft_quote_regime,
                    effective_target_follow_debt_ticks,
                    structural_debt_ticks,
                    now,
                );
                publish_debt = Some(total_publish_debt.max(debt_accum));
                let structural_velocity_ready = shadow_velocity_tps <= structural_velocity_cap_tps;
                let structural_force_ready =
                    structural_debt_ticks >= structural_force_publish_threshold;
                let structural_publish_margin =
                    Self::glft_structural_publish_margin_ticks(glft_quote_regime);
                let structural_publish_threshold = structural_threshold + structural_publish_margin;
                let structural_debt_ready = structural_debt_ticks >= structural_publish_threshold
                    && structural_dwell_ready
                    && (structural_velocity_ready || structural_force_ready);
                let target_follow_debt_ready = effective_target_follow_debt_ticks
                    >= target_follow_threshold
                    && target_dwell_ready;
                // Debt accumulator release is reserved for persistent unresolved debt and
                // should not trigger from purely tiny target-follow noise; thresholds are
                // already regime-raised above baseline.
                let debt_accum_ready = debt_accum >= debt_release_threshold
                    && (structural_dwell_ready || target_dwell_ready);
                let publish_debt_ready =
                    structural_debt_ready || target_follow_debt_ready || debt_accum_ready;
                publish_reason = if recovery_publish_ready {
                    publish_hard_safety_exception = true;
                    Some(PolicyPublishCause::Recovery)
                } else if invalid_quote_state {
                    publish_hard_safety_exception = true;
                    Some(PolicyPublishCause::Safety)
                } else if !glft_soft_stale && regime_stable_for_publish && publish_debt_ready {
                    Some(PolicyPublishCause::Policy)
                } else {
                    None
                };
                if let Some(reason_kind) = publish_reason {
                    let cooldown =
                        Self::glft_publish_reason_cooldown(reason_kind, glft_quote_regime);
                    if cooldown > std::time::Duration::ZERO && elapsed < cooldown {
                        publish_reason = None;
                        publish_hard_safety_exception = false;
                    }
                }
                if let Some(reason_kind) = publish_reason {
                    if Self::glft_publish_should_settle_to_shadow(
                        reason_kind,
                        glft_quote_regime,
                        price_gap_to_shadow,
                        tick,
                        distance_to_shadow_target_ticks,
                    ) {
                        // Debt publishes use capped continuous release:
                        // converge toward shadow target in bounded steps, then cool down.
                        price = if matches!(reason_kind, PolicyPublishCause::Policy) {
                            if Self::glft_debt_fast_settle_ready(
                                glft_quote_regime,
                                total_publish_debt,
                                structural_debt_ticks,
                                shadow_velocity_tps,
                            ) {
                                shadow_target_price
                            } else {
                                self.glft_apply_debt_release_cap(
                                    slot_price,
                                    shadow_target_price,
                                    glft_quote_regime,
                                    total_publish_debt,
                                    structural_debt_ticks,
                                )
                            }
                        } else {
                            // Recovery/invalid-state keeps immediate settle semantics.
                            shadow_target_price
                        };
                        if let Some((trusted_dist, action_target_dist, _)) =
                            self.slot_relative_misalignment(slot, slot_price, price)
                        {
                            distance_to_trusted_mid_ticks = Some(trusted_dist);
                            distance_to_action_target_ticks = Some(action_target_dist);
                        }
                        if let Some((_, shadow_target_dist, _)) =
                            self.slot_relative_misalignment(slot, slot_price, shadow_target_price)
                        {
                            distance_to_shadow_target_ticks = Some(shadow_target_dist);
                        }
                    }
                }
                needs_reprice = force_glft_drift_reprice
                    || cross_reprice_override
                    || slot_direction != Some(slot.direction)
                    || (slot_price - price).abs() >= (reprice_band - reprice_eps).max(0.0)
                    || (slot_size - size).abs() > 0.1;
            }
            if needs_reprice {
                if pair_arb_force_freshness_republish
                    && pair_arb_freshness_reason == "pairing_timed_up_reprice"
                {
                    self.stats.pair_arb_pairing_upward_reprice =
                        self.stats.pair_arb_pairing_upward_reprice.saturating_add(1);
                }
                let action_price =
                    self.pair_arb_action_price_for_post_only(slot, price, size, reason);
                if self.cfg.strategy.is_pair_arb() && publish_reason.is_none() {
                    publish_reason = Some(PolicyPublishCause::Policy);
                }
                if glft_shadow_mode && publish_reason.is_none() {
                    self.stats.shadow_suppressed_updates += 1;
                    return;
                }
                if glft_shadow_mode {
                    if let Some(reason_kind) = publish_reason {
                        let bypass_budget = publish_hard_safety_exception
                            || Self::glft_publish_reason_is_abnormal(reason_kind);
                        let budget_cost =
                            Self::glft_publish_reason_budget_cost(reason_kind, glft_quote_regime);
                        if !bypass_budget
                            && !self.consume_slot_publish_budget(
                                slot,
                                glft_quote_regime,
                                budget_cost,
                                now,
                            )
                        {
                            self.stats.publish_budget_suppressed =
                                self.stats.publish_budget_suppressed.saturating_add(1);
                            self.stats.shadow_suppressed_updates += 1;
                            return;
                        }
                    }
                }
                if glft_shadow_mode && matches!(publish_reason, Some(PolicyPublishCause::Policy)) {
                    self.consume_slot_publish_debt_release(slot, glft_quote_regime);
                }
                self.record_publish_reason_stats(publish_reason);
                if matches!(publish_reason, Some(PolicyPublishCause::Recovery)) {
                    self.note_recovery_publish_for_slot(slot, now);
                }
                self.emit_quote_log(
                    slot,
                    action_price,
                    size,
                    reason,
                    log_msg.as_deref(),
                    Some(raw_target_price),
                    Some(normalized_target_price),
                    publish_reason,
                    publish_debt,
                    distance_to_trusted_mid_ticks,
                    distance_to_action_target_ticks,
                    distance_to_shadow_target_ticks,
                );
                self.slot_last_publish_reason[slot.index()] = publish_reason;
                debug!(
                    "🔄 reprice {:?} {:?} {:.3}→{:.3} sz={:.1} band={:.3} publish_cause={:?}",
                    slot.side,
                    slot.direction,
                    slot_price,
                    action_price,
                    size,
                    reprice_band,
                    publish_reason
                );
                self.place_slot(slot, action_price, size, reason).await;
            }
        }
    }

    pub(super) fn pair_arb_action_price_for_post_only(
        &self,
        slot: OrderSlot,
        strategic_target: f64,
        size: f64,
        reason: BidReason,
    ) -> f64 {
        if !self.cfg.strategy.is_pair_arb()
            || reason != BidReason::Provide
            || slot.direction != TradeDirection::Buy
        {
            return strategic_target;
        }
        let inv = self.current_working_inventory();
        let risk_effect =
            crate::polymarket::strategy::pair_arb::PairArbStrategy::candidate_risk_effect(
                &inv, slot.side, size,
            );
        let best_ask = match slot.side {
            Side::Yes => self.book.yes_ask,
            Side::No => self.book.no_ask,
        };
        if best_ask <= 0.0 {
            return strategic_target;
        }
        let best_bid = match slot.side {
            Side::Yes => self.book.yes_bid,
            Side::No => self.book.no_bid,
        };
        let base_safety_margin = self.post_only_safety_margin_for(slot.side, best_bid, best_ask);
        let tick = self.cfg.tick_size.max(1e-9);
        let extra_ticks = f64::from(self.slot_pair_arb_cross_reject_extra_ticks[slot.index()]);
        let safety_margin = base_safety_margin + extra_ticks * tick;
        let post_only_cap = best_ask - safety_margin;
        let last_rejected_cap = self.slot_pair_arb_last_cross_rejected_action_price[slot.index()]
            .map(|last| (last - tick).max(0.0));
        let action_price = if let Some(reject_cap) = last_rejected_cap {
            strategic_target.min(post_only_cap).min(reject_cap)
        } else {
            strategic_target.min(post_only_cap)
        };
        let reject_repriced = last_rejected_cap
            .map(|_| action_price + 1e-9 < strategic_target.min(post_only_cap))
            .unwrap_or(false);
        if risk_effect
            == crate::polymarket::strategy::pair_arb::PairArbRiskEffect::PairingOrReducing
            && action_price + 1e-9 < strategic_target
        {
            debug!(
                "🧭 pair_arb action clamp | slot={} candidate_role=pairing strategic_target={:.4} pair_arb_retry_action_price={:.4} best_ask={:.4} pair_arb_cross_reject_extra_ticks={:.0} pair_arb_cross_reject_last_price={:.4} pair_arb_cross_reject_repriced={}",
                slot.as_str(),
                strategic_target,
                action_price,
                best_ask,
                extra_ticks,
                self.slot_pair_arb_last_cross_rejected_action_price[slot.index()].unwrap_or(0.0),
                reject_repriced,
            );
        }
        action_price
    }

    fn pair_arb_effective_reprice_band(&self, reason: BidReason) -> f64 {
        let base = self.maker_keep_band(reason);
        if !self.cfg.strategy.is_pair_arb() || reason != BidReason::Provide {
            return base;
        }
        let tick = self.cfg.tick_size.max(1e-9);
        let inv = self.current_working_inventory();
        let abs_net = inv.net_diff.abs();
        let bid_size = self.cfg.bid_size.max(tick);
        let extra_ticks = if abs_net + 1e-9 < bid_size {
            2.0
        } else if abs_net + 1e-9 < 2.0 * bid_size {
            1.0
        } else {
            0.0
        };
        base + extra_ticks * tick
    }

    fn pair_arb_local_unreleased_matched_notional_usdc(&self) -> f64 {
        let snapshot = *self.inv_rx.borrow();
        let settled_spent = (snapshot.settled.yes_qty * snapshot.settled.yes_avg_cost)
            + (snapshot.settled.no_qty * snapshot.settled.no_avg_cost);
        let working_spent = (snapshot.working.yes_qty * snapshot.working.yes_avg_cost)
            + (snapshot.working.no_qty * snapshot.working.no_avg_cost);
        let unreleased = (working_spent - settled_spent).max(0.0);
        if unreleased.is_finite() {
            unreleased
        } else {
            0.0
        }
    }

    pub(super) async fn clear_target(&mut self, side: Side, reason: CancelReason) {
        self.clear_slot_target(OrderSlot::new(side, TradeDirection::Buy), reason)
            .await;
    }

    pub(super) async fn clear_slot_target(&mut self, slot: OrderSlot, reason: CancelReason) {
        let scope = self.default_slot_reset_scope(reason);
        self.clear_slot_target_with_scope(slot, reason, scope).await;
    }

    pub(super) async fn clear_slot_target_with_scope(
        &mut self,
        slot: OrderSlot,
        reason: CancelReason,
        scope: SlotResetScope,
    ) {
        let active = self.slot_target(slot).is_some();
        if !active {
            return;
        }
        let was_oracle_lag_maker = self
            .slot_target(slot)
            .is_some_and(|t| t.reason == BidReason::OracleLagProvide);
        self.note_cancel_reason(reason);

        self.slot_targets[slot.index()] = None;
        self.slot_shadow_targets[slot.index()] = None;
        self.slot_shadow_since[slot.index()] = None;
        self.slot_last_publish_reason[slot.index()] = None;
        self.slot_pair_arb_state_keys[slot.index()] = None;
        self.slot_pair_arb_intent_state_keys[slot.index()] = None;
        self.slot_pair_arb_target_epochs[slot.index()] = None;
        self.slot_pair_arb_intent_epochs[slot.index()] = None;
        self.slot_pair_arb_fill_recheck_pending[slot.index()] = false;
        self.slot_pair_arb_cross_reject_reprice_pending[slot.index()] = false;
        self.slot_pair_arb_state_republish_latched[slot.index()] = false;
        match scope {
            SlotResetScope::Soft => self.soft_reset_slot_publish_state(slot),
            SlotResetScope::Full => self.full_reset_slot_publish_state(slot),
        }
        match slot {
            OrderSlot::YES_BUY => self.yes_target = None,
            OrderSlot::NO_BUY => self.no_target = None,
            _ => {}
        }
        self.sync_buy_side_wrapper(slot);
        if was_oracle_lag_maker {
            self.oracle_lag_maker_state_key = None;
            self.oracle_lag_maker_followup_last_ts = None;
            self.oracle_lag_maker_followup_last_candidate_price[slot.index()] = None;
        }

        debug!("🗑️ Cancel {} ({:?}, {:?})", slot.as_str(), reason, scope);
        self.stats.cancel_events = self.stats.cancel_events.saturating_add(1);
        if self.cfg.dry_run {
            info!(
                "📝 DRY cancel {} ({:?}, {:?})",
                slot.as_str(),
                reason,
                scope
            );
            return;
        }

        let _ = self
            .om_tx
            .send(OrderManagerCmd::ClearTarget { slot, reason })
            .await;
    }

    pub(super) fn soft_release_slot_target(&mut self, slot: OrderSlot) {
        if self.slot_targets[slot.index()].is_none() {
            return;
        }
        let was_oracle_lag_maker = self
            .slot_target(slot)
            .is_some_and(|t| t.reason == BidReason::OracleLagProvide);
        self.slot_targets[slot.index()] = None;
        self.slot_shadow_targets[slot.index()] = None;
        self.slot_shadow_since[slot.index()] = None;
        self.slot_last_publish_reason[slot.index()] = None;
        self.slot_pair_arb_intent_state_keys[slot.index()] = None;
        self.slot_pair_arb_target_epochs[slot.index()] = None;
        self.slot_pair_arb_intent_epochs[slot.index()] = None;
        self.slot_pair_arb_fill_recheck_pending[slot.index()] = true;
        self.slot_pair_arb_cross_reject_reprice_pending[slot.index()] = false;
        self.slot_pair_arb_state_republish_latched[slot.index()] = false;
        self.soft_reset_slot_publish_state(slot);
        match slot {
            OrderSlot::YES_BUY => self.yes_target = None,
            OrderSlot::NO_BUY => self.no_target = None,
            _ => {}
        }
        self.sync_buy_side_wrapper(slot);
        if was_oracle_lag_maker {
            self.oracle_lag_maker_state_key = None;
            self.oracle_lag_maker_followup_last_ts = None;
            self.oracle_lag_maker_followup_last_candidate_price[slot.index()] = None;
        }
        debug!(
            "🧭 Soft release {} after OMS/exchange release",
            slot.as_str()
        );
    }

    pub(super) async fn dispatch_taker_intent(
        &mut self,
        side: Side,
        direction: TradeDirection,
        size: f64,
        purpose: TradePurpose,
        limit_price: Option<f64>,
    ) {
        if self.cfg.strategy.is_oracle_lag_sniping() && self.cfg.oracle_lag_sniping.lab_only {
            info!(
                "🧪 oracle_lag_lab_skip_taker | side={:?} direction={:?} size={:.4} purpose={:?} limit_price={}",
                side,
                direction,
                size,
                purpose,
                limit_price
                    .map(|p| format!("{p:.4}"))
                    .unwrap_or_else(|| "none".to_string())
            );
            return;
        }
        let rounded = (size * 100.0).floor() / 100.0;
        if rounded < 0.01 {
            debug!(
                "🧩 Skip taker {:?} {:?}: size {:.4} below lot floor 0.01",
                direction, side, size
            );
            return;
        }
        if self.cfg.strategy.is_oracle_lag_sniping()
            && direction == TradeDirection::Buy
            && self.post_close_winner_side == Some(side)
        {
            let (winner_bid, winner_ask, winner_ask_tradable, winner_spread_ticks, quality_source) =
                self.oracle_lag_winner_book_quality(
                    side,
                    self.post_close_hint_winner_bid,
                    self.post_close_hint_winner_ask_raw,
                    self.post_close_hint_book_source,
                    self.post_close_hint_distance_to_final_ms,
                );
            // Submit-time revalidation must share the same quality semantics as hint fire.
            // Otherwise we may fire on hint quality and immediately fail submit on
            // a stricter live-only rule (fire/submit semantic mismatch).
            let submit_quality_eligible =
                quality_source != "no_book" && quality_source != "hint_stale_ignored";
            if winner_bid > ORACLE_LAG_NO_TAKER_ABOVE_PRICE {
                warn!(
                    "🚫 oracle_lag_sniping taker blocked | side={:?} winner_bid={:.4} winner_ask={:.4} winner_ask_tradable={} winner_spread_ticks={:.2} quality_source={} threshold={:.4} purpose={:?}",
                    side,
                    winner_bid,
                    winner_ask,
                    winner_ask_tradable,
                    winner_spread_ticks,
                    quality_source,
                    ORACLE_LAG_NO_TAKER_ABOVE_PRICE,
                    purpose
                );
                return;
            }
            if !winner_ask_tradable
                || !submit_quality_eligible
                || winner_ask <= 0.0
                || winner_ask > ORACLE_LAG_NO_TAKER_ABOVE_PRICE + 1e-9
            {
                info!(
                    "⏭️ oracle_lag_taker_submit_skip | side={:?} reason=submit_revalidate_failed winner_bid={:.4} winner_ask={:.4} winner_ask_tradable={} winner_spread_ticks={:.2} quality_source={} submit_quality_eligible={} threshold={:.4} purpose={:?}",
                    side,
                    winner_bid,
                    winner_ask,
                    winner_ask_tradable,
                    winner_spread_ticks,
                    quality_source,
                    submit_quality_eligible,
                    ORACLE_LAG_NO_TAKER_ABOVE_PRICE,
                    purpose
                );
                return;
            }
            if let Some(limit) = limit_price {
                if limit + 1e-9 < winner_ask {
                    info!(
                        "⏭️ oracle_lag_taker_submit_skip | side={:?} reason=limit_below_live_ask winner_bid={:.4} winner_ask={:.4} limit={:.4} quality_source={} purpose={:?}",
                        side,
                        winner_bid,
                        winner_ask,
                        limit,
                        quality_source,
                        purpose
                    );
                    return;
                }
            }
        }

        // Hot-path optimization:
        // For OracleLag one-shot taker, let OMS perform its own slot cleanup in one pass.
        // Avoid synchronous pre-clear awaits here, which add avoidable tail latency.
        if !matches!(purpose, TradePurpose::OracleLagSnipe) {
            for slot in OrderSlot::side_slots(side) {
                self.clear_slot_target(slot, CancelReason::Reprice).await;
            }
        }
        if self.cfg.dry_run {
            let order_type = if limit_price.is_some() {
                "FAK"
            } else {
                "TakerMarket"
            };
            let limit_price_s = limit_price
                .map(|p| format!("{p:.4}"))
                .unwrap_or_else(|| "none".to_string());
            let notional_usdc_s = limit_price
                .map(|p| format!("{:.4}", p * rounded))
                .unwrap_or_else(|| "none".to_string());
            info!(
                "🧪 dry_taker_preview | strategy={:?} side={:?} direction={:?} order_type={} purpose={:?} limit_price={} size={:.2} notional_usdc={}",
                self.cfg.strategy,
                side,
                direction,
                order_type,
                purpose,
                limit_price_s,
                rounded,
                notional_usdc_s,
            );
            return;
        }
        let _ = self
            .om_tx
            .send(OrderManagerCmd::OneShotTakerHedge {
                side,
                direction,
                size: rounded,
                purpose,
                limit_price,
            })
            .await;
    }

    /// Endgame de-risk: prefer selling dominant inventory; fallback to opposite BUY only for shortfall.
    pub(super) async fn dispatch_taker_derisk(
        &mut self,
        dominant_side: Side,
        size: f64,
        inv: &InventoryState,
    ) {
        if size <= f64::EPSILON {
            return;
        }
        let dominant_qty = match dominant_side {
            Side::Yes => inv.yes_qty.max(0.0),
            Side::No => inv.no_qty.max(0.0),
        };
        let sell_size = dominant_qty.min(size).max(0.0);
        if sell_size >= 0.01 {
            self.dispatch_taker_intent(
                dominant_side,
                TradeDirection::Sell,
                sell_size,
                TradePurpose::Exit,
                None,
            )
            .await;
        }

        let shortfall = (size - sell_size).max(0.0);
        if shortfall >= 0.01 {
            let buy_side = match dominant_side {
                Side::Yes => Side::No,
                Side::No => Side::Yes,
            };
            warn!(
                "⚠️ Endgame de-risk fallback: dominant={:?} sell_size={:.2} shortfall={:.2} -> buy {:?}",
                dominant_side, sell_size, shortfall, buy_side
            );
            self.dispatch_taker_intent(
                buy_side,
                TradeDirection::Buy,
                shortfall,
                TradePurpose::Exit,
                None,
            )
            .await;
        }
    }

    pub(super) async fn handle_market_data(&mut self, msg: MarketDataMsg) {
        match msg {
            MarketDataMsg::BookTick {
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                ..
            } => {
                self.update_book(yes_bid, yes_ask, no_bid, no_ask);
                self.stats.ticks += 1;
            }
            MarketDataMsg::TradeTick { .. } => {
                // Trades are primarily for OFI actor; Coordinator mostly skips
                // but we could track last trade prices here if needed.
            }
            MarketDataMsg::OracleLagSelection {
                round_end_ts,
                selected,
                rank,
                reason,
            } => {
                let current = self.oracle_lag_selected_round_end_ts.unwrap_or(0);
                if self.oracle_lag_selected_round_end_ts.is_none() || round_end_ts > current {
                    self.clear_oracle_lag_live_maker_orders("selection_new_round")
                        .await;
                    self.oracle_lag_selected_round_end_ts = Some(round_end_ts);
                    // Oracle-lag now executes per-market directly; no arbiter gating.
                    self.oracle_lag_is_selected = true;
                    self.oracle_lag_defer_to_round_tail = false;
                    self.oracle_lag_fak_last_dispatch = None;
                    self.oracle_lag_fak_shots_this_round = 0;
                    self.oracle_lag_tail_round_done = None;
                    self.oracle_lag_fak_inflight_by_slug.clear();
                    self.oracle_lag_maker_followup_last_ts = None;
                    self.oracle_lag_maker_followup_last_candidate_price =
                        std::array::from_fn(|_| None);
                    self.oracle_lag_round_halted = false;
                    self.oracle_lag_round_halt_kind = None;
                    self.reset_oracle_lag_hint_book_cache();
                    info!(
                        "🏆 oracle_lag_arbiter_selection | round_end_ts={} selected={} rank={} reason={}",
                        round_end_ts, selected, rank, reason
                    );
                } else if round_end_ts == current {
                    debug!(
                        "⏭️ oracle_lag_arbiter_selection_duplicate | round_end_ts={} selected={} rank={} reason={} — ignored",
                        round_end_ts, selected, rank, reason
                    );
                } else {
                    debug!(
                        "⏭️ oracle_lag_arbiter_selection_stale | round_end_ts={} current_round={:?} — ignored",
                        round_end_ts, self.oracle_lag_selected_round_end_ts
                    );
                }
            }
            MarketDataMsg::OracleLagTailAction {
                round_end_ts,
                side,
                mode,
                limit_price,
                target_slug,
                reason,
            } => {
                info!(
                    "⏭️ oracle_lag_tail_action_ignored | round_end_ts={} side={:?} mode={:?} target_slug={} reason={} limit_price={:.4}",
                    round_end_ts, side, mode, target_slug, reason, limit_price
                );
            }
            MarketDataMsg::WinnerHint {
                slug,
                hint_id,
                side,
                source,
                ref_price,
                observed_price,
                final_detect_unix_ms,
                emit_unix_ms,
                winner_bid: hint_winner_bid,
                winner_ask_raw: hint_winner_ask_raw,
                winner_evidence_recv_ms,
                winner_book_source,
                winner_distance_to_final_ms,
                open_is_exact,
                ts,
            } => {
                let is_new_hint_round =
                    self.post_close_winner_final_detect_unix_ms != Some(final_detect_unix_ms);
                let changed = self.post_close_winner_side != Some(side)
                    || self.post_close_winner_source != Some(source)
                    || self.post_close_winner_open_is_exact != Some(open_is_exact)
                    || is_new_hint_round;
                self.post_close_winner_side = Some(side);
                self.post_close_winner_source = Some(source);
                self.post_close_winner_open_is_exact = Some(open_is_exact);
                self.post_close_winner_ref_price = ref_price;
                self.post_close_winner_observed_price = observed_price;
                self.post_close_winner_final_detect_unix_ms = Some(final_detect_unix_ms);
                self.post_close_winner_emit_unix_ms = Some(emit_unix_ms);
                self.post_close_winner_evidence_recv_ms = Some(winner_evidence_recv_ms);
                self.post_close_winner_ts = Some(ts);
                self.post_close_hint_winner_bid = hint_winner_bid.max(0.0);
                self.post_close_hint_winner_ask_raw = hint_winner_ask_raw.max(0.0);
                self.post_close_hint_book_source = winner_book_source;
                self.post_close_hint_distance_to_final_ms = winner_distance_to_final_ms;
                if changed {
                    info!(
                        "🏁 post_close winner hint | slug={} hint_id={} side={:?} source={:?} open_exact={} observed={:.4} ref={:.4} final_detect_unix_ms={} emit_unix_ms={} evidence_recv_ms={} evidence_to_final_ms={} new_hint_round={}",
                        slug,
                        hint_id,
                        side,
                        source,
                        open_is_exact,
                        observed_price,
                        ref_price,
                        final_detect_unix_ms,
                        emit_unix_ms,
                        winner_evidence_recv_ms,
                        winner_distance_to_final_ms,
                        is_new_hint_round,
                    );
                }
                if self.cfg.strategy.is_oracle_lag_sniping()
                    && self.cfg.oracle_lag_sniping.market_enabled
                {
                    // Winner hint is the immediate hot path for the first taker attempt.
                    // Round-tail action remains as cross-market fallback only.
                    self.oracle_lag_defer_to_round_tail = false;
                    if is_new_hint_round {
                        self.clear_oracle_lag_live_maker_orders("winner_hint_new_round")
                            .await;
                        self.oracle_lag_fak_last_dispatch = None;
                        self.oracle_lag_fak_shots_this_round = 0;
                        self.oracle_lag_fak_inflight_by_slug.remove(&slug);
                        self.oracle_lag_maker_followup_last_ts = None;
                        self.oracle_lag_maker_followup_last_candidate_price =
                            std::array::from_fn(|_| None);
                        self.oracle_lag_round_halted = false;
                        self.oracle_lag_round_halt_kind = None;
                    }
                    if self.oracle_lag_round_halted {
                        info!(
                            "⏭️ oracle_lag_winner_hint_skip | reason=round_halted side={:?} source={:?} halt_kind={:?}",
                            side, source, self.oracle_lag_round_halt_kind
                        );
                        return;
                    }
                    if self
                        .oracle_lag_fak_inflight_by_slug
                        .get(&slug)
                        .is_some_and(|v| *v == hint_id)
                    {
                        info!(
                            "⏭️ oracle_lag_winner_hint_skip | reason=inflight_same_hint slug={} hint_id={} side={:?} source={:?}",
                            slug, hint_id, side, source
                        );
                        return;
                    }
                    if self
                        .oracle_lag_fak_inflight_by_slug
                        .get(&slug)
                        .is_some_and(|v| *v != hint_id)
                    {
                        self.oracle_lag_fak_inflight_by_slug.remove(&slug);
                    }
                    info!(
                        "🧭 oracle_lag_order_mode | mode=winner_hint_immediate slug={} hint_id={} side={:?} source={:?} hint_book_source={} hint_distance_to_final_ms={}",
                        slug, hint_id, side, source, winner_book_source, winner_distance_to_final_ms
                    );
                    let (
                        winner_bid,
                        winner_ask,
                        winner_ask_tradable,
                        winner_spread_ticks,
                        winner_book_quality_source,
                    ) = self.oracle_lag_winner_book_quality(
                        side,
                        hint_winner_bid,
                        hint_winner_ask_raw,
                        winner_book_source,
                        winner_distance_to_final_ms,
                    );
                    if winner_ask_tradable
                        && winner_ask > 0.0
                        && winner_ask <= ORACLE_LAG_NO_TAKER_ABOVE_PRICE + 1e-9
                    {
                        let size =
                            self.oracle_lag_effective_order_size(ORACLE_LAG_NO_TAKER_ABOVE_PRICE);
                        if size >= 0.01 {
                            info!(
                                "⚡ oracle_lag_winner_hint_fire | slug={} hint_id={} side={:?} winner_bid={:.4} winner_ask={:.4} winner_spread_ticks={:.2} quality_source={} limit_price={:.4} size={:.2}",
                                slug,
                                hint_id,
                                side,
                                winner_bid,
                                winner_ask,
                                winner_spread_ticks,
                                winner_book_quality_source,
                                ORACLE_LAG_NO_TAKER_ABOVE_PRICE,
                                size,
                            );
                            self.dispatch_taker_intent(
                                side,
                                TradeDirection::Buy,
                                size,
                                TradePurpose::OracleLagSnipe,
                                Some(ORACLE_LAG_NO_TAKER_ABOVE_PRICE),
                            )
                            .await;
                            self.oracle_lag_fak_last_dispatch = Some(Instant::now());
                            self.oracle_lag_fak_shots_this_round =
                                self.oracle_lag_fak_shots_this_round.saturating_add(1);
                            self.oracle_lag_fak_inflight_by_slug
                                .insert(slug.clone(), hint_id);
                        } else {
                            info!(
                                "⏭️ oracle_lag_winner_hint_skip | slug={} hint_id={} side={:?} reason=size_below_lot_floor winner_bid={:.4} winner_ask={:.4} quality_source={} size={:.4}",
                                slug,
                                hint_id,
                                side,
                                winner_bid,
                                winner_ask,
                                winner_book_quality_source,
                                size,
                            );
                        }
                    } else {
                        info!(
                            "⏭️ oracle_lag_winner_hint_skip | slug={} hint_id={} side={:?} reason=missing_or_untradable_winner_ask winner_bid={:.4} winner_ask={:.4} winner_ask_tradable={} winner_spread_ticks={:.2} quality_source={} threshold={:.4}",
                            slug,
                            hint_id,
                            side,
                            winner_bid,
                            winner_ask,
                            winner_ask_tradable,
                            winner_spread_ticks,
                            winner_book_quality_source,
                            ORACLE_LAG_NO_TAKER_ABOVE_PRICE,
                        );
                    }
                }
            }
        }
    }

    fn oracle_lag_winner_book_quality(
        &self,
        side: Side,
        hint_winner_bid: f64,
        hint_winner_ask_raw: f64,
        hint_book_source: &'static str,
        hint_distance_to_final_ms: u64,
    ) -> (f64, f64, bool, f64, &'static str) {
        let hint_bid = hint_winner_bid.max(0.0);
        let hint_ask = hint_winner_ask_raw.max(0.0);
        let (book_bid, book_ask) = match side {
            Side::Yes => (self.book.yes_bid, self.book.yes_ask),
            Side::No => (self.book.no_bid, self.book.no_ask),
        };
        let live_has_winner_side = book_bid > 0.0 || book_ask > 0.0;
        let hint_is_fresh = hint_distance_to_final_ms
            <= crate::polymarket::coordinator::ORACLE_LAG_HINT_FALLBACK_MAX_AGE_MS;
        let hint_has_price = hint_bid > 0.0 || hint_ask > 0.0;
        let (winner_bid, winner_ask, source) = if hint_has_price && hint_is_fresh && hint_ask > 0.0
        {
            // Keep hint/execute source consistent for winner-side ask:
            // when fresh hint has ask, prefer it over partially-missing live side book.
            let source = match hint_book_source {
                "ws_partial" => "hint_ws_partial_fresh_preferred",
                "clob_rest" => "hint_clob_rest_fresh_preferred",
                _ => "hint_fresh_preferred",
            };
            let winner_bid = if hint_bid > 0.0 { hint_bid } else { book_bid };
            (winner_bid, hint_ask, source)
        } else if live_has_winner_side {
            (book_bid, book_ask, "live_book")
        } else if hint_has_price && hint_is_fresh {
            let source = match hint_book_source {
                "ws_partial" => "hint_ws_partial_fresh",
                "clob_rest" => "hint_clob_rest_fresh",
                _ => "hint_fresh",
            };
            (hint_bid, hint_ask, source)
        } else if hint_has_price {
            (0.0, 0.0, "hint_stale_ignored")
        } else {
            (0.0, 0.0, "no_book")
        };
        let tick = if winner_bid
            > crate::polymarket::coordinator::ORACLE_LAG_MICRO_TICK_BID_BOUNDARY
            || winner_ask > 0.96
            || (winner_bid > 0.0 && winner_bid < 0.04)
            || (winner_ask > 0.0 && winner_ask < 0.04)
        {
            0.001
        } else {
            self.cfg.tick_size.max(1e-9)
        };
        let winner_spread_ticks = if winner_bid > 0.0 && winner_ask > 0.0 {
            (winner_ask - winner_bid) / tick
        } else {
            0.0
        };
        let winner_ask_tradable =
            winner_ask > 0.0 && (winner_bid <= 0.0 || winner_ask > winner_bid + 0.5 * tick + 1e-9);
        (
            winner_bid,
            winner_ask,
            winner_ask_tradable,
            winner_spread_ticks,
            source,
        )
    }

    pub(super) async fn place_slot(
        &mut self,
        slot: OrderSlot,
        price: f64,
        size: f64,
        reason: BidReason,
    ) {
        if price <= 0.0 {
            let cancel_reason = match reason {
                BidReason::Hedge => CancelReason::Reprice,
                BidReason::Provide | BidReason::OracleLagProvide => CancelReason::InventoryLimit,
            };
            self.clear_slot_target(slot, cancel_reason).await;
            return;
        }

        let target = DesiredTarget {
            side: slot.side,
            direction: slot.direction,
            price,
            size,
            reason,
        };

        let replacing = self.slot_target(slot).is_some();
        self.slot_targets[slot.index()] = Some(target.clone());
        self.slot_last_ts[slot.index()] = Instant::now();
        if self.cfg.strategy.is_pair_arb() && slot.direction == TradeDirection::Buy {
            let inv = self.current_working_inventory();
            let phase = self.endgame_phase();
            self.slot_pair_arb_state_keys[slot.index()] =
                Some(self.pair_arb_state_key(&inv, phase));
            self.slot_pair_arb_target_epochs[slot.index()] = Some(self.pair_arb_decision_epoch);
            self.slot_pair_arb_fill_recheck_pending[slot.index()] = false;
        } else {
            self.slot_pair_arb_target_epochs[slot.index()] = None;
        }
        self.sync_buy_side_wrapper(slot);

        self.stats.placed += 1;
        if replacing {
            self.stats.replace_events = self.stats.replace_events.saturating_add(1);
        } else {
            self.stats.publish_events = self.stats.publish_events.saturating_add(1);
        }
        self.log_oracle_lag_submit_latency(slot, price, size, reason, replacing);

        if self.cfg.dry_run {
            let notional_usdc = price * size;
            info!(
                "🧪 dry_order_preview | strategy={:?} slot={} side={:?} direction={:?} order_type=Limit reason={:?} price={:.4} size={:.2} notional_usdc={:.4} replacing={}",
                self.cfg.strategy,
                slot.as_str(),
                slot.side,
                slot.direction,
                reason,
                price,
                size,
                notional_usdc,
                replacing,
            );
            return;
        }

        if self.cfg.strategy.is_pair_arb()
            && reason == BidReason::Provide
            && slot.direction == TradeDirection::Buy
        {
            let local_unreleased_matched_notional_usdc =
                self.pair_arb_local_unreleased_matched_notional_usdc();
            if local_unreleased_matched_notional_usdc > 1e-9 {
                let _ = self
                    .om_tx
                    .send(OrderManagerCmd::SetPairArbHeadroom {
                        slot,
                        local_unreleased_matched_notional_usdc,
                    })
                    .await;
            }
        }

        let _ = self.om_tx.send(OrderManagerCmd::SetTarget(target)).await;
    }

    fn log_oracle_lag_submit_latency(
        &mut self,
        slot: OrderSlot,
        price: f64,
        size: f64,
        reason: BidReason,
        replacing: bool,
    ) {
        if !self.cfg.strategy.is_oracle_lag_sniping()
            || reason != BidReason::Provide
            || slot.direction != TradeDirection::Buy
        {
            return;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let market_end_ts = self.cfg.market_end_ts;
        let market_end_ms = market_end_ts.map(|ts| ts.saturating_mul(1_000));
        let delta_from_end_ms = market_end_ms.map(|end_ms| now_ms.saturating_sub(end_ms));
        let winner_to_submit_ms = self
            .post_close_winner_ts
            .map(|ts| ts.elapsed().as_millis() as u64);
        let final_detect_to_submit_ms = self
            .post_close_winner_final_detect_unix_ms
            .map(|ms| now_ms.saturating_sub(ms));
        let emit_to_submit_ms = self
            .post_close_winner_emit_unix_ms
            .map(|ms| now_ms.saturating_sub(ms));
        let evidence_to_submit_ms = self.post_close_winner_evidence_recv_ms.and_then(|ms| {
            if ms > 0 {
                Some(now_ms.saturating_sub(ms))
            } else {
                None
            }
        });
        let notional_usdc = price * size;
        let first_submit = !self.oracle_lag_first_submit_logged;
        if first_submit {
            self.oracle_lag_first_submit_logged = true;
        }
        info!(
            "⏱️ oracle_lag_submit_latency | first_submit={} unix_ms={} market_end_ts={:?} delta_from_end_ms={:?} winner_to_submit_ms={:?} final_detect_to_submit_ms={:?} emit_to_submit_ms={:?} evidence_to_submit_ms={:?} winner_side={:?} winner_source={:?} slot={} side={:?} direction={:?} type=Limit replacing={} price={:.4} size={:.2} notional_usdc={:.4}",
            first_submit,
            now_ms,
            market_end_ts,
            delta_from_end_ms,
            winner_to_submit_ms,
            final_detect_to_submit_ms,
            emit_to_submit_ms,
            evidence_to_submit_ms,
            self.post_close_winner_side,
            self.post_close_winner_source,
            slot.as_str(),
            slot.side,
            slot.direction,
            replacing,
            price,
            size,
            notional_usdc,
        );
    }

    async fn slot_place_or_reprice_glft_policy(
        &mut self,
        slot: OrderSlot,
        raw_target_price: f64,
        normalized_target_price: f64,
        size: f64,
        reason: BidReason,
        log_msg: Option<&str>,
        recovery_publish_ready: bool,
        now: Instant,
    ) {
        let glft = *self.glft_rx.borrow();
        if slot.direction == TradeDirection::Buy
            && matches!(glft.drift_mode, crate::polymarket::glft::DriftMode::Frozen)
        {
            // During Frozen drift, suppress all BUY slot publishes to avoid
            // chasing the dominant side while reference alignment is degraded.
            self.stats.policy_noop_ticks = self.stats.policy_noop_ticks.saturating_add(1);
            debug!(
                "🧊 GLFT frozen BUY suppressed for {:?} (regime={:?}, drift_raw={:.1}, drift_ewma={:.1})",
                slot, glft.quote_regime, glft.basis_drift_ticks, glft.drift_ewma_ticks
            );
            return;
        }
        let quote_regime = Some(glft.quote_regime);
        let active = self.slot_target(slot).is_some();
        let slot_price = self.slot_target(slot).map(|t| t.price).unwrap_or(0.0);
        let slot_size = self.slot_target(slot).map(|t| t.size).unwrap_or(0.0);
        let slot_direction = self.slot_target(slot).map(|t| t.direction);
        let tick = self.cfg.tick_size.max(1e-9);
        let prev_committed_policy = self.slot_policy_states[slot.index()];
        let had_committed_policy = self.slot_policy_states[slot.index()].is_some();

        let regime_age = self
            .update_slot_regime_state(slot, quote_regime, now)
            .unwrap_or_default();
        let regime_stable_for_publish =
            regime_age >= Self::glft_regime_publish_settle_dwell(quote_regime);
        let shadow_target =
            self.update_slot_shadow_target(slot, normalized_target_price, size, reason);
        let (policy, transition) = self.update_slot_quote_policy(
            slot,
            normalized_target_price,
            size,
            glft,
            now,
            regime_stable_for_publish,
        );
        let policy_age = self.slot_policy_since[slot.index()]
            .map(|since| now.saturating_duration_since(since))
            .unwrap_or_default();
        let policy_dwell = Self::glft_policy_publish_dwell(quote_regime);

        let (best_bid, best_ask) = match slot.side {
            Side::Yes => (self.book.yes_bid, self.book.yes_ask),
            Side::No => (self.book.no_bid, self.book.no_ask),
        };
        let has_valid_side_book = best_bid > 0.0 && best_ask > best_bid;
        let unsafe_depth_ticks = if active && has_valid_side_book {
            self.slot_quote_unsafe_depth_ticks(
                &self.current_working_inventory(),
                slot,
                slot_price,
                slot_size.max(size),
                reason,
                best_bid,
                best_ask,
            )
        } else {
            None
        };
        let hard_unsafe_state = unsafe_depth_ticks
            .map(|depth| !depth.is_finite())
            .unwrap_or(false);
        let soft_unsafe_depth_ticks = unsafe_depth_ticks
            .filter(|depth| depth.is_finite())
            .unwrap_or(0.0);
        let (distance_to_trusted_mid_ticks, distance_to_policy_ticks, force_realign) = if active {
            self.slot_relative_misalignment(slot, slot_price, policy.action_price)
                .unwrap_or((0.0, 0.0, false))
        } else {
            (0.0, 0.0, false)
        };
        let distance_to_shadow_target_ticks = if active {
            self.slot_relative_misalignment(slot, slot_price, shadow_target.price)
                .map(|(_, shadow_dist, _)| shadow_dist)
                .unwrap_or(0.0)
        } else {
            0.0
        };
        let policy_aligned = active
            && slot_direction == Some(slot.direction)
            && (slot_price - policy.action_price).abs() <= tick * 0.5
            && (slot_size - policy.size).abs() <= 0.1;
        let severe_force_realign = if force_realign {
            let (trusted_force_threshold, target_force_threshold) =
                Self::glft_force_realign_thresholds(glft.quote_regime);
            distance_to_trusted_mid_ticks >= trusted_force_threshold + 6.0
                && distance_to_policy_ticks >= target_force_threshold + 6.0
        } else {
            false
        };
        let soft_unsafe_trigger = active
            && !hard_unsafe_state
            && severe_force_realign
            && policy_age >= policy_dwell
            && regime_stable_for_publish
            && soft_unsafe_depth_ticks
                >= Self::glft_policy_soft_unsafe_publish_threshold(quote_regime);
        let publish_cause = if recovery_publish_ready {
            Some(PolicyPublishCause::Recovery)
        } else if hard_unsafe_state || soft_unsafe_trigger {
            Some(PolicyPublishCause::Safety)
        } else if !active {
            if policy_age >= policy_dwell && self.glft_initial_dwell_book_healthy(slot) {
                Some(if had_committed_policy {
                    PolicyPublishCause::Policy
                } else {
                    PolicyPublishCause::Initial
                })
            } else {
                None
            }
        } else if !policy_aligned
            && policy_age >= policy_dwell
            && regime_stable_for_publish
            && transition.is_some()
        {
            Some(PolicyPublishCause::Policy)
        } else {
            None
        };

        if publish_cause.is_none() {
            if transition.is_some() {
                self.stats.shadow_suppressed_updates =
                    self.stats.shadow_suppressed_updates.saturating_add(1);
            } else {
                self.stats.policy_noop_ticks = self.stats.policy_noop_ticks.saturating_add(1);
            }
            return;
        }

        let publish_cause = publish_cause.unwrap();
        if active && matches!(publish_cause, PolicyPublishCause::Policy) {
            let since_last_publish = now.saturating_duration_since(self.slot_last_ts(slot));
            let mut required_republish_interval =
                Self::glft_policy_min_republish_interval(quote_regime);
            if let (Some(PolicyTransitionReason::PriceBucket), Some(prev_policy)) =
                (transition, prev_committed_policy)
            {
                if Self::glft_policy_is_minor_pricebucket_transition(
                    prev_policy,
                    policy,
                    glft.quote_regime,
                    tick,
                ) {
                    required_republish_interval = required_republish_interval.max(
                        Self::glft_policy_minor_pricebucket_publish_hold(quote_regime),
                    );
                }
            }
            if since_last_publish < required_republish_interval {
                self.stats.shadow_suppressed_updates =
                    self.stats.shadow_suppressed_updates.saturating_add(1);
                return;
            }
        }
        let bypass_budget = Self::glft_publish_reason_is_abnormal(publish_cause);
        let budget_cost = Self::glft_publish_reason_budget_cost(publish_cause, quote_regime);
        if !bypass_budget && !self.consume_slot_publish_budget(slot, quote_regime, budget_cost, now)
        {
            self.stats.publish_budget_suppressed =
                self.stats.publish_budget_suppressed.saturating_add(1);
            self.stats.shadow_suppressed_updates =
                self.stats.shadow_suppressed_updates.saturating_add(1);
            return;
        }

        if force_realign {
            self.stats.forced_realign_count = self.stats.forced_realign_count.saturating_add(1);
            if matches!(publish_cause, PolicyPublishCause::Safety) {
                self.stats.forced_realign_hard_count =
                    self.stats.forced_realign_hard_count.saturating_add(1);
            }
        }

        let publish_price = policy.action_price;
        let publish_size = policy.size;
        let publish_debt_ticks = if active {
            distance_to_policy_ticks.max(distance_to_shadow_target_ticks)
        } else {
            0.0
        };
        if matches!(publish_cause, PolicyPublishCause::Recovery) {
            self.note_recovery_publish_for_slot(slot, now);
        }
        self.record_publish_reason_stats(Some(publish_cause));
        self.emit_quote_log(
            slot,
            publish_price,
            publish_size,
            reason,
            log_msg,
            Some(raw_target_price),
            Some(normalized_target_price),
            Some(publish_cause),
            Some(publish_debt_ticks),
            Some(distance_to_trusted_mid_ticks),
            Some(distance_to_policy_ticks),
            Some(distance_to_shadow_target_ticks),
        );
        self.slot_last_publish_reason[slot.index()] = Some(publish_cause);
        self.place_slot(slot, publish_price, publish_size, reason)
            .await;
    }

    fn glft_policy_price_bucket_ticks(quote_regime: crate::polymarket::glft::QuoteRegime) -> f64 {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => 14.0,
            crate::polymarket::glft::QuoteRegime::Tracking => 16.0,
            crate::polymarket::glft::QuoteRegime::Guarded => 18.0,
            crate::polymarket::glft::QuoteRegime::Blocked => 99.0,
        }
    }

    fn glft_policy_publish_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(6_400)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(8_200)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(10_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(9_999)
            }
            None => std::time::Duration::from_millis(6_400),
        }
    }

    fn glft_policy_commit_dwell(
        quote_regime: crate::polymarket::glft::QuoteRegime,
    ) -> std::time::Duration {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => {
                std::time::Duration::from_millis(5_200)
            }
            crate::polymarket::glft::QuoteRegime::Tracking => {
                std::time::Duration::from_millis(6_400)
            }
            crate::polymarket::glft::QuoteRegime::Guarded => {
                std::time::Duration::from_millis(7_600)
            }
            crate::polymarket::glft::QuoteRegime::Blocked => {
                std::time::Duration::from_millis(9_999)
            }
        }
    }

    fn glft_policy_min_republish_interval(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(11_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(13_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(15_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(9_999)
            }
            None => std::time::Duration::from_millis(11_000),
        }
    }

    fn glft_policy_soft_unsafe_publish_threshold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 10.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 12.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 14.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 10.0,
        }
    }

    fn glft_quantize_policy_price(&self, slot: OrderSlot, price: f64, bucket_ticks: f64) -> f64 {
        let tick = self.cfg.tick_size.max(1e-9);
        let bucket = (bucket_ticks.max(1.0).round() * tick).max(tick);
        let safe = self.safe_price(price);
        let snapped = match slot.direction {
            TradeDirection::Buy => (safe / bucket).floor() * bucket,
            TradeDirection::Sell => (safe / bucket).ceil() * bucket,
        };
        let bounded = snapped.clamp(tick, 1.0 - tick);
        match slot.direction {
            TradeDirection::Buy => self.quantize_down_to_tick(bounded),
            TradeDirection::Sell => self.quantize_up_to_tick(bounded),
        }
    }

    pub(super) fn build_slot_quote_policy(
        &self,
        slot: OrderSlot,
        normalized_target_price: f64,
        size: f64,
        glft: crate::polymarket::glft::GlftSignalSnapshot,
    ) -> QuotePolicyState {
        let price = self.glft_quantize_policy_price(
            slot,
            normalized_target_price,
            Self::glft_policy_price_bucket_ticks(glft.quote_regime),
        );
        let action_price = match slot.direction {
            TradeDirection::Buy => {
                self.quantize_down_to_tick(self.safe_price(normalized_target_price))
            }
            TradeDirection::Sell => {
                self.quantize_up_to_tick(self.safe_price(normalized_target_price))
            }
        };
        let tick = self.cfg.tick_size.max(1e-9);
        let bucket_ticks = Self::glft_policy_price_bucket_ticks(glft.quote_regime)
            .max(1.0)
            .round() as i64;
        let bucket_width = (bucket_ticks as f64 * tick).max(tick);
        QuotePolicyState {
            active: true,
            policy_price: price,
            action_price,
            size,
            price_band_tick: (price / bucket_width).round() as i64,
            size_bucket: (size * 10.0).round() as i32,
            side_mode: QuotePolicySideMode::NormalBuy,
        }
    }

    fn classify_policy_transition(
        prev: Option<QuotePolicyState>,
        next: QuotePolicyState,
        quote_regime: crate::polymarket::glft::QuoteRegime,
        tick: f64,
    ) -> Option<PolicyTransitionReason> {
        match prev {
            None => Some(PolicyTransitionReason::Activate),
            Some(prev) if prev.active != next.active => {
                if next.active {
                    Some(PolicyTransitionReason::Activate)
                } else {
                    Some(PolicyTransitionReason::Suppress)
                }
            }
            Some(prev) if prev.side_mode != next.side_mode => {
                Some(PolicyTransitionReason::Suppress)
            }
            Some(prev) if prev.size_bucket != next.size_bucket => {
                Some(PolicyTransitionReason::SizeBucket)
            }
            Some(prev) if prev.price_band_tick != next.price_band_tick => {
                // Regime bucket can shift while executable action remains unchanged.
                // Treat such moves as policy no-op to avoid transition churn.
                let action_delta_ticks =
                    ((next.action_price - prev.action_price).abs() / tick.max(1e-9)).abs();
                let min_action_delta_ticks =
                    Self::glft_policy_min_action_transition_ticks(quote_regime);
                if action_delta_ticks >= min_action_delta_ticks {
                    Some(PolicyTransitionReason::PriceBucket)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn glft_policy_min_action_transition_ticks(
        quote_regime: crate::polymarket::glft::QuoteRegime,
    ) -> f64 {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => 3.0,
            crate::polymarket::glft::QuoteRegime::Tracking => 4.0,
            crate::polymarket::glft::QuoteRegime::Guarded => 5.0,
            crate::polymarket::glft::QuoteRegime::Blocked => 99.0,
        }
    }

    fn glft_policy_is_minor_pricebucket_transition(
        prev: QuotePolicyState,
        next: QuotePolicyState,
        quote_regime: crate::polymarket::glft::QuoteRegime,
        tick: f64,
    ) -> bool {
        if !matches!(
            quote_regime,
            crate::polymarket::glft::QuoteRegime::Aligned
                | crate::polymarket::glft::QuoteRegime::Tracking
        ) {
            return false;
        }
        let band_jump = (prev.price_band_tick - next.price_band_tick).abs();
        if band_jump != 1 {
            return false;
        }
        let action_delta_ticks =
            ((next.action_price - prev.action_price).abs() / tick.max(1e-9)).abs();
        action_delta_ticks <= 4.0 + 1e-9
    }

    fn glft_policy_minor_pricebucket_commit_dwell(
        quote_regime: crate::polymarket::glft::QuoteRegime,
    ) -> std::time::Duration {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => {
                std::time::Duration::from_millis(16_000)
            }
            crate::polymarket::glft::QuoteRegime::Tracking => {
                std::time::Duration::from_millis(18_000)
            }
            crate::polymarket::glft::QuoteRegime::Guarded => {
                std::time::Duration::from_millis(9_999)
            }
            crate::polymarket::glft::QuoteRegime::Blocked => {
                std::time::Duration::from_millis(9_999)
            }
        }
    }

    fn glft_policy_minor_pricebucket_publish_hold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(18_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(20_000)
            }
            _ => std::time::Duration::ZERO,
        }
    }

    pub(super) fn update_slot_quote_policy(
        &mut self,
        slot: OrderSlot,
        normalized_target_price: f64,
        size: f64,
        glft: crate::polymarket::glft::GlftSignalSnapshot,
        now: Instant,
        allow_commit: bool,
    ) -> (QuotePolicyState, Option<PolicyTransitionReason>) {
        let idx = slot.index();
        let next = self.build_slot_quote_policy(slot, normalized_target_price, size, glft);
        let active_slot = self.slot_target_active(slot);
        let tick = self.cfg.tick_size.max(1e-9);
        let candidate_transition = Self::classify_policy_transition(
            self.slot_policy_candidates[idx],
            next,
            glft.quote_regime,
            tick,
        );
        if candidate_transition.is_some() {
            self.slot_policy_candidates[idx] = Some(next);
            self.slot_policy_candidate_since[idx] = Some(now);
        } else {
            self.slot_policy_candidates[idx] = Some(next);
        }
        let candidate_since = self.slot_policy_candidate_since[idx].unwrap_or(now);
        let candidate_age = now.saturating_duration_since(candidate_since);
        let commit_ready =
            allow_commit && candidate_age >= Self::glft_policy_commit_dwell(glft.quote_regime);
        let committed = self.slot_policy_states[idx];
        let mut committed_transition = if commit_ready {
            Self::classify_policy_transition(committed, next, glft.quote_regime, tick)
        } else {
            None
        };
        let mut blocked_minor_pricebucket = false;
        if let (Some(prev), Some(PolicyTransitionReason::PriceBucket)) =
            (committed, committed_transition)
        {
            if Self::glft_policy_is_minor_pricebucket_transition(
                prev,
                next,
                glft.quote_regime,
                tick,
            ) {
                let minor_commit_dwell =
                    Self::glft_policy_minor_pricebucket_commit_dwell(glft.quote_regime);
                if candidate_age < minor_commit_dwell {
                    committed_transition = None;
                    blocked_minor_pricebucket = true;
                }
            }
        }
        if let (Some(prev), Some(PolicyTransitionReason::PriceBucket)) =
            (committed, committed_transition)
        {
            let band_jump = (prev.price_band_tick - next.price_band_tick).abs();
            let action_delta_ticks = ((next.action_price - prev.action_price).abs() / tick).abs();
            info!(
                "🧭 GLFT policy band transition {} regime={:?} band={}→{} jump={} action={:.3}→{:.3} delta_ticks={:.1} candidate_age_ms={} commit_ready={}",
                slot.as_str(),
                glft.quote_regime,
                prev.price_band_tick,
                next.price_band_tick,
                band_jump,
                prev.action_price,
                next.action_price,
                action_delta_ticks,
                candidate_age.as_millis(),
                commit_ready
            );
        }
        if committed_transition.is_some() {
            self.slot_policy_states[idx] = Some(next);
            self.slot_policy_since[idx] = match committed_transition {
                Some(PolicyTransitionReason::Activate) if active_slot => self.slot_shadow_since
                    [idx]
                    .or_else(|| Some(self.slot_last_ts(slot)))
                    .or(Some(candidate_since)),
                _ => Some(candidate_since),
            };
            self.slot_last_policy_transition[idx] = committed_transition;
            self.stats.policy_transition_events =
                self.stats.policy_transition_events.saturating_add(1);
        } else if committed.is_none() && commit_ready {
            self.slot_policy_states[idx] = Some(next);
            self.slot_policy_since[idx] = Some(candidate_since);
        } else if let Some(mut committed_state) = self.slot_policy_states[idx] {
            // Keep executable action in sync even when bucket-level policy does not transition.
            // This prevents republish-after-clear from reusing stale in-band prices.
            // Exception: when a minor price-bucket move is deliberately held for
            // extended dwell, keep the committed action frozen so it can commit later.
            if !blocked_minor_pricebucket {
                committed_state.action_price = next.action_price;
                committed_state.size = next.size;
                self.slot_policy_states[idx] = Some(committed_state);
            }
        }
        (
            self.slot_policy_states[idx].unwrap_or(next),
            committed_transition,
        )
    }

    fn emit_quote_log(
        &self,
        slot: OrderSlot,
        price: f64,
        size: f64,
        reason: BidReason,
        log_msg: Option<&str>,
        raw_target: Option<f64>,
        normalized_target: Option<f64>,
        publish_reason: Option<PolicyPublishCause>,
        publish_debt: Option<f64>,
        distance_to_trusted_mid_ticks: Option<f64>,
        distance_to_action_target_ticks: Option<f64>,
        distance_to_shadow_target_ticks: Option<f64>,
    ) {
        // GLFT slot quotes are sometimes nudged right before action (governor/keep-band).
        // Log actionable price/size so quote logs match actual place/reprice.
        if self.cfg.strategy.is_glft_mm() && reason == BidReason::Provide {
            let glft = *self.glft_rx.borrow();
            let inv = self.current_working_inventory();
            let book = self.book;
            let q_norm = (inv.net_diff / self.cfg.max_net_diff.max(1e-9)).clamp(-1.0, 1.0);
            let p_anchor = glft
                .trusted_mid
                .clamp(self.cfg.tick_size, 1.0 - self.cfg.tick_size);
            let alpha_prob = compute_glft_alpha_shift(
                glft.alpha_flow,
                self.cfg.glft_ofi_alpha,
                self.cfg.tick_size,
                glft.fit_status,
            );
            let fit = IntensityFitSnapshot {
                a: glft.fit_a,
                k: glft.fit_k,
                quality: glft.fit_quality,
            };
            let ofi = *self.ofi_rx.borrow();
            let heat_score = ofi.yes.heat_score.max(ofi.no.heat_score);
            let offsets = compute_optimal_offsets(
                q_norm,
                glft.sigma_prob,
                glft.tau_secs,
                fit,
                self.cfg.glft_gamma,
                self.cfg.glft_xi,
                self.cfg.bid_size,
                self.cfg.max_net_diff,
                self.cfg.tick_size,
            );
            let heat_excess = (heat_score - 1.0).max(0.0).min(4.0);
            let spread_mult = (1.0 + self.cfg.glft_ofi_spread_beta * glft.alpha_flow.abs().powi(2))
                * (1.0 + 0.10 * heat_excess.sqrt() * heat_excess.sqrt().min(2.0));
            let half_spread = (offsets.half_spread_base * spread_mult).max(self.cfg.tick_size);
            let r_yes = (p_anchor + alpha_prob - offsets.inventory_shift)
                .clamp(self.cfg.tick_size, 1.0 - self.cfg.tick_size);
            let shaped = shape_glft_quotes(
                r_yes,
                half_spread,
                glft.quote_regime,
                self.cfg.tick_size,
                heat_score,
            );
            let yes_buy = self.aggressive_price_for(
                Side::Yes,
                shaped.yes_buy_ceiling,
                book.yes_bid,
                book.yes_ask,
            );
            let yes_sell = self.aggressive_sell_price_for(
                Side::Yes,
                shaped.yes_sell_floor,
                book.yes_bid,
                book.yes_ask,
            );
            let no_buy = self.aggressive_price_for(
                Side::No,
                shaped.no_buy_ceiling,
                book.no_bid,
                book.no_ask,
            );
            let no_sell = self.aggressive_sell_price_for(
                Side::No,
                shaped.no_sell_floor,
                book.no_bid,
                book.no_ask,
            );
            let fit_source = match glft.fit_source {
                GlftFitSource::Bootstrap => "bootstrap",
                GlftFitSource::WarmStart => "warm-start",
                GlftFitSource::LastGoodFit => "last-good-fit",
            };
            let shadow_target = self
                .slot_shadow_target(slot)
                .map(|target| target.price)
                .unwrap_or(normalized_target.unwrap_or(price));
            let shadow_velocity_tps = self.slot_shadow_velocity_tps[slot.index()];
            let publish_reason = publish_reason
                .map(|reason| reason.as_str())
                .or_else(|| self.slot_publish_reason(slot).map(|reason| reason.as_str()))
                .unwrap_or("none");
            info!(
                "📐 GLFT {} {:?}@{:.3} sz={:.1} | source={} fit={:?}/{:?} state={:?} regime={:?} drift_mode={:?} drift_raw={:.1} drift_ewma={:.1} drift_persist_ms={} slope_tps={:.2} shadow_v_tps={:.2} anchor={:.3} basis={:.3} basis_raw={:.3} basis_clamped={:.3} modeled_mid={:.3} trusted_mid={:.3} synthetic_mid={:.3} poly_soft_stale={} stale_secs={:.2} alpha={:.3} heat={:.2} sigma={:.7} a={:.3} k={:.3} q_norm={:.3} inv_shift={:.6} half_base={:.4} spread_mult={:.3} dominant_side={:?} dominant_buy_penalty_ticks={:.1} dominant_buy_suppressed={} r_yes_pre={:.3} r_yes_post={:.3} raw_target={:.3} normalized_target={:.3} shadow_target={:.3} publish_target={:.3} publish_cause={} publish_debt={:.1} dist_trusted_ticks={:.1} dist_action_ticks={:.1} dist_shadow_ticks={:.1} pre[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3} final[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3}",
                slot.as_str(),
                slot.direction,
                price,
                size,
                fit_source,
                glft.fit_quality,
                if glft.ready && !glft.stale {
                    "live"
                } else {
                    "stale"
                },
                glft.signal_state,
                glft.quote_regime,
                glft.drift_mode,
                glft.drift_raw_ticks,
                glft.drift_ewma_ticks,
                glft.drift_persist_ms,
                glft.trusted_mid_slope_tps,
                shadow_velocity_tps,
                glft.anchor_prob,
                glft.basis_prob,
                glft.basis_raw,
                glft.basis_clamped,
                glft.modeled_mid,
                glft.trusted_mid,
                glft.synthetic_mid_yes,
                glft.poly_soft_stale,
                glft.stale_secs,
                glft.alpha_flow,
                heat_score,
                glft.sigma_prob,
                glft.fit_a,
                glft.fit_k,
                q_norm,
                offsets.inventory_shift,
                offsets.half_spread_base,
                spread_mult,
                shaped.dominant_side,
                shaped.dominant_buy_penalty_ticks,
                shaped.dominant_buy_suppressed,
                shaped.r_yes_pre,
                shaped.r_yes_post,
                raw_target.unwrap_or(price),
                normalized_target.unwrap_or(price),
                shadow_target,
                price,
                publish_reason,
                publish_debt.unwrap_or(0.0),
                distance_to_trusted_mid_ticks.unwrap_or(0.0),
                distance_to_action_target_ticks.unwrap_or(0.0),
                distance_to_shadow_target_ticks.unwrap_or(0.0),
                shaped.yes_buy_ceiling,
                shaped.yes_sell_floor,
                shaped.no_buy_ceiling,
                shaped.no_sell_floor,
                yes_buy,
                yes_sell,
                no_buy,
                no_sell,
            );
            return;
        }
        if let Some(msg) = log_msg {
            info!("{}", msg);
        }
    }

    fn glft_buy_corridor_ticks(quote_regime: crate::polymarket::glft::QuoteRegime) -> f64 {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => 8.0,
            crate::polymarket::glft::QuoteRegime::Tracking => 7.0,
            crate::polymarket::glft::QuoteRegime::Guarded
            | crate::polymarket::glft::QuoteRegime::Blocked => 6.0,
        }
    }

    fn shadow_velocity_ewma(prev: f64, raw: f64) -> f64 {
        // ~1.8s half-life at 5m cadence provides enough smoothing without
        // hiding rapid target swings that should slow publish cadence.
        let alpha = 1.0 - 2f64.powf(-1.0 / 1.8);
        prev + alpha * (raw - prev)
    }

    fn shadow_velocity_decay(prev: f64, dt_secs: f64) -> f64 {
        if prev <= f64::EPSILON || dt_secs <= 0.0 {
            return prev.max(0.0);
        }
        // Decay toward zero when shadow target stops moving; otherwise a
        // single fast jump can keep velocity elevated for too long.
        let half_life_secs = 2.4;
        let decay = 2f64.powf(-(dt_secs / half_life_secs));
        (prev * decay).max(0.0)
    }

    fn glft_shadow_publish_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(2_200)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(2_800)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(3_400)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(3_600)
            }
            None => std::time::Duration::from_millis(2_200),
        }
    }

    fn glft_initial_dwell_book_healthy(&self, slot: OrderSlot) -> bool {
        let side_ok = match slot.side {
            Side::Yes => self.book.yes_bid > 0.0 && self.book.yes_ask > self.book.yes_bid,
            Side::No => self.book.no_bid > 0.0 && self.book.no_ask > self.book.no_bid,
        };
        let opposite_ok = match slot.side {
            Side::Yes => self.book.no_bid > 0.0 && self.book.no_ask > self.book.no_bid,
            Side::No => self.book.yes_bid > 0.0 && self.book.yes_ask > self.book.yes_bid,
        };
        let glft = *self.glft_rx.borrow();
        side_ok
            && opposite_ok
            && glft.stale_secs.is_finite()
            && glft.stale_secs <= 1.0
            && !matches!(
                glft.quote_regime,
                crate::polymarket::glft::QuoteRegime::Blocked
            )
    }

    pub(super) fn glft_publish_target_debt_threshold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 7.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 8.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 9.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 7.0,
        }
    }

    pub(super) fn glft_publish_structural_debt_threshold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 6.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 7.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 8.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 6.0,
        }
    }

    fn glft_structural_publish_velocity_cap_tps(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 10.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 8.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 6.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 4.0,
            None => 10.0,
        }
    }

    fn glft_structural_force_publish_threshold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 24.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 20.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 16.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 24.0,
        }
    }

    fn glft_structural_publish_margin_ticks(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 3.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 3.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 2.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 3.0,
        }
    }

    fn glft_publish_debt_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        // Debt-sync should be sparser than initial publish: it is a structural
        // convergence loop, not a per-tick tracking loop.
        Self::glft_debt_publish_cooldown(quote_regime)
    }

    fn glft_debt_publish_cooldown(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(4_800)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(5_600)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(6_400)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(6_000)
            }
            None => std::time::Duration::from_millis(4_800),
        }
    }

    fn glft_publish_reason_cooldown(
        reason: PolicyPublishCause,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match reason {
            PolicyPublishCause::Initial => std::time::Duration::ZERO,
            PolicyPublishCause::Policy => Self::glft_debt_publish_cooldown(quote_regime),
            PolicyPublishCause::Safety => std::time::Duration::from_millis(150),
            PolicyPublishCause::Recovery => std::time::Duration::ZERO,
        }
    }

    fn glft_publish_should_settle_to_shadow(
        reason: PolicyPublishCause,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        shadow_price_gap: f64,
        tick: f64,
        shadow_gap_ticks: Option<f64>,
    ) -> bool {
        let settle_gap = match reason {
            PolicyPublishCause::Recovery | PolicyPublishCause::Safety => 2.0 * tick,
            PolicyPublishCause::Policy => match quote_regime {
                Some(crate::polymarket::glft::QuoteRegime::Guarded) => 5.0 * tick,
                Some(crate::polymarket::glft::QuoteRegime::Tracking) => 4.0 * tick,
                _ => 3.0 * tick,
            },
            PolicyPublishCause::Initial => f64::INFINITY,
        };
        let min_shadow_gap_ticks = match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 5.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 4.0,
            _ => 4.0,
        };
        let shadow_large_enough = shadow_gap_ticks
            .map(|ticks| ticks >= min_shadow_gap_ticks)
            .unwrap_or(false);
        shadow_price_gap >= settle_gap && shadow_large_enough
    }

    fn glft_debt_single_release_cap_ticks(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        publish_debt_ticks: f64,
        structural_debt_ticks: f64,
    ) -> f64 {
        let base = match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 10.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 8.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 6.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 3.0,
            None => 10.0,
        };
        // Keep release monotonic but bounded: debt scheduler decides *when* to publish,
        // price governor decides *how fast* published orders move.
        let debt = publish_debt_ticks.max(structural_debt_ticks).max(0.0);
        let dynamic_extra = ((debt - base).max(0.0) * 0.12).clamp(0.0, 3.0);
        (base + dynamic_extra).max(1.0)
    }

    fn glft_debt_fast_settle_threshold_ticks(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 9.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 10.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 11.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 9.0,
        }
    }

    fn glft_debt_fast_settle_velocity_cap_tps(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 2.5,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 2.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 1.6,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 0.0,
            None => 2.5,
        }
    }

    fn glft_debt_fast_settle_ready(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        publish_debt_ticks: f64,
        structural_debt_ticks: f64,
        shadow_velocity_tps: f64,
    ) -> bool {
        let debt = publish_debt_ticks.max(structural_debt_ticks).max(0.0);
        debt >= Self::glft_debt_fast_settle_threshold_ticks(quote_regime)
            && shadow_velocity_tps <= Self::glft_debt_fast_settle_velocity_cap_tps(quote_regime)
    }

    fn glft_hard_debt_settle_threshold_ticks(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 14.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 12.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 10.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 14.0,
        }
    }

    pub(super) fn glft_apply_debt_release_cap(
        &self,
        current_price: f64,
        shadow_target_price: f64,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        publish_debt_ticks: f64,
        structural_debt_ticks: f64,
    ) -> f64 {
        let tick = self.cfg.tick_size.max(1e-9);
        let hard_settle_threshold = Self::glft_hard_debt_settle_threshold_ticks(quote_regime);
        let max_debt_ticks = publish_debt_ticks.max(structural_debt_ticks).max(0.0);
        if max_debt_ticks >= hard_settle_threshold {
            return shadow_target_price;
        }
        let max_step_ticks = Self::glft_debt_single_release_cap_ticks(
            quote_regime,
            publish_debt_ticks,
            structural_debt_ticks,
        )
        .max(1.0);
        let max_step = max_step_ticks * tick;
        let gap = shadow_target_price - current_price;
        if gap.abs() <= max_step + 1e-9 {
            return shadow_target_price;
        }

        let stepped = self.glft_governed_price(current_price, shadow_target_price, max_step_ticks);
        if (stepped - current_price).abs() >= tick * 0.5 {
            stepped
        } else if shadow_target_price > current_price {
            self.quantize_up_to_tick((current_price + tick).min(shadow_target_price))
        } else {
            self.quantize_down_to_tick((current_price - tick).max(shadow_target_price))
        }
    }

    fn record_publish_reason_stats(&mut self, publish_reason: Option<PolicyPublishCause>) {
        match publish_reason {
            Some(PolicyPublishCause::Initial) => {
                self.stats.publish_from_initial = self.stats.publish_from_initial.saturating_add(1);
            }
            Some(PolicyPublishCause::Policy) => {
                self.stats.publish_from_policy = self.stats.publish_from_policy.saturating_add(1);
            }
            Some(PolicyPublishCause::Safety) => {
                self.stats.publish_from_safety = self.stats.publish_from_safety.saturating_add(1);
            }
            Some(PolicyPublishCause::Recovery) => {
                self.stats.publish_from_recovery =
                    self.stats.publish_from_recovery.saturating_add(1);
            }
            None => {}
        }
    }

    fn glft_stale_quote_thresholds(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> (f64, f64) {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => (12.0, 8.0),
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => (10.0, 7.0),
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => (8.0, 6.0),
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => (6.0, 5.0),
            None => (12.0, 8.0),
        }
    }

    fn glft_allow_target_follow_debt(
        _quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> bool {
        // Structural-convergence mode:
        // disable target-follow debt in all regimes so publish only fires on
        // reference/safety misalignment, not model micro-tracking.
        false
    }

    fn glft_clamp_slot_target_price(&self, slot: OrderSlot, price: f64) -> f64 {
        if !(price > 0.0) || slot.direction != TradeDirection::Buy {
            return price;
        }
        let glft = *self.glft_rx.borrow();
        let tick = self.cfg.tick_size.max(1e-9);
        let max_dev = Self::glft_buy_corridor_ticks(glft.quote_regime) * tick;
        let side_trusted_mid = match slot.side {
            Side::Yes => glft.trusted_mid,
            Side::No => 1.0 - glft.trusted_mid,
        }
        .clamp(tick, 1.0 - tick);
        let lower = (side_trusted_mid - max_dev).clamp(tick, 1.0 - tick);
        let upper = (side_trusted_mid + max_dev).clamp(tick, 1.0 - tick);
        price.clamp(lower, upper)
    }

    fn update_slot_shadow_target(
        &mut self,
        slot: OrderSlot,
        normalized_target_price: f64,
        size: f64,
        reason: BidReason,
    ) -> DesiredTarget {
        let idx = slot.index();
        let next = DesiredTarget {
            side: slot.side,
            direction: slot.direction,
            price: normalized_target_price,
            size,
            reason,
        };
        let now = Instant::now();
        if let Some(last_ts) = self.slot_shadow_last_change_ts[idx] {
            let dt = now.saturating_duration_since(last_ts).as_secs_f64();
            let prev_velocity = self.slot_shadow_velocity_tps[idx].max(0.0);
            self.slot_shadow_velocity_tps[idx] = Self::shadow_velocity_decay(prev_velocity, dt);
        }
        let changed = match self.slot_shadow_targets[idx].as_ref() {
            Some(current) => {
                current.direction != next.direction
                    || current.reason != next.reason
                    || (current.price - next.price).abs() > 1e-9
                    || (current.size - next.size).abs() > 0.1
            }
            None => true,
        };
        if changed {
            let raw_velocity_tps = self.slot_shadow_targets[idx]
                .as_ref()
                .and_then(|current| {
                    self.slot_shadow_last_change_ts[idx].map(|last_ts| {
                        let dt = now
                            .saturating_duration_since(last_ts)
                            .as_secs_f64()
                            .max(1e-3);
                        let tick = self.cfg.tick_size.max(1e-9);
                        ((next.price - current.price).abs() / tick) / dt
                    })
                })
                .unwrap_or(0.0);
            let prev_velocity = self.slot_shadow_velocity_tps[idx].max(0.0);
            self.slot_shadow_velocity_tps[idx] = if prev_velocity <= f64::EPSILON {
                raw_velocity_tps
            } else {
                Self::shadow_velocity_ewma(prev_velocity, raw_velocity_tps)
            };
            self.slot_shadow_last_change_ts[idx] = Some(now);
            self.slot_shadow_targets[idx] = Some(next.clone());
            self.slot_shadow_since[idx] = Some(now);
        }
        self.slot_shadow_targets[idx].clone().unwrap_or(next)
    }

    pub(super) fn slot_relative_misalignment(
        &self,
        slot: OrderSlot,
        quoted_price: f64,
        target_price: f64,
    ) -> Option<(f64, f64, bool)> {
        if !self.cfg.strategy.is_glft_mm() {
            return None;
        }
        let glft = *self.glft_rx.borrow();
        let side_trusted_mid = match slot.side {
            Side::Yes => glft.trusted_mid,
            Side::No => 1.0 - glft.trusted_mid,
        };
        let tick = self.cfg.tick_size.max(1e-9);
        let distance_to_trusted_mid_ticks = ((quoted_price - side_trusted_mid).abs()) / tick;
        let distance_to_target_ticks = ((quoted_price - target_price).abs()) / tick;
        let (trusted_threshold, target_threshold) =
            Self::glft_force_realign_thresholds(glft.quote_regime);
        let force_realign = distance_to_trusted_mid_ticks > trusted_threshold
            && distance_to_target_ticks > target_threshold;
        Some((
            distance_to_trusted_mid_ticks,
            distance_to_target_ticks,
            force_realign,
        ))
    }

    fn glft_force_realign_thresholds(
        quote_regime: crate::polymarket::glft::QuoteRegime,
    ) -> (f64, f64) {
        match quote_regime {
            crate::polymarket::glft::QuoteRegime::Aligned => (14.0, 10.0),
            crate::polymarket::glft::QuoteRegime::Tracking => (13.0, 9.0),
            crate::polymarket::glft::QuoteRegime::Guarded => (12.0, 8.0),
            crate::polymarket::glft::QuoteRegime::Blocked => (11.0, 8.0),
        }
    }

    fn slot_quote_unsafe_depth_ticks(
        &self,
        inv: &InventoryState,
        slot: OrderSlot,
        quoted_price: f64,
        quoted_size: f64,
        reason: BidReason,
        best_bid: f64,
        best_ask: f64,
    ) -> Option<f64> {
        let tick = self.cfg.tick_size.max(1e-9);
        match slot.direction {
            TradeDirection::Buy => {
                let max_post_only_ceiling = 1.0 - tick;
                let maker_safe_cap =
                    self.aggressive_price_for(slot.side, max_post_only_ceiling, best_bid, best_ask);
                if maker_safe_cap <= 0.0 {
                    return Some(f64::INFINITY);
                }
                if quoted_price > maker_safe_cap + 1e-9 {
                    return Some(((quoted_price - maker_safe_cap) / tick).max(0.0));
                }
                if !self.passes_outcome_floor_for_buy(
                    inv,
                    slot.side,
                    quoted_size.max(0.0),
                    quoted_price,
                    reason,
                ) {
                    return Some(f64::INFINITY);
                }
                None
            }
            TradeDirection::Sell => {
                let min_post_only_floor = tick;
                let maker_safe_floor = self.aggressive_sell_price_for(
                    slot.side,
                    min_post_only_floor,
                    best_bid,
                    best_ask,
                );
                if maker_safe_floor <= 0.0 {
                    return Some(f64::INFINITY);
                }
                if quoted_price + 1e-9 < maker_safe_floor {
                    return Some(((maker_safe_floor - quoted_price) / tick).max(0.0));
                }
                None
            }
        }
    }

    fn sync_buy_side_wrapper(&mut self, slot: OrderSlot) {
        match slot {
            OrderSlot::YES_BUY => {
                self.yes_target = self.slot_targets[slot.index()].clone();
                self.yes_last_ts = self.slot_last_ts[slot.index()];
            }
            OrderSlot::NO_BUY => {
                self.no_target = self.slot_targets[slot.index()].clone();
                self.no_last_ts = self.slot_last_ts[slot.index()];
            }
            _ => {}
        }
    }
}
