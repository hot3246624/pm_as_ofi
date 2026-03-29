use std::time::Instant;

use tracing::{debug, info, warn};

use crate::polymarket::glft::{
    compute_glft_alpha_shift, compute_optimal_offsets, shape_glft_quotes, GlftFitSource,
    IntensityFitSnapshot,
};

use super::*;

impl StrategyCoordinator {
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
        let current_target = self.slot_target(slot).cloned();
        let last_ts = self.slot_last_ts(slot);
        let active = current_target.is_some();
        let slot_price = current_target.as_ref().map(|t| t.price).unwrap_or(0.0);
        let slot_size = current_target.as_ref().map(|t| t.size).unwrap_or(0.0);
        let slot_direction = current_target.as_ref().map(|t| t.direction);
        let now = Instant::now();
        let reprice_eps = 1e-9;
        let raw_target_price = price;
        if self.cfg.strategy == StrategyKind::GlftMm && reason == BidReason::Provide {
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
        if self.cfg.strategy == StrategyKind::GlftMm {
            let glft = *self.glft_rx.borrow();
            if !self.glft_is_tradeable_snapshot(glft) {
                if active {
                    self.clear_slot_target(slot, CancelReason::StaleData).await;
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
        };
        let elapsed = last_ts.elapsed();
        let debounce = std::time::Duration::from_millis(debounce_ms);
        if elapsed < debounce {
            self.stats.skipped_debounce += 1;
            return;
        }
        let recent_cross_reject =
            self.recent_cross_reject(slot.side, std::time::Duration::from_secs(2));

        let force_glft_drift_reprice = self.cfg.strategy == StrategyKind::GlftMm && active && {
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

        if self.cfg.strategy == StrategyKind::GlftMm && active && !recent_cross_reject {
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

        let glft_snapshot = if self.cfg.strategy == StrategyKind::GlftMm {
            Some(*self.glft_rx.borrow())
        } else {
            None
        };
        let glft_quote_regime = glft_snapshot.map(|s| s.quote_regime);
        let glft_soft_stale = glft_snapshot.map(|s| s.poly_soft_stale).unwrap_or(false);
        let regime_transition_active =
            self.glft_update_slot_regime_state(slot, glft_quote_regime, now);
        let mut shadow_publish_dwell = Self::glft_shadow_publish_dwell(glft_quote_regime);
        if regime_transition_active {
            shadow_publish_dwell += Self::glft_regime_transition_extra_dwell(glft_quote_regime);
        }
        let mut publish_reason: Option<SlotPublishReason> = None;
        let mut distance_to_trusted_mid_ticks: Option<f64> = None;
        let mut distance_to_action_target_ticks: Option<f64> = None;
        let mut distance_to_shadow_target_ticks: Option<f64> = None;
        let mut publish_debt: Option<f64> = None;
        let mut publish_hard_safety_exception = false;
        let mut shadow_target_price = normalized_target_price;
        let glft_shadow_mode =
            self.cfg.strategy == StrategyKind::GlftMm && reason == BidReason::Provide;
        if glft_shadow_mode {
            let shadow_target =
                self.update_slot_shadow_target(slot, normalized_target_price, size, reason);
            shadow_target_price = shadow_target.price;
            let shadow_age = self
                .slot_shadow_since(slot)
                .map(|ts| ts.elapsed())
                .unwrap_or_default();
            if active {
                if let Some((trusted_dist, action_target_dist, _force_realign)) =
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
                if recent_cross_reject {
                    publish_reason = Some(SlotPublishReason::CrossRecovery);
                    publish_hard_safety_exception = true;
                } else if shadow_age >= shadow_publish_dwell
                    && self.glft_initial_dwell_book_healthy(slot)
                {
                    publish_reason = Some(SlotPublishReason::Initial);
                }
            }
        }

        // GLFT soft throttle: avoid repricing every sub-second micro-jitter.
        // Keep fast paths for large drift and crossed-book recovery.
        if self.cfg.strategy == StrategyKind::GlftMm
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
            if self.cfg.strategy == StrategyKind::GlftMm {
                if !self.glft_is_tradeable_now() {
                    return;
                }
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
            if glft_shadow_mode && matches!(publish_reason, Some(SlotPublishReason::Debt)) {
                self.consume_slot_publish_debt_release(slot, glft_quote_regime);
            }
            self.record_publish_reason_stats(publish_reason);
            self.emit_quote_log(
                slot,
                price,
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
            self.place_slot(slot, price, size, reason).await;
        } else {
            if self.cfg.strategy == StrategyKind::GlftMm {
                if !self.glft_is_tradeable_now() {
                    self.clear_slot_target(slot, CancelReason::StaleData).await;
                    return;
                }
            }
            let reprice_band = self.maker_keep_band(reason);
            let cross_reprice_override = self.cfg.strategy == StrategyKind::GlftMm
                && recent_cross_reject
                && (slot_price - price).abs() >= self.cfg.tick_size.max(1e-9) - reprice_eps;
            let mut needs_reprice = force_glft_drift_reprice
                || cross_reprice_override
                || slot_direction != Some(slot.direction)
                || (slot_price - price).abs() >= (reprice_band - reprice_eps).max(0.0)
                || (slot_size - size).abs() > 0.1;
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
                let inv = *self.inv_rx.borrow();
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
                let invalid_quote_state = unsafe_depth_ticks
                    .map(|depth| !depth.is_finite())
                    .unwrap_or(false);
                let tick = self.cfg.tick_size.max(1e-9);
                let (stale_trusted_ticks, stale_target_ticks) =
                    Self::glft_stale_quote_thresholds(glft_quote_regime);
                let target_debt_ticks = (price_gap_to_shadow / tick).floor();
                let size_debt_ticks = if size_gap > 0.1 { 4.0 } else { 0.0 };
                let reference_debt_ticks = distance_to_trusted_mid_ticks
                    .zip(distance_to_shadow_target_ticks)
                    .map(|(trusted_dist, shadow_target_dist)| {
                        (trusted_dist - stale_trusted_ticks)
                            .max(0.0)
                            .max((shadow_target_dist - stale_target_ticks).max(0.0))
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
                let target_follow_debt_ticks = target_debt_ticks.max(size_debt_ticks);
                let structural_debt_ticks = reference_debt_ticks.max(safety_debt_ticks);
                let allow_target_follow_debt =
                    Self::glft_allow_target_follow_debt(glft_quote_regime) && !glft_soft_stale;
                let effective_target_follow_debt_ticks = if allow_target_follow_debt {
                    target_follow_debt_ticks
                } else {
                    0.0
                };
                let total_publish_debt =
                    effective_target_follow_debt_ticks.max(structural_debt_ticks);
                let mut target_follow_threshold =
                    Self::glft_publish_target_debt_threshold(glft_quote_regime);
                let mut structural_threshold =
                    Self::glft_publish_structural_debt_threshold(glft_quote_regime);
                let mut debt_release_threshold =
                    Self::glft_publish_debt_release_threshold(glft_quote_regime);
                let mut debt_dwell = Self::glft_publish_debt_dwell(glft_quote_regime);
                if regime_transition_active {
                    let bump = Self::glft_transition_debt_threshold_bump(glft_quote_regime);
                    target_follow_threshold += bump;
                    structural_threshold += bump;
                    debt_release_threshold += bump;
                    debt_dwell += Self::glft_regime_transition_extra_dwell(glft_quote_regime);
                }
                if glft_soft_stale {
                    self.consume_slot_publish_debt_release(slot, glft_quote_regime);
                }
                // Structural debt can bypass elapsed-vs-shadow asymmetry because it reflects
                // safety/reference integrity instead of micro target chasing.
                let structural_dwell_ready = elapsed >= debt_dwell || shadow_age >= debt_dwell;
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
                let publish_debt_ready = (structural_debt_ticks >= structural_threshold
                    && structural_dwell_ready)
                    || (effective_target_follow_debt_ticks >= target_follow_threshold
                        && target_dwell_ready)
                    || (debt_accum >= debt_release_threshold
                        && (structural_dwell_ready || target_dwell_ready));
                publish_reason = if recent_cross_reject {
                    publish_hard_safety_exception = true;
                    Some(SlotPublishReason::CrossRecovery)
                } else if invalid_quote_state {
                    publish_hard_safety_exception = true;
                    Some(SlotPublishReason::InvalidState)
                } else if !glft_soft_stale && publish_debt_ready {
                    Some(SlotPublishReason::Debt)
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
                        // Once a publish reason is confirmed, settle directly to shadow target
                        // instead of emitting another partial governor step. This removes
                        // repetitive 2-4 tick debt-chasing reprices.
                        price = shadow_target_price;
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
                if glft_shadow_mode && matches!(publish_reason, Some(SlotPublishReason::Debt)) {
                    self.consume_slot_publish_debt_release(slot, glft_quote_regime);
                }
                self.record_publish_reason_stats(publish_reason);
                self.emit_quote_log(
                    slot,
                    price,
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
                    price,
                    size,
                    reprice_band,
                    publish_reason
                );
                self.place_slot(slot, price, size, reason).await;
            }
        }
    }

    pub(super) async fn clear_target(&mut self, side: Side, reason: CancelReason) {
        self.clear_slot_target(OrderSlot::new(side, TradeDirection::Buy), reason)
            .await;
    }

    pub(super) async fn clear_slot_target(&mut self, slot: OrderSlot, reason: CancelReason) {
        let active = self.slot_target(slot).is_some();
        if !active {
            return;
        }
        match reason {
            CancelReason::ToxicFlow => self.stats.cancel_toxic += 1,
            CancelReason::StaleData => self.stats.cancel_stale += 1,
            CancelReason::InventoryLimit => self.stats.cancel_inv += 1,
            CancelReason::Reprice => self.stats.cancel_reprice += 1,
            _ => {}
        }

        self.slot_targets[slot.index()] = None;
        self.slot_shadow_targets[slot.index()] = None;
        self.slot_shadow_since[slot.index()] = None;
        self.slot_last_publish_reason[slot.index()] = None;
        self.reset_slot_publish_state(slot);
        match slot {
            OrderSlot::YES_BUY => self.yes_target = None,
            OrderSlot::NO_BUY => self.no_target = None,
            _ => {}
        }
        self.sync_buy_side_wrapper(slot);

        debug!("🗑️ Cancel {} ({:?})", slot.as_str(), reason);
        self.stats.cancel_events = self.stats.cancel_events.saturating_add(1);
        if self.cfg.dry_run {
            info!("📝 DRY cancel {} ({:?})", slot.as_str(), reason);
            return;
        }

        let _ = self
            .om_tx
            .send(OrderManagerCmd::ClearTarget { slot, reason })
            .await;
    }

    pub(super) async fn dispatch_taker_intent(
        &mut self,
        side: Side,
        direction: TradeDirection,
        size: f64,
        purpose: TradePurpose,
    ) {
        let rounded = (size * 100.0).floor() / 100.0;
        if rounded < 0.01 {
            debug!(
                "🧩 Skip taker {:?} {:?}: size {:.4} below lot floor 0.01",
                direction, side, size
            );
            return;
        }

        for slot in OrderSlot::side_slots(side) {
            self.clear_slot_target(slot, CancelReason::Reprice).await;
        }
        if self.cfg.dry_run {
            info!(
                "📝 DRY TAKER {:?} {:?} sz={:.2} purpose={:?}",
                direction, side, rounded, purpose
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
        }
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
                BidReason::Provide => CancelReason::InventoryLimit,
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
        self.sync_buy_side_wrapper(slot);

        self.stats.placed += 1;
        if replacing {
            self.stats.replace_events = self.stats.replace_events.saturating_add(1);
        } else {
            self.stats.publish_events = self.stats.publish_events.saturating_add(1);
        }

        if self.cfg.dry_run {
            info!(
                "📝 DRY {:?} {:?} {:?}@{:.3} sz={:.1}",
                reason, slot.direction, slot.side, price, size
            );
            return;
        }

        let _ = self.om_tx.send(OrderManagerCmd::SetTarget(target)).await;
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
        publish_reason: Option<SlotPublishReason>,
        publish_debt: Option<f64>,
        distance_to_trusted_mid_ticks: Option<f64>,
        distance_to_action_target_ticks: Option<f64>,
        distance_to_shadow_target_ticks: Option<f64>,
    ) {
        // GLFT slot quotes are sometimes nudged right before action (governor/keep-band).
        // Log actionable price/size so quote logs match actual place/reprice.
        if self.cfg.strategy == StrategyKind::GlftMm && reason == BidReason::Provide {
            let glft = *self.glft_rx.borrow();
            let inv = *self.inv_rx.borrow();
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
            let publish_reason = publish_reason
                .map(|reason| reason.as_str())
                .or_else(|| self.slot_publish_reason(slot).map(|reason| reason.as_str()))
                .unwrap_or("none");
            info!(
                "📐 GLFT {} {:?}@{:.3} sz={:.1} | source={} fit={:?}/{:?} state={:?} regime={:?} drift_mode={:?} drift_raw={:.1} drift_ewma={:.1} drift_persist_ms={} anchor={:.3} basis={:.3} basis_raw={:.3} basis_clamped={:.3} modeled_mid={:.3} trusted_mid={:.3} synthetic_mid={:.3} poly_soft_stale={} stale_secs={:.2} alpha={:.3} heat={:.2} sigma={:.7} a={:.3} k={:.3} q_norm={:.3} inv_shift={:.6} half_base={:.4} spread_mult={:.3} dominant_side={:?} dominant_buy_penalty_ticks={:.1} dominant_buy_suppressed={} r_yes_pre={:.3} r_yes_post={:.3} raw_target={:.3} normalized_target={:.3} shadow_target={:.3} publish_target={:.3} publish_cause={} publish_debt={:.1} dist_trusted_ticks={:.1} dist_action_ticks={:.1} dist_shadow_ticks={:.1} pre[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3} final[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3}",
                slot.as_str(),
                slot.direction,
                price,
                size,
                fit_source,
                glft.fit_quality,
                if glft.ready && !glft.stale { "live" } else { "stale" },
                glft.signal_state,
                glft.quote_regime,
                glft.drift_mode,
                glft.drift_raw_ticks,
                glft.drift_ewma_ticks,
                glft.drift_persist_ms,
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

    fn glft_update_slot_regime_state(
        &mut self,
        slot: OrderSlot,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        now: Instant,
    ) -> bool {
        let idx = slot.index();
        let Some(regime) = quote_regime else {
            self.slot_last_regime_seen[idx] = None;
            self.slot_regime_changed_at[idx] = now;
            return false;
        };
        if self.slot_last_regime_seen[idx] != Some(regime) {
            self.slot_last_regime_seen[idx] = Some(regime);
            self.slot_regime_changed_at[idx] = now;
            return true;
        }
        now.saturating_duration_since(self.slot_regime_changed_at[idx])
            < Self::glft_regime_transition_grace(regime)
    }

    fn glft_regime_transition_grace(
        regime: crate::polymarket::glft::QuoteRegime,
    ) -> std::time::Duration {
        match regime {
            crate::polymarket::glft::QuoteRegime::Aligned => std::time::Duration::from_millis(500),
            crate::polymarket::glft::QuoteRegime::Tracking => std::time::Duration::from_millis(850),
            crate::polymarket::glft::QuoteRegime::Guarded => std::time::Duration::from_millis(1200),
            crate::polymarket::glft::QuoteRegime::Blocked => std::time::Duration::from_millis(1500),
        }
    }

    fn glft_regime_transition_extra_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(250)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(350)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(450)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(600)
            }
            None => std::time::Duration::from_millis(250),
        }
    }

    fn glft_transition_debt_threshold_bump(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 1.5,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 1.25,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 1.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 0.5,
            None => 1.0,
        }
    }

    fn glft_shadow_publish_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(700)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(1000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(1400)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(1800)
            }
            None => std::time::Duration::from_millis(700),
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
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 4.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 5.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 6.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 4.0,
        }
    }

    pub(super) fn glft_publish_structural_debt_threshold(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 2.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 3.0,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 4.0,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 99.0,
            None => 2.0,
        }
    }

    fn glft_publish_debt_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        Self::glft_shadow_publish_dwell(quote_regime)
    }

    fn glft_publish_reason_cooldown(
        reason: SlotPublishReason,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match reason {
            SlotPublishReason::Initial => std::time::Duration::ZERO,
            SlotPublishReason::Debt => Self::glft_shadow_publish_dwell(quote_regime),
            SlotPublishReason::InvalidState => std::time::Duration::from_millis(150),
            SlotPublishReason::CrossRecovery => std::time::Duration::ZERO,
        }
    }

    fn glft_publish_should_settle_to_shadow(
        reason: SlotPublishReason,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        shadow_price_gap: f64,
        tick: f64,
        shadow_gap_ticks: Option<f64>,
    ) -> bool {
        let settle_gap = match reason {
            SlotPublishReason::CrossRecovery | SlotPublishReason::InvalidState => 2.0 * tick,
            SlotPublishReason::Debt => match quote_regime {
                Some(crate::polymarket::glft::QuoteRegime::Guarded) => 4.0 * tick,
                Some(crate::polymarket::glft::QuoteRegime::Tracking) => 3.0 * tick,
                _ => 2.0 * tick,
            },
            SlotPublishReason::Initial => f64::INFINITY,
        };
        // Shadow-gap gating should follow regime strictness but avoid 2-tick boundary chatter
        // in Aligned/Tracking where most debt publishes happen.
        let min_shadow_gap_ticks = match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 3.0,
            _ => 2.0,
        };
        let shadow_large_enough = shadow_gap_ticks
            .map(|ticks| ticks >= min_shadow_gap_ticks)
            .unwrap_or(false);
        shadow_price_gap >= settle_gap && shadow_large_enough
    }

    fn record_publish_reason_stats(&mut self, publish_reason: Option<SlotPublishReason>) {
        if matches!(publish_reason, Some(SlotPublishReason::InvalidState)) {
            self.stats.forced_realign_count = self.stats.forced_realign_count.saturating_add(1);
            self.stats.forced_realign_hard_count =
                self.stats.forced_realign_hard_count.saturating_add(1);
        }
        if matches!(publish_reason, Some(SlotPublishReason::Debt)) {
            self.stats.debt_realign_triggers = self.stats.debt_realign_triggers.saturating_add(1);
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
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> bool {
        matches!(
            quote_regime,
            None | Some(crate::polymarket::glft::QuoteRegime::Aligned)
        )
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
        let next = DesiredTarget {
            side: slot.side,
            direction: slot.direction,
            price: normalized_target_price,
            size,
            reason,
        };
        let idx = slot.index();
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
            self.slot_shadow_targets[idx] = Some(next.clone());
            self.slot_shadow_since[idx] = Some(Instant::now());
        }
        self.slot_shadow_targets[idx].clone().unwrap_or(next)
    }

    pub(super) fn slot_relative_misalignment(
        &self,
        slot: OrderSlot,
        quoted_price: f64,
        target_price: f64,
    ) -> Option<(f64, f64, bool)> {
        if self.cfg.strategy != StrategyKind::GlftMm {
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
        let force_realign = distance_to_trusted_mid_ticks > 20.0 && distance_to_target_ticks > 14.0;
        Some((
            distance_to_trusted_mid_ticks,
            distance_to_target_ticks,
            force_realign,
        ))
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
