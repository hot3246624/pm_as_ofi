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
                    TradeDirection::Buy => slot_price + drift_cap < raw_target_price,
                    TradeDirection::Sell => slot_price > raw_target_price + drift_cap,
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

        let glft_drift_mode = if self.cfg.strategy == StrategyKind::GlftMm {
            Some(self.glft_rx.borrow().drift_mode)
        } else {
            None
        };
        let glft_heat_score = if self.cfg.strategy == StrategyKind::GlftMm {
            let ofi = *self.ofi_rx.borrow();
            ofi.yes.heat_score.max(ofi.no.heat_score)
        } else {
            0.0
        };
        let shadow_publish_dwell =
            Self::glft_shadow_publish_dwell(glft_drift_mode, glft_heat_score);
        let mut publish_reason: Option<SlotPublishReason> = None;
        let mut distance_to_trusted_mid_ticks: Option<f64> = None;
        let mut distance_to_normalized_target_ticks: Option<f64> = None;
        let mut debt_forced_realign = false;
        let mut debt_realign_selected = false;
        let mut publish_hard_safety_exception = false;
        let glft_shadow_mode =
            self.cfg.strategy == StrategyKind::GlftMm && reason == BidReason::Provide;
        if glft_shadow_mode {
            let shadow_target =
                self.update_slot_shadow_target(slot, normalized_target_price, size, reason);
            let shadow_age = self
                .slot_shadow_since(slot)
                .map(|ts| ts.elapsed())
                .unwrap_or_default();
            if active {
                if let Some((trusted_dist, target_dist, force_realign)) =
                    self.slot_relative_misalignment(slot, slot_price, normalized_target_price)
                {
                    distance_to_trusted_mid_ticks = Some(trusted_dist);
                    distance_to_normalized_target_ticks = Some(target_dist);
                    if force_realign {
                        publish_reason = Some(SlotPublishReason::ForcedRealign);
                        publish_hard_safety_exception = true;
                    }
                    let debt = self.update_slot_realign_debt(
                        slot,
                        trusted_dist,
                        target_dist,
                        glft_drift_mode,
                        glft_heat_score,
                        now,
                    );
                    debt_forced_realign =
                        Self::glft_realign_debt_trigger(debt, glft_drift_mode, glft_heat_score);
                }
            } else {
                self.reset_slot_realign_debt(slot);
                if let Some((trusted_dist, target_dist, _)) = self.slot_relative_misalignment(
                    slot,
                    shadow_target.price,
                    normalized_target_price,
                ) {
                    distance_to_trusted_mid_ticks = Some(trusted_dist);
                    distance_to_normalized_target_ticks = Some(target_dist);
                }
                if recent_cross_reject {
                    publish_reason = Some(SlotPublishReason::CrossRejectRecovery);
                    publish_hard_safety_exception = true;
                } else if shadow_age >= shadow_publish_dwell {
                    publish_reason = Some(SlotPublishReason::InitialDwell);
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
            let target_gap_ticks = ((slot_price - normalized_target_price).abs() / tick).floor();
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
                        || Self::glft_publish_reason_is_emergency(reason_kind);
                    let budget_cost = Self::glft_publish_reason_budget_cost(
                        reason_kind,
                        glft_drift_mode,
                        glft_heat_score,
                    );
                    if !bypass_budget
                        && !self.consume_slot_publish_budget(
                            slot,
                            glft_drift_mode,
                            glft_heat_score,
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
            self.record_publish_reason_stats(publish_reason, debt_realign_selected);
            self.emit_quote_log(
                slot,
                price,
                size,
                reason,
                log_msg.as_deref(),
                Some(raw_target_price),
                Some(normalized_target_price),
                publish_reason,
                distance_to_trusted_mid_ticks,
                distance_to_normalized_target_ticks,
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
            let needs_reprice = force_glft_drift_reprice
                || cross_reprice_override
                || slot_direction != Some(slot.direction)
                || (slot_price - price).abs() >= (reprice_band - reprice_eps).max(0.0)
                || (slot_size - size).abs() > 0.1;
            if glft_shadow_mode && publish_reason.is_none() && needs_reprice {
                let shadow_age = self
                    .slot_shadow_since(slot)
                    .map(|ts| ts.elapsed())
                    .unwrap_or_default();
                let price_gap = (slot_price - normalized_target_price).abs();
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
                let unsafe_current_quote = unsafe_depth_ticks.is_some();
                let (unsafe_trusted_ticks, unsafe_target_ticks) =
                    Self::glft_unsafe_distance_thresholds(glft_drift_mode, glft_heat_score);
                let unsafe_needs_publish = distance_to_trusted_mid_ticks
                    .zip(distance_to_normalized_target_ticks)
                    .map(|(trusted_dist, target_dist)| {
                        trusted_dist > unsafe_trusted_ticks || target_dist > unsafe_target_ticks
                    })
                    .unwrap_or(false);
                let tick = self.cfg.tick_size.max(1e-9);
                let (prealign_trusted_ticks, prealign_target_ticks) = match glft_drift_mode {
                    Some(crate::polymarket::glft::DriftMode::Damped) => (7.0, 4.0),
                    Some(crate::polymarket::glft::DriftMode::Frozen) => (6.0, 4.0),
                    Some(crate::polymarket::glft::DriftMode::Paused) => (5.0, 3.0),
                    _ => (8.0, 4.0),
                };
                let prealign_extra_ticks = Self::glft_heat_prealign_extra_ticks(glft_heat_score);
                let prealign_trusted_ticks = prealign_trusted_ticks + prealign_extra_ticks;
                let prealign_target_ticks = prealign_target_ticks + prealign_extra_ticks;
                let hard_forced_realign = distance_to_trusted_mid_ticks
                    .zip(distance_to_normalized_target_ticks)
                    .map(|(trusted_dist, target_dist)| trusted_dist > 16.0 && target_dist > 10.0)
                    .unwrap_or(false);
                let prealign_required = distance_to_trusted_mid_ticks
                    .zip(distance_to_normalized_target_ticks)
                    .map(|(trusted_dist, target_dist)| {
                        trusted_dist > prealign_trusted_ticks && target_dist > prealign_target_ticks
                    })
                    .unwrap_or(false);
                let prealign_escalated = distance_to_trusted_mid_ticks
                    .zip(distance_to_normalized_target_ticks)
                    .map(|(trusted_dist, target_dist)| {
                        trusted_dist > prealign_trusted_ticks + 2.0
                            || target_dist > prealign_target_ticks + 2.0
                    })
                    .unwrap_or(false);
                let price_move_ticks = match glft_drift_mode {
                    Some(crate::polymarket::glft::DriftMode::Damped) => 5.0,
                    Some(crate::polymarket::glft::DriftMode::Frozen) => 6.0,
                    Some(crate::polymarket::glft::DriftMode::Paused) => 7.0,
                    _ => 5.0,
                } + Self::glft_heat_price_move_extra_ticks(glft_heat_score);
                let price_move_threshold = price_move_ticks * tick;
                let price_move_min_age =
                    Self::glft_price_move_min_age(glft_drift_mode, glft_heat_score);
                let prealign_min_age = Self::glft_prealign_min_age(glft_drift_mode);
                let unsafe_min_age = Self::glft_unsafe_publish_min_age(glft_drift_mode);
                let forced_realign_min_age =
                    Self::glft_forced_realign_min_age(glft_drift_mode, glft_heat_score);
                let unsafe_moderate_ticks =
                    Self::glft_unsafe_moderate_ticks(glft_drift_mode, glft_heat_score);
                let mut attempted_directional_publish = false;
                publish_reason = if recent_cross_reject {
                    publish_hard_safety_exception = true;
                    Some(SlotPublishReason::CrossRejectRecovery)
                } else if hard_forced_realign {
                    publish_hard_safety_exception = true;
                    Some(SlotPublishReason::ForcedRealign)
                } else if debt_forced_realign && elapsed >= forced_realign_min_age {
                    attempted_directional_publish = true;
                    match Self::slot_price_move_direction(slot_price, normalized_target_price) {
                        Some(direction) if self.confirm_slot_price_move(slot, direction) => {
                            debt_realign_selected = true;
                            Some(SlotPublishReason::ForcedRealign)
                        }
                        _ => None,
                    }
                } else if prealign_required {
                    let can_try_prealign = prealign_escalated || elapsed >= prealign_min_age;
                    if can_try_prealign {
                        attempted_directional_publish = true;
                        match Self::slot_price_move_direction(slot_price, normalized_target_price) {
                            Some(direction) if self.confirm_slot_price_move(slot, direction) => {
                                Some(SlotPublishReason::DriftPrealign)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else if unsafe_current_quote && unsafe_needs_publish {
                    let severe_unsafe = unsafe_depth_ticks
                        .map(|depth_ticks| !depth_ticks.is_finite() || depth_ticks >= 4.0)
                        .unwrap_or(false);
                    let moderate_unsafe = unsafe_depth_ticks
                        .map(|depth_ticks| {
                            depth_ticks.is_finite() && depth_ticks >= unsafe_moderate_ticks
                        })
                        .unwrap_or(false);
                    if severe_unsafe
                        && unsafe_depth_ticks
                            .map(|depth_ticks| !depth_ticks.is_finite())
                            .unwrap_or(false)
                    {
                        publish_hard_safety_exception = true;
                        Some(SlotPublishReason::UnsafeQuote)
                    } else if severe_unsafe && elapsed >= unsafe_min_age / 2 {
                        publish_hard_safety_exception = true;
                        Some(SlotPublishReason::UnsafeQuote)
                    } else if moderate_unsafe
                        && elapsed >= unsafe_min_age
                        && shadow_age >= shadow_publish_dwell
                    {
                        attempted_directional_publish = true;
                        match Self::slot_price_move_direction(slot_price, normalized_target_price) {
                            Some(direction) if self.confirm_slot_price_move(slot, direction) => {
                                Some(SlotPublishReason::UnsafeQuote)
                            }
                            _ if shadow_age
                                >= unsafe_min_age.saturating_add(shadow_publish_dwell) =>
                            {
                                Some(SlotPublishReason::UnsafeQuote)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else if size_gap > 0.1 {
                    Some(SlotPublishReason::SizeMove)
                } else if price_gap >= price_move_threshold {
                    if elapsed >= price_move_min_age {
                        attempted_directional_publish = true;
                        match Self::slot_price_move_direction(slot_price, normalized_target_price) {
                            Some(direction) if self.confirm_slot_price_move(slot, direction) => {
                                Some(SlotPublishReason::PriceMove)
                            }
                            _ if shadow_age >= shadow_publish_dwell => {
                                Some(SlotPublishReason::ShadowDwell)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else if shadow_age >= shadow_publish_dwell {
                    Some(SlotPublishReason::ShadowDwell)
                } else {
                    None
                };
                if !attempted_directional_publish {
                    self.reset_slot_price_move_confirmation(slot);
                }
                if let Some(reason_kind) = publish_reason {
                    let cooldown = Self::glft_publish_reason_cooldown(
                        reason_kind,
                        glft_drift_mode,
                        glft_heat_score,
                    );
                    if cooldown > std::time::Duration::ZERO && elapsed < cooldown {
                        publish_reason = None;
                        debt_realign_selected = false;
                        publish_hard_safety_exception = false;
                    }
                }
                if publish_reason.is_some() {
                    self.reset_slot_price_move_confirmation(slot);
                }
            }
            if needs_reprice {
                if glft_shadow_mode && publish_reason.is_none() {
                    self.stats.shadow_suppressed_updates += 1;
                    return;
                }
                if glft_shadow_mode {
                    if let Some(reason_kind) = publish_reason {
                        let bypass_budget = publish_hard_safety_exception
                            || Self::glft_publish_reason_is_emergency(reason_kind);
                        let budget_cost = Self::glft_publish_reason_budget_cost(
                            reason_kind,
                            glft_drift_mode,
                            glft_heat_score,
                        );
                        if !bypass_budget
                            && !self.consume_slot_publish_budget(
                                slot,
                                glft_drift_mode,
                                glft_heat_score,
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
                self.record_publish_reason_stats(publish_reason, debt_realign_selected);
                self.emit_quote_log(
                    slot,
                    price,
                    size,
                    reason,
                    log_msg.as_deref(),
                    Some(raw_target_price),
                    Some(normalized_target_price),
                    publish_reason,
                    distance_to_trusted_mid_ticks,
                    distance_to_normalized_target_ticks,
                );
                self.slot_last_publish_reason[slot.index()] = publish_reason;
                debug!(
                    "🔄 reprice {:?} {:?} {:.3}→{:.3} sz={:.1} band={:.3} publish_reason={:?}",
                    slot.side,
                    slot.direction,
                    slot_price,
                    price,
                    size,
                    reprice_band,
                    publish_reason
                );
                self.stats.cancel_reprice += 1;
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
        self.reset_slot_price_move_confirmation(slot);
        match slot {
            OrderSlot::YES_BUY => self.yes_target = None,
            OrderSlot::NO_BUY => self.no_target = None,
            _ => {}
        }
        self.sync_buy_side_wrapper(slot);

        debug!("🗑️ Cancel {} ({:?})", slot.as_str(), reason);
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

        self.slot_targets[slot.index()] = Some(target.clone());
        self.slot_last_ts[slot.index()] = Instant::now();
        self.reset_slot_realign_debt(slot);
        self.reset_slot_price_move_confirmation(slot);
        self.sync_buy_side_wrapper(slot);

        self.stats.placed += 1;

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
        distance_to_trusted_mid_ticks: Option<f64>,
        distance_to_normalized_target_ticks: Option<f64>,
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
            let spread_mult = (1.0 + self.cfg.glft_ofi_spread_beta * glft.alpha_flow.abs().powi(2))
                * (1.0 + 0.15 * (heat_score - 1.0).max(0.0).min(4.0));
            let half_spread = (offsets.half_spread_base * spread_mult).max(self.cfg.tick_size);
            let r_yes = (p_anchor + alpha_prob - offsets.inventory_shift)
                .clamp(self.cfg.tick_size, 1.0 - self.cfg.tick_size);
            let shaped = shape_glft_quotes(
                r_yes,
                half_spread,
                glft.drift_mode,
                glft.basis_drift_ticks,
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
                "📐 GLFT {} {:?}@{:.3} sz={:.1} | source={} fit={:?}/{:?} state={:?} drift_mode={:?} drift_ticks={:.1} anchor={:.3} basis={:.3} basis_raw={:.3} basis_clamped={:.3} modeled_mid={:.3} trusted_mid={:.3} synthetic_mid={:.3} stale_secs={:.2} alpha={:.3} heat={:.2} sigma={:.7} a={:.3} k={:.3} q_norm={:.3} inv_shift={:.6} half_base={:.4} spread_mult={:.3} dominant_side={:?} dominant_buy_penalty_ticks={:.1} dominant_buy_suppressed={} r_yes_pre={:.3} r_yes_post={:.3} raw_target={:.3} normalized_target={:.3} shadow_target={:.3} publish_target={:.3} publish_reason={} dist_trusted_ticks={:.1} dist_target_ticks={:.1} pre[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3} final[yb/ys/nb/ns]={:.3}/{:.3}/{:.3}/{:.3}",
                slot.as_str(),
                slot.direction,
                price,
                size,
                fit_source,
                glft.fit_quality,
                if glft.ready && !glft.stale { "live" } else { "stale" },
                glft.signal_state,
                glft.drift_mode,
                glft.basis_drift_ticks,
                glft.anchor_prob,
                glft.basis_prob,
                glft.basis_raw,
                glft.basis_clamped,
                glft.modeled_mid,
                glft.trusted_mid,
                glft.synthetic_mid_yes,
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
                distance_to_trusted_mid_ticks.unwrap_or(0.0),
                distance_to_normalized_target_ticks.unwrap_or(0.0),
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

    fn glft_buy_corridor_ticks(drift_mode: crate::polymarket::glft::DriftMode) -> f64 {
        match drift_mode {
            crate::polymarket::glft::DriftMode::Normal => 8.0,
            crate::polymarket::glft::DriftMode::Damped => 7.0,
            crate::polymarket::glft::DriftMode::Frozen => 6.0,
            crate::polymarket::glft::DriftMode::Paused => 5.0,
        }
    }

    fn glft_heat_publish_level(heat_score: f64) -> u8 {
        if !heat_score.is_finite() {
            return 0;
        }
        if heat_score >= 8.0 {
            3
        } else if heat_score >= 4.0 {
            2
        } else if heat_score >= 2.0 {
            1
        } else {
            0
        }
    }

    fn glft_heat_price_move_extra_ticks(heat_score: f64) -> f64 {
        match Self::glft_heat_publish_level(heat_score) {
            0 => 0.0,
            1 => 1.0,
            2 => 2.0,
            _ => 3.0,
        }
    }

    fn glft_heat_prealign_extra_ticks(heat_score: f64) -> f64 {
        match Self::glft_heat_publish_level(heat_score) {
            0 | 1 => 0.0,
            2 => 1.0,
            _ => 2.0,
        }
    }

    fn glft_shadow_publish_dwell(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> std::time::Duration {
        let base = match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => {
                std::time::Duration::from_millis(1400)
            }
            Some(crate::polymarket::glft::DriftMode::Frozen) => {
                std::time::Duration::from_millis(1700)
            }
            Some(crate::polymarket::glft::DriftMode::Paused) => {
                std::time::Duration::from_millis(2000)
            }
            _ => std::time::Duration::from_millis(1200),
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => std::time::Duration::ZERO,
            1 => std::time::Duration::from_millis(180),
            2 => std::time::Duration::from_millis(320),
            _ => std::time::Duration::from_millis(500),
        };
        base.saturating_add(extra)
    }

    fn glft_prealign_min_age(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
    ) -> std::time::Duration {
        match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => {
                std::time::Duration::from_millis(1400)
            }
            Some(crate::polymarket::glft::DriftMode::Frozen) => {
                std::time::Duration::from_millis(1700)
            }
            Some(crate::polymarket::glft::DriftMode::Paused) => {
                std::time::Duration::from_millis(2000)
            }
            _ => std::time::Duration::from_millis(1200),
        }
    }

    fn glft_unsafe_publish_min_age(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
    ) -> std::time::Duration {
        match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => {
                std::time::Duration::from_millis(1600)
            }
            Some(crate::polymarket::glft::DriftMode::Frozen) => {
                std::time::Duration::from_millis(1900)
            }
            Some(crate::polymarket::glft::DriftMode::Paused) => {
                std::time::Duration::from_millis(2200)
            }
            _ => std::time::Duration::from_millis(1400),
        }
    }

    fn glft_forced_realign_min_age(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> std::time::Duration {
        let base = match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => {
                std::time::Duration::from_millis(1200)
            }
            Some(crate::polymarket::glft::DriftMode::Frozen) => {
                std::time::Duration::from_millis(1500)
            }
            Some(crate::polymarket::glft::DriftMode::Paused) => {
                std::time::Duration::from_millis(1800)
            }
            _ => std::time::Duration::from_millis(900),
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => std::time::Duration::ZERO,
            1 => std::time::Duration::from_millis(120),
            2 => std::time::Duration::from_millis(220),
            _ => std::time::Duration::from_millis(320),
        };
        base.saturating_add(extra)
    }

    fn glft_publish_reason_cooldown(
        reason: SlotPublishReason,
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> std::time::Duration {
        let base = match reason {
            SlotPublishReason::DriftPrealign => match drift_mode {
                Some(crate::polymarket::glft::DriftMode::Damped) => {
                    std::time::Duration::from_millis(1600)
                }
                Some(crate::polymarket::glft::DriftMode::Frozen) => {
                    std::time::Duration::from_millis(1800)
                }
                Some(crate::polymarket::glft::DriftMode::Paused) => {
                    std::time::Duration::from_millis(2000)
                }
                _ => std::time::Duration::from_millis(1400),
            },
            SlotPublishReason::ShadowDwell => match drift_mode {
                Some(crate::polymarket::glft::DriftMode::Damped) => {
                    std::time::Duration::from_millis(1800)
                }
                Some(crate::polymarket::glft::DriftMode::Frozen) => {
                    std::time::Duration::from_millis(2000)
                }
                Some(crate::polymarket::glft::DriftMode::Paused) => {
                    std::time::Duration::from_millis(2200)
                }
                _ => std::time::Duration::from_millis(1600),
            },
            SlotPublishReason::PriceMove => match drift_mode {
                Some(crate::polymarket::glft::DriftMode::Damped) => {
                    std::time::Duration::from_millis(1600)
                }
                Some(crate::polymarket::glft::DriftMode::Frozen) => {
                    std::time::Duration::from_millis(1800)
                }
                Some(crate::polymarket::glft::DriftMode::Paused) => {
                    std::time::Duration::from_millis(2000)
                }
                _ => std::time::Duration::from_millis(1600),
            },
            SlotPublishReason::UnsafeQuote => match drift_mode {
                Some(crate::polymarket::glft::DriftMode::Damped) => {
                    std::time::Duration::from_millis(1500)
                }
                Some(crate::polymarket::glft::DriftMode::Frozen) => {
                    std::time::Duration::from_millis(1800)
                }
                Some(crate::polymarket::glft::DriftMode::Paused) => {
                    std::time::Duration::from_millis(2000)
                }
                _ => std::time::Duration::from_millis(1200),
            },
            SlotPublishReason::ForcedRealign => match drift_mode {
                Some(crate::polymarket::glft::DriftMode::Damped) => {
                    std::time::Duration::from_millis(1800)
                }
                Some(crate::polymarket::glft::DriftMode::Frozen) => {
                    std::time::Duration::from_millis(2100)
                }
                Some(crate::polymarket::glft::DriftMode::Paused) => {
                    std::time::Duration::from_millis(2400)
                }
                _ => std::time::Duration::from_millis(1500),
            },
            _ => std::time::Duration::ZERO,
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => std::time::Duration::ZERO,
            1 => std::time::Duration::from_millis(140),
            2 => std::time::Duration::from_millis(260),
            _ => std::time::Duration::from_millis(420),
        };
        base.saturating_add(extra)
    }

    fn record_publish_reason_stats(
        &mut self,
        publish_reason: Option<SlotPublishReason>,
        debt_realign_selected: bool,
    ) {
        if matches!(publish_reason, Some(SlotPublishReason::ForcedRealign)) {
            self.stats.forced_realign_count = self.stats.forced_realign_count.saturating_add(1);
            if debt_realign_selected {
                self.stats.debt_realign_triggers =
                    self.stats.debt_realign_triggers.saturating_add(1);
            } else {
                self.stats.forced_realign_hard_count =
                    self.stats.forced_realign_hard_count.saturating_add(1);
            }
        }
    }

    fn glft_price_move_min_age(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> std::time::Duration {
        let base = match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => {
                std::time::Duration::from_millis(1000)
            }
            Some(crate::polymarket::glft::DriftMode::Frozen) => {
                std::time::Duration::from_millis(1200)
            }
            Some(crate::polymarket::glft::DriftMode::Paused) => {
                std::time::Duration::from_millis(1500)
            }
            _ => std::time::Duration::from_millis(900),
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => std::time::Duration::ZERO,
            1 => std::time::Duration::from_millis(100),
            2 => std::time::Duration::from_millis(180),
            _ => std::time::Duration::from_millis(280),
        };
        base.saturating_add(extra)
    }

    fn glft_unsafe_distance_thresholds(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> (f64, f64) {
        let (base_trusted, base_target) = match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => (10.0, 6.5),
            Some(crate::polymarket::glft::DriftMode::Frozen) => (9.0, 6.0),
            Some(crate::polymarket::glft::DriftMode::Paused) => (8.0, 5.0),
            _ => (11.0, 7.0),
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => 0.0,
            1 => 0.5,
            2 => 1.0,
            _ => 1.5,
        };
        (base_trusted + extra, base_target + extra)
    }

    fn glft_unsafe_moderate_ticks(
        drift_mode: Option<crate::polymarket::glft::DriftMode>,
        heat_score: f64,
    ) -> f64 {
        let base = match drift_mode {
            Some(crate::polymarket::glft::DriftMode::Damped) => 2.5,
            Some(crate::polymarket::glft::DriftMode::Frozen) => 2.5,
            Some(crate::polymarket::glft::DriftMode::Paused) => 2.0,
            _ => 3.0,
        };
        let extra = match Self::glft_heat_publish_level(heat_score) {
            0 => 0.0,
            1 => 0.25,
            2 => 0.5,
            _ => 0.75,
        };
        base + extra
    }

    fn glft_clamp_slot_target_price(&self, slot: OrderSlot, price: f64) -> f64 {
        if !(price > 0.0) || slot.direction != TradeDirection::Buy {
            return price;
        }
        let glft = *self.glft_rx.borrow();
        let tick = self.cfg.tick_size.max(1e-9);
        let max_dev = Self::glft_buy_corridor_ticks(glft.drift_mode) * tick;
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

    fn slot_price_move_direction(from_price: f64, to_price: f64) -> Option<PriceMoveDirection> {
        if to_price > from_price + 1e-9 {
            Some(PriceMoveDirection::Up)
        } else if to_price + 1e-9 < from_price {
            Some(PriceMoveDirection::Down)
        } else {
            None
        }
    }

    pub(super) fn confirm_slot_price_move(
        &mut self,
        slot: OrderSlot,
        direction: PriceMoveDirection,
    ) -> bool {
        self.confirm_slot_price_move_at(slot, direction, Instant::now())
    }

    pub(super) fn confirm_slot_price_move_at(
        &mut self,
        slot: OrderSlot,
        direction: PriceMoveDirection,
        now: Instant,
    ) -> bool {
        const REQUIRED_STREAK: u8 = 3;
        const REQUIRED_HOLD_MS: u64 = 600;
        let idx = slot.index();
        let same_direction = self.slot_price_move_dir[idx] == Some(direction);
        let next_streak = if same_direction {
            self.slot_price_move_streak[idx]
                .saturating_add(1)
                .min(REQUIRED_STREAK)
        } else {
            1
        };
        self.slot_price_move_dir[idx] = Some(direction);
        self.slot_price_move_streak[idx] = next_streak;
        if !same_direction || self.slot_price_move_since[idx].is_none() {
            self.slot_price_move_since[idx] = Some(now);
        }
        let held_long_enough = self.slot_price_move_since[idx]
            .map(|since| {
                now.saturating_duration_since(since).as_millis() >= REQUIRED_HOLD_MS as u128
            })
            .unwrap_or(false);
        next_streak >= REQUIRED_STREAK && held_long_enough
    }

    pub(super) fn reset_slot_price_move_confirmation(&mut self, slot: OrderSlot) {
        let idx = slot.index();
        self.slot_price_move_dir[idx] = None;
        self.slot_price_move_streak[idx] = 0;
        self.slot_price_move_since[idx] = None;
    }

    fn slot_relative_misalignment(
        &self,
        slot: OrderSlot,
        quoted_price: f64,
        normalized_target_price: f64,
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
        let distance_to_normalized_target_ticks =
            ((quoted_price - normalized_target_price).abs()) / tick;
        let force_realign =
            distance_to_trusted_mid_ticks > 16.0 && distance_to_normalized_target_ticks > 10.0;
        Some((
            distance_to_trusted_mid_ticks,
            distance_to_normalized_target_ticks,
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
