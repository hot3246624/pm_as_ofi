use tracing::debug;

use super::*;

impl StrategyCoordinator {
    // ═════════════════════════════════════════════════
    // Pricing engine
    // ═════════════════════════════════════════════════

    // ═════════════════════════════════════════════════
    // Opt-1: A-S Time Decay Factor
    // ═════════════════════════════════════════════════

    /// Returns a multiplier for `as_skew_factor` that grows linearly from 1.0
    /// at market open to `(1 + as_time_decay_k)` at market close.
    ///
    /// Formula: `1.0 + k * elapsed_fraction`
    /// where `elapsed_fraction = elapsed / total_duration`, clamped to [0, 1].
    ///
    /// With default k=2.0: the factor ranges from 1× at open to 3× at close.
    /// This matches the A-S model's γσ²(T-t) term — as T-t → 0 the urgency to
    /// close inventory increases, expressed here as a growing skew penalty.
    pub(crate) fn compute_time_decay_factor(&self) -> f64 {
        let k = self.cfg.as_time_decay_k;
        if k <= 0.0 {
            return 1.0;
        }
        let Some(end_ts) = self.cfg.market_end_ts else {
            return 1.0;
        };
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_secs >= end_ts {
            return 1.0 + k; // Market over — max urgency
        }
        // Total window = from bot start (market_start) to end_ts.
        // Use wall-clock elapsed since we need absolute time to end_ts.
        let elapsed = self.market_start.elapsed().as_secs_f64();
        let remaining = (end_ts - now_secs) as f64;
        let total = elapsed + remaining;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed_frac = (elapsed / total).clamp(0.0, 1.0);
        1.0 + k * elapsed_frac
    }

    /// Step hedge ceiling: pair_target within normal risk, max_portfolio_cost only at/over max_net_diff.
    pub(crate) fn hedge_target(&self, net_diff: f64) -> f64 {
        let pair = self.cfg.pair_target;
        let max_cost = self.cfg.max_portfolio_cost;
        if max_cost <= pair || self.cfg.max_net_diff <= f64::EPSILON {
            return pair;
        }
        if net_diff.abs() >= self.cfg.max_net_diff {
            max_cost
        } else {
            pair
        }
    }

    /// Compute the maximum acceptable incremental hedge price on the missing side,
    /// respecting the post-hedge target combined cost.
    ///
    /// Key idea: when we already hold inventory on the hedge side, we should use
    /// *incremental* budget instead of `target - avg_held_side` static subtraction.
    /// This avoids systematically underpricing hedges when side quantities are uneven.
    pub(crate) fn incremental_hedge_ceiling(
        &self,
        inv: &InventoryState,
        hedge_side: Side,
        hedge_size: f64,
        hedge_target: f64,
    ) -> f64 {
        if hedge_size <= f64::EPSILON {
            return 0.0;
        }

        match hedge_side {
            Side::No => {
                let q = inv.no_qty.max(0.0);
                let target_no_avg = hedge_target - inv.yes_avg_cost;
                if target_no_avg <= 0.0 {
                    return 0.0;
                }
                if q <= f64::EPSILON {
                    return target_no_avg;
                }
                let existing_cost = q * inv.no_avg_cost.max(0.0);
                let total_allowed = target_no_avg * (q + hedge_size);
                let incremental_allowed = total_allowed - existing_cost;
                if incremental_allowed <= 0.0 {
                    0.0
                } else {
                    incremental_allowed / hedge_size
                }
            }
            Side::Yes => {
                let q = inv.yes_qty.max(0.0);
                let target_yes_avg = hedge_target - inv.no_avg_cost;
                if target_yes_avg <= 0.0 {
                    return 0.0;
                }
                if q <= f64::EPSILON {
                    return target_yes_avg;
                }
                let existing_cost = q * inv.yes_avg_cost.max(0.0);
                let total_allowed = target_yes_avg * (q + hedge_size);
                let incremental_allowed = total_allowed - existing_cost;
                if incremental_allowed <= 0.0 {
                    0.0
                } else {
                    incremental_allowed / hedge_size
                }
            }
        }
    }

    /// Determine hedge size with minimum size constraints.
    /// Returns None if hedge should be skipped.
    pub(super) fn hedge_size_from_net(&self, net_diff: f64) -> Option<f64> {
        let raw = net_diff.abs();
        if raw <= f64::EPSILON {
            return None;
        }
        let min_hedge = self.cfg.min_hedge_size.max(0.0);
        if min_hedge > 0.0 && raw + 1e-9 < min_hedge {
            return None;
        }
        let min_order = self.cfg.min_order_size.max(0.0);
        if min_order > 0.0 && raw + 1e-9 < min_order {
            if self.cfg.hedge_round_up {
                return Some(min_order);
            }
            return None;
        }
        Some(raw)
    }

    /// Optional hedge-size bump to satisfy venue marketable-BUY minimum notional.
    ///
    /// Returns `Some(new_size)` only when bump is enabled and within configured
    /// extra-size caps; otherwise returns `None` and caller keeps original size.
    pub(super) fn bump_hedge_size_for_marketable_floor(
        &self,
        price: f64,
        size: f64,
    ) -> Option<f64> {
        let min_notional = self.cfg.hedge_min_marketable_notional;
        if min_notional <= 0.0 || price <= 0.0 || size <= 0.0 {
            return None;
        }
        let required = ((min_notional / price) * 100.0).ceil() / 100.0;
        if required <= size + 1e-9 {
            return None;
        }
        let extra = required - size;
        let max_extra_abs = self.cfg.hedge_min_marketable_max_extra.max(0.0);
        let max_extra_pct = (size * self.cfg.hedge_min_marketable_max_extra_pct.max(0.0)).max(0.0);
        if extra <= max_extra_abs + 1e-9 && extra <= max_extra_pct + 1e-9 {
            Some(required)
        } else {
            debug!(
                "🧩 Hedge min-notional bump rejected: price={:.3} size={:.2} -> req={:.2} (extra={:.2} > caps abs={:.2} pct={:.2})",
                price, size, required, extra, max_extra_abs, max_extra_pct
            );
            None
        }
    }

    pub(super) fn slot_target(&self, slot: OrderSlot) -> Option<&DesiredTarget> {
        self.slot_targets[slot.index()]
            .as_ref()
            .or_else(|| match slot {
                OrderSlot::YES_BUY => self.yes_target.as_ref(),
                OrderSlot::NO_BUY => self.no_target.as_ref(),
                _ => None,
            })
    }

    pub(super) fn slot_target_active(&self, slot: OrderSlot) -> bool {
        self.slot_target(slot).is_some()
    }

    pub(super) fn slot_shadow_target(&self, slot: OrderSlot) -> Option<&DesiredTarget> {
        self.slot_shadow_targets[slot.index()].as_ref()
    }

    pub(super) fn slot_last_ts(&self, slot: OrderSlot) -> Instant {
        match slot {
            OrderSlot::YES_BUY if self.slot_targets[slot.index()].is_none() => self.yes_last_ts,
            OrderSlot::NO_BUY if self.slot_targets[slot.index()].is_none() => self.no_last_ts,
            _ => self.slot_last_ts[slot.index()],
        }
    }

    pub(super) fn slot_shadow_since(&self, slot: OrderSlot) -> Option<Instant> {
        self.slot_shadow_since[slot.index()]
    }

    pub(super) fn slot_publish_reason(&self, slot: OrderSlot) -> Option<PolicyPublishCause> {
        self.slot_last_publish_reason[slot.index()]
    }

    pub(super) fn soft_reset_slot_publish_state(&mut self, slot: OrderSlot) {
        let idx = slot.index();
        self.slot_publish_budget[idx] = Self::glft_publish_budget_cap();
        self.slot_last_budget_refill[idx] = std::time::Instant::now();
        self.slot_publish_debt_accum[idx] = 0.0;
        self.slot_last_debt_refill[idx] = std::time::Instant::now();
        self.slot_shadow_velocity_tps[idx] = 0.0;
        self.slot_shadow_last_change_ts[idx] = None;
        self.slot_last_policy_transition[idx] = None;
        self.slot_absent_clear_since[idx] = None;
        self.stats.soft_reset_count = self.stats.soft_reset_count.saturating_add(1);
    }

    pub(super) fn full_reset_slot_publish_state(&mut self, slot: OrderSlot) {
        let idx = slot.index();
        self.soft_reset_slot_publish_state(slot);
        self.slot_policy_candidates[idx] = None;
        self.slot_policy_candidate_since[idx] = None;
        self.slot_policy_states[idx] = None;
        self.slot_policy_since[idx] = None;
        self.slot_last_regime_seen[idx] = None;
        self.slot_regime_changed_at[idx] = std::time::Instant::now();
        self.stats.full_reset_count = self.stats.full_reset_count.saturating_add(1);
    }

    pub(super) fn default_slot_reset_scope(&self, reason: CancelReason) -> SlotResetScope {
        if self.cfg.strategy != StrategyKind::GlftMm {
            return SlotResetScope::Full;
        }
        match reason {
            CancelReason::Reprice => SlotResetScope::Soft,
            CancelReason::StaleData
            | CancelReason::ToxicFlow
            | CancelReason::InventoryLimit
            | CancelReason::EndgameRiskGate
            | CancelReason::Shutdown
            | CancelReason::MarketExpired
            | CancelReason::Startup => SlotResetScope::Full,
        }
    }

    pub(super) fn update_slot_regime_state(
        &mut self,
        slot: OrderSlot,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        now: std::time::Instant,
    ) -> Option<std::time::Duration> {
        let regime = quote_regime?;
        let idx = slot.index();
        if self.slot_last_regime_seen[idx] != Some(regime) {
            self.slot_last_regime_seen[idx] = Some(regime);
            self.slot_regime_changed_at[idx] = now;
            return Some(std::time::Duration::ZERO);
        }
        Some(now.saturating_duration_since(self.slot_regime_changed_at[idx]))
    }

    pub(super) fn glft_regime_publish_settle_dwell(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> std::time::Duration {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => {
                std::time::Duration::from_millis(2_800)
            }
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => {
                std::time::Duration::from_millis(4_000)
            }
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => {
                std::time::Duration::from_millis(5_200)
            }
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => {
                std::time::Duration::from_millis(3_200)
            }
            None => std::time::Duration::from_millis(2_800),
        }
    }

    pub(super) fn consume_slot_publish_budget(
        &mut self,
        slot: OrderSlot,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        cost: f64,
        now: std::time::Instant,
    ) -> bool {
        self.refill_slot_publish_budget(slot, quote_regime, now);
        let idx = slot.index();
        let normalized_cost = if cost.is_finite() {
            cost.max(0.20)
        } else {
            1.0
        };
        if self.slot_publish_budget[idx] + 1e-9 >= normalized_cost {
            self.slot_publish_budget[idx] =
                (self.slot_publish_budget[idx] - normalized_cost).max(0.0);
            true
        } else {
            false
        }
    }

    pub(super) fn glft_publish_reason_is_abnormal(reason: PolicyPublishCause) -> bool {
        matches!(
            reason,
            PolicyPublishCause::Safety | PolicyPublishCause::Recovery
        )
    }

    pub(super) fn glft_publish_reason_budget_cost(
        reason: PolicyPublishCause,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        let base: f64 = match reason {
            PolicyPublishCause::Initial => 0.75,
            PolicyPublishCause::Policy => 0.95,
            PolicyPublishCause::Safety => 1.25,
            PolicyPublishCause::Recovery => 1.60,
        };
        let regime_mult: f64 = match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 1.08,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 1.14,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 1.20,
            _ => 1.0,
        };
        (base * regime_mult).clamp(0.6, 2.4)
    }

    fn refill_slot_publish_budget(
        &mut self,
        slot: OrderSlot,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        now: std::time::Instant,
    ) {
        let idx = slot.index();
        let last = self.slot_last_budget_refill[idx];
        let dt = now.saturating_duration_since(last).as_secs_f64().max(0.0);
        if dt <= f64::EPSILON {
            return;
        }
        self.slot_last_budget_refill[idx] = now;
        let refill_rate = Self::glft_publish_budget_refill_per_sec(quote_regime);
        let cap = Self::glft_publish_budget_cap();
        let next = self.slot_publish_budget[idx] + dt * refill_rate;
        self.slot_publish_budget[idx] = next.min(cap).max(0.0);
    }

    pub(super) fn update_slot_publish_debt_accumulator(
        &mut self,
        slot: OrderSlot,
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
        target_follow_debt_ticks: f64,
        structural_debt_ticks: f64,
        now: std::time::Instant,
    ) -> f64 {
        let idx = slot.index();
        let last = self.slot_last_debt_refill[idx];
        let dt = now.saturating_duration_since(last).as_secs_f64().max(0.0);
        self.slot_last_debt_refill[idx] = now;

        if dt > f64::EPSILON {
            let decay = Self::glft_publish_debt_decay_per_sec(quote_regime) * dt;
            self.slot_publish_debt_accum[idx] =
                (self.slot_publish_debt_accum[idx] - decay).max(0.0);
        }

        let target_threshold = Self::glft_publish_target_debt_threshold(quote_regime);
        let structural_threshold = Self::glft_publish_structural_debt_threshold(quote_regime);
        let target_clear_floor = target_threshold * 0.50;
        let structural_clear_floor = structural_threshold * 0.50;

        // Architecture intent:
        // - Direct debt thresholds drive immediate publish.
        // - Accumulator only integrates *excess above thresholds* so it represents
        //   persistent unresolved misalignment, not normal 1-2 tick tracking noise.
        // - Once both debt sources cool to low-water bands, reset accumulator to avoid
        //   stale debt re-triggering publishes after market has already re-aligned.
        if target_follow_debt_ticks <= target_clear_floor
            && structural_debt_ticks <= structural_clear_floor
        {
            self.slot_publish_debt_accum[idx] = 0.0;
            return 0.0;
        }

        let target_excess = (target_follow_debt_ticks.max(0.0) - target_threshold).max(0.0);
        let structural_excess = (structural_debt_ticks.max(0.0) - structural_threshold).max(0.0);
        if dt > f64::EPSILON && (target_excess > 0.0 || structural_excess > 0.0) {
            let integrated = Self::glft_publish_debt_gain_per_sec()
                * dt
                * (0.60 * target_excess + 1.20 * structural_excess);
            self.slot_publish_debt_accum[idx] =
                (self.slot_publish_debt_accum[idx] + integrated).min(Self::glft_publish_debt_cap());
        }

        self.slot_publish_debt_accum[idx]
    }

    pub(super) fn consume_slot_publish_debt_release(
        &mut self,
        slot: OrderSlot,
        _quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) {
        let idx = slot.index();
        // Publish cycle should settle accumulated debt decisively; partial subtraction
        // tends to create repetitive near-periodic debt publishes in trends.
        self.slot_publish_debt_accum[idx] = 0.0;
        self.slot_last_debt_refill[idx] = std::time::Instant::now();
    }

    fn glft_publish_debt_gain_per_sec() -> f64 {
        1.2
    }

    fn glft_publish_debt_decay_per_sec(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 3.0,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 2.2,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 1.6,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 1.0,
            None => 3.0,
        }
    }

    pub(super) fn glft_publish_debt_release_threshold(
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

    fn glft_publish_debt_cap() -> f64 {
        14.0
    }

    fn glft_publish_budget_cap() -> f64 {
        1.6
    }

    fn glft_publish_budget_refill_per_sec(
        quote_regime: Option<crate::polymarket::glft::QuoteRegime>,
    ) -> f64 {
        let rate: f64 = match quote_regime {
            Some(crate::polymarket::glft::QuoteRegime::Aligned) => 0.90,
            Some(crate::polymarket::glft::QuoteRegime::Tracking) => 0.75,
            Some(crate::polymarket::glft::QuoteRegime::Guarded) => 0.60,
            Some(crate::polymarket::glft::QuoteRegime::Blocked) => 0.50,
            None => 0.90,
        };
        rate.max(0.20)
    }

    pub(super) fn side_target_reason(&self, side: Side) -> Option<BidReason> {
        self.slot_target(OrderSlot::new(side, TradeDirection::Buy))
            .map(|t| t.reason)
    }

    pub(super) fn side_target(&self, side: Side) -> Option<&DesiredTarget> {
        self.slot_target(OrderSlot::new(side, TradeDirection::Buy))
    }

    pub(crate) fn can_place_strategy_intent(
        &self,
        inv: &InventoryState,
        intent: Option<StrategyIntent>,
    ) -> bool {
        match intent {
            Some(intent) => match (intent.side, intent.direction) {
                (Side::Yes, TradeDirection::Buy) => {
                    self.can_buy_yes(inv, intent.size)
                        && self.passes_outcome_floor_for_buy(
                            inv,
                            Side::Yes,
                            intent.size,
                            intent.price,
                            intent.reason,
                        )
                        && (self.cfg.strategy != StrategyKind::GlftMm
                            || self.passes_pair_cost_guard_for_buy(
                                inv,
                                Side::Yes,
                                intent.size,
                                intent.price,
                                intent.reason,
                            ))
                }
                (Side::No, TradeDirection::Buy) => {
                    self.can_buy_no(inv, intent.size)
                        && self.passes_outcome_floor_for_buy(
                            inv,
                            Side::No,
                            intent.size,
                            intent.price,
                            intent.reason,
                        )
                        && (self.cfg.strategy != StrategyKind::GlftMm
                            || self.passes_pair_cost_guard_for_buy(
                                inv,
                                Side::No,
                                intent.size,
                                intent.price,
                                intent.reason,
                            ))
                }
                (Side::Yes, TradeDirection::Sell) => self.can_sell_yes(inv, intent.size),
                (Side::No, TradeDirection::Sell) => self.can_sell_no(inv, intent.size),
            },
            None => true,
        }
    }

    pub(super) fn should_clear_on_toxic(&self, side: Side) -> bool {
        matches!(self.side_target_reason(side), Some(BidReason::Provide))
    }

    pub(super) fn can_buy_yes(&self, inv: &InventoryState, size: f64) -> bool {
        inv.net_diff + size <= self.cfg.max_net_diff + 1e-4
    }

    pub(super) fn can_buy_no(&self, inv: &InventoryState, size: f64) -> bool {
        inv.net_diff - size >= -self.cfg.max_net_diff - 1e-4
    }

    pub(super) fn can_sell_yes(&self, inv: &InventoryState, size: f64) -> bool {
        size > 0.0 && inv.yes_qty + 1e-4 >= size
    }

    pub(super) fn can_sell_no(&self, inv: &InventoryState, size: f64) -> bool {
        size > 0.0 && inv.no_qty + 1e-4 >= size
    }

    pub(super) fn can_hedge_buy_yes(&self, inv: &InventoryState, size: f64) -> bool {
        inv.net_diff + size <= self.cfg.max_net_diff + 1e-4
    }

    pub(super) fn can_hedge_buy_no(&self, inv: &InventoryState, size: f64) -> bool {
        inv.net_diff - size >= -self.cfg.max_net_diff - 1e-4
    }

    pub(crate) fn post_only_safety_margin_for(
        &self,
        side: Side,
        best_bid: f64,
        best_ask: f64,
    ) -> f64 {
        let mut margin_ticks = self.cfg.post_only_safety_ticks.max(0.5);
        if best_bid > 0.0 && best_ask > best_bid {
            let spread_ticks = (best_ask - best_bid) / self.cfg.tick_size.max(1e-9);
            if spread_ticks <= self.cfg.post_only_tight_spread_ticks {
                margin_ticks += self.cfg.post_only_extra_tight_ticks.max(0.0);
            }
        }
        if self.cfg.strategy == StrategyKind::GlftMm {
            // GLFT updates all four slots at high cadence; keep one extra safety tick
            // to reduce repeated post-only cross rejects under fast book flicker.
            margin_ticks += 1.0;
        }
        margin_ticks += f64::from(self.maker_friction(side).extra_safety_ticks);
        margin_ticks * self.cfg.tick_size.max(1e-9)
    }

    #[allow(dead_code)]
    pub(crate) fn post_only_safety_margin(&self, best_bid: f64, best_ask: f64) -> f64 {
        self.post_only_safety_margin_for(Side::Yes, best_bid, best_ask)
    }

    /// Aggressive Maker price: min(ceiling, best_ask - safety_margin).
    ///
    /// CRITICAL: If best_ask is unavailable (empty book), return 0.0.
    /// NEVER fall back to ceiling — that caused the phantom 0.490 oscillation.
    /// Bidding at ceiling when no ask exists = paying maximum price into a void.
    pub(crate) fn aggressive_price_for(
        &self,
        side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> f64 {
        if ceiling <= 0.0 || ceiling >= 1.0 {
            return 0.0;
        }
        if best_ask <= 0.0 {
            // No sell-side liquidity — refuse to bid.
            // This prevents "Blind Crossing" where we bid into a stale/empty book.
            return 0.0;
        }

        if ceiling >= best_ask - 1e-9 {
            // The ceiling is at or above the current best ask.
            // b/c we are Post-Only, this order would be REJECTED.
            // We clamp it to 1 tick below ask, but if the ask is already very low,
            // we should be aware of this.
            debug!(
                "⚠️ aggressive_price: ceiling ({:.3}) >= best_ask ({:.3}) | applying maker safety margin",
                ceiling, best_ask
            );
        }

        // Shared margin policy with strategy quote clamping.
        let safety_margin = self.post_only_safety_margin_for(side, best_bid, best_ask);
        let safe_below = best_ask - safety_margin;
        if safe_below <= 0.0 {
            return 0.0;
        }
        self.safe_price(ceiling.min(safe_below))
    }

    pub(crate) fn aggressive_sell_price_for(
        &self,
        side: Side,
        floor: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> f64 {
        if floor <= 0.0 || floor >= 1.0 {
            return 0.0;
        }
        if best_bid <= 0.0 {
            return 0.0;
        }

        let safety_margin = self.post_only_safety_margin_for(side, best_bid, best_ask);
        let safe_above = best_bid + safety_margin;
        if safe_above >= 1.0 {
            return 0.0;
        }
        self.safe_price(floor.max(safe_above))
    }

    #[allow(dead_code)]
    pub(crate) fn aggressive_price(&self, ceiling: f64, best_bid: f64, best_ask: f64) -> f64 {
        self.aggressive_price_for(Side::Yes, ceiling, best_bid, best_ask)
    }

    pub(super) fn maker_keep_band(&self, reason: BidReason) -> f64 {
        let tick = self.cfg.tick_size.max(1e-9);
        match reason {
            BidReason::Provide => self.cfg.reprice_threshold.max(2.0 * tick),
            BidReason::Hedge => self.cfg.reprice_threshold.max(tick),
        }
    }

    pub(super) fn keep_existing_maker_if_safe(
        &self,
        inv: &InventoryState,
        side: Side,
        direction: TradeDirection,
        price: f64,
        size: f64,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
        reason: BidReason,
    ) -> bool {
        let Some(current) = self.slot_target(OrderSlot::new(side, direction)) else {
            return false;
        };
        let slot = OrderSlot::new(side, direction);
        let order_age = self.slot_last_ts(slot).elapsed();
        if current.direction != direction || current.reason != reason {
            return false;
        }
        if (current.size - size).abs() > 0.1 {
            return false;
        }
        if self.cfg.strategy == StrategyKind::GlftMm
            && reason == BidReason::Provide
            && self.recent_cross_reject(side, Duration::from_secs(2))
        {
            // After a fresh crossed-book reject, do not keep stale quotes within
            // the normal keep-band; force a fast move toward the latest target.
            return false;
        }
        // Provide path tolerance: allow retaining an existing order that is up to
        // keep-band above the newly computed ceiling, as long as it is still maker-safe.
        // This suppresses needless 1-2 tick churn under fast micro-oscillation.
        let ceiling_tol = match reason {
            BidReason::Provide => self.maker_keep_band(reason),
            BidReason::Hedge => 0.0,
        };
        let max_valid_price = (1.0 - self.cfg.tick_size.max(1e-9)).max(0.0);
        let effective_ceiling = (ceiling + ceiling_tol).min(max_valid_price);
        if current.price > effective_ceiling + 1e-9 {
            return false;
        }
        let safe_limit = self.aggressive_price_for(side, effective_ceiling, best_bid, best_ask);
        if safe_limit <= 0.0 || current.price > safe_limit + 1e-9 {
            return false;
        }
        if direction == TradeDirection::Buy
            && !self.passes_outcome_floor_for_buy(inv, side, current.size, current.price, reason)
        {
            return false;
        }
        if self.cfg.strategy == StrategyKind::GlftMm {
            let tick = self.cfg.tick_size.max(1e-9);
            let glft = *self.glft_rx.borrow();
            let signal_state = glft.signal_state;
            // Pre-live assimilation is stricter to prevent polluted startup quotes from sticking.
            let aligned_drift_cap = match signal_state {
                crate::polymarket::glft::GlftSignalState::Bootstrapping
                | crate::polymarket::glft::GlftSignalState::Assimilating => tick,
                crate::polymarket::glft::GlftSignalState::Live => {
                    let base_ticks = match glft.quote_regime {
                        crate::polymarket::glft::QuoteRegime::Aligned => 3.0,
                        crate::polymarket::glft::QuoteRegime::Tracking => 5.0,
                        crate::polymarket::glft::QuoteRegime::Guarded => 6.0,
                        crate::polymarket::glft::QuoteRegime::Blocked => 2.0,
                    };
                    base_ticks * tick
                }
            };
            let trusted_mid_keep_cap = match signal_state {
                crate::polymarket::glft::GlftSignalState::Bootstrapping
                | crate::polymarket::glft::GlftSignalState::Assimilating => 4.0 * tick,
                crate::polymarket::glft::GlftSignalState::Live => {
                    let base_ticks = match glft.quote_regime {
                        crate::polymarket::glft::QuoteRegime::Aligned => 8.0,
                        crate::polymarket::glft::QuoteRegime::Tracking => 10.0,
                        crate::polymarket::glft::QuoteRegime::Guarded => 12.0,
                        crate::polymarket::glft::QuoteRegime::Blocked => 6.0,
                    };
                    base_ticks * tick
                }
            };
            let trusted_keep_grace = std::time::Duration::from_millis(1_200);
            let side_trusted_mid = match side {
                Side::Yes => glft.trusted_mid,
                Side::No => 1.0 - glft.trusted_mid,
            }
            .clamp(tick, 1.0 - tick);
            let stale_keep_cap = tick;
            let stale_keep_grace = std::time::Duration::from_millis(1_500);
            let aligned = match direction {
                TradeDirection::Buy => current.price + aligned_drift_cap >= price,
                TradeDirection::Sell => current.price <= price + aligned_drift_cap,
            };
            if !aligned {
                return false;
            }
            let stale_misaligned = match direction {
                TradeDirection::Buy => current.price + stale_keep_cap < price,
                TradeDirection::Sell => current.price > price + stale_keep_cap,
            };
            if stale_misaligned && order_age >= stale_keep_grace {
                return false;
            }
            let trusted_mid_misaligned =
                (current.price - side_trusted_mid).abs() > trusted_mid_keep_cap;
            if trusted_mid_misaligned && order_age >= trusted_keep_grace {
                return false;
            }
        }
        let band = self.maker_keep_band(reason) + 1e-9;
        match direction {
            // For maker BUY, keeping an existing lower price is always safer than
            // chasing up every micro-tick. Reprice only when current is too high.
            TradeDirection::Buy => current.price <= price + band,
            // Symmetric rule for SELL (used by endgame/exit paths): keep if
            // current is not too low versus newly desired price.
            TradeDirection::Sell => current.price + band >= price,
        }
    }

    fn price_bounds(&self) -> Option<(f64, f64, f64)> {
        let tick = self.cfg.tick_size;
        if !(0.0..1.0).contains(&tick) {
            return None;
        }
        let max_ticks = (1.0 / tick).floor() - 1.0;
        if max_ticks < 1.0 {
            return None;
        }
        let min_price = tick;
        let max_price = max_ticks * tick;
        Some((tick, min_price, max_price))
    }

    pub(crate) fn quantize_down_to_tick(&self, p: f64) -> f64 {
        let Some((tick, min_price, max_price)) = self.price_bounds() else {
            return 0.0;
        };
        let clamped = p.clamp(min_price, max_price);
        let steps = ((clamped / tick) + 1e-9).floor();
        (steps * tick).clamp(min_price, max_price)
    }

    pub(crate) fn quantize_up_to_tick(&self, p: f64) -> f64 {
        let Some((tick, min_price, max_price)) = self.price_bounds() else {
            return 0.0;
        };
        let clamped = p.clamp(min_price, max_price);
        let steps = ((clamped / tick) - 1e-9).ceil();
        (steps * tick).clamp(min_price, max_price)
    }

    pub(crate) fn quantize_toward_target(&self, current: f64, target: f64) -> f64 {
        if target > current {
            self.quantize_up_to_tick(target)
        } else if target < current {
            self.quantize_down_to_tick(target)
        } else {
            self.safe_price(target)
        }
    }

    pub(crate) fn glft_governor_step_ticks(&self, gap_ticks: f64) -> f64 {
        if gap_ticks >= 12.0 {
            4.0
        } else if gap_ticks >= 6.0 {
            3.0
        } else if gap_ticks >= 3.0 {
            2.0
        } else {
            1.0
        }
    }

    pub(crate) fn glft_governed_price(
        &self,
        current_price: f64,
        normalized_target_price: f64,
        step_ticks: f64,
    ) -> f64 {
        let tick = self.cfg.tick_size.max(1e-9);
        let step = step_ticks.max(1.0) * tick;
        if normalized_target_price > current_price {
            self.quantize_up_to_tick((current_price + step).min(normalized_target_price))
        } else if normalized_target_price < current_price {
            self.quantize_down_to_tick((current_price - step).max(normalized_target_price))
        } else {
            normalized_target_price
        }
    }

    /// FIX #2: Clamp + floor to tick. Prevents negative/out-of-range prices.
    pub(crate) fn safe_price(&self, p: f64) -> f64 {
        self.quantize_down_to_tick(p)
    }
}
