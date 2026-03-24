use std::time::Instant;

use tracing::{debug, info, warn};

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
        let reprice_eps = 1e-9;

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

        if self.cfg.strategy == StrategyKind::GlftMm && active {
            let tick = self.cfg.tick_size.max(1e-9);
            if (price - slot_price).abs() > tick {
                price = if price > slot_price {
                    slot_price + tick
                } else {
                    slot_price - tick
                };
                price = self.safe_price(price);
            }
        }

        if !active {
            self.emit_quote_log(slot, price, size, reason, log_msg.as_deref());
            self.place_slot(slot, price, size, reason).await;
        } else {
            let reprice_band = self.maker_keep_band(reason);
            let needs_reprice = slot_direction != Some(slot.direction)
                || (slot_price - price).abs() >= (reprice_band - reprice_eps).max(0.0)
                || (slot_size - size).abs() > 0.1;
            if needs_reprice {
                self.emit_quote_log(slot, price, size, reason, log_msg.as_deref());
                debug!(
                    "🔄 reprice {:?} {:?} {:.3}→{:.3} sz={:.1} band={:.3}",
                    slot.side, slot.direction, slot_price, price, size, reprice_band
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
        if reason == CancelReason::Reprice {
            self.stats.cancel_reprice += 1;
        }

        self.slot_targets[slot.index()] = None;
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
    ) {
        // GLFT slot quotes are sometimes nudged right before action (governor/keep-band).
        // Log actionable price/size so quote logs match actual place/reprice.
        if self.cfg.strategy == StrategyKind::GlftMm && reason == BidReason::Provide {
            info!(
                "📐 GLFT {} {:?}@{:.3} sz={:.1}",
                slot.as_str(),
                slot.direction,
                price,
                size
            );
            return;
        }
        if let Some(msg) = log_msg {
            info!("{}", msg);
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
