use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::glft::{compute_optimal_offsets, FitQuality, IntensityFitSnapshot};
use crate::polymarket::messages::{BidReason, OrderSlot};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct GlftMmStrategy;

pub(crate) static GLFT_MM_STRATEGY: GlftMmStrategy = GlftMmStrategy;

impl QuoteStrategy for GlftMmStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::GlftMm
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let Some(glft) = input.glft else {
            return StrategyQuotes::default();
        };
        if glft.stale
            || !glft.ready
            || !matches!(glft.fit_quality, FitQuality::Warm | FitQuality::Ready)
        {
            return StrategyQuotes::default();
        }
        if !(input.book.yes_bid > 0.0
            && input.book.yes_ask > input.book.yes_bid
            && input.book.no_bid > 0.0
            && input.book.no_ask > input.book.no_bid)
        {
            return StrategyQuotes::default();
        }

        let cfg = coordinator.cfg();
        let q_norm = (input.inv.net_diff / cfg.max_net_diff.max(1e-9)).clamp(-1.0, 1.0);
        let p_anchor =
            (glft.anchor_prob + glft.basis_prob).clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let alpha_prob = cfg.glft_ofi_alpha * glft.alpha_flow;
        let fit = IntensityFitSnapshot {
            a: glft.fit_a,
            k: glft.fit_k,
            quality: glft.fit_quality,
        };
        let offsets = compute_optimal_offsets(
            q_norm,
            glft.sigma_prob,
            glft.tau_norm,
            fit,
            cfg.glft_gamma,
            cfg.glft_xi,
            cfg.bid_size,
            cfg.max_net_diff,
            cfg.tick_size,
        );
        let spread_mult = 1.0 + cfg.glft_ofi_spread_beta * glft.alpha_flow.abs().powi(2);
        let half_spread = (offsets.half_spread_base * spread_mult).max(cfg.tick_size);
        let r_yes = (p_anchor + alpha_prob - offsets.inventory_shift)
            .clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let r_no = (1.0 - r_yes).clamp(cfg.tick_size, 1.0 - cfg.tick_size);

        let mut quotes = StrategyQuotes::default();
        let yes_buy_ceiling = (r_yes - half_spread).clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let yes_sell_floor = (r_yes + half_spread).clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let no_buy_ceiling = (r_no - half_spread).clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let no_sell_floor = (r_no + half_spread).clamp(cfg.tick_size, 1.0 - cfg.tick_size);

        let yes_buy = coordinator.aggressive_price_for(
            Side::Yes,
            yes_buy_ceiling,
            input.book.yes_bid,
            input.book.yes_ask,
        );
        let yes_sell = coordinator.aggressive_sell_price_for(
            Side::Yes,
            yes_sell_floor,
            input.book.yes_bid,
            input.book.yes_ask,
        );
        let no_buy = coordinator.aggressive_price_for(
            Side::No,
            no_buy_ceiling,
            input.book.no_bid,
            input.book.no_ask,
        );
        let no_sell = coordinator.aggressive_sell_price_for(
            Side::No,
            no_sell_floor,
            input.book.no_bid,
            input.book.no_ask,
        );

        maybe_set_slot_quote(
            coordinator,
            input,
            &mut quotes,
            OrderSlot::YES_BUY,
            yes_buy,
            cfg.bid_size,
        );
        maybe_set_slot_quote(
            coordinator,
            input,
            &mut quotes,
            OrderSlot::YES_SELL,
            yes_sell,
            cfg.bid_size,
        );
        maybe_set_slot_quote(
            coordinator,
            input,
            &mut quotes,
            OrderSlot::NO_BUY,
            no_buy,
            cfg.bid_size,
        );
        maybe_set_slot_quote(
            coordinator,
            input,
            &mut quotes,
            OrderSlot::NO_SELL,
            no_sell,
            cfg.bid_size,
        );
        quotes
    }
}

fn maybe_set_slot_quote(
    coordinator: &StrategyCoordinator,
    input: StrategyTickInput<'_>,
    quotes: &mut StrategyQuotes,
    slot: OrderSlot,
    price: f64,
    size: f64,
) {
    if !(price > 0.0 && size > 0.0) {
        return;
    }
    let intent = StrategyIntent {
        side: slot.side,
        direction: slot.direction,
        price,
        size,
        reason: BidReason::Provide,
    };
    if coordinator.can_place_strategy_intent(input.inv, Some(intent)) {
        quotes.set(intent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::coordinator::Book;
    use crate::polymarket::glft::{FitQuality, GlftSignalSnapshot};
    use crate::polymarket::messages::{InventoryState, MarketDataMsg, OfiSnapshot};

    fn make_coord(cfg: crate::polymarket::coordinator::CoordinatorConfig) -> StrategyCoordinator {
        let (_ofi_tx, ofi_rx) = tokio::sync::watch::channel(OfiSnapshot::default());
        let (_inv_tx, inv_rx) = tokio::sync::watch::channel(InventoryState::default());
        let (_md_tx, md_rx) = tokio::sync::watch::channel(MarketDataMsg::BookTick {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
            ts: std::time::Instant::now(),
        });
        let (_glft_tx, glft_rx) = tokio::sync::watch::channel(GlftSignalSnapshot::default());
        let (om_tx, _om_rx) = tokio::sync::mpsc::channel(1);
        let (_kill_tx, kill_rx) = tokio::sync::mpsc::channel(1);
        let (_feedback_tx, feedback_rx) = tokio::sync::mpsc::channel(1);
        StrategyCoordinator::with_aux_rx(
            cfg,
            ofi_rx,
            inv_rx,
            md_rx,
            glft_rx,
            om_tx,
            kill_rx,
            feedback_rx,
        )
    }

    fn live_snapshot() -> GlftSignalSnapshot {
        GlftSignalSnapshot {
            anchor_prob: 0.50,
            basis_prob: 0.0,
            alpha_flow: 0.0,
            sigma_prob: 0.005,
            tau_norm: 0.5,
            fit_a: 5.0,
            fit_k: 10.0,
            fit_quality: FitQuality::Ready,
            ready: true,
            stale: false,
        }
    }

    #[test]
    fn glft_requires_live_signal() {
        let coord = make_coord(crate::polymarket::coordinator::CoordinatorConfig::default());
        let book = Book {
            yes_bid: 0.49,
            yes_ask: 0.50,
            no_bid: 0.49,
            no_ask: 0.50,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let snapshot = GlftSignalSnapshot::default();
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                glft: Some(&snapshot),
            },
        );
        assert!(quotes.yes_buy.is_none());
    }

    #[test]
    fn glft_quotes_all_four_slots_when_inventory_is_balanced() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.49,
            yes_ask: 0.51,
            no_bid: 0.49,
            no_ask: 0.51,
        };
        let inv = InventoryState {
            yes_qty: 5.0,
            no_qty: 5.0,
            yes_avg_cost: 0.50,
            no_avg_cost: 0.50,
            portfolio_cost: 1.0,
            ..Default::default()
        };
        let metrics = coord.derive_inventory_metrics(&inv);
        let snapshot = live_snapshot();
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                glft: Some(&snapshot),
            },
        );
        assert!(quotes.yes_buy.is_some(), "expected YES buy slot");
        assert!(quotes.yes_sell.is_some(), "expected YES sell slot");
        assert!(quotes.no_buy.is_some(), "expected NO buy slot");
        assert!(quotes.no_sell.is_some(), "expected NO sell slot");
    }

    #[test]
    fn glft_blocks_risk_increasing_slots_at_positive_net_limit() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.58,
            yes_ask: 0.60,
            no_bid: 0.39,
            no_ask: 0.42,
        };
        let inv = InventoryState {
            yes_qty: 15.0,
            no_qty: 0.0,
            yes_avg_cost: 0.55,
            net_diff: 10.0,
            portfolio_cost: 8.25,
            ..Default::default()
        };
        let metrics = coord.derive_inventory_metrics(&inv);
        let snapshot = live_snapshot();
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.yes_buy.is_none(),
            "YES buy should be blocked at +max_net_diff"
        );
        assert!(
            quotes.no_sell.is_none(),
            "NO sell should be blocked at +max_net_diff"
        );
        assert!(
            quotes.yes_sell.is_some(),
            "YES sell should remain available to de-risk"
        );
        assert!(
            quotes.no_buy.is_some(),
            "NO buy should remain available to reduce net exposure"
        );
    }
}
