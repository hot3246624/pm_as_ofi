use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::glft::{
    compute_glft_alpha_shift, compute_optimal_offsets, shape_glft_quotes, FitQuality,
    IntensityFitSnapshot,
};
use crate::polymarket::messages::{BidReason, OrderSlot};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct GlftMmStrategy;

pub(crate) static GLFT_MM_STRATEGY: GlftMmStrategy = GlftMmStrategy;
const GLFT_DAMPED_CHEAP_BUY_MAX_DEV_TRUSTED_TICKS: f64 = 8.0;
const GLFT_GUARDED_CHEAP_BUY_MAX_DEV_TRUSTED_TICKS: f64 = 6.0;
const GLFT_GUARDED_CHEAP_BUY_MAX_DEV_SYNTH_TICKS: f64 = 8.0;

fn aggregated_heat_score(ofi: Option<&crate::polymarket::messages::OfiSnapshot>) -> f64 {
    ofi.map(|snapshot| snapshot.yes.heat_score.max(snapshot.no.heat_score))
        .unwrap_or(0.0)
}

fn heat_spread_mult(heat_score: f64) -> f64 {
    // P2: Damp heat's contribution to spread multiplier.
    // Old: 1.0 + 0.15 * (heat - 1).clamp(0, 4) → max +60% at heat=5
    // New: softer curve via sqrt dampening → max +40% at heat=5, +20% at heat=2
    let excess = (heat_score - 1.0).max(0.0).min(4.0);
    1.0 + 0.10 * excess.sqrt() * excess.sqrt().min(2.0)
}

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
        if !coordinator.glft_is_tradeable_snapshot(*glft)
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
        let p_anchor = glft.trusted_mid.clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let alpha_prob = compute_glft_alpha_shift(
            glft.alpha_flow,
            cfg.glft_ofi_alpha,
            cfg.tick_size,
            glft.fit_status,
        );
        let fit = IntensityFitSnapshot {
            a: glft.fit_a,
            k: glft.fit_k,
            quality: glft.fit_quality,
        };
        let heat_score = aggregated_heat_score(input.ofi);
        let offsets = compute_optimal_offsets(
            q_norm,
            glft.sigma_prob,
            glft.tau_secs,
            fit,
            cfg.glft_gamma,
            cfg.glft_xi,
            cfg.bid_size,
            cfg.max_net_diff,
            cfg.tick_size,
        );
        let spread_mult = (1.0 + cfg.glft_ofi_spread_beta * glft.alpha_flow.abs().powi(2))
            * heat_spread_mult(heat_score);
        let half_spread = (offsets.half_spread_base * spread_mult).max(cfg.tick_size);
        let r_yes = (p_anchor + alpha_prob - offsets.inventory_shift)
            .clamp(cfg.tick_size, 1.0 - cfg.tick_size);
        let shaped = shape_glft_quotes(
            r_yes,
            half_spread,
            glft.quote_regime,
            cfg.tick_size,
            heat_score,
        );

        let mut quotes = StrategyQuotes::default();
        let yes_buy = coordinator.aggressive_price_for(
            Side::Yes,
            shaped.yes_buy_ceiling,
            input.book.yes_bid,
            input.book.yes_ask,
        );
        let yes_sell = coordinator.aggressive_sell_price_for(
            Side::Yes,
            shaped.yes_sell_floor,
            input.book.yes_bid,
            input.book.yes_ask,
        );
        let no_buy = coordinator.aggressive_price_for(
            Side::No,
            shaped.no_buy_ceiling,
            input.book.no_bid,
            input.book.no_ask,
        );
        let no_sell = coordinator.aggressive_sell_price_for(
            Side::No,
            shaped.no_sell_floor,
            input.book.no_bid,
            input.book.no_ask,
        );
        let yes_buy_guarded = !suppressed_by_drift_edge_guard(
            glft.synthetic_mid_yes,
            glft.trusted_mid,
            glft.quote_regime,
            cfg.tick_size,
            OrderSlot::YES_BUY,
            yes_buy,
        );
        let no_buy_guarded = !suppressed_by_drift_edge_guard(
            glft.synthetic_mid_yes,
            glft.trusted_mid,
            glft.quote_regime,
            cfg.tick_size,
            OrderSlot::NO_BUY,
            no_buy,
        );

        if !shaped.suppress_yes_buy && yes_buy_guarded {
            maybe_set_slot_quote(
                coordinator,
                input,
                &mut quotes,
                OrderSlot::YES_BUY,
                yes_buy,
                cfg.bid_size,
            );
        }
        maybe_set_slot_quote(
            coordinator,
            input,
            &mut quotes,
            OrderSlot::YES_SELL,
            yes_sell,
            cfg.bid_size,
        );
        if !shaped.suppress_no_buy && no_buy_guarded {
            maybe_set_slot_quote(
                coordinator,
                input,
                &mut quotes,
                OrderSlot::NO_BUY,
                no_buy,
                cfg.bid_size,
            );
        }
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

fn suppressed_by_drift_edge_guard(
    synthetic_mid_yes: f64,
    trusted_mid_yes: f64,
    quote_regime: crate::polymarket::glft::QuoteRegime,
    tick_size: f64,
    slot: OrderSlot,
    price: f64,
) -> bool {
    if price <= 0.0 {
        return false;
    }
    let trusted_dev_ticks = match quote_regime {
        crate::polymarket::glft::QuoteRegime::Tracking => {
            Some(GLFT_DAMPED_CHEAP_BUY_MAX_DEV_TRUSTED_TICKS)
        }
        crate::polymarket::glft::QuoteRegime::Guarded => {
            Some(GLFT_GUARDED_CHEAP_BUY_MAX_DEV_TRUSTED_TICKS)
        }
        _ => None,
    };
    let Some(max_dev_ticks) = trusted_dev_ticks else {
        return false;
    };
    let cheap_slot = if synthetic_mid_yes <= 0.5 {
        OrderSlot::YES_BUY
    } else {
        OrderSlot::NO_BUY
    };
    if slot != cheap_slot {
        return false;
    }
    let synthetic_side_mid = if slot == OrderSlot::YES_BUY {
        synthetic_mid_yes
    } else {
        1.0 - synthetic_mid_yes
    };
    let trusted_side_mid = if slot == OrderSlot::YES_BUY {
        trusted_mid_yes
    } else {
        1.0 - trusted_mid_yes
    };
    let tick = tick_size.max(1e-9);
    let trusted_dev = max_dev_ticks * tick;
    if price + 1e-9 < trusted_side_mid - trusted_dev {
        return true;
    }
    if matches!(quote_regime, crate::polymarket::glft::QuoteRegime::Guarded) {
        let synth_dev = GLFT_GUARDED_CHEAP_BUY_MAX_DEV_SYNTH_TICKS * tick;
        if price + 1e-9 < synthetic_side_mid - synth_dev {
            return true;
        }
    }
    false
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
    use crate::polymarket::glft::{
        DriftMode, FitQuality, GlftFitSource, GlftFitStatus, GlftReadinessBlockers,
        GlftSignalSnapshot, GlftSignalState, QuoteRegime, WarmStartStatus,
    };
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
            basis_raw: 0.0,
            basis_clamped: 0.0,
            modeled_mid: 0.5,
            trusted_mid: 0.5,
            synthetic_mid_yes: 0.5,
            poly_soft_stale: false,
            basis_drift_ticks: 0.0,
            drift_raw_ticks: 0.0,
            drift_ewma_ticks: 0.0,
            drift_persist_ms: 0,
            alpha_flow: 0.0,
            sigma_prob: 0.005,
            tau_norm: 0.5,
            tau_secs: 150.0,
            fit_a: 5.0,
            fit_k: 10.0,
            fit_quality: FitQuality::Ready,
            fit_source: GlftFitSource::LastGoodFit,
            warm_start_status: WarmStartStatus::Accepted,
            fit_status: GlftFitStatus::LiveReady,
            readiness_blockers: GlftReadinessBlockers::default(),
            ready_elapsed_ms: 2_500,
            signal_state: GlftSignalState::Live,
            quote_regime: QuoteRegime::Aligned,
            reference_confidence: 1.0,
            reference_health: crate::polymarket::glft::ReferenceHealth::Healthy,
            drift_mode: DriftMode::Normal,
            hard_basis_unstable: false,
            ready: true,
            stale: false,
            stale_secs: 0.0,
        }
    }

    #[test]
    fn glft_requires_live_signal() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.strategy = StrategyKind::GlftMm;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.49,
            yes_ask: 0.50,
            no_bid: 0.49,
            no_ask: 0.50,
        };
        let inv = InventoryState {
            yes_qty: 5.0,
            yes_avg_cost: 0.58,
            portfolio_cost: 2.9,
            ..Default::default()
        };
        let metrics = coord.derive_inventory_metrics(&inv);
        let snapshot = GlftSignalSnapshot::default();
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
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
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(quotes.yes_buy.is_some(), "expected YES buy slot");
        assert!(quotes.yes_sell.is_some(), "expected YES sell slot");
        assert!(quotes.no_buy.is_some(), "expected NO buy slot");
        assert!(quotes.no_sell.is_some(), "expected NO sell slot");
    }

    #[test]
    fn glft_uses_signal_trusted_mid_to_avoid_anchor_runaway_quotes() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.47,
            yes_ask: 0.49,
            no_bid: 0.50,
            no_ask: 0.52,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.anchor_prob = 0.99;
        snapshot.basis_prob = 0.10;
        snapshot.modeled_mid = 0.99;
        snapshot.synthetic_mid_yes = 0.48;
        snapshot.trusted_mid = 0.52;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        let yes_buy = quotes.yes_buy.expect("YES buy quote expected");
        let no_buy = quotes.no_buy.expect("NO buy quote expected");
        assert!(
            yes_buy.price > 0.30 && no_buy.price > 0.30,
            "quotes should stay near trusted mid, not collapse to deep penny bids"
        );
    }

    #[test]
    fn glft_frozen_suppresses_dominant_buy_only() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.60,
            yes_ask: 0.61,
            no_bid: 0.38,
            no_ask: 0.39,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.anchor_prob = 0.62;
        snapshot.trusted_mid = 0.62;
        snapshot.modeled_mid = 0.62;
        snapshot.synthetic_mid_yes = 0.50;
        snapshot.basis_drift_ticks = 11.0;
        snapshot.quote_regime = QuoteRegime::Guarded;
        snapshot.drift_mode = DriftMode::Frozen;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.yes_buy.is_none(),
            "dominant YES buy should be suppressed"
        );
        assert!(quotes.no_buy.is_some(), "cheap-side NO buy should remain");
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
                ofi: None,
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

    #[test]
    fn glft_frozen_edge_guard_suppresses_penny_yes_buy() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.10,
            yes_ask: 0.11,
            no_bid: 0.88,
            no_ask: 0.90,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.trusted_mid = 0.11;
        snapshot.anchor_prob = 0.11;
        snapshot.modeled_mid = 0.11;
        snapshot.synthetic_mid_yes = 0.35;
        snapshot.quote_regime = QuoteRegime::Guarded;
        snapshot.drift_mode = DriftMode::Frozen;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.yes_buy.is_none(),
            "frozen edge guard should suppress penny YES buy"
        );
    }

    #[test]
    fn glft_frozen_edge_guard_suppresses_penny_no_buy() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.88,
            yes_ask: 0.90,
            no_bid: 0.10,
            no_ask: 0.11,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.trusted_mid = 0.89;
        snapshot.anchor_prob = 0.89;
        snapshot.modeled_mid = 0.89;
        snapshot.synthetic_mid_yes = 0.65;
        snapshot.quote_regime = QuoteRegime::Guarded;
        snapshot.drift_mode = DriftMode::Frozen;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.no_buy.is_none(),
            "frozen edge guard should suppress penny NO buy"
        );
    }

    #[test]
    fn glft_damped_edge_guard_suppresses_cheap_buy_far_below_trusted_mid() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.10,
            yes_ask: 0.11,
            no_bid: 0.88,
            no_ask: 0.90,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.trusted_mid = 0.30;
        snapshot.anchor_prob = 0.30;
        snapshot.modeled_mid = 0.30;
        snapshot.synthetic_mid_yes = 0.25;
        snapshot.quote_regime = QuoteRegime::Tracking;
        snapshot.drift_mode = DriftMode::Damped;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.yes_buy.is_none(),
            "damped edge guard should suppress YES buy far below trusted mid"
        );
        assert!(
            quotes.no_buy.is_some(),
            "cheap-side guard should not suppress opposite NO buy slot"
        );
    }

    #[test]
    fn glft_edge_guard_does_not_apply_outside_frozen_mode() {
        let mut cfg = crate::polymarket::coordinator::CoordinatorConfig::default();
        cfg.bid_size = 5.0;
        cfg.max_net_diff = 10.0;
        let coord = make_coord(cfg);
        let book = Book {
            yes_bid: 0.10,
            yes_ask: 0.11,
            no_bid: 0.88,
            no_ask: 0.90,
        };
        let inv = InventoryState::default();
        let metrics = coord.derive_inventory_metrics(&inv);
        let mut snapshot = live_snapshot();
        snapshot.trusted_mid = 0.11;
        snapshot.anchor_prob = 0.11;
        snapshot.modeled_mid = 0.11;
        snapshot.synthetic_mid_yes = 0.12;
        snapshot.quote_regime = QuoteRegime::Aligned;
        snapshot.drift_mode = DriftMode::Normal;
        let quotes = StrategyKind::GlftMm.compute_quotes(
            &coord,
            StrategyTickInput {
                inv: &inv,
                book: &book,
                metrics: &metrics,
                ofi: None,
                glft: Some(&snapshot),
            },
        );
        assert!(
            quotes.yes_buy.is_some(),
            "edge guard should not suppress in non-frozen mode"
        );
    }
}
