use super::*;
use crate::polymarket::glft::{
    DriftMode, FitQuality, GlftFitStatus, GlftReadinessBlockers, GlftSignalState, QuoteRegime,
    ReferenceHealth, WarmStartStatus,
};
use std::time::Duration;
use tokio::time::timeout;

fn cfg() -> CoordinatorConfig {
    CoordinatorConfig {
        // Keep test baseline aligned with the current parked implementation.
        strategy: StrategyKind::GabagoolGrid,
        pair_target: 0.98,
        max_net_diff: 10.0,
        bid_size: 2.0,
        tick_size: 0.01,
        reprice_threshold: 0.001,
        debounce_ms: 0, // disable for tests
        as_skew_factor: 0.03,
        dry_run: false,
        ..CoordinatorConfig::default()
    }
}

fn with_strategy(mut c: CoordinatorConfig, strategy: StrategyKind) -> CoordinatorConfig {
    c.strategy = strategy;
    c
}

#[derive(Clone)]
struct TestInventoryTx(watch::Sender<InventorySnapshot>);

impl TestInventoryTx {
    fn send(&self, inv: InventoryState) -> Result<(), watch::error::SendError<InventorySnapshot>> {
        self.0.send(InventorySnapshot {
            settled: inv,
            working: inv,
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
        })
    }
}

fn make(
    c: CoordinatorConfig,
) -> (
    watch::Sender<OfiSnapshot>,
    TestInventoryTx,
    watch::Sender<MarketDataMsg>,
    mpsc::Sender<KillSwitchSignal>,
    mpsc::Receiver<OrderManagerCmd>,
    StrategyCoordinator,
) {
    let (o, or) = watch::channel(OfiSnapshot::default());
    let (i, ir) = watch::channel(InventorySnapshot::default());
    let (m, mr) = watch::channel(MarketDataMsg::BookTick {
        yes_bid: 0.0,
        yes_ask: 0.0,
        no_bid: 0.0,
        no_ask: 0.0,
        ts: Instant::now(),
    });
    let (e, er) = mpsc::channel(16);
    let (k, kr) = mpsc::channel(16);
    (
        o,
        TestInventoryTx(i),
        m,
        k,
        er,
        StrategyCoordinator::with_kill_rx(c, or, ir, mr, e, kr),
    )
}

fn make_with_glft(
    c: CoordinatorConfig,
) -> (
    watch::Sender<OfiSnapshot>,
    TestInventoryTx,
    watch::Sender<MarketDataMsg>,
    watch::Sender<GlftSignalSnapshot>,
    mpsc::Sender<KillSwitchSignal>,
    mpsc::Receiver<OrderManagerCmd>,
    StrategyCoordinator,
) {
    let (o, or) = watch::channel(OfiSnapshot::default());
    let (i, ir) = watch::channel(InventorySnapshot::default());
    let (m, mr) = watch::channel(MarketDataMsg::BookTick {
        yes_bid: 0.0,
        yes_ask: 0.0,
        no_bid: 0.0,
        no_ask: 0.0,
        ts: Instant::now(),
    });
    let (g, gr) = watch::channel(GlftSignalSnapshot::default());
    let (e, er) = mpsc::channel(16);
    let (k, kr) = mpsc::channel(16);
    let (_f, fr) = mpsc::channel(16);
    let (_r, rr) = mpsc::channel(16);
    (
        o,
        TestInventoryTx(i),
        m,
        g,
        k,
        er,
        StrategyCoordinator::with_aux_rx(c, or, ir, mr, gr, e, kr, fr, rr),
    )
}

fn bt(yb: f64, ya: f64, nb: f64, na: f64) -> MarketDataMsg {
    MarketDataMsg::BookTick {
        yes_bid: yb,
        yes_ask: ya,
        no_bid: nb,
        no_ask: na,
        ts: Instant::now(),
    }
}

fn book(yb: f64, ya: f64, nb: f64, na: f64) -> Book {
    Book {
        yes_bid: yb,
        yes_ask: ya,
        no_bid: nb,
        no_ask: na,
    }
}

fn live_glft_snapshot() -> GlftSignalSnapshot {
    GlftSignalSnapshot {
        signal_state: GlftSignalState::Live,
        fit_status: GlftFitStatus::LiveReady,
        fit_quality: FitQuality::Ready,
        warm_start_status: WarmStartStatus::Accepted,
        ready: true,
        stale: false,
        quote_regime: QuoteRegime::Aligned,
        reference_confidence: 1.0,
        reference_health: ReferenceHealth::Healthy,
        stale_secs: 0.0,
        ..GlftSignalSnapshot::default()
    }
}

fn phase_builder_quotes(c: CoordinatorConfig, inv: InventoryState, book: Book) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::PhaseBuilder));
    let metrics = coord.derive_inventory_metrics(&inv);
    StrategyKind::PhaseBuilder.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &crate::polymarket::messages::InventorySnapshot {
                settled: inv,
                working: inv,
                pending_yes_qty: 0.0,
                pending_no_qty: 0.0,
                fragile: false,
            },
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        },
    )
}

fn gabagool_grid_quotes(c: CoordinatorConfig, inv: InventoryState, book: Book) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::GabagoolGrid));
    let metrics = coord.derive_inventory_metrics(&inv);
    StrategyKind::GabagoolGrid.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &crate::polymarket::messages::InventorySnapshot {
                settled: inv,
                working: inv,
                pending_yes_qty: 0.0,
                pending_no_qty: 0.0,
                fragile: false,
            },
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        },
    )
}

fn gabagool_corridor_quotes(
    c: CoordinatorConfig,
    inv: InventoryState,
    book: Book,
) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::GabagoolCorridor));
    let metrics = coord.derive_inventory_metrics(&inv);
    StrategyKind::GabagoolCorridor.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &crate::polymarket::messages::InventorySnapshot {
                settled: inv,
                working: inv,
                pending_yes_qty: 0.0,
                pending_no_qty: 0.0,
                fragile: false,
            },
            book: &book,
            metrics: &metrics,
            ofi: None,
            glft: None,
        },
    )
}

fn pair_arb_quotes(
    c: CoordinatorConfig,
    inv: InventoryState,
    book: Book,
    ofi: Option<OfiSnapshot>,
) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::PairArb));
    let metrics = coord.derive_inventory_metrics(&inv);
    StrategyKind::PairArb.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &crate::polymarket::messages::InventorySnapshot {
                settled: inv,
                working: inv,
                pending_yes_qty: 0.0,
                pending_no_qty: 0.0,
                fragile: false,
            },
            book: &book,
            metrics: &metrics,
            ofi: ofi.as_ref(),
            glft: None,
        },
    )
}

fn pair_arb_quotes_with_snapshot(
    c: CoordinatorConfig,
    settled: InventoryState,
    working: InventoryState,
    inventory: InventorySnapshot,
    book: Book,
    ofi: Option<OfiSnapshot>,
) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::PairArb));
    let metrics = coord.derive_inventory_metrics(&working);
    StrategyKind::PairArb.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &working,
            settled_inv: &settled,
            working_inv: &working,
            inventory: &inventory,
            book: &book,
            metrics: &metrics,
            ofi: ofi.as_ref(),
            glft: None,
        },
    )
}

#[test]
fn test_pair_arb_uses_working_inventory_for_tiered_yes_cap() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    c.bid_size = 5.0;
    c.max_net_diff = 15.0;
    c.pair_arb.tier_1_mult = 0.75;
    c.pair_arb.tier_2_mult = 0.50;

    let settled = InventoryState {
        yes_qty: 5.0,
        yes_avg_cost: 0.62,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 0.0,
        ..Default::default()
    };
    let working = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.61,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 5.0,
        ..Default::default()
    };
    let inventory = InventorySnapshot {
        settled,
        working,
        pending_yes_qty: 5.0,
        pending_no_qty: 0.0,
        fragile: false,
    };

    let tier_1_mult = c.pair_arb.tier_1_mult;
    let quotes = pair_arb_quotes_with_snapshot(
        c,
        settled,
        working,
        inventory,
        book(0.61, 0.62, 0.34, 0.35),
        None,
    );
    if let Some(intent) = quotes.get(OrderSlot::YES_BUY) {
        let max_yes = working.yes_avg_cost * tier_1_mult;
        assert!(
            intent.price <= max_yes + 1e-9,
            "YES quote must honor working-net tier cap: got {:.4}, max {:.4}",
            intent.price,
            max_yes
        );
    }
}

#[test]
fn test_pair_arb_execution_recheck_rejects_stale_same_side_quote_from_working_inventory() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.bid_size = 5.0;
    config.max_net_diff = 15.0;
    config.pair_arb.tier_1_mult = 0.75;
    config.pair_arb.tier_2_mult = 0.50;
    let (_o, i, _m, _k, _er, coord) = make(config);

    let settled = InventoryState {
        yes_qty: 5.0,
        yes_avg_cost: 0.62,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 0.0,
        ..Default::default()
    };
    let working = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.61,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 5.0,
        ..Default::default()
    };
    let _ = i.0.send(InventorySnapshot {
        settled,
        working,
        pending_yes_qty: 5.0,
        pending_no_qty: 0.0,
        fragile: false,
    });

    let ub = book(0.61, 0.62, 0.34, 0.35);
    assert!(
        !coord.pair_arb_quote_still_admissible(
            &working,
            &ub,
            OrderSlot::YES_BUY,
            0.57,
            5.0,
            EndgamePhase::Normal,
        ),
        "fill-triggered recheck should invalidate stale YES build quotes once working net_diff reaches tiered cap range"
    );
}

// ── Price clamping ──

#[test]
fn test_safe_price_clamps_negative() {
    let (_, _, _, _, _, c) = make(cfg());
    assert!((c.safe_price(-0.5) - 0.01).abs() < 1e-9);
}

#[test]
fn test_safe_price_clamps_over_one() {
    let (_, _, _, _, _, c) = make(cfg());
    assert!((c.safe_price(1.5) - 0.99).abs() < 1e-9);
}

#[test]
fn test_safe_price_normal() {
    let (_, _, _, _, _, c) = make(cfg());
    assert!((c.safe_price(0.45) - 0.45).abs() < 1e-9);
}

#[test]
fn test_quantize_toward_target_advances_on_float_boundary() {
    let (_, _, _, _, _, c) = make(cfg());
    let current = 0.09;
    let target = 0.09 + 0.01; // can become 0.099999999...
    let next = c.quantize_toward_target(current, target);
    assert!(
        next >= 0.10 - 1e-9,
        "expected monotonic advance to next tick, got {:.5}",
        next
    );
}

#[test]
fn test_glft_governor_step_ticks_scales_with_gap() {
    let (_, _, _, _, _, c) = make(cfg());
    assert!((c.glft_governor_step_ticks(1.0) - 1.0).abs() < 1e-9);
    assert!((c.glft_governor_step_ticks(3.0) - 2.0).abs() < 1e-9);
    assert!((c.glft_governor_step_ticks(6.0) - 3.0).abs() < 1e-9);
    assert!((c.glft_governor_step_ticks(12.0) - 4.0).abs() < 1e-9);
}

#[test]
fn test_glft_governed_price_moves_faster_without_overshoot() {
    let (_, _, _, _, _, c) = make(cfg());
    let up = c.glft_governed_price(0.20, 0.60, 4.0);
    assert!((up - 0.24).abs() < 1e-9, "unexpected up-step: {up:.5}");

    let up_small = c.glft_governed_price(0.24, 0.25, 4.0);
    assert!(
        (up_small - 0.25).abs() < 1e-9,
        "overshot small up target: {up_small:.5}"
    );

    let down = c.glft_governed_price(0.40, 0.10, 3.0);
    assert!(
        (down - 0.37).abs() < 1e-9,
        "unexpected down-step: {down:.5}"
    );
}

// ── Aggressive pricing ──

#[test]
fn test_aggressive_ceiling_wins() {
    let (_, _, _, _, _, c) = make(cfg());
    assert!((c.aggressive_price(0.50, 0.40, 0.55) - 0.50).abs() < 1e-9);
}

#[test]
fn test_aggressive_ask_wins() {
    let (_, _, _, _, _, c) = make(cfg());
    // Tight spread adds one extra safety tick:
    // best_bid=0.50 best_ask=0.52, margin=(2+1) ticks -> 0.49
    assert!((c.aggressive_price(0.60, 0.50, 0.52) - 0.49).abs() < 1e-9);
}

#[test]
fn test_cross_book_feedback_increases_side_specific_margin() {
    let (_, _, _, _, _, mut c) = make(cfg());
    let base = c.post_only_safety_margin_for(Side::Yes, 0.30, 0.34);
    c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
        slot: OrderSlot::YES_BUY,
        ts: Instant::now(),
        rejected_action_price: 0.33,
    });
    let widened = c.post_only_safety_margin_for(Side::Yes, 0.30, 0.34);
    assert!((widened - (base + c.cfg.tick_size)).abs() < 1e-9);
}

#[test]
fn test_cross_book_feedback_caps_and_decays() {
    let (_, _, _, _, _, mut c) = make(cfg());
    let t0 = Instant::now();
    for _ in 0..5 {
        c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
            slot: OrderSlot::NO_BUY,
            ts: t0,
            rejected_action_price: 0.41,
        });
    }
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 3);

    c.no_maker_friction.last_cross_reject_ts = Some(t0);
    c.decay_maker_friction(t0 + Duration::from_secs(4));
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 2);
    c.decay_maker_friction(t0 + Duration::from_secs(8));
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 1);
    c.decay_maker_friction(t0 + Duration::from_secs(12));
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 0);
}

#[test]
fn test_pair_arb_cross_reject_sticky_margin_increases_and_clears_on_accept() {
    let (_, _, _, _, _, mut c) = make(with_strategy(cfg(), StrategyKind::PairArb));
    c.book = book(0.43, 0.45, 0.40, 0.42);
    let slot = OrderSlot::YES_BUY;

    let base = c.pair_arb_action_price_for_post_only(slot, 0.44, 5.0, BidReason::Provide);

    c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
        slot,
        ts: Instant::now(),
        rejected_action_price: 0.44,
    });
    assert_eq!(c.slot_pair_arb_cross_reject_extra_ticks[slot.index()], 1);
    assert!(c.slot_pair_arb_cross_reject_reprice_pending[slot.index()]);
    let widened = c.pair_arb_action_price_for_post_only(slot, 0.44, 5.0, BidReason::Provide);
    assert!(
        (widened - (base - c.cfg.tick_size)).abs() < 1e-9,
        "expected one extra sticky tick: base={base:.4} widened={widened:.4} tick={:.4}",
        c.cfg.tick_size
    );

    c.handle_execution_feedback(ExecutionFeedback::OrderAccepted {
        slot,
        ts: Instant::now(),
    });
    assert_eq!(c.slot_pair_arb_cross_reject_extra_ticks[slot.index()], 0);
    assert!(!c.slot_pair_arb_cross_reject_reprice_pending[slot.index()]);
    let reset = c.pair_arb_action_price_for_post_only(slot, 0.44, 5.0, BidReason::Provide);
    assert!((reset - base).abs() < 1e-9);
}

#[test]
fn test_pair_arb_cross_reject_retry_price_steps_down_monotonically() {
    let (_, _, _, _, _, mut c) = make(with_strategy(cfg(), StrategyKind::PairArb));
    c.book = book(0.30, 0.31, 0.66, 0.67);
    let slot = OrderSlot::NO_BUY;
    let tick = c.cfg.tick_size.max(1e-9);

    let mut prev_action =
        c.pair_arb_action_price_for_post_only(slot, 0.66, 5.0, BidReason::Provide);
    for _ in 0..6 {
        c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
            slot,
            ts: Instant::now(),
            rejected_action_price: prev_action,
        });
        assert!(
            c.slot_pair_arb_cross_reject_reprice_pending[slot.index()],
            "cross reject must force pending reprice"
        );
        let next_action =
            c.pair_arb_action_price_for_post_only(slot, 0.66, 5.0, BidReason::Provide);
        assert!(
            next_action <= prev_action - tick + 1e-9,
            "action price must step down by >= 1 tick after cross reject: prev={prev_action:.4} next={next_action:.4} tick={tick:.4}"
        );
        prev_action = next_action;
    }
}

#[test]
fn test_pair_arb_state_forced_republish_is_latched() {
    let (_, _, _, _, _, mut c) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let slot = OrderSlot::YES_BUY;
    c.book = book(0.23, 0.24, 0.75, 0.76);
    c.slot_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    });
    c.yes_target = c.slot_targets[slot.index()].clone();
    c.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: None,
        net_bucket: PairArbNetBucket::Flat,
        risk_open_cutoff_active: false,
    });

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.30,
        no_qty: 5.0,
        no_avg_cost: 0.75,
        net_diff: 5.0,
        ..Default::default()
    };
    let intent = StrategyIntent {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.24,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let ub = c.book;

    let d1 = c.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        Some(intent),
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(matches!(d1, RetentionDecision::Republish));
    assert_eq!(c.stats.pair_arb_state_forced_republish, 1);
    assert!(c.slot_pair_arb_state_republish_latched[slot.index()]);

    let d2 = c.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        Some(intent),
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(matches!(d2, RetentionDecision::Republish));
    assert_eq!(c.stats.pair_arb_state_forced_republish, 1);
}

#[test]
fn test_pair_arb_state_forced_republish_stays_single_under_repeated_rechecks() {
    let (_, _, _, _, _, mut c) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let slot = OrderSlot::NO_BUY;
    c.book = book(0.23, 0.24, 0.75, 0.76);
    c.slot_targets[slot.index()] = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.55,
        size: 5.0,
        reason: BidReason::Provide,
    });
    c.no_target = c.slot_targets[slot.index()].clone();
    c.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::Yes),
        net_bucket: PairArbNetBucket::Mid,
        risk_open_cutoff_active: false,
    });

    let inv = InventoryState {
        yes_qty: 9.0,
        yes_avg_cost: 0.40,
        no_qty: 5.0,
        no_avg_cost: 0.55,
        net_diff: 4.0,
        ..Default::default()
    };
    let intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.53,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let ub = c.book;

    for _ in 0..64 {
        let d = c.evaluate_slot_retention(
            &inv,
            &ub,
            slot,
            Some(intent),
            CancelReason::Reprice,
            EndgamePhase::Normal,
        );
        assert!(matches!(d, RetentionDecision::Republish));
    }

    assert_eq!(
        c.stats.pair_arb_state_forced_republish, 1,
        "state-forced republish must be latched and counted once until ack/clear"
    );
    assert!(
        c.slot_pair_arb_state_republish_latched[slot.index()],
        "latch should remain set while still waiting for replace to complete"
    );
}

#[test]
fn test_recent_cross_reject_disables_glft_keep_band() {
    let (_, _, _, _, _, mut c) = make(with_strategy(cfg(), StrategyKind::GlftMm));
    let slot = OrderSlot::NO_BUY;
    let now = Instant::now();
    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.46,
        size: 5.0,
        reason: BidReason::Provide,
    };
    c.slot_targets[slot.index()] = Some(target.clone());
    c.no_target = Some(target);
    c.slot_last_ts[slot.index()] = now - Duration::from_secs(3);
    c.no_last_ts = now - Duration::from_secs(3);
    c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
        slot,
        ts: now,
        rejected_action_price: 0.46,
    });

    let inv = InventoryState::default();
    assert!(!c.keep_existing_maker_if_safe(
        &inv,
        Side::No,
        TradeDirection::Buy,
        0.45,
        5.0,
        0.45,
        0.44,
        0.47,
        BidReason::Provide,
    ));
}

#[test]
fn test_pair_arb_quote_still_admissible_uses_current_bucket_tier_semantics() {
    let (_, _, _, _, _, coord) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let flat = InventoryState {
        yes_qty: 5.0,
        yes_avg_cost: 0.40,
        no_qty: 5.0,
        no_avg_cost: 0.49,
        net_diff: 0.0,
        ..Default::default()
    };
    let ub = book(0.54, 0.55, 0.48, 0.49);
    assert!(coord.pair_arb_quote_still_admissible(
        &flat,
        &ub,
        OrderSlot::YES_BUY,
        0.32,
        5.0,
        EndgamePhase::Normal,
    ));
    // Flat state should not pre-apply tier1 based on full-size projection.
    assert!(coord.pair_arb_quote_still_admissible(
        &flat,
        &ub,
        OrderSlot::YES_BUY,
        0.42,
        5.0,
        EndgamePhase::Normal,
    ));
    assert!(!coord.pair_arb_quote_still_admissible(
        &flat,
        &ub,
        OrderSlot::YES_BUY,
        0.73,
        5.0,
        EndgamePhase::Normal,
    ));
}

#[test]
fn test_glft_keep_alignment_uses_cold_vs_live_drift_caps() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, glft_tx, _k, _er, mut c) = make_with_glft(config);

    let slot = OrderSlot::YES_BUY;
    let now = Instant::now();
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.17,
        size: 5.0,
        reason: BidReason::Provide,
    };
    c.slot_targets[slot.index()] = Some(target.clone());
    c.yes_target = Some(target);
    c.slot_last_ts[slot.index()] = now - Duration::from_secs(2);
    c.yes_last_ts = now - Duration::from_secs(2);

    let inv = InventoryState::default();
    // Pre-live assimilation: 1 tick drift cap => 0.17 -> desired 0.19 should not keep.
    assert!(!c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.19,
        5.0,
        0.19,
        0.18,
        0.30,
        BidReason::Provide,
    ));

    let mut live = GlftSignalSnapshot::default();
    live.signal_state = crate::polymarket::glft::GlftSignalState::Live;
    live.ready = true;
    let _ = glft_tx.send(live);
    c.slot_last_ts[slot.index()] = Instant::now();
    c.yes_last_ts = Instant::now();

    // Live: 2 tick drift cap => same order is still aligned and can be kept.
    assert!(c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.19,
        5.0,
        0.19,
        0.18,
        0.30,
        BidReason::Provide,
    ));
}

#[test]
fn test_keep_existing_hedge_uses_one_tick_band() {
    let (_, _, _, _, _, mut c) = make(cfg());
    c.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 2.0,
        reason: BidReason::Hedge,
    });
    let inv = InventoryState::default();

    assert!(c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.51,
        2.0,
        0.60,
        0.49,
        0.56,
        BidReason::Hedge,
    ));
    assert!(!c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.48,
        2.0,
        0.60,
        0.49,
        0.56,
        BidReason::Hedge,
    ));
}

#[test]
fn test_keep_existing_buy_does_not_chase_up_when_current_is_safer() {
    let (_, _, _, _, _, mut c) = make(cfg());
    c.no_target = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    });
    let inv = InventoryState::default();

    assert!(c.keep_existing_maker_if_safe(
        &inv,
        Side::No,
        TradeDirection::Buy,
        0.51,
        2.0,
        0.60,
        0.44,
        0.56,
        BidReason::Provide,
    ));
}

#[test]
fn test_keep_existing_provide_allows_small_ceiling_overshoot_within_band() {
    let (_, _, _, _, _, mut c) = make(cfg());
    c.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.31,
        size: 2.0,
        reason: BidReason::Provide,
    });
    let inv = InventoryState::default();

    // 1 tick above newly computed target can still be retained if maker-safe.
    assert!(c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.30,
        2.0,
        0.30,
        0.29,
        0.35,
        BidReason::Provide,
    ));

    // Beyond keep band should no longer be retained.
    assert!(!c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.27,
        2.0,
        0.27,
        0.29,
        0.35,
        BidReason::Provide,
    ));
}

#[test]
fn test_pair_arb_keep_existing_uses_looser_resting_buy_safety() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_, _, _, _, _, mut c) = make(config);
    c.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.49,
        size: 2.0,
        reason: BidReason::Provide,
    });
    let inv = InventoryState::default();

    // With best ask = 0.50 and tick = 0.01, pair_arb retain path allows
    // keeping a resting BUY at 0.49 (one tick below ask).
    assert!(c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.50,
        2.0,
        0.50,
        0.48,
        0.50,
        BidReason::Provide,
    ));
}

#[test]
fn test_non_pair_arb_keep_existing_still_uses_strict_submit_safety() {
    let (_, _, _, _, _, mut c) = make(cfg());
    c.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.49,
        size: 2.0,
        reason: BidReason::Provide,
    });
    let inv = InventoryState::default();

    // Non-pair_arb strategies keep the stricter aggressive_price safety margin.
    assert!(!c.keep_existing_maker_if_safe(
        &inv,
        Side::Yes,
        TradeDirection::Buy,
        0.50,
        2.0,
        0.50,
        0.48,
        0.50,
        BidReason::Provide,
    ));
}

#[tokio::test]
async fn test_keep_existing_provide_when_still_maker_safe() {
    let (o, i, m, _, mut er, mut coord) = make(cfg());
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 2.0,
        reason: BidReason::Provide,
    });

    let ub = book(0.29, 0.33, 0.20, 0.24);
    let _ = m.send(bt(ub.yes_bid, ub.yes_ask, ub.no_bid, ub.no_ask));
    coord
        .apply_provide_side_action(
            &InventoryState::default(),
            &ub,
            Side::Yes,
            ProvideSideAction::Place {
                intent: StrategyIntent {
                    side: Side::Yes,
                    direction: TradeDirection::Buy,
                    price: 0.31,
                    size: 2.0,
                    reason: BidReason::Provide,
                },
            },
        )
        .await;

    assert!(timeout(Duration::from_millis(20), er.recv()).await.is_err());
    assert!(
        coord.stats.retain_hits > 0,
        "provide keep path should increment retain_hits for observability"
    );
}

#[tokio::test]
async fn test_pair_arb_absent_intent_retain_does_not_clear_active_buy() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (o, i, m, _, mut er, mut coord) = make(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 2.0,
        reason: BidReason::Provide,
    });

    let ub = book(0.29, 0.35, 0.20, 0.24);
    let _ = m.send(bt(ub.yes_bid, ub.yes_ask, ub.no_bid, ub.no_ask));
    coord
        .apply_provide_side_action(
            &InventoryState::default(),
            &ub,
            Side::Yes,
            ProvideSideAction::Clear {
                reason: CancelReason::Reprice,
            },
        )
        .await;

    assert!(
        timeout(Duration::from_millis(20), er.recv()).await.is_err(),
        "pair_arb absent-intent retain should not emit clear command while order is maker-safe"
    );
    assert!(
        coord.slot_target(OrderSlot::YES_BUY).is_some(),
        "pair_arb retain should keep active buy target"
    );
    assert!(
        coord.stats.retain_hits > 0,
        "pair_arb retain path should increment retain_hits for observability"
    );
}

#[tokio::test]
async fn test_reprice_when_existing_provide_no_longer_maker_safe() {
    let (o, i, m, _, mut er, mut coord) = make(cfg());
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.31,
        size: 2.0,
        reason: BidReason::Provide,
    });

    let ub = book(0.29, 0.31, 0.20, 0.24);
    let _ = m.send(bt(ub.yes_bid, ub.yes_ask, ub.no_bid, ub.no_ask));
    coord
        .apply_provide_side_action(
            &InventoryState::default(),
            &ub,
            Side::Yes,
            ProvideSideAction::Place {
                intent: StrategyIntent {
                    side: Side::Yes,
                    direction: TradeDirection::Buy,
                    price: 0.29,
                    size: 2.0,
                    reason: BidReason::Provide,
                },
            },
        )
        .await;

    match timeout(Duration::from_millis(100), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.side, Side::Yes);
            assert!((target.price - 0.29).abs() < 1e-9);
        }
        other => panic!(
            "expected SetTarget after unsafe maker drift, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_fast_plus_minus_one_tick_oscillation_keeps_existing_order() {
    let (o, i, _m, _, mut er, mut coord) = make(cfg());
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    coord.no_target = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 2.0,
        reason: BidReason::Provide,
    });
    let inv = InventoryState::default();

    for (bid, ask, next_price) in [(0.29, 0.33, 0.31), (0.30, 0.34, 0.30), (0.29, 0.33, 0.31)] {
        let ub = book(0.20, 0.24, bid, ask);
        coord
            .apply_provide_side_action(
                &inv,
                &ub,
                Side::No,
                ProvideSideAction::Place {
                    intent: StrategyIntent {
                        side: Side::No,
                        direction: TradeDirection::Buy,
                        price: next_price,
                        size: 2.0,
                        reason: BidReason::Provide,
                    },
                },
            )
            .await;
    }

    assert!(timeout(Duration::from_millis(20), er.recv()).await.is_err());
}

#[test]
fn test_glft_forced_realign_threshold_at_14_10() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, coord) = make_with_glft(config);
    let _ = g.send(GlftSignalSnapshot {
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    });

    // Below threshold: trusted=13 ticks, target=9 ticks => no force_realign
    let result = coord
        .slot_relative_misalignment(OrderSlot::YES_BUY, 0.37, 0.28)
        .unwrap();
    assert!(!result.2, "13/9 ticks should NOT trigger forced realign");

    // Above threshold: trusted=15 ticks, target=11 ticks => force_realign
    let result = coord
        .slot_relative_misalignment(OrderSlot::YES_BUY, 0.35, 0.24)
        .unwrap();
    assert!(result.2, "15/11 ticks should trigger forced realign");
}

#[test]
fn test_glft_forced_realign_no_longer_triggers_early_bypass() {
    // Verify that soft force-realign is not too eager below 14/10 thresholds.
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, coord) = make_with_glft(config);
    let _ = g.send(GlftSignalSnapshot {
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    });

    // trusted=13 ticks, target=9 ticks => should NOT trigger
    let result = coord
        .slot_relative_misalignment(OrderSlot::YES_BUY, 0.37, 0.28)
        .unwrap();
    assert!(
        !result.2,
        "13/9 ticks should NOT trigger forced realign with 14/10 soft thresholds"
    );
}

#[test]
fn test_glft_publish_target_debt_threshold_mapping_is_not_globally_disabled() {
    assert_eq!(
        StrategyCoordinator::glft_publish_target_debt_threshold(Some(QuoteRegime::Aligned)),
        7.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_target_debt_threshold(Some(QuoteRegime::Tracking)),
        8.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_target_debt_threshold(Some(QuoteRegime::Guarded)),
        9.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_target_debt_threshold(Some(QuoteRegime::Blocked)),
        99.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_target_debt_threshold(None),
        7.0
    );
}

#[test]
fn test_glft_structural_and_release_threshold_mapping_is_regime_specific() {
    assert_eq!(
        StrategyCoordinator::glft_publish_structural_debt_threshold(Some(QuoteRegime::Aligned)),
        6.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_structural_debt_threshold(Some(QuoteRegime::Tracking)),
        7.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_structural_debt_threshold(Some(QuoteRegime::Guarded)),
        8.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_structural_debt_threshold(Some(QuoteRegime::Blocked)),
        99.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_debt_release_threshold(Some(QuoteRegime::Aligned)),
        9.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_debt_release_threshold(Some(QuoteRegime::Tracking)),
        10.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_debt_release_threshold(Some(QuoteRegime::Guarded)),
        11.0
    );
    assert_eq!(
        StrategyCoordinator::glft_publish_debt_release_threshold(Some(QuoteRegime::Blocked)),
        99.0
    );
}

#[tokio::test]
async fn test_glft_publish_budget_suppresses_non_emergency_reprice() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, _m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let glft_live = GlftSignalSnapshot {
        trusted_mid: 0.80,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft_live);

    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(target.clone());
    coord.slot_last_ts[OrderSlot::NO_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_target = Some(target);
    coord.slot_shadow_targets[OrderSlot::NO_BUY.index()] = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.58,
        size: 6.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[OrderSlot::NO_BUY.index()] =
        Some(Instant::now() - Duration::from_secs(3));
    let glft = glft_live;
    coord.slot_policy_states[OrderSlot::NO_BUY.index()] =
        Some(coord.build_slot_quote_policy(OrderSlot::NO_BUY, 0.30, 5.0, glft));
    coord.slot_policy_since[OrderSlot::NO_BUY.index()] =
        Some(Instant::now() - Duration::from_secs(3));
    coord.slot_policy_candidates[OrderSlot::NO_BUY.index()] =
        Some(coord.build_slot_quote_policy(OrderSlot::NO_BUY, 0.70, 6.0, glft));
    coord.slot_policy_candidate_since[OrderSlot::NO_BUY.index()] =
        Some(Instant::now() - Duration::from_secs(3));
    coord.slot_publish_budget[OrderSlot::NO_BUY.index()] = 0.0;
    coord.slot_last_budget_refill[OrderSlot::NO_BUY.index()] = Instant::now();
    coord.slot_last_regime_seen[OrderSlot::NO_BUY.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[OrderSlot::NO_BUY.index()] =
        Instant::now() - Duration::from_secs(5);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.70, 6.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(20), er.recv()).await.is_err(),
        "budget exhausted should suppress non-emergency reprice"
    );
    assert!(
        coord.stats.publish_budget_suppressed
            + coord.stats.shadow_suppressed_updates
            + coord.stats.policy_noop_ticks
            >= 1,
        "expected local suppression when non-emergency publish is withheld"
    );
}

#[tokio::test]
async fn test_glft_sync_publish_is_budget_governed() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    coord.book = book(0.45, 0.46, 0.54, 0.55);
    let glft = GlftSignalSnapshot {
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.yes_target = Some(target);
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.55,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(12));
    coord.slot_policy_states[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.30, 5.0, glft));
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(12));
    coord.slot_policy_candidates[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.55, 5.0, glft));
    coord.slot_policy_candidate_since[slot.index()] =
        Some(Instant::now() - Duration::from_secs(12));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.slot_publish_budget[slot.index()] = 2.0;
    coord.slot_last_budget_refill[slot.index()] = Instant::now() - Duration::from_secs(1);
    coord.slot_publish_debt_accum[slot.index()] = 14.0;
    coord.slot_last_debt_refill[slot.index()] = Instant::now() - Duration::from_secs(1);
    coord.slot_publish_budget[slot.index()] = 0.0;
    coord.slot_last_budget_refill[slot.index()] = Instant::now();

    coord
        .slot_place_or_reprice(slot, 0.55, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "budget exhausted should suppress sync-driven publish"
    );
    assert_eq!(coord.stats.publish_budget_suppressed, 1);
}

#[tokio::test]
async fn test_glft_sync_publish_tracks_counter_when_published() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    let glft = GlftSignalSnapshot {
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.yes_target = Some(target);
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.70,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(12));
    coord.slot_policy_states[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.30, 5.0, glft));
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(12));
    coord.slot_policy_candidates[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.58, 5.0, glft));
    coord.slot_policy_candidate_since[slot.index()] =
        Some(Instant::now() - Duration::from_secs(12));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.slot_publish_budget[slot.index()] = 2.0;
    coord.slot_last_budget_refill[slot.index()] = Instant::now() - Duration::from_secs(1);
    coord.slot_publish_debt_accum[slot.index()] = 14.0;
    coord.slot_last_debt_refill[slot.index()] = Instant::now() - Duration::from_secs(1);

    coord
        .slot_place_or_reprice(slot, 0.58, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(100), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.side, Side::Yes);
            assert!(target.price > 0.30);
        }
        other => panic!("expected policy-driven publish, got {:?}", other),
    }
    assert_eq!(
        coord.slot_last_publish_reason[slot.index()],
        Some(PolicyPublishCause::Policy)
    );
    assert_eq!(coord.stats.publish_from_policy, 1);
}

#[test]
fn test_glft_policy_same_band_move_does_not_transition() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let glft = live_glft_snapshot();
    let now = Instant::now();

    let baseline = coord.build_slot_quote_policy(slot, 0.30, 5.0, glft);
    coord.slot_policy_states[slot.index()] = Some(baseline);
    coord.slot_policy_since[slot.index()] = Some(now - Duration::from_secs(10));
    coord.slot_policy_candidates[slot.index()] = Some(baseline);
    coord.slot_policy_candidate_since[slot.index()] = Some(now - Duration::from_secs(10));

    let (_policy, transition) = coord.update_slot_quote_policy(slot, 0.35, 5.0, glft, now, true);
    assert_eq!(transition, None, "same price band should not transition");
    assert_eq!(coord.stats.policy_transition_events, 0);
}

#[test]
fn test_glft_policy_same_band_move_syncs_committed_action_price() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let glft = live_glft_snapshot();
    let now = Instant::now();

    let baseline = coord.build_slot_quote_policy(slot, 0.30, 5.0, glft);
    coord.slot_policy_states[slot.index()] = Some(baseline);
    coord.slot_policy_since[slot.index()] = Some(now - Duration::from_secs(10));
    coord.slot_policy_candidates[slot.index()] = Some(baseline);
    coord.slot_policy_candidate_since[slot.index()] = Some(now - Duration::from_secs(10));

    let (policy, transition) = coord.update_slot_quote_policy(slot, 0.35, 5.0, glft, now, true);
    assert_eq!(transition, None, "same band should not transition");
    assert!(
        (policy.action_price - 0.35).abs() < 1e-9,
        "committed action_price should track latest normalized target even without transition"
    );
    assert!(
        (policy.policy_price - baseline.policy_price).abs() < 1e-9,
        "policy band anchor should remain unchanged when transition is None"
    );
}

#[test]
fn test_glft_policy_band_move_without_action_change_does_not_transition() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let glft = live_glft_snapshot();
    let now = Instant::now();

    let mut baseline = coord.build_slot_quote_policy(slot, 0.48, 5.0, glft);
    // Simulate decoupled state after no-transition sync:
    // executable action has already tracked down, but policy band anchor is old.
    baseline.action_price = 0.38;
    coord.slot_policy_states[slot.index()] = Some(baseline);
    coord.slot_policy_since[slot.index()] = Some(now - Duration::from_secs(10));
    coord.slot_policy_candidates[slot.index()] = Some(baseline);
    coord.slot_policy_candidate_since[slot.index()] = Some(now - Duration::from_secs(10));

    let (_policy, transition) = coord.update_slot_quote_policy(slot, 0.38, 5.0, glft, now, true);
    assert_eq!(
        transition, None,
        "price band shift without executable action delta should be treated as no-op"
    );
}

#[test]
fn test_glft_policy_band_move_with_one_tick_action_delta_does_not_transition() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let glft = live_glft_snapshot();
    let now = Instant::now();

    // Aligned bucket width = 12 ticks. 0.59 -> 0.60 crosses policy band,
    // but action delta is only 1 tick and should be treated as no-op.
    let baseline = coord.build_slot_quote_policy(slot, 0.59, 5.0, glft);
    coord.slot_policy_states[slot.index()] = Some(baseline);
    coord.slot_policy_since[slot.index()] = Some(now - Duration::from_secs(10));
    coord.slot_policy_candidates[slot.index()] = Some(baseline);
    coord.slot_policy_candidate_since[slot.index()] = Some(now - Duration::from_secs(10));

    let (_policy, transition) = coord.update_slot_quote_policy(slot, 0.60, 5.0, glft, now, true);
    assert_eq!(
        transition, None,
        "1-tick action delta across policy band should not transition"
    );
}

#[test]
fn test_glft_soft_reset_preserves_policy_state() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::NO_BUY;
    let glft = live_glft_snapshot();
    let policy = coord.build_slot_quote_policy(slot, 0.52, 5.0, glft);

    coord.slot_policy_candidates[slot.index()] = Some(policy);
    coord.slot_policy_candidate_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_policy_states[slot.index()] = Some(policy);
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Tracking);

    coord.soft_reset_slot_publish_state(slot);

    assert_eq!(coord.slot_policy_candidates[slot.index()], Some(policy));
    assert_eq!(coord.slot_policy_states[slot.index()], Some(policy));
    assert_eq!(
        coord.slot_last_regime_seen[slot.index()],
        Some(QuoteRegime::Tracking)
    );
    assert_eq!(coord.stats.soft_reset_count, 1);
}

#[test]
fn test_glft_recovery_publish_dedup_blocks_same_cross_episode() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let first_cross = Instant::now() - Duration::from_millis(100);
    coord.yes_maker_friction.last_cross_reject_ts = Some(first_cross);

    assert!(
        coord.should_publish_recovery_for_slot(slot, Duration::from_secs(2), Instant::now()),
        "first recent cross reject should be eligible for one recovery publish"
    );

    coord.note_recovery_publish_for_slot(slot, Instant::now() - Duration::from_millis(1_500));

    assert!(
        !coord.should_publish_recovery_for_slot(slot, Duration::from_secs(2), Instant::now()),
        "same cross-reject episode must not repeatedly republish recovery"
    );

    coord.yes_maker_friction.last_cross_reject_ts = Some(Instant::now());
    assert!(
        coord.should_publish_recovery_for_slot(slot, Duration::from_secs(2), Instant::now()),
        "a new cross-reject episode should re-arm recovery publish"
    );
}

#[tokio::test]
async fn test_glft_source_recovery_poly_only_short_block_uses_soft_reset_continuity() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let glft = live_glft_snapshot();
    let policy = coord.build_slot_quote_policy(slot, 0.52, 5.0, glft);

    coord.slot_policy_candidates[slot.index()] = Some(policy);
    coord.slot_policy_candidate_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_policy_states[slot.index()] = Some(policy);
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.52,
        size: 5.0,
        reason: BidReason::Provide,
    });
    let old_shadow_since = Instant::now() - Duration::from_secs(2);
    coord.slot_shadow_since[slot.index()] = Some(old_shadow_since);

    let now = Instant::now();
    coord.glft_source_blocked_since = Some(now - Duration::from_secs(2));
    coord.glft_source_blocked_saw_poly = true;
    coord.glft_source_blocked_saw_binance = false;

    coord.update_glft_source_recovery_state(live_glft_snapshot(), now);

    assert!(coord.glft_republish_settle_until.is_some());
    assert_eq!(coord.slot_policy_states[slot.index()], Some(policy));
    assert_eq!(coord.slot_policy_candidates[slot.index()], Some(policy));
    assert_eq!(
        coord.slot_shadow_targets[slot.index()]
            .as_ref()
            .map(|t| t.price),
        Some(0.52)
    );
    assert_eq!(
        coord.slot_shadow_since[slot.index()],
        Some(old_shadow_since)
    );
    assert_eq!(coord.stats.full_reset_count, 0);
    assert_eq!(coord.stats.soft_reset_count, OrderSlot::ALL.len() as u64);
}

#[tokio::test]
async fn test_glft_source_recovery_clears_recovery_publish_state() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    coord.yes_maker_friction.last_cross_reject_ts =
        Some(Instant::now() - Duration::from_millis(100));
    coord.note_recovery_publish_for_slot(slot, Instant::now() - Duration::from_millis(1_500));
    assert!(!coord.should_publish_recovery_for_slot(slot, Duration::from_secs(2), Instant::now()));

    let now = Instant::now();
    coord.glft_source_blocked_since = Some(now - Duration::from_secs(5));
    coord.glft_source_blocked_saw_poly = true;
    coord.update_glft_source_recovery_state(live_glft_snapshot(), now);

    assert!(coord.slot_last_recovery_cross_seen_at[slot.index()].is_none());
    assert!(coord.slot_last_recovery_publish_at[slot.index()].is_none());
    assert!(
        coord.should_publish_recovery_for_slot(slot, Duration::from_secs(2), now),
        "source recovery should clear slot recovery dedup so the next live recovery can publish"
    );
}

#[test]
fn test_glft_full_reset_clears_policy_state() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::NO_BUY;
    let glft = live_glft_snapshot();
    let policy = coord.build_slot_quote_policy(slot, 0.52, 5.0, glft);

    coord.slot_policy_candidates[slot.index()] = Some(policy);
    coord.slot_policy_candidate_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_policy_states[slot.index()] = Some(policy);
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(3));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Tracking);

    coord.full_reset_slot_publish_state(slot);

    assert_eq!(coord.slot_policy_candidates[slot.index()], None);
    assert_eq!(coord.slot_policy_states[slot.index()], None);
    assert_eq!(coord.slot_last_regime_seen[slot.index()], None);
    assert_eq!(coord.stats.soft_reset_count, 1);
    assert_eq!(coord.stats.full_reset_count, 1);
}

// ── evaluate_slot_retention: absent-intent dwell-first protection ──────────────

#[test]
fn test_evaluate_slot_retention_warmup_dwell_fires_on_first_absent_tick() {
    // The warmup dwell must start timing on the very first absent-intent tick.
    // Before the dwell elapses, evaluate_slot_retention must return Retain
    // regardless of how far the order is from trusted_mid.
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    // absent_clear_since is None — first absent tick
    assert!(coord.slot_absent_clear_since[slot.index()].is_none());

    let glft = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Guarded,
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let inv = InventoryState::default();
    let ub = book(0.45, 0.55, 0.45, 0.55);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Retain),
        "First absent tick with Guarded 4s warmup must retain (elapsed=0 < 4000ms)",
    );
    assert!(
        coord.slot_absent_clear_since[slot.index()].is_some(),
        "Timer must be initialized on first absent tick",
    );
}

#[test]
fn test_evaluate_slot_retention_retains_order_at_threshold_after_guarded_dwell() {
    // An order exactly at the Guarded hard_stale threshold (14 ticks) must be
    // retained even after the 4s warmup dwell has elapsed.
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    // price=0.50, trusted_mid=0.64 → dist = |0.50 - 0.64| / 0.01 = 14.0 ticks == threshold
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    // Simulate dwell already elapsed (5s > 4000ms Guarded warmup)
    coord.slot_absent_clear_since[slot.index()] = Some(Instant::now() - Duration::from_secs(5));

    let glft = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Guarded,
        trusted_mid: 0.64,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let inv = InventoryState::default();
    let ub = book(0.45, 0.55, 0.45, 0.55);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Retain),
        "14.0 ticks <= Guarded threshold (14) must retain after dwell expires",
    );
}

#[test]
fn test_evaluate_slot_retention_evicts_order_beyond_threshold_after_aligned_dwell() {
    // After the Aligned warmup (1200ms) elapses, an order more than 16 ticks
    // away from trusted_mid must be evicted with Clear(Reprice, Soft).
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    // price=0.70, trusted_mid=0.50 → dist = floor(|0.70-0.50|/0.01) = 19 ticks > Aligned threshold 16.
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.70,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    // Simulate Aligned warmup elapsed (2s > 1200ms)
    coord.slot_absent_clear_since[slot.index()] = Some(Instant::now() - Duration::from_secs(2));

    let glft = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Aligned,
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let inv = InventoryState::default();
    let ub = book(0.45, 0.55, 0.45, 0.55);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(
            decision,
            RetentionDecision::Clear(CancelReason::Reprice, SlotResetScope::Soft)
        ),
        "19 ticks > Aligned threshold (16) must evict with Clear(Reprice, Soft) after dwell expires",
    );
    assert!(
        coord.slot_absent_clear_since[slot.index()].is_none(),
        "Timer must be cleared after eviction",
    );
}

#[test]
fn test_evaluate_slot_retention_blocked_regime_clears_without_dwell() {
    // Blocked regime must bypass the warmup dwell entirely and return Clear
    // on the very first absent-intent tick, regardless of distance.
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    // absent_clear_since is None — ensure dwell path is never entered
    assert!(coord.slot_absent_clear_since[slot.index()].is_none());

    let glft = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Blocked,
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let inv = InventoryState::default();
    let ub = book(0.45, 0.55, 0.45, 0.55);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Clear(CancelReason::Reprice, _)),
        "Blocked regime must clear immediately without any dwell grace period",
    );
    assert!(
        coord.slot_absent_clear_since[slot.index()].is_none(),
        "Timer must remain None after blocked clear",
    );
}

#[test]
fn test_pair_arb_evaluate_slot_retention_absent_intent_uses_dwell_and_soft_clear() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);

    let inv = InventoryState::default();
    let stable_book = book(0.43, 0.55, 0.45, 0.57);
    let first = coord.evaluate_slot_retention(
        &inv,
        &stable_book,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(first, RetentionDecision::Retain),
        "pair_arb should retain on first absent-intent tick (dwell)"
    );
    assert!(coord.slot_absent_clear_since[slot.index()].is_some());

    coord.slot_absent_clear_since[slot.index()] = Some(Instant::now() - Duration::from_secs(2));
    let after_dwell_safe = coord.evaluate_slot_retention(
        &inv,
        &stable_book,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(after_dwell_safe, RetentionDecision::Retain),
        "pair_arb should retain after dwell when order is still maker-safe"
    );

    // Drive order into unsafe crossed state: buy price >= ask - tick/2
    let crossed_book = book(0.43, 0.44, 0.45, 0.57);
    let unsafe_decision = coord.evaluate_slot_retention(
        &inv,
        &crossed_book,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(
            unsafe_decision,
            RetentionDecision::Clear(CancelReason::Reprice, SlotResetScope::Soft)
        ),
        "pair_arb should soft-clear absent intent once order is no longer safe"
    );
}

#[tokio::test]
async fn test_pair_arb_publish_reason_stats_for_initial_and_reprice() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, _i, _m, _k, mut er, mut coord) = make(config);

    coord
        .slot_place_or_reprice(OrderSlot::YES_BUY, 0.40, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial SetTarget, got {:?}", other),
    }
    assert_eq!(coord.stats.publish_from_initial, 1);
    assert_eq!(
        coord.slot_last_publish_reason[OrderSlot::YES_BUY.index()],
        Some(PolicyPublishCause::Initial)
    );

    coord.slot_last_ts[OrderSlot::YES_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord
        .slot_place_or_reprice(OrderSlot::YES_BUY, 0.35, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "risk-increasing same-side downward drift should be state-driven retain, not continuous reprice"
    );
    assert_eq!(coord.stats.publish_from_policy, 0);
    assert_eq!(
        coord.slot_last_publish_reason[OrderSlot::YES_BUY.index()],
        Some(PolicyPublishCause::Initial)
    );
}

#[tokio::test]
async fn test_pair_arb_directional_retain_does_not_chase_higher_buy_bid() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, _i, _m, _k, mut er, mut coord) = make(config);

    // Initial publish.
    coord
        .slot_place_or_reprice(OrderSlot::YES_BUY, 0.40, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::YES_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.yes_last_ts = Instant::now() - Duration::from_secs(2);

    // Upward price move would normally trigger reprice by threshold, but pair_arb
    // should retain lower bid to avoid chasing up.
    coord
        .slot_place_or_reprice(slot, 0.46, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "pair_arb retain should avoid sending reprice command on higher buy bid"
    );
    assert!(
        coord.stats.retain_hits > 0,
        "directional retain should increment retain_hits"
    );
    let live = coord
        .slot_target(slot)
        .expect("slot target must stay active");
    assert!(
        (live.price - 0.40).abs() < 1e-9,
        "retained order price must remain at lower live bid"
    );
}

#[tokio::test]
async fn test_pair_arb_risk_increasing_retains_when_fresh_target_moves_down_two_ticks() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.45,
        no_qty: 0.0,
        no_avg_cost: 0.0,
        net_diff: 10.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::YES_BUY, 0.18, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial YES SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::YES_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.yes_last_ts = Instant::now() - Duration::from_secs(2);

    coord
        .slot_place_or_reprice(slot, 0.16, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "risk-increasing same-side leg should not continuously reprice within one state bucket"
    );
    let live = coord
        .slot_target(slot)
        .expect("slot target must remain active after retain");
    assert!((live.price - 0.18).abs() < 1e-9);
}

#[tokio::test]
async fn test_pair_arb_state_bucket_change_republishes_higher_buy_bid() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let high_net = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.45,
        no_qty: 0.0,
        no_avg_cost: 0.0,
        net_diff: 10.0,
        ..Default::default()
    };
    let _ = i.send(high_net);

    coord
        .slot_place_or_reprice(OrderSlot::YES_BUY, 0.18, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial SetTarget, got {:?}", other),
    }

    let mid_net = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.45,
        no_qty: 5.0,
        no_avg_cost: 0.42,
        net_diff: 5.0,
        ..Default::default()
    };
    let _ = i.send(mid_net);

    let slot = OrderSlot::YES_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord
        .slot_place_or_reprice(slot, 0.36, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.slot(), slot);
            assert!((target.price - 0.36).abs() < 1e-9);
        }
        other => panic!(
            "expected republish SetTarget on state change, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_pairing_side_holds_between_state_triggers_on_upward_drift() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.58,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 15.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.35, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial NO SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::NO_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_last_ts = Instant::now() - Duration::from_secs(2);

    coord
        .slot_place_or_reprice(slot, 0.39, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Err(_) => {}
        other => panic!(
            "expected no upward reprice for pairing side between state triggers, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_pairing_side_timed_upward_reprice_after_stale_window() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.58,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 15.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.35, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial NO SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::NO_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.no_last_ts = Instant::now() - Duration::from_secs(12);

    // Pairing leg gets a slow timed upward refresh after stale window.
    coord
        .slot_place_or_reprice(slot, 0.39, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.slot(), slot);
            assert!((target.price - 0.39).abs() < 1e-9);
        }
        other => panic!(
            "expected timed upward republish for stale pairing side, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_pairing_side_retains_small_upward_drift_between_triggers() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.58,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 15.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.35, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial NO SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::NO_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_last_ts = Instant::now() - Duration::from_secs(2);

    // Pairing leg remains state/event-driven and should retain between triggers.
    coord
        .slot_place_or_reprice(slot, 0.37, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Err(_) => {}
        other => panic!(
            "expected no upward reprice for pairing side between state triggers, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_pairing_side_retains_small_downward_drift_between_triggers() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.58,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 15.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.40, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial NO SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::NO_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_last_ts = Instant::now() - Duration::from_secs(2);

    coord
        .slot_place_or_reprice(slot, 0.37, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Err(_) => {}
        other => panic!(
            "expected no downward reprice for pairing side between state triggers, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_pairing_side_holds_on_large_downward_drift_between_triggers() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.58,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 15.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.40, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(_))) => {}
        other => panic!("expected initial NO SetTarget, got {:?}", other),
    }

    let slot = OrderSlot::NO_BUY;
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_last_ts = Instant::now() - Duration::from_secs(2);

    coord
        .slot_place_or_reprice(slot, 0.36, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(50), er.recv()).await {
        Err(_) => {}
        other => panic!(
            "expected no downward reprice for pairing side between state triggers, got {:?}",
            other
        ),
    }
}

#[test]
fn test_pair_arb_opposite_slot_blocked_does_not_gate_pair_arb_quotes() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.max_net_diff = 15.0;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.45,
        no_qty: 0.0,
        no_avg_cost: 0.0,
        net_diff: 10.0,
        ..Default::default()
    };
    let _ub = book(0.44, 0.46, 0.54, 0.56);
    let now = Instant::now();
    let no_slot = OrderSlot::NO_BUY;
    coord.pair_arb_slot_blocked_for_ms[no_slot.index()] = 45_000;
    coord.pair_arb_slot_blocked_at[no_slot.index()] = Some(now);

    let yes_intent = StrategyIntent {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.20,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let no_intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.55,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let ofi = OfiSnapshot::default();

    let yes_allowed = coord.slot_quote_allowed(
        &inv,
        OrderSlot::YES_BUY,
        Some(yes_intent),
        false,
        false,
        EndgamePhase::Normal,
        &ofi,
    );
    let no_allowed = coord.slot_quote_allowed(
        &inv,
        OrderSlot::NO_BUY,
        Some(no_intent),
        false,
        false,
        EndgamePhase::Normal,
        &ofi,
    );

    assert!(
        yes_allowed,
        "risk-increasing YES buy should be decided by pair_arb pricing constraints, not opposite-slot fuse"
    );
    assert!(no_allowed, "pairing/reducing NO buy should remain allowed");
}

#[test]
fn test_pair_arb_retention_republishes_pairing_side_when_resting_quote_is_not_post_only_safe() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let slot = OrderSlot::NO_BUY;
    let current = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.59,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.no_target = Some(current);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);

    let inv = InventoryState {
        yes_qty: 20.0,
        yes_avg_cost: 0.30,
        no_qty: 5.0,
        no_avg_cost: 0.55,
        net_diff: 15.0,
        ..Default::default()
    };
    let intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.55,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let decision = coord.evaluate_slot_retention(
        &inv,
        &book(0.29, 0.30, 0.58, 0.59),
        slot,
        Some(intent),
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Republish),
        "pairing side should republish when the resting quote is no longer post-only safe"
    );
}

#[test]
fn test_pair_arb_retention_republishes_risk_increasing_side_when_resting_quote_is_not_post_only_safe(
) {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let slot = OrderSlot::YES_BUY;
    let current = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.40,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.yes_target = Some(current);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);

    let inv = InventoryState {
        yes_qty: 15.0,
        yes_avg_cost: 0.80,
        no_qty: 5.0,
        no_avg_cost: 0.20,
        net_diff: 10.0,
        ..Default::default()
    };
    let intent = StrategyIntent {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.36,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let decision = coord.evaluate_slot_retention(
        &inv,
        &book(0.35, 0.36, 0.64, 0.65),
        slot,
        Some(intent),
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Republish),
        "risk-increasing side should republish when the resting quote is no longer post-only safe"
    );
}

#[test]
fn test_pair_arb_retention_keeps_pairing_side_on_large_upward_drift_without_state_change() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let slot = OrderSlot::NO_BUY;
    let current = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.47,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.no_target = Some(current);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);

    let inv = InventoryState {
        yes_qty: 15.0,
        yes_avg_cost: 0.55,
        no_qty: 5.0,
        no_avg_cost: 0.35,
        net_diff: 10.0,
        ..Default::default()
    };
    let intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.57,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let decision = coord.evaluate_slot_retention(
        &inv,
        &book(0.34, 0.35, 0.74, 0.75),
        slot,
        Some(intent),
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Retain),
        "pairing side should retain between state triggers even on large upward drift"
    );
}

#[test]
fn test_pair_arb_absent_intent_state_change_soft_clears_even_if_maker_safe() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::Yes),
        net_bucket: PairArbNetBucket::Mid,
        risk_open_cutoff_active: false,
    });

    let inv = InventoryState {
        yes_qty: 15.0,
        yes_avg_cost: 0.30,
        no_qty: 5.0,
        no_avg_cost: 0.40,
        net_diff: 10.0,
        ..Default::default()
    };
    let ub = book(0.29, 0.40, 0.59, 0.60);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(
            decision,
            RetentionDecision::Clear(CancelReason::Reprice, SlotResetScope::Soft)
        ),
        "state-key change with absent intent should soft-clear even if old order is still maker-safe"
    );
}

#[test]
fn test_pair_arb_progress_regime_enters_stalled_after_no_progress() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let now = Instant::now();
    coord.pair_arb_progress_state.last_pair_progress_at = Some(now - Duration::from_secs(61));

    let stalled = coord.pair_arb_progress_regime(
        &InventoryState {
            yes_qty: 10.0,
            yes_avg_cost: 0.40,
            no_qty: 0.0,
            no_avg_cost: 0.0,
            net_diff: 10.0,
            ..Default::default()
        },
        now,
    );
    assert_eq!(stalled, PairProgressRegime::Stalled);

    coord.pair_arb_progress_state.last_pair_progress_at = Some(now);
    let healthy = coord.pair_arb_progress_regime(
        &InventoryState {
            yes_qty: 10.0,
            yes_avg_cost: 0.40,
            no_qty: 0.0,
            no_avg_cost: 0.0,
            net_diff: 10.0,
            ..Default::default()
        },
        now,
    );
    assert_eq!(healthy, PairProgressRegime::Healthy);
}

#[test]
fn test_pair_arb_progress_updates_on_working_only_advance() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.bid_size = 5.0;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let now = Instant::now();

    coord.pair_arb_progress_state.last_pair_progress_at = Some(now - Duration::from_secs(120));
    coord.pair_arb_progress_state.last_pair_progress_paired_qty = 0.0;

    let snapshot = InventorySnapshot {
        settled: InventoryState::default(),
        working: InventoryState {
            yes_qty: 5.0,
            yes_avg_cost: 0.41,
            no_qty: 5.0,
            no_avg_cost: 0.47,
            net_diff: 0.0,
            ..Default::default()
        },
        pending_yes_qty: 0.0,
        pending_no_qty: 0.0,
        fragile: true,
    };
    coord.observe_pair_arb_inventory_transition(&snapshot, now);

    assert_eq!(
        coord.pair_arb_progress_state.last_pair_progress_at,
        Some(now),
        "working-only paired progress should refresh stalled timer even when settled is unchanged"
    );
    assert!(
        (coord.pair_arb_progress_state.last_pair_progress_paired_qty - 5.0).abs()
            < PAIR_ARB_NET_EPS
    );
    assert_eq!(
        coord.pair_arb_decision_epoch, 1,
        "working-only inventory transition should still bump decision epoch"
    );
}

#[test]
fn test_pair_arb_progress_delta_scales_with_bid_size() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.bid_size = 10.0; // progress delta = max(10*0.2, 0.25) = 2.0
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let now = Instant::now();

    coord.pair_arb_progress_state.last_pair_progress_at = None;
    coord.pair_arb_progress_state.last_pair_progress_paired_qty = 0.0;

    let small_progress = InventorySnapshot {
        settled: InventoryState::default(),
        working: InventoryState {
            yes_qty: 1.0,
            yes_avg_cost: 0.40,
            no_qty: 1.0,
            no_avg_cost: 0.42,
            net_diff: 0.0,
            ..Default::default()
        },
        pending_yes_qty: 0.0,
        pending_no_qty: 0.0,
        fragile: true,
    };
    coord.observe_pair_arb_inventory_transition(&small_progress, now);
    assert!(
        coord
            .pair_arb_progress_state
            .last_pair_progress_at
            .is_none(),
        "paired progress below bid-size scaled threshold should not refresh progress clock"
    );
    assert!(
        coord
            .pair_arb_progress_state
            .last_pair_progress_paired_qty
            .abs()
            < PAIR_ARB_NET_EPS,
        "paired progress marker should stay unchanged when delta is below threshold"
    );

    let large_progress = InventorySnapshot {
        settled: InventoryState::default(),
        working: InventoryState {
            yes_qty: 2.1,
            yes_avg_cost: 0.40,
            no_qty: 2.1,
            no_avg_cost: 0.42,
            net_diff: 0.0,
            ..Default::default()
        },
        pending_yes_qty: 0.0,
        pending_no_qty: 0.0,
        fragile: true,
    };
    coord.observe_pair_arb_inventory_transition(&large_progress, now + Duration::from_secs(1));
    assert_eq!(
        coord.pair_arb_progress_state.last_pair_progress_at,
        Some(now + Duration::from_secs(1)),
        "paired progress should update once scaled threshold is crossed"
    );
    assert!(
        (coord.pair_arb_progress_state.last_pair_progress_paired_qty - 2.1).abs()
            < PAIR_ARB_NET_EPS
    );
}

#[test]
fn test_pair_arb_fill_recheck_rechecks_absent_intent_even_without_state_change() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);
    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::Yes),
        net_bucket: PairArbNetBucket::High,
        risk_open_cutoff_active: false,
    });
    coord.slot_pair_arb_fill_recheck_pending[slot.index()] = true;

    let inv = InventoryState {
        yes_qty: 15.0,
        yes_avg_cost: 0.20,
        no_qty: 5.0,
        no_avg_cost: 0.49,
        net_diff: 10.0,
        ..Default::default()
    };
    let ub = book(0.29, 0.40, 0.59, 0.60);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(
            decision,
            RetentionDecision::Clear(CancelReason::Reprice, SlotResetScope::Soft)
        ),
        "fill-triggered recheck should soft-clear same-state quote once it becomes inadmissible"
    );
}

#[test]
fn test_pair_arb_absent_intent_no_longer_clears_only_from_stalled_progress() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.20,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.yes_target = Some(target);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::Yes),
        net_bucket: PairArbNetBucket::High,
        risk_open_cutoff_active: false,
    });
    coord.pair_arb_progress_state.last_pair_progress_at =
        Some(Instant::now() - Duration::from_secs(61));

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.30,
        no_qty: 0.0,
        no_avg_cost: 0.0,
        net_diff: 10.0,
        ..Default::default()
    };
    let ub = book(0.19, 0.21, 0.79, 0.80);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Retain),
        "stalled-progress diagnostics alone should not force a clear on pair_arb runtime path"
    );
}

#[test]
fn test_pair_arb_absent_intent_keeps_stalled_pairing_leg() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    let slot = OrderSlot::NO_BUY;
    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.60,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.no_target = Some(target);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::Yes),
        net_bucket: PairArbNetBucket::High,
        risk_open_cutoff_active: false,
    });
    coord.pair_arb_progress_state.last_pair_progress_at =
        Some(Instant::now() - Duration::from_secs(61));

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.30,
        no_qty: 0.0,
        no_avg_cost: 0.0,
        net_diff: 10.0,
        ..Default::default()
    };
    let ub = book(0.19, 0.21, 0.59, 0.61);
    let decision = coord.evaluate_slot_retention(
        &inv,
        &ub,
        slot,
        None,
        CancelReason::Reprice,
        EndgamePhase::Normal,
    );
    assert!(
        matches!(decision, RetentionDecision::Retain),
        "stalled guard must not clear pairing/reducing leg when absent intent"
    );
}

#[tokio::test]
async fn test_pair_arb_state_improvement_reanchors_higher_quote_after_fill() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 5.0,
        yes_avg_cost: 0.46,
        no_qty: 5.0,
        no_avg_cost: 0.49,
        net_diff: 0.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    let slot = OrderSlot::NO_BUY;
    let current = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.37,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.no_target = Some(current);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::No),
        net_bucket: PairArbNetBucket::Low,
        risk_open_cutoff_active: false,
    });
    coord.slot_pair_arb_fill_recheck_pending[slot.index()] = true;

    let ub = book(0.45, 0.46, 0.50, 0.51);
    coord.book = ub;

    coord
        .slot_place_or_reprice(slot, 0.45, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.slot(), slot);
            assert!((target.price - 0.45).abs() < 1e-9);
        }
        other => panic!(
            "expected republish SetTarget after reanchor, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_worsening_reanchors_missing_side_quote() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    config.reprice_threshold = 0.02;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 3.0,
        yes_avg_cost: 0.50,
        no_qty: 14.0,
        no_avg_cost: 0.40,
        net_diff: -11.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    let slot = OrderSlot::YES_BUY;
    let current = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.yes_target = Some(current);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.slot_pair_arb_state_keys[slot.index()] = Some(PairArbStateKey {
        dominant_side: Some(Side::No),
        net_bucket: PairArbNetBucket::Low,
        risk_open_cutoff_active: false,
    });
    coord.slot_pair_arb_fill_recheck_pending[slot.index()] = true;

    let ub = book(0.55, 0.56, 0.44, 0.45);
    coord.book = ub;

    coord
        .slot_place_or_reprice(slot, 0.56, 5.0, BidReason::Provide, None)
        .await;
    match timeout(Duration::from_millis(50), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.slot(), slot);
            let margin = coord.post_only_safety_margin_for(Side::Yes, 0.55, 0.56);
            let expected = (0.56f64).min(0.56 - margin);
            assert!((target.price - expected).abs() < 1e-9);
        }
        other => panic!(
            "expected republish SetTarget after pairing-urgency reanchor, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_pair_arb_stale_target_epoch_is_dropped_before_place() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.debounce_ms = 0;
    let (_o, i, _m, _k, mut er, mut coord) = make(config);
    let inv = InventoryState::default();
    let _ = i.send(inv);
    let slot = OrderSlot::YES_BUY;
    coord.slot_pair_arb_intent_state_keys[slot.index()] =
        Some(coord.pair_arb_state_key(&inv, EndgamePhase::Normal));
    coord.slot_pair_arb_intent_epochs[slot.index()] = Some(1);
    coord.pair_arb_decision_epoch = 2;

    coord
        .slot_place_or_reprice(slot, 0.45, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(50), er.recv()).await.is_err(),
        "stale target epoch should be dropped before any place/reprice command"
    );
    assert_eq!(coord.stats.pair_arb_stale_target_dropped, 1);
}

#[test]
fn test_pair_arb_state_key_disabled_mode_collapses_mid_high_buckets() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.pair_arb.tier_mode = PairArbTierMode::Disabled;
    let (_o, _i, _m, _k, _e, coord) = make(config);

    let flat = InventoryState {
        net_diff: 0.0,
        ..Default::default()
    };
    let low = InventoryState {
        net_diff: 2.0,
        ..Default::default()
    };
    let mid = InventoryState {
        net_diff: 7.0,
        ..Default::default()
    };
    let high = InventoryState {
        net_diff: 12.0,
        ..Default::default()
    };

    assert_eq!(
        coord
            .pair_arb_state_key(&flat, EndgamePhase::Normal)
            .net_bucket,
        PairArbNetBucket::Flat
    );
    assert_eq!(
        coord
            .pair_arb_state_key(&low, EndgamePhase::Normal)
            .net_bucket,
        PairArbNetBucket::Low
    );
    assert_eq!(
        coord
            .pair_arb_state_key(&mid, EndgamePhase::Normal)
            .net_bucket,
        PairArbNetBucket::Low
    );
    assert_eq!(
        coord
            .pair_arb_state_key(&high, EndgamePhase::Normal)
            .net_bucket,
        PairArbNetBucket::Low
    );
}

#[test]
fn test_pair_arb_endgame_phase_change_marks_recheck_pending() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, i, _m, _k, _er, mut coord) = make(config);

    let inv = InventoryState {
        yes_qty: 8.0,
        yes_avg_cost: 0.50,
        no_qty: 10.0,
        no_avg_cost: 0.45,
        net_diff: -2.0,
        ..Default::default()
    };
    let _ = i.send(inv);

    let mut st = coord.init_execution_state(&inv, StrategyQuotes::default());
    st.endgame_phase = EndgamePhase::SoftClose;
    coord.apply_endgame_controls(&inv, &book(0.49, 0.50, 0.50, 0.51), &mut st);

    assert!(coord.slot_pair_arb_fill_recheck_pending[OrderSlot::YES_BUY.index()]);
    assert!(coord.slot_pair_arb_fill_recheck_pending[OrderSlot::NO_BUY.index()]);
}

#[test]
fn test_pair_arb_soft_close_deadband_blocks_both_sides() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    config.bid_size = 5.0;
    let (_o, _i, _m, _k, _er, coord) = make(config);

    let inv = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.50,
        no_qty: 12.0,
        no_avg_cost: 0.45,
        net_diff: -2.0,
        ..Default::default()
    };
    assert!(coord.pair_arb_soft_close_blocks_side(&inv, Side::Yes));
    assert!(coord.pair_arb_soft_close_blocks_side(&inv, Side::No));
}

#[test]
fn test_pair_arb_merge_aware_round_accounting_tracks_realized_pair_metrics() {
    let mut config = cfg();
    config.strategy = StrategyKind::PairArb;
    let (_o, _i, _m, _k, _er, mut coord) = make(config);

    coord.last_settled_inv_snapshot = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.40,
        no_qty: 10.0,
        no_avg_cost: 0.45,
        net_diff: 0.0,
        ..Default::default()
    };
    coord.last_working_inv_snapshot = InventoryState {
        yes_qty: 10.0,
        yes_avg_cost: 0.40,
        no_qty: 10.0,
        no_avg_cost: 0.45,
        net_diff: 0.0,
        ..Default::default()
    };
    coord.observe_pair_arb_inventory_transition(
        &InventorySnapshot {
            settled: InventoryState {
                yes_qty: 5.0,
                yes_avg_cost: 0.40,
                no_qty: 5.0,
                no_avg_cost: 0.45,
                net_diff: 0.0,
                ..Default::default()
            },
            working: InventoryState {
                yes_qty: 5.0,
                yes_avg_cost: 0.40,
                no_qty: 5.0,
                no_avg_cost: 0.45,
                net_diff: 0.0,
                ..Default::default()
            },
            pending_yes_qty: 0.0,
            pending_no_qty: 0.0,
            fragile: false,
        },
        Instant::now(),
    );

    assert!((coord.round_realized_pair_metrics.realized_pair_qty - 5.0).abs() < 1e-9);
    assert!((coord.round_realized_pair_metrics.realized_pair_locked_pnl - 0.75).abs() < 1e-9);
    assert!((coord.round_realized_pair_metrics.merged_cash_released - 5.0).abs() < 1e-9);
}

#[tokio::test]
async fn test_glft_soft_reset_republish_uses_policy_not_initial() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _k, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    coord.book = book(0.45, 0.46, 0.54, 0.55);
    let glft = GlftSignalSnapshot {
        trusted_mid: 0.50,
        ..live_glft_snapshot()
    };
    let _ = g.send(glft);

    let slot = OrderSlot::YES_BUY;
    let current = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(8);
    coord.yes_target = Some(current);
    let policy = coord.build_slot_quote_policy(slot, 0.45, 5.0, glft);
    coord.slot_policy_candidates[slot.index()] = Some(policy);
    coord.slot_policy_candidate_since[slot.index()] = Some(Instant::now() - Duration::from_secs(8));
    coord.slot_policy_states[slot.index()] = Some(policy);
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(8));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(8);

    coord
        .clear_slot_target_with_scope(slot, CancelReason::Reprice, SlotResetScope::Soft)
        .await;
    match timeout(Duration::from_millis(100), er.recv()).await {
        Ok(Some(OrderManagerCmd::ClearTarget {
            slot: clear_slot,
            reason,
        })) => {
            assert_eq!(clear_slot, slot);
            assert_eq!(reason, CancelReason::Reprice);
        }
        other => panic!("expected soft-reset clear first, got {:?}", other),
    }

    coord
        .slot_place_or_reprice(slot, 0.45, 5.0, BidReason::Provide, None)
        .await;

    match timeout(Duration::from_millis(100), er.recv()).await {
        Ok(Some(OrderManagerCmd::SetTarget(target))) => {
            assert_eq!(target.side, Side::Yes);
            assert!(
                (target.price - 0.45).abs() < 1e-9,
                "soft-reset policy republish must use current normalized target, not stale committed price"
            );
        }
        other => panic!(
            "expected policy republish after soft reset, got {:?}",
            other
        ),
    }
    assert_eq!(
        coord.slot_last_publish_reason[slot.index()],
        Some(PolicyPublishCause::Policy)
    );
}

#[test]
fn test_glft_debt_publish_is_step_capped() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, coord) = make_with_glft(config);
    let current_price = 0.30;
    let shadow_target = 0.58;
    let next = coord.glft_apply_debt_release_cap(
        current_price,
        shadow_target,
        Some(QuoteRegime::Aligned),
        8.0,
        6.0,
    );
    let moved_ticks = ((next - current_price) / 0.01).abs();
    assert!(
        moved_ticks <= 10.0 + 1e-9,
        "debt release should stay in bounded small steps (<=10 ticks, got {:.1})",
        moved_ticks
    );
    assert!(next > current_price && next < shadow_target);
}

#[test]
fn test_glft_debt_publish_uses_regime_based_cap() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, coord) = make_with_glft(config);
    let current_price = 0.30;
    let shadow_target = 0.58;
    let aligned_next = coord.glft_apply_debt_release_cap(
        current_price,
        shadow_target,
        Some(QuoteRegime::Aligned),
        8.0,
        6.0,
    );
    let guarded_next = coord.glft_apply_debt_release_cap(
        current_price,
        shadow_target,
        Some(QuoteRegime::Guarded),
        8.0,
        6.0,
    );
    let aligned_ticks = ((aligned_next - current_price) / 0.01).abs();
    let guarded_ticks = ((guarded_next - current_price) / 0.01).abs();
    assert!(
        aligned_ticks >= guarded_ticks,
        "Aligned should allow >= Guarded release step (aligned={:.1}, guarded={:.1})",
        aligned_ticks,
        guarded_ticks
    );
    assert!(
        guarded_ticks <= 7.0 + 1e-9,
        "Guarded release should remain conservative (<=7 ticks, got {:.1})",
        guarded_ticks
    );
}

#[test]
fn test_glft_debt_publish_hard_settle_on_extreme_debt() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, coord) = make_with_glft(config);
    let current_price = 0.30;
    let shadow_target = 0.58;
    let next = coord.glft_apply_debt_release_cap(
        current_price,
        shadow_target,
        Some(QuoteRegime::Aligned),
        24.0,
        20.0,
    );
    assert!(
        (next - shadow_target).abs() <= 1e-9,
        "extreme debt should settle directly to shadow target"
    );
}

#[test]
fn test_glft_debt_accumulator_resets_on_low_water() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    coord.slot_publish_debt_accum[slot.index()] = 9.0;
    coord.slot_last_debt_refill[slot.index()] = Instant::now() - Duration::from_secs(1);
    let next = coord.update_slot_publish_debt_accumulator(
        slot,
        Some(QuoteRegime::Aligned),
        2.0,
        2.5,
        Instant::now(),
    );
    assert!(
        next <= 1e-9,
        "low-water debt should clear accumulator immediately, got {:.3}",
        next
    );
    assert!(
        coord.slot_publish_debt_accum[slot.index()] <= 1e-9,
        "slot debt accumulator should be cleared on low-water"
    );
}

#[tokio::test]
async fn test_glft_debt_path_does_not_autopublish_after_regime_settle() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    let _ = g.send(live_glft_snapshot());

    let slot = OrderSlot::YES_BUY;
    let target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(12);
    coord.yes_target = Some(target);
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.72,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(12));
    let glft = live_glft_snapshot();
    coord.slot_policy_states[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.40, 5.0, glft));
    coord.slot_policy_since[slot.index()] = Some(Instant::now() - Duration::from_secs(7));
    coord.slot_policy_candidates[slot.index()] =
        Some(coord.build_slot_quote_policy(slot, 0.72, 5.0, glft));
    coord.slot_policy_candidate_since[slot.index()] = Some(Instant::now() - Duration::from_secs(7));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Tracking);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_millis(50);

    coord
        .slot_place_or_reprice(slot, 0.72, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "fresh regime transition should suppress debt publish"
    );

    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(3);
    coord
        .slot_place_or_reprice(slot, 0.72, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(40), er.recv()).await.is_err(),
        "settled regime alone should not trigger debt-only autopublish",
    );
}

#[tokio::test]
async fn test_glft_source_recovery_ignores_short_flaps() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    let shadow = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_shadow_targets[slot.index()] = Some(shadow);
    let old_shadow_since = Instant::now() - Duration::from_secs(2);
    coord.slot_shadow_since[slot.index()] = Some(old_shadow_since);
    let now = Instant::now();
    coord.glft_source_blocked_since = Some(now - Duration::from_millis(100));

    coord.update_glft_source_recovery_state(live_glft_snapshot(), now);

    assert!(coord.glft_republish_settle_until.is_none());
    assert_eq!(
        coord.slot_shadow_targets[slot.index()]
            .as_ref()
            .map(|t| t.price),
        Some(0.45)
    );
    assert_eq!(
        coord.slot_shadow_since[slot.index()],
        Some(old_shadow_since)
    );
}

#[tokio::test]
async fn test_glft_source_recovery_resets_shadow_after_persistent_block() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, _g, _k, _er, mut coord) = make_with_glft(config);
    let slot = OrderSlot::YES_BUY;
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(2));
    let now = Instant::now();
    coord.glft_source_blocked_since = Some(now - Duration::from_secs(5));

    coord.update_glft_source_recovery_state(live_glft_snapshot(), now);

    assert!(coord.glft_republish_settle_until.is_some());
    assert!(coord.slot_shadow_targets[slot.index()].is_none());
    assert_eq!(coord.slot_shadow_since[slot.index()], Some(now));
}

#[tokio::test]
async fn test_glft_not_ready_clears_active_slots_before_execution() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));

    let live = live_glft_snapshot();
    let _ = g.send(live);

    let yes_target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let no_target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.54,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(yes_target.clone());
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(no_target.clone());
    coord.slot_last_ts[OrderSlot::YES_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.slot_last_ts[OrderSlot::NO_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.yes_target = Some(yes_target);
    coord.no_target = Some(no_target);

    let mut not_ready = live;
    not_ready.signal_state = GlftSignalState::Assimilating;
    not_ready.fit_status = GlftFitStatus::Bootstrap;
    not_ready.ready = false;
    not_ready.stale = true;
    let _ = g.send(not_ready);

    coord
        .execute_slot_market_making(
            &InventoryState::default(),
            &book(0.45, 0.46, 0.54, 0.55),
            StrategyQuotes::default(),
            false,
            false,
            false,
            false,
        )
        .await;

    let mut cleared_yes = false;
    let mut cleared_no = false;
    for _ in 0..2 {
        match timeout(Duration::from_millis(100), er.recv()).await {
            Ok(Some(OrderManagerCmd::ClearTarget { slot, reason })) => {
                assert_eq!(reason, CancelReason::StaleData);
                if slot == OrderSlot::YES_BUY {
                    cleared_yes = true;
                }
                if slot == OrderSlot::NO_BUY {
                    cleared_no = true;
                }
            }
            other => panic!("expected stale ClearTarget, got {:?}", other),
        }
    }
    assert!(
        cleared_yes && cleared_no,
        "both buy slots should be cleared"
    );
    assert!(coord.slot_target(OrderSlot::YES_BUY).is_none());
    assert!(coord.slot_target(OrderSlot::NO_BUY).is_none());
}

#[tokio::test]
async fn test_glft_blocked_source_stats_split_binance_vs_poly() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, _, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    // Mark readiness once so blocked source counters are attributed on blocked edges.
    let _ = g.send(live_glft_snapshot());
    coord.tick().await;

    let mut blocked = live_glft_snapshot();
    blocked.quote_regime = QuoteRegime::Blocked;
    blocked.reference_health = ReferenceHealth::Blocked;
    blocked.ready = false;
    blocked.stale = true;

    blocked.readiness_blockers.await_binance = true;
    blocked.readiness_blockers.await_poly_book = false;
    let _ = g.send(blocked);
    coord.tick().await;
    assert_eq!(coord.stats.blocked_due_source, 1);
    assert_eq!(coord.stats.blocked_due_binance, 1);
    assert_eq!(coord.stats.blocked_due_poly, 0);
    assert_eq!(coord.stats.blocked_due_divergence, 0);

    // Reset blocked edge so the next blocked tick is counted as a new entry.
    let mut live = live_glft_snapshot();
    live.readiness_blockers.await_binance = false;
    live.readiness_blockers.await_poly_book = false;
    live.quote_regime = QuoteRegime::Aligned;
    let _ = g.send(live);
    coord.tick().await;

    let mut blocked_poly = blocked;
    blocked_poly.readiness_blockers.await_binance = false;
    blocked_poly.readiness_blockers.await_poly_book = true;
    let _ = g.send(blocked_poly);
    coord.tick().await;
    assert_eq!(coord.stats.blocked_due_source, 2);
    assert_eq!(coord.stats.blocked_due_binance, 1);
    assert_eq!(coord.stats.blocked_due_poly, 1);
    assert_eq!(coord.stats.blocked_due_divergence, 0);
}

#[tokio::test]
async fn test_glft_paused_clears_active_slots_before_execution() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));

    let paused = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Blocked,
        drift_mode: DriftMode::Frozen,
        basis_drift_ticks: 12.5,
        reference_health: ReferenceHealth::Blocked,
        hard_basis_unstable: true,
        ..live_glft_snapshot()
    };
    let _ = g.send(paused);

    let yes_target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let no_target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.54,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(yes_target.clone());
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(no_target.clone());
    coord.slot_last_ts[OrderSlot::YES_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.slot_last_ts[OrderSlot::NO_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.yes_target = Some(yes_target);
    coord.no_target = Some(no_target);

    coord
        .execute_slot_market_making(
            &InventoryState::default(),
            &book(0.45, 0.46, 0.54, 0.55),
            StrategyQuotes::default(),
            false,
            false,
            false,
            false,
        )
        .await;

    let mut cleared_yes = false;
    let mut cleared_no = false;
    for _ in 0..2 {
        match timeout(Duration::from_millis(100), er.recv()).await {
            Ok(Some(OrderManagerCmd::ClearTarget { slot, reason })) => {
                assert_eq!(reason, CancelReason::StaleData);
                if slot == OrderSlot::YES_BUY {
                    cleared_yes = true;
                }
                if slot == OrderSlot::NO_BUY {
                    cleared_no = true;
                }
            }
            other => panic!("expected stale ClearTarget, got {:?}", other),
        }
    }
    assert!(
        cleared_yes && cleared_no,
        "both buy slots should be cleared"
    );
    assert!(coord.slot_target(OrderSlot::YES_BUY).is_none());
    assert!(coord.slot_target(OrderSlot::NO_BUY).is_none());
}

#[tokio::test]
async fn test_glft_high_drift_blocks_new_slot_place() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));
    let _ = g.send(GlftSignalSnapshot {
        quote_regime: QuoteRegime::Blocked,
        drift_mode: DriftMode::Frozen,
        basis_drift_ticks: 13.0,
        reference_health: ReferenceHealth::Blocked,
        hard_basis_unstable: true,
        ..live_glft_snapshot()
    });

    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.52, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(20), er.recv()).await.is_err(),
        "glft high drift should block new place commands"
    );
}

#[tokio::test]
async fn test_glft_short_source_block_retains_active_slots_without_clear() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));

    let blocked = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Blocked,
        reference_health: ReferenceHealth::Blocked,
        ready: false,
        stale: true,
        readiness_blockers: GlftReadinessBlockers {
            await_binance: false,
            await_poly_book: true,
            await_fit: false,
            basis_unstable: false,
            sigma_unstable: false,
            min_warmup_not_elapsed: false,
        },
        ..live_glft_snapshot()
    };
    let _ = g.send(blocked);
    coord.glft_source_blocked_since = Some(Instant::now() - Duration::from_millis(500));

    let yes_target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let no_target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.54,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(yes_target.clone());
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(no_target.clone());
    coord.yes_target = Some(yes_target);
    coord.no_target = Some(no_target);

    coord
        .execute_slot_market_making(
            &InventoryState::default(),
            &book(0.45, 0.46, 0.54, 0.55),
            StrategyQuotes::default(),
            false,
            false,
            false,
            false,
        )
        .await;

    assert!(
        timeout(Duration::from_millis(40), er.recv()).await.is_err(),
        "short source block should retain active slots without stale clear"
    );
    assert!(coord.slot_target(OrderSlot::YES_BUY).is_some());
    assert!(coord.slot_target(OrderSlot::NO_BUY).is_some());
}

#[tokio::test]
async fn test_glft_long_source_block_clears_active_slots() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));

    let blocked = GlftSignalSnapshot {
        quote_regime: QuoteRegime::Blocked,
        reference_health: ReferenceHealth::Blocked,
        ready: false,
        stale: true,
        readiness_blockers: GlftReadinessBlockers {
            await_binance: false,
            await_poly_book: true,
            await_fit: false,
            basis_unstable: false,
            sigma_unstable: false,
            min_warmup_not_elapsed: false,
        },
        ..live_glft_snapshot()
    };
    let _ = g.send(blocked);
    coord.glft_source_blocked_since = Some(Instant::now() - Duration::from_secs(3));

    let yes_target = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    let no_target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.54,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(yes_target.clone());
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(no_target.clone());
    coord.yes_target = Some(yes_target);
    coord.no_target = Some(no_target);

    coord
        .execute_slot_market_making(
            &InventoryState::default(),
            &book(0.45, 0.46, 0.54, 0.55),
            StrategyQuotes::default(),
            false,
            false,
            false,
            false,
        )
        .await;

    let mut cleared_yes = false;
    let mut cleared_no = false;
    for _ in 0..2 {
        match timeout(Duration::from_millis(100), er.recv()).await {
            Ok(Some(OrderManagerCmd::ClearTarget { slot, reason })) => {
                assert_eq!(reason, CancelReason::StaleData);
                if slot == OrderSlot::YES_BUY {
                    cleared_yes = true;
                }
                if slot == OrderSlot::NO_BUY {
                    cleared_no = true;
                }
            }
            other => panic!(
                "expected stale ClearTarget under long source block, got {:?}",
                other
            ),
        }
    }
    assert!(cleared_yes && cleared_no);
}

#[tokio::test]
async fn test_glft_frozen_drift_suppresses_buy_publish_even_when_tradeable() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.45, 0.46, 0.54, 0.55));

    let mut frozen = live_glft_snapshot();
    frozen.quote_regime = QuoteRegime::Guarded;
    frozen.drift_mode = DriftMode::Frozen;
    frozen.basis_drift_ticks = 10.2;
    frozen.drift_ewma_ticks = 9.1;
    frozen.reference_health = ReferenceHealth::Guarded;
    let _ = g.send(frozen);

    let slot = OrderSlot::YES_BUY;
    let existing = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(existing.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(3);
    coord.yes_target = Some(existing);

    coord
        .slot_place_or_reprice(slot, 0.55, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(30), er.recv()).await.is_err(),
        "frozen drift should suppress buy publish/reprice commands"
    );
    assert_eq!(
        coord.slot_target(slot).map(|t| t.price),
        Some(0.45),
        "active buy target should remain unchanged while frozen"
    );
}

#[tokio::test]
async fn test_glft_soft_reprice_throttle_skips_small_early_jitter() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.49, 0.50, 0.50, 0.51));
    let mut glft = live_glft_snapshot();
    // Keep NO-side trusted mid near existing quote so small jitter stays non-structural.
    glft.trusted_mid = 0.45;
    let _ = g.send(glft);

    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::NO_BUY.index()] = Some(target.clone());
    coord.slot_last_ts[OrderSlot::NO_BUY.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_target = Some(target);

    // Small move (2 ticks) within first 1.5s should be throttled.
    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.52, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(20), er.recv()).await.is_err(),
        "expected no reprice for early small jitter"
    );

    // Large target-follow drift alone should still not publish without structural debt.
    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.90, 5.0, BidReason::Provide, None)
        .await;
    tokio::time::sleep(Duration::from_millis(3600)).await;
    coord
        .slot_place_or_reprice(OrderSlot::NO_BUY, 0.90, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(80), er.recv()).await.is_err(),
        "expected no publish when only target-follow divergence exists"
    );
}

#[tokio::test]
async fn test_glft_target_follow_debt_does_not_publish_without_structural_misalignment() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.49, 0.50, 0.50, 0.51));
    let _ = g.send(live_glft_snapshot());

    let slot = OrderSlot::NO_BUY;
    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.no_target = Some(target);

    // Prime shadow and directional confirmation path.
    coord
        .slot_place_or_reprice(slot, 0.90, 5.0, BidReason::Provide, None)
        .await;
    tokio::time::sleep(Duration::from_millis(3200)).await;

    let mut published = false;
    for _ in 0..6 {
        coord
            .slot_place_or_reprice(slot, 0.90, 5.0, BidReason::Provide, None)
            .await;
        if let Ok(Some(OrderManagerCmd::SetTarget(_t))) =
            timeout(Duration::from_millis(80), er.recv()).await
        {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    assert!(
        !published,
        "target-follow-only debt should not publish without structural misalignment"
    );
}

#[tokio::test]
async fn test_glft_structural_debt_ignores_shadow_gap_when_trusted_aligned() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.49, 0.50, 0.50, 0.51));
    let mut glft = live_glft_snapshot();
    glft.quote_regime = QuoteRegime::Aligned;
    glft.trusted_mid = 0.50;
    let _ = g.send(glft);

    let slot = OrderSlot::NO_BUY;
    let current = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(current.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(3);
    coord.no_target = Some(current);
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.82,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(4));
    coord.slot_shadow_velocity_tps[slot.index()] = 0.0;
    coord.slot_shadow_last_change_ts[slot.index()] = Some(Instant::now() - Duration::from_secs(5));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(10);

    coord
        .slot_place_or_reprice(slot, 0.82, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(80), er.recv()).await.is_err(),
        "shadow gap alone must not trigger structural debt publish when trusted distance is small"
    );
}

#[tokio::test]
async fn test_glft_structural_debt_waits_until_shadow_velocity_cools() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, m, g, _, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    let _ = m.send(bt(0.49, 0.50, 0.50, 0.51));
    let _ = g.send(live_glft_snapshot());

    let slot = OrderSlot::NO_BUY;
    let target = DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 5.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[slot.index()] = Some(target.clone());
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(3);
    coord.no_target = Some(target);
    coord.slot_shadow_targets[slot.index()] = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.82,
        size: 5.0,
        reason: BidReason::Provide,
    });
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(4));
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Aligned);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(10);
    coord.slot_shadow_velocity_tps[slot.index()] = 30.0;
    coord.slot_shadow_last_change_ts[slot.index()] = Some(Instant::now());

    coord
        .slot_place_or_reprice(slot, 0.82, 5.0, BidReason::Provide, None)
        .await;
    assert!(
        timeout(Duration::from_millis(60), er.recv()).await.is_err(),
        "high shadow velocity should suppress structural debt publish"
    );

    // Allow velocity to decay below the aligned structural cap.
    coord.slot_shadow_last_change_ts[slot.index()] = Some(Instant::now() - Duration::from_secs(25));
    coord.slot_shadow_since[slot.index()] = Some(Instant::now() - Duration::from_secs(4));
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(3);
    coord
        .slot_place_or_reprice(slot, 0.82, 5.0, BidReason::Provide, None)
        .await;
    let cooled_velocity = coord.slot_shadow_velocity_tps[slot.index()];
    assert!(
        cooled_velocity < 10.0,
        "shadow velocity should decay below aligned cap, got {:.2} tps",
        cooled_velocity
    );
}

#[tokio::test]
async fn test_glft_guarded_edge_initial_publish_needs_extra_headroom() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (o, i, _m, g, _k, mut er, mut coord) = make_with_glft(config);
    let _ = o.send(OfiSnapshot::default());
    let _ = i.send(InventoryState::default());
    coord.book = book(0.94, 0.96, 0.04, 0.05);
    coord.last_valid_book = coord.book;
    coord.last_valid_ts_yes = Instant::now();
    coord.last_valid_ts_no = Instant::now();

    let mut glft = live_glft_snapshot();
    glft.quote_regime = QuoteRegime::Guarded;
    glft.drift_mode = DriftMode::Frozen;
    glft.trusted_mid = 0.93;
    glft.synthetic_mid_yes = 0.96;
    let _ = g.send(glft);
    let slot = OrderSlot::NO_BUY;
    coord.no_last_ts = Instant::now() - Duration::from_secs(2);
    coord.slot_last_ts[slot.index()] = Instant::now() - Duration::from_secs(2);
    coord.slot_last_regime_seen[slot.index()] = Some(QuoteRegime::Guarded);
    coord.slot_regime_changed_at[slot.index()] = Instant::now() - Duration::from_secs(10);

    // 1st call only seeds shadow target.
    coord
        .slot_place_or_reprice(slot, 0.01, 5.0, BidReason::Provide, None)
        .await;
    tokio::time::sleep(Duration::from_millis(3600)).await;
    // 2nd call reaches initial publish window, but edge headroom is still fragile.
    coord
        .slot_place_or_reprice(slot, 0.01, 5.0, BidReason::Provide, None)
        .await;

    assert!(
        timeout(Duration::from_millis(60), er.recv()).await.is_err(),
        "guarded edge quote should be suppressed when best_ask barely equals post-only margin"
    );
    assert!(coord.slot_target(slot).is_none());
}

#[tokio::test]
async fn test_glft_slot_rejects_increment_inv_limit_skip_counter() {
    let mut config = cfg();
    config.strategy = StrategyKind::GlftMm;
    let (_o, _i, _m, g, _k, _er, mut coord) = make_with_glft(config);
    let _ = g.send(live_glft_snapshot());

    let inv = InventoryState {
        net_diff: coord.cfg.max_net_diff,
        ..Default::default()
    };
    let mut quotes = StrategyQuotes::default();
    quotes.set(StrategyIntent {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.40,
        size: coord.cfg.bid_size,
        reason: BidReason::Provide,
    });

    coord
        .execute_slot_market_making(
            &inv,
            &book(0.39, 0.41, 0.59, 0.61),
            quotes,
            false,
            false,
            false,
            false,
        )
        .await;

    assert_eq!(
        coord.stats.skipped_inv_limit, 1,
        "slot inventory-limit rejects should be counted for shutdown observability"
    );
}

#[test]
fn test_incremental_hedge_ceiling_uses_existing_inventory_budget() {
    let (_, _, _, _, _, c) = make(cfg());
    let inv = InventoryState {
        yes_qty: 25.5,
        no_qty: 15.5,
        yes_avg_cost: 0.3929,
        no_avg_cost: 0.4301,
        net_diff: 10.0,
        portfolio_cost: 0.8229,
    };

    let hedge_size = 10.0;
    let target = 0.98;
    let static_ceiling = target - inv.yes_avg_cost;
    let incremental_ceiling = c.incremental_hedge_ceiling(&inv, Side::No, hedge_size, target);

    // New ceiling should be materially higher than static subtraction when
    // hedge-side inventory already exists at lower average cost.
    assert!(incremental_ceiling > static_ceiling + 1e-6);
    assert!((incremental_ceiling - 0.83045).abs() < 1e-4);
}

#[test]
fn test_incremental_hedge_ceiling_respects_target_after_trade() {
    let (_, _, _, _, _, c) = make(cfg());
    let inv = InventoryState {
        yes_qty: 25.5,
        no_qty: 15.5,
        yes_avg_cost: 0.3929,
        no_avg_cost: 0.4301,
        net_diff: 10.0,
        portfolio_cost: 0.8229,
    };

    let hedge_size = 10.0;
    let target = 1.02; // rescue cap
    let ceiling = c.incremental_hedge_ceiling(&inv, Side::No, hedge_size, target);
    let no_cost_after = inv.no_qty * inv.no_avg_cost + hedge_size * ceiling;
    let no_avg_after = no_cost_after / (inv.no_qty + hedge_size);
    assert!(inv.yes_avg_cost + no_avg_after <= target + 1e-9);
}

#[test]
fn test_inventory_metrics_for_unbalanced_position() {
    let (_, _, _, _, _, c) = make(cfg());
    let inv = InventoryState {
        yes_qty: 10.0,
        no_qty: 8.0,
        yes_avg_cost: 0.42,
        no_avg_cost: 0.45,
        net_diff: 2.0,
        portfolio_cost: 0.87,
    };
    let m = c.derive_inventory_metrics(&inv);

    assert!((m.paired_qty - 8.0).abs() < 1e-9);
    assert!((m.pair_cost - 0.87).abs() < 1e-9);
    assert!((m.paired_locked_pnl - 1.04).abs() < 1e-9);
    assert!((m.total_spent - 7.8).abs() < 1e-9);
    assert!((m.worst_case_outcome_pnl - 0.2).abs() < 1e-9);
    assert_eq!(m.dominant_side, Some(Side::Yes));
    assert!((m.residual_qty - 2.0).abs() < 1e-9);
    assert!((m.residual_inventory_value - 0.84).abs() < 1e-9);
}

#[test]
fn test_inventory_metrics_for_single_side_position() {
    let (_, _, _, _, _, c) = make(cfg());
    let inv = InventoryState {
        yes_qty: 5.0,
        no_qty: 0.0,
        yes_avg_cost: 0.30,
        no_avg_cost: 0.0,
        net_diff: 5.0,
        portfolio_cost: 0.0,
    };
    let m = c.derive_inventory_metrics(&inv);

    assert!(m.paired_qty.abs() < 1e-9);
    assert!(m.pair_cost.abs() < 1e-9);
    assert!(m.paired_locked_pnl.abs() < 1e-9);
    assert!((m.total_spent - 1.5).abs() < 1e-9);
    assert!((m.worst_case_outcome_pnl + 1.5).abs() < 1e-9);
    assert_eq!(m.dominant_side, Some(Side::Yes));
    assert!((m.residual_qty - 5.0).abs() < 1e-9);
    assert!((m.residual_inventory_value - 1.5).abs() < 1e-9);
}

#[test]
fn test_outcome_floor_blocks_expensive_provide_buy() {
    let mut c = cfg();
    c.max_net_diff = 2.0;
    c.pair_target = 0.50; // floor = -1.0
    let (_, _, _, _, _, coord) = make(c);
    let inv = InventoryState::default();
    let intent = StrategyIntent {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.80,
        size: 2.0,
        reason: BidReason::Provide,
    };
    assert!(
        !coord.can_place_strategy_intent(&inv, Some(intent)),
        "expensive buy should be blocked when projected worst-case breaches floor"
    );
}

#[test]
fn test_outcome_floor_allows_recovery_buy_when_already_below_floor() {
    let mut c = cfg();
    c.max_net_diff = 6.0;
    c.pair_target = 0.50; // floor = -3.0
    let (_, _, _, _, _, coord) = make(c);
    let inv = InventoryState {
        yes_qty: 5.0,
        yes_avg_cost: 0.80,
        net_diff: 5.0,
        ..Default::default()
    };
    // Buying NO at 0.20 improves worst-case even though it remains below floor.
    assert!(
        coord.passes_outcome_floor_for_buy(&inv, Side::No, 1.0, 0.20, BidReason::Hedge),
        "below-floor state should allow non-worsening/recovery buys"
    );
}

#[test]
fn test_glft_pair_cost_guard_blocks_newly_bad_pair_even_if_rebalancing() {
    let mut c = cfg();
    c.strategy = StrategyKind::GlftMm;
    c.pair_target = 0.98;
    let (_, _, _, _, _, coord) = make(c);
    let inv = InventoryState {
        yes_qty: 5.0,
        no_qty: 0.0,
        yes_avg_cost: 0.80,
        no_avg_cost: 0.0,
        net_diff: 5.0,
        portfolio_cost: 0.0,
    };
    let intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.30,
        size: 5.0,
        reason: BidReason::Provide,
    };
    assert!(
        !coord.can_place_strategy_intent(&inv, Some(intent)),
        "GLFT should block opening a newly paired position above pair_target"
    );
}

#[test]
fn test_glft_pair_cost_guard_allows_repair_when_already_above_target() {
    let mut c = cfg();
    c.strategy = StrategyKind::GlftMm;
    c.pair_target = 0.98;
    let (_, _, _, _, _, coord) = make(c);
    let inv = InventoryState {
        yes_qty: 5.0,
        no_qty: 5.0,
        yes_avg_cost: 0.80,
        no_avg_cost: 0.35,
        net_diff: 0.0,
        portfolio_cost: 1.15,
    };
    let intent = StrategyIntent {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.10,
        size: 5.0,
        reason: BidReason::Provide,
    };
    assert!(
        coord.can_place_strategy_intent(&inv, Some(intent)),
        "GLFT should allow buy that strictly improves an already-bad pair cost"
    );
}

// ── Per-side toxicity guard ──

#[tokio::test]
async fn test_toxic_cancels_only_toxic_side() {
    let (o, _i, m, _k, mut e, mut coord) = make(cfg());
    let existing = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(existing.clone());
    coord.yes_target = Some(existing);
    coord.no_target = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 2.0,
        reason: BidReason::Provide,
    });

    // Only YES is toxic — only YES should be canceled.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: -100.0,
            buy_volume: 0.0,
            sell_volume: 100.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    assert!(c1.is_ok());
    match c1.unwrap() {
        Some(OrderManagerCmd::ClearTarget { slot, reason }) => {
            assert_eq!(slot, OrderSlot::YES_BUY);
            assert_eq!(reason, CancelReason::ToxicFlow);
        }
        _ => panic!("expected YES clear on toxic side"),
    }

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_cancel_stats_track_toxic_and_inventory_reasons() {
    let mut c = cfg();
    c.dry_run = true;
    let (_o, _i, _m, _k, _er, mut coord) = make(c);

    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    });
    coord
        .clear_slot_target(OrderSlot::YES_BUY, CancelReason::ToxicFlow)
        .await;
    assert_eq!(coord.stats.cancel_toxic, 1);

    coord.no_target = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.52,
        size: 2.0,
        reason: BidReason::Provide,
    });
    coord
        .clear_slot_target(OrderSlot::NO_BUY, CancelReason::InventoryLimit)
        .await;
    assert_eq!(coord.stats.cancel_inv, 1);
}

#[tokio::test]
async fn test_other_side_can_still_quote_when_one_side_toxic() {
    let (o, _i, m, _k, mut e, mut coord) = make(with_strategy(cfg(), StrategyKind::PairArb));

    // NO is toxic, YES is healthy: coordinator should still place YES bid.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi::default(),
        no: SideOfi {
            ofi_score: -80.0,
            buy_volume: 0.0,
            sell_volume: 80.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let c = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    assert!(c.is_ok());
    match c.unwrap() {
        Some(OrderManagerCmd::SetTarget(target)) => {
            assert_eq!(target.side, Side::Yes);
            assert!(target.price > 0.0);
            assert_eq!(target.reason, BidReason::Provide);
        }
        _ => panic!("expected YES target while NO side is toxic"),
    }

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_toxic_cancel_with_empty_book() {
    let (o, _i, m, _k, mut e, mut coord) = make(cfg());
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    });
    coord.no_target = Some(DesiredTarget {
        side: Side::No,
        direction: TradeDirection::Buy,
        price: 0.50,
        size: 2.0,
        reason: BidReason::Provide,
    });

    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: -200.0,
            buy_volume: 0.0,
            sell_volume: 200.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.0, 0.0, 0.0, 0.0));

    let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    assert!(c1.is_ok());
    match c1.unwrap() {
        Some(OrderManagerCmd::ClearTarget { slot, reason }) => {
            assert_eq!(slot, OrderSlot::YES_BUY);
            assert_eq!(reason, CancelReason::ToxicFlow);
        }
        _ => panic!("expected YES clear even when book is empty"),
    }

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_toxic_does_not_clear_existing_hedge_target() {
    let (o, _i, m, _k, mut e, mut coord) = make(cfg());
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Hedge,
    });

    // YES is toxic, but existing hedge target on YES should not be force-canceled.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: -120.0,
            buy_volume: 0.0,
            sell_volume: 120.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let mut saw_bad_clear = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(40), e.recv()).await {
        if let OrderManagerCmd::ClearTarget { slot, reason } = cmd {
            if slot == OrderSlot::YES_BUY && reason == CancelReason::ToxicFlow {
                saw_bad_clear = true;
                break;
            }
        }
    }
    assert!(
        !saw_bad_clear,
        "Hedge target on toxic side should not be canceled by toxic guard"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_endgame_freeze_allows_derisk_taker() {
    let mut c = cfg();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 5);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 20;
    c.endgame_freeze_secs = 10;
    c.bid_size = 5.0;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        yes_avg_cost: 0.60,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.50, 0.52, 0.40, 0.42));

    let mut saw_taker = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(120), e.recv()).await {
        if let OrderManagerCmd::OneShotTakerHedge {
            side,
            direction,
            size,
            ..
        } = cmd
        {
            if side == Side::Yes && direction == TradeDirection::Sell && (size - 2.0).abs() < 1e-9 {
                saw_taker = true;
                break;
            }
        }
    }
    assert!(
        saw_taker,
        "Freeze phase should still allow one-shot de-risk taker hedge"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_endgame_soft_close_blocks_only_risk_increasing_provide() {
    let mut c = cfg();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 40);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 20;
    c.endgame_freeze_secs = 5;
    // Make small net_diff hedge ineligible, so we test provide gating only.
    c.min_order_size = 10.0;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let mut saw_yes_provide = false;
    let mut saw_no_provide = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(120), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            if target.reason == BidReason::Provide {
                if target.side == Side::Yes {
                    saw_yes_provide = true;
                } else if target.side == Side::No {
                    saw_no_provide = true;
                }
            }
        }
    }
    assert!(
        !saw_yes_provide,
        "SoftClose should block risk-increasing YES provide when net_diff is YES-heavy"
    );
    assert!(
        saw_no_provide,
        "SoftClose should allow risk-non-increasing NO provide for inventory repair"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_pair_arb_soft_close_deadband_clears_existing_buy_target() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 40);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 20;
    c.endgame_freeze_secs = 5;
    c.min_order_size = 10.0;

    let (_o, i, m, _k, mut e, mut coord) = make(c);
    let existing = DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    };
    coord.slot_targets[OrderSlot::YES_BUY.index()] = Some(existing.clone());
    coord.yes_target = Some(existing);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let mut reason = None;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(150), e.recv()).await {
        if let OrderManagerCmd::ClearTarget { slot, reason: r } = cmd {
            if slot == OrderSlot::YES_BUY {
                reason = Some(r);
                break;
            }
        }
    }
    assert_eq!(
        reason,
        Some(CancelReason::Reprice),
        "pair_arb soft-close deadband should clear existing buy targets when residual is tiny"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_pair_arb_soft_close_deadband_blocks_new_buys_when_flat() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 40);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 20;
    c.endgame_freeze_secs = 5;
    c.min_order_size = 10.0;

    let (_o, i, m, _k, mut e, mut coord) = make(c);
    let _ = i.send(InventoryState::default());

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let mut saw_yes_provide = false;
    let mut saw_no_provide = false;
    let mut saw_endgame_clear = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(150), e.recv()).await {
        match cmd {
            OrderManagerCmd::SetTarget(target) if target.reason == BidReason::Provide => {
                if target.side == Side::Yes {
                    saw_yes_provide = true;
                } else if target.side == Side::No {
                    saw_no_provide = true;
                }
            }
            OrderManagerCmd::ClearTarget { reason, .. }
                if reason == CancelReason::EndgameRiskGate =>
            {
                saw_endgame_clear = true;
            }
            _ => {}
        }
    }

    assert!(
        !saw_yes_provide && !saw_no_provide,
        "pair_arb soft-close deadband should block new buy provides when inventory is flat"
    );
    assert!(
        !saw_endgame_clear,
        "pair_arb soft-close should not clear both buy slots when net_diff is flat"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_hard_close_entry_keep_mode_skips_taker() {
    let mut c = cfg();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 20);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 30; // already in hard-close
    c.endgame_freeze_secs = 2;
    c.endgame_edge_keep_mult = 1.5;
    c.endgame_edge_exit_mult = 1.25;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        yes_avg_cost: 0.40,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    // ratio = 0.70 / 0.40 = 1.75 => keep-mode
    let _ = m.send(bt(0.70, 0.72, 0.20, 0.22));

    let mut saw_taker = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(120), e.recv()).await {
        if matches!(cmd, OrderManagerCmd::OneShotTakerHedge { .. }) {
            saw_taker = true;
            break;
        }
    }
    assert!(
        !saw_taker,
        "HardClose keep-mode should preserve directional inventory and skip taker"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_hard_close_edge_drop_triggers_full_taker_with_abs_net_size() {
    let mut c = cfg();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 20);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 30; // already in hard-close
    c.endgame_freeze_secs = 2;
    c.endgame_edge_keep_mult = 1.5;
    c.endgame_edge_exit_mult = 1.25;
    c.endgame_maker_repair_min_secs = 25; // remaining~20s => skip maker-repair, force taker
    c.bid_size = 5.0;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        yes_avg_cost: 0.40,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    // Tick-1: enter keep-mode (ratio=1.75)
    let _ = m.send(bt(0.70, 0.72, 0.20, 0.22));
    let _ = timeout(Duration::from_millis(80), e.recv()).await;

    // Tick-2: ratio drops below exit (0.45 / 0.40 = 1.125)
    let _ = m.send(bt(0.45, 0.47, 0.30, 0.32));

    let mut got = None;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(160), e.recv()).await {
        if let OrderManagerCmd::OneShotTakerHedge {
            side,
            direction,
            size,
            ..
        } = cmd
        {
            got = Some((side, direction, size));
            break;
        }
    }
    let (side, direction, size) = got.expect("Expected OneShotTakerHedge after edge drop");
    assert_eq!(side, Side::Yes);
    assert_eq!(direction, TradeDirection::Sell);
    assert!(
        (size - 2.0).abs() < 1e-9,
        "t-30 one-shot size must use abs(net_diff), not bid_size"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_hard_close_entry_below_keep_prefers_maker_repair_when_time_allows() {
    let mut c = cfg();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 20);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 30;
    c.endgame_freeze_secs = 2;
    c.endgame_maker_repair_min_secs = 5; // remaining~20s => maker-repair allowed
    c.endgame_edge_keep_mult = 1.5;
    c.endgame_edge_exit_mult = 1.25;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        net_diff: 2.0,
        yes_qty: 2.0,
        yes_avg_cost: 0.40,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    // ratio = 0.45 / 0.40 = 1.125 < keep; in hard-close, but time budget allows maker repair.
    let _ = m.send(bt(0.45, 0.47, 0.30, 0.32));

    let mut saw_taker = false;
    let mut saw_no_hedge = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(160), e.recv()).await {
        match cmd {
            OrderManagerCmd::OneShotTakerHedge { .. } => {
                saw_taker = true;
            }
            OrderManagerCmd::SetTarget(target) => {
                if target.side == Side::No && target.reason == BidReason::Hedge {
                    saw_no_hedge = true;
                }
            }
            _ => {}
        }
    }
    assert!(
        !saw_taker,
        "HardClose below keep should prefer maker repair when remaining time budget allows"
    );
    assert!(
        saw_no_hedge,
        "Maker repair path should keep opposite-side hedge quoting alive"
    );

    drop(m);
    let _ = h.await;
}

// ── Balanced mid pricing ──

#[tokio::test]
async fn test_balanced_mid_pricing() {
    let (_o, _i, m, _k, mut e, mut coord) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;

    let mut prices = std::collections::HashMap::new();
    if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c1 {
        prices.insert(target.side, target.price);
    }
    if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c2 {
        prices.insert(target.side, target.price);
    }
    // Tight spread (2 ticks) triggers extra maker safety margin:
    // ask=0.46, margin=(2+1)*0.01 => strict clamp to 0.43.
    assert!((prices[&Side::Yes] - 0.43).abs() < 1e-9);
    assert!((prices[&Side::No] - 0.50).abs() < 1e-9);

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_inventory_cost_clamp_disables_side_when_ceiling_below_tick() {
    let (_o, i, m, _k, mut e, coord) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let h = tokio::spawn(coord.run());
    let _ = i.send(InventoryState {
        no_qty: 5.0,
        no_avg_cost: 0.975, // pair_target - avg = 0.005 < tick(0.01) -> disable YES provide
        ..Default::default()
    });
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));

    let mut saw_yes = false;
    let mut saw_no = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(120), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            if target.side == Side::Yes {
                saw_yes = true;
            } else if target.side == Side::No {
                saw_no = true;
            }
        }
    }

    assert!(
        !saw_yes,
        "YES should be disabled when inventory clamp ceiling is below tick"
    );
    assert!(
        saw_no,
        "NO side should remain quotable under pure hard-constraint pair_arb semantics"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_balanced_excess_mid_capped() {
    let (_o, _i, m, _k, mut e, coord) = make(with_strategy(cfg(), StrategyKind::PairArb));
    let h = tokio::spawn(coord.run());
    // mid_yes=0.52, mid_no=0.50, sum=1.02 > 0.98
    let _ = m.send(bt(0.50, 0.54, 0.48, 0.52));
    let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    let mut prices = std::collections::HashMap::new();
    if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c1 {
        prices.insert(target.side, target.price);
    }
    if let Ok(Some(OrderManagerCmd::SetTarget(target))) = c2 {
        prices.insert(target.side, target.price);
    }
    if prices.contains_key(&Side::Yes) && prices.contains_key(&Side::No) {
        assert!(prices[&Side::Yes] + prices[&Side::No] <= 0.98 + 1e-9);
    } else {
        assert!(
            prices.is_empty(),
            "pair_arb should either quote both sides under cap or stay silent when utility threshold is not met"
        );
    }

    drop(m);
    let _ = h.await;
}

#[test]
fn test_pair_arb_ofi_hot_softens_same_side_risk_increasing_buy() {
    let c = with_strategy(cfg(), StrategyKind::PairArb);
    let inv = InventoryState::default();
    let b = book(0.44, 0.46, 0.48, 0.52);

    let base = pair_arb_quotes(c.clone(), inv, b, None);
    let hot = pair_arb_quotes(
        c,
        inv,
        b,
        Some(OfiSnapshot {
            yes: SideOfi {
                is_hot: true,
                heat_score: 2.0,
                ..Default::default()
            },
            ..Default::default()
        }),
    );

    assert!(
        base.buy_for(Side::Yes).is_some(),
        "base YES buy should exist"
    );
    assert!(
        hot.buy_for(Side::Yes).is_some(),
        "hot YES buy should still exist"
    );
    assert_eq!(hot.diagnostics.pair_arb_ofi_softened_quotes, 1);
    assert_eq!(hot.diagnostics.pair_arb_ofi_suppressed_quotes, 0);
}

#[test]
fn test_pair_arb_ofi_toxic_softens_same_side_risk_increasing_buy() {
    let c = with_strategy(cfg(), StrategyKind::PairArb);
    let inv = InventoryState::default();
    let b = book(0.44, 0.46, 0.48, 0.52);

    let base = pair_arb_quotes(c.clone(), inv, b, None);
    let toxic = pair_arb_quotes(
        c,
        inv,
        b,
        Some(OfiSnapshot {
            yes: SideOfi {
                is_toxic: true,
                toxic_buy: true,
                heat_score: 2.0,
                ..Default::default()
            },
            ..Default::default()
        }),
    );

    assert!(
        base.buy_for(Side::Yes).is_some(),
        "base YES buy should exist"
    );
    assert!(
        toxic.buy_for(Side::Yes).is_some(),
        "toxic YES buy should still exist when not saturated"
    );
    assert_eq!(toxic.diagnostics.pair_arb_ofi_softened_quotes, 1);
    assert_eq!(toxic.diagnostics.pair_arb_ofi_suppressed_quotes, 0);
}

#[test]
fn test_pair_arb_ofi_saturated_suppresses_same_side_risk_increasing_buy() {
    let c = with_strategy(cfg(), StrategyKind::PairArb);
    let inv = InventoryState::default();
    let b = book(0.44, 0.46, 0.48, 0.52);

    let saturated = pair_arb_quotes(
        c,
        inv,
        b,
        Some(OfiSnapshot {
            yes: SideOfi {
                is_toxic: true,
                toxic_buy: true,
                saturated: true,
                heat_score: 3.0,
                ..Default::default()
            },
            ..Default::default()
        }),
    );

    assert!(saturated.buy_for(Side::Yes).is_none());
    assert!(saturated.buy_for(Side::No).is_some());
    assert_eq!(saturated.diagnostics.pair_arb_ofi_softened_quotes, 0);
    assert_eq!(saturated.diagnostics.pair_arb_ofi_suppressed_quotes, 1);
}

#[test]
fn test_pair_arb_pairing_buy_ignores_toxic_ofi() {
    let c = with_strategy(cfg(), StrategyKind::PairArb);
    let inv = InventoryState {
        no_qty: 4.0,
        no_avg_cost: 0.30,
        net_diff: -4.0,
        ..Default::default()
    };
    let b = book(0.63, 0.65, 0.18, 0.20);

    let base = pair_arb_quotes(c.clone(), inv, b, None);
    let toxic = pair_arb_quotes(
        c,
        inv,
        b,
        Some(OfiSnapshot {
            yes: SideOfi {
                is_toxic: true,
                toxic_buy: true,
                saturated: true,
                heat_score: 3.0,
                ..Default::default()
            },
            ..Default::default()
        }),
    );

    let base_yes = base.buy_for(Side::Yes).expect("base pairing YES buy").price;
    let toxic_yes = toxic
        .buy_for(Side::Yes)
        .expect("toxic pairing YES buy")
        .price;
    assert!((base_yes - toxic_yes).abs() < 1e-9);
    assert_eq!(toxic.diagnostics.pair_arb_ofi_softened_quotes, 0);
    assert_eq!(toxic.diagnostics.pair_arb_ofi_suppressed_quotes, 0);
}

#[tokio::test]
async fn test_dip_buy_quotes_only_discounted_side() {
    let mut c = cfg();
    c.strategy = StrategyKind::DipBuy;
    c.dip_buy_max_entry_price = 0.20;

    let (_o, _i, m, _k, mut e, coord) = make(c);
    let h = tokio::spawn(coord.run());

    // YES side is discounted (<0.20), NO is expensive.
    let _ = m.send(bt(0.15, 0.17, 0.70, 0.72));

    let mut saw_yes = false;
    let mut saw_no = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(120), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            if target.side == Side::Yes {
                saw_yes = true;
            } else if target.side == Side::No {
                saw_no = true;
            }
        }
    }
    assert!(saw_yes, "dip_buy should quote discounted YES side");
    assert!(!saw_no, "dip_buy should not quote expensive NO side");

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_dip_buy_clears_old_provide_when_no_entry() {
    let mut c = cfg();
    c.strategy = StrategyKind::DipBuy;
    c.dip_buy_max_entry_price = 0.20;

    let (_o, _i, m, _k, mut e, mut coord) = make(c);
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.14,
        size: 2.0,
        reason: BidReason::Provide,
    });

    let h = tokio::spawn(coord.run());
    // Neither side under dip cap anymore.
    let _ = m.send(bt(0.30, 0.32, 0.70, 0.72));

    let got = timeout(Duration::from_millis(120), e.recv())
        .await
        .ok()
        .flatten();
    match got {
        Some(OrderManagerCmd::ClearTarget { slot, reason }) => {
            assert_eq!(slot, OrderSlot::YES_BUY);
            assert_eq!(reason, CancelReason::Reprice);
        }
        other => panic!("expected ClearTarget YES/Reprice, got {:?}", other),
    }

    drop(m);
    let _ = h.await;
}

#[test]
fn test_phase_builder_flat_quotes_only_cheap_side() {
    let mut c = cfg();
    c.dip_buy_max_entry_price = 0.20;

    let quotes = phase_builder_quotes(c, InventoryState::default(), book(0.08, 0.10, 0.31, 0.33));

    let yes = quotes.yes_buy.expect("expected YES provide quote");
    assert_eq!(yes.side, Side::Yes);
    assert_eq!(yes.direction, TradeDirection::Buy);
    assert_eq!(yes.reason, BidReason::Provide);
    assert!((yes.size - 2.0).abs() < 1e-9);
    assert!(
        quotes.no_buy.is_none(),
        "flat phase should quote only one cheap side"
    );
}

#[test]
fn test_phase_builder_flat_skips_when_gap_too_small() {
    let mut c = cfg();
    c.dip_buy_max_entry_price = 0.20;

    let quotes = phase_builder_quotes(c, InventoryState::default(), book(0.08, 0.10, 0.09, 0.11));

    assert!(quotes.yes_buy.is_none());
    assert!(quotes.no_buy.is_none());
}

#[test]
fn test_phase_builder_build_continues_same_side_only_below_avg_cost() {
    let mut c = cfg();
    c.dip_buy_max_entry_price = 0.20;

    let quotes = phase_builder_quotes(
        c,
        InventoryState {
            yes_qty: 4.0,
            yes_avg_cost: 0.12,
            net_diff: 4.0,
            ..Default::default()
        },
        book(0.09, 0.11, 0.70, 0.72),
    );

    let yes = quotes.yes_buy.expect("expected YES build quote");
    assert_eq!(yes.side, Side::Yes);
    assert_eq!(yes.direction, TradeDirection::Buy);
    assert!(
        quotes.no_buy.is_none(),
        "build phase should not fall back to dual provide"
    );
}

#[test]
fn test_phase_builder_pair_phase_stops_provide_at_build_budget() {
    let mut c = cfg();
    c.dip_buy_max_entry_price = 0.20;

    let quotes = phase_builder_quotes(
        c,
        InventoryState {
            yes_qty: 5.0,
            yes_avg_cost: 0.12,
            net_diff: 5.0,
            ..Default::default()
        },
        book(0.09, 0.11, 0.70, 0.72),
    );

    assert!(quotes.yes_buy.is_none());
    assert!(quotes.no_buy.is_none());
}

#[test]
fn test_phase_builder_reset_band_returns_to_flat_logic() {
    let mut c = cfg();
    c.dip_buy_max_entry_price = 0.20;

    let quotes = phase_builder_quotes(
        c,
        InventoryState {
            yes_qty: 3.0,
            no_qty: 2.0,
            yes_avg_cost: 0.18,
            no_avg_cost: 0.30,
            net_diff: 1.0,
            ..Default::default()
        },
        book(0.17, 0.19, 0.08, 0.10),
    );

    assert!(
        quotes.yes_buy.is_none(),
        "reset band should discard prior dominant side"
    );
    let no = quotes.no_buy.expect("expected NO flat quote after reset");
    assert_eq!(no.side, Side::No);
    assert_eq!(no.direction, TradeDirection::Buy);
}

#[test]
fn test_simulate_buy_is_pure_and_projects_metrics() {
    let (_, _, _, _, _, coord) = make(cfg());
    let inv = InventoryState {
        yes_qty: 2.0,
        no_qty: 1.0,
        yes_avg_cost: 0.30,
        no_avg_cost: 0.40,
        net_diff: 1.0,
        portfolio_cost: 0.70,
    };
    let original = inv;
    let projected = coord
        .simulate_buy(&inv, Side::No, 1.5, 0.20)
        .expect("projected metrics");

    assert_eq!(inv.yes_qty, original.yes_qty);
    assert_eq!(inv.no_qty, original.no_qty);
    assert!((projected.projected_yes_qty - 2.0).abs() < 1e-9);
    assert!((projected.projected_no_qty - 2.5).abs() < 1e-9);
    assert!((projected.projected_total_cost - 1.3).abs() < 1e-9);
    assert!((projected.projected_abs_net_diff - 0.5).abs() < 1e-9);
    assert!((projected.metrics.paired_qty - 2.0).abs() < 1e-9);
    assert!((projected.metrics.paired_locked_pnl - 0.84).abs() < 1e-9);
}

#[test]
fn test_gabagool_open_pair_band_can_quote_both_sides() {
    let quotes = gabagool_grid_quotes(
        cfg(),
        InventoryState::default(),
        book(0.20, 0.23, 0.70, 0.73),
    );

    let yes = quotes.yes_buy.expect("expected YES unified-buy quote");
    let no = quotes.no_buy.expect("expected NO unified-buy quote");
    assert_eq!(yes.side, Side::Yes);
    assert_eq!(yes.direction, TradeDirection::Buy);
    assert_eq!(yes.reason, BidReason::Provide);
    assert_eq!(no.side, Side::No);
    assert_eq!(no.direction, TradeDirection::Buy);
    assert_eq!(no.reason, BidReason::Provide);
}

#[test]
fn test_gabagool_same_side_rebuy_requires_one_tick_avg_improvement() {
    let blocked = gabagool_grid_quotes(
        cfg(),
        InventoryState {
            yes_qty: 20.0,
            no_qty: 20.0,
            yes_avg_cost: 0.50,
            no_avg_cost: 0.30,
            net_diff: 0.0,
            portfolio_cost: 0.80,
        },
        book(0.47, 0.49, 0.50, 0.52),
    );
    assert!(
        blocked.yes_buy.is_none(),
        "same-side add should be blocked when projected avg-cost improvement is < 1 tick"
    );

    let allowed = gabagool_grid_quotes(
        cfg(),
        InventoryState {
            yes_qty: 5.0,
            no_qty: 5.0,
            yes_avg_cost: 0.50,
            no_avg_cost: 0.30,
            net_diff: 0.0,
            portfolio_cost: 0.80,
        },
        book(0.47, 0.49, 0.50, 0.52),
    );
    let yes = allowed
        .yes_buy
        .expect("same-side add should be allowed when projected avg-cost improves by >= 1 tick");
    assert_eq!(yes.side, Side::Yes);
}

#[test]
fn test_gabagool_ignores_dip_buy_entry_cap() {
    let mut low_cap = cfg();
    low_cap.dip_buy_max_entry_price = 0.05;
    let mut high_cap = cfg();
    high_cap.dip_buy_max_entry_price = 0.90;

    let low = gabagool_grid_quotes(
        low_cap,
        InventoryState::default(),
        book(0.20, 0.23, 0.70, 0.73),
    );
    let high = gabagool_grid_quotes(
        high_cap,
        InventoryState::default(),
        book(0.20, 0.23, 0.70, 0.73),
    );

    let low_yes = low.yes_buy.expect("low-cap YES quote");
    let high_yes = high.yes_buy.expect("high-cap YES quote");
    let low_no = low.no_buy.expect("low-cap NO quote");
    let high_no = high.no_buy.expect("high-cap NO quote");

    assert!((low_yes.price - high_yes.price).abs() < 1e-9);
    assert!((low_no.price - high_no.price).abs() < 1e-9);
    assert!((low_yes.size - high_yes.size).abs() < 1e-9);
    assert!((low_no.size - high_no.size).abs() < 1e-9);
}

#[test]
fn test_gabagool_utility_threshold_blocks_marginal_quotes() {
    let quotes = gabagool_grid_quotes(
        cfg(),
        InventoryState::default(),
        book(0.50, 0.52, 0.50, 0.52),
    );

    assert!(
        quotes.yes_buy.is_none(),
        "YES quote should be suppressed when projected utility gain is below bid_size*tick"
    );
    assert!(
        quotes.no_buy.is_none(),
        "NO quote should be suppressed when projected utility gain is below bid_size*tick"
    );
}

#[test]
fn test_gabagool_corridor_balanced_build_quotes_both_sides() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;
    let quotes =
        gabagool_corridor_quotes(c, InventoryState::default(), book(0.20, 0.23, 0.70, 0.73));
    assert!(quotes.yes_buy.is_some(), "balanced state should quote YES");
    assert!(quotes.no_buy.is_some(), "balanced state should quote NO");
}

#[test]
fn test_gabagool_corridor_side_biased_prefers_cheap_side() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;
    let inv = InventoryState {
        yes_qty: 4.0,
        no_qty: 2.0,
        yes_avg_cost: 0.40,
        no_avg_cost: 0.40,
        net_diff: 2.0,
        portfolio_cost: 0.80,
    };
    let quotes = gabagool_corridor_quotes(c, inv, book(0.58, 0.60, 0.18, 0.20));
    assert!(
        quotes.yes_buy.is_none(),
        "side-biased mode should not keep expensive side"
    );
    let no = quotes.no_buy.expect("cheap side should stay quoted");
    assert_eq!(no.side, Side::No);
}

#[test]
fn test_gabagool_corridor_pairing_repair_prefers_missing_side() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;
    let inv = InventoryState {
        yes_qty: 4.0,
        no_qty: 2.0,
        yes_avg_cost: 0.80,
        no_avg_cost: 0.20,
        net_diff: 2.0,
        portfolio_cost: 1.00,
    };
    let quotes = gabagool_corridor_quotes(c, inv, book(0.82, 0.85, 0.09, 0.11));
    assert!(
        quotes.yes_buy.is_none(),
        "repair mode should suspend dominant side"
    );
    let no = quotes.no_buy.expect("repair side NO should be quoted");
    assert_eq!(no.side, Side::No);
}

#[test]
fn test_gabagool_corridor_risk_compression_blocks_risk_increase() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;
    c.max_net_diff = 20.0;
    let inv = InventoryState {
        yes_qty: 7.0,
        no_qty: 2.0,
        yes_avg_cost: 0.55,
        no_avg_cost: 0.30,
        net_diff: 5.0,
        portfolio_cost: 0.85,
    };
    let quotes = gabagool_corridor_quotes(c, inv, book(0.09, 0.11, 0.82, 0.84));
    assert!(
        quotes.yes_buy.is_none(),
        "risk-compression must block dominant-side expansion"
    );
    let no = quotes
        .no_buy
        .expect("risk-reducing side should remain available");
    assert_eq!(no.side, Side::No);
}

#[test]
fn test_gabagool_corridor_allows_same_side_add_above_avg() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;
    let inv = InventoryState {
        yes_qty: 4.0,
        no_qty: 4.0,
        yes_avg_cost: 0.30,
        no_avg_cost: 0.45,
        net_diff: 0.0,
        portfolio_cost: 0.75,
    };
    let quotes = gabagool_corridor_quotes(c, inv, book(0.38, 0.40, 0.53, 0.55));
    let yes = quotes
        .yes_buy
        .expect("corridor strategy should allow same-side add without avg-improvement constraint");
    assert!(yes.price > 0.30);
    assert!(quotes.no_buy.is_some());
}

#[tokio::test]
async fn test_phase_builder_build_coexists_with_opposite_side_hedge() {
    let mut c = cfg();
    c.strategy = StrategyKind::PhaseBuilder;
    c.dip_buy_max_entry_price = 0.20;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        yes_qty: 4.0,
        yes_avg_cost: 0.12,
        net_diff: 4.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.09, 0.11, 0.70, 0.72));

    let mut saw_yes_provide = false;
    let mut saw_no_hedge = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(160), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            if target.side == Side::Yes && target.reason == BidReason::Provide {
                saw_yes_provide = true;
            }
            if target.side == Side::No && target.reason == BidReason::Hedge {
                saw_no_hedge = true;
            }
        }
    }

    assert!(
        saw_yes_provide,
        "build phase should keep same-side provide alive"
    );
    assert!(
        saw_no_hedge,
        "unified execution layer should still post opposite-side hedge"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_phase_builder_outcome_floor_blocks_generated_provide() {
    let mut c = cfg();
    c.strategy = StrategyKind::PhaseBuilder;
    c.max_net_diff = 2.0;
    c.pair_target = 0.50;
    c.dip_buy_max_entry_price = 0.90;

    let (_o, _i, m, _k, mut e, coord) = make(c);
    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.78, 0.80, 0.95, 0.97));

    let cmd = timeout(Duration::from_millis(120), e.recv()).await;
    assert!(
        cmd.is_err(),
        "phase_builder provide should still be blocked by outcome floor when too expensive"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_phase_builder_ofi_toxic_suppresses_provide() {
    let mut c = cfg();
    c.strategy = StrategyKind::PhaseBuilder;
    c.dip_buy_max_entry_price = 0.20;

    let (o, _i, m, _k, mut e, coord) = make(c);
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: -150.0,
            buy_volume: 0.0,
            sell_volume: 150.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.08, 0.10, 0.31, 0.33));

    let cmd = timeout(Duration::from_millis(120), e.recv()).await;
    assert!(
        cmd.is_err(),
        "OFI toxic side should suppress phase_builder cheap-side provide"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_phase_builder_soft_close_blocks_risk_increasing_build_quote() {
    let mut c = cfg();
    c.strategy = StrategyKind::PhaseBuilder;
    c.dip_buy_max_entry_price = 0.20;
    c.min_order_size = 10.0; // suppress small hedge so we isolate provide gating
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    c.market_end_ts = Some(now_secs + 40);
    c.endgame_soft_close_secs = 60;
    c.endgame_hard_close_secs = 20;
    c.endgame_freeze_secs = 5;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        yes_qty: 4.0,
        yes_avg_cost: 0.12,
        net_diff: 4.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.09, 0.11, 0.70, 0.72));

    let cmd = timeout(Duration::from_millis(120), e.recv()).await;
    assert!(
        cmd.is_err(),
        "SoftClose should block phase_builder risk-increasing provide in build phase"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_gabagool_ofi_toxic_does_not_block_risk_reducing_unified_buy() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolGrid;

    let (o, i, m, _k, mut e, mut coord) = make(c);
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: -200.0,
            buy_volume: 0.0,
            sell_volume: 200.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: false,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });
    let _ = i.send(InventoryState {
        no_qty: 4.0,
        no_avg_cost: 0.30,
        net_diff: -4.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.63, 0.65, 0.18, 0.20));

    let cmd = timeout(Duration::from_millis(120), e.recv())
        .await
        .expect("timeout")
        .expect("cmd");
    match cmd {
        OrderManagerCmd::SetTarget(target) => {
            assert_eq!(target.side, Side::Yes);
            assert_eq!(target.reason, BidReason::Provide);
        }
        other => panic!(
            "expected YES provide despite toxic risk-reducing side, got {:?}",
            other
        ),
    }

    drop(m);
    let _ = h.await;
}

#[test]
fn test_pair_arb_ofi_toxic_does_not_block_pairing_buy_in_execution_layer() {
    let c = with_strategy(cfg(), StrategyKind::PairArb);
    let (_, _, _, _, _, coord) = make(c);
    let inv = InventoryState {
        no_qty: 4.0,
        no_avg_cost: 0.30,
        net_diff: -4.0,
        ..Default::default()
    };
    let ofi = OfiSnapshot {
        yes: SideOfi {
            ofi_score: -200.0,
            buy_volume: 0.0,
            sell_volume: 200.0,
            heat_score: 2.0,
            is_hot: true,
            is_toxic: true,
            toxic_buy: true,
            saturated: true,
            ..Default::default()
        },
        no: SideOfi::default(),
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    };
    let book = book(0.63, 0.65, 0.18, 0.20);
    let metrics = coord.derive_inventory_metrics(&inv);
    let mut quotes = StrategyKind::PairArb.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            settled_inv: &inv,
            working_inv: &inv,
            inventory: &crate::polymarket::messages::InventorySnapshot {
                settled: inv,
                working: inv,
                pending_yes_qty: 0.0,
                pending_no_qty: 0.0,
                fragile: false,
            },
            book: &book,
            metrics: &metrics,
            ofi: Some(&ofi),
            glft: None,
        },
    );

    assert!(
        quotes.buy_for(Side::Yes).is_some(),
        "pairing YES buy should survive strategy-level OFI shaping"
    );

    coord.apply_flow_risk(&inv, &mut quotes, false, false, true, false);

    let pairing_yes = quotes
        .buy_for(Side::Yes)
        .expect("pairing YES buy should survive execution-layer toxic gating for pair_arb");
    assert_eq!(pairing_yes.side, Side::Yes);
    assert_eq!(pairing_yes.direction, TradeDirection::Buy);
    assert!(pairing_yes.price > 0.0);
    assert_eq!(pairing_yes.reason, BidReason::Provide);
}

#[tokio::test]
async fn test_gabagool_normal_session_skips_directional_hedge_overlay() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolGrid;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        yes_qty: 12.0,
        yes_avg_cost: 0.50,
        net_diff: 12.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.48, 0.50, 0.46, 0.48));

    let cmd = timeout(Duration::from_millis(150), e.recv())
        .await
        .expect("timeout")
        .expect("cmd");
    match cmd {
        OrderManagerCmd::SetTarget(target) => {
            assert_eq!(target.side, Side::No);
            assert_eq!(target.reason, BidReason::Provide);
            assert!(
                (target.size - 2.0).abs() < 0.1,
                "gabagool_grid unified buy should keep bid_size sizing, not abs(net_diff)"
            );
        }
        other => panic!("expected unified NO provide, got {:?}", other),
    }

    while let Ok(Some(cmd)) = timeout(Duration::from_millis(50), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            assert_ne!(
                target.reason,
                BidReason::Hedge,
                "normal-session gabagool_grid should not dispatch directional hedge overlay"
            );
        }
    }

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_gabagool_corridor_normal_session_skips_directional_hedge_overlay() {
    let mut c = cfg();
    c.strategy = StrategyKind::GabagoolCorridor;

    let (_o, i, m, _k, mut e, coord) = make(c);
    let _ = i.send(InventoryState {
        yes_qty: 12.0,
        yes_avg_cost: 0.50,
        net_diff: 12.0,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.48, 0.50, 0.46, 0.48));

    let cmd = timeout(Duration::from_millis(150), e.recv())
        .await
        .expect("timeout")
        .expect("cmd");
    match cmd {
        OrderManagerCmd::SetTarget(target) => {
            assert_eq!(target.reason, BidReason::Provide);
            assert!(
                (target.size - 2.0).abs() < 0.1,
                "gabagool_corridor should keep bid_size sizing, not abs(net_diff)"
            );
        }
        other => panic!("expected unified provide, got {:?}", other),
    }

    while let Ok(Some(cmd)) = timeout(Duration::from_millis(50), e.recv()).await {
        if let OrderManagerCmd::SetTarget(target) = cmd {
            assert_ne!(
                target.reason,
                BidReason::Hedge,
                "normal-session gabagool_corridor should not dispatch directional hedge overlay"
            );
        }
    }

    drop(m);
    let _ = h.await;
}

// ── Debounce ──

#[tokio::test]
async fn test_debounce_skips_rapid_reprice() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::PairArb;
    cfg.debounce_ms = 5000; // 5 seconds - will definitely block
    let (_o, _i, m, _k, mut e, mut coord) = make(cfg);
    let h = tokio::spawn(coord.run());

    // First tick: places bids
    let _ = m.send(bt(0.44, 0.46, 0.48, 0.52));
    let c1 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    assert!(c1.is_ok());
    let c2 = tokio::time::timeout(std::time::Duration::from_millis(100), e.recv()).await;
    assert!(c2.is_ok());

    // Second tick with different prices — should be debounced
    let _ = m.send(bt(0.30, 0.32, 0.60, 0.62));
    let c3 = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
    assert!(c3.is_err()); // No commands = debounced

    drop(m);
    let _ = h.await;
}

// ── Empty book fallback ──

#[tokio::test]
async fn test_empty_book_skipped() {
    let (_o, _i, m, _k, mut e, coord) = make(cfg());
    let h = tokio::spawn(coord.run());
    // All zeros — no valid book
    let _ = m.send(bt(0.0, 0.0, 0.0, 0.0));
    let c = tokio::time::timeout(std::time::Duration::from_millis(50), e.recv()).await;
    assert!(c.is_err()); // No commands
    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_hedge_emergency_ceiling_toxic_flow() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::DipBuy;
    cfg.max_net_diff = 10.0;
    cfg.pair_target = 0.985;
    cfg.max_portfolio_cost = 1.02;

    let (o, i, m, _k, mut e, coord) = make(cfg);

    // 1. Setup inventory: heavily imbalanced (net = -10.0)
    let inv = InventoryState {
        net_diff: -10.0,
        yes_qty: 5.0,
        no_qty: 15.0,
        yes_avg_cost: 0.45,
        no_avg_cost: 0.45,
        portfolio_cost: 0.90,
    };
    let _ = i.send(inv); // watch::Sender::send is not async

    // 2. Trigger Toxic Flow kill on the OTHER side (NO)
    // This ensures the risky side is toxic, but the hedge side (YES) is healthy.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi::default(),
        no: SideOfi {
            ofi_score: 5000.0,
            heat_score: 5.0,
            is_hot: true,
            is_toxic: true,
            toxic_sell: true,
            ..Default::default()
        },
        reference_mid_yes: 0.45,
        ts: Instant::now(),
    });

    // 3. Hear the kill signal
    let h = tokio::spawn(async move { coord.run().await });

    // 4. Send a book update to trigger pricing
    let _ = m.send(bt(0.30, 0.70, 0.40, 0.60));

    let cmd = timeout(Duration::from_millis(100), e.recv()).await;
    if let Ok(Some(OrderManagerCmd::SetTarget(target))) = cmd {
        assert_eq!(target.side, Side::Yes);
        assert_eq!(target.reason, BidReason::Hedge);
        // Incremental emergency ceiling with existing YES inventory:
        // target_yes_avg=1.02-0.45=0.57 over (5+10) shares -> incremental cap 0.63
        assert!((target.price - 0.63).abs() < 1e-9);
    } else {
        panic!("Expected SetTarget hedge command, got {:?}", cmd);
    }
    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_stale_book_protection() {
    let (_o, _i, m, k, mut e, mut coord) = make(cfg());

    // 1. Send an initial valid update to populate last_valid_book
    coord.update_book(0.44, 0.46, 0.48, 0.52);

    // 2. Artificially backdate the timestamps to 6 seconds ago
    coord.last_valid_ts_yes = Instant::now() - Duration::from_secs(6);
    coord.last_valid_ts_no = Instant::now() - Duration::from_secs(6);

    let h = tokio::spawn(async move { coord.run().await });

    // 3. Trigger a pricing attempt via KillSwitchSignal (Direct Kill channel)
    // This calls tick() WITHOUT calling update_book(), so timestamps remain stale.
    let _ = k.send(KillSwitchSignal {
        side: Side::Yes,
        ofi_score: 1.0,
        ts: Instant::now(),
    });

    // 4. Command should NOT be sent due to staleness
    // Note: we check both sides in the new tick() logic.
    let cmd = timeout(Duration::from_millis(200), e.recv()).await;
    assert!(
        cmd.is_err(),
        "Expected timeout (no bid) due to stale book, but got {:?}",
        cmd
    );

    drop(m);
    let _ = h.await;
}
#[tokio::test]
async fn test_dynamic_hedge_sizing_shares() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::DipBuy;
    cfg.bid_size = 5.0;
    let (_o, i, m, _k, mut e, coord) = make(cfg);
    let h = tokio::spawn(coord.run());

    // Setup imbalance of 12.0 shares (YES excess)
    let _ = i.send(InventoryState {
        net_diff: 12.0,
        yes_qty: 12.0,
        yes_avg_cost: 0.50,
        ..Default::default()
    });

    // Trigger pricing with a valid book update
    let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

    // Should see a HEDGE order on NO with size = 12.0
    let mut found_hedge = false;
    while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
        timeout(Duration::from_millis(200), e.recv()).await
    {
        if target.side == Side::No
            && (target.size - 12.0).abs() < 0.1
            && target.reason == BidReason::Hedge
        {
            found_hedge = true;
            break;
        }
    }
    assert!(found_hedge, "Expected hedge order of size 12.0 on NO");

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_pair_arb_skips_directional_hedge_with_large_imbalance() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::PairArb;
    cfg.bid_size = 5.0;
    let (_o, i, m, _k, mut e, coord) = make(cfg);
    let h = tokio::spawn(coord.run());

    let _ = i.send(InventoryState {
        net_diff: 12.0,
        yes_qty: 12.0,
        yes_avg_cost: 0.50,
        ..Default::default()
    });
    let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

    let mut saw_hedge = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(200), e.recv()).await {
        match cmd {
            OrderManagerCmd::SetTarget(target) if target.reason == BidReason::Hedge => {
                saw_hedge = true;
                break;
            }
            OrderManagerCmd::OneShotTakerHedge { .. } => {
                saw_hedge = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        !saw_hedge,
        "pair_arb should not dispatch directional hedge or taker de-risk overlays"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_pair_arb_hard_close_skips_taker_and_maker_hedge() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::PairArb;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    cfg.market_end_ts = Some(now_secs + 15);
    cfg.endgame_soft_close_secs = 60;
    cfg.endgame_hard_close_secs = 30;
    cfg.endgame_freeze_secs = 5;

    let (_o, i, m, _k, mut e, coord) = make(cfg);
    let _ = i.send(InventoryState {
        net_diff: 4.0,
        yes_qty: 4.0,
        yes_avg_cost: 0.55,
        ..Default::default()
    });

    let h = tokio::spawn(coord.run());
    let _ = m.send(bt(0.30, 0.32, 0.68, 0.70));

    let mut saw_hedge = false;
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(200), e.recv()).await {
        match cmd {
            OrderManagerCmd::SetTarget(target) if target.reason == BidReason::Hedge => {
                saw_hedge = true;
                break;
            }
            OrderManagerCmd::OneShotTakerHedge { .. } => {
                saw_hedge = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        !saw_hedge,
        "pair_arb should stay buy-only in hard-close and not trigger taker/maker hedge overlays"
    );

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_inventory_limits_block_orders() {
    let mut cfg = cfg();
    cfg.as_skew_factor = 0.0;
    cfg.hedge_debounce_ms = 0;
    cfg.max_net_diff = 10.0;
    cfg.bid_size = 15.0; // Ordering 15 shares while limit is 10.
    let (_o, i, m, _k, mut e, coord) = make(cfg);
    let h = tokio::spawn(coord.run());

    let _ = i.send(InventoryState {
        net_diff: 0.0,
        ..Default::default()
    });

    let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

    // It might send a Cancel command if it thinks there's a target to clear.
    // We check that NO 'Place' command with size 50 is sent.
    while let Ok(cmd) = timeout(Duration::from_millis(100), e.recv()).await {
        match cmd {
            Some(OrderManagerCmd::SetTarget(target)) => {
                if target.size > 1.0 && target.price > 0.0 {
                    panic!(
                        "Should not place order of size {} when budget exceeded",
                        target.size
                    );
                }
            }
            _ => {} // Ignore CancelAll
        }
    }

    drop(m);
    let _ = h.await;
}

#[tokio::test]
async fn test_hedge_size_change_reprices() {
    let mut cfg = cfg();
    cfg.strategy = StrategyKind::DipBuy;
    cfg.as_skew_factor = 0.0;
    cfg.max_net_diff = 100.0;
    cfg.hedge_debounce_ms = 0;
    let (_o, i, m, _k, mut e, coord) = make(cfg);
    let h = tokio::spawn(coord.run());

    let _ = i.send(InventoryState {
        net_diff: 5.0,
        ..Default::default()
    });
    let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

    let mut first_size = None;
    while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
        timeout(Duration::from_millis(200), e.recv()).await
    {
        if target.side == Side::No
            && (target.size - 5.0).abs() < 0.1
            && target.reason == BidReason::Hedge
        {
            first_size = Some(target.size);
            break;
        }
    }
    assert!(
        first_size.is_some(),
        "Expected initial hedge size of 5.0 on NO"
    );

    let _ = i.send(InventoryState {
        net_diff: 8.0,
        yes_qty: 8.0,
        no_qty: 0.0,
        yes_avg_cost: 0.50,
        ..Default::default()
    });
    let _ = m.send(bt(0.48, 0.52, 0.48, 0.52));

    let mut updated = false;
    while let Ok(Some(OrderManagerCmd::SetTarget(target))) =
        timeout(Duration::from_millis(200), e.recv()).await
    {
        if target.side == Side::No
            && (target.size - 8.0).abs() < 0.1
            && target.reason == BidReason::Hedge
        {
            updated = true;
            break;
        }
    }
    assert!(updated, "Expected hedge size update to 8.0 on NO");

    drop(m);
    let _ = h.await;
}

#[test]
fn test_pair_arb_live_obs_heat_alert_on_high_softened_ratio() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    let (_, _, _, _, _, mut coord) = make(c);
    coord.stats.ofi_heat_events = 15;

    let lvl = coord.pair_arb_obs_heat_level(2_000, 0.52);
    assert_eq!(lvl, LiveObsLevel::Alert);
}

#[test]
fn test_pair_arb_live_obs_heat_requires_min_attempts_for_alert() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    let (_, _, _, _, _, mut coord) = make(c);
    coord.stats.ofi_heat_events = 5;

    let lvl = coord.pair_arb_obs_heat_level(500, 0.95);
    assert_eq!(lvl, LiveObsLevel::Ok);
}

#[test]
fn test_pair_arb_live_obs_heat_warns_on_high_event_regime() {
    let mut c = cfg();
    c.strategy = StrategyKind::PairArb;
    let (_, _, _, _, _, mut coord) = make(c);
    coord.stats.ofi_heat_events = 25;

    let lvl = coord.pair_arb_obs_heat_level(2_000, 0.39);
    assert_eq!(lvl, LiveObsLevel::Warn);
}
