use super::*;
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

fn make(
    c: CoordinatorConfig,
) -> (
    watch::Sender<OfiSnapshot>,
    watch::Sender<InventoryState>,
    watch::Sender<MarketDataMsg>,
    mpsc::Sender<KillSwitchSignal>,
    mpsc::Receiver<OrderManagerCmd>,
    StrategyCoordinator,
) {
    let (o, or) = watch::channel(OfiSnapshot::default());
    let (i, ir) = watch::channel(InventoryState::default());
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
        i,
        m,
        k,
        er,
        StrategyCoordinator::with_kill_rx(c, or, ir, mr, e, kr),
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

fn phase_builder_quotes(c: CoordinatorConfig, inv: InventoryState, book: Book) -> StrategyQuotes {
    let (_, _, _, _, _, coord) = make(with_strategy(c, StrategyKind::PhaseBuilder));
    let metrics = coord.derive_inventory_metrics(&inv);
    StrategyKind::PhaseBuilder.compute_quotes(
        &coord,
        StrategyTickInput {
            inv: &inv,
            book: &book,
            metrics: &metrics,
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
            book: &book,
            metrics: &metrics,
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
            book: &book,
            metrics: &metrics,
            glft: None,
        },
    )
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
    });
    let widened = c.post_only_safety_margin_for(Side::Yes, 0.30, 0.34);
    assert!((widened - (base + c.cfg.tick_size)).abs() < 1e-9);
}

#[test]
fn test_cross_book_feedback_caps_and_decays() {
    let (_, _, _, _, _, mut c) = make(cfg());
    for _ in 0..5 {
        c.handle_execution_feedback(ExecutionFeedback::PostOnlyCrossed {
            slot: OrderSlot::NO_BUY,
            ts: Instant::now(),
        });
    }
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 3);

    c.no_maker_friction.last_cross_reject_ts = Some(Instant::now() - Duration::from_secs(4));
    c.decay_maker_friction(Instant::now());
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 2);
    c.decay_maker_friction(Instant::now());
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 1);
    c.decay_maker_friction(Instant::now());
    assert_eq!(c.maker_friction(Side::No).extra_safety_ticks, 0);
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

// ── Per-side toxicity guard ──

#[tokio::test]
async fn test_toxic_cancels_only_toxic_side() {
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

    // Only YES is toxic — only YES should be canceled.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: 100.0,
            buy_volume: 100.0,
            sell_volume: 0.0,
            is_toxic: true,
        },
        no: SideOfi::default(),
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
async fn test_other_side_can_still_quote_when_one_side_toxic() {
    let (o, _i, m, _k, mut e, coord) = make(with_strategy(cfg(), StrategyKind::PairArb));

    // NO is toxic, YES is healthy: coordinator should still place YES bid.
    let _ = o.send(OfiSnapshot {
        yes: SideOfi::default(),
        no: SideOfi {
            ofi_score: -80.0,
            buy_volume: 0.0,
            sell_volume: 80.0,
            is_toxic: true,
        },
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
            ofi_score: 200.0,
            buy_volume: 200.0,
            sell_volume: 0.0,
            is_toxic: true,
        },
        no: SideOfi::default(),
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
            ofi_score: 120.0,
            buy_volume: 120.0,
            sell_volume: 0.0,
            is_toxic: true,
        },
        no: SideOfi::default(),
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
async fn test_endgame_soft_close_clear_reason_is_endgame_risk_gate() {
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
    coord.yes_target = Some(DesiredTarget {
        side: Side::Yes,
        direction: TradeDirection::Buy,
        price: 0.45,
        size: 2.0,
        reason: BidReason::Provide,
    });
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
        Some(CancelReason::EndgameRiskGate),
        "SoftClose risk gate should clear with EndgameRiskGate reason"
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
    let (_o, _i, m, _k, mut e, coord) = make(with_strategy(cfg(), StrategyKind::PairArb));
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
    assert!(saw_no, "NO side should still be quotable");

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
    assert!(prices[&Side::Yes] + prices[&Side::No] <= 0.98 + 1e-9);

    drop(m);
    let _ = h.await;
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
            ofi_score: 150.0,
            buy_volume: 150.0,
            sell_volume: 0.0,
            is_toxic: true,
        },
        no: SideOfi::default(),
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

    let (o, i, m, _k, mut e, coord) = make(c);
    let _ = o.send(OfiSnapshot {
        yes: SideOfi {
            ofi_score: 200.0,
            buy_volume: 200.0,
            sell_volume: 0.0,
            is_toxic: true,
        },
        no: SideOfi::default(),
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
    let (_o, _i, m, _k, mut e, coord) = make(cfg);
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
    cfg.strategy = StrategyKind::PairArb;
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
            is_toxic: true,
            ..Default::default()
        },
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
    cfg.strategy = StrategyKind::PairArb;
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
    cfg.strategy = StrategyKind::PairArb;
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
