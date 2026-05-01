use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::messages::{
    BidReason, CancelReason, DesiredTarget, ExecutionCmd, OrderManagerCmd, OrderResult, OrderSlot,
    SlotReleaseEvent, TradeDirection, TradeIntent, TradePurpose, TradeUrgency,
};
use super::types::Side;

const PENDING_SUBMIT_TIMEOUT: Duration = Duration::from_secs(8);
const PENDING_CANCEL_TIMEOUT: Duration = Duration::from_secs(8);
const PENDING_TIMEOUT_COOLDOWN: Duration = Duration::from_secs(2);
const SLOT_PUMP_HEARTBEAT: Duration = Duration::from_millis(200);
const SELL_AVAILABLE_WARMUP: Duration = Duration::from_millis(1_500);
const DEFAULT_BUY_FILL_REOPEN_COOLDOWN: Duration = Duration::ZERO;
const ORACLE_LAG_REPRICE_MIN_INTERVAL: Duration = Duration::from_millis(200);
const ORACLE_LAG_REPRICE_PRICE_EPS: f64 = 5e-4;
const ORACLE_LAG_REPRICE_SIZE_EPS: f64 = 0.05;

#[derive(Debug, Clone, PartialEq)]
pub enum OrderState {
    Idle,
    PendingSubmit(DesiredTarget),
    Live(DesiredTarget),
    PendingCancel(Option<DesiredTarget>),
}

#[derive(Debug, Clone, PartialEq)]
enum SideTakerState {
    Idle,
    Pending(TradeIntent),
    PendingSubmit(TradeIntent),
}

#[derive(Debug)]
pub struct SlotTracker {
    pub slot: OrderSlot,
    pub desired: Option<DesiredTarget>,
    pub pair_arb_local_unreleased_matched_notional_usdc: f64,
    pub clear_reason: CancelReason,
    pub state: OrderState,
    pub last_action: Instant,
    pub cooldown_until: Option<Instant>,
}

impl SlotTracker {
    pub fn new(slot: OrderSlot) -> Self {
        Self {
            slot,
            desired: None,
            pair_arb_local_unreleased_matched_notional_usdc: 0.0,
            clear_reason: CancelReason::InventoryLimit,
            state: OrderState::Idle,
            last_action: Instant::now(),
            cooldown_until: None,
        }
    }
}

pub struct OrderManager {
    slots: [SlotTracker; 4],
    side_takers: [SideTakerState; 2],
    sell_available_after: [Option<Instant>; 2],
    cmd_rx: mpsc::Receiver<OrderManagerCmd>,
    exec_tx: mpsc::Sender<ExecutionCmd>,
    result_rx: mpsc::Receiver<OrderResult>,
    slot_release_tx: mpsc::Sender<SlotReleaseEvent>,
    buy_fill_reopen_cooldown: Duration,
}

impl OrderManager {
    pub fn new(
        cmd_rx: mpsc::Receiver<OrderManagerCmd>,
        exec_tx: mpsc::Sender<ExecutionCmd>,
        result_rx: mpsc::Receiver<OrderResult>,
        slot_release_tx: mpsc::Sender<SlotReleaseEvent>,
    ) -> Self {
        Self::with_buy_fill_reopen_cooldown(
            cmd_rx,
            exec_tx,
            result_rx,
            slot_release_tx,
            DEFAULT_BUY_FILL_REOPEN_COOLDOWN,
        )
    }

    pub fn with_buy_fill_reopen_cooldown(
        cmd_rx: mpsc::Receiver<OrderManagerCmd>,
        exec_tx: mpsc::Sender<ExecutionCmd>,
        result_rx: mpsc::Receiver<OrderResult>,
        slot_release_tx: mpsc::Sender<SlotReleaseEvent>,
        buy_fill_reopen_cooldown: Duration,
    ) -> Self {
        Self {
            slots: std::array::from_fn(|idx| SlotTracker::new(OrderSlot::ALL[idx])),
            side_takers: [SideTakerState::Idle, SideTakerState::Idle],
            sell_available_after: [None, None],
            cmd_rx,
            exec_tx,
            result_rx,
            slot_release_tx,
            buy_fill_reopen_cooldown,
        }
    }

    fn tracker(&self, slot: OrderSlot) -> &SlotTracker {
        &self.slots[slot.index()]
    }

    fn tracker_mut(&mut self, slot: OrderSlot) -> &mut SlotTracker {
        &mut self.slots[slot.index()]
    }

    fn side_taker(&self, side: Side) -> &SideTakerState {
        &self.side_takers[side.index()]
    }

    fn side_taker_mut(&mut self, side: Side) -> &mut SideTakerState {
        &mut self.side_takers[side.index()]
    }

    fn side_slots_idle(&self, side: Side) -> bool {
        OrderSlot::side_slots(side)
            .into_iter()
            .all(|slot| matches!(self.tracker(slot).state, OrderState::Idle))
    }

    fn oracle_lag_live_reprice_needed(
        &self,
        slot: OrderSlot,
        live: &DesiredTarget,
        desired: &DesiredTarget,
    ) -> bool {
        if live.side != desired.side || live.direction != desired.direction {
            return true;
        }

        let size_delta = (live.size - desired.size).abs();
        if size_delta > ORACLE_LAG_REPRICE_SIZE_EPS {
            return true;
        }

        let price_delta = (live.price - desired.price).abs();
        if price_delta <= ORACLE_LAG_REPRICE_PRICE_EPS {
            return false;
        }

        let elapsed = self.tracker(slot).last_action.elapsed();
        if elapsed < ORACLE_LAG_REPRICE_MIN_INTERVAL {
            debug!(
                "⏭️ OMS oracle_lag reprice suppressed | slot={} live={:.4}@{:.2} desired={:.4}@{:.2} price_delta={:.6} size_delta={:.3} elapsed_ms={}",
                slot.as_str(),
                live.price,
                live.size,
                desired.price,
                desired.size,
                price_delta,
                size_delta,
                elapsed.as_millis(),
            );
            return false;
        }

        true
    }

    fn sell_available_after(&self, side: Side) -> Option<Instant> {
        self.sell_available_after[side.index()]
    }

    fn set_sell_available_after(&mut self, side: Side, until: Option<Instant>) {
        self.sell_available_after[side.index()] = until;
    }

    pub async fn run(mut self) {
        info!("🚦 OrderManager [OMS] started");
        let mut heartbeat = tokio::time::interval(SLOT_PUMP_HEARTBEAT);
        loop {
            tokio::select! {
                biased;
                result = self.result_rx.recv() => {
                    match result {
                        Some(OrderResult::OrderPlaced { slot, target }) => {
                            self.handle_placed(slot, target).await;
                            self.pump_slot(slot).await;
                        }
                        Some(OrderResult::OrderFailed { slot, cooldown_ms }) => {
                            self.handle_failed(slot, cooldown_ms).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        Some(OrderResult::SlotBusy { slot }) => {
                            self.handle_slot_busy(slot).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        Some(OrderResult::OrderFilled { slot }) => {
                            self.handle_filled(slot).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        Some(OrderResult::TakerHedgeDone { side }) => {
                            self.handle_taker_done(side).await;
                            for slot in OrderSlot::side_slots(side) {
                                self.pump_slot(slot).await;
                            }
                        }
                        Some(OrderResult::TakerHedgeFailed { side, cooldown_ms }) => {
                            self.handle_taker_failed(side, cooldown_ms).await;
                        }
                        Some(OrderResult::CancelAck { slot }) => {
                            self.handle_cancel_ack(slot).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        None => break,
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(OrderManagerCmd::SetTarget(target)) => {
                            let slot = target.slot();
                            self.handle_target(target).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        Some(OrderManagerCmd::SetPairArbHeadroom {
                            slot,
                            local_unreleased_matched_notional_usdc,
                        }) => {
                            self.tracker_mut(slot).pair_arb_local_unreleased_matched_notional_usdc =
                                local_unreleased_matched_notional_usdc.max(0.0);
                        }
                        Some(OrderManagerCmd::ClearTarget { slot, reason }) => {
                            self.handle_clear(slot, reason).await;
                            self.pump_slot(slot).await;
                            self.pump_side_taker(slot.side).await;
                        }
                        Some(OrderManagerCmd::OneShotTakerHedge {
                            side,
                            direction,
                            size,
                            purpose,
                            limit_price,
                            expected_fill_price,
                        }) => {
                            let oracle_lag_fast_path = matches!(purpose, TradePurpose::OracleLagSnipe)
                                && self.side_slots_idle(side);
                            self.handle_one_shot_taker(
                                side,
                                direction,
                                size,
                                purpose,
                                limit_price,
                                expected_fill_price,
                            )
                            .await;
                            if oracle_lag_fast_path {
                                debug!(
                                    "⚡ OMS fast one-shot taker path | side={:?} purpose={:?} reason=slots_idle_skip_side_pump",
                                    side, purpose
                                );
                            } else {
                                for slot in OrderSlot::side_slots(side) {
                                    self.pump_slot(slot).await;
                                }
                            }
                            self.pump_side_taker(side).await;
                        }
                        Some(OrderManagerCmd::CancelAll) => {
                            self.handle_cancel_all();
                            for slot in OrderSlot::ALL {
                                self.pump_slot(slot).await;
                            }
                        }
                        None => break,
                    }
                }
                _ = heartbeat.tick() => {
                    for slot in OrderSlot::ALL {
                        self.pump_slot(slot).await;
                    }
                    for side in [Side::Yes, Side::No] {
                        self.pump_side_taker(side).await;
                    }
                }
            }
        }
        info!("🚦 OrderManager shutting down");
    }

    async fn handle_target(&mut self, target: DesiredTarget) {
        let tracker = self.tracker_mut(target.slot());
        if target.price <= 0.0 || target.size <= 0.0 {
            tracker.desired = None;
        } else {
            tracker.desired = Some(target);
        }
    }

    async fn handle_clear(&mut self, slot: OrderSlot, reason: CancelReason) {
        let tracker = self.tracker_mut(slot);
        tracker.desired = None;
        tracker.pair_arb_local_unreleased_matched_notional_usdc = 0.0;
        tracker.clear_reason = reason;
    }

    async fn handle_one_shot_taker(
        &mut self,
        side: Side,
        direction: TradeDirection,
        size: f64,
        purpose: TradePurpose,
        limit_price: Option<f64>,
        expected_fill_price: Option<f64>,
    ) {
        if size <= 0.0 {
            return;
        }
        for slot in OrderSlot::side_slots(side) {
            let tracker = self.tracker_mut(slot);
            tracker.desired = None;
            tracker.clear_reason = CancelReason::Reprice;
        }
        *self.side_taker_mut(side) = SideTakerState::Pending(TradeIntent {
            side,
            direction,
            urgency: TradeUrgency::TakerFak,
            size,
            price: limit_price,
            expected_fill_price,
            purpose,
            local_unreleased_matched_notional_usdc: 0.0,
        });
    }

    fn handle_cancel_all(&mut self) {
        for slot in OrderSlot::ALL {
            let tracker = self.tracker_mut(slot);
            tracker.desired = None;
            tracker.pair_arb_local_unreleased_matched_notional_usdc = 0.0;
            tracker.clear_reason = CancelReason::Shutdown;
        }
        self.side_takers = [SideTakerState::Idle, SideTakerState::Idle];
    }

    async fn handle_placed(&mut self, slot: OrderSlot, target: DesiredTarget) {
        let tracker = self.tracker_mut(slot);
        match tracker.state {
            OrderState::PendingSubmit(_) | OrderState::Idle => {
                if matches!(tracker.state, OrderState::Idle)
                    && slot.direction == TradeDirection::Buy
                    && tracker
                        .cooldown_until
                        .is_some_and(|until| Instant::now() < until)
                {
                    warn!(
                        "🧭 OMS: {} late OrderPlaced ignored during buy reopen cooldown",
                        slot.as_str()
                    );
                    return;
                }
                tracker.state = OrderState::Live(target);
                tracker.last_action = Instant::now();
                info!("✅ OMS: {} OrderPlaced -> Live", slot.as_str());
            }
            _ => {}
        }
    }

    async fn handle_failed(&mut self, slot: OrderSlot, cooldown_ms: u64) {
        let tracker = self.tracker_mut(slot);
        tracker.state = OrderState::Idle;
        if cooldown_ms > 0 {
            let until = Instant::now() + Duration::from_millis(cooldown_ms);
            tracker.cooldown_until = Some(until);
            warn!(
                "⏳ OMS: {} OrderFailed — cooldown {}s",
                slot.as_str(),
                cooldown_ms / 1000,
            );
        }
    }

    async fn handle_slot_busy(&mut self, slot: OrderSlot) {
        let tracker = self.tracker_mut(slot);
        warn!(
            "⛔ OMS: {} SlotBusy — switching to PendingCancel to recover stale live slot",
            slot.as_str()
        );
        tracker.state = OrderState::PendingCancel(None);
        tracker.last_action = Instant::now();
        let _ = self
            .exec_tx
            .send(ExecutionCmd::CancelSlot {
                slot,
                reason: CancelReason::Reprice,
            })
            .await;
    }

    async fn handle_filled(&mut self, slot: OrderSlot) {
        let buy_fill_reopen_cooldown = self.buy_fill_reopen_cooldown;
        let tracker = self.tracker_mut(slot);
        info!("✅ OMS: {} OrderFilled -> Slot freed", slot.as_str());
        tracker.state = OrderState::Idle;
        tracker.desired = None;
        if slot.direction == TradeDirection::Buy && !buy_fill_reopen_cooldown.is_zero() {
            tracker.cooldown_until = Some(Instant::now() + buy_fill_reopen_cooldown);
        }
        let _ = self.slot_release_tx.send(SlotReleaseEvent { slot }).await;
        if slot.direction == TradeDirection::Buy {
            let until = Instant::now() + SELL_AVAILABLE_WARMUP;
            self.set_sell_available_after(slot.side, Some(until));
            info!(
                "⏳ OMS: {:?} sell availability warmup {}ms after {} fill",
                slot.side,
                SELL_AVAILABLE_WARMUP.as_millis(),
                slot.as_str(),
            );
        }
    }

    async fn handle_taker_done(&mut self, side: Side) {
        info!("✅ OMS: {:?} Taker hedge done -> Idle", side);
        *self.side_taker_mut(side) = SideTakerState::Idle;
    }

    async fn handle_taker_failed(&mut self, side: Side, cooldown_ms: u64) {
        warn!(
            "⚠️ OMS: {:?} Taker hedge failed -> Idle (cooldown {}ms)",
            side, cooldown_ms
        );
        *self.side_taker_mut(side) = SideTakerState::Idle;
        if cooldown_ms > 0 {
            let until = Instant::now() + Duration::from_millis(cooldown_ms);
            for slot in OrderSlot::side_slots(side) {
                self.tracker_mut(slot).cooldown_until = Some(until);
            }
        }
    }

    async fn handle_cancel_ack(&mut self, slot: OrderSlot) {
        let tracker = self.tracker_mut(slot);
        info!("🗑️ OMS: {} CancelAck -> Idle", slot.as_str());
        tracker.state = OrderState::Idle;
    }

    async fn pump_side_taker(&mut self, side: Side) {
        let state = self.side_taker(side).clone();
        match state {
            SideTakerState::Idle => {}
            SideTakerState::Pending(intent) => {
                let slots_idle = self.side_slots_idle(side);
                let oracle_lag_fast_path = matches!(intent.purpose, TradePurpose::OracleLagSnipe);
                if !slots_idle && !oracle_lag_fast_path {
                    return;
                }
                if !slots_idle && oracle_lag_fast_path {
                    info!(
                        "⚡ OMS fast taker dispatch | side={:?} purpose={:?} reason=oracle_lag_bypass_wait_idle",
                        side, intent.purpose
                    );
                }
                let cmd = ExecutionCmd::ExecuteIntent {
                    intent: intent.clone(),
                };
                *self.side_taker_mut(side) = SideTakerState::PendingSubmit(intent);
                let _ = self.exec_tx.send(cmd).await;
            }
            SideTakerState::PendingSubmit(_) => {}
        }
    }

    async fn pump_slot(&mut self, slot: OrderSlot) {
        if slot.direction == TradeDirection::Sell {
            if let Some(until) = self.sell_available_after(slot.side) {
                if Instant::now() < until {
                    return;
                }
                self.set_sell_available_after(slot.side, None);
                info!("✅ OMS: {:?} sell availability warmup complete", slot.side);
            }
        }

        {
            let tracker = self.tracker_mut(slot);
            if let Some(until) = tracker.cooldown_until {
                if Instant::now() < until {
                    return;
                }
                tracker.cooldown_until = None;
            }
        }

        let submit_timed_out = {
            let tracker = self.tracker(slot);
            matches!(tracker.state, OrderState::PendingSubmit(_))
                && tracker.last_action.elapsed() > PENDING_SUBMIT_TIMEOUT
        };
        if submit_timed_out {
            warn!(
                "⚠️ OMS: {} PendingSubmit timeout (>{}s) — forcing progress",
                slot.as_str(),
                PENDING_SUBMIT_TIMEOUT.as_secs()
            );
            let _ = self
                .exec_tx
                .send(ExecutionCmd::ReconcileNow {
                    reason: "pending_submit_timeout",
                })
                .await;
            let desired_is_none = self.tracker(slot).desired.is_none();
            if desired_is_none {
                let _ = self
                    .exec_tx
                    .send(ExecutionCmd::CancelSlot {
                        slot,
                        reason: CancelReason::Reprice,
                    })
                    .await;
                let tracker = self.tracker_mut(slot);
                tracker.state = OrderState::PendingCancel(None);
                tracker.last_action = Instant::now();
            } else {
                self.tracker_mut(slot).state = OrderState::Idle;
            }
            self.tracker_mut(slot).cooldown_until = Some(Instant::now() + PENDING_TIMEOUT_COOLDOWN);
        }

        let cancel_timed_out = {
            let tracker = self.tracker(slot);
            matches!(tracker.state, OrderState::PendingCancel(_))
                && tracker.last_action.elapsed() > PENDING_CANCEL_TIMEOUT
        };
        if cancel_timed_out {
            warn!(
                "⚠️ OMS: {} PendingCancel timeout (>{}s) — resetting to Idle",
                slot.as_str(),
                PENDING_CANCEL_TIMEOUT.as_secs()
            );
            let _ = self
                .exec_tx
                .send(ExecutionCmd::ReconcileNow {
                    reason: "pending_cancel_timeout",
                })
                .await;
            let tracker = self.tracker_mut(slot);
            tracker.state = OrderState::Idle;
            tracker.cooldown_until = Some(Instant::now() + PENDING_TIMEOUT_COOLDOWN);
        }

        let current_state = self.tracker(slot).state.clone();
        match current_state {
            OrderState::Idle => {
                if let Some(desired) = self.tracker(slot).desired.clone() {
                    let cmd = ExecutionCmd::ExecuteIntent {
                        intent: TradeIntent {
                            side: desired.side,
                            direction: desired.direction,
                            urgency: TradeUrgency::MakerPostOnly,
                            size: desired.size,
                            price: Some(desired.price),
                            expected_fill_price: None,
                            purpose: match desired.reason {
                                BidReason::Provide => TradePurpose::Provide,
                                BidReason::OracleLagProvide => TradePurpose::OracleLagSnipe,
                                BidReason::Hedge => TradePurpose::Hedge,
                            },
                            local_unreleased_matched_notional_usdc: self
                                .tracker(slot)
                                .pair_arb_local_unreleased_matched_notional_usdc,
                        },
                    };
                    let tracker = self.tracker_mut(slot);
                    tracker.state = OrderState::PendingSubmit(desired.clone());
                    tracker.last_action = Instant::now();
                    tracker.pair_arb_local_unreleased_matched_notional_usdc = 0.0;
                    let _ = self.exec_tx.send(cmd).await;
                }
            }
            OrderState::PendingSubmit(_) => {}
            OrderState::Live(live) => {
                if let Some(desired) = self.tracker(slot).desired.clone() {
                    let should_reprice = if live.reason == BidReason::OracleLagProvide
                        && desired.reason == BidReason::OracleLagProvide
                    {
                        self.oracle_lag_live_reprice_needed(slot, &live, &desired)
                    } else {
                        live != desired || live.reason != desired.reason
                    };
                    if should_reprice {
                        let _ = self
                            .exec_tx
                            .send(ExecutionCmd::CancelSlot {
                                slot,
                                reason: CancelReason::Reprice,
                            })
                            .await;
                        let tracker = self.tracker_mut(slot);
                        tracker.state = OrderState::PendingCancel(Some(live));
                        tracker.last_action = Instant::now();
                    }
                } else {
                    let clear_reason = self.tracker(slot).clear_reason;
                    let _ = self
                        .exec_tx
                        .send(ExecutionCmd::CancelSlot {
                            slot,
                            reason: clear_reason,
                        })
                        .await;
                    let tracker = self.tracker_mut(slot);
                    tracker.state = OrderState::PendingCancel(Some(live));
                    tracker.last_action = Instant::now();
                }
            }
            OrderState::PendingCancel(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    fn target(slot: OrderSlot, price: f64, size: f64, reason: BidReason) -> DesiredTarget {
        DesiredTarget {
            side: slot.side,
            direction: slot.direction,
            price,
            size,
            reason,
        }
    }

    #[tokio::test]
    async fn test_one_shot_taker_dispatches_execution_cmd() {
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (exec_tx, mut exec_rx) = mpsc::channel(8);
        let (result_tx, result_rx) = mpsc::channel(8);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(8);

        let om = OrderManager::new(cmd_rx, exec_tx, result_rx, slot_release_tx);
        let h = tokio::spawn(om.run());

        let _ = cmd_tx
            .send(OrderManagerCmd::OneShotTakerHedge {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                size: 2.0,
                purpose: TradePurpose::Hedge,
                limit_price: None,
                expected_fill_price: None,
            })
            .await;

        let cmd = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        match cmd {
            ExecutionCmd::ExecuteIntent { intent } => {
                assert_eq!(intent.side, Side::Yes);
                assert_eq!(intent.direction, TradeDirection::Buy);
                assert_eq!(intent.urgency, TradeUrgency::TakerFak);
                assert!((intent.size - 2.0).abs() < 1e-9);
            }
            other => panic!("expected ExecuteIntent(TakerFak), got {:?}", other),
        }

        let _ = result_tx
            .send(OrderResult::TakerHedgeDone { side: Side::Yes })
            .await;
        drop(cmd_tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_one_shot_taker_cancels_live_then_places() {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let om = OrderManager::new(cmd_rx, exec_tx, result_rx, slot_release_tx);
        let h = tokio::spawn(om.run());

        let slot = OrderSlot::YES_BUY;
        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                slot,
                0.45,
                5.0,
                BidReason::Provide,
            )))
            .await;
        let first = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(first, ExecutionCmd::ExecuteIntent { .. }));
        let _ = result_tx
            .send(OrderResult::OrderPlaced {
                slot,
                target: target(slot, 0.45, 5.0, BidReason::Provide),
            })
            .await;

        let _ = cmd_tx
            .send(OrderManagerCmd::OneShotTakerHedge {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                size: 2.0,
                purpose: TradePurpose::Hedge,
                limit_price: None,
                expected_fill_price: None,
            })
            .await;

        let cancel = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(
            cancel,
            ExecutionCmd::CancelSlot {
                slot: OrderSlot::YES_BUY,
                ..
            }
        ));

        let _ = result_tx.send(OrderResult::CancelAck { slot }).await;
        let taker = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        match taker {
            ExecutionCmd::ExecuteIntent { intent } => {
                assert_eq!(intent.side, Side::Yes);
                assert_eq!(intent.direction, TradeDirection::Buy);
                assert_eq!(intent.urgency, TradeUrgency::TakerFak);
                assert!((intent.size - 2.0).abs() < 1e-9);
            }
            other => panic!("expected ExecuteIntent(TakerFak), got {:?}", other),
        }

        drop(cmd_tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_sell_slot_waits_for_post_fill_warmup() {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let om = OrderManager::new(cmd_rx, exec_tx, result_rx, slot_release_tx);
        let h = tokio::spawn(om.run());

        let _ = result_tx
            .send(OrderResult::OrderFilled {
                slot: OrderSlot::NO_BUY,
            })
            .await;

        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                OrderSlot::NO_SELL,
                0.55,
                5.0,
                BidReason::Provide,
            )))
            .await;

        let warmup_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        loop {
            let now = tokio::time::Instant::now();
            if now >= warmup_deadline {
                break;
            }
            let remaining = warmup_deadline - now;
            match timeout(remaining, exec_rx.recv()).await {
                Err(_) | Ok(None) => break,
                Ok(Some(ExecutionCmd::ExecuteIntent { intent }))
                    if intent.side == Side::No && intent.direction == TradeDirection::Sell =>
                {
                    panic!(
                        "NO sell should stay blocked during warmup, got {:?}",
                        intent
                    );
                }
                Ok(Some(_)) => continue,
            }
        }

        let ready_deadline = tokio::time::Instant::now() + Duration::from_millis(2_000);
        loop {
            let now = tokio::time::Instant::now();
            if now >= ready_deadline {
                panic!("timed out waiting for delayed NO sell ExecuteIntent");
            }
            let remaining = ready_deadline - now;
            match timeout(remaining, exec_rx.recv()).await {
                Ok(Some(ExecutionCmd::ExecuteIntent { intent })) => {
                    if intent.side == Side::No && intent.direction == TradeDirection::Sell {
                        assert_eq!(intent.urgency, TradeUrgency::MakerPostOnly);
                        assert_eq!(intent.price, Some(0.55));
                        break;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => panic!("timed out waiting for delayed NO sell ExecuteIntent"),
            }
        }

        drop(cmd_tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_buy_slot_reopen_cooldown_delays_post_fill_reentry() {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let om = OrderManager::with_buy_fill_reopen_cooldown(
            cmd_rx,
            exec_tx,
            result_rx,
            slot_release_tx,
            Duration::from_millis(300),
        );
        let h = tokio::spawn(om.run());

        let _ = result_tx
            .send(OrderResult::OrderFilled {
                slot: OrderSlot::NO_BUY,
            })
            .await;

        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                OrderSlot::NO_BUY,
                0.37,
                96.0,
                BidReason::Provide,
            )))
            .await;

        let early_deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        loop {
            let now = tokio::time::Instant::now();
            if now >= early_deadline {
                break;
            }
            let remaining = early_deadline - now;
            match timeout(remaining, exec_rx.recv()).await {
                Err(_) | Ok(None) => break,
                Ok(Some(ExecutionCmd::ExecuteIntent { intent }))
                    if intent.side == Side::No && intent.direction == TradeDirection::Buy =>
                {
                    panic!(
                        "NO buy should stay blocked during post-fill reopen cooldown, got {:?}",
                        intent
                    );
                }
                Ok(Some(_)) => continue,
            }
        }

        let ready_deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
        loop {
            let now = tokio::time::Instant::now();
            if now >= ready_deadline {
                panic!("timed out waiting for delayed NO buy ExecuteIntent");
            }
            let remaining = ready_deadline - now;
            match timeout(remaining, exec_rx.recv()).await {
                Ok(Some(ExecutionCmd::ExecuteIntent { intent })) => {
                    if intent.side == Side::No && intent.direction == TradeDirection::Buy {
                        assert_eq!(intent.urgency, TradeUrgency::MakerPostOnly);
                        assert_eq!(intent.price, Some(0.37));
                        break;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => panic!("timed out waiting for delayed NO buy ExecuteIntent"),
            }
        }

        drop(cmd_tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_buy_slot_late_order_placed_is_ignored_during_reopen_cooldown() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, _exec_rx) = mpsc::channel(16);
        let (_result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let mut om = OrderManager::with_buy_fill_reopen_cooldown(
            cmd_rx,
            exec_tx,
            result_rx,
            slot_release_tx,
            Duration::from_millis(300),
        );

        om.handle_filled(OrderSlot::NO_BUY).await;
        om.handle_placed(
            OrderSlot::NO_BUY,
            target(OrderSlot::NO_BUY, 0.37, 96.0, BidReason::Provide),
        )
        .await;

        let tracker = om.tracker(OrderSlot::NO_BUY);
        assert!(matches!(tracker.state, OrderState::Idle));
        assert!(tracker.desired.is_none());
    }

    #[tokio::test]
    async fn test_live_buy_reason_change_same_price_forces_reprice() {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let om = OrderManager::new(cmd_rx, exec_tx, result_rx, slot_release_tx);
        let h = tokio::spawn(om.run());

        let slot = OrderSlot::YES_BUY;
        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                slot,
                0.52,
                96.0,
                BidReason::Provide,
            )))
            .await;

        let first = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(first, ExecutionCmd::ExecuteIntent { .. }));

        let _ = result_tx
            .send(OrderResult::OrderPlaced {
                slot,
                target: target(slot, 0.52, 96.0, BidReason::Provide),
            })
            .await;

        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                slot,
                0.52,
                96.0,
                BidReason::Hedge,
            )))
            .await;

        let second = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(
            second,
            ExecutionCmd::CancelSlot {
                slot: OrderSlot::YES_BUY,
                reason: CancelReason::Reprice,
            }
        ));

        drop(cmd_tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn test_slot_busy_transitions_to_cancel_instead_of_retry_loop() {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (exec_tx, mut exec_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = mpsc::channel(16);
        let (slot_release_tx, _slot_release_rx) = mpsc::channel(16);

        let om = OrderManager::new(cmd_rx, exec_tx, result_rx, slot_release_tx);
        let h = tokio::spawn(om.run());

        let slot = OrderSlot::YES_BUY;
        let _ = cmd_tx
            .send(OrderManagerCmd::SetTarget(target(
                slot,
                0.39,
                5.0,
                BidReason::Provide,
            )))
            .await;

        let first = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(first, ExecutionCmd::ExecuteIntent { .. }));

        let _ = result_tx.send(OrderResult::SlotBusy { slot }).await;

        let second = timeout(Duration::from_millis(100), exec_rx.recv())
            .await
            .expect("timeout")
            .expect("cmd");
        assert!(matches!(
            second,
            ExecutionCmd::CancelSlot {
                slot: OrderSlot::YES_BUY,
                reason: CancelReason::Reprice,
            }
        ));

        let no_more = timeout(Duration::from_millis(250), exec_rx.recv()).await;
        assert!(
            no_more.is_err(),
            "slot busy should not immediately fall back into another ExecuteIntent"
        );

        drop(cmd_tx);
        let _ = h.await;
    }
}
