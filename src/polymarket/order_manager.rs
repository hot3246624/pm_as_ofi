use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::messages::{
    BidReason, CancelReason, DesiredTarget, ExecutionCmd, OrderManagerCmd, OrderResult,
};
use super::types::Side;

const PENDING_SUBMIT_TIMEOUT: Duration = Duration::from_secs(8);
const PENDING_CANCEL_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, PartialEq)]
pub enum OrderState {
    /// No target, no live orders, no pending ops.
    Idle,
    /// We have sent a PlacePostOnlyBid to Executor, waiting for it to land.
    PendingSubmit(DesiredTarget),
    /// Order is confirmed placed on the network.
    Live(DesiredTarget),
    /// We have sent a CancelSide to Executor, waiting for CancelAck or failure.
    PendingCancel(Option<DesiredTarget>),
}

#[derive(Debug)]
pub struct SideTracker {
    pub side: Side,
    /// The state the Coordinator requested. If None, it means no order desired.
    pub desired: Option<DesiredTarget>,
    /// The physical state tracking the Executor's lifecycle.
    pub state: OrderState,
    pub last_action: Instant,
    /// BUG 2 FIX: Respect the cooldown sent by Executor on balance/allowance errors.
    /// Pump is a no-op until this instant passes.
    pub cooldown_until: Option<Instant>,
}

impl SideTracker {
    pub fn new(side: Side) -> Self {
        Self {
            side,
            desired: None,
            state: OrderState::Idle,
            last_action: Instant::now(),
            cooldown_until: None,
        }
    }
}

pub struct OrderManager {
    yes: SideTracker,
    no: SideTracker,

    cmd_rx: mpsc::Receiver<OrderManagerCmd>,
    exec_tx: mpsc::Sender<ExecutionCmd>,
    /// Receive order failure/success/ack from Executor
    result_rx: mpsc::Receiver<OrderResult>,
}

impl OrderManager {
    pub fn new(
        cmd_rx: mpsc::Receiver<OrderManagerCmd>,
        exec_tx: mpsc::Sender<ExecutionCmd>,
        result_rx: mpsc::Receiver<OrderResult>,
    ) -> Self {
        Self {
            yes: SideTracker::new(Side::Yes),
            no: SideTracker::new(Side::No),
            cmd_rx,
            exec_tx,
            result_rx,
        }
    }

    pub async fn run(mut self) {
        info!("🚦 OrderManager [OMS] started");
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(OrderManagerCmd::SetTarget(t)) => {
                            let side = t.side;
                            self.handle_target(t).await;
                            self.pump(side).await;
                        }
                        Some(OrderManagerCmd::CancelAll) => {
                            self.yes.desired = None;
                            self.no.desired = None;
                            self.pump(Side::Yes).await;
                            self.pump(Side::No).await;
                        }
                        None => break, // Channel closed
                    }
                }
                result = self.result_rx.recv() => {
                    match result {
                        Some(OrderResult::OrderPlaced { side, target }) => {
                            self.handle_placed(side, target).await;
                        }
                        Some(OrderResult::OrderFailed { side, cooldown_ms }) => {
                            self.handle_failed(side, cooldown_ms).await;
                        }
                        Some(OrderResult::OrderFilled { side }) => {
                            self.handle_filled(side).await;
                        }
                        Some(OrderResult::CancelAck { side }) => {
                            self.handle_cancel_ack(side).await;
                        }
                        None => break, // Ignore
                    }
                }
            }
        }
        info!("🚦 OrderManager shutting down");
    }

    async fn handle_target(&mut self, target: DesiredTarget) {
        let tracker = match target.side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };

        if target.price <= 0.0 || target.size <= 0.0 {
            tracker.desired = None;
        } else {
            tracker.desired = Some(target);
        }
    }

    async fn handle_placed(&mut self, side: Side, target: DesiredTarget) {
        let tracker = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        // Transition from PendingSubmit to Live.
        // If we are already in some other state (like PendingCancel), do not overwrite blindly,
        // but typically OrderPlaced follows PendingSubmit.
        if let OrderState::PendingSubmit(_) = tracker.state {
            tracker.state = OrderState::Live(target);
            info!("✅ OMS: {:?} OrderPlaced -> Live", side);
        } else if let OrderState::Idle = tracker.state {
            tracker.state = OrderState::Live(target);
            info!("✅ OMS: {:?} OrderPlaced (late) -> Live", side);
        }
        self.pump(side).await;
    }

    async fn handle_failed(&mut self, side: Side, cooldown_ms: u64) {
        let tracker = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        tracker.state = OrderState::Idle;
        // BUG 2 FIX: Honor the cooldown from Executor (e.g. 30s for balance errors).
        // Previously this field was silently discarded, causing dense retries on hard rejects.
        if cooldown_ms > 0 {
            let until = Instant::now() + Duration::from_millis(cooldown_ms);
            tracker.cooldown_until = Some(until);
            warn!(
                "⏳ OMS: {:?} OrderFailed — cooldown {}s (hard reject)",
                side,
                cooldown_ms / 1000,
            );
        } else {
            warn!("⚠️ OMS: {:?} OrderFailed -> Resetting state to Idle", side);
        }
        self.pump(side).await;
    }

    async fn handle_filled(&mut self, side: Side) {
        let tracker = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        info!("✅ OMS: {:?} OrderFilled -> Slot freed", side);
        tracker.state = OrderState::Idle;
        // Coordinator must re-issue Target if needed on next tick
        tracker.desired = None;
        self.pump(side).await;
    }

    async fn handle_cancel_ack(&mut self, side: Side) {
        let tracker = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        info!("🗑️ OMS: {:?} CancelAck -> Idle", side);
        tracker.state = OrderState::Idle;
        self.pump(side).await;
    }

    /// Evaluates `desired` vs `state` and emits diff commands to `Executor` if safe.
    async fn pump(&mut self, side: Side) {
        let tracker = match side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };

        // BUG 2 FIX: Respect cooldown window set on balance/allowance errors.
        if let Some(until) = tracker.cooldown_until {
            if Instant::now() < until {
                return; // Still cooling down — do not retry.
            }
            tracker.cooldown_until = None; // Cooldown expired; resume normal operation.
        }

        // Watchdog: prevent permanent deadlock if Executor feedback is dropped.
        // Safety is preserved by Executor's open_orders guard (no double placement).
        let submit_timed_out = matches!(tracker.state, OrderState::PendingSubmit(_))
            && tracker.last_action.elapsed() > PENDING_SUBMIT_TIMEOUT;
        if submit_timed_out {
            warn!(
                "⚠️ OMS: {:?} PendingSubmit timeout (>{}s) — forcing progress",
                side,
                PENDING_SUBMIT_TIMEOUT.as_secs()
            );
            if tracker.desired.is_none() {
                let _ = self
                    .exec_tx
                    .send(ExecutionCmd::CancelSide {
                        side,
                        reason: CancelReason::Reprice,
                    })
                    .await;
                tracker.state = OrderState::PendingCancel(None);
                tracker.last_action = Instant::now();
            } else {
                tracker.state = OrderState::Idle;
            }
        }

        let cancel_timed_out = matches!(tracker.state, OrderState::PendingCancel(_))
            && tracker.last_action.elapsed() > PENDING_CANCEL_TIMEOUT;
        if cancel_timed_out {
            warn!(
                "⚠️ OMS: {:?} PendingCancel timeout (>{}s) — resetting to Idle",
                side,
                PENDING_CANCEL_TIMEOUT.as_secs()
            );
            tracker.state = OrderState::Idle;
        }

        let current_state = tracker.state.clone();

        match current_state {
            OrderState::Idle => {
                if let Some(desired) = &tracker.desired {
                    let cmd = ExecutionCmd::PlacePostOnlyBid {
                        side: desired.side,
                        price: desired.price,
                        size: desired.size,
                        reason: BidReason::Provide,
                    };
                    tracker.state = OrderState::PendingSubmit(desired.clone());
                    tracker.last_action = Instant::now();
                    let _ = self.exec_tx.send(cmd).await;
                }
            }
            OrderState::PendingSubmit(pending) => {
                if let Some(desired) = &tracker.desired {
                    if pending == *desired {
                        // Already submitting what we want. Wait.
                    } else {
                        // We are submitting A, but now want B.
                        // Network logic: Wait until A is Live, or fails, before we cancel A and send B.
                        // OrderManager buffers `desired=B` and executes it on the next valid state transition.
                    }
                } else {
                    // Submitting A, but want None. Need to wait for Live/Failed to Cancel.
                }
            }
            OrderState::Live(live) => {
                if let Some(desired) = &tracker.desired {
                    if live == *desired {
                        // Perfect, our live order matches what we want.
                    } else {
                        // Mismatch! Must cancel `live` first.
                        let _ = self
                            .exec_tx
                            .send(ExecutionCmd::CancelSide {
                                side,
                                reason: CancelReason::Reprice,
                            })
                            .await;
                        tracker.state = OrderState::PendingCancel(Some(live));
                        tracker.last_action = Instant::now();
                    }
                } else {
                    // We have a live order, but want None.
                    let _ = self
                        .exec_tx
                        .send(ExecutionCmd::CancelSide {
                            side,
                            reason: CancelReason::InventoryLimit,
                        })
                        .await;
                    tracker.state = OrderState::PendingCancel(Some(live));
                    tracker.last_action = Instant::now();
                }
            }
            OrderState::PendingCancel(_) => {
                // Wait for CancelAck. Do nothing. If `desired` changed, we'll pick it up when Idle.
            }
        }
    }
}
