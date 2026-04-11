use tracing::info;

use super::*;

const PAIR_ARB_RISK_OPEN_CUTOFF_SECS: u64 = 90;

impl StrategyCoordinator {
    pub(super) fn apply_endgame_controls(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        st: &mut ExecutionState,
    ) {
        let phase_changed = st.endgame_phase != self.last_endgame_phase;
        if st.endgame_phase != self.last_endgame_phase {
            let remaining = self.seconds_to_market_end().unwrap_or_default();
            info!(
                "⏱️ Endgame phase: {:?} -> {:?} (t-{}s)",
                self.last_endgame_phase, st.endgame_phase, remaining
            );
            self.last_endgame_phase = st.endgame_phase;
        }
        if self.cfg.strategy == StrategyKind::PairArb {
            let risk_open_cutoff_active = self.pair_arb_risk_open_cutoff_active();
            let risk_open_cutoff_changed =
                risk_open_cutoff_active != self.pair_arb_last_risk_open_cutoff_active;
            if phase_changed || risk_open_cutoff_changed {
                self.slot_pair_arb_fill_recheck_pending[OrderSlot::YES_BUY.index()] = true;
                self.slot_pair_arb_fill_recheck_pending[OrderSlot::NO_BUY.index()] = true;
                self.pair_arb_bump_decision_epoch("phase_or_risk_window_changed");
            }
            if risk_open_cutoff_changed {
                info!(
                    "🧭 pair_arb risk_open_cutoff_changed={} (t-{}s)",
                    risk_open_cutoff_active,
                    self.seconds_to_market_end().unwrap_or_default(),
                );
            }
            self.pair_arb_last_risk_open_cutoff_active = risk_open_cutoff_active;
            if st.endgame_phase >= EndgamePhase::SoftClose {
                // PairArb only adopts the minimal SoftClose semantics:
                // block risk-increasing buys late in the round, but keep
                // pairing / risk-reducing buys alive. Do not enter HardClose,
                // maker-repair, or taker de-risk modes.
                self.apply_tail_risk_gate(inv, st);
            } else {
                self.edge_hold_state = None;
            }
            return;
        }
        if st.endgame_phase >= EndgamePhase::SoftClose {
            // Tail mode: stop risk-increasing intents but keep non-increasing maker repair alive.
            self.apply_tail_risk_gate(inv, st);
        }
        if st.endgame_phase >= EndgamePhase::HardClose {
            match self.hard_close_action(inv, ub, st.endgame_phase) {
                HardCloseAction::None => {}
                HardCloseAction::Keep {
                    side,
                    ratio,
                    reason,
                } => {
                    st.block_maker_hedge = true;
                    if reason == "entry_keep" {
                        info!(
                            "⏱️ Endgame keep-mode: side={:?} ratio={:.3} >= keep_mult={:.3}",
                            side, ratio, self.cfg.endgame_edge_keep_mult
                        );
                    }
                    st.disable_all_provide_with_reason(CancelReason::EndgameRiskGate);
                }
                HardCloseAction::MakerRepair {
                    side,
                    ratio,
                    reason,
                } => {
                    if reason == "entry_maker_repair" {
                        info!(
                            "🛠️ Endgame maker-repair mode: side={:?} ratio={:.3} t_rem={}s (min={}s)",
                            side,
                            ratio,
                            self.seconds_to_market_end().unwrap_or_default(),
                            self.cfg.endgame_maker_repair_min_secs
                        );
                    }
                }
                HardCloseAction::ForceTaker {
                    side,
                    size,
                    ratio,
                    reason,
                } => {
                    st.force_taker_side = Some(side);
                    st.force_taker_size = size;
                    st.block_maker_hedge = true;
                    if matches!(reason, "entry_below_keep" | "edge_drop_below_exit") {
                        info!(
                            "⚡ Endgame force taker: side={:?} size={:.2} ratio={:.3} reason={} keep={:.3} exit={:.3}",
                            side,
                            size,
                            ratio,
                            reason,
                            self.cfg.endgame_edge_keep_mult,
                            self.cfg.endgame_edge_exit_mult
                        );
                    }
                    st.disable_all_provide_with_reason(CancelReason::EndgameRiskGate);
                }
            }
        } else {
            self.edge_hold_state = None;
        }
    }

    pub(super) fn seconds_to_market_end(&self) -> Option<u64> {
        let end_ts = self.cfg.market_end_ts?;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Some(end_ts.saturating_sub(now_secs))
    }

    pub(super) fn endgame_phase(&self) -> EndgamePhase {
        let Some(remaining) = self.seconds_to_market_end() else {
            return EndgamePhase::Normal;
        };

        if remaining <= self.cfg.endgame_freeze_secs {
            EndgamePhase::Freeze
        } else if remaining <= self.cfg.endgame_hard_close_secs {
            EndgamePhase::HardClose
        } else if remaining <= self.cfg.endgame_soft_close_secs {
            EndgamePhase::SoftClose
        } else {
            EndgamePhase::Normal
        }
    }

    pub(crate) fn pair_arb_risk_open_cutoff_active(&self) -> bool {
        if self.cfg.strategy != StrategyKind::PairArb {
            return false;
        }
        self.seconds_to_market_end()
            .map(|remaining| remaining <= PAIR_ARB_RISK_OPEN_CUTOFF_SECS)
            .unwrap_or(false)
    }

    pub(super) fn hard_close_action(
        &mut self,
        inv: &InventoryState,
        ub: &Book,
        phase: EndgamePhase,
    ) -> HardCloseAction {
        if phase < EndgamePhase::HardClose {
            self.edge_hold_state = None;
            return HardCloseAction::None;
        }

        let net = inv.net_diff;
        if net.abs() <= f64::EPSILON {
            self.edge_hold_state = None;
            return HardCloseAction::None;
        }

        let (side, best_bid, avg_cost) = if net > 0.0 {
            (Side::Yes, ub.yes_bid, inv.yes_avg_cost)
        } else {
            (Side::No, ub.no_bid, inv.no_avg_cost)
        };
        let size = net.abs();
        let ratio = if best_bid > 0.0 && avg_cost > 0.0 {
            best_bid / avg_cost
        } else {
            0.0
        };
        let maker_repair_ok = self.can_continue_maker_repair(side, ub);

        let mut state = match self.edge_hold_state {
            Some(s) if s.side == side => s,
            _ => {
                let keep_allowed = ratio >= self.cfg.endgame_edge_keep_mult;
                let next = EdgeHoldState { side, keep_allowed };
                self.edge_hold_state = Some(next);
                if keep_allowed {
                    return HardCloseAction::Keep {
                        side,
                        ratio,
                        reason: "entry_keep",
                    };
                }
                if maker_repair_ok {
                    return HardCloseAction::MakerRepair {
                        side,
                        ratio,
                        reason: "entry_maker_repair",
                    };
                }
                return HardCloseAction::ForceTaker {
                    side,
                    size,
                    ratio,
                    reason: "entry_below_keep",
                };
            }
        };

        if state.keep_allowed {
            if ratio < self.cfg.endgame_edge_exit_mult {
                state.keep_allowed = false;
                self.edge_hold_state = Some(state);
                if maker_repair_ok {
                    return HardCloseAction::MakerRepair {
                        side,
                        ratio,
                        reason: "edge_drop_enter_repair",
                    };
                }
                return HardCloseAction::ForceTaker {
                    side,
                    size,
                    ratio,
                    reason: "edge_drop_below_exit",
                };
            }
            self.edge_hold_state = Some(state);
            HardCloseAction::Keep {
                side,
                ratio,
                reason: "keep_hysteresis",
            }
        } else {
            if maker_repair_ok {
                return HardCloseAction::MakerRepair {
                    side,
                    ratio,
                    reason: "de_risk_maker_repair",
                };
            }
            HardCloseAction::ForceTaker {
                side,
                size,
                ratio,
                reason: "de_risk_mode",
            }
        }
    }

    fn apply_tail_risk_gate(&self, inv: &InventoryState, st: &mut ExecutionState) {
        if !self.tail_intent_is_risk_non_increasing(inv, st.intent_yes) {
            st.block_provide(Side::Yes, CancelReason::EndgameRiskGate);
        }
        if !self.tail_intent_is_risk_non_increasing(inv, st.intent_no) {
            st.block_provide(Side::No, CancelReason::EndgameRiskGate);
        }
    }

    fn tail_intent_is_risk_non_increasing(
        &self,
        inv: &InventoryState,
        intent: Option<StrategyIntent>,
    ) -> bool {
        let Some(intent) = intent else {
            return true;
        };
        if self.cfg.strategy == StrategyKind::PairArb {
            return !self.pair_arb_soft_close_blocks_side(inv, intent.side);
        }
        let cur_abs = inv.net_diff.abs();
        let next_abs = self.projected_abs_net_diff(inv.net_diff, intent);
        next_abs <= cur_abs + 1e-6
    }

    pub(super) fn pair_arb_soft_close_blocks_side(&self, inv: &InventoryState, side: Side) -> bool {
        let deadband = 0.5 * self.cfg.bid_size;
        if inv.net_diff.abs() <= deadband + PAIR_ARB_NET_EPS {
            return true;
        }
        if inv.net_diff > PAIR_ARB_NET_EPS {
            return side == Side::Yes;
        }
        if inv.net_diff < -PAIR_ARB_NET_EPS {
            return side == Side::No;
        }
        false
    }

    pub(super) fn projected_abs_net_diff(&self, net: f64, intent: StrategyIntent) -> f64 {
        let delta = match (intent.side, intent.direction) {
            (Side::Yes, TradeDirection::Buy) => intent.size,
            (Side::Yes, TradeDirection::Sell) => -intent.size,
            (Side::No, TradeDirection::Buy) => -intent.size,
            (Side::No, TradeDirection::Sell) => intent.size,
        };
        (net + delta).abs()
    }

    fn can_continue_maker_repair(&self, dominant_side: Side, ub: &Book) -> bool {
        let remaining = self.seconds_to_market_end().unwrap_or_default();
        if remaining <= self.cfg.endgame_maker_repair_min_secs {
            return false;
        }
        match dominant_side {
            Side::Yes => ub.yes_bid > 0.0 || ub.no_ask > 0.0,
            Side::No => ub.no_bid > 0.0 || ub.yes_ask > 0.0,
        }
    }
}
