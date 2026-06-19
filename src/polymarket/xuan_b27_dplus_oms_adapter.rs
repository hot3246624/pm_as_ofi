use crate::polymarket::messages::{BidReason, DesiredTarget, OrderManagerCmd, TradeDirection};
use crate::polymarket::xuan_b27_dplus_execution_controller::{
    XuanB27DplusControllerDecision, XuanB27DplusControllerStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XuanB27DplusOmsAdapterStatus {
    Blocked,
    CommandsHeldAdapterDisabled,
    CommandsReady,
}

impl XuanB27DplusOmsAdapterStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blocked => "BLOCKED",
            Self::CommandsHeldAdapterDisabled => "COMMANDS_HELD_ADAPTER_DISABLED",
            Self::CommandsReady => "COMMANDS_READY",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct XuanB27DplusOmsAdapterDecision {
    pub status: XuanB27DplusOmsAdapterStatus,
    pub block_reasons: Vec<&'static str>,
    pub commands: Vec<OrderManagerCmd>,
    pub command_count: usize,
    pub orders_submitted: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct XuanB27DplusOmsAdapterInput<'a> {
    pub controller_decision: &'a XuanB27DplusControllerDecision,
    pub explicit_canary_approval: bool,
    pub account_truth_ready: bool,
    pub stop_on_unknown: bool,
    pub risk_caps_ready: bool,
    pub adapter_enabled: bool,
}

pub(crate) fn adapt_xuan_b27_dplus_to_oms_commands(
    input: XuanB27DplusOmsAdapterInput<'_>,
) -> XuanB27DplusOmsAdapterDecision {
    let decision = input.controller_decision;
    let mut block_reasons = decision.block_reasons.clone();

    if decision.status != XuanB27DplusControllerStatus::PreviewReadyForOmsAdapter {
        push_unique(&mut block_reasons, "controller_not_ready_for_oms_adapter");
    }
    if !input.explicit_canary_approval {
        push_unique(&mut block_reasons, "explicit_canary_approval_missing");
    }
    if input.stop_on_unknown && !input.account_truth_ready {
        push_unique(&mut block_reasons, "account_truth_unknown");
    }
    if !input.risk_caps_ready {
        push_unique(&mut block_reasons, "risk_caps_not_ready");
    }

    for intent in &decision.preview_intents {
        if !intent.preview_only || intent.submitted || !intent.post_only || intent.allow_taker {
            push_unique(&mut block_reasons, "preview_intent_not_safe_for_oms");
        }
        if intent.direction != TradeDirection::Buy {
            push_unique(&mut block_reasons, "only_passive_buy_intents_allowed");
        }
        if !(intent.price.is_finite() && intent.price > 0.0 && intent.price < 1.0) {
            push_unique(&mut block_reasons, "intent_price_invalid");
        }
        if !(intent.size.is_finite() && intent.size > 0.0) {
            push_unique(&mut block_reasons, "intent_size_invalid");
        }
        if intent.trace.order_attempt_id.is_empty() {
            push_unique(&mut block_reasons, "trace_missing_order_attempt_id");
        }
    }

    let adapter_only_missing = block_reasons.is_empty() && !input.adapter_enabled;
    if adapter_only_missing {
        push_unique(&mut block_reasons, "oms_adapter_disabled");
    }

    let commands = if block_reasons.is_empty() {
        decision
            .preview_intents
            .iter()
            .map(|intent| OrderManagerCmd::SetTargetWithTrace {
                target: DesiredTarget {
                    side: intent.side,
                    direction: intent.direction,
                    price: intent.price,
                    size: intent.size,
                    reason: BidReason::Provide,
                },
                trace: intent.trace.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };

    let status = if !commands.is_empty() {
        XuanB27DplusOmsAdapterStatus::CommandsReady
    } else if adapter_only_missing {
        XuanB27DplusOmsAdapterStatus::CommandsHeldAdapterDisabled
    } else {
        XuanB27DplusOmsAdapterStatus::Blocked
    };

    XuanB27DplusOmsAdapterDecision {
        status,
        command_count: commands.len(),
        commands,
        block_reasons,
        orders_submitted: false,
    }
}

fn push_unique(out: &mut Vec<&'static str>, value: &'static str) {
    if !out.contains(&value) {
        out.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::coordinator::{
        Book, CoordinatorConfig, XuanB27DplusMode, XuanB27DplusStrategyConfig,
    };
    use crate::polymarket::types::Side;
    use crate::polymarket::xuan_b27_dplus_execution_controller::{
        evaluate_xuan_b27_dplus_controller, XuanB27DplusControllerInput,
    };
    use crate::polymarket::xuan_b27_dplus_order_plan::XuanB27DplusOrderPlanInput;

    fn canary_cfg() -> XuanB27DplusStrategyConfig {
        let mut cfg = CoordinatorConfig::default().xuan_b27_dplus;
        cfg.mode = XuanB27DplusMode::Canary;
        cfg.target_qty = 5.0;
        cfg.edge = 0.04;
        cfg.max_open_cost_usdc = 50.0;
        cfg.max_strategy_exposure_usdc = 100.0;
        cfg.max_active_markets = 1;
        cfg.max_live_orders = 2;
        cfg.post_only = true;
        cfg.allow_passive_taker = false;
        cfg.stop_on_unknown = true;
        cfg
    }

    fn book() -> Book {
        Book {
            yes_bid: 0.42,
            yes_ask: 0.46,
            no_bid: 0.52,
            no_ask: 0.56,
        }
    }

    fn controller_decision(cfg: &XuanB27DplusStrategyConfig) -> XuanB27DplusControllerDecision {
        evaluate_xuan_b27_dplus_controller(XuanB27DplusControllerInput {
            plan_input: XuanB27DplusOrderPlanInput {
                cfg,
                tick_size: 0.01,
                market_enabled: true,
                explicit_canary_approval: true,
                account_truth_ready: true,
                book: book(),
                market_session_id: "btc-updown-5m:cond",
                candidate_prefix: "xuan_b27_dplus:btc-updown-5m:17",
                rolling_taker_failure_rate: None,
                websocket_lag_ms: None,
                order_ack_latency_ms: None,
            },
            controller_enabled: true,
            oms_adapter_implemented: true,
        })
    }

    fn adapter_input<'a>(
        decision: &'a XuanB27DplusControllerDecision,
    ) -> XuanB27DplusOmsAdapterInput<'a> {
        XuanB27DplusOmsAdapterInput {
            controller_decision: decision,
            explicit_canary_approval: true,
            account_truth_ready: true,
            stop_on_unknown: true,
            risk_caps_ready: true,
            adapter_enabled: true,
        }
    }

    #[test]
    fn xuan_b27_dplus_oms_adapter_builds_commands_only_after_all_gates() {
        let cfg = canary_cfg();
        let controller = controller_decision(&cfg);
        let decision = adapt_xuan_b27_dplus_to_oms_commands(adapter_input(&controller));

        assert_eq!(decision.status, XuanB27DplusOmsAdapterStatus::CommandsReady);
        assert_eq!(decision.status.as_str(), "COMMANDS_READY");
        assert_eq!(decision.command_count, 2);
        assert!(!decision.orders_submitted);
        assert!(decision.block_reasons.is_empty());
        match &decision.commands[0] {
            OrderManagerCmd::SetTargetWithTrace { target, trace } => {
                assert_eq!(target.side, Side::Yes);
                assert_eq!(target.direction, TradeDirection::Buy);
                assert_eq!(target.price, 0.42);
                assert_eq!(target.size, 5.0);
                assert_eq!(target.reason, BidReason::Provide);
                assert_eq!(
                    trace.order_attempt_id,
                    "xuan_b27_dplus:btc-updown-5m:17:yes:attempt:preview"
                );
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn xuan_b27_dplus_oms_adapter_holds_commands_when_disabled() {
        let cfg = canary_cfg();
        let controller = controller_decision(&cfg);
        let mut input = adapter_input(&controller);
        input.adapter_enabled = false;
        let decision = adapt_xuan_b27_dplus_to_oms_commands(input);

        assert_eq!(
            decision.status,
            XuanB27DplusOmsAdapterStatus::CommandsHeldAdapterDisabled
        );
        assert_eq!(decision.command_count, 0);
        assert!(decision.commands.is_empty());
        assert!(decision.block_reasons.contains(&"oms_adapter_disabled"));
        assert!(!decision.orders_submitted);
    }

    #[test]
    fn xuan_b27_dplus_oms_adapter_blocks_missing_explicit_approval() {
        let cfg = canary_cfg();
        let controller = controller_decision(&cfg);
        let mut input = adapter_input(&controller);
        input.explicit_canary_approval = false;
        let decision = adapt_xuan_b27_dplus_to_oms_commands(input);

        assert_eq!(decision.status, XuanB27DplusOmsAdapterStatus::Blocked);
        assert!(decision.commands.is_empty());
        assert!(decision
            .block_reasons
            .contains(&"explicit_canary_approval_missing"));
        assert_eq!(decision.command_count, 0);
    }

    #[test]
    fn xuan_b27_dplus_oms_adapter_blocks_unknown_account_truth() {
        let cfg = canary_cfg();
        let controller = controller_decision(&cfg);
        let mut input = adapter_input(&controller);
        input.account_truth_ready = false;
        let decision = adapt_xuan_b27_dplus_to_oms_commands(input);

        assert_eq!(decision.status, XuanB27DplusOmsAdapterStatus::Blocked);
        assert!(decision.commands.is_empty());
        assert!(decision.block_reasons.contains(&"account_truth_unknown"));
        assert_eq!(decision.command_count, 0);
    }

    #[test]
    fn xuan_b27_dplus_oms_adapter_blocks_risk_caps_not_ready() {
        let mut cfg = canary_cfg();
        cfg.max_live_orders = 3;
        let controller = controller_decision(&cfg);
        let mut input = adapter_input(&controller);
        input.risk_caps_ready = false;
        let decision = adapt_xuan_b27_dplus_to_oms_commands(input);

        assert_eq!(decision.status, XuanB27DplusOmsAdapterStatus::Blocked);
        assert!(decision.commands.is_empty());
        assert!(decision.block_reasons.contains(&"risk_caps_not_ready"));
        assert!(decision
            .block_reasons
            .contains(&"controller_not_ready_for_oms_adapter"));
        assert_eq!(decision.command_count, 0);
    }
}
