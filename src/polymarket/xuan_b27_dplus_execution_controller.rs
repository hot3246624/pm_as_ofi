use crate::polymarket::xuan_b27_dplus_order_plan::{
    build_xuan_b27_dplus_canary_order_plan, XuanB27DplusOrderIntentPreview,
    XuanB27DplusOrderPlanInput, XuanB27DplusOrderPlanStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XuanB27DplusControllerStatus {
    Blocked,
    PreviewHeldControllerDisabled,
    PreviewReadyForOmsAdapter,
}

impl XuanB27DplusControllerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blocked => "BLOCKED",
            Self::PreviewHeldControllerDisabled => "PREVIEW_HELD_CONTROLLER_DISABLED",
            Self::PreviewReadyForOmsAdapter => "PREVIEW_READY_FOR_OMS_ADAPTER",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct XuanB27DplusControllerDecision {
    pub status: XuanB27DplusControllerStatus,
    pub block_reasons: Vec<&'static str>,
    pub planner_status: XuanB27DplusOrderPlanStatus,
    pub preview_intents: Vec<XuanB27DplusOrderIntentPreview>,
    pub controller_enabled: bool,
    pub oms_adapter_implemented: bool,
    pub oms_command_count: usize,
    pub orders_submitted: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct XuanB27DplusControllerInput<'a> {
    pub plan_input: XuanB27DplusOrderPlanInput<'a>,
    pub controller_enabled: bool,
    pub oms_adapter_implemented: bool,
}

pub(crate) fn evaluate_xuan_b27_dplus_controller(
    input: XuanB27DplusControllerInput<'_>,
) -> XuanB27DplusControllerDecision {
    let plan = build_xuan_b27_dplus_canary_order_plan(input.plan_input);
    let mut block_reasons = plan.block_reasons.clone();

    if !input.controller_enabled {
        push_unique(&mut block_reasons, "controller_disabled");
    }
    if !input.oms_adapter_implemented {
        push_unique(&mut block_reasons, "oms_adapter_not_implemented");
    }

    let preview_intents = if plan.status == XuanB27DplusOrderPlanStatus::PreviewReady {
        plan.intents
    } else {
        Vec::new()
    };

    let status = if preview_intents.is_empty() {
        XuanB27DplusControllerStatus::Blocked
    } else if input.controller_enabled && input.oms_adapter_implemented {
        XuanB27DplusControllerStatus::PreviewReadyForOmsAdapter
    } else {
        XuanB27DplusControllerStatus::PreviewHeldControllerDisabled
    };

    XuanB27DplusControllerDecision {
        status,
        block_reasons,
        planner_status: plan.status,
        preview_intents,
        controller_enabled: input.controller_enabled,
        oms_adapter_implemented: input.oms_adapter_implemented,
        oms_command_count: 0,
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

    fn input<'a>(cfg: &'a XuanB27DplusStrategyConfig) -> XuanB27DplusControllerInput<'a> {
        XuanB27DplusControllerInput {
            plan_input: XuanB27DplusOrderPlanInput {
                cfg,
                tick_size: 0.01,
                market_enabled: true,
                explicit_canary_approval: true,
                account_truth_ready: true,
                book: book(),
                market_session_id: "btc-updown-5m:cond",
                candidate_prefix: "xuan_b27_dplus:btc-updown-5m:17",
            },
            controller_enabled: false,
            oms_adapter_implemented: false,
        }
    }

    #[test]
    fn xuan_b27_dplus_controller_holds_previews_when_disabled() {
        let cfg = canary_cfg();
        let decision = evaluate_xuan_b27_dplus_controller(input(&cfg));

        assert_eq!(
            decision.status,
            XuanB27DplusControllerStatus::PreviewHeldControllerDisabled
        );
        assert_eq!(decision.status.as_str(), "PREVIEW_HELD_CONTROLLER_DISABLED");
        assert_eq!(
            decision.planner_status,
            XuanB27DplusOrderPlanStatus::PreviewReady
        );
        assert_eq!(decision.preview_intents.len(), 2);
        assert!(decision.block_reasons.contains(&"controller_disabled"));
        assert!(decision
            .block_reasons
            .contains(&"oms_adapter_not_implemented"));
        assert_eq!(decision.oms_command_count, 0);
        assert!(!decision.orders_submitted);
    }

    #[test]
    fn xuan_b27_dplus_controller_readies_previews_for_oms_adapter() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.controller_enabled = true;
        input.oms_adapter_implemented = true;
        let decision = evaluate_xuan_b27_dplus_controller(input);

        assert_eq!(
            decision.status,
            XuanB27DplusControllerStatus::PreviewReadyForOmsAdapter
        );
        assert_eq!(decision.status.as_str(), "PREVIEW_READY_FOR_OMS_ADAPTER");
        assert_eq!(decision.preview_intents.len(), 2);
        assert!(decision.block_reasons.is_empty());
        assert_eq!(decision.oms_command_count, 0);
        assert!(!decision.orders_submitted);
    }

    #[test]
    fn xuan_b27_dplus_controller_blocks_missing_explicit_approval() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.plan_input.explicit_canary_approval = false;
        let decision = evaluate_xuan_b27_dplus_controller(input);

        assert_eq!(decision.status, XuanB27DplusControllerStatus::Blocked);
        assert!(decision.preview_intents.is_empty());
        assert!(decision
            .block_reasons
            .contains(&"explicit_canary_approval_missing"));
        assert_eq!(decision.oms_command_count, 0);
        assert!(!decision.orders_submitted);
    }

    #[test]
    fn xuan_b27_dplus_controller_blocks_unknown_account_truth() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.plan_input.account_truth_ready = false;
        let decision = evaluate_xuan_b27_dplus_controller(input);

        assert_eq!(decision.status, XuanB27DplusControllerStatus::Blocked);
        assert!(decision.preview_intents.is_empty());
        assert!(decision.block_reasons.contains(&"account_truth_unknown"));
        assert_eq!(decision.oms_command_count, 0);
    }

    #[test]
    fn xuan_b27_dplus_controller_blocks_risk_cap_violations() {
        let mut cfg = canary_cfg();
        cfg.max_live_orders = 3;
        cfg.max_open_cost_usdc = 0.0;
        let decision = evaluate_xuan_b27_dplus_controller(input(&cfg));

        assert_eq!(decision.status, XuanB27DplusControllerStatus::Blocked);
        assert!(decision.preview_intents.is_empty());
        assert!(decision
            .block_reasons
            .contains(&"max_live_orders_must_be_one_or_two"));
        assert!(decision
            .block_reasons
            .contains(&"max_open_cost_usdc_outside_g2_cap"));
        assert_eq!(decision.oms_command_count, 0);
    }
}
