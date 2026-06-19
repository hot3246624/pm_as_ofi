use crate::polymarket::coordinator::{Book, XuanB27DplusMode, XuanB27DplusStrategyConfig};
use crate::polymarket::messages::{OrderAttemptTrace, TradeDirection};
use crate::polymarket::strategy::StrategyKind;
use crate::polymarket::types::Side;

const EPS: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XuanB27DplusOrderPlanStatus {
    PreviewReady,
    Blocked,
}

impl XuanB27DplusOrderPlanStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreviewReady => "PREVIEW_READY",
            Self::Blocked => "BLOCKED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct XuanB27DplusOrderIntentPreview {
    pub side: Side,
    pub direction: TradeDirection,
    pub price: f64,
    pub size: f64,
    pub post_only: bool,
    pub allow_taker: bool,
    pub preview_only: bool,
    pub submitted: bool,
    pub trace: OrderAttemptTrace,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct XuanB27DplusOrderPlan {
    pub status: XuanB27DplusOrderPlanStatus,
    pub block_reasons: Vec<&'static str>,
    pub projected_open_cost_usdc: f64,
    pub intents: Vec<XuanB27DplusOrderIntentPreview>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct XuanB27DplusOrderPlanInput<'a> {
    pub cfg: &'a XuanB27DplusStrategyConfig,
    pub tick_size: f64,
    pub market_enabled: bool,
    pub explicit_canary_approval: bool,
    pub account_truth_ready: bool,
    pub book: Book,
    pub market_session_id: &'a str,
    pub candidate_prefix: &'a str,
    pub rolling_taker_failure_rate: Option<f64>,
    pub websocket_lag_ms: Option<u64>,
    pub order_ack_latency_ms: Option<u64>,
}

pub(crate) fn build_xuan_b27_dplus_canary_order_plan(
    input: XuanB27DplusOrderPlanInput<'_>,
) -> XuanB27DplusOrderPlan {
    let mut block_reasons = Vec::new();
    let cfg = input.cfg;

    if let Some(rate) = input.rolling_taker_failure_rate {
        if rate > 0.05 {
            block_reasons.push("rolling_taker_failure_rate_exceeds_threshold");
        }
    }
    if let Some(lag) = input.websocket_lag_ms {
        if lag > 1000 {
            block_reasons.push("websocket_lag_exceeds_threshold");
        }
    }
    if let Some(latency) = input.order_ack_latency_ms {
        if latency > 2000 {
            block_reasons.push("order_ack_latency_exceeds_threshold");
        }
    }

    if cfg.mode != XuanB27DplusMode::Canary {
        block_reasons.push("mode_not_canary");
    }
    if !input.explicit_canary_approval {
        block_reasons.push("explicit_canary_approval_missing");
    }
    if !input.market_enabled {
        block_reasons.push("market_disabled");
    }
    if cfg.stop_on_unknown && !input.account_truth_ready {
        block_reasons.push("account_truth_unknown");
    }
    if !cfg.post_only {
        block_reasons.push("post_only_required");
    }
    if cfg.allow_passive_taker {
        block_reasons.push("passive_taker_not_allowed");
    }
    if cfg.max_active_markets != 1 {
        block_reasons.push("max_active_markets_must_be_one");
    }
    if cfg.max_live_orders == 0 || cfg.max_live_orders > 2 {
        block_reasons.push("max_live_orders_must_be_one_or_two");
    }
    if cfg.max_open_cost_usdc <= EPS || cfg.max_open_cost_usdc > 100.0 {
        block_reasons.push("max_open_cost_usdc_outside_g2_cap");
    }
    if cfg.target_qty <= EPS || cfg.target_qty > 5.0 {
        block_reasons.push("target_qty_outside_g2_cap");
    }

    let yes = side_preview(
        Side::Yes,
        input.book.yes_ask,
        cfg,
        input.tick_size,
        input.market_session_id,
        input.candidate_prefix,
    );
    let no = side_preview(
        Side::No,
        input.book.no_ask,
        cfg,
        input.tick_size,
        input.market_session_id,
        input.candidate_prefix,
    );

    for reason in yes.block_reasons.iter().chain(no.block_reasons.iter()) {
        if !block_reasons.contains(reason) {
            block_reasons.push(reason);
        }
    }

    let projected_open_cost_usdc = if let (Some(yes_price), Some(no_price)) = (yes.price, no.price)
    {
        cfg.target_qty * (yes_price + no_price)
    } else {
        0.0
    };
    if projected_open_cost_usdc > cfg.max_open_cost_usdc + EPS {
        block_reasons.push("projected_open_cost_exceeds_cap");
    }

    let intents = if block_reasons.is_empty() {
        vec![
            intent_preview(
                Side::Yes,
                yes.price.expect("validated yes price"),
                cfg.target_qty,
                input.market_session_id,
                input.candidate_prefix,
            ),
            intent_preview(
                Side::No,
                no.price.expect("validated no price"),
                cfg.target_qty,
                input.market_session_id,
                input.candidate_prefix,
            ),
        ]
    } else {
        Vec::new()
    };

    XuanB27DplusOrderPlan {
        status: if intents.is_empty() {
            XuanB27DplusOrderPlanStatus::Blocked
        } else {
            XuanB27DplusOrderPlanStatus::PreviewReady
        },
        block_reasons,
        projected_open_cost_usdc,
        intents,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SidePreview {
    price: Option<f64>,
    block_reasons: Vec<&'static str>,
}

fn side_preview(
    side: Side,
    ask: f64,
    cfg: &XuanB27DplusStrategyConfig,
    tick_size: f64,
    market_session_id: &str,
    candidate_prefix: &str,
) -> SidePreview {
    let mut block_reasons = Vec::new();
    if !(ask.is_finite() && ask > 0.0 && ask < 1.0) {
        block_reasons.push(match side {
            Side::Yes => "yes_missing_ask",
            Side::No => "no_missing_ask",
        });
        return SidePreview {
            price: None,
            block_reasons,
        };
    }
    let price = quantize_down_to_tick(ask - cfg.edge, tick_size);
    if price <= 0.0 || price >= 1.0 {
        block_reasons.push(match side {
            Side::Yes => "yes_price_invalid",
            Side::No => "no_price_invalid",
        });
    }
    if price < cfg.seed_px_lo || price > cfg.seed_px_hi {
        block_reasons.push(match side {
            Side::Yes => "yes_price_outside_seed_band",
            Side::No => "no_price_outside_seed_band",
        });
    }
    if price + EPS >= ask {
        block_reasons.push(match side {
            Side::Yes => "yes_post_only_cross",
            Side::No => "no_post_only_cross",
        });
    }
    let candidate_id = candidate_id(side, candidate_prefix);
    let trace = order_attempt_trace(market_session_id, candidate_prefix, &candidate_id);
    if trace.order_attempt_id.is_empty() {
        block_reasons.push("trace_missing_order_attempt_id");
    }
    SidePreview {
        price: Some(price),
        block_reasons,
    }
}

fn intent_preview(
    side: Side,
    price: f64,
    size: f64,
    market_session_id: &str,
    candidate_prefix: &str,
) -> XuanB27DplusOrderIntentPreview {
    let candidate_id = candidate_id(side, candidate_prefix);
    XuanB27DplusOrderIntentPreview {
        side,
        direction: TradeDirection::Buy,
        price,
        size,
        post_only: true,
        allow_taker: false,
        preview_only: true,
        submitted: false,
        trace: order_attempt_trace(market_session_id, candidate_prefix, &candidate_id),
    }
}

fn candidate_id(side: Side, candidate_prefix: &str) -> String {
    let side = match side {
        Side::Yes => "yes",
        Side::No => "no",
    };
    format!("{candidate_prefix}:{side}")
}

fn order_attempt_trace(
    market_session_id: &str,
    candidate_prefix: &str,
    candidate_id: &str,
) -> OrderAttemptTrace {
    OrderAttemptTrace {
        strategy: StrategyKind::XuanB27Dplus.as_str().to_string(),
        run_id: candidate_prefix.to_string(),
        market_session_id: market_session_id.to_string(),
        candidate_id: candidate_id.to_string(),
        order_attempt_id: format!("{candidate_id}:attempt:preview"),
    }
}

fn quantize_down_to_tick(price: f64, tick_size: f64) -> f64 {
    let tick = tick_size.max(1e-9);
    let max_ticks = (1.0 / tick).floor() - 1.0;
    if max_ticks < 1.0 {
        return 0.0;
    }
    let min_price = tick;
    let max_price = max_ticks * tick;
    let clamped = price.clamp(min_price, max_price);
    let steps = ((clamped / tick) + 1e-9).floor();
    (steps * tick).clamp(min_price, max_price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::coordinator::CoordinatorConfig;

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

    fn input<'a>(cfg: &'a XuanB27DplusStrategyConfig) -> XuanB27DplusOrderPlanInput<'a> {
        XuanB27DplusOrderPlanInput {
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
        }
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_builds_preview_only_passive_buys() {
        let cfg = canary_cfg();
        let plan = build_xuan_b27_dplus_canary_order_plan(input(&cfg));

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::PreviewReady);
        assert_eq!(plan.status.as_str(), "PREVIEW_READY");
        assert!(plan.block_reasons.is_empty());
        assert!((plan.projected_open_cost_usdc - 4.7).abs() < 1e-9);
        assert_eq!(plan.intents.len(), 2);
        assert_eq!(plan.intents[0].side, Side::Yes);
        assert_eq!(plan.intents[0].direction, TradeDirection::Buy);
        assert_eq!(plan.intents[0].price, 0.42);
        assert_eq!(plan.intents[0].size, 5.0);
        assert!(plan.intents[0].post_only);
        assert!(!plan.intents[0].allow_taker);
        assert!(plan.intents[0].preview_only);
        assert!(!plan.intents[0].submitted);
        assert_eq!(
            plan.intents[0].trace.order_attempt_id,
            "xuan_b27_dplus:btc-updown-5m:17:yes:attempt:preview"
        );
        assert_eq!(plan.intents[1].side, Side::No);
        assert_eq!(plan.intents[1].price, 0.52);
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_blocks_without_explicit_approval() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.explicit_canary_approval = false;
        let plan = build_xuan_b27_dplus_canary_order_plan(input);

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan.intents.is_empty());
        assert!(plan
            .block_reasons
            .contains(&"explicit_canary_approval_missing"));
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_blocks_unknown_account_truth() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.account_truth_ready = false;
        let plan = build_xuan_b27_dplus_canary_order_plan(input);

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan.block_reasons.contains(&"account_truth_unknown"));
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_enforces_g2_caps() {
        let mut cfg = canary_cfg();
        cfg.max_live_orders = 3;
        cfg.target_qty = 6.0;
        let plan = build_xuan_b27_dplus_canary_order_plan(input(&cfg));

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan
            .block_reasons
            .contains(&"max_live_orders_must_be_one_or_two"));
        assert!(plan.block_reasons.contains(&"target_qty_outside_g2_cap"));
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_blocks_on_high_taker_failure_rate() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.rolling_taker_failure_rate = Some(0.06);
        let plan = build_xuan_b27_dplus_canary_order_plan(input);

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan.intents.is_empty());
        assert!(plan.block_reasons.contains(&"rolling_taker_failure_rate_exceeds_threshold"));
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_blocks_on_websocket_lag() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.websocket_lag_ms = Some(1500);
        let plan = build_xuan_b27_dplus_canary_order_plan(input);

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan.intents.is_empty());
        assert!(plan.block_reasons.contains(&"websocket_lag_exceeds_threshold"));
    }

    #[test]
    fn xuan_b27_dplus_canary_order_plan_blocks_on_order_ack_latency() {
        let cfg = canary_cfg();
        let mut input = input(&cfg);
        input.order_ack_latency_ms = Some(2500);
        let plan = build_xuan_b27_dplus_canary_order_plan(input);

        assert_eq!(plan.status, XuanB27DplusOrderPlanStatus::Blocked);
        assert!(plan.intents.is_empty());
        assert!(plan.block_reasons.contains(&"order_ack_latency_exceeds_threshold"));
    }
}
