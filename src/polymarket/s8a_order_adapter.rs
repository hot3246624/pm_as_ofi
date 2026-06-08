//! Native S8A order adapter contract.
//!
//! This module is intentionally pure and disabled by default. It does not sign,
//! submit, cancel, merge, redeem, fund, read secrets, or perform network IO. It
//! encodes the S8A-specific venue-order unit contract so the future effectful
//! wrapper does not reuse legacy executor market-buy share semantics.

use crate::polymarket::btc_completion_controller::BtcCompletionControllerReview;
use crate::polymarket::types::Side;

pub const S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT: bool = false;
pub const S8A_NATIVE_ORDER_ADAPTER_REVIEW_EVENT: &str = "s8a_native_order_adapter_review";
pub const S8A_SESSION_GATE_REVIEW_EVENT: &str = "s8a_session_gate_review";
pub const S8A_LIMIT_MIN_ORDER_SIZE_SHARES: f64 = 5.0;
pub const S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES: f64 = 5.0;
pub const S8A_MARKET_BUY_MIN_NOTIONAL_USDC: f64 = 1.0;
pub const S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES: f64 = 5.0;
pub const S8A_SEED_PX_LO: f64 = 0.05;
pub const S8A_SEED_PX_HI: f64 = 0.80;
pub const S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC: f64 = 5.0;
pub const S8A_SESSION_HARD_LOSS_CAP_USDC: f64 = 15.0;
pub const S8A_MAX_ROUNDS: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aAdapterOrderKind {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aAdapterOrderAction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aVenueOrderType {
    Gtc,
    Fak,
}

impl S8aVenueOrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gtc => "GTC",
            Self::Fak => "FAK",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum S8aVenueAmount {
    Shares(f64),
    UsdcNotional(f64),
}

impl S8aVenueAmount {
    pub fn value(&self) -> f64 {
        match self {
            Self::Shares(value) | Self::UsdcNotional(value) => *value,
        }
    }

    pub fn unit(&self) -> &'static str {
        match self {
            Self::Shares(_) => "SHARES",
            Self::UsdcNotional(_) => "USDC_NOTIONAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aNativeOrderAdapterRequest {
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub action: S8aAdapterOrderAction,
    pub order_kind: S8aAdapterOrderKind,
    pub limit_price: Option<f64>,
    pub limit_size_shares: Option<f64>,
    pub market_buy_notional_usdc: Option<f64>,
    pub market_sell_size_shares: Option<f64>,
    pub controller_admission_passed: bool,
    pub controller_review_execution_permitted: bool,
    pub source_guard_500_passed: bool,
    pub no_forced_complement: bool,
    pub official_order_minimums_verified: bool,
    pub fresh_exact_approval_hash_present: bool,
    pub approval_scope_matches_s8a: bool,
    pub effectful_submission_requested: bool,
    pub prints_secret_or_raw_signature: bool,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aNativeOrderAdapterBlockReason {
    MissingConditionId,
    MissingTokenId,
    ControllerAdmissionMissing,
    ControllerReviewPermitsExecution,
    SourceGuard500Missing,
    ForcedComplementRequested,
    OfficialOrderMinimumsNotVerified,
    EffectfulSubmissionRequestedWithoutFreshApproval,
    ApprovalScopeMismatch,
    SecretOrRawSignatureOutputAllowed,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    MissingOrInvalidLimitPrice,
    LimitPriceOutsideS8aSeedBand,
    LimitOrderSizeNotExactlyFiveShares,
    LimitSellUnsupportedForS8aEntry,
    MarketBuyNotionalBelowOneUsdc,
    MarketBuyAmountUnitMissing,
    MarketSellSharesBelowFive,
    MarketSellAmountUnitMissing,
}

impl S8aNativeOrderAdapterBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingConditionId => "BLOCK_MISSING_CONDITION_ID",
            Self::MissingTokenId => "BLOCK_MISSING_TOKEN_ID",
            Self::ControllerAdmissionMissing => "BLOCK_CONTROLLER_ADMISSION_MISSING",
            Self::ControllerReviewPermitsExecution => "BLOCK_CONTROLLER_REVIEW_PERMITS_EXECUTION",
            Self::SourceGuard500Missing => "BLOCK_SOURCE_GUARD_500_MISSING",
            Self::ForcedComplementRequested => "BLOCK_FORCED_COMPLEMENT_REQUESTED",
            Self::OfficialOrderMinimumsNotVerified => "BLOCK_OFFICIAL_ORDER_MINIMUMS_NOT_VERIFIED",
            Self::EffectfulSubmissionRequestedWithoutFreshApproval => {
                "BLOCK_EFFECTFUL_SUBMISSION_REQUESTED_WITHOUT_FRESH_APPROVAL"
            }
            Self::ApprovalScopeMismatch => "BLOCK_APPROVAL_SCOPE_MISMATCH",
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::MissingOrInvalidLimitPrice => "BLOCK_MISSING_OR_INVALID_LIMIT_PRICE",
            Self::LimitPriceOutsideS8aSeedBand => "BLOCK_LIMIT_PRICE_OUTSIDE_S8A_SEED_BAND",
            Self::LimitOrderSizeNotExactlyFiveShares => {
                "BLOCK_LIMIT_ORDER_SIZE_NOT_EXACTLY_FIVE_SHARES"
            }
            Self::LimitSellUnsupportedForS8aEntry => "BLOCK_LIMIT_SELL_UNSUPPORTED_FOR_S8A_ENTRY",
            Self::MarketBuyNotionalBelowOneUsdc => "BLOCK_MARKET_BUY_NOTIONAL_BELOW_ONE_USDC",
            Self::MarketBuyAmountUnitMissing => "BLOCK_MARKET_BUY_AMOUNT_UNIT_MISSING",
            Self::MarketSellSharesBelowFive => "BLOCK_MARKET_SELL_SHARES_BELOW_FIVE",
            Self::MarketSellAmountUnitMissing => "BLOCK_MARKET_SELL_AMOUNT_UNIT_MISSING",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aPreparedVenueOrder {
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub action: S8aAdapterOrderAction,
    pub order_type: S8aVenueOrderType,
    pub limit_price: Option<f64>,
    pub amount: S8aVenueAmount,
    pub execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aNativeOrderAdapterReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aNativeOrderAdapterBlockReason>,
    pub prepared_order: Option<S8aPreparedVenueOrder>,
    pub official_order_units_bound: bool,
    pub native_adapter_ready_for_exact_runtime: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_native_order_adapter(
    request: &S8aNativeOrderAdapterRequest,
) -> S8aNativeOrderAdapterReview {
    let mut block_reasons = Vec::new();

    if request.condition_id.trim().is_empty() {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::MissingConditionId);
    }
    if request.token_id.trim().is_empty() {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::MissingTokenId);
    }
    if !request.controller_admission_passed {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::ControllerAdmissionMissing);
    }
    if request.controller_review_execution_permitted {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::ControllerReviewPermitsExecution);
    }
    if !request.source_guard_500_passed {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::SourceGuard500Missing);
    }
    if !request.no_forced_complement {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::ForcedComplementRequested);
    }
    if !request.official_order_minimums_verified {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::OfficialOrderMinimumsNotVerified);
    }
    if request.effectful_submission_requested && !request.fresh_exact_approval_hash_present {
        block_reasons.push(
            S8aNativeOrderAdapterBlockReason::EffectfulSubmissionRequestedWithoutFreshApproval,
        );
    }
    if request.effectful_submission_requested && !request.approval_scope_matches_s8a {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::ApprovalScopeMismatch);
    }
    if request.prints_secret_or_raw_signature {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if request.uses_shared_ingress_or_shared_ws {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::SharedIngressOrSharedWsRequested);
    }
    if request.uses_c_artifacts {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::CArtifactsRequested);
    }

    let prepared_order = match (request.order_kind, request.action) {
        (S8aAdapterOrderKind::Limit, S8aAdapterOrderAction::Buy) => {
            limit_buy_order(request, &mut block_reasons)
        }
        (S8aAdapterOrderKind::Limit, S8aAdapterOrderAction::Sell) => {
            block_reasons.push(S8aNativeOrderAdapterBlockReason::LimitSellUnsupportedForS8aEntry);
            None
        }
        (S8aAdapterOrderKind::Market, S8aAdapterOrderAction::Buy) => {
            market_buy_order(request, &mut block_reasons)
        }
        (S8aAdapterOrderKind::Market, S8aAdapterOrderAction::Sell) => {
            market_sell_order(request, &mut block_reasons)
        }
    };

    let official_order_units_bound = prepared_order.as_ref().is_some_and(|order| {
        match (order.order_type, order.action, order.amount) {
            (S8aVenueOrderType::Gtc, S8aAdapterOrderAction::Buy, S8aVenueAmount::Shares(value)) => {
                (value - S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES).abs() <= 1e-9
            }
            (
                S8aVenueOrderType::Fak,
                S8aAdapterOrderAction::Buy,
                S8aVenueAmount::UsdcNotional(value),
            ) => value + 1e-9 >= S8A_MARKET_BUY_MIN_NOTIONAL_USDC,
            (
                S8aVenueOrderType::Fak,
                S8aAdapterOrderAction::Sell,
                S8aVenueAmount::Shares(value),
            ) => value + 1e-9 >= S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES,
            _ => false,
        }
    });

    S8aNativeOrderAdapterReview {
        event: S8A_NATIVE_ORDER_ADAPTER_REVIEW_EVENT,
        native_adapter_ready_for_exact_runtime: block_reasons.is_empty(),
        official_order_units_bound,
        prepared_order: if block_reasons.is_empty() {
            prepared_order
        } else {
            None
        },
        block_reasons,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aControllerAdmissionAdapterContext {
    pub yes_token_id: String,
    pub no_token_id: String,
    pub official_order_minimums_verified: bool,
    pub fresh_exact_approval_hash_present: bool,
    pub approval_scope_matches_s8a: bool,
    pub effectful_submission_requested: bool,
    pub prints_secret_or_raw_signature: bool,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
}

pub fn review_s8a_limit_entry_from_controller_admission(
    controller_review: &BtcCompletionControllerReview,
    context: &S8aControllerAdmissionAdapterContext,
) -> S8aNativeOrderAdapterReview {
    let Some(intent) = controller_review.intent.as_ref() else {
        return review_s8a_native_order_adapter(&S8aNativeOrderAdapterRequest {
            condition_id: String::new(),
            token_id: String::new(),
            side: Side::Yes,
            action: S8aAdapterOrderAction::Buy,
            order_kind: S8aAdapterOrderKind::Limit,
            limit_price: None,
            limit_size_shares: Some(S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES),
            market_buy_notional_usdc: None,
            market_sell_size_shares: None,
            controller_admission_passed: false,
            controller_review_execution_permitted: controller_review.execution_permitted,
            source_guard_500_passed: false,
            no_forced_complement: true,
            official_order_minimums_verified: context.official_order_minimums_verified,
            fresh_exact_approval_hash_present: context.fresh_exact_approval_hash_present,
            approval_scope_matches_s8a: context.approval_scope_matches_s8a,
            effectful_submission_requested: context.effectful_submission_requested,
            prints_secret_or_raw_signature: context.prints_secret_or_raw_signature,
            uses_shared_ingress_or_shared_ws: context.uses_shared_ingress_or_shared_ws,
            uses_c_artifacts: context.uses_c_artifacts,
        });
    };

    let token_id = match intent.side {
        Side::Yes => context.yes_token_id.clone(),
        Side::No => context.no_token_id.clone(),
    };

    review_s8a_native_order_adapter(&S8aNativeOrderAdapterRequest {
        condition_id: intent.condition_id.clone(),
        token_id,
        side: intent.side,
        action: S8aAdapterOrderAction::Buy,
        order_kind: S8aAdapterOrderKind::Limit,
        limit_price: Some(intent.price),
        limit_size_shares: Some(S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES),
        market_buy_notional_usdc: None,
        market_sell_size_shares: None,
        controller_admission_passed: true,
        controller_review_execution_permitted: controller_review.execution_permitted,
        source_guard_500_passed: true,
        no_forced_complement: true,
        official_order_minimums_verified: context.official_order_minimums_verified,
        fresh_exact_approval_hash_present: context.fresh_exact_approval_hash_present,
        approval_scope_matches_s8a: context.approval_scope_matches_s8a,
        effectful_submission_requested: context.effectful_submission_requested,
        prints_secret_or_raw_signature: context.prints_secret_or_raw_signature,
        uses_shared_ingress_or_shared_ws: context.uses_shared_ingress_or_shared_ws,
        uses_c_artifacts: context.uses_c_artifacts,
    })
}

fn limit_buy_order(
    request: &S8aNativeOrderAdapterRequest,
    block_reasons: &mut Vec<S8aNativeOrderAdapterBlockReason>,
) -> Option<S8aPreparedVenueOrder> {
    let price = match request.limit_price {
        Some(price) if price.is_finite() && price > 0.0 && price < 1.0 => price,
        _ => {
            block_reasons.push(S8aNativeOrderAdapterBlockReason::MissingOrInvalidLimitPrice);
            return None;
        }
    };
    if !(S8A_SEED_PX_LO..=S8A_SEED_PX_HI).contains(&price) {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::LimitPriceOutsideS8aSeedBand);
    }
    let shares = request.limit_size_shares.unwrap_or(0.0);
    if !shares.is_finite() || (shares - S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES).abs() > 1e-9 {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::LimitOrderSizeNotExactlyFiveShares);
    }
    Some(S8aPreparedVenueOrder {
        condition_id: request.condition_id.clone(),
        token_id: request.token_id.clone(),
        side: request.side,
        action: request.action,
        order_type: S8aVenueOrderType::Gtc,
        limit_price: Some(price),
        amount: S8aVenueAmount::Shares(shares),
        execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    })
}

fn market_buy_order(
    request: &S8aNativeOrderAdapterRequest,
    block_reasons: &mut Vec<S8aNativeOrderAdapterBlockReason>,
) -> Option<S8aPreparedVenueOrder> {
    let notional = match request.market_buy_notional_usdc {
        Some(value) if value.is_finite() => value,
        _ => {
            block_reasons.push(S8aNativeOrderAdapterBlockReason::MarketBuyAmountUnitMissing);
            return None;
        }
    };
    if notional + 1e-9 < S8A_MARKET_BUY_MIN_NOTIONAL_USDC {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::MarketBuyNotionalBelowOneUsdc);
    }
    Some(S8aPreparedVenueOrder {
        condition_id: request.condition_id.clone(),
        token_id: request.token_id.clone(),
        side: request.side,
        action: request.action,
        order_type: S8aVenueOrderType::Fak,
        limit_price: request.limit_price,
        amount: S8aVenueAmount::UsdcNotional(notional),
        execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    })
}

fn market_sell_order(
    request: &S8aNativeOrderAdapterRequest,
    block_reasons: &mut Vec<S8aNativeOrderAdapterBlockReason>,
) -> Option<S8aPreparedVenueOrder> {
    let shares = match request.market_sell_size_shares {
        Some(value) if value.is_finite() => value,
        _ => {
            block_reasons.push(S8aNativeOrderAdapterBlockReason::MarketSellAmountUnitMissing);
            return None;
        }
    };
    if shares + 1e-9 < S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES {
        block_reasons.push(S8aNativeOrderAdapterBlockReason::MarketSellSharesBelowFive);
    }
    Some(S8aPreparedVenueOrder {
        condition_id: request.condition_id.clone(),
        token_id: request.token_id.clone(),
        side: request.side,
        action: request.action,
        order_type: S8aVenueOrderType::Fak,
        limit_price: request.limit_price,
        amount: S8aVenueAmount::Shares(shares),
        execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct S8aInventory {
    pub yes_qty: f64,
    pub no_qty: f64,
    pub gross_quote_spend_usdc: f64,
}

impl S8aInventory {
    pub fn paired_qty(&self) -> f64 {
        self.yes_qty.min(self.no_qty)
    }

    pub fn residual_qty(&self) -> f64 {
        (self.yes_qty - self.no_qty).abs()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct S8aFillEvent {
    pub side: Side,
    pub action: S8aAdapterOrderAction,
    pub submitted_size_shares: f64,
    pub actual_filled_qty_shares: f64,
    pub avg_fill_price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aInventoryUpdateBlockReason {
    InvalidFilledQty,
    InvalidFillPrice,
    SellWouldExceedInventory,
}

impl S8aInventoryUpdateBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidFilledQty => "BLOCK_INVALID_FILLED_QTY",
            Self::InvalidFillPrice => "BLOCK_INVALID_FILL_PRICE",
            Self::SellWouldExceedInventory => "BLOCK_SELL_WOULD_EXCEED_INVENTORY",
        }
    }
}

pub fn apply_s8a_fill_to_inventory(
    inventory: S8aInventory,
    fill: S8aFillEvent,
) -> Result<S8aInventory, S8aInventoryUpdateBlockReason> {
    if !fill.actual_filled_qty_shares.is_finite() || fill.actual_filled_qty_shares < 0.0 {
        return Err(S8aInventoryUpdateBlockReason::InvalidFilledQty);
    }
    if !fill.avg_fill_price.is_finite() || fill.avg_fill_price < 0.0 || fill.avg_fill_price > 1.0 {
        return Err(S8aInventoryUpdateBlockReason::InvalidFillPrice);
    }

    let mut next = inventory;
    match (fill.action, fill.side) {
        (S8aAdapterOrderAction::Buy, Side::Yes) => next.yes_qty += fill.actual_filled_qty_shares,
        (S8aAdapterOrderAction::Buy, Side::No) => next.no_qty += fill.actual_filled_qty_shares,
        (S8aAdapterOrderAction::Sell, Side::Yes) => {
            if fill.actual_filled_qty_shares > next.yes_qty + 1e-9 {
                return Err(S8aInventoryUpdateBlockReason::SellWouldExceedInventory);
            }
            next.yes_qty -= fill.actual_filled_qty_shares;
        }
        (S8aAdapterOrderAction::Sell, Side::No) => {
            if fill.actual_filled_qty_shares > next.no_qty + 1e-9 {
                return Err(S8aInventoryUpdateBlockReason::SellWouldExceedInventory);
            }
            next.no_qty -= fill.actual_filled_qty_shares;
        }
    }
    if fill.action == S8aAdapterOrderAction::Buy {
        next.gross_quote_spend_usdc += fill.actual_filled_qty_shares * fill.avg_fill_price;
    }
    Ok(next)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct S8aSessionState {
    pub completed_rounds: u32,
    pub active_market_open: bool,
    pub realized_session_loss_usdc: f64,
    pub active_round_risk_at_work_usdc: f64,
    pub cumulative_gross_quote_spend_usdc: f64,
    pub total_order_submissions: u32,
    pub cancel_count: u32,
    pub recovery_tx_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aSessionGateBlockReason {
    MaxRoundsReached,
    ActiveMarketAlreadyOpen,
    RealizedLossCapWouldBeExceeded,
    ActiveRoundRiskAboveCap,
    GrossQuoteSpendCapExceeded,
    TotalOrderCapExceeded,
    CancelCapExceeded,
    RecoveryTxCapExceeded,
}

impl S8aSessionGateBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MaxRoundsReached => "BLOCK_MAX_ROUNDS_REACHED",
            Self::ActiveMarketAlreadyOpen => "BLOCK_ACTIVE_MARKET_ALREADY_OPEN",
            Self::RealizedLossCapWouldBeExceeded => "BLOCK_REALIZED_LOSS_CAP_WOULD_BE_EXCEEDED",
            Self::ActiveRoundRiskAboveCap => "BLOCK_ACTIVE_ROUND_RISK_ABOVE_CAP",
            Self::GrossQuoteSpendCapExceeded => "BLOCK_GROSS_QUOTE_SPEND_CAP_EXCEEDED",
            Self::TotalOrderCapExceeded => "BLOCK_TOTAL_ORDER_CAP_EXCEEDED",
            Self::CancelCapExceeded => "BLOCK_CANCEL_CAP_EXCEEDED",
            Self::RecoveryTxCapExceeded => "BLOCK_RECOVERY_TX_CAP_EXCEEDED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aSessionGateReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aSessionGateBlockReason>,
    pub can_open_new_round: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_new_round_gate(state: &S8aSessionState) -> S8aSessionGateReview {
    let mut block_reasons = Vec::new();
    if state.completed_rounds >= S8A_MAX_ROUNDS {
        block_reasons.push(S8aSessionGateBlockReason::MaxRoundsReached);
    }
    if state.active_market_open {
        block_reasons.push(S8aSessionGateBlockReason::ActiveMarketAlreadyOpen);
    }
    if state.realized_session_loss_usdc + S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC
        > S8A_SESSION_HARD_LOSS_CAP_USDC + 1e-9
    {
        block_reasons.push(S8aSessionGateBlockReason::RealizedLossCapWouldBeExceeded);
    }
    if state.active_round_risk_at_work_usdc > S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC + 1e-9 {
        block_reasons.push(S8aSessionGateBlockReason::ActiveRoundRiskAboveCap);
    }
    if state.cumulative_gross_quote_spend_usdc > 18.0 + 1e-9 {
        block_reasons.push(S8aSessionGateBlockReason::GrossQuoteSpendCapExceeded);
    }
    if state.total_order_submissions >= 9 {
        block_reasons.push(S8aSessionGateBlockReason::TotalOrderCapExceeded);
    }
    if state.cancel_count > 6 {
        block_reasons.push(S8aSessionGateBlockReason::CancelCapExceeded);
    }
    if state.recovery_tx_count > 3 {
        block_reasons.push(S8aSessionGateBlockReason::RecoveryTxCapExceeded);
    }

    S8aSessionGateReview {
        event: S8A_SESSION_GATE_REVIEW_EVENT,
        can_open_new_round: block_reasons.is_empty(),
        block_reasons,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::btc_completion_controller::{
        review_btc_completion_candidate, BtcCompletionCandidate, BtcCompletionConditionState,
        BtcCompletionControllerConfig, BtcCompletionControllerReview, BtcCompletionControllerState,
        BtcCompletionResearchIntent,
    };

    fn base_request() -> S8aNativeOrderAdapterRequest {
        S8aNativeOrderAdapterRequest {
            condition_id: "0xcondition".to_string(),
            token_id: "token-yes".to_string(),
            side: Side::Yes,
            action: S8aAdapterOrderAction::Buy,
            order_kind: S8aAdapterOrderKind::Limit,
            limit_price: Some(0.435),
            limit_size_shares: Some(5.0),
            market_buy_notional_usdc: None,
            market_sell_size_shares: None,
            controller_admission_passed: true,
            controller_review_execution_permitted: false,
            source_guard_500_passed: true,
            no_forced_complement: true,
            official_order_minimums_verified: true,
            fresh_exact_approval_hash_present: true,
            approval_scope_matches_s8a: true,
            effectful_submission_requested: true,
            prints_secret_or_raw_signature: false,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
        }
    }

    fn adapter_context() -> S8aControllerAdmissionAdapterContext {
        S8aControllerAdmissionAdapterContext {
            yes_token_id: "token-yes".to_string(),
            no_token_id: "token-no".to_string(),
            official_order_minimums_verified: true,
            fresh_exact_approval_hash_present: true,
            approval_scope_matches_s8a: true,
            effectful_submission_requested: true,
            prints_secret_or_raw_signature: false,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
        }
    }

    fn btc_candidate(condition_id: &str, side: Side, ts_ms: u64) -> BtcCompletionCandidate {
        BtcCompletionCandidate {
            asset: "BTC".to_string(),
            event_kind: "public_trade".to_string(),
            public_trade_taker_side: "BUY".to_string(),
            condition_id: condition_id.to_string(),
            slug: "btc-updown-5m-s8a-fixture".to_string(),
            ts_ms,
            offset_s: 10.0,
            time_to_end_s: Some(250.0),
            side,
            public_trade_price: 0.49,
            l1_pair_ask: 1.01,
            l1_pair_available_qty: 25.0,
            buy_available_qty: 25.0,
            strict_l1_age_ms: Some(10),
            strict_l2_age_ms: None,
            public_trade_recv_lag_ms: Some(10),
            forbidden_decision_fields_present: false,
            snapshot_index_used_as_rank: false,
        }
    }

    #[test]
    fn limit_entry_uses_five_share_gtc_and_never_permits_execution_in_adapter() {
        let review = review_s8a_native_order_adapter(&base_request());

        assert!(review.native_adapter_ready_for_exact_runtime);
        assert!(review.official_order_units_bound);
        assert!(!review.effectful_execution_permitted);
        let order = review.prepared_order.expect("valid limit order");
        assert_eq!(order.order_type, S8aVenueOrderType::Gtc);
        assert_eq!(order.amount, S8aVenueAmount::Shares(5.0));
        assert!(!order.execution_permitted);
    }

    #[test]
    fn limit_entry_rejects_sub_min_or_non_policy_size() {
        let mut request = base_request();
        request.limit_size_shares = Some(4.99);

        let review = review_s8a_native_order_adapter(&request);

        assert!(!review.native_adapter_ready_for_exact_runtime);
        assert!(review
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::LimitOrderSizeNotExactlyFiveShares));
        assert!(review.prepared_order.is_none());
    }

    #[test]
    fn market_buy_uses_usdc_notional_and_blocks_below_one_dollar() {
        let mut request = base_request();
        request.order_kind = S8aAdapterOrderKind::Market;
        request.market_buy_notional_usdc = Some(1.25);
        request.limit_size_shares = None;

        let review = review_s8a_native_order_adapter(&request);

        assert!(review.native_adapter_ready_for_exact_runtime);
        assert_eq!(
            review.prepared_order.as_ref().map(|order| order.amount),
            Some(S8aVenueAmount::UsdcNotional(1.25))
        );

        request.market_buy_notional_usdc = Some(0.99);
        let blocked = review_s8a_native_order_adapter(&request);
        assert!(blocked
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::MarketBuyNotionalBelowOneUsdc));
        assert!(blocked.prepared_order.is_none());
    }

    #[test]
    fn market_sell_uses_shares_and_blocks_sub_min_residual() {
        let mut request = base_request();
        request.action = S8aAdapterOrderAction::Sell;
        request.order_kind = S8aAdapterOrderKind::Market;
        request.market_sell_size_shares = Some(5.0);
        request.limit_price = Some(0.01);
        request.limit_size_shares = None;

        let review = review_s8a_native_order_adapter(&request);

        assert!(review.native_adapter_ready_for_exact_runtime);
        assert_eq!(
            review.prepared_order.as_ref().map(|order| order.amount),
            Some(S8aVenueAmount::Shares(5.0))
        );

        request.market_sell_size_shares = Some(4.8);
        let blocked = review_s8a_native_order_adapter(&request);
        assert!(blocked
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::MarketSellSharesBelowFive));
        assert!(blocked.prepared_order.is_none());
    }

    #[test]
    fn adapter_requires_natural_controller_admission_not_forced_complement() {
        let mut request = base_request();
        request.controller_admission_passed = false;
        request.no_forced_complement = false;

        let review = review_s8a_native_order_adapter(&request);

        assert!(review
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::ControllerAdmissionMissing));
        assert!(review
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::ForcedComplementRequested));
        assert!(review.prepared_order.is_none());
    }

    #[test]
    fn controller_admission_maps_to_native_five_share_limit_order() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let controller_review = review_btc_completion_candidate(
            &cfg,
            &state,
            &btc_candidate("0xcondition", Side::Yes, 1_000),
        );

        let adapter_review = review_s8a_limit_entry_from_controller_admission(
            &controller_review,
            &adapter_context(),
        );

        assert!(adapter_review.native_adapter_ready_for_exact_runtime);
        assert!(adapter_review.official_order_units_bound);
        let order = adapter_review.prepared_order.expect("prepared S8A order");
        assert_eq!(order.token_id, "token-yes");
        assert_eq!(order.amount, S8aVenueAmount::Shares(5.0));
        assert_eq!(order.limit_price, Some(0.435));
        assert!(!order.execution_permitted);
    }

    #[test]
    fn blocked_controller_admission_never_maps_to_order() {
        let controller_review = BtcCompletionControllerReview {
            intent: None,
            block_reason: None,
            execution_permitted: false,
        };

        let adapter_review = review_s8a_limit_entry_from_controller_admission(
            &controller_review,
            &adapter_context(),
        );

        assert!(!adapter_review.native_adapter_ready_for_exact_runtime);
        assert!(adapter_review
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::ControllerAdmissionMissing));
        assert!(adapter_review.prepared_order.is_none());
    }

    #[test]
    fn controller_review_with_execution_permission_is_rejected() {
        let controller_review = BtcCompletionControllerReview {
            intent: Some(BtcCompletionResearchIntent {
                condition_id: "0xcondition".to_string(),
                slug: "btc-updown-5m-s8a-fixture".to_string(),
                ts_ms: 1_000,
                side: Side::No,
                price: 0.445,
                qty: 5.0,
                execution_permitted: false,
            }),
            block_reason: None,
            execution_permitted: true,
        };

        let adapter_review = review_s8a_limit_entry_from_controller_admission(
            &controller_review,
            &adapter_context(),
        );

        assert!(!adapter_review.native_adapter_ready_for_exact_runtime);
        assert!(adapter_review
            .block_reasons
            .contains(&S8aNativeOrderAdapterBlockReason::ControllerReviewPermitsExecution));
        assert!(adapter_review.prepared_order.is_none());
    }

    #[test]
    fn s8a_controller_uses_actual_partial_fill_inventory_for_next_admission() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let mut state = BtcCompletionControllerState::default();
        state.set_condition_state(
            "0xcondition",
            BtcCompletionConditionState {
                yes_qty: 3.0,
                no_qty: 0.0,
                last_accept_ts_ms: Some(1_000),
            },
        );

        let same_side = review_btc_completion_candidate(
            &cfg,
            &state,
            &btc_candidate("0xcondition", Side::Yes, 7_000),
        );
        let complement = review_btc_completion_candidate(
            &cfg,
            &state,
            &btc_candidate("0xcondition", Side::No, 7_000),
        );

        assert!(same_side.intent.is_none());
        assert_eq!(
            same_side.block_reason.map(|reason| reason.as_str()),
            Some("BLOCK_INVENTORY_IMBALANCE_WOULD_EXCEED_CAP")
        );
        assert!(complement.intent.is_some());
    }

    #[test]
    fn actual_filled_qty_updates_inventory_not_submitted_size() {
        let inventory = S8aInventory::default();
        let fill = S8aFillEvent {
            side: Side::Yes,
            action: S8aAdapterOrderAction::Buy,
            submitted_size_shares: 5.0,
            actual_filled_qty_shares: 3.0,
            avg_fill_price: 0.40,
        };

        let next = apply_s8a_fill_to_inventory(inventory, fill).expect("valid partial fill");

        assert_eq!(next.yes_qty, 3.0);
        assert_eq!(next.no_qty, 0.0);
        assert_eq!(next.paired_qty(), 0.0);
        assert_eq!(next.residual_qty(), 3.0);
        assert!((next.gross_quote_spend_usdc - 1.20).abs() < 1e-9);
    }

    #[test]
    fn new_round_gate_enforces_three_round_fifteen_usdc_stop_model() {
        let pass = S8aSessionState {
            completed_rounds: 2,
            active_market_open: false,
            realized_session_loss_usdc: 10.0,
            active_round_risk_at_work_usdc: 0.0,
            cumulative_gross_quote_spend_usdc: 12.0,
            total_order_submissions: 4,
            cancel_count: 2,
            recovery_tx_count: 1,
        };
        assert!(review_s8a_new_round_gate(&pass).can_open_new_round);

        let blocked = S8aSessionState {
            realized_session_loss_usdc: 10.01,
            active_market_open: true,
            ..pass
        };
        let review = review_s8a_new_round_gate(&blocked);
        assert!(!review.can_open_new_round);
        assert!(review
            .block_reasons
            .contains(&S8aSessionGateBlockReason::ActiveMarketAlreadyOpen));
        assert!(review
            .block_reasons
            .contains(&S8aSessionGateBlockReason::RealizedLossCapWouldBeExceeded));
        assert!(!review.effectful_execution_permitted);
    }
}
