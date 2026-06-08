//! Native S8A order adapter contract.
//!
//! This module is intentionally pure and disabled by default. It does not sign,
//! submit, cancel, merge, redeem, fund, read secrets, or perform network IO. It
//! encodes the S8A-specific venue-order unit contract so the future effectful
//! wrapper does not reuse legacy executor market-buy share semantics.

use crate::polymarket::btc_completion_controller::{
    review_btc_completion_candidate, BtcCompletionCandidate, BtcCompletionControllerConfig,
    BtcCompletionControllerReview, BtcCompletionControllerState,
};
use crate::polymarket::types::Side;

pub const S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT: bool = false;
pub const S8A_NATIVE_ORDER_ADAPTER_REVIEW_EVENT: &str = "s8a_native_order_adapter_review";
pub const S8A_SCOUT_ADMISSION_REVIEW_EVENT: &str = "s8a_scout_admission_review";
pub const S8A_SESSION_GATE_REVIEW_EVENT: &str = "s8a_session_gate_review";
pub const S8A_RUNTIME_LOOP_BINDING_REVIEW_EVENT: &str = "s8a_runtime_loop_binding_review";
pub const S8A_RUNTIME_ORCHESTRATION_PREVIEW_EVENT: &str = "s8a_runtime_orchestration_preview";
pub const S8A_RUNTIME_WRAPPER_PREFLIGHT_REVIEW_EVENT: &str = "s8a_runtime_wrapper_preflight_review";
pub const S8A_ONE_RUN_ORCHESTRATION_REVIEW_EVENT: &str = "s8a_one_run_orchestration_review";
pub const S8A_REMOTE_RUNTIME_PROVISIONING_REVIEW_EVENT: &str =
    "s8a_remote_runtime_provisioning_review";
pub const S8A_ORDER_STATUS_FILL_EVIDENCE_REVIEW_EVENT: &str =
    "s8a_order_status_fill_evidence_review";
pub const S8A_REVIEWED_HOST: &str = "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com";
pub const S8A_OFFICIAL_CLOB_REST_URL: &str = "https://clob.polymarket.com";
pub const S8A_NATIVE_RUNTIME_SCOPE: &str =
    "B_STRATEGY_CANARY_S8A_MICRO_SHORT_CYCLE_ONE_RUN_MAX_THREE_ROUNDS_BTC5M_SIZE5_15USDC_LOSS_CAP_NATIVE_RUNTIME_S8X";
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

#[derive(Debug, Clone, PartialEq)]
pub struct S8aScoutBookSideSnapshot {
    pub token_id: String,
    pub best_ask: Option<f64>,
    pub top5_ask_qty: f64,
    pub book_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aScoutPublicBuyTrade {
    pub side: Side,
    pub taker_side: String,
    pub price: f64,
    pub size: f64,
    pub event_ts_ms: u64,
    pub observed_ts_ms: u64,
    pub recv_lag_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aScoutAdmissionSnapshot {
    pub asset: String,
    pub event_reason: String,
    pub slug: String,
    pub condition_id: String,
    pub yes: S8aScoutBookSideSnapshot,
    pub no: S8aScoutBookSideSnapshot,
    pub latest_public_buy_trade: Option<S8aScoutPublicBuyTrade>,
    pub offset_s: f64,
    pub time_to_end_s: Option<f64>,
    pub forbidden_decision_fields_present: bool,
    pub snapshot_index_used_as_rank: bool,
    pub source_is_b_owned_direct_public_ws: bool,
    pub shared_ingress_dependency: bool,
    pub b_owned_direct_public_ws_connection_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aScoutAdmissionBlockReason {
    NonBtcAsset,
    MissingConditionId,
    MissingTokenMapping,
    MissingPublicBuyTrade,
    NonBuyPublicTrade,
    InvalidPublicTradePrice,
    InvalidPublicTradeSize,
    InvalidOffset,
    InvalidBookAsk,
    InvalidBookAge,
    TokenMappingMismatch,
    NonBOwnedDirectPublicWsSource,
    SharedIngressDependency,
    TooManyBOwnedDirectPublicWs,
}

impl S8aScoutAdmissionBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonBtcAsset => "BLOCK_NON_BTC_ASSET",
            Self::MissingConditionId => "BLOCK_MISSING_CONDITION_ID",
            Self::MissingTokenMapping => "BLOCK_MISSING_TOKEN_MAPPING",
            Self::MissingPublicBuyTrade => "BLOCK_MISSING_PUBLIC_BUY_TRADE",
            Self::NonBuyPublicTrade => "BLOCK_NON_BUY_PUBLIC_TRADE",
            Self::InvalidPublicTradePrice => "BLOCK_INVALID_PUBLIC_TRADE_PRICE",
            Self::InvalidPublicTradeSize => "BLOCK_INVALID_PUBLIC_TRADE_SIZE",
            Self::InvalidOffset => "BLOCK_INVALID_OFFSET",
            Self::InvalidBookAsk => "BLOCK_INVALID_BOOK_ASK",
            Self::InvalidBookAge => "BLOCK_INVALID_BOOK_AGE",
            Self::TokenMappingMismatch => "BLOCK_TOKEN_MAPPING_MISMATCH",
            Self::NonBOwnedDirectPublicWsSource => "BLOCK_NON_B_OWNED_DIRECT_PUBLIC_WS_SOURCE",
            Self::SharedIngressDependency => "BLOCK_SHARED_INGRESS_DEPENDENCY",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aScoutAdmissionReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aScoutAdmissionBlockReason>,
    pub candidate: Option<BtcCompletionCandidate>,
    pub controller_review: Option<BtcCompletionControllerReview>,
    pub adapter_review: Option<S8aNativeOrderAdapterReview>,
    pub prepared_order: Option<S8aPreparedVenueOrder>,
    pub scout_source_bound: bool,
    pub controller_and_adapter_bound: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_scout_snapshot_for_limit_entry(
    cfg: &BtcCompletionControllerConfig,
    state: &BtcCompletionControllerState,
    snapshot: &S8aScoutAdmissionSnapshot,
    context: &S8aControllerAdmissionAdapterContext,
) -> S8aScoutAdmissionReview {
    let mut block_reasons = Vec::new();

    if snapshot.asset.trim() != "BTC" {
        block_reasons.push(S8aScoutAdmissionBlockReason::NonBtcAsset);
    }
    if snapshot.condition_id.trim().is_empty() {
        block_reasons.push(S8aScoutAdmissionBlockReason::MissingConditionId);
    }
    if snapshot.yes.token_id.trim().is_empty() || snapshot.no.token_id.trim().is_empty() {
        block_reasons.push(S8aScoutAdmissionBlockReason::MissingTokenMapping);
    }
    if snapshot.yes.token_id != context.yes_token_id || snapshot.no.token_id != context.no_token_id
    {
        block_reasons.push(S8aScoutAdmissionBlockReason::TokenMappingMismatch);
    }
    if !snapshot.source_is_b_owned_direct_public_ws {
        block_reasons.push(S8aScoutAdmissionBlockReason::NonBOwnedDirectPublicWsSource);
    }
    if snapshot.shared_ingress_dependency {
        block_reasons.push(S8aScoutAdmissionBlockReason::SharedIngressDependency);
    }
    if snapshot.b_owned_direct_public_ws_connection_count > 1 {
        block_reasons.push(S8aScoutAdmissionBlockReason::TooManyBOwnedDirectPublicWs);
    }
    if !snapshot.offset_s.is_finite() || snapshot.offset_s < 0.0 {
        block_reasons.push(S8aScoutAdmissionBlockReason::InvalidOffset);
    }

    let yes_ask = snapshot.yes.best_ask.unwrap_or(f64::NAN);
    let no_ask = snapshot.no.best_ask.unwrap_or(f64::NAN);
    if !yes_ask.is_finite() || yes_ask <= 0.0 || !no_ask.is_finite() || no_ask <= 0.0 {
        block_reasons.push(S8aScoutAdmissionBlockReason::InvalidBookAsk);
    }
    let strict_l1_age_ms = match (snapshot.yes.book_age_ms, snapshot.no.book_age_ms) {
        (Some(yes_age), Some(no_age)) => Some(yes_age.max(no_age)),
        _ => {
            block_reasons.push(S8aScoutAdmissionBlockReason::InvalidBookAge);
            None
        }
    };

    let Some(trade) = snapshot.latest_public_buy_trade.as_ref() else {
        block_reasons.push(S8aScoutAdmissionBlockReason::MissingPublicBuyTrade);
        return S8aScoutAdmissionReview {
            event: S8A_SCOUT_ADMISSION_REVIEW_EVENT,
            scout_source_bound: false,
            controller_and_adapter_bound: false,
            block_reasons,
            candidate: None,
            controller_review: None,
            adapter_review: None,
            prepared_order: None,
            effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
        };
    };

    if trade.taker_side.trim() != "BUY" {
        block_reasons.push(S8aScoutAdmissionBlockReason::NonBuyPublicTrade);
    }
    if !trade.price.is_finite() || trade.price <= 0.0 || trade.price >= 1.0 {
        block_reasons.push(S8aScoutAdmissionBlockReason::InvalidPublicTradePrice);
    }
    if !trade.size.is_finite() || trade.size <= 0.0 {
        block_reasons.push(S8aScoutAdmissionBlockReason::InvalidPublicTradeSize);
    }

    let recv_lag_ms = trade
        .recv_lag_ms
        .or_else(|| trade.observed_ts_ms.checked_sub(trade.event_ts_ms));

    let candidate = if block_reasons.is_empty() {
        Some(BtcCompletionCandidate {
            asset: snapshot.asset.clone(),
            event_kind: "public_trade".to_string(),
            public_trade_taker_side: trade.taker_side.clone(),
            condition_id: snapshot.condition_id.clone(),
            slug: snapshot.slug.clone(),
            ts_ms: trade.observed_ts_ms,
            offset_s: snapshot.offset_s,
            time_to_end_s: snapshot.time_to_end_s,
            side: trade.side,
            public_trade_price: trade.price,
            l1_pair_ask: yes_ask + no_ask,
            l1_pair_available_qty: snapshot.yes.top5_ask_qty.min(snapshot.no.top5_ask_qty),
            buy_available_qty: match trade.side {
                Side::Yes => snapshot.yes.top5_ask_qty,
                Side::No => snapshot.no.top5_ask_qty,
            },
            strict_l1_age_ms,
            strict_l2_age_ms: None,
            public_trade_recv_lag_ms: recv_lag_ms,
            forbidden_decision_fields_present: snapshot.forbidden_decision_fields_present,
            snapshot_index_used_as_rank: snapshot.snapshot_index_used_as_rank,
        })
    } else {
        None
    };

    let controller_review = candidate
        .as_ref()
        .map(|candidate| review_btc_completion_candidate(cfg, state, candidate));
    let adapter_review = controller_review
        .as_ref()
        .map(|review| review_s8a_limit_entry_from_controller_admission(review, context));
    let prepared_order = adapter_review
        .as_ref()
        .and_then(|review| review.prepared_order.clone());
    let controller_and_adapter_bound = controller_review
        .as_ref()
        .is_some_and(|review| review.intent.is_some())
        && adapter_review
            .as_ref()
            .is_some_and(|review| review.native_adapter_ready_for_exact_runtime);

    S8aScoutAdmissionReview {
        event: S8A_SCOUT_ADMISSION_REVIEW_EVENT,
        scout_source_bound: block_reasons.is_empty(),
        controller_and_adapter_bound,
        block_reasons,
        candidate,
        controller_review,
        adapter_review,
        prepared_order,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aOrderStatusEvidenceSource {
    PostOrderResponse,
    OrderStatusApi,
    TradeFillApi,
}

impl S8aOrderStatusEvidenceSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PostOrderResponse => "POST_ORDER_RESPONSE",
            Self::OrderStatusApi => "ORDER_STATUS_API",
            Self::TradeFillApi => "TRADE_FILL_API",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aObservedOrderStatus {
    Live,
    Matched,
    Filled,
    Cancelled,
    Rejected,
    Failed,
}

impl S8aObservedOrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "LIVE",
            Self::Matched => "MATCHED",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aOrderStatusFillEvidence {
    pub prepared_order_sha256: String,
    pub exact_approval_sha256: String,
    pub expected_exact_approval_sha256: String,
    pub exact_approval_hash_bound_to_order: bool,
    pub prepared_order_hash_bound_to_order: bool,
    pub approval_scope_matches_s8a: bool,
    pub order_id: Option<String>,
    pub failure_recorded: bool,
    pub status_source: S8aOrderStatusEvidenceSource,
    pub order_status: S8aObservedOrderStatus,
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub action: S8aAdapterOrderAction,
    pub condition_token_side_action_match_prepared_order: bool,
    pub submitted_size_shares: f64,
    pub actual_filled_qty_shares: f64,
    pub avg_fill_price: Option<f64>,
    pub open_order_remainder_qty_shares: f64,
    pub filled_qty_from_exchange_or_order_status: bool,
    pub submitted_size_counts_as_inventory: bool,
    pub partial_fill_threshold_used: bool,
    pub forced_complement_after_fill: bool,
    pub no_open_order_remainder_required: bool,
    pub prints_secret_or_raw_signature: bool,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
    pub funding_live_latest_or_deploy_requested: bool,
    pub effectful_execution_requested_in_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aOrderStatusFillEvidenceBlockReason {
    InvalidPreparedOrderHash,
    InvalidExactApprovalHash,
    ExactApprovalHashMismatch,
    ExactApprovalHashNotBoundToOrder,
    PreparedOrderHashNotBoundToOrder,
    ApprovalScopeMismatch,
    MissingOrderIdOrFailureRecord,
    PreparedOrderBindingMismatch,
    ConditionMismatch,
    TokenMismatch,
    SideMismatch,
    ActionMismatch,
    SubmittedSizeMismatch,
    InvalidSubmittedSize,
    InvalidActualFilledQty,
    FilledQtyExceedsSubmittedSize,
    MissingAverageFillPriceForPositiveFill,
    InvalidAverageFillPrice,
    InvalidOpenOrderRemainderQty,
    OpenOrderRemainderPresent,
    FilledQtyNotExchangeOrOrderStatusObserved,
    SubmittedSizeCountsAsInventory,
    PartialFillThresholdUsed,
    ForcedComplementAfterFill,
    SecretOrRawSignatureOutputAllowed,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionRequestedInReview,
}

impl S8aOrderStatusFillEvidenceBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidPreparedOrderHash => "BLOCK_INVALID_PREPARED_ORDER_HASH",
            Self::InvalidExactApprovalHash => "BLOCK_INVALID_EXACT_APPROVAL_HASH",
            Self::ExactApprovalHashMismatch => "BLOCK_EXACT_APPROVAL_HASH_MISMATCH",
            Self::ExactApprovalHashNotBoundToOrder => {
                "BLOCK_EXACT_APPROVAL_HASH_NOT_BOUND_TO_ORDER"
            }
            Self::PreparedOrderHashNotBoundToOrder => {
                "BLOCK_PREPARED_ORDER_HASH_NOT_BOUND_TO_ORDER"
            }
            Self::ApprovalScopeMismatch => "BLOCK_APPROVAL_SCOPE_MISMATCH",
            Self::MissingOrderIdOrFailureRecord => "BLOCK_MISSING_ORDER_ID_OR_FAILURE_RECORD",
            Self::PreparedOrderBindingMismatch => "BLOCK_PREPARED_ORDER_BINDING_MISMATCH",
            Self::ConditionMismatch => "BLOCK_CONDITION_MISMATCH",
            Self::TokenMismatch => "BLOCK_TOKEN_MISMATCH",
            Self::SideMismatch => "BLOCK_SIDE_MISMATCH",
            Self::ActionMismatch => "BLOCK_ACTION_MISMATCH",
            Self::SubmittedSizeMismatch => "BLOCK_SUBMITTED_SIZE_MISMATCH",
            Self::InvalidSubmittedSize => "BLOCK_INVALID_SUBMITTED_SIZE",
            Self::InvalidActualFilledQty => "BLOCK_INVALID_ACTUAL_FILLED_QTY",
            Self::FilledQtyExceedsSubmittedSize => "BLOCK_FILLED_QTY_EXCEEDS_SUBMITTED_SIZE",
            Self::MissingAverageFillPriceForPositiveFill => {
                "BLOCK_MISSING_AVERAGE_FILL_PRICE_FOR_POSITIVE_FILL"
            }
            Self::InvalidAverageFillPrice => "BLOCK_INVALID_AVERAGE_FILL_PRICE",
            Self::InvalidOpenOrderRemainderQty => "BLOCK_INVALID_OPEN_ORDER_REMAINDER_QTY",
            Self::OpenOrderRemainderPresent => "BLOCK_OPEN_ORDER_REMAINDER_PRESENT",
            Self::FilledQtyNotExchangeOrOrderStatusObserved => {
                "BLOCK_FILLED_QTY_NOT_EXCHANGE_OR_ORDER_STATUS_OBSERVED"
            }
            Self::SubmittedSizeCountsAsInventory => "BLOCK_SUBMITTED_SIZE_COUNTS_AS_INVENTORY",
            Self::PartialFillThresholdUsed => "BLOCK_PARTIAL_FILL_THRESHOLD_USED",
            Self::ForcedComplementAfterFill => "BLOCK_FORCED_COMPLEMENT_AFTER_FILL",
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionRequestedInReview => {
                "BLOCK_EFFECTFUL_EXECUTION_REQUESTED_IN_REVIEW"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aOrderStatusFillEvidenceReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aOrderStatusFillEvidenceBlockReason>,
    pub order_id_or_failure_recorded: bool,
    pub exact_approval_bound: bool,
    pub prepared_order_bound: bool,
    pub filled_qty_from_exchange_or_order_status_only: bool,
    pub no_open_order_remainder: bool,
    pub actual_filled_qty_ledger_ready: bool,
    pub fill_event: Option<S8aFillEvent>,
    pub effectful_execution_permitted: bool,
}

fn prepared_order_submitted_size_shares(order: &S8aPreparedVenueOrder) -> Option<f64> {
    match order.amount {
        S8aVenueAmount::Shares(value) => Some(value),
        S8aVenueAmount::UsdcNotional(_) => None,
    }
}

pub fn review_s8a_order_status_fill_evidence(
    prepared_order: &S8aPreparedVenueOrder,
    evidence: &S8aOrderStatusFillEvidence,
) -> S8aOrderStatusFillEvidenceReview {
    let mut block_reasons = Vec::new();

    if !is_sha256_hex(&evidence.prepared_order_sha256) {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidPreparedOrderHash);
    }
    if !is_sha256_hex(&evidence.exact_approval_sha256)
        || !is_sha256_hex(&evidence.expected_exact_approval_sha256)
    {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidExactApprovalHash);
    }
    if evidence.exact_approval_sha256 != evidence.expected_exact_approval_sha256 {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ExactApprovalHashMismatch);
    }
    if !evidence.exact_approval_hash_bound_to_order {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ExactApprovalHashNotBoundToOrder);
    }
    if !evidence.prepared_order_hash_bound_to_order {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::PreparedOrderHashNotBoundToOrder);
    }
    if !evidence.approval_scope_matches_s8a {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ApprovalScopeMismatch);
    }

    let order_id_or_failure_recorded = evidence
        .order_id
        .as_ref()
        .is_some_and(|id| !id.trim().is_empty())
        || evidence.failure_recorded;
    if !order_id_or_failure_recorded {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::MissingOrderIdOrFailureRecord);
    }

    if !evidence.condition_token_side_action_match_prepared_order {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::PreparedOrderBindingMismatch);
    }
    if evidence.condition_id != prepared_order.condition_id {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ConditionMismatch);
    }
    if evidence.token_id != prepared_order.token_id {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::TokenMismatch);
    }
    if evidence.side != prepared_order.side {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::SideMismatch);
    }
    if evidence.action != prepared_order.action {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ActionMismatch);
    }

    let expected_submitted_size = prepared_order_submitted_size_shares(prepared_order);
    if !evidence.submitted_size_shares.is_finite() || evidence.submitted_size_shares < 0.0 {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidSubmittedSize);
    }
    if expected_submitted_size
        .is_some_and(|expected| (expected - evidence.submitted_size_shares).abs() > 1e-9)
    {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::SubmittedSizeMismatch);
    }

    if !evidence.actual_filled_qty_shares.is_finite() || evidence.actual_filled_qty_shares < 0.0 {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidActualFilledQty);
    }
    if evidence.actual_filled_qty_shares > evidence.submitted_size_shares + 1e-9 {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::FilledQtyExceedsSubmittedSize);
    }

    let avg_fill_price = match evidence.avg_fill_price {
        Some(price) if price.is_finite() && (0.0..=1.0).contains(&price) => price,
        Some(_) => {
            block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidAverageFillPrice);
            0.0
        }
        None if evidence.actual_filled_qty_shares > 0.0 => {
            block_reasons.push(
                S8aOrderStatusFillEvidenceBlockReason::MissingAverageFillPriceForPositiveFill,
            );
            0.0
        }
        None => 0.0,
    };

    if !evidence.open_order_remainder_qty_shares.is_finite()
        || evidence.open_order_remainder_qty_shares < 0.0
    {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::InvalidOpenOrderRemainderQty);
    }
    if evidence.no_open_order_remainder_required && evidence.open_order_remainder_qty_shares > 1e-9
    {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::OpenOrderRemainderPresent);
    }
    if !evidence.filled_qty_from_exchange_or_order_status {
        block_reasons
            .push(S8aOrderStatusFillEvidenceBlockReason::FilledQtyNotExchangeOrOrderStatusObserved);
    }
    if evidence.submitted_size_counts_as_inventory {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::SubmittedSizeCountsAsInventory);
    }
    if evidence.partial_fill_threshold_used {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::PartialFillThresholdUsed);
    }
    if evidence.forced_complement_after_fill {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::ForcedComplementAfterFill);
    }
    if evidence.prints_secret_or_raw_signature {
        block_reasons
            .push(S8aOrderStatusFillEvidenceBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if evidence.uses_shared_ingress_or_shared_ws {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::SharedIngressOrSharedWsRequested);
    }
    if evidence.uses_c_artifacts {
        block_reasons.push(S8aOrderStatusFillEvidenceBlockReason::CArtifactsRequested);
    }
    if evidence.funding_live_latest_or_deploy_requested {
        block_reasons
            .push(S8aOrderStatusFillEvidenceBlockReason::FundingLiveLatestOrDeployRequested);
    }
    if evidence.effectful_execution_requested_in_review {
        block_reasons
            .push(S8aOrderStatusFillEvidenceBlockReason::EffectfulExecutionRequestedInReview);
    }

    let no_open_order_remainder = evidence.open_order_remainder_qty_shares <= 1e-9;
    let fill_event = if block_reasons.is_empty() {
        Some(S8aFillEvent {
            side: evidence.side,
            action: evidence.action,
            submitted_size_shares: evidence.submitted_size_shares,
            actual_filled_qty_shares: evidence.actual_filled_qty_shares,
            avg_fill_price,
        })
    } else {
        None
    };

    S8aOrderStatusFillEvidenceReview {
        event: S8A_ORDER_STATUS_FILL_EVIDENCE_REVIEW_EVENT,
        actual_filled_qty_ledger_ready: fill_event.is_some(),
        fill_event,
        order_id_or_failure_recorded,
        exact_approval_bound: is_sha256_hex(&evidence.exact_approval_sha256)
            && evidence.exact_approval_sha256 == evidence.expected_exact_approval_sha256
            && evidence.exact_approval_hash_bound_to_order
            && evidence.approval_scope_matches_s8a,
        prepared_order_bound: is_sha256_hex(&evidence.prepared_order_sha256)
            && evidence.prepared_order_hash_bound_to_order
            && evidence.condition_token_side_action_match_prepared_order,
        filled_qty_from_exchange_or_order_status_only: evidence
            .filled_qty_from_exchange_or_order_status,
        no_open_order_remainder,
        block_reasons,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
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

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeLoopBindingEvidence {
    pub native_effectful_runtime_source_bound: bool,
    pub runtime_requires_fresh_exact_approval: bool,
    pub runtime_preview_without_approval_exits_66: bool,
    pub no_order_auth_preview_available: bool,
    pub exact_order_path_preview_available: bool,
    pub actual_filled_qty_ledger_enabled: bool,
    pub submitted_size_counts_as_inventory: bool,
    pub partial_fill_threshold_enabled: bool,
    pub forced_complement_allowed: bool,
    pub session_caps_bound: bool,
    pub s7w_reconciliation_required: bool,
    pub s7w_exact_approved_receipt_tier_required: bool,
    pub review_only_fixture_receipt_accepted: bool,
    pub no_open_order_remainder_required: bool,
    pub residual_exposure_zero_required: bool,
    pub runtime_opens_own_ws: bool,
    pub b_owned_direct_public_ws_connection_count: u32,
    pub shared_ingress_dependency: bool,
    pub uses_c_artifacts: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub candidate_import_allowed: bool,
    pub prints_secret_or_raw_signature: bool,
    pub funding_live_latest_or_deploy_requested: bool,
    pub effectful_execution_requested_in_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aRuntimeLoopBindingBlockReason {
    SessionGateBlocked,
    ScoutAdmissionBlocked,
    ControllerAdapterNotBound,
    MissingPreparedOrder,
    NativeRuntimeSourceMissing,
    RuntimeDoesNotRequireFreshExactApproval,
    RuntimePreviewWithoutApprovalNot66,
    NoOrderAuthPreviewMissing,
    ExactOrderPathPreviewMissing,
    ActualFilledQtyLedgerMissing,
    SubmittedSizeCountsAsInventory,
    PartialFillThresholdEnabled,
    ForcedComplementAllowed,
    SessionCapsMissing,
    S7wReconciliationMissing,
    S7wExactApprovedReceiptTierNotRequired,
    ReviewOnlyFixtureReceiptAccepted,
    OpenOrderRemainderNotRequired,
    ResidualExposureZeroNotRequired,
    RuntimeOpensOwnWs,
    TooManyBOwnedDirectPublicWs,
    SharedIngressDependency,
    CArtifactsRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    CandidateImportRequested,
    SecretOrRawSignatureOutputAllowed,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionRequestedInReview,
    InventoryUpdateBlocked,
}

impl S8aRuntimeLoopBindingBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SessionGateBlocked => "BLOCK_SESSION_GATE_BLOCKED",
            Self::ScoutAdmissionBlocked => "BLOCK_SCOUT_ADMISSION_BLOCKED",
            Self::ControllerAdapterNotBound => "BLOCK_CONTROLLER_ADAPTER_NOT_BOUND",
            Self::MissingPreparedOrder => "BLOCK_MISSING_PREPARED_ORDER",
            Self::NativeRuntimeSourceMissing => "BLOCK_NATIVE_RUNTIME_SOURCE_MISSING",
            Self::RuntimeDoesNotRequireFreshExactApproval => {
                "BLOCK_RUNTIME_DOES_NOT_REQUIRE_FRESH_EXACT_APPROVAL"
            }
            Self::RuntimePreviewWithoutApprovalNot66 => {
                "BLOCK_RUNTIME_PREVIEW_WITHOUT_APPROVAL_NOT_66"
            }
            Self::NoOrderAuthPreviewMissing => "BLOCK_NO_ORDER_AUTH_PREVIEW_MISSING",
            Self::ExactOrderPathPreviewMissing => "BLOCK_EXACT_ORDER_PATH_PREVIEW_MISSING",
            Self::ActualFilledQtyLedgerMissing => "BLOCK_ACTUAL_FILLED_QTY_LEDGER_MISSING",
            Self::SubmittedSizeCountsAsInventory => "BLOCK_SUBMITTED_SIZE_COUNTS_AS_INVENTORY",
            Self::PartialFillThresholdEnabled => "BLOCK_PARTIAL_FILL_THRESHOLD_ENABLED",
            Self::ForcedComplementAllowed => "BLOCK_FORCED_COMPLEMENT_ALLOWED",
            Self::SessionCapsMissing => "BLOCK_SESSION_CAPS_MISSING",
            Self::S7wReconciliationMissing => "BLOCK_S7W_RECONCILIATION_MISSING",
            Self::S7wExactApprovedReceiptTierNotRequired => {
                "BLOCK_S7W_EXACT_APPROVED_RECEIPT_TIER_NOT_REQUIRED"
            }
            Self::ReviewOnlyFixtureReceiptAccepted => "BLOCK_REVIEW_ONLY_FIXTURE_RECEIPT_ACCEPTED",
            Self::OpenOrderRemainderNotRequired => "BLOCK_OPEN_ORDER_REMAINDER_NOT_REQUIRED",
            Self::ResidualExposureZeroNotRequired => "BLOCK_RESIDUAL_EXPOSURE_ZERO_NOT_REQUIRED",
            Self::RuntimeOpensOwnWs => "BLOCK_RUNTIME_OPENS_OWN_WS",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
            Self::SharedIngressDependency => "BLOCK_SHARED_INGRESS_DEPENDENCY",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionRequestedInReview => {
                "BLOCK_EFFECTFUL_EXECUTION_REQUESTED_IN_REVIEW"
            }
            Self::InventoryUpdateBlocked => "BLOCK_INVENTORY_UPDATE_BLOCKED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeLoopBindingReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aRuntimeLoopBindingBlockReason>,
    pub session_gate_review: S8aSessionGateReview,
    pub scout_admission_review: S8aScoutAdmissionReview,
    pub prepared_order: Option<S8aPreparedVenueOrder>,
    pub projected_inventory_after_fill: Option<S8aInventory>,
    pub inventory_update_block_reason: Option<S8aInventoryUpdateBlockReason>,
    pub loop_ready_for_fresh_exact_approval: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_runtime_loop_binding(
    cfg: &BtcCompletionControllerConfig,
    controller_state: &BtcCompletionControllerState,
    session_state: &S8aSessionState,
    inventory: S8aInventory,
    snapshot: &S8aScoutAdmissionSnapshot,
    context: &S8aControllerAdmissionAdapterContext,
    evidence: &S8aRuntimeLoopBindingEvidence,
    observed_fill: Option<S8aFillEvent>,
) -> S8aRuntimeLoopBindingReview {
    let session_gate_review = review_s8a_new_round_gate(session_state);
    let scout_admission_review =
        review_s8a_scout_snapshot_for_limit_entry(cfg, controller_state, snapshot, context);

    let mut block_reasons = Vec::new();
    if !session_gate_review.can_open_new_round {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::SessionGateBlocked);
    }
    if !scout_admission_review.scout_source_bound {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ScoutAdmissionBlocked);
    }
    if !scout_admission_review.controller_and_adapter_bound {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ControllerAdapterNotBound);
    }
    if scout_admission_review.prepared_order.is_none() {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::MissingPreparedOrder);
    }
    if !evidence.native_effectful_runtime_source_bound {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::NativeRuntimeSourceMissing);
    }
    if !evidence.runtime_requires_fresh_exact_approval {
        block_reasons
            .push(S8aRuntimeLoopBindingBlockReason::RuntimeDoesNotRequireFreshExactApproval);
    }
    if !evidence.runtime_preview_without_approval_exits_66 {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::RuntimePreviewWithoutApprovalNot66);
    }
    if !evidence.no_order_auth_preview_available {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::NoOrderAuthPreviewMissing);
    }
    if !evidence.exact_order_path_preview_available {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ExactOrderPathPreviewMissing);
    }
    if !evidence.actual_filled_qty_ledger_enabled {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ActualFilledQtyLedgerMissing);
    }
    if evidence.submitted_size_counts_as_inventory {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::SubmittedSizeCountsAsInventory);
    }
    if evidence.partial_fill_threshold_enabled {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::PartialFillThresholdEnabled);
    }
    if evidence.forced_complement_allowed {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ForcedComplementAllowed);
    }
    if !evidence.session_caps_bound {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::SessionCapsMissing);
    }
    if !evidence.s7w_reconciliation_required {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::S7wReconciliationMissing);
    }
    if !evidence.s7w_exact_approved_receipt_tier_required {
        block_reasons
            .push(S8aRuntimeLoopBindingBlockReason::S7wExactApprovedReceiptTierNotRequired);
    }
    if evidence.review_only_fixture_receipt_accepted {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ReviewOnlyFixtureReceiptAccepted);
    }
    if !evidence.no_open_order_remainder_required {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::OpenOrderRemainderNotRequired);
    }
    if !evidence.residual_exposure_zero_required {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::ResidualExposureZeroNotRequired);
    }
    if evidence.runtime_opens_own_ws {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::RuntimeOpensOwnWs);
    }
    if evidence.b_owned_direct_public_ws_connection_count > 1 {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::TooManyBOwnedDirectPublicWs);
    }
    if evidence.shared_ingress_dependency {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::SharedIngressDependency);
    }
    if evidence.uses_c_artifacts {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::CArtifactsRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.candidate_import_allowed {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::CandidateImportRequested);
    }
    if evidence.prints_secret_or_raw_signature {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if evidence.funding_live_latest_or_deploy_requested {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::FundingLiveLatestOrDeployRequested);
    }
    if evidence.effectful_execution_requested_in_review {
        block_reasons.push(S8aRuntimeLoopBindingBlockReason::EffectfulExecutionRequestedInReview);
    }

    let (projected_inventory_after_fill, inventory_update_block_reason) =
        match observed_fill.map(|fill| apply_s8a_fill_to_inventory(inventory, fill)) {
            Some(Ok(next_inventory)) => (Some(next_inventory), None),
            Some(Err(reason)) => {
                block_reasons.push(S8aRuntimeLoopBindingBlockReason::InventoryUpdateBlocked);
                (None, Some(reason))
            }
            None => (None, None),
        };

    let loop_ready_for_fresh_exact_approval = block_reasons.is_empty();

    S8aRuntimeLoopBindingReview {
        event: S8A_RUNTIME_LOOP_BINDING_REVIEW_EVENT,
        block_reasons,
        session_gate_review,
        prepared_order: scout_admission_review.prepared_order.clone(),
        scout_admission_review,
        projected_inventory_after_fill,
        inventory_update_block_reason,
        loop_ready_for_fresh_exact_approval,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeOrchestrationPreviewStep {
    pub snapshot: S8aScoutAdmissionSnapshot,
    pub observed_fill: Option<S8aFillEvent>,
    pub close_round_after_step: bool,
    pub s7w_reconciliation_passed: bool,
    pub realized_loss_delta_usdc: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aRuntimeOrchestrationPreviewBlockReason {
    EmptyStepSet,
    ActiveMarketConditionMismatch,
    LoopBindingBlocked,
    MissingObservedFillForAdmittedStep,
    InventoryUpdateBlocked,
    InvalidRealizedLossDelta,
    RoundCloseWithoutS7wReconciliation,
    RoundCloseWithResidualExposure,
    SessionLossCapExceeded,
    GrossQuoteSpendCapExceeded,
    TotalOrderCapExceeded,
}

impl S8aRuntimeOrchestrationPreviewBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyStepSet => "BLOCK_EMPTY_STEP_SET",
            Self::ActiveMarketConditionMismatch => "BLOCK_ACTIVE_MARKET_CONDITION_MISMATCH",
            Self::LoopBindingBlocked => "BLOCK_LOOP_BINDING_BLOCKED",
            Self::MissingObservedFillForAdmittedStep => {
                "BLOCK_MISSING_OBSERVED_FILL_FOR_ADMITTED_STEP"
            }
            Self::InventoryUpdateBlocked => "BLOCK_INVENTORY_UPDATE_BLOCKED",
            Self::InvalidRealizedLossDelta => "BLOCK_INVALID_REALIZED_LOSS_DELTA",
            Self::RoundCloseWithoutS7wReconciliation => {
                "BLOCK_ROUND_CLOSE_WITHOUT_S7W_RECONCILIATION"
            }
            Self::RoundCloseWithResidualExposure => "BLOCK_ROUND_CLOSE_WITH_RESIDUAL_EXPOSURE",
            Self::SessionLossCapExceeded => "BLOCK_SESSION_LOSS_CAP_EXCEEDED",
            Self::GrossQuoteSpendCapExceeded => "BLOCK_GROSS_QUOTE_SPEND_CAP_EXCEEDED",
            Self::TotalOrderCapExceeded => "BLOCK_TOTAL_ORDER_CAP_EXCEEDED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeOrchestrationPreviewReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aRuntimeOrchestrationPreviewBlockReason>,
    pub step_reviews: Vec<S8aRuntimeLoopBindingReview>,
    pub final_inventory: S8aInventory,
    pub final_controller_state: BtcCompletionControllerState,
    pub final_session_state: S8aSessionState,
    pub active_condition_id: Option<String>,
    pub prepared_order_count: u32,
    pub filled_order_count: u32,
    pub closed_round_count: u32,
    pub orchestration_ready_for_fresh_exact_approval: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_runtime_orchestration_preview(
    cfg: &BtcCompletionControllerConfig,
    initial_controller_state: &BtcCompletionControllerState,
    initial_session_state: S8aSessionState,
    initial_inventory: S8aInventory,
    steps: &[S8aRuntimeOrchestrationPreviewStep],
    context: &S8aControllerAdmissionAdapterContext,
    evidence: &S8aRuntimeLoopBindingEvidence,
) -> S8aRuntimeOrchestrationPreviewReview {
    let mut block_reasons = Vec::new();
    if steps.is_empty() {
        block_reasons.push(S8aRuntimeOrchestrationPreviewBlockReason::EmptyStepSet);
    }

    let mut step_reviews = Vec::new();
    let mut controller_state = initial_controller_state.clone();
    let mut session_state = initial_session_state;
    let mut inventory = initial_inventory;
    let mut active_condition_id: Option<String> = None;
    let mut prepared_order_count = 0_u32;
    let mut filled_order_count = 0_u32;
    let mut closed_round_count = 0_u32;

    for step in steps {
        if let Some(active_condition) = active_condition_id.as_ref() {
            if step.snapshot.condition_id != *active_condition {
                block_reasons
                    .push(S8aRuntimeOrchestrationPreviewBlockReason::ActiveMarketConditionMismatch);
                break;
            }
        }

        let mut session_for_step = session_state;
        if active_condition_id.is_some() {
            session_for_step.active_market_open = false;
        }
        let loop_review = review_s8a_runtime_loop_binding(
            cfg,
            &controller_state,
            &session_for_step,
            inventory,
            &step.snapshot,
            context,
            evidence,
            step.observed_fill,
        );
        if !loop_review.loop_ready_for_fresh_exact_approval {
            block_reasons.push(S8aRuntimeOrchestrationPreviewBlockReason::LoopBindingBlocked);
            step_reviews.push(loop_review);
            break;
        }
        if loop_review.prepared_order.is_some() {
            prepared_order_count += 1;
        }
        let Some(fill) = step.observed_fill else {
            block_reasons.push(
                S8aRuntimeOrchestrationPreviewBlockReason::MissingObservedFillForAdmittedStep,
            );
            step_reviews.push(loop_review);
            break;
        };
        let next_inventory = match apply_s8a_fill_to_inventory(inventory, fill) {
            Ok(next) => next,
            Err(_) => {
                block_reasons
                    .push(S8aRuntimeOrchestrationPreviewBlockReason::InventoryUpdateBlocked);
                step_reviews.push(loop_review);
                break;
            }
        };

        filled_order_count += 1;
        if fill.action == S8aAdapterOrderAction::Buy {
            session_state.cumulative_gross_quote_spend_usdc +=
                fill.actual_filled_qty_shares * fill.avg_fill_price;
        }
        session_state.total_order_submissions += 1;
        session_state.active_round_risk_at_work_usdc = next_inventory.residual_qty();
        session_state.active_market_open = true;
        active_condition_id = Some(step.snapshot.condition_id.clone());

        let mut condition_state = controller_state.condition_state(&step.snapshot.condition_id);
        condition_state.yes_qty = next_inventory.yes_qty;
        condition_state.no_qty = next_inventory.no_qty;
        condition_state.last_accept_ts_ms = loop_review
            .scout_admission_review
            .candidate
            .as_ref()
            .map(|candidate| candidate.ts_ms);
        controller_state.set_condition_state(step.snapshot.condition_id.clone(), condition_state);
        inventory = next_inventory;

        if !step.realized_loss_delta_usdc.is_finite() || step.realized_loss_delta_usdc < 0.0 {
            block_reasons.push(S8aRuntimeOrchestrationPreviewBlockReason::InvalidRealizedLossDelta);
            step_reviews.push(loop_review);
            break;
        }

        if step.close_round_after_step {
            if !step.s7w_reconciliation_passed {
                block_reasons.push(
                    S8aRuntimeOrchestrationPreviewBlockReason::RoundCloseWithoutS7wReconciliation,
                );
            }
            if inventory.residual_qty() > 1e-9 {
                block_reasons.push(
                    S8aRuntimeOrchestrationPreviewBlockReason::RoundCloseWithResidualExposure,
                );
            }
            if block_reasons.is_empty() {
                session_state.completed_rounds += 1;
                session_state.realized_session_loss_usdc += step.realized_loss_delta_usdc;
                session_state.active_market_open = false;
                session_state.active_round_risk_at_work_usdc = 0.0;
                active_condition_id = None;
                inventory = S8aInventory::default();
                controller_state.set_condition_state(
                    step.snapshot.condition_id.clone(),
                    crate::polymarket::btc_completion_controller::BtcCompletionConditionState {
                        yes_qty: 0.0,
                        no_qty: 0.0,
                        last_accept_ts_ms: condition_state.last_accept_ts_ms,
                    },
                );
                closed_round_count += 1;
            }
        }

        if session_state.realized_session_loss_usdc > S8A_SESSION_HARD_LOSS_CAP_USDC + 1e-9 {
            block_reasons.push(S8aRuntimeOrchestrationPreviewBlockReason::SessionLossCapExceeded);
        }
        if session_state.cumulative_gross_quote_spend_usdc > 18.0 + 1e-9 {
            block_reasons
                .push(S8aRuntimeOrchestrationPreviewBlockReason::GrossQuoteSpendCapExceeded);
        }
        if session_state.total_order_submissions > 9 {
            block_reasons.push(S8aRuntimeOrchestrationPreviewBlockReason::TotalOrderCapExceeded);
        }

        step_reviews.push(loop_review);
        if !block_reasons.is_empty() {
            break;
        }
    }

    let orchestration_ready_for_fresh_exact_approval =
        block_reasons.is_empty() && prepared_order_count > 0;

    S8aRuntimeOrchestrationPreviewReview {
        event: S8A_RUNTIME_ORCHESTRATION_PREVIEW_EVENT,
        block_reasons,
        step_reviews,
        final_inventory: inventory,
        final_controller_state: controller_state,
        final_session_state: session_state,
        active_condition_id,
        prepared_order_count,
        filled_order_count,
        closed_round_count,
        orchestration_ready_for_fresh_exact_approval,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeWrapperPreflightEvidence {
    pub reviewed_host: String,
    pub official_rest_url: String,
    pub approval_scope: String,
    pub s8g_packet_sha256: String,
    pub s8g_result_sha256: String,
    pub s8e_packet_sha256: String,
    pub s8e_result_sha256: String,
    pub local_runtime_source_sha256: String,
    pub remote_runtime_source_sha256: String,
    pub local_adapter_source_sha256: String,
    pub remote_adapter_source_sha256: String,
    pub prepared_order_fixture_sha256: String,
    pub fresh_ssh_preflight_performed: bool,
    pub fresh_ssh_preflight_age_ms: u64,
    pub max_fresh_ssh_preflight_age_ms: u64,
    pub preview_without_approval_exit_code: i32,
    pub no_order_auth_preview_passed: bool,
    pub exact_order_path_preview_passed: bool,
    pub exact_order_path_preview_no_submit: bool,
    pub execute_mode_requires_execute_approved: bool,
    pub execute_without_execute_approved_exits_nonzero: bool,
    pub no_secret_or_raw_signature_output: bool,
    pub no_order_cancel_merge_redeem_performed: bool,
    pub remote_b_process_table_clean: bool,
    pub runtime_opens_ws: bool,
    pub b_owned_direct_public_ws_connection_count: u32,
    pub shared_ingress_dependency: bool,
    pub uses_c_artifacts: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub candidate_import_allowed: bool,
    pub funding_live_latest_or_deploy_requested: bool,
    pub effectful_execution_requested_in_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aRuntimeWrapperPreflightBlockReason {
    OrchestrationPreviewNotReady,
    ReviewedHostMismatch,
    OfficialRestUrlMismatch,
    ApprovalScopeMismatch,
    InvalidS8gPacketHash,
    InvalidS8gResultHash,
    InvalidS8ePacketHash,
    InvalidS8eResultHash,
    InvalidRuntimeSourceHash,
    InvalidAdapterSourceHash,
    RuntimeSourceHashMismatch,
    AdapterSourceHashMismatch,
    InvalidPreparedOrderFixtureHash,
    FreshSshPreflightMissing,
    FreshSshPreflightStale,
    PreviewWithoutApprovalExitNot66,
    NoOrderAuthPreviewNotPassed,
    ExactOrderPathPreviewNotPassed,
    ExactOrderPathPreviewAllowsSubmit,
    ExecuteModeDoesNotRequireExecuteApproved,
    ExecuteWithoutExecuteApprovedDidNotFailClosed,
    SecretOrRawSignatureOutputAllowed,
    OrderCancelMergeRedeemPerformedInReview,
    RemoteBProcessTableNotClean,
    RuntimeOpensWs,
    TooManyBOwnedDirectPublicWs,
    SharedIngressDependency,
    CArtifactsRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    CandidateImportRequested,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionRequestedInReview,
}

impl S8aRuntimeWrapperPreflightBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OrchestrationPreviewNotReady => "BLOCK_ORCHESTRATION_PREVIEW_NOT_READY",
            Self::ReviewedHostMismatch => "BLOCK_REVIEWED_HOST_MISMATCH",
            Self::OfficialRestUrlMismatch => "BLOCK_OFFICIAL_REST_URL_MISMATCH",
            Self::ApprovalScopeMismatch => "BLOCK_APPROVAL_SCOPE_MISMATCH",
            Self::InvalidS8gPacketHash => "BLOCK_INVALID_S8G_PACKET_HASH",
            Self::InvalidS8gResultHash => "BLOCK_INVALID_S8G_RESULT_HASH",
            Self::InvalidS8ePacketHash => "BLOCK_INVALID_S8E_PACKET_HASH",
            Self::InvalidS8eResultHash => "BLOCK_INVALID_S8E_RESULT_HASH",
            Self::InvalidRuntimeSourceHash => "BLOCK_INVALID_RUNTIME_SOURCE_HASH",
            Self::InvalidAdapterSourceHash => "BLOCK_INVALID_ADAPTER_SOURCE_HASH",
            Self::RuntimeSourceHashMismatch => "BLOCK_RUNTIME_SOURCE_HASH_MISMATCH",
            Self::AdapterSourceHashMismatch => "BLOCK_ADAPTER_SOURCE_HASH_MISMATCH",
            Self::InvalidPreparedOrderFixtureHash => "BLOCK_INVALID_PREPARED_ORDER_FIXTURE_HASH",
            Self::FreshSshPreflightMissing => "BLOCK_FRESH_SSH_PREFLIGHT_MISSING",
            Self::FreshSshPreflightStale => "BLOCK_FRESH_SSH_PREFLIGHT_STALE",
            Self::PreviewWithoutApprovalExitNot66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_EXIT_NOT_66",
            Self::NoOrderAuthPreviewNotPassed => "BLOCK_NO_ORDER_AUTH_PREVIEW_NOT_PASSED",
            Self::ExactOrderPathPreviewNotPassed => "BLOCK_EXACT_ORDER_PATH_PREVIEW_NOT_PASSED",
            Self::ExactOrderPathPreviewAllowsSubmit => {
                "BLOCK_EXACT_ORDER_PATH_PREVIEW_ALLOWS_SUBMIT"
            }
            Self::ExecuteModeDoesNotRequireExecuteApproved => {
                "BLOCK_EXECUTE_MODE_DOES_NOT_REQUIRE_EXECUTE_APPROVED"
            }
            Self::ExecuteWithoutExecuteApprovedDidNotFailClosed => {
                "BLOCK_EXECUTE_WITHOUT_EXECUTE_APPROVED_DID_NOT_FAIL_CLOSED"
            }
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::OrderCancelMergeRedeemPerformedInReview => {
                "BLOCK_ORDER_CANCEL_MERGE_REDEEM_PERFORMED_IN_REVIEW"
            }
            Self::RemoteBProcessTableNotClean => "BLOCK_REMOTE_B_PROCESS_TABLE_NOT_CLEAN",
            Self::RuntimeOpensWs => "BLOCK_RUNTIME_OPENS_WS",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
            Self::SharedIngressDependency => "BLOCK_SHARED_INGRESS_DEPENDENCY",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionRequestedInReview => {
                "BLOCK_EFFECTFUL_EXECUTION_REQUESTED_IN_REVIEW"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRuntimeWrapperPreflightReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aRuntimeWrapperPreflightBlockReason>,
    pub wrapper_preflight_ready_for_fresh_exact_approval_request: bool,
    pub orchestration_preview_ready: bool,
    pub source_hashes_bound: bool,
    pub preview_gates_bound: bool,
    pub remote_preflight_bound: bool,
    pub forbidden_paths_absent: bool,
    pub effectful_execution_permitted: bool,
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub fn review_s8a_runtime_wrapper_preflight(
    orchestration: &S8aRuntimeOrchestrationPreviewReview,
    evidence: &S8aRuntimeWrapperPreflightEvidence,
) -> S8aRuntimeWrapperPreflightReview {
    let mut block_reasons = Vec::new();
    if !orchestration.orchestration_ready_for_fresh_exact_approval {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::OrchestrationPreviewNotReady);
    }
    if evidence.reviewed_host != S8A_REVIEWED_HOST {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::ReviewedHostMismatch);
    }
    if evidence.official_rest_url != S8A_OFFICIAL_CLOB_REST_URL {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::OfficialRestUrlMismatch);
    }
    if evidence.approval_scope != S8A_NATIVE_RUNTIME_SCOPE {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::ApprovalScopeMismatch);
    }
    if !is_sha256_hex(&evidence.s8g_packet_sha256) {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidS8gPacketHash);
    }
    if !is_sha256_hex(&evidence.s8g_result_sha256) {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidS8gResultHash);
    }
    if !is_sha256_hex(&evidence.s8e_packet_sha256) {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidS8ePacketHash);
    }
    if !is_sha256_hex(&evidence.s8e_result_sha256) {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidS8eResultHash);
    }
    if !is_sha256_hex(&evidence.local_runtime_source_sha256)
        || !is_sha256_hex(&evidence.remote_runtime_source_sha256)
    {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidRuntimeSourceHash);
    }
    if !is_sha256_hex(&evidence.local_adapter_source_sha256)
        || !is_sha256_hex(&evidence.remote_adapter_source_sha256)
    {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidAdapterSourceHash);
    }
    if evidence.local_runtime_source_sha256 != evidence.remote_runtime_source_sha256 {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::RuntimeSourceHashMismatch);
    }
    if evidence.local_adapter_source_sha256 != evidence.remote_adapter_source_sha256 {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::AdapterSourceHashMismatch);
    }
    if !is_sha256_hex(&evidence.prepared_order_fixture_sha256) {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::InvalidPreparedOrderFixtureHash);
    }
    if !evidence.fresh_ssh_preflight_performed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::FreshSshPreflightMissing);
    }
    if evidence.fresh_ssh_preflight_age_ms > evidence.max_fresh_ssh_preflight_age_ms {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::FreshSshPreflightStale);
    }
    if evidence.preview_without_approval_exit_code != 66 {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::PreviewWithoutApprovalExitNot66);
    }
    if !evidence.no_order_auth_preview_passed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::NoOrderAuthPreviewNotPassed);
    }
    if !evidence.exact_order_path_preview_passed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::ExactOrderPathPreviewNotPassed);
    }
    if !evidence.exact_order_path_preview_no_submit {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::ExactOrderPathPreviewAllowsSubmit);
    }
    if !evidence.execute_mode_requires_execute_approved {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::ExecuteModeDoesNotRequireExecuteApproved);
    }
    if !evidence.execute_without_execute_approved_exits_nonzero {
        block_reasons.push(
            S8aRuntimeWrapperPreflightBlockReason::ExecuteWithoutExecuteApprovedDidNotFailClosed,
        );
    }
    if !evidence.no_secret_or_raw_signature_output {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if !evidence.no_order_cancel_merge_redeem_performed {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::OrderCancelMergeRedeemPerformedInReview);
    }
    if !evidence.remote_b_process_table_clean {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::RemoteBProcessTableNotClean);
    }
    if evidence.runtime_opens_ws {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::RuntimeOpensWs);
    }
    if evidence.b_owned_direct_public_ws_connection_count > 1 {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::TooManyBOwnedDirectPublicWs);
    }
    if evidence.shared_ingress_dependency {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::SharedIngressDependency);
    }
    if evidence.uses_c_artifacts {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::CArtifactsRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.candidate_import_allowed {
        block_reasons.push(S8aRuntimeWrapperPreflightBlockReason::CandidateImportRequested);
    }
    if evidence.funding_live_latest_or_deploy_requested {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::FundingLiveLatestOrDeployRequested);
    }
    if evidence.effectful_execution_requested_in_review {
        block_reasons
            .push(S8aRuntimeWrapperPreflightBlockReason::EffectfulExecutionRequestedInReview);
    }

    let orchestration_preview_ready = orchestration.orchestration_ready_for_fresh_exact_approval;
    let source_hashes_bound = is_sha256_hex(&evidence.local_runtime_source_sha256)
        && evidence.local_runtime_source_sha256 == evidence.remote_runtime_source_sha256
        && is_sha256_hex(&evidence.local_adapter_source_sha256)
        && evidence.local_adapter_source_sha256 == evidence.remote_adapter_source_sha256;
    let preview_gates_bound = evidence.preview_without_approval_exit_code == 66
        && evidence.no_order_auth_preview_passed
        && evidence.exact_order_path_preview_passed
        && evidence.exact_order_path_preview_no_submit
        && evidence.execute_mode_requires_execute_approved
        && evidence.execute_without_execute_approved_exits_nonzero;
    let remote_preflight_bound = evidence.fresh_ssh_preflight_performed
        && evidence.fresh_ssh_preflight_age_ms <= evidence.max_fresh_ssh_preflight_age_ms
        && evidence.remote_b_process_table_clean
        && !evidence.runtime_opens_ws
        && evidence.b_owned_direct_public_ws_connection_count <= 1;
    let forbidden_paths_absent = evidence.no_secret_or_raw_signature_output
        && evidence.no_order_cancel_merge_redeem_performed
        && !evidence.shared_ingress_dependency
        && !evidence.uses_c_artifacts
        && !evidence.online_tuning_allowed
        && !evidence.strategy_discovery_allowed
        && !evidence.candidate_import_allowed
        && !evidence.funding_live_latest_or_deploy_requested
        && !evidence.effectful_execution_requested_in_review;

    S8aRuntimeWrapperPreflightReview {
        event: S8A_RUNTIME_WRAPPER_PREFLIGHT_REVIEW_EVENT,
        wrapper_preflight_ready_for_fresh_exact_approval_request: block_reasons.is_empty(),
        block_reasons,
        orchestration_preview_ready,
        source_hashes_bound,
        preview_gates_bound,
        remote_preflight_bound,
        forbidden_paths_absent,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aOneRunOrchestrationEvidence {
    pub native_runtime_loop_driver_source_bound: bool,
    pub scout_cache_to_controller_bound: bool,
    pub controller_to_native_order_primitive_bound: bool,
    pub runtime_accepts_only_prepared_orders: bool,
    pub planned_order_count: u32,
    pub exact_approval_hash_bound_to_every_submit: bool,
    pub exact_approval_scope_bound_to_every_submit: bool,
    pub runtime_order_result_ledger_enabled: bool,
    pub order_id_or_failure_recorded_per_attempt: bool,
    pub filled_qty_from_exchange_or_order_status_only: bool,
    pub actual_filled_qty_ledger_updates_inventory: bool,
    pub submitted_size_never_counts_as_inventory: bool,
    pub partial_fill_threshold_absent: bool,
    pub no_forced_complement: bool,
    pub session_caps_checked_before_each_submission: bool,
    pub stop_new_entry_on_loss_cap_or_active_market: bool,
    pub max_round_count: u32,
    pub session_hard_loss_cap_usdc: f64,
    pub max_active_market_count: u32,
    pub max_total_order_submissions: u32,
    pub s7w_reconciliation_bound_to_run_result: bool,
    pub exact_approved_receipt_tier_required: bool,
    pub positive_collateral_delta_required_for_recovery: bool,
    pub no_open_order_remainder_required: bool,
    pub residual_exposure_zero_required: bool,
    pub review_only_fixture_receipts_rejected: bool,
    pub runtime_opens_ws: bool,
    pub b_owned_direct_public_ws_connection_count: u32,
    pub shared_ingress_dependency: bool,
    pub uses_c_artifacts: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub candidate_import_allowed: bool,
    pub secret_or_raw_signature_output_allowed: bool,
    pub funding_live_latest_or_deploy_requested: bool,
    pub effectful_execution_requested_in_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aOneRunOrchestrationBlockReason {
    WrapperPreflightNotReady,
    OrchestrationPreviewNotReady,
    NativeRuntimeLoopDriverMissing,
    ScoutControllerBindingMissing,
    ControllerRuntimePrimitiveBindingMissing,
    RuntimeDoesNotAcceptOnlyPreparedOrders,
    PlannedOrderCountMismatch,
    PlannedOrderCountEmpty,
    PlannedOrderCountAboveCap,
    ExactApprovalHashNotBoundToEverySubmit,
    ExactApprovalScopeNotBoundToEverySubmit,
    RuntimeOrderResultLedgerMissing,
    OrderIdOrFailureNotRecordedPerAttempt,
    FilledQtyNotExchangeObserved,
    ActualFilledQtyLedgerMissing,
    SubmittedSizeCountsAsInventory,
    PartialFillThresholdEnabled,
    ForcedComplementAllowed,
    SessionCapsNotCheckedBeforeEachSubmission,
    StopNewEntryRuleMissing,
    MaxRoundCapMismatch,
    SessionHardLossCapMismatch,
    MaxActiveMarketCapMismatch,
    MaxTotalOrderSubmissionCapMismatch,
    S7wReconciliationNotBound,
    ExactApprovedReceiptTierNotRequired,
    PositiveCollateralDeltaNotRequiredForRecovery,
    OpenOrderRemainderNotRequired,
    ResidualExposureZeroNotRequired,
    ReviewOnlyFixtureReceiptAccepted,
    NoRoundClosurePath,
    RuntimeOpensWs,
    TooManyBOwnedDirectPublicWs,
    SharedIngressDependency,
    CArtifactsRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    CandidateImportRequested,
    SecretOrRawSignatureOutputAllowed,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionRequestedInReview,
}

impl S8aOneRunOrchestrationBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WrapperPreflightNotReady => "BLOCK_WRAPPER_PREFLIGHT_NOT_READY",
            Self::OrchestrationPreviewNotReady => "BLOCK_ORCHESTRATION_PREVIEW_NOT_READY",
            Self::NativeRuntimeLoopDriverMissing => "BLOCK_NATIVE_RUNTIME_LOOP_DRIVER_MISSING",
            Self::ScoutControllerBindingMissing => "BLOCK_SCOUT_CONTROLLER_BINDING_MISSING",
            Self::ControllerRuntimePrimitiveBindingMissing => {
                "BLOCK_CONTROLLER_RUNTIME_PRIMITIVE_BINDING_MISSING"
            }
            Self::RuntimeDoesNotAcceptOnlyPreparedOrders => {
                "BLOCK_RUNTIME_DOES_NOT_ACCEPT_ONLY_PREPARED_ORDERS"
            }
            Self::PlannedOrderCountMismatch => "BLOCK_PLANNED_ORDER_COUNT_MISMATCH",
            Self::PlannedOrderCountEmpty => "BLOCK_PLANNED_ORDER_COUNT_EMPTY",
            Self::PlannedOrderCountAboveCap => "BLOCK_PLANNED_ORDER_COUNT_ABOVE_CAP",
            Self::ExactApprovalHashNotBoundToEverySubmit => {
                "BLOCK_EXACT_APPROVAL_HASH_NOT_BOUND_TO_EVERY_SUBMIT"
            }
            Self::ExactApprovalScopeNotBoundToEverySubmit => {
                "BLOCK_EXACT_APPROVAL_SCOPE_NOT_BOUND_TO_EVERY_SUBMIT"
            }
            Self::RuntimeOrderResultLedgerMissing => "BLOCK_RUNTIME_ORDER_RESULT_LEDGER_MISSING",
            Self::OrderIdOrFailureNotRecordedPerAttempt => {
                "BLOCK_ORDER_ID_OR_FAILURE_NOT_RECORDED_PER_ATTEMPT"
            }
            Self::FilledQtyNotExchangeObserved => "BLOCK_FILLED_QTY_NOT_EXCHANGE_OBSERVED",
            Self::ActualFilledQtyLedgerMissing => "BLOCK_ACTUAL_FILLED_QTY_LEDGER_MISSING",
            Self::SubmittedSizeCountsAsInventory => "BLOCK_SUBMITTED_SIZE_COUNTS_AS_INVENTORY",
            Self::PartialFillThresholdEnabled => "BLOCK_PARTIAL_FILL_THRESHOLD_ENABLED",
            Self::ForcedComplementAllowed => "BLOCK_FORCED_COMPLEMENT_ALLOWED",
            Self::SessionCapsNotCheckedBeforeEachSubmission => {
                "BLOCK_SESSION_CAPS_NOT_CHECKED_BEFORE_EACH_SUBMISSION"
            }
            Self::StopNewEntryRuleMissing => "BLOCK_STOP_NEW_ENTRY_RULE_MISSING",
            Self::MaxRoundCapMismatch => "BLOCK_MAX_ROUND_CAP_MISMATCH",
            Self::SessionHardLossCapMismatch => "BLOCK_SESSION_HARD_LOSS_CAP_MISMATCH",
            Self::MaxActiveMarketCapMismatch => "BLOCK_MAX_ACTIVE_MARKET_CAP_MISMATCH",
            Self::MaxTotalOrderSubmissionCapMismatch => {
                "BLOCK_MAX_TOTAL_ORDER_SUBMISSION_CAP_MISMATCH"
            }
            Self::S7wReconciliationNotBound => "BLOCK_S7W_RECONCILIATION_NOT_BOUND",
            Self::ExactApprovedReceiptTierNotRequired => {
                "BLOCK_EXACT_APPROVED_RECEIPT_TIER_NOT_REQUIRED"
            }
            Self::PositiveCollateralDeltaNotRequiredForRecovery => {
                "BLOCK_POSITIVE_COLLATERAL_DELTA_NOT_REQUIRED_FOR_RECOVERY"
            }
            Self::OpenOrderRemainderNotRequired => "BLOCK_OPEN_ORDER_REMAINDER_NOT_REQUIRED",
            Self::ResidualExposureZeroNotRequired => "BLOCK_RESIDUAL_EXPOSURE_ZERO_NOT_REQUIRED",
            Self::ReviewOnlyFixtureReceiptAccepted => "BLOCK_REVIEW_ONLY_FIXTURE_RECEIPT_ACCEPTED",
            Self::NoRoundClosurePath => "BLOCK_NO_ROUND_CLOSURE_PATH",
            Self::RuntimeOpensWs => "BLOCK_RUNTIME_OPENS_WS",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
            Self::SharedIngressDependency => "BLOCK_SHARED_INGRESS_DEPENDENCY",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionRequestedInReview => {
                "BLOCK_EFFECTFUL_EXECUTION_REQUESTED_IN_REVIEW"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aOneRunOrchestrationReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aOneRunOrchestrationBlockReason>,
    pub wrapper_preflight_ready: bool,
    pub orchestration_preview_ready: bool,
    pub source_loop_bound: bool,
    pub order_submit_contract_bound: bool,
    pub filled_qty_ledger_bound: bool,
    pub session_caps_bound: bool,
    pub s7w_run_result_bound: bool,
    pub planned_order_count: u32,
    pub closed_round_count: u32,
    pub one_run_orchestration_ready_for_fresh_exact_approval: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_one_run_orchestration(
    orchestration: &S8aRuntimeOrchestrationPreviewReview,
    wrapper_preflight: &S8aRuntimeWrapperPreflightReview,
    evidence: &S8aOneRunOrchestrationEvidence,
) -> S8aOneRunOrchestrationReview {
    let mut block_reasons = Vec::new();
    if !wrapper_preflight.wrapper_preflight_ready_for_fresh_exact_approval_request {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::WrapperPreflightNotReady);
    }
    if !orchestration.orchestration_ready_for_fresh_exact_approval {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::OrchestrationPreviewNotReady);
    }
    if !evidence.native_runtime_loop_driver_source_bound {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::NativeRuntimeLoopDriverMissing);
    }
    if !evidence.scout_cache_to_controller_bound {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ScoutControllerBindingMissing);
    }
    if !evidence.controller_to_native_order_primitive_bound {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::ControllerRuntimePrimitiveBindingMissing);
    }
    if !evidence.runtime_accepts_only_prepared_orders {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::RuntimeDoesNotAcceptOnlyPreparedOrders);
    }
    if evidence.planned_order_count == 0 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::PlannedOrderCountEmpty);
    }
    if evidence.planned_order_count != orchestration.prepared_order_count {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::PlannedOrderCountMismatch);
    }
    if evidence.planned_order_count > 9 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::PlannedOrderCountAboveCap);
    }
    if !evidence.exact_approval_hash_bound_to_every_submit {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::ExactApprovalHashNotBoundToEverySubmit);
    }
    if !evidence.exact_approval_scope_bound_to_every_submit {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::ExactApprovalScopeNotBoundToEverySubmit);
    }
    if !evidence.runtime_order_result_ledger_enabled {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::RuntimeOrderResultLedgerMissing);
    }
    if !evidence.order_id_or_failure_recorded_per_attempt {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::OrderIdOrFailureNotRecordedPerAttempt);
    }
    if !evidence.filled_qty_from_exchange_or_order_status_only {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::FilledQtyNotExchangeObserved);
    }
    if !evidence.actual_filled_qty_ledger_updates_inventory {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ActualFilledQtyLedgerMissing);
    }
    if !evidence.submitted_size_never_counts_as_inventory {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::SubmittedSizeCountsAsInventory);
    }
    if !evidence.partial_fill_threshold_absent {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::PartialFillThresholdEnabled);
    }
    if !evidence.no_forced_complement {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ForcedComplementAllowed);
    }
    if !evidence.session_caps_checked_before_each_submission {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::SessionCapsNotCheckedBeforeEachSubmission);
    }
    if !evidence.stop_new_entry_on_loss_cap_or_active_market {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::StopNewEntryRuleMissing);
    }
    if evidence.max_round_count != S8A_MAX_ROUNDS {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::MaxRoundCapMismatch);
    }
    if (evidence.session_hard_loss_cap_usdc - S8A_SESSION_HARD_LOSS_CAP_USDC).abs() > 1e-9 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::SessionHardLossCapMismatch);
    }
    if evidence.max_active_market_count != 1 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::MaxActiveMarketCapMismatch);
    }
    if evidence.max_total_order_submissions != 9 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::MaxTotalOrderSubmissionCapMismatch);
    }
    if !evidence.s7w_reconciliation_bound_to_run_result {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::S7wReconciliationNotBound);
    }
    if !evidence.exact_approved_receipt_tier_required {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ExactApprovedReceiptTierNotRequired);
    }
    if !evidence.positive_collateral_delta_required_for_recovery {
        block_reasons
            .push(S8aOneRunOrchestrationBlockReason::PositiveCollateralDeltaNotRequiredForRecovery);
    }
    if !evidence.no_open_order_remainder_required {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::OpenOrderRemainderNotRequired);
    }
    if !evidence.residual_exposure_zero_required {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ResidualExposureZeroNotRequired);
    }
    if !evidence.review_only_fixture_receipts_rejected {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::ReviewOnlyFixtureReceiptAccepted);
    }
    if orchestration.closed_round_count == 0 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::NoRoundClosurePath);
    }
    if evidence.runtime_opens_ws {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::RuntimeOpensWs);
    }
    if evidence.b_owned_direct_public_ws_connection_count > 1 {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::TooManyBOwnedDirectPublicWs);
    }
    if evidence.shared_ingress_dependency {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::SharedIngressDependency);
    }
    if evidence.uses_c_artifacts {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::CArtifactsRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.candidate_import_allowed {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::CandidateImportRequested);
    }
    if evidence.secret_or_raw_signature_output_allowed {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if evidence.funding_live_latest_or_deploy_requested {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::FundingLiveLatestOrDeployRequested);
    }
    if evidence.effectful_execution_requested_in_review {
        block_reasons.push(S8aOneRunOrchestrationBlockReason::EffectfulExecutionRequestedInReview);
    }

    let source_loop_bound = evidence.native_runtime_loop_driver_source_bound
        && evidence.scout_cache_to_controller_bound
        && evidence.controller_to_native_order_primitive_bound;
    let order_submit_contract_bound = evidence.runtime_accepts_only_prepared_orders
        && evidence.exact_approval_hash_bound_to_every_submit
        && evidence.exact_approval_scope_bound_to_every_submit
        && evidence.runtime_order_result_ledger_enabled
        && evidence.order_id_or_failure_recorded_per_attempt;
    let filled_qty_ledger_bound = evidence.filled_qty_from_exchange_or_order_status_only
        && evidence.actual_filled_qty_ledger_updates_inventory
        && evidence.submitted_size_never_counts_as_inventory
        && evidence.partial_fill_threshold_absent
        && evidence.no_forced_complement;
    let session_caps_bound = evidence.session_caps_checked_before_each_submission
        && evidence.stop_new_entry_on_loss_cap_or_active_market
        && evidence.max_round_count == S8A_MAX_ROUNDS
        && (evidence.session_hard_loss_cap_usdc - S8A_SESSION_HARD_LOSS_CAP_USDC).abs() <= 1e-9
        && evidence.max_active_market_count == 1
        && evidence.max_total_order_submissions == 9;
    let s7w_run_result_bound = evidence.s7w_reconciliation_bound_to_run_result
        && evidence.exact_approved_receipt_tier_required
        && evidence.positive_collateral_delta_required_for_recovery
        && evidence.no_open_order_remainder_required
        && evidence.residual_exposure_zero_required
        && evidence.review_only_fixture_receipts_rejected
        && orchestration.closed_round_count > 0;

    S8aOneRunOrchestrationReview {
        event: S8A_ONE_RUN_ORCHESTRATION_REVIEW_EVENT,
        one_run_orchestration_ready_for_fresh_exact_approval: block_reasons.is_empty(),
        block_reasons,
        wrapper_preflight_ready: wrapper_preflight
            .wrapper_preflight_ready_for_fresh_exact_approval_request,
        orchestration_preview_ready: orchestration.orchestration_ready_for_fresh_exact_approval,
        source_loop_bound,
        order_submit_contract_bound,
        filled_qty_ledger_bound,
        session_caps_bound,
        s7w_run_result_bound,
        planned_order_count: evidence.planned_order_count,
        closed_round_count: orchestration.closed_round_count,
        effectful_execution_permitted: S8A_NATIVE_ORDER_ADAPTER_ENABLED_DEFAULT,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRemoteRuntimeProvisioningEvidence {
    pub one_run_orchestration_ready: bool,
    pub remote_host: String,
    pub remote_staging_path: String,
    pub remote_staging_path_is_b_owned: bool,
    pub remote_staging_path_is_service_dir: bool,
    pub mutates_service_dir: bool,
    pub runtime_source_sha256: String,
    pub remote_runtime_source_sha256: String,
    pub adapter_source_sha256: String,
    pub remote_adapter_source_sha256: String,
    pub clob_v2_source_sha256: String,
    pub remote_clob_v2_source_sha256: String,
    pub prepared_order_fixture_sha256: String,
    pub remote_prepared_order_fixture_sha256: String,
    pub one_run_plan_fixture_sha256: String,
    pub remote_one_run_plan_fixture_sha256: String,
    pub cargo_available_on_remote: bool,
    pub node_available_on_remote: bool,
    pub hash_bound_remote_worktree_ready: bool,
    pub remote_service_repo_dirty_or_unbound: bool,
    pub preview_without_approval_exit_code: Option<i32>,
    pub no_order_auth_preview_passed: bool,
    pub exact_order_path_preview_passed_no_submit: bool,
    pub one_run_driver_preview_passed_no_submit: bool,
    pub no_secret_or_raw_signature_output: bool,
    pub no_order_cancel_merge_redeem_performed: bool,
    pub shared_ingress_dependency: bool,
    pub uses_c_artifacts: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub candidate_import_allowed: bool,
    pub funding_live_latest_or_deploy_requested: bool,
    pub effectful_execution_requested_in_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S8aRemoteRuntimeProvisioningBlockReason {
    OneRunOrchestrationNotReady,
    RemoteHostMismatch,
    RemoteStagingPathNotBOwned,
    RemoteStagingPathIsServiceDir,
    MutatesServiceDir,
    InvalidRuntimeSourceHash,
    RuntimeSourceHashMismatch,
    InvalidAdapterSourceHash,
    AdapterSourceHashMismatch,
    InvalidClobV2SourceHash,
    ClobV2SourceHashMismatch,
    InvalidPreparedOrderFixtureHash,
    PreparedOrderFixtureHashMismatch,
    InvalidOneRunPlanFixtureHash,
    OneRunPlanFixtureHashMismatch,
    RemoteCargoUnavailable,
    RemoteNodeUnavailable,
    HashBoundRemoteWorktreeNotReady,
    RemoteServiceRepoDirtyOrUnbound,
    PreviewWithoutApprovalNotRunOrNot66,
    NoOrderAuthPreviewNotPassed,
    ExactOrderPathPreviewNotPassedNoSubmit,
    OneRunDriverPreviewNotPassedNoSubmit,
    SecretOrRawSignatureOutputAllowed,
    OrderCancelMergeRedeemPerformedInReview,
    SharedIngressDependency,
    CArtifactsRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    CandidateImportRequested,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionRequestedInReview,
}

impl S8aRemoteRuntimeProvisioningBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OneRunOrchestrationNotReady => "BLOCK_ONE_RUN_ORCHESTRATION_NOT_READY",
            Self::RemoteHostMismatch => "BLOCK_REMOTE_HOST_MISMATCH",
            Self::RemoteStagingPathNotBOwned => "BLOCK_REMOTE_STAGING_PATH_NOT_B_OWNED",
            Self::RemoteStagingPathIsServiceDir => "BLOCK_REMOTE_STAGING_PATH_IS_SERVICE_DIR",
            Self::MutatesServiceDir => "BLOCK_MUTATES_SERVICE_DIR",
            Self::InvalidRuntimeSourceHash => "BLOCK_INVALID_RUNTIME_SOURCE_HASH",
            Self::RuntimeSourceHashMismatch => "BLOCK_RUNTIME_SOURCE_HASH_MISMATCH",
            Self::InvalidAdapterSourceHash => "BLOCK_INVALID_ADAPTER_SOURCE_HASH",
            Self::AdapterSourceHashMismatch => "BLOCK_ADAPTER_SOURCE_HASH_MISMATCH",
            Self::InvalidClobV2SourceHash => "BLOCK_INVALID_CLOB_V2_SOURCE_HASH",
            Self::ClobV2SourceHashMismatch => "BLOCK_CLOB_V2_SOURCE_HASH_MISMATCH",
            Self::InvalidPreparedOrderFixtureHash => "BLOCK_INVALID_PREPARED_ORDER_FIXTURE_HASH",
            Self::PreparedOrderFixtureHashMismatch => "BLOCK_PREPARED_ORDER_FIXTURE_HASH_MISMATCH",
            Self::InvalidOneRunPlanFixtureHash => "BLOCK_INVALID_ONE_RUN_PLAN_FIXTURE_HASH",
            Self::OneRunPlanFixtureHashMismatch => "BLOCK_ONE_RUN_PLAN_FIXTURE_HASH_MISMATCH",
            Self::RemoteCargoUnavailable => "BLOCK_REMOTE_CARGO_UNAVAILABLE",
            Self::RemoteNodeUnavailable => "BLOCK_REMOTE_NODE_UNAVAILABLE",
            Self::HashBoundRemoteWorktreeNotReady => "BLOCK_HASH_BOUND_REMOTE_WORKTREE_NOT_READY",
            Self::RemoteServiceRepoDirtyOrUnbound => "BLOCK_REMOTE_SERVICE_REPO_DIRTY_OR_UNBOUND",
            Self::PreviewWithoutApprovalNotRunOrNot66 => {
                "BLOCK_PREVIEW_WITHOUT_APPROVAL_NOT_RUN_OR_NOT_66"
            }
            Self::NoOrderAuthPreviewNotPassed => "BLOCK_NO_ORDER_AUTH_PREVIEW_NOT_PASSED",
            Self::ExactOrderPathPreviewNotPassedNoSubmit => {
                "BLOCK_EXACT_ORDER_PATH_PREVIEW_NOT_PASSED_NO_SUBMIT"
            }
            Self::OneRunDriverPreviewNotPassedNoSubmit => {
                "BLOCK_ONE_RUN_DRIVER_PREVIEW_NOT_PASSED_NO_SUBMIT"
            }
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::OrderCancelMergeRedeemPerformedInReview => {
                "BLOCK_ORDER_CANCEL_MERGE_REDEEM_PERFORMED_IN_REVIEW"
            }
            Self::SharedIngressDependency => "BLOCK_SHARED_INGRESS_DEPENDENCY",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionRequestedInReview => {
                "BLOCK_EFFECTFUL_EXECUTION_REQUESTED_IN_REVIEW"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct S8aRemoteRuntimeProvisioningReview {
    pub event: &'static str,
    pub block_reasons: Vec<S8aRemoteRuntimeProvisioningBlockReason>,
    pub remote_hashes_bound: bool,
    pub remote_runtime_available: bool,
    pub remote_preview_gates_passed: bool,
    pub remote_staging_only: bool,
    pub forbidden_paths_absent: bool,
    pub ready_for_fresh_exact_approval_request: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_s8a_remote_runtime_provisioning(
    evidence: &S8aRemoteRuntimeProvisioningEvidence,
) -> S8aRemoteRuntimeProvisioningReview {
    let mut block_reasons = Vec::new();
    if !evidence.one_run_orchestration_ready {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::OneRunOrchestrationNotReady);
    }
    if evidence.remote_host != S8A_REVIEWED_HOST {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RemoteHostMismatch);
    }
    if !evidence.remote_staging_path_is_b_owned {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RemoteStagingPathNotBOwned);
    }
    if evidence.remote_staging_path_is_service_dir {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RemoteStagingPathIsServiceDir);
    }
    if evidence.mutates_service_dir {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::MutatesServiceDir);
    }

    if !is_sha256_hex(&evidence.runtime_source_sha256)
        || !is_sha256_hex(&evidence.remote_runtime_source_sha256)
    {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::InvalidRuntimeSourceHash);
    }
    if evidence.runtime_source_sha256 != evidence.remote_runtime_source_sha256 {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RuntimeSourceHashMismatch);
    }
    if !is_sha256_hex(&evidence.adapter_source_sha256)
        || !is_sha256_hex(&evidence.remote_adapter_source_sha256)
    {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::InvalidAdapterSourceHash);
    }
    if evidence.adapter_source_sha256 != evidence.remote_adapter_source_sha256 {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::AdapterSourceHashMismatch);
    }
    if !is_sha256_hex(&evidence.clob_v2_source_sha256)
        || !is_sha256_hex(&evidence.remote_clob_v2_source_sha256)
    {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::InvalidClobV2SourceHash);
    }
    if evidence.clob_v2_source_sha256 != evidence.remote_clob_v2_source_sha256 {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::ClobV2SourceHashMismatch);
    }
    if !is_sha256_hex(&evidence.prepared_order_fixture_sha256)
        || !is_sha256_hex(&evidence.remote_prepared_order_fixture_sha256)
    {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::InvalidPreparedOrderFixtureHash);
    }
    if evidence.prepared_order_fixture_sha256 != evidence.remote_prepared_order_fixture_sha256 {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::PreparedOrderFixtureHashMismatch);
    }
    if !is_sha256_hex(&evidence.one_run_plan_fixture_sha256)
        || !is_sha256_hex(&evidence.remote_one_run_plan_fixture_sha256)
    {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::InvalidOneRunPlanFixtureHash);
    }
    if evidence.one_run_plan_fixture_sha256 != evidence.remote_one_run_plan_fixture_sha256 {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::OneRunPlanFixtureHashMismatch);
    }

    if !evidence.cargo_available_on_remote {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RemoteCargoUnavailable);
    }
    if !evidence.node_available_on_remote {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::RemoteNodeUnavailable);
    }
    if !evidence.hash_bound_remote_worktree_ready {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::HashBoundRemoteWorktreeNotReady);
    }
    if evidence.remote_service_repo_dirty_or_unbound {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::RemoteServiceRepoDirtyOrUnbound);
    }
    if evidence.preview_without_approval_exit_code != Some(66) {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::PreviewWithoutApprovalNotRunOrNot66);
    }
    if !evidence.no_order_auth_preview_passed {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::NoOrderAuthPreviewNotPassed);
    }
    if !evidence.exact_order_path_preview_passed_no_submit {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::ExactOrderPathPreviewNotPassedNoSubmit);
    }
    if !evidence.one_run_driver_preview_passed_no_submit {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::OneRunDriverPreviewNotPassedNoSubmit);
    }
    if !evidence.no_secret_or_raw_signature_output {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::SecretOrRawSignatureOutputAllowed);
    }
    if !evidence.no_order_cancel_merge_redeem_performed {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::OrderCancelMergeRedeemPerformedInReview);
    }
    if evidence.shared_ingress_dependency {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::SharedIngressDependency);
    }
    if evidence.uses_c_artifacts {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::CArtifactsRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.candidate_import_allowed {
        block_reasons.push(S8aRemoteRuntimeProvisioningBlockReason::CandidateImportRequested);
    }
    if evidence.funding_live_latest_or_deploy_requested {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::FundingLiveLatestOrDeployRequested);
    }
    if evidence.effectful_execution_requested_in_review {
        block_reasons
            .push(S8aRemoteRuntimeProvisioningBlockReason::EffectfulExecutionRequestedInReview);
    }

    let remote_hashes_bound = is_sha256_hex(&evidence.runtime_source_sha256)
        && evidence.runtime_source_sha256 == evidence.remote_runtime_source_sha256
        && is_sha256_hex(&evidence.adapter_source_sha256)
        && evidence.adapter_source_sha256 == evidence.remote_adapter_source_sha256
        && is_sha256_hex(&evidence.clob_v2_source_sha256)
        && evidence.clob_v2_source_sha256 == evidence.remote_clob_v2_source_sha256
        && is_sha256_hex(&evidence.prepared_order_fixture_sha256)
        && evidence.prepared_order_fixture_sha256 == evidence.remote_prepared_order_fixture_sha256
        && is_sha256_hex(&evidence.one_run_plan_fixture_sha256)
        && evidence.one_run_plan_fixture_sha256 == evidence.remote_one_run_plan_fixture_sha256;
    let remote_runtime_available = evidence.cargo_available_on_remote
        && evidence.node_available_on_remote
        && evidence.hash_bound_remote_worktree_ready
        && !evidence.remote_service_repo_dirty_or_unbound;
    let remote_preview_gates_passed = evidence.preview_without_approval_exit_code == Some(66)
        && evidence.no_order_auth_preview_passed
        && evidence.exact_order_path_preview_passed_no_submit
        && evidence.one_run_driver_preview_passed_no_submit;
    let remote_staging_only = evidence.remote_staging_path_is_b_owned
        && !evidence.remote_staging_path_is_service_dir
        && !evidence.mutates_service_dir;
    let forbidden_paths_absent = evidence.no_secret_or_raw_signature_output
        && evidence.no_order_cancel_merge_redeem_performed
        && !evidence.shared_ingress_dependency
        && !evidence.uses_c_artifacts
        && !evidence.online_tuning_allowed
        && !evidence.strategy_discovery_allowed
        && !evidence.candidate_import_allowed
        && !evidence.funding_live_latest_or_deploy_requested
        && !evidence.effectful_execution_requested_in_review;

    S8aRemoteRuntimeProvisioningReview {
        event: S8A_REMOTE_RUNTIME_PROVISIONING_REVIEW_EVENT,
        ready_for_fresh_exact_approval_request: block_reasons.is_empty(),
        block_reasons,
        remote_hashes_bound,
        remote_runtime_available,
        remote_preview_gates_passed,
        remote_staging_only,
        forbidden_paths_absent,
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

    fn scout_snapshot(side: Side) -> S8aScoutAdmissionSnapshot {
        S8aScoutAdmissionSnapshot {
            asset: "BTC".to_string(),
            event_reason: "last_trade_price".to_string(),
            slug: "btc-updown-5m-s8a-fixture".to_string(),
            condition_id: "0xcondition".to_string(),
            yes: S8aScoutBookSideSnapshot {
                token_id: "token-yes".to_string(),
                best_ask: Some(0.50),
                top5_ask_qty: 25.0,
                book_age_ms: Some(10),
            },
            no: S8aScoutBookSideSnapshot {
                token_id: "token-no".to_string(),
                best_ask: Some(0.51),
                top5_ask_qty: 25.0,
                book_age_ms: Some(12),
            },
            latest_public_buy_trade: Some(S8aScoutPublicBuyTrade {
                side,
                taker_side: "BUY".to_string(),
                price: 0.49,
                size: 5.0,
                event_ts_ms: 1_000,
                observed_ts_ms: 1_010,
                recv_lag_ms: Some(10),
            }),
            offset_s: 10.0,
            time_to_end_s: Some(250.0),
            forbidden_decision_fields_present: false,
            snapshot_index_used_as_rank: false,
            source_is_b_owned_direct_public_ws: true,
            shared_ingress_dependency: false,
            b_owned_direct_public_ws_connection_count: 1,
        }
    }

    fn scout_snapshot_at(side: Side, observed_ts_ms: u64) -> S8aScoutAdmissionSnapshot {
        let mut snapshot = scout_snapshot(side);
        snapshot
            .latest_public_buy_trade
            .as_mut()
            .expect("trade")
            .event_ts_ms = observed_ts_ms.saturating_sub(10);
        snapshot
            .latest_public_buy_trade
            .as_mut()
            .expect("trade")
            .observed_ts_ms = observed_ts_ms;
        snapshot
    }

    fn clean_s8a_loop_binding_evidence() -> S8aRuntimeLoopBindingEvidence {
        S8aRuntimeLoopBindingEvidence {
            native_effectful_runtime_source_bound: true,
            runtime_requires_fresh_exact_approval: true,
            runtime_preview_without_approval_exits_66: true,
            no_order_auth_preview_available: true,
            exact_order_path_preview_available: true,
            actual_filled_qty_ledger_enabled: true,
            submitted_size_counts_as_inventory: false,
            partial_fill_threshold_enabled: false,
            forced_complement_allowed: false,
            session_caps_bound: true,
            s7w_reconciliation_required: true,
            s7w_exact_approved_receipt_tier_required: true,
            review_only_fixture_receipt_accepted: false,
            no_open_order_remainder_required: true,
            residual_exposure_zero_required: true,
            runtime_opens_own_ws: false,
            b_owned_direct_public_ws_connection_count: 1,
            shared_ingress_dependency: false,
            uses_c_artifacts: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            candidate_import_allowed: false,
            prints_secret_or_raw_signature: false,
            funding_live_latest_or_deploy_requested: false,
            effectful_execution_requested_in_review: false,
        }
    }

    fn clean_s8a_session_state() -> S8aSessionState {
        S8aSessionState {
            completed_rounds: 0,
            active_market_open: false,
            realized_session_loss_usdc: 0.0,
            active_round_risk_at_work_usdc: 0.0,
            cumulative_gross_quote_spend_usdc: 0.0,
            total_order_submissions: 0,
            cancel_count: 0,
            recovery_tx_count: 0,
        }
    }

    fn hex64(ch: char) -> String {
        std::iter::repeat(ch).take(64).collect()
    }

    fn clean_s8s_prepared_order() -> S8aPreparedVenueOrder {
        S8aPreparedVenueOrder {
            condition_id: "0x1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            token_id: "1000000000000000000000000000000000000000000000000000000000000001"
                .to_string(),
            side: Side::Yes,
            action: S8aAdapterOrderAction::Buy,
            order_type: S8aVenueOrderType::Gtc,
            limit_price: Some(0.435),
            amount: S8aVenueAmount::Shares(5.0),
            execution_permitted: false,
        }
    }

    fn clean_s8s_fill_evidence() -> S8aOrderStatusFillEvidence {
        let prepared = clean_s8s_prepared_order();
        S8aOrderStatusFillEvidence {
            prepared_order_sha256: hex64('a'),
            exact_approval_sha256: hex64('b'),
            expected_exact_approval_sha256: hex64('b'),
            exact_approval_hash_bound_to_order: true,
            prepared_order_hash_bound_to_order: true,
            approval_scope_matches_s8a: true,
            order_id: Some("0xorder-s8s-fixture".to_string()),
            failure_recorded: false,
            status_source: S8aOrderStatusEvidenceSource::OrderStatusApi,
            order_status: S8aObservedOrderStatus::Matched,
            condition_id: prepared.condition_id,
            token_id: prepared.token_id,
            side: prepared.side,
            action: prepared.action,
            condition_token_side_action_match_prepared_order: true,
            submitted_size_shares: 5.0,
            actual_filled_qty_shares: 3.0,
            avg_fill_price: Some(0.435),
            open_order_remainder_qty_shares: 2.0,
            filled_qty_from_exchange_or_order_status: true,
            submitted_size_counts_as_inventory: false,
            partial_fill_threshold_used: false,
            forced_complement_after_fill: false,
            no_open_order_remainder_required: false,
            prints_secret_or_raw_signature: false,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
            funding_live_latest_or_deploy_requested: false,
            effectful_execution_requested_in_review: false,
        }
    }

    fn clean_s8a_orchestration_review() -> S8aRuntimeOrchestrationPreviewReview {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let steps = vec![
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: scout_snapshot_at(Side::Yes, 1_010),
                observed_fill: Some(S8aFillEvent {
                    side: Side::Yes,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.435,
                }),
                close_round_after_step: false,
                s7w_reconciliation_passed: false,
                realized_loss_delta_usdc: 0.0,
            },
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: scout_snapshot_at(Side::No, 7_010),
                observed_fill: Some(S8aFillEvent {
                    side: Side::No,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.445,
                }),
                close_round_after_step: true,
                s7w_reconciliation_passed: true,
                realized_loss_delta_usdc: 0.0,
            },
        ];
        review_s8a_runtime_orchestration_preview(
            &cfg,
            &BtcCompletionControllerState::default(),
            clean_s8a_session_state(),
            S8aInventory::default(),
            &steps,
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
        )
    }

    fn clean_s8a_wrapper_preflight_evidence() -> S8aRuntimeWrapperPreflightEvidence {
        S8aRuntimeWrapperPreflightEvidence {
            reviewed_host: S8A_REVIEWED_HOST.to_string(),
            official_rest_url: S8A_OFFICIAL_CLOB_REST_URL.to_string(),
            approval_scope: S8A_NATIVE_RUNTIME_SCOPE.to_string(),
            s8g_packet_sha256: hex64('a'),
            s8g_result_sha256: hex64('b'),
            s8e_packet_sha256: hex64('c'),
            s8e_result_sha256: hex64('d'),
            local_runtime_source_sha256: hex64('e'),
            remote_runtime_source_sha256: hex64('e'),
            local_adapter_source_sha256: hex64('f'),
            remote_adapter_source_sha256: hex64('f'),
            prepared_order_fixture_sha256: hex64('1'),
            fresh_ssh_preflight_performed: true,
            fresh_ssh_preflight_age_ms: 1_000,
            max_fresh_ssh_preflight_age_ms: 300_000,
            preview_without_approval_exit_code: 66,
            no_order_auth_preview_passed: true,
            exact_order_path_preview_passed: true,
            exact_order_path_preview_no_submit: true,
            execute_mode_requires_execute_approved: true,
            execute_without_execute_approved_exits_nonzero: true,
            no_secret_or_raw_signature_output: true,
            no_order_cancel_merge_redeem_performed: true,
            remote_b_process_table_clean: true,
            runtime_opens_ws: false,
            b_owned_direct_public_ws_connection_count: 1,
            shared_ingress_dependency: false,
            uses_c_artifacts: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            candidate_import_allowed: false,
            funding_live_latest_or_deploy_requested: false,
            effectful_execution_requested_in_review: false,
        }
    }

    fn clean_s8a_one_run_orchestration_evidence(
        orchestration: &S8aRuntimeOrchestrationPreviewReview,
    ) -> S8aOneRunOrchestrationEvidence {
        S8aOneRunOrchestrationEvidence {
            native_runtime_loop_driver_source_bound: true,
            scout_cache_to_controller_bound: true,
            controller_to_native_order_primitive_bound: true,
            runtime_accepts_only_prepared_orders: true,
            planned_order_count: orchestration.prepared_order_count,
            exact_approval_hash_bound_to_every_submit: true,
            exact_approval_scope_bound_to_every_submit: true,
            runtime_order_result_ledger_enabled: true,
            order_id_or_failure_recorded_per_attempt: true,
            filled_qty_from_exchange_or_order_status_only: true,
            actual_filled_qty_ledger_updates_inventory: true,
            submitted_size_never_counts_as_inventory: true,
            partial_fill_threshold_absent: true,
            no_forced_complement: true,
            session_caps_checked_before_each_submission: true,
            stop_new_entry_on_loss_cap_or_active_market: true,
            max_round_count: S8A_MAX_ROUNDS,
            session_hard_loss_cap_usdc: S8A_SESSION_HARD_LOSS_CAP_USDC,
            max_active_market_count: 1,
            max_total_order_submissions: 9,
            s7w_reconciliation_bound_to_run_result: true,
            exact_approved_receipt_tier_required: true,
            positive_collateral_delta_required_for_recovery: true,
            no_open_order_remainder_required: true,
            residual_exposure_zero_required: true,
            review_only_fixture_receipts_rejected: true,
            runtime_opens_ws: false,
            b_owned_direct_public_ws_connection_count: 1,
            shared_ingress_dependency: false,
            uses_c_artifacts: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            candidate_import_allowed: false,
            secret_or_raw_signature_output_allowed: false,
            funding_live_latest_or_deploy_requested: false,
            effectful_execution_requested_in_review: false,
        }
    }

    fn clean_s8a_remote_runtime_provisioning_evidence() -> S8aRemoteRuntimeProvisioningEvidence {
        S8aRemoteRuntimeProvisioningEvidence {
            one_run_orchestration_ready: true,
            remote_host: S8A_REVIEWED_HOST.to_string(),
            remote_staging_path: "/home/ubuntu/b_strategy_staging/b_strategy_canary_s8n_fixture"
                .to_string(),
            remote_staging_path_is_b_owned: true,
            remote_staging_path_is_service_dir: false,
            mutates_service_dir: false,
            runtime_source_sha256: hex64('a'),
            remote_runtime_source_sha256: hex64('a'),
            adapter_source_sha256: hex64('b'),
            remote_adapter_source_sha256: hex64('b'),
            clob_v2_source_sha256: hex64('c'),
            remote_clob_v2_source_sha256: hex64('c'),
            prepared_order_fixture_sha256: hex64('d'),
            remote_prepared_order_fixture_sha256: hex64('d'),
            one_run_plan_fixture_sha256: hex64('e'),
            remote_one_run_plan_fixture_sha256: hex64('e'),
            cargo_available_on_remote: true,
            node_available_on_remote: true,
            hash_bound_remote_worktree_ready: true,
            remote_service_repo_dirty_or_unbound: false,
            preview_without_approval_exit_code: Some(66),
            no_order_auth_preview_passed: true,
            exact_order_path_preview_passed_no_submit: true,
            one_run_driver_preview_passed_no_submit: true,
            no_secret_or_raw_signature_output: true,
            no_order_cancel_merge_redeem_performed: true,
            shared_ingress_dependency: false,
            uses_c_artifacts: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            candidate_import_allowed: false,
            funding_live_latest_or_deploy_requested: false,
            effectful_execution_requested_in_review: false,
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
    fn trade_enhanced_scout_snapshot_maps_to_controller_and_native_adapter() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();

        let review = review_s8a_scout_snapshot_for_limit_entry(
            &cfg,
            &state,
            &scout_snapshot(Side::Yes),
            &adapter_context(),
        );

        assert!(review.scout_source_bound);
        assert!(review.controller_and_adapter_bound);
        assert!(!review.effectful_execution_permitted);
        assert_eq!(
            review
                .candidate
                .as_ref()
                .map(|candidate| candidate.l1_pair_ask),
            Some(1.01)
        );
        let order = review.prepared_order.expect("prepared S8A order");
        assert_eq!(order.token_id, "token-yes");
        assert_eq!(order.order_type, S8aVenueOrderType::Gtc);
        assert_eq!(order.amount, S8aVenueAmount::Shares(5.0));
        assert_eq!(order.limit_price, Some(0.435));
    }

    #[test]
    fn scout_snapshot_blocks_shared_ingress_or_multiple_b_ws_before_controller() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let mut snapshot = scout_snapshot(Side::Yes);
        snapshot.shared_ingress_dependency = true;
        snapshot.b_owned_direct_public_ws_connection_count = 2;

        let review =
            review_s8a_scout_snapshot_for_limit_entry(&cfg, &state, &snapshot, &adapter_context());

        assert!(!review.scout_source_bound);
        assert!(review.controller_review.is_none());
        assert!(review.prepared_order.is_none());
        assert!(review
            .block_reasons
            .contains(&S8aScoutAdmissionBlockReason::SharedIngressDependency));
        assert!(review
            .block_reasons
            .contains(&S8aScoutAdmissionBlockReason::TooManyBOwnedDirectPublicWs));
    }

    #[test]
    fn scout_snapshot_uses_recv_lag_source_guard_and_fails_closed_in_controller() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let mut snapshot = scout_snapshot(Side::No);
        snapshot
            .latest_public_buy_trade
            .as_mut()
            .expect("trade")
            .recv_lag_ms = Some(501);

        let review =
            review_s8a_scout_snapshot_for_limit_entry(&cfg, &state, &snapshot, &adapter_context());

        assert!(review.scout_source_bound);
        assert!(!review.controller_and_adapter_bound);
        assert_eq!(
            review
                .controller_review
                .as_ref()
                .and_then(|controller| controller.block_reason)
                .map(|reason| reason.as_str()),
            Some("BLOCK_PUBLIC_TRADE_RECV_LAG_TOO_HIGH")
        );
        assert!(review.prepared_order.is_none());
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

    #[test]
    fn s8f_loop_binding_accepts_clean_review_only_chain_and_actual_fill_projection() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let session = clean_s8a_session_state();
        let fill = S8aFillEvent {
            side: Side::Yes,
            action: S8aAdapterOrderAction::Buy,
            submitted_size_shares: 5.0,
            actual_filled_qty_shares: 3.0,
            avg_fill_price: 0.435,
        };

        let review = review_s8a_runtime_loop_binding(
            &cfg,
            &state,
            &session,
            S8aInventory::default(),
            &scout_snapshot(Side::Yes),
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
            Some(fill),
        );

        assert!(review.loop_ready_for_fresh_exact_approval);
        assert!(review.block_reasons.is_empty());
        assert!(review.session_gate_review.can_open_new_round);
        assert!(review.scout_admission_review.controller_and_adapter_bound);
        assert!(!review.effectful_execution_permitted);
        let order = review.prepared_order.expect("prepared order");
        assert_eq!(order.side, Side::Yes);
        assert_eq!(order.amount, S8aVenueAmount::Shares(5.0));
        let projected = review
            .projected_inventory_after_fill
            .expect("projected inventory");
        assert_eq!(projected.yes_qty, 3.0);
        assert_eq!(projected.no_qty, 0.0);
        assert_eq!(projected.residual_qty(), 3.0);
        assert!((projected.gross_quote_spend_usdc - 1.305).abs() < 1e-9);
    }

    #[test]
    fn s8f_loop_binding_blocks_session_caps_before_fresh_approval() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let mut session = clean_s8a_session_state();
        session.completed_rounds = 3;
        session.active_market_open = true;

        let review = review_s8a_runtime_loop_binding(
            &cfg,
            &state,
            &session,
            S8aInventory::default(),
            &scout_snapshot(Side::Yes),
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
            None,
        );

        assert!(!review.loop_ready_for_fresh_exact_approval);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::SessionGateBlocked));
        assert!(review
            .session_gate_review
            .block_reasons
            .contains(&S8aSessionGateBlockReason::MaxRoundsReached));
        assert!(review
            .session_gate_review
            .block_reasons
            .contains(&S8aSessionGateBlockReason::ActiveMarketAlreadyOpen));
        assert!(review.prepared_order.is_some());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8f_loop_binding_rejects_semantic_drift_and_fixture_receipts() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let mut evidence = clean_s8a_loop_binding_evidence();
        evidence.actual_filled_qty_ledger_enabled = false;
        evidence.submitted_size_counts_as_inventory = true;
        evidence.partial_fill_threshold_enabled = true;
        evidence.forced_complement_allowed = true;
        evidence.s7w_exact_approved_receipt_tier_required = false;
        evidence.review_only_fixture_receipt_accepted = true;

        let review = review_s8a_runtime_loop_binding(
            &cfg,
            &state,
            &clean_s8a_session_state(),
            S8aInventory::default(),
            &scout_snapshot(Side::No),
            &adapter_context(),
            &evidence,
            None,
        );

        assert!(!review.loop_ready_for_fresh_exact_approval);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::ActualFilledQtyLedgerMissing));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::SubmittedSizeCountsAsInventory));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::PartialFillThresholdEnabled));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::ForcedComplementAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::S7wExactApprovedReceiptTierNotRequired));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::ReviewOnlyFixtureReceiptAccepted));
    }

    #[test]
    fn s8f_loop_binding_rejects_shared_sources_c_artifacts_and_effectful_review() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let mut evidence = clean_s8a_loop_binding_evidence();
        evidence.runtime_opens_own_ws = true;
        evidence.b_owned_direct_public_ws_connection_count = 2;
        evidence.shared_ingress_dependency = true;
        evidence.uses_c_artifacts = true;
        evidence.online_tuning_allowed = true;
        evidence.strategy_discovery_allowed = true;
        evidence.candidate_import_allowed = true;
        evidence.prints_secret_or_raw_signature = true;
        evidence.funding_live_latest_or_deploy_requested = true;
        evidence.effectful_execution_requested_in_review = true;

        let review = review_s8a_runtime_loop_binding(
            &cfg,
            &state,
            &clean_s8a_session_state(),
            S8aInventory::default(),
            &scout_snapshot(Side::Yes),
            &adapter_context(),
            &evidence,
            None,
        );

        assert!(!review.loop_ready_for_fresh_exact_approval);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::RuntimeOpensOwnWs));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::TooManyBOwnedDirectPublicWs));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::SharedIngressDependency));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::CArtifactsRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::OnlineTuningAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::StrategyDiscoveryAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::CandidateImportRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::SecretOrRawSignatureOutputAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::FundingLiveLatestOrDeployRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::EffectfulExecutionRequestedInReview));
    }

    #[test]
    fn s8f_loop_binding_blocks_invalid_inventory_update() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let state = BtcCompletionControllerState::default();
        let bad_fill = S8aFillEvent {
            side: Side::No,
            action: S8aAdapterOrderAction::Sell,
            submitted_size_shares: 5.0,
            actual_filled_qty_shares: 5.0,
            avg_fill_price: 0.25,
        };

        let review = review_s8a_runtime_loop_binding(
            &cfg,
            &state,
            &clean_s8a_session_state(),
            S8aInventory::default(),
            &scout_snapshot(Side::No),
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
            Some(bad_fill),
        );

        assert!(!review.loop_ready_for_fresh_exact_approval);
        assert_eq!(
            review.inventory_update_block_reason,
            Some(S8aInventoryUpdateBlockReason::SellWouldExceedInventory)
        );
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeLoopBindingBlockReason::InventoryUpdateBlocked));
    }

    #[test]
    fn s8g_orchestration_preview_allows_natural_same_condition_pairing_and_close() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let steps = vec![
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: scout_snapshot_at(Side::Yes, 1_010),
                observed_fill: Some(S8aFillEvent {
                    side: Side::Yes,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.435,
                }),
                close_round_after_step: false,
                s7w_reconciliation_passed: false,
                realized_loss_delta_usdc: 0.0,
            },
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: scout_snapshot_at(Side::No, 7_010),
                observed_fill: Some(S8aFillEvent {
                    side: Side::No,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.445,
                }),
                close_round_after_step: true,
                s7w_reconciliation_passed: true,
                realized_loss_delta_usdc: 0.0,
            },
        ];

        let review = review_s8a_runtime_orchestration_preview(
            &cfg,
            &BtcCompletionControllerState::default(),
            clean_s8a_session_state(),
            S8aInventory::default(),
            &steps,
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
        );

        assert!(review.orchestration_ready_for_fresh_exact_approval);
        assert!(review.block_reasons.is_empty());
        assert_eq!(review.prepared_order_count, 2);
        assert_eq!(review.filled_order_count, 2);
        assert_eq!(review.closed_round_count, 1);
        assert_eq!(review.final_inventory, S8aInventory::default());
        assert_eq!(review.final_session_state.completed_rounds, 1);
        assert!(!review.final_session_state.active_market_open);
        assert_eq!(review.active_condition_id, None);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8g_orchestration_preview_blocks_new_condition_while_active_market_open() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let mut second = scout_snapshot_at(Side::No, 7_010);
        second.condition_id = "0xothercondition".to_string();
        let steps = vec![
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: scout_snapshot_at(Side::Yes, 1_010),
                observed_fill: Some(S8aFillEvent {
                    side: Side::Yes,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.435,
                }),
                close_round_after_step: false,
                s7w_reconciliation_passed: false,
                realized_loss_delta_usdc: 0.0,
            },
            S8aRuntimeOrchestrationPreviewStep {
                snapshot: second,
                observed_fill: Some(S8aFillEvent {
                    side: Side::No,
                    action: S8aAdapterOrderAction::Buy,
                    submitted_size_shares: 5.0,
                    actual_filled_qty_shares: 5.0,
                    avg_fill_price: 0.445,
                }),
                close_round_after_step: true,
                s7w_reconciliation_passed: true,
                realized_loss_delta_usdc: 0.0,
            },
        ];

        let review = review_s8a_runtime_orchestration_preview(
            &cfg,
            &BtcCompletionControllerState::default(),
            clean_s8a_session_state(),
            S8aInventory::default(),
            &steps,
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
        );

        assert!(!review.orchestration_ready_for_fresh_exact_approval);
        assert_eq!(
            review.block_reasons,
            vec![S8aRuntimeOrchestrationPreviewBlockReason::ActiveMarketConditionMismatch]
        );
        assert_eq!(review.prepared_order_count, 1);
        assert_eq!(review.filled_order_count, 1);
        assert_eq!(review.active_condition_id, Some("0xcondition".to_string()));
    }

    #[test]
    fn s8g_orchestration_preview_requires_s7w_and_zero_residual_to_close_round() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let steps = vec![S8aRuntimeOrchestrationPreviewStep {
            snapshot: scout_snapshot_at(Side::Yes, 1_010),
            observed_fill: Some(S8aFillEvent {
                side: Side::Yes,
                action: S8aAdapterOrderAction::Buy,
                submitted_size_shares: 5.0,
                actual_filled_qty_shares: 3.0,
                avg_fill_price: 0.435,
            }),
            close_round_after_step: true,
            s7w_reconciliation_passed: false,
            realized_loss_delta_usdc: 0.0,
        }];

        let review = review_s8a_runtime_orchestration_preview(
            &cfg,
            &BtcCompletionControllerState::default(),
            clean_s8a_session_state(),
            S8aInventory::default(),
            &steps,
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
        );

        assert!(!review.orchestration_ready_for_fresh_exact_approval);
        assert!(review.block_reasons.contains(
            &S8aRuntimeOrchestrationPreviewBlockReason::RoundCloseWithoutS7wReconciliation
        ));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeOrchestrationPreviewBlockReason::RoundCloseWithResidualExposure));
        assert_eq!(review.final_inventory.yes_qty, 3.0);
        assert_eq!(review.final_inventory.residual_qty(), 3.0);
    }

    #[test]
    fn s8h_wrapper_preflight_accepts_clean_review_only_shape() {
        let orchestration = clean_s8a_orchestration_review();
        let evidence = clean_s8a_wrapper_preflight_evidence();

        let review = review_s8a_runtime_wrapper_preflight(&orchestration, &evidence);

        assert!(review.wrapper_preflight_ready_for_fresh_exact_approval_request);
        assert!(review.block_reasons.is_empty());
        assert!(review.orchestration_preview_ready);
        assert!(review.source_hashes_bound);
        assert!(review.preview_gates_bound);
        assert!(review.remote_preflight_bound);
        assert!(review.forbidden_paths_absent);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8h_wrapper_preflight_rejects_blocked_orchestration() {
        let cfg = BtcCompletionControllerConfig::s8a_size5_runtime_default();
        let blocked_orchestration = review_s8a_runtime_orchestration_preview(
            &cfg,
            &BtcCompletionControllerState::default(),
            clean_s8a_session_state(),
            S8aInventory::default(),
            &[],
            &adapter_context(),
            &clean_s8a_loop_binding_evidence(),
        );

        let review = review_s8a_runtime_wrapper_preflight(
            &blocked_orchestration,
            &clean_s8a_wrapper_preflight_evidence(),
        );

        assert!(!review.wrapper_preflight_ready_for_fresh_exact_approval_request);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::OrchestrationPreviewNotReady));
        assert!(!review.orchestration_preview_ready);
    }

    #[test]
    fn s8h_wrapper_preflight_rejects_hash_preview_and_remote_drift() {
        let orchestration = clean_s8a_orchestration_review();
        let mut evidence = clean_s8a_wrapper_preflight_evidence();
        evidence.reviewed_host = "ubuntu@example.invalid".to_string();
        evidence.official_rest_url = "https://example.invalid".to_string();
        evidence.approval_scope = "WRONG_SCOPE".to_string();
        evidence.s8g_packet_sha256 = "not-a-hash".to_string();
        evidence.remote_runtime_source_sha256 = hex64('2');
        evidence.remote_adapter_source_sha256 = hex64('3');
        evidence.prepared_order_fixture_sha256 = "short".to_string();
        evidence.fresh_ssh_preflight_performed = false;
        evidence.fresh_ssh_preflight_age_ms = 600_001;
        evidence.preview_without_approval_exit_code = 0;
        evidence.no_order_auth_preview_passed = false;
        evidence.exact_order_path_preview_passed = false;
        evidence.exact_order_path_preview_no_submit = false;
        evidence.execute_mode_requires_execute_approved = false;
        evidence.execute_without_execute_approved_exits_nonzero = false;
        evidence.remote_b_process_table_clean = false;
        evidence.runtime_opens_ws = true;
        evidence.b_owned_direct_public_ws_connection_count = 2;

        let review = review_s8a_runtime_wrapper_preflight(&orchestration, &evidence);

        assert!(!review.wrapper_preflight_ready_for_fresh_exact_approval_request);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::ReviewedHostMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::OfficialRestUrlMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::ApprovalScopeMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::InvalidS8gPacketHash));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::RuntimeSourceHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::AdapterSourceHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::InvalidPreparedOrderFixtureHash));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::FreshSshPreflightMissing));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::FreshSshPreflightStale));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::PreviewWithoutApprovalExitNot66));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::NoOrderAuthPreviewNotPassed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::ExactOrderPathPreviewNotPassed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::ExactOrderPathPreviewAllowsSubmit));
        assert!(review.block_reasons.contains(
            &S8aRuntimeWrapperPreflightBlockReason::ExecuteModeDoesNotRequireExecuteApproved
        ));
        assert!(review.block_reasons.contains(
            &S8aRuntimeWrapperPreflightBlockReason::ExecuteWithoutExecuteApprovedDidNotFailClosed
        ));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::RemoteBProcessTableNotClean));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::RuntimeOpensWs));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::TooManyBOwnedDirectPublicWs));
    }

    #[test]
    fn s8h_wrapper_preflight_rejects_forbidden_paths_in_review() {
        let orchestration = clean_s8a_orchestration_review();
        let mut evidence = clean_s8a_wrapper_preflight_evidence();
        evidence.no_secret_or_raw_signature_output = false;
        evidence.no_order_cancel_merge_redeem_performed = false;
        evidence.shared_ingress_dependency = true;
        evidence.uses_c_artifacts = true;
        evidence.online_tuning_allowed = true;
        evidence.strategy_discovery_allowed = true;
        evidence.candidate_import_allowed = true;
        evidence.funding_live_latest_or_deploy_requested = true;
        evidence.effectful_execution_requested_in_review = true;

        let review = review_s8a_runtime_wrapper_preflight(&orchestration, &evidence);

        assert!(!review.wrapper_preflight_ready_for_fresh_exact_approval_request);
        assert!(!review.forbidden_paths_absent);
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::SecretOrRawSignatureOutputAllowed));
        assert!(review.block_reasons.contains(
            &S8aRuntimeWrapperPreflightBlockReason::OrderCancelMergeRedeemPerformedInReview
        ));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::SharedIngressDependency));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::CArtifactsRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::OnlineTuningAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::StrategyDiscoveryAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::CandidateImportRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::FundingLiveLatestOrDeployRequested));
        assert!(review
            .block_reasons
            .contains(&S8aRuntimeWrapperPreflightBlockReason::EffectfulExecutionRequestedInReview));
    }

    #[test]
    fn s8k_one_run_orchestration_accepts_clean_end_to_end_contract() {
        let orchestration = clean_s8a_orchestration_review();
        let wrapper_preflight = review_s8a_runtime_wrapper_preflight(
            &orchestration,
            &clean_s8a_wrapper_preflight_evidence(),
        );
        let evidence = clean_s8a_one_run_orchestration_evidence(&orchestration);

        let review =
            review_s8a_one_run_orchestration(&orchestration, &wrapper_preflight, &evidence);

        assert!(review.one_run_orchestration_ready_for_fresh_exact_approval);
        assert!(review.block_reasons.is_empty());
        assert!(review.wrapper_preflight_ready);
        assert!(review.orchestration_preview_ready);
        assert!(review.source_loop_bound);
        assert!(review.order_submit_contract_bound);
        assert!(review.filled_qty_ledger_bound);
        assert!(review.session_caps_bound);
        assert!(review.s7w_run_result_bound);
        assert_eq!(review.planned_order_count, 2);
        assert_eq!(review.closed_round_count, 1);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8k_one_run_orchestration_rejects_unready_preflight_and_count_drift() {
        let orchestration = clean_s8a_orchestration_review();
        let mut preflight_evidence = clean_s8a_wrapper_preflight_evidence();
        preflight_evidence.exact_order_path_preview_passed = false;
        let wrapper_preflight =
            review_s8a_runtime_wrapper_preflight(&orchestration, &preflight_evidence);
        let mut evidence = clean_s8a_one_run_orchestration_evidence(&orchestration);
        evidence.planned_order_count = orchestration.prepared_order_count + 1;

        let review =
            review_s8a_one_run_orchestration(&orchestration, &wrapper_preflight, &evidence);

        assert!(!review.one_run_orchestration_ready_for_fresh_exact_approval);
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::WrapperPreflightNotReady));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::PlannedOrderCountMismatch));
        assert!(!review.wrapper_preflight_ready);
    }

    #[test]
    fn s8k_one_run_orchestration_rejects_inventory_and_s7w_drift() {
        let orchestration = clean_s8a_orchestration_review();
        let wrapper_preflight = review_s8a_runtime_wrapper_preflight(
            &orchestration,
            &clean_s8a_wrapper_preflight_evidence(),
        );
        let mut evidence = clean_s8a_one_run_orchestration_evidence(&orchestration);
        evidence.filled_qty_from_exchange_or_order_status_only = false;
        evidence.submitted_size_never_counts_as_inventory = false;
        evidence.partial_fill_threshold_absent = false;
        evidence.no_forced_complement = false;
        evidence.s7w_reconciliation_bound_to_run_result = false;
        evidence.exact_approved_receipt_tier_required = false;
        evidence.review_only_fixture_receipts_rejected = false;

        let review =
            review_s8a_one_run_orchestration(&orchestration, &wrapper_preflight, &evidence);

        assert!(!review.one_run_orchestration_ready_for_fresh_exact_approval);
        assert!(!review.filled_qty_ledger_bound);
        assert!(!review.s7w_run_result_bound);
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::FilledQtyNotExchangeObserved));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::SubmittedSizeCountsAsInventory));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::PartialFillThresholdEnabled));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::ForcedComplementAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::S7wReconciliationNotBound));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::ExactApprovedReceiptTierNotRequired));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::ReviewOnlyFixtureReceiptAccepted));
    }

    #[test]
    fn s8k_one_run_orchestration_rejects_forbidden_runtime_paths() {
        let orchestration = clean_s8a_orchestration_review();
        let wrapper_preflight = review_s8a_runtime_wrapper_preflight(
            &orchestration,
            &clean_s8a_wrapper_preflight_evidence(),
        );
        let mut evidence = clean_s8a_one_run_orchestration_evidence(&orchestration);
        evidence.runtime_opens_ws = true;
        evidence.b_owned_direct_public_ws_connection_count = 2;
        evidence.shared_ingress_dependency = true;
        evidence.uses_c_artifacts = true;
        evidence.online_tuning_allowed = true;
        evidence.strategy_discovery_allowed = true;
        evidence.candidate_import_allowed = true;
        evidence.secret_or_raw_signature_output_allowed = true;
        evidence.funding_live_latest_or_deploy_requested = true;
        evidence.effectful_execution_requested_in_review = true;

        let review =
            review_s8a_one_run_orchestration(&orchestration, &wrapper_preflight, &evidence);

        assert!(!review.one_run_orchestration_ready_for_fresh_exact_approval);
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::RuntimeOpensWs));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::TooManyBOwnedDirectPublicWs));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::SharedIngressDependency));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::CArtifactsRequested));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::OnlineTuningAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::StrategyDiscoveryAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::CandidateImportRequested));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::SecretOrRawSignatureOutputAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::FundingLiveLatestOrDeployRequested));
        assert!(review
            .block_reasons
            .contains(&S8aOneRunOrchestrationBlockReason::EffectfulExecutionRequestedInReview));
    }

    #[test]
    fn s8s_order_status_fill_evidence_accepts_partial_fill_from_exchange_status_only() {
        let prepared = clean_s8s_prepared_order();
        let evidence = clean_s8s_fill_evidence();

        let review = review_s8a_order_status_fill_evidence(&prepared, &evidence);

        assert!(review.block_reasons.is_empty());
        assert!(review.actual_filled_qty_ledger_ready);
        assert!(review.order_id_or_failure_recorded);
        assert!(review.exact_approval_bound);
        assert!(review.prepared_order_bound);
        assert!(review.filled_qty_from_exchange_or_order_status_only);
        assert!(!review.no_open_order_remainder);
        assert!(!review.effectful_execution_permitted);

        let inventory = apply_s8a_fill_to_inventory(
            S8aInventory::default(),
            review
                .fill_event
                .expect("clean evidence creates fill event"),
        )
        .expect("partial fill applies to inventory");
        assert_eq!(inventory.yes_qty, 3.0);
        assert_eq!(inventory.no_qty, 0.0);
        assert!((inventory.gross_quote_spend_usdc - 1.305).abs() < 1e-9);
        assert_eq!(inventory.residual_qty(), 3.0);
    }

    #[test]
    fn s8s_order_status_fill_evidence_rejects_submitted_size_inventory_drift() {
        let prepared = clean_s8s_prepared_order();
        let mut evidence = clean_s8s_fill_evidence();
        evidence.filled_qty_from_exchange_or_order_status = false;
        evidence.submitted_size_counts_as_inventory = true;
        evidence.partial_fill_threshold_used = true;
        evidence.forced_complement_after_fill = true;
        evidence.actual_filled_qty_shares = 6.0;
        evidence.prints_secret_or_raw_signature = true;
        evidence.uses_shared_ingress_or_shared_ws = true;
        evidence.uses_c_artifacts = true;
        evidence.effectful_execution_requested_in_review = true;

        let review = review_s8a_order_status_fill_evidence(&prepared, &evidence);

        assert!(!review.actual_filled_qty_ledger_ready);
        assert!(review.fill_event.is_none());
        assert!(review.block_reasons.contains(
            &S8aOrderStatusFillEvidenceBlockReason::FilledQtyNotExchangeOrOrderStatusObserved
        ));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::SubmittedSizeCountsAsInventory));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::PartialFillThresholdUsed));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::ForcedComplementAfterFill));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::FilledQtyExceedsSubmittedSize));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::SecretOrRawSignatureOutputAllowed));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::SharedIngressOrSharedWsRequested));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::CArtifactsRequested));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::EffectfulExecutionRequestedInReview));
    }

    #[test]
    fn s8s_order_status_fill_evidence_rejects_unbound_or_unclosed_attempts() {
        let prepared = clean_s8s_prepared_order();
        let mut evidence = clean_s8s_fill_evidence();
        evidence.prepared_order_sha256 = "bad-hash".to_string();
        evidence.exact_approval_sha256 = hex64('c');
        evidence.exact_approval_hash_bound_to_order = false;
        evidence.prepared_order_hash_bound_to_order = false;
        evidence.approval_scope_matches_s8a = false;
        evidence.order_id = None;
        evidence.failure_recorded = false;
        evidence.condition_token_side_action_match_prepared_order = false;
        evidence.token_id = "different-token".to_string();
        evidence.avg_fill_price = None;
        evidence.no_open_order_remainder_required = true;

        let review = review_s8a_order_status_fill_evidence(&prepared, &evidence);

        assert!(!review.actual_filled_qty_ledger_ready);
        assert!(!review.order_id_or_failure_recorded);
        assert!(!review.exact_approval_bound);
        assert!(!review.prepared_order_bound);
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::InvalidPreparedOrderHash));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::ExactApprovalHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::ExactApprovalHashNotBoundToOrder));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::PreparedOrderHashNotBoundToOrder));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::ApprovalScopeMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::MissingOrderIdOrFailureRecord));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::PreparedOrderBindingMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::TokenMismatch));
        assert!(review.block_reasons.contains(
            &S8aOrderStatusFillEvidenceBlockReason::MissingAverageFillPriceForPositiveFill
        ));
        assert!(review
            .block_reasons
            .contains(&S8aOrderStatusFillEvidenceBlockReason::OpenOrderRemainderPresent));
    }

    #[test]
    fn s8n_remote_runtime_provisioning_accepts_hash_bound_preview_ready_staging() {
        let evidence = clean_s8a_remote_runtime_provisioning_evidence();

        let review = review_s8a_remote_runtime_provisioning(&evidence);

        assert!(review.ready_for_fresh_exact_approval_request);
        assert!(review.block_reasons.is_empty());
        assert!(review.remote_hashes_bound);
        assert!(review.remote_runtime_available);
        assert!(review.remote_preview_gates_passed);
        assert!(review.remote_staging_only);
        assert!(review.forbidden_paths_absent);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8n_remote_runtime_provisioning_blocks_dirty_old_repo_and_missing_runtime() {
        let mut evidence = clean_s8a_remote_runtime_provisioning_evidence();
        evidence.cargo_available_on_remote = false;
        evidence.node_available_on_remote = false;
        evidence.hash_bound_remote_worktree_ready = false;
        evidence.remote_service_repo_dirty_or_unbound = true;
        evidence.preview_without_approval_exit_code = None;
        evidence.no_order_auth_preview_passed = false;
        evidence.exact_order_path_preview_passed_no_submit = false;
        evidence.one_run_driver_preview_passed_no_submit = false;

        let review = review_s8a_remote_runtime_provisioning(&evidence);

        assert!(!review.ready_for_fresh_exact_approval_request);
        assert!(review.remote_hashes_bound);
        assert!(review.remote_staging_only);
        assert!(review.forbidden_paths_absent);
        assert!(!review.remote_runtime_available);
        assert!(!review.remote_preview_gates_passed);
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RemoteCargoUnavailable));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RemoteNodeUnavailable));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::HashBoundRemoteWorktreeNotReady));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RemoteServiceRepoDirtyOrUnbound));
        assert!(review.block_reasons.contains(
            &S8aRemoteRuntimeProvisioningBlockReason::PreviewWithoutApprovalNotRunOrNot66
        ));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::NoOrderAuthPreviewNotPassed));
        assert!(review.block_reasons.contains(
            &S8aRemoteRuntimeProvisioningBlockReason::ExactOrderPathPreviewNotPassedNoSubmit
        ));
        assert!(review.block_reasons.contains(
            &S8aRemoteRuntimeProvisioningBlockReason::OneRunDriverPreviewNotPassedNoSubmit
        ));
    }

    #[test]
    fn s8n_remote_runtime_provisioning_rejects_service_mutation_and_hash_drift() {
        let mut evidence = clean_s8a_remote_runtime_provisioning_evidence();
        evidence.remote_staging_path_is_b_owned = false;
        evidence.remote_staging_path_is_service_dir = true;
        evidence.mutates_service_dir = true;
        evidence.remote_runtime_source_sha256 = hex64('0');
        evidence.remote_adapter_source_sha256 = "bad-hash".to_string();
        evidence.remote_clob_v2_source_sha256 = hex64('1');
        evidence.remote_prepared_order_fixture_sha256 = hex64('2');
        evidence.remote_one_run_plan_fixture_sha256 = hex64('3');

        let review = review_s8a_remote_runtime_provisioning(&evidence);

        assert!(!review.ready_for_fresh_exact_approval_request);
        assert!(!review.remote_hashes_bound);
        assert!(!review.remote_staging_only);
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RemoteStagingPathNotBOwned));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RemoteStagingPathIsServiceDir));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::MutatesServiceDir));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::RuntimeSourceHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::InvalidAdapterSourceHash));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::AdapterSourceHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::ClobV2SourceHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::PreparedOrderFixtureHashMismatch));
        assert!(review
            .block_reasons
            .contains(&S8aRemoteRuntimeProvisioningBlockReason::OneRunPlanFixtureHashMismatch));
    }
}
