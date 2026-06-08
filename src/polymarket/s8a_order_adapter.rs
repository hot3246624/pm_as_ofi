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
}
