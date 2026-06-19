//! Review-only BTC completion controller extracted from S7D/S7E research.
//!
//! This module is pure and disabled by default. It has no order, cancel, merge,
//! redeem, signing, funding, network, or secret-reading path. Its only job is to
//! make the S7E no-leakage candidate admission contract testable.

use std::collections::BTreeMap;

use crate::polymarket::types::Side;
use serde_json::{json, Value};

pub const BTC_COMPLETION_CONTROLLER_ENABLED_DEFAULT: bool = false;
pub const BTC_COMPLETION_CONTROLLER_REVIEW_EVENT: &str = "btc_completion_controller_review";
pub const BTC_COMPLETION_STRATEGY_CANARY_GATE_REVIEW_EVENT: &str =
    "btc_completion_strategy_canary_gate_review";
pub const BTC_COMPLETION_STRATEGY_CANARY_PROOF_REVIEW_EVENT: &str =
    "btc_completion_strategy_canary_proof_review";
pub const BTC_COMPLETION_RECOVERY_PRIMITIVE_WRAPPER_BOUNDARY_REVIEW_EVENT: &str =
    "btc_completion_recovery_primitive_wrapper_boundary_review";
pub const BTC_COMPLETION_RECOVERY_PRIMITIVE_PREBUILD_REVIEW_EVENT: &str =
    "btc_completion_recovery_primitive_prebuild_review";
pub const BTC_COMPLETION_RECOVERY_PRIMITIVE_PREFLIGHT_REVIEW_EVENT: &str =
    "btc_completion_recovery_primitive_preflight_review";
pub const BTC_COMPLETION_RECOVERY_PRIMITIVE_EXECUTION_PLAN_REVIEW_EVENT: &str =
    "btc_completion_recovery_primitive_execution_plan_review";
pub const BTC_COMPLETION_RECOVERY_PRIMITIVE_RUN_RESULT_REVIEW_EVENT: &str =
    "btc_completion_recovery_primitive_run_result_review";
pub const BTC_COMPLETION_S8_MICRO_LONG_CYCLE_VALIDATION_REVIEW_EVENT: &str =
    "btc_completion_s8_micro_long_cycle_validation_review";
pub const BTC_COMPLETION_S8A_EXECUTION_WRAPPER_PREBUILD_REVIEW_EVENT: &str =
    "btc_completion_s8a_execution_wrapper_prebuild_review";
pub const BTC_COMPLETION_S8A_EFFECTFUL_RUNTIME_READINESS_REVIEW_EVENT: &str =
    "btc_completion_s8a_effectful_runtime_readiness_review";
pub const BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT: bool = false;
pub const BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256: &str =
    "a602729f6697c4ccc82a6cf7b3f1cefd049f8fda9b6884da56d5bb5a4e399fdf";
pub const BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256: &str =
    "1545a9268e6fa6bacb164ebac5c2878193863fb59309b3a38eeab1534847e00d";
pub const BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256: &str =
    "1fb20485b5e37931c7d8f087464edc9fb463aed3c41dcde90014ad5abf63852c";
pub const BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256: &str =
    "b2490770fe7cfaea6671f660eb4349a78ab43695b97811a8c1b8bf393662dea2";
pub const BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES: f64 = 5.0;
pub const BTC_COMPLETION_S8A_MARKET_BUY_MIN_NOTIONAL_USDC: f64 = 1.0;
pub const BTC_COMPLETION_S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES: f64 = 5.0;
pub const BTC_COMPLETION_EX_ANTE_POLICY_PAYLOAD_SCHEMA_VERSION: u32 = 1;
pub const BTC_COMPLETION_EX_ANTE_POLICY_PAYLOAD_SCHEMA_KEYS: [&str; 28] = [
    "allows_same_run_flatten_as_primary_exit",
    "block_reasons",
    "controller_policy_sha256",
    "effectful_execution_permitted",
    "emergency_flatten_is_canary_risk_control_only",
    "emergency_flatten_is_strategy_pnl_path",
    "event",
    "future_exact_canary_policy_ready",
    "hash_bound_policy_ready",
    "max_bad_pair_cost_ge_1_share",
    "max_capital_lock_ms",
    "max_capital_lock_usdc",
    "max_one_leg_exposure_usdc",
    "max_residual_cost_rate",
    "policy_schema_version",
    "recovery_path",
    "recovery_policy_sha256",
    "replay_result_sha256",
    "requires_external_position_reconciliation",
    "requires_no_open_order_remainder",
    "requires_recovery_receipt_factory",
    "requires_same_market_yes_no_close_window",
    "requires_source_guard_500",
    "scope_asset",
    "strategy_lane",
    "strategy_research_only",
    "uses_c_artifacts",
    "uses_shared_ingress_or_shared_ws",
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BtcCompletionControllerConfig {
    pub edge: f64,
    pub seed_qty: f64,
    pub cooldown_s: f64,
    pub imbalance_qty_cap: f64,
    pub offset_max_s: f64,
    pub time_to_end_min_s: f64,
    pub l1_pair_ask_max: f64,
    pub seed_px_lo: f64,
    pub seed_px_hi: f64,
    pub max_strict_l1_age_ms: u64,
    pub max_public_trade_recv_lag_ms: u64,
}

impl BtcCompletionControllerConfig {
    pub fn s7e_research_default() -> Self {
        Self {
            edge: 0.055,
            seed_qty: 1.25,
            cooldown_s: 5.0,
            imbalance_qty_cap: 1.25,
            offset_max_s: 120.0,
            time_to_end_min_s: 180.0,
            l1_pair_ask_max: 1.02,
            seed_px_lo: 0.05,
            seed_px_hi: 0.80,
            max_strict_l1_age_ms: 500,
            max_public_trade_recv_lag_ms: 500,
        }
    }

    pub fn s8a_size5_runtime_default() -> Self {
        let mut cfg = Self::s7e_research_default();
        cfg.seed_qty = 5.0;
        cfg.imbalance_qty_cap = 5.0;
        cfg
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionCandidate {
    pub asset: String,
    pub event_kind: String,
    pub public_trade_taker_side: String,
    pub condition_id: String,
    pub slug: String,
    pub ts_ms: u64,
    pub offset_s: f64,
    pub time_to_end_s: Option<f64>,
    pub side: Side,
    pub public_trade_price: f64,
    pub l1_pair_ask: f64,
    pub l1_pair_available_qty: f64,
    pub buy_available_qty: f64,
    pub strict_l1_age_ms: Option<u64>,
    pub strict_l2_age_ms: Option<u64>,
    pub public_trade_recv_lag_ms: Option<u64>,
    pub forbidden_decision_fields_present: bool,
    pub snapshot_index_used_as_rank: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BtcCompletionConditionState {
    pub yes_qty: f64,
    pub no_qty: f64,
    pub last_accept_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BtcCompletionControllerState {
    pub by_condition: BTreeMap<String, BtcCompletionConditionState>,
}

impl BtcCompletionControllerState {
    pub fn condition_state(&self, condition_id: &str) -> BtcCompletionConditionState {
        self.by_condition
            .get(condition_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn set_condition_state(
        &mut self,
        condition_id: impl Into<String>,
        state: BtcCompletionConditionState,
    ) {
        self.by_condition.insert(condition_id.into(), state);
    }

    pub fn record_accepted(&mut self, intent: &BtcCompletionResearchIntent) {
        let state = self
            .by_condition
            .entry(intent.condition_id.clone())
            .or_default();
        match intent.side {
            Side::Yes => state.yes_qty += intent.qty,
            Side::No => state.no_qty += intent.qty,
        }
        state.last_accept_ts_ms = Some(intent.ts_ms);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionControllerBlockReason {
    NonBtcAsset,
    NonPublicTradeEvent,
    NonBuyPublicTrade,
    ForbiddenLeakageFieldPresent,
    SnapshotIndexRankUsed,
    OffsetTooLate,
    TimeToEndTooShort,
    InvalidSeedPrice,
    PairAskAboveCap,
    InsufficientLiquidity,
    MissingStrictL1Age,
    StrictL1AgeTooStale,
    MissingPublicTradeRecvLag,
    PublicTradeRecvLagTooHigh,
    CooldownActive,
    InventoryImbalanceWouldExceedCap,
}

impl BtcCompletionControllerBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonBtcAsset => "BLOCK_NON_BTC_ASSET",
            Self::NonPublicTradeEvent => "BLOCK_NON_PUBLIC_TRADE_EVENT",
            Self::NonBuyPublicTrade => "BLOCK_NON_BUY_PUBLIC_TRADE",
            Self::ForbiddenLeakageFieldPresent => "BLOCK_FORBIDDEN_LEAKAGE_FIELD_PRESENT",
            Self::SnapshotIndexRankUsed => "BLOCK_SNAPSHOT_INDEX_RANK_USED",
            Self::OffsetTooLate => "BLOCK_OFFSET_TOO_LATE",
            Self::TimeToEndTooShort => "BLOCK_TIME_TO_END_TOO_SHORT",
            Self::InvalidSeedPrice => "BLOCK_INVALID_SEED_PRICE",
            Self::PairAskAboveCap => "BLOCK_PAIR_ASK_ABOVE_CAP",
            Self::InsufficientLiquidity => "BLOCK_INSUFFICIENT_LIQUIDITY",
            Self::MissingStrictL1Age => "BLOCK_MISSING_STRICT_L1_AGE",
            Self::StrictL1AgeTooStale => "BLOCK_STRICT_L1_AGE_TOO_STALE",
            Self::MissingPublicTradeRecvLag => "BLOCK_MISSING_PUBLIC_TRADE_RECV_LAG",
            Self::PublicTradeRecvLagTooHigh => "BLOCK_PUBLIC_TRADE_RECV_LAG_TOO_HIGH",
            Self::CooldownActive => "BLOCK_COOLDOWN_ACTIVE",
            Self::InventoryImbalanceWouldExceedCap => "BLOCK_INVENTORY_IMBALANCE_WOULD_EXCEED_CAP",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionResearchIntent {
    pub condition_id: String,
    pub slug: String,
    pub ts_ms: u64,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionControllerReview {
    pub intent: Option<BtcCompletionResearchIntent>,
    pub block_reason: Option<BtcCompletionControllerBlockReason>,
    pub execution_permitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPath {
    CompleteSetMerge,
    PostResolutionRedeem,
}

impl BtcCompletionRecoveryPath {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CompleteSetMerge => "COMPLETE_SET_MERGE",
            Self::PostResolutionRedeem => "POST_RESOLUTION_REDEEM",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionStrategyCanaryGatePolicy {
    pub scope_asset: String,
    pub controller_policy_sha256: String,
    pub replay_result_sha256: String,
    pub recovery_policy_sha256: Option<String>,
    pub recovery_path: Option<BtcCompletionRecoveryPath>,
    pub max_capital_lock_usdc: Option<f64>,
    pub max_capital_lock_ms: Option<u64>,
    pub max_residual_cost_rate: Option<f64>,
    pub max_bad_pair_cost_ge_1_share: Option<f64>,
    pub max_one_leg_exposure_usdc: Option<f64>,
    pub requires_source_guard_500: bool,
    pub requires_same_market_yes_no_close_window: bool,
    pub requires_external_position_reconciliation: bool,
    pub requires_recovery_receipt_factory: bool,
    pub requires_no_open_order_remainder: bool,
    pub allows_same_run_flatten_as_primary_exit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionStrategyCanaryGateBlockReason {
    NonBtcScope,
    MissingControllerPolicyHash,
    MissingReplayResultHash,
    MissingRecoveryPolicyHash,
    MissingRecoveryPath,
    MissingOrInvalidCapitalLockLimit,
    MissingOrInvalidCapitalLockDuration,
    MissingOrInvalidResidualCostPolicy,
    MissingOrInvalidBadPairCostPolicy,
    MissingOrInvalidOneLegExposureLimit,
    MissingSourceGuard500,
    MissingSameMarketCloseWindowGuard,
    MissingExternalPositionReconciliation,
    MissingRecoveryReceiptFactory,
    MissingNoOpenOrderRemainderGuard,
    SameRunFlattenPrimaryExit,
}

impl BtcCompletionStrategyCanaryGateBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonBtcScope => "BLOCK_NON_BTC_SCOPE",
            Self::MissingControllerPolicyHash => "BLOCK_MISSING_CONTROLLER_POLICY_HASH",
            Self::MissingReplayResultHash => "BLOCK_MISSING_REPLAY_RESULT_HASH",
            Self::MissingRecoveryPolicyHash => "BLOCK_MISSING_RECOVERY_POLICY_HASH",
            Self::MissingRecoveryPath => "BLOCK_MISSING_RECOVERY_PATH",
            Self::MissingOrInvalidCapitalLockLimit => "BLOCK_MISSING_OR_INVALID_CAPITAL_LOCK_LIMIT",
            Self::MissingOrInvalidCapitalLockDuration => {
                "BLOCK_MISSING_OR_INVALID_CAPITAL_LOCK_DURATION"
            }
            Self::MissingOrInvalidResidualCostPolicy => {
                "BLOCK_MISSING_OR_INVALID_RESIDUAL_COST_POLICY"
            }
            Self::MissingOrInvalidBadPairCostPolicy => {
                "BLOCK_MISSING_OR_INVALID_BAD_PAIR_COST_POLICY"
            }
            Self::MissingOrInvalidOneLegExposureLimit => {
                "BLOCK_MISSING_OR_INVALID_ONE_LEG_EXPOSURE_LIMIT"
            }
            Self::MissingSourceGuard500 => "BLOCK_MISSING_SOURCE_GUARD_500",
            Self::MissingSameMarketCloseWindowGuard => {
                "BLOCK_MISSING_SAME_MARKET_CLOSE_WINDOW_GUARD"
            }
            Self::MissingExternalPositionReconciliation => {
                "BLOCK_MISSING_EXTERNAL_POSITION_RECONCILIATION"
            }
            Self::MissingRecoveryReceiptFactory => "BLOCK_MISSING_RECOVERY_RECEIPT_FACTORY",
            Self::MissingNoOpenOrderRemainderGuard => "BLOCK_MISSING_NO_OPEN_ORDER_REMAINDER_GUARD",
            Self::SameRunFlattenPrimaryExit => "BLOCK_SAME_RUN_FLATTEN_PRIMARY_EXIT",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionStrategyCanaryGateReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionStrategyCanaryGateBlockReason>,
    pub hash_bound_policy_ready: bool,
    pub future_exact_canary_policy_ready: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionStrategyCanaryProofState {
    pub ex_ante_policy_payload_sha256: String,
    pub external_position_reconciliation_sha256: Option<String>,
    pub external_position_proof_tier: BtcCompletionProofTier,
    pub recovery_receipt_fixture_sha256: Option<String>,
    pub recovery_receipt_proof_tier: BtcCompletionProofTier,
    pub capital_lock_model_sha256: Option<String>,
    pub capital_lock_model_tier: BtcCompletionProofTier,
    pub inventory_residual_policy_sha256: Option<String>,
    pub inventory_residual_policy_tier: BtcCompletionProofTier,
    pub no_open_order_remainder_proof_sha256: Option<String>,
    pub no_open_order_remainder_proof_tier: BtcCompletionProofTier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionProofTier {
    Unavailable,
    ReviewOnlyFixture,
    PublicReadOnly,
    ExactApprovedPrimitiveReceipt,
}

impl BtcCompletionProofTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unavailable => "UNAVAILABLE",
            Self::ReviewOnlyFixture => "REVIEW_ONLY_FIXTURE",
            Self::PublicReadOnly => "PUBLIC_READ_ONLY",
            Self::ExactApprovedPrimitiveReceipt => "EXACT_APPROVED_PRIMITIVE_RECEIPT",
        }
    }

    fn supports_approval_request(&self) -> bool {
        *self != Self::Unavailable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionStrategyCanaryProofBlockReason {
    MissingOrInvalidExAntePolicyPayloadHash,
    MissingOrInvalidExternalPositionReconciliationProof,
    MissingExternalPositionProofTier,
    MissingOrInvalidRecoveryReceiptProof,
    MissingRecoveryReceiptProofTier,
    MissingOrInvalidCapitalLockModelProof,
    MissingCapitalLockModelProofTier,
    MissingOrInvalidInventoryResidualPolicyProof,
    MissingInventoryResidualPolicyProofTier,
    MissingOrInvalidNoOpenOrderRemainderProof,
    MissingNoOpenOrderRemainderProofTier,
}

impl BtcCompletionStrategyCanaryProofBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingOrInvalidExAntePolicyPayloadHash => {
                "BLOCK_MISSING_OR_INVALID_EX_ANTE_POLICY_PAYLOAD_HASH"
            }
            Self::MissingOrInvalidExternalPositionReconciliationProof => {
                "BLOCK_MISSING_OR_INVALID_EXTERNAL_POSITION_RECONCILIATION_PROOF"
            }
            Self::MissingExternalPositionProofTier => "BLOCK_MISSING_EXTERNAL_POSITION_PROOF_TIER",
            Self::MissingOrInvalidRecoveryReceiptProof => {
                "BLOCK_MISSING_OR_INVALID_RECOVERY_RECEIPT_PROOF"
            }
            Self::MissingRecoveryReceiptProofTier => "BLOCK_MISSING_RECOVERY_RECEIPT_PROOF_TIER",
            Self::MissingOrInvalidCapitalLockModelProof => {
                "BLOCK_MISSING_OR_INVALID_CAPITAL_LOCK_MODEL_PROOF"
            }
            Self::MissingCapitalLockModelProofTier => "BLOCK_MISSING_CAPITAL_LOCK_MODEL_PROOF_TIER",
            Self::MissingOrInvalidInventoryResidualPolicyProof => {
                "BLOCK_MISSING_OR_INVALID_INVENTORY_RESIDUAL_POLICY_PROOF"
            }
            Self::MissingInventoryResidualPolicyProofTier => {
                "BLOCK_MISSING_INVENTORY_RESIDUAL_POLICY_PROOF_TIER"
            }
            Self::MissingOrInvalidNoOpenOrderRemainderProof => {
                "BLOCK_MISSING_OR_INVALID_NO_OPEN_ORDER_REMAINDER_PROOF"
            }
            Self::MissingNoOpenOrderRemainderProofTier => {
                "BLOCK_MISSING_NO_OPEN_ORDER_REMAINDER_PROOF_TIER"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionStrategyCanaryProofReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionStrategyCanaryProofBlockReason>,
    pub proof_bundle_ready_to_request_exact_approval: bool,
    pub exact_approved_recovery_proof_present: bool,
    pub execution_evidence_complete: bool,
    pub fresh_exact_approval_still_required: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveWrapperBoundary {
    pub approval_draft_sha256: String,
    pub packet_sha256: String,
    pub proof_tier_state_sha256: String,
    pub selected_recovery_path: Option<BtcCompletionRecoveryPath>,
    pub max_condition_count: u32,
    pub max_recovery_tx_count: u32,
    pub max_order_count: u32,
    pub max_cancel_count: u32,
    pub preview_without_approval_exit_code: Option<i32>,
    pub no_secret_output_guard: bool,
    pub no_raw_signature_output_guard: bool,
    pub no_shared_ingress_guard: bool,
    pub no_c_artifacts_guard: bool,
    pub draft_not_issued_marker: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason {
    MissingOrInvalidApprovalDraftHash,
    MissingOrInvalidPacketHash,
    MissingOrInvalidProofTierStateHash,
    MissingSelectedRecoveryPath,
    ConditionCountNotOne,
    RecoveryTxCountNotOne,
    OrderCountNonZero,
    CancelCountNonZero,
    PreviewWithoutApprovalNotExit66,
    MissingNoSecretOutputGuard,
    MissingNoRawSignatureOutputGuard,
    MissingNoSharedIngressGuard,
    MissingNoCArtifactsGuard,
    MissingDraftNotIssuedMarker,
    EffectfulExecutionPermittedBeforeFreshApproval,
}

impl BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingOrInvalidApprovalDraftHash => {
                "BLOCK_MISSING_OR_INVALID_APPROVAL_DRAFT_HASH"
            }
            Self::MissingOrInvalidPacketHash => "BLOCK_MISSING_OR_INVALID_PACKET_HASH",
            Self::MissingOrInvalidProofTierStateHash => {
                "BLOCK_MISSING_OR_INVALID_PROOF_TIER_STATE_HASH"
            }
            Self::MissingSelectedRecoveryPath => "BLOCK_MISSING_SELECTED_RECOVERY_PATH",
            Self::ConditionCountNotOne => "BLOCK_CONDITION_COUNT_NOT_ONE",
            Self::RecoveryTxCountNotOne => "BLOCK_RECOVERY_TX_COUNT_NOT_ONE",
            Self::OrderCountNonZero => "BLOCK_ORDER_COUNT_NONZERO",
            Self::CancelCountNonZero => "BLOCK_CANCEL_COUNT_NONZERO",
            Self::PreviewWithoutApprovalNotExit66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_NOT_EXIT_66",
            Self::MissingNoSecretOutputGuard => "BLOCK_MISSING_NO_SECRET_OUTPUT_GUARD",
            Self::MissingNoRawSignatureOutputGuard => "BLOCK_MISSING_NO_RAW_SIGNATURE_OUTPUT_GUARD",
            Self::MissingNoSharedIngressGuard => "BLOCK_MISSING_NO_SHARED_INGRESS_GUARD",
            Self::MissingNoCArtifactsGuard => "BLOCK_MISSING_NO_C_ARTIFACTS_GUARD",
            Self::MissingDraftNotIssuedMarker => "BLOCK_MISSING_DRAFT_NOT_ISSUED_MARKER",
            Self::EffectfulExecutionPermittedBeforeFreshApproval => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_FRESH_APPROVAL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveWrapperBoundaryReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason>,
    pub wrapper_shape_ready_for_review_only_prebuild: bool,
    pub fresh_exact_approval_still_required: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitivePrebuildEvidence {
    pub wrapper_runtime_sha256: String,
    pub controller_source_sha256: String,
    pub packet_sha256: String,
    pub approval_draft_sha256: String,
    pub proof_tier_state_sha256: String,
    pub expected_preview_without_approval_exit_code: Option<i32>,
    pub wrapper_source_review_only: bool,
    pub wrapper_loads_secrets: bool,
    pub wrapper_emits_raw_signature: bool,
    pub wrapper_uses_shared_ingress_or_ws: bool,
    pub wrapper_uses_c_artifacts: bool,
    pub wrapper_stages_effectful_execution: bool,
    pub wrapper_executes_order_or_cancel: bool,
    pub wrapper_executes_recovery_tx: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPrimitivePrebuildBlockReason {
    BoundaryShapeNotReady,
    MissingOrInvalidWrapperRuntimeHash,
    MissingOrInvalidControllerSourceHash,
    PacketHashMismatch,
    ApprovalDraftHashMismatch,
    ProofTierStateHashMismatch,
    PreviewWithoutApprovalNotExit66,
    WrapperSourceNotReviewOnly,
    WrapperLoadsSecrets,
    WrapperEmitsRawSignature,
    WrapperUsesSharedIngressOrWs,
    WrapperUsesCArtifacts,
    WrapperStagesEffectfulExecution,
    WrapperExecutesOrderOrCancel,
    WrapperExecutesRecoveryTxBeforeApproval,
    EffectfulExecutionPermittedBeforeFreshApproval,
}

impl BtcCompletionRecoveryPrimitivePrebuildBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BoundaryShapeNotReady => "BLOCK_BOUNDARY_SHAPE_NOT_READY",
            Self::MissingOrInvalidWrapperRuntimeHash => {
                "BLOCK_MISSING_OR_INVALID_WRAPPER_RUNTIME_HASH"
            }
            Self::MissingOrInvalidControllerSourceHash => {
                "BLOCK_MISSING_OR_INVALID_CONTROLLER_SOURCE_HASH"
            }
            Self::PacketHashMismatch => "BLOCK_PACKET_HASH_MISMATCH",
            Self::ApprovalDraftHashMismatch => "BLOCK_APPROVAL_DRAFT_HASH_MISMATCH",
            Self::ProofTierStateHashMismatch => "BLOCK_PROOF_TIER_STATE_HASH_MISMATCH",
            Self::PreviewWithoutApprovalNotExit66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_NOT_EXIT_66",
            Self::WrapperSourceNotReviewOnly => "BLOCK_WRAPPER_SOURCE_NOT_REVIEW_ONLY",
            Self::WrapperLoadsSecrets => "BLOCK_WRAPPER_LOADS_SECRETS",
            Self::WrapperEmitsRawSignature => "BLOCK_WRAPPER_EMITS_RAW_SIGNATURE",
            Self::WrapperUsesSharedIngressOrWs => "BLOCK_WRAPPER_USES_SHARED_INGRESS_OR_WS",
            Self::WrapperUsesCArtifacts => "BLOCK_WRAPPER_USES_C_ARTIFACTS",
            Self::WrapperStagesEffectfulExecution => "BLOCK_WRAPPER_STAGES_EFFECTFUL_EXECUTION",
            Self::WrapperExecutesOrderOrCancel => "BLOCK_WRAPPER_EXECUTES_ORDER_OR_CANCEL",
            Self::WrapperExecutesRecoveryTxBeforeApproval => {
                "BLOCK_WRAPPER_EXECUTES_RECOVERY_TX_BEFORE_APPROVAL"
            }
            Self::EffectfulExecutionPermittedBeforeFreshApproval => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_FRESH_APPROVAL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitivePrebuildReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionRecoveryPrimitivePrebuildBlockReason>,
    pub prebuild_shape_ready_for_review_only_packet: bool,
    pub fresh_exact_approval_still_required: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitivePreflightEvidence {
    pub reviewed_host: String,
    pub ssh_preflight_fresh: bool,
    pub ssh_preflight_age_ms: Option<u64>,
    pub max_ssh_preflight_age_ms: u64,
    pub remote_packet_sha256: String,
    pub remote_wrapper_runtime_sha256: String,
    pub remote_controller_source_sha256: String,
    pub preview_without_approval_exit_code: Option<i32>,
    pub no_order_auth_preview_passed: bool,
    pub runtime_opens_ws: bool,
    pub max_b_owned_direct_public_ws_connections: u32,
    pub shared_ingress_dependency_detected: bool,
    pub remote_b_process_table_clean: bool,
    pub secret_or_env_dump_detected: bool,
    pub raw_signature_output_detected: bool,
    pub effectful_command_executed: bool,
    pub recovery_tx_submitted: bool,
    pub order_or_cancel_submitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPrimitivePreflightBlockReason {
    PrebuildShapeNotReady,
    HostNotReviewed,
    SshPreflightNotFresh,
    SshPreflightTooOld,
    RemotePacketHashMismatch,
    RemoteWrapperRuntimeHashMismatch,
    RemoteControllerSourceHashMismatch,
    PreviewWithoutApprovalNotExit66,
    NoOrderAuthPreviewNotPassed,
    RuntimeOpensWs,
    TooManyBOwnedDirectPublicWsConnections,
    SharedIngressDependencyDetected,
    RemoteBProcessTableNotClean,
    SecretOrEnvDumpDetected,
    RawSignatureOutputDetected,
    EffectfulCommandExecuted,
    RecoveryTxSubmittedBeforeFreshApproval,
    OrderOrCancelSubmitted,
}

impl BtcCompletionRecoveryPrimitivePreflightBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PrebuildShapeNotReady => "BLOCK_PREBUILD_SHAPE_NOT_READY",
            Self::HostNotReviewed => "BLOCK_HOST_NOT_REVIEWED",
            Self::SshPreflightNotFresh => "BLOCK_SSH_PREFLIGHT_NOT_FRESH",
            Self::SshPreflightTooOld => "BLOCK_SSH_PREFLIGHT_TOO_OLD",
            Self::RemotePacketHashMismatch => "BLOCK_REMOTE_PACKET_HASH_MISMATCH",
            Self::RemoteWrapperRuntimeHashMismatch => "BLOCK_REMOTE_WRAPPER_RUNTIME_HASH_MISMATCH",
            Self::RemoteControllerSourceHashMismatch => {
                "BLOCK_REMOTE_CONTROLLER_SOURCE_HASH_MISMATCH"
            }
            Self::PreviewWithoutApprovalNotExit66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_NOT_EXIT_66",
            Self::NoOrderAuthPreviewNotPassed => "BLOCK_NO_ORDER_AUTH_PREVIEW_NOT_PASSED",
            Self::RuntimeOpensWs => "BLOCK_RUNTIME_OPENS_WS",
            Self::TooManyBOwnedDirectPublicWsConnections => {
                "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS_CONNECTIONS"
            }
            Self::SharedIngressDependencyDetected => "BLOCK_SHARED_INGRESS_DEPENDENCY_DETECTED",
            Self::RemoteBProcessTableNotClean => "BLOCK_REMOTE_B_PROCESS_TABLE_NOT_CLEAN",
            Self::SecretOrEnvDumpDetected => "BLOCK_SECRET_OR_ENV_DUMP_DETECTED",
            Self::RawSignatureOutputDetected => "BLOCK_RAW_SIGNATURE_OUTPUT_DETECTED",
            Self::EffectfulCommandExecuted => "BLOCK_EFFECTFUL_COMMAND_EXECUTED",
            Self::RecoveryTxSubmittedBeforeFreshApproval => {
                "BLOCK_RECOVERY_TX_SUBMITTED_BEFORE_FRESH_APPROVAL"
            }
            Self::OrderOrCancelSubmitted => "BLOCK_ORDER_OR_CANCEL_SUBMITTED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitivePreflightReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionRecoveryPrimitivePreflightBlockReason>,
    pub preflight_shape_ready_for_future_exact_run: bool,
    pub fresh_exact_approval_still_required: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveExecutionPlan {
    pub exact_approval_text_sha256: String,
    pub exact_approval_text_present: bool,
    pub exact_approval_scope_matches_packet: bool,
    pub exact_approval_text_is_draft_not_issued: bool,
    pub exact_approval_already_consumed: bool,
    pub packet_sha256: String,
    pub approval_draft_sha256: String,
    pub preflight_result_sha256: String,
    pub selected_recovery_path: Option<BtcCompletionRecoveryPath>,
    pub condition_id: String,
    pub max_condition_count: u32,
    pub max_recovery_tx_count: u32,
    pub max_order_count: u32,
    pub max_cancel_count: u32,
    pub requests_funding_live_latest_or_deploy: bool,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
    pub imports_candidate: bool,
    pub prints_secret_or_raw_signature: bool,
    pub recovery_primitive_only: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason {
    PreflightShapeNotReady,
    MissingOrInvalidExactApprovalTextHash,
    ExactApprovalTextAbsent,
    ExactApprovalScopeMismatch,
    ExactApprovalTextStillDraftNotIssued,
    ExactApprovalAlreadyConsumed,
    PacketHashMismatch,
    ApprovalDraftHashMismatch,
    MissingOrInvalidPreflightResultHash,
    MissingOrMismatchedRecoveryPath,
    MissingConditionId,
    ConditionCountNotOne,
    RecoveryTxCountNotOne,
    OrderCountNonZero,
    CancelCountNonZero,
    FundingLiveLatestOrDeployRequested,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    CandidateImportRequested,
    SecretOrRawSignaturePrintRequested,
    NotRecoveryPrimitiveOnly,
    EffectfulExecutionPermittedBeforeRuntimeGate,
}

impl BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PreflightShapeNotReady => "BLOCK_PREFLIGHT_SHAPE_NOT_READY",
            Self::MissingOrInvalidExactApprovalTextHash => {
                "BLOCK_MISSING_OR_INVALID_EXACT_APPROVAL_TEXT_HASH"
            }
            Self::ExactApprovalTextAbsent => "BLOCK_EXACT_APPROVAL_TEXT_ABSENT",
            Self::ExactApprovalScopeMismatch => "BLOCK_EXACT_APPROVAL_SCOPE_MISMATCH",
            Self::ExactApprovalTextStillDraftNotIssued => {
                "BLOCK_EXACT_APPROVAL_TEXT_STILL_DRAFT_NOT_ISSUED"
            }
            Self::ExactApprovalAlreadyConsumed => "BLOCK_EXACT_APPROVAL_ALREADY_CONSUMED",
            Self::PacketHashMismatch => "BLOCK_PACKET_HASH_MISMATCH",
            Self::ApprovalDraftHashMismatch => "BLOCK_APPROVAL_DRAFT_HASH_MISMATCH",
            Self::MissingOrInvalidPreflightResultHash => {
                "BLOCK_MISSING_OR_INVALID_PREFLIGHT_RESULT_HASH"
            }
            Self::MissingOrMismatchedRecoveryPath => "BLOCK_MISSING_OR_MISMATCHED_RECOVERY_PATH",
            Self::MissingConditionId => "BLOCK_MISSING_CONDITION_ID",
            Self::ConditionCountNotOne => "BLOCK_CONDITION_COUNT_NOT_ONE",
            Self::RecoveryTxCountNotOne => "BLOCK_RECOVERY_TX_COUNT_NOT_ONE",
            Self::OrderCountNonZero => "BLOCK_ORDER_COUNT_NONZERO",
            Self::CancelCountNonZero => "BLOCK_CANCEL_COUNT_NONZERO",
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::SecretOrRawSignaturePrintRequested => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_PRINT_REQUESTED"
            }
            Self::NotRecoveryPrimitiveOnly => "BLOCK_NOT_RECOVERY_PRIMITIVE_ONLY",
            Self::EffectfulExecutionPermittedBeforeRuntimeGate => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_RUNTIME_GATE"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveExecutionPlanReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason>,
    pub execution_plan_lint_passed_for_fresh_exact_approval: bool,
    pub runtime_preflight_still_required: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveRunResultEvidence {
    pub execution_plan_review_sha256: String,
    pub exact_approval_text_sha256: String,
    pub condition_id: String,
    pub selected_recovery_path: Option<BtcCompletionRecoveryPath>,
    pub recovery_receipt_sha256: Option<String>,
    pub recovery_receipt_proof_tier: BtcCompletionProofTier,
    pub submitted_recovery_tx_count: u32,
    pub submitted_order_count: u32,
    pub submitted_cancel_count: u32,
    pub receipt_id_present: bool,
    pub collateral_pre_observation_sha256: String,
    pub collateral_post_observation_sha256: String,
    pub collateral_delta_positive: bool,
    pub local_ledger_condition_matches: bool,
    pub external_position_condition_matches: bool,
    pub receipt_factory_conversion_passed: bool,
    pub verified_recovery_event_created: bool,
    pub inventory_sync_draft_review_only: bool,
    pub no_open_order_remainder: bool,
    pub residual_exposure_zero: bool,
    pub secret_or_raw_signature_output_detected: bool,
    pub shared_ingress_dependency_detected: bool,
    pub funding_live_latest_or_deploy_touched: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionRecoveryPrimitiveRunResultBlockReason {
    ExecutionPlanLintNotPassed,
    MissingOrInvalidExecutionPlanReviewHash,
    ExactApprovalHashMismatch,
    ConditionIdMismatch,
    RecoveryPathMismatch,
    MissingOrInvalidRecoveryReceiptHash,
    RecoveryReceiptProofTierNotExactApproved,
    RecoveryTxCountNotOne,
    OrderCountNonZero,
    CancelCountNonZero,
    MissingReceiptId,
    MissingOrInvalidCollateralPreObservationHash,
    MissingOrInvalidCollateralPostObservationHash,
    CollateralDeltaNotPositive,
    LocalLedgerConditionMismatch,
    ExternalPositionConditionMismatch,
    ReceiptFactoryConversionMissing,
    VerifiedRecoveryEventMissing,
    InventorySyncDraftNotReviewOnly,
    OpenOrderRemainderPresent,
    ResidualExposureNonZero,
    SecretOrRawSignatureOutputDetected,
    SharedIngressDependencyDetected,
    FundingLiveLatestOrDeployTouched,
}

impl BtcCompletionRecoveryPrimitiveRunResultBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ExecutionPlanLintNotPassed => "BLOCK_EXECUTION_PLAN_LINT_NOT_PASSED",
            Self::MissingOrInvalidExecutionPlanReviewHash => {
                "BLOCK_MISSING_OR_INVALID_EXECUTION_PLAN_REVIEW_HASH"
            }
            Self::ExactApprovalHashMismatch => "BLOCK_EXACT_APPROVAL_HASH_MISMATCH",
            Self::ConditionIdMismatch => "BLOCK_CONDITION_ID_MISMATCH",
            Self::RecoveryPathMismatch => "BLOCK_RECOVERY_PATH_MISMATCH",
            Self::MissingOrInvalidRecoveryReceiptHash => {
                "BLOCK_MISSING_OR_INVALID_RECOVERY_RECEIPT_HASH"
            }
            Self::RecoveryReceiptProofTierNotExactApproved => {
                "BLOCK_RECOVERY_RECEIPT_PROOF_TIER_NOT_EXACT_APPROVED"
            }
            Self::RecoveryTxCountNotOne => "BLOCK_RECOVERY_TX_COUNT_NOT_ONE",
            Self::OrderCountNonZero => "BLOCK_ORDER_COUNT_NONZERO",
            Self::CancelCountNonZero => "BLOCK_CANCEL_COUNT_NONZERO",
            Self::MissingReceiptId => "BLOCK_MISSING_RECEIPT_ID",
            Self::MissingOrInvalidCollateralPreObservationHash => {
                "BLOCK_MISSING_OR_INVALID_COLLATERAL_PRE_OBSERVATION_HASH"
            }
            Self::MissingOrInvalidCollateralPostObservationHash => {
                "BLOCK_MISSING_OR_INVALID_COLLATERAL_POST_OBSERVATION_HASH"
            }
            Self::CollateralDeltaNotPositive => "BLOCK_COLLATERAL_DELTA_NOT_POSITIVE",
            Self::LocalLedgerConditionMismatch => "BLOCK_LOCAL_LEDGER_CONDITION_MISMATCH",
            Self::ExternalPositionConditionMismatch => "BLOCK_EXTERNAL_POSITION_CONDITION_MISMATCH",
            Self::ReceiptFactoryConversionMissing => "BLOCK_RECEIPT_FACTORY_CONVERSION_MISSING",
            Self::VerifiedRecoveryEventMissing => "BLOCK_VERIFIED_RECOVERY_EVENT_MISSING",
            Self::InventorySyncDraftNotReviewOnly => "BLOCK_INVENTORY_SYNC_DRAFT_NOT_REVIEW_ONLY",
            Self::OpenOrderRemainderPresent => "BLOCK_OPEN_ORDER_REMAINDER_PRESENT",
            Self::ResidualExposureNonZero => "BLOCK_RESIDUAL_EXPOSURE_NONZERO",
            Self::SecretOrRawSignatureOutputDetected => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_DETECTED"
            }
            Self::SharedIngressDependencyDetected => "BLOCK_SHARED_INGRESS_DEPENDENCY_DETECTED",
            Self::FundingLiveLatestOrDeployTouched => "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_TOUCHED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionRecoveryPrimitiveRunResultReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionRecoveryPrimitiveRunResultBlockReason>,
    pub run_result_receipt_reconciliation_passed: bool,
    pub exact_approved_recovery_receipt_present: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8MicroLongCycleValidationPlan {
    pub packet_draft_sha256: String,
    pub ex_ante_policy_sha256: String,
    pub s7w_run_result_linter_sha256: String,
    pub scope_asset: String,
    pub fixed_controller_policy: String,
    pub source_guard_500_required: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub shadow_participation_target_rate: f64,
    pub shadow_participation_fail_below_rate: f64,
    pub replay_holdout_market_participation_rate: f64,
    pub max_effectful_markets_total: u32,
    pub max_effectful_markets_per_day: u32,
    pub max_order_submissions_total: u32,
    pub max_capital_lock_usdc: f64,
    pub max_one_leg_exposure_usdc: f64,
    pub max_hold_duration_ms: u64,
    pub max_emergency_flatten_count: u32,
    pub emergency_flatten_is_risk_control_only: bool,
    pub emergency_flatten_is_strategy_pnl_path: bool,
    pub required_recovery_receipt_tier: BtcCompletionProofTier,
    pub requires_positive_collateral_delta: bool,
    pub requires_fee_inclusive_pnl_report: bool,
    pub requires_local_ledger_external_position_alignment: bool,
    pub requires_no_open_order_remainder: bool,
    pub requires_residual_exposure_zero: bool,
    pub stop_on_receipt_or_reconciliation_failure: bool,
    pub stop_on_participation_below_fail_threshold: bool,
    pub exact_approval_required_per_batch: bool,
    pub packet_draft_not_issued_marker: bool,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
    pub requests_funding_live_latest_or_deploy: bool,
    pub imports_candidate: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionS8MicroLongCycleValidationBlockReason {
    MissingOrInvalidPacketDraftHash,
    MissingOrInvalidExAntePolicyHash,
    MissingOrInvalidS7wRunResultLinterHash,
    NonBtcScope,
    FixedControllerPolicyMismatch,
    SourceGuard500Missing,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    ParticipationTargetTooLow,
    ParticipationFailThresholdTooLow,
    ReplayHoldoutParticipationBelowTarget,
    InvalidEffectfulMarketCap,
    InvalidOrderSubmissionCap,
    InvalidCapitalLockCap,
    InvalidOneLegExposureCap,
    InvalidHoldDurationCap,
    InvalidEmergencyFlattenCap,
    EmergencyFlattenNotRiskControlOnly,
    EmergencyFlattenAsStrategyPnlPath,
    RecoveryReceiptTierNotExactApproved,
    MissingPositiveCollateralDeltaRequirement,
    MissingFeeInclusivePnlReportRequirement,
    MissingLedgerExternalPositionAlignmentRequirement,
    MissingNoOpenOrderRemainderRequirement,
    MissingResidualExposureZeroRequirement,
    MissingStopOnReceiptFailure,
    MissingStopOnParticipationFailure,
    MissingExactApprovalBoundary,
    MissingDraftNotIssuedMarker,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    FundingLiveLatestOrDeployRequested,
    CandidateImportRequested,
    EffectfulExecutionPermittedBeforeFreshApproval,
}

impl BtcCompletionS8MicroLongCycleValidationBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingOrInvalidPacketDraftHash => "BLOCK_MISSING_OR_INVALID_PACKET_DRAFT_HASH",
            Self::MissingOrInvalidExAntePolicyHash => {
                "BLOCK_MISSING_OR_INVALID_EX_ANTE_POLICY_HASH"
            }
            Self::MissingOrInvalidS7wRunResultLinterHash => {
                "BLOCK_MISSING_OR_INVALID_S7W_RUN_RESULT_LINTER_HASH"
            }
            Self::NonBtcScope => "BLOCK_NON_BTC_SCOPE",
            Self::FixedControllerPolicyMismatch => "BLOCK_FIXED_CONTROLLER_POLICY_MISMATCH",
            Self::SourceGuard500Missing => "BLOCK_SOURCE_GUARD_500_MISSING",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::ParticipationTargetTooLow => "BLOCK_PARTICIPATION_TARGET_TOO_LOW",
            Self::ParticipationFailThresholdTooLow => "BLOCK_PARTICIPATION_FAIL_THRESHOLD_TOO_LOW",
            Self::ReplayHoldoutParticipationBelowTarget => {
                "BLOCK_REPLAY_HOLDOUT_PARTICIPATION_BELOW_TARGET"
            }
            Self::InvalidEffectfulMarketCap => "BLOCK_INVALID_EFFECTFUL_MARKET_CAP",
            Self::InvalidOrderSubmissionCap => "BLOCK_INVALID_ORDER_SUBMISSION_CAP",
            Self::InvalidCapitalLockCap => "BLOCK_INVALID_CAPITAL_LOCK_CAP",
            Self::InvalidOneLegExposureCap => "BLOCK_INVALID_ONE_LEG_EXPOSURE_CAP",
            Self::InvalidHoldDurationCap => "BLOCK_INVALID_HOLD_DURATION_CAP",
            Self::InvalidEmergencyFlattenCap => "BLOCK_INVALID_EMERGENCY_FLATTEN_CAP",
            Self::EmergencyFlattenNotRiskControlOnly => {
                "BLOCK_EMERGENCY_FLATTEN_NOT_RISK_CONTROL_ONLY"
            }
            Self::EmergencyFlattenAsStrategyPnlPath => {
                "BLOCK_EMERGENCY_FLATTEN_AS_STRATEGY_PNL_PATH"
            }
            Self::RecoveryReceiptTierNotExactApproved => {
                "BLOCK_RECOVERY_RECEIPT_TIER_NOT_EXACT_APPROVED"
            }
            Self::MissingPositiveCollateralDeltaRequirement => {
                "BLOCK_MISSING_POSITIVE_COLLATERAL_DELTA_REQUIREMENT"
            }
            Self::MissingFeeInclusivePnlReportRequirement => {
                "BLOCK_MISSING_FEE_INCLUSIVE_PNL_REPORT_REQUIREMENT"
            }
            Self::MissingLedgerExternalPositionAlignmentRequirement => {
                "BLOCK_MISSING_LEDGER_EXTERNAL_POSITION_ALIGNMENT_REQUIREMENT"
            }
            Self::MissingNoOpenOrderRemainderRequirement => {
                "BLOCK_MISSING_NO_OPEN_ORDER_REMAINDER_REQUIREMENT"
            }
            Self::MissingResidualExposureZeroRequirement => {
                "BLOCK_MISSING_RESIDUAL_EXPOSURE_ZERO_REQUIREMENT"
            }
            Self::MissingStopOnReceiptFailure => "BLOCK_MISSING_STOP_ON_RECEIPT_FAILURE",
            Self::MissingStopOnParticipationFailure => {
                "BLOCK_MISSING_STOP_ON_PARTICIPATION_FAILURE"
            }
            Self::MissingExactApprovalBoundary => "BLOCK_MISSING_EXACT_APPROVAL_BOUNDARY",
            Self::MissingDraftNotIssuedMarker => "BLOCK_MISSING_DRAFT_NOT_ISSUED_MARKER",
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::EffectfulExecutionPermittedBeforeFreshApproval => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_FRESH_APPROVAL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8MicroLongCycleValidationReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionS8MicroLongCycleValidationBlockReason>,
    pub s8_packet_ready_for_exact_approval_request: bool,
    pub shadow_participation_guard_ready: bool,
    pub result_reconciliation_uses_s7w: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8aExecutionWrapperPrebuildEvidence {
    pub loss_trigger_redesign_packet_sha256: String,
    pub partial_fill_addendum_sha256: String,
    pub three_round_cap_packet_sha256: String,
    pub s7w_run_result_linter_sha256: String,
    pub wrapper_source_sha256: String,
    pub controller_source_sha256: String,
    pub scope_asset: String,
    pub market_family: String,
    pub fixed_controller_policy: String,
    pub source_guard_500_required: bool,
    pub order_size: f64,
    pub api_min_order_size: f64,
    pub seed_px_hi: f64,
    pub l1_pair_ask_max: f64,
    pub max_round_count: u32,
    pub session_hard_loss_cap_usdc: f64,
    pub per_round_theoretical_max_loss_usdc: f64,
    pub target_runtime_ms: u64,
    pub max_active_market_count: u32,
    pub max_active_capital_lock_usdc: f64,
    pub max_cumulative_gross_quote_spend_usdc: f64,
    pub max_initial_buy_submissions: u32,
    pub max_emergency_exit_or_hedge_submissions: u32,
    pub max_total_order_submissions: u32,
    pub max_cancel_count: u32,
    pub max_recovery_tx_count: u32,
    pub shadow_participation_pass_rate: f64,
    pub shadow_participation_warning_below_rate: f64,
    pub roi_report_only_for_first_s8a: bool,
    pub residual_cost_rate_report_only_for_first_s8a: bool,
    pub minimum_roi_hard_gate_enabled: bool,
    pub uses_actual_filled_qty_inventory: bool,
    pub submitted_size_counts_as_inventory: bool,
    pub partial_fill_threshold_enabled: bool,
    pub forced_complement_after_partial_fill: bool,
    pub forced_complement_without_natural_signal: bool,
    pub api_min_order_size_enforced: bool,
    pub sub_min_residual_sell_blocked: bool,
    pub no_new_round_while_active_market: bool,
    pub pre_entry_realized_loss_plus_round_cap_guard: bool,
    pub active_risk_at_work_cap_guard: bool,
    pub realized_loss_only_after_close_or_recovery: bool,
    pub emergency_flatten_is_risk_control_only: bool,
    pub emergency_flatten_is_strategy_pnl_path: bool,
    pub s7w_reconciliation_required: bool,
    pub exact_approval_required: bool,
    pub draft_not_issued_marker: bool,
    pub preview_without_approval_exit_code: Option<i32>,
    pub no_order_auth_preview_required: bool,
    pub runtime_opens_own_ws: bool,
    pub max_b_owned_direct_public_ws_connections: u32,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub imports_candidate: bool,
    pub prints_secret_or_raw_signature: bool,
    pub requests_funding_live_latest_or_deploy: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionS8aExecutionWrapperPrebuildBlockReason {
    LossTriggerPacketHashMismatch,
    PartialFillPacketHashMismatch,
    ThreeRoundCapPacketHashMismatch,
    S7wRunResultLinterHashMismatch,
    MissingOrInvalidWrapperSourceHash,
    MissingOrInvalidControllerSourceHash,
    NonBtcScope,
    NonBtc5mMarketFamily,
    FixedControllerPolicyMismatch,
    SourceGuard500Missing,
    InvalidOrderSize,
    ApiMinimumOrderSizeMismatch,
    SeedPriceHighMismatch,
    PairAskCapMismatch,
    InvalidRoundOrLossCaps,
    InvalidRuntimeDurationCap,
    InvalidActiveMarketCap,
    InvalidCapitalOrSpendCaps,
    InvalidOrderCancelOrRecoveryCaps,
    ParticipationPassThresholdTooHigh,
    ParticipationWarningThresholdInvalid,
    RoiOrResidualIncorrectlyHardGated,
    ActualFilledQtyInventoryMissing,
    SubmittedSizeCountsAsInventory,
    PartialFillThresholdEnabled,
    ForcedComplementAfterPartialFill,
    ForcedComplementWithoutNaturalSignal,
    ApiMinimumOrderSizeNotEnforced,
    SubMinResidualSellAllowed,
    NewRoundAllowedWhileActiveMarket,
    MissingPreEntryLossGuard,
    MissingActiveRiskAtWorkGuard,
    RealizedLossDefinedBeforeClosure,
    EmergencyFlattenNotRiskControlOnly,
    EmergencyFlattenAsStrategyPnlPath,
    MissingS7wReconciliationRequirement,
    MissingExactApprovalRequirement,
    MissingDraftNotIssuedMarker,
    PreviewWithoutApprovalExitNot66,
    MissingNoOrderAuthPreviewRequirement,
    RuntimeOpensOwnWs,
    TooManyBOwnedDirectPublicWs,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    CandidateImportRequested,
    SecretOrRawSignatureOutputAllowed,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionPermittedBeforeFreshApproval,
}

impl BtcCompletionS8aExecutionWrapperPrebuildBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LossTriggerPacketHashMismatch => "BLOCK_LOSS_TRIGGER_PACKET_HASH_MISMATCH",
            Self::PartialFillPacketHashMismatch => "BLOCK_PARTIAL_FILL_PACKET_HASH_MISMATCH",
            Self::ThreeRoundCapPacketHashMismatch => "BLOCK_THREE_ROUND_CAP_PACKET_HASH_MISMATCH",
            Self::S7wRunResultLinterHashMismatch => "BLOCK_S7W_RUN_RESULT_LINTER_HASH_MISMATCH",
            Self::MissingOrInvalidWrapperSourceHash => {
                "BLOCK_MISSING_OR_INVALID_WRAPPER_SOURCE_HASH"
            }
            Self::MissingOrInvalidControllerSourceHash => {
                "BLOCK_MISSING_OR_INVALID_CONTROLLER_SOURCE_HASH"
            }
            Self::NonBtcScope => "BLOCK_NON_BTC_SCOPE",
            Self::NonBtc5mMarketFamily => "BLOCK_NON_BTC_5M_MARKET_FAMILY",
            Self::FixedControllerPolicyMismatch => "BLOCK_FIXED_CONTROLLER_POLICY_MISMATCH",
            Self::SourceGuard500Missing => "BLOCK_SOURCE_GUARD_500_MISSING",
            Self::InvalidOrderSize => "BLOCK_INVALID_ORDER_SIZE",
            Self::ApiMinimumOrderSizeMismatch => "BLOCK_API_MINIMUM_ORDER_SIZE_MISMATCH",
            Self::SeedPriceHighMismatch => "BLOCK_SEED_PRICE_HIGH_MISMATCH",
            Self::PairAskCapMismatch => "BLOCK_PAIR_ASK_CAP_MISMATCH",
            Self::InvalidRoundOrLossCaps => "BLOCK_INVALID_ROUND_OR_LOSS_CAPS",
            Self::InvalidRuntimeDurationCap => "BLOCK_INVALID_RUNTIME_DURATION_CAP",
            Self::InvalidActiveMarketCap => "BLOCK_INVALID_ACTIVE_MARKET_CAP",
            Self::InvalidCapitalOrSpendCaps => "BLOCK_INVALID_CAPITAL_OR_SPEND_CAPS",
            Self::InvalidOrderCancelOrRecoveryCaps => "BLOCK_INVALID_ORDER_CANCEL_OR_RECOVERY_CAPS",
            Self::ParticipationPassThresholdTooHigh => {
                "BLOCK_PARTICIPATION_PASS_THRESHOLD_TOO_HIGH"
            }
            Self::ParticipationWarningThresholdInvalid => {
                "BLOCK_PARTICIPATION_WARNING_THRESHOLD_INVALID"
            }
            Self::RoiOrResidualIncorrectlyHardGated => {
                "BLOCK_ROI_OR_RESIDUAL_INCORRECTLY_HARD_GATED"
            }
            Self::ActualFilledQtyInventoryMissing => "BLOCK_ACTUAL_FILLED_QTY_INVENTORY_MISSING",
            Self::SubmittedSizeCountsAsInventory => "BLOCK_SUBMITTED_SIZE_COUNTS_AS_INVENTORY",
            Self::PartialFillThresholdEnabled => "BLOCK_PARTIAL_FILL_THRESHOLD_ENABLED",
            Self::ForcedComplementAfterPartialFill => "BLOCK_FORCED_COMPLEMENT_AFTER_PARTIAL_FILL",
            Self::ForcedComplementWithoutNaturalSignal => {
                "BLOCK_FORCED_COMPLEMENT_WITHOUT_NATURAL_SIGNAL"
            }
            Self::ApiMinimumOrderSizeNotEnforced => "BLOCK_API_MINIMUM_ORDER_SIZE_NOT_ENFORCED",
            Self::SubMinResidualSellAllowed => "BLOCK_SUB_MIN_RESIDUAL_SELL_ALLOWED",
            Self::NewRoundAllowedWhileActiveMarket => "BLOCK_NEW_ROUND_ALLOWED_WHILE_ACTIVE_MARKET",
            Self::MissingPreEntryLossGuard => "BLOCK_MISSING_PRE_ENTRY_LOSS_GUARD",
            Self::MissingActiveRiskAtWorkGuard => "BLOCK_MISSING_ACTIVE_RISK_AT_WORK_GUARD",
            Self::RealizedLossDefinedBeforeClosure => "BLOCK_REALIZED_LOSS_DEFINED_BEFORE_CLOSURE",
            Self::EmergencyFlattenNotRiskControlOnly => {
                "BLOCK_EMERGENCY_FLATTEN_NOT_RISK_CONTROL_ONLY"
            }
            Self::EmergencyFlattenAsStrategyPnlPath => {
                "BLOCK_EMERGENCY_FLATTEN_AS_STRATEGY_PNL_PATH"
            }
            Self::MissingS7wReconciliationRequirement => {
                "BLOCK_MISSING_S7W_RECONCILIATION_REQUIREMENT"
            }
            Self::MissingExactApprovalRequirement => "BLOCK_MISSING_EXACT_APPROVAL_REQUIREMENT",
            Self::MissingDraftNotIssuedMarker => "BLOCK_MISSING_DRAFT_NOT_ISSUED_MARKER",
            Self::PreviewWithoutApprovalExitNot66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_EXIT_NOT_66",
            Self::MissingNoOrderAuthPreviewRequirement => {
                "BLOCK_MISSING_NO_ORDER_AUTH_PREVIEW_REQUIREMENT"
            }
            Self::RuntimeOpensOwnWs => "BLOCK_RUNTIME_OPENS_OWN_WS",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
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
            Self::EffectfulExecutionPermittedBeforeFreshApproval => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_FRESH_APPROVAL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8aExecutionWrapperPrebuildReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionS8aExecutionWrapperPrebuildBlockReason>,
    pub wrapper_prebuild_ready_for_exact_approval_request: bool,
    pub s8a_hash_boundary_bound: bool,
    pub s8a_runtime_semantics_bound: bool,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8aEffectfulRuntimeReadinessEvidence {
    pub prebuild_review_passed: bool,
    pub loss_trigger_redesign_packet_sha256: String,
    pub partial_fill_addendum_sha256: String,
    pub three_round_cap_packet_sha256: String,
    pub s7w_run_result_linter_sha256: String,
    pub wrapper_source_sha256: String,
    pub controller_source_sha256: String,
    pub wrapper_supports_preview_without_approval: bool,
    pub preview_without_approval_exit_code: Option<i32>,
    pub wrapper_supports_no_order_auth_preview: bool,
    pub no_order_auth_preview_uses_reviewed_auth_path: bool,
    pub no_order_auth_preview_prints_secret_or_signature: bool,
    pub wrapper_supports_exact_order_path: bool,
    pub exact_order_path_requires_approval_hash: bool,
    pub exact_order_path_rejects_missing_or_mismatched_approval: bool,
    pub uses_native_s8a_order_adapter: bool,
    pub limit_order_min_size_shares: f64,
    pub limit_entry_order_size_shares: f64,
    pub market_buy_min_notional_usdc: f64,
    pub market_buy_amount_is_usdc: bool,
    pub market_sell_amount_is_shares: bool,
    pub market_sell_min_size_shares: f64,
    pub sub_min_residual_market_sell_blocked: bool,
    pub order_minimums_verified_from_official_clob_market_info: bool,
    pub order_amount_units_verified_from_official_order_docs: bool,
    pub live_public_buy_signal_source_bound: bool,
    pub btc_5m_target_derivation_bound: bool,
    pub source_guard_500_live_enforced: bool,
    pub uses_s8a_controller_for_every_admission: bool,
    pub uses_actual_filled_qty_inventory: bool,
    pub submitted_size_counts_as_inventory: bool,
    pub partial_fill_threshold_enabled: bool,
    pub forced_complement_without_natural_signal: bool,
    pub max_round_count: u32,
    pub session_hard_loss_cap_usdc: f64,
    pub per_round_theoretical_max_loss_usdc: f64,
    pub order_size: f64,
    pub max_active_market_count: u32,
    pub max_initial_buy_submissions: u32,
    pub max_total_order_submissions: u32,
    pub max_cancel_count: u32,
    pub max_recovery_tx_count: u32,
    pub s7w_post_run_reconciliation_bound: bool,
    pub s7w_requires_exact_approved_receipt_tier: bool,
    pub s7w_rejects_review_only_fixture_receipt: bool,
    pub runtime_uses_s1a_same_run_flatten_plan: bool,
    pub runtime_uses_s3a_admission_or_pair_rank: bool,
    pub runtime_opens_own_ws: bool,
    pub max_b_owned_direct_public_ws_connections: u32,
    pub uses_shared_ingress_or_shared_ws: bool,
    pub uses_c_artifacts: bool,
    pub imports_candidate: bool,
    pub online_tuning_allowed: bool,
    pub strategy_discovery_allowed: bool,
    pub prints_secret_or_raw_signature: bool,
    pub requests_funding_live_latest_or_deploy: bool,
    pub effectful_execution_permitted_now: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtcCompletionS8aEffectfulRuntimeReadinessBlockReason {
    PrebuildReviewNotPassed,
    LossTriggerPacketHashMismatch,
    PartialFillPacketHashMismatch,
    ThreeRoundCapPacketHashMismatch,
    S7wRunResultLinterHashMismatch,
    MissingOrInvalidWrapperSourceHash,
    MissingOrInvalidControllerSourceHash,
    PreviewWithoutApprovalUnavailable,
    PreviewWithoutApprovalExitNot66,
    NoOrderAuthPreviewUnavailable,
    NoOrderAuthPreviewNotReviewedAuthPath,
    NoOrderAuthPreviewSecretOrSignatureOutput,
    ExactOrderPathUnavailable,
    ExactOrderPathMissingApprovalHashGate,
    ExactOrderPathDoesNotRejectApprovalMismatch,
    NativeS8aOrderAdapterMissing,
    InvalidLimitOrderMinimums,
    InvalidMarketBuyMinimumNotional,
    MarketBuyAmountUnitNotUsdc,
    MarketSellAmountUnitNotShares,
    InvalidMarketSellMinimumShares,
    SubMinResidualMarketSellNotBlocked,
    OrderMinimumsNotVerifiedFromOfficialSources,
    LivePublicBuySignalSourceMissing,
    Btc5mTargetDerivationMissing,
    SourceGuard500NotLiveEnforced,
    S8aControllerNotUsedForEveryAdmission,
    ActualFilledQtyInventoryMissing,
    SubmittedSizeCountsAsInventory,
    PartialFillThresholdEnabled,
    ForcedComplementWithoutNaturalSignal,
    InvalidRoundLossOrSizeCaps,
    InvalidOrderCancelOrRecoveryCaps,
    S7wPostRunReconciliationMissing,
    S7wExactApprovedReceiptTierMissing,
    S7wReviewOnlyFixtureReceiptAccepted,
    ReusesS1aSameRunFlattenPlan,
    ReusesS3aAdmissionOrPairRank,
    RuntimeOpensOwnWs,
    TooManyBOwnedDirectPublicWs,
    SharedIngressOrSharedWsRequested,
    CArtifactsRequested,
    CandidateImportRequested,
    OnlineTuningAllowed,
    StrategyDiscoveryAllowed,
    SecretOrRawSignatureOutputAllowed,
    FundingLiveLatestOrDeployRequested,
    EffectfulExecutionPermittedBeforeFreshApproval,
}

impl BtcCompletionS8aEffectfulRuntimeReadinessBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PrebuildReviewNotPassed => "BLOCK_PREBUILD_REVIEW_NOT_PASSED",
            Self::LossTriggerPacketHashMismatch => "BLOCK_LOSS_TRIGGER_PACKET_HASH_MISMATCH",
            Self::PartialFillPacketHashMismatch => "BLOCK_PARTIAL_FILL_PACKET_HASH_MISMATCH",
            Self::ThreeRoundCapPacketHashMismatch => "BLOCK_THREE_ROUND_CAP_PACKET_HASH_MISMATCH",
            Self::S7wRunResultLinterHashMismatch => "BLOCK_S7W_RUN_RESULT_LINTER_HASH_MISMATCH",
            Self::MissingOrInvalidWrapperSourceHash => {
                "BLOCK_MISSING_OR_INVALID_WRAPPER_SOURCE_HASH"
            }
            Self::MissingOrInvalidControllerSourceHash => {
                "BLOCK_MISSING_OR_INVALID_CONTROLLER_SOURCE_HASH"
            }
            Self::PreviewWithoutApprovalUnavailable => "BLOCK_PREVIEW_WITHOUT_APPROVAL_UNAVAILABLE",
            Self::PreviewWithoutApprovalExitNot66 => "BLOCK_PREVIEW_WITHOUT_APPROVAL_EXIT_NOT_66",
            Self::NoOrderAuthPreviewUnavailable => "BLOCK_NO_ORDER_AUTH_PREVIEW_UNAVAILABLE",
            Self::NoOrderAuthPreviewNotReviewedAuthPath => {
                "BLOCK_NO_ORDER_AUTH_PREVIEW_NOT_REVIEWED_AUTH_PATH"
            }
            Self::NoOrderAuthPreviewSecretOrSignatureOutput => {
                "BLOCK_NO_ORDER_AUTH_PREVIEW_SECRET_OR_SIGNATURE_OUTPUT"
            }
            Self::ExactOrderPathUnavailable => "BLOCK_EXACT_ORDER_PATH_UNAVAILABLE",
            Self::ExactOrderPathMissingApprovalHashGate => {
                "BLOCK_EXACT_ORDER_PATH_MISSING_APPROVAL_HASH_GATE"
            }
            Self::ExactOrderPathDoesNotRejectApprovalMismatch => {
                "BLOCK_EXACT_ORDER_PATH_DOES_NOT_REJECT_APPROVAL_MISMATCH"
            }
            Self::NativeS8aOrderAdapterMissing => "BLOCK_NATIVE_S8A_ORDER_ADAPTER_MISSING",
            Self::InvalidLimitOrderMinimums => "BLOCK_INVALID_LIMIT_ORDER_MINIMUMS",
            Self::InvalidMarketBuyMinimumNotional => "BLOCK_INVALID_MARKET_BUY_MINIMUM_NOTIONAL",
            Self::MarketBuyAmountUnitNotUsdc => "BLOCK_MARKET_BUY_AMOUNT_UNIT_NOT_USDC",
            Self::MarketSellAmountUnitNotShares => "BLOCK_MARKET_SELL_AMOUNT_UNIT_NOT_SHARES",
            Self::InvalidMarketSellMinimumShares => "BLOCK_INVALID_MARKET_SELL_MINIMUM_SHARES",
            Self::SubMinResidualMarketSellNotBlocked => {
                "BLOCK_SUB_MIN_RESIDUAL_MARKET_SELL_NOT_BLOCKED"
            }
            Self::OrderMinimumsNotVerifiedFromOfficialSources => {
                "BLOCK_ORDER_MINIMUMS_NOT_VERIFIED_FROM_OFFICIAL_SOURCES"
            }
            Self::LivePublicBuySignalSourceMissing => "BLOCK_LIVE_PUBLIC_BUY_SIGNAL_SOURCE_MISSING",
            Self::Btc5mTargetDerivationMissing => "BLOCK_BTC_5M_TARGET_DERIVATION_MISSING",
            Self::SourceGuard500NotLiveEnforced => "BLOCK_SOURCE_GUARD_500_NOT_LIVE_ENFORCED",
            Self::S8aControllerNotUsedForEveryAdmission => {
                "BLOCK_S8A_CONTROLLER_NOT_USED_FOR_EVERY_ADMISSION"
            }
            Self::ActualFilledQtyInventoryMissing => "BLOCK_ACTUAL_FILLED_QTY_INVENTORY_MISSING",
            Self::SubmittedSizeCountsAsInventory => "BLOCK_SUBMITTED_SIZE_COUNTS_AS_INVENTORY",
            Self::PartialFillThresholdEnabled => "BLOCK_PARTIAL_FILL_THRESHOLD_ENABLED",
            Self::ForcedComplementWithoutNaturalSignal => {
                "BLOCK_FORCED_COMPLEMENT_WITHOUT_NATURAL_SIGNAL"
            }
            Self::InvalidRoundLossOrSizeCaps => "BLOCK_INVALID_ROUND_LOSS_OR_SIZE_CAPS",
            Self::InvalidOrderCancelOrRecoveryCaps => "BLOCK_INVALID_ORDER_CANCEL_OR_RECOVERY_CAPS",
            Self::S7wPostRunReconciliationMissing => "BLOCK_S7W_POST_RUN_RECONCILIATION_MISSING",
            Self::S7wExactApprovedReceiptTierMissing => {
                "BLOCK_S7W_EXACT_APPROVED_RECEIPT_TIER_MISSING"
            }
            Self::S7wReviewOnlyFixtureReceiptAccepted => {
                "BLOCK_S7W_REVIEW_ONLY_FIXTURE_RECEIPT_ACCEPTED"
            }
            Self::ReusesS1aSameRunFlattenPlan => "BLOCK_REUSES_S1A_SAME_RUN_FLATTEN_PLAN",
            Self::ReusesS3aAdmissionOrPairRank => "BLOCK_REUSES_S3A_ADMISSION_OR_PAIR_RANK",
            Self::RuntimeOpensOwnWs => "BLOCK_RUNTIME_OPENS_OWN_WS",
            Self::TooManyBOwnedDirectPublicWs => "BLOCK_TOO_MANY_B_OWNED_DIRECT_PUBLIC_WS",
            Self::SharedIngressOrSharedWsRequested => "BLOCK_SHARED_INGRESS_OR_SHARED_WS_REQUESTED",
            Self::CArtifactsRequested => "BLOCK_C_ARTIFACTS_REQUESTED",
            Self::CandidateImportRequested => "BLOCK_CANDIDATE_IMPORT_REQUESTED",
            Self::OnlineTuningAllowed => "BLOCK_ONLINE_TUNING_ALLOWED",
            Self::StrategyDiscoveryAllowed => "BLOCK_STRATEGY_DISCOVERY_ALLOWED",
            Self::SecretOrRawSignatureOutputAllowed => {
                "BLOCK_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED"
            }
            Self::FundingLiveLatestOrDeployRequested => {
                "BLOCK_FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED"
            }
            Self::EffectfulExecutionPermittedBeforeFreshApproval => {
                "BLOCK_EFFECTFUL_EXECUTION_PERMITTED_BEFORE_FRESH_APPROVAL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BtcCompletionS8aEffectfulRuntimeReadinessReview {
    pub event: &'static str,
    pub block_reasons: Vec<BtcCompletionS8aEffectfulRuntimeReadinessBlockReason>,
    pub runtime_ready_for_exact_approval_request: bool,
    pub native_s8a_runtime_bound: bool,
    pub live_source_and_auth_preview_bound: bool,
    pub effectful_execution_permitted: bool,
}

pub fn review_btc_completion_candidate(
    cfg: &BtcCompletionControllerConfig,
    state: &BtcCompletionControllerState,
    candidate: &BtcCompletionCandidate,
) -> BtcCompletionControllerReview {
    if candidate.asset != "BTC" {
        return block(BtcCompletionControllerBlockReason::NonBtcAsset);
    }
    if candidate.event_kind != "public_trade" {
        return block(BtcCompletionControllerBlockReason::NonPublicTradeEvent);
    }
    if candidate.public_trade_taker_side != "BUY" {
        return block(BtcCompletionControllerBlockReason::NonBuyPublicTrade);
    }
    if candidate.forbidden_decision_fields_present {
        return block(BtcCompletionControllerBlockReason::ForbiddenLeakageFieldPresent);
    }
    if candidate.snapshot_index_used_as_rank {
        return block(BtcCompletionControllerBlockReason::SnapshotIndexRankUsed);
    }
    if candidate.offset_s > cfg.offset_max_s {
        return block(BtcCompletionControllerBlockReason::OffsetTooLate);
    }
    if let Some(time_to_end_s) = candidate.time_to_end_s {
        if time_to_end_s < cfg.time_to_end_min_s {
            return block(BtcCompletionControllerBlockReason::TimeToEndTooShort);
        }
    }

    let seed_price = candidate.public_trade_price - cfg.edge;
    if seed_price < cfg.seed_px_lo || seed_price > cfg.seed_px_hi {
        return block(BtcCompletionControllerBlockReason::InvalidSeedPrice);
    }
    if candidate.l1_pair_ask > cfg.l1_pair_ask_max {
        return block(BtcCompletionControllerBlockReason::PairAskAboveCap);
    }
    if candidate.l1_pair_available_qty < cfg.seed_qty || candidate.buy_available_qty < cfg.seed_qty
    {
        return block(BtcCompletionControllerBlockReason::InsufficientLiquidity);
    }
    match candidate.strict_l1_age_ms {
        Some(age_ms) if age_ms <= cfg.max_strict_l1_age_ms => {}
        Some(_) => return block(BtcCompletionControllerBlockReason::StrictL1AgeTooStale),
        None => return block(BtcCompletionControllerBlockReason::MissingStrictL1Age),
    }
    match candidate.public_trade_recv_lag_ms {
        Some(lag_ms) if lag_ms <= cfg.max_public_trade_recv_lag_ms => {}
        Some(_) => return block(BtcCompletionControllerBlockReason::PublicTradeRecvLagTooHigh),
        None => return block(BtcCompletionControllerBlockReason::MissingPublicTradeRecvLag),
    }

    let condition_state = state.condition_state(&candidate.condition_id);
    if let Some(last_ts) = condition_state.last_accept_ts_ms {
        let cooldown_ms = (cfg.cooldown_s * 1000.0).round() as u64;
        if candidate.ts_ms.saturating_sub(last_ts) < cooldown_ms {
            return block(BtcCompletionControllerBlockReason::CooldownActive);
        }
    }

    let (post_yes, post_no) = match candidate.side {
        Side::Yes => (
            condition_state.yes_qty + cfg.seed_qty,
            condition_state.no_qty,
        ),
        Side::No => (
            condition_state.yes_qty,
            condition_state.no_qty + cfg.seed_qty,
        ),
    };
    if (post_yes - post_no).abs() > cfg.imbalance_qty_cap {
        return block(BtcCompletionControllerBlockReason::InventoryImbalanceWouldExceedCap);
    }

    BtcCompletionControllerReview {
        intent: Some(BtcCompletionResearchIntent {
            condition_id: candidate.condition_id.clone(),
            slug: candidate.slug.clone(),
            ts_ms: candidate.ts_ms,
            side: candidate.side,
            price: seed_price,
            qty: cfg.seed_qty,
            execution_permitted: BTC_COMPLETION_CONTROLLER_ENABLED_DEFAULT,
        }),
        block_reason: None,
        execution_permitted: BTC_COMPLETION_CONTROLLER_ENABLED_DEFAULT,
    }
}

fn block(reason: BtcCompletionControllerBlockReason) -> BtcCompletionControllerReview {
    BtcCompletionControllerReview {
        intent: None,
        block_reason: Some(reason),
        execution_permitted: BTC_COMPLETION_CONTROLLER_ENABLED_DEFAULT,
    }
}

pub fn review_btc_completion_strategy_canary_gate(
    policy: &BtcCompletionStrategyCanaryGatePolicy,
) -> BtcCompletionStrategyCanaryGateReview {
    let mut block_reasons = Vec::new();

    if policy.scope_asset.trim() != "BTC" {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::NonBtcScope);
    }
    if policy.controller_policy_sha256.trim().is_empty() {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::MissingControllerPolicyHash);
    }
    if policy.replay_result_sha256.trim().is_empty() {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::MissingReplayResultHash);
    }
    if policy
        .recovery_policy_sha256
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::MissingRecoveryPolicyHash);
    }
    if policy.recovery_path.is_none() {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::MissingRecoveryPath);
    }
    if !finite_positive(policy.max_capital_lock_usdc) {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingOrInvalidCapitalLockLimit);
    }
    if policy.max_capital_lock_ms.unwrap_or_default() == 0 {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingOrInvalidCapitalLockDuration);
    }
    if !finite_ratio(policy.max_residual_cost_rate) {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingOrInvalidResidualCostPolicy);
    }
    if !finite_ratio(policy.max_bad_pair_cost_ge_1_share) {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingOrInvalidBadPairCostPolicy);
    }
    if !finite_positive(policy.max_one_leg_exposure_usdc) {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingOrInvalidOneLegExposureLimit);
    }
    if !policy.requires_source_guard_500 {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::MissingSourceGuard500);
    }
    if !policy.requires_same_market_yes_no_close_window {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingSameMarketCloseWindowGuard);
    }
    if !policy.requires_external_position_reconciliation {
        block_reasons.push(
            BtcCompletionStrategyCanaryGateBlockReason::MissingExternalPositionReconciliation,
        );
    }
    if !policy.requires_recovery_receipt_factory {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingRecoveryReceiptFactory);
    }
    if !policy.requires_no_open_order_remainder {
        block_reasons
            .push(BtcCompletionStrategyCanaryGateBlockReason::MissingNoOpenOrderRemainderGuard);
    }
    if policy.allows_same_run_flatten_as_primary_exit {
        block_reasons.push(BtcCompletionStrategyCanaryGateBlockReason::SameRunFlattenPrimaryExit);
    }

    let hash_bound_policy_ready = policy.controller_policy_sha256.trim().len() == 64
        && policy.replay_result_sha256.trim().len() == 64
        && policy
            .recovery_policy_sha256
            .as_deref()
            .map(str::trim)
            .map(|hash| hash.len() == 64)
            .unwrap_or(false);
    let future_exact_canary_policy_ready = block_reasons.is_empty() && hash_bound_policy_ready;

    BtcCompletionStrategyCanaryGateReview {
        event: BTC_COMPLETION_STRATEGY_CANARY_GATE_REVIEW_EVENT,
        block_reasons,
        hash_bound_policy_ready,
        future_exact_canary_policy_ready,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn btc_completion_ex_ante_policy_payload_review_only(
    strategy_lane: &str,
    policy: &BtcCompletionStrategyCanaryGatePolicy,
) -> Value {
    let review = review_btc_completion_strategy_canary_gate(policy);
    json!({
        "policy_schema_version": BTC_COMPLETION_EX_ANTE_POLICY_PAYLOAD_SCHEMA_VERSION,
        "event": BTC_COMPLETION_STRATEGY_CANARY_GATE_REVIEW_EVENT,
        "strategy_lane": strategy_lane,
        "strategy_research_only": true,
        "scope_asset": policy.scope_asset,
        "controller_policy_sha256": policy.controller_policy_sha256,
        "replay_result_sha256": policy.replay_result_sha256,
        "recovery_policy_sha256": policy.recovery_policy_sha256,
        "recovery_path": policy.recovery_path.map(|path| path.as_str()),
        "max_capital_lock_usdc": policy.max_capital_lock_usdc,
        "max_capital_lock_ms": policy.max_capital_lock_ms,
        "max_residual_cost_rate": policy.max_residual_cost_rate,
        "max_bad_pair_cost_ge_1_share": policy.max_bad_pair_cost_ge_1_share,
        "max_one_leg_exposure_usdc": policy.max_one_leg_exposure_usdc,
        "requires_source_guard_500": policy.requires_source_guard_500,
        "requires_same_market_yes_no_close_window": policy.requires_same_market_yes_no_close_window,
        "requires_external_position_reconciliation": policy.requires_external_position_reconciliation,
        "requires_recovery_receipt_factory": policy.requires_recovery_receipt_factory,
        "requires_no_open_order_remainder": policy.requires_no_open_order_remainder,
        "allows_same_run_flatten_as_primary_exit": policy.allows_same_run_flatten_as_primary_exit,
        "emergency_flatten_is_canary_risk_control_only": true,
        "emergency_flatten_is_strategy_pnl_path": false,
        "hash_bound_policy_ready": review.hash_bound_policy_ready,
        "future_exact_canary_policy_ready": review.future_exact_canary_policy_ready,
        "effectful_execution_permitted": review.effectful_execution_permitted,
        "block_reasons": review
            .block_reasons
            .iter()
            .map(BtcCompletionStrategyCanaryGateBlockReason::as_str)
            .collect::<Vec<_>>(),
        "uses_shared_ingress_or_shared_ws": false,
        "uses_c_artifacts": false,
    })
}

pub fn btc_completion_ex_ante_policy_payload_schema_failures(payload: &Value) -> Vec<String> {
    let Some(object) = payload.as_object() else {
        return vec!["payload_not_object".to_string()];
    };

    let mut failures = Vec::new();
    for key in BTC_COMPLETION_EX_ANTE_POLICY_PAYLOAD_SCHEMA_KEYS {
        if !object.contains_key(key) {
            failures.push(format!("missing_key:{key}"));
        }
    }

    if payload.get("policy_schema_version").and_then(Value::as_u64)
        != Some(BTC_COMPLETION_EX_ANTE_POLICY_PAYLOAD_SCHEMA_VERSION as u64)
    {
        failures.push("policy_schema_version_mismatch".to_string());
    }
    if payload.get("event").and_then(Value::as_str)
        != Some(BTC_COMPLETION_STRATEGY_CANARY_GATE_REVIEW_EVENT)
    {
        failures.push("event_mismatch".to_string());
    }
    if payload
        .get("strategy_research_only")
        .and_then(Value::as_bool)
        != Some(true)
    {
        failures.push("strategy_research_only_not_true".to_string());
    }
    if payload
        .get("effectful_execution_permitted")
        .and_then(Value::as_bool)
        != Some(false)
    {
        failures.push("effectful_execution_permitted_not_false".to_string());
    }
    if payload
        .get("emergency_flatten_is_canary_risk_control_only")
        .and_then(Value::as_bool)
        != Some(true)
    {
        failures.push("emergency_flatten_not_marked_canary_risk_control_only".to_string());
    }
    if payload
        .get("emergency_flatten_is_strategy_pnl_path")
        .and_then(Value::as_bool)
        != Some(false)
    {
        failures.push("emergency_flatten_marked_as_strategy_pnl_path".to_string());
    }
    if payload
        .get("uses_shared_ingress_or_shared_ws")
        .and_then(Value::as_bool)
        != Some(false)
    {
        failures.push("uses_shared_ingress_or_shared_ws_not_false".to_string());
    }
    if payload.get("uses_c_artifacts").and_then(Value::as_bool) != Some(false) {
        failures.push("uses_c_artifacts_not_false".to_string());
    }

    failures
}

pub fn review_btc_completion_strategy_canary_proof_state(
    proof: &BtcCompletionStrategyCanaryProofState,
) -> BtcCompletionStrategyCanaryProofReview {
    let mut block_reasons = Vec::new();

    if !hash64(&proof.ex_ante_policy_payload_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidExAntePolicyPayloadHash,
        );
    }
    if !optional_hash64(&proof.external_position_reconciliation_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidExternalPositionReconciliationProof,
        );
    }
    if !proof
        .external_position_proof_tier
        .supports_approval_request()
    {
        block_reasons
            .push(BtcCompletionStrategyCanaryProofBlockReason::MissingExternalPositionProofTier);
    }
    if !optional_hash64(&proof.recovery_receipt_fixture_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidRecoveryReceiptProof,
        );
    }
    if !proof
        .recovery_receipt_proof_tier
        .supports_approval_request()
    {
        block_reasons
            .push(BtcCompletionStrategyCanaryProofBlockReason::MissingRecoveryReceiptProofTier);
    }
    if !optional_hash64(&proof.capital_lock_model_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidCapitalLockModelProof,
        );
    }
    if !proof.capital_lock_model_tier.supports_approval_request() {
        block_reasons
            .push(BtcCompletionStrategyCanaryProofBlockReason::MissingCapitalLockModelProofTier);
    }
    if !optional_hash64(&proof.inventory_residual_policy_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidInventoryResidualPolicyProof,
        );
    }
    if !proof
        .inventory_residual_policy_tier
        .supports_approval_request()
    {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingInventoryResidualPolicyProofTier,
        );
    }
    if !optional_hash64(&proof.no_open_order_remainder_proof_sha256) {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidNoOpenOrderRemainderProof,
        );
    }
    if !proof
        .no_open_order_remainder_proof_tier
        .supports_approval_request()
    {
        block_reasons.push(
            BtcCompletionStrategyCanaryProofBlockReason::MissingNoOpenOrderRemainderProofTier,
        );
    }

    let exact_approved_recovery_proof_present =
        proof.recovery_receipt_proof_tier == BtcCompletionProofTier::ExactApprovedPrimitiveReceipt;
    BtcCompletionStrategyCanaryProofReview {
        event: BTC_COMPLETION_STRATEGY_CANARY_PROOF_REVIEW_EVENT,
        proof_bundle_ready_to_request_exact_approval: block_reasons.is_empty(),
        exact_approved_recovery_proof_present,
        execution_evidence_complete: block_reasons.is_empty()
            && exact_approved_recovery_proof_present,
        block_reasons,
        fresh_exact_approval_still_required: true,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_recovery_primitive_wrapper_boundary(
    boundary: &BtcCompletionRecoveryPrimitiveWrapperBoundary,
) -> BtcCompletionRecoveryPrimitiveWrapperBoundaryReview {
    let mut block_reasons = Vec::new();

    if !hash64(&boundary.approval_draft_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingOrInvalidApprovalDraftHash,
        );
    }
    if !hash64(&boundary.packet_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingOrInvalidPacketHash,
        );
    }
    if !hash64(&boundary.proof_tier_state_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingOrInvalidProofTierStateHash,
        );
    }
    if boundary.selected_recovery_path.is_none() {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingSelectedRecoveryPath,
        );
    }
    if boundary.max_condition_count != 1 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::ConditionCountNotOne);
    }
    if boundary.max_recovery_tx_count != 1 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::RecoveryTxCountNotOne);
    }
    if boundary.max_order_count != 0 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::OrderCountNonZero);
    }
    if boundary.max_cancel_count != 0 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::CancelCountNonZero);
    }
    if boundary.preview_without_approval_exit_code != Some(66) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::PreviewWithoutApprovalNotExit66,
        );
    }
    if !boundary.no_secret_output_guard {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoSecretOutputGuard,
        );
    }
    if !boundary.no_raw_signature_output_guard {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoRawSignatureOutputGuard,
        );
    }
    if !boundary.no_shared_ingress_guard {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoSharedIngressGuard,
        );
    }
    if !boundary.no_c_artifacts_guard {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoCArtifactsGuard,
        );
    }
    if !boundary.draft_not_issued_marker {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingDraftNotIssuedMarker,
        );
    }
    if boundary.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::EffectfulExecutionPermittedBeforeFreshApproval,
        );
    }

    BtcCompletionRecoveryPrimitiveWrapperBoundaryReview {
        event: BTC_COMPLETION_RECOVERY_PRIMITIVE_WRAPPER_BOUNDARY_REVIEW_EVENT,
        wrapper_shape_ready_for_review_only_prebuild: block_reasons.is_empty(),
        block_reasons,
        fresh_exact_approval_still_required: true,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_recovery_primitive_prebuild(
    boundary: &BtcCompletionRecoveryPrimitiveWrapperBoundary,
    evidence: &BtcCompletionRecoveryPrimitivePrebuildEvidence,
) -> BtcCompletionRecoveryPrimitivePrebuildReview {
    let boundary_review = review_btc_completion_recovery_primitive_wrapper_boundary(boundary);
    let mut block_reasons = Vec::new();

    if !boundary_review.wrapper_shape_ready_for_review_only_prebuild {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::BoundaryShapeNotReady);
    }
    if !hash64(&evidence.wrapper_runtime_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::MissingOrInvalidWrapperRuntimeHash,
        );
    }
    if !hash64(&evidence.controller_source_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::MissingOrInvalidControllerSourceHash,
        );
    }
    if evidence.packet_sha256 != boundary.packet_sha256 {
        block_reasons.push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::PacketHashMismatch);
    }
    if evidence.approval_draft_sha256 != boundary.approval_draft_sha256 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::ApprovalDraftHashMismatch);
    }
    if evidence.proof_tier_state_sha256 != boundary.proof_tier_state_sha256 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::ProofTierStateHashMismatch);
    }
    if evidence.expected_preview_without_approval_exit_code != Some(66) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::PreviewWithoutApprovalNotExit66,
        );
    }
    if !evidence.wrapper_source_review_only {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperSourceNotReviewOnly);
    }
    if evidence.wrapper_loads_secrets {
        block_reasons.push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperLoadsSecrets);
    }
    if evidence.wrapper_emits_raw_signature {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperEmitsRawSignature);
    }
    if evidence.wrapper_uses_shared_ingress_or_ws {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperUsesSharedIngressOrWs);
    }
    if evidence.wrapper_uses_c_artifacts {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperUsesCArtifacts);
    }
    if evidence.wrapper_stages_effectful_execution {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperStagesEffectfulExecution,
        );
    }
    if evidence.wrapper_executes_order_or_cancel {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperExecutesOrderOrCancel);
    }
    if evidence.wrapper_executes_recovery_tx {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperExecutesRecoveryTxBeforeApproval,
        );
    }
    if evidence.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePrebuildBlockReason::EffectfulExecutionPermittedBeforeFreshApproval,
        );
    }

    BtcCompletionRecoveryPrimitivePrebuildReview {
        event: BTC_COMPLETION_RECOVERY_PRIMITIVE_PREBUILD_REVIEW_EVENT,
        prebuild_shape_ready_for_review_only_packet: block_reasons.is_empty(),
        block_reasons,
        fresh_exact_approval_still_required: true,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_recovery_primitive_preflight(
    boundary: &BtcCompletionRecoveryPrimitiveWrapperBoundary,
    prebuild: &BtcCompletionRecoveryPrimitivePrebuildEvidence,
    preflight: &BtcCompletionRecoveryPrimitivePreflightEvidence,
) -> BtcCompletionRecoveryPrimitivePreflightReview {
    let prebuild_review = review_btc_completion_recovery_primitive_prebuild(boundary, prebuild);
    let mut block_reasons = Vec::new();

    if !prebuild_review.prebuild_shape_ready_for_review_only_packet {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::PrebuildShapeNotReady);
    }
    if preflight.reviewed_host.trim() != "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com"
    {
        block_reasons.push(BtcCompletionRecoveryPrimitivePreflightBlockReason::HostNotReviewed);
    }
    if !preflight.ssh_preflight_fresh {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::SshPreflightNotFresh);
    }
    match preflight.ssh_preflight_age_ms {
        Some(age_ms) if age_ms <= preflight.max_ssh_preflight_age_ms => {}
        _ => block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::SshPreflightTooOld),
    }
    if preflight.remote_packet_sha256 != boundary.packet_sha256 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::RemotePacketHashMismatch);
    }
    if preflight.remote_wrapper_runtime_sha256 != prebuild.wrapper_runtime_sha256 {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteWrapperRuntimeHashMismatch,
        );
    }
    if preflight.remote_controller_source_sha256 != prebuild.controller_source_sha256 {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteControllerSourceHashMismatch,
        );
    }
    if preflight.preview_without_approval_exit_code != Some(66) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::PreviewWithoutApprovalNotExit66,
        );
    }
    if !preflight.no_order_auth_preview_passed {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::NoOrderAuthPreviewNotPassed);
    }
    if preflight.runtime_opens_ws {
        block_reasons.push(BtcCompletionRecoveryPrimitivePreflightBlockReason::RuntimeOpensWs);
    }
    if preflight.max_b_owned_direct_public_ws_connections > 1 {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::TooManyBOwnedDirectPublicWsConnections,
        );
    }
    if preflight.shared_ingress_dependency_detected {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::SharedIngressDependencyDetected,
        );
    }
    if !preflight.remote_b_process_table_clean {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteBProcessTableNotClean);
    }
    if preflight.secret_or_env_dump_detected {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::SecretOrEnvDumpDetected);
    }
    if preflight.raw_signature_output_detected {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::RawSignatureOutputDetected);
    }
    if preflight.effectful_command_executed {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::EffectfulCommandExecuted);
    }
    if preflight.recovery_tx_submitted {
        block_reasons.push(
            BtcCompletionRecoveryPrimitivePreflightBlockReason::RecoveryTxSubmittedBeforeFreshApproval,
        );
    }
    if preflight.order_or_cancel_submitted {
        block_reasons
            .push(BtcCompletionRecoveryPrimitivePreflightBlockReason::OrderOrCancelSubmitted);
    }

    BtcCompletionRecoveryPrimitivePreflightReview {
        event: BTC_COMPLETION_RECOVERY_PRIMITIVE_PREFLIGHT_REVIEW_EVENT,
        preflight_shape_ready_for_future_exact_run: block_reasons.is_empty(),
        block_reasons,
        fresh_exact_approval_still_required: true,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_recovery_primitive_execution_plan(
    boundary: &BtcCompletionRecoveryPrimitiveWrapperBoundary,
    prebuild: &BtcCompletionRecoveryPrimitivePrebuildEvidence,
    preflight: &BtcCompletionRecoveryPrimitivePreflightEvidence,
    plan: &BtcCompletionRecoveryPrimitiveExecutionPlan,
) -> BtcCompletionRecoveryPrimitiveExecutionPlanReview {
    let preflight_review =
        review_btc_completion_recovery_primitive_preflight(boundary, prebuild, preflight);
    let mut block_reasons = Vec::new();

    if !preflight_review.preflight_shape_ready_for_future_exact_run {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::PreflightShapeNotReady);
    }
    if !hash64(&plan.exact_approval_text_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::MissingOrInvalidExactApprovalTextHash,
        );
    }
    if !plan.exact_approval_text_present {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalTextAbsent);
    }
    if !plan.exact_approval_scope_matches_packet {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalScopeMismatch,
        );
    }
    if plan.exact_approval_text_is_draft_not_issued {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalTextStillDraftNotIssued,
        );
    }
    if plan.exact_approval_already_consumed {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalAlreadyConsumed,
        );
    }
    if plan.packet_sha256 != boundary.packet_sha256 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::PacketHashMismatch);
    }
    if plan.approval_draft_sha256 != boundary.approval_draft_sha256 {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ApprovalDraftHashMismatch,
        );
    }
    if !hash64(&plan.preflight_result_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::MissingOrInvalidPreflightResultHash,
        );
    }
    if plan.selected_recovery_path != boundary.selected_recovery_path {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::MissingOrMismatchedRecoveryPath,
        );
    }
    if plan.condition_id.trim().is_empty() {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::MissingConditionId);
    }
    if plan.max_condition_count != 1 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ConditionCountNotOne);
    }
    if plan.max_recovery_tx_count != 1 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::RecoveryTxCountNotOne);
    }
    if plan.max_order_count != 0 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::OrderCountNonZero);
    }
    if plan.max_cancel_count != 0 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CancelCountNonZero);
    }
    if plan.requests_funding_live_latest_or_deploy {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::FundingLiveLatestOrDeployRequested,
        );
    }
    if plan.uses_shared_ingress_or_shared_ws {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::SharedIngressOrSharedWsRequested,
        );
    }
    if plan.uses_c_artifacts {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CArtifactsRequested);
    }
    if plan.imports_candidate {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CandidateImportRequested);
    }
    if plan.prints_secret_or_raw_signature {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::SecretOrRawSignaturePrintRequested,
        );
    }
    if !plan.recovery_primitive_only {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::NotRecoveryPrimitiveOnly);
    }
    if plan.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::EffectfulExecutionPermittedBeforeRuntimeGate,
        );
    }

    BtcCompletionRecoveryPrimitiveExecutionPlanReview {
        event: BTC_COMPLETION_RECOVERY_PRIMITIVE_EXECUTION_PLAN_REVIEW_EVENT,
        execution_plan_lint_passed_for_fresh_exact_approval: block_reasons.is_empty(),
        block_reasons,
        runtime_preflight_still_required: true,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_recovery_primitive_run_result(
    boundary: &BtcCompletionRecoveryPrimitiveWrapperBoundary,
    prebuild: &BtcCompletionRecoveryPrimitivePrebuildEvidence,
    preflight: &BtcCompletionRecoveryPrimitivePreflightEvidence,
    plan: &BtcCompletionRecoveryPrimitiveExecutionPlan,
    result: &BtcCompletionRecoveryPrimitiveRunResultEvidence,
) -> BtcCompletionRecoveryPrimitiveRunResultReview {
    let plan_review = review_btc_completion_recovery_primitive_execution_plan(
        boundary, prebuild, preflight, plan,
    );
    let mut block_reasons = Vec::new();

    if !plan_review.execution_plan_lint_passed_for_fresh_exact_approval {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::ExecutionPlanLintNotPassed);
    }
    if !hash64(&result.execution_plan_review_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingOrInvalidExecutionPlanReviewHash,
        );
    }
    if result.exact_approval_text_sha256 != plan.exact_approval_text_sha256 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::ExactApprovalHashMismatch);
    }
    if result.condition_id != plan.condition_id {
        block_reasons.push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::ConditionIdMismatch);
    }
    if result.selected_recovery_path != plan.selected_recovery_path {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::RecoveryPathMismatch);
    }
    if !optional_hash64(&result.recovery_receipt_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingOrInvalidRecoveryReceiptHash,
        );
    }
    let exact_approved_recovery_receipt_present =
        result.recovery_receipt_proof_tier == BtcCompletionProofTier::ExactApprovedPrimitiveReceipt;
    if !exact_approved_recovery_receipt_present {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::RecoveryReceiptProofTierNotExactApproved,
        );
    }
    if result.submitted_recovery_tx_count != 1 {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::RecoveryTxCountNotOne);
    }
    if result.submitted_order_count != 0 {
        block_reasons.push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::OrderCountNonZero);
    }
    if result.submitted_cancel_count != 0 {
        block_reasons.push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::CancelCountNonZero);
    }
    if !result.receipt_id_present {
        block_reasons.push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingReceiptId);
    }
    if !hash64(&result.collateral_pre_observation_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingOrInvalidCollateralPreObservationHash,
        );
    }
    if !hash64(&result.collateral_post_observation_sha256) {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingOrInvalidCollateralPostObservationHash,
        );
    }
    if !result.collateral_delta_positive {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::CollateralDeltaNotPositive);
    }
    if !result.local_ledger_condition_matches {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::LocalLedgerConditionMismatch);
    }
    if !result.external_position_condition_matches {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::ExternalPositionConditionMismatch,
        );
    }
    if !result.receipt_factory_conversion_passed {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::ReceiptFactoryConversionMissing,
        );
    }
    if !result.verified_recovery_event_created {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::VerifiedRecoveryEventMissing);
    }
    if !result.inventory_sync_draft_review_only {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::InventorySyncDraftNotReviewOnly,
        );
    }
    if !result.no_open_order_remainder {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::OpenOrderRemainderPresent);
    }
    if !result.residual_exposure_zero {
        block_reasons
            .push(BtcCompletionRecoveryPrimitiveRunResultBlockReason::ResidualExposureNonZero);
    }
    if result.secret_or_raw_signature_output_detected {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::SecretOrRawSignatureOutputDetected,
        );
    }
    if result.shared_ingress_dependency_detected {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::SharedIngressDependencyDetected,
        );
    }
    if result.funding_live_latest_or_deploy_touched {
        block_reasons.push(
            BtcCompletionRecoveryPrimitiveRunResultBlockReason::FundingLiveLatestOrDeployTouched,
        );
    }

    BtcCompletionRecoveryPrimitiveRunResultReview {
        event: BTC_COMPLETION_RECOVERY_PRIMITIVE_RUN_RESULT_REVIEW_EVENT,
        run_result_receipt_reconciliation_passed: block_reasons.is_empty(),
        exact_approved_recovery_receipt_present,
        block_reasons,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_s8_micro_long_cycle_validation_plan(
    plan: &BtcCompletionS8MicroLongCycleValidationPlan,
) -> BtcCompletionS8MicroLongCycleValidationReview {
    let mut block_reasons = Vec::new();

    if !hash64(&plan.packet_draft_sha256) {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingOrInvalidPacketDraftHash,
        );
    }
    if !hash64(&plan.ex_ante_policy_sha256) {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingOrInvalidExAntePolicyHash,
        );
    }
    if !hash64(&plan.s7w_run_result_linter_sha256) {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingOrInvalidS7wRunResultLinterHash,
        );
    }
    if plan.scope_asset != "BTC" {
        block_reasons.push(BtcCompletionS8MicroLongCycleValidationBlockReason::NonBtcScope);
    }
    if plan.fixed_controller_policy != "cool5_imb1.25_source_guard_500" {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::FixedControllerPolicyMismatch,
        );
    }
    if !plan.source_guard_500_required {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::SourceGuard500Missing);
    }
    if plan.online_tuning_allowed {
        block_reasons.push(BtcCompletionS8MicroLongCycleValidationBlockReason::OnlineTuningAllowed);
    }
    if plan.strategy_discovery_allowed {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::StrategyDiscoveryAllowed);
    }
    if !plan.shadow_participation_target_rate.is_finite()
        || plan.shadow_participation_target_rate < 0.98
        || plan.shadow_participation_target_rate > 1.0
    {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::ParticipationTargetTooLow);
    }
    if !plan.shadow_participation_fail_below_rate.is_finite()
        || plan.shadow_participation_fail_below_rate < 0.95
        || plan.shadow_participation_fail_below_rate > plan.shadow_participation_target_rate
    {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::ParticipationFailThresholdTooLow,
        );
    }
    if !plan.replay_holdout_market_participation_rate.is_finite()
        || plan.replay_holdout_market_participation_rate < plan.shadow_participation_target_rate
        || plan.replay_holdout_market_participation_rate > 1.0
    {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::ReplayHoldoutParticipationBelowTarget,
        );
    }
    if plan.max_effectful_markets_total == 0
        || plan.max_effectful_markets_per_day == 0
        || plan.max_effectful_markets_per_day > plan.max_effectful_markets_total
    {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidEffectfulMarketCap);
    }
    if plan.max_order_submissions_total == 0 {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidOrderSubmissionCap);
    }
    if !plan.max_capital_lock_usdc.is_finite() || plan.max_capital_lock_usdc <= 0.0 {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidCapitalLockCap);
    }
    if !plan.max_one_leg_exposure_usdc.is_finite() || plan.max_one_leg_exposure_usdc <= 0.0 {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidOneLegExposureCap);
    }
    if plan.max_hold_duration_ms == 0 {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidHoldDurationCap);
    }
    if plan.max_emergency_flatten_count == 0 {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::InvalidEmergencyFlattenCap);
    }
    if !plan.emergency_flatten_is_risk_control_only {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::EmergencyFlattenNotRiskControlOnly,
        );
    }
    if plan.emergency_flatten_is_strategy_pnl_path {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::EmergencyFlattenAsStrategyPnlPath,
        );
    }
    if plan.required_recovery_receipt_tier != BtcCompletionProofTier::ExactApprovedPrimitiveReceipt
    {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::RecoveryReceiptTierNotExactApproved,
        );
    }
    if !plan.requires_positive_collateral_delta {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingPositiveCollateralDeltaRequirement,
        );
    }
    if !plan.requires_fee_inclusive_pnl_report {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingFeeInclusivePnlReportRequirement,
        );
    }
    if !plan.requires_local_ledger_external_position_alignment {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingLedgerExternalPositionAlignmentRequirement,
        );
    }
    if !plan.requires_no_open_order_remainder {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingNoOpenOrderRemainderRequirement,
        );
    }
    if !plan.requires_residual_exposure_zero {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingResidualExposureZeroRequirement,
        );
    }
    if !plan.stop_on_receipt_or_reconciliation_failure {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::MissingStopOnReceiptFailure);
    }
    if !plan.stop_on_participation_below_fail_threshold {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::MissingStopOnParticipationFailure,
        );
    }
    if !plan.exact_approval_required_per_batch {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::MissingExactApprovalBoundary);
    }
    if !plan.packet_draft_not_issued_marker {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::MissingDraftNotIssuedMarker);
    }
    if plan.uses_shared_ingress_or_shared_ws {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::SharedIngressOrSharedWsRequested,
        );
    }
    if plan.uses_c_artifacts {
        block_reasons.push(BtcCompletionS8MicroLongCycleValidationBlockReason::CArtifactsRequested);
    }
    if plan.requests_funding_live_latest_or_deploy {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::FundingLiveLatestOrDeployRequested,
        );
    }
    if plan.imports_candidate {
        block_reasons
            .push(BtcCompletionS8MicroLongCycleValidationBlockReason::CandidateImportRequested);
    }
    if plan.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionS8MicroLongCycleValidationBlockReason::EffectfulExecutionPermittedBeforeFreshApproval,
        );
    }

    BtcCompletionS8MicroLongCycleValidationReview {
        event: BTC_COMPLETION_S8_MICRO_LONG_CYCLE_VALIDATION_REVIEW_EVENT,
        s8_packet_ready_for_exact_approval_request: block_reasons.is_empty(),
        shadow_participation_guard_ready: plan.shadow_participation_target_rate >= 0.98
            && plan.shadow_participation_fail_below_rate >= 0.95
            && plan.replay_holdout_market_participation_rate
                >= plan.shadow_participation_target_rate,
        result_reconciliation_uses_s7w: hash64(&plan.s7w_run_result_linter_sha256)
            && plan.required_recovery_receipt_tier
                == BtcCompletionProofTier::ExactApprovedPrimitiveReceipt,
        block_reasons,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_s8a_execution_wrapper_prebuild(
    evidence: &BtcCompletionS8aExecutionWrapperPrebuildEvidence,
) -> BtcCompletionS8aExecutionWrapperPrebuildReview {
    let mut block_reasons = Vec::new();

    if !hash_matches(
        &evidence.loss_trigger_redesign_packet_sha256,
        BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::LossTriggerPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.partial_fill_addendum_sha256,
        BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PartialFillPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.three_round_cap_packet_sha256,
        BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ThreeRoundCapPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.s7w_run_result_linter_sha256,
        BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::S7wRunResultLinterHashMismatch,
        );
    }
    if !hash64(&evidence.wrapper_source_sha256) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingOrInvalidWrapperSourceHash,
        );
    }
    if !hash64(&evidence.controller_source_sha256) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingOrInvalidControllerSourceHash,
        );
    }
    if evidence.scope_asset != "BTC" {
        block_reasons.push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::NonBtcScope);
    }
    if evidence.market_family != "btc-5min" {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::NonBtc5mMarketFamily);
    }
    if evidence.fixed_controller_policy != "cool5_imb1.25_source_guard_500" {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::FixedControllerPolicyMismatch,
        );
    }
    if !evidence.source_guard_500_required {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SourceGuard500Missing);
    }
    if (evidence.order_size - 5.0).abs() > 1e-9 {
        block_reasons.push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidOrderSize);
    }
    if (evidence.api_min_order_size - 5.0).abs() > 1e-9
        || evidence.order_size + 1e-9 < evidence.api_min_order_size
    {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ApiMinimumOrderSizeMismatch);
    }
    if (evidence.seed_px_hi - 0.80).abs() > 1e-9 {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SeedPriceHighMismatch);
    }
    if (evidence.l1_pair_ask_max - 1.02).abs() > 1e-9 {
        block_reasons.push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PairAskCapMismatch);
    }
    if evidence.max_round_count != 3
        || (evidence.session_hard_loss_cap_usdc - 15.0).abs() > 1e-9
        || (evidence.per_round_theoretical_max_loss_usdc - 5.0).abs() > 1e-9
    {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidRoundOrLossCaps);
    }
    if evidence.target_runtime_ms == 0 || evidence.target_runtime_ms > 14_400_000 {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidRuntimeDurationCap);
    }
    if evidence.max_active_market_count != 1 {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidActiveMarketCap);
    }
    if (evidence.max_active_capital_lock_usdc - 6.0).abs() > 1e-9
        || (evidence.max_cumulative_gross_quote_spend_usdc - 18.0).abs() > 1e-9
    {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidCapitalOrSpendCaps);
    }
    if evidence.max_initial_buy_submissions != 6
        || evidence.max_emergency_exit_or_hedge_submissions != 3
        || evidence.max_total_order_submissions != 9
        || evidence.max_cancel_count != 6
        || evidence.max_recovery_tx_count != 3
    {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidOrderCancelOrRecoveryCaps,
        );
    }
    if !evidence.shadow_participation_pass_rate.is_finite()
        || evidence.shadow_participation_pass_rate > 0.75
        || evidence.shadow_participation_pass_rate <= 0.0
    {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ParticipationPassThresholdTooHigh,
        );
    }
    if !evidence.shadow_participation_warning_below_rate.is_finite()
        || evidence.shadow_participation_warning_below_rate <= 0.0
        || evidence.shadow_participation_warning_below_rate
            >= evidence.shadow_participation_pass_rate
    {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ParticipationWarningThresholdInvalid,
        );
    }
    if !evidence.roi_report_only_for_first_s8a
        || !evidence.residual_cost_rate_report_only_for_first_s8a
        || evidence.minimum_roi_hard_gate_enabled
    {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::RoiOrResidualIncorrectlyHardGated,
        );
    }
    if !evidence.uses_actual_filled_qty_inventory {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ActualFilledQtyInventoryMissing,
        );
    }
    if evidence.submitted_size_counts_as_inventory {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SubmittedSizeCountsAsInventory,
        );
    }
    if evidence.partial_fill_threshold_enabled {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PartialFillThresholdEnabled);
    }
    if evidence.forced_complement_after_partial_fill {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ForcedComplementAfterPartialFill,
        );
    }
    if evidence.forced_complement_without_natural_signal {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ForcedComplementWithoutNaturalSignal,
        );
    }
    if !evidence.api_min_order_size_enforced {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ApiMinimumOrderSizeNotEnforced,
        );
    }
    if !evidence.sub_min_residual_sell_blocked {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SubMinResidualSellAllowed);
    }
    if !evidence.no_new_round_while_active_market {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::NewRoundAllowedWhileActiveMarket,
        );
    }
    if !evidence.pre_entry_realized_loss_plus_round_cap_guard {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingPreEntryLossGuard);
    }
    if !evidence.active_risk_at_work_cap_guard {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingActiveRiskAtWorkGuard,
        );
    }
    if !evidence.realized_loss_only_after_close_or_recovery {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::RealizedLossDefinedBeforeClosure,
        );
    }
    if !evidence.emergency_flatten_is_risk_control_only {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::EmergencyFlattenNotRiskControlOnly,
        );
    }
    if evidence.emergency_flatten_is_strategy_pnl_path {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::EmergencyFlattenAsStrategyPnlPath,
        );
    }
    if !evidence.s7w_reconciliation_required {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingS7wReconciliationRequirement,
        );
    }
    if !evidence.exact_approval_required {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingExactApprovalRequirement,
        );
    }
    if !evidence.draft_not_issued_marker {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingDraftNotIssuedMarker);
    }
    if evidence.preview_without_approval_exit_code != Some(66) {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PreviewWithoutApprovalExitNot66,
        );
    }
    if !evidence.no_order_auth_preview_required {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingNoOrderAuthPreviewRequirement,
        );
    }
    if evidence.runtime_opens_own_ws {
        block_reasons.push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::RuntimeOpensOwnWs);
    }
    if evidence.max_b_owned_direct_public_ws_connections > 1 {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::TooManyBOwnedDirectPublicWs);
    }
    if evidence.uses_shared_ingress_or_shared_ws {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SharedIngressOrSharedWsRequested,
        );
    }
    if evidence.uses_c_artifacts {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::CArtifactsRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.imports_candidate {
        block_reasons
            .push(BtcCompletionS8aExecutionWrapperPrebuildBlockReason::CandidateImportRequested);
    }
    if evidence.prints_secret_or_raw_signature {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SecretOrRawSignatureOutputAllowed,
        );
    }
    if evidence.requests_funding_live_latest_or_deploy {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::FundingLiveLatestOrDeployRequested,
        );
    }
    if evidence.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionS8aExecutionWrapperPrebuildBlockReason::EffectfulExecutionPermittedBeforeFreshApproval,
        );
    }

    let s8a_hash_boundary_bound = hash_matches(
        &evidence.loss_trigger_redesign_packet_sha256,
        BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256,
    ) && hash_matches(
        &evidence.partial_fill_addendum_sha256,
        BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256,
    ) && hash_matches(
        &evidence.three_round_cap_packet_sha256,
        BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256,
    ) && hash_matches(
        &evidence.s7w_run_result_linter_sha256,
        BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256,
    );
    let s8a_runtime_semantics_bound = evidence.scope_asset == "BTC"
        && evidence.market_family == "btc-5min"
        && evidence.fixed_controller_policy == "cool5_imb1.25_source_guard_500"
        && evidence.source_guard_500_required
        && (evidence.order_size - 5.0).abs() <= 1e-9
        && evidence.uses_actual_filled_qty_inventory
        && !evidence.submitted_size_counts_as_inventory
        && !evidence.partial_fill_threshold_enabled
        && !evidence.forced_complement_after_partial_fill
        && !evidence.forced_complement_without_natural_signal
        && evidence.no_new_round_while_active_market
        && evidence.pre_entry_realized_loss_plus_round_cap_guard
        && evidence.active_risk_at_work_cap_guard
        && evidence.realized_loss_only_after_close_or_recovery
        && evidence.s7w_reconciliation_required;

    BtcCompletionS8aExecutionWrapperPrebuildReview {
        event: BTC_COMPLETION_S8A_EXECUTION_WRAPPER_PREBUILD_REVIEW_EVENT,
        wrapper_prebuild_ready_for_exact_approval_request: block_reasons.is_empty(),
        s8a_hash_boundary_bound,
        s8a_runtime_semantics_bound,
        block_reasons,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

pub fn review_btc_completion_s8a_effectful_runtime_readiness(
    evidence: &BtcCompletionS8aEffectfulRuntimeReadinessEvidence,
) -> BtcCompletionS8aEffectfulRuntimeReadinessReview {
    let mut block_reasons = Vec::new();

    if !evidence.prebuild_review_passed {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PrebuildReviewNotPassed);
    }
    if !hash_matches(
        &evidence.loss_trigger_redesign_packet_sha256,
        BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::LossTriggerPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.partial_fill_addendum_sha256,
        BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PartialFillPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.three_round_cap_packet_sha256,
        BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ThreeRoundCapPacketHashMismatch,
        );
    }
    if !hash_matches(
        &evidence.s7w_run_result_linter_sha256,
        BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256,
    ) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wRunResultLinterHashMismatch,
        );
    }
    if !hash64(&evidence.wrapper_source_sha256) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MissingOrInvalidWrapperSourceHash,
        );
    }
    if !hash64(&evidence.controller_source_sha256) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MissingOrInvalidControllerSourceHash,
        );
    }
    if !evidence.wrapper_supports_preview_without_approval {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PreviewWithoutApprovalUnavailable,
        );
    }
    if evidence.preview_without_approval_exit_code != Some(66) {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PreviewWithoutApprovalExitNot66,
        );
    }
    if !evidence.wrapper_supports_no_order_auth_preview {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NoOrderAuthPreviewUnavailable,
        );
    }
    if !evidence.no_order_auth_preview_uses_reviewed_auth_path {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NoOrderAuthPreviewNotReviewedAuthPath,
        );
    }
    if evidence.no_order_auth_preview_prints_secret_or_signature {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NoOrderAuthPreviewSecretOrSignatureOutput,
        );
    }
    if !evidence.wrapper_supports_exact_order_path {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ExactOrderPathUnavailable);
    }
    if !evidence.exact_order_path_requires_approval_hash {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ExactOrderPathMissingApprovalHashGate,
        );
    }
    if !evidence.exact_order_path_rejects_missing_or_mismatched_approval {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ExactOrderPathDoesNotRejectApprovalMismatch,
        );
    }
    if !evidence.uses_native_s8a_order_adapter {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NativeS8aOrderAdapterMissing,
        );
    }
    if (evidence.limit_order_min_size_shares - BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES).abs()
        > 1e-9
        || (evidence.limit_entry_order_size_shares - BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES)
            .abs()
            > 1e-9
        || evidence.limit_entry_order_size_shares + 1e-9 < evidence.limit_order_min_size_shares
    {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidLimitOrderMinimums);
    }
    if (evidence.market_buy_min_notional_usdc - BTC_COMPLETION_S8A_MARKET_BUY_MIN_NOTIONAL_USDC)
        .abs()
        > 1e-9
    {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidMarketBuyMinimumNotional,
        );
    }
    if !evidence.market_buy_amount_is_usdc {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MarketBuyAmountUnitNotUsdc);
    }
    if !evidence.market_sell_amount_is_shares {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MarketSellAmountUnitNotShares,
        );
    }
    if (evidence.market_sell_min_size_shares - BTC_COMPLETION_S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES)
        .abs()
        > 1e-9
    {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidMarketSellMinimumShares,
        );
    }
    if !evidence.sub_min_residual_market_sell_blocked {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SubMinResidualMarketSellNotBlocked,
        );
    }
    if !evidence.order_minimums_verified_from_official_clob_market_info
        || !evidence.order_amount_units_verified_from_official_order_docs
    {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::OrderMinimumsNotVerifiedFromOfficialSources,
        );
    }
    if !evidence.live_public_buy_signal_source_bound {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::LivePublicBuySignalSourceMissing,
        );
    }
    if !evidence.btc_5m_target_derivation_bound {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::Btc5mTargetDerivationMissing,
        );
    }
    if !evidence.source_guard_500_live_enforced {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SourceGuard500NotLiveEnforced,
        );
    }
    if !evidence.uses_s8a_controller_for_every_admission {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S8aControllerNotUsedForEveryAdmission,
        );
    }
    if !evidence.uses_actual_filled_qty_inventory {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ActualFilledQtyInventoryMissing,
        );
    }
    if evidence.submitted_size_counts_as_inventory {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SubmittedSizeCountsAsInventory,
        );
    }
    if evidence.partial_fill_threshold_enabled {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PartialFillThresholdEnabled,
        );
    }
    if evidence.forced_complement_without_natural_signal {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ForcedComplementWithoutNaturalSignal,
        );
    }
    if evidence.max_round_count != 3
        || (evidence.session_hard_loss_cap_usdc - 15.0).abs() > 1e-9
        || (evidence.per_round_theoretical_max_loss_usdc - 5.0).abs() > 1e-9
        || (evidence.order_size - 5.0).abs() > 1e-9
        || evidence.max_active_market_count != 1
    {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidRoundLossOrSizeCaps);
    }
    if evidence.max_initial_buy_submissions != 6
        || evidence.max_total_order_submissions != 9
        || evidence.max_cancel_count != 6
        || evidence.max_recovery_tx_count != 3
    {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidOrderCancelOrRecoveryCaps,
        );
    }
    if !evidence.s7w_post_run_reconciliation_bound {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wPostRunReconciliationMissing,
        );
    }
    if !evidence.s7w_requires_exact_approved_receipt_tier {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wExactApprovedReceiptTierMissing,
        );
    }
    if !evidence.s7w_rejects_review_only_fixture_receipt {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wReviewOnlyFixtureReceiptAccepted,
        );
    }
    if evidence.runtime_uses_s1a_same_run_flatten_plan {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ReusesS1aSameRunFlattenPlan,
        );
    }
    if evidence.runtime_uses_s3a_admission_or_pair_rank {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ReusesS3aAdmissionOrPairRank,
        );
    }
    if evidence.runtime_opens_own_ws {
        block_reasons.push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::RuntimeOpensOwnWs);
    }
    if evidence.max_b_owned_direct_public_ws_connections > 1 {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::TooManyBOwnedDirectPublicWs,
        );
    }
    if evidence.uses_shared_ingress_or_shared_ws {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SharedIngressOrSharedWsRequested,
        );
    }
    if evidence.uses_c_artifacts {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::CArtifactsRequested);
    }
    if evidence.imports_candidate {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::CandidateImportRequested);
    }
    if evidence.online_tuning_allowed {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::OnlineTuningAllowed);
    }
    if evidence.strategy_discovery_allowed {
        block_reasons
            .push(BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::StrategyDiscoveryAllowed);
    }
    if evidence.prints_secret_or_raw_signature {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SecretOrRawSignatureOutputAllowed,
        );
    }
    if evidence.requests_funding_live_latest_or_deploy {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::FundingLiveLatestOrDeployRequested,
        );
    }
    if evidence.effectful_execution_permitted_now {
        block_reasons.push(
            BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::EffectfulExecutionPermittedBeforeFreshApproval,
        );
    }

    let order_minimums_bound = (evidence.limit_order_min_size_shares
        - BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES)
        .abs()
        <= 1e-9
        && (evidence.limit_entry_order_size_shares
            - BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES)
            .abs()
            <= 1e-9
        && (evidence.market_buy_min_notional_usdc
            - BTC_COMPLETION_S8A_MARKET_BUY_MIN_NOTIONAL_USDC)
            .abs()
            <= 1e-9
        && evidence.market_buy_amount_is_usdc
        && evidence.market_sell_amount_is_shares
        && (evidence.market_sell_min_size_shares
            - BTC_COMPLETION_S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES)
            .abs()
            <= 1e-9
        && evidence.sub_min_residual_market_sell_blocked
        && evidence.order_minimums_verified_from_official_clob_market_info
        && evidence.order_amount_units_verified_from_official_order_docs;

    let native_s8a_runtime_bound = evidence.wrapper_supports_exact_order_path
        && evidence.uses_native_s8a_order_adapter
        && order_minimums_bound
        && evidence.live_public_buy_signal_source_bound
        && evidence.btc_5m_target_derivation_bound
        && evidence.source_guard_500_live_enforced
        && evidence.uses_s8a_controller_for_every_admission
        && evidence.uses_actual_filled_qty_inventory
        && !evidence.submitted_size_counts_as_inventory
        && !evidence.partial_fill_threshold_enabled
        && !evidence.forced_complement_without_natural_signal
        && !evidence.runtime_uses_s1a_same_run_flatten_plan
        && !evidence.runtime_uses_s3a_admission_or_pair_rank;

    let live_source_and_auth_preview_bound = evidence.wrapper_supports_preview_without_approval
        && evidence.preview_without_approval_exit_code == Some(66)
        && evidence.wrapper_supports_no_order_auth_preview
        && evidence.no_order_auth_preview_uses_reviewed_auth_path
        && !evidence.no_order_auth_preview_prints_secret_or_signature
        && !evidence.runtime_opens_own_ws
        && evidence.max_b_owned_direct_public_ws_connections <= 1
        && !evidence.uses_shared_ingress_or_shared_ws;

    BtcCompletionS8aEffectfulRuntimeReadinessReview {
        event: BTC_COMPLETION_S8A_EFFECTFUL_RUNTIME_READINESS_REVIEW_EVENT,
        runtime_ready_for_exact_approval_request: block_reasons.is_empty(),
        native_s8a_runtime_bound,
        live_source_and_auth_preview_bound,
        block_reasons,
        effectful_execution_permitted:
            BTC_COMPLETION_STRATEGY_CANARY_GATE_EFFECTFUL_EXECUTION_DEFAULT,
    }
}

fn finite_positive(value: Option<f64>) -> bool {
    value.is_some_and(|value| value.is_finite() && value > 0.0)
}

fn finite_ratio(value: Option<f64>) -> bool {
    value.is_some_and(|value| value.is_finite() && (0.0..=1.0).contains(&value))
}

fn optional_hash64(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(hash64)
}

fn hash64(value: &str) -> bool {
    let value = value.trim();
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hash_matches(value: &str, expected: &str) -> bool {
    let value = value.trim();
    value == expected && hash64(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn candidate() -> BtcCompletionCandidate {
        BtcCompletionCandidate {
            asset: "BTC".to_string(),
            event_kind: "public_trade".to_string(),
            public_trade_taker_side: "BUY".to_string(),
            condition_id: "0xcondition".to_string(),
            slug: "btc-updown-5m-test".to_string(),
            ts_ms: 1_780_000_000_000,
            offset_s: 60.0,
            time_to_end_s: Some(240.0),
            side: Side::Yes,
            public_trade_price: 0.555,
            l1_pair_ask: 1.00,
            l1_pair_available_qty: 2.0,
            buy_available_qty: 2.0,
            strict_l1_age_ms: Some(40),
            strict_l2_age_ms: Some(80),
            public_trade_recv_lag_ms: Some(11),
            forbidden_decision_fields_present: false,
            snapshot_index_used_as_rank: false,
        }
    }

    fn sampled_candidate_base_row(
        candidate_row_id: u64,
        side: Side,
        ts_ms: u64,
        offset_s: f64,
        public_trade_price: f64,
        l1_pair_ask: f64,
        l1_pair_available_qty: f64,
        buy_available_qty: f64,
        public_trade_recv_lag_ms: u64,
    ) -> BtcCompletionCandidate {
        BtcCompletionCandidate {
            asset: "BTC".to_string(),
            event_kind: "public_trade".to_string(),
            public_trade_taker_side: "BUY".to_string(),
            condition_id: "0x2d2beeb33a1b467f56be74633e066d54203f96ccf9d18b03f706f0ae802a5b5e"
                .to_string(),
            slug: format!("btc-updown-5m-1777680000-row-{candidate_row_id}"),
            ts_ms,
            offset_s,
            time_to_end_s: Some(300.0 - offset_s),
            side,
            public_trade_price,
            l1_pair_ask,
            l1_pair_available_qty,
            buy_available_qty,
            strict_l1_age_ms: Some(5),
            strict_l2_age_ms: None,
            public_trade_recv_lag_ms: Some(public_trade_recv_lag_ms),
            forbidden_decision_fields_present: false,
            snapshot_index_used_as_rank: false,
        }
    }

    #[test]
    fn accepts_valid_candidate_as_review_only_intent() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let review = review_btc_completion_candidate(
            &cfg,
            &BtcCompletionControllerState::default(),
            &candidate(),
        );

        let intent = review
            .intent
            .expect("valid candidate should produce intent");
        assert_eq!(intent.condition_id, "0xcondition");
        assert_eq!(intent.side, Side::Yes);
        assert!((intent.price - 0.5).abs() < 1e-9);
        assert_eq!(intent.qty, 1.25);
        assert!(!intent.execution_permitted);
        assert!(!review.execution_permitted);
    }

    #[test]
    fn rejects_forbidden_future_or_evaluation_fields() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut c = candidate();
        c.forbidden_decision_fields_present = true;

        let review =
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c);

        assert_eq!(
            review.block_reason,
            Some(BtcCompletionControllerBlockReason::ForbiddenLeakageFieldPresent)
        );
        assert!(!review.execution_permitted);
    }

    #[test]
    fn rejects_snapshot_index_rank_usage() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut c = candidate();
        c.snapshot_index_used_as_rank = true;

        let review =
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c);

        assert_eq!(
            review.block_reason,
            Some(BtcCompletionControllerBlockReason::SnapshotIndexRankUsed)
        );
    }

    #[test]
    fn rejects_late_or_non_btc_candidates() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut c = candidate();
        c.offset_s = 121.0;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::OffsetTooLate)
        );

        c = candidate();
        c.asset = "ETH".to_string();
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::NonBtcAsset)
        );
    }

    #[test]
    fn rejects_insufficient_liquidity_or_pair_cost() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut c = candidate();
        c.buy_available_qty = 1.0;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::InsufficientLiquidity)
        );

        c = candidate();
        c.l1_pair_ask = 1.021;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::PairAskAboveCap)
        );
    }

    #[test]
    fn source_guard_500_fails_closed() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut c = candidate();
        c.strict_l1_age_ms = None;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::MissingStrictL1Age)
        );

        c = candidate();
        c.strict_l1_age_ms = Some(501);
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::StrictL1AgeTooStale)
        );

        c = candidate();
        c.public_trade_recv_lag_ms = None;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::MissingPublicTradeRecvLag)
        );

        c = candidate();
        c.public_trade_recv_lag_ms = Some(501);
        assert_eq!(
            review_btc_completion_candidate(&cfg, &BtcCompletionControllerState::default(), &c)
                .block_reason,
            Some(BtcCompletionControllerBlockReason::PublicTradeRecvLagTooHigh)
        );
    }

    #[test]
    fn cooldown_and_imbalance_fail_closed() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut state = BtcCompletionControllerState::default();
        let first = review_btc_completion_candidate(&cfg, &state, &candidate())
            .intent
            .expect("first candidate should pass");
        state.record_accepted(&first);

        let mut second = candidate();
        second.ts_ms += 1_000;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &second).block_reason,
            Some(BtcCompletionControllerBlockReason::CooldownActive)
        );

        second.ts_ms += 6_000;
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &second).block_reason,
            Some(BtcCompletionControllerBlockReason::InventoryImbalanceWouldExceedCap)
        );

        second.side = Side::No;
        let review = review_btc_completion_candidate(&cfg, &state, &second);
        assert!(review.intent.is_some());
    }

    #[test]
    fn sampled_candidate_base_rows_match_expected_boundary_decisions() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let state = BtcCompletionControllerState::default();

        let row1 = sampled_candidate_base_row(
            1,
            Side::Yes,
            1_777_680_000_520,
            0.520,
            0.49,
            1.01,
            66.34,
            73.79,
            11,
        );
        assert!(review_btc_completion_candidate(&cfg, &state, &row1)
            .intent
            .is_some());

        let row20_pair_cap = sampled_candidate_base_row(
            20,
            Side::Yes,
            1_777_680_003_375,
            3.375,
            0.50,
            1.03,
            23.99,
            23.99,
            11,
        );
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &row20_pair_cap).block_reason,
            Some(BtcCompletionControllerBlockReason::PairAskAboveCap)
        );

        let row47_liquidity = sampled_candidate_base_row(
            47,
            Side::Yes,
            1_777_680_009_401,
            9.401,
            0.53,
            1.01,
            1.20,
            272.14,
            11,
        );
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &row47_liquidity).block_reason,
            Some(BtcCompletionControllerBlockReason::InsufficientLiquidity)
        );

        let row311_late = sampled_candidate_base_row(
            311,
            Side::Yes,
            1_777_680_120_480,
            120.480,
            0.66,
            1.01,
            24.42,
            24.42,
            11,
        );
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &row311_late).block_reason,
            Some(BtcCompletionControllerBlockReason::OffsetTooLate)
        );

        let row8191_bad_seed_px = BtcCompletionCandidate {
            condition_id: "0x1e119d38bec6fdd3596df17a4398b10fd620f1866c9caa935f1e2fba21293fb4"
                .to_string(),
            slug: "btc-updown-5m-1777683000-row-8191".to_string(),
            ..sampled_candidate_base_row(
                8191,
                Side::Yes,
                1_777_683_067_155,
                67.155,
                0.87,
                1.02,
                15.41,
                15.41,
                11,
            )
        };
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &row8191_bad_seed_px).block_reason,
            Some(BtcCompletionControllerBlockReason::InvalidSeedPrice)
        );
    }

    #[test]
    fn sampled_candidate_base_sequence_enforces_cooldown_before_pairing() {
        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut state = BtcCompletionControllerState::default();

        let row1 = sampled_candidate_base_row(
            1,
            Side::Yes,
            1_777_680_000_520,
            0.520,
            0.49,
            1.01,
            66.34,
            73.79,
            11,
        );
        let intent = review_btc_completion_candidate(&cfg, &state, &row1)
            .intent
            .expect("row 1 should pass from empty state");
        state.record_accepted(&intent);

        let row2 = sampled_candidate_base_row(
            2,
            Side::No,
            1_777_680_000_889,
            0.889,
            0.52,
            1.01,
            71.74,
            111.32,
            11,
        );
        assert_eq!(
            review_btc_completion_candidate(&cfg, &state, &row2).block_reason,
            Some(BtcCompletionControllerBlockReason::CooldownActive)
        );
    }

    #[test]
    fn s7l_strategy_canary_gate_blocks_unbound_recovery_policy() {
        let policy = BtcCompletionStrategyCanaryGatePolicy {
            scope_asset: "BTC".to_string(),
            controller_policy_sha256: String::new(),
            replay_result_sha256: String::new(),
            recovery_policy_sha256: None,
            recovery_path: None,
            max_capital_lock_usdc: None,
            max_capital_lock_ms: None,
            max_residual_cost_rate: None,
            max_bad_pair_cost_ge_1_share: None,
            max_one_leg_exposure_usdc: None,
            requires_source_guard_500: false,
            requires_same_market_yes_no_close_window: false,
            requires_external_position_reconciliation: false,
            requires_recovery_receipt_factory: false,
            requires_no_open_order_remainder: false,
            allows_same_run_flatten_as_primary_exit: true,
        };

        let review = review_btc_completion_strategy_canary_gate(&policy);

        assert!(!review.hash_bound_policy_ready);
        assert!(!review.future_exact_canary_policy_ready);
        assert!(!review.effectful_execution_permitted);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionStrategyCanaryGateBlockReason::MissingControllerPolicyHash));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionStrategyCanaryGateBlockReason::MissingRecoveryPath));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionStrategyCanaryGateBlockReason::SameRunFlattenPrimaryExit));
    }

    #[test]
    fn s7l_strategy_canary_gate_accepts_hash_bound_hold_merge_policy_review_only() {
        let review = review_btc_completion_strategy_canary_gate(&complete_s7l_policy());

        assert_eq!(
            review.event,
            BTC_COMPLETION_STRATEGY_CANARY_GATE_REVIEW_EVENT
        );
        assert!(review.hash_bound_policy_ready);
        assert!(review.future_exact_canary_policy_ready);
        assert!(review.block_reasons.is_empty());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7l_strategy_canary_gate_rejects_same_run_flatten_primary_exit() {
        let mut policy = complete_s7l_policy();
        policy.allows_same_run_flatten_as_primary_exit = true;

        let review = review_btc_completion_strategy_canary_gate(&policy);

        assert!(!review.future_exact_canary_policy_ready);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionStrategyCanaryGateBlockReason::SameRunFlattenPrimaryExit));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7l_strategy_canary_gate_rejects_missing_recovery_proof_contracts() {
        let mut policy = complete_s7l_policy();
        policy.requires_external_position_reconciliation = false;
        policy.requires_recovery_receipt_factory = false;

        let review = review_btc_completion_strategy_canary_gate(&policy);

        assert!(!review.future_exact_canary_policy_ready);
        assert!(review.block_reasons.contains(
            &BtcCompletionStrategyCanaryGateBlockReason::MissingExternalPositionReconciliation
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionStrategyCanaryGateBlockReason::MissingRecoveryReceiptFactory));
    }

    #[test]
    fn s7m_ex_ante_policy_payload_schema_accepts_complete_review_only_policy() {
        let payload = btc_completion_ex_ante_policy_payload_review_only(
            "BTC_CORE_COMPLETION_V1",
            &complete_s7l_policy(),
        );

        assert!(btc_completion_ex_ante_policy_payload_schema_failures(&payload).is_empty());
        assert_eq!(payload["strategy_lane"], "BTC_CORE_COMPLETION_V1");
        assert_eq!(payload["strategy_research_only"], true);
        assert_eq!(payload["scope_asset"], "BTC");
        assert_eq!(payload["recovery_path"], "COMPLETE_SET_MERGE");
        assert_eq!(payload["future_exact_canary_policy_ready"], true);
        assert_eq!(payload["effectful_execution_permitted"], false);
        assert_eq!(
            payload["emergency_flatten_is_canary_risk_control_only"],
            true
        );
        assert_eq!(payload["emergency_flatten_is_strategy_pnl_path"], false);
        assert_eq!(payload["uses_shared_ingress_or_shared_ws"], false);
        assert_eq!(payload["uses_c_artifacts"], false);
        assert_eq!(
            payload["block_reasons"]
                .as_array()
                .expect("block_reasons should be an array")
                .len(),
            0
        );
    }

    #[test]
    fn s7m_ex_ante_policy_payload_schema_fails_closed_on_missing_key_or_execution_permission() {
        let mut payload = btc_completion_ex_ante_policy_payload_review_only(
            "BTC_CORE_COMPLETION_V1",
            &complete_s7l_policy(),
        );
        payload
            .as_object_mut()
            .expect("payload should be an object")
            .remove("controller_policy_sha256");

        let failures = btc_completion_ex_ante_policy_payload_schema_failures(&payload);
        assert!(failures.contains(&"missing_key:controller_policy_sha256".to_string()));

        let mut payload = btc_completion_ex_ante_policy_payload_review_only(
            "BTC_CORE_COMPLETION_V1",
            &complete_s7l_policy(),
        );
        payload
            .as_object_mut()
            .expect("payload should be an object")
            .insert("effectful_execution_permitted".to_string(), json!(true));

        let failures = btc_completion_ex_ante_policy_payload_schema_failures(&payload);
        assert!(failures.contains(&"effectful_execution_permitted_not_false".to_string()));

        let mut payload = btc_completion_ex_ante_policy_payload_review_only(
            "BTC_CORE_COMPLETION_V1",
            &complete_s7l_policy(),
        );
        payload
            .as_object_mut()
            .expect("payload should be an object")
            .insert(
                "emergency_flatten_is_strategy_pnl_path".to_string(),
                json!(true),
            );

        let failures = btc_completion_ex_ante_policy_payload_schema_failures(&payload);
        assert!(failures.contains(&"emergency_flatten_marked_as_strategy_pnl_path".to_string()));
    }

    #[test]
    fn s7m_ex_ante_policy_payload_keeps_block_reasons_for_incomplete_policy() {
        let mut policy = complete_s7l_policy();
        policy.recovery_policy_sha256 = None;
        policy.allows_same_run_flatten_as_primary_exit = true;
        let payload =
            btc_completion_ex_ante_policy_payload_review_only("BTC_CORE_COMPLETION_V1", &policy);

        assert!(btc_completion_ex_ante_policy_payload_schema_failures(&payload).is_empty());
        assert_eq!(payload["hash_bound_policy_ready"], false);
        assert_eq!(payload["future_exact_canary_policy_ready"], false);
        let reasons = payload["block_reasons"]
            .as_array()
            .expect("block_reasons should be an array")
            .iter()
            .map(|value| value.as_str().expect("reason should be string"))
            .collect::<Vec<_>>();
        assert!(reasons.contains(&"BLOCK_MISSING_RECOVERY_POLICY_HASH"));
        assert!(reasons.contains(&"BLOCK_SAME_RUN_FLATTEN_PRIMARY_EXIT"));
        assert_eq!(payload["effectful_execution_permitted"], false);
    }

    #[test]
    fn s7n_strategy_canary_proof_state_blocks_missing_proofs() {
        let proof = BtcCompletionStrategyCanaryProofState {
            ex_ante_policy_payload_sha256: String::new(),
            external_position_reconciliation_sha256: None,
            external_position_proof_tier: BtcCompletionProofTier::Unavailable,
            recovery_receipt_fixture_sha256: None,
            recovery_receipt_proof_tier: BtcCompletionProofTier::Unavailable,
            capital_lock_model_sha256: None,
            capital_lock_model_tier: BtcCompletionProofTier::Unavailable,
            inventory_residual_policy_sha256: None,
            inventory_residual_policy_tier: BtcCompletionProofTier::Unavailable,
            no_open_order_remainder_proof_sha256: None,
            no_open_order_remainder_proof_tier: BtcCompletionProofTier::Unavailable,
        };

        let review = review_btc_completion_strategy_canary_proof_state(&proof);

        assert_eq!(
            review.event,
            BTC_COMPLETION_STRATEGY_CANARY_PROOF_REVIEW_EVENT
        );
        assert!(!review.proof_bundle_ready_to_request_exact_approval);
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
        assert!(review.block_reasons.contains(
            &BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidExAntePolicyPayloadHash
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidRecoveryReceiptProof
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionStrategyCanaryProofBlockReason::MissingRecoveryReceiptProofTier
        ));
    }

    #[test]
    fn s7n_strategy_canary_proof_state_accepts_hash_bound_bundle_review_only() {
        let review = review_btc_completion_strategy_canary_proof_state(&complete_s7n_proof_state());

        assert!(review.block_reasons.is_empty());
        assert!(review.proof_bundle_ready_to_request_exact_approval);
        assert!(!review.exact_approved_recovery_proof_present);
        assert!(!review.execution_evidence_complete);
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7n_strategy_canary_proof_state_rejects_malformed_hash() {
        let mut proof = complete_s7n_proof_state();
        proof.recovery_receipt_fixture_sha256 = Some("not-a-sha".to_string());

        let review = review_btc_completion_strategy_canary_proof_state(&proof);

        assert!(!review.proof_bundle_ready_to_request_exact_approval);
        assert!(review.block_reasons.contains(
            &BtcCompletionStrategyCanaryProofBlockReason::MissingOrInvalidRecoveryReceiptProof
        ));
    }

    #[test]
    fn s7p_strategy_canary_proof_state_distinguishes_exact_receipt_from_fixture() {
        let mut proof = complete_s7n_proof_state();
        proof.recovery_receipt_proof_tier = BtcCompletionProofTier::ExactApprovedPrimitiveReceipt;

        let review = review_btc_completion_strategy_canary_proof_state(&proof);

        assert!(review.proof_bundle_ready_to_request_exact_approval);
        assert!(review.exact_approved_recovery_proof_present);
        assert!(review.execution_evidence_complete);
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7s_recovery_primitive_wrapper_boundary_accepts_strict_review_only_shape() {
        let review = review_btc_completion_recovery_primitive_wrapper_boundary(
            &complete_s7s_wrapper_boundary(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_RECOVERY_PRIMITIVE_WRAPPER_BOUNDARY_REVIEW_EVENT
        );
        assert!(review.wrapper_shape_ready_for_review_only_prebuild);
        assert!(review.block_reasons.is_empty());
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7s_recovery_primitive_wrapper_boundary_blocks_order_cancel_or_effectful_shape() {
        let mut boundary = complete_s7s_wrapper_boundary();
        boundary.max_order_count = 1;
        boundary.max_cancel_count = 1;
        boundary.effectful_execution_permitted_now = true;

        let review = review_btc_completion_recovery_primitive_wrapper_boundary(&boundary);

        assert!(!review.wrapper_shape_ready_for_review_only_prebuild);
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::OrderCountNonZero
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::CancelCountNonZero
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::EffectfulExecutionPermittedBeforeFreshApproval
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7s_recovery_primitive_wrapper_boundary_requires_preview_exit_66_and_guards() {
        let mut boundary = complete_s7s_wrapper_boundary();
        boundary.preview_without_approval_exit_code = Some(0);
        boundary.no_secret_output_guard = false;
        boundary.no_raw_signature_output_guard = false;

        let review = review_btc_completion_recovery_primitive_wrapper_boundary(&boundary);

        assert!(!review.wrapper_shape_ready_for_review_only_prebuild);
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::PreviewWithoutApprovalNotExit66
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoSecretOutputGuard
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveWrapperBoundaryBlockReason::MissingNoRawSignatureOutputGuard
        ));
    }

    #[test]
    fn s7t_recovery_primitive_prebuild_accepts_strict_review_only_evidence() {
        let review = review_btc_completion_recovery_primitive_prebuild(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_RECOVERY_PRIMITIVE_PREBUILD_REVIEW_EVENT
        );
        assert!(review.prebuild_shape_ready_for_review_only_packet);
        assert!(review.block_reasons.is_empty());
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7t_recovery_primitive_prebuild_rejects_hash_mismatch_and_bad_preview() {
        let mut evidence = complete_s7t_prebuild_evidence();
        evidence.packet_sha256 =
            "1111111111111111111111111111111111111111111111111111111111111111".to_string();
        evidence.expected_preview_without_approval_exit_code = Some(0);

        let review = review_btc_completion_recovery_primitive_prebuild(
            &complete_s7s_wrapper_boundary(),
            &evidence,
        );

        assert!(!review.prebuild_shape_ready_for_review_only_packet);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePrebuildBlockReason::PacketHashMismatch));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::PreviewWithoutApprovalNotExit66
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7t_recovery_primitive_prebuild_blocks_effectful_or_shared_paths() {
        let mut evidence = complete_s7t_prebuild_evidence();
        evidence.wrapper_loads_secrets = true;
        evidence.wrapper_emits_raw_signature = true;
        evidence.wrapper_uses_shared_ingress_or_ws = true;
        evidence.wrapper_uses_c_artifacts = true;
        evidence.wrapper_stages_effectful_execution = true;
        evidence.wrapper_executes_order_or_cancel = true;
        evidence.wrapper_executes_recovery_tx = true;
        evidence.effectful_execution_permitted_now = true;

        let review = review_btc_completion_recovery_primitive_prebuild(
            &complete_s7s_wrapper_boundary(),
            &evidence,
        );

        assert!(!review.prebuild_shape_ready_for_review_only_packet);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperLoadsSecrets));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperEmitsRawSignature
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperUsesSharedIngressOrWs
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperUsesCArtifacts));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperStagesEffectfulExecution
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperExecutesOrderOrCancel
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::WrapperExecutesRecoveryTxBeforeApproval
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePrebuildBlockReason::EffectfulExecutionPermittedBeforeFreshApproval
        ));
    }

    #[test]
    fn s7u_recovery_primitive_preflight_accepts_clean_review_only_shape() {
        let review = review_btc_completion_recovery_primitive_preflight(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_RECOVERY_PRIMITIVE_PREFLIGHT_REVIEW_EVENT
        );
        assert!(review.preflight_shape_ready_for_future_exact_run);
        assert!(review.block_reasons.is_empty());
        assert!(review.fresh_exact_approval_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7u_recovery_primitive_preflight_rejects_wrong_host_or_remote_hashes() {
        let mut preflight = complete_s7u_preflight_evidence();
        preflight.reviewed_host = "ubuntu@example.invalid".to_string();
        preflight.remote_packet_sha256 =
            "2222222222222222222222222222222222222222222222222222222222222222".to_string();
        preflight.remote_wrapper_runtime_sha256 =
            "3333333333333333333333333333333333333333333333333333333333333333".to_string();
        preflight.remote_controller_source_sha256 =
            "4444444444444444444444444444444444444444444444444444444444444444".to_string();

        let review = review_btc_completion_recovery_primitive_preflight(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &preflight,
        );

        assert!(!review.preflight_shape_ready_for_future_exact_run);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePreflightBlockReason::HostNotReviewed));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RemotePacketHashMismatch
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteWrapperRuntimeHashMismatch
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteControllerSourceHashMismatch
        ));
    }

    #[test]
    fn s7u_recovery_primitive_preflight_blocks_stale_or_effectful_paths() {
        let mut preflight = complete_s7u_preflight_evidence();
        preflight.ssh_preflight_fresh = false;
        preflight.ssh_preflight_age_ms = Some(60_001);
        preflight.preview_without_approval_exit_code = Some(0);
        preflight.no_order_auth_preview_passed = false;
        preflight.runtime_opens_ws = true;
        preflight.max_b_owned_direct_public_ws_connections = 2;
        preflight.shared_ingress_dependency_detected = true;
        preflight.remote_b_process_table_clean = false;
        preflight.secret_or_env_dump_detected = true;
        preflight.raw_signature_output_detected = true;
        preflight.effectful_command_executed = true;
        preflight.recovery_tx_submitted = true;
        preflight.order_or_cancel_submitted = true;

        let review = review_btc_completion_recovery_primitive_preflight(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &preflight,
        );

        assert!(!review.preflight_shape_ready_for_future_exact_run);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePreflightBlockReason::SshPreflightNotFresh));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePreflightBlockReason::SshPreflightTooOld));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::PreviewWithoutApprovalNotExit66
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::NoOrderAuthPreviewNotPassed
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePreflightBlockReason::RuntimeOpensWs));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::TooManyBOwnedDirectPublicWsConnections
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::SharedIngressDependencyDetected
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RemoteBProcessTableNotClean
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::SecretOrEnvDumpDetected
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RawSignatureOutputDetected
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::EffectfulCommandExecuted
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitivePreflightBlockReason::RecoveryTxSubmittedBeforeFreshApproval
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitivePreflightBlockReason::OrderOrCancelSubmitted));
    }

    #[test]
    fn s7v_recovery_primitive_execution_plan_lints_strict_exact_approval_shape() {
        let review = review_btc_completion_recovery_primitive_execution_plan(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &complete_s7v_execution_plan(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_RECOVERY_PRIMITIVE_EXECUTION_PLAN_REVIEW_EVENT
        );
        assert!(review.execution_plan_lint_passed_for_fresh_exact_approval);
        assert!(review.block_reasons.is_empty());
        assert!(review.runtime_preflight_still_required);
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7v_recovery_primitive_execution_plan_rejects_draft_or_consumed_approval() {
        let mut plan = complete_s7v_execution_plan();
        plan.exact_approval_text_present = false;
        plan.exact_approval_scope_matches_packet = false;
        plan.exact_approval_text_is_draft_not_issued = true;
        plan.exact_approval_already_consumed = true;
        plan.exact_approval_text_sha256 = "not-a-sha".to_string();

        let review = review_btc_completion_recovery_primitive_execution_plan(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &plan,
        );

        assert!(!review.execution_plan_lint_passed_for_fresh_exact_approval);
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::MissingOrInvalidExactApprovalTextHash
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalTextAbsent
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalScopeMismatch
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalTextStillDraftNotIssued
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ExactApprovalAlreadyConsumed
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7v_recovery_primitive_execution_plan_blocks_out_of_scope_paths() {
        let mut plan = complete_s7v_execution_plan();
        plan.max_condition_count = 2;
        plan.max_recovery_tx_count = 2;
        plan.max_order_count = 1;
        plan.max_cancel_count = 1;
        plan.requests_funding_live_latest_or_deploy = true;
        plan.uses_shared_ingress_or_shared_ws = true;
        plan.uses_c_artifacts = true;
        plan.imports_candidate = true;
        plan.prints_secret_or_raw_signature = true;
        plan.recovery_primitive_only = false;
        plan.effectful_execution_permitted_now = true;

        let review = review_btc_completion_recovery_primitive_execution_plan(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &plan,
        );

        assert!(!review.execution_plan_lint_passed_for_fresh_exact_approval);
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::ConditionCountNotOne
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::RecoveryTxCountNotOne
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::OrderCountNonZero));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CancelCountNonZero));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::FundingLiveLatestOrDeployRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::SharedIngressOrSharedWsRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CArtifactsRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::CandidateImportRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::SecretOrRawSignaturePrintRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::NotRecoveryPrimitiveOnly
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveExecutionPlanBlockReason::EffectfulExecutionPermittedBeforeRuntimeGate
        ));
    }

    #[test]
    fn s7w_recovery_primitive_run_result_accepts_exact_receipt_reconciliation_shape() {
        let review = review_btc_completion_recovery_primitive_run_result(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &complete_s7v_execution_plan(),
            &complete_s7w_run_result_evidence(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_RECOVERY_PRIMITIVE_RUN_RESULT_REVIEW_EVENT
        );
        assert!(review.run_result_receipt_reconciliation_passed);
        assert!(review.exact_approved_recovery_receipt_present);
        assert!(review.block_reasons.is_empty());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7w_recovery_primitive_run_result_rejects_fixture_receipt_tier() {
        let mut result = complete_s7w_run_result_evidence();
        result.recovery_receipt_proof_tier = BtcCompletionProofTier::ReviewOnlyFixture;

        let review = review_btc_completion_recovery_primitive_run_result(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &complete_s7v_execution_plan(),
            &result,
        );

        assert!(!review.run_result_receipt_reconciliation_passed);
        assert!(!review.exact_approved_recovery_receipt_present);
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::RecoveryReceiptProofTierNotExactApproved
        ));
    }

    #[test]
    fn s7w_recovery_primitive_run_result_blocks_unreconciled_or_contaminated_result() {
        let mut result = complete_s7w_run_result_evidence();
        result.condition_id = "other-condition".to_string();
        result.recovery_receipt_sha256 = Some("not-a-sha".to_string());
        result.submitted_recovery_tx_count = 2;
        result.submitted_order_count = 1;
        result.submitted_cancel_count = 1;
        result.receipt_id_present = false;
        result.collateral_delta_positive = false;
        result.local_ledger_condition_matches = false;
        result.external_position_condition_matches = false;
        result.receipt_factory_conversion_passed = false;
        result.verified_recovery_event_created = false;
        result.inventory_sync_draft_review_only = false;
        result.no_open_order_remainder = false;
        result.residual_exposure_zero = false;
        result.secret_or_raw_signature_output_detected = true;
        result.shared_ingress_dependency_detected = true;
        result.funding_live_latest_or_deploy_touched = true;

        let review = review_btc_completion_recovery_primitive_run_result(
            &complete_s7s_wrapper_boundary(),
            &complete_s7t_prebuild_evidence(),
            &complete_s7u_preflight_evidence(),
            &complete_s7v_execution_plan(),
            &result,
        );

        assert!(!review.run_result_receipt_reconciliation_passed);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveRunResultBlockReason::ConditionIdMismatch));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingOrInvalidRecoveryReceiptHash
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveRunResultBlockReason::RecoveryTxCountNotOne));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveRunResultBlockReason::OrderCountNonZero));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveRunResultBlockReason::CancelCountNonZero));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionRecoveryPrimitiveRunResultBlockReason::MissingReceiptId));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::CollateralDeltaNotPositive
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::LocalLedgerConditionMismatch
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::ExternalPositionConditionMismatch
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::ReceiptFactoryConversionMissing
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::VerifiedRecoveryEventMissing
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::InventorySyncDraftNotReviewOnly
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::OpenOrderRemainderPresent
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::ResidualExposureNonZero
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::SecretOrRawSignatureOutputDetected
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::SharedIngressDependencyDetected
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionRecoveryPrimitiveRunResultBlockReason::FundingLiveLatestOrDeployTouched
        ));
    }

    #[test]
    fn s8_micro_long_cycle_validation_plan_accepts_fixed_high_participation_packet_shape() {
        let review = review_btc_completion_s8_micro_long_cycle_validation_plan(&complete_s8_plan());

        assert_eq!(
            review.event,
            BTC_COMPLETION_S8_MICRO_LONG_CYCLE_VALIDATION_REVIEW_EVENT
        );
        assert!(review.s8_packet_ready_for_exact_approval_request);
        assert!(review.shadow_participation_guard_ready);
        assert!(review.result_reconciliation_uses_s7w);
        assert!(review.block_reasons.is_empty());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8_micro_long_cycle_validation_plan_rejects_participation_sacrifice() {
        let mut plan = complete_s8_plan();
        plan.shadow_participation_target_rate = 0.90;
        plan.shadow_participation_fail_below_rate = 0.80;
        plan.replay_holdout_market_participation_rate = 0.89;

        let review = review_btc_completion_s8_micro_long_cycle_validation_plan(&plan);

        assert!(!review.s8_packet_ready_for_exact_approval_request);
        assert!(!review.shadow_participation_guard_ready);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::ParticipationTargetTooLow
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::ParticipationFailThresholdTooLow
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::ReplayHoldoutParticipationBelowTarget
        ));
    }

    #[test]
    fn s8_micro_long_cycle_validation_plan_rejects_strategy_discovery_and_effectful_drift() {
        let mut plan = complete_s8_plan();
        plan.online_tuning_allowed = true;
        plan.strategy_discovery_allowed = true;
        plan.emergency_flatten_is_risk_control_only = false;
        plan.emergency_flatten_is_strategy_pnl_path = true;
        plan.required_recovery_receipt_tier = BtcCompletionProofTier::ReviewOnlyFixture;
        plan.uses_shared_ingress_or_shared_ws = true;
        plan.uses_c_artifacts = true;
        plan.imports_candidate = true;
        plan.effectful_execution_permitted_now = true;

        let review = review_btc_completion_s8_micro_long_cycle_validation_plan(&plan);

        assert!(!review.s8_packet_ready_for_exact_approval_request);
        assert!(!review.result_reconciliation_uses_s7w);
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8MicroLongCycleValidationBlockReason::OnlineTuningAllowed));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::StrategyDiscoveryAllowed
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::EmergencyFlattenNotRiskControlOnly
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::EmergencyFlattenAsStrategyPnlPath
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::RecoveryReceiptTierNotExactApproved
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::SharedIngressOrSharedWsRequested
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8MicroLongCycleValidationBlockReason::CArtifactsRequested));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::CandidateImportRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8MicroLongCycleValidationBlockReason::EffectfulExecutionPermittedBeforeFreshApproval
        ));
    }

    #[test]
    fn s8a_execution_wrapper_prebuild_accepts_three_round_inventory_driven_shape() {
        let review = review_btc_completion_s8a_execution_wrapper_prebuild(
            &complete_s8a_execution_wrapper_prebuild(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_S8A_EXECUTION_WRAPPER_PREBUILD_REVIEW_EVENT
        );
        assert!(review.wrapper_prebuild_ready_for_exact_approval_request);
        assert!(review.s8a_hash_boundary_bound);
        assert!(review.s8a_runtime_semantics_bound);
        assert!(review.block_reasons.is_empty());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8a_execution_wrapper_prebuild_rejects_semantic_drift() {
        let mut evidence = complete_s8a_execution_wrapper_prebuild();
        evidence.three_round_cap_packet_sha256 =
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string();
        evidence.order_size = 4.0;
        evidence.max_round_count = 2;
        evidence.session_hard_loss_cap_usdc = 10.0;
        evidence.submitted_size_counts_as_inventory = true;
        evidence.partial_fill_threshold_enabled = true;
        evidence.forced_complement_after_partial_fill = true;
        evidence.forced_complement_without_natural_signal = true;
        evidence.preview_without_approval_exit_code = Some(0);
        evidence.no_order_auth_preview_required = false;
        evidence.s7w_reconciliation_required = false;
        evidence.runtime_opens_own_ws = true;
        evidence.uses_shared_ingress_or_shared_ws = true;
        evidence.online_tuning_allowed = true;
        evidence.effectful_execution_permitted_now = true;

        let review = review_btc_completion_s8a_execution_wrapper_prebuild(&evidence);

        assert!(!review.wrapper_prebuild_ready_for_exact_approval_request);
        assert!(!review.s8a_hash_boundary_bound);
        assert!(!review.s8a_runtime_semantics_bound);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ThreeRoundCapPacketHashMismatch
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidOrderSize));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::InvalidRoundOrLossCaps
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SubmittedSizeCountsAsInventory
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PartialFillThresholdEnabled
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ForcedComplementAfterPartialFill
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ForcedComplementWithoutNaturalSignal
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::PreviewWithoutApprovalExitNot66
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingNoOrderAuthPreviewRequirement
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::MissingS7wReconciliationRequirement
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8aExecutionWrapperPrebuildBlockReason::RuntimeOpensOwnWs));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::SharedIngressOrSharedWsRequested
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8aExecutionWrapperPrebuildBlockReason::OnlineTuningAllowed));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::EffectfulExecutionPermittedBeforeFreshApproval
        ));
    }

    #[test]
    fn s8a_execution_wrapper_prebuild_keeps_participation_and_roi_semantics() {
        let mut evidence = complete_s8a_execution_wrapper_prebuild();
        evidence.shadow_participation_pass_rate = 0.95;
        evidence.minimum_roi_hard_gate_enabled = true;
        evidence.roi_report_only_for_first_s8a = false;

        let review = review_btc_completion_s8a_execution_wrapper_prebuild(&evidence);

        assert!(!review.wrapper_prebuild_ready_for_exact_approval_request);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::ParticipationPassThresholdTooHigh
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aExecutionWrapperPrebuildBlockReason::RoiOrResidualIncorrectlyHardGated
        ));
    }

    #[test]
    fn s8a_effectful_runtime_readiness_accepts_native_runtime_shape_without_permitting_execution() {
        let review = review_btc_completion_s8a_effectful_runtime_readiness(
            &complete_s8a_effectful_runtime_readiness(),
        );

        assert_eq!(
            review.event,
            BTC_COMPLETION_S8A_EFFECTFUL_RUNTIME_READINESS_REVIEW_EVENT
        );
        assert!(review.runtime_ready_for_exact_approval_request);
        assert!(review.native_s8a_runtime_bound);
        assert!(review.live_source_and_auth_preview_bound);
        assert!(review.block_reasons.is_empty());
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8a_effectful_runtime_readiness_rejects_review_only_wrapper_as_not_executable() {
        let mut evidence = complete_s8a_effectful_runtime_readiness();
        evidence.wrapper_supports_no_order_auth_preview = false;
        evidence.no_order_auth_preview_uses_reviewed_auth_path = false;
        evidence.wrapper_supports_exact_order_path = false;
        evidence.exact_order_path_requires_approval_hash = false;
        evidence.exact_order_path_rejects_missing_or_mismatched_approval = false;
        evidence.live_public_buy_signal_source_bound = false;
        evidence.btc_5m_target_derivation_bound = false;
        evidence.source_guard_500_live_enforced = false;
        evidence.uses_s8a_controller_for_every_admission = false;
        evidence.runtime_uses_s1a_same_run_flatten_plan = true;
        evidence.runtime_uses_s3a_admission_or_pair_rank = true;

        let review = review_btc_completion_s8a_effectful_runtime_readiness(&evidence);

        assert!(!review.runtime_ready_for_exact_approval_request);
        assert!(!review.native_s8a_runtime_bound);
        assert!(!review.live_source_and_auth_preview_bound);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NoOrderAuthPreviewUnavailable
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ExactOrderPathUnavailable
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::LivePublicBuySignalSourceMissing
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S8aControllerNotUsedForEveryAdmission
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ReusesS1aSameRunFlattenPlan
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ReusesS3aAdmissionOrPairRank
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8a_effectful_runtime_readiness_rejects_semantic_or_safety_drift() {
        let mut evidence = complete_s8a_effectful_runtime_readiness();
        evidence.submitted_size_counts_as_inventory = true;
        evidence.partial_fill_threshold_enabled = true;
        evidence.forced_complement_without_natural_signal = true;
        evidence.max_round_count = 4;
        evidence.session_hard_loss_cap_usdc = 20.0;
        evidence.s7w_requires_exact_approved_receipt_tier = false;
        evidence.s7w_rejects_review_only_fixture_receipt = false;
        evidence.runtime_opens_own_ws = true;
        evidence.max_b_owned_direct_public_ws_connections = 2;
        evidence.uses_shared_ingress_or_shared_ws = true;
        evidence.imports_candidate = true;
        evidence.online_tuning_allowed = true;
        evidence.effectful_execution_permitted_now = true;

        let review = review_btc_completion_s8a_effectful_runtime_readiness(&evidence);

        assert!(!review.runtime_ready_for_exact_approval_request);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SubmittedSizeCountsAsInventory
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::PartialFillThresholdEnabled
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::ForcedComplementWithoutNaturalSignal
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidRoundLossOrSizeCaps
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wExactApprovedReceiptTierMissing
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::S7wReviewOnlyFixtureReceiptAccepted
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::RuntimeOpensOwnWs));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::TooManyBOwnedDirectPublicWs
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SharedIngressOrSharedWsRequested
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::CandidateImportRequested
        ));
        assert!(review
            .block_reasons
            .contains(&BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::OnlineTuningAllowed));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::EffectfulExecutionPermittedBeforeFreshApproval
        ));
    }

    #[test]
    fn s8a_effectful_runtime_readiness_rejects_invalid_order_minimums_and_units() {
        let mut evidence = complete_s8a_effectful_runtime_readiness();
        evidence.limit_order_min_size_shares = 1.0;
        evidence.limit_entry_order_size_shares = 1.0;
        evidence.market_buy_min_notional_usdc = 0.50;
        evidence.market_buy_amount_is_usdc = false;
        evidence.market_sell_amount_is_shares = false;
        evidence.market_sell_min_size_shares = 1.0;
        evidence.sub_min_residual_market_sell_blocked = false;
        evidence.order_minimums_verified_from_official_clob_market_info = false;
        evidence.order_amount_units_verified_from_official_order_docs = false;

        let review = review_btc_completion_s8a_effectful_runtime_readiness(&evidence);

        assert!(!review.runtime_ready_for_exact_approval_request);
        assert!(!review.native_s8a_runtime_bound);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidLimitOrderMinimums
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidMarketBuyMinimumNotional
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MarketBuyAmountUnitNotUsdc
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::MarketSellAmountUnitNotShares
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::InvalidMarketSellMinimumShares
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::SubMinResidualMarketSellNotBlocked
        ));
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::OrderMinimumsNotVerifiedFromOfficialSources
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s8a_effectful_runtime_readiness_requires_native_s8a_order_adapter() {
        let mut evidence = complete_s8a_effectful_runtime_readiness();
        evidence.uses_native_s8a_order_adapter = false;

        let review = review_btc_completion_s8a_effectful_runtime_readiness(&evidence);

        assert!(!review.runtime_ready_for_exact_approval_request);
        assert!(!review.native_s8a_runtime_bound);
        assert!(review.block_reasons.contains(
            &BtcCompletionS8aEffectfulRuntimeReadinessBlockReason::NativeS8aOrderAdapterMissing
        ));
        assert!(!review.effectful_execution_permitted);
    }

    #[test]
    fn s7k_large_sample_fixture_matches_controller_decisions() {
        let fixture: Value = serde_json::from_str(include_str!(
            "fixtures/btc_completion_controller_s7k_large_sample.json"
        ))
        .expect("S7K fixture JSON should parse");
        assert_eq!(fixture["row_count"].as_u64(), Some(1200));

        let cfg = BtcCompletionControllerConfig::s7e_research_default();
        let mut state = BtcCompletionControllerState::default();
        let mut accept_count = 0_u64;
        let mut block_count = 0_u64;

        for row in fixture["rows"].as_array().expect("rows should be an array") {
            let candidate = BtcCompletionCandidate {
                asset: string_field(row, "asset"),
                event_kind: string_field(row, "event_kind"),
                public_trade_taker_side: string_field(row, "public_trade_taker_side"),
                condition_id: string_field(row, "condition_id"),
                slug: string_field(row, "slug"),
                ts_ms: u64_field(row, "ts_ms"),
                offset_s: f64_field(row, "offset_s"),
                time_to_end_s: Some(f64_field(row, "time_to_end_s")),
                side: side_field(row, "side"),
                public_trade_price: f64_field(row, "public_trade_price"),
                l1_pair_ask: f64_field(row, "l1_pair_ask"),
                l1_pair_available_qty: f64_field(row, "l1_pair_available_qty"),
                buy_available_qty: f64_field(row, "buy_available_qty"),
                strict_l1_age_ms: row["strict_l1_age_ms"].as_u64(),
                strict_l2_age_ms: row["strict_l2_age_ms"].as_u64(),
                public_trade_recv_lag_ms: row["public_trade_recv_lag_ms"].as_u64(),
                forbidden_decision_fields_present: false,
                snapshot_index_used_as_rank: false,
            };
            let expected = row["expected"]
                .as_str()
                .expect("expected should be a string");
            let review = review_btc_completion_candidate(&cfg, &state, &candidate);
            assert!(
                !review.execution_permitted,
                "row {} unexpectedly permitted execution",
                row["candidate_row_id"]
            );

            if expected == "ACCEPT_REVIEW_ONLY" {
                let intent = review
                    .intent
                    .expect("expected accepted row should produce intent");
                assert!(!intent.execution_permitted);
                assert_eq!(review.block_reason, None);
                assert!((intent.price - f64_field(row, "expected_price")).abs() < 1e-9);
                assert!((intent.qty - f64_field(row, "expected_qty")).abs() < 1e-9);
                state.record_accepted(&intent);
                accept_count += 1;
            } else {
                let actual = review
                    .block_reason
                    .expect("expected blocked row should include reason")
                    .as_str();
                assert_eq!(
                    actual, expected,
                    "candidate_row_id={}",
                    row["candidate_row_id"]
                );
                block_count += 1;
            }
        }

        assert_eq!(accept_count, 42);
        assert_eq!(block_count, 1158);
    }

    fn string_field(row: &Value, key: &str) -> String {
        row[key].as_str().expect("string fixture field").to_string()
    }

    fn u64_field(row: &Value, key: &str) -> u64 {
        row[key].as_u64().expect("u64 fixture field")
    }

    fn f64_field(row: &Value, key: &str) -> f64 {
        row[key].as_f64().expect("f64 fixture field")
    }

    fn side_field(row: &Value, key: &str) -> Side {
        match row[key].as_str().expect("side fixture field") {
            "YES" => Side::Yes,
            "NO" => Side::No,
            other => panic!("unexpected side {other}"),
        }
    }

    fn complete_s7l_policy() -> BtcCompletionStrategyCanaryGatePolicy {
        BtcCompletionStrategyCanaryGatePolicy {
            scope_asset: "BTC".to_string(),
            controller_policy_sha256:
                "1cc1210f24a8651ae0ff80e4458cee656185c692a105e90404188c5a9566b52a".to_string(),
            replay_result_sha256:
                "d25ac163702c76eaaa59e3db37e2011ebdecc070dd9592100156288ce49ac784".to_string(),
            recovery_policy_sha256: Some(
                "e1c38de56ecb449f1d0158a7cc333ba71d0349c58f407addc35ad1d312cd6ac8".to_string(),
            ),
            recovery_path: Some(BtcCompletionRecoveryPath::CompleteSetMerge),
            max_capital_lock_usdc: Some(18.0),
            max_capital_lock_ms: Some(86_400_000),
            max_residual_cost_rate: Some(0.03),
            max_bad_pair_cost_ge_1_share: Some(0.05),
            max_one_leg_exposure_usdc: Some(5.0),
            requires_source_guard_500: true,
            requires_same_market_yes_no_close_window: true,
            requires_external_position_reconciliation: true,
            requires_recovery_receipt_factory: true,
            requires_no_open_order_remainder: true,
            allows_same_run_flatten_as_primary_exit: false,
        }
    }

    fn complete_s7n_proof_state() -> BtcCompletionStrategyCanaryProofState {
        BtcCompletionStrategyCanaryProofState {
            ex_ante_policy_payload_sha256:
                "0575ca8450b7dbe7fa07e26a337dcf5d15f6e0b738b2a287d6bdfdd8b941ba06".to_string(),
            external_position_reconciliation_sha256: Some(
                "e38527af1e6c83cbe289a765a5fd922f26aef6e19b11bfdbd9c7f331ac3997bc".to_string(),
            ),
            external_position_proof_tier: BtcCompletionProofTier::PublicReadOnly,
            recovery_receipt_fixture_sha256: Some(
                "5d2390855f14ee93eeffbacfd80658c081ff4e2b6c411a1b3492678a6b9c0ac8".to_string(),
            ),
            recovery_receipt_proof_tier: BtcCompletionProofTier::ReviewOnlyFixture,
            capital_lock_model_sha256: Some(
                "b0db07808ed191744bd1780bbc861affc227bfb2097d3288fccd1f946d57ff71".to_string(),
            ),
            capital_lock_model_tier: BtcCompletionProofTier::ReviewOnlyFixture,
            inventory_residual_policy_sha256: Some(
                "2638add7d0456a21da4b962e9f3c1ddda5d76b234bd2c1c6dc44ece8c36587c2".to_string(),
            ),
            inventory_residual_policy_tier: BtcCompletionProofTier::ReviewOnlyFixture,
            no_open_order_remainder_proof_sha256: Some(
                "6e2cc11fcac7b42ea129521a39b29834a9edbbc13ad3b00dbdac1f2992c38c34".to_string(),
            ),
            no_open_order_remainder_proof_tier: BtcCompletionProofTier::ReviewOnlyFixture,
        }
    }

    fn complete_s7s_wrapper_boundary() -> BtcCompletionRecoveryPrimitiveWrapperBoundary {
        BtcCompletionRecoveryPrimitiveWrapperBoundary {
            approval_draft_sha256:
                "543730fe50c269e490e37d67bfdbff3317b8541065d941c65e550ee403f16b55".to_string(),
            packet_sha256: "699a28e30ae67e4b1da8803f82c2b8ca26e7262f9da2e4ffd019a279fdbebd88"
                .to_string(),
            proof_tier_state_sha256:
                "31d485d7e82a71d3dc8ae76f2b1a5bc28a8ef834fbc21f0ce63d031c9b08cf80".to_string(),
            selected_recovery_path: Some(BtcCompletionRecoveryPath::CompleteSetMerge),
            max_condition_count: 1,
            max_recovery_tx_count: 1,
            max_order_count: 0,
            max_cancel_count: 0,
            preview_without_approval_exit_code: Some(66),
            no_secret_output_guard: true,
            no_raw_signature_output_guard: true,
            no_shared_ingress_guard: true,
            no_c_artifacts_guard: true,
            draft_not_issued_marker: true,
            effectful_execution_permitted_now: false,
        }
    }

    fn complete_s7t_prebuild_evidence() -> BtcCompletionRecoveryPrimitivePrebuildEvidence {
        BtcCompletionRecoveryPrimitivePrebuildEvidence {
            wrapper_runtime_sha256:
                "7223df839a0915c09b702302b60a3da69d20ea081abc25fe2be66a1dade0a6ab".to_string(),
            controller_source_sha256:
                "d16ac53ed6041899e8a412789403c2b41dc0aad3ea3848623bbf3f9bf97498f2".to_string(),
            packet_sha256: "699a28e30ae67e4b1da8803f82c2b8ca26e7262f9da2e4ffd019a279fdbebd88"
                .to_string(),
            approval_draft_sha256:
                "543730fe50c269e490e37d67bfdbff3317b8541065d941c65e550ee403f16b55".to_string(),
            proof_tier_state_sha256:
                "31d485d7e82a71d3dc8ae76f2b1a5bc28a8ef834fbc21f0ce63d031c9b08cf80".to_string(),
            expected_preview_without_approval_exit_code: Some(66),
            wrapper_source_review_only: true,
            wrapper_loads_secrets: false,
            wrapper_emits_raw_signature: false,
            wrapper_uses_shared_ingress_or_ws: false,
            wrapper_uses_c_artifacts: false,
            wrapper_stages_effectful_execution: false,
            wrapper_executes_order_or_cancel: false,
            wrapper_executes_recovery_tx: false,
            effectful_execution_permitted_now: false,
        }
    }

    fn complete_s7u_preflight_evidence() -> BtcCompletionRecoveryPrimitivePreflightEvidence {
        BtcCompletionRecoveryPrimitivePreflightEvidence {
            reviewed_host: "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com".to_string(),
            ssh_preflight_fresh: true,
            ssh_preflight_age_ms: Some(5_000),
            max_ssh_preflight_age_ms: 60_000,
            remote_packet_sha256:
                "699a28e30ae67e4b1da8803f82c2b8ca26e7262f9da2e4ffd019a279fdbebd88".to_string(),
            remote_wrapper_runtime_sha256:
                "7223df839a0915c09b702302b60a3da69d20ea081abc25fe2be66a1dade0a6ab".to_string(),
            remote_controller_source_sha256:
                "d16ac53ed6041899e8a412789403c2b41dc0aad3ea3848623bbf3f9bf97498f2".to_string(),
            preview_without_approval_exit_code: Some(66),
            no_order_auth_preview_passed: true,
            runtime_opens_ws: false,
            max_b_owned_direct_public_ws_connections: 1,
            shared_ingress_dependency_detected: false,
            remote_b_process_table_clean: true,
            secret_or_env_dump_detected: false,
            raw_signature_output_detected: false,
            effectful_command_executed: false,
            recovery_tx_submitted: false,
            order_or_cancel_submitted: false,
        }
    }

    fn complete_s7v_execution_plan() -> BtcCompletionRecoveryPrimitiveExecutionPlan {
        BtcCompletionRecoveryPrimitiveExecutionPlan {
            exact_approval_text_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            exact_approval_text_present: true,
            exact_approval_scope_matches_packet: true,
            exact_approval_text_is_draft_not_issued: false,
            exact_approval_already_consumed: false,
            packet_sha256: "699a28e30ae67e4b1da8803f82c2b8ca26e7262f9da2e4ffd019a279fdbebd88"
                .to_string(),
            approval_draft_sha256:
                "543730fe50c269e490e37d67bfdbff3317b8541065d941c65e550ee403f16b55".to_string(),
            preflight_result_sha256:
                "f85441c6e7032d9170bc16ed667b9283a8d04f87bbea260dc129031a184557f2".to_string(),
            selected_recovery_path: Some(BtcCompletionRecoveryPath::CompleteSetMerge),
            condition_id: "0xcondition".to_string(),
            max_condition_count: 1,
            max_recovery_tx_count: 1,
            max_order_count: 0,
            max_cancel_count: 0,
            requests_funding_live_latest_or_deploy: false,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
            imports_candidate: false,
            prints_secret_or_raw_signature: false,
            recovery_primitive_only: true,
            effectful_execution_permitted_now: false,
        }
    }

    fn complete_s7w_run_result_evidence() -> BtcCompletionRecoveryPrimitiveRunResultEvidence {
        BtcCompletionRecoveryPrimitiveRunResultEvidence {
            execution_plan_review_sha256:
                "6118d2f7bceb740b7b975ddeb60de404820aa51a45ae78db2b8e9cbcd6ccadf8".to_string(),
            exact_approval_text_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            condition_id: "0xcondition".to_string(),
            selected_recovery_path: Some(BtcCompletionRecoveryPath::CompleteSetMerge),
            recovery_receipt_sha256: Some(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            ),
            recovery_receipt_proof_tier: BtcCompletionProofTier::ExactApprovedPrimitiveReceipt,
            submitted_recovery_tx_count: 1,
            submitted_order_count: 0,
            submitted_cancel_count: 0,
            receipt_id_present: true,
            collateral_pre_observation_sha256:
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
            collateral_post_observation_sha256:
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string(),
            collateral_delta_positive: true,
            local_ledger_condition_matches: true,
            external_position_condition_matches: true,
            receipt_factory_conversion_passed: true,
            verified_recovery_event_created: true,
            inventory_sync_draft_review_only: true,
            no_open_order_remainder: true,
            residual_exposure_zero: true,
            secret_or_raw_signature_output_detected: false,
            shared_ingress_dependency_detected: false,
            funding_live_latest_or_deploy_touched: false,
        }
    }

    fn complete_s8_plan() -> BtcCompletionS8MicroLongCycleValidationPlan {
        BtcCompletionS8MicroLongCycleValidationPlan {
            packet_draft_sha256: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .to_string(),
            ex_ante_policy_sha256:
                "0575ca8450b7dbe7fa07e26a337dcf5d15f6e0b738b2a287d6bdfdd8b941ba06".to_string(),
            s7w_run_result_linter_sha256:
                "b2490770fe7cfaea6671f660eb4349a78ab43695b97811a8c1b8bf393662dea2".to_string(),
            scope_asset: "BTC".to_string(),
            fixed_controller_policy: "cool5_imb1.25_source_guard_500".to_string(),
            source_guard_500_required: true,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            shadow_participation_target_rate: 0.98,
            shadow_participation_fail_below_rate: 0.95,
            replay_holdout_market_participation_rate: 1719.0 / 1728.0,
            max_effectful_markets_total: 12,
            max_effectful_markets_per_day: 3,
            max_order_submissions_total: 48,
            max_capital_lock_usdc: 60.0,
            max_one_leg_exposure_usdc: 6.0,
            max_hold_duration_ms: 86_400_000,
            max_emergency_flatten_count: 3,
            emergency_flatten_is_risk_control_only: true,
            emergency_flatten_is_strategy_pnl_path: false,
            required_recovery_receipt_tier: BtcCompletionProofTier::ExactApprovedPrimitiveReceipt,
            requires_positive_collateral_delta: true,
            requires_fee_inclusive_pnl_report: true,
            requires_local_ledger_external_position_alignment: true,
            requires_no_open_order_remainder: true,
            requires_residual_exposure_zero: true,
            stop_on_receipt_or_reconciliation_failure: true,
            stop_on_participation_below_fail_threshold: true,
            exact_approval_required_per_batch: true,
            packet_draft_not_issued_marker: true,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
            requests_funding_live_latest_or_deploy: false,
            imports_candidate: false,
            effectful_execution_permitted_now: false,
        }
    }

    fn complete_s8a_execution_wrapper_prebuild() -> BtcCompletionS8aExecutionWrapperPrebuildEvidence
    {
        BtcCompletionS8aExecutionWrapperPrebuildEvidence {
            loss_trigger_redesign_packet_sha256:
                BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256.to_string(),
            partial_fill_addendum_sha256: BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256
                .to_string(),
            three_round_cap_packet_sha256: BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256
                .to_string(),
            s7w_run_result_linter_sha256: BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256
                .to_string(),
            wrapper_source_sha256:
                "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            controller_source_sha256:
                "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
            scope_asset: "BTC".to_string(),
            market_family: "btc-5min".to_string(),
            fixed_controller_policy: "cool5_imb1.25_source_guard_500".to_string(),
            source_guard_500_required: true,
            order_size: 5.0,
            api_min_order_size: 5.0,
            seed_px_hi: 0.80,
            l1_pair_ask_max: 1.02,
            max_round_count: 3,
            session_hard_loss_cap_usdc: 15.0,
            per_round_theoretical_max_loss_usdc: 5.0,
            target_runtime_ms: 14_400_000,
            max_active_market_count: 1,
            max_active_capital_lock_usdc: 6.0,
            max_cumulative_gross_quote_spend_usdc: 18.0,
            max_initial_buy_submissions: 6,
            max_emergency_exit_or_hedge_submissions: 3,
            max_total_order_submissions: 9,
            max_cancel_count: 6,
            max_recovery_tx_count: 3,
            shadow_participation_pass_rate: 0.75,
            shadow_participation_warning_below_rate: 0.60,
            roi_report_only_for_first_s8a: true,
            residual_cost_rate_report_only_for_first_s8a: true,
            minimum_roi_hard_gate_enabled: false,
            uses_actual_filled_qty_inventory: true,
            submitted_size_counts_as_inventory: false,
            partial_fill_threshold_enabled: false,
            forced_complement_after_partial_fill: false,
            forced_complement_without_natural_signal: false,
            api_min_order_size_enforced: true,
            sub_min_residual_sell_blocked: true,
            no_new_round_while_active_market: true,
            pre_entry_realized_loss_plus_round_cap_guard: true,
            active_risk_at_work_cap_guard: true,
            realized_loss_only_after_close_or_recovery: true,
            emergency_flatten_is_risk_control_only: true,
            emergency_flatten_is_strategy_pnl_path: false,
            s7w_reconciliation_required: true,
            exact_approval_required: true,
            draft_not_issued_marker: true,
            preview_without_approval_exit_code: Some(66),
            no_order_auth_preview_required: true,
            runtime_opens_own_ws: false,
            max_b_owned_direct_public_ws_connections: 1,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            imports_candidate: false,
            prints_secret_or_raw_signature: false,
            requests_funding_live_latest_or_deploy: false,
            effectful_execution_permitted_now: false,
        }
    }

    fn complete_s8a_effectful_runtime_readiness(
    ) -> BtcCompletionS8aEffectfulRuntimeReadinessEvidence {
        BtcCompletionS8aEffectfulRuntimeReadinessEvidence {
            prebuild_review_passed: true,
            loss_trigger_redesign_packet_sha256:
                BTC_COMPLETION_S8A_LOSS_TRIGGER_REDESIGN_PACKET_SHA256.to_string(),
            partial_fill_addendum_sha256: BTC_COMPLETION_S8A_PARTIAL_FILL_ADDENDUM_SHA256
                .to_string(),
            three_round_cap_packet_sha256: BTC_COMPLETION_S8A_THREE_ROUND_CAP_PACKET_SHA256
                .to_string(),
            s7w_run_result_linter_sha256: BTC_COMPLETION_S7W_RUN_RESULT_LINTER_RESULT_SHA256
                .to_string(),
            wrapper_source_sha256:
                "3333333333333333333333333333333333333333333333333333333333333333".to_string(),
            controller_source_sha256:
                "4444444444444444444444444444444444444444444444444444444444444444".to_string(),
            wrapper_supports_preview_without_approval: true,
            preview_without_approval_exit_code: Some(66),
            wrapper_supports_no_order_auth_preview: true,
            no_order_auth_preview_uses_reviewed_auth_path: true,
            no_order_auth_preview_prints_secret_or_signature: false,
            wrapper_supports_exact_order_path: true,
            exact_order_path_requires_approval_hash: true,
            exact_order_path_rejects_missing_or_mismatched_approval: true,
            uses_native_s8a_order_adapter: true,
            limit_order_min_size_shares: BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES,
            limit_entry_order_size_shares: BTC_COMPLETION_S8A_LIMIT_MIN_ORDER_SIZE_SHARES,
            market_buy_min_notional_usdc: BTC_COMPLETION_S8A_MARKET_BUY_MIN_NOTIONAL_USDC,
            market_buy_amount_is_usdc: true,
            market_sell_amount_is_shares: true,
            market_sell_min_size_shares: BTC_COMPLETION_S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES,
            sub_min_residual_market_sell_blocked: true,
            order_minimums_verified_from_official_clob_market_info: true,
            order_amount_units_verified_from_official_order_docs: true,
            live_public_buy_signal_source_bound: true,
            btc_5m_target_derivation_bound: true,
            source_guard_500_live_enforced: true,
            uses_s8a_controller_for_every_admission: true,
            uses_actual_filled_qty_inventory: true,
            submitted_size_counts_as_inventory: false,
            partial_fill_threshold_enabled: false,
            forced_complement_without_natural_signal: false,
            max_round_count: 3,
            session_hard_loss_cap_usdc: 15.0,
            per_round_theoretical_max_loss_usdc: 5.0,
            order_size: 5.0,
            max_active_market_count: 1,
            max_initial_buy_submissions: 6,
            max_total_order_submissions: 9,
            max_cancel_count: 6,
            max_recovery_tx_count: 3,
            s7w_post_run_reconciliation_bound: true,
            s7w_requires_exact_approved_receipt_tier: true,
            s7w_rejects_review_only_fixture_receipt: true,
            runtime_uses_s1a_same_run_flatten_plan: false,
            runtime_uses_s3a_admission_or_pair_rank: false,
            runtime_opens_own_ws: false,
            max_b_owned_direct_public_ws_connections: 1,
            uses_shared_ingress_or_shared_ws: false,
            uses_c_artifacts: false,
            imports_candidate: false,
            online_tuning_allowed: false,
            strategy_discovery_allowed: false,
            prints_secret_or_raw_signature: false,
            requests_funding_live_latest_or_deploy: false,
            effectful_execution_permitted_now: false,
        }
    }
}
