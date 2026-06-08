//! Disabled-by-default PairArb harvest reconciliation gate.
//!
//! This module is intentionally pure and side-effect free. It does not sign,
//! order, cancel, merge, redeem, fund, read secrets, or perform network IO. The
//! only purpose is to encode the fail-closed contract that must be satisfied
//! before a future exact-approved hold/merge/redeem path can call any execution
//! primitive.

use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::str::FromStr;

/// PairArb harvesting must remain opt-in until a future exact-approved path
/// explicitly wires execution. The gate itself is review-only/pure.
pub const PAIR_ARB_HARVESTER_ENABLED_DEFAULT: bool = false;
pub const PAIR_ARB_HARVESTER_DECISION_EVENT: &str = "pair_arb_harvester_decision";
pub const RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT: &str = "recovery_primitive_wrapper_review";
pub const RECOVERY_PRIMITIVE_WRAPPER_EFFECTFUL_EXECUTION_DEFAULT: bool = false;
pub const RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_KEYS: [&str; 16] = [
    "collateral_delta_usdc",
    "condition_id",
    "confirmed_tx_hash_present",
    "effectful_execution_permitted",
    "event",
    "fixture_error",
    "full_set_qty",
    "market_slug",
    "path",
    "receipt_error",
    "receipt_present",
    "recovery_tx_or_relayer_receipt_id",
    "run_id",
    "submit_id_or_tx_hash_present",
    "verified_event_present",
    "wrapper_schema_version",
];
pub const RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarvestDecision {
    BlockMissingLocalLedger,
    BlockMissingLineageField,
    BlockPendingOrderRemainder,
    BlockNoLocalPairableFullSet,
    BlockExternalPositionUnknown,
    BlockConditionMismatch,
    BlockPositionMismatch,
    BlockNoMergeableFullSet,
    BlockApprovalRequiredRedeemableWithoutApproval,
    PairCoveredReconciledReviewOnlyApprovalRequired,
    BlockMissingRecoveryReceiptOrCollateralDelta,
    WouldAllowOnlyUnderFreshExactApprovalFixtureControl,
}

impl HarvestDecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BlockMissingLocalLedger => "BLOCK_MISSING_LOCAL_LEDGER",
            Self::BlockMissingLineageField => "BLOCK_MISSING_LINEAGE_FIELD",
            Self::BlockPendingOrderRemainder => "BLOCK_PENDING_ORDER_REMAINDER",
            Self::BlockNoLocalPairableFullSet => "BLOCK_NO_LOCAL_PAIRABLE_FULL_SET",
            Self::BlockExternalPositionUnknown => "BLOCK_EXTERNAL_POSITION_UNKNOWN",
            Self::BlockConditionMismatch => "BLOCK_CONDITION_MISMATCH",
            Self::BlockPositionMismatch => "BLOCK_POSITION_MISMATCH",
            Self::BlockNoMergeableFullSet => "BLOCK_NO_MERGEABLE_FULL_SET",
            Self::BlockApprovalRequiredRedeemableWithoutApproval => {
                "BLOCK_APPROVAL_REQUIRED_REDEEMABLE_WITHOUT_APPROVAL"
            }
            Self::PairCoveredReconciledReviewOnlyApprovalRequired => {
                "PAIR_COVERED_RECONCILED_REVIEW_ONLY_APPROVAL_REQUIRED"
            }
            Self::BlockMissingRecoveryReceiptOrCollateralDelta => {
                "BLOCK_MISSING_RECOVERY_RECEIPT_OR_COLLATERAL_DELTA"
            }
            Self::WouldAllowOnlyUnderFreshExactApprovalFixtureControl => {
                "WOULD_ALLOW_ONLY_UNDER_FRESH_EXACT_APPROVAL_FIXTURE_CONTROL"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalPairArbLedgerProof {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub same_run_buy_order_ids: Vec<String>,
    pub filled_yes_qty: Decimal,
    pub filled_no_qty: Decimal,
    pub open_yes_qty: Decimal,
    pub open_no_qty: Decimal,
}

impl LocalPairArbLedgerProof {
    pub fn pairable_full_set_qty(&self) -> Decimal {
        self.filled_yes_qty.min(self.filled_no_qty)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalPositionProof {
    pub condition_id: String,
    pub yes_qty: Decimal,
    pub no_qty: Decimal,
    pub mergeable_full_set_qty: Decimal,
    pub redeemable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionTokenSide {
    Yes,
    No,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicPositionSnapshotRow {
    pub condition_id: String,
    pub outcome: String,
    pub outcome_index: Option<u8>,
    pub size: Decimal,
    pub mergeable: bool,
    pub redeemable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalPositionAdapterResult {
    pub proof: Option<ExternalPositionProof>,
    pub ignored_row_count: usize,
    pub unknown_side_row_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecoveryEvidence {
    pub exact_approval_id: Option<String>,
    pub recovery_tx_or_relayer_receipt_id: Option<String>,
    pub collateral_delta_usdc: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarvestRecoveryPath {
    CompleteSetMerge,
    PostResolutionRedeem,
}

impl HarvestRecoveryPath {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CompleteSetMerge => "COMPLETE_SET_MERGE",
            Self::PostResolutionRedeem => "POST_RESOLUTION_REDEEM",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedHarvestRecoveryEvent {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub path: HarvestRecoveryPath,
    pub full_set_qty: Decimal,
    pub exact_approval_id: String,
    pub recovery_tx_or_relayer_receipt_id: String,
    pub collateral_delta_usdc: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimitiveReceipt {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub path: HarvestRecoveryPath,
    pub full_set_qty: Decimal,
    pub exact_approval_id: String,
    pub submit_id_or_tx_hash: String,
    pub confirmed_tx_hash: Option<String>,
    pub pre_collateral_usdc: Decimal,
    pub post_collateral_usdc: Decimal,
}

impl RecoveryPrimitiveReceipt {
    pub fn collateral_delta_usdc(&self) -> Decimal {
        self.post_collateral_usdc - self.pre_collateral_usdc
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimitiveRequest {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub path: HarvestRecoveryPath,
    pub full_set_qty: Decimal,
    pub exact_approval_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollateralBalanceObservation {
    pub source: String,
    pub account_ref: String,
    pub observed_at_ms: u64,
    pub balance_usdc: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimitiveWrapperEvidence {
    pub request: RecoveryPrimitiveRequest,
    pub submit_id_or_tx_hash: String,
    pub confirmed_tx_hash: Option<String>,
    pub pre_collateral: CollateralBalanceObservation,
    pub post_collateral: CollateralBalanceObservation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimitiveWrapperReviewRecord {
    pub receipt: Option<RecoveryPrimitiveReceipt>,
    pub receipt_error: Option<RecoveryPrimitiveReceiptBuildError>,
    pub effectful_execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryPrimitiveWrapperFixtureRow {
    pub request: RecoveryPrimitiveRequest,
    pub submit_id_or_tx_hash: String,
    pub confirmed_tx_hash: Option<String>,
    pub collateral_source: String,
    pub collateral_account_ref: String,
    pub pre_observed_at_ms: u64,
    pub post_observed_at_ms: u64,
    pub pre_raw_balance_units: i128,
    pub post_raw_balance_units: i128,
    pub scale_units_per_usdc: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollateralBalanceDeltaError {
    MissingSource,
    MissingAccountRef,
    SourceMismatch,
    AccountMismatch,
    NonMonotonicObservationTime,
    NegativeBalance,
    NonPositiveDelta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollateralBalanceObservationBuildError {
    MissingSource,
    MissingAccountRef,
    NonPositiveScale,
    NegativeRawBalance,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InventoryRecoverySyncDraft {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub path: HarvestRecoveryPath,
    pub full_set_size: Decimal,
    pub sync_id: String,
    pub exact_approval_id: String,
    pub recovery_tx_or_relayer_receipt_id: String,
    pub collateral_delta_usdc: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairArbHarvestPlanRequest {
    pub local: Option<LocalPairArbLedgerProof>,
    pub public_position_rows: Vec<PublicPositionSnapshotRow>,
    pub recovery_event: Option<VerifiedHarvestRecoveryEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairArbHarvestPlanRecord {
    pub event_name: &'static str,
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub gate: HarvestGateResult,
    pub external_proof_present: bool,
    pub ignored_position_row_count: usize,
    pub unknown_side_position_row_count: usize,
    pub recovery_event_present: bool,
    pub sync_draft: Option<InventoryRecoverySyncDraft>,
    pub sync_draft_error: Option<RecoverySyncDraftError>,
    pub execution_permitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverySyncDraftError {
    GateBlocked(HarvestDecision),
    RecoveryRunMismatch,
    RecoveryConditionMismatch,
    RecoveryMarketMismatch,
    RecoveryQtyInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPrimitiveReceiptError {
    MissingRunId,
    MissingConditionId,
    MissingMarketSlug,
    MissingExactApprovalId,
    MissingReceiptId,
    NonPositiveFullSetQty,
    NonPositiveCollateralDelta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPrimitiveRequestError {
    MissingExactApprovalId,
    GateBlocked(HarvestDecision),
    RecoveryPathUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPrimitiveReceiptBuildError {
    MissingReceiptId,
    CollateralDelta(CollateralBalanceDeltaError),
    PrimitiveReceipt(RecoveryPrimitiveReceiptError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPrimitiveWrapperFixtureError {
    PreCollateral(CollateralBalanceObservationBuildError),
    PostCollateral(CollateralBalanceObservationBuildError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct HarvestGateResult {
    pub decision: HarvestDecision,
    pub fail_closed: bool,
    pub reason: String,
    pub planned_harvest_qty: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairArbHarvesterEvidence {
    pub local: Option<LocalPairArbLedgerProof>,
    pub external: Option<ExternalPositionProof>,
    pub recovery: RecoveryEvidence,
}

impl PairArbHarvesterEvidence {
    pub fn evaluate(&self) -> HarvestGateResult {
        evaluate_harvest_candidate(self.local.as_ref(), self.external.as_ref(), &self.recovery)
    }
}

pub fn normalize_public_position_side(
    row: &PublicPositionSnapshotRow,
) -> Option<PositionTokenSide> {
    match row.outcome.trim().to_ascii_lowercase().as_str() {
        "yes" | "up" => Some(PositionTokenSide::Yes),
        "no" | "down" => Some(PositionTokenSide::No),
        _ => match row.outcome_index {
            Some(0) => Some(PositionTokenSide::Yes),
            Some(1) => Some(PositionTokenSide::No),
            _ => None,
        },
    }
}

/// Build an external position proof from public Data API-like position rows.
///
/// The adapter is intentionally read-only and conservative: rows for other
/// conditions are ignored, unknown sides are counted, and no proof is returned
/// if the requested condition is absent.
pub fn external_position_proof_from_public_rows(
    condition_id: &str,
    rows: &[PublicPositionSnapshotRow],
) -> ExternalPositionAdapterResult {
    let mut yes_qty = Decimal::ZERO;
    let mut no_qty = Decimal::ZERO;
    let mut mergeable_yes_qty = Decimal::ZERO;
    let mut mergeable_no_qty = Decimal::ZERO;
    let mut redeemable = false;
    let mut matched_rows = 0_usize;
    let mut ignored_row_count = 0_usize;
    let mut unknown_side_row_count = 0_usize;

    for row in rows {
        if row.condition_id != condition_id {
            ignored_row_count += 1;
            continue;
        }
        matched_rows += 1;
        redeemable |= row.redeemable;
        match normalize_public_position_side(row) {
            Some(PositionTokenSide::Yes) => {
                yes_qty += row.size;
                if row.mergeable {
                    mergeable_yes_qty += row.size;
                }
            }
            Some(PositionTokenSide::No) => {
                no_qty += row.size;
                if row.mergeable {
                    mergeable_no_qty += row.size;
                }
            }
            None => unknown_side_row_count += 1,
        }
    }

    let proof = if matched_rows == 0 {
        None
    } else {
        Some(ExternalPositionProof {
            condition_id: condition_id.to_string(),
            yes_qty,
            no_qty,
            mergeable_full_set_qty: mergeable_yes_qty.min(mergeable_no_qty),
            redeemable,
        })
    };

    ExternalPositionAdapterResult {
        proof,
        ignored_row_count,
        unknown_side_row_count,
    }
}

pub fn recovery_evidence_from_verified_event(
    event: &VerifiedHarvestRecoveryEvent,
) -> RecoveryEvidence {
    RecoveryEvidence {
        exact_approval_id: Some(event.exact_approval_id.clone()),
        recovery_tx_or_relayer_receipt_id: Some(event.recovery_tx_or_relayer_receipt_id.clone()),
        collateral_delta_usdc: Some(event.collateral_delta_usdc),
    }
}

pub fn verified_recovery_event_from_primitive_receipt(
    receipt: &RecoveryPrimitiveReceipt,
) -> Result<VerifiedHarvestRecoveryEvent, RecoveryPrimitiveReceiptError> {
    if receipt.run_id.trim().is_empty() {
        return Err(RecoveryPrimitiveReceiptError::MissingRunId);
    }
    if receipt.condition_id.trim().is_empty() {
        return Err(RecoveryPrimitiveReceiptError::MissingConditionId);
    }
    if receipt.market_slug.trim().is_empty() {
        return Err(RecoveryPrimitiveReceiptError::MissingMarketSlug);
    }
    if receipt.exact_approval_id.trim().is_empty() {
        return Err(RecoveryPrimitiveReceiptError::MissingExactApprovalId);
    }
    if receipt.submit_id_or_tx_hash.trim().is_empty()
        && receipt
            .confirmed_tx_hash
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return Err(RecoveryPrimitiveReceiptError::MissingReceiptId);
    }
    if receipt.full_set_qty <= Decimal::ZERO {
        return Err(RecoveryPrimitiveReceiptError::NonPositiveFullSetQty);
    }
    let collateral_delta_usdc = receipt.collateral_delta_usdc();
    if collateral_delta_usdc <= Decimal::ZERO {
        return Err(RecoveryPrimitiveReceiptError::NonPositiveCollateralDelta);
    }

    let recovery_tx_or_relayer_receipt_id = receipt
        .confirmed_tx_hash
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| receipt.submit_id_or_tx_hash.trim())
        .to_string();

    Ok(VerifiedHarvestRecoveryEvent {
        run_id: receipt.run_id.clone(),
        condition_id: receipt.condition_id.clone(),
        market_slug: receipt.market_slug.clone(),
        path: receipt.path,
        full_set_qty: receipt.full_set_qty,
        exact_approval_id: receipt.exact_approval_id.clone(),
        recovery_tx_or_relayer_receipt_id,
        collateral_delta_usdc,
    })
}

pub fn collateral_balance_observation_from_raw_units(
    source: &str,
    account_ref: &str,
    observed_at_ms: u64,
    raw_balance_units: i128,
    scale_units_per_usdc: i128,
) -> Result<CollateralBalanceObservation, CollateralBalanceObservationBuildError> {
    let source = source.trim();
    let account_ref = account_ref.trim();
    if source.is_empty() {
        return Err(CollateralBalanceObservationBuildError::MissingSource);
    }
    if account_ref.is_empty() {
        return Err(CollateralBalanceObservationBuildError::MissingAccountRef);
    }
    if scale_units_per_usdc <= 0 {
        return Err(CollateralBalanceObservationBuildError::NonPositiveScale);
    }
    if raw_balance_units < 0 {
        return Err(CollateralBalanceObservationBuildError::NegativeRawBalance);
    }

    let balance_usdc = Decimal::from_i128_with_scale(raw_balance_units, 0)
        / Decimal::from_i128_with_scale(scale_units_per_usdc, 0);
    Ok(CollateralBalanceObservation {
        source: source.to_string(),
        account_ref: account_ref.to_string(),
        observed_at_ms,
        balance_usdc,
    })
}

pub fn collateral_delta_from_balance_observations(
    pre: &CollateralBalanceObservation,
    post: &CollateralBalanceObservation,
) -> Result<Decimal, CollateralBalanceDeltaError> {
    if pre.source.trim().is_empty() || post.source.trim().is_empty() {
        return Err(CollateralBalanceDeltaError::MissingSource);
    }
    if pre.account_ref.trim().is_empty() || post.account_ref.trim().is_empty() {
        return Err(CollateralBalanceDeltaError::MissingAccountRef);
    }
    if pre.source != post.source {
        return Err(CollateralBalanceDeltaError::SourceMismatch);
    }
    if pre.account_ref != post.account_ref {
        return Err(CollateralBalanceDeltaError::AccountMismatch);
    }
    if post.observed_at_ms <= pre.observed_at_ms {
        return Err(CollateralBalanceDeltaError::NonMonotonicObservationTime);
    }
    if pre.balance_usdc < Decimal::ZERO || post.balance_usdc < Decimal::ZERO {
        return Err(CollateralBalanceDeltaError::NegativeBalance);
    }
    let delta = post.balance_usdc - pre.balance_usdc;
    if delta <= Decimal::ZERO {
        return Err(CollateralBalanceDeltaError::NonPositiveDelta);
    }
    Ok(delta)
}

pub fn recovery_primitive_receipt_from_request_and_balance_observations(
    request: &RecoveryPrimitiveRequest,
    submit_id_or_tx_hash: &str,
    confirmed_tx_hash: Option<&str>,
    pre: &CollateralBalanceObservation,
    post: &CollateralBalanceObservation,
) -> Result<RecoveryPrimitiveReceipt, RecoveryPrimitiveReceiptBuildError> {
    let submit_id_or_tx_hash = submit_id_or_tx_hash.trim();
    let confirmed_tx_hash = confirmed_tx_hash
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if submit_id_or_tx_hash.is_empty() && confirmed_tx_hash.is_none() {
        return Err(RecoveryPrimitiveReceiptBuildError::MissingReceiptId);
    }

    collateral_delta_from_balance_observations(pre, post)
        .map_err(RecoveryPrimitiveReceiptBuildError::CollateralDelta)?;

    let receipt = RecoveryPrimitiveReceipt {
        run_id: request.run_id.clone(),
        condition_id: request.condition_id.clone(),
        market_slug: request.market_slug.clone(),
        path: request.path,
        full_set_qty: request.full_set_qty,
        exact_approval_id: request.exact_approval_id.clone(),
        submit_id_or_tx_hash: submit_id_or_tx_hash.to_string(),
        confirmed_tx_hash,
        pre_collateral_usdc: pre.balance_usdc,
        post_collateral_usdc: post.balance_usdc,
    };

    verified_recovery_event_from_primitive_receipt(&receipt)
        .map_err(RecoveryPrimitiveReceiptBuildError::PrimitiveReceipt)?;
    Ok(receipt)
}

pub fn recovery_primitive_wrapper_shape_review_only(
    evidence: RecoveryPrimitiveWrapperEvidence,
) -> RecoveryPrimitiveWrapperReviewRecord {
    match recovery_primitive_receipt_from_request_and_balance_observations(
        &evidence.request,
        &evidence.submit_id_or_tx_hash,
        evidence.confirmed_tx_hash.as_deref(),
        &evidence.pre_collateral,
        &evidence.post_collateral,
    ) {
        Ok(receipt) => RecoveryPrimitiveWrapperReviewRecord {
            receipt: Some(receipt),
            receipt_error: None,
            effectful_execution_permitted: RECOVERY_PRIMITIVE_WRAPPER_EFFECTFUL_EXECUTION_DEFAULT,
        },
        Err(receipt_error) => RecoveryPrimitiveWrapperReviewRecord {
            receipt: None,
            receipt_error: Some(receipt_error),
            effectful_execution_permitted: RECOVERY_PRIMITIVE_WRAPPER_EFFECTFUL_EXECUTION_DEFAULT,
        },
    }
}

pub fn recovery_primitive_wrapper_review_record_to_recorder_payload(
    record: &RecoveryPrimitiveWrapperReviewRecord,
) -> Value {
    let verified_event = record
        .receipt
        .as_ref()
        .and_then(|receipt| verified_recovery_event_from_primitive_receipt(receipt).ok());
    json!({
        "event": RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT,
        "wrapper_schema_version": RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_VERSION,
        "fixture_error": Value::Null,
        "receipt_present": record.receipt.is_some(),
        "receipt_error": record.receipt_error.as_ref().map(|err| format!("{err:?}")),
        "effectful_execution_permitted": record.effectful_execution_permitted,
        "run_id": record.receipt.as_ref().map(|receipt| receipt.run_id.as_str()),
        "condition_id": record.receipt.as_ref().map(|receipt| receipt.condition_id.as_str()),
        "market_slug": record.receipt.as_ref().map(|receipt| receipt.market_slug.as_str()),
        "path": record.receipt.as_ref().map(|receipt| receipt.path.as_str()),
        "full_set_qty": record.receipt.as_ref().map(|receipt| receipt.full_set_qty.to_string()),
        "submit_id_or_tx_hash_present": record
            .receipt
            .as_ref()
            .map(|receipt| !receipt.submit_id_or_tx_hash.trim().is_empty())
            .unwrap_or(false),
        "confirmed_tx_hash_present": record
            .receipt
            .as_ref()
            .map(|receipt| receipt.confirmed_tx_hash.as_deref().map(str::trim).unwrap_or_default() != "")
            .unwrap_or(false),
        "verified_event_present": verified_event.is_some(),
        "recovery_tx_or_relayer_receipt_id": verified_event
            .as_ref()
            .map(|event| event.recovery_tx_or_relayer_receipt_id.as_str()),
        "collateral_delta_usdc": record
            .receipt
            .as_ref()
            .map(|receipt| receipt.collateral_delta_usdc().to_string()),
    })
}

pub fn recovery_primitive_wrapper_json_fixture_review_only(
    evidence: RecoveryPrimitiveWrapperEvidence,
) -> Value {
    let record = recovery_primitive_wrapper_shape_review_only(evidence);
    recovery_primitive_wrapper_review_record_to_recorder_payload(&record)
}

impl RecoveryPrimitiveWrapperFixtureRow {
    pub fn into_evidence(
        &self,
    ) -> Result<RecoveryPrimitiveWrapperEvidence, RecoveryPrimitiveWrapperFixtureError> {
        let pre_collateral = collateral_balance_observation_from_raw_units(
            &self.collateral_source,
            &self.collateral_account_ref,
            self.pre_observed_at_ms,
            self.pre_raw_balance_units,
            self.scale_units_per_usdc,
        )
        .map_err(RecoveryPrimitiveWrapperFixtureError::PreCollateral)?;
        let post_collateral = collateral_balance_observation_from_raw_units(
            &self.collateral_source,
            &self.collateral_account_ref,
            self.post_observed_at_ms,
            self.post_raw_balance_units,
            self.scale_units_per_usdc,
        )
        .map_err(RecoveryPrimitiveWrapperFixtureError::PostCollateral)?;
        Ok(RecoveryPrimitiveWrapperEvidence {
            request: self.request.clone(),
            submit_id_or_tx_hash: self.submit_id_or_tx_hash.clone(),
            confirmed_tx_hash: self.confirmed_tx_hash.clone(),
            pre_collateral,
            post_collateral,
        })
    }
}

pub fn recovery_primitive_wrapper_fixture_row_to_recorder_payload(
    row: &RecoveryPrimitiveWrapperFixtureRow,
) -> Value {
    match row.into_evidence() {
        Ok(evidence) => recovery_primitive_wrapper_json_fixture_review_only(evidence),
        Err(fixture_error) => json!({
            "event": RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT,
            "wrapper_schema_version": RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_VERSION,
            "receipt_present": false,
            "receipt_error": Value::Null,
            "fixture_error": format!("{fixture_error:?}"),
            "effectful_execution_permitted": RECOVERY_PRIMITIVE_WRAPPER_EFFECTFUL_EXECUTION_DEFAULT,
            "run_id": row.request.run_id.as_str(),
            "condition_id": row.request.condition_id.as_str(),
            "market_slug": row.request.market_slug.as_str(),
            "path": row.request.path.as_str(),
            "full_set_qty": row.request.full_set_qty.to_string(),
            "submit_id_or_tx_hash_present": !row.submit_id_or_tx_hash.trim().is_empty(),
            "confirmed_tx_hash_present": row.confirmed_tx_hash.as_deref().map(str::trim).unwrap_or_default() != "",
            "verified_event_present": false,
            "recovery_tx_or_relayer_receipt_id": Value::Null,
            "collateral_delta_usdc": Value::Null,
        }),
    }
}

pub fn recovery_primitive_wrapper_json_fixture_corpus_review_only(
    rows: &[RecoveryPrimitiveWrapperFixtureRow],
) -> Vec<Value> {
    rows.iter()
        .map(recovery_primitive_wrapper_fixture_row_to_recorder_payload)
        .collect()
}

pub fn recovery_primitive_wrapper_review_payload_schema_failures(payload: &Value) -> Vec<String> {
    let Some(object) = payload.as_object() else {
        return vec!["payload_not_json_object".to_string()];
    };
    let mut failures = Vec::new();
    for key in RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_KEYS {
        if !object.contains_key(key) {
            failures.push(format!("missing_key:{key}"));
        }
    }
    for key in object.keys() {
        if !RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_KEYS.contains(&key.as_str()) {
            failures.push(format!("unexpected_key:{key}"));
        }
    }
    if object.get("event")
        != Some(&Value::String(
            RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT.into(),
        ))
    {
        failures.push("invalid_event".to_string());
    }
    if object.get("wrapper_schema_version").and_then(Value::as_u64)
        != Some(u64::from(
            RECOVERY_PRIMITIVE_WRAPPER_REVIEW_PAYLOAD_SCHEMA_VERSION,
        ))
    {
        failures.push("invalid_wrapper_schema_version".to_string());
    }
    if object
        .get("effectful_execution_permitted")
        .and_then(Value::as_bool)
        != Some(false)
    {
        failures.push("effectful_execution_permitted_not_false".to_string());
    }
    failures
}

pub fn recovery_primitive_request_from_reconciled_proofs(
    local: &LocalPairArbLedgerProof,
    external: &ExternalPositionProof,
    path: HarvestRecoveryPath,
    exact_approval_id: &str,
) -> Result<RecoveryPrimitiveRequest, RecoveryPrimitiveRequestError> {
    let exact_approval_id = exact_approval_id.trim();
    if exact_approval_id.is_empty() {
        return Err(RecoveryPrimitiveRequestError::MissingExactApprovalId);
    }

    let base_gate = evaluate_harvest_candidate(
        Some(local),
        Some(external),
        &RecoveryEvidence {
            exact_approval_id: Some(exact_approval_id.to_string()),
            recovery_tx_or_relayer_receipt_id: None,
            collateral_delta_usdc: None,
        },
    );
    if base_gate.decision != HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta {
        return Err(RecoveryPrimitiveRequestError::GateBlocked(
            base_gate.decision,
        ));
    }

    match path {
        HarvestRecoveryPath::CompleteSetMerge => {
            if external.mergeable_full_set_qty < base_gate.planned_harvest_qty {
                return Err(RecoveryPrimitiveRequestError::RecoveryPathUnavailable);
            }
        }
        HarvestRecoveryPath::PostResolutionRedeem => {
            if !external.redeemable {
                return Err(RecoveryPrimitiveRequestError::RecoveryPathUnavailable);
            }
        }
    }

    Ok(RecoveryPrimitiveRequest {
        run_id: local.run_id.clone(),
        condition_id: local.condition_id.clone(),
        market_slug: local.market_slug.clone(),
        path,
        full_set_qty: base_gate.planned_harvest_qty,
        exact_approval_id: exact_approval_id.to_string(),
    })
}

/// Build a future inventory sync draft from a fully verified recovery event.
///
/// This does not emit `InventoryEvent::Merge`. It only returns an auditable
/// draft after the same fail-closed harvest gate passes.
pub fn inventory_recovery_sync_draft_from_verified_event(
    local: &LocalPairArbLedgerProof,
    external: &ExternalPositionProof,
    event: &VerifiedHarvestRecoveryEvent,
) -> Result<InventoryRecoverySyncDraft, RecoverySyncDraftError> {
    if event.run_id != local.run_id {
        return Err(RecoverySyncDraftError::RecoveryRunMismatch);
    }
    if event.condition_id != local.condition_id || event.condition_id != external.condition_id {
        return Err(RecoverySyncDraftError::RecoveryConditionMismatch);
    }
    if event.market_slug != local.market_slug {
        return Err(RecoverySyncDraftError::RecoveryMarketMismatch);
    }

    let recovery = recovery_evidence_from_verified_event(event);
    let gate = evaluate_harvest_candidate(Some(local), Some(external), &recovery);
    if gate.fail_closed {
        return Err(RecoverySyncDraftError::GateBlocked(gate.decision));
    }
    if event.full_set_qty <= Decimal::ZERO || event.full_set_qty > gate.planned_harvest_qty {
        return Err(RecoverySyncDraftError::RecoveryQtyInvalid);
    }

    Ok(InventoryRecoverySyncDraft {
        run_id: event.run_id.clone(),
        condition_id: event.condition_id.clone(),
        market_slug: event.market_slug.clone(),
        path: event.path,
        full_set_size: event.full_set_qty,
        sync_id: format!(
            "{}:{}:{}",
            event.run_id, event.condition_id, event.recovery_tx_or_relayer_receipt_id
        ),
        exact_approval_id: event.exact_approval_id.clone(),
        recovery_tx_or_relayer_receipt_id: event.recovery_tx_or_relayer_receipt_id.clone(),
        collateral_delta_usdc: event.collateral_delta_usdc,
    })
}

pub fn plan_pair_arb_harvest_review_only(
    request: PairArbHarvestPlanRequest,
) -> PairArbHarvestPlanRecord {
    let condition_id = request
        .local
        .as_ref()
        .map(|local| local.condition_id.clone())
        .or_else(|| {
            request
                .recovery_event
                .as_ref()
                .map(|event| event.condition_id.clone())
        })
        .unwrap_or_default();
    let market_slug = request
        .local
        .as_ref()
        .map(|local| local.market_slug.clone())
        .or_else(|| {
            request
                .recovery_event
                .as_ref()
                .map(|event| event.market_slug.clone())
        });

    let external_adapter = if condition_id.trim().is_empty() {
        ExternalPositionAdapterResult {
            proof: None,
            ignored_row_count: request.public_position_rows.len(),
            unknown_side_row_count: 0,
        }
    } else {
        external_position_proof_from_public_rows(&condition_id, &request.public_position_rows)
    };
    let recovery = request
        .recovery_event
        .as_ref()
        .map(recovery_evidence_from_verified_event)
        .unwrap_or_default();
    let gate = evaluate_harvest_candidate(
        request.local.as_ref(),
        external_adapter.proof.as_ref(),
        &recovery,
    );

    let (sync_draft, sync_draft_error) = match (
        request.local.as_ref(),
        external_adapter.proof.as_ref(),
        request.recovery_event.as_ref(),
    ) {
        (Some(local), Some(external), Some(event)) => {
            match inventory_recovery_sync_draft_from_verified_event(local, external, event) {
                Ok(draft) => (Some(draft), None),
                Err(err) => (None, Some(err)),
            }
        }
        _ => (None, None),
    };

    PairArbHarvestPlanRecord {
        event_name: PAIR_ARB_HARVESTER_DECISION_EVENT,
        condition_id,
        market_slug,
        gate,
        external_proof_present: external_adapter.proof.is_some(),
        ignored_position_row_count: external_adapter.ignored_row_count,
        unknown_side_position_row_count: external_adapter.unknown_side_row_count,
        recovery_event_present: request.recovery_event.is_some(),
        sync_draft,
        sync_draft_error,
        execution_permitted: false,
    }
}

pub fn harvester_plan_record_to_recorder_payload(record: &PairArbHarvestPlanRecord) -> Value {
    json!({
        "event": record.event_name,
        "condition_id": record.condition_id,
        "market_slug": record.market_slug,
        "decision": record.gate.decision.as_str(),
        "fail_closed": record.gate.fail_closed,
        "reason": record.gate.reason,
        "planned_harvest_qty": record.gate.planned_harvest_qty.to_string(),
        "external_proof_present": record.external_proof_present,
        "ignored_position_row_count": record.ignored_position_row_count,
        "unknown_side_position_row_count": record.unknown_side_position_row_count,
        "recovery_event_present": record.recovery_event_present,
        "sync_draft_present": record.sync_draft.is_some(),
        "sync_draft_error": record.sync_draft_error.as_ref().map(|err| format!("{err:?}")),
        "execution_permitted": record.execution_permitted,
    })
}

/// Fixture/schema-shaped input row used by review-only adapters.
///
/// This is intentionally not an execution command. It exists to map no-order
/// reconciliation artifacts into the same production gate used by future
/// harvester code.
#[derive(Debug, Clone, PartialEq)]
pub struct PairArbHarvesterFixtureRow {
    pub run_id: String,
    pub condition_id: String,
    pub market_slug: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub same_run_buy_order_ids: String,
    pub same_run_filled_yes_qty: String,
    pub same_run_filled_no_qty: String,
    pub same_run_open_yes_qty: String,
    pub same_run_open_no_qty: String,
    pub external_position_yes_qty: String,
    pub external_position_no_qty: String,
    pub external_mergeable_full_set_qty: String,
    pub redeemable: bool,
    pub exact_approval_id: Option<String>,
    pub recovery_tx_or_relayer_receipt_id: Option<String>,
    pub collateral_delta_usdc: Option<String>,
}

impl PairArbHarvesterFixtureRow {
    pub fn into_evidence(&self) -> PairArbHarvesterEvidence {
        let local = self.local_ledger_proof();
        let external = self.external_position_proof();
        let recovery = RecoveryEvidence {
            exact_approval_id: self.exact_approval_id.clone(),
            recovery_tx_or_relayer_receipt_id: self.recovery_tx_or_relayer_receipt_id.clone(),
            collateral_delta_usdc: self
                .collateral_delta_usdc
                .as_deref()
                .and_then(parse_review_decimal),
        };
        PairArbHarvesterEvidence {
            local,
            external,
            recovery,
        }
    }

    fn local_ledger_proof(&self) -> Option<LocalPairArbLedgerProof> {
        if is_missing_fixture_marker(&self.condition_id)
            || is_missing_fixture_marker(&self.token_id_yes)
            || is_missing_fixture_marker(&self.token_id_no)
            || is_missing_fixture_marker(&self.same_run_buy_order_ids)
        {
            return None;
        }
        Some(LocalPairArbLedgerProof {
            run_id: self.run_id.clone(),
            condition_id: self.condition_id.clone(),
            market_slug: self.market_slug.clone(),
            yes_token_id: self.token_id_yes.clone(),
            no_token_id: self.token_id_no.clone(),
            same_run_buy_order_ids: parse_order_ids(&self.same_run_buy_order_ids),
            filled_yes_qty: parse_review_decimal(&self.same_run_filled_yes_qty)?,
            filled_no_qty: parse_review_decimal(&self.same_run_filled_no_qty)?,
            open_yes_qty: parse_review_decimal(&self.same_run_open_yes_qty)?,
            open_no_qty: parse_review_decimal(&self.same_run_open_no_qty)?,
        })
    }

    fn external_position_proof(&self) -> Option<ExternalPositionProof> {
        if is_missing_fixture_marker(&self.condition_id)
            || is_missing_fixture_marker(&self.external_position_yes_qty)
            || is_missing_fixture_marker(&self.external_position_no_qty)
            || is_missing_fixture_marker(&self.external_mergeable_full_set_qty)
        {
            return None;
        }
        Some(ExternalPositionProof {
            condition_id: self.condition_id.clone(),
            yes_qty: parse_review_decimal(&self.external_position_yes_qty)?,
            no_qty: parse_review_decimal(&self.external_position_no_qty)?,
            mergeable_full_set_qty: parse_review_decimal(&self.external_mergeable_full_set_qty)?,
            redeemable: self.redeemable,
        })
    }
}

impl HarvestGateResult {
    fn blocked(decision: HarvestDecision, reason: impl Into<String>, qty: Decimal) -> Self {
        Self {
            decision,
            fail_closed: true,
            reason: reason.into(),
            planned_harvest_qty: qty,
        }
    }

    fn fixture_allow(reason: impl Into<String>, qty: Decimal) -> Self {
        Self {
            decision: HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl,
            fail_closed: false,
            reason: reason.into(),
            planned_harvest_qty: qty,
        }
    }
}

fn is_missing_fixture_marker(raw: &str) -> bool {
    let normalized = raw.trim();
    normalized.is_empty()
        || normalized.eq_ignore_ascii_case("UNKNOWN_NO_ORDER_FIXTURE")
        || normalized.eq_ignore_ascii_case("NO_ORDER_COUNTERFACTUAL")
        || normalized.eq_ignore_ascii_case("COUNTERFACTUAL_NOT_BOUND_NO_ORDER")
}

fn parse_order_ids(raw: &str) -> Vec<String> {
    if is_missing_fixture_marker(raw) {
        return Vec::new();
    }
    raw.split([';', '|', ','])
        .map(str::trim)
        .filter(|v| !is_missing_fixture_marker(v))
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_review_decimal(raw: &str) -> Option<Decimal> {
    if is_missing_fixture_marker(raw) {
        return None;
    }
    Decimal::from_str(raw.trim()).ok()
}

fn missing_lineage_fields(local: &LocalPairArbLedgerProof) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if local.run_id.trim().is_empty() {
        missing.push("run_id");
    }
    if local.condition_id.trim().is_empty() {
        missing.push("condition_id");
    }
    if local.market_slug.trim().is_empty() {
        missing.push("market_slug");
    }
    if local.yes_token_id.trim().is_empty() {
        missing.push("yes_token_id");
    }
    if local.no_token_id.trim().is_empty() {
        missing.push("no_token_id");
    }
    if local.same_run_buy_order_ids.is_empty() {
        missing.push("same_run_buy_order_ids");
    }
    missing
}

/// Evaluate whether a paired strategy position is safe to harvest.
///
/// A non-blocking result is still not live authorization. It is only the
/// fixture-level positive control that would let a future exact-approved caller
/// proceed to a separately reviewed merge/redeem primitive.
pub fn evaluate_harvest_candidate(
    local: Option<&LocalPairArbLedgerProof>,
    external: Option<&ExternalPositionProof>,
    recovery: &RecoveryEvidence,
) -> HarvestGateResult {
    let Some(local) = local else {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockMissingLocalLedger,
            "same_run_local_ledger_absent",
            Decimal::ZERO,
        );
    };

    let missing = missing_lineage_fields(local);
    if !missing.is_empty() {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockMissingLineageField,
            format!("missing_{}", missing.join(",")),
            Decimal::ZERO,
        );
    }

    if local.open_yes_qty > Decimal::ZERO || local.open_no_qty > Decimal::ZERO {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockPendingOrderRemainder,
            "same_run_open_order_remainder_nonzero",
            Decimal::ZERO,
        );
    }

    let pairable = local.pairable_full_set_qty();
    if pairable <= Decimal::ZERO {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockNoLocalPairableFullSet,
            "local_pairable_full_set_qty_zero",
            Decimal::ZERO,
        );
    }

    let Some(external) = external else {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockExternalPositionUnknown,
            "external_position_snapshot_absent",
            pairable,
        );
    };

    if external.condition_id != local.condition_id {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockConditionMismatch,
            "external_condition_id_does_not_match_local_ledger",
            pairable,
        );
    }

    if external.yes_qty < pairable || external.no_qty < pairable {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockPositionMismatch,
            "external_yes_or_no_qty_below_local_pairable_qty",
            pairable,
        );
    }

    if external.mergeable_full_set_qty < pairable && !external.redeemable {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockNoMergeableFullSet,
            "external_mergeable_full_set_qty_below_pairable_qty",
            pairable,
        );
    }

    if external.redeemable && recovery.exact_approval_id.is_none() {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockApprovalRequiredRedeemableWithoutApproval,
            "redeemable_path_seen_but_redeem_not_exact_approved",
            pairable,
        );
    }

    if recovery.exact_approval_id.is_none() {
        return HarvestGateResult::blocked(
            HarvestDecision::PairCoveredReconciledReviewOnlyApprovalRequired,
            "pair_reconciled_but_merge_or_redeem_requires_fresh_exact_approval",
            pairable,
        );
    }

    if recovery
        .recovery_tx_or_relayer_receipt_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
        || recovery.collateral_delta_usdc.unwrap_or(Decimal::ZERO) <= Decimal::ZERO
    {
        return HarvestGateResult::blocked(
            HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta,
            "missing_tx_or_positive_collateral_delta",
            pairable,
        );
    }

    HarvestGateResult::fixture_allow("all_fixture_preconditions_present", pairable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn good_local() -> LocalPairArbLedgerProof {
        LocalPairArbLedgerProof {
            run_id: "B_S5U_FIXTURE_RUN".to_string(),
            condition_id: "0xcondition".to_string(),
            market_slug: "btc-updown-5m-fixture".to_string(),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            same_run_buy_order_ids: vec!["buy-yes".to_string(), "buy-no".to_string()],
            filled_yes_qty: dec!(5),
            filled_no_qty: dec!(5),
            open_yes_qty: Decimal::ZERO,
            open_no_qty: Decimal::ZERO,
        }
    }

    fn good_external() -> ExternalPositionProof {
        ExternalPositionProof {
            condition_id: "0xcondition".to_string(),
            yes_qty: dec!(5),
            no_qty: dec!(5),
            mergeable_full_set_qty: dec!(5),
            redeemable: false,
        }
    }

    fn public_position(
        condition_id: &str,
        outcome: &str,
        outcome_index: Option<u8>,
        size: Decimal,
        mergeable: bool,
        redeemable: bool,
    ) -> PublicPositionSnapshotRow {
        PublicPositionSnapshotRow {
            condition_id: condition_id.to_string(),
            outcome: outcome.to_string(),
            outcome_index,
            size,
            mergeable,
            redeemable,
        }
    }

    fn assert_decision(
        expected: HarvestDecision,
        local: Option<&LocalPairArbLedgerProof>,
        external: Option<&ExternalPositionProof>,
        recovery: &RecoveryEvidence,
    ) -> HarvestGateResult {
        let result = evaluate_harvest_candidate(local, external, recovery);
        assert_eq!(result.decision, expected);
        result
    }

    fn s5o_fixture_row(market_slug: String) -> PairArbHarvesterFixtureRow {
        PairArbHarvesterFixtureRow {
            run_id: "b_strategy_canary_s5o_no_order_reconciliation_fixture_20260607T031500Z"
                .to_string(),
            condition_id: "COUNTERFACTUAL_NOT_BOUND_NO_ORDER".to_string(),
            market_slug,
            token_id_yes: "COUNTERFACTUAL_NOT_BOUND_NO_ORDER".to_string(),
            token_id_no: "COUNTERFACTUAL_NOT_BOUND_NO_ORDER".to_string(),
            same_run_buy_order_ids: "NO_ORDER_COUNTERFACTUAL".to_string(),
            same_run_filled_yes_qty: "5.0".to_string(),
            same_run_filled_no_qty: "5.0".to_string(),
            same_run_open_yes_qty: "0.0".to_string(),
            same_run_open_no_qty: "0.0".to_string(),
            external_position_yes_qty: "UNKNOWN_NO_ORDER_FIXTURE".to_string(),
            external_position_no_qty: "UNKNOWN_NO_ORDER_FIXTURE".to_string(),
            external_mergeable_full_set_qty: "UNKNOWN_NO_ORDER_FIXTURE".to_string(),
            redeemable: false,
            exact_approval_id: None,
            recovery_tx_or_relayer_receipt_id: None,
            collateral_delta_usdc: None,
        }
    }

    fn parse_s5o_fixture_rows() -> Vec<PairArbHarvesterFixtureRow> {
        let btc_rows =
            (0..20).map(|idx| s5o_fixture_row(format!("btc-updown-5m-s5o-fixture-{idx:02}")));
        let eth_rows =
            (0..11).map(|idx| s5o_fixture_row(format!("eth-updown-5m-s5o-fixture-{idx:02}")));
        btc_rows.chain(eth_rows).collect()
    }

    #[test]
    fn harvester_is_disabled_by_default() {
        assert!(!PAIR_ARB_HARVESTER_ENABLED_DEFAULT);
    }

    #[test]
    fn missing_local_ledger_fails_closed() {
        let external = good_external();
        let result = assert_decision(
            HarvestDecision::BlockMissingLocalLedger,
            None,
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn s5o_no_order_fixture_rows_fail_closed_through_production_adapter() {
        let rows = parse_s5o_fixture_rows();
        assert_eq!(rows.len(), 31);
        let btc_rows = rows
            .iter()
            .filter(|row| row.market_slug.starts_with("btc-"))
            .count();
        let eth_rows = rows
            .iter()
            .filter(|row| row.market_slug.starts_with("eth-"))
            .count();
        assert_eq!(btc_rows, 20);
        assert_eq!(eth_rows, 11);

        for row in rows {
            let evidence = row.into_evidence();
            let result = evidence.evaluate();
            assert_eq!(result.decision, HarvestDecision::BlockMissingLocalLedger);
            assert!(result.fail_closed);
            assert_eq!(result.planned_harvest_qty, Decimal::ZERO);
        }
    }

    #[test]
    fn fixture_row_adapter_maps_reconciled_pair_to_approval_required() {
        let row = PairArbHarvesterFixtureRow {
            run_id: "B_S5V_FIXTURE_RUN".to_string(),
            condition_id: "0xcondition".to_string(),
            market_slug: "eth-updown-5m-fixture".to_string(),
            token_id_yes: "yes-token".to_string(),
            token_id_no: "no-token".to_string(),
            same_run_buy_order_ids: "buy-yes|buy-no".to_string(),
            same_run_filled_yes_qty: "5".to_string(),
            same_run_filled_no_qty: "5".to_string(),
            same_run_open_yes_qty: "0".to_string(),
            same_run_open_no_qty: "0".to_string(),
            external_position_yes_qty: "5".to_string(),
            external_position_no_qty: "5".to_string(),
            external_mergeable_full_set_qty: "5".to_string(),
            redeemable: false,
            exact_approval_id: None,
            recovery_tx_or_relayer_receipt_id: None,
            collateral_delta_usdc: None,
        };
        let evidence = row.into_evidence();
        let result = evidence.evaluate();
        assert_eq!(
            result.decision,
            HarvestDecision::PairCoveredReconciledReviewOnlyApprovalRequired
        );
        assert!(result.fail_closed);
        assert_eq!(result.planned_harvest_qty, dec!(5));
    }

    #[test]
    fn public_position_adapter_aggregates_up_down_mergeable_full_set() {
        let rows = vec![
            public_position("0xcondition", "Up", None, dec!(3), true, false),
            public_position("0xcondition", "YES", None, dec!(2), true, false),
            public_position("0xcondition", "Down", None, dec!(4), true, false),
            public_position("0xcondition", "NO", None, dec!(1), true, false),
            public_position("0xother", "YES", None, dec!(99), true, false),
        ];
        let adapted = external_position_proof_from_public_rows("0xcondition", &rows);
        let proof = adapted.proof.expect("condition rows should produce proof");
        assert_eq!(adapted.ignored_row_count, 1);
        assert_eq!(adapted.unknown_side_row_count, 0);
        assert_eq!(proof.yes_qty, dec!(5));
        assert_eq!(proof.no_qty, dec!(5));
        assert_eq!(proof.mergeable_full_set_qty, dec!(5));
        assert!(!proof.redeemable);

        let local = good_local();
        let result =
            evaluate_harvest_candidate(Some(&local), Some(&proof), &RecoveryEvidence::default());
        assert_eq!(
            result.decision,
            HarvestDecision::PairCoveredReconciledReviewOnlyApprovalRequired
        );
    }

    #[test]
    fn public_position_adapter_uses_outcome_index_when_label_unknown() {
        let rows = vec![
            public_position("0xcondition", "Mystery", Some(0), dec!(5), true, false),
            public_position("0xcondition", "Mystery", Some(1), dec!(5), true, false),
        ];
        let adapted = external_position_proof_from_public_rows("0xcondition", &rows);
        let proof = adapted.proof.expect("indexed rows should produce proof");
        assert_eq!(adapted.unknown_side_row_count, 0);
        assert_eq!(proof.yes_qty, dec!(5));
        assert_eq!(proof.no_qty, dec!(5));
    }

    #[test]
    fn public_position_adapter_counts_unknown_side_rows() {
        let rows = vec![public_position(
            "0xcondition",
            "Mystery",
            None,
            dec!(5),
            true,
            false,
        )];
        let adapted = external_position_proof_from_public_rows("0xcondition", &rows);
        let proof = adapted
            .proof
            .expect("matched unknown-side row still produces zero-qty proof");
        assert_eq!(adapted.unknown_side_row_count, 1);
        assert_eq!(proof.yes_qty, Decimal::ZERO);
        assert_eq!(proof.no_qty, Decimal::ZERO);
    }

    #[test]
    fn public_position_adapter_redeemable_without_approval_fails_closed() {
        let rows = vec![
            public_position("0xcondition", "YES", None, dec!(5), false, true),
            public_position("0xcondition", "NO", None, dec!(5), false, true),
        ];
        let adapted = external_position_proof_from_public_rows("0xcondition", &rows);
        let proof = adapted.proof.expect("redeemable rows should produce proof");
        assert!(proof.redeemable);
        assert_eq!(proof.mergeable_full_set_qty, Decimal::ZERO);

        let local = good_local();
        let result =
            evaluate_harvest_candidate(Some(&local), Some(&proof), &RecoveryEvidence::default());
        assert_eq!(
            result.decision,
            HarvestDecision::BlockApprovalRequiredRedeemableWithoutApproval
        );
        assert!(result.fail_closed);
    }

    fn good_recovery_event() -> VerifiedHarvestRecoveryEvent {
        VerifiedHarvestRecoveryEvent {
            run_id: "B_S5U_FIXTURE_RUN".to_string(),
            condition_id: "0xcondition".to_string(),
            market_slug: "btc-updown-5m-fixture".to_string(),
            path: HarvestRecoveryPath::CompleteSetMerge,
            full_set_qty: dec!(5),
            exact_approval_id: "fixture-approval".to_string(),
            recovery_tx_or_relayer_receipt_id: "fixture-tx".to_string(),
            collateral_delta_usdc: dec!(5),
        }
    }

    fn good_primitive_receipt() -> RecoveryPrimitiveReceipt {
        RecoveryPrimitiveReceipt {
            run_id: "B_S5U_FIXTURE_RUN".to_string(),
            condition_id: "0xcondition".to_string(),
            market_slug: "btc-updown-5m-fixture".to_string(),
            path: HarvestRecoveryPath::CompleteSetMerge,
            full_set_qty: dec!(5),
            exact_approval_id: "fixture-approval".to_string(),
            submit_id_or_tx_hash: "fixture-relayer-submit-id".to_string(),
            confirmed_tx_hash: Some("fixture-confirmed-tx-hash".to_string()),
            pre_collateral_usdc: dec!(100),
            post_collateral_usdc: dec!(105),
        }
    }

    fn good_recovery_primitive_request() -> RecoveryPrimitiveRequest {
        recovery_primitive_request_from_reconciled_proofs(
            &good_local(),
            &good_external(),
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("good fixture proofs should build primitive request")
    }

    fn good_wrapper_fixture_row() -> RecoveryPrimitiveWrapperFixtureRow {
        RecoveryPrimitiveWrapperFixtureRow {
            request: good_recovery_primitive_request(),
            submit_id_or_tx_hash: "fixture-submit-id".to_string(),
            confirmed_tx_hash: Some("fixture-confirmed-tx".to_string()),
            collateral_source: "clob_balance_allowance".to_string(),
            collateral_account_ref: "B_PUBLIC_ACCOUNT_REF".to_string(),
            pre_observed_at_ms: 1_780_000_000_000,
            post_observed_at_ms: 1_780_000_001_000,
            pre_raw_balance_units: 100_000_000,
            post_raw_balance_units: 105_000_000,
            scale_units_per_usdc: 1_000_000,
        }
    }

    fn collateral_observation(
        balance_usdc: Decimal,
        observed_at_ms: u64,
    ) -> CollateralBalanceObservation {
        CollateralBalanceObservation {
            source: "clob_balance_allowance".to_string(),
            account_ref: "B_PUBLIC_ACCOUNT_REF".to_string(),
            observed_at_ms,
            balance_usdc,
        }
    }

    #[test]
    fn s6e_collateral_balance_observations_compute_positive_delta() {
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);
        let post = collateral_observation(dec!(105), 1_780_000_001_000);
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Ok(dec!(5))
        );
    }

    #[test]
    fn s6e_collateral_balance_observations_block_source_and_account_mismatch() {
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);

        let mut post = collateral_observation(dec!(105), 1_780_000_001_000);
        post.source = "other_source".to_string();
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Err(CollateralBalanceDeltaError::SourceMismatch)
        );

        let mut post = collateral_observation(dec!(105), 1_780_000_001_000);
        post.account_ref = "OTHER_ACCOUNT".to_string();
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Err(CollateralBalanceDeltaError::AccountMismatch)
        );
    }

    #[test]
    fn s6e_collateral_balance_observations_block_stale_or_nonpositive_delta() {
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);

        let post = collateral_observation(dec!(105), 1_780_000_000_000);
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Err(CollateralBalanceDeltaError::NonMonotonicObservationTime)
        );

        let post = collateral_observation(dec!(100), 1_780_000_001_000);
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Err(CollateralBalanceDeltaError::NonPositiveDelta)
        );
    }

    #[test]
    fn s6g_raw_balance_units_adapter_builds_observation() {
        let observation = collateral_balance_observation_from_raw_units(
            " clob_balance_allowance ",
            " B_PUBLIC_ACCOUNT_REF ",
            1_780_000_000_000,
            105_250_000,
            1_000_000,
        )
        .expect("positive raw balance units should build observation");
        assert_eq!(observation.source, "clob_balance_allowance");
        assert_eq!(observation.account_ref, "B_PUBLIC_ACCOUNT_REF");
        assert_eq!(observation.balance_usdc, dec!(105.25));
    }

    #[test]
    fn s6g_raw_balance_units_adapter_feeds_delta_contract() {
        let pre = collateral_balance_observation_from_raw_units(
            "clob_balance_allowance",
            "B_PUBLIC_ACCOUNT_REF",
            1_780_000_000_000,
            100_000_000,
            1_000_000,
        )
        .expect("pre balance should parse");
        let post = collateral_balance_observation_from_raw_units(
            "clob_balance_allowance",
            "B_PUBLIC_ACCOUNT_REF",
            1_780_000_001_000,
            105_000_000,
            1_000_000,
        )
        .expect("post balance should parse");
        assert_eq!(
            collateral_delta_from_balance_observations(&pre, &post),
            Ok(dec!(5))
        );
    }

    #[test]
    fn s6g_raw_balance_units_adapter_blocks_missing_source_or_account() {
        assert_eq!(
            collateral_balance_observation_from_raw_units(
                " ",
                "B_PUBLIC_ACCOUNT_REF",
                1_780_000_000_000,
                100_000_000,
                1_000_000,
            ),
            Err(CollateralBalanceObservationBuildError::MissingSource)
        );
        assert_eq!(
            collateral_balance_observation_from_raw_units(
                "clob_balance_allowance",
                " ",
                1_780_000_000_000,
                100_000_000,
                1_000_000,
            ),
            Err(CollateralBalanceObservationBuildError::MissingAccountRef)
        );
    }

    #[test]
    fn s6g_raw_balance_units_adapter_blocks_bad_units() {
        assert_eq!(
            collateral_balance_observation_from_raw_units(
                "clob_balance_allowance",
                "B_PUBLIC_ACCOUNT_REF",
                1_780_000_000_000,
                100_000_000,
                0,
            ),
            Err(CollateralBalanceObservationBuildError::NonPositiveScale)
        );
        assert_eq!(
            collateral_balance_observation_from_raw_units(
                "clob_balance_allowance",
                "B_PUBLIC_ACCOUNT_REF",
                1_780_000_000_000,
                -1,
                1_000_000,
            ),
            Err(CollateralBalanceObservationBuildError::NegativeRawBalance)
        );
    }

    #[test]
    fn s6f_recovery_primitive_request_builds_for_merge_path() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled mergeable full set should build primitive request");
        assert_eq!(request.run_id, local.run_id);
        assert_eq!(request.condition_id, local.condition_id);
        assert_eq!(request.path, HarvestRecoveryPath::CompleteSetMerge);
        assert_eq!(request.full_set_qty, dec!(5));
        assert_eq!(request.exact_approval_id, "fixture-approval");
    }

    #[test]
    fn s6f_recovery_primitive_request_builds_for_redeem_path() {
        let local = good_local();
        let mut external = good_external();
        external.mergeable_full_set_qty = Decimal::ZERO;
        external.redeemable = true;
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::PostResolutionRedeem,
            "fixture-approval",
        )
        .expect("redeemable reconciled pair should build primitive request");
        assert_eq!(request.path, HarvestRecoveryPath::PostResolutionRedeem);
        assert_eq!(request.full_set_qty, dec!(5));
    }

    #[test]
    fn s6f_recovery_primitive_request_blocks_missing_approval_or_unavailable_path() {
        let local = good_local();
        let external = good_external();
        assert_eq!(
            recovery_primitive_request_from_reconciled_proofs(
                &local,
                &external,
                HarvestRecoveryPath::CompleteSetMerge,
                "  ",
            ),
            Err(RecoveryPrimitiveRequestError::MissingExactApprovalId)
        );
        assert_eq!(
            recovery_primitive_request_from_reconciled_proofs(
                &local,
                &external,
                HarvestRecoveryPath::PostResolutionRedeem,
                "fixture-approval",
            ),
            Err(RecoveryPrimitiveRequestError::RecoveryPathUnavailable)
        );
    }

    #[test]
    fn s6f_recovery_primitive_request_surfaces_base_gate_block() {
        let mut local = good_local();
        local.open_yes_qty = dec!(1);
        let external = good_external();
        assert_eq!(
            recovery_primitive_request_from_reconciled_proofs(
                &local,
                &external,
                HarvestRecoveryPath::CompleteSetMerge,
                "fixture-approval",
            ),
            Err(RecoveryPrimitiveRequestError::GateBlocked(
                HarvestDecision::BlockPendingOrderRemainder
            ))
        );
    }

    #[test]
    fn s6i_receipt_factory_builds_verified_event_and_sync_draft() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let pre = collateral_balance_observation_from_raw_units(
            "clob_balance_allowance",
            "B_PUBLIC_ACCOUNT_REF",
            1_780_000_000_000,
            100_000_000,
            1_000_000,
        )
        .expect("pre balance should parse");
        let post = collateral_balance_observation_from_raw_units(
            "clob_balance_allowance",
            "B_PUBLIC_ACCOUNT_REF",
            1_780_000_001_000,
            105_000_000,
            1_000_000,
        )
        .expect("post balance should parse");

        let receipt = recovery_primitive_receipt_from_request_and_balance_observations(
            &request,
            "fixture-submit-id",
            Some("fixture-confirmed-tx"),
            &pre,
            &post,
        )
        .expect("request plus positive collateral delta should build receipt");
        let event = verified_recovery_event_from_primitive_receipt(&receipt)
            .expect("factory receipt should convert to verified event");
        assert_eq!(
            event.recovery_tx_or_relayer_receipt_id,
            "fixture-confirmed-tx"
        );
        assert_eq!(event.collateral_delta_usdc, dec!(5));

        let draft = inventory_recovery_sync_draft_from_verified_event(&local, &external, &event)
            .expect("verified event should build review-only sync draft");
        assert_eq!(draft.full_set_size, dec!(5));
        assert_eq!(draft.collateral_delta_usdc, dec!(5));
    }

    #[test]
    fn s6i_receipt_factory_blocks_missing_receipt_id() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);
        let post = collateral_observation(dec!(105), 1_780_000_001_000);

        assert_eq!(
            recovery_primitive_receipt_from_request_and_balance_observations(
                &request, " ", None, &pre, &post,
            ),
            Err(RecoveryPrimitiveReceiptBuildError::MissingReceiptId)
        );
    }

    #[test]
    fn s6i_receipt_factory_blocks_bad_balance_delta() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);
        let post = collateral_observation(dec!(100), 1_780_000_001_000);

        assert_eq!(
            recovery_primitive_receipt_from_request_and_balance_observations(
                &request,
                "fixture-submit-id",
                None,
                &pre,
                &post,
            ),
            Err(RecoveryPrimitiveReceiptBuildError::CollateralDelta(
                CollateralBalanceDeltaError::NonPositiveDelta
            ))
        );
    }

    #[test]
    fn s6i_receipt_factory_surfaces_invalid_request_lineage() {
        let local = good_local();
        let external = good_external();
        let mut request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        request.condition_id.clear();
        let pre = collateral_observation(dec!(100), 1_780_000_000_000);
        let post = collateral_observation(dec!(105), 1_780_000_001_000);

        assert_eq!(
            recovery_primitive_receipt_from_request_and_balance_observations(
                &request,
                "fixture-submit-id",
                None,
                &pre,
                &post,
            ),
            Err(RecoveryPrimitiveReceiptBuildError::PrimitiveReceipt(
                RecoveryPrimitiveReceiptError::MissingConditionId
            ))
        );
    }

    #[test]
    fn s6j_wrapper_shape_builds_receipt_but_never_permits_execution() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let evidence = RecoveryPrimitiveWrapperEvidence {
            request,
            submit_id_or_tx_hash: "fixture-submit-id".to_string(),
            confirmed_tx_hash: Some("fixture-confirmed-tx".to_string()),
            pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
            post_collateral: collateral_observation(dec!(105), 1_780_000_001_000),
        };
        let record = recovery_primitive_wrapper_shape_review_only(evidence);
        assert!(!record.effectful_execution_permitted);
        assert!(record.receipt_error.is_none());
        let receipt = record
            .receipt
            .expect("wrapper evidence should build receipt");
        assert_eq!(
            receipt.confirmed_tx_hash.as_deref(),
            Some("fixture-confirmed-tx")
        );
        let event = verified_recovery_event_from_primitive_receipt(&receipt)
            .expect("wrapper receipt should convert to verified event");
        assert_eq!(event.collateral_delta_usdc, dec!(5));
    }

    #[test]
    fn s6j_wrapper_shape_surfaces_missing_receipt_without_execution_permission() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let record =
            recovery_primitive_wrapper_shape_review_only(RecoveryPrimitiveWrapperEvidence {
                request,
                submit_id_or_tx_hash: " ".to_string(),
                confirmed_tx_hash: None,
                pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
                post_collateral: collateral_observation(dec!(105), 1_780_000_001_000),
            });
        assert!(!record.effectful_execution_permitted);
        assert!(record.receipt.is_none());
        assert_eq!(
            record.receipt_error,
            Some(RecoveryPrimitiveReceiptBuildError::MissingReceiptId)
        );
    }

    #[test]
    fn s6j_wrapper_shape_surfaces_bad_delta_without_execution_permission() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");
        let record =
            recovery_primitive_wrapper_shape_review_only(RecoveryPrimitiveWrapperEvidence {
                request,
                submit_id_or_tx_hash: "fixture-submit-id".to_string(),
                confirmed_tx_hash: None,
                pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
                post_collateral: collateral_observation(dec!(100), 1_780_000_001_000),
            });
        assert!(!record.effectful_execution_permitted);
        assert!(record.receipt.is_none());
        assert_eq!(
            record.receipt_error,
            Some(RecoveryPrimitiveReceiptBuildError::CollateralDelta(
                CollateralBalanceDeltaError::NonPositiveDelta
            ))
        );
    }

    #[test]
    fn s6k_wrapper_json_fixture_runner_emits_success_payload_without_execution() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");

        let payload =
            recovery_primitive_wrapper_json_fixture_review_only(RecoveryPrimitiveWrapperEvidence {
                request,
                submit_id_or_tx_hash: "fixture-submit-id".to_string(),
                confirmed_tx_hash: Some("fixture-confirmed-tx".to_string()),
                pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
                post_collateral: collateral_observation(dec!(105), 1_780_000_001_000),
            });

        assert_eq!(payload["event"], RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT);
        assert_eq!(payload["receipt_present"], true);
        assert_eq!(payload["receipt_error"], Value::Null);
        assert_eq!(payload["effectful_execution_permitted"], false);
        assert_eq!(payload["path"], "COMPLETE_SET_MERGE");
        assert_eq!(payload["full_set_qty"], "5");
        assert_eq!(payload["submit_id_or_tx_hash_present"], true);
        assert_eq!(payload["confirmed_tx_hash_present"], true);
        assert_eq!(payload["verified_event_present"], true);
        assert_eq!(
            payload["recovery_tx_or_relayer_receipt_id"],
            "fixture-confirmed-tx"
        );
        assert_eq!(payload["collateral_delta_usdc"], "5");
    }

    #[test]
    fn s6k_wrapper_json_fixture_runner_emits_error_payload_without_execution() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");

        let payload =
            recovery_primitive_wrapper_json_fixture_review_only(RecoveryPrimitiveWrapperEvidence {
                request,
                submit_id_or_tx_hash: "fixture-submit-id".to_string(),
                confirmed_tx_hash: None,
                pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
                post_collateral: collateral_observation(dec!(100), 1_780_000_001_000),
            });

        assert_eq!(payload["event"], RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT);
        assert_eq!(payload["receipt_present"], false);
        assert_eq!(
            payload["receipt_error"],
            "CollateralDelta(NonPositiveDelta)"
        );
        assert_eq!(payload["effectful_execution_permitted"], false);
        assert_eq!(payload["verified_event_present"], false);
        assert_eq!(payload["recovery_tx_or_relayer_receipt_id"], Value::Null);
        assert_eq!(payload["collateral_delta_usdc"], Value::Null);
    }

    #[test]
    fn s6k_wrapper_record_payload_uses_submit_id_when_no_confirmed_hash() {
        let local = good_local();
        let external = good_external();
        let request = recovery_primitive_request_from_reconciled_proofs(
            &local,
            &external,
            HarvestRecoveryPath::CompleteSetMerge,
            "fixture-approval",
        )
        .expect("reconciled proof should build primitive request");

        let payload =
            recovery_primitive_wrapper_json_fixture_review_only(RecoveryPrimitiveWrapperEvidence {
                request,
                submit_id_or_tx_hash: "fixture-submit-id".to_string(),
                confirmed_tx_hash: None,
                pre_collateral: collateral_observation(dec!(100), 1_780_000_000_000),
                post_collateral: collateral_observation(dec!(105), 1_780_000_001_000),
            });

        assert_eq!(payload["receipt_present"], true);
        assert_eq!(payload["confirmed_tx_hash_present"], false);
        assert_eq!(
            payload["recovery_tx_or_relayer_receipt_id"],
            "fixture-submit-id"
        );
        assert_eq!(payload["effectful_execution_permitted"], false);
    }

    #[test]
    fn s6l_wrapper_fixture_row_emits_success_payload() {
        let payload =
            recovery_primitive_wrapper_fixture_row_to_recorder_payload(&good_wrapper_fixture_row());
        assert_eq!(payload["event"], RECOVERY_PRIMITIVE_WRAPPER_REVIEW_EVENT);
        assert_eq!(payload["receipt_present"], true);
        assert_eq!(payload["fixture_error"], Value::Null);
        assert_eq!(payload["effectful_execution_permitted"], false);
        assert_eq!(
            payload["recovery_tx_or_relayer_receipt_id"],
            "fixture-confirmed-tx"
        );
        assert_eq!(payload["collateral_delta_usdc"], "5");
    }

    #[test]
    fn s6l_wrapper_fixture_row_blocks_bad_balance_scale() {
        let mut row = good_wrapper_fixture_row();
        row.scale_units_per_usdc = 0;
        let payload = recovery_primitive_wrapper_fixture_row_to_recorder_payload(&row);
        assert_eq!(payload["receipt_present"], false);
        assert_eq!(payload["fixture_error"], "PreCollateral(NonPositiveScale)");
        assert_eq!(payload["receipt_error"], Value::Null);
        assert_eq!(payload["effectful_execution_permitted"], false);
        assert_eq!(payload["verified_event_present"], false);
        assert_eq!(payload["collateral_delta_usdc"], Value::Null);
    }

    #[test]
    fn s6l_wrapper_fixture_corpus_keeps_mixed_rows_fail_closed() {
        let good = good_wrapper_fixture_row();
        let mut missing_receipt = good_wrapper_fixture_row();
        missing_receipt.submit_id_or_tx_hash.clear();
        missing_receipt.confirmed_tx_hash = None;
        let mut bad_delta = good_wrapper_fixture_row();
        bad_delta.post_raw_balance_units = bad_delta.pre_raw_balance_units;

        let payloads = recovery_primitive_wrapper_json_fixture_corpus_review_only(&[
            good,
            missing_receipt,
            bad_delta,
        ]);
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[0]["receipt_present"], true);
        assert_eq!(payloads[0]["effectful_execution_permitted"], false);
        assert_eq!(payloads[1]["receipt_present"], false);
        assert_eq!(payloads[1]["receipt_error"], "MissingReceiptId");
        assert_eq!(payloads[1]["effectful_execution_permitted"], false);
        assert_eq!(payloads[2]["receipt_present"], false);
        assert_eq!(
            payloads[2]["receipt_error"],
            "CollateralDelta(NonPositiveDelta)"
        );
        assert_eq!(payloads[2]["effectful_execution_permitted"], false);
    }

    #[test]
    fn s6m_wrapper_payload_schema_accepts_success_error_and_fixture_error() {
        let success =
            recovery_primitive_wrapper_fixture_row_to_recorder_payload(&good_wrapper_fixture_row());
        assert!(recovery_primitive_wrapper_review_payload_schema_failures(&success).is_empty());
        assert_eq!(success["wrapper_schema_version"], 1);
        assert_eq!(success["fixture_error"], Value::Null);

        let mut missing_receipt = good_wrapper_fixture_row();
        missing_receipt.submit_id_or_tx_hash.clear();
        missing_receipt.confirmed_tx_hash = None;
        let receipt_error =
            recovery_primitive_wrapper_fixture_row_to_recorder_payload(&missing_receipt);
        assert!(
            recovery_primitive_wrapper_review_payload_schema_failures(&receipt_error).is_empty()
        );
        assert_eq!(receipt_error["receipt_error"], "MissingReceiptId");
        assert_eq!(receipt_error["fixture_error"], Value::Null);

        let mut bad_scale = good_wrapper_fixture_row();
        bad_scale.scale_units_per_usdc = 0;
        let fixture_error = recovery_primitive_wrapper_fixture_row_to_recorder_payload(&bad_scale);
        assert!(
            recovery_primitive_wrapper_review_payload_schema_failures(&fixture_error).is_empty()
        );
        assert_eq!(
            fixture_error["fixture_error"],
            "PreCollateral(NonPositiveScale)"
        );
        assert_eq!(fixture_error["receipt_error"], Value::Null);
    }

    #[test]
    fn s6m_wrapper_payload_schema_detects_missing_and_unexpected_keys() {
        let mut payload =
            recovery_primitive_wrapper_fixture_row_to_recorder_payload(&good_wrapper_fixture_row());
        let object = payload.as_object_mut().expect("payload should be object");
        object.remove("receipt_present");
        object.insert("unexpected".to_string(), json!(true));

        let failures = recovery_primitive_wrapper_review_payload_schema_failures(&payload);
        assert!(failures.contains(&"missing_key:receipt_present".to_string()));
        assert!(failures.contains(&"unexpected_key:unexpected".to_string()));
    }

    #[test]
    fn s6m_wrapper_payload_schema_detects_bad_event_version_or_permission() {
        let mut payload =
            recovery_primitive_wrapper_fixture_row_to_recorder_payload(&good_wrapper_fixture_row());
        let object = payload.as_object_mut().expect("payload should be object");
        object.insert("event".to_string(), json!("wrong_event"));
        object.insert("wrapper_schema_version".to_string(), json!(999));
        object.insert("effectful_execution_permitted".to_string(), json!(true));

        let failures = recovery_primitive_wrapper_review_payload_schema_failures(&payload);
        assert!(failures.contains(&"invalid_event".to_string()));
        assert!(failures.contains(&"invalid_wrapper_schema_version".to_string()));
        assert!(failures.contains(&"effectful_execution_permitted_not_false".to_string()));
    }

    #[test]
    fn s6d_primitive_receipt_converts_to_verified_recovery_event() {
        let receipt = good_primitive_receipt();
        let event = verified_recovery_event_from_primitive_receipt(&receipt)
            .expect("complete primitive receipt should convert");
        assert_eq!(event.run_id, "B_S5U_FIXTURE_RUN");
        assert_eq!(event.condition_id, "0xcondition");
        assert_eq!(event.path, HarvestRecoveryPath::CompleteSetMerge);
        assert_eq!(
            event.recovery_tx_or_relayer_receipt_id,
            "fixture-confirmed-tx-hash"
        );
        assert_eq!(event.collateral_delta_usdc, dec!(5));
    }

    #[test]
    fn s6d_primitive_receipt_uses_submit_id_when_confirmed_hash_absent() {
        let mut receipt = good_primitive_receipt();
        receipt.confirmed_tx_hash = None;
        let event = verified_recovery_event_from_primitive_receipt(&receipt)
            .expect("submit id is acceptable receipt lineage before confirmed hash exists");
        assert_eq!(
            event.recovery_tx_or_relayer_receipt_id,
            "fixture-relayer-submit-id"
        );
    }

    #[test]
    fn s6d_primitive_receipt_blocks_missing_receipt_or_nonpositive_delta() {
        let mut receipt = good_primitive_receipt();
        receipt.submit_id_or_tx_hash.clear();
        receipt.confirmed_tx_hash = None;
        assert_eq!(
            verified_recovery_event_from_primitive_receipt(&receipt),
            Err(RecoveryPrimitiveReceiptError::MissingReceiptId)
        );

        let mut receipt = good_primitive_receipt();
        receipt.post_collateral_usdc = receipt.pre_collateral_usdc;
        assert_eq!(
            verified_recovery_event_from_primitive_receipt(&receipt),
            Err(RecoveryPrimitiveReceiptError::NonPositiveCollateralDelta)
        );

        let mut receipt = good_primitive_receipt();
        receipt.full_set_qty = Decimal::ZERO;
        assert_eq!(
            verified_recovery_event_from_primitive_receipt(&receipt),
            Err(RecoveryPrimitiveReceiptError::NonPositiveFullSetQty)
        );
    }

    #[test]
    fn s6d_primitive_receipt_planner_path_remains_review_only() {
        let local = good_local();
        let event = verified_recovery_event_from_primitive_receipt(&good_primitive_receipt())
            .expect("complete primitive receipt should convert");
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: Some(event),
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl
        );
        assert!(record.sync_draft.is_some());
        assert!(!record.execution_permitted);
    }

    #[test]
    fn recovery_sync_draft_requires_gate_pass_and_keeps_recovery_lineage() {
        let local = good_local();
        let external = good_external();
        let event = good_recovery_event();
        let draft = inventory_recovery_sync_draft_from_verified_event(&local, &external, &event)
            .expect("complete recovery evidence should build sync draft");
        assert_eq!(draft.full_set_size, dec!(5));
        assert_eq!(draft.path, HarvestRecoveryPath::CompleteSetMerge);
        assert_eq!(draft.exact_approval_id, "fixture-approval");
        assert_eq!(draft.recovery_tx_or_relayer_receipt_id, "fixture-tx");
        assert_eq!(draft.collateral_delta_usdc, dec!(5));
        assert!(draft.sync_id.contains("B_S5U_FIXTURE_RUN"));
        assert!(draft.sync_id.contains("0xcondition"));
        assert!(draft.sync_id.contains("fixture-tx"));
    }

    #[test]
    fn recovery_sync_draft_supports_redeemable_path_after_approval_and_receipt() {
        let local = good_local();
        let mut external = good_external();
        external.mergeable_full_set_qty = Decimal::ZERO;
        external.redeemable = true;
        let mut event = good_recovery_event();
        event.path = HarvestRecoveryPath::PostResolutionRedeem;
        let draft = inventory_recovery_sync_draft_from_verified_event(&local, &external, &event)
            .expect("redeemable path with approval and receipt should build sync draft");
        assert_eq!(draft.path, HarvestRecoveryPath::PostResolutionRedeem);
        assert_eq!(draft.full_set_size, dec!(5));
    }

    #[test]
    fn recovery_sync_draft_blocks_missing_collateral_delta_via_gate() {
        let local = good_local();
        let external = good_external();
        let mut event = good_recovery_event();
        event.collateral_delta_usdc = Decimal::ZERO;
        let err = inventory_recovery_sync_draft_from_verified_event(&local, &external, &event)
            .expect_err("zero collateral delta must block inventory sync");
        assert_eq!(
            err,
            RecoverySyncDraftError::GateBlocked(
                HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta
            )
        );
    }

    #[test]
    fn recovery_sync_draft_blocks_lineage_mismatch_and_oversized_qty() {
        let local = good_local();
        let external = good_external();

        let mut event = good_recovery_event();
        event.run_id = "OTHER_RUN".to_string();
        assert_eq!(
            inventory_recovery_sync_draft_from_verified_event(&local, &external, &event),
            Err(RecoverySyncDraftError::RecoveryRunMismatch)
        );

        let mut event = good_recovery_event();
        event.condition_id = "0xother".to_string();
        assert_eq!(
            inventory_recovery_sync_draft_from_verified_event(&local, &external, &event),
            Err(RecoverySyncDraftError::RecoveryConditionMismatch)
        );

        let mut event = good_recovery_event();
        event.market_slug = "other-market".to_string();
        assert_eq!(
            inventory_recovery_sync_draft_from_verified_event(&local, &external, &event),
            Err(RecoverySyncDraftError::RecoveryMarketMismatch)
        );

        let mut event = good_recovery_event();
        event.full_set_qty = dec!(6);
        assert_eq!(
            inventory_recovery_sync_draft_from_verified_event(&local, &external, &event),
            Err(RecoverySyncDraftError::RecoveryQtyInvalid)
        );
    }

    #[test]
    fn review_only_planner_blocks_missing_local_ledger_and_emits_payload_shape() {
        let request = PairArbHarvestPlanRequest {
            local: None,
            public_position_rows: vec![public_position(
                "0xcondition",
                "YES",
                None,
                dec!(5),
                true,
                false,
            )],
            recovery_event: None,
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(record.event_name, PAIR_ARB_HARVESTER_DECISION_EVENT);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::BlockMissingLocalLedger
        );
        assert!(record.gate.fail_closed);
        assert!(!record.execution_permitted);
        assert!(!record.external_proof_present);
        assert_eq!(record.ignored_position_row_count, 1);

        let payload = harvester_plan_record_to_recorder_payload(&record);
        assert_eq!(
            payload.pointer("/event").and_then(|v| v.as_str()),
            Some(PAIR_ARB_HARVESTER_DECISION_EVENT)
        );
        assert_eq!(
            payload.pointer("/decision").and_then(|v| v.as_str()),
            Some("BLOCK_MISSING_LOCAL_LEDGER")
        );
        assert_eq!(
            payload
                .pointer("/execution_permitted")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn review_only_planner_composes_public_positions_and_blocks_without_approval() {
        let local = good_local();
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: None,
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert!(record.external_proof_present);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::PairCoveredReconciledReviewOnlyApprovalRequired
        );
        assert!(record.gate.fail_closed);
        assert!(record.sync_draft.is_none());
        assert!(record.sync_draft_error.is_none());
        assert!(!record.execution_permitted);
    }

    #[test]
    fn review_only_planner_builds_sync_draft_but_never_execution_permission() {
        let local = good_local();
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: Some(good_recovery_event()),
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl
        );
        assert!(!record.gate.fail_closed);
        assert!(record.sync_draft.is_some());
        assert!(record.sync_draft_error.is_none());
        assert!(!record.execution_permitted);

        let payload = harvester_plan_record_to_recorder_payload(&record);
        assert_eq!(
            payload
                .pointer("/sync_draft_present")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/execution_permitted")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn review_only_planner_surfaces_recovery_lineage_error() {
        let local = good_local();
        let mut event = good_recovery_event();
        event.market_slug = "other-market".to_string();
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: Some(event),
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert!(record.sync_draft.is_none());
        assert_eq!(
            record.sync_draft_error,
            Some(RecoverySyncDraftError::RecoveryMarketMismatch)
        );
        assert!(!record.execution_permitted);

        let payload = harvester_plan_record_to_recorder_payload(&record);
        assert!(payload
            .pointer("/sync_draft_error")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .contains("RecoveryMarketMismatch"));
    }

    #[test]
    fn s6c_existing_public_redeemable_position_is_not_strategy_recovery_without_local_ledger() {
        let request = PairArbHarvestPlanRequest {
            local: None,
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), false, true),
                public_position("0xcondition", "NO", None, dec!(5), false, true),
            ],
            recovery_event: None,
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::BlockMissingLocalLedger
        );
        assert!(record.gate.fail_closed);
        assert!(record.sync_draft.is_none());
        assert!(!record.execution_permitted);
    }

    #[test]
    fn s6c_recovery_receipt_without_collateral_delta_blocks_inventory_sync() {
        let local = good_local();
        let mut event = good_recovery_event();
        event.collateral_delta_usdc = Decimal::ZERO;
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: Some(event),
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta
        );
        assert!(record.gate.fail_closed);
        assert!(record.sync_draft.is_none());
        assert_eq!(
            record.sync_draft_error,
            Some(RecoverySyncDraftError::GateBlocked(
                HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta
            ))
        );
        assert!(!record.execution_permitted);
    }

    #[test]
    fn s6c_complete_recovery_proof_builds_review_sync_draft_only() {
        let local = good_local();
        let request = PairArbHarvestPlanRequest {
            local: Some(local),
            public_position_rows: vec![
                public_position("0xcondition", "YES", None, dec!(5), true, false),
                public_position("0xcondition", "NO", None, dec!(5), true, false),
            ],
            recovery_event: Some(good_recovery_event()),
        };
        let record = plan_pair_arb_harvest_review_only(request);
        assert_eq!(
            record.gate.decision,
            HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl
        );
        assert!(!record.gate.fail_closed);
        assert!(record.sync_draft.is_some());
        assert!(!record.execution_permitted);

        let payload = harvester_plan_record_to_recorder_payload(&record);
        assert_eq!(
            payload.pointer("/decision").and_then(|v| v.as_str()),
            Some("WOULD_ALLOW_ONLY_UNDER_FRESH_EXACT_APPROVAL_FIXTURE_CONTROL")
        );
        assert_eq!(
            payload
                .pointer("/sync_draft_present")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/execution_permitted")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn fixture_row_adapter_positive_control_requires_recovery_evidence() {
        let row = PairArbHarvesterFixtureRow {
            run_id: "B_S5V_FIXTURE_RUN".to_string(),
            condition_id: "0xcondition".to_string(),
            market_slug: "eth-updown-5m-fixture".to_string(),
            token_id_yes: "yes-token".to_string(),
            token_id_no: "no-token".to_string(),
            same_run_buy_order_ids: "buy-yes|buy-no".to_string(),
            same_run_filled_yes_qty: "5".to_string(),
            same_run_filled_no_qty: "5".to_string(),
            same_run_open_yes_qty: "0".to_string(),
            same_run_open_no_qty: "0".to_string(),
            external_position_yes_qty: "5".to_string(),
            external_position_no_qty: "5".to_string(),
            external_mergeable_full_set_qty: "5".to_string(),
            redeemable: false,
            exact_approval_id: Some("fixture-approval".to_string()),
            recovery_tx_or_relayer_receipt_id: Some("fixture-tx".to_string()),
            collateral_delta_usdc: Some("5".to_string()),
        };
        let evidence = row.into_evidence();
        let result = evidence.evaluate();
        assert_eq!(
            result.decision,
            HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl
        );
        assert!(!result.fail_closed);
        assert_eq!(result.planned_harvest_qty, dec!(5));
    }

    #[test]
    fn missing_lineage_field_fails_closed() {
        let mut local = good_local();
        local.condition_id.clear();
        let external = good_external();
        let result = assert_decision(
            HarvestDecision::BlockMissingLineageField,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
        assert!(result.reason.contains("condition_id"));
    }

    #[test]
    fn open_order_remainder_fails_closed() {
        let mut local = good_local();
        local.open_no_qty = dec!(1);
        let external = good_external();
        let result = assert_decision(
            HarvestDecision::BlockPendingOrderRemainder,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn no_local_pairable_full_set_fails_closed() {
        let mut local = good_local();
        local.filled_no_qty = Decimal::ZERO;
        let external = good_external();
        let result = assert_decision(
            HarvestDecision::BlockNoLocalPairableFullSet,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn missing_external_position_fails_closed() {
        let local = good_local();
        let result = assert_decision(
            HarvestDecision::BlockExternalPositionUnknown,
            Some(&local),
            None,
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn condition_mismatch_fails_closed() {
        let local = good_local();
        let mut external = good_external();
        external.condition_id = "0xother".to_string();
        let result = assert_decision(
            HarvestDecision::BlockConditionMismatch,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn position_mismatch_fails_closed() {
        let local = good_local();
        let mut external = good_external();
        external.yes_qty = dec!(4);
        let result = assert_decision(
            HarvestDecision::BlockPositionMismatch,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn no_mergeable_full_set_fails_closed() {
        let local = good_local();
        let mut external = good_external();
        external.mergeable_full_set_qty = Decimal::ZERO;
        let result = assert_decision(
            HarvestDecision::BlockNoMergeableFullSet,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn redeemable_without_approval_fails_closed() {
        let local = good_local();
        let mut external = good_external();
        external.redeemable = true;
        let result = assert_decision(
            HarvestDecision::BlockApprovalRequiredRedeemableWithoutApproval,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn pair_reconciled_without_approval_stays_review_only() {
        let local = good_local();
        let external = good_external();
        let result = assert_decision(
            HarvestDecision::PairCoveredReconciledReviewOnlyApprovalRequired,
            Some(&local),
            Some(&external),
            &RecoveryEvidence::default(),
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn approval_without_recovery_receipt_fails_closed() {
        let local = good_local();
        let external = good_external();
        let recovery = RecoveryEvidence {
            exact_approval_id: Some("fixture-approval".to_string()),
            ..RecoveryEvidence::default()
        };
        let result = assert_decision(
            HarvestDecision::BlockMissingRecoveryReceiptOrCollateralDelta,
            Some(&local),
            Some(&external),
            &recovery,
        );
        assert!(result.fail_closed);
    }

    #[test]
    fn positive_control_requires_approval_receipt_and_collateral_delta() {
        let local = good_local();
        let external = good_external();
        let recovery = RecoveryEvidence {
            exact_approval_id: Some("fixture-approval".to_string()),
            recovery_tx_or_relayer_receipt_id: Some("fixture-tx".to_string()),
            collateral_delta_usdc: Some(dec!(5)),
        };
        let result = assert_decision(
            HarvestDecision::WouldAllowOnlyUnderFreshExactApprovalFixtureControl,
            Some(&local),
            Some(&external),
            &recovery,
        );
        assert!(!result.fail_closed);
        assert_eq!(result.planned_harvest_qty, dec!(5));
        assert_eq!(
            result.decision.as_str(),
            "WOULD_ALLOW_ONLY_UNDER_FRESH_EXACT_APPROVAL_FIXTURE_CONTROL"
        );
    }
}
