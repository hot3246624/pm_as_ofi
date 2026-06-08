use std::env;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use anyhow::{anyhow, Context, Result};
use pm_as_ofi::polymarket::clob_v2::{
    build_signed_limit_order_v2, builder_code_from_env, cancel_order_v2, fetch_order_status_v2,
    fetch_trades_for_market_asset_v2, infer_signature_type, post_order_v2, v2_contract_config,
    OrderSizingV2, V2OrderContext,
};
use pm_as_ofi::polymarket::executor::init_clob_client;
use pm_as_ofi::polymarket::messages::TradeDirection;
use pm_as_ofi::polymarket::s8a_order_adapter::{
    apply_s8a_fill_to_inventory, review_s8a_order_status_fill_evidence,
    review_s8a_scout_snapshot_for_limit_entry, S8aAdapterOrderAction,
    S8aControllerAdmissionAdapterContext, S8aFillEvent, S8aInventory, S8aObservedOrderStatus,
    S8aOrderStatusEvidenceSource, S8aOrderStatusFillEvidence, S8aPreparedVenueOrder,
    S8aScoutAdmissionSnapshot, S8aScoutBookSideSnapshot, S8aScoutPublicBuyTrade, S8aVenueAmount,
    S8aVenueOrderType, S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES, S8A_MARKET_BUY_MIN_NOTIONAL_USDC,
    S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES, S8A_MAX_ROUNDS, S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC,
    S8A_SEED_PX_HI, S8A_SEED_PX_LO, S8A_SESSION_HARD_LOSS_CAP_USDC,
};
use pm_as_ofi::polymarket::types::Side;
use pm_as_ofi::polymarket::{
    btc_completion_controller::{BtcCompletionControllerConfig, BtcCompletionControllerState},
    s8a_order_adapter::S8A_NATIVE_RUNTIME_SCOPE,
};
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::response::{
    OpenOrderResponse, PostOrderResponse, TradeResponse,
};
use polymarket_client_sdk::clob::types::{OrderStatusType, OrderType};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use uuid::Uuid;

const SCOPE: &str = S8A_NATIVE_RUNTIME_SCOPE;
const REVIEWED_HOST: &str = "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com";
const OFFICIAL_CLOB_REST_URL: &str = "https://clob.polymarket.com";
const ORDER_PRIMITIVE_NAME: &str = "clob_v2.build_signed_limit_order_v2/post_order_v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    ReviewOnly,
    PreviewNoApproval,
    NoOrderAuthPreview,
    ExactApprovedOrderPathPreview,
    LiveScoutToOrderPreview,
    OneRunDriverPreview,
    OrderStatusFillEvidencePreview,
    Execute,
}

#[derive(Debug, Clone, Default)]
struct Args {
    mode: Option<Mode>,
    reviewed_host: String,
    rest_url: String,
    prepared_order_json: Option<PathBuf>,
    expected_prepared_order_sha256: Option<String>,
    scout_snapshot_json: Option<PathBuf>,
    expected_scout_snapshot_sha256: Option<String>,
    one_run_plan_json: Option<PathBuf>,
    expected_one_run_plan_sha256: Option<String>,
    order_status_fill_evidence_json: Option<PathBuf>,
    expected_order_status_fill_evidence_sha256: Option<String>,
    exact_approval_sha256: Option<String>,
    expected_exact_approval_sha256: Option<String>,
    approval_scope: String,
    order_primitive_name: Option<String>,
    order_primitive_source_sha256: Option<String>,
    no_submit: bool,
    execute_approved: bool,
    print_secret: bool,
    print_raw_signature: bool,
    use_shared_ingress: bool,
    use_c_artifacts: bool,
    allow_online_tuning: bool,
    allow_candidate_import: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PreparedAmount {
    unit: String,
    value: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PreparedOrder {
    source: String,
    condition_id: String,
    token_id: String,
    side: String,
    action: String,
    order_type: String,
    limit_price: Option<f64>,
    amount: PreparedAmount,
    execution_permitted: bool,
    natural_controller_admission: bool,
    forced_complement: bool,
    source_guard_500_passed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OneRunStrategyScope {
    market_family: String,
    fixed_policy: String,
    source_guard_500_required: bool,
    online_tuning_allowed: bool,
    strategy_discovery_allowed: bool,
    candidate_import_allowed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OneRunCaps {
    max_round_count: u32,
    session_hard_loss_cap_usdc: f64,
    per_round_theoretical_max_loss_usdc: f64,
    max_active_market_count: u32,
    max_total_order_submissions: u32,
    max_cumulative_gross_quote_spend_usdc: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OneRunAttempt {
    prepared_order: PreparedOrder,
    actual_filled_qty_shares: f64,
    avg_fill_price: f64,
    order_id_or_failure_recorded: bool,
    filled_qty_from_exchange_or_order_status: bool,
    close_round_after_attempt: bool,
    s7w_reconciliation_passed: bool,
    exact_approved_receipt_tier_required: bool,
    positive_collateral_delta_required_for_recovery: bool,
    no_open_order_remainder: bool,
    residual_exposure_zero: bool,
    realized_loss_delta_usdc: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OneRunPlan {
    source: String,
    effectful_execution_permitted: bool,
    runtime_accepts_only_prepared_orders: bool,
    exact_approval_hash_bound_to_every_submit: bool,
    exact_approval_scope_bound_to_every_submit: bool,
    runtime_order_result_ledger_enabled: bool,
    actual_filled_qty_ledger_updates_inventory: bool,
    submitted_size_counts_as_inventory: bool,
    partial_fill_threshold_enabled: bool,
    forced_complement_allowed: bool,
    s7w_reconciliation_bound_to_run_result: bool,
    review_only_fixture_receipts_accepted: bool,
    opens_ws: bool,
    shared_ingress_dependency: bool,
    uses_c_artifacts: bool,
    secret_or_raw_signature_output_allowed: bool,
    funding_live_latest_or_deploy_requested: bool,
    strategy_scope: OneRunStrategyScope,
    caps: OneRunCaps,
    attempts: Vec<OneRunAttempt>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoutBookSideSnapshotInput {
    token_id: String,
    best_ask: Option<f64>,
    top5_ask_qty: f64,
    book_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoutPublicBuyTradeInput {
    side: String,
    taker_side: String,
    price: f64,
    size: f64,
    event_ts_ms: u64,
    observed_ts_ms: u64,
    recv_lag_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoutAdmissionSnapshotInput {
    asset: String,
    event_reason: String,
    slug: String,
    condition_id: String,
    yes: ScoutBookSideSnapshotInput,
    no: ScoutBookSideSnapshotInput,
    latest_public_buy_trade: Option<ScoutPublicBuyTradeInput>,
    offset_s: f64,
    time_to_end_s: Option<f64>,
    forbidden_decision_fields_present: bool,
    snapshot_index_used_as_rank: bool,
    source_is_b_owned_direct_public_ws: bool,
    shared_ingress_dependency: bool,
    b_owned_direct_public_ws_connection_count: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderStatusFillEvidenceInput {
    prepared_order_sha256: String,
    exact_approval_sha256: String,
    expected_exact_approval_sha256: String,
    exact_approval_hash_bound_to_order: bool,
    prepared_order_hash_bound_to_order: bool,
    approval_scope_matches_s8a: bool,
    order_id: Option<String>,
    failure_recorded: bool,
    status_source: String,
    order_status: String,
    condition_id: String,
    token_id: String,
    side: String,
    action: String,
    condition_token_side_action_match_prepared_order: bool,
    submitted_size_shares: f64,
    actual_filled_qty_shares: f64,
    avg_fill_price: Option<f64>,
    open_order_remainder_qty_shares: f64,
    filled_qty_from_exchange_or_order_status: bool,
    submitted_size_counts_as_inventory: bool,
    partial_fill_threshold_used: bool,
    forced_complement_after_fill: bool,
    no_open_order_remainder_required: bool,
    prints_secret_or_raw_signature: bool,
    uses_shared_ingress_or_shared_ws: bool,
    uses_c_artifacts: bool,
    funding_live_latest_or_deploy_requested: bool,
    effectful_execution_requested_in_review: bool,
}

fn hash64(value: Option<&str>) -> bool {
    value.is_some_and(|v| v.len() == 64 && v.chars().all(|ch| ch.is_ascii_hexdigit()))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn sha256_file(path: &PathBuf) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!("{:x}", sha2::Sha256::digest(bytes)))
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        reviewed_host: REVIEWED_HOST.to_string(),
        rest_url: OFFICIAL_CLOB_REST_URL.to_string(),
        approval_scope: SCOPE.to_string(),
        ..Args::default()
    };
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--mode" => {
                let raw = it.next().ok_or_else(|| anyhow!("--mode requires value"))?;
                args.mode = Some(match raw.as_str() {
                    "review-only" => Mode::ReviewOnly,
                    "preview-no-approval" => Mode::PreviewNoApproval,
                    "no-order-auth-preview" => Mode::NoOrderAuthPreview,
                    "exact-approved-order-path-preview" => Mode::ExactApprovedOrderPathPreview,
                    "live-scout-to-order-preview" => Mode::LiveScoutToOrderPreview,
                    "one-run-driver-preview" => Mode::OneRunDriverPreview,
                    "order-status-fill-evidence-preview" => Mode::OrderStatusFillEvidencePreview,
                    "execute" => Mode::Execute,
                    _ => return Err(anyhow!("unknown mode {raw}")),
                });
            }
            "--reviewed-host" => {
                args.reviewed_host = it
                    .next()
                    .ok_or_else(|| anyhow!("--reviewed-host requires value"))?;
            }
            "--rest-url" => {
                args.rest_url = it
                    .next()
                    .ok_or_else(|| anyhow!("--rest-url requires value"))?;
            }
            "--prepared-order-json" => {
                args.prepared_order_json =
                    Some(PathBuf::from(it.next().ok_or_else(|| {
                        anyhow!("--prepared-order-json requires value")
                    })?));
            }
            "--expected-prepared-order-sha256" => {
                args.expected_prepared_order_sha256 =
                    Some(it.next().ok_or_else(|| {
                        anyhow!("--expected-prepared-order-sha256 requires value")
                    })?);
            }
            "--scout-snapshot-json" => {
                args.scout_snapshot_json =
                    Some(PathBuf::from(it.next().ok_or_else(|| {
                        anyhow!("--scout-snapshot-json requires value")
                    })?));
            }
            "--expected-scout-snapshot-sha256" => {
                args.expected_scout_snapshot_sha256 =
                    Some(it.next().ok_or_else(|| {
                        anyhow!("--expected-scout-snapshot-sha256 requires value")
                    })?);
            }
            "--one-run-plan-json" => {
                args.one_run_plan_json =
                    Some(PathBuf::from(it.next().ok_or_else(|| {
                        anyhow!("--one-run-plan-json requires value")
                    })?));
            }
            "--expected-one-run-plan-sha256" => {
                args.expected_one_run_plan_sha256 = Some(
                    it.next()
                        .ok_or_else(|| anyhow!("--expected-one-run-plan-sha256 requires value"))?,
                );
            }
            "--order-status-fill-evidence-json" => {
                args.order_status_fill_evidence_json =
                    Some(PathBuf::from(it.next().ok_or_else(|| {
                        anyhow!("--order-status-fill-evidence-json requires value")
                    })?));
            }
            "--expected-order-status-fill-evidence-sha256" => {
                args.expected_order_status_fill_evidence_sha256 =
                    Some(it.next().ok_or_else(|| {
                        anyhow!("--expected-order-status-fill-evidence-sha256 requires value")
                    })?);
            }
            "--exact-approval-sha256" => {
                args.exact_approval_sha256 = Some(
                    it.next()
                        .ok_or_else(|| anyhow!("--exact-approval-sha256 requires value"))?,
                );
            }
            "--expected-exact-approval-sha256" => {
                args.expected_exact_approval_sha256 =
                    Some(it.next().ok_or_else(|| {
                        anyhow!("--expected-exact-approval-sha256 requires value")
                    })?);
            }
            "--approval-scope" => {
                args.approval_scope = it
                    .next()
                    .ok_or_else(|| anyhow!("--approval-scope requires value"))?;
            }
            "--order-primitive-name" => {
                args.order_primitive_name = Some(
                    it.next()
                        .ok_or_else(|| anyhow!("--order-primitive-name requires value"))?,
                );
            }
            "--order-primitive-source-sha256" => {
                args.order_primitive_source_sha256 =
                    Some(it.next().ok_or_else(|| {
                        anyhow!("--order-primitive-source-sha256 requires value")
                    })?);
            }
            "--no-submit" => args.no_submit = true,
            "--execute-approved" => args.execute_approved = true,
            "--print-secret" => args.print_secret = true,
            "--print-raw-signature" => args.print_raw_signature = true,
            "--use-shared-ingress" => args.use_shared_ingress = true,
            "--use-c-artifacts" => args.use_c_artifacts = true,
            "--allow-online-tuning" => args.allow_online_tuning = true,
            "--allow-candidate-import" => args.allow_candidate_import = true,
            other => return Err(anyhow!("unknown arg {other}")),
        }
    }
    Ok(args)
}

fn load_prepared_order(
    args: &Args,
    failures: &mut Vec<&'static str>,
) -> Option<(PreparedOrder, String)> {
    let Some(path) = args.prepared_order_json.as_ref() else {
        failures.push("PREPARED_ORDER_JSON_REQUIRED");
        return None;
    };
    let actual_sha = match sha256_file(path) {
        Ok(value) => value,
        Err(_) => {
            failures.push("PREPARED_ORDER_JSON_UNREADABLE");
            return None;
        }
    };
    if args
        .expected_prepared_order_sha256
        .as_deref()
        .is_some_and(|expected| expected != actual_sha)
    {
        failures.push("PREPARED_ORDER_SHA256_MISMATCH");
    }
    let parsed = match fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<PreparedOrder>(&raw).ok())
    {
        Some(value) => value,
        None => {
            failures.push("PREPARED_ORDER_JSON_INVALID");
            return None;
        }
    };
    Some((parsed, actual_sha))
}

fn load_one_run_plan(
    args: &Args,
    failures: &mut Vec<&'static str>,
) -> Option<(OneRunPlan, String)> {
    let Some(path) = args.one_run_plan_json.as_ref() else {
        failures.push("ONE_RUN_PLAN_JSON_REQUIRED");
        return None;
    };
    let actual_sha = match sha256_file(path) {
        Ok(value) => value,
        Err(_) => {
            failures.push("ONE_RUN_PLAN_JSON_UNREADABLE");
            return None;
        }
    };
    if args
        .expected_one_run_plan_sha256
        .as_deref()
        .is_some_and(|expected| expected != actual_sha)
    {
        failures.push("ONE_RUN_PLAN_SHA256_MISMATCH");
    }
    let parsed = match fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<OneRunPlan>(&raw).ok())
    {
        Some(value) => value,
        None => {
            failures.push("ONE_RUN_PLAN_JSON_INVALID");
            return None;
        }
    };
    Some((parsed, actual_sha))
}

fn load_scout_snapshot(
    args: &Args,
    failures: &mut Vec<&'static str>,
) -> Option<(S8aScoutAdmissionSnapshot, String)> {
    let Some(path) = args.scout_snapshot_json.as_ref() else {
        failures.push("SCOUT_SNAPSHOT_JSON_REQUIRED");
        return None;
    };
    let actual_sha = match sha256_file(path) {
        Ok(value) => value,
        Err(_) => {
            failures.push("SCOUT_SNAPSHOT_JSON_UNREADABLE");
            return None;
        }
    };
    if args
        .expected_scout_snapshot_sha256
        .as_deref()
        .is_some_and(|expected| expected != actual_sha)
    {
        failures.push("SCOUT_SNAPSHOT_SHA256_MISMATCH");
    }
    let parsed = match fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<ScoutAdmissionSnapshotInput>(&raw).ok())
    {
        Some(value) => value,
        None => {
            failures.push("SCOUT_SNAPSHOT_JSON_INVALID");
            return None;
        }
    };

    if !looks_like_condition_id(&parsed.condition_id) {
        failures.push("SCOUT_SNAPSHOT_CONDITION_ID_NOT_HASH_SHAPED");
    }
    if !is_decimal_token_id(&parsed.yes.token_id) || !is_decimal_token_id(&parsed.no.token_id) {
        failures.push("SCOUT_SNAPSHOT_TOKEN_ID_NOT_DECIMAL");
    }

    let latest_public_buy_trade = match parsed.latest_public_buy_trade {
        Some(trade) => Some(S8aScoutPublicBuyTrade {
            side: match side_from_str(&trade.side) {
                Ok(side) => side,
                Err(_) => {
                    failures.push("SCOUT_SNAPSHOT_TRADE_SIDE_INVALID");
                    Side::Yes
                }
            },
            taker_side: trade.taker_side,
            price: trade.price,
            size: trade.size,
            event_ts_ms: trade.event_ts_ms,
            observed_ts_ms: trade.observed_ts_ms,
            recv_lag_ms: trade.recv_lag_ms,
        }),
        None => None,
    };

    Some((
        S8aScoutAdmissionSnapshot {
            asset: parsed.asset,
            event_reason: parsed.event_reason,
            slug: parsed.slug,
            condition_id: parsed.condition_id,
            yes: S8aScoutBookSideSnapshot {
                token_id: parsed.yes.token_id,
                best_ask: parsed.yes.best_ask,
                top5_ask_qty: parsed.yes.top5_ask_qty,
                book_age_ms: parsed.yes.book_age_ms,
            },
            no: S8aScoutBookSideSnapshot {
                token_id: parsed.no.token_id,
                best_ask: parsed.no.best_ask,
                top5_ask_qty: parsed.no.top5_ask_qty,
                book_age_ms: parsed.no.book_age_ms,
            },
            latest_public_buy_trade,
            offset_s: parsed.offset_s,
            time_to_end_s: parsed.time_to_end_s,
            forbidden_decision_fields_present: parsed.forbidden_decision_fields_present,
            snapshot_index_used_as_rank: parsed.snapshot_index_used_as_rank,
            source_is_b_owned_direct_public_ws: parsed.source_is_b_owned_direct_public_ws,
            shared_ingress_dependency: parsed.shared_ingress_dependency,
            b_owned_direct_public_ws_connection_count: parsed
                .b_owned_direct_public_ws_connection_count,
        },
        actual_sha,
    ))
}

fn load_order_status_fill_evidence(
    args: &Args,
    failures: &mut Vec<&'static str>,
) -> Option<(S8aOrderStatusFillEvidence, String)> {
    let Some(path) = args.order_status_fill_evidence_json.as_ref() else {
        failures.push("ORDER_STATUS_FILL_EVIDENCE_JSON_REQUIRED");
        return None;
    };
    let actual_sha = match sha256_file(path) {
        Ok(value) => value,
        Err(_) => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_JSON_UNREADABLE");
            return None;
        }
    };
    if args
        .expected_order_status_fill_evidence_sha256
        .as_deref()
        .is_some_and(|expected| expected != actual_sha)
    {
        failures.push("ORDER_STATUS_FILL_EVIDENCE_SHA256_MISMATCH");
    }
    let parsed = match fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<OrderStatusFillEvidenceInput>(&raw).ok())
    {
        Some(value) => value,
        None => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_JSON_INVALID");
            return None;
        }
    };

    let side = match side_from_str(&parsed.side) {
        Ok(side) => side,
        Err(_) => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_SIDE_INVALID");
            Side::Yes
        }
    };
    let action = match adapter_action_from_str(&parsed.action) {
        Ok(action) => action,
        Err(_) => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_ACTION_INVALID");
            S8aAdapterOrderAction::Buy
        }
    };
    let status_source = match order_status_source_from_str(&parsed.status_source) {
        Ok(source) => source,
        Err(_) => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_SOURCE_INVALID");
            S8aOrderStatusEvidenceSource::OrderStatusApi
        }
    };
    let order_status = match observed_order_status_from_str(&parsed.order_status) {
        Ok(status) => status,
        Err(_) => {
            failures.push("ORDER_STATUS_FILL_EVIDENCE_STATUS_INVALID");
            S8aObservedOrderStatus::Failed
        }
    };

    Some((
        S8aOrderStatusFillEvidence {
            prepared_order_sha256: parsed.prepared_order_sha256,
            exact_approval_sha256: parsed.exact_approval_sha256,
            expected_exact_approval_sha256: parsed.expected_exact_approval_sha256,
            exact_approval_hash_bound_to_order: parsed.exact_approval_hash_bound_to_order,
            prepared_order_hash_bound_to_order: parsed.prepared_order_hash_bound_to_order,
            approval_scope_matches_s8a: parsed.approval_scope_matches_s8a,
            order_id: parsed.order_id,
            failure_recorded: parsed.failure_recorded,
            status_source,
            order_status,
            condition_id: parsed.condition_id,
            token_id: parsed.token_id,
            side,
            action,
            condition_token_side_action_match_prepared_order: parsed
                .condition_token_side_action_match_prepared_order,
            submitted_size_shares: parsed.submitted_size_shares,
            actual_filled_qty_shares: parsed.actual_filled_qty_shares,
            avg_fill_price: parsed.avg_fill_price,
            open_order_remainder_qty_shares: parsed.open_order_remainder_qty_shares,
            filled_qty_from_exchange_or_order_status: parsed
                .filled_qty_from_exchange_or_order_status,
            submitted_size_counts_as_inventory: parsed.submitted_size_counts_as_inventory,
            partial_fill_threshold_used: parsed.partial_fill_threshold_used,
            forced_complement_after_fill: parsed.forced_complement_after_fill,
            no_open_order_remainder_required: parsed.no_open_order_remainder_required,
            prints_secret_or_raw_signature: parsed.prints_secret_or_raw_signature,
            uses_shared_ingress_or_shared_ws: parsed.uses_shared_ingress_or_shared_ws,
            uses_c_artifacts: parsed.uses_c_artifacts,
            funding_live_latest_or_deploy_requested: parsed.funding_live_latest_or_deploy_requested,
            effectful_execution_requested_in_review: parsed.effectful_execution_requested_in_review,
        },
        actual_sha,
    ))
}

fn is_decimal_token_id(value: &str) -> bool {
    !value.trim().is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn looks_like_condition_id(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() == 66
        && trimmed.starts_with("0x")
        && trimmed[2..].chars().all(|ch| ch.is_ascii_hexdigit())
}

fn prepared_order_to_payload(order: &S8aPreparedVenueOrder) -> serde_json::Value {
    json!({
        "source": "native_s8a_adapter",
        "condition_id": order.condition_id,
        "token_id": order.token_id,
        "side": order.side.as_str(),
        "action": match order.action {
            S8aAdapterOrderAction::Buy => "BUY",
            S8aAdapterOrderAction::Sell => "SELL",
        },
        "order_type": order.order_type.as_str(),
        "limit_price": order.limit_price,
        "amount": {
            "unit": order.amount.unit(),
            "value": order.amount.value(),
        },
        "execution_permitted": order.execution_permitted,
        "natural_controller_admission": true,
        "forced_complement": false,
        "source_guard_500_passed": true
    })
}

fn validate_common(args: &Args, failures: &mut Vec<&'static str>) {
    if args.reviewed_host != REVIEWED_HOST {
        failures.push("REVIEWED_HOST_MISMATCH");
    }
    if args.rest_url != OFFICIAL_CLOB_REST_URL {
        failures.push("REST_URL_MUST_BE_OFFICIAL_CLOB");
    }
    if !hash64(args.exact_approval_sha256.as_deref()) {
        failures.push("EXACT_APPROVAL_SHA256_NOT_64HEX");
    }
    if args.expected_exact_approval_sha256.as_deref() != args.exact_approval_sha256.as_deref() {
        failures.push("EXACT_APPROVAL_SHA256_MISMATCH");
    }
    if args.approval_scope != SCOPE {
        failures.push("APPROVAL_SCOPE_MISMATCH");
    }
    if args.order_primitive_name.as_deref() != Some(ORDER_PRIMITIVE_NAME) {
        failures.push("ORDER_PRIMITIVE_NAME_MISMATCH");
    }
    if !hash64(args.order_primitive_source_sha256.as_deref()) {
        failures.push("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX");
    }
    if args.print_secret || args.print_raw_signature {
        failures.push("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED");
    }
    if args.use_shared_ingress || args.use_c_artifacts {
        failures.push("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED");
    }
    if args.allow_online_tuning || args.allow_candidate_import {
        failures.push("FORBIDDEN_ONLINE_TUNING_OR_CANDIDATE_IMPORT");
    }
}

fn validate_one_run_plan(plan: &OneRunPlan, failures: &mut Vec<&'static str>) -> serde_json::Value {
    if plan.source != "native_s8a_one_run_driver" {
        failures.push("ONE_RUN_PLAN_SOURCE_MISMATCH");
    }
    if plan.effectful_execution_permitted {
        failures.push("ONE_RUN_PLAN_MUST_BE_REVIEW_ONLY");
    }
    if !plan.runtime_accepts_only_prepared_orders {
        failures.push("ONE_RUN_DRIVER_MUST_ACCEPT_ONLY_PREPARED_ORDERS");
    }
    if !plan.exact_approval_hash_bound_to_every_submit {
        failures.push("EXACT_APPROVAL_HASH_NOT_BOUND_TO_EVERY_SUBMIT");
    }
    if !plan.exact_approval_scope_bound_to_every_submit {
        failures.push("EXACT_APPROVAL_SCOPE_NOT_BOUND_TO_EVERY_SUBMIT");
    }
    if !plan.runtime_order_result_ledger_enabled {
        failures.push("RUNTIME_ORDER_RESULT_LEDGER_MISSING");
    }
    if !plan.actual_filled_qty_ledger_updates_inventory {
        failures.push("ACTUAL_FILLED_QTY_LEDGER_MISSING");
    }
    if plan.submitted_size_counts_as_inventory {
        failures.push("SUBMITTED_SIZE_COUNTS_AS_INVENTORY");
    }
    if plan.partial_fill_threshold_enabled {
        failures.push("PARTIAL_FILL_THRESHOLD_ENABLED");
    }
    if plan.forced_complement_allowed {
        failures.push("FORCED_COMPLEMENT_ALLOWED");
    }
    if !plan.s7w_reconciliation_bound_to_run_result {
        failures.push("S7W_RECONCILIATION_NOT_BOUND_TO_RUN_RESULT");
    }
    if plan.review_only_fixture_receipts_accepted {
        failures.push("REVIEW_ONLY_FIXTURE_RECEIPTS_ACCEPTED");
    }
    if plan.opens_ws {
        failures.push("RUNTIME_DRIVER_MUST_NOT_OPEN_WS");
    }
    if plan.shared_ingress_dependency || plan.uses_c_artifacts {
        failures.push("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED");
    }
    if plan.secret_or_raw_signature_output_allowed {
        failures.push("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED");
    }
    if plan.funding_live_latest_or_deploy_requested {
        failures.push("FUNDING_LIVE_LATEST_OR_DEPLOY_REQUESTED");
    }
    if plan.strategy_scope.market_family != "btc-5min" {
        failures.push("ONE_RUN_MARKET_FAMILY_MUST_BE_BTC_5MIN");
    }
    if plan.strategy_scope.fixed_policy != "cool5_imb1.25_source_guard_500" {
        failures.push("ONE_RUN_FIXED_POLICY_MISMATCH");
    }
    if !plan.strategy_scope.source_guard_500_required {
        failures.push("SOURCE_GUARD_500_REQUIRED");
    }
    if plan.strategy_scope.online_tuning_allowed
        || plan.strategy_scope.strategy_discovery_allowed
        || plan.strategy_scope.candidate_import_allowed
    {
        failures.push("ONLINE_TUNING_STRATEGY_DISCOVERY_OR_CANDIDATE_IMPORT_ALLOWED");
    }
    if plan.caps.max_round_count != S8A_MAX_ROUNDS {
        failures.push("MAX_ROUND_COUNT_MISMATCH");
    }
    if (plan.caps.session_hard_loss_cap_usdc - S8A_SESSION_HARD_LOSS_CAP_USDC).abs() > 1e-9 {
        failures.push("SESSION_HARD_LOSS_CAP_MISMATCH");
    }
    if (plan.caps.per_round_theoretical_max_loss_usdc - S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC)
        .abs()
        > 1e-9
    {
        failures.push("PER_ROUND_THEORETICAL_MAX_LOSS_MISMATCH");
    }
    if plan.caps.max_active_market_count != 1 {
        failures.push("MAX_ACTIVE_MARKET_COUNT_MISMATCH");
    }
    if plan.caps.max_total_order_submissions != 9 {
        failures.push("MAX_TOTAL_ORDER_SUBMISSIONS_MISMATCH");
    }
    if plan.caps.max_cumulative_gross_quote_spend_usdc > 18.0 + 1e-9 {
        failures.push("MAX_CUMULATIVE_GROSS_QUOTE_SPEND_TOO_HIGH");
    }
    if plan.attempts.is_empty() {
        failures.push("ONE_RUN_ATTEMPTS_EMPTY");
    }
    if plan.attempts.len() > plan.caps.max_total_order_submissions as usize {
        failures.push("ONE_RUN_ATTEMPTS_EXCEED_ORDER_CAP");
    }

    let mut inventory = S8aInventory::default();
    let mut active_condition: Option<String> = None;
    let mut completed_rounds = 0_u32;
    let mut gross_quote_spend_usdc = 0.0_f64;
    let mut realized_session_loss_usdc = 0.0_f64;
    let mut closed_round_count = 0_u32;

    for attempt in &plan.attempts {
        validate_prepared_order(&attempt.prepared_order, failures);
        if attempt.prepared_order.execution_permitted {
            failures.push("PREPARED_ORDER_EXECUTION_MUST_BE_FALSE_BEFORE_RUNTIME");
        }
        if attempt.actual_filled_qty_shares < 0.0 || !attempt.actual_filled_qty_shares.is_finite() {
            failures.push("ONE_RUN_INVALID_ACTUAL_FILLED_QTY");
        }
        if !attempt.avg_fill_price.is_finite()
            || attempt.avg_fill_price < 0.0
            || attempt.avg_fill_price > 1.0
        {
            failures.push("ONE_RUN_INVALID_AVG_FILL_PRICE");
        }
        if !attempt.order_id_or_failure_recorded {
            failures.push("ORDER_ID_OR_FAILURE_NOT_RECORDED_PER_ATTEMPT");
        }
        if !attempt.filled_qty_from_exchange_or_order_status {
            failures.push("FILLED_QTY_NOT_EXCHANGE_OR_ORDER_STATUS_OBSERVED");
        }
        if active_condition
            .as_ref()
            .is_some_and(|condition| condition != &attempt.prepared_order.condition_id)
        {
            failures.push("ACTIVE_MARKET_CONDITION_MISMATCH");
        }
        if active_condition.is_none() {
            active_condition = Some(attempt.prepared_order.condition_id.clone());
        }
        let fill = S8aFillEvent {
            side: match attempt.prepared_order.side.as_str() {
                "YES" => Side::Yes,
                "NO" => Side::No,
                _ => Side::Yes,
            },
            action: match attempt.prepared_order.action.as_str() {
                "BUY" => S8aAdapterOrderAction::Buy,
                "SELL" => S8aAdapterOrderAction::Sell,
                _ => S8aAdapterOrderAction::Buy,
            },
            submitted_size_shares: match attempt.prepared_order.amount.unit.as_str() {
                "SHARES" => attempt.prepared_order.amount.value,
                _ => 0.0,
            },
            actual_filled_qty_shares: attempt.actual_filled_qty_shares,
            avg_fill_price: attempt.avg_fill_price,
        };
        match apply_s8a_fill_to_inventory(inventory, fill) {
            Ok(next) => {
                inventory = next;
                if attempt.prepared_order.action == "BUY" {
                    gross_quote_spend_usdc +=
                        attempt.actual_filled_qty_shares * attempt.avg_fill_price;
                }
            }
            Err(_) => failures.push("ONE_RUN_INVENTORY_UPDATE_FAILED"),
        }
        if inventory.residual_qty() > S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC + 1e-9 {
            failures.push("ACTIVE_ROUND_RISK_EXCEEDS_PER_ROUND_CAP");
        }
        if gross_quote_spend_usdc > plan.caps.max_cumulative_gross_quote_spend_usdc + 1e-9 {
            failures.push("CUMULATIVE_GROSS_QUOTE_SPEND_CAP_EXCEEDED");
        }
        if !attempt.realized_loss_delta_usdc.is_finite() || attempt.realized_loss_delta_usdc < 0.0 {
            failures.push("INVALID_REALIZED_LOSS_DELTA");
        }
        if attempt.close_round_after_attempt {
            if !attempt.s7w_reconciliation_passed {
                failures.push("ROUND_CLOSE_WITHOUT_S7W_RECONCILIATION");
            }
            if !attempt.exact_approved_receipt_tier_required {
                failures.push("EXACT_APPROVED_RECEIPT_TIER_NOT_REQUIRED");
            }
            if !attempt.positive_collateral_delta_required_for_recovery {
                failures.push("POSITIVE_COLLATERAL_DELTA_NOT_REQUIRED_FOR_RECOVERY");
            }
            if !attempt.no_open_order_remainder {
                failures.push("OPEN_ORDER_REMAINDER_NOT_REQUIRED");
            }
            if !attempt.residual_exposure_zero || inventory.residual_qty() > 1e-9 {
                failures.push("ROUND_CLOSE_WITH_RESIDUAL_EXPOSURE");
            }
            completed_rounds += 1;
            if completed_rounds > S8A_MAX_ROUNDS {
                failures.push("MAX_ROUNDS_EXCEEDED");
            }
            realized_session_loss_usdc += attempt.realized_loss_delta_usdc;
            if realized_session_loss_usdc > S8A_SESSION_HARD_LOSS_CAP_USDC + 1e-9 {
                failures.push("SESSION_HARD_LOSS_CAP_EXCEEDED");
            }
            inventory = S8aInventory::default();
            active_condition = None;
            closed_round_count += 1;
        }
    }
    if closed_round_count == 0 {
        failures.push("NO_ROUND_CLOSURE_PATH");
    }
    json!({
        "attempt_count": plan.attempts.len(),
        "closed_round_count": closed_round_count,
        "gross_quote_spend_usdc": gross_quote_spend_usdc,
        "realized_session_loss_usdc": realized_session_loss_usdc,
        "final_yes_qty": inventory.yes_qty,
        "final_no_qty": inventory.no_qty,
        "final_residual_qty": inventory.residual_qty(),
        "active_condition_id": active_condition
    })
}

fn validate_prepared_order(order: &PreparedOrder, failures: &mut Vec<&'static str>) {
    if order.source != "native_s8a_adapter" {
        failures.push("PREPARED_ORDER_SOURCE_NOT_NATIVE_S8A_ADAPTER");
    }
    if order.condition_id.trim().is_empty() {
        failures.push("PREPARED_ORDER_MISSING_CONDITION_ID");
    }
    if order.token_id.trim().is_empty() {
        failures.push("PREPARED_ORDER_MISSING_TOKEN_ID");
    }
    if !is_decimal_token_id(&order.token_id) {
        failures.push("PREPARED_ORDER_TOKEN_ID_NOT_DECIMAL");
    }
    if !looks_like_condition_id(&order.condition_id) {
        failures.push("PREPARED_ORDER_CONDITION_ID_NOT_HASH_SHAPED");
    }
    if !matches!(order.side.as_str(), "YES" | "NO") {
        failures.push("PREPARED_ORDER_SIDE_NOT_YES_OR_NO");
    }
    if !matches!(order.action.as_str(), "BUY" | "SELL") {
        failures.push("PREPARED_ORDER_ACTION_NOT_BUY_OR_SELL");
    }
    if !matches!(order.order_type.as_str(), "GTC" | "FAK") {
        failures.push("PREPARED_ORDER_TYPE_NOT_GTC_OR_FAK");
    }
    if order.execution_permitted {
        failures.push("PREPARED_ORDER_EXECUTION_MUST_BE_FALSE_BEFORE_RUNTIME");
    }
    if !order.natural_controller_admission {
        failures.push("PREPARED_ORDER_MISSING_NATURAL_CONTROLLER_ADMISSION");
    }
    if order.forced_complement {
        failures.push("PREPARED_ORDER_FORCED_COMPLEMENT_NOT_FALSE");
    }
    if !order.source_guard_500_passed {
        failures.push("PREPARED_ORDER_SOURCE_GUARD_500_NOT_PASSED");
    }
    match (order.order_type.as_str(), order.action.as_str()) {
        ("GTC", "BUY") => {
            if order.amount.unit != "SHARES" {
                failures.push("S8A_LIMIT_ENTRY_AMOUNT_UNIT_MUST_BE_SHARES");
            }
            if (order.amount.value - S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES).abs() > 1e-9 {
                failures.push("S8A_LIMIT_ENTRY_SIZE_MUST_BE_5_SHARES");
            }
            let Some(px) = order.limit_price else {
                failures.push("S8A_LIMIT_ENTRY_PRICE_MISSING");
                return;
            };
            if !(S8A_SEED_PX_LO..=S8A_SEED_PX_HI).contains(&px) {
                failures.push("S8A_LIMIT_ENTRY_PRICE_OUTSIDE_SEED_BAND");
            }
        }
        ("FAK", "BUY") => {
            if order.amount.unit != "USDC_NOTIONAL" {
                failures.push("S8A_MARKET_BUY_AMOUNT_UNIT_MUST_BE_USDC_NOTIONAL");
            }
            if order.amount.value + 1e-9 < S8A_MARKET_BUY_MIN_NOTIONAL_USDC {
                failures.push("S8A_MARKET_BUY_NOTIONAL_BELOW_1_USDC");
            }
        }
        ("FAK", "SELL") => {
            if order.amount.unit != "SHARES" {
                failures.push("S8A_MARKET_SELL_AMOUNT_UNIT_MUST_BE_SHARES");
            }
            if order.amount.value + 1e-9 < S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES {
                failures.push("S8A_MARKET_SELL_SIZE_BELOW_5_SHARES");
            }
        }
        ("GTC", "SELL") => failures.push("S8A_LIMIT_SELL_UNSUPPORTED"),
        _ => {}
    }
}

fn base_payload(status: &str) -> serde_json::Value {
    json!({
        "schema_version": "B_STRATEGY_CANARY_S8A_NATIVE_EFFECTFUL_RUNTIME_v1",
        "status": status,
        "generated_at_ms": now_ms(),
        "scope": SCOPE,
        "effectful_execution_permitted_without_fresh_approval": false,
        "reviewed_host": REVIEWED_HOST,
        "rest_url": OFFICIAL_CLOB_REST_URL,
        "order_primitive_name": ORDER_PRIMITIVE_NAME,
        "strategy_scope": {
            "market_family": "btc-5min",
            "fixed_policy": "cool5_imb1.25_source_guard_500",
            "source_guard_500_required": true,
            "online_tuning_allowed": false,
            "candidate_import_allowed": false
        },
        "runtime_caps": {
            "max_round_count": 3,
            "session_hard_loss_cap_usdc": 15.0,
            "max_active_market_count": 1,
            "entry_size_shares": 5.0
        },
        "forbidden": {
            "secret_print_copy_hash": true,
            "raw_signature_output": true,
            "shared_ingress_or_shared_ws": true,
            "c_artifacts": true,
            "funding_live_latest_deploy": true,
            "candidate_import": true
        }
    })
}

fn print_payload(mut payload: serde_json::Value, additions: serde_json::Value) {
    let map = payload.as_object_mut().expect("base payload object");
    if let Some(extra) = additions.as_object() {
        for (k, v) in extra {
            map.insert(k.clone(), v.clone());
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&payload).expect("json output")
    );
}

fn read_optional_api_credentials() -> Option<Credentials> {
    let key = env::var("POLYMARKET_API_KEY").ok();
    let secret = env::var("POLYMARKET_API_SECRET").ok();
    let passphrase = env::var("POLYMARKET_API_PASSPHRASE").ok();
    match (key, secret, passphrase) {
        (Some(key_raw), Some(secret), Some(passphrase))
            if !key_raw.trim().is_empty()
                && !secret.trim().is_empty()
                && !passphrase.trim().is_empty() =>
        {
            let key = Uuid::from_str(&key_raw).ok()?;
            Some(Credentials::new(key, secret, passphrase))
        }
        _ => None,
    }
}

fn parse_env_address(var: &str) -> Result<Option<Address>> {
    let raw = match env::var(var) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };
    let addr = Address::from_str(&raw)
        .with_context(|| format!("{var} is present but not a valid address"))?;
    Ok(Some(addr))
}

fn side_from_str(side: &str) -> Result<Side> {
    match side {
        "YES" => Ok(Side::Yes),
        "NO" => Ok(Side::No),
        _ => Err(anyhow!("invalid side")),
    }
}

fn direction_from_str(action: &str) -> Result<TradeDirection> {
    match action {
        "BUY" => Ok(TradeDirection::Buy),
        "SELL" => Ok(TradeDirection::Sell),
        _ => Err(anyhow!("invalid action")),
    }
}

fn adapter_action_from_str(action: &str) -> Result<S8aAdapterOrderAction> {
    match action {
        "BUY" => Ok(S8aAdapterOrderAction::Buy),
        "SELL" => Ok(S8aAdapterOrderAction::Sell),
        _ => Err(anyhow!("invalid adapter action")),
    }
}

fn order_type_from_str(order_type: &str) -> Result<OrderType> {
    match order_type {
        "GTC" => Ok(OrderType::GTC),
        "FAK" => Ok(OrderType::FAK),
        _ => Err(anyhow!("unsupported order type")),
    }
}

fn s8a_venue_order_type_from_str(order_type: &str) -> Result<S8aVenueOrderType> {
    match order_type {
        "GTC" => Ok(S8aVenueOrderType::Gtc),
        "FAK" => Ok(S8aVenueOrderType::Fak),
        _ => Err(anyhow!("unsupported S8A venue order type")),
    }
}

fn order_status_source_from_str(source: &str) -> Result<S8aOrderStatusEvidenceSource> {
    match source {
        "POST_ORDER_RESPONSE" => Ok(S8aOrderStatusEvidenceSource::PostOrderResponse),
        "ORDER_STATUS_API" => Ok(S8aOrderStatusEvidenceSource::OrderStatusApi),
        "TRADE_FILL_API" => Ok(S8aOrderStatusEvidenceSource::TradeFillApi),
        _ => Err(anyhow!("unsupported order status evidence source")),
    }
}

fn observed_order_status_from_str(status: &str) -> Result<S8aObservedOrderStatus> {
    match status {
        "LIVE" => Ok(S8aObservedOrderStatus::Live),
        "MATCHED" => Ok(S8aObservedOrderStatus::Matched),
        "FILLED" => Ok(S8aObservedOrderStatus::Filled),
        "CANCELLED" => Ok(S8aObservedOrderStatus::Cancelled),
        "REJECTED" => Ok(S8aObservedOrderStatus::Rejected),
        "FAILED" => Ok(S8aObservedOrderStatus::Failed),
        _ => Err(anyhow!("unsupported observed order status")),
    }
}

fn s8a_venue_amount_from_prepared(order: &PreparedOrder) -> Result<S8aVenueAmount> {
    match order.amount.unit.as_str() {
        "SHARES" => Ok(S8aVenueAmount::Shares(order.amount.value)),
        "USDC_NOTIONAL" => Ok(S8aVenueAmount::UsdcNotional(order.amount.value)),
        _ => Err(anyhow!("unsupported prepared order amount unit")),
    }
}

fn s8a_prepared_order_from_payload(order: &PreparedOrder) -> Result<S8aPreparedVenueOrder> {
    Ok(S8aPreparedVenueOrder {
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        side: side_from_str(&order.side)?,
        action: adapter_action_from_str(&order.action)?,
        order_type: s8a_venue_order_type_from_str(&order.order_type)?,
        limit_price: order.limit_price,
        amount: s8a_venue_amount_from_prepared(order)?,
        execution_permitted: order.execution_permitted,
    })
}

fn fill_event_payload(fill: &S8aFillEvent) -> serde_json::Value {
    json!({
        "side": fill.side.as_str(),
        "action": match fill.action {
            S8aAdapterOrderAction::Buy => "BUY",
            S8aAdapterOrderAction::Sell => "SELL",
        },
        "submitted_size_shares": fill.submitted_size_shares,
        "actual_filled_qty_shares": fill.actual_filled_qty_shares,
        "avg_fill_price": fill.avg_fill_price
    })
}

fn sdk_status_to_observed(status: &OrderStatusType) -> S8aObservedOrderStatus {
    match status {
        OrderStatusType::Live => S8aObservedOrderStatus::Live,
        OrderStatusType::Matched => S8aObservedOrderStatus::Matched,
        OrderStatusType::Canceled => S8aObservedOrderStatus::Cancelled,
        OrderStatusType::Delayed | OrderStatusType::Unmatched => S8aObservedOrderStatus::Live,
        OrderStatusType::Unknown(_) => S8aObservedOrderStatus::Failed,
        _ => S8aObservedOrderStatus::Failed,
    }
}

fn decimal_to_f64_lossy(value: rust_decimal::Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}

fn submitted_size_from_prepared(order: &PreparedOrder) -> f64 {
    match order.amount.unit.as_str() {
        "SHARES" => order.amount.value,
        _ => 0.0,
    }
}

fn filled_qty_from_post_response(order: &PreparedOrder, response: &PostOrderResponse) -> f64 {
    match order.action.as_str() {
        "BUY" => decimal_to_f64_lossy(response.taking_amount),
        "SELL" => decimal_to_f64_lossy(response.making_amount),
        _ => 0.0,
    }
}

fn filled_qty_from_order_status(status: &OpenOrderResponse) -> f64 {
    decimal_to_f64_lossy(status.size_matched)
}

fn open_remainder_from_order_status(status: &OpenOrderResponse) -> f64 {
    (decimal_to_f64_lossy(status.original_size) - decimal_to_f64_lossy(status.size_matched))
        .max(0.0)
}

fn trade_belongs_to_order(trade: &TradeResponse, order_id: &str) -> bool {
    trade.taker_order_id == order_id
        || trade
            .maker_orders
            .iter()
            .any(|maker| maker.order_id == order_id)
}

fn trade_fill_summary(trades: &[TradeResponse], order_id: &str) -> Option<(f64, f64)> {
    let mut qty = 0.0_f64;
    let mut notional = 0.0_f64;
    for trade in trades
        .iter()
        .filter(|trade| trade_belongs_to_order(trade, order_id))
    {
        let size = decimal_to_f64_lossy(trade.size);
        let price = decimal_to_f64_lossy(trade.price);
        if size > 0.0 && price.is_finite() {
            qty += size;
            notional += size * price;
        }
    }
    (qty > 0.0).then_some((qty, notional / qty))
}

fn status_fill_evidence_payload(evidence: &S8aOrderStatusFillEvidence) -> serde_json::Value {
    json!({
        "prepared_order_sha256": evidence.prepared_order_sha256,
        "exact_approval_sha256": evidence.exact_approval_sha256,
        "expected_exact_approval_sha256": evidence.expected_exact_approval_sha256,
        "exact_approval_hash_bound_to_order": evidence.exact_approval_hash_bound_to_order,
        "prepared_order_hash_bound_to_order": evidence.prepared_order_hash_bound_to_order,
        "approval_scope_matches_s8a": evidence.approval_scope_matches_s8a,
        "order_id": evidence.order_id,
        "failure_recorded": evidence.failure_recorded,
        "status_source": evidence.status_source.as_str(),
        "order_status": evidence.order_status.as_str(),
        "condition_id": evidence.condition_id,
        "token_id": evidence.token_id,
        "side": evidence.side.as_str(),
        "action": match evidence.action {
            S8aAdapterOrderAction::Buy => "BUY",
            S8aAdapterOrderAction::Sell => "SELL",
        },
        "condition_token_side_action_match_prepared_order": evidence.condition_token_side_action_match_prepared_order,
        "submitted_size_shares": evidence.submitted_size_shares,
        "actual_filled_qty_shares": evidence.actual_filled_qty_shares,
        "avg_fill_price": evidence.avg_fill_price,
        "open_order_remainder_qty_shares": evidence.open_order_remainder_qty_shares,
        "filled_qty_from_exchange_or_order_status": evidence.filled_qty_from_exchange_or_order_status,
        "submitted_size_counts_as_inventory": evidence.submitted_size_counts_as_inventory,
        "partial_fill_threshold_used": evidence.partial_fill_threshold_used,
        "forced_complement_after_fill": evidence.forced_complement_after_fill,
        "no_open_order_remainder_required": evidence.no_open_order_remainder_required,
        "prints_secret_or_raw_signature": evidence.prints_secret_or_raw_signature,
        "uses_shared_ingress_or_shared_ws": evidence.uses_shared_ingress_or_shared_ws,
        "uses_c_artifacts": evidence.uses_c_artifacts,
        "funding_live_latest_or_deploy_requested": evidence.funding_live_latest_or_deploy_requested,
        "effectful_execution_requested_in_review": evidence.effectful_execution_requested_in_review
    })
}

async fn execute(
    args: &Args,
    order: &PreparedOrder,
    prepared_order_sha256: &str,
) -> Result<serde_json::Value> {
    if !args.execute_approved {
        return Err(anyhow!("--execute-approved is required for execute mode"));
    }
    if args.no_submit {
        return Err(anyhow!("execute mode cannot run with --no-submit"));
    }
    if args.rest_url != OFFICIAL_CLOB_REST_URL {
        return Err(anyhow!("execute mode requires official CLOB REST URL"));
    }
    if order.order_type != "GTC" || order.action != "BUY" {
        return Err(anyhow!("first S8A runtime only supports GTC BUY entry"));
    }
    let private_key = env::var("POLYMARKET_PRIVATE_KEY").ok();
    let funder = parse_env_address("POLYMARKET_FUNDER_ADDRESS")?;
    let api_creds = read_optional_api_credentials();
    let (client, signer) =
        init_clob_client(&args.rest_url, private_key.as_deref(), funder, api_creds).await;
    let client = client.ok_or_else(|| anyhow!("authenticated CLOB client unavailable"))?;
    let signer = signer.ok_or_else(|| anyhow!("local signer unavailable"))?;
    let funder_addr = funder.unwrap_or_else(|| signer.address());
    let sig_type = infer_signature_type(
        signer.address(),
        Some(funder_addr),
        env::var("PM_SIGNATURE_TYPE")
            .ok()
            .and_then(|v| v.parse::<u8>().ok()),
    );
    let token_id = U256::from_str_radix(order.token_id.trim(), 10).context("invalid token id")?;
    let ctx = V2OrderContext {
        exchange: v2_contract_config(false).exchange,
        maker: funder_addr,
        token_id,
        direction: direction_from_str(&order.action)?,
        signature_type: sig_type,
        expiration: 0,
        metadata: B256::ZERO,
        builder: builder_code_from_env(),
    };
    let signed = build_signed_limit_order_v2(
        &signer,
        137,
        ctx,
        OrderSizingV2 {
            price: order
                .limit_price
                .ok_or_else(|| anyhow!("missing limit price"))?,
            size_shares: order.amount.value,
        },
    )
    .await?;
    let response = post_order_v2(
        &client,
        &signed,
        order_type_from_str(&order.order_type)?,
        true,
        false,
    )
    .await?;
    if !response.success {
        return Err(anyhow!(
            "post_order rejected status={:?} error={:?}",
            response.status,
            response.error_msg
        ));
    }
    if !matches!(
        response.status,
        OrderStatusType::Live | OrderStatusType::Matched
    ) {
        return Err(anyhow!(
            "post_order unexpected status={:?} error={:?}",
            response.status,
            response.error_msg
        ));
    }

    let market = B256::from_str(order.condition_id.trim()).context("invalid condition id")?;
    let initial_order_status = fetch_order_status_v2(&client, &response.order_id)
        .await
        .ok();
    let mut final_order_status = initial_order_status.clone();
    let mut open_remainder_qty = initial_order_status
        .as_ref()
        .map(open_remainder_from_order_status)
        .unwrap_or(0.0);
    let mut observed_status = initial_order_status
        .as_ref()
        .map(|status| sdk_status_to_observed(&status.status))
        .unwrap_or_else(|| sdk_status_to_observed(&response.status));

    let mut cancel_submitted = false;
    let mut cancel_payload = json!(null);
    if open_remainder_qty > 1e-9 && matches!(observed_status, S8aObservedOrderStatus::Live) {
        cancel_submitted = true;
        match cancel_order_v2(&client, &response.order_id).await {
            Ok(cancel_response) => {
                let canceled = cancel_response
                    .canceled
                    .iter()
                    .any(|order_id| order_id == &response.order_id);
                if canceled {
                    open_remainder_qty = 0.0;
                    observed_status = S8aObservedOrderStatus::Cancelled;
                }
                cancel_payload = json!({
                    "attempted": true,
                    "canceled": cancel_response.canceled,
                    "not_canceled": cancel_response.not_canceled,
                    "open_remainder_cleared_by_cancel": canceled
                });
                if let Ok(status_after_cancel) =
                    fetch_order_status_v2(&client, &response.order_id).await
                {
                    open_remainder_qty = open_remainder_from_order_status(&status_after_cancel);
                    observed_status = sdk_status_to_observed(&status_after_cancel.status);
                    final_order_status = Some(status_after_cancel);
                }
            }
            Err(err) => {
                cancel_payload = json!({
                    "attempted": true,
                    "error": err.to_string(),
                    "open_remainder_cleared_by_cancel": false
                });
            }
        }
    }

    let trades_result = fetch_trades_for_market_asset_v2(&client, market, token_id).await;
    let (trades, trades_error) = match trades_result {
        Ok(trades) => (trades, None),
        Err(err) => (Vec::new(), Some(err.to_string())),
    };
    let trade_summary = trade_fill_summary(&trades, &response.order_id);

    let status_for_fill = final_order_status
        .as_ref()
        .or(initial_order_status.as_ref());
    let status_fill_qty = status_for_fill
        .map(filled_qty_from_order_status)
        .unwrap_or(0.0);
    let post_fill_qty = filled_qty_from_post_response(order, &response);
    let (actual_filled_qty, avg_fill_price, status_source) =
        if let Some((trade_qty, trade_avg_price)) = trade_summary {
            (
                trade_qty,
                Some(trade_avg_price),
                S8aOrderStatusEvidenceSource::TradeFillApi,
            )
        } else if let Some(order_status) = status_for_fill {
            (
                status_fill_qty,
                (status_fill_qty > 0.0).then_some(decimal_to_f64_lossy(order_status.price)),
                S8aOrderStatusEvidenceSource::OrderStatusApi,
            )
        } else {
            (
                post_fill_qty,
                (post_fill_qty > 0.0).then_some(
                    order.limit_price.ok_or_else(|| {
                        anyhow!("missing limit price for post-order fill evidence")
                    })?,
                ),
                S8aOrderStatusEvidenceSource::PostOrderResponse,
            )
        };

    let prepared = s8a_prepared_order_from_payload(order)?;
    let exact_approval_sha256 = args
        .exact_approval_sha256
        .clone()
        .ok_or_else(|| anyhow!("missing exact approval hash"))?;
    let expected_exact_approval_sha256 = args
        .expected_exact_approval_sha256
        .clone()
        .ok_or_else(|| anyhow!("missing expected exact approval hash"))?;
    let fill_evidence = S8aOrderStatusFillEvidence {
        prepared_order_sha256: prepared_order_sha256.to_string(),
        exact_approval_sha256,
        expected_exact_approval_sha256,
        exact_approval_hash_bound_to_order: true,
        prepared_order_hash_bound_to_order: true,
        approval_scope_matches_s8a: args.approval_scope == SCOPE,
        order_id: Some(response.order_id.clone()),
        failure_recorded: false,
        status_source,
        order_status: observed_status,
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        side: side_from_str(&order.side)?,
        action: adapter_action_from_str(&order.action)?,
        condition_token_side_action_match_prepared_order: true,
        submitted_size_shares: submitted_size_from_prepared(order),
        actual_filled_qty_shares: actual_filled_qty,
        avg_fill_price,
        open_order_remainder_qty_shares: open_remainder_qty,
        filled_qty_from_exchange_or_order_status: true,
        submitted_size_counts_as_inventory: false,
        partial_fill_threshold_used: false,
        forced_complement_after_fill: false,
        no_open_order_remainder_required: true,
        prints_secret_or_raw_signature: false,
        uses_shared_ingress_or_shared_ws: false,
        uses_c_artifacts: false,
        funding_live_latest_or_deploy_requested: false,
        effectful_execution_requested_in_review: false,
    };
    let fill_review = review_s8a_order_status_fill_evidence(&prepared, &fill_evidence);
    let post_run_reconciliation_passed = fill_review.block_reasons.is_empty();

    Ok(json!({
        "orders_submitted_effectful": 1,
        "cancel_submissions_effectful": if cancel_submitted { 1 } else { 0 },
        "order_id": response.order_id,
        "post_order_status": format!("{:?}", response.status),
        "observed_order_status": fill_evidence.order_status.as_str(),
        "success": response.success,
        "taking_amount": response.taking_amount,
        "making_amount": response.making_amount,
        "trade_ids": response.trade_ids,
        "side": side_from_str(&order.side)?.as_str(),
        "action": order.action,
        "order_type": order.order_type,
        "submitted_size_shares": order.amount.value,
        "submitted_limit_price": order.limit_price,
        "actual_filled_qty_shares": actual_filled_qty,
        "avg_fill_price": avg_fill_price,
        "open_order_remainder_qty_shares": open_remainder_qty,
        "cancel_result": cancel_payload,
        "trades_checked": trades.len(),
        "trades_query_error": trades_error,
        "order_status_fill_evidence": status_fill_evidence_payload(&fill_evidence),
        "s8s_fill_evidence_review": {
            "event": fill_review.event,
            "block_reasons": fill_review.block_reasons.iter().map(|reason| reason.as_str()).collect::<Vec<_>>(),
            "actual_filled_qty_ledger_ready": fill_review.actual_filled_qty_ledger_ready,
            "filled_qty_from_exchange_or_order_status_only": fill_review.filled_qty_from_exchange_or_order_status_only,
            "no_open_order_remainder": fill_review.no_open_order_remainder,
            "effectful_execution_permitted": fill_review.effectful_execution_permitted,
            "fill_event": fill_review.fill_event.as_ref().map(fill_event_payload)
        },
        "post_run_reconciliation_passed": post_run_reconciliation_passed,
        "raw_signature_output": false
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let mode = args.mode.ok_or_else(|| anyhow!("--mode is required"))?;
    match mode {
        Mode::ReviewOnly => {
            print_payload(
                base_payload("PASS_REVIEW_ONLY_NATIVE_EFFECTFUL_RUNTIME_SOURCE_SHAPE"),
                json!({
                    "orders_submitted": 0,
                    "signing_performed": false,
                    "execute_mode_available_with_fresh_approval": true
                }),
            );
            Ok(())
        }
        Mode::PreviewNoApproval => {
            print_payload(
                base_payload("BLOCK_PREVIEW_WITHOUT_EXACT_APPROVAL_EXIT_66"),
                json!({
                    "orders_submitted": 0,
                    "signing_performed": false
                }),
            );
            std::process::exit(66);
        }
        Mode::NoOrderAuthPreview => {
            let mut failures = Vec::new();
            if args.reviewed_host != REVIEWED_HOST {
                failures.push("REVIEWED_HOST_MISMATCH");
            }
            if args.print_secret || args.print_raw_signature {
                failures.push("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED");
            }
            if args.use_shared_ingress || args.use_c_artifacts {
                failures.push("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED");
            }
            let status = if failures.is_empty() {
                "PASS_NO_ORDER_AUTH_PREVIEW_NO_SECRET_READ"
            } else {
                "BLOCK_NO_ORDER_AUTH_PREVIEW_FAIL_CLOSED"
            };
            print_payload(
                base_payload(status),
                json!({
                    "secret_values_read": false,
                    "secret_values_printed": false,
                    "raw_signature_output": false,
                    "failures": failures
                }),
            );
            if status.starts_with("PASS") {
                Ok(())
            } else {
                std::process::exit(2);
            }
        }
        Mode::LiveScoutToOrderPreview => {
            let mut failures = Vec::new();
            validate_common(&args, &mut failures);
            if !args.no_submit {
                failures.push("NO_SUBMIT_REQUIRED_IN_LIVE_SCOUT_TO_ORDER_PREVIEW");
            }
            let loaded = load_scout_snapshot(&args, &mut failures);
            let mut scout_snapshot_sha256 = None;
            let mut candidate_payload = json!(null);
            let mut controller_payload = json!(null);
            let mut adapter_payload = json!(null);
            let mut prepared_order_payload = json!(null);

            if let Some((snapshot, sha256)) = loaded.as_ref() {
                scout_snapshot_sha256 = Some(sha256.clone());
                let context = S8aControllerAdmissionAdapterContext {
                    yes_token_id: snapshot.yes.token_id.clone(),
                    no_token_id: snapshot.no.token_id.clone(),
                    official_order_minimums_verified: true,
                    fresh_exact_approval_hash_present: hash64(
                        args.exact_approval_sha256.as_deref(),
                    ),
                    approval_scope_matches_s8a: args.approval_scope == SCOPE,
                    effectful_submission_requested: true,
                    prints_secret_or_raw_signature: args.print_secret || args.print_raw_signature,
                    uses_shared_ingress_or_shared_ws: args.use_shared_ingress,
                    uses_c_artifacts: args.use_c_artifacts,
                };
                let review = review_s8a_scout_snapshot_for_limit_entry(
                    &BtcCompletionControllerConfig::s8a_size5_runtime_default(),
                    &BtcCompletionControllerState::default(),
                    snapshot,
                    &context,
                );
                if !review.scout_source_bound {
                    failures.push("SCOUT_SOURCE_NOT_BOUND");
                }
                if !review.controller_and_adapter_bound {
                    failures.push("CONTROLLER_ADAPTER_NOT_BOUND");
                }
                if review.prepared_order.is_none() {
                    failures.push("PREPARED_ORDER_NOT_PRODUCED_FROM_LIVE_SCOUT");
                }

                candidate_payload = review
                    .candidate
                    .as_ref()
                    .map(|candidate| {
                        json!({
                            "asset": candidate.asset,
                            "event_kind": candidate.event_kind,
                            "public_trade_taker_side": candidate.public_trade_taker_side,
                            "condition_id": candidate.condition_id,
                            "slug": candidate.slug,
                            "ts_ms": candidate.ts_ms,
                            "offset_s": candidate.offset_s,
                            "time_to_end_s": candidate.time_to_end_s,
                            "side": candidate.side.as_str(),
                            "public_trade_price": candidate.public_trade_price,
                            "l1_pair_ask": candidate.l1_pair_ask,
                            "l1_pair_available_qty": candidate.l1_pair_available_qty,
                            "buy_available_qty": candidate.buy_available_qty,
                            "strict_l1_age_ms": candidate.strict_l1_age_ms,
                            "public_trade_recv_lag_ms": candidate.public_trade_recv_lag_ms,
                            "forbidden_decision_fields_present": candidate.forbidden_decision_fields_present,
                            "snapshot_index_used_as_rank": candidate.snapshot_index_used_as_rank
                        })
                    })
                    .unwrap_or_else(|| json!(null));
                controller_payload = review
                    .controller_review
                    .as_ref()
                    .map(|controller| {
                        json!({
                            "intent_present": controller.intent.is_some(),
                            "intent": controller.intent.as_ref().map(|intent| json!({
                                "condition_id": intent.condition_id,
                                "slug": intent.slug,
                                "ts_ms": intent.ts_ms,
                                "side": intent.side.as_str(),
                                "price": intent.price,
                                "qty": intent.qty,
                                "execution_permitted": intent.execution_permitted
                            })),
                            "block_reason": controller.block_reason.map(|reason| reason.as_str()),
                            "execution_permitted": controller.execution_permitted
                        })
                    })
                    .unwrap_or_else(|| json!(null));
                adapter_payload = review
                    .adapter_review
                    .as_ref()
                    .map(|adapter| {
                        json!({
                            "native_adapter_ready_for_exact_runtime": adapter.native_adapter_ready_for_exact_runtime,
                            "official_order_units_bound": adapter.official_order_units_bound,
                            "block_reasons": adapter.block_reasons.iter().map(|reason| reason.as_str()).collect::<Vec<_>>(),
                            "effectful_execution_permitted": adapter.effectful_execution_permitted
                        })
                    })
                    .unwrap_or_else(|| json!(null));
                prepared_order_payload = review
                    .prepared_order
                    .as_ref()
                    .map(prepared_order_to_payload)
                    .unwrap_or_else(|| json!(null));
            }

            if !failures.is_empty() {
                print_payload(
                    base_payload("BLOCK_S8Y_LIVE_SCOUT_TO_ORDER_PREVIEW_FAIL_CLOSED"),
                    json!({
                        "orders_submitted": 0,
                        "signing_performed": false,
                        "raw_signature_output": false,
                        "scout_snapshot_sha256": scout_snapshot_sha256,
                        "candidate": candidate_payload,
                        "controller": controller_payload,
                        "adapter": adapter_payload,
                        "prepared_order": prepared_order_payload,
                        "failures": failures
                    }),
                );
                std::process::exit(2);
            }
            print_payload(
                base_payload("PASS_S8Y_LIVE_SCOUT_TO_ORDER_PREVIEW_NO_SUBMIT"),
                json!({
                    "orders_submitted": 0,
                    "signing_performed": false,
                    "raw_signature_output": false,
                    "scout_snapshot_sha256": scout_snapshot_sha256,
                    "candidate": candidate_payload,
                    "controller": controller_payload,
                    "adapter": adapter_payload,
                    "prepared_order": prepared_order_payload
                }),
            );
            Ok(())
        }
        Mode::OneRunDriverPreview => {
            let mut failures = Vec::new();
            validate_common(&args, &mut failures);
            if !args.no_submit {
                failures.push("NO_SUBMIT_REQUIRED_IN_ONE_RUN_DRIVER_PREVIEW");
            }
            let loaded = load_one_run_plan(&args, &mut failures);
            let mut summary = json!({});
            let mut one_run_plan_sha256 = None;
            if let Some((plan, sha256)) = loaded.as_ref() {
                one_run_plan_sha256 = Some(sha256.clone());
                summary = validate_one_run_plan(plan, &mut failures);
            }
            if !failures.is_empty() {
                print_payload(
                    base_payload("BLOCK_S8A_ONE_RUN_DRIVER_PREVIEW_FAIL_CLOSED"),
                    json!({
                        "orders_submitted": 0,
                        "signing_performed": false,
                        "raw_signature_output": false,
                        "one_run_plan_sha256": one_run_plan_sha256,
                        "one_run_summary": summary,
                        "failures": failures
                    }),
                );
                std::process::exit(2);
            }
            print_payload(
                base_payload("PASS_S8A_ONE_RUN_DRIVER_PREVIEW_NO_SUBMIT"),
                json!({
                    "orders_submitted": 0,
                    "signing_performed": false,
                    "raw_signature_output": false,
                    "one_run_plan_sha256": one_run_plan_sha256,
                    "one_run_summary": summary
                }),
            );
            Ok(())
        }
        Mode::OrderStatusFillEvidencePreview => {
            let mut failures = Vec::new();
            validate_common(&args, &mut failures);
            if !args.no_submit {
                failures.push("NO_SUBMIT_REQUIRED_IN_ORDER_STATUS_FILL_EVIDENCE_PREVIEW");
            }

            let loaded_order = load_prepared_order(&args, &mut failures);
            let loaded_evidence = load_order_status_fill_evidence(&args, &mut failures);
            let mut prepared_order_sha256 = None;
            let mut order_status_fill_evidence_sha256 = None;
            let mut review_payload = json!(null);

            if let (Some((order, order_sha)), Some((evidence, evidence_sha))) =
                (loaded_order.as_ref(), loaded_evidence.as_ref())
            {
                prepared_order_sha256 = Some(order_sha.clone());
                order_status_fill_evidence_sha256 = Some(evidence_sha.clone());
                validate_prepared_order(order, &mut failures);
                match s8a_prepared_order_from_payload(order) {
                    Ok(prepared) => {
                        let review = review_s8a_order_status_fill_evidence(&prepared, evidence);
                        if !review.block_reasons.is_empty() {
                            failures.push("ORDER_STATUS_FILL_EVIDENCE_REVIEW_BLOCKED");
                        }
                        review_payload = json!({
                            "event": review.event,
                            "block_reasons": review.block_reasons.iter().map(|reason| reason.as_str()).collect::<Vec<_>>(),
                            "order_id_or_failure_recorded": review.order_id_or_failure_recorded,
                            "exact_approval_bound": review.exact_approval_bound,
                            "prepared_order_bound": review.prepared_order_bound,
                            "status_source": evidence.status_source.as_str(),
                            "order_status": evidence.order_status.as_str(),
                            "filled_qty_from_exchange_or_order_status_only": review.filled_qty_from_exchange_or_order_status_only,
                            "submitted_size_counts_as_inventory": evidence.submitted_size_counts_as_inventory,
                            "partial_fill_threshold_used": evidence.partial_fill_threshold_used,
                            "forced_complement_after_fill": evidence.forced_complement_after_fill,
                            "no_open_order_remainder": review.no_open_order_remainder,
                            "actual_filled_qty_ledger_ready": review.actual_filled_qty_ledger_ready,
                            "effectful_execution_permitted": review.effectful_execution_permitted,
                            "fill_event": review.fill_event.as_ref().map(fill_event_payload)
                        });
                    }
                    Err(_) => failures.push("PREPARED_ORDER_TO_S8A_CONVERSION_FAILED"),
                }
            }

            if !failures.is_empty() {
                print_payload(
                    base_payload("BLOCK_S8S_ORDER_STATUS_FILL_EVIDENCE_PREVIEW_FAIL_CLOSED"),
                    json!({
                        "orders_submitted": 0,
                        "signing_performed": false,
                        "raw_signature_output": false,
                        "prepared_order_sha256": prepared_order_sha256,
                        "order_status_fill_evidence_sha256": order_status_fill_evidence_sha256,
                        "review": review_payload,
                        "failures": failures
                    }),
                );
                std::process::exit(2);
            }

            print_payload(
                base_payload("PASS_S8S_ORDER_STATUS_FILL_EVIDENCE_PREVIEW_NO_SUBMIT"),
                json!({
                    "orders_submitted": 0,
                    "signing_performed": false,
                    "raw_signature_output": false,
                    "prepared_order_sha256": prepared_order_sha256,
                    "order_status_fill_evidence_sha256": order_status_fill_evidence_sha256,
                    "review": review_payload
                }),
            );
            Ok(())
        }
        Mode::ExactApprovedOrderPathPreview | Mode::Execute => {
            let mut failures = Vec::new();
            validate_common(&args, &mut failures);
            if matches!(mode, Mode::ExactApprovedOrderPathPreview) && !args.no_submit {
                failures.push("NO_SUBMIT_REQUIRED_IN_PREVIEW");
            }
            let loaded = load_prepared_order(&args, &mut failures);
            if let Some((order, _)) = loaded.as_ref() {
                validate_prepared_order(order, &mut failures);
            }
            if !failures.is_empty() {
                print_payload(
                    base_payload("BLOCK_S8A_NATIVE_EFFECTFUL_RUNTIME_FAIL_CLOSED"),
                    json!({
                        "orders_submitted": 0,
                        "signing_performed": false,
                        "failures": failures
                    }),
                );
                std::process::exit(2);
            }
            let (order, prepared_order_sha256) =
                loaded.expect("prepared order loaded after validation");
            if matches!(mode, Mode::ExactApprovedOrderPathPreview) {
                print_payload(
                    base_payload("PASS_EXACT_APPROVED_ORDER_PATH_PREVIEW_NO_SUBMIT"),
                    json!({
                        "prepared_order_sha256": prepared_order_sha256,
                        "prepared_order": order,
                        "orders_submitted": 0,
                        "signing_performed": false,
                        "raw_signature_output": false
                    }),
                );
                return Ok(());
            }
            match execute(&args, &order, &prepared_order_sha256).await {
                Ok(exec_payload) => {
                    let post_run_reconciliation_passed = exec_payload
                        .get("post_run_reconciliation_passed")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    let orders_submitted = exec_payload
                        .get("orders_submitted_effectful")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(1);
                    let cancel_submissions = exec_payload
                        .get("cancel_submissions_effectful")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let status = if post_run_reconciliation_passed {
                        "PASS_S8Y_NATIVE_RUNTIME_ORDER_SUBMITTED_AND_RECONCILED"
                    } else {
                        "BLOCK_S8Y_NATIVE_RUNTIME_POST_SUBMIT_RECONCILIATION_FAILED"
                    };
                    print_payload(
                        base_payload(status),
                        json!({
                            "orders_submitted": orders_submitted,
                            "cancel_submissions": cancel_submissions,
                            "signing_performed": true,
                            "raw_signature_output": false,
                            "execution_result": exec_payload
                        }),
                    );
                    if post_run_reconciliation_passed {
                        Ok(())
                    } else {
                        std::process::exit(2);
                    }
                }
                Err(err) => {
                    print_payload(
                        base_payload("BLOCK_S8A_NATIVE_RUNTIME_EXECUTE_FAILED_CLOSED"),
                        json!({
                            "orders_submitted": 0,
                            "signing_performed": false,
                            "error": err.to_string(),
                            "raw_signature_output": false
                        }),
                    );
                    std::process::exit(2);
                }
            }
        }
    }
}
