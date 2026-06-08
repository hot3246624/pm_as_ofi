use std::env;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, U256};
use anyhow::{anyhow, Context, Result};
use pm_as_ofi::polymarket::clob_v2::{
    build_signed_limit_order_v2, builder_code_from_env, infer_signature_type, post_order_v2,
    v2_contract_config, OrderSizingV2, V2OrderContext,
};
use pm_as_ofi::polymarket::executor::init_clob_client;
use pm_as_ofi::polymarket::messages::TradeDirection;
use pm_as_ofi::polymarket::s8a_order_adapter::{
    apply_s8a_fill_to_inventory, S8aAdapterOrderAction, S8aFillEvent, S8aInventory,
    S8A_LIMIT_ENTRY_ORDER_SIZE_SHARES, S8A_MARKET_BUY_MIN_NOTIONAL_USDC,
    S8A_MARKET_SELL_MIN_ORDER_SIZE_SHARES, S8A_MAX_ROUNDS, S8A_PER_ROUND_THEORETICAL_MAX_LOSS_USDC,
    S8A_SEED_PX_HI, S8A_SEED_PX_LO, S8A_SESSION_HARD_LOSS_CAP_USDC,
};
use pm_as_ofi::polymarket::types::Side;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::{OrderStatusType, OrderType};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use uuid::Uuid;

const SCOPE: &str = "B_STRATEGY_CANARY_S8A_MICRO_SHORT_CYCLE_ONE_RUN_MAX_THREE_ROUNDS_BTC5M_SIZE5_15USDC_LOSS_CAP_NATIVE_RUNTIME_S8H";
const REVIEWED_HOST: &str = "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com";
const OFFICIAL_CLOB_REST_URL: &str = "https://clob.polymarket.com";
const ORDER_PRIMITIVE_NAME: &str = "clob_v2.build_signed_limit_order_v2/post_order_v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    ReviewOnly,
    PreviewNoApproval,
    NoOrderAuthPreview,
    ExactApprovedOrderPathPreview,
    OneRunDriverPreview,
    Execute,
}

#[derive(Debug, Clone, Default)]
struct Args {
    mode: Option<Mode>,
    reviewed_host: String,
    rest_url: String,
    prepared_order_json: Option<PathBuf>,
    expected_prepared_order_sha256: Option<String>,
    one_run_plan_json: Option<PathBuf>,
    expected_one_run_plan_sha256: Option<String>,
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
                    "one-run-driver-preview" => Mode::OneRunDriverPreview,
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
) -> Option<(PreparedOrder, Option<String>)> {
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
    Some((parsed, Some(actual_sha)))
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

fn order_type_from_str(order_type: &str) -> Result<OrderType> {
    match order_type {
        "GTC" => Ok(OrderType::GTC),
        "FAK" => Ok(OrderType::FAK),
        _ => Err(anyhow!("unsupported order type")),
    }
}

async fn execute(args: &Args, order: &PreparedOrder) -> Result<serde_json::Value> {
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
    Ok(json!({
        "order_id": response.order_id,
        "status": format!("{:?}", response.status),
        "success": response.success,
        "taking_amount": response.taking_amount,
        "making_amount": response.making_amount,
        "side": side_from_str(&order.side)?.as_str(),
        "action": order.action,
        "order_type": order.order_type,
        "submitted_size_shares": order.amount.value,
        "submitted_limit_price": order.limit_price,
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
            match execute(&args, &order).await {
                Ok(exec_payload) => {
                    print_payload(
                        base_payload("PASS_S8A_NATIVE_RUNTIME_ORDER_SUBMITTED"),
                        json!({
                            "orders_submitted": 1,
                            "signing_performed": true,
                            "raw_signature_output": false,
                            "execution_result": exec_payload
                        }),
                    );
                    Ok(())
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
