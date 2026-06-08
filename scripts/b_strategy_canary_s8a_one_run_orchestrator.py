#!/usr/bin/env python3
"""S8R review-only one-run orchestrator for the native S8A runtime.

This wrapper binds the live-scout preview, native prepared-order primitive,
actual-filled-qty ledger contract, S8A session caps, and S7W reconciliation
requirements into one reviewable no-submit flow.

It intentionally performs no network IO of its own, reads no secrets, signs no
payload, submits no orders, cancels nothing, and performs no recovery action.
The only subprocess it may invoke is the source-controlled native runtime, and
only in no-submit or fail-closed preview modes. S8Y made the single prepared
order primitive closed-loop capable; this orchestrator must still grow the full
multi-round live loop before another broad S8A exact approval is safe.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCOPE = (
    "B_STRATEGY_CANARY_S8A_MICRO_SHORT_CYCLE_ONE_RUN_MAX_THREE_ROUNDS_"
    "BTC5M_SIZE5_15USDC_LOSS_CAP_NATIVE_RUNTIME_S8X"
)
REVIEWED_HOST = "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com"
OFFICIAL_CLOB_REST_URL = "https://clob.polymarket.com"
ORDER_PRIMITIVE_NAME = "clob_v2.build_signed_limit_order_v2/post_order_v2"
S7W_RESULT_SHA256 = "b2490770fe7cfaea6671f660eb4349a78ab43695b97811a8c1b8bf393662dea2"
LOSS_TRIGGER_PACKET_SHA256 = "a602729f6697c4ccc82a6cf7b3f1cefd049f8fda9b6884da56d5bb5a4e399fdf"
PARTIAL_FILL_ADDENDUM_SHA256 = "1545a9268e6fa6bacb164ebac5c2878193863fb59309b3a38eeab1534847e00d"
THREE_ROUND_CAP_PACKET_SHA256 = "1fb20485b5e37931c7d8f087464edc9fb463aed3c41dcde90014ad5abf63852c"
S8P_PACKET_SHA256 = "f99af862f56774a2c09578fc11c1ba24b462f4b21ffd4538e8e478ad117709a7"
S8Q_PACKET_SHA256 = "9d076bd3e7889ff79b73801d9a575e2d925a240750c1392bb168474b427b417d"
S8Y_PACKET_SHA256 = "972bc47a39601cbced054c02c8c56e6b3d4cb6ae1e66a3d76a489b93426a6686"
S8Y_REMOTE_RESULT_SHA256 = "90cec9f0a9ac1da8c23979b43b8c7236d98837745a4ed137f4fdcc64fc0b6e63"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def hash64(value: str | None) -> bool:
    return value is not None and len(value) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in value)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def as_finite_float(value: Any, default: float = math.nan) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def write_json(path: Path, payload: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.write_bytes(data)
    return sha256_bytes(data)


def base_payload(status: str) -> dict[str, Any]:
    return {
        "schema_version": "B_STRATEGY_CANARY_S8T_ONE_RUN_ORCHESTRATOR_WITH_FILL_EVIDENCE_v1",
        "status": status,
        "generated_at": utc_now(),
        "scope": SCOPE,
        "effectful_execution_permitted": False,
        "execution_performed": False,
        "orders_submitted": 0,
        "cancels_submitted": 0,
        "signing_performed": False,
        "merge_or_redeem_performed": False,
        "funding_live_latest_deploy_performed": False,
        "hard_bound_hashes": {
            "s7w_result_sha256": S7W_RESULT_SHA256,
            "s8a_loss_trigger_redesign_packet_sha256": LOSS_TRIGGER_PACKET_SHA256,
            "s8a_partial_fill_addendum_sha256": PARTIAL_FILL_ADDENDUM_SHA256,
            "s8a_three_round_cap_packet_sha256": THREE_ROUND_CAP_PACKET_SHA256,
            "s8p_native_runtime_scope_live_scout_packet_sha256": S8P_PACKET_SHA256,
            "s8q_remote_runtime_scope_live_scout_packet_sha256": S8Q_PACKET_SHA256,
            "s8y_closed_loop_runtime_local_remote_packet_sha256": S8Y_PACKET_SHA256,
            "s8y_remote_closed_loop_runtime_gates_result_sha256": S8Y_REMOTE_RESULT_SHA256,
        },
        "strategy_scope": {
            "market_family": "btc-5min",
            "fixed_policy": "cool5_imb1.25_source_guard_500",
            "source_guard_500_required": True,
            "size": 5.0,
            "online_tuning_allowed": False,
            "strategy_discovery_allowed": False,
            "candidate_import_allowed": False,
        },
        "session_caps": {
            "max_rounds": 3,
            "session_hard_loss_cap_usdc": 15.0,
            "per_round_theoretical_max_loss_usdc": 5.0,
            "max_active_market": 1,
            "max_active_capital_lock_usdc": 6.0,
            "max_cumulative_gross_quote_spend_usdc": 18.0,
            "max_initial_buy_submissions": 6,
            "max_emergency_exit_or_hedge_submissions": 3,
            "max_total_order_submissions": 9,
            "max_cancel_count": 6,
            "max_recovery_tx_count": 3,
        },
        "inventory_semantics": {
            "actual_filled_qty_only": True,
            "submitted_size_counts_as_inventory": False,
            "partial_fill_threshold_enabled": False,
            "forced_complement_allowed": False,
            "sub_min_residual_sell_blocked": True,
        },
        "source_binding": {
            "input": "B-owned direct public scout snapshot",
            "runtime_opens_ws": False,
            "max_b_owned_direct_public_ws_connections": 1,
            "shared_ingress_dependency": False,
            "uses_c_artifacts": False,
        },
        "runtime_binding": {
            "native_runtime_required": True,
            "native_runtime_accepts_only_prepared_orders": True,
            "order_primitive_name": ORDER_PRIMITIVE_NAME,
            "preview_without_approval_exit_code": 66,
            "no_order_auth_preview_required": True,
            "exact_order_path_preview_no_submit_required": True,
            "live_scout_to_order_preview_no_submit_required": True,
            "order_status_fill_evidence_preview_no_submit_required": True,
            "one_run_driver_preview_no_submit_required": True,
            "execute_without_execute_approved_must_fail_closed": True,
        },
        "s7w_reconciliation": {
            "required": True,
            "exact_approved_receipt_tier_required": True,
            "review_only_fixture_receipt_rejected": True,
            "collateral_pre_post_observation_required": True,
            "positive_collateral_delta_required": True,
            "local_ledger_external_position_alignment_required": True,
            "receipt_factory_conversion_required": True,
            "verified_recovery_event_required": True,
            "no_open_order_remainder_required": True,
            "residual_exposure_zero_required": True,
        },
    }


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def run_child(label: str, cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    parsed: Any = None
    parse_error: str | None = None
    if proc.stdout.strip():
        try:
            parsed = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            parse_error = str(exc)
    return {
        "label": label,
        "argv": cmd,
        "returncode": proc.returncode,
        "stdout_sha256": sha256_bytes(proc.stdout.encode("utf-8")),
        "stderr_sha256": sha256_bytes(proc.stderr.encode("utf-8")),
        "stdout_json": parsed,
        "stdout_parse_error": parse_error,
    }


def child_status(child: dict[str, Any]) -> str | None:
    parsed = child.get("stdout_json")
    return parsed.get("status") if isinstance(parsed, dict) else None


def child_orders(child: dict[str, Any]) -> int | None:
    parsed = child.get("stdout_json")
    if isinstance(parsed, dict) and isinstance(parsed.get("orders_submitted"), int):
        return parsed["orders_submitted"]
    return None


def child_signing(child: dict[str, Any]) -> bool | None:
    parsed = child.get("stdout_json")
    if isinstance(parsed, dict) and isinstance(parsed.get("signing_performed"), bool):
        return parsed["signing_performed"]
    return None


def assert_no_submit_child(label: str, child: dict[str, Any], expected_rc: int | None, failures: list[str]) -> None:
    if expected_rc is not None and child["returncode"] != expected_rc:
        failures.append(f"{label}_RC_MISMATCH")
    if child.get("stdout_parse_error"):
        failures.append(f"{label}_STDOUT_NOT_JSON")
    orders = child_orders(child)
    if orders not in (0, None):
        failures.append(f"{label}_SUBMITTED_ORDERS")
    signing = child_signing(child)
    if signing is True:
        failures.append(f"{label}_PERFORMED_SIGNING")


def exact_args(args: argparse.Namespace) -> list[str]:
    return [
        "--reviewed-host",
        args.reviewed_host,
        "--rest-url",
        args.rest_url,
        "--exact-approval-sha256",
        args.exact_approval_sha256,
        "--expected-exact-approval-sha256",
        args.expected_exact_approval_sha256,
        "--approval-scope",
        args.approval_scope,
        "--order-primitive-name",
        args.order_primitive_name,
        "--order-primitive-source-sha256",
        args.order_primitive_source_sha256,
    ]


def prepared_order_failures(order: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(order, dict):
        return ["PREPARED_ORDER_NOT_OBJECT"]
    if order.get("source") != "native_s8a_adapter":
        failures.append("PREPARED_ORDER_SOURCE_MISMATCH")
    if order.get("execution_permitted") is not False:
        failures.append("PREPARED_ORDER_EXECUTION_MUST_BE_FALSE_IN_PREVIEW")
    if order.get("natural_controller_admission") is not True:
        failures.append("PREPARED_ORDER_NOT_NATURAL_ADMISSION")
    if order.get("forced_complement") is not False:
        failures.append("PREPARED_ORDER_FORCED_COMPLEMENT")
    if order.get("source_guard_500_passed") is not True:
        failures.append("PREPARED_ORDER_SOURCE_GUARD_500_NOT_PASSED")
    amount = order.get("amount")
    if not isinstance(amount, dict) or amount.get("unit") != "SHARES" or float(amount.get("value", 0.0)) != 5.0:
        failures.append("PREPARED_ORDER_NOT_SIZE5_SHARES")
    if order.get("order_type") != "GTC" or order.get("action") != "BUY":
        failures.append("PREPARED_ORDER_NOT_GTC_BUY")
    return failures


def one_run_summary_failures(summary: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(summary, dict):
        return ["ONE_RUN_SUMMARY_MISSING"]
    if summary.get("final_residual_qty") not in (0, 0.0):
        failures.append("ONE_RUN_FINAL_RESIDUAL_NOT_ZERO")
    if int(summary.get("closed_round_count", 0)) < 1:
        failures.append("ONE_RUN_NO_CLOSED_ROUND")
    if float(summary.get("gross_quote_spend_usdc", 999.0)) > 18.0:
        failures.append("ONE_RUN_GROSS_SPEND_CAP_EXCEEDED")
    if float(summary.get("realized_session_loss_usdc", 999.0)) > 15.0:
        failures.append("ONE_RUN_REALIZED_LOSS_CAP_EXCEEDED")
    return failures


def full_loop_contract_plan_failures(plan: Any) -> tuple[list[str], dict[str, Any]]:
    failures: list[str] = []
    state: dict[str, Any] = {
        "attempts_seen": 0,
        "orders_planned": 0,
        "initial_buy_submissions_planned": 0,
        "completed_rounds": 0,
        "active_condition_id": None,
        "realized_session_loss_usdc": 0.0,
        "gross_quote_spend_usdc": 0.0,
        "max_active_capital_lock_usdc_seen": 0.0,
        "max_active_round_risk_at_work_usdc_seen": 0.0,
        "inventory_by_condition": {},
        "transitions": [],
    }
    if not isinstance(plan, dict):
        return ["FULL_LOOP_PLAN_NOT_OBJECT"], state

    if plan.get("source") != "native_s8a_one_run_driver":
        failures.append("FULL_LOOP_PLAN_SOURCE_MISMATCH")
    if plan.get("effectful_execution_permitted") is not False:
        failures.append("FULL_LOOP_PLAN_MUST_BE_REVIEW_ONLY")
    if plan.get("runtime_accepts_only_prepared_orders") is not True:
        failures.append("FULL_LOOP_RUNTIME_MUST_ACCEPT_ONLY_PREPARED_ORDERS")
    if plan.get("runtime_order_result_ledger_enabled") is not True:
        failures.append("FULL_LOOP_RUNTIME_ORDER_RESULT_LEDGER_NOT_ENABLED")
    if plan.get("actual_filled_qty_ledger_updates_inventory") is not True:
        failures.append("FULL_LOOP_ACTUAL_FILLED_QTY_LEDGER_NOT_BOUND")
    if plan.get("submitted_size_counts_as_inventory") is not False:
        failures.append("FULL_LOOP_SUBMITTED_SIZE_COUNTS_AS_INVENTORY")
    if plan.get("partial_fill_threshold_enabled") is not False:
        failures.append("FULL_LOOP_PARTIAL_FILL_THRESHOLD_ENABLED")
    if plan.get("forced_complement_allowed") is not False:
        failures.append("FULL_LOOP_FORCED_COMPLEMENT_ALLOWED")
    if plan.get("s7w_reconciliation_bound_to_run_result") is not True:
        failures.append("FULL_LOOP_S7W_RECONCILIATION_NOT_BOUND")
    if plan.get("shared_ingress_dependency") is not False or plan.get("uses_c_artifacts") is not False:
        failures.append("FULL_LOOP_FORBIDDEN_SOURCE_DEPENDENCY")
    if plan.get("secret_or_raw_signature_output_allowed") is not False:
        failures.append("FULL_LOOP_SECRET_OR_RAW_SIGNATURE_OUTPUT_ALLOWED")
    if plan.get("funding_live_latest_or_deploy_requested") is not False:
        failures.append("FULL_LOOP_FUNDING_LIVE_LATEST_DEPLOY_REQUESTED")

    strategy_scope = plan.get("strategy_scope")
    if not isinstance(strategy_scope, dict):
        failures.append("FULL_LOOP_STRATEGY_SCOPE_MISSING")
    else:
        expected_strategy = {
            "market_family": "btc-5min",
            "fixed_policy": "cool5_imb1.25_source_guard_500",
            "source_guard_500_required": True,
            "online_tuning_allowed": False,
            "strategy_discovery_allowed": False,
            "candidate_import_allowed": False,
        }
        for key, expected in expected_strategy.items():
            if strategy_scope.get(key) != expected:
                failures.append(f"FULL_LOOP_STRATEGY_{key.upper()}_MISMATCH")

    caps = plan.get("caps")
    if not isinstance(caps, dict):
        failures.append("FULL_LOOP_CAPS_MISSING")
        caps = {}
    if int(caps.get("max_round_count", -1)) != 3:
        failures.append("FULL_LOOP_MAX_ROUND_COUNT_MISMATCH")
    if as_finite_float(caps.get("session_hard_loss_cap_usdc")) != 15.0:
        failures.append("FULL_LOOP_SESSION_LOSS_CAP_MISMATCH")
    if as_finite_float(caps.get("per_round_theoretical_max_loss_usdc")) != 5.0:
        failures.append("FULL_LOOP_PER_ROUND_THEORETICAL_LOSS_MISMATCH")
    if int(caps.get("max_active_market_count", -1)) != 1:
        failures.append("FULL_LOOP_ACTIVE_MARKET_CAP_MISMATCH")
    if as_finite_float(caps.get("max_cumulative_gross_quote_spend_usdc")) != 18.0:
        failures.append("FULL_LOOP_GROSS_QUOTE_SPEND_CAP_MISMATCH")
    if int(caps.get("max_total_order_submissions", -1)) != 9:
        failures.append("FULL_LOOP_TOTAL_ORDER_SUBMISSION_CAP_MISMATCH")

    attempts = plan.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return failures + ["FULL_LOOP_ATTEMPTS_MISSING"], state

    inventory: dict[str, dict[str, float]] = {}
    cost_by_condition: dict[str, float] = {}
    active_condition_id: str | None = None
    realized_session_loss_usdc = 0.0
    gross_quote_spend_usdc = 0.0
    completed_rounds = 0
    planned_order_count = 0
    initial_buy_count = 0

    for idx, attempt in enumerate(attempts, start=1):
        state["attempts_seen"] = idx
        if not isinstance(attempt, dict):
            failures.append(f"ATTEMPT_{idx}_NOT_OBJECT")
            continue
        order = attempt.get("prepared_order")
        failures.extend(f"ATTEMPT_{idx}_{failure}" for failure in prepared_order_failures(order))
        if not isinstance(order, dict):
            continue

        condition_id = order.get("condition_id")
        side = order.get("side")
        action = order.get("action")
        amount = order.get("amount") if isinstance(order.get("amount"), dict) else {}
        order_size = as_finite_float(amount.get("value"))
        actual_filled_qty = as_finite_float(attempt.get("actual_filled_qty_shares"))
        avg_fill_price = as_finite_float(attempt.get("avg_fill_price"))
        realized_loss_delta = as_finite_float(attempt.get("realized_loss_delta_usdc"), 0.0)
        close_round = bool(attempt.get("close_round_after_attempt"))

        if not isinstance(condition_id, str) or not condition_id:
            failures.append(f"ATTEMPT_{idx}_CONDITION_ID_MISSING")
            continue
        if active_condition_id is not None and condition_id != active_condition_id:
            failures.append(f"ATTEMPT_{idx}_ACTIVE_MARKET_CAP_WOULD_BE_EXCEEDED")
        if active_condition_id is None:
            if realized_session_loss_usdc + 5.0 > 15.0 + 1e-9:
                failures.append(f"ATTEMPT_{idx}_PRE_ENTRY_LOSS_CAP_BLOCK_MISSING")
            active_condition_id = condition_id

        if action == "BUY":
            initial_buy_count += 1
        planned_order_count += 1
        if planned_order_count > 9:
            failures.append(f"ATTEMPT_{idx}_TOTAL_ORDER_SUBMISSION_CAP_EXCEEDED")
        if initial_buy_count > 6:
            failures.append(f"ATTEMPT_{idx}_INITIAL_BUY_SUBMISSION_CAP_EXCEEDED")
        if completed_rounds >= 3:
            failures.append(f"ATTEMPT_{idx}_MAX_ROUNDS_ALREADY_COMPLETED")
        if order_size != 5.0:
            failures.append(f"ATTEMPT_{idx}_ORDER_SIZE_NOT_5")
        if actual_filled_qty < 0.0 or actual_filled_qty > order_size + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTUAL_FILLED_QTY_OUT_OF_RANGE")
        if actual_filled_qty > 0.0 and avg_fill_price < 0.0:
            failures.append(f"ATTEMPT_{idx}_AVG_FILL_PRICE_INVALID")
        if attempt.get("filled_qty_from_exchange_or_order_status") is not True:
            failures.append(f"ATTEMPT_{idx}_FILLED_QTY_NOT_EXCHANGE_DERIVED")
        if attempt.get("order_id_or_failure_recorded") is not True:
            failures.append(f"ATTEMPT_{idx}_ORDER_ID_OR_FAILURE_NOT_RECORDED")
        if attempt.get("no_open_order_remainder") is not True:
            failures.append(f"ATTEMPT_{idx}_OPEN_ORDER_REMAINDER_NOT_CLEARED")

        condition_inventory = inventory.setdefault(condition_id, {"YES": 0.0, "NO": 0.0})
        cost_by_condition.setdefault(condition_id, 0.0)
        filled_qty = max(actual_filled_qty, 0.0)
        fill_cost = filled_qty * max(avg_fill_price, 0.0)
        gross_quote_spend_usdc += fill_cost
        cost_by_condition[condition_id] += fill_cost
        if gross_quote_spend_usdc > 18.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_GROSS_QUOTE_SPEND_CAP_EXCEEDED")
        if side in ("YES", "NO"):
            condition_inventory[side] += filled_qty
        else:
            failures.append(f"ATTEMPT_{idx}_SIDE_UNSUPPORTED")

        residual_qty = abs(condition_inventory["YES"] - condition_inventory["NO"])
        paired_qty = min(condition_inventory["YES"], condition_inventory["NO"])
        active_round_risk_at_work_usdc = residual_qty
        active_capital_lock_usdc = cost_by_condition[condition_id]
        state["max_active_round_risk_at_work_usdc_seen"] = max(
            state["max_active_round_risk_at_work_usdc_seen"],
            active_round_risk_at_work_usdc,
        )
        state["max_active_capital_lock_usdc_seen"] = max(
            state["max_active_capital_lock_usdc_seen"],
            active_capital_lock_usdc,
        )
        if active_round_risk_at_work_usdc > 5.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTIVE_ROUND_RISK_CAP_EXCEEDED")
        if active_capital_lock_usdc > 6.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTIVE_CAPITAL_LOCK_CAP_EXCEEDED")

        realized_session_loss_usdc += max(realized_loss_delta, 0.0)
        if realized_session_loss_usdc > 15.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_REALIZED_SESSION_LOSS_CAP_EXCEEDED")

        if close_round:
            if residual_qty > 1e-9:
                failures.append(f"ATTEMPT_{idx}_CLOSE_ROUND_WITH_RESIDUAL_EXPOSURE")
            if attempt.get("s7w_reconciliation_passed") is not True:
                failures.append(f"ATTEMPT_{idx}_S7W_RECONCILIATION_NOT_PASSED_ON_CLOSE")
            if attempt.get("exact_approved_receipt_tier_required") is not True:
                failures.append(f"ATTEMPT_{idx}_EXACT_APPROVED_RECEIPT_TIER_NOT_REQUIRED_ON_CLOSE")
            if attempt.get("positive_collateral_delta_required_for_recovery") is not True:
                failures.append(f"ATTEMPT_{idx}_POSITIVE_COLLATERAL_DELTA_NOT_REQUIRED_ON_CLOSE")
            completed_rounds += 1
            active_condition_id = None

        state["transitions"].append(
            {
                "attempt_index": idx,
                "condition_id": condition_id,
                "side": side,
                "action": action,
                "actual_filled_qty_shares": actual_filled_qty,
                "submitted_size_counted_as_inventory": False,
                "yes_qty": condition_inventory["YES"],
                "no_qty": condition_inventory["NO"],
                "paired_qty": paired_qty,
                "residual_qty": residual_qty,
                "active_round_risk_at_work_usdc": active_round_risk_at_work_usdc,
                "active_capital_lock_usdc": active_capital_lock_usdc,
                "completed_rounds_after_attempt": completed_rounds,
                "close_round_after_attempt": close_round,
            }
        )

    state.update(
        {
            "orders_planned": planned_order_count,
            "initial_buy_submissions_planned": initial_buy_count,
            "completed_rounds": completed_rounds,
            "active_condition_id": active_condition_id,
            "realized_session_loss_usdc": realized_session_loss_usdc,
            "gross_quote_spend_usdc": gross_quote_spend_usdc,
            "inventory_by_condition": inventory,
        }
    )
    if completed_rounds < 1:
        failures.append("FULL_LOOP_NO_CLOSED_ROUND_IN_PLAN")
    return failures, state


def full_loop_contract_preview(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if args.reviewed_host != REVIEWED_HOST:
        failures.append("REVIEWED_HOST_MISMATCH")
    if args.rest_url != OFFICIAL_CLOB_REST_URL:
        failures.append("REST_URL_MUST_BE_OFFICIAL_CLOB")
    if args.approval_scope != SCOPE:
        failures.append("APPROVAL_SCOPE_MISMATCH")
    if args.order_primitive_name != ORDER_PRIMITIVE_NAME:
        failures.append("ORDER_PRIMITIVE_NAME_MISMATCH")
    if not hash64(args.exact_approval_sha256) or args.exact_approval_sha256 != args.expected_exact_approval_sha256:
        failures.append("EXACT_APPROVAL_SHA256_MISMATCH_OR_INVALID")
    if not hash64(args.order_primitive_source_sha256):
        failures.append("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX")
    if not args.no_submit:
        failures.append("NO_SUBMIT_REQUIRED")
    if args.execute_approved:
        failures.append("EXECUTE_APPROVED_FORBIDDEN_IN_CONTRACT_PREVIEW")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED")
    if args.allow_online_tuning or args.allow_candidate_import:
        failures.append("FORBIDDEN_ONLINE_TUNING_OR_CANDIDATE_IMPORT")

    runtime = Path(args.runtime_bin)
    plan = Path(args.one_run_plan_json)
    if not runtime.exists():
        failures.append("RUNTIME_BIN_MISSING")
    if not plan.exists():
        failures.append("ONE_RUN_PLAN_JSON_MISSING")

    plan_sha = sha256_file(plan) if plan.exists() else None
    if plan_sha != args.expected_one_run_plan_sha256:
        failures.append("ONE_RUN_PLAN_SHA256_MISMATCH")

    plan_payload = load_json(plan) if plan.exists() else {}
    plan_failures, loop_state = full_loop_contract_plan_failures(plan_payload)
    failures.extend(plan_failures)

    children: list[dict[str, Any]] = []
    if not failures:
        children.append(
            run_child(
                "one_run_driver_preview_contract_crosscheck",
                [
                    str(runtime),
                    "--mode",
                    "one-run-driver-preview",
                    "--one-run-plan-json",
                    str(plan),
                    "--expected-one-run-plan-sha256",
                    args.expected_one_run_plan_sha256,
                    *exact_args(args),
                    "--no-submit",
                ],
            )
        )
        assert_no_submit_child("ONE_RUN_DRIVER_PREVIEW_CONTRACT_CROSSCHECK", children[-1], 0, failures)
        one_run_payload = children[-1].get("stdout_json") or {}
        failures.extend(one_run_summary_failures(one_run_payload.get("one_run_summary")))

    child_summaries = [
        {
            "label": child["label"],
            "returncode": child["returncode"],
            "status": child_status(child),
            "orders_submitted": child_orders(child),
            "signing_performed": child_signing(child),
            "stdout_sha256": child["stdout_sha256"],
            "stderr_sha256": child["stderr_sha256"],
            "stdout_parse_error": child["stdout_parse_error"],
        }
        for child in children
    ]

    payload = base_payload(
        "PASS_S8AA_FULL_ONE_RUN_LOOP_CONTRACT_PREVIEW"
        if not failures
        else "BLOCK_S8AA_FULL_ONE_RUN_LOOP_CONTRACT_FAIL_CLOSED"
    )
    payload.update(
        {
            "schema_version": "B_STRATEGY_CANARY_S8AA_FULL_ONE_RUN_LOOP_CONTRACT_PREVIEW_v1",
            "review_only_no_submit": True,
            "full_one_run_loop_contract_preview_ready": not failures,
            "full_one_run_loop_execute_ready": False,
            "ready_for_fresh_exact_approval": False,
            "ready_for_fresh_exact_approval_reason": (
                "S8AA validates the full-loop state-machine contract and S8Y primitive "
                "binding under no-submit. A broad S8A exact approval still requires "
                "remote no-submit gates for this contract and an execute wrapper that "
                "calls the S8Y closed-loop primitive only after fresh authorization."
            ),
            "one_run_plan_sha256": plan_sha,
            "loop_state_machine": loop_state,
            "child_previews": child_summaries,
            "failures": failures,
            "secret_values_read": False,
            "secret_values_printed": False,
            "raw_signature_output": False,
        }
    )
    print_json(payload)
    return 0 if not failures else 2


def canonical_json_sha256(payload: Any) -> str:
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    return sha256_bytes(data)


def s9a_execute_wrapper_preview(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if args.reviewed_host != REVIEWED_HOST:
        failures.append("REVIEWED_HOST_MISMATCH")
    if args.rest_url != OFFICIAL_CLOB_REST_URL:
        failures.append("REST_URL_MUST_BE_OFFICIAL_CLOB")
    if args.approval_scope != SCOPE:
        failures.append("APPROVAL_SCOPE_MISMATCH")
    if args.order_primitive_name != ORDER_PRIMITIVE_NAME:
        failures.append("ORDER_PRIMITIVE_NAME_MISMATCH")
    if not hash64(args.exact_approval_sha256) or args.exact_approval_sha256 != args.expected_exact_approval_sha256:
        failures.append("EXACT_APPROVAL_SHA256_MISMATCH_OR_INVALID")
    if not hash64(args.order_primitive_source_sha256):
        failures.append("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX")
    if not args.no_submit:
        failures.append("NO_SUBMIT_REQUIRED")
    if args.execute_approved:
        failures.append("EXECUTE_APPROVED_FORBIDDEN_IN_S9A_PREVIEW")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED")
    if args.allow_online_tuning or args.allow_candidate_import:
        failures.append("FORBIDDEN_ONLINE_TUNING_OR_CANDIDATE_IMPORT")

    runtime = Path(args.runtime_bin)
    plan = Path(args.one_run_plan_json)
    if not runtime.exists():
        failures.append("RUNTIME_BIN_MISSING")
    if not plan.exists():
        failures.append("ONE_RUN_PLAN_JSON_MISSING")

    plan_sha = sha256_file(plan) if plan.exists() else None
    if plan_sha != args.expected_one_run_plan_sha256:
        failures.append("ONE_RUN_PLAN_SHA256_MISMATCH")

    plan_payload = load_json(plan) if plan.exists() else {}
    plan_failures, loop_state = full_loop_contract_plan_failures(plan_payload)
    failures.extend(plan_failures)

    submit_plan: list[dict[str, Any]] = []
    if isinstance(plan_payload, dict) and isinstance(plan_payload.get("attempts"), list):
        realized_loss_before = 0.0
        active_condition_id: str | None = None
        gross_quote_spend_before = 0.0
        orders_before = 0
        completed_rounds_before = 0
        for idx, attempt in enumerate(plan_payload["attempts"], start=1):
            order = attempt.get("prepared_order") if isinstance(attempt, dict) else None
            if not isinstance(order, dict):
                continue
            condition_id = order.get("condition_id")
            amount = order.get("amount") if isinstance(order.get("amount"), dict) else {}
            order_size = as_finite_float(amount.get("value"))
            limit_price = as_finite_float(order.get("limit_price"))
            prepared_order_sha = canonical_json_sha256(order)
            pre_submit_failures: list[str] = []
            if completed_rounds_before >= 3:
                pre_submit_failures.append("MAX_ROUNDS_COMPLETED")
            if realized_loss_before + 5.0 > 15.0 + 1e-9:
                pre_submit_failures.append("PRE_ENTRY_LOSS_CAP")
            if active_condition_id is not None and active_condition_id != condition_id:
                pre_submit_failures.append("ACTIVE_MARKET_CAP")
            if orders_before >= 9:
                pre_submit_failures.append("TOTAL_ORDER_SUBMISSION_CAP")
            projected_spend = gross_quote_spend_before + max(order_size, 0.0) * max(limit_price, 0.0)
            if projected_spend > 18.0 + 1e-9:
                pre_submit_failures.append("PROJECTED_GROSS_QUOTE_SPEND_CAP")
            if order.get("forced_complement") is not False:
                pre_submit_failures.append("FORCED_COMPLEMENT")
            if order.get("natural_controller_admission") is not True:
                pre_submit_failures.append("NOT_NATURAL_ADMISSION")

            submit_plan.append(
                {
                    "attempt_index": idx,
                    "condition_id": condition_id,
                    "side": order.get("side"),
                    "prepared_order_sha256": prepared_order_sha,
                    "pre_submit_gate_passed": not pre_submit_failures,
                    "pre_submit_failures": pre_submit_failures,
                    "future_runtime_execute_argv_template": [
                        str(runtime),
                        "--mode",
                        "execute",
                        "--prepared-order-json",
                        f"<S9A_ATTEMPT_{idx}_HASH_BOUND_PREPARED_ORDER.json>",
                        "--expected-prepared-order-sha256",
                        prepared_order_sha,
                        *exact_args(args),
                        "--execute-approved",
                    ],
                    "post_submit_required_evidence": [
                        "order_id_or_failure_recorded",
                        "order_status_or_trades_evidence",
                        "open_remainder_cancel_or_no_remainder_evidence",
                        "actual_filled_qty_from_exchange_only",
                        "local_inventory_update_from_actual_filled_qty_only",
                        "s7w_close_reconciliation_when_round_closes",
                    ],
                }
            )
            if pre_submit_failures:
                failures.append(f"ATTEMPT_{idx}_PRE_SUBMIT_GATE_FAIL")
            active_condition_id = condition_id if not attempt.get("close_round_after_attempt") else None
            gross_quote_spend_before += max(as_finite_float(attempt.get("actual_filled_qty_shares")), 0.0) * max(
                as_finite_float(attempt.get("avg_fill_price")),
                0.0,
            )
            realized_loss_before += max(as_finite_float(attempt.get("realized_loss_delta_usdc"), 0.0), 0.0)
            orders_before += 1
            if attempt.get("close_round_after_attempt"):
                completed_rounds_before += 1

    payload = base_payload(
        "PASS_S9A_EXECUTE_WRAPPER_BINDING_PREVIEW"
        if not failures
        else "BLOCK_S9A_EXECUTE_WRAPPER_BINDING_PREVIEW"
    )
    payload.update(
        {
            "schema_version": "B_STRATEGY_CANARY_S9A_EXECUTE_WRAPPER_BINDING_PREVIEW_v1",
            "review_only_no_submit": True,
            "execute_wrapper_binding_preview_ready": not failures,
            "effectful_execute_wrapper_enabled_now": False,
            "ready_for_fresh_exact_approval": False,
            "ready_for_fresh_exact_approval_reason": (
                "S9A binds future per-attempt runtime execute calls to pre-submit caps and "
                "post-submit filled_qty evidence, but this preview intentionally does not "
                "execute the child runtime. Remote no-submit gates and an explicit fresh "
                "approval index are still required before any effectful path."
            ),
            "one_run_plan_sha256": plan_sha,
            "loop_state_machine": loop_state,
            "future_submit_plan": submit_plan,
            "failures": failures,
            "secret_values_read": False,
            "secret_values_printed": False,
            "raw_signature_output": False,
        }
    )
    print_json(payload)
    return 0 if not failures else 2


def s9c_approved_loop_execute(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if not args.execute_approved:
        payload = base_payload("BLOCK_S9C_EXECUTE_REQUIRES_FRESH_APPROVAL_AND_EXECUTE_APPROVED_EXIT_66")
        payload.update(
            {
                "ready_for_fresh_exact_approval": False,
                "full_one_run_loop_execute_ready": True,
                "orders_submitted": 0,
                "cancels_submitted": 0,
                "signing_performed": False,
                "block_reasons": [
                    "FRESH_EXACT_APPROVAL_REQUIRED",
                    "EXECUTE_APPROVED_FLAG_REQUIRED",
                    "S8V_S8O_S8Y_CONSUMED_APPROVALS_MUST_NOT_BE_REUSED",
                ],
            }
        )
        print_json(payload)
        return 66

    if args.reviewed_host != REVIEWED_HOST:
        failures.append("REVIEWED_HOST_MISMATCH")
    if args.rest_url != OFFICIAL_CLOB_REST_URL:
        failures.append("REST_URL_MUST_BE_OFFICIAL_CLOB")
    if args.approval_scope != SCOPE:
        failures.append("APPROVAL_SCOPE_MISMATCH")
    if args.order_primitive_name != ORDER_PRIMITIVE_NAME:
        failures.append("ORDER_PRIMITIVE_NAME_MISMATCH")
    if not hash64(args.exact_approval_sha256) or args.exact_approval_sha256 != args.expected_exact_approval_sha256:
        failures.append("EXACT_APPROVAL_SHA256_MISMATCH_OR_INVALID")
    if not hash64(args.order_primitive_source_sha256):
        failures.append("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED")
    if args.allow_online_tuning or args.allow_candidate_import:
        failures.append("FORBIDDEN_ONLINE_TUNING_OR_CANDIDATE_IMPORT")
    if not args.no_submit and args.exact_approval_sha256 == "a" * 64:
        failures.append("PLACEHOLDER_EXACT_APPROVAL_HASH_FORBIDDEN_FOR_EFFECTFUL_EXECUTE")
    if not args.no_submit and args.order_primitive_source_sha256 == "b" * 64:
        failures.append("PLACEHOLDER_ORDER_PRIMITIVE_SOURCE_HASH_FORBIDDEN_FOR_EFFECTFUL_EXECUTE")
    if not args.no_submit:
        failures.append("S9E_FINAL_S7W_RECOVERY_COLLATERAL_RECONCILIATION_NOT_BOUND_FOR_EFFECTFUL_EXECUTE")
        failures.append("BROAD_S8A_EFFECTFUL_RUN_REMAINS_BLOCKED_ZERO_ORDER")

    runtime = Path(args.runtime_bin)
    plan = Path(args.one_run_plan_json)
    if not runtime.exists():
        failures.append("RUNTIME_BIN_MISSING")
    if not plan.exists():
        failures.append("ONE_RUN_PLAN_JSON_MISSING")

    plan_sha = sha256_file(plan) if plan.exists() else None
    if plan_sha != args.expected_one_run_plan_sha256:
        failures.append("ONE_RUN_PLAN_SHA256_MISMATCH")

    plan_payload = load_json(plan) if plan.exists() else {}
    plan_failures, loop_state = full_loop_contract_plan_failures(plan_payload)
    failures.extend(plan_failures)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    child_results: list[dict[str, Any]] = []
    submit_results: list[dict[str, Any]] = []
    inventory: dict[str, dict[str, float]] = {}
    cost_by_condition: dict[str, float] = {}
    active_condition_id: str | None = None
    realized_session_loss_usdc = 0.0
    gross_quote_spend_usdc = 0.0
    completed_rounds = 0
    total_orders = 0
    total_cancels = 0
    signing_performed = False

    attempts = plan_payload.get("attempts") if isinstance(plan_payload, dict) else None
    if failures or not isinstance(attempts, list):
        attempts = []

    for idx, attempt in enumerate(attempts, start=1):
        if not isinstance(attempt, dict):
            failures.append(f"ATTEMPT_{idx}_NOT_OBJECT")
            break
        order = attempt.get("prepared_order")
        order_failures = prepared_order_failures(order)
        if order_failures:
            failures.extend(f"ATTEMPT_{idx}_{failure}" for failure in order_failures)
            break
        assert isinstance(order, dict)
        condition_id = order.get("condition_id")
        side = order.get("side")
        amount = order.get("amount") if isinstance(order.get("amount"), dict) else {}
        order_size = as_finite_float(amount.get("value"))
        limit_price = as_finite_float(order.get("limit_price"))
        pre_submit_failures: list[str] = []
        if completed_rounds >= 3:
            pre_submit_failures.append("MAX_ROUNDS_COMPLETED")
        if realized_session_loss_usdc + 5.0 > 15.0 + 1e-9:
            pre_submit_failures.append("PRE_ENTRY_LOSS_CAP")
        if active_condition_id is not None and active_condition_id != condition_id:
            pre_submit_failures.append("ACTIVE_MARKET_CAP")
        if total_orders >= 9:
            pre_submit_failures.append("TOTAL_ORDER_SUBMISSION_CAP")
        if gross_quote_spend_usdc + max(order_size, 0.0) * max(limit_price, 0.0) > 18.0 + 1e-9:
            pre_submit_failures.append("PROJECTED_GROSS_QUOTE_SPEND_CAP")
        if order.get("natural_controller_admission") is not True:
            pre_submit_failures.append("NOT_NATURAL_ADMISSION")
        if order.get("forced_complement") is not False:
            pre_submit_failures.append("FORCED_COMPLEMENT")
        if pre_submit_failures:
            failures.append(f"ATTEMPT_{idx}_PRE_SUBMIT_GATE_FAIL")
            submit_results.append(
                {
                    "attempt_index": idx,
                    "pre_submit_gate_passed": False,
                    "pre_submit_failures": pre_submit_failures,
                    "orders_submitted": 0,
                    "cancels_submitted": 0,
                }
            )
            break

        prepared_order_path = output_dir / f"S9C_ATTEMPT_{idx}_HASH_BOUND_PREPARED_ORDER.json"
        prepared_order_sha = write_json(prepared_order_path, order)
        child_cmd = [
            str(runtime),
            "--mode",
            "execute",
            "--prepared-order-json",
            str(prepared_order_path),
            "--expected-prepared-order-sha256",
            prepared_order_sha,
            *exact_args(args),
            "--execute-approved",
        ]
        if args.no_submit:
            child = {
                "label": f"s9c_execute_attempt_{idx}_dry_run_no_submit",
                "argv": child_cmd,
                "returncode": 0,
                "stdout_sha256": None,
                "stderr_sha256": None,
                "stdout_json": {
                    "status": "PASS_S9C_EXECUTE_CHILD_TEMPLATE_NO_SUBMIT",
                    "orders_submitted": 0,
                    "cancels_submitted": 0,
                    "signing_performed": False,
                },
                "stdout_parse_error": None,
            }
            actual_filled_qty = as_finite_float(attempt.get("actual_filled_qty_shares"))
            avg_fill_price = as_finite_float(attempt.get("avg_fill_price"))
            open_remainder_qty = 0.0 if attempt.get("no_open_order_remainder") is True else order_size - actual_filled_qty
            post_run_reconciliation_passed = bool(attempt.get("s7w_reconciliation_passed")) or not attempt.get(
                "close_round_after_attempt"
            )
            fill_source = "PLAN_FIXTURE_NO_SUBMIT"
        else:
            child = run_child(f"s9c_execute_attempt_{idx}", child_cmd)
            parsed = child.get("stdout_json") if isinstance(child.get("stdout_json"), dict) else {}
            execution_result = parsed.get("execution_result") if isinstance(parsed, dict) else {}
            if child["returncode"] != 0:
                failures.append(f"ATTEMPT_{idx}_RUNTIME_EXECUTE_FAILED_CLOSED")
                child_results.append(child)
                break
            total_orders += int(parsed.get("orders_submitted", 0) or 0)
            total_cancels += int(parsed.get("cancel_submissions", 0) or 0)
            signing_performed = signing_performed or bool(parsed.get("signing_performed"))
            actual_filled_qty = as_finite_float(execution_result.get("actual_filled_qty_shares"), 0.0)
            avg_fill_price = as_finite_float(execution_result.get("avg_fill_price"), 0.0)
            open_remainder_qty = as_finite_float(execution_result.get("open_order_remainder_qty_shares"), 0.0)
            post_run_reconciliation_passed = bool(execution_result.get("post_run_reconciliation_passed"))
            fill_source = "S8Y_RUNTIME_EXECUTION_RESULT"

        if args.no_submit:
            total_orders += 0
            total_cancels += 0
        child_results.append(child)
        if actual_filled_qty < 0.0 or actual_filled_qty > order_size + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTUAL_FILLED_QTY_OUT_OF_RANGE")
        if open_remainder_qty > 1e-9:
            failures.append(f"ATTEMPT_{idx}_OPEN_ORDER_REMAINDER_NOT_CLEARED")
        condition_inventory = inventory.setdefault(str(condition_id), {"YES": 0.0, "NO": 0.0})
        cost_by_condition.setdefault(str(condition_id), 0.0)
        if side in ("YES", "NO"):
            condition_inventory[str(side)] += max(actual_filled_qty, 0.0)
        else:
            failures.append(f"ATTEMPT_{idx}_SIDE_UNSUPPORTED")
        fill_cost = max(actual_filled_qty, 0.0) * max(avg_fill_price, 0.0)
        gross_quote_spend_usdc += fill_cost
        cost_by_condition[str(condition_id)] += fill_cost
        residual_qty = abs(condition_inventory["YES"] - condition_inventory["NO"])
        active_round_risk_at_work_usdc = residual_qty
        active_capital_lock_usdc = cost_by_condition[str(condition_id)]
        if active_round_risk_at_work_usdc > 5.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTIVE_ROUND_RISK_CAP_EXCEEDED")
        if active_capital_lock_usdc > 6.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_ACTIVE_CAPITAL_LOCK_CAP_EXCEEDED")
        if gross_quote_spend_usdc > 18.0 + 1e-9:
            failures.append(f"ATTEMPT_{idx}_GROSS_QUOTE_SPEND_CAP_EXCEEDED")

        close_round = bool(attempt.get("close_round_after_attempt"))
        if close_round:
            if residual_qty > 1e-9:
                failures.append(f"ATTEMPT_{idx}_CLOSE_ROUND_WITH_RESIDUAL_EXPOSURE")
            if not post_run_reconciliation_passed:
                failures.append(f"ATTEMPT_{idx}_POST_RUN_RECONCILIATION_NOT_PASSED")
            completed_rounds += 1
            active_condition_id = None
        else:
            active_condition_id = str(condition_id)

        submit_results.append(
            {
                "attempt_index": idx,
                "prepared_order_path": str(prepared_order_path),
                "prepared_order_sha256": prepared_order_sha,
                "pre_submit_gate_passed": True,
                "child_returncode": child["returncode"],
                "child_status": child_status(child),
                "fill_source": fill_source,
                "actual_filled_qty_shares": actual_filled_qty,
                "avg_fill_price": avg_fill_price,
                "open_remainder_qty_shares": open_remainder_qty,
                "yes_qty": condition_inventory["YES"],
                "no_qty": condition_inventory["NO"],
                "residual_qty": residual_qty,
                "active_round_risk_at_work_usdc": active_round_risk_at_work_usdc,
                "active_capital_lock_usdc": active_capital_lock_usdc,
                "gross_quote_spend_usdc": gross_quote_spend_usdc,
                "post_run_reconciliation_passed": post_run_reconciliation_passed,
                "close_round_after_attempt": close_round,
            }
        )
        if failures:
            break

    child_summaries = [
        {
            "label": child["label"],
            "returncode": child["returncode"],
            "status": child_status(child),
            "orders_submitted": child_orders(child),
            "signing_performed": child_signing(child),
            "stdout_sha256": child["stdout_sha256"],
            "stderr_sha256": child["stderr_sha256"],
            "stdout_parse_error": child["stdout_parse_error"],
        }
        for child in child_results
    ]

    status = (
        "PASS_S9C_APPROVED_LOOP_EXECUTE_DRY_RUN_NO_SUBMIT"
        if args.no_submit and not failures
        else "PASS_S9C_APPROVED_LOOP_EXECUTED_AND_RECONCILED"
        if not failures
        else "BLOCK_S9C_APPROVED_LOOP_EXECUTE_FAIL_CLOSED"
    )
    payload = base_payload(status)
    payload.update(
        {
            "schema_version": "B_STRATEGY_CANARY_S9C_APPROVED_LOOP_EXECUTE_v1",
            "review_only_no_submit": args.no_submit,
            "effectful_execution_permitted": False,
            "execution_performed": not args.no_submit and not failures,
            "broad_effectful_execute_blocked_until_s7w_final_reconciliation": not args.no_submit,
            "orders_submitted": total_orders,
            "cancels_submitted": total_cancels,
            "signing_performed": signing_performed,
            "one_run_plan_sha256": plan_sha,
            "loop_state_machine": loop_state,
            "submit_results": submit_results,
            "child_executes": child_summaries,
            "final_inventory_by_condition": inventory,
            "completed_rounds": completed_rounds,
            "gross_quote_spend_usdc": gross_quote_spend_usdc,
            "realized_session_loss_usdc": realized_session_loss_usdc,
            "failures": failures,
            "secret_values_read": False,
            "secret_values_printed": False,
            "raw_signature_output": False,
        }
    )
    print_json(payload)
    return 0 if not failures else 2


def derive_order_status_fill_evidence(
    template: Any,
    prepared_order: dict[str, Any],
    prepared_order_sha256: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    evidence = dict(template) if isinstance(template, dict) else {}
    amount = prepared_order.get("amount") if isinstance(prepared_order.get("amount"), dict) else {}
    evidence.update(
        {
            "prepared_order_sha256": prepared_order_sha256,
            "exact_approval_sha256": args.exact_approval_sha256,
            "expected_exact_approval_sha256": args.expected_exact_approval_sha256,
            "approval_scope_matches_s8a": args.approval_scope == SCOPE,
            "condition_id": prepared_order.get("condition_id"),
            "token_id": prepared_order.get("token_id"),
            "side": prepared_order.get("side"),
            "action": prepared_order.get("action"),
            "condition_token_side_action_match_prepared_order": True,
            "submitted_size_shares": float(amount.get("value", 0.0)) if amount.get("unit") == "SHARES" else 0.0,
            "filled_qty_from_exchange_or_order_status": True,
            "submitted_size_counts_as_inventory": False,
            "partial_fill_threshold_used": False,
            "forced_complement_after_fill": False,
            "prints_secret_or_raw_signature": False,
            "uses_shared_ingress_or_shared_ws": False,
            "uses_c_artifacts": False,
            "funding_live_latest_or_deploy_requested": False,
            "effectful_execution_requested_in_review": False,
        }
    )
    if evidence.get("avg_fill_price") is None and prepared_order.get("limit_price") is not None:
        evidence["avg_fill_price"] = prepared_order["limit_price"]
    return evidence


def no_submit_orchestration_preview(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if args.reviewed_host != REVIEWED_HOST:
        failures.append("REVIEWED_HOST_MISMATCH")
    if args.rest_url != OFFICIAL_CLOB_REST_URL:
        failures.append("REST_URL_MUST_BE_OFFICIAL_CLOB")
    if args.approval_scope != SCOPE:
        failures.append("APPROVAL_SCOPE_MISMATCH")
    if args.order_primitive_name != ORDER_PRIMITIVE_NAME:
        failures.append("ORDER_PRIMITIVE_NAME_MISMATCH")
    if not hash64(args.exact_approval_sha256) or args.exact_approval_sha256 != args.expected_exact_approval_sha256:
        failures.append("EXACT_APPROVAL_SHA256_MISMATCH_OR_INVALID")
    if not hash64(args.order_primitive_source_sha256):
        failures.append("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX")
    if not args.no_submit:
        failures.append("NO_SUBMIT_REQUIRED")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_SHARED_OR_C_DEPENDENCY_REQUESTED")
    if args.allow_online_tuning or args.allow_candidate_import:
        failures.append("FORBIDDEN_ONLINE_TUNING_OR_CANDIDATE_IMPORT")

    runtime = Path(args.runtime_bin)
    if not runtime.exists():
        failures.append("RUNTIME_BIN_MISSING")
    scout = Path(args.scout_snapshot_json)
    plan = Path(args.one_run_plan_json)
    order_status_fill_evidence_template = Path(args.order_status_fill_evidence_json)
    if not scout.exists():
        failures.append("SCOUT_SNAPSHOT_JSON_MISSING")
    if not plan.exists():
        failures.append("ONE_RUN_PLAN_JSON_MISSING")
    if not order_status_fill_evidence_template.exists():
        failures.append("ORDER_STATUS_FILL_EVIDENCE_JSON_MISSING")

    scout_sha = sha256_file(scout) if scout.exists() else None
    plan_sha = sha256_file(plan) if plan.exists() else None
    order_status_fill_evidence_template_sha = (
        sha256_file(order_status_fill_evidence_template)
        if order_status_fill_evidence_template.exists()
        else None
    )
    if scout_sha != args.expected_scout_snapshot_sha256:
        failures.append("SCOUT_SNAPSHOT_SHA256_MISMATCH")
    if plan_sha != args.expected_one_run_plan_sha256:
        failures.append("ONE_RUN_PLAN_SHA256_MISMATCH")
    if (
        args.expected_order_status_fill_evidence_sha256
        and order_status_fill_evidence_template_sha != args.expected_order_status_fill_evidence_sha256
    ):
        failures.append("ORDER_STATUS_FILL_EVIDENCE_TEMPLATE_SHA256_MISMATCH")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    children: list[dict[str, Any]] = []
    if not failures:
        children.append(
            run_child(
                "preview_without_approval",
                [str(runtime), "--mode", "preview-no-approval"],
            )
        )
        assert_no_submit_child("PREVIEW_WITHOUT_APPROVAL", children[-1], 66, failures)

        children.append(
            run_child(
                "no_order_auth_preview",
                [str(runtime), "--mode", "no-order-auth-preview", "--reviewed-host", args.reviewed_host],
            )
        )
        assert_no_submit_child("NO_ORDER_AUTH_PREVIEW", children[-1], 0, failures)
        no_order_payload = children[-1].get("stdout_json") or {}
        if no_order_payload.get("secret_values_read") is not False:
            failures.append("NO_ORDER_AUTH_PREVIEW_READ_SECRET")

        children.append(
            run_child(
                "live_scout_to_order_preview",
                [
                    str(runtime),
                    "--mode",
                    "live-scout-to-order-preview",
                    "--scout-snapshot-json",
                    str(scout),
                    "--expected-scout-snapshot-sha256",
                    args.expected_scout_snapshot_sha256,
                    *exact_args(args),
                    "--no-submit",
                ],
            )
        )
        assert_no_submit_child("LIVE_SCOUT_TO_ORDER_PREVIEW", children[-1], 0, failures)
        live_payload = children[-1].get("stdout_json") or {}
        prepared_order = live_payload.get("prepared_order")
        failures.extend(prepared_order_failures(prepared_order))

        prepared_order_path = output_dir / "S8R_PREPARED_ORDER_FROM_LIVE_SCOUT_PREVIEW.json"
        prepared_order_sha = write_json(prepared_order_path, prepared_order)

        children.append(
            run_child(
                "exact_order_path_preview_from_live_scout_prepared_order",
                [
                    str(runtime),
                    "--mode",
                    "exact-approved-order-path-preview",
                    "--prepared-order-json",
                    str(prepared_order_path),
                    "--expected-prepared-order-sha256",
                    prepared_order_sha,
                    *exact_args(args),
                    "--no-submit",
                ],
            )
        )
        assert_no_submit_child("EXACT_ORDER_PATH_PREVIEW", children[-1], 0, failures)

        order_status_fill_evidence = derive_order_status_fill_evidence(
            load_json(order_status_fill_evidence_template),
            prepared_order,
            prepared_order_sha,
            args,
        )
        order_status_fill_evidence_path = (
            output_dir / "S8T_ORDER_STATUS_FILL_EVIDENCE_FROM_PREPARED_ORDER.json"
        )
        order_status_fill_evidence_sha = write_json(
            order_status_fill_evidence_path, order_status_fill_evidence
        )

        children.append(
            run_child(
                "order_status_fill_evidence_preview",
                [
                    str(runtime),
                    "--mode",
                    "order-status-fill-evidence-preview",
                    "--prepared-order-json",
                    str(prepared_order_path),
                    "--expected-prepared-order-sha256",
                    prepared_order_sha,
                    "--order-status-fill-evidence-json",
                    str(order_status_fill_evidence_path),
                    "--expected-order-status-fill-evidence-sha256",
                    order_status_fill_evidence_sha,
                    *exact_args(args),
                    "--no-submit",
                ],
            )
        )
        assert_no_submit_child("ORDER_STATUS_FILL_EVIDENCE_PREVIEW", children[-1], 0, failures)

        children.append(
            run_child(
                "one_run_driver_preview",
                [
                    str(runtime),
                    "--mode",
                    "one-run-driver-preview",
                    "--one-run-plan-json",
                    str(plan),
                    "--expected-one-run-plan-sha256",
                    args.expected_one_run_plan_sha256,
                    *exact_args(args),
                    "--no-submit",
                ],
            )
        )
        assert_no_submit_child("ONE_RUN_DRIVER_PREVIEW", children[-1], 0, failures)
        one_run_payload = children[-1].get("stdout_json") or {}
        failures.extend(one_run_summary_failures(one_run_payload.get("one_run_summary")))

        children.append(
            run_child(
                "execute_without_execute_approved",
                [
                    str(runtime),
                    "--mode",
                    "execute",
                    "--prepared-order-json",
                    str(prepared_order_path),
                    "--expected-prepared-order-sha256",
                    prepared_order_sha,
                    *exact_args(args),
                ],
            )
        )
        if children[-1]["returncode"] == 0:
            failures.append("EXECUTE_WITHOUT_EXECUTE_APPROVED_DID_NOT_FAIL_CLOSED")
        assert_no_submit_child("EXECUTE_WITHOUT_EXECUTE_APPROVED", children[-1], None, failures)
    else:
        prepared_order_path = None
        prepared_order_sha = None
        order_status_fill_evidence_path = None
        order_status_fill_evidence_sha = None

    child_summaries = [
        {
            "label": child["label"],
            "returncode": child["returncode"],
            "status": child_status(child),
            "orders_submitted": child_orders(child),
            "signing_performed": child_signing(child),
            "stdout_sha256": child["stdout_sha256"],
            "stderr_sha256": child["stderr_sha256"],
            "stdout_parse_error": child["stdout_parse_error"],
        }
        for child in children
    ]

    payload = base_payload(
        "PASS_S8T_ONE_RUN_ORCHESTRATOR_WITH_FILL_EVIDENCE_NO_SUBMIT_PREVIEW"
        if not failures
        else "BLOCK_S8T_ONE_RUN_ORCHESTRATOR_WITH_FILL_EVIDENCE_FAIL_CLOSED"
    )
    payload.update(
        {
            "review_only_no_submit": True,
            "ready_for_fresh_exact_approval": False,
            "ready_for_fresh_exact_approval_reason": (
                "S8Y makes the single prepared-order primitive closed-loop capable, "
                "but this orchestrator remains no-submit only. Fresh broad S8A approval "
                "still requires full live one-run loop execution binding."
            ),
            "scout_snapshot_sha256": scout_sha,
            "one_run_plan_sha256": plan_sha,
            "order_status_fill_evidence_template_sha256": order_status_fill_evidence_template_sha,
            "prepared_order_from_live_scout_path": str(prepared_order_path) if prepared_order_path else None,
            "prepared_order_from_live_scout_sha256": prepared_order_sha,
            "order_status_fill_evidence_from_prepared_order_path": (
                str(order_status_fill_evidence_path) if order_status_fill_evidence_path else None
            ),
            "order_status_fill_evidence_from_prepared_order_sha256": order_status_fill_evidence_sha,
            "child_previews": child_summaries,
            "failures": failures,
            "secret_values_read": False,
            "secret_values_printed": False,
            "raw_signature_output": False,
        }
    )
    print_json(payload)
    return 0 if not failures else 2


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(
            "review-only",
            "preview-no-approval",
            "no-submit-orchestration-preview",
            "full-loop-contract-preview",
            "s9a-execute-wrapper-preview",
            "execute",
        ),
        required=True,
    )
    parser.add_argument("--runtime-bin", default="target/debug/b_strategy_canary_s8a_native_effectful_runtime")
    parser.add_argument("--reviewed-host", default=REVIEWED_HOST)
    parser.add_argument("--rest-url", default=OFFICIAL_CLOB_REST_URL)
    parser.add_argument("--scout-snapshot-json", default="scripts/fixtures/s8a_live_scout_to_order_preview_snapshot.json")
    parser.add_argument("--expected-scout-snapshot-sha256", required=False)
    parser.add_argument("--one-run-plan-json", default="scripts/fixtures/s8a_one_run_driver_preview_plan.json")
    parser.add_argument("--expected-one-run-plan-sha256", required=False)
    parser.add_argument(
        "--order-status-fill-evidence-json",
        default="scripts/fixtures/s8a_order_status_fill_evidence_preview.json",
    )
    parser.add_argument("--expected-order-status-fill-evidence-sha256", required=False)
    parser.add_argument("--output-dir", default=".tmp_xuan/s8r_one_run_orchestrator_preview")
    parser.add_argument("--exact-approval-sha256", default="a" * 64)
    parser.add_argument("--expected-exact-approval-sha256", default="a" * 64)
    parser.add_argument("--approval-scope", default=SCOPE)
    parser.add_argument("--order-primitive-name", default=ORDER_PRIMITIVE_NAME)
    parser.add_argument("--order-primitive-source-sha256", default="b" * 64)
    parser.add_argument("--no-submit", action="store_true")
    parser.add_argument("--execute-approved", action="store_true")
    parser.add_argument("--print-secret", action="store_true")
    parser.add_argument("--print-raw-signature", action="store_true")
    parser.add_argument("--use-shared-ingress", action="store_true")
    parser.add_argument("--use-c-artifacts", action="store_true")
    parser.add_argument("--allow-online-tuning", action="store_true")
    parser.add_argument("--allow-candidate-import", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.mode == "review-only":
        print_json(base_payload("PASS_S8R_ONE_RUN_ORCHESTRATOR_REVIEW_ONLY_SOURCE_SHAPE"))
        return 0
    if args.mode == "preview-no-approval":
        print_json(base_payload("BLOCK_S8R_PREVIEW_WITHOUT_EXACT_APPROVAL_EXIT_66"))
        return 66
    if args.mode == "no-submit-orchestration-preview":
        if not args.expected_scout_snapshot_sha256:
            args.expected_scout_snapshot_sha256 = sha256_file(Path(args.scout_snapshot_json))
        if not args.expected_one_run_plan_sha256:
            args.expected_one_run_plan_sha256 = sha256_file(Path(args.one_run_plan_json))
        if not args.expected_order_status_fill_evidence_sha256:
            args.expected_order_status_fill_evidence_sha256 = sha256_file(
                Path(args.order_status_fill_evidence_json)
            )
        return no_submit_orchestration_preview(args)
    if args.mode == "full-loop-contract-preview":
        if not args.expected_one_run_plan_sha256:
            args.expected_one_run_plan_sha256 = sha256_file(Path(args.one_run_plan_json))
        return full_loop_contract_preview(args)
    if args.mode == "s9a-execute-wrapper-preview":
        if not args.expected_one_run_plan_sha256:
            args.expected_one_run_plan_sha256 = sha256_file(Path(args.one_run_plan_json))
        return s9a_execute_wrapper_preview(args)
    if not args.expected_one_run_plan_sha256:
        args.expected_one_run_plan_sha256 = sha256_file(Path(args.one_run_plan_json))
    return s9c_approved_loop_execute(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
