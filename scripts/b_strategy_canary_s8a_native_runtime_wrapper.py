#!/usr/bin/env python3
"""Review-only S8A native runtime wrapper boundary.

This wrapper is source-controlled so future S8A approval packets do not depend
on transient .tmp_xuan scripts. It intentionally performs no network IO, reads
no secrets, signs no payload, submits no orders, cancels nothing, and executes
no merge/redeem/funding/live/latest path.

Modes:
- review-only: emit the hash-bound wrapper contract and exit 0.
- preview-no-approval: prove missing approval fails closed with exit 66.
- no-order-auth-preview: prove the reviewed auth path can be previewed without
  reading or printing secrets/signatures; exits 0.
- exact-approved-order-path-preview: lint the exact-approved path shape without
  submitting; exits 0 only with matching approval hash and explicit no-submit.
- execute: intentionally unavailable in this review-only wrapper; exits 66.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


LOSS_TRIGGER_PACKET_SHA256 = "a602729f6697c4ccc82a6cf7b3f1cefd049f8fda9b6884da56d5bb5a4e399fdf"
PARTIAL_FILL_ADDENDUM_SHA256 = "1545a9268e6fa6bacb164ebac5c2878193863fb59309b3a38eeab1534847e00d"
THREE_ROUND_CAP_PACKET_SHA256 = "1fb20485b5e37931c7d8f087464edc9fb463aed3c41dcde90014ad5abf63852c"
S7W_RUN_RESULT_LINTER_SHA256 = "b2490770fe7cfaea6671f660eb4349a78ab43695b97811a8c1b8bf393662dea2"
S8A_NATIVE_ADAPTER_BINDING_PACKET_SHA256 = "16f3dbfec5f38e5d8bc230802884cb6ddee0df02c646a473dd1f2d5ce89b9a38"
S8B_SCOUT_TO_NATIVE_ADAPTER_PACKET_SHA256 = "4501c1a8b808c6517f39fd826972cc022d916800b5c83e5bd5332a3cd656348d"

SCOPE = "B_STRATEGY_CANARY_S8A_MICRO_SHORT_CYCLE_ONE_RUN_MAX_THREE_ROUNDS_BTC5M_SIZE5_15USDC_LOSS_CAP_NATIVE_RUNTIME"
REVIEWED_HOST = "ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com"
PREPARED_ORDER_SOURCE = "native_s8a_adapter"
ORDER_PRIMITIVE_NAME = "clob_v2.build_signed_limit_order_v2/post_order_v2"
S8A_LIMIT_ENTRY_SIZE_SHARES = 5.0
S8A_SEED_PX_LO = 0.05
S8A_SEED_PX_HI = 0.80
S8A_MARKET_BUY_MIN_NOTIONAL_USDC = 1.0
S8A_MARKET_SELL_MIN_SIZE_SHARES = 5.0


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def hash64(value: str | None) -> bool:
    if value is None or len(value) != 64:
        return False
    return all(ch in "0123456789abcdefABCDEF" for ch in value)


def load_json_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("prepared order payload must be a JSON object")
    return data


def optional_float(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def prepared_order_shape_failures(order: dict) -> list[str]:
    failures: list[str] = []
    if order.get("source") != PREPARED_ORDER_SOURCE:
        failures.append("PREPARED_ORDER_SOURCE_NOT_NATIVE_S8A_ADAPTER")
    if not str(order.get("condition_id", "")).strip():
        failures.append("PREPARED_ORDER_MISSING_CONDITION_ID")
    if not str(order.get("token_id", "")).strip():
        failures.append("PREPARED_ORDER_MISSING_TOKEN_ID")
    if order.get("side") not in ("YES", "NO"):
        failures.append("PREPARED_ORDER_SIDE_NOT_YES_OR_NO")
    if order.get("action") not in ("BUY", "SELL"):
        failures.append("PREPARED_ORDER_ACTION_NOT_BUY_OR_SELL")
    if order.get("order_type") not in ("GTC", "FAK"):
        failures.append("PREPARED_ORDER_TYPE_NOT_GTC_OR_FAK")
    if order.get("execution_permitted") is not False:
        failures.append("PREPARED_ORDER_EXECUTION_MUST_BE_FALSE_IN_PREVIEW")
    if order.get("natural_controller_admission") is not True:
        failures.append("PREPARED_ORDER_MISSING_NATURAL_CONTROLLER_ADMISSION")
    if order.get("forced_complement") is not False:
        failures.append("PREPARED_ORDER_FORCED_COMPLEMENT_NOT_FALSE")
    if order.get("source_guard_500_passed") is not True:
        failures.append("PREPARED_ORDER_SOURCE_GUARD_500_NOT_PASSED")

    amount = order.get("amount")
    if not isinstance(amount, dict):
        failures.append("PREPARED_ORDER_AMOUNT_NOT_OBJECT")
        amount = {}
    amount_unit = amount.get("unit")
    amount_value = optional_float(amount.get("value"))
    action = order.get("action")
    order_type = order.get("order_type")

    if order_type == "GTC":
        if action != "BUY":
            failures.append("S8A_LIMIT_ENTRY_MUST_BE_BUY")
        if amount_unit != "SHARES":
            failures.append("S8A_LIMIT_ENTRY_AMOUNT_UNIT_MUST_BE_SHARES")
        if amount_value is None or abs(amount_value - S8A_LIMIT_ENTRY_SIZE_SHARES) > 1e-9:
            failures.append("S8A_LIMIT_ENTRY_SIZE_MUST_BE_5_SHARES")
        limit_price = optional_float(order.get("limit_price"))
        if limit_price is None:
            failures.append("S8A_LIMIT_ENTRY_PRICE_MISSING_OR_INVALID")
        elif not (S8A_SEED_PX_LO <= limit_price <= S8A_SEED_PX_HI):
            failures.append("S8A_LIMIT_ENTRY_PRICE_OUTSIDE_SEED_BAND")
    elif order_type == "FAK" and action == "BUY":
        if amount_unit != "USDC_NOTIONAL":
            failures.append("S8A_MARKET_BUY_AMOUNT_UNIT_MUST_BE_USDC_NOTIONAL")
        if amount_value is None or amount_value + 1e-9 < S8A_MARKET_BUY_MIN_NOTIONAL_USDC:
            failures.append("S8A_MARKET_BUY_NOTIONAL_BELOW_1_USDC")
    elif order_type == "FAK" and action == "SELL":
        if amount_unit != "SHARES":
            failures.append("S8A_MARKET_SELL_AMOUNT_UNIT_MUST_BE_SHARES")
        if amount_value is None or amount_value + 1e-9 < S8A_MARKET_SELL_MIN_SIZE_SHARES:
            failures.append("S8A_MARKET_SELL_SIZE_BELOW_5_SHARES")
    return failures


def contract_payload(status: str) -> dict:
    return {
        "schema_version": "B_STRATEGY_CANARY_S8A_NATIVE_RUNTIME_WRAPPER_v1",
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": SCOPE,
        "effectful_execution_permitted": False,
        "execution_performed": False,
        "orders_submitted": 0,
        "cancels_submitted": 0,
        "signing_performed": False,
        "merge_or_redeem_performed": False,
        "funding_live_latest_deploy_performed": False,
        "hard_bound_hashes": {
            "loss_trigger_redesign_packet_sha256": LOSS_TRIGGER_PACKET_SHA256,
            "partial_fill_addendum_sha256": PARTIAL_FILL_ADDENDUM_SHA256,
            "three_round_cap_packet_sha256": THREE_ROUND_CAP_PACKET_SHA256,
            "s7w_run_result_linter_sha256": S7W_RUN_RESULT_LINTER_SHA256,
            "s8a_native_adapter_binding_packet_sha256": S8A_NATIVE_ADAPTER_BINDING_PACKET_SHA256,
            "s8b_scout_to_native_adapter_packet_sha256": S8B_SCOUT_TO_NATIVE_ADAPTER_PACKET_SHA256,
        },
        "strategy_scope": {
            "asset": "BTC",
            "market_family": "btc-5min",
            "fixed_controller_policy": "cool5_imb1.25_source_guard_500",
            "source_guard_500_required": True,
            "online_tuning_allowed": False,
            "strategy_discovery_allowed": False,
            "candidate_import_allowed": False,
        },
        "source_binding": {
            "input": "trade_enhanced_b_owned_direct_public_ws_scout_snapshot",
            "runtime_opens_own_ws": False,
            "max_b_owned_direct_public_ws_connections": 1,
            "uses_shared_ingress_or_shared_ws": False,
            "uses_c_artifacts": False,
            "scout_to_controller_adapter": "review_s8a_scout_snapshot_for_limit_entry",
        },
        "order_adapter_binding": {
            "native_adapter_required": True,
            "limit_entry_order_type": "GTC",
            "limit_entry_action": "BUY",
            "limit_entry_amount_unit": "shares",
            "limit_entry_size_shares": 5.0,
            "market_buy_amount_unit": "USDC_NOTIONAL",
            "market_buy_min_notional_usdc": 1.0,
            "market_sell_amount_unit": "SHARES",
            "market_sell_min_size_shares": 5.0,
            "sub_min_residual_market_sell_blocked": True,
        },
        "runtime_caps": {
            "max_round_count": 3,
            "session_hard_loss_cap_usdc": 15.0,
            "per_round_theoretical_max_loss_usdc": 5.0,
            "target_runtime_ms": 14_400_000,
            "max_active_market_count": 1,
            "max_active_capital_lock_usdc": 6.0,
            "max_cumulative_gross_quote_spend_usdc": 18.0,
            "max_initial_buy_submissions": 6,
            "max_emergency_exit_or_hedge_submissions": 3,
            "max_total_order_submissions": 9,
            "max_cancel_count": 6,
            "max_recovery_tx_count": 3,
        },
        "inventory_semantics": {
            "uses_actual_filled_qty_inventory": True,
            "submitted_size_counts_as_inventory": False,
            "partial_fill_threshold_enabled": False,
            "forced_complement_after_partial_fill": False,
            "forced_complement_without_natural_signal": False,
            "realized_loss_only_after_close_or_recovery": True,
        },
        "approval_guards": {
            "fresh_exact_approval_required": True,
            "preview_without_approval_exit_code": 66,
            "no_order_auth_preview_supported": True,
            "no_order_auth_preview_reads_secret": False,
            "no_order_auth_preview_prints_secret_or_raw_signature": False,
            "exact_order_path_requires_approval_hash": True,
            "exact_order_path_rejects_mismatched_approval": True,
            "exact_order_path_requires_native_prepared_order_json": True,
            "exact_order_path_requires_order_primitive_source_hash": True,
            "exact_order_path_order_primitive_name": ORDER_PRIMITIVE_NAME,
            "execute_mode_available_in_review_wrapper": False,
        },
        "s7w_reconciliation": {
            "required": True,
            "requires_exact_approved_receipt_tier": True,
            "rejects_review_only_fixture_receipt": True,
            "requires_no_open_order_remainder": True,
            "requires_residual_exposure_zero": True,
        },
        "forbidden": {
            "secret_print_copy_hash": True,
            "raw_signature_output": True,
            "shared_ingress_or_shared_ws": True,
            "c_artifacts": True,
            "funding_live_latest_deploy": True,
            "candidate_import": True,
        },
    }


def preview_no_approval() -> int:
    print(json.dumps(contract_payload("BLOCK_PREVIEW_WITHOUT_EXACT_APPROVAL_EXIT_66"), indent=2, sort_keys=True))
    return 66


def no_order_auth_preview(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if args.reviewed_host != REVIEWED_HOST:
        failures.append("REVIEWED_HOST_MISMATCH")
    if args.secret_path:
        failures.append("SECRET_PATH_MUST_NOT_BE_PASSED_TO_NO_ORDER_AUTH_PREVIEW")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_DEPENDENCY_REQUESTED")
    payload = contract_payload(
        "PASS_NO_ORDER_AUTH_PREVIEW_REVIEWED_AUTH_PATH_NO_SECRET_OUTPUT"
        if not failures
        else "BLOCK_NO_ORDER_AUTH_PREVIEW_FAIL_CLOSED"
    )
    payload["no_order_auth_preview"] = {
        "reviewed_host": args.reviewed_host,
        "reviewed_auth_path": True,
        "secret_values_read": False,
        "secret_values_printed": False,
        "raw_signature_output": False,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 2


def exact_order_path_preview(args: argparse.Namespace) -> int:
    failures: list[str] = []
    if not hash64(args.exact_approval_sha256):
        failures.append("EXACT_APPROVAL_SHA256_NOT_64HEX")
    if args.expected_exact_approval_sha256 != args.exact_approval_sha256:
        failures.append("EXACT_APPROVAL_SHA256_MISMATCH")
    if args.approval_scope != SCOPE:
        failures.append("APPROVAL_SCOPE_MISMATCH")
    if not args.no_submit:
        failures.append("NO_SUBMIT_REQUIRED_IN_PREVIEW")
    if args.order_primitive_name != ORDER_PRIMITIVE_NAME:
        failures.append("ORDER_PRIMITIVE_NAME_MISMATCH")
    if not hash64(args.order_primitive_source_sha256):
        failures.append("ORDER_PRIMITIVE_SOURCE_SHA256_NOT_64HEX")
    if args.print_secret or args.print_raw_signature:
        failures.append("SECRET_OR_RAW_SIGNATURE_OUTPUT_REQUESTED")
    if args.use_shared_ingress or args.use_c_artifacts:
        failures.append("FORBIDDEN_DEPENDENCY_REQUESTED")
    prepared_order: dict | None = None
    prepared_order_sha256: str | None = None
    if args.prepared_order_json is None:
        failures.append("PREPARED_ORDER_JSON_REQUIRED")
    else:
        try:
            prepared_order = load_json_file(args.prepared_order_json)
            prepared_order_sha256 = sha256_file(args.prepared_order_json)
            if args.expected_prepared_order_sha256 and (
                prepared_order_sha256 != args.expected_prepared_order_sha256
            ):
                failures.append("PREPARED_ORDER_SHA256_MISMATCH")
            failures.extend(prepared_order_shape_failures(prepared_order))
        except (OSError, ValueError, json.JSONDecodeError):
            failures.append("PREPARED_ORDER_JSON_UNREADABLE_OR_INVALID")
    payload = contract_payload(
        "PASS_EXACT_APPROVED_ORDER_PATH_PREVIEW_NO_SUBMIT"
        if not failures
        else "BLOCK_EXACT_APPROVED_ORDER_PATH_PREVIEW_FAIL_CLOSED"
    )
    payload["exact_order_path_preview"] = {
        "approval_scope": args.approval_scope,
        "exact_approval_hash_present": hash64(args.exact_approval_sha256),
        "approval_hash_matches_expected": args.expected_exact_approval_sha256
        == args.exact_approval_sha256,
        "no_submit": args.no_submit,
        "prepared_order_sha256": prepared_order_sha256,
        "prepared_order_shape_valid": prepared_order is not None
        and not prepared_order_shape_failures(prepared_order),
        "prepared_order": prepared_order,
        "order_primitive_name": args.order_primitive_name,
        "order_primitive_source_sha256": args.order_primitive_source_sha256,
        "order_primitive_source_hash_valid": hash64(args.order_primitive_source_sha256),
        "orders_submitted": 0,
        "signing_performed": False,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 2


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(
            "review-only",
            "preview-no-approval",
            "no-order-auth-preview",
            "exact-approved-order-path-preview",
            "execute",
        ),
        required=True,
    )
    parser.add_argument("--reviewed-host", default=REVIEWED_HOST)
    parser.add_argument("--secret-path")
    parser.add_argument("--exact-approval-sha256")
    parser.add_argument("--expected-exact-approval-sha256")
    parser.add_argument("--approval-scope", default=SCOPE)
    parser.add_argument("--prepared-order-json", type=Path)
    parser.add_argument("--expected-prepared-order-sha256")
    parser.add_argument("--order-primitive-name")
    parser.add_argument("--order-primitive-source-sha256")
    parser.add_argument("--no-submit", action="store_true")
    parser.add_argument("--print-secret", action="store_true")
    parser.add_argument("--print-raw-signature", action="store_true")
    parser.add_argument("--use-shared-ingress", action="store_true")
    parser.add_argument("--use-c-artifacts", action="store_true")
    parser.add_argument("--self-sha256-path", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_sha256_path is not None:
        payload = contract_payload("PASS_SELF_HASH_REVIEW")
        payload["self_sha256"] = sha256_file(args.self_sha256_path)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if args.mode == "review-only":
        print(json.dumps(contract_payload("PASS_REVIEW_ONLY_NATIVE_RUNTIME_WRAPPER_SHAPE"), indent=2, sort_keys=True))
        return 0
    if args.mode == "preview-no-approval":
        return preview_no_approval()
    if args.mode == "no-order-auth-preview":
        return no_order_auth_preview(args)
    if args.mode == "exact-approved-order-path-preview":
        return exact_order_path_preview(args)
    print(json.dumps(contract_payload("BLOCK_EXECUTE_MODE_UNAVAILABLE_IN_REVIEW_ONLY_WRAPPER_EXIT_66"), indent=2, sort_keys=True))
    return 66


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
