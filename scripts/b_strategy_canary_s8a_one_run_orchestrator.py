#!/usr/bin/env python3
"""S8R review-only one-run orchestrator for the native S8A runtime.

This wrapper binds the live-scout preview, native prepared-order primitive,
actual-filled-qty ledger contract, S8A session caps, and S7W reconciliation
requirements into one reviewable no-submit flow.

It intentionally performs no network IO of its own, reads no secrets, signs no
payload, submits no orders, cancels nothing, and performs no recovery action.
The only subprocess it may invoke is the source-controlled native runtime, and
only in no-submit or fail-closed preview modes unless a future exact-approved
execution path is explicitly implemented and freshly authorized.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCOPE = (
    "B_STRATEGY_CANARY_S8A_MICRO_SHORT_CYCLE_ONE_RUN_MAX_THREE_ROUNDS_"
    "BTC5M_SIZE5_15USDC_LOSS_CAP_NATIVE_RUNTIME_S8P"
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


def write_json(path: Path, payload: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.write_bytes(data)
    return sha256_bytes(data)


def base_payload(status: str) -> dict[str, Any]:
    return {
        "schema_version": "B_STRATEGY_CANARY_S8R_ONE_RUN_ORCHESTRATOR_v1",
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
    if not scout.exists():
        failures.append("SCOUT_SNAPSHOT_JSON_MISSING")
    if not plan.exists():
        failures.append("ONE_RUN_PLAN_JSON_MISSING")

    scout_sha = sha256_file(scout) if scout.exists() else None
    plan_sha = sha256_file(plan) if plan.exists() else None
    if scout_sha != args.expected_scout_snapshot_sha256:
        failures.append("SCOUT_SNAPSHOT_SHA256_MISMATCH")
    if plan_sha != args.expected_one_run_plan_sha256:
        failures.append("ONE_RUN_PLAN_SHA256_MISMATCH")

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
        "PASS_S8R_ONE_RUN_ORCHESTRATOR_NO_SUBMIT_PREVIEW"
        if not failures
        else "BLOCK_S8R_ONE_RUN_ORCHESTRATOR_FAIL_CLOSED"
    )
    payload.update(
        {
            "review_only_no_submit": True,
            "ready_for_fresh_exact_approval": False,
            "ready_for_fresh_exact_approval_reason": (
                "S8R has bound preview/orchestration gates. Fresh approval still requires "
                "a separate S8S order-status/fill-evidence adapter before effectful run."
            ),
            "scout_snapshot_sha256": scout_sha,
            "one_run_plan_sha256": plan_sha,
            "prepared_order_from_live_scout_path": str(prepared_order_path) if prepared_order_path else None,
            "prepared_order_from_live_scout_sha256": prepared_order_sha,
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
        choices=("review-only", "preview-no-approval", "no-submit-orchestration-preview", "execute"),
        required=True,
    )
    parser.add_argument("--runtime-bin", default="target/debug/b_strategy_canary_s8a_native_effectful_runtime")
    parser.add_argument("--reviewed-host", default=REVIEWED_HOST)
    parser.add_argument("--rest-url", default=OFFICIAL_CLOB_REST_URL)
    parser.add_argument("--scout-snapshot-json", default="scripts/fixtures/s8a_live_scout_to_order_preview_snapshot.json")
    parser.add_argument("--expected-scout-snapshot-sha256", required=False)
    parser.add_argument("--one-run-plan-json", default="scripts/fixtures/s8a_one_run_driver_preview_plan.json")
    parser.add_argument("--expected-one-run-plan-sha256", required=False)
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
        return no_submit_orchestration_preview(args)
    print_json(base_payload("BLOCK_S8R_EXECUTE_MODE_REQUIRES_FUTURE_FILLED_QTY_STATUS_ADAPTER_EXIT_66"))
    return 66


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
