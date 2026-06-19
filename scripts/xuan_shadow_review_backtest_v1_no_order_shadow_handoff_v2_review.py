#!/usr/bin/env python3
"""Review the BTC tiny-canary no-order shadow handoff v2 packet.

The v2 handoff intentionally splits runner output into:

* strict 33-column no_order_shadow_report.csv,
* safety/source audit manifest JSON,
* aggregate gate summary JSON.

This local reviewer confirms the handoff contract is coherent and checks whether
the three runner output files have actually arrived.  It does not start a
runner, import candidates, deploy, or enable live orders.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


STAMP = "20260529T0450Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

DEFAULT_HANDOFF = (
    CONTRACT_ROOT
    / "xuan_btc_tiny_canary_no_order_shadow_handoff_request_v2_latest"
    / "XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_HANDOFF_REQUEST_V2.json"
)
DEFAULT_EVAL = (
    CONTRACT_ROOT
    / "xuan_btc_tiny_canary_no_order_shadow_eval_latest"
    / "XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_EVAL.json"
)
DEFAULT_GATE_SPEC = (
    CONTRACT_ROOT
    / "xuan_btc_tiny_canary_shadow_evaluation_gate_spec_latest"
    / "XUAN_BTC_TINY_CANARY_SHADOW_EVALUATION_GATE_SPEC.json"
)
DEFAULT_RUNNER_REPORT_DIR = (
    CONTRACT_ROOT / "xuan_btc_tiny_canary_no_order_shadow_report_latest"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_no_order_shadow_handoff_v2_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_no_order_shadow_handoff_v2_review_{STAMP}"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def required_columns_from_gate(gate_spec: dict[str, Any]) -> list[str]:
    columns = body(gate_spec, "shadow_evaluation_gate").get("required_shadow_report_columns")
    return columns if isinstance(columns, list) else []


def output_status(path: Path, expected_header: list[str] | None = None) -> dict[str, Any]:
    exists = path.exists()
    header = csv_header(path) if exists and path.suffix == ".csv" else []
    return {
        "path": str(path),
        "exists": exists,
        "sha256": sha256(path),
        "header": header,
        "header_matches_expected": bool(expected_header is not None and header == expected_header),
    }


def runner_output_paths(handoff: dict[str, Any], runner_report_dir: Path) -> dict[str, Path]:
    root = runner_report_dir.expanduser().resolve()
    legacy_root = Path(str(handoff.get("inputs", {}).get("current_single_csv_eval_manifest") or "")).parent
    if not root.exists():
        root = legacy_root
    return {
        "main_csv": root / "no_order_shadow_report.csv",
        "audit_manifest_json": root / "no_order_shadow_audit_manifest.json",
        "gate_summary_json": root / "no_order_shadow_gate_summary.json",
    }


def load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json(path)


def safety_pass(audit_manifest: dict[str, Any]) -> bool:
    safety = body(audit_manifest, "safety")
    return (
        audit_manifest.get("status") == "KEEP_NO_ORDER_SHADOW_AUDIT_READY"
        and safety.get("dry_run_only") is True
        and safety.get("no_order") is True
        and safety.get("orders_sent") is False
        and safety.get("cancels_sent") is False
        and safety.get("redeems_sent") is False
        and safety.get("live_orders_allowed") is False
        and safety.get("candidate_import_allowed") is False
        and safety.get("deployable") is False
        and safety.get("private_key_loaded") is False
        and safety.get("no_private_key_loaded") is True
        and safety.get("null_order_client_or_stub") is True
        and safety.get("order_api_call_count") == 0
        and safety.get("cancel_api_call_count") == 0
        and safety.get("redeem_api_call_count") == 0
        and safety.get("candidate_import_call_count") == 0
        and safety.get("safety_failures") == []
    )


def gate_summary_pass(gate_summary: dict[str, Any]) -> bool:
    summary = body(gate_summary, "summary")
    return (
        gate_summary.get("status")
        == "KEEP_XUAN_BTC_TINY_CANARY_PUBLIC_L2_PROXY_NO_ORDER_SHADOW_GATE_PASS_RESEARCH_ONLY"
        and summary.get("evaluation_passed") is True
        and summary.get("threshold_failure_count") == 0
        and summary.get("candidate_count") == 52
        and summary.get("day_count") == 15
        and float(summary.get("l2_or_live_book_action_match_rate") or 0.0) >= 0.99
        and float(summary.get("top5_supports_seed_qty_rate") or 0.0) >= 0.99
        and float(summary.get("observed_residual_cost_share") or 1.0) <= 0.03
        and float(summary.get("fee_1_50_zero_stress_after_fee_pnl") or 0.0) > 0.0
        and float(summary.get("pair_edge_50pct_zero_stress_after_fee_pnl") or 0.0) > 0.0
    )


def current_eval_pass(current_eval: dict[str, Any]) -> bool:
    summary = body(current_eval, "summary")
    policy = body(current_eval, "policy")
    return (
        current_eval.get("status")
        == "KEEP_XUAN_BTC_TINY_CANARY_PUBLIC_L2_PROXY_NO_ORDER_SHADOW_EVALUATED_PROMOTION_BLOCKED_OWNER_TRUTH"
        and summary.get("evaluation_passed") is True
        and summary.get("threshold_failure_count") == 0
        and policy.get("research_only") is True
        and policy.get("dry_run_only") is True
        and policy.get("orders_allowed") is False
        and policy.get("live_orders_allowed") is False
        and policy.get("candidate_import_allowed") is False
        and policy.get("strategy_promotion_ready") is False
        and policy.get("private_truth_ready") is False
    )


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    handoff = load_json(args.handoff)
    current_eval = load_json(args.current_eval)
    gate_spec = load_json(args.gate_spec)
    expected_columns = required_columns_from_gate(gate_spec)
    paths = runner_output_paths(handoff, args.runner_report_dir)

    main_csv = output_status(paths["main_csv"], expected_columns)
    audit_manifest_status = output_status(paths["audit_manifest_json"])
    gate_summary_status = output_status(paths["gate_summary_json"])
    audit_manifest_body = load_optional_json(paths["audit_manifest_json"])
    gate_summary_body = load_optional_json(paths["gate_summary_json"])
    three_files_present = (
        main_csv["exists"]
        and audit_manifest_status["exists"]
        and gate_summary_status["exists"]
        and main_csv["header_matches_expected"]
    )
    handoff_ready = (
        handoff.get("status") == "KEEP_NO_ORDER_SHADOW_HANDOFF_REQUEST_V2_READY_SCHEMA_SPLIT_ACCEPTED"
        and body(handoff, "evaluator_update_request").get("read_three_files") is True
        and body(handoff, "evaluator_update_request").get("main_csv_strict_schema") is True
        and body(handoff, "evaluator_update_request").get(
            "fail_closed_if_any_safety_counter_nonzero_or_nonfalse"
        )
        is True
    )
    eval_is_old_missing_single_csv = (
        current_eval.get("status") == "BLOCKED_XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_REPORT_MISSING"
    )
    audit_safety_pass = safety_pass(audit_manifest_body)
    gate_pass = gate_summary_pass(gate_summary_body)
    eval_pass = current_eval_pass(current_eval)
    runner_output_ready = three_files_present and audit_safety_pass and gate_pass
    evaluator_update_required = (
        handoff_ready
        and not runner_output_ready
        and body(handoff, "evaluator_update_request").get(
            "legacy_single_csv_missing_status_should_be_replaced_by_three_file_arrival_check"
        )
        is True
    )
    remaining_blockers: list[str] = []
    if not handoff_ready:
        remaining_blockers.append("handoff_v2_contract_not_ready")
    if evaluator_update_required:
        remaining_blockers.append("evaluator_still_needs_three_file_arrival_check")
    if not runner_output_ready:
        remaining_blockers.append("no_order_shadow_three_file_runner_outputs_missing")
    if three_files_present and not audit_safety_pass:
        remaining_blockers.append("no_order_shadow_audit_safety_gate_failed")
    if three_files_present and not gate_pass:
        remaining_blockers.append("no_order_shadow_gate_summary_failed")
    if runner_output_ready and not eval_pass:
        remaining_blockers.append("current_eval_manifest_did_not_pass_three_file_report")
    if eval_is_old_missing_single_csv:
        remaining_blockers.append("current_eval_manifest_still_legacy_single_csv_missing_status")
    start_blockers = [
        "runtime_start_binding_gate_not_rerun_after_shadow_eval",
        "fresh_active_runner_conflict_check_required_before_any_start",
        "manual_approval_must_be_revalidated_at_start_time",
        "owner_private_truth_data_unavailable_until_future_owner_execution",
    ]
    status = (
        "KEEP_SHADOW_REVIEW_BACKTEST_V1_NO_ORDER_SHADOW_HANDOFF_V2_EVALUATED_GATE_PASS_START_BLOCKED_LOCAL_ONLY"
        if handoff_ready and runner_output_ready and eval_pass
        else "KEEP_SHADOW_REVIEW_BACKTEST_V1_NO_ORDER_SHADOW_HANDOFF_V2_READY_OUTPUTS_PENDING_LOCAL_ONLY"
    )

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_no_order_shadow_handoff_v2_review",
            "status": status,
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_no_order_shadow_handoff_v2_review.py",
            "inputs": {
                "handoff": str(args.handoff),
                "current_eval": str(args.current_eval),
                "gate_spec": str(args.gate_spec),
                "runner_report_dir": str(args.runner_report_dir),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "NO_ORDER_SHADOW_HANDOFF_V2_REVIEW.md"),
            },
            "decision": {
                "handoff_v2_schema_split_accepted": handoff_ready,
                "main_csv_strict_33_column_schema_required": True,
                "audit_manifest_required": True,
                "gate_summary_required": True,
                "three_file_runner_outputs_present": three_files_present,
                "runner_output_ready_for_evaluation": runner_output_ready,
                "evaluator_update_required": evaluator_update_required,
                "current_eval_is_legacy_single_csv_missing_status": eval_is_old_missing_single_csv,
                "no_order_shadow_audit_safety_pass": audit_safety_pass,
                "no_order_shadow_gate_summary_pass": gate_pass,
                "current_eval_pass": eval_pass,
                "no_order_shadow_start_authorized_by_this_review": False,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": (
                    "run_start_binding_gate_with_fresh_conflict_check"
                    if runner_output_ready and eval_pass
                    else "wait_for_ws_no_order_runner_three_file_outputs_then_run_updated_evaluator"
                ),
            },
            "handoff_summary": {
                "status": handoff.get("status"),
                "created_utc": handoff.get("created_utc"),
                "primary_owner": handoff.get("primary_owner"),
                "support_owner": handoff.get("support_owner"),
                "next_executable_action": handoff.get("next_executable_action"),
                "reason_for_v2": handoff.get("reason_for_v2"),
            },
            "current_eval_summary": {
                "status": current_eval.get("status"),
                "created_utc": current_eval.get("created_utc"),
                "input_shadow_report": current_eval.get("input_shadow_report"),
                "summary": current_eval.get("summary"),
                "policy": current_eval.get("policy"),
            },
            "required_outputs_review": {
                "main_csv": main_csv,
                "audit_manifest_json": audit_manifest_status,
                "gate_summary_json": gate_summary_status,
                "expected_main_csv_columns": expected_columns,
            },
            "runner_output_summary": {
                "audit_status": audit_manifest_body.get("status"),
                "audit_safety": body(audit_manifest_body, "safety"),
                "gate_status": gate_summary_body.get("status"),
                "gate_summary": body(gate_summary_body, "summary"),
            },
            "remaining_blockers_before_evaluation_or_start": remaining_blockers,
            "remaining_blockers_before_start": start_blockers,
            "warnings": [
                "This review evaluates no-order shadow research output; it is not a runner start manifest.",
                "No-order shadow output can support research evaluation only; it cannot create owner private truth.",
                "No start/import/live/remote action is authorized by this review.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    handoff = body(card, "handoff_summary")
    blockers = card.get("remaining_blockers_before_evaluation_or_start", [])
    lines = [
        "# No-Order Shadow Handoff V2 Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- handoff_v2_schema_split_accepted: `{decision.get('handoff_v2_schema_split_accepted')}`",
        f"- three_file_runner_outputs_present: `{decision.get('three_file_runner_outputs_present')}`",
        f"- no_order_shadow_audit_safety_pass: `{decision.get('no_order_shadow_audit_safety_pass')}`",
        f"- no_order_shadow_gate_summary_pass: `{decision.get('no_order_shadow_gate_summary_pass')}`",
        f"- current_eval_pass: `{decision.get('current_eval_pass')}`",
        f"- evaluator_update_required: `{decision.get('evaluator_update_required')}`",
        f"- no_order_shadow_start_authorized_by_this_review: `{decision.get('no_order_shadow_start_authorized_by_this_review')}`",
        "",
        "## Handoff",
        "",
        f"- handoff_status: `{handoff.get('status')}`",
        f"- primary_owner: `{handoff.get('primary_owner')}`",
        f"- support_owner: `{handoff.get('support_owner')}`",
        f"- next_executable_action: `{handoff.get('next_executable_action')}`",
        "",
        "## Remaining Blockers",
        "",
    ]
    for blocker in blockers:
        lines.append(f"- `{blocker}`")
    start_blockers = card.get("remaining_blockers_before_start", [])
    lines.extend(
        [
            "",
            "## Start Blockers",
            "",
        ]
    )
    for blocker in start_blockers:
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The v2 handoff contract is reasonable: keep the main report schema strict and move safety/source continuity to sidecar JSON. Passing no-order shadow output can support the next start-binding gate, but it does not clear runtime start, import, live orders, remote execution, or promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", type=Path, default=DEFAULT_HANDOFF)
    parser.add_argument("--current-eval", type=Path, default=DEFAULT_EVAL)
    parser.add_argument("--gate-spec", type=Path, default=DEFAULT_GATE_SPEC)
    parser.add_argument("--runner-report-dir", type=Path, default=DEFAULT_RUNNER_REPORT_DIR)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "NO_ORDER_SHADOW_HANDOFF_V2_REVIEW.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"scorecard": str(args.scorecard), "status": card.get("status")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
