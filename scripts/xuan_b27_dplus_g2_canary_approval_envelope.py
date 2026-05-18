#!/usr/bin/env python3
"""Validate a local exact G2 canary approval envelope.

This helper is deliberately local-only. It validates a structured approval
JSON that a future launcher can require before any sync/run path. It does not
parse chat text, connect to EC2, start processes, or send orders.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


EXPECTED_STRATEGY = "xuan_b27_dplus"
EXPECTED_SCOPE = "exact_g2_canary_sync_rebuild_and_run"
EXPECTED_RUN_CLASS = "G2_READ_WRITE_CANARY_SMOKE"
EXPECTED_MARKET = "btc-updown-5m"
EXPECTED_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
EXPECTED_EXECUTOR_REVIEW_STATUS = "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION"
EXPECTED_EXECUTOR_REVIEW_SCOPE = "local_no_network_g2_canary_effectful_executor_review"
REMOTE_PREFIX = "/home/ubuntu/xuan_research_runs/xuan_research_"


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def is_remote_run_path(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(REMOTE_PREFIX)


def bool_is(value: Any, expected: bool) -> bool:
    return value is expected


def safe_review_artifact_path(value: Any) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    path = Path(value)
    text = str(path)
    forbidden_parts = (
        "/mnt/poly-replay",
        "replay_published",
        "/raw/",
        "/raw",
        "raw/",
    )
    if "xuan_research_artifacts" not in path.parts:
        return None
    if any(part in text for part in forbidden_parts):
        return None
    return path


def validate(envelope: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    run = envelope.get("run") or {}
    risk = envelope.get("risk") or {}
    effects = envelope.get("allowed_side_effects") or {}
    forbidden = envelope.get("forbidden_side_effects") or {}
    review = envelope.get("post_run_review") or {}
    executor_review = envelope.get("executor_review") or {}

    if envelope.get("approval_scope") != EXPECTED_SCOPE:
        failures.append("wrong_approval_scope")
    if envelope.get("explicit_current_conversation_approval") is not True:
        failures.append("missing_explicit_current_conversation_approval")
    if envelope.get("heartbeat_or_generic_approval") is not False:
        failures.append("heartbeat_or_generic_approval_not_rejected")
    if envelope.get("strategy") != EXPECTED_STRATEGY:
        failures.append("wrong_strategy")
    if run.get("run_class") != EXPECTED_RUN_CLASS:
        failures.append("wrong_run_class")
    if run.get("market_slug") != EXPECTED_MARKET:
        failures.append("wrong_market_slug")
    if run.get("shared_ingress_role") != "client":
        failures.append("shared_ingress_not_client")
    if run.get("shared_ingress_root") != EXPECTED_SHARED_INGRESS_ROOT:
        failures.append("wrong_shared_ingress_root")
    if not is_remote_run_path(run.get("remote_worktree")):
        failures.append("remote_worktree_not_isolated")
    if not is_remote_run_path(run.get("remote_run_dir")):
        failures.append("remote_run_dir_not_isolated")
    if run.get("duration_seconds") != 1800:
        failures.append("wrong_duration_seconds")
    if run.get("max_rounds") != 6:
        failures.append("wrong_max_rounds")
    if run.get("sync_rebuild_approved") is not True:
        failures.append("sync_rebuild_not_approved")
    if run.get("canary_run_approved") is not True:
        failures.append("canary_run_not_approved")

    if risk.get("target_qty") != 5:
        failures.append("wrong_target_qty")
    if risk.get("max_live_orders") != 2:
        failures.append("wrong_max_live_orders")
    if float(risk.get("max_open_cost_usdc", 999.0)) > 50.0:
        failures.append("max_open_cost_exceeded")
    if float(risk.get("max_strategy_exposure_usdc", 999.0)) > 100.0:
        failures.append("max_strategy_exposure_exceeded")
    if risk.get("max_active_markets") != 1:
        failures.append("wrong_max_active_markets")
    if risk.get("post_only") is not True:
        failures.append("post_only_not_required")
    if risk.get("allow_passive_taker") is not False:
        failures.append("passive_taker_allowed")
    if risk.get("stop_on_unknown") is not True:
        failures.append("stop_on_unknown_not_required")

    if effects.get("capped_post_only_orders") is not True:
        failures.append("capped_post_only_orders_not_allowed")
    if effects.get("bounded_own_order_cancels") is not True:
        failures.append("bounded_own_cancels_not_allowed")
    for field in ("redeem_or_claim", "broker_control", "service_control", "shared_ingress_write", "env_write"):
        if effects.get(field) is not False:
            failures.append(f"{field}_allowed")

    for field in (
        "systemd_or_service_control",
        "broker_start_stop_repair",
        "shared_ingress_modification",
        "remote_env_file_write",
        "raw_replay_scan_or_write",
        "unbounded_live_trading",
    ):
        if forbidden.get(field) is not True:
            failures.append(f"{field}_not_forbidden")

    if review.get("summarizer") != "scripts/xuan_b27_dplus_summarize_g2_canary_run.py":
        failures.append("wrong_post_run_summarizer")
    if review.get("require_check_acceptance") is not True:
        failures.append("post_run_check_acceptance_missing")
    if review.get("require_secret_sentinel_scan") is not True:
        failures.append("post_run_secret_scan_missing")

    if executor_review.get("reviewed_effectful_executor_implementation") is not True:
        failures.append("effectful_executor_review_not_confirmed")
    if executor_review.get("review_status") != EXPECTED_EXECUTOR_REVIEW_STATUS:
        failures.append("wrong_effectful_executor_review_status")
    if executor_review.get("require_current_payload_allowlist_no_drift") is not True:
        failures.append("effectful_executor_payload_drift_gate_missing")
    if executor_review.get("require_not_heartbeat_or_generic_approval") is not True:
        failures.append("effectful_executor_generic_approval_gate_missing")
    review_path = safe_review_artifact_path(executor_review.get("review_artifact"))
    if review_path is None:
        failures.append("effectful_executor_review_artifact_path_not_safe")
    elif not review_path.exists():
        failures.append("effectful_executor_review_artifact_missing")
    else:
        review_artifact = read_json(review_path)
        review_side_effects = review_artifact.get("side_effects") or {}
        if review_artifact.get("artifact") != "xuan_b27_dplus_g2_canary_effectful_executor_review":
            failures.append("wrong_effectful_executor_review_artifact")
        if review_artifact.get("scope") != EXPECTED_EXECUTOR_REVIEW_SCOPE:
            failures.append("wrong_effectful_executor_review_scope")
        if review_artifact.get("status") != EXPECTED_EXECUTOR_REVIEW_STATUS:
            failures.append("effectful_executor_review_artifact_not_pass")
        if review_artifact.get("review_passed") is not True:
            failures.append("effectful_executor_review_artifact_not_passed")
        if review_artifact.get("fixture_review") is not False:
            failures.append("effectful_executor_review_artifact_is_fixture")
        if review_artifact.get("effectful_executor_implemented") is not True:
            failures.append("effectful_executor_implementation_not_recorded")
        if review_artifact.get("reviewed_effectful_executor_implementation") is not True:
            failures.append("effectful_executor_reviewed_implementation_not_recorded")
        if review_artifact.get("canary_run_authorized") is not False:
            failures.append("effectful_executor_review_canary_authorized")
        if review_artifact.get("started_canary") is not False:
            failures.append("effectful_executor_review_started_canary")
        if review_artifact.get("orders_sent") is not False:
            failures.append("effectful_executor_review_orders_sent")
        if review_artifact.get("cancels_sent") is not False:
            failures.append("effectful_executor_review_cancels_sent")
        if review_artifact.get("redeems_sent") is not False:
            failures.append("effectful_executor_review_redeems_sent")
        if review_artifact.get("auth_network_started") is not False:
            failures.append("effectful_executor_review_auth_network_started")
        if not review_side_effects or any(value is not False for value in review_side_effects.values()):
            failures.append("effectful_executor_review_side_effects_not_false")

    return failures


def summarize(path: Path) -> dict[str, Any]:
    envelope = read_json(path)
    failures = validate(envelope)
    executor_review = envelope.get("executor_review") or {}
    return {
        "artifact": "xuan_b27_dplus_g2_canary_approval_envelope_summary",
        "approval_envelope": str(path),
        "status": "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE" if not failures else "FAIL_EXACT_G2_CANARY_APPROVAL_ENVELOPE",
        "failures": failures,
        "strategy": envelope.get("strategy"),
        "approval_scope": envelope.get("approval_scope"),
        "explicit_current_conversation_approval": envelope.get("explicit_current_conversation_approval"),
        "heartbeat_or_generic_approval": envelope.get("heartbeat_or_generic_approval"),
        "run_class": (envelope.get("run") or {}).get("run_class"),
        "market_slug": (envelope.get("run") or {}).get("market_slug"),
        "shared_ingress_root": (envelope.get("run") or {}).get("shared_ingress_root"),
        "reviewed_effectful_executor_implementation": executor_review.get(
            "reviewed_effectful_executor_implementation"
        ),
        "effectful_executor_review_status": executor_review.get("review_status"),
        "effectful_executor_review_artifact": executor_review.get("review_artifact"),
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("approval_envelope", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    summary = summarize(args.approval_envelope)
    text = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    else:
        sys.stdout.write(text)
    if args.check and summary["status"] != "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
