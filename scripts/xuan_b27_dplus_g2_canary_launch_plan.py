#!/usr/bin/env python3
"""Create a local, no-network exact G2 canary launch/review plan.

The artifact is an approval checklist and execution envelope only. It does not
SSH, sync, build, start a process, connect to shared-ingress, or send orders.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MARKET_SLUG = "btc-updown-5m"
DEFAULT_DURATION_SECONDS = 1800
DEFAULT_MAX_ROUNDS = 6
REMOTE_RUN_PREFIX = "/home/ubuntu/xuan_research_runs/xuan_research_"
REMOTE_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
EXPECTED_EFFECTFUL_EXECUTOR_PHASES = {
    "create_isolated_remote_run_root_under_xuan_research_runs",
    "sync_allowlisted_source_files_only",
    "build_declared_remote_targets_only",
    "run_readonly_shared_ingress_preflight_before_canary",
    "inject_auth_vars_ephemerally_only",
    "start_single_bounded_g2_canary_process",
    "wait_or_stop_at_duration_or_round_limit",
    "pull_artifacts_to_xuan_research_artifacts",
}
EXPECTED_EXECUTOR_PLAN_SECTIONS = (
    "remote_run_root_plan",
    "payload_sync_plan",
    "remote_build_plan",
    "shared_ingress_preflight_plan",
    "auth_injection_plan",
    "bounded_canary_process_plan",
    "wait_stop_supervision_plan",
    "artifact_pullback_review_plan",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def latest_manifest(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    if artifact:
        matches = [p for p in matches if read_json(p).get("artifact") == artifact]
        if artifact.endswith("_smoke"):
            pass_matches = [p for p in matches if read_json(p).get("status") == "PASS"]
            if pass_matches:
                matches = pass_matches
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime_ns)


def latest_json_file(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern))]
    if artifact:
        matches = [p for p in matches if read_json(p).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime_ns)


def summarize(path: Path | None, value: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {"path": str(path) if path else None, "exists": bool(path and path.exists())}
    for field in fields:
        out[field] = value.get(field)
    return out


def all_false(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(item is False for item in value.values())


def executor_preflight_summary(
    smoke_path: Path | None,
    smoke: dict[str, Any],
    preflight_path: Path | None,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    phase_plan = preflight.get("phase_plan") or []
    phase_names = {item.get("phase") for item in phase_plan if isinstance(item, dict)}
    missing_effectful_phases = sorted(EXPECTED_EFFECTFUL_EXECUTOR_PHASES - phase_names)
    missing_plan_sections = [
        section
        for section in EXPECTED_EXECUTOR_PLAN_SECTIONS
        if not isinstance(preflight.get(section), dict)
        or preflight.get(section, {}).get("status") != "PASS"
    ]
    side_effect_sections_ok = True
    section_statuses: dict[str, Any] = {}
    for section in EXPECTED_EXECUTOR_PLAN_SECTIONS:
        section_value = preflight.get(section)
        if not isinstance(section_value, dict):
            section_statuses[section] = None
            side_effect_sections_ok = False
            continue
        section_statuses[section] = section_value.get("status")
        planned = section_value.get("planned_side_effects")
        if isinstance(planned, dict) and any(value is not False for value in planned.values()):
            side_effect_sections_ok = False

    return {
        "smoke": summarize(
            smoke_path,
            smoke,
            ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "executor"),
        ),
        "valid_preflight_manifest": str(preflight_path) if preflight_path else None,
        "valid_preflight_exists": bool(preflight_path and preflight_path.exists()),
        "status": preflight.get("status"),
        "scope": preflight.get("scope"),
        "effectful_executor_implemented": preflight.get("effectful_executor_implemented"),
        "executor_preflight_implemented": preflight.get("executor_preflight_implemented"),
        "ready_to_execute": False,
        "canary_run_authorized": preflight.get("canary_run_authorized"),
        "first_unimplemented_effectful_phase": preflight.get("first_unimplemented_effectful_phase"),
        "all_effectful_phase_plans_present": not missing_effectful_phases,
        "missing_effectful_phase_plans": missing_effectful_phases,
        "all_plan_sections_pass": not missing_plan_sections,
        "missing_or_failed_plan_sections": missing_plan_sections,
        "plan_section_statuses": section_statuses,
        "phase_side_effects_all_false": all(
            isinstance(item, dict) and item.get("side_effects_performed") is False
            for item in phase_plan
        ),
        "planned_side_effect_sections_all_false": side_effect_sections_ok,
        "artifact_pullback_review_plan_status": (
            preflight.get("artifact_pullback_review_plan") or {}
        ).get("status"),
        "orders_sent": preflight.get("orders_sent"),
        "cancels_sent": preflight.get("cancels_sent"),
        "redeems_sent": preflight.get("redeems_sent"),
        "auth_network_started": preflight.get("auth_network_started"),
        "started_canary": preflight.get("started_canary"),
        "top_level_side_effects_all_false": all_false(preflight.get("side_effects")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS)
    parser.add_argument("--market-slug", default=DEFAULT_MARKET_SLUG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_launch_plan_{label}"

    readiness_path = latest_manifest(
        root,
        "xuan_b27_dplus_auth_observer_readiness_*",
        artifact="xuan_b27_dplus_auth_observer_readiness_check",
    )
    bundle_path = latest_manifest(
        root,
        "xuan_b27_dplus_local_status_bundle_*",
        artifact="xuan_b27_dplus_local_status_bundle",
    )
    canary_readiness_plan_path = latest_manifest(
        root,
        "xuan_b27_dplus_canary_readiness_plan_smoke_*",
        artifact="xuan_b27_dplus_canary_readiness_plan_smoke",
    )
    runbook_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_runbook_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_runbook_smoke",
    )
    acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_acceptance_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_acceptance_smoke",
    )
    ec2_acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_30m_acceptance_smoke_*",
        artifact="xuan_b27_dplus_ec2_30m_acceptance_smoke",
    )
    source_truth_runtime_gate_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_source_truth_runtime_gate_smoke_*",
        artifact="xuan_b27_dplus_source_truth_runtime_gate_smoke",
    )
    g2_canary_executor_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_smoke",
    )
    ec2_acceptance_review_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_acceptance_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_acceptance_local_acceptance_review",
    )

    readiness = read_json(readiness_path)
    bundle = read_json(bundle_path)
    canary_readiness_smoke = read_json(canary_readiness_plan_path)
    runbook_smoke = read_json(runbook_smoke_path)
    acceptance_smoke = read_json(acceptance_smoke_path)
    ec2_acceptance_smoke = read_json(ec2_acceptance_smoke_path)
    source_truth_runtime_gate_smoke = read_json(source_truth_runtime_gate_smoke_path)
    g2_canary_executor_smoke = read_json(g2_canary_executor_smoke_path)
    g2_canary_executor_preflight_path = (
        g2_canary_executor_smoke_path.parent / "valid_preflight" / "manifest.json"
        if g2_canary_executor_smoke_path
        else None
    )
    g2_canary_executor_preflight = read_json(g2_canary_executor_preflight_path)
    ec2_acceptance_review = read_json(ec2_acceptance_review_path)

    canary_plan_manifest = read_json(
        Path(canary_readiness_smoke.get("plan_manifest"))
        if isinstance(canary_readiness_smoke.get("plan_manifest"), str)
        else None
    )
    canary_readiness = canary_plan_manifest.get("canary_readiness") or {}
    executor_preflight = executor_preflight_summary(
        g2_canary_executor_smoke_path,
        g2_canary_executor_smoke,
        g2_canary_executor_preflight_path,
        g2_canary_executor_preflight,
    )
    executor_preflight_ok = (
        executor_preflight["smoke"].get("status") == "PASS"
        and executor_preflight.get("status") == "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
        and executor_preflight.get("effectful_executor_implemented") is False
        and executor_preflight.get("executor_preflight_implemented") is True
        and executor_preflight.get("ready_to_execute") is False
        and executor_preflight.get("canary_run_authorized") is False
        and executor_preflight.get("first_unimplemented_effectful_phase") is None
        and executor_preflight.get("all_effectful_phase_plans_present") is True
        and executor_preflight.get("all_plan_sections_pass") is True
        and executor_preflight.get("phase_side_effects_all_false") is True
        and executor_preflight.get("planned_side_effect_sections_all_false") is True
        and executor_preflight.get("top_level_side_effects_all_false") is True
        and executor_preflight.get("orders_sent") is False
        and executor_preflight.get("cancels_sent") is False
        and executor_preflight.get("redeems_sent") is False
        and executor_preflight.get("auth_network_started") is False
        and executor_preflight.get("started_canary") is False
    )

    local_gates_ok = (
        readiness.get("status") == "READY_FOR_APPROVAL"
        and bundle.get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
        and bundle.get("canary_readiness_status") == "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL"
        and canary_plan_manifest.get("status") == "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL"
        and canary_readiness.get("ready_for_explicit_g2_canary_approval") is True
        and canary_readiness.get("canary_run_authorized") is False
        and runbook_smoke.get("status") == "PASS"
        and acceptance_smoke.get("status") == "PASS"
        and ec2_acceptance_smoke.get("status") == "PASS"
        and source_truth_runtime_gate_smoke.get("status") == "PASS"
        and source_truth_runtime_gate_smoke.get("wallet_redeem_cashflow_runtime_producers_wired") is True
        and source_truth_runtime_gate_smoke.get("redeem_truth_can_pass_from_runtime_producer") is True
        and ec2_acceptance_review.get("status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
        and ec2_acceptance_review.get("acceptance_passed") is True
        and executor_preflight_ok
    )
    status = "READY_FOR_EXPLICIT_G2_CANARY_SYNC_AND_RUN_APPROVAL" if local_gates_ok else "BLOCKED_G2_CANARY_LAUNCH_PLAN_INPUTS"
    launch_next_gate = (
        "Explicit main-thread approval plus reviewed effectful executor implementation for the exact G2 canary sync/rebuild and run plan."
        if status == "READY_FOR_EXPLICIT_G2_CANARY_SYNC_AND_RUN_APPROVAL"
        else
        "shadow trading acceptance failed; numeric OOS/walk-forward residual-control backtest is positive but the backtest dataset is scope-limited, so rerun on compliant declared strict/cache/completion data and/or pass larger no-order shadow trading acceptance before any G2 canary launch plan can become approvable."
        if bundle.get("canary_readiness_status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
        and bundle.get("canary_pair_arb_oos_compare_numeric_validation_passed") is True
        and bundle.get("canary_pair_arb_walkforward_compare_numeric_validation_passed") is True
        and (
            bundle.get("canary_pair_arb_oos_compare_requires_compliant_dataset") is True
            or bundle.get("canary_pair_arb_walkforward_compare_requires_compliant_dataset") is True
        )
        else
        "shadow trading acceptance failed; OOS and walk-forward qualified residual-control backtest exists, run larger no-order shadow trading with the same residual-control logic before any G2 canary launch plan can become approvable; retrain/tune strategy if shadow fails."
        if bundle.get("canary_readiness_status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
        and bundle.get("canary_pair_arb_oos_compare_validation_passed") is True
        and bundle.get("canary_pair_arb_walkforward_compare_validation_passed") is True
        else
        "shadow trading acceptance failed; collect larger positive nonzero backtest/shadow evidence or retrain/tune strategy before any G2 canary launch plan can become approvable."
        if bundle.get("canary_readiness_status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
        else
        "D+ passive/passive shadow trading acceptance with pair/PnL/residual metrics must pass before any G2 canary launch plan can become approvable."
    )
    remote_root = f"{REMOTE_RUN_PREFIX}dplus_g2_canary_{label}"
    remote_worktree = f"{remote_root}/worktree"
    remote_run_dir = f"{remote_root}/g2_canary_run_{label}"
    env_envelope = {
        "PM_STRATEGY": "xuan_b27_dplus",
        "PM_XUAN_B27_DPLUS_MODE": "canary",
        "PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL": "true",
        "PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_MARKET_SLUG": args.market_slug,
        "PM_XUAN_B27_DPLUS_TARGET_QTY": "5",
        "PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC": "50",
        "PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC": "100",
        "PM_XUAN_B27_DPLUS_MAX_ACTIVE_MARKETS": "1",
        "PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": "2",
        "PM_XUAN_B27_DPLUS_POST_ONLY": "true",
        "PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER": "false",
        "PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN": "true",
        "PM_SHARED_INGRESS_ROLE": "client",
        "PM_SHARED_INGRESS_ROOT": REMOTE_SHARED_INGRESS_ROOT,
    }
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_launch_plan",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_launch_plan",
        "local_gates_ok": local_gates_ok,
        "canary_run_authorized": False,
        "ready_to_execute": False,
        "effectful_executor_implemented": False,
        "requires_reviewed_effectful_executor_implementation": True,
        "execution_readiness_status": "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED",
        "executor_preflight": executor_preflight,
        "inputs": {
            "readiness": summarize(readiness_path, readiness, ("status",)),
            "status_bundle": summarize(
                bundle_path,
                bundle,
                ("verdict", "ec2_readonly_user_ws_status", "canary_readiness_status", "canary_next_gate"),
            ),
            "canary_readiness_plan_smoke": summarize(
                canary_readiness_plan_path,
                canary_readiness_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "plan_manifest"),
            ),
            "g2_canary_runbook_smoke": summarize(
                runbook_smoke_path,
                runbook_smoke,
                ("status", "scope", "exact_approval_required", "orders_sent", "auth_network_started", "started_canary"),
            ),
            "g2_canary_acceptance_smoke": summarize(
                acceptance_smoke_path,
                acceptance_smoke,
                ("status", "scope", "exact_approval_required", "orders_sent", "auth_network_started", "started_canary"),
            ),
            "ec2_readonly_acceptance_smoke": summarize(
                ec2_acceptance_smoke_path,
                ec2_acceptance_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "acceptance_review"),
            ),
            "source_truth_runtime_gate_smoke": summarize(
                source_truth_runtime_gate_smoke_path,
                source_truth_runtime_gate_smoke,
                (
                    "status",
                    "scope",
                    "wallet_redeem_cashflow_runtime_producers_wired",
                    "redeem_truth_can_pass_from_runtime_producer",
                    "orders_sent",
                    "auth_network_started",
                ),
            ),
            "g2_canary_executor_smoke": executor_preflight["smoke"],
        },
        "proposed_resync_rebuild": {
            "requires_explicit_sync_rebuild_approval": True,
            "reason": "latest EC2 read-only observer worktree predates local canary/controller/source-truth hardening",
            "remote_root": remote_root,
            "remote_worktree": remote_worktree,
            "allowed_remote_write_prefix": REMOTE_RUN_PREFIX,
            "build_targets": ["polymarket_v2"],
            "forbidden_paths": [
                "/srv/pm_as_ofi",
                "/mnt/poly-replay",
                "replay_published",
                "raw",
                ".env",
            ],
        },
        "proposed_g2_canary_run": {
            "requires_explicit_exact_run_approval": True,
            "run_class": "G2_READ_WRITE_CANARY_SMOKE",
            "remote_worktree": remote_worktree,
            "remote_run_dir": remote_run_dir,
            "duration_seconds": args.duration_seconds,
            "max_rounds": args.max_rounds,
            "market_slug": args.market_slug,
            "shared_ingress_role": "client",
            "shared_ingress_root": REMOTE_SHARED_INGRESS_ROOT,
            "env_envelope": env_envelope,
            "allowed_side_effects_after_exact_approval": {
                "capped_post_only_orders": True,
                "bounded_own_order_cancels": True,
                "redeem_or_claim": False,
            },
            "forbidden_side_effects_even_after_exact_approval": {
                "systemd_or_service_control": True,
                "broker_start_stop_repair": True,
                "shared_ingress_modification": True,
                "remote_env_file_write": True,
                "raw_replay_scan_or_write": True,
                "unbounded_live_trading": True,
            },
        },
        "review_acceptance_contract": {
            "required_status": "PASS_G2_CANARY_ACCEPTANCE",
            "required_fields": [
                "exact_approval_scope",
                "source_truth_all_pass",
                "order_ack_count",
                "order_attempt_trace_linked",
                "venue_order_id_linked",
                "user_ws_connected_count",
                "user_ws_subscribe_sent_count",
                "event_decode_error_count",
                "user_ws_decode_error_count",
                "recorder_critical_drop_count",
                "unexpected_taker_fill_count",
                "max_live_orders_observed",
                "max_open_cost_usdc_observed",
                "max_strategy_exposure_usdc_observed",
                "secret_values_recorded",
                "secret_values_written_to_disk",
            ],
            "hard_stop_failures": [
                "missing_exact_approval_scope",
                "source_truth_not_all_pass",
                "unexpected_taker_fill",
                "recorder_critical_drop",
                "max_live_orders_exceeded",
                "max_open_cost_exceeded",
                "max_strategy_exposure_exceeded",
                "shared_ingress_not_client",
                "wrong_shared_ingress_root",
                "secret_values_recorded",
            ],
        },
        "forbidden_side_effects": {
            "ssh_or_network": False,
            "systemd_or_service_control": False,
            "broker_start_stop_repair": False,
            "shared_ingress_modification": False,
            "order_cancel_redeem": False,
            "remote_env_file_write": False,
            "git_fetch_push": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_gate": launch_next_gate,
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status in {
        "READY_FOR_EXPLICIT_G2_CANARY_SYNC_AND_RUN_APPROVAL",
        "BLOCKED_G2_CANARY_LAUNCH_PLAN_INPUTS",
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
