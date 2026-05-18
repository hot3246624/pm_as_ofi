#!/usr/bin/env python3
"""Create a local, no-network plan for a 30m EC2 read-only acceptance run."""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MARKET_SLUG = "btc-updown-5m"
DEFAULT_DURATION_SECONDS = 1800
REQUIRED_REMOTE_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
REMOTE_RUN_PREFIX = "/home/ubuntu/xuan_research_runs/xuan_research_"


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--market-slug", default=DEFAULT_MARKET_SLUG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_ec2_30m_acceptance_plan_{label}"

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
    diagnostic_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_readonly_diagnostic_smoke_*",
        artifact="xuan_b27_dplus_ec2_readonly_diagnostic_smoke",
    )
    resync_rebuild_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_resync_rebuild_*",
        artifact="xuan_b27_dplus_ec2_resync_rebuild_local_summary",
    )
    diagnostic_review_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_local_acceptance_review",
    )

    readiness = read_json(readiness_path)
    bundle = read_json(bundle_path)
    diagnostic_smoke = read_json(diagnostic_smoke_path)
    resync_rebuild = read_json(resync_rebuild_path)
    diagnostic_review = read_json(diagnostic_review_path)

    side_effects = diagnostic_review.get("side_effects") or {}
    diagnostic_passed = (
        diagnostic_review.get("status") == "PASS_READONLY_USER_WS_DIAGNOSTIC"
        and diagnostic_review.get("acceptance_passed") is True
        and diagnostic_review.get("user_ws_connected_count", 0) > 0
        and diagnostic_review.get("user_ws_subscribe_sent_count", 0) > 0
        and diagnostic_review.get("user_ws_error_event_count") == 0
        and diagnostic_review.get("forbidden_event_count") == 0
        and diagnostic_review.get("event_decode_error_count") == 0
        and diagnostic_review.get("user_ws_decode_error_count") == 0
        and diagnostic_review.get("orders_possible") is False
        and diagnostic_review.get("recorder_critical_drop_count") == 0
        and side_effects.get("orders_sent") is False
        and side_effects.get("cancels_sent") is False
        and side_effects.get("redeems_sent") is False
        and side_effects.get("started_broker") is False
        and side_effects.get("systemd_called") is False
        and side_effects.get("service_control_called") is False
        and side_effects.get("env_files_written") is False
    )
    resync_ready = (
        resync_rebuild.get("status") == "PASS_RESYNC_REBUILD_READY"
        and str(resync_rebuild.get("remote_worktree", "")).startswith(REMOTE_RUN_PREFIX)
        and resync_rebuild.get("broker_preflight_status") == "OK"
        and resync_rebuild.get("entrypoint_manifest_status") == "PASS"
        and isinstance(resync_rebuild.get("observer_binary_sha256"), str)
        and len(resync_rebuild.get("observer_binary_sha256")) == 64
    )
    local_gates_ok = (
        readiness.get("status") == "READY_FOR_APPROVAL"
        and diagnostic_smoke.get("status") == "PASS"
        and bundle.get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_DIAGNOSTIC"
    )
    status = "READY_FOR_EXPLICIT_EC2_30M_ACCEPTANCE_APPROVAL" if (
        diagnostic_passed and resync_ready and local_gates_ok
    ) else "BLOCKED_30M_ACCEPTANCE_PLAN_INPUTS"

    remote_worktree = resync_rebuild.get("remote_worktree")
    remote_run_dir = f"{resync_rebuild.get('remote_root')}/acceptance_run_{label}" if resync_rebuild.get("remote_root") else None
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_ec2_30m_acceptance_plan",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_ec2_30m_acceptance_plan",
        "diagnostic_passed": diagnostic_passed,
        "resync_ready": resync_ready,
        "local_gates_ok": local_gates_ok,
        "inputs": {
            "readiness": summarize(readiness_path, readiness, ("status",)),
            "status_bundle": summarize(bundle_path, bundle, ("verdict", "ec2_readonly_user_ws_status")),
            "diagnostic_smoke": summarize(diagnostic_smoke_path, diagnostic_smoke, ("status", "scope")),
            "resync_rebuild": summarize(
                resync_rebuild_path,
                resync_rebuild,
                ("status", "remote_root", "remote_worktree", "observer_binary_sha256", "broker_preflight_status"),
            ),
            "diagnostic_review": summarize(
                diagnostic_review_path,
                diagnostic_review,
                (
                    "status",
                    "acceptance_passed",
                    "user_ws_connected_count",
                    "user_ws_subscribe_sent_count",
                    "user_ws_raw_count",
                    "forbidden_event_count",
                    "orders_possible",
                ),
            ),
        },
        "proposed_acceptance_run": {
            "requires_explicit_exact_run_approval": True,
            "remote_worktree": remote_worktree,
            "remote_run_dir": remote_run_dir,
            "allowed_remote_write_prefix": REMOTE_RUN_PREFIX,
            "shared_ingress_role": "client",
            "shared_ingress_root": REQUIRED_REMOTE_SHARED_INGRESS_ROOT,
            "duration_seconds": args.duration_seconds,
            "market_slug": args.market_slug,
            "wrapper": "scripts/xuan_b27_dplus_readonly_user_ws_observer.py",
            "wrapper_args": [
                "--approved-readonly-user-ws",
                "--duration-seconds",
                str(args.duration_seconds),
                "--market-slug",
                args.market_slug,
                "--shared-ingress-root",
                REQUIRED_REMOTE_SHARED_INGRESS_ROOT,
                "--output-dir",
                "<remote_run_dir>",
            ],
            "acceptance_gates": [
                "wrapper_status == PASS_READONLY_USER_WS_OBSERVER",
                "summary gate --check-readonly passes",
                "summary gate --require-user-ws-connection passes",
                "user_ws_connected_count > 0",
                "user_ws_subscribe_sent_count > 0",
                "user_ws_error_event_count == 0",
                "event_decode_error_count == 0",
                "user_ws_decode_error_count == 0",
                "forbidden_event_count == 0",
                "orders_possible == false",
                "recorder_critical_drop_count == 0",
            ],
            "non_acceptance_notes": [
                "This remains a read-only observer acceptance, not canary approval.",
                "A quiet account may produce no fills; fills_seen == 0 is acceptable if connection/subscription gates pass.",
            ],
        },
        "forbidden_side_effects": {
            "systemd_or_service_control": False,
            "broker_start_stop_repair": False,
            "shared_ingress_modification": False,
            "order_cancel_redeem": False,
            "live_trading": False,
            "remote_env_file_write": False,
            "tunnel": False,
            "git_fetch_push": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "started_observer": False,
            "started_user_ws": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_gate": "Explicit main-thread approval for this exact EC2 30m read-only acceptance run.",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == "READY_FOR_EXPLICIT_EC2_30M_ACCEPTANCE_APPROVAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
