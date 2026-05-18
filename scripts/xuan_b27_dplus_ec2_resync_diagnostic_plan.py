#!/usr/bin/env python3
"""Create a local, no-network plan for the next EC2 read-only diagnostic.

The plan is deliberately non-executable. It records the approval boundaries and
required gates for re-syncing the fixed observer entrypoint to an isolated EC2
worktree and then running a bounded read-only User WS diagnostic.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROPOSED_MARKET_SLUG = "btc-updown-5m"
PROPOSED_DIAGNOSTIC_DURATION_SECS = 300
REQUIRED_REMOTE_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
PROPOSED_REMOTE_ROOT_PREFIX = "/home/ubuntu/xuan_research_runs/xuan_research_dplus_entrypoint_resync"


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


def summarize_path(path: Path | None, value: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {"path": str(path) if path else None, "exists": bool(path and path.exists())}
    for field in fields:
        out[field] = value.get(field)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--duration-seconds", type=int, default=PROPOSED_DIAGNOSTIC_DURATION_SECS)
    parser.add_argument("--market-slug", default=PROPOSED_MARKET_SLUG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_ec2_resync_diagnostic_plan_{label}"

    readiness_path = latest_manifest(
        root,
        "xuan_b27_dplus_auth_observer_readiness_*",
        artifact="xuan_b27_dplus_auth_observer_readiness_check",
    )
    static_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_static_smoke_*",
        artifact="xuan_b27_dplus_readonly_user_ws_static_smoke",
    )
    summary_gate_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_summary_gate_smoke_*",
        artifact="xuan_b27_dplus_readonly_user_ws_summary_gate_smoke",
    )
    wrapper_refusal_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke_*",
        artifact="xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke",
    )
    bundle_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_local_status_bundle_smoke_*",
        artifact="xuan_b27_dplus_local_status_bundle_smoke",
    )
    postmortem_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_postmortem_*",
        artifact="xuan_b27_dplus_readonly_user_ws_postmortem",
    )
    old_sync_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_entrypoint_sync_*",
        artifact="xuan_b27_dplus_ec2_entrypoint_sync_local_summary",
    )
    old_observer_run_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_observer_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_local_acceptance_review",
    )

    readiness = read_json(readiness_path)
    static = read_json(static_path)
    summary_gate = read_json(summary_gate_path)
    wrapper_refusal = read_json(wrapper_refusal_path)
    bundle_smoke = read_json(bundle_smoke_path)
    postmortem = read_json(postmortem_path)
    old_sync = read_json(old_sync_path)
    old_observer_run = read_json(old_observer_run_path)

    local_gates_ok = all(
        (
            readiness.get("status") == "READY_FOR_APPROVAL",
            static.get("status") == "PASS",
            summary_gate.get("status") == "PASS",
            wrapper_refusal.get("status") == "PASS",
            bundle_smoke.get("status") == "PASS",
        )
    )
    stale_sync_confirmed = (
        old_sync.get("status") == "PASS_ENTRYPOINT_SYNC_READY"
        and old_observer_run.get("status") in {"FAIL_NO_USER_WS_RECORDS", "FAIL_NO_USER_WS_CONNECTION"}
        and postmortem.get("summary_return_code") == 4
    )
    proposed_remote_root = f"{PROPOSED_REMOTE_ROOT_PREFIX}_{label}"
    proposed_remote_worktree = f"{proposed_remote_root}/worktree"
    status = "READY_FOR_EXPLICIT_SYNC_AND_DIAGNOSTIC_APPROVAL" if local_gates_ok and stale_sync_confirmed else "BLOCKED_PLAN_INPUTS"

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_ec2_resync_diagnostic_plan",
        "status": status,
        "created_utc": label,
        "scope": "local_no_network_ec2_resync_diagnostic_plan",
        "strategy": "xuan_b27_dplus",
        "problem_statement": "Previous synced EC2 worktree predates rustls provider/User WS task-monitor fix; rerun requires isolated resync/rebuild first.",
        "local_gates_ok": local_gates_ok,
        "stale_sync_confirmed": stale_sync_confirmed,
        "previous_sync": summarize_path(
            old_sync_path,
            old_sync,
            ("status", "remote_worktree", "observer_binary_sha256", "broker_preflight_status"),
        ),
        "previous_observer_run": summarize_path(
            old_observer_run_path,
            old_observer_run,
            ("status", "acceptance_passed", "wrapper_summary_return_code", "user_ws_raw_count", "orders_possible"),
        ),
        "postmortem": summarize_path(
            postmortem_path,
            postmortem,
            ("status", "summary_return_code"),
        ),
        "local_gate_inputs": {
            "readiness": summarize_path(readiness_path, readiness, ("status",)),
            "static_smoke": summarize_path(static_path, static, ("status",)),
            "summary_gate_smoke": summarize_path(summary_gate_path, summary_gate, ("status",)),
            "wrapper_refusal_smoke": summarize_path(wrapper_refusal_path, wrapper_refusal, ("status",)),
            "bundle_smoke": summarize_path(bundle_smoke_path, bundle_smoke, ("status",)),
        },
        "proposed_resync": {
            "requires_explicit_resync_rebuild_approval": True,
            "remote_root": proposed_remote_root,
            "remote_worktree": proposed_remote_worktree,
            "allowed_remote_write_prefix": "/home/ubuntu/xuan_research_runs/xuan_research_",
            "forbidden_remote_write_prefixes": [
                "/srv/pm_as_ofi",
                "/srv/pm_as_ofi/shared-ingress-main",
                "/mnt/poly-replay",
                "raw",
                "replay_published",
            ],
            "must_exclude": [".env", "xuan_research_artifacts", "target", "raw", "replay", ".git"],
            "must_rebuild_binary": "cargo build --locked --bin xuan_b27_dplus_user_ws_observer",
            "must_run_remote_static_gates": [
                "python3 -m py_compile scripts/xuan_b27_dplus_readonly_user_ws_observer.py scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py scripts/xuan_b27_dplus_shared_ingress_preflight.py",
                "bash -n scripts/xuan_b27_dplus_readonly_user_ws_static_smoke.sh scripts/xuan_b27_dplus_readonly_user_ws_summary_gate_smoke.sh scripts/xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke.sh",
                "scripts/xuan_b27_dplus_shared_ingress_preflight.py --root /srv/pm_as_ofi/shared-ingress-main",
            ],
        },
        "proposed_diagnostic_run": {
            "requires_explicit_exact_run_approval": True,
            "remote_worktree": proposed_remote_worktree,
            "shared_ingress_role": "client",
            "shared_ingress_root": REQUIRED_REMOTE_SHARED_INGRESS_ROOT,
            "duration_seconds": args.duration_seconds,
            "market_slug": args.market_slug,
            "wrapper_args": [
                "--approved-readonly-user-ws",
                "--duration-seconds",
                str(args.duration_seconds),
                "--market-slug",
                args.market_slug,
                "--shared-ingress-root",
                REQUIRED_REMOTE_SHARED_INGRESS_ROOT,
            ],
            "acceptance_gate": [
                "scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py <run_dir> --check-readonly --require-user-ws-connection",
                "Expected pass condition: user_ws_connected_count > 0 and user_ws_subscribe_sent_count > 0; raw records may remain zero on a quiet account.",
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
            "packaged_files": False,
            "synced_files": False,
            "built_remote_binary": False,
            "started_observer": False,
            "started_user_ws": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_gate": "User must explicitly approve the resync/rebuild and the exact EC2 read-only diagnostic run before any SSH/network action.",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == "READY_FOR_EXPLICIT_SYNC_AND_DIAGNOSTIC_APPROVAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
