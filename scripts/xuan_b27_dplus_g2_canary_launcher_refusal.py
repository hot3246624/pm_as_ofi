#!/usr/bin/env python3
"""Refusal-only local launcher preflight for xuan_b27_dplus G2 canary.

This script is intentionally not an execution launcher. It validates the
structured approval envelope and then refuses before any SSH/sync/build/run
path. It gives future execution code a hard local contract to preserve.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from xuan_b27_dplus_g2_canary_approval_envelope import summarize as summarize_approval


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def manifest_base(out_dir: Path, label: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_launcher_refusal",
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_launcher_refusal",
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "synced_code": False,
            "built_remote_binary": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
            "service_control_used": False,
            "shared_ingress_modified": False,
            "env_files_written": False,
        },
        "output_dir": str(out_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--approval-envelope", type=Path)
    parser.add_argument("--approved-g2-canary-sync-and-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_launcher_refusal_{label}"
    manifest = manifest_base(out_dir, label)

    if not args.approved_g2_canary_sync_and_run:
        manifest.update(
            {
                "status": "REFUSED_NO_EXPLICIT_G2_CANARY_SYNC_AND_RUN_APPROVAL",
                "reason": "missing --approved-g2-canary-sync-and-run",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64

    if not args.approval_envelope:
        manifest.update(
            {
                "status": "REFUSED_MISSING_APPROVAL_ENVELOPE",
                "reason": "missing --approval-envelope",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64

    approval_summary = summarize_approval(args.approval_envelope)
    manifest["approval_envelope"] = str(args.approval_envelope)
    manifest["approval_summary"] = approval_summary
    if approval_summary.get("status") != "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE":
        manifest.update(
            {
                "status": "REFUSED_INVALID_APPROVAL_ENVELOPE",
                "reason": "approval envelope did not pass exact G2 canary checks",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 65

    manifest.update(
        {
            "status": "REFUSED_EXECUTION_PATH_NOT_IMPLEMENTED",
            "reason": "validated approval envelope, but this local launcher is refusal-only",
            "next_gate": "implement a separately reviewed exact-run executor before any SSH/sync/run path",
        }
    )
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
