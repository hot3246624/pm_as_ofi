#!/usr/bin/env python3
"""Publish a no-network run plan for compliant D+ backtest evidence.

The plan deliberately does not run a backtest. The existing pair-arb backtest
runner consumes a local SQLite `market_ticks` snapshot, while the promotion
evidence must come from declared strict/cache, completion-store, and optional
public-account-truth inputs. This artifact keeps that boundary explicit.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_compliant_backtest_run_plan"
PREFLIGHT_ARTIFACT = "xuan_b27_dplus_compliant_backtest_input_preflight"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def latest_manifest(root: Path, pattern: str, artifact: str) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    matches = [path for path in matches if read_json(path).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime_ns)


def all_side_effects_false(data: dict[str, Any]) -> bool:
    side_effects = data.get("side_effects") or {}
    return bool(side_effects) and all(value is False for value in side_effects.values())


def safe_preflight(preflight: dict[str, Any]) -> bool:
    return (
        preflight.get("artifact") == PREFLIGHT_ARTIFACT
        and preflight.get("raw_replay_scanned") is False
        and preflight.get("duckdb_tables_read") is False
        and preflight.get("orders_sent") is False
        and preflight.get("cancels_sent") is False
        and preflight.get("redeems_sent") is False
        and preflight.get("auth_network_started") is False
        and all_side_effects_false(preflight)
    )


def public_truth_ready(preflight: dict[str, Any]) -> bool:
    return (preflight.get("public_account_execution_truth_v1") or {}).get("ready") is True


def build_plan(preflight: dict[str, Any], preflight_path: Path | None) -> dict[str, Any]:
    safe = safe_preflight(preflight)
    inputs_available = preflight.get("preflight_passed") is True and safe
    adapter_ready = False
    existing_runner_input_type = "local_sqlite_snapshot_btc5m_market_ticks"
    required_dataset_type = "declared_strict_cache_plus_completion_store"
    status = (
        "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE"
        if not inputs_available
        else "BLOCKED_COMPLIANT_BACKTEST_ADAPTER_NOT_IMPLEMENTED"
    )
    return {
        "status": status,
        "ready_to_run_compliant_backtest": False,
        "inputs_available": inputs_available,
        "input_preflight_path": str(preflight_path) if preflight_path else None,
        "input_preflight_status": preflight.get("status"),
        "input_preflight_passed": preflight.get("preflight_passed"),
        "input_preflight_safe": safe,
        "missing_roots": preflight.get("missing_roots") or [],
        "strict_root": preflight.get("strict_root"),
        "completion_root": preflight.get("completion_root"),
        "public_truth_root": preflight.get("public_truth_root"),
        "strict_ready_label_count": preflight.get("strict_ready_label_count"),
        "completion_ready_label_count": preflight.get("completion_ready_label_count"),
        "public_account_execution_truth_v1_ready": public_truth_ready(preflight),
        "existing_runner": "src/bin/pair_arb_backtest.rs",
        "existing_runner_input_type": existing_runner_input_type,
        "required_dataset_type": required_dataset_type,
        "requires_compliant_store_adapter": True,
        "compliant_store_adapter_ready": adapter_ready,
        "adapter_requirement": (
            "Build or verify a local adapter that reads only declared strict/cache and "
            "completion-store inputs, emits pair-arb candidate/fill/residual metrics, "
            "declares data scope, and does not scan raw/replay."
        ),
        "required_report_declaration_fields": preflight.get("required_report_declaration_fields")
        or [
            "data_root",
            "dataset_type",
            "labels",
            "days",
            "market_prefix",
            "assets",
            "row_count",
            "excluded_20260514_20260515",
            "contains_20260518",
            "includes_public_account_execution_truth_v1",
        ],
        "allowed_days": "20260502..20260513 plus 20260516 and 20260517 when manifests are present",
        "excluded_days": preflight.get("excluded_days") or ["20260514", "20260515"],
        "not_ready_days": preflight.get("not_ready_days") or ["20260518"],
        "candidate_phases": [
            {
                "phase": "input_preflight",
                "status": "PASS" if inputs_available else "BLOCKED",
                "side_effects": "metadata_only",
            },
            {
                "phase": "schema_adapter_review",
                "status": "PENDING",
                "side_effects": "local_read_only_allowed_stores_only",
            },
            {
                "phase": "compliant_pair_arb_backtest",
                "status": "PENDING",
                "required_outputs": [
                    "declared grid manifest",
                    "OOS compare manifest",
                    "walk-forward compare manifest",
                    "public-account truth cross-check summary when available",
                ],
            },
        ],
        "blocked_reason": (
            "compliant strict/cache/completion/public-truth inputs unavailable locally"
            if not inputs_available
            else "existing pair_arb_backtest consumes local SQLite snapshots; compliant store adapter is not implemented"
        ),
        "next_gate": (
            "make compliant stores available locally or use an approved read-only metadata discovery path"
            if not inputs_available
            else "implement/review compliant strict-cache/completion-store adapter before running promotion backtest"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-preflight")
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    preflight_path = (
        Path(args.input_preflight)
        if args.input_preflight
        else latest_manifest(root, "xuan_b27_dplus_compliant_backtest_input_preflight_*", PREFLIGHT_ARTIFACT)
    )
    preflight = read_json(preflight_path)
    plan = build_plan(preflight, preflight_path)
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_compliant_backtest_run_plan",
        **plan,
        "raw_replay_scanned": False,
        "duckdb_tables_read": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "duckdb_tables_read": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
