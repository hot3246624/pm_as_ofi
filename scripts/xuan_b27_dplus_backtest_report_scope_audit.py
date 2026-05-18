#!/usr/bin/env python3
"""Audit D+ backtest report data-scope declarations.

The audit is intentionally local and metadata-only. It reads manifest JSON files
under xuan_research_artifacts and verifies that backtest/OOS/walk-forward
reports declare the data scope required by the 20260515 backtest data rule.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_backtest_report_scope_audit"
REPORT_ARTIFACTS = {
    "xuan_b27_dplus_pair_arb_backtest_json_grid",
    "xuan_b27_dplus_pair_arb_oos_compare",
    "xuan_b27_dplus_pair_arb_walkforward_compare",
}
REQUIRED_DECLARATION_FIELDS = (
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
)
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
FORBIDDEN_PATH_PARTS = {"raw", "replay_published", "poly-replay"}
LOCAL_SCOPE_DATASET_TYPES = {"local_sqlite_snapshot_btc5m"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def path_is_safe(path: str | None) -> bool:
    if not path:
        return False
    p = Path(path)
    text = str(p)
    if "/mnt/poly-replay" in text or "replay_published" in text:
        return False
    return not any(part in FORBIDDEN_PATH_PARTS for part in p.parts)


def latest_manifest(root: Path, pattern: str, artifact: str) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    matches = [path for path in matches if read_json(path).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime_ns)


def manifest_for_jsonl(root: Path, jsonl_path: str | None) -> Path | None:
    if not jsonl_path:
        return None
    path = Path(jsonl_path)
    if not path.is_absolute():
        path = root / path
    manifest = path.parent / "manifest.json"
    return manifest if manifest.exists() else None


def default_manifests(root: Path) -> list[Path]:
    paths: list[Path] = []
    oos = latest_manifest(root, "xuan_b27_dplus_pair_arb_oos_compare_*", "xuan_b27_dplus_pair_arb_oos_compare")
    walk = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_walkforward_compare_*",
        "xuan_b27_dplus_pair_arb_walkforward_compare",
    )
    for path in (oos, walk):
        if path:
            paths.append(path)
            data = read_json(path)
            for jsonl in (data.get("first_jsonl"), data.get("second_jsonl")):
                grid_manifest = manifest_for_jsonl(root, jsonl)
                if grid_manifest:
                    paths.append(grid_manifest)
            for split in data.get("splits") or []:
                grid_manifest = manifest_for_jsonl(root, split.get("jsonl"))
                if grid_manifest:
                    paths.append(grid_manifest)
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved not in seen:
            unique.append(path)
            seen.add(resolved)
    return unique


def declaration_value(data: dict[str, Any], field: str) -> Any:
    if field in data and data.get(field) is not None:
        return data.get(field)
    declaration = data.get("backtest_data_declaration") or {}
    return declaration.get(field)


def list_contains_forbidden_days(days: Any) -> bool:
    if not isinstance(days, list):
        return True
    day_set = {str(day) for day in days}
    return bool((day_set & FORBIDDEN_DAYS) or (day_set & NOT_READY_DAYS))


def side_effects_false(data: dict[str, Any]) -> bool:
    side_effects = data.get("side_effects") or {}
    direct = {
        "raw_replay_scanned": data.get("raw_replay_scanned"),
        "orders_sent": data.get("orders_sent"),
        "cancels_sent": data.get("cancels_sent"),
        "redeems_sent": data.get("redeems_sent"),
        "auth_network_started": data.get("auth_network_started"),
        "started_canary": data.get("started_canary"),
    }
    return all(value is False or value is None for value in direct.values()) and all(
        value is False for value in side_effects.values()
    )


def audit_one(root: Path, path: Path) -> dict[str, Any]:
    data = read_json(path)
    failures: list[str] = []
    artifact = data.get("artifact")
    if artifact not in REPORT_ARTIFACTS:
        failures.append("unsupported_artifact")
    if "_read_error" in data:
        failures.append("manifest_decode_error")

    values = {field: declaration_value(data, field) for field in REQUIRED_DECLARATION_FIELDS}
    missing = [field for field, value in values.items() if value is None or value == [] or value == ""]
    if missing:
        failures.append("missing_required_declaration_fields")
    if not path_is_safe(str(path)):
        failures.append("unsafe_manifest_path")
    if not path_is_safe(str(values.get("data_root"))):
        failures.append("unsafe_or_missing_data_root")
    if not isinstance(values.get("labels"), list) or not values.get("labels"):
        failures.append("labels_not_declared")
    if not isinstance(values.get("days"), list) or not values.get("days"):
        failures.append("days_not_declared")
    if list_contains_forbidden_days(values.get("days")):
        failures.append("forbidden_or_not_ready_days_in_report")
    if values.get("excluded_20260514_20260515") is not True:
        failures.append("excluded_20260514_20260515_not_true")
    if values.get("contains_20260518") is not False:
        failures.append("contains_20260518_not_false")
    if not isinstance(values.get("includes_public_account_execution_truth_v1"), bool):
        failures.append("public_truth_inclusion_not_boolean")
    if not isinstance(values.get("assets"), list) or not values.get("assets"):
        failures.append("assets_not_declared")
    if not isinstance(values.get("row_count"), int) or values.get("row_count", 0) <= 0:
        failures.append("row_count_not_positive_int")
    if not side_effects_false(data):
        failures.append("forbidden_side_effects_present")

    dataset_type = values.get("dataset_type")
    can_support_promotion = data.get("can_support_strategy_promotion")
    requires_compliant_dataset = data.get("requires_compliant_backtest_dataset_for_promotion")
    scope_limited = dataset_type in LOCAL_SCOPE_DATASET_TYPES or can_support_promotion is False
    validation_field = None
    numeric_validation_field = None
    if artifact == "xuan_b27_dplus_pair_arb_oos_compare":
        validation_field = "oos_validation_passed"
        numeric_validation_field = "numeric_oos_validation_passed"
    elif artifact == "xuan_b27_dplus_pair_arb_walkforward_compare":
        validation_field = "walkforward_validation_passed"
        numeric_validation_field = "numeric_walkforward_validation_passed"

    if scope_limited:
        if can_support_promotion is not False:
            failures.append("scope_limited_report_can_support_promotion_not_false")
        if artifact in {
            "xuan_b27_dplus_pair_arb_oos_compare",
            "xuan_b27_dplus_pair_arb_walkforward_compare",
        }:
            if requires_compliant_dataset is not True:
                failures.append("scope_limited_compare_missing_compliant_dataset_block")
            if data.get(validation_field) is not False:
                failures.append("scope_limited_compare_validation_not_false")
            if data.get(numeric_validation_field) is True and "DATASET_SCOPE_LIMITED" not in str(data.get("status")):
                failures.append("numeric_positive_scope_limited_status_missing")

    return {
        "path": str(path),
        "artifact": artifact,
        "status": data.get("status"),
        "audit_passed": not failures,
        "failures": failures,
        "data_root": values.get("data_root"),
        "dataset_type": dataset_type,
        "labels": values.get("labels"),
        "days": values.get("days"),
        "market_prefix": values.get("market_prefix"),
        "assets": values.get("assets"),
        "row_count": values.get("row_count"),
        "excluded_20260514_20260515": values.get("excluded_20260514_20260515"),
        "contains_20260518": values.get("contains_20260518"),
        "includes_public_account_execution_truth_v1": values.get(
            "includes_public_account_execution_truth_v1"
        ),
        "can_support_strategy_promotion": can_support_promotion,
        "requires_compliant_backtest_dataset_for_promotion": requires_compliant_dataset,
        "scope_limited": scope_limited,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", action="append", default=[])
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    manifest_paths = [Path(item) for item in args.manifest] if args.manifest else default_manifests(root)
    manifest_paths = [path if path.is_absolute() else root / path for path in manifest_paths]
    audits = [audit_one(root, path) for path in manifest_paths]
    audit_passed = bool(audits) and all(item["audit_passed"] for item in audits)
    scope_limited_count = sum(1 for item in audits if item["scope_limited"])
    promotion_supported_count = sum(
        1 for item in audits if item.get("can_support_strategy_promotion") is True
    )
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_backtest_report_data_scope_audit",
        "status": (
            "PASS_BACKTEST_REPORT_SCOPE_AUDIT"
            if audit_passed
            else "FAIL_BACKTEST_REPORT_SCOPE_AUDIT"
        ),
        "audit_passed": audit_passed,
        "audited_manifest_count": len(audits),
        "scope_limited_report_count": scope_limited_count,
        "promotion_supported_report_count": promotion_supported_count,
        "requires_compliant_backtest_dataset_for_promotion": scope_limited_count > 0,
        "required_declaration_fields": list(REQUIRED_DECLARATION_FIELDS),
        "audits": audits,
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
        "next_gate": (
            "rerun on compliant declared strict/cache/completion data or pass larger no-order shadow acceptance"
            if audit_passed and scope_limited_count > 0
            else "fix backtest report data-scope declarations before using evidence"
        ),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if audit_passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
