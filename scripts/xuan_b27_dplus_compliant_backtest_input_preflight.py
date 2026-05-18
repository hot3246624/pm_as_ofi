#!/usr/bin/env python3
"""Check whether compliant D+ backtest inputs are locally available.

This preflight is intentionally metadata-only. It checks allowed strict/cache,
completion-store, and public-account truth paths, but does not read DuckDB table
contents and never scans raw/replay paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_compliant_backtest_input_preflight"
DEFAULT_STRICT_ROOT = Path("/mnt/poly-cache/taker_buy_signal_core_v2_strict_l1")
DEFAULT_COMPLETION_ROOT = Path("/mnt/poly-verification-store/completion_unwind_event_store_v2")
DEFAULT_PUBLIC_TRUTH_ROOT = Path(
    "/mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513"
)
DEFAULT_SCOPE_LIMITED_COMPLETION_ROOT = Path(
    "/tmp/xuan_frontier_data/completion_unwind_event_store_v2"
)
DEFAULT_STRICT_LABELS = [
    "20260502_20260507",
    "20260508",
    "20260509",
    "20260510",
    "20260511",
    "20260512",
    "20260513",
    "20260516",
    "20260517",
]
DEFAULT_COMPLETION_LABELS = [
    "20260502_20260508",
    "20260509",
    "20260510",
    "20260511",
    "20260512",
    "20260513",
    "20260516",
    "20260517",
]
DEFAULT_SCOPE_LIMITED_COMPLETION_LABELS = ["20260509", "20260510", "20260511", "20260512", "20260513"]
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
FORBIDDEN_PATH_PARTS = ("raw", "replay_published", "poly-replay")


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def split_labels(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]


def path_is_safe(path: Path) -> bool:
    text = str(path)
    if "/mnt/poly-replay" in text or "replay_published" in text:
        return False
    return not any(part in FORBIDDEN_PATH_PARTS for part in path.parts)


def label_days(label: str) -> set[str]:
    return {part for part in label.replace("-", "_").split("_") if part.isdigit() and len(part) == 8}


def label_is_allowed(label: str) -> bool:
    days = label_days(label)
    return not bool(days & FORBIDDEN_DAYS) and not bool(days & NOT_READY_DAYS)


def inspect_strict_label(root: Path, label: str) -> dict[str, Any]:
    label_dir = root / label
    manifest = label_dir / "CACHE_MANIFEST.json"
    return {
        "label": label,
        "path": str(label_dir),
        "path_safe": path_is_safe(label_dir),
        "allowed_label": label_is_allowed(label),
        "forbidden_or_not_ready_days": sorted((label_days(label) & FORBIDDEN_DAYS) | (label_days(label) & NOT_READY_DAYS)),
        "exists": label_dir.exists(),
        "manifest": str(manifest),
        "manifest_exists": manifest.exists(),
        "ready": label_dir.exists() and manifest.exists() and path_is_safe(label_dir) and label_is_allowed(label),
    }


def inspect_completion_label(root: Path, label: str) -> dict[str, Any]:
    label_dir = root / label
    manifest = label_dir / "EVENT_STORE_MANIFEST.json"
    duckdb = label_dir / "event_store.duckdb"
    return {
        "label": label,
        "path": str(label_dir),
        "path_safe": path_is_safe(label_dir),
        "allowed_label": label_is_allowed(label),
        "forbidden_or_not_ready_days": sorted((label_days(label) & FORBIDDEN_DAYS) | (label_days(label) & NOT_READY_DAYS)),
        "exists": label_dir.exists(),
        "manifest": str(manifest),
        "manifest_exists": manifest.exists(),
        "event_store_duckdb": str(duckdb),
        "event_store_duckdb_exists": duckdb.exists(),
        "ready": label_dir.exists()
        and manifest.exists()
        and duckdb.exists()
        and path_is_safe(label_dir)
        and label_is_allowed(label),
    }


def inspect_public_truth(root: Path) -> dict[str, Any]:
    duckdb = root / "event_store.duckdb"
    manifest = root / "EVENT_STORE_MANIFEST.json"
    return {
        "label": root.name,
        "path": str(root),
        "path_safe": path_is_safe(root),
        "allowed_label": label_is_allowed(root.name),
        "exists": root.exists(),
        "manifest": str(manifest),
        "manifest_exists": manifest.exists(),
        "event_store_duckdb": str(duckdb),
        "event_store_duckdb_exists": duckdb.exists(),
        "table": "public_account_execution_events",
        "truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "ready": root.exists() and duckdb.exists() and path_is_safe(root) and label_is_allowed(root.name),
    }


def inspect_scope_limited_completion_label(root: Path, label: str) -> dict[str, Any]:
    label_dir = root / label
    duckdb = label_dir / "event_store.duckdb"
    return {
        "label": label,
        "path": str(label_dir),
        "path_safe": path_is_safe(label_dir),
        "allowed_label": label_is_allowed(label),
        "exists": label_dir.exists(),
        "event_store_duckdb": str(duckdb),
        "event_store_duckdb_exists": duckdb.exists(),
        "ready_for_scope_limited_research": label_dir.exists()
        and duckdb.exists()
        and path_is_safe(label_dir)
        and label_is_allowed(label),
        "can_support_strategy_promotion": False,
        "conclusion_scope": "BTC strict-V2 20260509..20260513 scope-limited research only",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-truth-root", default=str(DEFAULT_PUBLIC_TRUTH_ROOT))
    parser.add_argument("--scope-limited-completion-root", default=str(DEFAULT_SCOPE_LIMITED_COMPLETION_ROOT))
    parser.add_argument("--strict-labels", default=",".join(DEFAULT_STRICT_LABELS))
    parser.add_argument("--completion-labels", default=",".join(DEFAULT_COMPLETION_LABELS))
    parser.add_argument(
        "--scope-limited-completion-labels",
        default=",".join(DEFAULT_SCOPE_LIMITED_COMPLETION_LABELS),
    )
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    label = utc_label()
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)

    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_truth_root = Path(args.public_truth_root)
    scope_limited_completion_root = Path(args.scope_limited_completion_root)
    strict_labels = split_labels(args.strict_labels)
    completion_labels = split_labels(args.completion_labels)
    scope_limited_completion_labels = split_labels(args.scope_limited_completion_labels)
    strict = [inspect_strict_label(strict_root, item) for item in strict_labels]
    completion = [inspect_completion_label(completion_root, item) for item in completion_labels]
    public_truth = inspect_public_truth(public_truth_root)
    scope_limited_completion = [
        inspect_scope_limited_completion_label(scope_limited_completion_root, item)
        for item in scope_limited_completion_labels
    ]
    strict_ready_count = sum(1 for item in strict if item["ready"])
    completion_ready_count = sum(1 for item in completion if item["ready"])
    scope_limited_completion_ready_count = sum(
        1 for item in scope_limited_completion if item["ready_for_scope_limited_research"]
    )
    roots_ready = strict_root.exists() and completion_root.exists() and public_truth_root.exists()
    labels_ready = strict_ready_count == len(strict) and completion_ready_count == len(completion)
    preflight_passed = roots_ready and labels_ready and public_truth["ready"]
    missing_roots = [
        str(path)
        for path in (strict_root, completion_root, public_truth_root)
        if not path.exists()
    ]
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_metadata_only_compliant_backtest_input_preflight",
        "status": (
            "PASS_COMPLIANT_BACKTEST_INPUTS_AVAILABLE"
            if preflight_passed
            else "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE"
        ),
        "preflight_passed": preflight_passed,
        "can_run_compliant_backtest_locally": preflight_passed,
        "strict_root": str(strict_root),
        "completion_root": str(completion_root),
        "public_truth_root": str(public_truth_root),
        "missing_roots": missing_roots,
        "scope_limited_research_completion_root": str(scope_limited_completion_root),
        "scope_limited_research_completion_labels_expected": scope_limited_completion_labels,
        "scope_limited_research_completion_ready_label_count": scope_limited_completion_ready_count,
        "scope_limited_research_completion_available": (
            scope_limited_completion_ready_count == len(scope_limited_completion_labels)
            and bool(scope_limited_completion_labels)
        ),
        "scope_limited_research_completion_can_support_promotion": False,
        "scope_limited_research_completion_conclusion_scope": (
            "BTC strict-V2 20260509..20260513 research conclusion only; not full-data, multi-asset, account-truth, deployable, or canary-ready evidence"
        ),
        "strict_labels_expected": strict_labels,
        "completion_labels_expected": completion_labels,
        "strict_ready_label_count": strict_ready_count,
        "completion_ready_label_count": completion_ready_count,
        "strict_labels": strict,
        "completion_labels": completion,
        "public_account_execution_truth_v1": public_truth,
        "scope_limited_research_completion_labels": scope_limited_completion,
        "required_report_declaration_fields": [
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
        "normal_research_complete_days": "20260502..20260513 plus current approved 20260516 and 20260517",
        "excluded_days": sorted(FORBIDDEN_DAYS),
        "not_ready_days": sorted(NOT_READY_DAYS),
        "raw_replay_scanned": False,
        "duckdb_tables_read": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "next_gate": (
            "run compliant declared strict/cache/completion backtest or cross-check public account truth"
            if preflight_passed
            else "make compliant strict/cache/completion/public-truth stores available locally or use an approved read-only metadata discovery path"
        ),
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "duckdb_tables_read": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if preflight_passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
