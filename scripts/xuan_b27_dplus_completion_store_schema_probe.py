#!/usr/bin/env python3
"""Probe completion store schema/coverage for D+ research.

This is a local no-order research probe. It reads only declared completion
store DuckDB files, never raw/replay paths, and explicitly marks the output as
completion-only evidence until it is joined with strict-cache/public-truth
inputs.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_completion_store_schema_probe"
DEFAULT_POLY_BT_ROOT = Path(
    os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data")
)
DEFAULT_ROOT = DEFAULT_POLY_BT_ROOT / "verification_store/completion_unwind_event_store_v2"
SCOPE_LIMITED_ROOT = Path("/tmp/xuan_frontier_data/completion_unwind_event_store_v2")
DEFAULT_LABELS = ""
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
EXPECTED_TABLE = "completion_unwind_events"
REQUIRED_COLUMNS = [
    "day",
    "event_kind",
    "event_id",
    "ts_ms",
    "condition_id",
    "offset_s",
    "side",
    "winner_side",
    "side_bid",
    "side_ask",
    "opp_bid",
    "opp_ask",
    "l1_pair_ask",
    "l1_pair_bid",
    "public_trade_taker_side",
    "public_trade_price",
    "public_trade_size",
]
FORBIDDEN_PATH_PARTS = {"raw", "replay_published", "poly-replay"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def split_labels(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]


def label_days(label: str) -> set[str]:
    return {part for part in label.replace("-", "_").split("_") if part.isdigit() and len(part) == 8}


def label_is_allowed(label: str) -> bool:
    days = label_days(label)
    return not bool(days & FORBIDDEN_DAYS) and not bool(days & NOT_READY_DAYS)


def discover_labels(root: Path) -> list[str]:
    if not root.exists():
        return []
    labels = []
    for manifest in root.glob("*/EVENT_STORE_MANIFEST.json"):
        label = manifest.parent.name
        if label_is_allowed(label):
            labels.append(label)
    return sorted(labels)


def path_is_safe(path: Path) -> bool:
    text = str(path)
    if "/mnt/poly-replay" in text or "replay_published" in text:
        return False
    return not any(part in FORBIDDEN_PATH_PARTS for part in path.parts)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def dashed_day(day: str) -> str:
    clean = day.replace("-", "")
    if len(clean) == 8 and clean.isdigit():
        return f"{clean[0:4]}-{clean[4:6]}-{clean[6:8]}"
    return day


def expected_days_for_label(label: str, manifest: dict[str, Any]) -> set[str]:
    days = manifest.get("days") or []
    if days:
        return {str(day) for day in days} | {str(day).replace("-", "") for day in days}
    return set(label_days(label)) | {dashed_day(day) for day in label_days(label)}


def root_dataset_type(root: Path) -> str:
    if root == SCOPE_LIMITED_ROOT:
        return "scope_limited_completion_unwind_event_store_v2"
    if str(root).startswith(str(DEFAULT_POLY_BT_ROOT)):
        return "local_poly_backtest_completion_unwind_event_store_v2"
    return "local_completion_unwind_event_store_v2"


def query_one(db_path: Path, label: str) -> dict[str, Any]:
    label_dir = db_path.parent
    manifest_path = label_dir / "EVENT_STORE_MANIFEST.json"
    manifest = read_json(manifest_path)
    if not db_path.exists():
        return {
            "label": label,
            "path": str(db_path),
            "exists": False,
            "path_safe": path_is_safe(db_path),
            "manifest": str(manifest_path),
            "manifest_exists": manifest_path.exists(),
            "allowed_label": label_is_allowed(label),
            "ready": False,
            "failures": ["missing_event_store_duckdb"],
        }
    if not path_is_safe(db_path):
        return {
            "label": label,
            "path": str(db_path),
            "exists": True,
            "path_safe": False,
            "manifest": str(manifest_path),
            "manifest_exists": manifest_path.exists(),
            "allowed_label": label_is_allowed(label),
            "ready": False,
            "failures": ["unsafe_path"],
        }
    failures: list[str] = []
    if not label_is_allowed(label):
        failures.append("forbidden_or_not_ready_label")
    con = duckdb.connect(str(db_path), read_only=True)
    tables = {
        row[0]
        for row in con.execute(
            "select table_name from information_schema.tables where table_schema='main'"
        ).fetchall()
    }
    if EXPECTED_TABLE not in tables:
        failures.append("missing_completion_unwind_events_table")
        columns: list[dict[str, str]] = []
        column_names: set[str] = set()
        coverage = {}
    else:
        columns = [
            {"name": str(row[0]), "type": str(row[1])}
            for row in con.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where table_name = ?
                order by ordinal_position
                """,
                [EXPECTED_TABLE],
            ).fetchall()
        ]
        column_names = {item["name"] for item in columns}
        missing_columns = [name for name in REQUIRED_COLUMNS if name not in column_names]
        if missing_columns:
            failures.append("missing_required_columns")
        row = con.execute(
            f"""
            select
              count(*) as row_count,
              count(distinct day) as day_count,
              min(day) as min_day,
              max(day) as max_day,
              count(distinct condition_id) as market_count,
              min(ts_ms) as min_ts_ms,
              max(ts_ms) as max_ts_ms,
              sum(case when event_kind = 'l1_price_change' then 1 else 0 end) as l1_price_change_rows,
              sum(case when event_kind = 'public_trade' then 1 else 0 end) as public_trade_rows,
              sum(case when event_kind not in ('l1_price_change','public_trade') then 1 else 0 end) as other_event_kind_rows
            from {EXPECTED_TABLE}
            """
        ).fetchone()
        coverage = {
            "row_count": int(row[0] or 0),
            "day_count": int(row[1] or 0),
            "min_day": row[2],
            "max_day": row[3],
            "market_count": int(row[4] or 0),
            "min_ts_ms": int(row[5] or 0),
            "max_ts_ms": int(row[6] or 0),
            "l1_price_change_rows": int(row[7] or 0),
            "public_trade_rows": int(row[8] or 0),
            "other_event_kind_rows": int(row[9] or 0),
        }
        if coverage["row_count"] <= 0:
            failures.append("empty_completion_store")
        allowed_day_values = expected_days_for_label(label, manifest)
        if (
            not allowed_day_values
            or coverage["day_count"] > len({dashed_day(day) for day in allowed_day_values})
            or coverage["min_day"] not in allowed_day_values
            or coverage["max_day"] not in allowed_day_values
        ):
            failures.append("unexpected_day_coverage")
        if coverage["l1_price_change_rows"] <= 0 or coverage["public_trade_rows"] <= 0:
            failures.append("missing_required_event_kinds")
    con.close()
    return {
        "label": label,
        "path": str(db_path),
        "exists": True,
        "path_safe": True,
        "manifest": str(manifest_path),
        "manifest_exists": manifest_path.exists(),
        "allowed_label": label_is_allowed(label),
        "expected_days": sorted(expected_days_for_label(label, manifest)),
        "ready": not failures,
        "failures": failures,
        "table": EXPECTED_TABLE,
        "table_exists": EXPECTED_TABLE in tables,
        "column_count": len(columns),
        "columns": columns,
        "required_columns": REQUIRED_COLUMNS,
        "missing_required_columns": [name for name in REQUIRED_COLUMNS if name not in column_names],
        **coverage,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--labels", default=DEFAULT_LABELS)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    label = utc_label()
    root = Path(args.root)
    labels = split_labels(args.labels) or discover_labels(root)
    probes = [query_one(root / item / "event_store.duckdb", item) for item in labels]
    passed = bool(probes) and all(item.get("ready") is True for item in probes)
    total_rows = sum(int(item.get("row_count") or 0) for item in probes)
    total_markets = sum(int(item.get("market_count") or 0) for item in probes)
    dataset_type = root_dataset_type(root)
    can_support_strategy_promotion = False
    requires_compliant_join = True
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_completion_store_schema_probe",
        "status": (
            "PASS_SCOPE_LIMITED_COMPLETION_STORE_SCHEMA_PROBE"
            if passed and dataset_type == "scope_limited_completion_unwind_event_store_v2"
            else "PASS_LOCAL_COMPLETION_STORE_SCHEMA_PROBE"
            if passed
            else "FAIL_COMPLETION_STORE_SCHEMA_PROBE"
        ),
        "probe_passed": passed,
        "data_root": str(root),
        "dataset_type": dataset_type,
        "labels": labels,
        "days": sorted(
            {
                dashed_day(str(day))
                for probe in probes
                for day in (probe.get("expected_days") or [])
                if len(str(day).replace("-", "")) == 8
            }
        ),
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": total_rows,
        "market_count_sum_by_day": total_markets,
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": False,
        "can_support_strategy_promotion": can_support_strategy_promotion,
        "requires_compliant_backtest_dataset_for_promotion": requires_compliant_join,
        "conclusion_scope": (
            "local completion-store event-layer schema/coverage only; promotion still requires strict-cache join, "
            "public-account audit/proxy-truth cross-check, and final source-of-truth replay validation"
        ),
        "table": EXPECTED_TABLE,
        "required_columns": REQUIRED_COLUMNS,
        "probes": probes,
        "raw_replay_scanned": False,
        "duckdb_tables_read": True,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
        "next_gate": "use this only for scope-limited adapter research; promotion still requires declared strict/cache/completion/public-truth inputs",
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
