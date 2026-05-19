#!/usr/bin/env python3
"""Probe strict-cache/completion/public-audit join feasibility for D+.

This is a local read-only adapter feasibility probe. It reads only declared
POLY_BT_ROOT cache/store DuckDB files and manifests. It does not scan raw/replay
paths, does not start network processes, and does not simulate or place orders.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_compliant_adapter_join_probe"
DEFAULT_POLY_BT_ROOT = Path(
    os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data")
)
DEFAULT_STRICT_ROOT = DEFAULT_POLY_BT_ROOT / "backtest_cache/taker_buy_signal_core_v2_strict_l1"
DEFAULT_COMPLETION_ROOT = DEFAULT_POLY_BT_ROOT / "verification_store/completion_unwind_event_store_v2"
DEFAULT_PUBLIC_AUDIT_DB = (
    DEFAULT_POLY_BT_ROOT
    / "verification_store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb"
)
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
FORBIDDEN_PATH_PARTS = {"raw", "replay_published", "poly-replay"}
STRICT_TABLE = "taker_buy_signal_candidates"
COMPLETION_TABLE = "completion_unwind_events"
PUBLIC_TABLE = "public_account_execution_events"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


def day_token(day: str) -> str:
    return day.replace("-", "")


def dashed_day(day: str) -> str:
    clean = day_token(day)
    if len(clean) == 8 and clean.isdigit():
        return f"{clean[0:4]}-{clean[4:6]}-{clean[6:8]}"
    return day


def label_days(label: str, manifest: dict[str, Any]) -> list[str]:
    days = manifest.get("days") or []
    if days:
        return sorted({dashed_day(str(day)) for day in days})
    parts = [part for part in label.replace("-", "_").split("_") if len(part) == 8 and part.isdigit()]
    return sorted({dashed_day(part) for part in parts})


def allowed_days(days: list[str]) -> bool:
    clean = {day_token(day) for day in days}
    return not bool(clean & FORBIDDEN_DAYS) and not bool(clean & NOT_READY_DAYS)


def discover_labels(root: Path, manifest_name: str) -> list[str]:
    labels: list[str] = []
    if not root.exists():
        return labels
    for manifest_path in root.glob(f"*/{manifest_name}"):
        manifest = read_json(manifest_path)
        days = label_days(manifest_path.parent.name, manifest)
        if days and allowed_days(days):
            labels.append(manifest_path.parent.name)
    return sorted(labels)


def table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            "select column_name from information_schema.columns where table_name=?",
            [table],
        ).fetchall()
    }


def strict_probe(root: Path, label: str) -> dict[str, Any]:
    label_dir = root / label
    manifest_path = label_dir / "CACHE_MANIFEST.json"
    db_path = label_dir / "cache.duckdb"
    manifest = read_json(manifest_path)
    days = label_days(label, manifest)
    failures: list[str] = []
    if not path_is_safe(label_dir):
        failures.append("unsafe_path")
    if not allowed_days(days):
        failures.append("forbidden_or_not_ready_days")
    if not manifest_path.exists():
        failures.append("missing_cache_manifest")
    if not db_path.exists():
        failures.append("missing_cache_duckdb")
    row_count = 0
    keys: set[tuple[str, str]] = set()
    day_counts: dict[str, int] = {}
    required_columns = {"day", "condition_id", "slug", "trigger_ts_ms", "first_side", "strict_l1_immediate_pair"}
    missing_columns: list[str] = []
    if not failures:
        con = duckdb.connect(str(db_path), read_only=True)
        cols = table_columns(con, STRICT_TABLE)
        missing_columns = sorted(required_columns - cols)
        if missing_columns:
            failures.append("missing_required_strict_columns")
        else:
            row_count = int(con.execute(f"select count(*) from {STRICT_TABLE}").fetchone()[0] or 0)
            for day, condition_id, count in con.execute(
                f"""
                select day, condition_id, count(*)
                from {STRICT_TABLE}
                group by day, condition_id
                """
            ).fetchall():
                d = dashed_day(str(day))
                keys.add((d, str(condition_id)))
                day_counts[d] = day_counts.get(d, 0) + int(count or 0)
        con.close()
    return {
        "label": label,
        "days": days,
        "path": str(label_dir),
        "manifest": str(manifest_path),
        "duckdb": str(db_path),
        "ready": not failures,
        "failures": failures,
        "missing_required_columns": missing_columns,
        "row_count": row_count,
        "market_day_key_count": len(keys),
        "day_counts": day_counts,
        "_keys": keys,
    }


def completion_probe(root: Path, label: str) -> dict[str, Any]:
    label_dir = root / label
    manifest_path = label_dir / "EVENT_STORE_MANIFEST.json"
    db_path = label_dir / "event_store.duckdb"
    manifest = read_json(manifest_path)
    days = label_days(label, manifest)
    failures: list[str] = []
    if not path_is_safe(label_dir):
        failures.append("unsafe_path")
    if not allowed_days(days):
        failures.append("forbidden_or_not_ready_days")
    if not manifest_path.exists():
        failures.append("missing_event_store_manifest")
    if not db_path.exists():
        failures.append("missing_event_store_duckdb")
    row_count = 0
    keys: set[tuple[str, str]] = set()
    day_counts: dict[str, int] = {}
    required_columns = {"day", "condition_id", "event_kind", "ts_ms", "side", "winner_side", "l1_pair_ask"}
    missing_columns: list[str] = []
    if not failures:
        con = duckdb.connect(str(db_path), read_only=True)
        cols = table_columns(con, COMPLETION_TABLE)
        missing_columns = sorted(required_columns - cols)
        if missing_columns:
            failures.append("missing_required_completion_columns")
        else:
            row_count = int(con.execute(f"select count(*) from {COMPLETION_TABLE}").fetchone()[0] or 0)
            for day, condition_id, count in con.execute(
                f"""
                select day, condition_id, count(*)
                from {COMPLETION_TABLE}
                group by day, condition_id
                """
            ).fetchall():
                d = dashed_day(str(day))
                keys.add((d, str(condition_id)))
                day_counts[d] = day_counts.get(d, 0) + int(count or 0)
        con.close()
    return {
        "label": label,
        "days": days,
        "path": str(label_dir),
        "manifest": str(manifest_path),
        "duckdb": str(db_path),
        "ready": not failures,
        "failures": failures,
        "missing_required_columns": missing_columns,
        "row_count": row_count,
        "market_day_key_count": len(keys),
        "day_counts": day_counts,
        "_keys": keys,
    }


def public_audit_probe(db_path: Path) -> dict[str, Any]:
    manifest_path = db_path.parent / "EVENT_STORE_MANIFEST.json"
    manifest = read_json(manifest_path)
    days = label_days(db_path.parent.name, manifest)
    failures: list[str] = []
    if not path_is_safe(db_path):
        failures.append("unsafe_path")
    if not allowed_days(days):
        failures.append("forbidden_or_not_ready_days")
    if not manifest_path.exists():
        failures.append("missing_public_audit_manifest")
    if not db_path.exists():
        failures.append("missing_public_audit_duckdb")
    row_count = 0
    keys: set[tuple[str, str]] = set()
    day_counts: dict[str, int] = {}
    event_kind_counts: dict[str, int] = {}
    required_columns = {"day", "condition_id", "slug", "event_kind", "fill_price", "fill_qty"}
    missing_columns: list[str] = []
    if not failures:
        con = duckdb.connect(str(db_path), read_only=True)
        cols = table_columns(con, PUBLIC_TABLE)
        missing_columns = sorted(required_columns - cols)
        if missing_columns:
            failures.append("missing_required_public_audit_columns")
        else:
            row_count = int(con.execute(f"select count(*) from {PUBLIC_TABLE}").fetchone()[0] or 0)
            for day, condition_id, count in con.execute(
                f"""
                select day, condition_id, count(*)
                from {PUBLIC_TABLE}
                group by day, condition_id
                """
            ).fetchall():
                d = dashed_day(str(day))
                keys.add((d, str(condition_id)))
                day_counts[d] = day_counts.get(d, 0) + int(count or 0)
            event_kind_counts = {
                str(kind): int(count or 0)
                for kind, count in con.execute(
                    f"select event_kind, count(*) from {PUBLIC_TABLE} group by event_kind order by event_kind"
                ).fetchall()
            }
        con.close()
    return {
        "label": db_path.parent.name,
        "days": days,
        "path": str(db_path.parent),
        "manifest": str(manifest_path),
        "duckdb": str(db_path),
        "ready": not failures,
        "failures": failures,
        "missing_required_columns": missing_columns,
        "row_count": row_count,
        "market_day_key_count": len(keys),
        "day_counts": day_counts,
        "event_kind_counts": event_kind_counts,
        "truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "_keys": keys,
    }


def strip_private_keys(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for item in items:
        copy = dict(item)
        copy.pop("_keys", None)
        output.append(copy)
    return output


def pct(num: int, den: int) -> float:
    return round(num / den, 6) if den else 0.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    label = utc_label()
    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    strict_labels = discover_labels(strict_root, "CACHE_MANIFEST.json")
    completion_labels = discover_labels(completion_root, "EVENT_STORE_MANIFEST.json")
    strict = [strict_probe(strict_root, item) for item in strict_labels]
    completion = [completion_probe(completion_root, item) for item in completion_labels]
    public_audit = public_audit_probe(public_audit_db)

    strict_keys = set().union(*(item["_keys"] for item in strict)) if strict else set()
    completion_keys = set().union(*(item["_keys"] for item in completion)) if completion else set()
    public_keys = public_audit.get("_keys") or set()
    strict_days = sorted({day for day, _ in strict_keys})
    completion_days = sorted({day for day, _ in completion_keys})
    public_days = sorted({day for day, _ in public_keys})
    strict_completion_overlap = strict_keys & completion_keys
    strict_public_overlap = strict_keys & public_keys
    strict_days_missing_completion = sorted(set(strict_days) - set(completion_days))
    strict_days_missing_public_audit = sorted(set(strict_days) - set(public_days))
    completion_days_extra_vs_strict = sorted(set(completion_days) - set(strict_days))
    all_inputs_ready = (
        bool(strict)
        and bool(completion)
        and all(item["ready"] for item in strict)
        and all(item["ready"] for item in completion)
        and public_audit["ready"]
    )
    strict_completion_join_feasible = all_inputs_ready and not strict_days_missing_completion
    public_audit_full_coverage = all_inputs_ready and not strict_days_missing_public_audit
    status = (
        "PASS_STRICT_COMPLETION_JOIN_FEASIBLE_PUBLIC_AUDIT_PARTIAL"
        if strict_completion_join_feasible and not public_audit_full_coverage
        else "PASS_COMPLIANT_ADAPTER_JOIN_FEASIBLE"
        if strict_completion_join_feasible and public_audit_full_coverage
        else "BLOCKED_COMPLIANT_ADAPTER_JOIN_INPUT_GAP"
    )
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_read_only_compliant_adapter_join_feasibility_probe",
        "status": status,
        "probe_passed": strict_completion_join_feasible,
        "data_root": str(DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_probe",
        "labels": {
            "strict": strict_labels,
            "completion": completion_labels,
            "public_audit": public_audit.get("label"),
        },
        "days": sorted(set(strict_days) | set(completion_days) | set(public_days)),
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": sum(int(item["row_count"]) for item in strict)
        + sum(int(item["row_count"]) for item in completion)
        + int(public_audit.get("row_count") or 0),
        "strict_row_count": sum(int(item["row_count"]) for item in strict),
        "completion_row_count": sum(int(item["row_count"]) for item in completion),
        "public_audit_row_count": int(public_audit.get("row_count") or 0),
        "strict_market_day_key_count": len(strict_keys),
        "completion_market_day_key_count": len(completion_keys),
        "public_audit_market_day_key_count": len(public_keys),
        "strict_completion_overlap_key_count": len(strict_completion_overlap),
        "strict_completion_overlap_rate": pct(len(strict_completion_overlap), len(strict_keys)),
        "strict_public_audit_overlap_key_count": len(strict_public_overlap),
        "strict_public_audit_overlap_rate": pct(len(strict_public_overlap), len(strict_keys)),
        "strict_completion_join_feasible": strict_completion_join_feasible,
        "public_audit_full_strict_day_coverage": public_audit_full_coverage,
        "strict_days": strict_days,
        "completion_days": completion_days,
        "public_audit_days": public_days,
        "strict_days_missing_completion": strict_days_missing_completion,
        "strict_days_missing_public_audit": strict_days_missing_public_audit,
        "completion_days_extra_vs_strict": completion_days_extra_vs_strict,
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": public_audit["ready"],
        "public_account_truth_level": public_audit.get("truth_level"),
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "conclusion_scope": (
            "adapter join feasibility only; not strategy PnL, private owner-trade truth, "
            "or source-of-truth replay validation"
        ),
        "strict_inputs": strip_private_keys(strict),
        "completion_inputs": strip_private_keys(completion),
        "public_audit": {k: v for k, v in public_audit.items() if k != "_keys"},
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
        "next_gate": (
            "implement strict-cache plus completion adapter for days with public-audit coverage first; "
            "treat 2026-05-16/17 public-audit coverage as missing unless a matching audit store appears"
        ),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if strict_completion_join_feasible else 2


if __name__ == "__main__":
    raise SystemExit(main())
