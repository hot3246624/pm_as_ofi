#!/usr/bin/env python3
"""Run local D+ compliant event-store metrics over declared cache/store inputs.

This runner is intentionally read-only and no-network. It reads only local
strict V2 cache DuckDBs, completion/unwind store DuckDBs, and the public-account
audit DuckDB declared under POLY_BT_ROOT. It does not call the older SQLite
snapshot pair_arb_backtest runner.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_compliant_metrics_runner"
PREFLIGHT_ARTIFACT = "xuan_b27_dplus_compliant_backtest_input_preflight"
ADAPTER_JOIN_ARTIFACT = "xuan_b27_dplus_compliant_adapter_join_probe"
DEFAULT_POLY_BT_ROOT = Path(
    os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data")
)
DEFAULT_STRICT_ROOT = DEFAULT_POLY_BT_ROOT / "backtest_cache/taker_buy_signal_core_v2_strict_l1"
DEFAULT_COMPLETION_ROOT = DEFAULT_POLY_BT_ROOT / "verification_store/completion_unwind_event_store_v2"
DEFAULT_PUBLIC_AUDIT_DB = (
    DEFAULT_POLY_BT_ROOT
    / "verification_store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb"
)
STRICT_TABLE = "taker_buy_signal_candidates"
COMPLETION_TABLE = "completion_unwind_events"
PUBLIC_TABLE = "public_account_execution_events"
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
FORBIDDEN_PATH_PARTS = {"raw", "replay_published", "poly-replay"}
REQUIRED_DECLARATION_FIELDS = [
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
]


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


def path_is_safe(path: Path) -> bool:
    text = str(path)
    if "/mnt/poly-replay" in text or "replay_published" in text:
        return False
    return not any(part in FORBIDDEN_PATH_PARTS for part in path.parts)


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


def table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    parts = table.split(".")
    if len(parts) == 2:
        catalog_name, table_name = parts
        rows = con.execute(
            """
            select column_name
            from information_schema.columns
            where table_catalog=? and table_name=?
            """,
            [catalog_name, table_name],
        ).fetchall()
        return {str(row[0]) for row in rows}
    return {
        str(row[0])
        for row in con.execute(
            "select column_name from information_schema.columns where table_name=?",
            [table],
        ).fetchall()
    }


def table_count(db_path: Path, table: str) -> int:
    if not db_path.exists():
        return 0
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return int(con.execute(f"select count(*) from {table}").fetchone()[0] or 0)
    finally:
        con.close()


def discover_inputs(root: Path, manifest_name: str, db_name: str) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    items: list[dict[str, Any]] = []
    for manifest_path in sorted(root.glob(f"*/{manifest_name}")):
        label = manifest_path.parent.name
        manifest = read_json(manifest_path)
        days = label_days(label, manifest)
        db_path = manifest_path.parent / db_name
        ready = (
            bool(days)
            and allowed_days(days)
            and path_is_safe(manifest_path.parent)
            and db_path.exists()
        )
        if ready:
            items.append(
                {
                    "label": label,
                    "days": days,
                    "path": str(manifest_path.parent),
                    "manifest": str(manifest_path),
                    "duckdb": str(db_path),
                }
            )
    return items


def safe_preflight(preflight: dict[str, Any]) -> bool:
    side_effects = preflight.get("side_effects") or {}
    return (
        preflight.get("artifact") == PREFLIGHT_ARTIFACT
        and preflight.get("preflight_passed") is True
        and preflight.get("raw_replay_scanned") is False
        and preflight.get("duckdb_tables_read") is False
        and preflight.get("orders_sent") is False
        and preflight.get("cancels_sent") is False
        and preflight.get("redeems_sent") is False
        and preflight.get("auth_network_started") is False
        and all(value is False for value in side_effects.values())
    )


def safe_adapter_join(adapter: dict[str, Any]) -> bool:
    side_effects = adapter.get("side_effects") or {}
    return (
        adapter.get("artifact") == ADAPTER_JOIN_ARTIFACT
        and adapter.get("probe_passed") is True
        and adapter.get("raw_replay_scanned") is False
        and adapter.get("orders_sent") is False
        and adapter.get("cancels_sent") is False
        and adapter.get("redeems_sent") is False
        and adapter.get("auth_network_started") is False
        and all(value is False for value in side_effects.values())
    )


def public_audit_summary(public_audit_db: Path) -> dict[str, Any]:
    manifest_path = public_audit_db.parent / "EVENT_STORE_MANIFEST.json"
    manifest = read_json(manifest_path)
    days = label_days(public_audit_db.parent.name, manifest)
    failures: list[str] = []
    if not path_is_safe(public_audit_db):
        failures.append("unsafe_path")
    if not allowed_days(days):
        failures.append("forbidden_or_not_ready_days")
    if not manifest_path.exists():
        failures.append("missing_public_audit_manifest")
    if not public_audit_db.exists():
        failures.append("missing_public_audit_duckdb")
    row_count = 0
    market_keys: set[tuple[str, str]] = set()
    if not failures:
        con = duckdb.connect(str(public_audit_db), read_only=True)
        try:
            cols = table_columns(con, PUBLIC_TABLE)
            missing = {"day", "condition_id", "event_kind", "fill_price", "fill_qty"} - cols
            if missing:
                failures.append(f"missing_public_audit_columns:{','.join(sorted(missing))}")
            else:
                row_count = int(con.execute(f"select count(*) from {PUBLIC_TABLE}").fetchone()[0] or 0)
                for day, condition_id in con.execute(
                    f"select distinct day, condition_id from {PUBLIC_TABLE}"
                ).fetchall():
                    market_keys.add((dashed_day(str(day)), str(condition_id)))
        finally:
            con.close()
    return {
        "label": public_audit_db.parent.name,
        "days": days,
        "duckdb": str(public_audit_db),
        "ready": not failures,
        "failures": failures,
        "row_count": row_count,
        "market_day_key_count": len(market_keys),
        "truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "_keys": market_keys,
    }


def pct(num: float, den: float) -> float:
    return round(num / den, 6) if den else 0.0


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_csv_floats(text: str) -> list[float]:
    values = []
    for part in text.split(","):
        part = part.strip()
        if part:
            values.append(float(part))
    return values


def parse_csv_ints(text: str) -> list[int]:
    return [int(value) for value in parse_csv_floats(text)]


def sql_list(values: list[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_config_for_pair(
    strict_db: Path,
    completion_db: Path,
    common_days: list[str],
    clip: int,
    pair_cap: float,
    completion_window_s: float,
    first_max_price: float,
    max_candidates: int,
) -> list[dict[str, Any]]:
    buy_full_col = f"buy_full_{clip}"
    buy_vwap_col = f"buy_vwap_{clip}"
    buy_filled_col = f"buy_filled_{clip}"
    buy_worst_col = f"buy_worst_px_{clip}"
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"attach {sql_string(str(strict_db))} as strict_db (read_only)")
        con.execute(f"attach {sql_string(str(completion_db))} as completion_db (read_only)")
        strict_cols = table_columns(con, f"strict_db.{STRICT_TABLE}")
        completion_cols = table_columns(con, f"completion_db.{COMPLETION_TABLE}")
        required_strict = {
            "day",
            "slug",
            "condition_id",
            "trigger_ts_ms",
            "first_side",
            "first_l2_vwap",
            "first_l2_filled",
        }
        required_completion = {
            "day",
            "condition_id",
            "ts_ms",
            "side",
            buy_full_col,
            buy_vwap_col,
            buy_filled_col,
            buy_worst_col,
        }
        missing = sorted((required_strict - strict_cols) | (required_completion - completion_cols))
        if missing:
            raise RuntimeError(f"missing required columns: {missing}")
        limit_clause = f"limit {int(max_candidates)}" if max_candidates > 0 else ""
        days_sql = sql_list(common_days)
        query = f"""
        with strict_candidates as (
            select
                row_number() over (order by day, condition_id, trigger_ts_ms, first_side) as candidate_id,
                day,
                slug,
                condition_id,
                trigger_ts_ms,
                -trigger_ts_ms as neg_trigger_ts_ms,
                first_side,
                case when first_side = 'YES' then 'NO' else 'YES' end as completion_side,
                cast(first_l2_vwap as double) as first_price,
                {clip}.0 as first_qty
            from strict_db.{STRICT_TABLE}
            where day in ({days_sql})
              and first_side in ('YES', 'NO')
              and first_l2_vwap is not null
              and first_l2_vwap > 0
              and first_l2_vwap <= {first_max_price}
              and coalesce(first_l2_filled, 0) >= {clip}
            order by day, condition_id, trigger_ts_ms, first_side
            {limit_clause}
        ),
        completion_candidates as (
            select
                day,
                condition_id,
                ts_ms,
                -ts_ms as neg_ts_ms,
                side,
                cast({buy_vwap_col} as double) as completion_price,
                cast({buy_filled_col} as double) as completion_qty,
                cast({buy_worst_col} as double) as completion_worst_price
            from completion_db.{COMPLETION_TABLE}
            where day in ({days_sql})
              and side in ('YES', 'NO')
              and {buy_full_col} = true
              and {buy_vwap_col} is not null
              and {buy_vwap_col} > 0
              and {buy_filled_col} >= {clip}
        )
        select
            sc.candidate_id,
            sc.day,
            sc.slug,
            sc.condition_id,
            sc.trigger_ts_ms,
            sc.first_side,
            sc.completion_side,
            sc.first_price,
            sc.first_qty,
            cc.ts_ms as completion_ts_ms,
            cc.completion_price,
            cc.completion_qty,
            cc.completion_worst_price
        from strict_candidates sc
        asof left join completion_candidates cc
              on cc.day = sc.day
             and cc.condition_id = sc.condition_id
             and cc.side = sc.completion_side
             and sc.first_price + cc.completion_price <= {pair_cap}
             and sc.neg_trigger_ts_ms >= cc.neg_ts_ms
        where cc.ts_ms is null
           or cc.ts_ms <= sc.trigger_ts_ms + cast({completion_window_s * 1000.0} as bigint)
        """
        columns = [str(item[0]) for item in con.execute(query + " limit 0").description]
        return [dict(zip(columns, row)) for row in con.execute(query).fetchall()]
    finally:
        con.close()


def summarize_rows(rows: list[dict[str, Any]], fee_rate: float) -> dict[str, Any]:
    candidate_count = len(rows)
    paired_rows = [row for row in rows if row.get("completion_ts_ms") is not None]
    market_keys = {(row.get("day"), row.get("condition_id")) for row in rows}
    paired_qty = 0.0
    residual_qty = 0.0
    first_cost = 0.0
    completion_cost = 0.0
    pair_cost_qty = 0.0
    pair_gross_pnl = 0.0
    daily: dict[str, dict[str, float]] = {}
    for row in rows:
        day = str(row.get("day"))
        day_metrics = daily.setdefault(
            day,
            {
                "candidate_count": 0.0,
                "paired_count": 0.0,
                "pair_qty": 0.0,
                "residual_qty": 0.0,
                "first_cost": 0.0,
                "completion_cost": 0.0,
                "pair_gross_pnl": 0.0,
            },
        )
        qty = safe_float(row.get("first_qty"))
        first_price = safe_float(row.get("first_price"))
        day_metrics["candidate_count"] += 1
        first_cost += first_price * qty
        day_metrics["first_cost"] += first_price * qty
        if row.get("completion_ts_ms") is None:
            residual_qty += qty
            day_metrics["residual_qty"] += qty
            continue
        completion_price = safe_float(row.get("completion_price"))
        pair_qty = qty
        paired_qty += pair_qty
        completion_cost += completion_price * pair_qty
        pair_cost_qty += (first_price + completion_price) * pair_qty
        pnl = (1.0 - first_price - completion_price) * pair_qty
        pair_gross_pnl += pnl
        day_metrics["paired_count"] += 1
        day_metrics["pair_qty"] += pair_qty
        day_metrics["completion_cost"] += completion_price * pair_qty
        day_metrics["pair_gross_pnl"] += pnl
    residual_cost = 0.0
    for row in rows:
        if row.get("completion_ts_ms") is None:
            residual_cost += safe_float(row.get("first_price")) * safe_float(row.get("first_qty"))
    fee = fee_rate * (first_cost + completion_cost)
    fee_worst_case_pnl = pair_gross_pnl - residual_cost - fee
    for item in daily.values():
        item["residual_cost"] = 0.0
        item["fee"] = fee_rate * (item["first_cost"] + item["completion_cost"])
        item["fee_worst_case_pnl"] = item["pair_gross_pnl"] - item["fee"]
    for row in rows:
        if row.get("completion_ts_ms") is None:
            day = str(row.get("day"))
            cost = safe_float(row.get("first_price")) * safe_float(row.get("first_qty"))
            daily[day]["residual_cost"] += cost
            daily[day]["fee_worst_case_pnl"] -= cost
    worst_day = min((item["fee_worst_case_pnl"] for item in daily.values()), default=0.0)
    return {
        "candidate_count": candidate_count,
        "market_count": len(market_keys),
        "paired_candidate_count": len(paired_rows),
        "unpaired_candidate_count": candidate_count - len(paired_rows),
        "completion_rate": pct(len(paired_rows), candidate_count),
        "pair_qty": round(paired_qty, 6),
        "residual_qty": round(residual_qty, 6),
        "qty_residual_rate": pct(residual_qty, paired_qty + residual_qty),
        "first_cost": round(first_cost, 6),
        "completion_cost": round(completion_cost, 6),
        "residual_cost": round(residual_cost, 6),
        "fee": round(fee, 6),
        "pair_gross_pnl": round(pair_gross_pnl, 6),
        "fee_worst_case_pnl": round(fee_worst_case_pnl, 6),
        "stress100_worst_pnl": round(pair_gross_pnl - residual_cost - 0.01 * (first_cost + completion_cost), 6),
        "net_pair_cost_wavg": round(pair_cost_qty / paired_qty, 6) if paired_qty else None,
        "fee_worst_case_roi": pct(fee_worst_case_pnl, first_cost + completion_cost),
        "worst_day_fee_worst_case_pnl": round(worst_day, 6),
        "positive_daily_count": sum(1 for item in daily.values() if item["fee_worst_case_pnl"] > 0),
        "day_count": len(daily),
        "daily": {
            day: {key: round(value, 6) for key, value in metrics.items()}
            for day, metrics in sorted(daily.items())
        },
    }


def build_data_declaration(
    strict_inputs: list[dict[str, Any]],
    completion_inputs: list[dict[str, Any]],
    public_audit: dict[str, Any],
    row_count: int,
    used_days: list[str],
) -> dict[str, Any]:
    strict_labels = [item["label"] for item in strict_inputs]
    completion_labels = [item["label"] for item in completion_inputs]
    public_days = set(public_audit.get("days") or [])
    missing_public_days = sorted(set(used_days) - public_days)
    return {
        "data_root": str(DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_metrics",
        "labels": {
            "strict": strict_labels,
            "completion": completion_labels,
            "public_audit": public_audit.get("label"),
        },
        "days": used_days,
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": row_count,
        "excluded_20260514_20260515": True,
        "contains_20260514_20260515": False,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": public_audit.get("ready") is True,
        "public_account_truth_level": public_audit.get("truth_level"),
        "public_account_truth_missing_days": missing_public_days,
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "conclusion_scope": (
            "local read-only strict/cache plus completion-store event metrics; public-account audit is proxy truth "
            "and may be partial, so this is not deployable or source-of-truth replay validation"
        ),
        "required_report_declaration_fields": REQUIRED_DECLARATION_FIELDS,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-preflight", required=True)
    parser.add_argument("--adapter-join-probe", required=True)
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--output-dir")
    parser.add_argument("--clips", default="10,25,60")
    parser.add_argument("--pair-caps", default="0.94,0.96,0.98")
    parser.add_argument("--completion-window-s", type=float, default=30.0)
    parser.add_argument("--first-max-price", type=float, default=0.60)
    parser.add_argument("--fee-rate", type=float, default=0.0283)
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=1000,
        help="Per strict/completion label-pair cap. Use 0 only for a deliberate full scan.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    label = utc_label()
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    preflight = read_json(Path(args.input_preflight))
    adapter_join = read_json(Path(args.adapter_join_probe))
    strict_inputs = discover_inputs(strict_root, "CACHE_MANIFEST.json", "cache.duckdb")
    completion_inputs = discover_inputs(completion_root, "EVENT_STORE_MANIFEST.json", "event_store.duckdb")
    public_audit = public_audit_summary(public_audit_db)
    input_safe = (
        safe_preflight(preflight)
        and safe_adapter_join(adapter_join)
        and bool(strict_inputs)
        and bool(completion_inputs)
        and public_audit.get("ready") is True
    )
    all_days = sorted(
        set().union(*(set(item["days"]) for item in strict_inputs), *(set(item["days"]) for item in completion_inputs))
        if strict_inputs and completion_inputs
        else set()
    )
    row_count = int(adapter_join.get("row_count") or 0)
    data_declaration = build_data_declaration(
        strict_inputs,
        completion_inputs,
        public_audit,
        row_count,
        all_days,
    )
    rows_by_config: list[dict[str, Any]] = []
    if input_safe:
        for clip in parse_csv_ints(args.clips):
            for pair_cap in parse_csv_floats(args.pair_caps):
                collected_rows: list[dict[str, Any]] = []
                for strict_item in strict_inputs:
                    strict_days = set(strict_item["days"])
                    for completion_item in completion_inputs:
                        common_days = sorted(strict_days & set(completion_item["days"]))
                        if not common_days:
                            continue
                        collected_rows.extend(
                            run_config_for_pair(
                                Path(strict_item["duckdb"]),
                                Path(completion_item["duckdb"]),
                                common_days,
                                clip,
                                pair_cap,
                                args.completion_window_s,
                                args.first_max_price,
                                args.max_candidates,
                            )
                        )
                summary = summarize_rows(collected_rows, args.fee_rate)
                row = {
                    "kind": "pair_arb_backtest_run",
                    "run": len(rows_by_config) + 1,
                    "config": {
                        "runner": ARTIFACT,
                        "clip": clip,
                        "pair_cap": pair_cap,
                        "completion_window_s": args.completion_window_s,
                        "first_max_price": args.first_max_price,
                        "fee_rate": args.fee_rate,
                        "max_candidates_per_label_pair": args.max_candidates,
                        "candidate_model": "strict_first_leg_then_future_opposite_completion_event",
                    },
                    "metrics": summary,
                }
                rows_by_config.append(row)
    metrics_path = output_dir / "metrics.jsonl"
    metrics_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows_by_config)
    )
    nonzero = [row for row in rows_by_config if row["metrics"]["candidate_count"] > 0]
    positive = [row for row in nonzero if row["metrics"]["fee_worst_case_pnl"] > 0]
    best = max(
        nonzero,
        key=lambda row: (
            safe_float(row["metrics"]["fee_worst_case_pnl"]),
            -safe_float(row["metrics"]["qty_residual_rate"]),
        ),
        default=None,
    )
    low_residual_positive = [
        row
        for row in positive
        if row["metrics"]["qty_residual_rate"] <= 0.05
        and row["metrics"]["worst_day_fee_worst_case_pnl"] >= 0
    ]
    status = (
        "BLOCKED_COMPLIANT_METRICS_INPUT_GAP"
        if not input_safe
        else "KEEP_DESCRIPTIVE_METRICS_TARGET"
        if low_residual_positive
        else "UNKNOWN_COMPLIANT_METRICS_RESEARCH_ONLY"
        if positive
        else "DISCARD_NO_FEE_POSITIVE_COMPLIANT_METRICS"
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_declared_cache_store_compliant_metrics",
        "status": status,
        "input_safe": input_safe,
        "input_preflight_path": str(Path(args.input_preflight)),
        "input_preflight_status": preflight.get("status"),
        "adapter_join_probe_path": str(Path(args.adapter_join_probe)),
        "adapter_join_probe_status": adapter_join.get("status"),
        "adapter_join_probe_safe": safe_adapter_join(adapter_join),
        "backtest_data_declaration": data_declaration,
        "data_root": data_declaration["data_root"],
        "dataset_type": data_declaration["dataset_type"],
        "labels": data_declaration["labels"],
        "days": data_declaration["days"],
        "market_prefix": data_declaration["market_prefix"],
        "assets": data_declaration["assets"],
        "row_count": data_declaration["row_count"],
        "excluded_20260514_20260515": data_declaration["excluded_20260514_20260515"],
        "contains_20260518": data_declaration["contains_20260518"],
        "includes_public_account_execution_truth_v1": data_declaration[
            "includes_public_account_execution_truth_v1"
        ],
        "public_account_truth_level": data_declaration["public_account_truth_level"],
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "run_count": len(rows_by_config),
        "max_candidates_per_label_pair": args.max_candidates,
        "full_scan": args.max_candidates == 0,
        "candidate_sample_limited": args.max_candidates > 0,
        "nonzero_candidate_run_count": len(nonzero),
        "positive_fee_worst_case_run_count": len(positive),
        "low_residual_positive_run_count": len(low_residual_positive),
        "best_run": best,
        "metrics_jsonl": str(metrics_path),
        "strict_inputs": strict_inputs,
        "completion_inputs": completion_inputs,
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
            "inspect low-residual positive configs and add causal residual reducer"
            if low_residual_positive
            else "use this compliant runner as the metrics substrate; do not promote until residual/worst-day and public-audit scope clear"
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0 if input_safe else 2


if __name__ == "__main__":
    raise SystemExit(main())
