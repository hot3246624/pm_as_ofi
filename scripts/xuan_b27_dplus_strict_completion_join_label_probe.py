#!/usr/bin/env python3
"""Probe row-level strict-cache to completion-store label alignment for D+.

This local read-only probe recomputes a narrow 30s completion label for strict
taker-buy candidates by joining strict cache rows to completion_unwind events on
day/condition_id/time window. It is an adapter sanity check, not strategy PnL or
promotion evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_strict_completion_join_label_probe"
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


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def split_labels(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]


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


def discover(root: Path, manifest_name: str, explicit: str) -> dict[str, dict[str, Any]]:
    labels = split_labels(explicit)
    if not labels and root.exists():
        labels = sorted(path.parent.name for path in root.glob(f"*/{manifest_name}"))
    out: dict[str, dict[str, Any]] = {}
    for label in labels:
        label_dir = root / label
        manifest_path = label_dir / manifest_name
        manifest = read_json(manifest_path)
        days = label_days(label, manifest)
        if days and allowed_days(days):
            out[label] = {
                "label": label,
                "path": str(label_dir),
                "manifest": str(manifest_path),
                "days": days,
                "manifest_exists": manifest_path.exists(),
                "path_safe": path_is_safe(label_dir),
            }
    return out


def sql_list(items: list[str]) -> str:
    return ", ".join("'" + item.replace("'", "''") + "'" for item in items)


def finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def public_audit_days(db_path: Path) -> tuple[list[str], dict[str, int]]:
    if not db_path.exists() or not path_is_safe(db_path):
        return [], {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            f"select day, count(*) from {PUBLIC_TABLE} group by day order by day"
        ).fetchall()
    finally:
        con.close()
    day_counts = {dashed_day(str(day)): int(count or 0) for day, count in rows}
    return sorted(day_counts), day_counts


def run_one(
    strict_root: Path,
    completion_root: Path,
    strict_label: str,
    completion_label: str,
    days: list[str],
    window_ms: int,
    event_kind_filter: str,
) -> dict[str, Any]:
    strict_db = strict_root / strict_label / "cache.duckdb"
    completion_db = completion_root / completion_label / "event_store.duckdb"
    failures: list[str] = []
    if not path_is_safe(strict_db) or not path_is_safe(completion_db):
        failures.append("unsafe_path")
    if not strict_db.exists():
        failures.append("missing_strict_cache_duckdb")
    if not completion_db.exists():
        failures.append("missing_completion_duckdb")
    if failures:
        return {
            "strict_label": strict_label,
            "completion_label": completion_label,
            "days": days,
            "ready": False,
            "failures": failures,
        }

    con = duckdb.connect(":memory:")
    con.execute(f"attach '{strict_db}' as strict_db (read_only)")
    con.execute(f"attach '{completion_db}' as completion_db (read_only)")
    days_sql = sql_list(days)
    event_filter_sql = ""
    if event_kind_filter != "all":
        event_filter_sql = " and c.event_kind = '" + event_kind_filter.replace("'", "''") + "'"
    query = f"""
    with strict_candidates as (
      select
        row_number() over () as candidate_id,
        day,
        condition_id,
        slug,
        cast(trigger_ts_ms as bigint) as trigger_ts_ms,
        first_side,
        cast(first_l2_vwap as double) as first_l2_vwap,
        cast(min_pair_cost_30s as double) as cache_min_pair_cost_30s,
        cast(strict_l1_immediate_pair as double) as strict_l1_immediate_pair
      from strict_db.{STRICT_TABLE}
      where day in ({days_sql})
    ),
    joined as (
      select
        s.candidate_id,
        s.day,
        s.condition_id,
        s.slug,
        s.trigger_ts_ms,
        s.first_side,
        s.first_l2_vwap,
        s.cache_min_pair_cost_30s,
        s.strict_l1_immediate_pair,
        min(c.side_ask) as joined_min_completion_ask,
        min(c.ts_ms - s.trigger_ts_ms) filter (where c.side_ask is not null) as joined_min_delay_ms
      from strict_candidates s
      left join completion_db.{COMPLETION_TABLE} c
        on c.day = s.day
       and c.condition_id = s.condition_id
       and c.side <> s.first_side
       and c.ts_ms between s.trigger_ts_ms and s.trigger_ts_ms + {int(window_ms)}
       {event_filter_sql}
      group by
        s.candidate_id, s.day, s.condition_id, s.slug, s.trigger_ts_ms, s.first_side,
        s.first_l2_vwap, s.cache_min_pair_cost_30s, s.strict_l1_immediate_pair
    ),
    scored as (
      select
        *,
        first_l2_vwap + joined_min_completion_ask as joined_min_pair_cost,
        abs((first_l2_vwap + joined_min_completion_ask) - cache_min_pair_cost_30s) as abs_delta
      from joined
    )
    select
      count(*) as strict_candidates,
      count(joined_min_completion_ask) as joined_candidates,
      sum(case when cache_min_pair_cost_30s is not null then 1 else 0 end) as cache_labeled_candidates,
      avg(abs_delta) as avg_abs_delta,
      max(abs_delta) as max_abs_delta,
      sum(case when abs_delta <= 0.000001 then 1 else 0 end) as exact_match_count,
      sum(case when abs_delta <= 0.005 then 1 else 0 end) as within_half_cent_count,
      sum(case when abs_delta <= 0.01 then 1 else 0 end) as within_one_cent_count,
      sum(case when joined_min_pair_cost <= 1.0 then 1 else 0 end) as joined_pair_le_100_count,
      sum(case when cache_min_pair_cost_30s <= 1.0 then 1 else 0 end) as cache_pair_le_100_count,
      min(joined_min_pair_cost) as joined_min_pair_cost_min,
      max(joined_min_pair_cost) as joined_min_pair_cost_max
    from scored
    """
    row = con.execute(query).fetchone()
    mismatch_rows = con.execute(
        f"""
        with strict_candidates as (
          select
            row_number() over () as candidate_id,
            day,
            condition_id,
            slug,
            cast(trigger_ts_ms as bigint) as trigger_ts_ms,
            first_side,
            cast(first_l2_vwap as double) as first_l2_vwap,
            cast(min_pair_cost_30s as double) as cache_min_pair_cost_30s
          from strict_db.{STRICT_TABLE}
          where day in ({days_sql})
        ),
        joined as (
          select
            s.candidate_id, s.day, s.condition_id, s.slug, s.trigger_ts_ms, s.first_side,
            s.first_l2_vwap, s.cache_min_pair_cost_30s,
            min(c.side_ask) as joined_min_completion_ask
          from strict_candidates s
          left join completion_db.{COMPLETION_TABLE} c
            on c.day = s.day
           and c.condition_id = s.condition_id
           and c.side <> s.first_side
           and c.ts_ms between s.trigger_ts_ms and s.trigger_ts_ms + {int(window_ms)}
           {event_filter_sql}
          group by s.candidate_id, s.day, s.condition_id, s.slug, s.trigger_ts_ms, s.first_side,
                   s.first_l2_vwap, s.cache_min_pair_cost_30s
        )
        select
          day, slug, condition_id, trigger_ts_ms, first_side, first_l2_vwap,
          joined_min_completion_ask,
          first_l2_vwap + joined_min_completion_ask as joined_min_pair_cost,
          cache_min_pair_cost_30s,
          abs((first_l2_vwap + joined_min_completion_ask) - cache_min_pair_cost_30s) as abs_delta
        from joined
        where joined_min_completion_ask is not null and cache_min_pair_cost_30s is not null
        order by abs_delta desc
        limit 5
        """
    ).fetchall()
    con.close()

    strict_candidates = int(row[0] or 0)
    joined_candidates = int(row[1] or 0)
    exact = int(row[5] or 0)
    half_cent = int(row[6] or 0)
    one_cent = int(row[7] or 0)
    return {
        "strict_label": strict_label,
        "completion_label": completion_label,
        "days": days,
        "event_kind_filter": event_kind_filter,
        "ready": True,
        "failures": [],
        "strict_candidates": strict_candidates,
        "strict_candidate_count": strict_candidates,
        "joined_candidates": joined_candidates,
        "joined_candidate_count": joined_candidates,
        "cache_labeled_candidates": int(row[2] or 0),
        "join_coverage_rate": round(joined_candidates / strict_candidates, 6) if strict_candidates else 0.0,
        "avg_abs_delta": finite(row[3]),
        "max_abs_delta": finite(row[4]),
        "exact_match_count": exact,
        "exact_match_rate": round(exact / joined_candidates, 6) if joined_candidates else 0.0,
        "within_half_cent_count": half_cent,
        "within_half_cent_rate": round(half_cent / joined_candidates, 6) if joined_candidates else 0.0,
        "within_one_cent_count": one_cent,
        "within_one_cent_rate": round(one_cent / joined_candidates, 6) if joined_candidates else 0.0,
        "joined_pair_le_100_count": int(row[8] or 0),
        "cache_pair_le_100_count": int(row[9] or 0),
        "joined_pair_le_100_delta_vs_cache": int(row[8] or 0) - int(row[9] or 0),
        "joined_min_pair_cost_min": finite(row[10]),
        "joined_min_pair_cost_max": finite(row[11]),
        "top_mismatches": [
            {
                "day": item[0],
                "slug": item[1],
                "condition_id": item[2],
                "trigger_ts_ms": int(item[3]),
                "first_side": item[4],
                "first_l2_vwap": finite(item[5]),
                "joined_min_completion_ask": finite(item[6]),
                "joined_min_pair_cost": finite(item[7]),
                "cache_min_pair_cost_30s": finite(item[8]),
                "abs_delta": finite(item[9]),
            }
            for item in mismatch_rows
        ],
    }


def aggregate(probes: list[dict[str, Any]]) -> dict[str, Any]:
    total_strict = sum(int(item.get("strict_candidates") or 0) for item in probes)
    total_joined = sum(int(item.get("joined_candidates") or 0) for item in probes)
    total_exact = sum(int(item.get("exact_match_count") or 0) for item in probes)
    total_half_cent = sum(int(item.get("within_half_cent_count") or 0) for item in probes)
    total_one_cent = sum(int(item.get("within_one_cent_count") or 0) for item in probes)
    total_joined_le_100 = sum(int(item.get("joined_pair_le_100_count") or 0) for item in probes)
    total_cache_le_100 = sum(int(item.get("cache_pair_le_100_count") or 0) for item in probes)
    return {
        "strict_candidate_count": total_strict,
        "joined_candidate_count": total_joined,
        "join_coverage_rate": round(total_joined / total_strict, 6) if total_strict else 0.0,
        "exact_match_count": total_exact,
        "exact_match_rate": round(total_exact / total_joined, 6) if total_joined else 0.0,
        "within_half_cent_count": total_half_cent,
        "within_half_cent_rate": round(total_half_cent / total_joined, 6) if total_joined else 0.0,
        "within_one_cent_count": total_one_cent,
        "within_one_cent_rate": round(total_one_cent / total_joined, 6) if total_joined else 0.0,
        "joined_pair_le_100_count": total_joined_le_100,
        "cache_pair_le_100_count": total_cache_le_100,
        "joined_pair_le_100_delta_vs_cache": total_joined_le_100 - total_cache_le_100,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--strict-labels", default="")
    parser.add_argument("--completion-labels", default="")
    parser.add_argument("--window-ms", type=int, default=30_000)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    label = utc_label()
    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    strict = discover(strict_root, "CACHE_MANIFEST.json", args.strict_labels)
    completion = discover(completion_root, "EVENT_STORE_MANIFEST.json", args.completion_labels)
    public_days, public_day_counts = public_audit_days(public_audit_db)

    event_kind_variants = ["all", "l1_price_change", "public_trade"]
    variant_probes: dict[str, list[dict[str, Any]]] = {variant: [] for variant in event_kind_variants}
    for variant in event_kind_variants:
        for strict_label, strict_info in strict.items():
            strict_days = set(strict_info["days"])
            for completion_label, completion_info in completion.items():
                overlap = sorted(strict_days & set(completion_info["days"]))
                if overlap:
                    variant_probes[variant].append(
                        run_one(
                            strict_root,
                            completion_root,
                            strict_label,
                            completion_label,
                            overlap,
                            args.window_ms,
                            variant,
                        )
                    )

    probes = variant_probes["all"]
    primary = aggregate(probes)
    variant_summaries = {
        variant: {"event_kind_filter": variant, **aggregate(items)}
        for variant, items in variant_probes.items()
    }
    best_half_cent_variant = max(
        variant_summaries.values(),
        key=lambda item: (float(item["within_half_cent_rate"]), float(item["within_one_cent_rate"])),
    )
    total_strict = int(primary["strict_candidate_count"])
    total_joined = int(primary["joined_candidate_count"])
    join_coverage = float(primary["join_coverage_rate"])
    exact_rate = float(primary["exact_match_rate"])
    half_cent_rate = float(primary["within_half_cent_rate"])
    one_cent_rate = float(primary["within_one_cent_rate"])
    days = sorted({day for info in strict.values() for day in info["days"]})
    strict_days_missing_public_audit = sorted(set(days) - set(public_days))
    label_alignment_passed = total_strict > 0 and join_coverage >= 0.999 and half_cent_rate >= 0.90
    fail_reasons: list[str] = []
    if total_strict <= 0:
        fail_reasons.append("no_strict_candidates")
    if join_coverage < 0.999:
        fail_reasons.append("join_coverage_below_0_999")
    if half_cent_rate < 0.90:
        fail_reasons.append("within_half_cent_rate_below_0_90")
    if one_cent_rate < 0.95:
        fail_reasons.append("within_one_cent_rate_below_0_95")
    if best_half_cent_variant["within_half_cent_rate"] < 0.90:
        fail_reasons.append("no_event_kind_variant_reaches_half_cent_threshold")
    top_abs_mismatches = sorted(
        (
            mismatch
            for item in probes
            for mismatch in item.get("top_mismatches", [])
            if mismatch.get("abs_delta") is not None
        ),
        key=lambda item: float(item["abs_delta"]),
        reverse=True,
    )[:10]
    status = (
        "PASS_STRICT_COMPLETION_JOIN_LABEL_ALIGNMENT_PUBLIC_AUDIT_PARTIAL"
        if label_alignment_passed and strict_days_missing_public_audit
        else "PASS_STRICT_COMPLETION_JOIN_LABEL_ALIGNMENT"
        if label_alignment_passed
        else "FAIL_STRICT_COMPLETION_JOIN_LABEL_ALIGNMENT"
    )
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_read_only_strict_completion_join_label_probe",
        "status": status,
        "probe_passed": label_alignment_passed,
        "data_root": str(DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_label_probe",
        "labels": {
            "strict": sorted(strict),
            "completion": sorted(completion),
            "public_audit": public_audit_db.parent.name if public_audit_db.exists() else None,
        },
        "days": days,
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": total_strict,
        **primary,
        "primary_event_kind_filter": "all",
        "event_kind_variant_summaries": variant_summaries,
        "best_half_cent_event_kind_variant": best_half_cent_variant["event_kind_filter"],
        "fail_reasons": fail_reasons,
        "top_abs_mismatches": top_abs_mismatches,
        "window_ms": args.window_ms,
        "public_audit_days": public_days,
        "public_audit_day_counts": public_day_counts,
        "strict_days_missing_public_audit": strict_days_missing_public_audit,
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": bool(public_days),
        "public_account_truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "conclusion_scope": (
            "strict-cache to completion-store label alignment only; not strategy PnL, "
            "private owner-trade truth, or source-of-truth replay validation"
        ),
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
        "next_gate": (
            "build the adapter on the joined strict/completion candidate rows and report public-audit-covered "
            "05-02..13 separately from 05-16/17"
            if label_alignment_passed
            else "investigate strict cache completion label mismatch before using adapter outputs"
        ),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if label_alignment_passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
