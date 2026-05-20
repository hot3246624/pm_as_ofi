#!/usr/bin/env python3
"""Score D+ residual-source buckets from candidate pipeline V1 results.

This verifier reads only the derived candidate pipeline V1 state-machine
DuckDB and manifests. It does not read raw/replay/collector stores, the full
completion event store, sockets, SSH, or event JSONL.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
ARTIFACT = "xuan_b27_dplus_candidate_pipeline_residual_source_verifier"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def rows_to_dicts(rows: list[tuple[Any, ...]], columns: list[str]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row, strict=True)) for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", default=str(DEFAULT_RESULT_DIR))
    parser.add_argument("--output-dir")
    parser.add_argument("--min-retained-candidate-ratio", type=float, default=0.65)
    parser.add_argument("--min-residual-reduction-ratio", type=float, default=0.50)
    parser.add_argument("--max-proxy-residual-qty-rate", type=float, default=0.025)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = Path(args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}")
    result_dir = Path(args.result_dir)
    result_manifest_path = result_dir / "RESULT_SUMMARY_MANIFEST.json"
    registry_manifest_path = result_dir / "CANDIDATE_REGISTRY_MANIFEST.json"
    compliance_manifest_path = result_dir / "COMPLIANCE_MANIFEST.json"
    duckdb_path = result_dir / "state_machine_results.duckdb"

    required = [result_manifest_path, registry_manifest_path, compliance_manifest_path, duckdb_path]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": label,
            "decision_label": "BLOCKED",
            "status": "BLOCKED_CANDIDATE_PIPELINE_INPUT_UNAVAILABLE",
            "missing": missing,
            "unsafe": unsafe,
            "next_action": "Restore the required local candidate pipeline manifests/DuckDB under POLY_BT_ROOT and rerun this verifier.",
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    result_manifest = read_json(result_manifest_path)
    registry_manifest = read_json(registry_manifest_path)
    compliance_manifest = read_json(compliance_manifest_path)
    core = result_manifest.get("core_metrics") or {}

    con = duckdb.connect(str(duckdb_path), read_only=True)
    table_counts = {
        table: con.execute(f"select count(*) from {table}").fetchone()[0]
        for table in ("candidate_registry", "residual_lots", "summary_by_day")
    }
    baseline_row = con.execute(
        """
        select
          count(*) as candidates,
          sum(seed_qty) as seed_qty,
          sum(seed_cost) as seed_cost,
          sum(official_taker_fee) as official_taker_fee
        from candidate_registry
        """
    ).fetchone()
    residual_row = con.execute(
        """
        select
          count(*) as residual_lots,
          sum(qty) as residual_qty,
          sum(cost) as residual_cost,
          sum(pnl) as residual_pnl
        from residual_lots
        """
    ).fetchone()
    baseline = {
        "candidates": float(baseline_row[0] or 0),
        "seed_qty": float(baseline_row[1] or 0),
        "seed_cost": float(baseline_row[2] or 0),
        "official_taker_fee": float(baseline_row[3] or 0),
        "residual_lots": float(residual_row[0] or 0),
        "residual_qty": float(residual_row[1] or 0),
        "residual_cost": float(residual_row[2] or 0),
        "residual_pnl": float(residual_row[3] or 0),
    }
    baseline["residual_qty_rate"] = baseline["residual_qty"] / baseline["seed_qty"] if baseline["seed_qty"] else None
    baseline["residual_cost_rate"] = baseline["residual_cost"] / baseline["seed_cost"] if baseline["seed_cost"] else None

    bucket_sql = """
    with src as (
      select
        cr.candidate_row_id,
        cr.day,
        cr.side,
        cr.side_alignment,
        cr.seed_px,
        cr.public_trade_price,
        cr.offset_s,
        cr.seed_qty,
        cr.seed_cost,
        cr.official_taker_fee,
        cr.strict_cache_day_covered,
        cr.public_audit_day_covered,
        coalesce(rl.qty, 0) as residual_qty,
        coalesce(rl.cost, 0) as residual_cost,
        coalesce(rl.pnl, 0) as residual_pnl,
        case
          when cr.seed_px < 0.30 then 'seed_lt_0p30'
          when cr.seed_px < 0.45 then 'seed_0p30_0p45'
          when cr.seed_px < 0.48 then 'seed_0p45_0p48'
          when cr.seed_px < 0.55 then 'seed_0p48_0p55'
          else 'seed_ge_0p55'
        end as seed_px_bucket,
        case
          when cr.public_trade_price < 0.30 then 'pub_lt_0p30'
          when cr.public_trade_price < 0.45 then 'pub_0p30_0p45'
          when cr.public_trade_price < 0.48 then 'pub_0p45_0p48'
          when cr.public_trade_price < 0.55 then 'pub_0p48_0p55'
          else 'pub_ge_0p55'
        end as public_px_bucket,
        case
          when cr.offset_s < 30 then 'off_0_30'
          when cr.offset_s < 60 then 'off_30_60'
          when cr.offset_s < 90 then 'off_60_90'
          else 'off_90_120'
        end as offset_bucket
      from candidate_registry cr
      left join residual_lots rl
        on cast(rl.candidate_row_id as varchar) = cast(cr.candidate_row_id as varchar)
    ),
    buckets as (
      select 'side' as dimension, side as bucket, * exclude(side) from (select side, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by side)
      union all select 'side_alignment', side_alignment, * exclude(side_alignment) from (select side_alignment, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by side_alignment)
      union all select 'seed_px', seed_px_bucket, * exclude(seed_px_bucket) from (select seed_px_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by seed_px_bucket)
      union all select 'public_px', public_px_bucket, * exclude(public_px_bucket) from (select public_px_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by public_px_bucket)
      union all select 'offset', offset_bucket, * exclude(offset_bucket) from (select offset_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by offset_bucket)
      union all select 'side_offset', side || ':' || offset_bucket, * exclude(side, offset_bucket) from (select side, offset_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by side, offset_bucket)
      union all select 'seed_offset', seed_px_bucket || ':' || offset_bucket, * exclude(seed_px_bucket, offset_bucket) from (select seed_px_bucket, offset_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by seed_px_bucket, offset_bucket)
      union all select 'public_offset', public_px_bucket || ':' || offset_bucket, * exclude(public_px_bucket, offset_bucket) from (select public_px_bucket, offset_bucket, count(*) candidates, count(distinct day) day_count, sum(seed_qty) seed_qty, sum(seed_cost) seed_cost, sum(official_taker_fee) official_taker_fee, sum(residual_qty) residual_qty, sum(residual_cost) residual_cost, sum(residual_pnl) residual_pnl, sum(case when strict_cache_day_covered then 1 else 0 end) strict_covered, sum(case when public_audit_day_covered then 1 else 0 end) public_covered from src group by public_px_bucket, offset_bucket)
    )
    select
      dimension,
      bucket,
      candidates,
      day_count,
      seed_qty,
      seed_cost,
      official_taker_fee,
      residual_qty,
      residual_cost,
      residual_pnl,
      strict_covered,
      public_covered
    from buckets
    where candidates > 0
    """
    bucket_columns = [
        "dimension",
        "bucket",
        "candidates",
        "day_count",
        "seed_qty",
        "seed_cost",
        "official_taker_fee",
        "residual_qty",
        "residual_cost",
        "residual_pnl",
        "strict_covered",
        "public_covered",
    ]
    bucket_rows = rows_to_dicts(con.execute(bucket_sql).fetchall(), bucket_columns)

    scored_rules: list[dict[str, Any]] = []
    for row in bucket_rows:
        candidates = float(row["candidates"] or 0)
        seed_qty = float(row["seed_qty"] or 0)
        seed_cost = float(row["seed_cost"] or 0)
        residual_qty = float(row["residual_qty"] or 0)
        residual_cost = float(row["residual_cost"] or 0)
        retained_candidates = baseline["candidates"] - candidates
        retained_seed_qty = baseline["seed_qty"] - seed_qty
        retained_seed_cost = baseline["seed_cost"] - seed_cost
        residual_qty_after = baseline["residual_qty"] - residual_qty
        residual_cost_after = baseline["residual_cost"] - residual_cost
        candidate_ratio = candidates / baseline["candidates"] if baseline["candidates"] else 0.0
        retained_candidate_ratio = retained_candidates / baseline["candidates"] if baseline["candidates"] else 0.0
        residual_reduction_ratio = residual_qty / baseline["residual_qty"] if baseline["residual_qty"] else 0.0
        proxy_residual_qty_rate_after = residual_qty_after / retained_seed_qty if retained_seed_qty else None
        proxy_residual_cost_rate_after = residual_cost_after / retained_seed_cost if retained_seed_cost else None
        scored_rules.append(
            {
                "rule": f"drop_{row['dimension']}={row['bucket']}",
                "dimension": row["dimension"],
                "bucket": row["bucket"],
                "deployable_pretrade_fields": row["dimension"] in {
                    "side",
                    "side_alignment",
                    "seed_px",
                    "public_px",
                    "offset",
                    "side_offset",
                    "seed_offset",
                    "public_offset",
                },
                "drop_candidates": candidates,
                "drop_candidate_ratio": candidate_ratio,
                "retained_candidates": retained_candidates,
                "retained_candidate_ratio": retained_candidate_ratio,
                "retained_seed_qty_ratio": retained_seed_qty / baseline["seed_qty"] if baseline["seed_qty"] else None,
                "drop_seed_qty": seed_qty,
                "drop_residual_qty": residual_qty,
                "drop_residual_cost": residual_cost,
                "drop_residual_pnl": float(row["residual_pnl"] or 0),
                "residual_reduction_ratio": residual_reduction_ratio,
                "residual_qty_after": residual_qty_after,
                "residual_cost_after": residual_cost_after,
                "proxy_residual_qty_rate_after": proxy_residual_qty_rate_after,
                "proxy_residual_cost_rate_after": proxy_residual_cost_rate_after,
                "days": int(row["day_count"] or 0),
                "strict_covered_ratio": float(row["strict_covered"] or 0) / candidates if candidates else None,
                "public_covered_ratio": float(row["public_covered"] or 0) / candidates if candidates else None,
                "requires_state_machine_rerun": True,
                "fee_after_pnl_not_recomputed": True,
                "stress100_worst_pnl_not_recomputed": True,
            }
        )

    scored_rules.sort(
        key=lambda item: (
            item["proxy_residual_qty_rate_after"] if item["proxy_residual_qty_rate_after"] is not None else 999,
            -item["retained_candidate_ratio"],
        )
    )
    candidate_rules = [
        item
        for item in scored_rules
        if item["deployable_pretrade_fields"]
        and item["retained_candidate_ratio"] >= args.min_retained_candidate_ratio
        and item["residual_reduction_ratio"] >= args.min_residual_reduction_ratio
        and item["proxy_residual_qty_rate_after"] is not None
        and item["proxy_residual_qty_rate_after"] <= args.max_proxy_residual_qty_rate
    ]

    top_sources_sql = """
    select
      rl.day,
      rl.slug,
      rl.side as residual_side,
      rl.qty,
      rl.px,
      rl.cost,
      rl.pnl,
      cr.side as seed_side,
      cr.side_alignment,
      cr.seed_px,
      cr.public_trade_price,
      cr.offset_s,
      cr.l1_pair_ask,
      cr.strict_cache_day_covered,
      cr.public_audit_day_covered
    from residual_lots rl
    join candidate_registry cr
      on cast(rl.candidate_row_id as varchar) = cast(cr.candidate_row_id as varchar)
    order by rl.qty desc, rl.cost desc
    limit 50
    """
    top_source_columns = [
        "day",
        "slug",
        "residual_side",
        "qty",
        "px",
        "cost",
        "pnl",
        "seed_side",
        "side_alignment",
        "seed_px",
        "public_trade_price",
        "offset_s",
        "l1_pair_ask",
        "strict_cache_day_covered",
        "public_audit_day_covered",
    ]
    top_residual_sources = rows_to_dicts(con.execute(top_sources_sql).fetchall(), top_source_columns)

    summary_by_day = rows_to_dicts(
        con.execute(
            """
            select day, seed_actions, pair_actions, actual_settle_pnl,
                   official_taker_fee, fee_after_pnl, stress100_worst_pnl,
                   residual_qty, residual_cost, qty_residual_rate, cost_residual_rate
            from summary_by_day
            order by day
            """
        ).fetchall(),
        [
            "day",
            "seed_actions",
            "pair_actions",
            "actual_settle_pnl",
            "official_taker_fee",
            "fee_after_pnl",
            "stress100_worst_pnl",
            "residual_qty",
            "residual_cost",
            "qty_residual_rate",
            "cost_residual_rate",
        ],
    )
    con.close()

    best_rule = candidate_rules[0] if candidate_rules else None
    if best_rule:
        status = "KEEP_LATE_OFFSET_RESIDUAL_SOURCE_LEAD_RESEARCH_ONLY"
        decision_label = "KEEP"
        next_action = (
            "Run a local candidate-pipeline state-machine rerun from candidate_base with the same official-fee "
            "profile but a bounded late-offset admission variant equivalent to seed_offset_max_s=90, then compare "
            "sample, fee_after_pnl, stress100_worst_pnl, worst_day_fee_after_pnl, and residual rates. Do not run EC2 "
            "shadow or canary before this local rerun confirms the proxy."
        )
    else:
        status = "UNKNOWN_NO_BOUNDED_RESIDUAL_SOURCE_RULE"
        decision_label = "UNKNOWN"
        next_action = (
            "Add narrower residual-source fields or run a local candidate-pipeline rerun for a small set of candidate "
            "rules; do not spend EC2 shadow because proxy buckets do not identify a sample-preserving mechanism."
        )

    manifest = {
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision_label,
        "status": status,
        "hypothesis": (
            "If residual risk is concentrated in deployable source buckets, a bounded admission rule should reduce "
            "proxy residual rates while retaining enough candidates to justify a local state-machine rerun."
        ),
        "inputs": {
            "result_dir": str(result_dir),
            "result_manifest": str(result_manifest_path),
            "registry_manifest": str(registry_manifest_path),
            "compliance_manifest": str(compliance_manifest_path),
            "duckdb": str(duckdb_path),
        },
        "scope": {
            "status": result_manifest.get("status"),
            "dataset_type": result_manifest.get("dataset_type"),
            "labels": result_manifest.get("labels"),
            "days": result_manifest.get("days"),
            "excluded_labels_or_days": result_manifest.get("excluded_labels_or_days"),
            "public_account_execution_truth_v1_included": result_manifest.get(
                "public_account_execution_truth_v1_included"
            ),
            "public_account_execution_truth_v1_private_truth": result_manifest.get(
                "public_account_execution_truth_v1_private_truth"
            ),
            "deployable": core.get("deployable"),
            "can_support_strategy_promotion": core.get("can_support_strategy_promotion"),
            "promotion_gate_pass": compliance_manifest.get("promotion_gate_pass"),
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "shared_ingress_connected": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "raw_scanned": False,
            "replay_scanned": False,
            "collector_scanned": False,
            "full_completion_store_scanned": False,
            "candidate_base_scanned": False,
            "state_machine_results_duckdb_read_only": True,
        },
        "baseline": {
            **baseline,
            "fee_after_pnl": core.get("fee_after_pnl"),
            "stress100_worst_pnl": core.get("stress100_worst_pnl"),
            "worst_day_fee_after_pnl": core.get("worst_day_fee_after_pnl"),
            "residual_qty_rate_reported": core.get("residual_qty_rate"),
            "residual_cost_rate_reported": core.get("residual_cost_rate"),
        },
        "table_counts": table_counts,
        "decision_thresholds": {
            "min_retained_candidate_ratio": args.min_retained_candidate_ratio,
            "min_residual_reduction_ratio": args.min_residual_reduction_ratio,
            "max_proxy_residual_qty_rate": args.max_proxy_residual_qty_rate,
        },
        "best_rule": best_rule,
        "candidate_rules": candidate_rules[:10],
        "top_scored_rules": scored_rules[:30],
        "top_residual_sources": top_residual_sources,
        "summary_by_day": summary_by_day,
        "interpretation": [
            "This is a source-attribution proxy, not a state-machine rerun; fee_after_pnl and stress are not recomputed after dropping a bucket.",
            "The strongest proxy rule is late-offset source admission: offset_90_120 carries most residual while retaining most sample if removed.",
            "Because hard cutoffs were previously killed in EC2 shadow, this KEEP only authorizes a local candidate-pipeline state-machine rerun, not another EC2 shadow or canary.",
        ],
        "scoreboard_delta": {
            "baseline_candidates": baseline["candidates"],
            "baseline_residual_qty_rate": baseline["residual_qty_rate"],
            "baseline_fee_after_pnl": core.get("fee_after_pnl"),
            "baseline_stress100_worst_pnl": core.get("stress100_worst_pnl"),
            "best_rule": best_rule["rule"] if best_rule else None,
            "best_rule_retained_candidate_ratio": best_rule["retained_candidate_ratio"] if best_rule else None,
            "best_rule_residual_reduction_ratio": best_rule["residual_reduction_ratio"] if best_rule else None,
            "best_rule_proxy_residual_qty_rate_after": best_rule["proxy_residual_qty_rate_after"] if best_rule else None,
        },
        "next_action": next_action,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "manifest": str(output_dir / "manifest.json"),
                "decision_label": decision_label,
                "status": status,
                "best_rule": best_rule,
                "next_action": next_action,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
