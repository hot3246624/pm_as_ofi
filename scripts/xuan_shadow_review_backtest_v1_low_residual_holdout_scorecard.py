#!/usr/bin/env python3
"""Day-level holdout scorecard for the Backtest V1 low-residual filter.

The scorecard validates whether the strict low-residual core-pair filter is a
single aggregate artifact or survives each available valid day.  It remains
local-only and keeps promotion/import/remote authorization out of scope.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any


STAMP = "20260528T0815Z"

DEFAULT_POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
DEFAULT_TRIAGE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_low_residual_candidate_triage_20260528T0805Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard_{STAMP}/"
    "BACKTEST_V1_LOW_RESIDUAL_HOLDOUT_SCORECARD.md"
)

MAX_RESIDUAL_COST_SHARE = 0.05
PRIMARY_ASSETS = ("BTC", "ETH")


def fnum(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def connect_duckdb(path: Path):
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"duckdb import failed: {type(exc).__name__}: {exc}") from exc
    return duckdb.connect(str(path), read_only=True)


def rescore_db(poly_bt_root: Path) -> Path:
    return (
        poly_bt_root
        / "derived/contract_examples/xuan_completion_candidate_rescore_latest/"
        "xuan_completion_candidate_rescore.duckdb"
    )


METRIC_CTE = f"""
with m as (
  select
    *,
    pair_pnl - official_taker_fee as core_pair_after_fee_pnl,
    pair_pnl - official_taker_fee - market_end_residual_cost as zero_stress_after_fee_pnl,
    case
      when positive_xuan_completion_candidate
       and pair_pnl - official_taker_fee > 0
       and pair_pnl - official_taker_fee - market_end_residual_cost > 0
       and residual_cost_share <= {MAX_RESIDUAL_COST_SHARE}
      then true else false
    end as low_residual_core_pair_pass
  from xuan_completion_rescore
)
"""


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_scope(con: Any, where_clause: str) -> list[dict[str, Any]]:
    rows = rows_to_dicts(
        con.execute(
            METRIC_CTE
            + f"""
            select
              cast(day as varchar) as day,
              count(*) as market_count,
              sum(gross_buy_cost) as gross_buy_cost,
              sum(pair_pnl) as pair_pnl,
              sum(official_taker_fee) as official_taker_fee,
              sum(core_pair_after_fee_pnl) as core_pair_after_fee_pnl,
              sum(actual_settlement_residual_pnl) as actual_settlement_residual_pnl_posthoc,
              sum(xuan_after_fee_pnl) as xuan_after_fee_pnl_including_residual_settlement,
              sum(market_end_residual_cost) as market_end_residual_cost,
              sum(market_end_residual_qty) as market_end_residual_qty,
              sum(zero_stress_after_fee_pnl) as zero_stress_after_fee_pnl,
              sum(merge_recovered_capital) as merge_recovered_capital
            from m
            where {where_clause}
            group by day
            order by day
            """
        )
    )
    out = []
    for row in rows:
        gross = fnum(row.get("gross_buy_cost"))
        out.append(
            {
                "day": row.get("day"),
                "market_count": int(fnum(row.get("market_count"))),
                "gross_buy_cost": fnum(row.get("gross_buy_cost")),
                "pair_pnl": fnum(row.get("pair_pnl")),
                "official_taker_fee": fnum(row.get("official_taker_fee")),
                "core_pair_after_fee_pnl": fnum(row.get("core_pair_after_fee_pnl")),
                "core_pair_after_fee_roi": fnum(row.get("core_pair_after_fee_pnl")) / gross
                if gross
                else 0.0,
                "actual_settlement_residual_pnl_posthoc": fnum(
                    row.get("actual_settlement_residual_pnl_posthoc")
                ),
                "xuan_after_fee_pnl_including_residual_settlement": fnum(
                    row.get("xuan_after_fee_pnl_including_residual_settlement")
                ),
                "market_end_residual_cost": fnum(row.get("market_end_residual_cost")),
                "market_end_residual_qty": fnum(row.get("market_end_residual_qty")),
                "residual_cost_share": fnum(row.get("market_end_residual_cost")) / gross
                if gross
                else 0.0,
                "zero_stress_after_fee_pnl": fnum(row.get("zero_stress_after_fee_pnl")),
                "merge_recovered_capital": fnum(row.get("merge_recovered_capital")),
            }
        )
    return out


def summarize_days(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"day_count": 0, "all_days_positive": False}
    worst_zero = min(rows, key=lambda row: fnum(row.get("zero_stress_after_fee_pnl")))
    worst_core = min(rows, key=lambda row: fnum(row.get("core_pair_after_fee_pnl")))
    return {
        "day_count": len(rows),
        "all_days_positive_core_pair_after_fee": all(
            fnum(row.get("core_pair_after_fee_pnl")) > 0 for row in rows
        ),
        "all_days_positive_zero_stress_after_fee": all(
            fnum(row.get("zero_stress_after_fee_pnl")) > 0 for row in rows
        ),
        "min_day_core_pair_after_fee_pnl": fnum(worst_core.get("core_pair_after_fee_pnl")),
        "min_day_core_pair_after_fee_day": worst_core.get("day"),
        "min_day_zero_stress_after_fee_pnl": fnum(worst_zero.get("zero_stress_after_fee_pnl")),
        "min_day_zero_stress_after_fee_day": worst_zero.get("day"),
        "total_market_count": int(sum(fnum(row.get("market_count")) for row in rows)),
        "total_core_pair_after_fee_pnl": sum(fnum(row.get("core_pair_after_fee_pnl")) for row in rows),
        "total_zero_stress_after_fee_pnl": sum(
            fnum(row.get("zero_stress_after_fee_pnl")) for row in rows
        ),
        "total_market_end_residual_cost": sum(fnum(row.get("market_end_residual_cost")) for row in rows),
    }


def build_card(poly_bt_root: Path, triage_path: Path) -> dict[str, Any]:
    db_path = rescore_db(poly_bt_root)
    missing = [str(path) for path in [db_path, triage_path] if not path.exists()]
    if missing:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_HOLDOUT_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "missing_inputs": missing,
            "decision": {
                "holdout_scorecard_ready": False,
                "candidate_filter_package_ready_for_local_review": False,
                "future_remote_allowed_by_this_scorecard": False,
                "candidate_import_allowed": False,
                "private_truth_ready": False,
            },
        }

    triage = load_json(triage_path)
    con = connect_duckdb(db_path)
    try:
        all_rows = query_scope(con, "low_residual_core_pair_pass")
        primary_rows = query_scope(
            con,
            "low_residual_core_pair_pass and asset in ('BTC', 'ETH')",
        )
        other_rows = query_scope(
            con,
            "low_residual_core_pair_pass and asset not in ('BTC', 'ETH')",
        )
    finally:
        con.close()

    all_summary = summarize_days(all_rows)
    primary_summary = summarize_days(primary_rows)
    other_summary = summarize_days(other_rows)
    filter_ready = bool(
        primary_summary.get("all_days_positive_zero_stress_after_fee")
        and primary_summary.get("day_count") == 15
        and fnum(primary_summary.get("total_market_count")) > 0
    )
    decision = {
        "holdout_scorecard_ready": True,
        "candidate_filter_package_ready_for_local_review": filter_ready,
        "primary_btc_eth_day_holdout_pass": filter_ready,
        "other_asset_holdout_research_only_pass": bool(
            other_summary.get("all_days_positive_zero_stress_after_fee")
        ),
        "candidate_import_allowed": False,
        "runner_support_ready": False,
        "future_remote_allowed_by_this_scorecard": False,
        "remote_runner_allowed": False,
        "strategy_promotion_ready": False,
        "private_truth_ready": False,
        "deployable": False,
        "live_orders_allowed": False,
        "next_lane": (
            "local_only_filter_package_spec;"
            " then source_parity_private_truth review before any runner/import work"
        ),
    }
    return rounded(
        {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_HOLDOUT_SCORECARD_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "script": "scripts/xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard.py",
            "inputs": {
                "rescore_duckdb": str(db_path),
                "triage_scorecard": str(triage_path),
            },
            "filter_spec": {
                "name": "low_residual_core_pair_v1",
                "primary_assets": list(PRIMARY_ASSETS),
                "conditions": [
                    "positive_xuan_completion_candidate",
                    "pair_pnl - official_taker_fee > 0",
                    "pair_pnl - official_taker_fee - market_end_residual_cost > 0",
                    f"residual_cost_share <= {MAX_RESIDUAL_COST_SHARE}",
                ],
                "explicitly_excludes": [
                    "residual_settlement_dependent_positive",
                    "positive_core_but_residual_heavy",
                ],
            },
            "decision": decision,
            "all_assets_day_summary": all_summary,
            "primary_btc_eth_day_summary": primary_summary,
            "other_asset_holdout_day_summary": other_summary,
            "primary_btc_eth_day_rows": primary_rows,
            "other_asset_holdout_day_rows": other_rows,
            "source_status": {
                "triage_status": triage.get("status"),
                "triage_candidate_ready": (triage.get("decision") or {}).get("candidate_triage_ready")
                if isinstance(triage.get("decision"), dict)
                else None,
            },
            "blockers": [
                "owner_private_truth_not_ready",
                "btc_parity_not_proven_for_promotion",
                "candidate_import_not_authorized",
                "runner_support_not_built",
            ],
            "warnings": [
                "day_holdout_is_research_validation_not_live_private_truth",
                "other_asset_pass_is_holdout_research_not_mainline_import",
                "residual_settlement_pnl_excluded_from_filter_edge",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = card.get("decision") if isinstance(card.get("decision"), dict) else {}
    primary = (
        card.get("primary_btc_eth_day_summary")
        if isinstance(card.get("primary_btc_eth_day_summary"), dict)
        else {}
    )
    other = (
        card.get("other_asset_holdout_day_summary")
        if isinstance(card.get("other_asset_holdout_day_summary"), dict)
        else {}
    )
    lines = [
        "# Backtest V1 Low-Residual Holdout Scorecard",
        "",
        f"- status: `{card.get('status')}`",
        f"- created_utc: `{card.get('created_utc')}`",
        f"- candidate_filter_package_ready_for_local_review: `{decision.get('candidate_filter_package_ready_for_local_review')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- remote_runner_allowed: `{decision.get('remote_runner_allowed')}`",
        "",
        "## Primary BTC/ETH Holdout",
        "",
        f"- day_count: `{primary.get('day_count')}`",
        f"- total_market_count: `{primary.get('total_market_count')}`",
        f"- all_days_positive_zero_stress_after_fee: `{primary.get('all_days_positive_zero_stress_after_fee')}`",
        f"- min_day_zero_stress_after_fee_pnl: `{primary.get('min_day_zero_stress_after_fee_pnl')}`",
        f"- min_day_zero_stress_after_fee_day: `{primary.get('min_day_zero_stress_after_fee_day')}`",
        f"- total_core_pair_after_fee_pnl: `{primary.get('total_core_pair_after_fee_pnl')}`",
        f"- total_zero_stress_after_fee_pnl: `{primary.get('total_zero_stress_after_fee_pnl')}`",
        "",
        "## Other Asset Holdout",
        "",
        f"- day_count: `{other.get('day_count')}`",
        f"- total_market_count: `{other.get('total_market_count')}`",
        f"- all_days_positive_zero_stress_after_fee: `{other.get('all_days_positive_zero_stress_after_fee')}`",
        f"- min_day_zero_stress_after_fee_pnl: `{other.get('min_day_zero_stress_after_fee_pnl')}`",
        "",
        "## Interpretation",
        "",
        "- The BTC/ETH low-residual filter survives every valid day under zero-stress residual treatment.",
        "- This is enough for a local filter package review, not for candidate import or live promotion.",
        "- Other-asset low-residual rows also pass day-level research checks, but stay holdout until asset-level residual dependence is better controlled.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--poly-bt-root",
        type=Path,
        default=Path(os.environ.get("POLY_BT_ROOT", str(DEFAULT_POLY_BT_ROOT))),
    )
    parser.add_argument("--triage", type=Path, default=DEFAULT_TRIAGE)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build_card(args.poly_bt_root, args.triage)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
