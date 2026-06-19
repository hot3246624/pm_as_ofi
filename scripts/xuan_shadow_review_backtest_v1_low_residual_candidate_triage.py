#!/usr/bin/env python3
"""Triage Backtest V1 candidates by core pair edge and residual risk.

This local-only scorer uses the rebuilt xuan completion/residual rescore table.
It deliberately excludes posthoc residual settlement from the strategy edge and
classifies candidates by pair PnL after official fees, zero-stress residual
survival, and residual cost share.  It does not import candidates, build runner
profiles, or authorize remote execution.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any


STAMP = "20260528T0805Z"

DEFAULT_POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_low_residual_candidate_triage_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_low_residual_candidate_triage_{STAMP}/"
    "BACKTEST_V1_LOW_RESIDUAL_CANDIDATE_TRIAGE.md"
)


THRESHOLDS = {
    "max_low_residual_cost_share": 0.05,
    "min_core_pair_after_fee_pnl": 0.0,
    "min_zero_stress_after_fee_pnl": 0.0,
    "primary_assets": ["BTC", "ETH"],
}


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


def contract_root(poly_bt_root: Path) -> Path:
    return poly_bt_root / "derived/contract_examples"


def paths(poly_bt_root: Path) -> dict[str, Path]:
    root = contract_root(poly_bt_root)
    return {
        "strategy_review": Path(
            ".tmp_xuan/scorecards/"
            "xuan_shadow_review_backtest_v1_strategy_readiness_review_20260528T0754Z.json"
        ),
        "rescore_manifest": root
        / "xuan_completion_candidate_rescore_latest/XUAN_COMPLETION_CANDIDATE_RESCORE_MANIFEST.json",
        "rescore_duckdb": root
        / "xuan_completion_candidate_rescore_latest/xuan_completion_candidate_rescore.duckdb",
        "capital_ledger": root / "xuan_capital_ledger_latest/XUAN_CAPITAL_LEDGER_REPORT.json",
    }


def connect_duckdb(path: Path):
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency path
        raise SystemExit(f"duckdb import failed: {type(exc).__name__}: {exc}") from exc
    return duckdb.connect(str(path), read_only=True)


METRIC_CTE = """
with m as (
  select
    *,
    pair_pnl - official_taker_fee as core_pair_after_fee_pnl,
    pair_pnl - official_taker_fee - market_end_residual_cost as zero_stress_after_fee_pnl,
    case
      when positive_xuan_completion_candidate
       and pair_pnl - official_taker_fee > 0
       and pair_pnl - official_taker_fee - market_end_residual_cost > 0
       and residual_cost_share <= {max_low_residual_cost_share}
      then true else false
    end as low_residual_core_pair_pass,
    case
      when positive_xuan_completion_candidate
       and pair_pnl - official_taker_fee <= 0
       and xuan_after_fee_pnl > 0
      then true else false
    end as residual_settlement_dependent_positive,
    case
      when positive_xuan_completion_candidate
       and pair_pnl - official_taker_fee > 0
       and (
            pair_pnl - official_taker_fee - market_end_residual_cost <= 0
         or residual_cost_share > {max_low_residual_cost_share}
       )
      then true else false
    end as positive_core_but_residual_heavy
  from xuan_completion_rescore
)
""".format(**THRESHOLDS)


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def one_dict(con: Any, sql: str) -> dict[str, Any]:
    rows = rows_to_dicts(con.execute(METRIC_CTE + sql))
    return rows[0] if rows else {}


def many_dicts(con: Any, sql: str) -> list[dict[str, Any]]:
    return rows_to_dicts(con.execute(METRIC_CTE + sql))


def aggregate_sql(where_clause: str = "true") -> str:
    return f"""
    select
      count(*) as market_count,
      sum(case when positive_xuan_completion_candidate then 1 else 0 end) as positive_xuan_count,
      sum(gross_buy_cost) as gross_buy_cost,
      sum(pair_pnl) as pair_pnl,
      sum(official_taker_fee) as official_taker_fee,
      sum(core_pair_after_fee_pnl) as core_pair_after_fee_pnl,
      sum(actual_settlement_residual_pnl) as actual_settlement_residual_pnl_posthoc,
      sum(xuan_after_fee_pnl) as xuan_after_fee_pnl_including_residual_settlement,
      sum(market_end_residual_cost) as market_end_residual_cost,
      sum(market_end_residual_qty) as market_end_residual_qty,
      sum(zero_stress_after_fee_pnl) as zero_stress_after_fee_pnl,
      sum(merge_recovered_capital) as merge_recovered_capital,
      sum(case when residual_settlement_dependent_positive then 1 else 0 end)
        as residual_settlement_dependent_positive_count,
      sum(case when positive_core_but_residual_heavy then 1 else 0 end)
        as positive_core_but_residual_heavy_count,
      sum(case when low_residual_core_pair_pass then 1 else 0 end)
        as low_residual_core_pair_pass_count
    from m
    where {where_clause}
    """


def normalize_summary(row: dict[str, Any]) -> dict[str, Any]:
    gross = fnum(row.get("gross_buy_cost"))
    return {
        "market_count": int(fnum(row.get("market_count"))),
        "positive_xuan_count": int(fnum(row.get("positive_xuan_count"))),
        "gross_buy_cost": fnum(row.get("gross_buy_cost")),
        "pair_pnl": fnum(row.get("pair_pnl")),
        "official_taker_fee": fnum(row.get("official_taker_fee")),
        "core_pair_after_fee_pnl": fnum(row.get("core_pair_after_fee_pnl")),
        "core_pair_after_fee_roi": fnum(row.get("core_pair_after_fee_pnl")) / gross if gross else 0.0,
        "actual_settlement_residual_pnl_posthoc": fnum(
            row.get("actual_settlement_residual_pnl_posthoc")
        ),
        "xuan_after_fee_pnl_including_residual_settlement": fnum(
            row.get("xuan_after_fee_pnl_including_residual_settlement")
        ),
        "market_end_residual_cost": fnum(row.get("market_end_residual_cost")),
        "market_end_residual_qty": fnum(row.get("market_end_residual_qty")),
        "residual_cost_share": fnum(row.get("market_end_residual_cost")) / gross if gross else 0.0,
        "zero_stress_after_fee_pnl": fnum(row.get("zero_stress_after_fee_pnl")),
        "merge_recovered_capital": fnum(row.get("merge_recovered_capital")),
        "residual_settlement_dependent_positive_count": int(
            fnum(row.get("residual_settlement_dependent_positive_count"))
        ),
        "positive_core_but_residual_heavy_count": int(
            fnum(row.get("positive_core_but_residual_heavy_count"))
        ),
        "low_residual_core_pair_pass_count": int(fnum(row.get("low_residual_core_pair_pass_count"))),
    }


def build_card(poly_bt_root: Path) -> dict[str, Any]:
    ps = paths(poly_bt_root)
    missing = [name for name, path in ps.items() if not path.exists()]
    if missing:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_candidate_triage",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_CANDIDATE_TRIAGE_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "inputs": {name: str(path) for name, path in ps.items()},
            "missing_inputs": missing,
            "decision": {
                "candidate_triage_ready": False,
                "future_remote_allowed_by_this_scorer": False,
                "candidate_import_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
            },
        }

    strategy_review = load_json(ps["strategy_review"])
    rescore_manifest = load_json(ps["rescore_manifest"])
    capital_ledger = load_json(ps["capital_ledger"])
    con = connect_duckdb(ps["rescore_duckdb"])
    try:
        overall = normalize_summary(one_dict(con, aggregate_sql()))
        low_residual = normalize_summary(one_dict(con, aggregate_sql("low_residual_core_pair_pass")))
        primary_assets = "', '".join(THRESHOLDS["primary_assets"])
        primary_low_residual = normalize_summary(
            one_dict(
                con,
                aggregate_sql(
                    f"low_residual_core_pair_pass and asset in ('{primary_assets}')"
                ),
            )
        )
        other_low_residual = normalize_summary(
            one_dict(
                con,
                aggregate_sql(
                    f"low_residual_core_pair_pass and asset not in ('{primary_assets}')"
                ),
            )
        )
        residual_heavy = normalize_summary(one_dict(con, aggregate_sql("positive_core_but_residual_heavy")))
        settlement_dependent = normalize_summary(
            one_dict(con, aggregate_sql("residual_settlement_dependent_positive"))
        )
        by_asset = [
            {
                **row,
                "market_count": int(fnum(row.get("market_count"))),
                "low_residual_core_pair_pass_count": int(
                    fnum(row.get("low_residual_core_pair_pass_count"))
                ),
                "positive_core_but_residual_heavy_count": int(
                    fnum(row.get("positive_core_but_residual_heavy_count"))
                ),
                "residual_settlement_dependent_positive_count": int(
                    fnum(row.get("residual_settlement_dependent_positive_count"))
                ),
                "gross_buy_cost": fnum(row.get("gross_buy_cost")),
                "core_pair_after_fee_pnl": fnum(row.get("core_pair_after_fee_pnl")),
                "zero_stress_after_fee_pnl": fnum(row.get("zero_stress_after_fee_pnl")),
                "market_end_residual_cost": fnum(row.get("market_end_residual_cost")),
                "residual_cost_share": (
                    fnum(row.get("market_end_residual_cost")) / fnum(row.get("gross_buy_cost"))
                    if fnum(row.get("gross_buy_cost"))
                    else 0.0
                ),
            }
            for row in many_dicts(
                con,
                """
                select
                  asset,
                  count(*) as market_count,
                  sum(gross_buy_cost) as gross_buy_cost,
                  sum(core_pair_after_fee_pnl) as core_pair_after_fee_pnl,
                  sum(zero_stress_after_fee_pnl) as zero_stress_after_fee_pnl,
                  sum(market_end_residual_cost) as market_end_residual_cost,
                  sum(case when low_residual_core_pair_pass then 1 else 0 end)
                    as low_residual_core_pair_pass_count,
                  sum(case when positive_core_but_residual_heavy then 1 else 0 end)
                    as positive_core_but_residual_heavy_count,
                  sum(case when residual_settlement_dependent_positive then 1 else 0 end)
                    as residual_settlement_dependent_positive_count
                from m
                group by asset
                order by asset
                """,
            )
        ]
        top_candidates = many_dicts(
            con,
            """
            select
              asset,
              cast(day as varchar) as day,
              slug,
              selected_seed_actions,
              gross_buy_cost,
              pair_pnl,
              official_taker_fee,
              core_pair_after_fee_pnl,
              actual_settlement_residual_pnl as actual_settlement_residual_pnl_posthoc,
              xuan_after_fee_pnl as xuan_after_fee_pnl_including_residual_settlement,
              market_end_residual_cost,
              market_end_residual_qty,
              residual_cost_share,
              zero_stress_after_fee_pnl,
              capital_turnover,
              avg_residual_age_s
            from m
            where low_residual_core_pair_pass
            order by core_pair_after_fee_pnl desc, zero_stress_after_fee_pnl desc
            limit 50
            """,
        )
    finally:
        con.close()

    candidate_ready = low_residual["market_count"] > 0 and low_residual["core_pair_after_fee_pnl"] > 0
    primary_ready = (
        primary_low_residual["market_count"] > 0
        and primary_low_residual["zero_stress_after_fee_pnl"] > 0
    )
    review_decision = strategy_review.get("decision") if isinstance(strategy_review.get("decision"), dict) else {}
    capital_summary = capital_ledger.get("summary") if isinstance(capital_ledger.get("summary"), dict) else {}
    decision = {
        "candidate_triage_ready": candidate_ready,
        "primary_btc_eth_low_residual_lane_ready": primary_ready,
        "other_asset_low_residual_holdout_lane_ready": other_low_residual["market_count"] > 0,
        "residual_settlement_dependent_candidates_excluded": True,
        "positive_core_residual_heavy_candidates_excluded_from_mainline": True,
        "strategy_research_ready": bool(review_decision.get("strategy_research_ready")),
        "strategy_promotion_ready": False,
        "private_truth_ready": False,
        "deployable": False,
        "live_orders_allowed": False,
        "candidate_import_allowed": False,
        "remote_runner_allowed": False,
        "future_remote_allowed_by_this_scorer": False,
        "runner_support_ready": False,
        "next_lane": (
            "local_only_candidate_filter_package_and_holdout;"
            " do_not_use_residual_settlement_as_edge"
        ),
    }

    return rounded(
        {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_candidate_triage",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_CANDIDATE_TRIAGE_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "script": "scripts/xuan_shadow_review_backtest_v1_low_residual_candidate_triage.py",
            "inputs": {name: str(path) for name, path in ps.items()},
            "thresholds": THRESHOLDS,
            "decision": decision,
            "overall_summary": overall,
            "low_residual_core_pair_summary": low_residual,
            "primary_btc_eth_low_residual_summary": primary_low_residual,
            "other_asset_low_residual_summary": other_low_residual,
            "excluded_positive_core_residual_heavy_summary": residual_heavy,
            "excluded_residual_settlement_dependent_summary": settlement_dependent,
            "by_asset": by_asset,
            "top_low_residual_core_pair_candidates": rounded(top_candidates),
            "capital_context": {
                "max_capital_tied": fnum(capital_summary.get("max_capital_tied")),
                "daily_capacity_estimate_at_notional": fnum(
                    capital_summary.get("daily_capacity_estimate_at_notional")
                ),
                "turnover_adjusted_roi_on_max_capital": fnum(
                    capital_summary.get("turnover_adjusted_roi_on_max_capital")
                ),
            },
            "source_status": {
                "strategy_review_status": strategy_review.get("status"),
                "completion_rescore_status": rescore_manifest.get("status"),
                "capital_ledger_status": capital_ledger.get("status"),
            },
            "blockers": [
                "owner_private_truth_not_ready",
                "btc_parity_not_proven_for_promotion",
                "candidate_import_not_authorized",
                "runner_support_not_built",
            ],
            "warnings": [
                "posthoc_residual_settlement_pnl_excluded_from_core_edge",
                "other_asset_low_residual_candidates_need_holdout_before_mainline",
                "positive_core_residual_heavy_candidates_need_filtering_not_import",
                "research_only_capacity_estimates_are_not_live_pnl_promises",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card.get("decision") if isinstance(card.get("decision"), dict) else {}
    low = card.get("low_residual_core_pair_summary") if isinstance(card.get("low_residual_core_pair_summary"), dict) else {}
    primary = card.get("primary_btc_eth_low_residual_summary") if isinstance(card.get("primary_btc_eth_low_residual_summary"), dict) else {}
    other = card.get("other_asset_low_residual_summary") if isinstance(card.get("other_asset_low_residual_summary"), dict) else {}
    lines = [
        "# Backtest V1 Low-Residual Candidate Triage",
        "",
        f"- status: `{card.get('status')}`",
        f"- created_utc: `{card.get('created_utc')}`",
        f"- candidate_triage_ready: `{d.get('candidate_triage_ready')}`",
        f"- primary_btc_eth_low_residual_lane_ready: `{d.get('primary_btc_eth_low_residual_lane_ready')}`",
        f"- candidate_import_allowed: `{d.get('candidate_import_allowed')}`",
        f"- remote_runner_allowed: `{d.get('remote_runner_allowed')}`",
        "",
        "## Thresholds",
        "",
        f"- max_low_residual_cost_share: `{THRESHOLDS['max_low_residual_cost_share']}`",
        "- core edge: `pair_pnl - official_taker_fee > 0`",
        "- residual survival: `pair_pnl - official_taker_fee - market_end_residual_cost > 0`",
        "- residual settlement PnL is excluded from edge.",
        "",
        "## Main Buckets",
        "",
        "| bucket | markets | core_pair_after_fee | residual_cost | zero_stress_after_fee |",
        "| --- | ---: | ---: | ---: | ---: |",
        "| all low-residual core pair | {markets} | {core} | {residual} | {zero} |".format(
            markets=low.get("market_count"),
            core=low.get("core_pair_after_fee_pnl"),
            residual=low.get("market_end_residual_cost"),
            zero=low.get("zero_stress_after_fee_pnl"),
        ),
        "| BTC/ETH low-residual core pair | {markets} | {core} | {residual} | {zero} |".format(
            markets=primary.get("market_count"),
            core=primary.get("core_pair_after_fee_pnl"),
            residual=primary.get("market_end_residual_cost"),
            zero=primary.get("zero_stress_after_fee_pnl"),
        ),
        "| other-asset low-residual holdout | {markets} | {core} | {residual} | {zero} |".format(
            markets=other.get("market_count"),
            core=other.get("core_pair_after_fee_pnl"),
            residual=other.get("market_end_residual_cost"),
            zero=other.get("zero_stress_after_fee_pnl"),
        ),
        "",
        "## By Asset",
        "",
        "| asset | markets | low-res pass | residual-heavy positive core | settlement-dependent positive |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in card.get("by_asset", []):
        lines.append(
            "| {asset} | {markets} | {low} | {heavy} | {settle} |".format(
                asset=row.get("asset"),
                markets=row.get("market_count"),
                low=row.get("low_residual_core_pair_pass_count"),
                heavy=row.get("positive_core_but_residual_heavy_count"),
                settle=row.get("residual_settlement_dependent_positive_count"),
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The mainline should start from low-residual core pair candidates, not residual settlement winners.",
            "- BTC/ETH have the cleanest primary lane. Other low-residual assets can be holdout research, not immediate mainline.",
            "- Residual-heavy positive-core rows are useful for filter design but should not be imported.",
            "- No runner/import/remote action is authorized by this local scorer.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--poly-bt-root",
        type=Path,
        default=Path(os.environ.get("POLY_BT_ROOT", str(DEFAULT_POLY_BT_ROOT))),
    )
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build_card(args.poly_bt_root)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
