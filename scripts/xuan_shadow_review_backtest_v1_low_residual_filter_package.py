#!/usr/bin/env python3
"""Build a local-only filter package for low-residual Backtest V1 candidates.

The package turns the validated low_residual_core_pair_v1 rule into concrete
CSV candidate sets plus a source/parity/private-truth checklist.  It is a
research artifact only: no candidate import, runner profile, manifest,
preauthorization, remote execution, deploy, or live order is authorized here.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
from pathlib import Path
from typing import Any


STAMP = "20260528T0834Z"

DEFAULT_POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
DEFAULT_STRATEGY_REVIEW = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_strategy_readiness_review_20260528T0754Z.json"
)
DEFAULT_TRIAGE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_low_residual_candidate_triage_20260528T0805Z.json"
)
DEFAULT_HOLDOUT = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_low_residual_holdout_scorecard_20260528T0815Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_low_residual_filter_package_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_low_residual_filter_package_{STAMP}"
)

FILTER_NAME = "low_residual_core_pair_v1"
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


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


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

SELECT_COLUMNS = """
select
  asset,
  cast(day as varchar) as day,
  condition_id,
  slug,
  first_action_ts_ms,
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
  merge_recovered_capital,
  capital_turnover,
  avg_residual_age_s
from m
"""


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_rows(con: Any, where_clause: str) -> list[dict[str, Any]]:
    return rows_to_dicts(
        con.execute(
            METRIC_CTE
            + SELECT_COLUMNS
            + f"""
            where {where_clause}
            order by asset, day, core_pair_after_fee_pnl desc, zero_stress_after_fee_pnl desc
            """
        )
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: clean(val) for key, val in row.items()})


def aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    gross = sum(fnum(row.get("gross_buy_cost")) for row in rows)
    core = sum(fnum(row.get("core_pair_after_fee_pnl")) for row in rows)
    residual_cost = sum(fnum(row.get("market_end_residual_cost")) for row in rows)
    return {
        "market_count": len(rows),
        "gross_buy_cost": gross,
        "pair_pnl": sum(fnum(row.get("pair_pnl")) for row in rows),
        "official_taker_fee": sum(fnum(row.get("official_taker_fee")) for row in rows),
        "core_pair_after_fee_pnl": core,
        "core_pair_after_fee_roi": core / gross if gross else 0.0,
        "actual_settlement_residual_pnl_posthoc": sum(
            fnum(row.get("actual_settlement_residual_pnl_posthoc")) for row in rows
        ),
        "xuan_after_fee_pnl_including_residual_settlement": sum(
            fnum(row.get("xuan_after_fee_pnl_including_residual_settlement")) for row in rows
        ),
        "market_end_residual_cost": residual_cost,
        "market_end_residual_qty": sum(fnum(row.get("market_end_residual_qty")) for row in rows),
        "residual_cost_share": residual_cost / gross if gross else 0.0,
        "zero_stress_after_fee_pnl": sum(fnum(row.get("zero_stress_after_fee_pnl")) for row in rows),
        "merge_recovered_capital": sum(fnum(row.get("merge_recovered_capital")) for row in rows),
    }


def group_counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        name = str(row.get(key) or "")
        out[name] = out.get(name, 0) + 1
    return dict(sorted(out.items()))


def checklist(strategy: dict[str, Any], triage: dict[str, Any], holdout: dict[str, Any]) -> list[dict[str, Any]]:
    strategy_decision = body(strategy, "decision")
    triage_decision = body(triage, "decision")
    holdout_decision = body(holdout, "decision")
    source_status = body(strategy, "source_status")
    return [
        {
            "check": "local_install_ok",
            "status": "PASS" if strategy_decision.get("backtest_v1_install_ok") else "BLOCKED",
            "evidence": source_status.get("local_install_status"),
        },
        {
            "check": "metric_boundaries_enforced",
            "status": "PASS" if strategy_decision.get("residual_settlement_not_used_as_strategy_edge") else "BLOCKED",
            "evidence": "core edge is pair_pnl - official_taker_fee; residual settlement is posthoc",
        },
        {
            "check": "low_residual_triage_ready",
            "status": "PASS" if triage_decision.get("candidate_triage_ready") else "BLOCKED",
            "evidence": triage.get("status"),
        },
        {
            "check": "primary_day_holdout_pass",
            "status": "PASS" if holdout_decision.get("primary_btc_eth_day_holdout_pass") else "BLOCKED",
            "evidence": body(holdout, "primary_btc_eth_day_summary"),
        },
        {
            "check": "btc_parity_for_promotion",
            "status": "BLOCKED",
            "evidence": source_status.get("btc_parity_status"),
        },
        {
            "check": "xuan_bridge_complete",
            "status": "PASS" if strategy_decision.get("xuan_bridge_complete") else "BLOCKED",
            "evidence": source_status.get("xuan_bridge_status"),
        },
        {
            "check": "owner_private_truth_ready",
            "status": "BLOCKED",
            "evidence": "historical backtest/shadow cannot provide owner private truth",
        },
        {
            "check": "runner_or_import_support_ready",
            "status": "BLOCKED",
            "evidence": "no runner profile or candidate import contract authorized by this package",
        },
    ]


def build_card(poly_bt_root: Path, strategy_path: Path, triage_path: Path, holdout_path: Path, artifact_dir: Path) -> dict[str, Any]:
    db_path = rescore_db(poly_bt_root)
    inputs = {
        "rescore_duckdb": db_path,
        "strategy_review": strategy_path,
        "triage_scorecard": triage_path,
        "holdout_scorecard": holdout_path,
    }
    missing = [name for name, path in inputs.items() if not path.exists()]
    if missing:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_filter_package",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_FILTER_PACKAGE_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "missing_inputs": missing,
            "inputs": {name: str(path) for name, path in inputs.items()},
            "decision": {
                "filter_package_ready": False,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "private_truth_ready": False,
            },
        }

    strategy = load_json(strategy_path)
    triage = load_json(triage_path)
    holdout = load_json(holdout_path)
    primary_clause = (
        "low_residual_core_pair_pass and asset in ('BTC', 'ETH')"
    )
    holdout_clause = (
        "low_residual_core_pair_pass and asset not in ('BTC', 'ETH')"
    )
    con = connect_duckdb(db_path)
    try:
        primary_rows = query_rows(con, primary_clause)
        holdout_rows = query_rows(con, holdout_clause)
    finally:
        con.close()

    primary_csv = artifact_dir / "low_residual_core_pair_v1_primary_btc_eth_candidates.csv"
    holdout_csv = artifact_dir / "low_residual_core_pair_v1_other_asset_holdout_candidates.csv"
    write_csv(primary_csv, primary_rows)
    write_csv(holdout_csv, holdout_rows)

    checks = checklist(strategy, triage, holdout)
    hard_blockers = [
        item["check"]
        for item in checks
        if item["status"] == "BLOCKED"
        and item["check"]
        in {
            "btc_parity_for_promotion",
            "xuan_bridge_complete",
            "owner_private_truth_ready",
            "runner_or_import_support_ready",
        }
    ]
    filter_ready = (
        bool(primary_rows)
        and body(holdout, "decision").get("candidate_filter_package_ready_for_local_review")
        and body(triage, "decision").get("primary_btc_eth_low_residual_lane_ready")
    )

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_low_residual_filter_package",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_LOW_RESIDUAL_FILTER_PACKAGE_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "script": "scripts/xuan_shadow_review_backtest_v1_low_residual_filter_package.py",
            "inputs": {name: str(path) for name, path in inputs.items()},
            "outputs": {
                "artifact_dir": str(artifact_dir),
                "primary_btc_eth_candidates_csv": str(primary_csv),
                "other_asset_holdout_candidates_csv": str(holdout_csv),
                "markdown": str(artifact_dir / "BACKTEST_V1_LOW_RESIDUAL_FILTER_PACKAGE.md"),
            },
            "filter_spec": {
                "name": FILTER_NAME,
                "version": "research_v1",
                "mainline_assets": list(PRIMARY_ASSETS),
                "holdout_assets": "all non-BTC/ETH assets passing the same rule",
                "sql_predicate": (
                    "positive_xuan_completion_candidate "
                    "and pair_pnl - official_taker_fee > 0 "
                    "and pair_pnl - official_taker_fee - market_end_residual_cost > 0 "
                    f"and residual_cost_share <= {MAX_RESIDUAL_COST_SHARE}"
                ),
                "metric_policy": {
                    "core_strategy_edge": "pair_pnl - official_taker_fee",
                    "residual_settlement_pnl": "posthoc attribution, excluded from edge",
                    "merge_recovered_capital": "capital recovery, not edge",
                    "market_end_residual_cost_qty": "single-leg risk; zero-stress tested",
                },
                "explicit_exclusions": [
                    "residual_settlement_dependent_positive",
                    "positive_core_but_residual_heavy",
                    "queue_screener_best_queue_pnl_as_strategy_pnl",
                ],
            },
            "decision": {
                "filter_package_ready_for_local_review": filter_ready,
                "primary_btc_eth_candidate_csv_written": bool(primary_rows),
                "other_asset_holdout_csv_written": bool(holdout_rows),
                "source_parity_private_truth_checklist_ready": True,
                "candidate_import_allowed": False,
                "runner_support_ready": False,
                "manifest_or_preauthorization_ready": False,
                "future_remote_allowed_by_this_package": False,
                "remote_runner_allowed": False,
                "strategy_promotion_ready": False,
                "private_truth_ready": False,
                "deployable": False,
                "live_orders_allowed": False,
                "next_lane": "local_only_source_parity_private_truth_review_then_optional_runner_import_contract",
            },
            "primary_btc_eth_summary": aggregate(primary_rows),
            "other_asset_holdout_summary": aggregate(holdout_rows),
            "primary_by_asset_counts": group_counts(primary_rows, "asset"),
            "holdout_by_asset_counts": group_counts(holdout_rows, "asset"),
            "source_parity_private_truth_checklist": checks,
            "hard_blockers_before_import_or_remote": hard_blockers,
            "top_primary_candidates": sorted(
                primary_rows,
                key=lambda row: fnum(row.get("core_pair_after_fee_pnl")),
                reverse=True,
            )[:25],
            "warnings": [
                "filter_package_is_research_only",
                "primary_csv_is_not_candidate_import_authorization",
                "btc_parity_owner_private_truth_and_runner_import_contract_remain_blocked",
                "other_assets_are_holdout_research_until_asset_level_residual_dependence_is_controlled",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    primary = body(card, "primary_btc_eth_summary")
    holdout = body(card, "other_asset_holdout_summary")
    lines = [
        "# Backtest V1 Low-Residual Filter Package",
        "",
        f"- status: `{card.get('status')}`",
        f"- created_utc: `{card.get('created_utc')}`",
        f"- filter_package_ready_for_local_review: `{decision.get('filter_package_ready_for_local_review')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- remote_runner_allowed: `{decision.get('remote_runner_allowed')}`",
        "",
        "## Filter",
        "",
        f"- name: `{FILTER_NAME}`",
        f"- mainline assets: `{', '.join(PRIMARY_ASSETS)}`",
        "- condition: `positive_xuan_completion_candidate and pair_pnl - official_taker_fee > 0 and pair_pnl - official_taker_fee - market_end_residual_cost > 0 and residual_cost_share <= 0.05`",
        "- residual settlement PnL is excluded from the strategy edge.",
        "",
        "## Candidate Outputs",
        "",
        f"- primary BTC/ETH CSV: `{body(card, 'outputs').get('primary_btc_eth_candidates_csv')}`",
        f"- other-asset holdout CSV: `{body(card, 'outputs').get('other_asset_holdout_candidates_csv')}`",
        "",
        "## Primary BTC/ETH Summary",
        "",
        f"- markets: `{primary.get('market_count')}`",
        f"- core_pair_after_fee_pnl: `{primary.get('core_pair_after_fee_pnl')}`",
        f"- market_end_residual_cost: `{primary.get('market_end_residual_cost')}`",
        f"- zero_stress_after_fee_pnl: `{primary.get('zero_stress_after_fee_pnl')}`",
        "",
        "## Other-Asset Holdout Summary",
        "",
        f"- markets: `{holdout.get('market_count')}`",
        f"- core_pair_after_fee_pnl: `{holdout.get('core_pair_after_fee_pnl')}`",
        f"- market_end_residual_cost: `{holdout.get('market_end_residual_cost')}`",
        f"- zero_stress_after_fee_pnl: `{holdout.get('zero_stress_after_fee_pnl')}`",
        "",
        "## Checklist",
        "",
        "| check | status | evidence |",
        "| --- | --- | --- |",
    ]
    for item in card.get("source_parity_private_truth_checklist", []):
        evidence = json.dumps(item.get("evidence"), sort_keys=True)[:180]
        lines.append(f"| `{item.get('check')}` | `{item.get('status')}` | `{evidence}` |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The package is ready for local review as a filter spec and candidate CSV, not as import authorization.",
            "- BTC/ETH are the mainline because they passed low-residual triage and every valid-day holdout.",
            "- Import/runner/remote remain blocked by BTC parity, incomplete bridge/import contract, and owner private truth.",
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
    parser.add_argument("--strategy-review", type=Path, default=DEFAULT_STRATEGY_REVIEW)
    parser.add_argument("--triage", type=Path, default=DEFAULT_TRIAGE)
    parser.add_argument("--holdout", type=Path, default=DEFAULT_HOLDOUT)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(
        args.poly_bt_root,
        args.strategy_review,
        args.triage,
        args.holdout,
        args.artifact_dir,
    )
    markdown = args.artifact_dir / "BACKTEST_V1_LOW_RESIDUAL_FILTER_PACKAGE.md"
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
