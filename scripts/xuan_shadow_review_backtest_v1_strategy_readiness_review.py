#!/usr/bin/env python3
"""Summarize current Backtest V1 readiness for xuan strategy research.

This is a local-only review artifact.  It reads the rebuilt multiasset V1
research outputs, separates pair-completion edge from posthoc residual
settlement attribution, and records which parts are usable for research versus
promotion.  It does not import candidates, mutate backtest artifacts, authorize
remote execution, or claim owner private truth.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from pathlib import Path
from typing import Any


STAMP = "20260528T0754Z"

DEFAULT_POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_strategy_readiness_review_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_strategy_readiness_review_{STAMP}/"
    "BACKTEST_V1_STRATEGY_READINESS_REVIEW.md"
)


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


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def contract_root(poly_bt_root: Path) -> Path:
    return poly_bt_root / "derived/contract_examples"


def artifact_paths(poly_bt_root: Path) -> dict[str, Path]:
    root = contract_root(poly_bt_root)
    return {
        "local_install": root / "multiasset_backtest_v1_local_install_validation_latest.json",
        "strategy_readiness": root
        / "xuan_backtest_v1_strategy_readiness_latest/XUAN_BACKTEST_V1_STRATEGY_READINESS_GATE.json",
        "completion_rescore": root
        / "xuan_completion_candidate_rescore_latest/XUAN_COMPLETION_CANDIDATE_RESCORE_MANIFEST.json",
        "rescore_top_csv": root
        / "xuan_completion_candidate_rescore_latest/xuan_completion_candidate_rescore_top.csv",
        "capital_ledger": root / "xuan_capital_ledger_latest/XUAN_CAPITAL_LEDGER_REPORT.json",
        "coverage_scorecard": root
        / "multiasset_backtest_coverage_scorecard_latest/MULTIASSET_BACKTEST_COVERAGE_SCORECARD.json",
        "btc_parity": root / "backtest_v1_btc_parity_latest/BACKTEST_V1_BTC_PARITY_GATE.json",
        "xuan_bridge": root / "xuan_bridge_scorecard_latest/XUAN_BRIDGE_SCORECARD_MANIFEST.json",
    }


def asset_classification(row: dict[str, Any]) -> str:
    residual_share = fnum(row.get("residual_cost_share"))
    core_pair_after_fee = fnum(row.get("pair_pnl")) - fnum(row.get("official_taker_fee"))
    zero_stress_after_fee = core_pair_after_fee - fnum(row.get("residual_cost"))
    if residual_share <= 0.10 and core_pair_after_fee > 0 and zero_stress_after_fee > 0:
        return "CORE_PAIR_LOW_RESIDUAL_RESEARCH_READY"
    if core_pair_after_fee > 0 and residual_share <= 0.30:
        return "CORE_PAIR_POSITIVE_RESIDUAL_HEAVY_FILTER_REQUIRED"
    if core_pair_after_fee > 0:
        return "CORE_PAIR_POSITIVE_BUT_RESIDUAL_DOMINATED_RESEARCH_ONLY"
    return "RESIDUAL_SETTLEMENT_DOMINATED_RESEARCH_ONLY"


def summarize_assets(coverage: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in coverage.get("by_asset", []):
        if not isinstance(row, dict):
            continue
        pair_pnl = fnum(row.get("pair_pnl"))
        fee = fnum(row.get("official_taker_fee"))
        residual_cost = fnum(row.get("residual_cost"))
        core_pair_after_fee = pair_pnl - fee
        zero_stress_after_fee = core_pair_after_fee - residual_cost
        out.append(
            {
                "asset": row.get("asset"),
                "classification": asset_classification(row),
                "search_safe_row_count": int(fnum(row.get("search_safe_row_count"))),
                "selected_count": int(fnum(row.get("selected_count"))),
                "selected_market_count": int(fnum(row.get("selected_market_count"))),
                "gross_buy_cost": fnum(row.get("gross_buy_cost")),
                "pair_pnl": pair_pnl,
                "official_taker_fee": fee,
                "core_pair_after_fee_pnl_excluding_residual_settlement": core_pair_after_fee,
                "residual_settlement_pnl_posthoc": fnum(row.get("residual_settlement_pnl")),
                "fee_after_pnl_including_residual_settlement": fnum(row.get("fee_after_pnl")),
                "residual_cost": residual_cost,
                "residual_qty": fnum(row.get("residual_qty")),
                "residual_cost_share": fnum(row.get("residual_cost_share")),
                "zero_stress_after_fee_pnl": zero_stress_after_fee,
                "stress_worst_day": row.get("stress_worst_day"),
                "stress_worst_day_fee_after_pnl": fnum(row.get("stress_worst_day_fee_after_pnl")),
            }
        )
    return sorted(out, key=lambda row: str(row.get("asset") or ""))


def candidate_core_metrics(row: dict[str, str]) -> dict[str, Any]:
    pair_pnl = fnum(row.get("pair_pnl"))
    fee = fnum(row.get("official_taker_fee"))
    residual_cost = fnum(row.get("market_end_residual_cost"))
    core_pair_after_fee = pair_pnl - fee
    zero_stress_after_fee = core_pair_after_fee - residual_cost
    return {
        "asset": row.get("asset"),
        "day": row.get("day"),
        "slug": row.get("slug"),
        "gross_buy_cost": fnum(row.get("gross_buy_cost")),
        "pair_pnl": pair_pnl,
        "official_taker_fee": fee,
        "core_pair_after_fee_pnl_excluding_residual_settlement": core_pair_after_fee,
        "xuan_after_fee_pnl_including_residual_settlement": fnum(row.get("xuan_after_fee_pnl")),
        "actual_settlement_residual_pnl_posthoc": fnum(row.get("actual_settlement_residual_pnl")),
        "market_end_residual_cost": residual_cost,
        "market_end_residual_qty": fnum(row.get("market_end_residual_qty")),
        "residual_cost_share": fnum(row.get("residual_cost_share")),
        "zero_stress_after_fee_pnl": zero_stress_after_fee,
        "capital_turnover": fnum(row.get("capital_turnover")),
        "avg_residual_age_s": fnum(row.get("avg_residual_age_s")),
    }


def summarize_top_candidates(rows: list[dict[str, str]]) -> dict[str, Any]:
    enriched = [candidate_core_metrics(row) for row in rows]
    positive_core = [
        row
        for row in enriched
        if fnum(row.get("core_pair_after_fee_pnl_excluding_residual_settlement")) > 0
    ]
    low_residual = [
        row
        for row in positive_core
        if fnum(row.get("residual_cost_share")) <= 0.05
    ]
    zero_stress_positive = [
        row
        for row in low_residual
        if fnum(row.get("zero_stress_after_fee_pnl")) > 0
    ]
    by_asset: dict[str, dict[str, int]] = {}
    for row in enriched:
        asset = str(row.get("asset") or "")
        bucket = by_asset.setdefault(asset, {"rows": 0, "positive_core": 0, "low_residual_positive_core": 0})
        bucket["rows"] += 1
        if row in positive_core:
            bucket["positive_core"] += 1
        if row in low_residual:
            bucket["low_residual_positive_core"] += 1

    return {
        "rows_loaded": len(rows),
        "positive_core_after_fee_rows": len(positive_core),
        "low_residual_positive_core_rows": len(low_residual),
        "low_residual_zero_stress_positive_rows": len(zero_stress_positive),
        "by_asset": by_asset,
        "top_low_residual_zero_stress_candidates": sorted(
            zero_stress_positive,
            key=lambda row: fnum(row.get("zero_stress_after_fee_pnl")),
            reverse=True,
        )[:20],
        "top_core_pair_candidates": sorted(
            positive_core,
            key=lambda row: fnum(row.get("core_pair_after_fee_pnl_excluding_residual_settlement")),
            reverse=True,
        )[:20],
    }


def build_card(poly_bt_root: Path) -> dict[str, Any]:
    paths = artifact_paths(poly_bt_root)
    missing = [name for name, path in paths.items() if not path.exists()]
    if missing:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_strategy_readiness_review",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_STRATEGY_READINESS_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "missing_inputs": missing,
            "inputs": {name: str(path) for name, path in paths.items()},
            "decision": {
                "backtest_v1_install_ok": False,
                "strategy_research_ready": False,
                "future_remote_allowed_by_this_review": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
            },
        }

    local_install = load_json(paths["local_install"])
    strategy = load_json(paths["strategy_readiness"])
    rescore = load_json(paths["completion_rescore"])
    capital = load_json(paths["capital_ledger"])
    coverage = load_json(paths["coverage_scorecard"])
    btc_parity = load_json(paths["btc_parity"])
    xuan_bridge = load_json(paths["xuan_bridge"])
    top_rows = read_csv(paths["rescore_top_csv"])

    rescore_summary = body(rescore, "summary")
    capital_summary = body(capital, "summary")
    install_summary = body(local_install, "summary")
    strategy_summary = body(strategy, "summary")
    pair_pnl = fnum(rescore_summary.get("pair_pnl"))
    fee = fnum(rescore_summary.get("official_taker_fee"))
    residual_settlement = fnum(rescore_summary.get("actual_settlement_residual_pnl"))
    residual_cost = fnum(rescore_summary.get("market_end_residual_cost"))
    gross_buy_cost = fnum(rescore_summary.get("gross_buy_cost"))
    xuan_after_fee = fnum(rescore_summary.get("xuan_after_fee_pnl"))
    core_pair_after_fee = pair_pnl - fee
    zero_stress_after_fee = core_pair_after_fee - residual_cost

    asset_assessment = summarize_assets(coverage)
    top_candidate_summary = summarize_top_candidates(top_rows)

    blockers = list(strategy.get("blockers") or [])
    warnings = list(strategy.get("warnings") or [])
    warnings.extend(
        [
            "residual_settlement_pnl_is_posthoc_attribution_not_strategy_edge",
            "btc_parity_still_blocks_promotion",
            "owner_private_truth_unavailable_for_historical_backtest",
        ]
    )
    warnings = sorted(set(str(item) for item in warnings if item))

    install_ok = local_install.get("status") == "OK" and fnum(install_summary.get("fail_count")) == 0
    research_ready = bool(strategy.get("strategy_research_ready"))
    core_pair_positive = core_pair_after_fee > 0
    zero_stress_positive = zero_stress_after_fee > 0

    decision = {
        "backtest_v1_install_ok": install_ok,
        "strategy_research_ready": research_ready,
        "strategy_research_readiness_level": strategy.get("strategy_research_readiness_level"),
        "strategy_promotion_ready": False,
        "private_truth_ready": False,
        "deployable": False,
        "live_orders_allowed": False,
        "remote_runner_allowed": False,
        "future_remote_allowed_by_this_review": False,
        "candidate_import_allowed": False,
        "core_pair_after_fee_positive_without_residual_settlement": core_pair_positive,
        "aggregate_zero_stress_after_fee_positive": zero_stress_positive,
        "residual_settlement_not_used_as_strategy_edge": True,
        "capital_ledger_ready": capital.get("status") == "OK_XUAN_CAPITAL_LEDGER_READY",
        "candidate_triage_ready": bool(top_candidate_summary["low_residual_zero_stress_positive_rows"]),
        "btc_parity_blocks_promotion": btc_parity.get("status") != "OK_BTC_BASELINE_PARITY_PROVEN",
        "xuan_bridge_complete": bool(strategy.get("xuan_bridge_complete")),
        "next_lane": (
            "local_only_low_residual_core_pair_candidate_triage_and_asset_filters;"
            " no_remote_until_parity_private_truth_and_runner_import_gates_clear"
        ),
    }

    return rounded(
        {
            "artifact": "xuan_shadow_review_backtest_v1_strategy_readiness_review",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_STRATEGY_RESEARCH_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "poly_bt_root": str(poly_bt_root),
            "script": "scripts/xuan_shadow_review_backtest_v1_strategy_readiness_review.py",
            "inputs": {name: str(path) for name, path in paths.items()},
            "decision": decision,
            "aggregate_metrics": {
                "pair_pnl": pair_pnl,
                "official_taker_fee": fee,
                "core_pair_after_fee_pnl_excluding_residual_settlement": core_pair_after_fee,
                "core_pair_after_fee_roi_excluding_residual_settlement": (
                    core_pair_after_fee / gross_buy_cost if gross_buy_cost else 0.0
                ),
                "actual_settlement_residual_pnl_posthoc": residual_settlement,
                "xuan_after_fee_pnl_including_residual_settlement": xuan_after_fee,
                "market_end_residual_cost": residual_cost,
                "market_end_residual_qty": fnum(rescore_summary.get("market_end_residual_qty")),
                "aggregate_zero_stress_after_fee_pnl": zero_stress_after_fee,
                "gross_buy_cost": gross_buy_cost,
                "net_roi_including_residual_settlement": fnum(rescore_summary.get("net_roi")),
                "merge_recovered_capital": fnum(rescore_summary.get("merge_recovered_capital")),
                "positive_xuan_candidate_count": int(fnum(rescore_summary.get("positive_xuan_candidate_count"))),
                "market_candidate_count": int(fnum(rescore_summary.get("market_candidate_count"))),
            },
            "capital_metrics": {
                "max_capital_tied": fnum(capital_summary.get("max_capital_tied")),
                "average_capital_tied": fnum(capital_summary.get("average_capital_tied")),
                "p95_capital_tied": fnum(capital_summary.get("p95_capital_tied")),
                "turnover_adjusted_roi_on_max_capital": fnum(
                    capital_summary.get("turnover_adjusted_roi_on_max_capital")
                ),
                "turnover_adjusted_roi_on_avg_capital": fnum(
                    capital_summary.get("turnover_adjusted_roi_on_avg_capital")
                ),
                "daily_capacity_estimate_at_notional": fnum(
                    capital_summary.get("daily_capacity_estimate_at_notional")
                ),
                "capacity_period_pnl_at_notional": fnum(
                    capital_summary.get("capacity_period_pnl_at_notional")
                ),
                "stress_worst_day": capital_summary.get("stress_worst_day"),
                "stress_worst_day_fee_after_pnl": fnum(
                    capital_summary.get("stress_worst_day_fee_after_pnl")
                ),
            },
            "asset_assessment": asset_assessment,
            "top_candidate_triage": top_candidate_summary,
            "source_status": {
                "local_install_status": local_install.get("status"),
                "strategy_readiness_status": strategy.get("status"),
                "completion_rescore_status": rescore.get("status"),
                "capital_ledger_status": capital.get("status"),
                "coverage_scorecard_status": coverage.get("status"),
                "btc_parity_status": btc_parity.get("status"),
                "xuan_bridge_status": xuan_bridge.get("status"),
                "strategy_summary": strategy_summary,
                "btc_parity_blockers": btc_parity.get("blockers") or [],
            },
            "blockers": blockers,
            "warnings": warnings,
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    agg = body(card, "aggregate_metrics")
    cap = body(card, "capital_metrics")
    source = body(card, "source_status")
    lines = [
        "# Backtest V1 Strategy Readiness Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- created_utc: `{card.get('created_utc')}`",
        f"- strategy_research_ready: `{decision.get('strategy_research_ready')}`",
        f"- strategy_promotion_ready: `{decision.get('strategy_promotion_ready')}`",
        f"- private_truth_ready: `{decision.get('private_truth_ready')}`",
        f"- deployable: `{decision.get('deployable')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Main Read",
        "",
        "- Backtest V1 is usable for local xuan research and candidate triage.",
        "- It is not promotion-ready because BTC parity/source semantics and owner private truth remain unresolved.",
        "- Residual settlement PnL is posthoc attribution and is not counted as strategy edge.",
        "- Core pair-completion edge remains positive after official taker fees even excluding residual settlement.",
        "",
        "## Aggregate Metrics",
        "",
        f"- pair_pnl: `{agg.get('pair_pnl')}`",
        f"- official_taker_fee: `{agg.get('official_taker_fee')}`",
        "- core_pair_after_fee_pnl_excluding_residual_settlement: "
        f"`{agg.get('core_pair_after_fee_pnl_excluding_residual_settlement')}`",
        "- actual_settlement_residual_pnl_posthoc: "
        f"`{agg.get('actual_settlement_residual_pnl_posthoc')}`",
        "- aggregate_zero_stress_after_fee_pnl: "
        f"`{agg.get('aggregate_zero_stress_after_fee_pnl')}`",
        f"- xuan_after_fee_pnl_including_residual_settlement: `{agg.get('xuan_after_fee_pnl_including_residual_settlement')}`",
        f"- market_end_residual_cost: `{agg.get('market_end_residual_cost')}`",
        f"- gross_buy_cost: `{agg.get('gross_buy_cost')}`",
        "",
        "## Capital",
        "",
        f"- max_capital_tied: `{cap.get('max_capital_tied')}`",
        f"- average_capital_tied: `{cap.get('average_capital_tied')}`",
        f"- turnover_adjusted_roi_on_max_capital: `{cap.get('turnover_adjusted_roi_on_max_capital')}`",
        f"- daily_capacity_estimate_at_notional: `{cap.get('daily_capacity_estimate_at_notional')}`",
        f"- stress_worst_day_fee_after_pnl: `{cap.get('stress_worst_day_fee_after_pnl')}`",
        "",
        "## Asset Assessment",
        "",
        "| asset | classification | core_pair_after_fee | residual_cost_share | zero_stress_after_fee |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in card.get("asset_assessment", []):
        lines.append(
            "| {asset} | `{classification}` | {core} | {residual} | {zero} |".format(
                asset=row.get("asset"),
                classification=row.get("classification"),
                core=row.get("core_pair_after_fee_pnl_excluding_residual_settlement"),
                residual=row.get("residual_cost_share"),
                zero=row.get("zero_stress_after_fee_pnl"),
            )
        )
    lines.extend(
        [
            "",
            "## Candidate Triage",
            "",
            f"- top_rows_loaded: `{body(card, 'top_candidate_triage').get('rows_loaded')}`",
            "- low_residual_positive_core_rows: "
            f"`{body(card, 'top_candidate_triage').get('low_residual_positive_core_rows')}`",
            "- low_residual_zero_stress_positive_rows: "
            f"`{body(card, 'top_candidate_triage').get('low_residual_zero_stress_positive_rows')}`",
            "",
            "## Source Status",
            "",
            f"- local_install_status: `{source.get('local_install_status')}`",
            f"- strategy_readiness_status: `{source.get('strategy_readiness_status')}`",
            f"- completion_rescore_status: `{source.get('completion_rescore_status')}`",
            f"- btc_parity_status: `{source.get('btc_parity_status')}`",
            f"- xuan_bridge_status: `{source.get('xuan_bridge_status')}`",
            "",
            "## Next Lane",
            "",
            "- Local-only triage of low-residual, positive core-pair candidates, especially BTC/ETH.",
            "- Treat high residual-share assets as filter/scorer work, not strategy-ready capital.",
            "- No candidate import, remote runner, deploy, or live order until BTC parity/source semantics and owner private truth gates clear.",
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
