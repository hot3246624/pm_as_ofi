#!/usr/bin/env python3
"""Compare xuan no-order runtime metrics against public leaderboard benchmarks."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_float(value: Any, default: float | None = 0.0) -> float | None:
    if value is None or value == "":
        return default
    try:
        val = float(value)
    except (TypeError, ValueError):
        return default
    return val if math.isfinite(val) else default


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def build(args: argparse.Namespace) -> dict[str, Any]:
    public_card_path = Path(args.public_benchmark_scorecard).expanduser().resolve()
    capital_path = Path(args.capital_roi_scorecard).expanduser().resolve()
    runtime_path = Path(args.runtime_scorecard).expanduser().resolve()
    public_card = read_json(public_card_path)
    capital = read_json(capital_path)
    runtime = read_json(runtime_path)

    targets = public_card.get("benchmark_targets", {})
    review_targets = targets.get("xuan_capacity_ladder_review_targets", {})
    capital_agg = capital.get("aggregate", {})
    round_roi = capital_agg.get("round_roi", {})
    cap_totals = capital_agg.get("totals", {})
    runtime_agg = runtime.get("aggregate") or runtime.get("metrics", {})

    edge_on_redeem = as_float(round_roi.get("edge_on_redeem_notional"), None)
    roi_on_total_cash_spend = as_float(round_roi.get("roi_on_total_cash_spend"), None)
    pair_pnl = as_float(cap_totals.get("pair_pnl"), as_float(runtime_agg.get("pair_pnl"), None))
    pair_qty = as_float(cap_totals.get("pair_qty"), None)
    actual_pair_cost = 1.0 - edge_on_redeem if edge_on_redeem is not None else None
    residual_qty_share = as_float(runtime_agg.get("residual_qty_share"), None)
    residual_cost_share = as_float(runtime_agg.get("residual_cost_share"), None)
    strict_rescue_closes = as_float(runtime_agg.get("strict_rescue_closes"), None)
    filled_qty = as_float(cap_totals.get("filled_qty"), None)
    filled_cost = as_float(cap_totals.get("filled_cost"), None)
    residual_qty = as_float(cap_totals.get("residual_qty"), None)
    residual_cost = as_float(cap_totals.get("residual_cost"), None)
    cap_strict_rescue_closes = as_float(cap_totals.get("strict_rescue_closes"), None)
    if residual_qty_share is None and residual_qty is not None and filled_qty:
        residual_qty_share = residual_qty / filled_qty
    if residual_cost_share is None and residual_cost is not None and filled_cost:
        residual_cost_share = residual_cost / filled_cost
    if (strict_rescue_closes is None or strict_rescue_closes <= 0) and cap_strict_rescue_closes is not None:
        strict_rescue_closes = cap_strict_rescue_closes

    b55_pair_cost = as_float(targets.get("b55_actual_pair_cost"), None)
    b55_edge = as_float(targets.get("b55_pair_edge"), None)
    b55_residual = as_float(targets.get("b55_residual_rate"), None)
    ce25_residual = as_float(targets.get("ce25_residual_rate"), None)
    target_pair_cost = as_float(review_targets.get("target_actual_pair_cost_lte"), None)
    review_pair_cost = as_float(review_targets.get("review_actual_pair_cost_lte"), None)
    target_residual = as_float(review_targets.get("target_residual_rate_lte"), None)
    hard_residual = as_float(review_targets.get("hard_residual_rate_lte"), None)

    hard_blockers: list[str] = []
    soft_warnings: list[str] = []
    if actual_pair_cost is None:
        hard_blockers.append("missing_actual_pair_cost")
    elif review_pair_cost is not None and actual_pair_cost > review_pair_cost:
        hard_blockers.append("actual_pair_cost_above_public_review_target")
    elif target_pair_cost is not None and actual_pair_cost > target_pair_cost:
        soft_warnings.append("actual_pair_cost_above_b55_like_target")

    if residual_qty_share is None:
        hard_blockers.append("missing_residual_qty_share")
    elif hard_residual is not None and residual_qty_share > hard_residual:
        hard_blockers.append("residual_qty_share_above_public_hard_target")
    elif target_residual is not None and residual_qty_share > target_residual:
        soft_warnings.append("residual_qty_share_above_b55_like_target")

    if pair_pnl is None or pair_pnl <= 0:
        hard_blockers.append("pair_pnl_not_positive")
    if roi_on_total_cash_spend is None:
        hard_blockers.append("missing_roi_on_total_cash_spend")
    elif roi_on_total_cash_spend <= 0:
        hard_blockers.append("roi_on_total_cash_spend_not_positive")
    if strict_rescue_closes is None or strict_rescue_closes <= 0:
        hard_blockers.append("strict_rescue_closes_missing")

    status = (
        "KEEP_PUBLIC_BENCHMARK_COMPARISON_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_PUBLIC_BENCHMARK_COMPARISON_BLOCKED"
    )
    comparison = {
        "actual_pair_cost_after_fee": round_opt(actual_pair_cost),
        "edge_on_redeem_notional_after_fee": round_opt(edge_on_redeem),
        "roi_on_total_cash_spend_after_fee": round_opt(roi_on_total_cash_spend),
        "pair_pnl_after_fee": round_opt(pair_pnl),
        "pair_qty": round_opt(pair_qty),
        "residual_qty_share": round_opt(residual_qty_share),
        "residual_cost_share": round_opt(residual_cost_share),
        "strict_rescue_closes": round_opt(strict_rescue_closes),
        "vs_b55": {
            "b55_actual_pair_cost": round_opt(b55_pair_cost),
            "b55_pair_edge": round_opt(b55_edge),
            "b55_residual_rate": round_opt(b55_residual),
            "pair_cost_delta_vs_b55": round_opt(actual_pair_cost - b55_pair_cost)
            if actual_pair_cost is not None and b55_pair_cost is not None
            else None,
            "edge_delta_vs_b55": round_opt(edge_on_redeem - b55_edge)
            if edge_on_redeem is not None and b55_edge is not None
            else None,
            "residual_qty_share_delta_vs_b55": round_opt(residual_qty_share - b55_residual)
            if residual_qty_share is not None and b55_residual is not None
            else None,
        },
        "vs_ce25": {
            "ce25_residual_rate": round_opt(ce25_residual),
            "residual_qty_share_delta_vs_ce25": round_opt(residual_qty_share - ce25_residual)
            if residual_qty_share is not None and ce25_residual is not None
            else None,
        },
    }
    return {
        "artifact": "xuan_no_order_public_benchmark_comparison_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_no_order_public_benchmark_comparison_scorer.py",
        "status": status,
        "inputs": {
            "public_benchmark_scorecard": str(public_card_path),
            "capital_roi_scorecard": str(capital_path),
            "runtime_scorecard": str(runtime_path),
        },
        "public_review_targets": {
            "target_actual_pair_cost_lte": target_pair_cost,
            "review_actual_pair_cost_lte": review_pair_cost,
            "target_residual_rate_lte": target_residual,
            "hard_residual_rate_lte": hard_residual,
            "fee_after_cash_pnl_must_be_positive": review_targets.get("fee_after_cash_pnl_must_be_positive"),
        },
        "comparison": comparison,
        "decision": {
            "research_only": True,
            "public_benchmark_comparison_pass": not hard_blockers,
            "deployable": False,
            "remote_runner_allowed": False,
            "hard_blockers": hard_blockers,
            "soft_warnings": soft_warnings,
        },
    }


def write_markdown(path: Path, card: dict[str, Any]) -> None:
    c = card["comparison"]
    b55 = c["vs_b55"]
    ce25 = c["vs_ce25"]
    d = card["decision"]
    lines = [
        "# Xuan Public Benchmark Comparison",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) if d['hard_blockers'] else 'none'}`",
        f"- soft_warnings: `{', '.join(d['soft_warnings']) if d['soft_warnings'] else 'none'}`",
        "",
        "## Candidate",
        "",
        f"- actual_pair_cost_after_fee: `{c['actual_pair_cost_after_fee']}`",
        f"- edge_on_redeem_notional_after_fee: `{c['edge_on_redeem_notional_after_fee']}`",
        f"- roi_on_total_cash_spend_after_fee: `{c['roi_on_total_cash_spend_after_fee']}`",
        f"- pair_pnl_after_fee: `{c['pair_pnl_after_fee']}`",
        f"- pair_qty: `{c['pair_qty']}`",
        f"- residual_qty_share: `{c['residual_qty_share']}`",
        f"- residual_cost_share: `{c['residual_cost_share']}`",
        f"- strict_rescue_closes: `{c['strict_rescue_closes']}`",
        "",
        "## Versus Public Benchmarks",
        "",
        f"- b55_actual_pair_cost: `{b55['b55_actual_pair_cost']}`",
        f"- b55_pair_edge: `{b55['b55_pair_edge']}`",
        f"- b55_residual_rate: `{b55['b55_residual_rate']}`",
        f"- pair_cost_delta_vs_b55: `{b55['pair_cost_delta_vs_b55']}`",
        f"- edge_delta_vs_b55: `{b55['edge_delta_vs_b55']}`",
        f"- residual_qty_share_delta_vs_b55: `{b55['residual_qty_share_delta_vs_b55']}`",
        f"- ce25_residual_rate: `{ce25['ce25_residual_rate']}`",
        f"- residual_qty_share_delta_vs_ce25: `{ce25['residual_qty_share_delta_vs_ce25']}`",
        "",
        "## Guardrails",
        "",
        "- This comparison is research-only.",
        "- Public profiles are calibration targets, not private execution truth.",
        "- Passing this scorer does not authorize live orders, deploy, restart, shared-service mutation, or remote execution.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-benchmark-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--runtime-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()
    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        write_markdown(Path(args.markdown).expanduser().resolve(), card)
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if not card["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
