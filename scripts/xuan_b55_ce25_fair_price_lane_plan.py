#!/usr/bin/env python3
"""Build a local fair-price research lane from b55/ce25 public benchmarks."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def pct(value: float | None, digits: int = 4) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value * 100.0, digits)


def find_account(public_review: dict[str, Any], label: str) -> dict[str, Any]:
    for account in public_review.get("accounts", []):
        if account.get("label") == label:
            return account
    raise SystemExit(f"public account not found: {label}")


def sum_actual(bucket: dict[str, Any], names: list[str]) -> float:
    return sum(as_float(bucket.get(name, {}).get("actual")) for name in names)


def render_markdown(card: dict[str, Any]) -> str:
    evidence = card["benchmark_evidence"]
    admission = card["admission_contract"]
    lines = [
        "# Xuan B55/Ce25 Fair-Price Lane Plan",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- lane_ready: `{card['decision']['lane_ready']}`",
        f"- remote_runner_allowed: `{card['decision']['remote_runner_allowed']}`",
        f"- deployable: `{card['decision']['deployable']}`",
        f"- hard_blockers: `{', '.join(card['decision']['hard_blockers']) or 'none'}`",
        "",
        "## Benchmark Evidence",
        "",
        f"- b55_actual_pair_cost: `{evidence['b55_actual_pair_cost']}`",
        f"- b55_pair_edge_pct: `{evidence['b55_pair_edge_pct']}`",
        f"- b55_residual_rate_pct: `{evidence['b55_residual_rate_pct']}`",
        f"- ce25_actual_pair_cost: `{evidence['ce25_actual_pair_cost']}`",
        f"- ce25_residual_rate_pct: `{evidence['ce25_residual_rate_pct']}`",
        f"- b55_preferred_entry_15m_to_1m_actual_share_pct: `{evidence['b55_preferred_entry_15m_to_1m_actual_share_pct']}`",
        f"- b55_core_price_35c_to_90c_actual_share_pct: `{evidence['b55_core_price_35c_to_90c_actual_share_pct']}`",
        f"- b55_mid_price_50c_to_80c_actual_share_pct: `{evidence['b55_mid_price_50c_to_80c_actual_share_pct']}`",
        f"- b55_btc_eth_pair_profit_share_pct: `{evidence['b55_btc_eth_pair_profit_share_pct']}`",
        "",
        "## Admission Contract",
        "",
        f"- assets: `{', '.join(admission['assets'])}`",
        f"- timeframes: `{', '.join(admission['timeframes'])}`",
        f"- entry_windows: `{', '.join(admission['entry_windows'])}`",
        f"- core_price_bands: `{', '.join(admission['core_price_bands'])}`",
        f"- target_actual_pair_cost_lte: `{admission['target_actual_pair_cost_lte']}`",
        f"- review_actual_pair_cost_lte: `{admission['review_actual_pair_cost_lte']}`",
        f"- initial_residual_target_lte: `{admission['initial_residual_target_lte']}`",
        f"- hard_residual_lte: `{admission['hard_residual_lte']}`",
        f"- fair_price_edge_min: `{admission['fair_price_edge_min']}`",
        f"- max_effective_fee_rate_hard: `{admission['max_effective_fee_rate_hard']}`",
        "",
        "## Required Fair-Price Inputs",
        "",
    ]
    for item in card["fair_price_input_contract"]["required_fields"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Research Phases", ""])
    for phase in card["research_phases"]:
        lines.append(f"- {phase['phase']}: {phase['goal']}")
    lines.extend(["", "## Kill Criteria", ""])
    for criterion in card["kill_criteria"]:
        lines.append(f"- {criterion}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is local/research-only planning.",
            "- It does not mutate localagg, shared ingress, raw/replay, production executors, or collector outputs.",
            "- It does not authorize remote execution or live orders.",
            "- b55 and ce25 are public benchmarks, not private truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    public_review = read_json(Path(args.public_benchmark_scorecard).expanduser().resolve())
    b55 = find_account(public_review, "b55")
    ce25 = find_account(public_review, "ce25")
    targets = public_review.get("benchmark_targets", {})
    strategy = b55.get("strategy_correction", {})
    timing = strategy.get("entry_timing_actual_cost", {})
    prices = strategy.get("price_band_actual_cost", {})
    pair_quality = strategy.get("pair_quality", {})
    buy_actual = as_float(b55.get("buy_actual_cost"))
    pair_profit_total = as_float(b55.get("paired_actual_profit"))
    b55_assets = b55.get("assets", {})
    btc_eth_pair_profit = as_float(b55_assets.get("BTC", {}).get("paired_actual_profit")) + as_float(
        b55_assets.get("ETH", {}).get("paired_actual_profit")
    )
    preferred_entry_actual = sum_actual(timing, ["-15~-5m", "-5~-1m"])
    core_price_actual = sum_actual(prices, ["35-50", "50-65", "65-80", "80-90"])
    mid_price_actual = sum_actual(prices, ["50-65", "65-80"])

    hard_blockers: list[str] = []
    if public_review.get("status") != "KEEP_PUBLIC_LEADERBOARD_TRADER_REVIEW_READY_RESEARCH_ONLY":
        hard_blockers.append("public_benchmark_review_not_ready")
    if targets.get("b55_cash_pnl_contains_old_inventory_redeem") is not True:
        hard_blockers.append("b55_old_inventory_cash_caveat_missing")

    admission = {
        "assets": ["BTC", "ETH"],
        "timeframes": ["15m", "1h_or_named"],
        "entry_windows": ["end_minus_15m_to_5m", "end_minus_5m_to_1m"],
        "avoid_as_primary": ["last_60s_panic_only", "5m_hard_chase_only", "SOL_XRP_until_separately_validated"],
        "core_price_bands": ["35c_to_50c", "50c_to_65c", "65c_to_80c", "80c_to_90c"],
        "preferred_price_band": "50c_to_80c",
        "target_actual_pair_cost_lte": 0.965,
        "review_actual_pair_cost_lte": 0.975,
        "non_arbitrage_pair_cost_gte": 1.0,
        "initial_residual_target_lte": 0.10,
        "review_residual_lte": 0.15,
        "hard_residual_lte": 0.20,
        "fair_price_edge_min": 0.015,
        "fair_price_edge_review_min": 0.010,
        "min_edge_on_redeem_notional_after_fee": 0.02,
        "target_edge_on_redeem_notional_after_fee": 0.035,
        "max_effective_fee_rate_target": 0.0125,
        "max_effective_fee_rate_hard": 0.0175,
    }
    card = {
        "artifact": "xuan_b55_ce25_fair_price_lane_plan",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_b55_ce25_fair_price_lane_plan.py",
        "status": "KEEP_B55_CE25_FAIR_PRICE_LANE_PLAN_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_B55_CE25_FAIR_PRICE_LANE_PLAN_BLOCKED",
        "inputs": {
            "public_benchmark_scorecard": str(Path(args.public_benchmark_scorecard).expanduser().resolve()),
        },
        "benchmark_evidence": {
            "b55_actual_pair_cost": round_opt(as_float(b55.get("actual_pair_cost"))),
            "b55_pair_edge_pct": pct(as_float(b55.get("pair_edge"))),
            "b55_gross_pair_cost": round_opt(as_float(pair_quality.get("gross_pair_cost"))),
            "b55_pair_fee_cost": round_opt(as_float(pair_quality.get("pair_fee_cost"))),
            "b55_fee_rate_on_gross_pct": pct(as_float(b55.get("fee_rate_on_gross"))),
            "b55_residual_rate_pct": pct(as_float(b55.get("residual_rate_on_bought_qty"))),
            "b55_current_residual_rate_pct": pct(as_float(b55.get("current_residual_rate"))),
            "b55_new_position_mtm_ex_rebate": targets.get("b55_new_position_mtm_ex_rebate"),
            "b55_cash_pnl_contains_old_inventory_redeem": targets.get("b55_cash_pnl_contains_old_inventory_redeem"),
            "ce25_actual_pair_cost": round_opt(as_float(ce25.get("actual_pair_cost"))),
            "ce25_residual_rate_pct": pct(as_float(ce25.get("residual_rate_on_bought_qty"))),
            "b55_preferred_entry_15m_to_1m_actual_share": round_opt(preferred_entry_actual / buy_actual if buy_actual > 0 else None),
            "b55_preferred_entry_15m_to_1m_actual_share_pct": pct(preferred_entry_actual / buy_actual if buy_actual > 0 else None),
            "b55_core_price_35c_to_90c_actual_share": round_opt(core_price_actual / buy_actual if buy_actual > 0 else None),
            "b55_core_price_35c_to_90c_actual_share_pct": pct(core_price_actual / buy_actual if buy_actual > 0 else None),
            "b55_mid_price_50c_to_80c_actual_share": round_opt(mid_price_actual / buy_actual if buy_actual > 0 else None),
            "b55_mid_price_50c_to_80c_actual_share_pct": pct(mid_price_actual / buy_actual if buy_actual > 0 else None),
            "b55_btc_eth_pair_profit_share": round_opt(btc_eth_pair_profit / pair_profit_total if pair_profit_total > 0 else None),
            "b55_btc_eth_pair_profit_share_pct": pct(btc_eth_pair_profit / pair_profit_total if pair_profit_total > 0 else None),
            "split_count_all_accounts": targets.get("split_count_all_accounts"),
        },
        "admission_contract": admission,
        "fair_price_input_contract": {
            "source_boundary": "read_only_inputs_only_no_localagg_or_shared_service_mutation",
            "required_fields": [
                "market_slug",
                "asset",
                "timeframe",
                "market_close_ts",
                "seconds_to_close",
                "polymarket_best_bid",
                "polymarket_best_ask",
                "venue_mid_binance",
                "venue_mid_coinbase",
                "venue_mid_okx",
                "venue_mid_bybit",
                "venue_sample_ts",
                "venue_latency_ms",
                "venue_outlier_flags",
                "aggregate_fair_probability",
                "fair_probability_method",
            ],
            "aggregation_rule": "median_or_liquidity_weighted_mid_with_outlier_rejection_and_latency_budget",
            "latency_budget_ms": 250,
            "outlier_policy": "drop_single_venue_jumps_and_require_at_least_three_valid_venues",
        },
        "research_phases": [
            {
                "phase": "phase_0_public_benchmark_pin",
                "goal": "keep b55/ce25 corrected PnL caveats and pair-quality targets attached to every review packet",
            },
            {
                "phase": "phase_1_local_fair_price_replay_design",
                "goal": "build local-only BTC/ETH 15m/1h admission rows with fair_probability, no shared-service mutation",
            },
            {
                "phase": "phase_2_source_truth_validation",
                "goal": "prove fair-price rows link to trade/L1/L2 source ids before any runtime claim",
            },
            {
                "phase": "phase_3_no_order_shadow_probe",
                "goal": "run bounded PM_DRY_RUN=1 no-order probes only after explicit authorization",
            },
            {
                "phase": "phase_4_capacity_ladder_merge",
                "goal": "compare cap stages against b55/ce25 pair-cost and residual gates before moving above cap_25",
            },
        ],
        "kill_criteria": [
            "fee_included_actual_pair_cost_above_0.975_on_review_window",
            "residual_qty_share_above_0.20_or_residual_cost_share_above_0.15",
            "effective_fee_rate_above_0.0175_without_offsetting_pair_cost_improvement",
            "public_pnl_or_runtime_pnl_depends_on_old_inventory_redeem_without_explicit_attribution",
            "source_linkage_missing_for_trade_l1_l2_or_fair_price_inputs",
            "positive_pnl_requires_directional_residual_not_labeled_as_directional_lane",
            "any_live_order_deploy_restart_or_shared_service_mutation_request",
        ],
        "decision": {
            "lane_ready": not hard_blockers,
            "hard_blockers": hard_blockers,
            "research_only": True,
            "remote_runner_allowed": False,
            "requires_explicit_bounded_no_order_authorization": True,
            "deployable": False,
            "live_orders_allowed": False,
            "shared_service_mutation_allowed": False,
            "localagg_mutation_allowed": False,
            "raw_replay_mutation_allowed": False,
        },
    }
    return card


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-benchmark-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["decision"]["lane_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
