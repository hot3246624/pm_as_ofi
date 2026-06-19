#!/usr/bin/env python3
"""Gate xuan-frontier capacity stages against public pair-quality benchmarks."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def as_bool(value: Any) -> bool:
    return bool(value) is True


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def pct(value: float | None, digits: int = 4) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value * 100.0, digits)


def find_stage(capacity_plan: dict[str, Any], stage_name: str) -> dict[str, Any]:
    ladder = capacity_plan.get("ladder")
    if not isinstance(ladder, list):
        ladder = capacity_plan.get("capacity_ladder")
    for stage in ladder if isinstance(ladder, list) else []:
        if not isinstance(stage, dict):
            continue
        if stage.get("stage") == stage_name:
            return stage
    raise SystemExit(f"stage not found in capacity plan: {stage_name}")


def cmp_gate(name: str, actual: float, op: str, threshold: float) -> dict[str, Any]:
    if op == ">=":
        passed = actual >= threshold
    elif op == "<=":
        passed = actual <= threshold
    elif op == "==":
        passed = actual == threshold
    else:
        raise ValueError(f"unsupported op: {op}")
    return {
        "name": name,
        "actual": round_opt(actual),
        "op": op,
        "threshold": round_opt(threshold),
        "passed": passed,
    }


def bool_gate(name: str, actual: bool, expected: bool) -> dict[str, Any]:
    return {
        "name": name,
        "actual": actual,
        "expected": expected,
        "passed": actual is expected,
    }


def render_markdown(card: dict[str, Any]) -> str:
    metrics = card["metrics"]
    public = card["public_benchmark"]
    decision = card["decision"]
    lines = [
        "# Xuan Capacity Stage Public Benchmark Gate",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- stage: `{card['stage']}`",
        f"- capacity_stage_gate_pass: `{decision['capacity_stage_gate_pass']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        f"- soft_warnings: `{', '.join(decision['soft_warnings']) or 'none'}`",
        "",
        "## Metrics",
        "",
        f"- pair_pnl_after_fee: `{metrics['pair_pnl_after_fee']}`",
        f"- pair_qty_redeem_notional: `{metrics['pair_qty_redeem_notional']}`",
        f"- actual_pair_cost_after_fee: `{metrics['actual_pair_cost_after_fee']}`",
        f"- edge_on_redeem_notional_after_fee_pct: `{metrics['edge_on_redeem_notional_after_fee_pct']}`",
        f"- roi_on_total_cash_spend_after_fee_pct: `{metrics['roi_on_total_cash_spend_after_fee_pct']}`",
        f"- residual_qty_share_pct: `{metrics['residual_qty_share_pct']}`",
        f"- residual_cost_share_pct: `{metrics['residual_cost_share_pct']}`",
        f"- residual_cost_to_pair_qty_pct: `{metrics['residual_cost_to_pair_qty_pct']}`",
        f"- strict_rescue_closes: `{metrics['strict_rescue_closes']}`",
        f"- strict_rescue_source_blocks: `{metrics['strict_rescue_source_blocks']}`",
        f"- rescue_l1_age_ms_max: `{metrics['rescue_l1_age_ms_max']}`",
        "",
        "## Public Benchmark",
        "",
        f"- benchmark_scope: `{public['benchmark_scope']}`",
        f"- b55_actual_pair_cost: `{public['b55_actual_pair_cost']}`",
        f"- b55_pair_edge_pct: `{public['b55_pair_edge_pct']}`",
        f"- b55_residual_rate_pct: `{public['b55_residual_rate_pct']}`",
        f"- b55_new_position_mtm_ex_rebate: `{public['b55_new_position_mtm_ex_rebate']}`",
        f"- ce25_actual_pair_cost: `{public['ce25_actual_pair_cost']}`",
        f"- ce25_residual_rate_pct: `{public['ce25_residual_rate_pct']}`",
        f"- pair_cost_delta_vs_b55: `{public['pair_cost_delta_vs_b55']}`",
        f"- residual_qty_share_delta_vs_b55: `{public['residual_qty_share_delta_vs_b55']}`",
        "",
        "## Hard Gates",
        "",
    ]
    for gate in card["hard_gates"]:
        if "op" in gate:
            lines.append(
                f"- {gate['name']}: `{gate['actual']} {gate['op']} {gate['threshold']}` -> `{gate['passed']}`"
            )
        else:
            lines.append(f"- {gate['name']}: `{gate['actual']}` expected `{gate['expected']}` -> `{gate['passed']}`")
    lines.extend(["", "## Soft Review Gates", ""])
    for gate in card["soft_review_gates"]:
        lines.append(f"- {gate['name']}: `{gate['actual']} {gate['op']} {gate['threshold']}` -> `{gate['passed']}`")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This scorer is local/research-only.",
            "- It does not authorize remote execution.",
            "- It does not authorize live orders, deploy, restart, shared-ingress mutation, collector rebuild, or raw/replay mutation.",
            "- Public leaderboard accounts are benchmarks for pair quality/residual shape, not private truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime = read_json(Path(args.runtime_summary).expanduser().resolve())
    capital = read_json(Path(args.capital_roi_scorecard).expanduser().resolve())
    public_review = read_json(Path(args.public_benchmark_scorecard).expanduser().resolve())
    capacity_plan = read_json(Path(args.capacity_plan).expanduser().resolve())
    stage = find_stage(capacity_plan, args.stage)
    gates = stage.get("pass_gates", {})

    runtime_metrics = runtime.get("metrics", {})
    safety = runtime.get("safety", {})
    cap_agg = capital.get("aggregate", {})
    totals = cap_agg.get("totals", {})
    round_roi = cap_agg.get("round_roi", {})
    targets = public_review.get("benchmark_targets", {})
    review_targets = targets.get("xuan_capacity_ladder_review_targets", {})

    pair_qty = as_float(totals.get("pair_qty"), as_float(runtime_metrics.get("pair_qty")))
    pair_cost = as_float(round_roi.get("pair_cost"))
    if pair_cost <= 0 and pair_qty > 0:
        edge = as_float(round_roi.get("edge_on_redeem_notional"))
        pair_cost = pair_qty * (1.0 - edge)
    actual_pair_cost = pair_cost / pair_qty if pair_qty > 0 else None
    edge_on_redeem = as_float(round_roi.get("edge_on_redeem_notional"))
    roi_total = as_float(round_roi.get("roi_on_total_cash_spend"))
    pair_pnl = as_float(totals.get("pair_pnl"), as_float(runtime_metrics.get("pair_pnl")))
    filled_qty = as_float(totals.get("filled_qty"), as_float(runtime_metrics.get("filled_qty")))
    filled_cost = as_float(totals.get("filled_cost"), as_float(runtime_metrics.get("filled_cost")))
    residual_qty = as_float(totals.get("residual_qty"), as_float(runtime_metrics.get("residual_qty")))
    residual_cost = as_float(totals.get("residual_cost"), as_float(runtime_metrics.get("residual_cost")))
    residual_qty_share = residual_qty / filled_qty if filled_qty > 0 else as_float(runtime_metrics.get("residual_qty_share"))
    residual_cost_share = residual_cost / filled_cost if filled_cost > 0 else as_float(runtime_metrics.get("residual_cost_share"))
    residual_cost_to_pair_qty = as_float(round_roi.get("residual_cost_to_pair_qty"))
    strict_rescue_source_blocks = as_float(runtime_metrics.get("strict_rescue_source_blocks"))
    rescue_l1_age_ms_max = as_float(runtime_metrics.get("rescue_l1_age_ms_max"))

    hard_gates = [
        bool_gate("pm_dry_run", as_bool(safety.get("pm_dry_run")), True),
        bool_gate("orders_sent", as_bool(safety.get("orders_sent")), False),
        bool_gate("deploy_or_restart", as_bool(safety.get("deploy_or_restart")), False),
        bool_gate("shared_service_mutation", as_bool(safety.get("shared_service_mutation")), False),
        bool_gate("remote_repo_mutation", as_bool(safety.get("remote_repo_mutation")), False),
        cmp_gate("min_edge_on_redeem_notional_after_fee", edge_on_redeem, ">=", as_float(gates.get("min_edge_on_redeem_notional_after_fee"))),
        cmp_gate("min_roi_on_total_cash_spend_after_fee", roi_total, ">=", as_float(gates.get("min_roi_on_total_cash_spend_after_fee"))),
        cmp_gate("min_realized_pair_pnl_after_fee", pair_pnl, ">=", as_float(gates.get("min_realized_pair_pnl"))),
        cmp_gate("min_pair_qty", pair_qty, ">=", as_float(gates.get("min_pair_qty"))),
        cmp_gate("max_residual_cost_share", residual_cost_share, "<=", as_float(gates.get("max_residual_cost_share"))),
        cmp_gate("max_residual_qty_share", residual_qty_share, "<=", as_float(gates.get("max_residual_qty_share"))),
        cmp_gate("max_residual_cost_to_pair_qty", residual_cost_to_pair_qty, "<=", as_float(gates.get("max_residual_cost_to_pair_qty"))),
        cmp_gate("max_rescue_l1_age_ms", rescue_l1_age_ms_max, "<=", as_float(gates.get("max_rescue_l1_age_ms"))),
        cmp_gate("strict_rescue_source_blocks", strict_rescue_source_blocks, "==", as_float(gates.get("strict_rescue_source_blocks"))),
        cmp_gate(
            "public_review_actual_pair_cost_hard",
            actual_pair_cost if actual_pair_cost is not None else 99.0,
            "<=",
            as_float(review_targets.get("review_actual_pair_cost_lte")),
        ),
        cmp_gate(
            "public_review_residual_rate_hard",
            residual_qty_share,
            "<=",
            as_float(review_targets.get("hard_residual_rate_lte")),
        ),
    ]
    soft_review_gates = [
        cmp_gate(
            "public_target_actual_pair_cost",
            actual_pair_cost if actual_pair_cost is not None else 99.0,
            "<=",
            as_float(review_targets.get("target_actual_pair_cost_lte")),
        ),
        cmp_gate(
            "public_target_residual_rate",
            residual_qty_share,
            "<=",
            as_float(review_targets.get("target_residual_rate_lte")),
        ),
    ]
    hard_blockers = [gate["name"] for gate in hard_gates if not gate["passed"]]
    soft_warnings = [gate["name"] for gate in soft_review_gates if not gate["passed"]]
    status = (
        "KEEP_CAPACITY_STAGE_PUBLIC_BENCHMARK_GATE_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_CAPACITY_STAGE_PUBLIC_BENCHMARK_GATE_BLOCKED"
    )

    b55_pair_cost = as_float(targets.get("b55_actual_pair_cost"))
    b55_residual_rate = as_float(targets.get("b55_residual_rate"))
    ce25_pair_cost = as_float(targets.get("ce25_actual_pair_cost"))
    ce25_residual_rate = as_float(targets.get("ce25_residual_rate"))
    card = {
        "artifact": "xuan_capacity_stage_public_benchmark_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_capacity_stage_public_benchmark_gate.py",
        "status": status,
        "stage": args.stage,
        "inputs": {
            "runtime_summary": str(Path(args.runtime_summary).expanduser().resolve()),
            "capital_roi_scorecard": str(Path(args.capital_roi_scorecard).expanduser().resolve()),
            "public_benchmark_scorecard": str(Path(args.public_benchmark_scorecard).expanduser().resolve()),
            "capacity_plan": str(Path(args.capacity_plan).expanduser().resolve()),
        },
        "metrics": {
            "pair_pnl_after_fee": round_opt(pair_pnl),
            "pair_qty_redeem_notional": round_opt(pair_qty),
            "actual_pair_cost_after_fee": round_opt(actual_pair_cost),
            "edge_on_redeem_notional_after_fee": round_opt(edge_on_redeem),
            "edge_on_redeem_notional_after_fee_pct": pct(edge_on_redeem),
            "roi_on_total_cash_spend_after_fee": round_opt(roi_total),
            "roi_on_total_cash_spend_after_fee_pct": pct(roi_total),
            "residual_qty_share": round_opt(residual_qty_share),
            "residual_qty_share_pct": pct(residual_qty_share),
            "residual_cost_share": round_opt(residual_cost_share),
            "residual_cost_share_pct": pct(residual_cost_share),
            "residual_cost_to_pair_qty": round_opt(residual_cost_to_pair_qty),
            "residual_cost_to_pair_qty_pct": pct(residual_cost_to_pair_qty),
            "strict_rescue_closes": round_opt(as_float(totals.get("strict_rescue_closes"), as_float(runtime_metrics.get("strict_rescue_closes")))),
            "strict_rescue_source_blocks": round_opt(strict_rescue_source_blocks),
            "rescue_l1_age_ms_max": round_opt(rescue_l1_age_ms_max),
        },
        "public_benchmark": {
            "benchmark_scope": targets.get("b55_benchmark_scope"),
            "b55_cash_pnl_contains_old_inventory_redeem": targets.get("b55_cash_pnl_contains_old_inventory_redeem"),
            "b55_actual_pair_cost": round_opt(b55_pair_cost),
            "b55_pair_edge_pct": pct(as_float(targets.get("b55_pair_edge"))),
            "b55_residual_rate": round_opt(b55_residual_rate),
            "b55_residual_rate_pct": pct(b55_residual_rate),
            "b55_new_position_mtm_ex_rebate": targets.get("b55_new_position_mtm_ex_rebate"),
            "b55_new_position_mtm_including_rebate": targets.get("b55_new_position_mtm_including_rebate"),
            "ce25_actual_pair_cost": round_opt(ce25_pair_cost),
            "ce25_residual_rate": round_opt(ce25_residual_rate),
            "ce25_residual_rate_pct": pct(ce25_residual_rate),
            "split_count_all_accounts": targets.get("split_count_all_accounts"),
            "pair_cost_delta_vs_b55": round_opt((actual_pair_cost - b55_pair_cost) if actual_pair_cost is not None else None),
            "pair_cost_delta_vs_ce25": round_opt((actual_pair_cost - ce25_pair_cost) if actual_pair_cost is not None else None),
            "residual_qty_share_delta_vs_b55": round_opt(residual_qty_share - b55_residual_rate),
            "residual_qty_share_delta_vs_ce25": round_opt(residual_qty_share - ce25_residual_rate),
        },
        "capacity_stage": {
            "round_redeem_notional": stage.get("round_redeem_notional"),
            "duration_s": stage.get("duration_s"),
            "round_offsets": stage.get("round_offsets"),
            "profile_overrides": stage.get("profile_overrides"),
            "pass_gates": gates,
        },
        "hard_gates": hard_gates,
        "soft_review_gates": soft_review_gates,
        "decision": {
            "capacity_stage_gate_pass": not hard_blockers,
            "hard_blockers": hard_blockers,
            "soft_warnings": soft_warnings,
            "research_only": True,
            "remote_runner_allowed": False,
            "requires_explicit_bounded_no_order_authorization": True,
            "deployable": False,
            "live_orders_allowed": False,
            "shared_service_mutation_allowed": False,
            "collector_rebuild_allowed": False,
        },
    }
    return card


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-summary", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--public-benchmark-scorecard", required=True)
    parser.add_argument("--capacity-plan", required=True)
    parser.add_argument("--stage", default="cap_25")
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
    return 0 if card["decision"]["capacity_stage_gate_pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
