#!/usr/bin/env python3
"""Map the public 04b6 benchmark into xuan cap25 residual-tail work.

The output is a local-only interpretation artifact. It uses 04b6 as a public
execution-shape benchmark, not as private truth and not as live authorization.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_benchmark_probe_20260526T1236Z.json")
DEFAULT_TAIL_GAP = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_tail_gap_20260526T0925Z.json")
DEFAULT_TAIL_RUNTIME = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-tail-mitigated-20260526T0432Z_runtime_summary.json"
)
DEFAULT_TAIL_CAPITAL = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-tail-mitigated-20260526T0432Z_capital_reuse_roi.json"
)
DEFAULT_LOW_LOT_RUNTIME = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-low-lot-rebalance-20260526T0700Z_runtime_summary.json"
)
DEFAULT_LOW_LOT_GATE = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-low-lot-rebalance-20260526T0700Z_capacity_stage_public_benchmark_gate.json"
)
DEFAULT_REBALANCE_PLAN = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_tail_density_residual_rebalance_plan_20260526T1704Z.json"
)
DEFAULT_MIDPOINT_RESULT = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_tail_density_midpoint_result_20260526T0945Z.json"
)
DEFAULT_SCORECARD_ROOT = Path(".tmp_xuan/scorecards")
DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def maybe_load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json(path)


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def fnum(value: Any, default: float | None = None) -> float | None:
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


def metrics_from_runtime(card: dict[str, Any]) -> dict[str, Any]:
    metrics = body(card, "metrics")
    return {
        "accepted_actions": fnum(metrics.get("accepted_actions")),
        "queue_supported_fills": fnum(metrics.get("queue_supported_fills")),
        "strict_rescue_closes": fnum(metrics.get("strict_rescue_closes")),
        "pair_pnl": fnum(metrics.get("pair_pnl")),
        "residual_qty": fnum(metrics.get("residual_qty")),
        "residual_cost": fnum(metrics.get("residual_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share")),
        "residual_cost_share": fnum(metrics.get("residual_cost_share")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks")),
    }


def add_capital_fields(metrics: dict[str, Any], capital: dict[str, Any]) -> dict[str, Any]:
    out = dict(metrics)
    round_roi = body(body(capital, "aggregate"), "round_roi")
    out["residual_cost_to_pair_qty"] = fnum(round_roi.get("residual_cost_to_pair_qty"))
    out["worst_case_pair_pnl_if_residual_zero"] = fnum(
        round_roi.get("worst_case_pair_pnl_if_residual_zero")
    )
    return out


def gate_eval(metrics: dict[str, Any], *, public_target: float) -> dict[str, bool]:
    return {
        "density_min_rescues": (metrics.get("strict_rescue_closes") or 0.0) >= 7.0,
        "residual_qty_public_hard": (metrics.get("residual_qty_share") or 9.0) <= 0.20,
        "residual_qty_public_target": (metrics.get("residual_qty_share") or 9.0)
        <= public_target,
        "residual_cost_share_hard": (metrics.get("residual_cost_share") or 9.0) <= 0.15,
        "residual_cost_to_pair_qty_hard": (
            metrics.get("residual_cost_to_pair_qty") is not None
            and (metrics.get("residual_cost_to_pair_qty") or 9.0) <= 0.05
        ),
        "positive_pair_pnl": (metrics.get("pair_pnl") or -1.0) > 0.0,
        "source_clean": (metrics.get("strict_rescue_source_blocks") or 0.0) == 0.0,
    }


def candidate_row(name: str, metrics: dict[str, Any], public_target: float) -> dict[str, Any]:
    gates = gate_eval(metrics, public_target=public_target)
    return {
        "name": name,
        "metrics": metrics,
        "gates": gates,
        "all_required_gates_pass": all(gates.values()),
        "failed_gates": [key for key, passed in gates.items() if not passed],
    }


def build_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    b = card["benchmark_transfer"]
    lines = [
        "# Xuan 04b6 Residual Transfer Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- transfer_plan_ready: `{d['transfer_plan_ready']}`",
        f"- next_action: `{d['next_action']}`",
        f"- do_not_expand_capacity: `{d['do_not_expand_capacity']}`",
        f"- future_remote_allowed_by_this_plan: `{d['future_remote_allowed_by_this_plan']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## 04b6 Transfer",
        "",
        f"- 04b6 residual qty share: `{b['benchmark_residual_qty_share']}`",
        f"- xuan public residual target used here: `{b['xuan_public_residual_target']}`",
        f"- 04b6-like aspirational residual target: `{b['benchmark_like_residual_aspiration']}`",
        f"- benchmark is private truth: `{b['benchmark_private_truth_ready']}`",
        f"- pair cost comparable: `{b['pair_cost_comparable_to_xuan']}`",
        "",
        "## Candidate Read",
        "",
        "| candidate | rescues | residual qty share | residual cost share | residual cost / pair qty | failed gates |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in card["candidate_matrix"]:
        m = row["metrics"]
        lines.append(
            "| "
            f"{row['name']} | "
            f"`{m.get('strict_rescue_closes')}` | "
            f"`{m.get('residual_qty_share')}` | "
            f"`{m.get('residual_cost_share')}` | "
            f"`{m.get('residual_cost_to_pair_qty')}` | "
            f"`{', '.join(row['failed_gates']) or 'none'}` |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
        ]
    )
    lines.extend(f"- `{item}`" for item in card["interpretation"])
    lines.extend(
        [
            "",
            "## Required Before Future Remote",
            "",
        ]
    )
    lines.extend(f"- `{item}`" for item in d["required_before_future_remote"])
    lines.append("")
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    public_04b6 = load_json(args.public_04b6_scorecard)
    residual_tail = load_json(args.residual_tail_gap_scorecard)
    tail_runtime = load_json(args.tail_runtime_scorecard)
    tail_capital = load_json(args.tail_capital_scorecard)
    low_lot_runtime = load_json(args.low_lot_runtime_scorecard)
    low_lot_gate = load_json(args.low_lot_gate_scorecard)
    rebalance_plan = load_json(args.rebalance_plan_scorecard)
    midpoint_result = maybe_load_json(args.midpoint_result_scorecard)

    benchmark_residual = fnum(
        body(public_04b6, "pairish_buy_only").get("residual_qty_share_all_buys")
    )
    benchmark_like_aspiration = (
        min(args.max_public_target_residual_qty_share, benchmark_residual + args.benchmark_residual_buffer)
        if benchmark_residual is not None
        else args.max_public_target_residual_qty_share
    )
    xuan_public_target = args.max_public_target_residual_qty_share

    current_metrics = dict(body(residual_tail, "current_stage_metrics"))
    tail_metrics = add_capital_fields(metrics_from_runtime(tail_runtime), tail_capital)
    low_metrics = metrics_from_runtime(low_lot_runtime)
    low_gate_metrics = body(low_lot_gate, "metrics")
    low_metrics["residual_cost_to_pair_qty"] = fnum(low_gate_metrics.get("residual_cost_to_pair_qty"))
    low_metrics["worst_case_pair_pnl_if_residual_zero"] = None

    candidate_matrix = [
        candidate_row("2041Z_cap25_current", current_metrics, xuan_public_target),
        candidate_row("0432Z_tail_residual_bound", tail_metrics, xuan_public_target),
        candidate_row("0700Z_low_lot_density_recovered", low_metrics, xuan_public_target),
    ]

    midpoint_valid = False
    midpoint_decision = body(midpoint_result, "decision")
    if midpoint_result:
        midpoint_valid = (
            is_keep(midpoint_result)
            and midpoint_decision.get("remote_runner_completed") is True
            and not midpoint_decision.get("hard_blockers")
        )

    required = [
        "keep cap25; do not expand to cap75 while residual_cost_to_pair_qty is blocked",
        "use midpoint_with_pnl_floor profile only after manifest verifier and runner conflict check",
                "future sample must clear residual_cost_to_pair_qty <= 0.05 and residual qty share <= xuan public target",
        "strict rescues must remain >=7 with source blocks zero",
        "do not treat 04b6 public proxy data as private_truth_ready",
        "do not relax strict_rescue_surplus_net_cap above 1.02 or re-enable closeability cancel",
    ]
    hard_blockers: list[str] = []
    if not is_keep(public_04b6):
        hard_blockers.append("public_04b6_probe_not_keep")
    if not is_keep(residual_tail):
        hard_blockers.append("residual_tail_gap_not_keep")
    if not is_keep(rebalance_plan):
        hard_blockers.append("tail_density_residual_rebalance_plan_not_keep")
    if midpoint_result and midpoint_valid:
        hard_blockers.append("midpoint_result_already_valid_recheck_needed")

    interpretation = [
        "04b6 supports the residual target shape: high BTC activity can coexist with mid-teen-or-lower public residual.",
        "04b6 does not prove maker/taker or owner fee truth for xuan; it is a public execution-shape benchmark.",
        "xuan pair edge is not the primary blocker; residual_cost_to_pair_qty is the binding cap25 gate.",
        "0432Z is closest to the 04b6 residual shape but misses strict rescue density by one close.",
        "0700Z recovers density but violates residual gates, so repeating it is not useful.",
        "0945Z midpoint exit-137 remains incomplete and cannot be promotion evidence.",
    ]

    transfer_ready = not hard_blockers
    status_value = (
        "KEEP_PUBLIC_04B6_RESIDUAL_TRANSFER_PLAN_READY_LOCAL_ONLY"
        if transfer_ready
        else "UNKNOWN_PUBLIC_04B6_RESIDUAL_TRANSFER_PLAN_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_public_04b6_residual_transfer_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_public_04b6_residual_transfer_plan.py",
            "status": status_value,
            "inputs": {
                "public_04b6_scorecard": str(args.public_04b6_scorecard),
                "residual_tail_gap_scorecard": str(args.residual_tail_gap_scorecard),
                "tail_runtime_scorecard": str(args.tail_runtime_scorecard),
                "tail_capital_scorecard": str(args.tail_capital_scorecard),
                "low_lot_runtime_scorecard": str(args.low_lot_runtime_scorecard),
                "low_lot_gate_scorecard": str(args.low_lot_gate_scorecard),
                "rebalance_plan_scorecard": str(args.rebalance_plan_scorecard),
                "midpoint_result_scorecard": str(args.midpoint_result_scorecard),
            },
            "source_statuses": {
                "public_04b6": status(public_04b6),
                "residual_tail_gap": status(residual_tail),
                "tail_runtime": status(tail_runtime),
                "tail_capital": status(tail_capital),
                "low_lot_runtime": status(low_lot_runtime),
                "low_lot_gate": status(low_lot_gate),
                "rebalance_plan": status(rebalance_plan),
                "midpoint_result": status(midpoint_result) if midpoint_result else "MISSING",
            },
            "benchmark_transfer": {
                "benchmark_residual_qty_share": benchmark_residual,
                "benchmark_private_truth_ready": bool(public_04b6.get("private_truth_ready")),
                "pair_cost_comparable_to_xuan": False,
                "xuan_public_residual_target": xuan_public_target,
                "max_public_target_residual_qty_share": args.max_public_target_residual_qty_share,
                "benchmark_residual_buffer": args.benchmark_residual_buffer,
                "benchmark_like_residual_aspiration": benchmark_like_aspiration,
            },
            "candidate_matrix": candidate_matrix,
            "interpretation": interpretation,
            "decision": {
                "transfer_plan_ready": transfer_ready,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_allowed_by_this_plan": False,
                "cap75_remote_rationale_ready": False,
                "do_not_expand_capacity": True,
                "midpoint_result_valid_for_promotion": midpoint_valid,
                "hard_blockers": hard_blockers,
                "required_before_future_remote": required,
                "next_action": "stage_or_reuse_midpoint_profile_manifest_only_after_conflict_check_no_cap75",
            },
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-04b6-scorecard", type=Path, default=DEFAULT_04B6)
    parser.add_argument("--residual-tail-gap-scorecard", type=Path, default=DEFAULT_TAIL_GAP)
    parser.add_argument("--tail-runtime-scorecard", type=Path, default=DEFAULT_TAIL_RUNTIME)
    parser.add_argument("--tail-capital-scorecard", type=Path, default=DEFAULT_TAIL_CAPITAL)
    parser.add_argument("--low-lot-runtime-scorecard", type=Path, default=DEFAULT_LOW_LOT_RUNTIME)
    parser.add_argument("--low-lot-gate-scorecard", type=Path, default=DEFAULT_LOW_LOT_GATE)
    parser.add_argument("--rebalance-plan-scorecard", type=Path, default=DEFAULT_REBALANCE_PLAN)
    parser.add_argument("--midpoint-result-scorecard", type=Path, default=DEFAULT_MIDPOINT_RESULT)
    parser.add_argument("--max-public-target-residual-qty-share", type=float, default=0.15)
    parser.add_argument("--benchmark-residual-buffer", type=float, default=0.04)
    parser.add_argument("--scorecard-root", type=Path, default=DEFAULT_SCORECARD_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--stamp", default=time.strftime("%Y%m%dT%H%MZ", time.gmtime()))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scorecard = build(args)
    args.scorecard_root.mkdir(parents=True, exist_ok=True)
    artifact_dir = args.artifact_root / f"xuan_public_04b6_residual_transfer_plan_{args.stamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scorecard_path = args.scorecard_root / f"xuan_public_04b6_residual_transfer_plan_{args.stamp}.json"
    markdown_path = artifact_dir / "PUBLIC_04B6_RESIDUAL_TRANSFER_PLAN.md"
    scorecard["scorecard_path"] = str(scorecard_path)
    scorecard["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(build_markdown(scorecard), encoding="utf-8")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
