#!/usr/bin/env python3
"""Plan xuan-frontier no-order capacity ladder after shadow-review approval."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as handle:
        return json.load(handle)


def as_float(value: Any, default: float = 0.0) -> float:
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


def pct(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value * 100.0


def build_stage(notional: float, *, duration_s: int, offsets: str, base_profile: dict[str, Any]) -> dict[str, Any]:
    # Target qty is redeem notional in binary markets. Conservative caps allow
    # the dry-run to express capacity while keeping residual budget bounded.
    target_qty = notional
    imbalance_qty_cap = max(1.25, notional * 0.25)
    max_open_cost = notional * 1.20
    max_seed_qty = notional * 3.0
    surplus_abs = notional * 0.40
    return {
        "stage": f"cap_{int(notional)}",
        "round_redeem_notional": notional,
        "duration_s": duration_s,
        "round_offsets": offsets,
        "profile_overrides": {
            "target_qty": target_qty,
            "max_seed_qty": max_seed_qty,
            "max_open_cost": max_open_cost,
            "imbalance_qty_cap": imbalance_qty_cap,
            "surplus_budget_max_abs_unpaired_cost": surplus_abs,
            "max_salvage_qty": max(notional * 3.0, as_float(base_profile.get("max_salvage_qty"), 250.0)),
            "soft_cap": base_profile.get("soft_cap", 0.98),
            "debt_floor": base_profile.get("debt_floor", 0.95),
            "debt_budget": base_profile.get("debt_budget", 1.0),
            "risk_seed_pending_opp_credit": base_profile.get("risk_seed_pending_opp_credit", 0.5),
            "pair_completion_net_cap": base_profile.get("pair_completion_net_cap", 1.05),
            "pair_completion_min_pair_pnl_after": base_profile.get("pair_completion_min_pair_pnl_after", 0.0),
            "strict_rescue_salvage_net_cap": base_profile.get("strict_rescue_salvage_net_cap", 0.95),
            "strict_rescue_surplus_net_cap": base_profile.get("strict_rescue_surplus_net_cap", 1.02),
            "strict_rescue_min_pair_pnl_after": base_profile.get("strict_rescue_min_pair_pnl_after", 0.0),
            "strict_rescue_skip_low_cost_lots": base_profile.get("strict_rescue_skip_low_cost_lots", True),
        },
        "pass_gates": {
            "min_edge_on_redeem_notional_after_fee": 0.02,
            "min_roi_on_total_cash_spend_after_fee": 0.015,
            "min_realized_pair_pnl": 0.0,
            "min_pair_qty": notional * 0.20,
            "max_residual_cost_share": 0.15,
            "max_residual_qty_share": 0.20,
            "max_residual_cost_to_pair_qty": 0.05,
            "max_rescue_l1_age_ms": 50,
            "strict_rescue_source_blocks": 0,
            "orders_sent": False,
            "dry_run": True,
        },
        "promotion_rule": "advance_to_next_stage_only_if_all_pass_gates_hold",
    }


def render_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Xuan-Frontier Capacity Ladder Plan",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- next_stage: `{card['decision']['next_stage']}`",
        f"- remote_runner_allowed: `{card['decision']['remote_runner_allowed']}`",
        f"- deployable: `{card['decision']['deployable']}`",
        f"- live_orders_allowed: `{card['decision']['live_orders_allowed']}`",
        "",
        "## Baseline",
        "",
    ]
    for key, value in card["baseline"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Ladder"])
    for stage in card["ladder"]:
        lines.extend(
            [
                "",
                f"### {stage['stage']}",
                "",
                f"- round_redeem_notional: `{stage['round_redeem_notional']}`",
                f"- duration_s: `{stage['duration_s']}`",
                f"- round_offsets: `{stage['round_offsets']}`",
                "- profile_overrides:",
            ]
        )
        for key, value in stage["profile_overrides"].items():
            lines.append(f"  - {key}: `{value}`")
        lines.append("- pass_gates:")
        for key, value in stage["pass_gates"].items():
            lines.append(f"  - {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- This is local planning only.",
            "- It does not authorize remote execution.",
            "- All future samples must remain PM_DRY_RUN=1/no-order unless separately approved.",
            "- Live orders, deploy, restart, shared-ingress mutation, collector rebuild, and raw/replay mutation remain out of scope.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    approval = read_json(Path(args.approval_gate).expanduser().resolve())
    capital = read_json(Path(args.capital_roi_scorecard).expanduser().resolve())
    profile_card = read_json(Path(args.profile_scorecard).expanduser().resolve())
    profile = profile_card.get("profile", {})
    cap_agg = capital.get("aggregate", {})
    totals = cap_agg.get("totals", {})
    round_roi = cap_agg.get("round_roi", {})
    approval_ready = approval.get("decision", {}).get("shadow_review_approval_ready") is True
    hard_blockers: list[str] = []
    if not approval_ready:
        hard_blockers.append("shadow_review_approval_gate_not_ready")
    if capital.get("status") != "KEEP_CAPITAL_REUSE_ROI_SCORER_PASS_RESEARCH_ONLY":
        hard_blockers.append("capital_reuse_roi_not_ready")

    notionals = [as_float(item) for item in args.round_notionals.split(",") if item.strip()]
    stages = [
        build_stage(notional, duration_s=args.duration_s, offsets=args.round_offsets, base_profile=profile)
        for notional in notionals
    ]
    status = (
        "KEEP_CAPACITY_LADDER_PLAN_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_CAPACITY_LADDER_PLAN_BLOCKED"
    )
    return {
        "artifact": "xuan_shadow_capacity_ladder_plan",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_capacity_ladder_plan.py",
        "status": status,
        "inputs": {
            "approval_gate": str(Path(args.approval_gate).expanduser().resolve()),
            "capital_roi_scorecard": str(Path(args.capital_roi_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "baseline": {
            "pair_pnl_after_fee": round_opt(as_float(totals.get("pair_pnl"))),
            "pair_qty_redeem_notional": round_opt(as_float(totals.get("pair_qty"))),
            "edge_on_redeem_notional": round_opt(as_float(round_roi.get("edge_on_redeem_notional"))),
            "roi_on_total_cash_spend": round_opt(as_float(round_roi.get("roi_on_total_cash_spend"))),
            "residual_cost_to_pair_qty": round_opt(as_float(round_roi.get("residual_cost_to_pair_qty"))),
            "current_profile_target_qty": profile.get("target_qty"),
            "current_profile_max_open_cost": profile.get("max_open_cost"),
        },
        "ladder": stages,
        "decision": {
            "capacity_ladder_ready": not hard_blockers,
            "next_stage": stages[0]["stage"] if stages and not hard_blockers else None,
            "remote_runner_allowed": False,
            "requires_explicit_bounded_no_order_authorization": True,
            "deployable": False,
            "live_orders_allowed": False,
            "hard_blockers": hard_blockers,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--approval-gate", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--profile-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown", required=True)
    parser.add_argument("--round-notionals", default="25,75,150,300")
    parser.add_argument("--duration-s", type=int, default=1800)
    parser.add_argument("--round-offsets", default="0,1,2,3,4,5,6,7,8,9,10,11,12")
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if not card["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
