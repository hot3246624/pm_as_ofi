#!/usr/bin/env python3
"""Materialize the tail-density re-balance profile from the local plan.

This helper only emits a profile scorecard for manifest smoke tests. It does
not launch remote jobs, authorize deployment, send live orders, or mutate any
shared service.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


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


def ensure_defaults(profile: dict[str, Any]) -> dict[str, Any]:
    out = dict(profile)
    out.setdefault("duration_s", 1800)
    out.setdefault("round_offsets", "0,1,2,3,4,5,6,7,8,9,10,11,12")
    out.setdefault("target_qty", 25.0)
    out.setdefault("queue_share", 0.5)
    out.setdefault("fill_haircut", 0.25)
    out.setdefault("max_seed_qty", 75.0)
    out.setdefault("max_open_cost", 30.0)
    out.setdefault("seed_l1_cap", 1.02)
    out.setdefault("seed_px_lo", 0.05)
    out.setdefault("seed_px_hi", 0.9)
    out.setdefault("seed_offset_max_s", 120)
    out.setdefault("order_ttl_s", 120)
    out.setdefault("imbalance_cost_cap", 1000000000.0)
    out.setdefault("surplus_budget_mode", "block")
    out.setdefault("surplus_budget_bootstrap", 1.0)
    out.setdefault("surplus_budget_mult", 0.5)
    out.setdefault("taker_fee_rate", 0.07)
    out.setdefault("strict_rescue_salvage_net_cap", 0.95)
    out.setdefault("salvage_age_s", 30)
    out.setdefault("max_salvage_qty", 250.0)
    out.setdefault("strict_rescue_l1_age_max_ms", 50)
    out.setdefault("strict_rescue_close_size_haircut", 1.0)
    out.setdefault("source_quality_require_l1_source", True)
    out.setdefault("source_quality_require_l2_source", True)
    out.setdefault("source_quality_require_trade_source", True)
    out.setdefault("strict_rescue_require_book_source", True)
    out.setdefault("strict_rescue_require_l2_source", True)
    out.setdefault("strict_rescue_skip_low_cost_lots", True)
    out.setdefault("soft_cap", 0.98)
    out.setdefault("debt_floor", 0.95)
    out.setdefault("debt_budget", 1.0)
    out.setdefault("risk_seed_pending_opp_credit", 0.5)
    out.setdefault("activation_mode", "opp_seen")
    out.setdefault("activation_window_s", 15.0)
    out.setdefault("late_repair_after_s", 90.0)
    out.setdefault("pair_completion_net_cap", 1.05)
    out.setdefault("pair_completion_min_pair_pnl_after", 0.0)
    out.setdefault("strict_rescue_surplus_net_cap", 1.02)
    out.setdefault("strict_rescue_min_pair_pnl_after", 0.0)
    out.setdefault("allow_concurrent_shared_ingress_readers", True)
    out.setdefault("write_normalized_lifecycle", True)
    out.setdefault("write_rescue_block_diagnostics", True)
    return out


def build_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["profile"]
    lines = [
        "# Xuan Tail Density Rebalance Profile",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- profile_ready: `{d['profile_ready']}`",
        f"- manifest_smoke_required: `{d['manifest_smoke_required']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Key Parameters",
        "",
        f"- salvage_min_lot_cost: `{p['salvage_min_lot_cost']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- imbalance_qty_cap: `{p['imbalance_qty_cap']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        f"- risk_seed_cancel_on_closeability_net_cap: `{p.get('risk_seed_cancel_on_closeability_net_cap')}`",
        "",
        "## Guardrails",
        "",
        "- This profile scorecard is local-only and does not authorize remote execution.",
        "- Manifest builder and verifier must KEEP before any future bounded dry-run.",
        "- Do not expand capacity until cap25 residual and density gates both clear.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    plan = load_json(args.rebalance_plan_scorecard)
    base = load_json(args.base_profile_scorecard)
    plan_hypothesis = body(plan, "next_profile_hypothesis")
    overrides = body(plan_hypothesis, "profile_overrides")
    base_profile = body(base, "profile")
    profile = ensure_defaults({**base_profile, **overrides})

    hard_blockers: list[str] = []
    if not is_keep(plan):
        hard_blockers.append("rebalance_plan_not_keep")
    if not base_profile:
        hard_blockers.append("base_profile_missing")
    if fnum(profile.get("salvage_min_lot_cost"), 9.0) >= fnum(
        base_profile.get("salvage_min_lot_cost"), args.reference_salvage_min_lot_cost
    ):
        hard_blockers.append("salvage_min_lot_cost_not_lowered")
    if fnum(profile.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(profile.get("surplus_budget_max_abs_unpaired_cost"), 9.0) > args.max_surplus_budget_abs_unpaired_cost:
        hard_blockers.append("surplus_budget_above_tail_bound")
    if fnum(profile.get("imbalance_qty_cap"), 9.0) > args.max_imbalance_qty_cap:
        hard_blockers.append("imbalance_qty_cap_above_tail_bound")

    status_value = (
        "KEEP_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_PROFILE_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_PROFILE_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_shadow_review_tail_density_rebalance_profile",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_tail_density_rebalance_profile.py",
            "status": status_value,
            "candidate": plan_hypothesis.get("candidate") or "cap25_low_lot_rescue_density_rebalance",
            "inputs": {
                "rebalance_plan_scorecard": args.rebalance_plan_scorecard,
                "base_profile_scorecard": args.base_profile_scorecard,
            },
            "source_statuses": {
                "rebalance_plan": status(plan),
                "base_profile": status(base),
            },
            "decision": {
                "profile_ready": status_value.startswith("KEEP"),
                "future_remote_allowed_by_this_profile": False,
                "manifest_smoke_required": True,
                "manifest_verifier_required": True,
                "remote_runner_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "next_action": "run_manifest_builder_and_verifier_smoke_for_tail_density_rebalance_profile"
                if status_value.startswith("KEEP")
                else "fix_tail_density_rebalance_profile_locally",
            },
            "profile_changes": [
                {
                    "field": "salvage_min_lot_cost",
                    "current": fnum(base_profile.get("salvage_min_lot_cost"), args.reference_salvage_min_lot_cost),
                    "proposed": profile.get("salvage_min_lot_cost"),
                    "reason": "recover one or more strict rescues from low-cost-lot blocks without widening net-cap economics",
                },
                {
                    "field": "surplus_budget_max_abs_unpaired_cost",
                    "current": base_profile.get("surplus_budget_max_abs_unpaired_cost"),
                    "proposed": profile.get("surplus_budget_max_abs_unpaired_cost"),
                    "reason": "preserve tail residual bound from the prior mitigation run",
                },
                {
                    "field": "imbalance_qty_cap",
                    "current": base_profile.get("imbalance_qty_cap"),
                    "proposed": profile.get("imbalance_qty_cap"),
                    "reason": "preserve low residual quantity bound",
                },
                {
                    "field": "strict_rescue_surplus_net_cap",
                    "current": base_profile.get("strict_rescue_surplus_net_cap"),
                    "proposed": profile.get("strict_rescue_surplus_net_cap"),
                    "reason": "do not relax explicit-surplus economics",
                },
            ],
            "profile": profile,
            "safety": {
                "remote_runner_allowed": False,
                "deployable": False,
                "live_trading_allowed": False,
                "orders_sent": False,
                "shared_service_mutation": False,
                "remote_repo_mutation": False,
                "collector_rebuild": False,
                "cron_loop": False,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebalance-plan-scorecard", required=True)
    parser.add_argument("--base-profile-scorecard", required=True)
    parser.add_argument("--reference-salvage-min-lot-cost", type=float, default=0.25)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    parser.add_argument("--max-surplus-budget-abs-unpaired-cost", type=float, default=1.44)
    parser.add_argument("--max-imbalance-qty-cap", type=float, default=5.0)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        md = Path(args.markdown).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
