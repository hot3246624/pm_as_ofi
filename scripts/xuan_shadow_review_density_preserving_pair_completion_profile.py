#!/usr/bin/env python3
"""Materialize the density-preserving pair-completion profile.

This helper turns the local pair-completion plan into a profile scorecard that
can be checked by the manifest builder and scaffold smoke. It is local-only and
does not launch remote jobs, authorize deploys, restart services, or send
orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_PLAN = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_plan_20260526T1602Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_20260526T1610Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_density_preserving_pair_completion_profile_20260526T1610Z/PAIR_COMPLETION_PROFILE.md"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
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


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["profile"]
    lines = [
        "# Xuan Density-Preserving Pair-Completion Profile",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- profile_ready: `{d['profile_ready']}`",
        f"- manifest_smoke_required: `{d['manifest_smoke_required']}`",
        f"- manifest_verifier_required: `{d['manifest_verifier_required']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Key Parameters",
        "",
        f"- target_qty: `{p['target_qty']}`",
        f"- risk_seed_pair_completion_required_above_net_cap: `{p['risk_seed_pair_completion_required_above_net_cap']}`",
        f"- risk_seed_pair_completion_min_qty: `{p['risk_seed_pair_completion_min_qty']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- pair_completion_min_pair_pnl_after: `{p['pair_completion_min_pair_pnl_after']}`",
        f"- risk_seed_pending_opp_credit: `{p['risk_seed_pending_opp_credit']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        f"- rescue_block_diagnostics_max_per_slug: `{p['rescue_block_diagnostics_max_per_slug']}`",
        "",
        "## Guardrails",
        "",
        "- Local profile only; no remote execution is authorized.",
        "- Keep cap25; do not expand to cap75 until density and residual gates clear together.",
        "- Source requirements and surplus caps remain enabled.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    plan = load_json(args.plan_scorecard)
    hypothesis = body(plan, "next_profile_hypothesis")
    profile = dict(body(hypothesis, "primary_profile_overrides"))
    profile.setdefault("rescue_block_diagnostics_max_per_slug", args.rescue_block_diagnostics_max_per_slug)

    hard_blockers: list[str] = []
    if not is_keep(plan):
        hard_blockers.append("pair_completion_plan_not_keep")
    if body(plan, "decision").get("future_remote_allowed_by_this_plan") is not False:
        hard_blockers.append("plan_remote_allowed_not_false")
    if not profile:
        hard_blockers.append("primary_profile_missing")
    if fnum(profile.get("duration_s"), 0.0) > args.max_duration_s:
        hard_blockers.append("duration_exceeds_1800")
    if fnum(profile.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("target_qty_not_cap25")
    if profile.get("profile_family") != "residual_guard":
        hard_blockers.append("profile_family_not_residual_guard")
    if profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(profile.get("risk_seed_pending_opp_credit"), 9.0) != 0.0:
        hard_blockers.append("risk_seed_pending_opp_credit_not_zero")
    if fnum(profile.get("risk_seed_pair_completion_required_above_net_cap"), 0.0) < args.min_required_above_net_cap:
        hard_blockers.append("pair_completion_required_trigger_too_low")
    if fnum(profile.get("risk_seed_pair_completion_required_above_net_cap"), 9.0) > args.max_required_above_net_cap:
        hard_blockers.append("pair_completion_required_trigger_too_high")
    if fnum(profile.get("pair_completion_net_cap"), 9.0) > args.max_pair_completion_net_cap:
        hard_blockers.append("pair_completion_net_cap_too_loose")
    if fnum(profile.get("pair_completion_min_pair_pnl_after"), 0.0) < args.min_pair_completion_pnl_after:
        hard_blockers.append("pair_completion_pnl_floor_too_low")
    if fnum(profile.get("surplus_budget_max_abs_unpaired_cost"), 9.0) > args.max_surplus_budget_abs_unpaired_cost:
        hard_blockers.append("surplus_budget_relaxed")
    if fnum(profile.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if fnum(profile.get("rescue_block_diagnostics_max_per_slug"), 0.0) <= 0:
        hard_blockers.append("rescue_block_diagnostics_cap_missing")
    if fnum(profile.get("rescue_block_diagnostics_max_per_slug"), 10**12) > args.max_rescue_block_diagnostics_per_slug:
        hard_blockers.append("rescue_block_diagnostics_cap_too_high")
    for key in (
        "source_quality_require_l1_source",
        "source_quality_require_l2_source",
        "source_quality_require_trade_source",
        "strict_rescue_require_book_source",
        "strict_rescue_require_l2_source",
    ):
        if profile.get(key) is not True:
            hard_blockers.append(f"{key}_not_true")

    profile_ready = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_density_preserving_pair_completion_profile",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_density_preserving_pair_completion_profile.py",
        "status": (
            "KEEP_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PROFILE_READY_LOCAL_ONLY"
            if profile_ready
            else "UNKNOWN_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PROFILE_BLOCKED_LOCAL_ONLY"
        ),
        "candidate": hypothesis.get("candidate") or "cap25_density_preserving_pair_completion_guard",
        "inputs": {
            "plan_scorecard": str(args.plan_scorecard),
        },
        "source_statuses": {
            "plan": status(plan),
        },
        "decision": {
            "profile_ready": profile_ready,
            "future_remote_allowed_by_this_profile": False,
            "manifest_smoke_required": True,
            "manifest_verifier_required": True,
            "cap75_remote_rationale_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "next_action": "run_runner_scaffold_smoke_and_manifest_verifier"
            if profile_ready
            else "fix_density_preserving_pair_completion_profile_locally",
        },
        "profile_changes": [
            {
                "field": "risk_seed_pair_completion_required_above_net_cap",
                "proposed": profile.get("risk_seed_pair_completion_required_above_net_cap"),
                "reason": "reduce overfiltering by moving the risk-seed completion trigger above the prior p50 cost point",
            },
            {
                "field": "pair_completion_net_cap",
                "proposed": profile.get("pair_completion_net_cap"),
                "reason": "recover pair-completion density while keeping pair cost below 1.0",
            },
            {
                "field": "pair_completion_min_pair_pnl_after",
                "proposed": profile.get("pair_completion_min_pair_pnl_after"),
                "reason": "avoid treating near-flat pair completion as sufficient evidence",
            },
            {
                "field": "risk_seed_pending_opp_credit",
                "proposed": profile.get("risk_seed_pending_opp_credit"),
                "reason": "do not credit unfilled opposite intent as risk protection",
            },
            {
                "field": "rescue_block_diagnostics_max_per_slug",
                "proposed": profile.get("rescue_block_diagnostics_max_per_slug"),
                "reason": "preserve strict-rescue block diagnostics without repeating the 1654Z JSONL volume that likely caused exit 137",
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
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-scorecard", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--max-duration-s", type=float, default=1800.0)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--min-required-above-net-cap", type=float, default=0.985)
    parser.add_argument("--max-required-above-net-cap", type=float, default=1.01)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=0.99)
    parser.add_argument("--min-pair-completion-pnl-after", type=float, default=0.01)
    parser.add_argument("--max-surplus-budget-abs-unpaired-cost", type=float, default=1.44)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    parser.add_argument("--rescue-block-diagnostics-max-per-slug", type=int, default=5000)
    parser.add_argument("--max-rescue-block-diagnostics-per-slug", type=int, default=10000)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = args.scorecard_json.expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = args.markdown.expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
