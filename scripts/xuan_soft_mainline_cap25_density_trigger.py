#!/usr/bin/env python3
"""Build a local density trigger for cap25 soft-mainline reproduction.

The target_qty=25 cap25 lane is a capacity-scaled reproduction candidate, so it
should not be blocked solely by the latest target_qty=5 density history. This
scorer combines the cap25 reproduction review, manifest verifier, and backfill
candidate evidence into a conservative local trigger. It never launches remote
jobs or promotes shadow.
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
    p = Path(path).expanduser().resolve()
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    return raw if isinstance(raw, dict) else {}


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    out = fnum(value)
    return int(out) if out is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def candidate_from_review(review: dict[str, Any]) -> dict[str, Any]:
    candidate = body(review, "candidate")
    metrics = body(candidate, "metrics")
    profile = body(candidate, "profile")
    return {
        "accepted_actions": inum(metrics.get("accepted_actions")),
        "queue_supported_fills": inum(metrics.get("queue_supported_fills")),
        "strict_rescue_closes": inum(metrics.get("strict_rescue_closes")),
        "strict_rescue_qty": fnum(metrics.get("strict_rescue_qty")),
        "pair_pnl": fnum(metrics.get("pair_pnl"), 0.0) or 0.0,
        "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share")),
        "residual_cost_share": fnum(metrics.get("residual_cost_share")),
        "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
        "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks"), 0.0) or 0.0,
        "target_qty": fnum(profile.get("target_qty")),
        "imbalance_qty_cap": fnum(profile.get("imbalance_qty_cap")),
        "max_open_cost": fnum(profile.get("max_open_cost")),
        "strict_rescue_surplus_net_cap": fnum(profile.get("strict_rescue_surplus_net_cap")),
        "risk_seed_cancel_on_closeability_net_cap": fnum(profile.get("risk_seed_cancel_on_closeability_net_cap")),
    }


def evaluate_candidate(obs: dict[str, Any], args: argparse.Namespace) -> list[str]:
    blockers: list[str] = []
    if obs["accepted_actions"] < args.min_accepted_actions:
        blockers.append("cap25_accepted_actions_below_min")
    if obs["queue_supported_fills"] < args.min_fills:
        blockers.append("cap25_fills_below_min")
    if obs["strict_rescue_closes"] < args.min_strict_rescue_closes:
        blockers.append("cap25_strict_rescue_closes_below_min")
    if obs["pair_pnl"] < args.min_pair_pnl:
        blockers.append("cap25_pair_pnl_below_min")
    if obs["residual_qty_share"] is None or obs["residual_qty_share"] > args.max_residual_qty_share:
        blockers.append("cap25_residual_qty_share_above_max")
    if obs["residual_cost_share"] is None or obs["residual_cost_share"] > args.max_residual_cost_share:
        blockers.append("cap25_residual_cost_share_above_max")
    if obs["rescue_l1_age_ms_max"] is not None and obs["rescue_l1_age_ms_max"] > args.max_rescue_l1_age_ms:
        blockers.append("cap25_rescue_l1_age_above_max")
    if obs["accepted_l1_age_ms_max"] is not None and obs["accepted_l1_age_ms_max"] > args.max_accepted_l1_age_ms:
        blockers.append("cap25_accepted_l1_age_above_max")
    if obs["strict_rescue_source_blocks"] > args.max_source_blocks:
        blockers.append("cap25_source_blocks_present")
    if obs["risk_seed_cancel_on_closeability_net_cap"] is not None:
        blockers.append("cap25_closeability_cancel_guard_enabled")
    if obs["strict_rescue_surplus_net_cap"] is None:
        blockers.append("cap25_missing_explicit_surplus_cap")
    elif obs["strict_rescue_surplus_net_cap"] > args.max_strict_rescue_surplus_net_cap:
        blockers.append("cap25_surplus_cap_relaxed")
    return blockers


def find_backfill_candidate(existing_scan: dict[str, Any], label_substr: str) -> dict[str, Any]:
    candidates = existing_scan.get("top_candidates")
    if not isinstance(candidates, list):
        return {}
    for item in candidates:
        if isinstance(item, dict) and label_substr in str(item.get("label") or ""):
            return item
    return {}


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    obs = card["cap25_candidate_observed"]
    lines = [
        "# Cap25 Density Trigger",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Cap25 Backfill Signal",
        "",
        f"- accepted/fills/rescues: `{obs['accepted_actions']}` / `{obs['queue_supported_fills']}` / `{obs['strict_rescue_closes']}`",
        f"- pair PnL: `{obs['pair_pnl']}`",
        f"- residual qty/cost share: `{obs['residual_qty_share']}` / `{obs['residual_cost_share']}`",
        f"- target qty / imbalance cap: `{obs['target_qty']}` / `{obs['imbalance_qty_cap']}`",
        "",
        "## Guardrails",
        "",
        "- This trigger is local planning evidence only.",
        "- It distinguishes cap25 reproduction from target_qty=5 same-profile repeats.",
        "- Remote execution remains disabled here and requires a future heartbeat, manifest verifier, and no runner conflict.",
        "- Do not copy the failed cap25 residual-repair variant or widen surplus cap above 1.02.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    review = load_json(args.cap25_reproduction_review)
    verifier = load_json(args.cap25_manifest_verifier)
    existing_scan = load_json(args.existing_window_candidates)
    run_trigger = load_json(args.latest_run_trigger_policy)

    decision = body(review, "decision")
    observed = candidate_from_review(review)
    candidate_blockers = evaluate_candidate(observed, args)
    review_blockers = [str(item) for item in listify(decision.get("hard_blockers")) if item]
    verifier_blockers = [str(item) for item in listify(verifier.get("hard_blockers")) if item]
    backfill_candidate = find_backfill_candidate(existing_scan, args.backfill_label_substring)

    hard_blockers: list[str] = []
    if not is_keep(review):
        hard_blockers.append("cap25_reproduction_review_not_keep")
    hard_blockers.extend(review_blockers)
    if not is_keep(verifier):
        hard_blockers.append("cap25_manifest_verifier_not_keep")
    hard_blockers.extend(verifier_blockers)
    if not backfill_candidate:
        hard_blockers.append("cap25_backfill_candidate_missing_from_existing_window_scan")
    hard_blockers.extend(candidate_blockers)
    if decision.get("do_not_copy_failed_repair_variant") is not True:
        hard_blockers.append("failed_repair_variant_not_explicitly_rejected")
    if decision.get("requires_fresh_reproduction_runtime") is not True:
        hard_blockers.append("fresh_reproduction_requirement_missing")
    hard_blockers = sorted(set(hard_blockers))

    latest_target5_blocked = status(run_trigger).startswith("BLOCKED")
    if hard_blockers:
        card_status = "BLOCKED_SOFT_MAINLINE_CAP25_DENSITY_TRIGGER_LOCAL_ONLY"
        next_action = "stay_local_until_cap25_review_and_manifest_are_clean"
        density_ready = False
    else:
        card_status = "KEEP_SOFT_MAINLINE_CAP25_DENSITY_TRIGGER_READY_LOCAL_ONLY"
        next_action = "prepare_one_bounded_cap25_reproduction_on_future_heartbeat_after_runner_conflict_check"
        density_ready = True

    card = {
        "artifact": "xuan_soft_mainline_cap25_density_trigger",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_cap25_density_trigger.py",
        "status": card_status,
        "inputs": {
            "cap25_reproduction_review": str(Path(args.cap25_reproduction_review).expanduser()),
            "cap25_manifest_verifier": str(Path(args.cap25_manifest_verifier).expanduser()),
            "existing_window_candidates": str(Path(args.existing_window_candidates).expanduser()),
            "latest_run_trigger_policy": str(Path(args.latest_run_trigger_policy).expanduser())
            if args.latest_run_trigger_policy
            else None,
        },
        "decision": {
            "deployable": False,
            "shadow_ready": False,
            "remote_runner_allowed": False,
            "research_only": True,
            "cap25_reproduction_density_ready": density_ready,
            "hard_blockers": hard_blockers,
            "next_action": next_action,
            "requires_future_heartbeat": True,
            "requires_no_runner_conflict_check": True,
            "requires_manifest_verifier_pass": True,
            "requires_fresh_runtime": True,
        },
        "cap25_candidate_observed": observed,
        "cap25_backfill_candidate": backfill_candidate,
        "target5_latest_density_context": {
            "status": status(run_trigger) or None,
            "blocked": latest_target5_blocked,
            "interpretation": (
                "target_qty_5_weak_density_is_a_holdout_context_not_an_automatic_cap25_block"
                if latest_target5_blocked and density_ready
                else "target_qty_5_context_recorded"
            ),
        },
        "thresholds": {
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_strict_rescue_closes": args.min_strict_rescue_closes,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_source_blocks": args.max_source_blocks,
            "max_strict_rescue_surplus_net_cap": args.max_strict_rescue_surplus_net_cap,
        },
        "guardrails": [
            "Local trigger only; no remote launch and no shadow/deploy approval.",
            "Cap25 reproduction can be considered separately from target_qty=5 weak-density history because the profile is capacity scaled.",
            "The failed residual-repair variant must not be copied.",
            "Any future run remains one bounded 1800s PM_DRY_RUN with source gates and fixed launch template.",
        ],
    }
    return rounded(card)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cap25-reproduction-review",
        default=".tmp_xuan/scorecards/xuan_soft_mainline_cap25_reproduction_review_20260526T0006Z.json",
    )
    parser.add_argument(
        "--cap25-manifest-verifier",
        default=".tmp_xuan/scorecards/no_order_soft_mainline_cap25_reproduction_manifest_verifier_smoke_20260526T0006Z.json",
    )
    parser.add_argument(
        "--existing-window-candidates",
        default=".tmp_xuan/scorecards/xuan_soft_mainline_existing_window_candidates_20260525T1530Z.json",
    )
    parser.add_argument(
        "--latest-run-trigger-policy",
        default=".tmp_xuan/scorecards/xuan_soft_mainline_run_trigger_policy_20260525T1204Z.json",
    )
    parser.add_argument("--backfill-label-substring", default="capacity-ladder-cap25-20260524T0723Z")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--min-accepted-actions", type=int, default=25)
    parser.add_argument("--min-fills", type=int, default=20)
    parser.add_argument("--min-strict-rescue-closes", type=int, default=7)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-source-blocks", type=float, default=0.0)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
