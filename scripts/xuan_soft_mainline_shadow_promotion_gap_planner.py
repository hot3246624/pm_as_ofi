#!/usr/bin/env python3
"""Plan the remaining soft-mainline shadow promotion gap.

This local-only helper turns the shadow-promotion gate blockers into the next
piece of evidence the autoresearch loop needs. It does not launch remote jobs,
approve shadow, or change any strategy economics.
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


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    num = fnum(value)
    return int(num) if num is not None else default


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


def is_wait(card: dict[str, Any]) -> bool:
    return status(card).startswith("UNKNOWN") and "WAIT" in status(card)


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def source_caveat_absorbed(source_audit: dict[str, Any]) -> bool:
    return bool(body(source_audit, "decision").get("source_caveat_absorbed"))


def pick_status(
    *,
    shadow_gate: dict[str, Any],
    run_trigger: dict[str, Any],
    missing_qualified_windows: int,
) -> tuple[str, str]:
    if is_keep(shadow_gate):
        return (
            "KEEP_SOFT_MAINLINE_SHADOW_PROMOTION_GAP_ALREADY_CLEAR_LOCAL_ONLY",
            "prepare_shadow_review_packet_without_remote",
        )
    if is_wait(run_trigger):
        return (
            "UNKNOWN_SOFT_MAINLINE_SHADOW_PROMOTION_GAP_WAIT_FOR_PREFIX_MINUTES_LOCAL_ONLY",
            "wait_for_min_decision_prefix_before_remote_decision",
        )
    if not is_keep(run_trigger):
        return (
            "BLOCKED_SOFT_MAINLINE_SHADOW_PROMOTION_GAP_WAIT_FOR_FRESH_DENSITY_LOCAL_ONLY",
            "wait_for_fresh_prefix_or_preflight_density_signal_before_remote",
        )
    if missing_qualified_windows > 0:
        return (
            "UNKNOWN_SOFT_MAINLINE_SHADOW_PROMOTION_GAP_NEEDS_ONE_QUALIFIED_WINDOW_LOCAL_ONLY",
            "prepare_one_bounded_no_cancel_soft_mainline_remote_on_future_heartbeat_after_verifier",
        )
    return (
        "UNKNOWN_SOFT_MAINLINE_SHADOW_PROMOTION_GAP_RECHECK_RUNTIME_REPEAT_GATE_LOCAL_ONLY",
        "rerun_postrun_bundle_and_shadow_promotion_gate",
    )


def build_markdown(card: dict[str, Any]) -> str:
    gap = card["gap_summary"]
    decision = card["decision"]
    floor = card["next_qualified_window_target"]
    run_trigger = card["run_trigger_summary"]
    lines = [
        "# Soft-Mainline Shadow Promotion Gap",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Current Gap",
        "",
        f"- Qualified tradeable windows: `{gap['qualified_tradeable_windows']}` / `{gap['min_qualified_windows']}`.",
        f"- Active abstention holdouts: `{gap['active_abstain_windows']}` / `{gap['min_active_abstain_windows']}`.",
        f"- Clean strict rescue closes still needed by repeat gap: `{gap['repeat_remaining_strict_rescue_closes']}`.",
        f"- Bridge accepted actions shortfall: `{gap['bridge_accepted_actions_shortfall']}`.",
        f"- Shadow blockers: `{', '.join(gap['shadow_gate_blockers']) or 'none'}`.",
        "",
        "## Next Qualifying Window Target",
        "",
        f"- accepted actions >= `{floor['min_accepted_actions']}`",
        f"- fills >= `{floor['min_fills']}`",
        f"- strict rescue closes >= `{floor['min_strict_rescue_closes']}`",
        f"- pair PnL >= `{floor['min_pair_pnl']}`",
        f"- residual qty/cost share <= `{floor['max_residual_qty_share']}` / `{floor['max_residual_cost_share']}`",
        f"- rescue L1 age <= `{floor['max_rescue_l1_age_ms']}` ms and source blocks <= `{floor['max_source_blocks']}` or absorbed before admission",
        "",
        "## Run Trigger",
        "",
        f"- run trigger status: `{run_trigger['status']}`",
        f"- latest next action: `{run_trigger['next_action']}`",
        "- Do not run longer than 1800s to compensate for weak rescue density.",
        "- Do not treat the next sample as shadow approval unless runtime, repeat, regime, and promotion gates all pass.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    shadow_gate = load_json(args.shadow_promotion_gate_scorecard)
    regime = load_json(args.regime_generalization_scorecard)
    run_trigger = load_json(args.run_trigger_policy_scorecard)
    repeat_gap = load_json(args.repeat_gap_scorecard)
    source_audit = load_json(args.source_caveat_audit_scorecard)
    bridge_gap = load_json(args.bridge_shadow_gap_scorecard)

    shadow_decision = body(shadow_gate, "decision")
    regime_summary = body(regime, "summary") or body(shadow_gate, "regime_summary")
    run_decision = body(run_trigger, "decision")
    repeat_remaining = body(repeat_gap, "remaining_gaps")
    repeat_floor = body(repeat_gap, "next_window_floor")
    bridge_remaining = body(bridge_gap, "remaining_gaps")
    source_absorbed = source_caveat_absorbed(source_audit)

    min_qualified = inum(body(shadow_gate, "thresholds").get("min_qualified_windows"), args.min_qualified_windows)
    min_active_abstain = inum(
        body(shadow_gate, "thresholds").get("min_active_abstain_windows"),
        args.min_active_abstain_windows,
    )
    qualified = inum(regime_summary.get("qualified_tradeable_count"))
    active_abstain = inum(regime_summary.get("active_abstain_count"))
    missing_qualified = max(0, min_qualified - qualified)
    missing_active_abstain = max(0, min_active_abstain - active_abstain)
    repeat_remaining_rescues = inum(repeat_remaining.get("total_strict_rescue_closes"))
    bridge_accepted_shortfall = inum(bridge_remaining.get("accepted_actions"))

    gate_status, next_action = pick_status(
        shadow_gate=shadow_gate,
        run_trigger=run_trigger,
        missing_qualified_windows=missing_qualified,
    )
    if run_decision.get("next_action") and gate_status.startswith("BLOCKED"):
        next_action = str(run_decision["next_action"])

    hard_blockers = sorted(
        {
            str(item)
            for item in [
                *listify(shadow_decision.get("hard_blockers")),
                *listify(run_decision.get("hard_blockers")),
            ]
            if item
        }
    )
    if missing_qualified:
        hard_blockers.append("missing_additional_qualified_tradeable_window")
    if missing_active_abstain:
        hard_blockers.append("missing_active_weak_abstention_holdout")
    if args.source_caveat_audit_scorecard and not source_absorbed:
        hard_blockers.append("source_caveat_audit_not_absorbed")
    hard_blockers = sorted(set(hard_blockers))

    # The repeat gap may need only five more rescues, but shadow review still
    # requires the next window to pass the per-window sample gate.
    min_rescues_for_next_window = max(
        args.min_window_strict_rescue_closes,
        inum(repeat_floor.get("min_strict_rescue_closes_to_clear_aggregate")),
    )
    next_window_target = {
        "min_accepted_actions": inum(repeat_floor.get("min_accepted_actions"), args.min_window_accepted_actions),
        "min_fills": inum(repeat_floor.get("min_fills"), args.min_window_fills),
        "min_strict_rescue_closes": min_rescues_for_next_window,
        "min_pair_pnl": fnum(repeat_floor.get("min_pair_pnl"), args.min_pair_pnl),
        "max_residual_qty_share": fnum(repeat_floor.get("max_residual_qty_share"), args.max_residual_qty_share),
        "max_residual_cost_share": fnum(repeat_floor.get("max_residual_cost_share"), args.max_residual_cost_share),
        "max_rescue_net_pair_cost": fnum(repeat_floor.get("max_rescue_net_pair_cost"), args.max_rescue_net_pair_cost),
        "max_rescue_l1_age_ms": fnum(repeat_floor.get("max_rescue_l1_age_ms"), args.max_rescue_l1_age_ms),
        "max_source_blocks": args.max_source_blocks,
        "allow_absorbed_source_blocks": True,
        "must_pass_per_window_gates": True,
    }

    card = {
        "artifact": "xuan_soft_mainline_shadow_promotion_gap_planner",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_shadow_promotion_gap_planner.py",
        "status": gate_status,
        "inputs": {
            "shadow_promotion_gate_scorecard": str(Path(args.shadow_promotion_gate_scorecard).expanduser())
            if args.shadow_promotion_gate_scorecard
            else None,
            "regime_generalization_scorecard": str(Path(args.regime_generalization_scorecard).expanduser())
            if args.regime_generalization_scorecard
            else None,
            "run_trigger_policy_scorecard": str(Path(args.run_trigger_policy_scorecard).expanduser())
            if args.run_trigger_policy_scorecard
            else None,
            "repeat_gap_scorecard": str(Path(args.repeat_gap_scorecard).expanduser())
            if args.repeat_gap_scorecard
            else None,
            "source_caveat_audit_scorecard": str(Path(args.source_caveat_audit_scorecard).expanduser())
            if args.source_caveat_audit_scorecard
            else None,
            "bridge_shadow_gap_scorecard": str(Path(args.bridge_shadow_gap_scorecard).expanduser())
            if args.bridge_shadow_gap_scorecard
            else None,
        },
        "source_statuses": {
            "shadow_promotion_gate": status(shadow_gate) or None,
            "regime_generalization": status(regime) or None,
            "run_trigger_policy": status(run_trigger) or None,
            "repeat_gap": status(repeat_gap) or None,
            "source_caveat_audit": status(source_audit) or None,
            "bridge_shadow_gap": status(bridge_gap) or None,
        },
        "gap_summary": {
            "qualified_tradeable_windows": qualified,
            "min_qualified_windows": min_qualified,
            "missing_qualified_tradeable_windows": missing_qualified,
            "active_abstain_windows": active_abstain,
            "min_active_abstain_windows": min_active_abstain,
            "missing_active_abstain_windows": missing_active_abstain,
            "repeat_remaining_strict_rescue_closes": repeat_remaining_rescues,
            "bridge_accepted_actions_shortfall": bridge_accepted_shortfall,
            "bridge_gap_status": status(bridge_gap) or None,
            "shadow_gate_blockers": listify(shadow_decision.get("hard_blockers")),
            "run_trigger_blockers": listify(run_decision.get("hard_blockers")),
        },
        "source_caveat_summary": {
            "provided": bool(args.source_caveat_audit_scorecard),
            "absorbed": source_absorbed,
            "source_block_rows": body(source_audit, "observed").get("source_block_rows"),
            "accepted_l1_age_ms_max": body(source_audit, "observed").get("accepted_l1_age_ms_max"),
            "rescue_l1_age_ms_max": body(source_audit, "observed").get("rescue_l1_age_ms_max"),
            "strict_rescue_source_blocks": body(source_audit, "observed").get("strict_rescue_source_blocks"),
        },
        "next_qualified_window_target": next_window_target,
        "run_trigger_summary": {
            "status": status(run_trigger) or None,
            "next_action": run_decision.get("next_action"),
            "run_longer_recommended": body(run_trigger, "run_duration_policy").get("run_longer_recommended"),
            "standard_duration_s": body(run_trigger, "run_duration_policy").get("standard_duration_s"),
        },
        "autoresearch_loop": [
            "keep no-cancel soft-mainline economics fixed",
            "wait for fresh prefix or completed density signal instead of blind 30m repeats",
            "run at most one bounded 1800s PM_DRY_RUN sample only when density is ready",
            "postrun score runtime, repeat, regime, run-trigger, and promotion gates",
            "use weak active windows as abstention holdouts, not as failed universal-trading targets",
        ],
        "decision": {
            "next_action": next_action,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_review_ready": is_keep(shadow_gate),
            "deployable": False,
            "hard_blockers": hard_blockers,
            "source_caveat_absorbed": source_absorbed,
            "bridge_accepted_actions_shortfall": bridge_accepted_shortfall,
        },
        "guardrails": [
            "This planner is local planning evidence only.",
            "It does not launch remote jobs or approve shadow.",
            "A fresh qualified window must pass per-window gates; aggregate arithmetic alone is insufficient.",
            "Do not widen economics, run longer than 1800s, or return to ce25 hard gates to manufacture density.",
            "Absorbed source blocks do not count as accepted/rescue contamination.",
        ],
    }
    return rounded(card)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-promotion-gate-scorecard", required=True)
    parser.add_argument("--regime-generalization-scorecard", required=True)
    parser.add_argument("--run-trigger-policy-scorecard", required=True)
    parser.add_argument("--repeat-gap-scorecard", required=True)
    parser.add_argument("--source-caveat-audit-scorecard")
    parser.add_argument("--bridge-shadow-gap-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--min-qualified-windows", type=int, default=2)
    parser.add_argument("--min-active-abstain-windows", type=int, default=1)
    parser.add_argument("--min-window-accepted-actions", type=int, default=25)
    parser.add_argument("--min-window-fills", type=int, default=20)
    parser.add_argument("--min-window-strict-rescue-closes", type=int, default=7)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-rescue-net-pair-cost", type=float, default=0.95)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
