#!/usr/bin/env python3
"""Gate soft-mainline shadow promotion with regime holdout evidence.

This local-only gate prevents a single good soft-mainline window from becoming
shadow evidence by itself. Runtime and repeat-window scorers still need to pass,
but regime generalization must also show enough qualified windows and at least
one active weak-density abstention holdout.
"""

from __future__ import annotations

import argparse
import json
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


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def is_unknown(card: dict[str, Any]) -> bool:
    return status(card).startswith("UNKNOWN")


def is_blocked(card: dict[str, Any]) -> bool:
    return status(card).startswith("BLOCKED")


def decision(card: dict[str, Any]) -> dict[str, Any]:
    raw = card.get("decision")
    return raw if isinstance(raw, dict) else {}


def summary(card: dict[str, Any]) -> dict[str, Any]:
    raw = card.get("summary")
    return raw if isinstance(raw, dict) else {}


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def source_caveat_absorbed(source_audit: dict[str, Any]) -> bool:
    return bool(body(source_audit, "decision").get("source_caveat_absorbed"))


def bridge_gap_clear(bridge_gap: dict[str, Any]) -> bool:
    return status(bridge_gap).startswith("KEEP_SOFT_MAINLINE_BRIDGE_SHADOW_GAP_CLEAR")


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime_shadow = load_json(args.runtime_shadow_scorecard)
    repeat = load_json(args.repeat_scorecard)
    regime = load_json(args.regime_generalization_scorecard)
    run_trigger = load_json(args.run_trigger_policy_scorecard)
    source_audit = load_json(args.source_caveat_audit_scorecard)
    bridge_gap = load_json(args.bridge_shadow_gap_scorecard)

    regime_summary = summary(regime)
    regime_decision = decision(regime)
    qualified = int(regime_summary.get("qualified_tradeable_count") or 0)
    active_abstain = int(regime_summary.get("active_abstain_count") or 0)
    regime_hard_blockers = listify(regime_decision.get("hard_blockers_for_shadow"))
    bridge_clear = bridge_gap_clear(bridge_gap)

    hard_blockers: list[str] = []
    if not is_keep(runtime_shadow) and not bridge_clear:
        hard_blockers.append("runtime_shadow_readiness_not_keep")
    if not is_keep(repeat) and not bridge_clear:
        hard_blockers.append("repeat_window_scorer_not_keep")
    if not is_keep(regime):
        hard_blockers.append("regime_generalization_not_keep")
    if qualified < args.min_qualified_windows:
        hard_blockers.append("qualified_tradeable_windows_below_min")
    if active_abstain < args.min_active_abstain_windows:
        hard_blockers.append("active_weak_abstention_holdouts_below_min")
    if args.source_caveat_audit_scorecard and not source_caveat_absorbed(source_audit):
        hard_blockers.append("source_caveat_audit_not_absorbed")
    hard_blockers.extend(str(item) for item in regime_hard_blockers)
    hard_blockers = sorted(set(hard_blockers))

    if not hard_blockers:
        if bridge_clear and (not is_keep(runtime_shadow) or not is_keep(repeat)):
            gate_status = "KEEP_SOFT_MAINLINE_SHADOW_PROMOTION_GATE_BRIDGE_READY_RESEARCH_ONLY"
            recommendation = "shadow_review_packet_can_include_bridge_interpreted_runtime_repeat_pass"
        else:
            gate_status = "KEEP_SOFT_MAINLINE_SHADOW_PROMOTION_GATE_READY_RESEARCH_ONLY"
            recommendation = "shadow_review_packet_can_include_regime_gate_pass"
        shadow_review_ready = True
    elif is_unknown(runtime_shadow) or is_unknown(repeat):
        gate_status = "UNKNOWN_SOFT_MAINLINE_SHADOW_PROMOTION_GATE_SAMPLE_OR_REPEAT_BLOCKED_LOCAL_ONLY"
        recommendation = "collect_or_score_more_qualified_windows_before_shadow_review"
        shadow_review_ready = False
    else:
        gate_status = "BLOCKED_SOFT_MAINLINE_SHADOW_PROMOTION_GATE_REGIME_OR_SAMPLE_BLOCKED_LOCAL_ONLY"
        recommendation = "do_not_promote_until_runtime_repeat_and_regime_holdout_gates_pass"
        shadow_review_ready = False

    return {
        "artifact": "xuan_soft_mainline_shadow_promotion_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_shadow_promotion_gate.py",
        "status": gate_status,
        "inputs": {
            "runtime_shadow_scorecard": str(Path(args.runtime_shadow_scorecard).expanduser()),
            "repeat_scorecard": str(Path(args.repeat_scorecard).expanduser()),
            "regime_generalization_scorecard": str(Path(args.regime_generalization_scorecard).expanduser()),
            "run_trigger_policy_scorecard": str(Path(args.run_trigger_policy_scorecard).expanduser())
            if args.run_trigger_policy_scorecard
            else None,
            "source_caveat_audit_scorecard": str(Path(args.source_caveat_audit_scorecard).expanduser())
            if args.source_caveat_audit_scorecard
            else None,
            "bridge_shadow_gap_scorecard": str(Path(args.bridge_shadow_gap_scorecard).expanduser())
            if args.bridge_shadow_gap_scorecard
            else None,
        },
        "source_statuses": {
            "runtime_shadow": status(runtime_shadow) or None,
            "repeat": status(repeat) or None,
            "regime_generalization": status(regime) or None,
            "run_trigger_policy": status(run_trigger) or None,
            "source_caveat_audit": status(source_audit) or None,
            "bridge_shadow_gap": status(bridge_gap) or None,
        },
        "regime_summary": {
            "window_count": regime_summary.get("window_count"),
            "qualified_tradeable_count": qualified,
            "active_abstain_count": active_abstain,
            "traffic_abstain_count": regime_summary.get("traffic_abstain_count"),
            "regimes_seen": regime_summary.get("regimes_seen"),
            "overfit_risk_control": regime_summary.get("overfit_risk_control"),
            "hard_blockers_for_shadow": regime_hard_blockers,
        },
        "source_caveat_summary": {
            "provided": bool(args.source_caveat_audit_scorecard),
            "absorbed": source_caveat_absorbed(source_audit),
            "source_block_rows": body(source_audit, "observed").get("source_block_rows"),
            "accepted_l1_age_ms_max": body(source_audit, "observed").get("accepted_l1_age_ms_max"),
            "rescue_l1_age_ms_max": body(source_audit, "observed").get("rescue_l1_age_ms_max"),
            "strict_rescue_source_blocks": body(source_audit, "observed").get("strict_rescue_source_blocks"),
        },
        "bridge_summary": {
            "provided": bool(args.bridge_shadow_gap_scorecard),
            "clear": bridge_clear,
            "remaining_gaps": body(bridge_gap, "remaining_gaps"),
            "decision": body(bridge_gap, "decision"),
        },
        "thresholds": {
            "min_qualified_windows": args.min_qualified_windows,
            "min_active_abstain_windows": args.min_active_abstain_windows,
        },
        "decision": {
            "recommendation": recommendation,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_review_ready": shadow_review_ready,
            "deployable": False,
            "hard_blockers": hard_blockers,
            "source_caveat_absorbed": source_caveat_absorbed(source_audit),
            "bridge_runtime_repeat_interpretation": bridge_clear
            and (not is_keep(runtime_shadow) or not is_keep(repeat)),
            "legacy_runtime_repeat_blockers_explained_by_bridge": bridge_clear
            and set(hard_blockers).isdisjoint(
                {"runtime_shadow_readiness_not_keep", "repeat_window_scorer_not_keep"}
            ),
        },
        "guardrails": [
            "This gate is local planning/review evidence only.",
            "It does not launch remote jobs or approve deployment.",
            "A single good window cannot promote shadow without regime holdout coverage.",
            "Weak active windows must remain in the ledger as abstention holdouts.",
            "Absorbed source caveats do not relax source gates; they only prove stale rows were blocked before admission.",
            "Bridge interpretation does not weaken legacy runtime/repeat scorers; it uses explicit-surplus bridge evidence to explain their blockers.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-shadow-scorecard", required=True)
    parser.add_argument("--repeat-scorecard", required=True)
    parser.add_argument("--regime-generalization-scorecard", required=True)
    parser.add_argument("--run-trigger-policy-scorecard")
    parser.add_argument("--source-caveat-audit-scorecard")
    parser.add_argument("--bridge-shadow-gap-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-qualified-windows", type=int, default=2)
    parser.add_argument("--min-active-abstain-windows", type=int, default=1)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
