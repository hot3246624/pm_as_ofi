#!/usr/bin/env python3
"""Summarize the bridge-aware soft-mainline shadow evidence gap.

The standard runtime/repeat gates are intentionally strict and can miss the
bridge context: one large clean base window plus one fresh explicit-surplus
window may clear rescue/PnL/residual economics while still being short on a
single aggregate count. This local-only scorer makes that bridge gap explicit.
It never approves shadow, remote execution, or deployment by itself.
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
        return [rounded(item) for item in value]
    return value


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def gap_min(threshold: Any, observed: Any, *, integer: bool = False) -> float | int:
    t = fnum(threshold, 0.0) or 0.0
    o = fnum(observed, 0.0) or 0.0
    gap = max(0.0, t - o)
    return math.ceil(gap) if integer else gap


def gap_max(observed: Any, threshold: Any) -> float:
    t = fnum(threshold, 0.0) or 0.0
    o = fnum(observed, 0.0) or 0.0
    return max(0.0, o - t)


def build_markdown(card: dict[str, Any]) -> str:
    agg = card["bridge_aggregate"]
    gaps = card["remaining_gaps"]
    decision = card["decision"]
    lines = [
        "# Bridge-Aware Shadow Gap",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Aggregate",
        "",
        f"- bridge windows: `{agg['bridge_eligible_window_count']}` / `{card['thresholds']['min_bridge_eligible_windows']}`",
        f"- accepted/fills/rescues: `{agg['accepted_actions']}` / `{agg['queue_supported_fills']}` / `{agg['strict_rescue_closes']}`",
        f"- pair PnL: `{agg['pair_pnl']}`",
        f"- residual qty/cost share: `{agg['residual_qty_share']}` / `{agg['residual_cost_share']}`",
        "",
        "## Remaining Gap",
        "",
        f"- accepted actions shortfall: `{gaps['accepted_actions']}`",
        f"- fills shortfall: `{gaps['queue_supported_fills']}`",
        f"- rescue close shortfall: `{gaps['strict_rescue_closes']}`",
        f"- pair PnL shortfall: `{gaps['pair_pnl']}`",
        f"- residual qty/cost excess: `{gaps['residual_qty_share_excess']}` / `{gaps['residual_cost_share_excess']}`",
        "",
        "## Guardrails",
        "",
        "- This is bridge planning evidence only, not shadow approval.",
        "- Any new window must still pass per-window quality gates; do not chase only the aggregate accepted count.",
        "- Keep PM_DRY_RUN, 1800s max duration, source gates, and explicit-surplus economics unchanged.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    bridge = load_json(args.bridge_scorecard)
    runtime = load_json(args.latest_runtime_summary)
    density = load_json(args.latest_density_preflight)
    source_audit = load_json(args.latest_source_caveat_audit)
    aggregate = body(bridge, "aggregate")
    thresholds = body(bridge, "thresholds")
    blockers = [str(item) for item in listify(bridge.get("hard_blockers")) if item]

    gaps = {
        "accepted_actions": gap_min(
            thresholds.get("min_total_accepted_actions"),
            aggregate.get("accepted_actions"),
            integer=True,
        ),
        "queue_supported_fills": gap_min(
            thresholds.get("min_total_fills"),
            aggregate.get("queue_supported_fills"),
            integer=True,
        ),
        "strict_rescue_closes": gap_min(
            thresholds.get("min_total_rescue_closes"),
            aggregate.get("strict_rescue_closes"),
            integer=True,
        ),
        "pair_pnl": gap_min(thresholds.get("min_total_pair_pnl"), aggregate.get("pair_pnl")),
        "bridge_eligible_windows": gap_min(
            thresholds.get("min_bridge_eligible_windows"),
            aggregate.get("bridge_eligible_window_count"),
            integer=True,
        ),
        "residual_qty_share_excess": gap_max(
            aggregate.get("residual_qty_share"),
            thresholds.get("max_total_residual_qty_share"),
        ),
        "residual_cost_share_excess": gap_max(
            aggregate.get("residual_cost_share"),
            thresholds.get("max_total_residual_cost_share"),
        ),
    }
    blocker_set = set(blockers)
    accepted_only = blocker_set == {"total_accepted_actions_below_min"}
    small_accepted_gap = gaps["accepted_actions"] <= args.small_accepted_gap_max
    bridge_clear = status(bridge).startswith("KEEP") and not blockers

    if bridge_clear:
        card_status = "KEEP_SOFT_MAINLINE_BRIDGE_SHADOW_GAP_CLEAR_LOCAL_ONLY"
        next_action = "rerun_formal_shadow_promotion_gate_before_any_shadow_review"
    elif accepted_only and small_accepted_gap:
        card_status = "UNKNOWN_SOFT_MAINLINE_BRIDGE_SHADOW_GAP_ACCEPTED_SHORT_LOCAL_ONLY"
        next_action = "collect_or_score_one_more_quality_bridge_window_before_shadow_review"
    else:
        card_status = "UNKNOWN_SOFT_MAINLINE_BRIDGE_SHADOW_GAP_QUALITY_OR_SAMPLE_BLOCKED_LOCAL_ONLY"
        next_action = "stay_local_until_bridge_quality_blockers_are_understood"

    density_observed = body(density, "observed")
    runtime_metrics = body(runtime, "metrics")
    source_blocks = inum(density_observed.get("source_blocks"))
    source_audit_status = status(source_audit)
    source_audit_absorbed = bool(body(source_audit, "decision").get("source_caveat_absorbed"))
    density_blockers = body(density, "decision").get("hard_blockers")
    density_blockers = [str(item) for item in density_blockers] if isinstance(density_blockers, list) else []
    caveats: list[str] = []
    if source_blocks > args.max_source_blocks:
        if source_audit_absorbed:
            caveats.append("latest_density_source_blocks_absorbed_by_source_caveat_audit")
        else:
            caveats.append("latest_density_source_blocks_above_max")
    if status(density).startswith("BLOCKED"):
        if source_audit_absorbed and density_blockers == ["source_blocks_above_max"]:
            caveats.append("latest_density_preflight_source_only_block_absorbed")
        else:
            caveats.append("latest_density_preflight_not_keep")
    if args.latest_source_caveat_audit and not source_audit_absorbed:
        caveats.append("latest_source_caveat_audit_not_absorbed")
    if runtime_metrics and fnum(runtime_metrics.get("rescue_net_pair_cost_max"), 0.0) > (
        fnum(thresholds.get("bridge_surplus_net_cap"), 1.02) or 1.02
    ):
        caveats.append("latest_rescue_net_pair_cost_above_bridge_surplus_cap")

    card = {
        "artifact": "xuan_soft_mainline_bridge_shadow_gap_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_bridge_shadow_gap_scorer.py",
        "status": card_status,
        "inputs": {
            "bridge_scorecard": str(Path(args.bridge_scorecard).expanduser()) if args.bridge_scorecard else None,
            "latest_runtime_summary": str(Path(args.latest_runtime_summary).expanduser())
            if args.latest_runtime_summary
            else None,
            "latest_density_preflight": str(Path(args.latest_density_preflight).expanduser())
            if args.latest_density_preflight
            else None,
            "latest_source_caveat_audit": str(Path(args.latest_source_caveat_audit).expanduser())
            if args.latest_source_caveat_audit
            else None,
        },
        "source_statuses": {
            "bridge": status(bridge) or None,
            "latest_runtime_summary": status(runtime) or None,
            "latest_density_preflight": status(density) or None,
            "latest_source_caveat_audit": source_audit_status or None,
        },
        "bridge_aggregate": aggregate,
        "thresholds": thresholds,
        "remaining_gaps": gaps,
        "bridge_blockers": blockers,
        "latest_window_caveats": sorted(set(caveats)),
        "decision": {
            "deployable": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "research_only": True,
            "bridge_repeat_gap_clear": bridge_clear,
            "bridge_accepted_only_gap": accepted_only,
            "small_accepted_gap": small_accepted_gap,
            "latest_source_caveat_absorbed": source_audit_absorbed,
            "next_action": next_action,
            "hard_blockers": blockers,
            "soft_caveats": sorted(set(caveats)),
        },
        "next_evidence_target": {
            "aggregate_accepted_actions_shortfall": gaps["accepted_actions"],
            "note": "A future evidence window still must pass per-window quality gates; do not target a tiny low-quality accepted-count patch.",
            "min_window_accepted_actions": thresholds.get("min_window_accepted_actions"),
            "min_window_fills": thresholds.get("min_window_fills"),
            "min_window_pair_pnl": thresholds.get("min_window_pair_pnl"),
            "max_window_residual_qty_share": thresholds.get("max_window_residual_qty_share"),
            "max_window_residual_cost_share": thresholds.get("max_window_residual_cost_share"),
        },
        "guardrails": [
            "Local scorer only; no remote launch, shadow approval, or deploy approval.",
            "Bridge evidence must be reconciled with formal runtime/repeat/regime gates before shadow review.",
            "Accepted-count gaps do not justify relaxing source, rescue, PnL, or residual economics.",
        ],
    }
    return rounded(card)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-scorecard", required=True)
    parser.add_argument("--latest-runtime-summary")
    parser.add_argument("--latest-density-preflight")
    parser.add_argument("--latest-source-caveat-audit")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--small-accepted-gap-max", type=int, default=5)
    parser.add_argument("--max-source-blocks", type=int, default=0)
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
