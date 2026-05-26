#!/usr/bin/env python3
"""Build a local research packet for the capped cap25 result.

This packet consolidates the clean runtime, source-blip-aware density gate, and
04b6 public proxy benchmark. It is not a promotion, deployment, live, or cap75
authorization artifact.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


RUN_TAG = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"
DEFAULT_DENSITY_GATE = Path(".tmp_xuan/scorecards/xuan_shadow_review_source_blip_aware_density_gate_20260526T1840Z.json")
DEFAULT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_capped_diagnostics_result_20260526T1757Z.json")
DEFAULT_POSTRUN = Path(f".tmp_xuan/scorecards/{RUN_TAG}_postrun_bundle.json")
DEFAULT_SOURCE_RECON = Path(".tmp_xuan/scorecards/xuan_shadow_review_source_blip_density_reconciliation_20260526T1832Z.json")
DEFAULT_PUBLIC_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_current_proxy_review_20260526T1802Z.json")
DEFAULT_CAPACITY_PLAN = Path(".tmp_xuan/scorecards/xuan_shadow_review_capacity_ladder_evidence_plan_20260526T0855Z.json")
DEFAULT_PRIVATE_TRUTH = Path(".tmp_xuan/scorecards/xuan_shadow_review_private_truth_request_20260526T0824Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_research_packet_20260526T1845Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_cap25_capped_research_packet_20260526T1845Z/CAP25_CAPPED_RESEARCH_PACKET.md"
)


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open(encoding="utf-8") as handle:
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


def pct(value: Any) -> float | None:
    num = fnum(value)
    return None if num is None else num * 100.0


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def build(args: argparse.Namespace) -> dict[str, Any]:
    density_gate = load_json(args.density_gate_scorecard)
    result = load_json(args.result_scorecard)
    postrun = load_json(args.postrun_bundle_scorecard)
    source_recon = load_json(args.source_reconciliation_scorecard)
    public_04b6 = load_json(args.public_04b6_scorecard)
    capacity_plan = load_json(args.capacity_plan_scorecard)
    private_truth = load_json(args.private_truth_scorecard)

    gate_decision = body(density_gate, "decision")
    result_decision = body(result, "decision")
    result_observed = body(result, "observed")
    gate_observed = body(density_gate, "observed")
    public_04b6_pair = body(public_04b6, "paired_5m_15m_false")
    public_04b6_taker = body(public_04b6, "taker_proxy")
    private_truth_decision = body(private_truth, "decision")
    capacity_decision = body(capacity_plan, "decision")

    packet_ready = (
        is_keep(density_gate)
        and bool(gate_decision.get("cap25_research_review_ready"))
        and bool(gate_decision.get("source_blip_aware_density_pass"))
        and bool(result_decision.get("remote_completed"))
        and result_decision.get("remote_exit_code") == 0
        and not bool(result_decision.get("aggregate_missing"))
        and bool(result_decision.get("postrun_complete"))
        and is_keep(postrun)
        and is_keep(source_recon)
    )

    hard_blockers: list[str] = []
    if not packet_ready:
        hard_blockers.append("cap25_research_packet_inputs_not_ready")
    if bool(gate_decision.get("future_remote_allowed_by_this_gate")):
        hard_blockers.append("density_gate_allows_remote_unexpected")
    if bool(gate_decision.get("cap75_remote_rationale_ready")):
        hard_blockers.append("density_gate_allows_cap75_unexpected")
    if bool(gate_decision.get("deployable")) or bool(gate_decision.get("live_orders_allowed")):
        hard_blockers.append("density_gate_live_or_deploy_unexpected")

    status_value = (
        "KEEP_SHADOW_REVIEW_CAP25_CAPPED_RESEARCH_PACKET_READY_LOCAL_ONLY"
        if not hard_blockers
        else "BLOCKED_SHADOW_REVIEW_CAP25_CAPPED_RESEARCH_PACKET_LOCAL_ONLY"
    )

    cap25_vs_04b6 = {
        "xuan_cap25_residual_qty_share": result_observed.get("residual_qty_share"),
        "xuan_cap25_residual_cost_share": result_observed.get("residual_cost_share"),
        "xuan_cap25_pair_pnl": result_observed.get("pair_pnl"),
        "xuan_cap25_rescue_net_pair_cost_proxy": None,
        "public_04b6_pair_cost_5m_15m": public_04b6_pair.get("weighted_pair_cost"),
        "public_04b6_residual_qty_share": public_04b6_pair.get("residual_qty_share"),
        "public_04b6_residual_cost_share": public_04b6_pair.get("residual_cost_share"),
        "public_04b6_taker_notional_share": public_04b6_taker.get("takerOnly_true_notional_share_of_false"),
        "read": "04b6 is a public maker-heavy benchmark; xuan cap25 is cleaner runtime evidence but still has higher residual share.",
    }

    card = {
        "artifact": "xuan_shadow_review_cap25_capped_research_packet",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_cap25_capped_research_packet.py",
        "status": status_value,
        "inputs": {
            "density_gate_scorecard": str(args.density_gate_scorecard),
            "result_scorecard": str(args.result_scorecard),
            "postrun_bundle_scorecard": str(args.postrun_bundle_scorecard),
            "source_reconciliation_scorecard": str(args.source_reconciliation_scorecard),
            "public_04b6_scorecard": str(args.public_04b6_scorecard),
            "capacity_plan_scorecard": str(args.capacity_plan_scorecard),
            "private_truth_scorecard": str(args.private_truth_scorecard),
        },
        "source_statuses": {
            "density_gate": status(density_gate),
            "result": status(result),
            "postrun_bundle": status(postrun),
            "source_reconciliation": status(source_recon),
            "public_04b6": status(public_04b6),
            "capacity_plan": status(capacity_plan),
            "private_truth": status(private_truth),
        },
        "decision": {
            "cap25_capped_research_packet_ready": not hard_blockers,
            "cap25_source_blip_aware_density_review_ready": bool(
                gate_decision.get("cap25_research_review_ready")
            ),
            "runner_density_gate_pass": bool(gate_decision.get("runner_density_gate_pass")),
            "source_blip_aware_density_pass": bool(gate_decision.get("source_blip_aware_density_pass")),
            "runtime_clean": bool(gate_decision.get("runtime_clean")),
            "source_blip_absorbed": bool(gate_decision.get("source_blip_absorbed")),
            "private_truth_ready": bool(private_truth_decision.get("private_truth_ready")),
            "capacity_current_stage_gate_pass": bool(capacity_decision.get("current_stage_gate_pass")),
            "promotion_ready": False,
            "shadow_review_ready": False,
            "paper_shadow_only": True,
            "research_only": True,
            "future_remote_allowed_by_this_packet": False,
            "future_remote_requires_new_local_rationale": True,
            "cap75_remote_rationale_ready": False,
            "same_profile_repeat_allowed": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "hard_blockers": hard_blockers,
            "next_action": "local_only_capacity_interpretation_or_residual_tail_plan_before_any_new_remote",
        },
        "observed": {
            "accepted_actions": result_observed.get("accepted_actions"),
            "queue_supported_fills": result_observed.get("queue_supported_fills"),
            "strict_rescue_or_salvage_rows": result_observed.get("strict_rescue_or_salvage_rows"),
            "fill_rate_pct": pct(result_observed.get("fill_rate")),
            "rescue_per_candidate_pct": pct(result_observed.get("rescue_per_candidate")),
            "rescue_per_fill_pct": pct(result_observed.get("rescue_per_fill")),
            "pair_pnl": result_observed.get("pair_pnl"),
            "residual_qty_share_pct": pct(result_observed.get("residual_qty_share")),
            "residual_cost_share_pct": pct(result_observed.get("residual_cost_share")),
            "source_block_rows": gate_observed.get("source_block_rows"),
            "source_block_l1_age_ms_max": gate_observed.get("source_block_l1_age_ms_max"),
            "strict_rescue_source_blocks": gate_observed.get("strict_rescue_source_blocks"),
        },
        "public_04b6_proxy_comparison": cap25_vs_04b6,
        "interpretation": {
            "cap25_signal": "reviewable_research_runtime_with_source_blip_aware_density",
            "capacity_signal": "cap75_still_blocked_until_current_stage_capacity_gate_clears",
            "private_truth_signal": "not_private_truth",
            "public_benchmark_signal": "04b6_supports_maker_heavy_low_taker_shape_but_not_copy_or_private_truth",
            "promotion_signal": "not_promotion_evidence",
        },
    }
    return rounded(card)


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    observed = card["observed"]
    comparison = card["public_04b6_proxy_comparison"]
    lines = [
        "# Xuan Cap25 Capped Research Packet",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- cap25_capped_research_packet_ready: `{decision['cap25_capped_research_packet_ready']}`",
        f"- runner_density_gate_pass: `{decision['runner_density_gate_pass']}`",
        f"- source_blip_aware_density_pass: `{decision['source_blip_aware_density_pass']}`",
        f"- private_truth_ready: `{decision['private_truth_ready']}`",
        f"- capacity_current_stage_gate_pass: `{decision['capacity_current_stage_gate_pass']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- promotion_ready: `{decision['promotion_ready']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Cap25 Observed",
        "",
        f"- accepted/fills/rescues: `{observed['accepted_actions']}` / `{observed['queue_supported_fills']}` / `{observed['strict_rescue_or_salvage_rows']}`",
        f"- pair_pnl: `{observed['pair_pnl']}`",
        f"- residual qty/cost share pct: `{observed['residual_qty_share_pct']}` / `{observed['residual_cost_share_pct']}`",
        f"- source_block_rows/source_block_l1_age_ms_max: `{observed['source_block_rows']}` / `{observed['source_block_l1_age_ms_max']}`",
        "",
        "## Public 04b6 Proxy",
        "",
        f"- pair_cost_5m_15m: `{comparison['public_04b6_pair_cost_5m_15m']}`",
        f"- residual qty/cost share: `{comparison['public_04b6_residual_qty_share']}` / `{comparison['public_04b6_residual_cost_share']}`",
        f"- taker_notional_share: `{comparison['public_04b6_taker_notional_share']}`",
        "",
        "## Boundary",
        "",
        "- Research-only packet; no deploy, no live orders, no cap75/150/300.",
        "- Historical shadow/no-order runs remain non-private-truth.",
        "- Future remote requires a separate local rationale.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--density-gate-scorecard", type=Path, default=DEFAULT_DENSITY_GATE)
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--postrun-bundle-scorecard", type=Path, default=DEFAULT_POSTRUN)
    parser.add_argument("--source-reconciliation-scorecard", type=Path, default=DEFAULT_SOURCE_RECON)
    parser.add_argument("--public-04b6-scorecard", type=Path, default=DEFAULT_PUBLIC_04B6)
    parser.add_argument("--capacity-plan-scorecard", type=Path, default=DEFAULT_CAPACITY_PLAN)
    parser.add_argument("--private-truth-scorecard", type=Path, default=DEFAULT_PRIVATE_TRUTH)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown(card) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
