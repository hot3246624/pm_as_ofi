#!/usr/bin/env python3
"""Plan remaining caveats after bridge-aware shadow review approval.

This local-only helper turns approval caveats into explicit evidence gaps.
It does not launch remote jobs, approve deploy, approve live orders, or change
strategy economics.
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


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def is_keep(card: dict[str, Any]) -> bool:
    raw_status = status(card) or ""
    return raw_status.startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


CAVEAT_PLANS: dict[str, dict[str, Any]] = {
    "private_truth_not_ready": {
        "category": "private_truth_reconciliation",
        "live_deploy_blocker": True,
        "required_evidence": [
            "Owner-scoped order/fill/inventory truth reconciliation for the approved windows.",
            "No mismatch between no-order event ledger, fills, residual inventory, and private truth.",
        ],
        "safe_local_checks": [
            "Prepare a private-truth request using only local scorecard identifiers and aggregate metrics.",
            "If private summaries already exist locally, reconcile them without touching raw/replay or shared services.",
        ],
        "future_remote_dry_run_justified": False,
        "remote_rationale": "A dry-run cannot prove private truth; this needs owner/private truth data.",
        "next_artifact": "private_truth_reconciliation_request_or_scorecard",
    },
    "projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence": {
        "category": "capacity_evidence",
        "live_deploy_blocker": True,
        "required_evidence": [
            "Capacity ladder evidence at the proposed size instead of linear extrapolation from cap25.",
            "Per-stage source, PnL, rescue, residual, and safety gates remain clean.",
        ],
        "safe_local_checks": [
            "Build a staged capacity-ladder plan with explicit stop conditions before any new run.",
            "Verify that bridge evidence is not reused as runtime capacity proof.",
        ],
        "future_remote_dry_run_justified": False,
        "may_justify_future_bounded_dry_run_after_local_plan": True,
        "remote_rationale": "Only a separately reviewed capacity-stage manifest can justify a future bounded dry-run.",
        "next_artifact": "capacity_ladder_evidence_plan",
    },
    "residual_cost_share_still_live_caveat": {
        "category": "residual_cost_risk",
        "live_deploy_blocker": True,
        "required_evidence": [
            "Residual cost share improves or has a bounded loss-control explanation at live size.",
            "Residual tail diagnostics show no mature material lot pattern that can defeat pair PnL.",
        ],
        "safe_local_checks": [
            "Summarize mature material residual lots and worst residual-zero stress by window.",
            "Separate residual reporting caveats from strategy changes; do not widen surplus caps to hide residuals.",
        ],
        "future_remote_dry_run_justified": False,
        "may_justify_future_bounded_dry_run_after_local_plan": True,
        "remote_rationale": "A future run is useful only if a local residual-tail hypothesis defines a measurable target.",
        "next_artifact": "residual_tail_gap_scorecard",
    },
    "residual_qty_share_above_public_hard_target": {
        "category": "public_benchmark_residual",
        "live_deploy_blocker": True,
        "required_evidence": [
            "Residual qty share meets the public hard target, or the benchmark scope difference is explicitly accepted.",
            "Bridge residual gates remain clear while public residual caveat is tracked separately.",
        ],
        "safe_local_checks": [
            "Quantify public hard-target gap and compare it to xuan bridge residual gates.",
            "Keep the public benchmark caveat visible in handoff artifacts; do not promote it to deploy clearance.",
        ],
        "future_remote_dry_run_justified": False,
        "may_justify_future_bounded_dry_run_after_local_plan": True,
        "remote_rationale": "A future run needs a residual-lowering profile or benchmark-specific rationale.",
        "next_artifact": "public_residual_target_gap_scorecard",
    },
    "residual_zero_stress_negative_size_capacity_needed_before_live": {
        "category": "residual_zero_stress",
        "live_deploy_blocker": True,
        "required_evidence": [
            "Residual-zero stress is non-negative at the target capacity or bounded by explicit size controls.",
            "Worst-case residual liquidation does not erase pair PnL at the proposed live size.",
        ],
        "safe_local_checks": [
            "Compute the current residual-zero stress gap and the maximum size implied by that gap.",
            "Design a residual-zero stress gate for capacity review before any live decision.",
        ],
        "future_remote_dry_run_justified": False,
        "may_justify_future_bounded_dry_run_after_local_plan": True,
        "remote_rationale": "A dry-run is useful only after local size/stress thresholds are fixed.",
        "next_artifact": "residual_zero_stress_capacity_plan",
    },
}


def caveat_plan(caveat: str, evidence: dict[str, Any], related: dict[str, Any]) -> dict[str, Any]:
    template = CAVEAT_PLANS.get(
        caveat,
        {
            "category": "unclassified",
            "live_deploy_blocker": True,
            "required_evidence": ["Classify this caveat before promotion."],
            "safe_local_checks": ["Add a local scorer mapping this caveat to required evidence."],
            "future_remote_dry_run_justified": False,
            "remote_rationale": "Unclassified caveats cannot justify remote work.",
            "next_artifact": "classified_caveat_plan",
        },
    )
    observed: dict[str, Any] = {}
    if caveat == "private_truth_not_ready":
        observed = {
            "replay_status": evidence.get("replay_status"),
            "private_truth_ready": False,
        }
    elif caveat == "projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence":
        observed = {
            "profit_per_round_if_300_redeem_notional_filled": evidence.get(
                "profit_per_round_if_300_redeem_notional_filled"
            ),
            "profit_per_day_if_300_redeem_notional_filled_every_round": evidence.get(
                "profit_per_day_if_300_redeem_notional_filled_every_round"
            ),
            "surplus_bridge_window_count": body(related.get("surplus_bridge", {}), "aggregate").get(
                "bridge_eligible_window_count"
            ),
        }
    elif caveat == "residual_cost_share_still_live_caveat":
        observed = {
            "residual_cost": evidence.get("residual_cost"),
            "residual_cost_to_pair_qty": evidence.get("residual_cost_to_pair_qty"),
            "runtime_residual_cost_share": body(related.get("runtime", {}), "metrics").get(
                "residual_cost_share"
            ),
            "bridge_residual_cost_share": body(related.get("surplus_bridge", {}), "aggregate").get(
                "residual_cost_share"
            ),
            "mature_material_residual_cost": body(
                related.get("young_tiny_residual", {}), "residual_summary"
            )
            .get("class_cost", {})
            .get("mature_material"),
        }
    elif caveat == "residual_qty_share_above_public_hard_target":
        public = related.get("public_benchmark", {})
        observed = {
            "runtime_residual_qty_share": body(related.get("runtime", {}), "metrics").get(
                "residual_qty_share"
            ),
            "bridge_residual_qty_share": body(related.get("surplus_bridge", {}), "aggregate").get(
                "residual_qty_share"
            ),
            "public_target_residual_rate_lte": body(public, "public_review_targets").get(
                "target_residual_rate_lte"
            ),
            "public_hard_residual_rate_lte": body(public, "public_review_targets").get(
                "hard_residual_rate_lte"
            ),
            "public_target_misses": body(public, "decision").get("public_target_misses"),
            "residual_qty_share_delta_vs_b55": evidence.get("residual_qty_share_delta_vs_b55"),
        }
    elif caveat == "residual_zero_stress_negative_size_capacity_needed_before_live":
        observed = {
            "worst_case_pair_pnl_if_residual_zero": evidence.get(
                "worst_case_pair_pnl_if_residual_zero"
            ),
            "pair_pnl": evidence.get("pair_pnl"),
            "pair_qty": evidence.get("pair_qty"),
            "residual_cost": evidence.get("residual_cost"),
        }
    return {
        "caveat": caveat,
        **template,
        "observed_evidence": rounded(observed),
    }


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    evidence = card["approval_evidence"]
    lines = [
        "# Xuan Shadow Review Caveat Gap Plan",
        "",
        f"Status: `{card['status']}`",
        f"Research shadow review ready: `{decision['research_shadow_review_ready']}`",
        f"Deployable: `{decision['deployable']}`",
        f"Live orders allowed: `{decision['live_orders_allowed']}`",
        f"Remote runner allowed now: `{decision['remote_runner_allowed']}`",
        f"Future remote requires separate rationale: `{decision['future_remote_requires_separate_rationale']}`",
        "",
        "## Current Review Evidence",
        "",
        f"- approval gate: `{card['source_statuses']['approval_gate']}`",
        f"- shadow evidence ledger: `{card['source_statuses']['shadow_evidence_ledger']}`",
        f"- surplus bridge: `{card['source_statuses']['surplus_bridge']}`",
        f"- public benchmark: `{card['source_statuses']['public_benchmark']}`",
        f"- young/tiny residual: `{card['source_statuses']['young_tiny_residual']}`",
        f"- pair PnL: `{evidence.get('pair_pnl')}`",
        f"- residual cost to pair qty: `{evidence.get('residual_cost_to_pair_qty')}`",
        f"- worst residual-zero PnL: `{evidence.get('worst_case_pair_pnl_if_residual_zero')}`",
        "",
        "## Caveat Plan",
        "",
    ]
    for item in card["caveat_plans"]:
        lines.extend(
            [
                f"### {item['caveat']}",
                "",
                f"- category: `{item['category']}`",
                f"- live deploy blocker: `{item['live_deploy_blocker']}`",
                f"- future remote dry-run justified now: `{item['future_remote_dry_run_justified']}`",
                f"- next artifact: `{item['next_artifact']}`",
                f"- remote rationale: {item['remote_rationale']}",
                "",
                "Required evidence:",
                *(f"- {entry}" for entry in item["required_evidence"]),
                "",
                "Safe local checks:",
                *(f"- {entry}" for entry in item["safe_local_checks"]),
                "",
            ]
        )
    lines.extend(
        [
            "## Guardrails",
            "",
            "- This plan does not approve deploy, live orders, service restarts, or shared-service mutation.",
            "- This plan does not justify another remote run by itself.",
            "- Any future remote needs a separate bounded PM_DRY_RUN rationale and manifest/verifier pass.",
            "- Keep weak-density windows as holdouts against overfitting.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    approval = load_json(args.approval_gate_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    public = load_json(args.public_benchmark_comparison_scorecard)
    young = load_json(args.young_tiny_residual_scorecard)
    surplus = load_json(args.surplus_bridge_scorecard)
    ledger = load_json(args.shadow_evidence_ledger_scorecard)

    approval_decision = body(approval, "decision")
    approval_evidence = body(approval, "evidence")
    approval_bridge = body(approval, "bridge_interpretation")
    approval_caveats = [str(item) for item in listify(approval_decision.get("caveats")) if item]
    approval_hard_blockers = [
        str(item) for item in listify(approval_decision.get("hard_blockers")) if item
    ]

    related = {
        "runtime": runtime,
        "public_benchmark": public,
        "young_tiny_residual": young,
        "surplus_bridge": surplus,
        "shadow_evidence_ledger": ledger,
    }
    plans = [caveat_plan(caveat, approval_evidence, related) for caveat in approval_caveats]

    approval_keep = is_keep(approval) and bool(approval_decision.get("shadow_review_approval_ready"))
    review_surface_clean = bool(body(ledger, "decision").get("review_surface_clean"))
    all_known = all(item["category"] != "unclassified" for item in plans)
    if approval_hard_blockers:
        card_status = "UNKNOWN_SHADOW_REVIEW_CAVEAT_GAP_PLAN_APPROVAL_BLOCKED_LOCAL_ONLY"
        next_action = "resolve_approval_gate_hard_blockers_before_caveat_planning"
    elif approval_keep and all_known:
        card_status = "KEEP_SHADOW_REVIEW_CAVEAT_GAP_PLAN_READY_LOCAL_ONLY"
        next_action = "work_local_live_deploy_caveat_artifacts_without_remote"
    else:
        card_status = "UNKNOWN_SHADOW_REVIEW_CAVEAT_GAP_PLAN_NEEDS_CLASSIFICATION_LOCAL_ONLY"
        next_action = "classify_unmapped_caveats_before_remote_or_deploy_decision"

    runtime_metrics = body(runtime, "metrics")
    public_decision = body(public, "decision")
    public_targets = body(public, "public_review_targets")
    young_decision = body(young, "decision")
    young_summary = body(young, "residual_summary")
    surplus_aggregate = body(surplus, "aggregate")

    return rounded(
        {
            "artifact": "xuan_shadow_review_caveat_gap_planner",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_caveat_gap_planner.py",
            "status": card_status,
            "inputs": {
                "approval_gate_scorecard": args.approval_gate_scorecard,
                "runtime_summary_scorecard": args.runtime_summary_scorecard,
                "public_benchmark_comparison_scorecard": args.public_benchmark_comparison_scorecard,
                "young_tiny_residual_scorecard": args.young_tiny_residual_scorecard,
                "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
                "shadow_evidence_ledger_scorecard": args.shadow_evidence_ledger_scorecard,
            },
            "source_statuses": {
                "approval_gate": status(approval),
                "runtime_summary": status(runtime),
                "public_benchmark": status(public),
                "young_tiny_residual": status(young),
                "surplus_bridge": status(surplus),
                "shadow_evidence_ledger": status(ledger),
            },
            "decision": {
                "research_shadow_review_ready": approval_keep,
                "review_surface_clean": review_surface_clean,
                "paper_shadow_only": True,
                "research_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_requires_separate_rationale": True,
                "future_remote_allowed_by_this_plan": False,
                "next_action": next_action,
                "hard_blockers": approval_hard_blockers,
                "caveats": approval_caveats,
                "unclassified_caveats": [
                    item["caveat"] for item in plans if item["category"] == "unclassified"
                ],
            },
            "approval_bridge_interpretation": {
                "bridge_repeat_ready": approval_bridge.get("bridge_repeat_ready"),
                "repeat_metric_source": approval_bridge.get("repeat_metric_source"),
                "surplus_bridge_status": approval_bridge.get("surplus_bridge_status"),
                "surplus_bridge_accepted_actions": approval_bridge.get(
                    "surplus_bridge_accepted_actions"
                ),
                "surplus_bridge_strict_rescue_closes": approval_bridge.get(
                    "surplus_bridge_strict_rescue_closes"
                ),
                "surplus_bridge_residual_qty_share": approval_bridge.get(
                    "surplus_bridge_residual_qty_share"
                ),
                "surplus_bridge_residual_cost_share": approval_bridge.get(
                    "surplus_bridge_residual_cost_share"
                ),
            },
            "approval_evidence": {
                "pair_pnl": approval_evidence.get("pair_pnl"),
                "pair_qty": approval_evidence.get("pair_qty"),
                "edge_on_redeem_notional": approval_evidence.get("edge_on_redeem_notional"),
                "roi_on_total_cash_spend": approval_evidence.get("roi_on_total_cash_spend"),
                "residual_cost": approval_evidence.get("residual_cost"),
                "residual_cost_to_pair_qty": approval_evidence.get(
                    "residual_cost_to_pair_qty"
                ),
                "worst_case_pair_pnl_if_residual_zero": approval_evidence.get(
                    "worst_case_pair_pnl_if_residual_zero"
                ),
                "actual_pair_cost_after_fee": approval_evidence.get("actual_pair_cost_after_fee"),
                "pair_cost_delta_vs_b55": approval_evidence.get("pair_cost_delta_vs_b55"),
                "residual_qty_share_delta_vs_b55": approval_evidence.get(
                    "residual_qty_share_delta_vs_b55"
                ),
            },
            "runtime_evidence": {
                "accepted_actions": runtime_metrics.get("accepted_actions"),
                "queue_supported_fills": runtime_metrics.get("queue_supported_fills"),
                "strict_rescue_closes": runtime_metrics.get("strict_rescue_closes"),
                "pair_pnl": runtime_metrics.get("pair_pnl"),
                "residual_qty_share": runtime_metrics.get("residual_qty_share"),
                "residual_cost_share": runtime_metrics.get("residual_cost_share"),
                "accepted_l1_age_ms_max": runtime_metrics.get("accepted_l1_age_ms_max"),
                "rescue_l1_age_ms_max": runtime_metrics.get("rescue_l1_age_ms_max"),
                "strict_rescue_source_blocks": runtime_metrics.get("strict_rescue_source_blocks"),
            },
            "bridge_evidence": {
                "accepted_actions": surplus_aggregate.get("accepted_actions"),
                "queue_supported_fills": surplus_aggregate.get("queue_supported_fills"),
                "strict_rescue_closes": surplus_aggregate.get("strict_rescue_closes"),
                "pair_pnl": surplus_aggregate.get("pair_pnl"),
                "roi_on_filled_cost": surplus_aggregate.get("roi_on_filled_cost"),
                "residual_qty_share": surplus_aggregate.get("residual_qty_share"),
                "residual_cost_share": surplus_aggregate.get("residual_cost_share"),
                "bridge_eligible_window_count": surplus_aggregate.get(
                    "bridge_eligible_window_count"
                ),
            },
            "public_residual_evidence": {
                "public_benchmark_comparison_pass": public_decision.get(
                    "public_benchmark_comparison_pass"
                ),
                "shadow_review_compatible_via_bridge": public_decision.get(
                    "shadow_review_compatible_via_bridge"
                ),
                "public_target_misses": public_decision.get("public_target_misses"),
                "target_residual_rate_lte": public_targets.get("target_residual_rate_lte"),
                "hard_residual_rate_lte": public_targets.get("hard_residual_rate_lte"),
            },
            "young_residual_evidence": {
                "per_window_residual_gate_clear": young_decision.get(
                    "per_window_residual_gate_clear"
                ),
                "residual_caveats": young_decision.get("residual_caveats"),
                "mature_material_cost": body(young_summary, "class_cost").get(
                    "mature_material"
                ),
                "mature_material_qty": body(young_summary, "class_qty").get("mature_material"),
                "lot_count": young_summary.get("lot_count"),
            },
            "caveat_plans": plans,
            "guardrails": {
                "no_remote_launched": True,
                "no_deploy_or_restart": True,
                "no_live_orders": True,
                "no_shared_service_mutation": True,
                "no_strategy_economics_relaxed": True,
                "weak_density_holdout_preserved": True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--approval-gate-scorecard", required=True)
    parser.add_argument("--runtime-summary-scorecard")
    parser.add_argument("--public-benchmark-comparison-scorecard")
    parser.add_argument("--young-tiny-residual-scorecard")
    parser.add_argument("--surplus-bridge-scorecard")
    parser.add_argument("--shadow-evidence-ledger-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(build_markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if not scorecard["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
