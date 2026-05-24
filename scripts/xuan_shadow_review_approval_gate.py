#!/usr/bin/env python3
"""Build a fee-aware xuan-frontier shadow-review approval gate."""

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
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def as_bool(value: Any) -> bool:
    return value is True


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def pct(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value * 100.0


def fail_unless(condition: bool, blocker: str, hard_blockers: list[str]) -> None:
    if not condition:
        hard_blockers.append(blocker)


def build_gate(args: argparse.Namespace) -> dict[str, Any]:
    packet_path = Path(args.shadow_packet).expanduser().resolve()
    capital_path = Path(args.capital_roi_scorecard).expanduser().resolve()
    repeat_path = Path(args.repeat_scorecard).expanduser().resolve()
    replay_path = Path(args.replay_scorecard).expanduser().resolve()

    packet = read_json(packet_path)
    capital = read_json(capital_path)
    repeat = read_json(repeat_path)
    replay = read_json(replay_path)

    packet_summary = packet.get("summary", {})
    packet_decision = packet.get("decision", {})
    capital_decision = capital.get("decision", {})
    capital_aggregate = capital.get("aggregate", {})
    capital_totals = capital_aggregate.get("totals", {})
    round_roi = capital_aggregate.get("round_roi", {})
    capital_reuse = capital_aggregate.get("capital_reuse", {})
    projection = capital_aggregate.get("projection", {})
    repeat_decision = repeat.get("decision", {})
    repeat_aggregate = repeat.get("aggregate", {})
    replay_decision = replay.get("decision", {})

    fee_accounting = capital.get("assumptions", {}).get("fee_accounting")
    taker_fee = as_float(capital_totals.get("taker_fee"))
    edge_on_redeem = as_float(round_roi.get("edge_on_redeem_notional"))
    roi_on_total_cash = as_float(round_roi.get("roi_on_total_cash_spend"))
    worst_residual_zero = as_float(round_roi.get("worst_case_pair_pnl_if_residual_zero"))
    residual_cost_to_pair_qty = as_float(round_roi.get("residual_cost_to_pair_qty"))

    hard_blockers: list[str] = []
    caveats: list[str] = []

    fail_unless(
        packet.get("status") == "KEEP_SHADOW_REVIEW_PACKET_READY_RESEARCH_ONLY",
        "shadow_packet_not_ready",
        hard_blockers,
    )
    fail_unless(as_bool(packet_summary.get("shadow_review_ready")), "shadow_packet_not_ready_flag", hard_blockers)
    fail_unless(packet_summary.get("hard_blockers") == [], "shadow_packet_hard_blockers_present", hard_blockers)
    fail_unless(as_bool(packet_decision.get("research_only")), "shadow_packet_not_research_only", hard_blockers)
    fail_unless(packet_decision.get("deployable") is False, "shadow_packet_deployable_true", hard_blockers)
    fail_unless(
        packet_decision.get("remote_runner_allowed") is False,
        "shadow_packet_remote_runner_allowed_true",
        hard_blockers,
    )
    fail_unless(
        packet_summary.get("runtime_status") == "KEEP_THIRD_WINDOW_RUNTIME_SUMMARY_SAFE_RESEARCH_ONLY",
        "runtime_summary_not_safe_keep",
        hard_blockers,
    )
    fail_unless(
        packet_summary.get("concurrency_status")
        == "KEEP_NO_ORDER_CONCURRENT_SHARED_INGRESS_EVIDENCE_PASS_RESEARCH_ONLY",
        "concurrent_shared_ingress_evidence_not_clean",
        hard_blockers,
    )
    fail_unless(
        repeat.get("status") == "KEEP_REPEAT_WINDOW_SHADOW_REVIEW_SCORER_PASS_RESEARCH_ONLY_READY_FOR_REVIEW",
        "repeat_window_scorer_not_ready",
        hard_blockers,
    )
    fail_unless(as_bool(repeat_decision.get("shadow_review_ready")), "repeat_window_shadow_ready_false", hard_blockers)
    fail_unless(repeat_decision.get("deployable") is False, "repeat_window_deployable_true", hard_blockers)
    fail_unless(
        replay.get("status") == "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY",
        "replay_source_truth_status_not_validated",
        hard_blockers,
    )
    fail_unless(
        replay_decision.get("source_truth_ready") is True
        or "SOURCE_TRUTH_VALIDATED" in str(replay.get("status", "")),
        "replay_source_truth_not_ready",
        hard_blockers,
    )
    fail_unless(
        capital.get("status") == "KEEP_CAPITAL_REUSE_ROI_SCORER_PASS_RESEARCH_ONLY",
        "capital_reuse_roi_not_keep",
        hard_blockers,
    )
    fail_unless(
        as_bool(capital_decision.get("shadow_review_ready_metric_addendum")),
        "capital_reuse_roi_addendum_not_ready",
        hard_blockers,
    )
    fail_unless(
        fee_accounting == "strict_rescue_taker_fee_per_share_is_in_net_pair_cost_and_pair_pnl",
        "fee_accounting_not_explicitly_in_pair_pnl",
        hard_blockers,
    )
    fail_unless(taker_fee > 0.0, "taker_fee_not_observed", hard_blockers)
    fail_unless(edge_on_redeem >= args.min_edge_on_redeem, "edge_on_redeem_below_review_floor", hard_blockers)
    fail_unless(roi_on_total_cash > 0.0, "roi_on_total_cash_non_positive", hard_blockers)
    fail_unless(
        as_float(repeat_aggregate.get("strict_rescue_closes")) >= args.min_repeat_rescue_closes,
        "repeat_strict_rescue_closes_below_min",
        hard_blockers,
    )
    fail_unless(
        as_float(repeat_aggregate.get("residual_cost_share")) <= args.max_repeat_residual_cost_share,
        "repeat_residual_cost_share_above_max",
        hard_blockers,
    )
    fail_unless(
        as_float(repeat_aggregate.get("residual_qty_share")) <= args.max_repeat_residual_qty_share,
        "repeat_residual_qty_share_above_max",
        hard_blockers,
    )

    if worst_residual_zero < 0:
        caveats.append("residual_zero_stress_negative_size_capacity_needed_before_live")
    if residual_cost_to_pair_qty > args.max_residual_cost_to_pair_qty_for_live_hint:
        caveats.append("residual_cost_share_still_live_caveat")
    if as_float(projection.get("round_notional")) > 0:
        caveats.append("projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence")
    if replay_decision.get("private_truth_ready") is not True:
        caveats.append("private_truth_not_ready")

    shadow_review_approved = not hard_blockers
    status = (
        "KEEP_SHADOW_REVIEW_APPROVAL_GATE_PASS_RESEARCH_ONLY"
        if shadow_review_approved
        else "UNKNOWN_SHADOW_REVIEW_APPROVAL_GATE_BLOCKED"
    )
    return {
        "artifact": "xuan_shadow_review_approval_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_approval_gate.py",
        "status": status,
        "inputs": {
            "shadow_packet": str(packet_path),
            "capital_roi_scorecard": str(capital_path),
            "repeat_scorecard": str(repeat_path),
            "replay_scorecard": str(replay_path),
            "min_edge_on_redeem": args.min_edge_on_redeem,
            "min_repeat_rescue_closes": args.min_repeat_rescue_closes,
            "max_repeat_residual_cost_share": args.max_repeat_residual_cost_share,
            "max_repeat_residual_qty_share": args.max_repeat_residual_qty_share,
        },
        "decision": {
            "shadow_review_approval_ready": shadow_review_approved,
            "paper_shadow_only": shadow_review_approved,
            "research_only": True,
            "deployable": False,
            "live_orders_allowed": False,
            "remote_runner_allowed": False,
            "requires_separate_live_deploy_approval": True,
            "requires_capacity_ladder_before_live": True,
            "hard_blockers": hard_blockers,
            "caveats": sorted(set(caveats)),
        },
        "evidence": {
            "replay_status": replay.get("status"),
            "packet_status": packet.get("status"),
            "repeat_status": repeat.get("status"),
            "capital_roi_status": capital.get("status"),
            "fee_accounting": fee_accounting,
            "taker_fee": taker_fee,
            "pair_pnl": as_float(capital_totals.get("pair_pnl")),
            "pair_qty": as_float(capital_totals.get("pair_qty")),
            "edge_on_redeem_notional": edge_on_redeem,
            "roi_on_pair_cost": as_float(round_roi.get("roi_on_pair_cost")),
            "roi_on_total_cash_spend": roi_on_total_cash,
            "legacy_roi_on_filled_seed_cost": as_float(round_roi.get("roi_on_filled_seed_cost")),
            "max_window_gross_cash_need": as_float(capital_reuse.get("max_window_gross_cash_need")),
            "turnover_filled_cost_on_max_open": as_float(
                capital_reuse.get("turnover_filled_cost_on_max_window_open_seed_cost")
            ),
            "capital_roi_on_max_window_gross_cash_need": as_float(
                capital_reuse.get("capital_roi_on_max_window_gross_cash_need")
            ),
            "residual_cost": as_float(capital_totals.get("residual_cost")),
            "residual_cost_to_pair_qty": residual_cost_to_pair_qty,
            "worst_case_pair_pnl_if_residual_zero": worst_residual_zero,
            "profit_per_round_if_300_redeem_notional_filled": as_float(
                projection.get("profit_per_round_if_redeem_notional_filled")
            ),
            "profit_per_day_if_300_redeem_notional_filled_every_round": as_float(
                projection.get("profit_per_day_if_redeem_notional_filled_every_round")
            ),
        },
        "approval_scope": {
            "allowed": [
                "shadow_review",
                "paper_shadow_review",
                "bounded_no_order_research_review",
                "local_scorecard_generation",
            ],
            "not_allowed": [
                "live_orders",
                "production_deploy",
                "service_restart",
                "shared_ingress_mutation",
                "collector_rebuild_or_publish",
                "raw_replay_mutation",
            ],
        },
        "next_review_questions": [
            "Can 300 USD per-round redeem-notional capacity be demonstrated without edge decay?",
            "Does residual-zero stress stay non-negative at larger size or over longer windows?",
            "Can private owner order/fill/inventory truth be reconciled before any live decision?",
        ],
    }


def markdown(scorecard: dict[str, Any]) -> str:
    decision = scorecard["decision"]
    evidence = scorecard["evidence"]
    scope = scorecard["approval_scope"]
    lines = [
        "# Xuan-Frontier Shadow Review Approval Gate",
        "",
        "## Decision",
        "",
        f"- status: `{scorecard['status']}`",
        f"- shadow_review_approval_ready: `{decision['shadow_review_approval_ready']}`",
        f"- paper_shadow_only: `{decision['paper_shadow_only']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) if decision['hard_blockers'] else 'none'}`",
        f"- caveats: `{', '.join(decision['caveats']) if decision['caveats'] else 'none'}`",
        "",
        "## Fee-Aware Evidence",
        "",
        f"- fee_accounting: `{evidence['fee_accounting']}`",
        f"- taker_fee: `{round_opt(evidence['taker_fee'], 6)}`",
        f"- pair_pnl_after_fee: `{round_opt(evidence['pair_pnl'], 6)}`",
        f"- pair_qty_redeem_notional: `{round_opt(evidence['pair_qty'], 6)}`",
        f"- edge_on_redeem_notional: `{round_opt(pct(evidence['edge_on_redeem_notional']), 4)}%`",
        f"- roi_on_pair_cost: `{round_opt(pct(evidence['roi_on_pair_cost']), 4)}%`",
        f"- roi_on_total_cash_spend: `{round_opt(pct(evidence['roi_on_total_cash_spend']), 4)}%`",
        "",
        "## Capital Reuse",
        "",
        f"- max_window_gross_cash_need: `{round_opt(evidence['max_window_gross_cash_need'], 6)}`",
        f"- filled_cost_turnover_on_max_open: `{round_opt(evidence['turnover_filled_cost_on_max_open'], 4)}x`",
        f"- capital_roi_on_max_window_gross_cash_need: `{round_opt(pct(evidence['capital_roi_on_max_window_gross_cash_need']), 4)}%`",
        "",
        "## Capacity Hypothesis",
        "",
        f"- profit_per_round_if_300_redeem_notional_filled: `{round_opt(evidence['profit_per_round_if_300_redeem_notional_filled'], 6)}`",
        f"- profit_per_day_if_300_redeem_notional_filled_every_round: `{round_opt(evidence['profit_per_day_if_300_redeem_notional_filled_every_round'], 6)}`",
        "- This is a linear capacity hypothesis, not demonstrated 300 USD runtime capacity.",
        "",
        "## Residual Risk",
        "",
        f"- residual_cost: `{round_opt(evidence['residual_cost'], 6)}`",
        f"- residual_cost_to_pair_qty: `{round_opt(pct(evidence['residual_cost_to_pair_qty']), 4)}%`",
        f"- worst_case_pair_pnl_if_residual_zero: `{round_opt(evidence['worst_case_pair_pnl_if_residual_zero'], 6)}`",
        "",
        "## Approved Scope",
        "",
        *(f"- {item}" for item in scope["allowed"]),
        "",
        "## Not Approved",
        "",
        *(f"- {item}" for item in scope["not_allowed"]),
        "",
        "## Next Review Questions",
        "",
        *(f"- {item}" for item in scorecard["next_review_questions"]),
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-packet", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--repeat-scorecard", required=True)
    parser.add_argument("--replay-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    parser.add_argument("--min-edge-on-redeem", type=float, default=0.02)
    parser.add_argument("--min-repeat-rescue-closes", type=float, default=20.0)
    parser.add_argument("--max-repeat-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-repeat-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-to-pair-qty-for-live-hint", type=float, default=0.02)
    args = parser.parse_args()

    scorecard = build_gate(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(scorecard))
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if not scorecard["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
