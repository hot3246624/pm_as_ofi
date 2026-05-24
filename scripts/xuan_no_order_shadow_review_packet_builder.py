#!/usr/bin/env python3
"""Build a local xuan-frontier no-order shadow-review evidence packet.

This packet is an evidence organizer, not a promotion scorer. It pulls together
the current replay source-truth scorecard, runtime scorecards, repeat-window
gate status, and the next bounded profile so reviewers have one stable artifact
for the current state.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def metric_line(label: str, value: Any) -> str:
    return f"- {label}: {fmt(value)}"


def build_packet(args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    replay_path = Path(args.replay_scorecard).expanduser().resolve()
    runtime_path = Path(args.runtime_scorecard).expanduser().resolve()
    repeat_path = Path(args.repeat_scorecard).expanduser().resolve()
    gap_path = Path(args.gap_plan_scorecard).expanduser().resolve()
    profile_path = Path(args.profile_scorecard).expanduser().resolve()
    concurrency_path = Path(args.concurrency_scorecard).expanduser().resolve() if args.concurrency_scorecard else None
    capital_path = Path(args.capital_roi_scorecard).expanduser().resolve() if args.capital_roi_scorecard else None

    replay = read_json(replay_path)
    runtime = read_json(runtime_path)
    repeat = read_json(repeat_path)
    gap = read_json(gap_path)
    profile = read_json(profile_path)
    concurrency = read_json(concurrency_path) if concurrency_path and concurrency_path.exists() else None
    capital = read_json(capital_path) if capital_path and capital_path.exists() else None

    replay_metrics = replay.get("metrics", {})
    replay_econ = replay_metrics.get("economics", {})
    runtime_metrics = runtime.get("metrics", {})
    repeat_aggregate = repeat.get("aggregate", {})
    remaining_gaps = gap.get("remaining_gaps", {})
    next_floor = gap.get("next_window_floor", {})
    profile_body = profile.get("profile", {})

    hard_blockers = list(repeat.get("hard_blockers", []))
    concurrency_clean = True
    if concurrency is not None:
        concurrency_clean = concurrency.get("decision", {}).get("concurrent_shared_ingress_evidence_clean") is True
        hard_blockers.extend(f"concurrency:{item}" for item in concurrency.get("hard_blockers", []))
    hard_blockers = sorted(set(hard_blockers))
    shadow_ready = repeat.get("decision", {}).get("shadow_review_ready") is True and concurrency_clean
    packet_status = (
        "KEEP_SHADOW_REVIEW_PACKET_READY_RESEARCH_ONLY"
        if shadow_ready
        else "UNKNOWN_SHADOW_REVIEW_PACKET_PENDING_REPEAT_WINDOW_GAP"
    )

    summary = {
        "replay_status": replay.get("status"),
        "runtime_status": runtime.get("status"),
        "repeat_window_status": repeat.get("status"),
        "gap_plan_status": gap.get("status"),
        "profile_status": profile.get("status"),
        "concurrency_status": concurrency.get("status") if concurrency else None,
        "capital_roi_status": capital.get("status") if capital else None,
        "shadow_review_ready": shadow_ready,
        "deployable": False,
        "remote_runner_allowed": False,
        "hard_blockers": hard_blockers,
    }

    packet_json = {
        "status": packet_status,
        "summary": summary,
        "paths": {
            "replay_scorecard": str(replay_path),
            "runtime_scorecard": str(runtime_path),
            "repeat_scorecard": str(repeat_path),
            "gap_plan_scorecard": str(gap_path),
            "profile_scorecard": str(profile_path),
            "concurrency_scorecard": str(concurrency_path) if concurrency_path else None,
            "capital_roi_scorecard": str(capital_path) if capital_path else None,
        },
        "replay_evidence": {
            "fee_after_with_rescue": as_float(replay_econ.get("fee_after_with_rescue")),
            "roi_after_rescue": as_float(replay_econ.get("roi_after_rescue")),
            "residual_cost_after_rescue": as_float(replay_econ.get("residual_cost_after_rescue")),
            "worst_day_stress100_with_rescue": as_float(replay_econ.get("worst_day_stress100_with_rescue")),
            "source_truth_ready": replay.get("decision", {}).get("source_truth_ready", True),
            "private_truth_ready": False,
        },
        "latest_runtime_window": {
            "accepted_actions": runtime_metrics.get("accepted_actions"),
            "queue_supported_fills": runtime_metrics.get("queue_supported_fills"),
            "strict_rescue_closes": runtime_metrics.get("strict_rescue_closes"),
            "strict_rescue_qty": runtime_metrics.get("strict_rescue_qty"),
            "pair_pnl": runtime_metrics.get("pair_pnl"),
            "residual_qty_share": runtime_metrics.get("residual_qty_share"),
            "residual_cost_share": runtime_metrics.get("residual_cost_share"),
            "rescue_net_pair_cost_max": runtime_metrics.get("rescue_net_pair_cost_max"),
            "strict_rescue_source_blocks": runtime_metrics.get("strict_rescue_source_blocks"),
        },
        "repeat_window": {
            "aggregate": repeat_aggregate,
            "remaining_gaps": remaining_gaps,
            "next_window_floor": next_floor,
        },
        "concurrent_shared_ingress_evidence": {
            "status": concurrency.get("status") if concurrency else None,
            "clean": concurrency_clean,
            "hard_blockers": concurrency.get("hard_blockers", []) if concurrency else [],
            "warnings": concurrency.get("warnings", []) if concurrency else [],
        },
        "capital_reuse_roi": capital.get("aggregate", {}) if capital else {},
        "next_profile": profile_body,
        "decision": {
            "research_only": True,
            "shadow_review_ready": shadow_ready,
            "deployable": False,
            "remote_runner_allowed": False,
            "needs_explicit_remote_authorization_for_next_sample": True,
        },
    }

    lines = [
        "# Xuan-Frontier No-Order Shadow-Review Evidence Packet",
        "",
        "## Status",
        metric_line("packet_status", packet_status),
        metric_line("shadow_review_ready", shadow_ready),
        metric_line("deployable", False),
        metric_line("remote_runner_allowed", False),
        metric_line("hard_blockers", ", ".join(hard_blockers) if hard_blockers else "none"),
        "",
        "## Capital-Reuse ROI",
        metric_line("status", capital.get("status") if capital else None),
        metric_line(
            "edge_on_redeem_notional",
            capital.get("aggregate", {}).get("round_roi", {}).get("edge_on_redeem_notional") if capital else None,
        ),
        metric_line(
            "roi_on_pair_cost",
            capital.get("aggregate", {}).get("round_roi", {}).get("roi_on_pair_cost") if capital else None,
        ),
        metric_line(
            "legacy_roi_on_filled_seed_cost",
            capital.get("aggregate", {}).get("round_roi", {}).get("roi_on_filled_seed_cost") if capital else None,
        ),
        metric_line(
            "max_window_gross_cash_need",
            capital.get("aggregate", {}).get("capital_reuse", {}).get("max_window_gross_cash_need") if capital else None,
        ),
        metric_line(
            "capital_roi_on_max_window_gross_cash_need",
            capital.get("aggregate", {}).get("capital_reuse", {}).get("capital_roi_on_max_window_gross_cash_need") if capital else None,
        ),
        metric_line(
            "profit_per_round_if_300_redeem_notional_filled",
            capital.get("aggregate", {}).get("projection", {}).get("profit_per_round_if_redeem_notional_filled") if capital else None,
        ),
        metric_line(
            "profit_per_day_if_300_redeem_notional_filled_every_round",
            capital.get("aggregate", {}).get("projection", {}).get("profit_per_day_if_redeem_notional_filled_every_round") if capital else None,
        ),
        metric_line(
            "worst_case_pair_pnl_if_residual_zero",
            capital.get("aggregate", {}).get("round_roi", {}).get("worst_case_pair_pnl_if_residual_zero") if capital else None,
        ),
        "",
        "## Replay Evidence",
        metric_line("status", replay.get("status")),
        metric_line("fee_after_with_rescue", packet_json["replay_evidence"]["fee_after_with_rescue"]),
        metric_line("roi_after_rescue", packet_json["replay_evidence"]["roi_after_rescue"]),
        metric_line("residual_cost_after_rescue", packet_json["replay_evidence"]["residual_cost_after_rescue"]),
        metric_line("worst_day_stress100_with_rescue", packet_json["replay_evidence"]["worst_day_stress100_with_rescue"]),
        "",
        "## Latest Comparable Runtime Window",
        metric_line("status", runtime.get("status")),
        metric_line("accepted_actions", runtime_metrics.get("accepted_actions")),
        metric_line("fills", runtime_metrics.get("queue_supported_fills")),
        metric_line("strict_rescue_closes", runtime_metrics.get("strict_rescue_closes")),
        metric_line("pair_pnl", runtime_metrics.get("pair_pnl")),
        metric_line("residual_qty_share", runtime_metrics.get("residual_qty_share")),
        metric_line("residual_cost_share", runtime_metrics.get("residual_cost_share")),
        metric_line("rescue_net_pair_cost_max", runtime_metrics.get("rescue_net_pair_cost_max")),
        metric_line("strict_rescue_source_blocks", runtime_metrics.get("strict_rescue_source_blocks")),
        "",
        "## Concurrent Shared-Ingress Evidence",
        metric_line("status", concurrency.get("status") if concurrency else None),
        metric_line("clean", concurrency_clean),
        metric_line(
            "hard_blockers",
            ", ".join(concurrency.get("hard_blockers", [])) if concurrency and concurrency.get("hard_blockers") else "none",
        ),
        "",
        "## Repeat-Window Gate",
        metric_line("status", repeat.get("status")),
        metric_line("accepted_actions", repeat_aggregate.get("accepted_actions")),
        metric_line("fills", repeat_aggregate.get("queue_supported_fills")),
        metric_line("strict_rescue_closes", repeat_aggregate.get("strict_rescue_closes")),
        metric_line("pair_pnl", repeat_aggregate.get("pair_pnl")),
        metric_line("residual_qty_share", repeat_aggregate.get("residual_qty_share")),
        metric_line("residual_cost_share", repeat_aggregate.get("residual_cost_share")),
        "",
        "## Remaining Gap",
        metric_line("strict_rescue_closes_needed", remaining_gaps.get("total_strict_rescue_closes")),
        metric_line("accepted_actions_needed", remaining_gaps.get("total_accepted_actions")),
        metric_line("fills_needed", remaining_gaps.get("total_fills")),
        metric_line("pair_pnl_needed", remaining_gaps.get("total_pair_pnl")),
        "",
        "## Next Same-Profile Sample",
        metric_line("duration_s", profile_body.get("duration_s")),
        metric_line("round_offsets", profile_body.get("round_offsets")),
        metric_line("soft_cap", profile_body.get("soft_cap")),
        metric_line("debt_floor", profile_body.get("debt_floor")),
        metric_line("debt_budget", profile_body.get("debt_budget")),
        metric_line("target_rescue_net_cap", profile_body.get("target_rescue_net_cap")),
        metric_line("strict_rescue_salvage_net_cap", profile_body.get("strict_rescue_salvage_net_cap")),
        metric_line("min_strict_rescue_closes_to_clear_aggregate", next_floor.get("min_strict_rescue_closes_to_clear_aggregate")),
        "",
        "## Guardrails",
        "- This packet is research-only and not deployable.",
        "- It does not authorize a remote run.",
        "- The next remote dry-run still requires explicit user authorization.",
        "- Forced diagnostics, relaxed-source diagnostics, counterfactual sweeps, and public trader analysis are excluded from promotion evidence.",
        "",
    ]
    return packet_json, "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-scorecard", required=True)
    parser.add_argument("--runtime-scorecard", required=True)
    parser.add_argument("--repeat-scorecard", required=True)
    parser.add_argument("--gap-plan-scorecard", required=True)
    parser.add_argument("--profile-scorecard", required=True)
    parser.add_argument("--concurrency-scorecard", default=None)
    parser.add_argument("--capital-roi-scorecard", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--scorecard-json", required=True)
    args = parser.parse_args()

    started = time.time()
    packet, markdown = build_packet(args)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = output_dir / "SHADOW_REVIEW_EVIDENCE_PACKET.md"
    markdown_path.write_text(markdown + "\n")

    scorecard = {
        "artifact": "xuan_no_order_shadow_review_packet_builder",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_shadow_review_packet_builder.py",
        "markdown_path": str(markdown_path),
        **packet,
    }
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
