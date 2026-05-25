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


def read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return read_json(path)


def optional_path(value: str | None) -> Path | None:
    if not value:
        return None
    return Path(value).expanduser().resolve()


def status_is_keep(card: dict[str, Any] | None) -> bool:
    return isinstance(card, dict) and str(card.get("status", "")).startswith("KEEP")


def decision_hard_blockers(card: dict[str, Any] | None) -> list[str]:
    if not isinstance(card, dict):
        return []
    blockers = card.get("hard_blockers")
    if blockers is None:
        blockers = card.get("decision", {}).get("hard_blockers", [])
    return list(blockers or [])


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
    public_benchmark_path = optional_path(args.public_benchmark_comparison_scorecard)
    young_tiny_residual_path = optional_path(args.young_tiny_residual_scorecard)
    surplus_bridge_path = optional_path(args.surplus_bridge_scorecard)
    bridge_shadow_gap_path = optional_path(args.bridge_shadow_gap_scorecard)
    shadow_promotion_gate_path = optional_path(args.shadow_promotion_gate_scorecard)
    shadow_promotion_gap_path = optional_path(args.shadow_promotion_gap_scorecard)
    shadow_evidence_ledger_path = optional_path(args.shadow_evidence_ledger_scorecard)
    source_caveat_audit_path = optional_path(args.source_caveat_audit_scorecard)

    replay = read_json(replay_path)
    runtime = read_json(runtime_path)
    repeat = read_json(repeat_path)
    gap = read_json(gap_path)
    profile = read_json(profile_path)
    concurrency = read_json(concurrency_path) if concurrency_path and concurrency_path.exists() else None
    capital = read_json(capital_path) if capital_path and capital_path.exists() else None
    public_benchmark = read_optional_json(public_benchmark_path)
    young_tiny_residual = read_optional_json(young_tiny_residual_path)
    surplus_bridge = read_optional_json(surplus_bridge_path)
    bridge_shadow_gap = read_optional_json(bridge_shadow_gap_path)
    shadow_promotion_gate = read_optional_json(shadow_promotion_gate_path)
    shadow_promotion_gap = read_optional_json(shadow_promotion_gap_path)
    shadow_evidence_ledger = read_optional_json(shadow_evidence_ledger_path)
    source_caveat_audit = read_optional_json(source_caveat_audit_path)

    replay_metrics = replay.get("metrics", {})
    replay_econ = replay_metrics.get("economics", {})
    runtime_metrics = runtime.get("metrics", {})
    repeat_aggregate = repeat.get("aggregate", {})
    remaining_gaps = gap.get("remaining_gaps", {})
    next_floor = gap.get("next_window_floor", {})
    profile_body = profile.get("profile", {})

    legacy_repeat_blockers = sorted(set(repeat.get("hard_blockers", [])))
    hard_blockers = list(legacy_repeat_blockers)
    concurrency_clean = True
    concurrency_blockers: list[str] = []
    if concurrency is not None:
        concurrency_clean = concurrency.get("decision", {}).get("concurrent_shared_ingress_evidence_clean") is True
        concurrency_blockers = [f"concurrency:{item}" for item in concurrency.get("hard_blockers", [])]

    bridge_cards = {
        "surplus_bridge": surplus_bridge,
        "bridge_shadow_gap": bridge_shadow_gap,
        "shadow_promotion_gate": shadow_promotion_gate,
        "shadow_promotion_gap": shadow_promotion_gap,
        "shadow_evidence_ledger": shadow_evidence_ledger,
    }
    bridge_required_cards_present = all(card is not None for card in bridge_cards.values())
    bridge_required_cards_keep = bridge_required_cards_present and all(status_is_keep(card) for card in bridge_cards.values())
    bridge_hard_blockers = sorted(
        {
            f"{name}:{blocker}"
            for name, card in bridge_cards.items()
            for blocker in decision_hard_blockers(card)
        }
    )
    bridge_shadow_ready = bridge_required_cards_keep and not bridge_hard_blockers
    legacy_shadow_ready = repeat.get("decision", {}).get("shadow_review_ready") is True

    if bridge_shadow_ready:
        hard_blockers = list(concurrency_blockers)
    else:
        hard_blockers.extend(concurrency_blockers)
        hard_blockers.extend(bridge_hard_blockers)
    hard_blockers = sorted(set(hard_blockers))
    shadow_ready = concurrency_clean and (legacy_shadow_ready or bridge_shadow_ready)
    if shadow_ready and bridge_shadow_ready and not legacy_shadow_ready:
        packet_status = "KEEP_SHADOW_REVIEW_PACKET_BRIDGE_READY_RESEARCH_ONLY"
    elif shadow_ready:
        packet_status = "KEEP_SHADOW_REVIEW_PACKET_READY_RESEARCH_ONLY"
    else:
        packet_status = (
            "UNKNOWN_SHADOW_REVIEW_PACKET_PENDING_REPEAT_WINDOW_GAP"
        )

    public_target_misses = (
        public_benchmark.get("decision", {}).get("public_target_misses")
        if public_benchmark
        else "UNKNOWN_SHADOW_REVIEW_PACKET_PENDING_REPEAT_WINDOW_GAP"
    )
    if not isinstance(public_target_misses, list):
        public_target_misses = public_benchmark.get("decision", {}).get("hard_blockers", []) if public_benchmark else []
    young_residual_caveats = decision_hard_blockers(young_tiny_residual)

    summary = {
        "replay_status": replay.get("status"),
        "runtime_status": runtime.get("status"),
        "repeat_window_status": repeat.get("status"),
        "gap_plan_status": gap.get("status"),
        "profile_status": profile.get("status"),
        "concurrency_status": concurrency.get("status") if concurrency else None,
        "capital_roi_status": capital.get("status") if capital else None,
        "public_benchmark_status": public_benchmark.get("status") if public_benchmark else None,
        "young_tiny_residual_status": young_tiny_residual.get("status") if young_tiny_residual else None,
        "surplus_bridge_status": surplus_bridge.get("status") if surplus_bridge else None,
        "bridge_shadow_gap_status": bridge_shadow_gap.get("status") if bridge_shadow_gap else None,
        "shadow_promotion_gate_status": shadow_promotion_gate.get("status") if shadow_promotion_gate else None,
        "shadow_promotion_gap_status": shadow_promotion_gap.get("status") if shadow_promotion_gap else None,
        "shadow_evidence_ledger_status": shadow_evidence_ledger.get("status") if shadow_evidence_ledger else None,
        "source_caveat_audit_status": source_caveat_audit.get("status") if source_caveat_audit else None,
        "shadow_review_ready": shadow_ready,
        "legacy_shadow_review_ready": legacy_shadow_ready,
        "bridge_shadow_review_ready": bridge_shadow_ready,
        "shadow_review_ready_source": "bridge" if bridge_shadow_ready and not legacy_shadow_ready else "legacy" if legacy_shadow_ready else None,
        "deployable": False,
        "remote_runner_allowed": False,
        "hard_blockers": hard_blockers,
        "legacy_repeat_blockers_preserved": legacy_repeat_blockers,
        "public_benchmark_caveats": public_target_misses,
        "young_tiny_residual_caveats": young_residual_caveats,
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
            "public_benchmark_comparison_scorecard": str(public_benchmark_path)
            if public_benchmark_path
            else None,
            "young_tiny_residual_scorecard": str(young_tiny_residual_path) if young_tiny_residual_path else None,
            "surplus_bridge_scorecard": str(surplus_bridge_path) if surplus_bridge_path else None,
            "bridge_shadow_gap_scorecard": str(bridge_shadow_gap_path) if bridge_shadow_gap_path else None,
            "shadow_promotion_gate_scorecard": str(shadow_promotion_gate_path) if shadow_promotion_gate_path else None,
            "shadow_promotion_gap_scorecard": str(shadow_promotion_gap_path) if shadow_promotion_gap_path else None,
            "shadow_evidence_ledger_scorecard": str(shadow_evidence_ledger_path) if shadow_evidence_ledger_path else None,
            "source_caveat_audit_scorecard": str(source_caveat_audit_path) if source_caveat_audit_path else None,
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
            "legacy_shadow_review_ready": legacy_shadow_ready,
            "legacy_hard_blockers": legacy_repeat_blockers,
            "aggregate": repeat_aggregate,
            "remaining_gaps": remaining_gaps,
            "next_window_floor": next_floor,
        },
        "bridge_shadow_evidence": {
            "bridge_shadow_review_ready": bridge_shadow_ready,
            "bridge_required_cards_present": bridge_required_cards_present,
            "bridge_required_cards_keep": bridge_required_cards_keep,
            "statuses": {name: card.get("status") if card else None for name, card in bridge_cards.items()},
            "hard_blockers": bridge_hard_blockers,
            "surplus_bridge_aggregate": surplus_bridge.get("aggregate", {}) if surplus_bridge else {},
            "bridge_shadow_gap_remaining_gaps": bridge_shadow_gap.get("remaining_gaps", {}) if bridge_shadow_gap else {},
            "shadow_promotion_gate_decision": shadow_promotion_gate.get("decision", {}) if shadow_promotion_gate else {},
            "shadow_promotion_gap_decision": shadow_promotion_gap.get("decision", {}) if shadow_promotion_gap else {},
            "shadow_evidence_ledger_summary": shadow_evidence_ledger.get("summary", {}) if shadow_evidence_ledger else {},
            "source_caveat_audit_status": source_caveat_audit.get("status") if source_caveat_audit else None,
        },
        "concurrent_shared_ingress_evidence": {
            "status": concurrency.get("status") if concurrency else None,
            "clean": concurrency_clean,
            "hard_blockers": concurrency.get("hard_blockers", []) if concurrency else [],
            "warnings": concurrency.get("warnings", []) if concurrency else [],
        },
        "capital_reuse_roi": capital.get("aggregate", {}) if capital else {},
        "public_benchmark_comparison": public_benchmark.get("comparison", {}) if public_benchmark else {},
        "review_caveats": {
            "public_benchmark_status": public_benchmark.get("status") if public_benchmark else None,
            "public_benchmark_caveats": public_target_misses,
            "public_shadow_review_compatible_via_bridge": (
                public_benchmark.get("decision", {}).get("shadow_review_compatible_via_bridge")
                if public_benchmark
                else None
            ),
            "young_tiny_residual_status": young_tiny_residual.get("status") if young_tiny_residual else None,
            "young_tiny_residual_caveats": young_residual_caveats,
            "young_tiny_bridge_interpretation": (
                "bridge/per-window residual gates supersede this local diagnostic for shadow packet readiness"
                if bridge_shadow_ready and young_residual_caveats
                else None
            ),
        },
        "next_profile": profile_body,
        "decision": {
            "research_only": True,
            "shadow_review_ready": shadow_ready,
            "bridge_shadow_review_ready": bridge_shadow_ready,
            "legacy_shadow_review_ready": legacy_shadow_ready,
            "legacy_runtime_repeat_blockers_explained_by_bridge": bridge_shadow_ready and not legacy_shadow_ready,
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
        metric_line("shadow_review_ready_source", summary["shadow_review_ready_source"]),
        metric_line("legacy_shadow_review_ready", legacy_shadow_ready),
        metric_line("bridge_shadow_review_ready", bridge_shadow_ready),
        metric_line("deployable", False),
        metric_line("remote_runner_allowed", False),
        metric_line("hard_blockers", ", ".join(hard_blockers) if hard_blockers else "none"),
        metric_line(
            "legacy_repeat_blockers_preserved",
            ", ".join(legacy_repeat_blockers) if legacy_repeat_blockers else "none",
        ),
        "",
        "## Bridge-Aware Shadow Evidence",
        metric_line("surplus_bridge_status", surplus_bridge.get("status") if surplus_bridge else None),
        metric_line("bridge_shadow_gap_status", bridge_shadow_gap.get("status") if bridge_shadow_gap else None),
        metric_line("shadow_promotion_gate_status", shadow_promotion_gate.get("status") if shadow_promotion_gate else None),
        metric_line("shadow_promotion_gap_status", shadow_promotion_gap.get("status") if shadow_promotion_gap else None),
        metric_line("shadow_evidence_ledger_status", shadow_evidence_ledger.get("status") if shadow_evidence_ledger else None),
        metric_line("source_caveat_audit_status", source_caveat_audit.get("status") if source_caveat_audit else None),
        metric_line("bridge_hard_blockers", ", ".join(bridge_hard_blockers) if bridge_hard_blockers else "none"),
        metric_line("bridge_accepted_actions", surplus_bridge.get("aggregate", {}).get("accepted_actions") if surplus_bridge else None),
        metric_line("bridge_fills", surplus_bridge.get("aggregate", {}).get("queue_supported_fills") if surplus_bridge else None),
        metric_line("bridge_strict_rescue_closes", surplus_bridge.get("aggregate", {}).get("strict_rescue_closes") if surplus_bridge else None),
        metric_line("bridge_pair_pnl", surplus_bridge.get("aggregate", {}).get("pair_pnl") if surplus_bridge else None),
        metric_line("bridge_residual_qty_share", surplus_bridge.get("aggregate", {}).get("residual_qty_share") if surplus_bridge else None),
        metric_line("bridge_residual_cost_share", surplus_bridge.get("aggregate", {}).get("residual_cost_share") if surplus_bridge else None),
        "",
        "## Capital-Reuse ROI",
        metric_line("status", capital.get("status") if capital else None),
        metric_line(
            "fee_accounting",
            capital.get("assumptions", {}).get("fee_accounting") if capital else None,
        ),
        metric_line(
            "taker_fee",
            capital.get("aggregate", {}).get("totals", {}).get("taker_fee") if capital else None,
        ),
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
        "## Public Benchmark Comparison",
        metric_line("status", public_benchmark.get("status") if public_benchmark else None),
        metric_line(
            "actual_pair_cost_after_fee",
            public_benchmark.get("comparison", {}).get("actual_pair_cost_after_fee") if public_benchmark else None,
        ),
        metric_line(
            "b55_actual_pair_cost",
            public_benchmark.get("comparison", {}).get("vs_b55", {}).get("b55_actual_pair_cost") if public_benchmark else None,
        ),
        metric_line(
            "pair_cost_delta_vs_b55",
            public_benchmark.get("comparison", {}).get("vs_b55", {}).get("pair_cost_delta_vs_b55") if public_benchmark else None,
        ),
        metric_line(
            "residual_qty_share_delta_vs_b55",
            public_benchmark.get("comparison", {}).get("vs_b55", {}).get("residual_qty_share_delta_vs_b55") if public_benchmark else None,
        ),
        metric_line(
            "residual_qty_share_delta_vs_ce25",
            public_benchmark.get("comparison", {}).get("vs_ce25", {}).get("residual_qty_share_delta_vs_ce25") if public_benchmark else None,
        ),
        metric_line(
            "public_benchmark_caveats",
            ", ".join(public_target_misses) if public_target_misses else "none",
        ),
        metric_line(
            "shadow_review_compatible_via_bridge",
            public_benchmark.get("decision", {}).get("shadow_review_compatible_via_bridge") if public_benchmark else None,
        ),
        "",
        "## Residual Diagnostic Caveats",
        metric_line("young_tiny_residual_status", young_tiny_residual.get("status") if young_tiny_residual else None),
        metric_line(
            "young_tiny_residual_caveats",
            ", ".join(young_residual_caveats) if young_residual_caveats else "none",
        ),
        metric_line(
            "bridge_interpretation",
            "bridge/per-window residual gates explain local residual diagnostic caveats"
            if bridge_shadow_ready and young_residual_caveats
            else None,
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
    parser.add_argument("--public-benchmark-comparison-scorecard", default=None)
    parser.add_argument("--young-tiny-residual-scorecard", default=None)
    parser.add_argument("--surplus-bridge-scorecard", default=None)
    parser.add_argument("--bridge-shadow-gap-scorecard", default=None)
    parser.add_argument("--shadow-promotion-gate-scorecard", default=None)
    parser.add_argument("--shadow-promotion-gap-scorecard", default=None)
    parser.add_argument("--shadow-evidence-ledger-scorecard", default=None)
    parser.add_argument("--source-caveat-audit-scorecard", default=None)
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
