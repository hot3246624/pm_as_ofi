#!/usr/bin/env python3
"""Source-blip-aware density interpretation for the capped cap25 result.

This is a research-only interpretation layer. It can mark a completed cap25
sample as locally reviewable when the only density blocker is a rejected source
blip already absorbed by the source caveat audit. It does not change runner
admission, authorize another remote run, expand capacity, deploy, or send orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


RUN_TAG = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"
DEFAULT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_capped_diagnostics_result_20260526T1757Z.json")
DEFAULT_RECONCILIATION = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_source_blip_density_reconciliation_20260526T1832Z.json"
)
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_density_preflight_gate.json")
DEFAULT_PREFIX = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_density_prefix_scorer.json")
DEFAULT_SOURCE = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_source_caveat_audit.json")
DEFAULT_POSTRUN = Path(f".tmp_xuan/scorecards/{RUN_TAG}_postrun_bundle.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_source_blip_aware_density_gate_20260526T1840Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_source_blip_aware_density_gate_20260526T1840Z/DENSITY_GATE.md"
)

SOURCE_ONLY_BLOCKERS = {
    "source_blocks_above_max",
    "no_prefix_passed_density_gate_after_min_decision_minutes",
}


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def gte(value: Any, threshold: Any) -> bool:
    actual = fnum(value)
    target = fnum(threshold)
    return actual is not None and target is not None and actual >= target


def lte(value: Any, threshold: Any) -> bool:
    actual = fnum(value)
    target = fnum(threshold)
    return actual is not None and target is not None and actual <= target


def non_source_density_checks(observed: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, bool]:
    return {
        "candidate_rows": gte(observed.get("candidate_rows"), thresholds.get("min_candidates")),
        "accepted_actions": gte(observed.get("accepted_actions"), thresholds.get("min_accepted_actions")),
        "queue_supported_fills": gte(observed.get("queue_supported_fills"), thresholds.get("min_fills")),
        "strict_rescue_or_salvage_rows": gte(
            observed.get("strict_rescue_or_salvage_rows"), thresholds.get("min_rescue_closes")
        ),
        "fill_rate": gte(observed.get("fill_rate"), thresholds.get("min_fill_rate")),
        "rescue_per_candidate": gte(
            observed.get("rescue_per_candidate"), thresholds.get("min_rescue_per_candidate")
        ),
        "rescue_per_fill": gte(observed.get("rescue_per_fill"), thresholds.get("min_rescue_per_fill")),
        "pair_pnl": gte(observed.get("pair_pnl"), thresholds.get("min_pair_pnl")),
        "residual_qty_share": lte(observed.get("residual_qty_share"), thresholds.get("max_residual_qty_share")),
        "residual_cost_share": lte(observed.get("residual_cost_share"), thresholds.get("max_residual_cost_share")),
        "accepted_l1_age_ms_max": lte(
            observed.get("accepted_l1_age_ms_max"), thresholds.get("max_accepted_l1_age_ms")
        ),
        "rescue_l1_age_ms_max": lte(observed.get("rescue_l1_age_ms_max"), thresholds.get("max_rescue_l1_age_ms")),
        "bad_event_json": (fnum(observed.get("bad_event_json"), 0.0) or 0.0) == 0.0,
        "strict_rescue_source_blocks": (fnum(observed.get("strict_rescue_source_blocks"), 0.0) or 0.0) == 0.0,
    }


def source_absorption_checks(source: dict[str, Any], reconciliation: dict[str, Any]) -> dict[str, bool]:
    source_decision = body(source, "decision")
    source_observed = body(source, "observed")
    source_thresholds = body(source, "thresholds")
    recon_decision = body(reconciliation, "decision")
    allowed_reasons = set(str(item) for item in listify(source_thresholds.get("allowed_source_block_reasons")))
    observed_reasons = set(str(item) for item in body(source_observed, "source_block_reasons").keys())
    source_rows = fnum(source_observed.get("source_block_rows"), 0.0) or 0.0
    return {
        "reconciliation_keep": is_keep(reconciliation),
        "reconciliation_ready": bool(recon_decision.get("source_blip_reconciliation_ready")),
        "source_audit_keep": is_keep(source),
        "source_caveat_absorbed": bool(source_decision.get("source_caveat_absorbed")),
        "no_accepted_or_rescue_source_contamination": not bool(
            source_decision.get("accepted_or_rescue_source_contamination")
        ),
        "source_block_rows_positive": source_rows > 0.0,
        "source_block_rows_within_absorbed_max": source_rows
        <= (fnum(source_thresholds.get("max_absorbed_source_blocks"), 0.0) or 0.0),
        "source_block_l1_age_within_absorbed_max": (
            fnum(source_observed.get("source_block_l1_age_ms_max"), 0.0) or 0.0
        )
        <= (fnum(source_thresholds.get("max_absorbed_source_block_l1_age_ms"), 0.0) or 0.0),
        "source_block_reasons_allowed": (not observed_reasons) or observed_reasons.issubset(allowed_reasons),
        "strict_rescue_source_blocks_zero": (fnum(source_observed.get("strict_rescue_source_blocks"), 0.0) or 0.0)
        == 0.0,
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    result = load_json(args.result_scorecard)
    reconciliation = load_json(args.reconciliation_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    source = load_json(args.source_caveat_audit_scorecard)
    postrun = load_json(args.postrun_bundle_scorecard)

    result_decision = body(result, "decision")
    observed = body(result, "observed")
    thresholds = body(observed, "density_thresholds")
    density_decision = body(density, "decision")
    prefix_decision = body(prefix, "decision")
    recon_decision = body(reconciliation, "decision")

    runtime_checks = {
        "remote_completed": bool(result_decision.get("remote_completed")),
        "remote_exit_code_zero": result_decision.get("remote_exit_code") == 0,
        "aggregate_present": not bool(result_decision.get("aggregate_missing")),
        "postrun_complete": bool(result_decision.get("postrun_complete")),
        "postrun_keep": is_keep(postrun),
        "wrapper_not_killed": not bool(result_decision.get("wrapper_killed")),
    }
    non_source_checks = non_source_density_checks(observed, thresholds)
    source_checks = source_absorption_checks(source, reconciliation)

    original_density_blockers = [str(item) for item in listify(result_decision.get("density_blockers"))]
    original_hard_blockers = [str(item) for item in listify(result_decision.get("hard_blockers"))]
    hard_blockers: list[str] = []
    for name, passed in runtime_checks.items():
        if not passed:
            hard_blockers.append(name)
    for name, passed in non_source_checks.items():
        if not passed:
            hard_blockers.append(name)
    for name, passed in source_checks.items():
        if not passed:
            hard_blockers.append(name)

    non_source_gate_pass = all(non_source_checks.values())
    source_blip_absorbed = all(source_checks.values())
    source_only_original_blockers = set(original_hard_blockers).issubset(SOURCE_ONLY_BLOCKERS) and set(
        original_density_blockers
    ).issubset(SOURCE_ONLY_BLOCKERS)
    if not source_only_original_blockers:
        hard_blockers.append("original_density_blockers_not_source_only")

    runtime_clean = all(runtime_checks.values())
    reconciliation_agrees = (
        bool(recon_decision.get("runtime_clean"))
        and bool(recon_decision.get("postrun_complete"))
        and bool(recon_decision.get("density_otherwise_clear"))
        and bool(recon_decision.get("source_blip_absorbed"))
        and not listify(recon_decision.get("hard_blockers"))
    )
    if not reconciliation_agrees:
        hard_blockers.append("reconciliation_does_not_agree")

    seen: set[str] = set()
    hard_blockers = [item for item in hard_blockers if not (item in seen or seen.add(item))]

    source_blip_aware_density_pass = not hard_blockers
    status_value = (
        "KEEP_SHADOW_REVIEW_SOURCE_BLIP_AWARE_DENSITY_GATE_PASS_RESEARCH_ONLY"
        if source_blip_aware_density_pass
        else "BLOCKED_SHADOW_REVIEW_SOURCE_BLIP_AWARE_DENSITY_GATE_LOCAL_ONLY"
    )

    card = {
        "artifact": "xuan_shadow_review_source_blip_aware_density_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_source_blip_aware_density_gate.py",
        "status": status_value,
        "inputs": {
            "result_scorecard": str(args.result_scorecard),
            "reconciliation_scorecard": str(args.reconciliation_scorecard),
            "density_preflight_scorecard": str(args.density_preflight_scorecard),
            "density_prefix_scorecard": str(args.density_prefix_scorecard),
            "source_caveat_audit_scorecard": str(args.source_caveat_audit_scorecard),
            "postrun_bundle_scorecard": str(args.postrun_bundle_scorecard),
        },
        "source_statuses": {
            "result": status(result),
            "reconciliation": status(reconciliation),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "source_caveat_audit": status(source),
            "postrun_bundle": status(postrun),
        },
        "decision": {
            "runtime_clean": runtime_clean,
            "runner_density_gate_pass": bool(result_decision.get("density_gate_pass")),
            "source_blip_aware_density_pass": source_blip_aware_density_pass,
            "non_source_density_gate_pass": non_source_gate_pass,
            "source_blip_absorbed": source_blip_absorbed,
            "source_only_original_blockers": source_only_original_blockers,
            "cap25_research_review_ready": source_blip_aware_density_pass,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "paper_shadow_only": True,
            "research_only": True,
            "same_profile_repeat_allowed": False,
            "future_remote_allowed_by_this_gate": False,
            "future_remote_requires_new_local_rationale": True,
            "cap75_remote_rationale_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "do_not_weaken_runner_source_gates": True,
            "absorbed_blips_apply_to_rejected_rows_only": True,
            "hard_blockers": hard_blockers,
            "next_action": (
                "local_only_build_cap25_research_review_packet_or_capacity_interpretation_from_source_blip_aware_gate"
                if source_blip_aware_density_pass
                else "local_only_resolve_source_blip_aware_density_gate_blockers"
            ),
        },
        "observed": {
            "accepted_actions": observed.get("accepted_actions"),
            "queue_supported_fills": observed.get("queue_supported_fills"),
            "strict_rescue_or_salvage_rows": observed.get("strict_rescue_or_salvage_rows"),
            "fill_rate": observed.get("fill_rate"),
            "rescue_per_candidate": observed.get("rescue_per_candidate"),
            "rescue_per_fill": observed.get("rescue_per_fill"),
            "pair_pnl": observed.get("pair_pnl"),
            "residual_qty_share": observed.get("residual_qty_share"),
            "residual_cost_share": observed.get("residual_cost_share"),
            "accepted_l1_age_ms_max": observed.get("accepted_l1_age_ms_max"),
            "rescue_l1_age_ms_max": observed.get("rescue_l1_age_ms_max"),
            "source_block_rows": observed.get("source_block_rows"),
            "source_block_l1_age_ms_max": observed.get("source_block_l1_age_ms_max"),
            "strict_rescue_source_blocks": observed.get("strict_rescue_source_blocks"),
            "bad_event_json": observed.get("bad_event_json"),
            "original_density_blockers": original_density_blockers,
            "original_hard_blockers": original_hard_blockers,
            "density_preflight_hard_blockers": listify(density_decision.get("hard_blockers")),
            "density_prefix_hard_blockers": listify(prefix_decision.get("hard_blockers")),
        },
        "checks": {
            "runtime": runtime_checks,
            "non_source_density": non_source_checks,
            "source_absorption": source_checks,
        },
        "interpretation": {
            "density_signal": (
                "source_blip_aware_density_clear"
                if source_blip_aware_density_pass
                else "source_blip_aware_density_blocked"
            ),
            "source_signal": (
                "rejected_source_blip_absorbed_without_accepted_or_rescue_contamination"
                if source_blip_absorbed
                else "source_blip_not_absorbed"
            ),
            "runtime_signal": "completed_cleanly_with_aggregate" if runtime_clean else "runtime_not_clean",
            "promotion_signal": "research_reviewable_not_promotion",
            "capacity_signal": "cap75_still_blocked",
            "private_truth_signal": "not_private_truth",
        },
    }
    return rounded(card)


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    observed = card["observed"]
    lines = [
        "# Xuan Source-Blip-Aware Density Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- runner_density_gate_pass: `{decision['runner_density_gate_pass']}`",
        f"- source_blip_aware_density_pass: `{decision['source_blip_aware_density_pass']}`",
        f"- cap25_research_review_ready: `{decision['cap25_research_review_ready']}`",
        f"- promotion_ready: `{decision['promotion_ready']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{decision['future_remote_allowed_by_this_gate']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Observed",
        "",
        f"- accepted/fills/rescues: `{observed['accepted_actions']}` / `{observed['queue_supported_fills']}` / `{observed['strict_rescue_or_salvage_rows']}`",
        f"- fill_rate: `{observed['fill_rate']}`",
        f"- rescue_per_candidate/rescue_per_fill: `{observed['rescue_per_candidate']}` / `{observed['rescue_per_fill']}`",
        f"- pair_pnl: `{observed['pair_pnl']}`",
        f"- residual qty/cost share: `{observed['residual_qty_share']}` / `{observed['residual_cost_share']}`",
        f"- source_block_rows/source_block_l1_age_ms_max: `{observed['source_block_rows']}` / `{observed['source_block_l1_age_ms_max']}`",
        "",
        "## Boundary",
        "",
        "- This is a local research interpretation layer only.",
        "- It does not change runner source admission or economic gates.",
        "- It does not authorize another remote run, capacity expansion, deploy, or live orders.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--reconciliation-scorecard", type=Path, default=DEFAULT_RECONCILIATION)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--density-prefix-scorecard", type=Path, default=DEFAULT_PREFIX)
    parser.add_argument("--source-caveat-audit-scorecard", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--postrun-bundle-scorecard", type=Path, default=DEFAULT_POSTRUN)
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
