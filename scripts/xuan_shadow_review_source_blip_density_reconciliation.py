#!/usr/bin/env python3
"""Reconcile absorbed source blips against the density gate.

This is a local interpretation layer only. It does not relax admission in the
runner, authorize remote execution, expand capacity, deploy, or send orders.
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
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_density_preflight_gate.json")
DEFAULT_PREFIX = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_density_prefix_scorer.json")
DEFAULT_SOURCE = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_source_caveat_audit.json")
DEFAULT_EVENT_DIAG = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_event_diagnostics.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_source_blip_density_reconciliation_20260526T1832Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_source_blip_density_reconciliation_20260526T1832Z/RECONCILIATION.md"
)

SOURCE_PREFIX_BLOCKERS = {
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


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def density_otherwise_clear(result: dict[str, Any]) -> tuple[bool, list[str]]:
    blockers = [str(item) for item in listify(body(result, "decision").get("density_blockers"))]
    remaining = [item for item in blockers if item not in SOURCE_PREFIX_BLOCKERS]
    return (not remaining), remaining


def source_blip_absorbed(source: dict[str, Any]) -> tuple[bool, list[str]]:
    decision = body(source, "decision")
    observed = body(source, "observed")
    thresholds = body(source, "thresholds")
    blockers: list[str] = []

    if not is_keep(source):
        blockers.append("source_audit_not_keep")
    if not bool(decision.get("source_caveat_absorbed")):
        blockers.append("source_caveat_not_absorbed")
    if bool(decision.get("accepted_or_rescue_source_contamination")):
        blockers.append("accepted_or_rescue_source_contamination")

    source_rows = fnum(observed.get("source_block_rows"), 0.0) or 0.0
    max_rows = fnum(thresholds.get("max_absorbed_source_blocks"), 0.0) or 0.0
    if source_rows <= 0.0:
        blockers.append("no_source_blip_to_reconcile")
    if source_rows > max_rows:
        blockers.append("source_block_rows_above_absorbed_max")

    max_age = fnum(thresholds.get("max_absorbed_source_block_l1_age_ms"), 0.0) or 0.0
    age = fnum(observed.get("source_block_l1_age_ms_max"), 0.0) or 0.0
    if age > max_age:
        blockers.append("source_block_l1_age_above_absorbed_max")

    strict_rescue_source_blocks = fnum(observed.get("strict_rescue_source_blocks"), 0.0) or 0.0
    if strict_rescue_source_blocks > 0.0:
        blockers.append("strict_rescue_source_blocks_present")

    allowed = set(str(item) for item in listify(thresholds.get("allowed_source_block_reasons")))
    reasons = set(str(item) for item in body(observed, "source_block_reasons").keys())
    if allowed and not reasons.issubset(allowed):
        blockers.append("source_block_reason_not_allowed")

    return (not blockers), blockers


def build(args: argparse.Namespace) -> dict[str, Any]:
    result = load_json(args.result_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    source = load_json(args.source_caveat_audit_scorecard)
    event_diag = load_json(args.event_diagnostics_scorecard)

    result_decision = body(result, "decision")
    result_observed = body(result, "observed")
    source_observed = body(source, "observed")
    event_counts = body(event_diag, "event_counts")
    strict_reasons = body(event_diag, "strict_rescue_block_reasons")
    density_ok_except_source, density_remaining = density_otherwise_clear(result)
    source_ok, source_blockers = source_blip_absorbed(source)

    runtime_clean = bool(result_decision.get("remote_completed")) and not bool(result_decision.get("aggregate_missing"))
    postrun_complete = bool(result_decision.get("postrun_complete"))
    result_reconciliation_ready = runtime_clean and postrun_complete and density_ok_except_source and source_ok

    hard_blockers: list[str] = []
    if not runtime_clean:
        hard_blockers.append("runtime_not_clean")
    if not postrun_complete:
        hard_blockers.append("postrun_incomplete")
    hard_blockers.extend(density_remaining)
    hard_blockers.extend(source_blockers)
    seen: set[str] = set()
    hard_blockers = [item for item in hard_blockers if not (item in seen or seen.add(item))]

    status_value = (
        "KEEP_SHADOW_REVIEW_SOURCE_BLIP_DENSITY_RECONCILIATION_READY_LOCAL_ONLY"
        if result_reconciliation_ready
        else "BLOCKED_SHADOW_REVIEW_SOURCE_BLIP_DENSITY_RECONCILIATION_LOCAL_ONLY"
    )

    card = {
        "artifact": "xuan_shadow_review_source_blip_density_reconciliation",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_source_blip_density_reconciliation.py",
        "status": status_value,
        "inputs": {
            "result_scorecard": str(args.result_scorecard),
            "density_preflight_scorecard": str(args.density_preflight_scorecard),
            "density_prefix_scorecard": str(args.density_prefix_scorecard),
            "source_caveat_audit_scorecard": str(args.source_caveat_audit_scorecard),
            "event_diagnostics_scorecard": str(args.event_diagnostics_scorecard),
        },
        "source_statuses": {
            "result": status(result),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "source_caveat_audit": status(source),
            "event_diagnostics": status(event_diag) or "NO_STATUS_FIELD",
        },
        "decision": {
            "runtime_clean": runtime_clean,
            "postrun_complete": postrun_complete,
            "density_otherwise_clear": density_ok_except_source,
            "density_remaining_blockers_after_source_blip": density_remaining,
            "source_blip_absorbed": source_ok,
            "source_blip_blockers": source_blockers,
            "source_blip_reconciliation_ready": result_reconciliation_ready,
            "promotion_ready": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_reconciliation": False,
            "future_remote_requires_new_local_rationale": True,
            "same_profile_repeat_allowed": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "do_not_weaken_source_or_economic_gates": True,
            "hard_blockers": hard_blockers,
            "next_action": "local_only_design_source_blip_aware_prefix_or_density_gate_before_any_new_cap25_sample",
        },
        "observed": {
            "run_exit_code": result_decision.get("remote_exit_code"),
            "accepted_actions": result_observed.get("accepted_actions"),
            "queue_supported_fills": result_observed.get("queue_supported_fills"),
            "strict_rescue_or_salvage_rows": result_observed.get("strict_rescue_or_salvage_rows"),
            "fill_rate": result_observed.get("fill_rate"),
            "rescue_per_candidate": result_observed.get("rescue_per_candidate"),
            "rescue_per_fill": result_observed.get("rescue_per_fill"),
            "pair_pnl": result_observed.get("pair_pnl"),
            "residual_qty_share": result_observed.get("residual_qty_share"),
            "residual_cost_share": result_observed.get("residual_cost_share"),
            "source_block_rows": source_observed.get("source_block_rows"),
            "source_block_l1_age_ms_max": source_observed.get("source_block_l1_age_ms_max"),
            "source_block_reasons": body(source_observed, "source_block_reasons"),
            "strict_rescue_source_blocks": source_observed.get("strict_rescue_source_blocks"),
            "pair_completion_blocks": event_counts.get("pair_completion_block"),
            "surplus_budget_blocks": event_counts.get("surplus_budget_block"),
            "strict_rescue_block_reasons": strict_reasons,
        },
        "interpretation": {
            "runtime_signal": "completed_cleanly_with_aggregate" if runtime_clean else "runtime_not_clean",
            "density_signal": (
                "all_non_source_density_thresholds_clear"
                if density_ok_except_source
                else "non_source_density_thresholds_still_blocked"
            ),
            "source_signal": "single_rejected_source_blip_absorbed" if source_ok else "source_blip_not_absorbed",
            "promotion_signal": "not_promotion_evidence_local_interpretation_only",
            "do_not_expand_capacity": True,
            "do_not_repeat_same_profile": True,
        },
    }
    return rounded(card)


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    observed = card["observed"]
    lines = [
        "# Xuan Source-Blip Density Reconciliation",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- runtime_clean: `{decision['runtime_clean']}`",
        f"- density_otherwise_clear: `{decision['density_otherwise_clear']}`",
        f"- source_blip_absorbed: `{decision['source_blip_absorbed']}`",
        f"- promotion_ready: `{decision['promotion_ready']}`",
        f"- future_remote_allowed_by_this_reconciliation: `{decision['future_remote_allowed_by_this_reconciliation']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Observed",
        "",
        f"- accepted/fills/rescues: `{observed['accepted_actions']}` / `{observed['queue_supported_fills']}` / `{observed['strict_rescue_or_salvage_rows']}`",
        f"- rescue_per_candidate/rescue_per_fill: `{observed['rescue_per_candidate']}` / `{observed['rescue_per_fill']}`",
        f"- pair_pnl: `{observed['pair_pnl']}`",
        f"- residual qty/cost share: `{observed['residual_qty_share']}` / `{observed['residual_cost_share']}`",
        f"- source_block_rows/source_block_l1_age_ms_max: `{observed['source_block_rows']}` / `{observed['source_block_l1_age_ms_max']}`",
        "",
        "## Boundary",
        "",
        "- Local-only interpretation. This does not relax runner source admission.",
        "- No cap75/150/300 remote rationale, no deploy, and no live orders.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--density-prefix-scorecard", type=Path, default=DEFAULT_PREFIX)
    parser.add_argument("--source-caveat-audit-scorecard", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAG)
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
