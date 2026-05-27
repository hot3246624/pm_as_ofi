#!/usr/bin/env python3
"""Build an old-system tail-mark snapshot evidence profile.

This local-only artifact keeps the successful capped cap25 economic profile
unchanged and adds sparse residual_tail_mark_snapshot diagnostics. The goal is
to measure mature residual mark/recovery evidence under the old system before
more profile tuning. It does not use backtest V1, authorize remote execution,
deploy, live orders, or private-truth promotion.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0338Z"

DEFAULT_BASE_PROFILE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_capped_diagnostics_20260526T1750Z.json"
)
DEFAULT_LOT_FRONTIER = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_20260527T0325Z.json"
)
DEFAULT_MARK_GATE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_mark_recovery_gate_20260527T0305Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_tail_mark_snapshot_profile_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_tail_mark_snapshot_profile_{STAMP}/"
    "TAIL_MARK_SNAPSHOT_PROFILE.md"
)

INSTRUMENTATION_KEYS = {
    "profile_family",
    "tail_mitigation_hypothesis",
    "write_tail_mark_snapshots",
    "tail_mark_snapshot_min_age_s",
    "tail_mark_snapshot_max_per_lot",
    "tail_mark_snapshot_min_interval_s",
}


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def behavior_deltas(base: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    deltas: list[dict[str, Any]] = []
    for key in sorted(set(base) | set(candidate)):
        if key in INSTRUMENTATION_KEYS:
            continue
        if base.get(key) != candidate.get(key):
            deltas.append({"key": key, "base": base.get(key), "candidate": candidate.get(key)})
    return deltas


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    profile = card["candidate_profile"]
    frontier = card["input_frontier_read"]
    lines = [
        "# Old-System Tail Mark Snapshot Profile",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- tail_mark_snapshot_profile_ready: `{d['tail_mark_snapshot_profile_ready']}`",
        f"- behavior_profile_unchanged_except_instrumentation: `{d['behavior_profile_unchanged_except_instrumentation']}`",
        f"- tail_mark_snapshot_evidence_rationale_ready: `{d['tail_mark_snapshot_evidence_rationale_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_profile: `{d['future_remote_allowed_by_this_profile']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Candidate Profile",
        "",
        f"- target_qty/duration_s: `{profile['target_qty']}` / `{profile['duration_s']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{profile['surplus_budget_max_abs_unpaired_cost']}`",
        f"- pair_completion_net_cap: `{profile['pair_completion_net_cap']}`",
        f"- strict_rescue_surplus_net_cap: `{profile['strict_rescue_surplus_net_cap']}`",
        f"- write_tail_mark_snapshots: `{profile['write_tail_mark_snapshots']}`",
        f"- tail_mark_snapshot_min_age_s: `{profile['tail_mark_snapshot_min_age_s']}`",
        f"- tail_mark_snapshot_max_per_lot: `{profile['tail_mark_snapshot_max_per_lot']}`",
        f"- tail_mark_snapshot_min_interval_s: `{profile['tail_mark_snapshot_min_interval_s']}`",
        "",
        "## Why This Exists",
        "",
        f"- lot frontier status: `{frontier['lot_frontier_status']}`",
        f"- mature evidence ready: `{frontier['observed_mature_recovery_evidence_ready']}`",
        f"- early covered cost share: `{frontier['early_mark_covered_cost_share']}`",
        f"- mature covered cost share: `{frontier['mature_mark_covered_cost_share']}`",
        "",
        "## Boundary",
        "",
        "- This keeps economic controls unchanged and adds sparse diagnostics only.",
        "- It is a manifest input, not a launch authorization.",
        "- Do not use backtest V1, public/proxy evidence, or no-order history as private truth.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    base_card = load_json(args.base_profile_scorecard)
    lot_frontier = load_json(args.lot_frontier_scorecard)
    mark_gate = load_json(args.mark_gate_scorecard)

    base_profile = body(base_card, "profile")
    if not base_profile:
        raise SystemExit(f"{args.base_profile_scorecard} did not contain a profile object")

    candidate_profile = dict(base_profile)
    candidate_profile.update(
        {
            "profile_family": "residual_mark_recovery_evidence",
            "tail_mitigation_hypothesis": "old_system_tail_mark_snapshot_recovery_evidence",
            "write_tail_mark_snapshots": True,
            "tail_mark_snapshot_min_age_s": args.tail_mark_snapshot_min_age_s,
            "tail_mark_snapshot_max_per_lot": args.tail_mark_snapshot_max_per_lot,
            "tail_mark_snapshot_min_interval_s": args.tail_mark_snapshot_min_interval_s,
        }
    )

    lot_decision = body(lot_frontier, "decision")
    early_mark = body(body(lot_frontier, "batch_observed"), "early_mark_proxy_aggregate_no_fee")
    mature_mark = body(body(lot_frontier, "batch_observed"), "mature_actionable_mark_aggregate_no_fee")
    layer_results = body(mark_gate, "layer_results")
    economic_layer = body(layer_results, "economic_break_even_repricing")
    research_layer = body(layer_results, "research_mark_recovery_review")
    deltas = behavior_deltas(base_profile, candidate_profile)

    hard_blockers: list[str] = []
    if not is_keep(base_card):
        hard_blockers.append("base_capped_profile_not_keep")
    if "MATURE_MARK_EVIDENCE_GAP" not in status(lot_frontier):
        hard_blockers.append("lot_frontier_not_blocked_on_mature_mark_gap")
    if body(mark_gate, "decision").get("observed_recovery_evidence_ready") is not False:
        hard_blockers.append("mark_gate_unexpectedly_observed_recovery_ready")
    if deltas:
        hard_blockers.append("candidate_changed_behavior_controls")
    if fnum(candidate_profile.get("duration_s"), 0.0) > args.max_duration_s:
        hard_blockers.append("duration_exceeds_1800")
    if fnum(candidate_profile.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("profile_not_cap25")
    if candidate_profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_enabled")
    for key in (
        "source_quality_require_l1_source",
        "source_quality_require_l2_source",
        "source_quality_require_trade_source",
        "strict_rescue_require_book_source",
        "strict_rescue_require_l2_source",
    ):
        if candidate_profile.get(key) is not True:
            hard_blockers.append(f"{key}_not_true")
    if args.tail_mark_snapshot_max_per_lot <= 0:
        hard_blockers.append("tail_mark_snapshot_max_per_lot_not_positive")
    if args.tail_mark_snapshot_min_age_s < fnum(candidate_profile.get("salvage_age_s"), 0.0):
        hard_blockers.append("tail_mark_snapshot_min_age_below_salvage_age")

    ready = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_old_system_tail_mark_snapshot_profile",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_old_system_tail_mark_snapshot_profile.py",
        "status": (
            "KEEP_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_PROFILE_READY_LOCAL_ONLY"
            if ready
            else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_PROFILE_LOCAL_ONLY"
        ),
        "inputs": {
            "base_profile_scorecard": str(args.base_profile_scorecard),
            "lot_frontier_scorecard": str(args.lot_frontier_scorecard),
            "mark_gate_scorecard": str(args.mark_gate_scorecard),
            "use_backtest_v1_candidate_audit_pack": False,
        },
        "input_statuses": {
            "base_profile": status(base_card),
            "lot_frontier": status(lot_frontier),
            "mark_gate": status(mark_gate),
        },
        "candidate_profile": rounded(candidate_profile),
        "behavior_control_deltas": rounded(deltas),
        "input_frontier_read": rounded(
            {
                "lot_frontier_status": status(lot_frontier),
                "observed_mature_recovery_evidence_ready": lot_decision.get(
                    "observed_mature_recovery_evidence_ready"
                ),
                "early_mark_covered_cost_share": early_mark.get("covered_cost_share"),
                "early_mark_recovery_rate_on_total_cost": early_mark.get(
                    "mark_recovery_rate_no_fee_on_total_cost"
                ),
                "mature_mark_covered_cost_share": mature_mark.get("covered_cost_share"),
                "mature_mark_recovery_rate_on_total_cost": mature_mark.get(
                    "mark_recovery_rate_no_fee_on_total_cost"
                ),
                "research_review_recovery_rate": research_layer.get("required_recovery_rate"),
                "economic_break_even_recovery_rate": economic_layer.get("required_recovery_rate"),
                "frontier_blockers": listify(lot_decision.get("hard_blockers")),
            }
        ),
        "decision": {
            "tail_mark_snapshot_profile_ready": ready,
            "behavior_profile_unchanged_except_instrumentation": not bool(deltas),
            "tail_mark_snapshot_evidence_rationale_ready": ready,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_profile": False,
            "future_remote_requires_manifest_verifier_pre_authorization": True,
            "future_remote_requires_active_runner_conflict_check": True,
            "remote_runner_allowed": False,
            "cap75_remote_rationale_ready": False,
            "deployable": False,
            "live_orders_allowed": False,
            "private_truth_ready": False,
            "research_only": True,
            "paper_shadow_only": True,
            "hard_blockers": sorted(set(hard_blockers)),
            "next_action": (
                "build_manifest_scaffold_and_pre_authorization_for_tail_mark_snapshot_evidence_before_any_remote"
                if ready
                else "fix_profile_or_input_blockers_before_manifest"
            ),
        },
    }
    return rounded(card)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-profile-scorecard", type=Path, default=DEFAULT_BASE_PROFILE)
    parser.add_argument("--lot-frontier-scorecard", type=Path, default=DEFAULT_LOT_FRONTIER)
    parser.add_argument("--mark-gate-scorecard", type=Path, default=DEFAULT_MARK_GATE)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--tail-mark-snapshot-min-age-s", type=float, default=30.0)
    parser.add_argument("--tail-mark-snapshot-max-per-lot", type=int, default=4)
    parser.add_argument("--tail-mark-snapshot-min-interval-s", type=float, default=60.0)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--max-duration-s", type=int, default=1800)
    args = parser.parse_args()
    card = build(args)

    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card) + "\n", encoding="utf-8")

    print(json.dumps(card, indent=2, sort_keys=True))
    if card["decision"]["hard_blockers"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
