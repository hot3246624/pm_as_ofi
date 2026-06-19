#!/usr/bin/env python3
"""Interpret the old-system tail-mark snapshot PM_DRY_RUN result.

This local-only gate combines the clean runtime/postrun bundle with the
lot-level mark/recovery frontier. It decides whether the new tail-mark evidence
actually supports residual reclassification, or whether residual remains a
capacity/repricing blocker under the old xuan no-order system.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


TAG = "xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z"
STAMP = "20260527T0407Z"

DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{TAG}_runtime_summary.json")
DEFAULT_POSTRUN = Path(f".tmp_xuan/scorecards/{TAG}_postrun_bundle.json")
DEFAULT_MARK_FRONTIER = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_{STAMP}.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_tail_mark_snapshot_result_gate_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_tail_mark_snapshot_result_gate_{STAMP}/"
    "TAIL_MARK_SNAPSHOT_RESULT_GATE.md"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


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


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    r = card["runtime_metrics"]
    m = card["mark_recovery"]
    lines = [
        "# Old-System Tail Mark Snapshot Result Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- runtime_clean: `{d['runtime_clean']}`",
        f"- tail_mark_evidence_ready: `{d['tail_mark_evidence_ready']}`",
        f"- residual_reclassification_allowed_for_research: `{d['residual_reclassification_allowed_for_research']}`",
        f"- residual_reclassification_allowed_for_capacity: `{d['residual_reclassification_allowed_for_capacity']}`",
        f"- same_profile_repeat_allowed: `{d['same_profile_repeat_allowed']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Runtime",
        "",
        f"- accepted/fills/rescues: `{r['accepted_actions']}` / `{r['queue_supported_fills']}` / `{r['strict_rescue_closes']}`",
        f"- pair_pnl / ROI: `{r['pair_pnl']}` / `{r['roi_on_filled_cost']}`",
        f"- residual cost/qty share: `{r['residual_cost_share']}` / `{r['residual_qty_share']}`",
        f"- source blocks: `{r['strict_rescue_source_blocks']}`",
        "",
        "## Mark Recovery",
        "",
        f"- early no-fee recovery on total cost: `{m['early_no_fee_recovery_rate_on_total_cost']}`",
        f"- mature no-fee recovery on total cost: `{m['mature_no_fee_recovery_rate_on_total_cost']}`",
        f"- mature after-fee recovery on total cost: `{m['mature_after_fee_recovery_rate_on_total_cost']}`",
        f"- break-even / research thresholds: `{m['economic_break_even_recovery_rate']}` / `{m['research_review_recovery_rate']}`",
        f"- mature after-fee mark value / gross capacity required reduction: `{m['mature_after_fee_mark_value']}` / `{m['gross_capacity_required_reduction']}`",
        "",
        "## Boundary",
        "",
        "- Old/current xuan no-order system only; backtest V1 is not used.",
        "- This is research-only and does not authorize cap75, deploy, live orders, or private truth.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime = load_json(args.runtime_summary)
    postrun = load_json(args.postrun_bundle)
    frontier = load_json(args.mark_frontier)

    metrics = body(runtime, "metrics")
    safety = body(runtime, "safety")
    postrun_statuses = body(postrun, "scorecard_statuses")
    frontier_decision = body(frontier, "decision")
    observed = body(frontier, "batch_observed")
    early = body(observed, "early_mark_proxy_aggregate_no_fee")
    mature_no_fee = body(observed, "mature_actionable_mark_aggregate_no_fee")
    mature_after_fee = body(observed, "mature_actionable_mark_aggregate_after_fee")
    thresholds = body(frontier, "thresholds")

    runtime_clean = (
        status(runtime).startswith("KEEP")
        and safety.get("pm_dry_run") is True
        and safety.get("orders_sent") is False
        and safety.get("shared_service_mutation") is False
        and safety.get("remote_repo_mutation") is False
    )
    postrun_complete = status(postrun).startswith("KEEP")
    tail_mark_ready = frontier_decision.get("observed_mature_recovery_evidence_ready") is True
    research_reclass = frontier_decision.get("residual_reclassification_allowed_for_research") is True
    capacity_reclass = frontier_decision.get("residual_reclassification_allowed_for_capacity") is True

    hard_blockers: list[str] = []
    if not runtime_clean:
        hard_blockers.append("runtime_not_clean")
    if not postrun_complete:
        hard_blockers.append("postrun_not_complete")
    if not tail_mark_ready:
        hard_blockers.append("tail_mark_evidence_not_ready")
    if not research_reclass:
        hard_blockers.append("mature_after_fee_recovery_below_research_threshold")
    if not capacity_reclass:
        hard_blockers.append("mature_after_fee_mark_value_below_gross_capacity_reduction")
    if fnum(metrics.get("strict_rescue_source_blocks"), 0.0) not in (0.0, None):
        hard_blockers.append("strict_rescue_source_blocks_present")

    passed = runtime_clean and postrun_complete and tail_mark_ready and research_reclass and capacity_reclass
    card = {
        "artifact": "xuan_shadow_review_old_system_tail_mark_snapshot_result_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_old_system_tail_mark_snapshot_result_gate.py",
        "status": (
            "KEEP_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_RESULT_GATE_PASS_LOCAL_ONLY"
            if passed
            else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_RESULT_RECOVERY_CAPACITY_LOCAL_ONLY"
        ),
        "inputs": {
            "runtime_summary": str(args.runtime_summary),
            "postrun_bundle": str(args.postrun_bundle),
            "mark_frontier": str(args.mark_frontier),
            "use_backtest_v1_candidate_audit_pack": False,
        },
        "input_statuses": {
            "runtime": status(runtime),
            "postrun": status(postrun),
            "mark_frontier": status(frontier),
        },
        "runtime_metrics": rounded(
            {
                "accepted_actions": fnum(metrics.get("accepted_actions")),
                "queue_supported_fills": fnum(metrics.get("queue_supported_fills")),
                "strict_rescue_closes": fnum(metrics.get("strict_rescue_closes")),
                "pair_pnl": fnum(metrics.get("pair_pnl")),
                "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
                "residual_cost": fnum(metrics.get("residual_cost")),
                "residual_qty": fnum(metrics.get("residual_qty")),
                "residual_cost_share": fnum(metrics.get("residual_cost_share")),
                "residual_qty_share": fnum(metrics.get("residual_qty_share")),
                "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
                "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
                "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks")),
            }
        ),
        "mark_recovery": rounded(
            {
                "early_no_fee_recovery_rate_on_total_cost": fnum(
                    early.get("mark_value_no_fee_recovery_rate_on_total_cost")
                ),
                "mature_no_fee_recovery_rate_on_total_cost": fnum(
                    mature_no_fee.get("mark_value_no_fee_recovery_rate_on_total_cost")
                ),
                "mature_after_fee_recovery_rate_on_total_cost": fnum(
                    mature_after_fee.get("mark_value_after_fee_recovery_rate_on_total_cost")
                ),
                "mature_after_fee_mark_value": fnum(mature_after_fee.get("mark_value_after_fee")),
                "covered_cost_share_after_fee": fnum(mature_after_fee.get("covered_cost_share")),
                "tail_mark_snapshot_rows": fnum(body(observed, "diagnostics").get("tail_mark_snapshot_rows")),
                "economic_break_even_recovery_rate": fnum(thresholds.get("economic_break_even_recovery_rate")),
                "research_review_recovery_rate": fnum(thresholds.get("research_review_recovery_rate")),
                "gross_capacity_required_reduction": fnum(thresholds.get("gross_capacity_required_reduction")),
            }
        ),
        "postrun_scorecard_statuses": postrun_statuses,
        "interpretation": {
            "read": (
                "tail-mark instrumentation solved the evidence gap, but this sample does not support residual "
                "reclassification: mature after-fee recovery is below break-even/research thresholds and below "
                "the gross capacity reduction requirement."
            ),
            "early_vs_mature_read": (
                "early no-fee recovery looked acceptable, but actionable mature after-fee recovery deteriorated; "
                "residual risk is time/fee sensitive, not merely a metric artifact."
            ),
        },
        "decision": {
            "runtime_clean": runtime_clean,
            "postrun_complete": postrun_complete,
            "tail_mark_evidence_ready": tail_mark_ready,
            "residual_reclassification_allowed_for_research": research_reclass,
            "residual_reclassification_allowed_for_capacity": capacity_reclass,
            "same_profile_repeat_allowed": False,
            "bounded_cap25_remote_rationale_ready": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_gate": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "research_only": True,
            "paper_shadow_only": True,
            "hard_blockers": sorted(set(hard_blockers)),
            "next_action": (
                "local-only design a residual age/fee aware control or repricing frontier; do not repeat "
                "tail-mark snapshot profile or run cap75"
            ),
        },
    }
    return rounded(card)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-summary", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--postrun-bundle", type=Path, default=DEFAULT_POSTRUN)
    parser.add_argument("--mark-frontier", type=Path, default=DEFAULT_MARK_FRONTIER)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()
    card = build(args)

    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card) + "\n", encoding="utf-8")

    print(json.dumps(card, indent=2, sort_keys=True))
    if not card["status"].startswith("KEEP"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
