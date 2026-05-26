#!/usr/bin/env python3
"""Diagnose why the mature-tail cap25 profile failed locally.

The diagnosis consumes a completed no-order dry-run output root and the mature
tail result scorecard. It does not authorize a new remote run; it turns the
failed runtime into a concrete local redesign target.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_result_20260526T2148Z.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_CAPACITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_capacity_stage_public_benchmark_gate.json")
DEFAULT_EVENT_DIAG = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_event_diagnostics.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_failure_diagnosis_20260526T2148Z.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def collect_residual_lots(root: Path) -> list[dict[str, Any]]:
    lots: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.residual_fifo_lots.csv")):
        slug = path.name.replace(".residual_fifo_lots.csv", "")
        for row in read_csv_rows(path):
            cost = fnum(row.get("cost"))
            qty = fnum(row.get("qty"))
            if cost <= 0 and qty <= 0:
                continue
            lots.append(
                {
                    "slug": slug,
                    "quote_intent_id": row.get("quote_intent_id"),
                    "side": row.get("side"),
                    "qty": qty,
                    "px": fnum(row.get("px")),
                    "cost": cost,
                    "fill_ms": fnum(row.get("fill_ms")),
                    "closeability_debt": fnum(row.get("closeability_debt")),
                    "closeability_debt_per_share": fnum(row.get("closeability_debt_per_share")),
                }
            )
    return lots


def collect_action_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = load_json(manifest_path)
        action_file = root / manifest.get("files", {}).get("would_action_decisions", "")
        for row in read_csv_rows(action_file):
            row["_manifest_slug"] = manifest_path.name.replace(".normalized_lifecycle_manifest.json", "")
            rows.append(row)
    return rows


def summarize_lots(lots: list[dict[str, Any]]) -> dict[str, Any]:
    total_cost = sum(fnum(lot.get("cost")) for lot in lots)
    total_qty = sum(fnum(lot.get("qty")) for lot in lots)
    by_slug: dict[str, dict[str, Any]] = defaultdict(lambda: {"cost": 0.0, "qty": 0.0, "lot_count": 0})
    for lot in lots:
        item = by_slug[str(lot.get("slug"))]
        item["cost"] += fnum(lot.get("cost"))
        item["qty"] += fnum(lot.get("qty"))
        item["lot_count"] += 1
    top_lots = sorted(lots, key=lambda item: fnum(item.get("cost")), reverse=True)[:12]
    return {
        "total_cost": total_cost,
        "total_qty": total_qty,
        "lot_count": len(lots),
        "top_lots": top_lots,
        "top_lot_cost_share": fnum(top_lots[0].get("cost")) / total_cost if total_cost and top_lots else 0.0,
        "top_two_cost_share": sum(fnum(lot.get("cost")) for lot in top_lots[:2]) / total_cost if total_cost else 0.0,
        "by_slug": [
            {"slug": slug, **vals, "cost_share": vals["cost"] / total_cost if total_cost else 0.0}
            for slug, vals in sorted(by_slug.items(), key=lambda item: item[1]["cost"], reverse=True)
        ],
    }


def action_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    accepted = [row for row in rows if row.get("decision") == "accept"]
    blocked = [row for row in rows if row.get("decision") != "accept"]
    block_reasons = Counter(row.get("block_reason") or "unknown" for row in blocked)
    pair_completion = Counter(row.get("pair_completion_decision") or "unknown" for row in accepted)
    surplus = Counter(row.get("surplus_budget_decision") or "unknown" for row in rows)
    projected = [
        fnum(row.get("surplus_budget_projected_unpaired_cost"))
        for row in accepted
        if row.get("surplus_budget_projected_unpaired_cost") not in (None, "")
    ]
    return {
        "accepted": len(accepted),
        "blocked": len(blocked),
        "block_reasons": dict(block_reasons.most_common(10)),
        "accepted_pair_completion_decisions": dict(pair_completion.most_common()),
        "surplus_budget_decisions": dict(surplus.most_common()),
        "accepted_surplus_projected_unpaired_cost_max": max(projected) if projected else None,
        "accepted_surplus_projected_unpaired_cost_p90": sorted(projected)[int(0.9 * (len(projected) - 1))]
        if projected
        else None,
    }


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    g = card["required_reductions"]
    lines = [
        "# Xuan Mature Tail Failure Diagnosis",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- diagnosis_ready: `{d['diagnosis_ready']}`",
        f"- future_remote_allowed_by_this_diagnosis: `{d['future_remote_allowed_by_this_diagnosis']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Required Reductions",
        "",
        f"- residual_cost_reduction_for_cost_share: `{g['residual_cost_reduction_for_cost_share']}`",
        f"- residual_cost_reduction_for_cost_to_pair_qty: `{g['residual_cost_reduction_for_cost_to_pair_qty']}`",
        f"- residual_qty_reduction_for_qty_share: `{g['residual_qty_reduction_for_qty_share']}`",
        "",
        "## Residual Tail",
        "",
        f"- residual_lot_count: `{card['residual_lots']['lot_count']}`",
        f"- residual_total_cost: `{card['residual_lots']['total_cost']}`",
        f"- residual_total_qty: `{card['residual_lots']['total_qty']}`",
        f"- top_two_cost_share: `{card['residual_lots']['top_two_cost_share']}`",
        "",
        "## Interpretation",
        "",
        f"- failure_mode: `{card['interpretation']['failure_mode']}`",
        f"- next_local_target: `{card['interpretation']['next_local_target']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize another remote run.",
        "- Does not authorize capacity expansion, deploy, restart, live orders, or shared service mutation.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    result = load_json(args.result_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    capacity = load_json(args.capacity_stage_scorecard)
    event_diag = load_json(args.event_diagnostics_scorecard)
    profile = load_json(args.profile_scorecard)

    runtime_metrics = runtime.get("metrics", {})
    capacity_metrics = capacity.get("metrics", {})
    residual_cost = fnum(runtime_metrics.get("residual_cost"))
    residual_qty = fnum(runtime_metrics.get("residual_qty"))
    filled_cost = fnum(runtime_metrics.get("filled_cost"))
    filled_qty = fnum(runtime_metrics.get("filled_qty"))
    pair_qty = fnum(capacity_metrics.get("pair_qty_redeem_notional"))

    max_residual_cost_by_share = filled_cost * 0.15
    max_residual_cost_by_pair_qty = pair_qty * 0.05
    max_residual_qty = filled_qty * 0.20
    required_reductions = {
        "residual_cost_reduction_for_cost_share": max(0.0, residual_cost - max_residual_cost_by_share),
        "residual_cost_reduction_for_cost_to_pair_qty": max(0.0, residual_cost - max_residual_cost_by_pair_qty),
        "residual_qty_reduction_for_qty_share": max(0.0, residual_qty - max_residual_qty),
        "binding_residual_cost_reduction": max(
            max(0.0, residual_cost - max_residual_cost_by_share),
            max(0.0, residual_cost - max_residual_cost_by_pair_qty),
        ),
        "max_residual_cost_by_share": max_residual_cost_by_share,
        "max_residual_cost_by_pair_qty": max_residual_cost_by_pair_qty,
        "max_residual_qty": max_residual_qty,
    }

    lots = collect_residual_lots(root)
    lot_summary = summarize_lots(lots)
    actions = action_diagnostics(collect_action_rows(root))
    selected_old_qids = {
        str(lot.get("quote_intent_id"))
        for lot in profile.get("selected_tail_lots", []) or []
        if lot.get("quote_intent_id")
    }
    current_qids = {str(lot.get("quote_intent_id")) for lot in lots if lot.get("quote_intent_id")}
    selected_old_tail_absent = not bool(selected_old_qids & current_qids)

    hard_blockers = []
    if not result.get("status", "").startswith("BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT"):
        hard_blockers.append("unexpected_result_status")
    if required_reductions["binding_residual_cost_reduction"] <= 0:
        hard_blockers.append("no_binding_residual_cost_reduction_found")
    if result.get("decision", {}).get("source_review_clean") is not True:
        hard_blockers.append("source_not_clean")

    failure_mode = (
        "surplus_budget_130_did_not_generalize_to_new_window_residual_tail"
        if selected_old_tail_absent
        else "selected_tail_reappeared_or_was_not_cut"
    )
    next_local_target = (
        "design local counterfactual controls that force residual_cost <= "
        f"{min(max_residual_cost_by_share, max_residual_cost_by_pair_qty):.6f} "
        "while preserving accepted/fill/rescue density; surplus budget alone is insufficient"
    )
    card = {
        "artifact": "xuan_shadow_review_mature_tail_failure_diagnosis",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_failure_diagnosis.py",
        "status": "KEEP_SHADOW_REVIEW_MATURE_TAIL_FAILURE_DIAGNOSIS_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "result_scorecard": str(Path(args.result_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "capacity_stage_scorecard": str(Path(args.capacity_stage_scorecard).expanduser().resolve()),
            "event_diagnostics_scorecard": str(Path(args.event_diagnostics_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "result": result.get("status"),
            "runtime": runtime.get("status"),
            "capacity": capacity.get("status"),
            "profile": profile.get("status"),
        },
        "required_reductions": required_reductions,
        "residual_lots": lot_summary,
        "action_diagnostics": actions,
        "event_diagnostics": {
            "event_rows_scanned": event_diag.get("event_rows_scanned"),
            "bad_json": event_diag.get("bad_json"),
            "block_reason_counts": event_diag.get("block_reason_counts"),
            "pair_completion_block_reasons": event_diag.get("pair_completion_block_reasons"),
            "strict_rescue_block_reasons": event_diag.get("strict_rescue_block_reasons"),
            "surplus_budget_allowed": event_diag.get("surplus_budget_allowed"),
        },
        "interpretation": {
            "failure_mode": failure_mode,
            "selected_old_tail_qids_absent_from_current_residual": selected_old_tail_absent,
            "top_two_lots_do_not_cover_binding_reduction": lot_summary["top_two_cost_share"] * residual_cost
            < required_reductions["binding_residual_cost_reduction"],
            "next_local_target": next_local_target,
            "profile_redesign_hint": (
                "evaluate pair-completion/min-lot/late-rescue controls before another remote; "
                "do not relax source/economic gates and do not repeat surplus_budget_130 alone"
            ),
        },
        "decision": {
            "diagnosis_ready": True,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_diagnosis": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": "build_local_counterfactual_tail_redesign_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--capacity-stage-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAG)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
