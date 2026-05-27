#!/usr/bin/env python3
"""Build an old-system lot-level mark/recovery batch frontier.

This artifact consumes the capped cap25 local remote outputs and asks whether
the residual lots have enough observable, no-lookahead mark/recovery evidence
to clear the old-system mark recovery gate. It deliberately distinguishes:

* early observable mark proxy, which can appear before a lot is actionable;
* mature/actionable mark evidence, which is required for recovery credit;
* gross capacity exposure, which remains separate from economic repricing.

It does not use backtest V1, raw/shared remote data, or any live/private truth.
It does not authorize remote runs, deployment, live orders, or capacity
expansion.
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


STAMP = "20260527T0325Z"
RUN_CAPPED = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"

DEFAULT_MARK_GATE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_mark_recovery_gate_20260527T0305Z.json"
)
DEFAULT_REMOTE_OUTPUTS = Path(f".tmp_xuan/local_verifier_artifacts/{RUN_CAPPED}/remote_outputs")
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_{STAMP}/"
    "OLD_SYSTEM_LOT_MARK_RECOVERY_BATCH_FRONTIER.md"
)


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


def load_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    with resolved.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_residual_lots(remote_outputs: Path) -> list[dict[str, Any]]:
    lots: list[dict[str, Any]] = []
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            cost = fnum(row.get("cost"), 0.0) or 0.0
            qty = fnum(row.get("qty"), 0.0) or 0.0
            if cost <= 0.0 and qty <= 0.0:
                continue
            lots.append(
                {
                    "source_file": path.name,
                    "slug": row.get("slug"),
                    "quote_intent_id": row.get("quote_intent_id"),
                    "side": row.get("side"),
                    "qty": qty,
                    "px": fnum(row.get("px"), 0.0) or 0.0,
                    "cost": cost,
                    "fill_ms": fnum(row.get("fill_ms"), 0.0) or 0.0,
                    "closeability_debt": fnum(row.get("closeability_debt"), 0.0) or 0.0,
                }
            )
    return lots


def load_summary_diagnostics(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.summary.json")):
        card = load_json(path)
        slug = path.name.removesuffix(".summary.json")
        diag = body(card, "rescue_block_diagnostics")
        if diag:
            out[slug] = diag
    return out


def mark_value_from_close_ask(qty: float, close_ask: float | None, fee_per_share: float = 0.0) -> float | None:
    if close_ask is None:
        return None
    value_per_share = 1.0 - close_ask - fee_per_share
    return max(0.0, qty * value_per_share)


def scan_mark_events(remote_outputs: Path, lots: list[dict[str, Any]], mature_age_ms: float) -> dict[str, dict[str, Any]]:
    by_qid = {str(lot["quote_intent_id"]): lot for lot in lots}
    marks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reason_counts: dict[str, Counter[str]] = defaultdict(Counter)

    slugs = sorted({str(lot["slug"]) for lot in lots if lot.get("slug")})
    for slug in slugs:
        path = remote_outputs.expanduser().resolve() / f"{slug}.events.jsonl"
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("kind") != "strict_rescue_block":
                    continue
                qid = event.get("oldest_quote_intent_id") or event.get("quote_intent_id")
                if qid not in by_qid:
                    continue
                reason = str(event.get("block_reason") or "")
                reason_counts[qid][reason] += 1
                lot = by_qid[qid]
                qty = fnum(event.get("oldest_lot_qty"), fnum(lot.get("qty"), 0.0)) or 0.0
                held_px = fnum(event.get("oldest_lot_px"), fnum(lot.get("px"), 0.0))
                close_ask = fnum(event.get("raw_comp_ask"))
                ts_ms = fnum(event.get("ts_ms"))
                age_ms = fnum(event.get("lot_age_ms"), fnum(event.get("oldest_lot_age_ms"), 0.0)) or 0.0
                mark_value = mark_value_from_close_ask(qty, close_ask)
                cost = fnum(lot.get("cost"), 0.0) or 0.0
                marks[qid].append(
                    {
                        "ts_ms": ts_ms,
                        "age_ms": age_ms,
                        "mature_actionable": age_ms >= mature_age_ms,
                        "block_reason": reason,
                        "held_px": held_px,
                        "close_ask": close_ask,
                        "qty": qty,
                        "cost": cost,
                        "mark_value_no_fee": mark_value,
                        "mark_recovery_rate_no_fee": mark_value / cost if mark_value is not None and cost else None,
                    }
                )

    out: dict[str, dict[str, Any]] = {}
    for qid, lot in by_qid.items():
        rows = marks.get(qid, [])
        early_rows = [row for row in rows if row.get("mark_value_no_fee") is not None]
        mature_rows = [
            row
            for row in rows
            if row.get("mature_actionable") and row.get("mark_value_no_fee") is not None
        ]
        best_early = max(early_rows, key=lambda row: fnum(row.get("mark_value_no_fee"), -1.0) or -1.0) if early_rows else None
        best_mature = (
            max(mature_rows, key=lambda row: fnum(row.get("mark_value_no_fee"), -1.0) or -1.0)
            if mature_rows
            else None
        )
        out[qid] = {
            "lot": lot,
            "strict_rescue_block_rows_for_lot": len(rows),
            "block_reason_counts": dict(reason_counts.get(qid, Counter())),
            "best_early_mark_no_fee": best_early,
            "best_mature_actionable_mark_no_fee": best_mature,
            "has_any_mark_proxy": best_early is not None,
            "has_mature_actionable_mark": best_mature is not None,
        }
    return rounded(out)


def aggregate_mark(lot_marks: dict[str, dict[str, Any]], field: str) -> dict[str, Any]:
    total_cost = 0.0
    total_qty = 0.0
    total_mark = 0.0
    covered_cost = 0.0
    covered_qty = 0.0
    missing: list[str] = []
    for qid, item in lot_marks.items():
        lot = body(item, "lot")
        cost = fnum(lot.get("cost"), 0.0) or 0.0
        qty = fnum(lot.get("qty"), 0.0) or 0.0
        total_cost += cost
        total_qty += qty
        mark = body(item, field)
        mark_value = fnum(mark.get("mark_value_no_fee"))
        if mark_value is None:
            missing.append(qid)
            continue
        total_mark += mark_value
        covered_cost += cost
        covered_qty += qty
    return rounded(
        {
            "total_residual_cost": total_cost,
            "total_residual_qty": total_qty,
            "covered_cost": covered_cost,
            "covered_qty": covered_qty,
            "covered_cost_share": covered_cost / total_cost if total_cost else None,
            "covered_qty_share": covered_qty / total_qty if total_qty else None,
            "mark_value_no_fee": total_mark,
            "mark_recovery_rate_no_fee_on_total_cost": total_mark / total_cost if total_cost else None,
            "mark_recovery_rate_no_fee_on_covered_cost": total_mark / covered_cost if covered_cost else None,
            "missing_lot_qids": missing,
        }
    )


def top_lots(lot_marks: dict[str, dict[str, Any]], n: int = 8) -> list[dict[str, Any]]:
    rows = []
    for qid, item in lot_marks.items():
        lot = body(item, "lot")
        early = body(item, "best_early_mark_no_fee")
        mature = body(item, "best_mature_actionable_mark_no_fee")
        rows.append(
            {
                "quote_intent_id": qid,
                "slug": lot.get("slug"),
                "side": lot.get("side"),
                "qty": lot.get("qty"),
                "cost": lot.get("cost"),
                "strict_rescue_block_rows_for_lot": item.get("strict_rescue_block_rows_for_lot"),
                "has_any_mark_proxy": item.get("has_any_mark_proxy"),
                "has_mature_actionable_mark": item.get("has_mature_actionable_mark"),
                "best_early_recovery_rate_no_fee": early.get("mark_recovery_rate_no_fee"),
                "best_early_close_ask": early.get("close_ask"),
                "best_early_age_ms": early.get("age_ms"),
                "best_mature_recovery_rate_no_fee": mature.get("mark_recovery_rate_no_fee"),
                "best_mature_close_ask": mature.get("close_ask"),
                "block_reason_counts": item.get("block_reason_counts"),
            }
        )
    return sorted(rounded(rows), key=lambda row: fnum(row.get("cost"), 0.0) or 0.0, reverse=True)[:n]


def build(args: argparse.Namespace) -> dict[str, Any]:
    mark_gate = load_json(args.mark_recovery_gate)
    risk = body(mark_gate, "residual_risk_split")
    layer_results = body(mark_gate, "layer_results")
    research_layer = body(layer_results, "research_mark_recovery_review")
    economic_layer = body(layer_results, "economic_break_even_repricing")
    gross_layer = body(layer_results, "gross_capacity_exposure")

    lots = load_residual_lots(args.remote_outputs)
    diagnostics = load_summary_diagnostics(args.remote_outputs)
    lot_marks = scan_mark_events(args.remote_outputs, lots, args.mature_age_ms)
    early_aggregate = aggregate_mark(lot_marks, "best_early_mark_no_fee")
    mature_aggregate = aggregate_mark(lot_marks, "best_mature_actionable_mark_no_fee")

    break_even_rate = fnum(economic_layer.get("required_recovery_rate"), 1.0) or 1.0
    research_rate = fnum(research_layer.get("required_recovery_rate"), 1.0) or 1.0
    gross_reduction = fnum(gross_layer.get("binding_required_reduction"), 0.0) or 0.0

    early_recovery_rate = fnum(early_aggregate.get("mark_recovery_rate_no_fee_on_total_cost"), 0.0) or 0.0
    mature_recovery_rate = fnum(mature_aggregate.get("mark_recovery_rate_no_fee_on_total_cost"), 0.0) or 0.0
    mature_mark_value = fnum(mature_aggregate.get("mark_value_no_fee"), 0.0) or 0.0
    mature_coverage_full = not list(mature_aggregate.get("missing_lot_qids") or [])

    early_proxy_research_pass = early_recovery_rate >= research_rate
    mature_research_pass = mature_coverage_full and mature_recovery_rate >= research_rate
    economic_break_even_pass = mature_coverage_full and mature_recovery_rate >= break_even_rate
    gross_capacity_reduction_pass = mature_mark_value >= gross_reduction

    diagnostic_suppressed_total = sum(fnum(diag.get("suppressed"), 0.0) or 0.0 for diag in diagnostics.values())
    diagnostic_written_total = sum(fnum(diag.get("written"), 0.0) or 0.0 for diag in diagnostics.values())
    suppressed_by_reason: Counter[str] = Counter()
    for diag in diagnostics.values():
        for reason, count in body(diag, "suppressed_by_reason").items():
            suppressed_by_reason[str(reason)] += int(count or 0)

    blocker_reasons = []
    if not mature_coverage_full:
        blocker_reasons.append("mature_actionable_mark_missing_for_residual_lots")
    if diagnostic_suppressed_total > 0:
        blocker_reasons.append("strict_rescue_block_diagnostics_capped_before_tail_mark_evidence")
    if not mature_research_pass:
        blocker_reasons.append("observed_mature_recovery_below_research_threshold")
    if not gross_capacity_reduction_pass:
        blocker_reasons.append("gross_capacity_reduction_not_proven_by_mature_marks")

    status_value = (
        "KEEP_SHADOW_REVIEW_OLD_SYSTEM_LOT_MARK_RECOVERY_BATCH_FRONTIER_PASS_LOCAL_ONLY"
        if mature_research_pass and gross_capacity_reduction_pass
        else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_LOT_MARK_RECOVERY_BATCH_FRONTIER_MATURE_MARK_EVIDENCE_GAP_LOCAL_ONLY"
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier.py",
            "status": status_value,
            "inputs": {
                "mark_recovery_gate": str(args.mark_recovery_gate),
                "remote_outputs": str(args.remote_outputs),
                "mature_age_ms": args.mature_age_ms,
            },
            "source_statuses": {
                "mark_recovery_gate": status(mark_gate),
            },
            "thresholds": {
                "economic_break_even_recovery_rate": break_even_rate,
                "research_review_recovery_rate": research_rate,
                "gross_capacity_required_reduction": gross_reduction,
                "mature_age_ms": args.mature_age_ms,
            },
            "batch_observed": {
                "residual_lot_count": len(lots),
                "early_mark_proxy_aggregate_no_fee": early_aggregate,
                "mature_actionable_mark_aggregate_no_fee": mature_aggregate,
                "top_residual_lot_marks": top_lots(lot_marks),
                "diagnostics": {
                    "summary_files_with_rescue_block_diagnostics": len(diagnostics),
                    "strict_rescue_block_diagnostics_written": diagnostic_written_total,
                    "strict_rescue_block_diagnostics_suppressed": diagnostic_suppressed_total,
                    "suppressed_by_reason": dict(suppressed_by_reason),
                },
            },
            "interpretation": {
                "early_proxy_read": (
                    "early observable marks can be inspected but do not count as actionable recovery credit "
                    "when lots are blocked by age/min-cost rules"
                ),
                "mature_mark_read": (
                    "current capped diagnostics do not retain enough mature tail mark rows to prove recovery"
                    if not mature_coverage_full
                    else "mature tail marks are available for every residual lot"
                ),
                "capacity_read": "gross capacity remains blocked unless mature marks or actual closes cover the required reduction",
                "data_gap_read": (
                    "future local evidence should record tail mark snapshots separately from capped rescue-block spam"
                ),
            },
            "decision": {
                "old_system_lot_mark_recovery_batch_frontier_ready": True,
                "use_backtest_v1_candidate_audit_pack": False,
                "early_mark_proxy_research_pass_no_fee": early_proxy_research_pass,
                "economic_break_even_repricing_pass": economic_break_even_pass,
                "research_mark_recovery_review_pass": mature_research_pass,
                "gross_capacity_exposure_pass": gross_capacity_reduction_pass,
                "observed_mature_recovery_evidence_ready": mature_coverage_full,
                "residual_reclassification_allowed_for_research": mature_research_pass,
                "residual_reclassification_allowed_for_capacity": gross_capacity_reduction_pass,
                "bounded_cap25_remote_rationale_ready": False,
                "cap75_remote_rationale_ready": False,
                "future_remote_allowed_by_this_frontier": False,
                "future_remote_requires_tail_mark_snapshot_instrumentation": True,
                "same_profile_repeat_allowed": False,
                "promotion_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "hard_blockers": blocker_reasons,
                "next_action": (
                    "add local tail-mark snapshot scorer/instrumentation that records sparse mature marks for residual lots; "
                    "do not repeat profile tuning until mark evidence can be audited"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    thresholds = card["thresholds"]
    observed = card["batch_observed"]
    early = observed["early_mark_proxy_aggregate_no_fee"]
    mature = observed["mature_actionable_mark_aggregate_no_fee"]
    diagnostics = observed["diagnostics"]
    lines = [
        "# Xuan Old-System Lot Mark Recovery Batch Frontier",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- early_mark_proxy_research_pass_no_fee: `{d['early_mark_proxy_research_pass_no_fee']}`",
        f"- economic_break_even_repricing_pass: `{d['economic_break_even_repricing_pass']}`",
        f"- research_mark_recovery_review_pass: `{d['research_mark_recovery_review_pass']}`",
        f"- gross_capacity_exposure_pass: `{d['gross_capacity_exposure_pass']}`",
        f"- future_remote_allowed_by_this_frontier: `{d['future_remote_allowed_by_this_frontier']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Thresholds",
        "",
        f"- economic break-even recovery rate: `{thresholds['economic_break_even_recovery_rate']}`",
        f"- research review recovery rate: `{thresholds['research_review_recovery_rate']}`",
        f"- gross capacity required reduction: `{thresholds['gross_capacity_required_reduction']}`",
        f"- mature age ms: `{thresholds['mature_age_ms']}`",
        "",
        "## Batch Observed",
        "",
        f"- residual lot count: `{observed['residual_lot_count']}`",
        f"- early mark proxy recovery on total cost: `{early['mark_recovery_rate_no_fee_on_total_cost']}`",
        f"- mature actionable recovery on total cost: `{mature['mark_recovery_rate_no_fee_on_total_cost']}`",
        f"- mature missing qids: `{', '.join(mature['missing_lot_qids']) or 'none'}`",
        f"- diagnostics written/suppressed: `{diagnostics['strict_rescue_block_diagnostics_written']}` / `{diagnostics['strict_rescue_block_diagnostics_suppressed']}`",
        "",
        "## Interpretation",
        "",
        f"- {card['interpretation']['early_proxy_read']}",
        f"- {card['interpretation']['mature_mark_read']}",
        f"- {card['interpretation']['data_gap_read']}",
        "",
        "## Boundary",
        "",
        "- Old/current xuan no-order system only; no backtest V1 evidence.",
        "- Local-only and research-only; does not authorize remote, cap75/150/300, deploy, live orders, or private truth.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mark-recovery-gate", type=Path, default=DEFAULT_MARK_GATE)
    parser.add_argument("--remote-outputs", type=Path, default=DEFAULT_REMOTE_OUTPUTS)
    parser.add_argument("--mature-age-ms", type=float, default=30000.0)
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
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
