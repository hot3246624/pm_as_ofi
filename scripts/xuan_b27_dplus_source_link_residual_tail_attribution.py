#!/usr/bin/env python3
"""Select the next local D+ verifier from source-link residual-tail evidence.

Inputs are the already-pulled no-order shadow aggregate and source-link score.
This script does not read events JSONL, raw/replay stores, sockets, SSH, or
candidate/replay databases. It is attribution/spec selection only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


FLOAT_TOL = 1e-9


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def sum_numbers(obj: Any) -> float:
    if isinstance(obj, dict):
        return sum(sum_numbers(value) for value in obj.values())
    if isinstance(obj, (int, float)):
        return float(obj)
    return 0.0


def pair_qty_midpoint(label: str) -> float:
    if label in {"pair_qty_zero", "pair_qty_unknown"}:
        return 0.0
    if label == "pair_qty_le_1":
        return 0.5
    if label == "pair_qty_1_2":
        return 1.5
    if label == "pair_qty_2_5":
        return 3.5
    if label == "pair_qty_eq_5":
        return 5.0
    if label == "pair_qty_gt_5":
        return 6.0
    return 0.0


def nested_pair_qty_proxy(bucket: dict[str, Any]) -> float:
    total = 0.0
    for label, count in bucket.items():
        if isinstance(count, (int, float)):
            total += pair_qty_midpoint(str(label)) * float(count)
    return round(total, 6)


def get_number(mapping: dict[str, Any], key: str) -> float:
    value = mapping.get(key, 0.0)
    return float(value) if isinstance(value, (int, float)) else 0.0


def build_cross_rows(diag: dict[str, Any]) -> list[dict[str, Any]]:
    admitted = diag.get("transition_count_by_status_side_offset_risk_direction", {}).get("admitted", {})
    pair_actions = diag.get("immediate_pair_action_count_by_source_side_offset_risk_direction", {})
    pair_qty_buckets = diag.get("immediate_pair_qty_bucket_by_source_side_offset_risk_direction", {})
    pair_cost_buckets = diag.get("immediate_pair_cost_bucket_by_source_side_offset_risk_direction", {})
    residual_qty = diag.get("residual_qty_by_source_side_offset_risk_direction", {})
    residual_cost = diag.get("residual_cost_by_source_side_offset_risk_direction", {})
    residual_count = diag.get("residual_count_by_source_side_offset_risk_direction", {})
    keys = sorted(set(admitted) | set(pair_actions) | set(pair_qty_buckets) | set(residual_qty) | set(residual_cost))
    total_admitted = sum_numbers(admitted)
    total_pair_qty_proxy = sum(
        nested_pair_qty_proxy(bucket) for bucket in pair_qty_buckets.values() if isinstance(bucket, dict)
    )
    total_residual_cost = sum_numbers(residual_cost)
    total_residual_qty = sum_numbers(residual_qty)
    rows: list[dict[str, Any]] = []
    for key in keys:
        pair_qty_proxy = nested_pair_qty_proxy(pair_qty_buckets.get(key, {})) if isinstance(pair_qty_buckets.get(key, {}), dict) else 0.0
        resid_cost = get_number(residual_cost, key)
        resid_qty = get_number(residual_qty, key)
        admitted_count = get_number(admitted, key)
        parts = key.split("|")
        side = parts[0] if len(parts) > 0 else ""
        offset_bucket = parts[1] if len(parts) > 1 else ""
        risk_direction = parts[2] if len(parts) > 2 else ""
        rows.append(
            {
                "source_side_offset_risk_direction": key,
                "side": side,
                "offset_bucket": offset_bucket,
                "risk_direction": risk_direction,
                "admitted_transitions": round(admitted_count, 6),
                "admitted_transition_share": round(admitted_count / total_admitted, 6) if total_admitted else 0.0,
                "immediate_pair_actions": round(get_number(pair_actions, key), 6),
                "immediate_pair_qty_proxy": pair_qty_proxy,
                "immediate_pair_qty_proxy_share": round(pair_qty_proxy / total_pair_qty_proxy, 6)
                if total_pair_qty_proxy
                else 0.0,
                "immediate_pair_cost_buckets": pair_cost_buckets.get(key, {}),
                "residual_qty": round(resid_qty, 6),
                "residual_qty_share": round(resid_qty / total_residual_qty, 6) if total_residual_qty else 0.0,
                "residual_cost": round(resid_cost, 6),
                "residual_cost_share": round(resid_cost / total_residual_cost, 6) if total_residual_cost else 0.0,
                "residual_count": round(get_number(residual_count, key), 6),
            }
        )
    rows.sort(key=lambda row: (-row["residual_cost"], -row["residual_qty"], row["source_side_offset_risk_direction"]))
    return rows


def aggregate_group(rows: list[dict[str, Any]], *, side: str | None = None, risk_direction: str | None = None) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if (side is None or row["side"] == side)
        and (risk_direction is None or row["risk_direction"] == risk_direction)
        and (row["residual_cost"] > FLOAT_TOL or row["residual_qty"] > FLOAT_TOL or row["admitted_transitions"] > FLOAT_TOL)
    ]
    out = {
        "selector": {
            "side": side or "ANY",
            "risk_direction": risk_direction or "ANY",
        },
        "row_count": len(selected),
        "keys": [row["source_side_offset_risk_direction"] for row in selected],
        "admitted_transitions": round(sum(row["admitted_transitions"] for row in selected), 6),
        "immediate_pair_actions": round(sum(row["immediate_pair_actions"] for row in selected), 6),
        "immediate_pair_qty_proxy": round(sum(row["immediate_pair_qty_proxy"] for row in selected), 6),
        "residual_qty": round(sum(row["residual_qty"] for row in selected), 6),
        "residual_cost": round(sum(row["residual_cost"] for row in selected), 6),
        "rows": selected,
    }
    return out


def projection(metrics: dict[str, Any], group: dict[str, Any], total_pair_qty_proxy: float) -> dict[str, Any]:
    candidates = float(metrics.get("candidates", 0.0) or 0.0)
    filled_qty = float(metrics.get("filled_qty", 0.0) or 0.0)
    filled_cost = float(metrics.get("filled_cost", 0.0) or 0.0)
    residual_qty = float(metrics.get("residual_qty", 0.0) or 0.0)
    residual_cost = float(metrics.get("residual_cost", 0.0) or 0.0)
    admitted = float(group["admitted_transitions"])
    pair_qty_proxy = float(group["immediate_pair_qty_proxy"])
    projected_residual_qty = max(0.0, residual_qty - float(group["residual_qty"]))
    projected_residual_cost = max(0.0, residual_cost - float(group["residual_cost"]))
    return {
        "removed_or_capped_transition_count_proxy": round(admitted, 6),
        "candidate_retention_if_blocked_proxy": round((candidates - admitted) / candidates, 6) if candidates else None,
        "pair_qty_proxy_retention_if_blocked_proxy": round((total_pair_qty_proxy - pair_qty_proxy) / total_pair_qty_proxy, 6)
        if total_pair_qty_proxy
        else None,
        "projected_residual_qty_share_if_tail_removed": round(projected_residual_qty / filled_qty, 8)
        if filled_qty
        else None,
        "projected_residual_cost_share_if_tail_removed": round(projected_residual_cost / filled_cost, 8)
        if filled_cost
        else None,
        "note": (
            "Projection is attribution only. The selected verifier must rerun the state machine with a pre-trade "
            "inventory rule; this script does not authorize summary-only removal."
        ),
    }


def select_next(aggregate: dict[str, Any], source_score: dict[str, Any]) -> dict[str, Any]:
    event_lite = aggregate.get("event_lite", {})
    diag = event_lite.get("source_link_transition_diagnostics", {})
    if not isinstance(diag, dict):
        raise ValueError("aggregate has no event_lite.source_link_transition_diagnostics object")
    metrics = aggregate.get("metrics", {})
    rows = build_cross_rows(diag)
    total_pair_qty_proxy = round(sum(row["immediate_pair_qty_proxy"] for row in rows), 6)
    total_residual_cost = round(sum(row["residual_cost"] for row in rows), 6)
    total_residual_qty = round(sum(row["residual_qty"] for row in rows), 6)
    yes_risk = aggregate_group(rows, side="YES", risk_direction="risk_increasing")
    yes_all = aggregate_group(rows, side="YES")
    risk_all = aggregate_group(rows, risk_direction="risk_increasing")
    residual_rows = [row for row in rows if row["residual_cost"] > FLOAT_TOL or row["residual_qty"] > FLOAT_TOL]
    residual_rows.sort(key=lambda row: (-row["residual_cost"], -row["residual_qty"], row["source_side_offset_risk_direction"]))

    source_parity_ok = source_score.get("aggregate_parity", {}).get("passed") is True
    missing_cross = source_score.get("guardrails", {}).get("missing_cross_bucket_fields", [])
    selected = {
        "name": "risk_increasing_pair_fill_cap_v1",
        "decision": "KEEP_LOCAL_VERIFIER_SELECTED",
        "mechanism_family": "inventory_path_non_price_qty_cap",
        "why_this_not_summary_removal": (
            "The verifier should cap live seed quantity before admission using current opposite residual inventory. "
            "It must not remove completed summaries or discover rows from replay_store_v2."
        ),
        "rule_sketch": [
            "Use late_repair90 as control.",
            "When a candidate seed is risk_increasing (same_qty >= opp_qty before seed), cap accepted seed qty to current opposite residual qty.",
            "If the capped qty is dust/zero, block as no opposite residual to pair.",
            "Leave underweight repair and pairing-improving seeds on the existing path.",
            "Report qty_reduction, blocks, seed_actions, pair_qty, residual_qty/cost rates, weighted_pair_cost, p90 proxy if available, stress100, worst-day, research_ranking and promotion_gate separately.",
        ],
        "sample_policy": {
            "local_only": True,
            "candidate_pipeline_source": "candidate_base/candidate_registry under POLY_BT_ROOT only",
            "allowed_days": [
                "2026-05-02",
                "2026-05-03",
                "2026-05-04",
                "2026-05-05",
                "2026-05-06",
                "2026-05-07",
                "2026-05-08",
                "2026-05-09",
                "2026-05-10",
                "2026-05-11",
                "2026-05-12",
                "2026-05-13",
                "2026-05-16",
                "2026-05-17",
                "2026-05-18",
            ],
            "forbidden_days": ["2026-05-14", "2026-05-15", "2026-05-19"],
        },
        "pass_criteria_for_future_verifier": {
            "seed_action_retention_vs_late_repair90_min": 0.75,
            "pair_qty_retention_vs_late_repair90_min": 0.85,
            "residual_qty_rate_must_improve": True,
            "residual_cost_rate_must_improve": True,
            "stress100_worst_pnl_must_not_decline_materially": True,
            "promotion_gate_passed": False,
            "private_truth_ready": False,
            "deployable": False,
        },
    }
    blockers: list[str] = []
    if not source_parity_ok:
        blockers.append("source_link_aggregate_parity_not_passed")
    if missing_cross:
        blockers.append("source_link_cross_bucket_fields_missing")
    if yes_risk["residual_cost"] <= 0:
        blockers.append("yes_risk_increasing_residual_tail_absent")
    decision_label = (
        "KEEP_SOURCE_LINK_RESIDUAL_TAIL_LOCAL_VERIFIER_SELECTED"
        if not blockers
        else "UNKNOWN_SOURCE_LINK_RESIDUAL_TAIL_FIELDS_OR_SIGNAL_INSUFFICIENT"
    )
    return {
        "artifact": "xuan_b27_dplus_source_link_residual_tail_attribution",
        "decision": "KEEP" if not blockers else "UNKNOWN",
        "decision_label": decision_label,
        "blockers": blockers,
        "input_summary": {
            "source_link_score_status": source_score.get("status"),
            "source_link_aggregate_parity_passed": source_parity_ok,
            "missing_cross_bucket_fields": missing_cross,
            "shadow_acceptance_status_context": "FAIL_SHADOW_TRADING_SAMPLE_SIZE",
            "metrics": {
                "candidates": metrics.get("candidates"),
                "pair_pnl": metrics.get("pair_pnl"),
                "net_pair_cost_p90": metrics.get("net_pair_cost_p90"),
                "residual_qty": metrics.get("residual_qty"),
                "residual_cost": metrics.get("residual_cost"),
                "filled_qty": metrics.get("filled_qty"),
                "filled_cost": metrics.get("filled_cost"),
            },
        },
        "residual_tail_totals_from_source_link": {
            "total_pair_qty_proxy": total_pair_qty_proxy,
            "total_residual_qty": total_residual_qty,
            "total_residual_cost": total_residual_cost,
        },
        "top_residual_source_cross_buckets": residual_rows[:10],
        "group_attribution": {
            "yes_risk_increasing": {
                **yes_risk,
                "projection": projection(metrics, yes_risk, total_pair_qty_proxy),
            },
            "yes_all": yes_all,
            "risk_increasing_all_sides": risk_all,
        },
        "selected_next_local_verifier": selected if not blockers else None,
        "research_ranking": {
            "label": decision_label,
            "decision": "KEEP" if not blockers else "UNKNOWN",
            "interpretation": (
                "Source-link attribution selects one local candidate-pipeline verifier. "
                "It is not strategy economics, source-truth validation, private truth, or promotion evidence."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_VERIFIER_SELECTION_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh": False,
            "events_jsonl_read": False,
            "raw_replay_scan": False,
            "shared_ingress_modified": False,
            "orders_cancels_redeems": False,
        },
        "next_executable_action": (
            "Implement and run a local candidate-pipeline state-machine verifier for "
            "risk_increasing_pair_fill_cap_v1, using candidate_base/candidate_registry only. "
            "Do not SSH or start another shadow until that verifier returns KEEP."
            if not blockers
            else "Name missing fields and do not proceed to shadow."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate", required=True, help="Pulled aggregate_report.json")
    parser.add_argument("--source-link-score", required=True, help="Source-link scorer output JSON")
    parser.add_argument("--output", required=True, help="Output manifest JSON")
    args = parser.parse_args()

    report = select_next(load_json(Path(args.aggregate)), load_json(Path(args.source_link_score)))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
