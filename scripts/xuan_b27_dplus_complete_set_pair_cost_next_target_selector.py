#!/usr/bin/env python3
"""Select the next D+ complete-set pair-cost local verifier target.

This status-hygiene selector consumes only the local complete-set pair-cost
adapter outputs. It does not implement a trading rule, discover replay rows, or
make promotion claims. Its job is to decide whether the adapter signal names a
single non-price, sample-preserving local mechanism target.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)

REQUIRED_FIELDS = (
    "source_seed_candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "side",
    "offset_s",
    "seed_qty",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_risk_direction",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
)


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_ratio(num: float, den: float) -> float:
    return num / den if den else 0.0


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def field_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"overall": 0.0, "by_field": {field: 0.0 for field in REQUIRED_FIELDS}, "missing_fields": list(REQUIRED_FIELDS)}
    by_field = {
        field: safe_ratio(sum(1 for row in rows if row.get(field) not in (None, "")), len(rows))
        for field in REQUIRED_FIELDS
    }
    present = sum(1 for row in rows for field in REQUIRED_FIELDS if row.get(field) not in (None, ""))
    return {
        "overall": round(safe_ratio(present, len(rows) * len(REQUIRED_FIELDS)), 8),
        "by_field": {field: round(value, 8) for field, value in by_field.items()},
        "missing_fields": [field for field, value in by_field.items() if value < 1.0],
    }


def target_group(row: dict[str, Any]) -> bool:
    return (
        row.get("source_risk_direction") == "repair_or_pairing_improving"
        and as_float(row.get("offset_s")) >= 90.0
        and 0.0 < as_float(row.get("pre_seed_opp_qty")) < 1.0
    )


def offset_bucket(row: dict[str, Any]) -> str:
    offset_s = as_float(row.get("offset_s"))
    if offset_s < 30.0:
        return "offset_0_30"
    if offset_s < 60.0:
        return "offset_30_60"
    if offset_s < 90.0:
        return "offset_60_90"
    return "offset_90_120"


def pre_opp_bucket(row: dict[str, Any]) -> str:
    qty = as_float(row.get("pre_seed_opp_qty"))
    if qty <= 0.0:
        return "pre_opp_0"
    if qty < 1.0:
        return "pre_opp_0_1"
    if qty < 5.0:
        return "pre_opp_1_5"
    return "pre_opp_ge5"


def group_rows(rows: list[dict[str, Any]], key_fn) -> list[dict[str, Any]]:
    totals = totals_for(rows)
    grouped: dict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        key = key_fn(row)
        grouped[key]["row_count"] += 1
        for field in (
            "seed_qty",
            "source_pair_qty",
            "source_pair_cost",
            "source_pair_pnl",
            "source_residual_qty",
            "source_residual_cost",
        ):
            grouped[key][field] += as_float(row.get(field))
    output = []
    for group, values in grouped.items():
        seed_qty = values["seed_qty"]
        output.append(
            {
                "group": group,
                "row_count": int(values["row_count"]),
                "seed_qty": round(seed_qty, 8),
                "seed_qty_share": round(safe_ratio(seed_qty, totals["seed_qty"]), 8),
                "source_pair_qty": round(values["source_pair_qty"], 8),
                "source_pair_qty_share": round(safe_ratio(values["source_pair_qty"], totals["source_pair_qty"]), 8),
                "source_pair_cost_wavg": round(values["source_pair_cost"] / values["source_pair_qty"], 8)
                if values["source_pair_qty"]
                else None,
                "source_pair_pnl": round(values["source_pair_pnl"], 8),
                "source_pair_pnl_share": round(safe_ratio(values["source_pair_pnl"], totals["source_pair_pnl"]), 8),
                "source_residual_qty": round(values["source_residual_qty"], 8),
                "source_residual_qty_share": round(safe_ratio(values["source_residual_qty"], totals["source_residual_qty"]), 8),
                "source_residual_cost": round(values["source_residual_cost"], 8),
                "source_residual_cost_share": round(safe_ratio(values["source_residual_cost"], totals["source_residual_cost"]), 8),
                "residual_qty_rate": round(safe_ratio(values["source_residual_qty"], seed_qty), 8),
                "residual_cost_per_seed_qty": round(safe_ratio(values["source_residual_cost"], seed_qty), 8),
            }
        )
    output.sort(
        key=lambda row: (
            as_float(row["source_residual_cost_share"]),
            as_float(row["residual_cost_per_seed_qty"]),
        ),
        reverse=True,
    )
    return output


def totals_for(rows: list[dict[str, Any]]) -> defaultdict[str, float]:
    totals: defaultdict[str, float] = defaultdict(float)
    for row in rows:
        for field in (
            "seed_qty",
            "source_pair_qty",
            "source_pair_cost",
            "source_pair_pnl",
            "source_residual_qty",
            "source_residual_cost",
        ):
            totals[field] += as_float(row.get(field))
        totals["row_count"] += 1
    return totals


def summarize_target(rows: list[dict[str, Any]]) -> dict[str, Any]:
    totals = totals_for(rows)
    target = [row for row in rows if target_group(row)]
    target_totals = totals_for(target)
    return {
        "name": "late_repair_dust_opp_repair_or_pairing_improving",
        "predicate": {
            "source_risk_direction": "repair_or_pairing_improving",
            "offset_s_min_inclusive": 90.0,
            "pre_seed_opp_qty_min_exclusive": 0.0,
            "pre_seed_opp_qty_max_exclusive": 1.0,
            "uses_price_fields": False,
            "uses_summary_only_future_outcome": False,
        },
        "row_count": int(target_totals["row_count"]),
        "seed_qty": round(target_totals["seed_qty"], 8),
        "seed_qty_share": round(safe_ratio(target_totals["seed_qty"], totals["seed_qty"]), 8),
        "source_pair_qty": round(target_totals["source_pair_qty"], 8),
        "source_pair_qty_share": round(safe_ratio(target_totals["source_pair_qty"], totals["source_pair_qty"]), 8),
        "source_pair_pnl": round(target_totals["source_pair_pnl"], 8),
        "source_pair_pnl_share": round(safe_ratio(target_totals["source_pair_pnl"], totals["source_pair_pnl"]), 8),
        "source_residual_qty": round(target_totals["source_residual_qty"], 8),
        "source_residual_qty_share": round(safe_ratio(target_totals["source_residual_qty"], totals["source_residual_qty"]), 8),
        "source_residual_cost": round(target_totals["source_residual_cost"], 8),
        "source_residual_cost_share": round(safe_ratio(target_totals["source_residual_cost"], totals["source_residual_cost"]), 8),
        "residual_qty_rate": round(safe_ratio(target_totals["source_residual_qty"], target_totals["seed_qty"]), 8),
        "residual_cost_per_seed_qty": round(safe_ratio(target_totals["source_residual_cost"], target_totals["seed_qty"]), 8),
    }


def select_target(adapter_score: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    coverage = field_coverage(rows)
    target = summarize_target(rows)
    adapter_label = adapter_score.get("decision_label")
    blockers: list[str] = []
    if adapter_score.get("decision") != "KEEP":
        blockers.append("adapter_not_keep")
    if coverage["missing_fields"]:
        blockers.append("selected_seed_context_required_fields_missing")
    if target["source_residual_cost_share"] < 0.5:
        blockers.append("target_residual_cost_not_concentrated_enough")
    if target["seed_qty_share"] > 0.05:
        blockers.append("target_blast_radius_too_large_for_first_local_sizer")
    if target["source_pair_qty_share"] > 0.05:
        blockers.append("target_pair_qty_share_too_large_for_first_local_sizer")
    if target["row_count"] <= 0:
        blockers.append("target_group_empty")

    if blockers:
        decision = "UNKNOWN"
        label = "UNKNOWN_COMPLETE_SET_PAIR_COST_NO_SAFE_LOCAL_MECHANISM_SELECTED"
        next_target = None
    else:
        decision = "KEEP"
        label = "KEEP_COMPLETE_SET_DUST_OPP_LATE_REPAIR_SIZER_TARGET_SELECTED"
        next_target = "complete_set_dust_opp_late_repair_sizer_v1"

    return {
        "artifact": "xuan_b27_dplus_complete_set_pair_cost_next_target_selector",
        "decision": decision,
        "decision_label": label,
        "scope": "local_status_hygiene_next_target_selection",
        "inputs": {
            "adapter_decision_label": adapter_label,
            "adapter_scope": adapter_score.get("scope"),
        },
        "field_coverage": coverage,
        "target_signal": target,
        "ranked_groups": {
            "pre_opp_offset_risk": group_rows(
                rows,
                lambda row: "|".join(
                    [
                        pre_opp_bucket(row),
                        offset_bucket(row),
                        str(row.get("source_risk_direction") or "missing"),
                    ]
                ),
            )[:12],
            "pre_opp_only": group_rows(rows, pre_opp_bucket)[:8],
            "offset_only": group_rows(rows, offset_bucket)[:8],
        },
        "selected_next_target": next_target,
        "selected_mechanism_contract": {
            "name": next_target,
            "kind": "default_off_local_candidate_pipeline_quantity_sizer_verifier" if next_target else None,
            "live_observable_fields": [
                "offset_s",
                "pre_seed_same_qty",
                "pre_seed_opp_qty",
                "pre_seed_same_cost",
                "pre_seed_opp_cost",
                "ledger_proxy_before",
                "source_risk_direction",
            ],
            "candidate_rule_for_next_verifier": (
                "When a late repair_or_pairing_improving seed has 0 < pre_seed_opp_qty < dust_qty, "
                "test a dynamic quantity sizer such as qty=min(base_qty, pre_seed_opp_qty + dust_qty). "
                "This must keep the seed-action sample mostly intact while reducing residual exposure; it is "
                "not a static price cap, public-price cap, side/offset deletion, cooldown/admission cap, or "
                "summary-only removal."
            )
            if next_target
            else None,
            "fee_rate_source_policy": (
                "Use state-machine profile official_fee_rate and fee=shares*fee_rate*price*(1-price); "
                "do not hard-code account-average fee rates."
            ),
            "output_contract": {
                "score_json": "complete_set_dust_opp_late_repair_sizer_verifier_score.json",
                "must_report": [
                    "control",
                    "variant",
                    "delta",
                    "seed_action_retention",
                    "gross_buy_qty_retention",
                    "pair_qty_retention",
                    "weighted_pair_cost_delta",
                    "residual_qty_rate_reduction",
                    "residual_cost_rate_reduction",
                    "fee_after_pnl_delta",
                    "stress100_worst_pnl_delta",
                    "worst_day_fee_after_pnl_delta",
                    "research_ranking",
                    "promotion_gate",
                ],
            },
            "pass_fail_for_next_verifier": {
                "keep_if": [
                    "seed_action_retention >= 0.995",
                    "gross_buy_qty_retention >= 0.97",
                    "pair_qty_retention >= 0.98",
                    "residual_qty_rate_reduction >= 0.20",
                    "residual_cost_rate_reduction >= 0.20",
                    "weighted_pair_cost not worse by more than 1%",
                    "fee_after_pnl, stress100_worst_pnl, and worst_day_fee_after_pnl remain positive",
                    "private_truth_ready=false, deployable=false, promotion_gate.passed=false",
                ],
                "discard_if": [
                    "sample or pair_qty collapses",
                    "residual rates do not improve",
                    "stress/worst-day risk materially worsens",
                    "implementation becomes price/cap/static side-offset/summary-only/frozen mechanism",
                ],
            },
        },
        "blockers": blockers,
        "guardrails": {
            "local_only": True,
            "starts_shadow_or_canary": False,
            "uses_replay_store_for_broad_search": False,
            "uses_public_or_l1_price_cap": False,
            "uses_summary_only_removal": False,
            "claims_private_truth": False,
            "claims_deployable_or_promotion": False,
            "frozen_mechanisms_not_reused": [
                "risk_increasing_pair_fill_cap_v1",
                "ledger_tail_first_pair_priority_v1",
                "fill_to_balance90_shadow_revival",
                "portfolio_ledger_shadow_revival",
            ],
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "KEEP selects a bounded local verifier target from pre-action non-price context. It does not "
                "prove strategy economics, private truth, shadow readiness, canary readiness, or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_NEXT_TARGET_SELECTION_ONLY",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement a default-off complete_set_dust_opp_late_repair_sizer_v1 local verifier in the "
            "candidate-pipeline state machine, then run a no-network smoke and full allowed-day local score."
            if next_target
            else "Return to source-field review; do not start SSH/shadow/canary from this adapter."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter-score", type=Path, required=True)
    parser.add_argument("--selected-seed-context", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for path in (args.adapter_score, args.selected_seed_context):
        if not path.exists():
            raise SystemExit(f"missing required input: {path}")
        if not path_safe(path):
            raise SystemExit(f"unsafe input path rejected: {path}")
    if not path_safe(args.output_dir):
        raise SystemExit(f"unsafe output path rejected: {args.output_dir}")
    adapter_score = json.loads(args.adapter_score.read_text())
    rows = read_csv(args.selected_seed_context)
    report = select_target(adapter_score, rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "manifest.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(out)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
