#!/usr/bin/env python3
"""Review remaining complete-set mechanisms after dust-opposite sizer discard.

This local-only status-hygiene review determines whether the complete-set
adapter still exposes a different non-price, sample-preserving mechanism after
the exact dust-opposite late-repair sizer failed. It does not run a strategy,
query replay, start SSH/shadow, or make promotion claims.
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

TARGET_PREDICATE = {
    "source_risk_direction": "repair_or_pairing_improving",
    "offset_s_min_inclusive": 90.0,
    "pre_seed_opp_qty_min_exclusive": 0.0,
    "pre_seed_opp_qty_max_exclusive": 1.0,
}


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


def is_target(row: dict[str, Any]) -> bool:
    return (
        row.get("source_risk_direction") == "repair_or_pairing_improving"
        and as_float(row.get("offset_s")) >= 90.0
        and 0.0 < as_float(row.get("pre_seed_opp_qty")) < 1.0
    )


def totals(rows: list[dict[str, Any]]) -> defaultdict[str, float]:
    out: defaultdict[str, float] = defaultdict(float)
    for row in rows:
        out["row_count"] += 1
        for field in (
            "seed_qty",
            "source_pair_qty",
            "source_pair_pnl",
            "source_residual_qty",
            "source_residual_cost",
        ):
            out[field] += as_float(row.get(field))
    return out


def buffer_ladder(rows: list[dict[str, Any]], *, dust_qty: float = 1.0) -> list[dict[str, Any]]:
    target_rows = [row for row in rows if is_target(row)]
    base = totals(target_rows)
    output = []
    for buffer in (0.0, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25):
        seed_qty = 0.0
        pairable_qty = 0.0
        residual_qty = 0.0
        residual_cost = 0.0
        blocked_by_dust = 0
        for row in target_rows:
            original_qty = as_float(row.get("seed_qty"))
            opp_qty = as_float(row.get("pre_seed_opp_qty"))
            seed_px = as_float(row.get("seed_px"))
            capped_qty = min(original_qty, opp_qty + buffer)
            if capped_qty <= dust_qty:
                blocked_by_dust += 1
            seed_qty += capped_qty
            pair_take = min(capped_qty, opp_qty)
            pairable_qty += pair_take
            leftover = max(0.0, capped_qty - opp_qty)
            residual_qty += leftover
            residual_cost += leftover * seed_px
        output.append(
            {
                "buffer": buffer,
                "target_seed_qty_retention_proxy": round(safe_ratio(seed_qty, base["seed_qty"]), 8),
                "target_pairable_qty_retention_proxy": round(safe_ratio(pairable_qty, base["source_pair_qty"]), 8),
                "target_residual_qty_reduction_proxy": round(1.0 - safe_ratio(residual_qty, base["source_residual_qty"]), 8)
                if base["source_residual_qty"]
                else None,
                "target_residual_cost_reduction_proxy": round(
                    1.0 - safe_ratio(residual_cost, base["source_residual_cost"]), 8
                )
                if base["source_residual_cost"]
                else None,
                "blocked_by_dust_count_if_enforced": blocked_by_dust,
                "blocked_by_dust_share_if_enforced": round(safe_ratio(blocked_by_dust, base["row_count"]), 8),
            }
        )
    return output


def review(*, adapter_score: dict[str, Any], sizer_score: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    all_totals = totals(rows)
    target_rows = [row for row in rows if is_target(row)]
    target_totals = totals(target_rows)
    sizer_label = sizer_score.get("decision_label")
    sizer_metrics = ((sizer_score.get("research_ranking") or {}).get("metrics") or {})
    remaining_blockers = [
        "exact_dust_opp_plus_one_sizer_residual_reduction_below_threshold",
        "sub_dust_qty_caps_would_violate_sample_preservation_or_order_minimum_proxy",
        "stronger_buffer_sweeps_are_qty_cap_target_sweeps",
        "target_fields_do_not_identify_same_window_followup_close_or_source_sequence",
        "future_source_pair_or_residual_attribution_cannot_be_used_as_live_rule",
    ]
    return {
        "artifact": "xuan_b27_dplus_complete_set_post_sizer_gap_review",
        "decision": "UNKNOWN",
        "decision_label": "UNKNOWN_COMPLETE_SET_POST_SIZER_NO_SAFE_LOCAL_MECHANISM",
        "scope": "local_status_hygiene_post_sizer_gap_review",
        "inputs": {
            "adapter_decision_label": adapter_score.get("decision_label"),
            "sizer_decision_label": sizer_label,
        },
        "target_predicate": TARGET_PREDICATE,
        "target_signal": {
            "row_count": int(target_totals["row_count"]),
            "seed_qty": round(target_totals["seed_qty"], 8),
            "seed_qty_share": round(safe_ratio(target_totals["seed_qty"], all_totals["seed_qty"]), 8),
            "source_pair_qty": round(target_totals["source_pair_qty"], 8),
            "source_pair_qty_share": round(safe_ratio(target_totals["source_pair_qty"], all_totals["source_pair_qty"]), 8),
            "source_pair_pnl": round(target_totals["source_pair_pnl"], 8),
            "source_residual_qty": round(target_totals["source_residual_qty"], 8),
            "source_residual_qty_share": round(
                safe_ratio(target_totals["source_residual_qty"], all_totals["source_residual_qty"]), 8
            ),
            "source_residual_cost": round(target_totals["source_residual_cost"], 8),
            "source_residual_cost_share": round(
                safe_ratio(target_totals["source_residual_cost"], all_totals["source_residual_cost"]), 8
            ),
        },
        "discarded_exact_sizer": {
            "name": "complete_set_dust_opp_late_repair_sizer_v1",
            "decision_label": sizer_label,
            "residual_qty_rate_reduction": sizer_metrics.get("residual_qty_rate_reduction"),
            "residual_cost_rate_reduction": sizer_metrics.get("residual_cost_rate_reduction"),
            "seed_action_retention": sizer_metrics.get("seed_action_retention"),
            "pair_qty_retention": sizer_metrics.get("pair_qty_retention"),
            "fee_after_pnl_delta": sizer_metrics.get("fee_after_pnl_delta"),
            "stress100_worst_pnl_delta": sizer_metrics.get("stress100_worst_pnl_delta"),
        },
        "buffer_ladder_proxy": buffer_ladder(rows),
        "why_no_new_target_selected": remaining_blockers,
        "exact_missing_fields_or_capabilities": [
            "same_window_source_sequence_id for the triggering public/source event",
            "quote_intent_id/source_order_id or equivalent local candidate-to-live action join",
            "runner-observed pending/opposite order availability at the late repair decision",
            "ability to preserve sample while placing sub-dust or partial repair quantities below current dust_qty=1.0",
            "action-level follow-up close/rescue linkage after the late dust-opposite seed",
            "pre-action signal that separates residual-reducing late dust-opposite cases from still-profitable paired cases without using future source_pair/source_residual attribution",
        ],
        "rejected_next_directions": [
            "repeat exact qty=min(base_qty, pre_seed_opp_qty + dust_qty) sizer",
            "more aggressive buffer/target sweep that collapses into qty cap",
            "static side/offset deletion",
            "L1/public price cap",
            "summary-only removal",
            "cooldown/admission cap",
            "risk_increasing_pair_fill_cap_v1",
            "ledger_tail_first_pair_priority_v1",
            "fill-to-balance or portfolio-ledger shadow revival",
            "replay broad strategy search",
        ],
        "research_ranking": {
            "decision": "UNKNOWN",
            "label": "UNKNOWN_COMPLETE_SET_POST_SIZER_NO_SAFE_LOCAL_MECHANISM",
            "interpretation": (
                "Complete-set concentration remains useful, but current local pre-action fields do not support another "
                "safe sample-preserving mechanism after the exact sizer failed. This is a field/capability gap, not D+ total failure."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_GAP_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "Stop local complete-set buffer/cap variants. Choose a different research lane with explicit new fields, "
            "or wait for approved no-order/action-level data that supplies same-window source_sequence/order linkage."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter-score", type=Path, required=True)
    parser.add_argument("--selected-seed-context", type=Path, required=True)
    parser.add_argument("--sizer-score", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for path in (args.adapter_score, args.selected_seed_context, args.sizer_score):
        if not path.exists():
            raise SystemExit(f"missing required input: {path}")
        if not path_safe(path):
            raise SystemExit(f"unsafe input path rejected: {path}")
    if not path_safe(args.output_dir):
        raise SystemExit(f"unsafe output path rejected: {args.output_dir}")
    adapter_score = json.loads(args.adapter_score.read_text())
    sizer_score = json.loads(args.sizer_score.read_text())
    rows = read_csv(args.selected_seed_context)
    report = review(adapter_score=adapter_score, sizer_score=sizer_score, rows=rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "manifest.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(out)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
