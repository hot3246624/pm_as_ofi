#!/usr/bin/env python3
"""Review whether source-link residual-tail summaries can support a new rule.

This is local-only status hygiene. It reads pulled summary/aggregate artifacts
and source-link scorer output, then names the exact fields missing before a
sample-preserving non-price mechanism can be designed. It does not read events
JSONL, raw/replay stores, sockets, SSH, or live/shared-ingress state.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_TOP_RESIDUAL_FIELDS = (
    "quote_intent_id",
    "source_order_id",
    "side",
    "qty",
    "cost",
    "px",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "trigger_source_sequence_id",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
)


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


def review(aggregate: dict[str, Any], source_score: dict[str, Any]) -> dict[str, Any]:
    event_lite = aggregate.get("event_lite", {})
    diag = event_lite.get("source_link_transition_diagnostics", {}) if isinstance(event_lite, dict) else {}
    if not isinstance(diag, dict):
        raise ValueError("aggregate has no event_lite.source_link_transition_diagnostics")
    source_sequence_presence = diag.get("source_sequence_presence_by_status", {})
    admitted_source_sequence_present = 0.0
    admitted_source_sequence_missing = 0.0
    if isinstance(source_sequence_presence, dict):
        admitted_bucket = source_sequence_presence.get("admitted", {})
        if isinstance(admitted_bucket, dict):
            admitted_source_sequence_present = sum_numbers(admitted_bucket.get("present", 0.0))
            admitted_source_sequence_missing = sum_numbers(admitted_bucket.get("missing", 0.0))

    top_residual_lots = aggregate.get("top_residual_lots", [])
    top_residual_field_presence: dict[str, bool] = {field: False for field in REQUIRED_TOP_RESIDUAL_FIELDS}
    if isinstance(top_residual_lots, list):
        for lot in top_residual_lots:
            if not isinstance(lot, dict):
                continue
            for field in REQUIRED_TOP_RESIDUAL_FIELDS:
                if field in lot and lot.get(field) not in (None, ""):
                    top_residual_field_presence[field] = True

    missing_top_residual_fields = [field for field, present in top_residual_field_presence.items() if not present]
    missing_cross_bucket_fields = source_score.get("guardrails", {}).get("missing_cross_bucket_fields", [])
    aggregate_parity_passed = source_score.get("aggregate_parity", {}).get("passed") is True
    risk_rows = source_score.get("rankings", {}).get("top_residual_source_cross_buckets", [])
    exact_missing_fields = []
    if admitted_source_sequence_present <= 0:
        exact_missing_fields.append("admitted source_sequence_id / trigger_source_sequence_id")
    exact_missing_fields.extend(f"top_residual_lots.{field}" for field in missing_top_residual_fields)
    exact_missing_fields.extend(f"source_link_cross_bucket.{field}" for field in missing_cross_bucket_fields)

    return {
        "artifact": "xuan_b27_dplus_source_link_residual_tail_field_gap_review",
        "decision": "UNKNOWN",
        "decision_label": "UNKNOWN_RESIDUAL_TAIL_ACTION_FIELDS_INSUFFICIENT_FOR_NEW_MECHANISM",
        "scope": "local_status_hygiene_summary_field_gap_review",
        "source_link_status": {
            "scorer_status": source_score.get("status"),
            "aggregate_parity_passed": aggregate_parity_passed,
            "cross_bucket_fields_present": not missing_cross_bucket_fields,
            "top_residual_cross_bucket_count": len(risk_rows) if isinstance(risk_rows, list) else None,
        },
        "field_gaps": {
            "admitted_source_sequence_present": admitted_source_sequence_present,
            "admitted_source_sequence_missing": admitted_source_sequence_missing,
            "top_residual_lot_count": len(top_residual_lots) if isinstance(top_residual_lots, list) else None,
            "top_residual_field_presence": top_residual_field_presence,
            "missing_top_residual_fields": missing_top_residual_fields,
            "missing_cross_bucket_fields": missing_cross_bucket_fields,
            "exact_missing_fields": exact_missing_fields,
        },
        "why_no_new_trade_rule_selected": [
            "Current fields identify residual-heavy buckets, but do not link an individual admitted source action to both its immediate pair contribution and final residual lot.",
            "The latest tested bucket-derived rule, risk_increasing_pair_fill_cap_v1, increased pair/PnL but worsened residual and stress, so repeating bucket-derived caps is not justified.",
            "A second rule from the same aggregate buckets would either be summary-only removal, a static side/offset cutoff, or a price/cap family variant already frozen.",
        ],
        "selected_next_target": {
            "name": "source_link_residual_tail_exemplars_v1",
            "lane": "implementability_review",
            "type": "default_off_runner_instrumentation_spec",
            "required_fields": [
                "quote_intent_id",
                "source_order_id",
                "source_sequence_id",
                "condition_id",
                "slug",
                "side",
                "offset_s",
                "source_risk_direction",
                "trigger_px",
                "trigger_size",
                "pre_seed_same_qty",
                "pre_seed_opp_qty",
                "pre_seed_same_cost",
                "pre_seed_opp_cost",
                "ledger_proxy_before",
                "ledger_proxy_after",
                "source_pair_qty",
                "source_pair_cost",
                "source_pair_pnl",
                "source_residual_qty",
                "source_residual_cost",
                "source_residual_age_ms",
            ],
            "output_contract": [
                "summary.event_lite.source_link_residual_tail_exemplars",
                "aggregate.event_lite.source_link_residual_tail_exemplars top-N merged by residual cost",
                "no events JSONL pull required",
                "default disabled; existing summary shape unchanged unless flag is enabled",
            ],
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "research_ranking": {
            "decision": "UNKNOWN",
            "label": "UNKNOWN_RESIDUAL_TAIL_ACTION_FIELDS_INSUFFICIENT_FOR_NEW_MECHANISM",
            "interpretation": "Attribution is useful, but action-level source/residual fields are missing. Build instrumentation before proposing another mechanism.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "FIELD_GAP_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
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
        "next_executable_action": "Implement local no-network default-off source_link_residual_tail_exemplars_v1 instrumentation and smoke; do not start EC2 shadow/canary.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate", required=True)
    parser.add_argument("--source-link-score", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = review(load_json(Path(args.aggregate)), load_json(Path(args.source_link_score)))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
