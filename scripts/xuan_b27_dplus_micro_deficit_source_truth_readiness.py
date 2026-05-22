#!/usr/bin/env python3
"""Select and score micro-deficit repair guard source-truth readiness.

This script only reads local candidate_seed_outcome_separator_export_v1 rows and
optionally an existing replay_store_v2 selected-action response. It does not
query replay_store_v2 by itself, read events JSONL, scan raw/replay stores,
connect to shared-ingress, or touch live trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALIDATED_STATUS = "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY"
FORBIDDEN_DAYS = {"2026-05-14", "2026-05-15", "2026-05-19"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, default))


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def is_micro_deficit_target(row: dict[str, Any], max_deficit_qty: float, open_qty_cap: float) -> bool:
    same_qty = as_float(row.get("pre_seed_same_qty"))
    opp_qty = as_float(row.get("pre_seed_opp_qty"))
    return (
        row.get("source_risk_direction") == "repair_or_pairing_improving"
        and 0.0 < opp_qty - same_qty <= max_deficit_qty + 1e-12
        and same_qty + opp_qty <= open_qty_cap + 1e-12
    )


def summarize_targets(rows: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> dict[str, Any]:
    def totals(selected: list[dict[str, Any]]) -> dict[str, float]:
        return {
            "rows": float(len(selected)),
            "seed_qty": sum(as_float(row.get("seed_qty")) for row in selected),
            "pair_qty": sum(as_float(row.get("source_pair_qty")) for row in selected),
            "pair_pnl": sum(as_float(row.get("source_pair_pnl")) for row in selected),
            "residual_qty": sum(as_float(row.get("source_residual_qty")) for row in selected),
            "residual_cost": sum(as_float(row.get("source_residual_cost")) for row in selected),
        }

    all_totals = totals(rows)
    target_totals = totals(target_rows)

    def share(key: str) -> float | None:
        return target_totals[key] / all_totals[key] if all_totals[key] else None

    return {
        "all_seed_totals": all_totals,
        "target_totals": target_totals,
        "target_shares": {
            "row_share": share("rows"),
            "seed_qty_share": share("seed_qty"),
            "pair_qty_share": share("pair_qty"),
            "residual_qty_share": share("residual_qty"),
            "residual_cost_share": share("residual_cost"),
        },
    }


def select_rows(rows: list[dict[str, Any]], sample_cap: int, per_day_cap: int) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("day") or ""),
            -as_float(row.get("source_residual_cost")),
            -as_float(row.get("source_residual_qty")),
            as_int(row.get("ts_ms")),
            str(row.get("condition_id") or ""),
            str(row.get("source_seed_candidate_row_id") or ""),
        ),
    )
    selected: list[dict[str, Any]] = []
    selected_by_day: dict[str, int] = defaultdict(int)
    selected_conditions: set[str] = set()
    for row in ordered:
        if len(selected) >= sample_cap:
            break
        day = str(row.get("day") or "")
        condition_id = str(row.get("condition_id") or "")
        if selected_by_day[day] >= per_day_cap or condition_id in selected_conditions:
            continue
        selected.append(row)
        selected_by_day[day] += 1
        selected_conditions.add(condition_id)
    if len(selected) < sample_cap:
        seen = {id(row) for row in selected}
        for row in ordered:
            if len(selected) >= sample_cap:
                break
            day = str(row.get("day") or "")
            if id(row) in seen or selected_by_day[day] >= per_day_cap:
                continue
            selected.append(row)
            selected_by_day[day] += 1
            seen.add(id(row))
    return selected


def build_requests(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in selected:
        rows.append(
            {
                "validation_request_id": "micro_deficit_repair_guard_source_truth_v1",
                "day": row.get("day", ""),
                "condition_id": row.get("condition_id", ""),
                "slug": row.get("slug", ""),
                "candidate_action_id": row.get("source_seed_candidate_row_id", ""),
                "action_ts_ms": row.get("ts_ms", ""),
                "side": row.get("side", ""),
                "seed_qty": row.get("seed_qty", ""),
                "seed_px": row.get("seed_px", ""),
                "expected_decision": "would_defer_micro_deficit_repair_guard",
                "decision_reason": "pre_action_micro_deficit_repair_guard_selected_source_truth_readiness",
                "fee_rate": row.get("official_fee_rate", ""),
                "fee_rate_source": "candidate_seed_outcome_separator_export_v1:official_fee_rate",
                "source_seed_action_id": row.get("source_seed_action_id", ""),
                "source_seed_candidate_row_id": row.get("source_seed_candidate_row_id", ""),
                "source_risk_direction": row.get("source_risk_direction", ""),
                "pre_seed_same_qty": row.get("pre_seed_same_qty", ""),
                "pre_seed_opp_qty": row.get("pre_seed_opp_qty", ""),
                "pre_seed_open_qty": row.get("pre_seed_open_qty", ""),
                "trigger_px": row.get("trigger_px", ""),
                "trigger_size": row.get("trigger_size", ""),
                "outcome_labels_are_post_action": row.get("outcome_labels_are_post_action", ""),
            }
        )
    return rows


def response_summary(response_dir: Path | None, selected_count: int) -> dict[str, Any]:
    if response_dir is None:
        return {"available": False, "reason": "response_dir_not_supplied"}
    manifest_path = response_dir / "VALIDATION_RESPONSE_MANIFEST.json"
    action_decisions_path = response_dir / "action_decisions.csv"
    if not manifest_path.exists() or not action_decisions_path.exists():
        return {
            "available": False,
            "reason": "response_manifest_or_action_decisions_missing",
            "response_dir": str(response_dir),
        }
    manifest = read_json(manifest_path)
    rows = read_csv(action_decisions_path)
    ref_fields = [
        "market_meta_ref",
        "book_l1_ref",
        "book_l2_ref",
        "trade_before_ref",
        "trade_after_ref",
        "settlement_ref",
    ]
    ref_counts = {field: sum(bool(str(row.get(field) or "").strip()) for row in rows) for field in ref_fields}
    status_counts: dict[str, int] = defaultdict(int)
    reason_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        status_counts[str(row.get("validation_status") or "")] += 1
        for reason in str(row.get("reasons") or "").split("|"):
            if reason:
                reason_counts[reason] += 1
    private_counts = {
        str(key): as_int(value)
        for key, value in (manifest.get("private_table_counts") or {}).items()
    }
    all_private_zero = all(value == 0 for value in private_counts.values())
    all_refs_present = all(count == selected_count for count in ref_counts.values())
    all_validated = (
        len(rows) == selected_count
        and selected_count > 0
        and status_counts.get(VALIDATED_STATUS, 0) == selected_count
        and manifest.get("status") == VALIDATED_STATUS
    )
    return {
        "available": True,
        "response_dir": str(response_dir),
        "response_manifest": str(manifest_path),
        "action_decisions": str(action_decisions_path),
        "status": manifest.get("status"),
        "row_count": len(rows),
        "status_counts": dict(sorted(status_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "ref_counts": ref_counts,
        "all_refs_present": all_refs_present,
        "all_validated": all_validated,
        "private_table_counts": private_counts,
        "all_private_counts_zero": all_private_zero,
        "public_or_proxy_truth_only": manifest.get("public_or_proxy_truth_only") is True,
        "private_truth_ready": manifest.get("private_truth_ready") is True,
        "deployable": manifest.get("deployable") is True,
        "promotion_gate_pass": manifest.get("promotion_gate_pass") is True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--separator-csv", type=Path, required=True)
    parser.add_argument("--response-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-cap", type=int, default=60)
    parser.add_argument("--per-day-cap", type=int, default=4)
    parser.add_argument("--max-deficit-qty", type=float, default=0.25)
    parser.add_argument("--open-qty-cap", type=float, default=1.0)
    args = parser.parse_args()

    rows = read_csv(args.separator_csv)
    target_rows = [
        row
        for row in rows
        if is_micro_deficit_target(row, args.max_deficit_qty, args.open_qty_cap)
    ]
    selected = select_rows(target_rows, max(0, args.sample_cap), max(0, args.per_day_cap))
    request_rows = build_requests(selected)
    request_fields = list(request_rows[0].keys()) if request_rows else [
        "validation_request_id",
        "day",
        "condition_id",
        "slug",
        "candidate_action_id",
        "action_ts_ms",
        "side",
        "seed_qty",
        "seed_px",
        "expected_decision",
        "decision_reason",
        "fee_rate",
        "fee_rate_source",
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    requests_path = args.output_dir / "selected_micro_deficit_seed_requests.csv"
    write_csv(requests_path, request_rows, request_fields)

    selected_days = sorted({str(row.get("day") or "") for row in selected})
    forbidden_selected = sorted(day for day in selected_days if day in FORBIDDEN_DAYS)
    selected_by_day: dict[str, int] = defaultdict(int)
    side_counts: dict[str, int] = defaultdict(int)
    residual_bucket_counts: dict[str, int] = defaultdict(int)
    pair_bucket_counts: dict[str, int] = defaultdict(int)
    for row in selected:
        selected_by_day[str(row.get("day") or "")] += 1
        side_counts[str(row.get("side") or "")] += 1
        residual_bucket_counts[str(row.get("residual_tail_outcome_bucket") or "")] += 1
        pair_bucket_counts[str(row.get("pair_outcome_bucket") or "")] += 1
    response = response_summary(args.response_dir, len(selected))
    blockers = []
    if not selected:
        blockers.append("no_micro_deficit_target_rows_selected")
    if forbidden_selected:
        blockers.append("forbidden_days_selected")
    if not response.get("available"):
        blockers.append(str(response.get("reason") or "response_unavailable"))
    elif not response.get("all_validated"):
        blockers.append("selected_actions_not_all_source_truth_validated")
    elif not response.get("all_refs_present"):
        blockers.append("selected_actions_missing_source_refs")
    if response.get("private_truth_ready") or response.get("deployable") or response.get("promotion_gate_pass"):
        blockers.append("response_claims_private_deployable_or_promotion")

    if forbidden_selected:
        decision = "DISCARD"
        label = "DISCARD_MICRO_DEFICIT_SOURCE_TRUTH_FORBIDDEN_DAY_SELECTED"
    elif not blockers:
        decision = "KEEP"
        label = "KEEP_MICRO_DEFICIT_SOURCE_TRUTH_READINESS_SELECTED_LOOKUP"
    else:
        decision = "UNKNOWN"
        label = "UNKNOWN_MICRO_DEFICIT_SOURCE_TRUTH_READINESS_GAPS"

    manifest = {
        "artifact": "xuan_b27_dplus_micro_deficit_source_truth_readiness",
        "created_utc": utc_label(),
        "lane": "causal_verifier_source_truth_readiness",
        "decision": decision,
        "decision_label": label,
        "blockers": blockers,
        "inputs": {
            "separator_csv": str(args.separator_csv),
            "response_dir": str(args.response_dir) if args.response_dir else None,
        },
        "selection": {
            "schema_version": "micro_deficit_source_truth_selection_v1",
            "target_predicate": {
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_opp_qty_minus_same_qty_min_exclusive": 0.0,
                "pre_seed_opp_qty_minus_same_qty_max_inclusive": args.max_deficit_qty,
                "pre_seed_open_qty_max_inclusive": args.open_qty_cap,
            },
            "target_row_count": len(target_rows),
            "selected_seed_count": len(selected),
            "sample_cap": args.sample_cap,
            "per_day_cap": args.per_day_cap,
            "selected_days": selected_days,
            "selected_by_day": dict(sorted(selected_by_day.items())),
            "selected_condition_count": len({str(row.get("condition_id") or "") for row in selected}),
            "selected_side_counts": dict(sorted(side_counts.items())),
            "selected_pair_bucket_counts": dict(sorted(pair_bucket_counts.items())),
            "selected_residual_tail_bucket_counts": dict(sorted(residual_bucket_counts.items())),
            "forbidden_days_selected": forbidden_selected,
            "requests_csv": str(requests_path),
            "request_contract": "selected action request rows for replay_store_v2 truth_validation_response_builder",
        },
        "offline_target_evidence": summarize_targets(rows, target_rows),
        "response_summary": response,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "KEEP means selected affected micro-deficit seed rows have replay_store_v2 source-truth lookup "
                "coverage sufficient to prepare a bounded no-order shadow-review question. It is not private "
                "truth, deployable evidence, canary evidence, or promotion evidence."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "SOURCE_TRUTH_READINESS_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "queried_replay_store_v2": False,
            "read_response_from_existing_selected_lookup": response.get("available") is True,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "ssh_started": False,
            "shadow_or_canary_started": False,
            "shared_ingress_connected_or_modified": False,
            "broker_service_env_live_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "next_executable_action": (
            "If KEEP, select one bounded no-order shadow-review question for micro_deficit_repair_guard_v1. "
            "Do not start remote shadow/canary from this artifact alone."
        ),
    }
    write_json(args.output_dir / "manifest.json", manifest)
    print(args.output_dir / "manifest.json")
    return 0 if decision == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
