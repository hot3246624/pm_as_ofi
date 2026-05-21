#!/usr/bin/env python3
"""Score replay-derived truth_validation_response_v1 action outputs.

This local-only scorer audits selected-action validation responses. It does not
discover candidates, query replay_store_v2, read events JSONL, connect to
shared-ingress, or touch order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALIDATED_STATUS = "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY"
SCHEMA_VERSION = "truth_validation_response_action_scorer_v1"
REQUIRED_SOURCE_REFS = ("market_meta_ref", "book_l1_ref", "book_l2_ref")
OPTIONAL_SOURCE_REFS = ("trade_before_ref", "trade_after_ref", "settlement_ref")
STRICT_CLOSE_FIELDS = (
    "close_action_id",
    "close_ts_ms",
    "close_side",
    "close_qty",
    "close_px",
    "fee_rate_source",
    "book_l1_ref",
    "book_l2_ref",
    "trade_before_ref",
    "trade_after_ref",
)
PRIVATE_FALSE_FIELDS = ("private_truth_ready", "deployable", "promotion_gate_pass")
RESIDUAL_FLOAT_TOLERANCE = 1e-9


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--response-manifest", type=Path, required=True)
    parser.add_argument("--action-decisions", type=Path)
    parser.add_argument("--residual-fifo-lots", type=Path)
    parser.add_argument("--strict-rescue-closes", type=Path)
    parser.add_argument("--selected-actions", type=Path)
    parser.add_argument("--state-machine-duckdb", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def resolve_output_path(manifest: dict[str, Any], key: str, fallback: Path) -> Path:
    outputs = manifest.get("outputs")
    if isinstance(outputs, dict) and outputs.get(key):
        return Path(str(outputs[key]))
    return fallback


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return default
    return default


def truthy_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    if value in (0, "0", "false", "False", "", None):
        return True
    return False


def load_selected_actions(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    obj = read_json(path)
    if isinstance(obj.get("actions"), list):
        return [dict(row) for row in obj["actions"] if isinstance(row, dict)]
    if isinstance(obj, list):
        return [dict(row) for row in obj if isinstance(row, dict)]
    return []


def source_ref_metrics(rows: list[dict[str, str]], fields: tuple[str, ...]) -> dict[str, Any]:
    total = len(rows)
    by_field: dict[str, Any] = {}
    for field in fields:
        present = sum(1 for row in rows if row.get(field, "").strip())
        by_field[field] = {
            "present": present,
            "missing": total - present,
            "coverage": (present / total) if total else 0.0,
        }
    return by_field


def fee_metrics(rows: list[dict[str, str]], selected_actions: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    source_present = sum(1 for row in rows if row.get("fee_rate_source", "").strip())
    expected_by_action = {
        str(action.get("candidate_action_id")): as_float(action.get("expected_official_taker_fee"), None)
        for action in selected_actions
        if action.get("candidate_action_id") not in (None, "")
    }
    deltas: list[float] = []
    for row in rows:
        expected = expected_by_action.get(str(row.get("candidate_action_id")))
        if expected is None:
            continue
        actual = as_float(row.get("official_fee"), None)
        if actual is None:
            continue
        deltas.append(abs(actual - expected))
    return {
        "fee_rate_source_present": source_present,
        "fee_rate_source_missing": total - source_present,
        "fee_rate_source_coverage": (source_present / total) if total else 0.0,
        "formula": "fee = shares * fee_rate * price * (1 - price)",
        "expected_fee_join_count": len(deltas),
        "max_abs_delta_vs_selected_expected_official_taker_fee": max(deltas) if deltas else None,
        "avg_abs_delta_vs_selected_expected_official_taker_fee": (sum(deltas) / len(deltas)) if deltas else None,
    }


def decision_consistency(rows: list[dict[str, str]]) -> dict[str, Any]:
    inconsistent: list[dict[str, str]] = []
    for row in rows:
        expected = row.get("expected_decision", "").strip().lower()
        status = row.get("validation_status", "").strip()
        if expected == "accept" and status != VALIDATED_STATUS:
            inconsistent.append(
                {
                    "validation_request_id": row.get("validation_request_id", ""),
                    "candidate_action_id": row.get("candidate_action_id", ""),
                    "expected_decision": row.get("expected_decision", ""),
                    "validation_status": status,
                }
            )
    return {
        "expected_accept_count": sum(1 for row in rows if row.get("expected_decision", "").strip().lower() == "accept"),
        "inconsistent_count": len(inconsistent),
        "examples": inconsistent[:10],
    }


def private_guard_metrics(manifest: dict[str, Any]) -> dict[str, Any]:
    promotion_gate = manifest.get("promotion_gate") if isinstance(manifest.get("promotion_gate"), dict) else {}
    fields = {field: truthy_false(manifest.get(field)) for field in PRIVATE_FALSE_FIELDS}
    fields["promotion_gate.passed"] = truthy_false(promotion_gate.get("passed"))
    fields["promotion_gate.private_truth_ready"] = truthy_false(promotion_gate.get("private_truth_ready"))
    fields["promotion_gate.deployable"] = truthy_false(promotion_gate.get("deployable"))
    fields["promotion_gate.g2_canary_ready"] = truthy_false(promotion_gate.get("g2_canary_ready"))
    return {
        "all_false": all(fields.values()),
        "field_checks": fields,
        "promotion_gate_status": promotion_gate.get("status", ""),
    }


def residual_metrics(residual_rows: list[dict[str, str]], selected_actions: list[dict[str, Any]]) -> dict[str, Any]:
    selected_ids = {str(action.get("candidate_action_id")) for action in selected_actions if action.get("candidate_action_id") not in (None, "")}
    source_ids = {str(row.get("source_seed_action_id")) for row in residual_rows if row.get("source_seed_action_id")}
    covered_ids = selected_ids & source_ids
    consistency = residual_selected_metadata_consistency(residual_rows, selected_actions)
    return {
        "selected_action_count": len(selected_ids),
        "residual_fifo_row_count": len(residual_rows),
        "selected_actions_with_response_residual_lots": len(covered_ids),
        "selected_residual_coverage": (len(covered_ids) / len(selected_ids)) if selected_ids else None,
        "selected_metadata_consistency": consistency,
        "coverage_interpretation": (
            "No residual rows in response output; this may be expected for action-decision-only source-truth responses."
            if not residual_rows
            else "Residual rows are present in response output."
        ),
    }


def has_selected_residual_metadata(action: dict[str, Any]) -> bool:
    return any(
        action.get(field) not in (None, "")
        for field in ("lot_id", "source_seed_action_id", "residual_target_qty", "residual_target_cost")
    )


def residual_selected_metadata_consistency(
    residual_rows: list[dict[str, str]],
    selected_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    expected_actions = [
        action
        for action in selected_actions
        if action.get("candidate_action_id") not in (None, "") and has_selected_residual_metadata(action)
    ]
    rows_by_candidate: dict[str, list[dict[str, str]]] = {}
    rows_by_source_seed: dict[str, list[dict[str, str]]] = {}
    for row in residual_rows:
        candidate_action_id = str(row.get("candidate_action_id", ""))
        source_seed_action_id = str(row.get("source_seed_action_id", ""))
        if candidate_action_id:
            rows_by_candidate.setdefault(candidate_action_id, []).append(row)
        if source_seed_action_id:
            rows_by_source_seed.setdefault(source_seed_action_id, []).append(row)

    missing: list[dict[str, Any]] = []
    mismatch_examples: list[dict[str, Any]] = []
    lot_id_match_count = 0
    source_seed_match_count = 0
    qty_match_count = 0
    cost_match_count = 0
    status_validated_count = 0
    max_qty_delta: float | None = None
    max_cost_delta: float | None = None

    for action in expected_actions:
        candidate_action_id = str(action.get("candidate_action_id"))
        expected_source_seed = str(action.get("source_seed_action_id", candidate_action_id))
        matches = rows_by_source_seed.get(expected_source_seed) or rows_by_candidate.get(candidate_action_id) or []
        if not matches:
            missing.append(
                {
                    "candidate_action_id": candidate_action_id,
                    "source_seed_action_id": expected_source_seed,
                    "lot_id": action.get("lot_id", ""),
                }
            )
            continue
        row = matches[0]
        mismatches: dict[str, Any] = {}

        expected_lot_id = action.get("lot_id")
        if expected_lot_id not in (None, ""):
            if str(row.get("lot_id", "")) == str(expected_lot_id):
                lot_id_match_count += 1
            else:
                mismatches["lot_id"] = {"expected": expected_lot_id, "actual": row.get("lot_id", "")}

        if str(row.get("source_seed_action_id", "")) == expected_source_seed:
            source_seed_match_count += 1
        else:
            mismatches["source_seed_action_id"] = {
                "expected": expected_source_seed,
                "actual": row.get("source_seed_action_id", ""),
            }

        if action.get("residual_target_qty") not in (None, ""):
            expected_qty = as_float(action.get("residual_target_qty"), None)
            actual_qty = as_float(row.get("remaining_qty"), None)
            if expected_qty is None or actual_qty is None:
                mismatches["remaining_qty"] = {"expected": action.get("residual_target_qty"), "actual": row.get("remaining_qty", "")}
            else:
                delta = abs(actual_qty - expected_qty)
                max_qty_delta = delta if max_qty_delta is None else max(max_qty_delta, delta)
                if delta <= RESIDUAL_FLOAT_TOLERANCE:
                    qty_match_count += 1
                else:
                    mismatches["remaining_qty"] = {"expected": expected_qty, "actual": actual_qty, "delta": delta}

        if action.get("residual_target_cost") not in (None, ""):
            expected_cost = as_float(action.get("residual_target_cost"), None)
            actual_cost = as_float(row.get("remaining_cost"), None)
            if expected_cost is None or actual_cost is None:
                mismatches["remaining_cost"] = {
                    "expected": action.get("residual_target_cost"),
                    "actual": row.get("remaining_cost", ""),
                }
            else:
                delta = abs(actual_cost - expected_cost)
                max_cost_delta = delta if max_cost_delta is None else max(max_cost_delta, delta)
                if delta <= RESIDUAL_FLOAT_TOLERANCE:
                    cost_match_count += 1
                else:
                    mismatches["remaining_cost"] = {"expected": expected_cost, "actual": actual_cost, "delta": delta}

        if row.get("validation_status", "") == VALIDATED_STATUS:
            status_validated_count += 1
        else:
            mismatches["validation_status"] = {"expected": VALIDATED_STATUS, "actual": row.get("validation_status", "")}

        if mismatches:
            mismatch_examples.append(
                {
                    "candidate_action_id": candidate_action_id,
                    "source_seed_action_id": expected_source_seed,
                    "mismatches": mismatches,
                }
            )

    expected_count = len(expected_actions)
    matched_count = expected_count - len(missing)
    return {
        "expected_residual_action_count": expected_count,
        "response_residual_row_count": len(residual_rows),
        "matched_residual_action_count": matched_count,
        "missing_residual_action_count": len(missing),
        "lot_id_match_count": lot_id_match_count,
        "source_seed_action_id_match_count": source_seed_match_count,
        "remaining_qty_match_count": qty_match_count,
        "remaining_cost_match_count": cost_match_count,
        "validation_status_validated_count": status_validated_count,
        "match_coverage": (matched_count / expected_count) if expected_count else None,
        "all_expected_residual_actions_matched": len(missing) == 0 and len(mismatch_examples) == 0,
        "max_abs_remaining_qty_delta": max_qty_delta,
        "max_abs_remaining_cost_delta": max_cost_delta,
        "missing_examples": missing[:10],
        "mismatch_examples": mismatch_examples[:10],
        "interpretation": (
            "No selected residual metadata was supplied; residual FIFO consistency check is not applicable."
            if expected_count == 0
            else "Selected residual metadata was compared to response residual_fifo_lots.csv rows."
        ),
    }


def residual_join_metrics(state_machine_duckdb: Path | None, selected_actions: list[dict[str, Any]]) -> dict[str, Any]:
    selected_ids = sorted({str(action.get("candidate_action_id")) for action in selected_actions if action.get("candidate_action_id") not in (None, "")})
    if not state_machine_duckdb or not state_machine_duckdb.exists() or not selected_ids:
        return {
            "join_attempted": False,
            "reason": "state_machine_duckdb or selected action ids unavailable",
        }
    import duckdb

    con = duckdb.connect(str(state_machine_duckdb), read_only=True)
    placeholders = ",".join(["?"] * len(selected_ids))
    rows = con.execute(
        f"""
        select source_seed_action_id, count(*) as lot_count, sum(qty) as qty, sum(cost) as cost
        from residual_lots
        where source_seed_action_id in ({placeholders})
        group by 1
        order by 1
        """,
        selected_ids,
    ).fetchall()
    covered = {str(row[0]) for row in rows}
    return {
        "join_attempted": True,
        "selected_action_count": len(selected_ids),
        "selected_actions_with_state_machine_residual_lots": len(covered),
        "selected_residual_coverage": len(covered) / len(selected_ids),
        "lot_count": sum(int(row[1] or 0) for row in rows),
        "qty": sum(as_float(row[2]) for row in rows),
        "cost": sum(as_float(row[3]) for row in rows),
        "covered_action_ids": sorted(covered)[:50],
    }


def strict_close_metrics(close_rows: list[dict[str, str]]) -> dict[str, Any]:
    missing_by_field: dict[str, int] = {}
    for field in STRICT_CLOSE_FIELDS:
        missing_by_field[field] = sum(1 for row in close_rows if not row.get(field, "").strip())
    if not close_rows:
        missing_fields = list(STRICT_CLOSE_FIELDS)
    else:
        missing_fields = [field for field, missing in missing_by_field.items() if missing]
    return {
        "strict_rescue_close_row_count": len(close_rows),
        "missing_fields": missing_fields,
        "missing_by_field": missing_by_field,
        "coverage_interpretation": (
            "No strict rescue close rows were supplied; full close timing/cost/source-truth validation remains a named gap."
            if not close_rows
            else "Strict rescue close rows were supplied; inspect field-level missing counts."
        ),
    }


def write_scored_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0].keys()) + [
        "score_market_meta_ref_present",
        "score_book_l1_ref_present",
        "score_book_l2_ref_present",
        "score_fee_rate_source_present",
        "score_expected_accept_consistent",
    ] if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["score_market_meta_ref_present"] = bool(row.get("market_meta_ref", "").strip())
            out["score_book_l1_ref_present"] = bool(row.get("book_l1_ref", "").strip())
            out["score_book_l2_ref_present"] = bool(row.get("book_l2_ref", "").strip())
            out["score_fee_rate_source_present"] = bool(row.get("fee_rate_source", "").strip())
            out["score_expected_accept_consistent"] = (
                row.get("expected_decision", "").strip().lower() != "accept"
                or row.get("validation_status", "").strip() == VALIDATED_STATUS
            )
            writer.writerow(out)


def main() -> int:
    args = parse_args()
    response_manifest = read_json(args.response_manifest)
    response_dir = args.response_manifest.parent
    action_path = args.action_decisions or resolve_output_path(response_manifest, "action_decisions", response_dir / "action_decisions.csv")
    residual_path = args.residual_fifo_lots or resolve_output_path(response_manifest, "residual_fifo_lots", response_dir / "residual_fifo_lots.csv")
    closes_path = args.strict_rescue_closes or resolve_output_path(response_manifest, "strict_rescue_closes", response_dir / "strict_rescue_closes.csv")
    action_rows = read_csv(action_path)
    residual_rows = read_csv(residual_path)
    close_rows = read_csv(closes_path)
    selected_actions = load_selected_actions(args.selected_actions)

    days = sorted({row.get("day", "") for row in action_rows if row.get("day")})
    conditions = sorted({row.get("condition_id", "") for row in action_rows if row.get("condition_id")})
    status_counts = dict(Counter(row.get("validation_status", "") for row in action_rows))
    required_refs = source_ref_metrics(action_rows, REQUIRED_SOURCE_REFS)
    optional_refs = source_ref_metrics(action_rows, OPTIONAL_SOURCE_REFS)
    fees = fee_metrics(action_rows, selected_actions)
    decisions = decision_consistency(action_rows)
    private_guard = private_guard_metrics(response_manifest)
    residual = residual_metrics(residual_rows, selected_actions)
    residual_join = residual_join_metrics(args.state_machine_duckdb, selected_actions)
    closes = strict_close_metrics(close_rows)

    required_ref_complete = all(metric["coverage"] == 1.0 for metric in required_refs.values())
    fee_source_complete = fees["fee_rate_source_coverage"] == 1.0
    decision_consistent = decisions["inconsistent_count"] == 0
    private_guard_ok = private_guard["all_false"]
    residual_consistency = residual["selected_metadata_consistency"]
    residual_consistent = (
        residual_consistency["expected_residual_action_count"] == 0
        or residual_consistency["all_expected_residual_actions_matched"]
    )
    if not action_rows:
        decision = "UNKNOWN"
        label = "UNKNOWN_TRUTH_VALIDATION_RESPONSE_EMPTY"
    elif required_ref_complete and fee_source_complete and decision_consistent and private_guard_ok and residual_consistent:
        decision = "KEEP"
        label = "KEEP_TRUTH_VALIDATION_RESPONSE_ACTION_SCORE_READY"
    else:
        decision = "UNKNOWN"
        label = "UNKNOWN_TRUTH_VALIDATION_RESPONSE_ACTION_SCORE_GAPS"

    score = {
        "schema_version": SCHEMA_VERSION,
        "artifact": "xuan_b27_dplus_truth_validation_response_action_score",
        "created_utc": utc_label(),
        "response_manifest": str(args.response_manifest),
        "action_decisions": str(action_path),
        "selected_actions": str(args.selected_actions) if args.selected_actions else "",
        "state_machine_duckdb": str(args.state_machine_duckdb) if args.state_machine_duckdb else "",
        "decision": decision,
        "label": label,
        "row_count": len(action_rows),
        "status_counts": status_counts,
        "day_count": len(days),
        "condition_count": len(conditions),
        "days": days,
        "required_source_refs": required_refs,
        "optional_source_refs": optional_refs,
        "fee_metrics": fees,
        "decision_consistency": decisions,
        "private_guard": private_guard,
        "residual_fifo": residual,
        "state_machine_residual_lots_join": residual_join,
        "strict_rescue_closes": closes,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": "Action-level scorer grades replay-derived source-truth response quality only. It is not private-truth or promotion evidence.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "SOURCE_TRUTH_SCORER_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "can_support_strategy_promotion": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "broad_strategy_search": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "named_gaps": {
            "residual_fifo_response_rows_absent": len(residual_rows) == 0,
            "residual_fifo_selected_metadata_mismatch": not residual_consistent,
            "strict_rescue_close_missing_fields": closes["missing_fields"],
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "score.json", score)
    write_json(
        args.output_dir / "manifest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact": "xuan_b27_dplus_truth_validation_response_action_scorer",
            "created_utc": score["created_utc"],
            "status": label,
            "decision": decision,
            "score": str(args.output_dir / "score.json"),
            "scored_action_decisions": str(args.output_dir / "scored_action_decisions.csv"),
            "research_ranking": score["research_ranking"],
            "promotion_gate": score["promotion_gate"],
            "side_effects": score["side_effects"],
        },
    )
    write_scored_csv(args.output_dir / "scored_action_decisions.csv", action_rows)
    print(args.output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
