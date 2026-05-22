#!/usr/bin/env python3
"""Verify D+ residual-tail ledger context joins.

This local-only verifier consumes selected_seed_ledger_context_export_v1 rows
and residual-tail exemplar scorer score.json. It checks whether local
candidate/state-machine seed context can be joined to no-order exemplar source
context by explicit id or bounded condition/side/time/trigger keys.

It does not read events JSONL, replay/raw stores, sockets, SSH, or live paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
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
)
REQUIRED_SELECTED_FIELDS = (
    "schema_version",
    "profile_name",
    "source_seed_candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "trigger_ts_ms",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "source_risk_direction",
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
    "source_residual_age_s",
    "external_shadow_ids_available",
    "external_shadow_id_policy",
)
COMPARABLE_FIELDS = (
    "offset_s",
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
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def as_int(value: Any, default: int = 0) -> int:
    return int(round(as_float(value, default)))


def safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator > 0.0 else 0.0


def field_present(row: dict[str, Any], field: str) -> bool:
    if field not in row:
        return False
    value = row.get(field)
    return value is not None and value != ""


def exemplar_rows(score: dict[str, Any]) -> list[dict[str, Any]]:
    rankings = score.get("rankings") if isinstance(score.get("rankings"), dict) else {}
    rows = rankings.get("top_residual_tail_exemplars")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def group_rows(score: dict[str, Any]) -> list[dict[str, Any]]:
    rankings = score.get("rankings") if isinstance(score.get("rankings"), dict) else {}
    rows = rankings.get("top_source_side_offset_risk_direction_groups")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def selected_id(row: dict[str, Any]) -> str:
    return str(row.get("source_seed_candidate_row_id") or row.get("source_seed_action_id") or "")


def exemplar_id_candidates(row: dict[str, Any]) -> list[str]:
    values = [
        row.get("source_seed_candidate_row_id"),
        row.get("candidate_row_id"),
        row.get("source_order_id"),
        row.get("source_seed_action_id"),
    ]
    return [str(value) for value in values if value not in (None, "")]


def normalized_condition(row: dict[str, Any]) -> str:
    return str(row.get("condition_id") or row.get("slug") or "")


def context_match(selected: dict[str, Any], exemplar: dict[str, Any], args: argparse.Namespace) -> bool:
    if normalized_condition(selected) != normalized_condition(exemplar):
        return False
    if str(selected.get("side") or "") != str(exemplar.get("side") or ""):
        return False
    selected_ts = as_int(selected.get("trigger_ts_ms", selected.get("ts_ms")))
    exemplar_ts = as_int(exemplar.get("trigger_ts_ms", exemplar.get("ts_ms")))
    if abs(selected_ts - exemplar_ts) > args.max_trigger_ts_delta_ms:
        return False
    if abs(as_float(selected.get("trigger_px")) - as_float(exemplar.get("trigger_px"))) > args.max_trigger_px_delta:
        return False
    if abs(as_float(selected.get("trigger_size")) - as_float(exemplar.get("trigger_size"))) > args.max_trigger_size_delta:
        return False
    return True


def best_match(
    selected_rows: list[dict[str, Any]],
    by_id: dict[str, dict[str, Any]],
    exemplar: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, str, list[str]]:
    ids = exemplar_id_candidates(exemplar)
    for candidate_id in ids:
        row = by_id.get(candidate_id)
        if row is not None:
            return row, "id", ids
    context_candidates = [row for row in selected_rows if context_match(row, exemplar, args)]
    if context_candidates:
        context_candidates.sort(
            key=lambda row: (
                abs(as_int(row.get("trigger_ts_ms", row.get("ts_ms"))) - as_int(exemplar.get("trigger_ts_ms"))),
                abs(as_float(row.get("trigger_px")) - as_float(exemplar.get("trigger_px"))),
                abs(as_float(row.get("trigger_size")) - as_float(exemplar.get("trigger_size"))),
                selected_id(row),
            )
        )
        return context_candidates[0], "condition_side_trigger", ids
    return None, "unmatched", ids


def selected_required_coverage(rows: list[dict[str, Any]]) -> tuple[float, dict[str, float], list[str]]:
    denominator = max(len(rows), 1)
    by_field = {
        field: round(safe_ratio(sum(1 for row in rows if field_present(row, field)), denominator), 8)
        for field in REQUIRED_SELECTED_FIELDS
    }
    missing = [field for field, coverage in by_field.items() if coverage < 1.0]
    if not rows:
        return 0.0, by_field, list(REQUIRED_SELECTED_FIELDS)
    present = sum(1 for row in rows for field in REQUIRED_SELECTED_FIELDS if field_present(row, field))
    return present / (len(rows) * len(REQUIRED_SELECTED_FIELDS)), by_field, missing


def score_ready(score: dict[str, Any]) -> tuple[bool, list[str]]:
    checks = score.get("checks") if isinstance(score.get("checks"), dict) else {}
    failures: list[str] = []
    for name in (
        "paths_safe",
        "report_shape_ok",
        "aggregate_exemplars_subset_of_summaries",
        "required_field_coverage_ok",
        "source_sequence_coverage_ok",
    ):
        if checks.get(name) is not True:
            failures.append(f"exemplar_score.checks.{name}")
    if score.get("promotion_gate", {}).get("passed") is True:
        failures.append("exemplar_score.promotion_gate_unexpectedly_passed")
    side_effects = score.get("side_effects") if isinstance(score.get("side_effects"), dict) else {}
    if side_effects.get("events_jsonl_read") is True:
        failures.append("exemplar_score.events_jsonl_read")
    if side_effects.get("orders_sent") is True:
        failures.append("exemplar_score.orders_sent")
    return not failures, failures


def compare_rows(selected: dict[str, Any], exemplar: dict[str, Any]) -> dict[str, Any]:
    deltas: dict[str, Any] = {}
    max_abs_delta = 0.0
    for field in COMPARABLE_FIELDS:
        selected_value = as_float(selected.get(field))
        exemplar_value = as_float(exemplar.get(field))
        delta = selected_value - exemplar_value
        max_abs_delta = max(max_abs_delta, abs(delta))
        deltas[field] = {
            "selected": round(selected_value, 12),
            "exemplar": round(exemplar_value, 12),
            "abs_delta": round(abs(delta), 12),
        }
    if field_present(exemplar, "source_residual_age_ms"):
        selected_age_s = as_float(selected.get("source_residual_age_s"))
        exemplar_age_s = as_float(exemplar.get("source_residual_age_ms")) / 1000.0
        delta = selected_age_s - exemplar_age_s
        max_abs_delta = max(max_abs_delta, abs(delta))
        deltas["source_residual_age_s"] = {
            "selected": round(selected_age_s, 12),
            "exemplar": round(exemplar_age_s, 12),
            "abs_delta": round(abs(delta), 12),
        }
    return {
        "max_abs_delta": round(max_abs_delta, 12),
        "deltas": deltas,
    }


def verify(
    *,
    selected_seed_ledger_contexts: Path,
    exemplar_score_path: Path,
    output_dir: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    label = utc_label()
    paths_safe = path_is_safe(selected_seed_ledger_contexts) and path_is_safe(exemplar_score_path) and path_is_safe(output_dir)
    selected_rows = load_csv(selected_seed_ledger_contexts)
    score = load_json(exemplar_score_path)
    exemplars = exemplar_rows(score)
    groups = group_rows(score)
    selected_coverage, selected_coverage_by_field, selected_missing_fields = selected_required_coverage(selected_rows)
    score_ok, score_failures = score_ready(score)
    by_id = {selected_id(row): row for row in selected_rows if selected_id(row)}

    matches: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    max_context_delta = 0.0
    join_mode_counts: dict[str, int] = {}
    for exemplar in exemplars:
        selected, mode, attempted_ids = best_match(selected_rows, by_id, exemplar, args)
        join_mode_counts[mode] = join_mode_counts.get(mode, 0) + 1
        if selected is None:
            unmatched.append(
                {
                    "condition_id": exemplar.get("condition_id"),
                    "slug": exemplar.get("slug"),
                    "side": exemplar.get("side"),
                    "trigger_ts_ms": exemplar.get("trigger_ts_ms"),
                    "trigger_px": exemplar.get("trigger_px"),
                    "trigger_size": exemplar.get("trigger_size"),
                    "source_order_id": exemplar.get("source_order_id"),
                    "source_sequence_id": exemplar.get("source_sequence_id"),
                    "attempted_ids": attempted_ids,
                    "missing_join_keys": [
                        "source_seed_candidate_row_id/candidate_row_id/source_order_id id match",
                        "condition_id+side+trigger_ts_ms+trigger_px+trigger_size context match",
                    ],
                }
            )
            continue
        comparison = compare_rows(selected, exemplar)
        max_context_delta = max(max_context_delta, as_float(comparison["max_abs_delta"]))
        matches.append(
            {
                "join_mode": mode,
                "source_seed_candidate_row_id": selected_id(selected),
                "condition_id": selected.get("condition_id"),
                "side": selected.get("side"),
                "trigger_ts_ms": selected.get("trigger_ts_ms"),
                "source_order_id": exemplar.get("source_order_id"),
                "source_sequence_id": exemplar.get("source_sequence_id"),
                "max_abs_delta": comparison["max_abs_delta"],
                "deltas": comparison["deltas"],
            }
        )

    exemplar_count = len(exemplars)
    selected_count = len(selected_rows)
    matched_count = len(matches)
    join_coverage = safe_ratio(matched_count, exemplar_count)
    selected_residual_rows = sum(1 for row in selected_rows if as_float(row.get("source_residual_qty")) > 0.0)
    selected_pair_rows = sum(1 for row in selected_rows if as_float(row.get("source_pair_qty")) > 0.0)
    top_group = groups[0] if groups else {}
    top_group_residual_share = as_float(top_group.get("source_residual_cost_share"))
    top_group_pair_share = as_float(top_group.get("source_pair_qty_share"))
    local_external_ids_available = any(
        str(row.get("external_shadow_ids_available", "")).lower() == "true" for row in selected_rows
    )
    local_external_ids_blank = all(
        not field_present(row, "quote_intent_id")
        and not field_present(row, "source_order_id")
        and not field_present(row, "source_sequence_id")
        for row in selected_rows
    )

    blockers: list[str] = []
    if not paths_safe:
        blockers.append("unsafe_input_or_output_path")
    if selected_coverage < args.required_selected_field_coverage_min or selected_missing_fields:
        blockers.append("selected_seed_ledger_context_required_fields_missing")
    blockers.extend(score_failures)
    if exemplar_count <= 0:
        blockers.append("exemplar_score_has_no_top_residual_tail_exemplars")
    if join_coverage < args.min_join_coverage:
        blockers.append("exemplar_to_selected_seed_join_coverage_below_threshold")
    if max_context_delta > args.max_context_abs_delta:
        blockers.append("joined_context_numeric_delta_above_threshold")
    if selected_residual_rows <= 0:
        blockers.append("selected_seed_export_has_no_residual_context_rows")
    if selected_pair_rows <= 0:
        blockers.append("selected_seed_export_has_no_pair_context_rows")
    if local_external_ids_available:
        blockers.append("local_candidate_export_unexpectedly_claims_external_shadow_ids")
    if not local_external_ids_blank:
        blockers.append("local_candidate_export_filled_external_shadow_ids_without_mapping")

    if not paths_safe or local_external_ids_available:
        decision = "DISCARD"
        status = "DISCARD_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_GUARDRAIL_FAIL"
    elif blockers:
        decision = "UNKNOWN"
        status = "UNKNOWN_RESIDUAL_TAIL_LEDGER_CONTEXT_JOIN_OR_FIELD_GAP"
    else:
        decision = "KEEP"
        status = "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_READY"

    report = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_residual_tail_ledger_context_verifier",
        "created_utc": label,
        "decision": decision,
        "decision_label": status,
        "scope": "local_no_network_residual_tail_ledger_context_verifier",
        "inputs": {
            "selected_seed_ledger_contexts": str(selected_seed_ledger_contexts),
            "exemplar_score": str(exemplar_score_path),
        },
        "checks": {
            "paths_safe": paths_safe,
            "selected_seed_rows_present": selected_count > 0,
            "selected_required_field_coverage_ok": selected_coverage >= args.required_selected_field_coverage_min
            and not selected_missing_fields,
            "exemplar_score_ready": score_ok,
            "exemplar_rows_present": exemplar_count > 0,
            "join_coverage_ok": join_coverage >= args.min_join_coverage,
            "context_numeric_delta_ok": max_context_delta <= args.max_context_abs_delta,
            "selected_residual_context_present": selected_residual_rows > 0,
            "selected_pair_context_present": selected_pair_rows > 0,
            "local_external_shadow_ids_unavailable_and_blank": (not local_external_ids_available and local_external_ids_blank),
        },
        "thresholds": {
            "required_selected_field_coverage_min": args.required_selected_field_coverage_min,
            "min_join_coverage": args.min_join_coverage,
            "max_trigger_ts_delta_ms": args.max_trigger_ts_delta_ms,
            "max_trigger_px_delta": args.max_trigger_px_delta,
            "max_trigger_size_delta": args.max_trigger_size_delta,
            "max_context_abs_delta": args.max_context_abs_delta,
        },
        "blockers": blockers,
        "selected_export_metrics": {
            "selected_row_count": selected_count,
            "required_field_coverage": round(selected_coverage, 8),
            "required_field_coverage_by_field": selected_coverage_by_field,
            "missing_required_fields": selected_missing_fields,
            "rows_with_residual_context": selected_residual_rows,
            "rows_with_pair_context": selected_pair_rows,
            "external_shadow_ids_available": local_external_ids_available,
            "external_shadow_ids_blank": local_external_ids_blank,
        },
        "join_metrics": {
            "exemplar_row_count": exemplar_count,
            "matched_exemplar_count": matched_count,
            "join_coverage": round(join_coverage, 8),
            "join_mode_counts": dict(sorted(join_mode_counts.items())),
            "max_context_abs_delta": round(max_context_delta, 12),
            "unmatched_examples": unmatched[:10],
            "matched_examples": matches[:10],
        },
        "mechanism_signal": {
            "top_source_side_offset_risk_direction": top_group.get("source_side_offset_risk_direction"),
            "top_group_residual_cost_share": top_group_residual_share,
            "top_group_pair_qty_share": top_group_pair_share,
            "sample_preserving_pair_context_available": top_group_pair_share >= args.min_pair_qty_context_share,
            "summary_only_removal_recommended": False,
            "interpretation": (
                "Joined ledger context can support local mechanism hypotheses only when sample/pair context is "
                "preserved. Side/offset/risk_direction remains diagnostic context, not a deletion rule."
            ),
        },
        "research_ranking": {
            "decision": decision,
            "label": status,
            "interpretation": "Local verifier readiness and join quality only; strategy economics and promotion gate remain separate.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "LEDGER_CONTEXT_VERIFIER_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "network_started": False,
            "ssh_started": False,
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
            "Use this verifier on a future approved residual-tail exemplar shadow pullback plus matching selected seed "
            "ledger-context export; if KEEP, design one local sample-preserving non-price ledger/context mechanism. "
            "Do not start remote/canary from verifier tooling alone."
            if decision == "KEEP"
            else "Fix exact selected/export/exemplar join gaps before proposing any new mechanism."
        ),
    }
    write_json(output_dir / "score.json", report)
    write_json(
        output_dir / "manifest.json",
        {
            "artifact": "xuan_b27_dplus_residual_tail_ledger_context_verifier_output",
            "decision": report["decision"],
            "decision_label": report["decision_label"],
            "score_json": str(output_dir / "score.json"),
            "promotion_gate": report["promotion_gate"],
            "side_effects": report["side_effects"],
            "created_utc": report["created_utc"],
        },
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selected-seed-ledger-contexts", type=Path, required=True)
    parser.add_argument("--exemplar-score", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--required-selected-field-coverage-min", type=float, default=1.0)
    parser.add_argument("--min-join-coverage", type=float, default=0.95)
    parser.add_argument("--max-trigger-ts-delta-ms", type=int, default=0)
    parser.add_argument("--max-trigger-px-delta", type=float, default=1e-9)
    parser.add_argument("--max-trigger-size-delta", type=float, default=1e-9)
    parser.add_argument("--max-context-abs-delta", type=float, default=1e-9)
    parser.add_argument("--min-pair-qty-context-share", type=float, default=0.10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = verify(
        selected_seed_ledger_contexts=args.selected_seed_ledger_contexts,
        exemplar_score_path=args.exemplar_score,
        output_dir=args.output_dir,
        args=args,
    )
    print(args.output_dir / "score.json")
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
