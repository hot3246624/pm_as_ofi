#!/usr/bin/env python3
"""Score micro-deficit signatures in D+ no-order summary artifacts.

This is a local-only reader for already-pulled runner manifest,
aggregate_report.json, and *.summary.json files. It does not read events
JSONL, raw/replay stores, sockets, SSH, or live order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUNNER_SCRIPT = "xuan_dplus_passive_passive_shadow_runner.py"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
)

REQUIRED_EXEMPLAR_FIELDS = (
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
)

SOURCE_LINK_REQUIRED_FIELDS = (
    "transition_count_by_status_side_offset_risk_direction",
    "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction",
    "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction",
    "pre_seed_same_cost_bucket_by_status_side_offset_risk_direction",
    "pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction",
    "immediate_pair_qty_bucket_by_source_side_offset_risk_direction",
    "residual_cost_by_source_side_offset_risk_direction",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return numerator / denominator


def path_is_safe(path: Path | None) -> bool:
    if path is None:
        return True
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def expand_summary_paths(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.is_dir():
            out.extend(sorted(path.glob("*.summary.json")))
        else:
            out.append(path)
    if not out:
        raise ValueError("no summary JSON files found")
    return out


def nested_add(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        key_s = str(key)
        if isinstance(value, dict):
            child = dest.setdefault(key_s, {})
            if isinstance(child, dict):
                nested_add(child, value)
            else:
                dest[key_s] = value
        elif isinstance(value, (int, float)):
            dest[key_s] = round(float(dest.get(key_s, 0.0)) + float(value), 8)
        else:
            dest[key_s] = value


def compare_nested(expected: Any, actual: Any, path: str = "") -> list[str]:
    diffs: list[str] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            child = f"{path}.{key}" if path else str(key)
            if key not in expected:
                diffs.append(f"{child}: unexpected aggregate key")
            elif key not in actual:
                diffs.append(f"{child}: missing aggregate key")
            else:
                diffs.extend(compare_nested(expected[key], actual[key], child))
        return diffs
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if abs(float(expected) - float(actual)) > 1e-6:
            diffs.append(f"{path}: expected {expected}, aggregate {actual}")
        return diffs
    if expected != actual:
        diffs.append(f"{path}: expected {expected!r}, aggregate {actual!r}")
    return diffs


def sum_numbers(obj: Any) -> float:
    if isinstance(obj, dict):
        return sum(sum_numbers(value) for value in obj.values())
    if isinstance(obj, (int, float)):
        return float(obj)
    return 0.0


def source_link_diag(obj: dict[str, Any]) -> dict[str, Any]:
    event_lite = obj.get("event_lite")
    if not isinstance(event_lite, dict):
        return {}
    diag = event_lite.get("source_link_transition_diagnostics")
    return diag if isinstance(diag, dict) else {}


def residual_tail_exemplars(obj: dict[str, Any]) -> list[dict[str, Any]]:
    event_lite = obj.get("event_lite")
    if not isinstance(event_lite, dict):
        return []
    rows = event_lite.get("source_link_residual_tail_exemplars")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def field_present(row: dict[str, Any], field: str) -> bool:
    value = row.get(field)
    return value is not None and value != ""


def coverage(rows: list[dict[str, Any]], field: str) -> float:
    return safe_ratio(sum(1 for row in rows if field_present(row, field)), len(rows))


def is_micro_deficit(row: dict[str, Any], max_deficit_qty: float, open_qty_cap: float) -> bool:
    if row.get("source_risk_direction") != "repair_or_pairing_improving":
        return False
    same_qty = as_float(row.get("pre_seed_same_qty"))
    opp_qty = as_float(row.get("pre_seed_opp_qty"))
    deficit = opp_qty - same_qty
    open_qty = same_qty + opp_qty
    return 0.0 < deficit <= max_deficit_qty + 1e-12 and open_qty <= open_qty_cap + 1e-12


def offset_bucket(value: Any) -> str:
    offset = as_float(value, default=-1.0)
    if offset < 0.0:
        return "offset_unknown"
    if offset < 30.0:
        return "offset_0_30"
    if offset < 60.0:
        return "offset_30_60"
    if offset < 90.0:
        return "offset_60_90"
    if offset < 120.0:
        return "offset_90_120"
    return "offset_gte_120"


def group_micro_deficit(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = "|".join(
            [
                str(row.get("side", "UNKNOWN") or "UNKNOWN"),
                offset_bucket(row.get("offset_s")),
                str(row.get("source_risk_direction", "UNKNOWN") or "UNKNOWN"),
            ]
        )
        group = groups.setdefault(
            key,
            {
                "source_side_offset_risk_direction": key,
                "row_count": 0,
                "source_pair_qty": 0.0,
                "source_pair_pnl": 0.0,
                "source_residual_qty": 0.0,
                "source_residual_cost": 0.0,
            },
        )
        group["row_count"] += 1
        group["source_pair_qty"] += as_float(row.get("source_pair_qty"))
        group["source_pair_pnl"] += as_float(row.get("source_pair_pnl"))
        group["source_residual_qty"] += as_float(row.get("source_residual_qty"))
        group["source_residual_cost"] += as_float(row.get("source_residual_cost"))
    total_residual_cost = sum(as_float(row["source_residual_cost"]) for row in groups.values())
    total_pair_qty = sum(as_float(row["source_pair_qty"]) for row in groups.values())
    output: list[dict[str, Any]] = []
    for group in groups.values():
        output.append(
            {
                "source_side_offset_risk_direction": group["source_side_offset_risk_direction"],
                "row_count": group["row_count"],
                "source_pair_qty": round(as_float(group["source_pair_qty"]), 8),
                "source_pair_pnl": round(as_float(group["source_pair_pnl"]), 8),
                "source_residual_qty": round(as_float(group["source_residual_qty"]), 8),
                "source_residual_cost": round(as_float(group["source_residual_cost"]), 8),
                "source_residual_cost_share_of_micro_deficit": round(
                    safe_ratio(as_float(group["source_residual_cost"]), total_residual_cost), 8
                ),
                "source_pair_qty_share_of_micro_deficit": round(
                    safe_ratio(as_float(group["source_pair_qty"]), total_pair_qty), 8
                ),
            }
        )
    output.sort(key=lambda row: (-row["source_residual_cost"], -row["source_pair_qty"], row["source_side_offset_risk_direction"]))
    return output


def no_order_safety(runner_manifest: dict[str, Any]) -> dict[str, Any]:
    if not runner_manifest:
        return {"checked": False, "passed": False, "missing": ["runner_manifest"]}
    safety = runner_manifest.get("safety") if isinstance(runner_manifest.get("safety"), dict) else {}
    checks = {
        "script_ok": runner_manifest.get("script") in (RUNNER_SCRIPT, None),
        "orders_sent_false": safety.get("orders_sent", False) is False and runner_manifest.get("orders_sent", False) is False,
        "cancels_sent_false": runner_manifest.get("cancels_sent", False) is False,
        "redeems_sent_false": runner_manifest.get("redeems_sent", False) is False,
        "started_canary_false": runner_manifest.get("started_canary", False) is False,
    }
    return {"checked": True, "passed": all(checks.values()), "checks": checks}


def build_score(args: argparse.Namespace) -> dict[str, Any]:
    paths = [args.runner_manifest, args.aggregate_report, args.output_dir] + args.summary
    unsafe = [str(path) for path in paths if not path_is_safe(path)]
    if unsafe:
        raise ValueError(f"refusing forbidden paths: {unsafe}")

    runner_manifest = load_json(args.runner_manifest)
    aggregate_report = load_json(args.aggregate_report)
    summary_paths = expand_summary_paths(args.summary)
    summaries = [load_json(path) for path in summary_paths]
    summary_diags = [source_link_diag(summary) for summary in summaries]
    summary_diags = [diag for diag in summary_diags if diag]
    aggregate_diag = source_link_diag(aggregate_report)
    merged_diag: dict[str, Any] = {}
    for diag in summary_diags:
        nested_add(merged_diag, diag)

    missing_source_link_fields = [
        field for field in SOURCE_LINK_REQUIRED_FIELDS if field not in aggregate_diag and field not in merged_diag
    ]
    parity_diffs: list[str] = []
    if aggregate_diag and merged_diag:
        for field in SOURCE_LINK_REQUIRED_FIELDS:
            if field in aggregate_diag or field in merged_diag:
                parity_diffs.extend(compare_nested(merged_diag.get(field, {}), aggregate_diag.get(field, {}), field))

    exemplar_rows: list[dict[str, Any]] = []
    for summary in summaries:
        exemplar_rows.extend(residual_tail_exemplars(summary))
    aggregate_exemplars = residual_tail_exemplars(aggregate_report)
    if aggregate_exemplars:
        exemplar_rows.extend(aggregate_exemplars)
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in exemplar_rows:
        key = (
            str(row.get("quote_intent_id", "")),
            str(row.get("source_order_id", "")),
            str(row.get("source_sequence_id", "")),
            str(row.get("condition_id", "")),
        )
        deduped[key] = row
    exemplar_rows = list(deduped.values())

    field_coverages = {field: round(coverage(exemplar_rows, field), 8) for field in REQUIRED_EXEMPLAR_FIELDS}
    missing_or_low_fields = [
        field for field, value in field_coverages.items() if value < args.min_required_field_coverage
    ]
    micro_rows = [
        row
        for row in exemplar_rows
        if is_micro_deficit(row, args.max_deficit_qty, args.open_qty_cap)
    ]
    total_residual_cost = sum(as_float(row.get("source_residual_cost")) for row in exemplar_rows)
    total_residual_qty = sum(as_float(row.get("source_residual_qty")) for row in exemplar_rows)
    total_pair_qty = sum(as_float(row.get("source_pair_qty")) for row in exemplar_rows)
    micro_residual_cost = sum(as_float(row.get("source_residual_cost")) for row in micro_rows)
    micro_residual_qty = sum(as_float(row.get("source_residual_qty")) for row in micro_rows)
    micro_pair_qty = sum(as_float(row.get("source_pair_qty")) for row in micro_rows)
    micro_pair_pnl = sum(as_float(row.get("source_pair_pnl")) for row in micro_rows)
    micro_groups = group_micro_deficit(micro_rows)

    metrics = {
        "summary_count": len(summary_paths),
        "source_link_summary_count": len(summary_diags),
        "exemplar_count": len(exemplar_rows),
        "micro_deficit_exemplar_count": len(micro_rows),
        "micro_deficit_row_share": round(safe_ratio(len(micro_rows), len(exemplar_rows)), 8),
        "micro_deficit_source_residual_cost": round(micro_residual_cost, 8),
        "micro_deficit_source_residual_cost_share": round(safe_ratio(micro_residual_cost, total_residual_cost), 8),
        "micro_deficit_source_residual_qty": round(micro_residual_qty, 8),
        "micro_deficit_source_residual_qty_share": round(safe_ratio(micro_residual_qty, total_residual_qty), 8),
        "micro_deficit_source_pair_qty": round(micro_pair_qty, 8),
        "micro_deficit_source_pair_qty_share": round(safe_ratio(micro_pair_qty, total_pair_qty), 8),
        "micro_deficit_source_pair_pnl": round(micro_pair_pnl, 8),
        "top_micro_deficit_group_residual_cost_share": round(
            as_float(micro_groups[0]["source_residual_cost_share_of_micro_deficit"]) if micro_groups else 0.0,
            8,
        ),
        "source_sequence_coverage": round(coverage(exemplar_rows, "source_sequence_id"), 8),
        "quote_intent_coverage": round(coverage(exemplar_rows, "quote_intent_id"), 8),
        "source_order_coverage": round(coverage(exemplar_rows, "source_order_id"), 8),
    }

    blockers: list[str] = []
    if not no_order_safety(runner_manifest)["passed"]:
        blockers.append("no_order_safety_not_confirmed")
    if missing_source_link_fields:
        blockers.append("source_link_required_fields_missing")
    if parity_diffs:
        blockers.append("source_link_aggregate_parity_failed")
    if not exemplar_rows:
        blockers.append("residual_tail_exemplars_missing")
    if missing_or_low_fields:
        blockers.append("residual_tail_required_field_coverage_below_threshold")
    if metrics["source_sequence_coverage"] < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_threshold")
    if metrics["micro_deficit_exemplar_count"] < args.min_micro_deficit_count:
        blockers.append("micro_deficit_exemplar_count_below_threshold")
    if metrics["micro_deficit_source_residual_cost_share"] < args.min_micro_deficit_residual_cost_share:
        blockers.append("micro_deficit_residual_cost_share_below_threshold")

    if blockers:
        decision = "UNKNOWN"
        label = "UNKNOWN_MICRO_DEFICIT_NO_ORDER_SUMMARY_INPUTS_INSUFFICIENT"
        next_action = (
            "Do not start remote/canary from this scorer output. If this came from a completed approved no-order "
            "run, name missing fields and decide whether instrumentation must be extended."
        )
    else:
        decision = "KEEP"
        label = "KEEP_MICRO_DEFICIT_NO_ORDER_SUMMARY_SCORER_READY"
        next_action = (
            "Use this scorer after an approved micro-deficit no-order shadow pullback to decide whether one local "
            "runner implementation target can be proposed; do not treat the score as private truth or promotion."
        )

    return {
        "artifact": "xuan_b27_dplus_micro_deficit_no_order_summary_scorer",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only_summary_reader": True,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected_or_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "runner_manifest": str(args.runner_manifest),
            "aggregate_report": str(args.aggregate_report),
            "summary_count": len(summary_paths),
        },
        "contract": {
            "micro_deficit_predicate": {
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_opp_qty_minus_same_qty_min_exclusive": 0.0,
                "pre_seed_opp_qty_minus_same_qty_max_inclusive": args.max_deficit_qty,
                "pre_seed_same_plus_opp_qty_max_inclusive": args.open_qty_cap,
            },
            "post_action_labels_not_live_criteria": [
                "source_pair_qty",
                "source_pair_cost",
                "source_pair_pnl",
                "source_residual_qty",
                "source_residual_cost",
                "source_residual_age_ms",
            ],
            "source_link_exact_micro_deficit_volume_supported": False,
            "exact_micro_deficit_signature_source": "source_link_residual_tail_exemplars",
        },
        "no_order_safety": no_order_safety(runner_manifest),
        "source_link_checks": {
            "summary_with_source_link_count": len(summary_diags),
            "missing_required_fields": missing_source_link_fields,
            "aggregate_parity_passed": not parity_diffs,
            "aggregate_parity_diff_count": len(parity_diffs),
            "aggregate_parity_diffs_sample": parity_diffs[:10],
        },
        "exemplar_field_coverage": field_coverages,
        "metrics": metrics,
        "rankings": {
            "top_micro_deficit_groups": micro_groups[:10],
        },
        "thresholds": {
            "min_required_field_coverage": args.min_required_field_coverage,
            "min_source_sequence_coverage": args.min_source_sequence_coverage,
            "min_micro_deficit_count": args.min_micro_deficit_count,
            "min_micro_deficit_residual_cost_share": args.min_micro_deficit_residual_cost_share,
        },
        "blockers": blockers,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "KEEP means the no-order summary surfaces are sufficient to review the micro-deficit signature. "
                "It is not strategy economics promotion, private truth, deployable evidence, canary evidence, "
                "or a live trading rule."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SUMMARY_SCORER_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-manifest", type=Path, required=True)
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--summary", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-deficit-qty", type=float, default=0.25)
    parser.add_argument("--open-qty-cap", type=float, default=1.0)
    parser.add_argument("--min-required-field-coverage", type=float, default=0.95)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    parser.add_argument("--min-micro-deficit-count", type=int, default=3)
    parser.add_argument("--min-micro-deficit-residual-cost-share", type=float, default=0.20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_score(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 3


if __name__ == "__main__":
    raise SystemExit(main())
