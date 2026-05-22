#!/usr/bin/env python3
"""Score D+ residual-tail exemplar no-order shadow summaries.

This reader only consumes local runner manifest, aggregate_report.json,
summary JSON, and optional source-link score artifacts. It does not read
events JSONL, replay/raw stores, sockets, SSH, or live order paths.
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
OPTIONAL_EXEMPLAR_FIELDS = (
    "trigger_ts_ms",
    "lot_id",
    "seed_px",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path | None) -> dict[str, Any]:
    if not path:
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


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, default))


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
    return out


def event_lite_exemplars(obj: dict[str, Any]) -> list[dict[str, Any]]:
    event_lite = obj.get("event_lite")
    if not isinstance(event_lite, dict):
        return []
    rows = event_lite.get("source_link_residual_tail_exemplars")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def exemplar_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("quote_intent_id", "")),
        str(row.get("source_order_id", "")),
        str(row.get("lot_id", "")),
    )


def field_present(row: dict[str, Any], field: str) -> bool:
    if field not in row:
        return False
    value = row.get(field)
    return value is not None and value != ""


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


def count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        label = str(row.get(key, "UNKNOWN") or "UNKNOWN")
        counts[label] = counts.get(label, 0) + 1
    return dict(sorted(counts.items()))


def concentration(values: list[float], total: float, limit: int) -> float:
    if total <= 0.0:
        return 0.0
    return sum(sorted(values, reverse=True)[:limit]) / total


def group_exemplars(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        side = str(row.get("side", "UNKNOWN") or "UNKNOWN")
        bucket = offset_bucket(row.get("offset_s"))
        risk = str(row.get("source_risk_direction", "UNKNOWN") or "UNKNOWN")
        key = f"{side}|{bucket}|{risk}"
        group = groups.setdefault(
            key,
            {
                "source_side_offset_risk_direction": key,
                "side": side,
                "offset_bucket": bucket,
                "risk_direction": risk,
                "row_count": 0,
                "source_pair_qty": 0.0,
                "source_pair_cost": 0.0,
                "source_pair_pnl": 0.0,
                "source_residual_qty": 0.0,
                "source_residual_cost": 0.0,
                "quote_intent_ids": set(),
                "source_sequence_ids": set(),
            },
        )
        group["row_count"] += 1
        group["source_pair_qty"] += as_float(row.get("source_pair_qty"))
        group["source_pair_cost"] += as_float(row.get("source_pair_cost"))
        group["source_pair_pnl"] += as_float(row.get("source_pair_pnl"))
        group["source_residual_qty"] += as_float(row.get("source_residual_qty"))
        group["source_residual_cost"] += as_float(row.get("source_residual_cost"))
        if field_present(row, "quote_intent_id"):
            group["quote_intent_ids"].add(str(row["quote_intent_id"]))
        if field_present(row, "source_sequence_id"):
            group["source_sequence_ids"].add(str(row["source_sequence_id"]))

    total_pair_qty = sum(as_float(group["source_pair_qty"]) for group in groups.values())
    total_residual_cost = sum(as_float(group["source_residual_cost"]) for group in groups.values())
    output: list[dict[str, Any]] = []
    for group in groups.values():
        pair_qty = as_float(group["source_pair_qty"])
        residual_cost = as_float(group["source_residual_cost"])
        row = {
            "source_side_offset_risk_direction": group["source_side_offset_risk_direction"],
            "side": group["side"],
            "offset_bucket": group["offset_bucket"],
            "risk_direction": group["risk_direction"],
            "row_count": group["row_count"],
            "source_pair_qty": round(pair_qty, 6),
            "source_pair_cost": round(as_float(group["source_pair_cost"]), 6),
            "source_pair_pnl": round(as_float(group["source_pair_pnl"]), 6),
            "source_residual_qty": round(as_float(group["source_residual_qty"]), 6),
            "source_residual_cost": round(residual_cost, 6),
            "source_pair_qty_share": round(safe_ratio(pair_qty, total_pair_qty), 6),
            "source_residual_cost_share": round(safe_ratio(residual_cost, total_residual_cost), 6),
            "quote_intent_count": len(group["quote_intent_ids"]),
            "source_sequence_count": len(group["source_sequence_ids"]),
        }
        row["summary_only_removal_refused"] = bool(
            row["source_residual_cost_share"] > 0.0 and row["source_pair_qty_share"] >= 0.10
        )
        output.append(row)
    output.sort(
        key=lambda row: (
            -as_float(row["source_residual_cost"]),
            -as_float(row["source_pair_qty"]),
            row["source_side_offset_risk_direction"],
        )
    )
    return output


def no_order_safety(runner_manifest: dict[str, Any]) -> dict[str, Any]:
    if not runner_manifest:
        return {
            "checked": False,
            "passed": False,
            "missing": ["runner_manifest"],
        }
    safety = runner_manifest.get("safety") if isinstance(runner_manifest.get("safety"), dict) else {}
    checks = {
        "script_ok": runner_manifest.get("script") == RUNNER_SCRIPT,
        "orders_sent_false": safety.get("orders_sent") is False and runner_manifest.get("orders_sent", False) is False,
        "cancels_sent_false": runner_manifest.get("cancels_sent", False) is False,
        "redeems_sent_false": runner_manifest.get("redeems_sent", False) is False,
        "started_canary_false": runner_manifest.get("started_canary", False) is False,
    }
    return {
        "checked": True,
        "passed": all(checks.values()),
        "checks": checks,
        "dry_run": safety.get("dry_run"),
        "shared_ingress_role": safety.get("shared_ingress_role"),
        "shared_ingress_root": safety.get("shared_ingress_root"),
    }


def metric_gates(metrics: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    candidates = as_int(metrics.get("candidates"))
    filled_qty = as_float(metrics.get("filled_qty"))
    filled_cost = as_float(metrics.get("filled_cost"))
    pair_qty = as_float(metrics.get("pair_qty"))
    pair_pnl = as_float(metrics.get("pair_pnl"))
    taker_fee = as_float(metrics.get("taker_fee"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    material_residual_lots = as_int(metrics.get("material_residual_lots"))
    net_pair_cost_p90 = as_float(
        metrics.get("net_pair_cost_proxy_p90", metrics.get("net_pair_cost_p90")),
        default=0.0,
    )
    residual_qty_share = safe_ratio(residual_qty, filled_qty)
    residual_cost_share = safe_ratio(residual_cost, filled_cost)
    fee_adjusted_pair_pnl_proxy = pair_pnl - taker_fee
    pair_tail_loss_proxy = max(net_pair_cost_p90 - 1.0, 0.0) * max(pair_qty, 0.0) * args.pair_tail_loss_fraction
    pair_tail_loss_share = (
        float("inf") if pair_tail_loss_proxy > 0.0 and fee_adjusted_pair_pnl_proxy <= 0.0
        else safe_ratio(pair_tail_loss_proxy, fee_adjusted_pair_pnl_proxy)
    )
    failures: list[dict[str, Any]] = []
    if candidates < args.min_candidates:
        failures.append({"metric": "candidates", "actual": candidates, "required_min": args.min_candidates})
    if net_pair_cost_p90 > args.max_net_pair_cost_p90:
        failures.append(
            {"metric": "net_pair_cost_p90", "actual": net_pair_cost_p90, "required_max": args.max_net_pair_cost_p90}
        )
    if residual_qty_share > args.max_residual_qty_share_of_filled:
        failures.append(
            {
                "metric": "residual_qty_share_of_filled",
                "actual": round(residual_qty_share, 8),
                "required_max": args.max_residual_qty_share_of_filled,
            }
        )
    if residual_cost_share > args.max_residual_cost_share_of_filled_cost:
        failures.append(
            {
                "metric": "residual_cost_share_of_filled_cost",
                "actual": round(residual_cost_share, 8),
                "required_max": args.max_residual_cost_share_of_filled_cost,
            }
        )
    if pair_tail_loss_share > args.max_pair_tail_loss_share_of_pair_pnl:
        failures.append(
            {
                "metric": "pair_tail_loss_share_of_pair_pnl",
                "actual": "inf" if pair_tail_loss_share == float("inf") else round(pair_tail_loss_share, 8),
                "required_max": args.max_pair_tail_loss_share_of_pair_pnl,
            }
        )
    if material_residual_lots > args.max_material_residual_lots:
        failures.append(
            {
                "metric": "material_residual_lots",
                "actual": material_residual_lots,
                "required_max": args.max_material_residual_lots,
            }
        )
    return {
        "passed": not failures,
        "failures": failures,
        "metrics": {
            "candidates": candidates,
            "filled_qty": round(filled_qty, 6),
            "filled_cost": round(filled_cost, 6),
            "pair_qty": round(pair_qty, 6),
            "pair_pnl": round(pair_pnl, 6),
            "taker_fee": round(taker_fee, 6),
            "fee_adjusted_pair_pnl_proxy": round(fee_adjusted_pair_pnl_proxy, 6),
            "net_pair_cost_p90": net_pair_cost_p90,
            "residual_qty": round(residual_qty, 6),
            "residual_cost": round(residual_cost, 6),
            "residual_qty_share_of_filled": round(residual_qty_share, 8),
            "residual_cost_share_of_filled_cost": round(residual_cost_share, 8),
            "material_residual_lots": material_residual_lots,
            "pair_tail_loss_proxy": round(pair_tail_loss_proxy, 6),
            "pair_tail_loss_share_of_pair_pnl": (
                "inf" if pair_tail_loss_share == float("inf") else round(pair_tail_loss_share, 8)
            ),
        },
    }


def score(
    *,
    aggregate_path: Path,
    runner_manifest_path: Path | None,
    source_link_score_path: Path | None,
    summary_paths: list[Path],
    args: argparse.Namespace,
) -> dict[str, Any]:
    label = utc_label()
    paths_safe = all(
        path_is_safe(path)
        for path in [aggregate_path, runner_manifest_path, source_link_score_path, *summary_paths]
    )
    aggregate = load_json(aggregate_path)
    runner_manifest = load_json(runner_manifest_path) if runner_manifest_path else {}
    source_link_score = load_json(source_link_score_path) if source_link_score_path else {}
    summaries = [load_json(path) for path in summary_paths]
    aggregate_rows = event_lite_exemplars(aggregate)
    summary_rows = [row for summary in summaries for row in event_lite_exemplars(summary)]
    row_count = len(aggregate_rows)
    summary_row_count = len(summary_rows)

    field_denominator = max(row_count * len(REQUIRED_EXEMPLAR_FIELDS), 1)
    present_pairs = sum(1 for row in aggregate_rows for field in REQUIRED_EXEMPLAR_FIELDS if field_present(row, field))
    field_coverage = present_pairs / field_denominator if row_count else 0.0
    field_coverage_by_field = {
        field: round(
            safe_ratio(sum(1 for row in aggregate_rows if field_present(row, field)), row_count),
            8,
        )
        for field in REQUIRED_EXEMPLAR_FIELDS
    }
    optional_field_coverage_by_field = {
        field: round(
            safe_ratio(sum(1 for row in aggregate_rows if field_present(row, field)), row_count),
            8,
        )
        for field in OPTIONAL_EXEMPLAR_FIELDS
    }
    missing_required_fields = [
        field for field, coverage in field_coverage_by_field.items() if coverage < args.required_field_coverage_min
    ]
    source_sequence_coverage = safe_ratio(
        sum(1 for row in aggregate_rows if field_present(row, "source_sequence_id")),
        row_count,
    )
    source_order_coverage = safe_ratio(
        sum(1 for row in aggregate_rows if field_present(row, "source_order_id")),
        row_count,
    )
    quote_intent_coverage = safe_ratio(
        sum(1 for row in aggregate_rows if field_present(row, "quote_intent_id")),
        row_count,
    )

    summary_key_set = {exemplar_key(row) for row in summary_rows}
    aggregate_subset_missing = [
        exemplar_key(row) for row in aggregate_rows if summary_paths and exemplar_key(row) not in summary_key_set
    ]
    aggregate_subset_ok = not summary_paths or not aggregate_subset_missing

    residual_costs = [max(as_float(row.get("source_residual_cost")), 0.0) for row in aggregate_rows]
    total_exemplar_residual_cost = sum(residual_costs)
    total_exemplar_pair_qty = sum(max(as_float(row.get("source_pair_qty")), 0.0) for row in aggregate_rows)
    group_rows = group_exemplars(aggregate_rows)
    summary_only_refusal_examples = [row for row in group_rows if row["summary_only_removal_refused"]][:5]

    report_shape_ok = (
        aggregate.get("kind") == "aggregate_report"
        and aggregate.get("script") == RUNNER_SCRIPT
        and row_count > 0
    )
    safety = no_order_safety(runner_manifest)
    metric_report = metric_gates(aggregate.get("metrics", {}) if isinstance(aggregate.get("metrics"), dict) else {}, args)
    source_link_score_ok = (
        bool(source_link_score)
        and source_link_score.get("aggregate_parity", {}).get("passed") is True
        and not source_link_score.get("guardrails", {}).get("missing_cross_bucket_fields", [])
    )
    source_link_missing = [] if source_link_score_ok else ["source_link_score.aggregate_parity_or_cross_bucket_fields"]

    field_failures: list[dict[str, Any]] = []
    if not paths_safe:
        field_failures.append({"field": "input_paths", "reason": "forbidden_path_fragment"})
    if not report_shape_ok:
        field_failures.append({"field": "aggregate_report", "reason": "missing aggregate kind/script or exemplar rows"})
    if not summary_paths:
        field_failures.append({"field": "summary_paths", "reason": "no summary JSON paths supplied for aggregate subset check"})
    if not aggregate_subset_ok:
        field_failures.append(
            {
                "field": "aggregate_summary_subset",
                "reason": "aggregate exemplars not present in supplied summaries",
                "missing_examples": [list(item) for item in aggregate_subset_missing[:5]],
            }
        )
    if field_coverage < args.required_field_coverage_min or missing_required_fields:
        field_failures.append(
            {
                "field": "source_link_residual_tail_exemplars",
                "reason": "required field coverage below threshold",
                "required_min": args.required_field_coverage_min,
                "actual": round(field_coverage, 8),
                "missing_required_fields": missing_required_fields,
            }
        )
    if source_sequence_coverage < args.source_sequence_coverage_min:
        field_failures.append(
            {
                "field": "source_sequence_id",
                "reason": "top residual exemplar source_sequence coverage below threshold",
                "required_min": args.source_sequence_coverage_min,
                "actual": round(source_sequence_coverage, 8),
            }
        )

    shadow_gate_failures = []
    if not safety["passed"]:
        shadow_gate_failures.append({"gate": "no_order_safety", "details": safety})
    if not source_link_score_ok:
        shadow_gate_failures.append({"gate": "source_link_scorer", "missing": source_link_missing})
    shadow_gate_failures.extend({"gate": "metric_budget", **failure} for failure in metric_report["failures"])

    scorer_ready = report_shape_ok and not field_failures and aggregate_subset_ok
    review_passed = scorer_ready and not shadow_gate_failures
    if review_passed:
        status = "KEEP_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_PASS"
        decision = "KEEP"
        next_action = "Use exemplar concentration to select one local sample-preserving mechanism; do not claim private truth or promotion."
    elif field_failures:
        status = "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_FIELD_OR_PARITY_GAP"
        decision = "UNKNOWN"
        next_action = "Do not start another shadow; fix missing residual-tail exemplar fields or pullback summaries first."
    else:
        status = "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF"
        decision = "UNKNOWN"
        next_action = "Do not promote; use exemplar concentration only for local mechanism selection if it is sample-preserving."

    top_exemplars = sorted(
        aggregate_rows,
        key=lambda row: (
            -as_float(row.get("source_residual_cost")),
            -as_float(row.get("source_residual_qty")),
            str(row.get("quote_intent_id", "")),
        ),
    )[:10]
    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_residual_tail_exemplar_shadow_scorer",
        "created_utc": label,
        "status": status,
        "decision": decision,
        "scope": "local_no_network_residual_tail_exemplar_shadow_review",
        "source_tool": f"tools/{RUNNER_SCRIPT}",
        "inputs": {
            "aggregate_report": str(aggregate_path),
            "runner_manifest": str(runner_manifest_path) if runner_manifest_path else None,
            "source_link_score": str(source_link_score_path) if source_link_score_path else None,
            "summaries": [str(path) for path in summary_paths],
        },
        "checks": {
            "paths_safe": paths_safe,
            "report_shape_ok": report_shape_ok,
            "summary_paths_present": bool(summary_paths),
            "aggregate_exemplars_subset_of_summaries": aggregate_subset_ok,
            "no_order_safety_ok": safety["passed"],
            "source_link_scorer_ok": source_link_score_ok,
            "metric_budgets_ok": metric_report["passed"],
            "required_field_coverage_ok": field_coverage >= args.required_field_coverage_min and not missing_required_fields,
            "source_sequence_coverage_ok": source_sequence_coverage >= args.source_sequence_coverage_min,
        },
        "thresholds": {
            "min_candidates": args.min_candidates,
            "max_net_pair_cost_p90": args.max_net_pair_cost_p90,
            "max_residual_qty_share_of_filled": args.max_residual_qty_share_of_filled,
            "max_residual_cost_share_of_filled_cost": args.max_residual_cost_share_of_filled_cost,
            "max_pair_tail_loss_share_of_pair_pnl": args.max_pair_tail_loss_share_of_pair_pnl,
            "max_material_residual_lots": args.max_material_residual_lots,
            "required_field_coverage_min": args.required_field_coverage_min,
            "source_sequence_coverage_min": args.source_sequence_coverage_min,
            "pair_tail_loss_fraction": args.pair_tail_loss_fraction,
        },
        "field_failures": field_failures,
        "shadow_gate_failures": shadow_gate_failures,
        "metric_budget": metric_report,
        "exemplar_metrics": {
            "aggregate_exemplar_row_count": row_count,
            "summary_exemplar_row_count": summary_row_count,
            "required_field_coverage": round(field_coverage, 8),
            "required_field_coverage_by_field": field_coverage_by_field,
            "optional_field_coverage_by_field": optional_field_coverage_by_field,
            "source_sequence_coverage": round(source_sequence_coverage, 8),
            "source_order_coverage": round(source_order_coverage, 8),
            "quote_intent_coverage": round(quote_intent_coverage, 8),
            "side_counts": count_by(aggregate_rows, "side"),
            "risk_direction_counts": count_by(aggregate_rows, "source_risk_direction"),
            "total_exemplar_source_residual_cost": round(total_exemplar_residual_cost, 6),
            "total_exemplar_source_pair_qty": round(total_exemplar_pair_qty, 6),
            "top1_residual_cost_share": round(concentration(residual_costs, total_exemplar_residual_cost, 1), 6),
            "top3_residual_cost_share": round(concentration(residual_costs, total_exemplar_residual_cost, 3), 6),
            "top5_residual_cost_share": round(concentration(residual_costs, total_exemplar_residual_cost, 5), 6),
        },
        "rankings": {
            "top_residual_tail_exemplars": top_exemplars,
            "top_source_side_offset_risk_direction_groups": group_rows[:10],
        },
        "guardrails": {
            "summary_only_removal_recommended": False,
            "summary_only_removal_refusal_count": len(summary_only_refusal_examples),
            "summary_only_removal_refusal_examples": summary_only_refusal_examples,
            "interpretation": (
                "Residual-tail exemplars can nominate local mechanism hypotheses. They must not be converted into "
                "after-the-fact summary removal, price caps, static side/offset cutoffs, or promotion evidence."
            ),
        },
        "research_ranking": {
            "decision": decision,
            "label": status,
            "scorer_ready": scorer_ready,
            "review_passed": review_passed,
            "interpretation": (
                "This is no-order shadow-review evidence only. Even a KEEP review requires a later local mechanism "
                "verifier before another exact remote run, and still does not prove private truth."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "shadow_runner_started": False,
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
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--runner-manifest", type=Path)
    parser.add_argument("--source-link-score", type=Path)
    parser.add_argument("--summary", action="append", type=Path, default=[])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-candidates", type=int, default=100)
    parser.add_argument("--max-net-pair-cost-p90", type=float, default=1.0)
    parser.add_argument("--max-residual-qty-share-of-filled", type=float, default=0.15)
    parser.add_argument("--max-residual-cost-share-of-filled-cost", type=float, default=0.20)
    parser.add_argument("--max-pair-tail-loss-share-of-pair-pnl", type=float, default=0.05)
    parser.add_argument("--max-material-residual-lots", type=int, default=0)
    parser.add_argument("--required-field-coverage-min", type=float, default=1.0)
    parser.add_argument("--source-sequence-coverage-min", type=float, default=0.95)
    parser.add_argument("--pair-tail-loss-fraction", type=float, default=0.10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary_paths = expand_summary_paths(args.summary)
    report = score(
        aggregate_path=args.aggregate_report,
        runner_manifest_path=args.runner_manifest,
        source_link_score_path=args.source_link_score,
        summary_paths=summary_paths,
        args=args,
    )
    write_json(args.output_dir / "score.json", report)
    write_json(
        args.output_dir / "manifest.json",
        {
            "artifact": "xuan_b27_dplus_residual_tail_exemplar_shadow_scorer_output",
            "status": report["status"],
            "decision": report["decision"],
            "score_json": str(args.output_dir / "score.json"),
            "promotion_gate": report["promotion_gate"],
            "side_effects": report["side_effects"],
            "created_utc": report["created_utc"],
        },
    )
    print(args.output_dir / "score.json")
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
