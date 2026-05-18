#!/usr/bin/env python3
"""Evaluate local Rust no-order shadow/dry-run strategy acceptance evidence.

This gate reads only local JSON artifacts. It does not start observers, connect
to network, inspect raw/replay stores, or touch any order/cancel/redeem path.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_NO_ORDER_SAFETY": 2,
    "FAIL_MISSING_STRATEGY_PERFORMANCE_EVIDENCE": 3,
    "FAIL_CANDIDATE_QUALITY": 4,
    "FAIL_SOURCE_TRUTH_UNKNOWN_HANDLING": 5,
    "FAIL_RECORDER_COMPLETENESS": 6,
    "FAIL_STRATEGY_PERFORMANCE_EVIDENCE": 7,
    "FAIL_OBSERVER_SUMMARY_MISSING": 8,
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return default
    return default


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


def first_number(data: dict[str, Any], keys: tuple[str, ...], default: float = 0.0) -> float:
    for key in keys:
        if key in data:
            return as_float(data.get(key), default)
    return default


def false_side_effects(data: dict[str, Any], fields: tuple[str, ...]) -> bool:
    side_effects = data.get("side_effects") or {}
    return all(data.get(field, False) is False for field in fields) and all(
        side_effects.get(field, False) is False for field in fields
    )


def market_prefix_ok(markets: list[Any], prefix: str) -> bool:
    if not markets:
        return False
    return all(isinstance(market, str) and (market == prefix or market.startswith(f"{prefix}-")) for market in markets)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--observer-summary", type=Path, required=True)
    parser.add_argument("--performance-evidence", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-observer-ticks", type=int, default=100_000)
    parser.add_argument("--min-elapsed-seconds", type=float, default=1_800.0)
    parser.add_argument("--min-market-count", type=int, default=1)
    parser.add_argument("--min-trace-previews", type=int, default=1)
    parser.add_argument("--min-would-track", type=int, default=1)
    parser.add_argument("--min-track-ratio", type=float, default=0.5)
    parser.add_argument("--min-performance-samples", type=int, default=100)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_rust_shadow_strategy_acceptance_{label}"

    summary = read_json(args.observer_summary)
    performance = read_json(args.performance_evidence)

    run_manifest = summary.get("run_manifest") or {}
    candidate_counts = summary.get("observer_candidate_counts") or {}
    event_counts = summary.get("event_counts") or {}
    source_truth_counts = summary.get("source_truth_verdict_counts") or {}
    markets = summary.get("markets") or []

    trace_previews = as_int(candidate_counts.get("trace_previews"))
    would_track = as_int(candidate_counts.get("would_track"))
    would_place = as_int(candidate_counts.get("would_place"))
    submitted_previews = as_int(candidate_counts.get("submitted_previews"))
    track_ratio = (would_track / trace_previews) if trace_previews else 0.0
    observer_ticks = as_int(event_counts.get("xuan_b27_dplus_observer_tick"))
    elapsed_seconds = as_float(run_manifest.get("elapsed_seconds"))

    observer_summary_present = (
        args.observer_summary.exists()
        and summary.get("artifact") == "xuan_b27_dplus_observer_summary"
        and "_read_error" not in summary
    )
    no_order_safety_ok = (
        summary.get("status") == "PASS_NO_ORDER_OBSERVER"
        and as_int(summary.get("no_order_violation_count")) == 0
        and submitted_previews == 0
        and would_place == 0
        and run_manifest.get("dry_run") is True
        and run_manifest.get("orders_allowed") is False
        and run_manifest.get("cancels_allowed") is False
        and run_manifest.get("redeems_allowed") is False
    )
    recorder_completeness_ok = (
        as_int(summary.get("jsonl_file_count")) > 0
        and as_int(summary.get("decode_error_count")) == 0
        and as_int(summary.get("truncated_final_line_count")) == 0
        and observer_ticks >= args.min_observer_ticks
        and elapsed_seconds >= args.min_elapsed_seconds
        and len(markets) >= args.min_market_count
        and market_prefix_ok(markets, args.market_prefix)
    )
    candidate_quality_ok = (
        trace_previews >= args.min_trace_previews
        and would_track >= args.min_would_track
        and track_ratio >= args.min_track_ratio
    )
    source_truth_unknown_handled = (
        as_int(source_truth_counts.get("UNKNOWN")) > 0
        and as_int(source_truth_counts.get("FAIL")) == 0
        and no_order_safety_ok
    )

    performance_path_exists = bool(args.performance_evidence and args.performance_evidence.exists())
    perf_side_effects_ok = false_side_effects(
        performance,
        (
            "orders_sent",
            "cancels_sent",
            "redeems_sent",
            "auth_network_started",
            "started_canary",
            "started_observer",
            "started_user_ws",
        ),
    )
    performance_sample_count = first_number(
        performance,
        ("candidate_sample_count", "shadow_candidate_sample_count", "sample_count"),
    )
    accepted_candidate_count = first_number(performance, ("accepted_candidate_count", "would_track_count"))
    positive_edge_candidate_count = first_number(performance, ("positive_edge_candidate_count", "positive_ev_count"))
    expected_value_bps = first_number(performance, ("expected_value_bps", "median_expected_value_bps"))
    median_candidate_edge_bps = first_number(performance, ("median_candidate_edge_bps", "median_edge_bps"))
    performance_evidence_ok = (
        performance_path_exists
        and performance.get("artifact") == "xuan_b27_dplus_shadow_strategy_performance_evidence"
        and performance.get("status") == "PASS_STRATEGY_PERFORMANCE_EVIDENCE"
        and performance.get("strategy") == "xuan_b27_dplus"
        and performance.get("scope") in {
            "local_no_order_shadow_or_replay_performance",
            "rust_no_order_shadow_or_dry_run_performance",
        }
        and performance.get("evidence_passed") is True
        and performance_sample_count >= args.min_performance_samples
        and accepted_candidate_count > 0
        and positive_edge_candidate_count > 0
        and expected_value_bps >= 0.0
        and median_candidate_edge_bps >= 0.0
        and perf_side_effects_ok
    )

    failures: list[str] = []
    if not observer_summary_present:
        failures.append("observer_summary_missing_or_unreadable")
        status = "FAIL_OBSERVER_SUMMARY_MISSING"
    elif not no_order_safety_ok:
        failures.append("no_order_safety_failed")
        status = "FAIL_NO_ORDER_SAFETY"
    elif not recorder_completeness_ok:
        failures.append("recorder_completeness_failed")
        status = "FAIL_RECORDER_COMPLETENESS"
    elif not source_truth_unknown_handled:
        failures.append("source_truth_unknown_handling_not_observed")
        status = "FAIL_SOURCE_TRUTH_UNKNOWN_HANDLING"
    elif not candidate_quality_ok:
        failures.append("candidate_quality_failed")
        status = "FAIL_CANDIDATE_QUALITY"
    elif not performance_path_exists:
        failures.append("strategy_performance_evidence_missing")
        status = "FAIL_MISSING_STRATEGY_PERFORMANCE_EVIDENCE"
    elif not performance_evidence_ok:
        failures.append("strategy_performance_evidence_failed")
        status = "FAIL_STRATEGY_PERFORMANCE_EVIDENCE"
    else:
        status = PASS_STATUS

    strategy_acceptance_passed = status == PASS_STATUS
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_rust_shadow_strategy_acceptance",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "rust_no_order_shadow_or_dry_run_strategy_acceptance",
        "strategy_acceptance_passed": strategy_acceptance_passed,
        "failures": failures,
        "observer_summary": {
            "path": str(args.observer_summary),
            "exists": args.observer_summary.exists(),
            "status": summary.get("status"),
            "run_dir": summary.get("run_dir"),
            "dry_run": run_manifest.get("dry_run"),
            "elapsed_seconds": elapsed_seconds,
            "orders_allowed": run_manifest.get("orders_allowed"),
            "cancels_allowed": run_manifest.get("cancels_allowed"),
            "redeems_allowed": run_manifest.get("redeems_allowed"),
        },
        "strategy_metrics": {
            "market_prefix": args.market_prefix,
            "market_count": len(markets),
            "observer_tick_count": observer_ticks,
            "jsonl_file_count": as_int(summary.get("jsonl_file_count")),
            "decode_error_count": as_int(summary.get("decode_error_count")),
            "truncated_final_line_count": as_int(summary.get("truncated_final_line_count")),
            "trace_previews": trace_previews,
            "would_track": would_track,
            "would_place": would_place,
            "submitted_previews": submitted_previews,
            "track_ratio": track_ratio,
            "source_truth_unknown_count": as_int(source_truth_counts.get("UNKNOWN")),
            "source_truth_fail_count": as_int(source_truth_counts.get("FAIL")),
            "no_order_violation_count": as_int(summary.get("no_order_violation_count")),
        },
        "acceptance_checks": {
            "observer_summary_present": observer_summary_present,
            "no_order_safety_ok": no_order_safety_ok,
            "recorder_completeness_ok": recorder_completeness_ok,
            "candidate_quality_ok": candidate_quality_ok,
            "source_truth_unknown_handled": source_truth_unknown_handled,
            "performance_evidence_present": performance_path_exists,
            "performance_evidence_ok": performance_evidence_ok,
        },
        "performance_evidence": {
            "path": str(args.performance_evidence) if args.performance_evidence else None,
            "exists": performance_path_exists,
            "artifact": performance.get("artifact"),
            "status": performance.get("status"),
            "scope": performance.get("scope"),
            "evidence_passed": performance.get("evidence_passed"),
            "candidate_sample_count": performance_sample_count,
            "accepted_candidate_count": accepted_candidate_count,
            "positive_edge_candidate_count": positive_edge_candidate_count,
            "expected_value_bps": expected_value_bps,
            "median_candidate_edge_bps": median_candidate_edge_bps,
            "side_effects_ok": perf_side_effects_ok,
        },
        "requirements": {
            "min_observer_ticks": args.min_observer_ticks,
            "min_elapsed_seconds": args.min_elapsed_seconds,
            "min_trace_previews": args.min_trace_previews,
            "min_would_track": args.min_would_track,
            "min_track_ratio": args.min_track_ratio,
            "min_performance_samples": args.min_performance_samples,
        },
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
            "network_started": False,
            "ssh_started": False,
        },
        "next_gate": (
            "strategy acceptance evidence is sufficient for canary readiness plumbing review"
            if strategy_acceptance_passed
            else "produce real no-order shadow/dry-run strategy performance evidence before G2 canary"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
