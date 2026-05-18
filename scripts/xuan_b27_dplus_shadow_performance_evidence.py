#!/usr/bin/env python3
"""Create local no-order shadow/dry-run performance evidence for D+.

This consumes a local observer summary plus local edge-sample JSONL. It does
not read raw/replay stores, connect to network, or touch order/cancel/redeem
paths. The output is intended as input to
``xuan_b27_dplus_rust_shadow_strategy_acceptance.py``.
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_STRATEGY_PERFORMANCE_EVIDENCE"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_OBSERVER_SUMMARY_NOT_SAFE": 2,
    "FAIL_PERFORMANCE_EVIDENCE_DECODE_ERROR": 3,
    "FAIL_INSUFFICIENT_SAMPLE_SIZE": 4,
    "FAIL_EDGE_QUALITY": 5,
    "FAIL_FORBIDDEN_SIDE_EFFECT": 6,
    "FAIL_MARKET_SCOPE": 7,
    "FAIL_EDGE_ONLY_NOT_PERFORMANCE": 8,
}


FORBIDDEN_SAMPLE_FIELDS = (
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "started_canary",
    "auth_network_started",
)

ACCEPTED_PERFORMANCE_BASES = {
    "shadow_replay_outcome",
    "independent_shadow_outcome",
    "dry_run_outcome",
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not path.exists():
        return records, [{"line": None, "error": "missing_edge_samples_file"}]
    with path.open() as fh:
        for line_no, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append({"line": line_no, "error": str(exc)})
                continue
            if not isinstance(value, dict):
                errors.append({"line": line_no, "error": "record_not_object"})
                continue
            records.append(value)
    return records, errors


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


def market_prefix_ok(markets: list[Any], prefix: str) -> bool:
    if not markets:
        return False
    return all(isinstance(market, str) and (market == prefix or market.startswith(f"{prefix}-")) for market in markets)


def sample_market_prefix_ok(samples: list[dict[str, Any]], prefix: str) -> bool:
    if not samples:
        return False
    for sample in samples:
        market = sample.get("market_slug") or sample.get("slug")
        if not isinstance(market, str) or not (market == prefix or market.startswith(f"{prefix}-")):
            return False
    return True


def observer_summary_safe(summary: dict[str, Any], market_prefix: str) -> bool:
    run_manifest = summary.get("run_manifest") or {}
    candidates = summary.get("observer_candidate_counts") or {}
    return (
        summary.get("artifact") == "xuan_b27_dplus_observer_summary"
        and summary.get("status") == "PASS_NO_ORDER_OBSERVER"
        and as_int(summary.get("decode_error_count")) == 0
        and as_int(summary.get("truncated_final_line_count")) == 0
        and as_int(summary.get("no_order_violation_count")) == 0
        and as_int(candidates.get("would_track")) > 0
        and as_int(candidates.get("would_place")) == 0
        and as_int(candidates.get("submitted_previews")) == 0
        and run_manifest.get("dry_run") is True
        and run_manifest.get("orders_allowed") is False
        and run_manifest.get("cancels_allowed") is False
        and run_manifest.get("redeems_allowed") is False
        and market_prefix_ok(summary.get("markets") or [], market_prefix)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--observer-summary", type=Path, required=True)
    parser.add_argument("--edge-samples", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-samples", type=int, default=100)
    parser.add_argument("--min-accepted-candidates", type=int, default=1)
    parser.add_argument("--min-positive-edge-ratio", type=float, default=0.5)
    parser.add_argument("--min-mean-expected-value-bps", type=float, default=0.0)
    parser.add_argument("--min-median-edge-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_performance_evidence_{label}"

    summary = read_json(args.observer_summary)
    samples, decode_errors = read_jsonl(args.edge_samples)
    summary_safe = observer_summary_safe(summary, args.market_prefix)
    sample_market_ok = sample_market_prefix_ok(samples, args.market_prefix)
    forbidden_side_effect_samples = [
        index
        for index, sample in enumerate(samples, 1)
        if any(sample.get(field) is True for field in FORBIDDEN_SAMPLE_FIELDS)
    ]
    accepted_samples = [
        sample for sample in samples if sample.get("accepted") is True or sample.get("would_track") is True
    ]
    edge_values = [
        as_float(sample.get("edge_bps", sample.get("candidate_edge_bps")))
        for sample in accepted_samples
    ]
    expected_values = [
        as_float(sample.get("expected_value_bps", sample.get("ev_bps")))
        for sample in accepted_samples
    ]
    positive_edges = [value for value in edge_values if value > 0.0]
    performance_bases = {
        str(sample.get("performance_basis"))
        for sample in accepted_samples
        if sample.get("performance_basis") is not None
    }
    performance_basis_ok = bool(performance_bases) and performance_bases.issubset(ACCEPTED_PERFORMANCE_BASES)
    sample_count = len(samples)
    accepted_count = len(accepted_samples)
    positive_count = len(positive_edges)
    positive_ratio = positive_count / accepted_count if accepted_count else 0.0
    mean_expected_value_bps = statistics.fmean(expected_values) if expected_values else 0.0
    median_edge_bps = statistics.median(edge_values) if edge_values else 0.0

    sample_size_ok = (
        sample_count >= args.min_samples
        and accepted_count >= args.min_accepted_candidates
    )
    edge_quality_ok = (
        positive_ratio >= args.min_positive_edge_ratio
        and mean_expected_value_bps >= args.min_mean_expected_value_bps
        and median_edge_bps >= args.min_median_edge_bps
    )

    failures: list[str] = []
    if not summary_safe:
        failures.append("observer_summary_not_safe")
        status = "FAIL_OBSERVER_SUMMARY_NOT_SAFE"
    elif decode_errors:
        failures.append("edge_samples_decode_error")
        status = "FAIL_PERFORMANCE_EVIDENCE_DECODE_ERROR"
    elif not sample_market_ok:
        failures.append("market_scope_failed")
        status = "FAIL_MARKET_SCOPE"
    elif forbidden_side_effect_samples:
        failures.append("forbidden_side_effect_sample")
        status = "FAIL_FORBIDDEN_SIDE_EFFECT"
    elif not performance_basis_ok:
        failures.append("edge_only_samples_are_not_strategy_performance")
        status = "FAIL_EDGE_ONLY_NOT_PERFORMANCE"
    elif not sample_size_ok:
        failures.append("insufficient_sample_size")
        status = "FAIL_INSUFFICIENT_SAMPLE_SIZE"
    elif not edge_quality_ok:
        failures.append("edge_quality_failed")
        status = "FAIL_EDGE_QUALITY"
    else:
        status = PASS_STATUS

    evidence_passed = status == PASS_STATUS
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_strategy_performance_evidence",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_order_shadow_or_replay_performance",
        "evidence_passed": evidence_passed,
        "failures": failures,
        "observer_summary": {
            "path": str(args.observer_summary),
            "exists": args.observer_summary.exists(),
            "status": summary.get("status"),
            "safe_for_performance_evidence": summary_safe,
        },
        "edge_samples": {
            "path": str(args.edge_samples),
            "exists": args.edge_samples.exists(),
            "decode_error_count": len(decode_errors),
            "decode_errors_sample": decode_errors[:5],
            "market_scope_ok": sample_market_ok,
            "forbidden_side_effect_sample_count": len(forbidden_side_effect_samples),
            "forbidden_side_effect_sample_indices": forbidden_side_effect_samples[:20],
            "performance_bases": sorted(performance_bases),
            "performance_basis_ok": performance_basis_ok,
        },
        "performance_metrics": {
            "candidate_sample_count": sample_count,
            "accepted_candidate_count": accepted_count,
            "positive_edge_candidate_count": positive_count,
            "positive_edge_ratio": positive_ratio,
            "expected_value_bps": mean_expected_value_bps,
            "median_candidate_edge_bps": median_edge_bps,
            "min_samples": args.min_samples,
            "min_accepted_candidates": args.min_accepted_candidates,
            "min_positive_edge_ratio": args.min_positive_edge_ratio,
            "min_mean_expected_value_bps": args.min_mean_expected_value_bps,
            "min_median_edge_bps": args.min_median_edge_bps,
        },
        "candidate_sample_count": sample_count,
        "accepted_candidate_count": accepted_count,
        "positive_edge_candidate_count": positive_count,
        "expected_value_bps": mean_expected_value_bps,
        "median_candidate_edge_bps": median_edge_bps,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "network_started": False,
            "ssh_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
        "next_gate": (
            "feed this evidence into Rust shadow strategy acceptance"
            if evidence_passed
            else "produce stronger no-order shadow/dry-run edge samples before strategy acceptance"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
