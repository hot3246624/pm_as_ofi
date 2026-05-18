#!/usr/bin/env python3
"""Bridge local D+ observer edge samples to realized no-order outcome samples.

This script only joins already-local edge samples with already-local outcome
labels. It does not infer outcomes from observer prices, read raw/replay stores,
connect to network, or touch order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_OUTCOME_LABEL_BRIDGE"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_UNSAFE_INPUT_PATH": 2,
    "FAIL_EDGE_SAMPLE_DECODE": 3,
    "FAIL_OUTCOME_LABEL_DECODE": 4,
    "FAIL_MARKET_SCOPE": 5,
    "FAIL_FORBIDDEN_SIDE_EFFECT": 6,
    "FAIL_INSUFFICIENT_JOINED_OUTCOMES": 7,
    "FAIL_EDGE_ONLY_LABELS": 8,
    "FAIL_REALIZED_PERFORMANCE_QUALITY": 9,
}

ACCEPTED_PERFORMANCE_BASES = {
    "shadow_replay_outcome",
    "independent_shadow_outcome",
    "dry_run_outcome",
}

FORBIDDEN_FIELDS = (
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "auth_network_started",
    "started_canary",
    "started_observer",
    "started_user_ws",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def safe_local_artifact_path(root: Path, path: Path) -> bool:
    resolved = path.resolve()
    artifacts = (root / "xuan_research_artifacts").resolve()
    if artifacts not in resolved.parents and resolved != artifacts:
        return False
    parts = {part.lower() for part in resolved.parts}
    forbidden = {"raw", "replay", "replay_published"}
    return not (parts & forbidden) and "/mnt/poly-replay" not in str(resolved)


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not path.exists():
        return records, [{"line": None, "error": "missing_file"}]
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


def market_prefix_ok(market: Any, prefix: str) -> bool:
    return isinstance(market, str) and (market == prefix or market.startswith(f"{prefix}-"))


def join_key(record: dict[str, Any]) -> str | None:
    candidate_id = record.get("candidate_id")
    if isinstance(candidate_id, str) and candidate_id:
        return f"candidate:{candidate_id}"
    order_attempt_id = record.get("order_attempt_id")
    if isinstance(order_attempt_id, str) and order_attempt_id:
        return f"order_attempt:{order_attempt_id}"
    return None


def has_forbidden_side_effect(record: dict[str, Any]) -> bool:
    side_effects = record.get("side_effects") or {}
    return any(record.get(field) is True for field in FORBIDDEN_FIELDS) or any(
        side_effects.get(field) is True for field in FORBIDDEN_FIELDS
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--edge-samples", type=Path, required=True)
    parser.add_argument("--outcome-labels", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-joined-outcomes", type=int, default=100)
    parser.add_argument("--min-positive-realized-ratio", type=float, default=0.5)
    parser.add_argument("--min-mean-realized-edge-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_outcome_label_bridge_{label}"
    joined_path = out_dir / "realized_edge_samples.jsonl"

    path_safe = (
        safe_local_artifact_path(root, args.edge_samples)
        and safe_local_artifact_path(root, args.outcome_labels)
    )
    edge_samples, edge_errors = read_jsonl(args.edge_samples) if path_safe else ([], [])
    outcome_labels, label_errors = read_jsonl(args.outcome_labels) if path_safe else ([], [])

    labels_by_key: dict[str, dict[str, Any]] = {}
    duplicate_label_keys: list[str] = []
    wrong_market_count = 0
    forbidden_side_effect_count = 0
    edge_only_label_count = 0

    for label_record in outcome_labels:
        key = join_key(label_record)
        market = label_record.get("market_slug") or label_record.get("slug")
        if not market_prefix_ok(market, args.market_prefix):
            wrong_market_count += 1
            continue
        if has_forbidden_side_effect(label_record):
            forbidden_side_effect_count += 1
            continue
        basis = label_record.get("performance_basis")
        if basis not in ACCEPTED_PERFORMANCE_BASES:
            edge_only_label_count += 1
            continue
        if not key:
            continue
        if key in labels_by_key:
            duplicate_label_keys.append(key)
            continue
        labels_by_key[key] = label_record

    joined_samples: list[dict[str, Any]] = []
    unjoined_edge_count = 0
    edge_wrong_market_count = 0
    edge_side_effect_count = 0

    for sample in edge_samples:
        market = sample.get("market_slug") or sample.get("slug")
        if not market_prefix_ok(market, args.market_prefix):
            edge_wrong_market_count += 1
            continue
        if has_forbidden_side_effect(sample):
            edge_side_effect_count += 1
            continue
        key = join_key(sample)
        label_record = labels_by_key.get(key or "")
        if not label_record:
            unjoined_edge_count += 1
            continue
        realized_edge_bps = as_float(label_record.get("realized_edge_bps", label_record.get("expected_value_bps")))
        joined = dict(sample)
        joined["accepted"] = True
        joined["would_track"] = True
        joined["candidate_edge_bps"] = as_float(sample.get("edge_bps", sample.get("candidate_edge_bps")))
        joined["realized_edge_bps"] = realized_edge_bps
        joined["edge_bps"] = realized_edge_bps
        joined["expected_value_bps"] = realized_edge_bps
        joined["performance_basis"] = label_record.get("performance_basis")
        joined["outcome_label_source"] = label_record.get("outcome_label_source")
        joined["outcome_label_id"] = label_record.get("outcome_label_id")
        for field in FORBIDDEN_FIELDS:
            joined[field] = False
        joined_samples.append(joined)

    realized_values = [as_float(sample.get("realized_edge_bps")) for sample in joined_samples]
    positive_realized_count = sum(1 for value in realized_values if value > 0.0)
    positive_realized_ratio = positive_realized_count / len(realized_values) if realized_values else 0.0
    mean_realized_edge_bps = statistics.fmean(realized_values) if realized_values else 0.0

    joined_count = len(joined_samples)
    joined_count_ok = joined_count >= args.min_joined_outcomes
    realized_quality_ok = (
        positive_realized_ratio >= args.min_positive_realized_ratio
        and mean_realized_edge_bps >= args.min_mean_realized_edge_bps
    )

    failures: list[str] = []
    if not path_safe:
        failures.append("unsafe_input_path")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif edge_errors:
        failures.append("edge_sample_decode_error")
        status = "FAIL_EDGE_SAMPLE_DECODE"
    elif label_errors:
        failures.append("outcome_label_decode_error")
        status = "FAIL_OUTCOME_LABEL_DECODE"
    elif wrong_market_count or edge_wrong_market_count:
        failures.append("market_scope_failed")
        status = "FAIL_MARKET_SCOPE"
    elif forbidden_side_effect_count or edge_side_effect_count:
        failures.append("forbidden_side_effect")
        status = "FAIL_FORBIDDEN_SIDE_EFFECT"
    elif edge_only_label_count:
        failures.append("outcome_labels_do_not_provide_realized_performance_basis")
        status = "FAIL_EDGE_ONLY_LABELS"
    elif not joined_count_ok:
        failures.append("insufficient_joined_outcomes")
        status = "FAIL_INSUFFICIENT_JOINED_OUTCOMES"
    elif not realized_quality_ok:
        failures.append("realized_performance_quality_failed")
        status = "FAIL_REALIZED_PERFORMANCE_QUALITY"
    else:
        status = PASS_STATUS

    out_dir.mkdir(parents=True, exist_ok=True)
    with joined_path.open("w") as fh:
        for sample in joined_samples:
            fh.write(json.dumps(sample, sort_keys=True) + "\n")

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_outcome_label_bridge",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_outcome_label_bridge",
        "failures": failures,
        "edge_samples": str(args.edge_samples),
        "outcome_labels": str(args.outcome_labels),
        "realized_edge_samples": str(joined_path),
        "path_safe": path_safe,
        "edge_sample_count": len(edge_samples),
        "outcome_label_count": len(outcome_labels),
        "joined_outcome_count": joined_count,
        "unjoined_edge_count": unjoined_edge_count,
        "duplicate_label_key_count": len(duplicate_label_keys),
        "duplicate_label_keys_sample": duplicate_label_keys[:10],
        "edge_decode_error_count": len(edge_errors),
        "edge_decode_errors_sample": edge_errors[:5],
        "outcome_label_decode_error_count": len(label_errors),
        "outcome_label_decode_errors_sample": label_errors[:5],
        "wrong_market_count": wrong_market_count + edge_wrong_market_count,
        "forbidden_side_effect_count": forbidden_side_effect_count + edge_side_effect_count,
        "edge_only_label_count": edge_only_label_count,
        "positive_realized_count": positive_realized_count,
        "positive_realized_ratio": positive_realized_ratio,
        "mean_realized_edge_bps": mean_realized_edge_bps,
        "min_joined_outcomes": args.min_joined_outcomes,
        "min_positive_realized_ratio": args.min_positive_realized_ratio,
        "min_mean_realized_edge_bps": args.min_mean_realized_edge_bps,
        "strategy_performance_evidence_ready": status == PASS_STATUS,
        "performance_basis": sorted({str(sample.get("performance_basis")) for sample in joined_samples}),
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "network_started": False,
            "ssh_started": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
        "next_gate": (
            "feed realized_edge_samples into shadow performance evidence"
            if status == PASS_STATUS
            else "provide independent realized no-order outcome labels before performance evidence can pass"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
