#!/usr/bin/env python3
"""Build a local D+ no-order shadow/dry-run run artifact from outcome labels.

This is a local artifact harness only. It joins an already-local no-order
observer summary, edge samples, and independent realized outcome labels into
the run-manifest plus outcome-event shape consumed by
``xuan_b27_dplus_realized_outcome_labels.py``. It does not infer outcomes from
edge-only observer data, read raw/replay stores, connect to network, or touch
order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_NO_ORDER_SHADOW_DRY_RUN"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_UNSAFE_INPUT_PATH": 2,
    "FAIL_OBSERVER_SUMMARY_NOT_SAFE": 3,
    "FAIL_INPUT_DECODE": 4,
    "FAIL_MARKET_SCOPE": 5,
    "FAIL_FORBIDDEN_SIDE_EFFECT": 6,
    "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS": 7,
    "FAIL_DUPLICATE_LABEL_KEYS": 8,
    "FAIL_INSUFFICIENT_JOINED_OUTCOMES": 9,
    "FAIL_REALIZED_OUTCOME_QUALITY": 10,
}

ACCEPTED_PERFORMANCE_BASES = {
    "dry_run_outcome",
    "independent_shadow_outcome",
    "shadow_replay_outcome",
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


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_read_error": "missing_file"}
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}
    return value if isinstance(value, dict) else {"_read_error": "json_not_object"}


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not path.exists():
        return records, [{"file": str(path), "line": None, "error": "missing_file"}]
    with path.open() as fh:
        for line_no, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append({"file": str(path), "line": line_no, "error": str(exc)})
                continue
            if not isinstance(value, dict):
                errors.append({"file": str(path), "line": line_no, "error": "record_not_object"})
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


def market_prefix_ok(market: Any, prefix: str) -> bool:
    return isinstance(market, str) and (market == prefix or market.startswith(f"{prefix}-"))


def markets_prefix_ok(markets: list[Any], prefix: str) -> bool:
    return bool(markets) and all(market_prefix_ok(market, prefix) for market in markets)


def has_forbidden_side_effect(record: dict[str, Any]) -> bool:
    side_effects = record.get("side_effects") or {}
    return any(record.get(field) is True for field in FORBIDDEN_FIELDS) or any(
        side_effects.get(field) is True for field in FORBIDDEN_FIELDS
    )


def join_key(record: dict[str, Any]) -> str | None:
    candidate_id = record.get("candidate_id")
    if isinstance(candidate_id, str) and candidate_id:
        return f"candidate:{candidate_id}"
    order_attempt_id = record.get("order_attempt_id")
    if isinstance(order_attempt_id, str) and order_attempt_id:
        return f"order_attempt:{order_attempt_id}"
    return None


def observer_summary_safe(summary: dict[str, Any], market_prefix: str) -> bool:
    run_manifest = summary.get("run_manifest") or {}
    candidates = summary.get("observer_candidate_counts") or {}
    return (
        summary.get("artifact") == "xuan_b27_dplus_observer_summary"
        and summary.get("status") == "PASS_NO_ORDER_OBSERVER"
        and "_read_error" not in summary
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
        and markets_prefix_ok(summary.get("markets") or [], market_prefix)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--observer-summary", type=Path, required=True)
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
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_no_order_shadow_run_artifact_{label}"
    events_path = out_dir / "outcome_events.jsonl"
    joined_edges_path = out_dir / "joined_edge_samples.jsonl"

    path_safe = (
        safe_local_artifact_path(root, args.observer_summary)
        and safe_local_artifact_path(root, args.edge_samples)
        and safe_local_artifact_path(root, args.outcome_labels)
        and safe_local_artifact_path(root, out_dir)
    )
    summary = read_json(args.observer_summary) if path_safe else {}
    edge_samples, edge_errors = read_jsonl(args.edge_samples) if path_safe else ([], [])
    outcome_labels, label_errors = read_jsonl(args.outcome_labels) if path_safe else ([], [])

    labels_by_key: dict[str, dict[str, Any]] = {}
    duplicate_label_keys: list[str] = []
    wrong_market_count = 0
    forbidden_side_effect_count = 0
    edge_only_or_unknown_basis_count = 0

    for label_record in outcome_labels:
        market = label_record.get("market_slug") or label_record.get("slug")
        if not market_prefix_ok(market, args.market_prefix):
            wrong_market_count += 1
            continue
        if has_forbidden_side_effect(label_record):
            forbidden_side_effect_count += 1
            continue
        if label_record.get("performance_basis") not in ACCEPTED_PERFORMANCE_BASES:
            edge_only_or_unknown_basis_count += 1
            continue
        key = join_key(label_record)
        if not key:
            continue
        if key in labels_by_key:
            duplicate_label_keys.append(key)
            continue
        labels_by_key[key] = label_record

    joined_edges: list[dict[str, Any]] = []
    outcome_events: list[dict[str, Any]] = []
    edge_wrong_market_count = 0
    edge_side_effect_count = 0
    unjoined_edge_count = 0

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
        realized_edge_bps = as_float(
            label_record.get("realized_edge_bps", label_record.get("expected_value_bps"))
        )
        candidate_id = sample.get("candidate_id") or label_record.get("candidate_id")
        order_attempt_id = sample.get("order_attempt_id") or label_record.get("order_attempt_id")
        event = {
            "event": "xuan_b27_dplus_dry_run_outcome_label",
            "market_slug": market,
            "candidate_id": candidate_id,
            "order_attempt_id": order_attempt_id,
            "outcome_label_id": label_record.get("outcome_label_id"),
            "outcome_label_source": label_record.get("outcome_label_source")
            or "local_no_order_shadow_run_artifact",
            "performance_basis": label_record.get("performance_basis"),
            "candidate_edge_bps": as_float(sample.get("edge_bps", sample.get("candidate_edge_bps"))),
            "realized_edge_bps": realized_edge_bps,
            "expected_value_bps": realized_edge_bps,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
        }
        outcome_events.append(event)
        joined_edge = dict(sample)
        joined_edge["realized_edge_bps"] = realized_edge_bps
        joined_edge["performance_basis"] = label_record.get("performance_basis")
        for field in FORBIDDEN_FIELDS:
            joined_edge[field] = False
        joined_edges.append(joined_edge)

    realized_values = [as_float(event.get("realized_edge_bps")) for event in outcome_events]
    positive_realized_count = sum(1 for value in realized_values if value > 0.0)
    positive_realized_ratio = positive_realized_count / len(realized_values) if realized_values else 0.0
    mean_realized_edge_bps = statistics.fmean(realized_values) if realized_values else 0.0

    failures: list[str] = []
    summary_safe = observer_summary_safe(summary, args.market_prefix) if path_safe else False
    if not path_safe:
        failures.append("unsafe_input_path")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif not summary_safe:
        failures.append("observer_summary_not_safe")
        status = "FAIL_OBSERVER_SUMMARY_NOT_SAFE"
    elif edge_errors or label_errors:
        failures.append("input_decode_error")
        status = "FAIL_INPUT_DECODE"
    elif wrong_market_count or edge_wrong_market_count:
        failures.append("market_scope_failed")
        status = "FAIL_MARKET_SCOPE"
    elif forbidden_side_effect_count or edge_side_effect_count:
        failures.append("forbidden_side_effect")
        status = "FAIL_FORBIDDEN_SIDE_EFFECT"
    elif edge_only_or_unknown_basis_count:
        failures.append("edge_only_or_unknown_basis")
        status = "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS"
    elif duplicate_label_keys:
        failures.append("duplicate_label_keys")
        status = "FAIL_DUPLICATE_LABEL_KEYS"
    elif len(outcome_events) < args.min_joined_outcomes:
        failures.append("insufficient_joined_outcomes")
        status = "FAIL_INSUFFICIENT_JOINED_OUTCOMES"
    elif (
        positive_realized_ratio < args.min_positive_realized_ratio
        or mean_realized_edge_bps < args.min_mean_realized_edge_bps
    ):
        failures.append("realized_outcome_quality_failed")
        status = "FAIL_REALIZED_OUTCOME_QUALITY"
    else:
        status = PASS_STATUS

    out_dir.mkdir(parents=True, exist_ok=True)
    with events_path.open("w") as fh:
        for event in outcome_events:
            fh.write(json.dumps(event, sort_keys=True) + "\n")
    with joined_edges_path.open("w") as fh:
        for sample in joined_edges:
            fh.write(json.dumps(sample, sort_keys=True) + "\n")

    market_slug = None
    if outcome_events:
        market_slug = outcome_events[0].get("market_slug")
    elif summary.get("markets"):
        market_slug = (summary.get("markets") or [None])[0]

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_no_order_shadow_run_artifact",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_order_shadow_or_dry_run",
        "no_order": True,
        "market_slug": market_slug,
        "markets": summary.get("markets") or ([market_slug] if market_slug else []),
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_observer": False,
        "started_user_ws": False,
        "started_canary": False,
        "failures": failures,
        "observer_summary": str(args.observer_summary),
        "observer_summary_safe": summary_safe,
        "edge_samples": str(args.edge_samples),
        "outcome_labels": str(args.outcome_labels),
        "outcome_events": str(events_path),
        "joined_edge_samples": str(joined_edges_path),
        "path_safe": path_safe,
        "edge_sample_count": len(edge_samples),
        "outcome_label_count": len(outcome_labels),
        "outcome_event_count": len(outcome_events),
        "joined_edge_count": len(joined_edges),
        "unjoined_edge_count": unjoined_edge_count,
        "duplicate_label_key_count": len(duplicate_label_keys),
        "duplicate_label_keys_sample": duplicate_label_keys[:10],
        "edge_decode_error_count": len(edge_errors),
        "outcome_label_decode_error_count": len(label_errors),
        "wrong_market_count": wrong_market_count + edge_wrong_market_count,
        "forbidden_side_effect_count": forbidden_side_effect_count + edge_side_effect_count,
        "edge_only_or_unknown_basis_count": edge_only_or_unknown_basis_count,
        "positive_realized_count": positive_realized_count,
        "positive_realized_ratio": positive_realized_ratio,
        "mean_realized_edge_bps": mean_realized_edge_bps,
        "min_joined_outcomes": args.min_joined_outcomes,
        "min_positive_realized_ratio": args.min_positive_realized_ratio,
        "min_mean_realized_edge_bps": args.min_mean_realized_edge_bps,
        "realized_outcome_label_run_ready": status == PASS_STATUS,
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
            "feed this run artifact into realized outcome-label producer"
            if status == PASS_STATUS
            else "provide safe no-order observer summary, edge samples, and independent realized labels"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
