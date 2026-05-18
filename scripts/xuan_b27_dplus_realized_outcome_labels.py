#!/usr/bin/env python3
"""Produce local realized outcome labels from Rust no-order shadow artifacts.

The producer only accepts explicit outcome-label events already present in a
local xuan_research artifact. It does not read raw/replay stores, infer labels
from observer edge, connect to network, or touch order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_REALIZED_OUTCOME_LABELS"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_UNSAFE_INPUT_PATH": 2,
    "FAIL_RUN_MANIFEST_NOT_SAFE": 3,
    "FAIL_OUTCOME_EVENT_DECODE": 4,
    "FAIL_MARKET_SCOPE": 5,
    "FAIL_FORBIDDEN_SIDE_EFFECT": 6,
    "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS": 7,
    "FAIL_INSUFFICIENT_OUTCOME_LABELS": 8,
    "FAIL_REALIZED_OUTCOME_QUALITY": 9,
    "FAIL_NO_OUTCOME_EVENTS": 10,
}

ACCEPTED_PERFORMANCE_BASES = {
    "dry_run_outcome",
    "independent_shadow_outcome",
    "shadow_replay_outcome",
}

ACCEPTED_EVENT_NAMES = {
    "xuan_b27_dplus_shadow_outcome_label",
    "xuan_b27_dplus_dry_run_outcome_label",
    "xuan_b27_dplus_realized_outcome_label",
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

ACCEPTED_RUN_STATUSES = {
    "PASS_RUST_NO_ORDER_SHADOW_DRY_RUN",
    "PASS_NO_ORDER_SHADOW_DRY_RUN",
    "PASS_NO_ORDER_DRY_RUN",
    "PASS_LOCAL_NO_ORDER_SHADOW_FIXTURE",
}

ACCEPTED_RUN_SCOPES = {
    "rust_no_order_shadow_or_dry_run",
    "local_no_order_shadow_or_dry_run",
    "local_no_network_no_order_shadow_fixture",
}


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


def market_prefix_ok(market: Any, prefix: str) -> bool:
    return isinstance(market, str) and (market == prefix or market.startswith(f"{prefix}-"))


def event_payload(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return record


def event_name(record: dict[str, Any], data: dict[str, Any]) -> str:
    for source in (data, record.get("payload"), record):
        if isinstance(source, dict) and isinstance(source.get("event"), str):
            return str(source["event"])
    return ""


def has_forbidden_side_effect(record: dict[str, Any]) -> bool:
    side_effects = record.get("side_effects") or {}
    return any(record.get(field) is True for field in FORBIDDEN_FIELDS) or any(
        side_effects.get(field) is True for field in FORBIDDEN_FIELDS
    )


def find_run_manifest(run_dir: Path) -> tuple[Path | None, dict[str, Any]]:
    for name in ("run_manifest.json", "shadow_run_manifest.json", "manifest.json"):
        path = run_dir / name
        if path.exists():
            return path, read_json(path)
    return None, {}


def run_manifest_safe(manifest: dict[str, Any], market_prefix: str) -> bool:
    side_effects = manifest.get("side_effects") or {}
    market = manifest.get("market_slug") or manifest.get("slug")
    markets = manifest.get("markets") if isinstance(manifest.get("markets"), list) else []
    market_ok = market_prefix_ok(market, market_prefix) or (
        bool(markets) and all(market_prefix_ok(item, market_prefix) for item in markets)
    )
    no_forbidden_effects = all(manifest.get(field, False) is False for field in FORBIDDEN_FIELDS) and all(
        side_effects.get(field, False) is False for field in FORBIDDEN_FIELDS
    )
    no_order_permissions = (
        manifest.get("orders_allowed", False) is False
        and manifest.get("cancels_allowed", False) is False
        and manifest.get("redeems_allowed", False) is False
    )
    return (
        manifest.get("strategy") == "xuan_b27_dplus"
        and manifest.get("status") in ACCEPTED_RUN_STATUSES
        and manifest.get("scope") in ACCEPTED_RUN_SCOPES
        and manifest.get("no_order") is True
        and market_ok
        and no_forbidden_effects
        and no_order_permissions
    )


def discover_event_files(run_dir: Path) -> list[Path]:
    names = ("outcome_events.jsonl", "realized_outcomes.jsonl", "shadow_outcomes.jsonl")
    files = [run_dir / name for name in names if (run_dir / name).exists()]
    files.extend(sorted((run_dir / "recorder").rglob("events.jsonl")))
    return files


def normalize_label(record: dict[str, Any], market_prefix: str) -> tuple[dict[str, Any] | None, str | None]:
    data = event_payload(record)
    event = event_name(record, data)
    if event not in ACCEPTED_EVENT_NAMES:
        return None, None
    market = data.get("market_slug") or data.get("slug")
    if not market_prefix_ok(market, market_prefix):
        return None, "wrong_market"
    if has_forbidden_side_effect(data) or has_forbidden_side_effect(record):
        return None, "forbidden_side_effect"
    basis = data.get("performance_basis")
    if basis not in ACCEPTED_PERFORMANCE_BASES:
        return None, "edge_only_or_unknown_basis"
    candidate_id = data.get("candidate_id")
    order_attempt_id = data.get("order_attempt_id")
    if not isinstance(candidate_id, str) and not isinstance(order_attempt_id, str):
        return None, "missing_join_key"
    realized_edge_bps = as_float(data.get("realized_edge_bps", data.get("expected_value_bps")))
    label = {
        "market_slug": market,
        "candidate_id": candidate_id,
        "order_attempt_id": order_attempt_id,
        "outcome_label_id": data.get("outcome_label_id"),
        "outcome_label_source": data.get("outcome_label_source"),
        "performance_basis": basis,
        "realized_edge_bps": realized_edge_bps,
        "expected_value_bps": realized_edge_bps,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
    }
    return label, None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-labels", type=int, default=100)
    parser.add_argument("--min-positive-realized-ratio", type=float, default=0.5)
    parser.add_argument("--min-mean-realized-edge-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_realized_outcome_labels_{label}"
    labels_path = out_dir / "outcome_labels.jsonl"

    path_safe = safe_local_artifact_path(root, args.run_dir)
    run_manifest_path, run_manifest = find_run_manifest(args.run_dir) if path_safe else (None, {})
    manifest_safe = path_safe and run_manifest_safe(run_manifest, args.market_prefix)
    event_files = discover_event_files(args.run_dir) if manifest_safe else []

    labels: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    reject_counts = {
        "wrong_market": 0,
        "forbidden_side_effect": 0,
        "edge_only_or_unknown_basis": 0,
        "missing_join_key": 0,
    }
    unsafe_event_file_count = 0

    for path in event_files:
        if not safe_local_artifact_path(root, path):
            unsafe_event_file_count += 1
            continue
        records, errors = read_jsonl(path)
        decode_errors.extend(errors)
        for record in records:
            normalized, reason = normalize_label(record, args.market_prefix)
            if normalized is not None:
                labels.append(normalized)
            elif reason in reject_counts:
                reject_counts[reason] += 1

    realized_values = [as_float(item.get("realized_edge_bps")) for item in labels]
    positive_count = sum(1 for value in realized_values if value > 0.0)
    positive_ratio = positive_count / len(realized_values) if realized_values else 0.0
    mean_realized_edge_bps = statistics.fmean(realized_values) if realized_values else 0.0

    failures: list[str] = []
    if not path_safe:
        failures.append("unsafe_input_path")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif not manifest_safe:
        failures.append("run_manifest_not_safe")
        status = "FAIL_RUN_MANIFEST_NOT_SAFE"
    elif decode_errors:
        failures.append("outcome_event_decode_error")
        status = "FAIL_OUTCOME_EVENT_DECODE"
    elif unsafe_event_file_count:
        failures.append("unsafe_event_file")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif reject_counts["wrong_market"]:
        failures.append("market_scope_failed")
        status = "FAIL_MARKET_SCOPE"
    elif reject_counts["forbidden_side_effect"]:
        failures.append("forbidden_side_effect")
        status = "FAIL_FORBIDDEN_SIDE_EFFECT"
    elif reject_counts["edge_only_or_unknown_basis"]:
        failures.append("edge_only_or_unknown_basis")
        status = "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS"
    elif not event_files or not labels:
        failures.append("no_outcome_events")
        status = "FAIL_NO_OUTCOME_EVENTS"
    elif len(labels) < args.min_labels:
        failures.append("insufficient_outcome_labels")
        status = "FAIL_INSUFFICIENT_OUTCOME_LABELS"
    elif positive_ratio < args.min_positive_realized_ratio or mean_realized_edge_bps < args.min_mean_realized_edge_bps:
        failures.append("realized_outcome_quality_failed")
        status = "FAIL_REALIZED_OUTCOME_QUALITY"
    else:
        status = PASS_STATUS

    out_dir.mkdir(parents=True, exist_ok=True)
    with labels_path.open("w") as fh:
        for item in labels:
            fh.write(json.dumps(item, sort_keys=True) + "\n")

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_realized_outcome_labels",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_realized_outcome_label_producer",
        "failures": failures,
        "run_dir": str(args.run_dir),
        "run_manifest": str(run_manifest_path) if run_manifest_path else None,
        "run_manifest_status": run_manifest.get("status"),
        "run_manifest_safe": manifest_safe,
        "path_safe": path_safe,
        "event_file_count": len(event_files),
        "unsafe_event_file_count": unsafe_event_file_count,
        "outcome_labels": str(labels_path),
        "outcome_label_count": len(labels),
        "decode_error_count": len(decode_errors),
        "decode_errors_sample": decode_errors[:5],
        "reject_counts": reject_counts,
        "positive_realized_count": positive_count,
        "positive_realized_ratio": positive_ratio,
        "mean_realized_edge_bps": mean_realized_edge_bps,
        "min_labels": args.min_labels,
        "min_positive_realized_ratio": args.min_positive_realized_ratio,
        "min_mean_realized_edge_bps": args.min_mean_realized_edge_bps,
        "outcome_labels_ready": status == PASS_STATUS,
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
            "feed outcome_labels into outcome-label bridge"
            if status == PASS_STATUS
            else "produce explicit realized no-order outcome events in Rust shadow/dry-run artifacts"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
