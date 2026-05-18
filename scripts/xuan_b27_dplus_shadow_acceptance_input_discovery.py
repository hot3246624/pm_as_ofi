#!/usr/bin/env python3
"""Discover real local D+ shadow acceptance inputs.

This is a no-network inventory gate. It scans only local
``xuan_research_artifacts`` manifests, excludes smoke/fixture/raw/replay
paths, and classifies whether there is a non-fixture no-order shadow/dry-run
run artifact that can be passed to the Rust shadow strategy acceptance runner.
It never starts observers, reads raw/replay stores, connects to network, or
touches order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


READY_STATUS = "READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER"
BLOCKED_NO_INPUTS_STATUS = "BLOCKED_NO_REAL_SHADOW_ACCEPTANCE_INPUTS"
BLOCKED_INVALID_INPUTS_STATUS = "BLOCKED_SHADOW_ACCEPTANCE_INPUTS_INVALID"
FAIL_UNSAFE_SCAN_ROOT = "FAIL_UNSAFE_SCAN_ROOT"

ACCEPTED_ARTIFACTS = {
    "xuan_b27_dplus_no_order_shadow_run_artifact",
    "xuan_b27_dplus_rust_no_order_shadow_run",
}

ACCEPTED_RUN_STATUSES = {
    "PASS_NO_ORDER_SHADOW_DRY_RUN",
    "PASS_RUST_NO_ORDER_SHADOW_DRY_RUN",
}

ACCEPTED_RUN_SCOPES = {
    "local_no_order_shadow_or_dry_run",
    "rust_no_order_shadow_or_dry_run",
}

ACCEPTED_EVENT_NAMES = {
    "xuan_b27_dplus_shadow_outcome_label",
    "xuan_b27_dplus_dry_run_outcome_label",
    "xuan_b27_dplus_realized_outcome_label",
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

FORBIDDEN_DIR_NAMES = {"raw", "replay", "replay_published"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}
    return value if isinstance(value, dict) else {"_read_error": "json_not_object"}


def read_jsonl(path: Path, max_records: int = 100_000) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not path.exists():
        return records, [{"file": str(path), "line": None, "error": "missing_file"}]
    with path.open() as fh:
        for line_no, raw in enumerate(fh, 1):
            if len(records) >= max_records:
                break
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


def safe_local_artifact_path(root: Path, path: Path) -> bool:
    resolved = path.resolve()
    artifacts = (root / "xuan_research_artifacts").resolve()
    if artifacts not in resolved.parents and resolved != artifacts:
        return False
    parts = {part.lower() for part in resolved.parts}
    return not (parts & FORBIDDEN_DIR_NAMES) and "/mnt/poly-replay" not in str(resolved)


def path_is_fixture_or_smoke(path: Path) -> bool:
    return any(
        "fixture" in part.lower() or "_smoke_" in part.lower() or part.lower() == "fixtures"
        for part in path.resolve().parts
    )


def iter_manifest_paths(scan_root: Path) -> list[Path]:
    paths: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(scan_root):
        dirnames[:] = [
            item for item in dirnames if item.lower() not in FORBIDDEN_DIR_NAMES
        ]
        if "manifest.json" in filenames:
            paths.append(Path(dirpath) / "manifest.json")
    return sorted(paths)


def market_prefix_ok(value: Any, prefix: str) -> bool:
    return isinstance(value, str) and (value == prefix or value.startswith(f"{prefix}-"))


def markets_prefix_ok(manifest: dict[str, Any], prefix: str) -> bool:
    market = manifest.get("market_slug") or manifest.get("slug")
    markets = manifest.get("markets") if isinstance(manifest.get("markets"), list) else []
    if market_prefix_ok(market, prefix):
        return True
    return bool(markets) and all(market_prefix_ok(item, prefix) for item in markets)


def has_forbidden_side_effect(record: dict[str, Any]) -> bool:
    side_effects = record.get("side_effects") or {}
    return any(record.get(field) is True for field in FORBIDDEN_FIELDS) or any(
        side_effects.get(field) is True for field in FORBIDDEN_FIELDS
    )


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


def resolve_input_path(root: Path, run_dir: Path, value: Any, default_name: str) -> Path:
    path = Path(str(value)) if isinstance(value, str) and value else run_dir / default_name
    if path.is_absolute():
        return path
    root_relative = root / path
    if root_relative.exists():
        return root_relative
    return run_dir / path


def manifest_candidate_safe(root: Path, manifest_path: Path, manifest: dict[str, Any], market_prefix: str) -> tuple[bool, list[str]]:
    run_dir = manifest_path.parent
    failures: list[str] = []
    if manifest.get("artifact") not in ACCEPTED_ARTIFACTS:
        failures.append("artifact_not_no_order_shadow_run")
    if manifest.get("status") not in ACCEPTED_RUN_STATUSES:
        failures.append("status_not_pass_no_order_shadow_run")
    if manifest.get("strategy") != "xuan_b27_dplus":
        failures.append("strategy_mismatch")
    if manifest.get("scope") not in ACCEPTED_RUN_SCOPES:
        failures.append("scope_mismatch")
    if manifest.get("no_order") is not True:
        failures.append("no_order_not_true")
    for field in ("orders_allowed", "cancels_allowed", "redeems_allowed"):
        if manifest.get(field) is not False:
            failures.append(f"{field}_not_false")
    if has_forbidden_side_effect(manifest):
        failures.append("manifest_forbidden_side_effect")
    if not markets_prefix_ok(manifest, market_prefix):
        failures.append("market_scope_mismatch")

    observer_summary = resolve_input_path(root, run_dir, manifest.get("observer_summary"), "observer_summary.json")
    edge_samples = resolve_input_path(root, run_dir, manifest.get("edge_samples"), "edge_samples.jsonl")
    outcome_events = resolve_input_path(root, run_dir, manifest.get("outcome_events"), "outcome_events.jsonl")
    for label, path in (
        ("observer_summary", observer_summary),
        ("edge_samples", edge_samples),
        ("outcome_events", outcome_events),
    ):
        if not safe_local_artifact_path(root, path):
            failures.append(f"{label}_path_unsafe")
        elif not path.exists():
            failures.append(f"{label}_missing")
    return not failures, failures


def inspect_outcome_events(
    root: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
    market_prefix: str,
    min_outcome_events: int,
) -> tuple[bool, dict[str, Any], list[str]]:
    run_dir = manifest_path.parent
    event_path = resolve_input_path(root, run_dir, manifest.get("outcome_events"), "outcome_events.jsonl")
    if not safe_local_artifact_path(root, event_path):
        return False, {"path": str(event_path)}, ["outcome_events_path_unsafe"]

    records, errors = read_jsonl(event_path)
    accepted_count = 0
    wrong_market_count = 0
    forbidden_count = 0
    bad_basis_count = 0
    unknown_event_count = 0
    realized_values: list[float] = []
    failures: list[str] = []

    for record in records:
        data = event_payload(record)
        event = event_name(record, data)
        if event not in ACCEPTED_EVENT_NAMES:
            unknown_event_count += 1
            continue
        market = data.get("market_slug") or data.get("slug")
        if not market_prefix_ok(market, market_prefix):
            wrong_market_count += 1
            continue
        if has_forbidden_side_effect(record) or has_forbidden_side_effect(data):
            forbidden_count += 1
            continue
        if data.get("performance_basis") not in ACCEPTED_PERFORMANCE_BASES:
            bad_basis_count += 1
            continue
        accepted_count += 1
        value = data.get("realized_edge_bps", data.get("expected_value_bps"))
        if isinstance(value, (int, float)):
            realized_values.append(float(value))

    if errors:
        failures.append("outcome_events_decode_error")
    if accepted_count < min_outcome_events:
        failures.append("insufficient_accepted_outcome_events")
    if wrong_market_count:
        failures.append("wrong_market_outcome_events")
    if forbidden_count:
        failures.append("forbidden_side_effect_outcome_events")
    if bad_basis_count:
        failures.append("edge_only_or_unknown_outcome_basis")
    if accepted_count and realized_values and sum(realized_values) / len(realized_values) < 0.0:
        failures.append("negative_mean_realized_edge_bps")

    summary = {
        "path": str(event_path),
        "record_count": len(records),
        "decode_error_count": len(errors),
        "accepted_outcome_event_count": accepted_count,
        "wrong_market_count": wrong_market_count,
        "forbidden_side_effect_count": forbidden_count,
        "edge_only_or_unknown_basis_count": bad_basis_count,
        "unknown_event_count": unknown_event_count,
        "mean_realized_edge_bps": (
            sum(realized_values) / len(realized_values) if realized_values else None
        ),
    }
    return not failures, summary, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan-root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-outcome-events", type=int, default=100)
    parser.add_argument(
        "--allow-fixture-root",
        action="store_true",
        help="Only for local smoke fixtures; real discovery must omit this.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    scan_root = (args.scan_root or root / "xuan_research_artifacts").resolve()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_acceptance_input_discovery_{label}"

    scan_root_safe = safe_local_artifact_path(root, scan_root)
    candidates: list[dict[str, Any]] = []
    fixture_candidate_count = 0
    invalid_real_candidate_count = 0
    valid_real_candidates: list[dict[str, Any]] = []
    manifest_count = 0

    if scan_root_safe:
        for manifest_path in iter_manifest_paths(scan_root):
            if not safe_local_artifact_path(root, manifest_path):
                continue
            manifest_count += 1
            manifest = read_json(manifest_path)
            if manifest.get("artifact") not in ACCEPTED_ARTIFACTS:
                continue
            fixture_or_smoke = path_is_fixture_or_smoke(manifest_path)
            if fixture_or_smoke and not args.allow_fixture_root:
                fixture_candidate_count += 1
                continue
            safe_manifest, manifest_failures = manifest_candidate_safe(
                root, manifest_path, manifest, args.market_prefix
            )
            events_ok, event_summary, event_failures = inspect_outcome_events(
                root,
                manifest_path,
                manifest,
                args.market_prefix,
                args.min_outcome_events,
            )
            failures = manifest_failures + event_failures
            candidate = {
                "manifest": str(manifest_path),
                "run_dir": str(manifest_path.parent),
                "artifact": manifest.get("artifact"),
                "status": manifest.get("status"),
                "scope": manifest.get("scope"),
                "fixture_or_smoke_input": fixture_or_smoke,
                "manifest_safe": safe_manifest,
                "outcome_events_ok": events_ok,
                "outcome_events": event_summary,
                "failures": failures,
            }
            candidates.append(candidate)
            if safe_manifest and events_ok:
                valid_real_candidates.append(candidate)
            else:
                invalid_real_candidate_count += 1

    if not scan_root_safe:
        status = FAIL_UNSAFE_SCAN_ROOT
        failures = ["unsafe_scan_root"]
    elif valid_real_candidates:
        status = READY_STATUS
        failures = []
    elif invalid_real_candidate_count:
        status = BLOCKED_INVALID_INPUTS_STATUS
        failures = ["shadow_acceptance_inputs_invalid"]
    else:
        status = BLOCKED_NO_INPUTS_STATUS
        failures = ["no_non_fixture_shadow_acceptance_inputs"]

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_acceptance_input_discovery",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_shadow_acceptance_input_discovery",
        "scan_root": str(scan_root),
        "scan_root_safe": scan_root_safe,
        "allow_fixture_root": args.allow_fixture_root,
        "market_prefix": args.market_prefix,
        "min_outcome_events": args.min_outcome_events,
        "manifest_count": manifest_count,
        "fixture_or_smoke_candidate_count": fixture_candidate_count,
        "real_candidate_count": len(candidates),
        "valid_real_candidate_count": len(valid_real_candidates),
        "invalid_real_candidate_count": invalid_real_candidate_count,
        "candidates": candidates[:25],
        "ready_run_dirs": [candidate["run_dir"] for candidate in valid_real_candidates],
        "failures": failures,
        "real_shadow_acceptance_input_published": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_observer": False,
        "started_user_ws": False,
        "started_canary": False,
        "side_effects": {
            "network_started": False,
            "ssh_started": False,
            "scanned_raw_or_replay": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
        "next_gate": (
            "run xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py on a ready run_dir"
            if status == READY_STATUS
            else "produce a non-fixture no-order shadow/dry-run run artifact with realized outcome labels"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 2 if status == FAIL_UNSAFE_SCAN_ROOT else 0


if __name__ == "__main__":
    raise SystemExit(main())
