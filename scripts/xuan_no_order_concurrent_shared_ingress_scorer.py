#!/usr/bin/env python3
"""Score concurrent shared-ingress no-order evidence.

This scorer is local-only. It verifies that a no-order runtime output explicitly
claimed concurrent shared-ingress read-only mode, remained dry-run/no-order, and
recorded enough source-linkage coverage to make the sample auditable even while
other read-only clients used the same market socket.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        val = int(float(value))
        return val if math.isfinite(float(val)) else default
    except (TypeError, ValueError):
        return default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def score(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    hard_blockers: list[str] = []
    warnings: list[str] = []
    required = ["manifest.json", "aggregate_report.json", "run_exit_code.txt", "run_stderr.log"]
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        return {
            "status": "UNKNOWN_CONCURRENT_SHARED_INGRESS_EVIDENCE_MISSING_FILES",
            "output_root": str(root),
            "missing_files": missing,
            "hard_blockers": [f"missing_file:{name}" for name in missing],
            "decision": {"deployable": False, "remote_runner_allowed": False},
        }

    manifest = read_json(root / "manifest.json")
    aggregate = read_json(root / "aggregate_report.json")
    safety = manifest.get("safety", {})
    config = manifest.get("config", {})
    concurrency = manifest.get("concurrency", {})
    source = aggregate.get("source_linkage", {})
    run_exit = (root / "run_exit_code.txt").read_text().strip()
    stderr_text = (root / "run_stderr.log").read_text()

    if run_exit != "0":
        hard_blockers.append("run_exit_code_not_zero")
    if stderr_text:
        hard_blockers.append("stderr_not_empty")
    if safety.get("orders_sent") is not False:
        hard_blockers.append("orders_sent_not_false")
    if str(safety.get("dry_run")) != "1":
        hard_blockers.append("pm_dry_run_not_1")
    if config.get("allow_concurrent_shared_ingress_readers") is not True:
        hard_blockers.append("config_missing_allow_concurrent_shared_ingress_readers")
    if concurrency.get("allow_concurrent_shared_ingress_readers") is not True:
        hard_blockers.append("manifest_missing_allow_concurrent_shared_ingress_readers")
    if concurrency.get("mode") != "shared_ingress_read_only_concurrent_reader":
        hard_blockers.append("manifest_concurrency_mode_not_read_only")
    if concurrency.get("evidence_claim") != "clean_concurrent_shared_ingress_reader":
        hard_blockers.append("manifest_concurrency_evidence_claim_not_clean")
    contract = concurrency.get("read_only_contract", {})
    if contract.get("mutates_shared_ingress") is not False:
        hard_blockers.append("read_only_contract_mutates_shared_ingress_not_false")
    if contract.get("sends_orders") is not False:
        hard_blockers.append("read_only_contract_sends_orders_not_false")
    if contract.get("uses_shared_ingress_as_client") is not True:
        hard_blockers.append("read_only_contract_missing_socket_client")
    if not str(concurrency.get("socket_path", "")).endswith("/market.sock"):
        hard_blockers.append("concurrency_socket_path_not_market_sock")
    instance_id = safety.get("instance_id")
    output_dir = str(concurrency.get("output_dir") or "")
    if not instance_id:
        hard_blockers.append("missing_instance_id")
    elif instance_id not in output_dir:
        hard_blockers.append("output_dir_not_instance_scoped")

    start_snapshot = concurrency.get("process_snapshot_start")
    end_snapshot = concurrency.get("process_snapshot_end")
    if not isinstance(start_snapshot, list):
        hard_blockers.append("missing_process_snapshot_start")
    if not isinstance(end_snapshot, list):
        hard_blockers.append("missing_process_snapshot_end")
    if as_int(concurrency.get("concurrent_runner_count_start")) != len(start_snapshot or []):
        hard_blockers.append("concurrent_runner_count_start_mismatch")
    if as_int(concurrency.get("concurrent_runner_count_end")) != len(end_snapshot or []):
        hard_blockers.append("concurrent_runner_count_end_mismatch")
    if not start_snapshot and args.require_observed_concurrent_runner:
        hard_blockers.append("no_concurrent_runner_observed_at_start")
    if start_snapshot:
        warnings.append("concurrent_runner_observed_at_start")
    if end_snapshot:
        warnings.append("concurrent_runner_observed_at_end")

    source_checks = {}
    for prefix in ("book_l1", "book_l2", "trade"):
        observations = as_int(source.get(f"{prefix}_observations"))
        nonempty = as_int(source.get(f"{prefix}_source_nonempty"))
        missing_count = as_int(source.get(f"{prefix}_source_missing"))
        event_time_nonempty = as_int(source.get(f"{prefix}_event_time_nonempty"))
        source_checks[prefix] = {
            "observations": observations,
            "source_nonempty": nonempty,
            "source_missing": missing_count,
            "event_time_nonempty": event_time_nonempty,
            "first_source_sequence_id": source.get(f"{prefix}_first_source_sequence_id"),
            "last_source_sequence_id": source.get(f"{prefix}_last_source_sequence_id"),
        }
        if observations < args.min_observations:
            hard_blockers.append(f"{prefix}_observations_below_min")
        if nonempty < args.min_nonempty_sources:
            hard_blockers.append(f"{prefix}_source_nonempty_below_min")
        if missing_count > args.max_missing_sources:
            hard_blockers.append(f"{prefix}_source_missing_above_max")
        if event_time_nonempty < args.min_event_time_observations:
            hard_blockers.append(f"{prefix}_event_time_nonempty_below_min")

    status = (
        "KEEP_NO_ORDER_CONCURRENT_SHARED_INGRESS_EVIDENCE_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_NO_ORDER_CONCURRENT_SHARED_INGRESS_EVIDENCE_BLOCKED"
    )
    return {
        "status": status,
        "output_root": str(root),
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": sorted(set(warnings)),
        "safety": {
            "pm_dry_run": str(safety.get("dry_run")) == "1",
            "orders_sent": safety.get("orders_sent"),
            "shared_ingress_root": safety.get("shared_ingress_root"),
            "instance_id": instance_id,
        },
        "concurrency": concurrency,
        "source_checks": source_checks,
        "decision": {
            "concurrent_shared_ingress_evidence_clean": not hard_blockers,
            "deployable": False,
            "remote_runner_allowed": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-observations", type=int, default=1)
    parser.add_argument("--min-nonempty-sources", type=int, default=1)
    parser.add_argument("--min-event-time-observations", type=int, default=1)
    parser.add_argument("--max-missing-sources", type=int, default=0)
    parser.add_argument("--require-observed-concurrent-runner", action="store_true")
    args = parser.parse_args()

    started = time.time()
    result = score(Path(args.output_root).expanduser().resolve(), args)
    scorecard = {
        "artifact": "xuan_no_order_concurrent_shared_ingress_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_concurrent_shared_ingress_scorer.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))
    if result["hard_blockers"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
