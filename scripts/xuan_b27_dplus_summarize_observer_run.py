#!/usr/bin/env python3
"""Summarize a local xuan_b27_dplus observer artifact directory."""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path


FORBIDDEN_EVENTS = {
    "order_accepted",
    "order_rejected",
    "cancel_accepted",
    "cancel_rejected",
    "redeem_result",
    "merge_executed",
}


def load_jsonl(path: Path):
    with path.open() as fh:
        lines = fh.readlines()
        for line_no, raw_line in enumerate(lines, 1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line)
            except json.JSONDecodeError as exc:
                truncated_final_line = line_no == len(lines) and not raw_line.endswith("\n")
                if truncated_final_line:
                    yield line_no, {"_truncated_final_line": str(exc)}
                else:
                    yield line_no, {"_decode_error": str(exc)}


def event_name(record: dict, path: Path) -> str:
    payload = record.get("payload")
    if isinstance(payload, dict):
        event = payload.get("event")
        if isinstance(event, str):
            return event
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("event"), str):
            return data["event"]
    if isinstance(record.get("event"), str):
        return record["event"]
    return path.name


def event_data(record: dict) -> dict:
    payload = record.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return record


def summarize(run_dir: Path) -> dict:
    recorder = run_dir / "recorder"
    counters = collections.Counter()
    blocked = collections.Counter()
    market_enabled = collections.Counter()
    source_truth = collections.Counter()
    markets = set()
    previews = 0
    submitted_previews = 0
    would_track = 0
    would_place = 0
    no_order_violations = []
    decode_errors = []
    truncated_final_lines = []

    jsonl_files = sorted(recorder.rglob("*.jsonl"))
    for path in jsonl_files:
        for line_no, record in load_jsonl(path):
            if "_decode_error" in record:
                decode_errors.append({"file": str(path), "line": line_no, "error": record["_decode_error"]})
                continue
            if "_truncated_final_line" in record:
                truncated_final_lines.append(
                    {"file": str(path), "line": line_no, "error": record["_truncated_final_line"]}
                )
                continue
            ev = event_name(record, path)
            data = event_data(record)
            counters[ev] += 1
            if record.get("slug"):
                markets.add(record["slug"])
            if data.get("market_slug"):
                markets.add(data["market_slug"])
            if data.get("slug"):
                markets.add(data["slug"])

            if ev in FORBIDDEN_EVENTS:
                no_order_violations.append({"file": str(path), "line": line_no, "event": ev})
            if data.get("orders_sent_by_this_module") is True:
                no_order_violations.append(
                    {"file": str(path), "line": line_no, "event": "orders_sent_by_this_module=true"}
                )

            candidates = data.get("observer_candidates") or []
            if not isinstance(candidates, list):
                continue
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                if candidate.get("would_track") is True:
                    would_track += 1
                if candidate.get("would_place") is True:
                    would_place += 1
                reason = candidate.get("blocked_reason")
                if isinstance(reason, str):
                    blocked[reason] += 1
                if "market_enabled" in data:
                    market_enabled[str(bool(data["market_enabled"])).lower()] += 1
                trace = candidate.get("order_attempt_trace_preview")
                if isinstance(trace, dict):
                    previews += 1
                    if trace.get("submitted") is True:
                        submitted_previews += 1
                gate = data.get("source_of_truth_gate")
                if isinstance(gate, dict):
                    source_truth[str(gate.get("verdict"))] += 1

    run_manifest = run_dir / "run_manifest.json"
    start_manifest = run_dir / "start_manifest.json"
    manifest = {}
    if run_manifest.exists():
        manifest["run_manifest"] = json.loads(run_manifest.read_text())
    if start_manifest.exists():
        manifest["start_manifest"] = json.loads(start_manifest.read_text())

    status = "PASS_NO_ORDER_OBSERVER"
    if no_order_violations or submitted_previews or would_place:
        status = "FAIL_NO_ORDER_VIOLATION"
    elif blocked and not would_track:
        status = "PARTIAL_NO_ORDER_BUT_ALL_CANDIDATES_BLOCKED"

    return {
        "artifact": "xuan_b27_dplus_observer_summary",
        "run_dir": str(run_dir),
        "status": status,
        "jsonl_file_count": len(jsonl_files),
        "event_counts": dict(counters),
        "markets": sorted(markets),
        "observer_candidate_counts": {
            "would_track": would_track,
            "would_place": would_place,
            "trace_previews": previews,
            "submitted_previews": submitted_previews,
        },
        "blocked_reason_counts": dict(blocked),
        "market_enabled_counts": dict(market_enabled),
        "source_truth_verdict_counts": dict(source_truth),
        "no_order_violation_count": len(no_order_violations),
        "no_order_violations_sample": no_order_violations[:20],
        "decode_error_count": len(decode_errors),
        "decode_errors_sample": decode_errors[:20],
        "truncated_final_line_count": len(truncated_final_lines),
        "truncated_final_lines_sample": truncated_final_lines[:20],
        **manifest,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--check-no-order",
        action="store_true",
        help="Exit non-zero if the artifact contains any order/cancel/redeem or submitted-preview evidence.",
    )
    parser.add_argument(
        "--require-tracked-candidates",
        action="store_true",
        help="Exit non-zero unless at least one observer candidate has would_track=true.",
    )
    parser.add_argument(
        "--require-market-prefix",
        help="Exit non-zero if any recorder market slug is outside this exact prefix, e.g. btc-updown-5m.",
    )
    args = parser.parse_args()
    summary = summarize(args.run_dir)
    text = json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n")
    print(text)
    if args.check_no_order and summary["status"] == "FAIL_NO_ORDER_VIOLATION":
        return 2
    if args.require_market_prefix:
        prefix = args.require_market_prefix.strip()
        bad_markets = [
            market
            for market in summary["markets"]
            if market != prefix and not market.startswith(f"{prefix}-")
        ]
        if bad_markets:
            return 4
    if args.require_tracked_candidates and summary["observer_candidate_counts"]["would_track"] <= 0:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
