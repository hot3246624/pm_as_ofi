#!/usr/bin/env python3
"""Summarize a standalone xuan_b27_dplus read-only User WS observer run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


FORBIDDEN_EVENT_FRAGMENTS = (
    "order_accepted",
    "order_submitted",
    "order_rejected",
    "cancel",
    "redeem",
    "claim",
)
USER_WS_LIFECYCLE_EVENTS = {
    "user_ws_listener_started",
    "user_ws_connect_attempt",
    "user_ws_connected",
    "user_ws_subscribe_sent",
    "user_ws_text_received",
    "user_ws_closed_normally",
    "user_ws_connect_or_read_error",
    "user_ws_server_error_payload",
    "user_ws_closed_by_server",
    "user_ws_read_error",
}
USER_WS_ERROR_EVENTS = {
    "user_ws_connect_or_read_error",
    "user_ws_server_error_payload",
    "user_ws_closed_by_server",
    "user_ws_read_error",
}


def iter_jsonl(path: Path):
    with path.open(errors="ignore") as fh:
        for lineno, line in enumerate(fh, 1):
            raw = line.strip()
            if not raw:
                continue
            try:
                yield lineno, json.loads(raw), None
            except json.JSONDecodeError as exc:
                yield lineno, None, str(exc)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def summarize(run_dir: Path) -> dict[str, Any]:
    manifest = load_json(run_dir / "run_manifest.json")
    wrapper_manifest = load_json(run_dir / "wrapper_manifest.json")
    recorder_root = Path(manifest.get("recorder_root") or run_dir / "recorder")

    summary: dict[str, Any] = {
        "artifact": "xuan_b27_dplus_readonly_user_ws_summary",
        "run_dir": str(run_dir),
        "run_status": manifest.get("status"),
        "wrapper_status": wrapper_manifest.get("status"),
        "orders_possible": manifest.get("orders_possible"),
        "shared_ingress_status": manifest.get("shared_ingress_status"),
        "resolved_slug": manifest.get("resolved_slug"),
        "market_id": manifest.get("market_id"),
        "user_ws_auth_source": manifest.get("user_ws_auth_source"),
        "fills_seen_manifest": manifest.get("fills_seen", 0),
        "recorder_root": str(recorder_root),
        "user_ws_raw_count": 0,
        "user_ws_decode_error_count": 0,
        "event_decode_error_count": 0,
        "user_ws_fill_parsed_count": 0,
        "user_ws_lifecycle_event_count": 0,
        "user_ws_connected_count": 0,
        "user_ws_subscribe_sent_count": 0,
        "user_ws_error_event_count": 0,
        "fill_sink_count": 0,
        "forbidden_event_count": 0,
        "forbidden_event_samples": [],
        "events_files": [],
        "user_ws_files": [],
        "status": "UNKNOWN",
    }

    for path in sorted(recorder_root.glob("**/user_ws.jsonl")):
        summary["user_ws_files"].append(str(path))
        for _lineno, value, error in iter_jsonl(path):
            if error:
                summary["user_ws_decode_error_count"] += 1
                continue
            if value is not None:
                summary["user_ws_raw_count"] += 1

    for path in sorted(recorder_root.glob("**/events.jsonl")):
        summary["events_files"].append(str(path))
        for lineno, value, error in iter_jsonl(path):
            if error:
                summary["event_decode_error_count"] += 1
                continue
            payload = (value or {}).get("payload") or {}
            event = str(payload.get("event") or "")
            lower = event.lower()
            if event == "user_ws_fill_parsed":
                summary["user_ws_fill_parsed_count"] += 1
            if event in USER_WS_LIFECYCLE_EVENTS:
                summary["user_ws_lifecycle_event_count"] += 1
            if event == "user_ws_connected":
                summary["user_ws_connected_count"] += 1
            if event == "user_ws_subscribe_sent":
                summary["user_ws_subscribe_sent_count"] += 1
            if event in USER_WS_ERROR_EVENTS:
                summary["user_ws_error_event_count"] += 1
            if event == "xuan_b27_dplus_readonly_user_ws_fill_sink":
                summary["fill_sink_count"] += 1
            if any(fragment in lower for fragment in FORBIDDEN_EVENT_FRAGMENTS):
                summary["forbidden_event_count"] += 1
                if len(summary["forbidden_event_samples"]) < 5:
                    summary["forbidden_event_samples"].append(
                        {"path": str(path), "lineno": lineno, "event": event}
                    )

    if summary["forbidden_event_count"] > 0 or manifest.get("orders_possible") is not False:
        summary["status"] = "FAIL_READONLY_VIOLATION"
    elif summary["user_ws_decode_error_count"] > 0 or summary["event_decode_error_count"] > 0:
        summary["status"] = "FAIL_RECORDER_DECODE_ERROR"
    elif str(summary["run_status"] or "").startswith("FAIL_USER_WS_"):
        summary["status"] = "FAIL_USER_WS_TASK"
    elif summary["user_ws_error_event_count"] > 0:
        summary["status"] = "FAIL_USER_WS_ERROR_EVENT"
    elif summary["run_status"] == "SHARED_INGRESS_BROKER_UNAVAILABLE":
        summary["status"] = "BLOCKED_SHARED_INGRESS_BROKER_UNAVAILABLE"
    elif summary["run_status"] == "PASS_READONLY_USER_WS_OBSERVER":
        summary["status"] = "PASS_READONLY_USER_WS_OBSERVER"
    else:
        summary["status"] = "UNKNOWN_RUN_STATUS"

    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check-readonly", action="store_true")
    parser.add_argument("--require-user-ws-records", action="store_true")
    parser.add_argument("--require-user-ws-connection", action="store_true")
    args = parser.parse_args()

    summary = summarize(args.run_dir)
    gate_failures: list[str] = []
    if args.check_readonly and summary["status"] in {
        "FAIL_READONLY_VIOLATION",
        "FAIL_RECORDER_DECODE_ERROR",
        "FAIL_USER_WS_TASK",
        "FAIL_USER_WS_ERROR_EVENT",
    }:
        gate_failures.append(str(summary["status"]))
    if args.require_user_ws_connection and (
        summary["user_ws_connected_count"] <= 0 or summary["user_ws_subscribe_sent_count"] <= 0
    ):
        gate_failures.append("FAIL_NO_USER_WS_CONNECTION")
    if args.require_user_ws_records and summary["user_ws_raw_count"] <= 0:
        gate_failures.append("FAIL_NO_USER_WS_RECORDS")

    summary["gate_requirements"] = {
        "check_readonly": args.check_readonly,
        "require_user_ws_connection": args.require_user_ws_connection,
        "require_user_ws_records": args.require_user_ws_records,
    }
    summary["gate_failures"] = gate_failures
    summary["gate_status"] = gate_failures[0] if gate_failures else "PASS"

    text = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    else:
        sys.stdout.write(text)

    if any(
        failure
        in {
            "FAIL_READONLY_VIOLATION",
            "FAIL_RECORDER_DECODE_ERROR",
            "FAIL_USER_WS_TASK",
            "FAIL_USER_WS_ERROR_EVENT",
        }
        for failure in gate_failures
    ):
        return 2
    if "FAIL_NO_USER_WS_CONNECTION" in gate_failures:
        return 4
    if "FAIL_NO_USER_WS_RECORDS" in gate_failures:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
