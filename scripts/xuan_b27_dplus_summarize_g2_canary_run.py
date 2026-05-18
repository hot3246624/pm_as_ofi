#!/usr/bin/env python3
"""Summarize and gate a local xuan_b27_dplus G2 canary run artifact.

This is a local post-run review helper only. It reads JSON/JSONL artifacts,
checks the G2 acceptance contract, and optionally scans for caller-provided
secret sentinel strings. It does not connect to any service or start processes.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


EXPECTED_STRATEGY = "xuan_b27_dplus"
EXPECTED_MARKET_SLUG = "btc-updown-5m"
EXPECTED_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
MAX_LIVE_ORDERS = 2
MAX_OPEN_COST_USDC = 50.0
MAX_STRATEGY_EXPOSURE_USDC = 100.0
FORBIDDEN_CANARY_EVENT_FRAGMENTS = (
    "redeem",
    "claim",
    "systemd",
    "service_control",
    "broker_started",
    "broker_stopped",
    "shared_ingress_modified",
)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


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


def first_existing(run_dir: Path, names: tuple[str, ...]) -> Path | None:
    for name in names:
        path = run_dir / name
        if path.exists():
            return path
    return None


def scan_secret_sentinels(root: Path, sentinels: list[str]) -> list[dict[str, Any]]:
    if not sentinels:
        return []
    hits: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(root))
        if ".git" in path.parts:
            continue
        try:
            text = path.read_text(errors="ignore")
        except Exception:
            continue
        for sentinel in sentinels:
            if sentinel and sentinel in text:
                hits.append({"path": rel, "sentinel": sentinel})
                if len(hits) >= 20:
                    return hits
    return hits


def evaluate(review: dict[str, Any], event_stats: dict[str, Any], secret_hits: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    if review.get("exact_approval_scope") is not True:
        failures.append("missing_exact_approval_scope")
    if review.get("strategy") != EXPECTED_STRATEGY:
        failures.append("wrong_strategy")
    if review.get("market_slug") != EXPECTED_MARKET_SLUG:
        failures.append("wrong_market_slug")
    if review.get("shared_ingress_role") != "client":
        failures.append("shared_ingress_not_client")
    if review.get("shared_ingress_root") != EXPECTED_SHARED_INGRESS_ROOT:
        failures.append("wrong_shared_ingress_root")
    if review.get("gate_status") != "PASS":
        failures.append("gate_not_pass")
    if review.get("source_truth_all_pass") is not True:
        failures.append("source_truth_not_all_pass")
    if review.get("order_ack_count", 0) <= 0:
        failures.append("no_order_ack")
    if review.get("order_attempt_trace_linked") is not True:
        failures.append("order_attempt_trace_missing")
    if review.get("venue_order_id_linked") is not True:
        failures.append("venue_order_id_missing")
    if review.get("user_ws_connected_count", 0) <= 0:
        failures.append("user_ws_not_connected")
    if review.get("user_ws_subscribe_sent_count", 0) <= 0:
        failures.append("user_ws_not_subscribed")
    if review.get("event_decode_error_count") != 0 or event_stats.get("event_decode_error_count", 0) != 0:
        failures.append("event_decode_error")
    if review.get("user_ws_decode_error_count") != 0:
        failures.append("user_ws_decode_error")
    if review.get("recorder_critical_drop_count") != 0:
        failures.append("recorder_critical_drop")
    if review.get("unexpected_taker_fill_count") != 0:
        failures.append("unexpected_taker_fill")
    if review.get("max_live_orders_observed", 999) > MAX_LIVE_ORDERS:
        failures.append("max_live_orders_exceeded")
    if float(review.get("max_open_cost_usdc_observed", 999.0)) > MAX_OPEN_COST_USDC:
        failures.append("max_open_cost_exceeded")
    if float(review.get("max_strategy_exposure_usdc_observed", 999.0)) > MAX_STRATEGY_EXPOSURE_USDC:
        failures.append("max_strategy_exposure_exceeded")
    for field in (
        "broker_started_or_modified",
        "systemd_or_service_control_used",
        "modified_shared_ingress",
        "env_files_written",
        "secret_values_recorded",
        "secret_values_written_to_disk",
    ):
        if review.get(field) is not False:
            failures.append(field)
    if event_stats.get("forbidden_canary_event_count", 0) > 0:
        failures.append("forbidden_canary_event")
    if secret_hits:
        failures.append("secret_sentinel_leak")
    return failures


def summarize(run_dir: Path, sentinels: list[str]) -> dict[str, Any]:
    review_path = first_existing(
        run_dir,
        (
            "g2_canary_acceptance_review.json",
            "local_acceptance_review.json",
            "canary_acceptance_review.json",
        ),
    )
    run_manifest_path = first_existing(
        run_dir,
        (
            "g2_canary_run_manifest.json",
            "run_manifest.json",
        ),
    )
    review = read_json(review_path) if review_path else {}
    run_manifest = read_json(run_manifest_path) if run_manifest_path else {}
    recorder_root = Path(run_manifest.get("recorder_root") or review.get("recorder_root") or run_dir / "recorder")

    event_stats: dict[str, Any] = {
        "events_files": [],
        "event_decode_error_count": 0,
        "order_accepted_count": 0,
        "user_ws_fill_parsed_count": 0,
        "forbidden_canary_event_count": 0,
        "forbidden_canary_event_samples": [],
    }
    if recorder_root.exists():
        for path in sorted(recorder_root.glob("**/events.jsonl")):
            event_stats["events_files"].append(str(path))
            for lineno, value, error in iter_jsonl(path):
                if error:
                    event_stats["event_decode_error_count"] += 1
                    continue
                payload = (value or {}).get("payload") or {}
                event = str(payload.get("event") or "")
                lower = event.lower()
                if event == "order_accepted":
                    event_stats["order_accepted_count"] += 1
                if event == "user_ws_fill_parsed":
                    event_stats["user_ws_fill_parsed_count"] += 1
                if any(fragment in lower for fragment in FORBIDDEN_CANARY_EVENT_FRAGMENTS):
                    event_stats["forbidden_canary_event_count"] += 1
                    if len(event_stats["forbidden_canary_event_samples"]) < 5:
                        event_stats["forbidden_canary_event_samples"].append(
                            {"path": str(path), "lineno": lineno, "event": event}
                        )

    secret_hits = scan_secret_sentinels(run_dir, sentinels)
    failures = evaluate(review, event_stats, secret_hits)
    status = "PASS_G2_CANARY_ACCEPTANCE" if not failures else "FAIL_G2_CANARY_ACCEPTANCE"
    return {
        "artifact": "xuan_b27_dplus_g2_canary_run_summary",
        "run_dir": str(run_dir),
        "review_path": str(review_path) if review_path else None,
        "run_manifest_path": str(run_manifest_path) if run_manifest_path else None,
        "recorder_root": str(recorder_root),
        "status": status,
        "failures": failures,
        "review": {
            "exact_approval_scope": review.get("exact_approval_scope"),
            "strategy": review.get("strategy"),
            "market_slug": review.get("market_slug"),
            "shared_ingress_role": review.get("shared_ingress_role"),
            "shared_ingress_root": review.get("shared_ingress_root"),
            "gate_status": review.get("gate_status"),
            "source_truth_all_pass": review.get("source_truth_all_pass"),
            "order_ack_count": review.get("order_ack_count"),
            "max_live_orders_observed": review.get("max_live_orders_observed"),
            "max_open_cost_usdc_observed": review.get("max_open_cost_usdc_observed"),
            "max_strategy_exposure_usdc_observed": review.get("max_strategy_exposure_usdc_observed"),
        },
        "event_stats": event_stats,
        "secret_scan": {
            "sentinel_count": len(sentinels),
            "hit_count": len(secret_hits),
            "hits": secret_hits,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "orders_sent_by_summary": False,
            "cancels_sent_by_summary": False,
            "redeems_sent_by_summary": False,
            "broker_modified_by_summary": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check-acceptance", action="store_true")
    parser.add_argument("--secret-sentinel", action="append", default=[])
    args = parser.parse_args()

    summary = summarize(args.run_dir, args.secret_sentinel)
    text = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    else:
        sys.stdout.write(text)
    if args.check_acceptance and summary["status"] != "PASS_G2_CANARY_ACCEPTANCE":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
