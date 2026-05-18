#!/usr/bin/env python3
"""Extract local D+ observer edge samples from no-order recorder artifacts.

The output is edge-sample input, not strategy performance acceptance. Samples
are marked ``observer_edge_only_not_realized`` so the performance gate cannot
mistake them for independent outcome evidence.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_EVENTS = {
    "order_accepted",
    "order_rejected",
    "cancel_accepted",
    "cancel_rejected",
    "redeem_result",
    "merge_executed",
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


def safe_artifact_path(root: Path, path: Path) -> bool:
    resolved = path.resolve()
    artifacts = (root / "xuan_research_artifacts").resolve()
    if artifacts not in resolved.parents and resolved != artifacts:
        return False
    parts = {part.lower() for part in resolved.parts}
    forbidden = {"raw", "replay_published"}
    return not (parts & forbidden) and "/mnt/poly-replay" not in str(resolved)


def event_data(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return record


def event_name(record: dict[str, Any], data: dict[str, Any]) -> str:
    event = data.get("event")
    if isinstance(event, str):
        return event
    payload = record.get("payload")
    if isinstance(payload, dict):
        event = payload.get("event")
        if isinstance(event, str):
            return event
    event = record.get("event")
    return event if isinstance(event, str) else ""


def market_prefix_ok(market: str, prefix: str) -> bool:
    return market == prefix or market.startswith(f"{prefix}-")


def observer_summary_safe(summary: dict[str, Any], market_prefix: str) -> bool:
    run_manifest = summary.get("run_manifest") or {}
    candidate_counts = summary.get("observer_candidate_counts") or {}
    markets = summary.get("markets") or []
    return (
        summary.get("artifact") == "xuan_b27_dplus_observer_summary"
        and summary.get("status") == "PASS_NO_ORDER_OBSERVER"
        and as_int(summary.get("decode_error_count")) == 0
        and as_int(summary.get("no_order_violation_count")) == 0
        and as_int(candidate_counts.get("would_track")) > 0
        and as_int(candidate_counts.get("would_place")) == 0
        and as_int(candidate_counts.get("submitted_previews")) == 0
        and run_manifest.get("dry_run") is True
        and run_manifest.get("orders_allowed") is False
        and run_manifest.get("cancels_allowed") is False
        and run_manifest.get("redeems_allowed") is False
        and bool(markets)
        and all(isinstance(market, str) and market_prefix_ok(market, market_prefix) for market in markets)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--observer-summary", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--max-samples", type=int, default=5000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_edge_samples_{label}"
    summary_path = args.observer_summary or args.run_dir / "observer_summary.json"
    edge_samples_path = out_dir / "edge_samples.jsonl"

    path_safe = safe_artifact_path(root, args.run_dir) and safe_artifact_path(root, summary_path)
    summary = read_json(summary_path)
    summary_safe = observer_summary_safe(summary, args.market_prefix)

    samples: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    forbidden_events: list[dict[str, Any]] = []
    wrong_market_count = 0
    event_files = sorted((args.run_dir / "recorder").rglob("events.jsonl")) if path_safe else []

    for path in event_files:
        if len(samples) >= args.max_samples:
            break
        if not safe_artifact_path(root, path):
            decode_errors.append({"file": str(path), "line": None, "error": "unsafe_artifact_path"})
            continue
        with path.open() as fh:
            for line_no, raw in enumerate(fh, 1):
                if len(samples) >= args.max_samples:
                    break
                line = raw.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    decode_errors.append({"file": str(path), "line": line_no, "error": str(exc)})
                    continue
                data = event_data(record)
                ev = event_name(record, data)
                if ev in FORBIDDEN_EVENTS:
                    forbidden_events.append({"file": str(path), "line": line_no, "event": ev})
                    continue
                candidates = data.get("observer_candidates")
                if not isinstance(candidates, list):
                    continue
                market_slug = data.get("slug") or data.get("market_slug") or record.get("slug") or path.parent.name
                if not isinstance(market_slug, str) or not market_prefix_ok(market_slug, args.market_prefix):
                    wrong_market_count += 1
                    continue
                for candidate in candidates:
                    if len(samples) >= args.max_samples:
                        break
                    if not isinstance(candidate, dict):
                        continue
                    book_ask = as_float(candidate.get("book_ask"))
                    observer_price = as_float(candidate.get("observer_price"))
                    edge_bps = max(0.0, (book_ask - observer_price) * 10_000.0)
                    trace = candidate.get("order_attempt_trace_preview") or {}
                    samples.append(
                        {
                            "market_slug": market_slug,
                            "candidate_id": candidate.get("candidate_id"),
                            "side": candidate.get("side"),
                            "accepted": candidate.get("would_track") is True,
                            "would_track": candidate.get("would_track") is True,
                            "would_place": candidate.get("would_place") is True,
                            "blocked_reason": candidate.get("blocked_reason"),
                            "book_bid": candidate.get("book_bid"),
                            "book_ask": book_ask,
                            "observer_price": observer_price,
                            "edge_bps": edge_bps,
                            "expected_value_bps": edge_bps,
                            "performance_basis": "observer_edge_only_not_realized",
                            "order_attempt_id": trace.get("order_attempt_id"),
                            "preview_only": trace.get("preview_only"),
                            "submitted": trace.get("submitted"),
                            "orders_sent": False,
                            "cancels_sent": False,
                            "redeems_sent": False,
                            "auth_network_started": False,
                            "started_canary": False,
                        }
                    )

    accepted_count = sum(1 for sample in samples if sample.get("accepted") is True)
    would_place_count = sum(1 for sample in samples if sample.get("would_place") is True)
    submitted_count = sum(1 for sample in samples if sample.get("submitted") is True)
    positive_edge_count = sum(1 for sample in samples if as_float(sample.get("edge_bps")) > 0.0)

    status = (
        "PASS_EDGE_SAMPLES_EXTRACTED"
        if path_safe
        and summary_safe
        and samples
        and accepted_count > 0
        and would_place_count == 0
        and submitted_count == 0
        and positive_edge_count > 0
        and not decode_errors
        and not forbidden_events
        and wrong_market_count == 0
        else "FAIL_EDGE_SAMPLE_EXTRACTION"
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    with edge_samples_path.open("w") as fh:
        for sample in samples:
            fh.write(json.dumps(sample, sort_keys=True) + "\n")

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_edge_samples",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_observer_edge_sample_extraction",
        "run_dir": str(args.run_dir),
        "observer_summary": str(summary_path),
        "path_safe": path_safe,
        "observer_summary_safe": summary_safe,
        "edge_samples": str(edge_samples_path),
        "sample_count": len(samples),
        "accepted_candidate_count": accepted_count,
        "positive_edge_candidate_count": positive_edge_count,
        "would_place_count": would_place_count,
        "submitted_count": submitted_count,
        "decode_error_count": len(decode_errors),
        "decode_errors_sample": decode_errors[:5],
        "forbidden_event_count": len(forbidden_events),
        "forbidden_events_sample": forbidden_events[:5],
        "wrong_market_count": wrong_market_count,
        "performance_basis": "observer_edge_only_not_realized",
        "strategy_performance_evidence_ready": False,
        "requires_independent_outcome_evidence": True,
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
        "next_gate": "add independent outcome/replay labels before strategy performance evidence can pass",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == "PASS_EDGE_SAMPLES_EXTRACTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
