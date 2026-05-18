#!/usr/bin/env python3
"""Produce D+ dry-run outcome labels from local L1 recorder artifacts.

The labeler joins local D+ observer edge samples to the same run's local
``market_md.jsonl`` L1 books. A candidate is positive only when its passive
observer price is touched by the future same-side ask inside the lookahead
window. Untouched candidates are kept as negative dry-run outcomes, so this
cannot turn edge-only observer samples into a fake pass.

This reads only ``xuan_research_artifacts`` paths. It does not read raw/replay
stores, connect to network, start observers, or touch order/cancel/redeem
paths.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_L1_DRY_RUN_OUTCOME_LABELS"

RC_BY_STATUS = {
    PASS_STATUS: 0,
    "FAIL_UNSAFE_INPUT_PATH": 2,
    "FAIL_OBSERVER_SUMMARY_NOT_SAFE": 3,
    "FAIL_EDGE_SAMPLE_DECODE": 4,
    "FAIL_EVENT_DECODE": 5,
    "FAIL_MARKET_MD_DECODE": 6,
    "FAIL_MARKET_SCOPE": 7,
    "FAIL_FORBIDDEN_SIDE_EFFECT": 8,
    "FAIL_INSUFFICIENT_LABELS": 9,
    "FAIL_REALIZED_OUTCOME_QUALITY": 10,
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


def market_prefix_ok(value: Any, prefix: str) -> bool:
    return isinstance(value, str) and (value == prefix or value.startswith(f"{prefix}-"))


def safe_local_artifact_path(root: Path, path: Path) -> bool:
    resolved = path.resolve()
    artifacts = (root / "xuan_research_artifacts").resolve()
    if artifacts not in resolved.parents and resolved != artifacts:
        return False
    parts = {part.lower() for part in resolved.parts}
    forbidden = {"raw", "replay", "replay_published"}
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
    for source in (data, record.get("payload"), record):
        if isinstance(source, dict) and isinstance(source.get("event"), str):
            return str(source["event"])
    return ""


def has_forbidden_side_effect(record: dict[str, Any]) -> bool:
    side_effects = record.get("side_effects") or {}
    return any(record.get(field) is True for field in FORBIDDEN_FIELDS) or any(
        side_effects.get(field) is True for field in FORBIDDEN_FIELDS
    )


def observer_summary_safe(summary: dict[str, Any], market_prefix: str) -> bool:
    run_manifest = summary.get("run_manifest") or {}
    candidates = summary.get("observer_candidate_counts") or {}
    markets = summary.get("markets") or []
    return (
        summary.get("artifact") == "xuan_b27_dplus_observer_summary"
        and summary.get("status") == "PASS_NO_ORDER_OBSERVER"
        and as_int(summary.get("decode_error_count")) == 0
        and as_int(summary.get("truncated_final_line_count")) <= 1
        and as_int(summary.get("no_order_violation_count")) == 0
        and as_int(candidates.get("would_track")) > 0
        and as_int(candidates.get("would_place")) == 0
        and as_int(candidates.get("submitted_previews")) == 0
        and run_manifest.get("dry_run") is True
        and run_manifest.get("orders_allowed") is False
        and run_manifest.get("cancels_allowed") is False
        and run_manifest.get("redeems_allowed") is False
        and bool(markets)
        and all(market_prefix_ok(market, market_prefix) for market in markets)
    )


class RangeMin:
    def __init__(self, values: list[float]) -> None:
        self.values = values
        self.logs = [0] * (len(values) + 1)
        for index in range(2, len(values) + 1):
            self.logs[index] = self.logs[index // 2] + 1
        self.table: list[list[float]] = []
        if values:
            self.table.append(values[:])
            span = 1
            while span * 2 <= len(values):
                prev = self.table[-1]
                self.table.append(
                    [min(prev[index], prev[index + span]) for index in range(len(values) - span * 2 + 1)]
                )
                span *= 2

    def min(self, left: int, right: int) -> float:
        if left >= right or not self.values:
            return math.inf
        length = right - left
        power = self.logs[length]
        span = 1 << power
        return min(self.table[power][left], self.table[power][right - span])


def first_touch_index(range_min: RangeMin, left: int, right: int, price: float) -> int | None:
    if left >= right or range_min.min(left, right) > price:
        return None
    lo = left
    hi = right - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if range_min.min(left, mid + 1) <= price:
            hi = mid
        else:
            lo = mid + 1
    return lo


def candidate_join_key(record: dict[str, Any]) -> str | None:
    candidate_id = record.get("candidate_id")
    if isinstance(candidate_id, str) and candidate_id:
        return candidate_id
    return None


def find_candidate_events(
    root: Path,
    run_dir: Path,
    edge_samples: list[dict[str, Any]],
    market_prefix: str,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], int, int]:
    wanted = {
        key for sample in edge_samples if (key := candidate_join_key(sample))
    }
    found: dict[str, dict[str, Any]] = {}
    decode_errors: list[dict[str, Any]] = []
    wrong_market_count = 0
    forbidden_event_count = 0

    event_files = sorted((run_dir / "recorder").rglob("events.jsonl"))
    for path in event_files:
        if len(found) >= len(wanted):
            break
        if not safe_local_artifact_path(root, path):
            decode_errors.append({"file": str(path), "line": None, "error": "unsafe_path"})
            continue
        with path.open() as fh:
            for line_no, raw in enumerate(fh, 1):
                if len(found) >= len(wanted):
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
                if event_name(record, data) in FORBIDDEN_EVENTS or has_forbidden_side_effect(record):
                    forbidden_event_count += 1
                    continue
                candidates = data.get("observer_candidates")
                if not isinstance(candidates, list):
                    continue
                market = data.get("market_slug") or data.get("slug") or record.get("slug") or path.parent.name
                if not market_prefix_ok(market, market_prefix):
                    wrong_market_count += 1
                    continue
                recv_unix_ms = as_int(record.get("recv_unix_ms"))
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    candidate_id = candidate.get("candidate_id")
                    if not isinstance(candidate_id, str) or candidate_id not in wanted or candidate_id in found:
                        continue
                    trace = candidate.get("order_attempt_trace_preview") or {}
                    found[candidate_id] = {
                        "candidate_id": candidate_id,
                        "market_slug": market,
                        "recv_unix_ms": recv_unix_ms,
                        "side": candidate.get("side"),
                        "observer_price": as_float(candidate.get("observer_price")),
                        "book_ask": as_float(candidate.get("book_ask")),
                        "book_bid": as_float(candidate.get("book_bid")),
                        "target_qty": as_float(candidate.get("target_qty")),
                        "order_attempt_id": trace.get("order_attempt_id"),
                    }
    return found, decode_errors, wrong_market_count, forbidden_event_count


def load_market_books(
    root: Path,
    run_dir: Path,
    slugs: set[str],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    books: dict[str, dict[str, Any]] = {}
    decode_errors: list[dict[str, Any]] = []
    for slug in sorted(slugs):
        matches = sorted((run_dir / "recorder").glob(f"*/*{slug}/market_md.jsonl"))
        if not matches:
            matches = sorted((run_dir / "recorder").rglob(f"{slug}/market_md.jsonl"))
        path = matches[0] if matches else None
        times: list[int] = []
        yes_asks: list[float] = []
        no_asks: list[float] = []
        if path and safe_local_artifact_path(root, path):
            with path.open() as fh:
                for line_no, raw in enumerate(fh, 1):
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as exc:
                        decode_errors.append({"file": str(path), "line": line_no, "error": str(exc)})
                        continue
                    payload = record.get("payload") or {}
                    if payload.get("kind") != "book_l1":
                        continue
                    times.append(as_int(record.get("recv_unix_ms")))
                    yes_asks.append(as_float(payload.get("yes_ask"), math.inf))
                    no_asks.append(as_float(payload.get("no_ask"), math.inf))
        elif path:
            decode_errors.append({"file": str(path), "line": None, "error": "unsafe_path"})
        else:
            decode_errors.append({"file": slug, "line": None, "error": "missing_market_md"})
        books[slug] = {
            "path": str(path) if path else None,
            "times": times,
            "yes_asks": yes_asks,
            "no_asks": no_asks,
            "yes_min": RangeMin(yes_asks),
            "no_min": RangeMin(no_asks),
        }
    return books, decode_errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--observer-summary", type=Path)
    parser.add_argument("--edge-samples", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--lookahead-seconds", type=float, default=60.0)
    parser.add_argument("--miss-penalty-ratio", type=float, default=1.0)
    parser.add_argument("--min-labels", type=int, default=100)
    parser.add_argument("--min-touch-ratio", type=float, default=0.5)
    parser.add_argument("--min-mean-realized-edge-bps", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_l1_dry_run_outcome_labels_{label}"
    labels_path = out_dir / "outcome_labels.jsonl"
    summary_path = args.observer_summary or args.run_dir / "observer_summary.json"

    path_safe = (
        safe_local_artifact_path(root, args.run_dir)
        and safe_local_artifact_path(root, summary_path)
        and safe_local_artifact_path(root, args.edge_samples)
        and safe_local_artifact_path(root, out_dir)
    )
    summary = read_json(summary_path) if path_safe else {}
    edge_samples, edge_errors = read_jsonl(args.edge_samples) if path_safe else ([], [])
    summary_safe = observer_summary_safe(summary, args.market_prefix)

    candidate_events, event_decode_errors, event_wrong_market_count, forbidden_event_count = (
        find_candidate_events(root, args.run_dir, edge_samples, args.market_prefix)
        if path_safe
        else ({}, [], 0, 0)
    )
    slugs = {
        event["market_slug"]
        for event in candidate_events.values()
        if isinstance(event.get("market_slug"), str)
    }
    books, market_decode_errors = load_market_books(root, args.run_dir, slugs) if path_safe else ({}, [])

    labels: list[dict[str, Any]] = []
    missing_event_count = 0
    missing_book_count = 0
    wrong_market_count = 0
    forbidden_sample_count = 0
    duplicate_sample_count = 0
    lookahead_ms = int(args.lookahead_seconds * 1000.0)
    seen_candidate_ids: set[str] = set()

    for sample in edge_samples:
        market = sample.get("market_slug") or sample.get("slug")
        if not market_prefix_ok(market, args.market_prefix):
            wrong_market_count += 1
            continue
        if has_forbidden_side_effect(sample):
            forbidden_sample_count += 1
            continue
        candidate_id = sample.get("candidate_id")
        if not isinstance(candidate_id, str):
            missing_event_count += 1
            continue
        if candidate_id in seen_candidate_ids:
            duplicate_sample_count += 1
            continue
        seen_candidate_ids.add(candidate_id)
        event = candidate_events.get(candidate_id)
        if not event:
            missing_event_count += 1
            continue
        book = books.get(str(event["market_slug"]))
        if not book or not book.get("times"):
            missing_book_count += 1
            continue
        times = book["times"]
        start = bisect.bisect_right(times, as_int(event["recv_unix_ms"]))
        end = bisect.bisect_right(times, as_int(event["recv_unix_ms"]) + lookahead_ms)
        side = str(event.get("side"))
        observer_price = as_float(event.get("observer_price"))
        range_min = book["yes_min"] if side == "yes" else book["no_min"]
        index = first_touch_index(range_min, start, end, observer_price)
        touched = index is not None
        touch_ask = (
            (book["yes_asks"] if side == "yes" else book["no_asks"])[index]
            if index is not None
            else None
        )
        edge_bps = as_float(sample.get("edge_bps", sample.get("candidate_edge_bps")))
        realized_edge_bps = edge_bps if touched else -edge_bps * args.miss_penalty_ratio
        labels.append(
            {
                "event": "xuan_b27_dplus_dry_run_outcome_label",
                "market_slug": event.get("market_slug"),
                "candidate_id": candidate_id,
                "order_attempt_id": sample.get("order_attempt_id") or event.get("order_attempt_id"),
                "outcome_label_id": f"l1_dry_run:{candidate_id}",
                "outcome_label_source": "local_l1_book_touch_dry_run",
                "performance_basis": "dry_run_outcome",
                "side": side,
                "candidate_event_unix_ms": event.get("recv_unix_ms"),
                "lookahead_seconds": args.lookahead_seconds,
                "fill_touched": touched,
                "touch_unix_ms": times[index] if index is not None else None,
                "touch_latency_ms": (
                    times[index] - as_int(event["recv_unix_ms"]) if index is not None else None
                ),
                "observer_price": observer_price,
                "candidate_book_ask": event.get("book_ask"),
                "future_touch_ask": touch_ask,
                "candidate_edge_bps": edge_bps,
                "realized_edge_bps": realized_edge_bps,
                "expected_value_bps": realized_edge_bps,
                "orders_sent": False,
                "cancels_sent": False,
                "redeems_sent": False,
                "auth_network_started": False,
                "started_observer": False,
                "started_user_ws": False,
                "started_canary": False,
            }
        )

    realized_values = [as_float(label.get("realized_edge_bps")) for label in labels]
    touched_count = sum(1 for label in labels if label.get("fill_touched") is True)
    touch_ratio = touched_count / len(labels) if labels else 0.0
    mean_realized_edge_bps = statistics.fmean(realized_values) if realized_values else 0.0
    median_realized_edge_bps = statistics.median(realized_values) if realized_values else 0.0

    failures: list[str] = []
    if not path_safe:
        failures.append("unsafe_input_path")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif not summary_safe:
        failures.append("observer_summary_not_safe")
        status = "FAIL_OBSERVER_SUMMARY_NOT_SAFE"
    elif edge_errors:
        failures.append("edge_sample_decode_error")
        status = "FAIL_EDGE_SAMPLE_DECODE"
    elif event_decode_errors:
        failures.append("event_decode_error")
        status = "FAIL_EVENT_DECODE"
    elif market_decode_errors:
        failures.append("market_md_decode_error")
        status = "FAIL_MARKET_MD_DECODE"
    elif wrong_market_count or event_wrong_market_count:
        failures.append("market_scope_failed")
        status = "FAIL_MARKET_SCOPE"
    elif forbidden_sample_count or forbidden_event_count:
        failures.append("forbidden_side_effect")
        status = "FAIL_FORBIDDEN_SIDE_EFFECT"
    elif len(labels) < args.min_labels or missing_event_count or missing_book_count:
        failures.append("insufficient_l1_dry_run_labels")
        status = "FAIL_INSUFFICIENT_LABELS"
    elif touch_ratio < args.min_touch_ratio or mean_realized_edge_bps < args.min_mean_realized_edge_bps:
        failures.append("realized_outcome_quality_failed")
        status = "FAIL_REALIZED_OUTCOME_QUALITY"
    else:
        status = PASS_STATUS

    out_dir.mkdir(parents=True, exist_ok=True)
    with labels_path.open("w") as fh:
        for label_record in labels:
            fh.write(json.dumps(label_record, sort_keys=True) + "\n")

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_l1_dry_run_outcome_labels",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_l1_dry_run_outcome_label_producer",
        "run_dir": str(args.run_dir),
        "observer_summary": str(summary_path),
        "edge_samples": str(args.edge_samples),
        "outcome_labels": str(labels_path),
        "path_safe": path_safe,
        "observer_summary_safe": summary_safe,
        "lookahead_seconds": args.lookahead_seconds,
        "miss_penalty_ratio": args.miss_penalty_ratio,
        "label_count": len(labels),
        "touched_count": touched_count,
        "touch_ratio": touch_ratio,
        "mean_realized_edge_bps": mean_realized_edge_bps,
        "median_realized_edge_bps": median_realized_edge_bps,
        "missing_candidate_event_count": missing_event_count,
        "missing_book_count": missing_book_count,
        "duplicate_sample_count": duplicate_sample_count,
        "wrong_market_count": wrong_market_count + event_wrong_market_count,
        "forbidden_side_effect_count": forbidden_sample_count + forbidden_event_count,
        "edge_decode_error_count": len(edge_errors),
        "event_decode_error_count": len(event_decode_errors),
        "market_md_decode_error_count": len(market_decode_errors),
        "failures": failures,
        "performance_basis": "dry_run_outcome",
        "real_outcome_labels_published": status == PASS_STATUS,
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
            "feed labels into no-order shadow run artifact and Rust strategy acceptance runner"
            if status == PASS_STATUS
            else "inspect L1 dry-run label quality before strategy acceptance"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
