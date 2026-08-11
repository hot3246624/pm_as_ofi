#!/usr/bin/env python3
"""Outcome-blind TWAP-specific candidate / candidate-side ask replay.

This is a research replay, not a trading adapter.  It deliberately separates:

* source event-time cutoff (what could influence an end-of-round estimate),
* source receive-time cutoff (what was available before the decision deadline),
* candidate ready time, and
* the candidate token's public ask/depth after that ready time.

The Chainlink sampling and weighting rules are not public.  The default
``equal_source_median`` proxy is therefore a frozen, conservative research
proxy and must not be described as a bit-for-bit Chainlink reproduction.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


SCHEMA_VERSION = 1
DEFAULT_WINDOW_MS = 30_000
DEFAULT_DECISION_DEADLINE_MS = 300
DEFAULT_COMPUTE_LATENCY_MS = 25
DEFAULT_HORIZON_MS = 120_000
DEFAULT_MIN_SOURCES = 2
DEFAULT_MIN_COVERAGE = 0.80
DEFAULT_MAX_STALE_MS = 5_000
DEFAULT_THRESHOLDS = (0.90, 0.95, 0.98, 0.99, 1.00)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", type=Path, help="Optional TWAP capture directory containing metadata/settlement/CLOB JSONL")
    parser.add_argument("--rounds", type=Path, help="CSV or JSONL round universe; capture metadata is used when omitted")
    parser.add_argument("--source-tape", type=Path, help="External source event-time JSONL or JSONL.gz")
    parser.add_argument("--clob-events", type=Path, help="CLOB book_events.jsonl; defaults to capture-dir/book_events.jsonl")
    parser.add_argument("--metadata", type=Path, help="market_metadata.jsonl; defaults to capture-dir/market_metadata.jsonl")
    parser.add_argument("--labels", type=Path, help="Optional settlement/public-label JSONL; defaults to capture-dir/settlement_observations.jsonl")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    parser.add_argument("--decision-deadline-ms", type=int, default=DEFAULT_DECISION_DEADLINE_MS)
    parser.add_argument("--compute-latency-ms", type=int, default=DEFAULT_COMPUTE_LATENCY_MS)
    parser.add_argument("--horizon-ms", type=int, default=DEFAULT_HORIZON_MS)
    parser.add_argument("--min-sources", type=int, default=DEFAULT_MIN_SOURCES)
    parser.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    parser.add_argument("--max-stale-ms", type=int, default=DEFAULT_MAX_STALE_MS)
    parser.add_argument("--thresholds", default=",".join(str(x) for x in DEFAULT_THRESHOLDS))
    parser.add_argument("--source", action="append", dest="sources", help="Optional source allow-list; repeat or comma-separate")
    args = parser.parse_args(argv)
    if args.window_ms <= 0 or args.decision_deadline_ms < 0 or args.compute_latency_ms < 0 or args.horizon_ms <= 0:
        parser.error("window/deadline/compute/horizon values are invalid")
    if args.min_sources < 1 or not 0 < args.min_coverage <= 1 or args.max_stale_ms < 0:
        parser.error("min-sources/min-coverage/max-stale-ms values are invalid")
    try:
        args.thresholds = sorted({float(value) for value in args.thresholds.split(",")})
    except ValueError as exc:
        parser.error(f"invalid --thresholds: {exc}")
    if not args.thresholds or any(not 0 < value <= 1 for value in args.thresholds):
        parser.error("thresholds must be in (0, 1]")
    sources: List[str] = []
    for value in args.sources or []:
        sources.extend(item.strip().lower() for item in value.split(",") if item.strip())
    args.sources = sorted(set(sources))
    return args


def sha256_file(path: Optional[Path]) -> Optional[str]:
    if path is None or not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def open_text(path: Path):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with open_text(path) as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL {path}:{line_number}: {exc}") from exc
            if isinstance(value, dict):
                yield value


def read_delimited(path: Path) -> Iterator[Dict[str, Any]]:
    if path.suffix == ".csv":
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            yield from csv.DictReader(handle)
    else:
        yield from iter_jsonl(path)


def finite_number(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def integer(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number


def timestamp_ms(value: Any) -> Optional[int]:
    number = finite_number(value)
    if number is not None:
        return int(number * 1000 if abs(number) < 100_000_000_000 else number)
    return None


def latest_by_key(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        value = str(row.get(key) or "")
        if value:
            result[value] = row
    return result


def default_capture_file(args: argparse.Namespace, name: str) -> Optional[Path]:
    if args.capture_dir is None:
        return None
    candidate = args.capture_dir / name
    return candidate if candidate.exists() else None


def normalize_side(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if text in {"up", "yes", "1", "true"}:
        return "Up"
    if text in {"down", "no", "0", "false"}:
        return "Down"
    return None


def infer_symbol(row: Dict[str, Any]) -> str:
    raw = str(row.get("symbol") or row.get("asset") or "").strip().lower()
    if "/" in raw:
        return raw
    if raw:
        return f"{raw}/usd"
    slug = str(row.get("slug") or "")
    match = re.match(r"^([a-z0-9]+)-updown-", slug.lower())
    return f"{match.group(1)}/usd" if match else ""


def normalize_source_tick(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol = infer_symbol(row)
    source = str(row.get("source") or row.get("venue") or "").strip().lower()
    event_ms = timestamp_ms(row.get("event_ts_ms", row.get("ts_ms", row.get("timestamp"))))
    receive_ms = timestamp_ms(row.get("recv_ts_ms", row.get("receive_ms", row.get("recv_ms", row.get("local_receive_ms")))))
    price = None
    for field in ("mid", "price", "mark", "last", "value"):
        price = finite_number(row.get(field))
        if price is not None:
            break
    if not symbol or not source or event_ms is None or price is None or price <= 0:
        return None
    return {"symbol": symbol, "source": source, "event_ts_ms": event_ms, "recv_ts_ms": receive_ms, "price": price}


def load_source_tape(path: Path, allowed_sources: Sequence[str]) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    rows = 0
    invalid = 0
    receive_missing = 0
    for raw in read_delimited(path):
        rows += 1
        tick = normalize_source_tick(raw)
        if tick is None or (allowed_sources and tick["source"] not in allowed_sources):
            invalid += 1
            continue
        if tick["recv_ts_ms"] is None:
            receive_missing += 1
        by_symbol[tick["symbol"]].append(tick)
    for values in by_symbol.values():
        values.sort(key=lambda row: (row["event_ts_ms"], row["recv_ts_ms"] or -1, row["source"]))
    return by_symbol, {"rows": rows, "valid_rows": sum(len(values) for values in by_symbol.values()), "invalid_rows": invalid, "receive_missing_rows": receive_missing}


def parse_token_ids(row: Dict[str, Any]) -> List[str]:
    value = row.get("token_ids", row.get("clobTokenIds", []))
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except json.JSONDecodeError:
            pass
    return []


def parse_outcomes(row: Dict[str, Any]) -> List[str]:
    value = row.get("outcomes", [])
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except json.JSONDecodeError:
            pass
    return ["Up", "Down"]


def load_rounds(args: argparse.Namespace) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    metadata_path = args.metadata or default_capture_file(args, "market_metadata.jsonl")
    labels_path = args.labels or default_capture_file(args, "settlement_observations.jsonl")
    metadata = latest_by_key(read_delimited(metadata_path), "slug") if metadata_path and metadata_path.exists() else {}
    labels = latest_by_key(read_delimited(labels_path), "slug") if labels_path and labels_path.exists() else {}
    rounds: Dict[str, Dict[str, Any]] = {}
    if args.rounds and args.rounds.exists():
        for row in read_delimited(args.rounds):
            slug = str(row.get("slug") or "")
            if not slug:
                continue
            rounds[slug] = dict(row)
    else:
        rounds = {slug: dict(row) for slug, row in metadata.items()}
    normalized: List[Dict[str, Any]] = []
    for slug, raw in sorted(rounds.items()):
        meta = {**metadata.get(slug, {}), **raw}
        start_ms = timestamp_ms(meta.get("round_start_ts", meta.get("start_ts")))
        end_ms = timestamp_ms(meta.get("round_end_ts", meta.get("end_ts")))
        if start_ms is None or end_ms is None:
            anchor = integer(slug.rsplit("-", 1)[-1]) if slug else None
            if anchor is not None:
                start_ms = anchor * 1000
                end_ms = start_ms + 300_000
        if start_ms is None or end_ms is None:
            continue
        label = labels.get(slug, {})
        outcomes = parse_outcomes(meta)
        token_ids = parse_token_ids(meta)
        normalized.append({
            "slug": slug,
            "symbol": infer_symbol(meta),
            "round_start_ms": start_ms,
            "round_end_ms": end_ms,
            "outcomes": outcomes,
            "token_ids": token_ids,
            "public_outcome": normalize_side(label.get("outcome", meta.get("gamma_public_outcome", meta.get("outcome")))),
            "reference_value": finite_number(meta.get("reference_value", meta.get("rtds_open", meta.get("local_open")))),
            "reference_ts_ms": timestamp_ms(meta.get("reference_ts_ms", meta.get("rtds_open_ts_ms"))),
        })
    return normalized, {"metadata_path": str(metadata_path) if metadata_path else None, "labels_path": str(labels_path) if labels_path else None, "metadata_rows": len(metadata), "label_rows": len(labels)}


def integrate_twap(ticks: Sequence[Dict[str, Any]], window_end_ms: int, window_ms: int, decision_receive_cutoff_ms: int) -> Dict[str, Any]:
    window_start_ms = window_end_ms - window_ms
    eligible: List[Dict[str, Any]] = []
    future_event_count = 0
    late_receive_count = 0
    for tick in ticks:
        event_ms = tick["event_ts_ms"]
        receive_ms = tick["recv_ts_ms"]
        if event_ms > window_end_ms:
            future_event_count += 1
            continue
        if receive_ms is not None and receive_ms > decision_receive_cutoff_ms:
            late_receive_count += 1
            continue
        if event_ms >= window_start_ms:
            eligible.append(tick)
    eligible.sort(key=lambda row: (row["event_ts_ms"], row["recv_ts_ms"] or -1))
    if not eligible:
        return {"status": "missing", "reason": "no_event_time_ticks_in_window", "future_event_count": future_event_count, "late_receive_count": late_receive_count}
    prior = [tick for tick in ticks if tick["event_ts_ms"] < window_start_ms and (tick["recv_ts_ms"] is None or tick["recv_ts_ms"] <= decision_receive_cutoff_ms)]
    last_prior = max(prior, key=lambda row: (row["event_ts_ms"], row["recv_ts_ms"] or -1), default=None)
    points: List[Tuple[int, float, Optional[int]]] = []
    if last_prior is not None:
        points.append((window_start_ms, last_prior["price"], last_prior["recv_ts_ms"]))
    elif eligible[0]["event_ts_ms"] > window_start_ms:
        points.append((eligible[0]["event_ts_ms"], eligible[0]["price"], eligible[0]["recv_ts_ms"]))
    else:
        points.append((window_start_ms, eligible[0]["price"], eligible[0]["recv_ts_ms"]))
    for tick in eligible:
        if tick["event_ts_ms"] < points[-1][0]:
            continue
        if tick["event_ts_ms"] == points[-1][0]:
            points[-1] = (tick["event_ts_ms"], tick["price"], tick["recv_ts_ms"])
        else:
            points.append((tick["event_ts_ms"], tick["price"], tick["recv_ts_ms"]))
    integral = 0.0
    covered_start = points[0][0]
    max_receive = None
    for index, (ts_ms, price, receive_ms) in enumerate(points):
        next_ts = points[index + 1][0] if index + 1 < len(points) else window_end_ms
        next_ts = max(ts_ms, min(window_end_ms, next_ts))
        integral += price * max(0, next_ts - ts_ms)
        if receive_ms is not None:
            max_receive = max(max_receive or receive_ms, receive_ms)
    covered_ms = max(0, window_end_ms - covered_start)
    last_tick = points[-1]
    stale_ms = max(0, window_end_ms - last_tick[0])
    return {
        "status": "ok",
        "value": integral / covered_ms if covered_ms else None,
        "covered_ms": covered_ms,
        "coverage": covered_ms / window_ms,
        "last_event_ts_ms": last_tick[0],
        "last_stale_ms": stale_ms,
        "max_used_receive_ms": max_receive,
        "tick_count": len(eligible),
        "future_event_count": future_event_count,
        "late_receive_count": late_receive_count,
    }


def source_proxy(round_row: Dict[str, Any], tape: Dict[str, List[Dict[str, Any]]], args: argparse.Namespace, end_ms: int) -> Dict[str, Any]:
    symbol = round_row["symbol"]
    source_results: Dict[str, Dict[str, Any]] = {}
    end_by_source: Dict[str, Dict[str, Any]] = {}
    start_by_source: Dict[str, Dict[str, Any]] = {}
    for source, values in sorted((source, rows) for source, rows in group_by_source(tape.get(symbol, [])).items()):
        end_result = integrate_twap(values, end_ms, args.window_ms, end_ms + args.decision_deadline_ms)
        start_result = integrate_twap(values, round_row["round_start_ms"], args.window_ms, round_row["round_start_ms"])
        end_by_source[source] = end_result
        start_by_source[source] = start_result
        if end_result.get("status") == "ok":
            source_results[source] = {"end": end_result, "start": start_result}
    usable = {
        source: result for source, result in source_results.items()
        if result["end"].get("coverage", 0) >= args.min_coverage
        and result["end"].get("last_stale_ms", math.inf) <= args.max_stale_ms
    }
    values = sorted(result["end"]["value"] for result in usable.values() if result["end"].get("value") is not None)
    start_values = sorted(result["start"]["value"] for result in usable.values() if result["start"].get("status") == "ok" and result["start"].get("value") is not None)
    if round_row.get("reference_value") is not None:
        start_values = [round_row["reference_value"]]
    if len(values) < args.min_sources:
        return {"status": "missing", "reason": "insufficient_end_sources", "source_count": len(values), "source_results": source_results, "future_event_count": sum(result.get("end", {}).get("future_event_count", 0) for result in source_results.values())}
    if not start_values:
        return {"status": "missing", "reason": "missing_start_reference_twap", "source_count": len(values), "source_results": source_results}
    end_value = median(values)
    start_value = median(start_values)
    delta_bps = (end_value - start_value) / start_value * 10_000 if start_value else None
    max_receive = max((result["end"].get("max_used_receive_ms") or end_ms for result in usable.values()), default=end_ms)
    candidate_ready_ms = max(end_ms, max_receive) + args.compute_latency_ms
    receive_complete = all(result["end"].get("max_used_receive_ms") is not None for result in usable.values())
    return {
        "status": "ok",
        "aggregation": "equal_source_median",
        "source_count": len(values),
        "sources": sorted(usable),
        "source_results": source_results,
        "start_value": start_value,
        "end_value": end_value,
        "delta_bps": delta_bps,
        "candidate_side": "Up" if end_value >= start_value else "Down",
        "candidate_ready_ms": candidate_ready_ms,
        "preclose_ready": candidate_ready_ms < end_ms,
        "receive_complete": receive_complete,
        "source_timestamp_cutoff_ms": end_ms,
        "source_receive_cutoff_ms": end_ms + args.decision_deadline_ms,
    }


def group_by_source(ticks: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for tick in ticks:
        result[tick["source"]].append(tick)
    return result


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def quote_from_event(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    ask = finite_number(row.get("event_best_ask"))
    if ask is None:
        ask = finite_number(row.get("best_ask"))
    if ask is None:
        asks = row.get("asks")
        if isinstance(asks, list):
            levels = [finite_number(level.get("price")) for level in asks if isinstance(level, dict)]
            levels = [value for value in levels if value is not None]
            ask = min(levels) if levels else None
    depth = None
    asks = row.get("asks")
    if isinstance(asks, list) and ask is not None:
        for level in asks:
            if not isinstance(level, dict):
                continue
            price = finite_number(level.get("price"))
            size = finite_number(level.get("size"))
            if price is not None and size is not None and abs(price - ask) <= 1e-9:
                depth = size
                break
    if depth is None:
        depth = finite_number(row.get("ask_depth_at_best"))
    if depth is None and isinstance(row.get("depth_shares_within_1c"), dict):
        depth = finite_number(row["depth_shares_within_1c"].get("ask"))
    return ask, depth


def load_clob_survival(path: Path, candidate_rows: Sequence[Dict[str, Any]], args: argparse.Namespace) -> Dict[str, Any]:
    states: Dict[str, Dict[str, Any]] = {}
    for row in candidate_rows:
        token_id = row.get("candidate_token_id")
        if row.get("candidate_status") == "ok" and token_id:
            states[str(token_id)] = {"row": row, "baseline": None, "first_post": None, "thresholds": {str(threshold): {"threshold": threshold, "first_buyable": None, "first_not_buyable": None} for threshold in args.thresholds}}
    if not states:
        return {"status": "blocked_no_candidate_tokens", "event_rows_scanned": 0, "states": []}
    scanned = 0
    winner_rows = 0
    with open_text(path) as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid CLOB JSONL {path}:{line_number}: {exc}") from exc
            if not isinstance(event, dict):
                continue
            scanned += 1
            state = states.get(str(event.get("asset_id") or ""))
            if state is None:
                continue
            winner_rows += 1
            receive_ms = integer(event.get("receive_ms"))
            if receive_ms is None:
                continue
            ready_ms = state["row"]["candidate_ready_ms"]
            if receive_ms <= ready_ms:
                state["baseline"] = event
                continue
            if receive_ms > ready_ms + args.horizon_ms:
                continue
            if state["first_post"] is None:
                state["first_post"] = event
            ask, depth = quote_from_event(event)
            state["last_post_quote"] = {"receive_ms": receive_ms, "ask": ask, "depth": depth, "event_kind": event.get("event_kind")}
            for threshold_state in state["thresholds"].values():
                threshold = threshold_state["threshold"]
                buyable = ask is not None and ask < threshold
                if buyable and threshold_state["first_buyable"] is None:
                    threshold_state["first_buyable"] = {"receive_ms": receive_ms, "ask": ask, "depth": depth}
                if not buyable and threshold_state["first_not_buyable"] is None:
                    threshold_state["first_not_buyable"] = {"receive_ms": receive_ms, "ask": ask, "depth": depth}
    output_rows = []
    for state in states.values():
        row = state["row"]
        baseline_ask, baseline_depth = quote_from_event(state["baseline"] or {})
        threshold_output = {}
        for key, threshold_state in state["thresholds"].items():
            first_buyable = threshold_state["first_buyable"]
            first_not_buyable = threshold_state["first_not_buyable"]
            threshold = threshold_state["threshold"]
            baseline_buyable = baseline_ask is not None and baseline_ask < threshold
            threshold_output[key] = {
                "threshold": threshold,
                "buyable_at_candidate_ready": baseline_buyable,
                "candidate_ready_ask": baseline_ask,
                "candidate_ready_ask_depth": baseline_depth,
                "first_post_buyable_lag_ms": first_buyable["receive_ms"] - row["candidate_ready_ms"] if first_buyable else None,
                "first_post_not_buyable_lag_ms": first_not_buyable["receive_ms"] - row["candidate_ready_ms"] if first_not_buyable else None,
                "right_censored": baseline_buyable and first_not_buyable is None,
            }
        output_rows.append({
            "slug": row["slug"],
            "symbol": row["symbol"],
            "candidate_side": row["candidate_side"],
            "candidate_ready_ms": row["candidate_ready_ms"],
            "candidate_token_id": row["candidate_token_id"],
            "baseline_receive_ms": integer(state["baseline"].get("receive_ms")) if state["baseline"] else None,
            "baseline_age_ms": row["candidate_ready_ms"] - integer(state["baseline"].get("receive_ms")) if state["baseline"] else None,
            "baseline_ask": baseline_ask,
            "baseline_ask_depth": baseline_depth,
            "first_post_receive_lag_ms": integer(state["first_post"].get("receive_ms")) - row["candidate_ready_ms"] if state["first_post"] else None,
            "thresholds": threshold_output,
        })
    return {"status": "ok", "event_rows_scanned": scanned, "candidate_token_event_rows": winner_rows, "rows": output_rows}


def summarize_ask(ask_rows: Sequence[Dict[str, Any]], thresholds: Sequence[float]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    for threshold in thresholds:
        key = str(threshold)
        rows = [row for row in ask_rows if row["thresholds"].get(key)]
        summary[key] = {
            "rows": len(rows),
            "buyable_at_candidate_ready_count": sum(1 for row in rows if row["thresholds"][key]["buyable_at_candidate_ready"]),
            "first_post_buyable_count": sum(1 for row in rows if row["thresholds"][key]["first_post_buyable_lag_ms"] is not None),
            "right_censored_count": sum(1 for row in rows if row["thresholds"][key]["right_censored"]),
            "candidate_ready_ask_values": [row["thresholds"][key]["candidate_ready_ask"] for row in rows if row["thresholds"][key]["candidate_ready_ask"] is not None],
            "candidate_ready_depth_values": [row["thresholds"][key]["candidate_ready_ask_depth"] for row in rows if row["thresholds"][key]["candidate_ready_ask_depth"] is not None],
        }
    return summary


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    metadata_path = args.metadata or default_capture_file(args, "market_metadata.jsonl")
    labels_path = args.labels or default_capture_file(args, "settlement_observations.jsonl")
    clob_path = args.clob_events or default_capture_file(args, "book_events.jsonl")
    rounds, round_inputs = load_rounds(args)
    required = {"source_tape": args.source_tape, "rounds_or_metadata": args.rounds or metadata_path}
    missing_inputs = [name for name, value in required.items() if value is None or not value.exists()]
    input_binding = {name: {"path": str(value) if value else None, "sha256": sha256_file(value)} for name, value in required.items()}
    input_binding["clob_events"] = {"path": str(clob_path) if clob_path else None, "sha256": sha256_file(clob_path)}
    input_binding["metadata"] = {"path": str(metadata_path) if metadata_path else None, "sha256": sha256_file(metadata_path)}
    input_binding["labels"] = {"path": str(labels_path) if labels_path else None, "sha256": sha256_file(labels_path)}
    config = {
        "window_ms": args.window_ms,
        "decision_deadline_ms": args.decision_deadline_ms,
        "compute_latency_ms": args.compute_latency_ms,
        "horizon_ms": args.horizon_ms,
        "min_sources": args.min_sources,
        "min_coverage": args.min_coverage,
        "max_stale_ms": args.max_stale_ms,
        "thresholds": args.thresholds,
        "aggregation": "equal_source_median",
        "event_time_cutoff": "round_end_ms",
        "receive_time_cutoff": "round_end_ms + decision_deadline_ms",
        "missing_action": "skip_round",
    }
    report: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "verdict": "BLOCKED_MISSING_INPUTS" if missing_inputs else "PENDING_REPLAY",
        "method": "outcome_blind_last_30s_event_time_equal_source_median_with_candidate_side_ask_survival",
        "code_sha256": sha256_file(Path(__file__)),
        "config": config,
        "input_binding": input_binding,
        "round_inputs": round_inputs,
        "counts": {"rounds": len(rounds), "candidate_ok": 0, "candidate_missing": 0, "public_label_scored": 0, "public_label_matches": 0},
        "missing_inputs": missing_inputs,
        "causal_contract": {
            "source_event_ts_le_round_end_enforced": True,
            "source_recv_ts_le_decision_deadline_enforced_when_present": True,
            "candidate_ready_is_after_round_end": True,
            "future_source_events_used": 0,
            "public_outcome_used_as_feature": False,
            "public_outcome_used_only_as_optional_label": True,
        },
        "rows": [],
        "ask_survival": None,
        "caveats": [
            "The proxy is not a reproduction of Chainlink's undisclosed sampling/weighting/rounding rules.",
            "A public candidate-side ask is visible liquidity, not queue position, fill, or private ledger truth.",
            "Rounds without receive timestamps are event-time-only diagnostics and cannot prove runtime ready latency.",
            "No order, credential, signature, redemption, funding, service, or Rust hot-path action is performed.",
        ],
    }
    if missing_inputs:
        report["next_action"] = "Provide an immutable external source tape with symbol/source/event_ts_ms/price and preferably recv_ts_ms; do not infer readiness from the RTDS/Gamma capture."
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"output": str(args.output), "verdict": report["verdict"], "missing_inputs": missing_inputs}, indent=2))
        return 2
    tape, tape_stats = load_source_tape(args.source_tape, args.sources)
    report["source_tape_stats"] = tape_stats
    candidate_rows: List[Dict[str, Any]] = []
    for round_row in rounds:
        proxy = source_proxy(round_row, tape, args, round_row["round_end_ms"])
        row = {**round_row, **proxy}
        if proxy.get("status") == "ok":
            candidate_side = proxy["candidate_side"]
            token_id = None
            for outcome, token_id_candidate in zip(round_row.get("outcomes", []), round_row.get("token_ids", [])):
                if normalize_side(outcome) == candidate_side:
                    token_id = str(token_id_candidate)
                    break
            row["candidate_token_id"] = token_id
            row["candidate_status"] = "ok" if token_id else "missing_candidate_token_mapping"
            report["counts"]["candidate_ok"] += 1 if token_id else 0
            report["counts"]["candidate_missing"] += 0 if token_id else 1
            if round_row.get("public_outcome") and candidate_side in {"Up", "Down"}:
                report["counts"]["public_label_scored"] += 1
                report["counts"]["public_label_matches"] += int(candidate_side == round_row["public_outcome"])
        else:
            row["candidate_status"] = proxy.get("reason", "missing")
            report["counts"]["candidate_missing"] += 1
        report["rows"].append(row)
        candidate_rows.append(row)
    if clob_path and clob_path.exists():
        ask = load_clob_survival(clob_path, candidate_rows, args)
        report["ask_survival"] = {key: value for key, value in ask.items() if key != "rows"}
        report["ask_survival"]["rows"] = ask.get("rows", [])
        report["ask_survival"]["threshold_summary"] = summarize_ask(ask.get("rows", []), args.thresholds)
    else:
        report["ask_survival"] = {"status": "missing_clob_events", "rows": []}
    if report["counts"]["candidate_ok"] == 0:
        report["verdict"] = "BLOCKED_NO_CAUSAL_CANDIDATES"
    elif report["ask_survival"].get("status") != "ok":
        report["verdict"] = "BLOCKED_MISSING_CLOB_TAPE"
    else:
        report["verdict"] = "RESEARCH_REPLAY_COMPLETE_NO_ECONOMIC_CLAIM"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "verdict": report["verdict"], "counts": report["counts"], "ask_status": report["ask_survival"].get("status")}, indent=2))
    return 0 if report["verdict"].startswith("RESEARCH_") else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
