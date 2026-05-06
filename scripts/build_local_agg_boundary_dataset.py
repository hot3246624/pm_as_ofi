#!/usr/bin/env python3
import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")


def parse_slug_anchor_ts(slug: str) -> Optional[int]:
    try:
        return int(slug.rsplit("-", 1)[1])
    except Exception:
        return None


def detect_interval_secs_from_slug(slug: str) -> Optional[int]:
    m = re.search(r"-updown-(\d+)([mh])(?:-|$)", slug)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    return n * 60 if unit == "m" else n * 3600 if unit == "h" else None


def parse_slug_round_end(slug: str) -> Optional[int]:
    anchor = parse_slug_anchor_ts(slug)
    if anchor is None:
        return None
    interval = detect_interval_secs_from_slug(slug)
    return anchor + interval if interval is not None else anchor


def parse_line_ts(line: str) -> str:
    parts = line.split(" ", 1)
    return parts[0] if parts else ""


def parse_key_values(payload_line: str) -> Dict[str, str]:
    if "|" not in payload_line:
        return {}
    payload = payload_line.split("|", 1)[1].strip()
    out: Dict[str, str] = {}
    for token in payload.split():
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def to_float(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    try:
        v = float(raw)
    except Exception:
        return None
    return v if math.isfinite(v) else None


def to_int(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


@dataclass
class TruthRow:
    instance_id: str
    log_file: str
    line_ts: str
    slug: str
    symbol: str
    round_end_ts: int
    status: str
    rtds_open: Optional[float]
    rtds_close: Optional[float]
    rtds_side: Optional[str]
    local_open: Optional[float]
    local_close: Optional[float]
    side_match: Optional[bool]
    local_started_ms: Optional[int]
    local_ready_ms: Optional[int]
    local_deadline_ms: Optional[int]


def parse_truth_rows(instance_id: str, log_path: Path, mode: str) -> List[TruthRow]:
    accepted_tag = "local_price_agg_full_shadow_vs_rtds |" if mode == "full" else "local_price_agg_vs_rtds |"
    filtered_tag = None if mode == "full" else "local_price_agg_vs_rtds_filtered |"
    unresolved_tag = "local_price_agg_full_shadow_unresolved |" if mode == "full" else "local_price_agg_vs_rtds_unresolved |"
    out: List[TruthRow] = []
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            status = ""
            if accepted_tag in line:
                status = "accepted"
            elif filtered_tag and filtered_tag in line:
                status = "filtered"
            elif unresolved_tag in line:
                status = "unresolved"
            else:
                continue
            kv = parse_key_values(line)
            slug = kv.get("slug", "")
            symbol = kv.get("symbol", "").lower()
            if not slug or not symbol:
                continue
            round_end_ts = parse_slug_round_end(slug)
            if round_end_ts is None:
                continue
            side_match_raw = kv.get("side_match_vs_rtds_open")
            side_match = None
            if side_match_raw == "true":
                side_match = True
            elif side_match_raw == "false":
                side_match = False
            out.append(
                TruthRow(
                    instance_id=instance_id,
                    log_file=log_path.name,
                    line_ts=parse_line_ts(line),
                    slug=slug,
                    symbol=symbol,
                    round_end_ts=round_end_ts,
                    status=status,
                    rtds_open=to_float(kv.get("rtds_open")),
                    rtds_close=to_float(kv.get("rtds_close")),
                    rtds_side=kv.get("rtds_side"),
                    local_open=to_float(kv.get("local_open")),
                    local_close=to_float(kv.get("local_close")),
                    side_match=side_match,
                    local_started_ms=to_int(kv.get("local_started_ms")),
                    local_ready_ms=to_int(kv.get("local_ready_ms")),
                    local_deadline_ms=to_int(kv.get("local_deadline_ms")),
                )
            )
    return out


def parse_router_v1_timing(log_path: Path) -> Dict[Tuple[str, int], dict]:
    tags = (
        "local_price_agg_boundary_shadow_vs_rtds |",
        "local_price_agg_boundary_shadow_filtered |",
        "local_price_agg_boundary_shadow_unresolved |",
    )
    out: Dict[Tuple[str, int], dict] = {}
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not any(tag in line for tag in tags) or "policy=boundary_symbol_router_v1" not in line:
                continue
            kv = parse_key_values(line)
            slug = kv.get("slug", "")
            symbol = kv.get("symbol", "").lower()
            if not slug or not symbol:
                continue
            round_end_ts = parse_slug_round_end(slug)
            if round_end_ts is None:
                continue
            ready_ms = to_int(kv.get("local_ready_ms"))
            current = out.get((symbol, round_end_ts))
            # Prefer the earliest router-v1 decision timestamp: it is the data cut
            # actually available to the runtime decision, unlike full-shadow rows
            # that may wait until a later unresolved deadline.
            if current is None or (
                ready_ms is not None
                and (
                    current.get("local_ready_ms") is None
                    or ready_ms < current["local_ready_ms"]
                )
            ):
                out[(symbol, round_end_ts)] = {
                    "local_started_ms": to_int(kv.get("local_started_ms")),
                    "local_ready_ms": ready_ms,
                    "local_deadline_ms": to_int(kv.get("local_deadline_ms")),
                    "local_timing_source": "boundary_symbol_router_v1",
                }
    return out


def discover_instance_logs(logs_root: Path, instance_glob: str) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    for inst_dir in sorted(logs_root.glob(instance_glob)):
        if not inst_dir.is_dir():
            continue
        for log_path in sorted(inst_dir.glob("local_agg_lab_*.log")):
            out.append((inst_dir.name, log_path))
        runs_dir = inst_dir / "runs"
        if runs_dir.is_dir():
            for run_dir in sorted(runs_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                for log_path in sorted(run_dir.glob("local_agg_lab_*.log")):
                    out.append((f"{inst_dir.name}/{run_dir.name}", log_path))
    return out


def parse_boundary_probe(path: Path, mode: str) -> Dict[Tuple[str, int], Tuple[int, dict]]:
    out: Dict[Tuple[str, int], Tuple[int, dict]] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("mode") != mode:
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end_ts = obj.get("round_end_ts")
            unix_ms = obj.get("unix_ms")
            if not symbol or not isinstance(round_end_ts, int) or not isinstance(unix_ms, int):
                continue
            key = (symbol, round_end_ts)
            prev = out.get(key)
            if prev is None or unix_ms >= prev[0]:
                out[key] = (unix_ms, obj)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build long-form boundary tape dataset for local price aggregator rule search.")
    ap.add_argument("--logs-root", default="/Users/hot/web3Scientist/pm_as_ofi/logs")
    ap.add_argument("--instance-glob", default="local-agg*lab*")
    ap.add_argument("--mode", default="close_only", choices=["full", "close_only"])
    ap.add_argument("--out-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_boundary_dataset.csv")
    ap.add_argument(
        "--cap-local-ready-lag-ms",
        type=int,
        default=0,
        help=(
            "Offline simulation only: cap local_ready_ms/local_deadline_ms at "
            "round_end + this many ms so downstream ready-aware evaluation tests "
            "a hard production decision window, e.g. 300."
        ),
    )
    args = ap.parse_args()

    logs_root = Path(args.logs_root)
    discovered = discover_instance_logs(logs_root, args.instance_glob)
    rows = []
    for instance_id, log_path in discovered:
        if not log_path.exists():
            # Old runs can be compacted while the long all-runs replay is starting.
            # Skip stale discovery entries instead of aborting the whole monitor.
            continue
        truth_rows = parse_truth_rows(instance_id, log_path, args.mode)
        if not truth_rows:
            continue
        boundary_map = parse_boundary_probe(log_path.parent / "local_price_agg_boundary_tape.jsonl", args.mode)
        router_timing = parse_router_v1_timing(log_path)
        for row in truth_rows:
            tape = boundary_map.get((row.symbol, row.round_end_ts))
            if tape is None:
                continue
            _, obj = tape
            timing = router_timing.get((row.symbol, row.round_end_ts), {})
            local_started_ms = timing.get("local_started_ms", row.local_started_ms)
            local_ready_ms = timing.get("local_ready_ms", row.local_ready_ms)
            local_deadline_ms = timing.get("local_deadline_ms", row.local_deadline_ms)
            local_timing_source = timing.get("local_timing_source", "truth_row")
            if args.cap_local_ready_lag_ms > 0:
                cap_ready_ms = row.round_end_ts * 1000 + args.cap_local_ready_lag_ms
                if local_ready_ms is None or local_ready_ms > cap_ready_ms:
                    local_ready_ms = cap_ready_ms
                    local_timing_source = f"{local_timing_source}+cap_{args.cap_local_ready_lag_ms}ms"
                local_deadline_ms = cap_ready_ms
            for source_tape in obj.get("source_tapes") or []:
                source = str(source_tape.get("source", "")).lower()
                if source not in SOURCES:
                    continue
                for phase, ticks in (
                    ("open", source_tape.get("open_window_ticks") or []),
                    ("close", source_tape.get("close_window_ticks") or []),
                ):
                    for tick in ticks:
                        ts_ms = tick.get("ts_ms")
                        offset_ms = tick.get("offset_ms")
                        price = tick.get("price")
                        if not isinstance(ts_ms, int) or not isinstance(offset_ms, int):
                            continue
                        price_f = to_float(str(price)) if not isinstance(price, (int, float)) else float(price)
                        if price_f is None:
                            continue
                        rows.append(
                            {
                                "instance_id": row.instance_id,
                                "log_file": row.log_file,
                                "line_ts": row.line_ts,
                                "mode": args.mode,
                                "slug": row.slug,
                                "symbol": row.symbol,
                                "round_end_ts": row.round_end_ts,
                                "status": row.status,
                                "rtds_open": row.rtds_open,
                                "rtds_close": row.rtds_close,
                                "rtds_side": row.rtds_side,
                                "local_open": row.local_open,
                                "local_close": row.local_close,
                                "side_match": row.side_match,
                                "local_started_ms": local_started_ms,
                                "local_ready_ms": local_ready_ms,
                                "local_deadline_ms": local_deadline_ms,
                                "local_timing_source": local_timing_source,
                                "source": source,
                                "phase": phase,
                                "ts_ms": ts_ms,
                                "offset_ms": offset_ms,
                                "price": price_f,
                                "boundary_window_ms": obj.get("boundary_window_ms"),
                            }
                        )

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "instance_id",
                "log_file",
                "line_ts",
                "mode",
                "slug",
                "symbol",
                "round_end_ts",
                "status",
                "rtds_open",
                "rtds_close",
                "rtds_side",
                "local_open",
                "local_close",
                "side_match",
                "local_started_ms",
                "local_ready_ms",
                "local_deadline_ms",
                "local_timing_source",
                "source",
                "phase",
                "ts_ms",
                "offset_ms",
                "price",
                "boundary_window_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"rows={len(rows)} out_csv={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
