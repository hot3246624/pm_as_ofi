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
class CompareRow:
    instance_id: str
    log_file: str
    line_ts: str
    slug: str
    symbol: str
    round_end_ts: int
    status: str
    filter_reason: str
    rtds_open: Optional[float]
    rtds_close: Optional[float]
    rtds_side: Optional[str]
    local_close: Optional[float]
    local_side: Optional[str]
    side_match: Optional[bool]
    local_sources: Optional[int]
    local_close_spread_bps: Optional[float]
    local_close_exact_sources: Optional[int]
    local_to_rtds_detect_gap_ms: Optional[int]
    safe_preclose_relief_applied: Optional[bool]
    safe_single_source_relief_applied: Optional[bool]
    safe_low_conf_relief_applied: Optional[bool]


@dataclass
class SourcePoint:
    source: str
    open_price: Optional[float]
    open_ts_ms: Optional[int]
    open_exact: Optional[bool]
    close_price: Optional[float]
    raw_close_price: Optional[float]
    close_ts_ms: Optional[int]
    close_exact: Optional[bool]
    close_abs_delta_ms: Optional[int]
    close_pick_kind: Optional[str]


def instance_sort_key(instance_id: str) -> Tuple[int, str]:
    m = re.search(r"lab(\d+)$", instance_id)
    if m:
        return (int(m.group(1)), instance_id)
    return (0, instance_id)


def status_rank(status: str) -> int:
    return {"accepted": 2, "filtered": 1, "unresolved": 0}.get(status, -1)


def discover_instance_logs(logs_root: Path, instance_glob: str) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []

    def append_dir_logs(base_instance_id: str, log_dir: Path) -> None:
        for log_path in sorted(log_dir.glob("local_agg_lab_*.log")):
            out.append((base_instance_id, log_path))
        for log_path in sorted(log_dir.glob("polymarket.*.log")):
            out.append((f"{base_instance_id}/{log_path.name}", log_path))
        for log_path in sorted(log_dir.glob("polymarket.*.log.*")):
            out.append((f"{base_instance_id}/{log_path.name}", log_path))

    for inst_dir in sorted(logs_root.glob(instance_glob)):
        if not inst_dir.is_dir():
            continue
        append_dir_logs(inst_dir.name, inst_dir)
        runs_dir = inst_dir / "runs"
        if runs_dir.is_dir():
            for run_dir in sorted(runs_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                append_dir_logs(f"{inst_dir.name}/{run_dir.name}", run_dir)
    return out


def parse_compare_rows(instance_id: str, log_path: Path) -> List[CompareRow]:
    out: List[CompareRow] = []
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            status = ""
            if "local_price_agg_vs_rtds |" in line:
                status = "accepted"
            elif "local_price_agg_vs_rtds_filtered |" in line:
                status = "filtered"
            elif "local_price_agg_vs_rtds_unresolved |" in line:
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
            rtds_open = to_float(kv.get("rtds_open"))
            rtds_close = to_float(kv.get("rtds_close"))
            local_side = kv.get("local_side_vs_rtds_open")
            if local_side in ("Some(Yes)", "Yes"):
                local_side = "Yes"
            elif local_side in ("Some(No)", "No"):
                local_side = "No"
            else:
                local_side = None
            side_match_raw = kv.get("side_match_vs_rtds_open")
            side_match = None
            if side_match_raw == "true":
                side_match = True
            elif side_match_raw == "false":
                side_match = False
            out.append(
                CompareRow(
                    instance_id=instance_id,
                    log_file=log_path.name,
                    line_ts=parse_line_ts(line),
                    slug=slug,
                    symbol=symbol,
                    round_end_ts=round_end_ts,
                    status=status,
                    filter_reason=kv.get("reason", ""),
                    rtds_open=rtds_open,
                    rtds_close=rtds_close,
                    rtds_side=kv.get("rtds_side"),
                    local_close=to_float(kv.get("local_close")),
                    local_side=local_side,
                    side_match=side_match,
                    local_sources=to_int(kv.get("local_sources")),
                    local_close_spread_bps=to_float(kv.get("local_close_spread_bps")),
                    local_close_exact_sources=to_int(kv.get("local_close_exact_sources")),
                    local_to_rtds_detect_gap_ms=to_int(kv.get("local_to_rtds_detect_gap_ms")),
                    safe_preclose_relief_applied=(kv.get("safe_preclose_relief_applied") == "true") if "safe_preclose_relief_applied" in kv else None,
                    safe_single_source_relief_applied=(kv.get("safe_single_source_relief_applied") == "true") if "safe_single_source_relief_applied" in kv else None,
                    safe_low_conf_relief_applied=(kv.get("safe_low_conf_relief_applied") == "true") if "safe_low_conf_relief_applied" in kv else None,
                )
            )
    return out


def parse_source_probe(path: Path, mode: str) -> Dict[Tuple[str, int], Dict[str, SourcePoint]]:
    out: Dict[Tuple[str, int], Tuple[int, Dict[str, SourcePoint]]] = {}
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("mode") != mode or obj.get("status") != "ready":
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end_ts = obj.get("round_end_ts")
            unix_ms = obj.get("unix_ms")
            if not symbol or not isinstance(round_end_ts, int) or not isinstance(unix_ms, int):
                continue
            source_points: Dict[str, SourcePoint] = {}
            for sp in obj.get("source_points") or []:
                src = str(sp.get("source", "")).lower()
                if src not in SOURCES:
                    continue
                source_points[src] = SourcePoint(
                    source=src,
                    open_price=to_float(sp.get("open_price")),
                    open_ts_ms=to_int(sp.get("open_ts_ms")),
                    open_exact=bool(sp["open_exact"]) if "open_exact" in sp and sp.get("open_exact") is not None else None,
                    close_price=to_float(sp.get("close_price")),
                    raw_close_price=to_float(sp.get("raw_close_price")),
                    close_ts_ms=to_int(sp.get("close_ts_ms")),
                    close_exact=bool(sp["close_exact"]) if "close_exact" in sp and sp.get("close_exact") is not None else None,
                    close_abs_delta_ms=to_int(sp.get("close_abs_delta_ms")),
                    close_pick_kind=sp.get("close_pick_kind"),
                )
            key = (symbol, round_end_ts)
            prev = out.get(key)
            if prev is None or unix_ms >= prev[0]:
                out[key] = (unix_ms, source_points)
    return {k: v for k, (_, v) in out.items()}


def point_quality(points: Dict[str, SourcePoint]) -> Tuple[int, int, int, int, float]:
    source_count = len(points)
    open_count = sum(1 for p in points.values() if p.open_price is not None and p.open_ts_ms is not None)
    close_exact_count = sum(1 for p in points.values() if p.close_exact is True)
    open_exact_count = sum(1 for p in points.values() if p.open_exact is True)
    deltas = [p.close_abs_delta_ms for p in points.values() if p.close_abs_delta_ms is not None]
    avg_delta = sum(deltas) / len(deltas) if deltas else 10**12
    return (source_count, open_count, close_exact_count, open_exact_count, avg_delta)


def best_row_per_instance(rows: Iterable[CompareRow]) -> Dict[Tuple[str, str, int], CompareRow]:
    best: Dict[Tuple[str, str, int], CompareRow] = {}
    for row in rows:
        key = (row.instance_id, row.symbol, row.round_end_ts)
        prev = best.get(key)
        if prev is None:
            best[key] = row
            continue
        score = (status_rank(row.status), row.local_sources or -1, row.local_close is not None, row.line_ts)
        prev_score = (status_rank(prev.status), prev.local_sources or -1, prev.local_close is not None, prev.line_ts)
        if score > prev_score:
            best[key] = row
    return best


def choose_canonical_rows(rows: Iterable[CompareRow], source_maps: Dict[str, Dict[Tuple[str, int], Dict[str, SourcePoint]]]) -> List[CompareRow]:
    per_instance = best_row_per_instance(rows)
    by_key: Dict[Tuple[str, int], List[CompareRow]] = {}
    for row in per_instance.values():
        by_key.setdefault((row.symbol, row.round_end_ts), []).append(row)

    canonical: List[CompareRow] = []
    for (_symbol, _round_end_ts), candidates in sorted(by_key.items(), key=lambda kv: kv[0]):
        best_row: Optional[CompareRow] = None
        best_score = None
        for row in candidates:
            points = source_maps.get(row.instance_id, {}).get((row.symbol, row.round_end_ts), {})
            q = point_quality(points)
            score = (
                q[0],  # more sources
                q[1],  # more open points
                q[2],  # more close exact
                q[3],  # more open exact
                -(q[4]),  # smaller avg delta
                status_rank(row.status),  # accepted/filtered/unresolved only as tiebreak
                instance_sort_key(row.instance_id),  # later lab wins tiebreak
            )
            if best_score is None or score > best_score:
                best_score = score
                best_row = row
        if best_row is not None:
            canonical.append(best_row)
    return canonical


def write_round_wide(rows: Iterable[CompareRow], source_maps: Dict[str, Dict[Tuple[str, int], Dict[str, SourcePoint]]], out_path: Path) -> None:
    base_fields = [
        "instance_id",
        "log_file",
        "line_ts",
        "slug",
        "symbol",
        "round_end_ts",
        "status",
        "filter_reason",
        "rtds_open",
        "rtds_close",
        "rtds_side",
        "local_close",
        "local_side",
        "side_match",
        "local_sources",
        "local_close_spread_bps",
        "local_close_exact_sources",
        "local_to_rtds_detect_gap_ms",
        "safe_preclose_relief_applied",
        "safe_single_source_relief_applied",
        "safe_low_conf_relief_applied",
    ]
    src_fields: List[str] = []
    for src in SOURCES:
        src_fields.extend(
            [
                f"{src}_open_price",
                f"{src}_open_ts_ms",
                f"{src}_open_exact",
                f"{src}_close_price",
                f"{src}_raw_close_price",
                f"{src}_close_ts_ms",
                f"{src}_close_exact",
                f"{src}_close_abs_delta_ms",
                f"{src}_close_pick_kind",
            ]
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=base_fields + src_fields)
        writer.writeheader()
        for row in rows:
            rec = {k: getattr(row, k) for k in base_fields}
            points = source_maps.get(row.instance_id, {}).get((row.symbol, row.round_end_ts), {})
            for src in SOURCES:
                point = points.get(src)
                rec[f"{src}_open_price"] = point.open_price if point else ""
                rec[f"{src}_open_ts_ms"] = point.open_ts_ms if point else ""
                rec[f"{src}_open_exact"] = point.open_exact if point else ""
                rec[f"{src}_close_price"] = point.close_price if point else ""
                rec[f"{src}_raw_close_price"] = point.raw_close_price if point else ""
                rec[f"{src}_close_ts_ms"] = point.close_ts_ms if point else ""
                rec[f"{src}_close_exact"] = point.close_exact if point else ""
                rec[f"{src}_close_abs_delta_ms"] = point.close_abs_delta_ms if point else ""
                rec[f"{src}_close_pick_kind"] = point.close_pick_kind if point else ""
            writer.writerow(rec)


def write_source_long(rows: Iterable[CompareRow], source_maps: Dict[str, Dict[Tuple[str, int], Dict[str, SourcePoint]]], out_path: Path) -> None:
    fields = [
        "instance_id",
        "log_file",
        "line_ts",
        "slug",
        "symbol",
        "round_end_ts",
        "status",
        "filter_reason",
        "rtds_open",
        "rtds_close",
        "rtds_side",
        "source",
        "open_price",
        "open_ts_ms",
        "open_exact",
        "close_price",
        "raw_close_price",
        "close_ts_ms",
        "close_exact",
        "close_abs_delta_ms",
        "close_pick_kind",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            points = source_maps.get(row.instance_id, {}).get((row.symbol, row.round_end_ts), {})
            for src, point in points.items():
                writer.writerow(
                    {
                        "instance_id": row.instance_id,
                        "log_file": row.log_file,
                        "line_ts": row.line_ts,
                        "slug": row.slug,
                        "symbol": row.symbol,
                        "round_end_ts": row.round_end_ts,
                        "status": row.status,
                        "filter_reason": row.filter_reason,
                        "rtds_open": row.rtds_open,
                        "rtds_close": row.rtds_close,
                        "rtds_side": row.rtds_side,
                        "source": src,
                        "open_price": point.open_price,
                        "open_ts_ms": point.open_ts_ms,
                        "open_exact": point.open_exact,
                        "close_price": point.close_price,
                        "raw_close_price": point.raw_close_price,
                        "close_ts_ms": point.close_ts_ms,
                        "close_exact": point.close_exact,
                        "close_abs_delta_ms": point.close_abs_delta_ms,
                        "close_pick_kind": point.close_pick_kind,
                    }
                )


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a unified local-agg training dataset from lab logs + source probes.")
    ap.add_argument("--logs-root", default="/Users/hot/web3Scientist/pm_as_ofi/logs")
    ap.add_argument("--instance-glob", default="local-agg-closeonly-gated-lab*")
    ap.add_argument("--mode", default="close_only", choices=["close_only", "full"])
    ap.add_argument("--out-round-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_rounds.csv")
    ap.add_argument("--out-source-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_sources.csv")
    ap.add_argument("--out-canonical-round-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_canonical_rounds.csv")
    ap.add_argument("--out-canonical-source-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_canonical_sources.csv")
    args = ap.parse_args()

    logs_root = Path(args.logs_root)
    discovered = discover_instance_logs(logs_root, args.instance_glob)
    if not discovered:
        print("No lab logs found.")
        return 1

    source_maps: Dict[str, Dict[Tuple[str, int], Dict[str, SourcePoint]]] = {}
    rows: List[CompareRow] = []
    for instance_id, log_path in discovered:
        rows.extend(parse_compare_rows(instance_id, log_path))
        src_path = log_path.parent / "local_price_agg_sources.jsonl"
        if instance_id not in source_maps:
            source_maps[instance_id] = parse_source_probe(src_path, args.mode)

    rows.sort(key=lambda r: (r.instance_id, r.round_end_ts, r.symbol, r.status, r.line_ts))
    write_round_wide(rows, source_maps, Path(args.out_round_csv))
    write_source_long(rows, source_maps, Path(args.out_source_csv))

    canonical_rows = choose_canonical_rows(rows, source_maps)
    write_round_wide(canonical_rows, source_maps, Path(args.out_canonical_round_csv))
    write_source_long(canonical_rows, source_maps, Path(args.out_canonical_source_csv))

    statuses: Dict[str, int] = {}
    for row in rows:
        statuses[row.status] = statuses.get(row.status, 0) + 1
    print(f"instances={len({r.instance_id for r in rows})} rows={len(rows)} statuses={statuses}")
    print(f"round_csv={args.out_round_csv}")
    print(f"source_csv={args.out_source_csv}")
    print(f"canonical_rows={len(canonical_rows)}")
    print(f"canonical_round_csv={args.out_canonical_round_csv}")
    print(f"canonical_source_csv={args.out_canonical_source_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
