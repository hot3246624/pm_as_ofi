#!/usr/bin/env python3
"""Research boundary-time/source lag behind local-agg close tails.

This is an offline attribution tool. It does not change runtime behavior or
gate decisions. Its purpose is to answer whether accepted high-error rows are
caused by the aggregation formula itself, or by not seeing/using the source
tick that most closely matches the Chainlink/RDTS close at the boundary.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")


def clean_line(line: str) -> str:
    return ANSI_RE.sub("", line).strip()


def parse_kv_segment(line: str) -> dict[str, str]:
    if "|" in line:
        line = line.split("|", 1)[1].strip()
    out: dict[str, str] = {}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        out[key] = value
    return out


def fnum(raw: str | None) -> float | None:
    if raw in (None, "", "-", "nan"):
        return None
    try:
        value = float(raw)
    except Exception:
        return None
    return value if math.isfinite(value) else None


def inum(raw: str | None) -> int | None:
    if raw in (None, "", "-", "nan"):
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def bps_diff(pred: float, truth: float) -> float:
    return abs(pred - truth) / max(abs(truth), 1e-12) * 10_000.0


def signed_bps(pred: float, truth: float) -> float:
    return (pred - truth) / max(abs(truth), 1e-12) * 10_000.0


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - rank) + ordered[hi] * (rank - lo)


@dataclass
class GateRow:
    symbol: str
    round_end_ts: int
    gate_status: str
    row_gate_status: str
    close_diff_bps: float
    rtds_open: float
    rtds_close: float
    local_close: float | None
    local_close_ts_ms: int | None
    local_sources: str
    source_subset: str
    rule: str
    min_sources: int | None
    close_abs_delta_ms: int | None
    direction_margin_bps: float | None
    local_close_spread_bps: float | None


@dataclass
class Candidate:
    source: str
    kind: str
    price: float
    ts_ms: int | None
    abs_delta_ms: int | None
    offset_ms: int | None
    error_bps: float
    signed_error_bps: float


@dataclass
class BoundaryContext:
    candidates: list[Candidate] = field(default_factory=list)


def parse_price_at(raw: str | None) -> tuple[float | None, int | None]:
    if not raw or raw == "-":
        return None, None
    if "@" not in raw:
        return fnum(raw), None
    price_raw, ts_raw = raw.rsplit("@", 1)
    return fnum(price_raw), inum(ts_raw)


def parse_gate_rows(log_files: Iterable[Path]) -> list[GateRow]:
    rows: dict[tuple[str, int], GateRow] = {}
    for path in log_files:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = clean_line(raw)
                if "local_price_agg_uncertainty_gate_shadow |" not in line:
                    continue
                kv = parse_kv_segment(line)
                if kv.get("phase") != "final":
                    continue
                symbol = kv.get("symbol", "").lower()
                round_end_ts = inum(kv.get("round_end_ts"))
                close_diff = fnum(kv.get("close_diff_bps"))
                rtds_open = fnum(kv.get("rtds_open"))
                rtds_close = fnum(kv.get("rtds_close"))
                if not symbol or round_end_ts is None or close_diff is None or rtds_open is None or rtds_close is None:
                    continue
                local_close, local_close_ts_ms = parse_price_at(kv.get("local_close"))
                rows[(symbol, round_end_ts)] = GateRow(
                    symbol=symbol,
                    round_end_ts=round_end_ts,
                    gate_status=kv.get("gate_status", ""),
                    row_gate_status=kv.get("row_gate_status", ""),
                    close_diff_bps=close_diff,
                    rtds_open=rtds_open,
                    rtds_close=rtds_close,
                    local_close=local_close,
                    local_close_ts_ms=local_close_ts_ms,
                    local_sources=kv.get("local_sources", ""),
                    source_subset=kv.get("source_subset", ""),
                    rule=kv.get("rule", ""),
                    min_sources=inum(kv.get("min_sources")),
                    close_abs_delta_ms=inum(kv.get("close_abs_delta_ms")),
                    direction_margin_bps=fnum(kv.get("direction_margin_bps")),
                    local_close_spread_bps=fnum(kv.get("local_close_spread_bps")),
                )
    return sorted(rows.values(), key=lambda row: (row.round_end_ts, row.symbol))


def parse_boundary_value(raw: str) -> tuple[int | None, float | None, int | None]:
    # Example: 27938ms/40.6@1778595572062
    if raw == "-":
        return None, None, None
    m = re.match(r"(?P<delta>-?\d+)ms/(?P<price>[-+0-9.eE]+)@(?P<ts>\d+)$", raw)
    if not m:
        return None, None, None
    return int(m.group("delta")), float(m.group("price")), int(m.group("ts"))


def parse_source_boundary(line: str, truth_by_key: dict[tuple[str, int], float]) -> tuple[tuple[str, int], list[Candidate]] | None:
    kv = parse_kv_segment(line.split(" source_boundary=", 1)[0])
    symbol = kv.get("symbol", "").lower()
    round_end_ts = inum(kv.get("round_end_ts"))
    if not symbol or round_end_ts is None:
        return None
    truth = truth_by_key.get((symbol, round_end_ts))
    if truth is None:
        return None
    boundary_raw = line.split(" source_boundary=", 1)[1]
    candidates: list[Candidate] = []
    # Segments are separated by " | source:" in the diagnostic tail.
    for segment in re.split(r"\s+\|\s+", boundary_raw):
        if ":" not in segment:
            continue
        source, rest = segment.split(":", 1)
        source = source.strip().lower()
        if source not in SOURCES:
            continue
        fields: dict[str, str] = {}
        for token in rest.split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            fields[key] = value
        for kind in ("close_exact", "first_after_end", "nearest_end"):
            delta, price, ts_ms = parse_boundary_value(fields.get(kind, "-"))
            if price is None:
                continue
            offset_ms = None
            if ts_ms is not None:
                offset_ms = ts_ms - round_end_ts * 1000
            candidates.append(
                Candidate(
                    source=source,
                    kind=f"diagnostic_{kind}",
                    price=price,
                    ts_ms=ts_ms,
                    abs_delta_ms=abs(delta) if delta is not None else None,
                    offset_ms=offset_ms,
                    error_bps=bps_diff(price, truth),
                    signed_error_bps=signed_bps(price, truth),
                )
            )
    return (symbol, round_end_ts), candidates


def add_tape_candidates(run_dir: Path, contexts: dict[tuple[str, int], BoundaryContext], truth_by_key: dict[tuple[str, int], float]) -> None:
    path = run_dir / "local_price_agg_boundary_tape.jsonl"
    if not path.exists():
        return
    seen: set[tuple[str, int, str, str, int, float]] = set()
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end_ts = obj.get("round_end_ts")
            if not isinstance(round_end_ts, int):
                continue
            truth = truth_by_key.get((symbol, round_end_ts))
            if truth is None:
                continue
            ctx = contexts.setdefault((symbol, round_end_ts), BoundaryContext())
            mode = str(obj.get("mode", ""))
            for source_tape in obj.get("source_tapes") or []:
                source = str(source_tape.get("source", "")).lower()
                if source not in SOURCES:
                    continue
                for tick in source_tape.get("close_window_ticks") or []:
                    ts_ms = tick.get("ts_ms")
                    offset_ms = tick.get("offset_ms")
                    price = tick.get("price")
                    if not isinstance(ts_ms, int) or not isinstance(offset_ms, int) or not isinstance(price, (int, float)):
                        continue
                    key = (symbol, round_end_ts, source, mode, ts_ms, float(price))
                    if key in seen:
                        continue
                    seen.add(key)
                    ctx.candidates.append(
                        Candidate(
                            source=source,
                            kind=f"tape_{mode}_close",
                            price=float(price),
                            ts_ms=ts_ms,
                            abs_delta_ms=abs(offset_ms),
                            offset_ms=offset_ms,
                            error_bps=bps_diff(float(price), truth),
                            signed_error_bps=signed_bps(float(price), truth),
                        )
                    )


def add_unresolved_candidates(log_files: Iterable[Path], contexts: dict[tuple[str, int], BoundaryContext], truth_by_key: dict[tuple[str, int], float]) -> None:
    for path in log_files:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = clean_line(raw)
                if "local_price_agg_unresolved |" not in line or " source_boundary=" not in line:
                    continue
                parsed = parse_source_boundary(line, truth_by_key)
                if parsed is None:
                    continue
                key, candidates = parsed
                contexts.setdefault(key, BoundaryContext()).candidates.extend(candidates)


def unique_candidates(candidates: list[Candidate]) -> list[Candidate]:
    seen: set[tuple[str, str, int | None, float]] = set()
    out: list[Candidate] = []
    for candidate in candidates:
        key = (candidate.source, candidate.kind, candidate.ts_ms, round(candidate.price, 12))
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def score_rows(rows: list[GateRow]) -> dict[str, float | int | None]:
    errors = [row.close_diff_bps for row in rows]
    return {
        "n": len(rows),
        "mean_bps": statistics.mean(errors) if errors else None,
        "p95_bps": percentile(errors, 0.95),
        "max_bps": max(errors) if errors else None,
    }


def run_files(run_dir: Path) -> list[Path]:
    files = sorted(run_dir.glob("local_agg_lab_*.log"))
    if files:
        return files
    return sorted(run_dir.glob("polymarket.log*"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Attribute local-agg boundary lag/source tails without changing runtime behavior.")
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--symbol", default="")
    ap.add_argument("--gate-status", default="", help="Optional final gate_status filter, e.g. accepted or gated.")
    ap.add_argument("--row-gate-status", default="", help="Optional row_gate_status filter, e.g. accepted or gated.")
    ap.add_argument("--tail-n", type=int, default=12)
    ap.add_argument("--min-error-bps", type=float, default=0.0)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    log_files = run_files(run_dir)
    rows = parse_gate_rows(log_files)
    if args.symbol:
        rows = [row for row in rows if row.symbol == args.symbol.lower()]
    if args.gate_status:
        rows = [row for row in rows if row.gate_status == args.gate_status]
    if args.row_gate_status:
        rows = [row for row in rows if row.row_gate_status == args.row_gate_status]
    if args.min_error_bps > 0:
        rows = [row for row in rows if row.close_diff_bps >= args.min_error_bps]

    truth_by_key = {(row.symbol, row.round_end_ts): row.rtds_close for row in rows}
    contexts: dict[tuple[str, int], BoundaryContext] = {}
    add_tape_candidates(run_dir, contexts, truth_by_key)
    add_unresolved_candidates(log_files, contexts, truth_by_key)

    by_symbol: dict[str, list[GateRow]] = {}
    enriched = []
    for row in rows:
        by_symbol.setdefault(row.symbol, []).append(row)
        ctx = contexts.get((row.symbol, row.round_end_ts), BoundaryContext())
        candidates = sorted(unique_candidates(ctx.candidates), key=lambda item: item.error_bps)
        best = candidates[0] if candidates else None
        selected_sources = set(filter(None, row.local_sources.split(";")))
        best_selected = next((item for item in candidates if item.source in selected_sources), None)
        enriched.append(
            {
                "symbol": row.symbol,
                "round_end_ts": row.round_end_ts,
                "gate_status": row.gate_status,
                "row_gate_status": row.row_gate_status,
                "close_diff_bps": row.close_diff_bps,
                "rtds_open": row.rtds_open,
                "rtds_close": row.rtds_close,
                "local_close": row.local_close,
                "local_close_ts_ms": row.local_close_ts_ms,
                "local_sources": row.local_sources,
                "source_subset": row.source_subset,
                "rule": row.rule,
                "close_abs_delta_ms": row.close_abs_delta_ms,
                "direction_margin_bps": row.direction_margin_bps,
                "local_close_spread_bps": row.local_close_spread_bps,
                "best_candidate": best.__dict__ if best else None,
                "best_selected_source_candidate": best_selected.__dict__ if best_selected else None,
                "best_improvement_bps": (row.close_diff_bps - best.error_bps) if best else None,
                "candidate_count": len(candidates),
                "top_candidates": [item.__dict__ for item in candidates[:5]],
            }
        )

    symbol_summary = {}
    for symbol, symbol_rows in sorted(by_symbol.items()):
        symbol_enriched = [row for row in enriched if row["symbol"] == symbol]
        improvements = [
            float(row["best_improvement_bps"])
            for row in symbol_enriched
            if row.get("best_improvement_bps") is not None
        ]
        best_errors = [
            float(row["best_candidate"]["error_bps"])
            for row in symbol_enriched
            if row.get("best_candidate")
        ]
        summary = score_rows(symbol_rows)
        summary.update(
            {
                "candidate_rows": len(best_errors),
                "best_candidate_p95_bps": percentile(best_errors, 0.95),
                "best_candidate_max_bps": max(best_errors) if best_errors else None,
                "median_improvement_bps": statistics.median(improvements) if improvements else None,
                "p95_improvement_bps": percentile(improvements, 0.95),
                "rows_improved_gt_1bps": sum(1 for value in improvements if value > 1.0),
            }
        )
        symbol_summary[symbol] = summary

    tail_rows = sorted(enriched, key=lambda row: float(row["close_diff_bps"]), reverse=True)[: args.tail_n]
    output = {
        "config": {
            "run_dir": str(run_dir),
            "symbol": args.symbol or None,
            "gate_status": args.gate_status or None,
            "row_gate_status": args.row_gate_status or None,
            "min_error_bps": args.min_error_bps,
        },
        "summary": score_rows(rows),
        "by_symbol": symbol_summary,
        "tails": tail_rows,
    }

    text = json.dumps(output, indent=2, sort_keys=True)
    print(text)
    if args.out_json:
        Path(args.out_json).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
