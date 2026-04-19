#!/usr/bin/env python3
"""
Export post-close winner-hint rounds to CSV with precision-safe prices.

Why this exists:
- `post_close_emit_winner_hint` previously logged ref/observed as 4 decimals.
- Runtime decision uses raw f64 comparison (`close >= open_ref`), so analysis
  must not rely on display-rounded numbers.

This exporter pairs:
1) `chainlink_result_ready` (precision source, open/close 6+ decimals)
2) `post_close_emit_winner_hint` (includes frontend snapshot fields)

Output includes:
- open_ref / final_price (precision-preserving)
- delta_close_minus_open
- winner_by_delta (recomputed from delta)
- winner_match_logged
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def parse_kv_pairs(line: str) -> Dict[str, str]:
    # key=value where value has no spaces in our log schema.
    return dict(re.findall(r"([a-zA-Z0-9_]+)=([^\s]+)", line))


def parse_opt_float(raw: str) -> Optional[float]:
    if raw == "None":
        return None
    if raw.startswith("Some(") and raw.endswith(")"):
        inner = raw[5:-1]
        if inner == "None" or inner == "":
            return None
        try:
            return float(inner)
        except ValueError:
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_opt_int(raw: str) -> Optional[int]:
    if raw == "None":
        return None
    if raw.startswith("Some(") and raw.endswith(")"):
        inner = raw[5:-1]
        if inner == "None" or inner == "":
            return None
        try:
            return int(inner)
        except ValueError:
            return None
    try:
        return int(raw)
    except ValueError:
        return None


def parse_opt_bool(raw: str) -> Optional[bool]:
    if raw == "None":
        return None
    if raw.startswith("Some(") and raw.endswith(")"):
        inner = raw[5:-1]
        if inner == "true":
            return True
        if inner == "false":
            return False
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    return None


def round_start_from_slug(slug: str) -> Optional[int]:
    m = re.search(r"-(\d+)$", slug)
    if not m:
        return None
    return int(m.group(1))


def symbol_from_slug(slug: str) -> Optional[str]:
    m = re.match(r"([a-z0-9]+)-updown-", slug)
    if not m:
        return None
    return f"{m.group(1)}/usd"


@dataclass
class ChainlinkRow:
    slug: str
    symbol: str
    side: str
    open_exact: str
    open_ref: float
    close: float
    detect_ms: int
    latency_from_end_ms: int
    open_ts_ms: int
    close_ts_ms: int


@dataclass
class EmitRow:
    log_file: str
    slug: str
    side: str
    source: str
    open_exact: str
    final_detect_unix_ms: int
    ref_price_log: float
    observed_price_log: float
    latency_from_end_ms: int
    frontend_open: Optional[float]
    frontend_close: Optional[float]
    frontend_ts_ms: Optional[int]
    frontend_completed: Optional[bool]
    frontend_cached: Optional[bool]


@dataclass
class FrontendApiRow:
    open_price: Optional[float]
    close_price: Optional[float]
    timestamp_ms: Optional[int]
    completed: Optional[bool]
    cached: Optional[bool]


def parse_log_file(path: Path) -> Tuple[Dict[Tuple[str, int], ChainlinkRow], List[EmitRow]]:
    chain: Dict[Tuple[str, int], ChainlinkRow] = {}
    emits: List[EmitRow] = []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = strip_ansi(raw_line).strip()
            if "chainlink_result_ready |" in line:
                kv = parse_kv_pairs(line)
                try:
                    row = ChainlinkRow(
                        slug=kv["slug"],
                        symbol=kv.get("symbol", ""),
                        side=kv.get("side", ""),
                        open_exact=kv.get("open_exact", ""),
                        open_ref=float(kv["open_ref"]),
                        close=float(kv["close"]),
                        detect_ms=int(kv["detect_ms"]),
                        latency_from_end_ms=int(kv.get("latency_from_end_ms", "0")),
                        open_ts_ms=int(kv["open_ts_ms"]),
                        close_ts_ms=int(kv["close_ts_ms"]),
                    )
                    chain[(row.slug, row.detect_ms)] = row
                except (KeyError, ValueError):
                    continue
            elif "post_close_emit_winner_hint |" in line:
                kv = parse_kv_pairs(line)
                try:
                    emit = EmitRow(
                        log_file=str(path),
                        slug=kv["slug"],
                        side=kv.get("side", ""),
                        source=kv.get("source", ""),
                        open_exact=kv.get("open_exact", ""),
                        final_detect_unix_ms=int(kv["final_detect_unix_ms"]),
                        ref_price_log=float(kv["ref_price"]),
                        observed_price_log=float(kv["observed_price"]),
                        latency_from_end_ms=int(kv.get("latency_from_end_ms", "0")),
                        frontend_open=parse_opt_float(kv.get("frontend_open", "None")),
                        frontend_close=parse_opt_float(kv.get("frontend_close", "None")),
                        frontend_ts_ms=parse_opt_int(kv.get("frontend_ts_ms", "None")),
                        frontend_completed=parse_opt_bool(kv.get("frontend_completed", "None")),
                        frontend_cached=parse_opt_bool(kv.get("frontend_cached", "None")),
                    )
                    emits.append(emit)
                except (KeyError, ValueError):
                    continue
    return chain, emits


def winner_by_delta(delta: float) -> str:
    return "Yes" if delta >= 0.0 else "No"


def fmt_float(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:.9f}"


def fmt_int(v: Optional[int]) -> str:
    if v is None:
        return ""
    return str(v)


def iter_logs(patterns: Iterable[str]) -> List[Path]:
    files: List[Path] = []
    for p in patterns:
        files.extend(Path(x) for x in glob.glob(p))
    # deterministic order
    return sorted(set(files), key=lambda x: str(x))


def ts_to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_frontend_round_prices(symbol: str, round_start_ts: int) -> FrontendApiRow:
    """
    Fetch frontend crypto-price endpoint used by polymarket web app.
    symbol: like "xrp/usd" or "xrp" (case-insensitive)
    """
    base = "https://polymarket.com/api/crypto/crypto-price"
    sym = symbol.split("/")[0].upper()
    end_ts = round_start_ts + 300
    query = urlencode(
        {
            "symbol": sym,
            "eventStartTime": ts_to_iso_utc(round_start_ts),
            "variant": "fiveminute",
            "endDate": ts_to_iso_utc(end_ts),
        }
    )
    url = f"{base}?{query}"
    req = Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "pm-as-ofi-exporter/1.0",
        },
    )
    try:
        with urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body)
    except Exception:
        return FrontendApiRow(None, None, None, None, None)

    return FrontendApiRow(
        open_price=data.get("openPrice"),
        close_price=data.get("closePrice"),
        timestamp_ms=data.get("timestamp"),
        completed=data.get("completed"),
        cached=data.get("cached"),
    )


def precision_rank(src: str) -> int:
    return 2 if src == "chainlink_result_ready" else 1


def pick_better_row(cur: Dict[str, str], nxt: Dict[str, str]) -> bool:
    """
    Return True if nxt should replace cur for same (symbol, round_start_ts).
    """
    cur_score = (
        precision_rank(cur["precision_source"]),
        1 if cur["open_exact"] == "true" else 0,
        1 if cur["winner_match_logged"] == "true" else 0,
        -int(cur["latency_from_end_ms"] or "0"),
        -int(cur["detect_ms"] or "0"),
    )
    nxt_score = (
        precision_rank(nxt["precision_source"]),
        1 if nxt["open_exact"] == "true" else 0,
        1 if nxt["winner_match_logged"] == "true" else 0,
        -int(nxt["latency_from_end_ms"] or "0"),
        -int(nxt["detect_ms"] or "0"),
    )
    return nxt_score > cur_score


def main() -> int:
    ap = argparse.ArgumentParser(description="Export precision-safe post-close rounds CSV")
    ap.add_argument(
        "--logs",
        nargs="+",
        required=True,
        help="Log file glob(s), e.g. logs/polymarket.*-updown-5m.log.2026-04-19",
    )
    ap.add_argument(
        "--out",
        default="docs/stage_g_multi_dryrun.csv",
        help="Output CSV path (default: docs/stage_g_multi_dryrun.csv)",
    )
    args = ap.parse_args()

    files = iter_logs(args.logs)
    if not files:
        raise SystemExit("No log files matched --logs patterns.")

    chain_all: Dict[Tuple[str, int], ChainlinkRow] = {}
    emit_all: List[EmitRow] = []
    for path in files:
        chain, emits = parse_log_file(path)
        chain_all.update(chain)
        emit_all.extend(emits)

    out_rows = []
    for e in emit_all:
        key = (e.slug, e.final_detect_unix_ms)
        chain = chain_all.get(key)
        if chain is not None:
            open_ref = chain.open_ref
            final_price = chain.close
            side_logged = chain.side or e.side
            precision_source = "chainlink_result_ready"
            symbol = chain.symbol or (symbol_from_slug(e.slug) or "")
            open_ts_ms = chain.open_ts_ms
            close_ts_ms = chain.close_ts_ms
        else:
            open_ref = e.ref_price_log
            final_price = e.observed_price_log
            side_logged = e.side
            precision_source = "post_close_emit_winner_hint"
            symbol = symbol_from_slug(e.slug) or ""
            open_ts_ms = None
            close_ts_ms = None

        delta = final_price - open_ref
        side_by_delta = winner_by_delta(delta)
        side_match = str(side_logged == side_by_delta).lower()

        f_delta = None
        f_side = ""
        if e.frontend_open is not None and e.frontend_close is not None:
            f_delta = e.frontend_close - e.frontend_open
            f_side = winner_by_delta(f_delta)

        round_start_ts = round_start_from_slug(e.slug)
        out_rows.append(
            {
                "slug": e.slug,
                "symbol": symbol,
                "round_start_ts": fmt_int(round_start_ts),
                "source": e.source,
                "open_exact": e.open_exact,
                "detect_ms": str(e.final_detect_unix_ms),
                "open_ts_ms": fmt_int(open_ts_ms),
                "close_ts_ms": fmt_int(close_ts_ms),
                "latency_from_end_ms": str(e.latency_from_end_ms),
                "side_logged": side_logged,
                "open_ref": fmt_float(open_ref),
                "final_price": fmt_float(final_price),
                "delta_close_minus_open": fmt_float(delta),
                "winner_by_delta": side_by_delta,
                "winner_match_logged": side_match,
                "frontend_log_open": fmt_float(e.frontend_open),
                "frontend_log_close": fmt_float(e.frontend_close),
                "frontend_log_delta_close_minus_open": fmt_float(f_delta),
                "frontend_log_winner_by_delta": f_side,
                "frontend_log_ts_ms": fmt_int(e.frontend_ts_ms),
                "frontend_log_completed": "" if e.frontend_completed is None else str(e.frontend_completed).lower(),
                "frontend_log_cached": "" if e.frontend_cached is None else str(e.frontend_cached).lower(),
                "precision_source": precision_source,
            }
        )

    # Deduplicate repeated emit lines (same coin/round), keep the most reliable row.
    dedup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in out_rows:
        key = (row["symbol"], row["round_start_ts"])
        cur = dedup.get(key)
        if cur is None or pick_better_row(cur, row):
            dedup[key] = row

    out_rows = list(dedup.values())

    # Refresh frontend open/close via current endpoint (validation-only).
    frontend_cache: Dict[Tuple[str, int], FrontendApiRow] = {}
    for row in out_rows:
        try:
            round_start_ts = int(row["round_start_ts"])
        except Exception:
            round_start_ts = 0
        key = (row["symbol"], round_start_ts)
        if key not in frontend_cache and round_start_ts > 0 and row["symbol"]:
            frontend_cache[key] = fetch_frontend_round_prices(row["symbol"], round_start_ts)
        fr = frontend_cache.get(key, FrontendApiRow(None, None, None, None, None))
        f_delta = None
        f_side = ""
        if fr.open_price is not None and fr.close_price is not None:
            f_delta = fr.close_price - fr.open_price
            f_side = winner_by_delta(f_delta)
        row["frontend_api_open"] = fmt_float(fr.open_price)
        row["frontend_api_close"] = fmt_float(fr.close_price)
        row["frontend_api_delta_close_minus_open"] = fmt_float(f_delta)
        row["frontend_api_winner_by_delta"] = f_side
        row["frontend_api_ts_ms"] = fmt_int(fr.timestamp_ms)
        row["frontend_api_completed"] = "" if fr.completed is None else str(fr.completed).lower()
        row["frontend_api_cached"] = "" if fr.cached is None else str(fr.cached).lower()

    # Group same symbol together; each symbol sorted by round_start_ts.
    out_rows.sort(key=lambda r: (r["symbol"], int(r["round_start_ts"] or "0")))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "slug",
        "symbol",
        "round_start_ts",
        "source",
        "open_exact",
        "detect_ms",
        "open_ts_ms",
        "close_ts_ms",
        "latency_from_end_ms",
        "side_logged",
        "open_ref",
        "final_price",
        "delta_close_minus_open",
        "winner_by_delta",
        "winner_match_logged",
        "frontend_api_open",
        "frontend_api_close",
        "frontend_api_delta_close_minus_open",
        "frontend_api_winner_by_delta",
        "frontend_api_ts_ms",
        "frontend_api_completed",
        "frontend_api_cached",
        "frontend_log_open",
        "frontend_log_close",
        "frontend_log_delta_close_minus_open",
        "frontend_log_winner_by_delta",
        "frontend_log_ts_ms",
        "frontend_log_completed",
        "frontend_log_cached",
        "precision_source",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"wrote {len(out_rows)} rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
