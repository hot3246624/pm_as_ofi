#!/usr/bin/env python3
"""Replay the HYPE source-lag regime selector against local-agg run logs.

This mirrors the Rust shadow candidate selector in polymarket_v2.rs:
it uses only decision-time features (RDTS open, current local source set,
source spread, current direction margin, and visible source-lag candidates).
The RDTS close is used only after selection to score the candidate.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from research_local_agg_boundary_lag import (
    BoundaryContext,
    Candidate,
    GateRow,
    bps_diff,
    percentile,
    signed_bps,
    unique_candidates,
)
from research_local_agg_lag_selector_models import filter_rows, load_run_rows


def side_yes(price: float, open_price: float) -> bool:
    return price >= open_price


def margin_bps(price: float, open_price: float) -> float:
    return abs(price - open_price) / max(abs(open_price), 1e-12) * 10_000.0


def candidate_pool(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    source: str,
    max_age_ms: int,
    min_age_ms: int = 0,
) -> list[Candidate]:
    if row.local_close is None:
        return []
    current_side = side_yes(row.local_close, row.rtds_open)
    out: list[Candidate] = []
    for candidate in unique_candidates(
        contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates
    ):
        if candidate.source != source or candidate.offset_ms is None:
            continue
        age = abs(candidate.offset_ms)
        if candidate.offset_ms > 0 or age > max_age_ms or age < min_age_ms:
            continue
        if side_yes(candidate.price, row.rtds_open) != current_side:
            continue
        out.append(candidate)
    return out


def deepest_pre(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    source: str,
    max_age_ms: int,
    min_age_ms: int = 0,
) -> Candidate | None:
    return max(
        candidate_pool(row, contexts, source, max_age_ms, min_age_ms),
        key=lambda c: (margin_bps(c.price, row.rtds_open), -abs(c.offset_ms or 0), c.kind),
        default=None,
    )


def closest_pre(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    source: str,
    max_age_ms: int,
) -> Candidate | None:
    return min(
        candidate_pool(row, contexts, source, max_age_ms),
        key=lambda c: (abs(c.offset_ms or 0), c.kind),
        default=None,
    )


def select_hype_source_lag_regime(
    row: GateRow, contexts: dict[tuple[str, int], BoundaryContext]
) -> tuple[str, Candidate] | tuple[None, None]:
    if row.symbol != "hype/usd" or row.local_close is None:
        return None, None
    sources = set(filter(None, row.local_sources.split(";")))
    spread = row.local_close_spread_bps or 0.0
    margin = margin_bps(row.local_close, row.rtds_open)

    if "coinbase" in sources and spread >= 3.5:
        if candidate := deepest_pre(row, contexts, "coinbase", 5_000):
            return "hype_source_lag_coinbase_spread", candidate

    if sources == {"hyperliquid", "okx"} and spread >= 4.0:
        if candidate := closest_pre(row, contexts, "hyperliquid", 5_000):
            return "hype_source_lag_hl_okx_highspread", candidate

    if sources == {"bybit", "hyperliquid", "okx"} and spread <= 2.0 and margin >= 10.0:
        if candidate := deepest_pre(row, contexts, "coinbase", 30_000, 20_000):
            return "hype_source_lag_coinbase_slow_cluster", candidate

    if sources == {"bybit", "hyperliquid", "okx"} and spread >= 3.0 and margin >= 7.0:
        if candidate := closest_pre(row, contexts, "hyperliquid", 5_000):
            return "hype_source_lag_hl_tri_spread", candidate

    if sources == {"bybit", "hyperliquid", "okx"} and spread <= 1.0 and margin >= 10.0:
        if candidate := closest_pre(row, contexts, "hyperliquid", 5_000):
            return "hype_source_lag_hl_tri_tight", candidate

    if sources == {"bybit", "hyperliquid"} and spread <= 1.0 and margin >= 10.0:
        if candidate := closest_pre(row, contexts, "hyperliquid", 5_000):
            return "hype_source_lag_hl_bybit_tight", candidate

    if (
        sources == {"bybit", "hyperliquid"}
        and 2.8 <= spread <= 3.0
        and 9.0 <= margin <= 10.0
    ):
        if candidate := closest_pre(row, contexts, "hyperliquid", 5_000):
            return "hype_source_lag_hl_bybit_midspread", candidate

    return None, None


def score(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "n": 0,
            "p50_bps": None,
            "p90_bps": None,
            "p95_bps": None,
            "p99_bps": None,
            "max_bps": None,
            "pct_le_1bps": None,
        }
    return {
        "n": len(values),
        "p50_bps": percentile(values, 0.50),
        "p90_bps": percentile(values, 0.90),
        "p95_bps": percentile(values, 0.95),
        "p99_bps": percentile(values, 0.99),
        "max_bps": max(values),
        "pct_le_1bps": sum(1 for value in values if value <= 1.0) / len(values),
    }


def evaluate_run(run_dir: Path, gate_status: str) -> dict[str, Any]:
    rows, contexts = load_run_rows(run_dir)
    rows = filter_rows(rows, "", gate_status, "", 0.0)
    by_symbol_current: dict[str, list[float]] = defaultdict(list)
    by_symbol_regime: dict[str, list[float]] = defaultdict(list)
    selected_rows: list[dict[str, Any]] = []
    side_errors = 0

    for row in rows:
        if row.local_close is None:
            continue
        reason, candidate = select_hype_source_lag_regime(row, contexts)
        selected_price = candidate.price if candidate else row.local_close
        selected_error = bps_diff(selected_price, row.rtds_close) if candidate else row.close_diff_bps
        if side_yes(selected_price, row.rtds_open) != side_yes(row.rtds_close, row.rtds_open):
            side_errors += 1
        by_symbol_current[row.symbol].append(row.close_diff_bps)
        by_symbol_regime[row.symbol].append(selected_error)
        if candidate:
            selected_rows.append(
                {
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "reason": reason,
                    "current_error_bps": row.close_diff_bps,
                    "selected_error_bps": selected_error,
                    "selected_signed_bps": signed_bps(candidate.price, row.rtds_close),
                    "selected_source": candidate.source,
                    "selected_kind": candidate.kind,
                    "selected_offset_ms": candidate.offset_ms,
                    "selected_price": candidate.price,
                    "local_sources": row.local_sources,
                    "local_spread_bps": row.local_close_spread_bps,
                    "direction_margin_bps": row.direction_margin_bps,
                }
            )

    current_all = [value for values in by_symbol_current.values() for value in values]
    regime_all = [value for values in by_symbol_regime.values() for value in values]
    symbols = sorted(set(by_symbol_current) | set(by_symbol_regime))
    return {
        "run_dir": str(run_dir),
        "gate_status": gate_status,
        "rows": len(rows),
        "selected_count": len(selected_rows),
        "selected_side_errors": side_errors,
        "current": score(current_all),
        "regime": score(regime_all),
        "by_symbol": {
            symbol: {
                "current": score(by_symbol_current[symbol]),
                "regime": score(by_symbol_regime[symbol]),
            }
            for symbol in symbols
        },
        "selected_rows": sorted(
            selected_rows, key=lambda item: item["current_error_bps"], reverse=True
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dir", action="append", required=True)
    ap.add_argument("--gate-status", default="accepted")
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    output = {
        "runs": [evaluate_run(Path(run_dir), args.gate_status) for run_dir in args.run_dir],
    }
    text = json.dumps(output, indent=2, sort_keys=True)
    print(text)
    if args.out_json:
        Path(args.out_json).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
