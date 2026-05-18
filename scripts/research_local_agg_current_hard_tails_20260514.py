#!/usr/bin/env python3
"""Replay current 2026-05-14 XRP/DOGE hard-tail candidates.

Offline research only. This script does not change runtime behavior, service
state, gates, trading, or production posture.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
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
from research_local_agg_doge_shallow_window import parse_sources
from research_local_agg_frozen_debias_family import select_deterministic_price
from research_local_agg_lag_selector_models import filter_rows, load_run_rows


@dataclass(frozen=True)
class RunRow:
    run_id: str
    row: GateRow
    contexts: dict[tuple[str, int], BoundaryContext]


def side_yes(price: float, open_price: float) -> bool:
    return price >= open_price


def margin_bps(price: float, open_price: float) -> float:
    return abs(price - open_price) / max(abs(open_price), 1e-12) * 10_000.0


def score(errors: list[float], side_errors: int) -> dict[str, float | int | None]:
    if not errors:
        return {
            "n": 0,
            "mean_bps": None,
            "p50_bps": None,
            "p90_bps": None,
            "p95_bps": None,
            "p99_bps": None,
            "max_bps": None,
            "pct_le_1bps": None,
            "side_errors": side_errors,
        }
    return {
        "n": len(errors),
        "mean_bps": statistics.mean(errors),
        "p50_bps": percentile(errors, 0.50),
        "p90_bps": percentile(errors, 0.90),
        "p95_bps": percentile(errors, 0.95),
        "p99_bps": percentile(errors, 0.99),
        "max_bps": max(errors),
        "pct_le_1bps": sum(1 for value in errors if value <= 1.0) / len(errors),
        "side_errors": side_errors,
    }


def load_inputs(run_dirs: list[str], gate_status: str) -> list[RunRow]:
    out: list[RunRow] = []
    for raw_run_dir in run_dirs:
        run_dir = Path(raw_run_dir)
        rows, contexts = load_run_rows(run_dir)
        rows = filter_rows(rows, "", gate_status, "", 0.0)
        out.extend(RunRow(run_id=run_dir.name, row=row, contexts=contexts) for row in rows)
    return sorted(out, key=lambda item: (item.run_id, item.row.round_end_ts, item.row.symbol))


def xrp_single_binance_yes_stale_upper_margin(row: GateRow) -> bool:
    if row.symbol != "xrp/usd" or row.local_close is None:
        return False
    return (
        row.source_subset == "only_binance_coinbase"
        and row.rule == "nearest_abs"
        and row.local_sources == "binance"
        and side_yes(row.local_close, row.rtds_open)
        and row.direction_margin_bps is not None
        and 3.5 <= row.direction_margin_bps < 4.6
        and row.close_abs_delta_ms is not None
        and 1_500 <= row.close_abs_delta_ms <= 2_500
    )


def select_xrp_coinbase_pre_proxy(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
) -> Candidate | None:
    if not xrp_single_binance_yes_stale_upper_margin(row) or row.local_close is None:
        return None
    local_side = side_yes(row.local_close, row.rtds_open)
    candidates: list[Candidate] = []
    for candidate in unique_candidates(
        contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates
    ):
        if candidate.source != "coinbase":
            continue
        if candidate.offset_ms is None or candidate.offset_ms > 0:
            continue
        if abs(candidate.offset_ms) > 20_000:
            continue
        if side_yes(candidate.price, row.rtds_open) == local_side:
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates, key=lambda item: (abs(item.offset_ms or 0), item.error_bps, item.kind))


def select_doge_bybit_okx_deeper(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    *,
    min_local_margin_bps: float,
    min_source_spread_bps: float,
    max_source_spread_bps: float,
    min_deeper_margin_bps: float,
    pre_ms: int,
    sources: tuple[str, ...],
) -> Candidate | None:
    if row.symbol != "doge/usd" or row.local_close is None:
        return None
    if row.source_subset != "drop_binance" or row.rule != "last_before":
        return None
    if set(filter(None, row.local_sources.split(";"))) != {"bybit", "okx"}:
        return None
    if row.local_close_spread_bps is None:
        return None
    if not (min_source_spread_bps <= row.local_close_spread_bps <= max_source_spread_bps):
        return None

    local_side = side_yes(row.local_close, row.rtds_open)
    if local_side:
        return None
    local_margin = margin_bps(row.local_close, row.rtds_open)
    if local_margin < min_local_margin_bps:
        return None

    candidates: list[Candidate] = []
    for candidate in unique_candidates(
        contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates
    ):
        if candidate.source not in sources:
            continue
        if candidate.offset_ms is None or candidate.offset_ms > 0:
            continue
        if abs(candidate.offset_ms) > pre_ms:
            continue
        if side_yes(candidate.price, row.rtds_open) != local_side:
            continue
        candidate_margin = margin_bps(candidate.price, row.rtds_open)
        if candidate_margin < local_margin + min_deeper_margin_bps:
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            margin_bps(item.price, row.rtds_open),
            abs(item.offset_ms or 0),
            item.source,
            item.kind,
        ),
    )


def evaluate(
    run_rows: list[RunRow],
    *,
    mode: str,
    doge_sources: tuple[str, ...],
    doge_pre_ms: int,
    doge_min_local_margin_bps: float,
    doge_min_source_spread_bps: float,
    doge_max_source_spread_bps: float,
    doge_min_deeper_margin_bps: float,
    regression_threshold_bps: float,
    tail_n: int,
) -> dict[str, Any]:
    base_errors: list[float] = []
    candidate_errors: list[float] = []
    base_side_errors = 0
    candidate_side_errors = 0
    selected_rows: list[dict[str, Any]] = []
    row_regressions: list[dict[str, Any]] = []
    by_symbol_base: dict[str, list[float]] = defaultdict(list)
    by_symbol_candidate: dict[str, list[float]] = defaultdict(list)
    by_symbol_base_side: dict[str, int] = defaultdict(int)
    by_symbol_candidate_side: dict[str, int] = defaultdict(int)
    by_run_base: dict[str, list[float]] = defaultdict(list)
    by_run_candidate: dict[str, list[float]] = defaultdict(list)
    by_run_base_side: dict[str, int] = defaultdict(int)
    by_run_candidate_side: dict[str, int] = defaultdict(int)

    for item in run_rows:
        row = item.row
        if row.local_close is None:
            continue
        base_reason, base_price, base_extra = select_deterministic_price(
            row,
            item.contexts,
            doge_sources=parse_sources("all"),
            doge_pre_ms=1_000,
            doge_min_improve_margin_bps=6.0,
            doge_min_local_margin_bps=8.0,
            doge_max_source_spread_bps=4.0,
            doge_okx_sources=parse_sources("cex"),
            doge_okx_pre_ms=30_000,
            doge_okx_min_local_margin_bps=30.0,
            doge_okx_min_deeper_margin_bps=2.0,
        )
        if base_price is None:
            continue

        final_reason = base_reason
        final_price = base_price
        final_extra: dict[str, Any] = dict(base_extra)
        dropped = False

        if mode in {"xrp_gate", "combined_gate"} and xrp_single_binance_yes_stale_upper_margin(row):
            dropped = True
            final_reason = "xrp_single_binance_yes_stale_upper_margin_containment"
        elif mode in {"xrp_proxy", "combined_proxy"}:
            candidate = select_xrp_coinbase_pre_proxy(row, item.contexts)
            if candidate is not None:
                final_reason = "xrp_single_binance_coinbase_pre_proxy"
                final_price = candidate.price
                final_extra = {
                    "selected_source": candidate.source,
                    "selected_kind": candidate.kind,
                    "selected_offset_ms": candidate.offset_ms,
                    "selected_price": candidate.price,
                    "selected_signed_bps": signed_bps(candidate.price, row.rtds_close),
                }

        if not dropped and mode in {"doge_deeper", "combined_gate", "combined_proxy"}:
            candidate = select_doge_bybit_okx_deeper(
                row,
                item.contexts,
                min_local_margin_bps=doge_min_local_margin_bps,
                min_source_spread_bps=doge_min_source_spread_bps,
                max_source_spread_bps=doge_max_source_spread_bps,
                min_deeper_margin_bps=doge_min_deeper_margin_bps,
                pre_ms=doge_pre_ms,
                sources=doge_sources,
            )
            if candidate is not None:
                final_reason = "doge_bybit_okx_same_side_deeper_window"
                final_price = candidate.price
                final_extra = {
                    "selected_source": candidate.source,
                    "selected_kind": candidate.kind,
                    "selected_offset_ms": candidate.offset_ms,
                    "selected_price": candidate.price,
                    "selected_signed_bps": signed_bps(candidate.price, row.rtds_close),
                }

        base_error = bps_diff(base_price, row.rtds_close)
        base_side_error = side_yes(base_price, row.rtds_open) != side_yes(row.rtds_close, row.rtds_open)
        base_side_errors += int(base_side_error)
        base_errors.append(base_error)
        by_symbol_base[row.symbol].append(base_error)
        by_symbol_base_side[row.symbol] += int(base_side_error)
        by_run_base[item.run_id].append(base_error)
        by_run_base_side[item.run_id] += int(base_side_error)

        if dropped:
            selected_rows.append(
                {
                    "run_id": item.run_id,
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "candidate_reason": final_reason,
                    "base_reason": base_reason,
                    "base_error_bps": base_error,
                    "candidate_error_bps": None,
                    "improvement_vs_base_bps": None,
                    "base_side_error": base_side_error,
                    "candidate_side_error": None,
                    "source_subset": row.source_subset,
                    "rule": row.rule,
                    "local_sources": row.local_sources,
                    "direction_margin_bps": row.direction_margin_bps,
                    "local_spread_bps": row.local_close_spread_bps,
                    "close_abs_delta_ms": row.close_abs_delta_ms,
                    "coverage_action": "drop_from_accepted",
                }
            )
            continue

        candidate_error = bps_diff(final_price, row.rtds_close)
        candidate_side_error = side_yes(final_price, row.rtds_open) != side_yes(
            row.rtds_close, row.rtds_open
        )
        candidate_side_errors += int(candidate_side_error)
        candidate_errors.append(candidate_error)
        by_symbol_candidate[row.symbol].append(candidate_error)
        by_symbol_candidate_side[row.symbol] += int(candidate_side_error)
        by_run_candidate[item.run_id].append(candidate_error)
        by_run_candidate_side[item.run_id] += int(candidate_side_error)

        if final_reason != base_reason:
            selected_rows.append(
                {
                    "run_id": item.run_id,
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "candidate_reason": final_reason,
                    "base_reason": base_reason,
                    "base_error_bps": base_error,
                    "candidate_error_bps": candidate_error,
                    "improvement_vs_base_bps": base_error - candidate_error,
                    "base_side_error": base_side_error,
                    "candidate_side_error": candidate_side_error,
                    "source_subset": row.source_subset,
                    "rule": row.rule,
                    "local_sources": row.local_sources,
                    "direction_margin_bps": row.direction_margin_bps,
                    "local_spread_bps": row.local_close_spread_bps,
                    "close_abs_delta_ms": row.close_abs_delta_ms,
                    **final_extra,
                }
            )
        if candidate_error - base_error > regression_threshold_bps:
            row_regressions.append(
                {
                    "run_id": item.run_id,
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "base_reason": base_reason,
                    "candidate_reason": final_reason,
                    "base_error_bps": base_error,
                    "candidate_error_bps": candidate_error,
                    "regression_bps": candidate_error - base_error,
                    "source_subset": row.source_subset,
                    "rule": row.rule,
                    "local_sources": row.local_sources,
                    "direction_margin_bps": row.direction_margin_bps,
                    "local_spread_bps": row.local_close_spread_bps,
                    "close_abs_delta_ms": row.close_abs_delta_ms,
                }
            )

    by_symbol = {}
    for symbol in sorted(set(by_symbol_base) | set(by_symbol_candidate)):
        by_symbol[symbol] = {
            "base": score(by_symbol_base[symbol], by_symbol_base_side[symbol]),
            "candidate": score(by_symbol_candidate[symbol], by_symbol_candidate_side[symbol]),
        }
    by_run = {}
    for run_id in sorted(set(by_run_base) | set(by_run_candidate)):
        by_run[run_id] = {
            "base": score(by_run_base[run_id], by_run_base_side[run_id]),
            "candidate": score(by_run_candidate[run_id], by_run_candidate_side[run_id]),
        }
    return {
        "base": score(base_errors, base_side_errors),
        "candidate": score(candidate_errors, candidate_side_errors),
        "by_symbol": by_symbol,
        "by_run": by_run,
        "selected_count": len(selected_rows),
        "selected_rows": sorted(
            selected_rows,
            key=lambda item: (
                str(item.get("run_id")),
                str(item.get("symbol")),
                int(item.get("round_end_ts") or 0),
            ),
        )[:tail_n],
        "row_regression_count": len(row_regressions),
        "row_regressions": sorted(
            row_regressions,
            key=lambda item: float(item.get("regression_bps") or 0.0),
            reverse=True,
        )[:tail_n],
    }


def decide(output: dict[str, Any]) -> tuple[str, str]:
    candidate = output["candidate"]
    base = output["base"]
    if output["row_regression_count"]:
        return "discard", "candidate has row-level regressions above threshold"
    if (candidate.get("side_errors") or 0) > 0:
        return "discard", "candidate leaves accepted side errors"
    if (base.get("side_errors") or 0) > 0 and candidate.get("side_errors") == 0:
        return "keep_research_only", "candidate removes side errors on fixed replay"
    if (candidate.get("max_bps") or float("inf")) < (base.get("max_bps") or float("inf")):
        return "keep_research_only", "candidate reduces max without side regression"
    if (candidate.get("p95_bps") or float("inf")) < (base.get("p95_bps") or float("inf")):
        return "keep_research_only", "candidate reduces p95 without side regression"
    return "discard", "candidate does not dominate baseline"


def append_ledger(path: Path, output: dict[str, Any]) -> None:
    row = {
        "timestamp": int(time.time()),
        "candidate_id": output["candidate_id"],
        "hypothesis": output["hypothesis"],
        "touched_files": ["scripts/research_local_agg_current_hard_tails_20260514.py"],
        "replay_runs": output["config"]["run_ids"],
        "global_base": output["base"],
        "global_candidate": output["candidate"],
        "selected_count": output["selected_count"],
        "row_regression_count": output["row_regression_count"],
        "decision": output["decision"],
        "decision_note": output["decision_note"],
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", action="append", required=True)
    parser.add_argument("--gate-status", default="accepted")
    parser.add_argument(
        "--mode",
        choices=["xrp_gate", "xrp_proxy", "doge_deeper", "combined_gate", "combined_proxy"],
        required=True,
    )
    parser.add_argument("--doge-sources", default="bybit,coinbase,okx")
    parser.add_argument("--doge-pre-ms", type=int, default=30_000)
    parser.add_argument("--doge-min-local-margin-bps", type=float, default=12.0)
    parser.add_argument("--doge-min-source-spread-bps", type=float, default=2.5)
    parser.add_argument("--doge-max-source-spread-bps", type=float, default=4.6)
    parser.add_argument("--doge-min-deeper-margin-bps", type=float, default=3.0)
    parser.add_argument("--regression-threshold-bps", type=float, default=0.5)
    parser.add_argument("--tail-n", type=int, default=30)
    parser.add_argument("--out-json", default="")
    parser.add_argument("--ledger-jsonl", default="")
    args = parser.parse_args()

    run_rows = load_inputs(args.run_dir, args.gate_status)
    result = evaluate(
        run_rows,
        mode=args.mode,
        doge_sources=parse_sources(args.doge_sources),
        doge_pre_ms=args.doge_pre_ms,
        doge_min_local_margin_bps=args.doge_min_local_margin_bps,
        doge_min_source_spread_bps=args.doge_min_source_spread_bps,
        doge_max_source_spread_bps=args.doge_max_source_spread_bps,
        doge_min_deeper_margin_bps=args.doge_min_deeper_margin_bps,
        regression_threshold_bps=args.regression_threshold_bps,
        tail_n=args.tail_n,
    )
    output = {
        "candidate_id": f"current_hard_tails_20260514_{args.mode}",
        "hypothesis": (
            "The 2026-05-14 XRP side-error and DOGE bybit/OKX hard tails can be "
            "contained or repaired with narrow decision-time regimes without "
            "side/BTC regression."
        ),
        "config": {
            "run_dirs": args.run_dir,
            "run_ids": [Path(path).name for path in args.run_dir],
            "gate_status": args.gate_status,
            "mode": args.mode,
            "doge_sources": parse_sources(args.doge_sources),
            "doge_pre_ms": args.doge_pre_ms,
            "doge_min_local_margin_bps": args.doge_min_local_margin_bps,
            "doge_min_source_spread_bps": args.doge_min_source_spread_bps,
            "doge_max_source_spread_bps": args.doge_max_source_spread_bps,
            "doge_min_deeper_margin_bps": args.doge_min_deeper_margin_bps,
            "regression_threshold_bps": args.regression_threshold_bps,
        },
        **result,
    }
    decision, note = decide(output)
    output["decision"] = decision
    output["decision_note"] = note

    text = json.dumps(output, indent=2, sort_keys=True)
    print(text)
    if args.out_json:
        Path(args.out_json).write_text(text + "\n", encoding="utf-8")
    if args.ledger_jsonl:
        append_ledger(Path(args.ledger_jsonl), output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
