#!/usr/bin/env python3
"""Walk-forward source-lag selector research for local-agg close projection.

This is an offline research harness. It turns the boundary-lag attribution
candidate set into causal-ish walk-forward selectors: for each round it trains
only on earlier rounds, scores visible source/time candidates by historical
source/kind/lag-bucket reliability, applies a learned signed-error bias, and
compares the selected price to the current deployed local close.

It does not change runtime behavior, gates, source sets, or production posture.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from research_local_agg_boundary_lag import (
    BoundaryContext,
    Candidate,
    GateRow,
    add_tape_candidates,
    add_unresolved_candidates,
    bps_diff,
    parse_gate_rows,
    percentile,
    run_files,
    signed_bps,
    unique_candidates,
)


LAG_BUCKETS = (
    ("after", -math.inf, -1.0),
    ("exact", -1.0, 1.0),
    ("pre_0_250ms", 1.0, 250.0),
    ("pre_250ms_1s", 250.0, 1_000.0),
    ("pre_1s_3s", 1_000.0, 3_000.0),
    ("pre_3s_8s", 3_000.0, 8_000.0),
    ("pre_8s_20s", 8_000.0, 20_000.0),
    ("pre_20s_60s", 20_000.0, 60_000.0),
    ("pre_gt_60s", 60_000.0, math.inf),
)


@dataclass
class SelectorParams:
    key: tuple[str, ...]
    n: int
    median_signed_bps: float
    median_abs_bps: float
    p90_abs_bps: float
    p95_abs_bps: float


@dataclass
class SelectedRow:
    symbol: str
    round_end_ts: int
    gate_status: str
    row_gate_status: str
    current_error_bps: float
    selected_error_bps: float
    selected_signed_bps: float
    selected_debiased_error_bps: float
    selected_debiased_signed_bps: float
    side_error: bool
    debiased_side_error: bool
    selected_source: str
    selected_kind: str
    selected_lag_bucket: str
    selected_offset_ms: int | None
    selected_key: tuple[str, ...]
    selected_train_n: int
    selected_score_bps: float
    current_local_sources: str
    current_rule: str
    candidate_count: int


def lag_bucket(candidate: Candidate) -> str:
    if candidate.offset_ms is None:
        return "unknown"
    if candidate.offset_ms > 0:
        return "after"
    age_ms = abs(candidate.offset_ms)
    for name, lo, hi in LAG_BUCKETS:
        if lo <= age_ms < hi:
            return name
    return "unknown"


def kind_family(kind: str) -> str:
    if kind.startswith("diagnostic_"):
        return "diagnostic"
    if kind.startswith("tape_"):
        return "tape"
    return kind


def bps_from_open(price: float, open_price: float) -> float:
    return abs(price - open_price) / max(abs(open_price), 1e-12) * 10_000.0


def margin_bucket(price: float, open_price: float) -> str:
    margin = bps_from_open(price, open_price)
    if margin < 1.0:
        return "m_lt_1"
    if margin < 2.0:
        return "m_1_2"
    if margin < 5.0:
        return "m_2_5"
    if margin < 10.0:
        return "m_5_10"
    if margin < 20.0:
        return "m_10_20"
    return "m_ge_20"


def depth_label(row: GateRow, candidate: Candidate) -> str:
    if row.local_close is None:
        return "unknown_depth"
    current_side_up = row.local_close >= row.rtds_open
    candidate_side_up = candidate.price >= row.rtds_open
    if candidate_side_up != current_side_up:
        return "flip"
    current_margin = bps_from_open(row.local_close, row.rtds_open)
    candidate_margin = bps_from_open(candidate.price, row.rtds_open)
    if candidate_margin > current_margin + 0.25:
        return "same_deeper"
    if candidate_margin + 0.25 < current_margin:
        return "same_shallower"
    return "same_near"


def score_values(errors: list[float]) -> SelectorParams | None:
    if not errors:
        return None
    abs_errors = [abs(value) for value in errors]
    return SelectorParams(
        key=(),
        n=len(errors),
        median_signed_bps=statistics.median(errors),
        median_abs_bps=statistics.median(abs_errors),
        p90_abs_bps=percentile(abs_errors, 0.90) or max(abs_errors),
        p95_abs_bps=percentile(abs_errors, 0.95) or max(abs_errors),
    )


def candidate_keys(row: GateRow, candidate: Candidate) -> list[tuple[str, ...]]:
    bucket = lag_bucket(candidate)
    family = kind_family(candidate.kind)
    source = candidate.source
    symbol = row.symbol
    selected = "selected_source" if source in set(filter(None, row.local_sources.split(";"))) else "other_source"
    margin = margin_bucket(candidate.price, row.rtds_open)
    depth = depth_label(row, candidate)
    return [
        ("symbol_source_kind_lag_depth", symbol, source, family, bucket, depth),
        ("symbol_source_lag_depth", symbol, source, bucket, depth),
        ("symbol_source_depth", symbol, source, depth),
        ("symbol_kind_lag_depth", symbol, family, bucket, depth),
        ("symbol_depth_lag", symbol, depth, bucket),
        ("source_kind_lag_depth", source, family, bucket, depth),
        ("global_kind_lag_depth", family, bucket, depth),
        ("symbol_source_kind_lag", symbol, source, family, bucket),
        ("symbol_source_lag", symbol, source, bucket),
        ("symbol_source", symbol, source),
        ("symbol_kind_lag", symbol, family, bucket),
        ("symbol_selected_lag", symbol, selected, bucket),
        ("symbol_margin_lag", symbol, margin, bucket),
        ("source_margin_lag", source, margin, bucket),
        ("global_margin_lag", margin, bucket),
        ("source_kind_lag", source, family, bucket),
        ("source_lag", source, bucket),
        ("source", source),
        ("symbol", symbol),
        ("global_kind_lag", family, bucket),
        ("global_lag", bucket),
        ("global",),
    ]


def choose_params(
    history: dict[tuple[str, ...], list[float]],
    keys: list[tuple[str, ...]],
    min_train: int,
) -> SelectorParams | None:
    fallback: SelectorParams | None = None
    for key in keys:
        values = history.get(key, [])
        if not values:
            continue
        params = score_values(values)
        if params is None:
            continue
        params.key = key
        if fallback is None:
            fallback = params
        if params.n >= min_train:
            return params
    return fallback


def debias_price(price: float, median_signed_bps: float) -> float:
    return price / (1.0 + median_signed_bps / 10_000.0)


def score_candidate(
    row: GateRow,
    candidate: Candidate,
    params: SelectorParams | None,
    min_train: int,
    recency_penalty_bps_per_sec: float,
    after_penalty_bps: float,
) -> tuple[float, float, float, tuple[str, ...], int]:
    if params is None:
        base = 25.0
        bias = 0.0
        key: tuple[str, ...] = ("none",)
        n = 0
    else:
        support_penalty = max(0, min_train - params.n) * 0.25
        base = params.p90_abs_bps + 0.35 * params.median_abs_bps + support_penalty
        bias = params.median_signed_bps
        key = params.key
        n = params.n
    if candidate.offset_ms is not None:
        base += abs(candidate.offset_ms) / 1_000.0 * recency_penalty_bps_per_sec
        if candidate.offset_ms > 0:
            base += after_penalty_bps
    # Prefer candidates on the same side as the known opening anchor. This is
    # not enough to guarantee profitability, but prevents selectors from
    # choosing a historically reliable source that flips settlement direction.
    candidate_side = candidate.price >= row.rtds_open
    current_side = row.local_close >= row.rtds_open if row.local_close is not None else candidate_side
    if candidate_side != current_side:
        base += 2.0
    debiased = debias_price(candidate.price, bias)
    raw_error = bps_diff(candidate.price, row.rtds_close)
    debiased_error = bps_diff(debiased, row.rtds_close)
    return base, raw_error, debiased_error, key, n


def score_rows(rows: list[SelectedRow], error_getter: Callable[[SelectedRow], float]) -> dict[str, float | int | None]:
    errors = [error_getter(row) for row in rows]
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
            "side_errors": None,
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
        "side_errors": sum(1 for row in rows if row.side_error),
    }


def summarize_selected(rows: list[SelectedRow]) -> dict:
    by_symbol: dict[str, list[SelectedRow]] = defaultdict(list)
    for row in rows:
        by_symbol[row.symbol].append(row)
    return {
        "current": score_rows(rows, lambda row: row.current_error_bps),
        "selector_raw": score_rows(rows, lambda row: row.selected_error_bps),
        "selector_debiased": {
            **score_rows(rows, lambda row: row.selected_debiased_error_bps),
            "side_errors": sum(1 for row in rows if row.debiased_side_error),
        },
        "by_symbol": {
            symbol: {
                "current": score_rows(symbol_rows, lambda row: row.current_error_bps),
                "selector_raw": score_rows(symbol_rows, lambda row: row.selected_error_bps),
                "selector_debiased": {
                    **score_rows(symbol_rows, lambda row: row.selected_debiased_error_bps),
                    "side_errors": sum(1 for row in symbol_rows if row.debiased_side_error),
                },
            }
            for symbol, symbol_rows in sorted(by_symbol.items())
        },
    }


def add_history(history: dict[tuple[str, ...], list[float]], row: GateRow, candidates: list[Candidate]) -> None:
    for candidate in candidates:
        for key in candidate_keys(row, candidate):
            history[key].append(candidate.signed_error_bps)


def filter_rows(
    rows: list[GateRow],
    symbol: str,
    gate_status: str,
    row_gate_status: str,
    min_error_bps: float,
) -> list[GateRow]:
    if symbol:
        rows = [row for row in rows if row.symbol == symbol.lower()]
    if gate_status:
        rows = [row for row in rows if row.gate_status == gate_status]
    if row_gate_status:
        rows = [row for row in rows if row.row_gate_status == row_gate_status]
    if min_error_bps > 0:
        rows = [row for row in rows if row.close_diff_bps >= min_error_bps]
    return rows


def load_run_rows(run_dir: Path) -> tuple[list[GateRow], dict[tuple[str, int], BoundaryContext]]:
    rows = parse_gate_rows(run_files(run_dir))
    truth_by_key = {(row.symbol, row.round_end_ts): row.rtds_close for row in rows}
    contexts: dict[tuple[str, int], BoundaryContext] = {}
    add_tape_candidates(run_dir, contexts, truth_by_key)
    add_unresolved_candidates(run_files(run_dir), contexts, truth_by_key)
    return rows, contexts


def merge_contexts(
    target: dict[tuple[str, int], BoundaryContext],
    source: dict[tuple[str, int], BoundaryContext],
) -> None:
    for key, ctx in source.items():
        target.setdefault(key, BoundaryContext()).candidates.extend(ctx.candidates)


def seed_history(
    history: dict[tuple[str, ...], list[float]],
    rows: list[GateRow],
    contexts: dict[tuple[str, int], BoundaryContext],
) -> int:
    seeded = 0
    for row in sorted(rows, key=lambda item: (item.round_end_ts, item.symbol)):
        candidates = unique_candidates(contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates)
        if not candidates:
            continue
        add_history(history, row, candidates)
        seeded += 1
    return seeded


def evaluate(
    train_rows: list[GateRow],
    eval_rows: list[GateRow],
    contexts: dict[tuple[str, int], BoundaryContext],
    min_train: int,
    warmup_rows: int,
    recency_penalty_bps_per_sec: float,
    after_penalty_bps: float,
) -> tuple[list[SelectedRow], dict]:
    history: dict[tuple[str, ...], list[float]] = defaultdict(list)
    seeded_rows = seed_history(history, train_rows, contexts)
    selected_rows: list[SelectedRow] = []
    skipped = defaultdict(int)
    ordered = sorted(eval_rows, key=lambda row: (row.round_end_ts, row.symbol))

    for idx, row in enumerate(ordered):
        candidates = sorted(
            unique_candidates(contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates),
            key=lambda item: (item.error_bps, item.source, item.kind),
        )
        if not candidates:
            skipped["no_candidates"] += 1
            continue

        if seeded_rows > 0 or idx >= warmup_rows:
            scored = []
            for candidate in candidates:
                params = choose_params(history, candidate_keys(row, candidate), min_train)
                score, raw_error, debiased_error, key, n = score_candidate(
                    row,
                    candidate,
                    params,
                    min_train,
                    recency_penalty_bps_per_sec,
                    after_penalty_bps,
                )
                scored.append((score, raw_error, debiased_error, key, n, candidate, params))
            scored.sort(
                key=lambda item: (
                    item[0],
                    0 if item[5].source in set(filter(None, row.local_sources.split(";"))) else 1,
                    abs(item[5].offset_ms or 0),
                    item[5].source,
                    item[5].kind,
                )
            )
            score, raw_error, debiased_error, key, n, candidate, params = scored[0]
            bias = params.median_signed_bps if params is not None else 0.0
            debiased_price = debias_price(candidate.price, bias)
            truth_side = row.rtds_close >= row.rtds_open
            selected_rows.append(
                SelectedRow(
                    symbol=row.symbol,
                    round_end_ts=row.round_end_ts,
                    gate_status=row.gate_status,
                    row_gate_status=row.row_gate_status,
                    current_error_bps=row.close_diff_bps,
                    selected_error_bps=raw_error,
                    selected_signed_bps=signed_bps(candidate.price, row.rtds_close),
                    selected_debiased_error_bps=debiased_error,
                    selected_debiased_signed_bps=signed_bps(debiased_price, row.rtds_close),
                    side_error=(candidate.price >= row.rtds_open) != truth_side,
                    debiased_side_error=(debiased_price >= row.rtds_open) != truth_side,
                    selected_source=candidate.source,
                    selected_kind=candidate.kind,
                    selected_lag_bucket=lag_bucket(candidate),
                    selected_offset_ms=candidate.offset_ms,
                    selected_key=key,
                    selected_train_n=n,
                    selected_score_bps=score,
                    current_local_sources=row.local_sources,
                    current_rule=row.rule,
                    candidate_count=len(candidates),
                )
            )

        add_history(history, row, candidates)

    skipped["seeded_train_rows"] = seeded_rows
    return selected_rows, dict(skipped)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate walk-forward source-lag selectors against local-agg tails.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument(
        "--train-run-dir",
        action="append",
        default=[],
        help="Optional historical run dir(s) used only to seed selector history before evaluating --run-dir.",
    )
    parser.add_argument("--symbol", default="")
    parser.add_argument("--gate-status", default="", help="Optional final gate_status filter, e.g. accepted or gated.")
    parser.add_argument("--row-gate-status", default="", help="Optional row_gate_status filter.")
    parser.add_argument("--train-gate-status", default="", help="Optional training gate_status filter. Defaults to all train rows.")
    parser.add_argument("--train-row-gate-status", default="", help="Optional training row_gate_status filter.")
    parser.add_argument("--min-error-bps", type=float, default=0.0)
    parser.add_argument("--min-train", type=int, default=12)
    parser.add_argument("--warmup-rows", type=int, default=30)
    parser.add_argument("--recency-penalty-bps-per-sec", type=float, default=0.02)
    parser.add_argument("--after-penalty-bps", type=float, default=0.75)
    parser.add_argument("--tail-n", type=int, default=12)
    parser.add_argument("--out-json", default="")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    rows_all, contexts = load_run_rows(run_dir)
    rows = filter_rows(rows_all, args.symbol, args.gate_status, args.row_gate_status, args.min_error_bps)

    train_rows: list[GateRow] = []
    for train_run in args.train_run_dir:
        train_all, train_contexts = load_run_rows(Path(train_run))
        train_rows.extend(
            filter_rows(
                train_all,
                args.symbol,
                args.train_gate_status,
                args.train_row_gate_status,
                0.0,
            )
        )
        merge_contexts(contexts, train_contexts)

    selected_rows, skipped = evaluate(
        train_rows,
        rows,
        contexts,
        min_train=args.min_train,
        warmup_rows=args.warmup_rows,
        recency_penalty_bps_per_sec=args.recency_penalty_bps_per_sec,
        after_penalty_bps=args.after_penalty_bps,
    )
    tails = sorted(selected_rows, key=lambda row: row.current_error_bps, reverse=True)[: args.tail_n]
    selector_tails = sorted(selected_rows, key=lambda row: row.selected_debiased_error_bps, reverse=True)[: args.tail_n]
    output = {
        "config": {
            "run_dir": str(run_dir),
            "train_run_dir": args.train_run_dir,
            "symbol": args.symbol or None,
            "gate_status": args.gate_status or None,
            "row_gate_status": args.row_gate_status or None,
            "train_gate_status": args.train_gate_status or None,
            "train_row_gate_status": args.train_row_gate_status or None,
            "min_error_bps": args.min_error_bps,
            "min_train": args.min_train,
            "warmup_rows": args.warmup_rows,
            "recency_penalty_bps_per_sec": args.recency_penalty_bps_per_sec,
            "after_penalty_bps": args.after_penalty_bps,
        },
        "input_rows": len(rows),
        "train_rows": len(train_rows),
        "evaluated_rows": len(selected_rows),
        "skipped": skipped,
        "summary": summarize_selected(selected_rows),
        "current_error_tails": [row.__dict__ for row in tails],
        "selector_error_tails": [row.__dict__ for row in selector_tails],
    }
    text = json.dumps(output, indent=2, sort_keys=True)
    print(text)
    if args.out_json:
        Path(args.out_json).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
