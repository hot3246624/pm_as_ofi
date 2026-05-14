#!/usr/bin/env python3
"""Replay frozen selector-history/debias family candidates.

This is an offline research harness only. It does not change runtime behavior,
gate decisions, source sets, or production posture.

The script evaluates a two-layer stack:

1. Deterministic source-regime repairs already proposed for the simple runtime
   checkpoint: deployed HYPE normalization, HYPE mid-spread addendum, DOGE
   shallow-window, and DOGE OKX-only deeper-window.
2. Frozen selector-history/debias candidates for SOL, BNB, and XRP. These are
   intentionally modeled as read-only artifacts: train maps are passed in as
   fixed inputs, and no online learning is represented.

The output includes global/per-symbol metrics and row-level regressions for
candidate_error - base_error above a configured threshold.
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
from typing import Any, Callable

from research_local_agg_boundary_lag import (
    GateRow,
    bps_diff,
    percentile,
    signed_bps,
    unique_candidates,
)
from research_local_agg_combined_source_regimes import (
    RunData,
    hype_normalized_price,
    load_run_data,
    parse_train_map,
    select_doge_okx_deeper_candidate,
    select_hype_midspread_addendum,
)
from research_local_agg_doge_shallow_window import parse_sources, select_doge_shallow_candidate
from research_local_agg_lag_selector_models import (
    BoundaryContext,
    SelectedRow,
    candidate_keys,
    choose_params,
    debias_price,
    lag_bucket,
    merge_contexts,
    score_candidate,
    seed_history,
)


@dataclass(frozen=True)
class DebiasSpec:
    name: str
    symbol: str
    train_map: dict[str, list[str]]
    min_train: int
    trigger: Callable[[SelectedRow], bool]
    max_move_bps: float | None = None


def side_yes(price: float, open_price: float) -> bool:
    return price >= open_price


def depth_label(selected: SelectedRow) -> str:
    return selected.selected_key[-1] if selected.selected_key else ""


def score_errors(errors: list[float], side_errors: int) -> dict[str, float | int | None]:
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


def candidate_price_from_selected(row: GateRow, selected: SelectedRow) -> float:
    # SelectedRow stores signed bps relative to RDTS close, not the candidate
    # price. This reconstruction is equivalent for offline replay metrics.
    return row.rtds_close * (1.0 + selected.selected_debiased_signed_bps / 10_000.0)


def move_bps(price: float, reference_price: float) -> float:
    return bps_diff(price, reference_price)


def prepare_debias_candidates(
    run_data: list[RunData],
    spec: DebiasSpec,
) -> dict[tuple[str, str, int], SelectedRow]:
    by_run = {item.run_id: item for item in run_data}
    selected_by_key: dict[tuple[str, str, int], SelectedRow] = {}
    for run_id, item in by_run.items():
        train_rows: list[GateRow] = []
        contexts: dict[tuple[str, int], BoundaryContext] = dict(item.contexts)
        for train_run_id in spec.train_map.get(run_id, []):
            train = by_run.get(train_run_id)
            if train is None:
                continue
            train_rows.extend(row for row in train.rows if row.symbol == spec.symbol)
            merge_contexts(contexts, train.contexts)
        eval_rows = [row for row in item.rows if row.symbol == spec.symbol]
        if not eval_rows:
            continue
        selected_rows = evaluate_lag_selector_frozen(
            train_rows,
            eval_rows,
            contexts,
            min_train=spec.min_train,
            recency_penalty_bps_per_sec=0.02,
            after_penalty_bps=0.75,
        )
        for selected in selected_rows:
            if spec.trigger(selected):
                selected_by_key[(run_id, selected.symbol, selected.round_end_ts)] = selected
    return selected_by_key


def evaluate_lag_selector_frozen(
    train_rows: list[GateRow],
    eval_rows: list[GateRow],
    contexts: dict[tuple[str, int], BoundaryContext],
    *,
    min_train: int,
    recency_penalty_bps_per_sec: float,
    after_penalty_bps: float,
) -> list[SelectedRow]:
    """Evaluate selector candidates with train rows only.

    The existing walk-forward research helper appends each evaluated row to
    history after scoring it. That is causal, but not a frozen artifact. This
    helper deliberately does not update history from eval rows.
    """

    history: dict[tuple[str, ...], list[float]] = defaultdict(list)
    seed_history(history, train_rows, contexts)
    selected_rows: list[SelectedRow] = []
    ordered = sorted(eval_rows, key=lambda row: (row.round_end_ts, row.symbol))

    for row in ordered:
        candidates = sorted(
            unique_candidates(
                contexts.get((row.symbol, row.round_end_ts), BoundaryContext()).candidates
            ),
            key=lambda item: (item.error_bps, item.source, item.kind),
        )
        if not candidates:
            continue

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
        debiased = debias_price(candidate.price, bias)
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
                selected_debiased_signed_bps=signed_bps(debiased, row.rtds_close),
                side_error=(candidate.price >= row.rtds_open) != truth_side,
                debiased_side_error=(debiased >= row.rtds_open) != truth_side,
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

    return selected_rows


def select_deterministic_price(
    row: GateRow,
    contexts: dict[tuple[str, int], Any],
    *,
    doge_sources: tuple[str, ...],
    doge_pre_ms: int,
    doge_min_improve_margin_bps: float,
    doge_min_local_margin_bps: float,
    doge_max_source_spread_bps: float | None,
    doge_okx_sources: tuple[str, ...],
    doge_okx_pre_ms: int,
    doge_okx_min_local_margin_bps: float,
    doge_okx_min_deeper_margin_bps: float,
) -> tuple[str, float | None, dict[str, Any]]:
    base_reason, base_price, _ = hype_normalized_price(row, contexts)
    if base_price is None:
        return "missing_base", None, {}

    final_reason = base_reason
    final_price = base_price
    extra: dict[str, Any] = {}

    hype_reason, hype_candidate = select_hype_midspread_addendum(row, contexts)
    if hype_candidate is not None:
        final_reason = hype_reason or "hype_midspread_addendum"
        final_price = hype_candidate.price
        extra = {
            "selected_source": hype_candidate.source,
            "selected_kind": hype_candidate.kind,
            "selected_offset_ms": hype_candidate.offset_ms,
            "selected_price": hype_candidate.price,
            "selected_signed_bps": signed_bps(hype_candidate.price, row.rtds_close),
        }
        return final_reason, final_price, extra

    doge_selected = select_doge_shallow_candidate(
        row,
        contexts,
        sources=doge_sources,
        pre_ms=doge_pre_ms,
        min_improve_margin_bps=doge_min_improve_margin_bps,
        min_local_margin_bps=doge_min_local_margin_bps,
        max_source_spread_bps=doge_max_source_spread_bps,
    )
    if doge_selected is not None:
        candidate = doge_selected.candidate
        final_reason = doge_selected.reason
        final_price = candidate.price
        extra = {
            "selected_source": candidate.source,
            "selected_kind": candidate.kind,
            "selected_offset_ms": candidate.offset_ms,
            "selected_price": candidate.price,
            "selected_signed_bps": signed_bps(candidate.price, row.rtds_close),
        }
        return final_reason, final_price, extra

    doge_okx_reason, doge_okx_candidate = select_doge_okx_deeper_candidate(
        row,
        contexts,
        sources=doge_okx_sources,
        pre_ms=doge_okx_pre_ms,
        min_local_margin_bps=doge_okx_min_local_margin_bps,
        min_deeper_margin_bps=doge_okx_min_deeper_margin_bps,
    )
    if doge_okx_candidate is not None:
        final_reason = doge_okx_reason or "doge_okx_deeper_window"
        final_price = doge_okx_candidate.price
        extra = {
            "selected_source": doge_okx_candidate.source,
            "selected_kind": doge_okx_candidate.kind,
            "selected_offset_ms": doge_okx_candidate.offset_ms,
            "selected_price": doge_okx_candidate.price,
            "selected_signed_bps": signed_bps(doge_okx_candidate.price, row.rtds_close),
        }

    return final_reason, final_price, extra


def evaluate_family(
    run_data: list[RunData],
    *,
    doge_sources: tuple[str, ...],
    doge_pre_ms: int,
    doge_min_improve_margin_bps: float,
    doge_min_local_margin_bps: float,
    doge_max_source_spread_bps: float | None,
    doge_okx_sources: tuple[str, ...],
    doge_okx_pre_ms: int,
    doge_okx_min_local_margin_bps: float,
    doge_okx_min_deeper_margin_bps: float,
    specs: list[DebiasSpec],
    regression_threshold_bps: float,
    tail_n: int,
) -> dict[str, Any]:
    debias_by_name = {spec.name: prepare_debias_candidates(run_data, spec) for spec in specs}
    debias_by_key: dict[tuple[str, str, int], tuple[str, SelectedRow]] = {}
    for spec in specs:
        for key, selected in debias_by_name[spec.name].items():
            debias_by_key[key] = (spec.name, selected)

    base_errors: list[float] = []
    deterministic_errors: list[float] = []
    candidate_errors: list[float] = []
    base_side_errors = 0
    deterministic_side_errors = 0
    candidate_side_errors = 0
    by_symbol_base: dict[str, list[float]] = defaultdict(list)
    by_symbol_deterministic: dict[str, list[float]] = defaultdict(list)
    by_symbol_candidate: dict[str, list[float]] = defaultdict(list)
    by_symbol_base_side: dict[str, int] = defaultdict(int)
    by_symbol_deterministic_side: dict[str, int] = defaultdict(int)
    by_symbol_candidate_side: dict[str, int] = defaultdict(int)
    by_run_base: dict[str, list[float]] = defaultdict(list)
    by_run_deterministic: dict[str, list[float]] = defaultdict(list)
    by_run_candidate: dict[str, list[float]] = defaultdict(list)
    by_run_base_side: dict[str, int] = defaultdict(int)
    by_run_deterministic_side: dict[str, int] = defaultdict(int)
    by_run_candidate_side: dict[str, int] = defaultdict(int)
    selected_rows: list[dict[str, Any]] = []
    row_regressions: list[dict[str, Any]] = []

    for item in run_data:
        for row in sorted(item.rows, key=lambda r: (r.round_end_ts, r.symbol)):
            base_reason, base_price, _ = hype_normalized_price(row, item.contexts)
            if base_price is None:
                continue
            deterministic_reason, deterministic_price, deterministic_extra = select_deterministic_price(
                row,
                item.contexts,
                doge_sources=doge_sources,
                doge_pre_ms=doge_pre_ms,
                doge_min_improve_margin_bps=doge_min_improve_margin_bps,
                doge_min_local_margin_bps=doge_min_local_margin_bps,
                doge_max_source_spread_bps=doge_max_source_spread_bps,
                doge_okx_sources=doge_okx_sources,
                doge_okx_pre_ms=doge_okx_pre_ms,
                doge_okx_min_local_margin_bps=doge_okx_min_local_margin_bps,
                doge_okx_min_deeper_margin_bps=doge_okx_min_deeper_margin_bps,
            )
            if deterministic_price is None:
                continue

            final_reason = deterministic_reason
            final_price = deterministic_price
            extra = dict(deterministic_extra)

            selected_debias = debias_by_key.get((item.run_id, row.symbol, row.round_end_ts))
            if selected_debias is not None and deterministic_reason == base_reason:
                debias_name, selected = selected_debias
                spec_by_name = {spec.name: spec for spec in specs}
                selected_price = candidate_price_from_selected(row, selected)
                debias_move_bps = move_bps(selected_price, deterministic_price)
                spec = spec_by_name[debias_name]
                if spec.max_move_bps is None or debias_move_bps <= spec.max_move_bps:
                    final_reason = debias_name
                    final_price = selected_price
                    extra = {
                        "selected_source": selected.selected_source,
                        "selected_kind": selected.selected_kind,
                        "selected_lag_bucket": selected.selected_lag_bucket,
                        "selected_offset_ms": selected.selected_offset_ms,
                        "selected_key": list(selected.selected_key),
                        "selected_train_n": selected.selected_train_n,
                        "selected_score_bps": selected.selected_score_bps,
                        "selected_debiased_signed_bps": selected.selected_debiased_signed_bps,
                        "debias_move_bps": debias_move_bps,
                        "max_move_bps": spec.max_move_bps,
                    }

            base_error = bps_diff(base_price, row.rtds_close)
            deterministic_error = bps_diff(deterministic_price, row.rtds_close)
            candidate_error = bps_diff(final_price, row.rtds_close)
            truth_side = side_yes(row.rtds_close, row.rtds_open)
            base_side_error = side_yes(base_price, row.rtds_open) != truth_side
            deterministic_side_error = side_yes(deterministic_price, row.rtds_open) != truth_side
            candidate_side_error = side_yes(final_price, row.rtds_open) != truth_side

            base_errors.append(base_error)
            deterministic_errors.append(deterministic_error)
            candidate_errors.append(candidate_error)
            base_side_errors += int(base_side_error)
            deterministic_side_errors += int(deterministic_side_error)
            candidate_side_errors += int(candidate_side_error)

            by_symbol_base[row.symbol].append(base_error)
            by_symbol_deterministic[row.symbol].append(deterministic_error)
            by_symbol_candidate[row.symbol].append(candidate_error)
            by_symbol_base_side[row.symbol] += int(base_side_error)
            by_symbol_deterministic_side[row.symbol] += int(deterministic_side_error)
            by_symbol_candidate_side[row.symbol] += int(candidate_side_error)
            by_run_base[item.run_id].append(base_error)
            by_run_deterministic[item.run_id].append(deterministic_error)
            by_run_candidate[item.run_id].append(candidate_error)
            by_run_base_side[item.run_id] += int(base_side_error)
            by_run_deterministic_side[item.run_id] += int(deterministic_side_error)
            by_run_candidate_side[item.run_id] += int(candidate_side_error)

            if final_reason != base_reason:
                record = {
                    "run_id": item.run_id,
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "base_reason": base_reason,
                    "candidate_reason": final_reason,
                    "base_error_bps": base_error,
                    "deterministic_error_bps": deterministic_error,
                    "candidate_error_bps": candidate_error,
                    "improvement_vs_base_bps": base_error - candidate_error,
                    "regression_vs_base_bps": candidate_error - base_error,
                    "base_side_error": base_side_error,
                    "candidate_side_error": candidate_side_error,
                    "source_subset": row.source_subset,
                    "local_sources": row.local_sources,
                    "rule": row.rule,
                    "local_spread_bps": row.local_close_spread_bps,
                    "direction_margin_bps": row.direction_margin_bps,
                    **extra,
                }
                selected_rows.append(record)
                if candidate_error - base_error > regression_threshold_bps:
                    row_regressions.append(record)

    symbols = sorted(set(by_symbol_base) | set(by_symbol_candidate))
    runs = sorted(set(by_run_base) | set(by_run_candidate))
    return {
        "scored_rows": len(base_errors),
        "selected_count": len(selected_rows),
        "selected_side_errors": sum(1 for row in selected_rows if row["candidate_side_error"]),
        "row_regression_count": len(row_regressions),
        "base": score_errors(base_errors, base_side_errors),
        "deterministic": score_errors(deterministic_errors, deterministic_side_errors),
        "candidate": score_errors(candidate_errors, candidate_side_errors),
        "by_symbol": {
            symbol: {
                "base": score_errors(by_symbol_base[symbol], by_symbol_base_side[symbol]),
                "deterministic": score_errors(
                    by_symbol_deterministic[symbol],
                    by_symbol_deterministic_side[symbol],
                ),
                "candidate": score_errors(
                    by_symbol_candidate[symbol],
                    by_symbol_candidate_side[symbol],
                ),
            }
            for symbol in symbols
        },
        "by_run": {
            run_id: {
                "base": score_errors(by_run_base[run_id], by_run_base_side[run_id]),
                "deterministic": score_errors(
                    by_run_deterministic[run_id],
                    by_run_deterministic_side[run_id],
                ),
                "candidate": score_errors(by_run_candidate[run_id], by_run_candidate_side[run_id]),
            }
            for run_id in runs
        },
        "selected_rows": sorted(
            selected_rows,
            key=lambda item: (item["base_error_bps"], item["improvement_vs_base_bps"]),
            reverse=True,
        )[:tail_n],
        "row_regressions": sorted(
            row_regressions,
            key=lambda item: item["regression_vs_base_bps"],
            reverse=True,
        )[:tail_n],
    }


def decide(output: dict[str, Any]) -> tuple[str, str]:
    base = output["base"]
    candidate = output["candidate"]
    if output["selected_count"] <= 0:
        return "discard", "frozen debias family did not fire"
    if (candidate.get("side_errors") or 0) > (base.get("side_errors") or 0):
        return "discard", "candidate increases side errors"
    btc_base = output["by_symbol"].get("btc/usd", {}).get("base", {})
    btc_candidate = output["by_symbol"].get("btc/usd", {}).get("candidate", {})
    if (btc_candidate.get("p95_bps") or 0.0) > (btc_base.get("p95_bps") or 0.0) + 1e-9:
        return "discard", "candidate regresses BTC p95"
    if (btc_candidate.get("max_bps") or 0.0) > (btc_base.get("max_bps") or 0.0) + 1e-9:
        return "discard", "candidate regresses BTC max"
    if output["row_regression_count"] > 0:
        return "keep_research_only", "candidate improves aggregate metrics but has row regressions to review"
    if (candidate.get("max_bps") or 0.0) < (base.get("max_bps") or 0.0):
        return "keep_research_only", "candidate reduces max without side/BTC regression"
    if (candidate.get("p95_bps") or 0.0) < (base.get("p95_bps") or 0.0):
        return "keep_research_only", "candidate reduces p95 without side/BTC regression"
    return "discard", "candidate does not dominate baseline"


def append_ledger(path: Path, output: dict[str, Any], decision: str, note: str) -> None:
    row = {
        "timestamp": int(time.time()),
        "candidate_id": output["candidate_id"],
        "hypothesis": output["hypothesis"],
        "touched_files": ["scripts/research_local_agg_frozen_debias_family.py"],
        "replay_runs": output["config"]["run_ids"],
        "global_base": output["base"],
        "global_candidate": output["candidate"],
        "by_symbol": output["by_symbol"],
        "selected_count": output["selected_count"],
        "selected_side_errors": output["selected_side_errors"],
        "row_regression_count": output["row_regression_count"],
        "btc_regression_flag": decision == "discard" and "BTC" in note.upper(),
        "complexity_note": output["complexity_note"],
        "decision": decision,
        "decision_note": note,
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", action="append", required=True)
    parser.add_argument("--gate-status", default="accepted")
    parser.add_argument("--doge-sources", default="all")
    parser.add_argument("--doge-pre-ms", type=int, default=1_000)
    parser.add_argument("--doge-min-improve-margin-bps", type=float, default=6.0)
    parser.add_argument("--doge-min-local-margin-bps", type=float, default=8.0)
    parser.add_argument("--doge-max-source-spread-bps", type=float, default=4.0)
    parser.add_argument("--doge-no-max-source-spread", action="store_true")
    parser.add_argument("--doge-okx-sources", default="cex")
    parser.add_argument("--doge-okx-pre-ms", type=int, default=30_000)
    parser.add_argument("--doge-okx-min-local-margin-bps", type=float, default=30.0)
    parser.add_argument("--doge-okx-min-deeper-margin-bps", type=float, default=2.0)
    parser.add_argument("--sol-train-map", action="append", default=[])
    parser.add_argument("--sol-min-train", type=int, default=8)
    parser.add_argument("--sol-max-move-bps", type=float, default=0.0)
    parser.add_argument("--disable-sol-debias", action="store_true")
    parser.add_argument("--bnb-train-map", action="append", default=[])
    parser.add_argument("--bnb-min-train", type=int, default=20)
    parser.add_argument("--bnb-max-move-bps", type=float, default=0.0)
    parser.add_argument("--disable-bnb-debias", action="store_true")
    parser.add_argument("--xrp-train-map", action="append", default=[])
    parser.add_argument("--xrp-min-train", type=int, default=20)
    parser.add_argument("--xrp-max-move-bps", type=float, default=0.0)
    parser.add_argument("--disable-xrp-debias", action="store_true")
    parser.add_argument("--regression-threshold-bps", type=float, default=0.5)
    parser.add_argument("--tail-n", type=int, default=30)
    parser.add_argument("--out-json", default="")
    parser.add_argument("--ledger-jsonl", default="")
    args = parser.parse_args()

    run_data = load_run_data(args.run_dir, args.gate_status)
    doge_max_spread = None if args.doge_no_max_source_spread else args.doge_max_source_spread_bps
    specs = [
        spec
        for spec in [
            None
            if args.disable_sol_debias
            else DebiasSpec(
                name="sol_same_shallower_coinbase_frozen_debias",
                symbol="sol/usd",
                train_map=parse_train_map(args.sol_train_map),
                min_train=args.sol_min_train,
                trigger=lambda selected: selected.selected_source == "coinbase"
                and depth_label(selected) == "same_shallower",
                max_move_bps=args.sol_max_move_bps or None,
            ),
            None
            if args.disable_bnb_debias
            else DebiasSpec(
                name="bnb_has_bybit_frozen_debias",
                symbol="bnb/usd",
                train_map=parse_train_map(args.bnb_train_map),
                min_train=args.bnb_min_train,
                trigger=lambda selected: "bybit"
                in set(filter(None, selected.current_local_sources.split(";"))),
                max_move_bps=args.bnb_max_move_bps or None,
            ),
            None
            if args.disable_xrp_debias
            else DebiasSpec(
                name="xrp_same_near_frozen_debias",
                symbol="xrp/usd",
                train_map=parse_train_map(args.xrp_train_map),
                min_train=args.xrp_min_train,
                trigger=lambda selected: depth_label(selected) == "same_near",
                max_move_bps=args.xrp_max_move_bps or None,
            ),
        ]
        if spec is not None
    ]
    result = evaluate_family(
        run_data,
        doge_sources=parse_sources(args.doge_sources),
        doge_pre_ms=args.doge_pre_ms,
        doge_min_improve_margin_bps=args.doge_min_improve_margin_bps,
        doge_min_local_margin_bps=args.doge_min_local_margin_bps,
        doge_max_source_spread_bps=doge_max_spread,
        doge_okx_sources=parse_sources(args.doge_okx_sources),
        doge_okx_pre_ms=args.doge_okx_pre_ms,
        doge_okx_min_local_margin_bps=args.doge_okx_min_local_margin_bps,
        doge_okx_min_deeper_margin_bps=args.doge_okx_min_deeper_margin_bps,
        specs=specs,
        regression_threshold_bps=args.regression_threshold_bps,
        tail_n=args.tail_n,
    )
    output = {
        "candidate_id": "frozen_debias_family_hype_doge_sol_bnb_xrp",
        "hypothesis": (
            "A frozen selector-history/debias family can improve SOL/BNB/XRP p95 "
            "after deterministic HYPE/DOGE normalization without side/BTC regression."
        ),
        "complexity_note": (
            "offline-only replay of deterministic HYPE/DOGE plus frozen SOL/BNB/XRP "
            "debiased selectors; requires a model artifact and separate approval before runtime"
        ),
        "config": {
            "run_dirs": args.run_dir,
            "run_ids": [Path(path).name for path in args.run_dir],
            "gate_status": args.gate_status,
            "sol_train_map": parse_train_map(args.sol_train_map),
            "sol_min_train": args.sol_min_train,
            "sol_max_move_bps": args.sol_max_move_bps or None,
            "disable_sol_debias": args.disable_sol_debias,
            "bnb_train_map": parse_train_map(args.bnb_train_map),
            "bnb_min_train": args.bnb_min_train,
            "bnb_max_move_bps": args.bnb_max_move_bps or None,
            "disable_bnb_debias": args.disable_bnb_debias,
            "xrp_train_map": parse_train_map(args.xrp_train_map),
            "xrp_min_train": args.xrp_min_train,
            "xrp_max_move_bps": args.xrp_max_move_bps or None,
            "disable_xrp_debias": args.disable_xrp_debias,
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
        append_ledger(Path(args.ledger_jsonl), output, decision, note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
