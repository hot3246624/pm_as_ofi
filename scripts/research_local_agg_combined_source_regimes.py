#!/usr/bin/env python3
"""Replay combined source-regime research candidates against local-agg runs.

This is an offline research harness only. It does not change runtime behavior,
gate decisions, source sets, or production posture.

Current combined candidate stack:

- HYPE deployed source-lag regime selector, used as a normalized baseline.
- HYPE hyperliquid/OKX mid-spread addendum for the uncovered accepted tail.
- DOGE same-side shallow pre-boundary source-window selector.
- SOL walk-forward debiased selector, restricted to same_shallower Coinbase
  selections to avoid observed row-level regressions.
- BNB walk-forward debiased selector, restricted to rows where the runtime
  source set contains Bybit. This is a research-only candidate for the BNB
  drop_okx regime and is not deployed.

All candidate triggers use decision-time features only. RDTS close is used only
after selection for scoring.
"""

from __future__ import annotations

import argparse
import json
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
)
from research_local_agg_doge_shallow_window import parse_sources, select_doge_shallow_candidate
from research_local_agg_hype_source_lag_regime import (
    deepest_pre,
    margin_bps,
    select_hype_source_lag_regime,
)
from research_local_agg_lag_selector_models import (
    SelectedRow,
    evaluate as evaluate_lag_selector,
    filter_rows,
    load_run_rows,
    merge_contexts,
)


@dataclass(frozen=True)
class RunData:
    run_id: str
    run_dir: Path
    rows: list[GateRow]
    contexts: dict[tuple[str, int], BoundaryContext]


def side_yes(price: float, open_price: float) -> bool:
    return price >= open_price


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


def load_run_data(run_dirs: list[str], gate_status: str) -> list[RunData]:
    out: list[RunData] = []
    for raw_run_dir in run_dirs:
        run_dir = Path(raw_run_dir)
        rows, contexts = load_run_rows(run_dir)
        rows = filter_rows(rows, "", gate_status, "", 0.0)
        out.append(RunData(run_id=run_dir.name, run_dir=run_dir, rows=rows, contexts=contexts))
    return out


def hype_normalized_price(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
) -> tuple[str, float | None, Candidate | None]:
    if row.local_close is None:
        return "missing_local", None, None
    if row.symbol == "hype/usd":
        reason, candidate = select_hype_source_lag_regime(row, contexts)
        if candidate is not None:
            return f"hype:{reason}", candidate.price, candidate
    return "runtime", row.local_close, None


def select_hype_midspread_addendum(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
) -> tuple[str, Candidate] | tuple[None, None]:
    if row.symbol != "hype/usd" or row.local_close is None:
        return None, None
    sources = set(filter(None, row.local_sources.split(";")))
    spread = row.local_close_spread_bps or 0.0
    margin = margin_bps(row.local_close, row.rtds_open)
    if sources != {"hyperliquid", "okx"}:
        return None, None
    if not (2.0 <= spread <= 3.0 and margin >= 5.0):
        return None, None
    candidate = deepest_pre(row, contexts, "hyperliquid", 5_000)
    if candidate is None:
        return None, None
    return "hype_source_lag_hl_okx_midspread", candidate


def sol_depth(selected: SelectedRow) -> str:
    return selected.selected_key[-1] if selected.selected_key else ""


def prepare_sol_candidates(
    run_data: list[RunData],
    train_map: dict[str, list[str]],
    min_train: int,
    warmup_rows: int,
    trigger_depth: str,
    trigger_source: str,
) -> dict[tuple[str, str, int], SelectedRow]:
    by_run = {item.run_id: item for item in run_data}
    selected_by_key: dict[tuple[str, str, int], SelectedRow] = {}
    for run_id, item in by_run.items():
        train_rows: list[GateRow] = []
        contexts: dict[tuple[str, int], BoundaryContext] = dict(item.contexts)
        for train_run_id in train_map.get(run_id, []):
            train = by_run.get(train_run_id)
            if train is None:
                continue
            train_rows.extend([row for row in train.rows if row.symbol == "sol/usd"])
            merge_contexts(contexts, train.contexts)
        eval_rows = [row for row in item.rows if row.symbol == "sol/usd"]
        if not eval_rows:
            continue
        selected_rows, _ = evaluate_lag_selector(
            train_rows,
            eval_rows,
            contexts,
            min_train=min_train,
            warmup_rows=warmup_rows,
            recency_penalty_bps_per_sec=0.02,
            after_penalty_bps=0.75,
        )
        for selected in selected_rows:
            if sol_depth(selected) != trigger_depth:
                continue
            if selected.selected_source != trigger_source:
                continue
            selected_by_key[(run_id, selected.symbol, selected.round_end_ts)] = selected
    return selected_by_key


def prepare_bnb_candidates(
    run_data: list[RunData],
    train_map: dict[str, list[str]],
    min_train: int,
    warmup_rows: int,
    require_local_source: str,
) -> dict[tuple[str, str, int], SelectedRow]:
    by_run = {item.run_id: item for item in run_data}
    selected_by_key: dict[tuple[str, str, int], SelectedRow] = {}
    for run_id, item in by_run.items():
        train_rows: list[GateRow] = []
        contexts: dict[tuple[str, int], BoundaryContext] = dict(item.contexts)
        for train_run_id in train_map.get(run_id, []):
            train = by_run.get(train_run_id)
            if train is None:
                continue
            train_rows.extend([row for row in train.rows if row.symbol == "bnb/usd"])
            merge_contexts(contexts, train.contexts)
        eval_rows = [row for row in item.rows if row.symbol == "bnb/usd"]
        if not eval_rows:
            continue
        selected_rows, _ = evaluate_lag_selector(
            train_rows,
            eval_rows,
            contexts,
            min_train=min_train,
            warmup_rows=warmup_rows,
            recency_penalty_bps_per_sec=0.02,
            after_penalty_bps=0.75,
        )
        for selected in selected_rows:
            local_sources = set(filter(None, selected.current_local_sources.split(";")))
            if require_local_source and require_local_source not in local_sources:
                continue
            selected_by_key[(run_id, selected.symbol, selected.round_end_ts)] = selected
    return selected_by_key


def evaluate_combined(
    run_data: list[RunData],
    *,
    doge_sources: tuple[str, ...],
    doge_pre_ms: int,
    doge_min_improve_margin_bps: float,
    doge_min_local_margin_bps: float,
    doge_max_source_spread_bps: float | None,
    sol_train_map: dict[str, list[str]],
    sol_min_train: int,
    sol_trigger_depth: str,
    sol_trigger_source: str,
    bnb_train_map: dict[str, list[str]],
    bnb_min_train: int,
    bnb_require_local_source: str,
    tail_n: int,
) -> dict[str, Any]:
    sol_candidates = prepare_sol_candidates(
        run_data,
        train_map=sol_train_map,
        min_train=sol_min_train,
        warmup_rows=0,
        trigger_depth=sol_trigger_depth,
        trigger_source=sol_trigger_source,
    )
    bnb_candidates = prepare_bnb_candidates(
        run_data,
        train_map=bnb_train_map,
        min_train=bnb_min_train,
        warmup_rows=0,
        require_local_source=bnb_require_local_source,
    )
    base_errors: list[float] = []
    candidate_errors: list[float] = []
    base_side_errors = 0
    candidate_side_errors = 0
    by_symbol_base: dict[str, list[float]] = defaultdict(list)
    by_symbol_candidate: dict[str, list[float]] = defaultdict(list)
    by_symbol_base_side: dict[str, int] = defaultdict(int)
    by_symbol_candidate_side: dict[str, int] = defaultdict(int)
    by_run_base: dict[str, list[float]] = defaultdict(list)
    by_run_candidate: dict[str, list[float]] = defaultdict(list)
    by_run_base_side: dict[str, int] = defaultdict(int)
    by_run_candidate_side: dict[str, int] = defaultdict(int)
    selected_rows: list[dict[str, Any]] = []

    for item in run_data:
        for row in sorted(item.rows, key=lambda r: (r.round_end_ts, r.symbol)):
            base_reason, base_price, hype_candidate = hype_normalized_price(row, item.contexts)
            if base_price is None:
                continue
            final_reason = base_reason
            final_price = base_price
            extra: dict[str, Any] = {}

            doge_selected = select_doge_shallow_candidate(
                row,
                item.contexts,
                sources=doge_sources,
                pre_ms=doge_pre_ms,
                min_improve_margin_bps=doge_min_improve_margin_bps,
                min_local_margin_bps=doge_min_local_margin_bps,
                max_source_spread_bps=doge_max_source_spread_bps,
            )
            if doge_selected is not None:
                final_reason = doge_selected.reason
                final_price = doge_selected.candidate.price
                extra = {
                    "selected_source": doge_selected.candidate.source,
                    "selected_kind": doge_selected.candidate.kind,
                    "selected_offset_ms": doge_selected.candidate.offset_ms,
                    "selected_price": doge_selected.candidate.price,
                    "selected_signed_bps": signed_bps(doge_selected.candidate.price, row.rtds_close),
                }
            else:
                sol_selected = sol_candidates.get((item.run_id, row.symbol, row.round_end_ts))
                if sol_selected is not None:
                    final_reason = "sol_same_shallower_coinbase_debiased"
                    final_price = row.rtds_close * (1.0 + sol_selected.selected_debiased_signed_bps / 10_000.0)
                    extra = {
                        "selected_source": sol_selected.selected_source,
                        "selected_kind": sol_selected.selected_kind,
                        "selected_lag_bucket": sol_selected.selected_lag_bucket,
                        "selected_offset_ms": sol_selected.selected_offset_ms,
                        "selected_key": list(sol_selected.selected_key),
                        "selected_train_n": sol_selected.selected_train_n,
                        "selected_score_bps": sol_selected.selected_score_bps,
                        "selected_debiased_signed_bps": sol_selected.selected_debiased_signed_bps,
                    }
                else:
                    bnb_selected = bnb_candidates.get((item.run_id, row.symbol, row.round_end_ts))
                    if bnb_selected is not None:
                        final_reason = "bnb_has_bybit_debiased"
                        final_price = row.rtds_close * (1.0 + bnb_selected.selected_debiased_signed_bps / 10_000.0)
                        extra = {
                            "selected_source": bnb_selected.selected_source,
                            "selected_kind": bnb_selected.selected_kind,
                            "selected_lag_bucket": bnb_selected.selected_lag_bucket,
                            "selected_offset_ms": bnb_selected.selected_offset_ms,
                            "selected_key": list(bnb_selected.selected_key),
                            "selected_train_n": bnb_selected.selected_train_n,
                            "selected_score_bps": bnb_selected.selected_score_bps,
                            "selected_debiased_signed_bps": bnb_selected.selected_debiased_signed_bps,
                        }
                    elif hype_candidate is not None:
                        extra = {
                            "selected_source": hype_candidate.source,
                            "selected_kind": hype_candidate.kind,
                            "selected_offset_ms": hype_candidate.offset_ms,
                            "selected_price": hype_candidate.price,
                            "selected_signed_bps": signed_bps(hype_candidate.price, row.rtds_close),
                        }
                    else:
                        hype_addendum_reason, hype_addendum_candidate = (
                            select_hype_midspread_addendum(row, item.contexts)
                        )
                        if hype_addendum_candidate is not None:
                            final_reason = hype_addendum_reason or "hype_midspread_addendum"
                            final_price = hype_addendum_candidate.price
                            extra = {
                                "selected_source": hype_addendum_candidate.source,
                                "selected_kind": hype_addendum_candidate.kind,
                                "selected_offset_ms": hype_addendum_candidate.offset_ms,
                                "selected_price": hype_addendum_candidate.price,
                                "selected_signed_bps": signed_bps(
                                    hype_addendum_candidate.price, row.rtds_close
                                ),
                            }

            base_error = bps_diff(base_price, row.rtds_close)
            candidate_error = bps_diff(final_price, row.rtds_close)
            truth_side = side_yes(row.rtds_close, row.rtds_open)
            base_side_error = side_yes(base_price, row.rtds_open) != truth_side
            candidate_side_error = side_yes(final_price, row.rtds_open) != truth_side

            base_errors.append(base_error)
            candidate_errors.append(candidate_error)
            base_side_errors += int(base_side_error)
            candidate_side_errors += int(candidate_side_error)
            by_symbol_base[row.symbol].append(base_error)
            by_symbol_candidate[row.symbol].append(candidate_error)
            by_symbol_base_side[row.symbol] += int(base_side_error)
            by_symbol_candidate_side[row.symbol] += int(candidate_side_error)
            by_run_base[item.run_id].append(base_error)
            by_run_candidate[item.run_id].append(candidate_error)
            by_run_base_side[item.run_id] += int(base_side_error)
            by_run_candidate_side[item.run_id] += int(candidate_side_error)

            if final_reason != base_reason:
                selected_rows.append(
                    {
                        "run_id": item.run_id,
                        "symbol": row.symbol,
                        "round_end_ts": row.round_end_ts,
                        "base_reason": base_reason,
                        "candidate_reason": final_reason,
                        "base_error_bps": base_error,
                        "candidate_error_bps": candidate_error,
                        "improvement_bps": base_error - candidate_error,
                        "base_side_error": base_side_error,
                        "candidate_side_error": candidate_side_error,
                        "source_subset": row.source_subset,
                        "local_sources": row.local_sources,
                        "rule": row.rule,
                        "local_spread_bps": row.local_close_spread_bps,
                        "direction_margin_bps": row.direction_margin_bps,
                        **extra,
                    }
                )

    symbols = sorted(set(by_symbol_base) | set(by_symbol_candidate))
    runs = sorted(set(by_run_base) | set(by_run_candidate))
    return {
        "scored_rows": len(base_errors),
        "selected_count": len(selected_rows),
        "selected_side_errors": sum(1 for row in selected_rows if row["candidate_side_error"]),
        "base": score_errors(base_errors, base_side_errors),
        "candidate": score_errors(candidate_errors, candidate_side_errors),
        "by_symbol": {
            symbol: {
                "base": score_errors(by_symbol_base[symbol], by_symbol_base_side[symbol]),
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
                "candidate": score_errors(by_run_candidate[run_id], by_run_candidate_side[run_id]),
            }
            for run_id in runs
        },
        "selected_rows": sorted(
            selected_rows,
            key=lambda item: (item["base_error_bps"], item["improvement_bps"]),
            reverse=True,
        )[:tail_n],
        "selected_rows_all_count": len(selected_rows),
    }


def parse_train_map(values: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Expected RUN=TRAIN1,TRAIN2 train map, got {raw!r}")
        run_id, train_raw = raw.split("=", 1)
        out[run_id] = [item for item in train_raw.split(",") if item]
    return out


def decide(output: dict[str, Any]) -> tuple[str, str]:
    base = output["base"]
    candidate = output["candidate"]
    base_btc = output["by_symbol"].get("btc/usd", {}).get("base", {})
    candidate_btc = output["by_symbol"].get("btc/usd", {}).get("candidate", {})
    if output["selected_count"] <= 0:
        return "discard", "combined selector did not fire"
    if (candidate.get("side_errors") or 0) > (base.get("side_errors") or 0):
        return "discard", "combined selector increases side errors"
    if (candidate_btc.get("p95_bps") or 0.0) > (base_btc.get("p95_bps") or 0.0) + 1e-9:
        return "discard", "combined selector regresses BTC p95"
    if (candidate_btc.get("max_bps") or 0.0) > (base_btc.get("max_bps") or 0.0) + 1e-9:
        return "discard", "combined selector regresses BTC max"
    if (candidate.get("max_bps") or 0.0) < (base.get("max_bps") or 0.0):
        return "keep_research_only", "combined selector reduces max without side/BTC regression"
    if (candidate.get("p95_bps") or 0.0) < (base.get("p95_bps") or 0.0):
        return "keep_research_only", "combined selector reduces p95 without side/BTC regression"
    return "discard", "combined selector does not dominate baseline"


def append_ledger(path: Path, output: dict[str, Any], decision: str, note: str) -> None:
    row = {
        "timestamp": int(time.time()),
        "candidate_id": output["candidate_id"],
        "hypothesis": output["hypothesis"],
        "touched_files": ["scripts/research_local_agg_combined_source_regimes.py"],
        "replay_runs": output["config"]["run_ids"],
        "global_base": output["base"],
        "global_candidate": output["candidate"],
        "by_symbol": output["by_symbol"],
        "selected_count": output["selected_count"],
        "selected_side_errors": output["selected_side_errors"],
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
    parser.add_argument(
        "--sol-train-map",
        action="append",
        default=[],
        help="Mapping RUN=TRAIN1,TRAIN2 for causal SOL selector seeding.",
    )
    parser.add_argument("--sol-min-train", type=int, default=8)
    parser.add_argument("--sol-trigger-depth", default="same_shallower")
    parser.add_argument("--sol-trigger-source", default="coinbase")
    parser.add_argument(
        "--bnb-train-map",
        action="append",
        default=[],
        help="Mapping RUN=TRAIN1,TRAIN2 for causal BNB selector seeding.",
    )
    parser.add_argument("--bnb-min-train", type=int, default=20)
    parser.add_argument("--bnb-require-local-source", default="bybit")
    parser.add_argument("--tail-n", type=int, default=30)
    parser.add_argument("--out-json", default="")
    parser.add_argument("--ledger-jsonl", default="")
    args = parser.parse_args()

    run_data = load_run_data(args.run_dir, args.gate_status)
    doge_max_spread = None if args.doge_no_max_source_spread else args.doge_max_source_spread_bps
    train_map = parse_train_map(args.sol_train_map)
    bnb_train_map = parse_train_map(args.bnb_train_map)
    result = evaluate_combined(
        run_data,
        doge_sources=parse_sources(args.doge_sources),
        doge_pre_ms=args.doge_pre_ms,
        doge_min_improve_margin_bps=args.doge_min_improve_margin_bps,
        doge_min_local_margin_bps=args.doge_min_local_margin_bps,
        doge_max_source_spread_bps=doge_max_spread,
        sol_train_map=train_map,
        sol_min_train=args.sol_min_train,
        sol_trigger_depth=args.sol_trigger_depth,
        sol_trigger_source=args.sol_trigger_source,
        bnb_train_map=bnb_train_map,
        bnb_min_train=args.bnb_min_train,
        bnb_require_local_source=args.bnb_require_local_source,
        tail_n=args.tail_n,
    )
    output = {
        "candidate_id": "combined_hype_midspread_doge_sol_bnb_source_regimes",
        "hypothesis": (
            "A common source-regime framework with validated per-symbol triggers "
            "can reduce accepted close tails without side/BTC regression."
        ),
        "complexity_note": (
            "offline-only combination of deployed HYPE normalization, HYPE mid-spread "
            "addendum, DOGE shallow-window research champion, SOL same_shallower Coinbase "
            "debiased trigger, and BNB has-Bybit debiased trigger"
        ),
        "config": {
            "run_dirs": args.run_dir,
            "run_ids": [Path(path).name for path in args.run_dir],
            "gate_status": args.gate_status,
            "doge_sources": parse_sources(args.doge_sources),
            "doge_pre_ms": args.doge_pre_ms,
            "doge_min_improve_margin_bps": args.doge_min_improve_margin_bps,
            "doge_min_local_margin_bps": args.doge_min_local_margin_bps,
            "doge_max_source_spread_bps": doge_max_spread,
            "sol_train_map": train_map,
            "sol_min_train": args.sol_min_train,
            "sol_trigger_depth": args.sol_trigger_depth,
            "sol_trigger_source": args.sol_trigger_source,
            "bnb_train_map": bnb_train_map,
            "bnb_min_train": args.bnb_min_train,
            "bnb_require_local_source": args.bnb_require_local_source,
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
