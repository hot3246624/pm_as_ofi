#!/usr/bin/env python3
"""Evaluate a DOGE same-side shallow pre-boundary source-window selector.

This is an offline research harness only. It does not change runtime behavior,
gate decisions, source sets, or production posture.

The hypothesis is that some DOGE tails are not fixed by a static source lag:
the oracle/proxy may prefer a same-side, shallower price visible in the
pre-close source tape over the freshest local last-before basket. The selector
uses only decision-time features:

- symbol/source tape around the boundary
- local close side and distance from the RDTS open anchor
- local source spread

The RDTS close is used only after selection for scoring.
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
    unique_candidates,
)
from research_local_agg_hype_source_lag_regime import select_hype_source_lag_regime
from research_local_agg_lag_selector_models import filter_rows, load_run_rows


SOURCE_PRESETS = {
    "all": ("binance", "bybit", "coinbase", "hyperliquid", "okx"),
    "cex": ("binance", "bybit", "coinbase", "okx"),
    "runtime_core": ("binance", "bybit", "okx"),
}


@dataclass(frozen=True)
class RunRow:
    run_id: str
    run_dir: Path
    row: GateRow
    contexts: dict[tuple[str, int], BoundaryContext]


@dataclass(frozen=True)
class SelectedCandidate:
    reason: str
    candidate: Candidate


def side_yes(price: float, open_price: float) -> bool:
    return price >= open_price


def margin_bps(price: float, open_price: float) -> float:
    return abs(price - open_price) / max(abs(open_price), 1e-12) * 10_000.0


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


def parse_sources(raw: str) -> tuple[str, ...]:
    preset = SOURCE_PRESETS.get(raw)
    if preset is not None:
        return preset
    return tuple(sorted(source.strip().lower() for source in raw.split(",") if source.strip()))


def base_price(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    apply_hype_regime: bool,
) -> tuple[str, float | None, Candidate | None]:
    if row.local_close is None:
        return "missing_local", None, None
    if apply_hype_regime and row.symbol == "hype/usd":
        reason, candidate = select_hype_source_lag_regime(row, contexts)
        if candidate is not None:
            return f"hype:{reason}", candidate.price, candidate
    return "runtime", row.local_close, None


def select_doge_shallow_candidate(
    row: GateRow,
    contexts: dict[tuple[str, int], BoundaryContext],
    sources: tuple[str, ...],
    pre_ms: int,
    min_improve_margin_bps: float,
    min_local_margin_bps: float,
    max_source_spread_bps: float | None,
) -> SelectedCandidate | None:
    if row.symbol != "doge/usd" or row.local_close is None:
        return None
    if max_source_spread_bps is not None:
        spread = row.local_close_spread_bps
        if spread is None or spread > max_source_spread_bps:
            return None

    local_margin = margin_bps(row.local_close, row.rtds_open)
    if local_margin < min_local_margin_bps:
        return None

    local_side = side_yes(row.local_close, row.rtds_open)
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
        if candidate_margin > local_margin - min_improve_margin_bps:
            continue
        candidates.append(candidate)

    if not candidates:
        return None

    selected = min(
        candidates,
        key=lambda candidate: (
            margin_bps(candidate.price, row.rtds_open),
            abs(candidate.offset_ms or 0),
            candidate.source,
            candidate.kind,
        ),
    )
    return SelectedCandidate("doge_same_side_shallowest_pre_window", selected)


def load_inputs(run_dirs: list[str], gate_status: str) -> list[RunRow]:
    out: list[RunRow] = []
    for raw_run_dir in run_dirs:
        run_dir = Path(raw_run_dir)
        rows, contexts = load_run_rows(run_dir)
        rows = filter_rows(rows, "", gate_status, "", 0.0)
        run_id = run_dir.name
        out.extend(RunRow(run_id=run_id, run_dir=run_dir, row=row, contexts=contexts) for row in rows)
    return sorted(out, key=lambda item: (item.row.round_end_ts, item.row.symbol, item.run_id))


def evaluate(
    run_rows: list[RunRow],
    *,
    apply_hype_regime: bool,
    sources: tuple[str, ...],
    pre_ms: int,
    min_improve_margin_bps: float,
    min_local_margin_bps: float,
    max_source_spread_bps: float | None,
    tail_n: int,
) -> dict[str, Any]:
    base_errors: list[float] = []
    candidate_errors: list[float] = []
    base_side_errors = 0
    candidate_side_errors = 0
    selected_rows: list[dict[str, Any]] = []
    per_symbol_base: dict[str, list[float]] = defaultdict(list)
    per_symbol_candidate: dict[str, list[float]] = defaultdict(list)
    per_symbol_base_side_errors: dict[str, int] = defaultdict(int)
    per_symbol_candidate_side_errors: dict[str, int] = defaultdict(int)
    per_run_base: dict[str, list[float]] = defaultdict(list)
    per_run_candidate: dict[str, list[float]] = defaultdict(list)
    per_run_base_side_errors: dict[str, int] = defaultdict(int)
    per_run_candidate_side_errors: dict[str, int] = defaultdict(int)

    for item in run_rows:
        row = item.row
        base_reason, current_price, current_candidate = base_price(
            row,
            item.contexts,
            apply_hype_regime=apply_hype_regime,
        )
        if current_price is None:
            continue

        base_error = bps_diff(current_price, row.rtds_close)
        selected = select_doge_shallow_candidate(
            row,
            item.contexts,
            sources=sources,
            pre_ms=pre_ms,
            min_improve_margin_bps=min_improve_margin_bps,
            min_local_margin_bps=min_local_margin_bps,
            max_source_spread_bps=max_source_spread_bps,
        )
        final_price = selected.candidate.price if selected is not None else current_price
        candidate_error = bps_diff(final_price, row.rtds_close)

        truth_side = side_yes(row.rtds_close, row.rtds_open)
        base_side_error = side_yes(current_price, row.rtds_open) != truth_side
        candidate_side_error = side_yes(final_price, row.rtds_open) != truth_side
        base_side_errors += int(base_side_error)
        candidate_side_errors += int(candidate_side_error)
        per_symbol_base_side_errors[row.symbol] += int(base_side_error)
        per_symbol_candidate_side_errors[row.symbol] += int(candidate_side_error)
        per_run_base_side_errors[item.run_id] += int(base_side_error)
        per_run_candidate_side_errors[item.run_id] += int(candidate_side_error)

        base_errors.append(base_error)
        candidate_errors.append(candidate_error)
        per_symbol_base[row.symbol].append(base_error)
        per_symbol_candidate[row.symbol].append(candidate_error)
        per_run_base[item.run_id].append(base_error)
        per_run_candidate[item.run_id].append(candidate_error)

        if selected is not None:
            candidate = selected.candidate
            selected_rows.append(
                {
                    "run_id": item.run_id,
                    "symbol": row.symbol,
                    "round_end_ts": row.round_end_ts,
                    "reason": selected.reason,
                    "base_reason": base_reason,
                    "base_error_bps": base_error,
                    "selected_error_bps": candidate_error,
                    "improvement_bps": base_error - candidate_error,
                    "selected_signed_bps": signed_bps(candidate.price, row.rtds_close),
                    "selected_source": candidate.source,
                    "selected_kind": candidate.kind,
                    "selected_offset_ms": candidate.offset_ms,
                    "selected_price": candidate.price,
                    "local_close": row.local_close,
                    "local_sources": row.local_sources,
                    "source_subset": row.source_subset,
                    "rule": row.rule,
                    "source_spread_bps": row.local_close_spread_bps,
                    "local_margin_bps": margin_bps(row.local_close, row.rtds_open)
                    if row.local_close is not None
                    else None,
                    "selected_margin_bps": margin_bps(candidate.price, row.rtds_open),
                    "truth_margin_bps": margin_bps(row.rtds_close, row.rtds_open),
                    "base_side_error": base_side_error,
                    "selected_side_error": candidate_side_error,
                    "hype_base_candidate": current_candidate.__dict__ if current_candidate else None,
                }
            )

    symbols = sorted(set(per_symbol_base) | set(per_symbol_candidate))
    runs = sorted(set(per_run_base) | set(per_run_candidate))
    return {
        "input_rows": len(run_rows),
        "scored_rows": len(base_errors),
        "selected_count": len(selected_rows),
        "selected_side_errors": sum(1 for row in selected_rows if row["selected_side_error"]),
        "base": score_errors(base_errors, base_side_errors),
        "candidate": score_errors(candidate_errors, candidate_side_errors),
        "by_symbol": {
            symbol: {
                "base": score_errors(per_symbol_base[symbol], per_symbol_base_side_errors[symbol]),
                "candidate": score_errors(
                    per_symbol_candidate[symbol],
                    per_symbol_candidate_side_errors[symbol],
                ),
            }
            for symbol in symbols
        },
        "by_run": {
            run_id: {
                "base": score_errors(per_run_base[run_id], per_run_base_side_errors[run_id]),
                "candidate": score_errors(
                    per_run_candidate[run_id],
                    per_run_candidate_side_errors[run_id],
                ),
            }
            for run_id in runs
        },
        "selected_rows": sorted(
            selected_rows,
            key=lambda item: (item["base_error_bps"], item["improvement_bps"]),
            reverse=True,
        )[:tail_n],
    }


def decide(output: dict[str, Any]) -> tuple[str, str]:
    base = output["base"]
    candidate = output["candidate"]
    base_btc = output["by_symbol"].get("btc/usd", {}).get("base", {})
    candidate_btc = output["by_symbol"].get("btc/usd", {}).get("candidate", {})

    if output["selected_count"] <= 0:
        return "discard", "selector did not fire on the fixed replay suite"
    if (candidate.get("side_errors") or 0) > (base.get("side_errors") or 0):
        return "discard", "candidate increases side errors"
    if (candidate_btc.get("p95_bps") or 0.0) > (base_btc.get("p95_bps") or 0.0) + 1e-9:
        return "discard", "candidate regresses BTC p95"
    if (candidate_btc.get("max_bps") or 0.0) > (base_btc.get("max_bps") or 0.0) + 1e-9:
        return "discard", "candidate regresses BTC max"

    base_max = base.get("max_bps") or 0.0
    candidate_max = candidate.get("max_bps") or 0.0
    base_p95 = base.get("p95_bps") or 0.0
    candidate_p95 = candidate.get("p95_bps") or 0.0
    if candidate_max < base_max and candidate_p95 <= base_p95 + 0.05:
        return "keep_research_only", "candidate reduces max without side/BTC regression"
    if candidate_max <= base_max and candidate_p95 < base_p95:
        return "keep_research_only", "candidate reduces p95 without max/side/BTC regression"
    return "discard", "candidate does not dominate the baseline"


def append_ledger(path: Path, output: dict[str, Any], decision: str, note: str) -> None:
    row = {
        "timestamp": int(time.time()),
        "candidate_id": output["candidate_id"],
        "hypothesis": output["hypothesis"],
        "touched_files": ["scripts/research_local_agg_doge_shallow_window.py"],
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
    parser.add_argument("--sources", default="all")
    parser.add_argument("--pre-ms", type=int, default=1_000)
    parser.add_argument("--min-improve-margin-bps", type=float, default=5.0)
    parser.add_argument("--min-local-margin-bps", type=float, default=8.0)
    parser.add_argument("--max-source-spread-bps", type=float, default=4.0)
    parser.add_argument("--no-max-source-spread", action="store_true")
    parser.add_argument("--apply-hype-regime", action="store_true")
    parser.add_argument("--tail-n", type=int, default=20)
    parser.add_argument("--out-json", default="")
    parser.add_argument("--ledger-jsonl", default="")
    args = parser.parse_args()

    run_rows = load_inputs(args.run_dir, args.gate_status)
    sources = parse_sources(args.sources)
    max_source_spread_bps = None if args.no_max_source_spread else args.max_source_spread_bps
    result = evaluate(
        run_rows,
        apply_hype_regime=args.apply_hype_regime,
        sources=sources,
        pre_ms=args.pre_ms,
        min_improve_margin_bps=args.min_improve_margin_bps,
        min_local_margin_bps=args.min_local_margin_bps,
        max_source_spread_bps=max_source_spread_bps,
        tail_n=args.tail_n,
    )
    output = {
        "candidate_id": "doge_same_side_shallowest_source_window_formal",
        "hypothesis": (
            "DOGE tails may reflect the oracle choosing a same-side shallower "
            "boundary price inside the pre-close source tape rather than the "
            "freshest last-before local basket."
        ),
        "complexity_note": (
            "offline-only per-symbol source-window selector; no runtime change "
            "unless a fixed replay suite dominates the deployed champion"
        ),
        "config": {
            "run_dirs": args.run_dir,
            "run_ids": [Path(path).name for path in args.run_dir],
            "gate_status": args.gate_status,
            "sources": sources,
            "pre_ms": args.pre_ms,
            "min_improve_margin_bps": args.min_improve_margin_bps,
            "min_local_margin_bps": args.min_local_margin_bps,
            "max_source_spread_bps": max_source_spread_bps,
            "apply_hype_regime": args.apply_hype_regime,
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
