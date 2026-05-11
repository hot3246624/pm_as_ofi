#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


NUMERIC_FIELDS = (
    "round_end_ts",
    "source_count",
    "exact_sources",
    "close_abs_delta_ms",
    "source_spread_bps",
    "direction_margin_bps",
    "close_diff_bps",
)


@dataclass(frozen=True)
class GateConfig:
    min_samples: int
    min_margin_bps: float
    quantile: float
    max_train_quantile_bps: float
    max_train_max_bps: float
    safety_bps: float
    max_side_rate: float
    max_source_spread_bps: float
    max_round_max_margin_bps: float
    rescue_min_samples: int
    rescue_max_train_max_bps: float

    def label(self) -> str:
        spread = "off" if self.max_source_spread_bps <= 0 else f"{self.max_source_spread_bps:g}"
        round_margin = "off" if self.max_round_max_margin_bps <= 0 else f"{self.max_round_max_margin_bps:g}"
        rescue = (
            "off"
            if self.rescue_min_samples <= 0 or self.rescue_max_train_max_bps <= 0
            else f"n{self.rescue_min_samples}_max{self.rescue_max_train_max_bps:g}"
        )
        return (
            f"n{self.min_samples}_m{self.min_margin_bps:g}_q{self.quantile:g}"
            f"_maxq{self.max_train_quantile_bps:g}_max{self.max_train_max_bps:g}"
            f"_safe{self.safety_bps:g}"
            f"_side{self.max_side_rate:g}_spr{spread}_rmaxm{round_margin}"
            f"_rescue{rescue}"
        )


class BucketStats:
    __slots__ = ("n", "side_errors", "errors", "q", "q95", "q99", "max_bps", "mean_bps")

    def __init__(self) -> None:
        self.n = 0
        self.side_errors = 0
        self.errors: list[float] = []
        self.q = math.nan
        self.q95 = math.nan
        self.q99 = math.nan
        self.max_bps = math.nan
        self.mean_bps = math.nan

    def add(self, row: dict) -> None:
        err = to_float(row.get("close_diff_bps"))
        if err is None:
            return
        self.n += 1
        self.side_errors += 1 if parse_bool(row.get("side_error")) else 0
        self.errors.append(err)

    def finalize(self, quantile: float) -> "BucketStats":
        if not self.errors:
            return self
        self.errors.sort()
        self.q = percentile(self.errors, quantile)
        self.q95 = percentile(self.errors, 0.95)
        self.q99 = percentile(self.errors, 0.99)
        self.max_bps = self.errors[-1]
        self.mean_bps = sum(self.errors) / len(self.errors)
        return self


def parse_bool(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y"}


def to_float(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except Exception:
        return None
    return value if math.isfinite(value) else None


def to_int(raw: object) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        value = int(float(raw))
    except Exception:
        return None
    return value


def percentile(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    if len(values) == 1:
        return values[0]
    q = max(0.0, min(1.0, q))
    pos = q * (len(values) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return values[lo]
    weight = pos - lo
    return values[lo] * (1.0 - weight) + values[hi] * weight


def bucket(value: float | None, cuts: tuple[float, ...], labels: tuple[str, ...]) -> str:
    if value is None:
        return "missing"
    for cut, label in zip(cuts, labels):
        if value <= cut + 1e-12:
            return label
    return labels[-1]


def delta_bucket(row: dict) -> str:
    return bucket(
        to_float(row.get("close_abs_delta_ms")),
        (50, 100, 250, 500, 1000, 2000, 3500),
        ("d050", "d100", "d250", "d500", "d1000", "d2000", "d3500", "d_gt3500"),
    )


def spread_bucket(row: dict) -> str:
    return bucket(
        to_float(row.get("source_spread_bps")),
        (0, 0.5, 1, 2, 4, 8, 12),
        ("s0", "s050", "s100", "s200", "s400", "s800", "s1200", "s_gt1200"),
    )


def margin_bucket(row: dict) -> str:
    return bucket(
        to_float(row.get("direction_margin_bps")),
        (0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55),
        ("m050", "m100", "m200", "m300", "m500", "m800", "m1300", "m2100", "m3400", "m5500", "m_gt5500"),
    )


def source_count_bucket(row: dict) -> str:
    n = to_int(row.get("source_count"))
    if n is None:
        return "n_missing"
    if n <= 1:
        return "n1"
    if n == 2:
        return "n2"
    if n == 3:
        return "n3"
    return "n4p"


def exact_bucket(row: dict) -> str:
    n = to_int(row.get("exact_sources"))
    return "exact" if n is not None and n > 0 else "no_exact"


def pred_side(row: dict) -> str:
    return "yes" if parse_bool(row.get("pred_yes")) else "no"


def key_levels(row: dict) -> list[tuple[str, tuple[str, ...]]]:
    symbol = str(row.get("symbol", ""))
    subset = str(row.get("source_subset", ""))
    rule = str(row.get("rule", ""))
    sources = str(row.get("sources", ""))
    side = pred_side(row)
    sc = source_count_bucket(row)
    ex = exact_bucket(row)
    d = delta_bucket(row)
    s = spread_bucket(row)
    m = margin_bucket(row)
    return [
        ("full", (symbol, subset, rule, sources, side, sc, ex, d, s, m)),
        ("no_delta", (symbol, subset, rule, sources, side, sc, ex, s, m)),
        ("no_spread", (symbol, subset, rule, sources, side, sc, ex, d, m)),
        ("shape_margin", (symbol, subset, rule, sources, side, sc, ex, m)),
        ("shape", (symbol, subset, rule, sources, side, sc, ex)),
        ("symbol_rule_quality_full", (symbol, rule, side, sc, ex, d, s, m)),
        ("symbol_rule_quality_no_delta", (symbol, rule, side, sc, ex, s, m)),
        ("symbol_rule_quality_no_spread", (symbol, rule, side, sc, ex, d, m)),
        ("symbol_rule_quality_margin", (symbol, rule, side, sc, ex, m)),
        ("symbol_rule_quality", (symbol, rule, side, sc, ex)),
        ("symbol_quality_margin", (symbol, side, sc, ex, m)),
        ("symbol_quality", (symbol, side, sc, ex)),
    ]


def rescue_key(row: dict) -> tuple[str, tuple[str, ...]]:
    return (
        "rescue_shape",
        (
            str(row.get("symbol", "")),
            str(row.get("source_subset", "")),
            str(row.get("rule", "")),
            str(row.get("sources", "")),
        ),
    )


def median(values: list[float]) -> float:
    if not values:
        return math.nan
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def build_round_features(rows: Iterable[dict]) -> dict[str, dict]:
    by_round: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if row.get("status") == "ok":
            by_round[str(row.get("round_end_ts", ""))].append(row)
    out: dict[str, dict] = {}
    for round_end_ts, round_rows in by_round.items():
        margins = [v for v in (to_float(row.get("direction_margin_bps")) for row in round_rows) if v is not None]
        deltas = [v for v in (to_float(row.get("close_abs_delta_ms")) for row in round_rows) if v is not None]
        spreads = [v for v in (to_float(row.get("source_spread_bps")) for row in round_rows) if v is not None]
        out[round_end_ts] = {
            "round_ok_count": len(round_rows),
            "round_median_margin_bps": median(margins),
            "round_max_margin_bps": max(margins) if margins else math.nan,
            "round_median_abs_delta_ms": median(deltas),
            "round_max_abs_delta_ms": max(deltas) if deltas else math.nan,
            "round_max_source_spread_bps": max(spreads) if spreads else math.nan,
        }
    return out


def load_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        round_end_ts = to_int(row.get("round_end_ts"))
        if round_end_ts is None:
            continue
        row["round_end_ts"] = str(round_end_ts)
        out.append(row)
    return out


def build_index(rows: Iterable[dict], quantile: float) -> dict[tuple[str, tuple[str, ...]], BucketStats]:
    index: dict[tuple[str, tuple[str, ...]], BucketStats] = defaultdict(BucketStats)
    for row in rows:
        if row.get("status") != "ok":
            continue
        for level, key in key_levels(row):
            index[(level, key)].add(row)
    return {key: stats.finalize(quantile) for key, stats in index.items()}


def build_rescue_index(rows: Iterable[dict], quantile: float) -> dict[tuple[str, tuple[str, ...]], BucketStats]:
    index: dict[tuple[str, tuple[str, ...]], BucketStats] = defaultdict(BucketStats)
    for row in rows:
        if row.get("status") != "ok":
            continue
        index[rescue_key(row)].add(row)
    return {key: stats.finalize(quantile) for key, stats in index.items()}


def choose_stats(row: dict, index: dict[tuple[str, tuple[str, ...]], BucketStats], cfg: GateConfig) -> tuple[str | None, BucketStats | None]:
    for level, key in key_levels(row):
        stats = index.get((level, key))
        if stats is not None and stats.n >= cfg.min_samples:
            return level, stats
    return None, None


def choose_rescue_stats(
    row: dict,
    rescue_index: dict[tuple[str, tuple[str, ...]], BucketStats],
    cfg: GateConfig,
) -> tuple[str | None, BucketStats | None]:
    if cfg.rescue_min_samples <= 0 or cfg.rescue_max_train_max_bps <= 0:
        return None, None
    level, key = rescue_key(row)
    stats = rescue_index.get((level, key))
    if stats is None:
        return None, None
    if stats.n < cfg.rescue_min_samples:
        return None, None
    if stats.side_errors > 0:
        return None, None
    if stats.max_bps > cfg.rescue_max_train_max_bps + 1e-12:
        return None, None
    return level, stats


def level_keeps_delta(level: str | None) -> bool:
    return level in {
        "full",
        "no_spread",
        "symbol_rule_quality_full",
        "symbol_rule_quality_no_spread",
    }


def stale_pair_requires_delta_history(row: dict, level: str | None) -> bool:
    if level_keeps_delta(level):
        return False
    if str(row.get("symbol", "")) != "hype/usd":
        return False
    if str(row.get("source_subset", "")) != "drop_binance":
        return False
    if str(row.get("rule", "")) != "after_then_before":
        return False
    if str(row.get("sources", "")) != "bybit;hyperliquid":
        return False
    if source_count_bucket(row) != "n2" or exact_bucket(row) != "no_exact":
        return False
    delta_ms = to_float(row.get("close_abs_delta_ms"))
    return delta_ms is not None and delta_ms >= 2_500.0


def gate_row(
    row: dict,
    index: dict[tuple[str, tuple[str, ...]], BucketStats],
    rescue_index: dict[tuple[str, tuple[str, ...]], BucketStats],
    cfg: GateConfig,
    round_features: dict[str, dict],
) -> tuple[bool, dict]:
    if row.get("status") != "ok":
        return False, {
            "gate_status": row.get("status", "not_ok"),
            "gate_reason": row.get("filter_reason") or row.get("status", "not_ok"),
        }
    round_meta = round_features.get(str(row.get("round_end_ts", "")), {})
    round_max_margin = to_float(round_meta.get("round_max_margin_bps"))
    if (
        cfg.max_round_max_margin_bps > 0
        and round_max_margin is not None
        and round_max_margin > cfg.max_round_max_margin_bps + 1e-12
    ):
        meta = {
            "gate_status": "gated",
            "gate_reason": "round_max_margin_too_wide",
        }
        meta.update(round_meta)
        return False, meta
    margin = to_float(row.get("direction_margin_bps"))
    spread = to_float(row.get("source_spread_bps"))
    if margin is None:
        return False, {"gate_status": "gated", "gate_reason": "missing_margin"}
    if margin + 1e-12 < cfg.min_margin_bps:
        return False, {"gate_status": "gated", "gate_reason": "below_min_margin"}
    if cfg.max_source_spread_bps > 0 and spread is not None and spread > cfg.max_source_spread_bps + 1e-12:
        return False, {"gate_status": "gated", "gate_reason": "above_max_source_spread"}

    level, stats = choose_stats(row, index, cfg)
    if stats is None:
        return False, {"gate_status": "gated", "gate_reason": "insufficient_history"}
    if stale_pair_requires_delta_history(row, level):
        return False, {
            "gate_status": "gated",
            "gate_reason": "stale_pair_requires_delta_history",
            "gate_key_level": level,
            "gate_train_n": stats.n,
            "gate_train_side_errors": stats.side_errors,
            "gate_train_q_bps": stats.q,
            "gate_train_q95_bps": stats.q95,
            "gate_train_q99_bps": stats.q99,
            "gate_train_mean_bps": stats.mean_bps,
            "gate_train_max_bps": stats.max_bps,
            "gate_required_margin_bps": stats.q + cfg.safety_bps,
        }

    side_rate = stats.side_errors / stats.n if stats.n else 1.0
    if side_rate > cfg.max_side_rate + 1e-12:
        return False, {
            "gate_status": "gated",
            "gate_reason": "train_side_rate",
            "gate_key_level": level,
            "gate_train_n": stats.n,
            "gate_train_side_errors": stats.side_errors,
            "gate_train_q_bps": stats.q,
            "gate_train_q95_bps": stats.q95,
            "gate_train_q99_bps": stats.q99,
            "gate_train_mean_bps": stats.mean_bps,
            "gate_train_max_bps": stats.max_bps,
        }
    if stats.q > cfg.max_train_quantile_bps + 1e-12:
        return False, {
            "gate_status": "gated",
            "gate_reason": "train_quantile_too_wide",
            "gate_key_level": level,
            "gate_train_n": stats.n,
            "gate_train_side_errors": stats.side_errors,
            "gate_train_q_bps": stats.q,
            "gate_train_q95_bps": stats.q95,
            "gate_train_q99_bps": stats.q99,
            "gate_train_mean_bps": stats.mean_bps,
            "gate_train_max_bps": stats.max_bps,
        }
    if cfg.max_train_max_bps > 0 and stats.max_bps > cfg.max_train_max_bps + 1e-12:
        return False, {
            "gate_status": "gated",
            "gate_reason": "train_max_too_wide",
            "gate_key_level": level,
            "gate_train_n": stats.n,
            "gate_train_side_errors": stats.side_errors,
            "gate_train_q_bps": stats.q,
            "gate_train_q95_bps": stats.q95,
            "gate_train_q99_bps": stats.q99,
            "gate_train_mean_bps": stats.mean_bps,
            "gate_train_max_bps": stats.max_bps,
        }
    required_margin = stats.q + cfg.safety_bps
    if margin + 1e-12 < required_margin:
        rescue_level, rescue_stats = choose_rescue_stats(row, rescue_index, cfg)
        if rescue_stats is not None:
            return True, {
                "gate_status": "accepted",
                "gate_reason": "shape_history_max_rescue",
                "gate_key_level": rescue_level,
                "gate_train_n": rescue_stats.n,
                "gate_train_side_errors": rescue_stats.side_errors,
                "gate_train_q_bps": rescue_stats.q,
                "gate_train_q95_bps": rescue_stats.q95,
                "gate_train_q99_bps": rescue_stats.q99,
                "gate_train_mean_bps": rescue_stats.mean_bps,
                "gate_train_max_bps": rescue_stats.max_bps,
                "gate_required_margin_bps": rescue_stats.max_bps,
            }
        return False, {
            "gate_status": "gated",
            "gate_reason": "margin_below_trained_error",
            "gate_key_level": level,
            "gate_train_n": stats.n,
            "gate_train_side_errors": stats.side_errors,
            "gate_train_q_bps": stats.q,
            "gate_train_q95_bps": stats.q95,
            "gate_train_q99_bps": stats.q99,
            "gate_train_mean_bps": stats.mean_bps,
            "gate_train_max_bps": stats.max_bps,
            "gate_required_margin_bps": required_margin,
        }
    meta = {
        "gate_status": "accepted",
        "gate_reason": "",
        "gate_key_level": level,
        "gate_train_n": stats.n,
        "gate_train_side_errors": stats.side_errors,
        "gate_train_q_bps": stats.q,
        "gate_train_q95_bps": stats.q95,
        "gate_train_q99_bps": stats.q99,
        "gate_train_mean_bps": stats.mean_bps,
        "gate_train_max_bps": stats.max_bps,
        "gate_required_margin_bps": required_margin,
    }
    meta.update(round_meta)
    return True, meta


def make_windows(rows: list[dict], min_train_rounds: int, test_step_rounds: int, max_windows: int) -> list[tuple[int, int, list[dict], list[dict]]]:
    rounds = sorted({int(row["round_end_ts"]) for row in rows})
    windows = []
    for start_idx in range(min_train_rounds, len(rounds), test_step_rounds):
        test_rounds = set(rounds[start_idx : start_idx + test_step_rounds])
        if not test_rounds:
            continue
        train_max = rounds[start_idx - 1]
        test_max = max(test_rounds)
        train_rows = [row for row in rows if int(row["round_end_ts"]) <= train_max]
        test_rows = [row for row in rows if int(row["round_end_ts"]) in test_rounds]
        windows.append((train_max, test_max, train_rows, test_rows))
    if max_windows > 0:
        windows = windows[-max_windows:]
    return windows


def score_rows(gated_rows: list[dict]) -> dict:
    out = {
        "eval_rows": len(gated_rows),
        "input_ok": 0,
        "accepted": 0,
        "gated": 0,
        "filtered": 0,
        "missing": 0,
        "side_errors": 0,
        "mean_bps": None,
        "max_bps": 0.0,
        "p95_bps": None,
        "p99_bps": None,
        "accepted_coverage": 0.0,
    }
    errors = []
    for row in gated_rows:
        if row.get("status") == "ok":
            out["input_ok"] += 1
        if row.get("gate_status") == "accepted":
            out["accepted"] += 1
            err = to_float(row.get("close_diff_bps"))
            if err is not None:
                errors.append(err)
            out["side_errors"] += 1 if parse_bool(row.get("side_error")) else 0
        elif row.get("gate_status") == "filtered":
            out["filtered"] += 1
        elif row.get("gate_status") == "missing":
            out["missing"] += 1
        else:
            out["gated"] += 1
    if errors:
        errors.sort()
        out["mean_bps"] = sum(errors) / len(errors)
        out["max_bps"] = errors[-1]
        out["p95_bps"] = percentile(errors, 0.95)
        out["p99_bps"] = percentile(errors, 0.99)
    out["accepted_coverage"] = out["accepted"] / out["input_ok"] if out["input_ok"] else 0.0
    return out


def evaluate_config(rows: list[dict], cfg: GateConfig, min_train_rounds: int, test_step_rounds: int, max_windows: int) -> tuple[dict, list[dict]]:
    windows = make_windows(rows, min_train_rounds, test_step_rounds, max_windows)
    gated_rows: list[dict] = []
    for train_max, test_max, train_rows, test_rows in windows:
        index = build_index(train_rows, cfg.quantile)
        rescue_index = build_rescue_index(train_rows, cfg.quantile)
        round_features = build_round_features(test_rows)
        for row in test_rows:
            accepted, meta = gate_row(row, index, rescue_index, cfg, round_features)
            out = dict(row)
            out.update({k: value for k, value in meta.items()})
            out["gate_config"] = cfg.label()
            out["train_max_round_end_ts"] = train_max
            out["test_window_max_round_end_ts"] = test_max
            if accepted:
                out["gate_status"] = "accepted"
            gated_rows.append(out)
    summary = score_rows(gated_rows)
    summary.update(
        {
            "config": cfg.label(),
            "min_samples": cfg.min_samples,
            "min_margin_bps": cfg.min_margin_bps,
            "quantile": cfg.quantile,
            "max_train_quantile_bps": cfg.max_train_quantile_bps,
            "max_train_max_bps": cfg.max_train_max_bps,
            "safety_bps": cfg.safety_bps,
            "max_side_rate": cfg.max_side_rate,
            "max_source_spread_bps": cfg.max_source_spread_bps,
            "max_round_max_margin_bps": cfg.max_round_max_margin_bps,
            "rescue_min_samples": cfg.rescue_min_samples,
            "rescue_max_train_max_bps": cfg.rescue_max_train_max_bps,
            "windows": len(windows),
        }
    )
    return summary, gated_rows


def grid_configs(args: argparse.Namespace) -> list[GateConfig]:
    min_samples = parse_int_list(args.grid_min_samples)
    margins = parse_float_list(args.grid_min_margin_bps)
    quantiles = parse_float_list(args.grid_quantile)
    max_qs = parse_float_list(args.grid_max_train_quantile_bps)
    safeties = parse_float_list(args.grid_safety_bps)
    side_rates = parse_float_list(args.grid_max_side_rate)
    spreads = parse_float_list(args.grid_max_source_spread_bps)
    round_max_margins = parse_float_list(args.grid_max_round_max_margin_bps)
    configs = []
    for n in min_samples:
        for margin in margins:
            for q in quantiles:
                for max_q in max_qs:
                    for safety in safeties:
                        for side_rate in side_rates:
                            for spread in spreads:
                                for round_max_margin in round_max_margins:
                                    configs.append(
                                        GateConfig(
                                            n,
                                            margin,
                                            q,
                                            max_q,
                                            args.max_train_max_bps,
                                            safety,
                                            side_rate,
                                            spread,
                                            round_max_margin,
                                            args.rescue_min_samples,
                                            args.rescue_max_train_max_bps,
                                        )
                                    )
    return configs


def parse_float_list(raw: str) -> list[float]:
    return [float(item) for item in raw.split(",") if item.strip()]


def parse_int_list(raw: str) -> list[int]:
    return [int(item) for item in raw.split(",") if item.strip()]


def sort_key(summary: dict) -> tuple:
    max_bps = float(summary.get("max_bps") or 0.0)
    side = int(summary.get("side_errors") or 0)
    accepted = int(summary.get("accepted") or 0)
    coverage = float(summary.get("accepted_coverage") or 0.0)
    mean_bps = float(summary.get("mean_bps") or 0.0)
    return (
        side,
        max(0.0, max_bps - 5.0),
        -accepted,
        -coverage,
        max_bps,
        mean_bps,
    )


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base_fields = [
        "gate_config",
        "train_max_round_end_ts",
        "test_window_max_round_end_ts",
        "gate_status",
        "gate_reason",
        "gate_key_level",
        "gate_train_n",
        "gate_train_side_errors",
        "gate_train_q_bps",
        "gate_train_q95_bps",
        "gate_train_q99_bps",
        "gate_train_mean_bps",
        "gate_train_max_bps",
        "gate_required_margin_bps",
        "round_ok_count",
        "round_median_margin_bps",
        "round_max_margin_bps",
        "round_median_abs_delta_ms",
        "round_max_abs_delta_ms",
        "round_max_source_spread_bps",
    ]
    original_fields = []
    for row in rows:
        for key in row:
            if key not in base_fields and key not in original_fields:
                original_fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=base_fields + original_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def export_model(path: Path, rows: list[dict], cfg: GateConfig) -> None:
    index = build_index(rows, cfg.quantile)
    rescue_index = build_rescue_index(rows, cfg.quantile)
    buckets = []
    for (level, key), stats in sorted(index.items(), key=lambda item: (item[0][0], item[0][1])):
        if stats.n < cfg.min_samples:
            continue
        buckets.append(
            {
                "level": level,
                "key": list(key),
                "n": stats.n,
                "side_errors": stats.side_errors,
                "side_rate": stats.side_errors / stats.n if stats.n else 1.0,
                "q_bps": stats.q,
                "q95_bps": stats.q95,
                "q99_bps": stats.q99,
                "mean_bps": stats.mean_bps,
                "max_bps": stats.max_bps,
            }
        )
    rescue_buckets = []
    if cfg.rescue_min_samples > 0 and cfg.rescue_max_train_max_bps > 0:
        for (level, key), stats in sorted(rescue_index.items(), key=lambda item: (item[0][0], item[0][1])):
            if stats.n < cfg.rescue_min_samples:
                continue
            if stats.side_errors > 0:
                continue
            if stats.max_bps > cfg.rescue_max_train_max_bps + 1e-12:
                continue
            rescue_buckets.append(
                {
                    "level": level,
                    "key": list(key),
                    "n": stats.n,
                    "side_errors": stats.side_errors,
                    "side_rate": stats.side_errors / stats.n if stats.n else 1.0,
                    "q_bps": stats.q,
                    "q95_bps": stats.q95,
                    "q99_bps": stats.q99,
                    "mean_bps": stats.mean_bps,
                    "max_bps": stats.max_bps,
                }
            )
    model = {
        "model": "local_agg_uncertainty_gate_v1",
        "config": {
            "min_samples": cfg.min_samples,
            "min_margin_bps": cfg.min_margin_bps,
            "quantile": cfg.quantile,
            "max_train_quantile_bps": cfg.max_train_quantile_bps,
            "max_train_max_bps": cfg.max_train_max_bps,
            "safety_bps": cfg.safety_bps,
            "max_side_rate": cfg.max_side_rate,
            "max_source_spread_bps": cfg.max_source_spread_bps,
            "max_round_max_margin_bps": cfg.max_round_max_margin_bps,
            "rescue_min_samples": cfg.rescue_min_samples,
            "rescue_max_train_max_bps": cfg.rescue_max_train_max_bps,
        },
        "row_count": len(rows),
        "ok_training_rows": sum(1 for row in rows if row.get("status") == "ok"),
        "bucket_count": len(buckets),
        "rescue_bucket_count": len(rescue_buckets),
        "bucket_key_order": [
            "symbol",
            "source_subset",
            "rule",
            "sources",
            "pred_side",
            "source_count_bucket",
            "exact_bucket",
            "delta_bucket",
            "spread_bucket",
            "margin_bucket",
        ],
        "supported_bucket_levels": [
            "full",
            "no_delta",
            "no_spread",
            "shape_margin",
            "shape",
            "symbol_rule_quality_full",
            "symbol_rule_quality_no_delta",
            "symbol_rule_quality_no_spread",
            "symbol_rule_quality_margin",
            "symbol_rule_quality",
            "symbol_quality_margin",
            "symbol_quality",
        ],
        "rescue_bucket_key_order": [
            "symbol",
            "source_subset",
            "rule",
            "sources",
        ],
        "buckets": buckets,
        "rescue_buckets": rescue_buckets,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward uncertainty gate for local agg router eval rows.")
    ap.add_argument("--eval-csv", required=True, help="CSV produced by evaluate_local_agg_boundary_router.py")
    ap.add_argument("--out-csv", default="/tmp/local_agg_uncertainty_gate_eval.csv")
    ap.add_argument("--summary-json", default="/tmp/local_agg_uncertainty_gate_summary.json")
    ap.add_argument("--export-model-json", default="")
    ap.add_argument("--min-train-rounds", type=int, default=64)
    ap.add_argument("--test-step-rounds", type=int, default=16)
    ap.add_argument("--max-windows", type=int, default=0)
    ap.add_argument("--grid-search", action="store_true")
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--min-accepted", type=int, default=50)
    ap.add_argument("--min-samples", type=int, default=20)
    ap.add_argument("--min-margin-bps", type=float, default=1.0)
    ap.add_argument("--quantile", type=float, default=0.95)
    ap.add_argument("--max-train-quantile-bps", type=float, default=5.0)
    ap.add_argument("--max-train-max-bps", type=float, default=5.0, help="0 disables the historical bucket max-error gate")
    ap.add_argument("--safety-bps", type=float, default=0.75)
    ap.add_argument("--max-side-rate", type=float, default=0.0)
    ap.add_argument("--max-source-spread-bps", type=float, default=0.0, help="0 disables this global spread gate")
    ap.add_argument("--max-round-max-margin-bps", type=float, default=0.0, help="0 disables the round-level volatility gate")
    ap.add_argument("--rescue-min-samples", type=int, default=0, help="0 disables shape-history max-error rescue")
    ap.add_argument("--rescue-max-train-max-bps", type=float, default=0.0, help="Max train error for rescue buckets; 0 disables rescue")
    ap.add_argument("--grid-min-samples", default="5,10,20,30,50")
    ap.add_argument("--grid-min-margin-bps", default="0.5,1,2,3,5")
    ap.add_argument("--grid-quantile", default="0.9,0.95,0.99")
    ap.add_argument("--grid-max-train-quantile-bps", default="3,5,8,12")
    ap.add_argument("--grid-safety-bps", default="0,0.25,0.5,1")
    ap.add_argument("--grid-max-side-rate", default="0")
    ap.add_argument("--grid-max-source-spread-bps", default="0,2,5,8,12")
    ap.add_argument("--grid-max-round-max-margin-bps", default="0,21,34,55")
    args = ap.parse_args()

    rows = load_rows(Path(args.eval_csv))
    if not rows:
        raise SystemExit(f"no rows loaded from {args.eval_csv}")

    if args.grid_search:
        summaries = []
        best_rows: list[dict] = []
        best_summary: dict | None = None
        for cfg in grid_configs(args):
            summary, gated_rows = evaluate_config(rows, cfg, args.min_train_rounds, args.test_step_rounds, args.max_windows)
            if summary["accepted"] < args.min_accepted:
                continue
            summaries.append(summary)
            if best_summary is None or sort_key(summary) < sort_key(best_summary):
                best_summary = summary
                best_rows = gated_rows
        summaries.sort(key=sort_key)
        if best_summary is None:
            raise SystemExit("grid produced no config above --min-accepted")
        out_summary = {
            "mode": "grid_search",
            "eval_csv": args.eval_csv,
            "row_count": len(rows),
            "best": best_summary,
            "top": summaries[: args.top_n],
        }
        write_csv(Path(args.out_csv), best_rows)
        export_cfg = GateConfig(
            min_samples=int(best_summary["min_samples"]),
            min_margin_bps=float(best_summary["min_margin_bps"]),
            quantile=float(best_summary["quantile"]),
            max_train_quantile_bps=float(best_summary["max_train_quantile_bps"]),
            max_train_max_bps=float(best_summary["max_train_max_bps"]),
            safety_bps=float(best_summary["safety_bps"]),
            max_side_rate=float(best_summary["max_side_rate"]),
            max_source_spread_bps=float(best_summary["max_source_spread_bps"]),
            max_round_max_margin_bps=float(best_summary["max_round_max_margin_bps"]),
            rescue_min_samples=int(best_summary["rescue_min_samples"]),
            rescue_max_train_max_bps=float(best_summary["rescue_max_train_max_bps"]),
        )
    else:
        cfg = GateConfig(
            min_samples=args.min_samples,
            min_margin_bps=args.min_margin_bps,
            quantile=args.quantile,
            max_train_quantile_bps=args.max_train_quantile_bps,
            max_train_max_bps=args.max_train_max_bps,
            safety_bps=args.safety_bps,
            max_side_rate=args.max_side_rate,
            max_source_spread_bps=args.max_source_spread_bps,
            max_round_max_margin_bps=args.max_round_max_margin_bps,
            rescue_min_samples=args.rescue_min_samples,
            rescue_max_train_max_bps=args.rescue_max_train_max_bps,
        )
        summary, gated_rows = evaluate_config(rows, cfg, args.min_train_rounds, args.test_step_rounds, args.max_windows)
        out_summary = {
            "mode": "single",
            "eval_csv": args.eval_csv,
            "row_count": len(rows),
            "best": summary,
            "top": [summary],
        }
        write_csv(Path(args.out_csv), gated_rows)
        export_cfg = cfg

    summary_path = Path(args.summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(out_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.export_model_json:
        export_model(Path(args.export_model_json), rows, export_cfg)

    best = out_summary["best"]
    print(
        json.dumps(
            {
                "best_config": best["config"],
                "accepted": best["accepted"],
                "coverage": round(best["accepted_coverage"], 6),
                "side_errors": best["side_errors"],
                "mean_bps": None if best["mean_bps"] is None else round(best["mean_bps"], 6),
                "max_bps": round(best["max_bps"], 6),
                "p95_bps": None if best["p95_bps"] is None else round(best["p95_bps"], 6),
                "p99_bps": None if best["p99_bps"] is None else round(best["p99_bps"], 6),
                "summary_json": str(summary_path),
                "out_csv": args.out_csv,
                "model_json": args.export_model_json,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
