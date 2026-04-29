#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_WEIGHTS = {
    "binance": 0.5,
    "bybit": 0.5,
    "okx": 0.5,
    "coinbase": 1.0,
    "hyperliquid": 1.5,
}

CLOSE_ONLY_WEIGHTS = {
    "binance": 1.0,
    "bybit": 1.0,
    "okx": 1.0,
    "coinbase": 1.0,
    "hyperliquid": 0.5,
}

LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS = 2_500
LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS = 12.0
LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS = 900.0
LOCAL_PRICE_AGG_EXACT_BOOST = 1.25
LOCAL_PRICE_AGG_COMPARE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS = 5.5
LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS = 6.0
LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_SOURCES = 2
LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS = 3.0
LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS = 2.0
LOCAL_PRICE_AGG_COMPARE_SAFE_SINGLE_SOURCE_RELIEF_MIN_DIRECTION_MARGIN_BPS = 2.0
LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS = 5.0
LOCAL_PRICE_AGG_MIN_CONFIDENCE = 0.85
LOCAL_PRICE_AGG_RELIEF_MIN_CONFIDENCE = 0.80
LOCAL_PRICE_AGG_RELIEF_MAX_SOURCE_SPREAD_BPS = 2.0
LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_SOURCES = 2
LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MAX_SOURCE_SPREAD_BPS = 5.0
LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_DIRECTION_MARGIN_BPS = 3.0
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_SOURCES = 3
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS = 4.0
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS = 0.6
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE = 2.5
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS_TWO_SOURCE = 1.0
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_CONFIDENCE = 0.79
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_SOURCES = 2
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MAX_SOURCE_SPREAD_BPS = 5.1
LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_DIRECTION_MARGIN_BPS = 2.0

ROUTER_POLICY = {
    "bnb/usd": {
        "source_subset": "drop_okx",
        "rule": "after_then_before",
        "min_sources": 1,
        "weights": {
            "binance": 0.5,
            "bybit": 0.5,
            "coinbase": 1.0,
            "hyperliquid": 1.5,
        },
    },
    "btc/usd": {
        "source_subset": "only_coinbase",
        "rule": "after_then_before",
        "min_sources": 1,
        "weights": {"coinbase": 1.0},
    },
    "doge/usd": {
        "source_subset": "drop_binance",
        "rule": "last_before",
        "min_sources": 1,
        "weights": {
            "bybit": 0.5,
            "okx": 0.5,
            "coinbase": 1.0,
            "hyperliquid": 1.5,
        },
    },
    "eth/usd": {
        "source_subset": "only_coinbase",
        "rule": "last_before",
        "min_sources": 1,
        "weights": {"coinbase": 1.0},
    },
    "hype/usd": {
        "source_subset": "drop_binance",
        "rule": "after_then_before",
        "min_sources": 2,
        "weights": {
            "bybit": 1.0,
            "okx": 1.0,
            "coinbase": 1.0,
            "hyperliquid": 0.25,
        },
    },
    "sol/usd": {
        "source_subset": "only_okx_coinbase",
        "rule": "after_then_before",
        "min_sources": 2,
        "weights": {"okx": 0.5, "coinbase": 1.0},
    },
    "xrp/usd": {
        "source_subset": "only_binance_coinbase",
        "rule": "nearest_abs",
        "min_sources": 1,
        "weights": {"binance": 0.462086, "coinbase": 2.329335},
    },
}

ROUTER_SOURCE_FALLBACK_POLICY = {
    "bnb/usd": {
        "source_subset": "bnb_okx_fallback",
        "rule": "after_then_before",
        "min_sources": 1,
        "sources": {"okx"},
        "min_margin_bps": 7.0,
    },
    "sol/usd": {
        "source_subset": "sol_coinbase_fallback",
        "rule": "after_then_before",
        "min_sources": 1,
        "sources": {"coinbase"},
        "min_margin_bps": 5.0,
    },
}


def load_bias_cache(path: str) -> dict[tuple[str, str], float]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        rows = json.loads(p.read_text())
    except Exception:
        return {}
    biases = {}
    for row in rows if isinstance(rows, list) else []:
        symbol = str(row.get("symbol", "")).lower()
        source = str(row.get("source", "")).lower()
        try:
            bias_bps = float(row.get("bias_bps", 0.0))
        except Exception:
            continue
        if symbol and source and math.isfinite(bias_bps):
            biases[(symbol, source)] = bias_bps
    return biases


def allowed_sources(source_subset: str) -> set[str]:
    if source_subset == "full":
        return set(DEFAULT_WEIGHTS)
    if source_subset.startswith("drop_"):
        dropped = set(token for token in source_subset.removeprefix("drop_").split("_") if token)
        return set(DEFAULT_WEIGHTS) - dropped
    if source_subset.startswith("only_"):
        return set(token for token in source_subset.removeprefix("only_").split("_") if token)
    raise ValueError(f"unsupported source_subset={source_subset}")


def pick_price(
    src_points: Iterable[tuple[int, float]],
    rule: str,
    pre_ms: int,
    post_ms: int,
) -> tuple[int, float] | None:
    pts = [(off, price) for off, price in src_points if -pre_ms <= off <= post_ms]
    if not pts:
        return None
    if rule == "last_before":
        before = [(off, price) for off, price in pts if off <= 0]
        return max(before, key=lambda x: x[0]) if before else None
    if rule == "nearest_abs":
        return min(pts, key=lambda x: (abs(x[0]), x[0] > 0, x[0]))
    if rule == "after_then_before":
        after = [(off, price) for off, price in pts if off >= 0]
        if after:
            return min(after, key=lambda x: x[0])
        before = [(off, price) for off, price in pts if off <= 0]
        return max(before, key=lambda x: x[0]) if before else None
    raise ValueError(f"unsupported rule={rule}")


def pick_close_only_price(src_points: Iterable[tuple[int, float]], close_tol_ms: int) -> tuple[int, float, bool, int] | None:
    pts = [(off, price) for off, price in src_points if -close_tol_ms <= off <= close_tol_ms]
    if not pts:
        return None
    exact = [(off, price) for off, price in pts if off == 0]
    if exact:
        off, price = exact[-1]
        return off, price, True, 0
    after = [(off, price) for off, price in pts if off > 0]
    if after:
        off, price = min(after, key=lambda x: x[0])
        return off, price, False, abs(off)
    off, price = min(pts, key=lambda x: (abs(x[0]), x[0]))
    return off, price, False, abs(off)


def temporal_weight(abs_delta_ms: int) -> float:
    return 1.0 / (1.0 + (abs_delta_ms / LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS))


def apply_source_bias(symbol: str, source: str, price: float, biases: dict[tuple[str, str], float]) -> float:
    bias_bps = biases.get((symbol, source), 0.0)
    adjusted = price * (1.0 + bias_bps / 10_000.0)
    return adjusted if math.isfinite(adjusted) and adjusted > 0.0 else price


def close_only_hit(sample: dict, biases: dict[tuple[str, str], float]) -> dict | None:
    per_source = []
    for source in DEFAULT_WEIGHTS:
        picked = pick_close_only_price(
            (
                (off, price)
                for src, off, price in sample["close_points"]
                if src == source
            ),
            LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS,
        )
        if picked is None:
            continue
        off, raw_price, exact, abs_delta_ms = picked
        price = apply_source_bias(sample["symbol"], source, raw_price, biases)
        weight = (
            CLOSE_ONLY_WEIGHTS.get(source, 1.0)
            * temporal_weight(abs_delta_ms)
            * (LOCAL_PRICE_AGG_EXACT_BOOST if exact else 1.0)
        )
        if weight > 0:
            per_source.append((source, off, price, weight, exact))
    if not per_source:
        return None
    price_values = [price for _, _, price, _, _ in per_source]
    source_spread_bps = 0.0
    if len(price_values) >= 2:
        center = sum(price_values) / len(price_values)
        source_spread_bps = (max(price_values) - min(price_values)) / max(abs(center), 1e-12) * 10_000.0
    if source_spread_bps > LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS + 1e-9:
        return None
    weight_sum = sum(weight for _, _, _, weight, _ in per_source)
    if weight_sum <= 0:
        return None
    close_price = sum(price * weight for _, _, price, weight, _ in per_source) / weight_sum
    return {
        "close_price": close_price,
        "source_count": len(per_source),
        "source_spread_bps": source_spread_bps,
        "exact_sources": sum(1 for *_, exact in per_source if exact),
        "source_offsets": [off for _, off, _, _, _ in per_source],
        "source_prices": [(source, price) for source, _, price, _, _ in per_source],
    }


def close_only_filter_reason(symbol: str, hit: dict | None, rtds_open: float) -> tuple[str | None, float | None]:
    if hit is None:
        return None, None
    close_price = hit["close_price"]
    side_yes = close_price >= rtds_open
    source_agreement = 0.0
    if hit["source_prices"]:
        source_agreement = sum(1 for _, price in hit["source_prices"] if (price >= rtds_open) == side_yes) / len(hit["source_prices"])
    direction_margin_bps = abs(close_price - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    all_preclose = hit["exact_sources"] == 0 and all(off < 0 for off in hit["source_offsets"])
    preclose_relief = (
        all_preclose
        and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS
        and direction_margin_bps + 1e-9 < LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS
    )
    safe_preclose_relief = (
        all_preclose
        and hit["source_count"] >= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_SOURCES
        and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
        and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS
    )
    sol_safe_preclose_relief = (
        symbol == "sol/usd"
        and all_preclose
        and not (
            hit["source_count"] == 2
            and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE + 1e-9
            and direction_margin_bps + 1e-9 < 3.0
        )
        and (
            (
                hit["source_count"] >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_SOURCES
                and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
                and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS
            )
            or (
                hit["source_count"] >= 2
                and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE + 1e-9
                and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS_TWO_SOURCE
            )
        )
    )
    safe_single_source_relief = (
        hit["source_count"] == 1
        and hit["exact_sources"] == 0
        and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_SAFE_SINGLE_SOURCE_RELIEF_MIN_DIRECTION_MARGIN_BPS
    )
    if (
        all_preclose
        and not preclose_relief
        and not safe_preclose_relief
        and not sol_safe_preclose_relief
        and not safe_single_source_relief
        and direction_margin_bps + 1e-9 < LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS
    ):
        return "preclose_near_flat", direction_margin_bps
    single_source_min_margin = max(1.25, LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS)
    if (
        symbol == "hype/usd"
        and hit["source_count"] == 1
        and hit["exact_sources"] == 0
        and direction_margin_bps + 1e-9 >= 20.0
    ):
        return "hype_close_only_single_far_margin", direction_margin_bps
    if (
        hit["source_count"] == 1
        and hit["exact_sources"] == 0
        and not safe_single_source_relief
        and direction_margin_bps + 1e-9 < single_source_min_margin
    ):
        return "single_source_near_flat", direction_margin_bps
    strong_direction_relief = (
        hit["source_count"] >= 3
        and source_agreement >= 1.0 - 1e-9
        and hit["source_spread_bps"] <= 5.0 + 1e-9
        and direction_margin_bps >= 10.0 - 1e-9
    )
    confidence_relief = (
        hit["source_count"] >= 2
        and source_agreement >= 1.0 - 1e-9
        and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
        and direction_margin_bps >= LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS - 1e-9
    )
    safe_low_conf_relief = (
        hit["source_count"] >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_SOURCES
        and source_agreement >= 1.0 - 1e-9
        and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
        and direction_margin_bps >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_DIRECTION_MARGIN_BPS - 1e-9
    )
    sol_safe_low_conf_relief = (
        symbol == "sol/usd"
        and hit["source_count"] >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_SOURCES
        and source_agreement >= 1.0 - 1e-9
        and hit["source_spread_bps"] <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MAX_SOURCE_SPREAD_BPS + 1e-9
        and direction_margin_bps + 1e-9 >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_DIRECTION_MARGIN_BPS
    )
    if hit["source_count"] == 1:
        confidence = 0.93 if hit["exact_sources"] > 0 else 0.87
    else:
        source_factor = min(hit["source_count"], 3) / 3.0
        spread_factor = max(0.0, min(1.0, 1.0 - (hit["source_spread_bps"] / LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS)))
        exact_factor = min(hit["exact_sources"], 2) / 2.0
        confidence = max(0.0, min(1.0, 0.50 * source_agreement + 0.20 * source_factor + 0.20 * spread_factor + 0.10 * exact_factor))
    effective_min_confidence = (
        min(
            LOCAL_PRICE_AGG_MIN_CONFIDENCE,
            LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_CONFIDENCE if sol_safe_low_conf_relief else LOCAL_PRICE_AGG_RELIEF_MIN_CONFIDENCE,
        )
        if confidence_relief or strong_direction_relief or safe_low_conf_relief or sol_safe_low_conf_relief
        else LOCAL_PRICE_AGG_MIN_CONFIDENCE
    )
    if confidence + 1e-9 < effective_min_confidence:
        return "low_confidence", direction_margin_bps
    return None, direction_margin_bps


def source_fallback_hit(sample: dict) -> dict | None:
    fallback = ROUTER_SOURCE_FALLBACK_POLICY.get(sample["symbol"])
    if fallback is None:
        return None
    per_source = []
    for source in fallback["sources"]:
        picked = pick_price(
            (
                (off, price)
                for src, off, price in sample["close_points"]
                if src == source
            ),
            fallback["rule"],
            5000,
            500,
        )
        if picked is not None:
            per_source.append((source, picked[0], picked[1]))
    if len(per_source) < fallback["min_sources"]:
        return None
    pred_close = sum(price for _, _, price in per_source) / len(per_source)
    rtds_open = sample["rtds_open"]
    direction_margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    exact_sources = sum(1 for _, off, _ in per_source if off == 0)
    if exact_sources != 0 or direction_margin_bps + 1e-9 < fallback["min_margin_bps"]:
        return None
    prices = [price for _, _, price in per_source]
    spread_bps = 0.0
    if len(prices) >= 2:
        spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0
    return {
        "source_subset": fallback["source_subset"],
        "rule": fallback["rule"],
        "min_sources": fallback["min_sources"],
        "close_price": pred_close,
        "source_count": len(per_source),
        "source_spread_bps": spread_bps,
        "exact_sources": exact_sources,
        "sources": ";".join(source for source, _, _ in sorted(per_source)),
    }


def instance_samples(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    by_instance = defaultdict(list)
    for row in rows:
        by_instance[(row["symbol"], int(row["round_end_ts"]), row["instance_id"])].append(row)

    samples = []
    for (symbol, round_end_ts, instance_id), rs in by_instance.items():
        first = rs[0]
        samples.append(
            {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "instance_id": instance_id,
                "log_file": first.get("log_file", ""),
                "rtds_open": float(first["rtds_open"]),
                "rtds_close": float(first["rtds_close"]),
                "close_points": [
                    (row["source"], int(row["offset_ms"]), float(row["price"]))
                    for row in rs
                    if row["phase"] == "close"
                ],
            }
        )
    return samples


def select_samples(samples: list[dict], sample_mode: str, instance_id: str) -> list[dict]:
    if instance_id:
        return [sample for sample in samples if sample["instance_id"] == instance_id]
    if sample_mode == "all":
        return samples

    selected = {}
    for sample in samples:
        key = (sample["symbol"], sample["round_end_ts"])
        if sample_mode == "latest":
            candidate = (sample["instance_id"], len(sample["close_points"]), sample)
            current = selected.get(key)
            if current is None or candidate[:2] > current[:2]:
                selected[key] = candidate
            continue

        close_count = len(sample["close_points"])
        candidate = (close_count, sample["instance_id"], sample)
        current = selected.get(key)
        if current is None or candidate[:2] > current[:2]:
            selected[key] = candidate
    return [candidate[-1] for candidate in selected.values()]


def router_filter_reason(symbol: str, rule: str, source_count: int, exact_sources: int,
                         spread_bps: float, margin_bps: float, side_yes: bool,
                         close_only_reason: str | None = None,
                         close_only_margin_bps: float | None = None) -> str | None:
    if symbol == "bnb/usd":
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps < 1.0
        ):
            return "bnb_single_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps >= 2.0
            and margin_bps < 1.5
        ):
            return "bnb_high_spread_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps <= 0.8
            and margin_bps < 2.2
        ):
            return "bnb_tight_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and margin_bps < 1.0
        ):
            return "bnb_yes_near_flat"
        if close_only_reason == "preclose_near_flat" and source_count == 1 and exact_sources == 0:
            return "bnb_close_only_preclose_near_flat"
    elif symbol == "btc/usd":
        if rule == "after_then_before" and source_count == 1 and exact_sources == 0 and margin_bps < 0.25:
            return "btc_single_near_flat"
        if rule == "after_then_before" and source_count == 2 and exact_sources == 0 and margin_bps < 1.0:
            return "btc_two_source_near_flat"
    elif symbol == "xrp/usd":
        if rule == "nearest_abs" and source_count >= 2 and exact_sources == 0 and margin_bps < 0.45:
            return "xrp_nearest_near_flat"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 2.0
            and margin_bps < 2.0
        ):
            return "xrp_nearest_wide_spread_near_flat"
        if rule == "nearest_abs" and source_count == 1 and exact_sources == 0 and margin_bps < 0.5:
            return "xrp_single_nearest_near_flat"
        if rule == "last_before" and exact_sources == 0 and side_yes and margin_bps < 1.5:
            return "xrp_last_yes_near_flat"
    elif symbol == "doge/usd":
        if rule == "last_before" and source_count == 1 and exact_sources == 0 and margin_bps < 1.5:
            return "doge_single_last_near_flat"
        if rule == "last_before" and source_count >= 2 and exact_sources == 0 and margin_bps < 1.5:
            return "doge_last_multi_near_flat"
        if rule == "last_before" and source_count >= 3 and exact_sources == 0 and spread_bps >= 8.0:
            return "doge_last_high_spread"
        if rule == "nearest_abs" and source_count >= 2 and exact_sources == 0 and margin_bps < 0.5:
            return "doge_nearest_near_flat"
    elif symbol == "hype/usd":
        if rule == "after_then_before" and source_count >= 3 and exact_sources == 0 and 2.0 <= spread_bps <= 4.0 and margin_bps >= 40.0:
            return "hype_three_source_stale_spread_fallback"
        if rule == "after_then_before" and source_count == 1 and exact_sources == 0 and margin_bps < 3.0:
            return "hype_single_after_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.0
            and margin_bps < 2.0
        ):
            return "hype_two_after_tight_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps >= 2.0
            and margin_bps < 1.5
        ):
            return "hype_two_after_wide_spread_near_flat"
        if rule == "nearest_abs" and exact_sources == 0:
            if source_count >= 2 and spread_bps <= 1.0 and margin_bps < 1.8:
                return "hype_nearest_near_flat"
            if source_count == 1 and margin_bps < 2.0:
                return "hype_single_nearest_near_flat"
    elif symbol == "eth/usd":
        if rule == "last_before" and source_count == 1 and exact_sources == 0 and margin_bps < 1.5:
            return "eth_single_last_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.5
            and margin_bps < 0.35
        ):
            return "eth_two_last_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.1
            and margin_bps < 0.2
        ):
            return "eth_two_last_tight_near_flat"
    elif symbol == "sol/usd":
        if rule == "after_then_before" and source_count == 2 and exact_sources == 0 and margin_bps < 1.0:
            return "sol_after_near_flat"
        if (
            close_only_reason == "preclose_near_flat"
            and close_only_margin_bps is not None
            and close_only_margin_bps < 0.2
            and source_count == 2
            and spread_bps <= 0.1
        ):
            return "sol_close_only_preclose_near_flat"
    return None


def evaluate_sample(sample: dict, pre_ms: int, post_ms: int, biases: dict[tuple[str, str], float]) -> dict:
    symbol = sample["symbol"]
    policy = ROUTER_POLICY[symbol]
    fallback_hit = close_only_hit(sample, biases)
    fallback_filter_reason, fallback_margin_bps = close_only_filter_reason(symbol, fallback_hit, sample["rtds_open"])
    per_source = []
    for source in allowed_sources(policy["source_subset"]):
        picked = pick_price(
            (
                (off, price)
                for src, off, price in sample["close_points"]
                if src == source
            ),
            policy["rule"],
            pre_ms,
            post_ms,
        )
        if picked is None:
            continue
        weight = policy["weights"].get(source, 0.0)
        if weight > 0.0:
            per_source.append((source, picked[0], picked[1], weight))

    rtds_open = sample["rtds_open"]
    rtds_close = sample["rtds_close"]
    if len(per_source) < policy["min_sources"]:
        fallback_source_hit = source_fallback_hit(sample)
        if fallback_source_hit is not None:
            pred_close = fallback_source_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            return {
                "status": "ok",
                "filter_reason": "",
                "symbol": symbol,
                "round_end_ts": sample["round_end_ts"],
                "instance_id": sample["instance_id"],
                "log_file": sample.get("log_file", ""),
                "source_subset": fallback_source_hit["source_subset"],
                "rule": fallback_source_hit["rule"],
                "min_sources": fallback_source_hit["min_sources"],
                "pred_close": pred_close,
                "rtds_open": rtds_open,
                "rtds_close": rtds_close,
                "pred_yes": side_yes,
                "truth_yes": truth_yes,
                "side_error": side_yes != truth_yes,
                "close_diff_bps": close_diff_bps,
                "direction_margin_bps": margin_bps,
                "source_count": fallback_source_hit["source_count"],
                "source_spread_bps": fallback_source_hit["source_spread_bps"],
                "exact_sources": fallback_source_hit["exact_sources"],
                "sources": fallback_source_hit["sources"],
            }
        if symbol == "hype/usd" and fallback_hit is not None and fallback_filter_reason is None:
            pred_close = fallback_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            return {
                "status": "ok",
                "filter_reason": "",
                "symbol": symbol,
                "round_end_ts": sample["round_end_ts"],
                "instance_id": sample["instance_id"],
                "log_file": sample.get("log_file", ""),
                "source_subset": "close_only_fallback",
                "rule": "close_only",
                "min_sources": 1,
                "pred_close": pred_close,
                "rtds_open": rtds_open,
                "rtds_close": rtds_close,
                "pred_yes": side_yes,
                "truth_yes": truth_yes,
                "side_error": side_yes != truth_yes,
                "close_diff_bps": close_diff_bps,
                "direction_margin_bps": margin_bps,
                "source_count": fallback_hit["source_count"],
                "source_spread_bps": fallback_hit["source_spread_bps"],
                "exact_sources": fallback_hit["exact_sources"],
                "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
            }
        return {
            "status": "missing",
            "symbol": symbol,
            "round_end_ts": sample["round_end_ts"],
            "source_count": len(per_source),
        }

    pred_close = sum(weight * price for _, _, price, weight in per_source) / sum(
        weight for _, _, _, weight in per_source
    )
    side_yes = pred_close >= rtds_open
    truth_yes = rtds_close >= rtds_open
    exact_sources = sum(1 for _, off, _, _ in per_source if off == 0)
    spread_bps = 0.0
    if len(per_source) >= 2:
        prices = [price for _, _, price, _ in per_source]
        spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0
    margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
    reason = router_filter_reason(
        symbol,
        policy["rule"],
        len(per_source),
        exact_sources,
        spread_bps,
        margin_bps,
        side_yes,
        fallback_filter_reason,
        fallback_margin_bps,
    )
    if reason and symbol == "hype/usd" and fallback_hit is not None and fallback_filter_reason is None:
        pred_close = fallback_hit["close_price"]
        side_yes = pred_close >= rtds_open
        truth_yes = rtds_close >= rtds_open
        close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
        margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
        return {
            "status": "ok",
            "filter_reason": "",
            "symbol": symbol,
            "round_end_ts": sample["round_end_ts"],
            "instance_id": sample["instance_id"],
            "log_file": sample.get("log_file", ""),
            "source_subset": "close_only_fallback",
            "rule": "close_only",
            "min_sources": 1,
            "pred_close": pred_close,
            "rtds_open": rtds_open,
            "rtds_close": rtds_close,
            "pred_yes": side_yes,
            "truth_yes": truth_yes,
            "side_error": side_yes != truth_yes,
            "close_diff_bps": close_diff_bps,
            "direction_margin_bps": margin_bps,
            "source_count": fallback_hit["source_count"],
            "source_spread_bps": fallback_hit["source_spread_bps"],
            "exact_sources": fallback_hit["exact_sources"],
            "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
        }
    return {
        "status": "filtered" if reason else "ok",
        "filter_reason": reason or "",
        "symbol": symbol,
        "round_end_ts": sample["round_end_ts"],
        "instance_id": sample["instance_id"],
        "log_file": sample.get("log_file", ""),
        "source_subset": policy["source_subset"],
        "rule": policy["rule"],
        "min_sources": policy["min_sources"],
        "pred_close": pred_close,
        "rtds_open": rtds_open,
        "rtds_close": rtds_close,
        "pred_yes": side_yes,
        "truth_yes": truth_yes,
        "side_error": side_yes != truth_yes,
        "close_diff_bps": close_diff_bps,
        "direction_margin_bps": margin_bps,
        "source_count": len(per_source),
        "source_spread_bps": spread_bps,
        "exact_sources": exact_sources,
        "sources": ";".join(source for source, _, _, _ in sorted(per_source)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the Rust boundary_symbol_router_v1 policy.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_router_eval.csv")
    parser.add_argument("--test-rounds", type=int, default=16)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument(
        "--bias-cache",
        default="logs/local-agg-boundary-challenger-lab/local_price_agg_bias_cache.instance.json",
        help="optional local close-only source bias cache used to mirror runtime fallback decisions",
    )
    parser.add_argument(
        "--sample-mode",
        choices=("best", "latest", "all"),
        default="best",
        help="best=max close ticks per symbol/round; latest=max instance id; all=every instance.",
    )
    parser.add_argument("--instance-id", default="", help="Evaluate only one instance id, e.g. 20260429_180155.")
    args = parser.parse_args()

    samples = [
        sample
        for sample in select_samples(
            instance_samples(Path(args.boundary_csv)),
            args.sample_mode,
            args.instance_id,
        )
        if sample["symbol"] in ROUTER_POLICY
    ]
    biases = load_bias_cache(args.bias_cache)
    rounds = sorted({sample["round_end_ts"] for sample in samples})
    test_rounds = set(rounds[-args.test_rounds:]) if args.test_rounds > 0 else set()

    rows = []
    summary = defaultdict(lambda: {"ok": 0, "side": 0, "filtered": 0, "missing": 0, "bps_sum": 0.0, "bps_max": 0.0})
    reasons = Counter()
    for sample in samples:
        row = evaluate_sample(sample, args.pre_ms, args.post_ms, biases)
        split = "test" if sample["round_end_ts"] in test_rounds else "train"
        row["split"] = split
        rows.append(row)
        if row["status"] == "filtered":
            reasons[(row["symbol"], row["filter_reason"])] += 1
        for key in ((row["symbol"], split), ("ALL", split)):
            bucket = summary[key]
            if row["status"] == "missing":
                bucket["missing"] += 1
            elif row["status"] == "filtered":
                bucket["filtered"] += 1
            else:
                bucket["ok"] += 1
                bucket["side"] += int(row["side_error"])
                bucket["bps_sum"] += row["close_diff_bps"]
                bucket["bps_max"] = max(bucket["bps_max"], row["close_diff_bps"])

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "split",
        "instance_id",
        "log_file",
        "symbol",
        "round_end_ts",
        "status",
        "filter_reason",
        "source_subset",
        "rule",
        "min_sources",
        "source_count",
        "sources",
        "exact_sources",
        "source_spread_bps",
        "direction_margin_bps",
        "close_diff_bps",
        "pred_close",
        "rtds_open",
        "rtds_close",
        "pred_yes",
        "truth_yes",
        "side_error",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    for key in sorted(summary):
        bucket = summary[key]
        mean_bps = bucket["bps_sum"] / bucket["ok"] if bucket["ok"] else None
        print(
            key,
            {
                "ok": bucket["ok"],
                "side": bucket["side"],
                "filtered": bucket["filtered"],
                "missing": bucket["missing"],
                "mean_bps": None if mean_bps is None else round(mean_bps, 6),
                "max_bps": round(bucket["bps_max"], 6),
            },
        )
    print("filter_reasons", dict(sorted(reasons.items())))
    print({"rows": len(rows), "out_csv": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
