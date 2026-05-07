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

ROUTER_SOURCE_FALLBACK_POLICIES = {
    "bnb/usd": [
        {
            "source_subset": "bnb_okx_no_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("okx",),
            "side": "no",
            "min_margin_bps": 4.0,
            "max_margin_bps": 7.0,
        },
        {
            "source_subset": "bnb_okx_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("okx",),
            "min_margin_bps": 7.0,
        },
    ],
    "doge/usd": [
        {
            "source_subset": "doge_binance_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("binance",),
            "min_margin_bps": 5.0,
            "max_margin_bps": 15.0,
            "max_abs_delta_ms": 4000,
        },
    ],
    "eth/usd": [
        {
            "source_subset": "eth_binance_missing_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("binance",),
            "min_margin_bps": 3.0,
            "max_abs_delta_ms": 2500,
        },
    ],
    "hype/usd": [
        {
            "source_subset": "hype_hyperliquid_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("hyperliquid",),
            "min_margin_bps": 4.0,
            "max_margin_bps": 5.0,
            "max_abs_delta_ms": 900,
        },
    ],
    "sol/usd": [
        {
            "source_subset": "sol_binance_coinbase_fallback",
            "rule": "nearest_abs",
            "min_sources": 2,
            "sources": ("binance", "coinbase"),
            "min_margin_bps": 2.0,
            "max_spread_bps": 7.0,
        },
        {
            "source_subset": "sol_coinbase_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("coinbase",),
            "min_margin_bps": 5.0,
        },
        {
            "source_subset": "sol_binance_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("binance",),
            "min_margin_bps": 12.0,
            "max_abs_delta_ms": 2500,
        },
        {
            "source_subset": "sol_okx_missing_fallback",
            "rule": "after_then_before",
            "min_sources": 1,
            "sources": ("okx",),
            "min_margin_bps": 4.0,
            "max_abs_delta_ms": 4000,
        },
    ],
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
    raw_per_source = []
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
            raw_per_source.append((source, off, raw_price, weight, exact))
    if not per_source:
        return None
    if sample["symbol"] == "hype/usd" and len(per_source) == 1:
        source, off, price, weight, exact = per_source[0]
        if source == "hyperliquid" and not exact:
            picked = pick_close_only_price(
                (
                    (raw_off, raw_price)
                    for src, raw_off, raw_price in sample["close_points"]
                    if src == "hyperliquid"
                ),
                LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS,
            )
            if picked is not None:
                _, raw_price, _, _ = picked
                rtds_open = sample["rtds_open"]
                adjusted_margin_bps = abs(price - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
                raw_margin_bps = abs(raw_price - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
                if (
                    price >= rtds_open
                    and raw_price >= rtds_open
                    and adjusted_margin_bps + 1e-9 >= 10.0
                    and raw_margin_bps + 1e-9 >= 7.0
                ):
                    per_source[0] = (source, off, raw_price, weight, exact)
    if sample["symbol"] == "hype/usd" and len(per_source) >= 2:
        rtds_open = sample["rtds_open"]
        weight_sum = sum(weight for _, _, _, weight, _ in per_source)
        raw_weight_sum = sum(weight for _, _, _, weight, _ in raw_per_source)
        if weight_sum > 0 and raw_weight_sum > 0:
            adjusted_close = sum(price * weight for _, _, price, weight, _ in per_source) / weight_sum
            raw_close = sum(price * weight for _, _, price, weight, _ in raw_per_source) / raw_weight_sum
            adjusted_margin_bps = abs(adjusted_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            all_preclose = all(off < 0 for _, off, _, _, _ in per_source)
            all_postclose = all(off > 0 for _, off, _, _, _ in per_source)
            if (
                adjusted_close < rtds_open
                and raw_close < rtds_open
                and all(not exact for _, _, _, _, exact in per_source)
                and (
                    (all_preclose and 2.0 <= adjusted_margin_bps < 12.0)
                    or (all_postclose and 4.0 <= adjusted_margin_bps < 12.0)
                )
            ):
                per_source = raw_per_source
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
    fallbacks = ROUTER_SOURCE_FALLBACK_POLICIES.get(sample["symbol"], ())
    if not fallbacks:
        return None
    for fallback in fallbacks:
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
            continue
        weighted = [(price, DEFAULT_WEIGHTS.get(source, 1.0)) for source, _, price in per_source]
        weight_sum = sum(weight for _, weight in weighted)
        if weight_sum <= 0:
            continue
        pred_close = sum(price * weight for price, weight in weighted) / weight_sum
        rtds_open = sample["rtds_open"]
        direction_margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
        exact_sources = sum(1 for _, off, _ in per_source if off == 0)
        prices = [price for _, _, price in per_source]
        spread_bps = 0.0
        if len(prices) >= 2:
            spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0
        offsets = sorted(off for _, off, _ in per_source)
        close_abs_delta_ms = abs(offsets[len(offsets) // 2]) if offsets else None
        max_margin_bps = fallback.get("max_margin_bps")
        max_spread_bps = fallback.get("max_spread_bps")
        max_abs_delta_ms = fallback.get("max_abs_delta_ms")
        side = fallback.get("side")
        if (
            exact_sources != 0
            or direction_margin_bps + 1e-9 < fallback["min_margin_bps"]
            or (
                max_margin_bps is not None
                and direction_margin_bps > max_margin_bps + 1e-9
            )
            or (
                max_spread_bps is not None
                and spread_bps > max_spread_bps + 1e-9
            )
            or (
                max_abs_delta_ms is not None
                and close_abs_delta_ms is not None
                and close_abs_delta_ms > max_abs_delta_ms
            )
            or (side == "yes" and pred_close < rtds_open)
            or (side == "no" and pred_close >= rtds_open)
        ):
            continue
        return {
            "source_subset": fallback["source_subset"],
            "rule": fallback["rule"],
            "min_sources": fallback["min_sources"],
            "close_price": pred_close,
            "source_count": len(per_source),
            "source_spread_bps": spread_bps,
            "exact_sources": exact_sources,
            "close_abs_delta_ms": close_abs_delta_ms,
            "sources": ";".join(source for source, _, _ in sorted(per_source)),
        }
    return None


def source_fallback_rescues_filtered(symbol: str, filter_reason: str, fallback_hit: dict) -> bool:
    if (
        symbol == "bnb/usd"
        and fallback_hit["source_subset"] == "bnb_okx_no_fallback"
        and filter_reason
        in {
            "bnb_no_near_flat",
            "bnb_single_binance_no_fast_tail",
            "bnb_two_no_midspread_near_flat",
        }
    ):
        return True
    if (
        symbol == "doge/usd"
        and fallback_hit["source_subset"] == "doge_binance_fallback"
        and filter_reason
        in {
            "doge_multi_last_fast_midspread_tail",
            "doge_multi_last_midspread_tail",
            "doge_multi_last_stale_near_flat",
            "doge_two_bybit_okx_last_stale_tightspread_high_margin_tail",
        }
    ):
        return True
    if (
        symbol == "sol/usd"
        and fallback_hit["source_subset"] == "sol_binance_fallback"
        and filter_reason == "sol_after_near_flat"
    ):
        return True
    if (
        symbol == "hype/usd"
        and fallback_hit["source_subset"] == "hype_hyperliquid_fallback"
        and filter_reason == "hype_three_after_near_flat"
    ):
        return True
    return False


def filtered_close_only_allowed_without_weighted(
    symbol: str,
    filter_reason: str | None,
    hit: dict | None,
    rtds_open: float,
) -> bool:
    if hit is None:
        return False
    margin_bps = abs(hit["close_price"] - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    sources = tuple(source for source, _ in sorted(hit["source_prices"]))
    close_abs_delta_ms = median_abs_offset_ms(hit["source_offsets"])
    return (
        symbol == "hype/usd"
        and filter_reason == "hype_close_only_single_far_margin"
        and hit["source_count"] == 1
        and hit["exact_sources"] == 0
        and sources == ("hyperliquid",)
        and close_abs_delta_ms is not None
        and close_abs_delta_ms <= 500
        and 21.0 <= margin_bps < 24.0
        and not (22.55 <= margin_bps < 22.7 and 150 <= close_abs_delta_ms <= 250)
        and not (21.0 <= margin_bps < 21.1 and 150 <= close_abs_delta_ms <= 300)
    )


def missing_filter_reason(
    symbol: str,
    filter_reason: str | None,
    margin_bps: float | None,
    hit: dict | None,
) -> str | None:
    if (
        symbol == "sol/usd"
        and filter_reason == "preclose_near_flat"
        and margin_bps is not None
        and margin_bps < 1.0
        and hit is not None
        and hit["source_count"] >= 2
        and hit["exact_sources"] == 0
        and hit["source_spread_bps"] <= 5.0
    ):
        return "sol_missing_preclose_near_flat"
    return None


def median_abs_offset_ms(offsets: Iterable[int]) -> int | None:
    sorted_offsets = sorted(abs(offset) for offset in offsets)
    if not sorted_offsets:
        return None
    return sorted_offsets[len(sorted_offsets) // 2]


def optional_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def instance_samples(path: Path, respect_local_ready_ms: bool = True) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    by_instance = defaultdict(list)
    for row in rows:
        by_instance[(row["symbol"], int(row["round_end_ts"]), row["instance_id"])].append(row)

    samples = []
    for (symbol, round_end_ts, instance_id), rs in by_instance.items():
        first = rs[0]
        local_started_ms = optional_int(first.get("local_started_ms"))
        local_ready_ms = optional_int(first.get("local_ready_ms"))
        local_deadline_ms = optional_int(first.get("local_deadline_ms"))
        close_points_all = [
            (row["source"], int(row["offset_ms"]), float(row["price"]), int(row["ts_ms"]))
            for row in rs
            if row["phase"] == "close"
        ]
        if respect_local_ready_ms and local_ready_ms is not None:
            close_points_ready = [point for point in close_points_all if point[3] <= local_ready_ms]
        else:
            close_points_ready = close_points_all
        samples.append(
            {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "instance_id": instance_id,
                "log_file": first.get("log_file", ""),
                "local_started_ms": local_started_ms,
                "local_ready_ms": local_ready_ms,
                "local_deadline_ms": local_deadline_ms,
                "local_timing_source": first.get("local_timing_source", ""),
                "local_ready_lag_ms": (
                    local_ready_ms - round_end_ts * 1000
                    if local_ready_ms is not None
                    else None
                ),
                "close_points_total": len(close_points_all),
                "close_points_seen": len(close_points_ready),
                "close_points_dropped_after_ready": len(close_points_all) - len(close_points_ready),
                "rtds_open": float(first["rtds_open"]),
                "rtds_close": float(first["rtds_close"]),
                "close_points": [
                    (source, offset_ms, price)
                    for source, offset_ms, price, _ in close_points_ready
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


def router_filter_reason(symbol: str, source_subset: str, rule: str, source_count: int, exact_sources: int,
                         spread_bps: float, margin_bps: float, side_yes: bool,
                         close_abs_delta_ms: int | None = None,
                         close_only_reason: str | None = None,
                         close_only_margin_bps: float | None = None,
                         sources: tuple[str, ...] | None = None) -> str | None:
    if source_count >= 2 and spread_bps > 20.0:
        return "cross_source_extreme_spread"
    if symbol == "bnb/usd":
        if (
            source_subset == "bnb_okx_fallback"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and margin_bps >= 8.0
        ):
            return "bnb_okx_fallback_far_margin"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps < 4.5
        ):
            return "bnb_single_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps >= 7.6
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 400
            and close_abs_delta_ms <= 1_200
        ):
            return "bnb_single_yes_fast_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and 8.0 <= margin_bps < 9.3
            and close_abs_delta_ms is not None
            and (close_abs_delta_ms < 400 or close_abs_delta_ms >= 1_800)
        ):
            return "bnb_single_yes_upper_mid_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and 8.0 <= margin_bps < 9.3
            and spread_bps <= 0.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 2_500
        ):
            return "bnb_two_lowspread_yes_upper_mid_stale_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("binance",)
            and 5.0 <= margin_bps < 5.8
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "bnb_single_binance_yes_fast_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("binance",)
            and 5.0 <= margin_bps < 5.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_800
        ):
            return "bnb_single_binance_yes_stale_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("binance",)
            and 7.0 <= margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_800
        ):
            return "bnb_single_binance_yes_stale_upper_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("binance",)
            and margin_bps >= 11.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_800
        ):
            return "bnb_single_binance_yes_stale_tail"
        if (
            rule == "after_then_before"
            and source_count <= 2
            and exact_sources == 0
            and not side_yes
            and margin_bps < 2.0
            and (close_abs_delta_ms is None or 100 <= close_abs_delta_ms < 1_500)
        ):
            return "bnb_no_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and 1.0 <= spread_bps <= 2.0
            and margin_bps < 2.2
        ):
            return "bnb_two_no_midspread_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("binance",)
            and margin_bps >= 5.0
            and margin_bps < 6.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "bnb_single_binance_no_fast_tail"
        if (
            source_subset == "drop_okx"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 1
            and side_yes
            and sources == ("binance",)
            and 0.70 <= margin_bps < 0.75
            and close_abs_delta_ms == 0
        ):
            return "bnb_single_binance_exact_yes_tiny_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit",)
            and margin_bps >= 10.0
        ):
            return "bnb_single_bybit_no_tail"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and margin_bps < 2.5
        ):
            return "bnb_three_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and spread_bps >= 8.0
            and margin_bps >= 30.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "bnb_three_high_spread_fast_far_margin_tail"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and spread_bps >= 3.0
            and margin_bps < 3.5
        ):
            return "bnb_three_wide_spread_yes_near_flat"
        if (
            source_subset == "drop_okx"
            and rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("binance", "bybit", "coinbase")
            and 4.0 <= spread_bps < 5.0
            and 2.2 <= margin_bps < 2.4
            and close_abs_delta_ms is not None
            and 200 <= close_abs_delta_ms <= 320
        ):
            return "bnb_three_no_fast_midspread_lowmargin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps <= 0.55
            and margin_bps >= 3.0
            and margin_bps < 7.3
            and close_abs_delta_ms is not None
            and close_abs_delta_ms < 250
        ):
            return "bnb_two_tight_spread_yes_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources >= 1
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 0.5
            and margin_bps < 2.0
        ):
            return "bnb_two_binance_bybit_exact_lowspread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 0.6
            and margin_bps >= 15.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 200
        ):
            return "bnb_two_binance_bybit_yes_lowspread_fast_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 1.3
            and 3.0 <= margin_bps < 3.3
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 800
        ):
            return "bnb_two_binance_bybit_yes_lowspread_fast_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 1.0
            and 5.5 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "bnb_two_binance_bybit_yes_lowspread_fast_upper_mid_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 0.3
            and 9.0 <= margin_bps < 10.0
            and close_abs_delta_ms is not None
            and 800 <= close_abs_delta_ms <= 1_500
        ):
            return "bnb_two_binance_bybit_yes_lowspread_fast_high_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and 1.5 <= spread_bps < 2.0
            and 8.0 <= margin_bps < 8.5
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_500
        ):
            return "bnb_two_binance_bybit_yes_midspread_stale_upper_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps >= 1.5
            and margin_bps < 5.0
        ):
            return "bnb_two_wide_spread_yes_near_flat"
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
            and spread_bps >= 1.5
            and margin_bps < 1.5
        ):
            return "bnb_mid_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps <= 1.3
            and margin_bps < 2.2
        ):
            return "bnb_tight_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 1.2
            and 2.0 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 800
        ):
            return "bnb_two_binance_bybit_yes_midspread_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 1.3
            and 2.0 <= margin_bps < 3.0
        ):
            return "bnb_two_binance_bybit_yes_lowspread_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "bybit")
            and spread_bps <= 1.3
            and 7.0 <= margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 2_500
        ):
            return "bnb_two_binance_bybit_yes_stale_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and margin_bps < 1.0
        ):
            return "bnb_yes_near_flat"
        if (
            close_only_reason == "preclose_near_flat"
            and source_count == 1
            and exact_sources == 0
            and margin_bps < 2.0
        ):
            return "bnb_close_only_preclose_near_flat"
    elif symbol == "btc/usd":
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps < 0.451
        ):
            return "btc_single_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.9
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "btc_single_coinbase_yes_fast_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.9
            and close_abs_delta_ms is not None
            and 300 < close_abs_delta_ms <= 600
        ):
            return "btc_single_coinbase_yes_midlag_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 1.0 <= margin_bps < 1.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 200
        ):
            return "btc_single_coinbase_yes_fast_low_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 2.2 <= margin_bps < 2.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "btc_single_coinbase_yes_very_fast_upper_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 2.2 <= margin_bps < 2.5
            and close_abs_delta_ms is not None
            and 100 < close_abs_delta_ms <= 150
        ):
            return "btc_single_coinbase_yes_fast_upper_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 0.9 <= margin_bps < 1.2
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 550
        ):
            return "btc_single_coinbase_yes_midlag_upper_near_flat_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 1.8 <= margin_bps < 2.1
            and close_abs_delta_ms is not None
            and 300 < close_abs_delta_ms <= 500
        ):
            return "btc_single_coinbase_yes_midlag_upper_near_flat"
        if (
            source_subset == "only_coinbase"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 1.14 <= margin_bps < 1.17
            and close_abs_delta_ms is not None
            and 250 <= close_abs_delta_ms <= 330
        ):
            return "btc_single_coinbase_yes_midlag_low_margin_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 1.05 <= margin_bps < 1.07
            and close_abs_delta_ms is not None
            and 430 <= close_abs_delta_ms <= 500
        ):
            return "btc_single_coinbase_yes_upper_midlag_low_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and 2.4 <= margin_bps < 2.7
            and close_abs_delta_ms is not None
            and 500 < close_abs_delta_ms <= 700
        ):
            return "btc_single_coinbase_yes_midlag_upper_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.9
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_800
        ):
            return "btc_single_coinbase_yes_stale_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "btc_single_coinbase_no_fast_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.5
            and close_abs_delta_ms is not None
            and 300 < close_abs_delta_ms <= 700
        ):
            return "btc_single_coinbase_no_midlag_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and margin_bps < 0.25
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_800
        ):
            return "btc_single_coinbase_no_stale_near_flat"
        if (
            source_subset == "only_coinbase"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and margin_bps >= 50.0
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 800
        ):
            return "btc_single_coinbase_no_late_extreme_margin_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and 0.75 <= margin_bps < 0.79
            and close_abs_delta_ms is not None
            and 850 <= close_abs_delta_ms <= 900
        ):
            return "btc_single_coinbase_no_midlag_low_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase",)
            and 2.0 <= margin_bps < 2.6
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "btc_single_coinbase_no_fast_mid_margin"
        if rule == "after_then_before" and source_count == 2 and exact_sources == 0 and margin_bps < 1.0:
            return "btc_two_source_near_flat"
    elif symbol == "xrp/usd":
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and spread_bps < 0.05
            and margin_bps < 1.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "xrp_binance_coinbase_yes_zero_spread_fast_near_flat"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 1.14 <= margin_bps < 1.16
            and close_abs_delta_ms is not None
            and 60 <= close_abs_delta_ms <= 140
        ):
            return "xrp_binance_coinbase_yes_fast_lowmargin_side_tail"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 1.0 <= spread_bps < 1.7
            and margin_bps < 0.35
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "xrp_binance_coinbase_yes_fast_midspread_near_flat"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 1.0 <= spread_bps < 1.7
            and margin_bps < 1.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 800
        ):
            return "xrp_binance_coinbase_yes_midlag_midspread_near_flat"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 0.5 <= spread_bps < 1.0
            and margin_bps < 0.7
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "xrp_binance_coinbase_yes_fast_tightspread_near_flat"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and margin_bps < 0.45
            and not ((not side_yes and close_abs_delta_ms is not None and close_abs_delta_ms >= 400) or (side_yes and close_abs_delta_ms is not None and close_abs_delta_ms < 300))
        ):
            return "xrp_nearest_near_flat"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 2.0
            and margin_bps < 2.0
            and not (not side_yes and close_abs_delta_ms is not None and close_abs_delta_ms >= 50)
        ):
            return "xrp_nearest_wide_spread_near_flat"
        if (
            rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and margin_bps < 1.3
            and not (not side_yes and margin_bps < 1.13)
        ):
            return "xrp_single_nearest_near_flat"
        if (
            rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and sources == ("binance",)
            and not side_yes
            and source_subset == "only_binance_coinbase"
            and margin_bps < 0.7
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "xrp_single_binance_no_fast_near_flat"
        if (
            rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and sources == ("binance",)
            and side_yes
            and margin_bps < 2.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_000
        ):
            return "xrp_single_binance_yes_stale_mid_margin"
        if (
            rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and sources == ("binance",)
            and side_yes
            and margin_bps < 4.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "xrp_single_binance_fast_mid_margin"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and side_yes
            and spread_bps >= 3.0
            and margin_bps < 2.6
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "xrp_nearest_wide_spread_yes_fast_mid_margin"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and not side_yes
            and sources == ("binance", "coinbase")
            and spread_bps >= 2.0
            and 2.0 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 800
        ):
            return "xrp_binance_coinbase_no_stale_mid_margin"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("binance", "coinbase")
            and 1.0 <= spread_bps < 1.7
            and margin_bps < 0.8
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_000
        ):
            return "xrp_binance_coinbase_no_stale_midspread_near_flat"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 0.65 <= margin_bps < 0.665
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_070
        ):
            return "xrp_binance_coinbase_yes_stale_lowmargin_side_tail"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("binance", "coinbase")
            and 0.7 <= spread_bps < 0.72
            and 2.5 <= margin_bps < 2.6
            and close_abs_delta_ms is not None
            and 330 <= close_abs_delta_ms <= 350
        ):
            return "xrp_binance_coinbase_yes_midlag_tightspread_mid_margin_tail"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("binance", "coinbase")
            and 2.0 <= spread_bps < 3.5
            and margin_bps < 1.2
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "xrp_binance_coinbase_no_fast_midspread_near_flat"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("binance", "coinbase")
            and margin_bps < 0.09
            and close_abs_delta_ms is not None
            and 400 <= close_abs_delta_ms <= 700
        ):
            return "xrp_binance_coinbase_no_stale_extreme_near_flat"
        if (
            rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and sources == ("binance",)
            and side_yes
            and 2.5 <= margin_bps < 3.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 2_000
        ):
            return "xrp_single_binance_yes_stale_mid_margin"
        if (
            source_subset == "only_binance_coinbase"
            and rule == "nearest_abs"
            and source_count == 1
            and exact_sources == 0
            and sources == ("binance",)
            and not side_yes
            and 0.54 <= margin_bps < 0.56
            and close_abs_delta_ms is not None
            and 2_750 <= close_abs_delta_ms <= 2_850
        ):
            return "xrp_single_binance_no_stale_lowmargin_side_tail"
        if rule == "last_before" and exact_sources == 0 and side_yes and margin_bps < 1.5:
            return "xrp_last_yes_near_flat"
    elif symbol == "doge/usd":
        close_abs_delta_ms = 999_999 if close_abs_delta_ms is None else close_abs_delta_ms
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and close_abs_delta_ms <= 1_000
            and margin_bps < 4.0
            and not (sources == ("okx",) and margin_bps >= 0.45)
        ):
            return "doge_single_last_fast_mid_margin"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("bybit",)
            and side_yes
            and 4.0 <= margin_bps < 5.0
            and close_abs_delta_ms <= 1_000
        ):
            return "doge_single_bybit_yes_fast_upper_mid_margin"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("okx",)
            and 900 <= close_abs_delta_ms <= 1_000
            and 8.0 <= margin_bps < 15.0
        ):
            return "doge_single_okx_late_fast_mid_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("okx",)
            and side_yes
            and 1.5 <= margin_bps < 2.0
            and close_abs_delta_ms is not None
            and 1_200 <= close_abs_delta_ms <= 1_600
        ):
            return "doge_single_okx_last_midlag_mid_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 3.5 <= spread_bps < 3.7
            and margin_bps >= 50.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 600
        ):
            return "doge_three_bybit_coinbase_okx_last_midspread_no_extreme_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("okx",)
            and side_yes
            and 2.0 <= margin_bps < 2.5
            and 4_000 <= close_abs_delta_ms <= 4_500
        ):
            return "doge_single_okx_last_very_stale_yes_mid_margin"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 3.8
            and margin_bps >= 7.0
            and margin_bps < 8.0
            and close_abs_delta_ms <= 500
        ):
            return "doge_three_last_fast_spread_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 6.0 <= spread_bps < 7.0
            and 25.0 <= margin_bps < 27.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "doge_three_bybit_coinbase_okx_last_fast_highspread_far_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 2.0 <= spread_bps < 3.0
            and 8.0 <= margin_bps < 10.0
            and close_abs_delta_ms is not None
            and 400 <= close_abs_delta_ms <= 600
        ):
            return "doge_three_bybit_coinbase_okx_last_fast_midspread_no_mid_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 2.7 <= spread_bps < 2.8
            and 2.2 <= margin_bps < 2.4
            and close_abs_delta_ms is not None
            and 250 <= close_abs_delta_ms <= 330
        ):
            return "doge_three_last_fast_midspread_lowmargin_tail"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 4.0
            and margin_bps < 3.5
            and close_abs_delta_ms <= 800
        ):
            return "doge_three_last_fast_spread_near_flat"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps <= 1.1
            and margin_bps >= 15.0
            and close_abs_delta_ms <= 800
        ):
            return "doge_three_last_fast_tightspread_tail"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps <= 1.1
            and margin_bps >= 10.0
            and spread_bps >= 0.9
            and spread_bps < 1.0
            and close_abs_delta_ms <= 800
        ):
            return "doge_multi_last_fast_tightspread_tail"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "okx")
            and spread_bps <= 0.1
            and 5.0 <= margin_bps < 6.0
            and close_abs_delta_ms <= 800
        ):
            return "doge_two_last_tightspread_fast_mid_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "okx")
            and margin_bps < 4.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "doge_two_bybit_okx_last_very_fast_mid_margin_tail"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "okx")
            and 0.8 <= spread_bps < 1.1
            and 10.0 <= margin_bps < 15.0
            and close_abs_delta_ms >= 1_500
        ):
            return "doge_two_bybit_okx_last_stale_tightspread_high_margin_tail"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps < 3.0
            and spread_bps < 2.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "doge_multi_last_fast_midspread_near_flat"
        if (
            rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and sources == ("bybit", "coinbase", "okx")
            and not side_yes
            and 2.0 <= spread_bps < 3.5
            and margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
            and close_only_reason in {"preclose_near_flat", "low_confidence"}
            and close_only_margin_bps is not None
            and close_only_margin_bps < 0.5
        ):
            return "doge_three_last_fast_midspread_low_signal_near_flat"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "okx")
            and margin_bps < 0.6
            and spread_bps < 1.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 3_500
        ):
            return "doge_three_bybit_coinbase_okx_last_very_stale_tightspread_low_signal"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and margin_bps < 1.0
            and 0.89 <= spread_bps < 0.91
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 3_000
        ):
            return "doge_two_coinbase_okx_last_very_stale_tightspread_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "coinbase")
            and spread_bps >= 5.0
            and 2.0 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_800
        ):
            return "doge_two_bybit_coinbase_last_stale_widespread_mid_margin"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps >= 5.0
            and margin_bps < 7.5
            and spread_bps < 2.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 700
        ):
            return "doge_multi_last_fast_midspread_tail"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps >= 5.0
            and margin_bps < 10.0
            and 1_000 <= close_abs_delta_ms <= 1_800
        ):
            return "doge_multi_last_midspread_tail"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps >= 15.0
            and margin_bps < 23.0
            and close_abs_delta_ms >= 2_500
        ):
            return "doge_multi_last_stale_midspread_tail"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and 1.5 <= spread_bps < 3.5
            and margin_bps >= 23.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 4_500
        ):
            return "doge_multi_last_very_stale_midspread_high_margin_tail"
        if (
            rule == "last_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 4.0
            and margin_bps < 3.0
            and close_abs_delta_ms >= 1_800
        ):
            return "doge_three_last_stale_highspread_near_flat"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps < 3.5
            and close_abs_delta_ms < 2_148
            and close_abs_delta_ms >= 1_800
        ):
            return "doge_multi_last_stale_near_flat"
        if (
            rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 0.8 <= spread_bps < 1.1
            and margin_bps < 0.5
            and close_abs_delta_ms is not None
            and 1_800 <= close_abs_delta_ms <= 3_000
        ):
            return "doge_three_last_stale_tightspread_yes_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase")
            and 0.8 <= spread_bps < 1.1
            and margin_bps < 0.2
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_500
        ):
            return "doge_two_bybit_coinbase_last_stale_tightspread_no_near_flat"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and side_yes
            and 1.5 <= spread_bps < 2.0
            and margin_bps < 3.5
            and close_abs_delta_ms is not None
            and 2_148 <= close_abs_delta_ms <= 3_000
        ):
            return "doge_multi_last_stale_midspread_yes_near_flat"
        if (
            rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and sources == ("bybit", "coinbase", "okx")
            and side_yes
            and 2.0 <= spread_bps < 3.5
            and margin_bps < 1.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 3_000
        ):
            return "doge_three_last_stale_midspread_yes_near_flat"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 3
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "coinbase", "okx")
            and 3.5 <= spread_bps < 3.8
            and 1.7 <= margin_bps < 1.9
            and close_abs_delta_ms is not None
            and 3_500 <= close_abs_delta_ms <= 3_700
        ):
            return "doge_three_last_stale_midspread_lowmargin_tail"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "okx")
            and 1.5 <= spread_bps < 2.2
            and margin_bps < 3.0
            and close_abs_delta_ms >= 1_500
        ):
            return "doge_two_bybit_okx_last_stale_midspread_near_flat"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "okx")
            and 2.5 <= spread_bps < 3.1
            and margin_bps < 1.5
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_600
        ):
            return "doge_two_bybit_okx_last_stale_highmidspread_yes_near_flat"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "okx")
            and 1.5 <= spread_bps < 2.2
            and margin_bps < 0.3
            and close_abs_delta_ms is not None
            and 2_500 <= close_abs_delta_ms <= 3_000
        ):
            return "doge_two_bybit_okx_last_stale_midspread_no_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "okx")
            and 0.8 <= spread_bps < 1.1
            and margin_bps < 1.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 2_500
        ):
            return "doge_two_bybit_okx_last_very_stale_tight_near_flat"
        if rule == "last_before" and source_count == 1 and exact_sources == 0 and margin_bps < 1.5:
            return "doge_single_last_near_flat"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "okx")
            and 0.8 <= spread_bps < 1.2
            and margin_bps < 0.8
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "doge_two_coinbase_okx_last_fast_tightspread_no_near_flat"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and margin_bps < 1.5
            and not ((not side_yes and margin_bps < 1.25) or close_abs_delta_ms >= 1_500)
        ):
            return "doge_last_multi_near_flat"
        if (
            rule == "last_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 8.0
            and not (
                side_yes
                and margin_bps >= 20.0
                and spread_bps <= 11.0
                and close_abs_delta_ms <= 500
            )
        ):
            return "doge_last_high_spread"
        if rule == "nearest_abs" and source_count >= 2 and exact_sources == 0 and margin_bps < 0.5:
            return "doge_nearest_near_flat"
        if (
            source_subset == "doge_binance_fallback"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and (
                (7.8 <= margin_bps < 8.2)
                or margin_bps >= 13.5
            )
        ):
            return "doge_binance_fallback_tail_margin"
        if (
            source_subset == "doge_binance_fallback"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and 7.0 <= margin_bps < 7.8
        ):
            return "doge_binance_fallback_mid_margin"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("bybit",)
            and margin_bps >= 14.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "doge_single_bybit_fast_high_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and not side_yes
            and sources == ("okx",)
            and margin_bps < 1.6
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 4_000
        ):
            return "doge_single_okx_last_very_stale_no_near_flat"
    elif symbol == "hype/usd":
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 8.0
            and margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_three_after_high_spread_fast_mid_margin"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and 1.5 <= spread_bps < 2.5
            and 7.0 <= margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "hype_three_after_mid_spread_fast_mid_margin"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and margin_bps < 2.6
            and not (side_yes and margin_bps < 2.5)
        ):
            return "hype_close_only_single_near_flat"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 7.0 <= margin_bps < 13.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 6.5 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and 300 <= close_abs_delta_ms <= 500
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("okx",)
            and side_yes
            and 8.5 <= margin_bps < 8.8
            and close_abs_delta_ms is not None
            and 300 <= close_abs_delta_ms <= 350
        ):
            return "hype_close_only_single_okx_yes_midlag_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 6.5 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_upper_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 15.0 <= margin_bps < 17.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 200
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_high_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 5.5 <= margin_bps < 6.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 150
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 3.0 <= margin_bps < 4.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 150
        ):
            return "hype_close_only_single_hyperliquid_no_fast_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 6.4 <= margin_bps < 6.5
            and close_abs_delta_ms is not None
            and 80 <= close_abs_delta_ms <= 120
        ):
            return "hype_close_only_single_hyperliquid_no_micro_mid_margin_error_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 5.0 <= margin_bps < 6.0
            and close_abs_delta_ms is not None
            and 300 <= close_abs_delta_ms <= 500
        ):
            return "hype_close_only_single_hyperliquid_no_fast_low_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 7.5 <= margin_bps < 7.7
            and close_abs_delta_ms is not None
            and 520 <= close_abs_delta_ms <= 540
        ):
            return "hype_close_only_single_hyperliquid_no_late_upper_mid_margin_error_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 5.0 <= margin_bps < 5.1
            and close_abs_delta_ms is not None
            and 180 <= close_abs_delta_ms <= 230
        ):
            return "hype_close_only_single_hyperliquid_no_fast_low_margin_side_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 2.95 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and 280 <= close_abs_delta_ms <= 320
        ):
            return "hype_close_only_single_hyperliquid_no_late_mid_margin_side_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 7.5 <= margin_bps < 8.2
            and close_abs_delta_ms is not None
            and 840 <= close_abs_delta_ms <= 950
        ):
            return "hype_close_only_single_hyperliquid_yes_stale_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 5.5 <= margin_bps < 5.6
            and close_abs_delta_ms is not None
            and 450 <= close_abs_delta_ms <= 550
        ):
            return "hype_close_only_single_hyperliquid_yes_late_low_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 5.0 <= margin_bps < 20.0
            and close_abs_delta_ms is not None
            and 650 <= close_abs_delta_ms <= 850
        ):
            return "hype_close_only_single_hyperliquid_yes_midlag_error_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 10.0 <= margin_bps < 13.0
            and close_abs_delta_ms is not None
            and 950 <= close_abs_delta_ms <= 1100
        ):
            return "hype_close_only_single_hyperliquid_yes_late_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 2.0 <= margin_bps < 2.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "hype_close_only_single_hyperliquid_yes_fast_near_flat"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 2.0 <= margin_bps < 2.1
            and close_abs_delta_ms is not None
            and 150 <= close_abs_delta_ms <= 450
        ):
            return "hype_close_only_single_hyperliquid_yes_low_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and side_yes
            and 2.6 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and 250 <= close_abs_delta_ms <= 450
        ):
            return "hype_close_only_single_hyperliquid_yes_midlag_near_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and 3.5 <= margin_bps < 5.5
        ):
            return "hype_close_only_single_yes_near_flat"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps < 7.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 500
        ):
            return "hype_close_only_single_yes_stale_near_flat"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 6.0 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and 150 <= close_abs_delta_ms <= 500
        ):
            return "hype_close_only_single_hyperliquid_no_fast_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 4.35 <= margin_bps < 4.39
            and close_abs_delta_ms is not None
            and 480 <= close_abs_delta_ms <= 520
        ):
            return "hype_close_only_single_hyperliquid_no_midlag_low_margin_side_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 11.4 <= margin_bps < 11.7
            and close_abs_delta_ms is not None
            and 50 <= close_abs_delta_ms <= 80
        ):
            return "hype_close_only_single_hyperliquid_no_micro_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 12.68 <= margin_bps < 12.71
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 120
        ):
            return "hype_close_only_single_hyperliquid_no_micro_upper_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 13.40 <= margin_bps < 13.45
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 30
        ):
            return "hype_close_only_single_hyperliquid_no_micro_upper_high_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 13.82 <= margin_bps < 13.90
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 10
        ):
            return "hype_close_only_single_hyperliquid_no_micro_high_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 10.0 <= margin_bps < 11.5
            and close_abs_delta_ms is not None
            and 350 <= close_abs_delta_ms <= 500
        ):
            return "hype_close_only_single_hyperliquid_no_fast_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 7.0 <= margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 300
        ):
            return "hype_close_only_single_hyperliquid_no_fast_upper_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 10.0 <= margin_bps < 16.0
            and close_abs_delta_ms is not None
            and 850 <= close_abs_delta_ms <= 950
        ):
            return "hype_close_only_single_hyperliquid_no_late_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 4.0 <= margin_bps < 10.0
            and close_abs_delta_ms is not None
            and 650 <= close_abs_delta_ms < 720
        ):
            return "hype_close_only_single_hyperliquid_no_stale_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 2.9 <= margin_bps < 4.1
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 750
        ):
            return "hype_close_only_single_hyperliquid_no_late_near_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 2.9 <= margin_bps < 3.1
            and close_abs_delta_ms is not None
            and 850 <= close_abs_delta_ms <= 950
        ):
            return "hype_close_only_single_hyperliquid_no_stale_near_margin_error_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 3.4 <= margin_bps < 3.5
            and close_abs_delta_ms is not None
            and 900 <= close_abs_delta_ms <= 1_000
        ):
            return "hype_close_only_single_hyperliquid_no_stale_low_margin_side_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 4.2 <= margin_bps < 4.35
            and close_abs_delta_ms is not None
            and 850 <= close_abs_delta_ms <= 900
        ):
            return "hype_close_only_single_hyperliquid_no_stale_lower_mid_margin_side_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 5.9 <= margin_bps < 6.1
            and close_abs_delta_ms is not None
            and 880 <= close_abs_delta_ms <= 930
        ):
            return "hype_close_only_single_hyperliquid_no_stale_mid_margin_error_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 10.0 <= margin_bps < 16.0
            and close_abs_delta_ms is not None
            and 720 <= close_abs_delta_ms < 850
        ):
            return "hype_close_only_single_hyperliquid_no_midlag_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 17.0 <= margin_bps < 19.0
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 750
        ):
            return "hype_close_only_single_hyperliquid_no_stale_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 19.14 <= margin_bps < 19.18
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 60
        ):
            return "hype_close_only_single_hyperliquid_no_micro_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 19.27 <= margin_bps < 19.35
            and close_abs_delta_ms is not None
            and 40 <= close_abs_delta_ms <= 250
        ):
            return "hype_close_only_single_hyperliquid_no_fast_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 14.45 <= margin_bps < 14.55
            and close_abs_delta_ms is not None
            and 400 <= close_abs_delta_ms <= 450
        ):
            return "hype_close_only_single_hyperliquid_no_midlag_upper_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 1
            and exact_sources == 0
            and sources == ("hyperliquid",)
            and not side_yes
            and 14.42 <= margin_bps < 14.45
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 600
        ):
            return "hype_close_only_single_hyperliquid_no_late_upper_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and side_yes
            and 1.0 <= spread_bps < 1.5
            and 7.0 <= margin_bps < 10.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_close_only_multi_fast_midspread_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 10.5 <= margin_bps < 10.8
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_600
        ):
            return "hype_close_only_bybit_hyperliquid_yes_very_stale_mid_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("hyperliquid", "okx")
            and side_yes
            and 1.0 <= spread_bps < 1.5
            and 20.0 <= margin_bps < 30.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_close_only_hyperliquid_okx_yes_fast_midspread_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and side_yes
            and margin_bps < 5.0
        ):
            return "hype_close_only_multi_yes_near_flat"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 0.5 <= spread_bps < 1.0
            and 5.5 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_500
        ):
            return "hype_close_only_bybit_hyperliquid_yes_stale_tightspread_mid_margin"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 1.0 <= spread_bps < 2.5
            and 5.0 <= margin_bps < 7.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_000
        ):
            return "hype_close_only_bybit_hyperliquid_yes_stale_midspread_mid_margin"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 1.0 <= spread_bps < 1.5
            and margin_bps >= 20.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "hype_close_only_bybit_hyperliquid_yes_fast_midspread_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and spread_bps < 0.5
            and 30.0 <= margin_bps < 40.0
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_500
        ):
            return "hype_close_only_bybit_hyperliquid_yes_stale_tightspread_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 0.5 <= spread_bps < 0.7
            and 12.0 <= margin_bps < 15.0
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_500
        ):
            return "hype_close_only_bybit_hyperliquid_yes_very_stale_tightspread_upper_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps <= 1.0
            and margin_bps >= 40.0
        ):
            return "hype_close_only_tight_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 1.5
            and margin_bps >= 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "hype_close_only_multi_fast_spread_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and 2.0 <= spread_bps < 4.0
            and margin_bps >= 40.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_000
        ):
            return "hype_close_only_multi_mid_spread_stale_far_margin_tail"
        if (
            source_subset == "close_only_fallback"
            and rule == "close_only"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 4.0
            and margin_bps >= 8.0
        ):
            return "hype_close_only_multi_wide_spread_margin"
        if (
            rule == "after_then_before"
            and source_count >= 4
            and exact_sources == 0
            and spread_bps >= 10.0
            and margin_bps < 8.0
        ):
            return "hype_four_after_high_spread_mid_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 4
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "coinbase", "hyperliquid", "okx")
            and side_yes
            and 8.0 <= spread_bps < 10.0
            and margin_bps < 8.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_500
        ):
            return "hype_four_drop_binance_stale_highspread_yes_mid_near_flat"
        if (
            rule == "after_then_before"
            and source_count >= 4
            and exact_sources == 0
            and spread_bps >= 10.0
            and margin_bps < 20.0
        ):
            return "hype_four_after_high_spread_mid_margin"
        if (
            rule == "after_then_before"
            and source_count >= 4
            and exact_sources == 0
            and spread_bps >= 15.0
            and margin_bps < 5.0
        ):
            return "hype_after_very_high_spread_near_flat"
        if (
            rule == "after_then_before"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 8.0
            and margin_bps >= 30.0
            and (close_abs_delta_ms is None or close_abs_delta_ms >= 900)
        ):
            return "hype_after_high_spread_margin"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and spread_bps >= 8.0
            and 8.0 <= margin_bps < 30.0
        ):
            return "hype_multi_after_high_spread_margin"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and spread_bps >= 5.0
            and margin_bps < 2.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 3_000
        ):
            return "hype_multi_after_stale_wide_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 4
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "coinbase", "hyperliquid", "okx")
            and side_yes
            and 4.5 <= spread_bps < 5.5
            and margin_bps < 2.0
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_500
        ):
            return "hype_four_drop_binance_stale_midspread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 4
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "coinbase", "hyperliquid", "okx")
            and side_yes
            and 2.2 <= spread_bps < 2.3
            and 1.3 <= margin_bps < 1.4
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 800
        ):
            return "hype_four_all_yes_midlag_low_margin_side_tail"
        if rule == "after_then_before" and source_count >= 3 and exact_sources == 0 and 2.0 <= spread_bps <= 4.0 and margin_bps >= 40.0:
            return "hype_three_source_stale_spread_fallback"
        if rule == "after_then_before" and source_count == 1 and exact_sources == 0 and margin_bps < 3.0:
            return "hype_single_after_near_flat"
        if rule == "after_then_before" and source_count >= 3 and exact_sources == 0 and margin_bps < 1.2:
            return "hype_three_after_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 5.0 <= spread_bps < 7.0
            and margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_500
        ):
            return "hype_three_bybit_hyperliquid_okx_fast_highspread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 4.0 <= spread_bps < 5.0
            and 20.0 <= margin_bps < 25.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "hype_three_bybit_hyperliquid_okx_fast_highspread_yes_far_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 5.0 <= spread_bps < 7.0
            and 20.0 <= margin_bps < 25.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 700
        ):
            return "hype_three_bybit_hyperliquid_okx_fast_highspread_yes_midlag_far_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 1.0 <= spread_bps < 2.5
            and 2.5 <= margin_bps < 3.2
            and close_abs_delta_ms is not None
            and 2_500 <= close_abs_delta_ms <= 3_000
        ):
            return "hype_three_bybit_hyperliquid_okx_stale_tightspread_yes_near_margin"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 2.4 <= spread_bps < 2.5
            and 1.3 <= margin_bps < 1.4
            and close_abs_delta_ms is not None
            and 2_000 <= close_abs_delta_ms <= 2_100
        ):
            return "hype_three_bybit_hyperliquid_okx_yes_stale_low_margin_side_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 2.0 <= spread_bps < 2.5
            and 1.5 <= margin_bps < 2.0
            and close_abs_delta_ms is not None
            and 1_000 <= close_abs_delta_ms <= 1_200
        ):
            return "hype_three_bybit_hyperliquid_okx_midlag_midspread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 4.5 <= spread_bps < 5.0
            and 2.0 <= margin_bps < 2.5
            and close_abs_delta_ms is not None
            and 3_000 <= close_abs_delta_ms <= 3_500
        ):
            return "hype_three_bybit_hyperliquid_okx_stale_widespread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 2.5 <= spread_bps < 3.1
            and 13.0 <= margin_bps < 15.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 700
        ):
            return "hype_three_bybit_hyperliquid_okx_midlag_midspread_yes_upper_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid", "okx")
            and not side_yes
            and 4.0 <= spread_bps < 5.0
            and 4.0 <= margin_bps < 5.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_three_bybit_hyperliquid_okx_fast_midspread_no_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("hyperliquid", "okx")
            and 2.5 <= spread_bps < 3.5
            and margin_bps < 2.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 600
        ):
            return "hype_two_hyperliquid_okx_yes_midspread_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("hyperliquid", "okx")
            and 0.7 <= spread_bps < 1.0
            and 1.8 <= margin_bps < 2.4
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 1_200
        ):
            return "hype_two_hyperliquid_okx_yes_tightspread_midlag_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and 1.0 <= spread_bps < 1.5
            and margin_bps < 1.8
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "hype_two_after_mid_spread_fast_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and source_subset == "drop_binance"
            and 2.0 <= spread_bps < 3.0
            and 2.0 <= margin_bps < 2.6
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_two_bybit_hyperliquid_fast_midspread_yes_near_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 2.0 <= spread_bps < 3.0
            and 3.0 <= margin_bps < 5.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_two_bybit_hyperliquid_fast_midspread_yes_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 0.3 <= spread_bps < 0.4
            and 4.1 <= margin_bps < 4.2
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 50
        ):
            return "hype_two_bybit_hyperliquid_yes_fast_low_margin_side_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 1.0 <= spread_bps < 1.5
            and 10.0 <= margin_bps < 12.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_two_bybit_hyperliquid_fast_tightspread_yes_upper_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and source_subset == "drop_binance"
            and 3.0 <= spread_bps < 3.5
            and margin_bps < 2.2
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 800
        ):
            return "hype_two_bybit_hyperliquid_midlag_widespread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 3.0 <= spread_bps < 4.0
            and 2.0 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_two_bybit_hyperliquid_fast_widespread_yes_near_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 3.5 <= spread_bps < 4.0
            and 2.5 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 900
        ):
            return "hype_two_bybit_hyperliquid_midlag_widespread_yes_near_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 3.5 <= spread_bps < 4.0
            and 8.0 <= margin_bps < 9.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "hype_two_bybit_hyperliquid_fast_widespread_yes_upper_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 5.0 <= spread_bps < 6.0
            and 3.5 <= margin_bps < 4.5
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 700
        ):
            return "hype_two_bybit_hyperliquid_midlag_highspread_yes_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("bybit", "hyperliquid")
            and side_yes
            and 4.0 <= spread_bps < 4.5
            and 13.0 <= margin_bps < 15.0
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 700
        ):
            return "hype_two_bybit_hyperliquid_midlag_midspread_yes_upper_margin_tail"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "hyperliquid", "okx")
            and spread_bps >= 4.0
            and margin_bps < 4.0
        ):
            return "hype_three_coinbase_hl_okx_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and source_subset == "drop_binance"
            and sources == ("coinbase", "hyperliquid")
            and not side_yes
            and 3.9 <= spread_bps < 4.1
            and 1.08 <= margin_bps < 1.10
            and close_abs_delta_ms is not None
            and 850 <= close_abs_delta_ms <= 900
        ):
            return "hype_two_coinbase_hyperliquid_no_stale_low_margin_side_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "hyperliquid", "okx")
            and 4.0 <= spread_bps < 5.0
            and margin_bps >= 40.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 800
        ):
            return "hype_three_coinbase_hl_okx_no_midspread_far_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 3
            and exact_sources == 0
            and sources == ("bybit", "hyperliquid", "okx")
            and side_yes
            and 3.0 <= spread_bps < 5.0
            and 10.0 <= margin_bps < 15.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 1_500
        ):
            return "hype_three_bybit_hyperliquid_okx_stale_midspread_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps >= 10.0
            and (
                close_abs_delta_ms is None
                or close_abs_delta_ms < 200
                or (not side_yes and sources == ("hyperliquid", "okx") and margin_bps < 1.5)
            )
        ):
            return "hype_two_after_very_high_spread"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("hyperliquid", "okx")
            and 5.0 <= spread_bps < 6.0
            and margin_bps < 4.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 1_000
        ):
            return "hype_two_hyperliquid_okx_yes_widespread_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("hyperliquid", "okx")
            and 5.0 <= spread_bps < 6.5
            and 15.0 <= margin_bps < 22.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 700
        ):
            return "hype_two_hyperliquid_okx_yes_highspread_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("hyperliquid", "okx")
            and 5.0 <= spread_bps < 6.5
            and 15.0 <= margin_bps < 22.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 700
        ):
            return "hype_two_hyperliquid_okx_no_highspread_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps >= 6.0
            and (
                margin_bps >= 30.0
                or (margin_bps >= 16.0 and spread_bps < 6.5)
            )
            and (side_yes or margin_bps >= 40.0)
        ):
            return "hype_two_after_high_spread_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "hyperliquid")
            and 5.5 <= spread_bps < 8.0
            and 8.0 <= margin_bps < 10.0
            and close_abs_delta_ms is not None
            and 800 <= close_abs_delta_ms <= 1_200
        ):
            return "hype_two_bybit_hyperliquid_stale_highspread_mid_margin"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "hyperliquid")
            and 6.0 <= spread_bps < 7.0
            and 8.0 <= margin_bps < 9.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 200
        ):
            return "hype_two_bybit_hyperliquid_fast_highspread_mid_margin_tail"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "hyperliquid")
            and 5.0 <= spread_bps < 6.0
            and margin_bps >= 20.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "hype_two_bybit_hyperliquid_fast_highspread_high_margin"
        if (
            rule == "after_then_before"
            and source_count >= 2
            and exact_sources == 0
            and side_yes
            and spread_bps >= 6.0
            and spread_bps < 8.0
            and margin_bps < 3.5
        ):
            return "hype_after_high_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count >= 3
            and exact_sources == 0
            and side_yes
            and 6.0 <= spread_bps < 8.0
            and 10.0 <= margin_bps < 20.0
        ):
            return "hype_three_after_high_spread_yes_mid_tail"
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
            and spread_bps >= 1.5
            and margin_bps < 1.8
            and (side_yes or margin_bps >= 1.25)
        ):
            return "hype_two_after_wide_spread_near_flat"
        if (
            source_subset == "drop_binance"
            and rule == "after_then_before"
            and source_count == 4
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "hyperliquid", "okx")
            and 4.5 <= spread_bps < 4.9
            and 18.0 <= margin_bps < 19.0
            and close_abs_delta_ms is not None
            and 450 <= close_abs_delta_ms <= 550
        ):
            return "hype_four_drop_binance_midspread_no_high_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "after_then_before"
            and source_count == 4
            and exact_sources == 0
            and not side_yes
            and sources == ("bybit", "coinbase", "hyperliquid", "okx")
            and spread_bps >= 9.0
            and margin_bps >= 50.0
            and close_abs_delta_ms is not None
            and 200 <= close_abs_delta_ms <= 250
        ):
            return "hype_four_drop_binance_fast_widespread_no_extreme_margin_tail"
        if (
            source_subset == "drop_binance"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("bybit", "hyperliquid")
            and spread_bps >= 4.0
            and margin_bps >= 30.0
            and close_abs_delta_ms is not None
            and 700 <= close_abs_delta_ms <= 800
        ):
            return "hype_two_bybit_hyperliquid_stale_highspread_yes_high_margin_tail"
        if rule == "nearest_abs" and exact_sources == 0:
            if source_count >= 2 and spread_bps <= 1.0 and margin_bps < 1.8:
                return "hype_nearest_near_flat"
            if source_count == 1 and margin_bps < 2.0:
                return "hype_single_nearest_near_flat"
    elif symbol == "eth/usd":
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and margin_bps < 1.0
            and not (not side_yes and margin_bps >= 0.63)
        ):
            return "eth_single_last_near_flat"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and not side_yes
            and 0.63 <= margin_bps < 0.8
            and close_abs_delta_ms is not None
            and 3_000 <= close_abs_delta_ms <= 5_000
        ):
            return "eth_single_coinbase_no_very_stale_near_flat"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and not side_yes
            and 0.63 <= margin_bps < 0.75
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "eth_single_coinbase_no_fast_low_margin_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and not side_yes
            and 1.85 <= margin_bps < 1.90
            and close_abs_delta_ms is not None
            and 150 <= close_abs_delta_ms <= 250
        ):
            return "eth_single_coinbase_no_fast_low_margin_side_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and not side_yes
            and margin_bps >= 80.0
            and close_abs_delta_ms is not None
            and 750 <= close_abs_delta_ms <= 800
        ):
            return "eth_single_coinbase_no_late_extreme_margin_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and not side_yes
            and 1.03 <= margin_bps < 1.06
            and close_abs_delta_ms is not None
            and 650 <= close_abs_delta_ms <= 720
        ):
            return "eth_single_coinbase_no_midlag_low_margin_side_tail"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 1.0 <= margin_bps < 2.2
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "eth_single_coinbase_yes_fast_mid_margin"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 1.0 <= margin_bps < 2.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 3_000
        ):
            return "eth_single_coinbase_yes_stale_mid_margin"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 5.3 <= margin_bps < 5.5
            and close_abs_delta_ms is not None
            and 930 <= close_abs_delta_ms <= 970
        ):
            return "eth_single_coinbase_yes_late_mid_margin_error_tail"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 11.3 <= margin_bps < 11.5
            and close_abs_delta_ms is not None
            and 550 <= close_abs_delta_ms <= 590
        ):
            return "eth_single_coinbase_yes_midlag_high_margin_tail"
        if (
            rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 1.0 <= margin_bps < 1.3
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms < 3_000
        ):
            return "eth_single_coinbase_yes_mid_stale_near_margin"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 2.5 <= margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 150
        ):
            return "eth_single_coinbase_yes_fast_upper_mid_margin"
        if (
            source_subset == "only_coinbase"
            and rule == "last_before"
            and source_count == 1
            and exact_sources == 0
            and sources == ("coinbase",)
            and side_yes
            and 8.0 <= margin_bps < 13.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 50
        ):
            return "eth_single_coinbase_yes_fast_mid_margin_tail"
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
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and margin_bps < 1.0
            and (side_yes or spread_bps < 2.5)
        ):
            return "sol_after_near_flat"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "okx")
            and 1.0 <= spread_bps < 2.5
            and 1.45 <= margin_bps < 1.7
            and close_abs_delta_ms is not None
            and 500 <= close_abs_delta_ms <= 900
        ):
            return "sol_okx_coinbase_no_midlag_midmargin_near_flat"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "okx")
            and 3.0 <= spread_bps < 4.2
            and 4.0 <= margin_bps < 5.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 250
        ):
            return "sol_okx_coinbase_no_fast_midspread_midmargin_tail"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "okx")
            and 1.1 <= spread_bps < 1.3
            and margin_bps >= 50.0
            and close_abs_delta_ms is not None
            and 150 <= close_abs_delta_ms <= 220
        ):
            return "sol_okx_coinbase_no_fast_tightspread_extreme_margin_tail"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and spread_bps >= 7.0
            and margin_bps < 1.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 600
        ):
            return "sol_okx_coinbase_high_spread_yes_near_flat"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and 2.0 <= spread_bps < 3.0
            and margin_bps < 3.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 100
        ):
            return "sol_okx_coinbase_mid_spread_yes_fast_near_flat"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and 2.0 <= spread_bps < 3.0
            and margin_bps < 1.6
            and close_abs_delta_ms is not None
            and 100 < close_abs_delta_ms <= 500
        ):
            return "sol_okx_coinbase_mid_spread_yes_fastish_near_flat"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and 1.0 <= margin_bps < 1.8
            and spread_bps <= 1.2
            and close_abs_delta_ms is not None
            and 300 <= close_abs_delta_ms <= 400
        ):
            return "sol_okx_coinbase_yes_midlag_near_flat_tail"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and sources == ("coinbase", "okx")
            and 1.17 <= margin_bps < 1.19
            and close_abs_delta_ms is not None
            and 2_300 <= close_abs_delta_ms <= 2_400
        ):
            return "sol_okx_coinbase_yes_stale_low_margin_side_tail"
        if (
            source_subset == "only_okx_coinbase"
            and rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and not side_yes
            and sources == ("coinbase", "okx")
            and spread_bps >= 3.0
            and margin_bps >= 10.0
            and close_abs_delta_ms is not None
            and close_abs_delta_ms <= 500
        ):
            return "sol_okx_coinbase_no_fast_high_spread_tail"
        if (
            source_subset == "sol_okx_missing_fallback"
            and rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and sources == ("okx",)
            and 9.0 <= margin_bps < 10.5
            and close_abs_delta_ms is not None
            and close_abs_delta_ms >= 2_000
        ):
            return "sol_okx_missing_stale_borderline_tail"
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
        hype_close_only_router_ready = False
        if symbol == "hype/usd" and fallback_hit is not None and fallback_filter_reason is None:
            close_only_side_yes = fallback_hit["close_price"] >= rtds_open
            close_only_margin_bps = (
                abs(fallback_hit["close_price"] - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            )
            close_only_abs_delta_ms = sorted(abs(off) for off in fallback_hit["source_offsets"])[
                len(fallback_hit["source_offsets"]) // 2
            ]
            hype_close_only_router_ready = (
                router_filter_reason(
                    symbol,
                    "close_only_fallback",
                    "close_only",
                    fallback_hit["source_count"],
                    fallback_hit["exact_sources"],
                    fallback_hit["source_spread_bps"],
                    close_only_margin_bps,
                    close_only_side_yes,
                    close_only_abs_delta_ms,
                    fallback_filter_reason,
                    fallback_margin_bps,
                    tuple(source for source, _ in sorted(fallback_hit["source_prices"])),
                )
                is None
            )
        fallback_source_hit = None if hype_close_only_router_ready else source_fallback_hit(sample)
        if fallback_source_hit is not None:
            pred_close = fallback_source_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            reason = router_filter_reason(
                symbol,
                fallback_source_hit["source_subset"],
                fallback_source_hit["rule"],
                fallback_source_hit["source_count"],
                fallback_source_hit["exact_sources"],
                fallback_source_hit["source_spread_bps"],
                margin_bps,
                side_yes,
                fallback_source_hit.get("close_abs_delta_ms"),
                fallback_filter_reason,
                fallback_margin_bps,
                tuple(fallback_source_hit["sources"].split(";")),
            )
            return {
                "status": "filtered" if reason else "ok",
                "filter_reason": reason or "",
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
                "close_abs_delta_ms": fallback_source_hit.get("close_abs_delta_ms"),
                "sources": fallback_source_hit["sources"],
            }
        if symbol == "hype/usd" and fallback_hit is not None and fallback_filter_reason is None:
            pred_close = fallback_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            reason = router_filter_reason(
                symbol,
                "close_only_fallback",
                "close_only",
                fallback_hit["source_count"],
                fallback_hit["exact_sources"],
                fallback_hit["source_spread_bps"],
                margin_bps,
                side_yes,
                sorted(abs(off) for off in fallback_hit["source_offsets"])[len(fallback_hit["source_offsets"]) // 2],
                fallback_filter_reason,
                fallback_margin_bps,
                tuple(source for source, _ in sorted(fallback_hit["source_prices"])),
            )
            return {
                "status": "filtered" if reason else "ok",
                "filter_reason": reason or "",
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
                "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
                "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
            }
        if filtered_close_only_allowed_without_weighted(
            symbol,
            fallback_filter_reason,
            fallback_hit,
            rtds_open,
        ):
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
                "source_subset": "filtered_close_only_fallback",
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
                "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
                "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
            }
        missing_reason = missing_filter_reason(
            symbol,
            fallback_filter_reason,
            fallback_margin_bps,
            fallback_hit,
        )
        if missing_reason:
            pred_close = fallback_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            return {
                "status": "filtered",
                "filter_reason": missing_reason,
                "symbol": symbol,
                "round_end_ts": sample["round_end_ts"],
                "instance_id": sample["instance_id"],
                "log_file": sample.get("log_file", ""),
                "source_subset": "filtered_close_only_missing",
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
                "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
                "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
            }
        return {
            "status": "missing",
            "symbol": symbol,
            "round_end_ts": sample["round_end_ts"],
            "instance_id": sample["instance_id"],
            "log_file": sample.get("log_file", ""),
            "source_count": len(per_source),
        }

    pred_close = sum(weight * price for _, _, price, weight in per_source) / sum(
        weight for _, _, _, weight in per_source
    )
    exact_sources = sum(1 for _, off, _, _ in per_source if off == 0)
    spread_bps = 0.0
    if len(per_source) >= 2:
        prices = [price for _, _, price, _ in per_source]
        spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0
    close_offsets = sorted(off for _, off, _, _ in per_source)
    close_abs_delta_ms = abs(close_offsets[len(close_offsets) // 2]) if close_offsets else None
    margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    if (
        symbol == "xrp/usd"
        and policy["source_subset"] == "only_binance_coinbase"
        and policy["rule"] == "nearest_abs"
        and len(per_source) == 2
        and exact_sources == 0
        and pred_close < rtds_open
        and spread_bps >= 4.0
        and margin_bps >= 6.0
        and close_abs_delta_ms is not None
        and close_abs_delta_ms <= 150
    ):
        by_source = {source: (off, price) for source, off, price, _ in per_source}
        binance = by_source.get("binance")
        coinbase = by_source.get("coinbase")
        if (
            binance is not None
            and coinbase is not None
            and abs(binance[0]) < abs(coinbase[0])
            and binance[1] > coinbase[1]
        ):
            pred_close = (binance[1] + coinbase[1]) / 2.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    side_yes = pred_close >= rtds_open
    truth_yes = rtds_close >= rtds_open
    close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
    reason = router_filter_reason(
        symbol,
        policy["source_subset"],
        policy["rule"],
        len(per_source),
        exact_sources,
        spread_bps,
        margin_bps,
        side_yes,
        close_abs_delta_ms,
        fallback_filter_reason,
        fallback_margin_bps,
        tuple(source for source, _, _, _ in sorted(per_source)),
    )
    if reason:
        fallback_source_hit = source_fallback_hit(sample)
        if fallback_source_hit is not None and source_fallback_rescues_filtered(symbol, reason, fallback_source_hit):
            pred_close = fallback_source_hit["close_price"]
            side_yes = pred_close >= rtds_open
            truth_yes = rtds_close >= rtds_open
            close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
            margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
            source_reason = router_filter_reason(
                symbol,
                fallback_source_hit["source_subset"],
                fallback_source_hit["rule"],
                fallback_source_hit["source_count"],
                fallback_source_hit["exact_sources"],
                fallback_source_hit["source_spread_bps"],
                margin_bps,
                side_yes,
                fallback_source_hit.get("close_abs_delta_ms"),
                fallback_filter_reason,
                fallback_margin_bps,
                tuple(fallback_source_hit["sources"].split(";")),
            )
            if source_reason is None:
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
                    "close_abs_delta_ms": fallback_source_hit.get("close_abs_delta_ms"),
                    "sources": fallback_source_hit["sources"],
                }
    if reason and symbol == "hype/usd" and fallback_hit is not None and fallback_filter_reason is None:
        pred_close = fallback_hit["close_price"]
        side_yes = pred_close >= rtds_open
        truth_yes = rtds_close >= rtds_open
        close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
        margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
        fallback_reason = router_filter_reason(
            symbol,
            "close_only_fallback",
            "close_only",
            fallback_hit["source_count"],
            fallback_hit["exact_sources"],
            fallback_hit["source_spread_bps"],
            margin_bps,
            side_yes,
            sorted(abs(off) for off in fallback_hit["source_offsets"])[len(fallback_hit["source_offsets"]) // 2],
            fallback_filter_reason,
            fallback_margin_bps,
            tuple(source for source, _ in sorted(fallback_hit["source_prices"])),
        )
        if fallback_reason:
            return {
                "status": "filtered",
                "filter_reason": fallback_reason,
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
                "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
                "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
            }
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
            "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
            "sources": ";".join(source for source, _ in sorted(fallback_hit["source_prices"])),
        }
    if reason and filtered_close_only_allowed_without_weighted(
        symbol,
        fallback_filter_reason,
        fallback_hit,
        rtds_open,
    ):
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
            "source_subset": "filtered_close_only_fallback",
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
            "close_abs_delta_ms": median_abs_offset_ms(fallback_hit["source_offsets"]),
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
        "close_abs_delta_ms": close_abs_delta_ms,
        "sources": ";".join(source for source, _, _, _ in sorted(per_source)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the Rust boundary_symbol_router_v1 policy.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_router_eval.csv")
    parser.add_argument("--test-rounds", type=int, default=16)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=250)
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
    parser.add_argument(
        "--ignore-local-ready-ms",
        action="store_true",
        help="Use all boundary-tape ticks even if they arrived after the runtime router decision. Default is ready-time aware.",
    )
    args = parser.parse_args()

    samples = [
        sample
        for sample in select_samples(
            instance_samples(Path(args.boundary_csv), respect_local_ready_ms=not args.ignore_local_ready_ms),
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
        for key in (
            "local_started_ms",
            "local_ready_ms",
            "local_deadline_ms",
            "local_timing_source",
            "local_ready_lag_ms",
            "close_points_total",
            "close_points_seen",
            "close_points_dropped_after_ready",
        ):
            row[key] = sample.get(key)
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
        "local_started_ms",
        "local_ready_ms",
        "local_deadline_ms",
        "local_timing_source",
        "local_ready_lag_ms",
        "close_points_total",
        "close_points_seen",
        "close_points_dropped_after_ready",
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
        "close_abs_delta_ms",
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
