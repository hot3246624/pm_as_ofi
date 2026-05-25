#!/usr/bin/env python3
"""Read-only D+ passive/passive shadow runner.

This adapter is for the B27-inspired D+ branch. It sends no orders. It creates
virtual passive BUY bids on both YES and NO from public SELL flow, counts a fill
only when later public SELL flow crosses the virtual bid with configurable queue
share, pairs filled YES/NO lots internally, and records optional FAK salvage
opportunities for aged residual lots.

It is a live-style fill plausibility probe, not source-of-truth verification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any


DUST = 1e-9


def now_ms() -> int:
    return int(time.time() * 1000)


def pct(vals: list[float] | list[int], p: float) -> float | None:
    if not vals:
        return None
    xs = sorted(vals)
    idx = min(len(xs) - 1, max(0, round((len(xs) - 1) * p)))
    return float(xs[idx])


def parse_env_exports(text: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        key, _, raw = line[len("export ") :].partition("=")
        if key and raw:
            env[key] = shlex.split(raw)[0] if raw.startswith(("'", '"')) else raw
    return env


def round_start_from_slug(slug: str) -> int:
    m = re.search(r"-(\d+)$", slug)
    return int(m.group(1)) if m else 0


def side_from_str(raw: str) -> str:
    s = (raw or "").upper()
    if s in {"YES", "UP"}:
        return "YES"
    if s in {"NO", "DOWN"}:
        return "NO"
    return s


def opp(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def side_bid(book: dict[str, float], side: str) -> float:
    return book.get("yes_bid" if side == "YES" else "no_bid", 0.0)


def side_ask(book: dict[str, float], side: str) -> float:
    return book.get("yes_ask" if side == "YES" else "no_ask", 0.0)


def fee_per_share(px: float, rate: float) -> float:
    return rate * min(max(px, 0.0), max(1.0 - px, 0.0))


def source_sequence_id(msg: dict[str, Any]) -> Any:
    for key in ("source_sequence_id", "source_seq", "sequence_id", "seq"):
        value = msg.get(key)
        if value is not None:
            return value
    return None


def parse_float_csv(raw: str | None) -> list[float]:
    if raw is None:
        return []
    out: list[float] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        out.append(float(text))
    return out


def profile_name_for_late_repair(value: float) -> str:
    text = ("%g" % value).replace("-", "m").replace(".", "p")
    return f"repair{text}"


def px_bucket(px: float | None) -> str:
    if px is None or px <= 0:
        return "none"
    if px < 0.30:
        return "px_lt_0p30"
    if px < 0.45:
        return "px_0p30_0p45"
    if px < 0.48:
        return "px_0p45_0p48"
    if px < 0.55:
        return "px_0p48_0p55"
    return "px_ge_0p55"


def offset_bucket(offset_s: float | None) -> str:
    if offset_s is None:
        return "unknown"
    if offset_s < 30:
        return "offset_0_30"
    if offset_s < 60:
        return "offset_30_60"
    if offset_s < 90:
        return "offset_60_90"
    if offset_s < 120:
        return "offset_90_120"
    return "offset_ge_120"


def pair_cost_bucket(cost: float | None) -> str:
    if cost is None or cost <= 0:
        return "none"
    if cost < 0.95:
        return "pair_cost_lt_0p95"
    if cost <= 1.00:
        return "pair_cost_0p95_1p00"
    if cost <= 1.05:
        return "pair_cost_1p00_1p05"
    return "pair_cost_gt_1p05"


def ce25_projected_pair_cost_bucket(cost: float | None) -> str:
    if cost is None or cost <= 0:
        return "projected_pair_cost_unknown"
    if cost < 0.90:
        return "projected_pair_cost_lt_0p90"
    if cost < 0.95:
        return "projected_pair_cost_0p90_0p95"
    if cost < 1.00:
        return "projected_pair_cost_0p95_1p00"
    return "projected_pair_cost_gte_1p00"


def ce25_projected_residual_bucket(rate: float | None) -> str:
    if rate is None or rate < 0:
        return "projected_residual_unknown"
    if rate < 0.10:
        return "projected_residual_lt_10pct"
    if rate < 0.15:
        return "projected_residual_10_15pct"
    if rate <= 0.20:
        return "projected_residual_15_20pct"
    return "projected_residual_gt_20pct"


def activation_state_bucket(required: bool, age_ms: int | float | None, opposite_seen_ms: int | None) -> str:
    if not required:
        return "activation_not_required"
    if age_ms is None:
        return "activation_required_missing_opp" if opposite_seen_ms is None else "activation_required_age_unknown"
    return f"activation_required_{age_ms_bucket('activation_opp_age', age_ms)}"


def asset_from_slug(slug: str) -> str:
    first = (slug or "").split("-", 1)[0]
    return first.upper() if first else "UNKNOWN"


def timeframe_from_slug(slug: str) -> str:
    lower = (slug or "").lower()
    if "updown-5m" in lower:
        return "5m"
    if "updown-15m" in lower:
        return "15m"
    if "updown-1h" in lower or "up-or-down" in lower:
        return "1h"
    if "updown-4h" in lower:
        return "4h"
    return "unknown"


def market_round_length_s(slug: str) -> float | None:
    timeframe = timeframe_from_slug(slug)
    if timeframe == "5m":
        return 300.0
    if timeframe == "15m":
        return 900.0
    if timeframe == "1h":
        return 3600.0
    if timeframe == "4h":
        return 14400.0
    return None


def seconds_to_expiry_from_offset(slug: str, offset_s: float | None) -> float | None:
    round_length = market_round_length_s(slug)
    if round_length is None or offset_s is None:
        return None
    return max(0.0, round_length - offset_s)


def qty_bucket(prefix: str, qty: float | None) -> str:
    if qty is None:
        return f"{prefix}_unknown"
    if qty <= DUST:
        return f"{prefix}_zero"
    if qty <= 1.0:
        return f"{prefix}_le_1"
    if qty <= 2.0:
        return f"{prefix}_1_2"
    if qty < 5.0:
        return f"{prefix}_2_5"
    if abs(qty - 5.0) <= 1e-9:
        return f"{prefix}_eq_5"
    return f"{prefix}_gt_5"


def deficit_qty_bucket(same_qty: float | None, opp_qty: float | None) -> str:
    if same_qty is None or opp_qty is None:
        return "deficit_unknown"
    deficit = opp_qty - same_qty
    if deficit <= DUST:
        return "deficit_le_0"
    if deficit <= 0.25 + 1e-12:
        return "deficit_0_0_25"
    if deficit <= 1.0 + 1e-12:
        return "deficit_0_25_1"
    if deficit <= 2.0 + 1e-12:
        return "deficit_1_2"
    return "deficit_gt_2"


def age_ms_bucket(prefix: str, age_ms: int | float | None) -> str:
    if age_ms is None:
        return f"{prefix}_unknown"
    if age_ms < 0:
        return f"{prefix}_negative"
    if age_ms <= 1_000:
        return f"{prefix}_0_1s"
    if age_ms <= 5_000:
        return f"{prefix}_1_5s"
    if age_ms <= 15_000:
        return f"{prefix}_5_15s"
    if age_ms <= 60_000:
        return f"{prefix}_15_60s"
    return f"{prefix}_gt_60s"


def add_count(hist: dict[str, float], key: str, amount: float = 1.0) -> None:
    hist[key] = round(hist.get(key, 0.0) + amount, 6)


def add_nested_count(hist: dict[str, dict[str, float]], key: str, subkey: str, amount: float = 1.0) -> None:
    bucket = hist.setdefault(key, {})
    bucket[subkey] = round(bucket.get(subkey, 0.0) + amount, 6)


def merge_count_hist(dest: dict[str, float], src: dict[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, (int, float)):
            add_count(dest, str(key), float(value))


def merge_nested_count_hist(dest: dict[str, dict[str, float]], src: dict[str, Any]) -> None:
    for key, bucket in src.items():
        if not isinstance(bucket, dict):
            continue
        for subkey, value in bucket.items():
            if isinstance(value, (int, float)):
                add_nested_count(dest, str(key), str(subkey), float(value))


def risk_adjusted_bucket(value: float) -> str:
    return "risk_adjusted_nonnegative" if value >= 0.0 else "risk_adjusted_negative"


def ledger_proxy_bucket(value: float | None) -> str:
    if value is None:
        return "ledger_proxy_unknown"
    if value < -1.0:
        return "ledger_proxy_lt_m1"
    if value < 0.0:
        return "ledger_proxy_m1_0"
    if value < 1.0:
        return "ledger_proxy_0_1"
    return "ledger_proxy_ge_1"


def inventory_risk_direction(same_qty: float | None, opp_qty: float | None) -> str:
    if same_qty is None or opp_qty is None:
        return "unknown"
    return "risk_increasing" if same_qty + DUST >= opp_qty else "repair_or_pairing_improving"


def is_micro_deficit_repair_context(
    same_qty: float | None,
    opp_qty: float | None,
    max_deficit_qty: float,
    open_qty_cap: float,
) -> bool:
    if same_qty is None or opp_qty is None:
        return False
    deficit = opp_qty - same_qty
    return 0.0 < deficit <= max_deficit_qty + 1e-12 and same_qty + opp_qty <= open_qty_cap + 1e-12


def side_offset_risk_key(side: str | None, offset_s: float | None, risk_direction: str) -> str:
    return f"{side or 'unknown'}|{offset_bucket(offset_s)}|{risk_direction}"


def source_opportunity_marker_key(
    side: str | None,
    offset_s: float | None,
    risk_direction: str,
    same_qty: float | None,
    opp_qty: float | None,
) -> str:
    open_qty = None if same_qty is None or opp_qty is None else same_qty + opp_qty
    return "|".join(
        (
            side or "unknown",
            offset_bucket(offset_s),
            risk_direction,
            qty_bucket("open_qty", open_qty),
            deficit_qty_bucket(same_qty, opp_qty),
        )
    )


def ledger_after_marker_bucket(value: float | None) -> str:
    if value is None:
        return "after_unknown"
    if value < -2.0:
        return "after_lt_m2"
    if value < -1.0:
        return "after_m2_m1"
    if value < -0.25:
        return "after_m1_m025"
    if value < 0.0:
        return "after_m025_0"
    if value < 0.25:
        return "after_0_025"
    if value < 1.0:
        return "after_025_1"
    return "after_gte_1"


def ledger_before_marker_bucket(value: float | None) -> str:
    if value is None:
        return "before_unknown"
    if value < -2.0:
        return "before_lt_m2"
    if value < -1.0:
        return "before_m2_m1"
    if value < -0.25:
        return "before_m1_m025"
    if value < 0.0:
        return "before_m025_0"
    if value < 0.25:
        return "before_0_025"
    if value < 1.0:
        return "before_025_1"
    return "before_gte_1"


def ledger_delta_marker_bucket(before: float | None, after: float | None) -> str:
    if before is None or after is None:
        return "delta_unknown"
    delta = after - before
    if delta < -2.0:
        return "delta_lt_m2"
    if delta < -1.0:
        return "delta_m2_m1"
    if delta < -0.25:
        return "delta_m1_m025"
    if delta < 0.0:
        return "delta_m025_0"
    if delta < 0.25:
        return "delta_0_025"
    if delta < 1.0:
        return "delta_025_1"
    return "delta_gte_1"


def source_opportunity_ledger_marker_key(
    side: str | None,
    offset_s: float | None,
    risk_direction: str,
    same_qty: float | None,
    opp_qty: float | None,
    ledger_proxy_after: float | None,
) -> str:
    return "|".join(
        (
            source_opportunity_marker_key(side, offset_s, risk_direction, same_qty, opp_qty),
            ledger_after_marker_bucket(ledger_proxy_after),
        )
    )


def source_opportunity_ledger_before_marker_key(
    side: str | None,
    offset_s: float | None,
    risk_direction: str,
    same_qty: float | None,
    opp_qty: float | None,
    ledger_proxy_before: float | None,
) -> str:
    return "|".join(
        (
            source_opportunity_marker_key(side, offset_s, risk_direction, same_qty, opp_qty),
            ledger_before_marker_bucket(ledger_proxy_before),
        )
    )


def source_opportunity_ledger_delta_marker_key(
    side: str | None,
    offset_s: float | None,
    risk_direction: str,
    same_qty: float | None,
    opp_qty: float | None,
    ledger_proxy_before: float | None,
    ledger_proxy_after: float | None,
) -> str:
    return "|".join(
        (
            source_opportunity_marker_key(side, offset_s, risk_direction, same_qty, opp_qty),
            ledger_delta_marker_bucket(ledger_proxy_before, ledger_proxy_after),
        )
    )


def closed_cycle_open_bucket(same_qty: float | None, opp_qty: float | None) -> str:
    if same_qty is None or opp_qty is None:
        return "open_unknown"
    open_qty = same_qty + opp_qty
    if open_qty <= 1.0 + DUST:
        return "open_le_1"
    if open_qty <= 2.5 + DUST:
        return "open_1_2_5"
    if open_qty <= 5.0 + DUST:
        return "open_2_5"
    return "open_gt_5"


def closed_cycle_balance_bucket(same_qty: float | None, opp_qty: float | None) -> str:
    if same_qty is None or opp_qty is None:
        return "balance_unknown"
    deficit = opp_qty - same_qty
    if deficit <= DUST:
        return "deficit_le_0"
    if deficit <= 0.25 + 1e-12:
        return "deficit_0_0_25"
    if deficit <= 1.25 + 1e-12:
        return "deficit_0_25_1_25"
    if deficit <= 2.0 + 1e-12:
        return "deficit_1_25_2"
    return "deficit_gt_2"


def closed_cycle_preseed_sides_bucket(same_qty: float | None, opp_qty: float | None) -> str:
    if same_qty is None or opp_qty is None:
        return "preseed_sides_unknown"
    if same_qty > DUST and opp_qty > DUST:
        return "both_preseed_sides"
    if same_qty > DUST:
        return "same_only_preseed"
    if opp_qty > DUST:
        return "opp_only_preseed"
    return "empty_preseed"


def closed_cycle_qty_bucket(prefix: str, qty: float | None) -> str:
    if qty is None:
        return f"{prefix}_unknown"
    if qty <= DUST:
        return f"{prefix}_zero"
    if qty <= 5.0 + DUST:
        return f"{prefix}_le_5"
    if qty <= 10.0 + DUST:
        return f"{prefix}_5_10"
    if qty <= 25.0 + DUST:
        return f"{prefix}_10_25"
    return f"{prefix}_gt_25"


def source_opportunity_closed_cycle_marker_key(
    side: str | None,
    offset_s: float | None,
    risk_direction: str,
    same_qty: float | None,
    opp_qty: float | None,
) -> str:
    return "|".join(
        (
            side or "unknown",
            offset_bucket(offset_s),
            risk_direction,
            closed_cycle_open_bucket(same_qty, opp_qty),
            closed_cycle_balance_bucket(same_qty, opp_qty),
            closed_cycle_preseed_sides_bucket(same_qty, opp_qty),
            closed_cycle_qty_bucket("same", same_qty),
            closed_cycle_qty_bucket("opp", opp_qty),
        )
    )


def merge_portfolio_ledger_diagnostics(dest: dict[str, Any], src: dict[str, Any]) -> None:
    dest["condition_count"] = int(dest.get("condition_count", 0)) + 1
    numeric_keys = (
        "seed_actions",
        "queue_supported_fills",
        "pair_actions",
        "pair_qty",
        "pair_cost_sum",
        "net_pair_cost_sum",
        "pair_pnl",
        "official_taker_fee_proxy",
        "residual_qty",
        "residual_cost",
        "stress_cost_proxy",
    )
    for key in numeric_keys:
        value = src.get(key)
        if isinstance(value, (int, float)):
            dest[key] = round(float(dest.get(key, 0.0)) + float(value), 6)
    for key in ("residual_qty_by_side", "residual_cost_by_side", "residual_lot_count_by_side"):
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    bucket = str(src.get("risk_adjusted_bucket") or risk_adjusted_bucket(float(src.get("conservative_risk_adjusted_proxy") or 0.0)))
    add_count(dest.setdefault("condition_count_by_risk_adjusted_bucket", {}), bucket)


def finalize_portfolio_ledger_diagnostics(diag: dict[str, Any]) -> None:
    pair_qty = float(diag.get("pair_qty") or 0.0)
    pair_cost_sum = float(diag.get("pair_cost_sum") or 0.0)
    net_pair_cost_sum = float(diag.get("net_pair_cost_sum") or 0.0)
    pair_pnl = float(diag.get("pair_pnl") or 0.0)
    fee = float(diag.get("official_taker_fee_proxy") or 0.0)
    residual_cost = float(diag.get("residual_cost") or 0.0)
    residual_qty = float(diag.get("residual_qty") or 0.0)
    stress = 0.01 * (2.0 * pair_qty + residual_qty)
    risk_adjusted = pair_pnl - fee - residual_cost - stress
    diag["avg_pair_cost"] = round(pair_cost_sum / pair_qty, 6) if pair_qty else 0.0
    diag["avg_net_pair_cost"] = round(net_pair_cost_sum / pair_qty, 6) if pair_qty else 0.0
    diag["stress_cost_proxy"] = round(stress, 6)
    diag["conservative_risk_adjusted_proxy"] = round(risk_adjusted, 6)
    diag["risk_adjusted_nonnegative"] = risk_adjusted >= 0.0
    diag["risk_adjusted_bucket"] = risk_adjusted_bucket(risk_adjusted)


def merge_source_link_transition_diagnostics(dest: dict[str, Any], src: dict[str, Any]) -> None:
    count_keys = (
        "transition_count_by_status",
        "transition_count_by_reason",
        "transition_count_by_side",
        "transition_count_by_offset_bucket",
        "immediate_pair_action_count_by_source_side_offset",
        "immediate_pair_action_count_by_source_side_offset_risk_direction",
        "residual_qty_by_source_side_offset",
        "residual_cost_by_source_side_offset",
        "residual_count_by_source_side_offset",
        "residual_qty_by_source_side_offset_risk_direction",
        "residual_cost_by_source_side_offset_risk_direction",
        "residual_count_by_source_side_offset_risk_direction",
    )
    nested_keys = (
        "transition_count_by_status_reason",
        "transition_count_by_status_side",
        "transition_count_by_status_offset_bucket",
        "transition_count_by_risk_direction",
        "transition_count_by_status_side_offset_risk_direction",
        "quote_intent_presence_by_status",
        "source_order_presence_by_status",
        "source_sequence_presence_by_status",
        "pre_seed_same_qty_bucket_by_status_reason",
        "pre_seed_opp_qty_bucket_by_status_reason",
        "pre_seed_same_cost_bucket_by_status_reason",
        "pre_seed_opp_cost_bucket_by_status_reason",
        "ledger_proxy_before_bucket_by_status_reason",
        "ledger_proxy_after_bucket_by_status_reason",
        "candidate_qty_bucket_by_status_reason",
        "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction",
        "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction",
        "pre_seed_same_cost_bucket_by_status_side_offset_risk_direction",
        "pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction",
        "ledger_proxy_before_bucket_by_status_side_offset_risk_direction",
        "ledger_proxy_after_bucket_by_status_side_offset_risk_direction",
        "candidate_qty_bucket_by_status_side_offset_risk_direction",
        "immediate_pair_qty_bucket_by_source_side_offset",
        "immediate_pair_cost_bucket_by_source_side_offset",
        "immediate_pair_qty_bucket_by_source_side_offset_risk_direction",
        "immediate_pair_cost_bucket_by_source_side_offset_risk_direction",
        "immediate_pair_source_quote_presence_by_side_offset",
        "immediate_pair_source_order_presence_by_side_offset",
        "residual_cost_bucket_by_source_side_offset",
        "residual_cost_bucket_by_source_side_offset_risk_direction",
        "residual_source_quote_presence_by_side_offset",
        "residual_source_order_presence_by_side_offset",
    )
    for key in count_keys:
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    for key in nested_keys:
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)


def merge_source_opportunity_marker_summary(dest: dict[str, Any], src: dict[str, Any]) -> None:
    if src.get("schema_version"):
        dest["schema_version"] = src.get("schema_version")
    if src.get("field_contract"):
        dest["field_contract"] = src.get("field_contract")
    count_keys = (
        "transition_count_by_status",
        "transition_count_by_reason",
        "transition_count_by_side_offset_risk_open_deficit",
        "micro_deficit_marker_count_by_status",
        "candidate_qty_sum_by_status_reason",
        "base_qty_sum_by_status_reason",
        "target_room_sum_by_status_reason",
        "room_cost_sum_by_status_reason",
        "imbalance_room_sum_by_status_reason",
        "transition_count_by_status_reason_side_offset_risk_open_deficit",
        "micro_deficit_marker_count_by_status_reason_side_offset_risk_open_deficit",
        "candidate_qty_sum_by_status_reason_side_offset_risk_open_deficit",
        "base_qty_sum_by_status_reason_side_offset_risk_open_deficit",
        "target_room_sum_by_status_reason_side_offset_risk_open_deficit",
        "room_cost_sum_by_status_reason_side_offset_risk_open_deficit",
        "imbalance_room_sum_by_status_reason_side_offset_risk_open_deficit",
        "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_after",
        "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_delta",
        "transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "target_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "imbalance_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    )
    nested_keys = (
        "transition_count_by_status_reason",
        "transition_count_by_status_side_offset_risk_open_deficit",
        "micro_deficit_marker_count_by_status_reason",
        "micro_deficit_marker_count_by_status_side_offset_risk_open_deficit",
        "candidate_qty_bucket_by_status_reason",
        "base_qty_bucket_by_status_reason",
        "target_room_bucket_by_status_reason",
        "room_cost_bucket_by_status_reason",
        "imbalance_room_bucket_by_status_reason",
        "pending_same_qty_bucket_by_status_reason",
        "pending_opp_qty_bucket_by_status_reason",
        "pending_same_order_count_bucket_by_status_reason",
        "pending_opp_order_count_bucket_by_status_reason",
        "opposite_seen_by_status_reason",
        "activation_opp_age_bucket_by_status_reason",
        "quote_intent_presence_by_status_reason",
        "source_order_presence_by_status_reason",
        "source_sequence_presence_by_status_reason",
        "candidate_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
        "base_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
        "target_room_bucket_by_status_reason_side_offset_risk_open_deficit",
        "room_cost_bucket_by_status_reason_side_offset_risk_open_deficit",
        "imbalance_room_bucket_by_status_reason_side_offset_risk_open_deficit",
        "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
        "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
        "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_deficit",
        "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_deficit",
        "opposite_seen_by_status_reason_side_offset_risk_open_deficit",
        "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_deficit",
        "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit",
        "source_order_presence_by_status_reason_side_offset_risk_open_deficit",
        "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit",
        "transition_count_by_status_side_offset_risk_open_deficit_ledger_after",
        "transition_count_by_status_side_offset_risk_open_deficit_ledger_before",
        "transition_count_by_status_side_offset_risk_open_deficit_ledger_delta",
        "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp",
        "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after",
        "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after",
        "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after",
        "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta",
        "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta",
        "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta",
        "candidate_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "base_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "target_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "room_cost_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "imbalance_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    )
    for key in count_keys:
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    for key in nested_keys:
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)


def merge_ce25_projected_guard_summary(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key in ("schema_version", "field_contract"):
        if key in src and key not in dest:
            dest[key] = src[key]
    for key in (
        "decision_count_by_status",
        "decision_count_by_guard",
        "decision_count_by_asset_timeframe_guard_status",
        "projected_pair_cost_bucket_by_guard",
        "projected_residual_bucket_by_guard",
        "final_window_policy_count",
    ):
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)


def merge_symmetric_activation_summary(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key in ("schema_version", "field_contract"):
        if key in src and key not in dest:
            dest[key] = src[key]
    for key in ("residual_leak_stress_cost_sum_by_status_reason_activation_bucket",):
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    for key in (
        "candidate_count_by_status_reason_activation_bucket",
        "candidate_count_by_status_reason_side_offset_activation_bucket",
        "projected_pair_cost_bucket_by_status_reason_activation_bucket",
        "projected_residual_rate_bucket_by_status_reason_activation_bucket",
        "activation_age_bucket_by_status_reason_activation_bucket",
        "source_sequence_presence_by_status_reason_activation_bucket",
    ):
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)


def merge_symmetric_activation_tail_attribution_summary(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key in ("schema_version", "field_contract"):
        if key in src and key not in dest:
            dest[key] = src[key]
    for key in (
        "pair_qty_sum_by_status_reason_activation_bucket",
        "residual_qty_sum_by_status_reason_activation_bucket",
        "residual_cost_sum_by_status_reason_activation_bucket",
        "pair_tail_loss_sum_by_status_reason_activation_bucket",
        "pair_qty_sum_by_status_reason_side_offset_activation_bucket",
        "residual_qty_sum_by_status_reason_side_offset_activation_bucket",
        "residual_cost_sum_by_status_reason_side_offset_activation_bucket",
        "pair_tail_loss_sum_by_status_reason_side_offset_activation_bucket",
    ):
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    for key in (
        "pair_cost_bucket_by_status_reason_activation_bucket",
        "source_sequence_presence_by_status_reason_activation_bucket",
        "pair_cost_bucket_by_status_reason_side_offset_activation_bucket",
        "source_sequence_presence_by_status_reason_side_offset_activation_bucket",
    ):
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)
    exemplars = src.get("residual_tail_exemplars_by_status_reason_activation_bucket")
    if isinstance(exemplars, list):
        dest.setdefault("residual_tail_exemplars_by_status_reason_activation_bucket", []).extend(
            item for item in exemplars if isinstance(item, dict)
        )


def merge_symmetric_activation_projected_residual_tail_join_summary(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key in ("schema_version", "field_contract"):
        if key in src and key not in dest:
            dest[key] = src[key]
    for key in (
        "pair_qty_sum_by_status_reason_projected_residual_bucket",
        "residual_qty_sum_by_status_reason_projected_residual_bucket",
        "residual_cost_sum_by_status_reason_projected_residual_bucket",
        "residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket",
    ):
        source = src.get(key)
        if isinstance(source, dict):
            merge_count_hist(dest.setdefault(key, {}), source)
    for key in ("source_sequence_presence_by_status_reason_projected_residual_bucket",):
        source = src.get(key)
        if isinstance(source, dict):
            merge_nested_count_hist(dest.setdefault(key, {}), source)


def event_time_ms(msg: dict[str, Any], fallback_ts_ms: int) -> int:
    value = msg.get("event_time_ms") or msg.get("market_event_time_ms") or msg.get("ts_ms")
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback_ts_ms


@dataclass(frozen=True)
class RunnerConfig:
    edge: float = 0.040
    queue_share: float = 0.50
    target_qty: float = 5.0
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 80.0
    seed_l1_cap: float = 1.02
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 120.0
    late_target_after_s: float | None = None
    late_target_qty: float | None = None
    late_repair_after_s: float | None = None
    late_repair_only_after_s: float | None = None
    late_repair_fill_to_balance_after_s: float | None = None
    micro_deficit_repair_guard: bool = False
    micro_deficit_repair_max_deficit_qty: float = 0.25
    micro_deficit_repair_open_qty_cap: float = 1.0
    cooldown_ms: int = 5_000
    order_ttl_ms: int = 120_000
    imbalance_qty_cap: float = 2.0
    imbalance_cost_cap: float = 1_000_000_000.0
    pairing_only_when_residual: bool = False
    activation_mode: str = "none"
    activation_window_s: float = 60.0
    dust_qty: float = 1.0
    taker_fee_rate: float = 0.07
    salvage_net_cap: float = 0.95
    salvage_age_ms: int = 30_000
    salvage_min_lot_cost: float = 0.25
    max_salvage_qty: float = 250.0
    event_lite_summary: bool = False
    pair_source_event_lite_summary: bool = False
    fill_to_balance_diagnostic_event_lite_summary: bool = False
    portfolio_ledger_event_lite_summary: bool = False
    source_link_transition_event_lite_summary: bool = False
    source_link_residual_tail_exemplars_event_lite_summary: bool = False
    source_opportunity_marker_event_lite_summary: bool = False
    source_opportunity_marker_reason_source_event_lite_summary: bool = False
    source_opportunity_ledger_marker_event_lite_summary: bool = False
    source_opportunity_ledger_before_delta_marker_event_lite_summary: bool = False
    source_opportunity_closed_cycle_marker_event_lite_summary: bool = False
    ce25_projected_guard_event_lite_summary: bool = False
    symmetric_activation_event_lite_summary: bool = False
    symmetric_activation_tail_attribution_event_lite_summary: bool = False
    symmetric_activation_projected_residual_tail_join_event_lite_summary: bool = False

    def target_for(self, offset_s: float | None) -> float:
        if (
            offset_s is not None
            and self.late_target_after_s is not None
            and self.late_target_qty is not None
            and offset_s >= self.late_target_after_s
        ):
            return min(self.target_qty, self.late_target_qty)
        return self.target_qty

    def late_target_active(self, offset_s: float | None) -> bool:
        return (
            offset_s is not None
            and self.late_target_after_s is not None
            and self.late_target_qty is not None
            and offset_s >= self.late_target_after_s
        )

    def late_repair_active(self, offset_s: float | None) -> bool:
        return offset_s is not None and self.late_repair_after_s is not None and offset_s >= self.late_repair_after_s

    def late_repair_only_active(self, offset_s: float | None) -> bool:
        return offset_s is not None and self.late_repair_only_after_s is not None and offset_s >= self.late_repair_only_after_s

    def late_repair_fill_to_balance_active(self, offset_s: float | None) -> bool:
        return (
            offset_s is not None
            and self.late_repair_fill_to_balance_after_s is not None
            and offset_s >= self.late_repair_fill_to_balance_after_s
        )


@dataclass
class VirtualOrder:
    id: int
    quote_intent_id: str
    condition_id: str
    side: str
    px: float
    qty: float
    created_ms: int
    accepted_ms: int
    offset_s: float
    trigger_px: float
    trigger_size: float
    trigger_ts_ms: int
    trigger_source_sequence_id: Any | None = None
    opposite_trigger_ts_ms: int | None = None
    source_risk_direction: str = "unknown"
    micro_deficit_repair_guard_candidate: bool = False
    micro_deficit_repair_deficit: float | None = None
    pre_seed_same_qty: float | None = None
    pre_seed_opp_qty: float | None = None
    pre_seed_same_cost: float | None = None
    pre_seed_opp_cost: float | None = None
    ledger_proxy_before: float | None = None
    ledger_proxy_after: float | None = None
    activation_required: bool = False
    activation_opp_age_ms: int | None = None
    opposite_seen_ms: int | None = None
    symmetric_activation_status: str = "admitted"
    symmetric_activation_reason: str = "candidate"
    symmetric_projected_pair_cost_bucket: str = "projected_pair_cost_unknown"
    symmetric_projected_residual_bucket: str = "projected_residual_unknown"
    queue_credit: float = 0.0
    first_bid_touch_ms: int | None = None
    first_trade_touch_ms: int | None = None
    fill_ms: int | None = None
    cancel_ms: int | None = None
    cancel_reason: str | None = None


@dataclass
class Lot:
    id: int
    quote_intent_id: str
    side: str
    qty: float
    px: float
    fill_ms: int
    source_order_id: int
    trigger_px: float | None = None
    trigger_size: float | None = None
    offset_s: float | None = None
    trigger_ts_ms: int | None = None
    trigger_source_sequence_id: Any | None = None
    source_risk_direction: str = "unknown"
    micro_deficit_repair_guard_candidate: bool = False
    micro_deficit_repair_deficit: float | None = None
    pre_seed_same_qty: float | None = None
    pre_seed_opp_qty: float | None = None
    pre_seed_same_cost: float | None = None
    pre_seed_opp_cost: float | None = None
    ledger_proxy_before: float | None = None
    ledger_proxy_after: float | None = None
    activation_required: bool = False
    activation_opp_age_ms: int | None = None
    opposite_seen_ms: int | None = None
    symmetric_activation_status: str = "admitted"
    symmetric_activation_reason: str = "candidate"
    symmetric_projected_pair_cost_bucket: str = "projected_pair_cost_unknown"
    symmetric_projected_residual_bucket: str = "projected_residual_unknown"
    source_pair_qty: float = 0.0
    source_pair_cost: float = 0.0
    source_pair_pnl: float = 0.0

    @property
    def cost(self) -> float:
        return self.qty * self.px


@dataclass
class Metrics:
    candidates: int = 0
    cancelled_orders: int = 0
    queue_supported_fills: int = 0
    touch_only_orders: int = 0
    pair_actions: int = 0
    salvage_actions: int = 0
    material_lockout_blocks: int = 0
    seed_qty: float = 0.0
    seed_cost: float = 0.0
    filled_qty: float = 0.0
    filled_cost: float = 0.0
    pair_qty: float = 0.0
    pair_cost_sum: float = 0.0
    net_pair_cost_sum: float = 0.0
    pair_pnl: float = 0.0
    taker_fee: float = 0.0
    completion_cost: float = 0.0
    salvage_qty: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    late_repair_fill_to_balance_candidates: int = 0
    late_repair_fill_to_balance_caps: int = 0
    late_repair_fill_to_balance_blocks: int = 0
    late_repair_fill_to_balance_qty_reduction: float = 0.0
    micro_deficit_repair_guard_candidates: int = 0
    micro_deficit_repair_guard_blocks: int = 0
    pair_costs: list[float] = field(default_factory=list)
    net_pair_costs: list[float] = field(default_factory=list)
    fill_wait_ms: list[int] = field(default_factory=list)
    pair_wait_ms: list[int] = field(default_factory=list)
    salvage_wait_ms: list[int] = field(default_factory=list)


class DPlusRunner:
    def __init__(self, slug: str, out_dir: Path, cfg: RunnerConfig, condition_id: str | None = None) -> None:
        self.slug = slug
        self.condition_id = condition_id or slug
        self.start_s = round_start_from_slug(slug)
        self.out_dir = out_dir
        self.cfg = cfg
        self.book: dict[str, float] = {}
        self.pending: list[VirtualOrder] = []
        self.lots: dict[str, list[Lot]] = {"YES": [], "NO": []}
        self.metrics = Metrics()
        self.book_ticks = 0
        self.trade_ticks = 0
        self.sell_triggers = 0
        self.blocked: dict[str, int] = {}
        self.next_order_id = 1
        self.next_lot_id = 1
        self.last_seed_ms = -(10**18)
        self.activation_last_seen_ms: dict[str, int | None] = {"YES": None, "NO": None}
        self.events_path = out_dir / f"{slug}.events.jsonl"
        self.summary_path = out_dir / f"{slug}.summary.json"
        self.event_lite_candidate_seed_px_buckets: dict[str, float] = {}
        self.event_lite_candidate_public_trade_px_buckets: dict[str, float] = {}
        self.event_lite_candidate_side_counts: dict[str, float] = {}
        self.event_lite_candidate_offset_buckets: dict[str, float] = {}
        self.event_lite_candidate_qty_by_seed_px_bucket: dict[str, float] = {}
        self.event_lite_fill_seed_px_buckets: dict[str, float] = {}
        self.event_lite_fill_side_counts: dict[str, float] = {}
        self.event_lite_block_by_reason_side: dict[str, dict[str, float]] = {}
        self.event_lite_block_by_reason_public_px_bucket: dict[str, dict[str, float]] = {}
        self.event_lite_block_by_reason_offset_bucket: dict[str, dict[str, float]] = {}
        self.event_lite_pair_source_action_count = 0
        self.event_lite_pair_source_record_count = 0
        self.event_lite_pair_cost_by_source_seed_px_bucket: dict[str, dict[str, float]] = {}
        self.event_lite_pair_cost_by_source_public_px_bucket: dict[str, dict[str, float]] = {}
        self.event_lite_pair_cost_by_source_offset_bucket: dict[str, dict[str, float]] = {}
        self.event_lite_pair_cost_by_source_side: dict[str, dict[str, float]] = {}
        self.event_lite_top_high_cost_pair_sources: list[dict[str, Any]] = []
        self.event_lite_f2b_diagnostics: dict[str, Any] = {
            "candidate_count_by_side": {},
            "candidate_count_by_offset_bucket": {},
            "candidate_count_by_side_offset": {},
            "status_count_by_side": {},
            "status_count_by_offset_bucket": {},
            "status_count_by_side_offset": {},
            "deficit_bucket_by_side_offset": {},
            "base_seed_qty_bucket_by_side_offset": {},
            "capped_qty_bucket_by_side_offset": {},
            "qty_reduction_bucket_by_side_offset": {},
            "qty_reduction_amount_by_side": {},
            "qty_reduction_amount_by_offset_bucket": {},
            "qty_reduction_amount_by_side_offset": {},
        }
        self.event_lite_source_link_transitions: dict[str, Any] = {
            "transition_count_by_status": {},
            "transition_count_by_reason": {},
            "transition_count_by_status_reason": {},
            "transition_count_by_side": {},
            "transition_count_by_offset_bucket": {},
            "transition_count_by_status_side": {},
            "transition_count_by_status_offset_bucket": {},
            "transition_count_by_risk_direction": {},
            "transition_count_by_status_side_offset_risk_direction": {},
            "quote_intent_presence_by_status": {},
            "source_order_presence_by_status": {},
            "source_sequence_presence_by_status": {},
            "pre_seed_same_qty_bucket_by_status_reason": {},
            "pre_seed_opp_qty_bucket_by_status_reason": {},
            "pre_seed_same_cost_bucket_by_status_reason": {},
            "pre_seed_opp_cost_bucket_by_status_reason": {},
            "ledger_proxy_before_bucket_by_status_reason": {},
            "ledger_proxy_after_bucket_by_status_reason": {},
            "candidate_qty_bucket_by_status_reason": {},
            "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction": {},
            "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction": {},
            "pre_seed_same_cost_bucket_by_status_side_offset_risk_direction": {},
            "pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction": {},
            "ledger_proxy_before_bucket_by_status_side_offset_risk_direction": {},
            "ledger_proxy_after_bucket_by_status_side_offset_risk_direction": {},
            "candidate_qty_bucket_by_status_side_offset_risk_direction": {},
            "immediate_pair_action_count_by_source_side_offset": {},
            "immediate_pair_action_count_by_source_side_offset_risk_direction": {},
            "immediate_pair_qty_bucket_by_source_side_offset": {},
            "immediate_pair_cost_bucket_by_source_side_offset": {},
            "immediate_pair_qty_bucket_by_source_side_offset_risk_direction": {},
            "immediate_pair_cost_bucket_by_source_side_offset_risk_direction": {},
            "immediate_pair_source_quote_presence_by_side_offset": {},
            "immediate_pair_source_order_presence_by_side_offset": {},
        }
        self.event_lite_source_opportunity_markers: dict[str, Any] = {
            "schema_version": "source_opportunity_marker_summary_v1",
            "transition_count_by_status": {},
            "transition_count_by_reason": {},
            "transition_count_by_status_reason": {},
            "transition_count_by_side_offset_risk_open_deficit": {},
            "transition_count_by_status_side_offset_risk_open_deficit": {},
            "micro_deficit_marker_count_by_status": {},
            "micro_deficit_marker_count_by_status_reason": {},
            "micro_deficit_marker_count_by_status_side_offset_risk_open_deficit": {},
            "candidate_qty_sum_by_status_reason": {},
            "base_qty_sum_by_status_reason": {},
            "target_room_sum_by_status_reason": {},
            "room_cost_sum_by_status_reason": {},
            "imbalance_room_sum_by_status_reason": {},
            "candidate_qty_bucket_by_status_reason": {},
            "base_qty_bucket_by_status_reason": {},
            "target_room_bucket_by_status_reason": {},
            "room_cost_bucket_by_status_reason": {},
            "imbalance_room_bucket_by_status_reason": {},
            "pending_same_qty_bucket_by_status_reason": {},
            "pending_opp_qty_bucket_by_status_reason": {},
            "pending_same_order_count_bucket_by_status_reason": {},
            "pending_opp_order_count_bucket_by_status_reason": {},
            "opposite_seen_by_status_reason": {},
            "activation_opp_age_bucket_by_status_reason": {},
            "quote_intent_presence_by_status_reason": {},
            "source_order_presence_by_status_reason": {},
            "source_sequence_presence_by_status_reason": {},
            "field_contract": {
                "live_pre_action_fields": [
                    "status",
                    "reason",
                    "side",
                    "offset_bucket",
                    "source_risk_direction",
                    "open_qty_bucket",
                    "deficit_bucket",
                    "candidate_qty",
                    "base_qty",
                    "target_room",
                    "room_cost",
                    "imbalance_room",
                    "pending_same_qty",
                    "pending_opp_qty",
                    "activation_opp_age_ms",
                    "opposite_seen",
                ],
                "post_action_outcome_labels_included": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
        }
        self.event_lite_ce25_projected_guard_summary: dict[str, Any] = {
            "schema_version": "ce25_projected_guard_summary_v1",
            "field_contract": {
                "default_off": True,
                "source": "pre_action_inventory_plus_intended_no_order_seed",
                "estimated_fee_per_share_policy": "0.0_for_passive_no_order_seed_proxy_only",
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
            "decision_count_by_status": {},
            "decision_count_by_guard": {},
            "decision_count_by_asset_timeframe_guard_status": {},
            "projected_pair_cost_bucket_by_guard": {},
            "projected_residual_bucket_by_guard": {},
            "final_window_policy_count": {},
        }
        self.event_lite_symmetric_activation_summary: dict[str, Any] = {
            "schema_version": "symmetric_activation_contract_summary_v1",
            "field_contract": {
                "default_off": True,
                "source": "pre_action_activation_opposite_seen_plus_inventory",
                "selected_edge": 0.07,
                "selected_activation_window_s": 7.5,
                "selected_min_opp_count": 1,
                "selected_leak_rate": 0.02,
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
            "candidate_count_by_status_reason_activation_bucket": {},
            "candidate_count_by_status_reason_side_offset_activation_bucket": {},
            "projected_pair_cost_bucket_by_status_reason_activation_bucket": {},
            "projected_residual_rate_bucket_by_status_reason_activation_bucket": {},
            "activation_age_bucket_by_status_reason_activation_bucket": {},
            "source_sequence_presence_by_status_reason_activation_bucket": {},
            "residual_leak_stress_cost_sum_by_status_reason_activation_bucket": {},
        }
        self.event_lite_symmetric_activation_tail_attribution_summary: dict[str, Any] = {
            "schema_version": "symmetric_activation_tail_attribution_summary_v1",
            "field_contract": {
                "default_off": True,
                "source": "source_attributed_realized_pair_residual_tail_joined_to_pre_action_activation_context",
                "status_reason_scope": ["admitted|candidate"],
                "join_axes": [
                    "status|reason|activation_bucket",
                    "status|reason|side|offset_bucket|activation_bucket",
                ],
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
            "pair_qty_sum_by_status_reason_activation_bucket": {},
            "pair_cost_bucket_by_status_reason_activation_bucket": {},
            "residual_qty_sum_by_status_reason_activation_bucket": {},
            "residual_cost_sum_by_status_reason_activation_bucket": {},
            "pair_tail_loss_sum_by_status_reason_activation_bucket": {},
            "source_sequence_presence_by_status_reason_activation_bucket": {},
            "pair_qty_sum_by_status_reason_side_offset_activation_bucket": {},
            "pair_cost_bucket_by_status_reason_side_offset_activation_bucket": {},
            "residual_qty_sum_by_status_reason_side_offset_activation_bucket": {},
            "residual_cost_sum_by_status_reason_side_offset_activation_bucket": {},
            "pair_tail_loss_sum_by_status_reason_side_offset_activation_bucket": {},
            "source_sequence_presence_by_status_reason_side_offset_activation_bucket": {},
            "residual_tail_exemplars_by_status_reason_activation_bucket": [],
        }
        self.event_lite_symmetric_activation_projected_residual_tail_join_summary: dict[str, Any] = {
            "schema_version": "symmetric_activation_projected_residual_tail_join_summary_v1",
            "field_contract": {
                "default_off": True,
                "source": "source_attributed_realized_pair_residual_tail_joined_to_pre_action_projected_residual_bucket",
                "status_reason_scope": ["admitted|candidate"],
                "join_axes": [
                    "status|reason|projected_residual_bucket",
                    "status|reason|side|offset_bucket|projected_residual_bucket",
                ],
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
            "pair_qty_sum_by_status_reason_projected_residual_bucket": {},
            "residual_qty_sum_by_status_reason_projected_residual_bucket": {},
            "residual_cost_sum_by_status_reason_projected_residual_bucket": {},
            "residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket": {},
            "source_sequence_presence_by_status_reason_projected_residual_bucket": {},
        }
        if self.cfg.source_opportunity_ledger_marker_event_lite_summary:
            self.event_lite_source_opportunity_markers["field_contract"][
                "ledger_marker_schema_version"
            ] = "source_opportunity_ledger_marker_v1"
            self.event_lite_source_opportunity_markers["field_contract"][
                "ledger_marker_join_key"
            ] = "status|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket|ledger_after_bucket"
            self.event_lite_source_opportunity_markers["field_contract"]["live_pre_action_fields"].append(
                "ledger_proxy_after_bucket"
            )
            self.event_lite_source_opportunity_markers.update(
                {
                    "transition_count_by_status_side_offset_risk_open_deficit_ledger_after": {},
                    "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_after": {},
                    "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after": {},
                    "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after": {},
                    "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after": {},
                }
            )
        if self.cfg.source_opportunity_ledger_before_delta_marker_event_lite_summary:
            self.event_lite_source_opportunity_markers["field_contract"][
                "ledger_before_delta_marker_schema_version"
            ] = "source_opportunity_ledger_before_delta_marker_v1"
            self.event_lite_source_opportunity_markers["field_contract"][
                "ledger_before_marker_join_key"
            ] = "status|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket|ledger_before_bucket"
            self.event_lite_source_opportunity_markers["field_contract"][
                "ledger_delta_marker_join_key"
            ] = "status|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket|ledger_delta_bucket"
            self.event_lite_source_opportunity_markers["field_contract"]["live_pre_action_fields"].extend(
                ["ledger_proxy_before_bucket", "ledger_proxy_delta_bucket_when_available"]
            )
            self.event_lite_source_opportunity_markers.update(
                {
                    "transition_count_by_status_side_offset_risk_open_deficit_ledger_before": {},
                    "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before": {},
                    "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before": {},
                    "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before": {},
                    "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before": {},
                    "transition_count_by_status_side_offset_risk_open_deficit_ledger_delta": {},
                    "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_delta": {},
                    "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta": {},
                    "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta": {},
                    "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta": {},
                }
            )
        if self.cfg.source_opportunity_closed_cycle_marker_event_lite_summary:
            self.event_lite_source_opportunity_markers["field_contract"][
                "closed_cycle_marker_schema_version"
            ] = "source_opportunity_closed_cycle_marker_v1"
            self.event_lite_source_opportunity_markers["field_contract"][
                "closed_cycle_marker_join_key"
            ] = (
                "status|reason|side|offset_bucket|source_risk_direction|open_bucket|balance_bucket|"
                "preseed_sides|same_qty_bucket|opp_qty_bucket"
            )
            self.event_lite_source_opportunity_markers["field_contract"][
                "post_action_pair_residual_labels_included"
            ] = False
            self.event_lite_source_opportunity_markers["field_contract"][
                "gross_pair_pnl_available_at_marker"
            ] = False
            self.event_lite_source_opportunity_markers["field_contract"][
                "residual_stress_available_at_marker"
            ] = False
            self.event_lite_source_opportunity_markers["field_contract"][
                "requires_post_run_scorer_join_for_pnl"
            ] = True
            self.event_lite_source_opportunity_markers["field_contract"]["live_pre_action_fields"].extend(
                [
                    "closed_cycle_open_bucket",
                    "closed_cycle_balance_bucket",
                    "closed_cycle_preseed_sides",
                    "closed_cycle_same_qty_bucket",
                    "closed_cycle_opp_qty_bucket",
                ]
            )
            self.event_lite_source_opportunity_markers.update(
                {
                    "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp": {},
                    "transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "target_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "imbalance_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "candidate_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "base_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "target_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "room_cost_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "imbalance_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                    "source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp": {},
                }
            )
        if self.cfg.source_opportunity_marker_reason_source_event_lite_summary:
            self.event_lite_source_opportunity_markers["field_contract"][
                "reason_source_coverage_schema_version"
            ] = "source_opportunity_marker_reason_source_coverage_v1"
            self.event_lite_source_opportunity_markers["field_contract"][
                "exact_reason_source_join_key"
            ] = "status|reason|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket"
            self.event_lite_source_opportunity_markers["field_contract"][
                "raw_quote_order_sequence_ids_included"
            ] = False
            self.event_lite_source_opportunity_markers.update(
                {
                    "transition_count_by_status_reason_side_offset_risk_open_deficit": {},
                    "micro_deficit_marker_count_by_status_reason_side_offset_risk_open_deficit": {},
                    "candidate_qty_sum_by_status_reason_side_offset_risk_open_deficit": {},
                    "base_qty_sum_by_status_reason_side_offset_risk_open_deficit": {},
                    "target_room_sum_by_status_reason_side_offset_risk_open_deficit": {},
                    "room_cost_sum_by_status_reason_side_offset_risk_open_deficit": {},
                    "imbalance_room_sum_by_status_reason_side_offset_risk_open_deficit": {},
                    "candidate_qty_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "base_qty_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "target_room_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "room_cost_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "imbalance_room_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "opposite_seen_by_status_reason_side_offset_risk_open_deficit": {},
                    "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_deficit": {},
                    "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit": {},
                    "source_order_presence_by_status_reason_side_offset_risk_open_deficit": {},
                    "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit": {},
                }
            )

    def quote_intent_id(self, order_id: int) -> str:
        return f"{self.slug}:quote:{order_id}"

    def blocked_quote_intent_id(self, side: str, ts_ms: int) -> str:
        return f"{self.slug}:blocked:{side}:{ts_ms}:{self.blocked.get('activation_opp_seen', 0)}"

    def offset_s(self, ts_ms: int) -> float | None:
        return ts_ms / 1000.0 - self.start_s if self.start_s else None

    def emit(self, obj: dict[str, Any]) -> None:
        with self.events_path.open("a") as f:
            f.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")

    def block(
        self,
        reason: str,
        *,
        side: str | None = None,
        public_trade_px: float | None = None,
        offset_s: float | None = None,
        qty: float | None = None,
        base_qty: float | None = None,
        target_qty: float | None = None,
        room_cost: float | None = None,
        imbalance_room: float | None = None,
        activation_opp_age_ms: int | None = None,
        opposite_seen_ms: int | None = None,
        quote_intent_id: str | None = None,
        source_order_id: int | None = None,
        source_sequence_id: Any | None = None,
    ) -> None:
        self.blocked[reason] = self.blocked.get(reason, 0) + 1
        if self.cfg.event_lite_summary:
            add_nested_count(self.event_lite_block_by_reason_side, reason, side or "unknown")
            add_nested_count(self.event_lite_block_by_reason_public_px_bucket, reason, px_bucket(public_trade_px))
            add_nested_count(self.event_lite_block_by_reason_offset_bucket, reason, offset_bucket(offset_s))
        if side in {"YES", "NO"}:
            same_qty = self.exposure_qty(side)
            opp_qty = self.exposure_qty(opp(side))
            same_cost = self.exposure_cost(side)
            opp_cost = self.exposure_cost(opp(side))
        else:
            same_qty = opp_qty = same_cost = opp_cost = None
        ledger_proxy_before = self.online_ledger_proxy()
        self.record_source_link_transition(
            status="blocked",
            reason=reason,
            side=side,
            offset_s=offset_s,
            qty=None,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost,
            opp_cost=opp_cost,
            ledger_proxy_before=ledger_proxy_before,
            quote_intent_id=quote_intent_id,
            source_order_id=source_order_id,
            source_sequence_id=source_sequence_id,
        )
        self.record_source_opportunity_marker(
            status="blocked",
            reason=reason,
            side=side,
            offset_s=offset_s,
            qty=qty,
            base_qty=base_qty,
            target_qty=target_qty,
            room_cost=room_cost,
            imbalance_room=imbalance_room,
            same_qty=same_qty,
            opp_qty=opp_qty,
            activation_opp_age_ms=activation_opp_age_ms,
            opposite_seen_ms=opposite_seen_ms,
            quote_intent_id=quote_intent_id,
            source_order_id=source_order_id,
            source_sequence_id=source_sequence_id,
            ledger_proxy_before=ledger_proxy_before,
        )

    def record_event_lite_candidate(self, side: str, seed_px: float, public_trade_px: float, offset_s: float | None, qty: float) -> None:
        if not self.cfg.event_lite_summary:
            return
        add_count(self.event_lite_candidate_seed_px_buckets, px_bucket(seed_px))
        add_count(self.event_lite_candidate_public_trade_px_buckets, px_bucket(public_trade_px))
        add_count(self.event_lite_candidate_side_counts, side)
        add_count(self.event_lite_candidate_offset_buckets, offset_bucket(offset_s))
        add_count(self.event_lite_candidate_qty_by_seed_px_bucket, px_bucket(seed_px), qty)

    def record_event_lite_fill(self, side: str, seed_px: float) -> None:
        if not self.cfg.event_lite_summary:
            return
        add_count(self.event_lite_fill_seed_px_buckets, px_bucket(seed_px))
        add_count(self.event_lite_fill_side_counts, side)

    def record_fill_to_balance_diagnostic(
        self,
        *,
        side: str,
        offset_s: float | None,
        deficit: float | None,
        base_seed_qty: float,
        capped_qty: float,
        qty_reduction: float,
        status: str,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.fill_to_balance_diagnostic_event_lite_summary):
            return
        offset_key = offset_bucket(offset_s)
        side_offset_key = f"{side}|{offset_key}"
        diag = self.event_lite_f2b_diagnostics
        add_count(diag["candidate_count_by_side"], side)
        add_count(diag["candidate_count_by_offset_bucket"], offset_key)
        add_count(diag["candidate_count_by_side_offset"], side_offset_key)
        add_nested_count(diag["status_count_by_side"], status, side)
        add_nested_count(diag["status_count_by_offset_bucket"], status, offset_key)
        add_nested_count(diag["status_count_by_side_offset"], side_offset_key, status)
        add_nested_count(diag["deficit_bucket_by_side_offset"], side_offset_key, qty_bucket("deficit", deficit))
        add_nested_count(diag["base_seed_qty_bucket_by_side_offset"], side_offset_key, qty_bucket("base_seed_qty", base_seed_qty))
        add_nested_count(diag["capped_qty_bucket_by_side_offset"], side_offset_key, qty_bucket("capped_qty", capped_qty))
        add_nested_count(diag["qty_reduction_bucket_by_side_offset"], side_offset_key, qty_bucket("qty_reduction", qty_reduction))
        if qty_reduction > DUST:
            add_count(diag["qty_reduction_amount_by_side"], side, qty_reduction)
            add_count(diag["qty_reduction_amount_by_offset_bucket"], offset_key, qty_reduction)
            add_count(diag["qty_reduction_amount_by_side_offset"], side_offset_key, qty_reduction)

    def record_source_link_transition(
        self,
        *,
        status: str,
        reason: str,
        side: str | None,
        offset_s: float | None,
        qty: float | None,
        same_qty: float | None,
        opp_qty: float | None,
        same_cost: float | None,
        opp_cost: float | None,
        ledger_proxy_before: float | None,
        ledger_proxy_after: float | None = None,
        quote_intent_id: str | None = None,
        source_order_id: int | None = None,
        source_sequence_id: Any | None = None,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.source_link_transition_event_lite_summary):
            return
        side_key = side or "unknown"
        offset_key = offset_bucket(offset_s)
        status_reason = f"{status}|{reason}"
        risk_direction = inventory_risk_direction(same_qty, opp_qty)
        cross_key = side_offset_risk_key(side, offset_s, risk_direction)
        diag = self.event_lite_source_link_transitions
        add_count(diag["transition_count_by_status"], status)
        add_count(diag["transition_count_by_reason"], reason)
        add_nested_count(diag["transition_count_by_status_reason"], status, reason)
        add_count(diag["transition_count_by_side"], side_key)
        add_count(diag["transition_count_by_offset_bucket"], offset_key)
        add_nested_count(diag["transition_count_by_status_side"], status, side_key)
        add_nested_count(diag["transition_count_by_status_offset_bucket"], status, offset_key)
        add_nested_count(diag["transition_count_by_risk_direction"], status, risk_direction)
        add_nested_count(diag["transition_count_by_status_side_offset_risk_direction"], status, cross_key)
        add_nested_count(diag["quote_intent_presence_by_status"], status, "present" if quote_intent_id else "missing")
        add_nested_count(diag["source_order_presence_by_status"], status, "present" if source_order_id is not None else "missing")
        add_nested_count(diag["source_sequence_presence_by_status"], status, "present" if source_sequence_id is not None else "missing")
        add_nested_count(diag["pre_seed_same_qty_bucket_by_status_reason"], status_reason, qty_bucket("same_qty", same_qty))
        add_nested_count(diag["pre_seed_opp_qty_bucket_by_status_reason"], status_reason, qty_bucket("opp_qty", opp_qty))
        add_nested_count(diag["pre_seed_same_cost_bucket_by_status_reason"], status_reason, qty_bucket("same_cost", same_cost))
        add_nested_count(diag["pre_seed_opp_cost_bucket_by_status_reason"], status_reason, qty_bucket("opp_cost", opp_cost))
        add_nested_count(
            diag["ledger_proxy_before_bucket_by_status_reason"],
            status_reason,
            ledger_proxy_bucket(ledger_proxy_before),
        )
        add_nested_count(
            diag["ledger_proxy_after_bucket_by_status_reason"],
            status_reason,
            ledger_proxy_bucket(ledger_proxy_after),
        )
        add_nested_count(diag["candidate_qty_bucket_by_status_reason"], status_reason, qty_bucket("candidate_qty", qty))
        add_nested_count(
            diag["pre_seed_same_qty_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            qty_bucket("same_qty", same_qty),
        )
        add_nested_count(
            diag["pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            qty_bucket("opp_qty", opp_qty),
        )
        add_nested_count(
            diag["pre_seed_same_cost_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            qty_bucket("same_cost", same_cost),
        )
        add_nested_count(
            diag["pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            qty_bucket("opp_cost", opp_cost),
        )
        add_nested_count(
            diag["ledger_proxy_before_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            ledger_proxy_bucket(ledger_proxy_before),
        )
        add_nested_count(
            diag["ledger_proxy_after_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            ledger_proxy_bucket(ledger_proxy_after),
        )
        add_nested_count(
            diag["candidate_qty_bucket_by_status_side_offset_risk_direction"],
            cross_key,
            qty_bucket("candidate_qty", qty),
        )

    def record_source_opportunity_marker(
        self,
        *,
        status: str,
        reason: str,
        side: str | None,
        offset_s: float | None,
        qty: float | None,
        base_qty: float | None,
        target_qty: float | None,
        room_cost: float | None,
        imbalance_room: float | None,
        same_qty: float | None,
        opp_qty: float | None,
        activation_opp_age_ms: int | None,
        opposite_seen_ms: int | None,
        quote_intent_id: str | None,
        source_order_id: int | None,
        source_sequence_id: Any | None,
        ledger_proxy_before: float | None = None,
        ledger_proxy_after: float | None = None,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.source_opportunity_marker_event_lite_summary):
            return
        side_key = side or "unknown"
        risk_direction = inventory_risk_direction(same_qty, opp_qty)
        marker_key = source_opportunity_marker_key(side, offset_s, risk_direction, same_qty, opp_qty)
        ledger_marker_key = source_opportunity_ledger_marker_key(
            side,
            offset_s,
            risk_direction,
            same_qty,
            opp_qty,
            ledger_proxy_after,
        )
        ledger_before_marker_key = source_opportunity_ledger_before_marker_key(
            side,
            offset_s,
            risk_direction,
            same_qty,
            opp_qty,
            ledger_proxy_before,
        )
        ledger_delta_marker_key = source_opportunity_ledger_delta_marker_key(
            side,
            offset_s,
            risk_direction,
            same_qty,
            opp_qty,
            ledger_proxy_before,
            ledger_proxy_after,
        )
        closed_cycle_marker_key = source_opportunity_closed_cycle_marker_key(
            side,
            offset_s,
            risk_direction,
            same_qty,
            opp_qty,
        )
        status_reason = f"{status}|{reason}"
        status_reason_marker = f"{status_reason}|{marker_key}"
        status_reason_ledger_marker = f"{status_reason}|{ledger_marker_key}"
        status_reason_ledger_before_marker = f"{status_reason}|{ledger_before_marker_key}"
        status_reason_ledger_delta_marker = f"{status_reason}|{ledger_delta_marker_key}"
        status_reason_closed_cycle_marker = f"{status_reason}|{closed_cycle_marker_key}"
        pending_same = self.pending_orders(side) if side in {"YES", "NO"} else []
        pending_opp = self.pending_orders(opp(side)) if side in {"YES", "NO"} else []
        pending_same_qty = sum(order.qty for order in pending_same)
        pending_opp_qty = sum(order.qty for order in pending_opp)
        target_room = None if target_qty is None or same_qty is None else target_qty - same_qty
        room_cost_value = room_cost if room_cost is not None else self.cfg.max_open_cost - self.total_open_cost()
        if imbalance_room is None and same_qty is not None and opp_qty is not None:
            imbalance_room = self.cfg.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        micro_deficit = is_micro_deficit_repair_context(
            same_qty,
            opp_qty,
            self.cfg.micro_deficit_repair_max_deficit_qty,
            self.cfg.micro_deficit_repair_open_qty_cap,
        )
        diag = self.event_lite_source_opportunity_markers
        add_count(diag["transition_count_by_status"], status)
        add_count(diag["transition_count_by_reason"], reason)
        add_nested_count(diag["transition_count_by_status_reason"], status, reason)
        add_count(diag["transition_count_by_side_offset_risk_open_deficit"], marker_key)
        add_nested_count(diag["transition_count_by_status_side_offset_risk_open_deficit"], status, marker_key)
        if micro_deficit:
            add_count(diag["micro_deficit_marker_count_by_status"], status)
            add_nested_count(diag["micro_deficit_marker_count_by_status_reason"], status, reason)
            add_nested_count(
                diag["micro_deficit_marker_count_by_status_side_offset_risk_open_deficit"],
                status,
                marker_key,
            )
        if qty is not None:
            add_count(diag["candidate_qty_sum_by_status_reason"], status_reason, max(0.0, qty))
        if base_qty is not None:
            add_count(diag["base_qty_sum_by_status_reason"], status_reason, max(0.0, base_qty))
        if target_room is not None:
            add_count(diag["target_room_sum_by_status_reason"], status_reason, max(0.0, target_room))
        if room_cost_value is not None:
            add_count(diag["room_cost_sum_by_status_reason"], status_reason, max(0.0, room_cost_value))
        if imbalance_room is not None:
            add_count(diag["imbalance_room_sum_by_status_reason"], status_reason, max(0.0, imbalance_room))
        add_nested_count(diag["candidate_qty_bucket_by_status_reason"], status_reason, qty_bucket("candidate_qty", qty))
        add_nested_count(diag["base_qty_bucket_by_status_reason"], status_reason, qty_bucket("base_qty", base_qty))
        add_nested_count(diag["target_room_bucket_by_status_reason"], status_reason, qty_bucket("target_room", target_room))
        add_nested_count(diag["room_cost_bucket_by_status_reason"], status_reason, qty_bucket("room_cost", room_cost_value))
        add_nested_count(diag["imbalance_room_bucket_by_status_reason"], status_reason, qty_bucket("imbalance_room", imbalance_room))
        add_nested_count(diag["pending_same_qty_bucket_by_status_reason"], status_reason, qty_bucket("pending_same_qty", pending_same_qty))
        add_nested_count(diag["pending_opp_qty_bucket_by_status_reason"], status_reason, qty_bucket("pending_opp_qty", pending_opp_qty))
        add_nested_count(
            diag["pending_same_order_count_bucket_by_status_reason"],
            status_reason,
            qty_bucket("pending_same_order_count", float(len(pending_same))),
        )
        add_nested_count(
            diag["pending_opp_order_count_bucket_by_status_reason"],
            status_reason,
            qty_bucket("pending_opp_order_count", float(len(pending_opp))),
        )
        add_nested_count(
            diag["opposite_seen_by_status_reason"],
            status_reason,
            "opposite_seen_present" if opposite_seen_ms is not None else "opposite_seen_missing",
        )
        add_nested_count(
            diag["activation_opp_age_bucket_by_status_reason"],
            status_reason,
            age_ms_bucket("activation_opp_age", activation_opp_age_ms),
        )
        add_nested_count(
            diag["quote_intent_presence_by_status_reason"],
            status_reason,
            "present" if quote_intent_id else "missing",
        )
        add_nested_count(
            diag["source_order_presence_by_status_reason"],
            status_reason,
            "present" if source_order_id is not None else "missing",
        )
        add_nested_count(
            diag["source_sequence_presence_by_status_reason"],
            status_reason,
            "present" if source_sequence_id is not None else "missing",
        )
        if self.cfg.source_opportunity_ledger_marker_event_lite_summary:
            add_nested_count(
                diag["transition_count_by_status_side_offset_risk_open_deficit_ledger_after"],
                status,
                ledger_marker_key,
            )
            add_count(
                diag["transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_after"],
                status_reason_ledger_marker,
            )
            add_nested_count(
                diag["quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after"],
                status_reason_ledger_marker,
                "present" if quote_intent_id else "missing",
            )
            add_nested_count(
                diag["source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after"],
                status_reason_ledger_marker,
                "present" if source_order_id is not None else "missing",
            )
            add_nested_count(
                diag["source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_after"],
                status_reason_ledger_marker,
                "present" if source_sequence_id is not None else "missing",
            )
        if self.cfg.source_opportunity_ledger_before_delta_marker_event_lite_summary:
            add_nested_count(
                diag["transition_count_by_status_side_offset_risk_open_deficit_ledger_before"],
                status,
                ledger_before_marker_key,
            )
            add_count(
                diag["transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before"],
                status_reason_ledger_before_marker,
            )
            add_nested_count(
                diag["quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before"],
                status_reason_ledger_before_marker,
                "present" if quote_intent_id else "missing",
            )
            add_nested_count(
                diag["source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before"],
                status_reason_ledger_before_marker,
                "present" if source_order_id is not None else "missing",
            )
            add_nested_count(
                diag["source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before"],
                status_reason_ledger_before_marker,
                "present" if source_sequence_id is not None else "missing",
            )
            add_nested_count(
                diag["transition_count_by_status_side_offset_risk_open_deficit_ledger_delta"],
                status,
                ledger_delta_marker_key,
            )
            add_count(
                diag["transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_delta"],
                status_reason_ledger_delta_marker,
            )
            add_nested_count(
                diag["quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta"],
                status_reason_ledger_delta_marker,
                "present" if quote_intent_id else "missing",
            )
            add_nested_count(
                diag["source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta"],
                status_reason_ledger_delta_marker,
                "present" if source_order_id is not None else "missing",
            )
            add_nested_count(
                diag["source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_delta"],
                status_reason_ledger_delta_marker,
                "present" if source_sequence_id is not None else "missing",
            )
        if self.cfg.source_opportunity_closed_cycle_marker_event_lite_summary:
            add_nested_count(
                diag["transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"],
                status,
                closed_cycle_marker_key,
            )
            add_count(
                diag["transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
            )
            if qty is not None:
                add_count(
                    diag["candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                    status_reason_closed_cycle_marker,
                    max(0.0, qty),
                )
            if base_qty is not None:
                add_count(
                    diag["base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                    status_reason_closed_cycle_marker,
                    max(0.0, base_qty),
                )
            if target_room is not None:
                add_count(
                    diag["target_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                    status_reason_closed_cycle_marker,
                    max(0.0, target_room),
                )
            if room_cost_value is not None:
                add_count(
                    diag["room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                    status_reason_closed_cycle_marker,
                    max(0.0, room_cost_value),
                )
            if imbalance_room is not None:
                add_count(
                    diag["imbalance_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                    status_reason_closed_cycle_marker,
                    max(0.0, imbalance_room),
                )
            add_nested_count(
                diag["candidate_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("candidate_qty", qty),
            )
            add_nested_count(
                diag["base_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("base_qty", base_qty),
            )
            add_nested_count(
                diag["target_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("target_room", target_room),
            )
            add_nested_count(
                diag["room_cost_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("room_cost", room_cost_value),
            )
            add_nested_count(
                diag["imbalance_room_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("imbalance_room", imbalance_room),
            )
            add_nested_count(
                diag["pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("pending_same_qty", pending_same_qty),
            )
            add_nested_count(
                diag["pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("pending_opp_qty", pending_opp_qty),
            )
            add_nested_count(
                diag["pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("pending_same_order_count", float(len(pending_same))),
            )
            add_nested_count(
                diag["pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                qty_bucket("pending_opp_order_count", float(len(pending_opp))),
            )
            add_nested_count(
                diag["opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                "opposite_seen_present" if opposite_seen_ms is not None else "opposite_seen_missing",
            )
            add_nested_count(
                diag["activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                age_ms_bucket("activation_opp_age", activation_opp_age_ms),
            )
            add_nested_count(
                diag["quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                "present" if quote_intent_id else "missing",
            )
            add_nested_count(
                diag["source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                "present" if source_order_id is not None else "missing",
            )
            add_nested_count(
                diag["source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"],
                status_reason_closed_cycle_marker,
                "present" if source_sequence_id is not None else "missing",
            )
        if not self.cfg.source_opportunity_marker_reason_source_event_lite_summary:
            return

        add_count(
            diag["transition_count_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
        )
        if micro_deficit:
            add_count(
                diag["micro_deficit_marker_count_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
            )
        if qty is not None:
            add_count(
                diag["candidate_qty_sum_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
                max(0.0, qty),
            )
        if base_qty is not None:
            add_count(
                diag["base_qty_sum_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
                max(0.0, base_qty),
            )
        if target_room is not None:
            add_count(
                diag["target_room_sum_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
                max(0.0, target_room),
            )
        if room_cost_value is not None:
            add_count(
                diag["room_cost_sum_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
                max(0.0, room_cost_value),
            )
        if imbalance_room is not None:
            add_count(
                diag["imbalance_room_sum_by_status_reason_side_offset_risk_open_deficit"],
                status_reason_marker,
                max(0.0, imbalance_room),
            )
        add_nested_count(
            diag["candidate_qty_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("candidate_qty", qty),
        )
        add_nested_count(
            diag["base_qty_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("base_qty", base_qty),
        )
        add_nested_count(
            diag["target_room_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("target_room", target_room),
        )
        add_nested_count(
            diag["room_cost_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("room_cost", room_cost_value),
        )
        add_nested_count(
            diag["imbalance_room_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("imbalance_room", imbalance_room),
        )
        add_nested_count(
            diag["pending_same_qty_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("pending_same_qty", pending_same_qty),
        )
        add_nested_count(
            diag["pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("pending_opp_qty", pending_opp_qty),
        )
        add_nested_count(
            diag["pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("pending_same_order_count", float(len(pending_same))),
        )
        add_nested_count(
            diag["pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            qty_bucket("pending_opp_order_count", float(len(pending_opp))),
        )
        add_nested_count(
            diag["opposite_seen_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            "opposite_seen_present" if opposite_seen_ms is not None else "opposite_seen_missing",
        )
        add_nested_count(
            diag["activation_opp_age_bucket_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            age_ms_bucket("activation_opp_age", activation_opp_age_ms),
        )
        add_nested_count(
            diag["quote_intent_presence_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            "present" if quote_intent_id else "missing",
        )
        add_nested_count(
            diag["source_order_presence_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            "present" if source_order_id is not None else "missing",
        )
        add_nested_count(
            diag["source_sequence_presence_by_status_reason_side_offset_risk_open_deficit"],
            status_reason_marker,
            "present" if source_sequence_id is not None else "missing",
        )

    def ce25_projected_guard_context(
        self,
        *,
        side: str,
        offset_s: float | None,
        qty: float,
        seed_px: float,
        same_qty: float,
        opp_qty: float,
        same_cost: float,
        opp_cost: float,
        estimated_fee_per_share: float = 0.0,
    ) -> dict[str, Any]:
        asset = asset_from_slug(self.slug)
        timeframe = timeframe_from_slug(self.slug)
        seconds_to_expiry = seconds_to_expiry_from_offset(self.slug, offset_s)
        if side == "YES":
            pre_yes_qty, pre_no_qty = same_qty, opp_qty
            pre_yes_cost, pre_no_cost = same_cost, opp_cost
        else:
            pre_yes_qty, pre_no_qty = opp_qty, same_qty
            pre_yes_cost, pre_no_cost = opp_cost, same_cost
        order_actual_cost = qty * (seed_px + estimated_fee_per_share)
        projected_yes_qty = pre_yes_qty + (qty if side == "YES" else 0.0)
        projected_no_qty = pre_no_qty + (qty if side == "NO" else 0.0)
        projected_yes_cost = pre_yes_cost + (order_actual_cost if side == "YES" else 0.0)
        projected_no_cost = pre_no_cost + (order_actual_cost if side == "NO" else 0.0)
        projected_pair_qty = min(projected_yes_qty, projected_no_qty)
        projected_total_bought_qty = projected_yes_qty + projected_no_qty
        projected_residual_qty = abs(projected_yes_qty - projected_no_qty)
        projected_pair_cost = None
        if projected_pair_qty > DUST and projected_yes_qty > DUST and projected_no_qty > DUST:
            projected_pair_cost = (projected_yes_cost / projected_yes_qty) + (
                projected_no_cost / projected_no_qty
            )
        projected_residual_rate = None
        if projected_total_bought_qty > DUST:
            projected_residual_rate = projected_residual_qty / projected_total_bought_qty
        pre_pair_qty = min(pre_yes_qty, pre_no_qty)
        pre_residual_qty = abs(pre_yes_qty - pre_no_qty)
        pre_total_qty = pre_yes_qty + pre_no_qty
        pre_residual_rate = pre_residual_qty / pre_total_qty if pre_total_qty > DUST else None
        action_intent = "initiation"
        if projected_pair_qty > pre_pair_qty + DUST:
            action_intent = "completion_cleanup"
        elif pre_residual_rate is not None and projected_residual_rate is not None and projected_residual_rate <= pre_residual_rate:
            action_intent = "completion_cleanup"
        return {
            "asset": asset,
            "timeframe": timeframe,
            "seconds_to_expiry": seconds_to_expiry,
            "action_intent": action_intent,
            "outcome": side,
            "order_qty": qty,
            "order_price": seed_px,
            "estimated_fee_per_share": estimated_fee_per_share,
            "pre_yes_qty": pre_yes_qty,
            "pre_no_qty": pre_no_qty,
            "pre_yes_actual_cost": pre_yes_cost,
            "pre_no_actual_cost": pre_no_cost,
            "projected_yes_qty": projected_yes_qty,
            "projected_no_qty": projected_no_qty,
            "projected_yes_actual_cost": projected_yes_cost,
            "projected_no_actual_cost": projected_no_cost,
            "projected_pair_qty": projected_pair_qty,
            "projected_pair_cost": projected_pair_cost,
            "projected_residual_qty": projected_residual_qty,
            "projected_total_bought_qty": projected_total_bought_qty,
            "projected_residual_rate_on_bought_qty": projected_residual_rate,
        }

    def ce25_projected_guard_decision(self, context: dict[str, Any]) -> dict[str, Any]:
        asset = str(context.get("asset") or "UNKNOWN")
        timeframe = str(context.get("timeframe") or "unknown")
        seconds_to_expiry = context.get("seconds_to_expiry")
        pair_cost = context.get("projected_pair_cost")
        residual_rate = context.get("projected_residual_rate_on_bought_qty")
        action_intent = str(context.get("action_intent") or "unknown")
        if pair_cost is None or residual_rate is None:
            return {"status": "would_block_missing_projected_fields", "guard": "fail_closed", "allowed": False}
        if pair_cost >= 1.0:
            return {"status": "would_block_pair_cost_gte_1_00", "guard": "hard_kill", "allowed": False}
        if seconds_to_expiry is not None and seconds_to_expiry <= 300 and residual_rate > 0.20:
            return {"status": "would_block_residual_gt_20pct_near_close", "guard": "hard_kill", "allowed": False}
        if seconds_to_expiry is not None and seconds_to_expiry <= 60 and action_intent == "initiation":
            return {"status": "would_block_final60_initiation", "guard": "final_window", "allowed": False}
        if seconds_to_expiry is not None and 60 < seconds_to_expiry <= 300 and action_intent != "completion_cleanup":
            return {"status": "would_block_final_1m_5m_not_cleanup", "guard": "final_window", "allowed": False}
        matched: list[str] = []
        if asset in {"ETH", "SOL"} and timeframe in {"5m", "15m"} and pair_cost < 0.90 and residual_rate < 0.15:
            matched.append("starter")
        if asset in {"BTC", "ETH", "SOL"} and timeframe in {"5m", "15m"} and pair_cost < 0.95 and residual_rate < 0.10:
            matched.append("core")
        if not matched:
            return {"status": "would_block_no_projected_guard_match", "guard": "no_match", "allowed": False}
        return {"status": "would_allow_projected_guard", "guard": "starter" if "starter" in matched else matched[0], "allowed": True}

    def record_ce25_projected_guard_diagnostic(
        self,
        *,
        side: str,
        offset_s: float | None,
        qty: float,
        seed_px: float,
        same_qty: float,
        opp_qty: float,
        same_cost: float,
        opp_cost: float,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.ce25_projected_guard_event_lite_summary):
            return
        context = self.ce25_projected_guard_context(
            side=side,
            offset_s=offset_s,
            qty=qty,
            seed_px=seed_px,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost,
            opp_cost=opp_cost,
        )
        decision = self.ce25_projected_guard_decision(context)
        guard = str(decision.get("guard") or "unknown")
        status = str(decision.get("status") or "unknown")
        asset = str(context.get("asset") or "UNKNOWN")
        timeframe = str(context.get("timeframe") or "unknown")
        diag = self.event_lite_ce25_projected_guard_summary
        add_nested_count(diag["decision_count_by_status"], status, side)
        add_nested_count(diag["decision_count_by_guard"], guard, status)
        add_nested_count(
            diag["decision_count_by_asset_timeframe_guard_status"],
            f"{asset}|{timeframe}|{guard}",
            status,
        )
        add_nested_count(
            diag["projected_pair_cost_bucket_by_guard"],
            guard,
            ce25_projected_pair_cost_bucket(context.get("projected_pair_cost")),
        )
        add_nested_count(
            diag["projected_residual_bucket_by_guard"],
            guard,
            ce25_projected_residual_bucket(context.get("projected_residual_rate_on_bought_qty")),
        )
        seconds_to_expiry = context.get("seconds_to_expiry")
        if seconds_to_expiry is None:
            final_policy = "seconds_to_expiry_unknown"
        elif seconds_to_expiry <= 60:
            final_policy = "final_0_60s"
        elif seconds_to_expiry <= 300:
            final_policy = "final_1m_5m"
        else:
            final_policy = "pre_final_5m_plus"
        add_nested_count(diag["final_window_policy_count"], final_policy, status)

    def record_symmetric_activation_diagnostic(
        self,
        *,
        status: str,
        reason: str,
        side: str,
        offset_s: float | None,
        qty: float,
        seed_px: float,
        same_qty: float,
        opp_qty: float,
        same_cost: float,
        opp_cost: float,
        activation_required: bool,
        activation_opp_age_ms: int | None,
        opposite_seen_ms: int | None,
        source_sequence_id: Any | None,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.symmetric_activation_event_lite_summary):
            return
        context = self.ce25_projected_guard_context(
            side=side,
            offset_s=offset_s,
            qty=max(0.0, qty),
            seed_px=seed_px,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost,
            opp_cost=opp_cost,
        )
        activation_bucket = activation_state_bucket(activation_required, activation_opp_age_ms, opposite_seen_ms)
        status_reason_activation = f"{status}|{reason}|{activation_bucket}"
        side_offset_activation = f"{side}|{offset_bucket(offset_s)}|{activation_bucket}"
        projected_residual_qty = context.get("projected_residual_qty")
        residual_leak_stress_cost = 0.0
        if isinstance(projected_residual_qty, (int, float)):
            residual_leak_stress_cost = max(0.0, float(projected_residual_qty)) * max(seed_px, 0.0) * 0.02
        diag = self.event_lite_symmetric_activation_summary
        add_nested_count(
            diag["candidate_count_by_status_reason_activation_bucket"],
            f"{status}|{reason}",
            activation_bucket,
        )
        add_nested_count(
            diag["candidate_count_by_status_reason_side_offset_activation_bucket"],
            f"{status}|{reason}",
            side_offset_activation,
        )
        add_nested_count(
            diag["projected_pair_cost_bucket_by_status_reason_activation_bucket"],
            status_reason_activation,
            ce25_projected_pair_cost_bucket(context.get("projected_pair_cost")),
        )
        add_nested_count(
            diag["projected_residual_rate_bucket_by_status_reason_activation_bucket"],
            status_reason_activation,
            ce25_projected_residual_bucket(context.get("projected_residual_rate_on_bought_qty")),
        )
        add_nested_count(
            diag["activation_age_bucket_by_status_reason_activation_bucket"],
            status_reason_activation,
            age_ms_bucket("activation_opp_age", activation_opp_age_ms),
        )
        add_nested_count(
            diag["source_sequence_presence_by_status_reason_activation_bucket"],
            status_reason_activation,
            "present" if source_sequence_id is not None else "missing",
        )
        add_count(
            diag["residual_leak_stress_cost_sum_by_status_reason_activation_bucket"],
            status_reason_activation,
            residual_leak_stress_cost,
        )

    def symmetric_activation_keys_for_source(self, lot: Lot) -> tuple[str, str]:
        activation_bucket = activation_state_bucket(
            lot.activation_required,
            lot.activation_opp_age_ms,
            lot.opposite_seen_ms,
        )
        status_reason = f"{lot.symmetric_activation_status}|{lot.symmetric_activation_reason}"
        side_offset_activation = f"{status_reason}|{lot.side}|{offset_bucket(lot.offset_s)}|{activation_bucket}"
        return f"{status_reason}|{activation_bucket}", side_offset_activation

    def symmetric_activation_projected_residual_keys_for_source(self, lot: Lot) -> tuple[str, str]:
        status_reason = f"{lot.symmetric_activation_status}|{lot.symmetric_activation_reason}"
        projected_residual_bucket = lot.symmetric_projected_residual_bucket
        side_offset_projected = (
            f"{status_reason}|{lot.side}|{offset_bucket(lot.offset_s)}|{projected_residual_bucket}"
        )
        return f"{status_reason}|{projected_residual_bucket}", side_offset_projected

    def record_symmetric_activation_projected_residual_pair_source(self, lot: Lot, qty: float) -> None:
        if not (
            self.cfg.event_lite_summary
            and self.cfg.symmetric_activation_projected_residual_tail_join_event_lite_summary
        ):
            return
        status_reason_projected, _side_offset_projected = self.symmetric_activation_projected_residual_keys_for_source(lot)
        diag = self.event_lite_symmetric_activation_projected_residual_tail_join_summary
        add_count(diag["pair_qty_sum_by_status_reason_projected_residual_bucket"], status_reason_projected, qty)
        add_nested_count(
            diag["source_sequence_presence_by_status_reason_projected_residual_bucket"],
            status_reason_projected,
            "present" if lot.trigger_source_sequence_id is not None else "missing",
            qty,
        )

    def record_symmetric_activation_tail_pair_source(self, lot: Lot, qty: float, net_pair_cost: float) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.symmetric_activation_tail_attribution_event_lite_summary):
            return
        status_reason_activation, side_offset_activation = self.symmetric_activation_keys_for_source(lot)
        diag = self.event_lite_symmetric_activation_tail_attribution_summary
        add_count(diag["pair_qty_sum_by_status_reason_activation_bucket"], status_reason_activation, qty)
        add_nested_count(
            diag["pair_cost_bucket_by_status_reason_activation_bucket"],
            status_reason_activation,
            pair_cost_bucket(net_pair_cost),
            qty,
        )
        add_count(diag["pair_tail_loss_sum_by_status_reason_activation_bucket"], status_reason_activation, max(0.0, (net_pair_cost - 1.0) * qty))
        add_nested_count(
            diag["source_sequence_presence_by_status_reason_activation_bucket"],
            status_reason_activation,
            "present" if lot.trigger_source_sequence_id is not None else "missing",
            qty,
        )
        add_count(diag["pair_qty_sum_by_status_reason_side_offset_activation_bucket"], side_offset_activation, qty)
        add_nested_count(
            diag["pair_cost_bucket_by_status_reason_side_offset_activation_bucket"],
            side_offset_activation,
            pair_cost_bucket(net_pair_cost),
            qty,
        )
        add_count(
            diag["pair_tail_loss_sum_by_status_reason_side_offset_activation_bucket"],
            side_offset_activation,
            max(0.0, (net_pair_cost - 1.0) * qty),
        )
        add_nested_count(
            diag["source_sequence_presence_by_status_reason_side_offset_activation_bucket"],
            side_offset_activation,
            "present" if lot.trigger_source_sequence_id is not None else "missing",
            qty,
        )

    def symmetric_activation_tail_residual_exemplars(
        self,
        residual_lots: list[Lot],
        summary_ts_ms: int,
    ) -> list[dict[str, Any]]:
        if not (self.cfg.event_lite_summary and self.cfg.symmetric_activation_tail_attribution_event_lite_summary):
            return []
        exemplars: list[dict[str, Any]] = []
        for lot in residual_lots:
            status_reason_activation, side_offset_activation = self.symmetric_activation_keys_for_source(lot)
            exemplars.append(
                {
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "lot_id": lot.id,
                    "quote_intent_id": lot.quote_intent_id,
                    "status_reason_activation_bucket": status_reason_activation,
                    "status_reason_side_offset_activation_bucket": side_offset_activation,
                    "side": lot.side,
                    "offset_bucket": offset_bucket(lot.offset_s),
                    "source_sequence_presence": "present" if lot.trigger_source_sequence_id is not None else "missing",
                    "source_sequence_id": lot.trigger_source_sequence_id,
                    "source_risk_direction": lot.source_risk_direction,
                    "residual_qty": round(lot.qty, 6),
                    "residual_cost": round(lot.cost, 6),
                    "source_pair_qty": round(lot.source_pair_qty, 6),
                    "source_pair_cost": round(lot.source_pair_cost, 6),
                    "source_pair_pnl": round(lot.source_pair_pnl, 6),
                    "residual_age_ms": max(0, summary_ts_ms - lot.fill_ms),
                    "seed_px": lot.px,
                    "trigger_px": lot.trigger_px,
                }
            )
        return sorted(exemplars, key=lambda item: item.get("residual_cost", 0.0), reverse=True)[:20]

    def record_source_link_pair_transition(
        self,
        *,
        qty: float,
        net_pair_cost: float,
        sources: list[Lot],
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.source_link_transition_event_lite_summary):
            return
        cost_bucket = pair_cost_bucket(net_pair_cost)
        diag = self.event_lite_source_link_transitions
        for lot in sources:
            side_offset = f"{lot.side}|{offset_bucket(lot.offset_s)}"
            side_offset_risk = side_offset_risk_key(lot.side, lot.offset_s, lot.source_risk_direction)
            add_count(diag["immediate_pair_action_count_by_source_side_offset"], side_offset)
            add_count(diag["immediate_pair_action_count_by_source_side_offset_risk_direction"], side_offset_risk)
            add_nested_count(diag["immediate_pair_qty_bucket_by_source_side_offset"], side_offset, qty_bucket("pair_qty", qty))
            add_nested_count(diag["immediate_pair_cost_bucket_by_source_side_offset"], side_offset, cost_bucket)
            add_nested_count(
                diag["immediate_pair_qty_bucket_by_source_side_offset_risk_direction"],
                side_offset_risk,
                qty_bucket("pair_qty", qty),
            )
            add_nested_count(
                diag["immediate_pair_cost_bucket_by_source_side_offset_risk_direction"],
                side_offset_risk,
                cost_bucket,
            )
            add_nested_count(
                diag["immediate_pair_source_quote_presence_by_side_offset"],
                side_offset,
                "present" if lot.quote_intent_id else "missing",
            )
            add_nested_count(
                diag["immediate_pair_source_order_presence_by_side_offset"],
                side_offset,
                "present" if lot.source_order_id is not None else "missing",
            )

    def source_link_transition_diagnostics(self, residual_lots: list[Lot]) -> dict[str, Any]:
        diag = json.loads(json.dumps(self.event_lite_source_link_transitions))
        residual_qty_by_source_side_offset: dict[str, float] = {}
        residual_cost_by_source_side_offset: dict[str, float] = {}
        residual_count_by_source_side_offset: dict[str, float] = {}
        residual_qty_by_source_side_offset_risk_direction: dict[str, float] = {}
        residual_cost_by_source_side_offset_risk_direction: dict[str, float] = {}
        residual_count_by_source_side_offset_risk_direction: dict[str, float] = {}
        residual_cost_bucket_by_source_side_offset: dict[str, dict[str, float]] = {}
        residual_cost_bucket_by_source_side_offset_risk_direction: dict[str, dict[str, float]] = {}
        residual_source_quote_presence_by_side_offset: dict[str, dict[str, float]] = {}
        residual_source_order_presence_by_side_offset: dict[str, dict[str, float]] = {}
        for lot in residual_lots:
            side_offset = f"{lot.side}|{offset_bucket(lot.offset_s)}"
            side_offset_risk = side_offset_risk_key(lot.side, lot.offset_s, lot.source_risk_direction)
            add_count(residual_qty_by_source_side_offset, side_offset, lot.qty)
            add_count(residual_cost_by_source_side_offset, side_offset, lot.cost)
            add_count(residual_count_by_source_side_offset, side_offset)
            add_count(residual_qty_by_source_side_offset_risk_direction, side_offset_risk, lot.qty)
            add_count(residual_cost_by_source_side_offset_risk_direction, side_offset_risk, lot.cost)
            add_count(residual_count_by_source_side_offset_risk_direction, side_offset_risk)
            add_nested_count(residual_cost_bucket_by_source_side_offset, side_offset, qty_bucket("residual_cost", lot.cost))
            add_nested_count(
                residual_cost_bucket_by_source_side_offset_risk_direction,
                side_offset_risk,
                qty_bucket("residual_cost", lot.cost),
            )
            add_nested_count(
                residual_source_quote_presence_by_side_offset,
                side_offset,
                "present" if lot.quote_intent_id else "missing",
            )
            add_nested_count(
                residual_source_order_presence_by_side_offset,
                side_offset,
                "present" if lot.source_order_id is not None else "missing",
            )
        diag.update(
            {
                "residual_qty_by_source_side_offset": residual_qty_by_source_side_offset,
                "residual_cost_by_source_side_offset": residual_cost_by_source_side_offset,
                "residual_count_by_source_side_offset": residual_count_by_source_side_offset,
                "residual_qty_by_source_side_offset_risk_direction": residual_qty_by_source_side_offset_risk_direction,
                "residual_cost_by_source_side_offset_risk_direction": residual_cost_by_source_side_offset_risk_direction,
                "residual_count_by_source_side_offset_risk_direction": residual_count_by_source_side_offset_risk_direction,
                "residual_cost_bucket_by_source_side_offset": residual_cost_bucket_by_source_side_offset,
                "residual_cost_bucket_by_source_side_offset_risk_direction": residual_cost_bucket_by_source_side_offset_risk_direction,
                "residual_source_quote_presence_by_side_offset": residual_source_quote_presence_by_side_offset,
                "residual_source_order_presence_by_side_offset": residual_source_order_presence_by_side_offset,
            }
        )
        return diag

    def source_link_residual_tail_exemplars(self, residual_lots: list[Lot], summary_ts_ms: int) -> list[dict[str, Any]]:
        if not (self.cfg.event_lite_summary and self.cfg.source_link_residual_tail_exemplars_event_lite_summary):
            return []
        exemplars: list[dict[str, Any]] = []
        for lot in residual_lots:
            residual_age_ms = max(0, summary_ts_ms - lot.fill_ms)
            exemplars.append(
                {
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "lot_id": lot.id,
                    "quote_intent_id": lot.quote_intent_id,
                    "source_order_id": lot.source_order_id,
                    "source_sequence_id": lot.trigger_source_sequence_id,
                    "side": lot.side,
                    "offset_s": lot.offset_s,
                    "source_risk_direction": lot.source_risk_direction,
                    "micro_deficit_repair_guard_candidate": lot.micro_deficit_repair_guard_candidate,
                    "micro_deficit_repair_deficit": round(lot.micro_deficit_repair_deficit or 0.0, 6),
                    "trigger_px": lot.trigger_px,
                    "trigger_size": lot.trigger_size,
                    "trigger_ts_ms": lot.trigger_ts_ms,
                    "pre_seed_same_qty": round(lot.pre_seed_same_qty or 0.0, 6),
                    "pre_seed_opp_qty": round(lot.pre_seed_opp_qty or 0.0, 6),
                    "pre_seed_open_qty": round((lot.pre_seed_same_qty or 0.0) + (lot.pre_seed_opp_qty or 0.0), 6),
                    "pre_seed_same_cost": round(lot.pre_seed_same_cost or 0.0, 6),
                    "pre_seed_opp_cost": round(lot.pre_seed_opp_cost or 0.0, 6),
                    "ledger_proxy_before": round(lot.ledger_proxy_before or 0.0, 6),
                    "ledger_proxy_after": round(lot.ledger_proxy_after or 0.0, 6),
                    "source_pair_qty": round(lot.source_pair_qty, 6),
                    "source_pair_cost": round(lot.source_pair_cost, 6),
                    "source_pair_pnl": round(lot.source_pair_pnl, 6),
                    "source_residual_qty": round(lot.qty, 6),
                    "source_residual_cost": round(lot.cost, 6),
                    "source_residual_age_ms": residual_age_ms,
                    "seed_px": lot.px,
                }
            )
        return sorted(exemplars, key=lambda item: item.get("source_residual_cost", 0.0), reverse=True)[:20]

    def record_pair_source_event_lite(
        self,
        *,
        matched_pair_id: str,
        ts_ms: int,
        qty: float,
        pair_cost: float,
        net_pair_cost: float,
        sources: list[Lot],
        action_kind: str,
    ) -> None:
        if not (self.cfg.event_lite_summary and self.cfg.pair_source_event_lite_summary):
            return
        cost_bucket = pair_cost_bucket(net_pair_cost)
        self.event_lite_pair_source_action_count += 1
        source_contexts: list[dict[str, Any]] = []
        for lot in sources:
            self.event_lite_pair_source_record_count += 1
            add_nested_count(self.event_lite_pair_cost_by_source_seed_px_bucket, cost_bucket, px_bucket(lot.px))
            add_nested_count(self.event_lite_pair_cost_by_source_public_px_bucket, cost_bucket, px_bucket(lot.trigger_px))
            add_nested_count(self.event_lite_pair_cost_by_source_offset_bucket, cost_bucket, offset_bucket(lot.offset_s))
            add_nested_count(self.event_lite_pair_cost_by_source_side, cost_bucket, lot.side)
            source_contexts.append(
                {
                    "quote_intent_id": lot.quote_intent_id,
                    "source_order_id": lot.source_order_id,
                    "side": lot.side,
                    "seed_px": lot.px,
                    "public_trade_px": lot.trigger_px,
                    "public_trade_size": lot.trigger_size,
                    "offset_s": lot.offset_s,
                    "trigger_ts_ms": lot.trigger_ts_ms,
                    "trigger_source_sequence_id": lot.trigger_source_sequence_id,
                }
            )
        if cost_bucket in {"pair_cost_1p00_1p05", "pair_cost_gt_1p05"}:
            self.event_lite_top_high_cost_pair_sources.append(
                {
                    "matched_pair_id": matched_pair_id,
                    "kind": action_kind,
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "qty": round(qty, 6),
                    "pair_cost": round(pair_cost, 6),
                    "net_pair_cost": round(net_pair_cost, 6),
                    "pair_cost_bucket": cost_bucket,
                    "sources": source_contexts,
                }
            )

    def portfolio_ledger_diagnostics(self, residual_lots: list[Lot]) -> dict[str, Any]:
        residual_qty_by_side: dict[str, float] = {}
        residual_cost_by_side: dict[str, float] = {}
        residual_lot_count_by_side: dict[str, float] = {}
        for lot in residual_lots:
            add_count(residual_qty_by_side, lot.side, lot.qty)
            add_count(residual_cost_by_side, lot.side, lot.cost)
            add_count(residual_lot_count_by_side, lot.side)
        m = self.metrics
        stress_cost_proxy = 0.01 * (2.0 * m.pair_qty + m.residual_qty)
        risk_adjusted_proxy = m.pair_pnl - m.taker_fee - m.residual_cost - stress_cost_proxy
        return {
            "condition_id": self.condition_id,
            "slug": self.slug,
            "rule": "portfolio_risk_adjusted_nonnegative",
            "seed_actions": m.candidates,
            "queue_supported_fills": m.queue_supported_fills,
            "pair_actions": m.pair_actions,
            "pair_qty": round(m.pair_qty, 6),
            "pair_cost_sum": round(m.pair_cost_sum, 6),
            "avg_pair_cost": round(m.pair_cost_sum / m.pair_qty, 6) if m.pair_qty else 0.0,
            "net_pair_cost_sum": round(m.net_pair_cost_sum, 6),
            "avg_net_pair_cost": round(m.net_pair_cost_sum / m.pair_qty, 6) if m.pair_qty else 0.0,
            "pair_pnl": round(m.pair_pnl, 6),
            "official_taker_fee_proxy": round(m.taker_fee, 6),
            "residual_qty": round(m.residual_qty, 6),
            "residual_cost": round(m.residual_cost, 6),
            "residual_qty_by_side": residual_qty_by_side,
            "residual_cost_by_side": residual_cost_by_side,
            "residual_lot_count_by_side": residual_lot_count_by_side,
            "stress_cost_proxy": round(stress_cost_proxy, 6),
            "conservative_risk_adjusted_proxy": round(risk_adjusted_proxy, 6),
            "risk_adjusted_nonnegative": risk_adjusted_proxy >= 0.0,
            "risk_adjusted_bucket": risk_adjusted_bucket(risk_adjusted_proxy),
        }

    def record_activation_seen(self, side: str, ts_ms: int) -> None:
        if side in self.activation_last_seen_ms:
            self.activation_last_seen_ms[side] = ts_ms

    def activation_allows_seed(self, side: str, ts_ms: int) -> tuple[bool, int | None]:
        if self.cfg.activation_mode == "none":
            return True, None
        if self.cfg.activation_mode != "opp_seen":
            raise ValueError(f"unsupported activation_mode={self.cfg.activation_mode!r}")
        opp_seen_ms = self.activation_last_seen_ms.get(opp(side))
        if opp_seen_ms is None:
            return False, None
        age_ms = ts_ms - opp_seen_ms
        return age_ms >= 0 and age_ms <= self.cfg.activation_window_s * 1000.0 + 1e-9, int(age_ms)

    def pending_orders(self, side: str | None = None) -> list[VirtualOrder]:
        out = [o for o in self.pending if not o.fill_ms and not o.cancel_ms]
        return [o for o in out if o.side == side] if side else out

    def lot_qty(self, side: str) -> float:
        return sum(lot.qty for lot in self.lots[side])

    def lot_cost(self, side: str) -> float:
        return sum(lot.cost for lot in self.lots[side])

    def exposure_qty(self, side: str) -> float:
        return self.lot_qty(side) + sum(order.qty for order in self.pending_orders(side))

    def exposure_cost(self, side: str) -> float:
        return self.lot_cost(side) + sum(order.qty * order.px for order in self.pending_orders(side))

    def total_open_cost(self) -> float:
        return self.exposure_cost("YES") + self.exposure_cost("NO")

    def online_ledger_proxy(self) -> float:
        exposure_qty = self.exposure_qty("YES") + self.exposure_qty("NO")
        return self.metrics.pair_pnl - self.metrics.taker_fee - self.total_open_cost() - 0.01 * (
            2.0 * self.metrics.pair_qty + exposure_qty
        )

    def mark_touches(self, ts_ms: int, trade_side: str | None = None, trade_px: float | None = None) -> None:
        for order in self.pending_orders():
            bid = side_bid(self.book, order.side)
            if bid >= order.px - 1e-12 and order.first_bid_touch_ms is None:
                order.first_bid_touch_ms = ts_ms
                self.emit({"kind": "touch", "touch_type": "bid_touch", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": order.side, "order_px": order.px, "bid": bid, "wait_ms": ts_ms - order.created_ms})
            if trade_side == order.side and trade_px is not None and trade_px <= order.px + 1e-12 and order.first_trade_touch_ms is None:
                order.first_trade_touch_ms = ts_ms
                self.emit({"kind": "touch", "touch_type": "trade_through", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": order.side, "order_px": order.px, "trade_px": trade_px, "wait_ms": ts_ms - order.created_ms})

    def cancel_expired(self, ts_ms: int) -> None:
        offset = self.offset_s(ts_ms)
        for order in self.pending_orders():
            expired = ts_ms - order.created_ms >= self.cfg.order_ttl_ms
            too_late = offset is not None and offset >= 240
            if not expired and not too_late:
                continue
            order.cancel_ms = ts_ms
            order.cancel_reason = "offset_240" if too_late else "ttl"
            self.metrics.cancelled_orders += 1
            if order.first_bid_touch_ms or order.first_trade_touch_ms:
                self.metrics.touch_only_orders += 1
            self.emit({"kind": "cancel", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "cancel_ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "reason": order.cancel_reason, "side": order.side, "price": order.px, "px": order.px, "size": order.qty, "qty": order.qty, "placed_ts_ms": order.created_ms, "accepted_ts_ms": order.accepted_ms, "opposite_trigger_ts_ms": order.opposite_trigger_ts_ms, "queue_credit": order.queue_credit})

    def fill_order(
        self,
        order: VirtualOrder,
        ts_ms: int,
        trade_px: float,
        trade_size: float,
        trigger_source_sequence_id: Any | None,
        trigger_event_time_ms: int,
    ) -> None:
        order.fill_ms = ts_ms
        self.metrics.queue_supported_fills += 1
        self.metrics.filled_qty += order.qty
        self.metrics.filled_cost += order.qty * order.px
        self.metrics.fill_wait_ms.append(ts_ms - order.created_ms)
        lot = Lot(
            id=self.next_lot_id,
            quote_intent_id=order.quote_intent_id,
            side=order.side,
            qty=order.qty,
            px=order.px,
            fill_ms=ts_ms,
            source_order_id=order.id,
            trigger_px=order.trigger_px,
            trigger_size=order.trigger_size,
            offset_s=order.offset_s,
            trigger_ts_ms=order.trigger_ts_ms,
            trigger_source_sequence_id=order.trigger_source_sequence_id,
            source_risk_direction=order.source_risk_direction,
            micro_deficit_repair_guard_candidate=order.micro_deficit_repair_guard_candidate,
            micro_deficit_repair_deficit=order.micro_deficit_repair_deficit,
            pre_seed_same_qty=order.pre_seed_same_qty,
            pre_seed_opp_qty=order.pre_seed_opp_qty,
            pre_seed_same_cost=order.pre_seed_same_cost,
            pre_seed_opp_cost=order.pre_seed_opp_cost,
            ledger_proxy_before=order.ledger_proxy_before,
            ledger_proxy_after=order.ledger_proxy_after,
            activation_required=order.activation_required,
            activation_opp_age_ms=order.activation_opp_age_ms,
            opposite_seen_ms=order.opposite_seen_ms,
            symmetric_activation_status=order.symmetric_activation_status,
            symmetric_activation_reason=order.symmetric_activation_reason,
            symmetric_projected_pair_cost_bucket=order.symmetric_projected_pair_cost_bucket,
            symmetric_projected_residual_bucket=order.symmetric_projected_residual_bucket,
        )
        self.next_lot_id += 1
        self.lots[order.side].append(lot)
        self.record_event_lite_fill(order.side, order.px)
        self.emit({"kind": "queue_supported_fill", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "fill_ts_ms": ts_ms, "event_time_ms": trigger_event_time_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "lot_id": lot.id, "side": order.side, "source": "no_order_public_trade_queue_proxy", "seed_px": order.px, "price": order.px, "size": order.qty, "qty": order.qty, "queue_share": self.cfg.queue_share, "queue_credit": order.queue_credit, "trade_px": trade_px, "trade_size": trade_size, "trigger_ts_ms": ts_ms, "trigger_source_sequence_id": trigger_source_sequence_id, "source_sequence_id": trigger_source_sequence_id, "market_md_source_sequence_id": trigger_source_sequence_id, "placed_ts_ms": order.created_ms, "accepted_ts_ms": order.accepted_ms, "opposite_trigger_ts_ms": order.opposite_trigger_ts_ms, "fill_wait_ms": ts_ms - order.created_ms})

    def pair_inventory(self, ts_ms: int) -> None:
        yes = self.lots["YES"]
        no = self.lots["NO"]
        while yes and no:
            a, b = yes[0], no[0]
            take = min(a.qty, b.qty)
            if take <= DUST:
                break
            pair_cost = a.px + b.px
            older = min(a.fill_ms, b.fill_ms)
            self.metrics.pair_actions += 1
            self.metrics.pair_qty += take
            self.metrics.pair_cost_sum += take * pair_cost
            self.metrics.net_pair_cost_sum += take * pair_cost
            self.metrics.pair_pnl += take * (1.0 - pair_cost)
            self.metrics.pair_costs.append(pair_cost)
            self.metrics.net_pair_costs.append(pair_cost)
            self.metrics.pair_wait_ms.append(ts_ms - older)
            pair_pnl = take * (1.0 - pair_cost)
            for source_lot in (a, b):
                source_lot.source_pair_qty += take
                source_lot.source_pair_cost += take * pair_cost
                source_lot.source_pair_pnl += pair_pnl
                self.record_symmetric_activation_projected_residual_pair_source(source_lot, take)
                self.record_symmetric_activation_tail_pair_source(source_lot, take, pair_cost)
            a.qty -= take
            b.qty -= take
            matched_pair_id = f"{self.slug}:pair:{self.metrics.pair_actions}"
            self.record_source_link_pair_transition(qty=take, net_pair_cost=pair_cost, sources=[a, b])
            self.record_pair_source_event_lite(
                matched_pair_id=matched_pair_id,
                ts_ms=ts_ms,
                qty=take,
                pair_cost=pair_cost,
                net_pair_cost=pair_cost,
                sources=[a, b],
                action_kind="internal_pair",
            )
            self.emit({"kind": "internal_pair", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "matched_pair_id": matched_pair_id, "yes_quote_intent_id": a.quote_intent_id, "no_quote_intent_id": b.quote_intent_id, "quote_intent_ids": [a.quote_intent_id, b.quote_intent_id], "qty": take, "yes_px": a.px, "no_px": b.px, "pair_cost": pair_cost, "delay_ms": ts_ms - older})
            if a.qty <= DUST:
                yes.pop(0)
            if b.qty <= DUST:
                no.pop(0)

    def try_salvage(self, ts_ms: int) -> None:
        if self.cfg.salvage_net_cap <= 0:
            return
        for held_side in ("YES", "NO"):
            lots = self.lots[held_side]
            if not lots:
                continue
            comp_side = opp(held_side)
            ask = side_ask(self.book, comp_side)
            if ask <= 0:
                continue
            fee = fee_per_share(ask, self.cfg.taker_fee_rate)
            paired = 0.0
            while lots and paired < self.cfg.max_salvage_qty - DUST:
                lot = lots[0]
                age = ts_ms - lot.fill_ms
                if age < self.cfg.salvage_age_ms or lot.cost < self.cfg.salvage_min_lot_cost:
                    break
                gross_pair = lot.px + ask
                net_pair = gross_pair + fee
                if net_pair > self.cfg.salvage_net_cap + 1e-12:
                    break
                take = min(lot.qty, self.cfg.max_salvage_qty - paired)
                paired += take
                self.metrics.salvage_actions += 1
                self.metrics.salvage_qty += take
                self.metrics.taker_fee += take * fee
                self.metrics.completion_cost += take * ask
                self.metrics.pair_actions += 1
                self.metrics.pair_qty += take
                self.metrics.pair_cost_sum += take * gross_pair
                self.metrics.net_pair_cost_sum += take * net_pair
                self.metrics.pair_pnl += take * (1.0 - net_pair)
                self.metrics.pair_costs.append(gross_pair)
                self.metrics.net_pair_costs.append(net_pair)
                self.metrics.salvage_wait_ms.append(age)
                self.metrics.pair_wait_ms.append(age)
                lot.source_pair_qty += take
                lot.source_pair_cost += take * net_pair
                lot.source_pair_pnl += take * (1.0 - net_pair)
                self.record_symmetric_activation_projected_residual_pair_source(lot, take)
                self.record_symmetric_activation_tail_pair_source(lot, take, net_pair)
                matched_pair_id = f"{self.slug}:salvage:{self.metrics.salvage_actions}"
                self.record_source_link_pair_transition(qty=take, net_pair_cost=net_pair, sources=[lot])
                self.record_pair_source_event_lite(
                    matched_pair_id=matched_pair_id,
                    ts_ms=ts_ms,
                    qty=take,
                    pair_cost=gross_pair,
                    net_pair_cost=net_pair,
                    sources=[lot],
                    action_kind="fak_salvage",
                )
                self.emit({"kind": "fak_salvage", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "matched_pair_id": matched_pair_id, "quote_intent_id": lot.quote_intent_id, "held_side": held_side, "comp_side": comp_side, "qty": take, "held_px": lot.px, "comp_ask": ask, "fee_per_share": fee, "net_pair_cost": net_pair, "age_ms": age})
                lot.qty -= take
                if lot.qty <= DUST:
                    lots.pop(0)
                else:
                    break

    def on_book(self, msg: dict[str, Any]) -> None:
        self.book_ticks += 1
        ts_ms = int(msg.get("ts_ms") or now_ms())
        self.book = {
            "yes_bid": float(msg.get("yes_bid") or 0.0),
            "yes_ask": float(msg.get("yes_ask") or 0.0),
            "no_bid": float(msg.get("no_bid") or 0.0),
            "no_ask": float(msg.get("no_ask") or 0.0),
        }
        self.mark_touches(ts_ms)
        self.cancel_expired(ts_ms)
        self.pair_inventory(ts_ms)
        self.try_salvage(ts_ms)

    def on_trade(self, msg: dict[str, Any]) -> None:
        self.trade_ticks += 1
        ts_ms = int(msg.get("ts_ms") or now_ms())
        side = side_from_str(str(msg.get("market_side") or ""))
        taker = str(msg.get("taker_side") or "").upper()
        px = float(msg.get("price") or 0.0)
        size = float(msg.get("size") or 0.0)
        offset = self.offset_s(ts_ms)
        trigger_source_sequence_id = source_sequence_id(msg)
        trigger_event_time_ms = event_time_ms(msg, ts_ms)

        def block_seed(reason: str, **kwargs: Any) -> None:
            self.block(
                reason,
                side=side,
                public_trade_px=px,
                offset_s=offset,
                source_sequence_id=trigger_source_sequence_id,
                **kwargs,
            )

        if taker == "SELL" and side in {"YES", "NO"}:
            self.mark_touches(ts_ms, side, px)
            for order in self.pending_orders(side):
                if px > order.px + 1e-12:
                    continue
                order.queue_credit += max(0.0, size * self.cfg.queue_share)
                if order.queue_credit + 1e-9 >= order.qty:
                    self.fill_order(order, ts_ms, px, size, trigger_source_sequence_id, trigger_event_time_ms)
            self.pair_inventory(ts_ms)
            self.try_salvage(ts_ms)
        else:
            self.mark_touches(ts_ms)
            self.try_salvage(ts_ms)
            return

        self.sell_triggers += 1
        self.cancel_expired(ts_ms)
        if offset is None or offset < self.cfg.seed_offset_min_s or offset >= self.cfg.seed_offset_max_s:
            block_seed("offset")
            return
        if not (self.cfg.seed_px_lo <= px <= self.cfg.seed_px_hi):
            block_seed("price")
            return
        yes_ask = side_ask(self.book, "YES")
        no_ask = side_ask(self.book, "NO")
        if yes_ask <= 0 or no_ask <= 0:
            block_seed("missing_pair_ask")
            return
        l1_pair = yes_ask + no_ask
        if l1_pair > self.cfg.seed_l1_cap + 1e-12:
            block_seed("l1_pair_ask_gt_cap")
            return
        if ts_ms - self.last_seed_ms < self.cfg.cooldown_ms:
            block_seed("cooldown")
            return

        same_qty = self.exposure_qty(side)
        opp_qty = self.exposure_qty(opp(side))
        target_qty = self.cfg.target_for(offset)
        seed_px = max(0.01, px - self.cfg.edge)
        opposite_seen_ms = self.activation_last_seen_ms.get(opp(side))
        same_cost_before = self.exposure_cost(side)
        opp_cost_before = self.exposure_cost(opp(side))
        if self.cfg.pairing_only_when_residual and same_qty > opp_qty + self.cfg.dust_qty:
            block_seed("pairing_only_when_residual", target_qty=target_qty, opposite_seen_ms=opposite_seen_ms)
            self.record_activation_seen(side, ts_ms)
            return
        if self.cfg.late_repair_only_active(offset) and same_qty >= opp_qty:
            block_seed("late_repair_only", target_qty=target_qty, opposite_seen_ms=opposite_seen_ms)
            self.record_activation_seen(side, ts_ms)
            return
        if self.cfg.late_repair_active(offset) and same_qty + self.cfg.dust_qty >= opp_qty:
            block_seed("late_repair_only", target_qty=target_qty, opposite_seen_ms=opposite_seen_ms)
            self.record_activation_seen(side, ts_ms)
            return
        risk_increasing_seed = same_qty + self.cfg.dust_qty >= opp_qty
        activation_ok, activation_opp_age_ms = self.activation_allows_seed(side, ts_ms)
        if risk_increasing_seed and not activation_ok:
            blocked_quote_intent_id = self.blocked_quote_intent_id(side, ts_ms)
            diagnostic_qty = min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut, max(0.0, target_qty - same_qty))
            self.record_symmetric_activation_diagnostic(
                status="blocked",
                reason="activation_opp_seen",
                side=side,
                offset_s=offset,
                qty=diagnostic_qty,
                seed_px=seed_px,
                same_qty=same_qty,
                opp_qty=opp_qty,
                same_cost=same_cost_before,
                opp_cost=opp_cost_before,
                activation_required=True,
                activation_opp_age_ms=activation_opp_age_ms,
                opposite_seen_ms=opposite_seen_ms,
                source_sequence_id=trigger_source_sequence_id,
            )
            block_seed(
                "activation_opp_seen",
                target_qty=target_qty,
                activation_opp_age_ms=activation_opp_age_ms,
                opposite_seen_ms=opposite_seen_ms,
                quote_intent_id=blocked_quote_intent_id,
            )
            self.emit(
                {
                    "kind": "activation_block",
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "quote_intent_id": blocked_quote_intent_id,
                    "side": side,
                    "price": seed_px,
                    "size": min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut),
                    "placed_ts_ms": ts_ms,
                    "accepted_ts_ms": None,
                    "source": "no_order_public_trade_activation_gate",
                    "source_sequence_id": trigger_source_sequence_id,
                    "market_md_source_sequence_id": trigger_source_sequence_id,
                    "trigger_ts_ms": ts_ms,
                    "trigger_event_time_ms": trigger_event_time_ms,
                    "public_trade_px": px,
                    "public_trade_size": size,
                    "offset_s": offset,
                    "activation_mode": self.cfg.activation_mode,
                    "activation_window_s": self.cfg.activation_window_s,
                    "activation_required": True,
                    "risk_increasing_seed": risk_increasing_seed,
                    "opposite_trigger_ts_ms": opposite_seen_ms,
                    "opp_last_seen_ms": opposite_seen_ms,
                }
            )
            self.record_activation_seen(side, ts_ms)
            return
        if same_qty >= target_qty - self.cfg.dust_qty:
            block_seed("target", target_qty=target_qty, opposite_seen_ms=opposite_seen_ms)
            self.record_activation_seen(side, ts_ms)
            return
        if max(0.0, self.exposure_cost(side) - self.exposure_cost(opp(side))) > self.cfg.imbalance_cost_cap + 1e-12:
            block_seed("imbalance_cost", target_qty=target_qty, opposite_seen_ms=opposite_seen_ms)
            self.record_activation_seen(side, ts_ms)
            return
        imbalance_room = self.cfg.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        if imbalance_room <= self.cfg.dust_qty:
            block_seed(
                "imbalance_qty",
                target_qty=target_qty,
                imbalance_room=imbalance_room,
                opposite_seen_ms=opposite_seen_ms,
            )
            self.record_activation_seen(side, ts_ms)
            return

        micro_deficit_repair_candidate = is_micro_deficit_repair_context(
            same_qty,
            opp_qty,
            self.cfg.micro_deficit_repair_max_deficit_qty,
            self.cfg.micro_deficit_repair_open_qty_cap,
        )
        micro_deficit_repair_deficit = max(0.0, opp_qty - same_qty)
        if micro_deficit_repair_candidate:
            self.metrics.micro_deficit_repair_guard_candidates += 1
        if self.cfg.micro_deficit_repair_guard and micro_deficit_repair_candidate:
            self.metrics.micro_deficit_repair_guard_blocks += 1
            block_seed(
                "micro_deficit_repair_guard",
                target_qty=target_qty,
                imbalance_room=imbalance_room,
                opposite_seen_ms=opposite_seen_ms,
            )
            self.record_activation_seen(side, ts_ms)
            return

        room_cost = self.cfg.max_open_cost - self.total_open_cost()
        base_qty = min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut, target_qty - same_qty, room_cost / max(seed_px, 1e-9), imbalance_room)
        fill_to_balance_active = self.cfg.late_repair_fill_to_balance_active(offset) and same_qty < opp_qty
        fill_to_balance_deficit = max(0.0, opp_qty - same_qty) if fill_to_balance_active else None
        qty = base_qty
        if fill_to_balance_active:
            self.metrics.late_repair_fill_to_balance_candidates += 1
            capped_qty = min(qty, fill_to_balance_deficit or 0.0)
            qty_reduction = max(0.0, qty - capped_qty)
            if qty_reduction > DUST:
                self.metrics.late_repair_fill_to_balance_caps += 1
                self.metrics.late_repair_fill_to_balance_qty_reduction += qty_reduction
            f2b_status = "block" if capped_qty <= self.cfg.dust_qty else ("cap" if qty_reduction > DUST else "uncapped")
            self.record_fill_to_balance_diagnostic(
                side=side,
                offset_s=offset,
                deficit=fill_to_balance_deficit,
                base_seed_qty=base_qty,
                capped_qty=capped_qty,
                qty_reduction=qty_reduction,
                status=f2b_status,
            )
            qty = capped_qty
        if qty <= self.cfg.dust_qty:
            if fill_to_balance_active:
                self.metrics.late_repair_fill_to_balance_blocks += 1
                block_seed(
                    "late_repair_fill_to_balance_qty",
                    qty=qty,
                    base_qty=base_qty,
                    target_qty=target_qty,
                    room_cost=room_cost,
                    imbalance_room=imbalance_room,
                    activation_opp_age_ms=activation_opp_age_ms,
                    opposite_seen_ms=opposite_seen_ms,
                )
            else:
                block_seed(
                    "qty_zero",
                    qty=qty,
                    base_qty=base_qty,
                    target_qty=target_qty,
                    room_cost=room_cost,
                    imbalance_room=imbalance_room,
                    activation_opp_age_ms=activation_opp_age_ms,
                    opposite_seen_ms=opposite_seen_ms,
                )
            self.record_activation_seen(side, ts_ms)
            return

        ledger_proxy_before = self.online_ledger_proxy()
        ledger_proxy_after = ledger_proxy_before - qty * seed_px - 0.01 * qty
        source_risk_direction = inventory_risk_direction(same_qty, opp_qty)
        quote_intent_id = self.quote_intent_id(self.next_order_id)
        opposite_trigger_ts_ms = (
            ts_ms - activation_opp_age_ms if activation_opp_age_ms is not None else opposite_seen_ms
        )
        projected_context = self.ce25_projected_guard_context(
            side=side,
            offset_s=offset,
            qty=qty,
            seed_px=seed_px,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost_before,
            opp_cost=opp_cost_before,
        )
        order = VirtualOrder(
            id=self.next_order_id,
            quote_intent_id=quote_intent_id,
            condition_id=self.condition_id,
            side=side,
            px=seed_px,
            qty=qty,
            created_ms=ts_ms,
            accepted_ms=ts_ms,
            offset_s=float(offset),
            trigger_px=px,
            trigger_size=size,
            trigger_ts_ms=ts_ms,
            trigger_source_sequence_id=trigger_source_sequence_id,
            opposite_trigger_ts_ms=opposite_trigger_ts_ms,
            source_risk_direction=source_risk_direction,
            micro_deficit_repair_guard_candidate=micro_deficit_repair_candidate,
            micro_deficit_repair_deficit=micro_deficit_repair_deficit,
            pre_seed_same_qty=same_qty,
            pre_seed_opp_qty=opp_qty,
            pre_seed_same_cost=same_cost_before,
            pre_seed_opp_cost=opp_cost_before,
            ledger_proxy_before=ledger_proxy_before,
            activation_required=risk_increasing_seed and self.cfg.activation_mode != "none",
            activation_opp_age_ms=activation_opp_age_ms,
            opposite_seen_ms=opposite_seen_ms,
            symmetric_activation_status="admitted",
            symmetric_activation_reason="candidate",
            symmetric_projected_pair_cost_bucket=ce25_projected_pair_cost_bucket(
                projected_context.get("projected_pair_cost")
            ),
            symmetric_projected_residual_bucket=ce25_projected_residual_bucket(
                projected_context.get("projected_residual_rate_on_bought_qty")
            ),
        )
        self.record_ce25_projected_guard_diagnostic(
            side=side,
            offset_s=offset,
            qty=qty,
            seed_px=seed_px,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost_before,
            opp_cost=opp_cost_before,
        )
        self.record_symmetric_activation_diagnostic(
            status="admitted",
            reason="candidate",
            side=side,
            offset_s=offset,
            qty=qty,
            seed_px=seed_px,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost_before,
            opp_cost=opp_cost_before,
            activation_required=risk_increasing_seed and self.cfg.activation_mode != "none",
            activation_opp_age_ms=activation_opp_age_ms,
            opposite_seen_ms=opposite_seen_ms,
            source_sequence_id=trigger_source_sequence_id,
        )
        self.record_source_opportunity_marker(
            status="admitted",
            reason="candidate",
            side=side,
            offset_s=offset,
            qty=qty,
            base_qty=base_qty,
            target_qty=target_qty,
            room_cost=room_cost,
            imbalance_room=imbalance_room,
            same_qty=same_qty,
            opp_qty=opp_qty,
            activation_opp_age_ms=activation_opp_age_ms,
            opposite_seen_ms=opposite_seen_ms,
            quote_intent_id=quote_intent_id,
            source_order_id=order.id,
            source_sequence_id=trigger_source_sequence_id,
            ledger_proxy_before=ledger_proxy_before,
            ledger_proxy_after=ledger_proxy_after,
        )
        self.next_order_id += 1
        self.pending.append(order)
        self.last_seed_ms = ts_ms
        self.metrics.candidates += 1
        self.metrics.seed_qty += qty
        self.metrics.seed_cost += qty * seed_px
        ledger_proxy_after = self.online_ledger_proxy()
        order.ledger_proxy_after = ledger_proxy_after
        self.record_event_lite_candidate(side, seed_px, px, offset, qty)
        self.record_source_link_transition(
            status="admitted",
            reason="candidate",
            side=side,
            offset_s=offset,
            qty=qty,
            same_qty=same_qty,
            opp_qty=opp_qty,
            same_cost=same_cost_before,
            opp_cost=opp_cost_before,
            ledger_proxy_before=ledger_proxy_before,
            ledger_proxy_after=ledger_proxy_after,
            quote_intent_id=quote_intent_id,
            source_order_id=order.id,
            source_sequence_id=trigger_source_sequence_id,
        )
        self.emit({"kind": "candidate", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "placed_ts_ms": ts_ms, "accepted_ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": side, "price": seed_px, "size": qty, "offset_s": offset, "public_trade_px": px, "public_trade_size": size, "trigger_ts_ms": ts_ms, "trigger_event_time_ms": trigger_event_time_ms, "source": "no_order_public_trade_candidate", "source_sequence_id": trigger_source_sequence_id, "market_md_source_sequence_id": trigger_source_sequence_id, "opposite_trigger_ts_ms": opposite_trigger_ts_ms, "seed_px": seed_px, "qty": qty, "edge": self.cfg.edge, "queue_share": self.cfg.queue_share, "l1_pair_ask": l1_pair, "same_exposure_qty": same_qty, "opp_exposure_qty": opp_qty, "target_qty": target_qty, "base_target_qty": self.cfg.target_qty, "base_seed_qty": base_qty, "late_target_active": self.cfg.late_target_active(offset), "late_repair_active": self.cfg.late_repair_active(offset), "late_repair_only_active": self.cfg.late_repair_only_active(offset), "late_repair_fill_to_balance_active": fill_to_balance_active, "late_repair_fill_to_balance_deficit": fill_to_balance_deficit, "late_repair_fill_to_balance_qty_reduction": round(base_qty - qty, 6) if fill_to_balance_active else 0.0, "micro_deficit_repair_guard_candidate": micro_deficit_repair_candidate, "micro_deficit_repair_deficit": round(micro_deficit_repair_deficit, 6), "micro_deficit_repair_guard_enabled": self.cfg.micro_deficit_repair_guard, "activation_mode": self.cfg.activation_mode, "activation_window_s": self.cfg.activation_window_s, "activation_required": risk_increasing_seed and self.cfg.activation_mode != "none", "risk_increasing_seed": risk_increasing_seed, "activation_opp_age_ms": activation_opp_age_ms, "open_cost": self.total_open_cost(), "yes_bid": self.book.get("yes_bid"), "yes_ask": yes_ask, "no_bid": self.book.get("no_bid"), "no_ask": no_ask})
        self.record_activation_seen(side, ts_ms)

    def write_summary(self, final: bool = False) -> None:
        summary_ts_ms = now_ms()
        if final:
            for order in self.pending_orders():
                order.cancel_ms = summary_ts_ms
                order.cancel_reason = "final"
                self.metrics.cancelled_orders += 1
                if order.first_bid_touch_ms or order.first_trade_touch_ms:
                    self.metrics.touch_only_orders += 1
        residual_lots = [lot for side in ("YES", "NO") for lot in self.lots[side] if lot.qty > DUST]
        residual_qty = sum(lot.qty for lot in residual_lots)
        residual_cost = sum(lot.cost for lot in residual_lots)
        material = [lot for lot in residual_lots if lot.qty > 6 or lot.cost > 6]
        self.metrics.residual_qty = residual_qty
        self.metrics.residual_cost = residual_cost
        m = self.metrics
        summary = {
            "kind": "summary",
            "script": "xuan_dplus_passive_passive_shadow_runner.py",
            "slug": self.slug,
            "final": final,
            "ts_ms": summary_ts_ms,
            "book_ticks": self.book_ticks,
            "trade_ticks": self.trade_ticks,
            "sell_triggers": self.sell_triggers,
            "blocked": self.blocked,
            "config": self.cfg.__dict__,
            "metrics": {
                "candidates": m.candidates,
                "queue_supported_fills": m.queue_supported_fills,
                "fill_rate": m.queue_supported_fills / m.candidates if m.candidates else 0.0,
                "touch_only_orders": m.touch_only_orders,
                "cancelled_orders": m.cancelled_orders,
                "pair_actions": m.pair_actions,
                "salvage_actions": m.salvage_actions,
                "completion_per_fill": m.pair_actions / m.queue_supported_fills if m.queue_supported_fills else 0.0,
                "seed_qty": round(m.seed_qty, 6),
                "seed_cost": round(m.seed_cost, 6),
                "filled_qty": round(m.filled_qty, 6),
                "filled_cost": round(m.filled_cost, 6),
                "pair_qty": round(m.pair_qty, 6),
                "qty_pair_share_of_filled": (2 * m.pair_qty / m.filled_qty) if m.filled_qty else 0.0,
                "pair_pnl": round(m.pair_pnl, 6),
                "taker_fee": round(m.taker_fee, 6),
                "completion_cost": round(m.completion_cost, 6),
                "roi_on_seed_cost": m.pair_pnl / m.seed_cost if m.seed_cost else 0.0,
                "roi_on_filled_cost": m.pair_pnl / m.filled_cost if m.filled_cost else 0.0,
                "residual_qty": round(residual_qty, 6),
                "residual_cost": round(residual_cost, 6),
                "material_residual_lots": len(material),
                "late_repair_fill_to_balance_candidates": m.late_repair_fill_to_balance_candidates,
                "late_repair_fill_to_balance_caps": m.late_repair_fill_to_balance_caps,
                "late_repair_fill_to_balance_blocks": m.late_repair_fill_to_balance_blocks,
                "late_repair_fill_to_balance_qty_reduction": round(m.late_repair_fill_to_balance_qty_reduction, 6),
                "micro_deficit_repair_guard_candidates": m.micro_deficit_repair_guard_candidates,
                "micro_deficit_repair_guard_blocks": m.micro_deficit_repair_guard_blocks,
                "pair_cost_p50": pct(m.pair_costs, 0.5),
                "pair_cost_p90": pct(m.pair_costs, 0.9),
                "net_pair_cost_p50": pct(m.net_pair_costs, 0.5),
                "net_pair_cost_p90": pct(m.net_pair_costs, 0.9),
                "fill_wait_p50_ms": pct(m.fill_wait_ms, 0.5),
                "fill_wait_p90_ms": pct(m.fill_wait_ms, 0.9),
                "pair_wait_p50_ms": pct(m.pair_wait_ms, 0.5),
                "pair_wait_p90_ms": pct(m.pair_wait_ms, 0.9),
                "salvage_wait_p50_ms": pct(m.salvage_wait_ms, 0.5),
                "salvage_wait_p90_ms": pct(m.salvage_wait_ms, 0.9),
            },
            "top_residual_lots": [
                {"lot_id": lot.id, "quote_intent_id": lot.quote_intent_id, "side": lot.side, "qty": round(lot.qty, 6), "cost": round(lot.cost, 6), "px": lot.px, "age_ms": summary_ts_ms - lot.fill_ms, "source_order_id": lot.source_order_id}
                for lot in sorted(residual_lots, key=lambda x: x.cost, reverse=True)[:10]
            ],
        }
        if self.cfg.event_lite_summary:
            residual_lot_px_buckets: dict[str, float] = {}
            residual_lot_side_qty: dict[str, float] = {}
            residual_lot_side_cost: dict[str, float] = {}
            residual_lot_side_count: dict[str, float] = {}
            for lot in residual_lots:
                add_count(residual_lot_px_buckets, px_bucket(lot.px), lot.qty)
                add_count(residual_lot_side_qty, lot.side, lot.qty)
                add_count(residual_lot_side_cost, lot.side, lot.cost)
                add_count(residual_lot_side_count, lot.side)
            pair_cost_buckets: dict[str, float] = {}
            net_pair_cost_buckets: dict[str, float] = {}
            for cost in m.pair_costs:
                add_count(pair_cost_buckets, pair_cost_bucket(cost))
            for cost in m.net_pair_costs:
                add_count(net_pair_cost_buckets, pair_cost_bucket(cost))
            summary["event_lite"] = {
                "candidate_seed_px_buckets": self.event_lite_candidate_seed_px_buckets,
                "candidate_public_trade_px_buckets": self.event_lite_candidate_public_trade_px_buckets,
                "candidate_side_counts": self.event_lite_candidate_side_counts,
                "candidate_offset_buckets": self.event_lite_candidate_offset_buckets,
                "candidate_qty_by_seed_px_bucket": self.event_lite_candidate_qty_by_seed_px_bucket,
                "fill_seed_px_buckets": self.event_lite_fill_seed_px_buckets,
                "fill_side_counts": self.event_lite_fill_side_counts,
                "block_by_reason_side": self.event_lite_block_by_reason_side,
                "block_by_reason_public_px_bucket": self.event_lite_block_by_reason_public_px_bucket,
                "block_by_reason_offset_bucket": self.event_lite_block_by_reason_offset_bucket,
                "residual_lot_px_qty_buckets": residual_lot_px_buckets,
                "residual_lot_side_qty": residual_lot_side_qty,
                "residual_lot_side_cost": residual_lot_side_cost,
                "residual_lot_side_count": residual_lot_side_count,
                "pair_cost_buckets": pair_cost_buckets,
                "net_pair_cost_buckets": net_pair_cost_buckets,
            }
            if self.cfg.pair_source_event_lite_summary:
                summary["event_lite"].update(
                    {
                        "pair_source_action_count": self.event_lite_pair_source_action_count,
                        "pair_source_record_count": self.event_lite_pair_source_record_count,
                        "pair_cost_by_source_seed_px_bucket": self.event_lite_pair_cost_by_source_seed_px_bucket,
                        "pair_cost_by_source_public_px_bucket": self.event_lite_pair_cost_by_source_public_px_bucket,
                        "pair_cost_by_source_offset_bucket": self.event_lite_pair_cost_by_source_offset_bucket,
                        "pair_cost_by_source_side": self.event_lite_pair_cost_by_source_side,
                        "top_high_cost_pair_sources": sorted(
                            self.event_lite_top_high_cost_pair_sources,
                            key=lambda item: item.get("net_pair_cost", 0.0),
                            reverse=True,
                        )[:20],
                    }
                )
            if self.cfg.fill_to_balance_diagnostic_event_lite_summary:
                summary["event_lite"]["late_repair_fill_to_balance_diagnostics"] = self.event_lite_f2b_diagnostics
            if self.cfg.portfolio_ledger_event_lite_summary:
                summary["event_lite"]["portfolio_ledger_diagnostics"] = self.portfolio_ledger_diagnostics(residual_lots)
            if self.cfg.source_link_transition_event_lite_summary:
                summary["event_lite"]["source_link_transition_diagnostics"] = self.source_link_transition_diagnostics(residual_lots)
            if self.cfg.source_link_residual_tail_exemplars_event_lite_summary:
                summary["event_lite"]["source_link_residual_tail_exemplars"] = self.source_link_residual_tail_exemplars(
                    residual_lots,
                    summary_ts_ms,
                )
            if self.cfg.source_opportunity_marker_event_lite_summary:
                summary["event_lite"]["source_opportunity_marker_summary"] = self.event_lite_source_opportunity_markers
            if self.cfg.ce25_projected_guard_event_lite_summary:
                summary["event_lite"]["ce25_projected_guard_summary"] = self.event_lite_ce25_projected_guard_summary
            if self.cfg.symmetric_activation_event_lite_summary:
                summary["event_lite"]["symmetric_activation_summary"] = self.event_lite_symmetric_activation_summary
            if self.cfg.symmetric_activation_projected_residual_tail_join_event_lite_summary:
                projected_tail_diag = json.loads(
                    json.dumps(self.event_lite_symmetric_activation_projected_residual_tail_join_summary)
                )
                for lot in residual_lots:
                    status_reason_projected, side_offset_projected = (
                        self.symmetric_activation_projected_residual_keys_for_source(lot)
                    )
                    add_count(
                        projected_tail_diag["residual_qty_sum_by_status_reason_projected_residual_bucket"],
                        status_reason_projected,
                        lot.qty,
                    )
                    add_count(
                        projected_tail_diag["residual_cost_sum_by_status_reason_projected_residual_bucket"],
                        status_reason_projected,
                        lot.cost,
                    )
                    add_count(
                        projected_tail_diag[
                            "residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket"
                        ],
                        side_offset_projected,
                        lot.qty,
                    )
                    add_nested_count(
                        projected_tail_diag[
                            "source_sequence_presence_by_status_reason_projected_residual_bucket"
                        ],
                        status_reason_projected,
                        "present" if lot.trigger_source_sequence_id is not None else "missing",
                        lot.qty,
                    )
                summary["event_lite"]["symmetric_activation_projected_residual_tail_join_summary"] = (
                    projected_tail_diag
                )
            if self.cfg.symmetric_activation_tail_attribution_event_lite_summary:
                tail_diag = json.loads(json.dumps(self.event_lite_symmetric_activation_tail_attribution_summary))
                for lot in residual_lots:
                    status_reason_activation, side_offset_activation = self.symmetric_activation_keys_for_source(lot)
                    add_count(tail_diag["residual_qty_sum_by_status_reason_activation_bucket"], status_reason_activation, lot.qty)
                    add_count(tail_diag["residual_cost_sum_by_status_reason_activation_bucket"], status_reason_activation, lot.cost)
                    add_nested_count(
                        tail_diag["source_sequence_presence_by_status_reason_activation_bucket"],
                        status_reason_activation,
                        "present" if lot.trigger_source_sequence_id is not None else "missing",
                        lot.qty,
                    )
                    add_count(
                        tail_diag["residual_qty_sum_by_status_reason_side_offset_activation_bucket"],
                        side_offset_activation,
                        lot.qty,
                    )
                    add_count(
                        tail_diag["residual_cost_sum_by_status_reason_side_offset_activation_bucket"],
                        side_offset_activation,
                        lot.cost,
                    )
                    add_nested_count(
                        tail_diag["source_sequence_presence_by_status_reason_side_offset_activation_bucket"],
                        side_offset_activation,
                        "present" if lot.trigger_source_sequence_id is not None else "missing",
                        lot.qty,
                    )
                tail_diag["residual_tail_exemplars_by_status_reason_activation_bucket"] = (
                    self.symmetric_activation_tail_residual_exemplars(residual_lots, summary_ts_ms)
                )
                summary["event_lite"]["symmetric_activation_tail_attribution_summary"] = tail_diag
        self.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def handle_market_message(runner: DPlusRunner, msg: dict[str, Any]) -> None:
    kind = msg.get("kind")
    if kind == "market_book_tick":
        runner.on_book(msg)
    elif kind == "market_trade_tick":
        runner.on_trade(msg)


async def run_market(market: dict[str, str], socket_path: str, out: Path, duration_s: int, cfg: RunnerConfig) -> None:
    slug = market["POLYMARKET_MARKET_SLUG"]
    runner = DPlusRunner(slug, out, cfg, condition_id=market.get("POLYMARKET_MARKET_ID"))
    reader, writer = await asyncio.open_unix_connection(socket_path)
    req = {
        "stream": "market",
        "symbols": [],
        "market_slug": slug,
        "market_id": market["POLYMARKET_MARKET_ID"],
        "yes_asset_id": market["POLYMARKET_YES_ASSET_ID"],
        "no_asset_id": market["POLYMARKET_NO_ASSET_ID"],
        "ws_base_url": "wss://ws-subscriptions-clob.polymarket.com/ws",
        "custom_feature_enabled": False,
    }
    writer.write((json.dumps(req, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    deadline = time.monotonic() + duration_s
    last_summary = 0.0
    try:
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                raw = b""
            if raw:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                handle_market_message(runner, msg)
            if time.monotonic() - last_summary >= 30:
                runner.write_summary(False)
                last_summary = time.monotonic()
    finally:
        runner.write_summary(True)
        writer.close()
        await writer.wait_closed()


async def run_market_profiles(
    market: dict[str, str],
    socket_path: str,
    out: Path,
    duration_s: int,
    profile_cfgs: dict[str, RunnerConfig],
) -> None:
    slug = market["POLYMARKET_MARKET_SLUG"]
    runners: dict[str, DPlusRunner] = {}
    for profile, cfg in profile_cfgs.items():
        profile_out = out / profile
        profile_out.mkdir(parents=True, exist_ok=True)
        runners[profile] = DPlusRunner(slug, profile_out, cfg, condition_id=market.get("POLYMARKET_MARKET_ID"))
    reader, writer = await asyncio.open_unix_connection(socket_path)
    req = {
        "stream": "market",
        "symbols": [],
        "market_slug": slug,
        "market_id": market["POLYMARKET_MARKET_ID"],
        "yes_asset_id": market["POLYMARKET_YES_ASSET_ID"],
        "no_asset_id": market["POLYMARKET_NO_ASSET_ID"],
        "ws_base_url": "wss://ws-subscriptions-clob.polymarket.com/ws",
        "custom_feature_enabled": False,
    }
    writer.write((json.dumps(req, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    deadline = time.monotonic() + duration_s
    last_summary = 0.0
    try:
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                raw = b""
            if raw:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                for runner in runners.values():
                    handle_market_message(runner, msg)
            if time.monotonic() - last_summary >= 30:
                for runner in runners.values():
                    runner.write_summary(False)
                last_summary = time.monotonic()
    finally:
        for runner in runners.values():
            runner.write_summary(True)
        writer.close()
        await writer.wait_closed()


def resolve_markets(repo: Path, prefix: str, offsets: str) -> list[dict[str, str]]:
    markets: list[dict[str, str]] = []
    for raw_offset in offsets.split(","):
        offset = raw_offset.strip()
        if not offset:
            continue
        proc = subprocess.run(
            ["/usr/bin/python3", str(repo / "scripts/resolve_market_ids.py"), "--prefix", prefix, "--round-offset", offset, "--format", "env"],
            cwd=repo,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        env = parse_env_exports(proc.stdout)
        if env:
            markets.append(env)
    return markets


def aggregate(out: Path) -> dict[str, Any]:
    summaries = []
    for path in sorted(out.glob("*.summary.json")):
        try:
            summaries.append(json.loads(path.read_text()))
        except Exception:
            continue
    totals: dict[str, float] = {}
    blocked: dict[str, int] = {}
    pair_costs: list[float] = []
    net_pair_costs: list[float] = []
    fill_waits: list[float] = []
    pair_waits: list[float] = []
    top_residual: list[dict[str, Any]] = []
    event_lite: dict[str, Any] = {
        "candidate_seed_px_buckets": {},
        "candidate_public_trade_px_buckets": {},
        "candidate_side_counts": {},
        "candidate_offset_buckets": {},
        "candidate_qty_by_seed_px_bucket": {},
        "fill_seed_px_buckets": {},
        "fill_side_counts": {},
        "residual_lot_px_qty_buckets": {},
        "residual_lot_side_qty": {},
        "residual_lot_side_cost": {},
        "residual_lot_side_count": {},
        "pair_cost_buckets": {},
        "net_pair_cost_buckets": {},
        "block_by_reason_side": {},
        "block_by_reason_public_px_bucket": {},
        "block_by_reason_offset_bucket": {},
    }
    event_lite_seen = False
    for s in summaries:
        for k, v in s.get("blocked", {}).items():
            blocked[k] = blocked.get(k, 0) + int(v)
        m = s.get("metrics", {})
        for k, v in m.items():
            if isinstance(v, (int, float)) and not k.endswith(("_p50", "_p90", "_avg", "_rate")):
                totals[k] = totals.get(k, 0.0) + float(v)
        for k, dest in [("pair_cost_p50", pair_costs), ("pair_cost_p90", pair_costs), ("net_pair_cost_p50", net_pair_costs), ("net_pair_cost_p90", net_pair_costs), ("fill_wait_p50_ms", fill_waits), ("fill_wait_p90_ms", fill_waits), ("pair_wait_p50_ms", pair_waits), ("pair_wait_p90_ms", pair_waits)]:
            if m.get(k) is not None:
                dest.append(float(m[k]))
        for lot in s.get("top_residual_lots", []):
            lot["slug"] = s.get("slug")
            top_residual.append(lot)
        lite = s.get("event_lite")
        if isinstance(lite, dict):
            event_lite_seen = True
            for key in (
                "candidate_seed_px_buckets",
                "candidate_public_trade_px_buckets",
                "candidate_side_counts",
                "candidate_offset_buckets",
                "candidate_qty_by_seed_px_bucket",
                "fill_seed_px_buckets",
                "fill_side_counts",
                "residual_lot_px_qty_buckets",
                "residual_lot_side_qty",
                "residual_lot_side_cost",
                "residual_lot_side_count",
                "pair_cost_buckets",
                "net_pair_cost_buckets",
            ):
                source = lite.get(key)
                if isinstance(source, dict):
                    merge_count_hist(event_lite[key], source)
            for key in (
                "block_by_reason_side",
                "block_by_reason_public_px_bucket",
                "block_by_reason_offset_bucket",
            ):
                source = lite.get(key)
                if isinstance(source, dict):
                    merge_nested_count_hist(event_lite[key], source)
            for key in (
                "pair_cost_by_source_seed_px_bucket",
                "pair_cost_by_source_public_px_bucket",
                "pair_cost_by_source_offset_bucket",
                "pair_cost_by_source_side",
            ):
                source = lite.get(key)
                if isinstance(source, dict):
                    merge_nested_count_hist(event_lite.setdefault(key, {}), source)
            for key in ("pair_source_action_count", "pair_source_record_count"):
                value = lite.get(key)
                if isinstance(value, (int, float)):
                    event_lite[key] = round(float(event_lite.get(key, 0.0)) + float(value), 6)
            f2b_diag = lite.get("late_repair_fill_to_balance_diagnostics")
            if isinstance(f2b_diag, dict):
                dest_diag = event_lite.setdefault("late_repair_fill_to_balance_diagnostics", {})
                for key in (
                    "candidate_count_by_side",
                    "candidate_count_by_offset_bucket",
                    "candidate_count_by_side_offset",
                    "qty_reduction_amount_by_side",
                    "qty_reduction_amount_by_offset_bucket",
                    "qty_reduction_amount_by_side_offset",
                ):
                    source = f2b_diag.get(key)
                    if isinstance(source, dict):
                        merge_count_hist(dest_diag.setdefault(key, {}), source)
                for key in (
                    "status_count_by_side",
                    "status_count_by_offset_bucket",
                    "status_count_by_side_offset",
                    "deficit_bucket_by_side_offset",
                    "base_seed_qty_bucket_by_side_offset",
                    "capped_qty_bucket_by_side_offset",
                    "qty_reduction_bucket_by_side_offset",
                ):
                    source = f2b_diag.get(key)
                    if isinstance(source, dict):
                        merge_nested_count_hist(dest_diag.setdefault(key, {}), source)
            portfolio_diag = lite.get("portfolio_ledger_diagnostics")
            if isinstance(portfolio_diag, dict):
                merge_portfolio_ledger_diagnostics(
                    event_lite.setdefault("portfolio_ledger_diagnostics", {}),
                    portfolio_diag,
                )
            source_link_diag = lite.get("source_link_transition_diagnostics")
            if isinstance(source_link_diag, dict):
                merge_source_link_transition_diagnostics(
                    event_lite.setdefault("source_link_transition_diagnostics", {}),
                    source_link_diag,
                )
            source_contexts = lite.get("top_high_cost_pair_sources")
            if isinstance(source_contexts, list):
                top_sources = event_lite.setdefault("top_high_cost_pair_sources", [])
                for item in source_contexts:
                    if isinstance(item, dict):
                        if "slug" not in item:
                            item = {**item, "slug": s.get("slug")}
                        top_sources.append(item)
            residual_tail_exemplars = lite.get("source_link_residual_tail_exemplars")
            if isinstance(residual_tail_exemplars, list):
                top_tail = event_lite.setdefault("source_link_residual_tail_exemplars", [])
                for item in residual_tail_exemplars:
                    if isinstance(item, dict):
                        if "slug" not in item:
                            item = {**item, "slug": s.get("slug")}
                        top_tail.append(item)
            source_opportunity_marker = lite.get("source_opportunity_marker_summary")
            if isinstance(source_opportunity_marker, dict):
                merge_source_opportunity_marker_summary(
                    event_lite.setdefault("source_opportunity_marker_summary", {}),
                    source_opportunity_marker,
                )
            ce25_projected_guard = lite.get("ce25_projected_guard_summary")
            if isinstance(ce25_projected_guard, dict):
                merge_ce25_projected_guard_summary(
                    event_lite.setdefault("ce25_projected_guard_summary", {}),
                    ce25_projected_guard,
                )
            symmetric_activation = lite.get("symmetric_activation_summary")
            if isinstance(symmetric_activation, dict):
                merge_symmetric_activation_summary(
                    event_lite.setdefault("symmetric_activation_summary", {}),
                    symmetric_activation,
                )
            symmetric_activation_tail = lite.get("symmetric_activation_tail_attribution_summary")
            if isinstance(symmetric_activation_tail, dict):
                merge_symmetric_activation_tail_attribution_summary(
                    event_lite.setdefault("symmetric_activation_tail_attribution_summary", {}),
                    symmetric_activation_tail,
                )
            symmetric_activation_projected_residual_tail = lite.get(
                "symmetric_activation_projected_residual_tail_join_summary"
            )
            if isinstance(symmetric_activation_projected_residual_tail, dict):
                merge_symmetric_activation_projected_residual_tail_join_summary(
                    event_lite.setdefault("symmetric_activation_projected_residual_tail_join_summary", {}),
                    symmetric_activation_projected_residual_tail,
                )
    candidates = totals.get("candidates", 0.0)
    fills = totals.get("queue_supported_fills", 0.0)
    filled_qty = totals.get("filled_qty", 0.0)
    seed_cost = totals.get("seed_cost", 0.0)
    filled_cost = totals.get("filled_cost", 0.0)
    pair_qty = totals.get("pair_qty", 0.0)
    pair_pnl = totals.get("pair_pnl", 0.0)
    aggregate_report = {
        "kind": "aggregate_report",
        "script": "xuan_dplus_passive_passive_shadow_runner.py",
        "slugs": len(summaries),
        "blocked": blocked,
        "metrics": {
            **{k: round(v, 6) for k, v in totals.items()},
            "fill_rate": fills / candidates if candidates else 0.0,
            "completion_per_fill": totals.get("pair_actions", 0.0) / fills if fills else 0.0,
            "qty_pair_share_of_filled": (2 * pair_qty / filled_qty) if filled_qty else 0.0,
            "roi_on_seed_cost": pair_pnl / seed_cost if seed_cost else 0.0,
            "roi_on_filled_cost": pair_pnl / filled_cost if filled_cost else 0.0,
            "pair_cost_proxy_p50": pct(pair_costs, 0.5),
            "pair_cost_proxy_p90": pct(pair_costs, 0.9),
            "net_pair_cost_proxy_p50": pct(net_pair_costs, 0.5),
            "net_pair_cost_proxy_p90": pct(net_pair_costs, 0.9),
            "fill_wait_proxy_p50_ms": pct(fill_waits, 0.5),
            "fill_wait_proxy_p90_ms": pct(fill_waits, 0.9),
            "pair_wait_proxy_p50_ms": pct(pair_waits, 0.5),
            "pair_wait_proxy_p90_ms": pct(pair_waits, 0.9),
        },
        "top_residual_lots": sorted(top_residual, key=lambda x: x.get("cost", 0), reverse=True)[:20],
    }
    if event_lite_seen:
        top_sources = event_lite.get("top_high_cost_pair_sources")
        if isinstance(top_sources, list):
            event_lite["top_high_cost_pair_sources"] = sorted(
                top_sources,
                key=lambda item: item.get("net_pair_cost", 0.0) if isinstance(item, dict) else 0.0,
                reverse=True,
            )[:50]
        portfolio_diag = event_lite.get("portfolio_ledger_diagnostics")
        if isinstance(portfolio_diag, dict):
            finalize_portfolio_ledger_diagnostics(portfolio_diag)
        residual_tail_exemplars = event_lite.get("source_link_residual_tail_exemplars")
        if isinstance(residual_tail_exemplars, list):
            event_lite["source_link_residual_tail_exemplars"] = sorted(
                residual_tail_exemplars,
                key=lambda item: item.get("source_residual_cost", 0.0) if isinstance(item, dict) else 0.0,
                reverse=True,
            )[:50]
        symmetric_activation_tail = event_lite.get("symmetric_activation_tail_attribution_summary")
        if isinstance(symmetric_activation_tail, dict):
            exemplars = symmetric_activation_tail.get("residual_tail_exemplars_by_status_reason_activation_bucket")
            if isinstance(exemplars, list):
                symmetric_activation_tail["residual_tail_exemplars_by_status_reason_activation_bucket"] = sorted(
                    exemplars,
                    key=lambda item: item.get("residual_cost", 0.0) if isinstance(item, dict) else 0.0,
                    reverse=True,
                )[:50]
        aggregate_report["event_lite"] = event_lite
    (out / "aggregate_report.json").write_text(json.dumps(aggregate_report, indent=2, sort_keys=True) + "\n")
    return aggregate_report


def aggregate_profiles(out: Path, profile_cfgs: dict[str, RunnerConfig]) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for profile, cfg in profile_cfgs.items():
        profile_out = out / profile
        report = aggregate(profile_out)
        profiles[profile] = {
            "output_dir": str(profile_out),
            "config": cfg.__dict__,
            "aggregate_report": report,
        }
    report = {
        "kind": "multi_profile_aggregate_report",
        "script": "xuan_dplus_passive_passive_shadow_runner.py",
        "profile_axis": "late_repair_after_s",
        "profiles": profiles,
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
    }
    (out / "multi_profile_aggregate_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/srv/pm_as_ofi/repo")
    ap.add_argument("--shared-ingress-root", default="/srv/pm_as_ofi/shared-ingress-main")
    ap.add_argument("--prefix", default="btc-updown-5m")
    ap.add_argument("--round-offsets", default="0,1,2,3")
    ap.add_argument("--duration-s", type=int, default=1800)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--edge", type=float, default=0.040)
    ap.add_argument("--queue-share", type=float, default=0.50)
    ap.add_argument("--target-qty", type=float, default=5.0)
    ap.add_argument("--fill-haircut", type=float, default=0.25)
    ap.add_argument("--max-seed-qty", type=float, default=60.0)
    ap.add_argument("--max-open-cost", type=float, default=80.0)
    ap.add_argument("--seed-l1-cap", type=float, default=1.02)
    ap.add_argument("--seed-px-lo", type=float, default=0.05)
    ap.add_argument("--seed-px-hi", type=float, default=0.90)
    ap.add_argument("--seed-offset-max-s", type=float, default=120.0)
    ap.add_argument("--late-target-after-s", type=float, default=None)
    ap.add_argument("--late-target-qty", type=float, default=None)
    ap.add_argument("--late-repair-after-s", type=float, default=None)
    ap.add_argument("--late-repair-only-after-s", type=float, default=None, help="after this offset, allow only imbalance-reducing seeds")
    ap.add_argument("--late-repair-fill-to-balance-after-s", type=float, default=None, help="after this offset, cap repair seed qty to the live opposite-minus-same deficit")
    ap.add_argument("--micro-deficit-repair-guard", action="store_true", help="default-off diagnostic variant: block tiny opposite-minus-same repair seeds using pre-action inventory context")
    ap.add_argument("--micro-deficit-repair-max-deficit-qty", type=float, default=0.25)
    ap.add_argument("--micro-deficit-repair-open-qty-cap", type=float, default=1.0)
    ap.add_argument("--profile-late-repair-after-s", default="", help="CSV repair_after_s values for same-window multi-profile causal verification")
    ap.add_argument("--order-ttl-s", type=float, default=120.0)
    ap.add_argument("--imbalance-qty-cap", type=float, default=2.0)
    ap.add_argument("--imbalance-cost-cap", type=float, default=1_000_000_000.0)
    ap.add_argument("--pairing-only-when-residual", action="store_true")
    ap.add_argument("--activation-mode", choices=["none", "opp_seen"], default="none")
    ap.add_argument("--activation-window-s", type=float, default=60.0)
    ap.add_argument("--event-lite-summary", action="store_true", help="emit summary-only candidate/fill/block/residual attribution buckets without pulling events JSONL")
    ap.add_argument("--pair-source-event-lite-summary", action="store_true", help="with --event-lite-summary, attribute pair-cost buckets back to source seed/public price, offset, side, and quote ids")
    ap.add_argument("--fill-to-balance-diagnostic-event-lite-summary", action="store_true", help="with --event-lite-summary and --late-repair-fill-to-balance-after-s, emit fill-to-balance deficit/base/capped/reduction diagnostic buckets")
    ap.add_argument("--portfolio-ledger-event-lite-summary", action="store_true", help="with --event-lite-summary, emit per-condition paired-inventory and residual risk-adjusted ledger diagnostics")
    ap.add_argument("--source-link-transition-event-lite-summary", action="store_true", help="with --event-lite-summary, emit candidate transition source-link diagnostics for admitted/blocked/pair/residual paths")
    ap.add_argument("--source-link-residual-tail-exemplars-event-lite-summary", action="store_true", help="with --event-lite-summary, emit top residual source exemplars with action-level source/pair/residual fields")
    ap.add_argument("--source-opportunity-marker-event-lite-summary", action="store_true", help="with --event-lite-summary, emit admitted/blocked opportunity denominators by pre-action open/deficit/source-risk buckets")
    ap.add_argument("--source-opportunity-marker-reason-source-event-lite-summary", action="store_true", help="with --source-opportunity-marker-event-lite-summary, emit exact status/reason/marker source coverage without raw ids or post-action labels")
    ap.add_argument("--source-opportunity-ledger-marker-event-lite-summary", action="store_true", help="with --source-opportunity-marker-event-lite-summary, emit ledger-after marker denominators without changing behavior")
    ap.add_argument("--source-opportunity-ledger-before-delta-marker-event-lite-summary", action="store_true", help="with --source-opportunity-marker-event-lite-summary, emit ledger-before and ledger-delta marker denominators without changing behavior")
    ap.add_argument("--source-opportunity-closed-cycle-marker-event-lite-summary", action="store_true", help="with --source-opportunity-marker-event-lite-summary, emit closed-cycle pre-action marker denominators without changing behavior")
    ap.add_argument("--ce25-projected-guard-event-lite-summary", action="store_true", help="with --event-lite-summary, emit default-off ce25 projected pair-cost/residual guard diagnostics without changing behavior")
    ap.add_argument("--symmetric-activation-event-lite-summary", action="store_true", help="with --event-lite-summary, emit default-off symmetric activation projected pair-cost/residual diagnostics without changing behavior")
    ap.add_argument("--symmetric-activation-tail-attribution-event-lite-summary", action="store_true", help="with --event-lite-summary, emit default-off source-attributed realized pair/residual tail diagnostics joined to activation buckets without changing behavior")
    ap.add_argument("--symmetric-activation-projected-residual-tail-join-event-lite-summary", action="store_true", help="with --event-lite-summary, emit default-off realized residual/pair diagnostics joined to pre-action projected residual buckets without changing behavior")
    ap.add_argument("--salvage-net-cap", type=float, default=0.95)
    ap.add_argument("--salvage-age-s", type=float, default=30.0)
    ap.add_argument("--salvage-min-lot-cost", type=float, default=0.25)
    args = ap.parse_args()

    if (args.late_target_after_s is None) != (args.late_target_qty is None):
        ap.error("--late-target-after-s and --late-target-qty must be provided together")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    cfg = RunnerConfig(
        edge=args.edge,
        queue_share=args.queue_share,
        target_qty=args.target_qty,
        fill_haircut=args.fill_haircut,
        max_seed_qty=args.max_seed_qty,
        max_open_cost=args.max_open_cost,
        seed_l1_cap=args.seed_l1_cap,
        seed_px_lo=args.seed_px_lo,
        seed_px_hi=args.seed_px_hi,
        seed_offset_max_s=args.seed_offset_max_s,
        late_target_after_s=args.late_target_after_s,
        late_target_qty=args.late_target_qty,
        late_repair_after_s=args.late_repair_after_s,
        late_repair_only_after_s=args.late_repair_only_after_s,
        late_repair_fill_to_balance_after_s=args.late_repair_fill_to_balance_after_s,
        micro_deficit_repair_guard=args.micro_deficit_repair_guard,
        micro_deficit_repair_max_deficit_qty=args.micro_deficit_repair_max_deficit_qty,
        micro_deficit_repair_open_qty_cap=args.micro_deficit_repair_open_qty_cap,
        order_ttl_ms=int(args.order_ttl_s * 1000),
        imbalance_qty_cap=args.imbalance_qty_cap,
        imbalance_cost_cap=args.imbalance_cost_cap,
        pairing_only_when_residual=args.pairing_only_when_residual,
        activation_mode=args.activation_mode,
        activation_window_s=args.activation_window_s,
        event_lite_summary=args.event_lite_summary,
        pair_source_event_lite_summary=args.pair_source_event_lite_summary,
        fill_to_balance_diagnostic_event_lite_summary=args.fill_to_balance_diagnostic_event_lite_summary,
        portfolio_ledger_event_lite_summary=args.portfolio_ledger_event_lite_summary,
        source_link_transition_event_lite_summary=args.source_link_transition_event_lite_summary,
        source_link_residual_tail_exemplars_event_lite_summary=args.source_link_residual_tail_exemplars_event_lite_summary,
        source_opportunity_marker_event_lite_summary=args.source_opportunity_marker_event_lite_summary,
        source_opportunity_marker_reason_source_event_lite_summary=args.source_opportunity_marker_reason_source_event_lite_summary,
        source_opportunity_ledger_marker_event_lite_summary=args.source_opportunity_ledger_marker_event_lite_summary,
        source_opportunity_ledger_before_delta_marker_event_lite_summary=args.source_opportunity_ledger_before_delta_marker_event_lite_summary,
        source_opportunity_closed_cycle_marker_event_lite_summary=args.source_opportunity_closed_cycle_marker_event_lite_summary,
        ce25_projected_guard_event_lite_summary=args.ce25_projected_guard_event_lite_summary,
        symmetric_activation_event_lite_summary=args.symmetric_activation_event_lite_summary,
        symmetric_activation_tail_attribution_event_lite_summary=args.symmetric_activation_tail_attribution_event_lite_summary,
        symmetric_activation_projected_residual_tail_join_event_lite_summary=args.symmetric_activation_projected_residual_tail_join_event_lite_summary,
        salvage_net_cap=args.salvage_net_cap,
        salvage_age_ms=int(args.salvage_age_s * 1000),
        salvage_min_lot_cost=args.salvage_min_lot_cost,
    )
    if cfg.imbalance_qty_cap <= cfg.dust_qty:
        raise SystemExit(f"--imbalance-qty-cap must exceed dust_qty={cfg.dust_qty}; otherwise every seed is blocked")
    if cfg.late_target_qty is not None and cfg.late_target_qty <= cfg.dust_qty:
        raise SystemExit(f"--late-target-qty must exceed dust_qty={cfg.dust_qty}; otherwise the late window cannot seed")
    if cfg.late_repair_only_after_s is not None and cfg.late_repair_only_after_s < 0:
        raise SystemExit("--late-repair-only-after-s must be non-negative")
    if cfg.late_repair_fill_to_balance_after_s is not None:
        if cfg.late_repair_fill_to_balance_after_s < 0:
            raise SystemExit("--late-repair-fill-to-balance-after-s must be non-negative")
        if cfg.late_repair_after_s is None and cfg.late_repair_only_after_s is None:
            raise SystemExit("--late-repair-fill-to-balance-after-s requires --late-repair-after-s or --late-repair-only-after-s")
    if cfg.micro_deficit_repair_guard:
        if cfg.late_repair_after_s is None and cfg.late_repair_only_after_s is None:
            raise SystemExit("--micro-deficit-repair-guard requires --late-repair-after-s or --late-repair-only-after-s")
        if cfg.micro_deficit_repair_max_deficit_qty <= 0:
            raise SystemExit("--micro-deficit-repair-max-deficit-qty must be positive")
        if cfg.micro_deficit_repair_open_qty_cap <= 0:
            raise SystemExit("--micro-deficit-repair-open-qty-cap must be positive")
    if cfg.activation_window_s <= 0:
        raise SystemExit("--activation-window-s must be positive")
    if cfg.pair_source_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--pair-source-event-lite-summary requires --event-lite-summary")
    if cfg.fill_to_balance_diagnostic_event_lite_summary:
        if not cfg.event_lite_summary:
            raise SystemExit("--fill-to-balance-diagnostic-event-lite-summary requires --event-lite-summary")
        if cfg.late_repair_fill_to_balance_after_s is None:
            raise SystemExit("--fill-to-balance-diagnostic-event-lite-summary requires --late-repair-fill-to-balance-after-s")
    if cfg.portfolio_ledger_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--portfolio-ledger-event-lite-summary requires --event-lite-summary")
    if cfg.source_link_transition_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--source-link-transition-event-lite-summary requires --event-lite-summary")
    if cfg.source_link_residual_tail_exemplars_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--source-link-residual-tail-exemplars-event-lite-summary requires --event-lite-summary")
    if cfg.source_opportunity_marker_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--source-opportunity-marker-event-lite-summary requires --event-lite-summary")
    if cfg.source_opportunity_marker_reason_source_event_lite_summary:
        if not cfg.event_lite_summary:
            raise SystemExit("--source-opportunity-marker-reason-source-event-lite-summary requires --event-lite-summary")
        if not cfg.source_opportunity_marker_event_lite_summary:
            raise SystemExit("--source-opportunity-marker-reason-source-event-lite-summary requires --source-opportunity-marker-event-lite-summary")
    if cfg.source_opportunity_ledger_marker_event_lite_summary:
        if not cfg.event_lite_summary:
            raise SystemExit("--source-opportunity-ledger-marker-event-lite-summary requires --event-lite-summary")
        if not cfg.source_opportunity_marker_event_lite_summary:
            raise SystemExit("--source-opportunity-ledger-marker-event-lite-summary requires --source-opportunity-marker-event-lite-summary")
    if cfg.source_opportunity_ledger_before_delta_marker_event_lite_summary:
        if not cfg.event_lite_summary:
            raise SystemExit("--source-opportunity-ledger-before-delta-marker-event-lite-summary requires --event-lite-summary")
        if not cfg.source_opportunity_marker_event_lite_summary:
            raise SystemExit("--source-opportunity-ledger-before-delta-marker-event-lite-summary requires --source-opportunity-marker-event-lite-summary")
    if cfg.source_opportunity_closed_cycle_marker_event_lite_summary:
        if not cfg.event_lite_summary:
            raise SystemExit("--source-opportunity-closed-cycle-marker-event-lite-summary requires --event-lite-summary")
        if not cfg.source_opportunity_marker_event_lite_summary:
            raise SystemExit("--source-opportunity-closed-cycle-marker-event-lite-summary requires --source-opportunity-marker-event-lite-summary")
    if cfg.ce25_projected_guard_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--ce25-projected-guard-event-lite-summary requires --event-lite-summary")
    if cfg.symmetric_activation_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--symmetric-activation-event-lite-summary requires --event-lite-summary")
    if cfg.symmetric_activation_tail_attribution_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--symmetric-activation-tail-attribution-event-lite-summary requires --event-lite-summary")
    if cfg.symmetric_activation_projected_residual_tail_join_event_lite_summary and not cfg.event_lite_summary:
        raise SystemExit("--symmetric-activation-projected-residual-tail-join-event-lite-summary requires --event-lite-summary")
    profile_late_repair_after_s = parse_float_csv(args.profile_late_repair_after_s)
    if any(value <= 0 for value in profile_late_repair_after_s):
        raise SystemExit("--profile-late-repair-after-s values must be positive")
    profile_cfgs: dict[str, RunnerConfig] = {}
    for value in profile_late_repair_after_s:
        name = profile_name_for_late_repair(value)
        if name in profile_cfgs:
            raise SystemExit(f"duplicate profile late_repair_after_s={value}")
        profile_cfgs[name] = replace(cfg, late_repair_after_s=value)
    markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets)
    manifest = {
        "kind": "manifest",
        "created_ms": now_ms(),
        "script": "xuan_dplus_passive_passive_shadow_runner.py",
        "duration_s": args.duration_s,
        "markets": markets,
        "config": cfg.__dict__,
        "mode": "multi_profile_late_repair_after_s" if profile_cfgs else "single_profile",
        "profiles": {
            name: {"profile_axis": "late_repair_after_s", "late_repair_after_s": cfg.late_repair_after_s, "output_dir": str(out / name)}
            for name, cfg in profile_cfgs.items()
        },
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dry_run": os.environ.get("PM_DRY_RUN"),
            "shared_ingress_role": os.environ.get("PM_SHARED_INGRESS_ROLE"),
            "shared_ingress_root": args.shared_ingress_root,
            "instance_id": os.environ.get("PM_INSTANCE_ID"),
        },
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    socket_path = str(Path(args.shared_ingress_root) / "market.sock")
    if profile_cfgs:
        await asyncio.gather(*(run_market_profiles(m, socket_path, out, args.duration_s, profile_cfgs) for m in markets))
        print(json.dumps(aggregate_profiles(out, profile_cfgs), indent=2, sort_keys=True))
    else:
        await asyncio.gather(*(run_market(m, socket_path, out, args.duration_s, cfg) for m in markets))
        print(json.dumps(aggregate(out), indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
