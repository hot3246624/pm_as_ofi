#!/usr/bin/env python3
"""Research-only NAGI/CE25/B27BC maker queue shadow pipeline.

This script is deliberately local and no-order.  It reads already-local CSV
rows, applies CE25-style coverage gates, evaluates NAGI-style maker bid queue
proxy opportunities, and tracks B27BC-style residual closer outcomes.  It does
not connect to Polymarket, does not import credentials, and never claims maker
fill truth.  Public SELL touch and visible depth are queue proxies only.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


RESEARCH_STATUS = (
    "KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_RESEARCH_PROXY_PRIVATE_TELEMETRY_REQUIRED_NOT_READY"
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def to_int(raw: Any) -> int | None:
    value = to_float(raw)
    return int(value) if value is not None else None


def to_bool(raw: Any) -> bool | None:
    if isinstance(raw, bool):
        return raw
    value = str(raw or "").strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return True
    if value in {"0", "false", "no", "n"}:
        return False
    return None


def first_float(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        if name in row:
            value = to_float(row.get(name))
            if value is not None:
                return value
    return None


def first_int(row: dict[str, Any], names: Iterable[str]) -> int | None:
    for name in names:
        if name in row:
            value = to_int(row.get(name))
            if value is not None:
                return value
    return None


def first_bool(row: dict[str, Any], names: Iterable[str]) -> bool | None:
    for name in names:
        if name in row:
            value = to_bool(row.get(name))
            if value is not None:
                return value
    return None


def norm_side(raw: Any) -> str | None:
    value = str(raw or "").strip().upper()
    if value in {"YES", "Y", "UP"}:
        return "YES"
    if value in {"NO", "N", "DOWN"}:
        return "NO"
    return None


def opposite_side(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def norm_taker_side(raw: Any) -> str | None:
    value = str(raw or "").strip().upper()
    if value in {"BUY", "B", "TAKER_BUY", "TAKERSIDE::BUY"}:
        return "BUY"
    if value in {"SELL", "S", "TAKER_SELL", "TAKERSIDE::SELL"}:
        return "SELL"
    return None


def infer_slug(row: dict[str, Any]) -> str:
    return str(row.get("slug") or row.get("market_slug") or row.get("market_id") or "")


def infer_asset(row: dict[str, Any]) -> str:
    asset = str(row.get("asset") or row.get("symbol") or "").strip().upper()
    if asset:
        return asset.replace("/USD", "")
    slug = infer_slug(row).lower()
    if slug.startswith("btc-") or "btc-updown" in slug:
        return "BTC"
    if slug.startswith("sol-") or "sol-updown" in slug:
        return "SOL"
    if slug.startswith("eth-") or "eth-updown" in slug:
        return "ETH"
    return ""


def infer_timeframe(row: dict[str, Any]) -> str:
    tf = str(row.get("timeframe") or row.get("tf") or "").strip().lower()
    if tf:
        return tf
    slug = infer_slug(row).lower()
    if "updown-5m" in slug:
        return "5m"
    if "updown-15m" in slug:
        return "15m"
    return ""


def infer_ts_ms(row: dict[str, Any], fallback: int) -> int:
    return (
        first_int(row, ("ts_ms", "timestamp_ms", "recv_ms", "event_ts_ms", "planned_ts_ms"))
        or fallback
    )


def infer_market_end_ts_ms(row: dict[str, Any]) -> int | None:
    explicit = first_int(row, ("market_end_ts_ms", "end_ts_ms", "round_end_ts_ms"))
    if explicit is not None:
        return explicit
    secs = first_int(row, ("market_end_ts", "end_ts", "round_end_ts"))
    if secs is not None:
        return secs * 1000 if secs < 10_000_000_000 else secs
    slug = infer_slug(row)
    tail = slug.rsplit("-", 1)[-1] if slug else ""
    if tail.isdigit():
        value = int(tail)
        return value * 1000 if value < 10_000_000_000 else value
    return None


def infer_remaining_s(row: dict[str, Any], ts_ms: int) -> float | None:
    explicit = first_float(
        row,
        (
            "remaining_s",
            "remaining_secs",
            "seconds_to_end",
            "secs_to_end",
            "seconds_to_market_end",
            "secs_to_market_end",
            "time_to_end_s",
            "round_remaining_s",
        ),
    )
    if explicit is not None:
        return explicit
    end_ts_ms = infer_market_end_ts_ms(row)
    if end_ts_ms is None:
        return None
    return max(0.0, (end_ts_ms - ts_ms) / 1000.0)


def infer_row_side(row: dict[str, Any]) -> str | None:
    for name in ("side", "market_side", "outcome", "public_trade_side", "first_side"):
        side = norm_side(row.get(name))
        if side is not None:
            return side
    return None


def side_prefixes(side: str) -> tuple[str, str, str]:
    if side == "YES":
        return ("yes", "up", "YES")
    return ("no", "down", "NO")


def infer_bid_px(row: dict[str, Any], side: str) -> float | None:
    lower, alias, upper = side_prefixes(side)
    return first_float(
        row,
        (
            f"{lower}_bid",
            f"{lower}_bid_px",
            f"{alias}_bid",
            f"{alias}_bid_px",
            f"{upper}_bid",
            "bid_px",
            "best_bid",
            "maker_bid_px",
            "public_trade_px",
            "public_trade_price",
            "trade_px",
            "trade_price",
            "price",
            "first_price",
        ),
    )


def infer_public_trade_px(row: dict[str, Any]) -> float | None:
    return first_float(
        row,
        (
            "public_trade_px",
            "public_trade_price",
            "trade_px",
            "trade_price",
            "price",
            "first_price",
        ),
    )


def infer_trade_qty(row: dict[str, Any]) -> float:
    return (
        first_float(
            row,
            (
                "public_trade_qty",
                "public_trade_size",
                "trade_qty",
                "trade_size",
                "size",
                "qty",
            ),
        )
        or 0.0
    )


def infer_visible_depth_qty(row: dict[str, Any], side: str) -> float:
    lower, alias, upper = side_prefixes(side)
    total = first_float(
        row,
        (
            f"{lower}_bid_top5_size",
            f"{lower}_top5_bid_size",
            f"{alias}_bid_top5_size",
            f"{alias}_top5_bid_size",
            f"{upper}_bid_top5_size",
            "bid_top5_size",
            "top5_bid_size",
            "visible_depth_qty",
            "queue_visible_qty",
        ),
    )
    if total is not None:
        return max(0.0, total)
    size = first_float(
        row,
        (
            f"{lower}_bid_size",
            f"{lower}_bid1_size",
            f"{alias}_bid_size",
            f"{alias}_bid1_size",
            f"{upper}_bid_size",
            "bid_size",
            "bid1_size",
        ),
    )
    return max(0.0, size or 0.0)


def infer_taker_side(row: dict[str, Any]) -> str | None:
    for name in (
        "public_taker_side",
        "public_trade_taker_side",
        "trade_taker_side",
        "taker_side",
        "aggressor_side",
    ):
        side = norm_taker_side(row.get(name))
        if side is not None:
            return side
    taker_sell = first_bool(row, ("public_taker_sell", "trade_taker_sell", "taker_sell"))
    if taker_sell is not None:
        return "SELL" if taker_sell else "BUY"
    taker_buy = first_bool(row, ("public_taker_buy", "trade_taker_buy", "taker_buy"))
    if taker_buy is not None:
        return "BUY" if taker_buy else "SELL"
    return None


def infer_public_sell_touch(row: dict[str, Any], quote_px: float) -> tuple[bool, str]:
    explicit = first_bool(
        row,
        (
            "public_sell_touch",
            "sell_touch",
            "touch",
            "touched",
            "maker_bid_touched",
        ),
    )
    if explicit is not None:
        return explicit, "explicit_public_sell_touch" if explicit else "explicit_no_touch"
    taker_side = infer_taker_side(row)
    if taker_side == "SELL":
        return True, "public_taker_sell"
    if taker_side == "BUY":
        return False, "public_taker_buy"
    trade_px = infer_public_trade_px(row)
    if trade_px is not None and trade_px + 1e-12 >= quote_px:
        return True, "price_touch_inferred_no_taker_truth"
    return False, "no_public_sell_touch"


def infer_age_ms(row: dict[str, Any]) -> float | None:
    return first_float(
        row,
        (
            "l2_age_ms",
            "raw_l2_age_ms",
            "book_age_ms",
            "l1_age_ms",
        ),
    )


def infer_align_lag_ms(row: dict[str, Any]) -> float | None:
    return first_float(
        row,
        (
            "align_lag_ms",
            "l2_align_lag_ms",
            "book_align_lag_ms",
            "planned_minus_source_ts_ms",
        ),
    )


def price_band(px: float) -> str:
    if 0.20 <= px < 0.35:
        return "20_35"
    if 0.35 <= px < 0.50:
        return "35_50"
    if 0.50 <= px < 0.65:
        return "50_65"
    if 0.65 <= px < 0.80:
        return "65_80"
    return "other"


@dataclass(frozen=True)
class PipelineConfig:
    queue_conversion: float = 0.25
    queue_ahead_multiplier: float = 1.0
    max_shadow_qty: float = 60.0
    min_shadow_qty: float = 5.0
    market_order_min_usdc: float = 1.0
    max_l2_age_ms: float = 1000.0
    max_align_lag_ms: float = 1000.0
    pair_cost_cap: float = 0.995
    residual_discount_s: float = 30.0
    hard_timeout_s: float = 180.0
    suppress_yes_first_open: bool = False
    quarantine_after_residual_discount: bool = False
    allowed_coverage_gates: tuple[str, ...] = ()
    allowed_open_sides: tuple[str, ...] = ()
    require_opposite_support_before_open: bool = False
    opposite_support_lookback_s: float = 0.0
    opposite_support_min_qty: float = 5.0
    opposite_support_pair_cost_cap: float = 0.995
    per_market_residual_budget_usdc: float = 0.0
    max_open_cost_usdc: float = 0.0
    reject_yes_open_after_up_residual_events: int = 0
    reject_yes_open_when_remaining_gt_s: float = 0.0
    reject_yes_open_in_price_bands: tuple[str, ...] = ()
    reject_no_open_in_price_bands: tuple[str, ...] = ()
    bad_pair_cost_target: float = 0.35
    residual_rate_target: float = 0.12
    min_markets_for_review: int = 100


@dataclass
class CoverageDecision:
    allowed: bool
    gate_id: str
    reason: str
    band: str
    remaining_bucket: str


@dataclass
class ActiveLot:
    slug: str
    condition_id: str
    side: str
    qty: float
    first_px: float
    opened_ts_ms: int
    window_key: str
    gate_id: str

    def age_s(self, ts_ms: int) -> float:
        return max(0.0, (ts_ms - self.opened_ts_ms) / 1000.0)


@dataclass
class OppositeSupportTouch:
    side: str
    bid_px: float
    ts_ms: int
    fillable_qty: float
    touch_reason: str


@dataclass
class WindowStats:
    observed_markets: set[str] = field(default_factory=set)
    opportunity_markets: set[str] = field(default_factory=set)
    queue_proxy_open_markets: set[str] = field(default_factory=set)
    queue_proxy_close_markets: set[str] = field(default_factory=set)
    coverage_gate_counts: dict[str, int] = field(default_factory=dict)
    events: int = 0
    opportunities: int = 0
    queue_proxy_opens: int = 0
    queue_proxy_closes: int = 0
    touch_only: int = 0
    residual_timeouts: int = 0
    residual_discount_events: int = 0
    buy_actual_proxy: float = 0.0
    paired_qty: float = 0.0
    paired_buy_cost: float = 0.0
    pair_pnl_proxy: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    bad_pair_cost_buy_proxy: float = 0.0
    up_first_down_residual_risk_events: int = 0

    def note_gate(self, gate_id: str) -> None:
        self.coverage_gate_counts[gate_id] = self.coverage_gate_counts.get(gate_id, 0) + 1

    def to_row(self, window_key: str, *, cfg: PipelineConfig) -> dict[str, Any]:
        market_count = len(self.observed_markets)
        coverage = len(self.opportunity_markets) / market_count if market_count else None
        queue_market_coverage = (
            len(self.queue_proxy_open_markets) / market_count if market_count else None
        )
        pair_cost = self.paired_buy_cost / self.paired_qty if self.paired_qty else None
        bad_share = (
            self.bad_pair_cost_buy_proxy / self.buy_actual_proxy
            if self.buy_actual_proxy > 0
            else None
        )
        resid_rate = (
            self.residual_qty / (self.residual_qty + self.paired_qty)
            if self.residual_qty + self.paired_qty > 0
            else None
        )
        conservative_pnl = self.pair_pnl_proxy - self.residual_cost
        roi = conservative_pnl / self.buy_actual_proxy if self.buy_actual_proxy > 0 else None
        return {
            "window_key": window_key,
            "status": RESEARCH_STATUS,
            "evidence_level": "research_proxy",
            "maker_taker_truth": "public_queue_proxy_only",
            "fee_rate": 0.0,
            "observed_markets": market_count,
            "opportunity_markets": len(self.opportunity_markets),
            "market_coverage": coverage,
            "queue_proxy_open_markets": len(self.queue_proxy_open_markets),
            "queue_proxy_market_coverage": queue_market_coverage,
            "events": self.events,
            "opportunities": self.opportunities,
            "queue_proxy_opens": self.queue_proxy_opens,
            "queue_proxy_closes": self.queue_proxy_closes,
            "touch_only": self.touch_only,
            "buy_actual_proxy": self.buy_actual_proxy,
            "paired_qty": self.paired_qty,
            "pair_cost": pair_cost,
            "pair_pnl_proxy": self.pair_pnl_proxy,
            "residual_qty": self.residual_qty,
            "residual_cost": self.residual_cost,
            "cash_pnl_proxy_conservative": conservative_pnl,
            "roi_proxy_conservative": roi,
            "bad_pc_ge_100_share": bad_share,
            "resid_rate": resid_rate,
            "bad_pc_target": cfg.bad_pair_cost_target,
            "residual_rate_target": cfg.residual_rate_target,
            "limit_order_min_shares": cfg.min_shadow_qty,
            "market_order_min_usdc": cfg.market_order_min_usdc,
            "residual_timeouts": self.residual_timeouts,
            "residual_discount_events": self.residual_discount_events,
            "up_first_down_residual_risk_events": self.up_first_down_residual_risk_events,
            "coverage_gate_counts": json.dumps(self.coverage_gate_counts, sort_keys=True),
        }


def window_key_for_row(row: dict[str, Any], ts_ms: int) -> str:
    explicit = row.get("window_id") or row.get("window_key") or row.get("profile_window")
    if explicit:
        return str(explicit)
    return dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d")


def coverage_decision(row: dict[str, Any], ts_ms: int, side: str | None, bid_px: float | None) -> CoverageDecision:
    if infer_asset(row) != "BTC":
        return CoverageDecision(False, "excluded_non_btc", "asset_not_btc", "unknown", "unknown")
    if infer_timeframe(row) != "5m":
        return CoverageDecision(False, "excluded_non_5m", "timeframe_not_5m", "unknown", "unknown")
    if side is None or bid_px is None:
        return CoverageDecision(False, "missing_side_or_price", "missing_side_or_price", "unknown", "unknown")
    band = price_band(bid_px)
    remaining_s = infer_remaining_s(row, ts_ms)
    if remaining_s is None:
        return CoverageDecision(False, "missing_remaining", "missing_remaining", band, "unknown")
    if remaining_s <= 60.0 + 1e-9:
        if band == "35_50":
            return CoverageDecision(True, "ce25_last60_35_50_primary", "allowed", band, "last60")
        if band == "50_65":
            return CoverageDecision(True, "ce25_last60_50_65_primary", "allowed", band, "last60")
        if band == "20_35":
            return CoverageDecision(True, "ce25_last60_20_35_overlay", "allowed_overlay", band, "last60")
        return CoverageDecision(False, "last60_outside_ce25_bands", "price_band_not_selected", band, "last60")
    if remaining_s <= 300.0 + 1e-9:
        if band == "35_50":
            return CoverageDecision(False, "ce25_1_5m_35_50_negative", "negative_bucket", band, "1_5m")
        return CoverageDecision(False, "ce25_1_5m_not_selected", "not_selected", band, "1_5m")
    return CoverageDecision(False, "outside_ce25_timing", "outside_selected_timing", band, "other")


class MakerShadowPipeline:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.active: dict[str, ActiveLot] = {}
        self.quarantined_markets: set[str] = set()
        self.support_history: dict[str, list[OppositeSupportTouch]] = {}
        self.market_finalized_residual_cost: dict[str, float] = {}
        self.up_first_down_residual_events = 0
        self.events: list[dict[str, Any]] = []
        self.windows: dict[str, WindowStats] = {}

    def stats(self, window_key: str) -> WindowStats:
        if window_key not in self.windows:
            self.windows[window_key] = WindowStats()
        return self.windows[window_key]

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def run(self, rows: Iterable[dict[str, Any]]) -> None:
        indexed = []
        for idx, row in enumerate(rows):
            ts_ms = infer_ts_ms(row, idx)
            indexed.append((ts_ms, idx, row))
        for ts_ms, idx, row in sorted(indexed, key=lambda item: (item[0], item[1])):
            self.process_row(row, ts_ms)
        for lot in list(self.active.values()):
            self.finalize_residual(lot, reason="end_of_input")
            self.active.pop(lot.slug, None)

    def process_row(self, row: dict[str, Any], ts_ms: int) -> None:
        slug = infer_slug(row) or f"row-{ts_ms}"
        side = infer_row_side(row)
        bid_px = infer_bid_px(row, side) if side else None
        window_key = window_key_for_row(row, ts_ms)
        stats = self.stats(window_key)
        stats.events += 1
        stats.observed_markets.add(slug)
        had_active_lot = slug in self.active
        if slug in self.active:
            self.try_close(row, ts_ms, self.active[slug])
        decision = coverage_decision(row, ts_ms, side, bid_px)
        stats.note_gate(decision.gate_id)
        if not decision.allowed:
            self.emit_base(row, ts_ms, "coverage_rejected", decision, side, bid_px, {})
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        stats.opportunities += 1
        stats.opportunity_markets.add(slug)
        if slug in self.active:
            self.emit_base(row, ts_ms, "open_skipped_active_residual", decision, side, bid_px, {})
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        if had_active_lot:
            self.emit_base(row, ts_ms, "open_skipped_completion_row", decision, side, bid_px, {})
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        if slug in self.quarantined_markets:
            self.emit_base(row, ts_ms, "open_rejected_residual_quarantine", decision, side, bid_px, {})
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        if self.cfg.allowed_coverage_gates and decision.gate_id not in self.cfg.allowed_coverage_gates:
            self.emit_base(
                row,
                ts_ms,
                "open_rejected_coverage_profile_filter",
                decision,
                side,
                bid_px,
                {"allowed_coverage_gates": list(self.cfg.allowed_coverage_gates)},
            )
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        if self.cfg.allowed_open_sides and side not in self.cfg.allowed_open_sides:
            self.emit_base(
                row,
                ts_ms,
                "open_rejected_side_profile_filter",
                decision,
                side,
                bid_px,
                {"allowed_open_sides": list(self.cfg.allowed_open_sides)},
            )
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        first_side_ok, first_side_payload = self.first_side_hazard_ok(row, ts_ms, side, bid_px)
        if not first_side_ok:
            self.emit_base(
                row,
                ts_ms,
                "open_rejected_first_side_hazard",
                decision,
                side,
                bid_px,
                first_side_payload,
            )
            self.record_opposite_support(row, ts_ms, side, bid_px)
            return
        self.try_open(row, ts_ms, decision, side, bid_px)
        self.record_opposite_support(row, ts_ms, side, bid_px)

    def l2_quality_ok(self, row: dict[str, Any]) -> tuple[bool, str]:
        age_ms = infer_age_ms(row)
        if age_ms is not None and age_ms > self.cfg.max_l2_age_ms:
            return False, "l2_age_exceeds_gate"
        align_lag_ms = infer_align_lag_ms(row)
        if align_lag_ms is not None and align_lag_ms > self.cfg.max_align_lag_ms:
            return False, "align_lag_exceeds_gate"
        return True, "ok"

    def first_side_hazard_ok(
        self,
        row: dict[str, Any],
        ts_ms: int,
        side: str | None,
        bid_px: float | None,
    ) -> tuple[bool, dict[str, Any]]:
        if side is None or bid_px is None or bid_px <= 0:
            return True, {}
        band = price_band(bid_px)
        if side == "YES":
            if (
                self.cfg.reject_yes_open_after_up_residual_events > 0
                and self.up_first_down_residual_events >= self.cfg.reject_yes_open_after_up_residual_events
            ):
                return False, {
                    "reason": "yes_first_after_up_residual_hazard",
                    "up_first_down_residual_events_seen": self.up_first_down_residual_events,
                    "reject_yes_open_after_up_residual_events": self.cfg.reject_yes_open_after_up_residual_events,
                }
            remaining_s = infer_remaining_s(row, ts_ms)
            if (
                remaining_s is not None
                and self.cfg.reject_yes_open_when_remaining_gt_s > 0.0
                and remaining_s > self.cfg.reject_yes_open_when_remaining_gt_s + 1e-9
            ):
                return False, {
                    "reason": "yes_first_remaining_bucket_hazard",
                    "remaining_s": remaining_s,
                    "reject_yes_open_when_remaining_gt_s": self.cfg.reject_yes_open_when_remaining_gt_s,
                }
            if band in self.cfg.reject_yes_open_in_price_bands:
                return False, {
                    "reason": "yes_first_price_band_hazard",
                    "price_band": band,
                    "reject_yes_open_in_price_bands": list(self.cfg.reject_yes_open_in_price_bands),
                }
        if side == "NO" and band in self.cfg.reject_no_open_in_price_bands:
            return False, {
                "reason": "no_first_price_band_hazard",
                "price_band": band,
                "reject_no_open_in_price_bands": list(self.cfg.reject_no_open_in_price_bands),
            }
        return True, {}

    def record_opposite_support(
        self,
        row: dict[str, Any],
        ts_ms: int,
        side: str | None,
        bid_px: float | None,
    ) -> None:
        if side is None or bid_px is None or bid_px <= 0:
            return
        quality_ok, _ = self.l2_quality_ok(row)
        if not quality_ok:
            return
        visible_depth = infer_visible_depth_qty(row, side)
        fillable_qty = visible_depth * self.cfg.queue_conversion / max(self.cfg.queue_ahead_multiplier, 1e-9)
        touched, touch_reason = infer_public_sell_touch(row, bid_px)
        if not touched or fillable_qty < self.cfg.opposite_support_min_qty:
            return
        slug = infer_slug(row)
        if not slug:
            return
        history = self.support_history.setdefault(slug, [])
        max_lookback_ms = max(self.cfg.opposite_support_lookback_s, 0.0) * 1000.0
        if max_lookback_ms > 0:
            history[:] = [touch for touch in history if ts_ms - touch.ts_ms <= max_lookback_ms]
        history.append(
            OppositeSupportTouch(
                side=side,
                bid_px=bid_px,
                ts_ms=ts_ms,
                fillable_qty=fillable_qty,
                touch_reason=touch_reason,
            )
        )

    def recent_opposite_support(
        self,
        row: dict[str, Any],
        ts_ms: int,
        side: str,
        bid_px: float,
    ) -> tuple[bool, dict[str, Any]]:
        if not self.cfg.require_opposite_support_before_open:
            return True, {}
        slug = infer_slug(row)
        max_age_s = max(self.cfg.opposite_support_lookback_s, 0.0)
        if not slug or max_age_s <= 0:
            return False, {"reason": "opposite_support_gate_unconfigured_or_missing_slug"}
        support_side = opposite_side(side)
        support_rows = []
        for touch in self.support_history.get(slug, []):
            age_s = max(0.0, (ts_ms - touch.ts_ms) / 1000.0)
            pair_cost = bid_px + touch.bid_px
            if (
                touch.side == support_side
                and age_s <= max_age_s + 1e-9
                and touch.fillable_qty >= self.cfg.opposite_support_min_qty
                and pair_cost <= self.cfg.opposite_support_pair_cost_cap + 1e-12
            ):
                support_rows.append((age_s, pair_cost, touch))
        if not support_rows:
            return False, {
                "reason": "no_recent_opposite_support",
                "opposite_support_side": support_side,
                "opposite_support_lookback_s": max_age_s,
                "opposite_support_min_qty": self.cfg.opposite_support_min_qty,
                "opposite_support_pair_cost_cap": self.cfg.opposite_support_pair_cost_cap,
            }
        age_s, pair_cost, touch = min(support_rows, key=lambda item: (item[0], item[1]))
        return True, {
            "opposite_support_side": support_side,
            "opposite_support_age_s": age_s,
            "opposite_support_px": touch.bid_px,
            "opposite_support_pair_cost": pair_cost,
            "opposite_support_fillable_qty": touch.fillable_qty,
            "opposite_support_touch_reason": touch.touch_reason,
        }

    def try_open(
        self,
        row: dict[str, Any],
        ts_ms: int,
        decision: CoverageDecision,
        side: str | None,
        bid_px: float | None,
    ) -> None:
        if side is None or bid_px is None or bid_px <= 0:
            return
        support_ok, support_payload = self.recent_opposite_support(row, ts_ms, side, bid_px)
        if not support_ok:
            self.emit_base(
                row,
                ts_ms,
                "open_rejected_no_recent_opposite_support",
                decision,
                side,
                bid_px,
                support_payload,
            )
            return
        if self.cfg.suppress_yes_first_open and side == "YES":
            self.emit_base(
                row,
                ts_ms,
                "open_rejected_yes_first_suppressor",
                decision,
                side,
                bid_px,
                {"reason": "suppress_yes_first_open"},
            )
            return
        quality_ok, quality_reason = self.l2_quality_ok(row)
        if not quality_ok:
            self.emit_base(row, ts_ms, "open_rejected_l2_quality", decision, side, bid_px, {"reason": quality_reason})
            return
        visible_depth = infer_visible_depth_qty(row, side)
        public_qty = infer_trade_qty(row)
        requested_qty = min(
            self.cfg.max_shadow_qty,
            max(public_qty, visible_depth * self.cfg.queue_conversion),
        )
        fillable_qty = visible_depth * self.cfg.queue_conversion / max(self.cfg.queue_ahead_multiplier, 1e-9)
        touched, touch_reason = infer_public_sell_touch(row, bid_px)
        if not touched:
            self.emit_base(
                row,
                ts_ms,
                "queue_proxy_no_touch",
                decision,
                side,
                bid_px,
                {
                    "touch_reason": touch_reason,
                    "visible_depth_qty": visible_depth,
                    "requested_qty": requested_qty,
                    "fillable_qty_proxy": fillable_qty,
                },
            )
            return
        if fillable_qty < self.cfg.min_shadow_qty or requested_qty < self.cfg.min_shadow_qty:
            self.emit_base(
                row,
                ts_ms,
                "queue_proxy_touch_insufficient_depth",
                decision,
                side,
                bid_px,
                {
                    "touch_reason": touch_reason,
                    "visible_depth_qty": visible_depth,
                    "requested_qty": requested_qty,
                    "fillable_qty_proxy": fillable_qty,
                },
            )
            self.stats(window_key_for_row(row, ts_ms)).touch_only += 1
            return
        qty = min(requested_qty, fillable_qty)
        if self.cfg.max_open_cost_usdc > 0.0:
            cost_capped_qty = self.cfg.max_open_cost_usdc / bid_px
            qty = min(qty, cost_capped_qty)
            if qty < self.cfg.min_shadow_qty:
                self.emit_base(
                    row,
                    ts_ms,
                    "open_rejected_max_open_cost_below_minimum",
                    decision,
                    side,
                    bid_px,
                    {
                        "touch_reason": touch_reason,
                        "visible_depth_qty": visible_depth,
                        "requested_qty": requested_qty,
                        "fillable_qty_proxy": fillable_qty,
                        "max_open_cost_usdc": self.cfg.max_open_cost_usdc,
                        "limit_order_min_shares": self.cfg.min_shadow_qty,
                    },
                )
                return
        projected_open_cost = qty * bid_px
        if self.cfg.per_market_residual_budget_usdc > 0.0:
            prior_residual_cost = self.market_finalized_residual_cost.get(infer_slug(row), 0.0)
            if prior_residual_cost + projected_open_cost > self.cfg.per_market_residual_budget_usdc + 1e-12:
                self.emit_base(
                    row,
                    ts_ms,
                    "open_rejected_per_market_residual_budget",
                    decision,
                    side,
                    bid_px,
                    {
                        "touch_reason": touch_reason,
                        "visible_depth_qty": visible_depth,
                        "requested_qty": requested_qty,
                        "fillable_qty_proxy": fillable_qty,
                        "projected_open_cost": projected_open_cost,
                        "prior_market_residual_cost": prior_residual_cost,
                        "per_market_residual_budget_usdc": self.cfg.per_market_residual_budget_usdc,
                    },
                )
                return
        condition_id = str(row.get("condition_id") or row.get("conditionId") or "")
        window_key = window_key_for_row(row, ts_ms)
        self.active[infer_slug(row)] = ActiveLot(
            slug=infer_slug(row),
            condition_id=condition_id,
            side=side,
            qty=qty,
            first_px=bid_px,
            opened_ts_ms=ts_ms,
            window_key=window_key,
            gate_id=decision.gate_id,
        )
        stats = self.stats(window_key)
        stats.queue_proxy_opens += 1
        stats.queue_proxy_open_markets.add(infer_slug(row))
        stats.buy_actual_proxy += qty * bid_px
        self.emit_base(
            row,
            ts_ms,
            "queue_proxy_open",
            decision,
            side,
            bid_px,
            {
                "touch_reason": touch_reason,
                "visible_depth_qty": visible_depth,
                "requested_qty": requested_qty,
                "fillable_qty_proxy": fillable_qty,
                "shadow_qty": qty,
                "fee_rate": 0.0,
                "maker_truth": "public_queue_proxy_only",
                **support_payload,
            },
        )

    def try_close(self, row: dict[str, Any], ts_ms: int, lot: ActiveLot) -> None:
        side = infer_row_side(row)
        if side != opposite_side(lot.side):
            self.note_residual_age(row, ts_ms, lot)
            return
        bid_px = infer_bid_px(row, side)
        if bid_px is None or bid_px <= 0:
            self.note_residual_age(row, ts_ms, lot)
            return
        pair_cost = lot.first_px + bid_px
        if pair_cost > self.cfg.pair_cost_cap + 1e-12:
            self.emit_base(
                row,
                ts_ms,
                "completion_rejected_pair_cost",
                coverage_decision(row, ts_ms, side, bid_px),
                side,
                bid_px,
                {
                    "first_side": lot.side,
                    "first_px": lot.first_px,
                    "pair_cost": pair_cost,
                    "pair_cost_cap": self.cfg.pair_cost_cap,
                    "age_s": lot.age_s(ts_ms),
                },
            )
            self.note_residual_age(row, ts_ms, lot)
            return
        visible_depth = infer_visible_depth_qty(row, side)
        fillable_qty = visible_depth * self.cfg.queue_conversion / max(self.cfg.queue_ahead_multiplier, 1e-9)
        touched, touch_reason = infer_public_sell_touch(row, bid_px)
        if not touched or fillable_qty < self.cfg.min_shadow_qty:
            self.emit_base(
                row,
                ts_ms,
                "completion_queue_proxy_not_supported",
                coverage_decision(row, ts_ms, side, bid_px),
                side,
                bid_px,
                {
                    "first_side": lot.side,
                    "first_px": lot.first_px,
                    "pair_cost": pair_cost,
                    "visible_depth_qty": visible_depth,
                    "fillable_qty_proxy": fillable_qty,
                    "touch_reason": touch_reason,
                    "age_s": lot.age_s(ts_ms),
                },
            )
            self.note_residual_age(row, ts_ms, lot)
            return
        qty = min(lot.qty, fillable_qty)
        window_stats = self.stats(lot.window_key)
        window_stats.queue_proxy_closes += 1
        window_stats.queue_proxy_close_markets.add(lot.slug)
        window_stats.buy_actual_proxy += qty * bid_px
        window_stats.paired_qty += qty
        window_stats.paired_buy_cost += qty * pair_cost
        window_stats.pair_pnl_proxy += qty * (1.0 - pair_cost)
        if pair_cost >= 1.0:
            window_stats.bad_pair_cost_buy_proxy += qty * pair_cost
        lot.qty -= qty
        self.emit_base(
            row,
            ts_ms,
            "queue_proxy_close",
            coverage_decision(row, ts_ms, side, bid_px),
            side,
            bid_px,
            {
                "first_side": lot.side,
                "first_px": lot.first_px,
                "pair_cost": pair_cost,
                "shadow_qty": qty,
                "visible_depth_qty": visible_depth,
                "fillable_qty_proxy": fillable_qty,
                "touch_reason": touch_reason,
                "age_s": lot.age_s(ts_ms),
                "fee_rate": 0.0,
            },
        )
        if lot.qty < self.cfg.min_shadow_qty:
            self.active.pop(lot.slug, None)

    def note_residual_age(self, row: dict[str, Any], ts_ms: int, lot: ActiveLot) -> None:
        age_s = lot.age_s(ts_ms)
        if lot.side == "YES" and age_s >= self.cfg.residual_discount_s:
            self.stats(lot.window_key).up_first_down_residual_risk_events += 1
            self.up_first_down_residual_events += 1
        if age_s >= self.cfg.hard_timeout_s:
            self.finalize_residual(lot, reason="hard_timeout")
            self.active.pop(lot.slug, None)
        elif age_s >= self.cfg.residual_discount_s:
            self.stats(lot.window_key).residual_discount_events += 1
            if self.cfg.quarantine_after_residual_discount:
                self.quarantined_markets.add(lot.slug)
            self.emit(
                {
                    "event": "residual_discount",
                    "slug": lot.slug,
                    "condition_id": lot.condition_id,
                    "ts_ms": ts_ms,
                    "window_key": lot.window_key,
                    "first_side": lot.side,
                    "residual_qty": lot.qty,
                    "residual_cost": lot.qty * lot.first_px,
                    "age_s": age_s,
                    "risk_flag": "up_first_down_residual" if lot.side == "YES" else "",
                    "maker_truth": "public_queue_proxy_only",
                }
            )

    def finalize_residual(self, lot: ActiveLot, *, reason: str) -> None:
        stats = self.stats(lot.window_key)
        stats.residual_qty += lot.qty
        residual_cost = lot.qty * lot.first_px
        stats.residual_cost += residual_cost
        self.market_finalized_residual_cost[lot.slug] = (
            self.market_finalized_residual_cost.get(lot.slug, 0.0) + residual_cost
        )
        if reason == "hard_timeout":
            stats.residual_timeouts += 1
        self.emit(
            {
                "event": "residual_finalized",
                "slug": lot.slug,
                "condition_id": lot.condition_id,
                "ts_ms": None,
                "window_key": lot.window_key,
                "first_side": lot.side,
                "residual_qty": lot.qty,
                "residual_cost": residual_cost,
                "reason": reason,
                "risk_flag": "up_first_down_residual" if lot.side == "YES" else "",
                "maker_truth": "public_queue_proxy_only",
            }
        )

    def emit_base(
        self,
        row: dict[str, Any],
        ts_ms: int,
        event: str,
        decision: CoverageDecision,
        side: str | None,
        bid_px: float | None,
        payload: dict[str, Any],
    ) -> None:
        self.emit(
            {
                "event": event,
                "slug": infer_slug(row),
                "condition_id": row.get("condition_id") or row.get("conditionId") or "",
                "ts_ms": ts_ms,
                "window_key": window_key_for_row(row, ts_ms),
                "side": side or "",
                "maker_bid_px": bid_px,
                "coverage_gate_id": decision.gate_id,
                "coverage_reason": decision.reason,
                "price_band": decision.band,
                "remaining_bucket": decision.remaining_bucket,
                "l2_age_ms": infer_age_ms(row),
                "align_lag_ms": infer_align_lag_ms(row),
                "maker_truth": "public_queue_proxy_only",
                "own_telemetry": False,
                **payload,
            }
        )

    def summary_rows(self) -> list[dict[str, Any]]:
        rows = [
            stats.to_row(window_key, cfg=self.cfg)
            for window_key, stats in sorted(self.windows.items())
        ]
        if len(rows) > 1:
            aggregate = WindowStats()
            for stats in self.windows.values():
                aggregate.observed_markets.update(stats.observed_markets)
                aggregate.opportunity_markets.update(stats.opportunity_markets)
                aggregate.queue_proxy_open_markets.update(stats.queue_proxy_open_markets)
                aggregate.queue_proxy_close_markets.update(stats.queue_proxy_close_markets)
                for key, value in stats.coverage_gate_counts.items():
                    aggregate.coverage_gate_counts[key] = aggregate.coverage_gate_counts.get(key, 0) + value
                for field_name in (
                    "events",
                    "opportunities",
                    "queue_proxy_opens",
                    "queue_proxy_closes",
                    "touch_only",
                    "residual_timeouts",
                    "residual_discount_events",
                    "buy_actual_proxy",
                    "paired_qty",
                    "paired_buy_cost",
                    "pair_pnl_proxy",
                    "residual_qty",
                    "residual_cost",
                    "bad_pair_cost_buy_proxy",
                    "up_first_down_residual_risk_events",
                ):
                    setattr(aggregate, field_name, getattr(aggregate, field_name) + getattr(stats, field_name))
            rows.append(aggregate.to_row("ALL", cfg=self.cfg))
        return rows

    def decision_register(self) -> dict[str, Any]:
        rows = self.summary_rows()
        overall = next((row for row in rows if row["window_key"] == "ALL"), rows[0] if rows else {})
        worst_roi = min(
            (row["roi_proxy_conservative"] for row in rows if row.get("roi_proxy_conservative") is not None),
            default=None,
        )
        stable_windows = [
            row
            for row in rows
            if row["window_key"] != "ALL"
            and (row.get("roi_proxy_conservative") or 0.0) > 0.0
            and (row.get("bad_pc_ge_100_share") is None or row["bad_pc_ge_100_share"] <= self.cfg.bad_pair_cost_target)
            and (row.get("resid_rate") is None or row["resid_rate"] <= self.cfg.residual_rate_target)
        ]
        blockers = [
            "own_maker_telemetry_missing",
            "maker_fill_truth_not_proven",
            "private_queue_priority_not_observed",
        ]
        if overall.get("observed_markets", 0) < self.cfg.min_markets_for_review:
            blockers.append("market_sample_below_review_floor")
        if overall.get("bad_pc_ge_100_share") is not None and overall["bad_pc_ge_100_share"] > self.cfg.bad_pair_cost_target:
            blockers.append("bad_pair_cost_share_above_target")
        if overall.get("resid_rate") is not None and overall["resid_rate"] > self.cfg.residual_rate_target:
            blockers.append("residual_rate_above_target")
        return {
            "generated_at": utc_now(),
            "status": RESEARCH_STATUS,
            "strategy_label": "NAGI-style no-order maker queue shadow + CE25 coverage gate + B27BC residual closer",
            "evidence_level": "research_proxy",
            "non_claims": {
                "ready": False,
                "private_truth": False,
                "maker_fill_truth": False,
                "order_execution": False,
                "canary": False,
                "live": False,
            },
            "component_assessment": {
                "nagi_maker_queue_shadow": "implemented_as_public_sell_touch_visible_depth_proxy",
                "ce25_coverage_gates": "implemented_as_btc5m_last60_20_35_35_50_50_65_gate_with_1_5m_35_50_negative",
                "b27bc_residual_closer": "implemented_as_pair_cost_cap_discount_timeout_residual_accounting",
                "xuan_fee_guard": "fee_rate_for_scoring_fixed_to_zero_and_taker_truth_required_for_any_higher_status",
                "order_minimum_guard": "post_only_limit_shadow_requires_at_least_5_shares; market_order_min_usdc_tracked_as_1_but_not_used_by_this_maker_only_pipeline",
            },
            "overall": overall,
            "rolling_window_count": len([row for row in rows if row["window_key"] != "ALL"]),
            "stable_window_count": len(stable_windows),
            "worst_window_roi_proxy_conservative": worst_roi,
            "blockers": blockers,
            "next_gate": "collect_own_authenticated_maker_shadow_telemetry" if not blockers else "review_proxy_metrics_and_blockers",
        }


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-csv", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--queue-conversion", type=float, default=PipelineConfig.queue_conversion)
    p.add_argument("--queue-ahead-multiplier", type=float, default=PipelineConfig.queue_ahead_multiplier)
    p.add_argument("--max-shadow-qty", type=float, default=PipelineConfig.max_shadow_qty)
    p.add_argument("--min-shadow-qty", type=float, default=PipelineConfig.min_shadow_qty)
    p.add_argument("--market-order-min-usdc", type=float, default=PipelineConfig.market_order_min_usdc)
    p.add_argument("--max-l2-age-ms", type=float, default=PipelineConfig.max_l2_age_ms)
    p.add_argument("--max-align-lag-ms", type=float, default=PipelineConfig.max_align_lag_ms)
    p.add_argument("--pair-cost-cap", type=float, default=PipelineConfig.pair_cost_cap)
    p.add_argument("--residual-discount-s", type=float, default=PipelineConfig.residual_discount_s)
    p.add_argument("--hard-timeout-s", type=float, default=PipelineConfig.hard_timeout_s)
    p.add_argument("--suppress-yes-first-open", action="store_true")
    p.add_argument("--quarantine-after-residual-discount", action="store_true")
    p.add_argument("--allowed-coverage-gate", action="append", default=[])
    p.add_argument("--allowed-open-side", action="append", default=[])
    p.add_argument("--require-opposite-support-before-open", action="store_true")
    p.add_argument("--opposite-support-lookback-s", type=float, default=PipelineConfig.opposite_support_lookback_s)
    p.add_argument("--opposite-support-min-qty", type=float, default=PipelineConfig.opposite_support_min_qty)
    p.add_argument("--opposite-support-pair-cost-cap", type=float, default=PipelineConfig.opposite_support_pair_cost_cap)
    p.add_argument("--per-market-residual-budget-usdc", type=float, default=PipelineConfig.per_market_residual_budget_usdc)
    p.add_argument("--max-open-cost-usdc", type=float, default=PipelineConfig.max_open_cost_usdc)
    p.add_argument(
        "--reject-yes-open-after-up-residual-events",
        type=int,
        default=PipelineConfig.reject_yes_open_after_up_residual_events,
    )
    p.add_argument(
        "--reject-yes-open-when-remaining-gt-s",
        type=float,
        default=PipelineConfig.reject_yes_open_when_remaining_gt_s,
    )
    p.add_argument("--reject-yes-open-in-price-band", action="append", default=[])
    p.add_argument("--reject-no-open-in-price-band", action="append", default=[])
    p.add_argument("--bad-pair-cost-target", type=float, default=PipelineConfig.bad_pair_cost_target)
    p.add_argument("--residual-rate-target", type=float, default=PipelineConfig.residual_rate_target)
    p.add_argument("--min-markets-for-review", type=int, default=PipelineConfig.min_markets_for_review)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = PipelineConfig(
        queue_conversion=args.queue_conversion,
        queue_ahead_multiplier=args.queue_ahead_multiplier,
        max_shadow_qty=args.max_shadow_qty,
        min_shadow_qty=args.min_shadow_qty,
        market_order_min_usdc=args.market_order_min_usdc,
        max_l2_age_ms=args.max_l2_age_ms,
        max_align_lag_ms=args.max_align_lag_ms,
        pair_cost_cap=args.pair_cost_cap,
        residual_discount_s=args.residual_discount_s,
        hard_timeout_s=args.hard_timeout_s,
        suppress_yes_first_open=args.suppress_yes_first_open,
        quarantine_after_residual_discount=args.quarantine_after_residual_discount,
        allowed_coverage_gates=tuple(args.allowed_coverage_gate),
        allowed_open_sides=tuple(args.allowed_open_side),
        require_opposite_support_before_open=args.require_opposite_support_before_open,
        opposite_support_lookback_s=args.opposite_support_lookback_s,
        opposite_support_min_qty=args.opposite_support_min_qty,
        opposite_support_pair_cost_cap=args.opposite_support_pair_cost_cap,
        per_market_residual_budget_usdc=args.per_market_residual_budget_usdc,
        max_open_cost_usdc=args.max_open_cost_usdc,
        reject_yes_open_after_up_residual_events=args.reject_yes_open_after_up_residual_events,
        reject_yes_open_when_remaining_gt_s=args.reject_yes_open_when_remaining_gt_s,
        reject_yes_open_in_price_bands=tuple(args.reject_yes_open_in_price_band),
        reject_no_open_in_price_bands=tuple(args.reject_no_open_in_price_band),
        bad_pair_cost_target=args.bad_pair_cost_target,
        residual_rate_target=args.residual_rate_target,
        min_markets_for_review=args.min_markets_for_review,
    )
    rows = load_csv(Path(args.input_csv))
    pipeline = MakerShadowPipeline(cfg)
    pipeline.run(rows)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    events_path = out / "nagi_ce25_b27bc_maker_shadow_events.jsonl"
    summary_path = out / "rolling_24h_summary.csv"
    decision_path = out / "decision_register.json"
    write_jsonl(events_path, pipeline.events)
    write_csv(summary_path, pipeline.summary_rows())
    decision_path.write_text(
        json.dumps(
            {
                "input_csv": str(Path(args.input_csv).resolve()),
                "config": cfg.__dict__,
                **pipeline.decision_register(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": RESEARCH_STATUS,
                "events": str(events_path),
                "summary": str(summary_path),
                "decision_register": str(decision_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
