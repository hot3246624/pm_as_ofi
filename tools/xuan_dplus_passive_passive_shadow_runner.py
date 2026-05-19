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
    pair_pnl: float = 0.0
    taker_fee: float = 0.0
    completion_cost: float = 0.0
    salvage_qty: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
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

    def quote_intent_id(self, order_id: int) -> str:
        return f"{self.slug}:quote:{order_id}"

    def blocked_quote_intent_id(self, side: str, ts_ms: int) -> str:
        return f"{self.slug}:blocked:{side}:{ts_ms}:{self.blocked.get('activation_opp_seen', 0)}"

    def offset_s(self, ts_ms: int) -> float | None:
        return ts_ms / 1000.0 - self.start_s if self.start_s else None

    def emit(self, obj: dict[str, Any]) -> None:
        with self.events_path.open("a") as f:
            f.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")

    def block(self, reason: str) -> None:
        self.blocked[reason] = self.blocked.get(reason, 0) + 1

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
        lot = Lot(id=self.next_lot_id, quote_intent_id=order.quote_intent_id, side=order.side, qty=order.qty, px=order.px, fill_ms=ts_ms, source_order_id=order.id)
        self.next_lot_id += 1
        self.lots[order.side].append(lot)
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
            self.metrics.pair_pnl += take * (1.0 - pair_cost)
            self.metrics.pair_costs.append(pair_cost)
            self.metrics.net_pair_costs.append(pair_cost)
            self.metrics.pair_wait_ms.append(ts_ms - older)
            a.qty -= take
            b.qty -= take
            matched_pair_id = f"{self.slug}:pair:{self.metrics.pair_actions}"
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
                self.metrics.pair_pnl += take * (1.0 - net_pair)
                self.metrics.pair_costs.append(gross_pair)
                self.metrics.net_pair_costs.append(net_pair)
                self.metrics.salvage_wait_ms.append(age)
                self.metrics.pair_wait_ms.append(age)
                matched_pair_id = f"{self.slug}:salvage:{self.metrics.salvage_actions}"
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
            self.block("offset")
            return
        if not (self.cfg.seed_px_lo <= px <= self.cfg.seed_px_hi):
            self.block("price")
            return
        yes_ask = side_ask(self.book, "YES")
        no_ask = side_ask(self.book, "NO")
        if yes_ask <= 0 or no_ask <= 0:
            self.block("missing_pair_ask")
            return
        l1_pair = yes_ask + no_ask
        if l1_pair > self.cfg.seed_l1_cap + 1e-12:
            self.block("l1_pair_ask_gt_cap")
            return
        if ts_ms - self.last_seed_ms < self.cfg.cooldown_ms:
            self.block("cooldown")
            return

        same_qty = self.exposure_qty(side)
        opp_qty = self.exposure_qty(opp(side))
        target_qty = self.cfg.target_for(offset)
        seed_px = max(0.01, px - self.cfg.edge)
        opposite_seen_ms = self.activation_last_seen_ms.get(opp(side))
        if self.cfg.pairing_only_when_residual and same_qty > opp_qty + self.cfg.dust_qty:
            self.block("pairing_only_when_residual")
            self.record_activation_seen(side, ts_ms)
            return
        if self.cfg.late_repair_active(offset) and same_qty + self.cfg.dust_qty >= opp_qty:
            self.block("late_repair_only")
            self.record_activation_seen(side, ts_ms)
            return
        risk_increasing_seed = same_qty + self.cfg.dust_qty >= opp_qty
        activation_ok, activation_opp_age_ms = self.activation_allows_seed(side, ts_ms)
        if risk_increasing_seed and not activation_ok:
            self.block("activation_opp_seen")
            blocked_quote_intent_id = self.blocked_quote_intent_id(side, ts_ms)
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
            self.block("target")
            self.record_activation_seen(side, ts_ms)
            return
        if max(0.0, self.exposure_cost(side) - self.exposure_cost(opp(side))) > self.cfg.imbalance_cost_cap + 1e-12:
            self.block("imbalance_cost")
            self.record_activation_seen(side, ts_ms)
            return
        imbalance_room = self.cfg.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        if imbalance_room <= self.cfg.dust_qty:
            self.block("imbalance_qty")
            self.record_activation_seen(side, ts_ms)
            return

        room_cost = self.cfg.max_open_cost - self.total_open_cost()
        qty = min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut, target_qty - same_qty, room_cost / max(seed_px, 1e-9), imbalance_room)
        if qty <= self.cfg.dust_qty:
            self.block("qty_zero")
            self.record_activation_seen(side, ts_ms)
            return

        quote_intent_id = self.quote_intent_id(self.next_order_id)
        opposite_trigger_ts_ms = (
            ts_ms - activation_opp_age_ms if activation_opp_age_ms is not None else opposite_seen_ms
        )
        order = VirtualOrder(id=self.next_order_id, quote_intent_id=quote_intent_id, condition_id=self.condition_id, side=side, px=seed_px, qty=qty, created_ms=ts_ms, accepted_ms=ts_ms, offset_s=float(offset), trigger_px=px, trigger_size=size, trigger_ts_ms=ts_ms, trigger_source_sequence_id=trigger_source_sequence_id, opposite_trigger_ts_ms=opposite_trigger_ts_ms)
        self.next_order_id += 1
        self.pending.append(order)
        self.last_seed_ms = ts_ms
        self.metrics.candidates += 1
        self.metrics.seed_qty += qty
        self.metrics.seed_cost += qty * seed_px
        self.emit({"kind": "candidate", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "placed_ts_ms": ts_ms, "accepted_ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": side, "price": seed_px, "size": qty, "offset_s": offset, "public_trade_px": px, "public_trade_size": size, "trigger_ts_ms": ts_ms, "trigger_event_time_ms": trigger_event_time_ms, "source": "no_order_public_trade_candidate", "source_sequence_id": trigger_source_sequence_id, "market_md_source_sequence_id": trigger_source_sequence_id, "opposite_trigger_ts_ms": opposite_trigger_ts_ms, "seed_px": seed_px, "qty": qty, "edge": self.cfg.edge, "queue_share": self.cfg.queue_share, "l1_pair_ask": l1_pair, "same_exposure_qty": same_qty, "opp_exposure_qty": opp_qty, "target_qty": target_qty, "base_target_qty": self.cfg.target_qty, "late_target_active": self.cfg.late_target_active(offset), "late_repair_active": self.cfg.late_repair_active(offset), "activation_mode": self.cfg.activation_mode, "activation_window_s": self.cfg.activation_window_s, "activation_required": risk_increasing_seed and self.cfg.activation_mode != "none", "risk_increasing_seed": risk_increasing_seed, "activation_opp_age_ms": activation_opp_age_ms, "open_cost": self.total_open_cost(), "yes_bid": self.book.get("yes_bid"), "yes_ask": yes_ask, "no_bid": self.book.get("no_bid"), "no_ask": no_ask})
        self.record_activation_seen(side, ts_ms)

    def write_summary(self, final: bool = False) -> None:
        if final:
            ts_ms = now_ms()
            for order in self.pending_orders():
                order.cancel_ms = ts_ms
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
            "ts_ms": now_ms(),
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
                {"lot_id": lot.id, "quote_intent_id": lot.quote_intent_id, "side": lot.side, "qty": round(lot.qty, 6), "cost": round(lot.cost, 6), "px": lot.px, "age_ms": now_ms() - lot.fill_ms, "source_order_id": lot.source_order_id}
                for lot in sorted(residual_lots, key=lambda x: x.cost, reverse=True)[:10]
            ],
        }
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
    ap.add_argument("--profile-late-repair-after-s", default="", help="CSV repair_after_s values for same-window multi-profile causal verification")
    ap.add_argument("--order-ttl-s", type=float, default=120.0)
    ap.add_argument("--imbalance-qty-cap", type=float, default=2.0)
    ap.add_argument("--imbalance-cost-cap", type=float, default=1_000_000_000.0)
    ap.add_argument("--pairing-only-when-residual", action="store_true")
    ap.add_argument("--activation-mode", choices=["none", "opp_seen"], default="none")
    ap.add_argument("--activation-window-s", type=float, default=60.0)
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
        order_ttl_ms=int(args.order_ttl_s * 1000),
        imbalance_qty_cap=args.imbalance_qty_cap,
        imbalance_cost_cap=args.imbalance_cost_cap,
        pairing_only_when_residual=args.pairing_only_when_residual,
        activation_mode=args.activation_mode,
        activation_window_s=args.activation_window_s,
        salvage_net_cap=args.salvage_net_cap,
        salvage_age_ms=int(args.salvage_age_s * 1000),
        salvage_min_lot_cost=args.salvage_min_lot_cost,
    )
    if cfg.imbalance_qty_cap <= cfg.dust_qty:
        raise SystemExit(f"--imbalance-qty-cap must exceed dust_qty={cfg.dust_qty}; otherwise every seed is blocked")
    if cfg.late_target_qty is not None and cfg.late_target_qty <= cfg.dust_qty:
        raise SystemExit(f"--late-target-qty must exceed dust_qty={cfg.dust_qty}; otherwise the late window cannot seed")
    if cfg.activation_window_s <= 0:
        raise SystemExit("--activation-window-s must be positive")
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
