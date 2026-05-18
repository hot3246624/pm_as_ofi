#!/usr/bin/env python3
"""Read-only M0001 maker-like shadow runner.

This is a safety-first bridge between the offline/P2 M0001 ledger and a live
shared-ingress dry-run. It never sends orders. It subscribes to market ticks,
creates virtual maker seed orders, accounts queue-supported fills separately
from mere touches, then runs a lot-level completion ledger with conditional
blocked-lot skip and material residual lockout.
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
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any


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


def high_side(book: dict[str, float]) -> str | None:
    yb = book.get("yes_bid", 0.0)
    nb = book.get("no_bid", 0.0)
    if yb <= 0.0 and nb <= 0.0:
        return None
    return "YES" if yb >= nb else "NO"


@dataclass(frozen=True)
class RunnerConfig:
    edge: float = 0.022
    queue_share: float = 0.50
    target_qty: float = 115.0
    late_target_mult: float = 0.5
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 250.0
    cycle_cap: int = 6
    seed_l1_cap: float = 1.02
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 240.0
    base_cap: float = 0.990
    late_cap: float = 0.995
    final_cap: float = 1.010
    cooldown_ms: int = 5000
    order_ttl_ms: int = 120_000
    skip_block_age_ms: int = 120_000
    skip_pair_margin: float = 0.001
    dust_qty: float = 1.0
    material_qty: float = 6.0
    material_cost: float = 6.0
    inventory_gate_qty: float = 6.0
    inventory_gate_cost: float = 6.0
    inventory_reduced_target: float = 30.0
    block_same_side_exposure: bool = False

    def cap_for(self, offset_s: float | None) -> float:
        if offset_s is None:
            return self.base_cap
        if offset_s >= 270:
            return self.final_cap
        if offset_s >= 240:
            return self.late_cap
        return self.base_cap

    def target_for(self, offset_s: float | None) -> float:
        if offset_s is None or offset_s >= 240:
            return 0.0
        if offset_s >= 180:
            return self.target_qty * self.late_target_mult
        return self.target_qty


@dataclass
class VirtualOrder:
    id: int
    side: str
    px: float
    qty: float
    created_ms: int
    offset_s: float
    trigger_px: float
    trigger_size: float
    queue_credit: float = 0.0
    first_bid_touch_ms: int | None = None
    first_trade_touch_ms: int | None = None
    fill_ms: int | None = None
    cancel_ms: int | None = None
    cancel_reason: str | None = None


@dataclass
class Lot:
    id: int
    side: str
    qty: float
    px: float
    cost: float
    fill_ms: int
    source_order_id: int
    paired_qty: float = 0.0
    completed_ms: int | None = None

    @property
    def open_qty(self) -> float:
        return max(0.0, self.qty - self.paired_qty)

    @property
    def open_cost(self) -> float:
        return self.open_qty * self.px


@dataclass
class Metrics:
    candidates: int = 0
    cancelled_orders: int = 0
    real_fills: int = 0
    queue_supported_fills: int = 0
    touch_only_orders: int = 0
    completions: int = 0
    skip_completions: int = 0
    material_lockout_blocks: int = 0
    seed_qty: float = 0.0
    seed_cost: float = 0.0
    filled_qty: float = 0.0
    filled_cost: float = 0.0
    paired_qty: float = 0.0
    pnl_pair: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    material_residual_events: int = 0
    pair_costs: list[float] = field(default_factory=list)
    fill_wait_ms: list[int] = field(default_factory=list)
    completion_wait_ms: list[int] = field(default_factory=list)


class M0001Runner:
    def __init__(self, slug: str, out_dir: Path, cfg: RunnerConfig) -> None:
        self.slug = slug
        self.start_s = round_start_from_slug(slug)
        self.out_dir = out_dir
        self.cfg = cfg
        self.book: dict[str, float] = {}
        self.pending: list[VirtualOrder] = []
        self.lots: list[Lot] = []
        self.metrics = Metrics()
        self.book_ticks = 0
        self.trade_ticks = 0
        self.sell_high_triggers = 0
        self.blocked: dict[str, int] = {}
        self.next_order_id = 1
        self.next_lot_id = 1
        self.last_seed_ms = -(10**18)
        self.cycles = 0
        self.events_path = out_dir / f"{slug}.events.jsonl"
        self.summary_path = out_dir / f"{slug}.summary.json"

    def offset_s(self, ts_ms: int) -> float | None:
        if not self.start_s:
            return None
        return ts_ms / 1000.0 - self.start_s

    def block(self, reason: str) -> None:
        self.blocked[reason] = self.blocked.get(reason, 0) + 1

    def emit(self, obj: dict[str, Any]) -> None:
        with self.events_path.open("a") as f:
            f.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")

    def open_lots(self, side: str | None = None) -> list[Lot]:
        lots = [lot for lot in self.lots if lot.open_qty > 1e-9]
        if side:
            lots = [lot for lot in lots if lot.side == side]
        return lots

    def pending_orders(self, side: str | None = None) -> list[VirtualOrder]:
        orders = [order for order in self.pending if not order.fill_ms and not order.cancel_ms]
        if side:
            orders = [order for order in orders if order.side == side]
        return orders

    def total_open_cost(self) -> float:
        return sum(lot.open_cost for lot in self.open_lots())

    def has_material_residual(self) -> bool:
        qty = sum(lot.open_qty for lot in self.open_lots())
        cost = sum(lot.open_cost for lot in self.open_lots())
        return qty > self.cfg.material_qty or cost > self.cfg.material_cost

    def mark_touches(self, ts_ms: int, trade_side: str | None = None, trade_px: float | None = None) -> None:
        for order in self.pending:
            if order.fill_ms or order.cancel_ms:
                continue
            bid = side_bid(self.book, order.side)
            if bid >= order.px - 1e-12 and order.first_bid_touch_ms is None:
                order.first_bid_touch_ms = ts_ms
                self.emit({
                    "kind": "touch_only",
                    "touch_type": "bid_touch",
                    "slug": self.slug,
                    "ts_ms": ts_ms,
                    "order_id": order.id,
                    "side": order.side,
                    "order_px": order.px,
                    "bid": bid,
                    "wait_ms": ts_ms - order.created_ms,
                })
            if trade_side == order.side and trade_px is not None and trade_px <= order.px + 1e-12:
                if order.first_trade_touch_ms is None:
                    order.first_trade_touch_ms = ts_ms
                    self.emit({
                        "kind": "touch_only",
                        "touch_type": "trade_through",
                        "slug": self.slug,
                        "ts_ms": ts_ms,
                        "order_id": order.id,
                        "side": order.side,
                        "order_px": order.px,
                        "trade_px": trade_px,
                        "wait_ms": ts_ms - order.created_ms,
                    })

    def cancel_expired(self, ts_ms: int) -> None:
        offset = self.offset_s(ts_ms)
        for order in self.pending:
            if order.fill_ms or order.cancel_ms:
                continue
            expired = ts_ms - order.created_ms >= self.cfg.order_ttl_ms
            too_late = offset is not None and offset >= 240
            if not expired and not too_late:
                continue
            order.cancel_ms = ts_ms
            order.cancel_reason = "offset_240" if too_late else "ttl"
            self.metrics.cancelled_orders += 1
            if order.first_bid_touch_ms or order.first_trade_touch_ms:
                self.metrics.touch_only_orders += 1
            self.emit({
                "kind": "cancel",
                "slug": self.slug,
                "ts_ms": ts_ms,
                "order_id": order.id,
                "reason": order.cancel_reason,
                "side": order.side,
                "px": order.px,
                "qty": order.qty,
                "queue_credit": order.queue_credit,
                "first_bid_touch_wait_ms": None if order.first_bid_touch_ms is None else order.first_bid_touch_ms - order.created_ms,
                "first_trade_touch_wait_ms": None if order.first_trade_touch_ms is None else order.first_trade_touch_ms - order.created_ms,
            })

    def fill_order(self, order: VirtualOrder, ts_ms: int, trade_px: float, trade_size: float) -> None:
        order.fill_ms = ts_ms
        self.metrics.queue_supported_fills += 1
        self.metrics.filled_qty += order.qty
        self.metrics.filled_cost += order.qty * order.px
        self.metrics.fill_wait_ms.append(ts_ms - order.created_ms)
        lot = Lot(
            id=self.next_lot_id,
            side=order.side,
            qty=order.qty,
            px=order.px,
            cost=order.qty * order.px,
            fill_ms=ts_ms,
            source_order_id=order.id,
        )
        self.next_lot_id += 1
        self.lots.append(lot)
        self.emit({
            "kind": "queue_supported_fill",
            "slug": self.slug,
            "ts_ms": ts_ms,
            "order_id": order.id,
            "lot_id": lot.id,
            "side": order.side,
            "seed_px": order.px,
            "qty": order.qty,
            "queue_share": self.cfg.queue_share,
            "queue_credit": order.queue_credit,
            "trigger_px": order.trigger_px,
            "trigger_size": order.trigger_size,
            "trade_px": trade_px,
            "trade_size": trade_size,
            "fill_wait_ms": ts_ms - order.created_ms,
            "first_bid_touch_wait_ms": None if order.first_bid_touch_ms is None else order.first_bid_touch_ms - order.created_ms,
            "first_trade_touch_wait_ms": None if order.first_trade_touch_ms is None else order.first_trade_touch_ms - order.created_ms,
        })

    def select_completion_lot(self, side: str, ask: float, cap: float, ts_ms: int) -> tuple[Lot | None, bool, dict[str, Any]]:
        lots = self.open_lots(side)
        if not lots:
            return None, False, {}
        first = lots[0]
        first_pair_cost = first.px + ask
        if first_pair_cost <= cap + 1e-12:
            return first, False, {"fifo_pair_cost": first_pair_cost}
        first_age_ms = ts_ms - first.fill_ms
        if first_age_ms < self.cfg.skip_block_age_ms:
            return None, False, {"blocked_lot_id": first.id, "blocked_pair_cost": first_pair_cost, "blocked_age_ms": first_age_ms}
        for younger in lots[1:]:
            pair_cost = younger.px + ask
            if pair_cost <= cap - self.cfg.skip_pair_margin + 1e-12:
                return younger, True, {
                    "blocked_lot_id": first.id,
                    "blocked_pair_cost": first_pair_cost,
                    "blocked_age_ms": first_age_ms,
                    "selected_lot_id": younger.id,
                    "selected_pair_cost": pair_cost,
                    "skip_margin": cap - pair_cost,
                }
        return None, False, {"blocked_lot_id": first.id, "blocked_pair_cost": first_pair_cost, "blocked_age_ms": first_age_ms}

    def try_completion(self, ts_ms: int) -> None:
        offset = self.offset_s(ts_ms)
        cap = self.cfg.cap_for(offset)
        completed_any = True
        while completed_any:
            completed_any = False
            for side in ("YES", "NO"):
                ask = side_ask(self.book, opp(side))
                if ask <= 0:
                    continue
                lot, skipped, context = self.select_completion_lot(side, ask, cap, ts_ms)
                if lot is None:
                    continue
                qty = lot.open_qty
                if qty <= 1e-9:
                    continue
                pair_cost = lot.px + ask
                lot.paired_qty += qty
                lot.completed_ms = ts_ms
                self.metrics.completions += 1
                self.metrics.paired_qty += qty
                self.metrics.pnl_pair += qty * (1.0 - pair_cost)
                self.metrics.pair_costs.append(pair_cost)
                self.metrics.completion_wait_ms.append(ts_ms - lot.fill_ms)
                if skipped:
                    self.metrics.skip_completions += 1
                self.emit({
                    "kind": "completion",
                    "slug": self.slug,
                    "ts_ms": ts_ms,
                    "lot_id": lot.id,
                    "source_order_id": lot.source_order_id,
                    "side": side,
                    "qty": qty,
                    "seed_px": lot.px,
                    "opp_ask": ask,
                    "pair_cost": pair_cost,
                    "cap": cap,
                    "offset_s": offset,
                    "delay_ms": ts_ms - lot.fill_ms,
                    "conditional_skip": skipped,
                    **context,
                })
                self.cycles += 1
                completed_any = True
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
        self.try_completion(ts_ms)

    def on_trade(self, msg: dict[str, Any]) -> None:
        self.trade_ticks += 1
        ts_ms = int(msg.get("ts_ms") or now_ms())
        side = side_from_str(str(msg.get("market_side") or ""))
        taker = str(msg.get("taker_side") or "").upper()
        px = float(msg.get("price") or 0.0)
        size = float(msg.get("size") or 0.0)
        offset = self.offset_s(ts_ms)
        self.mark_touches(ts_ms, side if taker == "SELL" else None, px if taker == "SELL" else None)
        self.cancel_expired(ts_ms)
        self.try_completion(ts_ms)

        if taker == "SELL":
            for order in self.pending:
                if order.fill_ms or order.cancel_ms or order.side != side:
                    continue
                if px > order.px + 1e-12:
                    continue
                order.queue_credit += max(0.0, size * self.cfg.queue_share)
                if order.queue_credit + 1e-9 >= order.qty:
                    self.fill_order(order, ts_ms, px, size)
            self.try_completion(ts_ms)

        if taker != "SELL":
            return
        hside = high_side(self.book)
        if hside is None or side != hside:
            return
        self.sell_high_triggers += 1
        if offset is None or offset < self.cfg.seed_offset_min_s or offset >= self.cfg.seed_offset_max_s:
            self.block("offset")
            return
        if not (self.cfg.seed_px_lo <= px <= self.cfg.seed_px_hi):
            self.block("price")
            return
        if self.cycles >= self.cfg.cycle_cap:
            self.block("cycle_cap")
            return
        if ts_ms - self.last_seed_ms < self.cfg.cooldown_ms:
            self.block("cooldown")
            return
        if self.has_material_residual():
            self.metrics.material_lockout_blocks += 1
            self.block("material_residual_lockout")
            self.emit({
                "kind": "material_residual_lockout",
                "slug": self.slug,
                "ts_ms": ts_ms,
                "open_qty": sum(l.open_qty for l in self.open_lots()),
                "open_cost": sum(l.open_cost for l in self.open_lots()),
            })
            return
        if self.open_lots(opp(side)):
            self.block("opposite_inventory")
            return
        same_side_lots = self.open_lots(side)
        same_side_qty = sum(lot.open_qty for lot in same_side_lots)
        same_side_cost = sum(lot.open_cost for lot in same_side_lots)
        same_side_pending = self.pending_orders(side)
        same_side_pending_qty = sum(order.qty for order in same_side_pending)
        same_side_pending_cost = sum(order.qty * order.px for order in same_side_pending)
        ask = side_ask(self.book, opp(side))
        if ask <= 0:
            self.block("missing_opp_ask")
            return
        if px + ask > self.cfg.seed_l1_cap + 1e-12:
            self.block("l1_pair_ask_gt_cap")
            return
        if self.cfg.block_same_side_exposure and (same_side_lots or same_side_pending):
            self.block("same_side_exposure_block")
            self.emit({
                "kind": "same_side_exposure_block",
                "slug": self.slug,
                "ts_ms": ts_ms,
                "side": side,
                "open_qty": same_side_qty,
                "open_cost": same_side_cost,
                "pending_qty": same_side_pending_qty,
                "pending_cost": same_side_pending_cost,
                "opp_ask": ask,
                "cap": self.cfg.cap_for(offset),
            })
            return
        if same_side_lots and (same_side_qty > self.cfg.inventory_gate_qty or same_side_cost > self.cfg.inventory_gate_cost):
            self.block("same_side_inventory_gate")
            self.emit({
                "kind": "same_side_inventory_gate",
                "slug": self.slug,
                "ts_ms": ts_ms,
                "side": side,
                "open_qty": same_side_qty,
                "open_cost": same_side_cost,
                "opp_ask": ask,
                "cap": self.cfg.cap_for(offset),
            })
            return
        target = self.cfg.target_for(offset)
        if same_side_lots:
            target = min(target, self.cfg.inventory_reduced_target)
        existing_side_qty = sum(l.open_qty for l in self.open_lots(side))
        if target <= existing_side_qty + 1e-9:
            self.block("target_met")
            return
        seed_px = max(0.01, px - self.cfg.edge)
        remaining_cost = self.cfg.max_open_cost - self.total_open_cost()
        qty = min(
            self.cfg.max_seed_qty,
            size * self.cfg.fill_haircut,
            target - existing_side_qty,
            remaining_cost / max(seed_px, 1e-9),
        )
        if qty <= 1e-9:
            self.block("qty_zero")
            return
        order = VirtualOrder(
            id=self.next_order_id,
            side=side,
            px=seed_px,
            qty=qty,
            created_ms=ts_ms,
            offset_s=float(offset),
            trigger_px=px,
            trigger_size=size,
        )
        self.next_order_id += 1
        self.pending.append(order)
        self.last_seed_ms = ts_ms
        self.metrics.candidates += 1
        self.metrics.seed_qty += qty
        self.metrics.seed_cost += qty * seed_px
        self.emit({
            "kind": "candidate",
            "slug": self.slug,
            "ts_ms": ts_ms,
            "order_id": order.id,
            "side": side,
            "offset_s": offset,
            "public_trade_px": px,
            "public_trade_size": size,
            "seed_px": seed_px,
            "qty": qty,
            "edge": self.cfg.edge,
            "queue_share": self.cfg.queue_share,
            "l1_pair_ask": px + ask,
            "open_qty_before": existing_side_qty,
            "open_cost_before": self.total_open_cost(),
            "yes_bid": self.book.get("yes_bid"),
            "yes_ask": self.book.get("yes_ask"),
            "no_bid": self.book.get("no_bid"),
            "no_ask": self.book.get("no_ask"),
        })

    def write_summary(self, final: bool = False) -> None:
        if final:
            ts_ms = now_ms()
            for order in self.pending:
                if not order.fill_ms and not order.cancel_ms:
                    order.cancel_ms = ts_ms
                    order.cancel_reason = "final"
                    self.metrics.cancelled_orders += 1
                    if order.first_bid_touch_ms or order.first_trade_touch_ms:
                        self.metrics.touch_only_orders += 1
        open_lots = self.open_lots()
        residual_qty = sum(l.open_qty for l in open_lots)
        residual_cost = sum(l.open_cost for l in open_lots)
        material_lots = [l for l in open_lots if l.open_qty > self.cfg.material_qty or l.open_cost > self.cfg.material_cost]
        self.metrics.residual_qty = residual_qty
        self.metrics.residual_cost = residual_cost
        self.metrics.material_residual_events = len(material_lots)
        m = self.metrics
        summary = {
            "kind": "summary",
            "script": "xuan_m0001_maker_shadow_runner.py",
            "slug": self.slug,
            "final": final,
            "ts_ms": now_ms(),
            "book_ticks": self.book_ticks,
            "trade_ticks": self.trade_ticks,
            "sell_high_triggers": self.sell_high_triggers,
            "blocked": self.blocked,
            "config": self.cfg.__dict__,
            "metrics": {
                "candidates": m.candidates,
                "real_fills": m.real_fills,
                "queue_supported_fills": m.queue_supported_fills,
                "touch_only_orders": m.touch_only_orders,
                "cancelled_orders": m.cancelled_orders,
                "completions": m.completions,
                "skip_completions": m.skip_completions,
                "material_lockout_blocks": m.material_lockout_blocks,
                "fill_rate": m.queue_supported_fills / m.candidates if m.candidates else 0.0,
                "completion_per_candidate": m.completions / m.candidates if m.candidates else 0.0,
                "completion_per_fill": m.completions / m.queue_supported_fills if m.queue_supported_fills else 0.0,
                "seed_qty": round(m.seed_qty, 6),
                "seed_cost": round(m.seed_cost, 6),
                "filled_qty": round(m.filled_qty, 6),
                "filled_cost": round(m.filled_cost, 6),
                "paired_qty": round(m.paired_qty, 6),
                "qty_pair_share_of_filled": m.paired_qty / m.filled_qty if m.filled_qty else 0.0,
                "pnl_pair": round(m.pnl_pair, 6),
                "roi_pair_on_seed_cost": m.pnl_pair / m.seed_cost if m.seed_cost else 0.0,
                "roi_pair_on_filled_cost": m.pnl_pair / m.filled_cost if m.filled_cost else 0.0,
                "residual_qty": round(residual_qty, 6),
                "residual_cost": round(residual_cost, 6),
                "material_residual_lots": len(material_lots),
                "pair_cost_p50": pct(m.pair_costs, 0.5),
                "pair_cost_p90": pct(m.pair_costs, 0.9),
                "pair_cost_avg": sum(m.pair_costs) / len(m.pair_costs) if m.pair_costs else None,
                "fill_wait_p50_ms": pct(m.fill_wait_ms, 0.5),
                "fill_wait_p90_ms": pct(m.fill_wait_ms, 0.9),
                "completion_wait_p50_ms": pct(m.completion_wait_ms, 0.5),
                "completion_wait_p90_ms": pct(m.completion_wait_ms, 0.9),
            },
            "top_residual_lots": [
                {
                    "lot_id": lot.id,
                    "side": lot.side,
                    "qty": round(lot.open_qty, 6),
                    "cost": round(lot.open_cost, 6),
                    "px": lot.px,
                    "age_ms": now_ms() - lot.fill_ms,
                    "source_order_id": lot.source_order_id,
                }
                for lot in sorted(open_lots, key=lambda x: x.open_cost, reverse=True)[:10]
            ],
        }
        self.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


async def run_market(market: dict[str, str], socket_path: str, out: Path, duration_s: int, cfg: RunnerConfig) -> None:
    slug = market["POLYMARKET_MARKET_SLUG"]
    runner = M0001Runner(slug, out, cfg)
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
                kind = msg.get("kind")
                if kind == "market_book_tick":
                    runner.on_book(msg)
                elif kind == "market_trade_tick":
                    runner.on_trade(msg)
            if time.monotonic() - last_summary >= 30:
                runner.write_summary(False)
                last_summary = time.monotonic()
    finally:
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
            [
                "/usr/bin/python3",
                str(repo / "scripts/resolve_market_ids.py"),
                "--prefix",
                prefix,
                "--round-offset",
                offset,
                "--format",
                "env",
            ],
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


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/srv/pm_as_ofi/repo")
    ap.add_argument("--shared-ingress-root", default="/srv/pm_as_ofi/shared-ingress-main")
    ap.add_argument("--prefix", default="btc-updown-5m")
    ap.add_argument("--round-offsets", default="0,1,2,3")
    ap.add_argument("--duration-s", type=int, default=1740)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--edge", type=float, default=0.022)
    ap.add_argument("--queue-share", type=float, default=0.50)
    ap.add_argument("--target-qty", type=float, default=115.0)
    ap.add_argument("--max-seed-qty", type=float, default=60.0)
    ap.add_argument("--max-open-cost", type=float, default=250.0)
    ap.add_argument("--seed-l1-cap", type=float, default=1.02)
    ap.add_argument("--seed-px-lo", type=float, default=0.05)
    ap.add_argument("--seed-px-hi", type=float, default=0.90)
    ap.add_argument("--seed-offset-max-s", type=float, default=240.0)
    ap.add_argument("--base-cap", type=float, default=0.990)
    ap.add_argument("--late-cap", type=float, default=0.995)
    ap.add_argument("--final-cap", type=float, default=1.010)
    ap.add_argument("--order-ttl-s", type=float, default=120.0)
    ap.add_argument("--inventory-gate-qty", type=float, default=6.0)
    ap.add_argument("--inventory-gate-cost", type=float, default=6.0)
    ap.add_argument("--inventory-reduced-target", type=float, default=30.0)
    ap.add_argument("--block-same-side-exposure", action="store_true")
    args = ap.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    cfg = RunnerConfig(
        edge=args.edge,
        queue_share=args.queue_share,
        target_qty=args.target_qty,
        max_seed_qty=args.max_seed_qty,
        max_open_cost=args.max_open_cost,
        seed_l1_cap=args.seed_l1_cap,
        seed_px_lo=args.seed_px_lo,
        seed_px_hi=args.seed_px_hi,
        seed_offset_max_s=args.seed_offset_max_s,
        base_cap=args.base_cap,
        late_cap=args.late_cap,
        final_cap=args.final_cap,
        order_ttl_ms=int(args.order_ttl_s * 1000),
        inventory_gate_qty=args.inventory_gate_qty,
        inventory_gate_cost=args.inventory_gate_cost,
        inventory_reduced_target=args.inventory_reduced_target,
        block_same_side_exposure=args.block_same_side_exposure,
    )
    markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets)
    manifest = {
        "kind": "manifest",
        "created_ms": now_ms(),
        "script": "xuan_m0001_maker_shadow_runner.py",
        "duration_s": args.duration_s,
        "markets": markets,
        "config": cfg.__dict__,
        "safety": {
            "orders_sent": False,
            "dry_run": os.environ.get("PM_DRY_RUN"),
            "shared_ingress_role": os.environ.get("PM_SHARED_INGRESS_ROLE"),
            "shared_ingress_root": args.shared_ingress_root,
            "instance_id": os.environ.get("PM_INSTANCE_ID"),
        },
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    socket_path = str(Path(args.shared_ingress_root) / "market.sock")
    await asyncio.gather(*(run_market(m, socket_path, out, args.duration_s, cfg) for m in markets))


if __name__ == "__main__":
    asyncio.run(main())
