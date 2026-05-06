#!/usr/bin/env python3
"""Replay PGT shadow orders with public SELL trade-through fills.

This is intentionally conservative:
- only dry-run BUY orders are considered;
- a shadow maker order fills only when replay has public taker SELL trades
  on the same market side at or below our bid while that order is live;
- fill quantity is capped by observed public SELL size and order size.

It does not claim queue truth. It is a lower-confidence-but-less-optimistic
sanity check than accepting every book-touch as a full maker fill.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RECORDER_ROOT = REPO_ROOT / "data" / "recorder"
DEFAULT_REPLAY_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/replay")


@dataclass(frozen=True)
class OrderLife:
    round_id: int
    slug: str
    date: str
    condition_id: str
    slot: str
    side: str
    price: float
    size: float
    reason: str
    accepted_ms: int
    end_ms: int


@dataclass(frozen=True)
class Fill:
    ts_ms: int
    side: str
    price: float
    size: float
    order_price: float
    reason: str


@dataclass
class RoundSim:
    round_id: int
    slug: str
    date: str
    matched_replay: bool = False
    orders: int = 0
    fills: list[Fill] = field(default_factory=list)

    @property
    def yes_qty(self) -> float:
        return sum(f.size for f in self.fills if f.side == "YES")

    @property
    def no_qty(self) -> float:
        return sum(f.size for f in self.fills if f.side == "NO")

    @property
    def yes_cost(self) -> float:
        return sum(f.size * f.price for f in self.fills if f.side == "YES")

    @property
    def no_cost(self) -> float:
        return sum(f.size * f.price for f in self.fills if f.side == "NO")

    @property
    def yes_avg(self) -> float:
        return self.yes_cost / self.yes_qty if self.yes_qty > 1e-9 else 0.0

    @property
    def no_avg(self) -> float:
        return self.no_cost / self.no_qty if self.no_qty > 1e-9 else 0.0

    @property
    def paired_qty(self) -> float:
        return min(self.yes_qty, self.no_qty)

    @property
    def pair_cost(self) -> float | None:
        if self.paired_qty <= 1e-9:
            return None
        return self.yes_avg + self.no_avg

    @property
    def residual_qty(self) -> float:
        return abs(self.yes_qty - self.no_qty)

    @property
    def locked_pnl(self) -> float:
        pc = self.pair_cost
        return 0.0 if pc is None else self.paired_qty * (1.0 - pc)

    @property
    def residual_cost_worst(self) -> float:
        if self.yes_qty > self.no_qty:
            return (self.yes_qty - self.no_qty) * self.yes_avg
        if self.no_qty > self.yes_qty:
            return (self.no_qty - self.yes_qty) * self.no_avg
        return 0.0

    @property
    def worst_case_pnl(self) -> float:
        return self.locked_pnl - self.residual_cost_worst

    @property
    def turnover_cost(self) -> float:
        return self.yes_cost + self.no_cost


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--instance", default="xuan_ladder_v1_brake_full")
    p.add_argument("--date", action="append", required=True)
    p.add_argument("--recorder-root", default=str(DEFAULT_RECORDER_ROOT))
    p.add_argument("--replay-root", default=str(DEFAULT_REPLAY_ROOT))
    p.add_argument("--last", type=int, default=0)
    p.add_argument("--details", type=int, default=24)
    p.add_argument(
        "--order-size-cap",
        type=float,
        default=0.0,
        help="Optional cap applied to each shadow order before replay simulation",
    )
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def event_payload(row: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    payload = row.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        return payload.get("event"), data if isinstance(data, dict) else {}
    return row.get("event"), row.get("data") if isinstance(row.get("data"), dict) else {}


def round_id_from_slug(slug: str) -> int:
    return int(slug.rsplit("-", 1)[1])


def close_order(
    orders: list[OrderLife],
    slug: str,
    date: str,
    condition_id: str,
    slot: str,
    order: dict[str, Any] | None,
    end_ms: int,
) -> None:
    if not order:
        return
    side = str(order.get("side") or "").upper()
    if side not in {"YES", "NO"}:
        return
    accepted_ms = int(order.get("accepted_ms") or 0)
    if accepted_ms <= 0 or end_ms < accepted_ms:
        return
    orders.append(
        OrderLife(
            round_id=round_id_from_slug(slug),
            slug=slug,
            date=date,
            condition_id=condition_id,
            slot=slot,
            side=side,
            price=float(order.get("price") or 0.0),
            size=float(order.get("size") or 0.0),
            reason=str(order.get("reason") or ""),
            accepted_ms=accepted_ms,
            end_ms=end_ms,
        )
    )


def load_orders(path: Path) -> list[OrderLife]:
    slug = path.parent.name
    date = path.parent.parent.name
    orders: list[OrderLife] = []
    active: dict[str, dict[str, Any]] = {}
    last_ms = 0
    last_condition_id = ""
    for line in path.open(encoding="utf-8", errors="ignore"):
        try:
            row = json.loads(line)
        except Exception:
            continue
        recv_ms = int(row.get("recv_unix_ms") or 0)
        last_ms = max(last_ms, recv_ms)
        condition_id = str(row.get("condition_id") or row.get("market_id") or last_condition_id)
        if condition_id:
            last_condition_id = condition_id
        event, data = event_payload(row)
        if event == "order_accepted" and str(data.get("direction", "")).lower() == "buy":
            slot = str(data.get("slot") or "")
            if slot:
                active[slot] = {
                    "accepted_ms": recv_ms,
                    "side": str(data.get("side") or "").upper(),
                    "price": float(data.get("price") or 0.0),
                    "size": float(data.get("size") or 0.0),
                    "reason": str(data.get("reason") or ""),
                }
        elif event == "cancel_ack":
            slot = str(data.get("slot") or "")
            if slot:
                close_order(orders, slug, date, last_condition_id, slot, active.pop(slot, None), recv_ms)
        elif event == "fill_snapshot":
            slot = str(data.get("slot") or "")
            if slot:
                close_order(orders, slug, date, last_condition_id, slot, active.pop(slot, None), recv_ms)
    for slot, order in list(active.items()):
        close_order(orders, slug, date, last_condition_id, slot, order, last_ms)
    return orders


def collect_orders(root: Path, instance: str, dates: list[str]) -> list[OrderLife]:
    base = root / instance
    orders: list[OrderLife] = []
    for date in dates:
        patterns = [
            f"[0-9]*/{date}/btc-updown-5m-*/events.jsonl",
            f"{date}/btc-updown-5m-*/events.jsonl",
        ]
        for pattern in patterns:
            for path in base.glob(pattern):
                orders.extend(load_orders(path))
    return sorted(orders, key=lambda o: (o.round_id, o.accepted_ms, o.slot))


def connect_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def build_market_index(replay_root: Path, slugs: set[str]) -> dict[str, tuple[Path, str]]:
    out: dict[str, tuple[Path, str]] = {}
    if not slugs:
        return out
    placeholders = ",".join("?" for _ in slugs)
    params = tuple(sorted(slugs))
    for db_path in sorted(replay_root.glob("20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/crypto_5m.sqlite")):
        try:
            conn = connect_ro(db_path)
        except sqlite3.Error:
            continue
        try:
            for slug, condition_id in conn.execute(
                f"SELECT slug, condition_id FROM market_meta WHERE symbol='BTC' AND slug IN ({placeholders})",
                params,
            ):
                out[str(slug)] = (db_path, str(condition_id))
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    return out


def trade_sells(
    conn: sqlite3.Connection,
    condition_id: str,
    side: str,
    limit_price: float,
    start_ms: int,
    end_ms: int,
) -> list[tuple[int, float, float]]:
    return [
        (int(ts), float(price), float(size))
        for ts, price, size in conn.execute(
            """
            SELECT COALESCE(trade_ts_ms, recv_ms), price, size
            FROM md_trades
            WHERE condition_id=?
              AND UPPER(COALESCE(market_side, ''))=?
              AND UPPER(COALESCE(taker_side, ''))='SELL'
              AND COALESCE(trade_ts_ms, recv_ms) >= ?
              AND COALESCE(trade_ts_ms, recv_ms) <= ?
              AND price <= ? + 1e-9
            ORDER BY COALESCE(trade_ts_ms, recv_ms), capture_seq
            """,
            (condition_id, side, start_ms, end_ms, limit_price),
        )
    ]


def simulate(
    orders: list[OrderLife],
    market_index: dict[str, tuple[Path, str]],
    order_size_cap: float = 0.0,
) -> list[RoundSim]:
    by_round: dict[int, RoundSim] = {}
    conns: dict[Path, sqlite3.Connection] = {}
    try:
        for order in orders:
            row = by_round.setdefault(
                order.round_id,
                RoundSim(round_id=order.round_id, slug=order.slug, date=order.date),
            )
            row.orders += 1
            market = market_index.get(order.slug)
            if market is None:
                continue
            db_path, condition_id = market
            row.matched_replay = True
            conn = conns.get(db_path)
            if conn is None:
                conn = connect_ro(db_path)
                conns[db_path] = conn
            order_size = min(order.size, order_size_cap) if order_size_cap > 1e-9 else order.size
            remaining = order_size
            for ts_ms, trade_price, trade_size in trade_sells(
                conn,
                condition_id,
                order.side,
                order.price,
                order.accepted_ms,
                order.end_ms,
            ):
                if remaining <= 1e-9:
                    break
                fill_size = min(remaining, trade_size)
                if fill_size <= 1e-9:
                    continue
                row.fills.append(
                    Fill(
                        ts_ms=ts_ms,
                        side=order.side,
                        price=order.price,
                        size=fill_size,
                        order_price=order.price,
                        reason=order.reason,
                    )
                )
                remaining -= fill_size
    finally:
        for conn in conns.values():
            conn.close()
    return sorted(by_round.values(), key=lambda r: r.round_id)


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * pct / 100.0
    lo = math.floor(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def summarize(rows: list[RoundSim]) -> dict[str, Any]:
    matched = [r for r in rows if r.matched_replay]
    filled = [r for r in matched if r.fills]
    paired = [r for r in matched if r.paired_qty > 1e-9]
    residual = [r for r in matched if r.residual_qty > 1e-9]
    paired_qty = sum(r.paired_qty for r in matched)
    paired_cost = sum(r.paired_qty * (r.pair_cost or 0.0) for r in matched)
    turnover = sum(r.turnover_cost for r in matched)
    pair_costs = [r.pair_cost for r in paired if r.pair_cost is not None]
    return {
        "rounds": len(rows),
        "matched_rounds": len(matched),
        "filled_rounds": len(filled),
        "paired_rounds": len(paired),
        "residual_rounds": len(residual),
        "orders": sum(r.orders for r in matched),
        "fills": sum(len(r.fills) for r in matched),
        "yes_qty": sum(r.yes_qty for r in matched),
        "no_qty": sum(r.no_qty for r in matched),
        "paired_qty": paired_qty,
        "residual_qty": sum(r.residual_qty for r in matched),
        "weighted_pair_cost": paired_cost / paired_qty if paired_qty > 1e-9 else None,
        "pair_cost_median": statistics.median(pair_costs) if pair_costs else None,
        "pair_cost_p90": percentile(pair_costs, 90.0),
        "locked_pnl": sum(r.locked_pnl for r in matched),
        "residual_cost_worst": sum(r.residual_cost_worst for r in matched),
        "worst_case_pnl": sum(r.worst_case_pnl for r in matched),
        "turnover_cost": turnover,
        "worst_case_roi": sum(r.worst_case_pnl for r in matched) / turnover if turnover > 1e-9 else None,
    }


def row_dict(r: RoundSim) -> dict[str, Any]:
    return {
        "round_id": r.round_id,
        "slug": r.slug,
        "matched_replay": r.matched_replay,
        "orders": r.orders,
        "fills": len(r.fills),
        "yes_qty": r.yes_qty,
        "yes_avg": r.yes_avg,
        "no_qty": r.no_qty,
        "no_avg": r.no_avg,
        "paired_qty": r.paired_qty,
        "pair_cost": r.pair_cost,
        "residual_qty": r.residual_qty,
        "locked_pnl": r.locked_pnl,
        "worst_case_pnl": r.worst_case_pnl,
        "fill_list": [
            {"ts_ms": f.ts_ms, "side": f.side, "price": f.price, "size": f.size, "reason": f.reason}
            for f in r.fills
        ],
    }


def main() -> None:
    args = parse_args()
    orders = collect_orders(Path(args.recorder_root), args.instance, args.date)
    if args.last > 0:
        keep_rounds = sorted({o.round_id for o in orders})[-args.last :]
        keep = set(keep_rounds)
        orders = [o for o in orders if o.round_id in keep]
    market_index = build_market_index(Path(args.replay_root), {o.slug for o in orders})
    rows = simulate(orders, market_index, order_size_cap=args.order_size_cap)
    result = {
        "instance": args.instance,
        "dates": args.date,
        "order_size_cap": args.order_size_cap if args.order_size_cap > 1e-9 else None,
        "summary": summarize(rows),
        "details": [row_dict(r) for r in rows[-args.details :]],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    s = result["summary"]
    print(
        f"instance={args.instance} dates={args.date} rounds={s['rounds']} matched={s['matched_rounds']} "
        f"filled={s['filled_rounds']} paired={s['paired_rounds']} residual={s['residual_rounds']} "
        f"orders={s['orders']} fills={s['fills']}"
    )
    print(
        f"qty yes={s['yes_qty']:.2f} no={s['no_qty']:.2f} paired={s['paired_qty']:.2f} "
        f"residual={s['residual_qty']:.2f}"
    )
    print(
        f"wpc={s['weighted_pair_cost']} pair_cost_median={s['pair_cost_median']} "
        f"p90={s['pair_cost_p90']} locked={s['locked_pnl']:.4f} "
        f"residual_worst={s['residual_cost_worst']:.4f} worst={s['worst_case_pnl']:.4f} "
        f"roi={s['worst_case_roi']}"
    )
    if args.details:
        print("details_tail:")
        for item in result["details"]:
            fills = " -> ".join(
                f"{f['side']}@{f['price']:.2f}x{f['size']:.2f}" for f in item["fill_list"]
            )
            print(
                f"  {item['round_id']} matched={item['matched_replay']} orders={item['orders']} "
                f"fills={item['fills']} yes={item['yes_qty']:.2f}@{item['yes_avg']:.3f} "
                f"no={item['no_qty']:.2f}@{item['no_avg']:.3f} paired={item['paired_qty']:.2f} "
                f"pc={item['pair_cost']} residual={item['residual_qty']:.2f} "
                f"worst={item['worst_case_pnl']:+.4f} fills={fills}"
            )


if __name__ == "__main__":
    main()
