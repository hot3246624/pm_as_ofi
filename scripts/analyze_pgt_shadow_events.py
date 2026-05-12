#!/usr/bin/env python3
"""Analyze PGT shadow JSONL recorder output.

This reads local recorder JSONL files only. It does not read raw market data and
does not modify recorder state.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class Fill:
    recv_ms: int
    side: str
    price: float
    size: float
    source: str = ""


@dataclass
class RoundRow:
    round_id: int
    slug: str
    path: str
    complete: bool = False
    fills: list[Fill] = field(default_factory=list)
    paired_qty: float = 0.0
    pair_cost: float = 0.0
    residual_qty: float = 0.0
    yes_qty: float = 0.0
    yes_avg_cost: float = 0.0
    no_qty: float = 0.0
    no_avg_cost: float = 0.0
    locked_pnl: float = 0.0
    residual_cost_worst_case: float = 0.0
    worst_case_pnl: float = 0.0
    taker_repairs: int = 0
    dry_run_touch_book: int = 0
    dry_run_touch_trade: int = 0
    dry_run_touch_other: int = 0
    cancel_sent: int = 0
    accepted_orders: int = 0
    merge_executed: int = 0
    last_recv_ms: int = 0

    @property
    def first_fill_price(self) -> float | None:
        return self.fills[0].price if self.fills else None

    @property
    def first_fill_side(self) -> str | None:
        return self.fills[0].side if self.fills else None

    @property
    def completion_delay_s(self) -> float | None:
        if not self.fills:
            return None
        first = self.fills[0]
        for fill in self.fills[1:]:
            if fill.side != first.side:
                return (fill.recv_ms - first.recv_ms) / 1000.0
        return None

    @property
    def turnover_cost(self) -> float:
        return self.paired_qty * self.pair_cost + self.residual_cost_worst_case


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--instance", default="xuan_ladder_v1_brake_full")
    p.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    p.add_argument("--root", default="data/recorder")
    p.add_argument("--last", type=int, default=24, help="last complete rounds window")
    p.add_argument("--from-round", type=int, help="only include rounds with id >= this value")
    p.add_argument("--to-round", type=int, help="only include rounds with id <= this value")
    p.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    p.add_argument("--details", action="store_true", help="print per-round rows for the last window")
    return p.parse_args()


def event_payload(row: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    payload = row.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        return payload.get("event"), data if isinstance(data, dict) else {}
    return row.get("event"), row.get("data") if isinstance(row.get("data"), dict) else {}


def load_round(path: Path) -> RoundRow:
    slug = path.parent.name
    try:
        round_id = int(slug.rsplit("-", 1)[1])
    except Exception:
        round_id = int(path.parts[-4])
    out = RoundRow(round_id=round_id, slug=slug, path=str(path))
    for line in path.open(encoding="utf-8", errors="ignore"):
        try:
            row = json.loads(line)
        except Exception:
            continue
        out.last_recv_ms = max(out.last_recv_ms, int(row.get("recv_unix_ms") or 0))
        event, data = event_payload(row)
        if event == "fill_snapshot" and str(data.get("direction", "")).lower() == "buy":
            side = str(data.get("side") or "").upper()
            if side in {"YES", "NO"}:
                out.fills.append(
                    Fill(
                        recv_ms=int(row.get("recv_unix_ms") or 0),
                        side=side,
                        price=float(data.get("price") or 0.0),
                        size=float(data.get("size") or 0.0),
                        source=str(data.get("fill_source") or data.get("source") or ""),
                    )
                )
        elif event == "pgt_shadow_summary":
            out.complete = True
            out.paired_qty = float(data.get("paired_qty") or 0.0)
            out.pair_cost = float(data.get("pair_cost") or 0.0)
            out.residual_qty = float(data.get("residual_qty") or 0.0)
            out.yes_qty = float(data.get("yes_qty") or 0.0)
            out.yes_avg_cost = float(data.get("yes_avg_cost") or 0.0)
            out.no_qty = float(data.get("no_qty") or 0.0)
            out.no_avg_cost = float(data.get("no_avg_cost") or 0.0)
        elif event == "taker_repair_sent":
            out.taker_repairs += 1
        elif event == "dry_run_touch_fill_confirmed":
            source = str(data.get("source") or "")
            if source == "book_touch":
                out.dry_run_touch_book += 1
            elif source == "trade_sell_touch":
                out.dry_run_touch_trade += 1
            else:
                out.dry_run_touch_other += 1
        elif event == "cancel_sent":
            out.cancel_sent += 1
        elif event == "order_accepted":
            out.accepted_orders += 1
        elif event == "merge_executed":
            out.merge_executed += 1

    out.locked_pnl = out.paired_qty * (1.0 - out.pair_cost)
    out.residual_cost_worst_case = (
        max(out.yes_qty - out.paired_qty, 0.0) * out.yes_avg_cost
        + max(out.no_qty - out.paired_qty, 0.0) * out.no_avg_cost
    )
    out.worst_case_pnl = out.locked_pnl - out.residual_cost_worst_case
    return out


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


def summarize(rows: list[RoundRow]) -> dict[str, Any]:
    paired_rows = [r for r in rows if r.paired_qty > 1e-9]
    residual_rows = [r for r in rows if r.residual_qty > 1e-9]
    paired_qty = sum(r.paired_qty for r in rows)
    paired_cost = sum(r.paired_qty * r.pair_cost for r in rows)
    turnover = sum(r.turnover_cost for r in rows)
    pair_costs = [r.pair_cost for r in paired_rows]
    delays = [r.completion_delay_s for r in rows if r.completion_delay_s is not None]
    fill_sources: dict[str, int] = {}
    for r in rows:
        for f in r.fills:
            key = f.source or "unknown"
            fill_sources[key] = fill_sources.get(key, 0) + 1
    return {
        "rounds": len(rows),
        "range": [rows[0].round_id, rows[-1].round_id] if rows else None,
        "paired_rounds": len(paired_rows),
        "residual_rounds": len(residual_rows),
        "weighted_pair_cost": paired_cost / paired_qty if paired_qty > 1e-9 else None,
        "paired_qty": paired_qty,
        "locked_pnl": sum(r.locked_pnl for r in rows),
        "residual_cost_worst_case": sum(r.residual_cost_worst_case for r in rows),
        "worst_case_pnl": sum(r.worst_case_pnl for r in rows),
        "turnover_cost": turnover,
        "worst_case_roi": (
            sum(r.worst_case_pnl for r in rows) / turnover if turnover > 1e-9 else None
        ),
        "pair_cost_min": min(pair_costs) if pair_costs else None,
        "pair_cost_median": statistics.median(pair_costs) if pair_costs else None,
        "pair_cost_p90": percentile(pair_costs, 90.0),
        "pair_cost_max": max(pair_costs) if pair_costs else None,
        "completion_delay_min_s": min(delays) if delays else None,
        "completion_delay_median_s": statistics.median(delays) if delays else None,
        "completion_delay_p90_s": percentile(delays, 90.0),
        "completion_delay_max_s": max(delays) if delays else None,
        "fills": sum(len(r.fills) for r in rows),
        "taker_repairs": sum(r.taker_repairs for r in rows),
        "dry_run_touch_book": sum(r.dry_run_touch_book for r in rows),
        "dry_run_touch_trade": sum(r.dry_run_touch_trade for r in rows),
        "dry_run_touch_other": sum(r.dry_run_touch_other for r in rows),
        "fill_sources": dict(sorted(fill_sources.items())),
        "cancel_sent": sum(r.cancel_sent for r in rows),
        "merge_executed": sum(r.merge_executed for r in rows),
        "residuals": [
            {
                "round_id": r.round_id,
                "first_side": r.first_fill_side,
                "first_price": r.first_fill_price,
                "residual_qty": r.residual_qty,
                "residual_cost_worst_case": r.residual_cost_worst_case,
                "fills": [
                    {
                        "side": f.side,
                        "price": f.price,
                        "size": f.size,
                        "source": f.source,
                    }
                    for f in r.fills
                ],
            }
            for r in residual_rows
        ],
    }


def bucket_summary(rows: list[RoundRow]) -> list[dict[str, Any]]:
    buckets = [(0.0, 0.42), (0.42, 0.45), (0.45, 0.48), (0.48, 0.50), (0.50, 0.60), (0.60, 1.0)]
    out = []
    for lo, hi in buckets:
        bucket_rows = [
            r
            for r in rows
            if r.first_fill_price is not None and lo < r.first_fill_price <= hi
        ]
        if not bucket_rows:
            continue
        item = summarize(bucket_rows)
        item["first_price_bucket"] = f"({lo},{hi}]"
        out.append(item)
    return out


def round_details(rows: list[RoundRow]) -> list[dict[str, Any]]:
    return [
        {
            "round_id": r.round_id,
            "pair_cost": r.pair_cost,
            "locked_pnl": r.locked_pnl,
            "worst_case_pnl": r.worst_case_pnl,
            "residual_qty": r.residual_qty,
            "completion_delay_s": r.completion_delay_s,
            "first_side": r.first_fill_side,
            "first_price": r.first_fill_price,
            "fills": [
                {
                    "side": f.side,
                    "price": f.price,
                    "size": f.size,
                    "recv_ms": f.recv_ms,
                    "source": f.source,
                }
                for f in r.fills
            ],
            "taker_repairs": r.taker_repairs,
            "dry_run_touch_book": r.dry_run_touch_book,
            "dry_run_touch_trade": r.dry_run_touch_trade,
            "dry_run_touch_other": r.dry_run_touch_other,
            "cancel_sent": r.cancel_sent,
            "accepted_orders": r.accepted_orders,
            "merge_executed": r.merge_executed,
        }
        for r in rows
    ]


def collect_rows(root: Path, instance: str, date: str) -> list[RoundRow]:
    base = root / instance
    rows_by_path: dict[Path, RoundRow] = {}
    patterns = (
        f"[0-9]*/{date}/btc-updown-5m-*/events.jsonl",
        f"{date}/btc-updown-5m-*/events.jsonl",
    )
    for pattern in patterns:
        for path in base.glob(pattern):
            rows_by_path[path] = load_round(path)
    return sorted(rows_by_path.values(), key=lambda r: r.round_id)


def filter_rows(
    rows: list[RoundRow],
    from_round: int | None,
    to_round: int | None,
) -> list[RoundRow]:
    if from_round is not None:
        rows = [r for r in rows if r.round_id >= from_round]
    if to_round is not None:
        rows = [r for r in rows if r.round_id <= to_round]
    return rows


def main() -> None:
    args = parse_args()
    rows = filter_rows(
        collect_rows(Path(args.root), args.instance, args.date),
        args.from_round,
        args.to_round,
    )
    complete = [r for r in rows if r.complete]
    last_complete = complete[-args.last :]
    incomplete = [r for r in rows if not r.complete]
    result = {
        "instance": args.instance,
        "date": args.date,
        "from_round": args.from_round,
        "to_round": args.to_round,
        "files": len(rows),
        "complete": len(complete),
        "incomplete": [
            {
                "round_id": r.round_id,
                "fills": len(r.fills),
                "accepted_orders": r.accepted_orders,
                "last_recv_ms": r.last_recv_ms,
            }
            for r in incomplete[-10:]
        ],
        "last_complete": summarize(last_complete),
        "all_complete": summarize(complete),
        "last_first_price_buckets": bucket_summary(last_complete),
        "all_first_price_buckets": bucket_summary(complete),
        # Backward-compatible alias retained for older ad-hoc consumers.
        "first_price_buckets": bucket_summary(complete),
        "last_round_details": round_details(last_complete),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    round_filter = ""
    if args.from_round is not None or args.to_round is not None:
        round_filter = f" from_round={args.from_round} to_round={args.to_round}"
    print(
        f"instance={args.instance} date={args.date}{round_filter} "
        f"files={len(rows)} complete={len(complete)}"
    )
    for name in ("last_complete", "all_complete"):
        s = result[name]
        print(
            f"{name}: rounds={s['rounds']} range={s['range']} paired={s['paired_rounds']} "
            f"residual={s['residual_rounds']} wpc={s['weighted_pair_cost']} "
            f"locked={s['locked_pnl']:.4f} residual_worst={s['residual_cost_worst_case']:.4f} "
            f"worst={s['worst_case_pnl']:.4f} roi={s['worst_case_roi']} "
            f"touch(book/trade/other)="
            f"{s['dry_run_touch_book']}/{s['dry_run_touch_trade']}/{s['dry_run_touch_other']}"
            f" fill_sources={s['fill_sources']}"
        )
    if result["incomplete"]:
        print(f"incomplete_tail={result['incomplete']}")
    print("last_first_price_buckets:")
    for b in result["last_first_price_buckets"]:
        print(
            f"  {b['first_price_bucket']} n={b['rounds']} residual={b['residual_rounds']} "
            f"locked={b['locked_pnl']:.4f} worst={b['worst_case_pnl']:.4f}"
        )
    print("all_first_price_buckets:")
    for b in result["all_first_price_buckets"]:
        print(
            f"  {b['first_price_bucket']} n={b['rounds']} residual={b['residual_rounds']} "
            f"locked={b['locked_pnl']:.4f} worst={b['worst_case_pnl']:.4f}"
        )
    if args.details:
        print("last_round_details:")
        for r in result["last_round_details"]:
            fills = " -> ".join(
                f"{f['side']}@{f['price']:.2f}x{f['size']:.0f}"
                + (f"[{f['source']}]" if f["source"] else "")
                for f in r["fills"]
            )
            delay = r["completion_delay_s"]
            delay_s = "none" if delay is None else f"{delay:.3f}s"
            print(
                f"  {r['round_id']} cost={r['pair_cost']:.3f} pnl={r['locked_pnl']:+.4f} "
                f"worst={r['worst_case_pnl']:+.4f} residual={r['residual_qty']:.2f} "
                f"delay={delay_s} taker={r['taker_repairs']} cancels={r['cancel_sent']} "
                f"orders={r['accepted_orders']} touch(book/trade/other)="
                f"{r['dry_run_touch_book']}/{r['dry_run_touch_trade']}/{r['dry_run_touch_other']} "
                f"fills={fills}"
            )


if __name__ == "__main__":
    main()
