#!/usr/bin/env python3
"""P2 public-evidence reachability audit for passive/improved first-leg prices."""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import duckdb


COLS = [
    "day",
    "event_kind",
    "event_id",
    "ts_ms",
    "condition_id",
    "offset_s",
    "side",
    "side_alignment",
    "l1_pair_ask",
    "public_trade_taker_side",
    "public_trade_price",
    "public_trade_size",
    "side_bid",
]
IDX = {name: idx for idx, name in enumerate(COLS)}


def get(row: tuple[Any, ...], name: str) -> Any:
    return row[IDX[name]]


def fnum(value: Any, default: float = math.nan) -> float:
    if value is None:
        return default
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def parse_store_arg(values: list[str]) -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    for value in values:
        store, _, days_s = value.partition(":")
        days = [item for item in days_s.split(",") if item]
        if not days:
            raise SystemExit(f"store argument must be STORE:YYYY-MM-DD[,YYYY-MM-DD...], got {value}")
        out.append((store, days))
    return out


def init_metrics() -> dict[str, Any]:
    return {
        "candidates": 0,
        "qty": 0.0,
        "bid_touch": 0,
        "bid_touch_qty": 0.0,
        "sell_touch": 0,
        "sell_touch_qty": 0.0,
        "any_touch": 0,
        "any_touch_qty": 0.0,
        "wait_bid_ms": [],
        "wait_sell_ms": [],
        "wait_any_ms": [],
    }


def pct(num: float, den: float) -> float | None:
    return None if den == 0 else round(num / den, 6)


def quantile_ms(values: list[int], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * q)))
    return round(ordered[idx] / 1000.0, 3)


def run_store(
    store: str,
    days: list[str],
    edges: list[float],
    ttl_ms: int,
    metrics: dict[float, dict[str, Any]],
) -> None:
    con = duckdb.connect(store, read_only=True)
    con.execute("PRAGMA threads=2")
    con.execute("PRAGMA memory_limit='4GB'")
    placeholders = ",".join("?" for _ in days)
    cur = con.execute(
        f"""
        select {",".join(COLS)}
        from completion_unwind_events
        where day in ({placeholders})
          and offset_s >= 0 and offset_s < 240
          and side in ('YES','NO')
          and event_kind in ('l1_price_change','public_trade')
        order by condition_id, side, ts_ms, event_id
        """,
        days,
    )
    pending: dict[float, dict[tuple[str, str], deque[dict[str, Any]]]] = {
        edge: defaultdict(deque) for edge in edges
    }
    start = time.time()
    batches = 0
    while True:
        rows = cur.fetchmany(50_000)
        if not rows:
            break
        batches += 1
        if batches % 20 == 0:
            print(json.dumps({"store": store, "batches": batches, "elapsed_s": round(time.time() - start, 1)}), flush=True)
        for row in rows:
            ts_ms = int(get(row, "ts_ms") or 0)
            key = (str(get(row, "condition_id") or ""), str(get(row, "side") or ""))
            event_kind = get(row, "event_kind")
            taker_side = get(row, "public_trade_taker_side")
            px = fnum(get(row, "public_trade_price"))
            side_bid = fnum(get(row, "side_bid"))

            for edge in edges:
                queue = pending[edge][key]
                keep: deque[dict[str, Any]] = deque()
                while queue:
                    seed = queue.popleft()
                    if ts_ms - seed["ts_ms"] > ttl_ms:
                        continue
                    if not seed["bid_done"] and not math.isnan(side_bid) and side_bid + 1e-12 >= seed["seed_px"]:
                        seed["bid_done"] = True
                        metrics[edge]["bid_touch"] += 1
                        metrics[edge]["bid_touch_qty"] += seed["qty"]
                        metrics[edge]["wait_bid_ms"].append(ts_ms - seed["ts_ms"])
                    if (
                        not seed["sell_done"]
                        and event_kind == "public_trade"
                        and taker_side == "SELL"
                        and not math.isnan(px)
                        and px <= seed["seed_px"] + 1e-12
                    ):
                        seed["sell_done"] = True
                        metrics[edge]["sell_touch"] += 1
                        metrics[edge]["sell_touch_qty"] += seed["qty"]
                        metrics[edge]["wait_sell_ms"].append(ts_ms - seed["ts_ms"])
                    if (seed["bid_done"] or seed["sell_done"]) and not seed["any_done"]:
                        seed["any_done"] = True
                        metrics[edge]["any_touch"] += 1
                        metrics[edge]["any_touch_qty"] += seed["qty"]
                        metrics[edge]["wait_any_ms"].append(ts_ms - seed["ts_ms"])
                    if not (seed["bid_done"] and seed["sell_done"]):
                        keep.append(seed)
                pending[edge][key] = keep

            if event_kind != "public_trade" or taker_side != "SELL" or get(row, "side_alignment") != "high":
                continue
            offset_s = fnum(get(row, "offset_s"))
            l1_pair = fnum(get(row, "l1_pair_ask"))
            size = fnum(get(row, "public_trade_size"), 0.0)
            if not (0 <= offset_s < 120 and 0.05 <= px <= 0.90 and size > 0):
                continue
            if math.isnan(l1_pair) or l1_pair > 1.02 + 1e-12:
                continue
            qty = min(60.0, size * 0.25)
            if qty <= 1.0:
                continue
            for edge in edges:
                seed_px = max(0.01, px - edge)
                metrics[edge]["candidates"] += 1
                metrics[edge]["qty"] += qty
                pending[edge][key].append(
                    {
                        "ts_ms": ts_ms,
                        "seed_px": seed_px,
                        "qty": qty,
                        "bid_done": False,
                        "sell_done": False,
                        "any_done": False,
                    }
                )
    con.close()


def summarize(metrics: dict[float, dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for edge, m in metrics.items():
        out[str(edge)] = {
            "candidates": m["candidates"],
            "qty": round(m["qty"], 4),
            "bid_touch_rate": pct(m["bid_touch"], m["candidates"]),
            "bid_touch_qty_rate": pct(m["bid_touch_qty"], m["qty"]),
            "sell_touch_rate": pct(m["sell_touch"], m["candidates"]),
            "sell_touch_qty_rate": pct(m["sell_touch_qty"], m["qty"]),
            "any_touch_rate": pct(m["any_touch"], m["candidates"]),
            "any_touch_qty_rate": pct(m["any_touch_qty"], m["qty"]),
            "wait_any_p50_s": quantile_ms(m["wait_any_ms"], 0.5),
            "wait_any_p90_s": quantile_ms(m["wait_any_ms"], 0.9),
            "wait_sell_p50_s": quantile_ms(m["wait_sell_ms"], 0.5),
            "wait_sell_p90_s": quantile_ms(m["wait_sell_ms"], 0.9),
            "wait_bid_p50_s": quantile_ms(m["wait_bid_ms"], 0.5),
            "wait_bid_p90_s": quantile_ms(m["wait_bid_ms"], 0.9),
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", action="append", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--edges", type=float, nargs="+", default=[0.022, 0.030, 0.040])
    parser.add_argument("--ttl-s", type=float, default=120.0)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics = {edge: init_metrics() for edge in args.edges}
    for store, days in parse_store_arg(args.store):
        run_store(store, days, args.edges, int(args.ttl_s * 1000), metrics)
    summary = summarize(metrics)
    payload = {
        "model": "public P2 evidence for future bid/public SELL touching virtual passive first-leg price",
        "edges": args.edges,
        "ttl_s": args.ttl_s,
        "summary": summary,
    }
    (out_dir / "reachability_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"out_dir": str(out_dir), **payload}, indent=2))


if __name__ == "__main__":
    main()
