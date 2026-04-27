#!/usr/bin/env python3
"""Build xuan 30s completion truth dataset and export default gate config."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


def repo_data_path(*parts: str) -> Path:
    local = REPO_ROOT.joinpath(*parts)
    if local.exists():
        return local
    sibling = REPO_ROOT.parent / "pm_as_ofi" / Path(*parts)
    if sibling.exists():
        return sibling
    return local


DEFAULT_TRADES = repo_data_path("data", "xuan", "trades_long.json")
DEFAULT_ACTIVITY = repo_data_path("data", "xuan", "activity_long.json")
DEFAULT_SUMMARY = REPO_ROOT / "docs" / "xuan_completion_gate_summary.json"
DEFAULT_DEFAULTS = REPO_ROOT / "configs" / "xuan_completion_gate_defaults.json"
DEFAULT_EPISODES = REPO_ROOT / "data" / "xuan" / "xuan_completion_episodes.csv"


FEATURE_PLANS = {
    "round_close_rel_s": [
        ("le_15", None, 15.0),
        ("15_60", 15.0, 60.0),
        ("60_120", 60.0, 120.0),
        ("120_180", 120.0, 180.0),
        ("gt_180", 180.0, None),
    ],
    "prior_imbalance_value": [
        ("lt_0.02", None, 0.02),
        ("0.02_0.05", 0.02, 0.05),
        ("0.05_0.08", 0.05, 0.08),
        ("gt_0.08", 0.08, None),
    ],
    "l1_spread_ticks_first_side": [
        ("le_1.5", None, 1.5),
        ("1.5_3", 1.5, 3.0),
        ("gt_3", 3.0, None),
    ],
    "l1_spread_ticks_opposite_side": [
        ("le_1.5", None, 1.5),
        ("1.5_3", 1.5, 3.0),
        ("gt_3", 3.0, None),
    ],
    "mid_skew_to_opposite": [
        ("near_zero", -0.02, 0.02),
        ("mild", -0.05, 0.05),
        ("extreme", None, None),
    ],
    "buy_fill_count_before_open": [
        ("le_1", None, 1.0),
        ("2_3", 2.0, 3.0),
        ("gt_3", 4.0, None),
    ],
}


@dataclass
class ReplaySource:
    path: Path
    conn: sqlite3.Connection
    schema_kind: str
    date_label: str
    slug_set: set[str]
    condition_meta: dict[str, dict[str, Any]]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    p.add_argument("--activity", type=Path, default=DEFAULT_ACTIVITY)
    p.add_argument("--db", type=Path, action="append", default=None)
    p.add_argument("--out-summary-json", type=Path, default=DEFAULT_SUMMARY)
    p.add_argument("--out-defaults-json", type=Path, default=DEFAULT_DEFAULTS)
    p.add_argument("--out-episodes-csv", type=Path, default=DEFAULT_EPISODES)
    p.add_argument("--max-book-age-ms", type=int, default=1500)
    return p.parse_args()


def find_default_replay_dbs() -> list[Path]:
    matches: list[Path] = []
    for pattern in (
        "data/replay_recorder/*/crypto_5m.sqlite",
        "../pm_as_ofi/data/replay_recorder/*/crypto_5m.sqlite",
    ):
        matches.extend(sorted(REPO_ROOT.glob(pattern)))
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in matches:
        if path not in seen and path.exists():
            seen.add(path)
            deduped.append(path)
    if not deduped:
        raise SystemExit("no replay sqlite found under data/replay_recorder/*/crypto_5m.sqlite")
    return deduped


def load_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"missing json file: {path}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise SystemExit(f"expected list json: {path}")
    return [row for row in rows if isinstance(row, dict)]


def trade_key(row: dict[str, Any]) -> str:
    tx = str(row.get("transactionHash") or "")
    if tx:
        return tx
    return "|".join(
        [
            str(row.get("slug") or ""),
            str(row.get("timestamp") or ""),
            str(row.get("asset") or ""),
            str(row.get("price") or ""),
            str(row.get("size") or ""),
        ]
    )


def merged_trade_rows(trades_path: Path, activity_path: Path) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for source_path in (activity_path, trades_path):
        for row in load_json_rows(source_path):
            if str(row.get("side") or "").upper() != "BUY":
                continue
            slug = str(row.get("slug") or "")
            if not slug.startswith("btc-updown-5m-"):
                continue
            key = trade_key(row)
            merged.setdefault(key, {}).update(row)
    return sorted(
        merged.values(),
        key=lambda row: (
            str(row.get("slug") or ""),
            float(row.get("timestamp") or 0),
            str(row.get("transactionHash") or ""),
            str(row.get("asset") or ""),
        ),
    )


def side_from_outcome(outcome: str) -> str:
    normalized = outcome.strip().lower()
    if normalized in {"up", "yes"}:
        return "YES"
    if normalized in {"down", "no"}:
        return "NO"
    return "UNKNOWN"


def as_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def as_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        return int(v)
    except Exception:
        return 0


def tick_tol(price: float, tick_size: float | None) -> float:
    tick = tick_size if tick_size and tick_size > 0 else 0.01
    if price >= 0.96 or price <= 0.04:
        tick = min(tick, 0.001)
    return max(tick * 0.51, 5e-4)


def table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def detect_schema_kind(conn: sqlite3.Connection) -> str:
    cols = set(table_columns(conn, "md_book_l1"))
    if {"condition_id", "recv_ms", "yes_bid_px", "yes_ask_px", "no_bid_px", "no_ask_px"}.issubset(cols):
        return "wide_condition"
    if {"slug", "recv_unix_ms", "side", "bid", "ask"}.issubset(cols):
        return "rowwise_asset"
    raise SystemExit(f"unsupported md_book_l1 schema columns: {sorted(cols)}")


def load_replay_source(path: Path) -> ReplaySource:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    schema_kind = detect_schema_kind(conn)
    if schema_kind == "rowwise_asset":
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_md_book_l1_slug_side_time ON md_book_l1(slug, side, recv_unix_ms)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_md_trades_slug_asset_time ON md_trades(slug, asset_id, recv_unix_ms)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_md_trades_slug_time ON md_trades(slug, recv_unix_ms)"
        )
        conn.commit()
    date_label = path.parent.name
    slug_set: set[str] = set()
    condition_meta: dict[str, dict[str, Any]] = {}
    if schema_kind == "wide_condition":
        rows = conn.execute("SELECT condition_id, slug FROM market_meta").fetchall()
        condition_meta = {
            str(row["condition_id"]): {"slug": str(row["slug"])}
            for row in rows
            if row["condition_id"] is not None
        }
        slug_set = {meta["slug"] for meta in condition_meta.values() if meta.get("slug")}
    else:
        rows = conn.execute("SELECT DISTINCT slug FROM md_book_l1").fetchall()
        slug_set = {str(row[0]) for row in rows}
    return ReplaySource(
        path=path,
        conn=conn,
        schema_kind=schema_kind,
        date_label=date_label,
        slug_set=slug_set,
        condition_meta=condition_meta,
    )


def market_present(source: ReplaySource, condition_id: str, slug: str) -> bool:
    if source.schema_kind == "wide_condition":
        return condition_id in source.condition_meta
    return slug in source.slug_set


def query_latest_book(
    source: ReplaySource,
    condition_id: str,
    slug: str,
    asset_id: str,
    outcome_side: str,
    trade_ts_ms: int,
) -> tuple[int, float | None, float | None, float | None] | None:
    if source.schema_kind == "wide_condition":
        row = source.conn.execute(
            """
            SELECT recv_ms, yes_bid_px, yes_ask_px, no_bid_px, no_ask_px
            FROM md_book_l1
            WHERE condition_id = ? AND recv_ms <= ?
            ORDER BY recv_ms DESC, capture_seq DESC
            LIMIT 1
            """,
            (condition_id, trade_ts_ms),
        ).fetchone()
        if row is None:
            return None
        recv_ms = as_int(row["recv_ms"])
        if outcome_side == "YES":
            return recv_ms, row["yes_bid_px"], row["yes_ask_px"], 0.01
        if outcome_side == "NO":
            return recv_ms, row["no_bid_px"], row["no_ask_px"], 0.01
        return recv_ms, None, None, 0.01
    row = source.conn.execute(
        """
        SELECT recv_unix_ms, bid, ask
        FROM md_book_l1
        WHERE slug = ? AND side = ? AND recv_unix_ms <= ?
        ORDER BY recv_unix_ms DESC, capture_seq DESC
        LIMIT 1
        """,
        (slug, asset_id, trade_ts_ms),
    ).fetchone()
    if row is None:
        return None
    return as_int(row["recv_unix_ms"]), row["bid"], row["ask"], 0.01


def query_trade_rate(
    source: ReplaySource,
    slug: str,
    asset_id: Optional[str],
    start_ms: int,
    end_ms: int,
) -> float:
    if asset_id:
        row = source.conn.execute(
            """
            SELECT COUNT(*)
            FROM md_trades
            WHERE slug = ? AND asset_id = ? AND recv_unix_ms BETWEEN ? AND ?
            """,
            (slug, asset_id, start_ms, end_ms),
        ).fetchone()
    else:
        row = source.conn.execute(
            """
            SELECT COUNT(*)
            FROM md_trades
            WHERE slug = ? AND recv_unix_ms BETWEEN ? AND ?
            """,
            (slug, start_ms, end_ms),
        ).fetchone()
    count = as_int(row[0] if row else 0)
    duration_s = max(1.0, (end_ms - start_ms) / 1000.0)
    return count / duration_s


def query_book_update_rate(
    source: ReplaySource,
    slug: str,
    asset_ids: list[str],
    start_ms: int,
    end_ms: int,
) -> float:
    if source.schema_kind == "wide_condition":
        row = source.conn.execute(
            """
            SELECT COUNT(*)
            FROM md_book_l1
            WHERE slug = ? AND recv_ms BETWEEN ? AND ?
            """,
            (slug, start_ms, end_ms),
        ).fetchone()
    else:
        placeholders = ",".join("?" for _ in asset_ids)
        params: list[Any] = [slug, start_ms, end_ms]
        query = """
            SELECT COUNT(*)
            FROM md_book_l1
            WHERE slug = ? AND recv_unix_ms BETWEEN ? AND ?
        """
        if asset_ids:
            query += f" AND side IN ({placeholders})"
            params.extend(asset_ids)
        row = source.conn.execute(query, params).fetchone()
    count = as_int(row[0] if row else 0)
    duration_s = max(1.0, (end_ms - start_ms) / 1000.0)
    return count / duration_s


def classify_proximity(
    trade_price: float,
    bid: float | None,
    ask: float | None,
    age_ms: int | None,
    max_book_age_ms: int,
    tol: float,
) -> str:
    if age_ms is None or age_ms < 0 or age_ms > max_book_age_ms:
        return "unknown"
    if bid is None or ask is None:
        return "unknown"
    dist_to_bid = abs(trade_price - bid)
    dist_to_ask = abs(ask - trade_price)
    if dist_to_bid + tol < dist_to_ask:
        return "maker_proximity"
    if dist_to_ask + tol < dist_to_bid:
        return "taker_proximity"
    return "balanced_proximity"


def spread_ticks(bid: float | None, ask: float | None, tick_size: float) -> float:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return 99.0
    return max(0.0, (float(ask) - float(bid)) / max(0.0001, tick_size))


def mid(bid: float | None, ask: float | None) -> float:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return 0.0
    return (float(bid) + float(ask)) / 2.0


def parse_round_start(slug: str) -> int:
    try:
        return int(slug.rsplit("-", 1)[1])
    except Exception:
        return 0


def bucket_name(plan: list[tuple[str, Optional[float], Optional[float]]], value: float) -> str:
    for label, min_v, max_v in plan:
        if min_v is not None and value < min_v:
            continue
        if max_v is not None and value > max_v:
            continue
        return label
    return "unknown"


def build_asset_maps(rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    assets: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        side = side_from_outcome(str(row.get("outcome") or ""))
        if side in {"YES", "NO"}:
            assets[str(row.get("slug") or "")][side] = str(row.get("asset") or "")
    return assets


def build_episodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    per_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        per_slug[str(row.get("slug") or "")].append(row)

    episodes: list[dict[str, Any]] = []
    for slug, slug_rows in per_slug.items():
        slug_rows.sort(key=lambda row: (as_float(row.get("timestamp")), str(row.get("transactionHash") or "")))
        current: Optional[dict[str, Any]] = None
        episode_id = 0
        total_prior_buys = 0

        def open_episode(side: str, row: dict[str, Any], qty: float) -> dict[str, Any]:
            nonlocal episode_id
            episode_id += 1
            ts_ms = int(as_float(row.get("timestamp")) * 1000)
            return {
                "slug": slug,
                "condition_id": str(row.get("conditionId") or ""),
                "episode_id": episode_id,
                "first_leg_ts_ms": ts_ms,
                "first_leg_side": side,
                "first_leg_clip": qty,
                "first_leg_price": as_float(row.get("price")),
                "first_qty": qty,
                "hedge_qty": 0.0,
                "first_opposite_ts_ms": None,
                "close_ts_ms": None,
                "same_side_before_opposite": False,
                "same_side_run_before_open": 0,
                "buy_fill_count_before_open": total_prior_buys,
            }

        for row in slug_rows:
            side = side_from_outcome(str(row.get("outcome") or ""))
            if side not in {"YES", "NO"}:
                continue
            total_prior_buys += 1
            qty = as_float(row.get("size"))
            if qty <= 0:
                continue
            ts_ms = int(as_float(row.get("timestamp")) * 1000)

            if current is None:
                current = open_episode(side, row, qty)
                continue

            if side == current["first_leg_side"]:
                current["first_qty"] += qty
                if current["first_opposite_ts_ms"] is None:
                    current["same_side_before_opposite"] = True
                continue

            residual_before = max(0.0, current["first_qty"] - current["hedge_qty"])
            consumed = min(qty, residual_before)
            if consumed > 0:
                current["hedge_qty"] += consumed
                if current["first_opposite_ts_ms"] is None:
                    current["first_opposite_ts_ms"] = ts_ms
            overshoot = max(0.0, qty - consumed)
            if current["hedge_qty"] >= current["first_qty"] - 1e-9:
                current["close_ts_ms"] = ts_ms
                episodes.append(current)
                current = None
            if overshoot > 1e-9:
                current = open_episode(side, row, overshoot)

        if current is not None:
            episodes.append(current)

    for row in episodes:
        first_delay_s = None
        close_delay_s = None
        if row.get("first_opposite_ts_ms") is not None:
            first_delay_s = (as_int(row["first_opposite_ts_ms"]) - as_int(row["first_leg_ts_ms"])) / 1000.0
        if row.get("close_ts_ms") is not None:
            close_delay_s = (as_int(row["close_ts_ms"]) - as_int(row["first_leg_ts_ms"])) / 1000.0
        row["first_opposite_delay_s"] = first_delay_s
        row["close_delay_s"] = close_delay_s
        row["label_clean_close_60s"] = bool(close_delay_s is not None and close_delay_s <= 60.0)
        row["label_complete_30s"] = bool(
            first_delay_s is not None
            and first_delay_s <= 30.0
            and close_delay_s is not None
            and close_delay_s <= 60.0
            and not row.get("same_side_before_opposite")
        )
    return episodes


def enrich_episode_with_market_features(
    episode: dict[str, Any],
    asset_maps: dict[str, dict[str, str]],
    sources: list[ReplaySource],
    latest_source_date: str,
    max_book_age_ms: int,
) -> dict[str, Any]:
    slug = str(episode.get("slug") or "")
    condition_id = str(episode.get("condition_id") or "")
    first_side = str(episode.get("first_leg_side") or "")
    opposite_side = "NO" if first_side == "YES" else "YES"
    first_asset = asset_maps.get(slug, {}).get(first_side)
    opposite_asset = asset_maps.get(slug, {}).get(opposite_side)
    ts_ms = as_int(episode.get("first_leg_ts_ms"))
    source_used: Optional[ReplaySource] = None
    first_book = None
    opposite_book = None

    if not first_asset or not opposite_asset:
        episode["censored"] = True
        episode["censored_reason"] = "asset_mapping_missing"
        return episode

    best_age = None
    for source in sources:
        if not market_present(source, condition_id, slug):
            continue
        fbook = query_latest_book(source, condition_id, slug, first_asset, first_side, ts_ms)
        obook = query_latest_book(source, condition_id, slug, opposite_asset, opposite_side, ts_ms)
        if fbook is None or obook is None:
            continue
        max_age = max(ts_ms - as_int(fbook[0]), ts_ms - as_int(obook[0]))
        if best_age is None or max_age < best_age:
            best_age = max_age
            source_used = source
            first_book = fbook
            opposite_book = obook

    if source_used is None or first_book is None or opposite_book is None:
        episode["censored"] = True
        episode["censored_reason"] = "no_l1_book"
        return episode

    first_age = ts_ms - as_int(first_book[0])
    opposite_age = ts_ms - as_int(opposite_book[0])
    book_age_ms = max(first_age, opposite_age)
    first_bid, first_ask, tick_size = first_book[1], first_book[2], first_book[3]
    opposite_bid, opposite_ask = opposite_book[1], opposite_book[2]
    first_mid = mid(first_bid, first_ask)
    opposite_mid = mid(opposite_bid, opposite_ask)
    prior_imbalance_value = abs(first_mid + opposite_mid - 1.0)
    prior_imbalance_bucket = bucket_name(FEATURE_PLANS["prior_imbalance_value"], prior_imbalance_value)
    first_spread = spread_ticks(first_bid, first_ask, tick_size or 0.01)
    opposite_spread = spread_ticks(opposite_bid, opposite_ask, tick_size or 0.01)
    maker_proxy = classify_proximity(
        as_float(episode.get("first_leg_price")),
        first_bid,
        first_ask,
        book_age_ms,
        max_book_age_ms,
        tick_tol(as_float(episode.get("first_leg_price")), tick_size),
    )
    recent_opposite_trade_rate_5s = query_trade_rate(
        source_used, slug, opposite_asset, ts_ms - 5_000, ts_ms
    )
    recent_total_trade_rate_15s = query_trade_rate(
        source_used, slug, None, ts_ms - 15_000, ts_ms
    )
    book_update_rate_5s = query_book_update_rate(
        source_used, slug, [first_asset, opposite_asset], ts_ms - 5_000, ts_ms
    )
    round_start = parse_round_start(slug)
    round_end = round_start + 300 if round_start else 0
    episode.update(
        {
            "round_open_rel_s": (ts_ms / 1000.0 - round_start) if round_start else 0.0,
            "round_close_rel_s": (round_end - ts_ms / 1000.0) if round_end else 0.0,
            "utc_hour_bucket": int((ts_ms // 1000) % 86_400 // 3_600),
            "prior_imbalance_value": prior_imbalance_value,
            "prior_imbalance_bucket": prior_imbalance_bucket,
            "same_side_run_before_open": as_int(episode.get("same_side_run_before_open")),
            "l1_spread_ticks_first_side": first_spread,
            "l1_spread_ticks_opposite_side": opposite_spread,
            "mid_skew_to_opposite": first_mid - (1.0 - opposite_mid) if first_mid and opposite_mid else 0.0,
            "recent_opposite_trade_rate_5s": recent_opposite_trade_rate_5s,
            "recent_total_trade_rate_15s": recent_total_trade_rate_15s,
            "book_update_rate_5s": book_update_rate_5s,
            "maker_taker_proxy": maker_proxy,
            "book_age_ms": book_age_ms,
            "overlap_source_date": source_used.date_label,
            "is_recent_overlap": source_used.date_label == latest_source_date,
            "censored": book_age_ms > max_book_age_ms,
            "censored_reason": "stale_book" if book_age_ms > max_book_age_ms else "",
        }
    )
    return episode


def mean_bool(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    return sum(1 for row in rows if row.get(key)) / len(rows)


def sign(value: float) -> int:
    if value > 1e-9:
        return 1
    if value < -1e-9:
        return -1
    return 0


def lift_to_score(lift: float) -> int:
    if lift >= 0.15:
        return 2
    if lift >= 0.05:
        return 1
    return 0


def bucket_matches(value: float, min_v: Optional[float], max_v: Optional[float]) -> bool:
    if min_v is not None and value < min_v:
        return False
    if max_v is not None and value > max_v:
        return False
    return True


def build_feature_bucket_defs(
    train_rows: list[dict[str, Any]],
    full_rows: list[dict[str, Any]],
    recent_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    baseline = mean_bool(train_rows, "label_complete_30s")
    out: dict[str, list[dict[str, Any]]] = {}
    for feature, plan in FEATURE_PLANS.items():
        buckets: list[dict[str, Any]] = []
        for label, min_v, max_v in plan:
            bucket_train = [row for row in train_rows if bucket_matches(as_float(row.get(feature)), min_v, max_v)]
            bucket_full = [row for row in full_rows if bucket_matches(as_float(row.get(feature)), min_v, max_v)]
            bucket_recent = [row for row in recent_rows if bucket_matches(as_float(row.get(feature)), min_v, max_v)]
            train_rate = mean_bool(bucket_train, "label_complete_30s")
            full_lift = mean_bool(bucket_full, "label_complete_30s") - mean_bool(full_rows, "label_complete_30s")
            recent_lift = mean_bool(bucket_recent, "label_complete_30s") - mean_bool(recent_rows, "label_complete_30s")
            score = lift_to_score(train_rate - baseline)
            if sign(full_lift) * sign(recent_lift) < 0:
                score = min(score, 0)
            buckets.append(
                {
                    "label": label,
                    "min": min_v,
                    "max": max_v,
                    "score": score,
                }
            )
        out[feature] = buckets
    return out


def fallback_feature_bucket_defs() -> dict[str, list[dict[str, Any]]]:
    return {
        "round_close_rel_s": [
            {"label": "le_15", "min": None, "max": 15.0, "score": 2},
            {"label": "15_60", "min": 15.0, "max": 60.0, "score": 2},
            {"label": "60_120", "min": 60.0, "max": 120.0, "score": 1},
            {"label": "120_180", "min": 120.0, "max": 180.0, "score": 0},
            {"label": "gt_180", "min": 180.0, "max": None, "score": 0},
        ],
        "prior_imbalance_value": [
            {"label": "lt_0.02", "min": None, "max": 0.02, "score": 1},
            {"label": "0.02_0.05", "min": 0.02, "max": 0.05, "score": 1},
            {"label": "0.05_0.08", "min": 0.05, "max": 0.08, "score": 0},
            {"label": "gt_0.08", "min": 0.08, "max": None, "score": 0},
        ],
        "l1_spread_ticks_first_side": [
            {"label": "le_1.5", "min": None, "max": 1.5, "score": 1},
            {"label": "1.5_3", "min": 1.5, "max": 3.0, "score": 0},
            {"label": "gt_3", "min": 3.0, "max": None, "score": 0},
        ],
        "l1_spread_ticks_opposite_side": [
            {"label": "le_1.5", "min": None, "max": 1.5, "score": 1},
            {"label": "1.5_3", "min": 1.5, "max": 3.0, "score": 0},
            {"label": "gt_3", "min": 3.0, "max": None, "score": 0},
        ],
        "mid_skew_to_opposite": [
            {"label": "near_zero", "min": -0.02, "max": 0.02, "score": 1},
            {"label": "mild", "min": -0.05, "max": 0.05, "score": 0},
            {"label": "extreme", "min": None, "max": None, "score": 0},
        ],
        "buy_fill_count_before_open": [
            {"label": "le_1", "min": None, "max": 1.0, "score": 1},
            {"label": "2_3", "min": 2.0, "max": 3.0, "score": 0},
            {"label": "gt_3", "min": 4.0, "max": None, "score": 0},
        ],
    }


def build_session_mults(recent_rows: list[dict[str, Any]]) -> list[float]:
    baseline = mean_bool(recent_rows, "label_complete_30s")
    if baseline <= 0.0:
        return [1.0] * 24
    hours = [1.0] * 24
    rows_by_hour: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in recent_rows:
        rows_by_hour[as_int(row.get("utc_hour_bucket"))].append(row)
    for hour in range(24):
        bucket = rows_by_hour.get(hour, [])
        if len(bucket) < 30:
            bucket = []
            for neighbor in ((hour - 1) % 24, hour, (hour + 1) % 24):
                bucket.extend(rows_by_hour.get(neighbor, []))
        if len(bucket) < 30:
            hours[hour] = 1.0
            continue
        ratio = mean_bool(bucket, "label_complete_30s") / baseline
        hours[hour] = max(0.50, min(1.25, ratio))
    return hours


def score_episode(
    row: dict[str, Any],
    feature_bucket_defs: dict[str, list[dict[str, Any]]],
    session_mults: list[float],
) -> dict[str, Any]:
    score = 0
    for feature, buckets in feature_bucket_defs.items():
        value = as_float(row.get(feature))
        for bucket in buckets:
            if bucket_matches(value, bucket.get("min"), bucket.get("max")):
                score += as_int(bucket.get("score"))
                break
    hour = as_int(row.get("utc_hour_bucket"))
    session_mult = session_mults[hour] if 0 <= hour < len(session_mults) else 1.0
    if score <= 0:
        clip_mult = 0.0
        score_bucket = "blocked"
    elif score <= 2:
        clip_mult = 0.5
        score_bucket = "half_clip"
    elif score <= 4:
        clip_mult = 1.0
        score_bucket = "full_clip"
    else:
        clip_mult = 1.25
        score_bucket = "upclip"
    hard_block = (
        as_int(row.get("same_side_run_before_open")) > 1
        or as_float(row.get("l1_spread_ticks_opposite_side")) > 3.0
        or as_int(row.get("book_age_ms")) > 500
        or session_mult < 0.75
    )
    allowed = not hard_block and clip_mult > 0
    return {
        "score": score,
        "score_bucket": score_bucket,
        "session_mult": session_mult,
        "allowed": allowed,
        "clip_mult": clip_mult,
    }


def attach_scores(
    rows: list[dict[str, Any]],
    feature_bucket_defs: dict[str, list[dict[str, Any]]],
    session_mults: list[float],
) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        enriched = dict(row)
        enriched.update(score_episode(enriched, feature_bucket_defs, session_mults))
        out.append(enriched)
    return out


def compute_window_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    makers = [row for row in rows if row.get("maker_taker_proxy") == "maker_proximity"]
    opposite_delays = [
        as_float(row.get("first_opposite_delay_s"))
        for row in rows
        if row.get("first_opposite_delay_s") is not None
    ]
    score_dist: dict[str, int] = defaultdict(int)
    session_dist: dict[str, int] = defaultdict(int)
    for row in rows:
        score_dist[str(row.get("score_bucket") or "unknown")] += 1
        session_dist[str(as_int(row.get("utc_hour_bucket")))] += 1
    gate_on = [row for row in rows if row.get("allowed")]
    baseline = mean_bool(rows, "label_complete_30s")
    gate_on_rate = mean_bool(gate_on, "label_complete_30s")
    return {
        "episode_count": len(rows),
        "completion_30s_hit_rate": baseline,
        "gate_on_completion_rate": gate_on_rate,
        "gate_on_lift_pct": gate_on_rate - baseline,
        "median_first_opposite_delay_s": median(opposite_delays) if opposite_delays else 0.0,
        "same_side_before_opposite_ratio": mean_bool(rows, "same_side_before_opposite"),
        "maker_proxy_ratio": len(makers) / len(rows) if rows else 0.0,
        "score_bucket_distribution": dict(sorted(score_dist.items())),
        "session_distribution": dict(sorted(session_dist.items())),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "slug",
        "episode_id",
        "first_leg_ts_ms",
        "round_open_rel_s",
        "round_close_rel_s",
        "utc_hour_bucket",
        "first_leg_side",
        "first_leg_clip",
        "prior_imbalance_bucket",
        "same_side_run_before_open",
        "l1_spread_ticks_first_side",
        "l1_spread_ticks_opposite_side",
        "mid_skew_to_opposite",
        "recent_opposite_trade_rate_5s",
        "recent_total_trade_rate_15s",
        "book_update_rate_5s",
        "maker_taker_proxy",
        "label_complete_30s",
        "label_clean_close_60s",
        "first_opposite_delay_s",
        "close_delay_s",
        "buy_fill_count_before_open",
        "score",
        "score_bucket",
        "session_mult",
        "allowed",
        "censored",
        "censored_reason",
        "overlap_source_date",
        "is_recent_overlap",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    trades = merged_trade_rows(args.trades, args.activity)
    asset_maps = build_asset_maps(trades)
    episodes = build_episodes(trades)
    db_paths = args.db or find_default_replay_dbs()
    sources = [load_replay_source(path) for path in db_paths]
    latest_source_date = max(source.date_label for source in sources)
    try:
        enriched = [
            enrich_episode_with_market_features(
                dict(episode), asset_maps, sources, latest_source_date, args.max_book_age_ms
            )
            for episode in episodes
        ]
    finally:
        for source in sources:
            source.conn.close()

    full_rows = [row for row in enriched if not row.get("censored")]
    recent_rows = [row for row in full_rows if row.get("is_recent_overlap")]
    if recent_rows:
        train_rows = [row for row in full_rows if not row.get("is_recent_overlap")]
        holdout_rows = recent_rows
        if not train_rows:
            split = max(1, int(len(full_rows) * 0.7))
            train_rows = full_rows[:split]
            holdout_rows = full_rows[split:]
    else:
        split = max(1, int(len(full_rows) * 0.7))
        train_rows = full_rows[:split]
        holdout_rows = full_rows[split:]
        recent_rows = holdout_rows

    feature_bucket_defs = build_feature_bucket_defs(train_rows, full_rows, recent_rows)
    fallback_applied = not any(
        as_int(bucket.get("score")) > 0
        for buckets in feature_bucket_defs.values()
        for bucket in buckets
    )
    if fallback_applied:
        feature_bucket_defs = fallback_feature_bucket_defs()
    session_mults = build_session_mults(recent_rows or full_rows)
    scored = attach_scores(enriched, feature_bucket_defs, session_mults)
    eligible_scored = [row for row in scored if not row.get("censored")]
    recent_scored = [row for row in eligible_scored if row.get("is_recent_overlap")]
    holdout_keys = {(row.get("slug"), row.get("episode_id")) for row in holdout_rows}
    holdout_scored = [
        row
        for row in scored
        if (row.get("slug"), row.get("episode_id")) in holdout_keys
    ]

    full_metrics = compute_window_metrics(eligible_scored)
    recent_metrics = compute_window_metrics(recent_scored or eligible_scored)
    holdout_metrics = compute_window_metrics(holdout_scored or recent_scored or eligible_scored)

    coverage_stats = {
        "full_episode_count": len(enriched),
        "full_overlap_episode_count": len(eligible_scored),
        "recent_episode_count": sum(1 for row in enriched if row.get("is_recent_overlap")),
        "recent_overlap_episode_count": len(recent_scored),
        "holdout_gate_on_completion_rate": holdout_metrics["gate_on_completion_rate"],
        "holdout_baseline_completion_rate": holdout_metrics["completion_30s_hit_rate"],
        "holdout_lift_pct": holdout_metrics["gate_on_lift_pct"],
    }
    provisional = coverage_stats["recent_overlap_episode_count"] < 300 or holdout_metrics["gate_on_lift_pct"] < 0.10

    defaults_payload = {
        "score_threshold_block": 0,
        "score_threshold_half_clip": 2,
        "score_threshold_full_clip": 4,
        "score_threshold_upclip": 5,
        "clip_mult_by_score_bucket": {
            "blocked": 0.0,
            "half_clip": 0.5,
            "full_clip": 1.0,
            "upclip": 1.25,
        },
        "session_mult_by_utc_hour": session_mults,
        "hard_block_rules": {
            "max_same_side_run_before_open": 1,
            "max_opposite_side_spread_ticks": 3.0,
            "max_book_age_ms": 500,
            "min_session_mult_for_open": 0.75,
            "min_recent_overlap_episodes": 30,
        },
        "feature_bucket_defs": {
            (
                "buy_fill_count"
                if feature == "buy_fill_count_before_open"
                else feature.replace("_value", "")
            ): buckets
            for feature, buckets in feature_bucket_defs.items()
        },
        "research_window": {
            "full_start_ts": min((as_int(row.get("first_leg_ts_ms")) // 1000 for row in enriched), default=0),
            "full_end_ts": max((as_int(row.get("first_leg_ts_ms")) // 1000 for row in enriched), default=0),
            "recent_start_ts": min((as_int(row.get("first_leg_ts_ms")) // 1000 for row in recent_scored), default=0),
            "recent_end_ts": max((as_int(row.get("first_leg_ts_ms")) // 1000 for row in recent_scored), default=0),
            "train_episode_count": len(train_rows),
            "holdout_episode_count": len(holdout_rows),
        },
        "coverage_stats": coverage_stats,
        "provisional": provisional,
    }
    summary_payload = {
        "coverage_stats": coverage_stats,
        "full_metrics": full_metrics,
        "recent_metrics": recent_metrics,
        "holdout_metrics": holdout_metrics,
        "xuan_targets": {
            "xuan_30s_completion_hit_rate": recent_metrics["completion_30s_hit_rate"],
            "xuan_median_first_opposite_delay_s": recent_metrics["median_first_opposite_delay_s"],
            "xuan_score_bucket_distribution": recent_metrics["score_bucket_distribution"],
            "xuan_session_distribution": recent_metrics["session_distribution"],
            "xuan_maker_proxy_ratio": recent_metrics["maker_proxy_ratio"],
        },
        "defaults_path": str(args.out_defaults_json),
        "fallback_applied": fallback_applied,
        "provisional": provisional,
    }

    args.out_defaults_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_defaults_json.write_text(
        json.dumps(defaults_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    args.out_summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_summary_json.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_csv(args.out_episodes_csv, scored)
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))
    print(f"wrote {args.out_defaults_json}")
    print(f"wrote {args.out_summary_json}")
    print(f"wrote {args.out_episodes_csv}")


if __name__ == "__main__":
    main()
