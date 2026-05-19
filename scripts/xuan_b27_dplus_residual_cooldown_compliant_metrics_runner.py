#!/usr/bin/env python3
"""Run D+ residual-cooldown probe metrics on compliant local inputs.

This runner is intentionally offline and read-only. It uses:

- strict/cache rows as the admission universe;
- completion_store rows for 30s completion evidence;
- public_audit rows for coverage/proxy-truth segmentation.

It does not scan raw replay paths, start network processes, or submit orders.
This is a companion to xuan_b27_dplus_compliant_metrics_runner.py. The main
runner keeps the broad metrics grid; this runner makes the residual-cooldown
probe reproducible under a declared strict/cache + completion + public-audit
path without replacing the main runner.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_residual_cooldown_compliant_metrics_runner"
DEFAULT_POLY_BT_ROOT = Path(os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data"))
DEFAULT_STRICT_ROOT = DEFAULT_POLY_BT_ROOT / "backtest_cache/taker_buy_signal_core_v2_strict_l1"
DEFAULT_COMPLETION_ROOT = DEFAULT_POLY_BT_ROOT / "verification_store/completion_unwind_event_store_v2"
DEFAULT_PUBLIC_AUDIT_DB = (
    DEFAULT_POLY_BT_ROOT
    / "verification_store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb"
)
STRICT_TABLE = "taker_buy_signal_candidates"
COMPLETION_TABLE = "completion_unwind_events"
PUBLIC_TABLE = "public_account_execution_events"
FORBIDDEN_DAYS = {"20260514", "20260515"}
NOT_READY_DAYS = {"20260518"}
FORBIDDEN_PATH_PARTS = {"raw", "replay_published", "poly-replay"}
DUST = 1e-9


@dataclass(frozen=True)
class Profile:
    name: str
    edge: float
    target_qty: float
    seed_px_lo: float
    seed_px_hi: float
    alignment: str = "all"
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 250.0
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 120.0
    seed_l1_pair_cap: float = 1.02
    cooldown_ms: int = 5_000
    imbalance_qty_cap: float = 1_000_000_000.0
    imbalance_cost_cap: float = 1_000_000_000.0
    dust_qty: float = 1.0
    residual_cooldown_age_s: float = 30.0
    residual_cooldown_cost_cap: float = 1_000_000_000.0
    pair_select: str = "fifo"


@dataclass
class Lot:
    qty: float
    px: float
    ts_ms: int
    side: str


@dataclass
class State:
    winner: str | None = None
    day: str | None = None
    slug: str | None = None
    inv: dict[str, deque[Lot]] | None = None
    last_seed_ts: int = -(10**18)
    last_ts_ms: int = 0
    active: bool = False

    def __post_init__(self) -> None:
        if self.inv is None:
            self.inv = {"YES": deque(), "NO": deque()}


PROBE_LOGIC_REFERENCES = {
    "profile_config": "Profile dataclass and build_profiles()",
    "admission": "maybe_seed()",
    "pairing": "pair_inventory() and choose_pair_indices()",
    "cooldown": "maybe_seed(): profile.cooldown_ms and residual_cooldown_* block",
    "residual_settlement": "settle()",
    "completion_store_evidence": "load_joined_candidates() completion_min_pair_cost_30s fields",
    "public_audit_segmentation": "load_public_audit_keys() and public_audit_* metrics",
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def day_token(day: str) -> str:
    return str(day).replace("-", "")


def dashed_day(day: str) -> str:
    clean = day_token(day)
    if len(clean) == 8 and clean.isdigit():
        return f"{clean[0:4]}-{clean[4:6]}-{clean[6:8]}"
    return str(day)


def sql_list(items: list[str]) -> str:
    return ", ".join("'" + item.replace("'", "''") + "'" for item in items)


def path_is_safe(path: Path) -> bool:
    text = str(path)
    if "/mnt/poly-replay" in text or "replay_published" in text:
        return False
    return not any(part in FORBIDDEN_PATH_PARTS for part in path.parts)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def label_days(label: str, manifest: dict[str, Any]) -> list[str]:
    days = manifest.get("days") or []
    if days:
        return sorted({dashed_day(str(day)) for day in days})
    parts = [part for part in label.replace("-", "_").split("_") if len(part) == 8 and part.isdigit()]
    return sorted({dashed_day(part) for part in parts})


def allowed_days(days: list[str]) -> bool:
    clean = {day_token(day) for day in days}
    return not bool(clean & FORBIDDEN_DAYS) and not bool(clean & NOT_READY_DAYS)


def discover_labels(root: Path, manifest_name: str) -> dict[str, list[str]]:
    labels: dict[str, list[str]] = {}
    if not root.exists():
        return labels
    for manifest_path in root.glob(f"*/{manifest_name}"):
        manifest = read_json(manifest_path)
        days = label_days(manifest_path.parent.name, manifest)
        if days and allowed_days(days) and path_is_safe(manifest_path.parent):
            labels[manifest_path.parent.name] = days
    return dict(sorted(labels.items()))


def split_labels(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def lot_qty(lots: deque[Lot]) -> float:
    return sum(lot.qty for lot in lots)


def lot_cost(lots: deque[Lot]) -> float:
    return sum(lot.qty * lot.px for lot in lots)


def aged_lot_cost(lots: deque[Lot], ts_ms: int, age_s: float) -> float:
    if age_s <= 0.0:
        return lot_cost(lots)
    cutoff_ms = age_s * 1000.0
    return sum(lot.qty * lot.px for lot in lots if ts_ms - lot.ts_ms >= cutoff_ms)


def pop_lot(lots: deque[Lot], idx: int) -> Lot:
    if idx == 0:
        return lots.popleft()
    lots.rotate(-idx)
    lot = lots.popleft()
    lots.rotate(idx)
    return lot


def choose_pair_indices(yes: deque[Lot], no: deque[Lot], mode: str) -> tuple[int, int]:
    if mode == "low_pair_cost":
        return (
            min(range(len(yes)), key=lambda idx: yes[idx].px),
            min(range(len(no)), key=lambda idx: no[idx].px),
        )
    if mode == "high_cost":
        return (
            max(range(len(yes)), key=lambda idx: yes[idx].px),
            max(range(len(no)), key=lambda idx: no[idx].px),
        )
    return 0, 0


def pair_inventory(profile: Profile, state: State, metrics: defaultdict[str, float], ts_ms: int) -> None:
    assert state.inv is not None
    yes = state.inv["YES"]
    no = state.inv["NO"]
    while yes and no:
        yes_idx, no_idx = choose_pair_indices(yes, no, profile.pair_select)
        a = yes[yes_idx]
        b = no[no_idx]
        take = min(a.qty, b.qty)
        if take <= DUST:
            break
        pair_cost = a.px + b.px
        older_ts = min(a.ts_ms, b.ts_ms)
        metrics["pair_actions"] += 1
        metrics["pair_qty"] += take
        metrics["pair_cost_sum"] += take * pair_cost
        metrics["pair_pnl"] += take * (1.0 - pair_cost)
        metrics["pair_delay_ms"] += take * max(0, ts_ms - older_ts)
        a.qty -= take
        b.qty -= take
        if a.qty <= DUST:
            pop_lot(yes, yes_idx)
        if b.qty <= DUST:
            pop_lot(no, no_idx)


def maybe_seed(row: dict[str, Any], profile: Profile, state: State, metrics: defaultdict[str, float]) -> None:
    side = str(row.get("first_side") or "")
    if side not in {"YES", "NO"}:
        return
    alignment = str(row.get("side_alignment") or "")
    if profile.alignment != "all" and alignment != profile.alignment:
        metrics["seed_block_alignment"] += 1
        return
    offset_s = float(row.get("offset_s") or 0.0)
    if not (profile.seed_offset_min_s <= offset_s < profile.seed_offset_max_s):
        metrics["seed_block_offset"] += 1
        return
    trade_px = float(row.get("public_trade_price") or math.nan)
    trade_size = float(row.get("public_trade_size") or 0.0)
    if trade_size <= DUST or not math.isfinite(trade_px):
        return
    if not (profile.seed_px_lo <= trade_px <= profile.seed_px_hi):
        metrics["seed_block_price_band"] += 1
        return
    l1_pair = row.get("strict_l1_immediate_pair")
    if l1_pair is None:
        l1_pair = row.get("l1_immediate_pair")
    l1_pair_f = float(l1_pair) if l1_pair is not None else math.nan
    if not math.isfinite(l1_pair_f) or l1_pair_f > profile.seed_l1_pair_cap + 1e-12:
        metrics["seed_block_l1_pair_cap"] += 1
        return
    ts_ms = int(row.get("trigger_ts_ms") or 0)
    if ts_ms - state.last_seed_ts < profile.cooldown_ms:
        metrics["seed_block_cooldown"] += 1
        return

    assert state.inv is not None
    same_qty = lot_qty(state.inv[side])
    opp_qty = lot_qty(state.inv[other(side)])
    aged_cost = aged_lot_cost(state.inv["YES"], ts_ms, profile.residual_cooldown_age_s) + aged_lot_cost(
        state.inv["NO"], ts_ms, profile.residual_cooldown_age_s
    )
    if aged_cost > profile.residual_cooldown_cost_cap + 1e-12 and same_qty + profile.dust_qty >= opp_qty:
        metrics["seed_block_residual_cooldown"] += 1
        return
    if same_qty >= profile.target_qty - profile.dust_qty:
        metrics["seed_block_target"] += 1
        return
    same_cost = lot_cost(state.inv[side])
    opp_cost = lot_cost(state.inv[other(side)])
    if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
        metrics["seed_block_imbalance_cost"] += 1
        return

    px = max(0.01, trade_px - profile.edge)
    open_cost = lot_cost(state.inv["YES"]) + lot_cost(state.inv["NO"])
    imbalance_room = profile.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= profile.dust_qty:
        metrics["seed_block_imbalance_qty"] += 1
        return
    qty = min(
        profile.max_seed_qty,
        trade_size * profile.fill_haircut,
        profile.target_qty - same_qty,
        (profile.max_open_cost - open_cost) / max(px, 1e-9),
        imbalance_room,
    )
    if qty <= profile.dust_qty:
        return

    state.inv[side].append(Lot(qty=qty, px=px, ts_ms=ts_ms, side=side))
    state.last_seed_ts = ts_ms
    state.last_ts_ms = max(state.last_ts_ms, ts_ms)
    state.active = True
    metrics["seed_actions"] += 1
    metrics["gross_buy_qty"] += qty
    metrics["gross_buy_cost"] += qty * px
    pair_inventory(profile, state, metrics, ts_ms)


def settle(states: dict[str, State], profile: Profile, metrics: defaultdict[str, float]) -> None:
    for condition_id, state in states.items():
        if not state.active:
            continue
        pair_inventory(profile, state, metrics, state.last_ts_ms)
        metrics["active_markets"] += 1
        winner = state.winner
        assert state.inv is not None
        for side in ("YES", "NO"):
            for lot in state.inv[side]:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                metrics["residual_qty"] += lot.qty
                metrics["residual_cost"] += cost
                metrics["residual_settle_payout"] += payout
                metrics["residual_settle_pnl"] += payout - cost
                if cost > 6.0:
                    metrics["resid_cost_gt6_markets"] += 1


def finish(profile: Profile, metrics: defaultdict[str, float]) -> dict[str, Any]:
    buy_cost = metrics["gross_buy_cost"]
    buy_qty = metrics["gross_buy_qty"]
    pair_qty = metrics["pair_qty"]
    active = metrics["active_markets"]
    actual_pnl = metrics["pair_pnl"] + metrics["residual_settle_pnl"]
    worst_residual_pnl = metrics["pair_pnl"] - metrics["residual_cost"]
    out = {
        **asdict(profile),
        "active_markets": int(active),
        "strict_candidate_rows": int(metrics["strict_candidate_rows"]),
        "completion_joined_candidates": int(metrics["completion_joined_candidates"]),
        "completion_pair_le_100_candidates": int(metrics["completion_pair_le_100_candidates"]),
        "public_audit_covered_candidates": int(metrics["public_audit_covered_candidates"]),
        "public_audit_covered_markets": int(metrics["public_audit_covered_markets"]),
        "seed_actions": int(metrics["seed_actions"]),
        "pair_actions": int(metrics["pair_actions"]),
        "gross_buy_qty": round(buy_qty, 6),
        "gross_buy_cost": round(buy_cost, 6),
        "pair_qty": round(pair_qty, 6),
        "pair_share_rate": round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6),
        "pair_cost_wavg": round((metrics["pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "pair_delay_wavg_s": round((metrics["pair_delay_ms"] / pair_qty / 1000.0) if pair_qty else 0.0, 6),
        "rounds_per_market": round((metrics["pair_actions"] / active) if active else 0.0, 6),
        "residual_qty": round(metrics["residual_qty"], 6),
        "residual_cost": round(metrics["residual_cost"], 6),
        "residual_settle_payout": round(metrics["residual_settle_payout"], 6),
        "residual_settle_pnl": round(metrics["residual_settle_pnl"], 6),
        "qty_residual_rate": round((metrics["residual_qty"] / buy_qty) if buy_qty else 0.0, 6),
        "cost_residual_rate": round((metrics["residual_cost"] / buy_cost) if buy_cost else 0.0, 6),
        "residual_cost_gt6_market_rate": round((metrics["resid_cost_gt6_markets"] / active) if active else 0.0, 6),
        "pair_pnl": round(metrics["pair_pnl"], 6),
        "actual_settle_pnl": round(actual_pnl, 6),
        "net_pnl": round(actual_pnl, 6),
        "net_roi": round((actual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "worst_residual_net_pnl": round(worst_residual_pnl, 6),
        "worst_residual_roi": round((worst_residual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "flat_residual_net_pnl": round(metrics["pair_pnl"], 6),
        "stress100_actual_pnl": round(actual_pnl - 0.01 * (2 * pair_qty + metrics["residual_qty"]), 6),
        "stress100_worst_pnl": round(worst_residual_pnl - 0.01 * (2 * pair_qty + metrics["residual_qty"]), 6),
        "seed_block_alignment": int(metrics["seed_block_alignment"]),
        "seed_block_offset": int(metrics["seed_block_offset"]),
        "seed_block_price_band": int(metrics["seed_block_price_band"]),
        "seed_block_l1_pair_cap": int(metrics["seed_block_l1_pair_cap"]),
        "seed_block_cooldown": int(metrics["seed_block_cooldown"]),
        "seed_block_target": int(metrics["seed_block_target"]),
        "seed_block_imbalance_qty": int(metrics["seed_block_imbalance_qty"]),
        "seed_block_imbalance_cost": int(metrics["seed_block_imbalance_cost"]),
        "seed_block_residual_cooldown": int(metrics["seed_block_residual_cooldown"]),
    }
    return out


def build_profiles(selected: list[str] | None = None) -> list[Profile]:
    caps = [0.5, 1.0, 1.5, 2.0, 1_000_000_000.0]
    profiles = []
    for cap in caps:
        suffix = "" if cap >= 1e8 else f"_rc30_{int(round(cap * 100)):03d}"
        profiles.append(
            Profile(
                name=f"dpass_all_e055_t5_px050_900_imb125{suffix}",
                edge=0.055,
                target_qty=5.0,
                seed_px_lo=0.05,
                seed_px_hi=0.90,
                alignment="all",
                imbalance_qty_cap=1.25,
                residual_cooldown_age_s=30.0,
                residual_cooldown_cost_cap=cap,
            )
        )
    if selected:
        wanted = set(selected)
        profiles = [profile for profile in profiles if profile.name in wanted]
        missing = wanted - {profile.name for profile in profiles}
        if missing:
            raise SystemExit(f"unknown profile(s): {', '.join(sorted(missing))}")
    return profiles


def profile_specs(profiles: list[Profile]) -> list[dict[str, Any]]:
    return [
        {
            **asdict(profile),
            "entry_rule": (
                "strict/cache candidate; first_side YES/NO; alignment all; "
                "0 <= offset_s < 120; public_trade_price in [0.05,0.90]; "
                "strict_l1_immediate_pair <= 1.02"
            ),
            "seed_rule": "qty=min(60, public_trade_size*0.25, target_qty-same_qty, open_cost_room/px, imbalance_room); px=max(0.01, public_trade_price-edge)",
            "pair_rule": "fifo internal YES/NO pair ledger; pair_pnl += qty*(1-yes_px-no_px)",
            "cooldown_rule": (
                "5s per-market seed cooldown; residual cooldown blocks same-side seed "
                "when aged residual cost over cap and same side is not below opposite side"
            ),
            "residual_rule": "settle residual lots by winner_side; also report worst_residual_net_pnl and stress100_worst_pnl",
            "function_refs": PROBE_LOGIC_REFERENCES,
        }
        for profile in profiles
    ]


def load_public_audit_keys(db_path: Path, days: list[str]) -> tuple[set[tuple[str, str]], dict[str, int], bool]:
    if not db_path.exists() or not path_is_safe(db_path):
        return set(), {}, False
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        days_sql = sql_list(days)
        rows = con.execute(
            f"""
            select day, condition_id, count(*)
            from {PUBLIC_TABLE}
            where day in ({days_sql})
            group by day, condition_id
            """
        ).fetchall()
    finally:
        con.close()
    keys = {(dashed_day(str(day)), str(condition_id)) for day, condition_id, _ in rows}
    day_counts: dict[str, int] = defaultdict(int)
    for day, _, count in rows:
        day_counts[dashed_day(str(day))] += int(count or 0)
    return keys, dict(day_counts), True


def load_joined_candidates(
    strict_db: Path,
    completion_db: Path,
    days: list[str],
    limit: int,
    completion_window_ms: int,
) -> list[dict[str, Any]]:
    con = duckdb.connect(":memory:")
    con.execute(f"attach '{strict_db}' as strict_db (read_only)")
    con.execute(f"attach '{completion_db}' as completion_db (read_only)")
    days_sql = sql_list(days)
    limit_sql = f"limit {int(limit)}" if limit > 0 else ""
    query = f"""
    with strict_candidates as (
      select
        row_number() over(order by trigger_ts_ms, condition_id, first_side) as candidate_id,
        day,
        slug,
        condition_id,
        winner_side,
        cast(trigger_ts_ms as bigint) as trigger_ts_ms,
        cast(offset_s as double) as offset_s,
        first_side,
        side_alignment,
        cast(public_trade_price as double) as public_trade_price,
        cast(public_trade_size as double) as public_trade_size,
        cast(first_l2_vwap as double) as first_l2_vwap,
        cast(l1_immediate_pair as double) as l1_immediate_pair,
        cast(strict_l1_immediate_pair as double) as strict_l1_immediate_pair
      from strict_db.{STRICT_TABLE}
      where day in ({days_sql})
      order by trigger_ts_ms, condition_id, first_side
      {limit_sql}
    ),
    joined as (
      select
        s.*,
        min(c.side_ask) filter (
          where c.side <> s.first_side
            and c.ts_ms between s.trigger_ts_ms and s.trigger_ts_ms + {int(completion_window_ms)}
        ) as completion_min_opp_ask_30s,
        min(c.ts_ms - s.trigger_ts_ms) filter (
          where c.side <> s.first_side
            and c.side_ask is not null
            and c.ts_ms between s.trigger_ts_ms and s.trigger_ts_ms + {int(completion_window_ms)}
        ) as completion_min_delay_ms,
        count(c.event_id) filter (
          where c.ts_ms between s.trigger_ts_ms and s.trigger_ts_ms + {int(completion_window_ms)}
        ) as completion_event_count_30s
      from strict_candidates s
      left join completion_db.{COMPLETION_TABLE} c
        on c.day = s.day
       and c.condition_id = s.condition_id
      group by
        s.candidate_id, s.day, s.slug, s.condition_id, s.winner_side, s.trigger_ts_ms, s.offset_s,
        s.first_side, s.side_alignment, s.public_trade_price, s.public_trade_size, s.first_l2_vwap,
        s.l1_immediate_pair, s.strict_l1_immediate_pair
    )
    select
      *,
      first_l2_vwap + completion_min_opp_ask_30s as completion_min_pair_cost_30s
    from joined
    order by trigger_ts_ms, condition_id, first_side
    """
    try:
        cols = [desc[0] for desc in con.execute(query).description]
        rows = [dict(zip(cols, row)) for row in con.fetchall()]
    finally:
        con.close()
    return rows


def run_metrics(
    rows: list[dict[str, Any]],
    profiles: list[Profile],
    public_keys: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    candidate_keys = {(dashed_day(str(row["day"])), str(row["condition_id"])) for row in rows}
    public_covered_markets = candidate_keys & public_keys
    for profile in profiles:
        states: dict[str, State] = {}
        metrics: defaultdict[str, float] = defaultdict(float)
        metrics["strict_candidate_rows"] = len(rows)
        metrics["public_audit_covered_markets"] = len(public_covered_markets)
        for row in rows:
            key = (dashed_day(str(row["day"])), str(row["condition_id"]))
            if key in public_keys:
                metrics["public_audit_covered_candidates"] += 1
            if row.get("completion_min_opp_ask_30s") is not None:
                metrics["completion_joined_candidates"] += 1
            pair_cost = row.get("completion_min_pair_cost_30s")
            if pair_cost is not None and float(pair_cost) <= 1.0:
                metrics["completion_pair_le_100_candidates"] += 1
            condition_id = str(row.get("condition_id") or "")
            if not condition_id:
                continue
            state = states.get(condition_id)
            if state is None:
                state = State(
                    winner=str(row.get("winner_side")) if row.get("winner_side") else None,
                    day=dashed_day(str(row.get("day") or "")),
                    slug=str(row.get("slug") or ""),
                )
                states[condition_id] = state
            elif state.winner is None and row.get("winner_side"):
                state.winner = str(row.get("winner_side"))
            state.last_ts_ms = max(state.last_ts_ms, int(row.get("trigger_ts_ms") or 0))
            maybe_seed(row, profile, state, metrics)
        settle(states, profile, metrics)
        results.append(finish(profile, metrics))
    results.sort(key=lambda row: (row["stress100_worst_pnl"], row["net_pnl"]), reverse=True)
    return results


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run D+ residual-cooldown metrics on compliant local inputs.")
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--strict-labels", default="", help="Comma-separated strict labels; default discovers allowed labels.")
    parser.add_argument("--completion-labels", default="", help="Comma-separated completion labels; default discovers allowed labels.")
    parser.add_argument("--profiles", default="", help="Comma-separated profile names; default runs the residual cooldown sweep.")
    parser.add_argument("--completion-window-ms", type=int, default=30_000)
    parser.add_argument("--max-strict-candidates-per-plan", type=int, default=0)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    strict_labels = discover_labels(strict_root, "CACHE_MANIFEST.json")
    completion_labels = discover_labels(completion_root, "EVENT_STORE_MANIFEST.json")
    if args.strict_labels:
        wanted = split_labels(args.strict_labels)
        strict_labels = {label: strict_labels[label] for label in wanted if label in strict_labels}
    if args.completion_labels:
        wanted = split_labels(args.completion_labels)
        completion_labels = {label: completion_labels[label] for label in wanted if label in completion_labels}
    profiles = build_profiles(split_labels(args.profiles) if args.profiles else None)

    plans: list[tuple[str, str, list[str]]] = []
    for strict_label, strict_days in strict_labels.items():
        for completion_label, completion_days in completion_labels.items():
            overlap = sorted(set(strict_days) & set(completion_days))
            if overlap:
                plans.append((strict_label, completion_label, overlap))
    all_days = sorted({day for _, _, days in plans for day in days})
    public_keys, public_day_counts, public_ready = load_public_audit_keys(public_audit_db, all_days)

    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    combined: list[dict[str, Any]] = []
    row_count = 0
    plan_manifests: list[dict[str, Any]] = []
    for idx, (strict_label, completion_label, days) in enumerate(plans, start=1):
        strict_db = strict_root / strict_label / "cache.duckdb"
        completion_db = completion_root / completion_label / "event_store.duckdb"
        if not strict_db.exists() or not completion_db.exists():
            continue
        rows = load_joined_candidates(
            strict_db,
            completion_db,
            days,
            args.max_strict_candidates_per_plan,
            args.completion_window_ms,
        )
        row_count += len(rows)
        results = run_metrics(rows, profiles, public_keys)
        for result in results:
            result.update({"strict_label": strict_label, "completion_label": completion_label, "days": days})
        combined.extend(results)
        per_path = output_dir / f"per_plan_{idx:02d}_{strict_label}_{completion_label}.json"
        per_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        plan_manifests.append(
            {
                "strict_label": strict_label,
                "completion_label": completion_label,
                "days": days,
                "strict_db": str(strict_db),
                "completion_db": str(completion_db),
                "strict_candidate_rows": len(rows),
                "result_path": str(per_path),
            }
        )

    # Aggregate across plans by summing additive outputs and recomputing derived ratios.
    aggregate_by_profile: dict[str, defaultdict[str, float]] = {}
    static_by_profile: dict[str, dict[str, Any]] = {}
    additive = {
        "active_markets",
        "strict_candidate_rows",
        "completion_joined_candidates",
        "completion_pair_le_100_candidates",
        "public_audit_covered_candidates",
        "public_audit_covered_markets",
        "seed_actions",
        "pair_actions",
        "gross_buy_qty",
        "gross_buy_cost",
        "pair_qty",
        "residual_qty",
        "residual_cost",
        "residual_settle_payout",
        "residual_settle_pnl",
        "pair_pnl",
        "seed_block_alignment",
        "seed_block_offset",
        "seed_block_price_band",
        "seed_block_l1_pair_cap",
        "seed_block_cooldown",
        "seed_block_target",
        "seed_block_imbalance_qty",
        "seed_block_imbalance_cost",
        "seed_block_residual_cooldown",
    }
    for row in combined:
        name = str(row["name"])
        aggregate_by_profile.setdefault(name, defaultdict(float))
        static_by_profile.setdefault(name, {k: row[k] for k in asdict(profiles[0]) if k in row})
        for key in additive:
            aggregate_by_profile[name][key] += float(row.get(key) or 0.0)
    aggregate_results: list[dict[str, Any]] = []
    for profile in profiles:
        metrics = aggregate_by_profile.get(profile.name, defaultdict(float))
        # Reuse finish-like derived metrics where possible. Some weighted fields
        # are not exactly recomputed from pair_cost_sum because per-plan reports
        # already round; keep them out of the aggregate to avoid false precision.
        buy_cost = metrics["gross_buy_cost"]
        buy_qty = metrics["gross_buy_qty"]
        pair_qty = metrics["pair_qty"]
        active = metrics["active_markets"]
        actual_pnl = metrics["pair_pnl"] + metrics["residual_settle_pnl"]
        worst_residual_pnl = metrics["pair_pnl"] - metrics["residual_cost"]
        aggregate_results.append(
            {
                **asdict(profile),
                "active_markets": int(active),
                "strict_candidate_rows": int(metrics["strict_candidate_rows"]),
                "completion_joined_candidates": int(metrics["completion_joined_candidates"]),
                "completion_pair_le_100_candidates": int(metrics["completion_pair_le_100_candidates"]),
                "public_audit_covered_candidates": int(metrics["public_audit_covered_candidates"]),
                "public_audit_covered_markets": int(metrics["public_audit_covered_markets"]),
                "seed_actions": int(metrics["seed_actions"]),
                "pair_actions": int(metrics["pair_actions"]),
                "gross_buy_qty": round(buy_qty, 6),
                "gross_buy_cost": round(buy_cost, 6),
                "pair_qty": round(pair_qty, 6),
                "pair_share_rate": round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6),
                "residual_qty": round(metrics["residual_qty"], 6),
                "residual_cost": round(metrics["residual_cost"], 6),
                "residual_settle_payout": round(metrics["residual_settle_payout"], 6),
                "residual_settle_pnl": round(metrics["residual_settle_pnl"], 6),
                "qty_residual_rate": round((metrics["residual_qty"] / buy_qty) if buy_qty else 0.0, 6),
                "cost_residual_rate": round((metrics["residual_cost"] / buy_cost) if buy_cost else 0.0, 6),
                "pair_pnl": round(metrics["pair_pnl"], 6),
                "actual_settle_pnl": round(actual_pnl, 6),
                "net_pnl": round(actual_pnl, 6),
                "net_roi": round((actual_pnl / buy_cost) if buy_cost else 0.0, 6),
                "worst_residual_net_pnl": round(worst_residual_pnl, 6),
                "worst_residual_roi": round((worst_residual_pnl / buy_cost) if buy_cost else 0.0, 6),
                "stress100_actual_pnl": round(actual_pnl - 0.01 * (2 * pair_qty + metrics["residual_qty"]), 6),
                "stress100_worst_pnl": round(worst_residual_pnl - 0.01 * (2 * pair_qty + metrics["residual_qty"]), 6),
                "seed_block_alignment": int(metrics["seed_block_alignment"]),
                "seed_block_offset": int(metrics["seed_block_offset"]),
                "seed_block_price_band": int(metrics["seed_block_price_band"]),
                "seed_block_l1_pair_cap": int(metrics["seed_block_l1_pair_cap"]),
                "seed_block_cooldown": int(metrics["seed_block_cooldown"]),
                "seed_block_target": int(metrics["seed_block_target"]),
                "seed_block_imbalance_qty": int(metrics["seed_block_imbalance_qty"]),
                "seed_block_imbalance_cost": int(metrics["seed_block_imbalance_cost"]),
                "seed_block_residual_cooldown": int(metrics["seed_block_residual_cooldown"]),
            }
        )
    aggregate_results.sort(key=lambda row: (row["stress100_worst_pnl"], row["net_pnl"]), reverse=True)

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "scope": "local_read_only_compliant_metrics_runner",
        "companion_to": "scripts/xuan_b27_dplus_compliant_metrics_runner.py",
        "status": "PASS_COMPLIANT_METRICS_RUNNER" if aggregate_results else "FAIL_COMPLIANT_METRICS_RUNNER_NO_ROWS",
        "data_root": str(DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_metrics",
        "strict_root": str(strict_root),
        "completion_root": str(completion_root),
        "public_audit_db": str(public_audit_db),
        "labels": {
            "strict": sorted(strict_labels),
            "completion": sorted(completion_labels),
            "public_audit": public_audit_db.parent.name if public_ready else None,
        },
        "days": all_days,
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": row_count,
        "profile_count": len(profiles),
        "profiles": profile_specs(profiles),
        "plans": plan_manifests,
        "public_audit_ready": public_ready,
        "public_audit_day_counts": public_day_counts,
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": public_ready,
        "public_account_truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "strict/cache admission plus completion-store metrics plus public-audit coverage segmentation; "
            "still not private queue truth or source-of-truth replay validation"
        ),
        "raw_replay_scanned": False,
        "duckdb_tables_read": True,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
        "outputs": {
            "combined_results_json": str(output_dir / "combined_results.json"),
            "combined_results_csv": str(output_dir / "combined_results.csv"),
        },
    }
    (output_dir / "combined_results.json").write_text(
        json.dumps(aggregate_results, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_csv(output_dir / "combined_results.csv", aggregate_results)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_dir / "manifest.json")
    return 0 if aggregate_results else 2


if __name__ == "__main__":
    raise SystemExit(main())
