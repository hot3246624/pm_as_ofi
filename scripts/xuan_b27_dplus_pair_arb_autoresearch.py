#!/usr/bin/env python3
"""Autoresearch-style multi-generation pair-arb backtest search.

This driver launches local `pair_arb_backtest` runs in iterative rounds,
keeps the best candidates from each generation, and mutates them to explore
neighbor points in the next generation.

Scope remains local/no-order:
- reads only explicit DB path passed by `--db`
- writes run artifacts under local xuan research artifacts
- does not touch live-order paths

The script intentionally optimizes a multi-objective score anchored on PnL,
pair cost, and residual risk, and exposes candidate details so we can track
"nagi-lite" frontier growth over generations.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from collections import defaultdict
import random
import shutil
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ARTIFACT = "xuan_b27_dplus_pair_arb_autoresearch"
DEFAULT_DB = "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db"
FIXED_TAKER_FEE_RATE: float | None = None
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
)
REPO_ROOT = Path(__file__).resolve().parents[1]
POLY_BT_ROOT = Path(os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data"))
DEFAULT_V1_INSTALL_MANIFEST = POLY_BT_ROOT / "derived/contract_examples/multiasset_backtest_v1_local_install_validation_latest.json"
DEFAULT_V1_STRATEGY_READINESS_MANIFEST = POLY_BT_ROOT / "derived/contract_examples/xuan_backtest_v1_strategy_readiness_latest/XUAN_BACKTEST_V1_STRATEGY_READINESS_GATE.json"
DEFAULT_V1_BTC_PARITY_MANIFEST = POLY_BT_ROOT / "derived/contract_examples/backtest_v1_btc_parity_latest/BACKTEST_V1_BTC_PARITY_GATE.json"
DEFAULT_GATE_PROFILE = REPO_ROOT / "configs/automation/xuan_b27_dplus_reproducibility_gate_profile.json"

DEFAULT_GATE_RULES: dict[str, Any] = {
    "search_safe_row_count_warn_ratio": 0.05,
    "search_safe_row_count_block_ratio": 0.10,
    "canonical_audit_selected_candidate_count_warn_ratio": 0.05,
    "canonical_audit_selected_candidate_count_block_ratio": 0.10,
    "max_fail_count": 0,
    "max_warn_count": 0,
}

RESEARCH_HARDLINE_FIELDS = {
    "private_truth_ready": False,
    "strategy_promotion_ready": False,
    "deployable": False,
    "live_orders_allowed": False,
    "candidate_import_allowed": False,
    "owner_private_truth_data_ready": False,
}

RESEARCH_MODE_OPTIONS = ("nagi", "ce25", "balanced")
DEFAULT_RESEARCH_MODE = "nagi"
DEFAULT_WALL_CLOCK_SECONDS = 3 * 3600
DEFAULT_SATURATION_PATIENCE_GENERATIONS = 2
DEFAULT_SATURATION_SCORE_DELTA = 1.0
DEFAULT_SATURATION_PNL_DELTA = 0.005

RESEARCH_SCORE_WEIGHTS = {
    "nagi": {
        "net_pnl": 1350.0,
        "pair_cost": 280.0,
        "pair_cost_clip": 2.0,
        "participation": 360.0,
        "fill_count": 16.0,
        "residual_qty": 75.0,
        "residual_loss_rate": 1100.0,
        "residual_loss_cost": 1600.0,
        "fee": 900.0,
    },
    "ce25": {
        "net_pnl": 1300.0,
        "pair_cost": 360.0,
        "pair_cost_clip": 2.0,
        "participation": 470.0,
        "fill_count": 22.0,
        "residual_qty": 48.0,
        "residual_loss_rate": 900.0,
        "residual_loss_cost": 1300.0,
        "fee": 900.0,
    },
    "balanced": {
        "net_pnl": 1320.0,
        "pair_cost": 320.0,
        "pair_cost_clip": 2.0,
        "participation": 410.0,
        "fill_count": 20.0,
        "residual_qty": 60.0,
        "residual_loss_rate": 1000.0,
        "residual_loss_cost": 1500.0,
        "fee": 900.0,
    },
}

RESEARCH_SEED_PROFILES: dict[str, list[dict[str, Any]]] = {
    "nagi": [
        {
            "max_net_diff": 0.08,
            "pair_target": 0.84,
            "bid_size": 0.06,
            "tier_1_mult": 0.55,
            "tier_2_mult": 0.14,
            "tier_mode": "disabled",
            "fill_model": "conservative",
            "risk_open_cutoff_secs": 210.0,
            "pair_cost_safety_margin": 0.01,
            "salvage_net_cap": 0.98,
            "salvage_start_remaining_secs": 220.0,
            "taker_fee_rate": 0.0,
            "directional_risk_filter_bps": 0.6,
            "directional_entry_min_bps": 0.0,
            "directional_price_source": "price",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.0,
            "reject_stale": True,
            "require_ws_fresh": True,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": True,
            "initial_balance": "",
        },
        {
            "max_net_diff": 0.16,
            "pair_target": 0.88,
            "bid_size": 0.10,
            "tier_1_mult": 0.45,
            "tier_2_mult": 0.10,
            "tier_mode": "continuous",
            "fill_model": "conservative",
            "risk_open_cutoff_secs": 270.0,
            "pair_cost_safety_margin": 0.00,
            "salvage_net_cap": 0.95,
            "salvage_start_remaining_secs": 260.0,
            "taker_fee_rate": 0.0,
            "directional_risk_filter_bps": 1.2,
            "directional_entry_min_bps": 0.15,
            "directional_price_source": "signal",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.06,
            "reject_stale": True,
            "require_ws_fresh": False,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": True,
            "initial_balance": "",
        },
        {
            "max_net_diff": 0.24,
            "pair_target": 0.90,
            "bid_size": 0.08,
            "tier_1_mult": 0.50,
            "tier_2_mult": 0.12,
            "tier_mode": "disabled",
            "fill_model": "aggressive",
            "risk_open_cutoff_secs": 300.0,
            "pair_cost_safety_margin": 0.00,
            "salvage_net_cap": 0.99,
            "salvage_start_remaining_secs": 240.0,
            "taker_fee_rate": 0.0,
            "directional_risk_filter_bps": 0.9,
            "directional_entry_min_bps": 0.25,
            "directional_price_source": "signal",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.08,
            "reject_stale": False,
            "require_ws_fresh": False,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": True,
            "initial_balance": "",
        },
    ],
    "ce25": [
        {
            "max_net_diff": 0.10,
            "pair_target": 0.75,
            "bid_size": 0.05,
            "tier_1_mult": 0.50,
            "tier_2_mult": 0.15,
            "tier_mode": "disabled",
            "fill_model": "conservative",
            "risk_open_cutoff_secs": 180.0,
            "pair_cost_safety_margin": 0.01,
            "salvage_net_cap": 0.95,
            "salvage_start_remaining_secs": 240.0,
            "taker_fee_rate": 0.0,
            "directional_risk_filter_bps": 0.0,
            "directional_entry_min_bps": 0.0,
            "directional_price_source": "price",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.0,
            "reject_stale": False,
            "require_ws_fresh": False,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": False,
            "initial_balance": "",
        },
        {
            "max_net_diff": 0.20,
            "pair_target": 0.80,
            "bid_size": 0.10,
            "tier_1_mult": 0.50,
            "tier_2_mult": 0.15,
            "tier_mode": "continuous",
            "fill_model": "conservative",
            "risk_open_cutoff_secs": 240.0,
            "pair_cost_safety_margin": 0.00,
            "salvage_net_cap": 0.90,
            "salvage_start_remaining_secs": 300.0,
            "taker_fee_rate": 0.07,
            "directional_risk_filter_bps": 0.0,
            "directional_entry_min_bps": 0.0,
            "directional_price_source": "price",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.0,
            "reject_stale": False,
            "require_ws_fresh": False,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": False,
            "initial_balance": "",
        },
        {
            "max_net_diff": 0.50,
            "pair_target": 0.85,
            "bid_size": 0.20,
            "tier_1_mult": 0.40,
            "tier_2_mult": 0.12,
            "tier_mode": "disabled",
            "fill_model": "conservative",
            "risk_open_cutoff_secs": 210.0,
            "pair_cost_safety_margin": 0.02,
            "salvage_net_cap": 1.00,
            "salvage_start_remaining_secs": 280.0,
            "taker_fee_rate": 0.00,
            "directional_risk_filter_bps": 0.0,
            "directional_entry_min_bps": 0.0,
            "directional_price_source": "price",
            "max_quote_age_secs": 0.0,
            "min_ask_depth": 0.0,
            "entry_pair_max_ask_sum": 0.0,
            "reject_stale": False,
            "require_ws_fresh": False,
            "require_two_sided_entry": False,
            "pairing_only_when_residual": False,
            "initial_balance": "",
        },
    ],
}

REPLAY_MARKET_TICKS_SCHEMA = {
    "ts",
    "remaining_sec",
    "btc_open",
    "btc_price",
    "btc_price_signal",
    "btc_price_binance",
    "ask_up",
    "bid_up",
    "ask_down",
    "bid_down",
    "ask_depth_L1_up",
    "ask_depth_L1_down",
    "ws_fresh",
    "quote_age_up",
    "quote_age_down",
    "is_stale",
}
REPLAY_MARKET_META_REQUIRED = {"slug", "payload_json"}
REPLAY_MD_BOOK_L1_REQUIRED = {"slug", "recv_unix_ms", "side", "bid", "ask"}
REPLAY_SETTLEMENT_RECORDS_ROWWISE_REQUIRED = {"slug", "event", "payload_json"}
REPLAY_SETTLEMENT_RECORDS_LEGACY_REQUIRED = {"condition_id", "outcome"}
REPLAY_DUCKDB_V2_META_REQUIRED = {"condition_id", "slug", "end_ms"}
REPLAY_DUCKDB_V2_BOOK_REQUIRED = {
    "condition_id",
    "recv_ms",
    "yes_bid_px",
    "yes_ask_px",
    "no_bid_px",
    "no_ask_px",
    "yes_bid_sz",
    "yes_ask_sz",
    "no_bid_sz",
    "no_ask_sz",
}
REPLAY_DUCKDB_V2_SETTLEMENT_REQUIRED = {
    "condition_id",
    "official_outcome",
    "winner_side",
    "settle_ms",
}


@dataclass(frozen=True)
class Candidate:
    max_net_diff: float
    pair_target: float
    bid_size: float
    tier_1_mult: float
    tier_2_mult: float
    tier_mode: str
    fill_model: str
    risk_open_cutoff_secs: float
    pair_cost_safety_margin: float
    salvage_net_cap: float
    salvage_start_remaining_secs: float
    taker_fee_rate: float
    directional_risk_filter_bps: float
    directional_entry_min_bps: float
    directional_price_source: str
    max_quote_age_secs: float
    min_ask_depth: float
    entry_pair_max_ask_sum: float
    reject_stale: bool
    require_ws_fresh: bool
    require_two_sided_entry: bool
    pairing_only_when_residual: bool
    initial_balance: str


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return 1 if value else 0
        return int(value)
    except (TypeError, ValueError):
        return default


def as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return str(value)


def as_path(value: Any) -> Path | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    return Path(str(value))


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "t", "yes", "y"}:
            return True
        if v in {"0", "false", "f", "no", "n"}:
            return False
    return False


def read_json(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists():
        return None
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    return raw if isinstance(raw, dict) else None


def file_sha256(path: Path | None) -> str | None:
    if not path or not path.exists():
        return None
    try:
        digest = hashlib.sha256()
        with path.open("rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def manifest_signature(payload: dict[str, Any]) -> str:
    normalized = dict(payload)
    for key in [
        "created_utc",
        "created_at_utc",
        "generated_at_utc",
        "started_at_utc",
        "finished_at_utc",
        "report_time_utc",
    ]:
        normalized.pop(key, None)
    return hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def parse_json_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, dict) else None


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    return table in {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}


def normalize_side(value: Any) -> str:
    return str(value or "").strip().upper()


def parse_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_outcome(payload: dict[str, Any] | None, event: str | None = None) -> str | None:
    if not payload:
        return None

    def _as_scalar(value: Any) -> Any | None:
        if isinstance(value, (list, tuple)):
            return _as_scalar(value[0]) if value else None
        if isinstance(value, dict):
            for key in (
                "winner_side",
                "winner",
                "outcome",
                "result",
                "result_side",
                "direction",
                "final",
                "resolved_winner_side",
                "resolved",
            ):
                nested = value.get(key)
                if nested is not None:
                    nested_scalar = _as_scalar(nested)
                    if nested_scalar is not None:
                        return nested_scalar
            return None
        return value

    candidate: Any | None = None
    for key in (
        "winner_side",
        "winner",
        "outcome",
        "result",
        "result_side",
        "direction",
        "final",
        "resolved_winner_side",
        "resolved",
    ):
        if key in payload:
            candidate = _as_scalar(payload[key])
            if candidate is not None:
                break
    if candidate is None and event:
        evt = str(event).strip().lower()
        if evt == "market_resolved":
            candidate = _as_scalar(payload.get("resolved_winner_side"))
        elif evt == "redeem_requested":
            candidate = _as_scalar(payload.get("resolved_winner_side"))
        elif evt == "redeem_result":
            candidate = _as_scalar(payload.get("winner_side"))
        elif evt == "taker_repair_sent":
            candidate = _as_scalar(payload.get("side"))
    if not candidate:
        return None
    value = str(candidate).strip().upper()
    if value in {"YES", "UP", "RIGHT", "1", "TRUE"}:
        return "UP"
    if value in {"NO", "DOWN", "LEFT", "0", "FALSE"}:
        return "DOWN"
    return None


def detect_backtest_db_schema(conn: sqlite3.Connection) -> str:
    cols = set(table_columns(conn, "market_ticks"))
    if {"market_ticks", "settlement_records"} <= {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}:
        if REPLAY_MARKET_TICKS_SCHEMA.issubset(set(cols)):
            return "legacy_market_ticks"

    if has_table(conn, "md_book_l1") and not has_table(conn, "market_ticks"):
        mbl_cols = set(table_columns(conn, "md_book_l1"))
        if REPLAY_MD_BOOK_L1_REQUIRED.issubset(mbl_cols):
            if has_table(conn, "market_meta") and has_table(conn, "settlement_records"):
                meta_cols = set(table_columns(conn, "market_meta"))
                settle_cols = set(table_columns(conn, "settlement_records"))
                if REPLAY_MARKET_META_REQUIRED.issubset(meta_cols) and (
                    REPLAY_SETTLEMENT_RECORDS_ROWWISE_REQUIRED.issubset(settle_cols)
                    or REPLAY_SETTLEMENT_RECORDS_LEGACY_REQUIRED.issubset(settle_cols)
                ):
                    return "replay_rowwise_ticks"
    return "unsupported"




def to_epoch_seconds(value: Any) -> int | None:
    v = parse_int(value)
    if v is None:
        return None
    if v >= 10_000_000_000:
        return v // 1000
    if v < 0:
        return None
    return v


def resolve_research_mode(value: str | None) -> str:
    mode = (value or "").strip().lower()
    if mode in RESEARCH_MODE_OPTIONS:
        return mode
    return DEFAULT_RESEARCH_MODE


def infer_round_duration_seconds(slug: str) -> int:
    if "-15m-" in slug:
        return 900
    if "-1h-" in slug or "-60m-" in slug:
        return 3600
    if "-4h-" in slug:
        return 14_400
    return 300


def infer_round_end_ts(slug: str, fallback: int | None = None) -> int | None:
    if fallback is not None:
        return fallback
    try:
        start_ts = int(slug.rsplit("-", 1)[1])
    except Exception:
        return None
    if start_ts <= 0:
        return None
    return start_ts + infer_round_duration_seconds(slug)


def infer_settlement_outcome_from_book(
    conn: sqlite3.Connection,
    slug: str | None,
    ts_end: int | None,
    recv_col: str,
    bid_col: str,
    ask_col: str,
) -> str | None:
    if not slug or ts_end is None:
        return None
    ts_ms = ts_end * 1000 if ts_end <= 10_000_000_000 else ts_end
    query = (
        f"SELECT side, {bid_col}, {ask_col} "
        f"FROM md_book_l1 WHERE slug = ? AND {recv_col} <= ? ORDER BY {recv_col} DESC LIMIT 200"
    )
    latest: dict[str, tuple[float, float]] = {}
    for side_raw, bid, ask in conn.execute(query, (slug, ts_ms)).fetchall():
        side = normalize_side(side_raw)
        if side == "UP":
            side = "YES"
        elif side == "DOWN":
            side = "NO"
        if side not in {"YES", "NO"}:
            continue
        if bid is None or ask is None:
            continue
        if side not in latest:
            latest[side] = (float(bid), float(ask))
        if len(latest) >= 2:
            break

    if "YES" not in latest or "NO" not in latest:
        return None
    yes_bid, yes_ask = latest["YES"]
    no_bid, no_ask = latest["NO"]
    if yes_bid <= 0 or yes_ask <= 0 or no_bid <= 0 or no_ask <= 0:
        return None
    yes_mid = (yes_bid + yes_ask) / 2.0
    no_mid = (no_bid + no_ask) / 2.0
    if yes_mid == no_mid:
        return None
    return "UP" if yes_mid > no_mid else "DOWN"


def infer_settlement_outcome_from_duckdb_book(
    conn, 
    condition_id: str | None,
    ts_end: int | None,
    recv_col: str,
    yes_bid_col: str,
    yes_ask_col: str,
    no_bid_col: str,
    no_ask_col: str,
) -> str | None:
    if not condition_id or ts_end is None:
        return None
    ts_ms = ts_end * 1000 if ts_end <= 10_000_000_000 else ts_end
    query = (
        f"SELECT {yes_bid_col}, {yes_ask_col}, {no_bid_col}, {no_ask_col} "
        f"FROM md_book_l1 WHERE condition_id = ? AND {recv_col} <= ? ORDER BY {recv_col} DESC LIMIT 200"
    )
    latest_yes = None
    latest_no = None
    for yes_bid, yes_ask, no_bid, no_ask in conn.execute(query, (condition_id, ts_ms)).fetchall():
        if latest_yes is None and yes_bid is not None and yes_ask is not None:
            latest_yes = (float(yes_bid), float(yes_ask))
        if latest_no is None and no_bid is not None and no_ask is not None:
            latest_no = (float(no_bid), float(no_ask))
        if latest_yes is not None and latest_no is not None:
            break

    if latest_yes is None or latest_no is None:
        return None

    yes_bid, yes_ask = latest_yes
    no_bid, no_ask = latest_no
    if yes_bid <= 0 or yes_ask <= 0 or no_bid <= 0 or no_ask <= 0:
        return None
    yes_mid = (yes_bid + yes_ask) / 2.0
    no_mid = (no_bid + no_ask) / 2.0
    if yes_mid == no_mid:
        return None
    return "UP" if yes_mid > no_mid else "DOWN"


def detect_duckdb_source_schema(conn) -> str:
    try:
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    except Exception:
        return "unsupported"
    tables = {row[0] for row in table_rows}
    if not {"market_meta", "md_book_l1", "settlement_records"} <= tables:
        return "unsupported"

    def _table_columns(name: str) -> set[str]:
        try:
            return {row[1] for row in conn.execute(f"PRAGMA table_info({name})").fetchall()}
        except Exception:
            return set()

    meta_cols = _table_columns("market_meta")
    book_cols = _table_columns("md_book_l1")
    settle_cols = _table_columns("settlement_records")
    if (
        REPLAY_DUCKDB_V2_META_REQUIRED.issubset(meta_cols)
        and REPLAY_DUCKDB_V2_BOOK_REQUIRED.issubset(book_cols)
        and REPLAY_DUCKDB_V2_SETTLEMENT_REQUIRED.issubset(settle_cols)
    ):
        return "replay_store_v2"
    return "unsupported"


def _as_float_local(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, AttributeError):
        return default


def materialize_duckdb_replay_store_v2_market_ticks(
    source_db: Path,
    target_db: Path,
    infer_settlement_outcome: bool = False,
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "source_db": str(source_db),
        "target_db": str(target_db),
        "status": "started",
    }

    settlement_rows: list[tuple[str, str, int]] = []
    settlement_seen: set[tuple[str, str, int]] = set()
    market_ticks_insert_batch: list[tuple[Any, ...]] = []

    missing_condition_count = 0
    missing_settlement_outcome_count = 0
    missing_settlement_time_count = 0
    inferred_settlement_outcome_count = 0
    total_settlement_rows = 0

    total_md_book_rows = 0
    market_tick_rows = 0
    missing_md_book_condition_count = 0
    market_tick_bucket_count = 0
    market_tick_skipped_no_pair_count = 0
    market_tick_bucket_no_round_end_count = 0

    try:
        import duckdb as _duckdb
    except Exception as exc:
        return {
            "status": "source_dependency_missing",
            "source_db": str(source_db),
            "error": f"duckdb module missing: {exc}",
        }

    try:
        with _duckdb.connect(str(source_db), read_only=True) as source_conn:
            schema_kind = detect_duckdb_source_schema(source_conn)
            if schema_kind != "replay_store_v2":
                return {
                    "status": "unsupported",
                    "schema": schema_kind,
                    "source_db": str(source_db),
                    "error": "unsupported duckdb replay schema",
                }

            market_meta_rows = source_conn.execute(
                "SELECT condition_id, slug, end_ms FROM market_meta WHERE condition_id IS NOT NULL AND slug IS NOT NULL"
            ).fetchall()
            if not market_meta_rows:
                return {
                    "status": "unsupported",
                    "source_db": str(source_db),
                    "error": "market_meta empty or missing condition_id/slug",
                }

            condition_meta: dict[str, dict[str, int | str | None]] = {}
            for condition_id, slug, end_ms in market_meta_rows:
                if condition_id is None or slug is None:
                    continue
                condition_meta[str(condition_id)] = {
                    "slug": str(slug),
                    "round_end_ts": to_epoch_seconds(end_ms),
                }

            settlement_cols = {
                row[1]
                for row in source_conn.execute("PRAGMA table_info(settlement_records)").fetchall()
            }
            settlement_select_cols: list[str] = ["condition_id", "settle_ms"]
            if "official_outcome" in settlement_cols:
                settlement_select_cols.append("official_outcome")
            if "winner_side" in settlement_cols:
                settlement_select_cols.append("winner_side")
            if "payload_json" in settlement_cols:
                settlement_select_cols.append("payload_json")
            if "event" in settlement_cols:
                settlement_select_cols.append("event")
            if "recv_ms" in settlement_cols:
                settlement_select_cols.append("recv_ms")
            if "time" in settlement_cols:
                settlement_select_cols.append("time")

            settlement_rows_cursor = source_conn.execute(
                f"SELECT {', '.join(settlement_select_cols)} "
                "FROM settlement_records ORDER BY settle_ms NULLS LAST, condition_id"
            )
            settlement_col_idx = {name: idx for idx, name in enumerate(settlement_select_cols)}

            def _s(name: str):
                idx = settlement_col_idx.get(name)
                if idx is None:
                    return None
                return row[idx]

            while True:
                settlement_rows_batch = settlement_rows_cursor.fetchmany(20000)
                if not settlement_rows_batch:
                    break
                for row in settlement_rows_batch:
                    total_settlement_rows += 1
                    condition_id_raw = _s("condition_id")
                    if condition_id_raw is None:
                        missing_condition_count += 1
                        continue
                    condition_id = str(condition_id_raw)
                    meta = condition_meta.get(condition_id)
                    if not meta:
                        missing_condition_count += 1
                        continue

                    ts_end = to_epoch_seconds(_s("settle_ms"))
                    if ts_end is None and _s("time") is not None:
                        ts_end = to_epoch_seconds(_s("time"))
                    if ts_end is None and _s("recv_ms") is not None:
                        ts_end = to_epoch_seconds(_s("recv_ms"))
                    if ts_end is None:
                        ts_end = to_epoch_seconds(meta.get("round_end_ts"))
                    if ts_end is None:
                        missing_settlement_time_count += 1
                        continue

                    payload = parse_json_payload(_s("payload_json")) if _s("payload_json") else None
                    event = str(_s("event")) if _s("event") else None
                    outcome = parse_outcome(payload, event)
                    if outcome not in {"UP", "DOWN"}:
                        if _s("official_outcome") is not None:
                            outcome = parse_outcome({"outcome": _s("official_outcome")}, event)
                    if outcome not in {"UP", "DOWN"}:
                        if _s("winner_side") is not None:
                            outcome = parse_outcome({"outcome": _s("winner_side")}, event)

                    if outcome not in {"UP", "DOWN"} and infer_settlement_outcome:
                        inferred = infer_settlement_outcome_from_duckdb_book(
                            source_conn,
                            condition_id,
                            ts_end,
                            "recv_ms",
                            "yes_bid_px",
                            "yes_ask_px",
                            "no_bid_px",
                            "no_ask_px",
                        )
                        if inferred is not None:
                            outcome = inferred
                            inferred_settlement_outcome_count += 1

                    if outcome not in {"UP", "DOWN"}:
                        missing_settlement_outcome_count += 1
                        continue

                    key = (condition_id, outcome, int(ts_end))
                    if key in settlement_seen:
                        continue
                    settlement_seen.add(key)
                    settlement_rows.append(key)

            target_conn = sqlite3.connect(str(target_db))
            with target_conn:
                target_conn.execute("DROP TABLE IF EXISTS market_ticks")
                target_conn.execute("DROP TABLE IF EXISTS settlement_records")
                target_conn.execute(
                    """
                    CREATE TABLE market_ticks (
                        ts INTEGER NOT NULL,
                        remaining_sec REAL NOT NULL,
                        btc_open REAL,
                        btc_price REAL,
                        btc_price_signal REAL,
                        btc_price_binance REAL,
                        ask_up REAL,
                        bid_up REAL,
                        ask_down REAL,
                        bid_down REAL,
                        ask_depth_L1_up REAL,
                        ask_depth_L1_down REAL,
                        ws_fresh INTEGER,
                        quote_age_up REAL,
                        quote_age_down REAL,
                        is_stale INTEGER,
                        condition_id TEXT NOT NULL
                    )
                    """
                )
                target_conn.execute(
                    """
                    CREATE TABLE settlement_records (
                        condition_id TEXT NOT NULL,
                        outcome TEXT NOT NULL,
                        ts_end INTEGER NOT NULL
                    )
                    """
                )

                if settlement_rows:
                    target_conn.executemany(
                        "INSERT INTO settlement_records(condition_id, outcome, ts_end) VALUES (?,?,?)",
                        settlement_rows,
                    )

                md_book_cursor = source_conn.execute(
                    "SELECT "
                    "condition_id, recv_ms, "
                    "yes_bid_px, yes_ask_px, no_bid_px, no_ask_px, "
                    "yes_bid_sz, yes_ask_sz, no_bid_sz, no_ask_sz, "
                    "source_kind "
                    "FROM md_book_l1 "
                    "WHERE condition_id IS NOT NULL AND recv_ms IS NOT NULL "
                    "ORDER BY condition_id, recv_ms"
                )
                while True:
                    md_book_batch = md_book_cursor.fetchmany(50000)
                    if not md_book_batch:
                        break
                    for row in md_book_batch:
                        total_md_book_rows += 1
                        condition_id_raw = row[0]
                        if condition_id_raw is None:
                            continue
                        condition_id = str(condition_id_raw)
                        meta = condition_meta.get(condition_id)
                        if not meta:
                            missing_md_book_condition_count += 1
                            continue

                        ts_sec = to_epoch_seconds(row[1])
                        if ts_sec is None:
                            continue

                        yes_bid = _as_float_local(row[2], 0.0)
                        yes_ask = _as_float_local(row[3], 0.0)
                        no_bid = _as_float_local(row[4], 0.0)
                        no_ask = _as_float_local(row[5], 0.0)
                        if yes_bid <= 0.0 or yes_ask <= 0.0 or no_bid <= 0.0 or no_ask <= 0.0:
                            market_tick_skipped_no_pair_count += 1
                            continue

                        yes_ask_sz = _as_float_local(row[7], 0.0)
                        no_bid_sz = _as_float_local(row[8], 0.0)

                        round_end_ts = to_epoch_seconds(meta.get("round_end_ts"))
                        if round_end_ts is None:
                            market_tick_bucket_no_round_end_count += 1
                            continue

                        remaining_sec = float(round_end_ts - ts_sec)
                        if remaining_sec < 0:
                            remaining_sec = 0.0

                        market_tick_rows += 1
                        market_tick_bucket_count += 1
                        market_ticks_insert_batch.append(
                            (
                                ts_sec,
                                remaining_sec,
                                0.0,
                                0.0,
                                0.0,
                                0.0,
                                yes_ask,
                                yes_bid,
                                no_ask,
                                no_bid,
                                yes_ask_sz,
                                no_bid_sz,
                                1 if row[10] else 0,
                                0.0,
                                0.0,
                                0,
                                condition_id,
                            )
                        )

                        if len(market_ticks_insert_batch) >= 5000:
                            target_conn.executemany(
                                "INSERT INTO market_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                market_ticks_insert_batch,
                            )
                            market_ticks_insert_batch.clear()

                if market_ticks_insert_batch:
                    target_conn.executemany(
                        "INSERT INTO market_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        market_ticks_insert_batch,
                    )
                    market_ticks_insert_batch.clear()

                target_conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_market_ticks_condition_ts ON market_ticks(condition_id, ts)"
                )
                target_conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_settlement_condition_ts_end ON settlement_records(condition_id, ts_end)"
                )

            info.update(
                {
                    "status": "ok",
                    "market_tick_rows": market_tick_rows,
                    "settlement_rows": len(settlement_rows),
                    "missing_settlement_outcome_count": missing_settlement_outcome_count,
                    "missing_condition_count": missing_condition_count,
                    "missing_settlement_time_count": missing_settlement_time_count,
                    "inferred_settlement_outcome_count": inferred_settlement_outcome_count,
                    "market_tick_bucket_count": market_tick_bucket_count,
                    "market_tick_skipped_no_pair_count": market_tick_skipped_no_pair_count,
                    "total_md_book_rows": total_md_book_rows,
                    "missing_md_book_condition_count": missing_md_book_condition_count,
                    "missing_md_book_round_end_count": market_tick_bucket_no_round_end_count,
                    "total_settlement_rows": total_settlement_rows,
                    "schema": schema_kind,
                    "market_meta_condition_count": len(condition_meta),
                    "settlement_coverage_ratio": (
                        len(settlement_rows) / total_settlement_rows if total_settlement_rows else 0.0
                    ),
                    "round_end_not_ready_count": market_tick_bucket_no_round_end_count,
                }
            )
    except Exception as exc:
        info.update({"status": "failed", "error": str(exc)})

    return info


def build_slug_round_index(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    slug_meta: dict[str, dict[str, Any]] = {}
    for slug, condition_id, payload_json in conn.execute(
        "SELECT slug, condition_id, payload_json FROM market_meta WHERE slug IS NOT NULL"
    ).fetchall():
        if slug is None:
            continue
        slug_key = str(slug)
        entry = slug_meta.setdefault(slug_key, {"condition_id": None, "round_end_ts": None})
        if condition_id is not None:
            entry["condition_id"] = str(condition_id)

        payload = parse_json_payload(payload_json)
        if not payload:
            continue

        round_end = parse_int(
            payload.get("round_end_ts") or payload.get("round_end") or payload.get("end_time")
        )
        if round_end is not None:
            entry["round_end_ts"] = round_end
        if not entry["condition_id"]:
            nested_condition = (
                payload.get("condition_id")
                or payload.get("market_id")
                or payload.get("market")
                or payload.get("id")
            )
            if nested_condition is not None:
                entry["condition_id"] = str(nested_condition)

    for slug, entry in slug_meta.items():
        entry["round_end_ts"] = infer_round_end_ts(slug, entry.get("round_end_ts"))

    return slug_meta


def materialize_legacy_market_ticks(
    source_db: Path,
    target_db: Path,
    infer_settlement_outcome: bool = False,
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "source_db": str(source_db),
        "target_db": str(target_db),
        "status": "started",
    }

    market_ticks_insert_batch: list[tuple[Any, ...]] = []
    settlement_rows: list[tuple[str, str, int]] = []
    settlement_seen: set[tuple[str, str, int]] = set()

    missing_condition_count = 0
    missing_settlement_outcome_count = 0
    missing_settlement_time_count = 0
    inferred_settlement_outcome_count = 0
    market_tick_skipped_no_pair_count = 0
    market_tick_bucket_count = 0
    total_md_book_rows = 0
    missing_md_book_condition_count = 0
    missing_md_book_round_end_count = 0
    total_settlement_rows = 0
    missing_round_end_count = 0

    try:
        with sqlite3.connect(str(source_db)) as source_conn, sqlite3.connect(str(target_db)) as target_conn:
            source_conn.row_factory = sqlite3.Row
            settle_cols = set(table_columns(source_conn, "settlement_records"))
            mbl_cols = set(table_columns(source_conn, "md_book_l1"))
            has_rowwise_settlement = REPLAY_SETTLEMENT_RECORDS_ROWWISE_REQUIRED.issubset(settle_cols)
            has_legacy_settlement = REPLAY_SETTLEMENT_RECORDS_LEGACY_REQUIRED.issubset(settle_cols)

            if not has_legacy_settlement and not has_rowwise_settlement:
                return {
                    "status": "unsupported_settlement_schema",
                    "source_db": str(source_db),
                    "settlement_columns": sorted(settle_cols),
                }
            if "slug" not in mbl_cols or "side" not in mbl_cols:
                return {
                    "status": "unsupported_md_book_schema",
                    "source_db": str(source_db),
                    "md_book_columns": sorted(mbl_cols),
                }

            ask_col = "ask" if "ask" in mbl_cols else "ask_price"
            bid_col = "bid" if "bid" in mbl_cols else "bid_price"
            ask_sz_col = "ask_sz" if "ask_sz" in mbl_cols else ("ask_size" if "ask_size" in mbl_cols else "ask")
            bid_sz_col = "bid_sz" if "bid_sz" in mbl_cols else ("bid_size" if "bid_size" in mbl_cols else "bid")
            recv_book_col = "recv_unix_ms" if "recv_unix_ms" in mbl_cols else ("recv_ms" if "recv_ms" in mbl_cols else None)
            source_col = "source" if "source" in mbl_cols else None
            recv_settlement_col = "recv_unix_ms" if "recv_unix_ms" in settle_cols else ("recv_ms" if "recv_ms" in settle_cols else None)
            if recv_book_col is None:
                return {
                    "status": "unsupported_md_book_schema",
                    "source_db": str(source_db),
                    "md_book_columns": sorted(mbl_cols),
                }

            slug_meta = build_slug_round_index(source_conn)
            condition_to_round_end: dict[str, int | None] = {
                str(meta.get("condition_id")): meta.get("round_end_ts")
                for meta in slug_meta.values()
                if meta.get("condition_id")
            }

            settlement_select_cols = []
            if "slug" in settle_cols:
                settlement_select_cols.append("slug")
            if "event" in settle_cols:
                settlement_select_cols.append("event")
            if "payload_json" in settle_cols:
                settlement_select_cols.append("payload_json")
            if "condition_id" in settle_cols:
                settlement_select_cols.append("condition_id")
            if "outcome" in settle_cols:
                settlement_select_cols.append("outcome")
            if "ts_end" in settle_cols:
                settlement_select_cols.append("ts_end")
            if "ts" in settle_cols:
                settlement_select_cols.append("ts")
            if "time" in settle_cols:
                settlement_select_cols.append("time")
            if recv_settlement_col:
                settlement_select_cols.append(recv_settlement_col)

            if not settlement_select_cols:
                return {
                    "status": "unsupported_settlement_schema",
                    "source_db": str(source_db),
                    "settlement_columns": sorted(settle_cols),
                }

            order_key = "slug" if "slug" in settle_cols else (recv_settlement_col or "rowid")
            settlement_query = f"SELECT {', '.join(settlement_select_cols)} FROM settlement_records ORDER BY {order_key}"

            settlement_rows = []
            for row in source_conn.execute(settlement_query).fetchall():
                total_settlement_rows += 1
                slug = str(row["slug"]) if ("slug" in row.keys() and row["slug"] is not None) else None

                payload = parse_json_payload(row["payload_json"]) if "payload_json" in row.keys() else None
                event = str(row["event"]) if "event" in row.keys() and row["event"] else None
                condition_id = (
                    str(row["condition_id"])
                    if "condition_id" in row.keys() and row["condition_id"] is not None
                    else None
                )
                if not condition_id and slug:
                    condition_id = slug_meta.get(slug, {}).get("condition_id")
                if not condition_id:
                    missing_condition_count += 1
                    continue

                meta = slug_meta.get(slug, {})
                ts_end = None
                if payload:
                    ts_end = to_epoch_seconds(payload.get("round_end_ts") or payload.get("round_end") or payload.get("event_ts"))
                if ts_end is None and "ts_end" in row.keys() and row["ts_end"] is not None:
                    ts_end = to_epoch_seconds(row["ts_end"])
                if ts_end is None and recv_settlement_col is not None and recv_settlement_col in row.keys() and row[recv_settlement_col] is not None:
                    ts_end = to_epoch_seconds(row[recv_settlement_col])
                if ts_end is None and "ts" in row.keys() and row["ts"] is not None:
                    ts_end = to_epoch_seconds(row["ts"])
                if ts_end is None and "time" in row.keys() and row["time"] is not None:
                    ts_end = to_epoch_seconds(row["time"])
                if ts_end is None and slug:
                    ts_end = to_epoch_seconds(meta.get("round_end_ts"))
                if ts_end is None:
                    ts_end = to_epoch_seconds(condition_to_round_end.get(condition_id))
                if ts_end is None:
                    missing_settlement_time_count += 1
                    missing_round_end_count += 1
                    continue

                outcome = parse_outcome(payload, event)
                if outcome not in {"UP", "DOWN"} and "outcome" in row.keys() and row["outcome"] is not None:
                    outcome = parse_outcome({"outcome": row["outcome"]}, event)

                if outcome not in {"UP", "DOWN"} and infer_settlement_outcome:
                    outcome = infer_settlement_outcome_from_book(
                        source_conn,
                        slug,
                        ts_end,
                        recv_book_col,
                        bid_col,
                        ask_col,
                    )
                    if outcome in {"UP", "DOWN"}:
                        inferred_settlement_outcome_count += 1

                if outcome not in {"UP", "DOWN"}:
                    missing_settlement_outcome_count += 1
                    continue

                key = (str(condition_id), outcome, int(ts_end))
                if key in settlement_seen:
                    continue
                settlement_seen.add(key)
                settlement_rows.append(key)

            target_conn.execute("DROP TABLE IF EXISTS market_ticks")
            target_conn.execute("DROP TABLE IF EXISTS settlement_records")
            target_conn.execute(
                """
                CREATE TABLE market_ticks (
                    ts INTEGER NOT NULL,
                    remaining_sec REAL NOT NULL,
                    btc_open REAL,
                    btc_price REAL,
                    btc_price_signal REAL,
                    btc_price_binance REAL,
                    ask_up REAL,
                    bid_up REAL,
                    ask_down REAL,
                    bid_down REAL,
                    ask_depth_L1_up REAL,
                    ask_depth_L1_down REAL,
                    ws_fresh INTEGER,
                    quote_age_up REAL,
                    quote_age_down REAL,
                    is_stale INTEGER,
                    condition_id TEXT NOT NULL
                )
                """
            )
            target_conn.execute(
                """
                CREATE TABLE settlement_records (
                    condition_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    ts_end INTEGER NOT NULL
                )
                """
            )

            if settlement_rows:
                target_conn.executemany(
                    "INSERT INTO settlement_records(condition_id, outcome, ts_end) VALUES (?,?,?)",
                    settlement_rows,
                )

            current_slug = None
            current_ts: int | None = None
            current: dict[str, dict[str, Any] | None] = {"YES": None, "NO": None}
            inserted_market_ticks = 0

            def flush_bucket() -> None:
                nonlocal inserted_market_ticks
                nonlocal market_tick_bucket_count
                nonlocal market_tick_skipped_no_pair_count
                nonlocal missing_md_book_condition_count
                nonlocal missing_md_book_round_end_count
                if current_slug is None or current_ts is None:
                    return
                yes = current.get("YES")
                no = current.get("NO")
                if not yes or not no:
                    market_tick_skipped_no_pair_count += 1
                    return

                meta = slug_meta.get(current_slug)
                condition_id = meta.get("condition_id") if meta else None
                if not condition_id:
                    missing_md_book_condition_count += 1
                    return

                round_end_ts = to_epoch_seconds(meta.get("round_end_ts")) if meta else None
                if round_end_ts is None:
                    missing_md_book_round_end_count += 1
                    return

                market_tick_bucket_count += 1
                remaining_sec = float(round_end_ts - current_ts)
                if remaining_sec < 0:
                    remaining_sec = 0.0

                market_ticks_insert_batch.append(
                    (
                        current_ts,
                        remaining_sec,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        as_float(yes.get("ask"), 0.0),
                        as_float(yes.get("bid"), 0.0),
                        as_float(no.get("ask"), 0.0),
                        as_float(no.get("bid"), 0.0),
                        as_float(yes.get("ask_sz"), 0.0),
                        as_float(no.get("ask_sz"), 0.0),
                        1 if (yes.get("source") or no.get("source")) else 0,
                        0.0,
                        0.0,
                        0,
                        str(condition_id),
                    )
                )

                if len(market_ticks_insert_batch) >= 5000:
                    target_conn.executemany(
                        "INSERT INTO market_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        market_ticks_insert_batch,
                    )
                    inserted_market_ticks += len(market_ticks_insert_batch)
                    market_ticks_insert_batch.clear()

            if source_col:
                md_book_query = (
                    f"SELECT slug, {recv_book_col}, side, {bid_col}, {ask_col}, {bid_sz_col}, {ask_sz_col}, {source_col} "
                    "FROM md_book_l1 "
                    f"WHERE slug IS NOT NULL AND {recv_book_col} IS NOT NULL "
                    f"ORDER BY slug, {recv_book_col}, side"
                )
            else:
                md_book_query = (
                    f"SELECT slug, {recv_book_col}, side, {bid_col}, {ask_col}, {bid_sz_col}, {ask_sz_col} "
                    "FROM md_book_l1 "
                    f"WHERE slug IS NOT NULL AND {recv_book_col} IS NOT NULL "
                    f"ORDER BY slug, {recv_book_col}, side"
                )

            for row in source_conn.execute(
                md_book_query
            ):
                slug = str(row["slug"]) if row["slug"] is not None else ""
                if not slug:
                    continue
                total_md_book_rows += 1
                ts_sec = to_epoch_seconds(row[recv_book_col])
                if ts_sec is None:
                    continue

                side = normalize_side(row["side"])
                if side == "UP":
                    side = "YES"
                elif side == "DOWN":
                    side = "NO"
                if side not in {"YES", "NO"}:
                    continue

                if current_slug != slug or current_ts != ts_sec:
                    flush_bucket()
                    current_slug = slug
                    current_ts = ts_sec
                    current = {"YES": None, "NO": None}

                current[side] = {
                    "bid": row[bid_col],
                    "ask": row[ask_col],
                    "bid_sz": row[bid_sz_col],
                    "ask_sz": row[ask_sz_col],
                    "source": row[source_col] if source_col else None,
                }

            flush_bucket()

            if market_ticks_insert_batch:
                target_conn.executemany(
                    "INSERT INTO market_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    market_ticks_insert_batch,
                )
                inserted_market_ticks += len(market_ticks_insert_batch)
                market_ticks_insert_batch.clear()

            target_conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_market_ticks_condition_ts ON market_ticks(condition_id, ts)"
            )
            target_conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_settlement_condition_ts_end ON settlement_records(condition_id, ts_end)"
            )
            target_conn.commit()

            info.update(
                {
                    "status": "ok",
                    "market_tick_rows": inserted_market_ticks,
                    "settlement_rows": len(settlement_rows),
                    "missing_settlement_outcome_count": missing_settlement_outcome_count,
                    "missing_condition_count": missing_condition_count,
                    "missing_round_end_count": missing_round_end_count,
                    "missing_settlement_time_count": missing_settlement_time_count,
                    "inferred_settlement_outcome_count": inferred_settlement_outcome_count,
                    "market_tick_bucket_count": market_tick_bucket_count,
                    "market_tick_skipped_no_pair_count": market_tick_skipped_no_pair_count,
                    "total_md_book_rows": total_md_book_rows,
                    "missing_md_book_condition_count": missing_md_book_condition_count,
                    "missing_md_book_round_end_count": missing_md_book_round_end_count,
                    "total_settlement_rows": total_settlement_rows,
                    "slug_count": len(slug_meta),
                    "settlement_coverage_ratio": (
                        len(settlement_rows) / total_settlement_rows if total_settlement_rows else 0.0
                    ),
                }
            )
    except sqlite3.Error as exc:
        info.update({"status": "failed", "error": str(exc)})

    return info


def resolve_backtest_db(
    db_path: Path,
    output_root: Path,
    *,
    force: bool = False,
    infer_settlement_outcome: bool = False,
) -> tuple[Path, dict[str, Any]]:
    compat_root = output_root / "compat_db"
    compat_root.mkdir(parents=True, exist_ok=True)
    db_sig = hashlib.md5(str(db_path).encode("utf-8")).hexdigest()[:10]
    compat_db = compat_root / f"{db_path.stem}.{db_sig}.compat_market_ticks.sqlite"

    if db_path.suffix.lower() == ".duckdb":
        if force and compat_db.exists():
            compat_db.unlink()
        if not compat_db.exists():
            info = materialize_duckdb_replay_store_v2_market_ticks(
                db_path,
                compat_db,
                infer_settlement_outcome=infer_settlement_outcome,
            )
        else:
            info = {
                "status": "reused",
                "source_db": str(db_path),
                "target_db": str(compat_db),
                "schema": "duckdb_replay_store_v2",
            }

        info.setdefault("source_db", str(db_path))
        info.setdefault("target_db", str(compat_db))
        info.setdefault("schema", "duckdb_replay_store_v2")
        if info.get("status") != "ok":
            return db_path, info
        return compat_db, info

    try:
        with sqlite3.connect(str(db_path)) as conn:
            schema_kind = detect_backtest_db_schema(conn)
    except sqlite3.Error as exc:
        return db_path, {
            "status": "source_open_failed",
            "schema": "unsupported",
            "error": str(exc),
            "source_db": str(db_path),
        }

    if schema_kind == "legacy_market_ticks":
        return db_path, {"status": "native", "schema": schema_kind, "source_db": str(db_path)}

    if schema_kind != "replay_rowwise_ticks":
        return db_path, {
            "status": "unsupported",
            "schema": schema_kind,
            "source_db": str(db_path),
            "error": "unsupported source schema",
        }

    if force and compat_db.exists():
        compat_db.unlink()

    if not compat_db.exists():
        info = materialize_legacy_market_ticks(
            db_path,
            compat_db,
            infer_settlement_outcome=infer_settlement_outcome,
        )
    else:
        info = {"status": "reused", "source_db": str(db_path), "target_db": str(compat_db), "schema": schema_kind}

    info.setdefault("source_db", str(db_path))
    info.setdefault("target_db", str(compat_db))
    info.setdefault("schema", schema_kind)

    if info.get("status") != "ok":
        return db_path, info

    return compat_db, info

def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def sqlite_scalar(conn: sqlite3.Connection, query: str) -> Any:
    row = conn.execute(query).fetchone()
    return row[0] if row else None


def load_gate_profile(path: Path | None) -> dict[str, Any]:
    if path:
        loaded = read_json(path)
        if isinstance(loaded, dict):
            return loaded
    return {
        "schema_version": 1,
        "name": "xuan_b27_dplus_reproducibility_gate",
        "thresholds": DEFAULT_GATE_RULES,
        "hardline_false_fields": RESEARCH_HARDLINE_FIELDS,
    }


def normalize_profile(profile: dict[str, Any] | None) -> dict[str, Any]:
    profile = profile or {}
    thresholds = dict(DEFAULT_GATE_RULES)
    thresholds.update(profile.get("thresholds", {}) if isinstance(profile.get("thresholds"), dict) else {})
    hardline = dict(RESEARCH_HARDLINE_FIELDS)
    hardline.update(profile.get("hardline_false_fields", {}) if isinstance(profile.get("hardline_false_fields"), dict) else {})
    saturation_defaults = dict(
        {
            "max_wall_clock_seconds": DEFAULT_WALL_CLOCK_SECONDS,
            "saturation_patience_generations": DEFAULT_SATURATION_PATIENCE_GENERATIONS,
            "saturation_score_delta": DEFAULT_SATURATION_SCORE_DELTA,
            "saturation_pnl_delta": DEFAULT_SATURATION_PNL_DELTA,
        }
    )
    saturation_defaults.update(
        profile.get("saturation_defaults", {})
        if isinstance(profile.get("saturation_defaults"), dict)
        else {}
    )
    return {
        "schema_version": int(profile.get("schema_version", 1)),
        "name": str(profile.get("name", "xuan_b27_dplus_reproducibility_gate")),
        "thresholds": thresholds,
        "hardline_false_fields": hardline,
        "saturation_defaults": saturation_defaults,
    }


def extract_manifest_fingerprint_data(manifest: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        return {}
    paths = as_dict(manifest.get("paths"))
    summary = as_dict(manifest.get("summary"))
    checksums = {
        "manifest_fingerprint": manifest_signature(manifest),
        "source_dataset_fingerprint": as_dict(manifest).get("source_dataset_fingerprint"),
        "source_dataset_version": as_dict(manifest).get("source_dataset_version"),
        "source_semantics_contract_id": as_dict(manifest).get("source_semantics_contract_id"),
        "runtime_binding": as_dict(manifest).get("runtime_binding"),
        "runtime_config": as_dict(manifest).get("runtime_config"),
        "schema_hash": as_dict(manifest).get("schema_hash"),
        "audit_hashes": as_dict(manifest).get("audit_hashes"),
    }
    if not checksums["audit_hashes"] and paths:
        audit_manifest = as_path(paths.get("audit_manifest"))
        audit_hash = file_sha256(audit_manifest) if isinstance(audit_manifest, Path) else None
        if audit_hash:
            checksums["audit_hash_fallback_sha256"] = audit_hash
    for key in ["search_safe_row_count", "canonical_audit_selected_candidate_count", "private_promotion_ready_count"]:
        if key in summary:
            checksums[f"summary_{key}"] = summary[key]
    return checksums


def drift_ratio(actual: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return abs((actual - target) / target)


def metric_check(name: str, actual: float | None, target: float | None, warn_ratio: float, block_ratio: float) -> dict[str, Any]:
    if actual is None:
        return {"name": name, "status": "MISSING_ACTUAL", "actual": None, "target": target, "drift_ratio": None}
    if target is None or target <= 0:
        return {"name": name, "status": "NO_TARGET", "actual": actual, "target": target, "drift_ratio": None}
    ratio = drift_ratio(float(actual), float(target))
    status = "PASS"
    if ratio > block_ratio:
        status = "BLOCK"
    elif ratio > warn_ratio:
        status = "WARN"
    return {"name": name, "status": status, "actual": actual, "target": target, "drift_ratio": ratio}


def evaluate_reproducibility_gate(
    args: argparse.Namespace,
    install_manifest: dict[str, Any] | None,
    strategy_readiness_manifest: dict[str, Any] | None,
    parity_manifest: dict[str, Any] | None,
    gate_profile: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    checks: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    thresholds = as_dict(gate_profile.get("thresholds"))
    hardline = as_dict(gate_profile.get("hardline_false_fields"))

    if not install_manifest:
        install_status = "MISSING_MANIFEST"
        blockers.append("v1_install_manifest_missing")
    else:
        install_status = as_str(install_manifest.get("status"), "UNKNOWN")

    if install_status != "OK":
        warnings.append(f"install_status_{install_status.lower()}")

    summary = as_dict(install_manifest.get("summary")) if install_manifest else {}
    target_search_safe = args.expected_search_safe_row_count
    target_canonical = args.expected_canonical_audit_selected_candidate_count
    if args.v1_install_manifest_reference and args.v1_install_manifest_reference.exists():
        ref_manifest = read_json(args.v1_install_manifest_reference)
        ref_summary = as_dict(ref_manifest.get("summary")) if isinstance(ref_manifest, dict) else {}
        target_search_safe = target_search_safe if target_search_safe is not None else as_int(ref_summary.get("search_safe_row_count"))
        target_canonical = target_canonical if target_canonical is not None else as_int(ref_summary.get("canonical_audit_selected_candidate_count"))

    if target_search_safe is None and install_manifest and as_dict(summary).get("search_safe_row_count") is not None:
        target_search_safe = as_int(summary.get("search_safe_row_count"))
    if target_canonical is None and install_manifest and as_dict(summary).get("canonical_audit_selected_candidate_count") is not None:
        target_canonical = as_int(summary.get("canonical_audit_selected_candidate_count"))

    checks.append(
        metric_check(
            "search_safe_row_count",
            as_float(summary.get("search_safe_row_count"), None)
            if "search_safe_row_count" in summary
            else None,
            float(target_search_safe or 0) if target_search_safe else None,
            as_float(thresholds.get("search_safe_row_count_warn_ratio"), 0.05),
            as_float(thresholds.get("search_safe_row_count_block_ratio"), 0.10),
        )
    )
    checks.append(
        metric_check(
            "canonical_audit_selected_candidate_count",
            as_float(summary.get("canonical_audit_selected_candidate_count"), None)
            if "canonical_audit_selected_candidate_count" in summary
            else None,
            float(target_canonical or 0) if target_canonical else None,
            as_float(thresholds.get("canonical_audit_selected_candidate_count_warn_ratio"), 0.05),
            as_float(thresholds.get("canonical_audit_selected_candidate_count_block_ratio"), 0.10),
        )
    )

    fail_count = as_int(summary.get("fail_count"), 0)
    warn_count = as_int(summary.get("warn_count"), 0)
    if fail_count is not None and fail_count > as_int(thresholds.get("max_fail_count"), 0):
        blockers.append("install_fail_count_over_threshold")
        warnings.append(f"fail_count_{fail_count}")
    if warn_count is not None and warn_count > as_int(thresholds.get("max_warn_count"), 0):
        warnings.append(f"warn_count_{warn_count}")

    for field, expected in hardline.items():
        field_seen = (
            as_bool(strategy_readiness_manifest.get(field))
            if isinstance(strategy_readiness_manifest, dict)
            else None
        )
        if field_seen is None and isinstance(parity_manifest, dict):
            field_seen = as_bool(parity_manifest.get(field))
        if field_seen is None:
            continue
        if field_seen != expected:
            blockers.append(f"hardline_violation:{field}={field_seen}")
            checks.append(
                {
                    "name": f"hardline:{field}",
                    "status": "BLOCK",
                    "actual": field_seen,
                    "target": expected,
                    "drift_ratio": None,
                }
            )
        else:
            checks.append(
                {
                    "name": f"hardline:{field}",
                    "status": "PASS",
                    "actual": field_seen,
                    "target": expected,
                    "drift_ratio": 0.0,
                }
            )

    status = "PASS"
    if blockers:
        status = "BLOCKED"
    elif any(check.get("status") == "BLOCK" for check in checks):
        status = "BLOCKED"
    elif any(check.get("status") == "WARN" for check in checks):
        status = "WARN"

    return {
        "status": status,
        "manifest_paths": {
            "v1_install_manifest": str(args.v1_install_manifest),
            "strategy_readiness_manifest": str(args.v1_strategy_readiness_manifest)
            if args.v1_strategy_readiness_manifest
            else None,
            "btc_parity_manifest": str(args.v1_btc_parity_manifest)
            if args.v1_btc_parity_manifest
            else None,
            "v1_install_manifest_reference": str(args.v1_install_manifest_reference)
            if args.v1_install_manifest_reference
            else None,
        },
        "manifest_signatures": {
            "v1_install": extract_manifest_fingerprint_data(install_manifest),
            "strategy_readiness": extract_manifest_fingerprint_data(strategy_readiness_manifest),
            "btc_parity": extract_manifest_fingerprint_data(parity_manifest),
        },
        "metrics": as_dict(summary),
        "checks": checks,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "expected_reference": {
            "search_safe_row_count": target_search_safe,
            "canonical_audit_selected_candidate_count": target_canonical,
            "policy_thresholds": as_dict(gate_profile.get("thresholds")),
            "hardline_false_fields": as_dict(gate_profile.get("hardline_false_fields")),
        },
    }, blockers


def evaluate_db_declaration(db_path: Path, limit: int, skip: int) -> dict[str, Any]:
    declaration: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "db": str(db_path),
        "rows_estimate": 0,
        "days": [],
        "skip": skip,
        "limit": limit,
        "dataset_type": "local_sqlite_snapshot",
        "can_support_strategy_promotion": False,
        "reporting_rule": (
            "Local-only iteration driver. Use published V1 manifests for promotion/private-truth claims."
        ),
    }

    if not db_path.exists() or not path_is_safe(db_path):
        declaration["status"] = "UNDECLARED_DB_PATH"
        return declaration

    try:
        with sqlite3.connect(str(db_path)) as conn:
            rows = sqlite_scalar(conn, "select count(*) from market_ticks")
            days = [
                row[0]
                for row in conn.execute(
                    "select distinct strftime('%Y%m%d', ts, 'unixepoch') from market_ticks order by 1"
                )
                if row[0]
            ]
            declaration.update(
                {
                    "status": "DECLARED",
                    "rows_estimate": int(rows or 0),
                    "days": days,
                    "day_count": len(days),
                    "min_ts": sqlite_scalar(conn, "select min(ts) from market_ticks"),
                    "max_ts": sqlite_scalar(conn, "select max(ts) from market_ticks"),
                    "row_count": int(rows or 0),
                    "condition_count": int(
                        sqlite_scalar(conn, "select count(distinct condition_id) from market_ticks")
                        or 0
                    ),
                }
            )
    except sqlite3.Error as exc:
        declaration["status"] = "DB_ERROR"
        declaration["error"] = str(exc)

    return declaration


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_jsonl(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        payload = line.strip()
        if not payload:
            continue
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def candidate_id(cfg: dict[str, Any]) -> str:
    body = json.dumps(cfg, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(body.encode("utf-8")).hexdigest()[:12]


def safe_float(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(out) or math.isinf(out):
        return default
    return max(lo, min(hi, out))


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def to_arg_list(cfg: dict[str, Any], args: argparse.Namespace) -> list[str]:
    cmd: list[str] = [
        "cargo",
        "run",
        "--release",
        "--quiet",
        "--bin",
        "pair_arb_backtest",
        "--",
        "--jsonl",
        "--db",
        str(args.db),
        "--limit",
        str(args.limit),
        "--skip",
        str(args.skip),
        "--max-net-diff",
        str(cfg["max_net_diff"]),
        "--pair-target",
        str(cfg["pair_target"]),
        "--bid-size",
        str(cfg["bid_size"]),
        "--tier1",
        str(cfg["tier_1_mult"]),
        "--tier2",
        str(cfg["tier_2_mult"]),
        "--tier-mode",
        cfg["tier_mode"],
        "--fill-model",
        cfg["fill_model"],
        "--cutoff",
        str(cfg["risk_open_cutoff_secs"]),
        "--margin",
        str(cfg["pair_cost_safety_margin"]),
        "--salvage-net-cap",
        str(cfg["salvage_net_cap"]),
        "--salvage-start-remaining",
        str(cfg["salvage_start_remaining_secs"]),
        "--taker-fee-rate",
        str(cfg["taker_fee_rate"]),
        "--salvage-failure-probability",
        "0.10",
        "--exit-window-secs",
        "10.0",
        "--exit-loss-limit",
        "0.05",
        "--directional-risk-filter-bps",
        str(cfg["directional_risk_filter_bps"]),
        "--directional-entry-min-bps",
        str(cfg["directional_entry_min_bps"]),
        "--directional-price-source",
        cfg["directional_price_source"],
        "--entry-pair-max-ask-sum",
        str(cfg["entry_pair_max_ask_sum"]),
    ]

    if cfg.get("reject_stale"):
        cmd.append("--reject-stale")
    if cfg.get("require_ws_fresh"):
        cmd.append("--require-ws-fresh")
    if cfg.get("max_quote_age_secs") > 0:
        cmd.extend(["--max-quote-age-sec", str(cfg["max_quote_age_secs"])])
    if cfg.get("min_ask_depth") > 0:
        cmd.extend(["--min-ask-depth", str(cfg["min_ask_depth"])])
    if cfg.get("require_two_sided_entry"):
        cmd.append("--require-two-sided-entry")
    if cfg.get("pairing_only_when_residual"):
        cmd.append("--pairing-only-when-residual")
    if cfg.get("initial_balance"):
        cmd.extend(["--initial-balance", str(cfg["initial_balance"])])
    return cmd


def make_cfg_dict(cfg: Candidate) -> dict[str, Any]:
    return {
        "max_net_diff": cfg.max_net_diff,
        "pair_target": cfg.pair_target,
        "bid_size": cfg.bid_size,
        "tier_1_mult": cfg.tier_1_mult,
        "tier_2_mult": cfg.tier_2_mult,
        "tier_mode": cfg.tier_mode,
        "fill_model": cfg.fill_model,
        "risk_open_cutoff_secs": cfg.risk_open_cutoff_secs,
        "pair_cost_safety_margin": cfg.pair_cost_safety_margin,
        "salvage_net_cap": cfg.salvage_net_cap,
        "salvage_start_remaining_secs": cfg.salvage_start_remaining_secs,
        "taker_fee_rate": cfg.taker_fee_rate,
        "directional_risk_filter_bps": cfg.directional_risk_filter_bps,
        "directional_entry_min_bps": cfg.directional_entry_min_bps,
        "directional_price_source": cfg.directional_price_source,
        "max_quote_age_secs": cfg.max_quote_age_secs,
        "min_ask_depth": cfg.min_ask_depth,
        "entry_pair_max_ask_sum": cfg.entry_pair_max_ask_sum,
        "reject_stale": cfg.reject_stale,
        "require_ws_fresh": cfg.require_ws_fresh,
        "require_two_sided_entry": cfg.require_two_sided_entry,
        "pairing_only_when_residual": cfg.pairing_only_when_residual,
        "initial_balance": cfg.initial_balance,
    }


def candidate_from_dict(d: dict[str, Any]) -> Candidate:
    return Candidate(
        max_net_diff=safe_float(d.get("max_net_diff"), 0.02, 8.0, 0.2),
        pair_target=safe_float(d.get("pair_target"), 0.50, 0.99, 0.80),
        bid_size=safe_float(d.get("bid_size"), 0.02, 2.5, 0.10),
        tier_1_mult=safe_float(d.get("tier_1_mult"), 0.05, 1.0, 0.50),
        tier_2_mult=safe_float(d.get("tier_2_mult"), 0.05, 0.40, 0.15),
        tier_mode=str(d.get("tier_mode") or "disabled"),
        fill_model=str(d.get("fill_model") or "conservative"),
        risk_open_cutoff_secs=safe_float(d.get("risk_open_cutoff_secs"), 45.0, 420.0, 180.0),
        pair_cost_safety_margin=safe_float(d.get("pair_cost_safety_margin"), 0.0, 0.12, 0.01),
        salvage_net_cap=clamp(safe_float(d.get("salvage_net_cap"), 0.0, 1.0, 0.95), 0.0, 1.0),
        salvage_start_remaining_secs=safe_float(d.get("salvage_start_remaining_secs"), 60.0, 600.0, 240.0),
        taker_fee_rate=FIXED_TAKER_FEE_RATE if FIXED_TAKER_FEE_RATE is not None else safe_float(d.get("taker_fee_rate"), 0.0, 0.10, 0.0),
        directional_risk_filter_bps=safe_float(d.get("directional_risk_filter_bps"), 0.0, 8.0, 0.0),
        directional_entry_min_bps=safe_float(d.get("directional_entry_min_bps"), 0.0, 4.0, 0.0),
        directional_price_source=str(d.get("directional_price_source") or "price"),
        max_quote_age_secs=safe_float(d.get("max_quote_age_secs"), 0.0, 1200.0, 0.0),
        min_ask_depth=safe_float(d.get("min_ask_depth"), 0.0, 100000.0, 0.0),
        entry_pair_max_ask_sum=safe_float(d.get("entry_pair_max_ask_sum"), 0.0, 2.0, 0.0),
        reject_stale=bool(d.get("reject_stale", False)),
        require_ws_fresh=bool(d.get("require_ws_fresh", False)),
        require_two_sided_entry=bool(d.get("require_two_sided_entry", False)),
        pairing_only_when_residual=bool(d.get("pairing_only_when_residual", False)),
        initial_balance=str(d.get("initial_balance") or ""),
    )


def metric(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = (row.get("metrics") or {}).get(key)
    return as_float(value, default)


def run_single_candidate(
    cfg: Candidate,
    generation: int,
    index: int,
    root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    cfg_dict = make_cfg_dict(cfg)
    cid = candidate_id(cfg_dict)
    run_dir = root / f"generation_{generation:02d}" / f"run_{index:02d}_{cid}"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = run_dir / "candidate.json"
    cmd = to_arg_list(cfg_dict, args)
    write_json(cfg_path, {
        "candidate_id": cid,
        "generation": generation,
        "index": index,
        "config": cfg_dict,
        "command": cmd,
        "created_utc": utc_label(),
    })

    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    stdout_text = proc.stdout or ""
    stderr_text = proc.stderr or ""
    (run_dir / "backtest_stdout.jsonl").write_text(stdout_text)
    (run_dir / "backtest_stderr.log").write_text(stderr_text)
    write_json(run_dir / "command.json", {"command": cmd})

    rows = parse_jsonl(stdout_text)
    if not rows:
        return {
            "candidate_id": cid,
            "run_dir": str(run_dir),
            "generation": generation,
            "index": index,
            "status": "FAIL_NO_BACKTEST_ROWS",
            "returncode": int(proc.returncode),
            "score": -1.0e9,
            "score_components": {
                "mode": args.research_mode,
                "failure": "no_jsonl_rows",
            },
            "config": cfg_dict,
            "error": "no pair_arb_backtest_run rows in jsonl output",
            "metrics": {},
            "summary": {
                "total_pnl": 0.0,
                "weighted_avg_pair_cost": 1.0,
                "paired_window_rate": 0.0,
                "fill_window_rate": 0.0,
                "residual_loss_rate": 1.0,
                "residual_cost": 0.0,
                "completion_cost": 0.0,
                "completion_fee": 0.0,
                "fills": 0.0,
                "paired_pnl": 0.0,
                "residual_pnl": 0.0,
                "pairing_net_pnl": 0.0,
            },
        }

    row = rows[-1]
    summary = {
        "total_pnl": metric(row, "total_pnl", 0.0),
        "weighted_avg_pair_cost": metric(row, "weighted_avg_pair_cost", 1.0),
        "paired_window_rate": metric(row, "paired_window_rate", 0.0),
        "fill_window_rate": metric(row, "fill_window_rate", 0.0),
        "residual_loss_rate": metric(row, "residual_loss_rate", 0.0),
        "residual_cost": metric(row, "residual_cost", 0.0),
        "completion_cost": metric(row, "completion_cost", 0.0),
        "completion_fee": metric(row, "completion_fee", 0.0),
        "fills": metric(row, "fills", 0.0),
        "paired_qty": metric(row, "paired_qty", 0.0),
        "residual_qty": metric(row, "residual_qty", 0.0),
        "completion_fills": metric(row, "completion_fills", 0.0),
        "completion_qty": metric(row, "completion_qty", 0.0),
        "paired_pnl": metric(row, "paired_pnl", 0.0),
        "residual_pnl": metric(row, "residual_pnl", 0.0),
    }
    residual_cost = max(0.0, -summary["residual_pnl"])
    if residual_cost == 0.0:
        residual_cost = summary["residual_cost"]
    summary["pairing_net_pnl"] = max(0.0, summary["paired_pnl"]) - residual_cost - max(0.0, summary["completion_fee"])

    score, score_components = compute_score(summary, args.research_mode)
    status = "OK"
    if proc.returncode != 0:
        status = "FAIL_COMMAND"
        score = -1.0e8
        score_components = {
            "mode": args.research_mode,
            "reason": "command_nonzero_return",
            "command_rc": proc.returncode,
        }
    elif summary["fills"] <= 0.0:
        status = "NO_FILL"
        score = -1.0e7
        score_components = {
            "mode": args.research_mode,
            "reason": "zero_fills",
            "command_rc": proc.returncode,
        }

    result = {
        "candidate_id": cid,
        "run_dir": str(run_dir),
        "generation": generation,
        "index": index,
        "status": status,
        "returncode": int(proc.returncode),
        "config": cfg_dict,
        "score": score,
        "score_components": score_components,
        "summary": summary,
        "raw_row": row,
    }
    write_json(run_dir / "result.json", result)
    return result


def compute_score(summary: dict[str, float], research_mode: str = DEFAULT_RESEARCH_MODE) -> tuple[float, dict[str, Any]]:
    mode = resolve_research_mode(research_mode)
    weights = RESEARCH_SCORE_WEIGHTS[mode]

    paired_pnl = summary.get("paired_pnl", 0.0)
    total_pnl = summary.get("total_pnl", 0.0)
    fill_windows = summary.get("fill_window_rate", 0.0)
    paired_windows = summary.get("paired_window_rate", 0.0)
    paired_qty = summary.get("paired_qty", 0.0)
    residual_qty = summary.get("residual_qty", 0.0)
    completion_fills = summary.get("completion_fills", 0.0)
    weighted_pair_cost = summary.get("weighted_avg_pair_cost", 1.0)
    residual_loss = clamp(summary.get("residual_loss_rate", 0.0), 0.0, 1.0)
    fills = summary.get("fills", 0.0)
    residual_pnl = summary.get("residual_pnl", 0.0)
    residual_cost = summary.get("residual_cost", 0.0)
    completion_fee = summary.get("completion_fee", 0.0)
    completion_cost = summary.get("completion_cost", 0.0)
    if residual_cost == 0.0:
        residual_cost = max(0.0, -residual_pnl)
    pairing_net_pnl = summary.get("pairing_net_pnl", 0.0)
    if pairing_net_pnl == 0.0:
        pairing_net_pnl = max(0.0, paired_pnl) - residual_cost - max(0.0, completion_fee)

    pair_cost_term = (1.0 - clamp(weighted_pair_cost, 0.0, weights["pair_cost_clip"])) * weights["pair_cost"]
    fill_term = clamp(fill_windows, 0.0, 1.0) * weights["participation"]
    paired_window_term = math.sqrt(max(0.0, paired_windows))
    paired_window_term = paired_window_term * 0.0
    fill_count_term = math.log1p(max(0.0, fills)) * weights["fill_count"]
    residual_qty_term = -math.log1p(max(0.0, residual_qty)) * weights["residual_qty"]
    completion_ratio_term = 0.0
    if fills > 0:
        completion_ratio_term = math.sqrt(completion_fills / fills) * 0.25 * weights["fill_count"]
    residual_loss_rate_term = (1.0 - residual_loss) * weights["residual_loss_rate"]
    residual_loss_term = -max(0.0, residual_cost) * weights["residual_loss_cost"]
    fee_term = -max(0.0, completion_fee) * weights["fee"]
    completion_cost_term = -max(0.0, completion_cost) * 0.1
    pnl_term = pairing_net_pnl * weights["net_pnl"]

    score_components = {
        "pnl_term": pnl_term,
        "pair_cost_term": pair_cost_term,
        "fill_term": fill_term,
        "paired_window_term": paired_window_term,
        "fill_count_term": fill_count_term,
        "paired_qty_term": math.log1p(max(0.0, paired_qty)) * 0.0,
        "residual_qty_term": residual_qty_term,
        "completion_ratio_term": completion_ratio_term,
        "residual_loss_rate_term": residual_loss_rate_term,
        "residual_loss_term": residual_loss_term,
        "fee_term": fee_term,
        "completion_cost_term": completion_cost_term,
        "total_pnl": total_pnl,
        "pairing_net_pnl": pairing_net_pnl,
        "residual_cost": residual_cost,
    }
    score = (
        pnl_term
        + pair_cost_term
        + fill_term
        + paired_window_term
        + fill_count_term
        + 0.0
        + residual_qty_term
        + completion_ratio_term
        + residual_loss_term
        + residual_loss_rate_term
        + fee_term
        + completion_cost_term
    )
    score_components["total_score"] = score
    return score, score_components


def mutate_candidate(parent: Candidate, rng: random.Random, explore_scale: float = 0.5, research_mode: str = "balanced") -> Candidate:
    mode = resolve_research_mode(research_mode)
    mutated = {
        "max_net_diff": parent.max_net_diff * (1.0 + rng.gauss(0.0, 0.25 * explore_scale)),
        "pair_target": parent.pair_target + rng.gauss(0.0, 0.02 * explore_scale),
        "bid_size": parent.bid_size * (1.0 + rng.gauss(0.0, 0.30 * explore_scale)),
        "tier_1_mult": parent.tier_1_mult + rng.gauss(0.0, 0.08 * explore_scale),
        "tier_2_mult": parent.tier_2_mult + rng.gauss(0.0, 0.04 * explore_scale),
        "tier_mode": parent.tier_mode,
        "fill_model": parent.fill_model,
        "risk_open_cutoff_secs": parent.risk_open_cutoff_secs + rng.gauss(0.0, 25.0 * explore_scale),
        "pair_cost_safety_margin": parent.pair_cost_safety_margin + rng.gauss(0.0, 0.01 * explore_scale),
        "salvage_net_cap": parent.salvage_net_cap + rng.gauss(0.0, 0.04 * explore_scale),
        "salvage_start_remaining_secs": parent.salvage_start_remaining_secs + rng.gauss(0.0, 25.0 * explore_scale),
        "taker_fee_rate": parent.taker_fee_rate + rng.gauss(0.0, 0.01 * explore_scale),
        "directional_risk_filter_bps": parent.directional_risk_filter_bps + rng.gauss(0.0, 0.5 * explore_scale),
        "directional_entry_min_bps": parent.directional_entry_min_bps + rng.gauss(0.0, 0.3 * explore_scale),
        "directional_price_source": parent.directional_price_source,
        "max_quote_age_secs": parent.max_quote_age_secs + rng.gauss(0.0, 40.0 * explore_scale),
        "min_ask_depth": parent.min_ask_depth + rng.gauss(0.0, 5.0 * explore_scale),
        "entry_pair_max_ask_sum": parent.entry_pair_max_ask_sum + rng.gauss(0.0, 0.02 * explore_scale),
        "reject_stale": parent.reject_stale,
        "require_ws_fresh": parent.require_ws_fresh,
        "require_two_sided_entry": parent.require_two_sided_entry,
        "pairing_only_when_residual": parent.pairing_only_when_residual,
        "initial_balance": parent.initial_balance,
    }

    if rng.random() < 0.28:
        mutated["reject_stale"] = True if mode == "nagi" else not parent.reject_stale
    elif rng.random() < 0.10:
        mutated["reject_stale"] = not parent.reject_stale
    if rng.random() < 0.20:
        mutated["require_ws_fresh"] = not parent.require_ws_fresh
    if rng.random() < 0.18:
        mutated["require_two_sided_entry"] = not parent.require_two_sided_entry
    if rng.random() < (0.10 if mode == "nagi" else 0.22):
        mutated["pairing_only_when_residual"] = not parent.pairing_only_when_residual
    if mode == "nagi" and rng.random() < 0.25:
        mutated["pairing_only_when_residual"] = True
    if mode == "ce25" and rng.random() < 0.30:
        mutated["pairing_only_when_residual"] = False
    if rng.random() < 0.20:
        mutated["fill_model"] = "aggressive" if parent.fill_model == "conservative" else "conservative"
    if rng.random() < 0.20:
        order = ["disabled", "discrete", "continuous"]
        mutated["tier_mode"] = rng.choice(order)
    if rng.random() < 0.20:
        mutated["directional_price_source"] = rng.choice(["price", "signal", "binance"])

    if mode == "nagi":
        if rng.random() < 0.20:
            mutated["directional_risk_filter_bps"] = clamp(
                parent.directional_risk_filter_bps + rng.gauss(0.0, 0.35 * explore_scale),
                0.0,
                8.0,
            )
        if rng.random() < 0.20:
            mutated["directional_entry_min_bps"] = clamp(
                parent.directional_entry_min_bps + rng.gauss(0.0, 0.20 * explore_scale),
                0.0,
                2.0,
            )
        if rng.random() < 0.18:
            mutated["fill_model"] = "conservative"
        if rng.random() < 0.20:
            mutated["pair_cost_safety_margin"] = clamp(
                parent.pair_cost_safety_margin + rng.gauss(0.0, 0.006 * explore_scale),
                0.0,
                0.08,
            )
    elif mode == "ce25":
        if rng.random() < 0.16:
            mutated["pairing_only_when_residual"] = False
        if rng.random() < 0.20:
            mutated["pair_cost_safety_margin"] = clamp(
                parent.pair_cost_safety_margin + rng.gauss(0.0, 0.01 * explore_scale),
                0.0,
                0.12,
            )

    return candidate_from_dict(mutated)


def random_candidate(rng: random.Random, research_mode: str = "balanced") -> Candidate:
    mode = resolve_research_mode(research_mode)
    base = {
        "max_net_diff": clamp(0.02 + rng.random() * 2.5, 0.02, 5.0),
        "pair_target": clamp(0.68 + rng.random() * 0.25, 0.5, 0.99),
        "bid_size": clamp(0.02 + rng.random() * 0.4, 0.01, 1.0),
        "tier_1_mult": clamp(0.15 + rng.random() * 0.7, 0.05, 1.0),
        "tier_2_mult": clamp(0.05 + rng.random() * 0.25, 0.01, 0.4),
        "tier_mode": rng.choice(["disabled", "continuous", "discrete"]),
        "fill_model": rng.choice(["conservative", "aggressive"]),
        "risk_open_cutoff_secs": clamp(80.0 + rng.random() * 260.0, 45.0, 420.0),
        "pair_cost_safety_margin": clamp(rng.random() * 0.08, 0.0, 0.12),
        "salvage_net_cap": clamp(0.6 + rng.random() * 0.4, 0.0, 1.0),
        "salvage_start_remaining_secs": clamp(90.0 + rng.random() * 300.0, 60.0, 600.0),
        "taker_fee_rate": clamp(rng.random() * 0.08, 0.0, 0.1),
        "directional_risk_filter_bps": clamp(rng.random() * 3.0, 0.0, 8.0),
        "directional_entry_min_bps": clamp(rng.random() * 1.0, 0.0, 4.0),
        "directional_price_source": rng.choice(["price", "signal", "binance"]),
        "max_quote_age_secs": 0.0,
        "min_ask_depth": 0.0,
        "entry_pair_max_ask_sum": clamp(rng.random() * 0.25, 0.0, 1.5),
        "reject_stale": rng.random() < 0.15,
        "require_ws_fresh": rng.random() < 0.15,
        "require_two_sided_entry": rng.random() < 0.15,
        "pairing_only_when_residual": False if mode != "nagi" else rng.random() < 0.85,
        "initial_balance": "",
    }
    return candidate_from_dict(base)


def baseline_from_manifest(manifest_path: Path | None) -> dict[str, Any] | None:
    if not manifest_path:
        return None
    try:
        data = json.loads(manifest_path.read_text())
    except Exception as exc:
        return {"status": "ERROR", "error": str(exc)}

    if not isinstance(data, dict):
        return {"status": "ERROR", "error": "manifest is not a JSON object"}

    best = data.get("best_positive_run")
    if not isinstance(best, dict):
        best = data.get("best") or {}

    return {
        "status": data.get("status", "UNKNOWN"),
        "path": str(manifest_path),
        "pnl": as_float((best or {}).get("pnl", data.get("pnl")), 0.0),
        "pair_cost": as_float((best or {}).get("weighted_avg_pair_cost"), 1.0),
        "residual_loss_rate": as_float((best or {}).get("residual_loss_rate"), 0.0),
        "fill_window_rate": as_float((best or {}).get("fill_window_rate"), 0.0),
    }


def build_seed_population(seed_size: int, rng: random.Random, research_mode: str = "balanced") -> list[Candidate]:
    mode = resolve_research_mode(research_mode)
    if mode == "balanced":
        base_profiles = RESEARCH_SEED_PROFILES["nagi"] + RESEARCH_SEED_PROFILES["ce25"]
    else:
        base_profiles = list(RESEARCH_SEED_PROFILES[mode])

    population = [candidate_from_dict(profile) for profile in base_profiles]
    while len(population) < seed_size:
        parent = rng.choice(population)
        population.append(mutate_candidate(parent, rng, explore_scale=0.45, research_mode=mode))

    population = list(dict.fromkeys(population))
    return population[:seed_size]


def summarize_generation(
    generation: int,
    results: list[dict[str, Any]],
    keep_top: int,
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    sorted_rows = sorted(results, key=lambda row: row.get("score", -1e99), reverse=True)
    top = sorted_rows[:keep_top]
    best = top[0] if top else None
    summary = {
        "generation": generation,
        "run_count": len(results),
        "pass_count": sum(1 for row in results if row.get("status") == "OK"),
        "fail_count": len(results) - sum(1 for row in results if row.get("status") == "OK"),
        "best": best,
        "top_candidates": top,
        "metrics": {
            "best_score": best.get("score") if best else None,
            "best_pnl": best.get("summary", {}).get("total_pnl") if best else None,
            "best_pairing_net_pnl": best.get("summary", {}).get("pairing_net_pnl") if best else None,
            "best_fills": best.get("summary", {}).get("fills") if best else None,
            "best_pair_cost": best.get("summary", {}).get("weighted_avg_pair_cost") if best else None,
            "best_residual_loss_rate": best.get("summary", {}).get("residual_loss_rate") if best else None,
            "best_completion_fee": best.get("summary", {}).get("completion_fee") if best else None,
            "best_completion_ratio": (
                (best.get("summary", {}).get("completion_fills") / best.get("summary", {}).get("fills"))
                if best and best.get("summary", {}).get("fills", 0) > 0
                else 0.0
            ),
            "baseline_pnl": baseline.get("pnl") if isinstance(baseline, dict) else None,
        },
    }

    if baseline and best and isinstance(baseline, dict):
        summary["reference_delta_pnl"] = as_float(best.get("summary", {}).get("total_pnl"), 0.0) - as_float(
            baseline.get("pnl"), 0.0
        )

    summary_rows = [
        {
            "candidate_id": row.get("candidate_id"),
            "score": row.get("score"),
            "status": row.get("status"),
            "pnl": (row.get("summary", {}) or {}).get("total_pnl"),
            "pairing_net_pnl": (row.get("summary", {}) or {}).get("pairing_net_pnl"),
            "pair_cost": (row.get("summary", {}) or {}).get("weighted_avg_pair_cost"),
            "residual_loss_rate": (row.get("summary", {}) or {}).get("residual_loss_rate"),
            "fills": (row.get("summary", {}) or {}).get("fills"),
            "participation": (row.get("summary", {}) or {}).get("fill_window_rate"),
        }
        for row in sorted_rows[:min(20, len(sorted_rows))]
    ]
    summary["leaderboard"] = summary_rows
    return summary


def run_generation(
    generation: int,
    population: list[Candidate],
    root: Path,
    args: argparse.Namespace,
    max_workers: int,
    research_mode: str = "balanced",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[Candidate]]:
    if not population:
        return [], [], []
    generation_root = root / f"generation_{generation:02d}"
    generation_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                run_single_candidate,
                cfg,
                generation=generation,
                index=idx,
                root=root,
                args=args,
            ): idx
            for idx, cfg in enumerate(population)
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                idx = futures[future]
                cfg = make_cfg_dict(population[idx])
                cid = candidate_id(cfg)
                result = {
                    "candidate_id": cid,
                    "generation": generation,
                    "index": idx,
                    "status": "FAIL_EXCEPTION",
                    "returncode": 1,
                    "score": -1.0e9,
                    "config": cfg,
                    "summary": {
                        "total_pnl": 0.0,
                        "weighted_avg_pair_cost": 1.0,
                        "paired_window_rate": 0.0,
                        "fill_window_rate": 0.0,
                        "residual_loss_rate": 1.0,
                        "fills": 0.0,
                    },
                    "error": str(exc),
                }
            results.append(result)

    results.sort(key=lambda row: row.get("score", -1e99), reverse=True)

    survivors = [candidate_from_dict(row.get("config", {})) for row in results[: args.keep_top]]

    generation_summary = summarize_generation(generation, results, args.keep_top, args.nagi_reference)
    (generation_root / "generation_summary.json").write_text(json.dumps(generation_summary, indent=2, sort_keys=True) + "\n")

    (generation_root / "results.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in results) + "\n"
    )

    return results, generation_summary["top_candidates"], survivors


def next_generation(
    survivors: list[Candidate],
    generation: int,
    population: int,
    rng: random.Random,
    exploration: int,
    research_mode: str = "balanced",
) -> list[Candidate]:
    if survivors:
        parents = survivors
    else:
        parents = []

    next_pop: list[Candidate] = []
    while len(next_pop) < population:
        if parents and rng.random() < 0.75:
            parent = rng.choice(parents)
            scale = 1.0 if generation > 0 else 0.8
            next_pop.append(
                mutate_candidate(parent, rng, explore_scale=scale, research_mode=resolve_research_mode(research_mode))
            )
        else:
            next_pop.append(random_candidate(rng, research_mode=resolve_research_mode(research_mode)))

    if exploration:
        for _ in range(exploration):
            idx = rng.randint(0, len(next_pop) - 1)
            next_pop[idx] = mutate_candidate(
                rng.choice(next_pop),
                rng,
                explore_scale=2.0,
                research_mode=resolve_research_mode(research_mode),
            )

    unique: list[Candidate] = []
    seen = set()
    for cfg in next_pop:
        cfg_dict = make_cfg_dict(cfg)
        cid = candidate_id(cfg_dict)
        if cid in seen:
            continue
        seen.add(cid)
        unique.append(cfg)
    return unique[:population]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pair-arb local autoresearch runner.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--v1-install-manifest", type=Path, default=DEFAULT_V1_INSTALL_MANIFEST)
    parser.add_argument("--v1-install-manifest-reference", type=Path)
    parser.add_argument("--v1-strategy-readiness-manifest", type=Path, default=DEFAULT_V1_STRATEGY_READINESS_MANIFEST)
    parser.add_argument("--v1-btc-parity-manifest", type=Path, default=DEFAULT_V1_BTC_PARITY_MANIFEST)
    parser.add_argument("--gate-profile", type=Path, default=DEFAULT_GATE_PROFILE)
    parser.add_argument("--expected-search-safe-row-count", type=int)
    parser.add_argument("--expected-canonical-audit-selected-candidate-count", type=int)
    parser.add_argument("--seed-size", type=int, default=12)
    parser.add_argument("--generations", type=int, default=3)
    parser.add_argument("--keep-top", type=int, default=3)
    parser.add_argument("--population", type=int, default=10)
    parser.add_argument("--exploration", type=int, default=2)
    parser.add_argument("--workers", type=int, default=max(1, min(6, (os.cpu_count() or 2))))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--skip", type=int, default=0)
    parser.add_argument("--random-seed", type=int, default=20260611)
    parser.add_argument("--initial-balance")
    parser.add_argument("--nagi-baseline-pnl", type=float)
    parser.add_argument("--nagi-baseline-manifest", type=Path)
    parser.add_argument("--max-runs", type=int, default=90)
    parser.add_argument("--infer-settlement-outcome", action="store_true")
    parser.add_argument("--max-wall-clock-seconds", type=int, default=DEFAULT_WALL_CLOCK_SECONDS)
    parser.add_argument("--saturation-patience-generations", type=int, default=DEFAULT_SATURATION_PATIENCE_GENERATIONS)
    parser.add_argument("--saturation-score-delta", type=float, default=DEFAULT_SATURATION_SCORE_DELTA)
    parser.add_argument("--saturation-pnl-delta", type=float, default=DEFAULT_SATURATION_PNL_DELTA)
    parser.add_argument(
        "--research-mode",
        choices=list(RESEARCH_MODE_OPTIONS),
        default=DEFAULT_RESEARCH_MODE,
    )
    parser.add_argument("--require-gate-pass", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.research_mode = resolve_research_mode(args.research_mode)
    
    global FIXED_TAKER_FEE_RATE
    db_lower = str(args.db).lower()
    if "btc" in db_lower or "crypto" in db_lower:
        FIXED_TAKER_FEE_RATE = 0.07
    elif any(frag in db_lower for frag in ["politics", "finance", "mention", "tech"]):
        FIXED_TAKER_FEE_RATE = 0.04
    else:
        FIXED_TAKER_FEE_RATE = 0.07
        
    root = Path(__file__).resolve().parents[1]
    stamp = utc_label()
    out_root = args.output_dir or (root / "xuan_research_artifacts" / f"{ARTIFACT}_{stamp}")
    if out_root.exists():
        if args.force:
            for item in out_root.iterdir():
                if item.name != "runner.log":
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
        else:
            out_root = root / "xuan_research_artifacts" / f"{ARTIFACT}_{stamp}_retry"
            out_root.mkdir(parents=True, exist_ok=True)
    else:
        out_root.mkdir(parents=True, exist_ok=True)

    gate_profile = normalize_profile(load_gate_profile(args.gate_profile))
    if args.gate_profile and not args.gate_profile.exists():
        write_json(args.gate_profile, gate_profile)
    saturation_defaults = as_dict(gate_profile.get("saturation_defaults"))
    if args.max_wall_clock_seconds == DEFAULT_WALL_CLOCK_SECONDS:
        args.max_wall_clock_seconds = as_int(
            saturation_defaults.get("max_wall_clock_seconds"), DEFAULT_WALL_CLOCK_SECONDS
        )
    if args.saturation_patience_generations == DEFAULT_SATURATION_PATIENCE_GENERATIONS:
        args.saturation_patience_generations = as_int(
            saturation_defaults.get("saturation_patience_generations"), DEFAULT_SATURATION_PATIENCE_GENERATIONS
        )
    if args.saturation_score_delta == DEFAULT_SATURATION_SCORE_DELTA:
        args.saturation_score_delta = as_float(
            saturation_defaults.get("saturation_score_delta"), DEFAULT_SATURATION_SCORE_DELTA
        )
    if args.saturation_pnl_delta == DEFAULT_SATURATION_PNL_DELTA:
        args.saturation_pnl_delta = as_float(
            saturation_defaults.get("saturation_pnl_delta"), DEFAULT_SATURATION_PNL_DELTA
        )

    install_manifest = read_json(args.v1_install_manifest)
    strategy_readiness_manifest = read_json(args.v1_strategy_readiness_manifest)
    btc_parity_manifest = read_json(args.v1_btc_parity_manifest)
    repro_gate, repro_blockers = evaluate_reproducibility_gate(
        args=args,
        install_manifest=install_manifest,
        strategy_readiness_manifest=strategy_readiness_manifest,
        parity_manifest=btc_parity_manifest,
        gate_profile=gate_profile,
    )
    write_json(out_root / "reproducibility_gate.json", repro_gate)

    if args.require_gate_pass and repro_gate.get("status") == "BLOCKED":
        print(f"reproducibility gate blocked: {repro_blockers}")
        return 1

    db_path = Path(args.db)
    if not db_path.exists() or not path_is_safe(db_path):
        print(f"db path unavailable or blocked: {db_path}")
        return 1

    resolved_db_path, db_resolution = resolve_backtest_db(
        db_path,
        out_root,
        force=args.force,
        infer_settlement_outcome=args.infer_settlement_outcome,
    )
    if db_resolution.get("status") in {"unsupported", "source_open_failed", "failed"}:
        print(f"cannot resolve db schema: {db_resolution}")
        return 1

    if str(resolved_db_path) != str(db_path):
        print(f"db compatibility shim created: {resolved_db_path}")
    args.db = str(resolved_db_path)

    declaration = evaluate_db_declaration(Path(args.db), args.limit, args.skip)
    declaration["compatibility"] = db_resolution
    if declaration.get("status") != "DECLARED":
        print(f"db declaration failed: {declaration.get('status')}")
        return 1

    baseline = None
    if args.nagi_baseline_manifest:
        baseline = baseline_from_manifest(args.nagi_baseline_manifest)
    elif args.nagi_baseline_pnl is not None:
        baseline = {"pnl": args.nagi_baseline_pnl, "path": "user-provided", "status": "PROVIDED"}
    args.nagi_reference = baseline

    rng = random.Random(args.random_seed)
    # ensure initial_balance flows into every candidate config
    if args.initial_balance:
        initial_balance = args.initial_balance
    else:
        initial_balance = ""

    seed_population = build_seed_population(args.seed_size, rng, research_mode=args.research_mode)
    seed_population = [
        candidate_from_dict({**make_cfg_dict(cfg), "initial_balance": initial_balance})
        for cfg in seed_population
    ]

    all_summaries: list[dict[str, Any]] = []
    all_results: list[dict[str, Any]] = []
    survivors = seed_population[:args.population]

    total_runs = 0
    run_loop_started_at = time.perf_counter()
    saturation_no_gain_generations = 0
    last_best_score: float | None = None
    last_best_pnl: float | None = None
    saturation_stop_reason: str | None = None

    for generation in range(args.generations):
        elapsed_seconds = time.perf_counter() - run_loop_started_at
        if elapsed_seconds >= args.max_wall_clock_seconds:
            saturation_stop_reason = "max_wall_clock_reached"
            break
        if total_runs >= args.max_runs:
            saturation_stop_reason = "max_runs_reached"
            break

        population = survivors[: args.population]
        if not population:
            population = build_seed_population(args.population, rng, research_mode=args.research_mode)
            population = [
                candidate_from_dict({**make_cfg_dict(cfg), "initial_balance": initial_balance})
                for cfg in population
            ]

        results, top, survivors = run_generation(
            generation=generation,
            population=population,
            root=out_root,
            args=args,
            max_workers=min(args.workers, len(population)),
            research_mode=args.research_mode,
        )
        if not results:
            break

        total_runs += len(results)
        generation_summary = {
            "generation": generation,
            "result_count": len(results),
            "top": top,
            "timestamp": utc_label(),
        }
        write_json(
            out_root / f"generation_{generation:02d}" / "manifest_summary.json",
            generation_summary,
        )

        all_summaries.append(generation_summary)
        all_results.extend(results)
        top_gen = summarize_generation(generation, results, args.keep_top, args.nagi_reference)
        current_best_score = as_float(top_gen.get("metrics", {}).get("best_score"), None if top_gen.get("metrics", {}).get("best_score") is None else 0.0)
        current_best_pnl = as_float(top_gen.get("metrics", {}).get("best_pnl"), None if top_gen.get("metrics", {}).get("best_pnl") is None else 0.0)

        if generation > 0:
            last_improved = False
            score_delta = None
            pnl_delta = None
            if last_best_score is not None and current_best_score is not None:
                score_delta = current_best_score - last_best_score
                if score_delta >= args.saturation_score_delta:
                    last_improved = True
            if current_best_pnl is not None and last_best_pnl is not None:
                pnl_delta = current_best_pnl - last_best_pnl
                if pnl_delta >= args.saturation_pnl_delta:
                    last_improved = True
            if not last_improved:
                saturation_no_gain_generations += 1
            else:
                saturation_no_gain_generations = 0

            top_gen["saturation"] = {
                "generation_score_delta": score_delta,
                "generation_pnl_delta": pnl_delta,
                "no_gain_generations": saturation_no_gain_generations,
                "saturation_patience": args.saturation_patience_generations,
            }
            if saturation_no_gain_generations >= args.saturation_patience_generations:
                saturation_stop_reason = "saturation_patience_reached"
                break

        if top_gen.get("metrics") and top_gen["metrics"].get("best_score") is not None:
            last_best_score = top_gen["metrics"]["best_score"]
        if top_gen.get("metrics") and top_gen["metrics"].get("best_pnl") is not None:
            last_best_pnl = top_gen["metrics"]["best_pnl"]

        all_summaries[-1] = top_gen
        write_json(
            out_root / f"generation_{generation:02d}" / "manifest_summary.json",
            top_gen,
        )

        if generation < args.generations - 1 and total_runs < args.max_runs:
            survivors = next_generation(
                survivors=survivors,
                generation=generation,
                population=args.population,
                rng=rng,
                exploration=args.exploration,
                research_mode=args.research_mode,
            )

    all_results.sort(key=lambda row: row.get("score", -1e99), reverse=True)
    best = all_results[0] if all_results else None
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": stamp,
        "status": "COMPLETED" if all_results else "FAILED",
        "reproducibility_gate_profile": gate_profile,
        "reproducibility_gate": repro_gate,
        "db": str(args.db),
        "output_root": str(out_root),
        "db_declaration": declaration,
        "run_args": {
            "resolved_db": str(args.db),
            "db_compatibility": db_resolution,
            "v1_install_manifest": str(args.v1_install_manifest),
            "v1_install_manifest_reference": str(args.v1_install_manifest_reference) if args.v1_install_manifest_reference else None,
            "v1_strategy_readiness_manifest": str(args.v1_strategy_readiness_manifest) if args.v1_strategy_readiness_manifest else None,
            "v1_btc_parity_manifest": str(args.v1_btc_parity_manifest) if args.v1_btc_parity_manifest else None,
            "gate_profile": str(args.gate_profile),
            "require_gate_pass": args.require_gate_pass,
            "expected_search_safe_row_count": args.expected_search_safe_row_count,
            "expected_canonical_audit_selected_candidate_count": args.expected_canonical_audit_selected_candidate_count,
            "generations": args.generations,
            "population": args.population,
            "seed_size": args.seed_size,
            "keep_top": args.keep_top,
            "exploration": args.exploration,
            "workers": args.workers,
            "limit": args.limit,
            "skip": args.skip,
            "random_seed": args.random_seed,
            "initial_balance": args.initial_balance,
            "max_runs": args.max_runs,
            "research_mode": args.research_mode,
        },
        "baseline_reference": baseline,
        "run_count": len(all_results),
        "generations_run": len(all_summaries),
        "saturation": {
            "patience_generations": args.saturation_patience_generations,
            "score_delta": args.saturation_score_delta,
            "pnl_delta": args.saturation_pnl_delta,
            "no_gain_generations_at_stop": saturation_no_gain_generations,
            "stopped_early_reason": saturation_stop_reason,
        },
        "generation_summaries": all_summaries,
        "best_candidate": best,
        "best_by_score": best.get("score") if best else None,
        "best_by_pnl": best.get("summary", {}).get("total_pnl") if best else None,
        "baseline_beats": None,
        "next_action": "run_followup_generation_or_increase_constraints" if best else "adjust_search_bounds_or_fix_db",
    }
    manifest["run_args"].update(
        {
            "max_wall_clock_seconds": args.max_wall_clock_seconds,
            "saturation_patience_generations": args.saturation_patience_generations,
            "saturation_score_delta": args.saturation_score_delta,
            "saturation_pnl_delta": args.saturation_pnl_delta,
        }
    )
    manifest["research_mode"] = args.research_mode
    manifest["strategy_objective"] = (
        "Objective = pairing_net_pnl - max(0,residual_cost)-completion_fee, "
        "with participation and pair-cost as secondary terms"
    )

    if saturation_stop_reason:
        manifest["next_action"] = "pause_and_promote_saturated_topology" if best else "investigate_stagnation"

    if baseline and best:
        manifest["baseline_beats"] = {
            "reference_pnl": as_float(baseline.get("pnl"), 0.0),
            "best_pnl": as_float(best.get("summary", {}).get("total_pnl"), 0.0),
            "delta_pnl": as_float(best.get("summary", {}).get("total_pnl"), 0.0)
            - as_float(baseline.get("pnl"), 0.0),
        }

    (out_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (out_root / "results.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "candidate_id": row.get("candidate_id"),
                    "generation": row.get("generation"),
                    "score": row.get("score"),
                    "status": row.get("status"),
                    "pnl": (row.get("summary", {}) or {}).get("total_pnl"),
                    "pairing_net_pnl": (row.get("summary", {}) or {}).get("pairing_net_pnl"),
                    "pair_cost": (row.get("summary", {}) or {}).get("weighted_avg_pair_cost"),
                    "fills": (row.get("summary", {}) or {}).get("fills"),
                    "residual_cost": (row.get("summary", {}) or {}).get("residual_cost"),
                    "completion_fee": (row.get("summary", {}) or {}).get("completion_fee"),
                    "run_dir": row.get("run_dir"),
                },
                sort_keys=True,
            )
            for row in all_results
        )
        + "\n"
    )

    if best:
        best_score = best.get("summary", {}).get("total_pnl")
        print(f"best_candidate={best.get('candidate_id')} score={best.get('score')} pnl={best_score}")
        print(f"research_mode={args.research_mode}")
        print(f"manifest={out_root / 'manifest.json'}")
        return 0 if best.get("status") == "OK" else 1
    print("No successful candidate found.")
    print(f"manifest={out_root / 'manifest.json'}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
