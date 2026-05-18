#!/usr/bin/env python3
"""Run a local no-network D+ pair-arb backtest grid and publish metrics.

This script only invokes the local Rust ``pair_arb_backtest`` binary against an
explicit snapshot DB path and writes artifacts under ``xuan_research_artifacts``.
It does not read raw/replay stores, connect to shared-ingress, or touch any
order/cancel/redeem path.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_pair_arb_backtest_json_grid"
DEFAULT_DB = "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db"
PNL_EPS = 1e-9
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
)
EXCLUDED_BACKTEST_DAYS = {"20260514", "20260515"}
NOT_READY_BACKTEST_DAYS = {"20260518"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def unique_output_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"failed to allocate unique output dir under {path.parent}")


def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def sqlite_scalar(conn: sqlite3.Connection, query: str) -> Any:
    row = conn.execute(query).fetchone()
    return row[0] if row else None


def backtest_data_declaration(db_path: Path, args: argparse.Namespace, rows: list[dict[str, Any]]) -> dict[str, Any]:
    declaration: dict[str, Any] = {
        "data_root": str(db_path),
        "dataset_type": "local_sqlite_snapshot_btc5m",
        "labels": [db_path.name],
        "days": [],
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": 0,
        "selected_window_count": max((metric(row, "windows") for row in rows), default=0.0),
        "excluded_20260514_20260515": True,
        "contains_20260514_20260515": False,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": False,
        "can_support_strategy_promotion": False,
        "conclusion_scope": "BTC local SQLite snapshot research only; not full/current strict-cache/completion/account-truth evidence",
    }
    if not db_path.exists() or not path_is_safe(db_path):
        declaration["status"] = "UNAVAILABLE"
        declaration["can_support_strategy_promotion"] = False
        return declaration
    try:
        with sqlite3.connect(str(db_path)) as conn:
            days = [
                str(row[0])
                for row in conn.execute(
                    "select distinct strftime('%Y%m%d', ts, 'unixepoch') from market_ticks order by 1"
                )
                if row[0]
            ]
            table_count = sqlite_scalar(
                conn,
                "select count(*) from sqlite_master where type='table' and name='public_account_execution_events'",
            )
            declaration.update(
                {
                    "status": "DECLARED",
                    "days": days,
                    "day_count": len(days),
                    "row_count": int(sqlite_scalar(conn, "select count(*) from market_ticks") or 0),
                    "condition_count": int(
                        sqlite_scalar(conn, "select count(distinct condition_id) from market_ticks")
                        or 0
                    ),
                    "min_ts": sqlite_scalar(conn, "select min(ts) from market_ticks"),
                    "max_ts": sqlite_scalar(conn, "select max(ts) from market_ticks"),
                    "contains_20260514_20260515": bool(EXCLUDED_BACKTEST_DAYS & set(days)),
                    "excluded_20260514_20260515": not bool(EXCLUDED_BACKTEST_DAYS & set(days)),
                    "contains_20260518": bool(NOT_READY_BACKTEST_DAYS & set(days)),
                    "includes_public_account_execution_truth_v1": bool(table_count),
                }
            )
    except sqlite3.Error as exc:
        declaration["status"] = "ERROR"
        declaration["error"] = str(exc)
    declaration["can_support_strategy_promotion"] = (
        False
    )
    declaration["scope_limitations"] = [
        "not strict-v2 taker-buy cache",
        "not completion/unwind event store v2",
        "not public_account_execution_truth_v1",
        "does not cover current approved 20260502..13/20260516/20260517 store labels",
    ]
    declaration["reporting_rule"] = (
        "Conclusions from this artifact must be described as BTC local SQLite snapshot research only, "
        "not full-data, multi-asset, account-truth, deployable, or canary-ready evidence."
    )
    declaration["requested_limit"] = args.limit
    declaration["requested_skip"] = args.skip
    return declaration


def parse_jsonl(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("kind") == "pair_arb_backtest_run":
            rows.append(value)
    return rows


def run_summary(row: dict[str, Any]) -> dict[str, Any]:
    cfg = row.get("config") or {}
    metrics = row.get("metrics") or {}
    return {
        "run": row.get("run"),
        "net": cfg.get("max_net_diff"),
        "target": cfg.get("pair_target"),
        "bid": cfg.get("bid_size"),
        "tier": f"{cfg.get('tier_1_mult')}/{cfg.get('tier_2_mult')}",
        "mode": cfg.get("tier_mode"),
        "cutoff": cfg.get("risk_open_cutoff_secs"),
        "margin": cfg.get("pair_cost_safety_margin"),
        "salvage_cap": cfg.get("salvage_net_cap"),
        "salvage_start": cfg.get("salvage_start_remaining_secs"),
        "taker_fee": cfg.get("taker_fee_rate"),
        "directional_risk_filter_bps": cfg.get("directional_risk_filter_bps"),
        "directional_entry_min_bps": cfg.get("directional_entry_min_bps"),
        "directional_price_source": cfg.get("directional_price_source"),
        "reject_stale": cfg.get("reject_stale"),
        "require_ws_fresh": cfg.get("require_ws_fresh"),
        "max_quote_age_secs": cfg.get("max_quote_age_secs"),
        "min_ask_depth": cfg.get("min_ask_depth"),
        "entry_pair_max_ask_sum": cfg.get("entry_pair_max_ask_sum"),
        "require_two_sided_entry": cfg.get("require_two_sided_entry"),
        "pairing_only_when_residual": cfg.get("pairing_only_when_residual"),
        "bal": cfg.get("initial_balance"),
        "fill": cfg.get("fill_model"),
        "windows": metrics.get("windows"),
        "filled_windows": metrics.get("filled_windows"),
        "paired_windows": metrics.get("paired_windows"),
        "fills": metrics.get("fills"),
        "completion_fills": metrics.get("completion_fills"),
        "completion_qty": metrics.get("completion_qty"),
        "completion_cost": metrics.get("completion_cost"),
        "completion_fee": metrics.get("completion_fee"),
        "paired_qty": metrics.get("paired_qty"),
        "paired_pnl": metrics.get("paired_pnl"),
        "residual_qty": metrics.get("residual_qty"),
        "residual_cost": metrics.get("residual_cost"),
        "residual_pnl": metrics.get("residual_pnl"),
        "pnl": metrics.get("total_pnl"),
        "avg": metrics.get("avg_pnl_per_window"),
        "weighted_avg_pair_cost": metrics.get("weighted_avg_pair_cost"),
        "avg_pair_qty_per_window": metrics.get("avg_pair_qty_per_window"),
        "avg_pair_qty_per_paired_window": metrics.get("avg_pair_qty_per_paired_window"),
        "avg_fills_per_filled_window": metrics.get("avg_fills_per_filled_window"),
        "avg_residual_qty_per_window": metrics.get("avg_residual_qty_per_window"),
        "avg_residual_cost_per_window": metrics.get("avg_residual_cost_per_window"),
        "fill_window_rate": metrics.get("fill_window_rate"),
        "paired_window_rate": metrics.get("paired_window_rate"),
        "residual_window_rate": metrics.get("residual_window_rate"),
        "residual_loss_rate": (
            100.0 * float(metrics.get("residual_loss_rate") or 0.0)
        ),
        "win_loss_zero": f"{metrics.get('wins')}/{metrics.get('losses')}/{metrics.get('zeros')}",
        "avg_win": metrics.get("avg_win"),
        "avg_loss": metrics.get("avg_loss"),
    }


def metric(row: dict[str, Any], name: str) -> float:
    value = (row.get("metrics") or {}).get(name)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--skip", type=int, default=0)
    parser.add_argument("--max-net-diff", default="0.1,0.2,0.25,0.5")
    parser.add_argument("--pair-target", default="0.75,0.80,0.85")
    parser.add_argument("--bid-size", default="0.05,0.10,0.20")
    parser.add_argument("--tier1", default="0.50")
    parser.add_argument("--tier2", default="0.15")
    parser.add_argument("--tier-mode", default="disabled,continuous")
    parser.add_argument("--fill-model", default="conservative")
    parser.add_argument("--cutoff", default="180,240")
    parser.add_argument("--margin", default="0.00,0.01,0.02")
    parser.add_argument("--salvage-net-cap", default="0.00,0.90,0.95,1.00")
    parser.add_argument("--salvage-start-remaining", default="240,300")
    parser.add_argument("--taker-fee-rate", default="0.00,0.07")
    parser.add_argument("--directional-risk-filter-bps", default="0.00")
    parser.add_argument("--directional-entry-min-bps", default="0.00")
    parser.add_argument("--directional-price-source", default="price")
    parser.add_argument("--reject-stale", action="store_true")
    parser.add_argument("--require-ws-fresh", action="store_true")
    parser.add_argument("--max-quote-age-sec", type=float, default=0.0)
    parser.add_argument("--min-ask-depth", type=float, default=0.0)
    parser.add_argument("--entry-pair-max-ask-sum", default="0.00")
    parser.add_argument("--require-two-sided-entry", action="store_true")
    parser.add_argument("--pairing-only-when-residual", action="store_true")
    parser.add_argument("--initial-balance")
    parser.add_argument(
        "--validation-scope",
        choices=["exploratory", "train", "oos"],
        default="exploratory",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or unique_output_dir(
        root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db)

    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--bin",
        "pair_arb_backtest",
        "--",
        "--jsonl",
        "--db",
        str(db_path),
        "--limit",
        str(args.limit),
        "--skip",
        str(args.skip),
        "--max-net-diff",
        args.max_net_diff,
        "--pair-target",
        args.pair_target,
        "--bid-size",
        args.bid_size,
        "--tier1",
        args.tier1,
        "--tier2",
        args.tier2,
        "--tier-mode",
        args.tier_mode,
        "--fill-model",
        args.fill_model,
        "--cutoff",
        args.cutoff,
        "--margin",
        args.margin,
        "--salvage-net-cap",
        args.salvage_net_cap,
        "--salvage-start-remaining",
        args.salvage_start_remaining,
        "--taker-fee-rate",
        args.taker_fee_rate,
        "--directional-risk-filter-bps",
        args.directional_risk_filter_bps,
        "--directional-entry-min-bps",
        args.directional_entry_min_bps,
        "--directional-price-source",
        args.directional_price_source,
        "--entry-pair-max-ask-sum",
        args.entry_pair_max_ask_sum,
    ]
    if args.reject_stale:
        cmd.append("--reject-stale")
    if args.require_ws_fresh:
        cmd.append("--require-ws-fresh")
    if args.max_quote_age_sec > 0:
        cmd.extend(["--max-quote-age-sec", str(args.max_quote_age_sec)])
    if args.min_ask_depth > 0:
        cmd.extend(["--min-ask-depth", str(args.min_ask_depth)])
    if args.require_two_sided_entry:
        cmd.append("--require-two-sided-entry")
    if args.pairing_only_when_residual:
        cmd.append("--pairing-only-when-residual")
    if args.initial_balance:
        cmd.extend(["--initial-balance", args.initial_balance])

    (out_dir / "command.json").write_text(json.dumps(cmd, indent=2) + "\n")
    safe_db = path_is_safe(db_path)
    proc = subprocess.run(
        cmd,
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    (out_dir / "backtest_stdout.jsonl").write_text(proc.stdout)
    (out_dir / "backtest_stderr.log").write_text(proc.stderr)
    rows = parse_jsonl(proc.stdout)
    data_declaration = backtest_data_declaration(db_path, args, rows)

    nonzero_rows = [row for row in rows if metric(row, "fills") > 0]
    positive_nonzero_rows = [
        row for row in nonzero_rows if metric(row, "total_pnl") > PNL_EPS
    ]
    best_nonzero = (
        max(nonzero_rows, key=lambda row: metric(row, "total_pnl"))
        if nonzero_rows
        else None
    )
    best_positive = (
        max(positive_nonzero_rows, key=lambda row: metric(row, "total_pnl"))
        if positive_nonzero_rows
        else None
    )
    lowest_residual_loss = (
        min(
            nonzero_rows,
            key=lambda row: (
                metric(row, "residual_loss_rate"),
                -metric(row, "total_pnl"),
            ),
        )
        if nonzero_rows
        else None
    )
    best_pair_cost = (
        min(
            nonzero_rows,
            key=lambda row: (
                metric(row, "weighted_avg_pair_cost") or 999.0,
                -metric(row, "total_pnl"),
            ),
        )
        if nonzero_rows
        else None
    )
    top_by_pnl = sorted(
        (run_summary(row) for row in nonzero_rows),
        key=lambda item: float(item.get("pnl") or 0.0),
        reverse=True,
    )[:10]

    positive_found = bool(positive_nonzero_rows and proc.returncode == 0 and safe_db)
    promotion_eligible = data_declaration.get("can_support_strategy_promotion") is True
    if positive_found and args.validation_scope == "oos" and promotion_eligible:
        status = "PASS_OOS_POSITIVE_BACKTEST_CONFIG"
    elif positive_found and args.validation_scope == "oos":
        status = "PASS_OOS_POSITIVE_BACKTEST_CONFIG_DATASET_SCOPE_LIMITED"
    elif positive_found and args.validation_scope == "train":
        status = "PASS_TRAIN_POSITIVE_BACKTEST_CONFIG_REQUIRES_OOS"
    elif positive_found:
        status = "PASS_EXPLORATORY_POSITIVE_BACKTEST_CONFIG_REQUIRES_OOS"
    else:
        status = "FAIL_NO_POSITIVE_BACKTEST_CONFIG"
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": label,
        "scope": "local_no_network_pair_arb_backtest_json_grid",
        "status": status,
        "db": str(db_path),
        "limit": args.limit,
        "skip": args.skip,
        "validation_scope": args.validation_scope,
        "backtest_data_declaration": data_declaration,
        "data_root": data_declaration.get("data_root"),
        "dataset_type": data_declaration.get("dataset_type"),
        "labels": data_declaration.get("labels"),
        "days": data_declaration.get("days"),
        "market_prefix": data_declaration.get("market_prefix"),
        "assets": data_declaration.get("assets"),
        "row_count": data_declaration.get("row_count"),
        "excluded_20260514_20260515": data_declaration.get("excluded_20260514_20260515"),
        "contains_20260514_20260515": data_declaration.get("contains_20260514_20260515"),
        "contains_20260518": data_declaration.get("contains_20260518"),
        "includes_public_account_execution_truth_v1": data_declaration.get(
            "includes_public_account_execution_truth_v1"
        ),
        "can_support_strategy_promotion": data_declaration.get(
            "can_support_strategy_promotion"
        ),
        "dataset_scope_blocks_strategy_promotion": data_declaration.get(
            "dataset_scope_blocks_strategy_promotion"
        ),
        "conclusion_scope": data_declaration.get("conclusion_scope"),
        "reporting_rule": data_declaration.get("reporting_rule"),
        "returncode": proc.returncode,
        "run_count": len(rows),
        "nonzero_fill_run_count": len(nonzero_rows),
        "positive_nonzero_run_count": len(positive_nonzero_rows),
        "numeric_oos_validation_passed": positive_found and args.validation_scope == "oos",
        "requires_oos_validation": positive_found
        and (
            args.validation_scope != "oos"
            or data_declaration.get("can_support_strategy_promotion") is not True
        ),
        "oos_validation_passed": positive_found
        and args.validation_scope == "oos"
        and data_declaration.get("can_support_strategy_promotion") is True,
        "dataset_scope_blocks_strategy_promotion": (
            data_declaration.get("can_support_strategy_promotion") is not True
        ),
        "best_positive_run": run_summary(best_positive) if best_positive else None,
        "best_nonzero_fill_run": run_summary(best_nonzero) if best_nonzero else None,
        "lowest_residual_loss_nonzero_fill_run": (
            run_summary(lowest_residual_loss) if lowest_residual_loss else None
        ),
        "lowest_pair_cost_nonzero_fill_run": (
            run_summary(best_pair_cost) if best_pair_cost else None
        ),
        "top_nonzero_by_pnl": top_by_pnl,
        "stdout": "backtest_stdout.jsonl",
        "stderr": "backtest_stderr.log",
        "command": "command.json",
        "side_effects": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "raw_replay_scanned": False,
            "raw_replay_written": False,
        },
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "raw_replay_scanned": False,
        "raw_replay_written": False,
        "safety": {
            "db_path_safe": safe_db,
            "local_only": True,
            "no_network_required": True,
        },
    }
    if proc.returncode != 0:
        manifest["status"] = "FAIL_BACKTEST_COMMAND"
    if not safe_db:
        manifest["status"] = "FAIL_UNSAFE_DB_PATH"

    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if manifest["status"] != "FAIL_BACKTEST_COMMAND" else 1


if __name__ == "__main__":
    raise SystemExit(main())
