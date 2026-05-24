#!/usr/bin/env python3
"""Specify projected pair-cost/residual guards from ce25/b55 public buckets.

This local-only script replays the existing public/deep-compare diagnostic
bucket results, then converts them into default-off projected guard contracts.
It does not fetch data, start services, run shadow, or touch trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ARTIFACT = "xuan_ce25_projected_pair_cost_residual_guard_spec"
DEFAULT_DEEP_COMPARE_DIR = Path(
    "/Users/hot/web3Scientist/poly_trans_research/data/exports/"
    "candidate_deep_compare_b55_ce25_20260523_103000_to_20260524_103000_bjt"
)
DEFAULT_RULE_REPORT = DEFAULT_DEEP_COMPARE_DIR / "strategy_rules_report.md"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)
REQUIRED_FILES = (
    "strategy_rules_report.md",
    "post_window_by_condition.csv",
    "ce25/market_cash_pnl.csv",
    "b55/market_cash_pnl.csv",
)
ACCOUNTS = ("ce25", "b55")
CORE_ASSETS = {"BTC", "ETH", "SOL"}
STARTER_ASSETS = {"ETH", "SOL"}
PAIR_TFS = {"5m", "15m"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_timeframe(slug: str, fallback: str) -> str:
    lower = (slug or "").lower()
    if "updown-15m" in lower:
        return "15m"
    if "updown-5m" in lower:
        return "5m"
    if "updown-4h" in lower:
        return "4h"
    if "up-or-down" in lower:
        return "1h_or_named"
    return fallback or "unknown"


def missing_required_files(deep_compare_dir: Path) -> list[str]:
    return [name for name in REQUIRED_FILES if not (deep_compare_dir / name).exists()]


def load_joined_rows(deep_compare_dir: Path, account: str) -> list[dict[str, Any]]:
    market_rows = {
        row["condition_id"]: row
        for row in read_csv_rows(deep_compare_dir / account / "market_cash_pnl.csv")
    }
    joined: list[dict[str, Any]] = []
    for row in read_csv_rows(deep_compare_dir / "post_window_by_condition.csv"):
        if row.get("label") != account:
            continue
        market = market_rows.get(row.get("condition_id", ""))
        if not market:
            continue
        parsed_tf = parse_timeframe(row.get("slug", ""), row.get("tf", ""))
        joined.append(
            {
                "account": account,
                "condition_id": row.get("condition_id"),
                "slug": row.get("slug"),
                "asset": row.get("asset"),
                "timeframe": parsed_tf,
                "cohort_cash": to_float(row.get("cohort_cash")),
                "actual_pair_cost": to_float(market.get("actual_pair_cost")),
                "residual_rate": to_float(market.get("resid_rate")),
                "buy_actual": to_float(market.get("buy_actual")),
                "pair_pnl": to_float(market.get("pair_pnl")),
                "residual_pnl_est": to_float(market.get("residual_pnl_est")),
            }
        )
    return joined


def summarize_rule(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = sum(row["cohort_cash"] for row in rows)
    wins = sum(1 for row in rows if row["cohort_cash"] > 0)
    losses = sum(1 for row in rows if row["cohort_cash"] < 0)
    pair_pnl = sum(row["pair_pnl"] for row in rows)
    residual = sum(row["residual_pnl_est"] for row in rows)
    return {
        "markets": len(rows),
        "cohort_pnl": round(pnl, 6),
        "wins": wins,
        "losses": losses,
        "buy_actual": round(sum(row["buy_actual"] for row in rows), 6),
        "pair_pnl": round(pair_pnl, 6),
        "residual_pnl_est": round(residual, 6),
        "avg_actual_pair_cost": round(
            sum(row["actual_pair_cost"] for row in rows) / len(rows), 6
        )
        if rows
        else None,
        "avg_residual_rate": round(sum(row["residual_rate"] for row in rows) / len(rows), 6)
        if rows
        else None,
    }


def rule_predicates() -> dict[str, Callable[[dict[str, Any]], bool]]:
    return {
        "all_markets": lambda row: True,
        "starter_eth_sol_5m15m_pair_cost_lt_090_residual_lt_15": lambda row: (
            row["asset"] in STARTER_ASSETS
            and row["timeframe"] in PAIR_TFS
            and row["actual_pair_cost"] < 0.90
            and row["residual_rate"] < 0.15
        ),
        "core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10": lambda row: (
            row["asset"] in CORE_ASSETS
            and row["timeframe"] in PAIR_TFS
            and row["actual_pair_cost"] < 0.95
            and row["residual_rate"] < 0.10
        ),
        "diagnostic_btc_eth_sol_5m15m_pair_cost_lt_098_residual_lt_10": lambda row: (
            row["asset"] in CORE_ASSETS
            and row["timeframe"] in PAIR_TFS
            and row["actual_pair_cost"] < 0.98
            and row["residual_rate"] < 0.10
        ),
        "hard_loss_btc_eth_sol_5m15m_pair_cost_ge_100": lambda row: (
            row["asset"] in CORE_ASSETS
            and row["timeframe"] in PAIR_TFS
            and row["actual_pair_cost"] >= 1.00
        ),
    }


def compute_rule_buckets(deep_compare_dir: Path) -> dict[str, Any]:
    predicates = rule_predicates()
    out: dict[str, Any] = {}
    for account in ACCOUNTS:
        rows = load_joined_rows(deep_compare_dir, account)
        out[account] = {}
        for name, predicate in predicates.items():
            selected = [row for row in rows if predicate(row)]
            out[account][name] = summarize_rule(selected)
    return out


def parse_report_assertions(report_text: str) -> dict[str, Any]:
    return {
        "report_exists": bool(report_text),
        "mentions_diagnostic_not_direct_replay": "diagnostic, not a direct historical fill replay" in report_text,
        "mentions_projected_pair_cost": "projected pair cost" in report_text,
        "mentions_pair_cost_hard_loss": "pair_cost >= 1.00" in report_text,
        "mentions_final_60s": "final 60 seconds" in report_text,
        "mentions_ce25_template": "ce25 is cleaner and easier to replicate" in report_text,
    }


def projected_guard_contracts() -> dict[str, Any]:
    required_pre_action_fields = [
        "condition_id",
        "asset",
        "timeframe",
        "market_start_ts",
        "market_end_ts",
        "now_ts",
        "seconds_to_expiry",
        "action_intent",
        "outcome",
        "order_qty",
        "order_price",
        "estimated_fee_per_share",
        "pre_yes_qty",
        "pre_no_qty",
        "pre_yes_actual_cost",
        "pre_no_actual_cost",
        "projected_yes_qty",
        "projected_no_qty",
        "projected_yes_actual_cost",
        "projected_no_actual_cost",
        "projected_pair_qty",
        "projected_pair_cost",
        "projected_residual_qty",
        "projected_total_bought_qty",
        "projected_residual_rate_on_bought_qty",
    ]
    formulae = {
        "projected_pair_qty": "min(projected_yes_qty, projected_no_qty)",
        "projected_pair_cost": (
            "cost allocated to the paired YES qty plus cost allocated to the paired NO qty, divided by "
            "projected_pair_qty; fail closed if paired cost allocation cannot be computed pre-action"
        ),
        "projected_residual_qty": "abs(projected_yes_qty - projected_no_qty)",
        "projected_residual_rate_on_bought_qty": (
            "projected_residual_qty / max(projected_total_bought_qty, epsilon); this mirrors public residual/buy_qty "
            "diagnostics and is not a price prediction"
        ),
        "completion_or_cleanup_action": (
            "action increases projected_pair_qty or reduces projected_residual_rate_on_bought_qty without worsening "
            "projected_pair_cost past the active cap"
        ),
    }
    return {
        "contract_name": "ce25_projected_pair_cost_residual_guard_v1",
        "default_off": True,
        "purpose": "Translate retrospective low-pair-cost/low-residual buckets into pre-action guardrails.",
        "required_pre_action_fields": required_pre_action_fields,
        "formulae": formulae,
        "starter_guard": {
            "assets": ["ETH", "SOL"],
            "timeframes": ["5m", "15m"],
            "projected_pair_cost_lt": 0.90,
            "projected_residual_rate_lt": 0.15,
            "no_initiation_final_seconds": 60,
            "final_1m_to_5m_policy": "completion_or_cleanup_only",
            "status": "starter_research_contract_not_live_strategy",
        },
        "core_guard": {
            "assets": ["BTC", "ETH", "SOL"],
            "excluded_assets": ["XRP"],
            "timeframes": ["5m", "15m"],
            "projected_pair_cost_lt": 0.95,
            "projected_residual_rate_lt": 0.10,
            "no_initiation_final_seconds": 60,
            "final_1m_to_5m_policy": "completion_or_cleanup_only",
            "status": "core_research_contract_not_live_strategy",
        },
        "hard_kill_rules": [
            {
                "rule": "projected_pair_cost_gte_1_00",
                "action": "block_new_or_additive_entry; allow only risk-reducing cleanup if it lowers residual",
            },
            {
                "rule": "projected_residual_rate_gt_20pct_near_close",
                "near_close_seconds": 300,
                "action": "block additive exposure and require residual-reducing plan before further scale",
            },
            {
                "rule": "single_market_large_unpaired_into_resolution",
                "action": "fail closed until projected residual path is below active cap",
            },
        ],
        "explicit_non_goals": [
            "not a pure Polymarket price-band cap",
            "not static asset/timeframe deletion",
            "not D+ micro-deficit/ledger/tiny-deficit/closed-cycle/fill-to-balance revival",
            "not fixture fair probability",
            "not private truth, deployability, canary, or promotion evidence",
        ],
        "fail_closed_conditions": [
            "any required pre-action field missing",
            "projected_pair_cost cannot be computed before order",
            "projected_residual_rate cannot be computed before order",
            "order is an initiation in final 60 seconds",
            "final 1-5 minute action is not completion_or_cleanup",
            "liquidity role or fair-probability is claimed from maker rebate, fee magnitude, account-level taker share, or fixture fields",
        ],
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(args.deep_compare_dir) or not path_safe(args.rule_report) or not path_safe(output_dir):
        raise RuntimeError("unsafe path under ce25 projected guard spec")
    missing = missing_required_files(args.deep_compare_dir)
    scope = {
        "local_only": True,
        "existing_public_exports_only": True,
        "new_data_fetch": False,
        "ssh_used": False,
        "shadow_started": False,
        "canary_started": False,
        "local_agg_started": False,
        "shared_ws_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "full_completion_store_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "trading_behavior_changed": False,
    }
    if missing:
        return {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": utc_label(),
            "lane": "ce25_projected_pair_cost_residual_guard_spec",
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_CE25_PROJECTED_GUARD_SPEC_INPUTS_MISSING",
            "scope": scope,
            "source_inputs": {
                "deep_compare_dir": str(args.deep_compare_dir),
                "rule_report": str(args.rule_report),
                "missing_required_files": missing,
            },
            "research_ranking": {
                "spec_ready": False,
                "promotion_gate_passed": False,
                "private_truth_ready": False,
                "deployable": False,
            },
            "promotion_gate": {
                "passed": False,
                "private_truth_ready": False,
                "deployable": False,
                "g2_canary_ready": False,
                "status": "INPUTS_MISSING_NOT_PROMOTION_EVIDENCE",
            },
            "next_executable_action": "Rerun on complete existing public/deep-compare exports; do not start diagnostics.",
        }
    report_text = args.rule_report.read_text(encoding="utf-8", errors="replace")
    buckets = compute_rule_buckets(args.deep_compare_dir)
    report_assertions = parse_report_assertions(report_text)
    ce25_core = buckets["ce25"]["core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10"]
    ce25_starter = buckets["ce25"]["starter_eth_sol_5m15m_pair_cost_lt_090_residual_lt_15"]
    ce25_hard_loss = buckets["ce25"]["hard_loss_btc_eth_sol_5m15m_pair_cost_ge_100"]
    b55_core = buckets["b55"]["core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10"]
    bucket_checks = {
        "ce25_starter_positive": ce25_starter["cohort_pnl"] > 0 and ce25_starter["losses"] == 0,
        "ce25_core_positive": ce25_core["cohort_pnl"] > 0 and ce25_core["losses"] <= 2,
        "b55_core_positive_control": b55_core["cohort_pnl"] > 0,
        "ce25_hard_loss_negative": ce25_hard_loss["cohort_pnl"] < 0 and ce25_hard_loss["losses"] > ce25_hard_loss["wins"],
        "report_says_diagnostic_not_replay": report_assertions["mentions_diagnostic_not_direct_replay"],
        "report_says_projected_guardrails": report_assertions["mentions_projected_pair_cost"],
    }
    spec_ready = all(bucket_checks.values())
    decision = "KEEP" if spec_ready else "UNKNOWN"
    label = (
        "KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_READY"
        if spec_ready
        else "UNKNOWN_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_BUCKETS_INSUFFICIENT"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_projected_pair_cost_residual_guard_spec",
        "decision": decision,
        "decision_label": label,
        "scope": scope,
        "source_inputs": {
            "deep_compare_dir": str(args.deep_compare_dir),
            "rule_report": str(args.rule_report),
            "deep_compare_files": list(REQUIRED_FILES),
        },
        "report_assertions": report_assertions,
        "retrospective_rule_buckets": buckets,
        "bucket_checks": bucket_checks,
        "projected_guard_contract": projected_guard_contracts(),
        "research_ranking": {
            "decision": decision,
            "label": label,
            "spec_ready": spec_ready,
            "preferred_template": "ce25",
            "starter_rule": "ETH/SOL 5m+15m projected_pair_cost<0.90 projected_residual<15%",
            "core_rule": "BTC/ETH/SOL 5m+15m projected_pair_cost<0.95 projected_residual<10%",
            "hard_kill": "projected_pair_cost>=1.00 or residual>20% near close",
            "retrospective_bucket_warning": (
                "The public buckets are realized diagnostics. Live use requires projected pre-action pair cost and "
                "projected residual; realized pair_cost/residual must never be used as live criteria."
            ),
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "promotion_gate": {
            "passed": False,
            "status": "PROJECTED_GUARD_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [
                "projected guard not wired into any no-order runner",
                "no authenticated owner order/fill/queue truth",
                "no non-fixture liquidity role join",
                "no fair-probability/uncertainty/edge join",
                "public retrospective buckets are not private truth",
            ],
        },
        "next_executable_action": (
            "Implement ce25_projected_guard_fixture_scorer_v1 local-only: feed synthetic pre-action state/order "
            "fixtures through the projected_pair_cost/projected_residual formulae, prove starter/core/hard-kill "
            "decisions fail closed, and keep promotion_gate.passed=false."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deep-compare-dir", type=Path, default=DEFAULT_DEEP_COMPARE_DIR)
    parser.add_argument("--rule-report", type=Path, default=DEFAULT_RULE_REPORT)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
