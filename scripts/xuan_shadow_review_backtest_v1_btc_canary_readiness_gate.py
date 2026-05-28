#!/usr/bin/env python3
"""Assess BTC-only readiness for a future live-small canary.

This gate narrows the Backtest V1 low-residual work to BTC only and separates
"research edge is good enough" from "safe to run a real owner canary".  It does
not import candidates, build a runner profile, run remote jobs, deploy, or send
orders.  It also emits a concrete request for the backtest/data colleague.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any


STAMP = "20260528T0922Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

DEFAULT_STRATEGY_REVIEW = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_strategy_readiness_review_20260528T0754Z.json"
)
DEFAULT_FILTER_PACKAGE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_low_residual_filter_package_20260528T0834Z.json"
)
DEFAULT_SOURCE_CONTRACT = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_source_parity_private_truth_contract_20260528T0904Z.json"
)
DEFAULT_BTC_PARITY = CONTRACT_ROOT / "backtest_v1_btc_parity_latest/BACKTEST_V1_BTC_PARITY_GATE.json"
DEFAULT_CAPITAL_LEDGER = CONTRACT_ROOT / "xuan_capital_ledger_latest/xuan_capital_ledger.duckdb"
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_btc_canary_readiness_gate_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_canary_readiness_gate_{STAMP}"
)

BTC_FILTER_NAME = "low_residual_core_pair_v1"


def fnum(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: clean(val) for key, val in row.items()})


def aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    gross = sum(fnum(row.get("gross_buy_cost")) for row in rows)
    core = sum(fnum(row.get("core_pair_after_fee_pnl")) for row in rows)
    residual_cost = sum(fnum(row.get("market_end_residual_cost")) for row in rows)
    days = sorted({str(row.get("day")) for row in rows if row.get("day")})
    markets = sorted({str(row.get("condition_id")) for row in rows if row.get("condition_id")})
    return {
        "asset": "BTC",
        "candidate_rows": len(rows),
        "day_count": len(days),
        "market_count": len(markets),
        "gross_buy_cost": gross,
        "pair_pnl": sum(fnum(row.get("pair_pnl")) for row in rows),
        "official_taker_fee": sum(fnum(row.get("official_taker_fee")) for row in rows),
        "core_pair_after_fee_pnl": core,
        "core_pair_after_fee_roi": core / gross if gross else 0.0,
        "actual_settlement_residual_pnl_posthoc": sum(
            fnum(row.get("actual_settlement_residual_pnl_posthoc")) for row in rows
        ),
        "xuan_after_fee_pnl_including_residual_settlement": sum(
            fnum(row.get("xuan_after_fee_pnl_including_residual_settlement")) for row in rows
        ),
        "market_end_residual_cost": residual_cost,
        "market_end_residual_qty": sum(fnum(row.get("market_end_residual_qty")) for row in rows),
        "residual_cost_share": residual_cost / gross if gross else 0.0,
        "zero_stress_after_fee_pnl": sum(fnum(row.get("zero_stress_after_fee_pnl")) for row in rows),
        "merge_recovered_capital": sum(fnum(row.get("merge_recovered_capital")) for row in rows),
        "days": days,
    }


def day_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("day")), []).append(row)
    out = []
    for day, day_rows in sorted(grouped.items()):
        row = aggregate(day_rows)
        out.append(
            {
                "day": day,
                "candidate_rows": row["candidate_rows"],
                "gross_buy_cost": row["gross_buy_cost"],
                "core_pair_after_fee_pnl": row["core_pair_after_fee_pnl"],
                "market_end_residual_cost": row["market_end_residual_cost"],
                "zero_stress_after_fee_pnl": row["zero_stress_after_fee_pnl"],
            }
        )
    worst = min(out, key=lambda item: fnum(item.get("zero_stress_after_fee_pnl"))) if out else {}
    return {
        "day_rows": out,
        "day_count": len(out),
        "all_days_zero_stress_positive": all(
            fnum(item.get("zero_stress_after_fee_pnl")) > 0 for item in out
        ),
        "worst_zero_stress_day": worst.get("day"),
        "worst_zero_stress_after_fee_pnl": fnum(worst.get("zero_stress_after_fee_pnl")),
    }


def connect_duckdb(path: Path):
    try:
        import duckdb  # type: ignore
    except Exception:
        return None
    if not path.exists():
        return None
    return duckdb.connect(str(path), read_only=True)


def current_btc_capital_context(path: Path) -> dict[str, Any]:
    con = connect_duckdb(path)
    if con is None:
        return {
            "capital_ledger_available": False,
            "filter_specific_capital_ledger_available": False,
        }
    try:
        rows = con.execute(
            """
            select
              max(capital_tied) as max_capital_tied,
              avg(capital_tied) as avg_capital_tied,
              quantile_cont(capital_tied, 0.95) as p95_capital_tied
            from asset_capital_curve
            where asset = 'BTC'
            """
        ).fetchall()
        daily = con.execute(
            """
            select cast(day as varchar) as day, max(capital_tied) as max_capital_tied
            from asset_capital_curve
            where asset = 'BTC'
            group by day
            order by day
            """
        ).fetchall()
    finally:
        con.close()
    row = rows[0] if rows else (None, None, None)
    return {
        "capital_ledger_available": True,
        "filter_specific_capital_ledger_available": False,
        "note": "Current capital ledger is asset-level/full rescore, not low_residual_core_pair_v1-filter-specific.",
        "btc_full_rescore_max_capital_tied": fnum(row[0]),
        "btc_full_rescore_avg_capital_tied": fnum(row[1]),
        "btc_full_rescore_p95_capital_tied": fnum(row[2]),
        "btc_full_rescore_daily_max_capital_tied": [
            {"day": day, "max_capital_tied": fnum(value)} for day, value in daily
        ],
    }


def colleague_request() -> list[dict[str, Any]]:
    return [
        {
            "priority": "P0",
            "request": "BTC source semantics resolution",
            "details": [
                "Either prove parity between old BTC baseline and V1 normalized BUY adapter, or explicitly accept V1 normalized BUY adapter as canonical for BTC research/canary.",
                "Emit source_semantics_contract_id and source_dataset_fingerprint.",
                "State whether old-baseline parity remains a promotion blocker after canonical acceptance.",
            ],
        },
        {
            "priority": "P0",
            "request": "Filter-specific BTC capital ledger",
            "details": [
                "Build capital ledger for BTC low_residual_core_pair_v1 only, not all BTC rescore rows.",
                "Include max/avg/p95 capital tied, daily max capital tied, market concurrency, merge/recovered capital timing, and per-market gross exposure.",
                "Emit report JSON plus CSV with deterministic fingerprints.",
            ],
        },
        {
            "priority": "P0",
            "request": "Import-contract candidate table",
            "details": [
                "Add filter_name, filter_version, source_dataset_fingerprint, source_semantics_contract_id, l2_top_overlay_contract_id, rescore_manifest_fingerprint, deterministic_candidate_rank, runner_profile_id, max_notional_cap, dry_run_only.",
                "Keep candidate table research-only until owner/private truth and runner preflight exist.",
            ],
        },
        {
            "priority": "P0",
            "request": "Owner private-truth canary schema",
            "details": [
                "Define future owner order/fill/fee/inventory/merge/redeem/PnL tables and source hashes.",
                "No historical V1/shadow data may be backfilled as owner private truth.",
            ],
        },
        {
            "priority": "P1",
            "request": "BTC live-feasibility microstructure checks",
            "details": [
                "For low_residual_core_pair_v1 BTC rows, add L2 top/depth/queue quality buckets by candidate rank and time-to-close.",
                "Report expected slippage/fee sensitivity under small notional caps.",
            ],
        },
    ]


def proposed_canary_plan() -> dict[str, Any]:
    return {
        "stage_0": {
            "name": "import_dry_run_only",
            "allowed_now": False,
            "purpose": "Validate schema, candidate identity, source fingerprints, active-runner conflict checks, and orders_sent=false path.",
            "requires": [
                "source_semantics_contract_id",
                "import_contract_fields_complete",
                "filter_specific_capital_ledger",
                "runner preflight gate",
            ],
        },
        "stage_1": {
            "name": "owner_live_small_canary",
            "allowed_now": False,
            "purpose": "Collect owner private truth under tiny BTC-only exposure.",
            "suggested_initial_limits_after_all_gates_clear": {
                "asset": "BTC",
                "market_family": "5m up/down",
                "wallet_allocation_cap_usd": 100.0,
                "per_market_gross_buy_cost_cap_usd": 2.5,
                "concurrent_capital_cap_usd": 25.0,
                "single_instance_only": True,
                "stop_on_any_unreconciled_order_or_fill": True,
                "stop_on_any_source_semantics_mismatch": True,
                "stop_on_any_residual_cost_above_usd": 5.0,
            },
        },
    }


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    strategy = load_json(args.strategy_review)
    package = load_json(args.filter_package)
    source_contract = load_json(args.source_contract)
    btc_parity = load_json(args.btc_parity)
    package_outputs = body(package, "outputs")
    primary_csv = Path(str(package_outputs.get("primary_btc_eth_candidates_csv") or ""))
    rows = read_csv(primary_csv) if primary_csv.exists() else []
    btc_rows = [row for row in rows if row.get("asset") == "BTC"]
    btc_csv = args.artifact_dir / "low_residual_core_pair_v1_btc_candidates.csv"
    write_csv(btc_csv, btc_rows)

    summary = aggregate(btc_rows)
    days = day_summary(btc_rows)
    capital = current_btc_capital_context(args.capital_ledger)
    source_decision = body(source_contract, "decision")
    hard_blockers = [
        "btc_source_parity_or_canonical_source_semantics",
        "filter_specific_capital_ledger_missing",
        "import_contract_fields_incomplete",
        "owner_private_truth_schema_not_populated",
        "runner_import_preflight_missing",
    ]
    research_ready = (
        summary["candidate_rows"] > 0
        and summary["core_pair_after_fee_pnl"] > 0
        and summary["zero_stress_after_fee_pnl"] > 0
        and days["all_days_zero_stress_positive"]
        and bool(source_decision.get("research_csv_ready"))
    )
    card = {
        "artifact": "xuan_shadow_review_backtest_v1_btc_canary_readiness_gate",
        "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_CANARY_NOT_LIVE_READY_LOCAL_ONLY",
        "created_utc": STAMP,
        "script": "scripts/xuan_shadow_review_backtest_v1_btc_canary_readiness_gate.py",
        "inputs": {
            "strategy_review": str(args.strategy_review),
            "filter_package": str(args.filter_package),
            "source_contract": str(args.source_contract),
            "btc_parity": str(args.btc_parity),
            "capital_ledger": str(args.capital_ledger),
        },
        "outputs": {
            "artifact_dir": str(args.artifact_dir),
            "btc_candidates_csv": str(btc_csv),
            "btc_candidates_csv_sha256": sha256(btc_csv),
            "markdown": str(args.artifact_dir / "BTC_CANARY_READINESS_GATE.md"),
            "colleague_request_markdown": str(args.artifact_dir / "BTC_BACKTEST_COLLEAGUE_REQUEST.md"),
        },
        "decision": {
            "btc_research_edge_ready": research_ready,
            "btc_live_small_canary_ready": False,
            "candidate_import_allowed": False,
            "runner_support_ready": False,
            "manifest_or_preauthorization_ready": False,
            "future_remote_allowed_by_this_gate": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "private_truth_ready": False,
            "needs_backtest_colleague": True,
            "recommended_source_semantics_direction": "accept_v1_normalized_buy_adapter_as_canonical_research_source_unless_old_parity_is_proven_quickly",
            "next_lane": "backtest_colleague_deliverables_then_import_dry_run_contract_review",
        },
        "btc_filter_summary": clean(summary),
        "btc_day_holdout_summary": clean(days),
        "btc_capital_context": clean(capital),
        "source_semantics_status": {
            "btc_parity_status": btc_parity.get("status"),
            "btc_parity_blockers": btc_parity.get("blockers") or [],
            "source_semantics_explanation": btc_parity.get("source_semantics_explanation") or {},
            "source_contract_status": source_contract.get("status"),
        },
        "proposed_canary_plan_after_blockers_clear": proposed_canary_plan(),
        "backtest_colleague_requests": colleague_request(),
        "hard_blockers_before_live_small_canary": hard_blockers,
        "warnings": [
            "This gate does not authorize live orders.",
            "BTC research edge is strong, but source semantics and owner truth are not live-ready.",
            "Filter-specific capital ledger is required before sizing any canary.",
            "Residual settlement PnL remains posthoc and is not strategy edge.",
        ],
    }
    return clean(card)


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    summary = body(card, "btc_filter_summary")
    days = body(card, "btc_day_holdout_summary")
    capital = body(card, "btc_capital_context")
    lines = [
        "# BTC Canary Readiness Gate",
        "",
        f"- status: `{card.get('status')}`",
        f"- btc_research_edge_ready: `{decision.get('btc_research_edge_ready')}`",
        f"- btc_live_small_canary_ready: `{decision.get('btc_live_small_canary_ready')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## BTC Research Evidence",
        "",
        f"- candidate_rows: `{summary.get('candidate_rows')}`",
        f"- core_pair_after_fee_pnl: `{summary.get('core_pair_after_fee_pnl')}`",
        f"- residual_cost_share: `{summary.get('residual_cost_share')}`",
        f"- zero_stress_after_fee_pnl: `{summary.get('zero_stress_after_fee_pnl')}`",
        f"- all_days_zero_stress_positive: `{days.get('all_days_zero_stress_positive')}`",
        f"- worst_zero_stress_day: `{days.get('worst_zero_stress_day')}`",
        f"- worst_zero_stress_after_fee_pnl: `{days.get('worst_zero_stress_after_fee_pnl')}`",
        "",
        "## Capital Caveat",
        "",
        f"- current_btc_full_rescore_max_capital_tied: `{capital.get('btc_full_rescore_max_capital_tied')}`",
        f"- filter_specific_capital_ledger_available: `{capital.get('filter_specific_capital_ledger_available')}`",
        "- A filter-specific capital ledger is required before sizing a canary.",
        "",
        "## Hard Blockers Before Live-Small Canary",
        "",
    ]
    lines.extend(f"- `{item}`" for item in card.get("hard_blockers_before_live_small_canary", []))
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Push BTC first.",
            "- Prefer accepting V1 normalized BUY adapter as canonical research source if old parity cannot be proven quickly.",
            "- Next step is not live trading; it is colleague deliverables plus import dry-run contract review.",
            "",
        ]
    )
    return "\n".join(lines)


def render_colleague_request(card: dict[str, Any]) -> str:
    lines = [
        "# BTC Backtest/Canary Handoff Request",
        "",
        "请优先支持 BTC `low_residual_core_pair_v1` 进入实盘小额 canary 前置验收。当前不是请求上线，也不是请求下单；目标是补齐 source/parity/import/private-truth 合同。",
        "",
    ]
    for item in card.get("backtest_colleague_requests", []):
        lines.append(f"## {item['priority']} - {item['request']}")
        lines.append("")
        lines.extend(f"- {detail}" for detail in item["details"])
        lines.append("")
    lines.extend(
        [
            "## 当前 BTC 研究证据",
            "",
            f"- BTC candidate rows: `{body(card, 'btc_filter_summary').get('candidate_rows')}`",
            f"- core_pair_after_fee_pnl: `{body(card, 'btc_filter_summary').get('core_pair_after_fee_pnl')}`",
            f"- zero_stress_after_fee_pnl: `{body(card, 'btc_filter_summary').get('zero_stress_after_fee_pnl')}`",
            f"- residual_cost_share: `{body(card, 'btc_filter_summary').get('residual_cost_share')}`",
            "",
            "## 不要做",
            "",
            "- 不要把 historical V1/shadow 当 owner private truth。",
            "- 不要把 residual settlement PnL 当策略 edge。",
            "- 不要直接导入候选或触发 runner/live。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-review", type=Path, default=DEFAULT_STRATEGY_REVIEW)
    parser.add_argument("--filter-package", type=Path, default=DEFAULT_FILTER_PACKAGE)
    parser.add_argument("--source-contract", type=Path, default=DEFAULT_SOURCE_CONTRACT)
    parser.add_argument("--btc-parity", type=Path, default=DEFAULT_BTC_PARITY)
    parser.add_argument("--capital-ledger", type=Path, default=DEFAULT_CAPITAL_LEDGER)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    markdown = args.artifact_dir / "BTC_CANARY_READINESS_GATE.md"
    colleague = args.artifact_dir / "BTC_BACKTEST_COLLEAGUE_REQUEST.md"
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    colleague.write_text(render_colleague_request(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
