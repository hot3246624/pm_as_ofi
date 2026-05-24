#!/usr/bin/env python3
"""Audit whether the public b55 24h profile is a replicable strategy template.

This script consumes an already-exported public-profile 24h dataset. It does
not fetch private truth, raw/replay stores, shared ingress, broker state, or
any live/trading path.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_public_b55_strategy_replicability_audit"
DEFAULT_EXPORT_DIR = Path(
    "/Users/hot/web3Scientist/poly_trans_research/data/exports/"
    "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt"
)
FORBIDDEN_OUTPUT_FRAGMENTS = (
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


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_OUTPUT_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def ratio(num: float, den: float) -> float | None:
    return round(num / den, 8) if den else None


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def aggregate_by_asset_tf(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        key = (str(row.get("asset") or "unknown"), str(row.get("tf") or "unknown"))
        target = grouped[key]
        for field in (
            "buy_actual",
            "cash_pnl",
            "pair_pnl",
            "residual_pnl_est",
            "paired_qty",
            "resid_qty",
            "up_qty",
            "down_qty",
            "trade_count",
        ):
            target[field] += as_float(row.get(field))
    out: list[dict[str, Any]] = []
    for (asset, tf), values in grouped.items():
        paired_qty = values["paired_qty"]
        bought_qty = values["up_qty"] + values["down_qty"]
        pair_pnl = values["pair_pnl"]
        out.append(
            {
                "asset": asset,
                "tf": tf,
                "trade_count": int(round(values["trade_count"])),
                "buy_actual": round(values["buy_actual"], 8),
                "cash_pnl": round(values["cash_pnl"], 8),
                "pair_pnl": round(pair_pnl, 8),
                "residual_pnl_est": round(values["residual_pnl_est"], 8),
                "paired_qty": round(paired_qty, 8),
                "resid_qty": round(values["resid_qty"], 8),
                "resid_rate_on_bought_qty": ratio(values["resid_qty"], bought_qty),
                "actual_pair_cost_proxy": round(1.0 - pair_pnl / paired_qty, 8) if paired_qty else None,
            }
        )
    return sorted(out, key=lambda item: as_float(item.get("pair_pnl")), reverse=True)


def sum_bucket_actual(buckets: dict[str, dict[str, Any]], names: set[str]) -> float:
    return round(sum(as_float((buckets.get(name) or {}).get("actual")) for name in names), 8)


def summarize_deep(deep: dict[str, Any], total_actual: float) -> dict[str, Any]:
    time_buckets = deep.get("time_buckets") if isinstance(deep.get("time_buckets"), dict) else {}
    price_bands = deep.get("price_bands") if isinstance(deep.get("price_bands"), dict) else {}
    entry_15_to_1_actual = sum_bucket_actual(time_buckets, {"-15~-5m", "-5~-1m"})
    entry_15_to_0_actual = sum_bucket_actual(time_buckets, {"-15~-5m", "-5~-1m", "-60~0s"})
    core_price_actual = sum_bucket_actual(price_bands, {"35-50", "50-65", "65-80", "80-90"})
    mid_price_actual = sum_bucket_actual(price_bands, {"50-65", "65-80"})
    tail_price_actual = sum_bucket_actual(price_bands, {"00-05", "05-10", "10-20", "97-100"})
    return {
        "entry_timing_actual_usdc": {
            "end_minus_15m_to_1m": entry_15_to_1_actual,
            "end_minus_15m_to_0s": entry_15_to_0_actual,
            "end_minus_60s_only": as_float((time_buckets.get("-60~0s") or {}).get("actual")),
            "earlier_than_60m": as_float((time_buckets.get("<-60m") or {}).get("actual")),
            "unknown": as_float((time_buckets.get("unknown") or {}).get("actual")),
        },
        "entry_timing_actual_share": {
            "end_minus_15m_to_1m": ratio(entry_15_to_1_actual, total_actual),
            "end_minus_15m_to_0s": ratio(entry_15_to_0_actual, total_actual),
            "end_minus_60s_only": ratio(as_float((time_buckets.get("-60~0s") or {}).get("actual")), total_actual),
            "earlier_than_60m": ratio(as_float((time_buckets.get("<-60m") or {}).get("actual")), total_actual),
        },
        "price_band_actual_usdc": {
            "core_35_90": core_price_actual,
            "mid_50_80": mid_price_actual,
            "tail_00_20_plus_97_100": tail_price_actual,
        },
        "price_band_actual_share": {
            "core_35_90": ratio(core_price_actual, total_actual),
            "mid_50_80": ratio(mid_price_actual, total_actual),
            "tail_00_20_plus_97_100": ratio(tail_price_actual, total_actual),
        },
        "raw_time_buckets": time_buckets,
        "raw_price_bands": price_bands,
        "top_residuals": (deep.get("residuals_top") or [])[:12],
        "residual_by_asset_tf": deep.get("residual_by_asset_tf") or {},
    }


def build_gate(
    summary: dict[str, Any],
    group_rows: list[dict[str, Any]],
    deep_summary: dict[str, Any],
    *,
    same_window_new_cash_pnl_estimate: float,
    old_position_redeem_benefit_estimate: float,
    max_fee_rate: float,
    max_pair_cost: float,
    max_first_iteration_residual_rate: float,
) -> dict[str, Any]:
    cashflow = summary["cashflow"]
    mtm = summary["mark_to_market"]
    pair = summary["pair_metrics_lifetime"]
    current = summary["current_positions"]
    fee_rate = as_float(cashflow.get("fee_rate_on_gross"))
    actual_pair_cost = as_float(pair.get("actual_pair_cost"))
    residual_rate = as_float(pair.get("lifetime_residual_rate_on_bought_qty"))
    p90_pair_cost = as_float((pair.get("per_market_actual_pair_cost") or {}).get("p90"))
    pair_profit = as_float(pair.get("paired_actual_profit"))
    cash_plus_current = as_float(mtm.get("pnl_with_current_value"))
    top_groups = [
        row
        for row in group_rows
        if row["asset"] in {"BTC", "ETH"} and row["tf"] in {"15m", "1h_or_named"}
    ][:6]
    safer_first_groups = [
        row
        for row in top_groups
        if as_float(row.get("cash_pnl")) > 0 and as_float(row.get("residual_pnl_est")) >= 0
    ]
    checks = {
        "low_fee_execution_ok": fee_rate <= max_fee_rate,
        "average_pair_cost_ok": actual_pair_cost <= max_pair_cost,
        "pair_profit_positive": pair_profit > 0,
        "cash_plus_current_value_positive": cash_plus_current > 0,
        "same_window_new_cash_estimate_positive": same_window_new_cash_pnl_estimate > 0,
        "residual_rate_ok_for_new_system": residual_rate <= max_first_iteration_residual_rate,
        "per_market_pair_cost_tail_ok": p90_pair_cost <= 1.0,
        "core_entry_window_present": as_float(
            deep_summary["entry_timing_actual_share"].get("end_minus_15m_to_1m")
        )
        >= 0.50,
        "core_price_band_present": as_float(deep_summary["price_band_actual_share"].get("core_35_90")) >= 0.50,
    }
    hard_blockers = [
        key for key in ("residual_rate_ok_for_new_system", "per_market_pair_cost_tail_ok") if not checks[key]
    ]
    cautions = []
    if not checks["same_window_new_cash_estimate_positive"]:
        cautions.append("same_window_new_cash_pnl_is_negative_after_old_redeem_adjustment")
    if residual_rate > max_first_iteration_residual_rate:
        cautions.append("b55_residual_rate_too_high_for_first_replication_target")
    if p90_pair_cost > 1.0:
        cautions.append("pair_edge_has_large_per_market_tail_even_though_average_is_good")
    if not safer_first_groups:
        cautions.append("top_pair_profit_groups_still_need_fair_price_residual_control")
    decision = "KEEP"
    label = "KEEP_B55_TEMPLATE_LEAD_NOT_DEPLOYABLE_FAIR_PRICE_AND_RESIDUAL_GATE_REQUIRED"
    return {
        "decision": decision,
        "label": label,
        "checks": checks,
        "hard_blockers_before_shadow_or_deployable_claim": hard_blockers,
        "cautions": cautions,
        "thresholds": {
            "max_fee_rate_on_gross": max_fee_rate,
            "max_average_actual_pair_cost": max_pair_cost,
            "max_first_iteration_residual_rate": max_first_iteration_residual_rate,
            "required_per_market_pair_cost_p90_lte": 1.0,
        },
        "user_reported_same_window_correction": {
            "old_position_redeem_benefit_estimate": old_position_redeem_benefit_estimate,
            "same_window_new_cash_pnl_estimate": same_window_new_cash_pnl_estimate,
            "interpretation": (
                "The public 24h account-level cash PnL is positive, but part of the realized cash appears to come "
                "from positions opened before the window. This makes b55 a strategy lead, not proof of locked "
                "new-window profitability."
            ),
        },
        "template_to_replicate": {
            "asset_timeframe_priority": [
                "BTC/ETH 1h_or_named first because these groups have positive cash plus residual estimates in the export.",
                "BTC/ETH 15m second: strong pair edge but residual PnL can dominate, so it requires fair-price gating.",
                "Do not start from 5m hard-chase behavior; 5m has worse residual/control burden.",
            ],
            "entry_window": "end_minus_15m_to_1m, with last_60s as auxiliary not primary",
            "price_band": "35c_90c core, especially 50c_80c; tiny-tail tickets are auxiliary only",
            "pair_cost_rule": "average actual pair cost must stay <=0.97; market-level pair cost >1.00 is not an arb leg",
            "residual_rule": "first xuan replication target residual <=10%, stricter than b55 observed 14.95%",
            "fee_rule": "if realized fee/gross approaches xuan's 2.5%-3% historical level, discard this strategy family",
            "fair_price_requirement": (
                "Need independent exchange fair probability from aggregated BTC/ETH spot/index feeds. The objective is "
                "not predicting direction directly; it is buying Polymarket prices below fair win probability after fees."
            ),
        },
        "top_btc_eth_15m_1h_groups": top_groups,
        "safer_first_pass_groups": safer_first_groups,
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    summary_path = args.export_dir / "summary.json"
    deep_path = args.export_dir / "deep_raw_analysis.json"
    per_market_path = args.export_dir / "per_market_cash_pnl.csv"
    summary = load_json(summary_path)
    deep = load_json(deep_path)
    per_market_rows = read_csv_rows(per_market_path)
    group_rows = aggregate_by_asset_tf(per_market_rows)
    total_actual = as_float(summary["cashflow"].get("buy_actual_cost"))
    deep_summary = summarize_deep(deep, total_actual)
    gate = build_gate(
        summary,
        group_rows,
        deep_summary,
        same_window_new_cash_pnl_estimate=args.same_window_new_cash_pnl_estimate,
        old_position_redeem_benefit_estimate=args.old_position_redeem_benefit_estimate,
        max_fee_rate=args.max_fee_rate,
        max_pair_cost=args.max_pair_cost,
        max_first_iteration_residual_rate=args.max_first_iteration_residual_rate,
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_strategy_replicability_audit",
        "decision": gate["decision"],
        "decision_label": gate["label"],
        "scope": {
            "public_profile_export_only": True,
            "private_owner_trade_truth_used": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dplus_failed_families_enabled_or_swept": False,
        },
        "source_export": {
            "export_dir": str(args.export_dir),
            "summary_json": str(summary_path),
            "deep_raw_analysis_json": str(deep_path),
            "per_market_cash_pnl_csv": str(per_market_path),
        },
        "observed_b55_24h": {
            "user": summary.get("user"),
            "window": summary.get("window"),
            "row_counts": summary.get("row_counts"),
            "activity_types": summary.get("activity_types"),
            "cashflow": summary.get("cashflow"),
            "mark_to_market": summary.get("mark_to_market"),
            "pair_metrics_lifetime": summary.get("pair_metrics_lifetime"),
            "current_positions": summary.get("current_positions"),
            "timing_and_price_summary": deep_summary,
            "asset_timeframe_cash_pair_residual": group_rows,
        },
        "research_ranking": {
            "decision": gate["decision"],
            "label": gate["label"],
            "b55_is_best_used_as": "profitability/fair-price/lifecycle template, not a ready low-risk arb strategy",
            "replicability_gate": gate,
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_B55_TEMPLATE_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement local-only b55_fair_price_entry_window_spec_v1: restrict to BTC/ETH 15m and 1h, "
            "entry -15m..-1m, public price band 35c..90c, realized fee/gross <=1.2%, average actual pair cost <=0.97, "
            "residual target <=10%, and require an explicit fair-price data source before any no-order shadow."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--same-window-new-cash-pnl-estimate", type=float, default=-2610.0)
    parser.add_argument("--old-position-redeem-benefit-estimate", type=float, default=5818.0)
    parser.add_argument("--max-fee-rate", type=float, default=0.012)
    parser.add_argument("--max-pair-cost", type=float, default=0.97)
    parser.add_argument("--max-first-iteration-residual-rate", type=float, default=0.10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    gate = manifest["research_ranking"]["replicability_gate"]
    observed = manifest["observed_b55_24h"]
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "cash_pnl": observed["cashflow"]["cash_pnl"],
                "cash_plus_current_value": observed["mark_to_market"]["pnl_with_current_value"],
                "actual_pair_cost": observed["pair_metrics_lifetime"]["actual_pair_cost"],
                "paired_actual_profit": observed["pair_metrics_lifetime"]["paired_actual_profit"],
                "residual_rate": observed["pair_metrics_lifetime"]["lifetime_residual_rate_on_bought_qty"],
                "same_window_new_cash_pnl_estimate": gate["user_reported_same_window_correction"][
                    "same_window_new_cash_pnl_estimate"
                ],
                "hard_blockers_before_shadow_or_deployable_claim": gate[
                    "hard_blockers_before_shadow_or_deployable_claim"
                ],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
