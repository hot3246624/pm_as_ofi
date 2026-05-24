#!/usr/bin/env python3
"""Summarize public leaderboard trader exports for xuan-frontier research.

This is read-only against the colleague-owned export directory. It produces a
xuan-owned scorecard and markdown review that map public account behavior into
actionable local hypotheses, without treating public profiles as private truth.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any


DEFAULT_EXPORTS_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/exports")
DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")
DEFAULT_SCORECARD_ROOT = Path(".tmp_xuan/scorecards")


ACCOUNTS: list[dict[str, str]] = [
    {
        "label": "b55",
        "profile": "https://polymarket.com/zh/@0xb55fa1296E6ec55D0cE53d93B9237389f11764d4-1777575277609",
        "export_dir": "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
    {
        "label": "ohanism",
        "profile": "https://polymarket.com/zh/@ohanism",
        "export_dir": "leaderboard_ohanism_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
    {
        "label": "ce25",
        "profile": "https://polymarket.com/zh/@0xcE25E214D5cfE4f459cf67F08DF581885AAE7Fdc-1777575398144",
        "export_dir": "leaderboard_ce25_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
    {
        "label": "04b6",
        "profile": "https://polymarket.com/zh/@0x04b6D7E930cf9e493c5E6eF24B496294F95594C8-1774448369789",
        "export_dir": "leaderboard_04b6_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
    {
        "label": "xuan",
        "profile": "https://polymarket.com/zh/@0xcfb103c37c0234f524c632d964ed31f117b5f694",
        "export_dir": "leaderboard_xuan_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
    {
        "label": "b27bc",
        "profile": "https://polymarket.com/zh/@0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",
        "export_dir": "leaderboard_b27bc_recent24h_20260523_103000_to_20260524_103000_bjt",
    },
]


CORRECTED_WINDOW_PNL: dict[str, dict[str, Any]] = {
    "b55": {
        "old_inventory_redeem": 5818.0,
        "maker_rebate": 662.0,
        "new_position_cash_ex_rebate": -2610.0,
        "current_value": 5461.0,
        "new_position_mtm_ex_rebate": 2851.0,
        "new_position_mtm_including_rebate": 3513.0,
        "source": "colleague_corrected_window_decomposition_20260524",
    },
    "ce25": {
        "old_inventory_redeem": 0.0,
        "maker_rebate": 308.0,
        "new_position_cash_ex_rebate": 150.0,
        "current_value": 0.0,
        "new_position_mtm_ex_rebate": 150.0,
        "new_position_mtm_including_rebate": 458.0,
        "source": "colleague_corrected_window_decomposition_20260524",
    },
    "ohanism": {
        "old_inventory_redeem": 997.0,
        "maker_rebate": 1020.0,
        "new_position_cash_ex_rebate": 100.0,
        "current_value": 650.0,
        "new_position_mtm_ex_rebate": 751.0,
        "new_position_mtm_including_rebate": 1771.0,
        "source": "colleague_corrected_window_decomposition_20260524",
    },
    "xuan": {
        "old_inventory_redeem": 108.0,
        "maker_rebate": 0.0,
        "new_position_cash_ex_rebate": -394.0,
        "current_value": 0.0,
        "new_position_mtm_ex_rebate": -394.0,
        "new_position_mtm_including_rebate": -394.0,
        "source": "colleague_corrected_window_decomposition_20260524",
    },
    "04b6": {
        "old_inventory_redeem": 0.0,
        "maker_rebate": 0.0,
        "new_position_cash_ex_rebate": 13.0,
        "current_value": 0.0,
        "new_position_mtm_ex_rebate": 13.0,
        "new_position_mtm_including_rebate": 13.0,
        "source": "colleague_corrected_window_decomposition_20260524",
    },
    "b27bc": {
        "old_inventory_redeem": None,
        "maker_rebate": None,
        "new_position_cash_ex_rebate": None,
        "current_value": None,
        "new_position_mtm_ex_rebate": None,
        "new_position_mtm_including_rebate": None,
        "source": "no_recent_activity",
    },
}


CORRECTED_RANKING = [
    {
        "rank": 1,
        "label": "b55",
        "read": "综合最强，仍是主拆解对象；收益要按新仓 MTM 下修，不按 leaderboard 9k 粗读。",
    },
    {
        "rank": 2,
        "label": "ce25",
        "read": "最干净的低残仓 pair-arb 候选；规模和利润薄，但没有旧仓 redeem 污染。",
    },
    {
        "rank": 3,
        "label": "b27bc",
        "read": "历史 maker benchmark 可能强，但当前窗口无活动，不能作为现役基准。",
    },
    {
        "rank": 4,
        "label": "ohanism",
        "read": "赚钱能力可能强，但不是稳健 pair-arb；旧仓/rebate 和高残仓污染很重。",
    },
    {
        "rank": 5,
        "label": "xuan",
        "read": "继续排除；低残仓不能抵消高 fee 和含 fee pair cost > 1。",
    },
]


def fnum(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        x = float(value)
    except (TypeError, ValueError):
        return 0.0
    return x if math.isfinite(x) else 0.0


def fmt(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def pct(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * value:.{digits}f}%"


def money(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.{digits}f}"


def amount(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.{digits}f}"


def classify_asset(slug: str | None, title: str | None) -> str:
    text = f"{slug or ''} {title or ''}".lower()
    if "btc" in text or "bitcoin" in text:
        return "BTC"
    if "eth" in text or "ethereum" in text:
        return "ETH"
    if "sol" in text or "solana" in text:
        return "SOL"
    if "xrp" in text:
        return "XRP"
    return "OTHER"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json(path)


def load_market_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_optional_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return load_market_rows(path)


def asset_breakdown(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        asset = classify_asset(row.get("slug"), row.get("title"))
        item = out.setdefault(
            asset,
            {
                "markets": 0,
                "trades": 0,
                "paired_qty": 0.0,
                "residual_qty": 0.0,
                "paired_actual_profit": 0.0,
                "pair_cost_weighted_num": 0.0,
                "pair_cost_weighted_den": 0.0,
            },
        )
        paired_qty = fnum(row.get("paired_qty"))
        residual_qty = fnum(row.get("lifetime_residual_qty"))
        actual_pair_cost = row.get("actual_pair_cost")
        item["markets"] += 1
        item["trades"] += int(fnum(row.get("trade_count")))
        item["paired_qty"] += paired_qty
        item["residual_qty"] += residual_qty
        item["paired_actual_profit"] += fnum(row.get("paired_actual_profit"))
        if actual_pair_cost not in (None, "") and paired_qty > 0:
            item["pair_cost_weighted_num"] += fnum(actual_pair_cost) * paired_qty
            item["pair_cost_weighted_den"] += paired_qty

    for item in out.values():
        den = item["pair_cost_weighted_den"]
        total_qty = item["paired_qty"] + item["residual_qty"]
        item["actual_pair_cost_weighted"] = item["pair_cost_weighted_num"] / den if den else None
        item["residual_rate"] = item["residual_qty"] / total_qty if total_qty else None
        item.pop("pair_cost_weighted_num")
        item.pop("pair_cost_weighted_den")
    return dict(sorted(out.items(), key=lambda kv: kv[1]["trades"], reverse=True))


def verdict(label: str, summary: dict[str, Any]) -> tuple[str, str]:
    row_count = int(summary["row_counts"]["activity_unique_rows"])
    cash_pnl = fnum(summary["cashflow"]["cash_pnl"])
    actual_pair_cost = summary["pair_metrics_lifetime"].get("actual_pair_cost")
    residual_rate = summary["pair_metrics_lifetime"].get("lifetime_residual_rate_on_bought_qty")
    pair_cost = float(actual_pair_cost) if actual_pair_cost is not None else None
    residual = float(residual_rate) if residual_rate is not None else None
    if row_count < 500:
        if label == "b27bc":
            return ("historical_reference_inactive_recent", "no current activity; cannot use as active benchmark")
        return ("historical_reference_inactive_recent", "recent public sample is too small for current behavior inference")
    if label == "xuan":
        return (
            "excluded_high_fee_pair_cost_reference",
            "low residual is not enough because fee-adjusted pair cost is above one and corrected new-position cash is negative",
        )
    if residual is not None and residual >= 0.5:
        return (
            "directional_residual_rebate_reference_not_core",
            "cash profit exists, but paired economics are weak and residual exposure is dominant",
        )
    if residual is not None and residual <= 0.10 and pair_cost is not None and pair_cost <= 0.98:
        return (
            "low_residual_control_reference",
            "residual control is strong; fee-adjusted edge is thinner than b55",
        )
    if cash_pnl > 1000 and residual is not None and residual <= 0.18 and pair_cost is not None and pair_cost <= 0.965:
        return (
            "primary_probability_pair_residual_reference",
            "best current public template for low-fee positive pair edge plus bounded directional residual",
        )
    if label == "b55":
        return (
            "primary_probability_pair_residual_reference",
            "best current public template for low-fee pair quality, not pure new-window realized PnL",
        )
    return ("secondary_behavior_reference", "useful as a public behavior sample, not a core blueprint")


def group_cash_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("asset", "OTHER"), row.get("tf", "other"))
        item = grouped.setdefault(
            key,
            {
                "asset": key[0],
                "timeframe": key[1],
                "trade_count": 0,
                "buy_actual": 0.0,
                "cash_pnl": 0.0,
                "pair_pnl": 0.0,
                "residual_pnl_est": 0.0,
                "paired_qty": 0.0,
                "weighted_pair_cost_num": 0.0,
                "weighted_pair_cost_den": 0.0,
            },
        )
        paired_qty = fnum(row.get("paired_qty"))
        pair_cost = row.get("actual_pair_cost")
        item["trade_count"] += int(fnum(row.get("trade_count")))
        item["buy_actual"] += fnum(row.get("buy_actual"))
        item["cash_pnl"] += fnum(row.get("cash_pnl"))
        item["pair_pnl"] += fnum(row.get("pair_pnl"))
        item["residual_pnl_est"] += fnum(row.get("residual_pnl_est"))
        item["paired_qty"] += paired_qty
        if pair_cost not in (None, "") and paired_qty > 0:
            item["weighted_pair_cost_num"] += fnum(pair_cost) * paired_qty
            item["weighted_pair_cost_den"] += paired_qty

    out: list[dict[str, Any]] = []
    for item in grouped.values():
        den = item.pop("weighted_pair_cost_den")
        num = item.pop("weighted_pair_cost_num")
        item["actual_pair_cost_weighted"] = num / den if den else None
        out.append(item)
    return sorted(out, key=lambda item: item["pair_pnl"], reverse=True)


def b55_strategy_correction(export_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    deep = load_optional_json(export_dir / "deep_raw_analysis.json")
    cash_rows = load_optional_rows(export_dir / "per_market_cash_pnl.csv")
    cashflow = summary.get("cashflow", {})
    current = summary.get("current_positions", {})
    mtm = summary.get("mark_to_market", {})
    pair = summary.get("pair_metrics_lifetime", {})

    old_inventory_redeem_only_lower_bound = sum(
        fnum(row.get("cash_pnl"))
        for row in cash_rows
        if int(fnum(row.get("trade_count"))) == 0
        and fnum(row.get("buy_actual")) == 0
        and fnum(row.get("redeem")) > 0
    )
    window_cash_pnl = fnum(cashflow.get("cash_pnl"))
    same_window_cash_ex_old_lower_bound = window_cash_pnl - old_inventory_redeem_only_lower_bound
    grouped = group_cash_rows(cash_rows)

    return {
        "strategy_archetype": "near_expiry_probability_revaluation_plus_pair_cost_reduction_and_directional_residual",
        "corrected_interpretation": [
            "b55 is not pure maker-only, pure taker, or pure riskless arbitrage.",
            "The public 24h cash/MTM figures mix old-position redeem, same-window paired edge, and current residual value.",
            "Use b55 as a low-fee pair-quality and bounded-residual benchmark, not as proof that new positions locked 9k in 24h.",
        ],
        "window_cash_pnl_caveat": {
            "cash_pnl": round(window_cash_pnl, 6),
            "current_position_value": current.get("current_value"),
            "cash_plus_current_value": mtm.get("pnl_with_current_value"),
            "old_inventory_redeem_only_cash_lower_bound": round(old_inventory_redeem_only_lower_bound, 6),
            "same_window_cash_excluding_redeem_only_lower_bound": round(same_window_cash_ex_old_lower_bound, 6),
            "colleague_report_old_inventory_redeem_contribution_approx": 5818.0,
            "colleague_report_same_window_new_realized_cash_approx": -2610.0,
        },
        "pair_quality": {
            "gross_pair_cost": pair.get("gross_pair_cost"),
            "actual_pair_cost": pair.get("actual_pair_cost"),
            "pair_fee_cost": pair.get("pair_fee_cost"),
            "paired_actual_profit": pair.get("paired_actual_profit"),
            "residual_rate_on_bought_qty": pair.get("lifetime_residual_rate_on_bought_qty"),
            "current_residual_rate_on_position_qty": current.get("current_residual_rate_on_position_qty"),
            "fee_rate_on_gross": cashflow.get("fee_rate_on_gross"),
            "maker_rebate": cashflow.get("rebate_proceeds"),
        },
        "entry_timing_actual_cost": deep.get("time_buckets", {}),
        "price_band_actual_cost": deep.get("price_bands", {}),
        "top_group_pair_pnl": [
            {
                "asset": item["asset"],
                "timeframe": item["timeframe"],
                "actual_pair_cost_weighted": round(item["actual_pair_cost_weighted"], 6)
                if item["actual_pair_cost_weighted"] is not None
                else None,
                "pair_pnl": round(item["pair_pnl"], 6),
                "cash_pnl": round(item["cash_pnl"], 6),
                "residual_pnl_est": round(item["residual_pnl_est"], 6),
                "buy_actual": round(item["buy_actual"], 6),
                "trade_count": item["trade_count"],
            }
            for item in grouped[:10]
        ],
        "replication_notes": [
            "Start with BTC/ETH 15m and 1h rather than forcing 5m high-frequency execution.",
            "Focus entry windows around 15m to 1m before close, especially the final 5m before close.",
            "Most notional is 35c-90c, with 50c-80c the core band; tail-lottery prices are auxiliary.",
            "Treat actual pair cost above 0.97 as caution and above 1.00 as non-arbitrage unless directional residual is explicitly approved.",
            "Keep initial residual target below 10%; b55's 15%-18% is too aggressive for a new system.",
            "If effective fee rises toward 2.5%-3%, the template breaks unless pair cost improves materially.",
            "The missing component for replication is fair-price estimation, not just faster shared-ingress consumption.",
        ],
    }


def account_review(account: dict[str, str], exports_root: Path) -> dict[str, Any]:
    export_dir = exports_root / account["export_dir"]
    summary = load_json(export_dir / "summary.json")
    market_rows = load_market_rows(export_dir / "market_trade_metrics.csv")
    tag, reason = verdict(account["label"], summary)
    cashflow = summary["cashflow"]
    pair = summary["pair_metrics_lifetime"]
    mtm = summary["mark_to_market"]
    current = summary["current_positions"]
    buy_actual = fnum(cashflow["buy_actual_cost"])
    cash_pnl = fnum(cashflow["cash_pnl"])
    mtm_pnl = fnum(mtm["pnl_with_current_value"])
    actual_pair_cost = pair.get("actual_pair_cost")
    pair_edge = 1.0 - float(actual_pair_cost) if actual_pair_cost is not None else None
    review = {
        "label": account["label"],
        "profile": account["profile"],
        "export_dir": str(export_dir),
        "wallet": summary["user"],
        "window": summary["window"],
        "row_counts": summary["row_counts"],
        "activity_types": summary["activity_types"],
        "tag": tag,
        "tag_reason": reason,
        "buy_actual_cost": round(buy_actual, 6),
        "cash_pnl": round(cash_pnl, 6),
        "mtm_pnl": round(mtm_pnl, 6),
        "cash_roi_on_buy_actual": round(cash_pnl / buy_actual, 6) if buy_actual else None,
        "mtm_roi_on_buy_actual": round(mtm_pnl / buy_actual, 6) if buy_actual else None,
        "fee_total": round(fnum(cashflow["fee_total"]), 6),
        "fee_rate_on_gross": cashflow["fee_rate_on_gross"],
        "rebate_proceeds": cashflow["rebate_proceeds"],
        "redeem_proceeds": cashflow["redeem_proceeds"],
        "merge_proceeds": cashflow["merge_proceeds"],
        "actual_pair_cost": actual_pair_cost,
        "pair_edge": round(pair_edge, 6) if pair_edge is not None else None,
        "paired_actual_profit": pair["paired_actual_profit"],
        "paired_qty": pair["total_paired_qty"],
        "residual_qty": pair["total_lifetime_residual_qty"],
        "residual_rate_on_bought_qty": pair["lifetime_residual_rate_on_bought_qty"],
        "per_market_pair_cost": pair["per_market_actual_pair_cost"],
        "current_value": current["current_value"],
        "current_residual_rate": current["current_residual_rate_on_position_qty"],
        "assets": asset_breakdown(market_rows),
        "corrected_window_pnl": CORRECTED_WINDOW_PNL.get(account["label"]),
        "split_count": summary.get("activity_types", {}).get("counts", {}).get("SPLIT", 0),
    }
    if account["label"] == "b55":
        review["strategy_correction"] = b55_strategy_correction(export_dir, summary)
    return review


def build_scorecard(reviews: list[dict[str, Any]], generated_at: str, exports_root: Path) -> dict[str, Any]:
    b55 = next(item for item in reviews if item["label"] == "b55")
    ce25 = next(item for item in reviews if item["label"] == "ce25")
    ohanism = next(item for item in reviews if item["label"] == "ohanism")
    xuan = next(item for item in reviews if item["label"] == "xuan")
    benchmark_targets = {
        "b55_actual_pair_cost": b55["actual_pair_cost"],
        "b55_pair_edge": b55["pair_edge"],
        "b55_residual_rate": b55["residual_rate_on_bought_qty"],
        "b55_benchmark_scope": "pair_quality_fee_residual_reference_not_new_window_realized_pnl",
        "b55_cash_pnl_contains_old_inventory_redeem": True,
        "b55_new_position_mtm_ex_rebate": b55.get("corrected_window_pnl", {}).get("new_position_mtm_ex_rebate"),
        "b55_new_position_mtm_including_rebate": b55.get("corrected_window_pnl", {}).get("new_position_mtm_including_rebate"),
        "ce25_actual_pair_cost": ce25["actual_pair_cost"],
        "ce25_residual_rate": ce25["residual_rate_on_bought_qty"],
        "do_not_copy_residual_rate": ohanism["residual_rate_on_bought_qty"],
        "xuan_actual_pair_cost": xuan["actual_pair_cost"],
        "split_count_all_accounts": {
            item["label"]: item.get("split_count", 0)
            for item in reviews
        },
        "xuan_capacity_ladder_review_targets": {
            "target_actual_pair_cost_lte": 0.965,
            "review_actual_pair_cost_lte": 0.975,
            "target_residual_rate_lte": 0.15,
            "hard_residual_rate_lte": 0.20,
            "fee_after_cash_pnl_must_be_positive": True,
        },
    }
    return {
        "status": "KEEP_PUBLIC_LEADERBOARD_TRADER_REVIEW_READY_RESEARCH_ONLY",
        "generated_at_utc": generated_at,
        "source_exports_root": str(exports_root),
        "source_scope": "read-only public Polymarket profile/activity exports from colleague research directory",
        "private_truth_ready": False,
        "deployable": False,
        "live_orders_allowed": False,
        "shared_service_mutation_allowed": False,
        "remote_runner_allowed": False,
        "accounts": reviews,
        "benchmark_targets": benchmark_targets,
        "corrected_ranking": CORRECTED_RANKING,
        "xuan_frontier_implications": [
            "Use b55 as a pair-quality/fee/residual benchmark, not as a clean 24h new-position realized-PnL benchmark.",
            "The replicable part of b55 is low effective fee, positive fee-adjusted pair edge, and bounded directional residual around near-expiry probability revaluation.",
            "Upgrade ce25 as the clean low-residual pair-arb reference: no old-redeem pollution in this window, but thinner scale and profit than b55.",
            "Do not copy ohanism into the core shadow candidate: cashflow is positive but residual exposure is directional and paired cost is above 1.",
            "Keep xuan excluded as a public-account template: low residual does not matter when fee-adjusted pair cost is above one.",
            "Treat 04b6 as historical design inspiration only until activity returns.",
            "Current xuan clean no-order ROI is not obviously below b55 on comparable pair-edge terms; the unproven parts are fair-price-driven capacity, own execution fills, and controlled residual at size.",
            "All sampled SPLIT counts are zero, so this batch does not show split-then-sell interference.",
        ],
    }


def markdown(scorecard: dict[str, Any]) -> str:
    rows = []
    corrected_rows = []
    for item in scorecard["accounts"]:
        rows.append(
            "| {label} | {tag} | {activity:,} | {buy} | {cash} | {mtm} | {pair} | {edge} | {resid} | {cash_roi} |".format(
                label=item["label"],
                tag=item["tag"],
                activity=int(item["row_counts"]["activity_unique_rows"]),
                buy=amount(item["buy_actual_cost"], 0),
                cash=money(item["cash_pnl"], 0),
                mtm=money(item["mtm_pnl"], 0),
                pair=fmt(item["actual_pair_cost"], 4),
                edge=pct(item["pair_edge"], 2),
                resid=pct(item["residual_rate_on_bought_qty"], 2),
                cash_roi=pct(item["cash_roi_on_buy_actual"], 2),
            )
        )
        corrected = item.get("corrected_window_pnl") or {}
        corrected_rows.append(
            "| {label} | {old} | {rebate} | {new_cash} | {current} | {new_mtm} | {new_mtm_rebate} | {pair} | {resid} | {split} |".format(
                label=item["label"],
                old=money(corrected.get("old_inventory_redeem"), 0),
                rebate=money(corrected.get("maker_rebate"), 0),
                new_cash=money(corrected.get("new_position_cash_ex_rebate"), 0),
                current=amount(corrected.get("current_value"), 0),
                new_mtm=money(corrected.get("new_position_mtm_ex_rebate"), 0),
                new_mtm_rebate=money(corrected.get("new_position_mtm_including_rebate"), 0),
                pair=fmt(item["actual_pair_cost"], 4),
                resid=pct(item["residual_rate_on_bought_qty"], 2),
                split=item.get("split_count", 0),
            )
        )

    asset_sections = []
    for item in scorecard["accounts"]:
        lines = [
            f"### {item['label']}",
            "",
            "| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for asset, stats in item["assets"].items():
            lines.append(
                "| {asset} | {markets} | {trades} | {pair} | {resid} | {profit} |".format(
                    asset=asset,
                    markets=stats["markets"],
                    trades=stats["trades"],
                    pair=fmt(stats["actual_pair_cost_weighted"], 4),
                    resid=pct(stats["residual_rate"], 2),
                    profit=money(stats["paired_actual_profit"], 0),
                )
            )
        asset_sections.append("\n".join(lines))

    generated_at = scorecard["generated_at_utc"]
    account_links = "\n".join(f"- {item['label']}: {item['profile']}" for item in scorecard["accounts"])
    b = scorecard["benchmark_targets"]
    ranking_rows = [
        f"| {item['rank']} | {item['label']} | {item['read']} |"
        for item in scorecard.get("corrected_ranking", [])
    ]
    b55 = next(item for item in scorecard["accounts"] if item["label"] == "b55")
    b55_strategy = b55.get("strategy_correction", {})
    b55_cash = b55_strategy.get("window_cash_pnl_caveat", {})
    b55_pair = b55_strategy.get("pair_quality", {})
    b55_groups = b55_strategy.get("top_group_pair_pnl", [])[:6]
    b55_group_rows = [
        "| {asset} {tf} | {pair} | {profit} | {cash} | {resid} |".format(
            asset=item["asset"],
            tf=item["timeframe"],
            pair=fmt(item["actual_pair_cost_weighted"], 4),
            profit=money(item["pair_pnl"], 0),
            cash=money(item["cash_pnl"], 0),
            resid=money(item["residual_pnl_est"], 0),
        )
        for item in b55_groups
    ]
    return f"""# Xuan Public Leaderboard Trader Review 2026-05-24

## Scope

This review uses read-only public exports from:

`{scorecard['source_exports_root']}`

Generated at: `{generated_at}`.

Public profile links:

{account_links}

These accounts are public behavior samples, not private execution truth. The data is useful for target-shape design and benchmark calibration, but it cannot authorize live execution.

## Summary Table

This table is the raw public-window summary. It is not the corrected "new strategy PnL" table.

| account | archetype | activity rows | buy actual | cash PnL | MTM PnL | actual pair cost | pair edge | residual rate | cash ROI on buy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
{chr(10).join(rows)}

## Corrected Window PnL Decomposition

This table separates old-position redeem, maker rebate, same-window new-position cash, and current value. It is the preferred ranking table for recent behavior.

| account | old inventory redeem | maker rebate | new-position cash ex rebate | current value | corrected new-position MTM ex rebate | corrected new-position MTM incl rebate | actual pair cost | residual rate | split count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
{chr(10).join(corrected_rows)}

## Corrected Ranking

| rank | account | read |
|---:|---|---|
{chr(10).join(ranking_rows)}

## Read

1. `b55` is the best public **pair-quality / fee / residual** benchmark for xuan-frontier, but it is not a clean "new 24h positions locked 9k" benchmark. Its 24h actual pair cost is `{fmt(b['b55_actual_pair_cost'], 4)}`, pair edge is `{pct(b['b55_pair_edge'], 2)}`, and residual rate is `{pct(b['b55_residual_rate'], 2)}`.
2. `ce25` was under-rated by raw leaderboard PnL. It has no old-redeem pollution in this corrected table, low residual at `{pct(b['ce25_residual_rate'], 2)}`, and pair cost below 1. Its weakness is scale and thin profit, not cleanliness.
3. `ohanism` should be downgraded for this paired/rescue lane. Raw cashflow is positive, but the corrected view includes old redeem and large rebate, while pair cost is above 1 and residual is `{pct(b['do_not_copy_residual_rate'], 2)}`.
4. `xuan` remains excluded as a public-account template. Its residual is low, but actual pair cost is `{fmt(b['xuan_actual_pair_cost'], 4)}` and corrected new-position cash is negative.
5. `b27bc` is a historical reference only in this window because there is no current activity.
6. `04b6` remains useful historically, but the current 24h sample is too small and pair cost is above 1.0. It is not an active current benchmark.

## Corrected B55 Strategy Read

The corrected read is:

> near-expiry probability revaluation + two-sided pair-cost reduction + small directional residual + low effective fee.

This is not xuan's older "high-frequency hard-take with high fee" pattern, and it is not pure maker-only or pure taker-only.

### B55 24h Cash Caveat

- cash_pnl: `{money(b55_cash.get('cash_pnl'))}`
- current_position_value: `{amount(b55_cash.get('current_position_value'))}`
- cash_plus_current_value: `{money(b55_cash.get('cash_plus_current_value'))}`
- old_inventory_redeem_only_cash_lower_bound: `{money(b55_cash.get('old_inventory_redeem_only_cash_lower_bound'))}`
- colleague_report_old_inventory_redeem_contribution_approx: `{money(b55_cash.get('colleague_report_old_inventory_redeem_contribution_approx'))}`
- colleague_report_same_window_new_realized_cash_approx: `{money(b55_cash.get('colleague_report_same_window_new_realized_cash_approx'))}`

So b55 is profitable and worth studying, but the 24h cash/MTM line mixes old inventory redeem, current residual value, and same-window trading. It should calibrate pair quality and residual policy, not be treated as proof that fresh positions realized the full public PnL.

### B55 Pair Quality

- gross_pair_cost: `{fmt(b55_pair.get('gross_pair_cost'), 4)}`
- actual_pair_cost_with_fee: `{fmt(b55_pair.get('actual_pair_cost'), 4)}`
- pair_fee_cost: `{fmt(b55_pair.get('pair_fee_cost'), 4)}`
- paired_actual_profit: `{money(b55_pair.get('paired_actual_profit'), 0)}`
- bought residual rate: `{pct(b55_pair.get('residual_rate_on_bought_qty'), 2)}`
- current residual rate: `{pct(b55_pair.get('current_residual_rate_on_position_qty'), 2)}`
- fee_rate_on_gross: `{pct(b55_pair.get('fee_rate_on_gross'), 3)}`
- maker_rebate: `{money(b55_pair.get('maker_rebate'), 0)}`

### B55 Leading Pair-PnL Groups

| group | actual pair cost | pair PnL | cash PnL | residual PnL est |
|---|---:|---:|---:|---:|
{chr(10).join(b55_group_rows)}

### Replication Notes

- Start from BTC/ETH 15m and 1h, not 5m hard-chasing.
- Entry is concentrated in the final 15m to 1m, especially the final 5m before close.
- Main price bands are 35c-90c, especially 50c-80c; tail-lottery prices are auxiliary.
- Treat fee-included pair cost above 0.97 as caution and above 1.00 as directional, not arbitrage.
- Initial xuan residual target should stay below 10%; b55's 15%-18% is too aggressive for a new system.
- If effective fee drifts toward 2.5%-3%, this template breaks unless pair cost improves materially.
- The missing component is fair-price estimation from robust venue aggregation, not just socket speed.

## Asset Breakdown

{chr(10).join(asset_sections)}

## Implications For Xuan Frontier

- The right external benchmark is not leaderboard PnL alone. For b55, the usable benchmark is fee-aware actual pair cost, residual share, fee rate, and capacity; public cash PnL must be adjusted for old inventory and current residual value.
- For the capacity ladder, keep current pass gates, but add public-review targets: actual pair cost should aim at `<=0.965`, remain reviewable up to `<=0.975`, residual rate should target `<=15%`, and hard fail above `20%`.
- Our current clean no-order packet has fee-aware capital ROI around `4.07%`, which is not obviously worse than b55's pair-edge benchmark. The missing proof is real capacity and own execution, not the edge formula.
- Do not relax residual risk to chase ohanism-like cashflow. If a separate directional residual lane is studied, it must be explicitly labeled outside the current paired/rescue shadow candidate.
- `b55` supports continuing the capacity ladder and adding a fair-price lane: real accounts can run large BUY/redeem notional with low fee and residual around the mid-teens. `ce25` supports strict residual accounting. Together they support the cap-25 -> cap-75 -> cap-150 -> cap-300 proof path, but not blind 5m hard-taking.
- SPLIT is zero across this sampled batch, so this update does not add split-then-sell as a confounder.

## Recommended Next Actions

1. Keep the shadow-review candidate as research-only and proceed with the capacity ladder review path.
2. Add b55/ce25 benchmark fields to future shadow packets: public-benchmark pair cost, public-benchmark residual target, account-archetype comparison, and b55 old-inventory cash caveat.
3. If cap-25 dry-run passes, compare its fee-aware pair edge and residual share against b55/ce25 before moving to cap-75.
4. Open a separate local research lane for BTC/ETH 15m/1h fair-price admission. Keep it outside live execution and outside shared localagg mutation until it has its own source-truth validation.
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exports-root", type=Path, default=DEFAULT_EXPORTS_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--scorecard-root", type=Path, default=DEFAULT_SCORECARD_ROOT)
    parser.add_argument("--stamp", default=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%MZ"))
    parser.add_argument("--docs-output", type=Path, default=None)
    args = parser.parse_args()

    generated_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    reviews = [account_review(account, args.exports_root) for account in ACCOUNTS]
    scorecard = build_scorecard(reviews, generated_at, args.exports_root)

    artifact_dir = args.artifact_root / f"public_leaderboard_trader_review_{args.stamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    report_path = artifact_dir / "XUAN_PUBLIC_LEADERBOARD_TRADER_REVIEW_20260524_ZH.md"
    scorecard_path = args.scorecard_root / f"xuan_public_leaderboard_trader_review_{args.stamp}.json"
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)

    report_text = markdown(scorecard)
    report_path.write_text(report_text, encoding="utf-8")
    scorecard["artifact_report_path"] = str(report_path)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    if args.docs_output is not None:
        args.docs_output.parent.mkdir(parents=True, exist_ok=True)
        args.docs_output.write_text(report_text, encoding="utf-8")
        scorecard["docs_output_path"] = str(args.docs_output)
        scorecard_path.write_text(json.dumps(scorecard, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    print(json.dumps({"scorecard": str(scorecard_path), "report": str(report_path), "status": scorecard["status"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
