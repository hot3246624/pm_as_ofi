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


def load_market_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


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
        return (
            "historical_reference_inactive_recent",
            "recent public sample is too small for current behavior inference",
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
            "primary_paired_accumulation_reference",
            "best current public template for high-volume paired BUY/redeem with bounded residual",
        )
    if label == "b55":
        return (
            "primary_paired_accumulation_reference",
            "best current public template after combining cashflow, MTM, pair cost, and residual",
        )
    return ("secondary_behavior_reference", "useful as a public behavior sample, not a core blueprint")


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
    }
    return review


def build_scorecard(reviews: list[dict[str, Any]], generated_at: str, exports_root: Path) -> dict[str, Any]:
    b55 = next(item for item in reviews if item["label"] == "b55")
    ce25 = next(item for item in reviews if item["label"] == "ce25")
    ohanism = next(item for item in reviews if item["label"] == "ohanism")
    benchmark_targets = {
        "b55_actual_pair_cost": b55["actual_pair_cost"],
        "b55_pair_edge": b55["pair_edge"],
        "b55_residual_rate": b55["residual_rate_on_bought_qty"],
        "ce25_residual_rate": ce25["residual_rate_on_bought_qty"],
        "do_not_copy_residual_rate": ohanism["residual_rate_on_bought_qty"],
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
        "xuan_frontier_implications": [
            "Use b55 as the primary high-throughput public benchmark: target roughly 0.96 actual pair cost with residual near or below 15%.",
            "Use ce25 as a residual-control reference, not an edge benchmark: its residual is low but fee-after cash PnL is much thinner.",
            "Do not copy ohanism into the core shadow candidate: cashflow is positive but residual exposure is directional and paired cost is above 1.",
            "Treat 04b6 as historical design inspiration only until activity returns.",
            "Current xuan clean no-order ROI is not obviously below b55 on comparable pair-edge terms; the unproven part is real capacity and actual execution fills.",
        ],
    }


def markdown(scorecard: dict[str, Any]) -> str:
    rows = []
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
    return f"""# Xuan Public Leaderboard Trader Review 2026-05-24

## Scope

This review uses read-only public exports from:

`{scorecard['source_exports_root']}`

Generated at: `{generated_at}`.

Public profile links:

{account_links}

These accounts are public behavior samples, not private execution truth. The data is useful for target-shape design and benchmark calibration, but it cannot authorize live execution.

## Summary Table

| account | archetype | activity rows | buy actual | cash PnL | MTM PnL | actual pair cost | pair edge | residual rate | cash ROI on buy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
{chr(10).join(rows)}

## Read

1. `b55` is the best public benchmark for xuan-frontier. Its 24h actual pair cost is `{fmt(b['b55_actual_pair_cost'], 4)}`, pair edge is `{pct(b['b55_pair_edge'], 2)}`, and residual rate is `{pct(b['b55_residual_rate'], 2)}`. This is not a zero-residual bot; it is high-throughput paired accumulation with residual held inside a manageable band.
2. `ce25` is the residual-control reference. It has lower residual than b55 at `{pct(b['ce25_residual_rate'], 2)}`, but fee-after cash PnL is much thinner. That says residual control alone is not enough; the admission price still needs a fee-aware edge.
3. `ohanism` should not be copied into the core candidate. The account is profitable in public cashflow, but residual is `{pct(b['do_not_copy_residual_rate'], 2)}` and paired cost is above 1.0. That is closer to directional/settlement plus rebate behavior than a clean paired engine.
4. `04b6` remains useful historically, but the current 24h sample is too small and pair cost is above 1.0. It is not an active current benchmark.

## Asset Breakdown

{chr(10).join(asset_sections)}

## Implications For Xuan Frontier

- The right external benchmark is not leaderboard PnL alone. The benchmark should be fee-aware actual pair cost, residual share, cash PnL, and capacity.
- For the capacity ladder, keep current pass gates, but add public-review targets: actual pair cost should aim at `<=0.965`, remain reviewable up to `<=0.975`, residual rate should target `<=15%`, and hard fail above `20%`.
- Our current clean no-order packet has fee-aware capital ROI around `4.07%`, which is not obviously worse than b55's pair-edge benchmark. The missing proof is real capacity and own execution, not the edge formula.
- Do not relax residual risk to chase ohanism-like cashflow. If a separate directional residual lane is studied, it must be explicitly labeled outside the current paired/rescue shadow candidate.
- `b55` supports continuing the capacity ladder: real accounts can run large BUY/redeem notional with residual around the mid-teens. `ce25` supports strict residual accounting. Together they support the cap-25 -> cap-75 -> cap-150 -> cap-300 proof path.

## Recommended Next Actions

1. Keep the shadow-review candidate as research-only and proceed with the capacity ladder review path.
2. Add b55/ce25 benchmark fields to future shadow packets: public-benchmark pair cost, public-benchmark residual target, and account-archetype comparison.
3. If cap-25 dry-run passes, compare its fee-aware pair edge and residual share against b55/ce25 before moving to cap-75.
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
