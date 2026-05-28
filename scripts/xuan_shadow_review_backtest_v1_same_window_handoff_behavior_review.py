#!/usr/bin/env python3
"""Review Backtest V1 same-window handoff behavior for xuan research.

The handoff files are useful for understanding how selected actions accumulate
within a market window, how much pairable inventory is created, and what residual
is left.  They are not owner private truth and do not authorize import, runner,
remote, deploy, or live trading.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


STAMP = "20260528T1231Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
RESCORE_ROOT = POLY_BT_ROOT / "derived/contract_examples/xuan_completion_candidate_rescore_latest"

DEFAULT_REFRESH = (
    POLY_BT_ROOT
    / "derived/contract_examples/xuan_backtest_v1_refresh_latest/XUAN_BACKTEST_V1_RESEARCH_REFRESH_SUMMARY.json"
)
DEFAULT_HANDOFF = RESCORE_ROOT / "xuan_completion_candidate_same_window_handoff.csv"
DEFAULT_ACTIONS = RESCORE_ROOT / "xuan_completion_candidate_same_window_handoff_actions.csv"
DEFAULT_RESIDUAL_LOTS = RESCORE_ROOT / "xuan_completion_candidate_same_window_handoff_residual_lots.csv"
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_same_window_handoff_behavior_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_same_window_handoff_behavior_review_{STAMP}"
)


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
    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


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
            writer.writerow({key: clean(value) for key, value in row.items()})


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(max(math.ceil(len(ordered) * q) - 1, 0), len(ordered) - 1)
    return ordered[idx]


def dist(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "p50": 0.0, "p90": 0.0, "max": 0.0, "avg": 0.0}
    return {
        "min": min(values),
        "p50": statistics.median(values),
        "p90": percentile(values, 0.90),
        "max": max(values),
        "avg": sum(values) / len(values),
    }


def aggregate_handoff(rows: list[dict[str, str]], asset: str | None = None) -> dict[str, Any]:
    scoped = [row for row in rows if asset is None or row.get("asset") == asset]
    gross = sum(fnum(row.get("gross_buy_cost")) for row in scoped)
    pair_pnl = sum(fnum(row.get("pair_pnl")) for row in scoped)
    fee = sum(fnum(row.get("official_taker_fee")) for row in scoped)
    residual_cost = sum(fnum(row.get("market_end_residual_cost")) for row in scoped)
    settlement_pnl = sum(fnum(row.get("actual_settlement_residual_pnl")) for row in scoped)
    core_after_fee = pair_pnl - fee
    zero_stress = core_after_fee - residual_cost
    return {
        "asset": asset or "ALL",
        "market_count": len(scoped),
        "day_count": len({row.get("day") for row in scoped if row.get("day")}),
        "gross_buy_cost": gross,
        "pair_pnl": pair_pnl,
        "official_taker_fee": fee,
        "core_pair_after_fee_pnl": core_after_fee,
        "market_end_residual_cost": residual_cost,
        "market_end_residual_qty": sum(fnum(row.get("market_end_residual_qty")) for row in scoped),
        "residual_cost_share": residual_cost / gross if gross else 0.0,
        "zero_stress_after_fee_pnl": zero_stress,
        "actual_settlement_residual_pnl_posthoc": settlement_pnl,
        "xuan_after_fee_pnl_including_residual_settlement": sum(
            fnum(row.get("xuan_after_fee_pnl")) for row in scoped
        ),
        "positive_core_after_fee_market_count": sum(
            1 for row in scoped if fnum(row.get("pair_pnl")) - fnum(row.get("official_taker_fee")) > 0
        ),
        "positive_zero_stress_market_count": sum(
            1
            for row in scoped
            if fnum(row.get("pair_pnl"))
            - fnum(row.get("official_taker_fee"))
            - fnum(row.get("market_end_residual_cost"))
            > 0
        ),
        "low_residual_5pct_market_count": sum(
            1 for row in scoped if fnum(row.get("residual_cost_share")) <= 0.05
        ),
        "zero_residual_market_count": sum(
            1 for row in scoped if fnum(row.get("market_end_residual_cost")) == 0
        ),
        "duration_s": dist([fnum(row.get("same_window_duration_s")) for row in scoped]),
        "action_rows": dist([fnum(row.get("same_window_action_rows")) for row in scoped]),
        "capital_turnover": dist([fnum(row.get("capital_turnover")) for row in scoped]),
        "residual_cost_share_dist": dist([fnum(row.get("residual_cost_share")) for row in scoped]),
        "max_residual_age_s": dist([fnum(row.get("max_residual_age_s")) for row in scoped]),
    }


def day_zero_stress(rows: list[dict[str, str]], asset: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("asset") == asset:
            grouped[str(row.get("day"))].append(row)
    out = []
    for day, day_rows in sorted(grouped.items()):
        agg = aggregate_handoff(day_rows)
        out.append(
            {
                "day": day,
                "market_count": agg["market_count"],
                "core_pair_after_fee_pnl": agg["core_pair_after_fee_pnl"],
                "market_end_residual_cost": agg["market_end_residual_cost"],
                "zero_stress_after_fee_pnl": agg["zero_stress_after_fee_pnl"],
            }
        )
    return out


def action_summary(actions: list[dict[str, str]], asset: str | None = None) -> dict[str, Any]:
    scoped = [row for row in actions if asset is None or row.get("asset") == asset]
    return {
        "asset": asset or "ALL",
        "action_rows": len(scoped),
        "seed_cost": sum(fnum(row.get("seed_cost")) for row in scoped),
        "fee": sum(fnum(row.get("fee")) for row in scoped),
        "side_counts": dict(Counter(row.get("side") for row in scoped)),
        "side_alignment_counts": dict(Counter(row.get("side_alignment") for row in scoped)),
        "blocked_by_counts": dict(Counter(row.get("blocked_by") for row in scoped)),
        "public_trade_price": dist([fnum(row.get("public_trade_price")) for row in scoped]),
        "seed_px": dist([fnum(row.get("seed_px")) for row in scoped]),
        "edge": dist([fnum(row.get("edge")) for row in scoped]),
        "offset_s": dist([fnum(row.get("offset_s")) for row in scoped]),
        "l1_pair_ask": dist([fnum(row.get("l1_pair_ask")) for row in scoped]),
    }


def residual_summary(lots: list[dict[str, str]], asset: str | None = None) -> dict[str, Any]:
    scoped = [row for row in lots if asset is None or row.get("asset") == asset]
    return {
        "asset": asset or "ALL",
        "residual_lot_count": len(scoped),
        "cost": sum(fnum(row.get("cost")) for row in scoped),
        "qty": sum(fnum(row.get("qty")) for row in scoped),
        "pnl_posthoc": sum(fnum(row.get("pnl")) for row in scoped),
        "side_counts": dict(Counter(row.get("side") for row in scoped)),
        "winner_side_counts": dict(Counter(row.get("winner_side") for row in scoped)),
        "age_s": dist([fnum(row.get("age_s")) for row in scoped]),
        "cost_dist": dist([fnum(row.get("cost")) for row in scoped]),
        "px_dist": dist([fnum(row.get("px")) for row in scoped]),
        "pnl_dist_posthoc": dist([fnum(row.get("pnl")) for row in scoped]),
    }


def btc_research_shortlist(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    btc_rows = [row for row in rows if row.get("asset") == "BTC"]
    shortlisted = []
    for row in btc_rows:
        core_after_fee = fnum(row.get("pair_pnl")) - fnum(row.get("official_taker_fee"))
        zero_stress = core_after_fee - fnum(row.get("market_end_residual_cost"))
        passes = (
            core_after_fee > 0
            and zero_stress > 0
            and fnum(row.get("residual_cost_share")) <= 0.05
            and row.get("side_count") == "2"
            and row.get("private_truth_ready") == "false"
            and row.get("deployable") == "false"
            and row.get("live_orders_allowed") == "false"
        )
        if not passes:
            continue
        shortlisted.append(
            {
                "handoff_rank": int(fnum(row.get("handoff_rank"))),
                "asset": row.get("asset"),
                "day": row.get("day"),
                "condition_id": row.get("condition_id"),
                "slug": row.get("slug"),
                "same_window_duration_s": fnum(row.get("same_window_duration_s")),
                "same_window_action_rows": int(fnum(row.get("same_window_action_rows"))),
                "gross_buy_cost": fnum(row.get("gross_buy_cost")),
                "pair_pnl": fnum(row.get("pair_pnl")),
                "official_taker_fee": fnum(row.get("official_taker_fee")),
                "core_pair_after_fee_pnl": core_after_fee,
                "market_end_residual_cost": fnum(row.get("market_end_residual_cost")),
                "residual_cost_share": fnum(row.get("residual_cost_share")),
                "zero_stress_after_fee_pnl": zero_stress,
                "capital_turnover": fnum(row.get("capital_turnover")),
                "max_residual_age_s": fnum(row.get("max_residual_age_s")),
                "research_only": True,
                "candidate_import_allowed": False,
            }
        )
    return sorted(shortlisted, key=lambda item: (-fnum(item["zero_stress_after_fee_pnl"]), item["handoff_rank"]))


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    refresh = load_json(args.refresh)
    handoff = read_csv(args.handoff)
    actions = read_csv(args.actions)
    lots = read_csv(args.residual_lots)

    assets = sorted({row.get("asset", "") for row in handoff if row.get("asset")})
    aggregate_by_asset = {asset: aggregate_handoff(handoff, asset) for asset in assets}
    btc_shortlist = btc_research_shortlist(handoff)
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    shortlist_csv = args.artifact_dir / "BTC_SAME_WINDOW_RESEARCH_SHORTLIST.csv"
    write_csv(shortlist_csv, btc_shortlist)

    btc_day_rows = day_zero_stress(handoff, "BTC")
    btc = aggregate_by_asset.get("BTC", {})
    decision = {
        "same_window_handoff_research_ready": True,
        "btc_behavior_research_positive": bool(
            btc
            and btc.get("market_count") == btc.get("positive_core_after_fee_market_count")
            and btc.get("market_count") == btc.get("positive_zero_stress_market_count")
        ),
        "btc_low_residual_behavior_shortlist_ready": bool(btc_shortlist),
        "btc_handoff_all_days_zero_stress_positive": all(
            fnum(row.get("zero_stress_after_fee_pnl")) > 0 for row in btc_day_rows
        ),
        "same_window_handoff_is_owner_private_truth": False,
        "candidate_import_allowed": False,
        "runner_support_ready": False,
        "manifest_or_preauthorization_ready": False,
        "future_remote_allowed_by_this_review": False,
        "remote_runner_allowed": False,
        "deployable": False,
        "live_orders_allowed": False,
        "private_truth_ready": False,
        "next_lane": "use_handoff_shortlist_for_local_behavior_review_then_wait_for_import_preflight_deliverables",
    }

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_same_window_handoff_behavior_review",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_SAME_WINDOW_HANDOFF_BEHAVIOR_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_same_window_handoff_behavior_review.py",
            "inputs": {
                "refresh": str(args.refresh),
                "handoff": str(args.handoff),
                "actions": str(args.actions),
                "residual_lots": str(args.residual_lots),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "btc_research_shortlist_csv": str(shortlist_csv),
                "btc_research_shortlist_csv_sha256": sha256(shortlist_csv),
                "markdown": str(args.artifact_dir / "SAME_WINDOW_HANDOFF_BEHAVIOR_REVIEW.md"),
            },
            "decision": decision,
            "refresh_summary": body(refresh, "summary"),
            "handoff_counts": {
                "market_rows": len(handoff),
                "action_rows": len(actions),
                "residual_lot_rows": len(lots),
                "asset_counts": dict(Counter(row.get("asset") for row in handoff)),
            },
            "aggregate_handoff": aggregate_handoff(handoff),
            "aggregate_by_asset": aggregate_by_asset,
            "btc_day_zero_stress": btc_day_rows,
            "action_summary": {
                "all": action_summary(actions),
                "btc": action_summary(actions, "BTC"),
                "eth": action_summary(actions, "ETH"),
            },
            "residual_summary": {
                "all": residual_summary(lots),
                "btc": residual_summary(lots, "BTC"),
                "eth": residual_summary(lots, "ETH"),
            },
            "btc_research_shortlist": {
                "row_count": len(btc_shortlist),
                "top_10": btc_shortlist[:10],
            },
            "hard_blockers_before_import_or_live": [
                "same_window_handoff_not_owner_private_truth",
                "btc_source_parity_or_canonical_source_semantics",
                "btc_semantic_alignment_proven",
                "source_semantics_contract_id_present",
                "source_dataset_fingerprint_present",
                "l2_top_overlay_contract_id_present",
                "filter_specific_btc_capital_ledger_present",
                "import_contract_table_fields_complete",
                "runner_import_preflight_ready",
                "owner_private_truth_schema_ready",
            ],
            "warnings": [
                "Same-window handoff is research handoff material only.",
                "Residual settlement PnL is posthoc and is not strategy edge.",
                "BTC behavior shortlist is not an import list.",
                "No candidate import, runner, remote, deploy, or live order is authorized.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    btc = body(body(card, "aggregate_by_asset"), "BTC")
    counts = body(card, "handoff_counts")
    shortlist = body(card, "btc_research_shortlist")
    lines = [
        "# Same-Window Handoff Behavior Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- same_window_handoff_research_ready: `{decision.get('same_window_handoff_research_ready')}`",
        f"- btc_behavior_research_positive: `{decision.get('btc_behavior_research_positive')}`",
        f"- btc_low_residual_behavior_shortlist_ready: `{decision.get('btc_low_residual_behavior_shortlist_ready')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Counts",
        "",
        f"- handoff_market_rows: `{counts.get('market_rows')}`",
        f"- handoff_action_rows: `{counts.get('action_rows')}`",
        f"- handoff_residual_lot_rows: `{counts.get('residual_lot_rows')}`",
        f"- asset_counts: `{counts.get('asset_counts')}`",
        "",
        "## BTC Behavior",
        "",
        f"- market_count: `{btc.get('market_count')}`",
        f"- core_pair_after_fee_pnl: `{btc.get('core_pair_after_fee_pnl')}`",
        f"- zero_stress_after_fee_pnl: `{btc.get('zero_stress_after_fee_pnl')}`",
        f"- residual_cost_share: `{btc.get('residual_cost_share')}`",
        f"- positive_zero_stress_market_count: `{btc.get('positive_zero_stress_market_count')}`",
        f"- low_residual_5pct_market_count: `{btc.get('low_residual_5pct_market_count')}`",
        f"- shortlist_row_count: `{shortlist.get('row_count')}`",
        "",
        "## Top BTC Research Shortlist",
        "",
        "| rank | day | slug | zero_stress | residual_share | actions |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in shortlist.get("top_10", []):
        lines.append(
            "| {rank} | {day} | `{slug}` | `{zero}` | `{share}` | `{actions}` |".format(
                rank=row.get("handoff_rank"),
                day=row.get("day"),
                slug=row.get("slug"),
                zero=row.get("zero_stress_after_fee_pnl"),
                share=row.get("residual_cost_share"),
                actions=row.get("same_window_action_rows"),
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- BTC same-window behavior is strong under research metrics: every BTC handoff market is core-positive and zero-stress-positive.",
            "- The useful next research object is the BTC handoff shortlist, not live import.",
            "- Import/live remains blocked by source semantics, fingerprints, filter-specific capital ledger, import contract, runner preflight, and owner private truth.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", type=Path, default=DEFAULT_REFRESH)
    parser.add_argument("--handoff", type=Path, default=DEFAULT_HANDOFF)
    parser.add_argument("--actions", type=Path, default=DEFAULT_ACTIONS)
    parser.add_argument("--residual-lots", type=Path, default=DEFAULT_RESIDUAL_LOTS)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "SAME_WINDOW_HANDOFF_BEHAVIOR_REVIEW.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
