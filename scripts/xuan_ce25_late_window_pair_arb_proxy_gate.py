#!/usr/bin/env python3
"""Score the ce25 late-window pair-arb public proxy.

This is a local-only public-export proxy gate. It uses the existing b55/ce25
deep-compare export and the committed lead-selection artifact, then separates
clean public-proxy evidence from promotion/deploy evidence. It does not fetch
data, start services, run shadow, or touch live trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ARTIFACT = "xuan_ce25_late_window_pair_arb_proxy_gate"
DEFAULT_DEEP_COMPARE_DIR = Path(
    "/Users/hot/web3Scientist/poly_trans_research/data/exports/"
    "candidate_deep_compare_b55_ce25_20260523_103000_to_20260524_103000_bjt"
)
DEFAULT_NEXT_LEAD_GATE = Path(
    "xuan_research_artifacts/"
    "xuan_public_profile_next_lead_gate_20260524T091916Z/"
    "manifest.json"
)
DEFAULT_NON_FIXTURE_REVIEW = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_non_fixture_input_source_review_20260524T090916Z/"
    "manifest.json"
)
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
REQUIRED_DEEP_COMPARE_FILES = (
    "summary.json",
    "post_window_cohort_followup.json",
    "cohort_groups_followup.csv",
    "ce25/market_cash_pnl.csv",
    "b55/market_cash_pnl.csv",
)
PRIMARY_ASSETS = {"BTC", "ETH", "SOL"}


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


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    return int(round(to_float(value, float(default))))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def round_or_none(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


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


def missing_deep_compare_files(deep_compare_dir: Path) -> list[str]:
    return [name for name in REQUIRED_DEEP_COMPARE_FILES if not (deep_compare_dir / name).exists()]


def load_inputs(deep_compare_dir: Path) -> dict[str, Any]:
    missing = missing_deep_compare_files(deep_compare_dir)
    if missing:
        raise FileNotFoundError(f"missing deep-compare files: {missing}")
    summary_rows = load_json(deep_compare_dir / "summary.json")
    followup_rows = load_json(deep_compare_dir / "post_window_cohort_followup.json")
    cohorts = read_csv_rows(deep_compare_dir / "cohort_groups_followup.csv")
    market_rows: dict[str, list[dict[str, str]]] = {}
    for account in ("ce25", "b55"):
        rows = read_csv_rows(deep_compare_dir / account / "market_cash_pnl.csv")
        for row in rows:
            row["_parsed_tf"] = parse_timeframe(row.get("slug", ""), row.get("tf", ""))
        market_rows[account] = rows
    return {
        "summary": {str(row.get("label")): row for row in summary_rows if isinstance(row, dict)},
        "followup": {str(row.get("label")): row for row in followup_rows if isinstance(row, dict)},
        "cohorts": cohorts,
        "market_rows": market_rows,
    }


def account_metrics(inputs: dict[str, Any], label: str) -> dict[str, Any]:
    summary = inputs["summary"].get(label, {})
    followup = inputs["followup"].get(label, {})
    return {
        "buy_actual": to_float(summary.get("buy_actual")),
        "actual_pair_cost": to_float(summary.get("actual_pair_cost")),
        "pair_cost_p90": to_float((summary.get("per_market_actual_pair_cost") or {}).get("p90")),
        "paired_actual_profit": to_float(summary.get("paired_actual_profit")),
        "residual_rate_on_buy_qty": to_float(summary.get("resid_rate_on_buy_qty")),
        "old_redeem_contamination": to_float(summary.get("old_no_buy_cash")),
        "post_window_same_condition_buy": to_float(followup.get("post_buy_same_cids")),
        "conservative_cohort_pnl_ex_rebate": to_float(
            followup.get("cohort_cash_add_post_cash_no_post_buy_subtract_post_buy")
        ),
        "rebate": to_float(summary.get("no_condition_rebate")),
    }


def cohort_predicate(group_name: str) -> Callable[[dict[str, str]], bool]:
    if group_name == "ce25_btc_eth_sol_15m":
        return lambda row: row.get("label") == "ce25" and row.get("asset") in PRIMARY_ASSETS and row.get("tf") == "15m"
    if group_name == "ce25_all_15m":
        return lambda row: row.get("label") == "ce25" and row.get("tf") == "15m"
    if group_name == "ce25_all_5m":
        return lambda row: row.get("label") == "ce25" and row.get("tf") == "5m"
    if group_name == "ce25_btc_5m":
        return lambda row: row.get("label") == "ce25" and row.get("asset") == "BTC" and row.get("tf") == "5m"
    if group_name == "b55_btc_eth_sol_15m":
        return lambda row: row.get("label") == "b55" and row.get("asset") in PRIMARY_ASSETS and row.get("tf") == "15m"
    if group_name == "b55_btc_eth_sol_1h_4h":
        return (
            lambda row: row.get("label") == "b55"
            and row.get("asset") in PRIMARY_ASSETS
            and row.get("tf") in {"1h_or_named", "4h"}
        )
    raise ValueError(f"unknown cohort group: {group_name}")


def market_predicate(group_name: str) -> Callable[[dict[str, str]], bool]:
    if group_name == "ce25_btc_eth_sol_15m":
        return lambda row: row.get("asset") in PRIMARY_ASSETS and row.get("_parsed_tf") == "15m"
    if group_name == "ce25_all_15m":
        return lambda row: row.get("_parsed_tf") == "15m"
    if group_name == "ce25_all_5m":
        return lambda row: row.get("_parsed_tf") == "5m"
    if group_name == "ce25_btc_5m":
        return lambda row: row.get("asset") == "BTC" and row.get("_parsed_tf") == "5m"
    if group_name == "b55_btc_eth_sol_15m":
        return lambda row: row.get("asset") in PRIMARY_ASSETS and row.get("_parsed_tf") == "15m"
    if group_name == "b55_btc_eth_sol_1h_4h":
        return lambda row: row.get("asset") in PRIMARY_ASSETS and row.get("_parsed_tf") in {"1h_or_named", "4h"}
    raise ValueError(f"unknown market group: {group_name}")


def aggregate_cohorts(rows: list[dict[str, str]]) -> dict[str, Any]:
    pair = sum(to_float(row.get("pair_pnl_base")) for row in rows)
    residual = sum(to_float(row.get("residual_pnl_est_base")) for row in rows)
    cohort = sum(to_float(row.get("cohort_cash_ex_rebate")) for row in rows)
    buy = sum(to_float(row.get("buy_plus_post_buy")) for row in rows)
    return {
        "rows": len(rows),
        "markets": sum(to_int(row.get("markets")) for row in rows),
        "buy_plus_post_buy": round(buy, 6),
        "cohort_cash_ex_rebate": round(cohort, 6),
        "roi_ex_rebate": round(cohort / buy, 6) if buy else None,
        "pair_pnl_base": round(pair, 6),
        "residual_pnl_est_base": round(residual, 6),
        "residual_drag_share_of_pair_pnl": round(abs(residual) / pair, 6) if pair > 0 else None,
        "asset_tf_rows": [
            {
                "asset": row.get("asset"),
                "timeframe": row.get("tf"),
                "markets": to_int(row.get("markets")),
                "cohort_cash_ex_rebate": round(to_float(row.get("cohort_cash_ex_rebate")), 6),
                "pair_pnl_base": round(to_float(row.get("pair_pnl_base")), 6),
                "residual_pnl_est_base": round(to_float(row.get("residual_pnl_est_base")), 6),
            }
            for row in rows
        ],
    }


def aggregate_markets(rows: list[dict[str, str]]) -> dict[str, Any]:
    costs = [to_float(row.get("actual_pair_cost")) for row in rows if row.get("actual_pair_cost")]
    tail_rows = [row for row in rows if row.get("actual_pair_cost") and to_float(row.get("actual_pair_cost")) > 1.0]
    cash_values = [to_float(row.get("cash_pnl")) for row in rows]
    pair = sum(to_float(row.get("pair_pnl")) for row in rows)
    residual = sum(to_float(row.get("residual_pnl_est")) for row in rows)
    cash = sum(cash_values)
    return {
        "markets": len(rows),
        "actual_pair_cost_p50": round_or_none(quantile(costs, 0.50)),
        "actual_pair_cost_p75": round_or_none(quantile(costs, 0.75)),
        "actual_pair_cost_p90": round_or_none(quantile(costs, 0.90)),
        "actual_pair_cost_gt_1_count": len(tail_rows),
        "actual_pair_cost_gt_1_share": round(len(tail_rows) / len(costs), 6) if costs else None,
        "cash_pnl": round(cash, 6),
        "pair_pnl": round(pair, 6),
        "residual_pnl_est": round(residual, 6),
        "positive_cash_markets": sum(1 for value in cash_values if value > 0),
        "negative_cash_markets": sum(1 for value in cash_values if value < 0),
        "worst_cash_markets": [
            {
                "slug": row.get("slug"),
                "asset": row.get("asset"),
                "timeframe": row.get("_parsed_tf"),
                "cash_pnl": round(to_float(row.get("cash_pnl")), 6),
                "actual_pair_cost": round(to_float(row.get("actual_pair_cost")), 6)
                if row.get("actual_pair_cost")
                else None,
                "residual_pnl_est": round(to_float(row.get("residual_pnl_est")), 6),
            }
            for row in sorted(rows, key=lambda item: to_float(item.get("cash_pnl")))[:5]
        ],
    }


def group_metrics(inputs: dict[str, Any], group_name: str, account: str) -> dict[str, Any]:
    c_pred = cohort_predicate(group_name)
    m_pred = market_predicate(group_name)
    cohorts = [row for row in inputs["cohorts"] if c_pred(row)]
    markets = [row for row in inputs["market_rows"][account] if m_pred(row)]
    return {
        "group": group_name,
        "account": account,
        "cohort_metrics": aggregate_cohorts(cohorts),
        "market_metrics": aggregate_markets(markets),
    }


def load_optional_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json(path)


def non_fixture_blocked(review: dict[str, Any]) -> bool:
    ranking = review.get("research_ranking") if isinstance(review.get("research_ranking"), dict) else {}
    if not ranking:
        return True
    return bool(ranking.get("stop_before_no_order_diagnostic", True)) or not bool(
        ranking.get("real_non_fixture_source_ready")
    )


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(args.deep_compare_dir) or not path_safe(args.next_lead_gate_manifest) or not path_safe(output_dir):
        raise RuntimeError("unsafe path under ce25 proxy gate")
    missing = missing_deep_compare_files(args.deep_compare_dir)
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
            "lane": "ce25_late_window_pair_arb_proxy_gate",
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_INPUTS_MISSING",
            "scope": scope,
            "source_inputs": {
                "deep_compare_dir": str(args.deep_compare_dir),
                "missing_deep_compare_files": missing,
                "next_lead_gate_manifest": str(args.next_lead_gate_manifest),
                "non_fixture_review_manifest": str(args.non_fixture_review_manifest),
            },
            "research_ranking": {
                "ce25_proxy_status": "UNKNOWN_INPUTS_MISSING",
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
            "next_executable_action": "Rerun on a complete existing deep-compare export; do not start services or diagnostics.",
        }

    inputs = load_inputs(args.deep_compare_dir)
    next_lead = load_optional_manifest(args.next_lead_gate_manifest)
    non_fixture = load_optional_manifest(args.non_fixture_review_manifest)
    ce25 = account_metrics(inputs, "ce25")
    b55 = account_metrics(inputs, "b55")
    groups = {
        "ce25_primary_15m_btc_eth_sol": group_metrics(inputs, "ce25_btc_eth_sol_15m", "ce25"),
        "ce25_all_15m": group_metrics(inputs, "ce25_all_15m", "ce25"),
        "ce25_all_5m_control": group_metrics(inputs, "ce25_all_5m", "ce25"),
        "ce25_btc_5m_negative_control": group_metrics(inputs, "ce25_btc_5m", "ce25"),
        "b55_15m_btc_eth_sol_control": group_metrics(inputs, "b55_btc_eth_sol_15m", "b55"),
        "b55_1h_4h_btc_eth_sol_strength_control": group_metrics(inputs, "b55_btc_eth_sol_1h_4h", "b55"),
    }
    primary = groups["ce25_primary_15m_btc_eth_sol"]
    primary_cohort = primary["cohort_metrics"]
    primary_market = primary["market_metrics"]
    negative = groups["ce25_btc_5m_negative_control"]["cohort_metrics"]
    no_order_blocked = non_fixture_blocked(non_fixture)
    proxy_checks = {
        "ce25_selected_by_previous_gate": (
            next_lead.get("research_ranking", {}).get("mimicability_target") == "ce25"
        ),
        "primary_has_at_least_200_markets": primary_cohort["markets"] >= 200,
        "primary_conservative_cohort_pnl_positive": primary_cohort["cohort_cash_ex_rebate"] > 0,
        "primary_pair_pnl_positive": primary_cohort["pair_pnl_base"] > 0,
        "account_residual_rate_at_or_below_10pct": ce25["residual_rate_on_buy_qty"] <= 0.10,
        "account_actual_pair_cost_below_0_98": ce25["actual_pair_cost"] <= 0.98,
        "account_no_old_redeem_contamination": abs(ce25["old_redeem_contamination"]) < 1e-9,
        "account_no_same_condition_post_buy": abs(ce25["post_window_same_condition_buy"]) < 1e-9,
        "btc_5m_negative_control_is_negative": negative["cohort_cash_ex_rebate"] < 0,
        "b55_strength_control_pair_engine_stronger": b55["actual_pair_cost"] < ce25["actual_pair_cost"]
        and b55["paired_actual_profit"] > ce25["paired_actual_profit"],
        "no_order_diagnostic_blocked_until_non_fixture_inputs": no_order_blocked,
    }
    promotion_checks = {
        "primary_pair_cost_p90_at_or_below_1": (primary_market["actual_pair_cost_p90"] or 999) <= 1.0,
        "primary_residual_pnl_nonnegative": primary_cohort["residual_pnl_est_base"] >= 0,
        "non_fixture_liquidity_and_fair_probability_ready": not no_order_blocked,
        "private_truth_ready": False,
    }
    proxy_ready = all(proxy_checks.values())
    promotion_ready = all(promotion_checks.values())
    decision = "KEEP" if proxy_ready else "UNKNOWN"
    label = (
        "KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_READY_TAIL_RESIDUAL_BLOCKED"
        if proxy_ready
        else "UNKNOWN_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_INSUFFICIENT"
    )
    if promotion_ready:
        label = "KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_PROMOTION_READY_UNEXPECTED"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_late_window_pair_arb_proxy_gate",
        "decision": decision,
        "decision_label": label,
        "scope": scope,
        "source_inputs": {
            "deep_compare_dir": str(args.deep_compare_dir),
            "deep_compare_files": list(REQUIRED_DEEP_COMPARE_FILES),
            "next_lead_gate_manifest": str(args.next_lead_gate_manifest),
            "non_fixture_review_manifest": str(args.non_fixture_review_manifest),
        },
        "account_metrics": {"ce25": ce25, "b55": b55},
        "group_metrics": groups,
        "proxy_checks": proxy_checks,
        "promotion_checks": promotion_checks,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "selected_public_proxy": "ce25_btc_eth_sol_15m_late_window",
            "negative_control": "ce25_btc_5m",
            "strength_control": "b55",
            "proxy_ready_for_next_local_audit": proxy_ready,
            "promotion_gate_passed": False,
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "interpretation": (
                "ce25 BTC/ETH/SOL 15m is a clean public-proxy lead because conservative cohort PnL and pair PnL "
                "are positive while account-level contamination and residual rate pass the first mimicability gate. "
                "It is not deployable: market-level pair-cost tail and residual drag remain material, and real "
                "liquidity-role/fair-probability joins are still missing."
            ),
        },
        "known_blockers": [
            "primary 15m market actual_pair_cost_p90 remains above 1.0",
            "primary 15m residual_pnl_est_base remains negative",
            "non-fixture per-trade liquidity_role still missing",
            "non-fixture fair_probability/uncertainty/edge joins still missing",
            "public cohort PnL is proxy evidence only and cannot prove private truth or deployability",
        ],
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROXY_GATE_ONLY_TAIL_RESIDUAL_AND_INPUTS_BLOCK_PROMOTION",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [name for name, ok in promotion_checks.items() if not ok],
        },
        "next_executable_action": (
            "Implement ce25_15m_market_tail_gap_audit_v1 local-only using existing ce25 market_cash_pnl and "
            "public activity rows: isolate whether the p90 pair-cost tail and residual drag are concentrated in "
            "observable market contexts without using pure price-band/static asset deletion or fixture probability."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deep-compare-dir", type=Path, default=DEFAULT_DEEP_COMPARE_DIR)
    parser.add_argument("--next-lead-gate-manifest", type=Path, default=DEFAULT_NEXT_LEAD_GATE)
    parser.add_argument("--non-fixture-review-manifest", type=Path, default=DEFAULT_NON_FIXTURE_REVIEW)
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
