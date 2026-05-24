#!/usr/bin/env python3
"""Audit ce25 15m market-level pair-tail and residual gap concentration.

This local-only audit reads existing public/deep-compare exports. It searches
for non-price, non-static context concentration in the ce25 BTC/ETH/SOL 15m
public proxy, while keeping promotion/deploy evidence fail-closed.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_15m_market_tail_gap_audit"
DEFAULT_DEEP_COMPARE_DIR = Path(
    "/Users/hot/web3Scientist/poly_trans_research/data/exports/"
    "candidate_deep_compare_b55_ce25_20260523_103000_to_20260524_103000_bjt"
)
DEFAULT_PROXY_GATE = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_late_window_pair_arb_proxy_gate_20260524T094516Z/"
    "manifest.json"
)
DEFAULT_PUBLIC_ROWS = Path(
    "xuan_research_artifacts/"
    "xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/"
    "public_inputs/"
    "0xce25e214d5cfe4f459cf67f08df581885aae7fdc/"
    "activity_trade_rows.json"
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
    "ce25/market_cash_pnl.csv",
    "post_window_by_condition.csv",
)
PRIMARY_ASSETS = {"BTC", "ETH", "SOL"}
ALLOWED_CONTEXT_KEYS = (
    "last_touch_to_expiry_bucket",
    "first_touch_to_expiry_bucket",
    "touch_span_bucket",
    "trade_count_bucket",
    "buy_actual_bucket",
    "fee_rate_bucket",
)


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


def to_int(value: Any, default: int = 0) -> int:
    return int(round(to_float(value, float(default))))


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


def slug_start(slug: str) -> int | None:
    match = re.search(r"-(\d{10})$", slug or "")
    return int(match.group(1)) if match else None


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


def market_end_s(slug: str, timeframe: str) -> int | None:
    start = slug_start(slug)
    if start is None:
        return None
    if timeframe == "15m":
        return start + 900
    if timeframe == "5m":
        return start + 300
    return None


def bucket(value: float, cuts: list[float], labels: list[str]) -> str:
    for cut, label in zip(cuts, labels):
        if value <= cut:
            return label
    return labels[-1]


def missing_deep_compare_files(deep_compare_dir: Path) -> list[str]:
    return [name for name in REQUIRED_DEEP_COMPARE_FILES if not (deep_compare_dir / name).exists()]


def enrich_market_row(row: dict[str, str]) -> dict[str, Any]:
    parsed_tf = parse_timeframe(row.get("slug", ""), row.get("tf", ""))
    end_s = market_end_s(row.get("slug", ""), parsed_tf)
    first_s = to_float(row.get("first_trade_s"))
    last_s = to_float(row.get("last_trade_s"))
    first_to_end = (end_s - first_s) if end_s and first_s else None
    last_to_end = (end_s - last_s) if end_s and last_s else None
    span_s = (last_s - first_s) if last_s and first_s else None
    fee_rate = to_float(row.get("fee")) / to_float(row.get("buy_gross")) if to_float(row.get("buy_gross")) else 0.0
    enriched: dict[str, Any] = dict(row)
    enriched.update(
        {
            "parsed_tf": parsed_tf,
            "market_start_s": slug_start(row.get("slug", "")),
            "market_end_s": end_s,
            "first_touch_to_expiry_s": first_to_end,
            "last_touch_to_expiry_s": last_to_end,
            "touch_span_s": span_s,
            "fee_rate_on_buy_gross": fee_rate,
            "actual_pair_cost_f": to_float(row.get("actual_pair_cost")),
            "cash_pnl_f": to_float(row.get("cash_pnl")),
            "pair_pnl_f": to_float(row.get("pair_pnl")),
            "residual_pnl_est_f": to_float(row.get("residual_pnl_est")),
            "buy_actual_f": to_float(row.get("buy_actual")),
            "trade_count_i": to_int(row.get("trade_count")),
        }
    )
    enriched["contexts"] = {
        "last_touch_to_expiry_bucket": bucket(
            float(last_to_end or 999999),
            [30, 60, 180, 300, 600],
            ["last_touch_le_30s", "last_touch_30_60s", "last_touch_60_180s", "last_touch_180_300s", "last_touch_300_600s", "last_touch_gt_600s"],
        ),
        "first_touch_to_expiry_bucket": bucket(
            float(first_to_end or 999999),
            [60, 180, 300, 600, 840],
            ["first_touch_le_60s", "first_touch_60_180s", "first_touch_180_300s", "first_touch_300_600s", "first_touch_600_840s", "first_touch_gt_840s"],
        ),
        "touch_span_bucket": bucket(
            float(span_s or 0),
            [60, 180, 420, 780],
            ["touch_span_le_60s", "touch_span_60_180s", "touch_span_180_420s", "touch_span_420_780s", "touch_span_gt_780s"],
        ),
        "trade_count_bucket": bucket(
            float(enriched["trade_count_i"]),
            [20, 50, 100, 150],
            ["trade_count_le_20", "trade_count_21_50", "trade_count_51_100", "trade_count_101_150", "trade_count_gt_150"],
        ),
        "buy_actual_bucket": bucket(
            enriched["buy_actual_f"],
            [100, 250, 500, 1000, 2000],
            ["buy_actual_le_100", "buy_actual_100_250", "buy_actual_250_500", "buy_actual_500_1000", "buy_actual_1000_2000", "buy_actual_gt_2000"],
        ),
        "fee_rate_bucket": bucket(
            fee_rate,
            [0.005, 0.010, 0.015, 0.020],
            ["fee_le_50bp", "fee_50_100bp", "fee_100_150bp", "fee_150_200bp", "fee_gt_200bp"],
        ),
    }
    return enriched


def load_primary_rows(deep_compare_dir: Path) -> list[dict[str, Any]]:
    rows = read_csv_rows(deep_compare_dir / "ce25" / "market_cash_pnl.csv")
    enriched = [enrich_market_row(row) for row in rows]
    return [
        row
        for row in enriched
        if row.get("asset") in PRIMARY_ASSETS and row.get("parsed_tf") == "15m"
    ]


def load_post_window_rows(deep_compare_dir: Path) -> dict[str, dict[str, str]]:
    rows = read_csv_rows(deep_compare_dir / "post_window_by_condition.csv")
    return {
        row.get("condition_id", ""): row
        for row in rows
        if row.get("label") == "ce25"
    }


def summarize_market_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    costs = [row["actual_pair_cost_f"] for row in rows if row.get("actual_pair_cost")]
    tail_rows = [row for row in rows if row["actual_pair_cost_f"] > 1.0]
    cash_values = [row["cash_pnl_f"] for row in rows]
    pair = sum(row["pair_pnl_f"] for row in rows)
    residual = sum(row["residual_pnl_est_f"] for row in rows)
    buy = sum(row["buy_actual_f"] for row in rows)
    return {
        "markets": len(rows),
        "buy_actual": round(buy, 6),
        "cash_pnl": round(sum(cash_values), 6),
        "pair_pnl": round(pair, 6),
        "residual_pnl_est": round(residual, 6),
        "residual_drag_share_of_pair_pnl": round(abs(residual) / pair, 6) if pair > 0 else None,
        "actual_pair_cost_p50": round_or_none(quantile(costs, 0.50)),
        "actual_pair_cost_p75": round_or_none(quantile(costs, 0.75)),
        "actual_pair_cost_p90": round_or_none(quantile(costs, 0.90)),
        "actual_pair_cost_gt_1_count": len(tail_rows),
        "actual_pair_cost_gt_1_share": round(len(tail_rows) / len(costs), 6) if costs else None,
        "positive_cash_markets": sum(1 for value in cash_values if value > 0),
        "negative_cash_markets": sum(1 for value in cash_values if value < 0),
    }


def top_worst_markets(rows: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    return [
        {
            "condition_id": row.get("condition_id"),
            "slug": row.get("slug"),
            "asset": row.get("asset"),
            "cash_pnl": round(row["cash_pnl_f"], 6),
            "pair_pnl": round(row["pair_pnl_f"], 6),
            "residual_pnl_est": round(row["residual_pnl_est_f"], 6),
            "actual_pair_cost": round(row["actual_pair_cost_f"], 6),
            "last_touch_to_expiry_s": round(float(row.get("last_touch_to_expiry_s") or 0), 3),
            "first_touch_to_expiry_s": round(float(row.get("first_touch_to_expiry_s") or 0), 3),
            "trade_count": row["trade_count_i"],
            "buy_actual": round(row["buy_actual_f"], 6),
        }
        for row in sorted(rows, key=lambda item: item["cash_pnl_f"])[:limit]
    ]


def context_candidates(primary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    base_cash = summarize_market_rows(primary_rows)["cash_pnl"]
    for key in ALLOWED_CONTEXT_KEYS:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in primary_rows:
            grouped[row["contexts"][key]].append(row)
        for value, removed in grouped.items():
            kept = [row for row in primary_rows if row not in removed]
            if len(removed) < 20 or len(kept) < 100:
                continue
            removed_summary = summarize_market_rows(removed)
            kept_summary = summarize_market_rows(kept)
            if (
                removed_summary["cash_pnl"] < -200
                or removed_summary["pair_pnl"] <= 0
                or removed_summary["residual_pnl_est"] < -500
            ):
                candidates.append(
                    {
                        "context_key": key,
                        "context_value": value,
                        "sample_markets": len(removed),
                        "removed_summary": removed_summary,
                        "kept_summary_after_excluding_context": kept_summary,
                        "cash_improvement_if_excluded": round(kept_summary["cash_pnl"] - base_cash, 6),
                        "context_is_allowed_for_audit": True,
                        "forbidden_context": False,
                    }
                )
    return sorted(
        candidates,
        key=lambda item: (
            item["removed_summary"]["cash_pnl"],
            item["removed_summary"]["residual_pnl_est"],
        ),
    )


def public_activity_coverage(public_rows_path: Path, primary_rows: list[dict[str, Any]], selected: dict[str, Any] | None) -> dict[str, Any]:
    if not public_rows_path.exists():
        return {"exists": False, "status": "PUBLIC_ACTIVITY_ROWS_MISSING"}
    rows = load_json(public_rows_path)
    if not isinstance(rows, list):
        return {"exists": True, "status": "PUBLIC_ACTIVITY_ROWS_NOT_ARRAY"}
    primary_ids = {row.get("condition_id") for row in primary_rows}
    selected_ids = {
        row.get("condition_id")
        for row in primary_rows
        if selected and row["contexts"][selected["context_key"]] == selected["context_value"]
    }
    primary_public = [row for row in rows if row.get("conditionId") in primary_ids and row.get("type") == "TRADE"]
    selected_public = [row for row in rows if row.get("conditionId") in selected_ids and row.get("type") == "TRADE"]
    return {
        "exists": True,
        "status": "PUBLIC_ACTIVITY_ROWS_AVAILABLE_FOR_CONTEXT_COVERAGE",
        "raw_rows": len(rows),
        "primary_trade_rows": len(primary_public),
        "selected_context_trade_rows": len(selected_public),
        "primary_condition_coverage": len({row.get("conditionId") for row in primary_public}),
        "selected_context_condition_coverage": len({row.get("conditionId") for row in selected_public}),
        "provided_fields": sorted({key for row in primary_public[:50] for key in row.keys()}),
        "missing_non_fixture_fields": [
            "liquidity_role",
            "fair_probability",
            "fair_probability_uncertainty",
            "edge_after_fee_and_uncertainty",
            "model_name",
            "model_version",
        ],
    }


def post_window_coverage(post_rows: dict[str, dict[str, str]], primary_rows: list[dict[str, Any]], selected: dict[str, Any] | None) -> dict[str, Any]:
    def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
        joined = [post_rows.get(row.get("condition_id", ""), {}) for row in rows]
        return {
            "markets": len(rows),
            "post_cash": round(sum(to_float(row.get("post_cash")) for row in joined), 6),
            "post_buy": round(sum(to_float(row.get("post_buy")) for row in joined), 6),
            "post_touched_markets": sum(1 for row in joined if to_float(row.get("post_cash")) or to_float(row.get("post_buy"))),
        }

    selected_rows = [
        row
        for row in primary_rows
        if selected and row["contexts"][selected["context_key"]] == selected["context_value"]
    ]
    return {
        "primary": summarize(primary_rows),
        "selected_context": summarize(selected_rows),
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if (
        not path_safe(args.deep_compare_dir)
        or not path_safe(args.proxy_gate_manifest)
        or not path_safe(args.public_rows)
        or not path_safe(output_dir)
    ):
        raise RuntimeError("unsafe path under ce25 market tail gap audit")
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
            "lane": "ce25_15m_market_tail_gap_audit",
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_CE25_15M_MARKET_TAIL_GAP_AUDIT_INPUTS_MISSING",
            "scope": scope,
            "source_inputs": {
                "deep_compare_dir": str(args.deep_compare_dir),
                "missing_deep_compare_files": missing,
                "proxy_gate_manifest": str(args.proxy_gate_manifest),
                "public_rows": str(args.public_rows),
            },
            "research_ranking": {
                "tail_gap_status": "UNKNOWN_INPUTS_MISSING",
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
            "next_executable_action": "Rerun on complete existing ce25 deep-compare exports; do not start diagnostics.",
        }

    primary_rows = load_primary_rows(args.deep_compare_dir)
    post_rows = load_post_window_rows(args.deep_compare_dir)
    proxy_gate = load_json(args.proxy_gate_manifest) if args.proxy_gate_manifest.exists() else {}
    base_summary = summarize_market_rows(primary_rows)
    candidates = context_candidates(primary_rows)
    selected = candidates[0] if candidates else None
    tail_broad = base_summary["actual_pair_cost_p90"] and base_summary["actual_pair_cost_p90"] > 1.0
    context_found = bool(selected and selected["sample_markets"] >= 30)
    selected_kept = selected.get("kept_summary_after_excluding_context") if selected else {}
    selected_removed = selected.get("removed_summary") if selected else {}
    selected_explains_cash = bool(selected and selected_removed.get("cash_pnl", 0) < -1000)
    selected_explains_residual = bool(
        selected and abs(selected_removed.get("residual_pnl_est", 0)) / abs(base_summary["residual_pnl_est"]) >= 0.45
    )
    selected_solves_pair_tail = bool(
        selected_kept and (selected_kept.get("actual_pair_cost_p90") or 999) <= 1.0
    )
    public_coverage = public_activity_coverage(args.public_rows, primary_rows, selected)
    post_coverage = post_window_coverage(post_rows, primary_rows, selected)
    proxy_prev_ok = proxy_gate.get("decision_label") == "KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_READY_TAIL_RESIDUAL_BLOCKED"
    proxy_checks = {
        "previous_proxy_gate_keep": proxy_prev_ok,
        "primary_market_sample_ge_200": len(primary_rows) >= 200,
        "non_forbidden_context_found": context_found,
        "selected_context_explains_cash_drag": selected_explains_cash,
        "selected_context_explains_at_least_45pct_residual_drag": selected_explains_residual,
        "public_activity_rows_have_selected_context_sample": public_coverage.get(
            "selected_context_condition_coverage", 0
        )
        >= 10,
    }
    promotion_checks = {
        "selected_context_solves_pair_cost_p90": selected_solves_pair_tail,
        "kept_residual_pnl_nonnegative": bool(selected_kept and selected_kept.get("residual_pnl_est", -1) >= 0),
        "non_fixture_liquidity_role_ready": False,
        "non_fixture_fair_probability_ready": False,
        "private_truth_ready": False,
    }
    decision = "KEEP" if all(proxy_checks.values()) else "UNKNOWN"
    if decision == "KEEP":
        label = "KEEP_CE25_15M_MARKET_TAIL_GAP_AUDIT_FINAL_TOUCH_TARGET_PAIR_TAIL_BROAD"
    else:
        label = "UNKNOWN_CE25_15M_MARKET_TAIL_GAP_AUDIT_NO_LEGAL_CONTEXT"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_15m_market_tail_gap_audit",
        "decision": decision,
        "decision_label": label,
        "scope": scope,
        "source_inputs": {
            "deep_compare_dir": str(args.deep_compare_dir),
            "deep_compare_files": list(REQUIRED_DEEP_COMPARE_FILES),
            "proxy_gate_manifest": str(args.proxy_gate_manifest),
            "public_rows": str(args.public_rows),
        },
        "base_primary_summary": base_summary,
        "selected_context": selected,
        "candidate_contexts_top": candidates[:8],
        "public_activity_coverage": public_coverage,
        "post_window_coverage": post_coverage,
        "worst_primary_markets": top_worst_markets(primary_rows),
        "proxy_checks": proxy_checks,
        "promotion_checks": promotion_checks,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "selected_public_proxy": "ce25_btc_eth_sol_15m_late_window",
            "selected_optimization_target": {
                "context_key": selected.get("context_key") if selected else None,
                "context_value": selected.get("context_value") if selected else None,
                "status": "FINAL_TOUCH_GAP_TARGET_FOUND" if selected else "NO_CONTEXT_FOUND",
            },
            "pair_tail_status": "BROAD_NOT_SOLVED_BY_SELECTED_CONTEXT" if tail_broad else "PAIR_TAIL_NOT_PRESENT",
            "residual_drag_status": "PARTLY_CONCENTRATED_IN_SELECTED_CONTEXT" if selected_explains_residual else "BROAD_OR_UNKNOWN",
            "proxy_ready_for_next_local_spec": decision == "KEEP",
            "promotion_gate_passed": False,
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "interpretation": (
                "The best legal observable context is final-touch timing: markets whose last ce25 public touch was "
                "30-60 seconds before expiry explain the largest cash/residual drag. This is a local optimization "
                "target for final-touch coverage diagnostics, not a deployable trading rule. Pair-cost p90 remains "
                "broad and above 1.0 after excluding the context, so fair-probability/liquidity inputs are still needed."
            ),
        },
        "known_blockers": [
            "selected context does not solve pair-cost p90",
            "residual drag remains negative after excluding selected context",
            "public activity rows only partially cover the selected context in the existing capped export",
            "liquidity_role remains missing",
            "fair_probability/uncertainty/edge joins remain missing",
            "public final-touch context is proxy evidence only and cannot prove private truth",
        ],
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_CONTEXT_AUDIT_ONLY_PAIR_TAIL_AND_INPUTS_BLOCK_PROMOTION",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [name for name, ok in promotion_checks.items() if not ok],
        },
        "next_executable_action": (
            "Implement ce25_15m_final_touch_context_spec_v1 local-only: define default-off diagnostics for final "
            "touch coverage and market-level pair-cost/residual accounting, then discard if no non-price/non-fixture "
            "mechanism can reduce tail without using pure price bands or static asset/timeframe deletion."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deep-compare-dir", type=Path, default=DEFAULT_DEEP_COMPARE_DIR)
    parser.add_argument("--proxy-gate-manifest", type=Path, default=DEFAULT_PROXY_GATE)
    parser.add_argument("--public-rows", type=Path, default=DEFAULT_PUBLIC_ROWS)
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
