#!/usr/bin/env python3
"""Select the next public-profile research lead from b55/ce25 evidence.

This local-only gate consumes an already exported b55/ce25 deep comparison and
the committed non-fixture input review. It separates "strongest account" from
"most copyable account" and stops before any no-order diagnostic when the real
liquidity-role/fair-probability joins are still missing.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_public_profile_next_lead_gate"
DEFAULT_DEEP_COMPARE_DIR = Path(
    "/Users/hot/web3Scientist/poly_trans_research/data/exports/"
    "candidate_deep_compare_b55_ce25_20260523_103000_to_20260524_103000_bjt"
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
)
ACCOUNTS = ("b55", "ce25")


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


def missing_deep_compare_files(deep_compare_dir: Path) -> list[str]:
    return [name for name in REQUIRED_DEEP_COMPARE_FILES if not (deep_compare_dir / name).exists()]


def load_deep_compare(deep_compare_dir: Path) -> dict[str, Any]:
    missing = missing_deep_compare_files(deep_compare_dir)
    if missing:
        raise FileNotFoundError(f"missing deep-compare files: {missing}")
    summary_rows = load_json(deep_compare_dir / "summary.json")
    followup_rows = load_json(deep_compare_dir / "post_window_cohort_followup.json")
    cohort_rows = read_csv_rows(deep_compare_dir / "cohort_groups_followup.csv")
    if not isinstance(summary_rows, list) or not isinstance(followup_rows, list):
        raise ValueError("deep-compare summary/followup must be JSON arrays")
    return {
        "summary": {str(row.get("label")): row for row in summary_rows if isinstance(row, dict)},
        "followup": {str(row.get("label")): row for row in followup_rows if isinstance(row, dict)},
        "cohorts": [row for row in cohort_rows if row.get("label") in ACCOUNTS],
    }


def account_observation(data: dict[str, Any], label: str) -> dict[str, Any]:
    summary = data["summary"].get(label) or {}
    followup = data["followup"].get(label) or {}
    cohorts = [row for row in data["cohorts"] if row.get("label") == label]
    positive_cohorts = [row for row in cohorts if to_float(row.get("cohort_cash_ex_rebate")) > 0]
    negative_cohorts = [row for row in cohorts if to_float(row.get("cohort_cash_ex_rebate")) < 0]
    timeframes = sorted({str(row.get("tf")) for row in cohorts if row.get("tf")})
    assets = sorted({str(row.get("asset")) for row in cohorts if row.get("asset")})
    return {
        "label": label,
        "buy_actual": to_float(summary.get("buy_actual")),
        "fee_rate_on_gross": to_float(summary.get("fee_rate_on_gross")),
        "old_redeem_contamination": to_float(summary.get("old_no_buy_cash")),
        "old_redeem_markets": to_int(summary.get("old_no_buy_markets")),
        "new_window_cash_ex_rebate": to_float(summary.get("with_buy_cash_ex_no_condition_rebate")),
        "rebate": to_float(summary.get("no_condition_rebate")),
        "actual_pair_cost": to_float(summary.get("actual_pair_cost")),
        "paired_actual_profit": to_float(summary.get("paired_actual_profit")),
        "paired_qty": to_float(summary.get("paired_qty")),
        "residual_rate_on_buy_qty": to_float(summary.get("resid_rate_on_buy_qty")),
        "pair_cost_p90": to_float((summary.get("per_market_actual_pair_cost") or {}).get("p90")),
        "post_window_redeem_for_window_cids": to_float(followup.get("post_cash_for_window_cids")),
        "post_window_same_condition_buy": to_float(followup.get("post_buy_same_cids")),
        "conservative_cohort_pnl_ex_rebate": to_float(
            followup.get("cohort_cash_add_post_cash_no_post_buy_subtract_post_buy")
        ),
        "conservative_cohort_pnl_including_rebate": to_float(
            followup.get("cohort_cash_add_post_cash_no_post_buy_subtract_post_buy")
        )
        + to_float(summary.get("no_condition_rebate")),
        "window_buy_conditions": to_int(followup.get("window_buy_cids")),
        "post_touched_with_buy_conditions": to_int(followup.get("post_touched_with_buy")),
        "asset_count": len(assets),
        "timeframe_count": len(timeframes),
        "assets": assets,
        "timeframes": timeframes,
        "positive_cohort_count": len(positive_cohorts),
        "negative_cohort_count": len(negative_cohorts),
    }


def strength_gate(accounts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    b55 = accounts["b55"]
    ce25 = accounts["ce25"]
    checks = {
        "b55_lower_average_pair_cost_than_ce25": b55["actual_pair_cost"] < ce25["actual_pair_cost"],
        "b55_higher_pair_profit_than_ce25": b55["paired_actual_profit"] > ce25["paired_actual_profit"],
        "b55_larger_buy_scale_than_ce25": b55["buy_actual"] > ce25["buy_actual"],
        "b55_conservative_cohort_pnl_positive": b55["conservative_cohort_pnl_ex_rebate"] > 0,
        "ce25_conservative_cohort_pnl_positive": ce25["conservative_cohort_pnl_ex_rebate"] > 0,
    }
    strongest = "b55" if all(checks.values()) else None
    return {
        "selected": strongest,
        "role": "strongest_pair_engine_and_scale_control" if strongest else "no_strength_lead_selected",
        "checks": checks,
        "interpretation": (
            "b55 remains the stronger account by average pair cost, paired profit, and scale; this does not make it "
            "the cleanest first system to mimic."
        ),
    }


def mimicability_gate(accounts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    gates: dict[str, dict[str, bool]] = {}
    for label, obs in accounts.items():
        gates[label] = {
            "positive_conservative_cohort_pnl": obs["conservative_cohort_pnl_ex_rebate"] > 0,
            "no_old_redeem_contamination": abs(obs["old_redeem_contamination"]) < 1e-9,
            "no_post_window_same_condition_buy_contamination": abs(obs["post_window_same_condition_buy"]) < 1e-9,
            "residual_rate_at_or_below_10pct": obs["residual_rate_on_buy_qty"] <= 0.10,
            "average_actual_pair_cost_below_0_98": obs["actual_pair_cost"] <= 0.98,
            "buy_scale_at_least_100k": obs["buy_actual"] >= 100_000,
            "pair_profit_positive": obs["paired_actual_profit"] > 0,
        }
    selected = "ce25" if all(gates.get("ce25", {}).values()) else None
    blockers = {label: [name for name, ok in checks.items() if not ok] for label, checks in gates.items()}
    return {
        "selected": selected,
        "role": "first_mimicability_template" if selected else "no_mimicability_template_selected",
        "gates": gates,
        "blockers": blockers,
        "interpretation": (
            "ce25 wins mimicability because the public cohort is cleaner: no old-redeem contamination, no same-condition "
            "post-window buy contamination, lower residual rate, positive conservative cohort PnL, and still sub-0.98 "
            "average actual pair cost."
        ),
    }


def cohort_summary(data: dict[str, Any], label: str) -> dict[str, Any]:
    cohorts = [row for row in data["cohorts"] if row.get("label") == label]

    def pack(rows: list[dict[str, str]], limit: int) -> list[dict[str, Any]]:
        rows = sorted(rows, key=lambda row: to_float(row.get("cohort_cash_ex_rebate")), reverse=True)
        return [
            {
                "asset": row.get("asset"),
                "timeframe": row.get("tf"),
                "markets": to_int(row.get("markets")),
                "buy_plus_post_buy": round(to_float(row.get("buy_plus_post_buy")), 6),
                "cohort_cash_ex_rebate": round(to_float(row.get("cohort_cash_ex_rebate")), 6),
                "pair_pnl_base": round(to_float(row.get("pair_pnl_base")), 6),
                "residual_pnl_est_base": round(to_float(row.get("residual_pnl_est_base")), 6),
            }
            for row in rows[:limit]
        ]

    positives = [row for row in cohorts if to_float(row.get("cohort_cash_ex_rebate")) > 0]
    negatives = [row for row in cohorts if to_float(row.get("cohort_cash_ex_rebate")) < 0]
    return {
        "top_positive": pack(positives, 8),
        "worst_negative": list(reversed(pack(list(reversed(sorted(negatives, key=lambda r: to_float(r.get('cohort_cash_ex_rebate'))))), 8))),
    }


def aggregate_rows(rows: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "markets": sum(to_int(row.get("markets")) for row in rows),
        "buy_plus_post_buy": round(sum(to_float(row.get("buy_plus_post_buy")) for row in rows), 6),
        "cohort_cash_ex_rebate": round(sum(to_float(row.get("cohort_cash_ex_rebate")) for row in rows), 6),
        "pair_pnl_base": round(sum(to_float(row.get("pair_pnl_base")) for row in rows), 6),
        "residual_pnl_est_base": round(sum(to_float(row.get("residual_pnl_est_base")) for row in rows), 6),
    }


def ce25_first_pass_proxy(data: dict[str, Any]) -> dict[str, Any]:
    ce25_rows = [row for row in data["cohorts"] if row.get("label") == "ce25"]
    core_15m = [row for row in ce25_rows if row.get("tf") == "15m" and row.get("asset") in {"BTC", "ETH", "SOL"}]
    all_15m = [row for row in ce25_rows if row.get("tf") == "15m"]
    five_min = [row for row in ce25_rows if row.get("tf") == "5m"]
    btc_5m = [row for row in ce25_rows if row.get("tf") == "5m" and row.get("asset") == "BTC"]
    return {
        "proposed_name": "ce25_late_window_pair_arb_proxy_gate_v1",
        "scope": "public-export/local-only proxy; not a no-order diagnostic and not deployable",
        "primary_subgroup": "ce25 BTC/ETH/SOL 15m late-window cohort",
        "primary_subgroup_metrics": aggregate_rows(core_15m),
        "all_ce25_15m_metrics": aggregate_rows(all_15m),
        "ce25_5m_control_metrics": aggregate_rows(five_min),
        "btc_5m_negative_control": aggregate_rows(btc_5m),
        "first_pass_rules_to_test_next": [
            "account lead ce25 only; b55 remains scale/strength control",
            "entry habitat remains late-window public activity; do not infer fair value from price band alone",
            "start with 15m BTC/ETH/SOL cohorts; keep BTC 5m as an explicit negative control",
            "require conservative cohort PnL ex rebate > 0 and no old-redeem/post-buy contamination",
            "require average actual pair cost below 0.98 and residual rate at or below 10% at account/cohort proxy level",
            "fail closed until per-trade liquidity role and non-fixture fair-probability joins exist",
        ],
        "why_not_b55_first": [
            "b55 has stronger pair engine but old-redeem contamination is nonzero",
            "b55 has same-condition post-window buy contamination",
            "b55 residual rate is above the first replication target",
            "b55 mixes 1h/4h and more assets, making the first engineering target less clean",
        ],
    }


def non_fixture_review_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "decision_label": "MISSING_NON_FIXTURE_REVIEW",
            "stop_before_no_order_diagnostic": True,
            "missing_inputs": [
                "liquidity_role",
                "fair_probability",
                "fair_probability_uncertainty",
                "edge_after_fee_and_uncertainty",
            ],
        }
    manifest = load_json(path)
    ranking = manifest.get("research_ranking") if isinstance(manifest.get("research_ranking"), dict) else {}
    return {
        "exists": True,
        "path": str(path),
        "decision_label": manifest.get("decision_label"),
        "stop_before_no_order_diagnostic": bool(ranking.get("stop_before_no_order_diagnostic", True)),
        "real_non_fixture_source_ready": bool(ranking.get("real_non_fixture_source_ready")),
        "missing_inputs": ranking.get("exact_missing_inputs") or [],
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(args.deep_compare_dir) or not path_safe(args.non_fixture_review_manifest) or not path_safe(output_dir):
        raise RuntimeError("unsafe path under public-profile next-lead gate")

    missing = missing_deep_compare_files(args.deep_compare_dir)
    review = non_fixture_review_summary(args.non_fixture_review_manifest)
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
        "dplus_failed_families_enabled_or_swept": False,
    }
    if missing:
        return {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": utc_label(),
            "lane": "public_profile_next_lead_gate",
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_PUBLIC_PROFILE_NEXT_LEAD_GATE_DEEP_COMPARE_INPUTS_MISSING",
            "scope": scope,
            "source_inputs": {
                "deep_compare_dir": str(args.deep_compare_dir),
                "missing_deep_compare_files": missing,
                "non_fixture_review_manifest": str(args.non_fixture_review_manifest),
            },
            "research_ranking": {
                "strength_lead": None,
                "mimicability_target": None,
                "control_account": None,
                "stop_before_no_order_diagnostic": True,
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
            "next_executable_action": "Provide the required deep-compare public export files or rerun this gate on a complete existing export; do not start shadow/no-order diagnostics.",
        }

    data = load_deep_compare(args.deep_compare_dir)
    accounts = {label: account_observation(data, label) for label in ACCOUNTS}
    strength = strength_gate(accounts)
    mimicability = mimicability_gate(accounts)
    ce25_selected = mimicability.get("selected") == "ce25"
    b55_control = strength.get("selected") == "b55"
    stop_no_order = bool(review.get("stop_before_no_order_diagnostic", True)) or not bool(
        review.get("real_non_fixture_source_ready")
    )
    decision = "KEEP" if ce25_selected and b55_control else "UNKNOWN"
    label = (
        "KEEP_PUBLIC_PROFILE_NEXT_LEAD_GATE_CE25_MIMICABILITY_SELECTED_B55_CONTROL"
        if decision == "KEEP"
        else "UNKNOWN_PUBLIC_PROFILE_NEXT_LEAD_GATE_NO_CLEAN_MIMICABILITY_SELECTION"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_profile_next_lead_gate",
        "decision": decision,
        "decision_label": label,
        "scope": scope,
        "source_inputs": {
            "deep_compare_dir": str(args.deep_compare_dir),
            "deep_compare_files": list(REQUIRED_DEEP_COMPARE_FILES),
            "non_fixture_review_manifest": str(args.non_fixture_review_manifest),
        },
        "account_observations": accounts,
        "strength_ranking": strength,
        "mimicability_ranking": mimicability,
        "cohort_summary": {label: cohort_summary(data, label) for label in ACCOUNTS},
        "ce25_first_pass_proxy": ce25_first_pass_proxy(data),
        "non_fixture_input_review": review,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "strength_lead": strength.get("selected"),
            "mimicability_target": mimicability.get("selected"),
            "control_account": "b55" if b55_control else None,
            "profitability_or_strength_rank_1": "b55",
            "copyability_or_mimicability_rank_1": "ce25" if ce25_selected else None,
            "stop_before_no_order_diagnostic": stop_no_order,
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "interpretation": (
                "b55 remains the best strength/control account, but ce25 is the next account to mimic because it is "
                "cleaner, lower residual, and less contaminated by old redeem or same-condition post-window buys. "
                "This is a public-proxy lead selection, not deployable evidence."
            ),
        },
        "known_blockers": [
            "non-fixture per-trade liquidity_role still missing",
            "non-fixture fair_source/probability/uncertainty/edge joins still missing",
            "ce25 per-market pair-cost p90 remains above 1.0",
            "ce25 selected public cohorts still have residual PnL drag, especially BTC 5m",
            "public export proxy cannot prove private truth, deployability, canary readiness, or promotion",
        ],
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROFILE_LEAD_SELECTION_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement ce25_late_window_pair_arb_proxy_gate_v1 local-only using the same existing deep-compare/public "
            "export rows: test ce25 15m BTC/ETH/SOL as the first mimicability subgroup, keep BTC 5m as a negative "
            "control and b55 as strength control, and fail closed until non-fixture liquidity-role/fair-probability "
            "joins exist."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deep-compare-dir", type=Path, default=DEFAULT_DEEP_COMPARE_DIR)
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
