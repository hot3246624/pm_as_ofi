#!/usr/bin/env python3
"""Define and validate the ce25 three-day pair-cost rule contract.

This local-only scorer turns the ce25 single-day public profile insight into a
fail-closed 72h validation contract. It does not fetch data or start any live,
shadow, service, or shared-WS path. A real three-day input must be supplied by a
safe offline export before this can become strategy evidence.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_three_day_pair_cost_validation_spec"
DEFAULT_ONE_DAY_SOURCE = Path(
    "xuan_research_artifacts/xuan_ce25_projected_pair_cost_residual_guard_spec_20260524T104842Z/manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)
CONTRACT_NAME = "ce25_three_day_pair_cost_validation_v1"
REQUIRED_COHORTS = ("starter_eth_sol_5m15m", "core_btc_eth_sol_5m15m", "hard_loss_pair_cost_ge_1")


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def safe_load(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path_safe(path):
        return None, "unsafe_path"
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def validation_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "account": "ce25",
        "lookback_days_min": 3,
        "purpose": "Confirm the ce25 low-pair-cost/low-residual rule is not a one-day artifact.",
        "retrospective_only": True,
        "live_translation": "Only projected_pair_cost/projected_residual may be used in runner diagnostics; realized pair_cost is never a live criterion.",
        "required_daily_cohorts": {
            "starter_eth_sol_5m15m": {
                "assets": ["ETH", "SOL"],
                "timeframes": ["5m", "15m"],
                "pair_cost_lt": 0.90,
                "residual_rate_lt": 0.15,
                "min_markets_per_day": 20,
                "require_daily_pnl_positive": True,
            },
            "core_btc_eth_sol_5m15m": {
                "assets": ["BTC", "ETH", "SOL"],
                "excluded_assets": ["XRP"],
                "timeframes": ["5m", "15m"],
                "pair_cost_lt": 0.95,
                "residual_rate_lt": 0.10,
                "min_markets_per_day": 40,
                "require_daily_pnl_positive": True,
            },
            "hard_loss_pair_cost_ge_1": {
                "assets": ["BTC", "ETH", "SOL"],
                "timeframes": ["5m", "15m"],
                "pair_cost_gte": 1.00,
                "require_daily_pnl_negative": True,
                "purpose": "Show the stop condition remains a real loss bucket across days.",
            },
        },
        "required_input_fields_per_daily_cohort": [
            "day_id",
            "account",
            "cohort",
            "markets",
            "buy_actual",
            "cohort_pnl_ex_rebate",
            "cohort_pnl_including_rebate",
            "pair_pnl",
            "residual_pnl_est",
            "wins",
            "losses",
            "avg_pair_cost",
            "residual_rate",
            "old_redeem_contamination",
            "post_window_same_condition_buy",
            "rebate",
        ],
        "optional_but_desired_fields": [
            "taker_only_sample_count",
            "taker_only_share",
            "maker_taker_role_source",
            "per_trade_liquidity_role_truth_coverage",
        ],
        "fail_closed_conditions": [
            "fewer than 3 distinct day_id values",
            "any required cohort missing for any day",
            "starter or core daily PnL is not positive",
            "starter/core average pair cost or residual rate violates threshold",
            "hard-loss pair_cost>=1.00 bucket is not negative each day",
            "old redeem contamination or post-window same-condition buy contamination is nonzero for ce25",
            "fixture data is presented as real strategy evidence",
            "strategy evidence is claimed without non-fixture per-trade liquidity role and fair-source probability joins",
        ],
    }


def one_day_observation(source: dict[str, Any]) -> dict[str, Any]:
    ce25 = ((source.get("retrospective_rule_buckets") or {}).get("ce25") or {})
    starter = ce25.get("starter_eth_sol_5m15m_pair_cost_lt_090_residual_lt_15") or {}
    core = ce25.get("core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10") or {}
    hard = ce25.get("hard_loss_btc_eth_sol_5m15m_pair_cost_ge_100") or {}
    return {
        "source_decision": source.get("decision_label"),
        "single_day_only": True,
        "starter": {
            "markets": starter.get("markets"),
            "cohort_pnl": starter.get("cohort_pnl"),
            "wins": starter.get("wins"),
            "losses": starter.get("losses"),
            "avg_actual_pair_cost": starter.get("avg_actual_pair_cost"),
            "avg_residual_rate": starter.get("avg_residual_rate"),
        },
        "core": {
            "markets": core.get("markets"),
            "cohort_pnl": core.get("cohort_pnl"),
            "wins": core.get("wins"),
            "losses": core.get("losses"),
            "avg_actual_pair_cost": core.get("avg_actual_pair_cost"),
            "avg_residual_rate": core.get("avg_residual_rate"),
        },
        "hard_loss": {
            "markets": hard.get("markets"),
            "cohort_pnl": hard.get("cohort_pnl"),
            "wins": hard.get("wins"),
            "losses": hard.get("losses"),
            "avg_actual_pair_cost": hard.get("avg_actual_pair_cost"),
            "avg_residual_rate": hard.get("avg_residual_rate"),
        },
    }


def daily_rows(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    rows = candidate.get("daily_cohort_rows")
    return rows if isinstance(rows, list) else []


def validate_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in daily_rows(candidate) if isinstance(row, dict)]
    days = sorted({str(row.get("day_id")) for row in rows if row.get("day_id") is not None})
    rows_by_day_cohort = {(str(row.get("day_id")), str(row.get("cohort"))): row for row in rows}
    fixture = bool(candidate.get("fixture"))
    blockers: list[str] = []
    if len(days) < 3:
        blockers.append("fewer_than_3_days")
    for day in days:
        for cohort in REQUIRED_COHORTS:
            if (day, cohort) not in rows_by_day_cohort:
                blockers.append(f"missing_{cohort}_for_{day}")
    daily_results: dict[str, Any] = {}
    for day in days:
        result: dict[str, Any] = {}
        for cohort in REQUIRED_COHORTS:
            row = rows_by_day_cohort.get((day, cohort))
            if not row:
                continue
            markets = as_int(row.get("markets"))
            pnl = as_float(row.get("cohort_pnl_ex_rebate"))
            pair_cost = as_float(row.get("avg_pair_cost"))
            residual = as_float(row.get("residual_rate"))
            old_redeem = as_float(row.get("old_redeem_contamination"))
            post_buy = as_float(row.get("post_window_same_condition_buy"))
            row_ok = True
            if old_redeem != 0.0:
                blockers.append(f"{day}_{cohort}_old_redeem_contamination_nonzero")
                row_ok = False
            if post_buy != 0.0:
                blockers.append(f"{day}_{cohort}_post_window_same_condition_buy_nonzero")
                row_ok = False
            if cohort == "starter_eth_sol_5m15m":
                if markets < 20:
                    blockers.append(f"{day}_starter_sample_below_20")
                    row_ok = False
                if pnl <= 0:
                    blockers.append(f"{day}_starter_pnl_not_positive")
                    row_ok = False
                if pair_cost >= 0.90:
                    blockers.append(f"{day}_starter_pair_cost_not_lt_0p90")
                    row_ok = False
                if residual >= 0.15:
                    blockers.append(f"{day}_starter_residual_not_lt_15pct")
                    row_ok = False
            elif cohort == "core_btc_eth_sol_5m15m":
                if markets < 40:
                    blockers.append(f"{day}_core_sample_below_40")
                    row_ok = False
                if pnl <= 0:
                    blockers.append(f"{day}_core_pnl_not_positive")
                    row_ok = False
                if pair_cost >= 0.95:
                    blockers.append(f"{day}_core_pair_cost_not_lt_0p95")
                    row_ok = False
                if residual >= 0.10:
                    blockers.append(f"{day}_core_residual_not_lt_10pct")
                    row_ok = False
            elif cohort == "hard_loss_pair_cost_ge_1":
                if pnl >= 0:
                    blockers.append(f"{day}_hard_loss_pnl_not_negative")
                    row_ok = False
                if pair_cost < 1.0:
                    blockers.append(f"{day}_hard_loss_pair_cost_not_gte_1p00")
                    row_ok = False
            result[cohort] = {
                "markets": markets,
                "cohort_pnl_ex_rebate": pnl,
                "avg_pair_cost": pair_cost,
                "residual_rate": residual,
                "wins": as_int(row.get("wins")),
                "losses": as_int(row.get("losses")),
                "passed": row_ok,
            }
        daily_results[day] = result
    complete_and_passed = not blockers and len(days) >= 3
    if fixture:
        evidence_status = "FIXTURE_VALIDATION_ONLY_NOT_STRATEGY_EVIDENCE"
    elif complete_and_passed:
        evidence_status = "NON_FIXTURE_THREE_DAY_PUBLIC_PROXY_READY_NOT_PRIVATE_TRUTH"
    else:
        evidence_status = "UNKNOWN_THREE_DAY_PUBLIC_PROXY_INSUFFICIENT"
    return {
        "candidate_status": "READY" if complete_and_passed else "UNKNOWN",
        "evidence_status": evidence_status,
        "fixture": fixture,
        "days": days,
        "day_count": len(days),
        "blockers": sorted(set(blockers)),
        "daily_results": daily_results,
        "strategy_evidence": (complete_and_passed and not fixture),
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    source, source_error = safe_load(args.one_day_source)
    observation = one_day_observation(source or {}) if not source_error and source else {}
    candidate_result: dict[str, Any]
    candidate_error = None
    if args.candidate_three_day_manifest is None:
        candidate_result = {
            "candidate_status": "UNKNOWN",
            "evidence_status": "UNKNOWN_THREE_DAY_INPUT_MISSING",
            "fixture": False,
            "days": [],
            "day_count": 0,
            "blockers": ["candidate_three_day_manifest_missing"],
            "daily_results": {},
            "strategy_evidence": False,
        }
    else:
        candidate, candidate_error = safe_load(args.candidate_three_day_manifest)
        candidate_result = validate_candidate(candidate or {}) if not candidate_error and candidate else {
            "candidate_status": "UNKNOWN",
            "evidence_status": "UNKNOWN_THREE_DAY_INPUT_UNREADABLE",
            "fixture": False,
            "days": [],
            "day_count": 0,
            "blockers": [candidate_error or "candidate_missing"],
            "daily_results": {},
            "strategy_evidence": False,
        }
    spec_ready = not source_error
    decision = "KEEP" if spec_ready else "UNKNOWN"
    if spec_ready:
        label = "KEEP_CE25_THREE_DAY_PAIR_COST_VALIDATION_SPEC_READY_REAL_INPUT_UNKNOWN"
    else:
        label = "UNKNOWN_CE25_THREE_DAY_PAIR_COST_VALIDATION_SPEC_INPUTS_INSUFFICIENT"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_three_day_pair_cost_validation_spec",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "new_data_fetched": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_full_store_scanned": False,
            "shared_ingress_or_live_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "one_day_source": str(args.one_day_source),
            "one_day_source_error": source_error,
            "candidate_three_day_manifest": str(args.candidate_three_day_manifest) if args.candidate_three_day_manifest else None,
            "candidate_three_day_manifest_error": candidate_error,
        },
        "validation_contract": validation_contract(),
        "one_day_observation": observation,
        "candidate_three_day_score": candidate_result,
        "research_ranking": {
            "status": label,
            "spec_ready": spec_ready,
            "three_day_real_input_status": candidate_result["evidence_status"],
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "starter_rule": "ETH/SOL 5m+15m pair_cost<0.90 residual<15%; live translation must use projected fields",
            "core_rule": "BTC/ETH/SOL 5m+15m pair_cost<0.95 residual<10%; live translation must use projected fields",
            "hard_kill_rule": "pair_cost>=1.00 must be a negative retrospective bucket and projected_pair_cost>=1.00 must fail closed live",
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [
                "three_day_non_fixture_public_proxy_missing_or_not_private_truth",
                "no per-trade liquidity_role truth",
                "no fair-source probability/uncertainty join",
                "no no-order pullback validation",
            ],
        },
        "next_executable_action": (
            "prepare a safe offline 72h ce25 public-export manifest matching ce25_three_day_pair_cost_validation_v1, "
            "or run this scorer on an already-created manifest; do not start no-order diagnostics"
        ),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--one-day-source", type=Path, default=DEFAULT_ONE_DAY_SOURCE)
    ap.add_argument("--candidate-three-day-manifest", type=Path, default=None)
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path(f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}"),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_manifest(args, args.output_dir)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
