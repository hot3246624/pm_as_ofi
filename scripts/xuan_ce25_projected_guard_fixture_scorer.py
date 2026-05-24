#!/usr/bin/env python3
"""Score synthetic pre-action fixtures against ce25 projected guard contract.

This local-only scorer consumes the committed projected-guard spec artifact and
synthetic fixtures. It computes projected pair cost/residual from pre-action
state plus an intended order, then applies the default-off guard contract. It
does not fetch data, start services, run shadow, or touch trading paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_projected_guard_fixture_scorer"
DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_projected_pair_cost_residual_guard_spec_20260524T104842Z/"
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
RAW_PRE_ACTION_FIELDS = [
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
]
PROJECTED_OUTPUT_FIELDS = [
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


def to_float(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    return float(value)


def missing_fields(row: dict[str, Any], fields: list[str]) -> list[str]:
    return [field for field in fields if field not in row or row[field] is None]


def default_fixtures() -> list[dict[str, Any]]:
    base = {
        "market_start_ts": "2026-05-24T10:00:00Z",
        "market_end_ts": "2026-05-24T10:15:00Z",
        "now_ts": "2026-05-24T10:07:00Z",
        "estimated_fee_per_share": 0.0,
    }
    return [
        {
            **base,
            "case": "starter_eth_completion_pass",
            "condition_id": "fixture-starter-eth",
            "asset": "ETH",
            "timeframe": "15m",
            "seconds_to_expiry": 480,
            "action_intent": "completion_cleanup",
            "outcome": "NO",
            "order_qty": 1.0,
            "order_price": 0.43,
            "pre_yes_qty": 10.0,
            "pre_no_qty": 9.0,
            "pre_yes_actual_cost": 4.0,
            "pre_no_actual_cost": 4.0,
            "expected_status": "ALLOW_PROJECTED_GUARD",
            "expected_primary_guard": "starter",
        },
        {
            **base,
            "case": "core_btc_pair_pass",
            "condition_id": "fixture-core-btc",
            "asset": "BTC",
            "timeframe": "5m",
            "seconds_to_expiry": 420,
            "action_intent": "completion_cleanup",
            "outcome": "NO",
            "order_qty": 5.0,
            "order_price": 0.45,
            "pre_yes_qty": 25.0,
            "pre_no_qty": 20.0,
            "pre_yes_actual_cost": 11.25,
            "pre_no_actual_cost": 9.0,
            "expected_status": "ALLOW_PROJECTED_GUARD",
            "expected_primary_guard": "core",
        },
        {
            **base,
            "case": "pair_cost_hard_kill",
            "condition_id": "fixture-hard-pair",
            "asset": "SOL",
            "timeframe": "15m",
            "seconds_to_expiry": 420,
            "action_intent": "completion_cleanup",
            "outcome": "NO",
            "order_qty": 1.0,
            "order_price": 0.55,
            "pre_yes_qty": 10.0,
            "pre_no_qty": 9.0,
            "pre_yes_actual_cost": 5.5,
            "pre_no_actual_cost": 4.95,
            "expected_status": "BLOCK_HARD_KILL_PAIR_COST_GTE_1_00",
        },
        {
            **base,
            "case": "near_close_residual_hard_kill",
            "condition_id": "fixture-hard-residual",
            "asset": "ETH",
            "timeframe": "15m",
            "seconds_to_expiry": 240,
            "action_intent": "completion_cleanup",
            "outcome": "YES",
            "order_qty": 2.0,
            "order_price": 0.40,
            "pre_yes_qty": 20.0,
            "pre_no_qty": 4.0,
            "pre_yes_actual_cost": 8.0,
            "pre_no_actual_cost": 1.6,
            "expected_status": "BLOCK_HARD_KILL_RESIDUAL_NEAR_CLOSE",
        },
        {
            **base,
            "case": "final_60s_initiation_block",
            "condition_id": "fixture-final-init",
            "asset": "SOL",
            "timeframe": "5m",
            "seconds_to_expiry": 45,
            "action_intent": "initiation",
            "outcome": "YES",
            "order_qty": 1.0,
            "order_price": 0.40,
            "pre_yes_qty": 10.0,
            "pre_no_qty": 10.0,
            "pre_yes_actual_cost": 4.0,
            "pre_no_actual_cost": 4.0,
            "expected_status": "BLOCK_FINAL_60S_INITIATION",
        },
        {
            **base,
            "case": "final_1m_5m_cleanup_pass",
            "condition_id": "fixture-final-cleanup",
            "asset": "SOL",
            "timeframe": "5m",
            "seconds_to_expiry": 120,
            "action_intent": "completion_cleanup",
            "outcome": "NO",
            "order_qty": 2.0,
            "order_price": 0.40,
            "pre_yes_qty": 12.0,
            "pre_no_qty": 10.0,
            "pre_yes_actual_cost": 4.8,
            "pre_no_actual_cost": 4.0,
            "expected_status": "ALLOW_PROJECTED_GUARD",
            "expected_primary_guard": "starter",
        },
        {
            **base,
            "case": "missing_pre_action_field_fail_closed",
            "condition_id": "fixture-missing-pre",
            "asset": "ETH",
            "timeframe": "15m",
            "seconds_to_expiry": 480,
            "action_intent": "completion_cleanup",
            "outcome": "NO",
            "order_qty": 1.0,
            "order_price": 0.43,
            "pre_yes_qty": 10.0,
            "pre_no_qty": 9.0,
            "pre_yes_actual_cost": 4.0,
            "expected_status": "FAIL_CLOSED_PRE_ACTION_FIELD_MISSING",
        },
    ]


def load_fixture_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return default_fixtures()
    if not path_safe(path):
        raise RuntimeError(f"unsafe fixture path: {path}")
    obj = load_json(path)
    rows = obj.get("fixtures") if isinstance(obj, dict) else obj
    if not isinstance(rows, list):
        raise RuntimeError("fixture file must be a list or an object with fixtures")
    return rows


def compute_projected(row: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    missing = missing_fields(row, RAW_PRE_ACTION_FIELDS)
    if missing:
        return None, {
            "status": "FAIL_CLOSED_PRE_ACTION_FIELD_MISSING",
            "missing_fields": missing,
        }
    try:
        order_qty = to_float(row["order_qty"])
        order_price = to_float(row["order_price"])
        fee = to_float(row["estimated_fee_per_share"])
        pre_yes_qty = to_float(row["pre_yes_qty"])
        pre_no_qty = to_float(row["pre_no_qty"])
        pre_yes_cost = to_float(row["pre_yes_actual_cost"])
        pre_no_cost = to_float(row["pre_no_actual_cost"])
    except (TypeError, ValueError) as exc:
        return None, {"status": "FAIL_CLOSED_NUMERIC_FIELD_INVALID", "error": str(exc)}
    if min(order_qty, order_price, fee, pre_yes_qty, pre_no_qty, pre_yes_cost, pre_no_cost) < 0:
        return None, {"status": "FAIL_CLOSED_NEGATIVE_NUMERIC_FIELD"}
    if order_qty <= 0:
        return None, {"status": "FAIL_CLOSED_ORDER_QTY_NONPOSITIVE"}
    outcome = str(row["outcome"]).upper()
    if outcome not in {"YES", "NO"}:
        return None, {"status": "FAIL_CLOSED_OUTCOME_INVALID"}
    order_actual_cost = order_qty * (order_price + fee)
    projected_yes_qty = pre_yes_qty + (order_qty if outcome == "YES" else 0.0)
    projected_no_qty = pre_no_qty + (order_qty if outcome == "NO" else 0.0)
    projected_yes_cost = pre_yes_cost + (order_actual_cost if outcome == "YES" else 0.0)
    projected_no_cost = pre_no_cost + (order_actual_cost if outcome == "NO" else 0.0)
    if projected_yes_qty <= 0 or projected_no_qty <= 0:
        return None, {"status": "FAIL_CLOSED_PAIR_SIDE_QTY_NONPOSITIVE"}
    projected_pair_qty = min(projected_yes_qty, projected_no_qty)
    if projected_pair_qty <= 0:
        return None, {"status": "FAIL_CLOSED_PROJECTED_PAIR_QTY_NONPOSITIVE"}
    yes_avg_cost = projected_yes_cost / projected_yes_qty
    no_avg_cost = projected_no_cost / projected_no_qty
    projected_pair_cost = yes_avg_cost + no_avg_cost
    projected_residual_qty = abs(projected_yes_qty - projected_no_qty)
    projected_total_bought_qty = projected_yes_qty + projected_no_qty
    if projected_total_bought_qty <= 0:
        return None, {"status": "FAIL_CLOSED_PROJECTED_TOTAL_QTY_NONPOSITIVE"}
    projected = {
        **row,
        "outcome": outcome,
        "order_actual_cost": round(order_actual_cost, 12),
        "projected_yes_qty": round(projected_yes_qty, 12),
        "projected_no_qty": round(projected_no_qty, 12),
        "projected_yes_actual_cost": round(projected_yes_cost, 12),
        "projected_no_actual_cost": round(projected_no_cost, 12),
        "projected_pair_qty": round(projected_pair_qty, 12),
        "projected_pair_cost": round(projected_pair_cost, 12),
        "projected_residual_qty": round(projected_residual_qty, 12),
        "projected_total_bought_qty": round(projected_total_bought_qty, 12),
        "projected_residual_rate_on_bought_qty": round(
            projected_residual_qty / projected_total_bought_qty, 12
        ),
    }
    return projected, None


def evaluate_projected(projected: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    required = list(contract.get("required_pre_action_fields") or [])
    missing = missing_fields(projected, required)
    if missing:
        return {
            "status": "FAIL_CLOSED_PROJECTED_FIELD_MISSING",
            "allowed": False,
            "missing_fields": missing,
        }

    asset = str(projected["asset"])
    timeframe = str(projected["timeframe"])
    action_intent = str(projected["action_intent"])
    seconds_to_expiry = to_float(projected["seconds_to_expiry"])
    pair_cost = to_float(projected["projected_pair_cost"])
    residual_rate = to_float(projected["projected_residual_rate_on_bought_qty"])

    if pair_cost >= 1.0:
        return {
            "status": "BLOCK_HARD_KILL_PAIR_COST_GTE_1_00",
            "allowed": False,
            "projected_pair_cost": pair_cost,
        }
    if seconds_to_expiry <= 300 and residual_rate > 0.20:
        return {
            "status": "BLOCK_HARD_KILL_RESIDUAL_NEAR_CLOSE",
            "allowed": False,
            "projected_residual_rate_on_bought_qty": residual_rate,
            "seconds_to_expiry": seconds_to_expiry,
        }
    if seconds_to_expiry <= 60 and action_intent == "initiation":
        return {
            "status": "BLOCK_FINAL_60S_INITIATION",
            "allowed": False,
            "seconds_to_expiry": seconds_to_expiry,
        }
    if 60 < seconds_to_expiry <= 300 and action_intent != "completion_cleanup":
        return {
            "status": "BLOCK_FINAL_1M_5M_NOT_COMPLETION_OR_CLEANUP",
            "allowed": False,
            "seconds_to_expiry": seconds_to_expiry,
            "action_intent": action_intent,
        }

    starter = contract["starter_guard"]
    core = contract["core_guard"]
    matched: list[str] = []
    if (
        asset in set(starter["assets"])
        and timeframe in set(starter["timeframes"])
        and pair_cost < float(starter["projected_pair_cost_lt"])
        and residual_rate < float(starter["projected_residual_rate_lt"])
    ):
        matched.append("starter")
    if (
        asset in set(core["assets"])
        and asset not in set(core.get("excluded_assets") or [])
        and timeframe in set(core["timeframes"])
        and pair_cost < float(core["projected_pair_cost_lt"])
        and residual_rate < float(core["projected_residual_rate_lt"])
    ):
        matched.append("core")
    if not matched:
        return {
            "status": "BLOCK_NO_PROJECTED_GUARD_MATCH",
            "allowed": False,
            "projected_pair_cost": pair_cost,
            "projected_residual_rate_on_bought_qty": residual_rate,
        }
    primary = "starter" if "starter" in matched else matched[0]
    return {
        "status": "ALLOW_PROJECTED_GUARD",
        "allowed": True,
        "primary_guard": primary,
        "matched_guards": matched,
        "strategy_evidence": False,
        "projected_pair_cost": pair_cost,
        "projected_residual_rate_on_bought_qty": residual_rate,
    }


def score_fixtures(
    rows: list[dict[str, Any]], contract: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        projected, compute_error = compute_projected(row)
        if compute_error:
            evaluation = {"allowed": False, **compute_error}
        else:
            evaluation = evaluate_projected(projected or {}, contract)
        expected_status = row.get("expected_status")
        expected_primary = row.get("expected_primary_guard")
        expected_ok = evaluation.get("status") == expected_status
        if expected_primary is not None:
            expected_ok = expected_ok and evaluation.get("primary_guard") == expected_primary
        scored.append(
            {
                "case": row.get("case"),
                "expected_status": expected_status,
                "expected_primary_guard": expected_primary,
                "expected_ok": expected_ok,
                "evaluation": evaluation,
                "projected_fields": {
                    field: projected[field]
                    for field in PROJECTED_OUTPUT_FIELDS
                    if projected is not None and field in projected
                },
            }
        )

    projected_probe = dict(rows[0]) if rows else {}
    projected, _ = compute_projected(projected_probe)
    missing_projected_eval = None
    if projected:
        projected.pop("projected_pair_cost", None)
        missing_projected_eval = evaluate_projected(projected, contract)

    checks = {
        "all_fixture_expectations_met": all(item["expected_ok"] for item in scored),
        "starter_fixture_allowed": any(
            item["evaluation"].get("allowed")
            and item["evaluation"].get("primary_guard") == "starter"
            for item in scored
        ),
        "core_fixture_allowed": any(
            item["evaluation"].get("allowed")
            and item["evaluation"].get("primary_guard") == "core"
            for item in scored
        ),
        "pair_cost_hard_kill_blocked": any(
            item["evaluation"].get("status") == "BLOCK_HARD_KILL_PAIR_COST_GTE_1_00"
            for item in scored
        ),
        "residual_near_close_hard_kill_blocked": any(
            item["evaluation"].get("status") == "BLOCK_HARD_KILL_RESIDUAL_NEAR_CLOSE"
            for item in scored
        ),
        "final60_initiation_blocked": any(
            item["evaluation"].get("status") == "BLOCK_FINAL_60S_INITIATION"
            for item in scored
        ),
        "missing_pre_action_field_fail_closed": any(
            item["evaluation"].get("status") == "FAIL_CLOSED_PRE_ACTION_FIELD_MISSING"
            for item in scored
        ),
        "missing_projected_field_fail_closed": (
            missing_projected_eval is not None
            and missing_projected_eval.get("status") == "FAIL_CLOSED_PROJECTED_FIELD_MISSING"
            and "projected_pair_cost" in missing_projected_eval.get("missing_fields", [])
        ),
    }
    return scored, {"checks": checks, "missing_projected_field_probe": missing_projected_eval}


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(args.spec_manifest) or not path_safe(output_dir):
        raise RuntimeError("unsafe path under ce25 projected guard fixture scorer")
    spec = load_json(args.spec_manifest)
    contract = spec.get("projected_guard_contract") if isinstance(spec, dict) else {}
    fixtures = load_fixture_rows(args.fixture_file)
    scope = {
        "local_only": True,
        "synthetic_fixtures_only": True,
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
    if not contract:
        decision = "UNKNOWN"
        label = "UNKNOWN_CE25_PROJECTED_GUARD_FIXTURE_SCORER_SPEC_MISSING"
        scored: list[dict[str, Any]] = []
        score = {"checks": {"spec_contract_present": False}}
    else:
        scored, score = score_fixtures(fixtures, contract)
        checks = {
            "spec_keep": spec.get("decision_label")
            == "KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_READY",
            "contract_present": bool(contract),
            "retrospective_warning_present": bool(
                spec.get("research_ranking", {}).get("retrospective_bucket_warning")
            ),
            "promotion_spec_false": not bool(spec.get("promotion_gate", {}).get("passed")),
            **score["checks"],
        }
        score["checks"] = checks
        blockers = [name for name, ok in checks.items() if not ok]
        decision = "KEEP" if not blockers else "UNKNOWN"
        label = (
            "KEEP_CE25_PROJECTED_GUARD_FIXTURE_SCORER_READY"
            if decision == "KEEP"
            else "UNKNOWN_CE25_PROJECTED_GUARD_FIXTURE_SCORER_INPUTS_INSUFFICIENT"
        )
        score["blockers"] = blockers

    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_projected_guard_fixture_scorer",
        "decision": decision,
        "decision_label": label,
        "scope": scope,
        "source_inputs": {
            "spec_manifest": str(args.spec_manifest),
            "fixture_file": str(args.fixture_file) if args.fixture_file else None,
            "fixtures_are_synthetic": True,
        },
        "score": score,
        "fixture_results": scored,
        "retrospective_rule_bucket_status": {
            "status": "REFERENCE_ONLY_NOT_LIVE_CRITERIA",
            "ce25_core_bucket": spec.get("retrospective_rule_buckets", {})
            .get("ce25", {})
            .get("core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10"),
            "ce25_hard_loss_bucket": spec.get("retrospective_rule_buckets", {})
            .get("ce25", {})
            .get("hard_loss_btc_eth_sol_5m15m_pair_cost_ge_100"),
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "scorer_ready": decision == "KEEP",
            "fixture_strategy_evidence": False,
            "next_local_only_target": "ce25_projected_guard_runner_instrumentation_spec_v1",
            "next_target_reason": (
                "fixture scorer validates formulae; next inspect whether a runner can expose the same pre-action "
                "fields as diagnostics without behavior changes"
            ),
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "promotion_gate": {
            "passed": False,
            "status": "FIXTURE_SCORER_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [
                "synthetic fixtures are not strategy evidence",
                "projected guard not wired as runner diagnostic",
                "no authenticated owner order/fill/queue truth",
                "no no-order diagnostic approval from this artifact",
            ],
        },
        "next_executable_action": (
            "Implement ce25_projected_guard_runner_instrumentation_spec_v1 local-only: inspect runner pre-action "
            "state availability for projected pair cost/residual diagnostics, prove default-off/no behavior change, "
            "or reject the line if required fields cannot be produced safely."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec-manifest", type=Path, default=DEFAULT_SPEC_MANIFEST)
    parser.add_argument("--fixture-file", type=Path)
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
