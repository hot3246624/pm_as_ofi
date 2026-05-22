#!/usr/bin/env python3
"""Score public/proxy complete-set pair-cost context for D+ research.

The verifier consumes public Polymarket trade rows plus no-order residual-tail
exemplar score output. It quantifies same-condition opposite-outcome pairing as
proxy inspiration only. It does not read private truth, replay/raw stores,
events JSONL, sockets, SSH, or live paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_PUBLIC_TRADES = Path(
    "xuan_research_artifacts/"
    "public_profile_trades_0xce25e214d5cfe4f459cf67f08df581885aae7fdc_20260522T094916Z/"
    "trades.json"
)
DEFAULT_EXEMPLAR_SCORE = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_residual_tail_exemplar_driver_20260522T083136Z/"
    "local_review_20260522T092012Z/residual_tail_exemplar_score/score.json"
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

TRADE_REQUIRED_FIELDS = (
    "conditionId",
    "slug",
    "outcome",
    "side",
    "timestamp",
    "price",
    "size",
)

EXEMPLAR_REQUIRED_FIELDS = (
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_ms",
    "source_sequence_id",
    "quote_intent_id",
    "source_order_id",
)


@dataclass
class Lot:
    outcome: str
    qty: float
    price: float
    ts_s: float
    tx: str


@dataclass
class Pair:
    condition_id: str
    slug: str
    qty: float
    first_outcome: str
    second_outcome: str
    first_price: float
    second_price: float
    first_ts_s: float
    second_ts_s: float
    pair_cost_sum: float
    pair_fee_per_share: float
    pair_edge: float
    near_dual_outcome_window_s: float


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1.0 - price)


def normalize_outcome(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"up", "yes"}:
        return "Up"
    if text.lower() in {"down", "no"}:
        return "Down"
    return text


def market_family(slug: str) -> str:
    return slug.split("-")[0] if slug else "unknown"


def timeframe_s(slug: str) -> int | None:
    parts = slug.split("-")
    for index, part in enumerate(parts):
        if part.endswith("m") and part[:-1].isdigit():
            return int(part[:-1]) * 60
        if part.endswith("h") and part[:-1].isdigit():
            return int(part[:-1]) * 3600
        if part == "5m":
            return 300
        if part == "15m":
            return 900
        if index + 1 < len(parts) and part.isdigit() and parts[index + 1] == "m":
            return int(part) * 60
    return None


def validate_trade_rows(rows: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(rows, list):
        return [], {"rows_are_list": False, "missing_by_index": {"root": ["list"]}}
    valid: list[dict[str, Any]] = []
    missing_by_index: dict[str, list[str]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            missing_by_index[str(index)] = ["object"]
            continue
        missing = [field for field in TRADE_REQUIRED_FIELDS if field not in row]
        if missing:
            missing_by_index[str(index)] = missing
            continue
        valid.append(row)
    return valid, {
        "rows_are_list": True,
        "row_count": len(rows),
        "valid_row_count": len(valid),
        "missing_by_index_sample": dict(list(missing_by_index.items())[:10]),
        "missing_row_count": len(missing_by_index),
    }


def pair_condition(
    condition_id: str,
    slug: str,
    rows: list[dict[str, Any]],
    *,
    fee_rate: float,
) -> tuple[list[Pair], dict[str, Any]]:
    queues: dict[str, deque[Lot]] = {"Up": deque(), "Down": deque()}
    pairs: list[Pair] = []
    rows_sorted = sorted(rows, key=lambda row: (as_float(row.get("timestamp")), str(row.get("transactionHash", ""))))
    outcome_qty: dict[str, float] = defaultdict(float)
    outcome_cost: dict[str, float] = defaultdict(float)
    side_count: dict[str, int] = defaultdict(int)
    for row in rows_sorted:
        outcome = normalize_outcome(row.get("outcome"))
        side = str(row.get("side", "")).upper()
        qty = as_float(row.get("size"))
        price = as_float(row.get("price"))
        ts_s = as_float(row.get("timestamp"))
        tx = str(row.get("transactionHash", ""))
        if side != "BUY" or outcome not in queues or qty <= 0:
            continue
        side_count[side] += 1
        outcome_qty[outcome] += qty
        outcome_cost[outcome] += qty * price
        lot = Lot(outcome=outcome, qty=qty, price=price, ts_s=ts_s, tx=tx)
        opposite = "Down" if outcome == "Up" else "Up"
        while lot.qty > 1e-12 and queues[opposite]:
            opp = queues[opposite][0]
            pair_qty = min(lot.qty, opp.qty)
            first = opp
            second = lot
            pair_fee = fee_per_share(first.price, fee_rate) + fee_per_share(second.price, fee_rate)
            pair_cost_sum = first.price + second.price
            pair_edge = 1.0 - pair_cost_sum - pair_fee
            pairs.append(
                Pair(
                    condition_id=condition_id,
                    slug=slug,
                    qty=pair_qty,
                    first_outcome=first.outcome,
                    second_outcome=second.outcome,
                    first_price=first.price,
                    second_price=second.price,
                    first_ts_s=first.ts_s,
                    second_ts_s=second.ts_s,
                    pair_cost_sum=pair_cost_sum,
                    pair_fee_per_share=pair_fee,
                    pair_edge=pair_edge,
                    near_dual_outcome_window_s=abs(second.ts_s - first.ts_s),
                )
            )
            lot.qty -= pair_qty
            opp.qty -= pair_qty
            if opp.qty <= 1e-12:
                queues[opposite].popleft()
        if lot.qty > 1e-12:
            queues[outcome].append(lot)

    residual_qty = sum(lot.qty for q in queues.values() for lot in q)
    residual_cost = sum(lot.qty * lot.price for q in queues.values() for lot in q)
    matched_qty = sum(pair.qty for pair in pairs)
    weighted_edge_sum = sum(pair.qty * pair.pair_edge for pair in pairs)
    weighted_pair_cost_sum = sum(pair.qty * pair.pair_cost_sum for pair in pairs)
    pair_fee_cost = sum(pair.qty * pair.pair_fee_per_share for pair in pairs)
    near_5s_qty = sum(pair.qty for pair in pairs if pair.near_dual_outcome_window_s <= 5.0)
    condition_score = {
        "condition_id": condition_id,
        "slug": slug,
        "market_family": market_family(slug),
        "timeframe_s": timeframe_s(slug),
        "trade_count": len(rows_sorted),
        "buy_count": side_count.get("BUY", 0),
        "outcome_qty": dict(sorted(outcome_qty.items())),
        "outcome_cost": {key: round(value, 8) for key, value in sorted(outcome_cost.items())},
        "both_outcomes": all(outcome_qty.get(outcome, 0.0) > 0 for outcome in ("Up", "Down")),
        "complete_set_matched_qty": round(matched_qty, 8),
        "complete_set_pair_count": len(pairs),
        "weighted_pair_cost_sum": round(weighted_pair_cost_sum / matched_qty, 8) if matched_qty else None,
        "weighted_pair_fee_per_share": round(pair_fee_cost / matched_qty, 8) if matched_qty else None,
        "weighted_pair_edge": round(weighted_edge_sum / matched_qty, 8) if matched_qty else None,
        "positive_edge_qty": round(sum(pair.qty for pair in pairs if pair.pair_edge > 0), 8),
        "nonpositive_edge_qty": round(sum(pair.qty for pair in pairs if pair.pair_edge <= 0), 8),
        "near_dual_outcome_qty_lte_5s": round(near_5s_qty, 8),
        "near_dual_outcome_qty_share_lte_5s": round(near_5s_qty / matched_qty, 8) if matched_qty else 0.0,
        "unpaired_residual_qty": round(residual_qty, 8),
        "unpaired_residual_cost": round(residual_cost, 8),
    }
    return pairs, condition_score


def score_public_trades(rows: list[dict[str, Any]], *, fee_rate: float) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("conditionId", "")), str(row.get("slug", "")))].append(row)
    all_pairs: list[Pair] = []
    condition_scores: list[dict[str, Any]] = []
    for (condition_id, slug), group_rows in grouped.items():
        pairs, condition_score = pair_condition(condition_id, slug, group_rows, fee_rate=fee_rate)
        all_pairs.extend(pairs)
        condition_scores.append(condition_score)

    trade_count = len(rows)
    condition_count = len(grouped)
    both_condition_count = sum(1 for score in condition_scores if score["both_outcomes"])
    matched_qty = sum(as_float(score["complete_set_matched_qty"]) for score in condition_scores)
    residual_qty = sum(as_float(score["unpaired_residual_qty"]) for score in condition_scores)
    residual_cost = sum(as_float(score["unpaired_residual_cost"]) for score in condition_scores)
    positive_edge_qty = sum(as_float(score["positive_edge_qty"]) for score in condition_scores)
    near_5s_qty = sum(as_float(score["near_dual_outcome_qty_lte_5s"]) for score in condition_scores)
    weighted_pair_edge = (
        sum(pair.qty * pair.pair_edge for pair in all_pairs) / matched_qty
        if matched_qty
        else None
    )
    weighted_pair_cost = (
        sum(pair.qty * pair.pair_cost_sum for pair in all_pairs) / matched_qty
        if matched_qty
        else None
    )
    by_family: dict[str, int] = defaultdict(int)
    by_timeframe: dict[str, int] = defaultdict(int)
    for score in condition_scores:
        by_family[str(score["market_family"])] += as_int(score["trade_count"])
        by_timeframe[str(score["timeframe_s"])] += as_int(score["trade_count"])
    ranked_conditions = sorted(
        condition_scores,
        key=lambda score: (
            as_float(score["complete_set_matched_qty"]),
            as_float(score["positive_edge_qty"]),
            -as_float(score["unpaired_residual_cost"]),
        ),
        reverse=True,
    )
    return {
        "trade_count": trade_count,
        "condition_count": condition_count,
        "both_outcome_condition_count": both_condition_count,
        "both_outcome_condition_ratio": round(both_condition_count / condition_count, 8) if condition_count else 0.0,
        "complete_set_pair_count": len(all_pairs),
        "complete_set_matched_qty": round(matched_qty, 8),
        "weighted_pair_cost_sum": round(weighted_pair_cost, 8) if weighted_pair_cost is not None else None,
        "weighted_pair_edge": round(weighted_pair_edge, 8) if weighted_pair_edge is not None else None,
        "positive_edge_qty": round(positive_edge_qty, 8),
        "positive_edge_qty_share": round(positive_edge_qty / matched_qty, 8) if matched_qty else 0.0,
        "near_dual_outcome_qty_lte_5s": round(near_5s_qty, 8),
        "near_dual_outcome_qty_share_lte_5s": round(near_5s_qty / matched_qty, 8) if matched_qty else 0.0,
        "unpaired_residual_qty": round(residual_qty, 8),
        "unpaired_residual_cost": round(residual_cost, 8),
        "unpaired_residual_qty_share_of_public_size": round(
            residual_qty / (matched_qty + residual_qty), 8
        )
        if (matched_qty + residual_qty)
        else 0.0,
        "by_family_trade_count": dict(sorted(by_family.items())),
        "by_timeframe_s_trade_count": dict(sorted(by_timeframe.items())),
        "ranked_condition_complete_set_pair_cost_groups": ranked_conditions[:20],
    }


def no_order_contract(score: dict[str, Any]) -> dict[str, Any]:
    checks = score.get("checks") if isinstance(score.get("checks"), dict) else {}
    metrics = score.get("exemplar_metrics") if isinstance(score.get("exemplar_metrics"), dict) else {}
    coverage_by_field = (
        metrics.get("required_field_coverage_by_field")
        if isinstance(metrics.get("required_field_coverage_by_field"), dict)
        else {}
    )
    missing_fields = [
        field for field in EXEMPLAR_REQUIRED_FIELDS if as_float(coverage_by_field.get(field), -1.0) < 1.0
    ]
    return {
        "score_status": score.get("status") or score.get("decision_label"),
        "field_contract_ok": checks.get("report_shape_ok") is True
        and as_float(metrics.get("required_field_coverage")) >= 1.0
        and as_float(metrics.get("source_sequence_coverage")) >= 0.95
        and not missing_fields,
        "required_field_coverage": as_float(metrics.get("required_field_coverage")),
        "source_sequence_coverage": as_float(metrics.get("source_sequence_coverage")),
        "quote_intent_coverage": as_float(metrics.get("quote_intent_coverage")),
        "source_order_coverage": as_float(metrics.get("source_order_coverage")),
        "missing_fields": missing_fields,
        "metric_budgets_ok": checks.get("metric_budgets_ok") is True,
        "metric_budgets_required_for_this_verifier": False,
    }


def write_condition_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "condition_id",
        "slug",
        "market_family",
        "timeframe_s",
        "trade_count",
        "both_outcomes",
        "complete_set_matched_qty",
        "complete_set_pair_count",
        "weighted_pair_cost_sum",
        "weighted_pair_fee_per_share",
        "weighted_pair_edge",
        "positive_edge_qty",
        "nonpositive_edge_qty",
        "near_dual_outcome_qty_lte_5s",
        "near_dual_outcome_qty_share_lte_5s",
        "unpaired_residual_qty",
        "unpaired_residual_cost",
    ]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def review(
    *,
    public_trades_path: Path,
    exemplar_score_path: Path,
    output_dir: Path,
    fee_rate: float,
    fee_rate_source: str,
) -> dict[str, Any]:
    paths = {
        "public_trades": public_trades_path,
        "residual_tail_exemplar_score": exemplar_score_path,
    }
    paths_safe = {key: path_safe(path) for key, path in paths.items()}
    paths_exist = {key: path.exists() for key, path in paths.items()}
    public_rows_raw = load_json(public_trades_path)
    valid_rows, trade_schema = validate_trade_rows(public_rows_raw)
    exemplar_score = load_json(exemplar_score_path)
    no_order = no_order_contract(exemplar_score)
    public_score = score_public_trades(valid_rows, fee_rate=fee_rate)
    condition_csv = output_dir / "condition_pair_cost_groups.csv"
    write_condition_csv(condition_csv, public_score["ranked_condition_complete_set_pair_cost_groups"])

    public_proxy_ok = (
        trade_schema["valid_row_count"] >= 500
        and public_score["condition_count"] >= 20
        and public_score["both_outcome_condition_ratio"] >= 0.80
        and public_score["complete_set_matched_qty"] > 0
        and public_score["weighted_pair_cost_sum"] is not None
    )
    positive_pair_edge = as_float(public_score["weighted_pair_edge"], -999.0) > 0
    spec_evidence_ok = all(paths_safe.values()) and all(paths_exist.values()) and public_proxy_ok and no_order["field_contract_ok"]
    decision = "KEEP" if spec_evidence_ok else "UNKNOWN"
    label = (
        "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_READY"
        if spec_evidence_ok
        else "UNKNOWN_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_INPUT_GAP"
    )
    blockers: list[str] = []
    limitations: list[str] = []
    if not all(paths_safe.values()) or not all(paths_exist.values()):
        blockers.append("unsafe_or_missing_input_paths")
    if not public_proxy_ok:
        blockers.append("public_trade_pair_cost_proxy_signal_insufficient")
    if not no_order["field_contract_ok"]:
        blockers.append("no_order_exemplar_field_contract_insufficient")
    if not positive_pair_edge:
        limitations.append("public_proxy_weighted_pair_edge_not_positive")

    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_complete_set_pair_cost_context_verifier",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_public_proxy_pair_cost_context_verifier",
        "inputs": {key: str(path) for key, path in paths.items()},
        "path_safety": paths_safe,
        "paths_exist": paths_exist,
        "blockers": blockers,
        "limitations": limitations,
        "outputs": {
            "score_json": str(output_dir / "complete_set_pair_cost_context_score.json"),
            "condition_pair_cost_groups_csv": str(condition_csv),
        },
        "fee_policy": {
            "fee_rate": fee_rate,
            "fee_rate_source": fee_rate_source,
            "fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "account_average_fee_rate_used": False,
        },
        "public_proxy_boundary": {
            "public_profile_is_proxy_inspiration_only": True,
            "learning_target_private_truth_available": False,
            "learning_target_private_truth_required": False,
            "our_private_truth_ready": False,
        },
        "public_trade_schema": trade_schema,
        "public_complete_set_pair_cost_context": public_score,
        "no_order_residual_tail_field_contract": no_order,
        "interpretation": {
            "complete_set_pair_cost_proxy_quantified": public_proxy_ok,
            "weighted_pair_edge_positive": positive_pair_edge,
            "direct_timeframe_match_to_current_dplus_shadow": "NO_PUBLIC_PROFILE_IS_15M_WHILE_CURRENT_DPLUS_RUNNER_IS_5M",
            "usefulness": (
                "The profile is useful as a complete-set/pair-cost design pattern if the local candidate "
                "pipeline can reproduce sample-preserving pair-cost context controls on our 5m D+ universe."
            ),
            "not_evidence_for": [
                "learning target private truth",
                "our private order/fill/inventory truth",
                "deployable",
                "shadow-ready",
                "canary",
                "promotion",
            ],
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "Verifier output quantifies public/proxy complete-set pair-cost context. It can nominate a "
                "local candidate-pipeline adapter, but cannot promote a strategy by itself."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROXY_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "Implement a local candidate-pipeline complete-set pair-cost context adapter only if it preserves "
            "sample and improves residual/pair-cost metrics versus late_repair90; do not start remote shadow."
            if spec_evidence_ok
            else "Collect exact missing public trade/no-order fields before building a candidate-pipeline adapter."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-trades", type=Path, default=DEFAULT_PUBLIC_TRADES)
    parser.add_argument("--exemplar-score", type=Path, default=DEFAULT_EXEMPLAR_SCORE)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument(
        "--fee-rate-source",
        default="explicit_public_proxy_pair_cost_context_fee_rate_zero_for_proxy_scoring",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = review(
        public_trades_path=args.public_trades,
        exemplar_score_path=args.exemplar_score,
        output_dir=args.output_dir,
        fee_rate=args.fee_rate,
        fee_rate_source=args.fee_rate_source,
    )
    score_path = args.output_dir / "complete_set_pair_cost_context_score.json"
    write_json(score_path, report)
    print(score_path)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
