#!/usr/bin/env python3
"""Train/holdout audit for D+ candidate-seed separator predicates.

This local-only audit consumes the candidate_seed_outcome_separator_export_v1
CSV. Post-action labels are used only for offline discovery on train days; the
selected predicate is frozen and expressed only in pre-action fields before it
is evaluated on holdout days. It does not read raw/replay stores, events JSONL,
sockets, SSH, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ARTIFACT = "xuan_b27_dplus_candidate_separator_train_holdout_audit_v1"
DEFAULT_SEPARATOR_CSV = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_NO_ORDER_COMPLETION = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_micro_deficit_completion_20260523T022553Z/"
    "manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/replay",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)
DUST = 1e-12


@dataclass(frozen=True)
class Predicate:
    name: str
    side: str
    risk_direction: str
    offset_bucket: str
    open_bucket: str
    deficit_bucket: str


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    with path.open() as fh:
        value = json.load(fh)
    return value if isinstance(value, dict) else {}


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= DUST:
        return 0.0
    return numerator / denominator


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"{path} has no rows")
    return rows


def offset_bucket(row: dict[str, Any]) -> str:
    offset = as_float(row.get("offset_s"), default=-1.0)
    if offset < 0.0:
        return "offset_unknown"
    if offset < 30.0:
        return "offset_0_30"
    if offset < 60.0:
        return "offset_30_60"
    if offset < 90.0:
        return "offset_60_90"
    if offset < 120.0:
        return "offset_90_120"
    return "offset_gte_120"


def pre_seed_open_qty(row: dict[str, Any]) -> float:
    if row.get("pre_seed_open_qty") not in (None, ""):
        return as_float(row.get("pre_seed_open_qty"))
    return as_float(row.get("pre_seed_same_qty")) + as_float(row.get("pre_seed_opp_qty"))


def open_bucket(row: dict[str, Any]) -> str:
    open_qty = pre_seed_open_qty(row)
    if open_qty <= 1.0 + DUST:
        return "open_le_1"
    if open_qty <= 2.5 + DUST:
        return "open_le_2_5"
    if open_qty <= 5.0 + DUST:
        return "open_le_5"
    return "open_gt_5"


def deficit(row: dict[str, Any]) -> float:
    return as_float(row.get("pre_seed_opp_qty")) - as_float(row.get("pre_seed_same_qty"))


def deficit_bucket(row: dict[str, Any]) -> str:
    value = deficit(row)
    if value <= 0.0 + DUST:
        return "deficit_le_0"
    if value <= 0.25 + DUST:
        return "deficit_0_0_25"
    if value <= 1.25 + DUST:
        return "deficit_0_25_1_25"
    return "deficit_gt_1_25"


def predicate_name(
    side: str,
    risk_direction: str,
    offset: str,
    open_qty: str,
    deficit_qty: str,
) -> str:
    return "|".join([side, offset, risk_direction, open_qty, deficit_qty])


def generate_predicates() -> list[Predicate]:
    predicates: list[Predicate] = []
    for side in ("ANY", "YES", "NO"):
        for risk_direction in ("ANY", "risk_increasing", "repair_or_pairing_improving"):
            if risk_direction == "ANY":
                continue
            for offset in ("ANY", "offset_0_30", "offset_30_60", "offset_60_90", "offset_90_120"):
                for open_qty in ("ANY", "open_le_1", "open_le_2_5", "open_le_5", "open_gt_5"):
                    for deficit_qty in (
                        "ANY",
                        "deficit_le_0",
                        "deficit_0_0_25",
                        "deficit_0_25_1_25",
                        "deficit_gt_1_25",
                    ):
                        if open_qty == "ANY" or deficit_qty == "ANY":
                            continue
                        predicates.append(
                            Predicate(
                                name=predicate_name(side, risk_direction, offset, open_qty, deficit_qty),
                                side=side,
                                risk_direction=risk_direction,
                                offset_bucket=offset,
                                open_bucket=open_qty,
                                deficit_bucket=deficit_qty,
                            )
                        )
    return predicates


def predicate_fn(predicate: Predicate) -> Callable[[dict[str, Any]], bool]:
    def matches(row: dict[str, Any]) -> bool:
        if predicate.side != "ANY" and str(row.get("side") or "") != predicate.side:
            return False
        if predicate.risk_direction != "ANY" and str(row.get("source_risk_direction") or "") != predicate.risk_direction:
            return False
        if predicate.offset_bucket != "ANY" and offset_bucket(row) != predicate.offset_bucket:
            return False
        if predicate.open_bucket != "ANY" and open_bucket(row) != predicate.open_bucket:
            return False
        if predicate.deficit_bucket != "ANY" and deficit_bucket(row) != predicate.deficit_bucket:
            return False
        return True

    return matches


def base_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    totals = {
        "rows": float(len(rows)),
        "seed_qty": 0.0,
        "seed_cost": 0.0,
        "pair_qty": 0.0,
        "pair_cost": 0.0,
        "pair_pnl": 0.0,
        "official_fee": 0.0,
        "residual_qty": 0.0,
        "residual_cost": 0.0,
    }
    for row in rows:
        totals["seed_qty"] += as_float(row.get("seed_qty"))
        totals["seed_cost"] += as_float(row.get("seed_cost"))
        totals["pair_qty"] += as_float(row.get("source_pair_qty"))
        totals["pair_cost"] += as_float(row.get("source_pair_cost"))
        totals["pair_pnl"] += as_float(row.get("source_pair_pnl"))
        totals["official_fee"] += as_float(row.get("official_fee"))
        totals["residual_qty"] += as_float(row.get("source_residual_qty"))
        totals["residual_cost"] += as_float(row.get("source_residual_cost"))
    return totals


def derived(totals: dict[str, float], rows: list[dict[str, Any]]) -> dict[str, Any]:
    fee_after = totals["pair_pnl"] - totals["official_fee"] - totals["residual_cost"]
    stress100 = fee_after - 0.01 * (2.0 * totals["pair_qty"] + totals["residual_qty"])
    day_fee_after: dict[str, float] = {}
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[str(row.get("day") or "")].append(row)
    for day, day_rows in by_day.items():
        dt = base_totals(day_rows)
        day_fee_after[day] = dt["pair_pnl"] - dt["official_fee"] - dt["residual_cost"]
    return {
        **{key: round(value, 8) for key, value in totals.items()},
        "weighted_pair_cost": round(safe_ratio(totals["pair_cost"], totals["pair_qty"]), 8),
        "residual_qty_rate": round(safe_ratio(totals["residual_qty"], totals["seed_qty"]), 8),
        "residual_cost_rate": round(safe_ratio(totals["residual_cost"], totals["seed_cost"]), 8),
        "fee_after_pnl_proxy": round(fee_after, 8),
        "stress100_worst_pnl_proxy": round(stress100, 8),
        "worst_day_fee_after_pnl_proxy": round(min(day_fee_after.values()) if day_fee_after else 0.0, 8),
        "day_count": len(by_day),
    }


def compare(control_rows: list[dict[str, Any]], variant_rows: list[dict[str, Any]]) -> dict[str, Any]:
    control_totals = base_totals(control_rows)
    variant_totals = base_totals(variant_rows)
    control = derived(control_totals, control_rows)
    variant = derived(variant_totals, variant_rows)
    return {
        "control": control,
        "variant": variant,
        "row_retention": safe_ratio(variant_totals["rows"], control_totals["rows"]),
        "seed_qty_retention": safe_ratio(variant_totals["seed_qty"], control_totals["seed_qty"]),
        "pair_qty_retention": safe_ratio(variant_totals["pair_qty"], control_totals["pair_qty"]),
        "residual_qty_rate_reduction": 1.0
        - safe_ratio(safe_ratio(variant_totals["residual_qty"], variant_totals["seed_qty"]), safe_ratio(control_totals["residual_qty"], control_totals["seed_qty"])),
        "residual_cost_rate_reduction": 1.0
        - safe_ratio(safe_ratio(variant_totals["residual_cost"], variant_totals["seed_cost"]), safe_ratio(control_totals["residual_cost"], control_totals["seed_cost"])),
        "weighted_pair_cost_worse_ratio": safe_ratio(
            safe_ratio(variant_totals["pair_cost"], variant_totals["pair_qty"]),
            safe_ratio(control_totals["pair_cost"], control_totals["pair_qty"]),
        ),
        "fee_after_pnl_proxy_delta": variant["fee_after_pnl_proxy"] - control["fee_after_pnl_proxy"],
        "stress100_worst_pnl_proxy_delta": variant["stress100_worst_pnl_proxy"] - control["stress100_worst_pnl_proxy"],
        "worst_day_fee_after_pnl_proxy_delta": variant["worst_day_fee_after_pnl_proxy"] - control["worst_day_fee_after_pnl_proxy"],
    }


def score_candidate(rows: list[dict[str, Any]], predicate: Predicate) -> dict[str, Any]:
    matches = predicate_fn(predicate)
    matched_rows = [row for row in rows if matches(row)]
    variant_rows = [row for row in rows if not matches(row)]
    comparison = compare(rows, variant_rows)
    match_totals = derived(base_totals(matched_rows), matched_rows)
    score = (
        2.0 * comparison["residual_cost_rate_reduction"]
        + comparison["residual_qty_rate_reduction"]
        - max(0.0, 0.95 - comparison["seed_qty_retention"])
        - max(0.0, 0.95 - comparison["pair_qty_retention"])
    )
    return {
        "predicate": {
            "name": predicate.name,
            "side": predicate.side,
            "source_risk_direction": predicate.risk_direction,
            "offset_bucket": predicate.offset_bucket,
            "open_bucket": predicate.open_bucket,
            "deficit_bucket": predicate.deficit_bucket,
        },
        "match": match_totals,
        "comparison": {key: round(value, 8) if isinstance(value, float) else value for key, value in comparison.items() if key not in {"control", "variant"}},
        "score": round(score, 8),
    }


def passes_gate(result: dict[str, Any], *, min_seed_qty_retention: float, min_pair_qty_retention: float) -> bool:
    comparison = result["comparison"]
    return (
        comparison["seed_qty_retention"] >= min_seed_qty_retention
        and comparison["pair_qty_retention"] >= min_pair_qty_retention
        and comparison["residual_qty_rate_reduction"] >= 0.20
        and comparison["residual_cost_rate_reduction"] >= 0.20
        and comparison["weighted_pair_cost_worse_ratio"] <= 1.01
        and result["match"]["rows"] >= 20.0
    )


def split_days(days: list[str], mode: str) -> tuple[set[str], set[str]]:
    if len(days) < 2:
        raise ValueError("need at least two days for train/holdout")
    if mode == "alternating":
        return set(days[::2]), set(days[1::2])
    split = max(1, int(math.ceil(len(days) * 0.6)))
    if split >= len(days):
        split = len(days) - 1
    return set(days[:split]), set(days[split:])


def no_order_reproduction(no_order_manifest: dict[str, Any]) -> dict[str, Any]:
    aggregate = no_order_manifest.get("aggregate_metrics") if isinstance(no_order_manifest.get("aggregate_metrics"), dict) else {}
    micro = no_order_manifest.get("scorer_results", {}).get("micro_deficit_summary", {})
    micro_metrics = micro.get("metrics") if isinstance(micro, dict) and isinstance(micro.get("metrics"), dict) else {}
    marker = no_order_manifest.get("scorer_results", {}).get("source_opportunity_marker_summary", {})
    marker = marker if isinstance(marker, dict) else {}
    normalized_risk_budget = (
        no_order_manifest.get("scorer_results", {})
        .get("shadow_acceptance", {})
        .get("promotion_gate", {})
        .get("normalized_risk_budget", {})
    )
    candidates = aggregate.get("candidates")
    net_pair_cost_p90 = aggregate.get("net_pair_cost_proxy_p90") or aggregate.get("net_pair_cost_p90")
    residual_qty_share = aggregate.get("residual_qty_share_of_filled")
    residual_cost_share = aggregate.get("residual_cost_share_of_filled_cost")
    pair_tail_loss_share = aggregate.get("pair_tail_loss_share_of_pair_pnl") or normalized_risk_budget.get(
        "pair_tail_loss_share_of_pair_pnl"
    )
    micro_deficit_exemplar_count = micro_metrics.get("micro_deficit_exemplar_count")
    if micro_deficit_exemplar_count is None and isinstance(micro, dict):
        micro_deficit_exemplar_count = micro.get("micro_deficit_exemplar_count")
    marker_total = marker.get("marker_total")
    admitted_marker_count = marker.get("admitted_marker_count")
    blocked_marker_count = marker.get("blocked_marker_count")
    strict_micro_deficit_marker_total = marker.get("strict_micro_deficit_marker_total")
    exact_reason_marker_total = marker.get("reason_source_exact_reason_marker_total")
    exact_reason_micro_deficit_marker_total = marker.get("reason_source_exact_reason_micro_deficit_marker_total")
    marker_reproduced = (
        as_float(aggregate.get("micro_deficit_repair_guard_candidates")) > 0.0
        or as_float(micro_deficit_exemplar_count) > 0.0
        or as_float(marker_total) > 0.0
        or as_float(exact_reason_marker_total) > 0.0
        or as_float(strict_micro_deficit_marker_total) > 0.0
        or as_float(exact_reason_micro_deficit_marker_total) > 0.0
    )
    risk_budget_passed = (
        as_float(candidates) >= 100.0
        and as_float(net_pair_cost_p90) <= 1.0
        and as_float(residual_qty_share) <= 0.15
        and as_float(residual_cost_share) <= 0.20
        and as_float(pair_tail_loss_share) <= 0.05
    )
    return {
        "available": bool(no_order_manifest),
        "decision_label": no_order_manifest.get("decision_label"),
        "candidates": candidates,
        "net_pair_cost_p90": net_pair_cost_p90,
        "residual_qty_share": residual_qty_share,
        "residual_cost_share": residual_cost_share,
        "pair_tail_loss_share": pair_tail_loss_share,
        "micro_deficit_repair_guard_candidates": aggregate.get("micro_deficit_repair_guard_candidates"),
        "strict_micro_deficit_exemplar_count": micro_deficit_exemplar_count,
        "source_opportunity_marker_total": marker_total,
        "source_opportunity_admitted_marker_count": admitted_marker_count,
        "source_opportunity_blocked_marker_count": blocked_marker_count,
        "strict_micro_deficit_marker_total": strict_micro_deficit_marker_total,
        "reason_source_exact_reason_marker_total": exact_reason_marker_total,
        "reason_source_exact_reason_micro_deficit_marker_total": exact_reason_micro_deficit_marker_total,
        "marker_reproduced": marker_reproduced,
        "risk_budget_passed": risk_budget_passed,
        "gate_passed": marker_reproduced and risk_budget_passed,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--separator-csv", type=Path, default=DEFAULT_SEPARATOR_CSV)
    parser.add_argument("--no-order-completion-manifest", type=Path, default=DEFAULT_NO_ORDER_COMPLETION)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--split-mode", choices=("alternating", "first60"), default="alternating")
    parser.add_argument("--min-seed-qty-retention", type=float, default=0.90)
    parser.add_argument("--min-pair-qty-retention", type=float, default=0.90)
    parser.add_argument(
        "--require-no-order-reproduction",
        action="store_true",
        help=(
            "Downgrade offline-stable predicates to UNKNOWN unless the no-order completion manifest reproduces "
            "the marker and passes the normalized risk budget."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    separator_csv = args.separator_csv if args.separator_csv.is_absolute() else root / args.separator_csv
    no_order_manifest_path = (
        args.no_order_completion_manifest
        if args.no_order_completion_manifest.is_absolute()
        else root / args.no_order_completion_manifest
    )
    output = args.output if args.output.is_absolute() else root / args.output

    paths = [separator_csv, output]
    if no_order_manifest_path.exists():
        paths.append(no_order_manifest_path)
    unsafe = [str(path) for path in paths if not path_safe(path)]
    if unsafe or not separator_csv.exists():
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": utc_now(),
            "decision": "BLOCKED",
            "decision_label": "BLOCKED_SEPARATOR_TRAIN_HOLDOUT_INPUT_UNAVAILABLE",
            "unsafe_paths": unsafe,
            "missing": [] if separator_csv.exists() else [str(separator_csv)],
            "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
        }
        write_json(output, manifest)
        print(json.dumps({"decision": manifest["decision"], "output": str(output)}, sort_keys=True))
        return 2

    rows = load_rows(separator_csv)
    days = sorted({str(row.get("day") or "") for row in rows if row.get("day")})
    train_days, holdout_days = split_days(days, args.split_mode)
    train_rows = [row for row in rows if row.get("day") in train_days]
    holdout_rows = [row for row in rows if row.get("day") in holdout_days]

    candidates = [score_candidate(train_rows, predicate) for predicate in generate_predicates()]
    viable = [
        candidate
        for candidate in candidates
        if passes_gate(
            candidate,
            min_seed_qty_retention=args.min_seed_qty_retention,
            min_pair_qty_retention=args.min_pair_qty_retention,
        )
    ]
    ranked = sorted(candidates, key=lambda item: (-float(item["score"]), -float(item["match"]["residual_cost"])))
    if viable:
        selected_train = sorted(viable, key=lambda item: (-float(item["score"]), -float(item["match"]["residual_cost"])))[0]
        selected_predicate = Predicate(
            name=selected_train["predicate"]["name"],
            side=selected_train["predicate"]["side"],
            risk_direction=selected_train["predicate"]["source_risk_direction"],
            offset_bucket=selected_train["predicate"]["offset_bucket"],
            open_bucket=selected_train["predicate"]["open_bucket"],
            deficit_bucket=selected_train["predicate"]["deficit_bucket"],
        )
    else:
        selected_train = ranked[0] if ranked else {}
        pred = selected_train.get("predicate", {})
        selected_predicate = Predicate(
            name=pred.get("name", "NONE"),
            side=pred.get("side", "ANY"),
            risk_direction=pred.get("source_risk_direction", "ANY"),
            offset_bucket=pred.get("offset_bucket", "ANY"),
            open_bucket=pred.get("open_bucket", "ANY"),
            deficit_bucket=pred.get("deficit_bucket", "ANY"),
        )

    selected_holdout = score_candidate(holdout_rows, selected_predicate)
    selected_full = score_candidate(rows, selected_predicate)
    holdout_gate_passed = passes_gate(
        selected_holdout,
        min_seed_qty_retention=args.min_seed_qty_retention,
        min_pair_qty_retention=args.min_pair_qty_retention,
    )
    no_order = no_order_reproduction(read_json(no_order_manifest_path))
    no_order_reproduced = bool(no_order.get("marker_reproduced"))
    no_order_gate_passed = bool(no_order.get("gate_passed"))

    if not viable:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_SEPARATOR_TRAIN_NO_VIABLE_PRE_ACTION_PREDICATE"
    elif holdout_gate_passed and args.require_no_order_reproduction and not no_order_gate_passed:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_SEPARATOR_TRAIN_HOLDOUT_OFFLINE_STABLE_NO_ORDER_REPRODUCTION_FAILED"
    elif holdout_gate_passed and not no_order_reproduced:
        decision = "KEEP"
        decision_label = "KEEP_SEPARATOR_TRAIN_HOLDOUT_OFFLINE_STABLE_NO_ORDER_REPRODUCTION_BLOCKED"
    elif holdout_gate_passed:
        decision = "KEEP"
        decision_label = "KEEP_SEPARATOR_TRAIN_HOLDOUT_OFFLINE_STABLE"
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_SEPARATOR_TRAIN_SIGNAL_HOLDOUT_UNSTABLE"

    manifest = {
        "artifact": ARTIFACT,
        "schema_version": "candidate_separator_train_holdout_audit_v1",
        "created_utc": utc_now(),
        "decision": decision,
        "decision_label": decision_label,
        "scope": {
            "local_only": True,
            "read_events_jsonl": False,
            "read_raw_replay_or_collector": False,
            "used_ssh_or_shadow": False,
            "modified_shared_ingress_or_live": False,
            "sent_orders_cancels_redeems": False,
        },
        "inputs": {
            "separator_csv": str(separator_csv),
            "no_order_completion_manifest": str(no_order_manifest_path) if no_order_manifest_path.exists() else None,
            "row_count": len(rows),
            "day_count": len(days),
            "train_days": sorted(train_days),
            "holdout_days": sorted(holdout_days),
            "unsafe_paths": unsafe,
        },
        "field_policy": {
            "offline_discovery_labels": [
                "source_pair_qty",
                "source_pair_cost",
                "source_pair_pnl",
                "source_residual_qty",
                "source_residual_cost",
                "pair_outcome_bucket",
                "residual_tail_outcome_bucket",
            ],
            "frozen_predicate_fields": [
                "side",
                "source_risk_direction",
                "offset_s bucket",
                "pre_seed_same_qty",
                "pre_seed_opp_qty",
                "pre_seed_open_qty",
            ],
            "policy": (
                "Post-action source_pair/source_residual labels are used only to select on train days. The selected "
                "predicate is frozen before holdout evaluation and contains only pre-action fields."
            ),
            "strict_no_order_reproduction_required": bool(args.require_no_order_reproduction),
        },
        "selection": {
            "viable_train_predicate_count": len(viable),
            "selected_predicate": selected_train.get("predicate"),
            "train": selected_train,
            "holdout": selected_holdout,
            "full": selected_full,
            "holdout_gate_passed": holdout_gate_passed,
            "train_holdout_drift": {
                "residual_qty_rate_reduction_delta": round(
                    selected_holdout["comparison"]["residual_qty_rate_reduction"]
                    - selected_train.get("comparison", {}).get("residual_qty_rate_reduction", 0.0),
                    8,
                ),
                "residual_cost_rate_reduction_delta": round(
                    selected_holdout["comparison"]["residual_cost_rate_reduction"]
                    - selected_train.get("comparison", {}).get("residual_cost_rate_reduction", 0.0),
                    8,
                ),
                "pair_qty_retention_delta": round(
                    selected_holdout["comparison"]["pair_qty_retention"]
                    - selected_train.get("comparison", {}).get("pair_qty_retention", 0.0),
                    8,
                ),
            },
            "top_train_candidates": ranked[:20],
        },
        "no_order_reproduction": no_order,
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "holdout_gate_passed": holdout_gate_passed,
            "no_order_marker_reproduced": no_order_reproduced,
            "no_order_gate_passed": no_order_gate_passed,
            "interpretation": (
                "KEEP here means the train/holdout audit tooling and offline frozen-predicate test are useful. If "
                "--require-no-order-reproduction is set, offline-stable predicates are UNKNOWN unless the no-order "
                "manifest also reproduces the marker and passes normalized risk budgets."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "TRAIN_HOLDOUT_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            "Produce local source/opportunity gap audit for the selected predicate: compare historical separator marker "
            "denominators against no-order marker availability and name missing same-window queue/opportunity fields; do "
            "not start another shadow or sweep thresholds."
        ),
    }
    write_json(output, manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
