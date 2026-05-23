#!/usr/bin/env python3
"""Search D+ pre-action signal families after strict no-order gating.

This local-only search consumes the candidate_seed_outcome_separator_export_v1
CSV and already-pulled no-order summaries. Post-action pair/residual labels are
used only for offline train ranking; selected predicates must be frozen before
holdout evaluation and must contain a genuinely new pre-action ledger signal.

It does not read events JSONL, raw/replay stores, collector stores, sockets,
SSH, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_new_pre_action_signal_family_search_v1"
DEFAULT_SEPARATOR_CSV = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_NO_ORDER_COMPLETION = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_source_opportunity_reason_source_completion_20260523T104314Z/"
    "manifest.json"
)
DEFAULT_NO_ORDER_SUMMARY_DIR = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_source_opportunity_reason_source_driver_20260523T092200Z/"
    "remote_clean/output"
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
NO_ORDER_SUPPORTED_FIELDS = {"side", "risk", "offset", "open", "deficit"}
NEW_SIGNAL_FIELDS = {"ledger_before", "ledger_after", "ledger_delta"}
STATIC_FIELDS = {"side", "risk", "offset"}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
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


def open_qty(row: dict[str, Any]) -> float:
    if row.get("pre_seed_open_qty") not in (None, ""):
        return as_float(row.get("pre_seed_open_qty"))
    return as_float(row.get("pre_seed_same_qty")) + as_float(row.get("pre_seed_opp_qty"))


def open_bucket(row: dict[str, Any]) -> str:
    value = open_qty(row)
    if value <= 1.0 + DUST:
        return "open_le_1"
    if value <= 2.5 + DUST:
        return "open_le_2_5"
    if value <= 5.0 + DUST:
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
    if value <= 2.0 + DUST:
        return "deficit_1_25_2"
    return "deficit_gt_2"


def ledger_bucket(value: float, name: str) -> str:
    if value < -2.0:
        return f"{name}_lt_m2"
    if value < -1.0:
        return f"{name}_m2_m1"
    if value < -0.25:
        return f"{name}_m1_m025"
    if value < 0.0:
        return f"{name}_m025_0"
    if value < 0.25:
        return f"{name}_0_025"
    if value < 1.0:
        return f"{name}_025_1"
    return f"{name}_gte_1"


def row_features(row: dict[str, Any]) -> dict[str, str]:
    before = as_float(row.get("ledger_proxy_before"))
    after = as_float(row.get("ledger_proxy_after"))
    return {
        "side": str(row.get("side") or ""),
        "risk": str(row.get("source_risk_direction") or ""),
        "offset": offset_bucket(row),
        "open": open_bucket(row),
        "deficit": deficit_bucket(row),
        "ledger_before": ledger_bucket(before, "before"),
        "ledger_after": ledger_bucket(after, "after"),
        "ledger_delta": ledger_bucket(after - before, "delta"),
    }


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"{path} has no rows")
    for row in rows:
        row["_features"] = row_features(row)  # type: ignore[index]
    return rows


def split_days(rows: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    days = sorted({str(row.get("day") or "") for row in rows if row.get("day")})
    if len(days) < 2:
        raise ValueError("need at least two days for train/holdout")
    return set(days[::2]), set(days[1::2])


def totals(rows: list[dict[str, Any]], indices: set[int]) -> dict[str, float]:
    out = {
        "rows": float(len(indices)),
        "seed_qty": 0.0,
        "seed_cost": 0.0,
        "pair_qty": 0.0,
        "pair_cost": 0.0,
        "pair_pnl": 0.0,
        "official_fee": 0.0,
        "residual_qty": 0.0,
        "residual_cost": 0.0,
    }
    for idx in indices:
        row = rows[idx]
        out["seed_qty"] += as_float(row.get("seed_qty"))
        out["seed_cost"] += as_float(row.get("seed_cost"))
        out["pair_qty"] += as_float(row.get("source_pair_qty"))
        out["pair_cost"] += as_float(row.get("source_pair_cost"))
        out["pair_pnl"] += as_float(row.get("source_pair_pnl"))
        out["official_fee"] += as_float(row.get("official_fee"))
        out["residual_qty"] += as_float(row.get("source_residual_qty"))
        out["residual_cost"] += as_float(row.get("source_residual_cost"))
    return out


def derived(base: dict[str, float]) -> dict[str, Any]:
    fee_after = base["pair_pnl"] - base["official_fee"] - base["residual_cost"]
    stress100 = fee_after - 0.01 * (2.0 * base["pair_qty"] + base["residual_qty"])
    return {
        **{key: round(value, 8) for key, value in base.items()},
        "weighted_pair_cost": round(safe_ratio(base["pair_cost"], base["pair_qty"]), 8),
        "residual_qty_rate": round(safe_ratio(base["residual_qty"], base["seed_qty"]), 8),
        "residual_cost_rate": round(safe_ratio(base["residual_cost"], base["seed_cost"]), 8),
        "fee_after_pnl_proxy": round(fee_after, 8),
        "stress100_worst_pnl_proxy": round(stress100, 8),
    }


def compare(rows: list[dict[str, Any]], control_indices: set[int], matched_indices: set[int]) -> dict[str, Any]:
    variant_indices = control_indices - matched_indices
    control = totals(rows, control_indices)
    variant = totals(rows, variant_indices)
    matched = totals(rows, matched_indices)
    c = derived(control)
    v = derived(variant)
    m = derived(matched)
    return {
        "matched": m,
        "control": c,
        "variant": v,
        "row_retention": round(safe_ratio(variant["rows"], control["rows"]), 8),
        "seed_qty_retention": round(safe_ratio(variant["seed_qty"], control["seed_qty"]), 8),
        "pair_qty_retention": round(safe_ratio(variant["pair_qty"], control["pair_qty"]), 8),
        "residual_qty_rate_reduction": round(
            1.0
            - safe_ratio(
                safe_ratio(variant["residual_qty"], variant["seed_qty"]),
                safe_ratio(control["residual_qty"], control["seed_qty"]),
            ),
            8,
        ),
        "residual_cost_rate_reduction": round(
            1.0
            - safe_ratio(
                safe_ratio(variant["residual_cost"], variant["seed_cost"]),
                safe_ratio(control["residual_cost"], control["seed_cost"]),
            ),
            8,
        ),
        "weighted_pair_cost_worse_ratio": round(
            safe_ratio(
                safe_ratio(variant["pair_cost"], variant["pair_qty"]),
                safe_ratio(control["pair_cost"], control["pair_qty"]),
            ),
            8,
        ),
        "fee_after_pnl_proxy_delta": round(v["fee_after_pnl_proxy"] - c["fee_after_pnl_proxy"], 8),
        "stress100_worst_pnl_proxy_delta": round(v["stress100_worst_pnl_proxy"] - c["stress100_worst_pnl_proxy"], 8),
    }


def passes_gate(comparison: dict[str, Any]) -> bool:
    return (
        comparison["matched"]["rows"] >= 20.0
        and comparison["seed_qty_retention"] >= 0.90
        and comparison["pair_qty_retention"] >= 0.90
        and comparison["residual_qty_rate_reduction"] >= 0.20
        and comparison["residual_cost_rate_reduction"] >= 0.20
        and comparison["weighted_pair_cost_worse_ratio"] <= 1.01
        and comparison["fee_after_pnl_proxy_delta"] >= 0.0
        and comparison["stress100_worst_pnl_proxy_delta"] >= 0.0
    )


def build_index(rows: list[dict[str, Any]], indices: set[int]) -> dict[tuple[str, str], set[int]]:
    out: dict[tuple[str, str], set[int]] = defaultdict(set)
    for idx in indices:
        features = rows[idx]["_features"]  # type: ignore[index]
        for key, value in features.items():
            out[(key, value)].add(idx)
    return out


def combo_name(combo: tuple[tuple[str, str], ...]) -> str:
    return "|".join(f"{key}={value}" for key, value in combo)


def is_frozen_micro_deficit_variant(combo: tuple[tuple[str, str], ...]) -> bool:
    values = dict(combo)
    return (
        values.get("risk") == "repair_or_pairing_improving"
        and values.get("offset") == "offset_90_120"
        and values.get("open") == "open_le_1"
        and values.get("deficit") == "deficit_0_0_25"
    )


def is_valid_new_family(combo: tuple[tuple[str, str], ...]) -> bool:
    fields = {key for key, _value in combo}
    if not fields & NEW_SIGNAL_FIELDS:
        return False
    if fields <= STATIC_FIELDS:
        return False
    if is_frozen_micro_deficit_variant(combo):
        return False
    return True


def enumerate_combos(
    index: dict[tuple[str, str], set[int]],
    min_train_match: int,
    *,
    max_optional_terms: int,
) -> list[tuple[tuple[str, str], ...]]:
    terms = sorted(index)
    combos: list[tuple[tuple[str, str], ...]] = []
    ledger_terms = [term for term in terms if term[0] in NEW_SIGNAL_FIELDS]
    optional_terms = [term for term in terms if term[0] in {"side", "risk", "offset", "open", "deficit"}]
    for ledger_term in ledger_terms:
        for width in range(0, max_optional_terms + 1):
            for optional in itertools.combinations(optional_terms, width):
                combo = tuple(sorted((ledger_term, *optional)))
                fields = [key for key, _value in combo]
                if len(fields) != len(set(fields)):
                    continue
                if not is_valid_new_family(combo):
                    continue
                matched = set.intersection(*(index[term] for term in combo))
                if len(matched) >= min_train_match:
                    combos.append(combo)
    return sorted(set(combos), key=combo_name)


def score_combo(
    rows: list[dict[str, Any]],
    combo: tuple[tuple[str, str], ...],
    train_index: dict[tuple[str, str], set[int]],
    holdout_index: dict[tuple[str, str], set[int]],
    train_indices: set[int],
    holdout_indices: set[int],
    full_index: dict[tuple[str, str], set[int]],
    full_indices: set[int],
) -> dict[str, Any]:
    train_matched = set.intersection(*(train_index[term] for term in combo))
    holdout_matched = set.intersection(*(holdout_index.get(term, set()) for term in combo)) if combo else set()
    full_matched = set.intersection(*(full_index.get(term, set()) for term in combo)) if combo else set()
    train = compare(rows, train_indices, train_matched)
    holdout = compare(rows, holdout_indices, holdout_matched)
    full = compare(rows, full_indices, full_matched)
    score = (
        2.0 * train["residual_cost_rate_reduction"]
        + train["residual_qty_rate_reduction"]
        - max(0.0, 0.95 - train["seed_qty_retention"])
        - max(0.0, 0.95 - train["pair_qty_retention"])
    )
    return {
        "predicate_name": combo_name(combo),
        "predicate_terms": [{"field": key, "value": value} for key, value in combo],
        "train": train,
        "holdout": holdout,
        "full": full,
        "train_gate_passed": passes_gate(train),
        "holdout_gate_passed": passes_gate(holdout),
        "score": round(score, 8),
    }


def train_score_combo(
    rows: list[dict[str, Any]],
    combo: tuple[tuple[str, str], ...],
    train_index: dict[tuple[str, str], set[int]],
    train_indices: set[int],
) -> dict[str, Any]:
    train_matched = set.intersection(*(train_index[term] for term in combo))
    train = compare(rows, train_indices, train_matched)
    score = (
        2.0 * train["residual_cost_rate_reduction"]
        + train["residual_qty_rate_reduction"]
        - max(0.0, 0.95 - train["seed_qty_retention"])
        - max(0.0, 0.95 - train["pair_qty_retention"])
    )
    return {
        "combo": combo,
        "predicate_name": combo_name(combo),
        "predicate_terms": [{"field": key, "value": value} for key, value in combo],
        "train": train,
        "train_gate_passed": passes_gate(train),
        "score": round(score, 8),
    }


def no_order_risk_budget(completion: dict[str, Any]) -> dict[str, Any]:
    aggregate = completion.get("aggregate_metrics") if isinstance(completion.get("aggregate_metrics"), dict) else {}
    normalized = (
        (completion.get("scorer_results") or {})
        .get("shadow_acceptance", {})
        .get("promotion_gate", {})
        .get("normalized_risk_budget", {})
    )
    candidates = aggregate.get("candidates")
    net_pair_cost_p90 = aggregate.get("net_pair_cost_proxy_p90") or aggregate.get("net_pair_cost_p90")
    residual_qty_share = aggregate.get("residual_qty_share_of_filled")
    residual_cost_share = aggregate.get("residual_cost_share_of_filled_cost")
    pair_tail_loss_share = aggregate.get("pair_tail_loss_share_of_pair_pnl") or normalized.get(
        "pair_tail_loss_share_of_pair_pnl"
    )
    passed = (
        as_float(candidates) >= 100.0
        and as_float(net_pair_cost_p90) <= 1.0
        and as_float(residual_qty_share) <= 0.15
        and as_float(residual_cost_share) <= 0.20
        and as_float(pair_tail_loss_share) <= 0.05
    )
    return {
        "candidates": candidates,
        "net_pair_cost_p90": net_pair_cost_p90,
        "residual_qty_share": residual_qty_share,
        "residual_cost_share": residual_cost_share,
        "pair_tail_loss_share": pair_tail_loss_share,
        "passed": passed,
    }


def flatten_counter(obj: Any, prefix: tuple[str, ...] = ()) -> Counter[str]:
    out: Counter[str] = Counter()
    if isinstance(obj, dict):
        for key, value in obj.items():
            out.update(flatten_counter(value, (*prefix, str(key))))
    elif isinstance(obj, (int, float)):
        out["|".join(prefix)] += float(obj)
    return out


def aggregate_no_order_marker_counts(summary_paths: list[Path]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for path in summary_paths:
        data = read_json(path)
        marker = ((data.get("event_lite") or {}).get("source_opportunity_marker_summary") or {})
        counts.update(flatten_counter(marker.get("transition_count_by_status_side_offset_risk_open_deficit") or {}))
    return counts


def no_order_marker_for_combo(combo: tuple[tuple[str, str], ...], marker_counts: Counter[str]) -> dict[str, Any]:
    terms = dict(combo)
    unsupported = sorted({key for key in terms if key not in NO_ORDER_SUPPORTED_FIELDS})
    if unsupported:
        return {
            "supported_by_current_no_order_summary": False,
            "unsupported_fields": unsupported,
            "marker_count": None,
            "admitted_count": None,
            "blocked_count": None,
            "marker_reproduced": False,
        }

    def key_matches(key: str) -> bool:
        parts = key.split("|")
        if len(parts) != 6:
            return False
        status, side, offset, risk, open_value, deficit_value = parts
        mapping = {
            "side": side,
            "offset": offset,
            "risk": risk,
            "open": open_value.replace("open_qty_", "open_"),
            "deficit": deficit_value,
        }
        return all(mapping.get(field) == value for field, value in terms.items())

    matched = {key: value for key, value in marker_counts.items() if key_matches(key)}
    admitted = sum(value for key, value in matched.items() if key.startswith("admitted|"))
    blocked = sum(value for key, value in matched.items() if key.startswith("blocked|"))
    total = admitted + blocked
    return {
        "supported_by_current_no_order_summary": True,
        "unsupported_fields": [],
        "marker_count": round(total, 8),
        "admitted_count": round(admitted, 8),
        "blocked_count": round(blocked, 8),
        "marker_reproduced": total > 0.0,
        "matched_marker_keys": dict(sorted(matched.items())[:50]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--separator-csv", type=Path, default=DEFAULT_SEPARATOR_CSV)
    parser.add_argument("--no-order-completion-manifest", type=Path, default=DEFAULT_NO_ORDER_COMPLETION)
    parser.add_argument("--no-order-summary-dir", type=Path, default=DEFAULT_NO_ORDER_SUMMARY_DIR)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-train-match", type=int, default=20)
    parser.add_argument("--max-optional-terms", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    separator_csv = args.separator_csv if args.separator_csv.is_absolute() else root / args.separator_csv
    completion_path = (
        args.no_order_completion_manifest
        if args.no_order_completion_manifest.is_absolute()
        else root / args.no_order_completion_manifest
    )
    summary_dir = args.no_order_summary_dir if args.no_order_summary_dir.is_absolute() else root / args.no_order_summary_dir
    output = args.output if args.output.is_absolute() else root / args.output
    summary_paths = sorted(summary_dir.glob("*.summary.json")) if summary_dir.exists() else []
    required = [separator_csv, completion_path, *summary_paths]
    unsafe = [str(path) for path in [*required, output] if not path_safe(path)]
    missing = [str(path) for path in [separator_csv, completion_path] if not path.exists()]
    if unsafe or missing:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": utc_now(),
            "decision": "BLOCKED",
            "decision_label": "BLOCKED_NEW_SIGNAL_FAMILY_SEARCH_INPUT_UNAVAILABLE",
            "unsafe_paths": unsafe,
            "missing": missing,
            "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
        }
        write_json(output, manifest)
        print(json.dumps({"decision": manifest["decision"], "output": str(output)}, sort_keys=True))
        return 2

    rows = load_rows(separator_csv)
    train_days, holdout_days = split_days(rows)
    train_indices = {idx for idx, row in enumerate(rows) if row.get("day") in train_days}
    holdout_indices = {idx for idx, row in enumerate(rows) if row.get("day") in holdout_days}
    full_indices = set(range(len(rows)))
    train_index = build_index(rows, train_indices)
    holdout_index = build_index(rows, holdout_indices)
    full_index = build_index(rows, full_indices)

    combos = enumerate_combos(train_index, args.min_train_match, max_optional_terms=args.max_optional_terms)
    train_scored = [train_score_combo(rows, combo, train_index, train_indices) for combo in combos]
    train_viable = [item for item in train_scored if item["train_gate_passed"]]
    ranked_train = sorted(
        train_viable or train_scored,
        key=lambda item: (-float(item["score"]), -float(item["train"]["matched"]["residual_cost"])),
    )
    holdout_scored = [
        score_combo(
            rows,
            tuple((term["field"], term["value"]) for term in item["predicate_terms"]),
            train_index,
            holdout_index,
            train_indices,
            holdout_indices,
            full_index,
            full_indices,
        )
        for item in ranked_train[: max(args.top_n * 5, 50)]
    ]
    viable = [item for item in holdout_scored if item["train_gate_passed"] and item["holdout_gate_passed"]]
    ranked = sorted(
        viable or holdout_scored,
        key=lambda item: (-float(item["score"]), -float(item["train"]["matched"]["residual_cost"])),
    )
    selected = ranked[0] if ranked else None
    completion = read_json(completion_path)
    risk_budget = no_order_risk_budget(completion)
    marker_counts = aggregate_no_order_marker_counts(summary_paths)
    selected_no_order = (
        no_order_marker_for_combo(tuple((term["field"], term["value"]) for term in selected["predicate_terms"]), marker_counts)
        if selected
        else {}
    )
    selected_gate_passed = bool(
        selected
        and selected["holdout_gate_passed"]
        and selected_no_order.get("marker_reproduced")
        and risk_budget.get("passed")
    )
    if selected is None:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_NEW_SIGNAL_FAMILY_NO_CANDIDATE_GENERATED"
    elif not selected["holdout_gate_passed"]:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_NEW_SIGNAL_FAMILY_NO_HOLDOUT_STABLE_PRE_ACTION_CANDIDATE"
    elif not selected_no_order.get("supported_by_current_no_order_summary"):
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_NEW_SIGNAL_FAMILY_HOLDOUT_STABLE_NO_ORDER_MARKER_FIELDS_MISSING"
    elif not selected_gate_passed:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_NEW_SIGNAL_FAMILY_HOLDOUT_STABLE_NO_ORDER_REPRODUCTION_FAILED"
    else:
        decision = "KEEP"
        decision_label = "KEEP_NEW_SIGNAL_FAMILY_STRICT_GATE_READY"

    missing_marker_fields = selected_no_order.get("unsupported_fields") or []
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": "new_pre_action_signal_family_search_v1",
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
            "no_order_completion_manifest": str(completion_path),
            "no_order_summary_dir": str(summary_dir),
            "no_order_summary_count": len(summary_paths),
            "row_count": len(rows),
            "train_days": sorted(train_days),
            "holdout_days": sorted(holdout_days),
            "unsafe_paths": unsafe,
        },
        "guardrails": {
            "requires_new_pre_action_field": sorted(NEW_SIGNAL_FIELDS),
            "excluded_families": [
                "frozen_micro_deficit_offset_90_120_open_le_1_deficit_0_0_25",
                "pure_side_offset_static_deletion",
                "price_or_public_l1_caps",
                "cooldown_or_admission_caps",
                "fill_to_balance_or_portfolio_ledger_revival",
                "source_pair_or_source_residual_live_criteria",
            ],
            "offline_labels_used_only_for_train_ranking": [
                "source_pair_qty",
                "source_pair_cost",
                "source_pair_pnl",
                "source_residual_qty",
                "source_residual_cost",
            ],
        },
        "search_summary": {
            "generated_candidate_count": len(train_scored),
            "train_viable_count": len(train_viable),
            "holdout_evaluated_count": len(holdout_scored),
            "train_holdout_viable_count": len(viable),
            "selected": selected,
            "top_candidates": ranked[: args.top_n],
        },
        "no_order_reproduction": {
            "risk_budget": risk_budget,
            "selected_marker": selected_no_order,
            "strict_gate_passed": selected_gate_passed,
            "current_summary_supported_fields": sorted(NO_ORDER_SUPPORTED_FIELDS),
            "missing_marker_fields_for_selected": missing_marker_fields,
        },
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "interpretation": (
                "This search deliberately requires a ledger-style pre-action signal to avoid repeating the frozen "
                "micro-deficit family. A holdout-stable result is still UNKNOWN unless current no-order summaries can "
                "reproduce the selected marker and the no-order risk budget passes."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_SIGNAL_SEARCH_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            "UNKNOWN: current no-order summaries do not expose the selected ledger marker fields; either implement a "
            "default-off ledger-marker denominator summary for the exact selected predicate or abandon this family if "
            "it overlaps a previously discarded dynamic ledger gate. Do not start a shadow, sweep thresholds, or claim "
            "private/deployable/promotion evidence from this local result."
            if missing_marker_fields
            else "If strict gate passed, prepare a bounded diagnostic question; otherwise search a different pre-action family."
        ),
    }
    write_json(output, manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
