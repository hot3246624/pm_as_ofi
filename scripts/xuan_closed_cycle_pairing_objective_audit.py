#!/usr/bin/env python3
"""Audit a fresh closed-cycle pairing objective after D+ discard.

The audit consumes only the allowed candidate_seed_outcome_separator export and
the fresh public profile reset manifest. It uses post-action pair/residual
labels only as offline labels, never as live criteria. The output is a research
objective verdict, not a deployable rule.
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


ARTIFACT = "xuan_closed_cycle_pairing_objective_audit_v1"
DEFAULT_SEPARATOR_CSV = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_PUBLIC_PROFILE_RESET = Path(
    "xuan_research_artifacts/xuan_public_profile_reset_review_20260524T022800Z/manifest.json"
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
CYCLE_FIELDS = {"open", "balance", "preseed_sides", "same_qty", "opp_qty"}
STATIC_FIELDS = {"side", "risk", "offset"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if not value.strip():
            return default
        try:
            return float(value)
        except ValueError:
            return default
    return default


def safe_ratio(num: float, den: float) -> float:
    return num / den if abs(den) > DUST else 0.0


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return round(float(ordered[idx]), 8)


def offset_bucket(value: float) -> str:
    if value < 0:
        return "offset_unknown"
    if value < 30:
        return "offset_0_30"
    if value < 60:
        return "offset_30_60"
    if value < 90:
        return "offset_60_90"
    if value < 120:
        return "offset_90_120"
    return "offset_ge_120"


def open_bucket(value: float) -> str:
    if value <= 1.0 + DUST:
        return "open_le_1"
    if value <= 2.5 + DUST:
        return "open_1_2_5"
    if value <= 5.0 + DUST:
        return "open_2_5"
    return "open_gt_5"


def qty_bucket(value: float, prefix: str) -> str:
    if value <= DUST:
        return f"{prefix}_zero"
    if value <= 5.0 + DUST:
        return f"{prefix}_le_5"
    if value <= 10.0 + DUST:
        return f"{prefix}_5_10"
    if value <= 25.0 + DUST:
        return f"{prefix}_10_25"
    return f"{prefix}_gt_25"


def balance_bucket(same_qty: float, opp_qty: float) -> str:
    value = opp_qty - same_qty
    if value <= DUST:
        return "deficit_le_0"
    if value <= 0.25 + DUST:
        return "deficit_0_0_25"
    if value <= 1.25 + DUST:
        return "deficit_0_25_1_25"
    if value <= 2.0 + DUST:
        return "deficit_1_25_2"
    return "deficit_gt_2"


def row_features(row: dict[str, Any]) -> dict[str, str]:
    same_qty = as_float(row.get("pre_seed_same_qty"))
    opp_qty = as_float(row.get("pre_seed_opp_qty"))
    open_qty = as_float(row.get("pre_seed_open_qty"), same_qty + opp_qty)
    if open_qty <= DUST:
        open_qty = same_qty + opp_qty
    if same_qty > DUST and opp_qty > DUST:
        sides = "both_preseed_sides"
    elif same_qty > DUST:
        sides = "same_only_preseed"
    elif opp_qty > DUST:
        sides = "opp_only_preseed"
    else:
        sides = "empty_preseed"
    return {
        "side": str(row.get("side") or ""),
        "risk": str(row.get("source_risk_direction") or ""),
        "offset": offset_bucket(as_float(row.get("offset_s"), -1.0)),
        "open": open_bucket(open_qty),
        "balance": balance_bucket(same_qty, opp_qty),
        "preseed_sides": sides,
        "same_qty": qty_bucket(same_qty, "same"),
        "opp_qty": qty_bucket(opp_qty, "opp"),
    }


def load_separator(path: Path) -> list[dict[str, Any]]:
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
    closed_cycle = base["pair_pnl"] - base["official_fee"] - base["residual_cost"]
    stress100 = closed_cycle - 0.01 * (2.0 * base["pair_qty"] + base["residual_qty"])
    return {
        **{key: round(value, 8) for key, value in base.items()},
        "weighted_pair_cost": round(safe_ratio(base["pair_cost"], base["pair_qty"]), 8),
        "gross_pair_pnl_positive": base["pair_pnl"] > 0.0,
        "closed_cycle_pnl_proxy": round(closed_cycle, 8),
        "stress100_closed_cycle_pnl_proxy": round(stress100, 8),
        "residual_qty_share_of_pair_plus_residual": round(
            safe_ratio(base["residual_qty"], base["pair_qty"] + base["residual_qty"]), 8
        ),
        "residual_cost_share_of_seed_cost": round(safe_ratio(base["residual_cost"], base["seed_cost"]), 8),
        "gross_positive_but_stress_negative": base["pair_pnl"] > 0.0 and stress100 <= 0.0,
    }


def predicate_is_forbidden(predicate: tuple[tuple[str, str], ...]) -> bool:
    fields = {field for field, _ in predicate}
    values = set(predicate)
    if fields <= STATIC_FIELDS:
        return True
    if not (fields & CYCLE_FIELDS):
        return True
    if ("balance", "deficit_0_0_25") in values:
        return True
    if ("open", "open_le_1") in values:
        return True
    return False


def predicate_warning(predicate: tuple[tuple[str, str], ...]) -> list[str]:
    warnings: list[str] = []
    values = set(predicate)
    fields = {field for field, _ in predicate}
    if len(predicate) == 1 and values & {
        ("same_qty", "same_zero"),
        ("preseed_sides", "opp_only_preseed"),
        ("open", "open_1_2_5"),
        ("balance", "deficit_0_25_1_25"),
    }:
        warnings.append("broad_first_opposite_seed_family_not_yet_a_rule")
    if fields & {"side", "offset"} and len(fields) <= 2:
        warnings.append("could_degenerate_to_static_side_or_offset_filter")
    return warnings


def build_feature_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], set[int]]:
    index: dict[tuple[str, str], set[int]] = defaultdict(set)
    for idx, row in enumerate(rows):
        for key, value in row["_features"].items():  # type: ignore[union-attr]
            index[(key, value)].add(idx)
    return index


def eval_predicate(
    rows: list[dict[str, Any]],
    feature_index: dict[tuple[str, str], set[int]],
    predicate: tuple[tuple[str, str], ...],
    train_idx: set[int],
    holdout_idx: set[int],
) -> dict[str, Any] | None:
    matched = set(range(len(rows)))
    for clause in predicate:
        matched &= feature_index.get(clause, set())
    train_matched = matched & train_idx
    holdout_matched = matched & holdout_idx
    if len(train_matched) < 200 or len(holdout_matched) < 100:
        return None
    train = derived(totals(rows, train_matched))
    holdout = derived(totals(rows, holdout_matched))
    if train["seed_qty"] < 1000.0 or holdout["seed_qty"] < 500.0:
        return None
    stable = (
        train["closed_cycle_pnl_proxy"] > 0.0
        and holdout["closed_cycle_pnl_proxy"] > 0.0
        and train["stress100_closed_cycle_pnl_proxy"] > 0.0
        and holdout["stress100_closed_cycle_pnl_proxy"] > 0.0
        and train["weighted_pair_cost"] <= 1.0
        and holdout["weighted_pair_cost"] <= 1.0
        and not train["gross_positive_but_stress_negative"]
        and not holdout["gross_positive_but_stress_negative"]
    )
    score = (
        float(holdout["stress100_closed_cycle_pnl_proxy"])
        + 0.25 * float(train["stress100_closed_cycle_pnl_proxy"])
        - 100.0 * max(0.0, float(holdout["residual_qty_share_of_pair_plus_residual"]) - 0.20)
    )
    return {
        "predicate": [{"field": field, "value": value} for field, value in predicate],
        "predicate_key": "&".join(f"{field}={value}" for field, value in predicate),
        "warnings": predicate_warning(predicate),
        "train": train,
        "holdout": holdout,
        "stable_closed_cycle_objective": stable,
        "score": round(score, 8),
    }


def search_candidates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    train_days, holdout_days = split_days(rows)
    train_idx = {idx for idx, row in enumerate(rows) if str(row.get("day") or "") in train_days}
    holdout_idx = {idx for idx, row in enumerate(rows) if str(row.get("day") or "") in holdout_days}
    feature_index = build_feature_index(rows)
    feature_values: dict[str, list[str]] = defaultdict(list)
    for (field, value) in feature_index:
        feature_values[field].append(value)
    for field in feature_values:
        feature_values[field] = sorted(feature_values[field])

    evaluated = 0
    candidates: list[dict[str, Any]] = []
    fields = sorted(feature_values)
    for width in (1, 2, 3):
        for field_combo in itertools.combinations(fields, width):
            for value_combo in itertools.product(*(feature_values[field] for field in field_combo)):
                predicate = tuple(zip(field_combo, value_combo))
                if predicate_is_forbidden(predicate):
                    continue
                result = eval_predicate(rows, feature_index, predicate, train_idx, holdout_idx)
                evaluated += 1
                if result and result["stable_closed_cycle_objective"]:
                    candidates.append(result)
    candidates.sort(
        key=lambda row: (
            not row["warnings"],
            float(row["score"]),
            float(row["holdout"]["stress100_closed_cycle_pnl_proxy"]),
        ),
        reverse=True,
    )
    return {
        "train_days": sorted(train_days),
        "holdout_days": sorted(holdout_days),
        "train_control": derived(totals(rows, train_idx)),
        "holdout_control": derived(totals(rows, holdout_idx)),
        "candidate_predicates_evaluated": evaluated,
        "stable_candidate_count": len(candidates),
        "top_candidates": candidates[:25],
        "selected_candidate": candidates[0] if candidates else None,
    }


def summarize_public_reset(manifest: dict[str, Any]) -> dict[str, Any]:
    ranked = manifest.get("cross_profile_summary", {}).get("ranked_profile_handles", [])
    return {
        "decision_label": manifest.get("decision_label"),
        "dplus_discard_boundary": manifest.get("dplus_discard_boundary"),
        "profile_count": manifest.get("cross_profile_summary", {}).get("profile_count"),
        "profiles_with_keep_signal": manifest.get("cross_profile_summary", {}).get("profiles_with_keep_signal"),
        "ranked_profile_handles": ranked,
        "candidate_direction": manifest.get("research_ranking", {}).get("candidate_direction"),
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    paths = {
        "separator_csv": args.separator_csv,
        "public_profile_reset_manifest": args.public_profile_reset_manifest,
        "output_dir": args.output_dir,
    }
    path_safety = {key: path_safe(path) for key, path in paths.items()}
    if not all(path_safety.values()):
        raise RuntimeError(f"unsafe paths: {path_safety}")
    rows = load_separator(args.separator_csv)
    public_reset = read_json(args.public_profile_reset_manifest)
    search = search_candidates(rows)
    selected = search.get("selected_candidate")
    blockers: list[str] = []
    if not selected:
        blockers.append("no_train_holdout_stable_closed_cycle_candidate")
    else:
        warnings = selected.get("warnings") or []
        if warnings:
            blockers.extend(f"selected_candidate_warning:{warning}" for warning in warnings)
        blockers.append("no_same_window_no_order_closed_cycle_marker_yet")
    decision = "KEEP" if selected else "UNKNOWN"
    label = (
        "KEEP_CLOSED_CYCLE_PAIRING_OBJECTIVE_AUDIT_READY_NO_ORDER_MARKER_BLOCKED"
        if selected
        else "UNKNOWN_CLOSED_CYCLE_PAIRING_OBJECTIVE_NO_STABLE_LOCAL_CANDIDATE"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "closed_cycle_pairing_objective_audit",
        "decision": decision,
        "decision_label": label,
        "inputs": {key: str(value) for key, value in paths.items()},
        "path_safety": path_safety,
        "scope": {
            "local_only": True,
            "public_profile_proxy_truth_only": True,
            "post_action_labels_used_only_for_offline_evaluation": True,
            "future_labels_used_as_live_criteria": False,
            "private_truth_ready": False,
            "deployable": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "public_profile_reset_summary": summarize_public_reset(public_reset),
        "search_policy": {
            "objective": "risk_adjusted_closed_cycle_pnl_after_fee_residual_and_stress",
            "frozen_dplus_values_excluded": [
                "balance=deficit_0_0_25",
                "open=open_le_1",
                "pure side/risk/offset static predicates",
            ],
            "gross_pair_pnl_success_rejected_if_stress_negative": True,
            "required_cycle_fields": sorted(CYCLE_FIELDS),
            "not_a_trading_rule": True,
        },
        "candidate_search": search,
        "blockers": blockers,
        "interpretation": {
            "historical_closed_cycle_objective_is_expressible": bool(selected),
            "selected_family_is_deployable": False,
            "why_not_deployable": (
                "The selected historical family is broad and lacks a same-window no-order closed-cycle marker. "
                "It is useful to fix the objective function, not to enable a rule."
                if selected
                else "No stable candidate passed the strict local objective screen."
            ),
            "methodology_correction": (
                "Gross pair PnL is now explicitly subordinated to fee-adjusted residual-stress-adjusted closed-cycle PnL."
            ),
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "selected_candidate_key": selected.get("predicate_key") if selected else None,
            "ranking_basis": "holdout stress100_closed_cycle_pnl_proxy with train stability and gross-positive/stress-negative rejection",
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_OBJECTIVE_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement a default-off closed-cycle no-order marker summary/scorer only if the runner can expose "
            "pre-action cycle denominators without behavior changes; otherwise return UNKNOWN. Do not start shadow "
            "from this local audit."
            if selected
            else "Return UNKNOWN and avoid another public-profile-derived shadow until allowed exports can express closed-cycle markers."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--separator-csv", type=Path, default=DEFAULT_SEPARATOR_CSV)
    parser.add_argument("--public-profile-reset-manifest", type=Path, default=DEFAULT_PUBLIC_PROFILE_RESET)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    selected = manifest["candidate_search"].get("selected_candidate")
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(args.output_dir / "manifest.json"),
                "stable_candidate_count": manifest["candidate_search"]["stable_candidate_count"],
                "selected_candidate_key": selected.get("predicate_key") if selected else None,
                "blockers": manifest["blockers"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
