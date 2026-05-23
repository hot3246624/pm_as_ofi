#!/usr/bin/env python3
"""Audit ledger-before/delta backtest-to-no-order transfer gaps.

This local-only audit consumes the historical candidate separator export plus
already-pulled no-order summary/score artifacts. It does not read events JSONL,
raw/replay stores, collector stores, sockets, SSH, shared-ingress, or live
trading state.
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


ARTIFACT = "xuan_b27_dplus_ledger_before_delta_gap_audit_v1"
DEFAULT_SEPARATOR_CSV = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_COMPLETION = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_ledger_before_delta_marker_completion_20260523T183809Z/"
    "manifest.json"
)
DEFAULT_SUMMARY_DIR = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_ledger_before_delta_marker_driver_20260523T173308Z/"
    "remote_clean/output"
)
DEFAULT_AGGREGATE = DEFAULT_SUMMARY_DIR / "aggregate_report.json"
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


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        try:
            return float(value)
        except ValueError:
            return default
    return default


def maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= DUST:
        return 0.0
    return numerator / denominator


def offset_bucket(offset_s: float) -> str:
    if offset_s < 0.0:
        return "offset_unknown"
    if offset_s < 30.0:
        return "offset_0_30"
    if offset_s < 60.0:
        return "offset_30_60"
    if offset_s < 90.0:
        return "offset_60_90"
    if offset_s < 120.0:
        return "offset_90_120"
    return "offset_ge_120"


def qty_bucket(prefix: str, qty: float | None) -> str:
    if qty is None:
        return f"{prefix}_unknown"
    if qty <= DUST:
        return f"{prefix}_zero"
    if qty <= 1.0:
        return f"{prefix}_le_1"
    if qty <= 2.0:
        return f"{prefix}_1_2"
    if qty < 5.0:
        return f"{prefix}_2_5"
    if abs(qty - 5.0) <= 1e-9:
        return f"{prefix}_eq_5"
    return f"{prefix}_gt_5"


def deficit_bucket(same_qty: float, opp_qty: float) -> str:
    deficit = opp_qty - same_qty
    if deficit <= DUST:
        return "deficit_le_0"
    if deficit <= 0.25 + 1e-12:
        return "deficit_0_0_25"
    if deficit <= 1.0 + 1e-12:
        return "deficit_0_25_1"
    if deficit <= 2.0 + 1e-12:
        return "deficit_1_2"
    return "deficit_gt_2"


def ledger_bucket(value: float | None, prefix: str) -> str:
    if value is None:
        return f"{prefix}_unknown"
    if value < -2.0:
        return f"{prefix}_lt_m2"
    if value < -1.0:
        return f"{prefix}_m2_m1"
    if value < -0.25:
        return f"{prefix}_m1_m025"
    if value < 0.0:
        return f"{prefix}_m025_0"
    if value < 0.25:
        return f"{prefix}_0_025"
    if value < 1.0:
        return f"{prefix}_025_1"
    return f"{prefix}_gte_1"


def row_features(row: dict[str, Any]) -> dict[str, str]:
    same_qty = as_float(row.get("pre_seed_same_qty"))
    opp_qty = as_float(row.get("pre_seed_opp_qty"))
    before = maybe_float(row.get("ledger_proxy_before"))
    after = maybe_float(row.get("ledger_proxy_after"))
    delta = after - before if before is not None and after is not None else None
    return {
        "side": str(row.get("side") or ""),
        "risk": str(row.get("source_risk_direction") or ""),
        "offset": offset_bucket(as_float(row.get("offset_s"), default=-1.0)),
        "open": qty_bucket("open_qty", same_qty + opp_qty),
        "deficit": deficit_bucket(same_qty, opp_qty),
        "ledger_before": ledger_bucket(before, "before"),
        "ledger_delta": ledger_bucket(delta, "delta"),
    }


def marker_key_from_features(features: dict[str, str], ledger_field: str) -> str:
    return "|".join(
        (
            features["side"],
            features["offset"],
            features["risk"],
            features["open"],
            features["deficit"],
            features[ledger_field],
        )
    )


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"{path} has no rows")
    for row in rows:
        features = row_features(row)
        row["_features"] = features  # type: ignore[index]
        row["_before_marker_key"] = marker_key_from_features(features, "ledger_before")  # type: ignore[index]
        row["_delta_marker_key"] = marker_key_from_features(features, "ledger_delta")  # type: ignore[index]
    return rows


def split_days(rows: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    days = sorted({str(row.get("day") or "") for row in rows if row.get("day")})
    if len(days) < 2:
        raise ValueError("need at least two days for train/holdout")
    return set(days[::2]), set(days[1::2])


def build_index(rows: list[dict[str, Any]], indices: set[int]) -> dict[tuple[str, str], set[int]]:
    out: dict[tuple[str, str], set[int]] = defaultdict(set)
    for idx in indices:
        features = rows[idx]["_features"]  # type: ignore[index]
        for key, value in features.items():
            out[(key, value)].add(idx)
    return out


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


def gate_failures(comparison: dict[str, Any], *, min_match: int) -> list[str]:
    failures: list[str] = []
    if comparison["matched"]["rows"] < min_match:
        failures.append("matched_rows_below_min")
    if comparison["seed_qty_retention"] < 0.90:
        failures.append("seed_qty_retention_below_90pct")
    if comparison["pair_qty_retention"] < 0.90:
        failures.append("pair_qty_retention_below_90pct")
    if comparison["residual_qty_rate_reduction"] < 0.20:
        failures.append("residual_qty_rate_reduction_below_20pct")
    if comparison["residual_cost_rate_reduction"] < 0.20:
        failures.append("residual_cost_rate_reduction_below_20pct")
    if comparison["weighted_pair_cost_worse_ratio"] > 1.01:
        failures.append("weighted_pair_cost_worse_ratio_above_1p01")
    if comparison["fee_after_pnl_proxy_delta"] < 0.0:
        failures.append("fee_after_pnl_proxy_delta_negative")
    if comparison["stress100_worst_pnl_proxy_delta"] < 0.0:
        failures.append("stress100_worst_pnl_proxy_delta_negative")
    return failures


def combo_name(combo: tuple[tuple[str, str], ...]) -> str:
    return "|".join(f"{key}={value}" for key, value in combo)


def forbidden_family(combo: tuple[tuple[str, str], ...]) -> bool:
    values = dict(combo)
    if (
        values.get("risk") == "repair_or_pairing_improving"
        and values.get("offset") == "offset_90_120"
        and values.get("open") == "open_qty_le_1"
        and values.get("deficit") == "deficit_0_0_25"
    ):
        return True
    if values.get("ledger_before") == "before_gte_1" and (
        values.get("offset") == "offset_90_120" or values.get("open") == "open_qty_le_1"
    ):
        return True
    return False


def flatten_marker_counts(summary_paths: list[Path]) -> dict[str, Counter[str]]:
    counts = {"ledger_before": Counter(), "ledger_delta": Counter()}
    field_by_kind = {
        "ledger_before": "transition_count_by_status_side_offset_risk_open_deficit_ledger_before",
        "ledger_delta": "transition_count_by_status_side_offset_risk_open_deficit_ledger_delta",
    }
    for path in summary_paths:
        data = read_json(path)
        marker = ((data.get("event_lite") or {}).get("source_opportunity_marker_summary") or {})
        if not isinstance(marker, dict):
            continue
        for kind, field in field_by_kind.items():
            table = marker.get(field) or {}
            if not isinstance(table, dict):
                continue
            for status, bucket in table.items():
                if not isinstance(bucket, dict):
                    continue
                for key, value in bucket.items():
                    if isinstance(value, (int, float)):
                        counts[kind][f"{status}|{key}"] += float(value)
    return counts


def key_matches_combo(key: str, combo: tuple[tuple[str, str], ...], ledger_field: str) -> bool:
    parts = key.split("|")
    if len(parts) != 7:
        return False
    status, side, offset, risk, open_bucket, deficit, ledger_bucket_value = parts
    fields = {
        "status": status,
        "side": side,
        "offset": offset,
        "risk": risk,
        "open": open_bucket,
        "deficit": deficit,
        ledger_field: ledger_bucket_value,
    }
    return all(fields.get(field) == value for field, value in combo)


def marker_for_combo(
    counts: Counter[str], combo: tuple[tuple[str, str], ...], ledger_field: str
) -> dict[str, Any]:
    matched = {key: value for key, value in counts.items() if key_matches_combo(key, combo, ledger_field)}
    admitted = sum(value for key, value in matched.items() if key.startswith("admitted|"))
    blocked = sum(value for key, value in matched.items() if key.startswith("blocked|"))
    return {
        "marker_total": round(admitted + blocked, 8),
        "admitted_count": round(admitted, 8),
        "blocked_count": round(blocked, 8),
        "matched_marker_keys": dict(sorted(matched.items(), key=lambda item: (-item[1], item[0]))[:20]),
    }


def distribution_from_counts(counts: dict[str, Counter[str]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for kind, counter in counts.items():
        by_status_bucket: Counter[str] = Counter()
        top: list[dict[str, Any]] = []
        for key, value in counter.items():
            parts = key.split("|")
            if len(parts) != 7:
                continue
            by_status_bucket[f"{parts[0]}|{parts[-1]}"] += value
            top.append({"marker_key": key, "transition_count": round(value, 8)})
        out[kind] = {
            "by_status_bucket": dict(sorted(by_status_bucket.items())),
            "top_marker_keys": sorted(top, key=lambda row: (-row["transition_count"], row["marker_key"]))[:30],
        }
    return out


def historical_distribution(rows: list[dict[str, Any]], train_indices: set[int], holdout_indices: set[int]) -> dict[str, Any]:
    def count(indices: set[int], field: str) -> dict[str, float]:
        counter: Counter[str] = Counter()
        for idx in indices:
            counter[rows[idx]["_features"][field]] += 1.0  # type: ignore[index]
        return dict(sorted(counter.items()))

    full_indices = set(range(len(rows)))
    before_counter: Counter[str] = Counter()
    delta_counter: Counter[str] = Counter()
    for idx in full_indices:
        before_counter[str(rows[idx]["_before_marker_key"])] += 1.0
        delta_counter[str(rows[idx]["_delta_marker_key"])] += 1.0
    return {
        "ledger_before_full": count(full_indices, "ledger_before"),
        "ledger_before_train": count(train_indices, "ledger_before"),
        "ledger_before_holdout": count(holdout_indices, "ledger_before"),
        "ledger_delta_full": count(full_indices, "ledger_delta"),
        "ledger_delta_train": count(train_indices, "ledger_delta"),
        "ledger_delta_holdout": count(holdout_indices, "ledger_delta"),
        "top_full_before_marker_keys": dict(before_counter.most_common(20)),
        "top_full_delta_marker_keys": dict(delta_counter.most_common(20)),
    }


def risk_budget_from_completion(completion: dict[str, Any]) -> dict[str, Any]:
    metrics = completion.get("trading_metrics") if isinstance(completion.get("trading_metrics"), dict) else {}
    promotion_gate = completion.get("promotion_gate") if isinstance(completion.get("promotion_gate"), dict) else {}
    source_gate = promotion_gate.get("source") if isinstance(promotion_gate.get("source"), dict) else {}
    normalized = source_gate.get("normalized_risk_budget") if isinstance(source_gate.get("normalized_risk_budget"), dict) else {}
    candidates = as_float(metrics.get("candidates"))
    net_pair_cost_p90 = as_float(metrics.get("net_pair_cost_p90"))
    residual_qty_share = as_float(metrics.get("residual_qty_share_of_filled"))
    residual_cost_share = as_float(metrics.get("residual_cost_share_of_filled_cost"))
    pair_tail_loss_share = as_float(
        metrics.get("pair_tail_loss_share_of_pair_pnl"),
        default=as_float(normalized.get("pair_tail_loss_share_of_pair_pnl")),
    )
    return {
        "candidates": candidates,
        "net_pair_cost_p90": net_pair_cost_p90,
        "residual_qty_share_of_filled": residual_qty_share,
        "residual_cost_share_of_filled_cost": residual_cost_share,
        "pair_tail_loss_share_of_pair_pnl": pair_tail_loss_share,
        "fee_adjusted_pair_pnl_proxy": as_float(metrics.get("fee_adjusted_pair_pnl_proxy")),
        "risk_adjusted_pnl_proxy": as_float(metrics.get("conservative_risk_adjusted_pnl_proxy")),
        "passed": (
            candidates >= 100.0
            and net_pair_cost_p90 <= 1.0
            and residual_qty_share <= 0.15
            and residual_cost_share <= 0.20
            and pair_tail_loss_share <= 0.05
        ),
        "promotion_gate_passed": bool(promotion_gate.get("passed")),
        "hard_blockers": source_gate.get("hard_blockers", []),
    }


def residual_tail_context(aggregate: dict[str, Any]) -> dict[str, Any]:
    exemplars = ((aggregate.get("event_lite") or {}).get("source_link_residual_tail_exemplars") or [])
    if not isinstance(exemplars, list):
        exemplars = []
    before_cost: Counter[str] = Counter()
    delta_cost: Counter[str] = Counter()
    before_count: Counter[str] = Counter()
    delta_count: Counter[str] = Counter()
    for exemplar in exemplars:
        if not isinstance(exemplar, dict):
            continue
        features = row_features(exemplar)
        before_key = marker_key_from_features(features, "ledger_before")
        delta_key = marker_key_from_features(features, "ledger_delta")
        residual_cost = as_float(exemplar.get("source_residual_cost"))
        before_count[before_key] += 1.0
        delta_count[delta_key] += 1.0
        before_cost[before_key] += residual_cost
        delta_cost[delta_key] += residual_cost
    return {
        "exemplar_count": len(exemplars),
        "top_before_by_residual_cost": [
            {"marker_key": key, "residual_cost": round(value, 8), "count": before_count.get(key, 0.0)}
            for key, value in before_cost.most_common(15)
        ],
        "top_delta_by_residual_cost": [
            {"marker_key": key, "residual_cost": round(value, 8), "count": delta_count.get(key, 0.0)}
            for key, value in delta_cost.most_common(15)
        ],
    }


def enumerate_candidates(
    rows: list[dict[str, Any]],
    train_indices: set[int],
    holdout_indices: set[int],
    marker_counts: dict[str, Counter[str]],
    *,
    min_match: int,
    max_optional_terms: int,
    min_marker_total: float,
) -> dict[str, Any]:
    train_index = build_index(rows, train_indices)
    holdout_index = build_index(rows, holdout_indices)
    terms = sorted(train_index)
    optional_terms = [term for term in terms if term[0] in {"side", "risk", "offset", "open", "deficit"}]
    generated = 0
    marker_supported = 0
    train_gate = 0
    train_holdout_gate = 0
    viable: list[dict[str, Any]] = []
    near_misses: list[dict[str, Any]] = []

    for ledger_field in ("ledger_before", "ledger_delta"):
        ledger_terms = [term for term in terms if term[0] == ledger_field and not term[1].endswith("_unknown")]
        for ledger_term in ledger_terms:
            for width in range(0, max_optional_terms + 1):
                for optional in itertools.combinations(optional_terms, width):
                    combo = tuple(sorted((ledger_term, *optional)))
                    fields = [field for field, _value in combo]
                    if len(fields) != len(set(fields)) or forbidden_family(combo):
                        continue
                    generated += 1
                    marker = marker_for_combo(marker_counts[ledger_field], combo, ledger_field)
                    if marker["marker_total"] < min_marker_total:
                        continue
                    marker_supported += 1
                    train_matched = set.intersection(*(train_index.get(term, set()) for term in combo))
                    holdout_matched = set.intersection(*(holdout_index.get(term, set()) for term in combo))
                    train = compare(rows, train_indices, train_matched)
                    holdout = compare(rows, holdout_indices, holdout_matched)
                    train_failures = gate_failures(train, min_match=min_match)
                    holdout_failures = gate_failures(holdout, min_match=min_match)
                    if not train_failures:
                        train_gate += 1
                    if not train_failures and not holdout_failures:
                        train_holdout_gate += 1
                    item = {
                        "predicate_name": combo_name(combo),
                        "ledger_marker_kind": ledger_field,
                        "predicate_terms": [{"field": field, "value": value} for field, value in combo],
                        "marker": marker,
                        "train": train,
                        "holdout": holdout,
                        "train_failures": train_failures,
                        "holdout_failures": holdout_failures,
                    }
                    score = (
                        marker["marker_total"] / 100.0
                        + max(0.0, holdout["residual_cost_rate_reduction"])
                        + max(0.0, holdout["residual_qty_rate_reduction"])
                        - 0.25 * len(train_failures)
                        - 0.50 * len(holdout_failures)
                    )
                    item["_score"] = round(score, 8)
                    near_misses.append(item)
                    if not train_failures and not holdout_failures:
                        viable.append(item)
    return {
        "generated_candidate_count": generated,
        "marker_supported_candidate_count": marker_supported,
        "train_gate_candidate_count": train_gate,
        "train_holdout_marker_candidate_count": train_holdout_gate,
        "viable_candidates": sorted(viable, key=lambda item: (-item["_score"], item["predicate_name"]))[:20],
        "near_misses": sorted(near_misses, key=lambda item: (-item["_score"], item["predicate_name"]))[:20],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--separator-csv", type=Path, default=DEFAULT_SEPARATOR_CSV)
    parser.add_argument("--completion-manifest", type=Path, default=DEFAULT_COMPLETION)
    parser.add_argument("--aggregate-report", type=Path, default=DEFAULT_AGGREGATE)
    parser.add_argument("--summary-dir", type=Path, default=DEFAULT_SUMMARY_DIR)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-match", type=int, default=20)
    parser.add_argument("--min-marker-total", type=float, default=1.0)
    parser.add_argument("--max-optional-terms", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    separator_csv = args.separator_csv if args.separator_csv.is_absolute() else root / args.separator_csv
    completion_path = args.completion_manifest if args.completion_manifest.is_absolute() else root / args.completion_manifest
    aggregate_path = args.aggregate_report if args.aggregate_report.is_absolute() else root / args.aggregate_report
    summary_dir = args.summary_dir if args.summary_dir.is_absolute() else root / args.summary_dir
    output = args.output if args.output.is_absolute() else root / args.output
    summary_paths = sorted(summary_dir.glob("*.summary.json")) if summary_dir.exists() else []
    required = [separator_csv, completion_path, aggregate_path, *summary_paths]
    unsafe = [str(path) for path in [*required, output] if not path_safe(path)]
    missing = [str(path) for path in [separator_csv, completion_path, aggregate_path] if not path.exists()]
    if unsafe or missing or not summary_paths:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": utc_now(),
            "decision": "BLOCKED",
            "decision_label": "BLOCKED_LEDGER_BEFORE_DELTA_GAP_INPUT_UNAVAILABLE",
            "unsafe_paths": unsafe,
            "missing": missing,
            "summary_count": len(summary_paths),
            "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
        }
        write_json(output, manifest)
        print(json.dumps({"decision": manifest["decision"], "output": str(output)}, sort_keys=True))
        return 2

    rows = load_rows(separator_csv)
    train_days, holdout_days = split_days(rows)
    train_indices = {idx for idx, row in enumerate(rows) if row.get("day") in train_days}
    holdout_indices = {idx for idx, row in enumerate(rows) if row.get("day") in holdout_days}
    completion = read_json(completion_path)
    aggregate = read_json(aggregate_path)
    marker_counts = flatten_marker_counts(summary_paths)
    risk_budget = risk_budget_from_completion(completion)
    candidates = enumerate_candidates(
        rows,
        train_indices,
        holdout_indices,
        marker_counts,
        min_match=args.min_match,
        max_optional_terms=args.max_optional_terms,
        min_marker_total=args.min_marker_total,
    )
    viable = candidates["viable_candidates"]
    blockers: list[str] = []
    cautions: list[str] = []
    if candidates["marker_supported_candidate_count"] <= 0:
        blockers.append("no_before_or_delta_no_order_marker_supported_candidate")
    if not viable:
        blockers.append("no_train_holdout_stable_candidate_with_nonzero_before_or_delta_marker")
    if not risk_budget["passed"]:
        cautions.append("latest_no_order_aggregate_risk_budget_failed_not_candidate_specific")
    if viable and viable[0]["marker"]["marker_total"] < 5.0:
        cautions.append("top_candidate_no_order_marker_total_below_5")

    if viable:
        decision = "KEEP"
        decision_label = "KEEP_LEDGER_BEFORE_DELTA_GAP_AUDIT_NEW_PRE_ACTION_FAMILY_FOUND"
        next_action = (
            "Prepare a local-only packet for the top before/delta family, then only run a bounded no-order diagnostic "
            "after confirming no active xuan-research runner and keeping all guards/trading rules disabled."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_LEDGER_BEFORE_DELTA_GAP_AUDIT_NO_SAFE_FAMILY"
        next_action = (
            "Return UNKNOWN for the current allowed before/delta marker field set. Do not SSH/start another shadow from "
            "this result; either add a genuinely new default-off pre-action denominator or change research direction."
        )

    manifest = {
        "artifact": ARTIFACT,
        "schema_version": "ledger_before_delta_gap_audit_v1",
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
            "completion_manifest": str(completion_path),
            "aggregate_report": str(aggregate_path),
            "summary_dir": str(summary_dir),
            "summary_count": len(summary_paths),
            "row_count": len(rows),
            "train_days": sorted(train_days),
            "holdout_days": sorted(holdout_days),
            "unsafe_paths": unsafe,
        },
        "guardrails": {
            "requires_ledger_before_or_delta_pre_action_field": True,
            "requires_no_order_marker_denominator": True,
            "requires_train_holdout_stability": True,
            "requires_sample_preservation": True,
            "source_pair_source_residual_used_as_live_criteria": False,
            "excluded_families": [
                "frozen_micro_deficit_offset_90_120_open_le_1_deficit_0_0_25",
                "frozen_ledger_before_gte_1_offset_90_120_or_open_le_1_sweep",
                "price_or_public_l1_caps",
                "static_side_or_offset_deletion",
                "cooldown_or_admission_caps",
                "fill_to_balance_or_portfolio_ledger_trading_rule_revival",
            ],
        },
        "risk_budget": risk_budget,
        "historical_distribution": historical_distribution(rows, train_indices, holdout_indices),
        "no_order_distribution": distribution_from_counts(marker_counts),
        "residual_tail_context": residual_tail_context(aggregate),
        "candidate_search": candidates,
        "blockers": blockers,
        "cautions": cautions,
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "interpretation": (
                "This audit can nominate a local research lead only when a before/delta predicate is stable on "
                "train/holdout and has nonzero same-window no-order marker denominator. It remains research-only "
                "and cannot prove private truth, deployability, canary readiness, or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_LEDGER_BEFORE_DELTA_GAP_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": next_action,
    }
    write_json(output, manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
