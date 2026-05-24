#!/usr/bin/env python3
"""Build compact diagnostics from no-order runner event JSONL files.

This is intentionally read-only: it scans ``*.events.jsonl`` files under a
single xuan-owned runtime output root and writes a small JSON summary suitable
for residual-regime scoring.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError):
        return None


def add_stat(stats: dict[str, list[float]], key: str, value: Any) -> None:
    val = as_float(value)
    if val is not None:
        stats.setdefault(key, []).append(val)


def percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * pct
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    weight = pos - lo
    return sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight


def summarize(values: Iterable[float]) -> dict[str, float | int | None]:
    vals = sorted(v for v in values if math.isfinite(v))
    if not vals:
        return {}
    return {
        "n": len(vals),
        "count": len(vals),
        "min": vals[0],
        "p50": percentile(vals, 0.50),
        "p90": percentile(vals, 0.90),
        "p99": percentile(vals, 0.99),
        "max": vals[-1],
    }


def first_number(obj: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        val = as_float(obj.get(key))
        if val is not None:
            return val
    return None


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {k: rounded(v) for k, v in value.items()}
    if isinstance(value, list):
        return [rounded(v) for v in value]
    return value


def scan(
    root: Path,
    *,
    risk_seed_pair_completion_required_above_net_cap: float | None = None,
    risk_seed_pair_completion_required_above_fair_price_pair_cost: float | None = None,
    risk_seed_pair_completion_min_qty: float = 0.0,
) -> dict[str, Any]:
    event_files = sorted(root.glob("*.events.jsonl"))
    event_counts: Counter[str] = Counter()
    block_reason_counts: Counter[str] = Counter()
    kind_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
    per_slug_event_counts: dict[str, Counter[str]] = defaultdict(Counter)
    per_slug_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
    per_slug_strict_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
    candidate_pair_completion_decisions: Counter[str] = Counter()
    candidate_fair_price_admission_decisions: Counter[str] = Counter()
    candidate_fair_price_admission_modes: Counter[str] = Counter()
    fair_price_admission_block_reasons: Counter[str] = Counter()
    pair_completion_block_reasons: Counter[str] = Counter()
    risk_seed_closeability_block_reasons: Counter[str] = Counter()
    cancel_reasons: Counter[str] = Counter()
    strict_rescue_block_reasons: Counter[str] = Counter()
    strict_rescue_block_components: Counter[str] = Counter()
    strict_rescue_lot_age_min_cost_classes: Counter[str] = Counter()
    strict_rescue_close_skipped_low_cost_lots: Counter[str] = Counter()
    candidate_pending_opp_credit_values: Counter[str] = Counter()
    candidate_soft_decisions: Counter[str] = Counter()
    candidate_risk_increasing_seed_values: Counter[str] = Counter()
    risk_seed_pair_completion_required_counterfactual: Counter[str] = Counter()
    per_slug_pair_completion_required_counterfactual: dict[str, Counter[str]] = defaultdict(Counter)
    source_missing = 0
    source_total = 0
    bad_json = 0
    raw_line_count = 0
    stats: dict[str, list[float]] = {}

    for path in event_files:
        with path.open() as handle:
            for line in handle:
                raw_line_count += 1
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue
                kind = str(obj.get("kind") or "<missing>")
                slug = str(obj.get("slug") or path.name.replace(".events.jsonl", ""))
                event_counts[kind] += 1
                per_slug_event_counts[slug][kind] += 1

                reason = str(
                    obj.get("block_reason")
                    or obj.get("source_quality_block_reason")
                    or obj.get("reason")
                    or ""
                )
                if reason:
                    block_reason_counts[reason] += 1
                    kind_reason_counts[kind][reason] += 1
                    per_slug_reason_counts[slug][reason] += 1

                if kind == "candidate":
                    source_total += 1
                    if not (
                        obj.get("source_quality_trade_source_sequence_id")
                        and obj.get("source_quality_l1_source_sequence_id")
                        and obj.get("source_quality_l2_source_sequence_id")
                    ):
                        source_missing += 1
                    add_stat(stats, "candidate_closeability_net_pair_cost", obj.get("closeability_net_pair_cost"))
                    add_stat(stats, "candidate_closeability_debt", obj.get("closeability_debt"))
                    add_stat(stats, "candidate_closeability_debt_per_share", obj.get("closeability_debt_per_share"))
                    add_stat(stats, "candidate_closeability_debt_post_open", obj.get("closeability_debt_post_open"))
                    add_stat(stats, "candidate_closeability_debt_pre_open", obj.get("closeability_debt_pre_open"))
                    add_stat(stats, "candidate_l1_age_ms", obj.get("source_quality_l1_age_ms"))
                    add_stat(stats, "candidate_pair_completion_avg_net_pair_cost", obj.get("pair_completion_avg_net_pair_cost"))
                    add_stat(stats, "candidate_pair_completion_worst_net_pair_cost", obj.get("pair_completion_worst_net_pair_cost"))
                    add_stat(stats, "candidate_pair_completion_projected_pair_pnl_after", obj.get("pair_completion_projected_pair_pnl_after"))
                    add_stat(stats, "candidate_fair_price_pair_cost_after_fee", obj.get("fair_price_pair_cost_after_fee"))
                    add_stat(stats, "candidate_fair_price_edge_after_fee", obj.get("fair_price_edge_after_fee"))
                    decision = str(obj.get("pair_completion_decision") or "<missing>")
                    candidate_pair_completion_decisions[decision] += 1
                    fair_decision = str(obj.get("fair_price_admission_decision") or "<missing>")
                    candidate_fair_price_admission_decisions[fair_decision] += 1
                    fair_mode = str(obj.get("fair_price_admission_mode") or "<missing>")
                    candidate_fair_price_admission_modes[fair_mode] += 1
                    soft_decision = str(obj.get("risk_seed_closeability_soft_decision") or "<missing>")
                    candidate_soft_decisions[soft_decision] += 1
                    risk_increasing_seed = bool(obj.get("risk_increasing_seed"))
                    candidate_risk_increasing_seed_values[str(risk_increasing_seed).lower()] += 1
                    if (
                        risk_seed_pair_completion_required_above_net_cap is not None
                        or risk_seed_pair_completion_required_above_fair_price_pair_cost is not None
                    ):
                        closeability_net_pair_cost = as_float(obj.get("closeability_net_pair_cost"))
                        fair_price_pair_cost_after_fee = as_float(obj.get("fair_price_pair_cost_after_fee"))
                        pair_completion_qty = as_float(obj.get("pair_completion_qty")) or 0.0
                        required_qty = max(1.0, risk_seed_pair_completion_min_qty)
                        applies_by_closeability = (
                            risk_seed_pair_completion_required_above_net_cap is not None
                            and (
                                closeability_net_pair_cost is None
                                or closeability_net_pair_cost
                                > risk_seed_pair_completion_required_above_net_cap + 1e-12
                            )
                        )
                        applies_by_fair_price = (
                            risk_seed_pair_completion_required_above_fair_price_pair_cost is not None
                            and fair_price_pair_cost_after_fee is not None
                            and fair_price_pair_cost_after_fee
                            > risk_seed_pair_completion_required_above_fair_price_pair_cost + 1e-12
                        )
                        applies = risk_increasing_seed and (applies_by_closeability or applies_by_fair_price)
                        if not applies:
                            decision = "not_applicable"
                        elif pair_completion_qty < required_qty - 1e-12:
                            decision = "would_block_required_pair_completion"
                            add_stat(
                                stats,
                                "risk_seed_pair_completion_required_would_block_closeability_net_pair_cost",
                                closeability_net_pair_cost,
                            )
                            add_stat(
                                stats,
                                "risk_seed_pair_completion_required_would_block_pair_completion_qty",
                                pair_completion_qty,
                            )
                            if applies_by_fair_price:
                                add_stat(
                                    stats,
                                    "risk_seed_pair_completion_required_would_block_fair_price_pair_cost_after_fee",
                                    fair_price_pair_cost_after_fee,
                                )
                        else:
                            decision = "would_allow_required_pair_completion"
                            add_stat(
                                stats,
                                "risk_seed_pair_completion_required_would_allow_closeability_net_pair_cost",
                                closeability_net_pair_cost,
                            )
                            add_stat(
                                stats,
                                "risk_seed_pair_completion_required_would_allow_pair_completion_qty",
                                pair_completion_qty,
                            )
                            if applies_by_fair_price:
                                add_stat(
                                    stats,
                                    "risk_seed_pair_completion_required_would_allow_fair_price_pair_cost_after_fee",
                                    fair_price_pair_cost_after_fee,
                                )
                        risk_seed_pair_completion_required_counterfactual[decision] += 1
                        per_slug_pair_completion_required_counterfactual[slug][decision] += 1
                    pending = obj.get("risk_seed_pending_opp_credit")
                    if pending is not None:
                        candidate_pending_opp_credit_values[str(pending)] += 1

                if kind == "risk_seed_closeability_block":
                    risk_seed_closeability_block_reasons[reason or "<missing>"] += 1
                    val = first_number(
                        obj,
                        (
                            "closeability_net_pair_cost",
                            "risk_seed_closeability_net_pair_cost",
                            "net_pair_cost",
                        ),
                    )
                    add_stat(stats, "risk_seed_closeability_net_pair_cost", val)

                if kind == "cancel":
                    cancel_reasons[reason or "<missing>"] += 1
                    add_stat(stats, "cancel_closeability_original_net_pair_cost", obj.get("closeability_net_pair_cost"))
                    add_stat(stats, "cancel_closeability_current_comp_ask", obj.get("closeability_current_comp_ask"))
                    add_stat(stats, "cancel_closeability_current_net_pair_cost", obj.get("closeability_current_net_pair_cost"))
                    add_stat(stats, "cancel_closeability_net_cap", obj.get("risk_seed_cancel_on_closeability_net_cap"))

                if kind == "fair_price_admission_block":
                    fair_price_admission_block_reasons[reason or str(obj.get("fair_price_admission_block_reason") or "<missing>")] += 1
                    add_stat(stats, "fair_price_block_pair_cost_after_fee", obj.get("fair_price_pair_cost_after_fee"))
                    add_stat(stats, "fair_price_block_edge_after_fee", obj.get("fair_price_edge_after_fee"))
                    add_stat(stats, "fair_price_block_seconds_to_close", obj.get("fair_price_seconds_to_close"))

                if kind == "pair_completion_block":
                    pair_completion_block_reasons[reason or str(obj.get("pair_completion_decision") or "<missing>")] += 1
                    add_stat(stats, "pair_completion_block_avg_net_pair_cost", obj.get("pair_completion_avg_net_pair_cost"))
                    add_stat(stats, "pair_completion_block_worst_net_pair_cost", obj.get("pair_completion_worst_net_pair_cost"))
                    add_stat(stats, "pair_completion_block_pair_pnl_delta", obj.get("pair_completion_pair_pnl_delta"))
                    add_stat(stats, "pair_completion_block_projected_pair_pnl_after", obj.get("pair_completion_projected_pair_pnl_after"))

                if kind == "strict_rescue_block":
                    strict_rescue_block_reasons[reason or "<missing>"] += 1
                    per_slug_strict_reason_counts[slug][reason or "<missing>"] += 1
                    component = str(obj.get("block_component") or "<missing>")
                    strict_rescue_block_components[component] += 1
                    add_stat(stats, "strict_rescue_block_l1_age_ms", obj.get("strict_rescue_l1_age_ms"))
                    add_stat(stats, "strict_rescue_block_l2_age_ms", obj.get("strict_rescue_l2_age_ms"))
                    add_stat(stats, "strict_rescue_block_lot_age_ms", obj.get("lot_age_ms"))
                    add_stat(stats, "strict_rescue_block_lot_cost", obj.get("lot_cost"))
                    add_stat(stats, "strict_rescue_block_oldest_lot_cost", obj.get("oldest_lot_cost"))
                    add_stat(stats, "strict_rescue_block_raw_comp_ask", obj.get("raw_comp_ask"))
                    lot_age_ms = as_float(obj.get("lot_age_ms"))
                    salvage_age_ms = as_float(obj.get("salvage_age_ms"))
                    lot_cost = as_float(obj.get("lot_cost"))
                    salvage_min_lot_cost = as_float(obj.get("salvage_min_lot_cost"))
                    if reason == "strict_rescue_lot_age_or_min_cost":
                        age_ok = (
                            lot_age_ms is not None
                            and salvage_age_ms is not None
                            and lot_age_ms >= salvage_age_ms
                        )
                        cost_ok = (
                            lot_cost is not None
                            and salvage_min_lot_cost is not None
                            and lot_cost >= salvage_min_lot_cost
                        )
                        if age_ok and cost_ok:
                            strict_rescue_lot_age_min_cost_classes["thresholds_ok_but_blocked"] += 1
                        elif age_ok:
                            strict_rescue_lot_age_min_cost_classes["min_cost_only"] += 1
                        elif cost_ok:
                            strict_rescue_lot_age_min_cost_classes["age_only"] += 1
                        else:
                            strict_rescue_lot_age_min_cost_classes["age_and_min_cost"] += 1
                        if lot_age_ms is not None and salvage_age_ms is not None:
                            add_stat(stats, "strict_rescue_block_lot_age_ms_to_threshold", salvage_age_ms - lot_age_ms)
                        if lot_cost is not None and salvage_min_lot_cost is not None:
                            add_stat(stats, "strict_rescue_block_lot_cost_to_threshold", salvage_min_lot_cost - lot_cost)
                    val = first_number(
                        obj,
                        (
                            "net_pair_cost",
                            "projected_net_pair_cost",
                            "strict_rescue_net_pair_cost",
                            "pair_net_cost",
                        ),
                    )
                    add_stat(stats, "strict_rescue_block_net_pair_cost", val)

                if kind == "fak_salvage":
                    add_stat(stats, "strict_rescue_close_net_pair_cost", obj.get("net_pair_cost"))
                    add_stat(stats, "strict_rescue_close_pair_pnl_delta", obj.get("strict_rescue_pair_pnl_delta"))
                    add_stat(stats, "strict_rescue_close_projected_pair_pnl_after", obj.get("strict_rescue_projected_pair_pnl_after"))
                    add_stat(stats, "strict_rescue_close_l1_age_ms", obj.get("strict_rescue_l1_age_ms"))
                    add_stat(stats, "strict_rescue_close_l2_age_ms", obj.get("strict_rescue_l2_age_ms"))
                    skipped = obj.get("strict_rescue_skipped_low_cost_lots")
                    if skipped is not None:
                        strict_rescue_close_skipped_low_cost_lots[str(skipped)] += 1

                if kind == "surplus_budget_block":
                    add_stat(stats, "surplus_budget_projected_unpaired_cost", obj.get("surplus_budget_projected_unpaired_cost"))
                    add_stat(stats, "surplus_budget_allowed", obj.get("surplus_budget_allowed"))

    out: dict[str, Any] = {
        "artifact": "remote_event_diagnostics",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "root": str(root),
        "event_file_count": len(event_files),
        "raw_line_count": raw_line_count,
        "event_rows_scanned": raw_line_count - bad_json,
        "bad_json": bad_json,
        "event_counts": dict(event_counts.most_common()),
        "per_slug_event_counts": {slug: dict(counter.most_common()) for slug, counter in sorted(per_slug_event_counts.items())},
        "per_slug_reason_counts": {slug: dict(counter.most_common()) for slug, counter in sorted(per_slug_reason_counts.items())},
        "per_slug_strict_rescue_block_reasons": {
            slug: dict(counter.most_common()) for slug, counter in sorted(per_slug_strict_reason_counts.items())
        },
        "block_reason_counts": dict(block_reason_counts.most_common()),
        "kind_reason_counts": {
            kind: dict(counter.most_common()) for kind, counter in sorted(kind_reason_counts.items())
        },
        "accepted_source_total": source_total,
        "accepted_source_missing_any": source_missing,
        "candidate_pair_completion_decisions": dict(candidate_pair_completion_decisions.most_common()),
        "candidate_fair_price_admission_decisions": dict(candidate_fair_price_admission_decisions.most_common()),
        "candidate_fair_price_admission_modes": dict(candidate_fair_price_admission_modes.most_common()),
        "candidate_pending_opp_credit_values": dict(candidate_pending_opp_credit_values.most_common()),
        "candidate_soft_decisions": dict(candidate_soft_decisions.most_common()),
        "candidate_risk_increasing_seed_values": dict(candidate_risk_increasing_seed_values.most_common()),
        "risk_seed_pair_completion_required_counterfactual": {
            "enabled": (
                risk_seed_pair_completion_required_above_net_cap is not None
                or risk_seed_pair_completion_required_above_fair_price_pair_cost is not None
            ),
            "required_above_net_cap": risk_seed_pair_completion_required_above_net_cap,
            "required_above_fair_price_pair_cost": risk_seed_pair_completion_required_above_fair_price_pair_cost,
            "min_qty": risk_seed_pair_completion_min_qty,
            "counts": dict(risk_seed_pair_completion_required_counterfactual.most_common()),
        },
        "per_slug_pair_completion_required_counterfactual": {
            slug: dict(counter.most_common())
            for slug, counter in sorted(per_slug_pair_completion_required_counterfactual.items())
        },
        "fair_price_admission_block_reasons": dict(fair_price_admission_block_reasons.most_common()),
        "pair_completion_block_reasons": dict(pair_completion_block_reasons.most_common()),
        "risk_seed_closeability_block_reasons": dict(risk_seed_closeability_block_reasons.most_common()),
        "cancel_reasons": dict(cancel_reasons.most_common()),
        "strict_rescue_block_reasons": dict(strict_rescue_block_reasons.most_common()),
        "strict_rescue_block_components": dict(strict_rescue_block_components.most_common()),
        "strict_rescue_lot_age_min_cost_classes": dict(strict_rescue_lot_age_min_cost_classes.most_common()),
        "strict_rescue_close_skipped_low_cost_lots": dict(strict_rescue_close_skipped_low_cost_lots.most_common()),
    }
    for key, values in sorted(stats.items()):
        out[key] = summarize(values)
    return rounded(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--risk-seed-pair-completion-required-above-net-cap", type=float, default=None)
    parser.add_argument("--risk-seed-pair-completion-required-above-fair-price-pair-cost", type=float, default=None)
    parser.add_argument("--risk-seed-pair-completion-min-qty", type=float, default=0.0)
    args = parser.parse_args()
    root = Path(args.output_root).expanduser().resolve()
    out_path = Path(args.output_json).expanduser().resolve()
    result = scan(
        root,
        risk_seed_pair_completion_required_above_net_cap=args.risk_seed_pair_completion_required_above_net_cap,
        risk_seed_pair_completion_required_above_fair_price_pair_cost=args.risk_seed_pair_completion_required_above_fair_price_pair_cost,
        risk_seed_pair_completion_min_qty=args.risk_seed_pair_completion_min_qty,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
