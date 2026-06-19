#!/usr/bin/env python3
"""Plan diagnostic-only ce25 pair-cost admission cap ladders.

This tool is intentionally local/read-only. It scans a no-order runtime event
root and estimates how many previously blocked fair-price admission rows would
become eligible at candidate pair-cost caps. The result is a diagnostic plan,
not replay evidence and not shadow-promotion evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def fnum(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def parse_float_csv(raw: str) -> list[float]:
    out: list[float] = []
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            val = float(item)
        except ValueError as exc:
            raise SystemExit(f"invalid float in list {raw!r}: {item!r}") from exc
        if not math.isfinite(val):
            raise SystemExit(f"non-finite float in list {raw!r}: {item!r}")
        out.append(val)
    return sorted(dict.fromkeys(out))


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
        "count": len(vals),
        "min": vals[0],
        "p50": percentile(vals, 0.50),
        "p90": percentile(vals, 0.90),
        "p99": percentile(vals, 0.99),
        "max": vals[-1],
    }


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def scan_events(root: Path) -> dict[str, Any]:
    event_files = sorted(root.glob("*.events.jsonl"))
    event_counts: Counter[str] = Counter()
    fair_block_costs: list[float] = []
    candidate_costs: list[float] = []
    cancel_current_costs: list[float] = []
    cancel_original_costs: list[float] = []
    fair_block_reason_counts: Counter[str] = Counter()
    cancel_reasons: Counter[str] = Counter()
    source_quality_reasons: Counter[str] = Counter()
    per_slug_fair_blocks: dict[str, Counter[str]] = defaultdict(Counter)
    bad_json = 0
    raw_line_count = 0

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
                if kind == "fair_price_admission_block":
                    reason = str(
                        obj.get("fair_price_admission_block_reason")
                        or obj.get("block_reason")
                        or "<missing>"
                    )
                    fair_block_reason_counts[reason] += 1
                    per_slug_fair_blocks[slug][reason] += 1
                    cost = fnum(obj.get("fair_price_pair_cost_after_fee"))
                    if cost is not None:
                        fair_block_costs.append(cost)
                elif kind == "candidate":
                    cost = fnum(obj.get("fair_price_pair_cost_after_fee"))
                    if cost is not None:
                        candidate_costs.append(cost)
                elif kind == "cancel":
                    cancel_reasons[str(obj.get("reason") or "<missing>")] += 1
                    current = fnum(obj.get("closeability_current_net_pair_cost"))
                    original = fnum(obj.get("closeability_net_pair_cost"))
                    if current is not None:
                        cancel_current_costs.append(current)
                    if original is not None:
                        cancel_original_costs.append(original)
                elif kind == "source_quality_block":
                    source_quality_reasons[str(obj.get("source_quality_block_reason") or "<missing>")] += 1

    return {
        "event_file_count": len(event_files),
        "raw_line_count": raw_line_count,
        "bad_json": bad_json,
        "event_counts": dict(event_counts.most_common()),
        "fair_price_admission_block_reasons": dict(fair_block_reason_counts.most_common()),
        "source_quality_block_reasons": dict(source_quality_reasons.most_common()),
        "cancel_reasons": dict(cancel_reasons.most_common()),
        "per_slug_fair_price_admission_block_reasons": {
            slug: dict(counter.most_common())
            for slug, counter in sorted(per_slug_fair_blocks.items())
        },
        "fair_block_costs": fair_block_costs,
        "candidate_costs": candidate_costs,
        "cancel_current_costs": cancel_current_costs,
        "cancel_original_costs": cancel_original_costs,
    }


def cap_row(
    *,
    cap: float,
    current_cap: float,
    promotion_cap: float,
    fair_block_costs: list[float],
    candidate_count: int,
    min_new_rows: int,
) -> dict[str, Any]:
    newly_eligible = [
        cost
        for cost in fair_block_costs
        if cost > current_cap + 1e-12 and cost <= cap + 1e-12
    ]
    cumulative_eligible = [
        cost
        for cost in fair_block_costs
        if cost <= cap + 1e-12
    ]
    diagnostic_only = cap > promotion_cap + 1e-12
    return {
        "cap": cap,
        "newly_eligible_block_rows": len(newly_eligible),
        "cumulative_eligible_block_rows": len(cumulative_eligible),
        "estimated_candidate_rows_including_current": candidate_count + len(cumulative_eligible),
        "newly_eligible_cost_after_fee": summarize(newly_eligible),
        "diagnostic_only": diagnostic_only,
        "promotion_evidence_allowed": not diagnostic_only,
        "meets_min_new_rows": len(newly_eligible) >= min_new_rows,
        "interpretation": (
            "diagnostic scale probe only; cannot be used as shadow-promotion evidence"
            if diagnostic_only
            else "within promotion cap boundary, but still requires fills/PnL/rescue evidence"
        ),
    }


def choose_next_cap(rows: list[dict[str, Any]], max_next_cap: float) -> float | None:
    for row in rows:
        cap = fnum(row.get("cap"))
        if cap is None:
            continue
        if cap <= max_next_cap + 1e-12 and row.get("meets_min_new_rows"):
            return cap
    return None


def render_markdown(card: dict[str, Any]) -> str:
    rec = card["recommendation"]
    lines = [
        "# CE25 Admission Ladder Diagnostic Plan",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- next_diagnostic_cap: `{rec.get('next_diagnostic_cap')}`",
        f"- promotion_cap: `{card['policy']['promotion_cap']}`",
        f"- cancel_guard_cap: `{card['policy']['cancel_guard_cap']}`",
        f"- remote_runner_allowed: `{card['decision']['remote_runner_allowed']}`",
        f"- deployable: `{card['decision']['deployable']}`",
        "",
        "## Current Window",
        "",
    ]
    for key, value in card["current_window"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Cap Ladder", ""])
    for row in card["cap_ladder"]:
        lines.extend(
            [
                f"### cap {row['cap']}",
                "",
                f"- newly_eligible_block_rows: `{row['newly_eligible_block_rows']}`",
                f"- estimated_candidate_rows_including_current: `{row['estimated_candidate_rows_including_current']}`",
                f"- diagnostic_only: `{row['diagnostic_only']}`",
                f"- promotion_evidence_allowed: `{row['promotion_evidence_allowed']}`",
                f"- interpretation: {row['interpretation']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Guardrails",
            "",
            "- This is a local diagnostic plan only.",
            "- It does not authorize remote execution.",
            "- Any future remote run remains PM_DRY_RUN=1/no-order, 1800s max, timeout 2100s.",
            "- Caps above the promotion cap are scale diagnostics, not promotion evidence.",
            "- Keep source gates, closeability cancel guard, and strict rescue economics enabled.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    scan = scan_events(root)
    candidate_count = len(scan["candidate_costs"])
    fair_block_costs = scan["fair_block_costs"]
    caps = parse_float_csv(args.candidate_caps)
    rows = [
        cap_row(
            cap=cap,
            current_cap=args.current_cap,
            promotion_cap=args.promotion_cap,
            fair_block_costs=fair_block_costs,
            candidate_count=candidate_count,
            min_new_rows=args.min_new_rows,
        )
        for cap in caps
    ]
    next_cap = choose_next_cap(rows, args.max_next_diagnostic_cap)
    hard_blockers: list[str] = []
    if scan["bad_json"]:
        hard_blockers.append("bad_event_json_present")
    if not fair_block_costs:
        hard_blockers.append("no_fair_price_block_costs")
    if next_cap is None:
        hard_blockers.append("no_candidate_cap_meets_min_new_rows_within_max_next_cap")

    status = (
        "KEEP_CE25_ADMISSION_LADDER_DIAGNOSTIC_PLAN_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_CE25_ADMISSION_LADDER_DIAGNOSTIC_PLAN_BLOCKED"
    )
    return {
        "artifact": "xuan_ce25_admission_ladder_diagnostic_planner",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_ce25_admission_ladder_diagnostic_planner.py",
        "status": status,
        "root": str(root),
        "policy": {
            "current_cap": args.current_cap,
            "promotion_cap": args.promotion_cap,
            "max_next_diagnostic_cap": args.max_next_diagnostic_cap,
            "cancel_guard_cap": args.cancel_guard_cap,
            "strict_rescue_salvage_net_cap": args.strict_rescue_salvage_net_cap,
            "duration_s_max": args.duration_s_max,
            "timeout_s": args.timeout_s,
            "min_new_rows": args.min_new_rows,
        },
        "current_window": {
            "event_file_count": scan["event_file_count"],
            "event_rows": scan["raw_line_count"] - scan["bad_json"],
            "candidate_rows": candidate_count,
            "fair_price_admission_block_rows": len(fair_block_costs),
            "cancel_reasons": scan["cancel_reasons"],
            "source_quality_block_reasons": scan["source_quality_block_reasons"],
            "candidate_cost_after_fee": summarize(scan["candidate_costs"]),
            "fair_price_block_cost_after_fee": summarize(fair_block_costs),
            "cancel_original_net_pair_cost": summarize(scan["cancel_original_costs"]),
            "cancel_current_net_pair_cost": summarize(scan["cancel_current_costs"]),
        },
        "cap_ladder": rows,
        "recommendation": {
            "next_diagnostic_cap": next_cap,
            "next_profile": {
                "fair_price_max_pair_cost": next_cap,
                "risk_seed_cancel_on_closeability_net_cap": args.cancel_guard_cap,
                "strict_rescue_salvage_net_cap": args.strict_rescue_salvage_net_cap,
                "pm_dry_run": True,
                "duration_s": args.duration_s_max,
                "timeout_s": args.timeout_s,
            }
            if next_cap is not None
            else None,
            "why": (
                "Use the smallest cap that recovers a non-trivial number of blocked rows "
                "while preserving the 0.98 closeability cancel guard. This isolates scale "
                "sensitivity without claiming promotion evidence."
            ),
        },
        "limitations": [
            "Upper-bound pass counts are from blocked event rows; a real run changes inventory, cooldowns, queue fills, and rescue opportunities.",
            "Caps above promotion_cap are diagnostic only and cannot make the strategy shadow-ready by themselves.",
            "A future run still needs clean safety, source linkage, fills, positive fee-aware PnL, and rescue/residual gates.",
        ],
        "decision": {
            "plan_ready": not hard_blockers,
            "remote_runner_allowed": False,
            "requires_new_heartbeat_or_explicit_authorization": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-out")
    parser.add_argument("--current-cap", type=float, default=0.98)
    parser.add_argument("--promotion-cap", type=float, default=0.98)
    parser.add_argument("--max-next-diagnostic-cap", type=float, default=0.985)
    parser.add_argument("--cancel-guard-cap", type=float, default=0.98)
    parser.add_argument("--strict-rescue-salvage-net-cap", type=float, default=0.95)
    parser.add_argument("--duration-s-max", type=int, default=1800)
    parser.add_argument("--timeout-s", type=int, default=2100)
    parser.add_argument("--min-new-rows", type=int, default=20)
    parser.add_argument("--candidate-caps", default="0.98,0.985,0.99,0.995,1.0")
    args = parser.parse_args()

    card = rounded(build(args))
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_out:
        md = Path(args.markdown_out).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(render_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
