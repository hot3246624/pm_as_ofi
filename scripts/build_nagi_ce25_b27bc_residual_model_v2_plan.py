#!/usr/bin/env python3
"""Build a local-only residual model v2 plan from maker-shadow artifacts.

The plan consumes an existing residual guard grid and a best-candidate event
log.  It ranks next model families and writes a review packet.  It never fetches
data, imports credentials, connects to Polymarket, or executes orders.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


STATUS = "KEEP_NAGI_CE25_B27BC_RESIDUAL_MODEL_V2_PLAN_REVIEWED_LOCAL_ONLY_NOT_READY"


def to_float(raw: Any, default: float = 0.0) -> float:
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def event_profile(path: Path) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    risk_counts: Counter[str] = Counter()
    first_side_counts: Counter[str] = Counter()
    gate_counts: Counter[str] = Counter()
    gate_event_counts: Counter[tuple[str, str]] = Counter()
    if not path.exists():
        return {
            "event_counts": {},
            "risk_counts": {},
            "first_side_counts": {},
            "gate_counts": {},
            "gate_event_counts": {},
        }
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            event = str(row.get("event") or "unknown")
            gate = str(row.get("coverage_gate_id") or "unknown")
            counts[event] += 1
            gate_counts[gate] += 1
            gate_event_counts[(gate, event)] += 1
            if event in {"residual_discount", "residual_finalized"}:
                risk_counts[str(row.get("risk_flag") or "unknown")] += 1
                first_side_counts[str(row.get("first_side") or "unknown")] += 1
    return {
        "event_counts": dict(counts),
        "risk_counts": dict(risk_counts),
        "first_side_counts": dict(first_side_counts),
        "gate_counts": dict(gate_counts),
        "gate_event_counts": {
            f"{gate}|{event}": count
            for (gate, event), count in sorted(gate_event_counts.items())
        },
    }


def summarize_grid(rows: list[dict[str, str]]) -> dict[str, Any]:
    pass_count = sum(row.get("passes_gate") == "True" for row in rows)
    positive_rows = [row for row in rows if to_float(row.get("roi_proxy_conservative"), -1.0) > 0.0]
    residual_ok_rows = [row for row in rows if to_float(row.get("resid_rate"), 1.0) <= 0.12]
    meaningful_rows = [
        row
        for row in rows
        if to_float(row.get("queue_proxy_opens")) >= 30.0
        and to_float(row.get("observed_markets")) >= 30.0
    ]
    best_by_roi = max(rows, key=lambda row: to_float(row.get("roi_proxy_conservative"), -999.0))
    best_by_residual = min(rows, key=lambda row: to_float(row.get("resid_rate"), 999.0))
    return {
        "variant_count": len(rows),
        "pass_count": pass_count,
        "positive_variant_count": len(positive_rows),
        "residual_ok_variant_count": len(residual_ok_rows),
        "meaningful_variant_count": len(meaningful_rows),
        "best_by_roi": best_by_roi,
        "best_by_residual": best_by_residual,
    }


def rank_model_families(grid: dict[str, Any], events: dict[str, Any]) -> list[dict[str, Any]]:
    best_resid = to_float(grid["best_by_residual"].get("resid_rate"), 1.0)
    best_roi = to_float(grid["best_by_roi"].get("roi_proxy_conservative"), -1.0)
    event_counts = events["event_counts"]
    risk_counts = events["risk_counts"]
    active_skips = int(event_counts.get("open_skipped_active_residual", 0))
    quarantine_rejects = int(event_counts.get("open_rejected_residual_quarantine", 0))
    pair_rejects = int(event_counts.get("completion_rejected_pair_cost", 0))
    unsupported_repairs = int(event_counts.get("completion_queue_proxy_not_supported", 0))
    residual_events = sum(risk_counts.values())
    up_risk = int(risk_counts.get("up_first_down_residual", 0))
    unknown_risk = int(risk_counts.get("unknown", 0))

    rows = [
        {
            "model_family": "pre_open_opposite_side_support_score",
            "priority": 1,
            "why": (
                "Simple post-open guard left best residual above target; "
                f"best_resid_rate={best_resid:.6f}. Need score before opening."
            ),
            "evidence": (
                f"completion_rejected_pair_cost={pair_rejects}; "
                f"completion_queue_proxy_not_supported={unsupported_repairs}; "
                "repair supply must be known before first leg."
            ),
            "next_experiment": (
                "For each candidate open, require recent opposite-side eligible touch/depth "
                "within the same condition before allowing queue_proxy_open."
            ),
        },
        {
            "model_family": "per_market_residual_budget",
            "priority": 2,
            "why": "Residual is market-local and repeated; quarantine helped but did not meet target.",
            "evidence": (
                f"open_skipped_active_residual={active_skips}; "
                f"open_rejected_residual_quarantine={quarantine_rejects}."
            ),
            "next_experiment": (
                "Track per-market residual_cost budget and disable additional opens when "
                "projected residual exceeds budget, not only after discount."
            ),
        },
        {
            "model_family": "first_side_hazard_score",
            "priority": 3,
            "why": "YES/UP-first risk remains visible but side-only filters were insufficient.",
            "evidence": (
                f"up_first_down_residual={up_risk}; other_or_unknown_residual={unknown_risk}; "
                f"residual_events={residual_events}."
            ),
            "next_experiment": (
                "Estimate side hazard from recent completion support and market state; "
                "do not use a blanket YES suppressor as the primary model."
            ),
        },
        {
            "model_family": "private_maker_telemetry_gate",
            "priority": 4,
            "why": "Public account BUY proxy cannot prove our maker fills or queue priority.",
            "evidence": (
                f"Best public-proxy ROI={best_roi:.6f}, but residual remains above target "
                "and maker truth is absent."
            ),
            "next_experiment": (
                "Require own maker telemetry before any claim above research_proxy; "
                "keep order execution out of this automation."
            ),
        },
    ]
    return rows


def write_report(path: Path, decision: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    latest = decision["grid_summary"]
    best_roi = latest["best_by_roi"]
    best_resid = latest["best_by_residual"]
    lines = [
        "# NAGI/CE25/B27BC Residual Model V2 Plan",
        "",
        f"Status: `{decision['status']}`",
        "",
        "This is a local-only no-order research plan. It does not claim maker fills, private truth, readiness, canary, or live status.",
        "",
        "## Why Simple Guards Failed",
        "",
        f"- Grid pass count: `{latest['pass_count']}` / `{latest['variant_count']}`.",
        f"- Best ROI variant: `{best_roi.get('variant_id')}`, ROI `{best_roi.get('roi_proxy_conservative')}`, residual `{best_roi.get('resid_rate')}`.",
        f"- Best residual variant: `{best_resid.get('variant_id')}`, ROI `{best_resid.get('roi_proxy_conservative')}`, residual `{best_resid.get('resid_rate')}`.",
        "- Residual remains above the 12% target even after timeout/discount/paircap/quarantine/side/coverage ablations.",
        "",
        "## Ranked Model Families",
        "",
    ]
    for row in rows:
        lines.append(
            f"{row['priority']}. `{row['model_family']}`: {row['why']} Evidence: {row['evidence']}"
        )
    lines.extend(["", f"Next executable action: `{decision['next_executable_action']}`", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--grid-results-csv", required=True)
    p.add_argument("--best-run-dir", required=True)
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    grid_rows = load_csv(Path(args.grid_results_csv))
    grid_summary = summarize_grid(grid_rows)
    best_run = Path(args.best_run_dir)
    best_decision = load_json(best_run / "decision_register.json")
    events = event_profile(best_run / "nagi_ce25_b27bc_maker_shadow_events.jsonl")
    model_rows = rank_model_families(grid_summary, events)
    decision = {
        "status": STATUS,
        "evidence_level": "research_proxy",
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "grid_results_csv": str(Path(args.grid_results_csv).resolve()),
        "best_run_dir": str(best_run.resolve()),
        "best_run_status": best_decision.get("status"),
        "grid_summary": grid_summary,
        "best_run_event_profile": events,
        "model_family_rank": model_rows,
        "next_executable_action": (
            "implement_pre_open_opposite_side_support_grid_local_only"
        ),
    }
    out = Path(args.output_dir)
    write_json(out / "decision_register.json", decision)
    write_csv(out / "model_family_rank.csv", model_rows)
    write_report(out / "RESIDUAL_MODEL_V2_PLAN.md", decision, model_rows)
    print(
        json.dumps(
            {
                "status": STATUS,
                "output_dir": str(out),
                "top_model_family": model_rows[0]["model_family"],
                "next_executable_action": decision["next_executable_action"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
