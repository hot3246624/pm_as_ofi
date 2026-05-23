#!/usr/bin/env python3
"""Diagnose residual regime failure for xuan-frontier no-order runtime output.

This is local-only analysis. It reads compact runtime artifacts and emits a
scorecard plus a short markdown handoff describing whether the current blocker
is source/staleness/concurrency or economic admission/rescue mismatch.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def share(numer: float, denom: float) -> float:
    return numer / denom if denom > 0 else 0.0


def slug_rows(output_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(output_root.glob("*.summary.json")):
        summary = read_json(path)
        metrics = summary.get("metrics", {})
        filled_qty = as_float(metrics.get("filled_qty"))
        filled_cost = as_float(metrics.get("filled_cost"))
        residual_qty = as_float(metrics.get("residual_qty"))
        residual_cost = as_float(metrics.get("residual_cost"))
        rows.append(
            {
                "slug": summary.get("slug") or path.name.replace(".summary.json", ""),
                "accepted_actions": as_float(metrics.get("candidates")),
                "fills": as_float(metrics.get("queue_supported_fills")),
                "strict_rescue_closes": as_float(metrics.get("strict_rescue_actions")),
                "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
                "pair_pnl": as_float(metrics.get("pair_pnl")),
                "filled_qty": filled_qty,
                "filled_cost": filled_cost,
                "residual_qty": residual_qty,
                "residual_cost": residual_cost,
                "residual_qty_share": share(residual_qty, filled_qty),
                "residual_cost_share": share(residual_cost, filled_cost),
                "closeability_debt_max_open": as_float(metrics.get("closeability_debt_max_open")),
                "closeability_debt_open": as_float(metrics.get("closeability_debt_open")),
                "blocked": summary.get("blocked", {}),
                "top_residual_lots": summary.get("top_residual_lots", [])[:5],
            }
        )
    return rows


def classify(args: argparse.Namespace) -> dict[str, Any]:
    output_root = Path(args.output_root).expanduser().resolve()
    aggregate = read_json(output_root / "aggregate_report.json")
    runtime = read_json(Path(args.runtime_summary).expanduser().resolve())
    comparison = read_json(Path(args.comparison_scorecard).expanduser().resolve())
    repeat = read_json(Path(args.repeat_scorecard).expanduser().resolve())
    event_diag = read_json(Path(args.event_diagnostics).expanduser().resolve())
    concurrency = read_json(Path(args.concurrency_scorecard).expanduser().resolve()) if args.concurrency_scorecard else {}
    lifecycle = read_json(Path(args.lifecycle_scorecard).expanduser().resolve()) if args.lifecycle_scorecard else {}

    metrics = aggregate.get("metrics", {})
    strict_reasons = event_diag.get("strict_rescue_block_reasons", {})
    net_cap_blocks = as_float(strict_reasons.get("strict_rescue_net_pair_cap"))
    lot_age_blocks = as_float(strict_reasons.get("strict_rescue_lot_age_or_min_cost"))
    l1_age_blocks = as_float(strict_reasons.get("strict_rescue_l1_age"))
    total_rescue_blocks = net_cap_blocks + lot_age_blocks + l1_age_blocks
    net_cap_share = share(net_cap_blocks, total_rescue_blocks)
    l1_age_share = share(l1_age_blocks, total_rescue_blocks)
    residual_qty_share = share(as_float(metrics.get("residual_qty")), as_float(metrics.get("filled_qty")))
    residual_cost_share = share(as_float(metrics.get("residual_cost")), as_float(metrics.get("filled_cost")))

    rows = slug_rows(output_root)
    high_residual = [
        row
        for row in rows
        if row["filled_qty"] > 0
        and (row["residual_qty_share"] > args.max_residual_qty_share or row["residual_cost_share"] > args.max_residual_cost_share)
    ]
    high_residual.sort(key=lambda row: (row["residual_qty_share"], row["residual_cost_share"], row["residual_cost"]), reverse=True)

    candidate_closeability = event_diag.get("candidate_closeability_net_pair_cost", {})
    blocked_net_pair = event_diag.get("strict_rescue_block_net_pair_cost", {})
    source_clean = concurrency.get("status") in {"", None, "KEEP_NO_ORDER_CONCURRENT_SHARED_INGRESS_EVIDENCE_PASS_RESEARCH_ONLY"}
    lifecycle_clean = lifecycle.get("status") in {"", None, "KEEP_NO_ORDER_STRICT_RESCUE_LIFECYCLE_SCORER_PASS_RESEARCH_ONLY"}
    pair_pnl = as_float(metrics.get("pair_pnl"))
    residual_blocked = residual_qty_share > args.max_residual_qty_share or residual_cost_share > args.max_residual_cost_share
    pnl_blocked = pair_pnl < args.min_pair_pnl
    economics_dominant = net_cap_share >= args.min_net_cap_block_share and l1_age_share <= args.max_l1_age_block_share
    admission_rescue_gap = (
        as_float(candidate_closeability.get("p50")) > args.salvage_net_cap
        and as_float(candidate_closeability.get("p90")) <= args.soft_cap + 1e-12
        and as_float(blocked_net_pair.get("min")) > args.salvage_net_cap
    )

    hard_blockers: list[str] = []
    if not source_clean:
        hard_blockers.append("source_or_concurrency_not_clean")
    if not lifecycle_clean:
        hard_blockers.append("lifecycle_not_clean")
    if pnl_blocked:
        hard_blockers.append("pair_pnl_below_min")
    if residual_blocked:
        hard_blockers.append("residual_share_above_runtime_gate")
    if economics_dominant and (residual_blocked or pnl_blocked):
        hard_blockers.append("strict_rescue_net_pair_cap_dominant")
    if admission_rescue_gap:
        hard_blockers.append("soft_admission_above_strict_rescue_cap")

    status = (
        "UNKNOWN_RESIDUAL_REGIME_ECONOMIC_ADMISSION_RESCUE_MISMATCH"
        if residual_blocked and economics_dominant and admission_rescue_gap
        else "UNKNOWN_RESIDUAL_REGIME_REQUIRES_CONTROLLER_REVIEW"
        if hard_blockers
        else "KEEP_RESIDUAL_REGIME_DIAGNOSTIC_CLEAN_RESEARCH_ONLY"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "inputs": {
            "output_root": str(output_root),
            "runtime_summary": str(Path(args.runtime_summary).expanduser().resolve()),
            "comparison_scorecard": str(Path(args.comparison_scorecard).expanduser().resolve()),
            "repeat_scorecard": str(Path(args.repeat_scorecard).expanduser().resolve()),
            "event_diagnostics": str(Path(args.event_diagnostics).expanduser().resolve()),
            "concurrency_scorecard": str(Path(args.concurrency_scorecard).expanduser().resolve()) if args.concurrency_scorecard else None,
            "lifecycle_scorecard": str(Path(args.lifecycle_scorecard).expanduser().resolve()) if args.lifecycle_scorecard else None,
        },
        "metrics": {
            "accepted_actions": as_float(metrics.get("candidates")),
            "fills": as_float(metrics.get("queue_supported_fills")),
            "strict_rescue_closes": as_float(metrics.get("strict_rescue_actions")),
            "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
            "pair_pnl": pair_pnl,
            "filled_qty": as_float(metrics.get("filled_qty")),
            "filled_cost": as_float(metrics.get("filled_cost")),
            "residual_qty": as_float(metrics.get("residual_qty")),
            "residual_cost": as_float(metrics.get("residual_cost")),
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "closeability_debt_max_open": as_float(metrics.get("closeability_debt_max_open")),
            "closeability_debt_open": as_float(metrics.get("closeability_debt_open")),
            "surplus_budget_blocks": as_float(metrics.get("surplus_budget_blocks")),
        },
        "strict_rescue_block_diagnostics": {
            "reason_counts": strict_reasons,
            "total": total_rescue_blocks,
            "net_pair_cap_share": net_cap_share,
            "l1_age_share": l1_age_share,
            "net_pair_cost_stats": blocked_net_pair,
            "l1_age_ms_stats": event_diag.get("strict_rescue_block_l1_age_ms", {}),
        },
        "admission_closeability": {
            "candidate_closeability_net_pair_cost": candidate_closeability,
            "risk_seed_closeability_block_reasons": event_diag.get("risk_seed_closeability_block_reasons", {}),
            "soft_cap": args.soft_cap,
            "strict_rescue_salvage_net_cap": args.salvage_net_cap,
            "target_rescue_net_cap": args.target_rescue_net_cap,
        },
        "per_slug": {
            "slug_count": len(rows),
            "high_residual_slug_count": len(high_residual),
            "worst_residual_slugs": high_residual[:8],
        },
        "upstream_score_status": {
            "runtime_summary": runtime.get("status"),
            "comparison": comparison.get("status"),
            "repeat_window": repeat.get("status"),
            "concurrency": concurrency.get("status"),
            "lifecycle": lifecycle.get("status"),
        },
        "recommendation": {
            "rerun_same_profile_blind": not hard_blockers,
            "target_rescue_net_cap": args.target_rescue_net_cap,
            "recommended_probe_salvage_net_cap": args.salvage_net_cap,
            "strict_rescue_net_pair_cap_dominant": economics_dominant,
            "next_profile": {
                "activation_mode": "opp_seen",
                "activation_window_s": 15,
                "late_repair_after_s": 90,
                "imbalance_qty_cap": 1.25,
                "soft_cap": args.soft_cap,
                "debt_floor": args.target_rescue_net_cap,
                "debt_budget": 1.0,
                "salvage_net_cap": args.salvage_net_cap,
            },
            "rationale": (
                "Source/concurrency/lifecycle checks are clean; residual is concentrated in windows where "
                "risk-increasing seed admission accepts closeability costs around the soft cap while strict "
                "rescue remains capped by the runtime salvage cap. Treat target_rescue_net_cap as the quality "
                "objective and salvage_net_cap as the bounded runtime floor; positive PnL and controlled "
                "residual are the pass/fail bottom line for this diagnostic."
            ),
        },
        "decision": {
            "deployable": False,
            "remote_runner_allowed": False,
            "shadow_review_ready": False,
        },
    }


def render_markdown(card: dict[str, Any]) -> str:
    metrics = card["metrics"]
    diag = card["strict_rescue_block_diagnostics"]
    rec = card["recommendation"]["next_profile"]
    lines = [
        "# Residual Regime Diagnostic",
        "",
        f"- status: `{card['status']}`",
        f"- hard_blockers: `{', '.join(card['hard_blockers']) or 'none'}`",
        f"- accepted_actions: `{metrics['accepted_actions']}`",
        f"- fills: `{metrics['fills']}`",
        f"- strict_rescue_closes: `{metrics['strict_rescue_closes']}`",
        f"- pair_pnl: `{round(metrics['pair_pnl'], 6)}`",
        f"- residual_qty_share: `{round(metrics['residual_qty_share'], 6)}`",
        f"- residual_cost_share: `{round(metrics['residual_cost_share'], 6)}`",
        "",
        "## Diagnosis",
        f"- strict_rescue_net_pair_cap_share: `{round(diag['net_pair_cap_share'], 6)}`",
        f"- strict_rescue_l1_age_share: `{round(diag['l1_age_share'], 6)}`",
        f"- blocked_net_pair_cost_stats: `{diag['net_pair_cost_stats']}`",
        f"- candidate_closeability_net_pair_cost: `{card['admission_closeability']['candidate_closeability_net_pair_cost']}`",
        "",
        "## Next Profile",
    ]
    for key, value in rec.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "This is local analysis only. It does not authorize remote execution, deployment, restart, or live trading.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--runtime-summary", required=True)
    parser.add_argument("--comparison-scorecard", required=True)
    parser.add_argument("--repeat-scorecard", required=True)
    parser.add_argument("--event-diagnostics", required=True)
    parser.add_argument("--concurrency-scorecard")
    parser.add_argument("--lifecycle-scorecard")
    parser.add_argument("--soft-cap", type=float, default=0.98)
    parser.add_argument("--salvage-net-cap", type=float, default=0.95)
    parser.add_argument("--target-rescue-net-cap", type=float, default=0.95)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--min-net-cap-block-share", type=float, default=0.50)
    parser.add_argument("--max-l1-age-block-share", type=float, default=0.01)
    parser.add_argument("--markdown", required=True)
    parser.add_argument("--scorecard-json", required=True)
    args = parser.parse_args()

    started = time.time()
    result = classify(args)
    scorecard = {
        "artifact": "xuan_no_order_residual_regime_diagnostic",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_residual_regime_diagnostic.py",
        **result,
    }
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    markdown_path = Path(args.markdown).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    markdown_path.write_text(render_markdown(rounded(scorecard)) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
