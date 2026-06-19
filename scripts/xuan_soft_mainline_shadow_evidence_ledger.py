#!/usr/bin/env python3
"""Build a concise soft-mainline shadow evidence ledger.

This local-only helper explains how the current windows are being used:
qualified tradeable evidence, abstention holdout evidence, or repeat/reference
support. It does not rescore strategy economics, launch remote jobs, or approve
shadow.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    num = fnum(value)
    return int(num) if num is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def rows_from_regime(regime: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for window in regime.get("windows") or []:
        if not isinstance(window, dict):
            continue
        observed = body(window, "observed")
        classification = body(window, "classification")
        tradeable = bool(classification.get("tradeable_under_policy"))
        rows.append(
            {
                "label": window.get("label"),
                "role": "qualified_tradeable_window" if tradeable else "abstention_holdout",
                "regime": classification.get("regime"),
                "tags": classification.get("tags") or [],
                "tradeable_under_policy": tradeable,
                "prefix_pass": bool(window.get("prefix_pass")),
                "earliest_prefix_pass_minutes": window.get("earliest_prefix_pass_minutes"),
                "candidate_rows": inum(observed.get("candidate_rows")),
                "touch_rows": inum(observed.get("touch_rows")),
                "accepted_actions": inum(observed.get("accepted_actions")),
                "queue_supported_fills": inum(observed.get("queue_supported_fills")),
                "strict_rescue_closes": inum(observed.get("strict_rescue_closes")),
                "fill_rate": fnum(observed.get("fill_rate")),
                "rescue_per_candidate": fnum(observed.get("rescue_per_candidate")),
                "rescue_per_fill": fnum(observed.get("rescue_per_fill")),
                "pair_pnl": fnum(observed.get("pair_pnl")),
                "roi_on_filled_cost": fnum(observed.get("roi_on_filled_cost")),
                "residual_qty_share": fnum(observed.get("residual_qty_share")),
                "residual_cost_share": fnum(observed.get("residual_cost_share")),
                "rescue_net_pair_cost_max": fnum(observed.get("rescue_net_pair_cost_max")),
                "source_blocks": inum(observed.get("source_blocks")),
            }
        )
    return rows


def reference_row_from_repeat_gap(repeat_gap: dict[str, Any]) -> dict[str, Any] | None:
    ref = body(repeat_gap, "reference_window")
    sample = body(ref, "sample")
    econ = body(ref, "economics")
    if not sample:
        return None
    return {
        "label": ref.get("instance_id") or "repeat_reference_window",
        "role": "repeat_reference_pass_window",
        "regime": "historical_soft_closeability_pass",
        "tags": [],
        "tradeable_under_policy": True,
        "prefix_pass": None,
        "earliest_prefix_pass_minutes": None,
        "candidate_rows": None,
        "touch_rows": None,
        "accepted_actions": inum(sample.get("accepted_actions")),
        "queue_supported_fills": inum(sample.get("queue_supported_fills")),
        "strict_rescue_closes": inum(sample.get("strict_rescue_closes")),
        "fill_rate": None,
        "rescue_per_candidate": None,
        "rescue_per_fill": None,
        "pair_pnl": fnum(econ.get("pair_pnl")),
        "roi_on_filled_cost": fnum(econ.get("roi_on_filled_cost")),
        "residual_qty_share": fnum(econ.get("residual_qty_share")),
        "residual_cost_share": fnum(econ.get("residual_cost_share")),
        "rescue_net_pair_cost_max": fnum(econ.get("rescue_net_pair_cost_max")),
        "source_blocks": None,
    }


def write_csv(path: str | None, rows: list[dict[str, Any]]) -> None:
    if not path:
        return
    out = Path(path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "label",
        "role",
        "regime",
        "tradeable_under_policy",
        "prefix_pass",
        "earliest_prefix_pass_minutes",
        "accepted_actions",
        "queue_supported_fills",
        "strict_rescue_closes",
        "pair_pnl",
        "residual_qty_share",
        "residual_cost_share",
        "rescue_net_pair_cost_max",
        "fill_rate",
        "rescue_per_candidate",
        "rescue_per_fill",
        "source_blocks",
        "tags",
    ]
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            record = {key: row.get(key) for key in fields}
            record["tags"] = ",".join(str(item) for item in row.get("tags") or [])
            writer.writerow(record)


def build_markdown(card: dict[str, Any]) -> str:
    summary = card["summary"]
    decision = card["decision"]
    review = card.get("review_surface") or {}
    lines = [
        "# Soft-Mainline Shadow Evidence Ledger",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Summary",
        "",
        f"- Qualified tradeable windows: `{summary['qualified_tradeable_windows']}`.",
        f"- Active abstention holdouts: `{summary['active_abstention_holdouts']}`.",
        f"- Repeat reference pass windows listed: `{summary['repeat_reference_windows']}`.",
        f"- Missing qualified windows for shadow: `{summary['missing_qualified_tradeable_windows']}`.",
        f"- Remaining clean strict rescues from repeat gap: `{summary['repeat_remaining_strict_rescue_closes']}`.",
        f"- Review surface clean: `{review.get('clean')}`.",
        f"- Public benchmark status: `{review.get('public_benchmark_comparison')}`.",
        f"- Young residual status: `{review.get('young_tiny_residual')}`.",
        "",
        "## Rows",
        "",
    ]
    for row in card["rows"]:
        lines.append(
            f"- `{row['label']}` `{row['role']}`: accepted `{row['accepted_actions']}`, "
            f"fills `{row['queue_supported_fills']}`, rescues `{row['strict_rescue_closes']}`, "
            f"pair_pnl `{row['pair_pnl']}`, residual_cost_share `{row['residual_cost_share']}`"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This ledger is explanation and audit evidence only.",
            "- It does not launch remote jobs or approve shadow.",
            "- A weak active window stays in the ledger as abstention evidence.",
            "- A historical strong window stays visible but cannot shortcut fresh repeat-window coverage.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    regime = load_json(args.regime_generalization_scorecard)
    shadow_gate = load_json(args.shadow_promotion_gate_scorecard)
    shadow_gap = load_json(args.shadow_promotion_gap_scorecard)
    run_trigger = load_json(args.run_trigger_policy_scorecard)
    repeat_gap = load_json(args.repeat_gap_scorecard)
    public_benchmark = load_json(args.public_benchmark_comparison_scorecard)
    young_residual = load_json(args.young_tiny_residual_scorecard)

    rows = rows_from_regime(regime)
    reference = reference_row_from_repeat_gap(repeat_gap)
    if reference:
        rows.insert(0, reference)

    regime_summary = body(regime, "summary")
    shadow_gap_summary = body(shadow_gap, "gap_summary")
    gap_decision = body(shadow_gap, "decision")
    run_decision = body(run_trigger, "decision")
    qualified = inum(regime_summary.get("qualified_tradeable_count"))
    active_abstain = inum(regime_summary.get("active_abstain_count"))
    repeat_refs = sum(1 for row in rows if row.get("role") == "repeat_reference_pass_window")
    missing_qualified = inum(shadow_gap_summary.get("missing_qualified_tradeable_windows"))

    optional_review_cards = [card for card in (public_benchmark, young_residual) if card]
    review_surface_clean = all(is_keep(card) for card in optional_review_cards)
    if is_keep(shadow_gate) and review_surface_clean:
        ledger_status = "KEEP_SOFT_MAINLINE_SHADOW_EVIDENCE_LEDGER_SHADOW_GATE_CLEAR_LOCAL_ONLY"
    elif is_keep(shadow_gate):
        ledger_status = "UNKNOWN_SOFT_MAINLINE_SHADOW_EVIDENCE_LEDGER_REVIEW_SURFACE_CAVEAT_LOCAL_ONLY"
    elif status(shadow_gap).startswith("BLOCKED"):
        ledger_status = "BLOCKED_SOFT_MAINLINE_SHADOW_EVIDENCE_LEDGER_WAIT_FOR_FRESH_DENSITY_LOCAL_ONLY"
    else:
        ledger_status = "UNKNOWN_SOFT_MAINLINE_SHADOW_EVIDENCE_LEDGER_SAMPLE_OR_REPEAT_GAP_LOCAL_ONLY"

    next_action = gap_decision.get("next_action") or run_decision.get("next_action") or "wait_for_fresh_density_signal"

    return rounded(
        {
            "artifact": "xuan_soft_mainline_shadow_evidence_ledger",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_soft_mainline_shadow_evidence_ledger.py",
            "status": ledger_status,
            "inputs": {
                "regime_generalization_scorecard": str(Path(args.regime_generalization_scorecard).expanduser()),
                "shadow_promotion_gate_scorecard": str(Path(args.shadow_promotion_gate_scorecard).expanduser()),
                "shadow_promotion_gap_scorecard": str(Path(args.shadow_promotion_gap_scorecard).expanduser()),
                "run_trigger_policy_scorecard": str(Path(args.run_trigger_policy_scorecard).expanduser()),
                "repeat_gap_scorecard": str(Path(args.repeat_gap_scorecard).expanduser()),
                "public_benchmark_comparison_scorecard": (
                    str(Path(args.public_benchmark_comparison_scorecard).expanduser())
                    if args.public_benchmark_comparison_scorecard
                    else None
                ),
                "young_tiny_residual_scorecard": (
                    str(Path(args.young_tiny_residual_scorecard).expanduser())
                    if args.young_tiny_residual_scorecard
                    else None
                ),
            },
            "source_statuses": {
                "regime_generalization": status(regime) or None,
                "shadow_promotion_gate": status(shadow_gate) or None,
                "shadow_promotion_gap": status(shadow_gap) or None,
                "run_trigger_policy": status(run_trigger) or None,
                "repeat_gap": status(repeat_gap) or None,
                "public_benchmark_comparison": status(public_benchmark) or None,
                "young_tiny_residual": status(young_residual) or None,
            },
            "review_surface": {
                "clean": review_surface_clean,
                "public_benchmark_comparison": status(public_benchmark) or None,
                "young_tiny_residual": status(young_residual) or None,
                "public_benchmark_caveats": public_benchmark.get("hard_blockers") or [],
                "young_residual_caveats": young_residual.get("residual_caveats")
                or young_residual.get("hard_blockers")
                or [],
                "scope": "local review surface only; deployable remains false",
            },
            "summary": {
                "rows": len(rows),
                "qualified_tradeable_windows": qualified,
                "active_abstention_holdouts": active_abstain,
                "repeat_reference_windows": repeat_refs,
                "missing_qualified_tradeable_windows": missing_qualified,
                "repeat_remaining_strict_rescue_closes": inum(
                    shadow_gap_summary.get("repeat_remaining_strict_rescue_closes")
                ),
                "overfit_risk_control": regime_summary.get("overfit_risk_control"),
            },
            "rows": rows,
            "decision": {
                "next_action": next_action,
                "remote_runner_allowed": False,
                "research_only": True,
                "shadow_review_ready": is_keep(shadow_gate) and review_surface_clean,
                "shadow_gate_ready": is_keep(shadow_gate),
                "review_surface_clean": review_surface_clean,
                "deployable": False,
                "hard_blockers": body(shadow_gap, "decision").get("hard_blockers") or [],
            },
            "guardrails": [
                "This ledger is local explanation evidence only.",
                "It does not launch remote jobs, approve shadow, or approve deployment.",
                "Historical strong windows remain visible but do not replace fresh qualified repeat coverage.",
                "Weak active windows remain holdouts against overfit rather than targets for forced trading.",
            ],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--regime-generalization-scorecard", required=True)
    parser.add_argument("--shadow-promotion-gate-scorecard", required=True)
    parser.add_argument("--shadow-promotion-gap-scorecard", required=True)
    parser.add_argument("--run-trigger-policy-scorecard", required=True)
    parser.add_argument("--repeat-gap-scorecard", required=True)
    parser.add_argument("--public-benchmark-comparison-scorecard")
    parser.add_argument("--young-tiny-residual-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--csv-output")
    parser.add_argument("--markdown-output")
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    write_csv(args.csv_output, card["rows"])
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
