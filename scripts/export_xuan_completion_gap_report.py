#!/usr/bin/env python3
"""Compare completion_first shadow report against xuan 30s completion targets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_XUAN_SUMMARY = REPO_ROOT / "docs" / "xuan_completion_gate_summary.json"
DEFAULT_RESEARCH_CONTRACT = REPO_ROOT / "configs" / "xuan_research_contract.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--report-json", required=True, help="completion_first_shadow_summary.json")
    p.add_argument("--xuan-summary-json", default=str(DEFAULT_XUAN_SUMMARY))
    p.add_argument("--baseline-report-json", help="Optional baseline shadow summary json")
    p.add_argument("--research-contract-json", default=str(DEFAULT_RESEARCH_CONTRACT))
    p.add_argument("--output-json", help="Optional explicit output path")
    return p.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing json file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"expected dict json: {path}")
    return payload


def nested_get(payload: dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = payload
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def build_decision_summary(
    report: dict[str, Any],
    xuan_summary: dict[str, Any],
    contract: dict[str, Any],
    baseline_report: dict[str, Any] | None,
) -> dict[str, Any]:
    report_summary = report.get("summary") or {}
    xuan_targets = xuan_summary.get("xuan_targets") or {}
    thresholds = contract.get("thresholds") or {}

    data_cfg = thresholds.get("data_usable") or {}
    recent_overlap = nested_get(xuan_summary, str(data_cfg.get("field") or ""), 0) or 0
    recent_overlap_min = int(data_cfg.get("recent_overlap_episode_min") or 0)
    data_usable = int(recent_overlap) >= recent_overlap_min

    research_cfg = thresholds.get("research_effective") or {}
    holdout_lift = nested_get(xuan_summary, str(research_cfg.get("field") or ""), 0.0) or 0.0
    holdout_lift_min = float(research_cfg.get("holdout_lift_min_abs") or 0.0)
    research_effective = float(holdout_lift) >= holdout_lift_min

    enforce_cfg = thresholds.get("enforce_discussion_ready") or {}
    xuan_hit = float(xuan_targets.get("xuan_30s_completion_hit_rate") or 0.0)
    shadow_hit = float(report_summary.get("30s_completion_hit_rate") or 0.0)
    xuan_delay = float(xuan_targets.get("xuan_median_first_opposite_delay_s") or 0.0)
    shadow_delay = float(report_summary.get("median_first_opposite_delay_s") or 0.0)
    xuan_gap = abs(shadow_hit - xuan_hit)
    delay_gap = abs(shadow_delay - xuan_delay)
    baseline_ready = baseline_report is not None
    baseline_lift = None
    if baseline_report is not None:
        baseline_summary = baseline_report.get("summary") or {}
        baseline_hit = float(baseline_summary.get("30s_completion_hit_rate") or 0.0)
        baseline_lift = shadow_hit - baseline_hit
    enforce_ready = (
        baseline_ready
        and baseline_lift is not None
        and baseline_lift >= float(enforce_cfg.get("shadow_vs_baseline_completion_lift_min_abs") or 0.0)
        and xuan_gap <= float(enforce_cfg.get("shadow_vs_xuan_completion_gap_max_abs") or 0.0)
        and delay_gap <= float(enforce_cfg.get("shadow_vs_xuan_delay_gap_max_s") or 0.0)
    )

    if not data_usable:
        verdict = "Data Not Usable"
    elif not research_effective:
        verdict = "Research Not Effective"
    else:
        verdict = "Shadow Gap Actionable"

    return {
        "verdict": verdict,
        "data_usable": {
            "pass": data_usable,
            "recent_overlap_episode_count": int(recent_overlap),
            "recent_overlap_episode_min": recent_overlap_min,
        },
        "research_effective": {
            "pass": research_effective,
            "holdout_lift_pct": float(holdout_lift),
            "holdout_lift_min_abs": holdout_lift_min,
        },
        "enforce_discussion_ready": {
            "pass": enforce_ready,
            "baseline_report_present": baseline_ready,
            "baseline_lift": baseline_lift,
            "shadow_vs_xuan_completion_gap_abs": xuan_gap,
            "shadow_vs_xuan_delay_gap_s": delay_gap,
        },
        "question_priority": contract.get("question_priority") or [],
        "shadow_gap_question_order": contract.get("shadow_gap_question_order") or [],
    }


def build_gap(
    report: dict[str, Any],
    xuan_summary: dict[str, Any],
    contract: dict[str, Any],
    baseline_report: dict[str, Any] | None,
) -> dict[str, Any]:
    report_summary = report.get("summary") or {}
    xuan_targets = xuan_summary.get("xuan_targets") or {}
    return {
        "shadow_summary": {
            "markets": report_summary.get("market_count", 0),
            "open_candidate_total": report_summary.get("open_candidate_total", 0),
            "open_allowed_total": report_summary.get("open_allowed_total", 0),
            "open_blocked_total": report_summary.get("open_blocked_total", 0),
            "completion_30s_hit_rate": report_summary.get("30s_completion_hit_rate"),
            "completion_30s_hit_rate_when_gate_on": report_summary.get("30s_completion_hit_rate_when_gate_on"),
            "completion_30s_hit_rate_when_gate_off": report_summary.get("30s_completion_hit_rate_when_gate_off"),
            "median_first_opposite_delay_s": report_summary.get("median_first_opposite_delay_s"),
            "score_bucket_distribution": report_summary.get("score_bucket_distribution", {}),
            "session_bucket_distribution": report_summary.get("session_bucket_distribution", {}),
        },
        "xuan_targets": xuan_targets,
        "gap_vs_xuan": {
            "completion_30s_hit_rate": (
                report_summary.get("30s_completion_hit_rate", 0.0)
                - xuan_targets.get("xuan_30s_completion_hit_rate", 0.0)
            ),
            "median_first_opposite_delay_s": (
                report_summary.get("median_first_opposite_delay_s", 0.0)
                - xuan_targets.get("xuan_median_first_opposite_delay_s", 0.0)
            ),
            "maker_proxy_ratio": None,
        },
        "decision_summary": build_decision_summary(report, xuan_summary, contract, baseline_report),
        "provisional": bool(xuan_summary.get("provisional")),
    }


def main() -> None:
    args = parse_args()
    report_path = Path(args.report_json)
    xuan_summary_path = Path(args.xuan_summary_json)
    contract_path = Path(args.research_contract_json)
    baseline_report = load_json(Path(args.baseline_report_json)) if args.baseline_report_json else None
    payload = build_gap(
        load_json(report_path),
        load_json(xuan_summary_path),
        load_json(contract_path),
        baseline_report,
    )
    if args.output_json:
        output_path = Path(args.output_json)
    else:
        output_path = report_path.with_name(f"xuan_completion_gap_{report_path.stem}.json")
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
