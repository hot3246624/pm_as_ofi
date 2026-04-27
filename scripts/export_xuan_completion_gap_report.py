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


def as_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def as_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        return int(v)
    except Exception:
        return 0


def build_control_screening(report: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    summary = report.get("summary") or {}
    rows = report.get("rows") or []
    cfg = contract.get("control_screen_thresholds") or {}

    def score_bucket_stats() -> tuple[int, float]:
        dist = summary.get("score_bucket_distribution") or {}
        counts = [as_int(v) for v in dist.values() if as_int(v) > 0]
        total = sum(counts)
        if total <= 0:
            return 0, 0.0
        return len(counts), max(counts) / float(total)

    def market_slug_stats() -> tuple[int, int]:
        want = str((cfg.get("single_venue_btc_5m") or {}).get("slug_substring") or "")
        if not rows:
            return 0, 0
        total = 0
        matched = 0
        for row in rows:
            slug = str(row.get("slug") or "")
            if not slug:
                continue
            total += 1
            if want and want in slug:
                matched += 1
        return total, matched

    checks: list[dict[str, Any]] = []

    total_markets, matched_markets = market_slug_stats()
    if total_markets == 0:
        venue_status = "insufficient"
    elif matched_markets == total_markets:
        venue_status = "pass"
    else:
        venue_status = "fail"
    checks.append(
        {
            "id": "single_venue_btc_5m",
            "kind": "must_match",
            "status": venue_status,
            "observed": {
                "market_count": total_markets,
                "btc_5m_market_count": matched_markets,
            },
            "note": "All active shadow markets should remain on BTC 5m.",
        }
    )

    open_candidate_total = as_int(summary.get("open_candidate_total"))
    coverage_cfg = cfg.get("high_round_coverage") or {}
    if coverage_cfg.get("require_nonzero_open_candidates") and open_candidate_total == 0:
        coverage_status = "fail"
        coverage_note = "No open candidates means there is no evidence of xuan-like round coverage."
    else:
        coverage_status = "insufficient"
        coverage_note = "Needs xuan relative round-coverage truth before hard pass/fail."
    checks.append(
        {
            "id": "high_round_coverage",
            "kind": "must_match",
            "status": coverage_status,
            "observed": {
                "open_candidate_total": open_candidate_total,
                "open_allowed_total": as_int(summary.get("open_allowed_total")),
                "market_count": as_int(summary.get("market_count")),
            },
            "note": coverage_note,
        }
    )

    same_side_p90 = as_float(summary.get("same_side_add_qty_ratio_p90"))
    dir_cfg = cfg.get("low_directionality") or {}
    dir_pass_max = as_float(dir_cfg.get("same_side_add_qty_ratio_p90_pass_max"))
    dir_watch_max = as_float(dir_cfg.get("same_side_add_qty_ratio_p90_watch_max"))
    if same_side_p90 <= dir_pass_max:
        dir_status = "pass"
    elif same_side_p90 <= dir_watch_max:
        dir_status = "watch"
    else:
        dir_status = "fail"
    checks.append(
        {
            "id": "low_directionality",
            "kind": "must_match",
            "status": dir_status,
            "observed": {
                "same_side_add_qty_ratio_p90": same_side_p90,
                "pass_max": dir_pass_max,
                "watch_max": dir_watch_max,
            },
            "note": "High same-side drift suggests directional contamination rather than completion-first behavior.",
        }
    )

    completion_hit = as_float(summary.get("30s_completion_hit_rate"))
    clean_closed = as_float(summary.get("clean_closed_episode_ratio_median"))
    completion_cfg = cfg.get("in_round_completion") or {}
    completion_pass = (
        completion_hit >= as_float(completion_cfg.get("completion_30s_hit_rate_pass_min"))
        and clean_closed >= as_float(completion_cfg.get("clean_closed_episode_ratio_median_pass_min"))
    )
    checks.append(
        {
            "id": "in_round_completion",
            "kind": "must_match",
            "status": "pass" if completion_pass else "fail",
            "observed": {
                "30s_completion_hit_rate": completion_hit,
                "clean_closed_episode_ratio_median": clean_closed,
                "completion_30s_hit_rate_pass_min": as_float(
                    completion_cfg.get("completion_30s_hit_rate_pass_min")
                ),
                "clean_closed_episode_ratio_median_pass_min": as_float(
                    completion_cfg.get("clean_closed_episode_ratio_median_pass_min")
                ),
            },
            "note": "xuan progress must show round-inside pairing, not only after-close cleanup.",
        }
    )

    clip_cfg = cfg.get("state_selected_clip") or {}
    nonzero_buckets, dominant_bucket_share = score_bucket_stats()
    clip_pass = (
        nonzero_buckets >= as_int(clip_cfg.get("min_nonzero_score_buckets"))
        and dominant_bucket_share < as_float(clip_cfg.get("max_single_bucket_share"))
    )
    checks.append(
        {
            "id": "state_selected_clip",
            "kind": "must_match",
            "status": "pass" if clip_pass else "fail",
            "observed": {
                "nonzero_score_bucket_count": nonzero_buckets,
                "dominant_score_bucket_share": dominant_bucket_share,
                "min_nonzero_score_buckets": as_int(clip_cfg.get("min_nonzero_score_buckets")),
                "max_single_bucket_share": as_float(clip_cfg.get("max_single_bucket_share")),
            },
            "note": "A single dominant score bucket usually means fixed-clip behavior, not state-selected clip.",
        }
    )

    checks.append(
        {
            "id": "maker_leaning_execution",
            "kind": "nice_to_have",
            "status": "insufficient",
            "observed": {},
            "note": "Still blocked on stronger maker/taker and fillability truth.",
        }
    )
    checks.append(
        {
            "id": "in_round_merge_capital_recycling",
            "kind": "nice_to_have",
            "status": "insufficient",
            "observed": {
                "merge_executed_total": as_int(summary.get("merge_executed_total")),
                "redeem_requested_total": as_int(summary.get("redeem_requested_total")),
            },
            "note": "Merge/redeem counts alone do not distinguish xuan-like recycling from slow cleanup.",
        }
    )

    anti_directional_status = "watch" if dir_status == "fail" else "insufficient"
    checks.append(
        {
            "id": "selective_directional_round_picker",
            "kind": "anti_target",
            "status": anti_directional_status,
            "observed": {
                "same_side_add_qty_ratio_p90": same_side_p90,
                "coverage_truth_ready": False,
            },
            "note": "Hard anti-target needs both low coverage and high directional skew; current screen only sees the skew side.",
        }
    )

    checks.append(
        {
            "id": "multi_venue_slow_merge_spread",
            "kind": "anti_target",
            "status": "clear" if venue_status == "pass" else ("warning" if venue_status == "fail" else "insufficient"),
            "observed": {
                "market_count": total_markets,
                "btc_5m_market_count": matched_markets,
            },
            "note": "Multi-venue deployment is a different archetype, even if the PnL surface looks smoother.",
        }
    )

    fixed_cfg = cfg.get("fixed_clip_post_close_batch_merge") or {}
    merge_or_redeem = as_int(summary.get("merge_executed_total")) + as_int(summary.get("redeem_requested_total"))
    fixed_clip_warning = (
        (not clip_pass)
        and completion_hit < as_float(fixed_cfg.get("completion_30s_hit_rate_fail_max"))
        and (
            not bool(fixed_cfg.get("require_merge_or_redeem_activity"))
            or merge_or_redeem > 0
        )
    )
    checks.append(
        {
            "id": "fixed_clip_post_close_batch_merge",
            "kind": "anti_target",
            "status": "warning" if fixed_clip_warning else "clear",
            "observed": {
                "clip_screen_pass": clip_pass,
                "30s_completion_hit_rate": completion_hit,
                "merge_or_redeem_total": merge_or_redeem,
            },
            "note": "If clip is effectively fixed and completion is weak, later merge/redeem can fake progress.",
        }
    )

    status_counts: dict[str, int] = {}
    for check in checks:
        status = str(check.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "checks": checks,
        "status_counts": status_counts,
    }


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
        "archetype_controls": contract.get("archetype_controls") or {},
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
        "control_screening": build_control_screening(report, contract),
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
