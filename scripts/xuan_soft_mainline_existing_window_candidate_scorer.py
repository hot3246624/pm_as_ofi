#!/usr/bin/env python3
"""Scan existing no-order runtime summaries for soft-mainline candidates.

This local-only scorer looks for historical windows that resemble the current
no-cancel soft-mainline evidence lane. It is deliberately conservative: finding
a candidate does not promote shadow, because older/capacity/legacy windows may
still need fresh reproduction under the current profile.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


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


def expand_paths(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = [Path(item) for item in glob.glob(pattern)]
        if matches:
            paths.extend(matches)
        else:
            path = Path(pattern)
            if path.exists():
                paths.append(path)
    return sorted({path.resolve() for path in paths})


def metrics_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["accepted_actions"],
        row["queue_supported_fills"],
        row["strict_rescue_closes"],
        row["pair_pnl"],
        row["residual_qty_share"],
        row["residual_cost_share"],
        row["rescue_net_pair_cost_max"],
        row["activation_mode"],
        row["risk_seed_pending_opp_credit"],
        row["strict_rescue_surplus_net_cap"],
    )


def classify(row: dict[str, Any], args: argparse.Namespace) -> tuple[str, list[str], bool]:
    blockers: list[str] = []
    if not str(row["status"]).startswith("KEEP"):
        blockers.append("runtime_status_not_keep")
    if row["risk_seed_cancel_on_closeability_net_cap"] is not None:
        blockers.append("closeability_cancel_guard_enabled")
    if row["accepted_actions"] < args.min_accepted_actions:
        blockers.append("accepted_actions_below_shadow_floor")
    if row["queue_supported_fills"] < args.min_fills:
        blockers.append("fills_below_shadow_floor")
    if row["strict_rescue_closes"] < args.min_rescue_closes:
        blockers.append("strict_rescue_closes_below_shadow_floor")
    if row["pair_pnl"] < args.min_pair_pnl:
        blockers.append("pair_pnl_below_floor")
    if row["residual_qty_share"] is None or row["residual_qty_share"] > args.max_residual_qty_share:
        blockers.append("residual_qty_share_above_max")
    if row["residual_cost_share"] is None or row["residual_cost_share"] > args.max_residual_cost_share:
        blockers.append("residual_cost_share_above_max")
    if row["rescue_l1_age_ms_max"] is not None and row["rescue_l1_age_ms_max"] > args.max_rescue_l1_age_ms:
        blockers.append("rescue_l1_age_above_max")
    if row["strict_rescue_source_blocks"] > args.max_source_blocks:
        blockers.append("source_blocks_present")

    strict_target_ok = (
        row["rescue_net_pair_cost_max"] is not None
        and row["rescue_net_pair_cost_max"] <= args.max_strict_target_rescue_net_pair_cost
    )
    explicit_surplus_ok = (
        row["strict_rescue_surplus_net_cap"] is not None
        and row["strict_rescue_surplus_net_cap"] <= args.max_surplus_rescue_net_pair_cost
        and row["strict_rescue_min_pair_pnl_after"] == 0.0
        and row["risk_seed_pending_opp_credit"] == args.expected_pending_opp_credit
    )
    if not strict_target_ok and not explicit_surplus_ok:
        blockers.append("rescue_economics_not_current_or_strict_target")

    sample_and_risk_ok = not blockers
    name = row["scorecard"].lower()
    current_mainline_name = "soft-mainline-explicit-surplus-no-cancel" in name
    if sample_and_risk_ok and current_mainline_name:
        return "fresh_mainline_candidate_shadow_review_input", blockers, True
    if sample_and_risk_ok and explicit_surplus_ok:
        return "backfill_current_economics_candidate_needs_reproduction_review", blockers, False
    if sample_and_risk_ok and strict_target_ok:
        return "historical_strict_target_reference_candidate", blockers, False

    soft_blockers = set(blockers)
    if (
        "accepted_actions_below_shadow_floor" in soft_blockers
        or "fills_below_shadow_floor" in soft_blockers
    ) and row["strict_rescue_closes"] >= args.min_rescue_closes and row["pair_pnl"] >= args.min_pair_pnl:
        return "thin_positive_rescue_window_not_shadow_sized", blockers, False
    if row["accepted_actions"] >= args.min_accepted_actions and row["queue_supported_fills"] >= args.min_fills:
        return "sample_sized_but_risk_or_rescue_blocked", blockers, False
    return "not_shadow_candidate", blockers, False


def row_from_scorecard(path: Path, data: dict[str, Any], args: argparse.Namespace) -> dict[str, Any] | None:
    metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
    if not metrics:
        return None
    row = {
        "scorecard": str(path),
        "label": path.name.removesuffix("_runtime_summary.json"),
        "status": data.get("status"),
        "accepted_actions": inum(metrics.get("accepted_actions")),
        "queue_supported_fills": inum(metrics.get("queue_supported_fills")),
        "strict_rescue_closes": inum(metrics.get("strict_rescue_closes")),
        "strict_rescue_qty": fnum(metrics.get("strict_rescue_qty")),
        "pair_pnl": fnum(metrics.get("pair_pnl"), 0.0) or 0.0,
        "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share")),
        "residual_cost_share": fnum(metrics.get("residual_cost_share")),
        "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks"), 0.0) or 0.0,
        "activation_mode": profile.get("activation_mode"),
        "risk_seed_pending_opp_credit": fnum(profile.get("risk_seed_pending_opp_credit")),
        "strict_rescue_surplus_net_cap": fnum(profile.get("strict_rescue_surplus_net_cap")),
        "strict_rescue_min_pair_pnl_after": fnum(profile.get("strict_rescue_min_pair_pnl_after")),
        "strict_rescue_salvage_net_cap": fnum(profile.get("strict_rescue_salvage_net_cap")),
        "risk_seed_cancel_on_closeability_net_cap": fnum(profile.get("risk_seed_cancel_on_closeability_net_cap")),
        "profile_family": profile.get("profile_family"),
    }
    role, blockers, fresh = classify(row, args)
    row["candidate_role"] = role
    row["blockers"] = blockers
    row["fresh_shadow_input_candidate"] = fresh
    return row


def write_csv(path: str | None, rows: list[dict[str, Any]]) -> None:
    if not path:
        return
    out = Path(path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "label",
        "candidate_role",
        "accepted_actions",
        "queue_supported_fills",
        "strict_rescue_closes",
        "pair_pnl",
        "residual_qty_share",
        "residual_cost_share",
        "rescue_net_pair_cost_max",
        "strict_rescue_surplus_net_cap",
        "risk_seed_pending_opp_credit",
        "fresh_shadow_input_candidate",
        "blockers",
        "scorecard",
    ]
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            record = {key: row.get(key) for key in fields}
            record["blockers"] = ",".join(str(item) for item in row.get("blockers") or [])
            writer.writerow(record)


def build_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Soft-Mainline Existing Window Candidate Scan",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{card['decision']['next_action']}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in card["summary"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Top Candidates", ""])
    for row in card["top_candidates"]:
        lines.append(
            f"- `{row['label']}` `{row['candidate_role']}`: accepted `{row['accepted_actions']}`, "
            f"fills `{row['queue_supported_fills']}`, rescues `{row['strict_rescue_closes']}`, "
            f"pair_pnl `{row['pair_pnl']}`, residual cost share `{row['residual_cost_share']}`"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This scan is local review evidence only.",
            "- Backfill candidates do not become shadow evidence without current-profile review or fresh reproduction.",
            "- Duplicate smoke reruns are collapsed by metrics/profile key.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for path in expand_paths(args.scorecard_glob):
        data = load_json(path)
        if not data:
            continue
        row = row_from_scorecard(path, data, args)
        if row:
            rows.append(row)

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    duplicate_count = 0
    for row in sorted(
        rows,
        key=lambda item: (
            -item["strict_rescue_closes"],
            -item["accepted_actions"],
            -item["queue_supported_fills"],
            str(item["scorecard"]),
        ),
    ):
        key = metrics_key(row)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped.append(row)

    role_counts: dict[str, int] = {}
    for row in deduped:
        role_counts[row["candidate_role"]] = role_counts.get(row["candidate_role"], 0) + 1
    review_candidates = [
        row
        for row in deduped
        if row["candidate_role"]
        in {
            "fresh_mainline_candidate_shadow_review_input",
            "backfill_current_economics_candidate_needs_reproduction_review",
            "historical_strict_target_reference_candidate",
            "thin_positive_rescue_window_not_shadow_sized",
        }
    ]
    formal_fresh = [row for row in deduped if row["fresh_shadow_input_candidate"]]

    if formal_fresh:
        status = "KEEP_SOFT_MAINLINE_EXISTING_WINDOW_SCAN_FRESH_CANDIDATE_FOUND_LOCAL_REVIEW_ONLY"
        next_action = "review_existing_fresh_candidate_before_any_remote"
    elif any(row["candidate_role"] == "backfill_current_economics_candidate_needs_reproduction_review" for row in deduped):
        status = "UNKNOWN_SOFT_MAINLINE_EXISTING_WINDOW_SCAN_BACKFILL_CANDIDATES_FOUND_LOCAL_ONLY"
        next_action = "review_backfill_candidate_and_decide_whether_fresh_reproduction_is_required"
    elif review_candidates:
        status = "UNKNOWN_SOFT_MAINLINE_EXISTING_WINDOW_SCAN_ONLY_THIN_OR_REFERENCE_CANDIDATES_LOCAL_ONLY"
        next_action = "keep_waiting_for_fresh_density_or_score_more_history"
    else:
        status = "BLOCKED_SOFT_MAINLINE_EXISTING_WINDOW_SCAN_NO_CANDIDATES_LOCAL_ONLY"
        next_action = "wait_for_fresh_density_signal_before_remote"

    card = {
        "artifact": "xuan_soft_mainline_existing_window_candidate_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_existing_window_candidate_scorer.py",
        "status": status,
        "inputs": {"scorecard_glob": args.scorecard_glob},
        "thresholds": {
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_rescue_closes": args.min_rescue_closes,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_strict_target_rescue_net_pair_cost": args.max_strict_target_rescue_net_pair_cost,
            "max_surplus_rescue_net_pair_cost": args.max_surplus_rescue_net_pair_cost,
            "expected_pending_opp_credit": args.expected_pending_opp_credit,
        },
        "summary": {
            "runtime_summary_files": len(rows),
            "deduped_windows": len(deduped),
            "duplicates_collapsed": duplicate_count,
            "review_candidate_count": len(review_candidates),
            "fresh_shadow_input_candidate_count": len(formal_fresh),
            "role_counts": role_counts,
        },
        "top_candidates": review_candidates[: args.max_candidates],
        "decision": {
            "next_action": next_action,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_review_ready": False,
            "deployable": False,
        },
        "guardrails": [
            "This scan is local review evidence only.",
            "It does not launch remote jobs or approve shadow.",
            "Backfill candidates require current-profile review or fresh reproduction before promotion use.",
            "Duplicate smoke reruns are collapsed by metrics/profile key.",
        ],
    }
    return rounded(card), deduped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scorecard-glob", nargs="+", default=[".tmp_xuan/scorecards/*runtime_summary*.json"])
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--csv-output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--max-candidates", type=int, default=20)
    parser.add_argument("--min-accepted-actions", type=int, default=25)
    parser.add_argument("--min-fills", type=int, default=20)
    parser.add_argument("--min-rescue-closes", type=int, default=7)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    parser.add_argument("--max-strict-target-rescue-net-pair-cost", type=float, default=0.95)
    parser.add_argument("--max-surplus-rescue-net-pair-cost", type=float, default=1.02)
    parser.add_argument("--expected-pending-opp-credit", type=float, default=0.5)
    args = parser.parse_args()

    card, rows = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    write_csv(args.csv_output, rows)
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
