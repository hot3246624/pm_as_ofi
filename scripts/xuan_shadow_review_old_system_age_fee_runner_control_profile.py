#!/usr/bin/env python3
"""Audit runner-supported controls for the old-system age/fee residual frontier.

The age/fee frontier identified specific residual lots whose mature after-fee
mark recovery decayed badly.  This local-only scorer asks whether existing
runner knobs can express that control without starving density:

* surplus_budget_max_abs_unpaired_cost;
* risk_seed_pair_completion_required_above_net_cap;
* existing pair-completion/source/economic gates.

It deliberately does not tune from backtest V1, touch raw/shared data, authorize
remote runs, deploy, live orders, or private-truth claims.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0527Z"
RUN_TAG = "xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z"

DEFAULT_REMOTE_OUTPUTS = Path(f".tmp_xuan/local_verifier_artifacts/{RUN_TAG}/remote_outputs")
DEFAULT_FRONTIER = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_residual_age_fee_control_frontier_20260527T0457Z.json"
)
DEFAULT_PROFILE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_tail_mark_snapshot_profile_20260527T0338Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_age_fee_runner_control_profile_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_old_system_age_fee_runner_control_profile_{STAMP}/"
    "AGE_FEE_RUNNER_CONTROL_PROFILE.md"
)


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    with resolved.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_accepted_actions(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.would_action_decisions.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if row.get("kind") == "candidate" and row.get("decision") == "accept" and qid:
                row = dict(row)
                row["_source_file"] = path.name
                out[qid] = row
    return out


def load_candidate_events(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                qid = event.get("quote_intent_id")
                if event.get("kind") == "candidate" and qid:
                    out[str(qid)] = event
    return out


def load_fill_qids(remote_outputs: Path) -> set[str]:
    fills: set[str] = set()
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.would_fill_events.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if qid:
                fills.add(qid)
    return fills


def load_residual_lots(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    lots: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if not qid:
                continue
            lots[qid] = {
                "quote_intent_id": qid,
                "slug": row.get("slug"),
                "side": row.get("side"),
                "qty": fnum(row.get("qty"), 0.0) or 0.0,
                "px": fnum(row.get("px"), 0.0) or 0.0,
                "cost": fnum(row.get("cost"), 0.0) or 0.0,
            }
    return lots


def load_metrics(remote_outputs: Path) -> dict[str, Any]:
    path = remote_outputs.expanduser().resolve() / "aggregate_report.json"
    if not path.exists():
        return {}
    return body(load_json(path), "metrics")


def action_snapshot(qid: str, row: dict[str, Any], events: dict[str, dict[str, Any]]) -> dict[str, Any]:
    event = events.get(qid, {})
    return {
        "quote_intent_id": qid,
        "slug": row.get("slug"),
        "side": row.get("side"),
        "price": fnum(row.get("price")),
        "qty": fnum(row.get("qty")),
        "risk_increasing_seed": event.get("risk_increasing_seed"),
        "closeability_net_pair_cost": fnum(row.get("closeability_net_pair_cost")),
        "pair_completion_qty": fnum(row.get("pair_completion_qty"), 0.0) or 0.0,
        "pair_completion_decision": row.get("pair_completion_decision"),
        "pair_completion_worst_net_pair_cost": fnum(row.get("pair_completion_worst_net_pair_cost")),
        "risk_seed_pair_completion_min_qty": fnum(row.get("risk_seed_pair_completion_min_qty")),
        "surplus_budget_projected_unpaired_cost": fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0)
        or 0.0,
        "source_quality_decision": row.get("source_quality_decision"),
        "source_quality_l1_age_ms": fnum(row.get("source_quality_l1_age_ms")),
    }


def selected_qids(frontier: dict[str, Any], key: str) -> set[str]:
    lot_frontier = body(frontier, "lot_frontier")
    subset = body(lot_frontier, key)
    return {str(qid) for qid in listify(subset.get("controlled_quote_intent_ids")) if qid}


def high_recovery_qids(frontier: dict[str, Any]) -> set[str]:
    research_rate = fnum(body(frontier, "thresholds").get("research_review_recovery_rate"), 0.6) or 0.6
    lot_frontier = body(frontier, "lot_frontier")
    out: set[str] = set()
    for row in listify(lot_frontier.get("top_residual_lots")):
        if not isinstance(row, dict):
            continue
        qid = row.get("quote_intent_id")
        recovery = fnum(row.get("mature_recovery_rate_after_fee"))
        if qid and recovery is not None and recovery >= research_rate:
            out.add(str(qid))
    return out


def simulate_control(
    accepted: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    fills: set[str],
    residual_lots: dict[str, dict[str, Any]],
    *,
    pair_completion_required_above_net_cap: float | None,
    surplus_budget_max_abs_unpaired_cost: float | None,
    selected_full_hard: set[str],
    selected_cost_mark: set[str],
    high_recovery: set[str],
    min_accepted_after: int,
    min_fills_after: int,
    min_fill_rate_after: float,
) -> dict[str, Any]:
    blocked: dict[str, list[str]] = {}
    for qid, row in accepted.items():
        reasons: list[str] = []
        projected_unpaired = fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0
        if (
            surplus_budget_max_abs_unpaired_cost is not None
            and projected_unpaired > surplus_budget_max_abs_unpaired_cost + 1e-12
        ):
            reasons.append("surplus_budget_max_abs_unpaired_cost")
        if pair_completion_required_above_net_cap is not None:
            event = events.get(qid, {})
            closeability = fnum(row.get("closeability_net_pair_cost"))
            pair_completion_qty = fnum(row.get("pair_completion_qty"), 0.0) or 0.0
            required_qty = max(1e-9, fnum(row.get("risk_seed_pair_completion_min_qty"), 1.25) or 1.25)
            if (
                event.get("risk_increasing_seed") is True
                and (closeability is None or closeability > pair_completion_required_above_net_cap + 1e-12)
                and pair_completion_qty < required_qty - 1e-12
            ):
                reasons.append("risk_seed_pair_completion_required_above_net_cap")
        if reasons:
            blocked[qid] = reasons

    blocked_set = set(blocked)
    accepted_after = len(accepted) - len(blocked_set)
    fills_after = len(fills) - len(blocked_set & fills)
    fill_rate_after = fills_after / accepted_after if accepted_after > 0 else None
    residual_cut_cost = sum(
        (fnum(residual_lots[qid].get("cost"), 0.0) or 0.0) for qid in blocked_set if qid in residual_lots
    )
    residual_cut_qty = sum(
        (fnum(residual_lots[qid].get("qty"), 0.0) or 0.0) for qid in blocked_set if qid in residual_lots
    )
    selected_full_hard_covered = selected_full_hard <= blocked_set
    selected_cost_mark_covered = selected_cost_mark <= blocked_set
    high_recovery_blocked = sorted(blocked_set & high_recovery)
    density_pass = (
        accepted_after >= min_accepted_after
        and fills_after >= min_fills_after
        and fill_rate_after is not None
        and fill_rate_after >= min_fill_rate_after
    )
    return rounded(
        {
            "pair_completion_required_above_net_cap": pair_completion_required_above_net_cap,
            "surplus_budget_max_abs_unpaired_cost": surplus_budget_max_abs_unpaired_cost,
            "blocked_accept_qids": sorted(blocked_set),
            "blocked_accept_count": len(blocked_set),
            "blocked_fill_qids": sorted(blocked_set & fills),
            "blocked_fill_count": len(blocked_set & fills),
            "accepted_after_lower_bound": accepted_after,
            "fills_after_lower_bound": fills_after,
            "fill_rate_after_lower_bound": fill_rate_after,
            "density_lower_bound_pass": density_pass,
            "selected_full_hard_qids_covered": selected_full_hard_covered,
            "selected_cost_mark_qids_covered": selected_cost_mark_covered,
            "selected_full_hard_missing_qids": sorted(selected_full_hard - blocked_set),
            "selected_cost_mark_missing_qids": sorted(selected_cost_mark - blocked_set),
            "high_recovery_residual_qids_blocked": high_recovery_blocked,
            "high_recovery_residual_preserved": not high_recovery_blocked,
            "residual_cut_cost": residual_cut_cost,
            "residual_cut_qty": residual_cut_qty,
            "block_reasons_by_qid": {qid: blocked[qid] for qid in sorted(blocked)},
        }
    )


def enumerate_existing_control_candidates(
    accepted: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    fills: set[str],
    residual_lots: dict[str, dict[str, Any]],
    profile: dict[str, Any],
    *,
    selected_full_hard: set[str],
    selected_cost_mark: set[str],
    high_recovery: set[str],
    min_accepted_after: int,
    min_fills_after: int,
    min_fill_rate_after: float,
) -> list[dict[str, Any]]:
    closeability_values = sorted(
        {
            fnum(row.get("closeability_net_pair_cost"))
            for row in accepted.values()
            if fnum(row.get("closeability_net_pair_cost")) is not None
        }
    )
    surplus_values = sorted(
        {
            fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0
            for row in accepted.values()
        }
    )
    current_pair_cap = fnum(profile.get("risk_seed_pair_completion_required_above_net_cap"))
    current_surplus_budget = fnum(profile.get("surplus_budget_max_abs_unpaired_cost"))

    pair_caps: set[float | None] = {None}
    if current_pair_cap is not None:
        pair_caps.add(current_pair_cap)
    for value in closeability_values:
        pair_caps.add(round(max(0.0, value - 0.000001), 6))
    surplus_budgets: set[float | None] = {None}
    if current_surplus_budget is not None:
        surplus_budgets.add(current_surplus_budget)
    for value in surplus_values:
        if value > 0:
            surplus_budgets.add(round(max(0.0, value - 0.000001), 6))

    rows = [
        simulate_control(
            accepted,
            events,
            fills,
            residual_lots,
            pair_completion_required_above_net_cap=pair_cap,
            surplus_budget_max_abs_unpaired_cost=budget,
            selected_full_hard=selected_full_hard,
            selected_cost_mark=selected_cost_mark,
            high_recovery=high_recovery,
            min_accepted_after=min_accepted_after,
            min_fills_after=min_fills_after,
            min_fill_rate_after=min_fill_rate_after,
        )
        for pair_cap in sorted(pair_caps, key=lambda item: 99.0 if item is None else item)
        for budget in sorted(surplus_budgets, key=lambda item: 99.0 if item is None else item)
    ]
    rows.sort(
        key=lambda row: (
            0 if row["selected_full_hard_qids_covered"] else 1,
            0 if row["high_recovery_residual_preserved"] else 1,
            0 if row["density_lower_bound_pass"] else 1,
            row["blocked_accept_count"],
            row["blocked_fill_count"],
        )
    )
    return rows


def best_ready_candidate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if (
            row["selected_full_hard_qids_covered"]
            and row["high_recovery_residual_preserved"]
            and row["density_lower_bound_pass"]
        ):
            return row
    return {}


def build(args: argparse.Namespace) -> dict[str, Any]:
    frontier = load_json(args.frontier_scorecard)
    profile_card = load_json(args.profile_scorecard)
    profile = body(profile_card, "candidate_profile")
    remote_outputs = args.remote_outputs.expanduser().resolve()
    accepted = load_accepted_actions(remote_outputs)
    events = load_candidate_events(remote_outputs)
    fills = load_fill_qids(remote_outputs)
    residual_lots = load_residual_lots(remote_outputs)
    metrics = load_metrics(remote_outputs)

    selected_cost = selected_qids(frontier, "selected_cost_mark_control_subset")
    selected_full = selected_qids(frontier, "selected_full_hard_control_subset")
    high_recovery = high_recovery_qids(frontier)
    rows = enumerate_existing_control_candidates(
        accepted,
        events,
        fills,
        residual_lots,
        profile,
        selected_full_hard=selected_full,
        selected_cost_mark=selected_cost,
        high_recovery=high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
    )
    ready = best_ready_candidate(rows)
    best_selected = next((row for row in rows if row["selected_full_hard_qids_covered"]), {})
    best_density = next((row for row in rows if row["density_lower_bound_pass"]), {})

    existing_runner_profile_ready = bool(ready)
    hard_blockers: list[str] = []
    if not existing_runner_profile_ready:
        hard_blockers.append("existing_runner_controls_cannot_cover_bad_tail_without_density_starvation")
    if not selected_full:
        hard_blockers.append("selected_full_hard_frontier_qids_missing")
    if not profile:
        hard_blockers.append("profile_input_missing")
    hard_blockers.extend(["private_truth_not_ready"])

    status_value = (
        "KEEP_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_RUNNER_CONTROL_PROFILE_READY_LOCAL_ONLY"
        if existing_runner_profile_ready
        else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_RUNNER_CONTROL_PROFILE_EXISTING_CONTROLS_STARVE_DENSITY_LOCAL_ONLY"
    )

    current_candidate = simulate_control(
        accepted,
        events,
        fills,
        residual_lots,
        pair_completion_required_above_net_cap=fnum(profile.get("risk_seed_pair_completion_required_above_net_cap")),
        surplus_budget_max_abs_unpaired_cost=fnum(profile.get("surplus_budget_max_abs_unpaired_cost")),
        selected_full_hard=selected_full,
        selected_cost_mark=selected_cost,
        high_recovery=high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_age_fee_runner_control_profile",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_age_fee_runner_control_profile.py",
            "status": status_value,
            "inputs": {
                "frontier_scorecard": str(args.frontier_scorecard),
                "profile_scorecard": str(args.profile_scorecard),
                "remote_outputs": str(args.remote_outputs),
            },
            "source_statuses": {
                "age_fee_frontier": status(frontier),
                "tail_mark_snapshot_profile": status(profile_card),
            },
            "current_runtime": {
                "accepted_actions": len(accepted),
                "queue_supported_fills": len(fills),
                "pair_pnl": metrics.get("pair_pnl"),
                "roi_on_filled_cost": metrics.get("roi_on_filled_cost"),
                "residual_cost": metrics.get("residual_cost"),
                "residual_qty": metrics.get("residual_qty"),
                "residual_cost_share": (
                    (fnum(metrics.get("residual_cost"), 0.0) or 0.0)
                    / (fnum(metrics.get("filled_cost"), 0.0) or 1.0)
                    if metrics
                    else None
                ),
                "residual_qty_share": (
                    (fnum(metrics.get("residual_qty"), 0.0) or 0.0)
                    / (fnum(metrics.get("filled_qty"), 0.0) or 1.0)
                    if metrics
                    else None
                ),
            },
            "frontier_targets": {
                "selected_cost_mark_qids": sorted(selected_cost),
                "selected_full_hard_qids": sorted(selected_full),
                "high_recovery_residual_qids_to_preserve": sorted(high_recovery),
            },
            "density_thresholds": {
                "min_accepted_after": args.min_accepted_after,
                "min_fills_after": args.min_fills_after,
                "min_fill_rate_after": args.min_fill_rate_after,
            },
            "existing_runner_control_audit": {
                "current_profile_control": current_candidate,
                "existing_runner_profile_ready": existing_runner_profile_ready,
                "ready_candidate": ready,
                "best_candidate_covering_selected_full_hard": best_selected,
                "best_density_preserving_candidate": best_density,
                "top_candidates": rows[:10],
                "controls_enumerated": [
                    "risk_seed_pair_completion_required_above_net_cap",
                    "surplus_budget_max_abs_unpaired_cost",
                ],
            },
            "runner_gap": {
                "runner_gap_identified": not existing_runner_profile_ready,
                "gap_read": (
                    "Existing generic closeability/pair-completion and surplus-budget knobs are too blunt for "
                    "the age/fee residual frontier: candidates that cover all bad residual lots block too many "
                    "accepted/fill rows, while density-preserving candidates leave target bad residual uncovered."
                ),
                "required_new_support": [
                    "fee-aware bad-tail admission or sizing rule keyed to mature mark/recovery risk, not qids",
                    "local replay/scorer proof that it preserves accepted/fill/rescue density",
                    "manifest/scaffold/preauthorization before any future bounded cap25 PM_DRY_RUN",
                ],
            },
            "candidate_profile": {
                **profile,
                "age_fee_runner_control_profile_ready": existing_runner_profile_ready,
                "age_fee_runner_control_candidate": ready,
            },
            "decision": {
                "old_system_age_fee_runner_control_scorer_ready": True,
                "existing_runner_profile_ready": existing_runner_profile_ready,
                "candidate_profile_ready": existing_runner_profile_ready,
                "selected_bad_tail_covered_by_existing_controls": bool(
                    ready or (best_selected and best_selected.get("selected_full_hard_qids_covered"))
                ),
                "density_preservation_proven_by_existing_controls": bool(
                    ready or (best_density and best_density.get("density_lower_bound_pass"))
                ),
                "high_recovery_residual_preserved_by_ready_candidate": bool(
                    ready and ready.get("high_recovery_residual_preserved")
                ),
                "runner_gap_identified": not existing_runner_profile_ready,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_profile": False,
                "future_remote_requires_new_runner_support_or_scorer": not existing_runner_profile_ready,
                "cap75_remote_rationale_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "research_only": True,
                "paper_shadow_only": True,
                "use_backtest_v1_candidate_audit_pack": False,
                "hard_blockers": hard_blockers,
                "next_action": (
                    "local-only design a non-qid age/fee-aware admission or sizing support path; do not run remote "
                    "until a runner-supported scorer/profile proves bad-tail coverage and density preservation"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    audit = card["existing_runner_control_audit"]
    targets = card["frontier_targets"]
    best_selected = audit["best_candidate_covering_selected_full_hard"]
    best_density = audit["best_density_preserving_candidate"]
    lines = [
        "# Xuan Old-System Age/Fee Runner Control Profile",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- existing_runner_profile_ready: `{decision['existing_runner_profile_ready']}`",
        f"- runner_gap_identified: `{decision['runner_gap_identified']}`",
        f"- bounded_cap25_remote_rationale_ready: `{decision['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_profile: `{decision['future_remote_allowed_by_this_profile']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers'])}`",
        "",
        "## Targets",
        "",
        f"- selected full hard qids: `{targets['selected_full_hard_qids']}`",
        f"- selected cost/mark qids: `{targets['selected_cost_mark_qids']}`",
        f"- high-recovery residual qids to preserve: `{targets['high_recovery_residual_qids_to_preserve']}`",
        "",
        "## Existing Controls",
        "",
        f"- controls enumerated: `{audit['controls_enumerated']}`",
        f"- current profile controls selected full hard qids: "
        f"`{audit['current_profile_control']['selected_full_hard_qids_covered']}`",
        "",
        "## Best Candidate Covering Selected Full Hard",
        "",
        f"- pair completion cap: `{best_selected.get('pair_completion_required_above_net_cap')}`",
        f"- surplus budget: `{best_selected.get('surplus_budget_max_abs_unpaired_cost')}`",
        f"- blocked accepts/fills: `{best_selected.get('blocked_accept_count')}` / `{best_selected.get('blocked_fill_count')}`",
        f"- accepted/fills after: `{best_selected.get('accepted_after_lower_bound')}` / `{best_selected.get('fills_after_lower_bound')}`",
        f"- density pass: `{best_selected.get('density_lower_bound_pass')}`",
        f"- high recovery preserved: `{best_selected.get('high_recovery_residual_preserved')}`",
        "",
        "## Best Density-Preserving Candidate",
        "",
        f"- pair completion cap: `{best_density.get('pair_completion_required_above_net_cap')}`",
        f"- surplus budget: `{best_density.get('surplus_budget_max_abs_unpaired_cost')}`",
        f"- selected full hard covered: `{best_density.get('selected_full_hard_qids_covered')}`",
        f"- missing selected qids: `{best_density.get('selected_full_hard_missing_qids')}`",
        f"- accepted/fills after: `{best_density.get('accepted_after_lower_bound')}` / `{best_density.get('fills_after_lower_bound')}`",
        "",
        "## Interpretation",
        "",
        f"- {card['runner_gap']['gap_read']}",
        "",
        "## Boundary",
        "",
        "- Old/current xuan no-order/shadow system only; backtest V1 candidate audit pack is not used.",
        "- Local-only and research-only; no remote authorization, cap75/150/300, deploy, live orders, or private truth.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote-outputs", type=Path, default=DEFAULT_REMOTE_OUTPUTS)
    parser.add_argument("--frontier-scorecard", type=Path, default=DEFAULT_FRONTIER)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--min-accepted-after", type=int, default=10)
    parser.add_argument("--min-fills-after", type=int, default=8)
    parser.add_argument("--min-fill-rate-after", type=float, default=0.70)
    parser.add_argument("--scorecard-out", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown-out", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    args.scorecard_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard_out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown_out.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps({"status": card["status"], "scorecard": str(args.scorecard_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
