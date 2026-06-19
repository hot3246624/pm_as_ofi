#!/usr/bin/env python3
"""Local counterfactual redesign for the failed mature-tail cap25 profile.

The mature-tail surplus-budget profile completed safely but failed density and
residual gates. This script tests stricter runner-supported blocking controls
against the observed lifecycle rows and decides whether any such local proxy is
strong enough to justify another bounded cap25 dry-run.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_CAPACITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_capacity_stage_public_benchmark_gate.json")
DEFAULT_FAILURE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_failure_diagnosis_20260526T2148Z.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_counterfactual_redesign_20260526T2148Z.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def collect_rows(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    actions: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    residual_lots: dict[str, dict[str, Any]] = {}
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = load_json(manifest_path)
        files = manifest.get("files", {})
        for row in read_csv_rows(root / files.get("would_action_decisions", "")):
            if row.get("decision") == "accept":
                actions.append(dict(row))
        for row in read_csv_rows(root / files.get("would_fill_events", "")):
            if row.get("kind") == "queue_supported_fill":
                fills.append(dict(row))
    for path in sorted(root.glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            qid = row.get("quote_intent_id")
            if qid:
                residual_lots[qid] = dict(row)
    actions.sort(key=lambda row: fnum(row.get("ts_ms")))
    fills.sort(key=lambda row: fnum(row.get("ts_ms")))
    return actions, fills, residual_lots


def fill_cost(row: dict[str, Any]) -> float:
    return fnum(row.get("qty")) * fnum(row.get("price")) + fnum(row.get("fee"))


def residual_cost(row: dict[str, Any]) -> float:
    return fnum(row.get("cost"))


def residual_qty(row: dict[str, Any]) -> float:
    return fnum(row.get("qty"))


def pairing_only_proxy_blocked(actions: list[dict[str, Any]], dust_qty: float = 1.0) -> set[str]:
    exposure: dict[str, dict[str, float]] = {}
    blocked: set[str] = set()
    for row in actions:
        slug = str(row.get("slug"))
        side = str(row.get("side"))
        qid = str(row.get("quote_intent_id"))
        qty = fnum(row.get("qty"))
        sides = exposure.setdefault(slug, {"YES": 0.0, "NO": 0.0})
        same_qty = sides.get(side, 0.0)
        opp_side = "NO" if side == "YES" else "YES"
        opp_qty = sides.get(opp_side, 0.0)
        if same_qty > opp_qty + dust_qty:
            blocked.add(qid)
            continue
        sides[side] = same_qty + qty
    return blocked


def evaluate_candidate(
    *,
    name: str,
    blocked_qids: set[str],
    actions: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    residual_lots: dict[str, dict[str, Any]],
    runtime_metrics: dict[str, Any],
    capacity_metrics: dict[str, Any],
    density_thresholds: dict[str, Any],
    parameters: dict[str, Any],
) -> dict[str, Any]:
    blocked_fills = [row for row in fills if str(row.get("quote_intent_id")) in blocked_qids]
    blocked_residual = {
        qid: lot for qid, lot in residual_lots.items() if qid in blocked_qids
    }
    filled_qty_after = max(0.0, fnum(runtime_metrics.get("filled_qty")) - sum(fnum(row.get("qty")) for row in blocked_fills))
    filled_cost_after = max(0.0, fnum(runtime_metrics.get("filled_cost")) - sum(fill_cost(row) for row in blocked_fills))
    residual_qty_after = max(0.0, fnum(runtime_metrics.get("residual_qty")) - sum(residual_qty(row) for row in blocked_residual.values()))
    residual_cost_after = max(0.0, fnum(runtime_metrics.get("residual_cost")) - sum(residual_cost(row) for row in blocked_residual.values()))
    accepted_after = len([row for row in actions if str(row.get("quote_intent_id")) not in blocked_qids])
    fills_after = len([row for row in fills if str(row.get("quote_intent_id")) not in blocked_qids])
    fill_rate_after = fills_after / accepted_after if accepted_after else 0.0
    pair_qty_optimistic = fnum(capacity_metrics.get("pair_qty_redeem_notional"))
    residual_cost_share_after = residual_cost_after / filled_cost_after if filled_cost_after else 99.0
    residual_qty_share_after = residual_qty_after / filled_qty_after if filled_qty_after else 99.0
    residual_cost_to_pair_qty_after = residual_cost_after / pair_qty_optimistic if pair_qty_optimistic else 99.0
    strict_rescue_upper_bound = fnum(runtime_metrics.get("strict_rescue_closes"))

    gates = {
        "accepted": accepted_after >= fnum(density_thresholds.get("min_accepted_actions")),
        "fills": fills_after >= fnum(density_thresholds.get("min_fills")),
        "fill_rate": fill_rate_after >= fnum(density_thresholds.get("min_fill_rate")),
        "strict_rescue_upper_bound": strict_rescue_upper_bound >= fnum(density_thresholds.get("min_rescue_closes")),
        "residual_cost_share": residual_cost_share_after <= 0.15,
        "residual_qty_share": residual_qty_share_after <= 0.20,
        "residual_cost_to_pair_qty_optimistic": residual_cost_to_pair_qty_after <= 0.05,
    }
    return {
        "name": name,
        "parameters": parameters,
        "blocked_accepts": len(blocked_qids),
        "blocked_fills": len(blocked_fills),
        "blocked_residual_lots": len(blocked_residual),
        "blocked_residual_cost": sum(residual_cost(row) for row in blocked_residual.values()),
        "blocked_residual_qty": sum(residual_qty(row) for row in blocked_residual.values()),
        "accepted_after": accepted_after,
        "fills_after": fills_after,
        "fill_rate_after": fill_rate_after,
        "strict_rescue_upper_bound": strict_rescue_upper_bound,
        "filled_qty_after": filled_qty_after,
        "filled_cost_after": filled_cost_after,
        "residual_qty_after": residual_qty_after,
        "residual_cost_after": residual_cost_after,
        "residual_qty_share_after": residual_qty_share_after,
        "residual_cost_share_after": residual_cost_share_after,
        "residual_cost_to_pair_qty_after_optimistic": residual_cost_to_pair_qty_after,
        "gates": gates,
        "all_density_and_residual_proxy_gates_pass": all(gates.values()),
        "all_residual_proxy_gates_pass": gates["residual_cost_share"]
        and gates["residual_qty_share"]
        and gates["residual_cost_to_pair_qty_optimistic"],
        "density_without_rescue_pass": gates["accepted"] and gates["fills"] and gates["fill_rate"],
    }


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    best = card["best_counterfactuals"]
    lines = [
        "# Xuan Mature Tail Counterfactual Redesign",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- existing_runner_blocking_profile_ready: `{d['existing_runner_blocking_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_redesign: `{d['future_remote_allowed_by_this_redesign']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Best Local Proxies",
        "",
    ]
    for item in best:
        lines.extend(
            [
                f"### {item['name']}",
                "",
                f"- parameters: `{item['parameters']}`",
                f"- accepted_after: `{item['accepted_after']}`",
                f"- fills_after: `{item['fills_after']}`",
                f"- strict_rescue_upper_bound: `{item['strict_rescue_upper_bound']}`",
                f"- residual_cost_after: `{item['residual_cost_after']}`",
                f"- residual_cost_share_after: `{item['residual_cost_share_after']}`",
                f"- residual_cost_to_pair_qty_after_optimistic: `{item['residual_cost_to_pair_qty_after_optimistic']}`",
                f"- all_density_and_residual_proxy_gates_pass: `{item['all_density_and_residual_proxy_gates_pass']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            "",
            f"- failure_mode: `{card['interpretation']['failure_mode']}`",
            f"- next_local_target: `{card['interpretation']['next_local_target']}`",
            "",
            "## Boundary",
            "",
            "- Local/research-only.",
            "- Does not authorize another remote run.",
            "- Does not authorize cap75/150/300, deploy, restart, live orders, or shared service mutation.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    capacity = load_json(args.capacity_stage_scorecard)
    failure = load_json(args.failure_diagnosis_scorecard)
    profile = load_json(args.profile_scorecard)
    actions, fills, residual_lots = collect_rows(root)
    runtime_metrics = runtime.get("metrics", {})
    capacity_metrics = capacity.get("metrics", {})
    thresholds = density.get("thresholds", {})

    candidate_rows: list[dict[str, Any]] = []
    surplus_thresholds = [1.1, 0.9, 0.7, 0.5, 0.3, 0.0]
    completion_thresholds = [0.985, 0.98, 0.9775, 0.975, 0.9725, 0.97]
    for threshold in surplus_thresholds:
        blocked = {
            str(row.get("quote_intent_id"))
            for row in actions
            if fnum(row.get("surplus_budget_projected_unpaired_cost")) > threshold + 1e-12
        }
        candidate_rows.append(
            evaluate_candidate(
                name=f"surplus_budget_max_abs_{threshold:g}",
                blocked_qids=blocked,
                actions=actions,
                fills=fills,
                residual_lots=residual_lots,
                runtime_metrics=runtime_metrics,
                capacity_metrics=capacity_metrics,
                density_thresholds=thresholds,
                parameters={"surplus_budget_max_abs_unpaired_cost": threshold},
            )
        )
    for threshold in completion_thresholds:
        blocked = {
            str(row.get("quote_intent_id"))
            for row in actions
            if fnum(row.get("closeability_net_pair_cost")) > threshold + 1e-12
            and fnum(row.get("pair_completion_qty")) < fnum(profile.get("candidate_profile", {}).get("risk_seed_pair_completion_min_qty"), 1.25) - 1e-12
        }
        candidate_rows.append(
            evaluate_candidate(
                name=f"completion_required_above_net_cap_{threshold:g}",
                blocked_qids=blocked,
                actions=actions,
                fills=fills,
                residual_lots=residual_lots,
                runtime_metrics=runtime_metrics,
                capacity_metrics=capacity_metrics,
                density_thresholds=thresholds,
                parameters={
                    "risk_seed_pair_completion_required_above_net_cap": threshold,
                    "risk_seed_pair_completion_min_qty": fnum(
                        profile.get("candidate_profile", {}).get("risk_seed_pair_completion_min_qty"), 1.25
                    ),
                },
            )
        )
    candidate_rows.append(
        evaluate_candidate(
            name="pairing_only_when_residual_proxy",
            blocked_qids=pairing_only_proxy_blocked(actions),
            actions=actions,
            fills=fills,
            residual_lots=residual_lots,
            runtime_metrics=runtime_metrics,
            capacity_metrics=capacity_metrics,
            density_thresholds=thresholds,
            parameters={"pairing_only_when_residual": True, "proxy_uses_accepted_exposure_sequence": True},
        )
    )
    for surplus_threshold in [0.7, 0.5, 0.3, 0.0]:
        for completion_threshold in [0.9775, 0.975, 0.9725, 0.97]:
            blocked_surplus = {
                str(row.get("quote_intent_id"))
                for row in actions
                if fnum(row.get("surplus_budget_projected_unpaired_cost")) > surplus_threshold + 1e-12
            }
            blocked_completion = {
                str(row.get("quote_intent_id"))
                for row in actions
                if fnum(row.get("closeability_net_pair_cost")) > completion_threshold + 1e-12
                and fnum(row.get("pair_completion_qty"))
                < fnum(profile.get("candidate_profile", {}).get("risk_seed_pair_completion_min_qty"), 1.25) - 1e-12
            }
            candidate_rows.append(
                evaluate_candidate(
                    name=f"combined_surplus_{surplus_threshold:g}_completion_{completion_threshold:g}",
                    blocked_qids=blocked_surplus | blocked_completion,
                    actions=actions,
                    fills=fills,
                    residual_lots=residual_lots,
                    runtime_metrics=runtime_metrics,
                    capacity_metrics=capacity_metrics,
                    density_thresholds=thresholds,
                    parameters={
                        "surplus_budget_max_abs_unpaired_cost": surplus_threshold,
                        "risk_seed_pair_completion_required_above_net_cap": completion_threshold,
                    },
                )
            )

    existing_passes = [row for row in candidate_rows if row["all_density_and_residual_proxy_gates_pass"]]
    residual_passes = [row for row in candidate_rows if row["all_residual_proxy_gates_pass"]]
    best_by_residual = sorted(
        candidate_rows,
        key=lambda row: (
            row["all_residual_proxy_gates_pass"],
            -row["residual_cost_after"],
            row["accepted_after"],
            row["fills_after"],
        ),
        reverse=True,
    )[:5]
    best_balanced = sorted(
        candidate_rows,
        key=lambda row: (
            row["density_without_rescue_pass"],
            -row["residual_cost_after"],
            row["accepted_after"],
            row["fills_after"],
        ),
        reverse=True,
    )[:5]
    hard_blockers = []
    if not existing_passes:
        hard_blockers.append("no_existing_blocking_control_passes_density_and_residual_proxy")
    if fnum(runtime_metrics.get("strict_rescue_closes")) < fnum(thresholds.get("min_rescue_closes")):
        hard_blockers.append("blocking_controls_cannot_raise_strict_rescue_count")

    card = {
        "artifact": "xuan_shadow_review_mature_tail_counterfactual_redesign",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_counterfactual_redesign.py",
        "status": "KEEP_SHADOW_REVIEW_MATURE_TAIL_COUNTERFACTUAL_REDESIGN_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "capacity_stage_scorecard": str(Path(args.capacity_stage_scorecard).expanduser().resolve()),
            "failure_diagnosis_scorecard": str(Path(args.failure_diagnosis_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "capacity": capacity.get("status"),
            "failure_diagnosis": failure.get("status"),
            "profile": profile.get("status"),
        },
        "observed": {
            "accepted_actions": len(actions),
            "fills": len(fills),
            "strict_rescue_closes": fnum(runtime_metrics.get("strict_rescue_closes")),
            "residual_cost": fnum(runtime_metrics.get("residual_cost")),
            "residual_qty": fnum(runtime_metrics.get("residual_qty")),
            "density_thresholds": thresholds,
            "required_reductions": failure.get("required_reductions", {}),
        },
        "counterfactual_summary": {
            "candidate_count": len(candidate_rows),
            "existing_runner_blocking_pass_count": len(existing_passes),
            "residual_proxy_pass_count": len(residual_passes),
            "best_residual_proxy_passes": sorted(
                residual_passes,
                key=lambda row: (row["accepted_after"], row["fills_after"], -row["residual_cost_after"]),
                reverse=True,
            )[:5],
            "best_by_residual": best_by_residual,
            "best_balanced": best_balanced,
        },
        "best_counterfactuals": best_balanced[:3],
        "interpretation": {
            "failure_mode": "existing_blocking_controls_trade_density_for_residual_and_cannot_raise_rescue_density",
            "strict_rescue_upper_bound": fnum(runtime_metrics.get("strict_rescue_closes")),
            "min_strict_rescue_required": fnum(thresholds.get("min_rescue_closes")),
            "next_local_target": (
                "build a close/rescue-increasing local hypothesis before any new remote; "
                "do not spend another 1800s on stricter block-only mature-tail profiles"
            ),
            "runner_feature_gap": (
                "a future profile likely needs residual-aware completion or rescue timing that increases closes, "
                "not just lower surplus budget or lower pair-completion-required threshold"
            ),
        },
        "decision": {
            "counterfactual_redesign_ready": True,
            "existing_runner_blocking_profile_ready": bool(existing_passes),
            "hard_blockers": sorted(set(hard_blockers)),
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_redesign": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": "local_close_or_rescue_increasing_profile_hypothesis_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--capacity-stage-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--failure-diagnosis-scorecard", type=Path, default=DEFAULT_FAILURE)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
