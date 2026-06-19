#!/usr/bin/env python3
"""Local rescue-increase hypothesis for the failed mature-tail cap25 run.

This artifact checks whether existing source/economic-preserving rescue knobs
can plausibly lift strict_rescue density after the mature-tail failure. It is
local-only and does not authorize another remote run.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_EVENT_DIAG = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_event_diagnostics.json")
DEFAULT_COUNTERFACTUAL = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_counterfactual_redesign_20260526T2148Z.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_rescue_increase_hypothesis_20260526T2148Z.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def fee_per_share(px: float, rate: float) -> float:
    price = min(max(px, 0.0), 1.0)
    return rate * price * (1.0 - price)


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def iter_events(root: Path) -> Any:
    for path in sorted(root.glob("*.events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def age_relaxation_proxy(root: Path, taker_fee_rate: float, net_cap: float, min_pair_pnl_delta: float) -> list[dict[str, Any]]:
    rows = []
    for age_ms in [1000, 2000, 3000, 5000, 10000, 15000, 30000]:
        qids: set[str] = set()
        econ_pass_qids: set[str] = set()
        event_count = 0
        for event in iter_events(root):
            if event.get("kind") != "strict_rescue_block":
                continue
            if event.get("block_reason") != "strict_rescue_lot_age_or_min_cost":
                continue
            lot_age_ms = fnum(event.get("lot_age_ms"))
            if lot_age_ms < age_ms:
                continue
            qid = str(event.get("oldest_quote_intent_id") or "")
            if qid:
                qids.add(qid)
            held_px = fnum(event.get("oldest_lot_px"))
            raw_comp_ask = fnum(event.get("raw_comp_ask"))
            qty = fnum(event.get("oldest_lot_qty"))
            net_pair_cost = held_px + raw_comp_ask + fee_per_share(raw_comp_ask, taker_fee_rate)
            pair_pnl_delta = (1.0 - net_pair_cost) * qty
            if qid and net_pair_cost <= net_cap + 1e-12 and pair_pnl_delta >= min_pair_pnl_delta - 1e-12:
                econ_pass_qids.add(qid)
            event_count += 1
        rows.append(
            {
                "salvage_age_s": age_ms / 1000.0,
                "age_eligible_event_count": event_count,
                "age_eligible_qids": len(qids),
                "econ_pass_qids_under_existing_caps": len(econ_pass_qids),
                "sample_econ_pass_qids": sorted(econ_pass_qids)[:10],
            }
        )
    return rows


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    lines = [
        "# Xuan Mature Tail Rescue-Increase Hypothesis",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- rescue_increase_profile_ready: `{d['rescue_increase_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_hypothesis: `{d['future_remote_allowed_by_this_hypothesis']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Rescue Blocks",
        "",
        f"- strict_rescue_closes: `{card['observed']['strict_rescue_closes']}`",
        f"- min_rescue_closes: `{card['observed']['min_rescue_closes']}`",
        f"- strict_rescue_block_reasons: `{card['rescue_block_reasons']}`",
        "",
        "## Age Relaxation Proxy",
        "",
    ]
    for row in card["age_relaxation_proxy"]:
        lines.append(
            f"- salvage_age_s `{row['salvage_age_s']}`: age_qids `{row['age_eligible_qids']}`, "
            f"econ_pass_qids `{row['econ_pass_qids_under_existing_caps']}`"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            f"- hypothesis: `{card['interpretation']['hypothesis']}`",
            f"- next_local_target: `{card['interpretation']['next_local_target']}`",
            "",
            "## Boundary",
            "",
            "- Local/research-only.",
            "- Does not authorize a remote run.",
            "- Does not authorize economic/source relaxation, cap75, deploy, restart, or live orders.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    event_diag = load_json(args.event_diagnostics_scorecard)
    counterfactual = load_json(args.counterfactual_redesign_scorecard)
    profile = load_json(args.profile_scorecard)
    candidate_profile = profile.get("candidate_profile", {})
    runtime_metrics = runtime.get("metrics", {})
    thresholds = density.get("thresholds", {})
    taker_fee_rate = fnum(candidate_profile.get("taker_fee_rate"), 0.07)
    strict_rescue_surplus_net_cap = fnum(candidate_profile.get("strict_rescue_surplus_net_cap"), 1.02)
    strict_rescue_min_pair_pnl_after = fnum(candidate_profile.get("strict_rescue_min_pair_pnl_after"), 0.02)
    age_proxy = age_relaxation_proxy(
        root=root,
        taker_fee_rate=taker_fee_rate,
        net_cap=strict_rescue_surplus_net_cap,
        min_pair_pnl_delta=strict_rescue_min_pair_pnl_after,
    )
    strict_rescue_closes = fnum(runtime_metrics.get("strict_rescue_closes"))
    min_rescue = fnum(thresholds.get("min_rescue_closes"))
    rescue_block_reasons = event_diag.get("strict_rescue_block_reasons", {})
    source_clean = fnum(runtime_metrics.get("strict_rescue_source_blocks")) == 0
    source_l1_clean = fnum(runtime_metrics.get("rescue_l1_age_ms_max")) <= fnum(thresholds.get("max_rescue_l1_age_ms"), 50.0)
    any_age_econ_pass = any(row["econ_pass_qids_under_existing_caps"] > 0 for row in age_proxy)

    hard_blockers = []
    if strict_rescue_closes < min_rescue:
        hard_blockers.append("strict_rescue_count_below_density_min")
    if not any_age_econ_pass:
        hard_blockers.append("lower_salvage_age_has_no_econ_pass_qids_under_existing_caps")
    if not source_clean:
        hard_blockers.append("strict_rescue_source_not_clean")
    if not source_l1_clean:
        hard_blockers.append("strict_rescue_l1_age_not_clean")
    if counterfactual.get("decision", {}).get("existing_runner_blocking_profile_ready") is not False:
        hard_blockers.append("counterfactual_blocking_result_unexpected")

    rescue_increase_profile_ready = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_mature_tail_rescue_increase_hypothesis",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_rescue_increase_hypothesis.py",
        "status": "KEEP_SHADOW_REVIEW_MATURE_TAIL_RESCUE_INCREASE_HYPOTHESIS_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "event_diagnostics_scorecard": str(Path(args.event_diagnostics_scorecard).expanduser().resolve()),
            "counterfactual_redesign_scorecard": str(Path(args.counterfactual_redesign_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "counterfactual_redesign": counterfactual.get("status"),
            "profile": profile.get("status"),
        },
        "observed": {
            "strict_rescue_closes": strict_rescue_closes,
            "min_rescue_closes": min_rescue,
            "strict_rescue_source_blocks": fnum(runtime_metrics.get("strict_rescue_source_blocks")),
            "rescue_l1_age_ms_max": fnum(runtime_metrics.get("rescue_l1_age_ms_max")),
            "strict_rescue_close_net_pair_cost_max": event_diag.get("strict_rescue_close_net_pair_cost", {}).get("max"),
            "strict_rescue_close_projected_pair_pnl_after_min": event_diag.get(
                "strict_rescue_close_projected_pair_pnl_after", {}
            ).get("min"),
        },
        "rescue_block_reasons": rescue_block_reasons,
        "rescue_block_diagnostics": event_diag.get("strict_rescue_block_diagnostics", {}),
        "age_relaxation_proxy": age_proxy,
        "interpretation": {
            "hypothesis": (
                "existing age/source-preserving rescue knobs are not enough; written age-block samples have "
                "zero qids passing current net-cap and pair-pnl economics"
            ),
            "dominant_blocker": "strict_rescue_surplus_net_cap" if rescue_block_reasons else None,
            "source_path_clean": source_clean and source_l1_clean,
            "economic_relaxation_disallowed": True,
            "next_local_target": (
                "design residual-aware pair-completion or close generation that creates more economically valid closes "
                "before another remote; do not relax strict_rescue_surplus_net_cap or source gates"
            ),
        },
        "decision": {
            "rescue_increase_hypothesis_ready": True,
            "rescue_increase_profile_ready": rescue_increase_profile_ready,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_hypothesis": False,
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
            "next_action": "local_residual_aware_pair_completion_or_close_generation_design_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAG)
    parser.add_argument("--counterfactual-redesign-scorecard", type=Path, default=DEFAULT_COUNTERFACTUAL)
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
