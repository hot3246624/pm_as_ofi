#!/usr/bin/env python3
"""Review the selected cap102 symmetric activation lead and draft a bounded no-order proposal.

This local-only review compares the selected strict-cache cap102 candidate against
the previous symmetric activation no-order parameters (edge=0.07,
activation_window_s=7.5) and emits the exact bounded diagnostic command/config
that would be required only if the user later approves it.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_strategy_discovery_symmetric_activation_no_order_proposal_review"
DEFAULT_GRID_REVIEW = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_discovery_grid_expansion_review_20260525T063000Z/"
    "manifest.json"
)
DEFAULT_PRIOR_DIAGNOSTIC = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_shadow_review_driver_20260525T011338Z/"
    "manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)
BASELINE_EDGE = 0.07
BASELINE_ACTIVATION_WINDOW_S = 7.5
BASELINE_LEAK_RATE = 0.005
BASELINE_MIN_OPP_COUNT = 1


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def path_is_forbidden(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    if path_is_forbidden(path):
        return None, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return None, "outside_current_worktree"
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def round_or_none(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def comparable_baseline(cap102_manifest: dict[str, Any] | None) -> dict[str, Any] | None:
    rows = (cap102_manifest or {}).get("qualified") or []
    if not isinstance(rows, list):
        return None
    exact = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if (
            as_float(row.get("edge")) == BASELINE_EDGE
            and as_float(row.get("activation_window_s")) == BASELINE_ACTIVATION_WINDOW_S
            and as_float(row.get("leak_rate")) == BASELINE_LEAK_RATE
            and as_float(row.get("min_opp_count")) == BASELINE_MIN_OPP_COUNT
        ):
            exact.append(row)
    if exact:
        return exact[0]
    return None


def selected_candidate(grid_review: dict[str, Any] | None) -> dict[str, Any] | None:
    value = ((grid_review or {}).get("research_ranking") or {}).get("selected_local_candidate")
    return value if isinstance(value, dict) else None


def metric_delta(selected: dict[str, Any], baseline: dict[str, Any], key: str) -> float | None:
    if key not in selected or key not in baseline:
        return None
    return as_float(selected.get(key)) - as_float(baseline.get(key))


def comparison_summary(selected: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "selected": selected,
        "baseline_previous_no_order_params": baseline,
        "delta_vs_previous_no_order_params": {
            "edge": round_or_none(metric_delta(selected, baseline, "edge")),
            "activation_window_s": round_or_none(metric_delta(selected, baseline, "activation_window_s")),
            "covered_pair_actions": round_or_none(metric_delta(selected, baseline, "covered_pair_actions")),
            "holdout_pair_actions": round_or_none(metric_delta(selected, baseline, "holdout_pair_actions")),
            "covered_residual_qty_rate": round_or_none(metric_delta(selected, baseline, "covered_residual_qty_rate")),
            "holdout_residual_qty_rate": round_or_none(metric_delta(selected, baseline, "holdout_residual_qty_rate")),
            "covered_worst_net_fee_after": round_or_none(metric_delta(selected, baseline, "covered_worst_net_fee_after")),
            "holdout_worst_net_fee_after": round_or_none(metric_delta(selected, baseline, "holdout_worst_net_fee_after")),
        },
    }


def build_remote_command(prior_run_config: dict[str, Any]) -> str:
    round_offsets = str(prior_run_config.get("round_offsets", "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25"))
    duration_s = int(as_float(prior_run_config.get("duration_s"), 3600))
    target_qty = as_float(prior_run_config.get("target_qty"), 5.0)
    max_open_cost = as_float(prior_run_config.get("max_open_cost"), 80.0)
    imbalance_qty_cap = as_float(prior_run_config.get("imbalance_qty_cap"), 1.25)
    seed_offset_max_s = int(as_float(prior_run_config.get("seed_offset_max_s"), 300))
    prefix = prior_run_config.get("prefix", "btc-updown-5m")
    repo = prior_run_config.get("repo", "/srv/pm_as_ofi/repo")
    shared_ingress_root = prior_run_config.get("PM_SHARED_INGRESS_ROOT", "/srv/pm_as_ofi/shared-ingress-main")
    return (
        "PM_DRY_RUN=true "
        f"PM_SHARED_INGRESS_ROOT={shared_ingress_root} "
        "PM_SHARED_INGRESS_ROLE=client "
        f"python3 {repo}/tools/xuan_dplus_passive_passive_shadow_runner.py "
        f"--repo {repo} "
        f"--shared-ingress-root {shared_ingress_root} "
        f"--prefix {prefix} "
        f"--round-offsets {round_offsets} "
        f"--duration-s {duration_s} "
        "--output-dir /srv/pm_as_ofi/xuan_research_runs/"
        "xuan_research_symmetric_activation_review_edge0075_actoppseen200_APPROVAL_TS/output "
        "--edge 0.075 "
        f"--target-qty {target_qty:g} "
        f"--max-open-cost {max_open_cost:g} "
        f"--imbalance-qty-cap {imbalance_qty_cap:g} "
        f"--seed-offset-max-s {seed_offset_max_s} "
        "--activation-mode opp_seen "
        "--activation-window-s 20 "
        "--event-lite-summary "
        "--pair-source-event-lite-summary "
        "--source-link-transition-event-lite-summary "
        "--source-link-residual-tail-exemplars-event-lite-summary "
        "--symmetric-activation-event-lite-summary "
        "--symmetric-activation-tail-attribution-event-lite-summary"
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    grid_review, grid_error = safe_load(args.grid_review_manifest, root)
    prior_diagnostic, prior_error = safe_load(args.prior_diagnostic_manifest, root)
    cap102_manifest_path = Path(
        str((((grid_review or {}).get("source_inputs") or {}).get("cap102_manifest")) or "")
    )
    cap102_manifest, cap102_error = (
        safe_load(cap102_manifest_path, root) if str(cap102_manifest_path) else (None, "missing")
    )
    blockers: list[str] = []
    if grid_error:
        blockers.append(f"grid_review:{grid_error}")
    if prior_error:
        blockers.append(f"prior_diagnostic:{prior_error}")
    if cap102_error:
        blockers.append(f"cap102_manifest:{cap102_error}")

    selected = selected_candidate(grid_review)
    baseline = comparable_baseline(cap102_manifest)
    prior_run_config = (prior_diagnostic or {}).get("run_config") if isinstance(prior_diagnostic, dict) else {}
    prior_disabled = (prior_diagnostic or {}).get("disabled") if isinstance(prior_diagnostic, dict) else {}

    if (grid_review or {}).get("decision_label") != "KEEP_STRATEGY_DISCOVERY_GRID_EXPANSION_CAP102_LEAD_READY":
        blockers.append("grid_review_not_keep_ready")
    if not selected:
        blockers.append("selected_cap102_candidate_missing")
        selected = {}
    if not baseline:
        blockers.append("baseline_edge007_window75_missing")
        baseline = {}
    if not isinstance(prior_run_config, dict) or not prior_run_config:
        blockers.append("prior_run_config_missing")
        prior_run_config = {}
    if as_float(prior_run_config.get("edge")) != BASELINE_EDGE:
        blockers.append("prior_diagnostic_edge_mismatch")
    if as_float(prior_run_config.get("activation_window_s")) != BASELINE_ACTIVATION_WINDOW_S:
        blockers.append("prior_diagnostic_activation_window_mismatch")
    if prior_run_config.get("activation_mode") != "opp_seen":
        blockers.append("prior_diagnostic_activation_mode_mismatch")
    if prior_run_config.get("symmetric_activation_event_lite_summary") is not True:
        blockers.append("prior_diagnostic_missing_symmetric_activation_summary_flag")

    decision = "KEEP" if not blockers else "UNKNOWN"
    decision_label = (
        "KEEP_SELECTED_CAP102_CANDIDATE_NO_ORDER_PROPOSAL_LOCAL_ONLY_READY"
        if decision == "KEEP"
        else "UNKNOWN_SELECTED_CAP102_CANDIDATE_NO_ORDER_PROPOSAL_INPUTS_INSUFFICIENT"
    )

    compare = comparison_summary(selected, baseline) if selected and baseline else None
    proposed_command = build_remote_command(prior_run_config) if prior_run_config else None
    proposal = {
        "approval_required_in_thread": True,
        "no_order_diagnostic_allowed_now": False,
        "exact_bounded_run_name": "xuan_research_symmetric_activation_review_edge0075_actoppseen200_APPROVAL_TS",
        "bounded_duration_s": int(as_float(prior_run_config.get("duration_s"), 3600)),
        "proposal_basis": (
            "Reuse the previously approved bounded symmetric-activation no-order review shape, "
            "but replace edge=0.07/window=7.5 with the selected cap102 train/holdout lead "
            "edge=0.075/window=20.0 while keeping PM_DRY_RUN=true and all trading-rule families disabled."
        ),
        "preconditions_if_later_approved": [
            "user gives exact approval in this thread for this specific bounded diagnostic only",
            "no active xuan-research runner exists before launch",
            "bounded preflight re-confirms shared-ingress metadata health without starting any second runner",
            "allowlisted pullback remains limited to output/manifest.json, output/aggregate_report.json, output/*.summary.json, stdout.log, stderr.log, runner.pid, and small sanity logs",
            "do not pull events JSONL, raw/replay SQLite, collector/raw/replay, full completion store, or other non-allowlisted artifacts",
        ],
        "disabled_families": prior_disabled,
        "environment": {
            "PM_DRY_RUN": "true",
            "PM_SHARED_INGRESS_ROOT": prior_run_config.get("PM_SHARED_INGRESS_ROOT"),
            "PM_SHARED_INGRESS_ROLE": prior_run_config.get("PM_SHARED_INGRESS_ROLE"),
        },
        "selected_candidate_bindings": {
            "edge": selected.get("edge"),
            "activation_mode": "opp_seen",
            "activation_window_s": selected.get("activation_window_s"),
            "leak_rate": selected.get("leak_rate"),
            "min_opp_count": selected.get("min_opp_count"),
        },
        "exact_remote_command_if_later_approved": proposed_command,
        "post_run_local_only_evaluation": [
            "pull back allowlisted outputs only",
            "run scripts/xuan_symmetric_activation_summary_scorer.py on the pulled back summary",
            "run scripts/xuan_symmetric_activation_tail_attribution_summary_scorer.py on the pulled back summary",
            "run scripts/xuan_symmetric_activation_acceptance_adapter_spec.py only if the allowlisted pullback already contains the required summary files",
            "report research_ranking and promotion_gate separately",
            "keep promotion_gate.passed=false unless a later accepted artifact proves otherwise",
        ],
    }

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "source_inputs": {
            "grid_review_manifest": str(args.grid_review_manifest),
            "grid_review_error": grid_error,
            "prior_diagnostic_manifest": str(args.prior_diagnostic_manifest),
            "prior_diagnostic_error": prior_error,
            "cap102_manifest": str(cap102_manifest_path) if str(cap102_manifest_path) else None,
            "cap102_manifest_error": cap102_error,
        },
        "comparison": compare,
        "prior_real_no_order_status": {
            "status": "UNKNOWN_RISK_BUDGET_FAIL_REMAINS_BLOCKING",
            "basis": (
                "The previous real symmetric activation no-order review remained diagnostic-only and must not be "
                "treated as private truth, deployable evidence, or promotion evidence."
            ),
            "prior_run_config": prior_run_config,
        },
        "proposal": proposal,
        "blockers": blockers,
        "next_executable_action": (
            "WAIT for exact in-thread approval before any bounded no-order diagnostic. Until then, treat this as a "
            "local-only comparison/proposal artifact and do not start shadow/SSH/live activity."
            if decision == "KEEP"
            else "WAIT/BLOCKED: missing current-worktree inputs required to name an exact bounded diagnostic."
        ),
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "selected_local_candidate": selected,
            "baseline_previous_no_order_params": baseline,
            "comparison_ready": decision == "KEEP",
            "no_order_diagnostic_allowed": False,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_ONLY_PROPOSAL_NOT_PROMOTION_EVIDENCE",
        },
        "scope": {
            "current_worktree_only": True,
            "local_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grid-review-manifest", type=Path, default=DEFAULT_GRID_REVIEW)
    parser.add_argument("--prior-diagnostic-manifest", type=Path, default=DEFAULT_PRIOR_DIAGNOSTIC)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
