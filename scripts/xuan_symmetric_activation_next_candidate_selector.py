#!/usr/bin/env python3
"""Select the next symmetric-activation action from current-worktree evidence only."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_next_candidate_selector"
DEFAULT_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_shadow_review_edge0075_completion_20260525T133305Z/"
    "manifest.json"
)
DEFAULT_STARTUP = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_shadow_review_edge0075_driver_20260525T120202Z/"
    "manifest.json"
)
DEFAULT_GAP_AUDIT = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_edge075_residual_gap_audit_20260525T142535Z/"
    "manifest.json"
)
DEFAULT_TAIL_JOIN_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_projected_residual_tail_join_summary_smoke_20260525T145535Z/"
    "scorer/manifest.json"
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
BASE_ACCEPTANCE_GATES = {
    "candidates_min": 100,
    "net_pair_cost_p90_max": 1.0,
    "residual_qty_share_max": 0.15,
    "residual_cost_share_max": 0.20,
    "pair_tail_loss_share_max": 0.05,
    "material_residual_lots_max": 0,
    "fee_adjusted_pair_pnl_proxy_positive": True,
}


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


def normalized_metrics(completion: dict[str, Any]) -> dict[str, Any]:
    adapter = completion.get("acceptance_adapter")
    shadow_gate = adapter.get("shadow_acceptance_gate") if isinstance(adapter, dict) else {}
    metrics = shadow_gate.get("metrics") if isinstance(shadow_gate, dict) else {}
    trading = completion.get("trading_metrics") if isinstance(completion.get("trading_metrics"), dict) else {}
    out = dict(metrics if isinstance(metrics, dict) else {})
    for key, value in trading.items():
        out.setdefault(key, value)
    return out


def residual_only_failure(completion: dict[str, Any]) -> bool:
    gate = ((completion.get("acceptance_adapter") or {}).get("shadow_acceptance_gate") or {})
    blockers = set(gate.get("blockers") or completion.get("promotion_gate", {}).get("hard_blockers") or [])
    normalized = {
        blocker.replace("normal_shadow_acceptance_passed_false", "")
        for blocker in blockers
        if blocker != "normal_shadow_acceptance_passed_false"
    }
    normalized.discard("")
    return normalized and normalized <= {"residual_qty_share_above_max", "residual_cost_share_above_max"}


def exact_command(startup: dict[str, Any]) -> str:
    cfg = startup.get("run_config") if isinstance(startup.get("run_config"), dict) else {}
    env = cfg.get("environment") if isinstance(cfg.get("environment"), dict) else {}
    repo = cfg.get("repo", "/srv/pm_as_ofi/repo")
    shared_ingress_root = env.get("PM_SHARED_INGRESS_ROOT", "/srv/pm_as_ofi/shared-ingress-main")
    round_offsets = ",".join(str(x) for x in cfg.get("round_offsets", list(range(26))))
    flags = list(cfg.get("enabled_summary_flags") or [])
    extra_flag = "--symmetric-activation-projected-residual-tail-join-event-lite-summary"
    if extra_flag not in flags:
        flags.append(extra_flag)
    flag_text = " ".join(flags)
    return (
        "PM_DRY_RUN=true "
        f"PM_SHARED_INGRESS_ROOT={shared_ingress_root} "
        "PM_SHARED_INGRESS_ROLE=client "
        f"python3 {repo}/tools/xuan_dplus_passive_passive_shadow_runner.py "
        f"--repo {repo} "
        f"--shared-ingress-root {shared_ingress_root} "
        f"--prefix {cfg.get('prefix', 'btc-updown-5m')} "
        f"--round-offsets {round_offsets} "
        f"--duration-s {int(as_float(cfg.get('duration_s'), 3600))} "
        "--output-dir /srv/pm_as_ofi/xuan_research_runs/"
        "xuan_research_symmetric_activation_review_edge0075_actoppseen200_projected_tail_APPROVAL_TS/output "
        f"--edge {as_float(cfg.get('edge'), 0.075):g} "
        f"--target-qty {as_float(cfg.get('target_qty'), 5.0):g} "
        f"--max-open-cost {as_float(cfg.get('max_open_cost'), 80.0):g} "
        f"--imbalance-qty-cap {as_float(cfg.get('imbalance_qty_cap'), 1.25):g} "
        f"--seed-offset-max-s {int(as_float(cfg.get('seed_offset_max_s'), 300))} "
        f"--activation-mode {cfg.get('activation_mode', 'opp_seen')} "
        f"--activation-window-s {as_float(cfg.get('activation_window_s'), 20.0):g} "
        f"{flag_text}"
    )


def build_proposal(startup: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    cfg = startup.get("run_config") if isinstance(startup.get("run_config"), dict) else {}
    flags = list(cfg.get("enabled_summary_flags") or [])
    projected_flag = "--symmetric-activation-projected-residual-tail-join-event-lite-summary"
    if projected_flag not in flags:
        flags.append(projected_flag)
    return {
        "approval_required_in_thread": True,
        "no_order_diagnostic_allowed_now": False,
        "exact_bounded_run_name": (
            "xuan_research_symmetric_activation_review_edge0075_actoppseen200_projected_tail_APPROVAL_TS"
        ),
        "purpose": (
            "Re-run the same bounded edge0.075/window20 no-order diagnostic with the newly added "
            "projected-residual tail-join summary so residual failure can be attributed to legal pre-action buckets."
        ),
        "run_config": {
            "PM_DRY_RUN": True,
            "prefix": cfg.get("prefix", "btc-updown-5m"),
            "round_offsets": cfg.get("round_offsets", list(range(26))),
            "duration_s": int(as_float(cfg.get("duration_s"), 3600)),
            "edge": as_float(cfg.get("edge"), 0.075),
            "target_qty": as_float(cfg.get("target_qty"), 5.0),
            "max_open_cost": as_float(cfg.get("max_open_cost"), 80.0),
            "imbalance_qty_cap": as_float(cfg.get("imbalance_qty_cap"), 1.25),
            "seed_offset_max_s": int(as_float(cfg.get("seed_offset_max_s"), 300)),
            "activation_mode": cfg.get("activation_mode", "opp_seen"),
            "activation_window_s": as_float(cfg.get("activation_window_s"), 20.0),
            "enabled_summary_flags": flags,
        },
        "acceptance_gates": {
            **BASE_ACCEPTANCE_GATES,
            "symmetric_activation_summary_v1_present": True,
            "symmetric_activation_tail_attribution_summary_v1_present": True,
            "symmetric_activation_projected_residual_tail_join_summary_v1_present": True,
            "nonzero_admitted_and_blocked_activation_denominators": True,
            "source_sequence_coverage_present": True,
            "aggregate_parity": True,
            "research_ranking_and_promotion_gate_separated": True,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "mechanism_resolution_gates_if_normal_budget_still_fails": {
            "projected_residual_tail_join_summary_present": True,
            "projected_residual_gt20_denominator_min": 20,
            "projected_residual_gt20_residual_cost_share_min": 0.50,
            "allowed_outcome_if_met": (
                "KEEP only as a local instrumentation/spec target for a future projected residual guard; "
                "still not deployable and not promotion evidence."
            ),
            "discard_if_not_met": (
                "DISCARD symmetric activation edge0.075/window20 if residual remains broad after projected-tail join."
            ),
        },
        "latest_failed_metrics_for_context": metrics,
        "exact_remote_command_if_later_approved": exact_command(startup),
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    completion, completion_error = safe_load(args.completion_manifest, root)
    startup, startup_error = safe_load(args.startup_manifest, root)
    gap_audit, gap_error = safe_load(args.gap_audit_manifest, root)
    tail_join_scorer, scorer_error = safe_load(args.tail_join_scorer_manifest, root)
    blockers: list[str] = []
    for name, error in (
        ("completion", completion_error),
        ("startup", startup_error),
        ("gap_audit", gap_error),
        ("tail_join_scorer", scorer_error),
    ):
        if error:
            blockers.append(f"{name}:{error}")

    completion = completion or {}
    startup = startup or {}
    gap_audit = gap_audit or {}
    tail_join_scorer = tail_join_scorer or {}
    metrics = normalized_metrics(completion)
    ranking = completion.get("research_ranking") if isinstance(completion.get("research_ranking"), dict) else {}
    economic_positive = bool(ranking.get("economic_pnl_positive")) or as_float(
        metrics.get("fee_adjusted_pair_pnl_proxy")
    ) > 0.0
    sample_ok = as_float(metrics.get("candidates")) >= BASE_ACCEPTANCE_GATES["candidates_min"]
    p90_ok = as_float(metrics.get("net_pair_cost_p90"), 99.0) <= BASE_ACCEPTANCE_GATES["net_pair_cost_p90_max"]
    pair_tail_ok = as_float(metrics.get("pair_tail_loss_share"), 99.0) <= BASE_ACCEPTANCE_GATES[
        "pair_tail_loss_share_max"
    ]
    residual_failed = not (
        as_float(metrics.get("residual_qty_share")) <= BASE_ACCEPTANCE_GATES["residual_qty_share_max"]
        and as_float(metrics.get("residual_cost_share")) <= BASE_ACCEPTANCE_GATES["residual_cost_share_max"]
    )
    gap_keep = (
        gap_audit.get("decision_label")
        == "KEEP_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_PROJECTED_RESIDUAL_JOIN_TARGET_READY"
    )
    scorer_keep = (
        tail_join_scorer.get("decision_label")
        == "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_RESIDUAL_TAIL_JOIN_SUMMARY_SCORER_READY"
    )
    completion_expected = (
        completion.get("decision_label")
        == "UNKNOWN_SYMMETRIC_ACTIVATION_EDGE0075_NO_ORDER_REVIEW_RESIDUAL_RISK_BUDGET_FAIL"
    )

    if not completion_expected:
        blockers.append("completion_not_latest_edge075_residual_unknown")
    if not economic_positive:
        decision = "DISCARD"
        blockers.append("economic_pnl_not_positive")
    elif not sample_ok or not p90_ok or not pair_tail_ok:
        decision = "DISCARD"
        blockers.append("base_sample_p90_or_pair_tail_not_good_enough_for_followup")
    elif not residual_failed or not residual_only_failure(completion):
        decision = "DISCARD"
        blockers.append("failure_not_isolated_to_residual_budget")
    elif not gap_keep:
        decision = "UNKNOWN"
        blockers.append("gap_audit_not_projected_residual_join_target")
    elif not scorer_keep:
        decision = "UNKNOWN"
        blockers.append("projected_residual_tail_join_scorer_not_ready")
    elif blockers:
        decision = "UNKNOWN"
    else:
        decision = "KEEP"

    decision_label = {
        "KEEP": "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_TAIL_JOIN_EXACT_DIAGNOSTIC_PROPOSAL_READY",
        "UNKNOWN": "UNKNOWN_SYMMETRIC_ACTIVATION_NEXT_CANDIDATE_SELECTOR_INPUTS_INSUFFICIENT",
        "DISCARD": "DISCARD_SYMMETRIC_ACTIVATION_EDGE075_WINDOW20_FOLLOWUP_NOT_JUSTIFIED",
    }[decision]

    proposal = build_proposal(startup, metrics) if decision == "KEEP" else None
    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "source_inputs": {
            "completion_manifest": str(args.completion_manifest),
            "completion_error": completion_error,
            "startup_manifest": str(args.startup_manifest),
            "startup_error": startup_error,
            "gap_audit_manifest": str(args.gap_audit_manifest),
            "gap_audit_error": gap_error,
            "tail_join_scorer_manifest": str(args.tail_join_scorer_manifest),
            "tail_join_scorer_error": scorer_error,
        },
        "observed_failure": {
            "latest_decision_label": completion.get("decision_label"),
            "metrics": metrics,
            "economic_positive": economic_positive,
            "sample_ok": sample_ok,
            "net_pair_cost_p90_ok": p90_ok,
            "pair_tail_loss_share_ok": pair_tail_ok,
            "residual_budget_failed": residual_failed,
            "residual_only_failure": residual_only_failure(completion),
        },
        "selector_checks": {
            "gap_audit_projected_residual_join_target_ready": gap_keep,
            "projected_residual_tail_join_scorer_ready": scorer_keep,
            "real_pullback_contains_projected_tail_join_summary": False,
            "next_run_requires_explicit_approval": True,
        },
        "proposal": proposal,
        "blockers": blockers,
        "next_executable_action": (
            "WAIT for exact in-thread approval before any bounded no-order diagnostic. If approved, run only the "
            "proposal named here; otherwise continue local-only selector/pivot work."
            if decision == "KEEP"
            else (
                "Pivot/discard symmetric activation edge0.075/window20 unless a future safe pullback already contains "
                "projected residual tail-join summary."
                if decision == "DISCARD"
                else "Wait for complete local inputs or pivot; do not start no-order diagnostics automatically."
            )
        ),
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "proposal_ready_requires_user_approval": decision == "KEEP",
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_ONLY_SELECTOR_NOT_PROMOTION_EVIDENCE",
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
    parser.add_argument("--completion-manifest", type=Path, default=DEFAULT_COMPLETION)
    parser.add_argument("--startup-manifest", type=Path, default=DEFAULT_STARTUP)
    parser.add_argument("--gap-audit-manifest", type=Path, default=DEFAULT_GAP_AUDIT)
    parser.add_argument("--tail-join-scorer-manifest", type=Path, default=DEFAULT_TAIL_JOIN_SCORER)
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
