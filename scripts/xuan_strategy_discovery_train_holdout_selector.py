#!/usr/bin/env python3
"""Select the next local train/holdout strategy discovery lane.

This selector keeps ce25/b55 public-profile work as feature priors only, then
chooses a concrete local discovery lane from current worktree manifests. It
does not fetch data, start services, use SSH/shadow/live, or read raw/replay
stores.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_strategy_discovery_train_holdout_selector"
CONTRACT_NAME = "xuan_strategy_discovery_train_holdout_contract_v1"
DEFAULT_STRATEGY_RESET = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_reset_next_lane_selector_20260524T162937Z/"
    "manifest.json"
)
DEFAULT_CE25_GUARD_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_projected_pair_cost_residual_guard_spec_20260524T104842Z/"
    "manifest.json"
)
DEFAULT_CE25_GUARD_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_projected_guard_summary_scorer_20260524T115000Z/"
    "manifest.json"
)
DEFAULT_SYMMETRIC_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_shadow_review_completion_20260525T022531Z/"
    "manifest.json"
)
DEFAULT_TAIL_GAP = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_tail_gap_audit_20260525T030401Z/"
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


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_is_forbidden(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def as_bool(value: Any) -> bool:
    return bool(value)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def selected_strategy_candidate(strategy_reset: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(strategy_reset, dict):
        return None
    line = strategy_reset.get("selected_strategy_line")
    if not isinstance(line, dict):
        return None
    selected = line.get("selected")
    return selected if isinstance(selected, dict) else None


def summarize_ce25_prior(spec: dict[str, Any] | None, scorer: dict[str, Any] | None) -> dict[str, Any]:
    spec_ready = (
        isinstance(spec, dict)
        and spec.get("decision_label") == "KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_READY"
        and not spec.get("promotion_gate", {}).get("passed")
    )
    scorer_ready = (
        isinstance(scorer, dict)
        and scorer.get("decision_label") == "KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_READY"
        and not scorer.get("promotion_gate", {}).get("passed")
    )
    ranking = spec.get("research_ranking", {}) if isinstance(spec, dict) else {}
    return {
        "status": "FEATURE_PRIOR_READY_NOT_EVIDENCE" if spec_ready and scorer_ready else "UNKNOWN_CE25_PRIOR_INCOMPLETE",
        "spec_ready": spec_ready,
        "summary_scorer_ready": scorer_ready,
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "starter_rule": ranking.get("starter_rule"),
        "core_rule": ranking.get("core_rule"),
        "hard_kill": ranking.get("hard_kill"),
        "use_in_discovery": [
            "projected_pair_cost",
            "projected_residual_rate",
            "late-window completion-vs-initiation policy",
            "ETH/SOL/BTC 5m/15m as a feature prior only",
        ],
        "must_not_use_as_live_criteria": [
            "realized public-profile pair_cost bucket",
            "single-day ce25 public PnL",
            "fixture projected guard decisions",
            "public-profile static price/asset/timeframe filter by itself",
        ],
    }


def summarize_symmetric_seed(
    strategy_reset: dict[str, Any] | None,
    completion: dict[str, Any] | None,
    tail_gap: dict[str, Any] | None,
) -> dict[str, Any]:
    selected = selected_strategy_candidate(strategy_reset)
    reset_ready = (
        isinstance(strategy_reset, dict)
        and strategy_reset.get("decision_label") == "KEEP_SYMMETRIC_ACTIVATION_NEXT_STRATEGY_SELECTED_RESEARCH_ONLY"
    )
    seed_ready = bool(
        selected
        and as_int(selected.get("covered_pair_actions")) >= 1000
        and as_int(selected.get("holdout_pair_actions")) >= 200
        and as_float(selected.get("covered_worst_net_fee_after")) > 0.0
        and as_float(selected.get("holdout_worst_net_fee_after")) > 0.0
    )
    completion_unknown = (
        isinstance(completion, dict)
        and completion.get("decision_label") == "UNKNOWN_SYMMETRIC_ACTIVATION_NO_ORDER_REVIEW_RISK_BUDGET_FAIL"
    )
    tail_gap_ready = (
        isinstance(tail_gap, dict)
        and tail_gap.get("decision_label") == "KEEP_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INSTRUMENTATION_TARGET_READY"
    )
    return {
        "status": "STRICT_CACHE_DISCOVERY_SEED_READY" if reset_ready and seed_ready else "UNKNOWN_STRICT_CACHE_SEED_INCOMPLETE",
        "selected": selected,
        "current_no_order_result": "UNKNOWN_RISK_BUDGET_FAIL" if completion_unknown else "NO_CURRENT_COMPLETION_OR_NOT_UNKNOWN",
        "tail_gap_instrumentation_target_ready": tail_gap_ready,
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "why_keep_as_training_seed": [
            "has current-worktree covered and holdout strict-cache evidence",
            "uses pre-action opposite-side activation rather than post-action outcomes",
            "fresh no-order failure was risk-budget, not instrumentation failure or economic-negative failure",
        ],
        "why_not_deploy": [
            "fresh no-order net_pair_cost_p90 exceeded 1.0",
            "fresh no-order residual_qty_share exceeded 15%",
            "fresh no-order pair_tail_loss_share exceeded 5%",
        ],
    }


def discover_real_tail_attribution_pullbacks(root: Path) -> list[str]:
    hits: list[str] = []
    for path in sorted((root / "xuan_research_artifacts").glob("**/aggregate_report.json")):
        if path_is_forbidden(path):
            continue
        try:
            obj = load_json(path)
        except Exception:
            continue
        lite = obj.get("event_lite") if isinstance(obj, dict) else None
        if not isinstance(lite, dict):
            continue
        if "symmetric_activation_tail_attribution_summary" not in lite:
            continue
        path_text = str(path)
        if "_smoke_" in path_text:
            continue
        hits.append(path_text)
    return hits


def train_holdout_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "purpose": (
            "Discover new strategies from train/holdout strict-cache evidence while treating public-profile "
            "accounts such as ce25 as feature priors, not proof."
        ),
        "allowed_discovery_labels": [
            "post-action outcomes may be used only inside offline train scoring",
            "frozen predicates must contain only pre-action fields before holdout evaluation",
            "holdout evaluation must run without reselecting thresholds",
        ],
        "allowed_pre_action_feature_families": [
            "opposite-side activation age/count",
            "same/opp inventory qty and cost before action",
            "projected pair cost and projected residual rate before action",
            "source_sequence presence and source-link pre-action context",
            "asset/timeframe/offset/time-to-expiry only when paired with a causal pre-action mechanism",
            "fee/liquidity role fields only when they are explicitly available before action",
        ],
        "objective": {
            "primary": "positive train and holdout fee-adjusted/stress PnL with bounded residual",
            "minimum_holdout_pair_actions": 200,
            "minimum_train_pair_actions": 1000,
            "reject_if_holdout_worst_net_fee_after_nonpositive": True,
            "reject_if_residual_rate_above_5pct_on_strict_cache": True,
            "future_no_order_acceptance_is_separate": True,
        },
        "forbidden": [
            "using realized pair_cost as live criteria",
            "using future settlement labels as live criteria",
            "single-day public-profile filters as strategy evidence",
            "pure Polymarket price cap or static side/offset/timeframe deletion",
            "D+ micro-deficit/ledger/tiny-deficit/closed-cycle/fill-to-balance failed families",
            "starting SSH/shadow/canary/live/local agg/shared WS from this contract",
            "raw/replay/full-store scans",
            "private_truth_ready/deployable/promotion claims",
        ],
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
    }


def build_queue(ce25_prior: dict[str, Any], symmetric_seed: dict[str, Any], tail_hits: list[str]) -> list[dict[str, Any]]:
    queue = [
        {
            "lane": "symmetric_activation_strict_cache_grid_expansion",
            "status": "READY_LOCAL_ONLY"
            if symmetric_seed["status"] == "STRICT_CACHE_DISCOVERY_SEED_READY"
            else "BLOCKED_STRICT_CACHE_SEED_INCOMPLETE",
            "why": (
                "Only current lane with multi-day covered+holdout strict-cache evidence and a known fresh "
                "no-order risk-budget failure to optimize against."
            ),
            "next_command_template": (
                "python3 scripts/xuan_symmetric_admission_activation_residual_stress_probe.py "
                "--edges 0.055,0.06,0.065,0.07,0.075 "
                "--leak-rates 0.005,0.01,0.02 "
                "--activation-window-s 3,5,7.5,10,15,20 "
                "--min-opp-counts 1,2 "
                "--strict-l1-pair-cap 1.00 "
                "--max-residual-qty-rate 0.04 "
                "--output-dir xuan_research_artifacts/<new-grid-artifact>"
            ),
            "evidence_status": "research_only_not_deployable",
        },
        {
            "lane": "ce25_projected_guard_train_holdout_transfer",
            "status": "FEATURE_PRIOR_ONLY_INPUT_BLOCKED"
            if ce25_prior["status"] == "FEATURE_PRIOR_READY_NOT_EVIDENCE"
            else "UNKNOWN_PRIOR_INCOMPLETE",
            "why": (
                "Projected pair-cost/residual diagnostics are implemented, but current worktree still lacks "
                "safe 72h ce25 daily-cohort evidence and authenticated liquidity/fair-probability truth."
            ),
            "next_command_template": None,
            "evidence_status": "not_strategy_evidence",
        },
        {
            "lane": "symmetric_activation_tail_attribution_real_pullback_scoring",
            "status": "READY_IF_ALLOWLISTED_PULLBACK_EXISTS" if tail_hits else "BLOCKED_REAL_PULLBACK_ABSENT",
            "why": (
                "Tail attribution summary is default-off/scored, but only smoke aggregates contain it right now."
            ),
            "available_pullbacks": tail_hits,
            "next_command_template": (
                "python3 scripts/xuan_symmetric_activation_tail_attribution_summary_scorer.py "
                "--aggregate <allowlisted aggregate_report.json> --summary-dir <allowlisted summary dir>"
            )
            if tail_hits
            else None,
            "evidence_status": "not_strategy_evidence_until_real_pullback_scored",
        },
    ]
    return queue


def select_next(queue: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in queue:
        if item["status"] == "READY_LOCAL_ONLY":
            return item
    for item in queue:
        if str(item["status"]).startswith("READY_"):
            return item
    return None


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    strategy_reset, strategy_reset_error = safe_load(args.strategy_reset_manifest, root)
    ce25_spec, ce25_spec_error = safe_load(args.ce25_guard_spec_manifest, root)
    ce25_scorer, ce25_scorer_error = safe_load(args.ce25_guard_scorer_manifest, root)
    symmetric_completion, symmetric_completion_error = safe_load(args.symmetric_completion_manifest, root)
    tail_gap, tail_gap_error = safe_load(args.tail_gap_manifest, root)

    ce25_prior = summarize_ce25_prior(ce25_spec, ce25_scorer)
    symmetric_seed = summarize_symmetric_seed(strategy_reset, symmetric_completion, tail_gap)
    tail_hits = discover_real_tail_attribution_pullbacks(root)
    queue = build_queue(ce25_prior, symmetric_seed, tail_hits)
    selected = select_next(queue)

    blockers: list[str] = []
    if strategy_reset_error:
        blockers.append(f"strategy_reset:{strategy_reset_error}")
    if ce25_spec_error:
        blockers.append(f"ce25_spec:{ce25_spec_error}")
    if ce25_scorer_error:
        blockers.append(f"ce25_scorer:{ce25_scorer_error}")
    if symmetric_completion_error:
        blockers.append(f"symmetric_completion:{symmetric_completion_error}")
    if tail_gap_error:
        blockers.append(f"tail_gap:{tail_gap_error}")
    if selected is None:
        blockers.append("no_ready_local_only_discovery_lane")

    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_READY"
        if decision == "KEEP"
        else "UNKNOWN_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_BLOCKED"
    )
    next_action = (
        "Run the selected local-only strict-cache grid expansion, then score frozen pre-action predicates on "
        "holdout without starting shadow/canary/live."
        if selected and selected["lane"] == "symmetric_activation_strict_cache_grid_expansion"
        else "Provide a ready current-worktree multi-day source or allowlisted pullback before continuing."
    )

    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "contract": train_holdout_contract(),
        "source_inputs": {
            "strategy_reset_manifest": str(args.strategy_reset_manifest),
            "strategy_reset_error": strategy_reset_error,
            "ce25_guard_spec_manifest": str(args.ce25_guard_spec_manifest),
            "ce25_guard_spec_error": ce25_spec_error,
            "ce25_guard_scorer_manifest": str(args.ce25_guard_scorer_manifest),
            "ce25_guard_scorer_error": ce25_scorer_error,
            "symmetric_completion_manifest": str(args.symmetric_completion_manifest),
            "symmetric_completion_error": symmetric_completion_error,
            "tail_gap_manifest": str(args.tail_gap_manifest),
            "tail_gap_error": tail_gap_error,
        },
        "ce25_feature_prior": ce25_prior,
        "symmetric_activation_seed": symmetric_seed,
        "real_tail_attribution_pullbacks": tail_hits,
        "candidate_queue": queue,
        "selected_next_lane": selected,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": label,
            "strategy_evidence": False,
            "selected_lane": selected.get("lane") if selected else None,
            "ce25_used_as_feature_prior": ce25_prior["status"] == "FEATURE_PRIOR_READY_NOT_EVIDENCE",
            "no_order_diagnostic_allowed": False,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "TRAIN_HOLDOUT_DISCOVERY_SELECTION_NOT_PROMOTION_EVIDENCE",
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
    parser.add_argument("--strategy-reset-manifest", type=Path, default=DEFAULT_STRATEGY_RESET)
    parser.add_argument("--ce25-guard-spec-manifest", type=Path, default=DEFAULT_CE25_GUARD_SPEC)
    parser.add_argument("--ce25-guard-scorer-manifest", type=Path, default=DEFAULT_CE25_GUARD_SCORER)
    parser.add_argument("--symmetric-completion-manifest", type=Path, default=DEFAULT_SYMMETRIC_COMPLETION)
    parser.add_argument("--tail-gap-manifest", type=Path, default=DEFAULT_TAIL_GAP)
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
