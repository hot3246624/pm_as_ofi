#!/usr/bin/env python3
"""Build a local-only L2/V1 bridge validation spec for xuan age/fee residual work.

This artifact turns the current refined old-system age/fee feature into an
auditable request for the rebuilt seven-asset L2/V1 backtest stack.  It does
not import candidates, authorize a runner profile, or create remote-run
rationale.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0657Z"

DEFAULT_CROSS_WINDOW = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_age_fee_cross_window_validation_20260527T0627Z.json"
)
DEFAULT_V1_CSV_GATE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_backtest_v1_integration_gate_local_default_csv_fallback_l2_review.json"
)
DEFAULT_V1_DUCKDB_GATE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_backtest_v1_integration_gate_local_default_duckdb_l2_review.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_l2_v1_age_fee_bridge_validation_spec_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_l2_v1_age_fee_bridge_validation_spec_{STAMP}/"
    "L2_V1_AGE_FEE_BRIDGE_VALIDATION_SPEC.md"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def source_status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "UNKNOWN")


def v1_gate_summary(card: dict[str, Any]) -> dict[str, Any]:
    summary = body(card, "summary")
    decision = body(card, "decision")
    return rounded(
        {
            "status": source_status(card),
            "audit_query_kind": summary.get("audit_query_kind"),
            "audit_query_ok": summary.get("audit_query_ok"),
            "audit_duckdb_query_ok": summary.get("audit_duckdb_query_ok"),
            "suite_ok": summary.get("suite_ok"),
            "readiness_ok": summary.get("readiness_ok"),
            "candidate_audit_ok": summary.get("candidate_audit_ok"),
            "audit_assets": summary.get("audit_assets") or [],
            "selected_candidate_count": summary.get("selected_candidate_count"),
            "positive_queue_candidate_count": summary.get("positive_queue_candidate_count"),
            "private_promotion_ready_count": summary.get("private_promotion_ready_count"),
            "forbidden_result_columns_empty": summary.get("forbidden_result_columns_empty"),
            "direct_candidate_import_ready": decision.get("direct_candidate_import_ready"),
            "remote_runner_allowed": decision.get("remote_runner_allowed"),
            "private_truth_ready": decision.get("private_truth_ready"),
            "hard_blockers": decision.get("hard_blockers") or [],
        }
    )


def cross_window_snapshot(card: dict[str, Any]) -> dict[str, Any]:
    decision = body(card, "decision")
    rule_history = body(card, "rule_history")
    v2 = body(rule_history, "v2_source_age_refined")
    target_sets = body(card, "target_sets")
    runs = card.get("runs") if isinstance(card.get("runs"), list) else []
    run_readouts = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        rules = body(run, "rules")
        v2_rule = body(rules, "v2_source_age_refined")
        capacity = body(v2_rule, "capacity_after")
        run_readouts.append(
            {
                "key": run.get("key"),
                "schema_complete_for_strict_feature_validation": run.get(
                    "schema_complete_for_strict_feature_validation"
                ),
                "v2_controlled_qids": v2_rule.get("controlled_qids") or [],
                "v2_non_residual_accepts_controlled": v2_rule.get("non_residual_accepts_controlled") or [],
                "v2_accepted_after": v2_rule.get("accepted_after"),
                "v2_fills_after": v2_rule.get("fills_after"),
                "v2_fill_rate_after": v2_rule.get("fill_rate_after"),
                "v2_capacity_all_pass": capacity.get("all_capacity_pass"),
                "v2_residual_cost_share_after": capacity.get("residual_cost_share_after"),
                "v2_residual_qty_share_after": capacity.get("residual_qty_share_after"),
            }
        )
    return rounded(
        {
            "status": source_status(card),
            "decision": {
                "complete_window_validation_pass": decision.get("complete_window_validation_pass"),
                "v2_refined_rule_no_false_positive_on_complete_windows": decision.get(
                    "v2_refined_rule_no_false_positive_on_complete_windows"
                ),
                "v2_refined_rule_fixes_0407_bad_tail_under_local_proxy": decision.get(
                    "v2_refined_rule_fixes_0407_bad_tail_under_local_proxy"
                ),
                "old_best_1705_strict_feature_validation_complete": decision.get(
                    "old_best_1705_strict_feature_validation_complete"
                ),
                "candidate_profile_ready": decision.get("candidate_profile_ready"),
                "runner_support_ready": decision.get("runner_support_ready"),
                "future_remote_allowed_by_this_validation": decision.get(
                    "future_remote_allowed_by_this_validation"
                ),
                "private_truth_ready": decision.get("private_truth_ready"),
                "hard_blockers": decision.get("hard_blockers") or [],
            },
            "target_sets": target_sets,
            "v2_rule": body(v2, "rule"),
            "run_readouts": run_readouts,
        }
    )


def required_field_contract() -> dict[str, list[str]]:
    return {
        "identity_and_lineage": [
            "asset",
            "slug",
            "condition_id",
            "token_id",
            "outcome_side",
            "market_type",
            "window_start_ts",
            "event_time_ms",
            "receive_time_ms",
            "source_sequence_id",
            "source_file_sha256",
        ],
        "l1_l2_parity": [
            "l1_best_bid_px",
            "l1_best_bid_qty",
            "l1_best_ask_px",
            "l1_best_ask_qty",
            "l2_bid_levels_top_n",
            "l2_ask_levels_top_n",
            "l1_from_l2_best_bid_px",
            "l1_from_l2_best_ask_px",
            "l1_l2_parity_decision",
            "book_age_ms",
        ],
        "candidate_features_for_v2": [
            "risk_increasing_seed",
            "pair_completion_qty",
            "source_quality_decision",
            "source_quality_l1_age_ms",
            "surplus_budget_projected_unpaired_cost",
            "qty",
            "price",
            "same_exposure_qty",
            "opp_exposure_qty",
        ],
        "source_and_economic_gates": [
            "public_trade_px",
            "public_trade_size",
            "source_quality_l1_required",
            "source_quality_trade_required",
            "closeability_net_pair_cost",
            "pair_completion_net_cap",
            "strict_rescue_surplus_net_cap",
            "rescue_source_quality_decision",
        ],
        "queue_and_pnl_model": [
            "queue_model_name",
            "queue_fill_qty",
            "queue_fill_haircut",
            "maker_fee_rate",
            "taker_fee_rate",
            "filled_cost",
            "queue_pnl",
            "pair_pnl",
            "merge_reuse_turnover_count",
            "capital_time_weighted_cost",
        ],
        "residual_and_mark_recovery": [
            "residual_lot_id",
            "quote_intent_id_or_deterministic_surrogate",
            "residual_qty",
            "residual_cost",
            "residual_age_s",
            "residual_mark_snapshot_age_s",
            "mature_after_fee_recovery_rate",
            "mature_after_fee_mark_value",
            "marked_pair_pnl_after_residual",
            "time_to_close_s",
        ],
        "private_truth_boundary": [
            "owner_private_truth_ready",
            "historical_shadow_no_order_only",
            "public_or_proxy_evidence_only",
            "promotion_blockers",
            "private_promotion_ready",
        ],
    }


def validation_questions() -> list[dict[str, Any]]:
    return [
        {
            "id": "l1_from_l2_parity",
            "question": "Can every search-safe L1 row used by xuan be reconstructed from the rebuilt L2 store with identical best bid/ask, age, and source lineage?",
            "expected_output": "L1_FROM_L2_PARITY_REPORT.json plus per-asset mismatch samples",
            "pass_criteria": [
                "seven assets covered: BNB/BTC/DOGE/ETH/HYPE/SOL/XRP",
                "valid days present and blocklisted days 2026-05-14/15/19 have zero search-safe rows",
                "normal queries use /Users/hot/web3Scientist/poly_backtest_data, not /Volumes/PolyData",
                "forbidden/private result columns remain empty",
                "L1-from-L2 best bid/ask parity mismatches are zero or fully explained by deterministic filters",
            ],
        },
        {
            "id": "old_anchor_replay_explanation",
            "question": "Can the bridge replay or explain old xuan anchors 20260522T1705, 20260525T2041, 20260526T1757, and 20260527T0407 without changing the strategy semantics?",
            "expected_output": "XUAN_OLD_ANCHOR_REPLAY_EXPLANATION.json",
            "pass_criteria": [
                "1705 missing candidate-event schema is either reconstructed or explicitly marked non-comparable",
                "2041 and 1757 preserve positive pair_pnl/ROI interpretation under the same old-system semantics",
                "0407 reproduces the bad mature after-fee residual tail class used by the v2 feature",
                "differences between queue_pnl and old pair_pnl/rescue/merge economics are decomposed, not collapsed into one score",
            ],
        },
        {
            "id": "v2_age_fee_feature_holdout",
            "question": "Does the refined non-qid v2 age/fee feature avoid bad mature residual tails without false positives on independent L2/V1 windows?",
            "expected_output": "XUAN_AGE_FEE_V2_HOLDOUT_SCORECARD.json",
            "pass_criteria": [
                "v2 fires on the known 0407 bad-tail class and preserves high-recovery residual lots",
                "false-positive controls on profitable non-residual accepts are zero on old complete anchors",
                "holdout reports are split by asset and include BTC, not only BNB/DOGE/HYPE candidate-audit assets",
                "accepted/fill density is not starved after v2 filtering",
                "pair_pnl or queue-to-pair bridge PnL remains positive after local proxy adjustments",
            ],
        },
        {
            "id": "dynamic_residual_mark_recovery",
            "question": "Does residual look recoverable after realistic mature after-fee marks, or only under early/no-fee marks?",
            "expected_output": "RESIDUAL_DYNAMIC_MARK_RECOVERY_REPORT.json",
            "pass_criteria": [
                "break-even residual recovery is computed from each runtime, not copied from a fixed threshold",
                "research recovery threshold is explicit and above break-even",
                "mature after-fee marked PnL is nonnegative for any residual reclassification",
                "gross residual capacity gates are checked separately from economic mark recovery",
            ],
        },
        {
            "id": "private_truth_boundary",
            "question": "Does the new stack keep historical shadow/no-order, public/proxy, and owner private truth as separate evidence classes?",
            "expected_output": "PRIVATE_TRUTH_BOUNDARY_REPORT.json",
            "pass_criteria": [
                "private_promotion_ready_count remains zero without future owner execution",
                "historical shadow/no-order rows never set private_truth_ready=true",
                "public/proxy evidence cannot clear deployable/live gates",
                "candidate import, remote runner, deploy, and live orders stay false unless owner private truth exists",
            ],
        },
    ]


def acceptance_thresholds() -> dict[str, Any]:
    return {
        "anchor_density_minimums": {
            "min_accepted_after": 10,
            "min_fills_after": 8,
            "min_fill_rate_after": 0.70,
            "note": "For old anchor local proxies; larger L2/V1 batches should report distributional density by asset and day.",
        },
        "residual_capacity_targets": {
            "max_residual_cost_share": 0.15,
            "max_residual_qty_share": 0.20,
            "max_residual_cost_to_pair_qty": 0.05,
            "scope": "Apply when v2 intentionally controls bad tail; do not claim v2 is a universal residual fix where it does not fire.",
        },
        "economic_targets": {
            "pair_pnl_after_proxy_must_be_positive": True,
            "marked_pair_pnl_after_mature_residual_must_be_nonnegative": True,
            "best_queue_pnl_negative_means": "Queue-only V1 candidates are not xuan full pair/rescue/merge strategy evidence.",
        },
        "safety_targets": {
            "source_quality_relaxation_allowed": False,
            "closeability_cancel_allowed": False,
            "pair_completion_net_cap_max": 0.99,
            "strict_rescue_surplus_net_cap_max": 1.02,
            "remote_or_live_authorized_by_this_spec": False,
        },
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    cross = load_json(args.cross_window)
    csv_gate = load_json(args.v1_csv_gate)
    duckdb_gate = load_json(args.v1_duckdb_gate)

    cross_snap = cross_window_snapshot(cross)
    csv_summary = v1_gate_summary(csv_gate)
    duckdb_summary = v1_gate_summary(duckdb_gate)

    v1_ready = (
        csv_summary["status"] == "KEEP_XUAN_BACKTEST_V1_INTEGRATION_GATE_READY_LOCAL_ONLY"
        and duckdb_summary["status"] == "KEEP_XUAN_BACKTEST_V1_INTEGRATION_GATE_READY_LOCAL_ONLY"
        and bool(duckdb_summary["candidate_audit_ok"])
        and bool(duckdb_summary["readiness_ok"])
    )
    v2_ready_for_spec = (
        cross_snap["status"] == "KEEP_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_CROSS_WINDOW_VALIDATION_READY_LOCAL_ONLY"
        and bool(cross_snap["decision"]["v2_refined_rule_no_false_positive_on_complete_windows"])
        and bool(cross_snap["decision"]["v2_refined_rule_fixes_0407_bad_tail_under_local_proxy"])
    )

    hard_blockers = [
        "owner_private_truth_not_ready",
        "runner_support_not_implemented",
        "independent_l2_v1_holdout_not_yet_run",
        "old_best_1705_missing_event_feature_schema_for_strict_validation",
    ]
    if not v1_ready:
        hard_blockers.append("backtest_v1_integration_gate_not_ready")
    if not v2_ready_for_spec:
        hard_blockers.append("v2_refined_old_system_validation_not_ready")
    if duckdb_summary.get("positive_queue_candidate_count") == 0:
        hard_blockers.append("v1_candidate_audit_has_no_positive_queue_pnl_for_direct_import")

    status = (
        "KEEP_SHADOW_REVIEW_L2_V1_AGE_FEE_BRIDGE_VALIDATION_SPEC_READY_LOCAL_ONLY"
        if v1_ready and v2_ready_for_spec
        else "BLOCKED_SHADOW_REVIEW_L2_V1_AGE_FEE_BRIDGE_VALIDATION_SPEC_INPUTS_NOT_READY_LOCAL_ONLY"
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_l2_v1_age_fee_bridge_validation_spec",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_l2_v1_age_fee_bridge_validation_spec.py",
            "status": status,
            "inputs": {
                "cross_window_validation": str(args.cross_window),
                "v1_csv_gate": str(args.v1_csv_gate),
                "v1_duckdb_gate": str(args.v1_duckdb_gate),
                "poly_bt_root_expected": "/Users/hot/web3Scientist/poly_backtest_data",
            },
            "source_snapshots": {
                "old_system_age_fee_cross_window_validation": cross_snap,
                "backtest_v1_csv_gate": csv_summary,
                "backtest_v1_duckdb_gate": duckdb_summary,
            },
            "problem_statement": {
                "l2_rebuild_is_necessary_but_not_sufficient": True,
                "why_negative_v1_queue_pnl_is_not_final_xuan_strategy_pnl": (
                    "V1 candidate audit currently scores upstream search-safe queue-only candidates. "
                    "It does not include the old xuan pair/rescue/merge residual accounting or owner private truth."
                ),
                "why_l2_bridge_is_required": (
                    "The rebuilt L2 store must explain the divergence between V1 queue-only results and old-system "
                    "positive pair_pnl anchors before any xuan feature, profile, or candidate import is trusted."
                ),
            },
            "required_field_contract": required_field_contract(),
            "validation_questions": validation_questions(),
            "acceptance_thresholds": acceptance_thresholds(),
            "requested_deliverables_from_l2_v1_rebuild": [
                "L1_FROM_L2_PARITY_REPORT.json",
                "XUAN_OLD_ANCHOR_REPLAY_EXPLANATION.json",
                "XUAN_AGE_FEE_V2_HOLDOUT_SCORECARD.json",
                "RESIDUAL_DYNAMIC_MARK_RECOVERY_REPORT.json",
                "PRIVATE_TRUTH_BOUNDARY_REPORT.json",
                "schema manifest with table/view names, fingerprints, and no /Volumes/PolyData references for normal queries",
            ],
            "decision": {
                "l2_v1_age_fee_bridge_validation_spec_ready": v1_ready and v2_ready_for_spec,
                "l2_rebuild_alone_sufficient_for_xuan": False,
                "v2_refined_feature_ready_for_independent_validation_request": v2_ready_for_spec,
                "v2_runner_profile_ready": False,
                "candidate_import_ready": False,
                "direct_v1_candidate_import_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_spec": False,
                "future_remote_requires_independent_l2_v1_holdout": True,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "promotion_ready": False,
                "research_only": True,
                "paper_shadow_only": True,
                "hard_blockers": hard_blockers,
                "next_action": (
                    "hand this spec to the L2/V1 rebuild path or implement a local bridge scorer once the L2 event store "
                    "exposes the required fields; do not build manifest/preauthorization/remote until the bridge holdout passes"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    thresholds = card["acceptance_thresholds"]
    cross = card["source_snapshots"]["old_system_age_fee_cross_window_validation"]
    v2_rule = cross["v2_rule"]
    duckdb = card["source_snapshots"]["backtest_v1_duckdb_gate"]

    lines = [
        "# Xuan L2/V1 Age/Fee Bridge Validation Spec",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- spec_ready: `{decision['l2_v1_age_fee_bridge_validation_spec_ready']}`",
        f"- l2_rebuild_alone_sufficient_for_xuan: `{decision['l2_rebuild_alone_sufficient_for_xuan']}`",
        f"- v2_runner_profile_ready: `{decision['v2_runner_profile_ready']}`",
        f"- future_remote_allowed_by_this_spec: `{decision['future_remote_allowed_by_this_spec']}`",
        f"- private_truth_ready: `{decision['private_truth_ready']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers'])}`",
        "",
        "## Why This Exists",
        "",
        card["problem_statement"]["why_negative_v1_queue_pnl_is_not_final_xuan_strategy_pnl"],
        "",
        card["problem_statement"]["why_l2_bridge_is_required"],
        "",
        "## Current V1 Gate Read",
        "",
        f"- duckdb_status: `{duckdb['status']}`",
        f"- audit_assets: `{', '.join(duckdb['audit_assets'])}`",
        f"- selected_candidate_count: `{duckdb['selected_candidate_count']}`",
        f"- positive_queue_candidate_count: `{duckdb['positive_queue_candidate_count']}`",
        f"- private_promotion_ready_count: `{duckdb['private_promotion_ready_count']}`",
        "",
        "## Refined V2 Feature To Validate",
        "",
        "Common conditions:",
    ]
    for item in v2_rule.get("common", []):
        lines.append(f"- `{item}`")
    lines.extend(["", "Clauses:"])
    for item in v2_rule.get("clauses", []):
        lines.append(f"- `{item}`")
    lines.extend(
        [
            "",
            "## Required Questions For The L2/V1 Rebuild",
            "",
        ]
    )
    for question in card["validation_questions"]:
        lines.append(f"### {question['id']}")
        lines.append("")
        lines.append(question["question"])
        lines.append("")
        lines.append(f"Expected output: `{question['expected_output']}`")
        lines.append("")
        for criterion in question["pass_criteria"]:
            lines.append(f"- {criterion}")
        lines.append("")
    lines.extend(
        [
            "## Acceptance Thresholds",
            "",
            f"- anchor min accepted/fills/fill_rate: `{thresholds['anchor_density_minimums']['min_accepted_after']}` / `{thresholds['anchor_density_minimums']['min_fills_after']}` / `{thresholds['anchor_density_minimums']['min_fill_rate_after']}`",
            f"- residual cost/qty/cost_to_pair targets: `{thresholds['residual_capacity_targets']['max_residual_cost_share']}` / `{thresholds['residual_capacity_targets']['max_residual_qty_share']}` / `{thresholds['residual_capacity_targets']['max_residual_cost_to_pair_qty']}`",
            f"- pair_completion_net_cap_max: `{thresholds['safety_targets']['pair_completion_net_cap_max']}`",
            f"- strict_rescue_surplus_net_cap_max: `{thresholds['safety_targets']['strict_rescue_surplus_net_cap_max']}`",
            "",
            "## Required Fields",
            "",
        ]
    )
    for section, fields in card["required_field_contract"].items():
        lines.append(f"### {section}")
        lines.append("")
        for field in fields:
            lines.append(f"- `{field}`")
        lines.append("")
    lines.extend(
        [
            "## Next Action",
            "",
            decision["next_action"],
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cross-window", type=Path, default=DEFAULT_CROSS_WINDOW)
    parser.add_argument("--v1-csv-gate", type=Path, default=DEFAULT_V1_CSV_GATE)
    parser.add_argument("--v1-duckdb-gate", type=Path, default=DEFAULT_V1_DUCKDB_GATE)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    card = build(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps({"status": card["status"], "scorecard": str(args.scorecard), "markdown": str(args.markdown)}, indent=2))


if __name__ == "__main__":
    main()
