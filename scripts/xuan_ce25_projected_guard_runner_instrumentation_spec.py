#!/usr/bin/env python3
"""Spec default-off runner instrumentation for ce25 projected guard diagnostics.

This local-only source review inspects the current no-order shadow runner and
committed projected-guard artifacts. It determines whether the runner can expose
pre-action projected pair-cost/residual diagnostics without behavior changes.
It does not modify the runner, fetch data, start services, run shadow, or touch
trading paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_projected_guard_runner_instrumentation_spec"
DEFAULT_RUNNER = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")
DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_projected_pair_cost_residual_guard_spec_20260524T104842Z/"
    "manifest.json"
)
DEFAULT_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_projected_guard_fixture_scorer_20260524T105851Z/"
    "manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


SOURCE_CAPABILITIES = {
    "same_opp_qty_available": [
        "same_qty = self.exposure_qty(side)",
        "opp_qty = self.exposure_qty(opp(side))",
    ],
    "same_opp_cost_available": [
        "same_cost_before = self.exposure_cost(side)",
        "opp_cost_before = self.exposure_cost(opp(side))",
    ],
    "intended_order_available": [
        "seed_px = max(0.01, px - self.cfg.edge)",
        "qty = base_qty",
        "order = VirtualOrder(",
        "side=side",
        "px=seed_px",
        "qty=qty",
    ],
    "candidate_pre_append_hook_available": [
        "quote_intent_id = self.quote_intent_id(self.next_order_id)",
        "self.pending.append(order)",
        "self.metrics.candidates += 1",
    ],
    "event_lite_pattern_available": [
        "event_lite_summary: bool = False",
        "ap.add_argument(\"--event-lite-summary\"",
        "summary[\"event_lite\"]",
    ],
    "default_off_cli_pattern_available": [
        "source_opportunity_closed_cycle_marker_event_lite_summary: bool = False",
        "ap.add_argument(\"--source-opportunity-closed-cycle-marker-event-lite-summary\"",
    ],
    "current_realized_pair_cost_not_live_criteria": [
        "pair_cost = a.px + b.px",
        "self.metrics.pair_costs.append(pair_cost)",
    ],
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def source_checks(text: str) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for name, needles in SOURCE_CAPABILITIES.items():
        missing = [needle for needle in needles if needle not in text]
        checks[name] = {"ok": not missing, "missing": missing, "needles": needles}
    return checks


def spec_fields_present(spec: dict[str, Any]) -> dict[str, Any]:
    contract = spec.get("projected_guard_contract") if isinstance(spec, dict) else {}
    fields = set(contract.get("required_pre_action_fields") or [])
    required = {
        "pre_yes_qty",
        "pre_no_qty",
        "pre_yes_actual_cost",
        "pre_no_actual_cost",
        "order_qty",
        "order_price",
        "estimated_fee_per_share",
        "projected_pair_cost",
        "projected_residual_rate_on_bought_qty",
    }
    missing = sorted(required - fields)
    return {"ok": not missing, "missing": missing, "required_subset": sorted(required)}


def scorer_ready(scorer: dict[str, Any]) -> dict[str, Any]:
    checks = scorer.get("score", {}).get("checks", {}) if isinstance(scorer, dict) else {}
    required_checks = [
        "all_fixture_expectations_met",
        "starter_fixture_allowed",
        "core_fixture_allowed",
        "pair_cost_hard_kill_blocked",
        "residual_near_close_hard_kill_blocked",
        "missing_pre_action_field_fail_closed",
        "missing_projected_field_fail_closed",
    ]
    missing = [name for name in required_checks if not checks.get(name)]
    return {"ok": not missing, "missing_or_false_checks": missing, "required_checks": required_checks}


def instrumentation_contract() -> dict[str, Any]:
    return {
        "contract_name": "ce25_projected_guard_runner_instrumentation_v1",
        "default_off_flag": "--ce25-projected-guard-event-lite-summary",
        "dependencies": ["--event-lite-summary"],
        "behavior_change": False,
        "placement": "after qty is finalized and before VirtualOrder is appended in DPlusRunner.on_trade",
        "admitted_candidate_scope": (
            "diagnose candidate orders only after existing gates compute seed_px/base_qty/qty; do not unblock or "
            "block anything in v1"
        ),
        "canonical_mapping": {
            "if_side_yes": {
                "pre_yes_qty": "same_qty",
                "pre_no_qty": "opp_qty",
                "pre_yes_actual_cost": "same_cost_before",
                "pre_no_actual_cost": "opp_cost_before",
            },
            "if_side_no": {
                "pre_yes_qty": "opp_qty",
                "pre_no_qty": "same_qty",
                "pre_yes_actual_cost": "opp_cost_before",
                "pre_no_actual_cost": "same_cost_before",
            },
            "outcome": "side",
            "order_qty": "qty",
            "order_price": "seed_px",
            "estimated_fee_per_share": "0.0 for passive no-order seed proxy; nonzero real fee remains out of scope",
        },
        "new_helper_functions": [
            "market_round_length_s(slug) -> 300/900/3600/14400 for updown 5m/15m/1h/4h; otherwise None",
            "seconds_to_expiry_from_offset(slug, offset_s) -> max(round_length_s - offset_s, 0)",
            "projected_guard_context(side, qty, seed_px, same_qty, opp_qty, same_cost, opp_cost, estimated_fee_per_share)",
            "projected_guard_decision(context, ce25 starter/core/hard-kill thresholds)",
        ],
        "summary_fields": {
            "event_lite.ce25_projected_guard_summary.schema_version": "ce25_projected_guard_summary_v1",
            "event_lite.ce25_projected_guard_summary.field_contract.post_action_outcome_labels_included": False,
            "event_lite.ce25_projected_guard_summary.field_contract.private_truth_ready": False,
            "event_lite.ce25_projected_guard_summary.field_contract.deployable": False,
            "event_lite.ce25_projected_guard_summary.field_contract.promotion_gate_passed": False,
            "event_lite.ce25_projected_guard_summary.decision_count_by_status": {},
            "event_lite.ce25_projected_guard_summary.decision_count_by_guard": {},
            "event_lite.ce25_projected_guard_summary.decision_count_by_asset_timeframe_guard_status": {},
            "event_lite.ce25_projected_guard_summary.projected_pair_cost_bucket_by_guard": {},
            "event_lite.ce25_projected_guard_summary.projected_residual_bucket_by_guard": {},
            "event_lite.ce25_projected_guard_summary.final_window_policy_count": {},
        },
        "smoke_plan": [
            "default-off runner fixture summary does not contain ce25_projected_guard_summary",
            "enabled runner fixture summary contains ce25_projected_guard_summary_v1",
            "fixture admitted starter/core candidates match projected scorer formulae",
            "fixture hard-kill candidate is counted as diagnostic blocked-but-behavior-unchanged",
            "CLI dependency fails closed when flag is used without --event-lite-summary",
        ],
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    for path in (args.runner, args.spec_manifest, args.scorer_manifest, output_dir):
        if not path_safe(path):
            raise RuntimeError(f"unsafe path: {path}")
    runner_text = args.runner.read_text(encoding="utf-8", errors="replace")
    spec = load_json(args.spec_manifest)
    scorer = load_json(args.scorer_manifest)
    source = source_checks(runner_text)
    spec_check = spec_fields_present(spec)
    scorer_check = scorer_ready(scorer)
    capability_checks = {
        **{name: item["ok"] for name, item in source.items()},
        "spec_required_fields_present": spec_check["ok"],
        "fixture_scorer_ready": scorer_check["ok"],
        "spec_promotion_false": not bool(spec.get("promotion_gate", {}).get("passed")),
        "scorer_promotion_false": not bool(scorer.get("promotion_gate", {}).get("passed")),
    }
    blockers = [name for name, ok in capability_checks.items() if not ok]
    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_READY"
        if decision == "KEEP"
        else "UNKNOWN_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_BLOCKED"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_projected_guard_runner_instrumentation_spec",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "source_review_only": True,
            "new_data_fetch": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_started": False,
            "local_agg_started": False,
            "shared_ws_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "trading_behavior_changed": False,
        },
        "source_inputs": {
            "runner": str(args.runner),
            "spec_manifest": str(args.spec_manifest),
            "scorer_manifest": str(args.scorer_manifest),
        },
        "capability_checks": capability_checks,
        "source_capabilities": source,
        "spec_required_fields_check": spec_check,
        "fixture_scorer_check": scorer_check,
        "blockers": blockers,
        "instrumentation_contract": instrumentation_contract(),
        "research_ranking": {
            "decision": decision,
            "label": label,
            "instrumentation_spec_ready": decision == "KEEP",
            "next_local_only_target": "ce25_projected_guard_runner_instrumentation_default_off_v1",
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "promotion_gate": {
            "passed": False,
            "status": "RUNNER_INSTRUMENTATION_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [
                "spec only; runner code not changed in this artifact",
                "default-off diagnostics must be implemented and smoked before any diagnostic run",
                "synthetic fixtures and public buckets are not private truth",
                "no no-order diagnostic approval from this artifact",
            ],
        },
        "next_executable_action": (
            "Implement ce25_projected_guard_runner_instrumentation_default_off_v1 in "
            "tools/xuan_dplus_passive_passive_shadow_runner.py plus smoke: add default-off summary-only diagnostics, "
            "prove default-off absence/enabled presence/dependency fail-closed/no behavior change, and keep "
            "promotion_gate.passed=false."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--spec-manifest", type=Path, default=DEFAULT_SPEC_MANIFEST)
    parser.add_argument("--scorer-manifest", type=Path, default=DEFAULT_SCORER_MANIFEST)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
