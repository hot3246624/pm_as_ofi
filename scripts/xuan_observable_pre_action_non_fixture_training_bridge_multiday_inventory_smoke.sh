#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_root = out_dir / "fixtures"
fixture_root.mkdir(parents=True, exist_ok=True)
script = Path("scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py")


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def joined_row(i, day, split):
    return {
        "schema_version": "observable_pre_action_training_bridge_joined_row_v1",
        "bridge_row_id": f"bridge-{day}-{i}",
        "feature_row_index": i,
        "offline_label_row_index": str(i),
        "join_policy_version": "observable_pre_action_training_input_bridge_join_policy_v1",
        "join_method": "exact_id:source_seed_candidate_row_id",
        "join_confidence": "exact",
        "split": split,
        "condition_id": f"condition-{day}-{i // 10}",
        "market_slug": f"btc-updown-5m-{1900000000 + i}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + i * 1000,
        "side": "YES" if i % 2 == 0 else "NO",
        "offset_s": 45.0,
        "status_before_action": "admitted" if i % 2 == 0 else "blocked",
        "reason": "candidate" if i % 2 == 0 else "target",
        "block_reason": None if i % 2 == 0 else "target",
        "source_sequence_present": True,
        "quote_intent_present": i % 2 == 0,
        "source_order_present": i % 2 == 0,
        "source_risk_direction": "repair_or_pairing_improving" if i % 3 == 0 else "risk_increasing",
        "pre_seed_same_qty": 0.0,
        "pre_seed_opp_qty": 1.25,
        "pre_seed_same_cost": 0.0,
        "pre_seed_opp_cost": 0.55,
        "pre_seed_open_qty": 1.25,
        "pre_seed_open_cost": 0.55,
        "candidate_qty": 1.25,
        "source_pair_qty": "2.0",
        "source_pair_cost": "1.75",
        "source_pair_pnl": "0.25",
        "source_residual_qty": "0.0",
        "source_residual_cost": "0.0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "residual_none",
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def materializer_manifest(summary_path, rows_path, **overrides):
    data = {
        "artifact": "xuan_observable_pre_action_non_fixture_training_bridge_materializer",
        "contract_name": "observable_pre_action_non_fixture_training_bridge_materializer_v1",
        "decision": "KEEP",
        "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_READY",
        "materialized": True,
        "blockers": [],
        "readiness_blockers": [],
        "outputs": {"joined_rows": str(rows_path), "summary": str(summary_path)},
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
        "safety": {
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
            "ssh_used": False,
            "canary_or_live_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "trading_behavior_changed": False,
        },
    }
    data.update(overrides)
    return data


def materialize(case, *, days, rows_per_day, split_policy, fixture_only=False, private_claim=False, write_rows=True):
    case_dir = fixture_root / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    rows = []
    train_days = []
    holdout_days = []
    for day_index, day in enumerate(days):
        if split_policy == "ready":
            split = "train" if day_index < 3 else "holdout"
        elif split_policy == "single_unused":
            split = "unused"
        else:
            split = "unused"
        if split == "train":
            train_days.append(day)
        if split == "holdout":
            holdout_days.append(day)
        for _ in range(rows_per_day):
            rows.append(joined_row(len(rows), day, split))
    split_counts = {}
    for row in rows:
        split_counts[row["split"]] = split_counts.get(row["split"], 0) + 1
    summary_path = case_dir / "observable_pre_action_training_bridge_summary.json"
    rows_path = case_dir / "observable_pre_action_training_bridge_joined_rows.jsonl"
    summary = {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": fixture_only,
        "materializer_contract": "observable_pre_action_non_fixture_training_bridge_materializer_v1",
        "feature_row_count": len(rows),
        "offline_label_row_count": len(rows),
        "joined_row_count": len(rows),
        "join_coverage": 1.0,
        "join_method_counts": {"exact_id:source_seed_candidate_row_id": len(rows)},
        "split_counts": split_counts,
        "train_days": sorted(set(train_days)),
        "holdout_days": sorted(set(holdout_days)),
        "available_days": sorted(set(days)),
        "source_sequence_coverage": 1.0,
        "label_row_reuse_count": 0,
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
        "trading_behavior_changed": False,
        "strategy_evidence": False,
        "private_truth_ready": private_claim,
        "deployable": False,
        "promotion_gate": {"passed": False},
    }
    write_json(summary_path, summary)
    if write_rows:
        write_jsonl(rows_path, rows)
    write_json(case_dir / "manifest.json", materializer_manifest(summary_path, rows_path, private_truth_ready=private_claim))
    return summary_path


def run_case(case, summary):
    output_dir = out_dir / case
    subprocess.run(
        [
            "python3",
            str(script),
            "--only-explicit",
            "--allow-fixture-like-paths-for-smoke",
            "--bridge-summary",
            str(summary),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
    )
    return json.loads((output_dir / "manifest.json").read_text())


days = ["2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24", "2026-05-25"]
cases = {
    "complete": run_case("complete", materialize("complete", days=days, rows_per_day=40, split_policy="ready")),
    "single_day": run_case(
        "single_day", materialize("single_day", days=["2026-05-26"], rows_per_day=120, split_policy="single_unused")
    ),
    "fixture_summary": run_case(
        "fixture_summary", materialize("fixture_summary", days=days, rows_per_day=40, split_policy="ready", fixture_only=True)
    ),
    "missing_rows": run_case(
        "missing_rows", materialize("missing_rows", days=days, rows_per_day=40, split_policy="ready", write_rows=False)
    ),
    "private_claim": run_case(
        "private_claim", materialize("private_claim", days=days, rows_per_day=40, split_policy="ready", private_claim=True)
    ),
}

checks = {
    "complete_keep": cases["complete"]["decision_label"]
    == "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_READY",
    "complete_ready_candidate_named": bool(cases["complete"].get("ready_candidate", {}).get("joined_rows")),
    "single_day_unknown": cases["single_day"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS",
    "fixture_summary_not_accepted": cases["fixture_summary"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS"
    and cases["fixture_summary"]["score"]["accepted_bridge_output_count"] == 0,
    "missing_rows_fail_closed": cases["missing_rows"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS"
    and cases["missing_rows"]["score"]["accepted_bridge_output_count"] == 0,
    "private_claim_discard": cases["private_claim"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_FORBIDDEN_INPUT",
    "no_private_deployable_promotion_claim": all(
        case["private_truth_ready"] is False and case["deployable"] is False and case["promotion_gate"]["passed"] is False
        for case in cases.values()
    ),
}
manifest = {
    "artifact": "xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory_smoke",
    "checks": checks,
    "case_decisions": {name: case["decision_label"] for name, case in cases.items()},
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_INVENTORY_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_INVENTORY_SMOKE_FAIL",
    "strategy_evidence": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
}
write_json(out_dir / "manifest.json", manifest)
if not all(checks.values()):
    raise SystemExit(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" > /dev/null
echo "smoke ok: $out_dir"
