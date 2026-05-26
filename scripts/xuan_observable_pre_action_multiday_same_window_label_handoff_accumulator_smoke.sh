#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/xuan_multiday_label_handoff_accumulator_pycache_$ts}"
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT

python3 -m py_compile scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py >> "$log" 2>&1

python3 - "$out_dir" "$ts" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
created_utc = sys.argv[2]
fixture_root = out_dir / "fixtures"
fixture_root.mkdir(parents=True, exist_ok=True)
script = Path("scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py")


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def joined_row(handoff_id, day, idx, *, candidate_id=None, action_id=None, forbidden_live=False):
    admitted = idx % 2 == 0
    return {
        "schema_version": "observable_pre_action_training_bridge_joined_row_v1",
        "bridge_row_id": f"{handoff_id}-bridge-{idx}",
        "source_seed_candidate_row_id": candidate_id or f"{handoff_id}-candidate-{idx}",
        "source_seed_action_id": action_id if action_id is not None else (f"{handoff_id}-action-{idx}" if admitted else ""),
        "feature_row_index": idx,
        "offline_label_row_index": str(idx),
        "join_policy_version": "observable_pre_action_training_input_bridge_join_policy_v1",
        "join_method": "exact_id:source_seed_candidate_row_id",
        "join_confidence": "exact",
        "split": "unused",
        "condition_id": f"condition-{day}",
        "market_slug": f"btc-updown-5m-{1900000000 + idx}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + idx * 1000,
        "side": "YES" if idx % 2 == 0 else "NO",
        "offset_s": 45.0,
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else "cooldown",
        "block_reason": None if admitted else "cooldown",
        "source_sequence_present": True,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "source_risk_direction": "risk_increasing" if idx % 3 else "repair_or_pairing_improving",
        "pre_seed_same_qty": 0.0,
        "pre_seed_opp_qty": 1.25,
        "pre_seed_same_cost": 0.0,
        "pre_seed_opp_cost": 0.55,
        "pre_seed_open_qty": 1.25,
        "pre_seed_open_cost": 0.55,
        "candidate_qty": 1.25 if admitted else None,
        "source_pair_qty": "2.0",
        "source_pair_cost": "1.75",
        "source_pair_pnl": "0.25",
        "source_residual_qty": "0.0",
        "source_residual_cost": "0.0",
        "source_residual_age_s": "0.0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "residual_none",
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": forbidden_live,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def source_manifest(summary_path, rows_path, *, private_claim=False, materialized=True):
    return {
        "artifact": "xuan_observable_pre_action_non_fixture_training_bridge_materializer",
        "contract_name": "observable_pre_action_non_fixture_training_bridge_materializer_v1",
        "decision": "KEEP" if materialized else "UNKNOWN",
        "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_READY",
        "materialized": materialized,
        "blockers": [] if materialized else ["forced_materializer_not_materialized"],
        "outputs": {"joined_rows": str(rows_path), "summary": str(summary_path)},
        "strategy_evidence": False,
        "private_truth_ready": private_claim,
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


def materialize_source(
    case_dir,
    day,
    rows_per_day,
    *,
    handoff_id=None,
    fixture_only=False,
    duplicate_candidate=None,
    duplicate_action=None,
    forbidden_live=False,
    private_claim=False,
    write_rows=True,
    count_delta=0,
    materialized=True,
):
    handoff_id = handoff_id or f"handoff-{day}"
    source_dir = case_dir / handoff_id
    source_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for idx in range(rows_per_day):
        candidate_id = duplicate_candidate if idx == 0 and duplicate_candidate else None
        action_id = duplicate_action if idx == 0 and duplicate_action else None
        rows.append(
            joined_row(
                handoff_id,
                day,
                idx,
                candidate_id=candidate_id,
                action_id=action_id,
                forbidden_live=forbidden_live and idx == 0,
            )
        )
    summary_path = source_dir / "observable_pre_action_training_bridge_summary.json"
    rows_path = source_dir / "observable_pre_action_training_bridge_joined_rows.jsonl"
    summary_count = len(rows) + count_delta
    summary = {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": fixture_only,
        "source_handoff_id": handoff_id,
        "materializer_contract": "observable_pre_action_non_fixture_training_bridge_materializer_v1",
        "join_policy_version": "observable_pre_action_training_input_bridge_join_policy_v1",
        "feature_row_count": summary_count,
        "offline_label_row_count": summary_count,
        "joined_row_count": summary_count,
        "join_coverage": 1.0,
        "join_method_counts": {"exact_id:source_seed_candidate_row_id": len(rows)},
        "split_counts": {"unused": len(rows)},
        "available_days": [day],
        "train_days": [],
        "holdout_days": [],
        "source_sequence_coverage": 1.0,
        "label_row_reuse_count": 0,
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": forbidden_live,
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
    write_json(source_dir / "manifest.json", source_manifest(summary_path, rows_path, private_claim=private_claim, materialized=materialized))
    return summary_path


def make_case(name, days, rows_per_day=40, **kwargs):
    case_dir = fixture_root / name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    summaries = []
    duplicate_candidate = None
    duplicate_action = None
    for index, day in enumerate(days):
        if kwargs.get("duplicate_candidate") and index == 1:
            duplicate_candidate = f"handoff-{days[0]}-candidate-0"
        else:
            duplicate_candidate = None
        if kwargs.get("duplicate_action") and index == 1:
            duplicate_action = f"handoff-{days[0]}-action-0"
        else:
            duplicate_action = None
        handoff_id = "duplicate-handoff" if kwargs.get("duplicate_handoff") and index < 2 else None
        summaries.append(
            materialize_source(
                case_dir,
                day,
                rows_per_day,
                handoff_id=handoff_id,
                fixture_only=kwargs.get("fixture_only", False),
                duplicate_candidate=duplicate_candidate,
                duplicate_action=duplicate_action,
                forbidden_live=kwargs.get("forbidden_live", False) and index == 0,
                private_claim=kwargs.get("private_claim", False) and index == 0,
                write_rows=not (kwargs.get("missing_rows", False) and index == 0),
                count_delta=1 if kwargs.get("count_mismatch", False) and index == 0 else 0,
                materialized=not (kwargs.get("manifest_not_materialized", False) and index == 0),
            )
        )
    return summaries


def run_case(name, summaries):
    output = out_dir / name
    cmd = [
        "python3",
        str(script),
        "--allow-fixture-like-paths-for-smoke",
        "--fixture-output",
        "--created-utc",
        created_utc,
        "--output-dir",
        str(output),
    ]
    for summary in summaries:
        cmd.extend(["--bridge-summary", str(summary)])
    subprocess.run(cmd, check=True)
    return json.loads((output / "manifest.json").read_text())


days = ["2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24", "2026-05-25"]
cases = {
    "complete": run_case("complete", make_case("complete", days)),
    "single_day": run_case("single_day", make_case("single_day", ["2026-05-26"], rows_per_day=120)),
    "duplicate_handoff": run_case("duplicate_handoff", make_case("duplicate_handoff", days, duplicate_handoff=True)),
    "duplicate_candidate": run_case("duplicate_candidate", make_case("duplicate_candidate", days, duplicate_candidate=True)),
    "duplicate_action": run_case("duplicate_action", make_case("duplicate_action", days, duplicate_action=True)),
    "fixture_as_real": run_case("fixture_as_real", make_case("fixture_as_real", days, fixture_only=True)),
    "forbidden_live": run_case("forbidden_live", make_case("forbidden_live", days, forbidden_live=True)),
    "missing_rows": run_case("missing_rows", make_case("missing_rows", days, missing_rows=True)),
    "provenance_mismatch": run_case("provenance_mismatch", make_case("provenance_mismatch", days, count_mismatch=True)),
    "private_claim": run_case("private_claim", make_case("private_claim", days, private_claim=True)),
}

complete_summary = json.loads((out_dir / "complete" / "observable_pre_action_training_bridge_summary.json").read_text())
checks = {
    "complete_keep": cases["complete"]["decision_label"]
    == "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_READY",
    "complete_outputs_present": bool(cases["complete"]["outputs"]["joined_rows"])
    and bool(cases["complete"]["outputs"]["summary"]),
    "complete_chronological_split": complete_summary["train_days"] == ["2026-05-21", "2026-05-22", "2026-05-23"]
    and complete_summary["holdout_days"] == ["2026-05-24", "2026-05-25"]
    and complete_summary["split_counts"] == {"holdout": 80, "train": 120},
    "single_day_unknown": cases["single_day"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS",
    "duplicate_handoff_unknown": cases["duplicate_handoff"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS"
    and "duplicate_handoff_id" in cases["duplicate_handoff"]["blockers"],
    "duplicate_candidate_unknown": cases["duplicate_candidate"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS"
    and "duplicate_candidate_exact_id_across_sources" in cases["duplicate_candidate"]["blockers"],
    "duplicate_action_unknown": cases["duplicate_action"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS"
    and "duplicate_action_exact_id_across_sources" in cases["duplicate_action"]["blockers"],
    "fixture_as_real_discard": cases["fixture_as_real"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_FORBIDDEN_INPUT",
    "forbidden_live_discard": cases["forbidden_live"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_FORBIDDEN_INPUT",
    "missing_rows_unknown": cases["missing_rows"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS",
    "provenance_mismatch_unknown": cases["provenance_mismatch"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS",
    "private_claim_discard": cases["private_claim"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_FORBIDDEN_INPUT",
    "no_private_deployable_promotion_claim": all(
        case["private_truth_ready"] is False and case["deployable"] is False and case["promotion_gate"]["passed"] is False
        for case in cases.values()
    ),
}
manifest = {
    "artifact": "xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator_smoke",
    "case_decisions": {name: data["decision_label"] for name, data in cases.items()},
    "checks": checks,
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_SMOKE_FAIL",
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
