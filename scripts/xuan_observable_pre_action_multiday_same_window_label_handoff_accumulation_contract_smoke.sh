#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/xuan_multiday_label_handoff_accumulation_pycache_$ts}"
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT

python3 -m py_compile scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract.py >> "$log" 2>&1

python3 - "$out_dir" "$ts" <<'PY' >> "$log" 2>&1
import json
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
created_utc = sys.argv[2]
case_dir = out_dir / "cases"
case_dir.mkdir(parents=True, exist_ok=True)
script = Path("scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract.py")


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def handoff(day, rows, *, handoff_id=None, duplicate_candidate=None, fixture=False, forbidden_live=False, private_claim=False):
    handoff_id = handoff_id or f"handoff-{day}"
    candidate_ids = [f"{handoff_id}-candidate-{idx}" for idx in range(rows)]
    if duplicate_candidate is not None and candidate_ids:
        candidate_ids[0] = duplicate_candidate
    return {
        "handoff_id": handoff_id,
        "day": day,
        "row_count": rows,
        "join_coverage": 1.0,
        "source_sequence_coverage": 1.0,
        "source_seed_candidate_row_ids": candidate_ids,
        "source_seed_action_ids": [f"{handoff_id}-action-{idx}" for idx in range(max(1, rows // 4))],
        "labels_allowed_for_train_holdout_scoring": True,
        "labels_allowed_in_live_predicate": forbidden_live,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "source_pair_source_residual_labels_are_post_action": True,
        "fixture": fixture,
        "private_truth_ready": private_claim,
        "deployable": False,
        "promotion_gate": {"passed": False},
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
        "ssh_used": False,
        "canary_or_live_started": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False,
    }


def case(name, handoffs, **overrides):
    data = {
        "schema_version": "observable_pre_action_multiday_same_window_label_handoff_accumulation_case_v1",
        "fixture": False,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
        "ssh_used": False,
        "canary_or_live_started": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False,
        "labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "handoffs": handoffs,
    }
    data.update(overrides)
    path = case_dir / f"{name}.json"
    write_json(path, data)
    return path


def run_case(name, path):
    output = out_dir / name
    subprocess.run(
        ["python3", str(script), "--case-json", str(path), "--created-utc", created_utc, "--output-dir", str(output)],
        check=True,
    )
    return json.loads((output / "manifest.json").read_text())


days = ["2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24", "2026-05-25"]
complete_handoffs = [handoff(day, 40) for day in days]
duplicate_handoffs = [handoff(day, 40) for day in days]
duplicate_handoffs[1]["source_seed_candidate_row_ids"][0] = duplicate_handoffs[0]["source_seed_candidate_row_ids"][0]

cases = {
    "complete": run_case("complete", case("complete", complete_handoffs)),
    "single_day": run_case("single_day", case("single_day", [handoff("2026-05-26", 120)])),
    "duplicate_id": run_case("duplicate_id", case("duplicate_id", duplicate_handoffs)),
    "fixture": run_case("fixture", case("fixture", [handoff(day, 40, fixture=True) for day in days])),
    "forbidden_live": run_case(
        "forbidden_live",
        case("forbidden_live", [handoff(day, 40, forbidden_live=(idx == 0)) for idx, day in enumerate(days)]),
    ),
    "private_claim": run_case(
        "private_claim",
        case("private_claim", [handoff(day, 40, private_claim=(idx == 0)) for idx, day in enumerate(days)]),
    ),
}

checks = {
    "complete_keep": cases["complete"]["decision_label"]
    == "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CASE_READY",
    "complete_split_ready": cases["complete"]["case_validation"]["score"]["train_rows"] == 120
    and cases["complete"]["case_validation"]["score"]["holdout_rows"] == 80,
    "single_day_unknown": cases["single_day"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CASE_GAPS",
    "duplicate_id_unknown": cases["duplicate_id"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CASE_GAPS"
    and "duplicate_candidate_exact_id_across_handoffs" in cases["duplicate_id"]["case_validation"]["blockers"],
    "fixture_discard": cases["fixture"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_FORBIDDEN_INPUT",
    "forbidden_live_discard": cases["forbidden_live"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_FORBIDDEN_INPUT",
    "private_claim_discard": cases["private_claim"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_FORBIDDEN_INPUT",
    "no_private_deployable_promotion_claim": all(
        case_manifest["private_truth_ready"] is False
        and case_manifest["deployable"] is False
        and case_manifest["promotion_gate"]["passed"] is False
        for case_manifest in cases.values()
    ),
}
manifest = {
    "artifact": "xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_smoke",
    "case_decisions": {name: data["decision_label"] for name, data in cases.items()},
    "checks": checks,
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_SMOKE_FAIL",
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
