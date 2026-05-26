#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_arrival_inventory_smoke_$ts}"
case_dir="$ROOT/tmp_scripts/xuan_same_window_label_handoff_arrival_inventory_real_like_$ts"
mkdir -p "$out_dir" "$case_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  scripts/xuan_observable_pre_action_same_window_label_handoff_arrival_inventory.py \
  scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py >> "$log" 2>&1

python3 - "$case_dir" "$out_dir" <<'PY' >> "$log" 2>&1
import csv
import json
import sys
from pathlib import Path

case_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
case_dir.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def feature_row(idx: int, *, day: str = "2026-05-26") -> dict[str, object]:
    admitted = idx % 2 == 0
    same_qty = 0.0 if idx % 3 == 0 else 5.0
    opp_qty = 5.0 if same_qty == 0.0 else 0.0
    return {
        "schema_version": "observable_pre_action_candidate_row_v1",
        "condition_id": f"condition-{idx}",
        "market_slug": f"btc-updown-5m-{2100000000 + idx * 300}",
        "day_id": day,
        "quote_ts_ms": 2100000000000 + idx * 1000,
        "side": "YES" if admitted else "NO",
        "offset_s": float(60 + idx),
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else "target",
        "block_reason": "" if admitted else "target",
        "source_sequence_present": True,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "pre_seed_same_qty": same_qty,
        "pre_seed_opp_qty": opp_qty,
        "pre_seed_same_cost": same_qty * 0.4,
        "pre_seed_opp_cost": opp_qty * 0.4,
        "pre_seed_open_qty": same_qty + opp_qty,
        "pre_seed_open_cost": (same_qty + opp_qty) * 0.4,
        "candidate_qty": 5.0,
        "source_risk_direction": "repair_or_pairing_improving" if admitted else "risk_increasing",
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def label_for(row: dict[str, object], *, live_policy: str = "offline labels are forbidden in live predicates") -> dict[str, object]:
    return {
        "condition_id": row["condition_id"],
        "slug": row["market_slug"],
        "day": row["day_id"],
        "ts_ms": row["quote_ts_ms"],
        "trigger_ts_ms": row["quote_ts_ms"],
        "side": row["side"],
        "offset_s": row["offset_s"],
        "source_risk_direction": row["source_risk_direction"],
        "pre_seed_same_qty": row["pre_seed_same_qty"],
        "pre_seed_opp_qty": row["pre_seed_opp_qty"],
        "pre_seed_same_cost": row["pre_seed_same_cost"],
        "pre_seed_opp_cost": row["pre_seed_opp_cost"],
        "pre_seed_open_qty": row["pre_seed_open_qty"],
        "pre_seed_open_cost": row["pre_seed_open_cost"],
        "seed_qty": row["candidate_qty"],
        "trigger_size": row["candidate_qty"],
        "source_pair_qty": "2.0",
        "source_pair_cost": "1.5",
        "source_pair_pnl": "0.25",
        "source_residual_qty": "0.0",
        "source_residual_cost": "0.0",
        "source_residual_age_s": "0.0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "residual_none",
        "outcome_labels_are_post_action": "True",
        "pre_action_field_policy": "pre-action context fields only",
        "post_action_label_policy": "source_pair/source_residual labels train-holdout only",
        "live_rule_safety_policy": live_policy,
        "source_seed_candidate_row_id": "",
        "source_seed_action_id": "",
    }


def write_label_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_case(name: str, rows: list[dict[str, object]], labels: list[dict[str, object]], *, fixture: bool = False, live_predicate: bool = False, missing_feature: bool = False) -> Path:
    d = case_dir / name
    d.mkdir(parents=True, exist_ok=True)
    feature_rows = d / "observable_pre_action_candidate_rows.jsonl"
    source_summary = d / "observable_pre_action_source_link_summary.json"
    feature_manifest = d / "observable_pre_action_feature_join_manifest.json"
    same_window = d / "same_window_aggregate_summary.json"
    label_csv = d / "offline_labels.csv"
    handoff = d / "same_window_offline_label_handoff.json"
    if not missing_feature:
        write_jsonl(feature_rows, rows)
    write_label_csv(label_csv, labels)
    field_contract = {
        "default_off": True,
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }
    write_json(source_summary, {"schema_version": "observable_pre_action_source_link_summary_v1", "row_count": len(rows), "field_contract": field_contract})
    write_json(feature_manifest, {"schema_version": "observable_pre_action_feature_join_manifest_v1", "candidate_row_count": len(rows), "field_contract": field_contract})
    write_json(same_window, {"schema_version": "same_window_aggregate_summary_v1", "candidate_row_count": len(rows)})
    write_json(
        handoff,
        {
            "schema_version": "observable_pre_action_same_window_offline_label_handoff_v1",
            "handoff_id": f"{name}_handoff",
            "fixture": fixture,
            "strategy_evidence": False,
            "scope": {
                "current_worktree_only": True,
                "local_only": True,
                "new_data_fetched": False,
                "external_worktree_read": False,
                "ssh_used": False,
                "shadow_started": False,
                "canary_or_live_started": False,
                "events_jsonl_read": False,
                "events_jsonl_pulled": False,
                "raw_replay_or_full_store_scanned": False,
                "shared_ingress_or_broker_or_live_modified": False,
                "shared_ws_or_local_agg_or_service_started": False,
                "orders_cancels_redeems_sent": False,
                "trading_behavior_changed": False,
            },
            "provenance": {
                "events_jsonl_read": False,
                "events_jsonl_pulled": False,
                "raw_replay_or_full_store_scanned": False,
                "external_worktree_read": False,
                "ssh_used_by_validator": False,
                "orders_cancels_redeems_sent": False,
                "trading_behavior_changed": False,
            },
            "label_policy": {
                "labels_allowed_for_train_holdout_scoring": True,
                "labels_allowed_in_live_predicate": live_predicate,
                "realized_pair_cost_allowed_as_live_criteria": False,
            },
            "files": {
                "feature_join_candidate_rows": str(feature_rows),
                "feature_join_source_link_summary": str(source_summary),
                "feature_join_manifest": str(feature_manifest),
                "same_window_aggregate_summary": str(same_window),
                "offline_label_csv": str(label_csv),
            },
            "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
        },
    )
    return handoff


features = [feature_row(idx) for idx in range(10)]
labels = [label_for(row) for row in features]
old_labels = [label_for(feature_row(idx, day="2026-05-18")) for idx in range(10)]
forbidden_labels = [label_for(row, live_policy="labels allowed in live predicates") for row in features]

paths = {
    "good": str(write_case("real_like", features, labels)),
    "old": str(write_case("old_non_overlap", features, old_labels)),
    "fixture": str(write_case("fixture_case", features, labels, fixture=True)),
    "missing": str(write_case("missing_case", features, labels, missing_feature=True)),
    "forbidden": str(write_case("forbidden_live", features, forbidden_labels, live_predicate=True)),
}
write_json(out_dir / "paths.json", paths)
PY

common_args=(
  --min-feature-rows 4
  --min-joined-rows 4
  --min-join-coverage 0.95
  --min-slug-overlap-count 1
  --min-condition-overlap-count 1
  --created-utc "2026-05-26T00:00:00Z"
)

python3 scripts/xuan_observable_pre_action_same_window_label_handoff_arrival_inventory.py \
  "${common_args[@]}" \
  --output-dir "$out_dir/default" >> "$log" 2>&1

for name in good old fixture missing forbidden; do
  handoff="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$out_dir/paths.json" "$name")"
  python3 scripts/xuan_observable_pre_action_same_window_label_handoff_arrival_inventory.py \
    "${common_args[@]}" \
    --candidate-handoff-manifest "$handoff" \
    --only-explicit-handoffs \
    --output-dir "$out_dir/$name" >> "$log" 2>&1
done

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])


def load(name: str) -> dict:
    return json.loads((out / name / "manifest.json").read_text())


default = load("default")
good = load("good")
old = load("old")
fixture = load("fixture")
missing = load("missing")
forbidden = load("forbidden")

assert default["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert "non_fixture_same_window_label_handoff_manifest_absent" in default["readiness"]["blockers"]
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_READY"
assert good["readiness"]["ready"] is True
assert old["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert fixture["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert missing["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert forbidden["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
for manifest in (default, good, old, fixture, missing, forbidden):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["private_truth_ready"] is False
    assert manifest["deployable"] is False
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["safety"]["events_jsonl_read"] is False
    assert manifest["safety"]["raw_replay_or_full_store_scan"] is False

summary = {
    "artifact": "xuan_observable_pre_action_same_window_label_handoff_arrival_inventory_smoke",
    "checks": {
        "default_no_real_handoff_unknown": True,
        "explicit_good_handoff_keep": True,
        "old_non_overlap_not_ready": True,
        "fixture_handoff_not_ready": True,
        "missing_handoff_not_ready": True,
        "forbidden_live_handoff_not_ready": True,
        "promotion_gate_separated": True,
    },
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_SMOKE_READY",
    "deployable": False,
    "no_order_diagnostic_allowed": False,
    "private_truth_ready": False,
    "promotion_gate": {"passed": False},
    "schema_version": 1,
    "strategy_evidence": False,
}
(out / "manifest.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/default/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null

echo "smoke ok: $out_dir"
