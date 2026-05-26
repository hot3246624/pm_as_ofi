#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_non_fixture_training_bridge_materializer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_root = out_dir / "fixtures"
fixture_root.mkdir(parents=True, exist_ok=True)
script = Path("scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py")

csv_fields = [
    "schema_version",
    "source_seed_candidate_row_id",
    "source_seed_action_id",
    "condition_id",
    "slug",
    "day",
    "ts_ms",
    "trigger_ts_ms",
    "side",
    "offset_s",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "seed_qty",
    "trigger_size",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "outcome_labels_are_post_action",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
]


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def write_csv(path, rows, fields=None):
    fields = fields or csv_fields
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def field_contract(**overrides):
    data = {
        "schema_version": "observable_pre_action_feature_join_field_contract_v1",
        "default_off": True,
        "writes_events_jsonl": False,
        "reads_events_jsonl": False,
        "raw_replay_or_full_store_scan": False,
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate_passed": False,
    }
    data.update(overrides)
    return data


def feature_row(i, *, day):
    admitted = i % 2 == 0
    qty = 1.25
    risk = "repair_or_pairing_improving" if i % 3 == 0 else "risk_increasing"
    same_qty = 0.0 if risk == "repair_or_pairing_improving" else 1.25
    opp_qty = 1.25 if risk == "repair_or_pairing_improving" else 0.0
    return {
        "schema_version": "observable_pre_action_candidate_row_v1",
        "source_seed_candidate_row_id": f"seed-row-{i}",
        "source_seed_action_id": f"seed-action-{i}" if admitted else "",
        "condition_id": f"condition-{i // 10}",
        "market_slug": f"btc-updown-5m-{1900000000 + (i // 10) * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + i * 1000,
        "side": "YES" if i % 2 == 0 else "NO",
        "offset_s": float(30 + (i % 90)),
        "offset_bucket": "offset_30_120",
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else "target",
        "block_reason": None if admitted else "target",
        "source_sequence_present": True,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "pre_seed_same_qty": same_qty,
        "pre_seed_opp_qty": opp_qty,
        "pre_seed_same_cost": round(same_qty * 0.44, 8),
        "pre_seed_opp_cost": round(opp_qty * 0.44, 8),
        "pre_seed_open_qty": round(same_qty + opp_qty, 8),
        "pre_seed_open_cost": round((same_qty + opp_qty) * 0.44, 8),
        "candidate_qty": qty,
        "candidate_qty_bucket": "candidate_qty_1_2",
        "source_risk_direction": risk,
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def label_row(feature, i):
    return {
        "schema_version": "candidate_seed_outcome_separator_v1",
        "source_seed_candidate_row_id": feature["source_seed_candidate_row_id"],
        "source_seed_action_id": feature.get("source_seed_action_id") or "",
        "condition_id": feature["condition_id"],
        "slug": feature["market_slug"],
        "day": feature["day_id"],
        "ts_ms": str(feature["quote_ts_ms"]),
        "trigger_ts_ms": str(feature["quote_ts_ms"]),
        "side": feature["side"],
        "offset_s": str(feature["offset_s"]),
        "source_risk_direction": feature["source_risk_direction"],
        "pre_seed_same_qty": str(feature["pre_seed_same_qty"]),
        "pre_seed_opp_qty": str(feature["pre_seed_opp_qty"]),
        "pre_seed_same_cost": str(feature["pre_seed_same_cost"]),
        "pre_seed_opp_cost": str(feature["pre_seed_opp_cost"]),
        "pre_seed_open_qty": str(feature["pre_seed_open_qty"]),
        "pre_seed_open_cost": str(feature["pre_seed_open_cost"]),
        "seed_qty": str(feature["candidate_qty"]),
        "trigger_size": str(feature["candidate_qty"]),
        "source_pair_qty": "2.0",
        "source_pair_cost": "1.75",
        "source_pair_pnl": "0.25",
        "source_residual_qty": "0.0" if i % 3 == 0 else "1.0",
        "source_residual_cost": "0.0" if i % 3 == 0 else "0.35",
        "source_residual_age_s": "0.0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "residual_none" if i % 3 == 0 else "residual_small",
        "outcome_labels_are_post_action": "true",
        "pre_action_field_policy": "pre_action_fields_only",
        "post_action_label_policy": "offline_train_holdout_only",
        "live_rule_safety_policy": "labels_forbidden_in_live_rule",
    }


def handoff(row_count, **overrides):
    data = {
        "schema_version": "observable_pre_action_same_window_offline_label_handoff_v1",
        "fixture": False,
        "row_count": row_count,
        "files": {
            "feature_join_candidate_rows": "observable_pre_action_candidate_rows.jsonl",
            "feature_join_source_link_summary": "observable_pre_action_source_link_summary.json",
            "feature_join_manifest": "observable_pre_action_feature_join_manifest.json",
            "offline_label_csv": "observable_pre_action_same_window_offline_labels.csv",
            "same_window_aggregate_summary": "same_window_aggregate_summary.json",
        },
        "scope": {
            "events_jsonl_pulled": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "orders_cancels_redeems_sent": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "trading_behavior_changed": False,
        },
        "provenance": {
            "events_jsonl_pulled": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "orders_cancels_redeems_sent": False,
            "generated_from_in_memory_no_order_runner_state": True,
            "trading_behavior_changed": False,
        },
        "label_policy": {
            "labels_allowed_for_train_holdout_scoring": True,
            "labels_allowed_in_live_predicate": False,
            "realized_pair_cost_allowed_as_live_criteria": False,
            "source_pair_source_residual_labels_are_post_action": True,
        },
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    }
    data.update(overrides)
    return data


def materialize(case, *, days, rows_per_day=30, mutator=None):
    case_dir = fixture_root / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    features = []
    for day in days:
        for _ in range(rows_per_day):
            features.append(feature_row(len(features), day=day))
    labels = [label_row(feature, i) for i, feature in enumerate(features)]
    source_contract = field_contract()
    manifest_contract = field_contract()
    handoff_manifest = handoff(len(features))
    fields = list(csv_fields)
    if mutator:
        mutator(features, labels, source_contract, manifest_contract, handoff_manifest, fields)
    write_jsonl(case_dir / "observable_pre_action_candidate_rows.jsonl", features)
    write_csv(case_dir / "observable_pre_action_same_window_offline_labels.csv", labels, fields)
    write_json(
        case_dir / "observable_pre_action_source_link_summary.json",
        {
            "schema_version": "observable_pre_action_source_link_summary_v1",
            "row_count": len(features),
            "field_contract": source_contract,
        },
    )
    write_json(
        case_dir / "observable_pre_action_feature_join_manifest.json",
        {
            "schema_version": "observable_pre_action_feature_join_manifest_v1",
            "candidate_row_count": len(features),
            "field_contract": manifest_contract,
        },
    )
    write_json(
        case_dir / "same_window_aggregate_summary.json",
        {
            "schema_version": "same_window_aggregate_summary_v1",
            "offline_label_row_count": len(labels),
            "metrics": {"candidates": len(features)},
            "strategy_evidence": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate": {"passed": False},
        },
    )
    write_json(case_dir / "observable_pre_action_same_window_offline_label_handoff.json", handoff_manifest)
    return case_dir


def run_case(case, case_dir):
    output_dir = out_dir / case
    subprocess.run(
        [
            "python3",
            str(script),
            "--handoff-manifest",
            str(case_dir / "observable_pre_action_same_window_offline_label_handoff.json"),
            "--feature-rows",
            str(case_dir / "observable_pre_action_candidate_rows.jsonl"),
            "--source-link-summary",
            str(case_dir / "observable_pre_action_source_link_summary.json"),
            "--feature-join-manifest",
            str(case_dir / "observable_pre_action_feature_join_manifest.json"),
            "--offline-label-csv",
            str(case_dir / "observable_pre_action_same_window_offline_labels.csv"),
            "--same-window-aggregate-summary",
            str(case_dir / "same_window_aggregate_summary.json"),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
    )
    return json.loads((output_dir / "manifest.json").read_text())


days = ["2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24", "2026-05-25"]
cases = {}
cases["complete"] = run_case("complete", materialize("complete", days=days, rows_per_day=40))
cases["single_day"] = run_case("single_day", materialize("single_day", days=["2026-05-26"], rows_per_day=120))


def drop_exact(features, labels, source_contract, manifest_contract, handoff_manifest, fields):
    features[0].pop("source_seed_candidate_row_id", None)


cases["missing_exact"] = run_case(
    "missing_exact",
    materialize("missing_exact", days=days, rows_per_day=40, mutator=drop_exact),
)


def duplicate_label(features, labels, source_contract, manifest_contract, handoff_manifest, fields):
    labels[1]["source_seed_candidate_row_id"] = labels[0]["source_seed_candidate_row_id"]


cases["duplicate_label"] = run_case(
    "duplicate_label",
    materialize("duplicate_label", days=days, rows_per_day=40, mutator=duplicate_label),
)


def forbidden_policy(features, labels, source_contract, manifest_contract, handoff_manifest, fields):
    handoff_manifest["label_policy"]["labels_allowed_in_live_predicate"] = True


cases["forbidden_policy"] = run_case(
    "forbidden_policy",
    materialize("forbidden_policy", days=days, rows_per_day=40, mutator=forbidden_policy),
)

checks = {
    "complete_keep": cases["complete"]["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_READY",
    "complete_outputs_present": bool(cases["complete"]["outputs"]["joined_rows"]) and bool(cases["complete"]["outputs"]["summary"]),
    "single_day_unknown_materialized": cases["single_day"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_TRAIN_HOLDOUT_GAPS"
    and cases["single_day"]["materialized"] is True,
    "missing_exact_fail_closed": cases["missing_exact"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZER_FAIL_CLOSED"
    and cases["missing_exact"]["materialized"] is False,
    "duplicate_label_fail_closed": cases["duplicate_label"]["decision_label"]
    == "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZER_FAIL_CLOSED"
    and cases["duplicate_label"]["materialized"] is False,
    "forbidden_policy_discard": cases["forbidden_policy"]["decision_label"]
    == "DISCARD_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_FORBIDDEN_POLICY",
    "no_private_deployable_promotion_claim": all(
        case["private_truth_ready"] is False
        and case["deployable"] is False
        and case["promotion_gate"]["passed"] is False
        for case in cases.values()
    ),
}
manifest = {
    "artifact": "xuan_observable_pre_action_non_fixture_training_bridge_materializer_smoke",
    "checks": checks,
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZER_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZER_SMOKE_FAIL",
    "case_decisions": {name: case["decision_label"] for name, case in cases.items()},
    "outputs": {
        name: {
            "manifest": str(out_dir / name / "manifest.json"),
            "joined_rows": case["outputs"]["joined_rows"],
            "summary": case["outputs"]["summary"],
        }
        for name, case in cases.items()
    },
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
