#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_contract_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_contract.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import csv
import json
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_dir = out_dir / "fixtures"
fixture_dir.mkdir(parents=True, exist_ok=True)

script = Path("scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_contract.py")
feature_scorer_manifest = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/manifest.json"
)

CSV_FIELDS = [
    "schema_version",
    "profile_name",
    "validation_request_id",
    "day",
    "condition_id",
    "slug",
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "source_label",
    "ts_ms",
    "ts_iso",
    "side",
    "opposite_side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "trigger_ts_ms",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "outcome_labels_are_post_action",
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "external_shadow_ids_available",
    "external_shadow_id_policy",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
    "separator_export_reason",
]


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def load_manifest(path: Path) -> dict[str, object]:
    return json.loads((path / "manifest.json").read_text())


def write_csv(path: Path, *, drop_fields: set[str]) -> None:
    fields = [field for field in CSV_FIELDS if field not in drop_fields]
    rows = []
    for idx in range(10):
        day = f"2026-05-{idx % 5 + 1:02d}"
        row = {
            "schema_version": "candidate_seed_outcome_separator_v1",
            "profile_name": "fixture",
            "validation_request_id": "fixture",
            "day": day,
            "condition_id": f"condition-{idx}",
            "slug": f"btc-updown-5m-{1900000000 + idx * 300}",
            "source_seed_action_id": f"seed-action-{idx}",
            "source_seed_candidate_row_id": f"seed-row-{idx}",
            "source_label": "fixture",
            "ts_ms": str(1900000000000 + idx * 1000),
            "ts_iso": "2030-03-17T00:00:00Z",
            "side": "YES" if idx % 2 == 0 else "NO",
            "opposite_side": "NO" if idx % 2 == 0 else "YES",
            "offset_s": "30.0",
            "trigger_px": "0.45",
            "trigger_size": "5.0",
            "trigger_ts_ms": str(1900000000000 + idx * 1000),
            "seed_px": "0.45",
            "seed_qty": "5.0",
            "seed_cost": "2.25",
            "official_fee_rate": "0.0",
            "official_fee": "0.0",
            "source_risk_direction": "repair_or_pairing_improving" if idx % 3 == 0 else "risk_increasing",
            "pre_seed_same_qty": "0.0",
            "pre_seed_opp_qty": "5.0",
            "pre_seed_same_cost": "0.0",
            "pre_seed_opp_cost": "2.25",
            "pre_seed_open_qty": "5.0",
            "pre_seed_open_cost": "2.25",
            "ledger_proxy_before": "0.0",
            "ledger_proxy_after": "0.0",
            "source_pair_qty": "2.0",
            "source_pair_cost": "1.8",
            "source_pair_pnl": "0.2",
            "source_residual_qty": "0.0",
            "source_residual_cost": "0.0",
            "source_residual_age_s": "0.0",
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none",
            "outcome_labels_are_post_action": "true",
            "quote_intent_id": "",
            "source_order_id": "",
            "source_sequence_id": "",
            "external_shadow_ids_available": "false",
            "external_shadow_id_policy": "not_available",
            "pre_action_field_policy": "pre_action_only",
            "post_action_label_policy": "offline_train_holdout_only",
            "live_rule_safety_policy": "labels_forbidden_in_live_rule",
            "separator_export_reason": "fixture",
        }
        rows.append({key: value for key, value in row.items() if key in fields})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run_case(name, extra_args=None):
    case_dir = out_dir / name
    cmd = ["python3", str(script), "--output-dir", str(case_dir)]
    if extra_args:
        cmd.extend(extra_args)
    subprocess.run(cmd, check=True)
    return load_manifest(case_dir)


good = run_case("good")
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY", good
assert good["strategy_evidence"] is False
assert good["no_order_diagnostic_allowed"] is False
assert good["promotion_gate"]["passed"] is False

bad_label_csv = fixture_dir / "bad_missing_label.csv"
write_csv(bad_label_csv, drop_fields={"source_residual_cost"})
bad_label = run_case("bad_missing_label", ["--candidate-csv", str(bad_label_csv)])
assert bad_label["decision"] == "UNKNOWN"
assert any("offline_label_fields_missing" in item for item in bad_label["blockers"]), bad_label["blockers"]

bad_join_csv = fixture_dir / "bad_missing_join.csv"
write_csv(bad_join_csv, drop_fields={"side"})
bad_join = run_case("bad_missing_join", ["--candidate-csv", str(bad_join_csv)])
assert bad_join["decision"] == "UNKNOWN"
assert any("join_fields_missing" in item for item in bad_join["blockers"]), bad_join["blockers"]

bad_feature_scorer_path = fixture_dir / "bad_feature_scorer_manifest.json"
bad_feature_scorer = json.loads(feature_scorer_manifest.read_text())
bad_feature_scorer["decision_label"] = "UNKNOWN_BAD_FEATURE_SCORER"
write_json(bad_feature_scorer_path, bad_feature_scorer)
bad_feature = run_case("bad_feature_scorer", ["--feature-join-scorer-manifest", str(bad_feature_scorer_path)])
assert bad_feature["decision"] == "UNKNOWN"
assert any("feature_join_scorer_decision_not_expected" in item for item in bad_feature["blockers"]), bad_feature["blockers"]

summary = {
    "artifact": "xuan_observable_pre_action_rule_miner_training_input_bridge_contract_smoke",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_SMOKE_READY",
    "cases": {
        "good": good["decision_label"],
        "bad_missing_label": bad_label["decision_label"],
        "bad_missing_join": bad_join["decision_label"],
        "bad_feature_scorer": bad_feature["decision_label"],
    },
    "strategy_evidence": False,
    "no_order_diagnostic_allowed": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
}
write_json(out_dir / "manifest.json", summary)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" > /dev/null

echo "wrote $out_dir/manifest.json"
