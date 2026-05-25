#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-xuan_research_artifacts/xuan_observable_pre_action_rule_miner_feature_join_contract_smoke_20260525T192536Z}"
CREATED_UTC="2026-05-25T19:25:36Z"
SCRIPT="scripts/xuan_observable_pre_action_rule_miner_feature_join_contract.py"

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}/fixtures"
BAD_RUNNER="${OUT_DIR}/fixtures/bad_runner_missing_source_link.py"

python3 "${SCRIPT}" \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/good_real_source"

python3 - <<'PY' "${BAD_RUNNER}"
from pathlib import Path
dst = Path(__import__("sys").argv[1])
dst.write_text("# intentionally missing source-link hooks for fail-closed smoke\n")
PY

python3 "${SCRIPT}" \
  --runner "${BAD_RUNNER}" \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/bad_runner"

python3 - <<'PY' "${OUT_DIR}/fixtures/bad_inventory.json"
import json
import sys
from pathlib import Path
src = Path("xuan_research_artifacts/xuan_observable_pre_action_rule_miner_input_inventory_20260525T185536Z/manifest.json")
obj = json.load(src.open())
obj["decision_label"] = "KEEP_FAKE_READY_INVENTORY"
json.dump(obj, open(sys.argv[1], "w"), indent=2, sort_keys=True)
PY

python3 "${SCRIPT}" \
  --inventory-manifest "${OUT_DIR}/fixtures/bad_inventory.json" \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/bad_inventory"

python3 - <<'PY' "${OUT_DIR}" "${CREATED_UTC}"
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
created = sys.argv[2]

def load(path: Path):
    with path.open() as fh:
        return json.load(fh)

good = load(out / "good_real_source" / "manifest.json")
bad_runner = load(out / "bad_runner" / "manifest.json")
bad_inventory = load(out / "bad_inventory" / "manifest.json")

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_READY_EXPORTER_TARGET_NAMED", good["decision_label"]
assert good["current_direct_materialized_export_present"] is False
assert good["research_ranking"]["strategy_evidence"] is False
assert good["promotion_gate"]["passed"] is False
assert "observable_pre_action_candidate_rows.jsonl" in good["contract"]["phase_1_default_off_runner_exporter_target"]["writes_allowlisted_output_files"][0]
assert bad_runner["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_SOURCE_BLOCKED"
assert any(blocker.startswith("runner:") for blocker in bad_runner["blockers"]), bad_runner["blockers"]
assert bad_inventory["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_SOURCE_BLOCKED"
assert any(blocker.startswith("inventory:") for blocker in bad_inventory["blockers"]), bad_inventory["blockers"]

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_feature_join_contract_smoke",
    "schema_version": 1,
    "created_utc": created,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_SMOKE_PASS",
    "checks": {
        "real_source_contract_ready": True,
        "current_direct_export_not_claimed_ready": True,
        "bad_runner_fails_closed": True,
        "bad_inventory_fails_closed": True,
        "strategy_evidence_false": True,
        "promotion_gate_false": True
    },
    "strategy_evidence": False,
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False
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
        "trading_behavior_changed": False
    }
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"decision_label": manifest["decision_label"], "output": str(out)}, sort_keys=True))
PY
