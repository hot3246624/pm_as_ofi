#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_acceptance_adapter_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_symmetric_activation_acceptance_adapter_spec.py >> "$log" 2>&1

default_out="$out_dir/default"
good_fixture="$out_dir/good_fixture.json"
good_out="$out_dir/good"
bad_fixture="$out_dir/bad_fixture.json"
bad_out="$out_dir/bad"

python3 scripts/xuan_symmetric_activation_acceptance_adapter_spec.py \
  --output-dir "$default_out" \
  >> "$log" 2>&1

python3 - "$good_fixture" "$bad_fixture" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

good_path = Path(sys.argv[1])
bad_path = Path(sys.argv[2])
base = {
    "schema_version": "symmetric_activation_acceptance_adapter_input_v1",
    "fixture": True,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
    "shadow_acceptance": {
        "acceptance_passed": True,
        "checks": {"no_order_safety_ok": True},
        "trading_metrics": {
            "candidates": 128,
            "net_pair_cost_p90": 0.99,
            "residual_qty_share_of_filled": 0.12,
            "residual_cost_share_of_filled_cost": 0.14,
            "pair_tail_loss_share_of_pair_pnl": 0.02,
            "material_residual_lots": 0,
            "fee_adjusted_pair_pnl_proxy": 2.5
        }
    },
    "symmetric_activation_score": {
        "decision_label": "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY",
        "promotion_gate": {"passed": False},
        "research_ranking": {"strategy_evidence": False},
        "score": {
            "checks": {
                "schema_ok": True,
                "field_contract_ok": True,
                "aggregate_parity": True,
                "source_sequence_coverage_present": True
            },
            "summary": {
                "blocked_activation_denominator": 3,
                "admitted_activation_denominator": 4,
                "aggregate_total": 7,
                "summary_file_total": 7
            }
        }
    }
}
good_path.write_text(json.dumps(base, indent=2, sort_keys=True) + "\n")
bad = json.loads(json.dumps(base))
bad["shadow_acceptance"]["trading_metrics"]["candidates"] = 20
bad["shadow_acceptance"]["trading_metrics"]["net_pair_cost_p90"] = 1.07
bad["symmetric_activation_score"]["score"]["checks"]["source_sequence_coverage_present"] = False
bad["symmetric_activation_score"]["score"]["summary"]["blocked_activation_denominator"] = 0
bad_path.write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_symmetric_activation_acceptance_adapter_spec.py \
  --candidate-manifest "$good_fixture" \
  --output-dir "$good_out" \
  >> "$log" 2>&1

set +e
python3 scripts/xuan_symmetric_activation_acceptance_adapter_spec.py \
  --candidate-manifest "$bad_fixture" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1
bad_rc=$?
set -e
if [[ "$bad_rc" -eq 0 ]]; then
  echo "bad fixture unexpectedly passed" >> "$log"
  exit 1
fi

python3 - "$default_out/manifest.json" "$good_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

default = json.loads(Path(sys.argv[1]).read_text())
good = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
checks = {
    "default_keep_spec_ready": default["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_READY_REAL_INPUT_UNKNOWN",
    "default_real_unknown": default["real_input_status"] == "UNKNOWN_NO_SAFE_PULLBACK",
    "default_candidate_missing": default["adapter_evaluation"]["candidate_status"] == "UNKNOWN_NO_SAFE_PULLBACK",
    "default_summary_ready": default["summary_scorer_ready"] is True,
    "default_not_strategy_evidence": default["research_ranking"]["strategy_evidence"] is False,
    "default_promotion_false": default["promotion_gate"]["passed"] is False,
    "good_keep_fixture": good["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_READY_FIXTURE_PASS_NOT_REAL",
    "good_candidate_passed": good["adapter_evaluation"]["candidate_gate_passed"] is True,
    "good_fixture_marked": good["adapter_evaluation"]["fixture"] is True,
    "good_shadow_gate": good["adapter_evaluation"]["shadow_acceptance_gate"]["passed"] is True,
    "good_activation_gate": good["adapter_evaluation"]["symmetric_activation_gate"]["passed"] is True,
    "good_not_strategy_evidence": good["research_ranking"]["strategy_evidence"] is False,
    "good_promotion_false": good["promotion_gate"]["passed"] is False,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_INPUTS_INSUFFICIENT",
    "bad_candidate_failed": bad["adapter_evaluation"]["candidate_gate_passed"] is False,
    "bad_fail_closed_candidate": bad["adapter_evaluation"]["candidate_status"] == "ADAPTER_INPUT_FAIL_CLOSED",
    "bad_candidate_blocker": "shadow_candidates_below_min" in bad["blockers"],
    "bad_activation_blocker": "activation_activation_source_sequence_coverage_present_not_true" in bad["blockers"],
    "bad_promotion_false": bad["promotion_gate"]["passed"] is False,
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"symmetric activation acceptance adapter smoke failed: {failed}")
manifest = {
    "artifact": "xuan_symmetric_activation_acceptance_adapter_spec_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_SMOKE_PASS",
    "checks": checks,
    "default_manifest": sys.argv[1],
    "good_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
