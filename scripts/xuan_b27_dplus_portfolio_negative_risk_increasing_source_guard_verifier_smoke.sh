#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_portfolio_negative_risk_increasing_source_guard_verifier_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_portfolio_negative_risk_increasing_source_guard_verifier.py"
python3 -m py_compile "$script" scripts/xuan_b27_dplus_dynamic_condition_ledger_risk_state_verifier.py scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py >> "$log" 2>&1

set +e
python3 "$script" --output-dir "$out_dir/run" >> "$log" 2>&1
run_status=$?
set -e
test "$run_status" -eq 0 || test "$run_status" -eq 1

python3 - "$out_dir/run/manifest.json" "$out_dir" "$run_status" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
run_status = int(sys.argv[3])
score = json.loads(manifest_path.read_text())
checks = {
    "decision_known": score["decision"] in {"KEEP", "UNKNOWN", "DISCARD"},
    "decision_label_present": isinstance(score.get("decision_label"), str) and bool(score["decision_label"]),
    "default_off_local_verifier": score["mechanism"]["default_off_local_verifier"] is True,
    "not_price_cap": score["mechanism"]["not_price_cap"] is True,
    "not_summary_only_removal": score["mechanism"]["not_summary_only_removal"] is True,
    "repair_allowed_metric_present": "negative_ledger_repair_allowed_count" in score["research_ranking"],
    "promotion_gate_false": score["promotion_gate"]["passed"] is False,
    "private_truth_false": score["promotion_gate"]["private_truth_ready"] is False,
    "no_remote_side_effects": score["side_effects"]["ssh_started"] is False
    and score["side_effects"]["shared_ingress_connected"] is False
    and score["side_effects"]["orders_sent"] is False,
    "run_status_matches_decision": (run_status == 0) == (score["decision"] == "KEEP"),
}
failed = [key for key, value in checks.items() if not value]
if failed:
    raise AssertionError(f"portfolio negative risk guard smoke failed checks: {failed}")
smoke = {
    "artifact": "xuan_b27_dplus_portfolio_negative_risk_increasing_source_guard_verifier_smoke",
    "status": "PASS",
    "decision": "KEEP",
    "decision_label": "KEEP_PORTFOLIO_NEGATIVE_RISK_GUARD_VERIFIER_SMOKE_PASS",
    "run_manifest": str(manifest_path),
    "run_decision": score["decision"],
    "run_decision_label": score["decision_label"],
    "checks": checks,
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
        "status": "SMOKE_ONLY_NOT_PROMOTION_EVIDENCE"
    },
    "side_effects": score["side_effects"],
}
(out_dir / "manifest.json").write_text(json.dumps(smoke, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
printf 'PASS D+ portfolio negative risk guard verifier smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
