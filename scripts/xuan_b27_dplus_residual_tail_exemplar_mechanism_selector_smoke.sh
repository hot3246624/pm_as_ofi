#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_residual_tail_exemplar_mechanism_selector_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_residual_tail_exemplar_mechanism_selector.py"
score_fixture="xuan_research_artifacts/xuan_b27_dplus_residual_tail_exemplar_shadow_scorer_smoke_20260522T065011Z/pass_score/score.json"

python3 -m py_compile "$script" >> "$log" 2>&1

python3 "$script" \
  --score "$score_fixture" \
  --output "$out_dir/keep_selector.json" >> "$log" 2>&1

python3 - "$score_fixture" "$out_dir/weak_score.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
obj = json.loads(src.read_text())
for row in obj["rankings"]["top_source_side_offset_risk_direction_groups"]:
    row["source_residual_cost_share"] = 0.10
    row["source_pair_qty_share"] = 0.01
obj["exemplar_metrics"]["top1_residual_cost_share"] = 0.10
dst.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
PY

set +e
python3 "$script" \
  --score "$out_dir/weak_score.json" \
  --output "$out_dir/unknown_selector.json" >> "$log" 2>&1
unknown_rc=$?
set -e

python3 - "$out_dir" "$unknown_rc" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
unknown_rc = int(sys.argv[2])
keep = json.loads((out_dir / "keep_selector.json").read_text())
unknown = json.loads((out_dir / "unknown_selector.json").read_text())
checks = {
    "keep_selected_ledger_context_verifier": keep["decision_label"] == "KEEP_RESIDUAL_TAIL_EXEMPLAR_LEDGER_CONTEXT_VERIFIER_SELECTED",
    "keep_target_is_local_only": keep["selected_next_target"]["name"] == "residual_tail_ledger_context_verifier_v1",
    "keep_does_not_recommend_summary_removal": keep["guardrails"]["summary_only_removal_recommended"] is False,
    "keep_promotion_gate_false": keep["promotion_gate"]["passed"] is False,
    "keep_remote_not_approved": keep["guardrails"]["remote_run_approved"] is False,
    "unknown_rejected_weak_signal": unknown_rc != 0 and unknown["decision_label"] == "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_MECHANISM_SIGNAL_INSUFFICIENT",
    "unknown_names_concentration_blocker": "residual_tail_not_concentrated_enough" in unknown["blockers"],
    "no_side_effects": keep["side_effects"]["ssh_started"] is False
    and keep["side_effects"]["events_jsonl_read"] is False
    and keep["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"residual-tail exemplar mechanism selector smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_residual_tail_exemplar_mechanism_selector_smoke",
    "status": "PASS",
    "decision_label": "KEEP_RESIDUAL_TAIL_EXEMPLAR_MECHANISM_SELECTOR_READY",
    "hypothesis": "A local selector can turn residual-tail exemplar scorer output into a bounded local verifier target without recommending summary-only removal, frozen cap/price/cooldown families, remote runs, or promotion.",
    "inputs": {
        "score_fixture": "xuan_research_artifacts/xuan_b27_dplus_residual_tail_exemplar_shadow_scorer_smoke_20260522T065011Z/pass_score/score.json"
    },
    "outputs": {
        "keep_selector": str(out_dir / "keep_selector.json"),
        "unknown_selector": str(out_dir / "unknown_selector.json")
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_RESIDUAL_TAIL_EXEMPLAR_MECHANISM_SELECTOR_READY",
        "interpretation": "The selector is local tooling for future approved shadow score pullbacks. It is not strategy economics or promotion evidence."
    },
    "promotion_gate": {
        "passed": False,
        "status": "SELECTOR_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "service_control_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False
    },
    "next_action": "If a real approved residual-tail exemplar shadow score returns a KEEP selector output, implement only the named local verifier before any further remote run."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ residual-tail exemplar mechanism selector smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
