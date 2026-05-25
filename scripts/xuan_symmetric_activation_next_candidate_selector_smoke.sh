#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_next_candidate_selector_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_symmetric_activation_next_candidate_selector.py >> "$log" 2>&1

real_out="$out_dir/real"
python3 scripts/xuan_symmetric_activation_next_candidate_selector.py \
  --output-dir "$real_out" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
real = json.loads((out / "real/manifest.json").read_text())
assert real["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_TAIL_JOIN_EXACT_DIAGNOSTIC_PROPOSAL_READY"
proposal = real["proposal"]
assert proposal["approval_required_in_thread"] is True
assert proposal["no_order_diagnostic_allowed_now"] is False
assert "--symmetric-activation-projected-residual-tail-join-event-lite-summary" in proposal[
    "exact_remote_command_if_later_approved"
]
assert proposal["acceptance_gates"]["private_truth_ready"] is False
assert proposal["acceptance_gates"]["deployable"] is False
assert proposal["acceptance_gates"]["promotion_gate_passed"] is False
assert real["promotion_gate"]["passed"] is False

bad_scorer = out / "bad_scorer.json"
scorer = json.loads(
    Path(
        "xuan_research_artifacts/"
        "xuan_symmetric_activation_projected_residual_tail_join_summary_smoke_20260525T145535Z/"
        "scorer/manifest.json"
    ).read_text()
)
scorer["decision_label"] = "UNKNOWN_MUTATED_FOR_SMOKE"
bad_scorer.write_text(json.dumps(scorer, indent=2, sort_keys=True) + "\n")

bad_completion = out / "bad_completion.json"
completion = json.loads(
    Path(
        "xuan_research_artifacts/"
        "xuan_symmetric_activation_shadow_review_edge0075_completion_20260525T133305Z/"
        "manifest.json"
    ).read_text()
)
completion["research_ranking"]["economic_pnl_positive"] = False
completion["acceptance_adapter"]["shadow_acceptance_gate"]["metrics"]["fee_adjusted_pair_pnl_proxy"] = -1.0
bad_completion.write_text(json.dumps(completion, indent=2, sort_keys=True) + "\n")
PY

unknown_out="$out_dir/unknown"
python3 scripts/xuan_symmetric_activation_next_candidate_selector.py \
  --tail-join-scorer-manifest "$out_dir/bad_scorer.json" \
  --output-dir "$unknown_out" >> "$log" 2>&1

discard_out="$out_dir/discard"
python3 scripts/xuan_symmetric_activation_next_candidate_selector.py \
  --completion-manifest "$out_dir/bad_completion.json" \
  --output-dir "$discard_out" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
real = json.loads((out / "real/manifest.json").read_text())
unknown = json.loads((out / "unknown/manifest.json").read_text())
discard = json.loads((out / "discard/manifest.json").read_text())
assert unknown["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_NEXT_CANDIDATE_SELECTOR_INPUTS_INSUFFICIENT"
assert discard["decision_label"] == "DISCARD_SYMMETRIC_ACTIVATION_EDGE075_WINDOW20_FOLLOWUP_NOT_JUSTIFIED"
manifest = {
    "artifact": "xuan_symmetric_activation_next_candidate_selector_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_NEXT_CANDIDATE_SELECTOR_SMOKE_PASS",
    "checks": {
        "real_selector_keep_proposal_ready": True,
        "proposal_requires_approval": True,
        "proposal_has_projected_tail_join_flag": True,
        "bad_scorer_unknown": True,
        "economic_negative_discard": True,
    },
    "scope": {
        "local_only": True,
        "new_data_fetched": False,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "raw_replay_full_store_scanned": False,
        "shared_ingress_or_live_modified": False,
        "orders_cancels_redeems_sent": False,
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
