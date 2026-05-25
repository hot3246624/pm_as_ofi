#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_strategy_discovery_symmetric_activation_no_order_proposal_review_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_strategy_discovery_symmetric_activation_no_order_proposal_review.py >> "$log" 2>&1

real_out="$out_dir/real"
bad_out="$out_dir/bad"

python3 scripts/xuan_strategy_discovery_symmetric_activation_no_order_proposal_review.py \
  --output-dir "$real_out" \
  >> "$log" 2>&1

python3 - "$out_dir/bad_grid.json" "$out_dir/bad_prior.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

bad_grid = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "research_ranking": {"strategy_evidence": False, "selected_local_candidate": None},
    "source_inputs": {"cap102_manifest": ""},
    "promotion_gate": {"passed": False},
}
bad_prior = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "run_config": {
        "edge": 0.06,
        "activation_window_s": 5.0,
        "activation_mode": "none",
    },
    "disabled": {},
    "promotion_gate": {"passed": False},
}
Path(sys.argv[1]).write_text(json.dumps(bad_grid, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text(json.dumps(bad_prior, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_strategy_discovery_symmetric_activation_no_order_proposal_review.py \
  --grid-review-manifest "$out_dir/bad_grid.json" \
  --prior-diagnostic-manifest "$out_dir/bad_prior.json" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 - "$real_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
cmd = real["proposal"]["exact_remote_command_if_later_approved"]
checks = {
    "real_keep": real["decision_label"] == "KEEP_SELECTED_CAP102_CANDIDATE_NO_ORDER_PROPOSAL_LOCAL_ONLY_READY",
    "comparison_ready": real["research_ranking"]["comparison_ready"] is True,
    "selected_edge_075": real["research_ranking"]["selected_local_candidate"]["edge"] == 0.075,
    "selected_window_20": real["research_ranking"]["selected_local_candidate"]["activation_window_s"] == 20.0,
    "baseline_edge_07": real["research_ranking"]["baseline_previous_no_order_params"]["edge"] == 0.07,
    "approval_required": real["proposal"]["approval_required_in_thread"] is True,
    "no_order_not_allowed_now": real["proposal"]["no_order_diagnostic_allowed_now"] is False,
    "cmd_edge_075": "--edge 0.075" in cmd,
    "cmd_window_20": "--activation-window-s 20" in cmd,
    "cmd_dry_run": "PM_DRY_RUN=true" in cmd,
    "cmd_tail_attribution": "--symmetric-activation-tail-attribution-event-lite-summary" in cmd,
    "post_eval_tail_scorer": any(
        "xuan_symmetric_activation_tail_attribution_summary_scorer.py" in item
        for item in real["proposal"]["post_run_local_only_evaluation"]
    ),
    "real_not_strategy_evidence": real["research_ranking"]["strategy_evidence"] is False,
    "real_promotion_false": real["promotion_gate"]["passed"] is False,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_SELECTED_CAP102_CANDIDATE_NO_ORDER_PROPOSAL_INPUTS_INSUFFICIENT",
    "bad_promotion_false": bad["promotion_gate"]["passed"] is False,
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"symmetric activation no-order proposal smoke failed: {failed}")
manifest = {
    "artifact": "xuan_strategy_discovery_symmetric_activation_no_order_proposal_review_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_NO_ORDER_PROPOSAL_REVIEW_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
