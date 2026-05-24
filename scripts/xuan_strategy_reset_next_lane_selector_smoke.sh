#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_strategy_reset_next_lane_selector_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_strategy_reset_next_lane_selector.py >> "$log" 2>&1

real_out="$out_dir/real"
bad_out="$out_dir/bad"

python3 scripts/xuan_strategy_reset_next_lane_selector.py \
  --output-dir "$real_out" \
  >> "$log" 2>&1

python3 - "$out_dir/public_handoff.json" "$out_dir/bad_primary.json" "$out_dir/bad_support.json" "$out_dir/base.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

handoff = {
    "decision_label": "UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED",
    "research_ranking": {
        "status": "UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED",
        "exact_blockers": ["safe_72h_public_export_manifest_absent"],
        "no_order_diagnostic_allowed": False,
        "strategy_evidence": False,
    },
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
}
bad = {
    "artifact": "xuan_strategy_reset_next_lane_selector_bad_fixture",
    "status": "DISCARD_RESIDUAL_FRAGILE",
    "dataset_type": "fixture",
    "days": {"covered": ["2026-05-01"], "holdout": ["2026-05-02"]},
    "qualified": [
        {
            "edge": 0.06,
            "leak_rate": 0.04,
            "activation_window_s": 3.0,
            "min_opp_count": 1,
            "covered_pair_actions": 100,
            "holdout_pair_actions": 20,
            "covered_residual_qty_rate": 0.30,
            "holdout_residual_qty_rate": 0.40,
            "covered_worst_day": -1.0,
            "holdout_worst_day": -1.0,
            "covered_worst_net_fee_after": -10.0,
            "holdout_worst_net_fee_after": -5.0,
        }
    ],
}
Path(sys.argv[1]).write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
Path(sys.argv[3]).write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
Path(sys.argv[4]).write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_strategy_reset_next_lane_selector.py \
  --public-profile-handoff "$out_dir/public_handoff.json" \
  --primary-symmetric-manifest "$out_dir/bad_primary.json" \
  --support-symmetric-manifest "$out_dir/bad_support.json" \
  --base-residual-manifest "$out_dir/base.json" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 - "$real_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
selected = real["selected_strategy_line"]["selected"] or {}
checks = {
    "real_keep": real["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_NEXT_STRATEGY_SELECTED_RESEARCH_ONLY",
    "public_profile_discarded": real["public_profile_disposition"]["decision"] == "DISCARD",
    "selected_symmetric_line": real["research_ranking"]["selected_line"] == "symmetric_activation_residual_stress",
    "selected_has_holdout_sample": selected.get("holdout_pair_actions", 0) >= 200,
    "selected_holdout_positive": selected.get("holdout_worst_net_fee_after", 0) > 0,
    "selected_residual_bounded": selected.get("holdout_residual_qty_rate", 1) <= 0.05,
    "real_not_strategy_evidence": not real["research_ranking"]["strategy_evidence"],
    "real_no_no_order": not real["research_ranking"]["no_order_diagnostic_allowed"],
    "real_promotion_false": not real["promotion_gate"]["passed"],
    "bad_unknown": bad["decision_label"] == "UNKNOWN_STRATEGY_RESET_NEXT_LANE_SELECTOR_INSUFFICIENT",
    "bad_no_robust_blocker": "no_robust_symmetric_activation_candidate" in bad["blockers"],
    "bad_promotion_false": not bad["promotion_gate"]["passed"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"strategy reset next lane selector smoke failed: {failed}")
manifest = {
    "artifact": "xuan_strategy_reset_next_lane_selector_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_STRATEGY_RESET_NEXT_LANE_SELECTOR_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
