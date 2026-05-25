#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_strategy_discovery_grid_expansion_review_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_strategy_discovery_grid_expansion_review.py >> "$log" 2>&1

real_out="$out_dir/real"
bad_out="$out_dir/bad"

python3 scripts/xuan_strategy_discovery_grid_expansion_review.py \
  --output-dir "$real_out" \
  >> "$log" 2>&1

python3 - "$out_dir/bad_selector.json" "$out_dir/bad_cap100.json" "$out_dir/bad_cap102.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

bad_selector = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "selected_next_lane": {"lane": "none"},
    "promotion_gate": {"passed": False},
}
bad_grid = {
    "status": "DISCARD_ACTIVATION_DOES_NOT_FIX_LEAKAGE",
    "qualified": [],
    "filtered_row_count": 0,
    "row_count": 10,
    "config": {"strict_l1_pair_cap": 1.0},
}
for path, payload in zip(sys.argv[1:], [bad_selector, bad_grid, bad_grid]):
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_strategy_discovery_grid_expansion_review.py \
  --selector-manifest "$out_dir/bad_selector.json" \
  --cap100-manifest "$out_dir/bad_cap100.json" \
  --cap102-manifest "$out_dir/bad_cap102.json" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 - "$real_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
selected = real["cap102_result"]["selected_by_holdout_worst_net"] or {}
checks = {
    "real_keep": real["decision_label"] == "KEEP_STRATEGY_DISCOVERY_GRID_EXPANSION_CAP102_LEAD_READY",
    "cap100_discarded": real["cap100_result"]["status"] == "DISCARD_CAP100_OVERSTRICT_EMPTY_INPUT",
    "cap102_has_many_qualified": real["cap102_result"]["qualified_count"] >= 20,
    "selected_edge_075": selected.get("edge") == 0.075,
    "selected_holdout_positive": selected.get("holdout_worst_net_fee_after", 0) > 0,
    "selected_holdout_sample": selected.get("holdout_pair_actions", 0) >= 200,
    "selected_residual_bounded": selected.get("holdout_residual_qty_rate", 1) <= 0.04,
    "real_no_no_order": real["research_ranking"]["no_order_diagnostic_allowed"] is False,
    "real_not_strategy_evidence": real["research_ranking"]["strategy_evidence"] is False,
    "real_promotion_false": real["promotion_gate"]["passed"] is False,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_STRATEGY_DISCOVERY_GRID_EXPANSION_REVIEW_BLOCKED",
    "bad_promotion_false": bad["promotion_gate"]["passed"] is False,
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"strategy discovery grid expansion review smoke failed: {failed}")
manifest = {
    "artifact": "xuan_strategy_discovery_grid_expansion_review_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_STRATEGY_DISCOVERY_GRID_EXPANSION_REVIEW_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
