#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_strategy_discovery_train_holdout_selector_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_strategy_discovery_train_holdout_selector.py >> "$log" 2>&1

real_out="$out_dir/real"
bad_out="$out_dir/bad"

python3 scripts/xuan_strategy_discovery_train_holdout_selector.py \
  --output-dir "$real_out" \
  >> "$log" 2>&1

python3 - "$out_dir/bad_strategy_reset.json" "$out_dir/bad_ce25_spec.json" "$out_dir/bad_ce25_scorer.json" "$out_dir/bad_completion.json" "$out_dir/bad_tail_gap.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

bad_strategy_reset = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "selected_strategy_line": {"selected": None},
    "research_ranking": {"strategy_evidence": False},
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
}
bad_ce25_spec = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "research_ranking": {"no_order_diagnostic_allowed": False},
    "promotion_gate": {"passed": False},
}
bad_ce25_scorer = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "research_ranking": {"strategy_evidence": False},
    "promotion_gate": {"passed": False},
}
bad_completion = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "research_ranking": {"strategy_evidence": False},
    "promotion_gate": {"passed": False},
}
bad_tail_gap = {
    "decision_label": "UNKNOWN_BAD_FIXTURE",
    "research_ranking": {"strategy_evidence": False},
    "promotion_gate": {"passed": False},
}
payloads = [bad_strategy_reset, bad_ce25_spec, bad_ce25_scorer, bad_completion, bad_tail_gap]
for path, payload in zip(sys.argv[1:], payloads):
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_strategy_discovery_train_holdout_selector.py \
  --strategy-reset-manifest "$out_dir/bad_strategy_reset.json" \
  --ce25-guard-spec-manifest "$out_dir/bad_ce25_spec.json" \
  --ce25-guard-scorer-manifest "$out_dir/bad_ce25_scorer.json" \
  --symmetric-completion-manifest "$out_dir/bad_completion.json" \
  --tail-gap-manifest "$out_dir/bad_tail_gap.json" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 - "$real_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
queue = {item["lane"]: item for item in real["candidate_queue"]}
checks = {
    "real_keep": real["decision_label"] == "KEEP_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_READY",
    "selected_strict_cache_grid": real["selected_next_lane"]["lane"] == "symmetric_activation_strict_cache_grid_expansion",
    "selected_ready_local_only": real["selected_next_lane"]["status"] == "READY_LOCAL_ONLY",
    "ce25_prior_ready": real["ce25_feature_prior"]["status"] == "FEATURE_PRIOR_READY_NOT_EVIDENCE",
    "ce25_not_strategy_evidence": real["ce25_feature_prior"]["strategy_evidence"] is False,
    "ce25_queue_input_blocked": queue["ce25_projected_guard_train_holdout_transfer"]["status"] == "FEATURE_PRIOR_ONLY_INPUT_BLOCKED",
    "real_tail_pullback_absent": queue["symmetric_activation_tail_attribution_real_pullback_scoring"]["status"] == "BLOCKED_REAL_PULLBACK_ABSENT",
    "real_no_no_order": real["research_ranking"]["no_order_diagnostic_allowed"] is False,
    "real_promotion_false": real["promotion_gate"]["passed"] is False,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_BLOCKED",
    "bad_blocked": "no_ready_local_only_discovery_lane" in bad["blockers"],
    "bad_promotion_false": bad["promotion_gate"]["passed"] is False,
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"strategy discovery train/holdout selector smoke failed: {failed}")
manifest = {
    "artifact": "xuan_strategy_discovery_train_holdout_selector_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
