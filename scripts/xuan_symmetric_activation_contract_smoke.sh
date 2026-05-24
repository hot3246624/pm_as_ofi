#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_contract_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_symmetric_activation_contract.py >> "$log" 2>&1

good_out="$out_dir/good"
bad_selector_out="$out_dir/bad_selector"
bad_runner_out="$out_dir/bad_runner"

python3 scripts/xuan_symmetric_activation_contract.py \
  --output-dir "$good_out" \
  >> "$log" 2>&1

python3 - "$out_dir/bad_selector.json" "$out_dir/bad_runner.py" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

bad_selector = {
    "decision": "UNKNOWN",
    "decision_label": "UNKNOWN_STRATEGY_RESET_NEXT_LANE_SELECTOR_INSUFFICIENT",
    "research_ranking": {"selected_line": None},
    "selected_strategy_line": {"selected": None},
}
Path(sys.argv[1]).write_text(json.dumps(bad_selector, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text("# missing activation contract tokens\n")
PY

python3 scripts/xuan_symmetric_activation_contract.py \
  --selector-manifest "$out_dir/bad_selector.json" \
  --output-dir "$bad_selector_out" \
  >> "$log" 2>&1

python3 scripts/xuan_symmetric_activation_contract.py \
  --runner-source "$out_dir/bad_runner.py" \
  --output-dir "$bad_runner_out" \
  >> "$log" 2>&1

python3 - "$good_out/manifest.json" "$bad_selector_out/manifest.json" "$bad_runner_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad_selector = json.loads(Path(sys.argv[2]).read_text())
bad_runner = json.loads(Path(sys.argv[3]).read_text())
checks = {
    "good_keep": good["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_CONTRACT_READY",
    "good_runner_ready": good["runner_implementability"]["ready"] is True,
    "good_min_opp_supported": good["runner_implementability"]["supports_min_opp_count"] is True,
    "good_contract_schema": good["contract"]["summary_schema"]["schema_version"] == "symmetric_activation_contract_summary_v1",
    "good_default_off": good["contract"]["default_off_cli"]["not_a_trading_rule"] is True,
    "good_research_not_strategy_evidence": not good["research_ranking"]["strategy_evidence"],
    "good_no_no_order": not good["research_ranking"]["no_order_diagnostic_allowed"],
    "good_promotion_false": not good["promotion_gate"]["passed"],
    "bad_selector_unknown": bad_selector["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_CONTRACT_INPUTS_INSUFFICIENT",
    "bad_selector_blocker": "selected_strategy_line_missing" in bad_selector["blockers"],
    "bad_runner_unknown": bad_runner["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_CONTRACT_INPUTS_INSUFFICIENT",
    "bad_runner_token_blocker": any(item.startswith("missing_runner_token_") for item in bad_runner["blockers"]),
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"symmetric activation contract smoke failed: {failed}")
manifest = {
    "artifact": "xuan_symmetric_activation_contract_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_CONTRACT_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_selector_manifest": sys.argv[2],
    "bad_runner_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
