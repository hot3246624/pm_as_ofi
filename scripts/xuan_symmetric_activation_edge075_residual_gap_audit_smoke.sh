#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${XUAN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_edge075_residual_gap_audit_smoke_$ts}"
mkdir -p "$out_dir/good" "$out_dir/bad"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_symmetric_activation_edge075_residual_gap_audit.py >> "$log" 2>&1

python3 scripts/xuan_symmetric_activation_edge075_residual_gap_audit.py \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 - <<'PY' "$out_dir"
import json
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
driver = Path("xuan_research_artifacts/xuan_symmetric_activation_shadow_review_edge0075_driver_20260525T120202Z")
review = driver / "local_review_20260525T133422Z"
bad = root / "bad_inputs"
bad.mkdir(parents=True, exist_ok=True)

for name, src in {
    "aggregate.json": driver / "remote_clean/output/aggregate_report.json",
    "shadow.json": review / "shadow_acceptance/manifest.json",
    "activation.json": review / "symmetric_activation_summary_scorer/manifest.json",
    "tail.json": review / "symmetric_activation_tail_attribution_scorer_v3/manifest.json",
    "adapter.json": review / "symmetric_activation_acceptance_adapter_wrapped/manifest.json",
}.items():
    shutil.copyfile(src, bad / name)

tail = json.loads((bad / "tail.json").read_text())
tail["decision_label"] = "UNKNOWN_MUTATED_FOR_SMOKE"
(bad / "tail.json").write_text(json.dumps(tail, indent=2, sort_keys=True) + "\n")
PY

set +e
python3 scripts/xuan_symmetric_activation_edge075_residual_gap_audit.py \
  --aggregate "$out_dir/bad_inputs/aggregate.json" \
  --shadow-acceptance-manifest "$out_dir/bad_inputs/shadow.json" \
  --symmetric-activation-scorer-manifest "$out_dir/bad_inputs/activation.json" \
  --tail-attribution-scorer-manifest "$out_dir/bad_inputs/tail.json" \
  --acceptance-adapter-manifest "$out_dir/bad_inputs/adapter.json" \
  --output-dir "$out_dir/bad" >> "$log" 2>&1
bad_rc=$?
set -e

python3 - <<'PY' "$out_dir" "$bad_rc"
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
bad_rc = int(sys.argv[2])
good = json.loads((out / "good/manifest.json").read_text())
bad = json.loads((out / "bad/manifest.json").read_text())
checks = {
    "good_keep": good["decision_label"]
    == "KEEP_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_PROJECTED_RESIDUAL_JOIN_TARGET_READY",
    "good_has_missing_join_fields": bool(good["evaluation"]["missing_legal_join_fields"]),
    "good_no_promotion": good["promotion_gate"]["passed"] is False,
    "bad_nonzero_rc": bad_rc != 0,
    "bad_unknown_or_discard": bad["decision"] in {"UNKNOWN", "DISCARD"},
    "scope_no_shadow": good["scope"]["shadow_started"] is False,
    "scope_no_events": good["scope"]["events_jsonl_read"] is False,
}
manifest = {
    "artifact": "xuan_symmetric_activation_edge075_residual_gap_audit_smoke",
    "schema_version": 1,
    "checks": checks,
    "passed": all(checks.values()),
    "good_manifest": str(out / "good/manifest.json"),
    "bad_manifest": str(out / "bad/manifest.json"),
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
if not manifest["passed"]:
    raise SystemExit(1)
PY

printf '%s\n' "$out_dir/manifest.json"
