#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_source_opportunity_marker_reason_source_coverage_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_source_opportunity_marker_reason_source_coverage_spec.py"
score="$out_dir/spec_manifest.json"

python3 -m py_compile "$script" >> "$log" 2>&1
python3 "$script" --output "$score" >> "$log" 2>&1

python3 - "$out_dir" "$score" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
score_path = Path(sys.argv[2])
score = json.loads(score_path.read_text())
contract = score["field_contract"]

checks = {
    "spec_keep": score["decision_label"] == "KEEP_SOURCE_OPPORTUNITY_MARKER_REASON_SOURCE_COVERAGE_SPEC_READY",
    "default_off": score["implementation_policy"]["default_off"] is True,
    "exact_join_key_includes_reason_marker": contract["exact_join_key"] == "status|reason|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket",
    "source_sequence_presence_field_specified": "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit" in contract["new_bucket_fields"],
    "quote_intent_presence_field_specified": "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit" in contract["new_bucket_fields"],
    "post_action_labels_excluded": "source_pair_qty/source_pair_cost/source_pair_pnl" in contract["explicitly_excluded"],
    "private_truth_excluded": "private own order/fill/inventory truth" in contract["explicitly_excluded"],
    "promotion_gate_false": score["promotion_gate"]["passed"] is False,
    "no_side_effects": not any(
        score["scope"][key]
        for key in (
            "ssh_used",
            "shadow_started",
            "events_jsonl_read",
            "raw_replay_or_collector_scanned",
            "full_completion_store_scanned",
            "shared_ingress_connected_or_modified",
            "orders_cancels_redeems_sent",
        )
    ),
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"reason/source coverage spec smoke failed checks: {failed}")

manifest = {
    "artifact": "xuan_b27_dplus_source_opportunity_marker_reason_source_coverage_spec_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_SOURCE_OPPORTUNITY_MARKER_REASON_SOURCE_COVERAGE_SPEC_SMOKE_PASS",
    "spec_manifest": str(score_path),
    "checks": checks,
    "research_ranking": score["research_ranking"],
    "promotion_gate": score["promotion_gate"],
    "next_action": "Implement the default-off reason/source coverage summary locally if exact marker-by-reason/source coverage is required before another no-order diagnostic."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
