#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_source_opportunity_closed_cycle_marker_summary_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_source_opportunity_closed_cycle_marker_summary_scorer.py"
fixture_root="${SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_FIXTURE_ROOT:-}"
if [[ -z "$fixture_root" ]]; then
  fixture_root="$(find "$ROOT/xuan_research_artifacts" -maxdepth 1 -type d -name 'xuan_passive_shadow_runner_closed_cycle_marker_smoke_*' | sort | tail -1)"
fi
if [[ -z "$fixture_root" || ! -d "$fixture_root" ]]; then
  echo "missing source-opportunity closed-cycle marker smoke fixture root" >&2
  exit 2
fi

enabled_summary="$fixture_root/enabled/btc-updown-5m-1900000000.summary.json"
enabled_aggregate="$fixture_root/enabled/aggregate_report.json"
default_summary="$fixture_root/default_off/btc-updown-5m-1900000000.summary.json"
default_aggregate="$fixture_root/default_off/aggregate_report.json"

python3 -m py_compile "$script" >> "$log" 2>&1

python3 "$script" \
  --aggregate-report "$enabled_aggregate" \
  --summary "$enabled_summary" \
  --output-dir "$out_dir/enabled_score" >> "$log" 2>&1

set +e
python3 "$script" \
  --aggregate-report "$default_aggregate" \
  --summary "$default_summary" \
  --output-dir "$out_dir/default_off_score" >> "$log" 2>&1
default_rc=$?
set -e
if [[ "$default_rc" -ne 3 ]]; then
  echo "expected default-off fixture to return 3, got $default_rc" >&2
  exit 1
fi

python3 - "$out_dir" "$fixture_root" "$default_rc" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_root = sys.argv[2]
default_rc = int(sys.argv[3])

enabled = json.loads((out_dir / "enabled_score" / "manifest.json").read_text())
default_off = json.loads((out_dir / "default_off_score" / "manifest.json").read_text())

checks = {
    "py_compile": True,
    "enabled_keep": enabled["decision_label"] == "KEEP_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_DENOMINATOR_SCORER_READY",
    "enabled_aggregate_parity": enabled["aggregate_parity"]["passed"] is True,
    "enabled_marker_count_nonzero": enabled["closed_cycle_marker_denominator"]["marker_total"] >= 1,
    "enabled_admitted_marker_count_nonzero": enabled["closed_cycle_marker_denominator"]["admitted_marker_count"] >= 1,
    "enabled_exact_reason_coverage": enabled["closed_cycle_reason_source_coverage"]["exact_reason_marker_total"] >= 1,
    "enabled_schema_ok": enabled["field_checks"]["schema_ok"] is True,
    "enabled_join_key_ok": enabled["field_checks"]["join_key_ok"] is True,
    "enabled_closed_cycle_pre_action_declared": enabled["field_checks"]["closed_cycle_pre_action_fields_declared"] is True,
    "enabled_post_action_outcome_labels_excluded": enabled["field_checks"]["post_action_outcome_labels_excluded"] is True,
    "enabled_pair_residual_labels_excluded": enabled["field_checks"]["post_action_pair_residual_labels_excluded"] is True,
    "enabled_requires_post_run_join": enabled["field_checks"]["requires_post_run_scorer_join_for_pnl"] is True,
    "enabled_promotion_gate_false": enabled["promotion_gate"]["passed"] is False,
    "default_off_unknown": default_rc == 3 and default_off["decision_label"] == "UNKNOWN_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_SUMMARY_INPUTS_INSUFFICIENT",
    "default_off_missing_closed_cycle_fields": "required_source_opportunity_closed_cycle_marker_fields_missing" in default_off["blockers"],
}

if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"source-opportunity closed-cycle marker scorer smoke failed checks: {failed}")

manifest = {
    "artifact": "xuan_source_opportunity_closed_cycle_marker_summary_scorer_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_SUMMARY_SCORER_SMOKE_PASS",
    "inputs": {
        "fixture_root": fixture_root,
    },
    "outputs": {
        "enabled_score": str(out_dir / "enabled_score" / "manifest.json"),
        "default_off_score": str(out_dir / "default_off_score" / "manifest.json"),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_DENOMINATOR_SCORER_READY",
        "interpretation": "The scorer can validate closed-cycle marker fields/parity, detect selected denominators, and fail closed on default-off pullbacks without closed-cycle fields."
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
        "status": "CLOSED_CYCLE_MARKER_SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE"
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "full_completion_store_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "service_control_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False
    },
    "next_action": "Use this local scorer after a bounded b55-style closed-cycle no-order pullback, or switch lines immediately if the denominator is absent/thin."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
