#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_source_opportunity_ledger_before_delta_marker_summary_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_source_opportunity_ledger_before_delta_marker_summary_scorer.py"
fixture_root="${SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_FIXTURE_ROOT:-}"
if [[ -z "$fixture_root" ]]; then
  fixture_root="$(find "$ROOT/xuan_research_artifacts" -maxdepth 1 -type d -name 'xuan_b27_dplus_passive_passive_shadow_runner_source_opportunity_ledger_before_delta_marker_smoke_*' | sort | tail -1)"
fi
if [[ -z "$fixture_root" || ! -d "$fixture_root" ]]; then
  echo "missing source-opportunity ledger before/delta marker smoke fixture root" >&2
  exit 2
fi

enabled_summary="$fixture_root/enabled/btc-updown-5m-1900000000.summary.json"
enabled_aggregate="$fixture_root/enabled/aggregate_report.json"
default_summary="$fixture_root/default_off/btc-updown-5m-1900000000.summary.json"
default_aggregate="$fixture_root/default_off/aggregate_report.json"
old_no_order_root="$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_review_ledger_marker_driver_20260523T133605Z/remote_clean/output"

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

old_rc=0
if [[ -d "$old_no_order_root" && -f "$old_no_order_root/aggregate_report.json" ]]; then
  set +e
  python3 "$script" \
    --aggregate-report "$old_no_order_root/aggregate_report.json" \
    --summary "$old_no_order_root" \
    --output-dir "$out_dir/old_no_order_score" >> "$log" 2>&1
  old_rc=$?
  set -e
  if [[ "$old_rc" -ne 3 ]]; then
    echo "expected old no-order fixture without before/delta marker summary to return 3, got $old_rc" >&2
    exit 1
  fi
fi

python3 - "$out_dir" "$fixture_root" "$default_rc" "$old_rc" "$old_no_order_root" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_root = sys.argv[2]
default_rc = int(sys.argv[3])
old_rc = int(sys.argv[4])
old_no_order_root = Path(sys.argv[5])

enabled = json.loads((out_dir / "enabled_score" / "manifest.json").read_text())
default_off = json.loads((out_dir / "default_off_score" / "manifest.json").read_text())
old_score_path = out_dir / "old_no_order_score" / "manifest.json"
old_score = json.loads(old_score_path.read_text()) if old_score_path.exists() else None

checks = {
    "py_compile": True,
    "enabled_keep": enabled["decision_label"] == "KEEP_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_DENOMINATOR_SCORER_READY",
    "enabled_aggregate_parity": enabled["aggregate_parity"]["passed"] is True,
    "enabled_before_marker_count_nonzero": enabled["ledger_before_marker_denominator"]["marker_total"] >= 1,
    "enabled_delta_marker_count_nonzero": enabled["ledger_delta_marker_denominator"]["marker_total"] >= 1,
    "enabled_delta_blocked_count_nonzero": enabled["ledger_delta_marker_denominator"]["blocked_marker_count"] >= 1,
    "enabled_before_exact_reason_coverage": enabled["ledger_before_reason_source_coverage"]["exact_reason_marker_total"] >= 1,
    "enabled_delta_exact_reason_coverage": enabled["ledger_delta_reason_source_coverage"]["exact_reason_marker_total"] >= 1,
    "enabled_schema_ok": enabled["field_checks"]["before_delta_schema_ok"] is True,
    "enabled_before_pre_action_declared": enabled["field_checks"]["ledger_before_pre_action_field_declared"] is True,
    "enabled_delta_pre_action_declared": enabled["field_checks"]["ledger_delta_pre_action_field_declared"] is True,
    "enabled_post_action_labels_excluded": enabled["field_checks"]["post_action_labels_excluded"] is True,
    "enabled_promotion_gate_false": enabled["promotion_gate"]["passed"] is False,
    "default_off_unknown": default_rc == 3 and default_off["decision_label"] == "UNKNOWN_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_SUMMARY_INPUTS_INSUFFICIENT",
    "default_off_missing_before_delta_fields": "required_source_opportunity_ledger_before_delta_marker_fields_missing" in default_off["blockers"],
    "old_no_order_unknown_if_available": True,
}
if old_score is not None:
    checks["old_no_order_unknown_if_available"] = (
        old_rc == 3
        and old_score["decision_label"] == "UNKNOWN_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_SUMMARY_INPUTS_INSUFFICIENT"
        and "required_source_opportunity_ledger_before_delta_marker_fields_missing" in old_score["blockers"]
    )

if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"source-opportunity ledger before/delta marker scorer smoke failed checks: {failed}")

manifest = {
    "artifact": "xuan_b27_dplus_source_opportunity_ledger_before_delta_marker_summary_scorer_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_SUMMARY_SCORER_SMOKE_PASS",
    "inputs": {
        "fixture_root": fixture_root,
        "old_no_order_root": str(old_no_order_root) if old_score is not None else None
    },
    "outputs": {
        "enabled_score": str(out_dir / "enabled_score" / "manifest.json"),
        "default_off_score": str(out_dir / "default_off_score" / "manifest.json"),
        "old_no_order_score": str(old_score_path) if old_score is not None else None
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_DENOMINATOR_SCORER_READY",
        "interpretation": "The scorer can validate before/delta marker fields/parity, detect selected before and delta denominators, and fail closed on default-off or old pullbacks without before/delta fields."
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
        "status": "LEDGER_BEFORE_DELTA_MARKER_SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE"
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
    "next_action": "Use this local scorer after a bounded before/delta-marker no-order pullback, or prepare the exact no-order review packet without enabling any guard."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
