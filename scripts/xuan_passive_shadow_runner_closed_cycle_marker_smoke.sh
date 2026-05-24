#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_passive_shadow_runner_closed_cycle_marker_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile tools/xuan_dplus_passive_passive_shadow_runner.py >> "$log" 2>&1

python3 - "$ROOT" "$out_dir" <<'PY' >> "$log" 2>&1
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
tool_path = root / "tools/xuan_dplus_passive_passive_shadow_runner.py"
spec = importlib.util.spec_from_file_location("dplus_shadow_runner", tool_path)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def write_marker_summary(name: str, cfg):
    fixture_out = out_dir / name
    fixture_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner("btc-updown-5m-1900000000", fixture_out, cfg)
    runner.record_source_opportunity_marker(
        status="admitted",
        reason="candidate",
        side="YES",
        offset_s=95.0,
        qty=5.0,
        base_qty=5.0,
        target_qty=5.0,
        room_cost=77.5,
        imbalance_room=1.25,
        same_qty=0.0,
        opp_qty=0.75,
        activation_opp_age_ms=500,
        opposite_seen_ms=1_900_000_094_500,
        quote_intent_id="quote-admit",
        source_order_id=7,
        source_sequence_id="seq-admit",
    )
    runner.record_source_opportunity_marker(
        status="blocked",
        reason="target",
        side="YES",
        offset_s=130.0,
        qty=None,
        base_qty=5.0,
        target_qty=5.0,
        room_cost=12.0,
        imbalance_room=0.0,
        same_qty=6.0,
        opp_qty=1.0,
        activation_opp_age_ms=None,
        opposite_seen_ms=None,
        quote_intent_id=None,
        source_order_id=None,
        source_sequence_id="seq-block",
    )
    runner.write_summary(final=True)
    summary = json.loads(runner.summary_path.read_text())
    aggregate = mod.aggregate(fixture_out)
    return runner, summary, aggregate


base_cfg = mod.RunnerConfig(
    event_lite_summary=True,
    source_opportunity_marker_event_lite_summary=True,
)
default_runner, default_summary, default_aggregate = write_marker_summary("default_off", base_cfg)
default_diag = default_summary["event_lite"]["source_opportunity_marker_summary"]
assert "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp" not in default_diag
assert "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp" not in default_aggregate["event_lite"]["source_opportunity_marker_summary"]

enabled_cfg = mod.replace(base_cfg, source_opportunity_closed_cycle_marker_event_lite_summary=True)
enabled_runner, enabled_summary, enabled_aggregate = write_marker_summary("enabled", enabled_cfg)
diag = enabled_summary["event_lite"]["source_opportunity_marker_summary"]
agg_diag = enabled_aggregate["event_lite"]["source_opportunity_marker_summary"]

admitted_key = "YES|offset_90_120|repair_or_pairing_improving|open_le_1|deficit_0_25_1_25|opp_only_preseed|same_zero|opp_le_5"
blocked_key = "YES|offset_ge_120|risk_increasing|open_gt_5|deficit_le_0|both_preseed_sides|same_5_10|opp_le_5"
admitted_reason_key = f"admitted|candidate|{admitted_key}"
blocked_reason_key = f"blocked|target|{blocked_key}"

assert enabled_summary["config"]["source_opportunity_closed_cycle_marker_event_lite_summary"] is True
assert diag["field_contract"]["closed_cycle_marker_schema_version"] == "source_opportunity_closed_cycle_marker_v1"
assert diag["field_contract"]["post_action_outcome_labels_included"] is False
assert diag["field_contract"]["post_action_pair_residual_labels_included"] is False
assert diag["field_contract"]["gross_pair_pnl_available_at_marker"] is False
assert diag["field_contract"]["residual_stress_available_at_marker"] is False
assert diag["field_contract"]["requires_post_run_scorer_join_for_pnl"] is True
assert diag["transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"]["admitted"][admitted_key] == 1
assert diag["transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"]["blocked"][blocked_key] == 1
assert diag["transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key] == 1
assert diag["candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key] == 5.0
assert diag["base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][blocked_reason_key] == 5.0
assert diag["room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][blocked_reason_key] == 12.0
assert diag["pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key]["pending_same_qty_zero"] == 1
assert diag["opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key]["opposite_seen_present"] == 1
assert diag["activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key]["activation_opp_age_0_1s"] == 1
assert diag["quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key]["present"] == 1
assert diag["source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][admitted_reason_key]["present"] == 1
assert diag["source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][blocked_reason_key]["present"] == 1
assert diag["quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp"][blocked_reason_key]["missing"] == 1
assert agg_diag["transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"] == diag["transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"]
assert agg_diag["transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp"] == diag["transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp"]

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--event-lite-summary",
        "--source-opportunity-closed-cycle-marker-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--source-opportunity-closed-cycle-marker-event-lite-summary requires --source-opportunity-marker-event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_passive_shadow_runner_closed_cycle_marker_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_CLOSED_CYCLE_MARKER_SUMMARY_SMOKE_PASS",
    "scope": {
        "local_no_network_fixture": True,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "orders_cancels_redeems_sent": False,
    },
    "checks": {
        "default_off_closed_cycle_fields_absent": True,
        "enabled_closed_cycle_fields_present": True,
        "admitted_closed_cycle_marker_present": True,
        "blocked_closed_cycle_marker_present": True,
        "source_sequence_quote_order_coverage_present": True,
        "aggregate_merges_closed_cycle_marker_summary": True,
        "cli_requires_source_opportunity_marker": True,
        "post_action_outcome_labels_excluded": True,
        "post_action_pair_residual_labels_excluded": True,
        "trading_behavior_changed": False,
    },
    "artifacts": {
        "default_summary": str(default_runner.summary_path),
        "enabled_summary": str(enabled_runner.summary_path),
        "enabled_aggregate_report": str(out_dir / "enabled" / "aggregate_report.json"),
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
