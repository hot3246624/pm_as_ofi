#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_projected_residual_tail_join_summary_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  tools/xuan_dplus_passive_passive_shadow_runner.py \
  scripts/xuan_symmetric_activation_projected_residual_tail_join_summary_scorer.py >> "$log" 2>&1

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


def make_lot(
    mod,
    *,
    lot_id: int,
    side: str,
    qty: float,
    px: float,
    ts_ms: int,
    offset_s: float,
    seq: str,
    age_ms: int,
    projected_residual_bucket: str,
) -> object:
    return mod.Lot(
        id=lot_id,
        quote_intent_id=f"fixture-{lot_id}",
        side=side,
        qty=qty,
        px=px,
        fill_ms=ts_ms - age_ms,
        source_order_id=lot_id,
        trigger_px=px + 0.05,
        trigger_size=qty,
        offset_s=offset_s,
        trigger_ts_ms=ts_ms - age_ms,
        trigger_source_sequence_id=seq,
        source_risk_direction="risk_increasing",
        activation_required=True,
        activation_opp_age_ms=3_000,
        opposite_seen_ms=ts_ms - age_ms - 3_000,
        symmetric_activation_status="admitted",
        symmetric_activation_reason="candidate",
        symmetric_projected_residual_bucket=projected_residual_bucket,
    )


def run_case(root_out: Path, cfg: object) -> dict[str, object]:
    root_out.mkdir(parents=True, exist_ok=True)
    slug = "btc-updown-5m-1900000000"
    runner = mod.DPlusRunner(slug, root_out, cfg)
    base_ts_ms = int(mod.round_start_from_slug(slug) * 1000) + 120_000
    runner.lots["YES"].append(
        make_lot(
            mod,
            lot_id=1,
            side="YES",
            qty=2.0,
            px=0.52,
            ts_ms=base_ts_ms,
            offset_s=65.0,
            seq="seq-yes-pair",
            age_ms=8_000,
            projected_residual_bucket="projected_residual_gt_20pct",
        )
    )
    runner.lots["NO"].append(
        make_lot(
            mod,
            lot_id=2,
            side="NO",
            qty=2.0,
            px=0.43,
            ts_ms=base_ts_ms,
            offset_s=65.0,
            seq="seq-no-pair",
            age_ms=7_000,
            projected_residual_bucket="projected_residual_lt_10pct",
        )
    )
    runner.lots["YES"].append(
        make_lot(
            mod,
            lot_id=3,
            side="YES",
            qty=1.5,
            px=0.40,
            ts_ms=base_ts_ms,
            offset_s=65.0,
            seq="seq-yes-residual",
            age_ms=6_000,
            projected_residual_bucket="projected_residual_gt_20pct",
        )
    )
    runner.pair_inventory(base_ts_ms)
    runner.write_summary(final=True)
    return {
        "runner": runner,
        "summary": json.loads(runner.summary_path.read_text()),
    }


base_cfg = mod.RunnerConfig(
    event_lite_summary=True,
    activation_mode="opp_seen",
    activation_window_s=20.0,
    salvage_age_ms=10_000_000,
    salvage_net_cap=0.0,
)

default = run_case(out_dir / "default_off", base_cfg)
enabled_cfg = mod.replace(base_cfg, symmetric_activation_projected_residual_tail_join_event_lite_summary=True)
enabled = run_case(out_dir / "enabled", enabled_cfg)

default_agg = mod.aggregate(out_dir / "default_off")
enabled_agg = mod.aggregate(out_dir / "enabled")

assert default["summary"]["metrics"]["pair_actions"] == enabled["summary"]["metrics"]["pair_actions"] == 1
assert abs(default["summary"]["metrics"]["residual_qty"] - enabled["summary"]["metrics"]["residual_qty"]) <= 1e-9
assert "symmetric_activation_projected_residual_tail_join_summary" not in default_agg["event_lite"]

diag = enabled_agg["event_lite"]["symmetric_activation_projected_residual_tail_join_summary"]
projected_key = "admitted|candidate|projected_residual_gt_20pct"
side_key = "admitted|candidate|YES|offset_60_90|projected_residual_gt_20pct"
assert diag["schema_version"] == "symmetric_activation_projected_residual_tail_join_summary_v1"
assert diag["field_contract"]["default_off"] is True
assert diag["field_contract"]["post_action_outcome_labels_included"] is False
assert diag["field_contract"]["realized_pair_cost_used_as_live_criteria"] is False
assert diag["field_contract"]["trading_behavior_changed"] is False
assert diag["field_contract"]["private_truth_ready"] is False
assert diag["field_contract"]["deployable"] is False
assert diag["field_contract"]["promotion_gate_passed"] is False
assert diag["pair_qty_sum_by_status_reason_projected_residual_bucket"][projected_key] == 2.0
assert diag["residual_qty_sum_by_status_reason_projected_residual_bucket"][projected_key] == 1.5
assert diag["residual_cost_sum_by_status_reason_projected_residual_bucket"][projected_key] == 0.6
assert diag["residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket"][side_key] == 1.5
assert diag["source_sequence_presence_by_status_reason_projected_residual_bucket"][projected_key]["present"] == 3.5

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--symmetric-activation-projected-residual-tail-join-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--symmetric-activation-projected-residual-tail-join-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_symmetric_activation_projected_residual_tail_join_summary_fixture",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_RESIDUAL_TAIL_JOIN_SUMMARY_FIXTURE_READY",
    "checks": {
        "default_off_summary_absent": True,
        "enabled_summary_present": True,
        "pair_actions_unchanged": True,
        "residual_qty_unchanged": True,
        "projected_pair_qty_present": True,
        "projected_residual_qty_present": True,
        "side_offset_projected_residual_present": True,
        "source_sequence_coverage_present": True,
        "cli_requires_event_lite": True,
    },
}
(out_dir / "fixture_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_symmetric_activation_projected_residual_tail_join_summary_scorer.py \
  --default-aggregate "$out_dir/default_off/aggregate_report.json" \
  --enabled-aggregate "$out_dir/enabled/aggregate_report.json" \
  --enabled-summary-dir "$out_dir/enabled" \
  --output-dir "$out_dir/scorer" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
scorer = json.loads((out_dir / "scorer/manifest.json").read_text())
assert (
    scorer["decision_label"]
    == "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_RESIDUAL_TAIL_JOIN_SUMMARY_SCORER_READY"
)
manifest = {
    "artifact": "xuan_symmetric_activation_projected_residual_tail_join_summary_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_PROJECTED_RESIDUAL_TAIL_JOIN_SUMMARY_SMOKE_PASS",
    "checks": {
        "runner_fixture_ready": True,
        "scorer_ready": True,
        "default_off_summary_absent": True,
        "enabled_summary_present": True,
        "aggregate_pair_parity": True,
        "aggregate_residual_parity": True,
        "post_action_outcome_labels_excluded": True,
        "realized_pair_cost_not_live_criterion": True,
        "trading_behavior_changed": False,
    },
    "scope": {
        "local_no_network_fixture": True,
        "new_data_fetched": False,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "raw_replay_full_store_scanned": False,
        "shared_ingress_or_live_modified": False,
        "orders_cancels_redeems_sent": False,
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
