#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_source_opportunity_marker_smoke_$ts}"
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


def book(ts_ms: int) -> dict[str, object]:
    return {
        "kind": "market_book_tick",
        "ts_ms": ts_ms,
        "yes_bid": 0.30,
        "yes_ask": 0.50,
        "no_bid": 0.30,
        "no_ask": 0.50,
    }


def sell(ts_ms: int, side: str, price: float = 0.40, size: float = 20.0) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "source_sequence_id": f"seq-{side}-{ts_ms}",
        "market_side": side,
        "taker_side": "SELL",
        "price": price,
        "size": size,
    }


def lot(side: str, qty: float, ts_ms: int) -> object:
    return mod.Lot(
        id=700 + int(qty * 100),
        quote_intent_id=f"fixture-{side}-{qty}",
        side=side,
        qty=qty,
        px=0.40,
        fill_ms=ts_ms,
        source_order_id=1,
    )


def load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def run_fixture(name: str, cfg):
    slug = "btc-updown-5m-1900000000"
    base_ts = 1_900_000_000_000
    late_ts = base_ts + 95_000
    fixture_out = out_dir / name
    fixture_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner(slug, fixture_out, cfg)
    runner.on_book(book(base_ts + 1_000))
    runner.lots["NO"].append(lot("NO", 0.20, base_ts + 1_000))
    runner.on_trade(sell(late_ts - 4_000, "NO", price=0.40, size=20.0))
    runner.on_trade(sell(late_ts, "YES", price=0.40, size=20.0))
    runner.on_trade(sell(late_ts + 1_000, "YES", price=0.32, size=5.0))
    runner.write_summary(final=True)
    return runner, load_summary(runner.summary_path), mod.aggregate(fixture_out)


base_cfg = mod.RunnerConfig(
    edge=0.07,
    queue_share=1.0,
    activation_mode="opp_seen",
    activation_window_s=15.0,
    late_repair_only_after_s=90.0,
    event_lite_summary=True,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=10.0,
    salvage_age_ms=10_000_000,
    salvage_net_cap=0.0,
    cooldown_ms=0,
)

default_runner, default_summary, default_aggregate = run_fixture("default_off", base_cfg)
assert default_summary["metrics"]["candidates"] == 1
assert "source_opportunity_marker_summary" not in default_summary["event_lite"]
assert "source_opportunity_marker_summary" not in default_aggregate["event_lite"]

enabled_cfg = mod.replace(base_cfg, source_opportunity_marker_event_lite_summary=True)
enabled_runner, enabled_summary, enabled_aggregate = run_fixture("enabled", enabled_cfg)
diag = enabled_summary["event_lite"]["source_opportunity_marker_summary"]
agg_diag = enabled_aggregate["event_lite"]["source_opportunity_marker_summary"]
micro_key = "YES|offset_90_120|repair_or_pairing_improving|open_qty_le_1|deficit_0_0_25"

assert enabled_summary["config"]["source_opportunity_marker_event_lite_summary"] is True
assert enabled_summary["metrics"]["candidates"] == 1
assert enabled_summary["metrics"]["micro_deficit_repair_guard_candidates"] == 1
assert enabled_summary["metrics"]["micro_deficit_repair_guard_blocks"] == 0
assert enabled_summary["blocked"]["late_repair_only"] == 2
assert diag["schema_version"] == "source_opportunity_marker_summary_v1"
assert diag["field_contract"]["post_action_outcome_labels_included"] is False
assert diag["field_contract"]["private_truth_ready"] is False
assert diag["transition_count_by_status"]["admitted"] == 1
assert diag["transition_count_by_status"]["blocked"] == 2
assert diag["transition_count_by_status_reason"]["admitted"]["candidate"] == 1
assert diag["transition_count_by_status_reason"]["blocked"]["late_repair_only"] == 2
assert diag["transition_count_by_status_side_offset_risk_open_deficit"]["admitted"][micro_key] == 1
assert diag["micro_deficit_marker_count_by_status"]["admitted"] == 1
assert diag["micro_deficit_marker_count_by_status_reason"]["admitted"]["candidate"] == 1
assert diag["candidate_qty_sum_by_status_reason"]["admitted|candidate"] == 5.0
assert diag["base_qty_sum_by_status_reason"]["admitted|candidate"] == 5.0
assert diag["candidate_qty_bucket_by_status_reason"]["admitted|candidate"]["candidate_qty_eq_5"] == 1
assert diag["base_qty_bucket_by_status_reason"]["admitted|candidate"]["base_qty_eq_5"] == 1
assert diag["target_room_bucket_by_status_reason"]["admitted|candidate"]["target_room_eq_5"] == 1
assert diag["imbalance_room_bucket_by_status_reason"]["admitted|candidate"]["imbalance_room_gt_5"] == 1
assert diag["pending_same_order_count_bucket_by_status_reason"]["admitted|candidate"]["pending_same_order_count_zero"] == 1
assert diag["pending_opp_qty_bucket_by_status_reason"]["admitted|candidate"]["pending_opp_qty_zero"] == 1
assert diag["opposite_seen_by_status_reason"]["admitted|candidate"]["opposite_seen_present"] == 1
assert diag["activation_opp_age_bucket_by_status_reason"]["admitted|candidate"]["activation_opp_age_1_5s"] == 1
assert diag["quote_intent_presence_by_status_reason"]["admitted|candidate"]["present"] == 1
assert diag["source_order_presence_by_status_reason"]["admitted|candidate"]["present"] == 1
assert diag["source_sequence_presence_by_status_reason"]["admitted|candidate"]["present"] == 1
assert diag["quote_intent_presence_by_status_reason"]["blocked|late_repair_only"]["missing"] == 2
assert diag["source_order_presence_by_status_reason"]["blocked|late_repair_only"]["missing"] == 2
assert diag["source_sequence_presence_by_status_reason"]["blocked|late_repair_only"]["present"] == 2
assert agg_diag["transition_count_by_status"] == diag["transition_count_by_status"]
assert agg_diag["transition_count_by_status_reason"] == diag["transition_count_by_status_reason"]
assert agg_diag["transition_count_by_status_side_offset_risk_open_deficit"] == diag["transition_count_by_status_side_offset_risk_open_deficit"]
assert agg_diag["micro_deficit_marker_count_by_status"] == diag["micro_deficit_marker_count_by_status"]

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--source-opportunity-marker-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--source-opportunity-marker-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_source_opportunity_marker_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_SOURCE_OPPORTUNITY_MARKER_SUMMARY_SMOKE_PASS",
    "scope": {
        "local_no_network_fixture": True,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "orders_cancels_redeems_sent": False,
    },
    "checks": {
        "default_off_summary_absent": True,
        "enabled_summary_present": True,
        "admitted_blocked_denominators_present": True,
        "exact_open_deficit_micro_marker_present": True,
        "candidate_and_base_qty_present": True,
        "room_and_pending_buckets_present": True,
        "activation_opposite_seen_present": True,
        "blocked_source_sequence_coverage_present": True,
        "aggregate_merges_marker_summary": True,
        "cli_requires_event_lite": True,
        "post_action_outcome_labels_excluded": True,
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
