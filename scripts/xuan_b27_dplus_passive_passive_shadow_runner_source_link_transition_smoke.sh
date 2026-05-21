#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_source_link_transition_smoke_$ts}"
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


def sell(ts_ms: int, side: str, price: float, size: float = 20.0) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "source_sequence_id": f"seq-{side}-{ts_ms}",
        "market_side": side,
        "taker_side": "SELL",
        "price": price,
        "size": size,
    }


def load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def feed(runner, events: list[dict[str, object]]) -> None:
    for event in events:
        mod.handle_market_message(runner, event)
    runner.write_summary(final=True)


slug = "btc-updown-5m-1900000000"
base_ts = 1_900_000_000_000
events = [
    book(base_ts + 1_000),
    sell(base_ts + 20_000, "YES", 0.40),
    sell(base_ts + 21_000, "YES", 0.39),
    sell(base_ts + 30_000, "NO", 0.40),
    sell(base_ts + 121_000, "YES", 0.30),
    sell(base_ts + 122_000, "NO", 0.30),
]

base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=5.0,
    salvage_net_cap=0.0,
)

default_out = out_dir / "source_link_default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(slug, default_out, mod.replace(base_cfg, event_lite_summary=True))
feed(default_runner, events)
default_summary = load_summary(default_runner.summary_path)
default_report = mod.aggregate(default_out)
assert default_runner.metrics.pair_qty == 5.0
assert "source_link_transition_diagnostics" not in default_summary["event_lite"]
assert "source_link_transition_diagnostics" not in default_report["event_lite"]

enabled_out = out_dir / "source_link_enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
enabled_runner = mod.DPlusRunner(
    slug,
    enabled_out,
    mod.replace(base_cfg, event_lite_summary=True, source_link_transition_event_lite_summary=True),
)
feed(enabled_runner, events)
enabled_summary = load_summary(enabled_runner.summary_path)
enabled_report = mod.aggregate(enabled_out)
diag = enabled_summary["event_lite"]["source_link_transition_diagnostics"]
agg_diag = enabled_report["event_lite"]["source_link_transition_diagnostics"]

assert enabled_runner.metrics.candidates == 2
assert enabled_runner.metrics.queue_supported_fills == 2
assert enabled_runner.metrics.pair_qty == 5.0
assert enabled_summary["blocked"]["target"] == 1
assert enabled_summary["blocked"]["offset"] == 2
assert diag["transition_count_by_status"]["admitted"] == 2
assert diag["transition_count_by_status"]["blocked"] == 3
assert diag["transition_count_by_reason"]["candidate"] == 2
assert diag["transition_count_by_reason"]["target"] == 1
assert diag["transition_count_by_reason"]["offset"] == 2
assert diag["transition_count_by_status_reason"]["admitted"]["candidate"] == 2
assert diag["transition_count_by_status_reason"]["blocked"]["target"] == 1
assert diag["transition_count_by_status_reason"]["blocked"]["offset"] == 2
assert diag["quote_intent_presence_by_status"]["admitted"]["present"] == 2
assert diag["quote_intent_presence_by_status"]["blocked"]["missing"] == 3
assert diag["source_order_presence_by_status"]["admitted"]["present"] == 2
assert diag["source_sequence_presence_by_status"]["admitted"]["present"] == 2
assert diag["transition_count_by_status_side_offset_risk_direction"]["admitted"]["YES|offset_0_30|risk_increasing"] == 1
assert diag["transition_count_by_status_side_offset_risk_direction"]["admitted"]["NO|offset_30_60|repair_or_pairing_improving"] == 1
assert diag["transition_count_by_status_side_offset_risk_direction"]["blocked"]["YES|offset_0_30|risk_increasing"] == 1
assert diag["pre_seed_same_qty_bucket_by_status_reason"]["blocked|target"]["same_qty_eq_5"] == 1
assert diag["candidate_qty_bucket_by_status_reason"]["admitted|candidate"]["candidate_qty_eq_5"] == 2
assert diag["pre_seed_same_qty_bucket_by_status_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"]["same_qty_zero"] == 1
assert diag["pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction"]["NO|offset_30_60|repair_or_pairing_improving"]["opp_qty_eq_5"] == 1
assert diag["candidate_qty_bucket_by_status_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"]["candidate_qty_eq_5"] == 1
assert diag["candidate_qty_bucket_by_status_side_offset_risk_direction"]["NO|offset_30_60|repair_or_pairing_improving"]["candidate_qty_eq_5"] == 1
assert diag["ledger_proxy_before_bucket_by_status_reason"]["admitted|candidate"]
assert diag["ledger_proxy_after_bucket_by_status_reason"]["admitted|candidate"]
assert diag["ledger_proxy_before_bucket_by_status_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"]
assert diag["ledger_proxy_after_bucket_by_status_side_offset_risk_direction"]["NO|offset_30_60|repair_or_pairing_improving"]
assert diag["transition_count_by_risk_direction"]["blocked"]["risk_increasing"] == 3
assert diag["immediate_pair_action_count_by_source_side_offset"]["YES|offset_0_30"] == 1
assert diag["immediate_pair_action_count_by_source_side_offset"]["NO|offset_30_60"] == 1
assert diag["immediate_pair_action_count_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"] == 1
assert diag["immediate_pair_action_count_by_source_side_offset_risk_direction"]["NO|offset_30_60|repair_or_pairing_improving"] == 1
assert diag["immediate_pair_qty_bucket_by_source_side_offset"]["YES|offset_0_30"]["pair_qty_eq_5"] == 1
assert diag["immediate_pair_qty_bucket_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"]["pair_qty_eq_5"] == 1
assert diag["immediate_pair_cost_bucket_by_source_side_offset"]["YES|offset_0_30"]["pair_cost_lt_0p95"] == 1
assert diag["immediate_pair_cost_bucket_by_source_side_offset_risk_direction"]["NO|offset_30_60|repair_or_pairing_improving"]["pair_cost_lt_0p95"] == 1
assert diag["immediate_pair_source_quote_presence_by_side_offset"]["YES|offset_0_30"]["present"] == 1
assert diag["immediate_pair_source_order_presence_by_side_offset"]["NO|offset_30_60"]["present"] == 1
assert diag["residual_qty_by_source_side_offset"] == {}
assert diag["residual_qty_by_source_side_offset_risk_direction"] == {}
assert agg_diag["transition_count_by_status"] == diag["transition_count_by_status"]
assert agg_diag["transition_count_by_reason"] == diag["transition_count_by_reason"]
assert agg_diag["immediate_pair_action_count_by_source_side_offset"] == diag["immediate_pair_action_count_by_source_side_offset"]
assert agg_diag["transition_count_by_status_side_offset_risk_direction"] == diag["transition_count_by_status_side_offset_risk_direction"]
assert agg_diag["immediate_pair_action_count_by_source_side_offset_risk_direction"] == diag["immediate_pair_action_count_by_source_side_offset_risk_direction"]

residual_out = out_dir / "source_link_residual_enabled"
residual_out.mkdir(parents=True, exist_ok=True)
residual_runner = mod.DPlusRunner(
    slug,
    residual_out,
    mod.replace(base_cfg, event_lite_summary=True, source_link_transition_event_lite_summary=True),
)
feed(residual_runner, [book(base_ts + 1_000), sell(base_ts + 20_000, "YES", 0.40), sell(base_ts + 121_000, "YES", 0.30)])
residual_summary = load_summary(residual_runner.summary_path)
residual_diag = residual_summary["event_lite"]["source_link_transition_diagnostics"]
assert residual_summary["metrics"]["residual_qty"] == 5.0
assert residual_diag["residual_qty_by_source_side_offset"]["YES|offset_0_30"] == 5.0
assert residual_diag["residual_cost_by_source_side_offset"]["YES|offset_0_30"] == 1.65
assert residual_diag["residual_count_by_source_side_offset"]["YES|offset_0_30"] == 1
assert residual_diag["residual_qty_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"] == 5.0
assert residual_diag["residual_cost_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"] == 1.65
assert residual_diag["residual_count_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"] == 1
assert residual_diag["residual_cost_bucket_by_source_side_offset"]["YES|offset_0_30"]["residual_cost_1_2"] == 1
assert residual_diag["residual_cost_bucket_by_source_side_offset_risk_direction"]["YES|offset_0_30|risk_increasing"]["residual_cost_1_2"] == 1
assert residual_diag["residual_source_quote_presence_by_side_offset"]["YES|offset_0_30"]["present"] == 1
assert residual_diag["residual_source_order_presence_by_side_offset"]["YES|offset_0_30"]["present"] == 1

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "source_link_without_event_lite"),
        "--source-link-transition-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--source-link-transition-event-lite-summary requires --event-lite-summary" in (invalid.stdout + invalid.stderr)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_source_link_transition_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off source-link transition event-lite diagnostics expose admitted/blocked candidate transitions, immediate pair linkage, and residual source linkage without changing default behavior",
    "checks": {
        "default_event_lite_source_link_absent": True,
        "cli_requires_event_lite": True,
        "admitted_and_blocked_transition_counts": True,
        "quote_and_order_presence_counts": True,
        "pre_seed_inventory_buckets": True,
        "ledger_proxy_before_after_buckets": True,
        "cross_bucket_transition_counts": True,
        "cross_bucket_pre_seed_inventory_buckets": True,
        "cross_bucket_pair_source_buckets": True,
        "cross_bucket_residual_source_buckets": True,
        "immediate_pair_source_link_buckets": True,
        "residual_source_link_buckets": True,
        "aggregate_matches_per_summary": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(default_out / "aggregate_report.json"),
        "enabled_summary": str(enabled_runner.summary_path),
        "enabled_aggregate_report": str(enabled_out / "aggregate_report.json"),
        "residual_summary": str(residual_runner.summary_path),
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "service_control_used": False,
    },
    "next_action": "Run regression smokes, then keep this diagnostic default-off until a local verifier needs transfer-gap source linkage; do not treat diagnostics as promotion evidence.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner source-link transition smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
