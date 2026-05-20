#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_fill_to_balance_diagnostic_smoke_$ts}"
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
        id=100 + int(qty * 100),
        quote_intent_id=f"fixture-{side}-{qty}",
        side=side,
        qty=qty,
        px=0.40,
        fill_ms=ts_ms,
        source_order_id=1,
    )


def load_events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def run_fixture(out: Path, slug_start_s: int, cfg, opp_qty: float):
    slug = f"btc-updown-5m-{slug_start_s}"
    base_ts = slug_start_s * 1000
    late_ts = base_ts + 95_000
    runner = mod.DPlusRunner(slug, out, cfg)
    runner.on_book(book(base_ts + 1_000))
    runner.lots["NO"].append(lot("NO", opp_qty, base_ts + 1_000))
    runner.on_trade(sell(late_ts, "YES"))
    runner.write_summary(final=True)
    return runner, load_summary(runner.summary_path), load_events(runner.events_path)


base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    late_repair_only_after_s=90.0,
    late_repair_fill_to_balance_after_s=90.0,
    event_lite_summary=True,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=10.0,
    salvage_age_ms=10_000_000,
)

default_out = out_dir / "diagnostic_default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner, default_summary, default_events = run_fixture(default_out, 1_900_000_000, base_cfg, opp_qty=3.0)
default_aggregate = mod.aggregate(default_out)
default_candidates = [event for event in default_events if event.get("kind") == "candidate"]
assert default_summary["metrics"]["late_repair_fill_to_balance_caps"] == 1
assert default_candidates[0]["qty"] == 3.0
assert "late_repair_fill_to_balance_diagnostics" not in default_summary["event_lite"]
assert "late_repair_fill_to_balance_diagnostics" not in default_aggregate["event_lite"]

diag_cfg = mod.replace(base_cfg, fill_to_balance_diagnostic_event_lite_summary=True)
diag_out = out_dir / "diagnostic_enabled"
diag_out.mkdir(parents=True, exist_ok=True)

cap_runner, cap_summary, cap_events = run_fixture(diag_out, 1_900_000_300, diag_cfg, opp_qty=3.0)
block_runner, block_summary, block_events = run_fixture(diag_out, 1_900_000_600, diag_cfg, opp_qty=0.5)
uncapped_runner, uncapped_summary, uncapped_events = run_fixture(diag_out, 1_900_000_900, diag_cfg, opp_qty=7.0)
diag_aggregate = mod.aggregate(diag_out)

cap_candidates = [event for event in cap_events if event.get("kind") == "candidate"]
block_candidates = [event for event in block_events if event.get("kind") == "candidate"]
uncapped_candidates = [event for event in uncapped_events if event.get("kind") == "candidate"]

assert cap_runner.metrics.candidates == 1
assert cap_candidates[0]["qty"] == 3.0
assert cap_candidates[0]["late_repair_fill_to_balance_qty_reduction"] == 2.0
assert block_runner.metrics.candidates == 0
assert not block_candidates
assert block_summary["blocked"]["late_repair_fill_to_balance_qty"] == 1
assert uncapped_runner.metrics.candidates == 1
assert uncapped_candidates[0]["qty"] == 5.0
assert uncapped_candidates[0]["late_repair_fill_to_balance_qty_reduction"] == 0.0

side_offset = "YES|offset_90_120"
cap_diag = cap_summary["event_lite"]["late_repair_fill_to_balance_diagnostics"]
block_diag = block_summary["event_lite"]["late_repair_fill_to_balance_diagnostics"]
uncapped_diag = uncapped_summary["event_lite"]["late_repair_fill_to_balance_diagnostics"]
agg_diag = diag_aggregate["event_lite"]["late_repair_fill_to_balance_diagnostics"]

assert cap_diag["status_count_by_side_offset"][side_offset]["cap"] == 1
assert cap_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_2_5"] == 1
assert cap_diag["base_seed_qty_bucket_by_side_offset"][side_offset]["base_seed_qty_eq_5"] == 1
assert cap_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_2_5"] == 1
assert cap_diag["qty_reduction_bucket_by_side_offset"][side_offset]["qty_reduction_1_2"] == 1
assert cap_diag["qty_reduction_amount_by_side_offset"][side_offset] == 2.0

assert block_diag["status_count_by_side_offset"][side_offset]["block"] == 1
assert block_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_le_1"] == 1
assert block_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_le_1"] == 1
assert block_diag["qty_reduction_bucket_by_side_offset"][side_offset]["qty_reduction_2_5"] == 1
assert block_diag["qty_reduction_amount_by_side_offset"][side_offset] == 4.5

assert uncapped_diag["status_count_by_side_offset"][side_offset]["uncapped"] == 1
assert uncapped_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_gt_5"] == 1
assert uncapped_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_eq_5"] == 1
assert uncapped_diag["qty_reduction_bucket_by_side_offset"][side_offset]["qty_reduction_zero"] == 1

assert agg_diag["candidate_count_by_side"]["YES"] == 3
assert agg_diag["candidate_count_by_offset_bucket"]["offset_90_120"] == 3
assert agg_diag["candidate_count_by_side_offset"][side_offset] == 3
assert agg_diag["status_count_by_side_offset"][side_offset]["cap"] == 1
assert agg_diag["status_count_by_side_offset"][side_offset]["block"] == 1
assert agg_diag["status_count_by_side_offset"][side_offset]["uncapped"] == 1
assert agg_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_le_1"] == 1
assert agg_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_2_5"] == 1
assert agg_diag["deficit_bucket_by_side_offset"][side_offset]["deficit_gt_5"] == 1
assert agg_diag["base_seed_qty_bucket_by_side_offset"][side_offset]["base_seed_qty_eq_5"] == 3
assert agg_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_le_1"] == 1
assert agg_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_2_5"] == 1
assert agg_diag["capped_qty_bucket_by_side_offset"][side_offset]["capped_qty_eq_5"] == 1
assert agg_diag["qty_reduction_amount_by_side_offset"][side_offset] == 6.5
assert diag_aggregate["metrics"]["late_repair_fill_to_balance_candidates"] == 3
assert diag_aggregate["metrics"]["late_repair_fill_to_balance_caps"] == 2
assert diag_aggregate["metrics"]["late_repair_fill_to_balance_blocks"] == 1
assert diag_aggregate["metrics"]["late_repair_fill_to_balance_qty_reduction"] == 6.5

invalid_no_event_lite = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_no_event_lite"),
        "--late-repair-only-after-s",
        "90",
        "--late-repair-fill-to-balance-after-s",
        "90",
        "--fill-to-balance-diagnostic-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_no_event_lite.returncode != 0
assert "--fill-to-balance-diagnostic-event-lite-summary requires --event-lite-summary" in (
    invalid_no_event_lite.stdout + invalid_no_event_lite.stderr
)

invalid_no_fill_to_balance = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_no_fill_to_balance"),
        "--event-lite-summary",
        "--fill-to-balance-diagnostic-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_no_fill_to_balance.returncode != 0
assert "--fill-to-balance-diagnostic-event-lite-summary requires --late-repair-fill-to-balance-after-s" in (
    invalid_no_fill_to_balance.stdout + invalid_no_fill_to_balance.stderr
)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_fill_to_balance_diagnostic_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off fill-to-balance diagnostic event-lite buckets explain cap/block/uncapped repair paths without pulling events JSONL",
    "checks": {
        "default_event_lite_diagnostics_absent": True,
        "tiny_deficit_cap_counted": True,
        "dust_deficit_block_counted": True,
        "larger_repair_seed_uncapped_counted": True,
        "deficit_buckets_by_side_offset_exposed": True,
        "base_seed_qty_buckets_by_side_offset_exposed": True,
        "capped_qty_buckets_by_side_offset_exposed": True,
        "qty_reduction_buckets_and_amounts_by_side_offset_exposed": True,
        "aggregate_matches_summary_totals": True,
        "cli_requires_event_lite": True,
        "cli_requires_fill_to_balance": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(default_out / "aggregate_report.json"),
        "cap_summary": str(cap_runner.summary_path),
        "block_summary": str(block_runner.summary_path),
        "uncapped_summary": str(uncapped_runner.summary_path),
        "diagnostic_aggregate_report": str(diag_out / "aggregate_report.json"),
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
    "next_action": "After regression smokes pass, use the diagnostic flag only in a bounded no-order shadow to explain the fill-to-balance transfer gap; do not treat diagnostics as promotion evidence.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner fill-to-balance diagnostic smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
