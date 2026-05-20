#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_fill_to_balance_smoke_$ts}"
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


def run_fixture(name: str, cfg, opp_qty: float):
    slug = "btc-updown-5m-1900000000"
    base_ts = 1_900_000_000_000
    late_ts = base_ts + 95_000
    fixture_out = out_dir / name
    fixture_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner(slug, fixture_out, cfg)
    runner.on_book(book(base_ts + 1_000))
    runner.lots["NO"].append(lot("NO", opp_qty, base_ts + 1_000))
    runner.on_trade(sell(late_ts, "YES"))
    runner.write_summary(final=True)
    return runner, load_summary(runner.summary_path), load_events(runner.events_path), mod.aggregate(fixture_out)


base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    late_repair_only_after_s=90.0,
    event_lite_summary=True,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=10.0,
    salvage_age_ms=10_000_000,
)

default_runner, default_summary, default_events, default_aggregate = run_fixture(
    "default_late_repair_only",
    base_cfg,
    opp_qty=3.0,
)
default_candidates = [event for event in default_events if event.get("kind") == "candidate"]
assert default_runner.metrics.candidates == 1
assert default_summary["config"]["late_repair_fill_to_balance_after_s"] is None
assert default_candidates[0]["qty"] == 5.0
assert default_candidates[0]["late_repair_only_active"] is True
assert default_candidates[0]["late_repair_fill_to_balance_active"] is False
assert default_summary["metrics"]["late_repair_fill_to_balance_caps"] == 0
assert default_aggregate["metrics"]["late_repair_fill_to_balance_caps"] == 0

cap_cfg = mod.replace(base_cfg, late_repair_fill_to_balance_after_s=90.0)
cap_runner, cap_summary, cap_events, cap_aggregate = run_fixture(
    "fill_to_balance_cap",
    cap_cfg,
    opp_qty=3.0,
)
cap_candidates = [event for event in cap_events if event.get("kind") == "candidate"]
assert cap_runner.metrics.candidates == 1
assert cap_candidates[0]["qty"] == 3.0
assert cap_candidates[0]["base_seed_qty"] == 5.0
assert cap_candidates[0]["late_repair_fill_to_balance_active"] is True
assert cap_candidates[0]["late_repair_fill_to_balance_deficit"] == 3.0
assert cap_candidates[0]["late_repair_fill_to_balance_qty_reduction"] == 2.0
assert cap_summary["metrics"]["late_repair_fill_to_balance_candidates"] == 1
assert cap_summary["metrics"]["late_repair_fill_to_balance_caps"] == 1
assert cap_summary["metrics"]["late_repair_fill_to_balance_qty_reduction"] == 2.0
assert cap_aggregate["metrics"]["late_repair_fill_to_balance_caps"] == 1
assert cap_aggregate["metrics"]["late_repair_fill_to_balance_qty_reduction"] == 2.0

block_runner, block_summary, block_events, block_aggregate = run_fixture(
    "fill_to_balance_dust_block",
    cap_cfg,
    opp_qty=0.5,
)
block_candidates = [event for event in block_events if event.get("kind") == "candidate"]
assert block_runner.metrics.candidates == 0
assert not block_candidates
assert block_runner.blocked.get("late_repair_fill_to_balance_qty") == 1
assert block_summary["metrics"]["late_repair_fill_to_balance_blocks"] == 1
assert block_summary["blocked"]["late_repair_fill_to_balance_qty"] == 1
assert block_summary["event_lite"]["block_by_reason_side"]["late_repair_fill_to_balance_qty"]["YES"] == 1
assert block_aggregate["blocked"]["late_repair_fill_to_balance_qty"] == 1
assert block_aggregate["metrics"]["late_repair_fill_to_balance_blocks"] == 1
assert block_aggregate["event_lite"]["block_by_reason_side"]["late_repair_fill_to_balance_qty"]["YES"] == 1

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--late-repair-fill-to-balance-after-s",
        "90",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "requires --late-repair-after-s or --late-repair-only-after-s" in (invalid.stdout + invalid.stderr)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_fill_to_balance_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off late repair fill-to-balance caps late underweight repair seed qty without changing default late-repair-only behavior",
    "checks": {
        "default_path_preserves_late_repair_seed_qty": True,
        "default_config_is_off": True,
        "late_small_deficit_repair_seed_is_capped": True,
        "late_dust_deficit_repair_seed_is_blocked": True,
        "candidate_event_exposes_cap_context": True,
        "summary_exposes_cap_counts": True,
        "aggregate_exposes_cap_counts": True,
        "event_lite_exposes_block_bucket": True,
        "cli_requires_late_repair_gate": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "cap_summary": str(cap_runner.summary_path),
        "cap_aggregate_report": str(out_dir / "fill_to_balance_cap" / "aggregate_report.json"),
        "block_summary": str(block_runner.summary_path),
        "block_aggregate_report": str(out_dir / "fill_to_balance_dust_block" / "aggregate_report.json"),
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
    "next_action": "If accepted, run activation/multi-profile/pair-source regression smokes before queueing any EC2 no-order shadow.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner fill-to-balance smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
