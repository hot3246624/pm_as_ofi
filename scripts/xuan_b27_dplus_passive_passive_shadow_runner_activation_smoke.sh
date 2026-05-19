#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_activation_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile tools/xuan_dplus_passive_passive_shadow_runner.py >> "$log" 2>&1

python3 - "$ROOT" "$out_dir" <<'PY' >> "$log" 2>&1
import importlib.util
import json
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


def sell(ts_ms: int, side: str) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "market_side": side,
        "taker_side": "SELL",
        "price": 0.40,
        "size": 20.0,
    }


def load_events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


base_ts = 1_900_000_000_000

default_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000000",
    out_dir,
    mod.RunnerConfig(edge=0.07, activation_mode="none"),
)
default_runner.on_book(book(base_ts + 1_000))
default_runner.on_trade(sell(base_ts + 10_000, "YES"))
assert default_runner.metrics.candidates == 1
assert default_runner.blocked.get("activation_opp_seen", 0) == 0

activation_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000300",
    out_dir,
    mod.RunnerConfig(edge=0.07, activation_mode="opp_seen", activation_window_s=15.0),
)
activation_runner.on_book(book(base_ts + 301_000))
activation_runner.on_trade(sell(base_ts + 310_000, "YES"))
assert activation_runner.metrics.candidates == 0
assert activation_runner.blocked.get("activation_opp_seen") == 1
activation_runner.on_trade(sell(base_ts + 320_000, "NO"))
assert activation_runner.metrics.candidates == 1
events = load_events(activation_runner.events_path)
activation_blocks = [event for event in events if event.get("kind") == "activation_block"]
candidates = [event for event in events if event.get("kind") == "candidate"]
assert len(activation_blocks) == 1
assert len(candidates) == 1
assert candidates[0]["activation_required"] is True
assert candidates[0]["risk_increasing_seed"] is True
assert candidates[0]["activation_opp_age_ms"] == 10000

repair_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000600",
    out_dir,
    mod.RunnerConfig(edge=0.07, activation_mode="opp_seen", activation_window_s=15.0),
)
repair_runner.on_book(book(base_ts + 601_000))
repair_runner.lots["NO"].append(
    mod.Lot(id=1, side="NO", qty=3.0, px=0.40, fill_ms=base_ts + 601_000, source_order_id=1)
)
repair_runner.on_trade(sell(base_ts + 610_000, "YES"))
assert repair_runner.metrics.candidates == 1
repair_events = load_events(repair_runner.events_path)
repair_candidates = [event for event in repair_events if event.get("kind") == "candidate"]
assert repair_candidates[0]["activation_required"] is False
assert repair_candidates[0]["risk_increasing_seed"] is False

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_activation_smoke",
    "status": "PASS",
    "tool": str(tool_path),
    "hypothesis": "default-off opposite-side activation gate blocks only risk-increasing no-order shadow seeds",
    "checks": {
        "default_none_keeps_seed_path": True,
        "opp_seen_blocks_first_risk_increasing_seed": True,
        "opp_seen_allows_recent_opposite_side_seed": True,
        "risk_reducing_seed_bypasses_activation": True,
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
    "outputs": {
        "default_events": str(default_runner.events_path),
        "activation_events": str(activation_runner.events_path),
        "repair_events": str(repair_runner.events_path),
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner activation smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
