#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_multi_profile_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile tools/xuan_dplus_passive_passive_shadow_runner.py >> "$log" 2>&1

python3 - "$ROOT" "$out_dir" <<'PY' >> "$log" 2>&1
import importlib.util
import json
import sys
from dataclasses import replace
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


def summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def feed(runner, events: list[dict[str, object]]) -> None:
    for event in events:
        mod.handle_market_message(runner, event)
    runner.write_summary(final=True)


slug = "btc-updown-5m-1900000000"
base_ts = 1_900_000_000_000
events = [
    book(base_ts + 1_000),
    sell(base_ts + 50_000, "YES", 0.40),
    sell(base_ts + 55_000, "YES", 0.30),
    sell(base_ts + 65_000, "NO", 0.40),
    sell(base_ts + 70_000, "NO", 0.30),
]
base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    activation_window_s=15.0,
    seed_offset_max_s=70.0,
    late_repair_after_s=60.0,
    cooldown_ms=5_000,
    target_qty=5.0,
    imbalance_qty_cap=1.25,
    pairing_only_when_residual=True,
)

single_out = out_dir / "single_direct"
single_out.mkdir(parents=True, exist_ok=True)
single = mod.DPlusRunner(slug, single_out, base_cfg)
feed(single, events)
single_summary = summary(single.summary_path)

single_profile_out = out_dir / "single_profile" / "repair60"
single_profile_out.mkdir(parents=True, exist_ok=True)
single_profile = mod.DPlusRunner(slug, single_profile_out, replace(base_cfg, late_repair_after_s=60.0))
feed(single_profile, events)
single_profile_summary = summary(single_profile.summary_path)

selected_metric_keys = [
    "candidates",
    "queue_supported_fills",
    "pair_actions",
    "pair_qty",
    "pair_pnl",
    "residual_qty",
    "residual_cost",
    "net_pair_cost_p90",
]
for key in selected_metric_keys:
    assert single_summary["metrics"][key] == single_profile_summary["metrics"][key], key
assert single_summary["blocked"] == single_profile_summary["blocked"]

profile_cfgs = {
    "repair45": replace(base_cfg, late_repair_after_s=45.0),
    "repair60": replace(base_cfg, late_repair_after_s=60.0),
    "repair75": replace(base_cfg, late_repair_after_s=75.0),
}
multi_out = out_dir / "multi_profile"
multi_out.mkdir(parents=True, exist_ok=True)
runners = {}
for profile, cfg in profile_cfgs.items():
    profile_out = multi_out / profile
    profile_out.mkdir(parents=True, exist_ok=True)
    runners[profile] = mod.DPlusRunner(slug, profile_out, cfg)

for event in events:
    for runner in runners.values():
        mod.handle_market_message(runner, event)
for runner in runners.values():
    runner.write_summary(final=True)

report = mod.aggregate_profiles(multi_out, profile_cfgs)
profiles = report["profiles"]
for name in ("repair45", "repair60", "repair75"):
    assert name in profiles
    assert (multi_out / name / "aggregate_report.json").exists()
    assert (multi_out / name / f"{slug}.summary.json").exists()

m45 = profiles["repair45"]["aggregate_report"]["metrics"]
m60 = profiles["repair60"]["aggregate_report"]["metrics"]
m75 = profiles["repair75"]["aggregate_report"]["metrics"]
b45 = profiles["repair45"]["aggregate_report"]["blocked"]
b60 = profiles["repair60"]["aggregate_report"]["blocked"]
assert m45["candidates"] < m60["candidates"]
assert m60["candidates"] == m75["candidates"]
assert m60["pair_actions"] == m75["pair_actions"] == 1
assert b45.get("late_repair_only", 0) > b60.get("late_repair_only", 0)
assert report["profile_axis"] == "late_repair_after_s"

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_multi_profile_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "same-window multi-profile runner can compare repair_after_s profiles over identical fixture events without changing default single-profile metrics",
    "checks": {
        "default_single_profile_metrics_preserved": True,
        "multi_profile_outputs_separate_aggregates": True,
        "profiles_share_identical_fixture_stream": True,
        "profile_state_is_independent": True,
        "repair45_differs_from_repair60_on_late_repair_gate": True,
    },
    "outputs": {
        "single_direct_summary": str(single.summary_path),
        "single_profile_summary": str(single_profile.summary_path),
        "multi_profile_report": str(multi_out / "multi_profile_aggregate_report.json"),
        "multi_profile_dir": str(multi_out),
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
    "next_action": "Run one bounded EC2 same-host no-order multi-profile shadow A/B only after preflight confirms no overlapping runner.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner multi-profile smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
