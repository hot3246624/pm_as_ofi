#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_limit_order_min_shares_smoke_$ts}"
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


def sell(ts_ms: int, size: float) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "source_sequence_id": f"seq-YES-{ts_ms}-{size}",
        "market_side": "YES",
        "taker_side": "SELL",
        "price": 0.40,
        "size": size,
    }


def load_events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def run_fixture(name: str, cfg, trade_size: float):
    slug = "btc-updown-5m-1900000000"
    base_ts = 1_900_000_000_000
    fixture_out = out_dir / name
    fixture_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner(slug, fixture_out, cfg)
    runner.on_book(book(base_ts + 1_000))
    runner.on_trade(sell(base_ts + 20_000, trade_size))
    runner.write_summary(final=True)
    return runner, json.loads(runner.summary_path.read_text()), load_events(runner.events_path), mod.aggregate(fixture_out)


base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=10.0,
    event_lite_summary=True,
    source_link_transition_event_lite_summary=True,
    salvage_net_cap=0.0,
)

default_runner, default_summary, default_events, default_aggregate = run_fixture(
    "default_off_sub_min_allowed",
    base_cfg,
    trade_size=8.0,
)
default_candidates = [event for event in default_events if event.get("kind") == "candidate"]
assert default_runner.metrics.candidates == 1
assert default_candidates[0]["qty"] == 2.0
assert default_summary["config"]["limit_order_min_shares_guard"] is False
assert default_summary["blocked"].get("limit_order_min_shares") is None
assert default_aggregate["metrics"]["candidates"] == 1

guard_cfg = mod.replace(base_cfg, limit_order_min_shares_guard=True, limit_order_min_shares=5.0)
guard_runner, guard_summary, guard_events, guard_aggregate = run_fixture(
    "strict_min5_sub_min_blocked",
    guard_cfg,
    trade_size=8.0,
)
guard_candidates = [event for event in guard_events if event.get("kind") == "candidate"]
assert guard_runner.metrics.candidates == 0
assert not guard_candidates
assert guard_summary["config"]["limit_order_min_shares_guard"] is True
assert guard_summary["blocked"]["limit_order_min_shares"] == 1
assert guard_summary["event_lite"]["block_by_reason_side"]["limit_order_min_shares"]["YES"] == 1
assert guard_aggregate["blocked"]["limit_order_min_shares"] == 1

pass_runner, pass_summary, pass_events, pass_aggregate = run_fixture(
    "strict_min5_exact5_allowed",
    guard_cfg,
    trade_size=20.0,
)
pass_candidates = [event for event in pass_events if event.get("kind") == "candidate"]
assert pass_runner.metrics.candidates == 1
assert pass_candidates[0]["qty"] == 5.0
assert pass_summary["blocked"].get("limit_order_min_shares") is None
assert pass_aggregate["metrics"]["candidates"] == 1

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--limit-order-min-shares",
        "0.5",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "must exceed dust_qty" in (invalid.stdout + invalid.stderr)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_limit_order_min_shares_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off limit-order min-shares guard preserves existing behavior and blocks sub-min share limit intents only when enabled",
    "checks": {
        "default_off_sub_min_candidate_preserved": True,
        "strict_min5_sub_min_candidate_blocked": True,
        "strict_min5_exact5_candidate_allowed": True,
        "event_lite_block_reason_recorded": True,
        "aggregate_block_reason_recorded": True,
        "invalid_cli_min_shares_rejected": True,
    },
    "side_effects": {
        "network_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "sells_sent": False,
        "redeems_sent": False,
        "credential_read": False,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "PASS $out_dir"
