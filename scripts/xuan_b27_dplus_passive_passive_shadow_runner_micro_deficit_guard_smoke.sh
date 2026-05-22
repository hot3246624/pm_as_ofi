#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_micro_deficit_guard_smoke_$ts}"
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


def load_events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


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
    runner.on_trade(sell(late_ts, "YES", price=0.40, size=20.0))
    runner.on_trade(sell(late_ts + 1_000, "YES", price=0.32, size=5.0))
    runner.write_summary(final=True)
    return runner, load_summary(runner.summary_path), load_events(runner.events_path), mod.aggregate(fixture_out)


base_cfg = mod.RunnerConfig(
    edge=0.07,
    queue_share=1.0,
    activation_mode="none",
    late_repair_only_after_s=90.0,
    event_lite_summary=True,
    source_link_transition_event_lite_summary=True,
    source_link_residual_tail_exemplars_event_lite_summary=True,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=10.0,
    salvage_age_ms=10_000_000,
    salvage_net_cap=0.0,
)

default_runner, default_summary, default_events, default_aggregate = run_fixture("default_off", base_cfg)
default_candidates = [event for event in default_events if event.get("kind") == "candidate"]
assert default_summary["config"]["micro_deficit_repair_guard"] is False
assert default_summary["metrics"]["candidates"] == 1
assert default_summary["metrics"]["queue_supported_fills"] == 1
assert default_summary["metrics"]["micro_deficit_repair_guard_candidates"] == 1
assert default_summary["metrics"]["micro_deficit_repair_guard_blocks"] == 0
assert default_candidates[0]["micro_deficit_repair_guard_candidate"] is True
assert default_candidates[0]["micro_deficit_repair_deficit"] == 0.2
assert default_candidates[0]["risk_increasing_seed"] is True
assert default_aggregate["metrics"]["micro_deficit_repair_guard_candidates"] == 1
tail = default_summary["event_lite"]["source_link_residual_tail_exemplars"]
assert tail[0]["micro_deficit_repair_guard_candidate"] is True
assert tail[0]["micro_deficit_repair_deficit"] == 0.2
assert tail[0]["pre_seed_open_qty"] == 0.2
assert tail[0]["source_risk_direction"] == "repair_or_pairing_improving"

guard_cfg = mod.replace(base_cfg, micro_deficit_repair_guard=True)
guard_runner, guard_summary, guard_events, guard_aggregate = run_fixture("guard_enabled", guard_cfg)
guard_candidates = [event for event in guard_events if event.get("kind") == "candidate"]
assert guard_summary["config"]["micro_deficit_repair_guard"] is True
assert guard_summary["metrics"]["candidates"] == 0
assert guard_summary["metrics"]["micro_deficit_repair_guard_candidates"] == 2
assert guard_summary["metrics"]["micro_deficit_repair_guard_blocks"] == 2
assert guard_summary["blocked"]["micro_deficit_repair_guard"] == 2
assert not guard_candidates
assert guard_aggregate["metrics"]["micro_deficit_repair_guard_blocks"] == 2
assert guard_aggregate["blocked"]["micro_deficit_repair_guard"] == 2

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--micro-deficit-repair-guard",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--micro-deficit-repair-guard requires --late-repair-after-s or --late-repair-only-after-s" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_micro_deficit_guard_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_MICRO_DEFICIT_REPAIR_GUARD_RUNNER_SMOKE_PASS",
    "scope": {
        "local_no_network_fixture": True,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "orders_cancels_redeems_sent": False,
    },
    "checks": {
        "default_guard_off_preserves_candidate": True,
        "default_candidate_exposes_micro_deficit_marker": True,
        "default_exemplar_exposes_micro_deficit_marker": True,
        "guard_enabled_blocks_micro_deficit_seed": True,
        "guard_metrics_aggregate": True,
        "cli_requires_late_repair_gate": True,
        "source_risk_direction_repair_context_preserved": True,
    },
    "artifacts": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(out_dir / "default_off" / "aggregate_report.json"),
        "guard_summary": str(guard_runner.summary_path),
        "guard_aggregate_report": str(out_dir / "guard_enabled" / "aggregate_report.json"),
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
