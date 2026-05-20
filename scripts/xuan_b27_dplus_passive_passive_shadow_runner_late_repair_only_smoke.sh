#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_late_repair_only_smoke_$ts}"
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


def load_events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


base_ts = 1_900_000_000_000
late_ts = base_ts + 95_000

default_out = out_dir / "default"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000000",
    default_out,
    mod.RunnerConfig(edge=0.07, activation_mode="none", event_lite_summary=True),
)
default_runner.on_book(book(base_ts + 1_000))
default_runner.on_trade(sell(late_ts, "YES"))
default_runner.write_summary(final=True)
default_summary = load_summary(default_runner.summary_path)
default_events = load_events(default_runner.events_path)
default_candidates = [event for event in default_events if event.get("kind") == "candidate"]
assert default_runner.metrics.candidates == 1
assert default_runner.blocked.get("late_repair_only", 0) == 0
assert default_summary["config"]["late_repair_only_after_s"] is None
assert default_candidates[0]["late_repair_only_active"] is False

risk_out = out_dir / "risk_block"
risk_out.mkdir(parents=True, exist_ok=True)
risk_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000000",
    risk_out,
    mod.RunnerConfig(edge=0.07, activation_mode="none", late_repair_only_after_s=90.0, event_lite_summary=True),
)
risk_runner.on_book(book(base_ts + 1_000))
risk_runner.on_trade(sell(late_ts, "YES"))
risk_runner.write_summary(final=True)
risk_summary = load_summary(risk_runner.summary_path)
risk_aggregate = mod.aggregate(risk_out)
assert risk_runner.metrics.candidates == 0
assert risk_runner.blocked.get("late_repair_only") == 1
assert risk_summary["blocked"]["late_repair_only"] == 1
assert risk_aggregate["blocked"]["late_repair_only"] == 1
assert risk_summary["event_lite"]["block_by_reason_side"]["late_repair_only"]["YES"] == 1
assert risk_aggregate["event_lite"]["block_by_reason_side"]["late_repair_only"]["YES"] == 1

repair_out = out_dir / "repair_allow"
repair_out.mkdir(parents=True, exist_ok=True)
repair_runner = mod.DPlusRunner(
    "btc-updown-5m-1900000000",
    repair_out,
    mod.RunnerConfig(
        edge=0.07,
        activation_mode="none",
        late_repair_only_after_s=90.0,
        event_lite_summary=True,
        salvage_age_ms=10_000_000,
    ),
)
repair_runner.on_book(book(base_ts + 1_000))
repair_runner.lots["NO"].append(
    mod.Lot(
        id=1,
        quote_intent_id="fixture-no-lot",
        side="NO",
        qty=3.0,
        px=0.40,
        fill_ms=base_ts + 1_000,
        source_order_id=1,
    )
)
repair_runner.on_trade(sell(late_ts, "YES"))
repair_runner.write_summary(final=True)
repair_summary = load_summary(repair_runner.summary_path)
repair_events = load_events(repair_runner.events_path)
repair_candidates = [event for event in repair_events if event.get("kind") == "candidate"]
assert repair_runner.metrics.candidates == 1
assert repair_runner.blocked.get("late_repair_only", 0) == 0
assert repair_candidates[0]["late_repair_only_active"] is True
assert repair_candidates[0]["same_exposure_qty"] == 0
assert repair_candidates[0]["opp_exposure_qty"] == 3.0
assert repair_candidates[0]["risk_increasing_seed"] is False
assert repair_summary["event_lite"]["candidate_side_counts"]["YES"] == 1

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_late_repair_only_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off late-offset repair-only gate blocks risk-increasing late seeds while allowing underweight repair seeds",
    "checks": {
        "default_path_preserves_late_seed": True,
        "late_repair_only_default_off": True,
        "late_risk_increasing_seed_blocked": True,
        "late_underweight_repair_seed_allowed": True,
        "candidate_event_marks_late_repair_only_active": True,
        "summary_exposes_late_repair_only_block_count": True,
        "aggregate_exposes_late_repair_only_block_count": True,
        "event_lite_exposes_late_repair_only_block_bucket": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "risk_summary": str(risk_runner.summary_path),
        "risk_aggregate_report": str(risk_out / "aggregate_report.json"),
        "repair_summary": str(repair_runner.summary_path),
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
    "next_action": "Queue one bounded same-host no-order shadow with --late-repair-only-after-s 90 only after xuan-research preflight confirms no overlapping xuan-research runner and shared-ingress metadata is healthy.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner late-repair-only smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
