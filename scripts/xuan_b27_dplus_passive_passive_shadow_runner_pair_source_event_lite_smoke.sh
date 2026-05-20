#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_pair_source_event_lite_smoke_$ts}"
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
    sell(base_ts + 20_000, "YES", 0.58),
    sell(base_ts + 25_000, "YES", 0.50),
    sell(base_ts + 28_000, "NO", 0.58),
    sell(base_ts + 29_000, "NO", 0.50),
    sell(base_ts + 70_000, "YES", 0.40),
    sell(base_ts + 75_000, "YES", 0.30),
    sell(base_ts + 85_000, "NO", 0.40),
    sell(base_ts + 90_000, "NO", 0.30),
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

default_out = out_dir / "event_lite_default"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(slug, default_out, mod.replace(base_cfg, event_lite_summary=True))
feed(default_runner, events)
default_summary = load_summary(default_runner.summary_path)
default_report = mod.aggregate(default_out)
default_lite = default_summary["event_lite"]
assert default_runner.metrics.pair_actions == 2
assert "pair_cost_by_source_seed_px_bucket" not in default_lite
assert "pair_source_action_count" not in default_lite
assert "pair_cost_by_source_seed_px_bucket" not in default_report["event_lite"]

enabled_out = out_dir / "pair_source_enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
enabled_runner = mod.DPlusRunner(
    slug,
    enabled_out,
    mod.replace(base_cfg, event_lite_summary=True, pair_source_event_lite_summary=True),
)
feed(enabled_runner, events)
enabled_summary = load_summary(enabled_runner.summary_path)
enabled_report = mod.aggregate(enabled_out)
lite = enabled_summary["event_lite"]
aggregate_lite = enabled_report["event_lite"]

assert enabled_runner.metrics.pair_actions == 2
assert enabled_summary["metrics"]["pair_actions"] == 2
assert lite["pair_source_action_count"] == 2
assert lite["pair_source_record_count"] == 4
assert aggregate_lite["pair_source_action_count"] == 2
assert aggregate_lite["pair_source_record_count"] == 4
assert lite["pair_cost_by_source_seed_px_bucket"]["pair_cost_1p00_1p05"]["px_0p48_0p55"] == 2
assert lite["pair_cost_by_source_seed_px_bucket"]["pair_cost_lt_0p95"]["px_0p30_0p45"] == 2
assert lite["pair_cost_by_source_public_px_bucket"]["pair_cost_1p00_1p05"]["px_ge_0p55"] == 2
assert lite["pair_cost_by_source_offset_bucket"]["pair_cost_1p00_1p05"]["offset_0_30"] == 2
assert lite["pair_cost_by_source_side"]["pair_cost_1p00_1p05"]["YES"] == 1
assert lite["pair_cost_by_source_side"]["pair_cost_1p00_1p05"]["NO"] == 1
assert len(lite["top_high_cost_pair_sources"]) == 1
high = lite["top_high_cost_pair_sources"][0]
assert high["pair_cost_bucket"] == "pair_cost_1p00_1p05"
assert high["net_pair_cost"] == 1.02
assert {source["side"] for source in high["sources"]} == {"YES", "NO"}
assert all(source["quote_intent_id"].startswith(slug + ":quote:") for source in high["sources"])
assert aggregate_lite["top_high_cost_pair_sources"][0]["matched_pair_id"] == high["matched_pair_id"]

disabled_cfg_out = out_dir / "pair_source_without_event_lite"
disabled_cfg_out.mkdir(parents=True, exist_ok=True)
disabled_cfg_runner = mod.DPlusRunner(
    slug,
    disabled_cfg_out,
    mod.replace(base_cfg, event_lite_summary=False, pair_source_event_lite_summary=True),
)
feed(disabled_cfg_runner, events)
disabled_cfg_summary = load_summary(disabled_cfg_runner.summary_path)
assert "event_lite" not in disabled_cfg_summary

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_pair_source_event_lite_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off pair-source event-lite can attribute high pair-cost buckets to source seed/public/offset/side without changing default event-lite behavior",
    "checks": {
        "default_event_lite_pair_source_fields_absent": True,
        "enabled_pair_source_action_count_matches_pair_actions": True,
        "enabled_pair_source_record_count_matches_two_internal_pair_lots_per_pair": True,
        "high_cost_pair_source_seed_px_bucket_exposed": True,
        "high_cost_pair_source_public_px_bucket_exposed": True,
        "high_cost_pair_source_offset_bucket_exposed": True,
        "high_cost_pair_source_side_exposed": True,
        "top_high_cost_pair_context_has_quote_intent_ids": True,
        "aggregate_merges_pair_source_counts": True,
        "pair_source_without_event_lite_is_quiet": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(default_out / "aggregate_report.json"),
        "enabled_summary": str(enabled_runner.summary_path),
        "enabled_aggregate_report": str(enabled_out / "aggregate_report.json"),
        "disabled_cfg_summary": str(disabled_cfg_runner.summary_path),
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
    "next_action": "Run regression smokes, then queue one bounded no-order shadow with --event-lite-summary --pair-source-event-lite-summary only after local checks pass and EC2 preflight has no hard blocker.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner pair-source event-lite smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
