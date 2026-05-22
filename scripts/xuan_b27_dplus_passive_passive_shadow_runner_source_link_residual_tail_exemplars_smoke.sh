#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_source_link_residual_tail_exemplars_smoke_$ts}"
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


def near(actual: float, expected: float, eps: float = 1e-9) -> bool:
    return abs(float(actual) - expected) <= eps


slug = "btc-updown-5m-1900000000"
base_ts = 1_900_000_000_000
events = [
    book(base_ts + 1_000),
    sell(base_ts + 20_000, "YES", 0.40, 20.0),
    sell(base_ts + 121_000, "YES", 0.30, 20.0),
    sell(base_ts + 122_000, "NO", 0.40, 8.0),
    sell(base_ts + 123_000, "NO", 0.30, 8.0),
]

base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=5.0,
    seed_offset_max_s=180.0,
    salvage_net_cap=0.0,
)

default_out = out_dir / "residual_tail_default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(
    slug,
    default_out,
    mod.replace(base_cfg, event_lite_summary=True, source_link_transition_event_lite_summary=True),
)
feed(default_runner, events)
default_summary = load_summary(default_runner.summary_path)
default_report = mod.aggregate(default_out)
assert default_summary["metrics"]["residual_qty"] == 3.0
assert "source_link_residual_tail_exemplars" not in default_summary["event_lite"]
assert "source_link_residual_tail_exemplars" not in default_report["event_lite"]

enabled_out = out_dir / "residual_tail_enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
enabled_runner = mod.DPlusRunner(
    slug,
    enabled_out,
    mod.replace(
        base_cfg,
        event_lite_summary=True,
        source_link_transition_event_lite_summary=True,
        source_link_residual_tail_exemplars_event_lite_summary=True,
    ),
)
feed(enabled_runner, events)
enabled_summary = load_summary(enabled_runner.summary_path)
enabled_report = mod.aggregate(enabled_out)

assert enabled_summary["metrics"] == default_summary["metrics"]
exemplars = enabled_summary["event_lite"]["source_link_residual_tail_exemplars"]
aggregate_exemplars = enabled_report["event_lite"]["source_link_residual_tail_exemplars"]
assert len(exemplars) == 1
assert len(aggregate_exemplars) == 1
ex = exemplars[0]
required_fields = [
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_ms",
]
missing = [field for field in required_fields if field not in ex]
assert not missing, missing
assert ex["slug"] == slug
assert ex["condition_id"] == slug
assert ex["quote_intent_id"].startswith(f"{slug}:quote:")
assert ex["source_sequence_id"] == f"seq-YES-{base_ts + 20_000}"
assert ex["side"] == "YES"
assert near(ex["offset_s"], 20.0)
assert ex["source_risk_direction"] == "risk_increasing"
assert near(ex["trigger_px"], 0.40)
assert near(ex["trigger_size"], 20.0)
assert near(ex["pre_seed_same_qty"], 0.0)
assert near(ex["pre_seed_opp_qty"], 0.0)
assert near(ex["pre_seed_same_cost"], 0.0)
assert near(ex["pre_seed_opp_cost"], 0.0)
assert near(ex["ledger_proxy_before"], 0.0)
assert near(ex["ledger_proxy_after"], -1.7)
assert near(ex["source_pair_qty"], 2.0)
assert near(ex["source_pair_cost"], 1.32)
assert near(ex["source_pair_pnl"], 0.68)
assert near(ex["source_residual_qty"], 3.0)
assert near(ex["source_residual_cost"], 0.99)
assert ex["source_residual_age_ms"] >= 0
assert aggregate_exemplars[0]["quote_intent_id"] == ex["quote_intent_id"]
assert aggregate_exemplars[0]["source_residual_cost"] == ex["source_residual_cost"]

residual_diag = enabled_summary["event_lite"]["source_link_transition_diagnostics"]
assert residual_diag["residual_qty_by_source_side_offset"]["YES|offset_0_30"] == 3.0
assert residual_diag["residual_cost_by_source_side_offset"]["YES|offset_0_30"] == 0.99
assert residual_diag["immediate_pair_qty_bucket_by_source_side_offset"]["YES|offset_0_30"]["pair_qty_1_2"] == 1

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "residual_tail_without_event_lite"),
        "--source-link-residual-tail-exemplars-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--source-link-residual-tail-exemplars-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_source_link_residual_tail_exemplars_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off residual-tail exemplars expose action-level source, pre-seed, pair, and residual fields without changing default runner behavior",
    "checks": {
        "default_event_lite_residual_tail_exemplars_absent": True,
        "enabled_metrics_match_default": True,
        "summary_exemplar_has_required_fields": True,
        "summary_exemplar_links_quote_order_sequence": True,
        "summary_exemplar_has_pre_seed_inventory_and_ledger": True,
        "summary_exemplar_has_pair_contribution": True,
        "summary_exemplar_has_residual_contribution": True,
        "aggregate_merges_residual_tail_exemplars": True,
        "source_link_transition_residual_buckets_still_present": True,
        "cli_requires_event_lite": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(default_out / "aggregate_report.json"),
        "enabled_summary": str(enabled_runner.summary_path),
        "enabled_aggregate_report": str(enabled_out / "aggregate_report.json"),
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
        "events_jsonl_pulled": False,
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
        "status": "INSTRUMENTATION_SMOKE_ONLY_NOT_PROMOTION_EVIDENCE",
    },
    "next_action": "Run a local causal verifier over residual-tail exemplars once a bounded no-order shadow has produced summaries with this default-off flag; do not start EC2 shadow/canary from this smoke alone.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner residual-tail exemplars smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
