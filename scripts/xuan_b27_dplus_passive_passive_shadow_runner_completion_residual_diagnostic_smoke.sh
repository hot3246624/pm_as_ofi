#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_completion_residual_diagnostic_smoke_$ts}"
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


def make_lot(mod, *, side: str, px: float, qty: float = 5.0, fill_age_ms: int = 120_000):
    fill_ms = mod.now_ms() - fill_age_ms
    return mod.Lot(
        id=1,
        quote_intent_id=f"quote-{side}-{px}",
        side=side,
        qty=qty,
        px=px,
        fill_ms=fill_ms,
        source_order_id=101,
        trigger_px=px + 0.02,
        trigger_size=10.0,
        offset_s=42.0,
        trigger_ts_ms=fill_ms,
        trigger_source_sequence_id=f"seq-{side}-{px}",
        source_risk_direction="risk_increasing",
    )


def run_case(name: str, cfg, *, side: str, px: float, yes_ask: float, no_ask: float):
    case_out = out_dir / name
    case_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner(f"btc-updown-5m-190000{name[-4:]}", case_out, cfg)
    runner.book = {
        "yes_bid": max(0.0, yes_ask - 0.02) if yes_ask else 0.0,
        "yes_ask": yes_ask,
        "no_bid": max(0.0, no_ask - 0.02) if no_ask else 0.0,
        "no_ask": no_ask,
    }
    runner.lots[side].append(make_lot(mod, side=side, px=px))
    runner.write_summary(final=True)
    return runner, json.loads(runner.summary_path.read_text()), mod.aggregate(case_out)


base_cfg = mod.RunnerConfig(
    event_lite_summary=True,
    source_link_transition_event_lite_summary=True,
    salvage_net_cap=0.95,
    salvage_age_ms=30_000,
    salvage_min_lot_cost=0.25,
)

default_runner, default_summary, default_aggregate = run_case(
    "default_off_high_cost",
    base_cfg,
    side="YES",
    px=0.40,
    yes_ask=0.50,
    no_ask=0.70,
)
assert default_summary["metrics"]["residual_qty"] == 5.0
assert "completion_residual_diagnostics" not in default_summary["event_lite"]
assert "completion_residual_diagnostics" not in default_aggregate["event_lite"]

enabled_cfg = mod.replace(base_cfg, completion_residual_diagnostic_event_lite_summary=True)
high_runner, high_summary, high_aggregate = run_case(
    "enabled_high_cost",
    enabled_cfg,
    side="YES",
    px=0.40,
    yes_ask=0.50,
    no_ask=0.70,
)
assert high_summary["metrics"] == default_summary["metrics"]
high_diag = high_summary["event_lite"]["completion_residual_diagnostics"]
assert high_diag["field_contract"]["default_off"] is True
assert high_diag["field_contract"]["trading_behavior_changed"] is False
assert high_diag["residual_qty_by_completion_reason"]["no_completion_net_pair_cost_gt_cap"] == 5.0
assert high_diag["residual_cost_by_completion_reason"]["no_completion_net_pair_cost_gt_cap"] == 2.0
assert high_diag["net_pair_cost_bucket_by_reason"]["no_completion_net_pair_cost_gt_cap"]["pair_cost_gt_1p05"] == 1
assert high_diag["top_completion_residual_blockers"][0]["reason"] == "no_completion_net_pair_cost_gt_cap"
assert high_diag["top_completion_residual_blockers"][0]["net_pair_cost"] > 0.95
assert high_aggregate["event_lite"]["completion_residual_diagnostics"]["residual_qty_by_completion_reason"]["no_completion_net_pair_cost_gt_cap"] == 5.0

missing_runner, missing_summary, _ = run_case(
    "enabled_missing_ask",
    enabled_cfg,
    side="YES",
    px=0.30,
    yes_ask=0.50,
    no_ask=0.0,
)
missing_diag = missing_summary["event_lite"]["completion_residual_diagnostics"]
assert missing_diag["residual_qty_by_completion_reason"]["no_completion_missing_live_ask"] == 5.0
assert missing_diag["top_completion_residual_blockers"][0]["comp_ask"] is None

eligible_runner, eligible_summary, _ = run_case(
    "enabled_eligible",
    enabled_cfg,
    side="YES",
    px=0.30,
    yes_ask=0.50,
    no_ask=0.45,
)
eligible_diag = eligible_summary["event_lite"]["completion_residual_diagnostics"]
assert eligible_diag["residual_qty_by_completion_status"]["completion_eligible"] == 5.0
assert eligible_diag["residual_qty_by_completion_reason"]["completion_eligible_unpaired"] == 5.0
assert eligible_diag["top_completion_residual_blockers"][0]["net_pair_cost"] <= 0.95

final_salvage_runner, final_salvage_summary, _ = run_case(
    "enabled_final_salvage",
    mod.replace(enabled_cfg, final_salvage_on_summary=True),
    side="YES",
    px=0.30,
    yes_ask=0.50,
    no_ask=0.45,
)
assert final_salvage_summary["metrics"]["residual_qty"] == 0.0
assert final_salvage_summary["metrics"]["residual_cost"] == 0.0
assert final_salvage_summary["metrics"]["salvage_actions"] == 1
assert final_salvage_summary["metrics"]["pair_actions"] == 1
assert final_salvage_summary["event_lite"]["completion_residual_diagnostics"]["residual_qty_by_completion_reason"] == {}

market_min_runner, market_min_summary, _ = run_case(
    "enabled_market_min_quote_guard",
    mod.replace(
        enabled_cfg,
        final_salvage_on_summary=True,
        market_order_min_quote_guard=True,
        market_order_min_quote=1.0,
    ),
    side="YES",
    px=0.30,
    yes_ask=0.50,
    no_ask=0.17,
)
market_min_diag = market_min_summary["event_lite"]["completion_residual_diagnostics"]
assert market_min_summary["metrics"]["residual_qty"] == 5.0
assert market_min_summary["metrics"]["salvage_actions"] == 0
assert market_min_summary["blocked"]["market_order_min_quote"] == 1
assert market_min_diag["residual_qty_by_completion_reason"]["no_completion_market_order_quote_lt_min"] == 5.0
assert market_min_diag["top_completion_residual_blockers"][0]["completion_quote_notional"] == 0.85
assert market_min_diag["top_completion_residual_blockers"][0]["market_order_min_quote"] == 1.0
assert any(
    json.loads(line).get("kind") == "fak_salvage_block"
    and json.loads(line).get("reason") == "market_order_min_quote_lt_min"
    for line in market_min_runner.events_path.read_text().splitlines()
)

late_guard_out = out_dir / "late_pair_ask_pressure_guard"
late_guard_out.mkdir(parents=True, exist_ok=True)
late_guard_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=1.0,
    imbalance_qty_cap=5.0,
    seed_l1_cap=1.02,
    seed_offset_max_s=300.0,
    event_lite_summary=True,
    late_pair_ask_pressure_guard=True,
    late_pair_ask_pressure_after_s=100.0,
    late_pair_ask_pressure_max_pair_ask=1.00,
)
late_guard_runner = mod.DPlusRunner("btc-updown-5m-1900000000", late_guard_out, late_guard_cfg)
late_guard_runner.on_book(
    {
        "kind": "market_book_tick",
        "ts_ms": 1_900_000_150_000,
        "yes_bid": 0.49,
        "yes_ask": 0.50,
        "no_bid": 0.50,
        "no_ask": 0.51,
    }
)
late_guard_runner.on_trade(
    {
        "kind": "market_trade_tick",
        "ts_ms": 1_900_000_150_000,
        "source_sequence_id": "seq-late-pair-ask-pressure",
        "market_side": "YES",
        "taker_side": "SELL",
        "price": 0.20,
        "size": 10.0,
    }
)
late_guard_runner.write_summary(final=True)
late_guard_summary = json.loads(late_guard_runner.summary_path.read_text())
assert late_guard_summary["metrics"]["candidates"] == 0
assert late_guard_summary["blocked"]["late_pair_ask_pressure"] == 1
assert late_guard_summary["event_lite"]["block_by_reason_offset_bucket"]["late_pair_ask_pressure"]["offset_ge_120"] == 1

late_low_risk_out = out_dir / "late_low_price_risk_guard"
late_low_risk_out.mkdir(parents=True, exist_ok=True)
late_low_risk_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=1.0,
    imbalance_qty_cap=5.0,
    seed_l1_cap=1.10,
    seed_offset_max_s=300.0,
    event_lite_summary=True,
    late_low_price_risk_guard=True,
    late_low_price_risk_after_s=175.0,
    late_low_price_risk_seed_price_max=0.10,
    late_low_price_risk_min_pair_ask=1.00,
)
late_low_risk_runner = mod.DPlusRunner("btc-updown-5m-1900000001", late_low_risk_out, late_low_risk_cfg)
late_low_risk_runner.on_book(
    {
        "kind": "market_book_tick",
        "ts_ms": 1_900_000_191_000,
        "yes_bid": 0.08,
        "yes_ask": 0.09,
        "no_bid": 0.99,
        "no_ask": 1.00,
    }
)
late_low_risk_runner.on_trade(
    {
        "kind": "market_trade_tick",
        "ts_ms": 1_900_000_191_000,
        "source_sequence_id": "seq-late-low-price-risk",
        "market_side": "YES",
        "taker_side": "SELL",
        "price": 0.17,
        "size": 10.0,
    }
)
late_low_risk_runner.write_summary(final=True)
late_low_risk_summary = json.loads(late_low_risk_runner.summary_path.read_text())
assert late_low_risk_summary["metrics"]["candidates"] == 0
assert late_low_risk_summary["blocked"]["late_low_price_risk"] == 1
assert late_low_risk_summary["event_lite"]["block_by_reason_offset_bucket"]["late_low_price_risk"]["offset_ge_120"] == 1

multi_out = out_dir / "enabled_multi"
multi_out.mkdir(parents=True, exist_ok=True)
for runner in (high_runner, missing_runner, eligible_runner):
    source = runner.summary_path
    dest = multi_out / source.name
    dest.write_text(source.read_text())
multi_aggregate = mod.aggregate(multi_out)
multi_diag = multi_aggregate["event_lite"]["completion_residual_diagnostics"]
assert multi_diag["residual_qty_by_completion_status"]["no_completion"] == 10.0
assert multi_diag["residual_qty_by_completion_status"]["completion_eligible"] == 5.0
assert multi_diag["residual_qty_by_completion_reason"]["no_completion_net_pair_cost_gt_cap"] == 5.0
assert multi_diag["residual_qty_by_completion_reason"]["no_completion_missing_live_ask"] == 5.0
assert multi_diag["residual_qty_by_completion_reason"]["completion_eligible_unpaired"] == 5.0
assert multi_diag["top_completion_residual_blockers"][0]["residual_cost"] >= multi_diag["top_completion_residual_blockers"][-1]["residual_cost"]

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "completion_diag_without_event_lite"),
        "--completion-residual-diagnostic-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--completion-residual-diagnostic-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

invalid_market_min = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_market_order_min_quote"),
        "--market-order-min-quote",
        "0",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_market_min.returncode != 0
assert "--market-order-min-quote must be positive" in (
    invalid_market_min.stdout + invalid_market_min.stderr
)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_completion_residual_diagnostic_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off residual completion diagnostics explain no-completion causes without changing runner behavior",
    "checks": {
        "default_completion_residual_diagnostics_absent": True,
        "enabled_metrics_match_default": True,
        "high_cost_residual_classified": True,
        "missing_ask_residual_classified": True,
        "completion_eligible_residual_classified": True,
        "final_salvage_on_summary_clears_eligible_residual": True,
        "market_order_min_quote_guard_blocks_sub_min_fak_completion": True,
        "late_pair_ask_pressure_guard_blocks_late_expensive_pair_ask": True,
        "late_low_price_risk_guard_blocks_late_low_price_expensive_pair_ask": True,
        "aggregate_merges_completion_residual_diagnostics": True,
        "cli_requires_event_lite": True,
        "cli_validates_market_order_min_quote": True,
    },
    "side_effects": {
        "network_used": False,
        "shared_ingress_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "sells_sent": False,
        "redeems_sent": False,
        "credential_read": False,
        "readiness_claim": False,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "high_cost_summary": str(high_runner.summary_path),
        "missing_ask_summary": str(missing_runner.summary_path),
        "eligible_summary": str(eligible_runner.summary_path),
        "final_salvage_summary": str(final_salvage_runner.summary_path),
        "market_min_quote_guard_summary": str(market_min_runner.summary_path),
        "late_pair_ask_pressure_guard_summary": str(late_guard_runner.summary_path),
        "late_low_price_risk_guard_summary": str(late_low_risk_runner.summary_path),
        "multi_aggregate_report": str(multi_out / "aggregate_report.json"),
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "PASS $out_dir"
