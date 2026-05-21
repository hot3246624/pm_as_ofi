#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_portfolio_ledger_smoke_$ts}"
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


def paired_events(start_s: int) -> list[dict[str, object]]:
    base_ts = start_s * 1000
    return [
        book(base_ts + 1_000),
        sell(base_ts + 20_000, "YES", 0.40),
        sell(base_ts + 30_000, "NO", 0.40),
        sell(base_ts + 121_000, "YES", 0.30),
        sell(base_ts + 122_000, "NO", 0.30),
    ]


def residual_events(start_s: int) -> list[dict[str, object]]:
    base_ts = start_s * 1000
    return [
        book(base_ts + 1_000),
        sell(base_ts + 20_000, "YES", 0.40),
        sell(base_ts + 121_000, "YES", 0.30),
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

paired_slug = "btc-updown-5m-1900000000"
residual_slug = "btc-updown-5m-1900000300"

default_out = out_dir / "portfolio_default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(paired_slug, default_out, mod.replace(base_cfg, event_lite_summary=True))
feed(default_runner, paired_events(1_900_000_000))
default_summary = load_summary(default_runner.summary_path)
default_report = mod.aggregate(default_out)
assert default_runner.metrics.pair_qty == 5.0
assert "portfolio_ledger_diagnostics" not in default_summary["event_lite"]
assert "portfolio_ledger_diagnostics" not in default_report["event_lite"]

enabled_out = out_dir / "portfolio_enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
paired_runner = mod.DPlusRunner(
    paired_slug,
    enabled_out,
    mod.replace(base_cfg, event_lite_summary=True, portfolio_ledger_event_lite_summary=True),
)
residual_runner = mod.DPlusRunner(
    residual_slug,
    enabled_out,
    mod.replace(base_cfg, event_lite_summary=True, portfolio_ledger_event_lite_summary=True),
)
feed(paired_runner, paired_events(1_900_000_000))
feed(residual_runner, residual_events(1_900_000_300))
enabled_report = mod.aggregate(enabled_out)
paired_summary = load_summary(paired_runner.summary_path)
residual_summary = load_summary(residual_runner.summary_path)
paired_diag = paired_summary["event_lite"]["portfolio_ledger_diagnostics"]
residual_diag = residual_summary["event_lite"]["portfolio_ledger_diagnostics"]
aggregate_diag = enabled_report["event_lite"]["portfolio_ledger_diagnostics"]

assert paired_summary["metrics"]["pair_qty"] == 5.0
assert paired_diag["seed_actions"] == 2
assert paired_diag["queue_supported_fills"] == 2
assert paired_diag["pair_actions"] == 1
assert paired_diag["pair_qty"] == 5.0
assert paired_diag["pair_cost_sum"] == 3.3
assert paired_diag["avg_pair_cost"] == 0.66
assert paired_diag["pair_pnl"] == 1.7
assert paired_diag["official_taker_fee_proxy"] == 0.0
assert paired_diag["residual_qty"] == 0
assert paired_diag["conservative_risk_adjusted_proxy"] == 1.6
assert paired_diag["risk_adjusted_nonnegative"] is True
assert paired_diag["risk_adjusted_bucket"] == "risk_adjusted_nonnegative"

assert residual_summary["metrics"]["pair_qty"] == 0.0
assert residual_diag["seed_actions"] == 1
assert residual_diag["queue_supported_fills"] == 1
assert residual_diag["pair_qty"] == 0.0
assert residual_diag["pair_pnl"] == 0.0
assert residual_diag["residual_qty"] == 5.0
assert residual_diag["residual_cost"] == 1.65
assert residual_diag["residual_qty_by_side"]["YES"] == 5.0
assert residual_diag["residual_cost_by_side"]["YES"] == 1.65
assert residual_diag["conservative_risk_adjusted_proxy"] == -1.7
assert residual_diag["risk_adjusted_nonnegative"] is False
assert residual_diag["risk_adjusted_bucket"] == "risk_adjusted_negative"

assert aggregate_diag["condition_count"] == 2
assert aggregate_diag["seed_actions"] == 3
assert aggregate_diag["queue_supported_fills"] == 3
assert aggregate_diag["pair_actions"] == 1
assert aggregate_diag["pair_qty"] == 5.0
assert aggregate_diag["pair_cost_sum"] == 3.3
assert aggregate_diag["avg_pair_cost"] == 0.66
assert aggregate_diag["pair_pnl"] == 1.7
assert aggregate_diag["residual_qty"] == 5.0
assert aggregate_diag["residual_cost"] == 1.65
assert aggregate_diag["residual_qty_by_side"]["YES"] == 5.0
assert aggregate_diag["condition_count_by_risk_adjusted_bucket"]["risk_adjusted_nonnegative"] == 1
assert aggregate_diag["condition_count_by_risk_adjusted_bucket"]["risk_adjusted_negative"] == 1
assert aggregate_diag["conservative_risk_adjusted_proxy"] == -0.1
assert aggregate_diag["risk_adjusted_bucket"] == "risk_adjusted_negative"

disabled_cfg_out = out_dir / "portfolio_without_event_lite"
invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(disabled_cfg_out),
        "--portfolio-ledger-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--portfolio-ledger-event-lite-summary requires --event-lite-summary" in (invalid.stdout + invalid.stderr)

manifest = {
    "artifact": "xuan_b27_dplus_passive_passive_shadow_runner_portfolio_ledger_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "tool": str(tool_path),
    "hypothesis": "default-off portfolio ledger event-lite diagnostics expose condition-level paired inventory, residual risk, and risk-adjusted buckets without changing default behavior",
    "checks": {
        "default_event_lite_portfolio_ledger_absent": True,
        "paired_condition_ledger_positive": True,
        "residual_heavy_condition_ledger_negative": True,
        "aggregate_merges_condition_ledgers": True,
        "aggregate_reports_risk_adjusted_bucket_counts": True,
        "cli_requires_event_lite": True,
    },
    "outputs": {
        "default_summary": str(default_runner.summary_path),
        "default_aggregate_report": str(default_out / "aggregate_report.json"),
        "paired_summary": str(paired_runner.summary_path),
        "residual_summary": str(residual_runner.summary_path),
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
    },
    "next_action": "After regression smokes pass, use the diagnostic flag only to observe public-profile paired-inventory ledger quality in no-order shadows; do not treat diagnostics as promotion evidence.",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ passive/passive shadow runner portfolio ledger smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
