#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_runner_instrumentation_smoke_$ts}"
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
        "yes_ask": 0.45,
        "no_bid": 0.30,
        "no_ask": 0.45,
    }


def trade(ts_ms: int, side: str, public_px: float, size: float, seq: str) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "source_sequence_id": seq,
        "market_side": side,
        "taker_side": "SELL",
        "price": public_px,
        "size": size,
    }


def lot(side: str, qty: float, px: float, ts_ms: int) -> object:
    return mod.Lot(
        id=int(ts_ms % 100000) + (1 if side == "YES" else 2),
        quote_intent_id=f"fixture-{side}-{qty}-{px}",
        side=side,
        qty=qty,
        px=px,
        fill_ms=ts_ms,
        source_order_id=1,
    )


def run_case(root_out: Path, cfg: object) -> dict[str, object]:
    root_out.mkdir(parents=True, exist_ok=True)
    slug = "btc-updown-5m-1900000000"
    runner = mod.DPlusRunner(slug, root_out, cfg)
    base_ts_ms = int(mod.round_start_from_slug(slug) * 1000)
    first_ts = base_ts_ms + 100_000
    second_ts = first_ts + 3_000
    runner.on_book(book(first_ts - 1_000))
    runner.on_trade(trade(first_ts, "YES", 0.50, 2.0, "seq-blocked-yes"))
    runner.lots["YES"].append(lot("YES", 0.5, 0.45, first_ts + 1))
    before_candidates = runner.metrics.candidates
    runner.on_trade(trade(second_ts, "NO", 0.50, 2.0, "seq-admitted-no"))
    admitted_delta = runner.metrics.candidates - before_candidates
    runner.write_summary(final=True)
    return {
        "runner": runner,
        "summary": json.loads(runner.summary_path.read_text()),
        "admitted_delta": admitted_delta,
    }


base_cfg = mod.RunnerConfig(
    edge=0.07,
    queue_share=0.0,
    target_qty=10.0,
    fill_haircut=1.0,
    max_seed_qty=10.0,
    max_open_cost=100.0,
    imbalance_qty_cap=10.0,
    seed_l1_cap=1.00,
    seed_offset_max_s=301.0,
    cooldown_ms=0,
    activation_mode="opp_seen",
    activation_window_s=7.5,
    event_lite_summary=True,
    salvage_age_ms=10_000_000,
    salvage_net_cap=0.0,
)

default = run_case(out_dir / "default_off", base_cfg)
enabled_cfg = mod.replace(base_cfg, symmetric_activation_event_lite_summary=True)
enabled = run_case(out_dir / "enabled", enabled_cfg)

default_agg = mod.aggregate(out_dir / "default_off")
enabled_agg = mod.aggregate(out_dir / "enabled")

assert default["summary"]["metrics"]["candidates"] == enabled["summary"]["metrics"]["candidates"] == 1
assert default["summary"]["blocked"]["activation_opp_seen"] == enabled["summary"]["blocked"]["activation_opp_seen"] == 1
assert "symmetric_activation_summary" not in default_agg["event_lite"]

diag = enabled_agg["event_lite"]["symmetric_activation_summary"]
assert diag["schema_version"] == "symmetric_activation_contract_summary_v1"
assert diag["field_contract"]["default_off"] is True
assert diag["field_contract"]["post_action_outcome_labels_included"] is False
assert diag["field_contract"]["realized_pair_cost_used_as_live_criteria"] is False
assert diag["field_contract"]["trading_behavior_changed"] is False
assert diag["field_contract"]["private_truth_ready"] is False
assert diag["field_contract"]["deployable"] is False
assert diag["field_contract"]["promotion_gate_passed"] is False
assert diag["candidate_count_by_status_reason_activation_bucket"]["blocked|activation_opp_seen"]["activation_required_missing_opp"] == 1
assert diag["candidate_count_by_status_reason_activation_bucket"]["admitted|candidate"]["activation_required_activation_opp_age_1_5s"] == 1
assert diag["source_sequence_presence_by_status_reason_activation_bucket"]["blocked|activation_opp_seen|activation_required_missing_opp"]["present"] == 1
assert diag["source_sequence_presence_by_status_reason_activation_bucket"]["admitted|candidate|activation_required_activation_opp_age_1_5s"]["present"] == 1
assert diag["projected_pair_cost_bucket_by_status_reason_activation_bucket"]["admitted|candidate|activation_required_activation_opp_age_1_5s"]["projected_pair_cost_lt_0p90"] == 1

summary_diag = enabled["summary"]["event_lite"]["symmetric_activation_summary"]
assert summary_diag["schema_version"] == "symmetric_activation_contract_summary_v1"

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--symmetric-activation-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--symmetric-activation-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_symmetric_activation_runner_instrumentation_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY",
    "scope": {
        "local_no_network_fixture": True,
        "new_data_fetched": False,
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "raw_replay_full_store_scanned": False,
        "shared_ingress_or_live_modified": False,
        "orders_cancels_redeems_sent": False,
    },
    "checks": {
        "default_off_summary_absent": True,
        "enabled_summary_present": True,
        "candidate_count_unchanged": True,
        "blocked_count_unchanged": True,
        "aggregate_merges_summary": True,
        "blocked_activation_denominator": 1,
        "admitted_activation_denominator": 1,
        "source_sequence_coverage_present": True,
        "projected_pair_cost_bucket_present": True,
        "cli_requires_event_lite": True,
        "post_action_outcome_labels_excluded": True,
        "realized_pair_cost_not_live_criterion": True,
        "trading_behavior_changed": False,
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
    "next_executable_action": "implement symmetric_activation_summary_scorer_v1 local-only; read only summary/aggregate manifests and fail closed on missing or behavior-changing fields",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
