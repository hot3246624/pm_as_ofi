#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_$ts}"
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


def book(ts_ms: int) -> dict[str, object]:
    return {
        "kind": "market_book_tick",
        "ts_ms": ts_ms,
        "yes_bid": 0.30,
        "yes_ask": 0.45,
        "no_bid": 0.30,
        "no_ask": 0.45,
    }


def trade(ts_ms: int, side: str, public_px: float, size: float) -> dict[str, object]:
    return {
        "kind": "market_trade_tick",
        "ts_ms": ts_ms,
        "source_sequence_id": f"seq-{side}-{ts_ms}",
        "market_side": side,
        "taker_side": "SELL",
        "price": public_px,
        "size": size,
    }


def run_case(root_out: Path, name: str, slug: str, offset_s: float, pre_lots: list[object], side: str, public_px: float, size: float, cfg: object) -> dict[str, object]:
    root_out.mkdir(parents=True, exist_ok=True)
    runner = mod.DPlusRunner(slug, root_out, cfg)
    base_ts_ms = int(mod.round_start_from_slug(slug) * 1000)
    ts_ms = base_ts_ms + int(offset_s * 1000)
    runner.on_book(book(ts_ms - 1000))
    for item in pre_lots:
        runner.lots[item.side].append(item)
    before_candidates = runner.metrics.candidates
    runner.on_trade(trade(ts_ms, side, public_px, size))
    runner.write_summary(final=True)
    summary = json.loads(runner.summary_path.read_text())
    return {
        "runner": runner,
        "name": name,
        "summary": summary,
        "candidates_delta": runner.metrics.candidates - before_candidates,
        "blocked": dict(runner.blocked),
    }


def run_suite(root_out: Path, cfg: object) -> dict[str, object]:
    base = 1_900_000_000_000
    cases = [
        run_case(
            root_out,
            "starter_eth",
            "eth-updown-15m-1900000000",
            700.0,
            [lot("NO", 2.0, 0.40, base + 1)],
            "YES",
            0.50,
            2.0,
            cfg,
        ),
        run_case(
            root_out,
            "core_btc",
            "btc-updown-5m-1900000000",
            100.0,
            [lot("NO", 5.0, 0.45, base + 2)],
            "YES",
            0.52,
            5.0,
            cfg,
        ),
        run_case(
            root_out,
            "hard_kill_sol",
            "sol-updown-15m-1900000000",
            700.0,
            [lot("NO", 2.0, 0.55, base + 3)],
            "YES",
            0.62,
            2.0,
            cfg,
        ),
    ]
    aggregate = mod.aggregate(root_out)
    return {
        "cases": cases,
        "aggregate": aggregate,
        "candidate_count": sum(item["candidates_delta"] for item in cases),
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
    seed_offset_max_s=901.0,
    cooldown_ms=0,
    event_lite_summary=True,
    salvage_age_ms=10_000_000,
    salvage_net_cap=0.0,
)

default = run_suite(out_dir / "default_off", base_cfg)
enabled_cfg = mod.replace(base_cfg, ce25_projected_guard_event_lite_summary=True)
enabled = run_suite(out_dir / "enabled", enabled_cfg)

assert default["candidate_count"] == enabled["candidate_count"] == 3
assert "ce25_projected_guard_summary" not in default["aggregate"]["event_lite"]
enabled_diag = enabled["aggregate"]["event_lite"]["ce25_projected_guard_summary"]
assert enabled_diag["schema_version"] == "ce25_projected_guard_summary_v1"
assert enabled_diag["field_contract"]["default_off"] is True
assert enabled_diag["field_contract"]["post_action_outcome_labels_included"] is False
assert enabled_diag["field_contract"]["trading_behavior_changed"] is False
assert enabled_diag["field_contract"]["private_truth_ready"] is False
assert enabled_diag["field_contract"]["deployable"] is False
assert enabled_diag["field_contract"]["promotion_gate_passed"] is False
assert enabled_diag["decision_count_by_guard"]["starter"]["would_allow_projected_guard"] == 1
assert enabled_diag["decision_count_by_guard"]["core"]["would_allow_projected_guard"] == 1
assert enabled_diag["decision_count_by_guard"]["hard_kill"]["would_block_pair_cost_gte_1_00"] == 1
assert enabled_diag["projected_pair_cost_bucket_by_guard"]["starter"]["projected_pair_cost_lt_0p90"] == 1
assert enabled_diag["projected_pair_cost_bucket_by_guard"]["core"]["projected_pair_cost_0p90_0p95"] == 1
assert enabled_diag["projected_pair_cost_bucket_by_guard"]["hard_kill"]["projected_pair_cost_gte_1p00"] == 1
assert enabled_diag["projected_residual_bucket_by_guard"]["starter"]["projected_residual_lt_10pct"] == 1
assert enabled_diag["projected_residual_bucket_by_guard"]["core"]["projected_residual_lt_10pct"] == 1
assert enabled_diag["final_window_policy_count"]["final_1m_5m"]["would_allow_projected_guard"] == 2

for case in enabled["cases"]:
    summary = case["summary"]
    assert summary["metrics"]["candidates"] == 1
    diag = summary["event_lite"]["ce25_projected_guard_summary"]
    assert diag["schema_version"] == "ce25_projected_guard_summary_v1"

invalid = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_cli"),
        "--ce25-projected-guard-event-lite-summary",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid.returncode != 0
assert "--ce25-projected-guard-event-lite-summary requires --event-lite-summary" in (
    invalid.stdout + invalid.stderr
)

manifest = {
    "artifact": "xuan_ce25_projected_guard_runner_instrumentation_smoke",
    "created_utc": out_dir.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY",
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
        "default_off_ce25_summary_absent": True,
        "enabled_ce25_summary_present": True,
        "candidate_count_unchanged": True,
        "aggregate_merges_ce25_summary": True,
        "starter_allow_count": 1,
        "core_allow_count": 1,
        "hard_kill_pair_cost_block_count": 1,
        "cli_requires_event_lite": True,
        "post_action_outcome_labels_excluded": True,
        "trading_behavior_changed": False,
    },
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
    "next_executable_action": "implement ce25_projected_guard_summary_scorer_v1 local-only; read only summary/aggregate manifests and fail closed on missing or behavior-changing fields",
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
