#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_feature_join_runner_instrumentation_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

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


def feed(runner, events: list[dict[str, object]]) -> None:
    for event in events:
        mod.handle_market_message(runner, event)
    runner.write_summary(final=True)


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def load_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


slug = "btc-updown-5m-1900000000"
base_ts = 1_900_000_000_000
events = [
    book(base_ts + 1_000),
    sell(base_ts + 20_000, "YES", 0.40),
    sell(base_ts + 21_000, "YES", 0.39),
    sell(base_ts + 30_000, "NO", 0.40),
    sell(base_ts + 121_000, "YES", 0.30),
    sell(base_ts + 122_000, "NO", 0.30),
]

base_cfg = mod.RunnerConfig(
    edge=0.07,
    activation_mode="none",
    cooldown_ms=0,
    target_qty=5.0,
    fill_haircut=0.25,
    imbalance_qty_cap=5.0,
    salvage_net_cap=0.0,
    event_lite_summary=True,
    source_link_transition_event_lite_summary=True,
)

default_out = out_dir / "default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(
    slug,
    default_out,
    mod.replace(base_cfg, observable_pre_action_rule_miner_feature_join_output=False),
    condition_id="condition-fixture",
)
feed(default_runner, events)
default_summary = load_json(default_runner.summary_path)
default_agg = mod.aggregate(default_out)

for forbidden in (
    "observable_pre_action_candidate_rows.jsonl",
    "observable_pre_action_source_link_summary.json",
    "observable_pre_action_feature_join_manifest.json",
    f"{slug}.observable_pre_action_candidate_rows.jsonl",
    f"{slug}.observable_pre_action_source_link_summary.json",
    f"{slug}.observable_pre_action_feature_join_manifest.json",
):
    assert not (default_out / forbidden).exists(), forbidden
assert "observable_pre_action_feature_join_manifest" not in default_agg

enabled_out = out_dir / "enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
enabled_runner = mod.DPlusRunner(
    slug,
    enabled_out,
    mod.replace(base_cfg, observable_pre_action_rule_miner_feature_join_output=True),
    condition_id="condition-fixture",
)
feed(enabled_runner, events)
enabled_summary = load_json(enabled_runner.summary_path)
enabled_agg = mod.aggregate(enabled_out)

assert default_summary["metrics"]["candidates"] == enabled_summary["metrics"]["candidates"] == 2
assert default_summary["blocked"] == enabled_summary["blocked"] == {"target": 1, "offset": 2}
assert default_summary["metrics"]["queue_supported_fills"] == enabled_summary["metrics"]["queue_supported_fills"] == 2
assert default_summary["metrics"]["pair_actions"] == enabled_summary["metrics"]["pair_actions"] == 1

per_slug_rows_path = enabled_out / f"{slug}.observable_pre_action_candidate_rows.jsonl"
per_slug_summary_path = enabled_out / f"{slug}.observable_pre_action_source_link_summary.json"
per_slug_manifest_path = enabled_out / f"{slug}.observable_pre_action_feature_join_manifest.json"
top_rows_path = enabled_out / "observable_pre_action_candidate_rows.jsonl"
top_summary_path = enabled_out / "observable_pre_action_source_link_summary.json"
top_manifest_path = enabled_out / "observable_pre_action_feature_join_manifest.json"

for required in (
    per_slug_rows_path,
    per_slug_summary_path,
    per_slug_manifest_path,
    top_rows_path,
    top_summary_path,
    top_manifest_path,
):
    assert required.exists(), required

rows = load_jsonl(top_rows_path)
assert len(rows) == 5
assert {row["status_before_action"] for row in rows} == {"admitted", "blocked"}
assert sum(1 for row in rows if row["status_before_action"] == "admitted") == 2
assert sum(1 for row in rows if row["status_before_action"] == "blocked") == 3
assert all("source_sequence_present" in row for row in rows)
assert all("quote_intent_present" in row for row in rows)
assert all("source_order_present" in row for row in rows)
assert all("pre_seed_open_qty_bucket" in row for row in rows)
assert all("pre_seed_deficit_qty_bucket" in row for row in rows)
assert all("candidate_qty_bucket" in row for row in rows)
assert all("pre_seed_opp_avg_cost" in row for row in rows)
assert all("candidate_projected_pair_cost_from_pre_seed_opp_avg" in row for row in rows)
assert all("candidate_projected_pair_cost_bucket" in row for row in rows)
assert all("ledger_proxy_before" in row for row in rows)
assert all("ledger_proxy_after" in row for row in rows)
assert all("ledger_proxy_after_bucket" in row for row in rows)
assert all("candidate_seed_px" in row for row in rows)
assert all("candidate_seed_px_bucket" in row for row in rows)
assert all("candidate_public_trade_px" in row for row in rows)
assert all("candidate_public_trade_px_bucket" in row for row in rows)
assert all("candidate_l1_pair_ask" in row for row in rows)
assert all("candidate_l1_pair_ask_bucket" in row for row in rows)
assert all("candidate_side_bid" in row for row in rows)
assert all("candidate_side_ask" in row for row in rows)
assert all("candidate_side_spread" in row for row in rows)
assert all("candidate_side_spread_bucket" in row for row in rows)
assert all("entry_quality_side_mid_delta_5s" in row for row in rows)
assert all("entry_quality_side_mid_delta_5s_bucket" in row for row in rows)
assert all("entry_quality_side_mid_delta_15s" in row for row in rows)
assert all("entry_quality_side_mid_delta_15s_bucket" in row for row in rows)
assert all("entry_quality_side_mid_delta_30s" in row for row in rows)
assert all("entry_quality_side_mid_delta_30s_bucket" in row for row in rows)
assert all("entry_quality_side_trade_qty_10s" in row for row in rows)
assert all("entry_quality_opp_trade_qty_10s" in row for row in rows)
assert all("entry_quality_trade_qty_imbalance_10s" in row for row in rows)
assert all("entry_quality_trade_qty_imbalance_10s_bucket" in row for row in rows)
assert all("entry_quality_side_trade_qty_30s" in row for row in rows)
assert all("entry_quality_opp_trade_qty_30s" in row for row in rows)
assert all("entry_quality_trade_qty_imbalance_30s" in row for row in rows)
assert all("entry_quality_trade_qty_imbalance_30s_bucket" in row for row in rows)
assert all(row["post_action_outcome_labels_included"] is False for row in rows)
assert all(row["realized_pair_cost_used_as_live_criteria"] is False for row in rows)
assert all(row["candidate_projected_pair_cost_used_as_live_criteria"] is False for row in rows)
assert all(row["ledger_proxy_after_used_as_live_criteria"] is False for row in rows)
assert all(row["entry_quality_field_instrumentation_trading_behavior_changed"] is False for row in rows)
assert all(row["trading_behavior_changed"] is False for row in rows)

admitted = [row for row in rows if row["status_before_action"] == "admitted"]
blocked = [row for row in rows if row["status_before_action"] == "blocked"]
assert all(row["quote_intent_present"] is True for row in admitted)
assert all(row["source_order_present"] is True for row in admitted)
assert all(row["source_sequence_present"] is True for row in admitted + blocked)
assert any(row["candidate_projected_pair_cost_from_pre_seed_opp_avg"] is not None for row in admitted)
assert any(row["candidate_seed_px"] is not None for row in admitted)
assert any(row["candidate_public_trade_px"] is not None for row in admitted)
assert any(row["candidate_l1_pair_ask"] is not None for row in admitted)
assert any(row["candidate_side_spread"] is not None for row in admitted)
assert any(row["entry_quality_side_mid_delta_5s"] is not None for row in admitted)
assert any(row["entry_quality_trade_qty_imbalance_10s"] is not None for row in admitted)
assert any(row["block_reason"] == "target" for row in blocked)
assert any(row["block_reason"] == "offset" for row in blocked)

top_summary = load_json(top_summary_path)
top_manifest = load_json(top_manifest_path)
assert top_summary["schema_version"] == "observable_pre_action_source_link_summary_v1"
assert top_summary["row_count"] == 5
assert top_summary["row_count_by_status"]["admitted"] == 2
assert top_summary["row_count_by_status"]["blocked"] == 3
assert top_summary["source_sequence_presence_by_status"]["admitted"]["present"] == 2
assert top_summary["source_sequence_presence_by_status"]["blocked"]["present"] == 3
assert "candidate_projected_pair_cost_bucket_by_status_reason" in top_summary
assert "ledger_proxy_after_bucket_by_status_reason" in top_summary
assert "candidate_seed_px_bucket_by_status_reason" in top_summary
assert "candidate_public_trade_px_bucket_by_status_reason" in top_summary
assert "candidate_l1_pair_ask_bucket_by_status_reason" in top_summary
assert "candidate_side_spread_bucket_by_status_reason" in top_summary
assert "entry_quality_side_mid_delta_5s_bucket_by_status_reason" in top_summary
assert "entry_quality_side_mid_delta_15s_bucket_by_status_reason" in top_summary
assert "entry_quality_side_mid_delta_30s_bucket_by_status_reason" in top_summary
assert "entry_quality_trade_qty_imbalance_10s_bucket_by_status_reason" in top_summary
assert "entry_quality_trade_qty_imbalance_30s_bucket_by_status_reason" in top_summary
assert top_manifest["schema_version"] == "observable_pre_action_feature_join_manifest_v1"
assert top_manifest["candidate_row_count"] == 5
assert enabled_agg["observable_pre_action_feature_join_manifest"]["candidate_row_count"] == 5
assert top_manifest["field_contract"]["default_off"] is True
assert top_manifest["field_contract"]["reads_events_jsonl"] is False
assert top_manifest["field_contract"]["post_action_outcome_labels_included"] is False
assert top_manifest["field_contract"]["realized_pair_cost_used_as_live_criteria"] is False
assert top_manifest["field_contract"]["candidate_projected_pair_cost_used_as_live_criteria"] is False
assert top_manifest["field_contract"]["ledger_proxy_after_used_as_live_criteria"] is False
assert top_manifest["field_contract"]["entry_quality_field_instrumentation_trading_behavior_changed"] is False
assert top_manifest["field_contract"]["trading_behavior_changed"] is False
assert top_manifest["field_contract"]["private_truth_ready"] is False
assert top_manifest["field_contract"]["deployable"] is False
assert top_manifest["field_contract"]["promotion_gate_passed"] is False
assert top_manifest["safety"]["orders_sent"] is False
assert top_manifest["safety"]["events_jsonl_read"] is False
assert top_manifest["safety"]["raw_replay_or_full_store_scan"] is False

invalid_no_event = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_no_event"),
        "--observable-pre-action-rule-miner-feature-join-output",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_no_event.returncode != 0
assert "--observable-pre-action-rule-miner-feature-join-output requires --event-lite-summary" in (
    invalid_no_event.stdout + invalid_no_event.stderr
)

invalid_no_source_link = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_no_source_link"),
        "--event-lite-summary",
        "--observable-pre-action-rule-miner-feature-join-output",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_no_source_link.returncode != 0
assert (
    "--observable-pre-action-rule-miner-feature-join-output requires --source-link-transition-event-lite-summary"
    in (invalid_no_source_link.stdout + invalid_no_source_link.stderr)
)

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_feature_join_runner_instrumentation_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_RUNNER_INSTRUMENTATION_SMOKE_PASS",
    "checks": {
        "default_off_absent": True,
        "enabled_files_present": True,
        "top_level_allowlisted_files_present": True,
        "candidate_counts_unchanged": True,
        "block_counts_unchanged": True,
        "queue_supported_fills_unchanged": True,
        "row_level_status_before_action_present": True,
        "row_level_block_reason_present": True,
        "row_level_source_presence_present": True,
        "row_level_pre_seed_fields_present": True,
        "row_level_candidate_projected_pair_cost_fields_present": True,
        "row_level_ledger_proxy_fields_present": True,
        "row_level_entry_quality_fields_present": True,
        "summary_candidate_projected_pair_cost_bucket_present": True,
        "summary_ledger_proxy_after_bucket_present": True,
        "summary_entry_quality_bucket_present": True,
        "aggregate_manifest_present": True,
        "cli_requires_event_lite": True,
        "cli_requires_source_link_transition": True,
        "post_action_outcome_labels_excluded": True,
        "realized_pair_cost_used_as_live_criteria": False,
        "candidate_projected_pair_cost_used_as_live_criteria": False,
        "ledger_proxy_after_used_as_live_criteria": False,
        "entry_quality_field_instrumentation_trading_behavior_changed": False,
        "trading_behavior_changed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate_passed": False,
    },
    "outputs": {
        "default_dir": str(default_out),
        "enabled_dir": str(enabled_out),
        "candidate_rows_path": str(top_rows_path),
        "source_link_summary_path": str(top_summary_path),
        "feature_join_manifest_path": str(top_manifest_path),
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
