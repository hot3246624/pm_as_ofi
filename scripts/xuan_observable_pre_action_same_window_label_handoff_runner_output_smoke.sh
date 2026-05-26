#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_runner_output_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile tools/xuan_dplus_passive_passive_shadow_runner.py >> "$log" 2>&1
python3 -m py_compile scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py >> "$log" 2>&1
python3 -m py_compile scripts/xuan_observable_pre_action_same_window_label_handoff_arrival_inventory.py >> "$log" 2>&1

python3 - "$ROOT" "$out_dir" <<'PY' >> "$log" 2>&1
import csv
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
    observable_pre_action_rule_miner_feature_join_output=True,
)

default_out = out_dir / "default_off"
default_out.mkdir(parents=True, exist_ok=True)
default_runner = mod.DPlusRunner(
    slug,
    default_out,
    mod.replace(base_cfg, observable_pre_action_same_window_offline_label_handoff_output=False),
    condition_id="condition-fixture",
)
feed(default_runner, events)
default_summary = load_json(default_runner.summary_path)
default_agg = mod.aggregate(default_out)

for forbidden in (
    "observable_pre_action_same_window_offline_labels.csv",
    "observable_pre_action_same_window_offline_label_handoff.json",
    "same_window_aggregate_summary.json",
    f"{slug}.observable_pre_action_same_window_offline_labels.csv",
    f"{slug}.observable_pre_action_same_window_offline_label_handoff.json",
):
    assert not (default_out / forbidden).exists(), forbidden
assert "observable_pre_action_same_window_offline_label_handoff_manifest" not in default_agg

enabled_out = out_dir / "enabled"
enabled_out.mkdir(parents=True, exist_ok=True)
enabled_runner = mod.DPlusRunner(
    slug,
    enabled_out,
    mod.replace(base_cfg, observable_pre_action_same_window_offline_label_handoff_output=True),
    condition_id="condition-fixture",
)
feed(enabled_runner, events)
enabled_summary = load_json(enabled_runner.summary_path)
enabled_agg = mod.aggregate(enabled_out)

assert default_summary["metrics"]["candidates"] == enabled_summary["metrics"]["candidates"] == 2
assert default_summary["blocked"] == enabled_summary["blocked"] == {"target": 1, "offset": 2}
assert default_summary["metrics"]["queue_supported_fills"] == enabled_summary["metrics"]["queue_supported_fills"] == 2
assert default_summary["metrics"]["pair_actions"] == enabled_summary["metrics"]["pair_actions"] == 1

top_rows_path = enabled_out / "observable_pre_action_candidate_rows.jsonl"
top_summary_path = enabled_out / "observable_pre_action_source_link_summary.json"
top_manifest_path = enabled_out / "observable_pre_action_feature_join_manifest.json"
top_label_csv = enabled_out / "observable_pre_action_same_window_offline_labels.csv"
same_window_summary = enabled_out / "same_window_aggregate_summary.json"
handoff_manifest = enabled_out / "observable_pre_action_same_window_offline_label_handoff.json"

for required in (
    top_rows_path,
    top_summary_path,
    top_manifest_path,
    top_label_csv,
    same_window_summary,
    handoff_manifest,
):
    assert required.exists(), required

rows = load_jsonl(top_rows_path)
with top_label_csv.open(newline="") as fh:
    labels = list(csv.DictReader(fh))
assert len(rows) == len(labels) == 5
assert all(row.get("source_seed_candidate_row_id") for row in rows)
assert all(row.get("source_seed_candidate_row_id") for row in labels)
assert {row["source_seed_candidate_row_id"] for row in rows} == {
    row["source_seed_candidate_row_id"] for row in labels
}
assert all(row["post_action_outcome_labels_included"] is False for row in rows)
assert all(row["realized_pair_cost_used_as_live_criteria"] is False for row in rows)
assert all(row["trading_behavior_changed"] is False for row in rows)
assert all(str(row["outcome_labels_are_post_action"]).lower() == "true" for row in labels)
assert all("forbidden" in row["live_rule_safety_policy"] for row in labels)
assert any(float(row["source_pair_qty"] or 0.0) > 0.0 for row in labels)

handoff = load_json(handoff_manifest)
assert handoff["schema_version"] == "observable_pre_action_same_window_offline_label_handoff_v1"
assert handoff["scope"]["current_worktree_only"] is True
assert handoff["scope"]["local_only"] is True
assert handoff["provenance"]["events_jsonl_read"] is False
assert handoff["label_policy"]["labels_allowed_in_live_predicate"] is False
assert enabled_agg["observable_pre_action_same_window_offline_label_handoff_manifest"]["row_count"] == 5

contract_out = out_dir / "contract_validation"
contract_run = subprocess.run(
    [
        sys.executable,
        str(root / "scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py"),
        "--handoff-manifest",
        str(handoff_manifest),
        "--output-dir",
        str(contract_out),
        "--allow-fixture-paths-for-smoke",
        "--min-feature-rows",
        "1",
        "--min-joined-rows",
        "1",
        "--min-join-coverage",
        "1.0",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert contract_run.returncode == 0, contract_run.stderr
contract_manifest = load_json(contract_out / "manifest.json")
assert contract_manifest["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_READY"
assert contract_manifest["handoff_observations"]["joinability"]["join_coverage"] == 1.0
assert contract_manifest["research_ranking"]["strategy_evidence"] is False
assert contract_manifest["promotion_gate"]["passed"] is False

invalid_no_feature_join = subprocess.run(
    [
        sys.executable,
        str(tool_path),
        "--output-dir",
        str(out_dir / "invalid_no_feature_join"),
        "--observable-pre-action-same-window-offline-label-handoff-output",
    ],
    cwd=root,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
assert invalid_no_feature_join.returncode != 0
assert (
    "--observable-pre-action-same-window-offline-label-handoff-output requires --observable-pre-action-rule-miner-feature-join-output"
    in (invalid_no_feature_join.stdout + invalid_no_feature_join.stderr)
)

manifest = {
    "artifact": "xuan_observable_pre_action_same_window_label_handoff_runner_output_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_RUNNER_OUTPUT_SMOKE_PASS",
    "checks": {
        "default_off_absent": True,
        "enabled_handoff_files_present": True,
        "candidate_counts_unchanged": True,
        "block_counts_unchanged": True,
        "queue_supported_fills_unchanged": True,
        "pair_actions_unchanged": True,
        "exact_candidate_row_ids_join": True,
        "offline_labels_post_action_only": True,
        "contract_validator_keep": True,
        "relative_handoff_paths_validated": True,
        "cli_requires_feature_join": True,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate_passed": False,
    },
    "outputs": {
        "enabled_dir": str(enabled_out),
        "handoff_manifest": str(handoff_manifest),
        "offline_label_csv": str(top_label_csv),
        "contract_manifest": str(contract_out / "manifest.json"),
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/contract_validation/manifest.json" >/dev/null

echo "smoke ok: $out_dir" >> "$log"
