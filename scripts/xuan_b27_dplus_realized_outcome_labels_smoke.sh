#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_realized_outcome_labels_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
fixture_dir="$out_dir/fixtures"
: > "$log"

run_check() {
  local name="$1"
  shift
  {
    printf '== %s ==\n' "$name"
    "$@"
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

run_expect_rc() {
  local name="$1"
  local expected="$2"
  shift 2
  {
    printf '== %s ==\n' "$name"
    set +e
    "$@"
    local rc=$?
    set -e
    printf 'rc=%s expected=%s\n' "$rc" "$expected"
    [[ "$rc" == "$expected" ]]
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

producer="scripts/xuan_b27_dplus_realized_outcome_labels.py"
bridge="scripts/xuan_b27_dplus_outcome_label_bridge.py"
performance_script="scripts/xuan_b27_dplus_shadow_performance_evidence.py"
acceptance_script="scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"

run_check "producer_script_exists" test -x "$producer"
run_check "producer_script_py_compile" python3 -m py_compile "$producer"
run_check "producer_script_has_no_network_or_process_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$producer'"

run_check "fixtures_created" python3 - "$fixture_dir" <<'PY'
import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)

base_manifest = {
    "artifact": "xuan_b27_dplus_rust_no_order_shadow_run",
    "status": "PASS_LOCAL_NO_ORDER_SHADOW_FIXTURE",
    "strategy": "xuan_b27_dplus",
    "scope": "local_no_network_no_order_shadow_fixture",
    "no_order": True,
    "market_slug": "btc-updown-5m-1778893200",
    "orders_allowed": False,
    "cancels_allowed": False,
    "redeems_allowed": False,
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "auth_network_started": False,
    "started_canary": False,
    "side_effects": {
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "started_observer": False,
        "started_user_ws": False,
    },
}
summary = {
    "artifact": "xuan_b27_dplus_observer_summary",
    "status": "PASS_NO_ORDER_OBSERVER",
    "markets": ["btc-updown-5m-1778893200"],
    "event_counts": {"xuan_b27_dplus_observer_tick": 120000},
    "jsonl_file_count": 1,
    "observer_candidate_counts": {
        "would_track": 120,
        "would_place": 0,
        "submitted_previews": 0,
        "trace_previews": 120,
    },
    "source_truth_verdict_counts": {"UNKNOWN": 120, "FAIL": 0},
    "decode_error_count": 0,
    "truncated_final_line_count": 0,
    "no_order_violation_count": 0,
    "run_manifest": {
        "dry_run": True,
        "elapsed_seconds": 1800,
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
    },
}

def make_run(name, manifest=None, events=None, edge_only=False, wrong_market=False, side_effect=False, negative=False, low=False, decode=False):
    run = root / name
    run.mkdir(parents=True, exist_ok=True)
    (run / "manifest.json").write_text(json.dumps(manifest or base_manifest, indent=2, sort_keys=True) + "\n")
    if decode:
        (run / "outcome_events.jsonl").write_text("{not-json}\n")
        return run
    count = 5 if low else 120
    with (run / "outcome_events.jsonl").open("w") as fh:
        for i in range(count):
            fh.write(json.dumps({
                "event": "xuan_b27_dplus_dry_run_outcome_label",
                "market_slug": "eth-updown-5m-1778893200" if wrong_market else "btc-updown-5m-1778893200",
                "candidate_id": f"candidate-{i}",
                "order_attempt_id": f"attempt-{i}",
                "outcome_label_id": f"outcome-{i}",
                "outcome_label_source": "local_no_order_shadow_fixture",
                "performance_basis": "observer_edge_only_not_realized" if edge_only else "dry_run_outcome",
                "realized_edge_bps": -0.5 if negative else 0.9 + (i % 5) * 0.1,
                "orders_sent": side_effect and i == 0,
                "cancels_sent": False,
                "redeems_sent": False,
                "auth_network_started": False,
                "started_canary": False,
            }) + "\n")
    return run

make_run("pass_run")
bad_manifest = copy.deepcopy(base_manifest)
bad_manifest["orders_allowed"] = True
make_run("unsafe_manifest_run", manifest=bad_manifest)
make_run("decode_run", decode=True)
make_run("wrong_market_run", wrong_market=True)
make_run("side_effect_run", side_effect=True)
make_run("edge_only_run", edge_only=True)
make_run("low_run", low=True)
make_run("negative_run", negative=True)
(root / "observer_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
with (root / "edge_samples.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "order_attempt_id": f"attempt-{i}",
            "accepted": True,
            "would_track": True,
            "would_place": False,
            "edge_bps": 1.2,
            "expected_value_bps": 1.2,
            "performance_basis": "observer_edge_only_not_realized",
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
        }) + "\n")
PY

pass_dir="$out_dir/pass_labels"
unsafe_manifest_dir="$out_dir/unsafe_manifest"
decode_dir="$out_dir/decode"
wrong_market_dir="$out_dir/wrong_market"
side_effect_dir="$out_dir/side_effect"
edge_only_dir="$out_dir/edge_only"
low_dir="$out_dir/low"
negative_dir="$out_dir/negative"
bridge_dir="$out_dir/bridge_from_labels"
perf_dir="$out_dir/performance_from_labels"
acceptance_dir="$out_dir/acceptance_from_labels"

run_expect_rc "pass_labels_fixture" 0 "$producer" --run-dir "$fixture_dir/pass_run" --output-dir "$pass_dir"
run_expect_rc "unsafe_manifest_fixture" 3 "$producer" --run-dir "$fixture_dir/unsafe_manifest_run" --output-dir "$unsafe_manifest_dir"
run_expect_rc "decode_fixture" 4 "$producer" --run-dir "$fixture_dir/decode_run" --output-dir "$decode_dir"
run_expect_rc "wrong_market_fixture" 5 "$producer" --run-dir "$fixture_dir/wrong_market_run" --output-dir "$wrong_market_dir"
run_expect_rc "side_effect_fixture" 6 "$producer" --run-dir "$fixture_dir/side_effect_run" --output-dir "$side_effect_dir"
run_expect_rc "edge_only_fixture" 7 "$producer" --run-dir "$fixture_dir/edge_only_run" --output-dir "$edge_only_dir"
run_expect_rc "low_label_fixture" 8 "$producer" --run-dir "$fixture_dir/low_run" --output-dir "$low_dir"
run_expect_rc "negative_fixture" 9 "$producer" --run-dir "$fixture_dir/negative_run" --output-dir "$negative_dir"

run_expect_rc "bridge_accepts_produced_labels" 0 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$pass_dir/outcome_labels.jsonl" \
  --output-dir "$bridge_dir"
run_expect_rc "performance_accepts_produced_labels" 0 "$performance_script" \
  --observer-summary "$fixture_dir/observer_summary.json" \
  --edge-samples "$bridge_dir/realized_edge_samples.jsonl" \
  --output-dir "$perf_dir"
run_expect_rc "acceptance_accepts_produced_performance" 0 "$acceptance_script" \
  --observer-summary "$fixture_dir/observer_summary.json" \
  --performance-evidence "$perf_dir/manifest.json" \
  --output-dir "$acceptance_dir"

run_check "producer_manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$unsafe_manifest_dir/manifest.json" \
  "$decode_dir/manifest.json" \
  "$wrong_market_dir/manifest.json" \
  "$side_effect_dir/manifest.json" \
  "$edge_only_dir/manifest.json" \
  "$low_dir/manifest.json" \
  "$negative_dir/manifest.json" \
  "$bridge_dir/manifest.json" \
  "$perf_dir/manifest.json" \
  "$acceptance_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
expected = [
    "PASS_REALIZED_OUTCOME_LABELS",
    "FAIL_RUN_MANIFEST_NOT_SAFE",
    "FAIL_OUTCOME_EVENT_DECODE",
    "FAIL_MARKET_SCOPE",
    "FAIL_FORBIDDEN_SIDE_EFFECT",
    "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS",
    "FAIL_INSUFFICIENT_OUTCOME_LABELS",
    "FAIL_REALIZED_OUTCOME_QUALITY",
    "PASS_OUTCOME_LABEL_BRIDGE",
    "PASS_STRATEGY_PERFORMANCE_EVIDENCE",
    "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE",
]
ok = [item.get("status") for item in data] == expected
for item in data:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent", False) is False
    ok = ok and item.get("redeems_sent", False) is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and all(value is False for value in side_effects.values())
ok = ok and data[0].get("outcome_label_count") == 120
ok = ok and data[0].get("outcome_labels_ready") is True
ok = ok and data[8].get("strategy_performance_evidence_ready") is True
ok = ok and data[9].get("evidence_passed") is True
ok = ok and data[10].get("strategy_acceptance_passed") is True
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_realized_outcome_labels_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_realized_outcome_label_producer_gate",
  "producer": "$producer",
  "producer_pass_manifest": "$pass_dir/manifest.json",
  "bridge_from_labels_manifest": "$bridge_dir/manifest.json",
  "performance_from_labels_manifest": "$perf_dir/manifest.json",
  "acceptance_from_labels_manifest": "$acceptance_dir/manifest.json",
  "real_outcome_labels_published": false,
  "acceptance_artifact_published": false,
  "orders_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
