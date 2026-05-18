#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_outcome_label_bridge_smoke_$ts}"
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

bridge="scripts/xuan_b27_dplus_outcome_label_bridge.py"
performance_script="scripts/xuan_b27_dplus_shadow_performance_evidence.py"
acceptance_script="scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"

run_check "bridge_script_exists" test -x "$bridge"
run_check "bridge_script_py_compile" python3 -m py_compile "$bridge"
run_check "bridge_script_has_no_network_or_process_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$bridge'"

run_check "fixtures_created" python3 - "$fixture_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
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
            "edge_bps": 1.3 + (i % 5) * 0.1,
            "expected_value_bps": 1.3 + (i % 5) * 0.1,
            "performance_basis": "observer_edge_only_not_realized",
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
        }) + "\n")

with (root / "outcome_labels_pass.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "outcome_label_id": f"label-{i}",
            "outcome_label_source": "local_no_order_shadow_fixture",
            "performance_basis": "dry_run_outcome",
            "realized_edge_bps": 0.8 + (i % 7) * 0.1,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
        }) + "\n")

with (root / "outcome_labels_edge_only.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "performance_basis": "observer_edge_only_not_realized",
            "realized_edge_bps": 1.0,
        }) + "\n")

with (root / "outcome_labels_low_join.jsonl").open("w") as fh:
    for i in range(5):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "performance_basis": "dry_run_outcome",
            "realized_edge_bps": 1.0,
        }) + "\n")

with (root / "outcome_labels_negative.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "performance_basis": "dry_run_outcome",
            "realized_edge_bps": -0.2,
        }) + "\n")

with (root / "outcome_labels_side_effect.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "performance_basis": "dry_run_outcome",
            "realized_edge_bps": 1.0,
            "orders_sent": i == 0,
        }) + "\n")

with (root / "outcome_labels_wrong_market.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "eth-updown-5m-1778893200",
            "candidate_id": f"candidate-{i}",
            "performance_basis": "dry_run_outcome",
            "realized_edge_bps": 1.0,
        }) + "\n")

(root / "outcome_labels_decode_error.jsonl").write_text("{not-json}\n")
PY

pass_dir="$out_dir/bridge_pass"
decode_dir="$out_dir/bridge_decode_error"
wrong_market_dir="$out_dir/bridge_wrong_market"
side_effect_dir="$out_dir/bridge_side_effect"
low_join_dir="$out_dir/bridge_low_join"
edge_only_dir="$out_dir/bridge_edge_only"
negative_dir="$out_dir/bridge_negative"
perf_dir="$out_dir/performance_from_bridge"
acceptance_dir="$out_dir/acceptance_from_bridge"

run_expect_rc "pass_bridge_fixture" 0 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_pass.jsonl" \
  --output-dir "$pass_dir" \
  --min-joined-outcomes 100
run_expect_rc "outcome_decode_error_fixture" 4 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_decode_error.jsonl" \
  --output-dir "$decode_dir"
run_expect_rc "wrong_market_fixture" 5 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_wrong_market.jsonl" \
  --output-dir "$wrong_market_dir"
run_expect_rc "side_effect_fixture" 6 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_side_effect.jsonl" \
  --output-dir "$side_effect_dir"
run_expect_rc "low_join_fixture" 7 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_low_join.jsonl" \
  --output-dir "$low_join_dir"
run_expect_rc "edge_only_fixture" 8 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_edge_only.jsonl" \
  --output-dir "$edge_only_dir"
run_expect_rc "negative_realized_fixture" 9 "$bridge" \
  --edge-samples "$fixture_dir/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/outcome_labels_negative.jsonl" \
  --output-dir "$negative_dir"

run_expect_rc "performance_evidence_accepts_joined_outcomes" 0 "$performance_script" \
  --observer-summary "$fixture_dir/observer_summary.json" \
  --edge-samples "$pass_dir/realized_edge_samples.jsonl" \
  --output-dir "$perf_dir" \
  --min-samples 100

run_expect_rc "strategy_acceptance_accepts_fixture_performance" 0 "$acceptance_script" \
  --observer-summary "$fixture_dir/observer_summary.json" \
  --performance-evidence "$perf_dir/manifest.json" \
  --output-dir "$acceptance_dir" \
  --min-performance-samples 100

run_check "bridge_manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$decode_dir/manifest.json" \
  "$wrong_market_dir/manifest.json" \
  "$side_effect_dir/manifest.json" \
  "$low_join_dir/manifest.json" \
  "$edge_only_dir/manifest.json" \
  "$negative_dir/manifest.json" \
  "$perf_dir/manifest.json" \
  "$acceptance_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
expected_statuses = [
    "PASS_OUTCOME_LABEL_BRIDGE",
    "FAIL_OUTCOME_LABEL_DECODE",
    "FAIL_MARKET_SCOPE",
    "FAIL_FORBIDDEN_SIDE_EFFECT",
    "FAIL_INSUFFICIENT_JOINED_OUTCOMES",
    "FAIL_EDGE_ONLY_LABELS",
    "FAIL_REALIZED_PERFORMANCE_QUALITY",
    "PASS_STRATEGY_PERFORMANCE_EVIDENCE",
    "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE",
]
ok = [item.get("status") for item in data] == expected_statuses
for item in data:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent", False) is False
    ok = ok and item.get("redeems_sent", False) is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and all(value is False for value in side_effects.values())
ok = ok and data[0].get("joined_outcome_count") == 120
ok = ok and data[0].get("strategy_performance_evidence_ready") is True
ok = ok and data[0].get("performance_basis") == ["dry_run_outcome"]
ok = ok and data[7].get("evidence_passed") is True
ok = ok and data[8].get("strategy_acceptance_passed") is True
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_outcome_label_bridge_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_outcome_label_bridge_gate",
  "bridge": "$bridge",
  "bridge_pass_manifest": "$pass_dir/manifest.json",
  "performance_from_bridge_manifest": "$perf_dir/manifest.json",
  "acceptance_from_bridge_manifest": "$acceptance_dir/manifest.json",
  "real_outcome_bridge_published": false,
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
