#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke_$ts}"
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

script="scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"

run_check "acceptance_script_exists" test -x "$script"
run_check "acceptance_script_py_compile" python3 -m py_compile "$script"
run_check "acceptance_script_has_no_network_or_process_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

run_check "fixtures_created" python3 - "$fixture_dir" <<'PY'
import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)

summary = {
    "artifact": "xuan_b27_dplus_observer_summary",
    "run_dir": "/tmp/xuan_b27_dplus_shadow_fixture",
    "status": "PASS_NO_ORDER_OBSERVER",
    "jsonl_file_count": 12,
    "event_counts": {"xuan_b27_dplus_observer_tick": 120000},
    "markets": ["btc-updown-5m-1778893200"],
    "observer_candidate_counts": {
        "would_track": 90000,
        "would_place": 0,
        "trace_previews": 100000,
        "submitted_previews": 0,
    },
    "source_truth_verdict_counts": {"UNKNOWN": 100000},
    "no_order_violation_count": 0,
    "decode_error_count": 0,
    "truncated_final_line_count": 0,
    "run_manifest": {
        "artifact": "xuan_b27_dplus_auth_observer_run",
        "strategy": "xuan_b27_dplus",
        "dry_run": True,
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
        "elapsed_seconds": 1800.5,
    },
}
performance = {
    "artifact": "xuan_b27_dplus_shadow_strategy_performance_evidence",
    "status": "PASS_STRATEGY_PERFORMANCE_EVIDENCE",
    "strategy": "xuan_b27_dplus",
    "scope": "local_no_order_shadow_or_replay_performance",
    "evidence_passed": True,
    "candidate_sample_count": 1000,
    "accepted_candidate_count": 900,
    "positive_edge_candidate_count": 850,
    "expected_value_bps": 1.7,
    "median_candidate_edge_bps": 1.2,
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
    },
}

def write(name, value):
    (root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

write("observer_pass.json", summary)
write("performance_pass.json", performance)

no_order = copy.deepcopy(summary)
no_order["status"] = "FAIL_NO_ORDER_VIOLATION"
no_order["observer_candidate_counts"]["submitted_previews"] = 1
no_order["no_order_violation_count"] = 1
write("observer_no_order_violation.json", no_order)

no_candidates = copy.deepcopy(summary)
no_candidates["observer_candidate_counts"]["would_track"] = 0
no_candidates["observer_candidate_counts"]["trace_previews"] = 100000
write("observer_no_candidates.json", no_candidates)

no_unknown = copy.deepcopy(summary)
no_unknown["source_truth_verdict_counts"] = {"PASS": 100000}
write("observer_no_unknown.json", no_unknown)

decode_error = copy.deepcopy(summary)
decode_error["decode_error_count"] = 1
write("observer_decode_error.json", decode_error)

bad_perf = copy.deepcopy(performance)
bad_perf["status"] = "FAIL_STRATEGY_PERFORMANCE_EVIDENCE"
bad_perf["evidence_passed"] = False
bad_perf["positive_edge_candidate_count"] = 0
write("performance_fail.json", bad_perf)
PY

pass_dir="$out_dir/fixture_pass_acceptance"
missing_perf_dir="$out_dir/fixture_missing_performance"
no_order_dir="$out_dir/fixture_no_order_violation"
no_candidates_dir="$out_dir/fixture_no_candidates"
no_unknown_dir="$out_dir/fixture_no_unknown"
decode_error_dir="$out_dir/fixture_decode_error"
bad_perf_dir="$out_dir/fixture_bad_performance"

run_expect_rc "pass_fixture" 0 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --performance-evidence "$fixture_dir/performance_pass.json" \
  --output-dir "$pass_dir" \
  --min-observer-ticks 100000 \
  --min-performance-samples 100
run_expect_rc "missing_performance_fixture" 3 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --output-dir "$missing_perf_dir"
run_expect_rc "no_order_violation_fixture" 2 "$script" \
  --observer-summary "$fixture_dir/observer_no_order_violation.json" \
  --performance-evidence "$fixture_dir/performance_pass.json" \
  --output-dir "$no_order_dir"
run_expect_rc "no_candidates_fixture" 4 "$script" \
  --observer-summary "$fixture_dir/observer_no_candidates.json" \
  --performance-evidence "$fixture_dir/performance_pass.json" \
  --output-dir "$no_candidates_dir"
run_expect_rc "no_unknown_fixture" 5 "$script" \
  --observer-summary "$fixture_dir/observer_no_unknown.json" \
  --performance-evidence "$fixture_dir/performance_pass.json" \
  --output-dir "$no_unknown_dir"
run_expect_rc "decode_error_fixture" 6 "$script" \
  --observer-summary "$fixture_dir/observer_decode_error.json" \
  --performance-evidence "$fixture_dir/performance_pass.json" \
  --output-dir "$decode_error_dir"
run_expect_rc "bad_performance_fixture" 7 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --performance-evidence "$fixture_dir/performance_fail.json" \
  --output-dir "$bad_perf_dir"

run_check "fixture_manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$missing_perf_dir/manifest.json" \
  "$no_order_dir/manifest.json" \
  "$no_candidates_dir/manifest.json" \
  "$no_unknown_dir/manifest.json" \
  "$decode_error_dir/manifest.json" \
  "$bad_perf_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
statuses = [item.get("status") for item in data]
expected = [
    "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE",
    "FAIL_MISSING_STRATEGY_PERFORMANCE_EVIDENCE",
    "FAIL_NO_ORDER_SAFETY",
    "FAIL_CANDIDATE_QUALITY",
    "FAIL_SOURCE_TRUTH_UNKNOWN_HANDLING",
    "FAIL_RECORDER_COMPLETENESS",
    "FAIL_STRATEGY_PERFORMANCE_EVIDENCE",
]
ok = statuses == expected
for item in data:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("artifact") == "xuan_b27_dplus_rust_shadow_strategy_acceptance"
    ok = ok and item.get("scope") == "rust_no_order_shadow_or_dry_run_strategy_acceptance"
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent") is False
    ok = ok and item.get("redeems_sent") is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and all(value is False for value in side_effects.values())
ok = ok and data[0].get("strategy_acceptance_passed") is True
ok = ok and data[0].get("acceptance_checks", {}).get("performance_evidence_ok") is True
ok = ok and data[1].get("strategy_acceptance_passed") is False
ok = ok and data[1].get("acceptance_checks", {}).get("performance_evidence_present") is False
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_rust_shadow_strategy_acceptance_gate",
  "acceptance_script": "$script",
  "fixture_pass_manifest": "$pass_dir/manifest.json",
  "acceptance_artifact_published": false,
  "orders_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
