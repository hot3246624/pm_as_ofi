#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_performance_evidence_smoke_$ts}"
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

script="scripts/xuan_b27_dplus_shadow_performance_evidence.py"

run_check "performance_script_exists" test -x "$script"
run_check "performance_script_py_compile" python3 -m py_compile "$script"
run_check "performance_script_has_no_network_or_process_imports" \
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
    "status": "PASS_NO_ORDER_OBSERVER",
    "markets": ["btc-updown-5m-1778893200"],
    "observer_candidate_counts": {
        "would_track": 1000,
        "would_place": 0,
        "submitted_previews": 0,
        "trace_previews": 1000,
    },
    "decode_error_count": 0,
    "truncated_final_line_count": 0,
    "no_order_violation_count": 0,
    "run_manifest": {
        "dry_run": True,
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
    },
}
bad_summary = copy.deepcopy(summary)
bad_summary["observer_candidate_counts"]["would_place"] = 1
bad_summary["status"] = "FAIL_NO_ORDER_VIOLATION"
(root / "observer_pass.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
(root / "observer_unsafe.json").write_text(json.dumps(bad_summary, indent=2, sort_keys=True) + "\n")

with (root / "edge_pass.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "candidate_id": f"c{i}",
            "accepted": True,
            "would_track": True,
            "performance_basis": "shadow_replay_outcome",
            "edge_bps": 1.2 + (i % 5) * 0.1,
            "expected_value_bps": 0.8 + (i % 7) * 0.1,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
        }) + "\n")

with (root / "edge_low_sample.jsonl").open("w") as fh:
    for i in range(5):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "accepted": True,
            "performance_basis": "shadow_replay_outcome",
            "edge_bps": 1.0,
            "expected_value_bps": 1.0,
        }) + "\n")

with (root / "edge_negative.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "accepted": True,
            "performance_basis": "shadow_replay_outcome",
            "edge_bps": -0.2,
            "expected_value_bps": -0.1,
        }) + "\n")

with (root / "edge_side_effect.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "btc-updown-5m-1778893200",
            "accepted": True,
            "performance_basis": "shadow_replay_outcome",
            "edge_bps": 1.0,
            "expected_value_bps": 1.0,
            "orders_sent": i == 0,
        }) + "\n")

with (root / "edge_wrong_market.jsonl").open("w") as fh:
    for i in range(120):
        fh.write(json.dumps({
            "market_slug": "eth-updown-5m-1778893200",
            "accepted": True,
            "performance_basis": "shadow_replay_outcome",
            "edge_bps": 1.0,
            "expected_value_bps": 1.0,
        }) + "\n")

(root / "edge_only_not_performance.jsonl").write_text("".join(
    json.dumps({
        "market_slug": "btc-updown-5m-1778893200",
        "accepted": True,
        "performance_basis": "observer_edge_only_not_realized",
        "edge_bps": 1.0,
        "expected_value_bps": 1.0,
    }) + "\n"
    for _ in range(120)
))

(root / "edge_decode_error.jsonl").write_text("{not json}\n")
PY

pass_dir="$out_dir/fixture_pass_evidence"
unsafe_dir="$out_dir/fixture_unsafe_summary"
decode_dir="$out_dir/fixture_decode_error"
low_sample_dir="$out_dir/fixture_low_sample"
negative_dir="$out_dir/fixture_negative_edge"
side_effect_dir="$out_dir/fixture_side_effect"
wrong_market_dir="$out_dir/fixture_wrong_market"
edge_only_dir="$out_dir/fixture_edge_only"

run_expect_rc "pass_fixture" 0 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_pass.jsonl" \
  --output-dir "$pass_dir" \
  --min-samples 100
run_expect_rc "unsafe_summary_fixture" 2 "$script" \
  --observer-summary "$fixture_dir/observer_unsafe.json" \
  --edge-samples "$fixture_dir/edge_pass.jsonl" \
  --output-dir "$unsafe_dir"
run_expect_rc "decode_error_fixture" 3 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_decode_error.jsonl" \
  --output-dir "$decode_dir"
run_expect_rc "low_sample_fixture" 4 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_low_sample.jsonl" \
  --output-dir "$low_sample_dir"
run_expect_rc "negative_edge_fixture" 5 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_negative.jsonl" \
  --output-dir "$negative_dir"
run_expect_rc "side_effect_fixture" 6 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_side_effect.jsonl" \
  --output-dir "$side_effect_dir"
run_expect_rc "wrong_market_fixture" 7 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_wrong_market.jsonl" \
  --output-dir "$wrong_market_dir"
run_expect_rc "edge_only_fixture" 8 "$script" \
  --observer-summary "$fixture_dir/observer_pass.json" \
  --edge-samples "$fixture_dir/edge_only_not_performance.jsonl" \
  --output-dir "$edge_only_dir"

run_check "fixture_manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$unsafe_dir/manifest.json" \
  "$decode_dir/manifest.json" \
  "$low_sample_dir/manifest.json" \
  "$negative_dir/manifest.json" \
  "$side_effect_dir/manifest.json" \
  "$wrong_market_dir/manifest.json" \
  "$edge_only_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
statuses = [item.get("status") for item in data]
expected = [
    "PASS_STRATEGY_PERFORMANCE_EVIDENCE",
    "FAIL_OBSERVER_SUMMARY_NOT_SAFE",
    "FAIL_PERFORMANCE_EVIDENCE_DECODE_ERROR",
    "FAIL_INSUFFICIENT_SAMPLE_SIZE",
    "FAIL_EDGE_QUALITY",
    "FAIL_FORBIDDEN_SIDE_EFFECT",
    "FAIL_MARKET_SCOPE",
    "FAIL_EDGE_ONLY_NOT_PERFORMANCE",
]
ok = statuses == expected
for item in data:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("artifact") == "xuan_b27_dplus_shadow_strategy_performance_evidence"
    ok = ok and item.get("scope") == "local_no_order_shadow_or_replay_performance"
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent") is False
    ok = ok and item.get("redeems_sent") is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and all(value is False for value in side_effects.values())
ok = ok and data[0].get("evidence_passed") is True
ok = ok and data[0].get("candidate_sample_count") == 120
ok = ok and data[0].get("positive_edge_candidate_count") == 120
ok = ok and data[1].get("evidence_passed") is False
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shadow_performance_evidence_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shadow_performance_evidence_gate",
  "performance_script": "$script",
  "fixture_pass_manifest": "$pass_dir/manifest.json",
  "real_performance_evidence_published": false,
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
