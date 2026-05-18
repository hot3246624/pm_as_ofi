#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_no_order_shadow_run_artifact_smoke_$ts}"
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

producer="scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py"
label_producer="scripts/xuan_b27_dplus_realized_outcome_labels.py"
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

base_summary = {
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

def make_case(
    name,
    *,
    summary=None,
    label_count=120,
    label_basis="dry_run_outcome",
    label_market="btc-updown-5m-1778893200",
    side_effect=False,
    duplicate=False,
    negative=False,
    decode_labels=False,
):
    case = root / name
    case.mkdir(parents=True, exist_ok=True)
    (case / "observer_summary.json").write_text(
        json.dumps(summary or base_summary, indent=2, sort_keys=True) + "\n"
    )
    with (case / "edge_samples.jsonl").open("w") as fh:
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
    if decode_labels:
        (case / "outcome_labels.jsonl").write_text("{not-json}\n")
        return
    with (case / "outcome_labels.jsonl").open("w") as fh:
        for i in range(label_count):
            key_i = 0 if duplicate and i == 1 else i
            fh.write(json.dumps({
                "market_slug": label_market,
                "candidate_id": f"candidate-{key_i}",
                "order_attempt_id": f"attempt-{key_i}",
                "outcome_label_id": f"outcome-{i}",
                "outcome_label_source": "local_no_order_shadow_fixture",
                "performance_basis": label_basis,
                "realized_edge_bps": -0.5 if negative else 0.9 + (i % 5) * 0.1,
                "orders_sent": side_effect and i == 0,
                "cancels_sent": False,
                "redeems_sent": False,
                "auth_network_started": False,
                "started_canary": False,
            }) + "\n")

make_case("pass")
bad_summary = copy.deepcopy(base_summary)
bad_summary["run_manifest"]["orders_allowed"] = True
make_case("unsafe_observer", summary=bad_summary)
make_case("decode", decode_labels=True)
make_case("wrong_market", label_market="eth-updown-5m-1778893200")
make_case("side_effect", side_effect=True)
make_case("edge_only", label_basis="observer_edge_only_not_realized")
make_case("duplicate", duplicate=True)
make_case("low", label_count=5)
make_case("negative", negative=True)
PY

pass_dir="$out_dir/pass_run_artifact"
unsafe_observer_dir="$out_dir/unsafe_observer"
decode_dir="$out_dir/decode"
wrong_market_dir="$out_dir/wrong_market"
side_effect_dir="$out_dir/side_effect"
edge_only_dir="$out_dir/edge_only"
duplicate_dir="$out_dir/duplicate"
low_dir="$out_dir/low"
negative_dir="$out_dir/negative"
labels_dir="$out_dir/realized_labels_from_run"
bridge_dir="$out_dir/bridge_from_run_labels"
perf_dir="$out_dir/performance_from_run"
acceptance_dir="$out_dir/acceptance_from_run"

run_expect_rc "pass_run_artifact" 0 "$producer" \
  --observer-summary "$fixture_dir/pass/observer_summary.json" \
  --edge-samples "$fixture_dir/pass/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/pass/outcome_labels.jsonl" \
  --output-dir "$pass_dir"
run_expect_rc "unsafe_observer_fixture" 3 "$producer" \
  --observer-summary "$fixture_dir/unsafe_observer/observer_summary.json" \
  --edge-samples "$fixture_dir/unsafe_observer/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/unsafe_observer/outcome_labels.jsonl" \
  --output-dir "$unsafe_observer_dir"
run_expect_rc "decode_fixture" 4 "$producer" \
  --observer-summary "$fixture_dir/decode/observer_summary.json" \
  --edge-samples "$fixture_dir/decode/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/decode/outcome_labels.jsonl" \
  --output-dir "$decode_dir"
run_expect_rc "wrong_market_fixture" 5 "$producer" \
  --observer-summary "$fixture_dir/wrong_market/observer_summary.json" \
  --edge-samples "$fixture_dir/wrong_market/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/wrong_market/outcome_labels.jsonl" \
  --output-dir "$wrong_market_dir"
run_expect_rc "side_effect_fixture" 6 "$producer" \
  --observer-summary "$fixture_dir/side_effect/observer_summary.json" \
  --edge-samples "$fixture_dir/side_effect/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/side_effect/outcome_labels.jsonl" \
  --output-dir "$side_effect_dir"
run_expect_rc "edge_only_fixture" 7 "$producer" \
  --observer-summary "$fixture_dir/edge_only/observer_summary.json" \
  --edge-samples "$fixture_dir/edge_only/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/edge_only/outcome_labels.jsonl" \
  --output-dir "$edge_only_dir"
run_expect_rc "duplicate_fixture" 8 "$producer" \
  --observer-summary "$fixture_dir/duplicate/observer_summary.json" \
  --edge-samples "$fixture_dir/duplicate/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/duplicate/outcome_labels.jsonl" \
  --output-dir "$duplicate_dir"
run_expect_rc "low_fixture" 9 "$producer" \
  --observer-summary "$fixture_dir/low/observer_summary.json" \
  --edge-samples "$fixture_dir/low/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/low/outcome_labels.jsonl" \
  --output-dir "$low_dir"
run_expect_rc "negative_fixture" 10 "$producer" \
  --observer-summary "$fixture_dir/negative/observer_summary.json" \
  --edge-samples "$fixture_dir/negative/edge_samples.jsonl" \
  --outcome-labels "$fixture_dir/negative/outcome_labels.jsonl" \
  --output-dir "$negative_dir"

run_expect_rc "realized_labels_accept_run_artifact" 0 "$label_producer" \
  --run-dir "$pass_dir" \
  --output-dir "$labels_dir"
run_expect_rc "bridge_accepts_run_artifact_labels" 0 "$bridge" \
  --edge-samples "$fixture_dir/pass/edge_samples.jsonl" \
  --outcome-labels "$labels_dir/outcome_labels.jsonl" \
  --output-dir "$bridge_dir"
run_expect_rc "performance_accepts_run_artifact_labels" 0 "$performance_script" \
  --observer-summary "$fixture_dir/pass/observer_summary.json" \
  --edge-samples "$bridge_dir/realized_edge_samples.jsonl" \
  --output-dir "$perf_dir"
run_expect_rc "acceptance_accepts_run_artifact_performance" 0 "$acceptance_script" \
  --observer-summary "$fixture_dir/pass/observer_summary.json" \
  --performance-evidence "$perf_dir/manifest.json" \
  --output-dir "$acceptance_dir"

run_check "producer_manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$unsafe_observer_dir/manifest.json" \
  "$decode_dir/manifest.json" \
  "$wrong_market_dir/manifest.json" \
  "$side_effect_dir/manifest.json" \
  "$edge_only_dir/manifest.json" \
  "$duplicate_dir/manifest.json" \
  "$low_dir/manifest.json" \
  "$negative_dir/manifest.json" \
  "$labels_dir/manifest.json" \
  "$bridge_dir/manifest.json" \
  "$perf_dir/manifest.json" \
  "$acceptance_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
expected = [
    "PASS_NO_ORDER_SHADOW_DRY_RUN",
    "FAIL_OBSERVER_SUMMARY_NOT_SAFE",
    "FAIL_INPUT_DECODE",
    "FAIL_MARKET_SCOPE",
    "FAIL_FORBIDDEN_SIDE_EFFECT",
    "FAIL_EDGE_ONLY_OR_UNKNOWN_BASIS",
    "FAIL_DUPLICATE_LABEL_KEYS",
    "FAIL_INSUFFICIENT_JOINED_OUTCOMES",
    "FAIL_REALIZED_OUTCOME_QUALITY",
    "PASS_REALIZED_OUTCOME_LABELS",
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
ok = ok and data[0].get("artifact") == "xuan_b27_dplus_no_order_shadow_run_artifact"
ok = ok and data[0].get("scope") == "local_no_order_shadow_or_dry_run"
ok = ok and data[0].get("no_order") is True
ok = ok and data[0].get("orders_allowed") is False
ok = ok and data[0].get("cancels_allowed") is False
ok = ok and data[0].get("redeems_allowed") is False
ok = ok and data[0].get("outcome_event_count") == 120
ok = ok and data[0].get("realized_outcome_label_run_ready") is True
ok = ok and data[9].get("outcome_labels_ready") is True
ok = ok and data[10].get("strategy_performance_evidence_ready") is True
ok = ok and data[11].get("evidence_passed") is True
ok = ok and data[12].get("strategy_acceptance_passed") is True
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_no_order_shadow_run_artifact_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_no_order_shadow_run_artifact_gate",
  "producer": "$producer",
  "producer_pass_manifest": "$pass_dir/manifest.json",
  "realized_labels_manifest": "$labels_dir/manifest.json",
  "bridge_from_run_manifest": "$bridge_dir/manifest.json",
  "performance_from_run_manifest": "$perf_dir/manifest.json",
  "acceptance_from_run_manifest": "$acceptance_dir/manifest.json",
  "real_shadow_run_artifact_published": false,
  "acceptance_artifact_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "side_effects": {
    "network_started": false,
    "ssh_started": false,
    "started_observer": false,
    "started_user_ws": false,
    "started_canary": false,
    "orders_sent": false,
    "cancels_sent": false,
    "redeems_sent": false,
    "broker_modified": false
  },
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
