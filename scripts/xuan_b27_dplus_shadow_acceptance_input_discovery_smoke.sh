#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_acceptance_input_discovery_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
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

script="scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py"
shadow_harness_smoke="scripts/xuan_b27_dplus_no_order_shadow_run_artifact_smoke.sh"

run_check "discovery_script_exists" test -x "$script"
run_check "discovery_script_py_compile" python3 -m py_compile "$script"
run_check "discovery_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

shadow_smoke_dir="$out_dir/no_order_shadow_run_artifact_smoke"
run_check "shadow_harness_fixture_ready" "$shadow_harness_smoke" "$shadow_smoke_dir"
pass_run_dir="$(python3 - "$shadow_smoke_dir/manifest.json" <<'PY'
import json
import pathlib
import sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
print(pathlib.Path(manifest["producer_pass_manifest"]).parent)
PY
)"

actual_discovery_dir="$out_dir/actual_discovery"
fixture_ignored_dir="$out_dir/fixture_ignored"
valid_scan_root="$out_dir/valid_scan"
invalid_scan_root="$out_dir/invalid_side_effect_scan"
edge_only_scan_root="$out_dir/edge_only_scan"
unsafe_scan_root="$out_dir/raw"

run_check "actual_workspace_discovery_classifies_current_inputs" "$script" \
  --output-dir "$actual_discovery_dir"
run_check "fixture_inputs_ignored_without_fixture_flag" "$script" \
  --scan-root "$shadow_smoke_dir" \
  --output-dir "$fixture_ignored_dir" \
  --min-outcome-events 100

run_check "fixture_scan_roots_created" python3 - \
  "$pass_run_dir" "$valid_scan_root" "$invalid_scan_root" "$edge_only_scan_root" <<'PY'
import json
import pathlib
import shutil
import sys

pass_run = pathlib.Path(sys.argv[1])
valid_root = pathlib.Path(sys.argv[2])
invalid_root = pathlib.Path(sys.argv[3])
edge_root = pathlib.Path(sys.argv[4])

for target in (valid_root, invalid_root, edge_root):
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    shutil.copy2(pass_run / "manifest.json", target / "manifest.json")
    shutil.copy2(pass_run / "outcome_events.jsonl", target / "outcome_events.jsonl")
    manifest = json.loads((target / "manifest.json").read_text())
    manifest["outcome_events"] = str(target / "outcome_events.jsonl")
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

invalid_events = []
for line in (invalid_root / "outcome_events.jsonl").read_text().splitlines():
    event = json.loads(line)
    event["orders_sent"] = True
    invalid_events.append(json.dumps(event, sort_keys=True))
(invalid_root / "outcome_events.jsonl").write_text("\n".join(invalid_events) + "\n")

edge_events = []
for line in (edge_root / "outcome_events.jsonl").read_text().splitlines():
    event = json.loads(line)
    event["performance_basis"] = "observer_edge_only_not_realized"
    edge_events.append(json.dumps(event, sort_keys=True))
(edge_root / "outcome_events.jsonl").write_text("\n".join(edge_events) + "\n")
PY

valid_fixture_discovery="$out_dir/valid_fixture_discovery"
invalid_discovery="$out_dir/invalid_discovery"
edge_only_discovery="$out_dir/edge_only_discovery"
unsafe_discovery="$out_dir/unsafe_discovery"

run_check "valid_fixture_root_reports_ready" "$script" \
  --scan-root "$valid_scan_root" \
  --allow-fixture-root \
  --output-dir "$valid_fixture_discovery" \
  --min-outcome-events 100
run_check "side_effect_fixture_reports_invalid" "$script" \
  --scan-root "$invalid_scan_root" \
  --allow-fixture-root \
  --output-dir "$invalid_discovery" \
  --min-outcome-events 100
run_check "edge_only_fixture_reports_invalid" "$script" \
  --scan-root "$edge_only_scan_root" \
  --allow-fixture-root \
  --output-dir "$edge_only_discovery" \
  --min-outcome-events 100
mkdir -p "$unsafe_scan_root"
run_expect_rc "raw_scan_root_refused" 2 "$script" \
  --scan-root "$unsafe_scan_root" \
  --output-dir "$unsafe_discovery"

run_check "discovery_manifests_safe" python3 - \
  "$actual_discovery_dir/manifest.json" \
  "$fixture_ignored_dir/manifest.json" \
  "$valid_fixture_discovery/manifest.json" \
  "$invalid_discovery/manifest.json" \
  "$edge_only_discovery/manifest.json" \
  "$unsafe_discovery/manifest.json" <<'PY'
import json
import pathlib
import sys

actual, ignored, valid, invalid, edge_only, unsafe = [
    json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:]
]
items = [actual, ignored, valid, invalid, edge_only, unsafe]
ok = (
    actual.get("artifact") == "xuan_b27_dplus_shadow_acceptance_input_discovery"
    and actual.get("status") in {
        "BLOCKED_NO_REAL_SHADOW_ACCEPTANCE_INPUTS",
        "BLOCKED_SHADOW_ACCEPTANCE_INPUTS_INVALID",
        "READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER",
    }
    and actual.get("allow_fixture_root") is False
    and ignored.get("status") == "BLOCKED_NO_REAL_SHADOW_ACCEPTANCE_INPUTS"
    and ignored.get("fixture_or_smoke_candidate_count", 0) > 0
    and valid.get("status") == "READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER"
    and valid.get("valid_real_candidate_count") == 1
    and valid.get("allow_fixture_root") is True
    and invalid.get("status") == "BLOCKED_SHADOW_ACCEPTANCE_INPUTS_INVALID"
    and invalid.get("invalid_real_candidate_count") == 1
    and "forbidden_side_effect_outcome_events" in (invalid.get("candidates") or [{}])[0].get("failures", [])
    and edge_only.get("status") == "BLOCKED_SHADOW_ACCEPTANCE_INPUTS_INVALID"
    and edge_only.get("invalid_real_candidate_count") == 1
    and "edge_only_or_unknown_outcome_basis" in (edge_only.get("candidates") or [{}])[0].get("failures", [])
    and unsafe.get("status") == "FAIL_UNSAFE_SCAN_ROOT"
    and unsafe.get("scan_root_safe") is False
)
for item in items:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent") is False
    ok = ok and item.get("redeems_sent") is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_observer") is False
    ok = ok and item.get("started_user_ws") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and item.get("real_shadow_acceptance_input_published") is False
    ok = ok and all(value is False for value in side_effects.values())
raise SystemExit(0 if ok else 1)
PY

actual_status="$(python3 - "$actual_discovery_dir/manifest.json" <<'PY'
import json
import pathlib
import sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text()).get("status"))
PY
)"

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shadow_acceptance_input_discovery_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shadow_acceptance_input_discovery_gate",
  "discovery_script": "$script",
  "actual_discovery_manifest": "$actual_discovery_dir/manifest.json",
  "actual_discovery_status": "$actual_status",
  "valid_fixture_discovery_manifest": "$valid_fixture_discovery/manifest.json",
  "invalid_fixture_discovery_manifest": "$invalid_discovery/manifest.json",
  "edge_only_fixture_discovery_manifest": "$edge_only_discovery/manifest.json",
  "real_shadow_acceptance_input_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_observer": false,
  "started_user_ws": false,
  "started_canary": false,
  "side_effects": {
    "network_started": false,
    "ssh_started": false,
    "scanned_raw_or_replay": false,
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

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
