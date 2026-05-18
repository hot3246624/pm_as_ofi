#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke_$ts}"
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

runner="scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py"
shadow_harness_smoke="scripts/xuan_b27_dplus_no_order_shadow_run_artifact_smoke.sh"

run_check "runner_script_exists" test -x "$runner"
run_check "runner_script_py_compile" python3 -m py_compile "$runner"
run_check "runner_script_has_no_network_or_process_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$runner'"

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

fixture_runner_dir="$out_dir/fixture_runner"
publish_refusal_dir="$out_dir/publish_refusal"
unsafe_dir="/tmp/xuan_b27_dplus_runner_unsafe_$ts"
invalid_dir="$out_dir/invalid_manifest"
bridge_fail_dir="$out_dir/bridge_fail_run"
bridge_fail_runner_dir="$out_dir/bridge_fail_runner"

run_expect_rc "fixture_chain_passes_without_publishing_real_acceptance" 0 "$runner" \
  --run-dir "$pass_run_dir" \
  --allow-fixture \
  --output-dir "$fixture_runner_dir"
run_expect_rc "fixture_publish_refused" 4 "$runner" \
  --run-dir "$pass_run_dir" \
  --allow-fixture \
  --publish-real-acceptance \
  --output-dir "$publish_refusal_dir"
run_expect_rc "unsafe_input_path_refused" 2 "$runner" \
  --run-dir "$unsafe_dir" \
  --allow-fixture \
  --output-dir "$out_dir/unsafe_runner"

run_check "failure_fixtures_created" python3 - "$pass_run_dir" "$invalid_dir" "$bridge_fail_dir" <<'PY'
import json
import pathlib
import shutil
import sys

pass_run = pathlib.Path(sys.argv[1])
invalid_dir = pathlib.Path(sys.argv[2])
bridge_fail_dir = pathlib.Path(sys.argv[3])
invalid_dir.mkdir(parents=True, exist_ok=True)
bridge_fail_dir.mkdir(parents=True, exist_ok=True)

manifest = json.loads((pass_run / "manifest.json").read_text())
bad = dict(manifest)
bad["orders_allowed"] = True
bad["status"] = "FAIL_NO_ORDER_SHADOW_DRY_RUN"
(invalid_dir / "manifest.json").write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")

bridge_bad = dict(manifest)
bridge_bad["edge_samples"] = str(bridge_fail_dir / "missing_edge_samples.jsonl")
(bridge_fail_dir / "manifest.json").write_text(json.dumps(bridge_bad, indent=2, sort_keys=True) + "\n")
shutil.copy2(pass_run / "outcome_events.jsonl", bridge_fail_dir / "outcome_events.jsonl")
PY

run_expect_rc "invalid_manifest_refused" 3 "$runner" \
  --run-dir "$invalid_dir" \
  --allow-fixture \
  --output-dir "$out_dir/invalid_runner"
run_expect_rc "bridge_gate_failure_surfaces" 6 "$runner" \
  --run-dir "$bridge_fail_dir" \
  --allow-fixture \
  --output-dir "$bridge_fail_runner_dir"

run_check "runner_manifests_safe" python3 - \
  "$fixture_runner_dir/manifest.json" \
  "$publish_refusal_dir/manifest.json" \
  "$out_dir/unsafe_runner/manifest.json" \
  "$out_dir/invalid_runner/manifest.json" \
  "$bridge_fail_runner_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
expected = [
    "PASS_RUNNER_FIXTURE_CHAIN",
    "FAIL_REFUSED_FIXTURE_PUBLISH",
    "FAIL_UNSAFE_INPUT_PATH",
    "FAIL_RUN_MANIFEST_NOT_SAFE",
    "FAIL_OUTCOME_BRIDGE_GATE",
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
ok = ok and data[0].get("strategy_acceptance_status") == "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE"
ok = ok and data[0].get("strategy_acceptance_passed") is True
ok = ok and data[0].get("real_acceptance_artifact_published") is False
ok = ok and data[0].get("acceptance_artifact_published") is False
ok = ok and data[1].get("real_acceptance_artifact_published") is False
ok = ok and data[1].get("publish_real_acceptance_requested") is True
ok = ok and data[1].get("fixture_or_smoke_input") is True
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_rust_shadow_strategy_acceptance_runner_gate",
  "runner": "$runner",
  "fixture_runner_manifest": "$fixture_runner_dir/manifest.json",
  "fixture_publish_refusal_manifest": "$publish_refusal_dir/manifest.json",
  "real_acceptance_artifact_published": false,
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
