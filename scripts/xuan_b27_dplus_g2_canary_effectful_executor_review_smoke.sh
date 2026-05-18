#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_effectful_executor_review_smoke_$ts}"
fixtures="$out_dir/fixtures"
mkdir -p "$fixtures"

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

run_review_case() {
  local name="$1"
  local expected_rc="$2"
  shift 2
  local case_dir="$out_dir/$name"
  local rc=0
  scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py --output-dir "$case_dir" "$@" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$case_dir/manifest.json"
}

script="scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py"
executor_smoke_dir="$fixtures/executor_smoke"

run_check "review_script_exists" test -x "$script"
run_check "review_script_py_compile" python3 -m py_compile "$script"
run_check "review_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

run_check "executor_smoke_fixture" scripts/xuan_b27_dplus_g2_canary_executor_smoke.sh "$executor_smoke_dir"
preflight="$executor_smoke_dir/valid_preflight/manifest.json"

run_check "missing_preflight_path_refuses" run_review_case missing_preflight 64 --executor-preflight-manifest "$fixtures/missing_preflight.json"
run_check "unsafe_preflight_path_refuses" run_review_case unsafe_preflight 64 --executor-preflight-manifest "$ROOT/replay_published/not_allowed/manifest.json"

python3 - "$preflight" "$fixtures/bad_side_effect_preflight.json" "$fixtures/fixture_effectful_preflight.json" <<'PY'
import copy
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
bad_dest = pathlib.Path(sys.argv[2])
fixture_dest = pathlib.Path(sys.argv[3])
data = json.loads(source.read_text())

bad = copy.deepcopy(data)
bad["orders_sent"] = True
bad_dest.write_text(json.dumps(bad, sort_keys=True) + "\n")

fixture = copy.deepcopy(data)
fixture["status"] = "READY_EFFECTFUL_EXECUTOR_IMPLEMENTED_FIXTURE"
fixture["effectful_executor_implemented"] = True
fixture["executor_reviewed"] = True
fixture["fixture_effectful_executor_implementation"] = True
fixture_dest.write_text(json.dumps(fixture, sort_keys=True) + "\n")
PY

run_check "bad_side_effect_preflight_fails" run_review_case bad_side_effect 66 --executor-preflight-manifest "$fixtures/bad_side_effect_preflight.json"
run_check "current_refusal_preflight_blocks" run_review_case current_blocked 75 --executor-preflight-manifest "$preflight"
run_check "fixture_effectful_preflight_passes_only_with_flag" run_review_case fixture_pass 0 --allow-fixture-pass --executor-preflight-manifest "$fixtures/fixture_effectful_preflight.json"
run_check "fixture_effectful_preflight_without_flag_fails" run_review_case fixture_without_flag 66 --executor-preflight-manifest "$fixtures/fixture_effectful_preflight.json"

run_check "review_manifests_contract" python3 - "$out_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
blocked = json.loads((root / "current_blocked" / "manifest.json").read_text())
passed = json.loads((root / "fixture_pass" / "manifest.json").read_text())
bad = json.loads((root / "bad_side_effect" / "manifest.json").read_text())

def side_effects_false(data):
    effects = data.get("side_effects") or {}
    return effects and all(value is False for value in effects.values())

ok = (
    blocked.get("artifact") == "xuan_b27_dplus_g2_canary_effectful_executor_review"
    and blocked.get("status") == "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and blocked.get("scope") == "local_no_network_g2_canary_effectful_executor_review"
    and blocked.get("review_passed") is False
    and blocked.get("effectful_executor_implemented") is False
    and blocked.get("reviewed_effectful_executor_implementation") is False
    and blocked.get("executor_preflight_status") == "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and blocked.get("effectful_phase_plan_coverage", {}).get("complete") is True
    and blocked.get("review_input_side_effects_all_false") is True
    and blocked.get("canary_run_authorized") is False
    and blocked.get("orders_sent") is False
    and blocked.get("auth_network_started") is False
    and blocked.get("started_canary") is False
    and side_effects_false(blocked)
    and passed.get("status") == "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION"
    and passed.get("review_passed") is True
    and passed.get("effectful_executor_implemented") is True
    and passed.get("reviewed_effectful_executor_implementation") is True
    and passed.get("fixture_review") is True
    and passed.get("orders_sent") is False
    and passed.get("auth_network_started") is False
    and side_effects_false(passed)
    and bad.get("status") == "FAIL_EFFECTFUL_EXECUTOR_REVIEW_INPUTS"
    and "orders_sent_not_false" in (bad.get("failures") or [])
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<EOF
{
  "schema_version": 1,
  "artifact": "xuan_b27_dplus_g2_canary_effectful_executor_review_smoke",
  "status": "$status",
  "created_utc": "$ts",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_effectful_executor_review_gate",
  "reviewer": "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py",
  "real_effectful_executor_review_status": "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$out_dir/current_blocked/manifest.json" 2>/dev/null || true)",
  "real_effectful_executor_implemented": false,
  "real_review_passed": false,
  "fixture_pass_artifact_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log"
}
EOF

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
