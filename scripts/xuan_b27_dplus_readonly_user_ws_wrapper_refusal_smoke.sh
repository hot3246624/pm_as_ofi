#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke_$ts}"
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

run_wrapper_case() {
  local name="$1"
  local expected_rc="$2"
  local expected_status="$3"
  shift 3
  local case_dir="$out_dir/$name"
  mkdir -p "$case_dir"
  local rc=0
  scripts/xuan_b27_dplus_readonly_user_ws_observer.py \
    --duration-seconds 1 \
    --shared-ingress-root "$case_dir/missing-shared-ingress" \
    --output-dir "$case_dir/run" \
    "$@" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  python3 - "$case_dir/run/wrapper_manifest.json" "$expected_status" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_status = sys.argv[2]
data = json.loads(path.read_text())
ok = (
    data.get("artifact") == "xuan_b27_dplus_readonly_user_ws_observer_wrapper"
    and data.get("status") == expected_status
    and data.get("orders_allowed") is False
)
if ok and expected_status == "SHARED_INGRESS_BROKER_UNAVAILABLE":
    broker_preflight = pathlib.Path(data.get("broker_preflight_manifest") or "")
    dotenv_loaded = data.get("dotenv_loaded") or {}
    ok = (
        broker_preflight.exists()
        and data.get("broker_preflight_status") == "UNAVAILABLE"
        and data.get("broker_preflight_return_code") == 69
        and "dotenv_loaded_keys" not in data
        and "safe_env" not in data
        and dotenv_loaded.get("key_names_recorded") is False
        and dotenv_loaded.get("secret_values_recorded") is False
    )
raise SystemExit(0 if ok else 1)
PY
  test ! -f "$case_dir/run/run_manifest.json"
  test ! -f "$case_dir/run/logs/cargo_build_xuan_b27_dplus_user_ws_observer.log"
}

run_secret_redaction_case() {
  local name="missing_broker_secret_redaction"
  local case_dir="$out_dir/$name"
  mkdir -p "$case_dir"
  local api_key="xuan_fixture_api_key_do_not_leak"
  local api_secret="xuan_fixture_api_secret_do_not_leak"
  local api_passphrase="xuan_fixture_api_passphrase_do_not_leak"
  local private_key="xuan_fixture_private_key_do_not_leak"
  local token="xuan_fixture_token_do_not_leak"
  local rc=0
  env \
    POLYMARKET_API_KEY="$api_key" \
    POLYMARKET_API_SECRET="$api_secret" \
    POLYMARKET_API_PASSPHRASE="$api_passphrase" \
    POLYMARKET_PRIVATE_KEY="$private_key" \
    PM_TEST_SECRET_TOKEN="$token" \
    scripts/xuan_b27_dplus_readonly_user_ws_observer.py \
      --approved-readonly-user-ws \
      --duration-seconds 1 \
      --shared-ingress-root "$case_dir/missing-shared-ingress" \
      --output-dir "$case_dir/run" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "69" ]]; then
    printf 'unexpected rc for %s: got %s want 69\n' "$name" "$rc" >> "$log"
    return 1
  fi
  python3 - "$case_dir/run/wrapper_manifest.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
dotenv_loaded = data.get("dotenv_loaded") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_readonly_user_ws_observer_wrapper"
    and data.get("status") == "SHARED_INGRESS_BROKER_UNAVAILABLE"
    and data.get("orders_allowed") is False
    and data.get("broker_preflight_status") == "UNAVAILABLE"
    and data.get("broker_preflight_return_code") == 69
    and "dotenv_loaded_keys" not in data
    and "safe_env" not in data
    and dotenv_loaded.get("key_names_recorded") is False
    and dotenv_loaded.get("secret_values_recorded") is False
)
raise SystemExit(0 if ok else 1)
PY
  test ! -f "$case_dir/run/run_manifest.json"
  test ! -f "$case_dir/run/logs/cargo_build_xuan_b27_dplus_user_ws_observer.log"
  for secret in "$api_key" "$api_secret" "$api_passphrase" "$private_key" "$token"; do
    if rg -q --fixed-strings "$secret" "$case_dir"; then
      printf 'secret value leaked for %s: %s\n' "$name" "$secret" >> "$log"
      return 1
    fi
  done
  for key_name in POLYMARKET_API_KEY POLYMARKET_API_SECRET POLYMARKET_API_PASSPHRASE POLYMARKET_PRIVATE_KEY PM_TEST_SECRET_TOKEN; do
    if rg -q --fixed-strings "$key_name" "$case_dir"; then
      printf 'secret key name leaked for %s: %s\n' "$name" "$key_name" >> "$log"
      return 1
    fi
  done
}

run_check "wrapper_exists" test -x scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "missing_approval_refuses_before_build" \
  run_wrapper_case missing_approval 64 REFUSED_NO_EXPLICIT_APPROVAL
run_check "invalid_duration_refuses_before_build" \
  run_wrapper_case invalid_duration 64 REFUSED_INVALID_DURATION --approved-readonly-user-ws --duration-seconds 0
run_check "missing_broker_refuses_before_build" \
  run_wrapper_case missing_broker 69 SHARED_INGRESS_BROKER_UNAVAILABLE --approved-readonly-user-ws
run_check "missing_broker_artifacts_redact_secret_values" \
  run_secret_redaction_case

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_readonly_user_ws_wrapper_refusal_gate",
  "checks_log": "$log",
  "generated_at_utc": "$ts",
  "orders_sent": false,
  "auth_network_started": false
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
