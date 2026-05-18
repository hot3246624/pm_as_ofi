#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_static_smoke_$ts}"
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

src="src/bin/xuan_b27_dplus_user_ws_observer.rs"

run_check "cargo_build_readonly_user_ws_observer" \
  cargo build --bin xuan_b27_dplus_user_ws_observer
run_check "cargo_registers_readonly_user_ws_observer_bin" \
  rg -q 'name = "xuan_b27_dplus_user_ws_observer"' Cargo.toml
run_check "source_exists" test -f "$src"
run_check "requires_explicit_readonly_user_ws_approval" \
  rg -q -- '--approved-readonly-user-ws|REFUSED_NO_EXPLICIT_APPROVAL' "$src"
run_check "requires_shared_ingress_broker" \
  rg -q 'SHARED_INGRESS_BROKER_UNAVAILABLE|check_shared_ingress' "$src"
run_check "requires_shared_ingress_unix_socket_file_type" \
  bash -c "rg -q 'FileTypeExt' '$src' && rg -q 'is_socket\\(\\)' '$src' && rg -q 'socket_not_unix_socket' '$src'"
run_check "installs_rustls_crypto_provider_before_network_paths" python3 - "$src" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1]).read_text()
required = [
    "fn install_rustls_crypto_provider()",
    "rustls::crypto::ring::default_provider()",
    ".install_default()",
]
if not all(term in src for term in required):
    raise SystemExit(1)
call = src.find("install_rustls_crypto_provider();")
parse_args = src.find("let args = parse_args()?")
resolve_creds = src.find("resolve_user_ws_credentials(&rest_url)")
if call < 0 or parse_args < 0 or resolve_creds < 0:
    raise SystemExit(1)
raise SystemExit(0 if call < parse_args < resolve_creds else 1)
PY
run_check "has_no_standalone_public_ws_escape_hatch" \
  bash -c "! rg -q 'allow-standalone|standalone public|PM_SHARED_INGRESS_ROLE.*standalone' '$src'"
run_check "declares_orders_impossible" \
  rg -q 'orders_possible: false|\"orders_possible\": false' "$src"
run_check "run_manifest_has_no_secret_material_fields" python3 - "$src" <<'PY'
import pathlib
import re
import sys

src = pathlib.Path(sys.argv[1]).read_text()
match = re.search(r"struct RunManifest \{(?P<body>.*?)\n\}", src, re.S)
if not match:
    raise SystemExit(1)
body = match.group("body")
forbidden = ("api_key", "api_secret", "api_passphrase", "private_key", "credential", "token", "secret")
ok = "user_ws_auth_source" in body and not any(term in body.lower() for term in forbidden)
raise SystemExit(0 if ok else 1)
PY
run_check "uses_user_ws_listener" \
  rg -q 'UserWsListener::new|UserWsConfig' "$src"
run_check "fails_when_user_ws_task_exits_early" \
  bash -c "rg -q 'tokio::select!' '$src' && rg -q 'FAIL_USER_WS_TASK_EXITED|user_ws_task_exited_early|User WS task exited before duration elapsed' '$src'"
run_check "records_user_ws_lifecycle_events" \
  bash -c "rg -q 'emit_lifecycle_event|user_ws_connected|user_ws_subscribe_sent|user_ws_connect_or_read_error' src/polymarket/user_ws.rs"
run_check "uses_recorder_only_for_outputs" \
  rg -q 'RecorderHandle|RecorderConfig|record_user_ws_raw|readonly_user_ws_observer' "$src"
run_check "does_not_import_execution_actors" \
  bash -c "! rg -q 'use .*Executor|use .*OrderManager|use .*InventoryManager|ExecutionCmd|OrderManagerCmd|InventoryEvent' '$src'"
run_check "does_not_call_order_or_redeem_paths" \
  bash -c "! rg -q 'post_order|create_order|CancelAll|execute_market_merge|maybe_auto_claim|run_auto_claim_once' '$src'"
run_check "launch_wrapper_exists" test -x scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_requires_explicit_approval" \
  rg -q -- '--approved-readonly-user-ws|REFUSED_NO_EXPLICIT_APPROVAL' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_builds_current_standalone_binary" \
  rg -q '"cargo", "build", "--bin", "xuan_b27_dplus_user_ws_observer"' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_forces_shared_ingress_client" \
  rg -q '"PM_SHARED_INGRESS_ROLE": "client"' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_has_no_standalone_escape_hatch" \
  bash -c "! rg -q 'allow-standalone|PM_SHARED_INGRESS_ROLE.*standalone|standalone public|standalone market' scripts/xuan_b27_dplus_readonly_user_ws_observer.py"
run_check "launch_wrapper_propagates_broker_unavailable" \
  rg -q 'SHARED_INGRESS_BROKER_UNAVAILABLE|return 69' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_preflights_broker_before_build" \
  bash -c "rg -q 'xuan_b27_dplus_shared_ingress_preflight.py' scripts/xuan_b27_dplus_readonly_user_ws_observer.py && rg -q 'broker_preflight_manifest' scripts/xuan_b27_dplus_readonly_user_ws_observer.py"
run_check "launch_wrapper_does_not_record_env_key_names" \
  bash -c "! rg -q 'dotenv_loaded_keys|safe_env' scripts/xuan_b27_dplus_readonly_user_ws_observer.py && rg -q 'key_names_recorded.*False|key_names_recorded\": False' scripts/xuan_b27_dplus_readonly_user_ws_observer.py"
run_check "launch_wrapper_runs_readonly_summary_gate" \
  rg -q 'xuan_b27_dplus_summarize_readonly_user_ws_run.py|--check-readonly|--require-user-ws-connection' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "launch_wrapper_propagates_summary_gate_exit_codes" python3 - <<'PY'
import importlib.util
import pathlib

path = pathlib.Path("scripts/xuan_b27_dplus_readonly_user_ws_observer.py")
spec = importlib.util.spec_from_file_location("readonly_user_ws_wrapper", path)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

cases = [
    ("FAIL_READONLY_VIOLATION", 0, 2, 2),
    ("FAIL_NO_USER_WS_RECORDS", 0, 3, 3),
    ("FAIL_NO_USER_WS_CONNECTION", 0, 4, 4),
    ("FAIL_SUMMARY_GATE", 0, 7, 7),
    ("FAIL_SUMMARY_GATE", 0, 0, 70),
    ("FAIL_PROCESS_EXIT_NONZERO", 9, 0, 9),
    ("PASS_READONLY_USER_WS_OBSERVER", 0, 0, 0),
]
for status, process_rc, summary_rc, expected in cases:
    got = module.wrapper_exit_code(status, process_rc, summary_rc)
    if got != expected:
        raise SystemExit(
            f"{status}: got wrapper_exit_code={got}, expected {expected}"
        )
PY
run_check "readonly_user_ws_summarizer_exists" test -x scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "readonly_user_ws_summarizer_has_readonly_gate" \
  rg -q -- '--check-readonly|FAIL_READONLY_VIOLATION' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "readonly_user_ws_summarizer_fails_decode_errors" \
  rg -q 'FAIL_RECORDER_DECODE_ERROR' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "readonly_user_ws_summarizer_can_require_user_ws_records" \
  rg -q -- '--require-user-ws-records|user_ws_raw_count' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "readonly_user_ws_summarizer_can_require_user_ws_connection" \
  rg -q -- '--require-user-ws-connection|user_ws_connected_count|user_ws_subscribe_sent_count' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "readonly_user_ws_summarizer_checks_forbidden_events" \
  rg -q 'FORBIDDEN_EVENT_FRAGMENTS|forbidden_event_count' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_readonly_user_ws_static_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_static_compile_gate",
  "source": "$ROOT/$src",
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
