#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_auth_source_preflight_smoke_$ts}"
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

write_fixture() {
  local name="$1"
  shift
  {
    for line in "$@"; do
      printf '%s\n' "$line"
    done
  } > "$fixtures/$name.env"
}

run_case() {
  local name="$1"
  local expected_rc="$2"
  local expected_status="$3"
  local expected_source="$4"
  local expected_reason="$5"
  local case_dir="$out_dir/$name"
  local rc=0
  scripts/xuan_b27_dplus_auth_source_preflight.py \
    --dotenv "$fixtures/$name.env" \
    --output-dir "$case_dir" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  python3 - "$case_dir/manifest.json" "$expected_status" "$expected_source" "$expected_reason" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_status, expected_source, expected_reason = sys.argv[2:5]
data = json.loads(path.read_text())
text = path.read_text()
for forbidden in (
    "fixture-api-key",
    "fixture-api-secret",
    "fixture-api-passphrase",
    "fixture-private-key",
):
    if forbidden in text:
        raise SystemExit(f"secret value leaked into manifest: {forbidden}")
ok = (
    data.get("status") == expected_status
    and data.get("auth_source") == expected_source
    and data.get("reason") == expected_reason
    and data.get("secrets_redacted") is True
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and data.get("side_effects", {}).get("derived_credentials") is False
    and data.get("side_effects", {}).get("printed_secret_values") is False
)
raise SystemExit(0 if ok else 1)
PY
}

write_fixture complete_api \
  'POLYMARKET_API_KEY=fixture-api-key' \
  'POLYMARKET_API_SECRET=fixture-api-secret' \
  'POLYMARKET_API_PASSPHRASE=fixture-api-passphrase'
write_fixture private_key \
  'POLYMARKET_PRIVATE_KEY=fixture-private-key'
write_fixture partial_api_with_private_key \
  'POLYMARKET_API_KEY=fixture-api-key' \
  'POLYMARKET_PRIVATE_KEY=fixture-private-key'
write_fixture missing \
  'PM_DRY_RUN=true'

run_check "script_exists" test -x scripts/xuan_b27_dplus_auth_source_preflight.py
run_check "script_py_compile" python3 -m py_compile scripts/xuan_b27_dplus_auth_source_preflight.py
run_check "script_has_no_network_or_secret_output" \
  bash -c "! rg -q 'requests|websocket|socket|subprocess|init_clob|print\\(.*env|ExposeSecret|API_SECRET.*value|PRIVATE_KEY.*value|POLYMARKET_PRIVATE_KEY=.*|POLYMARKET_API_SECRET=.*' scripts/xuan_b27_dplus_auth_source_preflight.py"
run_check "complete_api_ok" run_case complete_api 0 OK explicit_api_credentials complete_POLYMARKET_API_credentials_present
run_check "private_key_ok" run_case private_key 0 OK derive_from_private_key POLYMARKET_PRIVATE_KEY_present
run_check "partial_api_blocks_even_with_private_key" run_case partial_api_with_private_key 67 UNAVAILABLE partial_api_credentials POLYMARKET_API_credentials_partial
run_check "missing_blocks" run_case missing 67 UNAVAILABLE missing missing_complete_POLYMARKET_API_credentials_or_POLYMARKET_PRIVATE_KEY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_auth_source_preflight_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_auth_source_fixture_gate",
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
