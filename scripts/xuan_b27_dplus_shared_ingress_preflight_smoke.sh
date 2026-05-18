#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shared_ingress_preflight_smoke_$ts}"
fixtures="$(mktemp -d "${TMPDIR:-/tmp}/xbdp_si.XXXXXX")"
mkdir -p "$out_dir"
mkdir -p "$fixtures"
trap 'rm -rf "$fixtures"' EXIT

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

run_case() {
  local name="$1"
  local expected_rc="$2"
  local expected_status="$3"
  local expected_reason="$4"
  local root_dir="$fixtures/$name"
  local case_dir="$out_dir/$name"
  local rc=0
  scripts/xuan_b27_dplus_shared_ingress_preflight.py \
    --root "$root_dir" \
    --output-dir "$case_dir" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  python3 - "$case_dir/manifest.json" "$expected_status" "$expected_reason" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_status, expected_reason = sys.argv[2:4]
data = json.loads(path.read_text())
side_effects = data.get("side_effects", {})
ok = (
    data.get("status") == expected_status
    and data.get("reason") == expected_reason
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY
}

python3 - "$fixtures" <<'PY'
import json
import pathlib
import socket
import sys
import time

root = pathlib.Path(sys.argv[1])
now_ms = int(time.time() * 1000)

def make_socket(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(str(path))
    finally:
        sock.close()

def write_manifest(case: str, heartbeat_ms: int, sockets: dict[str, pathlib.Path]) -> None:
    case_root = root / case
    case_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "protocol_version": 1,
        "schema_version": 1,
        "build_id": f"fixture-{case}",
        "pid": 12345,
        "started_ms": now_ms - 1000,
        "last_heartbeat_ms": heartbeat_ms,
        "chainlink_socket": str(sockets["chainlink_socket"]),
        "local_price_socket": str(sockets["local_price_socket"]),
        "market_socket": str(sockets["market_socket"]),
    }
    (case_root / "broker_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

healthy_sockets = {
    "chainlink_socket": root / "healthy" / "chainlink.sock",
    "local_price_socket": root / "healthy" / "local_price.sock",
    "market_socket": root / "healthy" / "market.sock",
}
for path in healthy_sockets.values():
    make_socket(path)
write_manifest("healthy", now_ms, healthy_sockets)

stale_sockets = {
    "chainlink_socket": root / "stale" / "chainlink.sock",
    "local_price_socket": root / "stale" / "local_price.sock",
    "market_socket": root / "stale" / "market.sock",
}
for path in stale_sockets.values():
    make_socket(path)
write_manifest("stale", now_ms - 60_000, stale_sockets)

missing_socket_root = root / "missing_socket"
missing_socket_sockets = {
    "chainlink_socket": missing_socket_root / "chainlink.sock",
    "local_price_socket": missing_socket_root / "local_price.sock",
    "market_socket": missing_socket_root / "market.sock",
}
make_socket(missing_socket_sockets["chainlink_socket"])
make_socket(missing_socket_sockets["local_price_socket"])
write_manifest("missing_socket", now_ms, missing_socket_sockets)

regular_file_root = root / "regular_file_socket"
regular_file_sockets = {
    "chainlink_socket": regular_file_root / "chainlink.sock",
    "local_price_socket": regular_file_root / "local_price.sock",
    "market_socket": regular_file_root / "market.sock",
}
make_socket(regular_file_sockets["chainlink_socket"])
make_socket(regular_file_sockets["local_price_socket"])
regular_file_sockets["market_socket"].parent.mkdir(parents=True, exist_ok=True)
regular_file_sockets["market_socket"].write_text("not a unix socket\n")
write_manifest("regular_file_socket", now_ms, regular_file_sockets)

missing_manifest_root = root / "missing_manifest"
missing_manifest_root.mkdir(parents=True, exist_ok=True)
PY

run_check "script_exists" test -x scripts/xuan_b27_dplus_shared_ingress_preflight.py
run_check "script_py_compile" python3 -m py_compile scripts/xuan_b27_dplus_shared_ingress_preflight.py
run_check "script_has_no_broker_side_effect_calls" \
  bash -c "! rg -q 'systemctl|run_shared_ingress|PM_SHARED_INGRESS_ROLE.*broker|subprocess|socket\\.connect|rsync|scp|ssh' scripts/xuan_b27_dplus_shared_ingress_preflight.py"
run_check "missing_manifest_blocks" run_case missing_manifest 69 UNAVAILABLE broker_manifest_missing
run_check "healthy_manifest_and_sockets_ok" run_case healthy 0 OK broker_manifest_and_sockets_ok
run_check "stale_heartbeat_blocks" bash -c '
  scripts/xuan_b27_dplus_shared_ingress_preflight.py --root "'"$fixtures"'/stale" --output-dir "'"$out_dir"'/stale" >/dev/null 2>&1 && exit 1 || true
  python3 - "'"$out_dir"'/stale/manifest.json" <<PY
import json, pathlib, sys
data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = data.get("status") == "UNAVAILABLE" and str(data.get("reason", "")).startswith("broker_heartbeat_stale_ms=")
raise SystemExit(0 if ok else 1)
PY
'
run_check "missing_socket_blocks" run_case missing_socket 69 UNAVAILABLE broker_socket_unavailable
run_check "regular_file_socket_blocks" bash -c '
  scripts/xuan_b27_dplus_shared_ingress_preflight.py --root "'"$fixtures"'/regular_file_socket" --output-dir "'"$out_dir"'/regular_file_socket" >/dev/null 2>&1 && exit 1 || true
  python3 - "'"$out_dir"'/regular_file_socket/manifest.json" <<PY
import json, pathlib, sys
data = json.loads(pathlib.Path(sys.argv[1]).read_text())
checks = data.get("socket_checks", [])
ok = (
    data.get("status") == "UNAVAILABLE"
    and data.get("reason") == "broker_socket_unavailable"
    and any(c.get("field") == "market_socket" and c.get("exists") is True and c.get("is_socket") is False and c.get("reason") == "not_socket" for c in checks)
)
raise SystemExit(0 if ok else 1)
PY
'

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shared_ingress_preflight_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shared_ingress_fixture_gate",
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
