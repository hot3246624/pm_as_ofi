#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_attempt_smoke_$ts}"
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

latest_attempt="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_user_ws_attempt_* 2>/dev/null | head -1 || true)"
attempt_manifest=""
if [[ -n "$latest_attempt" && -f "$latest_attempt/manifest.json" ]]; then
  attempt_manifest="$latest_attempt/manifest.json"
fi

run_check "latest_ec2_attempt_manifest_exists" test -n "$attempt_manifest"
run_check "latest_ec2_attempt_manifest_safe" python3 - "$attempt_manifest" <<'PY'
import json
import pathlib
import sys

if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
path = pathlib.Path(sys.argv[1])
if not path.exists():
    raise SystemExit(1)
data = json.loads(path.read_text())

allowed_statuses = {
    "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING",
    "PASS_READONLY_USER_WS_OBSERVER",
    "FAIL_READONLY_USER_WS_OBSERVER",
}
auth = data.get("authorization") or {}
ssh = data.get("ssh_preflight") or {}
broker = data.get("remote_shared_ingress") or {}
discovery = data.get("remote_entrypoint_discovery") or {}
side_effects = data.get("side_effects") or {}
socket_checks = broker.get("socket_checks") or []
forbidden_side_effect_fields = {
    "systemd_called",
    "service_control_called",
    "broker_started",
    "broker_stopped",
    "broker_repaired",
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "remote_files_written",
    "deployed_code",
    "scp_or_rsync_used",
    "tunnel_opened",
}
forbidden_side_effects_absent = bool(side_effects) and all(
    side_effects.get(field) is False for field in forbidden_side_effect_fields
)

common_ok = (
    data.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_attempt"
    and data.get("status") in allowed_statuses
    and auth.get("approved_in_current_conversation") is True
    and auth.get("orders_allowed") is False
    and auth.get("cancels_allowed") is False
    and auth.get("redeems_allowed") is False
    and ssh.get("status") == "OK"
    and broker.get("root") == "/srv/pm_as_ofi/shared-ingress-main"
    and broker.get("manifest_exists") is True
    and broker.get("protocol_version") == 1
    and broker.get("schema_version") == 1
    and bool(socket_checks)
    and all(check.get("exists") is True and check.get("is_socket") is True for check in socket_checks)
    and forbidden_side_effects_absent
)

if data.get("status") == "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING":
    closed_ok = (
        discovery.get("required_files_found_in_any_candidate_root") is False
        and bool(discovery.get("candidate_roots_checked"))
        and bool(discovery.get("required_files"))
        and side_effects.get("observer_started") is False
        and side_effects.get("user_ws_started") is False
        and side_effects.get("remote_files_written") is False
        and side_effects.get("deployed_code") is False
        and side_effects.get("scp_or_rsync_used") is False
        and side_effects.get("tunnel_opened") is False
        and isinstance(data.get("next_gate"), str)
        and "deployment/sync" in data.get("next_gate", "")
    )
elif data.get("status") == "PASS_READONLY_USER_WS_OBSERVER":
    closed_ok = (
        side_effects.get("observer_started") is True
        and side_effects.get("user_ws_started") is True
        and side_effects.get("orders_sent") is False
    )
else:
    closed_ok = side_effects.get("orders_sent") is False and side_effects.get("cancels_sent") is False

raise SystemExit(0 if common_ok and closed_ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_ec2_readonly_attempt_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_ec2_readonly_attempt_gate",
  "attempt_manifest": "$attempt_manifest",
  "orders_sent": false,
  "auth_network_started": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
