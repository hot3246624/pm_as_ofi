#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_diagnostic_smoke_$ts}"
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

latest_review="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_*/local_acceptance_review.json 2>/dev/null | head -1 || true)"

run_check "latest_diagnostic_review_exists" test -n "$latest_review"
run_check "latest_diagnostic_review_safe_and_passed" python3 - "$latest_review" <<'PY'
import json
import pathlib
import sys

if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
side_effects = data.get("side_effects") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_local_acceptance_review"
    and data.get("status") == "PASS_READONLY_USER_WS_DIAGNOSTIC"
    and data.get("acceptance_passed") is True
    and data.get("wrapper_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("wrapper_return_code") == 0
    and data.get("wrapper_summary_return_code") == 0
    and data.get("run_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("summary_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("gate_status") == "PASS"
    and data.get("gate_failures") == []
    and data.get("user_ws_connected_count", 0) > 0
    and data.get("user_ws_subscribe_sent_count", 0) > 0
    and data.get("user_ws_raw_count", 0) > 0
    and data.get("user_ws_error_event_count") == 0
    and data.get("forbidden_event_count") == 0
    and data.get("event_decode_error_count") == 0
    and data.get("user_ws_decode_error_count") == 0
    and data.get("orders_possible") is False
    and data.get("recorder_critical_drop_count") == 0
    and data.get("secret_values_recorded") is False
    and data.get("secret_values_written_to_disk") is False
    and data.get("broker_started_or_modified") is False
    and data.get("systemd_or_service_control_used") is False
    and data.get("order_cancel_redeem_allowed") is False
    and side_effects.get("started_observer") is True
    and side_effects.get("started_user_ws") is True
    and side_effects.get("started_broker") is False
    and side_effects.get("stopped_broker") is False
    and side_effects.get("systemd_called") is False
    and side_effects.get("service_control_called") is False
    and side_effects.get("orders_sent") is False
    and side_effects.get("cancels_sent") is False
    and side_effects.get("redeems_sent") is False
    and side_effects.get("modified_srv_pm_as_ofi") is False
    and side_effects.get("modified_shared_ingress") is False
    and side_effects.get("env_files_written") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_ec2_readonly_diagnostic_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_ec2_readonly_diagnostic_gate",
  "diagnostic_review": "$latest_review",
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
