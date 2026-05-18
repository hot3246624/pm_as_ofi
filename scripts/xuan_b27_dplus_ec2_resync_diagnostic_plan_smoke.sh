#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
plan_dir="$out_dir/plan_run"
plan_manifest="$plan_dir/manifest.json"
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

script="scripts/xuan_b27_dplus_ec2_resync_diagnostic_plan.py"

run_check "plan_script_exists" test -x "$script"
run_check "plan_script_py_compile" python3 -m py_compile "$script"
run_check "plan_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output' '$script'"
run_check "plan_script_run" "$script" --output-dir "$plan_dir" --duration-seconds 300 --market-slug btc-updown-5m
run_check "plan_manifest_exists" test -f "$plan_manifest"
run_check "plan_manifest_safe" python3 - "$plan_manifest" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
forbidden = data.get("forbidden_side_effects") or {}
resync = data.get("proposed_resync") or {}
run = data.get("proposed_diagnostic_run") or {}
gate_inputs = data.get("local_gate_inputs") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_ec2_resync_diagnostic_plan"
    and data.get("status") == "READY_FOR_EXPLICIT_SYNC_AND_DIAGNOSTIC_APPROVAL"
    and data.get("scope") == "local_no_network_ec2_resync_diagnostic_plan"
    and data.get("local_gates_ok") is True
    and data.get("stale_sync_confirmed") is True
    and resync.get("requires_explicit_resync_rebuild_approval") is True
    and str(resync.get("remote_root", "")).startswith("/home/ubuntu/xuan_research_runs/xuan_research_")
    and str(resync.get("remote_worktree", "")).startswith("/home/ubuntu/xuan_research_runs/xuan_research_")
    and resync.get("allowed_remote_write_prefix") == "/home/ubuntu/xuan_research_runs/xuan_research_"
    and "/srv/pm_as_ofi" in (resync.get("forbidden_remote_write_prefixes") or [])
    and "/srv/pm_as_ofi/shared-ingress-main" in (resync.get("forbidden_remote_write_prefixes") or [])
    and ".env" in (resync.get("must_exclude") or [])
    and "target" in (resync.get("must_exclude") or [])
    and "raw" in (resync.get("must_exclude") or [])
    and resync.get("must_rebuild_binary") == "cargo build --locked --bin xuan_b27_dplus_user_ws_observer"
    and run.get("requires_explicit_exact_run_approval") is True
    and run.get("shared_ingress_role") == "client"
    and run.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
    and run.get("duration_seconds") == 300
    and run.get("market_slug") == "btc-updown-5m"
    and "--approved-readonly-user-ws" in (run.get("wrapper_args") or [])
    and any("--require-user-ws-connection" in item for item in (run.get("acceptance_gate") or []))
    and all(value is False for value in side_effects.values())
    and all(value is False for value in forbidden.values())
    and gate_inputs.get("readiness", {}).get("status") == "READY_FOR_APPROVAL"
    and gate_inputs.get("static_smoke", {}).get("status") == "PASS"
    and gate_inputs.get("summary_gate_smoke", {}).get("status") == "PASS"
    and gate_inputs.get("wrapper_refusal_smoke", {}).get("status") == "PASS"
    and gate_inputs.get("bundle_smoke", {}).get("status") == "PASS"
    and data.get("postmortem", {}).get("summary_return_code") == 4
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_ec2_resync_diagnostic_plan_gate",
  "plan_manifest": "$plan_manifest",
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
