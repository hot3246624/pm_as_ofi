#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_contract_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
contract_dir="$out_dir/contract_run"
contract_manifest="$contract_dir/manifest.json"
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

script="scripts/xuan_b27_dplus_g2_canary_executor_contract.py"

run_check "contract_script_exists" test -x "$script"
run_check "contract_script_py_compile" python3 -m py_compile "$script"
run_check "contract_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "contract_script_run" "$script" --output-dir "$contract_dir"
run_check "contract_manifest_exists" test -f "$contract_manifest"
run_check "contract_manifest_safe" python3 - "$contract_manifest" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
inputs = data.get("inputs") or {}
allowed = data.get("allowed_side_effects_after_exact_executor_approval") or {}
forbidden = data.get("forbidden_side_effects_even_after_exact_executor_approval") or {}
side_effects = data.get("side_effects") or {}
phases = data.get("required_executor_phases") or []
aborts = data.get("mandatory_abort_conditions") or []
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_executor_contract"
    and data.get("status") == "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION"
    and data.get("scope") == "local_no_network_g2_canary_executor_contract"
    and data.get("inputs_ok") is True
    and data.get("executor_implemented") is False
    and data.get("canary_run_authorized") is False
    and inputs.get("status_bundle", {}).get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
    and inputs.get("status_bundle", {}).get("canary_readiness_status") in {
        "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
        "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED",
    }
    and inputs.get("canary_readiness_plan_smoke", {}).get("status") == "PASS"
    and inputs.get("canary_readiness_plan", {}).get("status") in {
        "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
        "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED",
    }
    and inputs.get("canary_readiness_plan", {}).get("pre_canary_plumbing_ready") is True
    and inputs.get("canary_readiness_plan", {}).get("rust_shadow_strategy_acceptance_ready") is True
    and inputs.get("canary_readiness_plan", {}).get("ready_for_explicit_g2_canary_approval") is False
    and inputs.get("canary_readiness_plan", {}).get("requires_rust_shadow_strategy_acceptance") is False
    and inputs.get("launch_plan_smoke", {}).get("status") == "PASS"
    and inputs.get("approval_envelope_smoke", {}).get("status") == "PASS"
    and inputs.get("post_run_review_smoke", {}).get("status") == "PASS"
    and inputs.get("launcher_refusal_smoke", {}).get("status") == "PASS"
    and inputs.get("payload_manifest_smoke", {}).get("status") == "PASS"
    and inputs.get("payload_manifest", {}).get("status") == "PASS"
    and isinstance(inputs.get("payload_manifest", {}).get("payload_file_count"), int)
    and inputs.get("payload_manifest", {}).get("payload_file_count") >= 40
    and inputs.get("payload_manifest", {}).get("missing_required_files") == []
    and inputs.get("payload_manifest", {}).get("missing_executable_files") == []
    and inputs.get("payload_manifest", {}).get("forbidden_path_hits") == []
    and inputs.get("payload_manifest", {}).get("duplicate_paths") == []
    and inputs.get("payload_current_check", {}).get("status") == "PASS"
    and inputs.get("payload_current_check", {}).get("drift_count") == 0
    and inputs.get("payload_current_check", {}).get("drift_sample") == []
    and "validate_current_local_readiness_artifacts" in phases
    and "load_reviewed_payload_allowlist_manifest" in phases
    and "run_local_post_run_review_and_secret_sentinel_scan" in phases
    and "approval_envelope_missing_or_invalid" in aborts
    and "rust_shadow_strategy_acceptance_missing" in aborts
    and "payload_allowlist_missing_or_invalid" in aborts
    and "payload_allowlist_drift_detected" in aborts
    and "any_forbidden_side_effect_detected" in aborts
    and allowed.get("start_single_bounded_canary") is True
    and allowed.get("capped_post_only_orders") is True
    and allowed.get("bounded_own_order_cancels") is True
    and all(value is True for value in forbidden.values())
    and all(value is False for value in side_effects.values())
    and "implement and review" in data.get("next_gate", "")
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_executor_contract_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_executor_contract_gate",
  "contract_manifest": "$contract_manifest",
  "orders_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
