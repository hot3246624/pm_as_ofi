#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_launch_plan_smoke_$ts}"
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

script="scripts/xuan_b27_dplus_g2_canary_launch_plan.py"

run_check "plan_script_exists" test -x "$script"
run_check "plan_script_py_compile" python3 -m py_compile "$script"
run_check "plan_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "plan_script_run" "$script" --output-dir "$plan_dir" --duration-seconds 1800 --max-rounds 6 --market-slug btc-updown-5m
run_check "plan_manifest_exists" test -f "$plan_manifest"
run_check "plan_manifest_safe" python3 - "$plan_manifest" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
inputs = data.get("inputs") or {}
resync = data.get("proposed_resync_rebuild") or {}
run = data.get("proposed_g2_canary_run") or {}
env = run.get("env_envelope") or {}
allowed = run.get("allowed_side_effects_after_exact_approval") or {}
forbidden_run = run.get("forbidden_side_effects_even_after_exact_approval") or {}
review = data.get("review_acceptance_contract") or {}
side_effects = data.get("side_effects") or {}
forbidden = data.get("forbidden_side_effects") or {}
executor = data.get("executor_preflight") or {}
executor_smoke = executor.get("smoke") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_launch_plan"
    and data.get("status") == "BLOCKED_G2_CANARY_LAUNCH_PLAN_INPUTS"
    and data.get("scope") == "local_no_network_g2_canary_launch_plan"
    and data.get("local_gates_ok") is False
    and data.get("canary_run_authorized") is False
    and data.get("ready_to_execute") is False
    and data.get("effectful_executor_implemented") is False
    and data.get("requires_reviewed_effectful_executor_implementation") is True
    and data.get("execution_readiness_status") == "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and executor_smoke.get("status") == "PASS"
    and executor_smoke.get("scope") == "local_no_network_g2_canary_executor_preflight_gate"
    and executor.get("valid_preflight_exists") is True
    and executor.get("status") == "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and executor.get("scope") == "local_no_network_g2_canary_executor_preflight"
    and executor.get("executor_preflight_implemented") is True
    and executor.get("effectful_executor_implemented") is False
    and executor.get("ready_to_execute") is False
    and executor.get("canary_run_authorized") is False
    and executor.get("first_unimplemented_effectful_phase") is None
    and executor.get("all_effectful_phase_plans_present") is True
    and executor.get("all_plan_sections_pass") is True
    and executor.get("phase_side_effects_all_false") is True
    and executor.get("planned_side_effect_sections_all_false") is True
    and executor.get("top_level_side_effects_all_false") is True
    and executor.get("artifact_pullback_review_plan_status") == "PASS"
    and executor.get("orders_sent") is False
    and executor.get("cancels_sent") is False
    and executor.get("redeems_sent") is False
    and executor.get("auth_network_started") is False
    and executor.get("started_canary") is False
    and inputs.get("readiness", {}).get("status") == "READY_FOR_APPROVAL"
    and inputs.get("status_bundle", {}).get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
    and inputs.get("status_bundle", {}).get("canary_readiness_status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
    and "shadow trading acceptance failed" in inputs.get("status_bundle", {}).get("canary_next_gate", "")
    and (
        "retrain/tune strategy" in inputs.get("status_bundle", {}).get("canary_next_gate", "")
        or "compliant declared strict/cache/completion data" in inputs.get("status_bundle", {}).get("canary_next_gate", "")
    )
    and inputs.get("canary_readiness_plan_smoke", {}).get("status") == "PASS"
    and inputs.get("g2_canary_runbook_smoke", {}).get("status") == "PASS"
    and inputs.get("g2_canary_acceptance_smoke", {}).get("status") == "PASS"
    and inputs.get("ec2_readonly_acceptance_smoke", {}).get("status") == "PASS"
    and inputs.get("source_truth_runtime_gate_smoke", {}).get("status") == "PASS"
    and inputs.get("source_truth_runtime_gate_smoke", {}).get("wallet_redeem_cashflow_runtime_producers_wired") is True
    and inputs.get("source_truth_runtime_gate_smoke", {}).get("redeem_truth_can_pass_from_runtime_producer") is True
    and inputs.get("g2_canary_executor_smoke", {}).get("status") == "PASS"
    and resync.get("requires_explicit_sync_rebuild_approval") is True
    and str(resync.get("remote_worktree", "")).startswith("/home/ubuntu/xuan_research_runs/xuan_research_")
    and resync.get("allowed_remote_write_prefix") == "/home/ubuntu/xuan_research_runs/xuan_research_"
    and "polymarket_v2" in (resync.get("build_targets") or [])
    and "/srv/pm_as_ofi" in (resync.get("forbidden_paths") or [])
    and run.get("requires_explicit_exact_run_approval") is True
    and run.get("run_class") == "G2_READ_WRITE_CANARY_SMOKE"
    and str(run.get("remote_worktree", "")).startswith("/home/ubuntu/xuan_research_runs/xuan_research_")
    and str(run.get("remote_run_dir", "")).startswith("/home/ubuntu/xuan_research_runs/xuan_research_")
    and run.get("duration_seconds") == 1800
    and run.get("max_rounds") == 6
    and run.get("market_slug") == "btc-updown-5m"
    and run.get("shared_ingress_role") == "client"
    and run.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
    and env.get("PM_STRATEGY") == "xuan_b27_dplus"
    and env.get("PM_XUAN_B27_DPLUS_MODE") == "canary"
    and env.get("PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL") == "true"
    and env.get("PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED") == "true"
    and env.get("PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED") == "true"
    and env.get("PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC") == "50"
    and env.get("PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC") == "100"
    and env.get("PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS") == "2"
    and env.get("PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER") == "false"
    and env.get("PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN") == "true"
    and allowed.get("capped_post_only_orders") is True
    and allowed.get("bounded_own_order_cancels") is True
    and allowed.get("redeem_or_claim") is False
    and all(value is True for value in forbidden_run.values())
    and review.get("required_status") == "PASS_G2_CANARY_ACCEPTANCE"
    and "source_truth_all_pass" in (review.get("required_fields") or [])
    and "unexpected_taker_fill" in (review.get("hard_stop_failures") or [])
    and all(value is False for value in side_effects.values())
    and all(value is False for value in forbidden.values())
    and "shadow trading acceptance failed" in data.get("next_gate", "")
    and (
        "retrain/tune strategy" in data.get("next_gate", "")
        or "compliant declared strict/cache/completion data" in data.get("next_gate", "")
    )
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_launch_plan_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_launch_plan_gate",
  "plan_manifest": "$plan_manifest",
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
