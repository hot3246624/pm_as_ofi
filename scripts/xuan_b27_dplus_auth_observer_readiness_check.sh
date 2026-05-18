#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_auth_observer_readiness_$ts}"
mkdir -p "$out_dir"

checks="$out_dir/readiness_checks.txt"
: > "$checks"
status="READY_FOR_APPROVAL"

run_check() {
  local name="$1"
  shift
  if "$@"; then
    printf 'PASS %s\n' "$name" >> "$checks"
  else
    printf 'FAIL %s\n' "$name" >> "$checks"
    status="BLOCKED_BEFORE_AUTH_OBSERVER"
  fi
}

latest_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_observer_safety_smoke_* 2>/dev/null | head -1 || true)"
smoke_manifest=""
if [[ -n "$latest_smoke" && -f "$latest_smoke/manifest.json" ]]; then
  smoke_manifest="$latest_smoke/manifest.json"
fi

latest_summary_gate_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_observer_summary_gate_smoke_* 2>/dev/null | head -1 || true)"
summary_gate_smoke_manifest=""
if [[ -n "$latest_summary_gate_smoke" && -f "$latest_summary_gate_smoke/manifest.json" ]]; then
  summary_gate_smoke_manifest="$latest_summary_gate_smoke/manifest.json"
fi

latest_readonly_user_ws_static_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_static_smoke_* 2>/dev/null | head -1 || true)"
readonly_user_ws_static_smoke_manifest=""
if [[ -n "$latest_readonly_user_ws_static_smoke" && -f "$latest_readonly_user_ws_static_smoke/manifest.json" ]]; then
  readonly_user_ws_static_smoke_manifest="$latest_readonly_user_ws_static_smoke/manifest.json"
fi

latest_readonly_user_ws_summary_gate_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_summary_gate_smoke_* 2>/dev/null | head -1 || true)"
readonly_user_ws_summary_gate_smoke_manifest=""
if [[ -n "$latest_readonly_user_ws_summary_gate_smoke" && -f "$latest_readonly_user_ws_summary_gate_smoke/manifest.json" ]]; then
  readonly_user_ws_summary_gate_smoke_manifest="$latest_readonly_user_ws_summary_gate_smoke/manifest.json"
fi

latest_readonly_user_ws_wrapper_refusal_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke_* 2>/dev/null | head -1 || true)"
readonly_user_ws_wrapper_refusal_smoke_manifest=""
if [[ -n "$latest_readonly_user_ws_wrapper_refusal_smoke" && -f "$latest_readonly_user_ws_wrapper_refusal_smoke/manifest.json" ]]; then
  readonly_user_ws_wrapper_refusal_smoke_manifest="$latest_readonly_user_ws_wrapper_refusal_smoke/manifest.json"
fi

local_status_bundle_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_local_status_bundle_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

latest_auth_source_preflight_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_auth_source_preflight_smoke_* 2>/dev/null | head -1 || true)"
auth_source_preflight_smoke_manifest=""
if [[ -n "$latest_auth_source_preflight_smoke" && -f "$latest_auth_source_preflight_smoke/manifest.json" ]]; then
  auth_source_preflight_smoke_manifest="$latest_auth_source_preflight_smoke/manifest.json"
fi

latest_shared_ingress_preflight_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_shared_ingress_preflight_smoke_* 2>/dev/null | head -1 || true)"
shared_ingress_preflight_smoke_manifest=""
if [[ -n "$latest_shared_ingress_preflight_smoke" && -f "$latest_shared_ingress_preflight_smoke/manifest.json" ]]; then
  shared_ingress_preflight_smoke_manifest="$latest_shared_ingress_preflight_smoke/manifest.json"
fi

latest_source_of_truth_schema_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_source_of_truth_schema_smoke_* 2>/dev/null | head -1 || true)"
source_of_truth_schema_smoke_manifest=""
if [[ -n "$latest_source_of_truth_schema_smoke" && -f "$latest_source_of_truth_schema_smoke/manifest.json" ]]; then
  source_of_truth_schema_smoke_manifest="$latest_source_of_truth_schema_smoke/manifest.json"
fi

latest_ec2_readonly_attempt_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_attempt_smoke_* 2>/dev/null | head -1 || true)"
ec2_readonly_attempt_smoke_manifest=""
if [[ -n "$latest_ec2_readonly_attempt_smoke" && -f "$latest_ec2_readonly_attempt_smoke/manifest.json" ]]; then
  ec2_readonly_attempt_smoke_manifest="$latest_ec2_readonly_attempt_smoke/manifest.json"
fi

latest_ec2_entrypoint_manifest_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_entrypoint_manifest_smoke_* 2>/dev/null | head -1 || true)"
ec2_entrypoint_manifest_smoke_manifest=""
if [[ -n "$latest_ec2_entrypoint_manifest_smoke" && -f "$latest_ec2_entrypoint_manifest_smoke/manifest.json" ]]; then
  ec2_entrypoint_manifest_smoke_manifest="$latest_ec2_entrypoint_manifest_smoke/manifest.json"
fi

latest_ec2_resync_diagnostic_plan_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke_* 2>/dev/null | head -1 || true)"
ec2_resync_diagnostic_plan_smoke_manifest=""
if [[ -n "$latest_ec2_resync_diagnostic_plan_smoke" && -f "$latest_ec2_resync_diagnostic_plan_smoke/manifest.json" ]]; then
  ec2_resync_diagnostic_plan_smoke_manifest="$latest_ec2_resync_diagnostic_plan_smoke/manifest.json"
fi

latest_ec2_readonly_diagnostic_smoke="$(ls -1dt "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_diagnostic_smoke_* 2>/dev/null | head -1 || true)"
ec2_readonly_diagnostic_smoke_manifest=""
if [[ -n "$latest_ec2_readonly_diagnostic_smoke" && -f "$latest_ec2_readonly_diagnostic_smoke/manifest.json" ]]; then
  ec2_readonly_diagnostic_smoke_manifest="$latest_ec2_readonly_diagnostic_smoke/manifest.json"
fi

ec2_30m_acceptance_plan_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_30m_acceptance_plan_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

ec2_30m_acceptance_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_ec2_30m_acceptance_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

canary_order_plan_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_canary_order_plan_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

execution_controller_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_execution_controller_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

oms_adapter_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_oms_adapter_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

runtime_wiring_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_runtime_wiring_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

source_truth_runtime_gate_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_source_truth_runtime_gate_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

canary_readiness_plan_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_canary_readiness_plan_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_runbook_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_runbook_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_acceptance_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_acceptance_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_launch_plan_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_launch_plan_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_review_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_review_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_approval_envelope_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_approval_envelope_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_launcher_refusal_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_launcher_refusal_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_executor_contract_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_contract_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_executor_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_executor_dry_run_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_dry_run_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_executor_payload_manifest_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

g2_canary_effectful_executor_review_smoke_manifest="$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_effectful_executor_review_smoke_*/manifest.json 2>/dev/null | head -1 || true)"

shared_ingress_preflight_manifest=""
if [[ -x "$ROOT/scripts/xuan_b27_dplus_shared_ingress_preflight.py" ]]; then
  shared_ingress_preflight_manifest="$("$ROOT/scripts/xuan_b27_dplus_shared_ingress_preflight.py" --soft 2>/dev/null || true)"
fi

auth_source_preflight_manifest=""
if [[ -x "$ROOT/scripts/xuan_b27_dplus_auth_source_preflight.py" ]]; then
  auth_source_preflight_manifest="$("$ROOT/scripts/xuan_b27_dplus_auth_source_preflight.py" 2>/dev/null || true)"
fi

linkage_preview="$ROOT/xuan_research_artifacts/xuan_b27_dplus_no_order_linkage_preview_20260515T0943Z.json"
runbook="$ROOT/docs/research/xuan/DPLUS_AUTH_OBSERVER_DRY_RUN_RUNBOOK_ZH.md"

run_check "latest_safety_smoke_manifest_exists" test -n "$smoke_manifest"
run_check "latest_safety_smoke_passed" python3 - "$smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
raise SystemExit(0 if j.get("status") == "PASS" and j.get("orders_sent") is False else 1)
PY
run_check "latest_summary_gate_smoke_manifest_exists" test -n "$summary_gate_smoke_manifest"
run_check "latest_summary_gate_smoke_passed" python3 - "$summary_gate_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
raise SystemExit(0 if j.get("status") == "PASS" and j.get("orders_sent") is False else 1)
PY
run_check "latest_readonly_user_ws_static_smoke_manifest_exists" test -n "$readonly_user_ws_static_smoke_manifest"
run_check "latest_readonly_user_ws_static_smoke_passed" python3 - "$readonly_user_ws_static_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
raise SystemExit(0 if j.get("status") == "PASS" else 1)
PY
run_check "latest_readonly_user_ws_summary_gate_smoke_manifest_exists" test -n "$readonly_user_ws_summary_gate_smoke_manifest"
run_check "latest_readonly_user_ws_summary_gate_smoke_passed" python3 - "$readonly_user_ws_summary_gate_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_readonly_user_ws_summary_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_readonly_user_ws_wrapper_refusal_smoke_manifest_exists" test -n "$readonly_user_ws_wrapper_refusal_smoke_manifest"
run_check "latest_readonly_user_ws_wrapper_refusal_smoke_passed" python3 - "$readonly_user_ws_wrapper_refusal_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_readonly_user_ws_wrapper_refusal_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_local_status_bundle_smoke_manifest_exists" test -n "$local_status_bundle_smoke_manifest"
run_check "latest_local_status_bundle_smoke_passed" python3 - "$local_status_bundle_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
raise SystemExit(0 if j.get("status") == "PASS" and j.get("scope") == "local_no_network_status_bundle_gate" else 1)
PY
run_check "latest_auth_source_preflight_smoke_manifest_exists" test -n "$auth_source_preflight_smoke_manifest"
run_check "latest_auth_source_preflight_smoke_passed" python3 - "$auth_source_preflight_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_auth_source_fixture_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_shared_ingress_preflight_smoke_manifest_exists" test -n "$shared_ingress_preflight_smoke_manifest"
run_check "latest_shared_ingress_preflight_smoke_passed" python3 - "$shared_ingress_preflight_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_shared_ingress_fixture_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_source_of_truth_schema_smoke_manifest_exists" test -n "$source_of_truth_schema_smoke_manifest"
run_check "latest_source_of_truth_schema_smoke_passed" python3 - "$source_of_truth_schema_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_source_of_truth_schema_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_readonly_attempt_smoke_manifest_exists" test -n "$ec2_readonly_attempt_smoke_manifest"
run_check "latest_ec2_readonly_attempt_smoke_passed" python3 - "$ec2_readonly_attempt_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_readonly_attempt_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_entrypoint_manifest_smoke_manifest_exists" test -n "$ec2_entrypoint_manifest_smoke_manifest"
run_check "latest_ec2_entrypoint_manifest_smoke_passed" python3 - "$ec2_entrypoint_manifest_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_entrypoint_manifest_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_resync_diagnostic_plan_smoke_manifest_exists" test -n "$ec2_resync_diagnostic_plan_smoke_manifest"
run_check "latest_ec2_resync_diagnostic_plan_smoke_passed" python3 - "$ec2_resync_diagnostic_plan_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_resync_diagnostic_plan_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_readonly_diagnostic_smoke_manifest_exists" test -n "$ec2_readonly_diagnostic_smoke_manifest"
run_check "latest_ec2_readonly_diagnostic_smoke_passed" python3 - "$ec2_readonly_diagnostic_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_readonly_diagnostic_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and isinstance(j.get("diagnostic_review"), str)
    and pathlib.Path(j.get("diagnostic_review")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_30m_acceptance_plan_smoke_manifest_exists" test -n "$ec2_30m_acceptance_plan_smoke_manifest"
run_check "latest_ec2_30m_acceptance_plan_smoke_passed" python3 - "$ec2_30m_acceptance_plan_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_30m_acceptance_plan_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and isinstance(j.get("plan_manifest"), str)
    and pathlib.Path(j.get("plan_manifest")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_ec2_30m_acceptance_smoke_manifest_exists" test -n "$ec2_30m_acceptance_smoke_manifest"
run_check "latest_ec2_30m_acceptance_smoke_passed" python3 - "$ec2_30m_acceptance_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_ec2_30m_acceptance_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and isinstance(j.get("acceptance_review"), str)
    and pathlib.Path(j.get("acceptance_review")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_canary_order_plan_smoke_manifest_exists" test -n "$canary_order_plan_smoke_manifest"
run_check "latest_canary_order_plan_smoke_passed" python3 - "$canary_order_plan_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_canary_order_plan_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("planner") == "src/polymarket/xuan_b27_dplus_order_plan.rs"
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_execution_controller_smoke_manifest_exists" test -n "$execution_controller_smoke_manifest"
run_check "latest_execution_controller_smoke_passed" python3 - "$execution_controller_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_execution_controller_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("controller") == "src/polymarket/xuan_b27_dplus_execution_controller.rs"
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_oms_adapter_smoke_manifest_exists" test -n "$oms_adapter_smoke_manifest"
run_check "latest_oms_adapter_smoke_passed" python3 - "$oms_adapter_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_oms_adapter_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("adapter") == "src/polymarket/xuan_b27_dplus_oms_adapter.rs"
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_runtime_wiring_smoke_manifest_exists" test -n "$runtime_wiring_smoke_manifest"
run_check "latest_runtime_wiring_smoke_passed" python3 - "$runtime_wiring_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_runtime_wiring_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("coordinator") == "src/polymarket/coordinator_xuan_b27_dplus.rs"
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_source_truth_runtime_gate_smoke_manifest_exists" test -n "$source_truth_runtime_gate_smoke_manifest"
run_check "latest_source_truth_runtime_gate_smoke_passed" python3 - "$source_truth_runtime_gate_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_source_truth_runtime_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("coordinator") == "src/polymarket/coordinator_xuan_b27_dplus.rs"
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_canary_readiness_plan_smoke_manifest_exists" test -n "$canary_readiness_plan_smoke_manifest"
run_check "latest_canary_readiness_plan_smoke_passed" python3 - "$canary_readiness_plan_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_canary_readiness_plan_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and isinstance(j.get("plan_manifest"), str)
    and pathlib.Path(j.get("plan_manifest")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_runbook_smoke_manifest_exists" test -n "$g2_canary_runbook_smoke_manifest"
run_check "latest_g2_canary_runbook_smoke_passed" python3 - "$g2_canary_runbook_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_runbook_gate"
    and j.get("exact_approval_required") is True
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and isinstance(j.get("runbook"), str)
    and pathlib.Path(j.get("runbook")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_acceptance_smoke_manifest_exists" test -n "$g2_canary_acceptance_smoke_manifest"
run_check "latest_g2_canary_acceptance_smoke_passed" python3 - "$g2_canary_acceptance_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_acceptance_fixture_gate"
    and j.get("exact_approval_required") is True
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and isinstance(j.get("fixture_results"), str)
    and pathlib.Path(j.get("fixture_results")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_launch_plan_smoke_manifest_exists" test -n "$g2_canary_launch_plan_smoke_manifest"
run_check "latest_g2_canary_launch_plan_smoke_passed" python3 - "$g2_canary_launch_plan_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_launch_plan_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and isinstance(j.get("plan_manifest"), str)
    and pathlib.Path(j.get("plan_manifest")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_review_smoke_manifest_exists" test -n "$g2_canary_review_smoke_manifest"
run_check "latest_g2_canary_review_smoke_passed" python3 - "$g2_canary_review_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_review_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("summarizer") == "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
    and pathlib.Path(j.get("summarizer")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_approval_envelope_smoke_manifest_exists" test -n "$g2_canary_approval_envelope_smoke_manifest"
run_check "latest_g2_canary_approval_envelope_smoke_passed" python3 - "$g2_canary_approval_envelope_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_approval_envelope_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("verifier") == "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py"
    and pathlib.Path(j.get("verifier")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_launcher_refusal_smoke_manifest_exists" test -n "$g2_canary_launcher_refusal_smoke_manifest"
run_check "latest_g2_canary_launcher_refusal_smoke_passed" python3 - "$g2_canary_launcher_refusal_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_launcher_refusal_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("launcher") == "scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py"
    and pathlib.Path(j.get("launcher")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_executor_contract_smoke_manifest_exists" test -n "$g2_canary_executor_contract_smoke_manifest"
run_check "latest_g2_canary_executor_contract_smoke_passed" python3 - "$g2_canary_executor_contract_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_executor_contract_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and isinstance(j.get("contract_manifest"), str)
    and pathlib.Path(j.get("contract_manifest")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_executor_smoke_manifest_exists" test -n "$g2_canary_executor_smoke_manifest"
run_check "latest_g2_canary_executor_smoke_passed" python3 - "$g2_canary_executor_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_executor_preflight_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor.py"
    and pathlib.Path(j.get("executor")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_executor_dry_run_smoke_manifest_exists" test -n "$g2_canary_executor_dry_run_smoke_manifest"
run_check "latest_g2_canary_executor_dry_run_smoke_passed" python3 - "$g2_canary_executor_dry_run_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_executor_dry_run_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py"
    and pathlib.Path(j.get("executor")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_executor_payload_manifest_smoke_manifest_exists" test -n "$g2_canary_executor_payload_manifest_smoke_manifest"
run_check "latest_g2_canary_executor_payload_manifest_smoke_passed" python3 - "$g2_canary_executor_payload_manifest_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_executor_payload_manifest_gate"
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and isinstance(j.get("payload_manifest"), str)
    and pathlib.Path(j.get("payload_manifest")).exists()
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_g2_canary_effectful_executor_review_smoke_manifest_exists" test -n "$g2_canary_effectful_executor_review_smoke_manifest"
run_check "latest_g2_canary_effectful_executor_review_smoke_passed" python3 - "$g2_canary_effectful_executor_review_smoke_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "PASS"
    and j.get("scope") == "local_no_network_g2_canary_effectful_executor_review_gate"
    and j.get("orders_sent") is False
    and j.get("cancels_sent") is False
    and j.get("redeems_sent") is False
    and j.get("auth_network_started") is False
    and j.get("started_canary") is False
    and j.get("reviewer") == "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py"
    and pathlib.Path(j.get("reviewer")).exists()
    and j.get("real_effectful_executor_review_status") == "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and j.get("real_effectful_executor_implemented") is False
    and j.get("real_review_passed") is False
    and j.get("fixture_pass_artifact_published") is False
)
raise SystemExit(0 if ok else 1)
PY

run_check "linkage_preview_exists" test -f "$linkage_preview"
run_check "linkage_preview_is_no_order" python3 - "$linkage_preview" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
chain = j.get("example_candidate_chain", {})
preview = chain.get("order_attempt_trace_preview", {})
ok = (
    j.get("orders_sent") is False
    and j.get("cancels_sent") is False
    and j.get("redeems_sent") is False
    and j.get("auth_network_started") is False
    and preview.get("preview_only") is True
    and preview.get("submitted") is False
    and preview.get("venue_order_id") is None
    and bool(preview.get("candidate_id"))
    and bool(preview.get("order_attempt_id"))
)
raise SystemExit(0 if ok else 1)
PY

run_check "runbook_exists" test -f "$runbook"
run_check "runbook_requires_dry_run" rg -q 'PM_DRY_RUN=true' "$runbook"
run_check "runbook_requires_auth_observer_mode" rg -q 'PM_XUAN_B27_DPLUS_MODE=auth_observer' "$runbook"
run_check "runbook_blocks_live_orders" rg -q 'PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=0' "$runbook"
run_check "runbook_has_stop_conditions" rg -q '立即停止条件' "$runbook"
run_check "runbook_requires_summary_no_order_gate" rg -q -- '--check-no-order' "$runbook"
run_check "runbook_requires_summary_candidate_gate" rg -q -- '--require-tracked-candidates' "$runbook"
run_check "runbook_documents_summary_gate_exit_codes" rg -q 'exit 0.*exit 2.*exit 3|exit 0' "$runbook"
run_check "approved_wrapper_exists" test -x scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_requires_explicit_flag" rg -q -- '--approved-no-order-observer' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_forces_dry_run" rg -q '"PM_DRY_RUN": "true"' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_blocks_live_orders" rg -q '"PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": "0"' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_builds_current_binary" rg -q '"cargo", "build", "--bin", "polymarket_v2"' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_records_binary_metadata" rg -q 'binary_mtime_ns' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_readonly_user_ws_requires_extra_flag" rg -q -- '--approved-readonly-user-ws' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_disables_user_ws_by_default" rg -q '"PM_XUAN_B27_DPLUS_USER_WS_IN_DRY_RUN": "true".*if args.approved_readonly_user_ws.*else "false"|approved_readonly_user_ws' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_preflights_readonly_user_ws_auth_source" rg -q 'READONLY_USER_WS_CREDENTIALS_PARTIAL|READONLY_USER_WS_AUTH_SOURCE_MISSING|POLYMARKET_PRIVATE_KEY' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_loads_local_dotenv" rg -q 'load_dotenv_into_env|dotenv_loaded' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_fails_nonzero_process_exit" rg -q 'FAIL_PROCESS_EXIT_NONZERO' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_forces_single_market_prefix_scope" rg -q '"PM_MULTI_MARKET_PREFIXES": args.market_slug|--require-market-prefix' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_defaults_to_shared_ingress_client" rg -q 'shared_ingress_role = "standalone" if args.allow_standalone_market_ws else "client"' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_requires_existing_shared_ingress_broker" rg -q 'SHARED_INGRESS_BROKER_UNAVAILABLE|broker_manifest.json' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "approved_wrapper_runs_summary_gate" rg -q -- '--check-no-order.*--require-tracked-candidates|--require-tracked-candidates' scripts/xuan_b27_dplus_auth_observer_dry_run.py
run_check "summary_gate_rejects_wrong_market_prefix" rg -q -- '--require-market-prefix|bad_markets' scripts/xuan_b27_dplus_summarize_observer_run.py
run_check "observer_preview_field_exists" rg -q 'order_attempt_trace_preview' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "observer_preview_not_submitted" rg -q '"submitted": false' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "observer_force_no_orders" rg -q '"orders_sent_by_this_module": false' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "observer_has_no_positive_order_flags" bash -c "! rg -q 'orders_sent_by_this_module\": true|would_place\": true' src/polymarket/coordinator_xuan_b27_dplus.rs"
run_check "executor_order_accepted_has_correlation" rg -q '"correlation": order_attempt_trace_payload' src/polymarket/executor.rs
run_check "executor_order_accepted_records_venue_order_id" rg -q '"venue_order_id": order_id\.clone\(\)' src/polymarket/executor.rs
run_check "user_ws_fill_truth_event_exists" rg -q 'user_ws_fill_parsed' src/polymarket/user_ws.rs
run_check "redeem_result_recorder_event_exists" rg -q 'emit_redeem_result' src/polymarket/recorder.rs
run_check "redeem_cashflow_correlation_fields_exist" rg -q 'redeem_attempt_id.*redeem_tx_hash.*cashflow_snapshot_id|cashflow_snapshot_id' src/polymarket/xuan_b27_dplus_correlation.rs
run_check "source_of_truth_schema_doc_exists" test -f docs/research/xuan/DPLUS_SOURCE_OF_TRUTH_SCHEMA_ZH.md
run_check "source_of_truth_schema_smoke_exists" test -x scripts/xuan_b27_dplus_source_of_truth_schema_smoke.sh
run_check "strategy_file_has_no_order_intent_surface" bash -c "! rg -q 'StrategyIntent|yes_buy|no_buy|yes_sell|no_sell' src/polymarket/strategy/xuan_b27_dplus.rs"
run_check "bin_readonly_user_ws_guard_is_dplus_auth_observer_only" rg -q 'PM_XUAN_B27_DPLUS_USER_WS_IN_DRY_RUN.*xuan_b27_dplus auth_observer dry-run|xuan_dplus_readonly_user_ws_in_dry_run' src/bin/polymarket_v2.rs
run_check "bin_readonly_user_ws_can_derive_credentials" rg -q 'deriving read-only User WS credentials from private key|could not derive CLOB/User WS API credentials' src/bin/polymarket_v2.rs
run_check "standalone_readonly_user_ws_bin_registered" rg -q 'name = "xuan_b27_dplus_user_ws_observer"' Cargo.toml
run_check "standalone_readonly_user_ws_requires_approval" rg -q -- '--approved-readonly-user-ws|REFUSED_NO_EXPLICIT_APPROVAL' src/bin/xuan_b27_dplus_user_ws_observer.rs
run_check "standalone_readonly_user_ws_requires_shared_ingress" rg -q 'SHARED_INGRESS_BROKER_UNAVAILABLE|check_shared_ingress' src/bin/xuan_b27_dplus_user_ws_observer.rs
run_check "standalone_readonly_user_ws_has_no_execution_actor_imports" bash -c "! rg -q 'use .*Executor|use .*OrderManager|use .*InventoryManager|ExecutionCmd|OrderManagerCmd|InventoryEvent' src/bin/xuan_b27_dplus_user_ws_observer.rs"
run_check "standalone_readonly_user_ws_has_no_order_calls" bash -c "! rg -q 'post_order|create_order|CancelAll|execute_market_merge|maybe_auto_claim|run_auto_claim_once' src/bin/xuan_b27_dplus_user_ws_observer.rs"
run_check "standalone_readonly_user_ws_wrapper_exists" test -x scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "standalone_readonly_user_ws_wrapper_refusal_smoke_exists" test -x scripts/xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke.sh
run_check "standalone_readonly_user_ws_wrapper_builds_current_binary" rg -q '"cargo", "build", "--bin", "xuan_b27_dplus_user_ws_observer"' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "standalone_readonly_user_ws_wrapper_requires_approval" rg -q -- '--approved-readonly-user-ws|REFUSED_NO_EXPLICIT_APPROVAL' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "standalone_readonly_user_ws_wrapper_forces_shared_ingress_client" rg -q '"PM_SHARED_INGRESS_ROLE": "client"' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "standalone_readonly_user_ws_wrapper_has_no_standalone_escape_hatch" bash -c "! rg -q 'allow-standalone|standalone public|standalone market' scripts/xuan_b27_dplus_readonly_user_ws_observer.py"
run_check "standalone_readonly_user_ws_wrapper_runs_summary_gate" rg -q 'xuan_b27_dplus_summarize_readonly_user_ws_run.py|--check-readonly|--require-user-ws-connection' scripts/xuan_b27_dplus_readonly_user_ws_observer.py
run_check "standalone_readonly_user_ws_summarizer_exists" test -x scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "standalone_readonly_user_ws_summarizer_has_gates" rg -q -- '--check-readonly|--require-user-ws-records|--require-user-ws-connection|FAIL_READONLY_VIOLATION' scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "standalone_readonly_user_ws_summary_gate_smoke_exists" test -x scripts/xuan_b27_dplus_readonly_user_ws_summary_gate_smoke.sh
run_check "shared_ingress_preflight_exists" test -x scripts/xuan_b27_dplus_shared_ingress_preflight.py
run_check "shared_ingress_preflight_is_readonly" bash -c "! rg -q 'systemctl|run_shared_ingress|PM_SHARED_INGRESS_ROLE.*broker|subprocess|socket\\.connect|rsync|scp|ssh' scripts/xuan_b27_dplus_shared_ingress_preflight.py"
run_check "shared_ingress_preflight_manifest_written" test -n "$shared_ingress_preflight_manifest"
run_check "shared_ingress_preflight_smoke_exists" test -x scripts/xuan_b27_dplus_shared_ingress_preflight_smoke.sh
run_check "auth_source_preflight_exists" test -x scripts/xuan_b27_dplus_auth_source_preflight.py
run_check "auth_source_preflight_has_no_network_or_secret_output" bash -c "! rg -q 'requests|websocket|socket|subprocess|init_clob|print\\(.*env|ExposeSecret|API_SECRET.*value|PRIVATE_KEY.*value' scripts/xuan_b27_dplus_auth_source_preflight.py"
run_check "auth_source_preflight_smoke_exists" test -x scripts/xuan_b27_dplus_auth_source_preflight_smoke.sh
run_check "auth_source_preflight_manifest_written" test -n "$auth_source_preflight_manifest"
run_check "auth_source_preflight_ok" python3 - "$auth_source_preflight_manifest" <<'PY'
import json, pathlib, sys
if len(sys.argv) < 2 or not sys.argv[1]:
    raise SystemExit(1)
p = pathlib.Path(sys.argv[1])
if not p.exists():
    raise SystemExit(1)
j = json.loads(p.read_text())
ok = (
    j.get("status") == "OK"
    and j.get("auth_source") in {"explicit_api_credentials", "derive_from_private_key"}
    and j.get("secrets_redacted") is True
    and j.get("orders_sent") is False
    and j.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "local_status_bundle_exists" test -x scripts/xuan_b27_dplus_local_status_bundle.py
run_check "local_status_bundle_smoke_exists" test -x scripts/xuan_b27_dplus_local_status_bundle_smoke.sh

cat > "$out_dir/manifest.json" <<EOF
{
  "schema_version": 1,
  "artifact": "xuan_b27_dplus_auth_observer_readiness_check",
  "status": "$status",
  "created_utc": "$ts",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_auth_observer_readiness",
  "orders_sent": false,
  "auth_network_started": false,
  "latest_safety_smoke_manifest": "$smoke_manifest",
  "latest_summary_gate_smoke_manifest": "$summary_gate_smoke_manifest",
  "latest_readonly_user_ws_static_smoke_manifest": "$readonly_user_ws_static_smoke_manifest",
  "latest_readonly_user_ws_summary_gate_smoke_manifest": "$readonly_user_ws_summary_gate_smoke_manifest",
  "latest_readonly_user_ws_wrapper_refusal_smoke_manifest": "$readonly_user_ws_wrapper_refusal_smoke_manifest",
  "latest_local_status_bundle_smoke_manifest": "$local_status_bundle_smoke_manifest",
  "latest_auth_source_preflight_smoke_manifest": "$auth_source_preflight_smoke_manifest",
  "latest_shared_ingress_preflight_smoke_manifest": "$shared_ingress_preflight_smoke_manifest",
  "latest_source_of_truth_schema_smoke_manifest": "$source_of_truth_schema_smoke_manifest",
  "latest_ec2_readonly_attempt_smoke_manifest": "$ec2_readonly_attempt_smoke_manifest",
  "latest_ec2_entrypoint_manifest_smoke_manifest": "$ec2_entrypoint_manifest_smoke_manifest",
  "latest_ec2_resync_diagnostic_plan_smoke_manifest": "$ec2_resync_diagnostic_plan_smoke_manifest",
  "latest_ec2_readonly_diagnostic_smoke_manifest": "$ec2_readonly_diagnostic_smoke_manifest",
  "latest_ec2_30m_acceptance_plan_smoke_manifest": "$ec2_30m_acceptance_plan_smoke_manifest",
  "latest_ec2_30m_acceptance_smoke_manifest": "$ec2_30m_acceptance_smoke_manifest",
  "latest_canary_order_plan_smoke_manifest": "$canary_order_plan_smoke_manifest",
  "latest_execution_controller_smoke_manifest": "$execution_controller_smoke_manifest",
  "latest_oms_adapter_smoke_manifest": "$oms_adapter_smoke_manifest",
  "latest_runtime_wiring_smoke_manifest": "$runtime_wiring_smoke_manifest",
  "latest_source_truth_runtime_gate_smoke_manifest": "$source_truth_runtime_gate_smoke_manifest",
  "latest_canary_readiness_plan_smoke_manifest": "$canary_readiness_plan_smoke_manifest",
  "latest_g2_canary_runbook_smoke_manifest": "$g2_canary_runbook_smoke_manifest",
  "latest_g2_canary_acceptance_smoke_manifest": "$g2_canary_acceptance_smoke_manifest",
  "latest_g2_canary_launch_plan_smoke_manifest": "$g2_canary_launch_plan_smoke_manifest",
  "latest_g2_canary_review_smoke_manifest": "$g2_canary_review_smoke_manifest",
  "latest_g2_canary_approval_envelope_smoke_manifest": "$g2_canary_approval_envelope_smoke_manifest",
  "latest_g2_canary_launcher_refusal_smoke_manifest": "$g2_canary_launcher_refusal_smoke_manifest",
  "latest_g2_canary_executor_contract_smoke_manifest": "$g2_canary_executor_contract_smoke_manifest",
  "latest_g2_canary_executor_smoke_manifest": "$g2_canary_executor_smoke_manifest",
  "latest_g2_canary_executor_dry_run_smoke_manifest": "$g2_canary_executor_dry_run_smoke_manifest",
  "latest_g2_canary_executor_payload_manifest_smoke_manifest": "$g2_canary_executor_payload_manifest_smoke_manifest",
  "latest_g2_canary_effectful_executor_review_smoke_manifest": "$g2_canary_effectful_executor_review_smoke_manifest",
  "latest_shared_ingress_preflight_manifest": "$shared_ingress_preflight_manifest",
  "latest_auth_source_preflight_manifest": "$auth_source_preflight_manifest",
  "linkage_preview_artifact": "$linkage_preview",
  "runbook": "$runbook",
  "checks_file": "$checks",
  "next_gate": "requires_healthy_shared_ingress_broker_and_explicit_user_approval_before_auth_observer_dry_run"
}
EOF

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "READY_FOR_APPROVAL" ]]; then
  exit 1
fi
