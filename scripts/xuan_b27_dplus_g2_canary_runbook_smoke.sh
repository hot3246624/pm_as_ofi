#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_runbook_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
runbook="docs/research/xuan/DPLUS_G2_CANARY_RUNBOOK_ZH.md"
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

latest_manifest() {
  local pattern="$1"
  local artifact="$2"
  python3 - "$ROOT" "$pattern" "$artifact" <<'PY'
import glob
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
pattern = sys.argv[2]
artifact = sys.argv[3]
matches = []
for raw in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json")):
    path = pathlib.Path(raw)
    try:
        data = json.loads(path.read_text())
    except Exception:
        continue
    if data.get("artifact") == artifact:
        matches.append(path)
if matches:
    print(max(matches, key=lambda p: p.stat().st_mtime_ns))
PY
}

acceptance_smoke_manifest="$(latest_manifest 'xuan_b27_dplus_ec2_30m_acceptance_smoke_*' 'xuan_b27_dplus_ec2_30m_acceptance_smoke')"
source_truth_runtime_gate_manifest="$(latest_manifest 'xuan_b27_dplus_source_truth_runtime_gate_smoke_*' 'xuan_b27_dplus_source_truth_runtime_gate_smoke')"
runtime_wiring_manifest="$(latest_manifest 'xuan_b27_dplus_runtime_wiring_smoke_*' 'xuan_b27_dplus_runtime_wiring_smoke')"
oms_adapter_manifest="$(latest_manifest 'xuan_b27_dplus_oms_adapter_smoke_*' 'xuan_b27_dplus_oms_adapter_smoke')"

run_check "runbook_exists" test -f "$runbook"
run_check "runbook_is_not_authorization" rg -q '不是授权' "$runbook"
run_check "runbook_names_exact_strategy" rg -q 'xuan_b27_dplus' "$runbook"
run_check "runbook_names_exact_market" rg -q 'btc-updown-5m' "$runbook"
run_check "runbook_requires_shared_ingress_client" rg -q 'PM_SHARED_INGRESS_ROLE=client' "$runbook"
run_check "runbook_requires_ec2_shared_ingress_root" rg -q '/srv/pm_as_ofi/shared-ingress-main' "$runbook"
run_check "runbook_requires_explicit_canary_approval" rg -q 'PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL=true' "$runbook"
run_check "runbook_requires_runtime_wiring" rg -q 'PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED=true' "$runbook"
run_check "runbook_requires_oms_adapter" rg -q 'PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED=true' "$runbook"
run_check "runbook_caps_open_cost" rg -q 'PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC=50' "$runbook"
run_check "runbook_caps_live_orders" rg -q 'PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=2' "$runbook"
run_check "runbook_caps_target_qty" rg -q 'PM_XUAN_B27_DPLUS_TARGET_QTY=5' "$runbook"
run_check "runbook_caps_strategy_exposure" rg -q 'PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC=100' "$runbook"
run_check "runbook_forces_post_only" rg -q 'PM_XUAN_B27_DPLUS_POST_ONLY=true' "$runbook"
run_check "runbook_forbids_passive_taker" rg -q 'PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER=false' "$runbook"
run_check "runbook_requires_stop_on_unknown" rg -q 'PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN=true' "$runbook"
run_check "runbook_has_stop_conditions" rg -q 'Stop Conditions|UNKNOWN/FAIL|unexpected taker fill|recorder critical drop' "$runbook"
run_check "runbook_has_acceptance_artifact_contract" rg -q 'Acceptance Artifact|exact_approval_scope=true|source_truth_all_pass=true' "$runbook"
run_check "runbook_documents_no_redeem_without_separate_approval" rg -q 'redeem/claim 需要单独 exact approval|未被 exact approval 覆盖的 redeem/claim' "$runbook"
run_check "runbook_has_no_runnable_shell_invocation" bash -c "! rg -q 'cargo run|python3 .*observer|ssh |scp |rsync |systemctl|service ' '$runbook'"

run_check "latest_30m_readonly_acceptance_smoke_exists" test -n "$acceptance_smoke_manifest"
run_check "latest_30m_readonly_acceptance_smoke_passed" python3 - "$acceptance_smoke_manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
ok = (
    data.get("status") == "PASS"
    and data.get("scope") == "local_no_network_ec2_30m_acceptance_gate"
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_source_truth_runtime_gate_smoke_exists" test -n "$source_truth_runtime_gate_manifest"
run_check "latest_source_truth_runtime_gate_smoke_passed" python3 - "$source_truth_runtime_gate_manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
ok = (
    data.get("status") == "PASS"
    and data.get("scope") == "local_no_network_source_truth_runtime_gate"
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and data.get("wallet_redeem_cashflow_runtime_producers_wired") is True
    and data.get("redeem_truth_can_pass_from_runtime_producer") is True
)
raise SystemExit(0 if ok else 1)
PY
run_check "latest_runtime_wiring_smoke_passed" python3 - "$runtime_wiring_manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
ok = data.get("status") == "PASS" and data.get("orders_sent") is False
raise SystemExit(0 if ok else 1)
PY
run_check "latest_oms_adapter_smoke_passed" python3 - "$oms_adapter_manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
ok = data.get("status") == "PASS" and data.get("orders_sent") is False
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_runbook_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_runbook_gate",
  "runbook": "$ROOT/$runbook",
  "exact_approval_required": true,
  "g2_caps": {
    "market_slug": "btc-updown-5m",
    "max_active_markets": 1,
    "max_live_orders": 2,
    "target_qty": 5,
    "max_open_cost_usdc": 50,
    "max_strategy_exposure_usdc": 100,
    "post_only": true,
    "allow_passive_taker": false,
    "stop_on_unknown": true
  },
  "latest_30m_readonly_acceptance_smoke_manifest": "$acceptance_smoke_manifest",
  "latest_source_truth_runtime_gate_smoke_manifest": "$source_truth_runtime_gate_manifest",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "started_canary": false,
  "started_observer": false,
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
