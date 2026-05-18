#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_dry_run_smoke_$ts}"
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

run_dry_run_case() {
  local name="$1"
  local expected_rc="$2"
  shift 2
  local case_dir="$out_dir/$name"
  local rc=0
  scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py --output-dir "$case_dir" "$@" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$case_dir/manifest.json"
}

python3 - "$fixtures" <<'PY'
from __future__ import annotations

import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
review_artifact = root / "valid_effectful_executor_review.json"
review_artifact.write_text(json.dumps({
    "artifact": "xuan_b27_dplus_g2_canary_effectful_executor_review",
    "status": "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION",
    "strategy": "xuan_b27_dplus",
    "scope": "local_no_network_g2_canary_effectful_executor_review",
    "review_passed": True,
    "fixture_review": False,
    "effectful_executor_implemented": True,
    "reviewed_effectful_executor_implementation": True,
    "canary_run_authorized": False,
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "auth_network_started": False,
    "started_canary": False,
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "started_canary": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "broker_modified": False,
        "service_control_called": False,
    },
}, sort_keys=True) + "\n")
base = {
    "artifact": "xuan_b27_dplus_g2_canary_approval_envelope",
    "approval_scope": "exact_g2_canary_sync_rebuild_and_run",
    "explicit_current_conversation_approval": True,
    "heartbeat_or_generic_approval": False,
    "strategy": "xuan_b27_dplus",
    "run": {
        "run_class": "G2_READ_WRITE_CANARY_SMOKE",
        "market_slug": "btc-updown-5m",
        "shared_ingress_role": "client",
        "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main",
        "remote_worktree": "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree",
        "remote_run_dir": "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z",
        "duration_seconds": 1800,
        "max_rounds": 6,
        "sync_rebuild_approved": True,
        "canary_run_approved": True,
    },
    "risk": {
        "target_qty": 5,
        "max_live_orders": 2,
        "max_open_cost_usdc": 50,
        "max_strategy_exposure_usdc": 100,
        "max_active_markets": 1,
        "post_only": True,
        "allow_passive_taker": False,
        "stop_on_unknown": True,
    },
    "allowed_side_effects": {
        "capped_post_only_orders": True,
        "bounded_own_order_cancels": True,
        "redeem_or_claim": False,
        "broker_control": False,
        "service_control": False,
        "shared_ingress_write": False,
        "env_write": False,
    },
    "forbidden_side_effects": {
        "systemd_or_service_control": True,
        "broker_start_stop_repair": True,
        "shared_ingress_modification": True,
        "remote_env_file_write": True,
        "raw_replay_scan_or_write": True,
        "unbounded_live_trading": True,
    },
    "post_run_review": {
        "summarizer": "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
        "require_check_acceptance": True,
        "require_secret_sentinel_scan": True,
    },
    "executor_review": {
        "reviewed_effectful_executor_implementation": True,
        "review_status": "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION",
        "review_artifact": str(review_artifact),
        "require_current_payload_allowlist_no_drift": True,
        "require_not_heartbeat_or_generic_approval": True,
    },
}
valid = copy.deepcopy(base)
invalid = copy.deepcopy(base)
invalid["run"]["remote_worktree"] = "/srv/pm_as_ofi/not-isolated"
(root / "valid_envelope.json").write_text(json.dumps(valid, sort_keys=True) + "\n")
(root / "invalid_envelope.json").write_text(json.dumps(invalid, sort_keys=True) + "\n")
PY

payload_fixture_dir="$fixtures/payload_manifest_run"
scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py --output-dir "$payload_fixture_dir" >> "$log" 2>&1
python3 - "$payload_fixture_dir/manifest.json" "$fixtures/stale_payload_manifest.json" <<'PY'
import copy
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
dest = pathlib.Path(sys.argv[2])
data = json.loads(source.read_text())
stale = copy.deepcopy(data)
stale["payload_records"][0]["sha256"] = "0" * 64
dest.write_text(json.dumps(stale, sort_keys=True) + "\n")
PY

script="scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py"

run_check "executor_dry_run_exists" test -x "$script"
run_check "executor_dry_run_py_compile" python3 -m py_compile "$script"
run_check "executor_dry_run_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "executor_dry_run_contract_smoke_exists" test -f "$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_contract_smoke_*/manifest.json 2>/dev/null | head -1)"
run_check "executor_dry_run_payload_manifest_smoke_exists" test -f "$(ls -1t "$ROOT"/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_*/manifest.json 2>/dev/null | head -1)"
run_check "missing_explicit_flag_refuses" run_dry_run_case missing_flag 64 --approval-envelope "$fixtures/valid_envelope.json"
run_check "missing_envelope_refuses" run_dry_run_case missing_envelope 64 --approved-g2-canary-sync-and-run
run_check "invalid_envelope_refuses" run_dry_run_case invalid_envelope 65 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/invalid_envelope.json"
run_check "stale_payload_manifest_refuses" run_dry_run_case stale_payload_manifest 68 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/valid_envelope.json" --payload-manifest "$fixtures/stale_payload_manifest.json"
run_check "valid_envelope_refuses_before_execution" run_dry_run_case valid_envelope 75 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/valid_envelope.json"
run_check "valid_manifest_contract" python3 - "$out_dir/valid_envelope/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
approval = data.get("approval_summary") or {}
contract = data.get("executor_contract") or {}
payload = data.get("payload_allowlist") or {}
payload_current = data.get("payload_current_check") or {}
phase_plan = data.get("phase_plan") or []
allowed = data.get("allowed_side_effects_after_exact_executor_approval") or {}
forbidden = data.get("forbidden_side_effects_even_after_exact_executor_approval") or {}
aborts = data.get("mandatory_abort_conditions") or []
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_executor_dry_run"
    and data.get("status") == "REFUSED_DRY_RUN_ONLY_EXECUTOR"
    and data.get("scope") == "local_no_network_g2_canary_executor_dry_run"
    and data.get("executor_implemented") is False
    and data.get("dry_run_only") is True
    and data.get("canary_run_authorized") is False
    and approval.get("status") == "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and contract.get("status") == "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION"
    and contract.get("inputs_ok") is True
    and contract.get("executor_implemented") is False
    and payload.get("status") == "PASS"
    and payload.get("scope") == "local_no_network_g2_canary_executor_payload_manifest"
    and isinstance(payload.get("payload_file_count"), int)
    and payload.get("payload_file_count") >= 40
    and payload.get("missing_required_files") == []
    and payload.get("missing_executable_files") == []
    and payload.get("forbidden_path_hits") == []
    and payload.get("duplicate_paths") == []
    and payload.get("requires_reviewed_executor_implementation") is True
    and payload.get("requires_exact_g2_canary_approval_before_sync") is True
    and payload_current.get("status") == "PASS"
    and payload_current.get("drift_count") == 0
    and payload_current.get("drift_sample") == []
    and data.get("approval_run_paths", {}).get("remote_worktree_isolated") is True
    and data.get("approval_run_paths", {}).get("remote_run_dir_isolated") is True
    and "approval_envelope_missing_or_invalid" in aborts
    and "payload_allowlist_missing_or_invalid" in aborts
    and "payload_allowlist_drift_detected" in aborts
    and "any_forbidden_side_effect_detected" in aborts
    and allowed.get("start_single_bounded_canary") is True
    and allowed.get("capped_post_only_orders") is True
    and forbidden.get("redeem_or_claim") is True
    and any(
        item.get("phase") == "load_reviewed_payload_allowlist_manifest"
        and item.get("dry_run_status") == "VALIDATED_CONTRACT_ONLY"
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "start_single_bounded_g2_canary_process"
        and item.get("dry_run_status") == "REFUSED_BEFORE_SIDE_EFFECT"
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and data.get("orders_sent") is False
    and data.get("cancels_sent") is False
    and data.get("redeems_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY
run_check "stale_payload_manifest_contract" python3 - "$out_dir/stale_payload_manifest/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
payload = data.get("payload_allowlist") or {}
payload_current = data.get("payload_current_check") or {}
ok = (
    data.get("status") == "REFUSED_PAYLOAD_ALLOWLIST_DRIFT"
    and payload.get("status") == "PASS"
    and payload.get("override_path")
    and payload_current.get("status") == "FAIL"
    and payload_current.get("drift_count", 0) >= 1
    and any(
        row.get("path") == "Cargo.toml" and row.get("reason") == "sha256_mismatch"
        for row in (payload_current.get("drift_sample") or [])
    )
    and data.get("orders_sent") is False
    and data.get("started_canary") is False
)
raise SystemExit(0 if ok else 1)
PY
run_check "invalid_manifest_contract" python3 - "$out_dir/invalid_envelope/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
approval = data.get("approval_summary") or {}
ok = (
    data.get("status") == "REFUSED_INVALID_APPROVAL_ENVELOPE"
    and approval.get("status") == "FAIL_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and "remote_worktree_not_isolated" in (approval.get("failures") or [])
    and data.get("orders_sent") is False
    and data.get("started_canary") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_executor_dry_run_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_executor_dry_run_gate",
  "executor": "$script",
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
