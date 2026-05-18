#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_approval_envelope_smoke_$ts}"
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

run_case() {
  local name="$1"
  local expected_rc="$2"
  local input="$fixtures/$name.json"
  local output="$out_dir/${name}_summary.json"
  local rc=0
  scripts/xuan_b27_dplus_g2_canary_approval_envelope.py "$input" --output "$output" --check >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$output"
}

python3 - "$fixtures" <<'PY'
from __future__ import annotations

import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
review_artifact = root / "valid_effectful_executor_review.json"
valid_review = {
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
}
review_artifact.write_text(json.dumps(valid_review, sort_keys=True) + "\n")

fixture_review_artifact = root / "fixture_effectful_executor_review.json"
fixture_review = copy.deepcopy(valid_review)
fixture_review["fixture_review"] = True
fixture_review_artifact.write_text(json.dumps(fixture_review, sort_keys=True) + "\n")

BASE = {
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


def write_case(name: str, mutate) -> None:
    data = copy.deepcopy(BASE)
    mutate(data)
    (root / f"{name}.json").write_text(json.dumps(data, sort_keys=True) + "\n")


write_case("pass_exact", lambda data: None)
write_case("fail_generic_approval", lambda data: data.update({
    "explicit_current_conversation_approval": False,
    "heartbeat_or_generic_approval": True,
}))
write_case("fail_wrong_market", lambda data: data["run"].update({"market_slug": "eth-updown-5m"}))
write_case("fail_cap", lambda data: data["risk"].update({"max_live_orders": 3}))
write_case("fail_remote_path", lambda data: data["run"].update({"remote_worktree": "/srv/pm_as_ofi/repo"}))
write_case("fail_redeem_allowed", lambda data: data["allowed_side_effects"].update({"redeem_or_claim": True}))
write_case("fail_env_write_allowed", lambda data: data["allowed_side_effects"].update({"env_write": True}))
write_case("fail_missing_review_scan", lambda data: data["post_run_review"].update({"require_secret_sentinel_scan": False}))
write_case("fail_missing_executor_review", lambda data: data.pop("executor_review"))
write_case("fail_bad_executor_review_status", lambda data: data["executor_review"].update({"review_status": "PENDING"}))
write_case("fail_missing_executor_review_artifact", lambda data: data["executor_review"].update({"review_artifact": str(root / "missing_review.json")}))
write_case("fail_fixture_executor_review_artifact", lambda data: data["executor_review"].update({"review_artifact": str(fixture_review_artifact)}))
PY

script="scripts/xuan_b27_dplus_g2_canary_approval_envelope.py"

run_check "verifier_exists" test -x "$script"
run_check "verifier_py_compile" python3 -m py_compile "$script"
run_check "verifier_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "pass_exact_accepts" run_case pass_exact 0
run_check "generic_approval_rejects" run_case fail_generic_approval 2
run_check "wrong_market_rejects" run_case fail_wrong_market 2
run_check "cap_violation_rejects" run_case fail_cap 2
run_check "remote_path_rejects" run_case fail_remote_path 2
run_check "redeem_allowed_rejects" run_case fail_redeem_allowed 2
run_check "env_write_allowed_rejects" run_case fail_env_write_allowed 2
run_check "missing_review_scan_rejects" run_case fail_missing_review_scan 2
run_check "missing_executor_review_rejects" run_case fail_missing_executor_review 2
run_check "bad_executor_review_status_rejects" run_case fail_bad_executor_review_status 2
run_check "missing_executor_review_artifact_rejects" run_case fail_missing_executor_review_artifact 2
run_check "fixture_executor_review_artifact_rejects" run_case fail_fixture_executor_review_artifact 2
run_check "pass_summary_contract" python3 - "$out_dir/pass_exact_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_approval_envelope_summary"
    and data.get("status") == "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and data.get("failures") == []
    and data.get("approval_scope") == "exact_g2_canary_sync_rebuild_and_run"
    and data.get("explicit_current_conversation_approval") is True
    and data.get("heartbeat_or_generic_approval") is False
    and data.get("run_class") == "G2_READ_WRITE_CANARY_SMOKE"
    and data.get("market_slug") == "btc-updown-5m"
    and data.get("reviewed_effectful_executor_implementation") is True
    and data.get("effectful_executor_review_status") == "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION"
    and isinstance(data.get("effectful_executor_review_artifact"), str)
    and all(value is False for value in (data.get("side_effects") or {}).values())
)
raise SystemExit(0 if ok else 1)
PY
run_check "generic_summary_contract" python3 - "$out_dir/fail_generic_approval_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("failures") or []
ok = (
    data.get("status") == "FAIL_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and "missing_explicit_current_conversation_approval" in failures
    and "heartbeat_or_generic_approval_not_rejected" in failures
)
raise SystemExit(0 if ok else 1)
PY
run_check "fixture_review_summary_contract" python3 - "$out_dir/fail_fixture_executor_review_artifact_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("failures") or []
ok = (
    data.get("status") == "FAIL_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and "effectful_executor_review_artifact_is_fixture" in failures
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_approval_envelope_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_approval_envelope_gate",
  "verifier": "$script",
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
