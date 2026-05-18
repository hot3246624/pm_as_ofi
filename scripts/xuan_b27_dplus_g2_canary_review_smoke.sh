#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_review_smoke_$ts}"
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

run_review_case() {
  local name="$1"
  local expected_rc="$2"
  local run_dir="$fixtures/$name"
  local summary="$out_dir/${name}_summary.json"
  local rc=0
  scripts/xuan_b27_dplus_summarize_g2_canary_run.py \
    "$run_dir" \
    --output "$summary" \
    --check-acceptance \
    --secret-sentinel XUAN_G2_CANARY_SECRET_SENTINEL >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$summary"
}

python3 - "$fixtures" <<'PY'
from __future__ import annotations

import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])

BASE = {
    "artifact": "xuan_b27_dplus_g2_canary_local_acceptance_review",
    "exact_approval_scope": True,
    "strategy": "xuan_b27_dplus",
    "market_slug": "btc-updown-5m",
    "shared_ingress_role": "client",
    "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main",
    "gate_status": "PASS",
    "source_truth_all_pass": True,
    "order_ack_count": 1,
    "order_attempt_trace_linked": True,
    "venue_order_id_linked": True,
    "user_ws_connected_count": 1,
    "user_ws_subscribe_sent_count": 1,
    "event_decode_error_count": 0,
    "user_ws_decode_error_count": 0,
    "recorder_critical_drop_count": 0,
    "unexpected_taker_fill_count": 0,
    "max_live_orders_observed": 2,
    "max_open_cost_usdc_observed": 50,
    "max_strategy_exposure_usdc_observed": 100,
    "broker_started_or_modified": False,
    "systemd_or_service_control_used": False,
    "modified_shared_ingress": False,
    "env_files_written": False,
    "secret_values_recorded": False,
    "secret_values_written_to_disk": False,
}


def write_json(path: pathlib.Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def write_jsonl(path: pathlib.Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))


def make_case(name: str, updates: dict | None = None, events: list[dict] | None = None, secret_log: bool = False) -> None:
    run_dir = root / name
    recorder = run_dir / "recorder" / "session"
    review = dict(BASE)
    review.update(updates or {})
    write_json(run_dir / "g2_canary_acceptance_review.json", review)
    write_json(run_dir / "g2_canary_run_manifest.json", {"recorder_root": str(run_dir / "recorder")})
    write_jsonl(
        recorder / "events.jsonl",
        events
        if events is not None
        else [
            {"payload": {"event": "order_accepted", "data": {"order_id": "order-1"}}},
            {"payload": {"event": "user_ws_fill_parsed", "data": {"trade_id": "trade-1"}}},
        ],
    )
    if secret_log:
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "logs" / "stderr.log").write_text("XUAN_G2_CANARY_SECRET_SENTINEL\n")


make_case("pass_clean")
make_case("fail_missing_approval", {"exact_approval_scope": False})
make_case("fail_cap", {"max_live_orders_observed": 3})
make_case("fail_source_truth", {"source_truth_all_pass": False})
make_case("fail_taker_fill", {"unexpected_taker_fill_count": 1})
make_case("fail_forbidden_redeem_event", events=[{"payload": {"event": "redeem_result", "data": {}}}])
make_case("fail_secret_leak", secret_log=True)
make_case("fail_decode_error")
(root / "fail_decode_error" / "recorder" / "session" / "events.jsonl").write_text("{not json\n")
PY

run_check "summarizer_exists" test -x scripts/xuan_b27_dplus_summarize_g2_canary_run.py
run_check "summarizer_py_compile" python3 -m py_compile scripts/xuan_b27_dplus_summarize_g2_canary_run.py
run_check "summarizer_has_no_network_process_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
run_check "pass_clean_accepts" run_review_case pass_clean 0
run_check "missing_approval_rejects" run_review_case fail_missing_approval 2
run_check "cap_violation_rejects" run_review_case fail_cap 2
run_check "source_truth_failure_rejects" run_review_case fail_source_truth 2
run_check "taker_fill_rejects" run_review_case fail_taker_fill 2
run_check "forbidden_redeem_event_rejects" run_review_case fail_forbidden_redeem_event 2
run_check "secret_leak_rejects" run_review_case fail_secret_leak 2
run_check "decode_error_rejects" run_review_case fail_decode_error 2
run_check "pass_summary_contract" python3 - "$out_dir/pass_clean_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_run_summary"
    and data.get("status") == "PASS_G2_CANARY_ACCEPTANCE"
    and data.get("failures") == []
    and data.get("review", {}).get("exact_approval_scope") is True
    and data.get("event_stats", {}).get("order_accepted_count") == 1
    and data.get("event_stats", {}).get("user_ws_fill_parsed_count") == 1
    and data.get("secret_scan", {}).get("hit_count") == 0
    and all(value is False for value in (data.get("side_effects") or {}).values())
)
raise SystemExit(0 if ok else 1)
PY
run_check "secret_summary_contract" python3 - "$out_dir/fail_secret_leak_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "FAIL_G2_CANARY_ACCEPTANCE"
    and "secret_sentinel_leak" in (data.get("failures") or [])
    and data.get("secret_scan", {}).get("hit_count") == 1
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_review_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_review_gate",
  "summarizer": "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
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
