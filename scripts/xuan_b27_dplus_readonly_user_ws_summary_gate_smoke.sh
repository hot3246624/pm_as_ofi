#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_readonly_user_ws_summary_gate_smoke_$ts}"
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

run_summary_case() {
  local name="$1"
  local expected_rc="$2"
  shift 2
  local run_dir="$fixtures/$name"
  local summary="$out_dir/${name}_summary.json"
  local rc=0
  scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py \
    "$run_dir" \
    --output "$summary" \
    "$@" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$summary"
}

python3 - "$fixtures" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])

def write_json(path: pathlib.Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")

def write_jsonl(path: pathlib.Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))

def make_case(name: str, *, run_status: str, orders_possible: bool, user_ws_rows: list[dict], event_rows: list[dict]) -> None:
    run_dir = root / name
    recorder = run_dir / "recorder" / "session"
    write_json(run_dir / "run_manifest.json", {
        "status": run_status,
        "orders_possible": orders_possible,
        "shared_ingress_status": "OK",
        "resolved_slug": "btc-updown-5m-fixture",
        "market_id": "fixture-market",
        "user_ws_auth_source": "fixture",
        "fills_seen": len(event_rows),
        "recorder_root": str(run_dir / "recorder"),
    })
    write_json(run_dir / "wrapper_manifest.json", {"status": "PASS_WRAPPER_FIXTURE"})
    if user_ws_rows:
        write_jsonl(recorder / "user_ws.jsonl", user_ws_rows)
    if event_rows:
        write_jsonl(recorder / "events.jsonl", event_rows)

lifecycle_rows = [
    {"payload": {"event": "user_ws_listener_started", "data": {"schema_version": 1}}},
    {"payload": {"event": "user_ws_connected", "data": {"schema_version": 1, "http_status": 101}}},
    {"payload": {"event": "user_ws_subscribe_sent", "data": {"schema_version": 1, "market_count": 1}}},
]

make_case(
    "pass_with_user_ws_records",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=False,
    user_ws_rows=[{"event_type": "trade", "order_id": "order-1"}],
    event_rows=lifecycle_rows + [
        {"payload": {"event": "user_ws_fill_parsed", "data": {"order_id": "order-1", "trade_id": "trade-1"}}},
        {"payload": {"event": "xuan_b27_dplus_readonly_user_ws_fill_sink", "data": {"order_id": "order-1"}}},
    ],
)
make_case(
    "pass_without_user_ws_records",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=False,
    user_ws_rows=[],
    event_rows=lifecycle_rows,
)
make_case(
    "pass_without_user_ws_connection",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=False,
    user_ws_rows=[],
    event_rows=[],
)
make_case(
    "blocked_broker_no_records",
    run_status="SHARED_INGRESS_BROKER_UNAVAILABLE",
    orders_possible=False,
    user_ws_rows=[],
    event_rows=[],
)
make_case(
    "forbidden_order_event",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=False,
    user_ws_rows=[{"event_type": "trade", "order_id": "order-2"}],
    event_rows=lifecycle_rows + [{"payload": {"event": "order_accepted", "data": {"order_id": "bad"}}}],
)
make_case(
    "orders_possible_true",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=True,
    user_ws_rows=[{"event_type": "trade", "order_id": "order-3"}],
    event_rows=lifecycle_rows,
)

make_case(
    "recorder_decode_error",
    run_status="PASS_READONLY_USER_WS_OBSERVER",
    orders_possible=False,
    user_ws_rows=[{"event_type": "trade", "order_id": "order-4"}],
    event_rows=lifecycle_rows,
)
(root / "recorder_decode_error" / "recorder" / "session" / "events.jsonl").write_text("{not valid json\\n")
PY

run_check "summarizer_exists" test -x scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py
run_check "pass_with_records_passes_all_gates" run_summary_case pass_with_user_ws_records 0 --check-readonly --require-user-ws-records --require-user-ws-connection
run_check "pass_without_records_passes_connection_gate" run_summary_case pass_without_user_ws_records 0 --check-readonly --require-user-ws-connection
run_check "pass_without_records_fails_record_gate" run_summary_case pass_without_user_ws_records 3 --check-readonly --require-user-ws-records
run_check "pass_without_connection_fails_connection_gate" run_summary_case pass_without_user_ws_connection 4 --check-readonly --require-user-ws-connection
run_check "blocked_broker_is_not_readonly_violation" run_summary_case blocked_broker_no_records 0 --check-readonly
run_check "forbidden_order_event_fails_readonly_gate" run_summary_case forbidden_order_event 2 --check-readonly
run_check "orders_possible_true_fails_readonly_gate" run_summary_case orders_possible_true 2 --check-readonly
run_check "recorder_decode_error_fails_readonly_gate" run_summary_case recorder_decode_error 2 --check-readonly
run_check "summary_counts_fill_and_sink_events" python3 - "$out_dir/pass_with_user_ws_records_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("user_ws_raw_count") == 1
    and data.get("user_ws_fill_parsed_count") == 1
    and data.get("user_ws_lifecycle_event_count") == 3
    and data.get("user_ws_connected_count") == 1
    and data.get("user_ws_subscribe_sent_count") == 1
    and data.get("fill_sink_count") == 1
    and data.get("forbidden_event_count") == 0
    and data.get("gate_status") == "PASS"
)
raise SystemExit(0 if ok else 1)
PY
run_check "summary_reports_no_connection_gate_failure" python3 - "$out_dir/pass_without_user_ws_connection_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("gate_status") == "FAIL_NO_USER_WS_CONNECTION"
    and data.get("gate_failures") == ["FAIL_NO_USER_WS_CONNECTION"]
    and data.get("gate_requirements", {}).get("require_user_ws_connection") is True
)
raise SystemExit(0 if ok else 1)
PY
run_check "summary_reports_no_records_gate_failure" python3 - "$out_dir/pass_without_user_ws_records_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_READONLY_USER_WS_OBSERVER"
    and data.get("gate_status") == "FAIL_NO_USER_WS_RECORDS"
    and data.get("gate_failures") == ["FAIL_NO_USER_WS_RECORDS"]
    and data.get("gate_requirements", {}).get("require_user_ws_records") is True
)
raise SystemExit(0 if ok else 1)
PY
run_check "summary_reports_forbidden_event_sample" python3 - "$out_dir/forbidden_order_event_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
samples = data.get("forbidden_event_samples") or []
ok = (
    data.get("status") == "FAIL_READONLY_VIOLATION"
    and data.get("forbidden_event_count") == 1
    and samples
    and samples[0].get("event") == "order_accepted"
)
raise SystemExit(0 if ok else 1)
PY
run_check "summary_reports_recorder_decode_error" python3 - "$out_dir/recorder_decode_error_summary.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "FAIL_RECORDER_DECODE_ERROR"
    and data.get("event_decode_error_count") == 1
    and data.get("user_ws_decode_error_count") == 0
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_readonly_user_ws_summary_gate_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_readonly_user_ws_summary_gate",
  "checks_log": "$log",
  "generated_at_utc": "$ts",
  "orders_sent": false,
  "auth_network_started": false
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
