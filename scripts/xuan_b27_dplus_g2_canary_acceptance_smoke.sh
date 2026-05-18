#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_acceptance_smoke_$ts}"
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

run_check "fixture_acceptance_gate" python3 - "$out_dir/fixture_results.json" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])

BASE = {
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


def evaluate(review: dict) -> tuple[str, list[str]]:
    failures: list[str] = []
    if review.get("exact_approval_scope") is not True:
        failures.append("missing_exact_approval_scope")
    if review.get("strategy") != "xuan_b27_dplus":
        failures.append("wrong_strategy")
    if review.get("market_slug") != "btc-updown-5m":
        failures.append("wrong_market_slug")
    if review.get("shared_ingress_role") != "client":
        failures.append("shared_ingress_not_client")
    if review.get("shared_ingress_root") != "/srv/pm_as_ofi/shared-ingress-main":
        failures.append("wrong_shared_ingress_root")
    if review.get("gate_status") != "PASS":
        failures.append("gate_not_pass")
    if review.get("source_truth_all_pass") is not True:
        failures.append("source_truth_not_all_pass")
    if review.get("order_ack_count", 0) <= 0:
        failures.append("no_order_ack")
    if review.get("order_attempt_trace_linked") is not True:
        failures.append("order_attempt_trace_missing")
    if review.get("venue_order_id_linked") is not True:
        failures.append("venue_order_id_missing")
    if review.get("user_ws_connected_count", 0) <= 0:
        failures.append("user_ws_not_connected")
    if review.get("user_ws_subscribe_sent_count", 0) <= 0:
        failures.append("user_ws_not_subscribed")
    if review.get("event_decode_error_count") != 0:
        failures.append("event_decode_error")
    if review.get("user_ws_decode_error_count") != 0:
        failures.append("user_ws_decode_error")
    if review.get("recorder_critical_drop_count") != 0:
        failures.append("recorder_critical_drop")
    if review.get("unexpected_taker_fill_count") != 0:
        failures.append("unexpected_taker_fill")
    if review.get("max_live_orders_observed", 999) > 2:
        failures.append("max_live_orders_exceeded")
    if review.get("max_open_cost_usdc_observed", 999.0) > 50:
        failures.append("max_open_cost_exceeded")
    if review.get("max_strategy_exposure_usdc_observed", 999.0) > 100:
        failures.append("max_strategy_exposure_exceeded")
    for field in (
        "broker_started_or_modified",
        "systemd_or_service_control_used",
        "modified_shared_ingress",
        "env_files_written",
        "secret_values_recorded",
        "secret_values_written_to_disk",
    ):
        if review.get(field) is not False:
            failures.append(field)
    return ("PASS_G2_CANARY_ACCEPTANCE" if not failures else "FAIL_G2_CANARY_ACCEPTANCE", failures)


def make(name: str, **updates: object) -> dict:
    review = dict(BASE)
    review.update(updates)
    status, failures = evaluate(review)
    return {
        "name": name,
        "expected_pass": name.startswith("pass_"),
        "status": status,
        "failures": failures,
    }


fixtures = [
    make("pass_exact_g2_canary_acceptance"),
    make("fail_missing_exact_approval", exact_approval_scope=False),
    make("fail_live_order_cap", max_live_orders_observed=3),
    make("fail_open_cost_cap", max_open_cost_usdc_observed=50.01),
    make("fail_source_truth_unknown", source_truth_all_pass=False),
    make("fail_unexpected_taker_fill", unexpected_taker_fill_count=1),
    make("fail_recorder_critical_drop", recorder_critical_drop_count=1),
    make("fail_shared_ingress_role", shared_ingress_role="standalone"),
    make("fail_shared_ingress_root", shared_ingress_root="/tmp/shared-ingress"),
    make("fail_secret_recorded", secret_values_recorded=True),
]
ok = all(
    (fixture["expected_pass"] and fixture["status"] == "PASS_G2_CANARY_ACCEPTANCE")
    or ((not fixture["expected_pass"]) and fixture["status"] == "FAIL_G2_CANARY_ACCEPTANCE")
    for fixture in fixtures
)
out.write_text(json.dumps({"fixtures": fixtures, "all_expected": ok}, indent=2, sort_keys=True) + "\n")
raise SystemExit(0 if ok else 1)
PY

run_check "script_static_no_network_or_process_control" \
  bash -c "! sed '/script_static_no_network_or_process_control/,+1d' '$0' | rg -q '(^|[[:space:]])(ssh|scp|rsync|curl|nc)[[:space:]]|systemctl|service[[:space:]]|subprocess|requests|websocket|socket\\.connect|cargo run|polymarket_v2|xuan_b27_dplus_user_ws_observer'"

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_acceptance_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_acceptance_fixture_gate",
  "fixture_results": "$out_dir/fixture_results.json",
  "exact_approval_required": true,
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
