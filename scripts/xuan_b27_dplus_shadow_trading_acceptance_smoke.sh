#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_trading_acceptance_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
script="scripts/xuan_b27_dplus_shadow_trading_acceptance.py"
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

write_fixture() {
  local dir="$1"
  local aggregate_json="$2"
  mkdir -p "$dir"
  cat > "$dir/manifest.json" <<'JSON'
{
  "kind": "manifest",
  "script": "xuan_dplus_passive_passive_shadow_runner.py",
  "duration_s": 1800,
  "markets": [
    {
      "POLYMARKET_MARKET_SLUG": "btc-updown-5m-1900000000",
      "POLYMARKET_MARKET_ID": "0xmarket",
      "POLYMARKET_YES_ASSET_ID": "yes",
      "POLYMARKET_NO_ASSET_ID": "no"
    }
  ],
  "safety": {
    "orders_sent": false,
    "dry_run": "true",
    "shared_ingress_role": "client",
    "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main"
  }
}
JSON
  printf '%s\n' "$aggregate_json" > "$dir/aggregate_report.json"
}

pass_aggregate='{
  "kind": "aggregate_report",
  "script": "xuan_dplus_passive_passive_shadow_runner.py",
  "slugs": 4,
  "blocked": {},
  "metrics": {
    "candidates": 140,
    "queue_supported_fills": 84,
    "fill_rate": 0.6,
    "pair_actions": 80,
    "seed_qty": 410.0,
    "seed_cost": 184.0,
    "filled_qty": 360.0,
    "filled_cost": 162.0,
    "pair_qty": 176.0,
    "qty_pair_share_of_filled": 0.9777777778,
    "pair_pnl": 15.25,
    "roi_on_seed_cost": 0.0828804348,
    "roi_on_filled_cost": 0.0941358025,
    "residual_qty": 2.0,
    "residual_cost": 0.8,
    "material_residual_lots": 0,
    "pair_cost_proxy_p50": 0.91,
    "pair_cost_proxy_p90": 0.95,
    "net_pair_cost_proxy_p50": 0.91,
    "net_pair_cost_proxy_p90": 0.96,
    "fill_wait_proxy_p50_ms": 2500,
    "fill_wait_proxy_p90_ms": 11000,
    "pair_wait_proxy_p50_ms": 7000,
    "pair_wait_proxy_p90_ms": 30000
  },
  "top_residual_lots": []
}'

negative_pnl_aggregate="${pass_aggregate/\"pair_pnl\": 15.25/\"pair_pnl\": -0.25}"
residual_bad_aggregate="${pass_aggregate/\"residual_qty\": 2.0/\"residual_qty\": 80.0}"
low_sample_aggregate="${pass_aggregate/\"candidates\": 140/\"candidates\": 2}"
low_sample_high_cost_aggregate="${low_sample_aggregate/\"net_pair_cost_proxy_p90\": 0.96/\"net_pair_cost_proxy_p90\": 1.07}"

pass_run="$out_dir/fixtures/pass_shadow"
negative_run="$out_dir/fixtures/negative_pnl"
residual_run="$out_dir/fixtures/residual_bad"
low_sample_run="$out_dir/fixtures/low_sample"
low_sample_high_cost_run="$out_dir/fixtures/low_sample_high_cost"
unsafe_run="$out_dir/fixtures/unsafe"

write_fixture "$pass_run" "$pass_aggregate"
write_fixture "$negative_run" "$negative_pnl_aggregate"
write_fixture "$residual_run" "$residual_bad_aggregate"
write_fixture "$low_sample_run" "$low_sample_aggregate"
write_fixture "$low_sample_high_cost_run" "$low_sample_high_cost_aggregate"
write_fixture "$unsafe_run" "$pass_aggregate"
python3 - "$unsafe_run/manifest.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
data["safety"]["orders_sent"] = True
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY

expect_status() {
  local name="$1"
  local expected="$2"
  local manifest="$3"
  shift 3
  {
    printf '== %s ==\n' "$name"
    set +e
    "$@"
    local rc=$?
    set -e
    python3 - "$manifest" "$expected" "$rc" <<'PY'
import json
import pathlib
import sys

manifest = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
rc = int(sys.argv[3])
data = json.loads(manifest.read_text())
if data.get("status") != expected:
    raise SystemExit(f"expected status {expected}, got {data.get('status')}")
if expected.startswith("PASS_") and rc != 0:
    raise SystemExit(f"expected rc 0 for {expected}, got {rc}")
if not expected.startswith("PASS_") and rc == 0:
    raise SystemExit(f"expected nonzero rc for {expected}")
PY
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

run_check "acceptance_script_exists" test -x "$script"
run_check "acceptance_script_py_compile" python3 -m py_compile "$script"
run_check "acceptance_script_has_no_network_process_or_raw_replay_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|sqlite3)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|sqlite3)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

pass_dir="$out_dir/pass_acceptance"
negative_dir="$out_dir/negative_pnl_acceptance"
residual_dir="$out_dir/residual_acceptance"
low_sample_dir="$out_dir/low_sample_acceptance"
low_sample_high_cost_dir="$out_dir/low_sample_high_cost_acceptance"
unsafe_dir="$out_dir/unsafe_acceptance"
missing_dir="$out_dir/missing_acceptance"

expect_status "pass_fixture_acceptance" "PASS_SHADOW_TRADING_ACCEPTANCE" "$pass_dir/manifest.json" \
  "$script" --shadow-run-dir "$pass_run" --output-dir "$pass_dir"
expect_status "negative_pnl_rejected" "FAIL_SHADOW_TRADING_PNL_METRICS" "$negative_dir/manifest.json" \
  "$script" --shadow-run-dir "$negative_run" --output-dir "$negative_dir"
expect_status "residual_risk_rejected" "FAIL_SHADOW_TRADING_PROMOTION_RISK_BUDGET" "$residual_dir/manifest.json" \
  "$script" --shadow-run-dir "$residual_run" --output-dir "$residual_dir"
expect_status "low_sample_rejected" "FAIL_SHADOW_TRADING_SAMPLE_SIZE" "$low_sample_dir/manifest.json" \
  "$script" --shadow-run-dir "$low_sample_run" --output-dir "$low_sample_dir"
expect_status "low_sample_high_cost_records_all_failures" "FAIL_SHADOW_TRADING_SAMPLE_SIZE" "$low_sample_high_cost_dir/manifest.json" \
  "$script" --shadow-run-dir "$low_sample_high_cost_run" --output-dir "$low_sample_high_cost_dir"
expect_status "unsafe_safety_rejected" "FAIL_SHADOW_TRADING_REPORT_SAFETY" "$unsafe_dir/manifest.json" \
  "$script" --shadow-run-dir "$unsafe_run" --output-dir "$unsafe_dir"
mkdir -p "$out_dir/fixtures/missing"
expect_status "missing_report_rejected" "FAIL_MISSING_SHADOW_TRADING_REPORT" "$missing_dir/manifest.json" \
  "$script" --shadow-run-dir "$out_dir/fixtures/missing" --output-dir "$missing_dir"

run_check "pass_manifest_exposes_required_metrics" python3 - "$pass_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
metrics = data.get("trading_metrics") or {}
checks = data.get("checks") or {}
research = data.get("research_ranking") or {}
promotion = data.get("promotion_gate") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_shadow_trading_acceptance"
    and data.get("status") == "PASS_SHADOW_TRADING_ACCEPTANCE"
    and data.get("scope") == "local_no_network_shadow_trading_acceptance"
    and data.get("source_tool") == "tools/xuan_dplus_passive_passive_shadow_runner.py"
    and data.get("acceptance_passed") is True
    and checks.get("sample_size_ok") is True
    and checks.get("pnl_metrics_ok") is True
    and checks.get("residual_risk_ok") is True
    and checks.get("promotion_risk_budget_ok") is True
    and research.get("label") == "KEEP_ECONOMIC_RESEARCH_ONLY"
    and research.get("economic_pnl_positive") is True
    and promotion.get("passed") is True
    and promotion.get("promotion_risk_budget_ok") is True
    and promotion.get("normalized_risk_budget", {}).get("residual_qty_share_of_filled") < 0.01
    and metrics.get("candidates") == 140
    and metrics.get("queue_supported_fills") == 84
    and metrics.get("pair_actions") == 80
    and metrics.get("pair_pnl") == 15.25
    and metrics.get("residual_qty") == 2.0
    and metrics.get("residual_cost") == 0.8
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and data.get("side_effects", {}).get("shadow_runner_started") is False
)
raise SystemExit(0 if ok else 1)
PY

run_check "combined_failure_manifest_records_all_failed_gates" python3 - "$low_sample_high_cost_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("failures") or []
metric_failures = data.get("metric_failures") or {}
sample_metrics = {item.get("metric") for item in metric_failures.get("sample_size") or []}
promotion_metrics = {item.get("metric") for item in metric_failures.get("promotion_risk_budget") or []}
does_not_prove = data.get("evidence_interpretation", {}).get("does_not_prove") or []
ok = (
    data.get("status") == "FAIL_SHADOW_TRADING_SAMPLE_SIZE"
    and data.get("acceptance_passed") is False
    and "shadow_trading_sample_size_failed" in failures
    and "shadow_trading_promotion_risk_budget_failed" in failures
    and "candidates" in sample_metrics
    and "pair_tail_loss_share_of_pair_pnl" in promotion_metrics
    and data.get("promotion_gate", {}).get("promotion_risk_budget_ok") is False
    and str(data.get("research_ranking", {}).get("label", "")).startswith("TRADEOFF_")
    and "shadow trading acceptance" in does_not_prove
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shadow_trading_acceptance_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shadow_trading_acceptance_gate",
  "acceptance_script": "$script",
  "pass_fixture_manifest": "$pass_dir/manifest.json",
  "real_shadow_trading_acceptance_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
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
