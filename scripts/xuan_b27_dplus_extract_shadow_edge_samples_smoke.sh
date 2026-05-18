#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_edge_samples_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
fixture_run="$out_dir/xuan_research_artifacts_fixture/xuan_b27_dplus_auth_observer_fixture"
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

script="scripts/xuan_b27_dplus_extract_shadow_edge_samples.py"
perf_script="scripts/xuan_b27_dplus_shadow_performance_evidence.py"

run_check "extractor_exists" test -x "$script"
run_check "extractor_py_compile" python3 -m py_compile "$script"
run_check "extractor_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

run_check "fixtures_created" python3 - "$fixture_run" <<'PY'
import json
import pathlib
import sys

run = pathlib.Path(sys.argv[1])
events = run / "recorder" / "2026-05-17" / "btc-updown-5m-1" / "events.jsonl"
events.parent.mkdir(parents=True, exist_ok=True)
summary = {
    "artifact": "xuan_b27_dplus_observer_summary",
    "status": "PASS_NO_ORDER_OBSERVER",
    "markets": ["btc-updown-5m-1"],
    "observer_candidate_counts": {
        "would_track": 2,
        "would_place": 0,
        "submitted_previews": 0,
        "trace_previews": 2,
    },
    "decode_error_count": 0,
    "no_order_violation_count": 0,
    "run_manifest": {
        "dry_run": True,
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
    },
}
(run / "observer_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
payload = {
    "payload": {
        "data": {
            "event": "xuan_b27_dplus_observer_tick",
            "market_slug": "btc-updown-5m-1",
            "observer_candidates": [
                {
                    "candidate_id": "c1",
                    "side": "yes",
                    "book_bid": 0.50,
                    "book_ask": 0.51,
                    "observer_price": 0.47,
                    "would_track": True,
                    "would_place": False,
                    "blocked_reason": "none",
                    "order_attempt_trace_preview": {
                        "order_attempt_id": "c1:attempt:preview",
                        "preview_only": True,
                        "submitted": False,
                    },
                },
                {
                    "candidate_id": "c2",
                    "side": "no",
                    "book_bid": 0.49,
                    "book_ask": 0.50,
                    "observer_price": 0.46,
                    "would_track": True,
                    "would_place": False,
                    "blocked_reason": "none",
                    "order_attempt_trace_preview": {
                        "order_attempt_id": "c2:attempt:preview",
                        "preview_only": True,
                        "submitted": False,
                    },
                },
            ],
        }
    }
}
events.write_text(json.dumps(payload, sort_keys=True) + "\n")
PY

extract_dir="$out_dir/extract_run"
perf_dir="$out_dir/perf_edge_only_run"
run_check "extract_fixture" "$script" --run-dir "$fixture_run" --output-dir "$extract_dir" --max-samples 10
run_check "extract_manifest_safe" python3 - "$extract_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_shadow_edge_samples"
    and data.get("status") == "PASS_EDGE_SAMPLES_EXTRACTED"
    and data.get("scope") == "local_no_network_observer_edge_sample_extraction"
    and data.get("sample_count") == 2
    and data.get("accepted_candidate_count") == 2
    and data.get("positive_edge_candidate_count") == 2
    and data.get("performance_basis") == "observer_edge_only_not_realized"
    and data.get("strategy_performance_evidence_ready") is False
    and data.get("requires_independent_outcome_evidence") is True
    and data.get("orders_sent") is False
    and data.get("cancels_sent") is False
    and data.get("redeems_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY

run_check "performance_gate_rejects_edge_only_samples" bash -c '
  set +e
  "$0" --observer-summary "$1" --edge-samples "$2" --output-dir "$3"
  rc=$?
  set -e
  [[ "$rc" == "8" ]]
' "$perf_script" "$fixture_run/observer_summary.json" "$extract_dir/edge_samples.jsonl" "$perf_dir"

run_check "performance_edge_only_manifest_safe" python3 - "$perf_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("artifact") == "xuan_b27_dplus_shadow_strategy_performance_evidence"
    and data.get("status") == "FAIL_EDGE_ONLY_NOT_PERFORMANCE"
    and data.get("evidence_passed") is False
    and data.get("edge_samples", {}).get("performance_basis_ok") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shadow_edge_samples_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shadow_edge_sample_extraction_gate",
  "extractor": "$script",
  "extract_manifest": "$extract_dir/manifest.json",
  "edge_only_rejected_by_performance_gate": true,
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
