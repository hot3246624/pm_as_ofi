#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_l1_dry_run_outcome_labels_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
fixture_dir="$out_dir/fixtures"
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

run_expect_rc() {
  local name="$1"
  local expected="$2"
  shift 2
  {
    printf '== %s ==\n' "$name"
    set +e
    "$@"
    local rc=$?
    set -e
    printf 'rc=%s expected=%s\n' "$rc" "$expected"
    [[ "$rc" == "$expected" ]]
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

producer="scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py"
run_artifact="scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py"
runner="scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py"

run_check "producer_exists" test -x "$producer"
run_check "producer_py_compile" python3 -m py_compile "$producer"
run_check "producer_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$producer'"

run_check "fixtures_created" python3 - "$fixture_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)

def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for record in records:
            fh.write(json.dumps(record, sort_keys=True) + "\n")

def make_run(name, touch=True, forbidden_sample=False):
    run = root / name
    slug = "btc-updown-5m-1778893200"
    base = 1778893210000
    summary = {
        "artifact": "xuan_b27_dplus_observer_summary",
        "status": "PASS_NO_ORDER_OBSERVER",
        "markets": [slug],
        "event_counts": {"xuan_b27_dplus_observer_tick": 120000},
        "jsonl_file_count": 1,
        "observer_candidate_counts": {
            "would_track": 120,
            "would_place": 0,
            "submitted_previews": 0,
            "trace_previews": 120,
        },
        "source_truth_verdict_counts": {"UNKNOWN": 120, "FAIL": 0},
        "decode_error_count": 0,
        "truncated_final_line_count": 0,
        "no_order_violation_count": 0,
        "run_manifest": {
            "dry_run": True,
            "elapsed_seconds": 1800,
            "orders_allowed": False,
            "cancels_allowed": False,
            "redeems_allowed": False,
        },
    }
    write_json(run / "observer_summary.json", summary)

    events = []
    market_md = []
    edge_samples = []
    for index in range(120):
        side = "yes" if index % 2 == 0 else "no"
        candidate_id = f"xuan_b27_dplus:{slug}:{index}:{side}"
        order_attempt_id = f"{candidate_id}:attempt:preview"
        event_ms = base + index * 1000
        observer_price = 0.47 if side == "yes" else 0.46
        events.append(
            {
                "recv_unix_ms": event_ms,
                "slug": slug,
                "payload": {
                    "data": {
                        "event": "xuan_b27_dplus_observer_tick",
                        "market_slug": slug,
                        "observer_candidates": [
                            {
                                "candidate_id": candidate_id,
                                "side": side,
                                "book_bid": 0.50,
                                "book_ask": 0.51,
                                "observer_price": observer_price,
                                "target_qty": 5.0,
                                "would_track": True,
                                "would_place": False,
                                "blocked_reason": "none",
                                "order_attempt_trace_preview": {
                                    "order_attempt_id": order_attempt_id,
                                    "preview_only": True,
                                    "submitted": False,
                                },
                            }
                        ],
                    }
                },
            }
        )
        ask = 0.40 if touch else 0.60
        market_md.append(
            {
                "recv_unix_ms": event_ms + 500,
                "slug": slug,
                "payload": {
                    "kind": "book_l1",
                    "yes_bid": 0.39,
                    "yes_ask": ask,
                    "no_bid": 0.39,
                    "no_ask": ask,
                },
            }
        )
        edge_samples.append(
            {
                "market_slug": slug,
                "candidate_id": candidate_id,
                "order_attempt_id": order_attempt_id,
                "side": side,
                "accepted": True,
                "would_track": True,
                "would_place": False,
                "preview_only": True,
                "submitted": False,
                "book_ask": 0.51,
                "observer_price": observer_price,
                "edge_bps": 1.2,
                "expected_value_bps": 1.2,
                "performance_basis": "observer_edge_only_not_realized",
                "orders_sent": forbidden_sample and index == 0,
                "cancels_sent": False,
                "redeems_sent": False,
                "auth_network_started": False,
                "started_canary": False,
            }
        )
    write_jsonl(run / "recorder" / "2026-05-17" / slug / "events.jsonl", events)
    write_jsonl(run / "recorder" / "2026-05-17" / slug / "market_md.jsonl", market_md)
    write_jsonl(run / "edge_samples.jsonl", edge_samples)
    return run

make_run("pass_run", touch=True)
make_run("quality_fail_run", touch=False)
make_run("side_effect_run", touch=True, forbidden_sample=True)
PY

pass_dir="$out_dir/pass_labels"
quality_fail_dir="$out_dir/quality_fail"
unsafe_dir="$out_dir/unsafe"
side_effect_dir="$out_dir/side_effect"
run_artifact_dir="$out_dir/no_order_shadow_run"
runner_dir="$out_dir/acceptance_runner"

run_expect_rc "pass_l1_labels_fixture" 0 "$producer" \
  --run-dir "$fixture_dir/pass_run" \
  --observer-summary "$fixture_dir/pass_run/observer_summary.json" \
  --edge-samples "$fixture_dir/pass_run/edge_samples.jsonl" \
  --output-dir "$pass_dir" \
  --lookahead-seconds 60 \
  --min-labels 100 \
  --min-touch-ratio 0.5 \
  --min-mean-realized-edge-bps 0

run_expect_rc "quality_fail_fixture" 10 "$producer" \
  --run-dir "$fixture_dir/quality_fail_run" \
  --observer-summary "$fixture_dir/quality_fail_run/observer_summary.json" \
  --edge-samples "$fixture_dir/quality_fail_run/edge_samples.jsonl" \
  --output-dir "$quality_fail_dir" \
  --lookahead-seconds 60 \
  --min-labels 100 \
  --min-touch-ratio 0.5 \
  --min-mean-realized-edge-bps 0

run_expect_rc "unsafe_path_fixture" 2 "$producer" \
  --run-dir "/tmp/xuan_b27_dplus_l1_dry_run_outcome_labels_unsafe" \
  --edge-samples "$fixture_dir/pass_run/edge_samples.jsonl" \
  --output-dir "$unsafe_dir"

run_expect_rc "side_effect_fixture" 8 "$producer" \
  --run-dir "$fixture_dir/side_effect_run" \
  --observer-summary "$fixture_dir/side_effect_run/observer_summary.json" \
  --edge-samples "$fixture_dir/side_effect_run/edge_samples.jsonl" \
  --output-dir "$side_effect_dir" \
  --lookahead-seconds 60 \
  --min-labels 100 \
  --min-touch-ratio 0.5 \
  --min-mean-realized-edge-bps 0

run_expect_rc "no_order_shadow_run_accepts_l1_labels" 0 "$run_artifact" \
  --observer-summary "$fixture_dir/pass_run/observer_summary.json" \
  --edge-samples "$fixture_dir/pass_run/edge_samples.jsonl" \
  --outcome-labels "$pass_dir/outcome_labels.jsonl" \
  --output-dir "$run_artifact_dir" \
  --min-joined-outcomes 100 \
  --min-positive-realized-ratio 0.5 \
  --min-mean-realized-edge-bps 0

run_expect_rc "acceptance_runner_accepts_fixture_chain_without_real_publish" 0 "$runner" \
  --run-dir "$run_artifact_dir" \
  --output-dir "$runner_dir" \
  --allow-fixture

run_check "manifests_safe" python3 - \
  "$pass_dir/manifest.json" \
  "$quality_fail_dir/manifest.json" \
  "$unsafe_dir/manifest.json" \
  "$side_effect_dir/manifest.json" \
  "$run_artifact_dir/manifest.json" \
  "$runner_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

paths = [pathlib.Path(path) for path in sys.argv[1:]]
data = [json.loads(path.read_text()) for path in paths]
expected_statuses = [
    "PASS_L1_DRY_RUN_OUTCOME_LABELS",
    "FAIL_REALIZED_OUTCOME_QUALITY",
    "FAIL_UNSAFE_INPUT_PATH",
    "FAIL_FORBIDDEN_SIDE_EFFECT",
    "PASS_NO_ORDER_SHADOW_DRY_RUN",
    "PASS_RUNNER_FIXTURE_CHAIN",
]
ok = [item.get("status") for item in data] == expected_statuses
for item in data:
    side_effects = item.get("side_effects") or {}
    ok = ok and item.get("orders_sent") is False
    ok = ok and item.get("cancels_sent", False) is False
    ok = ok and item.get("redeems_sent", False) is False
    ok = ok and item.get("auth_network_started") is False
    ok = ok and item.get("started_canary") is False
    ok = ok and all(value is False for value in side_effects.values())
ok = ok and data[0].get("label_count") == 120
ok = ok and data[0].get("real_outcome_labels_published") is True
ok = ok and data[0].get("touch_ratio") >= 0.5
ok = ok and data[1].get("real_outcome_labels_published") is False
ok = ok and data[4].get("realized_outcome_label_run_ready") is True
ok = ok and data[5].get("real_acceptance_artifact_published") is False
ok = ok and data[5].get("acceptance_artifact_published") is False
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_l1_dry_run_outcome_labels_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_l1_dry_run_outcome_label_gate",
  "producer": "$producer",
  "producer_pass_manifest": "$pass_dir/manifest.json",
  "producer_quality_fail_manifest": "$quality_fail_dir/manifest.json",
  "no_order_shadow_run_manifest": "$run_artifact_dir/manifest.json",
  "acceptance_runner_manifest": "$runner_dir/manifest.json",
  "real_outcome_labels_published": false,
  "real_shadow_run_artifact_published": false,
  "acceptance_artifact_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_observer": false,
  "started_user_ws": false,
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
