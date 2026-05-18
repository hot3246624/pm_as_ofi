#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_observer_summary_gate_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
checks="$out_dir/checks.txt"
: > "$checks"

write_fixture() {
  local run_dir="$1"
  local kind="$2"
  local market_dir="$run_dir/recorder/2026-05-15/btc-updown-5m-1778842800"
  mkdir -p "$market_dir"
  python3 - "$run_dir" "$market_dir" "$kind" <<'PY'
import json
import pathlib
import sys

run_dir = pathlib.Path(sys.argv[1])
market_dir = pathlib.Path(sys.argv[2])
kind = sys.argv[3]

(run_dir / "start_manifest.json").write_text(json.dumps({
    "artifact": "fixture_start",
    "dry_run": True,
    "orders_allowed": False,
    "cancels_allowed": False,
    "redeems_allowed": False,
}) + "\n")
(run_dir / "run_manifest.json").write_text(json.dumps({
    "artifact": "fixture_run",
    "status": "COMPLETED",
    "dry_run": True,
    "orders_allowed": False,
    "cancels_allowed": False,
    "redeems_allowed": False,
}) + "\n")

candidate = {
    "candidate_id": "xuan_b27_dplus:fixture:1:yes",
    "side": "yes",
    "would_track": kind == "tracked",
    "would_place": False,
    "blocked_reason": "none" if kind == "tracked" else "market_slug_filter",
    "order_attempt_trace_preview": {
        "candidate_id": "xuan_b27_dplus:fixture:1:yes",
        "order_attempt_id": "xuan_b27_dplus:fixture:1:yes:attempt:preview",
        "preview_only": True,
        "submitted": False,
        "venue_order_id": None,
    },
}
observer = {
    "slug": "btc-updown-5m-1778842800",
    "payload": {
        "data": {
            "event": "xuan_b27_dplus_observer_tick",
            "market_enabled": kind == "tracked",
            "orders_sent_by_this_module": False,
            "observer_candidates": [candidate],
            "source_of_truth_gate": {"verdict": "UNKNOWN"},
        }
    },
}

records = [observer]
if kind == "violation":
    records.append({
        "slug": "btc-updown-5m-1778842800",
        "payload": {"event": "order_accepted", "data": {"order_id": "bad"}},
    })

with (market_dir / "events.jsonl").open("w") as fh:
    for record in records:
        fh.write(json.dumps(record) + "\n")
PY
}

run_expect_rc() {
  local name="$1"
  local expected="$2"
  shift 2
  set +e
  "$@" > "$out_dir/$name.out" 2> "$out_dir/$name.err"
  local rc=$?
  set -e
  if [[ "$rc" == "$expected" ]]; then
    printf 'PASS %s rc=%s\n' "$name" "$rc" >> "$checks"
  else
    printf 'FAIL %s expected_rc=%s actual_rc=%s\n' "$name" "$expected" "$rc" >> "$checks"
    status="FAIL"
  fi
}

tracked="$out_dir/tracked_run"
blocked="$out_dir/blocked_run"
violation="$out_dir/violation_run"
write_fixture "$tracked" tracked
write_fixture "$blocked" blocked
write_fixture "$violation" violation

run_expect_rc "tracked_check_no_order_and_candidates" 0 \
  scripts/xuan_b27_dplus_summarize_observer_run.py "$tracked" --check-no-order --require-tracked-candidates
run_expect_rc "blocked_check_no_order_only" 0 \
  scripts/xuan_b27_dplus_summarize_observer_run.py "$blocked" --check-no-order
run_expect_rc "blocked_requires_candidates_fails" 3 \
  scripts/xuan_b27_dplus_summarize_observer_run.py "$blocked" --require-tracked-candidates
run_expect_rc "violation_check_no_order_fails" 2 \
  scripts/xuan_b27_dplus_summarize_observer_run.py "$violation" --check-no-order

cat > "$out_dir/manifest.json" <<EOF
{
  "schema_version": 1,
  "artifact": "xuan_b27_dplus_observer_summary_gate_smoke",
  "status": "$status",
  "created_utc": "$ts",
  "strategy": "xuan_b27_dplus",
  "scope": "local_fixture_no_network",
  "orders_sent": false,
  "auth_network_started": false,
  "checks_file": "$checks"
}
EOF

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
