#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${1:?usage: capture_pgt_instance_snapshot.sh <instance_id>}"
LOG_PATH="$ROOT/logs/$INSTANCE_ID/run.log"
RECORDER_ROOT="$ROOT/data/recorder/$INSTANCE_ID"
REPLAY_ROOT="$ROOT/data/replay/$INSTANCE_ID"
REPORT_ROOT="$ROOT/data/replay_reports/$INSTANCE_ID"
SNAPSHOT_ROOT="$ROOT/logs/$INSTANCE_ID"

mkdir -p "$REPORT_ROOT" "$SNAPSHOT_ROOT"

wait_for_log() {
  until [[ -f "$LOG_PATH" ]]; do
    sleep 2
  done
}

wait_for_exit_marker() {
  until grep -q "Fixed mode — exiting" "$LOG_PATH"; do
    sleep 5
  done
}

latest_recorder_date() {
  find "$RECORDER_ROOT" -mindepth 1 -maxdepth 1 -type d -print 2>/dev/null | sort | tail -n1 | xargs -I{} basename "{}"
}

extract_latest_slug() {
  grep "Round #1 — " "$LOG_PATH" | tail -n1 | sed -E 's/.*Round #1 — ([^[:space:]]+).*/\1/'
}

write_snapshot_markdown() {
  local date="$1"
  local slug="$2"
  local snapshot_md="$SNAPSHOT_ROOT/snapshot.md"
  {
    echo "# PGT Snapshot"
    echo
    echo "- instance_id: \`$INSTANCE_ID\`"
    echo "- date: \`$date\`"
    echo "- slug: \`$slug\`"
    echo
    echo "## Final Metrics"
    grep "🎯 FinalMetrics" "$LOG_PATH" | tail -n1 || true
    echo
    echo "## Shutdown"
    grep "🎯 Shutdown" "$LOG_PATH" | tail -n1 || true
    echo
    echo "## Last Fills"
    grep "📦 Fill:" "$LOG_PATH" | tail -n5 || true
    echo
    echo "## Report Row"
    if [[ -f "$REPORT_ROOT/pgt_shadow_report_crypto_5m.json" ]]; then
      python3 - "$REPORT_ROOT/pgt_shadow_report_crypto_5m.json" "$slug" <<'PY'
import json, sys
path, slug = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
for row in data.get("rows", []):
    if row.get("slug") == slug:
        print("```json")
        print(json.dumps(row, ensure_ascii=False, indent=2))
        print("```")
        break
else:
    print("_row not found_")
PY
    else
      echo "_report missing_"
    fi
  } > "$snapshot_md"
}

wait_for_log
wait_for_exit_marker

DATE_DIR="$(latest_recorder_date)"
if [[ -z "$DATE_DIR" ]]; then
  echo "no recorder date found for $INSTANCE_ID" >&2
  exit 1
fi
SLUG="$(extract_latest_slug)"

PM_INSTANCE_ID="$INSTANCE_ID" python3 scripts/build_replay_db.py --date "$DATE_DIR"
python3 scripts/export_pgt_shadow_report.py \
  --db "$REPLAY_ROOT/$DATE_DIR/crypto_5m.sqlite" \
  --output-dir "$REPORT_ROOT"
python3 scripts/export_xuan_pgt_gap_report.py \
  --date "$DATE_DIR" \
  --report-json "$REPORT_ROOT/pgt_shadow_report_crypto_5m.json" \
  --output-json "$REPORT_ROOT/xuan_pgt_gap_report_crypto_5m.json"

write_snapshot_markdown "$DATE_DIR" "$SLUG"
