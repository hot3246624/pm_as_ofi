#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-default}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
REPLAY_ROOT="${PM_REPLAY_ROOT:-$ROOT/data/replay_recorder/$INSTANCE_ID}"
REPORT_ROOT="${PM_REPLAY_REPORT_ROOT:-$ROOT/data/replay_reports/$INSTANCE_ID}"

mkdir -p "$LOG_ROOT" "$REPLAY_ROOT" "$REPORT_ROOT"

LOCK_DIR="$LOG_ROOT/rebuild_pgt_shadow_reports.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip pgt shadow rebuild (lock held)"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" >/dev/null 2>&1 || true' EXIT

DAYS="$(/usr/bin/python3 - <<'PY'
from datetime import datetime, timedelta, timezone
today = datetime.now(timezone.utc).date()
print(today.isoformat())
print((today - timedelta(days=1)).isoformat())
PY
)"

for day in $DAYS; do
  raw_dir="$RECORDER_ROOT/$day"
  if [[ ! -d "$raw_dir" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip day=$day (raw dir missing)"
    continue
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] build replay day=$day"
  /usr/bin/python3 "$ROOT/scripts/build_replay_db.py" \
    --input-root "$RECORDER_ROOT" \
    --output-root "$REPLAY_ROOT" \
    --date "$day"

  db_path="$REPLAY_ROOT/$day/crypto_5m.sqlite"
  if [[ ! -f "$db_path" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip reports day=$day (db missing: $db_path)"
    continue
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] export pgt shadow report day=$day"
  /usr/bin/python3 "$ROOT/scripts/export_pgt_shadow_report.py" \
    --date "$day" \
    --db "$db_path" \
    --output-dir "$REPORT_ROOT"

  report_json="$REPORT_ROOT/pgt_shadow_report_${day}.json"
  if [[ ! -f "$report_json" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip xuan gap day=$day (missing report: $report_json)"
    continue
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] export xuan gap report day=$day"
  /usr/bin/python3 "$ROOT/scripts/export_xuan_pgt_gap_report.py" \
    --report-json "$report_json" \
    --output-json "$REPORT_ROOT/xuan_pgt_gap_report_${day}.json"
done
