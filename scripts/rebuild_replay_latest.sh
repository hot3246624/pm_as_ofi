#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-default}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
REPLAY_ROOT="${PM_REPLAY_ROOT:-$ROOT/data/replay_recorder/$INSTANCE_ID}"

mkdir -p "$LOG_ROOT" "$REPLAY_ROOT"

LOCK_DIR="$LOG_ROOT/rebuild_replay.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip rebuild (lock held)"
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
done
