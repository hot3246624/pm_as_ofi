#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/logs" "$ROOT/data/replay_recorder"

LOCK_DIR="$ROOT/logs/rebuild_replay.lock"
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
  raw_dir="$ROOT/data/recorder/$day"
  if [[ ! -d "$raw_dir" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip day=$day (raw dir missing)"
    continue
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] build replay day=$day"
  /usr/bin/python3 "$ROOT/scripts/build_replay_db.py" \
    --input-root "$ROOT/data/recorder" \
    --output-root "$ROOT/data/replay_recorder" \
    --date "$day"
done
