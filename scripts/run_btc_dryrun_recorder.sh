#!/usr/bin/env bash
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-btc-dryrun-recorder}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"

mkdir -p "$LOG_ROOT" "$RECORDER_ROOT"

export PM_MULTI_MARKET_CHILD=1
export PM_DRY_RUN=true
export PM_STRATEGY=pair_arb
export PM_RECORDER_ENABLED=true
export PM_INSTANCE_ID="$INSTANCE_ID"
export PM_LOG_ROOT="$LOG_ROOT"
export PM_RECORDER_ROOT="$RECORDER_ROOT"
export PM_AUTO_CLAIM=false
export POLYMARKET_MARKET_SLUG=btc-updown-5m

BACKOFF_SEC="${PM_RECORDER_RESTART_BACKOFF_SEC:-5}"
LOG_FILE="$LOG_ROOT/btc_dryrun_recorder.log"
BINARY="$ROOT/target/debug/polymarket_v2"

if [[ ! -x "$BINARY" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] build missing binary: $BINARY" >> "$LOG_FILE"
  cargo build --bin polymarket_v2 >> "$LOG_FILE" 2>&1
fi

while true; do
  started_at="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "[$started_at] start btc dry-run recorder loop" >> "$LOG_FILE"

  "$BINARY" >> "$LOG_FILE" 2>&1
  exit_code=$?

  ended_at="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "[$ended_at] recorder exited code=$exit_code, restart in ${BACKOFF_SEC}s" >> "$LOG_FILE"
  sleep "$BACKOFF_SEC"
done
