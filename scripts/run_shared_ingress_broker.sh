#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-shared-ingress-broker}"
INSTANCE_ROOT="$ROOT/logs/$INSTANCE_ID"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_ROOT="${PM_LOG_ROOT:-$INSTANCE_ROOT/runs/$STAMP}"
PID_DIR="$ROOT/pids/$INSTANCE_ID"
RUNNER_PIDFILE="$PID_DIR/shared_ingress_broker.pid"
SHARED_INGRESS_ROOT="${PM_SHARED_INGRESS_ROOT:-$INSTANCE_ROOT/shared-ingress}"
mkdir -p "$LOG_ROOT" "$INSTANCE_ROOT" "$PID_DIR" "$SHARED_INGRESS_ROOT"
ln -sfn "$LOG_ROOT" "$INSTANCE_ROOT/current"

stop_process_group() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null || return 0
  local pgid
  pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || true)"
  if [[ -n "$pgid" ]]; then
    kill -- "-$pgid" 2>/dev/null || true
    sleep 1
  fi
  kill "$pid" 2>/dev/null || true
}

if [[ -f "$RUNNER_PIDFILE" ]]; then
  OLD_PID="$(cat "$RUNNER_PIDFILE" 2>/dev/null || true)"
  if [[ -n "${OLD_PID:-}" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[$(date '+%F %T')] stopping existing broker runner from pidfile: $OLD_PID"
    stop_process_group "$OLD_PID"
  fi
  rm -f "$RUNNER_PIDFILE"
fi

LOG_FILE="${1:-$LOG_ROOT/shared_ingress_broker_${STAMP}.log}"

export PM_INSTANCE_ID="$INSTANCE_ID"
export PM_LOG_ROOT="$LOG_ROOT"
export PM_SHARED_INGRESS_ROOT="$SHARED_INGRESS_ROOT"
export PM_SHARED_INGRESS_ROLE=broker
export PM_DRY_RUN=true
export PM_RECORDER_ENABLED=false
export PM_STRATEGY="${PM_STRATEGY:-oracle_lag_sniping}"
export PM_LOCAL_PRICE_AGG_ENABLED="${PM_LOCAL_PRICE_AGG_ENABLED:-true}"

echo "[$(date '+%F %T')] starting shared-ingress broker -> $LOG_FILE"
echo "  PM_SHARED_INGRESS_ROOT=$PM_SHARED_INGRESS_ROOT"

echo "$$" > "$RUNNER_PIDFILE"
trap 'rm -f "$RUNNER_PIDFILE"' EXIT

if [[ -x "$ROOT/target/release/polymarket_v2" ]]; then
  "$ROOT/target/release/polymarket_v2" 2>&1 | tee "$LOG_FILE"
else
  cargo run --bin polymarket_v2 --release 2>&1 | tee "$LOG_FILE"
fi
