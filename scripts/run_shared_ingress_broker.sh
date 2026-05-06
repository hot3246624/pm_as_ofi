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
SHARED_INGRESS_ROOT="${PM_SHARED_INGRESS_ROOT:-$ROOT/run/shared-ingress-main}"
mkdir -p "$LOG_ROOT" "$INSTANCE_ROOT" "$PID_DIR" "$SHARED_INGRESS_ROOT"
ln -sfn "$LOG_ROOT" "$INSTANCE_ROOT/current"

healthy_manifest_pid() {
  local manifest="$SHARED_INGRESS_ROOT/broker_manifest.json"
  [[ -f "$manifest" ]] || return 1
  python3 - "$manifest" <<'PY'
import json
import os
import sys
import time

path = sys.argv[1]
try:
    with open(path) as f:
        m = json.load(f)
    pid = int(m.get("pid") or 0)
    heartbeat = int(m.get("last_heartbeat_ms") or 0)
    sockets = [
        m.get("chainlink_socket"),
        m.get("local_price_socket"),
        m.get("market_socket"),
    ]
except Exception:
    sys.exit(1)

if pid <= 0 or time.time() * 1000 - heartbeat > 30_000:
    sys.exit(1)
if not all(p and os.path.exists(p) for p in sockets):
    sys.exit(1)
print(pid)
PY
}

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

pid_is_alive() {
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

if EXISTING_BROKER_PID="$(healthy_manifest_pid 2>/dev/null)"; then
  if pid_is_alive "$EXISTING_BROKER_PID"; then
    echo "[$(date '+%F %T')] healthy shared-ingress broker already running: pid=$EXISTING_BROKER_PID"
    echo "[$(date '+%F %T')] not starting duplicate broker; monitoring existing broker"
    echo "$$" > "$RUNNER_PIDFILE"
    cleanup_runner_pidfile() {
      if [[ -f "$RUNNER_PIDFILE" ]]; then
        local pid
        pid="$(cat "$RUNNER_PIDFILE" 2>/dev/null || true)"
        if [[ "$pid" == "$$" ]]; then
          rm -f "$RUNNER_PIDFILE"
        fi
      fi
    }
    trap cleanup_runner_pidfile EXIT INT TERM
    while pid_is_alive "$EXISTING_BROKER_PID"; do
      sleep 10
      if ! healthy_manifest_pid >/dev/null 2>&1; then
        echo "[$(date '+%F %T')] shared-ingress broker manifest became unhealthy; exiting monitor"
        exit 0
      fi
    done
    echo "[$(date '+%F %T')] shared-ingress broker pid exited: $EXISTING_BROKER_PID"
    exit 0
  fi
fi

if [[ -f "$RUNNER_PIDFILE" ]]; then
  OLD_PID="$(cat "$RUNNER_PIDFILE" 2>/dev/null || true)"
  if [[ -n "${OLD_PID:-}" ]] && pid_is_alive "$OLD_PID"; then
    echo "[$(date '+%F %T')] stopping existing broker runner from pidfile: $OLD_PID"
    stop_process_group "$OLD_PID"
  elif [[ -n "${OLD_PID:-}" ]]; then
    echo "[$(date '+%F %T')] removing stale broker pidfile: $OLD_PID"
  fi
  rm -f "$RUNNER_PIDFILE"
fi

LOG_FILE="${1:-$LOG_ROOT/shared_ingress_broker_${STAMP}.log}"

export PM_INSTANCE_ID="$INSTANCE_ID"
export PM_LOG_ROOT="$LOG_ROOT"
export PM_SHARED_INGRESS_ROOT="$SHARED_INGRESS_ROOT"
export PM_SHARED_INGRESS_ROLE=broker
export PM_SHARED_INGRESS_IDLE_EXIT_ENABLED="${PM_SHARED_INGRESS_IDLE_EXIT_ENABLED:-true}"
export PM_DRY_RUN=true
export PM_RECORDER_ENABLED=false
export PM_STRATEGY="${PM_STRATEGY:-oracle_lag_sniping}"
export PM_LOCAL_PRICE_AGG_ENABLED="${PM_LOCAL_PRICE_AGG_ENABLED:-true}"

echo "[$(date '+%F %T')] starting shared-ingress broker -> $LOG_FILE"
echo "  PM_SHARED_INGRESS_ROOT=$PM_SHARED_INGRESS_ROOT"

echo "$$" > "$RUNNER_PIDFILE"
cleanup_runner_pidfile() {
  if [[ -f "$RUNNER_PIDFILE" ]]; then
    local pid
    pid="$(cat "$RUNNER_PIDFILE" 2>/dev/null || true)"
    if [[ "$pid" == "$$" ]]; then
      rm -f "$RUNNER_PIDFILE"
    fi
  fi
}
trap cleanup_runner_pidfile EXIT INT TERM

if [[ -x "$ROOT/target/release/polymarket_v2" ]]; then
  "$ROOT/target/release/polymarket_v2" 2>&1 | tee "$LOG_FILE"
else
  cargo run --bin polymarket_v2 --release 2>&1 | tee "$LOG_FILE"
fi
