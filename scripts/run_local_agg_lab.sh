#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-local-agg-lab}"
INSTANCE_ROOT="$ROOT/logs/$INSTANCE_ID"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_ROOT="${PM_LOG_ROOT:-$INSTANCE_ROOT/runs/$STAMP}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
PID_DIR="$ROOT/pids/$INSTANCE_ID"
RUNNER_PIDFILE="$PID_DIR/local_agg_lab.pid"
BIAS_CACHE_PATH="${PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH:-$INSTANCE_ROOT/local_price_agg_bias_cache.instance.json}"
UNCERTAINTY_GATE_MODEL_PATH="${PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH:-$INSTANCE_ROOT/monitor_reports/local_agg_uncertainty_gate_model.latest.json}"
mkdir -p "$LOG_ROOT" "$INSTANCE_ROOT" "$PID_DIR"
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

pid_is_alive() {
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

if [[ -f "$RUNNER_PIDFILE" ]]; then
  OLD_PID="$(cat "$RUNNER_PIDFILE" 2>/dev/null || true)"
  if [[ -n "${OLD_PID:-}" ]] && pid_is_alive "$OLD_PID"; then
    echo "[$(date '+%F %T')] stopping existing lab runner from pidfile: $OLD_PID"
    stop_process_group "$OLD_PID"
  elif [[ -n "${OLD_PID:-}" ]]; then
    echo "[$(date '+%F %T')] removing stale lab pidfile: $OLD_PID"
  fi
  rm -f "$RUNNER_PIDFILE"
fi

# Same-instance single-writer guard:
# if an older lab is still writing under this LOG_ROOT, stop it before
# starting a new challenger. This prevents duplicated compare lines and
# polluted coverage stats.
if command -v lsof >/dev/null 2>&1; then
  EXISTING_PIDS=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    EXISTING_PIDS+=("$pid")
  done < <(
    lsof +D "$LOG_ROOT" 2>/dev/null \
      | awk 'NR > 1 {print $2}' \
      | sort -u
  )
  if [ "${#EXISTING_PIDS[@]}" -gt 0 ]; then
    echo "[$(date '+%F %T')] stopping existing lab writers under $LOG_ROOT: ${EXISTING_PIDS[*]}"
    for pid in "${EXISTING_PIDS[@]}"; do
      [[ "$pid" = "$$" ]] && continue
      stop_process_group "$pid"
    done
    sleep 1
  fi
fi

LOG_FILE="${1:-$LOG_ROOT/local_agg_lab_${STAMP}.log}"

export PM_STRATEGY=oracle_lag_sniping
export PM_DRY_RUN=true
export PM_RECORDER_ENABLED=false
export PM_INSTANCE_ID="$INSTANCE_ID"
export PM_LOG_ROOT="$LOG_ROOT"
export PM_RECORDER_ROOT="$RECORDER_ROOT"
export PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH="$BIAS_CACHE_PATH"
export PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED="${PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED:-false}"
export PM_SHARED_INGRESS_ROLE="${PM_SHARED_INGRESS_ROLE:-auto}"
export PM_SHARED_INGRESS_ROOT="${PM_SHARED_INGRESS_ROOT:-$ROOT/run/shared-ingress-main}"

# Lab mode: keep decision/data pipeline alive, but block all trading intents.
export PM_ORACLE_LAG_LAB_ONLY=true

# Run RTDS + LocalAgg shadow compare in parallel.
export PM_LOCAL_PRICE_AGG_ENABLED=true
export PM_LOCAL_PRICE_AGG_DECISION_ENABLED=false
# Shadow compare adds a fixed 250ms diagnostic grace in Rust; 50ms here gives
# an effective T+300ms close-decision deadline for the dry-run challenger.
export PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS="${PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS:-50}"
export PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED="${PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED:-true}"
export PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH="$UNCERTAINTY_GATE_MODEL_PATH"
export PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS="${PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS:-2500}"

# Keep self-built path optional; default off for clean A/B against RTDS + LocalAgg.
export PM_SELF_BUILT_PRICE_AGG_ENABLED="${PM_SELF_BUILT_PRICE_AGG_ENABLED:-false}"

echo "[$(date '+%F %T')] starting local-agg lab -> $LOG_FILE"
echo "  PM_STRATEGY=$PM_STRATEGY"
echo "  PM_ORACLE_LAG_LAB_ONLY=$PM_ORACLE_LAG_LAB_ONLY"
echo "  PM_LOCAL_PRICE_AGG_ENABLED=$PM_LOCAL_PRICE_AGG_ENABLED"
echo "  PM_LOCAL_PRICE_AGG_DECISION_ENABLED=$PM_LOCAL_PRICE_AGG_DECISION_ENABLED"
echo "  PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS=$PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS"
echo "  PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED=$PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED"
echo "  PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH=$PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH"
echo "  PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS=$PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS"
echo "  PM_SELF_BUILT_PRICE_AGG_ENABLED=$PM_SELF_BUILT_PRICE_AGG_ENABLED"
echo "  PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH=$PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH"
echo "  PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED=$PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED"

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

if [[ -x "$ROOT/scripts/rebuild_local_price_agg_bias_cache.py" || -f "$ROOT/scripts/rebuild_local_price_agg_bias_cache.py" ]]; then
  python3 "$ROOT/scripts/rebuild_local_price_agg_bias_cache.py" \
    --logs-root "$ROOT/logs" \
    --out "$PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH" >/dev/null 2>&1 || true
fi

if [[ -x "$ROOT/target/release/polymarket_v2" ]]; then
  "$ROOT/target/release/polymarket_v2" 2>&1 | tee "$LOG_FILE"
else
  cargo run --bin polymarket_v2 --release 2>&1 | tee "$LOG_FILE"
fi
