#!/usr/bin/env bash
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PREFIX="${1:-btc-updown-5m}"
INSTANCE_ID="${PM_INSTANCE_ID:-pgt-shadow-loop}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
SHARED_INGRESS_ROOT="${PM_SHARED_INGRESS_ROOT:-$ROOT/run/shared-ingress-main}"
SHARED_INGRESS_ROLE="${PM_SHARED_INGRESS_ROLE:-auto}"
PGT_SHADOW_PROFILE="${PM_PGT_SHADOW_PROFILE:-xuan_ladder_v1}"
BACKOFF_SEC="${PM_PGT_SHADOW_LOOP_BACKOFF_SEC:-5}"
MAX_ROUNDS="${PM_PGT_SHADOW_LOOP_MAX_ROUNDS:-0}"
LOOP_LOG="${PM_PGT_SHADOW_LOOP_LOG:-$LOG_ROOT/pgt_shadow_loop.log}"

mkdir -p "$LOG_ROOT" "$RECORDER_ROOT" "$SHARED_INGRESS_ROOT"

trap 'echo "[$(date "+%Y-%m-%d %H:%M:%S")] pgt shadow loop interrupted; exiting" >> "$LOOP_LOG"; exit 130' INT TERM

round=0
while true; do
  round=$((round + 1))
  started_at="$(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "[$started_at] pgt shadow loop round=$round prefix=$PREFIX instance_id=$INSTANCE_ID"
    echo "[$started_at] shared_ingress_role=$SHARED_INGRESS_ROLE shared_ingress_root=$SHARED_INGRESS_ROOT profile=$PGT_SHADOW_PROFILE"
  } >> "$LOOP_LOG"

  PM_INSTANCE_ID="$INSTANCE_ID" \
  PM_LOG_ROOT="$LOG_ROOT" \
  PM_RECORDER_ROOT="$RECORDER_ROOT" \
  PM_SHARED_INGRESS_ROLE="$SHARED_INGRESS_ROLE" \
  PM_SHARED_INGRESS_ROOT="$SHARED_INGRESS_ROOT" \
  PM_STRATEGY=pair_gated_tranche_arb \
  PM_PGT_SHADOW_PROFILE="$PGT_SHADOW_PROFILE" \
    bash "$ROOT/scripts/run_strategy_instance.sh" "$PREFIX" >> "$LOOP_LOG" 2>&1
  exit_code=$?

  ended_at="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "[$ended_at] pgt shadow loop round=$round exited code=$exit_code" >> "$LOOP_LOG"

  if (( exit_code == 130 || exit_code == 143 )); then
    echo "[$ended_at] pgt shadow loop child interrupted; exiting" >> "$LOOP_LOG"
    exit "$exit_code"
  fi

  if [[ "$MAX_ROUNDS" =~ ^[0-9]+$ ]] && (( MAX_ROUNDS > 0 && round >= MAX_ROUNDS )); then
    echo "[$ended_at] pgt shadow loop reached max rounds=$MAX_ROUNDS; exiting" >> "$LOOP_LOG"
    exit "$exit_code"
  fi

  echo "[$ended_at] pgt shadow loop restart in ${BACKOFF_SEC}s" >> "$LOOP_LOG"
  sleep "$BACKOFF_SEC"
done
