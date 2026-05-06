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
BACKOFF_SEC="${PM_PGT_SHADOW_LOOP_BACKOFF_SEC:-1}"
ERROR_BACKOFF_SEC="${PM_PGT_SHADOW_LOOP_ERROR_BACKOFF_SEC:-60}"
MAX_ROUNDS="${PM_PGT_SHADOW_LOOP_MAX_ROUNDS:-0}"
LOOP_LOG="${PM_PGT_SHADOW_LOOP_LOG:-$LOG_ROOT/pgt_shadow_loop.log}"
INTERVAL_SECS="${PM_PGT_SHADOW_LOOP_INTERVAL_SECS:-300}"
MIN_REMAINING_SECS="${PM_PGT_SHADOW_LOOP_MIN_REMAINING_SECS:-180}"
FIXED_PRESTART_SECS="${PM_PGT_FIXED_PRESTART_SECS:-10}"
POST_CLOSE_GRACE_SECS="${PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS:-45}"
FIXED_STALE_SKIP_GRACE_SECS="${PM_PGT_FIXED_STALE_SKIP_GRACE_SECS:-$POST_CLOSE_GRACE_SECS}"
OVERLAP_LOOP="${PM_PGT_SHADOW_LOOP_OVERLAP:-true}"
BINARY="$ROOT/target/debug/polymarket_v2"
BUILD_ONCE="${PM_PGT_SHADOW_BUILD_ONCE:-true}"
FIXED_AUTO_BUILD="${PM_PGT_FIXED_AUTO_BUILD:-false}"

mkdir -p "$LOG_ROOT" "$RECORDER_ROOT" "$SHARED_INGRESS_ROOT"

trap 'echo "[$(date "+%Y-%m-%d %H:%M:%S")] pgt shadow loop interrupted; stopping children and exiting" >> "$LOOP_LOG"; pids="$(jobs -pr)"; if [[ -n "$pids" ]]; then kill $pids 2>/dev/null || true; fi; exit 130' INT TERM

is_truthy() {
  case "${1:-}" in
    1|true|True|TRUE|yes|Yes|YES|on|On|ON) return 0 ;;
    *) return 1 ;;
  esac
}

case "$BUILD_ONCE" in
  0|false|False|FALSE|no|No|NO|off|Off|OFF)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] pgt shadow loop build_once=disabled binary=$BINARY fixed_auto_build=$FIXED_AUTO_BUILD" >> "$LOOP_LOG"
    ;;
  *)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] pgt shadow loop build_once=start binary=$BINARY fixed_auto_build=$FIXED_AUTO_BUILD" >> "$LOOP_LOG"
    if [[ ! -x "$BINARY" ]] || find "$ROOT/src" "$ROOT/Cargo.toml" "$ROOT/Cargo.lock" -type f -newer "$BINARY" 2>/dev/null | grep -q .; then
      if ! cargo build --bin polymarket_v2 >> "$LOOP_LOG" 2>&1; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] pgt shadow loop build_once=failed" >> "$LOOP_LOG"
        exit 75
      fi
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] pgt shadow loop build_once=done binary=$BINARY fixed_auto_build=$FIXED_AUTO_BUILD" >> "$LOOP_LOG"
    ;;
esac

choose_round_offset() {
  if [[ -n "${PM_PGT_SHADOW_LOOP_ROUND_OFFSET:-}" ]]; then
    echo "$PM_PGT_SHADOW_LOOP_ROUND_OFFSET"
    return
  fi
  if [[ ! "$INTERVAL_SECS" =~ ^[0-9]+$ ]] || [[ ! "$MIN_REMAINING_SECS" =~ ^[0-9]+$ ]] || (( INTERVAL_SECS <= 0 )); then
    echo "1"
    return
  fi

  # Polymarket crypto 5m slug timestamps are round starts. The fixed launcher
  # treats its min-remaining guard as if the slug timestamp were an end time, so
  # the loop computes the offset itself and disables that guard below.
  local now current_start elapsed remaining
  now="$(date +%s)"
  current_start="$(( (now / INTERVAL_SECS) * INTERVAL_SECS ))"
  elapsed="$(( now - current_start ))"
  remaining="$(( INTERVAL_SECS - elapsed ))"
  if (( remaining >= MIN_REMAINING_SECS )); then
    echo "0"
  else
    echo "1"
  fi
}

choose_prelaunch_round_offset() {
  local now_input="${1:-}"
  if [[ -n "${PM_PGT_SHADOW_LOOP_ROUND_OFFSET:-}" ]]; then
    echo "$PM_PGT_SHADOW_LOOP_ROUND_OFFSET"
    return
  fi
  if [[ ! "$INTERVAL_SECS" =~ ^[0-9]+$ ]] || [[ ! "$FIXED_PRESTART_SECS" =~ ^[0-9]+$ ]] || (( INTERVAL_SECS <= 0 )); then
    echo "1"
    return
  fi

  local now current_start current_launch
  now="${now_input:-$(date +%s)}"
  current_start="$(( (now / INTERVAL_SECS) * INTERVAL_SECS ))"
  current_launch="$(( current_start - FIXED_PRESTART_SECS ))"
  if (( now <= current_launch )); then
    echo "0"
  else
    echo "1"
  fi
}

target_start_for_offset() {
  local offset="${1:-1}"
  local now_input="${2:-}"
  local now current_start
  now="${now_input:-$(date +%s)}"
  current_start="$(( (now / INTERVAL_SECS) * INTERVAL_SECS ))"
  echo "$(( current_start + (INTERVAL_SECS * offset) ))"
}

target_stale_at() {
  local target_start="$1"
  local grace="${FIXED_STALE_SKIP_GRACE_SECS:-2}"
  if [[ ! "$INTERVAL_SECS" =~ ^[0-9]+$ ]] || [[ ! "$grace" =~ ^[0-9]+$ ]]; then
    grace=2
  fi
  echo "$(( target_start + INTERVAL_SECS + grace ))"
}

run_fixed_child() {
  local fixed_round_offset="$1"
  PM_INSTANCE_ID="$INSTANCE_ID" \
  PM_LOG_ROOT="$LOG_ROOT" \
  PM_RECORDER_ROOT="$RECORDER_ROOT" \
  PM_SHARED_INGRESS_ROLE="$SHARED_INGRESS_ROLE" \
  PM_SHARED_INGRESS_ROOT="$SHARED_INGRESS_ROOT" \
  PM_STRATEGY=pair_gated_tranche_arb \
  PM_PGT_SHADOW_PROFILE="$PGT_SHADOW_PROFILE" \
  PM_FIXED_ROUND_OFFSET="$fixed_round_offset" \
  PM_FIXED_MIN_REMAINING_SECS="${PM_FIXED_MIN_REMAINING_SECS:-disabled}" \
  PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS="$POST_CLOSE_GRACE_SECS" \
  PM_PGT_FIXED_INTERVAL_SECS="$INTERVAL_SECS" \
  PM_PGT_FIXED_STALE_SKIP_GRACE_SECS="$FIXED_STALE_SKIP_GRACE_SECS" \
  PM_PGT_SHADOW_REDEEM_LIFECYCLE_ENABLED="${PM_PGT_SHADOW_REDEEM_LIFECYCLE_ENABLED:-false}" \
  PM_PGT_FIXED_AUTO_BUILD="$FIXED_AUTO_BUILD" \
  PM_PGT_FIXED_PRESTART_SECS="$FIXED_PRESTART_SECS" \
  PM_PGT_FIXED_INSTANCE_PER_ROUND="${PM_PGT_FIXED_INSTANCE_PER_ROUND:-false}" \
    bash "$ROOT/scripts/run_strategy_instance.sh" "$PREFIX"
}

round=0
if is_truthy "$OVERLAP_LOOP"; then
  while true; do
    round=$((round + 1))
    started_at="$(date '+%Y-%m-%d %H:%M:%S')"
    schedule_now="$(date +%s)"
    fixed_round_offset="$(choose_prelaunch_round_offset "$schedule_now")"
    target_start="$(target_start_for_offset "$fixed_round_offset" "$schedule_now")"
    stale_at="$(target_stale_at "$target_start")"
    if (( schedule_now >= stale_at )); then
      echo "[$started_at] pgt shadow overlap stale schedule skipped prefix=$PREFIX instance_id=$INSTANCE_ID target_start=$target_start now=$schedule_now stale_at=$stale_at" >> "$LOOP_LOG"
      sleep "$BACKOFF_SEC"
      continue
    fi
    {
      echo "[$started_at] pgt shadow overlap schedule=$round prefix=$PREFIX instance_id=$INSTANCE_ID"
      echo "[$started_at] shared_ingress_role=$SHARED_INGRESS_ROLE shared_ingress_root=$SHARED_INGRESS_ROOT profile=$PGT_SHADOW_PROFILE fixed_round_offset=$fixed_round_offset target_start=$target_start prestart_secs=$FIXED_PRESTART_SECS post_close_grace_secs=$POST_CLOSE_GRACE_SECS stale_at=$stale_at"
    } >> "$LOOP_LOG"

    (
      child_started_at="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "[$child_started_at] pgt shadow overlap child start schedule=$round target_start=$target_start offset=$fixed_round_offset" >> "$LOOP_LOG"
      PM_PGT_FIXED_INSTANCE_PER_ROUND="${PM_PGT_FIXED_INSTANCE_PER_ROUND:-true}" run_fixed_child "$fixed_round_offset" >> "$LOOP_LOG" 2>&1
      exit_code=$?
      child_ended_at="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "[$child_ended_at] pgt shadow overlap child schedule=$round target_start=$target_start exited code=$exit_code" >> "$LOOP_LOG"
    ) &
    child_pid=$!
    echo "[$started_at] pgt shadow overlap child pid=$child_pid schedule=$round target_start=$target_start" >> "$LOOP_LOG"

    if [[ "$MAX_ROUNDS" =~ ^[0-9]+$ ]] && (( MAX_ROUNDS > 0 && round >= MAX_ROUNDS )); then
      echo "[$started_at] pgt shadow loop reached max schedules=$MAX_ROUNDS; exiting without killing children" >> "$LOOP_LOG"
      exit 0
    fi

    now="$(date +%s)"
    next_schedule_at="$(( target_start + 1 ))"
    sleep_secs="$BACKOFF_SEC"
    if (( next_schedule_at > now )); then
      sleep_secs="$(( next_schedule_at - now ))"
    fi
    echo "[$started_at] pgt shadow overlap next schedule in ${sleep_secs}s" >> "$LOOP_LOG"
    sleep "$sleep_secs"
  done
fi

while true; do
  round=$((round + 1))
  started_at="$(date '+%Y-%m-%d %H:%M:%S')"
  fixed_round_offset="$(choose_round_offset)"
  {
    echo "[$started_at] pgt shadow loop round=$round prefix=$PREFIX instance_id=$INSTANCE_ID"
    echo "[$started_at] shared_ingress_role=$SHARED_INGRESS_ROLE shared_ingress_root=$SHARED_INGRESS_ROOT profile=$PGT_SHADOW_PROFILE fixed_round_offset=$fixed_round_offset min_remaining_secs=$MIN_REMAINING_SECS"
  } >> "$LOOP_LOG"

  run_fixed_child "$fixed_round_offset" >> "$LOOP_LOG" 2>&1
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

  restart_delay="$BACKOFF_SEC"
  if (( exit_code != 0 )); then
    restart_delay="$ERROR_BACKOFF_SEC"
  fi
  echo "[$ended_at] pgt shadow loop restart in ${restart_delay}s" >> "$LOOP_LOG"
  sleep "$restart_delay"
done
