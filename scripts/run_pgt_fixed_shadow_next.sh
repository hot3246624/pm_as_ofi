#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PREFIX="${1:-btc-updown-5m}"
ROUND_OFFSET="${PM_FIXED_ROUND_OFFSET:-1}"
MIN_REMAINING_SECS="${PM_FIXED_MIN_REMAINING_SECS:-240}"

if [[ "$ROUND_OFFSET" =~ ^[0-9]+$ ]] && [[ "$MIN_REMAINING_SECS" =~ ^[0-9]+$ ]]; then
  now="$(date +%s)"
  current_base="$(( (now / 300) * 300 ))"
  target_end="$(( current_base + (300 * ROUND_OFFSET) ))"
  remaining="$(( target_end - now ))"
  if (( remaining < MIN_REMAINING_SECS )); then
    ROUND_OFFSET="$(( ROUND_OFFSET + 1 ))"
  fi
fi

resolve_env=""
if ! resolve_env="$(/usr/bin/python3 "$ROOT/scripts/resolve_market_ids.py" --prefix "$PREFIX" --round-offset "$ROUND_OFFSET" --format env)"; then
  echo "market_resolve_failed prefix=$PREFIX round_offset=$ROUND_OFFSET" >&2
  exit 75
fi
if [[ -z "$resolve_env" ]]; then
  echo "market_resolve_failed_empty prefix=$PREFIX round_offset=$ROUND_OFFSET" >&2
  exit 75
fi
eval "$resolve_env"
for required_var in POLYMARKET_MARKET_SLUG POLYMARKET_MARKET_ID POLYMARKET_YES_ASSET_ID POLYMARKET_NO_ASSET_ID; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "market_resolve_missing_${required_var} prefix=$PREFIX round_offset=$ROUND_OFFSET" >&2
    exit 75
  fi
done

INSTANCE_ID="${PM_INSTANCE_ID:-pgt-fixed-${POLYMARKET_MARKET_SLUG}}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
SHARED_INGRESS_ROLE="${PM_SHARED_INGRESS_ROLE:-auto}"
SHARED_INGRESS_ROOT="${PM_SHARED_INGRESS_ROOT:-$ROOT/run/shared-ingress-main}"
PGT_SHADOW_PROFILE="${PM_PGT_SHADOW_PROFILE:-replay_focused_v1}"
PGT_PAIR_TARGET="${PM_PAIR_TARGET:-0.975}"
PGT_OPEN_PAIR_BAND="${PM_OPEN_PAIR_BAND:-0.98}"
BINARY="$ROOT/target/debug/polymarket_v2"

mkdir -p "$LOG_ROOT" "$RECORDER_ROOT" "$SHARED_INGRESS_ROOT"

if [[ ! -x "$BINARY" ]] || find "$ROOT/src" "$ROOT/scripts" -type f -newer "$BINARY" | grep -q .; then
  cargo build --bin polymarket_v2
fi

echo "slug=$POLYMARKET_MARKET_SLUG"
echo "market_id=$POLYMARKET_MARKET_ID"
echo "yes_asset_id=$POLYMARKET_YES_ASSET_ID"
echo "no_asset_id=$POLYMARKET_NO_ASSET_ID"
echo "round_offset=$ROUND_OFFSET"
echo "min_remaining_secs=$MIN_REMAINING_SECS"
echo "instance_id=$INSTANCE_ID"
echo "shared_ingress_role=$SHARED_INGRESS_ROLE"
echo "shared_ingress_root=$SHARED_INGRESS_ROOT"
echo "pgt_shadow_profile=$PGT_SHADOW_PROFILE"
echo "pair_target=$PGT_PAIR_TARGET"
echo "open_pair_band=$PGT_OPEN_PAIR_BAND"

exec env \
  -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
  -u PM_MULTI_MARKET_PREFIXES -u PM_MULTI_MARKET_CHILD -u PM_ORACLE_LAG_SYMBOL_UNIVERSE \
  PM_DRY_RUN=true \
  PM_STRATEGY=pair_gated_tranche_arb \
  PM_PGT_SHADOW_PROFILE="$PGT_SHADOW_PROFILE" \
  PM_PAIR_TARGET="$PGT_PAIR_TARGET" \
  PM_OPEN_PAIR_BAND="$PGT_OPEN_PAIR_BAND" \
  PM_RECORDER_ENABLED=true \
  PM_AUTO_CLAIM=false \
  PM_INSTANCE_ID="$INSTANCE_ID" \
  PM_LOG_ROOT="$LOG_ROOT" \
  PM_RECORDER_ROOT="$RECORDER_ROOT" \
  PM_SHARED_INGRESS_ROLE="$SHARED_INGRESS_ROLE" \
  PM_SHARED_INGRESS_ROOT="$SHARED_INGRESS_ROOT" \
  POLYMARKET_MARKET_SLUG="$POLYMARKET_MARKET_SLUG" \
  POLYMARKET_MARKET_ID="$POLYMARKET_MARKET_ID" \
  POLYMARKET_YES_ASSET_ID="$POLYMARKET_YES_ASSET_ID" \
  POLYMARKET_NO_ASSET_ID="$POLYMARKET_NO_ASSET_ID" \
  "$BINARY"
