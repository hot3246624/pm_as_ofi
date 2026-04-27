#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INSTANCE_ID="${PM_INSTANCE_ID:-local-agg-lab}"
LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
RECORDER_ROOT="${PM_RECORDER_ROOT:-$ROOT/data/recorder/$INSTANCE_ID}"
BIAS_CACHE_PATH="${PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH:-$ROOT/logs/local_price_agg_bias_cache.shared.json}"
mkdir -p "$LOG_ROOT"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${1:-$LOG_ROOT/local_agg_lab_${STAMP}.log}"

export PM_STRATEGY=oracle_lag_sniping
export PM_DRY_RUN=true
export PM_RECORDER_ENABLED=false
export PM_INSTANCE_ID="$INSTANCE_ID"
export PM_LOG_ROOT="$LOG_ROOT"
export PM_RECORDER_ROOT="$RECORDER_ROOT"
export PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH="$BIAS_CACHE_PATH"
export PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED="${PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED:-false}"

# Lab mode: keep decision/data pipeline alive, but block all trading intents.
export PM_ORACLE_LAG_LAB_ONLY=true

# Run RTDS + LocalAgg shadow compare in parallel.
export PM_LOCAL_PRICE_AGG_ENABLED=true
export PM_LOCAL_PRICE_AGG_DECISION_ENABLED=false

# Keep self-built path optional; default off for clean A/B against RTDS + LocalAgg.
export PM_SELF_BUILT_PRICE_AGG_ENABLED="${PM_SELF_BUILT_PRICE_AGG_ENABLED:-false}"

echo "[$(date '+%F %T')] starting local-agg lab -> $LOG_FILE"
echo "  PM_STRATEGY=$PM_STRATEGY"
echo "  PM_ORACLE_LAG_LAB_ONLY=$PM_ORACLE_LAG_LAB_ONLY"
echo "  PM_LOCAL_PRICE_AGG_ENABLED=$PM_LOCAL_PRICE_AGG_ENABLED"
echo "  PM_LOCAL_PRICE_AGG_DECISION_ENABLED=$PM_LOCAL_PRICE_AGG_DECISION_ENABLED"
echo "  PM_SELF_BUILT_PRICE_AGG_ENABLED=$PM_SELF_BUILT_PRICE_AGG_ENABLED"
echo "  PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH=$PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH"
echo "  PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED=$PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED"

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
