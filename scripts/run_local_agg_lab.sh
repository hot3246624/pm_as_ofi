#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/logs"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${1:-$ROOT/logs/local_agg_lab_${STAMP}.log}"

export PM_STRATEGY=oracle_lag_sniping
export PM_DRY_RUN=true
export PM_RECORDER_ENABLED=false

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

cargo run --bin polymarket_v2 --release 2>&1 | tee "$LOG_FILE"

