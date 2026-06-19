#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB="${1:-}"
if [[ -z "${DB}" ]]; then
  echo "Usage: $0 <path-to-sqlite-db>" >&2
  exit 1
fi

if [[ ! -f "${DB}" ]]; then
  echo "db not found: ${DB}" >&2
  exit 1
fi

POLY_BT_ROOT="${POLY_BT_ROOT:-/Users/hot/web3Scientist/poly_backtest_data}"
export POLY_BT_ROOT

modes=(nagi balanced ce25)
seeds=(20260611 20260612 20260613)

for mode in "${modes[@]}"; do
  for seed in "${seeds[@]}"; do
    echo "[blast] mode=${mode} seed=${seed}"
    uv run --with duckdb python "${ROOT_DIR}/scripts/xuan_b27_dplus_pair_arb_autoresearch.py" \
      --db "${DB}" \
      --research-mode "${mode}" \
      --max-runs 120 \
      --max-wall-clock-seconds 10800 \
      --saturation-patience-generations 2 \
      --saturation-score-delta 1.0 \
      --saturation-pnl-delta 0.005 \
      --infer-settlement-outcome \
      --generations 4 \
      --seed-size 12 \
      --population 12 \
      --keep-top 4 \
      --exploration 2 \
      --workers 4 \
      --limit 1000 \
      --skip 0 \
      --random-seed "${seed}" \
      --output-dir "${ROOT_DIR}/xuan_research_artifacts/xuan_b27_dplus_autoresearch_${mode}_${seed}_$(date -u +%Y%m%dT%H%M%SZ)"
  done
done
