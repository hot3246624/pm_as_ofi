#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export POLY_BT_ROOT="${POLY_BT_ROOT:-/Users/hot/web3Scientist/poly_backtest_data}"

uv run --with duckdb python "${ROOT_DIR}/scripts/xuan_b27_dplus_autoresearch_auto_loop.py" "$@"
