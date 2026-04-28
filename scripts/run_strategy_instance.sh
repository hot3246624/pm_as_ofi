#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STRATEGY="${PM_STRATEGY:-oracle_lag_sniping}"

case "$STRATEGY" in
  oracle_lag_sniping|post_close_hype)
    exec bash "$ROOT/scripts/run_local_agg_lab.sh" "$@"
    ;;
  pair_gated_tranche_arb)
    exec bash "$ROOT/scripts/run_pgt_fixed_shadow_next.sh" "$@"
    ;;
  *)
    cat >&2 <<EOF
unsupported PM_STRATEGY for run_strategy_instance.sh: $STRATEGY

supported:
  - oracle_lag_sniping (alias: post_close_hype)
  - pair_gated_tranche_arb

use the strategy-specific launcher directly if you need another mode.
EOF
    exit 2
    ;;
esac
