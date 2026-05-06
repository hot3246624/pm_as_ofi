#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-python3}"
LOGS_ROOT="${LOGS_ROOT:-$ROOT/logs}"

echo "[1/6] build canonical close-only dataset"
"$PYTHON" "$ROOT/scripts/build_local_agg_training_dataset.py" \
  --mode close_only \
  --out-round-csv "$LOGS_ROOT/local_agg_training_rounds.csv" \
  --out-source-csv "$LOGS_ROOT/local_agg_training_sources.csv" \
  --out-canonical-round-csv "$LOGS_ROOT/local_agg_training_canonical_rounds.csv" \
  --out-canonical-source-csv "$LOGS_ROOT/local_agg_training_canonical_sources.csv"

echo "[2/6] build canonical full-shadow dataset"
"$PYTHON" "$ROOT/scripts/build_local_agg_training_dataset.py" \
  --mode full \
  --out-round-csv "$LOGS_ROOT/local_agg_full_training_rounds.csv" \
  --out-source-csv "$LOGS_ROOT/local_agg_full_training_sources.csv" \
  --out-canonical-round-csv "$LOGS_ROOT/local_agg_full_training_canonical_rounds.csv" \
  --out-canonical-source-csv "$LOGS_ROOT/local_agg_full_training_canonical_sources.csv"

echo "[3/6] fit canonical close-only search"
"$PYTHON" "$ROOT/scripts/fit_local_agg_models_from_training_csv.py" \
  --training-csv "$LOGS_ROOT/local_agg_training_canonical_rounds.csv" \
  --test-rounds 6 \
  --grid-start 0.5 --grid-end 1.5 --grid-step 0.5 \
  --close-decay-grid 500,900 \
  --exact-boost-grid 1.0,1.25 \
  --top-k 40 \
  --out-csv "$LOGS_ROOT/local_agg_train_test_search_canonical.csv"

echo "[4/6] fit full-shadow search"
"$PYTHON" "$ROOT/scripts/fit_local_agg_models_from_training_csv.py" \
  --training-csv "$LOGS_ROOT/local_agg_full_training_canonical_rounds.csv" \
  --test-rounds 6 \
  --grid-start 0.5 --grid-end 1.5 --grid-step 0.5 \
  --close-decay-grid 500,900 \
  --exact-boost-grid 1.0,1.25 \
  --top-k 40 \
  --out-csv "$LOGS_ROOT/local_agg_full_train_test_search.csv"

echo "[5/6] walk-forward family comparison (full-shadow)"
"$PYTHON" "$ROOT/scripts/walkforward_local_agg_models.py" \
  --training-csv "$LOGS_ROOT/local_agg_full_training_canonical_rounds.csv" \
  --family close_only \
  --out-csv "$LOGS_ROOT/local_agg_walkforward_full_close_only.csv"
"$PYTHON" "$ROOT/scripts/walkforward_local_agg_models.py" \
  --training-csv "$LOGS_ROOT/local_agg_full_training_canonical_rounds.csv" \
  --family return_anchor \
  --out-csv "$LOGS_ROOT/local_agg_walkforward_full_return_anchor.csv"

echo "[6/13] family recommendation"
"$PYTHON" "$ROOT/scripts/recommend_local_agg_family.py" \
  --logs-root "$LOGS_ROOT" \
  --out-csv "$LOGS_ROOT/local_agg_family_recommendation.csv"

if find "$LOGS_ROOT" -maxdepth 2 -name 'local_price_agg_boundary_tape.jsonl' | grep -q .; then
  echo "[7/13] build boundary close-only dataset"
  "$PYTHON" "$ROOT/scripts/build_local_agg_boundary_dataset.py"     --logs-root "$LOGS_ROOT"     --mode close_only     --out-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv"

  echo "[8/13] boundary close-only rule search"
  "$PYTHON" "$ROOT/scripts/search_local_agg_boundary_rules.py"     --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv"     --out-csv "$LOGS_ROOT/local_agg_boundary_rule_search.csv"

  echo "[9/13] boundary per-symbol recommendation"
  "$PYTHON" "$ROOT/scripts/recommend_local_agg_boundary_rules.py"     --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv"     --out-csv "$LOGS_ROOT/local_agg_boundary_symbol_recommendation.csv"

  echo "[10/13] boundary per-symbol policy evaluation"
  "$PYTHON" "$ROOT/scripts/evaluate_local_agg_boundary_policy.py"     --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv"     --policy-csv "$LOGS_ROOT/local_agg_boundary_symbol_recommendation.csv"     --out-csv "$LOGS_ROOT/local_agg_boundary_policy_eval.csv"

  echo "[11/13] boundary shared-core recommendation"
  "$PYTHON" "$ROOT/scripts/recommend_local_agg_boundary_core_policy.py" \
    --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv" \
    --out-csv "$LOGS_ROOT/local_agg_boundary_core_policy_recommendation.csv"

  echo "[12/13] boundary shared-core policy evaluation"
  "$PYTHON" "$ROOT/scripts/evaluate_local_agg_boundary_policy.py" \
    --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv" \
    --policy-csv "$LOGS_ROOT/local_agg_boundary_core_policy_recommendation.csv" \
    --out-csv "$LOGS_ROOT/local_agg_boundary_core_policy_eval.csv"

  echo "[13/13] boundary weighted recommendation + evaluation"
  "$PYTHON" "$ROOT/scripts/recommend_local_agg_boundary_weighted_policy.py" \
    --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv" \
    --out-csv "$LOGS_ROOT/local_agg_boundary_weighted_recommendation.csv"
  "$PYTHON" "$ROOT/scripts/evaluate_local_agg_boundary_policy.py" \
    --boundary-csv "$LOGS_ROOT/local_agg_boundary_dataset_close_only.csv" \
    --policy-csv "$LOGS_ROOT/local_agg_boundary_weighted_recommendation.csv" \
    --out-csv "$LOGS_ROOT/local_agg_boundary_weighted_policy_eval.csv"
else
  echo "[7/13] boundary tape not found; skipping boundary dataset/search"
fi

echo "done"
