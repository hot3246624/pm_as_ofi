#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE_HOST="${PM_LOCALAGG_REMOTE_HOST:-ubuntu@ec2-3-248-230-60.eu-west-1.compute.amazonaws.com}"
REMOTE_ROOT="${PM_LOCALAGG_REMOTE_ROOT:-/srv/pm_as_ofi}"
SSH_KEY="${PM_LOCALAGG_SSH_KEY:-$HOME/.ssh/polymarket-Ireland.pem}"
DEFAULT_GATE_MODEL_SRC="$ROOT/logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json"
if [[ ! -f "$DEFAULT_GATE_MODEL_SRC" ]] && [[ -f "/Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json" ]]; then
  DEFAULT_GATE_MODEL_SRC="/Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json"
fi
GATE_MODEL_SRC="${PM_LOCALAGG_GATE_MODEL_SRC:-$DEFAULT_GATE_MODEL_SRC}"
GATE_MODEL_DST_PRIMARY="${PM_LOCALAGG_GATE_MODEL_DST_PRIMARY:-$REMOTE_ROOT/logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json}"
GATE_MODEL_DST_SECONDARY="${PM_LOCALAGG_GATE_MODEL_DST_SECONDARY:-$REMOTE_ROOT/logs/local_agg_lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json}"

SSH_OPTS=(
  -i "$SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
)

rsync \
  -az \
  --delete \
  --exclude '.git' \
  --exclude '.git/' \
  --exclude 'target/' \
  --exclude 'logs/' \
  --exclude 'data/' \
  --exclude 'pids/' \
  --exclude 'run/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$ROOT/" \
  "$REMOTE_HOST:$REMOTE_ROOT/"

ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" \
  "mkdir -p \
    '$REMOTE_ROOT/logs/local-agg-boundary-challenger-lab/monitor_reports' \
    '$REMOTE_ROOT/logs/local_agg_lab/monitor_reports'"

if [[ -f "$GATE_MODEL_SRC" ]]; then
  scp "${SSH_OPTS[@]}" \
    "$GATE_MODEL_SRC" \
    "$REMOTE_HOST:$GATE_MODEL_DST_PRIMARY"
  scp "${SSH_OPTS[@]}" \
    "$GATE_MODEL_SRC" \
    "$REMOTE_HOST:$GATE_MODEL_DST_SECONDARY"
fi

ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" \
  "cd '$REMOTE_ROOT' && git status -sb && ls -lh target/release/polymarket_v2 2>/dev/null || true"
