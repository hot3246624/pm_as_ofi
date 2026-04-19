#!/bin/bash
# validate_multi_dryrun.sh — Stage G gate checker.
# Scans per-slug log files produced by a multi-market supervisor dry-run
# and asserts the exit-criteria gates from breezy-noodling-owl.md.
#
# Usage: scripts/validate_multi_dryrun.sh [log_dir] [date]
#   log_dir: default ./logs
#   date:    default today (YYYY-MM-DD). Matches the daily-rolling suffix.
#
# Exit code: 0 if all gates pass, non-zero (count of failed gates) otherwise.

set -u

LOG_DIR=${1:-logs}
DATE=${2:-$(date +%Y-%m-%d)}

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass=0
fail=0

gate() {
    local label="$1"
    local ok="$2"
    local detail="$3"
    if [ "$ok" = "1" ]; then
        echo -e "  ${GREEN}✓${NC} $label — $detail"
        pass=$((pass + 1))
    else
        echo -e "  ${RED}✗${NC} $label — $detail"
        fail=$((fail + 1))
    fi
}

# Locate slug files for today
shopt -s nullglob
FILES=("$LOG_DIR"/polymarket.*-updown-*.log.$DATE)
if [ ${#FILES[@]} -eq 0 ]; then
    echo -e "${RED}No per-slug log files found matching $LOG_DIR/polymarket.*-updown-*.log.$DATE${NC}"
    echo "Expected format: polymarket.<slug>.log.YYYY-MM-DD (produced by the supervisor after Stage F)."
    exit 99
fi

echo -e "${YELLOW}Validating ${#FILES[@]} per-slug log file(s) for $DATE${NC}"
echo ""

for f in "${FILES[@]}"; do
    slug=$(basename "$f" | command sed -E "s/^polymarket\.(.+)\.log\.$DATE$/\1/")
    echo -e "${YELLOW}── $slug ──${NC}"

    # Gate 1: hub stability — 0 WS connect errors
    ws_err=$(command grep -c "WS connect error" "$f" 2>/dev/null || echo 0)
    ws_err=${ws_err//[^0-9]/}
    ws_err=${ws_err:-0}
    [ "$ws_err" -eq 0 ] && g=1 || g=0
    gate "WS stability"        "$g" "ws_connect_errors=$ws_err"

    # Gate 2: round coverage — at least one winner_hint emitted, with resolved ratio >= 0.90
    emits=$(command grep -c "post_close_emit_winner_hint" "$f" 2>/dev/null || echo 0)
    unresolved=$(command grep -c "chainlink_winner_hint_unresolved" "$f" 2>/dev/null || echo 0)
    emits=${emits//[^0-9]/}; emits=${emits:-0}
    unresolved=${unresolved//[^0-9]/}; unresolved=${unresolved:-0}
    total=$((emits + unresolved))
    if [ "$total" -gt 0 ]; then
        ratio=$(awk "BEGIN{printf \"%.2f\", $emits / $total}")
        if awk "BEGIN{exit !($emits / $total >= 0.90)}"; then g=1; else g=0; fi
    else
        ratio="n/a"; g=0
    fi
    gate "Round resolve ≥90%"  "$g" "emits=$emits unresolved=$unresolved ratio=$ratio"

    # Gate 3: FAK per-round cap — no line shows shots > 3/3
    over=$(command grep -E "shots=[4-9]/3|shots=[1-9][0-9]+/3" "$f" 2>/dev/null | wc -l | tr -d ' ')
    [ "$over" -eq 0 ] && g=1 || g=0
    gate "FAK cap ≤3/round"    "$g" "violations=$over"

    # Gate 4: maker fallback wiring — every no_ask skip has a maker payload
    noask=$(command grep -c "oracle_lag_fak_skip.*reason=no_ask" "$f" 2>/dev/null || echo 0)
    maker=$(command grep -c "oracle_lag_fak_maker_payload" "$f" 2>/dev/null || echo 0)
    noask=${noask//[^0-9]/}; noask=${noask:-0}
    maker=${maker//[^0-9]/}; maker=${maker:-0}
    if [ "$noask" -eq 0 ]; then
        g=1
        detail="no_ask=0 (skip)"
    elif [ "$maker" -ge "$noask" ]; then
        g=1
        detail="no_ask=$noask maker_payloads=$maker"
    else
        g=0
        detail="no_ask=$noask maker_payloads=$maker — missing $((noask - maker))"
    fi
    gate "Maker fallback wired" "$g" "$detail"

    # Gate 5: latency — extract latency_from_end_ms values and compute p50/p95
    lat_vals=$(command grep -oE "latency_from_end_ms=[0-9]+" "$f" 2>/dev/null \
        | command sed 's/latency_from_end_ms=//' | sort -n)
    lat_count=$(echo "$lat_vals" | command grep -c . || echo 0)
    lat_count=${lat_count//[^0-9]/}; lat_count=${lat_count:-0}
    if [ "$lat_count" -gt 0 ]; then
        p50_idx=$(( (lat_count + 1) / 2 ))
        p95_idx=$(( (lat_count * 95 + 99) / 100 ))
        [ "$p95_idx" -gt "$lat_count" ] && p95_idx=$lat_count
        p50=$(echo "$lat_vals" | command sed -n "${p50_idx}p")
        p95=$(echo "$lat_vals" | command sed -n "${p95_idx}p")
        # Latency is dominated by Chainlink Data Streams tick delivery at the round-end
        # boundary (~1s inherent). Target: p50 ≤ 1500ms, p95 ≤ 2000ms.
        if [ "${p50:-0}" -le 1500 ] && [ "${p95:-0}" -le 2000 ]; then g=1; else g=0; fi
        gate "Latency p50≤1500 p95≤2000" "$g" "p50=${p50}ms p95=${p95}ms n=$lat_count"
    else
        gate "Latency p50≤1500 p95≤2000" 0 "no latency_from_end_ms samples"
    fi

    echo ""
done

echo "═══════════════════════════════════════════"
echo -e "Gates passed: ${GREEN}$pass${NC}   failed: ${RED}$fail${NC}"
if [ "$fail" -eq 0 ]; then
    echo -e "${GREEN}✅ ALL GATES PASS${NC}"
    exit 0
else
    echo -e "${RED}❌ $fail gate(s) failed${NC}"
    exit "$fail"
fi
