#!/bin/bash
# Polymarket V2 做市器停止脚本
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${RED}🛑 Stopping Polymarket V2 Market Makers${NC}"
echo ""

STOP_ALL=0
if [ "${1:-}" = "--all" ]; then
    STOP_ALL=1
fi

INSTANCE_ID="${PM_INSTANCE_ID:-}"

stop_process_group() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null || return 0

    local pgid
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || true)"

    if [ -n "$pgid" ]; then
        kill -- "-$pgid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 -- "-$pgid" 2>/dev/null || true
            sleep 1
        fi
    fi

    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 1
    fi
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
    fi
}

stop_log_writers() {
    local log_root="$1"
    [ -d "$log_root" ] || return 0
    command -v lsof >/dev/null 2>&1 || return 0

    writer_pids=()
    while IFS= read -r pid; do
        [ -n "$pid" ] || continue
        writer_pids+=("$pid")
    done < <(
        lsof +D "$log_root" 2>/dev/null \
            | awk 'NR > 1 {print $2}' \
            | sort -u
    )

    if [ "${#writer_pids[@]}" -gt 0 ]; then
        for pid in "${writer_pids[@]}"; do
            [ -n "$pid" ] || continue
            if kill -0 "$pid" 2>/dev/null; then
                stop_process_group "$pid"
                echo -e "  ${GREEN}✓${NC} Stopped writer under $log_root (PID $pid)"
                stopped=$((stopped + 1))
            fi
        done
    fi
}

stopped=0
if [ "$STOP_ALL" -eq 1 ]; then
    PID_DIRS=("$ROOT"/pids "$ROOT"/pids/*)
elif [ -n "$INSTANCE_ID" ]; then
    PID_DIRS=("$ROOT/pids/$INSTANCE_ID")
else
    PID_DIRS=("$ROOT/pids")
fi

for pid_dir in "${PID_DIRS[@]}"; do
    [ -d "$pid_dir" ] || continue
    for pidfile in "$pid_dir"/*.pid; do
        [ -f "$pidfile" ] || continue

        market=$(basename "$pidfile" .pid)
        pid=$(cat "$pidfile")

        if kill -0 "$pid" 2>/dev/null; then
            stop_process_group "$pid"
            echo -e "  ${GREEN}✓${NC} Stopped $market (PID $pid)"
            stopped=$((stopped + 1))
        else
            echo "  ⚠️  $market (PID $pid) already stopped"
        fi
        rm -f "$pidfile"
    done
done

if [ "$STOP_ALL" -eq 1 ]; then
    for log_root in "$ROOT"/logs/*; do
        [ -d "$log_root" ] || continue
        stop_log_writers "$log_root"
    done
elif [ -n "$INSTANCE_ID" ]; then
    LOG_ROOT="${PM_LOG_ROOT:-$ROOT/logs/$INSTANCE_ID}"
    stop_log_writers "$LOG_ROOT"
fi

if [ $stopped -eq 0 ]; then
    echo "No running processes found."
else
    echo ""
    echo -e "${GREEN}✅ Stopped $stopped market maker(s)${NC}"
fi
