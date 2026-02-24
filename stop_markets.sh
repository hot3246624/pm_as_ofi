#!/bin/bash
# Polymarket V2 做市器停止脚本
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${RED}🛑 Stopping Polymarket V2 Market Makers${NC}"
echo ""

PID_DIR="pids"

if [ ! -d "$PID_DIR" ]; then
    echo "No pids/ directory found — nothing to stop."
    exit 0
fi

stopped=0
for pidfile in "$PID_DIR"/*.pid; do
    [ -f "$pidfile" ] || continue

    market=$(basename "$pidfile" .pid)
    pid=$(cat "$pidfile")

    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo -e "  ${GREEN}✓${NC} Stopped $market (PID $pid)"
        stopped=$((stopped + 1))
    else
        echo "  ⚠️  $market (PID $pid) already stopped"
    fi
    rm -f "$pidfile"
done

# 兜底：清理残留进程（例如 PID 文件丢失）
pkill -f "polymarket_v2" 2>/dev/null || true

if [ $stopped -eq 0 ]; then
    echo "No running processes found."
else
    echo ""
    echo -e "${GREEN}✅ Stopped $stopped market maker(s)${NC}"
fi
