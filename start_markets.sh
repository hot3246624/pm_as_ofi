#!/bin/bash
# Polymarket V2 多市场做市器启动脚本
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Starting Polymarket V2 Market Maker${NC}"
echo ""

# 检查 .env 文件
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env file not found${NC}"
    echo "Please create .env based on .env.example"
    exit 1
fi

source .env

# 市场 slug 前缀列表（V2 使用前缀模式自动轮转）
MARKETS=(
    "btc-updown-15m"
    "eth-updown-15m"
    "sol-updown-15m"
)

mkdir -p logs
mkdir -p pids

# 清理旧的 PID 文件
rm -f pids/*.pid

echo -e "${YELLOW}Markets to run:${NC}"
for market in "${MARKETS[@]}"; do
    echo "  - $market"
done
echo ""

# 模式: DRY_RUN 或 LIVE
MODE=${1:-dry}
if [ "$MODE" = "live" ]; then
    if [ -z "$POLYMARKET_PRIVATE_KEY" ]; then
        echo -e "${RED}❌ LIVE 模式需要设置 POLYMARKET_PRIVATE_KEY${NC}"
        exit 1
    fi
    echo -e "${RED}⚠️  LIVE MODE — real orders will be placed${NC}"
    DRY_RUN_FLAG="false"
else
    echo -e "${GREEN}📝 DRY-RUN MODE — no real orders${NC}"
    DRY_RUN_FLAG="true"
fi
echo ""

# 启动每个市场
for market in "${MARKETS[@]}"; do
    echo -e "${GREEN}Starting V2 maker for:${NC} $market"

    POLYMARKET_MARKET_SLUG="$market" \
    PM_DRY_RUN="$DRY_RUN_FLAG" \
    RUST_LOG=info \
    nohup cargo run --bin polymarket_v2 --release \
        > logs/${market}.log 2>&1 &

    PID=$!
    echo $PID > pids/${market}.pid

    echo -e "  ${GREEN}✓${NC} PID: $PID -> logs/${market}.log"

    # 避免同时启动导致 API 限流
    sleep 2
done

echo ""
echo -e "${GREEN}✅ All markets started. Use stop_markets.sh to stop.${NC}"
echo -e "${YELLOW}Logs: tail -f logs/<slug>.log${NC}"
