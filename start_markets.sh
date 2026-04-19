#!/bin/bash
# Polymarket V2 多市场做市器启动脚本
# 单进程 + 多市场 supervisor (PM_MULTI_MARKET_PREFIXES)
# supervisor 会为每个 prefix 自动设置 PM_ORACLE_LAG_SYMBOL_UNIVERSE=<own symbol>,
# 每个子进程的 ChainlinkHub 只订阅自己的 symbol, 避免 49 个订阅 burst 触发 TLS 限流.
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Starting Polymarket V2 Market Maker (supervisor mode)${NC}"
echo ""

# 检查 .env 文件
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env file not found${NC}"
    echo "Please create .env based on .env.example"
    exit 1
fi

source .env

# 关键: 清理 PM_ORACLE_LAG_SYMBOL_UNIVERSE, 让 supervisor 为每个子进程自动设置
# 它自己的 symbol (run_multi_market_supervisor 的 is_err() 分支). 若 .env 里设置了
# 全集, supervisor 就不会 narrow 每个子进程 -> 每个 hub 仍订阅全部 7 个 symbol,
# 退回到之前的 49-订阅 burst 情况.
unset PM_ORACLE_LAG_SYMBOL_UNIVERSE

# 市场 slug 前缀列表
# oracle_lag_sniping 要求 timeframe=5m (见 oracle_lag_symbol_from_slug).
MARKETS=(
    "btc-updown-5m"
    "eth-updown-5m"
    "sol-updown-5m"
    "bnb-updown-5m"
    "hype-updown-5m"
    "doge-updown-5m"
    "xrp-updown-5m"
)

PREFIXES=$(IFS=,; echo "${MARKETS[*]}")

mkdir -p logs
mkdir -p pids
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

LOG_FILE="logs/supervisor-$(date +%Y%m%d-%H%M%S).log"

echo -e "${GREEN}Starting supervisor with prefixes:${NC} $PREFIXES"
echo -e "${GREEN}Log:${NC} $LOG_FILE"

# PM_INPROC_SUPERVISOR=1 selects the in-process (Stage D) path:
# one tokio runtime, one shared ChainlinkHub, per-slug tasks inside
# a JoinSet. The legacy OS-process supervisor is used when this is
# unset — it still works but doesn't scale past ~10 markets.
PM_MULTI_MARKET_PREFIXES="$PREFIXES" \
PM_INPROC_SUPERVISOR=1 \
PM_DRY_RUN="$DRY_RUN_FLAG" \
RUST_LOG=info \
nohup cargo run --bin polymarket_v2 --release \
    > "$LOG_FILE" 2>&1 &

SUPERVISOR_PID=$!
echo $SUPERVISOR_PID > pids/supervisor.pid

echo -e "  ${GREEN}✓${NC} supervisor PID: $SUPERVISOR_PID"
echo ""
echo -e "${GREEN}✅ Supervisor started. Use stop_markets.sh to stop.${NC}"
echo -e "${YELLOW}Tail log: tail -f $LOG_FILE${NC}"
