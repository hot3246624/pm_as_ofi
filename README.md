# pm_as_ofi / Polymarket V2 Maker-Only 引擎

## 1. 项目定位

**Polymarket V2 Maker-Only 做市引擎** — 通过在二元期权市场的 YES/NO 双边同时挂被动限价单（Post-Only），当双边都成交时锁定确定性利润。

### 核心特性
- 仅挂单（`post_only=true`），永不主动吃单
- OFI Toxicity Detection → Strategy-First Kill Switch (Global Provide Kill)
- Inventory Hedging State Machine (Balanced → Hedge → Emergency Rescue)
- Inventory Gate (`can_buy_*`) applies to all orders (Provide + Hedge)
- Certified User WS for exclusive Fill confirmation
- OrderFilled feedback loop for immediate slot release
- Fill Ledger VWAP recalculation for zero drift
- Automated market rotation for `btc-updown-5m/15m`
- Configurable Stale Book Protection (per-side 3s TTL + 30s hard expiry)
- Dynamic hedge sizing updates on size-only changes

主入口：`src/bin/polymarket_v2.rs`

## 2. 架构

```text
Market WS  ──→ BookAssembler ──→ Coordinator ──→ OrderManager ──→ Executor ──→ CLOB REST
          ├──→ OFI Engine ──watch──→ Coordinator (Decision)
User WS   ──→ FillSplitter ──→ InventoryManager ──watch──→ Coordinator
```

### 核心不变量
1. `Executor` 只负责下单/撤单，**不直接改库存**
2. 库存唯一来源是认证 `User WS` 的 `FillEvent`
3. `Confirmed` 事件幂等处理，不重复入账
4. 下单失败回传 `OrderFailed`，成交回传 `OrderFilled`，Coordinator 即时释放 slot
5. `can_buy_*` 为硬门控：仅基于净仓差与单侧持仓价值进行投影限制（定价策略由 Coordinator 控制）
6. User WS `maker_orders.owner` 是 API key UUID，不是钱包地址

## 3. 快速开始

```bash
# 1. 拷贝环境配置
cp .env.example .env

# 2. 填写你的私钥和钱包地址
vim .env

# 3. Dry Run（推荐先跑 1-2 轮验证）
PM_DRY_RUN=true cargo run --bin polymarket_v2

# 4. Live（确认 dry run 无误后）
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

> ⚠️ Live 模式需要 `POLYMARKET_PRIVATE_KEY` 有效且已授权 CTF Token Approvals。

## 4. 环境变量完整参考

### 4.1 连接与认证

| 变量 | 必填 | 说明 |
|------|------|------|
| `POLYMARKET_MARKET_SLUG` | ✅ | 市场前缀 (如 `btc-updown-5m`) 或完整 slug |
| `POLYMARKET_PRIVATE_KEY` | 实盘 | 钱包私钥（不带 0x 前缀） |
| `POLYMARKET_FUNDER_ADDRESS` | 实盘 | 资金钱包地址（用于 Proxy/Safe CLOB 认证与余额/授权读取） |
| `POLYMARKET_WS_BASE_URL` | ❌ | 默认 `wss://ws-subscriptions-clob.polymarket.com/ws` |
| `POLYMARKET_REST_URL` | ❌ | 默认 `https://clob.polymarket.com` |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | ❌ | 可选，不填则自动派生（用于 CLOB REST + User WS，不是 Builder 凭证） |
| `POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE` | ❌ | 仅 Safe 自动 Claim 需要（Relayer Builder 认证） |
| `POLYMARKET_RELAYER_URL` | ❌ | 默认 `https://relayer-v2.polymarket.com` |

### 4.2 策略参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_DRY_RUN` | `true` | 模拟模式开关 |
| `PM_PAIR_TARGET` | `0.99` | YES+NO 出价上限。越低越安全利润越高，但成交率下降 |
| `PM_BID_SIZE` | `5.0` | Size per bid in **Shares** (1 Share = $1 Max Risk) |
| `PM_TICK_SIZE` | `0.01` | Minimum price increment |
| `PM_REPRICE_THRESHOLD` | `0.010` | Price drift required to trigger re-quote |
| `PM_DEBOUNCE_MS` | `500` | Minimum interval between Provide orders (ms) |
| `PM_STALE_TTL_MS` | `3000` | Stale Book TTL in milliseconds |
| `PM_ENTRY_GRACE_SECONDS` | `30` | Time window after market start to enter (seconds) |

### 4.3 风控参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_MAX_NET_DIFF` | `10.0` | Max net directional inventory difference (**Shares**) |
| `PM_MAX_PORTFOLIO_COST` | `1.02` | 最大组合成本和（> 1.0 = 套利失败） |
| `PM_MAX_LOSS_PCT` | `0.02` | 最大可接受组合亏损比例（用于钳制 `PM_MAX_PORTFOLIO_COST`） |
| `PM_MAX_SIDE_SHARES` | `5.0` | 单侧最大持仓股数上限 |
| `PM_OFI_WINDOW_MS` | `3000` | OFI 滑窗长度（毫秒） |
| `PM_OFI_TOXICITY_THRESHOLD` | `50.0` | OFI 毒性阈值（越低越敏感） |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 强制刷新心跳 |

> 注：`PM_MAX_POSITION_VALUE` 已弃用，若仍设置将被视为 `PM_MAX_SIDE_SHARES`（单位为 shares）。
> `PM_MAX_PORTFOLIO_COST` 会被 `PM_MAX_LOSS_PCT` 自动钳制。

### 4.4 Claim 参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_CLAIM_MONITOR` | `true` | 日志监控可领取收益 |
| `PM_AUTO_CLAIM` | `false` | 开启自动领取 |
| `PM_AUTO_CLAIM_DRY_RUN` | `false` | 仅打印待领取，不执行 |
| `PM_AUTO_CLAIM_MIN_VALUE` | `0` | 单个 condition 最小价值阈值（美元） |
| `PM_AUTO_CLAIM_MAX_CONDITIONS` | `5` | 每轮最多处理的 condition 数 |
| `PM_AUTO_CLAIM_INTERVAL_SECONDS` | `300` | 两次自动领取最小间隔 |
| `PM_AUTO_CLAIM_WAIT_CONFIRM` | `false` | Safe 模式是否等待 relayer 交易确认（建议 false，避免阻塞交易轮次） |
| `PM_AUTO_CLAIM_WAIT_TIMEOUT_SECONDS` | `20` | `WAIT_CONFIRM=true` 时的最长等待秒数 |

## 5. $100 实盘测试参数推荐

> 以 $100 USDC.e 资金，`btc-updown-5m` 市场为例：

```env
# ─── $100 实盘首跑参数 ───
PM_DRY_RUN=false
POLYMARKET_MARKET_SLUG="btc-updown-5m"

# 策略核心
PM_PAIR_TARGET=0.985          # 保守让利 1.5%，每对锁利 $0.015
PM_BID_SIZE=5.0               # 每侧 $5 挂单
PM_TICK_SIZE=0.01
PM_REPRICE_THRESHOLD=0.010    # 1分钱漂移才换单，防撤单风暴
PM_DEBOUNCE_MS=500            # 半秒防抖

# 风控（$100 资金匹配）
PM_MAX_NET_DIFF=10.0          # 最大单侧 10 股偏差
PM_MAX_PORTFOLIO_COST=1.02    # 组合成本上限
PM_MAX_LOSS_PCT=0.02          # 最大可接受亏损比例（2%）
PM_MAX_SIDE_SHARES=50.0       # 单侧最多 50 股（总仓位上限）

# OFI
PM_OFI_WINDOW_MS=3000
PM_OFI_TOXICITY_THRESHOLD=50.0
PM_OFI_HEARTBEAT_MS=200

# 入场窗口
PM_ENTRY_GRACE_SECONDS=30
```

### 参数选择逻辑

| 参数 | 值 | 理由 |
|------|-----|------|
| `PAIR_TARGET=0.985` | 保守 | 每配对锁利 $0.015。先验证流程再追利润 |
| `BID_SIZE=5.0` | 适中 | 每侧 $5，一轮最多双侧 $10 暴露 |
| `MAX_NET_DIFF=10.0` | 安全 | 允许 2 轮未对冲偏差 |
| `MAX_SIDE_SHARES=50.0` | $100 一半 | 单侧最多 50 股，留足对冲余地 |
| `REPRICE_THRESHOLD=0.010` | 防抖 | 避免高频撤补消耗API限额 |
| `OFI_THRESHOLD=50.0` | 标准 | 首跑先用默认，观察日志再微调 |

## 6. 策略行为

| 状态 | 条件 | 行为 |
|------|------|------|
| **Balanced** | `net_diff ≈ 0` + Health OK + `can_buy_*` | Place YES+NO bids with A-S Skew |
| **Hedge / Rescue** | `net_diff ≠ 0` + hedge side healthy + `can_buy_*` | Place dynamic hedge order (`size = net_diff.abs()`) |
| **Kill (Toxicity)** | Side X is toxic | Provide prices set to 0.0; hedges only if healthy + `can_buy_*` |
| **Stale Protection** | Data older than TTL | Set price to 0.0 for that side; 30s expiry clears both |
| **Empty Book** | No usable book data | No new orders; if unhealthy then clear targets |

## 7. 调参指南

**提高成交率**（观察到长期无成交时）：
- `PM_PAIR_TARGET`: `0.985` → `0.990` → `0.995`
- `PM_REPRICE_THRESHOLD`: `0.010` → `0.005`
- `PM_DEBOUNCE_MS`: `500` → `200`

**加强防护**（观察到单边趋势碾压时）：
- `PM_OFI_TOXICITY_THRESHOLD`: `50` → `30`
- `PM_OFI_WINDOW_MS`: `3000` → `5000`

**放大仓位**（验证稳定后）：
- `PM_BID_SIZE`: `5` → `10` → `20`
- `PM_MAX_SIDE_SHARES`: `50` → `80`

## 8. 文档索引

| 文档 | 位置 | 内容 |
|------|------|------|
| **实盘检查单** | `PRODUCTION_READY.md` | 上线 preflight checklist |
| **策略核心** | `docs/STRATEGY_V2_CORE.md` | 状态机、定价公式、对冲与救火逻辑 |
| **测试指南** | `docs/TESTING.md` | 完整测试清单 |
| **API限频** | `docs/API_RATE_LIMITS.md` | 请求频率建议 |
| **价格精度** | `docs/PRICE_PRECISION.md` | 价格/数量精度与舍入 |

## 9. 代码结构

```text
src/
  bin/
    polymarket_v2.rs   # 主入口 + WS 解析 + 市场轮转
  polymarket/
    coordinator.rs     # 策略状态机
    executor.rs        # 下单/撤单执行器
    inventory.rs       # Fill Ledger + VWAP 库存管理
    messages.rs        # 全部消息类型定义
    ofi.rs             # OFI 毒性引擎
    user_ws.rs         # 认证 WS 成交监听
    types.rs           # Side 等基础类型
```
