# pm_as_ofi / Polymarket V2 Maker-Only 引擎

## 1. 项目定位

**Polymarket V2 Maker-Only 做市引擎** — 通过在二元期权市场的 YES/NO 双边同时挂被动限价单（Post-Only），当双边都成交时锁定确定性利润。

### 核心特性
- 仅挂单（`post_only=true`），永不主动吃单
- OFI Toxicity Detection → Strategy-First Kill Switch (Global Provide Kill)
- Inventory Hedging State Machine (Balanced → Hedge → Emergency Rescue)
- Inventory Gate split: Provide uses `can_buy_*`; Hedge uses net-only gate to prioritize de-risking
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
5. 风控门控分层：Provide 走 `can_buy_*`（净仓 + 单侧）；Hedge 走净仓门控优先降风险（允许越过单侧上限）
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

### 3.1 快速切换市场（BTC/ETH/XRP/SOL + 1h/4h/1d）

```bash
# BTC 4h
POLYMARKET_MARKET_SYMBOL=btc POLYMARKET_MARKET_TIMEFRAME=4h PM_DRY_RUN=true cargo run --bin polymarket_v2

# ETH 1h
POLYMARKET_MARKET_SYMBOL=eth POLYMARKET_MARKET_TIMEFRAME=1h PM_DRY_RUN=true cargo run --bin polymarket_v2

# SOL Daily
POLYMARKET_MARKET_SYMBOL=sol POLYMARKET_MARKET_TIMEFRAME=1d PM_DRY_RUN=true cargo run --bin polymarket_v2

# 也可直接指定前缀
POLYMARKET_MARKET_PREFIX=xrp-updown-4h PM_DRY_RUN=true cargo run --bin polymarket_v2
```

## 4. 环境变量完整参考

### 4.1 连接与认证

| 变量 | 必填 | 说明 |
|------|------|------|
| `POLYMARKET_MARKET_SLUG` | 推荐 | 市场前缀 (如 `btc-updown-5m`) 或完整 slug（优先级最高） |
| `POLYMARKET_MARKET_PREFIX` | ❌ | 前缀模式别名（如 `eth-updown-1h`），用于自动轮转 |
| `POLYMARKET_MARKET_SYMBOL` | ❌ | 与 `POLYMARKET_MARKET_TIMEFRAME` 组合，自动拼接 `<symbol>-updown-<tf>`，支持 `btc/eth/xrp/sol` |
| `POLYMARKET_MARKET_TIMEFRAME` | ❌ | 与 `POLYMARKET_MARKET_SYMBOL` 组合，支持 `1m/5m/15m/30m/1h/4h/1d` |
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
| `PM_BID_SIZE` | `5.0` | 提供单规模上限（Shares）；仅在显式配置 `PM_BID_PCT` 时才会被余额动态下调 |
| `PM_MIN_ORDER_SIZE` | `1.0 (auto)` | 最小订单数量（未设置时自动从 order_book 探测并向上调整；小于该值的订单会被跳过） |
| `PM_MIN_HEDGE_SIZE` | `0.0` | 对冲触发最小阈值（0=禁用） |
| `PM_HEDGE_ROUND_UP` | `false` | 对冲不足最小订单时是否向上取整 |
| `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` | `0.0` | 可选：仅对冲路径的 marketable-BUY 最小金额兜底（0=关闭） |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA` | `0.5` | 触发兜底时单次最多额外加仓（shares） |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT` | `0.15` | 触发兜底时最多额外比例（相对原对冲量） |
| `PM_MIN_MARKETABLE_NOTIONAL_FLOOR` | `0.0` | 全局 marketable-BUY 最小名义预检（0=关闭） |
| `PM_MIN_MARKETABLE_AUTO_DETECT` | `false` | 是否从交易所拒单自动学习 `min size:$X`（静态优先建议关闭） |
| `PM_MIN_MARKETABLE_COOLDOWN_MS` | `10000` | 触发 marketable 最小名义拒单后的侧边冷却时间 |
| `PM_TICK_SIZE` | `0.01` | Minimum price increment |
| `PM_REPRICE_THRESHOLD` | `0.010` | Price drift required to trigger re-quote |
| `PM_DEBOUNCE_MS` | `500` | Minimum interval between Provide orders (ms) |
| `PM_STALE_TTL_MS` | `3000` | Stale Book TTL in milliseconds |
| `PM_TOXIC_RECOVERY_HOLD_MS` | `1200` | 毒性恢复后保持冷却窗口，防止“撤-挂-撤-挂”抖动 |
| `PM_ENTRY_GRACE_SECONDS` | `30` | Time window after market start to enter (seconds) |

### 4.3 风控参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_MAX_NET_DIFF` | `10.0` | 净仓差上限（Shares）；仅在显式配置 `PM_NET_DIFF_PCT` 时才会被余额动态下调 |
| `PM_MAX_PORTFOLIO_COST` | `1.02` | 最大组合成本和（> 1.0 = 套利失败） |
| `PM_MAX_LOSS_PCT` | `0.02` | 最大可接受组合亏损比例（用于钳制 `PM_MAX_PORTFOLIO_COST`） |
| `PM_MAX_SIDE_SHARES` | `50.0` | 单侧最大持仓股数上限（仅当 `PM_MAX_POS_PCT>0` 时受动态约束） |
| `PM_RECONCILE_INTERVAL_SECS` | `30` | 订单对账周期（秒），用于修复 WS 断连盲区 |
| `PM_COORD_WATCHDOG_MS` | `500` | Coordinator 风控看门狗心跳（无行情也执行 stale/toxic 检查） |
| `PM_WS_CONNECT_TIMEOUT_MS` | `6000` | Market WS 单次连接超时（毫秒） |
| `PM_RESOLVE_TIMEOUT_MS` | `4000` | Gamma 市场解析请求超时（毫秒） |
| `PM_RESOLVE_RETRY_ATTEMPTS` | `4` | 每轮市场解析重试次数（指数退避） |
| `PM_BID_PCT` | `unset` | 可选：显式配置后按 `balance × pct` 动态下调 `PM_BID_SIZE` |
| `PM_NET_DIFF_PCT` | `unset` | 可选：显式配置后按 `balance × pct` 动态下调 `PM_MAX_NET_DIFF` |
| `PM_MAX_POS_PCT` | `0.0` | 总仓位占比上限（`0`=关闭动态 gross；`>0` 动态推导 `PM_MAX_SIDE_SHARES`） |
| `PM_DYNAMIC_GROSS_REFRESH_SECS` | `10` | 轮中动态刷新 `max_side_shares` 的周期（秒） |
| `PM_OFI_WINDOW_MS` | `3000` | OFI 滑窗长度（毫秒） |
| `PM_OFI_TOXICITY_THRESHOLD` | `300.0` | OFI 基准毒性阈值（adaptive=true 时作为初始/回退值） |
| `PM_OFI_ADAPTIVE` | `true/false` | 是否开启自适应阈值（`mean + k*sigma`） |
| `PM_OFI_ADAPTIVE_K` | `3.0` | 自适应阈值放大系数 |
| `PM_OFI_ADAPTIVE_MIN` | `120.0` | 自适应阈值下限 |
| `PM_OFI_ADAPTIVE_MAX` | `0.0` | 自适应阈值硬上限（`0`=关闭硬上限） |
| `PM_OFI_ADAPTIVE_RISE_CAP_PCT` | `0.20` | 每个 OFI 心跳阈值最大上升比例（`0`=关闭） |
| `PM_OFI_RATIO_ENTER` | `0.45` | 毒性进入比例门槛：`|buy-sell|/(buy+sell)` |
| `PM_OFI_RATIO_EXIT` | `0.30` | 毒性退出比例门槛（应 ≤ enter） |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 强制刷新心跳 |
| `PM_OFI_EXIT_RATIO` | `0.85` | OFI 退出滞回阈值比例（低于该比例才退出 toxic） |
| `PM_OFI_MIN_TOXIC_MS` | `800` | 单次 toxic 最短保持时间，抑制阈值抖动 |
| `PM_RECYCLE_ENABLED` | `true` | 启用余额压力回收器（余额拒单触发批量 merge） |
| `PM_RECYCLE_TRIGGER_REJECTS` | `2` | 触发阈值：窗口内余额拒单次数 |
| `PM_RECYCLE_TRIGGER_WINDOW_SECS` | `90` | 拒单统计窗口（秒） |
| `PM_RECYCLE_PROACTIVE` | `true` | 主动 headroom 探测：即使无拒单，也会在低水位时尝试回收 |
| `PM_RECYCLE_POLL_SECS` | `5` | 主动 headroom 探测周期（秒） |
| `PM_RECYCLE_COOLDOWN_SECS` | `120` | 两次回收最小间隔（秒） |
| `PM_RECYCLE_LOW_WATER_USDC` | `6` | 低水位：低于该余额才允许回收 |
| `PM_RECYCLE_TARGET_FREE_USDC` | `18` | 回收目标余额（高水位） |
| `PM_RECYCLE_MIN_BATCH_USDC` | `10` | 单次最小回收量（USDC） |
| `PM_RECYCLE_MAX_BATCH_USDC` | `30` | 单次最大回收量（USDC） |
| `PM_BALANCE_CACHE_TTL_MS` | `2000` | 下单前余额可负担性检查缓存 TTL（毫秒） |

> 注：`PM_MAX_POSITION_VALUE` 已弃用，若仍设置将被视为 `PM_MAX_SIDE_SHARES`（单位为 shares）。
> `PM_MAX_PORTFOLIO_COST` 会被 `PM_MAX_LOSS_PCT` 自动钳制。
> OFI 毒性退出阈值基于“进入 toxic 时的阈值”冻结计算，避免自适应阈值瞬时抬升导致的误恢复。
> `PM_COORD_WATCHDOG_MS` 用于在 WS 暂时断流时继续触发风控 tick，避免 stale 熔断依赖行情事件。
> 低价区出现成交不代表“无最小金额校验”：被动 maker 成交通常可成立，但极小金额 marketable BUY 仍可能被拒。默认配置采用“静态优先”（`FLOOR=0`、`AUTO_DETECT=false`），仅保留拒单分类+冷却防风暴；需要时可显式启用预检/自学习。
> 若日志频繁出现 `not enough balance / allowance`，应优先下调 `PM_MAX_SIDE_SHARES` / `PM_BID_SIZE`；若已启用动态 gross（`PM_MAX_POS_PCT>0`），再下调 `PM_MAX_POS_PCT`。
> 回收器采用“低水位触发 + 高水位回补 + 冷却 + 单轮上限”，默认是低频批量回收，不是每次拒单都小额 merge。

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
PM_MIN_ORDER_SIZE=5.0         # 最小订单数量（不配置则自动从 order_book 探测）
PM_MIN_HEDGE_SIZE=0.0         # 对冲触发最小阈值（0=禁用）
PM_HEDGE_ROUND_UP=false       # 对冲不足最小订单时是否向上取整
PM_MIN_MARKETABLE_AUTO_DETECT=false  # 默认关闭隐式学习（静态优先）
PM_MIN_MARKETABLE_COOLDOWN_MS=10000  # 最小金额拒单冷却
PM_TICK_SIZE=0.01
PM_REPRICE_THRESHOLD=0.010    # 1分钱漂移才换单，防撤单风暴
PM_DEBOUNCE_MS=500            # 半秒防抖

# 风控（$100 资金匹配）
PM_MAX_NET_DIFF=10.0          # 最大单侧 10 股偏差
PM_MAX_PORTFOLIO_COST=1.02    # 组合成本上限
PM_MAX_LOSS_PCT=0.02          # 最大可接受亏损比例（2%）
PM_MAX_SIDE_SHARES=50.0       # 单侧最多 50 股（总仓位上限）
PM_RECONCILE_INTERVAL_SECS=30 # 订单对账周期（秒）
PM_MAX_POS_PCT=0.0            # 静态优先：关闭动态 gross 上限
# PM_BID_PCT=0.02             # 显式配置才启用动态 BID_SIZE 覆盖
# PM_NET_DIFF_PCT=0.10        # 显式配置才启用动态 MAX_NET_DIFF 覆盖

# OFI
PM_OFI_WINDOW_MS=3000
PM_OFI_TOXICITY_THRESHOLD=300.0
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
| `OFI_THRESHOLD=300.0` | 稳健 | 作为自适应阈值的初始/回退基线，首跑后按日志微调 |

## 6. 策略行为

| 状态 | 条件 | 行为 |
|------|------|------|
| **Balanced** | `net_diff ≈ 0` + Health OK + `can_buy_*` | Place YES+NO bids with A-S Skew |
| **Hedge / Rescue** | `net_diff ≠ 0` + hedge side healthy + `can_hedge_buy_*` | Place dynamic hedge order (`size = net_diff.abs()`) |
| **Kill (Toxicity)** | Side X is toxic | Provide prices set to 0.0; hedges only if healthy + net gate |
| **Stale Protection** | Data older than TTL | Set price to 0.0 for that side; 30s expiry clears both |
| **Empty Book** | No usable book data | No new orders; if unhealthy then clear targets |

## 7. 调参指南

**提高成交率**（观察到长期无成交时）：
- `PM_PAIR_TARGET`: `0.985` → `0.990` → `0.995`
- `PM_REPRICE_THRESHOLD`: `0.010` → `0.005`
- `PM_DEBOUNCE_MS`: `500` → `200`

**加强防护**（观察到单边趋势碾压时）：
- `PM_OFI_TOXICITY_THRESHOLD`: `300` → `220`
- `PM_OFI_WINDOW_MS`: `3000` → `5000`

**放大仓位**（验证稳定后）：
- `PM_BID_SIZE`: `5` → `10` → `20`
- `PM_MAX_SIDE_SHARES`: `50` → `80`

## 8. 文档索引

| 文档 | 位置 | 内容 |
|------|------|------|
| **实盘检查单** | `PRODUCTION_READY.md` | 上线 preflight checklist |
| **策略核心** | `docs/STRATEGY_V2_CORE.md` | 状态机、定价公式、对冲与救火逻辑 |
| **参数手册** | `docs/CONFIG_REFERENCE_ZH.md` | `.env/.env.example` 全参数解释与建议 |
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
