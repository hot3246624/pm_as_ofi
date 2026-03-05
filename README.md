# pm_as_ofi / Polymarket V2 Maker-Only 引擎

## 1. 项目定位

**Polymarket V2 Maker-Only 做市引擎** — 通过在二元期权市场的 YES/NO 双边同时挂被动限价单（Post-Only），当双边都成交时锁定确定性利润。

### 核心特性
- 仅挂单（`post_only=true`），永不主动吃单
- OFI 毒性检测 → 全局 Kill Switch，毫秒级撤单避险
- 库存对冲状态机（Balanced → Hedge → Kill）
- 认证 User WS 独占确认成交，防止幻影库存
- 成交后自动释放 slot（`OrderFilled` 反馈闭环）
- Fill Ledger VWAP 精确重算，零漂移
- 支持 `*-updown-5m/15m` 前缀自动轮转
- 启动时自动 `CancelAll(Startup)` 清除历史残单
- 陈旧盘口 30 秒 TTL 保护

主入口：`src/bin/polymarket_v2.rs`

## 2. 架构

```text
Market WS ──→ BookAssembler ──→ Coordinator ──→ Executor ──→ CLOB REST
         ├──→ OFI Engine ──watch──→ Coordinator
User WS  ──→ FillSplitter ──→ InventoryManager (watch→ Coordinator)
                           └──→ Executor (OrderFilled→ Coordinator)
```

### 核心不变量
1. `Executor` 只负责下单/撤单，**不直接改库存**
2. 库存唯一来源是认证 `User WS` 的 `FillEvent`
3. `Confirmed` 事件幂等处理，不重复入账
4. 下单失败回传 `OrderFailed`，成交回传 `OrderFilled`，Coordinator 即时释放 slot
5. `can_open` 为硬门控：三重限制（净仓差/组合成本/单侧敞口）

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
| `POLYMARKET_FUNDER_ADDRESS` | 实盘 | 钱包地址（用于 User WS 成交归属匹配） |
| `POLYMARKET_WS_BASE_URL` | ❌ | 默认 `wss://ws-subscriptions-clob.polymarket.com/ws` |
| `POLYMARKET_REST_URL` | ❌ | 默认 `https://clob.polymarket.com` |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | ❌ | 可选，不填则自动派生 |

### 4.2 策略参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_DRY_RUN` | `true` | 模拟模式开关 |
| `PM_PAIR_TARGET` | `0.99` | YES+NO 出价上限。越低越安全利润越高，但成交率下降 |
| `PM_BID_SIZE` | `2.0` | 每次挂单股数（美元） |
| `PM_TICK_SIZE` | `0.01` | 最小价格步长 |
| `PM_REPRICE_THRESHOLD` | `0.010` | 价差偏移多少才触发重报价 |
| `PM_DEBOUNCE_MS` | `500` | 同侧挂单最小间隔（毫秒） |
| `PM_ENTRY_GRACE_SECONDS` | `30` | 开盘后允许入场窗口（秒） |

### 4.3 风控参数

| 变量 | 默认 | 说明 |
|------|------|------|
| `PM_MAX_NET_DIFF` | `5.0` | 最大单边净仓差（股） |
| `PM_MAX_PORTFOLIO_COST` | `1.02` | 最大组合成本和（> 1.0 = 套利失败） |
| `PM_MAX_POSITION_VALUE` | `5.0` | 单侧最大美元暴露 |
| `PM_OFI_WINDOW_MS` | `3000` | OFI 滑窗长度（毫秒） |
| `PM_OFI_TOXICITY_THRESHOLD` | `50.0` | OFI 毒性阈值（越低越敏感） |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 强制刷新心跳 |

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
PM_MAX_POSITION_VALUE=50.0    # 单侧最多 $50（总 $100 的一半）

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
| `MAX_POSITION_VALUE=50.0` | $100 一半 | 单侧最多占用总资金 50%，留足对冲余地 |
| `REPRICE_THRESHOLD=0.010` | 防抖 | 避免高频撤补消耗API限额 |
| `OFI_THRESHOLD=50.0` | 标准 | 首跑先用默认，观察日志再微调 |

## 6. 策略行为

| 状态 | 条件 | 行为 |
|------|------|------|
| **Balanced** | `net_diff≈0` + `can_open` | YES+NO 双边中间价挂单 |
| **Hedge** | `net_diff≠0` | 撤多余侧，aggressive 补缺腿 |
| **Global Kill** | 任一侧 OFI toxic | 双边全撤 |
| **Inv Limit** | `!can_open` | 撤风险侧，保留对冲侧 |

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
- `PM_MAX_POSITION_VALUE`: `50` → `80`

## 8. 文档索引

| 文档 | 位置 | 内容 |
|------|------|------|
| **实盘检查单** | `PRODUCTION_READY.md` | 上线 preflight checklist |
| **策略细节** | `docs/strategy_guide.md` | 状态机、生命周期、参数公式 |
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
