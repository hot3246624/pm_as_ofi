# pm_as_ofi / Polymarket Maker-Only 引擎

> 当前仓库目录已切换为 `pm_as_ofi`。

## 1. 项目定位

本项目当前主线是 **Polymarket V2 Maker-Only 引擎**：

- 仅挂单（`post_only=true`）
- 不主动吃单
- 通过库存约束 + OFI 毒性检测控制风险
- 支持 `*-updown-5m/15m` 前缀自动轮转

主入口：`src/bin/polymarket_v2.rs`

## 2. 核心不变量

1. `Executor` 只负责下单/撤单，**不直接改库存**
2. 库存唯一来源是认证 `User WS` 成交推送
3. 下单失败会回传 `OrderFailed`，协调器立即重置 slot，避免幽灵挂单状态
4. `can_open` 为硬门控：超限时禁止继续扩仓

## 3. 架构

```text
Market WS ──→ OFI Engine ──watch──┐
                                   ├─→ Coordinator ─→ Executor ─→ CLOB REST
User WS  ─────────────→ FillEvent ─┘           │
                                                └─→ InventoryManager (watch)
```

## 4. 快速开始

### 4.1 环境准备

```bash
cp .env.example .env
# 按需填写私钥、funder、策略参数
```

### 4.2 Dry Run（推荐先跑）

```bash
PM_DRY_RUN=true cargo run --bin polymarket_v2
```

### 4.3 Live

```bash
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

> Live 模式要求：
> - `POLYMARKET_PRIVATE_KEY` 可用
> - 能成功拿到/派生 API Key（用于 User WS）
> 否则程序会 fail-fast 退出。

## 5. 关键环境变量

### 5.1 市场与连接

- `POLYMARKET_MARKET_SLUG`：建议前缀，如 `btc-updown-15m`
- `POLYMARKET_WS_BASE_URL`：默认 `wss://ws-subscriptions-clob.polymarket.com/ws`
- `POLYMARKET_REST_URL`：默认 `https://clob.polymarket.com`
- `POLYMARKET_PRIVATE_KEY`：Live 必填
- `POLYMARKET_FUNDER_ADDRESS`：按账户类型需要时填写
- `POLYMARKET_API_KEY / SECRET / PASSPHRASE`：可选，不填则尝试自动派生

### 5.2 策略参数（Coordinator + Inventory + OFI）

- `PM_DRY_RUN`：`true/false`
- `PM_ENTRY_GRACE_SECONDS`：仅前缀模式生效；开盘后允许入场窗口（秒），默认 `30`
- `PM_PAIR_TARGET`：双腿目标和，默认 `0.99` (实盘建议首跑 0.98-0.985)
- `PM_BID_SIZE`：每次挂单数量，默认 `2.0`
- `PM_TICK_SIZE`：价格步长，默认 `0.001`
- `PM_REPRICE_THRESHOLD`：重报价阈值，默认 `0.010` (1美分，防抖降噪)
- `PM_DEBOUNCE_MS`：同侧挂单防抖，默认 `500` (半秒，降低 API 撤单限频风险)
- `PM_MAX_NET_DIFF`：最大净仓差，默认 `5.0`
- `PM_MAX_PORTFOLIO_COST`：最大组合成本和，默认 `1.02`
- `PM_MAX_POSITION_VALUE`：单侧最大美元敞口，默认 `$5.0`
- `PM_OFI_WINDOW_MS`：OFI 滑窗时间，默认 `3000` (3秒)
- `PM_OFI_TOXICITY_THRESHOLD`：OFI 毒性阈值，默认 `50.0`
- `PM_OFI_HEARTBEAT_MS`：OFI 强制刷新心跳，默认 `200` (毫秒)

### 5.3 实盘调参建议 (Live Tuning Guide)

在转入 `PM_DRY_RUN=false` 真金白银运行前，建议按以下顺序调参：

**1. 首跑安全参数（保守盈利模式）**
第一次跑请先牺牲成单率换取绝对安全：
- `PM_PAIR_TARGET=0.985` (让出更宽的点差，防止跨腿挂单带来的微小滑点)
- `PM_BID_SIZE=5.0` (用小资金测试真实 API 延迟与滑点)
- `PM_MAX_NET_DIFF=15.0` (不要超限，防止单边无限接盘)

**2. 进阶调参：如何增加成单率？**
如果你发现机器人一直挂在那里吃不到单（被其他做市商一直压底）：
- **微调 `PM_PAIR_TARGET`**：缓慢提升至 `0.990` -> `0.995`。加得越高，双边买价越高，越容易抢到单，但吃套的利润空间就越薄。
- **微调 `PM_REPRICE_THRESHOLD`**：从默认的 `0.010` 降到 `0.005`。这会让你的订单更敏锐地贴紧盘口，但代价是 API 撤单/挂单请求大增，有被限频封禁的风险。
- **缩小 `PM_DEBOUNCE_MS`**：从 `500` 降到 `200`。同样是为了更敏捷地换价。

**3. 风控调参：如何控制亏损率？**
如果你发现被单边趋势轧空（砸盘）：
- **降低 `PM_OFI_TOXICITY_THRESHOLD`**：比如从 `50` 降到 `30`。这会让 Kill Switch 变得神经质，一点风吹草动就会撤单保命。
- **放大 `PM_OFI_WINDOW_MS`**：从 `3000` 放大到 `5000`。这会让过去的砸盘记忆在脑海里停留更久（硬直更长），减少被趋势反扑的概率。

## 6. 策略行为摘要

- **Balanced**：`net_diff≈0` 且 `can_open=true`，双边提供流动性
- **Hedge**：`net_diff != 0`，只补缺腿，优先收敛库存
- **Global Kill**：任一侧 OFI 超阈值，双边撤单
- **!can_open**：
  - 若 `net_diff≈0`：撤双边 provide 单
  - 若 `net_diff>0`：撤 YES 侧（保留 NO 侧对冲能力）
  - 若 `net_diff<0`：撤 NO 侧（保留 YES 侧对冲能力）

详细见：`docs/strategy_guide.md`

## 7. 多市场启动脚本

```bash
# 默认 dry
./start_markets.sh

# live
./start_markets.sh live

# 停止
./stop_markets.sh
```

- 日志目录：`logs/`
- PID 目录：`pids/`
- 可在 `start_markets.sh` 中编辑 `MARKETS=(...)`

## 8. 代码结构（Polymarket V2 主线）

```text
src/
  bin/
    polymarket_v2.rs
  polymarket/
    coordinator.rs
    executor.rs
    inventory.rs
    messages.rs
    ofi.rs
    user_ws.rs
    types.rs
```

## 9. 开发命令

```bash
cargo check --bin polymarket_v2
cargo test --lib
cargo test --bin polymarket_v2
```

## 10. 文档索引

- `docs/strategy_guide.md`：策略与状态机细节
- `START_TESTING.md`：快速联调
- `TESTING.md`：完整测试清单
- `API_RATE_LIMITS.md`：请求频率建议
- `PRICE_PRECISION.md`：价格/数量精度与舍入
- `PRODUCTION_READY.md`：上线前检查项

## 11. 旧模块说明

仓库仍包含早期 AMM/MEV 代码与旧二进制（如 `polymarket_mm`）。

- 这些内容不属于当前主线文档范围
- 运行 V2 时请使用 `polymarket_v2`
