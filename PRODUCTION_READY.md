# 🚀 实盘飞行前检查单（$100 测试版）

## 1. 环境变量 (`.env`)

### 必填项
- [ ] `POLYMARKET_PRIVATE_KEY` — 有资金操作权的钱包私钥（不带 0x）
- [ ] `POLYMARKET_FUNDER_ADDRESS` — 钱包地址（用于 User WS 成交归属匹配）
- [ ] `PM_DRY_RUN=false`

### $100 推荐参数
- [ ] `PM_PAIR_TARGET=0.985` — 保守让利，每对锁利 $0.015
- [ ] `PM_BID_SIZE=5.0` — 每侧 $5 挂单
- [ ] `PM_MAX_NET_DIFF=10.0` — 最大净仓差 10 股
- [ ] `PM_MAX_POSITION_VALUE=50.0` — 单侧最多 $50
- [ ] `PM_MAX_PORTFOLIO_COST=1.02` — 组合成本上限
- [ ] `PM_REPRICE_THRESHOLD=0.010` — 1 分钱漂移才换单
- [ ] `PM_DEBOUNCE_MS=500` — 半秒防抖
- [ ] `PM_OFI_WINDOW_MS=3000` — OFI 判定滑窗
- [ ] `PM_OFI_TOXICITY_THRESHOLD=300.0` — 毒性阈值 (基于实盘最新观察)
- [ ] `PM_AS_SKEW_FACTOR=0.03` — 空仓/被套的 A-S 惩罚与激励系数参数
- [ ] `PM_ENTRY_GRACE_SECONDS=30` — 入场窗口

## 2. 链上资金
- [ ] Polygon 链上钱包有 ≥ $100 `USDC.e`
- [ ] 有少量 `POL`/`MATIC` 用于 Gas（建议 ≥ 0.5 POL）
- [ ] 已在 Polymarket 前端完成 CTF Token Approval
- [ ] 确认钱包地址与 `POLYMARKET_FUNDER_ADDRESS` 一致

## 3. 系统环境
- [ ] 服务器时钟同步（`ntpdate`），误差 < 1s
- [ ] 使用 `screen` / `tmux` 防止 SSH 断连
- [ ] 网络稳定，能持续连接 Polymarket WS

## 4. 启动流程

```bash
# Step 1: 先跑一轮 Dry Run 验证
PM_DRY_RUN=true cargo run --bin polymarket_v2
# 观察日志，确认 market resolution、WS 连接、策略 tick 正常

# Step 2: 切换实盘
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

## 5. 启动后验证（前 5 分钟）

- [ ] 日志出现 `✅ Polymarket CLOB client authenticated`
- [ ] 日志出现 `🧹 Startup CancelAll sent` — 启动清场成功
- [ ] 日志出现 `📤 PROVIDE PostOnlyBid` — 双边开始挂单
- [ ] 日志出现 `📋 Lifecycle: ... fully filled` — 有成交发生
- [ ] 日志出现 `✅ OrderFilled` — Coordinator 正确释放 slot
- [ ] 日志出现 `📦 Fill: ... YES=... NO=...` — 库存正确更新
- [ ] **无** `📦 Confirmed fill ... already tracked` 异常翻倍（正常出现 = Confirmed 幂等处理正常）

## 6. 紧急处置

### 正常停机
```bash
Ctrl+C  # 触发 CancelAll → 安全撤单 → 退出
```

### 异常停机（进程 crash / 网络断）
1. 打开 Polymarket 网页版 → 手动撤销所有挂单
2. 下次启动时，系统自动 `CancelAll(Startup)` 清理残单

### 危险信号
| 日志 | 含义 | 操作 |
|------|------|------|
| `☠️ GLOBAL KILL` | OFI 检测到毒流 | 正常，自动撤单保护 |
| `🚫 Inventory limit` | 仓位超限 | 正常，等对冲成交 |
| `❌ Failed to place` | API 下单失败 | 检查余额/网络 |
| `⚠️ Stale book (>30s)` | 30 秒无盘口 | 检查 WS 连接 |
| `❌ Cancel failed` | 撤单 API 失败 | 手动去网页撤 |
