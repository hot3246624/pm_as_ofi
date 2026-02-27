# 完整测试清单

本清单以当前主线 `polymarket_v2` 为准。

## 1. 静态检查

```bash
cargo check --bin polymarket_v2
cargo test --lib
cargo test --bin polymarket_v2
```

通过标准：

- `check` 无 error
- 单元测试全绿

## 2. 配置检查

必查项：

- `.env` 存在
- `POLYMARKET_WS_BASE_URL` 若自定义，必须包含 `/ws`
- live 模式下有 `POLYMARKET_PRIVATE_KEY`

建议命令：

```bash
rg -n "POLYMARKET_|PM_" .env .env.example
```

## 3. Dry Run 行为测试

```bash
PM_DRY_RUN=true cargo run --bin polymarket_v2
```

观察点：

1. 能解析市场并订阅成功
2. 有持续 `book/price_change/last_trade_price` 更新
3. 不应发真实订单
4. `net_diff` 保持在 0 附近（无真实 fill）

## 4. Live 冒烟测试（小仓）

```bash
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

观察点：

1. 认证成功（REST + User WS）
2. 下单失败会有 `OrderFailed` 回传并重置 slot
3. 有成交时，库存由 `REAL FILL` 驱动变化
4. `FAILED` 成交状态会触发库存回滚

## 5. 风控行为测试

建议分别构造或观察以下场景：

1. OFI 毒性触发：任一侧 `|ofi_score| > threshold`，应双边撤单
2. `can_open=false && net_diff≈0`：应撤双边 provide 单
3. `can_open=false && net_diff>0`：应只撤 YES 风险侧
4. `can_open=false && net_diff<0`：应只撤 NO 风险侧

## 6. 轮转测试（prefix 模式）

配置：

```bash
POLYMARKET_MARKET_SLUG="btc-updown-5m"
```

验证：

1. 到期后发送 `CancelAll`
2. 等待清理窗口后 abort 当前 session tasks
3. 自动进入下一个时间窗口 slug

## 7. 日志与指标建议

关键日志关键词：

- `GLOBAL KILL`
- `HEDGE`
- `CancelAll`
- `REAL FILL`
- `OrderFailed`

建议统计：

- 下单次数 / 撤单次数
- 成交率（fills / placed）
- 平均净仓绝对值
- 熔断触发频率

## 8. 回归测试触发时机

出现以下改动时必须全跑：

- `coordinator.rs` 状态机逻辑
- `executor.rs` 下单/撤单生命周期
- `user_ws.rs` 解析或去重逻辑
- `polymarket_v2.rs` 市场轮转与订阅流程
