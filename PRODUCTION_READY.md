# 生产就绪检查（V2）

更新时间：2026-02-24

该清单用于上线前自检，针对 `polymarket_v2`。

## 1. 必须为真

- [ ] `cargo check --bin polymarket_v2` 通过
- [ ] `cargo test --lib` 通过
- [ ] dry 模式可持续接收行情
- [ ] live 模式认证成功（REST + User WS）
- [ ] 下单失败会触发 `OrderFailed`，不会遗留幽灵 slot
- [ ] 市场轮转时会 `CancelAll` 并清理 session tasks

## 2. 运行前配置

最小 live 配置：

```bash
POLYMARKET_MARKET_SLUG="btc-updown-15m"
POLYMARKET_PRIVATE_KEY="..."
POLYMARKET_FUNDER_ADDRESS="0x..."
PM_DRY_RUN=false
```

建议同时配置：

```bash
PM_PAIR_TARGET=0.99
PM_BID_SIZE=1
PM_MAX_NET_DIFF=5
PM_MAX_POSITION_VALUE=5
PM_OFI_WINDOW_MS=3000
PM_OFI_TOXICITY_THRESHOLD=50
```

## 3. 风险闸门复核

- [ ] `post_only=true`（确保 maker-only）
- [ ] `can_open` 三重限制生效
- [ ] OFI 任一侧 toxic 会全局撤单
- [ ] User WS owner 过滤与 API key 格式一致

## 4. 监控建议（最少）

- [ ] 每分钟下单/撤单量
- [ ] `net_diff` 绝对值分布
- [ ] `portfolio_cost`
- [ ] `GLOBAL KILL` 触发次数
- [ ] `OrderFailed` 次数

## 5. 灰度上线节奏

1. 先 dry 运行 30-60 分钟，确认轮转/订阅稳定
2. live 小仓（`PM_BID_SIZE=1`, `PM_MAX_POSITION_VALUE=5`）
3. 观察至少 1 个交易时段再放大

## 6. 紧急处理

```bash
./stop_markets.sh
```

该脚本会：

- 按 PID 文件关闭进程
- 兜底 `pkill -f polymarket_v2` 清理残留

## 7. 结论模板

上线前请在内部记录中给出：

- 本次配置 hash（`.env` 关键参数）
- 检查时间窗口
- 观察到的异常与处理
- 是否允许扩大仓位
