# oracle_lag_sniping 实盘前检查清单

适用范围：`PM_STRATEGY=oracle_lag_sniping`（兼容旧别名 `post_close_hype`），当前支持 `PM_ORACLE_LAG_SYMBOL_UNIVERSE` 中的 `*-updown-5m`。

## 1. 启动前参数检查

- `PM_STRATEGY=oracle_lag_sniping`
- `POLYMARKET_MARKET_SLUG=btc-updown-5m`（或其他 `*-updown-5m`）
- `PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp`（或 `*`）
- `PM_POST_CLOSE_WINDOW_SECS=105`
- `PM_POST_CLOSE_CHAINLINK_WS_URL=wss://ws-live-data.polymarket.com`（默认可不填）
- `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS=8`
- `PM_POST_CLOSE_GAMMA_POLL_MS=300`
- 策略约束：`> 0.993` 不做 taker；maker 允许继续挂胜方单

建议保守值：

- `PM_DRY_RUN=true` 先跑 1-2 轮观察日志，再切 live
- `PM_BID_SIZE=5`
- `PM_MAX_NET_DIFF` 维持当前安全值

## 2. 轮次内必须出现的关键日志

每轮应至少看到以下链路（顺序可能有轻微交叉）：

1. `oracle_lag_sniping enabled for ...`
2. 收盘后出现 `winner hint` 日志：
  - 优先 `source=Chainlink`
  - 若 Chainlink 超时，允许 `source=Gamma`
3. `DRY/PLACE` 的 BUY 单只出现在胜方一侧
4. 出现下列延迟日志：
  - `oracle_lag_submit_latency`
  - 重点看 `first_submit=true` 的 `delta_from_end_ms` 是否 `<= 1500`

若没有看到 winner hint，说明当前轮不会出单（策略按设计静默）。

## 3. 风险红线（触发即停）

- 连续多轮都走 `Gamma fallback` 且明显慢于盘口窗口
- winner hint 与盘口显著矛盾（例如提示 YES 胜但 NO 挂单密集成交）
- 盘后窗口内出现双边开仓（不应发生）
- 订单被大量 `cross-book` 拒绝但没有价格步降
- `first_submit=true` 的 `delta_from_end_ms` 长期大于 `1500ms`

## 4. 切换 live 的最小门槛

满足以下条件再切 `PM_DRY_RUN=false`：

- 连续 3 轮 dry-run 出现稳定 winner hint（至少 2 轮 Chainlink）
- winner hint 到第一笔下单延迟稳定（目标 `<=1.5s`）
- 无异常双边下单、无明显 stale target

## 5. 实盘首轮观察点（5 分钟内）

- WinnerHint 来源分布：Chainlink / Gamma
- 第一笔下单时机：是否接近收盘后 1-5 秒窗口
- 成交后是否继续在同侧 best bid 附近循环挂单
- `CancelAll`/轮次切换时是否清理干净（无遗留挂单）

## 6. 失败回退策略

若出现连续异常，立即回退到：

- `PM_DRY_RUN=true`
- 或切回 `PM_STRATEGY=pair_arb`

并保留该轮日志用于复盘：

- `logs/polymarket.log.*`
- 关键时间点 winner hint 与 place/reprice 片段
