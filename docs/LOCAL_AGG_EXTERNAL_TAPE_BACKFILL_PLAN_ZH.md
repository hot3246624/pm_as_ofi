# 本地价格聚合器 72h 外部价格源 Tape 补齐计划

更新时间：2026-04-30 11:35 CST

## 结论

`poly_trans_research` 当前采集链路不满足本地价格聚合器训练需求。

它适合做 Polymarket 5 分钟市场回放，因为它采集：

- Polymarket `market_ws`：`book / price_change / best_bid_ask / last_trade_price`
- Polymarket `meta`
- Polymarket `settlement`
- 可选 `user truth / xuan poll`

但本地价格聚合器需要的是外部价格源在每个 5 分钟 round 结算边界附近的毫秒级价格 tape：

- Binance
- Coinbase
- OKX
- Bybit
- Hyperliquid
- 后续可加 Uniswap / DEX TWAP

因此，`poly_trans_research` 可以提供 round universe、Polymarket 市场侧 replay 和 settlement 辅助，但不能单独训练 `local_final -> RTDS final` 的聚合模型。

## 目标

补齐最近 72 小时的外部价格源 boundary tape，用于离线训练和 walk-forward 验证本地价格聚合器。

训练目标：

- validation accepted side mismatch = 0
- validation max close_diff_bps <= 5
- validation p95 close_diff_bps <= 3
- 单币种 accepted coverage >= 75%
- 总 accepted coverage >= 85%

## 时间切分

不要随机 2/3、1/3 切分。必须按时间切：

- train：前 48 小时
- validation：后 24 小时
- walk-forward 可选：每 24h 训练，后 6-12h 验证

原因：随机切分会让同一行情 regime 同时出现在训练和验证里，导致过拟合。

## 需要的输入

### 1. Round Universe

每个 crypto 5m market 一行：

```text
symbol,slug,round_start_ts,round_end_ts,condition_id,yes_token_id,no_token_id
```

来源：

- `poly_trans_research` 的 `market_meta`
- 或本仓库 live/shadow logs

必须覆盖：

- BTC/USD
- ETH/USD
- SOL/USD
- XRP/USD
- DOGE/USD
- BNB/USD
- HYPE/USD

### 2. RTDS / Data Streams Label

每个 symbol/round 一行：

```text
symbol,round_start_ts,round_end_ts,rtds_open,rtds_close,rtds_close_recv_ms
```

用途：

- `rtds_open` 是方向边界
- `rtds_close` 是 final truth
- `rtds_close_recv_ms` 用于延迟评估

如果无法历史补齐 RTDS label，只能先用现有 `logs/local_agg_boundary_dataset_close_only_latest.csv` 和 live challenger 的 label。

### 3. 外部价格源 Boundary Tape

核心数据。每个 source tick 一行，推荐落为 gzip JSONL 或 Parquet：

```text
symbol,source,event_ts_ms,recv_ts_ms,price,bid,ask,mid,mark,last,source_event_type,raw
```

要求：

- `event_ts_ms` 必须是交易所事件时间，毫秒级。
- `recv_ts_ms` 如果来自实时采集则必须保存；历史 REST backfill 没有真实 recv，可置空。
- `price` 可以是优先价格字段；同时保留 `bid/ask/mid/mark/last` 方便搜索不同聚合口径。
- `raw` 可选，但建议保留压缩原始 payload，便于事后修 parser。

每个 round 至少需要 `[T-5s, T+0.5s]`。

推荐拉宽到 `[T-10s, T+5s]`，训练时再切窗口：

- `[-5000ms, +500ms]`
- `[-3000ms, +500ms]`
- `[-1000ms, +1000ms]`
- `[-10000ms, +5000ms]`

## Source 优先级

优先补齐：

1. Binance spot / futures，按 Chainlink 可能口径分别保留。
2. Coinbase spot。
3. OKX spot / swap。
4. Bybit spot / linear。
5. Hyperliquid mark / oracle / mid。
6. Uniswap v3 pool TWAP / slot0，作为后续增强，不阻塞第一版。

必须在字段中区分 market type：

```text
source=binance_spot
source=binance_futures
source=okx_spot
source=okx_swap
```

不要把 spot/futures 混成一个 `binance`。

## 推荐目录

由另一个 agent 在 `poly_trans_research` 或单独数据目录内产出：

```text
/Users/hot/web3Scientist/poly_trans_research/data/external_tape/
  2026-04-27/
    external_boundary_ticks.jsonl.gz
  2026-04-28/
    external_boundary_ticks.jsonl.gz
  2026-04-29/
    external_boundary_ticks.jsonl.gz
  2026-04-30/
    external_boundary_ticks.jsonl.gz
  labels/
    rtds_round_labels.csv
  round_universe/
    crypto_5m_rounds.csv
```

## 最小可交付版本

第一版不需要做复杂数据库，只要保证文件可流式读取。

最低要求：

- 覆盖最近 72h。
- 每个 symbol 至少 4 个 source。
- 每个 round 在 `[T-5s,T+0.5s]` 至少有 1 条 tick 的 source 数 >= 2。
- 输出一个 coverage report。

Coverage report 字段：

```text
symbol,total_rounds,rounds_with_2_sources,rounds_with_3_sources,rounds_with_4_sources,median_ticks_per_round,p95_source_gap_ms,max_source_gap_ms
```

## 验收 SQL / 检查逻辑

另一个 agent 交付后，必须能回答：

- 每个 symbol 72h 有多少 round。
- 每个 round 的 source_count 分布。
- 每个 source 在 `T` 附近最近 tick 的 offset 分布。
- 是否存在 source 时间戳不是毫秒的问题。
- 是否存在明显未来数据泄漏，比如使用 `T+5s` 数据训练 `T+500ms` 策略。

## 与当前 live/shadow 的关系

离线 72h backfill 用于搜索模型和规则。

但生产前仍需 live/shadow 验证，因为历史 REST 回填通常没有：

- WebSocket 断线
- recv 延迟
- broker fanout 抖动
- source missing
- 时间戳乱序
- API 限流

上线前必须冻结规则后重新跑 live/shadow。

建议验收：

- 离线 validation 24h：`side=0, max<=5bps`
- 冻结后 live/shadow 8-12h：`side=0`
- 冻结后 live/shadow rolling 300 accepted：`side=0, max<=5bps`

## 给执行 Agent 的任务摘要

请实现外部价格源 72h boundary tape backfill：

1. 从 `poly_trans_research` 导出最近 72h crypto 5m round universe。
2. 为 BTC/ETH/SOL/XRP/DOGE/BNB/HYPE 拉取 Binance、Coinbase、OKX、Bybit、Hyperliquid 的 `[T-10s,T+5s]` 历史 tick/top-of-book/mark 数据。
3. 标准化成 `external_boundary_ticks.jsonl.gz`。
4. 产出 `coverage_report.csv`。
5. 不要修改 live strategy 代码，不要启动实盘。

