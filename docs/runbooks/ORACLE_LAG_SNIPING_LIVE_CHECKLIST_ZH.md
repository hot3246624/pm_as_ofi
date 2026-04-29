# oracle_lag_sniping 实盘前检查清单

适用范围：`PM_STRATEGY=oracle_lag_sniping`（兼容旧别名 `post_close_hype`），当前支持 `PM_ORACLE_LAG_SYMBOL_UNIVERSE` 中的 `*-updown-5m`。

## 1. 启动前参数检查

### 单市场模式（非 inproc）
- `PM_STRATEGY=oracle_lag_sniping`
- `POLYMARKET_MARKET_SLUG=btc-updown-5m`（或其他 `*-updown-5m`）
- `PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp`（或 `*`）
- `PM_POST_CLOSE_WINDOW_SECS=105`
- `PM_POST_CLOSE_CHAINLINK_WS_URL=wss://ws-live-data.polymarket.com`（默认可不填）
- `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS=8`
- 策略约束：`> 0.993` 不做 taker；maker 允许继续挂胜方单

### 多市场并发模式（inproc supervisor，默认不仲裁）
在上述基础上额外设置：
- `PM_INPROC_SUPERVISOR=1`（启用 in-process supervisor，共享 Chainlink Hub）
- `PM_MULTI_MARKET_PREFIXES=btc-updown-5m,eth-updown-5m,...`（7 个市场前缀）
- 默认参数（可不设，使用内置值）：
  - `PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED=false`（默认关闭）
- 如需临时启用仲裁，再设置：
  - `PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED=true`
  - `PM_ORACLE_LAG_ARBITER_COLLECTION_WINDOW_MS=200`
  - `PM_ORACLE_LAG_ARBITER_BOOK_MAX_AGE_MS=250`

建议保守值：

- `PM_DRY_RUN=true` 先跑 5 轮观察日志，再切 live
- `PM_BID_SIZE=5`
- `PM_MAX_NET_DIFF` 维持当前安全值
- 若要验证 dry-run 下完整执行链路（intent -> OMS -> simulated fill -> inventory/tranche），加 `PM_ORACLE_LAG_DRYRUN_EXECUTE=true`
- 切 live 前务必去掉 `PM_ORACLE_LAG_DRYRUN_EXECUTE`（或保持默认 `false`）

### dry-run 全链路验收（推荐）

建议在演练时同时开启 recorder：

- `PM_RECORDER_ENABLED=true`
- `PM_RECORDER_MARKET_MODE=structured`
- `PM_DRY_RUN_FILL_PROBABILITY=1.0`

最小通过标准：

- 日志出现 `dry_taker_execute` 与 `[DRY-RUN] Taker`
- `events.jsonl` 中出现 `fill_snapshot`、`tranche_opened`、`capital_state_snapshot`
- replay 表 `own_order_events` / `pair_tranche_events` / `capital_state_events` 均非 0

## 2. 轮次内必须出现的关键日志

### 单市场模式
每轮应至少看到以下链路（顺序可能有轻微交叉）：

1. `oracle_lag_sniping enabled for ...`
2. 收盘后出现 `winner hint` 日志：优先 `source=Chainlink`
3. `DRY/PLACE` 的 BUY 单只出现在胜方一侧
4. `oracle_lag_submit_latency`（FAK）或 `oracle_lag_first_submit_logged`（maker）
   - 重点看 `delta_from_end_ms <= 1500`

### 仲裁开启时（可选）新增日志
- 启动时：`cross_market_hint_arbiter spawned | collection_window_ms=200 book_max_age_ms=250`
- 每轮每市场 hint 到达时：`oracle_lag_arbiter_intake | round_end_ts=... slug=...`
- 仲裁结果（每市场一行）：`oracle_lag_arbiter_decision | rank=1/7 selected=true reason=best_ask_eff ...`
- 未选中市场应出现：`oracle_lag_cross_market_skip | side=... reason=not_selected by arbiter`
- 选中市场应出现：`oracle_lag_arbiter_selection | round_end_ts=... selected=true rank=1`

**每轮验收（仲裁开启）**：只有 1 个市场出现 `oracle_lag_submit_latency` 或 `dry_taker_execute`（未开启执行门时是 `dry_taker_preview`），其余市场出现 `oracle_lag_cross_market_skip`。

**每轮验收（默认不仲裁）**：每个市场独立 single-shot，不再出现 `oracle_lag_cross_market_skip`。

若没有看到 winner hint，说明当前轮不会出单（策略按设计静默）。

## 3. 风险红线（触发即停）

- 连续多轮都走 `Gamma fallback` 且明显慢于盘口窗口
- winner hint 与盘口显著矛盾（例如提示 YES 胜但 NO 挂单密集成交）
- 盘后窗口内出现双边开仓（不应发生）
- 订单被大量 `cross-book` 拒绝但没有价格步降
- `first_submit=true` 的 `delta_from_end_ms` 长期大于 `1500ms`
- **仲裁开启专项**：每轮出现 >1 个市场的 `oracle_lag_submit_latency`（说明仲裁失效）
- **仲裁开启专项**：连续多轮 `oracle_lag_arbiter_selection_stale` 日志（说明 round_end_ts 对不齐）

## 4. 切换 live 的最小门槛

满足以下条件再切 `PM_DRY_RUN=false`：

- 连续 5 轮 dry-run 出现稳定 winner hint（至少 4 轮 Chainlink）
- winner hint 到第一笔下单延迟稳定（目标 `<=1.5s`）
- 无异常双边下单、无明显 stale target
- **仲裁开启时**：每轮恰好只有 1 个市场收到 `selected=true`，其余均出现 `oracle_lag_cross_market_skip`
- **仲裁模式**：选中市场与当轮 `winner_ask_eff` 最低的市场一致（可从 `oracle_lag_arbiter_decision` 日志核查）

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

## 7. 精度核验 CSV（推荐）

为避免被日志显示精度误导，导出分析 CSV 时应优先使用 `chainlink_result_ready` 的高精度开/收盘值：

```bash
python3 scripts/export_multi_dryrun_csv.py \
  --logs "logs/polymarket.*-updown-5m.log.2026-04-19" \
  --out docs/stage_g_multi_dryrun.csv
```

CSV 会额外给出：
- `delta_close_minus_open`
- `winner_by_delta`
- `winner_match_logged`
- `precision_source`
