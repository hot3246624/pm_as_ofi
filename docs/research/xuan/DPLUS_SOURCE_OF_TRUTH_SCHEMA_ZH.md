# xuan_b27_dplus Source-of-Truth Schema

更新时间：2026-05-15

## 目标

这份 schema 只定义 `xuan_b27_dplus` 从 observer 到 canary 需要闭合的证据链。它不是回测指标表，也不是公开账户 proxy truth。任何字段缺失时，source-of-truth verdict 必须保持 `UNKNOWN`，不能升级为 `PASS`。

## 核心 id 链

### Candidate

来源：`xuan_b27_dplus_observer_tick.data.observer_candidates[]`

必需字段：

- `strategy`
- `run_id`
- `market_session_id`
- `candidate_id`
- `side`
- `observer_price`
- `target_qty`
- `order_attempt_trace_preview.order_attempt_id`

当前 observer 只允许 preview：

- `order_attempt_trace_preview.preview_only=true`
- `order_attempt_trace_preview.submitted=false`
- `order_attempt_trace_preview.venue_order_id=null`

### Order Attempt

来源：未来授权后的 dry-run/auth observer 或 canary controller。当前只存在 preview，不存在真实 attempt。

必需字段：

- `run_id`
- `market_session_id`
- `candidate_id`
- `order_attempt_id`
- `client_order_id`
- `venue_order_id`
- `submitted`
- `post_only`
- `dry_run`

当前 recorder join 点：

- `order_accepted.data.correlation.candidate_id`
- `order_accepted.data.correlation.order_attempt_id`
- `order_accepted.data.order_id`
- `order_accepted.data.venue_order_id`

### Fill Truth

来源：authenticated user websocket 解析后的 `user_ws_fill_parsed`。

必需字段：

- `order_id`
- `trade_id`
- `liquidity_role`
- `side`
- `direction`
- `filled_size`
- `price`
- `status`
- `fee_rate_bps`

当前 join 点：

- `user_ws_fill_parsed.data.order_id`
- `user_ws_fill_parsed.data.trade_id`

如果没有 authenticated user websocket fill，不能用 public-account proxy truth 替代私有 fill truth。

### Lot / Pair

来源：D+ lot ledger / reconciler。

必需字段：

- `lot_id`
- `pair_id`
- `candidate_id`
- `order_attempt_id`
- `venue_order_id`
- `trade_id`
- `side`
- `qty`
- `price`
- `fee`

lot 必须能回溯到 candidate 和 fill；pair 必须能回溯到两侧 lot。

### Redeem / Cashflow

来源：redeem result + wallet/cashflow snapshot。

必需字段：

- `run_id`
- `market_session_id`
- `pair_id`
- `redeem_attempt_id`
- `redeem_tx_hash`
- `cashflow_snapshot_id`

当前 Rust correlation spine 的判定：

- `missing_redeem_cashflow_truth_fields()` 任一缺失 => `UNKNOWN`
- 只有上述字段齐全 => `PASS`

## Verdict 规则

### Order / Fill

`order_fill_verdict()` 需要以下字段全部齐全：

- `run_id`
- `market_session_id`
- `candidate_id`
- `order_attempt_id`
- `client_order_id`
- `venue_order_id`
- `fill_event_id`
- `trade_id`
- `lot_id`

### Redeem / Cashflow

`redeem_cashflow_verdict()` 需要以下字段全部齐全：

- `run_id`
- `market_session_id`
- `pair_id`
- `redeem_attempt_id`
- `redeem_tx_hash`
- `cashflow_snapshot_id`

## 禁止替代

以下证据不能替代 private source-of-truth：

- B27/RWO public-account audit
- public Data API trade rows
- P2 public L1/L2 touch
- virtual queue-supported fill
- dry-run synthetic fill

它们可以用于研究和 plausibility，但不能单独让 canary/prod verdict 变成 `PASS`。
