# Xuan Symmetric Admission Implementability Review 2026-05-19

状态：`BLOCKED_SOURCE_TRUTH_REQUIRED`

范围：只审查 xuan-owned code/profile/analyzer 和已完成本地 artifact；未部署、未重启、未扫描 raw/replay/collector。

## 结论

`symmetric admission` 现在不是 shadow-ready。最新 residual leakage verifier 把门槛压得很清楚：

- 需要 maker price improvement 至少约 `7.5c`。
- 需要未配对 first-leg maker quote 的泄漏率不高于约 `0.5%`。
- 只要 leak 到 `1%`，即使 edge=0.08，holdout 日级 worst 仍为负。

现有代码已经能记录 `book_depth_touch` 填单和 depth evidence，但还不能证明上述两个门槛。

## 已具备的证据链

- `MarketDataMsg::BookDepthTick` 携带 `market_side`、best bid/ask、best size、best-level drop qty、event time、source sequence id。
- `FillSource::DryRunBookDepthTouch` 已与 `book_touch` / `trade_sell_touch` 分开。
- `dry_run_touch_fill_confirmed` 可以输出 `source=book_depth_touch`、depth best bid/ask、depth size/drop、event time、`depth_source_sequence_id`。
- `analyze_pgt_shadow_events` 已经能单独统计 `dry_run_touch_book_depth` 和 depth evidence coverage。

这些足够做 dry-run taxonomy，不足以证明可部署 maker lifecycle。

## 缺口

第一，缺 first-leg quote lifecycle 的完整分母。我们需要每个 maker quote 的 `quote_intent_id`，并串起：

- order plan / order sent / order accepted
- cancel sent / cancel ack
- fill / partial fill
- expiry reason
- condition_id / side / price / size / placed_ts / accepted_ts / cancel_ts
- matched pair id 或 opposite trigger ts

否则只能看到“确认被 touch 的 fill”，看不到所有未配对挂单里有多少实际泄漏。

第二，缺 price improvement 的真实分布。当前 verifier 假设 `public_trade_price - edge` 能被动成交；实现上需要记录：

- reference public trade price
- intended quote price
- realized/simulated fill price
- improvement = reference - fill
- 按 improvement bucket 的日级 worst fee-after PnL
- fill 对应的 depth/queue/source_sequence coverage

第三，`edge=0.07` inventory repair 虽然 public-covered aggregate 已经 stress-positive，但仍是 source-truth 阻塞：05-16/17 public truth 缺失，且 uncovered stress 仍为负。

## 决策

不应重启或 shadow。下一步应该先做 quote-lifecycle evidence artifact；如果短期拿不到这个证据，就继续在本地 verifier 侧降低 unmatched first-leg exposure，而不是把 symmetric admission 直接推到服务器。
