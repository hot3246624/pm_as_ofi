# ce25 Projected Pair-Cost / Residual Guard Spec - 2026-05-24

## 结论

本轮只使用已存在的 b55/ce25 deep-compare public export 和同事的 rules report，没有 fetch 新数据、没有启动 shadow/canary/local agg/shared WS/live，也没有读取 raw/replay/full store。

decision:
`KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_READY`

artifact:
`xuan_research_artifacts/xuan_ce25_projected_pair_cost_residual_guard_spec_20260524T104842Z/manifest.json`

含义：

- ce25 复制线不应因为上一轮 15m tail audit 直接丢弃。
- 同事报告里的 `pair_cost + residual` 规则是真正有价值的结构诊断。
- 这些规则不能直接作为 live 条件，因为报告中的 pair_cost/residual 是事后 market-level realized bucket。
- 下一步必须把它们翻译成每次下单前可计算的 `projected_pair_cost` 和 `projected_residual_rate_on_bought_qty`。
- 在 projected 字段没有接入前，不允许 no-order diagnostic、shadow、canary、live 或 deploy。

## 事后 bucket 复核

ce25 关键 bucket：

| 规则 | markets | cohort PnL | wins/losses | avg pair cost | avg residual |
|---|---:|---:|---:|---:|---:|
| all markets | 640 | +644.529813 | 323/317 | 0.965167 | 0.192265 |
| ETH/SOL 5m+15m, pair_cost < 0.90, residual < 15% | 61 | +1752.797631 | 61/0 | 0.796786 | 0.070275 |
| BTC/ETH/SOL 5m+15m, pair_cost < 0.95, residual < 10% | 119 | +5078.222860 | 118/1 | 0.861041 | 0.046593 |
| BTC/ETH/SOL 5m+15m, pair_cost >= 1.00 | 163 | -5589.455319 | 18/145 | 1.089349 | 0.135101 |

b55 control：

| 规则 | markets | cohort PnL | wins/losses | avg pair cost | avg residual |
|---|---:|---:|---:|---:|---:|
| ETH/SOL 5m+15m, pair_cost < 0.90, residual < 15% | 46 | +2932.153936 | 46/0 | 0.735752 | 0.073765 |
| BTC/ETH/SOL 5m+15m, pair_cost < 0.95, residual < 10% | 79 | +8264.336252 | 79/0 | 0.813246 | 0.046304 |
| BTC/ETH/SOL 5m+15m, pair_cost >= 1.00 | 140 | -13711.717569 | 18/122 | 1.117255 | 0.218717 |

这说明：

- `pair_cost >= 1.00` 是强负桶。
- `pair_cost < 0.95` 且 residual < 10% 是 ce25/b55 都成立的强正桶。
- ETH/SOL starter bucket 更窄、更干净，适合作为第一工程化合同。

## Projected 合同

合同名：
`ce25_projected_pair_cost_residual_guard_v1`

必须在下单前可用的字段包括：

- market: `condition_id`、asset、timeframe、start/end、`seconds_to_expiry`
- intended order: `action_intent`、outcome、order_qty、order_price、`estimated_fee_per_share`
- pre-state: `pre_yes_qty`、`pre_no_qty`、`pre_yes_actual_cost`、`pre_no_actual_cost`
- projected state: `projected_yes_qty`、`projected_no_qty`、`projected_yes_actual_cost`、`projected_no_actual_cost`
- guard outputs: `projected_pair_qty`、`projected_pair_cost`、`projected_residual_qty`、`projected_total_bought_qty`、`projected_residual_rate_on_bought_qty`

公式：

- `projected_pair_qty = min(projected_yes_qty, projected_no_qty)`
- `projected_pair_cost = paired YES cost + paired NO cost / projected_pair_qty`
- `projected_residual_qty = abs(projected_yes_qty - projected_no_qty)`
- `projected_residual_rate_on_bought_qty = projected_residual_qty / projected_total_bought_qty`

如果 paired cost allocation 不能在下单前确定，必须 fail closed。

## Guard 规则

starter 合同：

- assets: ETH/SOL
- timeframes: 5m/15m
- `projected_pair_cost < 0.90`
- `projected_residual_rate_on_bought_qty < 15%`
- final 60s 不允许 initiation
- final 1-5m 只允许 completion_or_cleanup

core 合同：

- assets: BTC/ETH/SOL
- timeframes: 5m/15m
- exclude XRP
- `projected_pair_cost < 0.95`
- `projected_residual_rate_on_bought_qty < 10%`
- final 60s 不允许 initiation
- final 1-5m 只允许 completion_or_cleanup

hard kill：

- `projected_pair_cost >= 1.00` 时禁止新开或加仓，只允许能降低 residual 的 cleanup。
- close 前 300s 若 projected residual > 20%，禁止继续扩大 exposure。
- 任何单市场大未配对仓位进入 resolution 前必须 fail closed，直到有 residual-reducing plan。

## 明确非目标

这不是：

- 纯 Polymarket price-band cap。
- 静态 asset/timeframe 删除。
- D+ micro-deficit/ledger/tiny-deficit/closed-cycle/fill-to-balance 复活。
- fixture fair-probability 策略。
- private truth、deployable、canary 或 promotion evidence。

## 下一步

实现 `ce25_projected_guard_fixture_scorer_v1`，local-only。

目标：

- 用 synthetic pre-action state/order fixture 计算 projected pair cost 和 residual。
- 证明 starter/core/hard-kill 分支可以正确判定。
- 缺任何 projected 字段时 fail closed。
- 继续保持 `promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false`。
