# ce25 Projected Guard Fixture Scorer - 2026-05-24

## 结论

本轮只使用已提交的 projected-guard spec artifact 和 synthetic fixtures，没有 fetch 新数据、没有启动 shadow/canary/local agg/shared WS/live，也没有读取 raw/replay/full store。

decision:
`KEEP_CE25_PROJECTED_GUARD_FIXTURE_SCORER_READY`

artifact:
`xuan_research_artifacts/xuan_ce25_projected_guard_fixture_scorer_20260524T105851Z/manifest.json`

含义：

- `ce25_projected_pair_cost_residual_guard_v1` 的公式可以用订单前字段执行。
- scorer 已经把 realized bucket 和 projected decision 分开。
- fixture 只证明公式和 fail-closed 逻辑，不是策略收益证据。
- 仍不允许 no-order diagnostic、shadow、canary、live 或 deploy。

## 通过的 fixture

| case | 判定 | guard | projected pair cost | projected residual |
|---|---|---|---:|---:|
| starter_eth_completion_pass | ALLOW | starter | 0.843 | 0.0000 |
| core_btc_pair_pass | ALLOW | core | 0.900 | 0.0000 |
| final_1m_5m_cleanup_pass | ALLOW | starter | 0.800 | 0.0000 |

这些 fixture 验证：

- ETH/SOL starter guard 可以按 `projected_pair_cost < 0.90`、projected residual `<15%` 判定。
- BTC/ETH/SOL core guard 可以按 `projected_pair_cost < 0.95`、projected residual `<10%` 判定。
- final 1-5m 只有 `completion_cleanup` 能通过。

## Fail-Closed 覆盖

| case | 判定 | 关键字段 |
|---|---|---|
| pair_cost_hard_kill | BLOCK_HARD_KILL_PAIR_COST_GTE_1_00 | projected pair cost 1.10 |
| near_close_residual_hard_kill | BLOCK_HARD_KILL_RESIDUAL_NEAR_CLOSE | residual 69.23%, close 前 240s |
| final_60s_initiation_block | BLOCK_FINAL_60S_INITIATION | close 前 45s initiation |
| missing_pre_action_field_fail_closed | FAIL_CLOSED_PRE_ACTION_FIELD_MISSING | 缺 pre_no_actual_cost |
| missing_projected_field_probe | FAIL_CLOSED_PROJECTED_FIELD_MISSING | 缺 projected_pair_cost |

这说明下一层如果不能安全产出 projected 字段，必须停在 UNKNOWN，不允许把事后 realized pair cost 代替。

## 明确边界

这不是：

- real public-profile strategy evidence。
- private truth。
- deployable/canary/promotion evidence。
- fixture probability 或 price-band/static asset-timeframe 过滤。
- D+ micro-deficit/ledger/tiny-deficit/closed-cycle/fill-to-balance 复活。

## 下一步

实现 `ce25_projected_guard_runner_instrumentation_spec_v1`，仍然 local-only。

目标：

- 审查 runner 是否能在行为不变、default-off 的前提下输出同样的 pre-action 字段。
- 明确 `pre_yes/no_qty`、`pre_yes/no_actual_cost`、estimated fee、intended order、seconds_to_expiry 的来源。
- 如果 runner 无法安全产出 projected pair cost/residual diagnostics，就直接 UNKNOWN 或 DISCARD，不启动 no-order diagnostic。
