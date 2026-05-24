# Symmetric Activation Contract

时间：2026-05-25 BJT

结论：`KEEP_SYMMETRIC_ACTIVATION_CONTRACT_READY`

## 目的

上一轮已把 public-profile mimicability 线冻结/丢弃，并选中新线：

`symmetric_activation_residual_stress`

本轮把它转成可执行的本地合同 `symmetric_activation_contract_v1`。这仍然是 research-only 合同，不是 runner 行为变更，也不是 shadow/canary/live 许可。

## 选中参数

| 字段 | 数值 |
|---|---:|
| asset | BTC |
| market_prefix | btc-updown-5m |
| edge | 0.07 |
| activation_mode | opp_seen |
| activation_window_s | 7.5 |
| min_opp_count | 1 |
| leak_rate | 0.02 |

covered/holdout 证据来自：

`xuan_research_artifacts/xuan_symmetric_activation_residual_stress_strict_count_boundary_20260519T1431Z/manifest.json`

## 合同字段

下一步默认关闭 instrumentation 需要只使用 pre-action 字段：

- `condition_id`
- `market_slug`
- `side`
- `ts_ms`
- `offset_s`
- `source_sequence_id`
- `public_trade_px`
- `public_trade_size`
- `seed_px`
- `candidate_qty`
- `pre_seed_same_qty`
- `pre_seed_opp_qty`
- `pre_seed_same_cost`
- `pre_seed_opp_cost`
- `risk_increasing_seed`
- `activation_required`
- `activation_mode`
- `activation_window_s`
- `activation_opposite_seen`
- `activation_opp_age_ms`
- `opposite_trigger_ts_ms`

派生诊断字段：

- `projected_pair_qty`
- `projected_pair_cost`
- `projected_residual_qty`
- `projected_residual_rate_on_bought_qty`
- `residual_leak_stress_cost`
- `activation_bucket`
- `activation_opp_age_bucket`
- `projected_pair_cost_bucket`
- `projected_residual_rate_bucket`
- `source_sequence_presence`

## Runner 可实现性

当前 `tools/xuan_dplus_passive_passive_shadow_runner.py` 已具备：

- `activation_mode=opp_seen`
- `activation_window_s`
- `activation_last_seen_ms`
- `activation_allows_seed`
- `activation_opp_age_ms`
- `opposite_trigger_ts_ms`
- `pre_seed_same_qty/pre_seed_opp_qty`
- `pre_seed_same_cost/pre_seed_opp_cost`
- `source_sequence_id`

选中参数是 `min_opp_count=1`，所以当前 runner 的 boolean opposite-seen 能表达该合同。若未来需要 `min_opp_count>1`，必须新增计数状态，不能复用当前合同。

## 明确不做

本轮没有：

- fetch 新数据
- 读取外部 worktree
- SSH
- shadow/canary/live
- observer dry-run
- local agg/service/shared WS
- raw/replay/full-store scan
- shared-ingress/broker/env/live 修改
- order/cancel/redeem

`private_truth_ready=false`，`deployable=false`，`promotion_gate.passed=false`，`no_order_diagnostic_allowed=false`。

## 下一步

实现默认关闭 runner summary：

`--symmetric-activation-event-lite-summary`

要求 smoke 证明：

- default-off 不输出 summary。
- enabled 输出 `symmetric_activation_contract_summary_v1`。
- aggregate parity 通过。
- 缺 `--event-lite-summary` fail closed。
- `field_contract` 保持 `trading_behavior_changed=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate_passed=false`。
