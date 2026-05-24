# Symmetric Activation Runner Instrumentation

时间：2026-05-25 BJT

结论：`KEEP_SYMMETRIC_ACTIVATION_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY`

## 本轮做了什么

在 `tools/xuan_dplus_passive_passive_shadow_runner.py` 中加入默认关闭 diagnostics flag：

`--symmetric-activation-event-lite-summary`

它只在同时开启 `--event-lite-summary` 时生效。默认关闭时 summary/aggregate 不出现相关字段，候选数量、block 数和 runner 行为不变。

## 输出 schema

开启后写入：

`event_lite.symmetric_activation_summary`

schema:

`symmetric_activation_contract_summary_v1`

核心字段：

- `candidate_count_by_status_reason_activation_bucket`
- `candidate_count_by_status_reason_side_offset_activation_bucket`
- `projected_pair_cost_bucket_by_status_reason_activation_bucket`
- `projected_residual_rate_bucket_by_status_reason_activation_bucket`
- `activation_age_bucket_by_status_reason_activation_bucket`
- `source_sequence_presence_by_status_reason_activation_bucket`
- `residual_leak_stress_cost_sum_by_status_reason_activation_bucket`

`field_contract` 固定：

- `default_off=true`
- `post_action_outcome_labels_included=false`
- `realized_pair_cost_used_as_live_criteria=false`
- `trading_behavior_changed=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate_passed=false`

## Smoke 结果

本地 synthetic fixture 验证：

- default-off summary absent。
- enabled summary present。
- candidate count unchanged。
- blocked activation count unchanged。
- aggregate merge 正常。
- blocked activation denominator = 1。
- admitted activation denominator = 1。
- source_sequence coverage present。
- projected pair-cost bucket present。
- 缺 `--event-lite-summary` 时 CLI fail closed。

## 边界

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

这是 runner instrumentation readiness，不是 strategy evidence、private truth、deployable、canary 或 promotion evidence。

## 下一步

实现 `symmetric_activation_summary_scorer_v1`：

- 只读 summary/aggregate manifest。
- 验证 schema、field_contract、aggregate parity。
- 验证 selected activation denominator、projected pair-cost/residual bucket、source_sequence coverage。
- 对缺字段、behavior-changing 字段、private/deployable/promotion claim fail closed。
