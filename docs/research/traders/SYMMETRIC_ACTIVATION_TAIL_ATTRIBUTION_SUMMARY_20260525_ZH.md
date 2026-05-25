# Symmetric Activation Tail Attribution Summary 2026-05-25

结论：`KEEP_SYMMETRIC_ACTIVATION_TAIL_ATTRIBUTION_SUMMARY_SCORER_READY`

本轮在本地 worktree 内完成默认关闭 `symmetric_activation_tail_attribution_summary_v1`，只扩展 runner 的 event-lite 诊断面，不改 seed gate、fill、pairing、salvage 或任何交易决策。新增：

- `tools/xuan_dplus_passive_passive_shadow_runner.py`
- `scripts/xuan_symmetric_activation_tail_attribution_summary_scorer.py`
- `scripts/xuan_symmetric_activation_tail_attribution_summary_smoke.sh`

对应 smoke artifact：

- `xuan_research_artifacts/xuan_symmetric_activation_tail_attribution_summary_smoke_20260525T041000Z/manifest.json`

对应 scorer artifact：

- `xuan_research_artifacts/xuan_symmetric_activation_tail_attribution_summary_smoke_20260525T041000Z/scorer/manifest.json`

## 实现范围

新增 flag `--symmetric-activation-tail-attribution-event-lite-summary`，依赖 `--event-lite-summary`，否则 fail closed。

新增 summary：

- `event_lite.symmetric_activation_tail_attribution_summary.schema_version = symmetric_activation_tail_attribution_summary_v1`
- `pair_qty_sum_by_status_reason_activation_bucket`
- `pair_cost_bucket_by_status_reason_activation_bucket`
- `residual_qty_sum_by_status_reason_activation_bucket`
- `residual_cost_sum_by_status_reason_activation_bucket`
- `pair_tail_loss_sum_by_status_reason_activation_bucket`
- `source_sequence_presence_by_status_reason_activation_bucket`
- 上述字段对应的 `status|reason|side|offset_bucket|activation_bucket` 版本
- `residual_tail_exemplars_by_status_reason_activation_bucket`

这里的 pair/tail 口径是 `source-attributed`，即一笔已配对 pair 会回写到其来源 lot；因此 aggregate `pair_qty` 与 `pair_tail_loss` 反映的是来源 lot 质量分布，不是唯一 pair action 计数。

## 已验证点

- 默认关闭时 aggregate 不含该 summary。
- 开启后 schema、field contract、aggregate merge、summary parity 均通过。
- CLI 依赖 fail closed。
- post-action outcome labels 仍排除。
- `realized_pair_cost_used_as_live_criteria=false`。
- `private_truth_ready=false`、`deployable=false`、`promotion_gate_passed=false`。
- 行为面未变；smoke 中 `pair_actions` 与 `residual_qty` 在 default-off / enabled 间一致。

## 限制

- 该 summary 只提供 local-only attribution 证据，不证明 `private_truth_ready`、promotion gate、deployable、canary readiness。
- `status_reason_scope` 当前只覆盖 `admitted|candidate`；blocked activation 继续由既有 `symmetric_activation_summary_v1` 负责。
- 该 summary 不能作为 live side/price/offset cap、realized pair-cost live criterion、或任何 no-order / shadow 启动依据。

## 下一步

只允许在已存在 allowlisted pullback summary/aggregate 的前提下，用新 scorer 读取本地 artifact，确认真实 UNKNOWN run 的 pair-cost / residual / pair-tail-loss 是否集中到合法 activation buckets；不得从这里直接启动新的 no-order diagnostic。
