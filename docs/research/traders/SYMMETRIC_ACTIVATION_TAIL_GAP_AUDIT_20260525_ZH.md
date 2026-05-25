# Symmetric Activation Tail Gap Audit

结论：`KEEP_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INSTRUMENTATION_TARGET_READY`。

本审计只读取 allowlisted pullback 的 aggregate/summary JSON 与本地 scorer manifest，不读取 events JSONL，不扫 raw/replay/full store，不启动 SSH、shadow、canary、local agg、shared WS 或 live 组件。

## 结果

这次 no-order 不是经济性全负：fee-adjusted pair PnL proxy 为正，但 normal shadow risk budget 没过：

- `net_pair_cost_p90=1.06 > 1.0`
- `residual_qty_share=16.572344% > 15%`
- `pair_tail_loss_share=10.400583% > 5%`

已有 summary 能说明风险不是完全随机：

- high pair-cost buckets 有 `16/77` 个 pair actions。
- high pair-cost source-record mass 主要集中在 `offset_ge_120`，但也有 `offset_30_60/60_90/0_30` 尾部，不能写成 offset 静态 cap。
- residual cost 主要来自 `YES` side，且 residual qty 主要在低价 bucket，但 side/price cap 属于 forbidden static filter，不能作为 live rule。
- source-link residual tail 在 `side|offset|risk_direction` 层面有集中行，但目前没有把这些行直接 join 到 `activation_bucket` 和 activation age。

## 缺口

当前 `symmetric_activation_summary_v1` 证明了 schema、field contract、source_sequence coverage 与 aggregate parity，也给出 admitted/blocked activation denominators。但它缺少直接解释风险预算失败的 attribution fields：

- pair-cost bucket by `status|reason|activation_bucket`
- pair-cost bucket by `status|reason|side|offset|activation_bucket`
- residual qty/cost by `status|reason|activation_bucket`
- residual qty/cost by `status|reason|side|offset|activation_bucket`
- pair-tail-loss by activation bucket
- residual-tail exemplars keyed by activation bucket

## 下一步

实现默认关闭 `symmetric_activation_tail_attribution_summary_v1`。它只做诊断 attribution，不改变行为，不把 realized pair cost 当 live criteria，不引入 side/price/offset static cap，也不重启 public-profile/D+ failed families。

`research_ranking` 和 `promotion_gate` 继续分开：本审计不是 private truth、deployable、canary 或 promotion evidence；不能从本审计直接启动另一个 no-order diagnostic。
