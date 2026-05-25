# Symmetric Activation Next Candidate Selector

日期：2026-05-25

## 结论

当前 selector 结论为：

`KEEP_SYMMETRIC_ACTIVATION_PROJECTED_TAIL_JOIN_EXACT_DIAGNOSTIC_PROPOSAL_READY`

这不是自动启动授权，也不是 deployable/promotion evidence。它只说明：在当前 worktree 内，edge `0.075` / activation window `20s` 的上一轮 no-order 失败仍有一个合法、最小的下一步诊断：同样的 bounded no-order 配置，但额外打开新实现的 projected residual tail-join summary。

## 为什么不是直接 DISCARD

上一轮真实 no-order：

- candidates `233`
- fee-adjusted pair PnL proxy `+4.579513`
- net pair cost p90 `0.99`
- pair tail loss share `0`
- material residual lots `0`

失败集中在 residual budget：

- residual qty share `29.650083%`
- residual cost share `26.014802%`

因此它不是经济性全负，也不是 instrumentation failure；但也不能部署或 canary。

## 为什么不能自动开跑

新的 projected residual tail-join summary 只是在上一轮 no-order 后实现的，上一轮真实 pullback 没有这份 summary，无法直接重打分。

下一次若要跑，只能是用户明确批准的 exact bounded diagnostic，并且仍然是 no-order / dry-run / allowlisted pullback。

## Exact Proposal

候选名称：

`xuan_research_symmetric_activation_review_edge0075_actoppseen200_projected_tail_APPROVAL_TS`

配置：

- `PM_DRY_RUN=true`
- prefix `btc-updown-5m`
- offsets `0..25`
- duration `3600s`
- edge `0.075`
- activation mode `opp_seen`
- activation window `20s`
- target qty `5`
- max open cost `80`
- imbalance qty cap `1.25`
- seed offset max `300s`

必须启用 summaries：

- `--event-lite-summary`
- `--pair-source-event-lite-summary`
- `--source-link-transition-event-lite-summary`
- `--source-link-residual-tail-exemplars-event-lite-summary`
- `--symmetric-activation-event-lite-summary`
- `--symmetric-activation-tail-attribution-event-lite-summary`
- `--symmetric-activation-projected-residual-tail-join-event-lite-summary`

仍然禁用：

- late repair
- fill-to-balance
- micro-deficit repair guard
- portfolio-ledger trading rule
- source-opportunity marker families
- public-profile single-day filters
- live/canary/orders/cancels/redeems

## 验收口径

常规 acceptance 仍必须通过：

- candidates `>=100`
- net pair cost p90 `<=1.0`
- residual qty share `<=15%`
- residual cost share `<=20%`
- pair tail loss share `<=5%`
- material residual lots `0`
- fee-adjusted pair PnL proxy positive

新增诊断要求：

- `symmetric_activation_projected_residual_tail_join_summary_v1` present
- source sequence coverage present
- aggregate parity pass
- research ranking 与 promotion gate 分离
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate.passed=false`

如果 normal risk budget 仍失败，但 projected residual `gt20` bucket 能解释 residual cost 的集中尾部，则只能 KEEP 为下一层 projected-residual guard spec target，不能视为 deployable。

如果 residual 仍然分散，或只能靠 side/offset/price cap、realized pair cost、future label 解释，则该参数线应 DISCARD 或 pivot。
