# Symmetric Activation No-Order Proposal Review

本轮只做 local-only proposal，不启动 shadow。

## 候选

当前本地 train/holdout lead 来自 cap102 grid expansion：

- `edge=0.075`
- `activation_mode=opp_seen`
- `activation_window_s=20`
- `leak_rate=0.005`
- `min_opp_count=1`
- holdout pair actions `900`
- holdout residual qty rate `1.0339%`
- holdout worst net fee-after `+283.972185`

对照参数是上一轮真实 no-order 的 `edge=0.07`、`activation_window_s=7.5`。上一轮真实 no-order 经济性为正，但 normalized risk budget 失败，所以不能 deploy/canary/promotion。

## Exact Diagnostic Shape

如果之后用户明确批准，下一次 exact bounded no-order diagnostic 应该只跑这一条：

- `PM_DRY_RUN=true`
- shared-ingress role `client`
- prefix `btc-updown-5m`
- offsets `0..25`
- duration `3600s`
- edge `0.075`
- target qty `5`
- max open cost `80`
- imbalance qty cap `1.25`
- seed offset max `300s`
- activation `opp_seen`
- activation window `20s`

必须开启 summary：

- `--event-lite-summary`
- `--pair-source-event-lite-summary`
- `--source-link-transition-event-lite-summary`
- `--source-link-residual-tail-exemplars-event-lite-summary`
- `--symmetric-activation-event-lite-summary`
- `--symmetric-activation-tail-attribution-event-lite-summary`

继续禁用：

- late repair only
- fill-to-balance
- micro-deficit guard
- portfolio ledger trading rule
- source-opportunity marker families
- public-profile single-day filters
- live/canary/orders/cancels/redeems

## 状态

该 proposal 不是启动授权，也不是策略证据。`promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false` 保持不变。
