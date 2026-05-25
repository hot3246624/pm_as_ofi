# Strategy Discovery Train/Holdout Selector

结论：当前应该继续推进本地 train/holdout 策略发现，而不是继续从单日 public-profile 里强挖部署结论。

## 决策

`ce25` 继续保留，但只作为 feature prior：

- projected pair cost
- projected residual rate
- late-window initiation/completion policy
- ETH/SOL/BTC 5m/15m 的候选特征

这些不能当成策略证据：

- 单日 public-profile PnL
- realized pair_cost bucket
- fixture projected-guard decision
- 纯 price/asset/timeframe filter

当前可执行本地 lane 是 `symmetric_activation_strict_cache_grid_expansion`。理由是它有当前 worktree 内的 covered/holdout strict-cache evidence，并且最近 no-order 失败是 risk-budget fail，不是 instrumentation failure 或经济性全负。

## 发现合同

允许：

- train 内使用 post-action labels 做离线发现
- 冻结后的 predicate 只能包含 pre-action fields
- holdout 上不重选阈值
- 使用 activation age/count、same/opp inventory、projected pair cost、projected residual、source_sequence/source-link context

禁止：

- realized pair_cost 作为 live criterion
- future settlement labels 作为 live criterion
- 单日 public-profile filters 作为 strategy evidence
- 纯 price cap、side cap、offset cap 或 timeframe 删除
- 复活 D+ micro-deficit/ledger/tiny-deficit/closed-cycle/fill-to-balance 失败族
- 从 selector 直接启动 SSH/shadow/canary/live/local agg/shared WS

## 下一步

运行本地 strict-cache grid expansion，扩展 symmetric activation 参数空间，然后只把冻结的 pre-action predicate 送到 holdout。任何 no-order diagnostic 仍需单独、明确批准。
