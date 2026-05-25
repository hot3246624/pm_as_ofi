# Strategy Discovery Grid Expansion Review

本轮把策略发现从 public-profile 模仿切到本地 strict-cache train/holdout 搜索。

## 结果

第一组更严参数 `strict_l1_pair_cap=1.00` 没有可用候选，qualified count 为 0。这个 cap 不是更稳健，而是把当前 strict-cache 可交易面打空，所以本轮直接丢弃该入口。

第二组 `strict_l1_pair_cap=1.02`、`max_residual_qty_rate=0.04` 跑出 120 个 qualified 参数组。按 holdout worst net 排序的当前最强候选：

- `edge=0.075`
- `activation_window_s=20`
- `leak_rate=0.005`
- `min_opp_count=1`
- covered pair actions `4700`
- covered residual qty rate `0.9826%`
- covered worst net fee-after `+1416.566739`
- holdout pair actions `900`
- holdout residual qty rate `1.0339%`
- holdout worst net fee-after `+283.972185`

## 解释

这是本地 train/holdout research lead，不是部署证据。最近真实 no-order symmetric activation run 仍然是 `UNKNOWN`，失败点是 normalized risk budget：p90 pair cost、residual qty share、pair tail loss share。

因此：

- cap=1.02 的候选保留为下一轮研究 lead。
- cap=1.00 的候选入口丢弃。
- ce25 继续只作为 projected pair-cost/residual 的 feature prior。
- 不自动启动 shadow/canary/live。

## 下一步

本地比较新候选 `edge=0.075/window=20s` 与上一轮 no-order 参数 `edge=0.07/window=7.5s`，生成 exact bounded no-order diagnostic proposal。只有用户明确批准后，才允许启动下一次 no-order diagnostic。
