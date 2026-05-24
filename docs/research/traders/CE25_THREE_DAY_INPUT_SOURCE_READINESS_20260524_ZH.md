# CE25 72h 输入源 readiness 审查

时间：2026-05-24
范围：只读当前 worktree 中已有 public/profile artifact、schema 和 source review。
结论：`UNKNOWN_CE25_THREE_DAY_INPUT_SOURCE_NOT_IN_CURRENT_WORKTREE`

## 背景

ce25 的单日 public proxy 很清楚：低 `pair_cost`、低残仓的桶表现很好，`pair_cost >= 1.00` 是明确亏损桶。但这仍然只是单日复盘。上一轮已经把它转成 `ce25_three_day_pair_cost_validation_v1` 合同，要求至少 3 天，每天都验证：

- starter：`ETH/SOL`、`5m/15m`、`pair_cost < 0.90`、残仓 `<15%`、PnL 为正。
- core：`BTC/ETH/SOL`、`5m/15m`、`pair_cost < 0.95`、残仓 `<10%`、PnL 为正。
- hard-loss：`BTC/ETH/SOL`、`5m/15m`、`pair_cost >= 1.00`、PnL 必须为负。

本轮只解决一个问题：当前仓库内是否已有安全、离线、可复跑的 72h 输入源。

## 本轮结果

新增脚本：

- `scripts/xuan_ce25_three_day_input_source_readiness.py`
- `scripts/xuan_ce25_three_day_input_source_readiness_smoke.sh`

新增 artifact：

- `xuan_research_artifacts/xuan_ce25_three_day_input_source_readiness_20260524T115616Z/manifest.json`
- `xuan_research_artifacts/xuan_ce25_three_day_input_source_readiness_smoke_20260524T115616Z/manifest.json`

当前 worktree 里可用材料：

| 输入 | 状态 |
|---|---|
| 3 天验证合同 | 已存在，合同 ready |
| ce25 public activity rows | 存在，3497 rows，但只覆盖 2026-05-23/2026-05-24 UTC，不是 72h daily cohort manifest |
| normalized real entry sample | 24 rows，其中 ce25 12 rows，不是 72h source |
| non-fixture liquidity/fair-source input | 仍缺 |
| candidate 72h source manifest | 缺失 |

因此当前结论是 `UNKNOWN`，不是 `DISCARD`。ce25 线索本身仍有效，但不能用当前 worktree 的单日/小样本 artifact 冒充三天验证。

## 安全输入合同

`ce25_three_day_input_source_readiness_v1` 要求 candidate source manifest 满足：

- `safe_offline_72h_export=true`
- 所有 `source_paths` 都在 `/Users/hot/web3Scientist/pm_as_ofi-xuan-research` 内
- 不 fetch 新数据、不读外部 worktree、不启动 service/local agg/shared WS、不 SSH、不 shadow/canary/live、不读 raw/replay/full-store
- `candidate_three_day_manifest_path` 指向包含 `daily_cohort_rows` 的 JSON
- `daily_cohort_rows` 至少覆盖 3 个 `day_id`，且每天包含 starter/core/hard-loss 三个 cohort
- validation command 必须调用：

```bash
python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --candidate-three-day-manifest <manifest.json> \
  --output-dir xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_score_<UTC>
```

明确拒绝的替代品：

- 单日 public proxy bucket
- price cap 或静态 asset/timeframe 删除
- 把 realized `pair_cost` 直接当 live criterion
- D+ 已失败的 micro-deficit / ledger / tiny-deficit / closed-cycle 家族
- fixture/synthetic daily rows 作为策略证据
- 从当前 worktree 直接读取外部 `poly_trans_research` 路径

## 决策

`research_ranking.status=UNKNOWN_CE25_THREE_DAY_INPUT_SOURCE_NOT_IN_CURRENT_WORKTREE`
`ce25_line_status=INPUT_BLOCKED_UNTIL_SAFE_72H_MANIFEST`
`promotion_gate.passed=false`
`private_truth_ready=false`
`deployable=false`
`no_order_diagnostic_allowed=false`

下一步只能二选一：

1. 把安全的 ce25 72h daily-cohort manifest 放入当前 worktree，然后运行 3 天验证 scorer。
2. 若短期拿不到安全 72h 输入，则把 ce25 mimicability 线暂时标记为 input-blocked，转向另一个已有 in-worktree 多日证据的 lead。
