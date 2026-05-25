# Xuan-Frontier Bridge-Aware Shadow Review Request - 2026-05-26

## 结论

xuan-frontier soft-mainline / cap25 explicit-surplus 证据已经达到研究侧 shadow review / paper-shadow review 口径。

这个结论只表示：no-order dry-run 证据、bridge-aware repeat gap、source/economic gate、public/young residual review surface 和 approval gate 已经清到 research review 标准。

这个结论不表示 deployable，不允许 live orders，不允许 production deploy，不允许 restart/shared service mutation，也不授权 shadow deployment。

## 当前审批状态

- approval gate：`KEEP_SHADOW_REVIEW_APPROVAL_GATE_PASS_RESEARCH_ONLY`
- approval scorecard：`.tmp_xuan/scorecards/xuan_shadow_review_approval_gate_bridgeaware_20260525T2254Z.json`
- shadow packet：`KEEP_SHADOW_REVIEW_PACKET_BRIDGE_READY_RESEARCH_ONLY`
- shadow evidence ledger：`KEEP_SOFT_MAINLINE_SHADOW_EVIDENCE_LEDGER_SHADOW_GATE_CLEAR_LOCAL_ONLY`
- surplus bridge：`KEEP_SOFT_MAINLINE_SURPLUS_BRIDGE_CLEARS_REPEAT_GAP_LOCAL_ONLY`
- bridge shadow gap：`KEEP_SOFT_MAINLINE_BRIDGE_SHADOW_GAP_CLEAR_LOCAL_ONLY`
- public benchmark comparison：`KEEP_PUBLIC_BENCHMARK_COMPARISON_BRIDGE_RESIDUAL_CAVEAT_RESEARCH_ONLY`
- young residual scorer：`KEEP_YOUNG_TINY_RESIDUAL_SCORER_PER_WINDOW_RESIDUAL_CAVEAT_LOCAL_ONLY`
- deployable：`false`
- live_orders_allowed：`false`
- remote_runner_allowed：`false`

代码口径已推到 branch `codex/xuan-frontier-depth-consumer`，截至 `20eb093e`。

## Evidence Surface

最新材料窗口是 `xuan-frontier-soft-mainline-cap25-reproduction-20260525T2041Z`。这是一次 bounded 1800s `PM_DRY_RUN=1` no-order cap25/soft-mainline reproduction。

安全结果：

- run_exit_code：`0`
- run_stderr：`0`
- remote_wrapper_stderr：`0`
- orders_sent：`false`
- no shared mutation：`true`
- no remote repo mutation：`true`
- no runner remains：`true`

单窗口运行指标：

- accepted_actions：`36`
- queue_supported_fills：`28`
- strict_rescue_closes：`13`
- pair_pnl：`+2.959632`
- ROI on filled cost：`14.4943%`
- residual_qty_share：`25.8929%`
- residual_cost_share：`19.1695%`
- accepted L1 age max：`25ms`
- rescue L1 age max：`0ms`
- strict_rescue_source_blocks：`0`
- rescue_net_pair_cost_max：`0.997325`

单窗口通过当前研究 gate：accepted/fills/rescues/PnL/residual/source/safety 全部达标。

## Bridge-Aware Repeat Evidence

当前 shadow review 不再依赖 legacy single-window / target-cap repeat scorer 直接 KEEP，而是使用 bridge-aware explicit-surplus evidence 解释 legacy blocker。

Bridge aggregate 使用：

- base：`xuan-frontier-soft-closeability-comparable-20260522T1705Z`
- fresh cap25：`xuan-frontier-soft-mainline-cap25-reproduction-20260525T2041Z`

聚合结果：

- bridge_eligible_window_count：`2`
- accepted_actions：`158`
- queue_supported_fills：`132`
- strict_rescue_closes：`26`
- pair_pnl：`+7.093115`
- ROI：`7.2316%`
- residual_qty_share：`18.4975%`
- residual_cost_share：`13.5589%`

这清掉了之前的 accepted gap、rescue gap、PnL/residual gate。approval gate 当前使用 `surplus_bridge` 作为 repeat metric source。

## 为什么 Legacy Scorer 仍显示 UNKNOWN

`runtime_shadow_readiness` 和 `repeat_window_scorer` 仍按旧的 strict single-window / target-cap 假设评分，因此会继续显示 UNKNOWN。

这不是当前研究结论的 blocker，原因是：

- shadow packet 已记录 `legacy_runtime_repeat_blockers_explained_by_bridge=true`
- bridge shadow gap 已清空 remaining gaps
- shadow promotion gate/gap 已 KEEP
- approval gate 已要求 `surplus_bridge` 存在；缺少 `surplus_bridge` 的 bridge packet 会 BLOCK

也就是说，底层 legacy scorer 没有被削弱；只是审批口径显式承认了 explicit-surplus bridge evidence。

## Public Benchmark Caveat

public benchmark comparison 当前状态是 `KEEP_PUBLIC_BENCHMARK_COMPARISON_BRIDGE_RESIDUAL_CAVEAT_RESEARCH_ONLY`。

原因：

- xuan 当前 pair cost / fee-aware edge 明显优于 b55 参考：
  - actual_pair_cost_after_fee：`0.897360`
  - b55_actual_pair_cost：`0.959218`
  - pair_cost_delta_vs_b55：`-0.061858`
- 但 xuan 的 residual_qty_share 仍高于 public hard target：
  - residual_qty_share_delta_vs_b55：`+0.109455`

因此这不是 deploy 通过项，只是 research shadow review caveat。当前 residual 在 xuan bridge gates 内可接受，但仍是 live/capacity 前必须继续压的风险。

## Residual / Capacity Caveats

approval gate 保留以下 caveats：

- `private_truth_not_ready`
- `projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence`
- `residual_cost_share_still_live_caveat`
- `residual_qty_share_above_public_hard_target`
- `residual_zero_stress_negative_size_capacity_needed_before_live`

当前 2041Z capital reuse / residual stress：

- residual_cost：`3.914275`
- residual_cost_to_pair_qty：`13.5747%`
- worst_case_pair_pnl_if_residual_zero：`-0.954643`
- max_window_gross_cash_need：`6.159042`
- capital_roi_on_max_window_gross_cash_need：`48.0534%`

这些支持 research review，但不支持 live deployment。

## Anti-Overfit Holdout

弱窗口仍作为 holdout 保留：

- 0510Z：`density_qualified_tradeable`
- 2041Z/cap25：`density_qualified_tradeable`
- 0621Z：`active_but_rescue_density_weak_abstain`

这意味着策略目标不是“所有 regime 都强行交易”，而是：在 rescue density/source/PnL/residual gates 清楚时交易，在 active but weak rescue-density regime 中 abstain。

## 请求批准的范围

请求批准进入：

- research shadow review
- paper-shadow review
- bounded no-order research review
- local scorecard / packet generation

明确不请求、不允许：

- live orders
- production deploy
- service restart
- shared ingress mutation
- collector rebuild/publish
- raw/replay mutation
- cron loop creation

## 建议下一步

如果批准 research shadow review，下一步应只做本地 review 包和 paper-shadow 审核口径收敛：

- 保持 `deployable=false`
- 保持 `orders_sent=false`
- 保持 source gates / strict rescue L1 / explicit surplus PnL floor
- 继续保留 0621Z weak-density holdout
- 在任何 live/shadow deployment 前另行审批
- 在任何新 remote 前先给出独立、明确、非重复的证据需求

当前不需要再跑 remote 来证明“是否清掉 shadow review blocker”；这个 blocker 已由 2041Z bridge-aware evidence 清掉。后续 remote 只应服务于新的问题，例如容量、长尾 residual、或不同时间段 repeat，而不是重复追逐已经清掉的 gate。
