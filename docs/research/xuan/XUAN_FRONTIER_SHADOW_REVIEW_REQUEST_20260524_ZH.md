# Xuan-Frontier Shadow Review Request - 2026-05-24

## 结论

xuan-frontier 当前应推进到 shadow review / 审批口径。这个结论只代表研究侧 no-order runtime evidence 已达 review 标准，不代表 deployable，也不代表允许实盘发单。

## 当前状态

- replay 状态：`SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY`
- runtime packet 状态：`KEEP_SHADOW_REVIEW_PACKET_READY_RESEARCH_ONLY`
- repeat-window 状态：`KEEP_REPEAT_WINDOW_SHADOW_REVIEW_SCORER_PASS_RESEARCH_ONLY_READY_FOR_REVIEW`
- approval gate 状态：`KEEP_SHADOW_REVIEW_APPROVAL_GATE_PASS_RESEARCH_ONLY`
- source-linkage：runtime shared-ingress 顶层 source ids 已确认可读
- safety：`PM_DRY_RUN=1`，orders_sent=false，无 deploy/restart/shared-service mutation
- deployable：false

## Fee 与 ROI 口径

本轮 ROI 已考虑 strict-rescue 补腿 taker fee。runner 在 strict rescue close 时计算：

- `fee_per_share = fee_per_share(close_ask, taker_fee_rate)`
- `net_pair_cost = held_px + close_ask + fee_per_share`
- `pair_pnl_delta = qty * (1 - net_pair_cost)`

因此 strict-rescue 的 `pair_pnl` 已经是扣补腿 fee 后的净值。三段 30m clean runtime windows 合计：

- taker_fee：`0.512064`
- completion_cost：`14.920500`
- pair_pnl：`+1.794311`
- pair_qty / redeem notional：`43.575000`
- pair_cost：`41.780689`
- edge_on_redeem_notional：`4.1178%`
- ROI on pair cost：`4.2946%`
- ROI on total cash spend, including completion cost and taker fee：`4.0736%`
- legacy ROI on filled seed cost：`6.2706%`

`legacy ROI on filled seed cost` 是历史流水口径，不应用来和外部账户每轮 ROI 直接比较。更接近外部“每轮 2-3%”的口径是 `edge_on_redeem_notional` 或 `ROI on pair cost`。

## Merge / Redeem 资金复用

capital reuse scorer 已按 FIFO 重建资金占用：

- internal pair 会释放 YES/NO 两腿 lot cost
- strict rescue 会释放 held lot cost
- strict rescue close cash 计入峰值 gross cash need

当前三段样本：

- max_window_gross_cash_need：`2.478813`
- filled cost turnover on max open：`11.5436x`
- capital ROI on max window gross cash need：`72.3859%`

这个高 capital ROI 来自 5m 场次内的快速 merge/redeem 资金复用；它不能被解释成已验证的大额容量收益。

## 300 USD 每轮假设

如果每个 5m 场次都能打满 `300 USD` redeem notional，并且 edge 不衰减：

- 预期利润 / 轮：`$12.353260`
- 理论 288 轮 / 天：`$3,557.738850`

这是线性容量假设，不是已验证的 300 USD runtime capacity。当前实际 no-order runtime 只是小规模样本。

## 风险

- residual_cost：`2.266313`
- residual_cost_to_pair_qty：`5.2009%`
- residual_cost_to_pair_pnl：`1.2631x`
- worst_case_pair_pnl_if_residual_zero：`-0.472002`

主要未证实项不是 fee 或 ROI 口径，而是更大 size 下的容量、queue/fill 可达性、residual tail 是否仍受控。

## 审批请求

请求批准进入 shadow review。建议审批边界：

- 仅 shadow / paper-shadow / no-order review，不允许 live orders
- 保持 source gates、strict-rescue source audit、surplus-backed rescue floor
- 继续输出 normalized lifecycle、capital reuse ROI、repeat-window scorer、shadow packet
- 若进入任何 live / deploy / restart / shared-service mutation，必须另行审批

## Approval Gate

本地 approval gate 已把 shadow review 与 live/deploy 边界分开：

- shadow_review_approval_ready：`true`
- paper_shadow_only：`true`
- deployable：`false`
- live_orders_allowed：`false`
- hard_blockers：`none`
- caveats：`private_truth_not_ready`、`projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence`、`residual_cost_share_still_live_caveat`、`residual_zero_stress_negative_size_capacity_needed_before_live`

允许范围：

- shadow_review
- paper_shadow_review
- bounded_no_order_research_review
- local_scorecard_generation

明确不允许：

- live_orders
- production_deploy
- service_restart
- shared_ingress_mutation
- collector_rebuild_or_publish
- raw_replay_mutation

## Next: Capacity Ladder

shadow review 之后的下一个研究问题是容量，而不是 fee 口径。当前已经生成本地容量阶梯计划：

- capacity ladder 状态：`KEEP_CAPACITY_LADDER_PLAN_READY_LOCAL_ONLY`
- next stage：`cap_25`
- cap_25 target_qty / round redeem notional：`25`
- cap_25 max_open_cost：`30`
- cap_25 max_seed_qty：`75`
- cap_25 imbalance_qty_cap：`6.25`
- cap_25 surplus_budget_max_abs_unpaired_cost：`10`
- cap_25 remote manifest：`THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY`
- cap_25 manifest verifier：`KEEP_THIRD_WINDOW_MANIFEST_VERIFIER_PASS_LOCAL_ONLY`

cap_25 通过后才允许进入 `75 -> 150 -> 300` 的容量阶梯。每一级都必须保持：

- `PM_DRY_RUN=1`
- orders_sent=false
- fee-aware positive PnL
- edge_on_redeem_notional_after_fee >= `2%`
- roi_on_total_cash_spend_after_fee >= `1.5%`
- residual_cost_share <= `15%`
- residual_qty_share <= `20%`
- residual_cost_to_pair_qty <= `5%`
- source blocks = `0`
- no deploy/restart/shared mutation

这仍然只是 bounded no-order research review，不是 live capacity approval。
