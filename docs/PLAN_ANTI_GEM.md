# Pair-Gated Tranche V1.2 优化分析与重构实现计划

## Summary
基于 `docs/XUANXUAN008_STRATEGY_V2_ZH.md` 的新证据，将策略重心从“是否能一对一配对”升级为“高 merged_ratio、低有效残余、临近结束批量 MERGE、补腿 clip 自适应”的完整复刻。实施目标是让当前 `PairGatedTrancheArb` 从概念状态机变成可 shadow 验收的策略执行框架，同时保留 maker/taker、PnL 口径等未裁决项为研究任务。

## Key Changes
- **文档重构**
  - 新增或更新一份 V1.2 设计文档，吸收 V2 结论：positions 终态证据、loser residual 解释、MERGE/REDEEM 节律、clip size 分布、session 节律、PnL mismatch。
  - 将证据等级明确为：pair-gated/merged_ratio/MERGE 主路径为 A；maker/taker 与真实 PnL 为 C；depth-aware clip 仅标为待裁决，不写成已证实。
  - 把旧策略假设“保留 winner residual”彻底移除，改为“winner redeem、loser dust retained”。

- **策略状态机重构**
  - 在 [pair_gated_tranche.rs](D:/web3work/pm_as_ofi/src/polymarket/strategy/pair_gated_tranche.rs:1) 中保留核心状态：`Flat/Residual -> ActiveFirstLeg -> CompletionOnly -> PairCovered -> MergeQueued -> Closed`。
  - 未完成 active tranche 时，默认禁止继续同侧 risk-increasing 开仓；`MAX=2` 只作为 shadow counter，不进入 live 行为。
  - CompletionOnly 不再一次性补完整 residual，而是按 adaptive clip 输出补腿 intent。

- **Adaptive Clip Model**
  - 新增 clip sizing 逻辑，默认 shadow 参数：
    - `base_clip_qty = 120`
    - `max_clip_qty = 250`
    - `min_clip_qty = 25`
    - imbalance 越高，completion clip 越大。
    - intra-round trade index 越靠后，first-leg clip 递减。
    - `remaining_secs <= 60` 进入 tail completion 升档。
  - live 初始行为采用保守上限：clip 不超过 residual、不超过盘口可接受量、不超过 repair-budgeted ceiling。

- **Repair-Budgeted Variable Pair Cost**
  - 补腿价格不使用固定 `UP + DOWN` target。
  - 统一使用：`first_leg_vwap + completion_price <= 1 - min_edge + repair_budget_per_share`。
  - `urgency_budget_shadow_5m` 先从 shadow 变量升级为报价输入，但只允许提高 completion ceiling，不允许绕过 hard loss cap。

- **PairHarvester 升级为 P0**
  - 新增 `PairHarvester` 行为：当 `seconds_to_market_end <= 25` 且 `pairable_qty >= 10` 时发 MERGE intent。
  - MERGE 优先级高于新开 first leg，也高于非紧急 completion。
  - REDEEM 作为 settlement 后 30–60 秒兜底路径，不作为主要资金回收路径。

- **Ledger / Telemetry**
  - 确保 `pair_tranche_events`、`pair_budget_events`、`capital_state_events` 在 shadow 运行中稳定产出。
  - [pair_ledger.rs](D:/web3work/pm_as_ofi/src/polymarket/pair_ledger.rs:1) 已有 `pair_cost_tranche` / `pair_cost_fifo_ref`，实现时只补齐事件落库和策略侧消费。
  - CapitalState 从“只计算”升级为“能触发 merge pressure shadow”，live 初期只记录，不主动扩大风险。

- **Research Tasks**
  - 新增 maker/taker 离线推断脚本：用本地 recorder 的 `md_book_l1` 对齐 xuan trade timestamp，比较成交价与当时 best bid/ask。
  - 新增 cash-flow PnL reconciliation：用 BUY、MERGE、REDEEM、residual valuation 重建真实收益，暂不信任单一 FIFO 或 positions realizedPnl。

## Test Plan
- **单元测试**
  - active tranche 未补齐时，同侧新开被阻止。
  - completion clip 不超过 residual、max clip、repair-budgeted ceiling。
  - tail 阶段允许 completion 升档，但不允许突破 hard loss cap。
  - `pairable_qty >= 10` 且 `seconds_to_market_end <= 25` 时生成 MERGE intent。
  - MERGE 后 pairable 减少，residual 与 recent closed 状态正确更新。

- **回放 / Shadow 验收**
  - shadow 至少跑 3 天，确认三类事件表非零且连续。
  - episode 指标达到：`clean_closed / closed >= 90%`，`same_side_add_qty_ratio <= 10%`，completion delay p90 <= 100s。
  - positions 风格指标达到：merged_ratio p50 >= 95%，loser residual 为主要尾迹。
  - MERGE 节律接近 V2 发现：主要集中在 close 前 20–30 秒，MERGE 回收金额显著高于 REDEEM。
  - PnL 验收只用 cash-flow ledger，不用未裁决的 FIFO 估算直接判断盈利。

## Assumptions
- 该计划命名为 `Pair-Gated Tranche Arb V1.2`，是 V1/V1.1 的吸收升级，不直接覆盖原始 V3.5 主计划。
- 初始实现以 shadow-first 为默认，live 行为只启用状态机硬约束、保守 completion、MERGE intent；adaptive clip 和 capital pressure 先记录再逐步启用。
- maker/taker 仍未裁决；策略默认 maker-first，但不把 xuan 的 maker 比例写成事实。
- `depth-aware` 暂不作为已证实设计目标，直到盘口深度与成交价格对齐分析完成。
