# 统一新主线：Pair-Gated Tranche Arb

**摘要**

- 采用 `docs/plans/PLAN_PAIR_ARB_V3_5_ZH.md` 作为总治理框架，保留四层纪律、2 周 shadow、`FAK/SELL/abandon` 默认关闭、`pair_arb` 仅作 baseline。
- 采纳 `docs/plans/PLAN_PAIR_ARB_V3_5_EVAL_OPTIMIZATION_ZH.md` 的两个核心升级：`H-0 Pair-Gated Tranche Automaton` 和 `H-5 Repair-Budgeted Variable Pair Cost`，并把它们提升为新主线的行为核心。
- 采纳 `docs/research/xuan/XUANXUAN008_STRATEGY_DEEP_DIVE_ZH.md` 的主判断：`xuan` 的最可信 edge 不是固定 `pair_target`，而是“小 tranche 一对一推进 + surplus/repair 预算 + rolling merge`。
- 采纳 `docs/strategies/STRATEGY_PAIR_GATED_TRANCHE_V1_ZH.md` 作为实现骨架，但做两处收敛：`maker-first` 作为 V1 默认执行边界；`bounded taker completion` 只做显式预留，不进入默认 enforce。
- 不把 `H-2 two-sided seed`、`H-3 depth-aware sizing`、`H-4 abandon 放宽` 放进 V1 核心；它们继续是后续候选研究项，不阻塞 V1 主线。

**关键改动**

- 新增策略线 `StrategyKind::PairGatedTrancheArb`，保留 `pair_arb` 原语义与原回测脚本；`PM_STRATEGY=pair_gated_tranche_arb` 作为新主线入口。
- `InventoryManager` 升级为账本真相源，除现有 `InventorySnapshot` 外再维护 `MarketPairLedger`；协调器只消费快照，不在 `pair_arb` 式全局均价上自行推断 tranche。
- 新增核心类型：
  - `PairTranche { id, state, first_side, first_qty, first_vwap, hedge_qty, hedge_vwap, residual_qty, pairable_qty, pair_cost_tranche, pair_cost_fifo, gross_surplus, spendable_surplus, repair_spent, opened_at, closed_at }`；`pair_cost` 拆为 `pair_cost_tranche`（first leg + completion vwap）与 `pair_cost_fifo`（FIFO 近似）两口径并存，enforce 决策必须以 `pair_cost_tranche` 为准（来自 V1.1 §2.2）。
  - `TrancheState { FlatOrResidual, FirstLegPending, CompletionOnly, PairCovered, MergeQueued, Closed }`
  - `MarketPairLedger { active_tranche, residual_side, residual_qty, surplus_bank, repair_budget_available, same_side_add_events, recent_closed, locked_capital_ratio, mergeable_full_sets }`；`locked_capital_ratio` 与 `mergeable_full_sets` 在 V1 阶段仅作为 telemetry 暴露，不接入行为层；行为 gate 由 V1.1 §2.4 验收后再启用。
  - `EpisodeMetrics { clean_closed_episode_ratio, same_side_add_qty_ratio, residual_before_new_open_p90, episode_close_delay_p50, episode_close_delay_p90 }`
- `StrategyTickInput` 扩展为显式携带 `pair_ledger` 与 `episode_metrics`；旧策略可忽略新字段，新策略必须只根据账本状态驱动行为。
- 行为规则固定如下：
  - 仅在 `no_active_tranche && no_first_leg_pending && abs(global_residual_qty) <= 10 && tail_freeze=false` 时允许开新 first leg。
  - first leg 默认只挂一侧，不双边 seed；侧别选择规则固定为“取当前两侧中更便宜、且可执行的那个 candidate”，若两侧价格差小于 1 tick 则本 tick 不开新仓。
  - first leg 候选价格复用现有 `pair_arb` 的 base maker quote 链作为入场评分器，但它只决定 entry side/price，不再作为 completion 的硬约束。
  - 一旦 first leg 成交进入 working inventory，状态切到 `CompletionOnly`；此后同侧 `risk-increasing` 全部抑制，只允许 opposite leg。
  - completion 报价上限固定为 `1 - first_vwap - min_edge_per_pair + realized_repair_budget_per_share`；V1 enforce 中 `urgency_budget_per_share=0`，只在后续 `bounded_taker` shadow 中启用。
  - `realized_repair_budget_per_share` 只能来自本 market 已闭合 tranche 的 `spendable_surplus`，不得借用未实现利润，也不得跨 market 共用。
  - `gross_surplus = pairable_qty * max(0, 1 - pair_cost)`；`spendable_surplus = max(0, gross_surplus - pairable_qty * min_edge_per_pair)`；`repair_budget_available = spendable_surplus * repair_budget_fraction - repair_spent`。
  - opposite overshoot 必须拆分：满足当前 tranche 所需的部分先 закрыть 当前 tranche，超出的部分立刻滚成下一条反向 residual tranche；严禁直接并回全局均价。
  - `PairCovered != Merged`：pair-covered 只表示会计闭合；`MERGE` 只减少 `pairable_qty` 与释放资金，不能错误关闭未覆盖 tranche。
- `CadenceTracker` 保留在 `coordinator_metrics.rs`，但指标来源改为 `PairLedgerSnapshot` 与 fill stream；必须输出 `same_side_add_before_covered`、`residual_before_new_open`、`episode_close_delay`、`pair_cost budget`。
- 研究层固定为：
  - 把 `BS-1` 拆成 `BS-1a 我方 execution truth` 与 `BS-1b xuan trader_side truth`。
  - `BS-1a` 立即采用已验证的 authenticated `/data/trades`；`BS-1b` 不再阻塞 V1。
  - 强制新增研究对象 `xuan_pair_episode_summary`、`xuan_pair_cost_budget_summary`，与原有三对象并存。
  - `H-0` 通过条件固定为 `clean_closed_episode_ratio >= 90%`、`same_side_add_qty_ratio <= 10%`、`episode_close_delay_p90 <= 60s`；实盘 enforce 前另需通过 V1.1 §2.1 升级 H-0（更大样本 + 市场分桶 + 分时分桶；数据不可获取时降级为我方 shadow ≥4 周同口径达标），仅阻塞 enforce、不阻塞 skeleton 与 shadow tooling。
  - `H-5` 通过条件固定为 `discount_pair_surplus / abs(repair_pair_loss) >= 1.25`、`cohort_net_pair_pnl > 0`、`repair_loss` 全程受 `realized spendable surplus` 约束。
- 回测与录制路径固定为：
  - `recorder` 新增 `tranche_opened`、`tranche_completed`、`same_side_add_before_covered`、`surplus_bank_updated`、`repair_budget_spent`、`merge_pairable_reduced` 事件。
  - `scripts/build_replay_db.py` 新增 tranche/budget 表，不改旧表语义。
  - 新建独立回测器 `backtest_pair_gated_tranche.py`，不要把 V1 逻辑塞进现有 `backtest_pair_arb.py`。
- 发布矩阵固定为：
  - 立即做：`A0 TrancheLedger`、`B0 Episode/Cadence metrics`、`D0 Harvester observer`、`PairGatedTrancheArb` skeleton、replay/backtest 扩展。
  - shadow 默认边界：`maker-only`、`single-leg open`、`completion-only`、`repair budget enforce in simulation`、`bounded taker` 仅统计不下单。
  - enforce 前提：连续 2 周 shadow、`clean_closed_episode_ratio >= 90%`、`same_side_add_qty_ratio <= 10%`、`cohort_net_pair_pnl > baseline pair_arb`、`repair_loss` 未超过预算、`tail residual` 不恶化；另需满足 V1.1 enforce 前置：`abs(pair_cost_tranche_p50 - pair_cost_fifo_p50) < 0.05`（V1.1 §2.2）、`locked_capital_ratio_p90 < 0.6`（V1.1 §2.4）、尾段 `t/T <= 0.2` PnL 不显著为负（V1.1 §2.5）；按市场分级 enforce（V1.1 §2.1），不要求全 7 市场齐过线。
  - `bounded taker completion` 的接口预留为 `CompletionMode::{MakerOnly, BoundedTakerShadow, BoundedTakerEnforce}`；只有 `BS-1a` 完整、maker-only shadow 达标后，才允许进入 `BoundedTakerShadow`。

**测试计划**

- 单测必须覆盖：`U100->D100`、`U100->D95(eps=10)`、`U100->D80->U50`、`U100->D130 overshoot`、`merge 只减少 pairable 不关闭 active tranche`。
- 单测必须覆盖 budget：cheap pair 记入 `spendable_surplus`、expensive completion 正确消耗 `repair_budget_available`、预算不足时 completion quote 被阻断。
- 集成测试必须覆盖：`pair_arb` 与新策略并存；新策略下 first leg 只允许一侧；进入 `CompletionOnly` 后同侧意图为 0；tail 窗口不再开新 tranche。
- replay/backtest 验收必须覆盖：从 recorder 事件重建 episode open/close、same-side add、overshoot rollover、surplus/repair 账本；`backtest_pair_gated_tranche.py` 输出与 replay 指标口径一致。
- shadow 验收必须输出：`why new open allowed/blocked`、`why completion allowed/blocked`、`budget before/after`、`projected pair cost`、`merge decision`。

**假设与默认值**

- 初始交易域固定为 `BTC 5m`，先不扩到通用市场；session 过滤沿用现有 liquidity/spread gate，不新增方向 alpha 模块。
- V1 默认参数固定为 shadow defaults：`residual_eps=10`、`residual_eps_observe=25`、`base_clip=120`、`max_clip=250`、`max_same_side_run=1`、`episode_timeout_secs=75`、`tail_completion_only_secs=60`、`tail_freeze_secs=30`、`harvest_min_full_set=10`、`harvest_min_interval_secs=20`、`repair_budget_fraction=0.50`、`min_edge_per_pair=0.005`、`max_repair_pair_cost=1.04`。
- `H-2/H-3/H-4` 明确延后；它们只能在 V1 稳定后作为 V2 候选增强，不得反向污染 V1 主线。
- `pair_arb` 继续作为 baseline、回退策略和比较对象；新主线不缝回旧 `pair_arb`。
