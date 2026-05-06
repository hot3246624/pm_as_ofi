 @xuanxuan008 策略深度解析 + Pair_Arb 升级计划 (v2)

 ▎ v2 重构驱动：审稿人指出 v1 以 dynamic pair_target 为核心是错位优化。xuan 的真正 edge 是 completion
 ▎ hazard——"对侧腿在 τ 内完成的概率 Pr(F(t+τ) | C(t), state)"。v2 围绕 hazard 建模重构，并修复三处实质错误：
 ▎ 1. P0 KPI 用 settled_changed 会被 pending→settled 15s timeout
 ▎ 污染（coordinator_metrics.rs:122、inventory.rs:244/256/284）
 ▎ 2. P2 的 abandon-sell 在当前聚合 VWAP 库存上不安全——FillRecord 无 cycle_id、dispatch_taker_derisk 从 inv.yes_qty
 ▎ 聚合池卖（inventory.rs:377、coordinator_order_io.rs:1268、messages.rs:293）
 ▎ 3. P4 误复用 capital recycler 的 low-balance 触发——polymarket_v2.rs:1340 if status.free_balance >=
 ▎ cfg.low_water_usdc return 语义与 harvest 错位

 变更日志（vs v1）

 ┌────────────────────┬──────────────────────────────────┬──────────────────────────────────────────────┐
 │        维度        │                v1                │                      v2                      │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ 核心概念           │ dynamic pair_target（定价 edge） │ completion hazard（时间 edge）               │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ 首阶段             │ P0 观测（settled）               │ Phase A cycle/tranche identity（前置）       │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ 观测 KPI           │ settled_changed                  │ working_changed + order ack 时刻             │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ v1 P3 → v2 Phase C │ "pick cheapest side first"       │ "regime classifier + two-sided passive seed" │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ v1 P4 → v2 Phase D │ 复用 try_recycle_merge           │ 独立 PairArbHarvester trigger                │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ v1 P1 → v2 Phase F │ 第 1 实质阶段                    │ 最末可选阶段（派生变量）                     │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ v1 P2 → v2 Phase E │ 独立 FAK escalation              │ 必须 Phase A 完成后才能做                    │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ 证据标注           │ 混合                             │ ★ 分级 + 明确"单点 vs 时间均值"              │
 ├────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────┤
 │ Recorder           │ 规划中                           │ 已在 prod，v2 提议做 extension               │
 └────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────┘

 ---
 PART 1 · @xuanxuan008 策略深度解析（v2）

 1.0 框架转换：completion hazard 为核心

 1.0.1 数学定义

 设 pair cycle 是一次"开首腿 → 对侧腿也填上 → 两侧都齐"的完整流程：
 - C(t) = 指示器"首腿在 t 时刻开仓"
 - F(t+τ) = 指示器"对侧腿在 t+τ 之前被填"
 - hazard h(τ | state) = P(F(t+τ) | C(t), state)

 xuan 的策略可抽象为一个状态决策器：
 π: state → {open, wait}
 π(s) = open  ⟺  E[edge × I(T_complete ≤ τ*) | s] − cost(s) > 0
 其中 τ* 是他可接受的最长单腿时间（经验值 ≈ 30s）。

 1.0.2 为什么 hazard 是主变量、pair_target 是派生

 单 cycle 期望 PnL 的正确分解：
 E[PnL | cycle opens] = edge(pair_target) × P(T ≤ τ*)
                      − single_leg_drawdown × E[T × I(T > τ*)]
 - hazard 高时（h(τ*) → 1）：单腿成本几乎可忽略，策略由 edge 主导 → target 可以紧贴市场
 - hazard 低时（h(τ*) → 0）：即使 edge 再大，E[T × I(T > τ*)] 吞掉一切 → 任何 target 都无法救

 结论：在 hazard 异质的市场中，regime filter 的 ROI >> target 优化的 ROI。v1 把 dynamic pair_target 放首位是在 hazard
  分布不受控的前提下优化第二阶变量。

 1.0.3 Completion 的互补结构 ★

 Polymarket 二元期权有结构性约束 YES + NO = $1（via split/merge, gasless）。这意味着两侧 flow 经套利者传递相关：
 - 若 YES 侧有大 taker 买盘推高 YES ask，套利者会 split 新铸 → 同时卖 YES 和 NO → NO 侧也出现 sell flow
 - 因此 "opposite fill" 的到达不只取决于单侧 book 深度，而依赖总市场活跃度
 - 这是 xuan 可能识别的核心微结构 edge：在总 turnover 高但单侧 book 看起来 thin 的时段仍然参与

 1.0.4 派生变量，不是决策变量

 ┌──────────────────────┬──────────────────────────────────────────────────────┐
 │       传统视角       │                     hazard 视角                      │
 ├──────────────────────┼──────────────────────────────────────────────────────┤
 │ pair_target 是主决策 │ pair_target 是 edge 约束之一，进 regime feature      │
 ├──────────────────────┼──────────────────────────────────────────────────────┤
 │ FAK 补腿是主执行路径 │ FAK 是异常兜底（只在误入 low-hazard regime 时触发）  │
 ├──────────────────────┼──────────────────────────────────────────────────────┤
 │ merge 是周期性动作   │ merge 是 "tranche harvest"——在 regime 恶化时主动变现 │
 ├──────────────────────┼──────────────────────────────────────────────────────┤
 │ 单腿时间越短越好     │ 单腿时间由 regime 决定；允许高 hazard 下短暂不对称   │
 └──────────────────────┴──────────────────────────────────────────────────────┘

 ---
 1.1 可观测事实 + 诚实证据标注

 ★ 分级：★★★ 直接证据 / ★★ 强推断 / ★ 合理假设 / ☆ 未定论

 诚实坦白：本节所有定量均来自 2 次 profile
 快照（T0、T1，间隔数小时）。任何"百分比"都是单点值，不代表时间均值，也不能当"已证明"的策略指纹。

 1.1.1 已证实事实

 ┌──────────────────────────────────────┬────────────────────────────────────────────────────┬───────────────────┐
 │                 事实                 │                        证据                        │       等级        │
 ├──────────────────────────────────────┼────────────────────────────────────────────────────┼───────────────────┤
 │ 在做 pair_arb                        │ T1 同市场持仓 482.6 Up + 367.9 Down（BTC Apr24     │ ★★★               │
 │                                      │ 5:40-5:45）                                        │                   │
 ├──────────────────────────────────────┼────────────────────────────────────────────────────┼───────────────────┤
 │ 高 velocity 盈利                     │ 4,260 笔 / 30 天 ≈ $1,460/日                       │ ★★★               │
 ├──────────────────────────────────────┼────────────────────────────────────────────────────┼───────────────────┤
 │ T1 快照净单腿 114.7 Up（24% 不对称） │ 直接计算                                           │ ★★★（但只是单点） │
 ├──────────────────────────────────────┼────────────────────────────────────────────────────┼───────────────────┤
 │ Merge 在 Polymarket 是 gasless +     │ 官方文档                                           │ ★★★               │
 │ 零手续费                             │                                                    │                   │
 └──────────────────────────────────────┴────────────────────────────────────────────────────┴───────────────────┘

 1.1.2 推断（必须实验验证）

 ┌────────────────────────────┬──────────────────────────┬─────────────────────────┬──────┐
 │            推断            │         支持证据         │         反证据          │ 等级 │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 持仓长期 ~76% 成对         │ T1 单点                  │ 无时间序列              │ ★    │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 入场以 maker 为主          │ 76% 成对比 FAK 应为 100% │ 无 order lifecycle 数据 │ ★    │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 5-10 市场并发              │ 140 笔/日 反推           │ 无 market 维度拆解      │ ☆    │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 并行跑 T-minus/oracle 狙击 │ PnL 数量级推算           │ 账户只有一个策略也可能  │ ☆    │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 不固定 pair_target         │ 用户观察                 │ 我们无直接交易价数据    │ ★    │
 ├────────────────────────────┼──────────────────────────┼─────────────────────────┼──────┤
 │ 30s 配对 p50/p95           │ 用户观察                 │ 无分布数据              │ ★    │
 └────────────────────────────┴──────────────────────────┴─────────────────────────┴──────┘

 1.1.3 数据缺口（阻塞定量主张）

 - 无 trade-level timestamp 流（Profile 是 SPA，WebFetch 只拿聚合）
 - 无钱包地址 → 无法链上回放 split/merge/swap
 - 无 order lifecycle（maker/taker/split 无法区分）
 - 无历史 PnL 时间序列

 推论：下游任何 "xuan 贡献比例 60% vs 40%"、"他 target 公式是 f(...)" 等，在拿到 trade stream
 或链上数据前都是假设讨论，不是可计算真值。

 ---
 1.2 Polymarket BTC 5-min 微结构（含互补结构）

 已文档确认：
 - YES + NO = $1（结算不变式）
 - Split / Merge gasless + 零协议费（relayer 赞助 CTF）
 - 典型 spread 2-4c / 侧；book 深度浅
 - T-10s 时约 85% 方向已由 BTC spot 确定
 - 500ms taker delay 已取消 → 纯跨所延迟 arb 已死

 对 hazard 的含义：
 - Completion 流向呈结构性相关（1.0.3）
 - Hazard feature 必须包含 "total market turnover" 而不只是 "opposite-side depth"
 - merge gasless ≠ "每成 1 对就 merge"（xuan T1 快照反例：持 367 对未 merge）——harvest
 有更聪明的触发（conjecture：hazard 恶化或 regime 切换时触发）

 ---
 1.3 候选假设（hazard-centric）

 每个假设回答三个问题：
 1. hazard 由什么驱动？
 2. xuan 如何挑 regime？
 3. 可被哪个实验证伪？

 H1 ★★ Regime-gated passive two-sided maker（主假设）

 - hazard 驱动：对侧 book 深度、总 turnover、OFI 非毒性、TTL 适中
 - regime 选择：h(30s | regime) ≥ h_threshold 才挂双边 seed
 - 证据：76% 成对率 + 未立即 merge 累积库存
 - 证伪指纹：fill 集中在 our bid 侧价位，到达时间与 regime features 相关

 H2 ★ 偶发 FAK sweep（辅助）

 - hazard ≈ 1（立即）
 - regime：ask_YES + ask_NO < target − 3*tick 且两侧深度 ≥ X
 - 证伪指纹：fill pair 在 <1s 间隔，price = ask side

 H3 ☆ Split-based entry

 - hazard ≈ split tx confirm 时间（秒级）
 - 证伪指纹：链上 split tx 频繁，order fill 反而少

 H4 ★ 总活跃度 regime filter（互补结构）★重要新增★

 - 利用 1.0.3 的互补结构
 - hazard ∝ total market turnover（跨 YES/NO 的总 fill rate），不只是某一侧 book
 - 证伪指纹：xuan 在 "两侧 book 都看着 thin 但总 trade volume 高" 的时段集中活跃

 假设组合的诚实版本

 - 单点快照无法定贡献比例。v1 曾写 "H1 60-70% / H4 20-30%" 是错误的定论表述。
 - 正确的说法：所有假设都与观察不矛盾，验证它们的唯一方式是 Phase B 的 hazard 经验分布 + Phase G 的 xuan trade
 stream。

 ---
 1.4 "不固定 pair_target" 的新解释

 - 不是 他用了更花哨的 target 公式
 - 是 他根本不把 target 当主决策变量
 - target 只是 regime classifier 的一个 feature（"当前盘口能提供多少 edge"）
 - 在 high-hazard regime 他可以容忍 target 紧贴市场；low-hazard regime 他直接不开仓
 - 所以从外部观察，他的有效 target 看起来浮动，但其实是 "不同 regime 下参不参与" 的二值决策

 ---
 1.5 "30s 配对" 的新解释

 - 不是 SLA（30s 内必须完成 pair 否则救腿）
 - 是 他只在 h(30s | regime) 高的 regime 下开首腿
 - 经验 completion 时间自然在 30s 内（因为 regime 已被筛选）
 - 偶尔 regime 估计失误 → 容忍有界单腿 →  不会 为救腿支付 FAK 成本（因为 FAK 成本 > edge）
 - 这解释了为什么他"几乎无单腿风险 + 24% 单腿残留"同时成立——24% 是统计噪声和 regime 漂移的自然残余，不是策略失败

 ---
 1.6 PnL 压力测试（加 caveat）

 算术一致性（数量级，不是时间均值）：
 - $43,813 / 30d ≈ $1,460/日 ≈ $61/h
 - 每小时 6 笔 prediction × 假设 60% 是盈利 pair × 300 股 × 3c edge ≈ $32/h（偏低）
 - 或：5-10 市场并发 × 10-20 对/小时 × 3c × 200 股 ≈ $60-240/h（匹配范围）

 诚实 caveat：
 - 上一版 v1 "H1 60-70% / H4 20-30%" 的拆分不成立，只是可行性论证
 - 数量级说"pair_arb 能解释全部 PnL" 但"pair_arb 一定占 60%+" 是过度推断
 - 不能排除 H4（总活跃度）或并行 T-minus 线贡献主要 PnL

 ---
 1.7 反向工程路径（recorder-centric）

 已具备（无须新建）：
 - recorder.rs + 主程序 polymarket_v2.rs:7031 接入
 - user_ws.rs:146 ingest 层 hook
 - executor.rs:184 order lifecycle 事件
 - inventory.rs:93 InventoryManager 持有 RecorderHandle
 - → fill、book update、order accepted/cancelled/rejected、inventory transitions 都已写入事件流

 缺的是：
 1. cycle_id labeling（Phase A 副产品）
 2. replay 工具（从 recorder event 重建 pair cycle + regime features + hazard distribution）
 3. Regime feature extraction（offline 或 online）
 4. Hazard estimator（离线 Python 或 Rust；输入 replay, 输出 h(τ | regime)）

 要获取 xuan 的数据（若能做就做 Phase G）：
 - 尝试 https://clob.polymarket.com/trades 或 data-api 以不同参数枚举公开 trade stream，反搜 maker address
 - 若能拿到地址：Polygonscan 看 split/merge/swap 和 CTF token transfer，精确反演 entry/exit paths
 - 1-2 周的 xuan trade stream + 同期 book snapshot 可校准我方 hazard 模型

 ---
 1.8 对升级方案的核心修正（整合所有评审点）

 1. Phase A 前置：cycle/tranche identity（否则 Phase E abandon-sell 不安全、Phase B 测不准）
 2. P0 KPI fix：working_changed 替代 settled_changed；t0 = order ack time
 3. Phase C 重写：should_open_two_sided_seed(regime) → Option<(yes_bid, no_bid)>，不是 pick-cheap-side
 4. Phase D 独立触发：不复用 recycler（语义错位）
 5. Phase F 降级：dynamic pair_target 是派生变量，最后做或不做
 6. Phase E 前置依赖 A：cycle_id 是 abandon-sell 安全性的必要条件
 7. Recorder 不需新建：已 live，只需 extension
 8. 证据诚实：所有定量标 ★ 等级；拒绝单点快照当时间均值用

 ---
 1.9 未定（必须实验回答）

 1. hazard 是否真的是 regime 的函数 还是 i.i.d. 噪声？Phase B 48h 数据后能定
 2. 互补结构（H4）是 xuan edge 来源吗 ？需要 total turnover 特征 + Phase B 条件分布对比
 3. xuan 是否并行跑 T-minus？需 Phase G 数据（或我方观察他在 T-10s 的 activity）
 4. regime feature 集合 最小必要集？（book depth、turnover、OFI、TTL、BTC spot drift、pair_sum edge）

 ---
 PART 2 · 升级方案（hazard-centric, 已重排）

 依赖图

 Phase A (cycle id, prereq) ─┬─ Phase B (hazard measure, KPI-correct)
                             │     └─ Phase C (regime gate)
                             │           └─ Phase F (dynamic target, optional)
                             │
                             └─ Phase E (completion push, needs A)

 Phase D (harvest merge, 独立)

 Phase G (xuan data acquisition, 并行外部研究)

 Phase A — Cycle / Tranche Identity（Prereq）

 Goal：为 inventory 的每笔 fill 打上 cycle_id，使后续 hazard 测量与 abandon-sell 安全。

 Why now（v1 忽略的阻塞）：
 - FillRecord（inventory.rs:377）是纯 VWAP aggregate
 - InventoryState（messages.rs:293）只有 yes_qty / no_qty / yes_avg_cost / no_avg_cost / net_diff /
 portfolio_cost，无 cycle 或 tranche 维度
 - dispatch_taker_derisk（coordinator_order_io.rs:1268）从 inv.yes_qty.max(0.0) 聚合池卖 → 会误卖历史 cycle 库存

 Cycle 定义：
 - Open 时刻：net_diff 从 0 → 非零 transition
 - Close 条件（任一）：
   - net_diff 自然回到 0（完整配对）
   - merge 吃掉该 cycle 的 paired portion
   - abandon-sell 平掉剩余 leg

 改动：
 - inventory.rs FillRecord 新增 cycle_id: Option<u64>
 - inventory.rs 新增 current_open_cycle_id: Option<u64> 与 cycle_counter: u64
 - inventory.rs apply_fill 在 matching 到 pending 或 direct confirm 时推断 cycle_id：
   - 首腿（net_diff 从 0 转出）：cycle_counter += 1，assign 当前 cycle_id
   - 补腿：沿用 open cycle_id
   - 对侧 sell：从该 cycle_id 的 tranche 中扣减（FIFO within cycle）
 - inventory.rs 新增可选的 per_cycle_ledger: HashMap<u64, CyclePosition>（仅用于 abandon-sell 的准确 tranche 定位；主
  VWAP 不动）
 - coordinator.rs 订阅 cycle open/close events，维护 active_cycle_features: HashMap<u64, RegimeSnapshot>
 - recorder.rs emit cycle_opened / cycle_closed 事件（用于 replay + hazard estimation）

 Config：无，永久开启

 Risk: medium。库存层改动，但 cycle_id: Option<u64> backward-compat，不影响其他策略路径。

 Verification：
 - 单测：并发开/关 cycle、rollback、timeout-promotion 不丢 cycle_id
 - replay：把 recorder event 回放，cycle 数 == paired_qty / 典型 size

 Phase B — Completion Hazard Measurement（Fixed KPI）

 Goal：按 regime bucket 测出 completion_time = t_opposite_fill − t_first_leg_ack 的经验分布。

 修正 v1 KPI bug（审稿人[P1]）：
 - v1 用 observe_pair_arb_inventory_transition 的 settled_changed 做 completion trigger
 - 但 fill 路径是 Matched → working (pending) → (Confirmed or 15s timeout-promote) →
 settled（inventory.rs:244/256/284/470）
 - → 30s completion 测量被 pending-promotion 延迟污染
 - v2 修正：trigger 改用 working_changed；t0 改用 order acceptance 时刻（ExecutionFeedback::OrderAccepted），不是
 settled 时刻

 正确公式：
 t0 = first_leg_order_accepted_at  (from OrderManager feedback)
 t1 = working_opposite_qty_became_positive  (from working inventory watch)
 completion_time = t1 - t0

 Regime buckets（起步特征；Phase C 会复用）：
 - book_depth_tier ∈ {thin, mid, deep}：两侧 top-5-level USDC 的 min
 - turnover_tier ∈ {low, mid, high}：过去 10s fill 数
 - ttl_tier ∈ {early, mid, late}：距 resolve 分 3 段
 - ofi_tier ∈ {benign, hot, toxic}：来自 OFI snapshot

 改动：
 - coordinator_metrics.rs 新增 HazardEstimator：
   - record_first_leg_ack(cycle_id, t, regime)
   - record_opposite_working_fill(cycle_id, t) — 只接受 working 变化
   - emit p50/p95 per bucket
 - coordinator.rs 在 ExecutionFeedback::OrderAccepted 分支触发 record_first_leg_ack
 - coordinator.rs watch working snapshot（非 settled），transition 时触发 record_opposite_working_fill
 - 📊 StrategyMetrics 日志新增 p50/p95 per bucket
 - 全量写入 recorder（Phase G 若落地可用外部 xuan 数据校准）

 Config：无，永久开启（pair_arb 下）

 Risk: low（纯观测）

 Dependency：A 优先但不硬阻塞（无 cycle_id 时 record_opposite_working_fill 用最近 pending first-leg 匹配，精度略降）

 Phase C — Regime Classifier + Two-Sided Passive Seed（v1 P3 重写）

 Goal：在 flat 状态决定"当前 regime 是否值得挂双边 passive seed"，以及建议的两侧价位。不是 "先挑哪侧冲"。

 v1 错误：should_open_first_leg(...) → Option<Side> + argmin(ask_price) 仍是单边 first-leg 视角。xuan 更像"识别适合
 two-sided maker 的 book geometry"。

 v2 API：
 fn should_open_two_sided_seed(
     coord: &StrategyCoordinator,
     inv: &InventoryState,
     ub: &UnifiedBook,
     ofi: &OfiSnapshot,
     regime: &RegimeFeatures,
 ) -> Option<TwoSidedSeed>;

 struct TwoSidedSeed { yes_bid_px: f64, no_bid_px: f64, expected_hazard: f64 }

 Gate 逻辑（概念）：
 h_est = HazardEstimator::estimate(regime)
 edge = effective_target − (mid_yes + mid_no)

 gate = h_est ≥ h_threshold
     AND edge ≥ min_edge_ticks * tick
     AND book_stable (两侧 spread ≤ 3 ticks)
     AND !ofi.both_toxic
     AND !pair_arb_risk_open_cutoff_active

 通过后价位分配沿用现有 pair_arb A-S + Gabagool 逻辑（strategy/pair_arb.rs:62-82）；gate 只是是否出 seed的开关。

 改动：
 - strategy/pair_arb.rs 在 compute_quotes:47 起点加 gate（当 net_diff ≈ 0）
 - coordinator.rs 新 PairArbRegimeGateConfig { enabled, shadow, h_threshold, min_edge_ticks, ... }
 - 计数器：regime_gate_pass / fail_by_{hazard,edge,spread,ofi,cutoff}

 Shadow 先跑：shadow_mode=true 只打日志"会通过/会拦截"，不真拦。对比 baseline 开仓率与 hazard 分布 1-2 周，再
 enforce。

 Config：
 PM_PAIR_ARB_REGIME_GATE_ENABLED=0
 PM_PAIR_ARB_REGIME_GATE_SHADOW=1   # 默认 shadow
 PM_PAIR_ARB_HAZARD_THRESHOLD=0.6   # 初值；Phase B 校准

 Risk: medium。过严不开仓；过松等同不 gate。缓解：shadow-first + threshold 可 env 调。

 Dependency：B 至少 48h 数据（校准 threshold）。

 Phase D — Active Merge Harvest（v1 P4 独立触发）

 Goal：独立的 pair harvest trigger，不依赖 capital recycler 的 low-balance 条件。

 v1 错误：
 - polymarket_v2.rs:1340 if status.free_balance >= cfg.low_water_usdc return — recycler
 在余额充足时直接退出，语义是"补流动性"
 - pair_arb harvest 的语义是"已成对库存变现以周转资金"
 - 两者目标函数不同，硬复用会导致 pair arb 积累大量未 harvest 库存

 v1 的另一个错误（从"gasless"直接推"min_merge_set=1 pair"）：
 - xuan T1 snapshot 恰显示他持 367 对未 merge
 - → 即使 gasless，他 harvest 也有更聪明的时机选择（conjecture：hazard 恶化 / regime 切换 / cycle 过老）

 改动：
 - bin/polymarket_v2.rs 新 actor PairArbHarvester（独立于 run_capital_recycler）
 - 触发条件（保守起步）：
   - paired_qty ≥ harvest_min_full_set（≥ 20 shares 而非 1）
   - 距上次 harvest ≥ harvest_min_interval_secs
   - 非 Phase E escalation 进行中
   - 两侧都无活跃 order（防 race）
   - pending_yes_qty < ε AND pending_no_qty < ε（settled-only safety）
   - （可选 v2.1）regime.hazard ↓ 或 ttl 临近 resolve → 优先 harvest
 - 调用 execute_market_merge（已支持 dry_run，claims.rs:320）
 - 不使用 plan_merge_batch_usdc / try_recycle_merge（recycler 专用）

 Config：
 PM_PAIR_ARB_HARVEST_ENABLED=0
 PM_PAIR_ARB_HARVEST_MIN_FULL_SET=20      # shares
 PM_PAIR_ARB_HARVEST_MIN_INTERVAL_SECS=60
 PM_PAIR_ARB_HARVEST_HAZARD_AWARE=false   # v2.1 开关

 Risk: high（真实链上 tx）。缓解：execute_market_merge(dry_run=true) 预演 → 小 min_full_set 单市场 A/B 验证 → 全量。

 Phase E — Completion Push（v1 P2 降级 + 需 Phase A）

 Goal：偶发情况下用 FAK 补腿或 sell 平腿。只有 Phase A 完成后才能做。

 v1 致命问题：
 - v1 的 "sell first leg" 在聚合 VWAP 库存上不安全
 - dispatch_taker_derisk 从 inv.yes_qty 池卖，会把其他 cycle 的库存一起卖掉
 - 必须用 Phase A 的 cycle_id 精确定位 "this cycle's first leg" 才能安全 sell

 改动（Phase A 完成后）：
 - EscalationPhase { Normal, MakerUp, Fak, Abandoning }
 - FAK 路径：OneShotTakerHedge { limit_price: Some(effective_target − opp_avg_cost − 1*tick) }（现有 messages.rs:379
 支持）
 - Abandon sell 路径：从指定 cycle_id 的 tranche 卖（新函数 dispatch_cycle_tranche_sell(cycle_id, ...)）
 - 不动其他 cycle 的库存

 注意权重（与 v1 不同）：
 - xuan 画像显示他不主动救腿；FAK/sell 只在极少数 edge case 触发
 - 阈值宽一点（force_pair_fak_after_ms=30000、abandon_after_ms=60000）
 - 正常应由 Phase C regime gate 避免开进 low-hazard regime

 Config：
 PM_PAIR_ARB_COMPLETION_PUSH_ENABLED=0
 PM_PAIR_ARB_FAK_AFTER_MS=30000
 PM_PAIR_ARB_ABANDON_AFTER_MS=60000
 PM_PAIR_ARB_FAK_MAX_SLIPPAGE_TICKS=3

 Dependency：Phase A 必需。

 Risk: high（真金白银）。

 Phase F — Dynamic pair_target（v1 P1 降级 optional）

 Goal（如果后续数据显示需要）：在某些 regime 下松一档 target。

 v1 错误：把这放在第 1 实质阶段是在 hazard 分布不受控的前提下优化第二阶变量。

 v2 处理：
 - 先不做
 - Phase C 数据积累后，若发现某些 regime 下 static target 次优（例如 high-hazard 下 target 可以紧 1-2 tick），再引入
 - 可能最终完全不需要做

 Config: PM_PAIR_ARB_DYNAMIC_TARGET_ENABLED=0（默认关且 Phase C 稳定运行 2 周后再评估）

 Phase G — xuan 数据采集（研究，外部并行）

 Goal：拿 xuan trade stream 做 hazard 真值校准。

 手段（按成本递增）：
 1. CLOB REST：https://clob.polymarket.com/trades 探索参数；反搜 xuan 的 maker 地址
 2. data-api activity：探测 endpoint 正确 path（v2 的
 https://data-api.polymarket.com/activity?user=xuanxuan008&limit=100 400 了；可能需要 address 而非 username）
 3. Polygonscan enum：拿到地址后，枚举 CTF / USDC 转账、split/merge tx
 4. Leaderboard scraping：若有 public leaderboard 展示 wallet

 ROI：若能取得 10 小时 trade stream + 同期我方 book snapshot（recorder 已有），可直接拟合他的 regime π 函数。可能单项
  ROI 高于内部工程任一阶段。

 ---
 Phase 依赖 + Flag 表（v2）

 ┌───────┬────────────────────┬─────────────────────────────────────┬────────┬────────────┬────────┐
 │ Phase │        名称        │                Flag                 │  默认  │    依赖    │  Risk  │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ A     │ Cycle identity     │ — 永久开启                          │ on     │ —          │ medium │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ B     │ Hazard measurement │ — 永久开启                          │ on     │ A (prefer) │ low    │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ C     │ Regime gate        │ PM_PAIR_ARB_REGIME_GATE_*           │ shadow │ B ≥ 48h    │ medium │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ D     │ Harvest merge      │ PM_PAIR_ARB_HARVEST_ENABLED         │ off    │ —          │ high   │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ E     │ Completion push    │ PM_PAIR_ARB_COMPLETION_PUSH_ENABLED │ off    │ A 必须     │ high   │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ F     │ Dynamic target     │ PM_PAIR_ARB_DYNAMIC_TARGET_ENABLED  │ off    │ C ≥ 2w     │ medium │
 ├───────┼────────────────────┼─────────────────────────────────────┼────────┼────────────┼────────┤
 │ G     │ xuan data          │ 外部                                │ —      │ —          │ low    │
 └───────┴────────────────────┴─────────────────────────────────────┴────────┴────────────┴────────┘

 Verification（recorder extension，v1 "新建记录" 段作废）

 现状：recorder.rs live；user_ws.rs:146、executor.rs:184、inventory.rs:93、polymarket_v2.rs:7031 已接入。

 v2 要做的 extension：
 1. Phase A cycle_id 写入 event stream（cycle_opened、cycle_closed、fill_labeled_with_cycle）
 2. Replay 工具：从 recorder event 重建 pair cycles + regime features + completion time
 3. Regime feature snapshot 在 book/fill event 同步写入（若未存）
 4. Hazard estimator（离线 Python 或 Rust binary）：输入 replay → 输出 h(τ | regime_bucket) 经验分布
 5. Regime gate threshold calibration：Phase B 数据 + A/B test（shadow vs enforce）

 Critical files

 核心改动：
 - src/polymarket/inventory.rs（Phase A cycle_id + per-cycle ledger）
 - src/polymarket/messages.rs（FillRecord.cycle_id, 可能新 cycle event）
 - src/polymarket/coordinator_metrics.rs（Phase B KPI fix：working 替代 settled）
 - src/polymarket/strategy/pair_arb.rs（Phase C regime gate; Phase E push；Phase F 可选）
 - src/bin/polymarket_v2.rs（Phase D PairArbHarvester）
 - src/polymarket/coordinator.rs（cycle 生命周期编排、regime features 聚合）
 - src/polymarket/coordinator_order_io.rs（Phase E 新 dispatch_cycle_tranche_sell）
 - src/polymarket/recorder.rs（Phase A cycle events）

 已确认的设计决策（v2）

 1. 核心概念：completion hazard（非 pair_target）
 2. Phase A 前置 + 必做：cycle/tranche identity
 3. P0 KPI fix：working_changed + order_accepted_at
 4. Phase C 重写：two-sided passive seed + regime classifier
 5. Phase D 独立触发：不复用 capital recycler
 6. Phase E 降级 + 依赖 A
 7. Phase F 降级 optional
 8. 排序：A → B → C → D → E → (F optional) + G 并行外部
 9. 证据原则：单点快照不当时间均值；所有定量标 ★

 开放问题

 1. Phase A cycle_id 是主 InventoryManager 内部维护还是仅 recorder-only ？
   - In-memory：抽象开销略大，但实时可用于 Phase E 安全 tranche-sell
   - Recorder-only：代码简单，但 Phase E 必须访问 recorder replay（不现实）
   - 倾向：in-memory 为准，仅 abandon-sell 逻辑读；recorder emit 用于离线 replay
 2. Phase D harvest 的 hazard-aware trigger 放 v2.0 还是 v2.1？
   - v2.0 简单 paired_qty ≥ threshold（安全保守）
   - v2.1 加 hazard-aware（更像 xuan 但数据依赖）
   - 倾向：v2.0 先落，v2.1 观测数据后再加
 3. 是否投入 Phase G（获取 xuan 地址 / trade stream）？
   - 高 ROI 但不在自己代码库
   - 可以我跑 CLOB REST 脚本尝试
 4. max_net_diff 上限调整：xuan 可容忍 ~100 股净单腿；我们现在 5.0。Phase C 上线后是否放宽到 30-50？
 5. 是否需要 BTC spot signal（Binance WS）接入 Phase C regime features？
   - 额外依赖（外部 WS 连接）但可能显著提升 regime classifier 准确性
   - 倾向：v2.0 内先不做；Phase B 数据证实 spot drift 是强 feature 后再加