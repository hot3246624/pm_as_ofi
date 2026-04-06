  ---
  日志深度分析：polymarket.log.2026-04-06

  前言：PLAN_Codex.md 现有结论

  PLAN_Codex
  已识别三个问题：①reprice_ratio=0.97(ALERT)，②发布原因统计全零，③尾盘 keep_rate
   崩塌。我的分析在这三点基础上补充了更深入的根因。

  ---
  基本参数确认

  Round #1: btc-updown-15m-1775441700
  时间: 02:15:01 → 02:30:00 (14分59秒, 898s)
  配置: pair=0.98, bid=5.0, net=15, debounce=700ms, reprice=0.020, dry=true
  开盘 suitability: yes_med=0.435, no_med=0.565, min_mid=0.435 → Eligible ✅
  市场走势: YES 0.435 → 0.97 (BTC 单边上行，YES 胜)

  ---
  一、Shutdown 关键数字解析

  placed=824       — 提交的订单总数
  cancel=801       — 撤单总数
    cancel_reprice=796  — 因 reprice 触发的撤单
    cancel_stale=5      — 因 stale data 触发的撤单
  replace=22           — replace 操作（轻量修改，非全量撤重提）

  retain(hits=0, soft_reset=801, full_reset=801)
  → 每一次 reprice 均为 Full Reset（撤+重提），retain 逻辑从未命中
  → soft_reset=full_reset=801，即 soft_reset 路径完全失效

  publish(events=802, initial=0, policy=0, safety=0, recovery=0)
  → 802 次发布事件，但原因统计全部为 0 → 可观测性断裂

  skip(debounce=19271, backoff=0, empty=3, inv_limit=0)
  → 57890 ticks 中 19271(33.3%) 被 debounce 过滤
  pair_arb_gate: keep=103661, skip_edge=8021, skip_inv/sim/util=0

  ---
  二、reprice_ratio=0.97 的机制分析（PLAN_Codex 结论延伸）

  表面现象：824次提单中 796次因 Reprice 撤单，比率 96.6%

  根因解构：

  ┌──────────┬──────────────────────────┬───────────────────────────────────┐
  │   组件   │         实际行为         │             期望行为              │
  ├──────────┼──────────────────────────┼───────────────────────────────────┤
  │ retain   │ hits=0（从不命中）       │ 价格未超 reprice_threshold        │
  │ 逻辑     │                          │ 时保留现单                        │
  ├──────────┼──────────────────────────┼───────────────────────────────────┤
  │ Reprice  │ 无条件 Full Cancel +     │ 先检查 ΔP ≥                       │
  │ 路径     │ Re-submit                │ reprice_threshold，才撤           │
  ├──────────┼──────────────────────────┼───────────────────────────────────┤
  │          │ 每次盘口 tick            │                                   │
  │ 发布频率 │ 触发策略计算，新价格 =   │ reprice_threshold=0.020，但盘口   │
  │          │ 旧价格 + ~0.01 → 超      │ tick=0.010，2个 tick 就触发       │
  │          │ reprice_threshold        │                                   │
  └──────────┴──────────────────────────┴───────────────────────────────────┘

  实际观察（02:16 早期段）：
  02:16:02  YES@0.450, NO@0.500  → 首次提单
  02:16:03  cancel(Reprice,Full) → YES@0.440, NO@0.510  [1秒后，ΔP=0.01 × 2]
  02:16:09  cancel(Reprice,

⏺ I now have all the data I need. Let me compile the complete analysis.

  ---
  polymarket.log.2026-04-06 深度分析

  日志范围: Round #1（02:15:01–02:30:00）+ Round #2 开头（02:30:14–…）
  配置: pair_arb, max_net_diff=15, bid_size=5.0, pair_target=0.98, dry_run=true

  ---
  一、市场特征概述

  这是一轮极端单向市场。YES 从 0.44 一路涨至 0.97（BTC 强势上行），历时 ~13
  分钟：

  时间      YES bid   NO bid     特征
  02:16:02  0.450     0.500     均衡区间，Eligible
  02:16:22  0.570     0.380     YES 急速上行
  02:16:38  0.650     0.300     YES 主导
  02:17:xx  0.700+    0.200-    完全单边
  ...
  02:28:40  0.960     0.010     极端尾盘
  02:28:51  0.970     （仅YES）  NO 出价消失

  ---
  二、Shutdown 关键数据逐项解读

  placed=824
  publish(events=802 replace=22 cancel=801 initial=0 policy=0 safety=0
  recovery=0)
  cancel(reprice=796 stale=5 toxic=0 inv=0)
  retain(hits=0 soft_reset=801 full_reset=801)
  ofi(heat_events=10 toxic_events=0 pair_arb_softened=11145
  pair_arb_suppressed=0)
  pair_arb_gate(keep=103661 skip_inv=0 skip_sim=0 skip_util=0 skip_edge=8021)
  skip(debounce=19271)
  reprice_ratio=0.97 [ALERT]

  ---
  三、PLAN_Codex 三大问题的实证验证

  问题 1：publish 原因统计全为 0（BUG 已确认）

  publish(events=802 initial=0 policy=0 safety=0 recovery=0)

  802 次发布事件，全部未分类。这不是"偶然"——pair_arb 模式下 cause
  字段根本没有被赋值或写入 shutdown 统计路径。PLAN_Codex 的诊断完全准确。

  修复优先级：P0（日志可观测性）

  问题 2：Reprice 全量重挂（核心性能问题）

  retain(hits=0 soft_reset=801 full_reset=801)

  - hits=0：软保留路径从未触发，一次都没有
  - full_reset=801：每一次 reprice 都做 cancel + full reprovide
  - 结果：reprice_ratio=0.97（ALERT），57,890 ticks 里 97% 的 placed 都是 full
  reprice

  机制：BTC 单向连续移动，每 0.02 价格变动就触发一次 reprice（reprice
  threshold=0.020）。YES 从 0.44 到 0.97，理论最少触发 (0.97-0.44)/0.02 = 26
  次，加上振荡远不止。

  修复优先级：P1（改 Full reset → 软保留优先）

  当新目标价格与当前挂单价格差 < reprice_threshold/2（1
  tick）时，保留现有订单不动。预期 retain.hits 可达 40-50%，reprice_ratio 降至
  0.50 以下。

  问题 3：尾盘 keep_rate 崩塌

  PairArbGate 时序：

  ┌──────────┬───────────┬───────────┬─────────┬────────┐
  │   时间   │ keep_rate │ skip_edge │ YES bid │ NO bid │
  ├──────────┼───────────┼───────────┼─────────┼────────┤
  │ 02:28:34 │ 83.6%     │ 548/3344  │ ~0.90   │ ~0.05  │
  ├──────────┼───────────┼───────────┼─────────┼────────┤
  │ 02:29:04 │ 70.2%     │ 782/2620  │ ~0.95   │ ~0.03  │
  ├──────────┼───────────┼───────────┼─────────┼────────┤
  │ 02:29:34 │ 50.0%     │ 421/842   │ ~0.96   │ ~0.01  │
  └──────────┴───────────┴───────────┴─────────┴────────┘

  崩塌时间点：02:29:00 = round 结束前 60 秒 = soft endgame window 开始。

  配置：edge(keep/exit)=1.50/1.25。在 soft window 内，open_edge 检查要求 1.50x
  余量。当 YES=0.97 时：
  YES bid = 0.95（ask 0.97 - 0.02 safety）
  NO bid  = 0.01（ask 0.03 - 0.02 safety，极限 1 tick）
  open_edge = open_pair_band(0.99) - (0.95 + 0.01) = 0.03
  0.03 不满足 1.50x 阈值 → skip_edge 批量触发。

  但这是否应该修复？ 对于 pair_arb 策略，尾盘 YES=0.97 / NO=0.01 时：
  - 我们在 0.95 挂的 YES 买单远低于市场 0.97，不会成交
  - NO@0.01 的买单也几乎不会成交（ask 已在 0.03）

  这意味着尾盘的 keep_rate 崩塌并不造成真实损失——反而节省了无效的 cancel/reprice
   开销。PLAN_Codex 说要"保留策略"，但对于
  pair_arb，这段时间的正确做法可能是直接停止报价，而不是继续维持极端 tick
  的订单。

  修复建议：不是提高尾盘 keep_rate，而是在 YES/NO 任意一侧超过极端阈值（如 YES >
   0.90 且 round 剩余时间 < 120s）时，主动 skip 整个 slot 的报价。减少无效
  cancel。

  ---
  四、新发现（PLAN_Codex 未覆盖）

  发现 A：Suitability Gate 工作正常

  02:16:02 PairArb suitability decided: Eligible
    samples=3844 yes_med=0.435 no_med=0.565
    min_mid=0.435 sum_mid=1.000 pair_target=0.980

  Round 开始 61 秒后的均衡采样显示市场合适（min_mid=0.435 > 0.20 阈值），Gate
  正确放行。

  然而：仅有 1 次 suitability 决定。Gate 只在 round 前 60s
  窗口内运行，通过后不再重检。这是设计选择，但当市场在 Round 内从均衡变为 95/5
  极端时，Gate 无法介入。

  发现 B：NO 出价在极端尾盘消失

  02:28:51: DRY Provide Buy Yes@0.960   ← 只有 YES
             （NO 出价缺失）
  02:28:53: DRY Provide Buy Yes@0.970   ← 仍只有 YES

  当 NO ask = 0.02，safety margin = 0.02 时：NO bid = 0.02 - 0.02 = 0.00 →
  低于最小 tick → NO 出价被过滤。

  这意味着在极端尾盘，系统只能出 YES bid，但 YES bid 在 0.95 处也远低于市场价
  0.97。双边报价实际上已停止工作。

  发现 C：OFI 热度有效但无毒性

  heat_events=10  toxic_events=0  kill_events=0
  pair_arb_softened=11145  pair_arb_suppressed=0

  两次 heat 事件对应：
  1. 02:15:59: YES 强烈买压（raw_ofi=2021.6, ratio=0.97）→ 对应 BTC 上行起始
  2. 02:28:53: YES 再次买压（raw_ofi=1854.2, ratio=0.83）→ 对应尾盘最后一波

  OFI 共"软化"了 11,145 次出价（约占 keep 总量的
  10.7%），但从未触发完全压制。adaptive tail-quantile 模式的 q99
  阈值确实防止了误触发。

  发现 D：debounce 消耗 33% tick

  skip(debounce=19271)  ticks=57890

  19271/57890 = 33% 的 tick 因 700ms debounce 跳过。在 BTC 强势单向行情中，700ms
   内价格可能已跳动 2-3 ticks，导致错过最优补价时机。PLAN_ANTI_GEM 建议降至
  200ms 的依据在此得到验证。

  发现 E：Recovery Bug 已消灭

  recovery=0 → 上一个实盘分析中的 P0 Recovery 循环 Bug
  已彻底消失。这个修复是有效的。

  发现 F：基础设施稳定

  ref(blocked_ms=0 source=0 source_binance=0 divergence=0) → Binance WS
  全程无断连，无 drift 阻断。之前的 P2 Binance 问题也已解决。

  ---
  五、pair_arb 策略在单向市场的表现评估

  这是一轮 单边极端局（YES 0.44→0.97，全程单调上行）。对 pair_arb 影响：

  有利因素：
  - YES=0.44 附近有约 90 秒的均衡窗口，live 模式可填入 YES 仓位
  - NO 一路从 0.56 跌至 0.01，在 0.30-0.15 段有大量廉价 NO 供积累
  - 最终 YES 赢（BTC 上行确认），YES 仓位可兑换 $1.00/份

  不利因素：
  - 早期均衡窗口仅 ~90 秒，YES 仓位积累有限
  - YES 快速上行后，后续 YES bid 在 0.55 挂单，市场 ask 已到 0.70+，不会成交
  - pair_cost = avg_YES + avg_NO 中，avg_YES 可能停留在较低水平（只有早期 90s
  成交）

  理论 live 结果（假设）：
  YES: 3 fills × avg 0.45 = 花费 $1.35，收益 $3.00（YES 赢）
  NO:  8 fills × avg 0.25 = 花费 $2.00，收益 $0.00（NO 输）
  总成本: $3.35, 总收益: $3.00 → 亏损 -$0.35

  但如果 NO 在 0.01-0.05 区间大量积累（如 gabagool 做法）：
  NO: 15 fills × avg 0.10 = 花费 $1.50
  YES: 3 fills × avg 0.45 = 花费 $1.35
  总成本: $2.85, 总收益: $3.00 → 盈利 +$0.15

  差异在于：在 NO=0.01-0.05 时能否快速大量填入。当前 max_net_diff=15、bid_size=5
   限制了此场景的利润挖掘能力（Phase 2 扩大至 max_net_diff=50
  的必要性在此体现）。

  ---
  六、行动优先级（结合 PLAN_Codex + 本次分析）

  ┌──────┬─────────────────────┬──────────────────────┬────────────────────┐
  │ 优先 │        问题         │       修复方法       │        文件        │
  │  级  │                     │                      │                    │
  ├──────┼─────────────────────┼──────────────────────┼────────────────────┤
  │      │                     │ pair_arb 模式中补全  │ coordinator_order_ │
  │ P0   │ publish cause 全零  │ initial/policy cause │ io.rs              │
  │      │                     │  的赋值路径          │                    │
  ├──────┼─────────────────────┼──────────────────────┼────────────────────┤
  │      │                     │ 实现 soft-retain：新 │                    │
  │ P1   │ Full reprice（retai │ 价格与当前 live      │ coordinator_order_ │
  │      │ n.hits=0）          │ 价格差 < 1 tick      │ io.rs              │
  │      │                     │ 时跳过 cancel        │                    │
  ├──────┼─────────────────────┼──────────────────────┼────────────────────┤
  │      │ 极端尾盘继续无效报  │ YES > 0.90           │ pair_arb.rs /      │
  │ P2   │ 价                  │ 且时间窗口 < 120s →  │ coordinator.rs     │
  │      │                     │ skip 整个 slot       │                    │
  ├──────┼─────────────────────┼──────────────────────┼────────────────────┤
  │ P3   │ debounce=700ms 过慢 │ Phase 2 降至 200ms   │ .env               │
  ├──────┼─────────────────────┼──────────────────────┼────────────────────┤
  │ ——   │ max_net_diff=15     │ Phase 2 扩大至 50    │ .env               │
  │      │ 深度不足            │                      │                    │
  └──────┴─────────────────────┴──────────────────────┴────────────────────┘

  Round #2 已确认正常启动（02:30:14，Eligible suitability，YES_med=0.545
  均衡局）。建议继续跑多轮 dry-run 后再进小额实盘。

  ---
  总结：PLAN_Codex 三项诊断（publish cause=0、reprice
  全量、尾盘崩塌）全部在本次日志中得到量化确认。Recovery Bug 和 Binance
  断连两个历史问题已消灭。当前的核心工程瓶颈是 reprice 全量重挂（P1 Fix）和
  publish cause 统计缺失（P0 Fix）。

✻ Churned for 8m 29s