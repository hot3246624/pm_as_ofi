@xuanxuan008 策略深度解析 + Pair_Arb 升级计划

▎ 本文档分两部分：Part 1 是对交易者 @xuanxuan008 策略画像的反向工程解析（新增）；Part 2 是基于该解析的我方
▎ pair_arb 升级落地方案（原有，已按解析调整优先级）。

PART 1 · @xuanxuan008 策略深度解析

生成时间：2026-04-24 | 数据源：Polymarket profile 两次快照 + 微结构研究 + 链上文档
置信度标注：★★★ = 直接证据 / ★★ = 强推断 / ★ = 合理假设 / ☆ = 未定论

1.0 执行摘要

核心判定 ★★★：@xuanxuan008 在做真实的同市场 YES+NO pair-arbitrage，主战场 BTC 5-min Up/Down。直接证据：T1
快照 top 2 持仓就是同一市场两侧（482.6 Up + 367.9 Down, BTC April 24 5:40-5:45AM ET）——这是 pair_arb
的物理指纹，无法被其他策略解释。

画像五要点：
1. 被动双向 maker 为主（★★）——持仓 76% 成对、24% 单腿，不是瞬时双吃的形态；若纯主动 FAK，成对率应 > 95%
2. 动态 pair_target（★★）——用户观察到他不用固定目标，符合深度/波动率/TTL 加权 target
3. Merge 在 Polymarket 上是 gasless + 零手续费（★★★，已文档确认）——这彻底改变 pair_arb 经济学，min_merge_set
不受 gas 约束
4. 允许有界单腿浮仓（★★，直接从持仓看到 114.7 股净
Up）——与"几乎无单腿风险"的表述冲突，或用户指的是"零完全裸露"而非"零临时不平衡"
5. 多市场并发做市（★）——以 4,260 笔 / 30 天 + 单市场 482 股尺寸估算，他同时在 5-10 个 5-min 市场挂单

三大不确定性（需进一步采集数据）：
- 入场是 maker 挂单等成交，还是 taker 主动扫单？（决定 P2 vs P3 哪个是核心）
- 有无外部信号（BTC spot、Binance orderbook、跨市场相关）？
- "30s 配对" 的具体定义：从首腿到成对的 p50 还是 p95？

1.1 可观测事实（Facts）

账户元数据 — 两次采样对比

|     指标     | T0（首次采样） | T1（数小时后） |       Δ       |
|--------------|----------------|----------------|---------------|
| 累计 PnL     | $43,367.95     | $43,813.04     | +$445.09      |
| 总交易笔数   | 4,238          | 4,260          | +22           |
| 当前持仓价值 | $88.64         | $427.51        | +$338.87      |
| 最大单笔盈利 | $409.98        | $409.98        | 0             |
| 注册         | April 2026     | April 2026     | 约 1 个月账龄 |

解读：两次采样之间他还在持续活跃（+22 笔、+$445 PnL），且 T1
持仓大幅膨胀——正好抓到他在一个市场里"持仓周期内"的中间态。

关键指纹：同一市场两侧共存 ★★★

T1 top 持仓：
1. BTC Up-or-Down April 24 5:40-5:45 AM ET · Up · 482.6 shares
2. BTC Up-or-Down April 24 5:40-5:45 AM ET · Down · 367.9 shares
3-5. 分散在 April 15-17 的 5-min 市场，各 10-15 股

推导：
- min(482.6, 367.9) = 367.9 完整对，可即时 merge 换 $367.9（gasless）
- 净 Up 单腿暴露 = 114.7 股，按 T1 P(Up)≈0.52 估 ≈ $60 notional
- 两次采样前 top 1 是 147 Up @ 50.2¢（不同市场）；T1 换成 482.6 Up @新市场——说明他 position 周转快、会换市场

速率推算

|      度量      |  推算值  |             支持度              |
|----------------|----------|---------------------------------|
| 日均 PnL       | $1,460   | ★★ 算术直接                     |
| 小时均 PnL     | $60      | ★★                              |
| 日均交易笔数   | 142      | ★★                              |
| 小时均交易笔数 | 6        | ★★                              |
| 均笔 gross PnL | $10.3    | ★ 假设胜负均摊                  |
| 资金周转率     | 约 3x/日 | ★ 由 $427 float / $1,460 PnL 推 |
| 持仓周期       | < 5 min  | ★★ 每市场最多活 5 min           |

数据缺口（无法获取）

- Activity 流（trade-by-trade）——Polymarket 是 JS SPA，WebFetch 只拿到 positions 聚合
- 钱包地址 0x... ——Profile 不显示，搜索未找到
- 逐笔 timestamp / maker-or-taker flag / split-or-merge 操作 ——需要 API 或链上数据
- 历史 PnL 时间序列 ——无法验证每日一致性还是峰值驱动

1.2 Polymarket BTC 5-min 微结构（背景事实）

结构性不变式（已文档确认）：
- YES + NO 结算和 ≡ $1.00（二元期权）
- Split：任意时刻 $1 USDC → 1 YES + 1 NO（gasless via relayer）
- Merge：任意时刻 1 YES + 1 NO → $1 USDC（gasless + 零协议手续费）
- Oracle：price feed 来自 CEX（主要 Binance/Coinbase）
- 结算粒度：即时（market ends 后秒级 resolve）

典型数值（研究文献 & 观察）：
- Spread：2-4c per side（流动市场），5-15c（稀薄市场）
- Book 深度：浅，大单会移价
- T-10s 信息确定性：约 85% 的方向已被 BTC spot 确定，但盘口未完全反映
- 500ms taker-quote delay 已取消（Polymarket 去年的变更）→ 纯跨交易所延迟 arb 已死

对 pair_arb 的三大含义：
1. gasless merge → 每完成 1 对就可以立刻兑现 $1，不需批量（除非 bot 端管理成本）
2. 2-4c spread → 单笔 pair edge 大约 1-3c；需走量而非单笔大利润
3. 5-min 窗口 → 必须要么在窗口内配对+merge 变现，要么持有到 resolve 接受二元风险

1.3 候选策略假设（多路径归因）

H1 ★★★ 双向被动 maker + 自然配对（首要假设）

机制：
- 在一个 5-min 市场的 YES 和 NO 两侧同时挂 maker bid
- 两侧价格和 = 动态 pair_target（例如 0.96）
- 哪侧 taker 先打过来就哪侧先成交，另一侧继续挂等成交
- 完成 1 对就 merge

匹配证据：
- ★★★ 持仓 76% 成对 24% 单腿——符合"maker 被动等对侧成交，暂时不平衡"的形态
- ★★ 持仓比例 482/367 ≈ 1.31——若主动 FAK 应该立即 1:1
- ★★ 低/中 trade velocity（6 笔/h）——maker 做市的自然节奏
- ★ 允许不立即 merge——正在继续累积仓位

反证据：
- ⚠️  用户观察"30s 配对 p95 ≈ 30s"——纯 maker 在浅 book 上 30s 配对率不一定能稳定达到

信号：每个活跃的 BTC 5-min 市场都成为被动目标。入场无显式 signal，退出通过 merge。
单腿风险处理：接受有界单腿，用对侧主动挂单吸引 taker 来平衡，而非主动平腿。

H2 ★★ 盘口 sum 异常 → 双 FAK 扫单（辅助假设）

机制：
- 扫描所有活跃市场的 ask(YES) + ask(NO)
- 若某时刻 sum < 0.97（即有 ≥ 3c edge），同时发两个 FAK 双买
- 成交后立即 merge（gasless）

匹配证据：
- ★★ 能解释 "30s 配对 p95"（两 FAK 秒级成交）
- ★★ 能解释"不固定 pair_target"（阈值随盘口实时判断）

反证据：
- ⚠️  持仓 76% 成对不是 FAK 双吃的形态（应近 100%）
- ⚠️  $1,460/天 的利润若靠 FAK 双吃需要每日很多 opportunity，与 Polymarket 整体 arb 被压薄的事实矛盾

可能性：作为 H1 的触发式辅助——平时挂 maker，遇到盘口 sum 明显下跌时动用 FAK。

H3 ★ Split-as-entry 合成单腿

机制：
- 不直接买某一侧 token，而是 $1 split → 1 YES + 1 NO
- 立即在市场上按最佳 bid 卖出其中一侧
- 剩下的另一侧 = 合成方向性仓位，成本 = $1 − 对侧卖出价

匹配证据：
- ★ 能解释为什么他"几乎无单腿风险"——split 本身即瞬时双持，无时间差
- ★ 解释静态 114.7 单腿残留：这是合成方向性仓位

反证据：
- ⚠️  无直接证据看到 split tx（需链上数据）
- ⚠️  效率上不一定比直接买便宜

可能性：作为 H1 的入场替代路径，在 bid-ask 结构对 split-and-sell 有利时采用。

H4 ★★ Oracle-lag / T-minus 狙击

机制：
- 利用"BTC 方向 T-10s 时 85% 确定但市场未完全定价"的事实
- 在临近 resolve 的最后 30-60s 里，根据 BTC spot 价位判断赢家侧
- 快速 FAK 吃赢家侧的 ask

匹配证据：
- ★★ 能解释盈利来源（这是 Polymarket 已知仍存在的 edge）
- ★ 与我方 oracle_lag_sniping 策略同构

反证据：
- ⚠️  这不是 pair_arb，而是方向性投注
- ⚠️  他当前持仓 482 Up + 367 Down 同市场——若是 T-minus 狙击应单边持仓

可能性：作为 独立策略线并存，与 pair_arb 分账户管理。若他实际同时跑两种策略，那我们应该分别对标。

H5 ☆ Kalshi-Polymarket 跨所三角

机制：在 Kalshi 和 Polymarket 都开同一 BTC 市场的对侧，锁跨所 edge。

反证据：此账户只有 Polymarket 数据。无法验证。

可能性：暂不考虑。

假设组合与权重

综合证据，我的最佳模型：
- 主策略（60-70% 贡献）：H1 双向被动 maker 在 5-10 个 5-min 市场并发
- 辅策略（20-30% 贡献）：H4 T-minus 狙击（最后 60s）
- 偶发（< 10% 贡献）：H2 FAK 扫单 in edge 特别大的异常时刻
- 不确定：H3 split-as-entry 可能作为入场工具，但无强证据

1.4 "不固定 pair_target" 的可能实现 ★

他的 target 大概率动态，由如下因子组成：

|              因子              |            方向            |             直觉             |
|--------------------------------|----------------------------|------------------------------|
| 滚动 (mid_YES + mid_NO) 中位数 | 越低越松 target            | 跟随市场公允值               |
| 书深度（两侧 min）             | 越浅越紧 target            | 浅书不敢让利，否则吃不到     |
| 近窗波动率 σ(mid_sum, 30s)     | 越高越紧 target            | 高波动需更大安全边际         |
| 距 resolve 剩余时间 TTL        | 越短越紧 target            | 临近结算风险更大             |
| OFI 毒性                       | 越毒性越紧 target          | 避免被动成交给 informed flow |
| BTC spot 与市场隐含概率偏差    | 偏差大时两侧 target 不对称 | 信号驱动单侧更紧             |

典型形式（我的建模猜测）：
base = 0.99
target_YES_side = base
  − k_vol * rolling_stddev(mid_sum, 30s)
  − k_depth * 1 / min(yes_depth, no_depth)
  − k_ttl * max(0, 1 - ttl/300)^2
  − k_ofi_yes * ofi_toxicity(YES)
  + k_spot_bias * sign(spot_drift)  // 若 BTC 上涨动量，允许 YES 出更高价
YES 侧和 NO 侧可以异步调整，不必对称。

1.5 "30s 配对" 的机械路径 ★★

用户观察"平均 30s 配对"。解读候选：

|            路径            |  p50 可达  | p95 可达 |     与观察契合度      |
|----------------------------|------------|----------|-----------------------|
| 纯被动双 maker             | 60s - 300s | 不可控   | ⚠️  难达到             |
| 被动 maker + 主动 FAK 兜底 | 5-30s      | 30-60s   | ✅ 强                 |
| 同时双 FAK 扫单            | 1-3s       | 5-10s    | ⚠️  过快               |
| 挑剔入场（只在明显机会）   | 视机会质量 | 30-60s   | ✅ 中                 |
| Split-then-sell            | 2-5s       | 5-10s    | ⚠️  过快且应完全无单腿 |

最可能：被动 maker 为主 + 当单腿 age > ~20s 时用 FAK 补齐。这与"几乎无单腿"+ "30s"同时成立。

但注意——他持仓 24% 单腿的事实提醒我们："30s 配对"可能不是他的绝对 SLA，而是"平均情况下 30s 内 pair
推进一次"。他愿意容忍一定的单腿暴露，尤其当 spot 信号看好这侧时。

1.6 PnL 压力测试（数量级一致性）

纯 pair_arb 模型：
- 均笔 edge = 2-3c per pair
- 均笔尺寸 = 200-500 股（与 top 持仓 482 一致）
- 每 pair PnL = 300 × $0.025 = $7.5
- 日完成 pair 数 = $1,460 / $7.5 ≈ 195 对 / 日

验证：若他并发 8 市场、每市场每 5 min 配 1 对，则 8 × 12 = 96 对/h，每日 24h 运行能达 2,304 对——远超所需
195。实际可能每小时 10-20 对，共 ~200 对/日。数量级一致 ✅。

但若考虑每笔需要一次 YES 买 + 一次 NO 买 + 1 次 merge = 3 动作，单日 200 pair = 600 动作 /
天。而他观察到交易笔数 142/日——这不一致。

调和解释（较合理）：
- 交易笔数统计的不是 CLOB 操作，而是"合约级 prediction"——即一个 market 算一笔，不管他在里面下了多少 order
- 或者：每 pair 用一次 split（而非两次 buy）完成入场，再卖一侧或直接持到 resolve——大幅降低操作数
- 或者：大部分收入来自 H4（T-minus 狙击）而非 pair_arb，pair_arb 仅 1/3 收入

更新的假设权重：
- H1 双向 maker 贡献可能被高估；实际可能 40-50%
- H4 T-minus 狙击 或 H3 split-entry 可能占 30-40%
- 其余为辅助

1.7 证据冲突与未定论

|           冲突           |    观察     |                              可能解释
| 持仓 114.7 股单腿 vs     | T1 快照     | 用户指的是"完全裸露"而非"临时不平衡"，他接受有界单腿作为做市库存
| "几乎无单腿风险"         |             |
|                          | T1 快照未   |
| 两市场同时都有大量两侧   | merge       | (a) 市场仍活跃他不想打断做市；(b) 等批次 merge 更省;(c) gasless
| vs 高 merge 频率         | 已成对的    | 所以其实懒得立即 merge
|                          | 367.9       |
| 交易笔数 142/日 vs 估算  | Profile     | "prediction" 计数口径 != CLOB order 数
| 600 动作/日              | 统计        |
| ~1 月账龄产生 $43K PnL   | 注册 April  | 很可能是成熟交易者切换的新账户、并非新手
|                          | 2026        |

1.8 反向工程路径（若要继续深入）

进一步采集的优先级：
1. 钱包地址 → 链上交易（高价值）：通过 Polymarket 公开 API 或社区、或观察 Polymarket 的 CLOB REST
(/trades?maker=<name>) 尝试反查。有了地址，Polygonscan 可以 enumerate 每笔 split/merge/swap，精确反演策略
2. Activity 流（Public API）：https://data-api.polymarket.com/ 有公开 activity endpoint，本次 fetch
失败可能是参数或路径错误——写一个小脚本用浏览器化 headers 重试
3. BTC spot 同步数据：用公开 websocket 抓 Binance BTCUSDT price，与他下单时刻对齐——可检验 H4 假设
4. 盘口快照：若我们已有的 bot 正在跑这些市场，调出历史 bookupdate + 他 fill 的时刻交叉——可反演他是 maker 还是
 taker

可执行的本地实验：
- Shadow 观察模式：在 8-10 个 BTC 5-min 市场用我们 bot 的 dry-run 模式并 log "would-take" 时刻，对比他的 fill
 timestamps → 差异即是他优于我们的信号
- 1-2 周数据即可高置信度反演他的核心信号
- 若我方 pair_arb 升级后能达到他同样的持仓结构，说明策略对齐

1.9 对我方升级方案的含义（与 Part 2 的呼应）

验证的决策：
- ✅ P0 观测是正确的第一步——他的配对时间分布就是关键 KPI
- ✅ P1 动态 pair_target 是真实的且必需——画像证据支持
- ✅ P4 merge 换现金 极度激进配置——gasless 意味着 min_merge_set 可以设为 1 pair

修正：
- ⚠️  P2（主动 FAK 补全）优先级应降低——他的 76% 成对率说明他并未强制 FAK 平腿；"FAK 兜底"
作为偶发机制保留即可，不是核心
- ⚠️  P3（入场闸门）优先级应提前——他的 edge 大概率来自"只在对的时机开仓"而非"事后补救"。建议把 P3 排在 P2 之前
- 💡 新增 P6：多市场并发 pair_arb 管理——他同时跑 5-10 市场，我们目前是 per-market Coordinator 已支持，但需要
portfolio-level 资金分配 & 盘口扫描器

新发现（建议考虑纳入）：
- 💡 BTC spot 信号通道——考虑接入 Binance BTCUSDT 实时价格，作为 pair_arb 入场闸门的额外信号（当 spot
与隐含概率大偏离时，目标侧 target 松一档）
- 💡 T-minus 狙击模块——我们已有 oracle_lag_sniping，可能需要扩展到 BTC 5-min 场景（非 oracle 结算的市场也有
T-minus edge）
- 💡 接受有界单腿浮仓——当前 max_net_diff=5.0 可能过紧；考虑分档：(净 < 10) 正常运作、(10-30) 减缓新开仓、(>
30) 强制平腿

优先级重排建议（实施时用，用户可在 Part 2 决定是否采纳）：
原：P0 → P1 → P2 → P3 → P4
新：P0 → P1 → P3(入场闸门) → P4(激进 merge) → P2(FAK 兜底) → P6(多市场协调) → T-minus 扩展

1.10 关键问题清单（未决）

1. 是否要复制 H4 T-minus 狙击：我们已有 oracle-lag-sniping，BTC 5-min 是否也适用？（注：BTC 5-min 市场用
oracle 但结算粒度不同）
2. max_net_diff 上限调整：xuanxuan008 展示 ~100 股净单腿；我们现在 5.0，是否准备放宽到
30-50？风险承受能力是否配套？
3. 是否尝试获取他的钱包地址做链上反演：获取方式包括 Polymarket CLOB REST API 查近期 trades 的 maker/taker
地址、社区求助、或订阅 leaderboard。这对 P3 入场闸门参数调校价值极高
4. Part 2 原 P2/P3 顺序调换是否同意（实施阶段决策）：本解析支持先上 P3 再上 P2

PART 2 · Pair_Arb 升级计划（按 Part 1 调整后）

Context

用户观察到 Polymarket 账户 https://polymarket.com/@xuanxuan008?tab=positions（累计 PnL ≈ $43K, 当前浮仓
$88，4238 次交易，风格偏高频 BTC 5 分钟 Up/Down）几乎没有单腿风险、平均 30s 内配对、不使用固定
pair_target。与我们当前 pair_arb 方向一致，但我方实现还停留在「静态 pair_target=0.99 + 1200ms warmup dwell +
被动等对侧成交」。

升级目标：
1. 让 pair_target 跟随市况动态变化（而不是 0.99 死值）
2. 单腿 p95 < 30s、p50 < 10s（通过主动 FAK 补全 / 必要时弃单）
3. 入场时机更挑剔（OFI + 盘口稳态 + 边际融合打分）
4. 配对成完成后主动 merge 换回 $1/对，释放资金（merge 链路已存在，只缺 pair_arb 触发器）
5. 先观测、后动刀：每阶段默认关闭或 shadow-only，逐步放开

关键既有资产（确认存在，可直接复用）：
- Merge 全链路：claims.rs:264 scan_mergeable_full_set_usdc、claims.rs:320
execute_market_merge、bin/polymarket_v2.rs:1138 plan_merge_batch_usdc、bin/polymarket_v2.rs:1287
try_recycle_merge、inventory.rs:299 apply_merge（今天由 capital recycler 在 reject/headroom
情境下触发，pair_arb 侧还未主动调用）
- FAK 执行：OrderManagerCmd::OneShotTakerHedge { limit_price }（messages.rs:379）+
coordinator_execution.rs:416 execute_directional_hedge（当前 pair_arb 在 coordinator_execution.rs:136 直接
return false 跳过；需开新入口）
- 时间衰减：coordinator_pricing.rs:23 compute_time_decay_factor
- 指标钩子：coordinator_metrics.rs:107 observe_pair_arb_inventory_transition（每 tick 都跑，扩展成本低）
- 状态机：PairArbStateKey、slot_pair_arb_* 数组（coordinator.rs:1044-1052）、PairArbNetBucket

Phase 0 — 时间-配对观测（Observability）

Goal：测出基线 single-leg p50/p95 和配对完成时间分布，给后续阶段做 A/B 对照。零行为改动。

Success metric：每 15s 的 📊 StrategyMetrics 日志新增 first_leg_open_age_ms / pair_completion_ms_avg /
pair_completion_ms_p95 / single_leg_duration_max_ms 四项，且数值合理。

改动：
- coordinator.rs:738-785 struct Stats 新增：pair_first_leg_open_events、pair_completion_events、pair_completi
on_time_ms_sum、pair_completion_time_ms_p95_reservoir: [u64;
64]、pair_single_leg_age_ms_max、pair_broken_by_sell_events。
- coordinator.rs:1127-1135 新增：pair_first_leg_opened_at: Option<Instant>、pair_first_leg_side:
Option<Side>、pair_current_cycle_id: u64。
- coordinator_metrics.rs:107 observe_pair_arb_inventory_transition 内：用已有的 settled_changed 分支（避免
pending flicker 重复计数）检测 net_diff 0 → ±x（开首腿）/±x → 0（配对完成）/|net_diff|
↓（部分配对），更新计数器与时间戳。
- coordinator_metrics.rs:611-632 扩展 📊 StrategyMetrics 日志。
- coordinator.rs:1165-1204 CoordinatorObsSnapshot 同步字段，给 round-end validator 使用。

Config / Flag：无。对 pair_arb 永久开启。

Rollback：代码层回滚即可（零行为影响）。

Risk：low。仅加计数器，失败时最差日志噪声。

Phase 1 — 动态 pair_target

Goal：把静态 0.99 替换为滚动、深度、时间衰减三因子的动态目标；深书紧、浅书松、临近到期松。

Success metric：执行时的 pair_cost 比 pair_target_dynamic 至少低 1 tick（1h 滚动窗口内 ≥ 95% 的
fill）；pair_target_dynamic ∈ [0.96, 0.99] 的时间占比 ≥ 99%；开关打开后 24h 的 realized edge 不劣化。

改动：
- coordinator.rs:92-103 PairArbStrategyConfig 追加：
  - dynamic_target_enabled: bool（默认 false）
  - dynamic_target_min/max: f64（默认 0.960 / 0.990）
  - dynamic_target_window_secs: u64（默认 60）
  - dynamic_target_min_edge_ticks: f64（默认 2.0；最小 (target − (mid+mid))）
  - dynamic_target_time_decay_weight: f64（默认 0.5）
- coordinator.rs:439 apply_pair_arb_env 解析 PM_PAIR_ARB_DYNAMIC_TARGET_* 环境变量。
- StrategyCoordinator 新增：pair_mid_sum_samples: VecDeque<(Instant,
f64)>、pair_book_depth_samples、pair_target_dynamic_cached: f64、pair_target_dynamic_cached_at。
- coordinator_pricing.rs（在 compute_time_decay_factor 旁）新增 compute_dynamic_pair_target：
base  = dynamic_target_max
stat  = median(pair_mid_sum_samples[last window])
depth = clamp(depth_p20 / depth_reference, 0.5, 1.0)
td    = compute_time_decay_factor()
target= clamp(
          max(stat + min_edge_ticks*tick, base * depth) - (td-1.0)*time_decay_weight*tick,
          dynamic_target_min, dynamic_target_max)
- 带滞回（|Δ| < 1 tick 不重算）防抖。
- 新方法 effective_pair_target(&self) -> f64，替换所有原 cfg.pair_target 读取点：
  - strategy/pair_arb.rs:63, 78, 173, 201 (excess、overflow、vwap_ceiling)
  - coordinator_metrics.rs:passes_pair_cost_guard_for_buy
  - coordinator_pricing.rs:hedge_target
- 书更新路径（last_valid_book 写入点）追加 1Hz 采样。
- 日志追加 pair_target_eff / pair_target_static / pair_target_source。

Config / Flag：PM_PAIR_ARB_DYNAMIC_TARGET_ENABLED=0（默认关）；推荐先 shadow（enabled=true 但
effective_pair_target() 仍返回 static，仅记录 dynamic 值）跑 48h 再真正切换。

Rollback：一条 env 关闭，100% 行为等价回退。

Risk：medium。窗口过短会抖、min 过紧会永远不开仓。缓解：默认关 + shadow mode + 滞回。

Phase 2 — 主动配对补全（sub-30s SLA）

Goal：单腿挂单超过阈值后按梯度升级——先抬价、再 FAK 吃对侧、最后兜底卖出首腿。用户已选 FAK 优先、卖出兜底。

Success metric：single_leg_duration_ms p95 < 30s、p50 < 10s；pair_broken_by_sell_events /
pair_first_leg_open_events < 20%。

Escalation ladder（时间从首腿成交起算）：

|   t    |                                                 动作
| 0–8s   | 维持常规 maker 报价不变
| 8–20s  | 每次 slot 重报时把对侧 maker 价抬 1 tick，上限 effective_pair_target() − opp_avg_cost −
|        | 1*tick（仍盈利）
| 20–45s | 发 1 次 OneShotTakerHedge { limit_price = maker_price + N*tick }（N ≤
|        | force_pair_fak_max_slippage_ticks），每 cycle 只 fire 1 次；失败则等 cycle_id 轮转
| >45s   | 若仍单腿：计算 expected_loss = (first_leg_avg_cost − first_leg_best_bid) × qty，若
|        | <force_pair_abandon_min_loss_per_share，FAK 卖首腿平仓；否则继续 maker 挂单

改动：
- coordinator.rs:92-103 PairArbStrategyConfig 追加：
  - force_pair_enabled: bool（默认 false）
  - force_pair_maker_escalate_ms: u64（默认 8000）
  - force_pair_fak_after_ms: u64（默认 20000）
  - force_pair_abandon_after_ms: u64（默认 45000）
  - force_pair_fak_max_slippage_ticks: u64（默认 3）
  - force_pair_abandon_min_loss_per_share: f64（默认 0.03）
  - force_pair_abandon_consecutive_ticks: u32（默认 2，防书抖误触）
- StrategyCoordinator 追加 pair_first_leg_escalation_phase: EscalationPhase（枚举 Normal / MakerUp / Fak /
Abandoning）、pair_last_escalation_action_at: Option<Instant>。
- coordinator_execution.rs 新增（不走传统 hedge overlay）：
  - pair_arb_execute_escalation(&mut self, inv, ub) -> bool（每 tick 在 execute_slot_market_making:672
之前调用，返回 true 表示本 tick 已接管）
  - pair_arb_dispatch_fak_completion(side, size, limit_price) — 包装 coordinator_order_io.rs:1209
dispatch_taker_intent，新 TradePurpose::PairArbForceComplete
  - pair_arb_dispatch_leg_abandon_sell(side, size, limit_price) — 用 coordinator_order_io.rs:1222
dispatch_taker_derisk 卖出路径
- messages.rs:370-385 追加 TradePurpose::{ PairArbForceComplete, PairArbLegAbandon }。
- coordinator_execution.rs:136 把 return false 改为「仅在 force_pair 未启用时」return false；启用时走新入口。
- Stats + CoordinatorObsSnapshot 追加：pair_arb_force_complete_fak_fires /
successes、pair_arb_leg_abandoned、pair_arb_escalation_entered_maker_up / fak。

关键安全互锁：
1. 在发 FAK 前先撤掉对侧 maker slot（用 dispatch_taker_intent 自带的 same-side cancel
路径，coordinator_order_io.rs:1180）避免 race double-fill。
2. FAK 前先过一遍 passes_pair_cost_guard_for_buy（用 effective_pair_target），防书动后 FAK 超预算。
3. 弃单卖出要求连续 2 tick 满足条件再 fire（force_pair_abandon_consecutive_ticks）。
4. Dry-run 路径已有（coordinator_order_io.rs:1184-1207），上线前全程 dry-run preview 验证。

Config / Flag：PM_PAIR_ARB_FORCE_PAIR_ENABLED=0（默认关）；先 BTC-UpDown 5min 小尺寸单市场开，PM_BID_SIZE=1
限 blast。紧急分级可通过单独把 force_pair_fak_after_ms 或 force_pair_abandon_after_ms 调到 u64::MAX
来只保留部分梯度。

Risk：high。这是 pair_arb 首次主动吃单/卖出。逻辑 bug 会真金白银烧。

Phase 3 — 入场闸门（pair-opportunity score）

Goal：开首腿前要求 edge、盘口稳态、OFI 非 toxic 三者同时满足。减少盲冲，降低 Phase 2 触发频次。

Success metric：pair_first_leg_open_events/hour 下降 20-40% 且 p95 配对时间、实现 PnL/pair
双改善；pair_broken_by_sell_events 比例下降。

改动：
- strategy/pair_arb.rs 在 compute_quotes:47 起点处（当 inv.net_diff.abs() < PAIR_ARB_NET_EPS 即 flat 时）新增
 should_open_first_leg(coord, inv, ub, ofi) -> Option<Side>，返回 Some(side) 才允许该侧进入 risk-increasing
报价。
- 打分：
slack        = effective_pair_target() − (mid_yes + mid_no)
vol_penalty  = max(stddev(pair_mid_sum_samples[last 30s]) * k_vol, 0)
ofi_toxic    = ofi.yes.is_toxic || ofi.no.is_toxic
book_stable  = yes_spread_ticks ≤ 3 && no_spread_ticks ≤ 3
score        = slack − vol_penalty − (ofi_toxic ? toxic_penalty : 0)
gate         = score ≥ open_score_threshold
             && book_stable && !ofi_toxic
             && !pair_arb_risk_open_cutoff_active
- 通过 gate 时，首腿选 argmin(ask_price)（先吃便宜那侧）。
- coordinator.rs:92-103 追加：
  - open_gate_enabled: bool（默认 false）
  - open_gate_shadow_mode: bool（默认 true）
  - open_score_threshold: f64（默认 0.015 = 1.5 tick）
  - vol_penalty_k / toxic_penalty
- 复用 OfiSnapshot.is_toxic/is_hot（messages.rs:293-313）。
- Stats 追加 pair_arb_open_gate_pass / fail_{by_slack,by_spread,by_ofi,by_cutoff}。

Shadow 先跑：open_gate_shadow_mode=true 时只打日志「会放行/会拦截」而不真的拦，diff 实际 open 记录 1
周再真正打开 enforce。

Config / Flag：PM_PAIR_ARB_OPEN_GATE_ENABLED=0 + PM_PAIR_ARB_OPEN_GATE_SHADOW=1。

Risk：medium。阈值过严 → 永远不开仓 → 错失 edge。Shadow-first 是主要缓解。

Phase 4 — 自动 merge 换现金（复用既有链路）

Goal：pair 成形后主动调 merge 把 1 YES + 1 NO 兑换成 $1 USDC，锁利 + 释放抵押资金。核心差异点——不持有到结算。

Success metric：任何有非零 realized_pair_qty 的 round-end 指标都显示 merged_cash_released > 0；free_balance
在 pair 成形后的 30s–60s 内回升；空转资金占比下降。

改动：
- bin/polymarket_v2.rs:1287 try_recycle_merge 附近扩展 RecycleTrigger：追加 PairSweep 变体（已有
RejectTriggered / HeadroomTriggered）。
- run_capital_recycler（约 bin/polymarket_v2.rs:1470）订阅每个市场的 CoordinatorObsSnapshot（或新
broadcast），当：
  - min(yes_qty_settled, no_qty_settled) ≥ min_full_set + fragile_buffer（要求 settled 而非 working，避免
pending fill 抖动）
  - snapshot.fragile == false
  - 两侧都没有挂单（不跟 Phase 2 FAK 抢）
  - Phase 2 状态 != Fak && != Abandoning（给 2s 缓冲）
  - 距上次 merge ≥ min_interval_secs

时请求 PairSweep 触发合并。
- 沿用 scan_mergeable_full_set_usdc、execute_market_merge（已含 dry_run
预览）、plan_merge_batch_usdc、InventoryEvent::Merge 回写通道（bin/polymarket_v2.rs:1442-1448）。
- 新 env：
  - PM_PAIR_ARB_MERGE_SWEEPER=0（默认关）
  - PM_PAIR_ARB_MERGE_MIN_FULL_SET=5.0（shares；按 $0.02 gas 估算合理，若 gas 实际更高需调）
  - PM_PAIR_ARB_MERGE_MIN_INTERVAL_SECS=30
  - PM_PAIR_ARB_MERGE_MAX_PER_ROUND=6
- 优先级：RejectTriggered > HeadroomTriggered > PairSweep。

Config / Flag：PM_PAIR_ARB_MERGE_SWEEPER=0，上线前 execute_market_merge(dry_run=true) 完整跑一遍。

Risk：high（真实链上 tx）。但底层已在生产跑（headroom recycler），本阶段只是新增触发器。主要风险——race
条件导致合并后仓位负数，已由 fragile=false && 双侧无挂单 && settled-only 三重互锁防护。

Phase 5 — （可选）同族市场 arbiter（延后）

Goal：BTC 5min UpDown 家族、Kalshi-Polymarket 对偶等同族同时刻避免两实例重复开首腿。

复用：现有 oracle-lag CrossMarketArbiter（bin/polymarket_v2.rs:6040）。扩展 PairArbFamilyLease { slug_family,
 side, ttl_ms } 消息；Phase 3 should_open_first_leg 放行前去 arbiter 抢租约。

本次先不做，等 P0–P4 全量稳定再评估。

Phase 依赖 & Flag 汇总

| Phase |                            Flag                             |       默认       |        依赖
| P0    | —（永久开启）                                               | on               | —
| P1    | PM_PAIR_ARB_DYNAMIC_TARGET_ENABLED                          | off → shadow →   | P0
|       |                                                             | on               |
| P2    | PM_PAIR_ARB_FORCE_PAIR_ENABLED                              | off              | P0
| P3    | PM_PAIR_ARB_OPEN_GATE_ENABLED +                             | shadow           | P0
|       | PM_PAIR_ARB_OPEN_GATE_SHADOW                                |                  |
| P4    | PM_PAIR_ARB_MERGE_SWEEPER                                   | off              | P2（FAK
|       |                                                             |                  | 静默期互锁）
| P5    | PM_PAIR_ARB_FAMILY_ARBITER_ENABLED                          | off              | P3

Critical Files

核心修改：
- /Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs (P1/P3)
- /Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator.rs (P0/P1/P2/P3 config + state)
- /Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs (P2 escalation entry)
- /Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_order_io.rs (P2 FAK/sell dispatch glue)
- /Users/hot/web3Scientist/pm_as_ofi/src/bin/polymarket_v2.rs (P4 sweeper trigger)

辅助修改：
- coordinator_metrics.rs (P0 观测)
- coordinator_pricing.rs (P1 动态 target helper)
- messages.rs (P2 TradePurpose 新变体)

只读复用（不改）：
- claims.rs（merge API 已就绪）
- inventory.rs（merge inventory sync 已就绪，apply_merge:299）

Verification

单元测试（沿用 strategy/pair_arb.rs:531+、coordinator_tests.rs:4500+ 的模式）：
- P0：test_pair_cycle_tracker_counts_open_and_completion、test_pair_cycle_tracker_handles_concurrent_partial_
fills
- P1：test_dynamic_pair_target_within_bounds、test_tightens_on_deep_book、test_slackens_near_expiry、test_hys
teresis_prevents_thrash
- P2：test_escalation_ladder_timings、test_abandon_requires_two_ticks、test_fak_respects_effective_pair_targe
t、test_fak_cancels_same_side_maker_first
- P3：test_open_gate_blocks_on_toxic_ofi / wide_spread / low_slack、test_open_gate_shadow_does_not_block
- P4：test_sweeper_requires_min_full_set、test_sweeper_defers_during_fak、test_sweeper_inventory_sync（对齐
inventory.rs:617 测试样式）

Backtest / 模拟：
- 扩展仓库根 backtest_pair_arb.py（若不存在则新增）输出 (t, mid_sum, pair_target_dynamic, single_leg_ms,
cycle_pnl) CSV
- 回放最近 2 周 fill，验证 P1/P3 降低 drawdown 且不杀配对率

Live validation 顺序（每步观察 48h）：
1. 合并 P0 代码 → 采集基线 p50/p95、completion_ms
2. P1 shadow（enabled=true、但 effective_pair_target() 仍读 static）→ 比较 dynamic 与 static 差异
3. P1 enforce 单市场（通过 scripts/start_markets.sh 的 per-market env）
4. P2 单市场 + PM_BID_SIZE=1（≈ $1 notional/shot）→ 盯第一个 FAK fire
5. P3 shadow → enforce
6. P4：execute_market_merge(dry_run=true) 预演 → 切真实 merge

关键监控指标（每次切换时都看）：
- single_leg_duration_ms p50/p95（P2 目标）
- pair_target_dynamic vs static 偏离分布（P1 目标）
- pair_arb_force_complete_fak_fires / pair_broken_by_sell_events 比值（P2 健康度）
- pair_arb_open_gate_fail_* 分布（P3 是否偏严）
- merged_cash_released / paired_qty_realized 比值（P4 覆盖率）
- realized edge per pair（所有阶段都看）

已确认的设计决策

1. 推进顺序：按 P0→P1→P2→P3→P4，每阶段 shadow/关闭优先、验证后再启用
2. P2 弃单优先级：FAK 优先补全，FAK 失败或盘口严重恶化才卖出首腿平仓
3. 外部交易者画像解读：两者兼有（主动补全 + 入场挑选），不确定比重 → 沿 P0→P4 顺序推进让数据说话

尚待确认（实施时再决定，不阻塞方案落地）

1. Polygon 上 CTF merge 单次 gas 成本约为多少 USDC？若 ~$0.02 则 PM_PAIR_ARB_MERGE_MIN_FULL_SET=5 合理，若
~$0.50 需调至 ≥50
2. Phase 1 动态 target 采样用 mid_yes+mid_no（平滑）还是 best_bid_yes+best_bid_no 与 best_ask_yes+best_ask_no
 的中点（更贴实盘，但噪声更大）——实施时先 mid+mid，留 env 可切
