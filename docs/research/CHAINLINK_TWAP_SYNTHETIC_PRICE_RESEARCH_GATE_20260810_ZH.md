# Chainlink TWAP 合成价格研究 Gate

更新时间：2026-08-11
研究分支：`codex/twap-boundary-shadow-migration`
本轮审计父提交：`3973aaf00a9f7323589d731fdacb0c11a997ceeb`

本项目的核心对象是 **5m round 结束后的 oracle-lag / stale-quote 交易**：在 round end 之后，用外部 source tape 形成 local synthetic price，争取在 Polymarket CLOB 盘口和 RTDS/Chainlink final label 完成重定价之前获得低延迟决策。第一道 gate 是 **price aggregation + latency lead**，不是完整 maker 账户审计。

## 1. 结论先行

| 路线 | 当前结论 | 含义 |
| --- | --- | --- |
| 直接复制 RTDS / Gamma outcome | **Strategy No-Go** | 公开 settlement label 不是 prediction alpha |
| public CLOB stale-quote 观测 | **机制输入 Go；当前样本 taker No-Go** | 9/9 胜方 token 在边界前已为 `0.99/1.00`，没有低于 `$1` 的可买 ask |
| 未扣费 taker pair / completion | **Economics No-Go** | 5m crypto fee 已经改变旧 pair-arb 的成本假设 |
| 外部 source tape → TWAP-specific predictor | **Pivot / Conditional Continue** | 只值得做一次 near-flat/final-flip 的因果回放，不得直接 promotion |
| maker + fair-value + rebate | **第二阶段 Conditional Go** | 必须先有 fill、adverse selection、inventory 和 rebate 证据 |
| live orders / credentials / service promotion | **No-Go** | 本 Gate 不授予任何 live authority |

这不是整个项目失败。工程底座（外部价格 tape、RTDS exact tick、CLOB event tape、Gamma public label、终态证据）是正确的；但原机制论证把**旧边界 spot 聚合器**当成了 **30 秒 TWAP estimator**，又把双 token 的任意首次 reprice 当成胜方可成交窗口。当前方向必须收窄为 **Pivot / Conditional Continue**：先证明“TWAP-specific candidate 在边界后产生时，胜方 ask 仍可买”，再谈成本或执行。私有 queue/fill/ledger 仍是 live promotion 的后置 gate，不是本轮盘后时延研究的前置条件。

## 2. 当前证据分层

### 2.1 Chainlink TWAP boundary shadow

固定修复后的 capture 已经证明了工程链路可以工作，但早期低分辨率样本仍有 CLOB 断线：

- Gamma round 起点按 slug 正确绑定，14/14 个 boundary 对齐；
- 14 个 boundary 中仅 8 个在 capture 内观察到 public Gamma outcome；
- `candidate_match_count=5`、`scored_settlement_count=8`，但严格完整覆盖且有 outcome 的样本只有 `n=1`；
- 28 个 token boundary rows 中只有 14 个有 best ask；
- boundary 时没有 token 同时具备 best bid 和 best ask，不能构成 orderable public book；
- public ask depth 不能证明私有队列位置、成交概率或真实残仓成本。

旧 capture 的 `candidate_match_count=5/8` 只能作为 public-label 复核，且该目录记录了 4 次 CLOB `socket_closed`/reconnect，不能作为 gap-free latency 或策略准确率样本。

因此该 lane 是 **settlement label / latency / stale-book observation infrastructure**；它本身不是 alpha，但正好用于测量盘后交易所需的领先窗口。

前一轮 collector 的窄修复在 `d30b7f7a0`；当前分支在此基础上由
`03d8632dc` 继续收紧终态核验，并补上 Gamma `twap-30s/60s` 显式 stream
URL 的窗口映射：

- 按 RTDS 要求发送 5 秒 heartbeat `PING`；
- 用 `full_accuracy_value` 的 exact E18 整数比较 candidate side；
- 保留 boundary observation 中的 exact start/end label。

### 2.2 Local synthetic price lane

本线的目标是用 Binance、Coinbase、OKX、Bybit、Hyperliquid 等外部 tape，在 round end 之前估算 Chainlink 的最终 TWAP，而不是复制 RTDS 传输链路。设计见[外部 tape 补齐计划](../LOCAL_AGG_EXTERNAL_TAPE_BACKFILL_PLAN_ZH.md)。

已有历史 replay 曾出现较好的 gated error/side 结果，但它们评估的是 round open/close 附近的边界 spot 聚合，不是官方 30 秒 TWAP 的时间积分。当前 Rust 热路径订阅的是 `crypto_prices_chainlink`；selector 选择边界点或 weighted close point，而不是对最后 30 秒 event-time tape 做冻结的 TWAP 积分。因此以下历史数字只能作为多源边界价格工程先验：

- accepted rows `1114`；
- accepted side errors `3`；
- accepted max error `8.152221bps`；
- `>=5bps` tail `44`。

这些结果说明多源 tape 和 gate 有工程价值，但不能证明 TWAP side。旧 convergence 记录的 local p50 `25–42ms` 也不是严格的实测 TWAP ready latency：dataset builder 的 `--cap-local-ready-lag-ms` 会把缺失或过晚的 `local_ready_ms` 改写成 `round_end + cap`，属于离线 deadline simulation。它不能与 RTDS 的实测接收延迟直接相减后宣称 lead。

正确下一步不是继续优化旧 selector，而是在已有 event-time source tape 上新增独立的 **TWAP-specific offline estimator**，冻结窗口、source cutoff、缺失行为和 ready time；不修改旧 Rust 热路径。只有该 estimator 与同 round 胜方/candidate-side ask 的存活窗口完成因果 join 后，才能谈 lead。

### 2.3 Pair/completion / maker research

Pair-Gated Tranche V1.1 已经显式承认三个关键假设仍需裁决：公开样本是否代表常态、我方是否能像参考账户一样 maker 到 first leg、以及 7-market 资金是否能靠 merge 周转。见[Pair-Gated Tranche V1.1](../strategies/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md)。

因此历史 L2 回放或 public-account 研究只能是 research hypothesis，不能直接升级为 live economics。

## 3. Chainlink TWAP 变化的正确解释

根据[官方 Chainlink TWAP 文档](https://docs.polymarket.com/market-data/chainlink-twap)：

- 30s/60s 是 lookback window，不是 publication cadence；
- `payload.timestamp` 是 Chainlink observation time，外层 timestamp 是 RTDS publisher time；
- `full_accuracy_value` 是 exact signed E18 value；
- RTDS 没有断线后的历史 replay；
- Chainlink 的采样边界、权重、rounding 和 missing-input 行为没有完整公开，不能自行声称复刻 settlement；本地合成器应以 side/保守价格区间和公开 outcome label 做实证校准，而不是宣称 bit-for-bit 复刻。

所以 RTDS 最适合做 **label、benchmark 和 latency reference**。盘后交易真正要验证的是：

```text
last-30s external event-time tape
        ↓
preregistered TWAP-specific side/interval estimate
        ↓
candidate_ready_ms + candidate token mapping
        ↓
candidate-side best ask/depth and ask withdrawal/raise time
```

核心量化对象是：

```text
usable_window_ms = candidate_side_ask_end_ms - candidate_ready_ms
```

其中必须先要求 `candidate_ready_ms >= round_end_ms`（纯盘后路径）且该时刻存在价格低于冻结上限的 candidate-side ask。RTDS final 到达与双 token 任意 reprice 只作诊断；Gamma outcome 只作离线标签，不进入热路径。

### 3.1 官方实时数据契约审计（2026-08-10）

官方 TWAP 页和 Market Stream 页对本项目有四个不可省略的字段约束：

| 官方字段/事件 | 在本项目中的正确解释 | 研究实现要求 |
| --- | --- | --- |
| `payload.timestamp` | Chainlink observation time | 作为观测年龄与 round boundary 对齐字段 |
| RTDS 外层 `timestamp` | RTDS publisher submission time | 与本地 `receive_ms` 分开记录，不能当作到达时间 |
| `payload.value` / `full_accuracy_value` | exact decimal / signed E18 fixed-point | 以字符串或整数比较；不能用 JS `number` 决定 side |
| `windowSeconds` / `window_s` | 30s/60s lookback window | 必须有显式 symbol+window 映射；不能从更新频率推断 |
| CLOB `book` | 完整公开 book snapshot | 记录 top-of-book、top-5、深度和本地接收时间 |
| CLOB `price_change` / `best_bid_ask` | 公开 quote transition | 记录每个事件的 event timestamp、本地接收时间、best bid/ask |

官方还明确：RTDS 订阅从下一次更新开始，断线后没有 snapshot/history/replay；直接客户端需每 5 秒发送 `PING` 并自行 reconnect/resubscribe。CLOB Market WebSocket 也要求应用层 heartbeat，并提供 `price_change` 与可选 `best_bid_ask` 事件。这意味着“没有 tick”不能被静默当作价格不变，必须按 round 标记为 gap 或 incomplete。

这次窄范围 collector 修复把上述契约落实到研究输出：

- `twap_ticks.jsonl` 同时保留 `observation_ts`、`publisher_ts`、`receive_ms`、`value_decimal` 和 `full_accuracy_value`；
- `book_events.jsonl` 默认取消每 token 1 秒节流，兼容官方新旧 CLOB event shape，记录 `event_ts_ms`、`event_to_receive_ms`、事件级 best bid/ask，并开启 `custom_feature_enabled`；
- CLOB 连接补 10 秒 `PING`，RTDS 继续按官方 5 秒 `PING`；
- RTDS 增加逐 stream silence watchdog；socket 即使仍为 open，只要 tick 超过 freshness threshold 就记录 gap、关闭并重连；
- `boundary_observations.jsonl` 增加 `round_end_detection_lag_ms`、start/end tick timing 和 5 秒 freshness gate；窗口不明或任一边界 tick 不新鲜时 `candidate_side=unknown`；
- `summary.json` 新增 RTDS observation/publisher 到本地接收的分布，以及按 boundary/token 对齐的 first quote/reprice lag 诊断。

注意：`round_end_detection_lag_ms` 仍是 1 秒 boundary poll 的观测诊断，不是交易时延；`first_quote_reprice_receive_lag_ms` 混合胜负两个 token，也不是可交易窗口。下一道 gate 必须使用 **candidate-side ask at candidate-ready**，并与 `local_ready_ms` 在同一 round、同一主机时钟口径下 join。

这仍不等于 local aggregator。collector 没有代替 Binance/Coinbase/OKX/Bybit/Hyperliquid tape，也没有产生 `local_ready_ms`；它只是把“合成候选 → RTDS benchmark → CLOB public reprice”的因果 join 所需观测字段补齐。下一轮 capture 必须使用这份高分辨率 JSONL；旧的 1 秒节流 capture 只能用于 connectivity/metadata 复核，不能用于亚秒 stale-quote 结论。

### 2.1.1 高分辨率终态验收（EC2，2026-08-10 UTC）

目录：`/home/ubuntu/b_strategy_staging/pm_as_ofi/twap_boundary_shadow_capture_reprice_20260810T155731Z/`。
该 run 使用的是修复前的 collector source commit
`ded5856dff4a9869285b81962745a443c11b2c9e`（代码 hash
`69e642a71549a24a22cf1062b3d6f922aa5b377aec1ec70cc66b3066a7f03d2e`），
因 `book_events.jsonl` 达到 1,489,752,620 bytes，原 collector 在 summary
阶段触发 Node `ERR_STRING_TOO_LONG`；raw JSONL 完整保留，随后由 recovery
finalizer（hash `d7a008bff71686a3f132bc0c68dfe0d2ddda21c51a74cb3225f82bb936930cea`）
重建终态。该工程故障不是策略失败，但必须计入证据链。

终态 verifier 结果为 `CONDITIONAL_RESEARCH_INSUFFICIENT_EVIDENCE`。原 verifier 通过了文件、round 和 exact-value 契约，但**遗漏了 RTDS silent-tail freshness 契约**：

修正后的 `terminal_verification.json` sha256 为
`3a0aaff28f5819bc839b6f79bd2bd45571ff6a927cc927bab35debfa62c356e3`。

- `terminal=true`、`mode=no-submit`、`live_orders_submitted=0`、`credentials_loaded=false`、`open_runs=[]`；
- 102 个 metadata rows、4,769 个 RTDS ticks、1,249,130 个 CLOB event rows、14 个 boundary、9 个 Gamma public observations；
- slug 派生的 5m round `102/102` 对齐；4,769/4,769 tick 保留 `observation_ts` 与 exact decimal/E18；manifest hash/bytes/lines 全部通过；
- RTDS reconnect `0`，但最后一条 RTDS tick 于 `16:03:59.371Z` 到达，run 到 `16:07:59.204Z` 才退出，silent tail 为 `239,833ms`；socket 仍显示 open，证明“无 reconnect”不等于无 gap；
- CLOB reconnect `1`，gap `1`（`2026-08-10T16:02:27.324Z`，`socket_closed`）。因此不能作全样本 gap-free 的 latency/strategy 结论，受影响 round 必须标记 incomplete。

### 2.1.2 这次终态数据实际说明了什么

时间口径必须分开：

| 量 | 观测结果 | 正确解释 |
| --- | ---: | --- |
| boundary poll lag | p50 `311ms`，p95/max `359ms` | 1 秒 poll 的 round-end 检测诊断，不是交易延迟 |
| RTDS observation → receive | p50 `1,643ms`，p95 `2,275ms`，max `3,014ms` | Chainlink observation 到 EC2 收到 RTDS 的延迟 |
| RTDS publisher → receive | p50 `237ms`，p95 `382ms`，max `734ms` | RTDS 发布到 EC2 收到的传输/处理延迟 |
| CLOB event → receive | p50 `10ms`，p95 `40ms`，p99 `112ms`，max `2,479ms` | 公开 CLOB event 的本地接收延迟 |
| first post-boundary quote | p50 `28ms`，p95/max `53/66ms` | 公开 quote 出现的接收滞后 |
| first quote reprice | p50 `3,519ms`，p90/max `111,908/112,224ms` | 双 token 任意 quote 变化；**不是胜方/candidate-side 可交易窗口** |

边界 L2 的旧汇总把胜负 token 混在一起：`best ask=0.01` 和数千 shares 深度主要是**败方 token**，不能支持“边界后仍可低价买到胜方”。按 Gamma outcome 事后映射胜方 token 后，9/9 round 在边界前最后公开 quote 均为 `best_bid=0.99, best_ask=1.00`；最后 snapshot 到边界前 3–522ms，期间没有 CLOB gap。对 `<0.90/<0.95/<0.98/<0.99/<1.00` 五个阈值，边界可买数均为 `0/9`，边界后 120 秒首次可买数也均为 `0/9`。这给当前样本的**纯盘后 taker stale-winner-ask**一个 scoped No-Go。

可复现报告：`scripts/audit_twap_boundary_winner_ask_window.mjs`；EC2 输出
`winner_ask_window_audit.json` 的 sha256 为
`d1f6f0d82239d14288ebeb5186856d2ef3abf7c8b8e7c4501204d1acdbaf2725`。

原始 metadata 的 `twap_window_s` 因旧解析器没有识别当前 Gamma description 中的
`...twap-30s-streams` 而为 null。该描述是显式窗口元数据，不是从更新频率推断。只读
后处理最初报告 9/9 `candidate_side` 与 Gamma public outcome 一致，但 freshness 审计后发现这 **9/9 全部不合格**：前 6 个 round 的 start tick 晚了 `180,000ms`，后 3 个 round 的 end tick 早了 `62,000–64,000ms`，没有一条同时具备 5 秒内的 start/end tick。

可复现的派生报告由
`scripts/audit_twap_boundary_shadow_candidate_labels.mjs` 生成，当前 EC2 输出为
`/home/ubuntu/b_strategy_staging/pm_as_ofi/twap_boundary_shadow_capture_reprice_20260810T155731Z/posthoc_candidate_label_audit.json`
（报告 hash `69133c846db0164776083557f4a9b20573496188d6eac915722116bb78b78e81`）。严格 scored=`0/9`、match rate=`null`；原 9/9 仅保留为 `diagnostic_unqualified`，不得再称准确率。当前分支已把显式 URL 映射、边界 freshness gate 和 RTDS silence watchdog 补进 collector/verifier。

真正使 verifier 不能升级的原因是 **三重缺口**：RTDS 边界覆盖不完整、胜方 ask 已在边界前重定价、以及 causal join 缺失。capture 没有
`source_timestamp_cutoff_ms`、`local_ready_ms`、`local_candidate_price/side`、
`decision_deadline_ms`，且 CLOB 有 gap。因此本 run 只能授予**原始数据链路/恢复工程价值**；不能授予完整 shadow pass、策略经济学或 live Go。

## 4. 后置成本与执行约束

费用不是第一道 latency/aggregation gate，但在确认存在 stale-quote lead 后必须进入净值评估。

官方[费用文档](https://docs.polymarket.com/trading/fees)当前给出的 crypto `feeRate=0.07`，公式为：

```text
fee = shares × feeRate × p × (1 - p)
```

按当前表格，100 shares 在 50¢ 的一腿约为 `$1.75`，两腿约 `$3.50`，尚未计 spread、slippage、latency 和残仓。官方 Predictions changelog 对 5m 初始曲线曾写过 50% 概率峰值 `1.56%`，两页存在口径差异；研究实现必须按每个 market 的实际 fee configuration 取值，不能硬编码其中一个数字。

Maker 不收平台 maker fee，但[Maker Rebates 文档](https://docs.polymarket.com/programs/maker-rebates)明确要求订单实际增加流动性并被成交；Crypto rebate pool 参数为 20%，按市场内竞争和 fee-equivalent 分配，不是每笔成交的固定保证收益。

Public market WebSocket 只能提供公开 book、price change 和 last-trade 等事件，见[官方实时市场数据文档](https://docs.polymarket.com/market-data/realtime-data#market-stream)。它不提供我们的 queue priority、order acceptance、partial fill、cancel race 或 owner ledger。

## 5. 下一道冻结研究 Gate

### Phase A：先用已有 immutable data

在新 capture 前，必须确认现有数据是否已经具备：

1. 每个 round 的 `round_start_ts / round_end_ts`；
2. 决策时刻之前的外部 source event/receive tape；
3. exact RTDS final value、`payload.timestamp` 和本地 receive time；
4. CLOB best bid/ask、top-5 depth、quote/reprice receive time；
5. local aggregator ready time、candidate price/side 和 source gap；
6. market-specific fee configuration，作为第二阶段成本字段；
7. reconnect、late tick 和 missing round 标记。

不能使用 round end 之后的 external tick 或 public outcome 作为 feature。

当前 local-agg challenger 的状态为 `action=no_current_run`、`eval_rows=0`；因此旧 `T+300ms`、coverage 和 bps 数字只能作为历史工程线索，不能当作当前 Gate 证据。高分辨率 EC2 capture 与历史 local-agg/source logs 只有在**同一 round、同一 clock domain、具备最后 30 秒逐事件 tape**时才有 join 资格；不能用不重叠日期或离线 cap 伪造 `local_ready_ms`。不能再用 Gamma settlement match 代替这条链路。

当前下一道最小 Gate 是：

1. 用已有 source tape 做 outcome-blind 工程 gate：实现最后 30 秒 event-time TWAP-specific estimator，冻结 source、权重、cutoff、missing action、candidate interval/side；它只能验证机制和接口。
2. 只在存在同 round 的 RTDS/CLOB tape 时回放；candidate ready 后按 **candidate token** 查询 ask/size，禁止使用双 token first reprice。主筛选样本是 near-flat 和最后数秒发生 side flip 的 round，强趋势 round 只作负控。
3. 若现有 immutable 数据没有同 round causal fields，才授权一次 rolling 24h、no-submit prospective capture，同时采集 source candidate、RTDS 与 CLOB。首日若 candidate-side ask survival 近乎为零，直接 scoped No-Go；只有首日通过冻结门槛才授权第二独立日期。

### Phase B：预注册 synthetic candidate

读取 label 之前冻结：

- asset universe；
- round horizon；
- decision deadline；
- source inclusion/exclusion；
- source timestamp cutoff；
- candidate side / price mapping；
- `local_ready_ms - round_end_ms` 的 signed 值；若候选在 round end 前已准备好，标记 `preclose_ready=true`，不能把它伪装成 round end 后的计算延迟；
- minimum margin；
- missing-data action（通常为 skip，而不是下注）；
- fee、spread、slippage、partial-fill、residual 和 latency stress。

`candidate_side` 只能作为预测输出；Gamma outcome 只能作为 public label；RTDS close 不能反向进入 feature。

### Phase C：盘后价格/时延 Gate

每个 accepted candidate 必须同时报告：

- local ready latency 的 p50/p95/max；
- `candidate_ready_ms - round_end_ms` 以及 candidate-ready 时刻的 ask/size；
- `usable_window_ms`（candidate-side ask 撤走/提价时间减 candidate ready）；
- 在每个 causal cutoff 下的 side correctness；
- absolute/signed TWAP error；
- CLOB stale quote 的持续时间、best ask/bid 和 top-1/top-5 可见深度；
- source gap、coverage、reconnect 和 late-tick rate；
- 第二阶段再加入 taker fee、spread、slippage、fill probability、partial fill、rebate 和 residual cost。

单独的 `side=0`、`close_diff_bps<5`、RTDS lead 或败方 public depth 都不能触发 promotion。第一道 Research Go 要求是：在严格 causal cutoff 下，TWAP-specific candidate 具有可接受 side/interval 质量，且 candidate ready 时胜方/candidate-side 仍有低于冻结限价、具有最低深度的 ask。完成这道 gate 后，才进入 fee-inclusive decision value；最终 live promotion 仍需真实订单路径证据。

### Phase D：仅在字段缺失时做一次 prospective capture

如果 Phase A 证明关键 causal field 缺失，才允许一次 bounded、no-submit、约 24h 的 prospective capture。它必须同时采集：

- 外部 source 的 `event_ts_ms / receive_ms / price`；
- RTDS `payload.timestamp / publisher timestamp / receive_ms`；
- CLOB candidate-token `book / price_change / best_bid_ask`；
- 同一主机上的 capture start/exit、source gap、candidate ready 和 round key。

短 capture 只做 producer compatibility、字段覆盖和断线工程验收；它只能验证实时可用性、覆盖率、断线行为和初步 conservative decision value，不能单独授予 formal strategy Go。首个合格日期仍必须通过 frozen candidate-side ask survival gate，才允许第二个独立日期。

## 6. 明确停止条件

以下任一条件成立，直接停止纯盘后 direct-TWAP / synthetic-lag 路线：

- edge 只在 RTDS/Gamma public label 出现之后才出现；
- 在 near-flat/final-flip round 中，candidate ready 时胜方/candidate-side ask 仍近乎总是 `1.00` 或已撤空；
- local ready 没有稳定早于 candidate-side ask 撤走/提价；
- synthetic predictor 在严格 causal cutoff 下不能稳定给出 side/price convergence；
- stale-quote window 只存在于极低 coverage 或 hindsight filtering 样本；
- 在第二阶段加入费用和合理滑点后，剩余 margin 不足以覆盖交易成本。

当前允许继续的唯一主线是：

```text
external-source TWAP-specific predictor
→ near-flat/final-flip causal replay
→ candidate-side ask survival measurement
→ fee-inclusive decision-value stress
→ separate private-truth requirement before live
```

本文件不授权 credential load、order submit/cancel、sign、redeem、funding、shared ingress/live service mutation 或旧 Rust 热路径修改。
