# Chainlink TWAP 合成价格研究 Gate

更新时间：2026-08-10  
研究分支：`codex/twap-boundary-shadow-migration`  
collector 修复基线：`d30b7f7a06eb30e93e032bc1b4432910fdee7ec5`

本项目的核心对象是 **5m round 结束后的 oracle-lag / stale-quote 交易**：在 round end 之后，用外部 source tape 形成 local synthetic price，争取在 Polymarket CLOB 盘口和 RTDS/Chainlink final label 完成重定价之前获得低延迟决策。第一道 gate 是 **price aggregation + latency lead**，不是完整 maker 账户审计。

## 1. 结论先行

| 路线 | 当前结论 | 含义 |
| --- | --- | --- |
| 直接复制 RTDS / Gamma outcome | **Strategy No-Go** | 公开 settlement label 不是 prediction alpha |
| public CLOB stale-quote 观测 | **Research Input Go** | 用于量化盘后错价窗口；不能单独宣称 live PnL |
| 未扣费 taker pair / completion | **Economics No-Go** | 5m crypto fee 已经改变旧 pair-arb 的成本假设 |
| 外部 source tape → synthetic TWAP predictor | **Conditional Research Go** | 只值得做 bounded no-submit 研究，不得直接 promotion |
| maker + fair-value + rebate | **第二阶段 Conditional Go** | 必须先有 fill、adverse selection、inventory 和 rebate 证据 |
| live orders / credentials / service promotion | **No-Go** | 本 Gate 不授予任何 live authority |

这不是整个项目失败。项目已经形成了可复用的外部价格源、local aggregator、RTDS label 和 CLOB book 观测链路；尚未成立的是“local lead 足以在成本和真实订单路径下稳定转化为净收益”的后置命题。私有 queue/fill/ledger 是 live promotion 的后置 gate，不应阻塞第一轮 latency/aggregation 研究。

## 2. 当前证据分层

### 2.1 Chainlink TWAP boundary shadow

固定修复后的 capture 已经证明了工程链路可以工作：

- Gamma round 起点按 slug 正确绑定，14/14 个 boundary 对齐；
- 14 个 boundary 中仅 8 个在 capture 内观察到 public Gamma outcome；
- `candidate_match_count=5`、`scored_settlement_count=8`，但严格完整覆盖且有 outcome 的样本只有 `n=1`；
- 28 个 token boundary rows 中只有 14 个有 best ask；
- boundary 时没有 token 同时具备 best bid 和 best ask，不能构成 orderable public book；
- public ask depth 不能证明私有队列位置、成交概率或真实残仓成本。

因此该 lane 是 **settlement label / latency / stale-book observation infrastructure**；它本身不是 alpha，但正好用于测量盘后交易所需的领先窗口。

当前 collector 的窄修复在 `d30b7f7a0`：

- 按 RTDS 要求发送 5 秒 heartbeat `PING`；
- 用 `full_accuracy_value` 的 exact E18 整数比较 candidate side；
- 保留 boundary observation 中的 exact start/end label。

### 2.2 Local synthetic price lane

本线的目标是用 Binance、Coinbase、OKX、Bybit、Hyperliquid 等外部 tape，在 round end 之前估算 Chainlink 的最终 TWAP，而不是复制 RTDS 传输链路。设计见[外部 tape 补齐计划](../LOCAL_AGG_EXTERNAL_TAPE_BACKFILL_PLAN_ZH.md)。

已有历史 replay 曾出现较好的 gated error/side 结果，但后续主 run 仍记录过：

- accepted rows `1114`；
- accepted side errors `3`；
- accepted max error `8.152221bps`；
- `>=5bps` tail `44`。

这说明当前 selector/gate 可以通过“过滤掉不确定样本”提高安全性，但仍需把研究重点放回盘后交易的两个主指标：**local ready latency** 和 **在 CLOB 重定价之前的 final-side/price convergence**。旧 convergence 记录曾观察到 local p50 约 `25–42ms`、p95/max 约 `215–273ms`，而 RTDS p50 约 `1.3–1.4s`、部分 run 的 p95/max 达到约 `6.6s`；这正是本项目最有价值的机制证据。现有 evaluator 还没有把这些 lead milliseconds 与 CLOB stale-quote window 逐 round 对齐，因此下一步应先完成 latency/price replay，再做 fee-inclusive PnL。

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
external pre-boundary tape
        ↓
synthetic estimate before round end
        ↓
Polymarket CLOB mispricing / maker quote / completion decision
        ↓
usable stale-quote window before public repricing
```

核心量化对象是：

```text
lead_ms = public_final_or_reprice_receive_ms - local_ready_ms
```

其中 `public_final_or_reprice_receive_ms` 至少分别记录 RTDS final 到达和 CLOB 首次明显重定价；Gamma outcome 只作离线标签，不进入热路径。

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
- `boundary_observations.jsonl` 增加 `round_end_detection_lag_ms` 和 start/end tick timing，未能从 Gamma 明确解析 30/60 窗口时不再默认 30，而是 `candidate_side=unknown`；
- `summary.json` 新增 RTDS observation/publisher 到本地接收的分布，以及按 boundary/token 对齐的 first quote/reprice lag 诊断。

注意：`round_end_detection_lag_ms` 仍是 1 秒 boundary poll 的观测诊断，不是交易时延；真正用于下一道 gate 的是逐事件 `first_quote_reprice_receive_lag_ms`，并且必须与 `local_ready_ms` 在同一 round、同一主机时钟口径下 join。

这仍不等于 local aggregator。collector 没有代替 Binance/Coinbase/OKX/Bybit/Hyperliquid tape，也没有产生 `local_ready_ms`；它只是把“合成候选 → RTDS benchmark → CLOB public reprice”的因果 join 所需观测字段补齐。下一轮 capture 必须使用这份高分辨率 JSONL；旧的 1 秒节流 capture 只能用于 connectivity/metadata 复核，不能用于亚秒 stale-quote 结论。

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

当前 local-agg challenger 的状态文件为 `action=no_current_run`、`eval_rows=0`；因此 5 月的 `T+300ms`、coverage 和 bps 数字只能作为历史工程线索，不能当作本次 Gate 的当前通过证据。推进顺序必须是：先用已有 immutable logs 做 schema/causal 可用性审计，再用修复后的高分辨率 CLOB collector 做一次 bounded EC2 no-submit capture，最后把同一 round 的 local candidate 与 public reprice 做 join；不能再用 Gamma settlement match 代替这条链路。

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
- `lead_ms` 相对 RTDS final 和 CLOB 首次重定价的分布；
- 在每个 causal cutoff 下的 side correctness；
- absolute/signed TWAP error；
- CLOB stale quote 的持续时间、best ask/bid 和 top-1/top-5 可见深度；
- source gap、coverage、reconnect 和 late-tick rate；
- 第二阶段再加入 taker fee、spread、slippage、fill probability、partial fill、rebate 和 residual cost。

单独的 `side=0`、`close_diff_bps<5` 或 public depth 都不能单独触发 promotion；第一道 Research Go 要求是：在严格 causal cutoff 下，local estimator 稳定早于 RTDS/CLOB public repricing，并保留足够 coverage。完成这道 gate 后，才进入 fee-inclusive decision value；最终 live promotion 仍需真实订单路径证据。

### Phase D：仅在字段缺失时做一次 prospective capture

如果 Phase A 证明关键 causal field 缺失，才允许一次 bounded、no-submit、约 24h 的 prospective capture。它只能验证实时可用性、覆盖率、断线行为和初步 conservative decision value，不能单独授予 formal strategy Go。

## 6. 明确停止条件

以下任一条件成立，直接停止 direct-TWAP / synthetic-lag 路线：

- edge 只在 RTDS/Gamma public label 出现之后才出现；
- local ready 没有稳定早于 RTDS final 或 CLOB 首次重定价；
- synthetic predictor 在严格 causal cutoff 下不能稳定给出 side/price convergence；
- stale-quote window 只存在于极低 coverage 或 hindsight filtering 样本；
- 在第二阶段加入费用和合理滑点后，剩余 margin 不足以覆盖交易成本。

当前允许继续的唯一主线是：

```text
external-source synthetic predictor
→ no-submit causal latency/price replay
→ CLOB stale-quote window measurement
→ fee-inclusive decision-value stress
→ separate private-truth requirement before live
```

本文件不授权 credential load、order submit/cancel、sign、redeem、funding、shared ingress/live service mutation 或旧 Rust 热路径修改。
