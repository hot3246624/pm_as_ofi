# Chainlink TWAP 合成价格研究 Gate

更新时间：2026-08-10  
研究分支：`codex/twap-boundary-shadow-migration`  
collector 修复基线：`d30b7f7a06eb30e93e032bc1b4432910fdee7ec5`

## 1. 结论先行

| 路线 | 当前结论 | 含义 |
| --- | --- | --- |
| 直接复制 RTDS / Gamma outcome | **Strategy No-Go** | 公开 settlement label 不是 prediction alpha |
| 纯 public CLOB L2 套利 | **Economics No-Go** | 没有私有 queue、fill、partial fill 和账户账本真相 |
| 未扣费 taker pair / completion | **Economics No-Go** | 5m crypto fee 已经改变旧 pair-arb 的成本假设 |
| 外部 source tape → synthetic TWAP predictor | **Conditional Research Go** | 只值得做 bounded no-submit 研究，不得直接 promotion |
| maker + fair-value + rebate | **第二阶段 Conditional Go** | 必须先有 fill、adverse selection、inventory 和 rebate 证据 |
| live orders / credentials / service promotion | **No-Go** | 本 Gate 不授予任何 live authority |

这不是整个项目失败。项目已经形成了可复用的行情、回放、费用和安全闸门；尚未成立的是“这些 public/readiness 证据可以转换成稳定净收益”的经济命题。

## 2. 当前证据分层

### 2.1 Chainlink TWAP boundary shadow

固定修复后的 capture 已经证明了工程链路可以工作：

- Gamma round 起点按 slug 正确绑定，14/14 个 boundary 对齐；
- 14 个 boundary 中仅 8 个在 capture 内观察到 public Gamma outcome；
- `candidate_match_count=5`、`scored_settlement_count=8`，但严格完整覆盖且有 outcome 的样本只有 `n=1`；
- 28 个 token boundary rows 中只有 14 个有 best ask；
- boundary 时没有 token 同时具备 best bid 和 best ask，不能构成 orderable public book；
- public ask depth 不能证明私有队列位置、成交概率或真实残仓成本。

因此该 lane 是 **label / latency / book-observation infrastructure**，不是已验证的 alpha。

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

这说明当前 selector/gate 可以通过“过滤掉不确定样本”提高安全性，但还没有证明在足够覆盖率下产生可交易净收益。现有 evaluator 主要评估 `close_diff_bps` 和 side，不等于 fee-inclusive CLOB PnL。

### 2.3 Pair/completion / maker research

Pair-Gated Tranche V1.1 已经显式承认三个关键假设仍需裁决：公开样本是否代表常态、我方是否能像参考账户一样 maker 到 first leg、以及 7-market 资金是否能靠 merge 周转。见[Pair-Gated Tranche V1.1](../strategies/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md)。

因此历史 L2 回放或 public-account 研究只能是 research hypothesis，不能直接升级为 live economics。

## 3. Chainlink TWAP 变化的正确解释

根据[官方 Chainlink TWAP 文档](https://docs.polymarket.com/market-data/chainlink-twap)：

- 30s/60s 是 lookback window，不是 publication cadence；
- `payload.timestamp` 是 Chainlink observation time，外层 timestamp 是 RTDS publisher time；
- `full_accuracy_value` 是 exact signed E18 value；
- RTDS 没有断线后的历史 replay；
- Chainlink 的采样边界、权重、rounding 和 missing-input 行为没有完整公开，不能自行声称复刻 settlement。

所以 RTDS 最适合做 **label、benchmark 和 latency reference**。真正可能产生 edge 的位置是：

```text
external pre-boundary tape
        ↓
synthetic estimate before round end
        ↓
Polymarket CLOB mispricing / maker quote / completion decision
        ↓
fee + spread + fill + inventory adjusted decision value
```

## 4. 费用与执行硬约束

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
2. exact RTDS open/close value 和 observation timestamp；
3. 决策时刻之前的外部 source event/receive tape；
4. 决策时刻的 CLOB best bid/ask 和至少 top-5 depth；
5. market-specific fee configuration；
6. source gap、reconnect、late tick 和 missing round 标记。

不能使用 round end 之后的 external tick 或 public outcome 作为 feature。

### Phase B：预注册 synthetic candidate

读取 label 之前冻结：

- asset universe；
- round horizon；
- decision deadline；
- source inclusion/exclusion；
- source timestamp cutoff；
- candidate side / price mapping；
- minimum margin；
- missing-data action（通常为 skip，而不是下注）；
- fee、spread、slippage、partial-fill、residual 和 latency stress。

`candidate_side` 只能作为预测输出；Gamma outcome 只能作为 public label；RTDS close 不能反向进入 feature。

### Phase C：净 decision-value gate

每个 accepted candidate 必须同时报告：

- side correctness；
- absolute/signed TWAP error；
- decision latency；
- public book 可见性和 top-1/top-5 支持量；
- assumed fill probability 和 partial fill；
- taker fee 或 maker rebate model；
- residual inventory / unwind cost；
- conservative fee-inclusive net decision value。

单独的 `side=0`、`close_diff_bps<5`、public depth 或 simulated PnL 都不能单独触发 promotion。必须在时间外样本和成本 stress 下仍保留正的保守净 decision value；否则立即 No-Go。

### Phase D：仅在字段缺失时做一次 prospective capture

如果 Phase A 证明关键 causal field 缺失，才允许一次 bounded、no-submit、约 24h 的 prospective capture。它只能验证实时可用性、覆盖率、断线行为和初步 conservative decision value，不能单独授予 formal strategy Go。

## 6. 明确停止条件

以下任一条件成立，直接停止 direct-TWAP 路线：

- edge 只在 RTDS/Gamma public label 出现之后才出现；
- synthetic predictor 在严格 causal cutoff 下不能稳定给出 margin；
- fee/slippage/fill/residual stress 后净值不为正；
- accepted 样本依赖极低 coverage 或 hindsight filtering；
- public book 看起来有深度，但无法建立私有 fill/queue/ledger bridge。

当前允许继续的唯一主线是：

```text
external-source synthetic predictor
→ no-submit causal replay
→ public execution-proxy stress
→ separate private-truth requirement
```

本文件不授权 credential load、order submit/cancel、sign、redeem、funding、shared ingress/live service mutation 或旧 Rust 热路径修改。
