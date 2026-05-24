# B55 / CE25 Public Entry Join Audit - 2026-05-24

## Decision

`KEEP_B55_CE25_PUBLIC_ENTRY_JOIN_AUDIT_READY_REAL_INPUTS_UNKNOWN`.

本轮完成的是 public entry join 审计工具和 fixture 合同验证，不是策略上线证据。当前 public export 只能提供市场级 summary/proxy，不能提供逐笔 entry row 与 fair-probability source 的真实 join，因此真实输入状态仍是 `UNKNOWN_PUBLIC_ENTRY_JOIN_FIELDS_MISSING`。

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_public_entry_join_audit_20260524T062245Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_public_entry_join_audit_smoke_20260524T062245Z/manifest.json`

## Public Joinability

当前已能从 public export 识别 BTC/ETH、15m/1h_or_named、入场窗口 `-900s..-60s` 的市场级候选：

- b55: market summary rows `739`，allowed BTC/ETH 15m/1h rows `192`，parseable boundary rows `154`，entry-window intersection rows `153`
- ce25: market summary rows `640`，allowed BTC/ETH 15m/1h rows `153`，parseable boundary rows `153`，entry-window intersection rows `153`

这些 rows 只能证明存在可 join 的市场/时间窗口，不等于逐笔交易有 fair edge。

## Missing Real Fields

真实评估仍缺：

- per-trade outcome
- per-trade size
- per-trade fee USDC
- liquidity role
- fair_probability
- fair_probability_uncertainty
- edge_after_fee_and_uncertainty
- source_count/source_names/source_dispersion_bps/max_source_age_ms join
- volatility_lookback_s/volatility_bps

因此不能从 public rows 直接推导 b55/ce25 真实可复制信号，也不能启动 shadow/canary/live。

## Smoke Result

Smoke 覆盖三类：

- 无 candidate entry join rows: 保持 KEEP tooling，但真实输入 UNKNOWN。
- 完整 fixture rows: b55 与 ce25 两行都能用 `account+condition_id` join 到 public export，并满足概率模型合同；但 fixture 不算真实输入。
- 缺 `fair_probability` 的 bad fixture: fail closed。

所有路径保持 `promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false`。

## Interpretation

这一步把 b55 主线和 ce25 对照组从“公开榜单叙述”推进到“可执行数据合同”。下一步不是 shadow，而是最小源数据缺口审计：确认现有安全 export 是否能生成逐笔 entry rows，带 fee/liquidity role，并与 fair source/volatility row 做时间 join。缺任一字段就继续 UNKNOWN。

## Next Action

Implement `b55_ce25_entry_row_source_gap_audit_v1`: determine the smallest safe local export needed to produce per-trade public entry rows with fee/liquidity role and joinable fair source fields, without starting local agg/shared WS/shadow/live. If no existing safe export exists, return UNKNOWN and name the exact required exporter fields.
