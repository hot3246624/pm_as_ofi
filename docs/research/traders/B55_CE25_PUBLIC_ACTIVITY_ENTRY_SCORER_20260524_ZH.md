# B55 / CE25 Public Activity Entry Scorer - 2026-05-24

## Decision

`KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_READY_REAL_UNKNOWN_FIXTURE_READY`.

本轮完成的是本地 scorer，不是策略验证。scorer 只读取已提交的 exporter fixture artifact 和上一层 export-spec validator 输出，固化两条判定：

- real public rows: `UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING`
- fixture rows: `READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_NOT_REAL_INPUT`

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_scorer_20260524T083746Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_scorer_smoke_20260524T083746Z/manifest.json`

## Score Summary

真实 public rows 已通过 exporter 转换出 `24` 行平衡样本：

- b55: `12`
- ce25: `12`

真实 rows 可观察字段包括：

- account / trade / condition / market slug
- asset / timeframe / market start-end
- quote timestamp / entry delta / time to expiry
- outcome / Polymarket price / size
- gross USDC / fee USDC / actual USDC

但真实 rows 被 spec validator fail-closed，因为缺少：

- per-trade `liquidity_role`
- fair source snapshot: `boundary_price`, `fair_spot_mid`, source count/names/dispersion/age
- volatility: `volatility_lookback_s`, `volatility_bps`
- probability output: `fair_probability`, `fair_probability_uncertainty`, `edge_after_fee_and_uncertainty`
- `model_name`, `model_version`

fixture rows 完整通过 spec validator，但它们的 liquidity role、fair source、volatility 和 probability 都是 synthetic，只能说明 schema/scorer 能工作，不能说明 b55/ce25 策略可复制。

## Required Non-Fixture Inputs

进入任何 no-order diagnostic 前，必须先补齐非 fixture 输入：

1. per-trade liquidity role truth
   - 接受：explicit public maker/taker、documented per-trade takerOnly、verified non-private fill metadata
   - 拒绝：maker rebate、fee magnitude、price improvement、账户级 taker share

2. fair-source snapshot join
   - fields: boundary price, fair spot mid, source count/names/dispersion/age
   - join tolerance: `1500ms`
   - source policy: safe pre-existing export only; no service/shared-WS/local-agg start

3. volatility and probability model
   - fields: volatility lookback/bps, fair probability, uncertainty, edge, model name/version
   - edge formula: `fair_probability - polymarket_price - fee_usdc / size - fair_probability_uncertainty`

## Smoke

Smoke 覆盖：

- good path: real UNKNOWN, fixture READY, both marked not strategy evidence
- bad path: fixture spec status 被改成 fail-closed 后 scorer 必须返回 UNKNOWN
- promotion/private/deployable gates 始终 false

## Next Action

实现 `b55_ce25_non_fixture_input_source_review_v1`：本地审查是否存在安全、已导出的非服务输入，可以提供 per-trade liquidity role 与 fair-source probability join。若不存在，返回 UNKNOWN，并在启动任何 no-order diagnostic 前停止。
