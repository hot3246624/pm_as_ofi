# B55 / CE25 Non-Fixture Input Source Review - 2026-05-24

## Decision

`UNKNOWN_B55_CE25_NON_FIXTURE_INPUT_SOURCE_MISSING`.

本轮只做本地 source review，不是策略验证。结论很直接：当前已有 public rows 和 public exports 可以支撑 entry-row normalization，但没有安全、已导出的非 fixture 输入能提供逐笔 `liquidity_role` 与 fair-probability/source join。因此 b55/ce25 不应进入 no-order diagnostic。

Artifact:

- `xuan_research_artifacts/xuan_b55_ce25_non_fixture_input_source_review_20260524T090916Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_non_fixture_input_source_review_smoke_20260524T090916Z/manifest.json`

## What Exists

已有 public activity rows:

- b55: `3492` rows
- ce25: `3497` rows

这些 rows 字段完整度足够做 entry normalization：

- `transactionHash`, `conditionId`, `eventSlug`, `slug`
- `timestamp`, `price`, `size`, `usdcSize`
- `outcome`, `outcomeIndex`, `side`, `type`

repo 里也有 source-code plumbing：

- public activity row writer / fetcher 代码
- local agg / source snapshot 相关代码 token
- fair-probability spec/scorer 代码
- volatility/source age/source count/source dispersion 相关代码 token

但这些只是代码能力或 fixture/spec，不是已经可 join 到 b55/ce25 entry rows 的非 fixture export。

## Missing Inputs

当前 exact missing inputs:

- `liquidity_role`
- `boundary_price`
- `fair_spot_mid`
- `source_count`
- `source_names`
- `source_dispersion_bps`
- `max_source_age_ms`
- `volatility_lookback_s`
- `volatility_bps`
- `fair_probability`
- `fair_probability_uncertainty`
- `edge_after_fee_and_uncertainty`
- `model_name`
- `model_version`

因此 `stop_before_no_order_diagnostic=true`。

## Rejected Shortcuts

以下不能替代缺失输入：

- maker rebate 推断逐笔 maker
- fee magnitude 推断 maker/taker
- 账户级 taker share 替代逐笔 role
- 纯 Polymarket price band
- source-code token 替代已导出的 time-aligned source rows
- fixture/synthetic fair probability

## Smoke

Smoke 覆盖三条路径：

- missing candidate: 返回 UNKNOWN，并点名缺 `liquidity_role` 与 `fair_probability`
- complete candidate: 只有在声明安全非服务 export、覆盖 b55/ce25、字段和 joins 完整时才返回 KEEP
- bad candidate: 缺 role/probability 时 fail-closed

## Next Action

不要启动 b55/ce25 no-order diagnostic。下一步只能二选一：

- 明确批准并准备安全 offline exporter，产出非服务的 liquidity-role / fair-source probability join；
- 或转向另一个 public-profile lead，要求它有更直接的非 fixture 可观察证据。
