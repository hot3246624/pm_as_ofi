# B55 / CE25 Public Activity Entry Exporter Fixture - 2026-05-24

## Decision

`KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_READY_REAL_STILL_UNKNOWN`.

本轮完成的是本地 fixture exporter，不是策略验证。它只把已有 public activity rows 转成可观察的 entry export 字段，并用 synthetic fixture 字段验证完整合同；真实导出仍是 `UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING`。

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_exporter_fixture_20260524T075245Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke_20260524T075245Z/manifest.json`

## Public Rows

输入只来自已存在的本地 public inputs：

- b55: `3492` public trade rows，合同内可转换 rows `422`
- ce25: `3497` public trade rows，合同内可转换 rows `496`

合同内筛选仍固定：

- accounts: b55 / ce25
- assets: BTC / ETH
- timeframes: `15m`, `1h_or_named`
- side/type: `BUY` / `TRADE`
- entry window: `-900s..-60s`
- Polymarket price: `0.35..0.90`

本轮 artifact 只抽取平衡样本各 `12` 行用于验证导出形态。

## Real Export Status

真实 rows 已能提供：

- `account`, `trade_id`, `condition_id`, `market_slug`
- `asset`, `timeframe`, `market_start_ts`, `market_end_ts`
- `quote_ts`, `entry_delta_s`, `time_to_expiry_s`
- `outcome`, `polymarket_price`, `size`
- `gross_usdc`, `fee_usdc`, `actual_usdc`

真实 rows 仍缺：

- `liquidity_role`
- `boundary_price`, `fair_spot_mid`
- `source_count`, `source_names`, `source_dispersion_bps`, `max_source_age_ms`
- `volatility_lookback_s`, `volatility_bps`
- `fair_probability`, `fair_probability_uncertainty`, `edge_after_fee_and_uncertainty`
- `model_name`, `model_version`

所以真实 export manifest 被上一层 spec validator 判为 fail-closed，不允许 shadow、deploy、canary 或 promotion。

## Fixture Export

fixture rows 在真实 public rows 上注入 synthetic 字段：

- `liquidity_role=maker`
- synthetic `boundary_price` / `fair_spot_mid`
- synthetic four-source snapshot quality
- synthetic volatility
- synthetic purchased-outcome probability / uncertainty / edge
- `model_name=fixture_entry_boundary_normal_probability`

这些字段只用于验证 schema 和 scorer 合同，不是 b55/ce25 的真实 fair-price 信号。

## Fail-Closed Checks

Smoke 覆盖：

- negative fee: `actual_usdc < price * size` 超过容差时拒绝
- bad slug metadata: 无法解析 market start/end 时拒绝
- sell side: 非 BUY 行拒绝
- real manifest: 缺 liquidity/fair probability，被 spec validator fail-closed
- fixture manifest: 完整字段通过，但仍被标记为 non-real fixture

所有路径保持：

- `promotion_gate.passed=false`
- `private_truth_ready=false`
- `deployable=false`

## Next Action

实现 `b55_ce25_public_activity_entry_scorer_v1`：本地运行 export spec validator，对 real manifest 输出 UNKNOWN、对 fixture manifest 输出 READY，并固化非 fixture 进入下一步所需的 liquidity-role 与 fair-source probability 输入清单。在这些真实 join 出现前，不启动 shadow/canary/live。
