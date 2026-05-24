# B55 / CE25 Entry Row Source Gap Audit - 2026-05-24

## Decision

`UNKNOWN_B55_CE25_ENTRY_ROW_SOURCE_GAP_REAL_EXPORT_FIELDS_MISSING`.

本轮完成的是 local-only source gap audit，不是策略验证。结论是：b55/ce25 线可以继续推进，但当前真实导出还没有足够字段做 fair-price entry 验证。

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_entry_row_source_gap_audit_20260524T065245Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_entry_row_source_gap_audit_smoke_20260524T065245Z/manifest.json`

## What Exists

现有能力分两块：

- public activity fetcher 存在，代码层可处理 `transactionHash`, `conditionId`, `slug`, `timestamp`, `price`, `size`, `usdcSize`, `outcome`, `side`, `type`。
- local price/source plumbing 存在，代码层有多源价格、source count/spread/age、boundary/round-end 相关解析能力。

这说明 b55/ce25 方向不是死路，但还没有拼成可验证的 entry-time research row。

## Missing Real Fields

真实阻塞字段：

- `fee_usdc`
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

关键阻塞更短：`liquidity_role`, `fair_probability`, `fair_probability_uncertainty`, `edge_after_fee_and_uncertainty`, source-quality time join, `volatility_bps`。

## Minimal Export Contract

下一步必须定义 `b55_ce25_entry_row_source_export_v1`，分五段 fail-closed：

- `public_activity_trade_rows_v1`: public Data API trade rows，保留 transaction hash、condition、slug、timestamp、price、size、usdcSize、outcome。
- `market_metadata_boundary_v1`: 为每行补 market start/end、entry delta、time-to-expiry；named 1h market 不能只靠 slug 猜。
- `liquidity_role_truth_source_v1`: 必须有显式 maker/taker/takerOnly 来源；不能用 maker rebate 或价格间接猜。
- `fair_source_snapshot_rows_v1`: quote_ts 附近的 safe source tape，提供 fair spot/source count/source names/source spread/source age/boundary。
- `entry_probability_model_output_v1`: 输出 purchased-outcome fair_probability、uncertainty、edge 和 model identity。

## Smoke Result

Smoke 覆盖三类：

- 无 candidate source manifest: real export UNKNOWN，缺口被明确列出。
- 完整 fixture source manifest: 合同可验证，但 fixture 不算真实输入。
- 缺 `liquidity_role` 与 `fair_probability` 的 bad fixture: fail closed。

所有路径保持 `promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false`。

## Next Action

Implement `b55_ce25_public_activity_entry_export_spec_v1`: define a fail-closed exporter contract for public activity trade rows plus market metadata, explicit fee/liquidity-role evidence, and optional safe fair-source snapshot manifest. Keep it local-only; return UNKNOWN until non-fixture rows provide `liquidity_role` and fair-probability joins for both b55 and ce25.
