# B55 / CE25 Public Activity Entry Export Spec - 2026-05-24

## Decision

`KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_READY`.

本轮完成的是 exporter spec，不是实盘验证。真实导出状态仍是 `UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING`，因为还没有非 fixture rows 同时提供 per-trade liquidity role 与 fair-probability/source join。

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_export_spec_20260524T072245Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_export_spec_smoke_20260524T072245Z/manifest.json`

## Export Contract

合同名：`b55_ce25_public_activity_entry_export_spec_v1`。

允许范围：

- accounts: `b55`, `ce25`
- assets: `BTC`, `ETH`
- timeframes: `15m`, `1h_or_named`
- entry window: `-900s..-60s`
- public activity: `type=TRADE`, `side=BUY`
- Polymarket price habitat: `0.35..0.90`

明确排除：5m hard chase、纯 price band、静态资产/周期删除、D+ micro-deficit/ledger/tiny-deficit/closed-cycle/cooldown/fill-to-balance families。

## Fee Policy

BUY 行的费用合同固定为：

- `actual_usdc = usdcSize`
- `gross_usdc = polymarket_price * size`
- `fee_usdc = actual_usdc - gross_usdc`
- `fee_per_share = fee_usdc / size`

但必须声明 `usdcSize` 是 public activity BUY 行的含 fee 实际成本。若 basis 未声明、`size <= 0`、或 `actual_usdc < gross_usdc` 超过容差，则 fail closed。

## Liquidity Role

`liquidity_role` 只能来自逐笔显式来源：

- public activity 中的 maker/taker 字段，如果 Polymarket 暴露；
- public export 中 documented `takerOnly` 字段；
- verified fill metadata 中的非私有 role 字段。

明确拒绝：

- 用 maker rebate 推断逐笔 maker；
- 用 fee 大小猜 maker/taker；
- 用 price improvement 或 queue behavior 猜 role；
- 用账户级 takerOnly share 代替逐笔 join。

## Fair Source Join

fair source snapshot 必须是安全已导出的 source tape，不能为了本 spec 启动 local agg/shared WS/service。

第一版 join tolerance:

- `max_join_abs_delta_ms = 1500`
- `min_source_count = 3`
- `max_source_age_ms = 1500`
- `max_source_dispersion_bps = 8`

输出必须包含 `fair_spot_mid`, `source_count`, `source_names`, `source_dispersion_bps`, `max_source_age_ms`, `boundary_price`。

## Probability Output

模型输出必须是 purchased-outcome win probability，不是方向标签。edge 公式固定：

`fair_probability - polymarket_price - fee_usdc / size - fair_probability_uncertainty`

必须提供 `fair_probability`, `fair_probability_uncertainty`, `edge_after_fee_and_uncertainty`, `model_name`, `model_version`。

## Smoke Result

Smoke 覆盖三类：

- 无 candidate export manifest: spec KEEP，但 real export UNKNOWN。
- 完整 fixture export manifest: 合同通过，但 fixture 不算真实输入。
- 缺 `liquidity_role` 与 fair-probability fields 的 bad fixture: fail closed。

所有路径保持 `promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false`。

## Next Action

Implement `b55_ce25_public_activity_entry_exporter_fixture_v1`: convert already available public activity rows into the export schema for fixtures, prove fee derivation and metadata parsing fail closed, and keep real export UNKNOWN until liquidity_role and fair-source probability joins are non-fixture.
