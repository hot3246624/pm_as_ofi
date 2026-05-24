# B55 Fair Price Entry Window Spec - 2026-05-24

## Decision

`UNKNOWN_B55_FAIR_PRICE_ENTRY_WINDOW_SPEC_SOURCE_AND_TAIL_BLOCKED`.

本轮把 b55 的可复制假设写成可执行合同，但不把它升级成 shadow 或 deployable。核心原因很直接：b55 的优势来自 fair-price 判断，而当前 xuan-research 线还没有一个已声明字段完整的 BTC/ETH fair-probability source manifest。没有这个源，`35c..90c` 只能说明交易 habitat，不能作为入场信号。

Artifact:

`xuan_research_artifacts/xuan_b55_fair_price_entry_window_spec_20260524T051115Z/manifest.json`

## Contract

第一版只允许：

- Asset: `BTC`, `ETH`
- Timeframe: `15m`, `1h_or_named`
- Entry window: market end 前 `15m..1m`
- Polymarket price habitat: `0.35..0.90`
- Realized fee/gross: `<=1.2%`
- Average actual pair cost: `<=0.97`
- Pair cost p90: `<=1.00`
- Residual rate: `<=10%`
- Minimum edge after fee and uncertainty: `>=2c/share`

Required fair-price fields include `fair_probability`, `fair_probability_uncertainty`, `edge_after_fee_and_uncertainty`, `boundary_price`, `fair_spot_mid`, `source_count`, `source_names`, `source_dispersion_bps`, `max_source_age_ms`, `model_name`, and `model_version`.

## Current Blockers

- `fair_price_source_missing_or_incomplete`: no explicit source manifest was provided.
- `per_trade_fee_and_liquidity_role_source_missing`: public profile export has inferred fee but not per-trade maker/taker role.
- `observed_b55_residual_above_replication_target`: b55 observed residual `14.9474%`, above the first xuan target `10%`.
- `observed_b55_pair_cost_tail_above_replication_target`: b55 per-market pair-cost p90 `1.164791`, above the target `1.00`.

## Interpretation

This is still a KEEP-quality research lead in spirit, but the executable spec must fail closed as UNKNOWN until fair-price and execution-fee sources are explicit. The first local verifier should not start from 5m or price caps. It should start from BTC/ETH 1h, then 15m only after fair-price and residual gates are available.

## Next Action

Build or locate a local-only fair-price source manifest for BTC/ETH 15m/1h with the required fields, then rerun `scripts/xuan_b55_fair_price_entry_window_spec.py`. Do not start shadow until fair-price, per-trade fee/liquidity role, residual, and pair-tail blockers are all explicit.
