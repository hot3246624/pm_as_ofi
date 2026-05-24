# B55 / CE25 Fair Probability Model Spec - 2026-05-24

## Decision

`KEEP_B55_CE25_FAIR_PROBABILITY_MODEL_SPEC_READY`.

本轮完成的是概率模型合同，不是策略上线证据。合同已经可执行、可 fail-closed；真实 b55/ce25 评估仍是 `UNKNOWN_REAL_FAIR_PROBABILITY_INPUTS_MISSING`，因为当前还没有非 fixture 的 fair-probability manifest。

Artifacts:

- `xuan_research_artifacts/xuan_b55_ce25_fair_probability_model_spec_20260524T055426Z/manifest.json`
- `xuan_research_artifacts/xuan_b55_ce25_fair_probability_model_spec_smoke_20260524T055145Z/manifest.json`

## Contract

第一版只允许：

- Accounts: `b55` 主线 + `ce25` 低残仓对照组
- Assets: `BTC`, `ETH`
- Timeframes: `15m`, `1h_or_named`
- Entry window: market end 前 `-900s..-60s`
- Polymarket price habitat: `0.35..0.90`
- Minimum source count: `3`
- Max source age: `1500ms`
- Max source dispersion: `8bps`
- Max probability uncertainty: `0.03`
- Minimum edge after fee and uncertainty: `0.02/share`
- Average pair cost target: `<=0.97`
- Pair-cost p90 target: `<=1.00`
- Residual target: `<=10%`

Candidate rows must include entry trade fields, explicit fee/liquidity role, market boundary, time-to-expiry, multi-source fair spot, source quality, volatility/uncertainty, `fair_probability`, `fair_probability_uncertainty`, and `edge_after_fee_and_uncertainty`.

## Edge Formula

The contract fixes the admission formula:

`edge_after_fee_and_uncertainty = fair_probability - polymarket_price - fee_per_share - fair_probability_uncertainty`

Admit only if that value is at least `0.02/share` and all source-quality, pair-tail, and residual gates pass.

## Smoke Result

Smoke covers three cases:

- No candidate manifest: spec remains KEEP as tooling, but real inputs stay UNKNOWN.
- Complete fixture manifest: candidate contract validates, but fixture is not real evidence.
- Bad fixture missing probability fields: fail-closed.

`promotion_gate.passed=false`, `private_truth_ready=false`, `deployable=false`, and no shadow/order side effects in every case.

## Interpretation

This prevents the b55 line from collapsing into a price cap. The next step is not shadow; it is a local entry join audit that tries to join public b55/ce25 entry rows to this contract. If volatility/uncertainty or per-trade fee/liquidity role is absent, it must return UNKNOWN instead of inventing a signal.

## Next Action

Implement `b55_ce25_public_entry_join_audit_v1`: join public b55/ce25 entry rows to the probability-model contract fields, first with fixtures and then with any existing safe exported source tapes if available. Keep it local-only and fail closed when required fields are absent.
