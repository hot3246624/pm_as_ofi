# Xuan Public Leaderboard Trader Review 2026-05-24

## Scope

This review uses read-only public exports from:

`/Users/hot/web3Scientist/poly_trans_research/data/exports`

Generated at: `2026-05-24T03:28:16.421402Z`.

Public profile links:

- b55: https://polymarket.com/zh/@0xb55fa1296E6ec55D0cE53d93B9237389f11764d4-1777575277609
- ohanism: https://polymarket.com/zh/@ohanism
- ce25: https://polymarket.com/zh/@0xcE25E214D5cfE4f459cf67F08DF581885AAE7Fdc-1777575398144
- 04b6: https://polymarket.com/zh/@0x04b6D7E930cf9e493c5E6eF24B496294F95594C8-1774448369789

These accounts are public behavior samples, not private execution truth. The data is useful for target-shape design and benchmark calibration, but it cannot authorize live execution.

## Summary Table

| account | archetype | activity rows | buy actual | cash PnL | MTM PnL | actual pair cost | pair edge | residual rate | cash ROI on buy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| b55 | primary_paired_accumulation_reference | 23,477 | $289,124 | +$3,870 | +$9,331 | 0.9592 | 4.08% | 14.95% | 1.34% |
| ohanism | directional_residual_rebate_reference_not_core | 23,206 | $210,632 | +$2,118 | +$2,768 | 1.0367 | -3.67% | 76.98% | 1.01% |
| ce25 | low_residual_control_reference | 17,709 | $144,401 | +$458 | +$458 | 0.9742 | 2.58% | 8.71% | 0.32% |
| 04b6 | historical_reference_inactive_recent | 40 | $460 | +$13 | +$13 | 1.0114 | -1.14% | 13.92% | 2.92% |

## Read

1. `b55` is the best public benchmark for xuan-frontier. Its 24h actual pair cost is `0.9592`, pair edge is `4.08%`, and residual rate is `14.95%`. This is not a zero-residual bot; it is high-throughput paired accumulation with residual held inside a manageable band.
2. `ce25` is the residual-control reference. It has lower residual than b55 at `8.71%`, but fee-after cash PnL is much thinner. That says residual control alone is not enough; the admission price still needs a fee-aware edge.
3. `ohanism` should not be copied into the core candidate. The account is profitable in public cashflow, but residual is `76.98%` and paired cost is above 1.0. That is closer to directional/settlement plus rebate behavior than a clean paired engine.
4. `04b6` remains useful historically, but the current 24h sample is too small and pair cost is above 1.0. It is not an active current benchmark.

## Asset Breakdown

### b55

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|
| BTC | 217 | 11649 | 0.9625 | 23.41% | +$6,735 |
| ETH | 183 | 5580 | 0.9448 | 30.10% | +$3,078 |
| SOL | 167 | 2942 | 0.9524 | 31.04% | +$1,060 |
| XRP | 162 | 2578 | 0.9821 | 31.24% | +$286 |
### ohanism

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|
| BTC | 400 | 11341 | 1.0514 | 85.26% | -$1,666 |
| ETH | 368 | 3766 | 0.9940 | 90.35% | +$33 |
| SOL | 328 | 1953 | 1.0158 | 93.45% | -$22 |
| XRP | 307 | 1389 | 0.8714 | 90.81% | +$165 |
### ce25

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|
| BTC | 182 | 8985 | 0.9796 | 12.89% | +$1,962 |
| ETH | 153 | 4091 | 0.9601 | 19.51% | +$1,120 |
| SOL | 154 | 2261 | 0.9519 | 23.62% | +$485 |
| XRP | 151 | 1743 | 0.9932 | 31.22% | +$38 |
### 04b6

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|
| BTC | 1 | 39 | 1.0114 | 24.44% | -$4 |

## Implications For Xuan Frontier

- The right external benchmark is not leaderboard PnL alone. The benchmark should be fee-aware actual pair cost, residual share, cash PnL, and capacity.
- For the capacity ladder, keep current pass gates, but add public-review targets: actual pair cost should aim at `<=0.965`, remain reviewable up to `<=0.975`, residual rate should target `<=15%`, and hard fail above `20%`.
- Our current clean no-order packet has fee-aware capital ROI around `4.07%`, which is not obviously worse than b55's pair-edge benchmark. The missing proof is real capacity and own execution, not the edge formula.
- Do not relax residual risk to chase ohanism-like cashflow. If a separate directional residual lane is studied, it must be explicitly labeled outside the current paired/rescue shadow candidate.
- `b55` supports continuing the capacity ladder: real accounts can run large BUY/redeem notional with residual around the mid-teens. `ce25` supports strict residual accounting. Together they support the cap-25 -> cap-75 -> cap-150 -> cap-300 proof path.

## Recommended Next Actions

1. Keep the shadow-review candidate as research-only and proceed with the capacity ladder review path.
2. Add b55/ce25 benchmark fields to future shadow packets: public-benchmark pair cost, public-benchmark residual target, and account-archetype comparison.
3. If cap-25 dry-run passes, compare its fee-aware pair edge and residual share against b55/ce25 before moving to cap-75.
