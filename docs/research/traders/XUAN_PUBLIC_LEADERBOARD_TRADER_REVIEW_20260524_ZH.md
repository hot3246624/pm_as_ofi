# Xuan Public Leaderboard Trader Review 2026-05-24

## Scope

This review uses read-only public exports from:

`/Users/hot/web3Scientist/poly_trans_research/data/exports`

Generated at: `2026-05-24T05:36:01.746253Z`.

Public profile links:

- b55: https://polymarket.com/zh/@0xb55fa1296E6ec55D0cE53d93B9237389f11764d4-1777575277609
- ohanism: https://polymarket.com/zh/@ohanism
- ce25: https://polymarket.com/zh/@0xcE25E214D5cfE4f459cf67F08DF581885AAE7Fdc-1777575398144
- 04b6: https://polymarket.com/zh/@0x04b6D7E930cf9e493c5E6eF24B496294F95594C8-1774448369789
- xuan: https://polymarket.com/zh/@0xcfb103c37c0234f524c632d964ed31f117b5f694
- b27bc: https://polymarket.com/zh/@0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82

These accounts are public behavior samples, not private execution truth. The data is useful for target-shape design and benchmark calibration, but it cannot authorize live execution.

## Summary Table

This table is the raw public-window summary. It is not the corrected "new strategy PnL" table.

| account | archetype | activity rows | buy actual | cash PnL | MTM PnL | actual pair cost | pair edge | residual rate | cash ROI on buy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| b55 | primary_probability_pair_residual_reference | 23,477 | $289,124 | +$3,870 | +$9,331 | 0.9592 | 4.08% | 14.95% | 1.34% |
| ohanism | directional_residual_rebate_reference_not_core | 23,206 | $210,632 | +$2,118 | +$2,768 | 1.0367 | -3.67% | 76.98% | 1.01% |
| ce25 | low_residual_control_reference | 17,709 | $144,401 | +$458 | +$458 | 0.9742 | 2.58% | 8.71% | 0.32% |
| 04b6 | historical_reference_inactive_recent | 40 | $460 | +$13 | +$13 | 1.0114 | -1.14% | 13.92% | 2.92% |
| xuan | excluded_high_fee_pair_cost_reference | 1,158 | $66,314 | -$286 | -$286 | 1.0069 | -0.69% | 0.43% | -0.43% |
| b27bc | historical_reference_inactive_recent | 0 | $0 | +$0 | +$0 | n/a | n/a | n/a | n/a |

## Corrected Window PnL Decomposition

This table separates old-position redeem, maker rebate, same-window new-position cash, and current value. It is the preferred ranking table for recent behavior.

| account | old inventory redeem | maker rebate | new-position cash ex rebate | current value | corrected new-position MTM ex rebate | corrected new-position MTM incl rebate | actual pair cost | residual rate | split count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b55 | +$5,818 | +$662 | -$2,610 | $5,461 | +$2,851 | +$3,513 | 0.9592 | 14.95% | 0 |
| ohanism | +$997 | +$1,020 | +$100 | $650 | +$751 | +$1,771 | 1.0367 | 76.98% | 0 |
| ce25 | +$0 | +$308 | +$150 | $0 | +$150 | +$458 | 0.9742 | 8.71% | 0 |
| 04b6 | +$0 | +$0 | +$13 | $0 | +$13 | +$13 | 1.0114 | 13.92% | 0 |
| xuan | +$108 | +$0 | -$394 | $0 | -$394 | -$394 | 1.0069 | 0.43% | 0 |
| b27bc | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | 0 |

## Corrected Ranking

| rank | account | read |
|---:|---|---|
| 1 | b55 | 综合最强，仍是主拆解对象；收益要按新仓 MTM 下修，不按 leaderboard 9k 粗读。 |
| 2 | ce25 | 最干净的低残仓 pair-arb 候选；规模和利润薄，但没有旧仓 redeem 污染。 |
| 3 | b27bc | 历史 maker benchmark 可能强，但当前窗口无活动，不能作为现役基准。 |
| 4 | ohanism | 赚钱能力可能强，但不是稳健 pair-arb；旧仓/rebate 和高残仓污染很重。 |
| 5 | xuan | 继续排除；低残仓不能抵消高 fee 和含 fee pair cost > 1。 |

## Read

1. `b55` is the best public **pair-quality / fee / residual** benchmark for xuan-frontier, but it is not a clean "new 24h positions locked 9k" benchmark. Its 24h actual pair cost is `0.9592`, pair edge is `4.08%`, and residual rate is `14.95%`.
2. `ce25` was under-rated by raw leaderboard PnL. It has no old-redeem pollution in this corrected table, low residual at `8.71%`, and pair cost below 1. Its weakness is scale and thin profit, not cleanliness.
3. `ohanism` should be downgraded for this paired/rescue lane. Raw cashflow is positive, but the corrected view includes old redeem and large rebate, while pair cost is above 1 and residual is `76.98%`.
4. `xuan` remains excluded as a public-account template. Its residual is low, but actual pair cost is `1.0069` and corrected new-position cash is negative.
5. `b27bc` is a historical reference only in this window because there is no current activity.
6. `04b6` remains useful historically, but the current 24h sample is too small and pair cost is above 1.0. It is not an active current benchmark.

## Corrected B55 Strategy Read

The corrected read is:

> near-expiry probability revaluation + two-sided pair-cost reduction + small directional residual + low effective fee.

This is not xuan's older "high-frequency hard-take with high fee" pattern, and it is not pure maker-only or pure taker-only.

### B55 24h Cash Caveat

- cash_pnl: `+$3,870.06`
- current_position_value: `$5,460.78`
- cash_plus_current_value: `+$9,330.84`
- old_inventory_redeem_only_cash_lower_bound: `+$5,553.89`
- colleague_report_old_inventory_redeem_contribution_approx: `+$5,818.00`
- colleague_report_same_window_new_realized_cash_approx: `-$2,610.00`

So b55 is profitable and worth studying, but the 24h cash/MTM line mixes old inventory redeem, current residual value, and same-window trading. It should calibrate pair quality and residual policy, not be treated as proof that fresh positions realized the full public PnL.

### B55 Pair Quality

- gross_pair_cost: `0.9483`
- actual_pair_cost_with_fee: `0.9592`
- pair_fee_cost: `0.0109`
- paired_actual_profit: `+$11,160`
- bought residual rate: `14.95%`
- current residual rate: `18.32%`
- fee_rate_on_gross: `1.171%`
- maker_rebate: `+$662`

### B55 Leading Pair-PnL Groups

| group | actual pair cost | pair PnL | cash PnL | residual PnL est |
|---|---:|---:|---:|---:|
| BTC 15m | 0.9620 | +$3,315 | -$1,005 | -$4,320 |
| BTC 1h_or_named | 0.9450 | +$1,618 | +$3,249 | +$1,631 |
| BTC 5m | 0.9725 | +$1,395 | -$1,332 | -$2,727 |
| ETH 15m | 0.9579 | +$1,115 | -$59 | -$1,174 |
| ETH 1h_or_named | 0.9298 | +$913 | +$1,188 | +$275 |
| ETH 5m | 0.9210 | +$802 | -$151 | -$953 |

### Replication Notes

- Start from BTC/ETH 15m and 1h, not 5m hard-chasing.
- Entry is concentrated in the final 15m to 1m, especially the final 5m before close.
- Main price bands are 35c-90c, especially 50c-80c; tail-lottery prices are auxiliary.
- Treat fee-included pair cost above 0.97 as caution and above 1.00 as directional, not arbitrage.
- Initial xuan residual target should stay below 10%; b55's 15%-18% is too aggressive for a new system.
- If effective fee drifts toward 2.5%-3%, this template breaks unless pair cost improves materially.
- The missing component is fair-price estimation from robust venue aggregation, not just socket speed.

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
### xuan

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|
| BTC | 119 | 938 | 1.0069 | 0.85% | -$454 |
### b27bc

| asset | markets | trades | weighted actual pair cost | residual rate | paired actual profit |
|---|---:|---:|---:|---:|---:|

## Implications For Xuan Frontier

- The right external benchmark is not leaderboard PnL alone. For b55, the usable benchmark is fee-aware actual pair cost, residual share, fee rate, and capacity; public cash PnL must be adjusted for old inventory and current residual value.
- For the capacity ladder, keep current pass gates, but add public-review targets: actual pair cost should aim at `<=0.965`, remain reviewable up to `<=0.975`, residual rate should target `<=15%`, and hard fail above `20%`.
- Our current clean no-order packet has fee-aware capital ROI around `4.07%`, which is not obviously worse than b55's pair-edge benchmark. The missing proof is real capacity and own execution, not the edge formula.
- Do not relax residual risk to chase ohanism-like cashflow. If a separate directional residual lane is studied, it must be explicitly labeled outside the current paired/rescue shadow candidate.
- `b55` supports continuing the capacity ladder and adding a fair-price lane: real accounts can run large BUY/redeem notional with low fee and residual around the mid-teens. `ce25` supports strict residual accounting. Together they support the cap-25 -> cap-75 -> cap-150 -> cap-300 proof path, but not blind 5m hard-taking.
- SPLIT is zero across this sampled batch, so this update does not add split-then-sell as a confounder.

## Recommended Next Actions

1. Keep the shadow-review candidate as research-only and proceed with the capacity ladder review path.
2. Add b55/ce25 benchmark fields to future shadow packets: public-benchmark pair cost, public-benchmark residual target, account-archetype comparison, and b55 old-inventory cash caveat.
3. If cap-25 dry-run passes, compare its fee-aware pair edge and residual share against b55/ce25 before moving to cap-75.
4. Open a separate local research lane for BTC/ETH 15m/1h fair-price admission. Keep it outside live execution and outside shared localagg mutation until it has its own source-truth validation.
