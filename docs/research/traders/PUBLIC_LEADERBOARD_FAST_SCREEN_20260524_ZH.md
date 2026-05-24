# Public Leaderboard Fast Screen - 2026-05-24

## Decision

`KEEP_PUBLIC_LEADERBOARD_FAST_SCREEN_READY`.

本轮只用公开 Polymarket profile/data-api 做快速结构筛选，候选来自 leaderboard 线索：`b55`、`ohanism`、`ce25`、`04b6`。榜单和同事的 24h fee-after/MTM 结论只作为候选池，不作为可部署结论。

Artifact: `xuan_research_artifacts/xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/manifest.json`.

Sources:

- https://polymarket.com/zh/@0xb55fa1296E6ec55D0cE53d93B9237389f11764d4-1777575277609
- https://polymarket.com/zh/@ohanism
- https://polymarket.com/zh/@0xcE25E214D5cfE4f459cf67F08DF581885AAE7Fdc-1777575398144
- https://polymarket.com/zh/@0x04b6D7E930cf9e493c5E6eF24B496294F95594C8-1774448369789
- Public API pulls: `https://data-api.polymarket.com/activity`, `https://data-api.polymarket.com/positions`

## Fast Read

| account | public sample window | rows | structure read |
| --- | --- | ---: | --- |
| `b55` | 2026-05-24T00:34:03Z to 2026-05-24T03:11:33Z | 3492 | Active and profitable-looking lead, positive pair proxy edge `+0.0290`, but residual proxy `34.25%` and short crypto share `38.37%`; not yet a low-residual copyable pairing template. |
| `ohanism` | 2026-05-23T21:55:44Z to 2026-05-24T03:12:45Z | 3490 | Strong activity/control lead, but pair proxy edge `-0.1059` and residual proxy `89.11%`; this is direction/settlement style, not closed-cycle pairing. |
| `ce25` | 2026-05-23T22:59:28Z to 2026-05-24T03:12:29Z | 3497 | Improved structure: pair proxy edge `+0.0127`, residual proxy `19.14%`, both-outcome ratio `92.86%`; still not enough by itself because fee sensitivity was previously poor. |
| `04b6` | 2026-05-22T17:56:31Z to 2026-05-23T06:53:43Z | 3500 | Cleanest infrastructure template: pair proxy edge `+0.0068`, residual proxy `15.91%`, BTC short-horizon share `95.86%`, both-outcome ratio `100%`; currently stale, with only 39 rows on 2026-05-23 in this pull. |

## Interpretation

同事关于 `b55` 的判断可以成立，但它回答的是 24h account-level fee-after cash/MTM 问题；本地快筛回答的是 public recent rows 的 pair/residual structure 问题。这两个口径不能互相替代。

因此当前拆成两条线：

- Profitability/MTM lead: `b55`，需要重建 24h realized/MTM、fee/rebate、taker/maker、市场选择和残仓年龄，而不是直接套 closed-cycle pairing 规则。
- Robust infrastructure template: `04b6`，继续用于闭环配对/低残仓目标函数，但它当前不是活跃跟踪对象。
- Direction/settlement controls: `ohanism` 和 `b55` 的方向暴露部分，必须和低残仓 pairing 目标分开评价。
- Fee-stress control: `ce25`，值得观察，但不能忽视 taker/fee drag。

## Next Action

先完成 `source_opportunity_closed_cycle_marker_summary_scorer_v1`，然后只跑一个 bounded no-order diagnostic 去验证 closed-cycle denominator 是否存在。如果同窗口 marker 稀薄或风险预算失败，立即 DISCARD 这条闭环配对线，转向 `b55` 的 24h realized/MTM profile audit，不再继续旧 D+ family 或无根据阈值扫参。

`research_ranking`: `KEEP_PUBLIC_LEADERBOARD_FAST_SCREEN_READY`.

`promotion_gate.passed=false`, `private_truth_ready=false`, `deployable=false`.
