# ce25 Projected Guard Runner Instrumentation Spec - 2026-05-24

## 结论

本轮只审查当前 runner 源码和已提交 projected-guard spec/scorer artifacts，没有修改 runner，没有 fetch 新数据、没有启动 shadow/canary/local agg/shared WS/live，也没有读取 raw/replay/full store。

decision:
`KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_READY`

artifact:
`xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_spec_20260524T112046Z/manifest.json`

含义：

- 当前 runner 已有足够的订单前 state，可做默认关闭 projected guard diagnostics。
- 下一步可以实现 `--ce25-projected-guard-event-lite-summary`。
- v1 只能输出 summary-only diagnostics，不允许改变候选/下单/配对行为。
- 仍不允许 no-order diagnostic、shadow、canary、live 或 deploy。

## 源码可用字段

源码审查确认：

| 能力 | 状态 | 来源 |
|---|---|---|
| same/opp qty | available | `same_qty = self.exposure_qty(side)` / `opp_qty = self.exposure_qty(opp(side))` |
| same/opp cost | available | `same_cost_before = self.exposure_cost(side)` / `opp_cost_before = self.exposure_cost(opp(side))` |
| intended order side/qty/price | available | `side` / `qty` / `seed_px` before `VirtualOrder` append |
| pre-append hook | available | after `qty` finalized, before `self.pending.append(order)` |
| event-lite summary pattern | available | existing `--event-lite-summary` and summary merge pattern |
| default-off CLI pattern | available | existing source-opportunity default-off flags |
| realized pair cost separation | available | realized `pair_cost = a.px + b.px` stays post-fill only |

## Canonical Mapping

如果 intended order side 是 YES：

- `pre_yes_qty = same_qty`
- `pre_no_qty = opp_qty`
- `pre_yes_actual_cost = same_cost_before`
- `pre_no_actual_cost = opp_cost_before`

如果 intended order side 是 NO：

- `pre_yes_qty = opp_qty`
- `pre_no_qty = same_qty`
- `pre_yes_actual_cost = opp_cost_before`
- `pre_no_actual_cost = same_cost_before`

共同字段：

- `outcome = side`
- `order_qty = qty`
- `order_price = seed_px`
- `estimated_fee_per_share = 0.0`

fee 说明：这里的 `0.0` 只代表当前 no-order passive seed proxy 的默认诊断假设；真实账户非零 fee、maker/taker role、rebate 仍是外部 blocker，不能从这个 artifact 推出 deployability。

## 默认关闭 instrumentation 合同

flag:
`--ce25-projected-guard-event-lite-summary`

dependency:
`--event-lite-summary`

placement:
在 `DPlusRunner.on_trade` 中 `qty` finalized 后、`VirtualOrder` append 前。

新增 helper：

- `market_round_length_s(slug)`
- `seconds_to_expiry_from_offset(slug, offset_s)`
- `projected_guard_context(...)`
- `projected_guard_decision(...)`

输出 summary：

- `event_lite.ce25_projected_guard_summary.schema_version = ce25_projected_guard_summary_v1`
- `decision_count_by_status`
- `decision_count_by_guard`
- `decision_count_by_asset_timeframe_guard_status`
- `projected_pair_cost_bucket_by_guard`
- `projected_residual_bucket_by_guard`
- `final_window_policy_count`
- field contract 明确 `post_action_outcome_labels_included=false`
- field contract 明确 `private_truth_ready=false`
- field contract 明确 `deployable=false`
- field contract 明确 `promotion_gate_passed=false`

## Smoke Plan

下一步实现 runner 后必须证明：

- 默认关闭时 summary 不含 `ce25_projected_guard_summary`。
- 开启时 summary 含 schema `ce25_projected_guard_summary_v1`。
- fixture starter/core candidate 与 fixture scorer 公式一致。
- hard-kill candidate 只作为 diagnostic 计数，不改变行为。
- 未同时开启 `--event-lite-summary` 时 CLI fail closed。

## 下一步

实现 `ce25_projected_guard_runner_instrumentation_default_off_v1`。

边界：

- 只加默认关闭 summary-only diagnostics。
- 不启用 guard，不改变候选、不改变配对、不改订单行为。
- 不启动 no-order diagnostic。
- 如果实现中发现 cost allocation 或 seconds_to_expiry 无法安全产出，则返回 UNKNOWN，不继续推进到 shadow。
