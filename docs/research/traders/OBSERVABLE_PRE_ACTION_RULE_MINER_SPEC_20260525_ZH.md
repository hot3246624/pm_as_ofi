# Observable Pre-Action Rule Miner Spec 2026-05-25

## 结论

`observable_pre_action_rule_miner_spec_v1` 已就绪，默认真实输入状态是：

```text
KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN
```

这不是新策略已经找到，而是下一轮本地规则发现的安全合同。它把旧线的失败经验固化成三层 gate：先本地 train 发现，再 holdout 固定验证，最后要求同一个 frozen predicate 在 no-order summary 中复现 denominator。任何经济诊断必须排在 denominator 复现之后。

## 输入合同

输入 manifest 必须是当前 worktree 内的 materialized source，禁止 raw/replay/full-store、events JSONL、外部 worktree、SSH、service 或 shared WS。

必须包含三类 source：

- `candidate_rows`：训练/验证用行级导出，含 pre-action 字段和 offline label 字段。
- `source_link_summary`：source_sequence、quote_intent、source_order 与 admit/block reason 的 same-window summary。
- `no_order_denominator_summary`：frozen predicate 的 same-window no-order denominator 复现证据。

## 允许字段

冻结后的 live-rule predicate 只能使用 pre-action observable fields，例如：

- `source_sequence_present`
- `quote_intent_present`
- `source_order_present`
- `pre_seed_same_qty`
- `pre_seed_opp_qty`
- `pre_seed_same_cost`
- `pre_seed_opp_cost`
- `open_qty_bucket`
- `deficit_bucket`
- `candidate_qty_bucket`
- `source_risk_direction`
- `offset_s`
- `block_reason`

## 禁止项

禁止把以下内容作为 live criteria：

- realized `pair_cost`
- future `source_pair_*` / `source_residual_*`
- settlement outcome / redeem result
- public-profile single-day profit bucket
- public price cap
- static side/offset/timeframe deletion
- micro-deficit、fill-to-balance、portfolio-ledger、closed-cycle、symmetric-activation 等失败族复活

## Smoke 结果

smoke 覆盖四条路径：

- 默认无真实输入：spec READY，但 real input UNKNOWN。
- 完整 fixture：通过合同，但明确不是 strategy evidence。
- 缺 `source_sequence_present`：fail closed。
- no-order denominator 不复现：fail closed。

## 下一步

下一步只能做 local-only `observable_pre_action_rule_miner_input_inventory_v1`：检查当前 worktree 是否已有足够安全的 materialized source 可以生成该输入 manifest。仍不得自动启动 no-order diagnostic。
