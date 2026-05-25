# Observable Pre-Action Rule Miner Input Inventory

时间：2026-05-25 18:55 UTC

## 结论

`UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_GAPS`

当前 worktree 有一块很有价值的候选行数据，但还不能组装成
`observable_pre_action_rule_miner_input_v1` 的真实非 fixture 输入。原因不是
样本太少，而是缺少行级 source-link truth 和新冻结规则的 same-window no-order
denominator。

## 已确认的安全输入

- `xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/candidate_seed_outcome_separator.csv`
  - 41,580 行
  - 覆盖 15 个 day
  - 包含 `condition_id`、`day`、`ts_ms`、`side`、`offset_s`、`pre_seed_*`、`source_risk_direction`
  - 包含 offline labels：`source_pair_*`、`source_residual_*`、`pair_outcome_bucket`、`residual_tail_outcome_bucket`

- `xuan_research_artifacts/xuan_b27_dplus_source_opportunity_gap_audit_strict_20260523T111500Z/manifest.json`
  - 有 `source_link_presence_by_status`
  - 有 admitted/blocked 的 aggregate source sequence / quote intent / source order 覆盖
  - 但它是聚合 summary，不能把 source presence 绑定回每一条 candidate row

- `xuan_research_artifacts/xuan_b27_dplus_candidate_separator_train_holdout_audit_strict_no_order_20260523T111500Z/manifest.json`
  - 有旧 candidate separator 的 no-order reproduction summary
  - 但旧 marker 没复现，`source_opportunity_marker_total=0`
  - 不能替代新 mined frozen rule 的 denominator gate

## 主要缺口

候选 CSV 还缺：

- `status_before_action`
- `block_reason`
- 行级 `source_sequence_present`
- 行级 `quote_intent_present`
- 行级 `source_order_present`

虽然 CSV 有 `quote_intent_id/source_order_id/source_sequence_id` 列，但本轮盘点确认这些列为空，且
`external_shadow_ids_available` 没有真实可用记录。因此不能把它们当作行级 source truth。

no-order denominator 还缺：

- 新 frozen rule 的 `frozen_rule_id`
- `same_window_denominator_count`
- `admitted_count`
- `blocked_count`
- source sequence / quote intent / source order coverage
- `aggregate_parity`
- 对应的 train/holdout split 与 frozen rule contract

## 可执行后续

下一步不应启动 no-order diagnostic。应先做 local-only 的
`observable_pre_action_rule_miner_feature_join_contract_v1`：定义未来安全输入如何把
candidate rows、row-level source presence、status/block reason、以及 exact same-window denominator
组装成一个可被 `scripts/xuan_observable_pre_action_rule_miner_spec.py` 验证的输入 manifest。

## 安全边界

本轮只读取当前 worktree 已提交 artifact、CSV/JSON、以及本地 spec/source 文件；没有 fetch 新数据、
没有 SSH、没有启动 shadow/canary/local agg/shared WS/live、没有读取 events JSONL、没有扫描 raw/replay/full store、
没有修改 shared-ingress/broker/env/live、没有发送 order/cancel/redeem。

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，
`promotion_gate.passed=false`，`private_truth_ready=false`，`deployable=false`。
