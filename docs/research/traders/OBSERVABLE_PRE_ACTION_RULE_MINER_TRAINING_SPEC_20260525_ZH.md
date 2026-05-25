# Observable Pre-Action Rule Miner Training Spec

结论：`KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN`。

本轮只做 current-worktree local-only training spec。没有 fetch 新数据，没有读取外部 worktree，没有 SSH，没有启动 shadow/canary/local agg/shared WS/live，没有读取或拉取 events JSONL，没有扫描 raw/replay/full store，也没有发送 order/cancel/redeem。

## 新增内容

- `scripts/xuan_observable_pre_action_rule_miner_training_spec.py`
- `scripts/xuan_observable_pre_action_rule_miner_training_spec_smoke.sh`
- artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/manifest.json`
- smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_spec_smoke_20260525T223507Z/manifest.json`

## 输入基线

本轮只用上一轮 bridge fixture scorer 的已提交输出证明 schema/leakage 口径：

- joined rows: `10`
- exact-id joins: `5`
- composite fallback joins: `5`
- train rows: `6`
- holdout rows: `4`
- train days: `2026-05-01` 到 `2026-05-03`
- holdout days: `2026-05-04` 到 `2026-05-05`

## 训练合同

允许进入 live predicate 的只能是 pre-action observable feature families：

- `status_before_action`
- `reason` / `block_reason`
- `source_sequence_present`
- `quote_intent_present`
- `source_order_present`
- `side`
- `offset_s` 派生 bucket
- pre-seed same/opp/open/deficit qty/cost 派生 bucket
- `candidate_qty` 派生 bucket
- `source_risk_direction`

明确禁止进入 live predicate：

- `source_pair_*`
- `source_residual_*`
- `pair_outcome_bucket`
- `residual_tail_outcome_bucket`
- realized pair cost
- settlement/redeem/future labels
- private truth
- public-profile 单日收益
- static side/offset/public-price cap
- D+ failed-family marker

## Offline Objective

labels 只能用于 train/holdout scoring：

- primary: maximize offline `source_pair_pnl`
- penalty: `source_residual_cost` 与 residual-tail outcome
- hard leakage rule: 任何 `source_pair/source_residual/outcome/realized/future` 字段进入 `predicate_feature_names` 都 fail closed

## 真实训练最小门槛

未来非 fixture 训练必须满足：

- train days `>=3`
- holdout days `>=2`
- train rows `>=100`
- holdout rows `>=50`
- selected train rows `>=50`
- selected holdout rows `>=20`
- single-day selected share `<=50%`
- source sequence coverage `>=0.95`
- join coverage `=1.0`

稳定性门槛：

- train objective positive
- holdout objective positive
- holdout residual cost share 不得比 train 恶化超过 50%
- holdout pair pnl 不得比 train 恶化超过 50%
- 至少两个 holdout days 有 selected rows
- 不允许退化成单一 static side/offset/public-price cap

## Rule Freeze Schema

未来 frozen rule 输出必须是 `frozen_observable_pre_action_rule_manifest_v1`，并显式包含：

- `predicate_expression`
- `predicate_feature_names`
- allowed/forbidden live feature families
- train/holdout label fields
- objective metrics
- stability gates
- same-window scope
- `uses_only_pre_action_fields=true`
- `uses_realized_pair_cost=false`
- `uses_future_labels=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate.passed=false`

## Denominator Dependency

任何 no-order proposal 前必须先跑 denominator replay：

- same-window denominator `>=100`
- rule matches `>=20`
- source_sequence coverage `>=0.95`
- aggregate parity true

## Smoke

smoke 覆盖：

- good fixture KEEP
- bridge scorer decision 非 KEEP fail closed
- joined row 允许 label 进 live predicate fail closed
- joined row 缺 offline label fail closed
- holdout split 缺失 fail closed
- bridge summary private truth claim fail closed
- denominator replay spec 非 KEEP fail closed

## 状态

这是训练合同 readiness，不是已训练规则，也不是策略证据。`current_real_input_status=UNKNOWN_NON_FIXTURE_JOINED_ROWS_MISSING`，`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，`private_truth_ready=false`，`deployable=false`，`promotion_gate.passed=false`。

下一步 local-only：实现 `observable_pre_action_rule_miner_training_fixture_scorer_v1`，用 synthetic joined rows 跑离线规则发现，证明 leakage/stability fail-closed；不得自动启动 no-order diagnostic。
