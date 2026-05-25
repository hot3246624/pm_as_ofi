# Observable Pre-Action Rule Miner Training Input Bridge Contract

结论：`KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY`。

本轮只做 current-worktree local-only contract/spec。没有 fetch 新数据，没有读取外部 worktree，没有 SSH，没有启动 shadow/canary/local agg/shared WS/live，没有读取或拉取 events JSONL，没有扫描 raw/replay/full store，也没有发送 order/cancel/redeem。

## 目标

当前 observable pre-action miner 已经有两块材料，但还不能直接训练：

- 未来 feature-join output 可以提供行级 pre-action/status/source truth。
- 旧 `candidate_seed_outcome_separator.csv` 可以提供 offline `source_pair/source_residual` labels。

本轮把二者未来如何安全 join 写成合同。该合同只定义训练输入桥，不产生真实策略证据，不授权 no-order diagnostic。

## 已核对的支撑项

- `KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_READY_EXPORTER_TARGET_NAMED`
- `KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY`
- `KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY`
- `UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS`
- runner 已存在默认关闭 feature-join output tokens。

旧 CSV label 源：

- path: `xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/candidate_seed_outcome_separator.csv`
- rows: `41580`
- days: `15`
- offline label fields: `source_pair_*`、`source_residual_*`、`pair_outcome_bucket`、`residual_tail_outcome_bucket`

## Join 合同

优先 exact key：

- `source_seed_candidate_row_id`
- fallback: `source_seed_action_id`

若 exact id 不同时存在，则用 composite fallback：

- exact equality: `condition_id`、`market_slug/slug`、`day_id/day`、`side`、`source_risk_direction`
- time tolerance: `quote_ts_ms` 对 `ts_ms`，最大 `250ms`，`trigger_ts_ms` 只作备用
- offset tolerance: `0.25s`
- pre-seed qty/cost tolerance: `1e-6`
- candidate qty 对 `seed_qty`，最大 `1e-6`，`trigger_size` 只作备用

必须一对一：

- feature row 最多匹配一个 label row
- label row 不可复用
- zero-match、multi-match、timestamp/offset/pre-seed/candidate mismatch 都 fail closed

## Label 隔离

`source_pair/source_residual`、outcome bucket 等字段只能用于 offline train/holdout scoring。

冻结 live predicate 只能使用 feature-join rows 里的 pre-action observable fields；不能使用 realized pair cost、post-action labels、settlement/redeem/future labels、public-profile 单日收益、static side/offset/public-price cap 或 D+ failed-family marker。

## Denominator 依赖

bridge 只负责把 feature rows 与 offline labels 安全对齐。真正生成 miner input manifest 前，还必须由 denominator replay scorer 在同一窗口生成：

- `observable_pre_action_no_order_denominator_summary.json`
- same-window denominator `>=100`
- rule matches `>=20`
- source sequence coverage `>=0.95`
- admitted/blocked counts 存在
- aggregate parity 为 true

## Smoke

smoke 覆盖：

- good path: contract KEEP
- 缺 offline label 字段：UNKNOWN fail closed
- 缺 join key：UNKNOWN fail closed
- feature-join scorer decision 异常：UNKNOWN fail closed

## 状态

`strategy_evidence=false`，`no_order_diagnostic_allowed=false`，`private_truth_ready=false`，`deployable=false`，`promotion_gate.passed=false`。

下一步 local-only：实现 `observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_v1`，用 synthetic feature rows 和 offline-label fixtures 验证一对一 join、tolerance、label separation 与 fail-closed 行为；不得自动启动 no-order diagnostic。
