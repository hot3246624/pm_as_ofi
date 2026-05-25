# Observable Pre-Action Training Input Bridge Fixture Scorer

结论：`KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY`。

本轮只做 current-worktree local-only fixture scorer。没有 fetch 新数据，没有读取外部 worktree，没有 SSH，没有启动 shadow/canary/local agg/shared WS/live，没有读取或拉取 events JSONL，没有扫描 raw/replay/full store，也没有发送 order/cancel/redeem。

## 新增内容

- `scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer.py`
- `scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_smoke.sh`
- artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/manifest.json`
- smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_smoke_20260525T220507Z/manifest.json`

## Scorer 合同

输入：

- `observable_pre_action_candidate_rows.jsonl`
- `observable_pre_action_source_link_summary.json`
- `observable_pre_action_feature_join_manifest.json`
- offline label CSV
- `frozen_observable_pre_action_rule_manifest.json`
- bridge contract manifest

输出：

- `observable_pre_action_training_bridge_joined_rows.jsonl`
- `observable_pre_action_training_bridge_summary.json`
- scorer `manifest.json`

## Good Fixture 结果

- joined rows: `10`
- exact-id joins: `5`
- composite fallback joins: `5`
- train rows: `6`
- holdout rows: `4`
- unused rows: `0`

Good path 同时验证：

- exact id join: `source_seed_candidate_row_id`
- composite fallback join: `condition_id`、`market_slug/slug`、`day_id/day`、`side`、`source_risk_direction`，加 timestamp/offset/pre-seed/candidate tolerance
- label row 不复用
- feature row 不含 post-action labels
- offline labels 只允许 train/holdout scoring，不允许 live predicate
- feature/source manifest row-count provenance parity

## Fail-Closed Smoke

smoke 覆盖以下 bad cases，全都返回 UNKNOWN：

- 缺 feature join key
- duplicate/multiple composite matches
- timestamp tolerance 外
- 缺 offline label field
- frozen rule 把 `source_pair_qty` 用作 live predicate
- source summary row-count mismatch
- feature manifest claim `deployable=true`

## 解释

这一步证明 bridge join 机制能被本地 fixture 精确表达，而且 label leakage / provenance mismatch 会 fail closed。

它仍然不是策略收益证据，因为当前 worktree 还没有非 fixture feature-join pullback，也没有真实 same-window denominator replay。`strategy_evidence=false`、`no_order_diagnostic_allowed=false`、`private_truth_ready=false`、`deployable=false`、`promotion_gate.passed=false`。

## 下一步

下一步 local-only：等待或盘点非 fixture feature-join pullback，或实现 offline miner training spec，使未来真实 bridge joined rows 能进行 train/holdout rule discovery。不得自动启动 no-order diagnostic。
