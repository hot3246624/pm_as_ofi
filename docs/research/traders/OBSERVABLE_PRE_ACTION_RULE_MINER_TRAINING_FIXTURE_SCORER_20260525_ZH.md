# Observable Pre-Action Rule Miner Training Fixture Scorer

结论：`KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_READY`。

本轮只做 current-worktree local-only fixture scorer。没有 fetch 新数据，没有读取外部 worktree，没有 SSH，没有启动 shadow/canary/local agg/shared WS/live，没有读取或拉取 events JSONL，没有扫描 raw/replay/full store，也没有发送 order/cancel/redeem。

## 新增内容

- `scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py`
- `scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_smoke.sh`
- artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_20260525T230507Z/manifest.json`
- frozen rule fixture: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_20260525T230507Z/frozen_observable_pre_action_rule_manifest.json`
- smoke artifact: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_smoke_20260525T230507Z/manifest.json`

## Scorer 行为

scorer 读取已提交 training spec 和 synthetic joined rows，只枚举 allowed pre-action feature families：

- status/reason/block_reason
- source presence booleans
- side
- offset bucket
- pre-seed qty/cost buckets
- candidate qty bucket
- source risk direction

它只把 `source_pair/source_residual` labels 用作 offline train/holdout objective：

`objective = source_pair_pnl - source_residual_cost - residual_tail_penalty`

## Good Fixture 结果

选出的 frozen predicate：

`pre_seed_same_qty_bucket == "pre_seed_same_qty_zero"`

指标：

- selected rows: `4`
- train rows: `2`
- holdout rows: `2`
- train days: `2026-05-01`, `2026-05-02`
- holdout days: `2026-05-04`, `2026-05-05`
- train objective: `+0.4`
- holdout objective: `+0.4`
- top-day selected share: `25%`

输出 frozen rule 保持：

- `uses_only_pre_action_fields=true`
- `uses_realized_pair_cost=false`
- `uses_future_labels=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate.passed=false`
- denominator replay required before any no-order proposal

## Fail-Closed Smoke

smoke 覆盖：

- forbidden live feature
- label leakage into live predicate
- insufficient train/holdout support
- single-day concentration
- holdout objective negative
- denominator dependency missing
- private/deployable claim

所有 bad cases 都返回 UNKNOWN。

## 解释

这一步证明 offline rule discovery mechanics 可以在 joined-row contract 上安全表达，并且能输出一个 fixture-only frozen rule manifest。

这不是策略证据，也不是 private truth 或 deployable evidence。当前真实非 fixture joined rows 仍缺失；不得自动启动 no-order diagnostic。

下一步 local-only：回到 real input inventory / safe feature-join pullback wait，只有在未来当前 worktree 出现非 fixture feature-join + bridge joined rows 后，才可跑同一套 scorer。不得自动启动 no-order diagnostic。
