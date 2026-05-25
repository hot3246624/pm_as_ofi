# Observable Pre-Action Rule Miner Real Input Inventory

结论：`UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS`。

本轮只做当前 worktree 的 local-only real-input inventory。没有 fetch 新数据、没有读取外部 worktree、没有 SSH、没有启动 shadow/canary/local agg/shared WS/live、没有读取或拉取 events JSONL、没有扫描 raw/replay/full store，也没有发送 order/cancel/redeem。

## 已检查内容

- 已提交 observable pre-action miner spec、feature-join output scorer、denominator replay fixture scorer artifacts。
- 当前 worktree 内的 `observable_pre_action_*` materialized JSON/JSONL。
- 当前 worktree 内候选 CSV：`xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/candidate_seed_outcome_separator.csv`。
- 当前 worktree allowlisted pullback summaries under `remote_clean/output`，只检查 `aggregate_report.json` 和 `*.summary.json` 文件存在性/目录形态，不读取 events JSONL。

## 结果

当前没有可直接喂给 `observable_pre_action_rule_miner_input_v1` 的非 fixture manifest。

具体 blockers：

- `non_fixture_observable_pre_action_input_manifest_absent`
- `non_fixture_feature_join_outputs_absent_or_incomplete`
- `non_fixture_denominator_summary_absent`
- `legacy_candidate_csv_not_assemble_ready`
- `allowlisted_pullbacks_lack_observable_pre_action_outputs`

## 证据

旧 candidate CSV 仍有价值，但不能直接作为 miner 输入：

- rows: `41580`
- days: `15`
- 离线 label 字段存在：`source_pair_*`、`source_residual_*`、`pair_outcome_bucket`、`residual_tail_outcome_bucket`
- 缺真实 pre-action contract 字段：`status_before_action`、`block_reason`
- `quote_intent_id/source_order_id/source_sequence_id` 全为空
- `external_shadow_ids_available_true=0`

allowlisted pullback output directories:

- count: `13`
- directories with `observable_pre_action_candidate_rows.jsonl` + source summary + feature manifest: `0`

existing denominator summaries:

- count: `3`
- all are fixture/smoke denominator replay outputs, not non-fixture source.

## 解释

这不是策略失败的新证据，而是输入桥缺口：我们已有两边材料，但还没有合法的同窗、同粒度、非 fixture materialized bridge。

- 一边是旧 CSV：有 train/holdout offline labels，但缺真实行级 source/status/denominator。
- 另一边是 feature-join instrumentation/scorer：能生成 pre-action 行级 source/status，但目前只有 local smoke 或未来 pullback 才会产生真实材料，并且它不带 offline labels。

## 下一步

下一步 local-only：`observable_pre_action_rule_miner_training_input_bridge_contract_v1`。

它应定义如何在未来 allowlisted feature-join output 出现后，把 row-level pre-action features 与旧 offline labels / same-window denominator 安全 join 成 miner input；只能使用 condition/slug/day/quote_ts/side/offset 等安全 key，并继续禁止 events JSONL/raw/replay/SSH/shadow/canary/live。不得自动启动 no-order diagnostic。
