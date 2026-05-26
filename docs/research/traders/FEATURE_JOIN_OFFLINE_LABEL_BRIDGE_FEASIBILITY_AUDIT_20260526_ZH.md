# Feature-Join Offline Label Bridge Feasibility Audit

日期：2026-05-26

结论：`UNKNOWN_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_INPUT_CONTRACT_GAPS`

本轮只做 local-only feasibility audit：读取允许的 feature-join rows 与当前 worktree 内 legacy offline-label CSV，验证是否可以按已提交 bridge contract 生成 non-fixture training bridge rows。未读取 events JSONL，未扫描 raw/replay/full store，未 SSH，未启动 shadow/canary/live/local agg/shared WS，未修改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

## 输入

- feature rows: `xuan_research_artifacts/xuan_observable_pre_action_feature_join_handoff_driver_20260526T020736Z/remote_clean/output/observable_pre_action_candidate_rows.jsonl`
- feature scorer manifest: `xuan_research_artifacts/xuan_observable_pre_action_feature_join_handoff_completion_20260526T041336Z/feature_join_scorer/manifest.json`
- offline label CSV: `xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/candidate_seed_outcome_separator.csv`
- bridge contract: `xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_contract_20260525T213507Z/manifest.json`

## 结果

新 feature-join rows 本身可用，但不能和旧 label CSV 安全 join：

- feature rows: `4177`
- feature day: `2026-05-26`
- feature slugs: `14`
- feature statuses: admitted `277`、blocked `3900`
- feature exact ids: `source_seed_candidate_row_id=0`、`source_seed_action_id=0`
- label rows: `41580`
- label days: `2026-05-02` 到 `2026-05-18` 的 15 天
- label slugs: `4303`
- label exact ids: `source_seed_candidate_row_id=41580`、`source_seed_action_id=41580`
- day overlap: `0`
- slug overlap: `0`
- condition overlap: `0`
- strict composite base-key overlap: `0`
- joined rows: `0`
- join coverage: `0.0`

阻塞项：

- `feature_exact_id_fields_absent`
- `feature_label_condition_overlap_absent`
- `feature_label_day_overlap_absent`
- `feature_label_slug_overlap_absent`

## 解释

这不是 feature-join instrumentation failure。真实 no-order run 已经生成并验证了 row-level pre-action feature rows；缺的是同窗口 offline label source。旧 CSV 虽然有 15 天、41580 行和 post-action source_pair/source_residual labels，但它覆盖的市场窗口和本次 feature rows 完全不重叠，而且 feature rows 没有可直接回连旧 CSV 的 exact source ids。

因此当前不能生成 non-fixture `observable_pre_action_training_bridge_joined_rows.jsonl`，不能训练真实 frozen rule，也不能进入 denominator replay 或 no-order proposal。

`strategy_evidence=false`

`no_order_diagnostic_allowed=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`

## 下一步

继续 local-only：定义 `observable_pre_action_same_window_offline_label_handoff_contract_v1`。该合同应明确未来同窗口 label handoff 必须提供的文件、join keys、字段 provenance 和 fail-closed 规则，尤其要求 feature rows 与 offline labels 在 `source_seed_candidate_row_id/source_seed_action_id` 或 strict composite keys 上同窗重叠。不得从旧 CSV、fixture、public-profile single-day filters、static caps、realized pair_cost live criteria 或 future-label live rules 推导策略。
