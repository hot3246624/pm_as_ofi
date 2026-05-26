# Observable Pre-Action Same-Window Label Handoff Runner Output

日期：2026-05-26

结论：`KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_RUNNER_OUTPUT_SMOKE_PASS`

用户已批准一个 exact bounded no-order diagnostic，用于生成同窗口 feature rows + offline labels handoff。启动前先补齐 runner 的默认关闭输出能力，避免启动后仍缺目标 handoff 文件。

## 实现

`tools/xuan_dplus_passive_passive_shadow_runner.py` 新增默认关闭 flag：

`--observable-pre-action-same-window-offline-label-handoff-output`

该 flag 依赖：

- `--event-lite-summary`
- `--source-link-transition-event-lite-summary`
- `--observable-pre-action-rule-miner-feature-join-output`

关闭时不产生任何 same-window label handoff 文件；开启时在已有 feature-join 输出之外，写出：

- `observable_pre_action_same_window_offline_labels.csv`
- `same_window_aggregate_summary.json`
- `observable_pre_action_same_window_offline_label_handoff.json`

offline label CSV 为每条 pre-action feature row 生成一条同窗口 label row，并用 `source_seed_candidate_row_id` 做 exact join。`source_pair_*` / `source_residual_*` 只写入 CSV label 文件，不进入 feature rows，也不能作为 live predicate。

## 安全边界

- 不改变 candidate/block/fill/pair 行为
- 不读或拉 events JSONL
- 不扫 raw/replay/full store
- 不发送 order/cancel/redeem
- 不改 shared-ingress/broker/env/live
- 不声明 private truth、deployable、promotion 或 canary readiness

## Smoke

Smoke artifact：

`xuan_research_artifacts/xuan_observable_pre_action_same_window_label_handoff_runner_output_smoke_20260526T091900Z/manifest.json`

验证内容：

- default-off 不产生 handoff/label 文件
- enabled 产生 feature rows、offline label CSV、same-window aggregate summary 和 handoff manifest
- candidate/block/fill/pair 计数不变
- feature rows 不包含 post-action labels
- offline labels 标记为 train/holdout-only post-action labels
- handoff manifest 使用相对路径时，合同验证器可通过
- exact candidate row id join coverage 为 100%
- CLI 缺 feature-join 依赖时 fail closed

## 状态

该 KEEP 只是 runner 输出能力 ready，不是策略证据。下一步才是启动已批准的 bounded no-order diagnostic，使用该新 flag 生成真实同窗口 handoff。

`strategy_evidence=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`
