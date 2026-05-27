# Backtest V1 HYPE Lead Prefix/Source Preflight Review

日期：2026-05-27（Asia/Shanghai）

## 结论

`xuan_backtest_v1_hype_lead_prefix_source_preflight_review_v1` 已完成。

真实决策：

`UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS`

Smoke 决策：

`KEEP_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_REVIEW_SMOKE_PASS`

解释：`hype-updown-5m` prefix 路径、runner 同窗口 handoff 输出、下游 materializer/inventory/accumulator/training contract 都能从当前源码中确认；但 backtest V1 lead 的完整 candidate filter 还不能被当前 runner CLI 精确表达。因此不能把这个 proposal 直接升级成可启动 diagnostic。

## 输入

只读当前 worktree 源码与已提交 proposal manifest：

- `xuan_research_artifacts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal_20260527T011815Z/manifest.json`
- `scripts/resolve_market_ids.py`
- `tools/xuan_dplus_passive_passive_shadow_runner.py`
- `scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py`
- `scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py`
- `scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory.py`
- `scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py`
- `scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py`

未执行 market resolver，未启动 runner，未 SSH，未读 events JSONL，未扫描 raw/replay/full store，未改 shared-ingress/broker/env/live，未发送 order/cancel/redeem。

## 已确认支持

`resolve_market_ids.py`：

- 支持通用 `--prefix`
- `slug_from_prefix(prefix, round_offset)` 直接拼接 prefix，不是 BTC-only allowlist
- 支持 `--format env`
- 输出 `POLYMARKET_MARKET_SLUG`、`POLYMARKET_YES_ASSET_ID`、`POLYMARKET_NO_ASSET_ID`
- 有 resolver cache 路径

Runner：

- 支持 `--prefix`
- `resolve_markets(Path(args.repo), args.prefix, args.round_offsets)` 使用 CLI prefix
- 支持 `--observable-pre-action-rule-miner-feature-join-output`
- 支持 `--observable-pre-action-same-window-offline-label-handoff-output`
- same-window handoff output 依赖 feature-join output，fail closed
- 启用 same-window handoff 时写 row-level `source_seed_candidate_row_id` 和 `source_seed_action_id`
- aggregate 输出 `observable_pre_action_candidate_rows.jsonl`、`observable_pre_action_same_window_offline_labels.csv`、`observable_pre_action_same_window_offline_label_handoff.json`

下游 contract：

- same-window handoff contract 检查 label policy
- materializer 使用 `exact_id:source_seed_candidate_row_id`
- multiday arrival inventory 检查 `source_handoff_id` 与 duplicate exact ids
- accumulator 检查 duplicate handoff/candidate/action exact id
- training scorer 保持 promotion/private/deployable 分离

## Candidate Filter 映射

当前 HYPE lead 参数：

- price `0.50..0.55`
- size `50..150`
- offset `0..30s`
- `max_l1_pair_ask=1.1`
- `max_l1_immediate_pair=2.0`
- `side_alignment=high`
- `pnl_cost_source=pair_ask`
- candidate key `30cfee296e1d0912f78109dd`

当前 runner 已有精确映射：

- `price_lo/price_hi` -> `--seed-px-lo/--seed-px-hi`
- `offset_lo/offset_hi` -> `seed_offset_min_s=0.0` + `--seed-offset-max-s`
- `max_l1_pair_ask` -> `--seed-l1-cap`

当前 runner 缺少精确 runtime adapter：

- `size_lo/size_hi`：没有 exact public trade size range CLI
- `max_l1_immediate_pair`：没有 exact immediate pair cap CLI
- `side_alignment=high`：没有 exact side-alignment CLI
- `pnl_cost_source=pair_ask`：没有 explicit pnl-cost-source CLI
- candidate key/filter manifest：没有 exact backtest V1 candidate filter manifest/key CLI

因此真实 blocker 是：

`candidate_filter_exact_runtime_adapter_absent`

## 未来批准前必须做的 Preflight

在任何 future approved diagnostic 之前，至少要先完成：

1. `python3 -m py_compile scripts/resolve_market_ids.py tools/xuan_dplus_passive_passive_shadow_runner.py`
2. `python3 scripts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review.py --proposal-manifest xuan_research_artifacts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal_20260527T011815Z/manifest.json --output-dir ${preflight_output_dir}`
3. `python3 scripts/resolve_market_ids.py --prefix hype-updown-5m --round-offset 0 --format env --cache-path ${market_resolver_cache}`
4. `python3 scripts/resolve_market_ids.py --prefix hype-updown-5m --round-offset 1 --format env --cache-path ${market_resolver_cache}`

第 3、4 步只是未来批准前的 resolver preflight 命令，本轮没有执行。

## Smoke

Smoke artifact：

`xuan_research_artifacts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review_smoke_20260527T015241Z/manifest.json`

覆盖：

- full support synthetic source：KEEP
- current-like candidate-filter gaps：UNKNOWN
- private claim：DISCARD
- proposal not KEEP：UNKNOWN
- resolver missing prefix：UNKNOWN
- runner missing handoff：UNKNOWN

## Safety

`research_ranking` 与 `promotion_gate` 分开报告：

- `prefix_source_preflight_review_complete=true`
- `resolver_prefix_source_supported=true`
- `runner_same_window_handoff_source_supported=true`
- `downstream_contracts_supported=true`
- `candidate_filter_exact_runtime_supported=false`
- `real_miner_input_ready=false`
- `strategy_evidence=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate.passed=false`

下一步 local-only：定义或实现一个默认关闭的 exact backtest V1 candidate-filter runtime adapter/spec，使 HYPE lead 参数能被 future handoff generator 精确表达；在此之前不得启动 no-order diagnostic。
