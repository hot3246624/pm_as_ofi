# CE25 projected guard runner instrumentation

时间：2026-05-24

## 结论

`ce25_projected_guard_summary_v1` 已在 no-order shadow runner 中实现为默认关闭、summary-only 诊断。它只读取下单前库存与 intended seed，不改变订单生成、size、block、pairing、salvage 或任何交易行为。

决策标签：`KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY`

这不是可部署策略、不是 private truth、不是 canary/promotion evidence。它只是把 ce25/b55 public proxy 中的 realized `pair_cost/residual` 桶，翻译成 runner 可观测的 projected pre-action 诊断字段。

## 新增接口

CLI flag：

```bash
--ce25-projected-guard-event-lite-summary
```

依赖：

```bash
--event-lite-summary
```

缺少 `--event-lite-summary` 时 fail closed。

## 输出位置

开启后，每个 summary 和 aggregate 中新增：

```json
event_lite.ce25_projected_guard_summary
```

schema：

```text
ce25_projected_guard_summary_v1
```

核心字段：

- `decision_count_by_status`
- `decision_count_by_guard`
- `decision_count_by_asset_timeframe_guard_status`
- `projected_pair_cost_bucket_by_guard`
- `projected_residual_bucket_by_guard`
- `final_window_policy_count`

field contract 固定声明：

- `default_off=true`
- `source=pre_action_inventory_plus_intended_no_order_seed`
- `estimated_fee_per_share_policy=0.0_for_passive_no_order_seed_proxy_only`
- `post_action_outcome_labels_included=false`
- `realized_pair_cost_used_as_live_criteria=false`
- `trading_behavior_changed=false`
- `private_truth_ready=false`
- `deployable=false`
- `promotion_gate_passed=false`

## 诊断规则

Projected fields 来自 candidate append 前的状态：

- side=YES 时，同侧为 YES，反侧为 NO；side=NO 时反过来。
- `projected_pair_cost = projected_yes_actual_cost/projected_yes_qty + projected_no_actual_cost/projected_no_qty`
- `projected_residual_rate_on_bought_qty = abs(projected_yes_qty - projected_no_qty) / (projected_yes_qty + projected_no_qty)`

诊断 guard：

- starter：ETH/SOL 5m/15m，projected pair cost `<0.90`，projected residual `<15%`
- core：BTC/ETH/SOL 5m/15m，projected pair cost `<0.95`，projected residual `<10%`
- hard kill diagnostic：projected pair cost `>=1.00`，或 close 前 projected residual `>20%`
- final window diagnostic：最后 60s initiation、最后 1-5m 非 completion/cleanup 会被标记为 would-block

这些标签当前只计数，不参与 live decision。

## Smoke

Artifact：

```text
xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z/manifest.json
```

Smoke 覆盖：

- default-off 下 `ce25_projected_guard_summary` 不出现
- enabled 下 summary 和 aggregate 均出现 schema `ce25_projected_guard_summary_v1`
- starter ETH 15m projected pair cost `<0.90` 计为 allow
- core BTC 5m projected pair cost `0.90-0.95` 计为 allow
- SOL 15m projected pair cost `>=1.00` 计为 hard-kill diagnostic
- default 与 enabled 的 candidate 数一致，证明不改行为
- CLI dependency fail closed

## 下一步

实现 `ce25_projected_guard_summary_scorer_v1`。它应只读取 summary/aggregate manifest，验证 schema、field contract、默认关闭/开启行为、aggregate parity、projected bucket denominators，并在缺 summary 或字段显示 behavior-changing/private/deployable/promotion 时 fail closed。
