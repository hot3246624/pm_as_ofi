# Symmetric Activation Acceptance Adapter Spec

结论：`symmetric_activation_acceptance_adapter_spec_v1` 已本地固化为 fail-closed 合同。它只定义未来 allowlisted no-order pullback 该如何和常规 shadow acceptance 一起评分，不启动 shadow、不读取 events JSONL/raw stores、不改变 runner 行为。

## 合同

输入必须同时具备：

- `scripts/xuan_b27_dplus_shadow_trading_acceptance.py` 生成的常规 shadow acceptance manifest。
- `scripts/xuan_symmetric_activation_summary_scorer.py` 生成的 symmetric activation scorer manifest。
- 已 allowlisted pullback 中的 `output/aggregate_report.json`、`output/manifest.json`、`output/*.summary.json`。

硬门槛：

- `candidates >= 100`
- `net_pair_cost_p90 <= 1.0`
- `residual_qty_share <= 15%`
- `residual_cost_share <= 20%`
- `pair_tail_loss_share <= 5%`
- `material_residual_lots = 0`
- `fee_adjusted_pair_pnl_proxy > 0`
- blocked/admitted activation denominator 都非零
- `source_sequence` coverage 存在
- aggregate parity 通过

## 当前状态

当前只有 synthetic runner/scorer smoke summary，没有安全真实 pullback，所以 real input 状态保持 `UNKNOWN_NO_SAFE_PULLBACK`。这不是策略通过，也不是可部署证据；只是 adapter/spec ready。

## 禁止解释

不得把 fixture/scorer readiness 当成 strategy evidence。不得因为 adapter ready 直接启动 no-order diagnostic。任何下一步 no-order 诊断都需要用户明确批准 exact bounded run。
