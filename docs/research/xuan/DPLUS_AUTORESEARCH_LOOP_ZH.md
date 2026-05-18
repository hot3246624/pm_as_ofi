# Xuan B27 D+ Autoresearch Loop

本文定义 `xuan_b27_dplus` 后续研究推进方式。目标是提高策略研究吞吐，而不是继续堆叠与上线无关的 gate。

## 当前问题

过去的推进方式过于串行：

1. 先补安全 gate，再补 artifact，再补 smoke，容易把工程安全进展误当成策略进展。
2. backtest / shadow 的结论没有长期统一 scoreboard，容易反复讨论单个正 PnL。
3. subagent 没有清晰 lane 和 write-set 边界，导致不能真正并行。
4. 自动化 prompt 过度携带历史状态，缺少“本轮必须提出假设、跑实验、记录淘汰或晋级”的执行约束。

正确方向是 autoresearch：每轮只做一个可证伪的策略假设，跑完后把结果压缩进同一张 scoreboard，并明确下一步是 kill、tune、还是 promote-to-shadow。

## 固定 Scoreboard

任何 D+ backtest / dry-run / shadow / probe 结果必须记录：

- `data_root`
- `dataset_type`
- `labels`
- `days`
- `market_prefix`
- `assets`
- `row_count`
- `excluded_20260514_20260515`
- `contains_20260518`
- `includes_public_account_execution_truth_v1`
- `can_support_strategy_promotion`
- `candidate_count` 或 `seed_actions`
- `pair_actions`
- `pair_qty`
- `pair_cost_wavg`
- `net_pair_cost_p50/p90`，如果可得
- `residual_qty`
- `residual_cost`
- `qty_residual_rate`
- `cost_residual_rate`
- `net_pnl`
- `stress100_actual_pnl`
- `worst_residual_net_pnl`
- `stress100_worst_pnl`
- `positive_stress100_worst_run_count`
- `orders_sent=false`
- `cancels_sent=false`
- `redeems_sent=false`

只要 `can_support_strategy_promotion=false`，结论只能写成 research-only，不能写成 canary-ready。

## Promotion Gate

D+ 进入 G2 canary 讨论前，至少需要同时满足：

1. 合规数据：strict/cache + completion store + public account truth 可见，且报告声明完整数据范围。
2. 数值通过：OOS / walk-forward 不只是 nominal PnL 正，还要 stress / residual 指标合格。
3. no-order shadow 通过：更大样本 passive/passive shadow report 通过 pair/PnL/residual acceptance。
4. 残仓尾部通过：`stress100_worst_pnl > 0` 或有明确、可验证的残仓修复机制。
5. source-of-truth / recorder / safety gates 继续保持 PASS。

当前 20260518 状态：scope-limited completion probe 在 edge 7% 时 `worst_residual_net_pnl` 刚转正，但 `stress100_worst_pnl` 仍为负，且 residual_qty 未下降。这是 break-even research signal，不是 promotion evidence。

## Subagent Lanes

并行 subagent 必须使用 disjoint write sets。不要让多个 agent 同时改同一脚本。

### Lane A: Residual Tail Research

Owner files:

- `tools/xuan_d_branch_passive_passive_redeem.py`
- `scripts/xuan_b27_dplus_scope_limited_completion_passive_probe_summary.py`
- 新增 `scripts/xuan_b27_dplus_*residual*_probe*.py`

Mission:

- 降低 `residual_qty`、`qty_residual_rate`、`stress100_worst_pnl`。
- 不允许只靠提高 edge 来掩盖残仓。

Kill criteria:

- 连续 3 轮没有降低 residual rate。
- 需要超过 7% passive discount 才能 break even。
- positive result 只来自 zero-seed 或极低样本配置。

### Lane B: Signal / Selection Research

Owner files:

- `src/bin/pair_arb_backtest.rs`
- `scripts/xuan_b27_dplus_pair_arb_backtest_grid.py`
- `scripts/xuan_b27_dplus_pair_arb_oos_compare.py`
- `scripts/xuan_b27_dplus_pair_arb_walkforward_compare.py`

Mission:

- 找到减少单边残仓的 entry / pairing / skip logic。
- 优先研究 two-sided entry、late-round cutoff、side-imbalance gating、dynamic target。

Kill criteria:

- OOS positive 但 sample 太薄。
- `residual_loss_rate` 或 `residual_window_rate` 不降。
- walk-forward 只能靠 scope-limited local SQLite 成立。

### Lane C: Shadow Trading Metrics

Owner files:

- `tools/xuan_dplus_passive_passive_shadow_runner.py`
- `scripts/xuan_b27_dplus_shadow_trading_acceptance.py`
- `scripts/xuan_b27_dplus_shadow_trading_report_discovery.py`

Mission:

- 输出标准 pair/PnL/residual shadow report。
- 在 shared-ingress read-only client 可用时，直接跑 no-order shadow；不可用则 fail closed。

Kill criteria:

- candidates < 100。
- `net_pair_cost_p90 > 1.0`。
- residual lots 或 residual cost 超过 acceptance。

### Lane D: Compliant Data Adapter

Owner files:

- `scripts/xuan_b27_dplus_compliant_backtest_input_preflight.py`
- `scripts/xuan_b27_dplus_compliant_backtest_run_plan.py`
- 新增 compliant strict/cache + completion adapter scripts。

Mission:

- 让研究结果从 scope-limited 升级到 promotion-capable dataset。
- 不读 raw/replay，不扫 `/mnt/poly-replay`，只读允许 manifest/store。

Kill criteria:

- 缺 public account truth。
- 缺 strict/cache 或 completion store。
- 数据包含 20260514/20260515 或未完整的 20260518。

### Lane E: Gate / Status Hygiene

Owner files:

- `scripts/xuan_b27_dplus_local_status_bundle.py`
- `scripts/xuan_b27_dplus_canary_readiness_plan.py`
- docs / queue。

Mission:

- 只维护 promotion blocker 和 scoreboard 汇总。
- 不新增与策略判断无关的 micro-gate。

Kill criteria:

- 新 gate 不改变 promotion decision。
- 新 gate 只是重复已有安全检查。

## Heartbeat Operating Rule

每次 heartbeat 只允许以下三类输出之一：

1. `EXPERIMENT_DONE`: 一个假设、一个 artifact、一个 scoreboard delta、一个 next action。
2. `BLOCKED`: 明确 blocker、缺什么、谁能解除。
3. `NOOP`: 没有新数据，不骚扰用户。

不应把“又补了一个 smoke”作为主要进展，除非该 smoke 阻止了错误 promotion。

## Immediate Research Focus

当前优先级：

1. 不再继续只提高 edge；先尝试降低 residual_qty。
2. 将 target / imbalance gating 的 zero-seed 配置提前过滤，避免浪费 sweep。
3. 研究 residual repair / two-sided entry / late cutoff / side-imbalance gating。
4. 一旦 shared-ingress read-only client 可用，跑更大 no-order shadow report。
5. 合规 store 可见后，复跑 promotion-capable OOS/walk-forward。
