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

## Scoring Contract

D+ 后续评分必须按 `DPLUS_SCORING_CONTRACT_ZH.md` 分三层写清楚：

1. Research mechanism ranking：比较机制相对 control 是否更好。若 `fee_after_pnl` / `stress100_worst_pnl` / `worst_day_fee_after_pnl` 仍为正，但 residual 或 p90 相对 control 恶化，只能写成 `TRADEOFF`、`MECHANISM_WEAKER` 或 research-only DISCARD，不能写成策略经济失败。
2. Shadow / canary hard gate：用于防止错误 promotion。`candidates>=100`、no-order safety、`material_residual_lots=0` 继续是硬 blocker；residual 和 pair-cost tail 必须看归一化 budget，例如 `residual_qty / filled_qty`、`residual_cost / filled_cost`、以及 pair-tail-loss share of PnL。旧的固定 `residual_qty<=10`、`residual_cost<=5`、`net_pair_cost_p90<=1.0` 只能作为 legacy reference，不再作为 research ranking 的一票否决。
3. Final economic objective：最终看 `pair_pnl + residual_settle_or_mark_pnl - official_fee - stress_or_impact_budget` 是否稳定为正，并通过 OOS / walk-forward / source-of-truth replay。

因此，后续 artifact 必须区分 `DISCARD_MECHANISM_WEAKER`、`BLOCKED_PROMOTION_RISK_BUDGET` 和 `DISCARD_ECONOMIC_NEGATIVE`。不要再把 residual 单项恶化直接等同于策略净 PnL 失败。

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

每轮开始前先拆成两类工作：

- Critical path：当前主线程马上要跑的一个实验。不要把这个实验交给 subagent 后空等。
- Sidecar lanes：不阻塞 critical path、且 write set 不重叠的分析/实现/验证任务。只有这些任务才交给 subagent。

subagent 的输出必须能直接进入下一轮假设选择：候选机制、证伪理由、artifact 路径、或可执行 patch。只读探索如果不能改变下一步，就不要开。

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

- candidates < 100 时不能 promotion，但 research ranking 仍看风险调整 PnL。
- 归一化 pair-tail-loss budget 或 residual budget 超过 acceptance 时不能 promotion。
- `material_residual_lots > 0` 或 no-order safety 不通过。

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

## Git / Merge Rule

每次产生 material artifact、代码、docs 或 queue 更新后，按以下顺序收尾：

1. 只 stage 本轮必要的小型代码、docs、manifest、summary JSON/CSV；不要整目录提交 runtime artifacts。
2. 运行对应最小验证，例如 `py_compile`、`bash -n`、`git diff --check`，以及代码路径需要的 cargo/test。
3. commit 到 `codex/xuan-research`。
4. push `codex/xuan-research`。
5. 将当前 HEAD 推进 `main`。如果 `main` non-fast-forward，先 `git fetch origin main`，非破坏性 merge `origin/main` 到 `codex/xuan-research`，重新跑相关最小验证，再 push branch 和 `main`。
6. 禁止 force push，禁止回滚或覆盖他人改动。

目的：heartbeat 小步高频产出时，不能只 push feature branch 后长期不 merge；否则其他 agent 在 main 上继续推进会扩大冲突面。

## Kill List

以下方向已被 2026-05-19 的 candidate-stable / compliant artifacts 证伪，除非出现新机制，不再消耗 shadow 窗口或重复 sweep：

- 单独放宽或收紧 `public_trade_size`。
- `public_trade_size` 与 offset / first-price 的简单交互。
- trigger-time side-cost / immediate pair cap 过滤。
- public-trade price / size / slippage 薄口袋。
- residual cooldown cost cap sweep；最严格 cap 会记录 block，但不改变 seed/pair/residual/PnL 路径。

下一轮必须换成真正不同的机制，例如改变 seed admission、repair pairing、库存预算、或严格定义新的 compliant 标签语义。

## Heartbeat Operating Rule

每次 heartbeat 只允许以下三类输出之一：

1. `EXPERIMENT_DONE`: 一个假设、一个 artifact、一个 scoreboard delta、一个 next action。
2. `BLOCKED`: 明确 blocker、缺什么、谁能解除。
3. `NOOP`: 没有新数据，不骚扰用户。

不应把“又补了一个 smoke”作为主要进展，除非该 smoke 阻止了错误 promotion。

## Immediate Research Focus

当前优先级：

1. 不再继续只提高 edge；先尝试降低 residual_qty。
2. 不再围绕 public-trade trigger 薄口袋或 residual cooldown cap 微调。
3. 将 target / imbalance gating 的 zero-seed 配置提前过滤，避免浪费 sweep。
4. 研究真正改变库存路径的 residual repair / two-sided entry / late cutoff / dynamic side gating。
5. 对 local `POLY_BT_ROOT` strict/cache + completion + public-audit proxy truth 保持 candidate-stable 语义；public audit 缺失的 05-16/17 必须单独标注。
6. 一旦有一个机制在 compliant runner 中同时改善 sample、residual、worst-day，再用 new-EC2 same-host no-order shadow 验证；未通过前不进入 G2 canary。
