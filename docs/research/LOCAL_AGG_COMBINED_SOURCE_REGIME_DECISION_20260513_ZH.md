# Local Agg Combined Source-Regime 候选决策包

更新时间：2026-05-13 20:16Z
生产 dry-run：`pm-local-agg-challenger.service`，run `20260513_045906`，server HEAD `08f6dc6`

## 当前状态

服务仍是 dry-run，未开启 live trading。最新安全检查：

- accepted=233，gated=299
- accepted_side_errors=0
- accepted_max_bps=6.893443
- accepted_p95_bps=3.747909
- latency_p95_ms=50.40，latency_max_ms=279.0

硬失败仍来自旧 DOGE accepted tail：`round_end_ts=1778670300`，`drop_binance`，`last_before`，`bybit;coinbase;okx`，误差 6.893443bps，方向正确。

新增重要状态：HYPE 在 `round_end_ts=1778697900` 出现一条 accepted 5.349395bps。该行方向正确但突破 max gate；这是已部署 HYPE selector 的未覆盖 regime，不是已知 DOGE 累计 max 的重复。

19:16Z replay 后新增重要状态：HYPE addendum 可以修复该 HYPE tail，但当前 run 又出现了另一条 DOGE accepted tail：

- `round_end_ts=1778699100`
- `source_subset=drop_binance`
- `rule=last_before`
- `local_sources=okx`
- `local_close=0.11298`
- `RTDS close=0.11291938`
- `close_diff_bps=5.368432`

因此，18:46Z 的 Option B 候选已经不再足以作为下一 dry-run checkpoint；必须加入 DOGE okx-only deeper-window addendum。

## 候选方案

候选名：`combined_hype_midspread_doge_sol_bnb_doge_okx_source_regimes`

它不是新 gate，而是源/时间窗口 regime selection：

- HYPE：沿用已部署的 source-lag regime selector。
- HYPE addendum：新增 `hyperliquid;okx` mid-spread regime，覆盖 spread 约 2.0-3.0bps、margin >=5bps 的 same-side pre-boundary Hyperliquid/slow-source选择。
- DOGE：same-side shallow pre-boundary source-window selector。
- DOGE addendum：新增 okx-only same-side deeper pre-boundary selector，覆盖 `local_sources=okx`、高 local margin 的单源 DOGE tail。
- SOL：只允许 `selected_depth=same_shallower` 且 `selected_source=coinbase` 的 causal debiased selector。
- BNB：只在 runtime local source set 包含 Bybit 时启用 min_train=20 的 causal debiased selector。

## 新增 HYPE 事件诊断

HYPE accepted tail：

- round_end_ts = `1778697900`
- source_subset = `drop_binance`
- rule = `after_then_before`
- local_sources = `hyperliquid;okx`
- source_spread_bps = 2.933430
- direction_margin_bps = 6.820188
- selected local_close = 39.206700
- RTDS close = 39.185738
- close_diff_bps = 5.349395

为什么已部署 HYPE selector 没有触发：

- 当前源集是 `hyperliquid;okx`。
- 已部署规则只覆盖 `hyperliquid;okx` 且 spread >= 4.0bps。
- 本次 spread=2.933430bps、margin=6.820188bps，因此未命中。

可见 boundary tape 证据：

- Hyperliquid pre-boundary candidate at offset -2863ms，price=39.1905，error≈1.215238bps。
- Coinbase slower candidate at offset -19113ms，price=39.19，error≈1.087641bps。

因果 grid replay：

- 在 HYPE accepted rows n=26 上，用已部署 HYPE selector 作为 base：max/p95/p99 = 5.349395 / 3.771700 / 4.974062。
- 新增 `hyperliquid;okx` mid-spread trigger 后：max/p95/p99 = 3.848062 / 3.488484 / 3.771700，side_errors=0。
- 触发 1 行，即当前 HYPE 5.349395bps tail。

结论：下一 runtime candidate 必须把这个 HYPE mid-spread regime 纳入 combined selector，否则 HYPE 仍可能独立造成 hard max failure。

## 离线固定 replay 证据

固定 replay runs：

- `20260511_083910`
- `20260512_014312`
- `20260513_045906`

Accepted rows n=934，已用 HYPE deployed selector 做归一化基线。

全局：

- base max/p95/p99 = 6.893443 / 2.979712 / 4.095850
- candidate max/p95/p99 = 4.984909 / 2.753598 / 3.999630
- side_errors = 0
- BTC unchanged

当前 run `20260513_045906`：

- base max/p95/p99 = 6.893443 / 3.270822 / 4.338356
- candidate max/p95/p99 = 4.531900 / 2.748921 / 3.745615
- side_errors = 0

Per-symbol summary before the HYPE addendum：

- DOGE max becomes 4.531900。
- SOL max/p95 becomes 4.093919 / 2.338273。
- BNB p95 improves 4.257723 -> 4.111051；<=1bps share improves 11.3% -> 39.6%。
- BTC p95/max remains 1.122772 / 3.033207。

HYPE addendum 不改变 BTC，且会把当前 HYPE max 从 5.349395 降到约 1.2-3.0bps，取决于选择 Hyperliquid closest/deepest 或 Coinbase slow candidate。推荐先用 Hyperliquid deepest pre-boundary candidate，因为它同源、same-side、offset -2863ms，且更接近 RTDS close。

## 19:16Z replay after HYPE addendum

将 HYPE `hyperliquid;okx` mid-spread addendum 合入 combined replay 后，固定三段 accepted rows n=969：

全局：

- base max/p95/p99 = 6.893443 / 3.053380 / 4.217880
- candidate max/p95/p99 = 5.368432 / 2.754067 / 4.089483
- side_errors = 0
- BTC unchanged

当前 run `20260513_045906`：

- base max/p95/p99 = 6.893443 / 3.438512 / 5.218596
- candidate max/p95/p99 = 5.368432 / 2.753316 / 4.198069
- side_errors = 0

关键解释：

- 旧 DOGE 6.893443 tail 会被 DOGE shallow-window selector 降到 3.665444bps。
- HYPE 5.349395 tail 会被 HYPE mid-spread addendum 降到 1.215238bps。
- 新 DOGE `local_sources=okx` tail 仍为 5.368432bps，成为新的 candidate max。

因此 18:46Z combined candidate 的决策应保持 `keep_research_only`，不能按原样实现并重启。

## DOGE okx-only tail preliminary diagnosis

新 DOGE tail 的 visible boundary tape 里有更好的 same-side deeper pre-boundary prices：

- best visible price around 0.11292，error≈0.054906bps，但大多在 boundary 前 10-28s，不能直接作为 deployable oracle-best。
- 一个因果窄触发原型：`local_sources=okx`、local margin >=30bps、same-side deeper pre-window candidate、pre_ms=30000、min_deeper=2bps，历史三段 accepted DOGE rows 上只触发当前这一行，side_errors=0，误差 5.368432 -> 2.711669。

该原型已在 19:46Z 被 formalized 到 combined replay harness。它仍是 current-only evidence，运行时必须窄触发并继续 dry-run 观察。

## 19:46Z replay after DOGE okx-only addendum

将 DOGE okx-only deeper-window addendum 合入 combined replay 后，固定三段 accepted rows n=978：

全局：

- base max/p95/p99 = 6.893443 / 3.069745 / 4.217712
- candidate max/p95/p99 = 4.984909 / 2.753222 / 4.057799
- side_errors = 0
- BTC unchanged：p95/max = 1.142648 / 3.033207

当前 run `20260513_045906`：

- base max/p95/p99 = 6.893443 / 3.453884 / 5.145021
- candidate max/p95/p99 = 4.531900 / 2.742623 / 3.981908
- side_errors = 0

Per-symbol candidate：

- DOGE max/p95 = 4.531900 / 3.833147，side_errors=0。
- HYPE max/p95 = 3.848062 / 3.488484，side_errors=0。
- SOL max/p95 = 4.093919 / 2.436279，side_errors=0。
- BNB max/p95 = 4.984909 / 4.109270，side_errors=0。

Selected evidence:

- DOGE 6.893443 -> 3.665444 via Binance pre-boundary shallow window.
- DOGE okx-only 5.368432 -> 2.711669 via OKX same-side deeper window at offset -4523ms.
- HYPE 5.349395 -> 1.215238 via Hyperliquid mid-spread pre-boundary window.

结论：新 combined candidate 再次把 fixed replay 的 all-run 和 current-run max 推回 `<5bps`，且没有 side/BTC regression。它可以作为新的 Decision Needed runtime proposal，但不能 unattended deploy。

## 20:16Z complexity-adjusted replay

实现审查发现：SOL/BNB replay 使用的是历史 source-lag selector + signed-error debias。它不是纯规则；要在 runtime 等价实现，需要额外的 selector history/model 文件、在线/离线统计一致性和更复杂的状态管理。

因此额外跑了一个更保守的 deterministic HYPE+DOGE-only 候选：

- HYPE deployed selector + HYPE mid-spread addendum
- DOGE shallow-window selector
- DOGE okx-only deeper-window addendum
- SOL/BNB 先不改变 runtime selection

固定三段 accepted rows n=985：

- global base max/p95/p99 = 6.893443 / 3.091145 / 4.224368
- deterministic candidate max/p95/p99 = 4.984909 / 3.029067 / 4.111681
- side_errors = 0
- BTC unchanged：p95/max = 1.140092 / 3.033207

当前 run `20260513_045906`：

- base max/p95/p99 = 6.893443 / 3.747908 / 5.087797
- deterministic candidate max/p95/p99 = 4.531900 / 3.413421 / 4.269339
- side_errors = 0

解释：

- deterministic HYPE+DOGE-only 与 full combined 的 all-run max 都受 BNB historical 4.984909 限制。
- full combined 的 p95 更好，但 runtime 风险显著更高。
- 按 autoresearch 的 complexity-adjusted rule，推荐先实现 deterministic HYPE+DOGE-only 作为下一 dry-run checkpoint；SOL/BNB 保持 shadow/research，等有可部署的 selector-history 机制后再考虑。

Remaining limiter：

- BNB historical max 4.984909 from `20260511_083910` remains unrepaired.
- Boundary tape attribution found no better visible close candidate for that row, so further reduction likely needs richer data such as bid/ask/microprice/depth or different target alignment.

## Implementation Scope If Approved

Expected files:

- `src/bin/polymarket_v2.rs`
- `scripts/research_local_agg_combined_source_regimes.py` only as mirrored offline evaluator, already updated on research branch
- optional focused runtime replay/check script if needed

Runtime shape:

- Extend the existing HYPE source-lag regime path with the new `hyperliquid;okx` mid-spread trigger.
- Add DOGE selector next to the HYPE source-lag regime path.
- Add SOL and BNB causal selector helpers using only decision-time fields already present at runtime:
  - source lag candidates
  - local source set
  - selected source/depth/lag bucket
  - historical selector stats from prior rows/runs only
- Keep dry-run/lab only.
- Do not change thresholds.
- Do not relax gates.
- Do not enable live orders.
- Do not restart shared ingress.

Validation before restart:

- `python3 -m py_compile` for changed research scripts
- fixed replay over the three benchmark runs
- `cargo check --bin polymarket_v2`
- `cargo build --release --bin polymarket_v2`

Deployment if approved:

- sync/build on EC2 under `/srv/pm_as_ofi/repo`
- restart only `pm-local-agg-challenger.service`
- new checkpoint clock starts at restart time
- preserve `08f6dc6` / run `20260513_045906` as baseline

## Risks

- This changes per-symbol source-window/model selection, not just diagnostics.
- The HYPE addendum is based on one new current-run trigger, so it should be implemented narrowly and watched closely after restart.
- DOGE selected_count is small; the current max repair is strong but sparse.
- SOL has stronger selected_count but still relies on a narrow trigger.
- BNB trigger improves p95/<=1bps, but its historical max remains 4.984909.
- Runtime implementation must exactly match offline selector semantics; mismatches could create false confidence.
- Restart resets the current run and will run the current worktree state, not the original `08f6dc6` binary state.

## Options

Option A: keep research only.

- No restart.
- Continue collecting dry-run samples under deployed HYPE-only selector.
- Next work focuses on schema enrichment and XRP/BNB refinement.
- Lowest operational risk, but current service remains hard-failed on cumulative DOGE max.

Option B1: implement deterministic HYPE+DOGE selector in dry-run.

- Change runtime source-window/model selection only for HYPE and DOGE.
- Include HYPE mid-spread addendum and both DOGE selectors.
- Do not implement SOL/BNB debiased selector in the first runtime checkpoint.
- Build/restart only `pm-local-agg-challenger.service`.
- Start a new dry-run checkpoint.
- 20:16Z evidence shows all-run candidate max 4.984909 and current-run candidate max 4.531900, side_errors=0, BTC unchanged.
- This is now the recommended next dry-run checkpoint if the user approves a challenger restart.

Option B2: implement full combined selector in dry-run.

- Change runtime source-window/model selection for HYPE, DOGE, SOL, and BNB, including DOGE okx-only deeper-window addendum.
- Requires a runtime source-lag selector/history model for SOL/BNB to match offline debiased semantics.
- Build/restart only `pm-local-agg-challenger.service`.
- Start a new dry-run checkpoint.
- 19:46Z evidence now shows all-run candidate max 4.984909 and current-run candidate max 4.531900, side_errors=0, BTC unchanged.
- This improves p95 more than B1 but has higher implementation risk; do not make it the first runtime checkpoint without an explicit selector-history design.

Option C: implement diagnostics/schema enrichment first.

- Add bid/ask/microprice/depth capture where low risk.
- Do not alter selected close price yet.
- Slower path, but addresses the BNB residual max that visible close tape cannot repair.

## Recommendation

Decision Needed: recommend Option B1 as the next dry-run checkpoint, provided the user explicitly approves a challenger restart. The deterministic HYPE+DOGE candidate restores the hard max gate in replay without side/BTC regression and avoids the SOL/BNB runtime model complexity.

Do not implement/restart unattended. The DOGE okx-only addendum is current-only evidence, and BNB historical max 4.984909 remains unrepaired, so Option C schema enrichment and SOL/BNB selector-history design should run next in parallel after any dry-run checkpoint reset.
