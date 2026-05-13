# Local Agg Combined Source-Regime 候选决策包

更新时间：2026-05-13 18:16Z  
生产 dry-run：`pm-local-agg-challenger.service`，run `20260513_045906`，server HEAD `08f6dc6`

## 当前状态

服务仍是 dry-run，未开启 live trading。最新安全检查：

- accepted=195，gated=267
- accepted_side_errors=0
- accepted_max_bps=6.893443
- accepted_p95_bps=3.300088
- latency_p95_ms=51.0，latency_max_ms=279.0

硬失败仍来自旧 DOGE accepted tail：`round_end_ts=1778670300`，`drop_binance`，`last_before`，`bybit;coinbase;okx`，误差 6.893443bps，方向正确。HYPE 已有 1 条 accepted，误差 1.965602bps；此前高误差 HYPE 行仍被 gate 拦截。

## 候选方案

候选名：`combined_hype_doge_sol_bnb_source_regimes`

它不是新 gate，而是源/时间窗口 regime selection：

- HYPE：沿用已部署的 source-lag regime selector。
- DOGE：same-side shallow pre-boundary source-window selector。
- SOL：只允许 `selected_depth=same_shallower` 且 `selected_source=coinbase` 的 causal debiased selector。
- BNB：只在 runtime local source set 包含 Bybit 时启用 min_train=20 的 causal debiased selector。

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

Per-symbol summary：

- DOGE max becomes 4.531900。
- SOL max/p95 becomes 4.093919 / 2.338273。
- BNB p95 improves 4.257723 -> 4.111051；<=1bps share improves 11.3% -> 39.6%。
- BTC p95/max remains 1.122772 / 3.033207。

Remaining limiter：

- BNB historical max 4.984909 from `20260511_083910` remains unrepaired.
- Boundary tape attribution found no better visible close candidate for that row, so further reduction likely needs richer data such as bid/ask/microprice/depth or different target alignment.

## Implementation Scope If Approved

Expected files:

- `src/bin/polymarket_v2.rs`
- `scripts/research_local_agg_combined_source_regimes.py` only as mirrored offline evaluator, already updated on research branch
- optional focused runtime replay/check script if needed

Runtime shape:

- Add DOGE selector next to the existing HYPE source-lag regime path.
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

Option B: implement combined selector in dry-run.

- Change runtime source-window/model selection for DOGE, SOL, and BNB.
- Build/restart only `pm-local-agg-challenger.service`.
- Start a new dry-run checkpoint.
- Expected to bring current-run max below 5bps in matched replay while keeping side_errors=0 and BTC unchanged.

Option C: implement diagnostics/schema enrichment first.

- Add bid/ask/microprice/depth capture where low risk.
- Do not alter selected close price yet.
- Slower path, but addresses the BNB residual max that visible close tape cannot repair.

## Recommendation

Recommend Option B as the next dry-run checkpoint, provided the user approves a challenger restart and accepts that this is still dry-run only. The offline evidence now dominates the deployed HYPE-only state on max/p95 without side/BTC regression. Option C should run in parallel afterward because BNB remains the limiting residual.

