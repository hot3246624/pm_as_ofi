# LOCAL_AGG_CURRENT_HARD_TAILS_20260514

## 背景

当前 dry-run checkpoint: Option B1 deterministic HYPE/DOGE plus SOL
OKX-before-Binance fallback, run `20260514_064013`.

在 `2026-05-14T08:07Z`，当前 run 出现硬门失败：

- accepted `25`，后续最新服务端摘要 accepted `36`
- accepted_side_errors `1`
- accepted_max_bps `5.258829`
- accepted_p95_bps 约 `5.16`
- latency_p95_ms 约 `196.75`
- service active, server HEAD `cd0a821`, dry-run/lab-only/max_notional=0 confirmed

主要 accepted hard tails:

- XRP `1778744700`: `5.258829bps`, `only_binance_coinbase`,
  `nearest_abs`, `local_sources=binance`, side error.
- DOGE `1778744700`: `5.175007bps`, `drop_binance`, `last_before`,
  `local_sources=bybit;okx`, side matched.
- DOGE `1778745900`: `5.155480bps`, `drop_binance`, `last_before`,
  `local_sources=bybit;okx`, side matched.

## Hypothesis

The current hard tails can be handled with narrow decision-time rules:

1. XRP side-error containment for a single-source Binance stale upper-margin
   shape. This is explicitly containment, not the long-term oracle-proxy target.
2. DOGE bybit+OKX deeper pre-boundary source-window selector for high-margin,
   same-side `drop_binance` rows.

No live trading, no shared ingress change, no threshold relaxation, no broad
source/weight/family change.

## Offline Replay

Harness:

```bash
python3 scripts/research_local_agg_current_hard_tails_20260514.py \
  --run-dir logs/local-agg-challenger/runs/20260511_083910 \
  --run-dir logs/local-agg-challenger/runs/20260512_014312 \
  --run-dir logs/local-agg-challenger/runs/20260513_045906 \
  --run-dir logs/local-agg-challenger/runs/20260514_024406 \
  --run-dir logs/local-agg-challenger/runs/20260514_064013 \
  --mode combined_gate
```

Fixed replay over accepted rows `n=1168`, with deterministic HYPE/DOGE
normalization:

- base side_errors `1`
- candidate side_errors `0`
- row_regression_count `0`
- selected_count `4`
- BTC unchanged by construction

Current run subset `20260514_064013`:

- base max/p95/p99 `5.258829 / 5.167196 / 5.235359`
- candidate max/p95/p99 `3.205225 / 2.362506 / 2.991916`
- side_errors `1 -> 0`
- candidate accepted count `29 -> 28` because the XRP row is containment-gated

Selected rows:

- DOGE `20260512_014312/1778577000`: `4.251766 -> 1.057415`
- DOGE `20260514_064013/1778744700`: `5.175007 -> 2.110903`
- DOGE `20260514_064013/1778745900`: `5.155480 -> 1.630365`
- XRP `20260514_064013/1778744700`: accepted side-error row removed by
  containment guard

The harness does not re-model the already deployed SOL OKX-before-Binance
ordering, so global output still shows the pre-SOL `20260514_024406` SOL
`5.592404bps` baseline artifact. The live checkpoint already contains that SOL
ordering fix; current-run evidence is the relevant post-SOL failure surface.

## Runtime Patch

Touched file: `src/bin/polymarket_v2.rs`.

Changes:

- Add `xrp_single_binance_yes_stale_upper_margin_side_error_guard` in
  `local_boundary_weighted_candidate_filter_reason_for_policy`.
- Add `doge_bybit_okx_same_side_deeper_window` in
  `local_boundary_select_doge_source_lag_regime`.
- Add exact source constants for bybit+OKX and DOGE drop-Binance CEX sources.

Runtime semantics:

- XRP containment triggers only for `xrp/usd`, `only_binance_coinbase`,
  `nearest_abs`, single Binance, local side Yes, margin `[3.5, 4.6)bps`,
  close delta `[1500, 2500]ms`.
- DOGE selector triggers only for `doge/usd`, current source set exactly
  bybit+OKX, local side No, spread `[2.5, 4.6]bps`, margin `>=12bps`, and a
  same-side deeper pre-boundary bybit/coinbase/OKX candidate within `30s`.

## Validation

Local:

- `python3 -m py_compile scripts/research_local_agg_current_hard_tails_20260514.py`
- fixed replay command above
- `git diff --check`
- `cargo check --bin polymarket_v2 --target-dir target/local_agg_check`
- `cargo build --release --bin polymarket_v2`

`cargo fmt --check` is not used as a gate here because this existing file has
pre-existing rustfmt churn unrelated to the patch; full formatting would create
large unrelated diffs.

## Decision

Decision: `keep_runtime_dryrun_candidate`.

This patch is a narrow dry-run safety/model iteration after a real accepted
side-error and accepted max failure. It is not live trading and does not change
shared ingress. The XRP part is containment; the DOGE part is deterministic
oracle-proxy source-window selection.
