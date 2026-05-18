# Local Agg HYPE/DOGE Residual Watch

更新时间：2026-05-14 02:16Z

本文是离线研究记录，不是 runtime 变更。目标是在 Option B1 deterministic HYPE/DOGE replay 之后，检查剩余 HYPE/DOGE sub-5 residual tails 是否已经形成新的可部署 regime。

## Safety Context

当前线上 checkpoint 仍未变化：

- server HEAD：`08f6dc6`
- service：`pm-local-agg-challenger.service` active/running，`NRestarts=0`
- run id：`20260513_045906`
- accepted / gated：332 / 410
- accepted_side_errors：0
- accepted max / p95：6.893443bps / 3.750916bps
- latency p95 / max：49.0ms / 279.0ms

官方 accepted hard tails 仍为既有 side-matched DOGE/HYPE rows；02:16Z 没有新增 accepted >5bps。

## Option B1 Replay Refresh

用最新同步的 current run 数据重跑 deterministic HYPE+DOGE-only：

- fixed accepted replay n = 1074
- base max/p95/p99 = 6.893443 / 3.133561 / 4.437615
- candidate max/p95/p99 = 4.984909 / 3.092606 / 4.227956
- current-run base max/p95/p99 = 6.893443 / 3.750915 / 5.234747
- current-run candidate max/p95/p99 = 4.979561 / 3.419201 / 4.510689
- side_errors = 0
- selected_count = 5

Selected repairs are unchanged:

- DOGE `1778670300`: 6.893443 -> 3.665444
- HYPE `1778719500`: 5.856760 -> 3.691040
- DOGE `1778699100`: 5.368432 -> 2.711669
- HYPE `1778697900`: 5.349395 -> 1.215238
- DOGE `1778614800`: 3.352468 -> 3.012507

## Residual Tails After Option B1

Top current-run HYPE/DOGE residuals after Option B1:

| Symbol | round_end_ts | source_subset | local_sources | base | candidate | note |
|---|---:|---|---|---:|---:|---|
| DOGE | 1778718300 | doge_binance_fallback | binance | 4.979561 | 4.979561 | uncovered Binance-only fallback |
| DOGE | 1778715300 | doge_binance_fallback | binance | 4.911904 | 4.911904 | uncovered Binance-only fallback |
| DOGE | 1778706300 | drop_binance | okx | 4.564417 | 4.564417 | okx-only but below current hard gate |
| DOGE | 1778659500 | doge_binance_fallback | binance | 4.531900 | 4.531900 | uncovered Binance-only fallback |
| HYPE | 1778715300 | drop_binance | bybit;hyperliquid;okx | 4.463476 | 4.463476 | not hyperliquid;okx exact set |

The limiting current-run residual after Option B1 is now DOGE Binance-only fallback at 4.979561bps. The all-runs residual max remains historical BNB 4.984909bps.

## DOGE Binance-Fallback Prototype

Hypothesis：DOGE Binance-only fallback residual tails may reflect the oracle using an older same-side, deeper pre-boundary CEX print rather than the runtime Binance after-then-before price.

Prototype trigger:

- symbol = DOGE
- `source_subset=doge_binance_fallback`
- local source set exactly `{binance}`
- local margin >= 5bps
- select same-side pre-boundary CEX/non-Binance candidate
- pre window 10s
- candidate margin at least 2bps deeper than local margin

Matched replay on accepted rows after Option B1:

| Config | selected | selected runs | row regressions >0.5bps | global p95 | p99 | max | current p95 | current max |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| pre=10000, min_local=5, min_deeper=2, sources=non_binance | 3 | 20260513 only | 0 | 3.044974 | 4.121911 | 4.984909 | 3.307240 | 4.564417 |

Selected current rows:

- DOGE `1778659500`: 4.531900 -> 1.856 via OKX at -5838ms
- DOGE `1778715300`: 4.911904 -> 0.495 via OKX at -6936ms
- DOGE `1778718300`: 4.979561 -> 2.356 via OKX at -1515ms

## Decision

Decision：`keep_research_only`。

Reason：

- It improves the current residual tail and has zero row regressions in replay.
- But it selects only three current-run rows and has no historical selected support.
- It depends on older pre-boundary prints up to 10s before boundary, which needs more out-of-sample evidence before runtime.
- It would be another DOGE source-window model change and must not be bundled silently into Option B1.

Current runtime recommendation remains unchanged:

- Option B1 deterministic HYPE+DOGE-only is still the only runtime proposal pending explicit approval.
- DOGE Binance-fallback deeper-window stays in research-only watch until it gets historical or future out-of-sample support.
