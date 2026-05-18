# Local Agg Offset/Window Grid

更新时间：2026-05-14 01:46Z

本文是离线研究记录，不是 runtime 变更。目标是检查一个更简单的假设：是否可以通过统一或分符号的 open/close offset/window、source pick rule、aggregation method，替代当前越来越多的窄 regime selector。

## Safety Context

最新 dry-run checkpoint 仍为：

- server HEAD：`08f6dc6`
- service：`pm-local-agg-challenger.service` active/running，`NRestarts=0`
- run id：`20260513_045906`
- accepted / gated：322 / 400
- accepted_side_errors：0
- accepted max / p95：6.893443bps / 3.780985bps
- latency p95 / max：49.0ms / 279.0ms

官方 accepted hard tails 仍是 side-matched：

- DOGE `1778670300`: 6.893443bps
- HYPE `1778719500`: 5.856760bps
- DOGE `1778699100`: 5.368432bps
- HYPE `1778697900`: 5.349395bps

没有重启、没有部署、没有 live trading。

## Setup

构建 ready-aware boundary dataset：

```bash
python3 scripts/build_local_agg_boundary_dataset.py \
  --logs-root logs/remote-runs \
  --instance-glob '202605*' \
  --mode full \
  --out-csv /tmp/localagg_boundary_remote_0146.csv \
  --cap-local-ready-lag-ms 300
```

输出：

- rows：766469 boundary rows
- samples by symbol：
  - BNB 362
  - BTC 363
  - DOGE 357
  - ETH 362
  - HYPE 362
  - SOL 361
  - XRP 360

Grid:

- pre_ms：500, 1000, 3000, 5000, 10000, 30000
- post_ms：0, 250, 500
- source methods：last_before, after_then_before, nearest_abs, linear_interp, linear_project
- aggregators：mean, median, trimmed_mean
- min_sources：1, 2, 3
- min_coverage：0.80
- ready-aware：true

## Result

Hard safety result：在 `min_coverage >= 0.80` 下，没有任何符号存在 `side_errors=0` 的 high-coverage grid candidate。

The p95-best candidates are therefore not deployable. They improve some distribution metrics only by accepting side-risk:

| Symbol | p95-best model | coverage | p95 | max | side errors |
|---|---|---:|---:|---:|---:|
| BNB | pre=5000 post=250 `current|after_then_before|mean|n1` | 1.000 | 4.984582 | 6.184405 | 45 |
| BTC | pre=5000 post=0 `current|last_before|median|n1` | 0.992 | 1.112606 | 3.033207 | 5 |
| DOGE | pre=5000 post=250 `current|after_then_before|mean|n1` | 0.910 | 4.284103 | 7.695925 | 14 |
| ETH | pre=5000 post=0 `current|last_before|mean|n1` | 0.964 | 1.774610 | 3.145336 | 11 |
| HYPE | pre=5000 post=0 `current|linear_project|mean|n1` | 1.000 | 6.742443 | 12.720472 | 29 |
| SOL | pre=5000 post=250 `only_coinbase|after_then_before|mean|n1` | 0.917 | 1.848515 | 4.082453 | 9 |
| XRP | pre=5000 post=250 `only_coinbase|after_then_before|mean|n1` | 0.931 | 1.942381 | 33.265230 | 6 |

## Interpretation

The high-coverage offset/window grid does not produce a trading-safe oracle proxy by itself:

- It cannot satisfy the strict `side_errors=0` requirement at 80% coverage.
- It introduces severe tail risk for HYPE and XRP under p95-optimized models.
- It does not replace the observed need for narrow per-symbol regime handling.
- It supports the current complexity-adjusted runtime recommendation: keep Option B1 deterministic HYPE+DOGE as the only runtime candidate pending explicit approval, and keep SOL/BNB/XRP debias research-only.

This also argues against a broad unattended change to aggregation family, source weights, or global window selection.

## Decision

Decision：`discard_broad_grid_runtime_candidate`。

Reason：

- No high-coverage candidate has zero side errors.
- P95-best candidates violate trading safety constraints.
- HYPE/DOGE tails are better explained by specific source-window regimes than by a global offset/window rule.

Next:

1. Keep Option B1 deterministic HYPE+DOGE as the only runtime proposal requiring explicit approval.
2. Continue robust offline baselines, but require side_errors=0 before any runtime discussion.
3. Use future grids for diagnostic attribution, not direct deployment.
