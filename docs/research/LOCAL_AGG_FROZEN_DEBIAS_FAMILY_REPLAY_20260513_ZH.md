# Local Agg Frozen Debias Family Replay

更新时间：2026-05-13 23:46Z

本文是离线研究记录，不是 runtime 变更。目标是把 SOL/BNB/XRP 的 selector-history/debias family 放进一个固定 replay harness，并强制输出 row-level regression table。

## Runtime Context

23:46Z safety check：

- service：`pm-local-agg-challenger.service` active/running，`NRestarts=0`
- server HEAD：`08f6dc6`
- run id：`20260513_045906`
- accepted / gated：299 / 350
- accepted side errors：0
- accepted max / p95：6.893443bps / 3.729867bps
- latency p95 / max：49.0ms / 279.0ms

新增观察：

- DOGE 新 accepted tail 4.911904bps，低于 hard max。
- HYPE 新 accepted tail 4.463476bps，低于 hard max。
- 累计 accepted max 仍是旧 DOGE 6.893443bps。

## New Harness

新增脚本：

```text
scripts/research_local_agg_frozen_debias_family.py
```

用途：

- 先应用 deterministic HYPE/DOGE normalization：
  - deployed HYPE source-lag selector
  - HYPE hyperliquid/OKX mid-spread addendum
  - DOGE shallow-window
  - DOGE OKX-only deeper-window
- 再应用 frozen selector-history/debias candidates：
  - SOL：`selected_source=coinbase` 且 `selected_depth=same_shallower`
  - BNB：runtime `local_sources` 包含 `bybit`
  - XRP：`selected_depth=same_near`
- 不在线学习：eval rows 不会被追加进 history。
- 输出 row-level regressions：默认 `candidate_error - base_error > 0.5bps`。

## Replay Setup

固定 accepted replay：

- `20260511_083910`
- `20260512_014312`
- `20260513_045906`

训练映射：

- SOL：20260512 train=20260511；20260513 train=20260511+20260512
- BNB：20260512 train=20260511；20260513 train=20260511+20260512
- XRP：20260512 train=20260511；20260513 train=20260511+20260512

## Results

Global metrics:

| Layer | n | max | p95 | p99 | side errors |
|---|---:|---:|---:|---:|---:|
| base | 1054 | 6.893443 | 3.108375 | 4.324283 | 0 |
| deterministic HYPE/DOGE | 1054 | 4.984909 | 3.088403 | 4.218160 | 0 |
| frozen debias family | 1054 | 4.984909 | 2.854649 | 4.234479 | 0 |

Per-symbol highlights:

| Symbol | base max/p95 | candidate max/p95 | interpretation |
|---|---:|---:|---|
| BTC | 3.033207 / 1.113698 | 3.033207 / 1.113698 | unchanged |
| DOGE | 6.893443 / 4.302332 | 4.911904 / 4.217749 | deterministic repair helps max |
| HYPE | 5.349395 / 4.248081 | 4.463476 / 3.741155 | deterministic repair helps max/p95 |
| SOL | 4.292957 / 2.673806 | 4.093919 / 2.426097 | debias helps |
| BNB | 4.984909 / 4.232251 | 4.984909 / 4.356750 | p95 worsens |
| XRP | 3.774017 / 2.612629 | 3.243953 / 2.334275 | debias helps |

Selection summary:

- selected_count：127
- selected_side_errors：0
- row_regression_count over 0.5bps：26

## Row Regression Evidence

Top regressions show why this cannot move to runtime yet:

| Run | Symbol | round_end_ts | reason | base error | candidate error | regression |
|---|---|---:|---|---:|---:|---:|
| 20260512 | XRP | 1778633700 | xrp_same_near_frozen_debias | 0.326144 | 2.852288 | +2.526144 |
| 20260512 | XRP | 1778619600 | xrp_same_near_frozen_debias | 0.115656 | 2.180390 | +2.064734 |
| 20260512 | SOL | 1778632800 | sol_same_shallower_coinbase_frozen_debias | 0.118240 | 2.112052 | +1.993811 |
| 20260512 | SOL | 1778624400 | sol_same_shallower_coinbase_frozen_debias | 0.373637 | 2.246594 | +1.872957 |
| 20260512 | XRP | 1778625000 | xrp_same_near_frozen_debias | 0.051062 | 1.798453 | +1.747391 |

Interpretation:

- Aggregate p95 improves, but the family sometimes damages rows where the existing runtime was already very accurate.
- This validates the safety-plan requirement for row-level regression guard.
- BNB frozen debias is not ready: BNB p95 worsens from 4.232251 to 4.356750 and historical max remains 4.984909.

## Decision

Decision：`keep_research_only`。

Reason：

- Side errors remain 0.
- BTC is unchanged.
- Global p95 improves versus deterministic-only.
- But p99 worsens slightly versus deterministic-only, BNB p95 worsens, and there are 26 row regressions above 0.5bps.

Runtime recommendation remains unchanged:

- Option B1 deterministic HYPE+DOGE-only remains the only low-complexity runtime candidate pending explicit user approval.
- SOL/BNB/XRP frozen debias requires a separate artifact, stricter row-regression guard, and a separate Decision Needed.

## Next Research Step

Tighten the frozen debias family before any runtime discussion:

1. Add a row-regression guard proxy based only on decision-time fields.
2. Disable or further condition BNB debias unless it improves p95 without hurting historical max/p95.
3. For SOL/XRP, avoid firing when current runtime price is already near the selected debiased projection or when score support comes from broad fallback keys.
4. Re-run fixed replay and require:
   - side_errors = 0
   - BTC unchanged
   - row_regression_count materially reduced
   - BNB p95 not worse
   - global p95/p99 no worse than deterministic-only
