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

## 00:16Z Guard Sweep Addendum

新增到 replay harness 的 guard：

- `--sol-max-move-bps`
- `--bnb-max-move-bps`
- `--xrp-max-move-bps`
- `--disable-sol-debias`
- `--disable-bnb-debias`
- `--disable-xrp-debias`

这里的 `move_bps` 是 debiased price 相对 deterministic/base price 的距离；它是 decision-time 可见字段，不使用 RDTS close。

### All-family move guard

| max move | selected | row regressions >0.5bps | global max | global p95 | BNB p95 |
|---:|---:|---:|---:|---:|---:|
| none | 127 | 26 | 4.984909 | 2.854649 | 4.356750 |
| 1.0bps | 44 | 5 | 4.984909 | 3.044974 | 4.356750 |
| 1.5bps | 66 | 10 | 4.984909 | 2.994790 | 4.356750 |
| 2.0bps | 96 | 20 | 4.984909 | 2.972717 | 4.356750 |

Interpretation:

- Move guard does reduce row regressions materially.
- BNB p95 remains worse under all tested move thresholds, so BNB needs to stay disabled or be conditioned separately.
- SOL still contributes regressions even at 1.0bps move cap.

### XRP-only clean guard

禁用 SOL/BNB，只保留 XRP `same_near` frozen debias，并设置 `xrp_max_move_bps=1.0`：

- selected_count：15
- row_regression_count：0
- global max：4.984909（unchanged versus deterministic）
- global p95：3.092537（unchanged versus deterministic）
- XRP max：3.774017（unchanged）
- XRP p95：2.612629 -> 2.494177
- side_errors：0
- BTC unchanged

Decision：`keep_research_only`。

This is clean but marginal. It is useful evidence that a decision-time move guard can prevent row damage, but it is not strong enough to justify runtime complexity or a separate Decision Needed yet.

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

## 01:16Z Current-Data Guard Sweep

用最新同步的 run `20260513_045906` accepted rows 重跑 guard sweep。固定 replay n=1055；deterministic HYPE/DOGE-only baseline 为 max/p95/p99 = 4.984909 / 3.093833 / 4.234153，side_errors=0。

| Config | selected | row regressions >0.5bps | max | global p95 | p99 | BNB p95 | SOL p95 | XRP p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| all-family move<=0.5bps | 14 | 0 | 4.984909 | 3.092572 | 4.234153 | 4.222999 | 2.673806 | 2.612629 |
| all-family move<=0.75bps | 27 | 2 | 4.984909 | 3.092572 | 4.309482 | 4.356559 | 2.673806 | 2.494177 |
| all-family move<=1.0bps | 45 | 5 | 4.984909 | 3.092572 | 4.309482 | 4.356559 | 2.673806 | 2.494177 |
| no-BNB move<=0.75bps | 17 | 0 | 4.984909 | 3.093833 | 4.234153 | 4.223761 | 2.673806 | 2.494177 |
| XRP-only move<=1.0bps | 16 | 0 | 4.984909 | 3.093833 | 4.234153 | 4.223761 | 2.673806 | 2.494177 |
| SOL-only move<=1.0bps | 22 | 2 | 4.984909 | 3.093833 | 4.234153 | 4.223761 | 2.673806 | 2.612629 |

Interpretation:

- `all-family move<=0.5bps` is clean but its global p95 gain over deterministic is only 0.001261bps, not enough to justify frozen debias runtime complexity.
- `all-family move>=0.75bps` reintroduces row regressions and BNB p95 regression.
- `no-BNB` / `XRP-only` guards can improve XRP p95 without row regressions, but do not move global p95/max.
- SOL remains too weak under this guard: useful selected rows are not enough to change SOL p95, while looser movement allows row regressions.

Decision remains `keep_research_only`; Option B1 deterministic HYPE/DOGE-only remains the only runtime candidate pending explicit approval.
