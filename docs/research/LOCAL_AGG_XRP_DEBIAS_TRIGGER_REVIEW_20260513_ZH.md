# Local Agg XRP Debias Trigger Review

更新时间：2026-05-13 23:16Z

本文是离线研究记录，不是 runtime 变更。目标是判断 XRP 是否存在比此前 `pre_20s_60s` current-only trigger 更可复用的 source-lag/debias 触发器。

## Safety Context

23:16Z runtime check：

- service：`pm-local-agg-challenger.service` active/running，`NRestarts=0`
- server HEAD：`08f6dc6`
- run id：`20260513_045906`
- accepted / gated：286 / 345
- accepted side errors：0
- accepted max / p95：6.893443bps / 3.453884bps
- latency p95 / max：49.0ms / 279.0ms

当前 accepted hard max 仍是旧 DOGE/HYPE tail；XRP 不是 runtime hard blocker：

- XRP accepted n=53
- XRP max / p95：3.346544bps / 2.563581bps
- top XRP tails：3.346544、3.243953、3.136432bps，均为 side-matched。

## Replay Setup

使用固定 causal-ish matched tests：

- test `20260512_014312`，train `20260511_083910`
- test `20260513_045906`，train `20260511_083910 + 20260512_014312`
- symbol：`xrp/usd`
- gate status：`accepted`
- helper：`scripts/research_local_agg_lag_selector_models.py`
- recency penalty：0.02 bps/sec
- after penalty：0.75 bps

## Baseline

| Test run | n | current max | current p95 | side errors |
|---|---:|---:|---:|---:|
| 20260512 | 84 | 3.774017 | 2.517392 | 0 |
| 20260513 | 53 | 3.346544 | 2.563581 | 0 |

## Generic Debias Result

With `min_train=8`, the generic XRP debiased selector is mixed:

| Test run | candidate max | candidate p95 | side errors | decision |
|---|---:|---:|---:|---|
| 20260512 | 2.408470 | 1.649679 | 0 | improves |
| 20260513 | 4.040741 | 2.316335 | 0 | max regresses |

结论：generic XRP debias 不能部署。它改善 20260513 p95，但把 max 从 3.346544 提高到 4.040741，违反 max/tail-risk 优先原则。

## Trigger Grid Result

对 decision-time categorical features 做 trigger grid：

- selected source
- selected lag bucket
- selected depth
- current local sources
- current rule
- selected kind / kind family
- selected key family
- selected score threshold
- selected train support threshold

最稳定的低复杂触发器是 selected depth `same_near`，特别是 `min_train >= 20` 时：

| Trigger | Test run | triggered | candidate max | candidate p95 | side errors |
|---|---:|---:|---:|---:|---:|
| `depth=same_near`, `min_train=20` | 20260512 | 31 | 2.408470 | 1.896921 | 0 |
| `depth=same_near`, `min_train=20` | 20260513 | 22 | 2.372484 | 2.214997 | 0 |

相较 baseline：

- 20260512 max：3.774017 -> 2.408470
- 20260512 p95：2.517392 -> 1.896921
- 20260513 max：3.346544 -> 2.372484
- 20260513 p95：2.563581 -> 2.214997
- side_errors：0 -> 0

This is materially stronger than the earlier `pre_20s_60s` current-only subset.

## Current-Only Triggers

仍有若干 current-only 触发器，例如：

- `lag=pre_20s_60s`
- `kind=tape_close_only_diag_close`

它们在 20260513 能改善部分 tail，但在 20260512 trigger count 为 0。按 autoresearch keep/discard 规则，这类证据只能作为 attribution，不能作为 runtime proposal。

## Interpretation

XRP 看起来不是一个简单 deterministic source-window 问题。更像是：

```text
nearest_abs runtime selection
+ Binance-dominant local sources
+ selected candidate 与 runtime close 基本 same_near
+ frozen historical signed-error debias
```

因此它与 SOL/BNB 更接近，属于 selector-history/debias family，而不是 HYPE/DOGE deterministic source-regime family。

## Decision

Decision：`keep_research_only`。

理由：

- `depth=same_near + min_train>=20` matched replay 同时改善 20260512 与 20260513 的 max/p95，side_errors=0。
- 但 runtime 需要 frozen selector table、model artifact id、support guard、row-regression guard 和 structured fire/skip logs。
- 不应并入 Option B1 deterministic HYPE+DOGE-only checkpoint。

下一步如果继续 XRP：

1. 把 `same_near/min_train>=20` 写入 frozen debias artifact 设计。
2. 与 SOL/BNB 一起做 selector-history/debias family replay。
3. 输出 row-level regression table，特别是 `candidate_error - base_error > 0.5bps`。
4. 只有在全 family matched replay 不恶化 BTC/global max 后，才单独提出 Decision Needed。

当前 runtime recommendation 不变：Option B1 仍只包含 deterministic HYPE+DOGE。
