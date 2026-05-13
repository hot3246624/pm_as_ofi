# Local Agg Runtime Implementation Plan

更新时间：2026-05-13 20:16Z

目标：在用户明确批准后，把离线验证通过的 deterministic HYPE+DOGE source-regime selector 实现到 `pm-local-agg-challenger.service` 的 dry-run runtime。本文是实施计划，不是部署记录；当前未改 Rust、未重启服务、未开启 live trading。

## 推荐范围

推荐先实现 Option B1：deterministic HYPE+DOGE-only。

不在第一阶段实现 SOL/BNB debiased selector。原因是 SOL/BNB replay 使用历史 source-lag selector 和 signed-error debias，runtime 等价实现需要 selector-history/model 文件及状态管理；这比 HYPE/DOGE 的确定性 source-window 规则风险高。

## Evidence

20:16Z deterministic HYPE+DOGE-only fixed replay，accepted rows n=985：

- global base max/p95/p99 = 6.893443 / 3.091145 / 4.224368
- candidate max/p95/p99 = 4.984909 / 3.029067 / 4.111681
- current run base max/p95/p99 = 6.893443 / 3.747908 / 5.087797
- current run candidate max/p95/p99 = 4.531900 / 3.413421 / 4.269339
- side_errors = 0
- BTC unchanged：p95/max = 1.140092 / 3.033207

Selected rows:

- DOGE `1778670300`: 6.893443 -> 3.665444 via Binance pre-boundary shallow window.
- DOGE `1778699100`: 5.368432 -> 2.711669 via OKX same-side deeper window.
- HYPE `1778697900`: 5.349395 -> 1.215238 via Hyperliquid mid-spread pre-boundary window.
- DOGE `1778614800`: 3.352468 -> 3.012507 via Binance pre-boundary shallow window.

## Rust Scope

Primary file:

- `src/bin/polymarket_v2.rs`

Existing runtime structures to reuse:

- `LocalSourceLagCloseCandidate`
- `collect_local_source_lag_candidates`
- `LocalBoundaryShadowHit`
- `local_boundary_select_hype_source_lag_regime`
- `local_hype_pick_deepest_pre_candidate`
- `local_hype_pick_closest_pre_candidate`
- `local_hype_source_lag_hit_from_candidate`

Recommended refactor:

- Rename or wrap HYPE-specific helper names into symbol-neutral helpers only if needed for clarity.
- Keep `policy_name="boundary_source_lag_regime"` so existing shadow/gate logging continues to work.
- Keep candidate source count as `1`, source spread as `0.0`, and rule as `LastBefore`, matching the current HYPE source-lag runtime path.

## Selector Semantics

### HYPE mid-spread addendum

Add one branch to `local_boundary_select_hype_source_lag_regime`:

- symbol must be `hype/usd`
- current source set exactly `{hyperliquid, okx}`
- `2.0 <= source_spread_bps <= 3.0`
- current margin from RTDS open `>= 5.0bps`
- choose same-side Hyperliquid pre-boundary candidate with `offset_ms <= 0` and age `<= 5000ms`
- choose deepest pre-boundary candidate by open-margin, with existing tie-breaks
- source subset name: `hype_source_lag_hl_okx_midspread`

### DOGE shallow-window selector

Add `local_boundary_select_doge_shallow_window`:

- symbol must be `doge/usd`
- current source spread must be finite and `<= 4.0bps`
- current margin from RTDS open `>= 8.0bps`
- candidate sources: `binance`, `bybit`, `coinbase`, `hyperliquid`, `okx`
- candidate offset must be pre-boundary, age `<= 1000ms`
- candidate must stay on the same side of RTDS open as current local close
- candidate margin must be at least `6.0bps` shallower than current local margin
- choose minimum candidate margin, then lower age, then source/kind tie-break
- source subset name: `doge_same_side_shallowest_pre_window`

### DOGE okx-only deeper-window selector

Add `local_boundary_select_doge_okx_deeper_window`:

- symbol must be `doge/usd`
- current source set exactly `{okx}`
- current margin from RTDS open `>= 30.0bps`
- candidate sources: `binance`, `bybit`, `coinbase`, `okx`
- candidate offset must be pre-boundary, age `<= 30000ms`
- candidate must stay on the same side of RTDS open as current local close
- candidate margin must be at least `2.0bps` deeper than current local margin
- choose minimum candidate margin, then lower age, then source/kind tie-break
- source subset name: `doge_okx_only_same_side_deeper_window`

## Call-Site Integration

At the current source-lag injection point:

- Build the weighted boundary hit as today.
- Compute one `source_regime_hit` from the weighted hit and `source_lag_candidates`.
- Order:
  - HYPE deployed selector with mid-spread addendum.
  - DOGE shallow-window selector.
  - DOGE okx-only deeper-window selector.
- Push `source_regime_hit` into `boundary_shadow_outcomes`.
- Use it before weighted boundary candidate in `weighted_shadow_candidate`.

Do not change:

- uncertainty gate thresholds
- source eligibility outside the selector
- live trading settings
- shared ingress
- latency deadlines
- accepted/gated policy thresholds

## Validation Commands

Local syntax/replay:

```bash
python3 -m py_compile scripts/research_local_agg_combined_source_regimes.py scripts/research_local_agg_doge_shallow_window.py
PYTHONPATH=scripts python3 scripts/research_local_agg_combined_source_regimes.py \
  --run-dir logs/local-agg-challenger/runs/20260511_083910 \
  --run-dir logs/local-agg-challenger/runs/20260512_014312 \
  --run-dir logs/local-agg-challenger/runs/20260513_045906 \
  --sol-trigger-source __disabled__ \
  --bnb-require-local-source __disabled__ \
  --out-json /tmp/localagg_deterministic_hype_doge_replay.json \
  --ledger-jsonl /tmp/local_agg_research_ledger.jsonl \
  --tail-n 30
```

Rust checks:

```bash
cargo check --bin polymarket_v2 --target-dir target/local_agg_check
cargo build --release --bin polymarket_v2
```

EC2 pre-restart checks after approval:

```bash
ssh -i ~/.ssh/polymarket-Ireland.pem ubuntu@ec2-3-248-230-60.eu-west-1.compute.amazonaws.com \
  'cd /srv/pm_as_ofi/repo && git rev-parse --short HEAD && systemctl is-active pm-local-agg-challenger.service'
```

Deployment only after approval:

- Sync exact approved commit to `/srv/pm_as_ofi/repo`.
- Build there.
- Restart only `pm-local-agg-challenger.service`.
- Start a new checkpoint clock.
- Preserve run `20260513_045906` as failed deployed-HYPE baseline.

## Post-Restart Watch

First checkpoint rules:

- 20 accepted samples: warmup only.
- 100 accepted samples: actionable safety checkpoint.
- 500 accepted samples with clean topology: canary discussion threshold.

Hard gates:

- accepted side errors must remain `0`
- accepted max must remain `<5bps`
- latency p95 must remain `<300ms`

If HYPE/DOGE fires:

- log reason, source, offset, selected price, local source set, source spread, direction margin
- inspect any selected-row regression immediately

If BNB remains limiting:

- do not add a gate reflexively
- prioritize non-behavioral schema enrichment: bid/ask, microprice, depth, spread, mark/index where available
