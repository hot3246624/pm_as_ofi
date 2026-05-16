# Xuan High Pressure Paper-Shadow Runbook - 2026-05-16

状态：`xuan_high_pressure_v1` 是 settlement-alpha paper-shadow 候选，不是 pair-completion arb，也不是可部署策略。

研究来源：

- KEEP 候选：`p50_65_allow_ms100_lb5_ratio1p0`
- 研究 artifact：`/home/ubuntu/xuan_frontier_runs/xuan-frontier-remote-verifier-heartbeat/20260516T074619Z_high_side_pressure_filter_0506_0513_v1`
- 2026-05-06..13：`fee100_pnl=+1926.93`，`fee100_roi=1.641%`，`cycles_per_market=8.76`，`active_rate=76.39%`，`global_net_qty_rate=2.85%`，`worst_market_pnl=-64.63`
- Split：05-06..10 `fee100=+1575.52`；05-11..13 `fee100=+351.41`

## Scope Guard

- 只允许 dry-run / paper-shadow。
- 不触碰 `pm-shared-ingress-broker.service`、shared ingress env/root/protocol/market universe、oracle lag、local agg、raw/replay、live executor、systemd。
- 不使用 2026-05-14/15 full-day 数据做研究口径。
- 不把该 profile 的 `residual_qty` 当作 pair-completion 失败；它是 settlement inventory。
- 不用旧全局 `PM_MAX_NET_DIFF` 判断该 profile 是否安全；该 profile 应以自身 side/gross caps 为准。

## Profile Mechanics

`xuan_high_pressure_v1` 当前映射：

- high-side public BUY pressure trigger
- offset window：60..180s
- BUY price band：0.50..0.65
- clip：10
- public flow haircut：10%
- side cap：100
- market gross cap：250
- 5s high-side BUY qty >= 10
- 5s high/low BUY qty ratio >= 1.0
- trade freshness：1500ms
- side cooldown：1000ms
- settlement-alpha lifecycle：跳过 pair-completion active-tranche/completion/residual/capital gates

## Proposed Paper-Shadow Env

只由运行面 owner 决定是否部署到 shadow。建议 env 形态：

```bash
PM_SHARED_INGRESS_ROLE=client
PM_DRY_RUN=true
PM_PGT_SHADOW_PROFILE=xuan_high_pressure_v1
PM_DRY_RUN_MARKET_TOUCH_TRADE_PARTIAL_FILLS=1
PM_DRY_RUN_MARKET_TOUCH_TRADE_FILL_FRACTION=0.25
```

不需要启动第二个 broker，不使用 `PM_SHARED_INGRESS_ROLE=standalone`。

## Local Readiness Commands

```bash
python3 -m unittest scripts.tests.test_analyze_pgt_shadow_events
cargo test public_buy_pressure --lib
cargo test xuan_high_pressure --lib
cargo test settlement_alpha_inventory_mode --lib
cargo test pair_gated_tranche --lib
cargo build --bin polymarket_v2
```

## Recorder / Analyzer Requirements

Paper-shadow 评估必须看到以下字段：

- `pgt_shadow_profile=xuan_high_pressure_v1`
- `winner_side`，或后续 `market_resolved.winner_side` / `redeem_requested.resolved_winner_side`
- `market_trade_ticks` / `market_sell_trade_ticks` / inferred BUY ticks
- `pgt_entry_pressure_sides`
- `pgt_entry_pressure_extra_ticks`
- `pgt_high_pressure_no_seed`
- `yes_qty/no_qty` 与 avg cost
- `settlement_alpha_pnl`
- `settlement_alpha_fee50_pnl`
- `settlement_alpha_fee100_pnl`

Analyzer command:

```bash
python3 scripts/analyze_pgt_shadow_events.py \
  --root data/recorder \
  --instance xuan-shadow \
  --date YYYY-MM-DD \
  --from-round <first_clean_round> \
  --last 24 \
  --details
```

## Paper-Shadow Scorecard

Primary:

- `settlement_alpha_fee100_pnl > 0`
- `settlement_alpha_fee100_roi > 0`
- active/filled rounds sufficiently dense to evaluate; first smoke can be 24 complete rounds, material check should be >= 80 complete rounds
- `winner_side` coverage close to 100%; missing winner means settlement metrics are incomplete
- no-seed distribution should be explainable, not dominated by `simulate_buy_blocked` or stale data

Risk:

- per-market / per-round inventory must stay within side cap 100 and gross cap 250
- no live/prod execution
- no pair-completion `worst_case_pnl` interpretation for this profile
- fill-source realism must be separately checked before any live discussion

## Current Blockers

1. It is still a Python-researched causal proxy mapped into Rust, not source-of-truth verified live execution.
2. Paper-shadow dry-run fill realism can diverge from live fills; User WS / authenticated execution truth remains a separate research lane.
3. The strategy carries directional settlement inventory; drawdown must be judged by fee-adjusted settlement PnL and market/day loss attribution, not pair residual.
4. Promotion requires post-shadow evidence that high-pressure trigger cadence, side/gross caps, and fee100 PnL survive real recorder data.

## Promotion Rule

Do not promote from local patch to shadow unless all local readiness commands pass and the run owner explicitly approves a dry-run paper-shadow restart.

Do not promote from paper-shadow to live unless:

- fee100 PnL remains positive over a material shadow window,
- drawdown is bounded by agreed market/day caps,
- fill realism is audited against authenticated/source-of-truth evidence,
- the strategy has a dedicated live risk cap and kill switch.
