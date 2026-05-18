# LOCAL_AGG_SOL_FALLBACK_ORDER_20260514

## Hypothesis

When SOL weighted and Coinbase fallbacks are unresolved, and both single-source
OKX and Binance fallbacks are visible, choosing OKX before Binance is a narrower
oracle-proxy improvement than adding a SOL debias model. It keeps dry-run
coverage unchanged and avoids the current accepted hard tail.

## Triggering Runtime Evidence

Checkpoint: Option B1 run `20260514_024406`, server HEAD `b1d889d`.

At `2026-05-14T06:11Z`, safety summary was:

- accepted `48`, gated `66`
- accepted_side_errors `0`
- accepted_max_bps `5.592404`
- accepted_p95_bps `3.987924`
- latency_p95_ms `51.3`, latency_max_ms `263.0`

Hard tail:

- symbol `sol/usd`
- round_end_ts `1778737800`
- source_subset `sol_binance_fallback`
- rule `after_then_before`
- local_sources `binance`
- local_close `91.21@1778737799760`
- rtds_close `91.159020193042707`
- close_diff `5.592404bps`
- direction_margin `14.707611bps`
- side matched

Same round visible alternatives:

- `sol_okx_missing_fallback`: `91.20@1778737797086`, error `4.495420bps`, same side
- `close_only_primary`: `91.202720306513413@1778737799760`, error `4.793833bps`, same side

## Fixed Replay Result

Replay set: accepted SOL rows from `20260511_083910`, `20260512_014312`,
`20260513_045906`, and `20260514_024406`.

Policy tested: if the router would accept `sol_binance_fallback` and
`sol_okx_missing_fallback` is also visible with same open-side and margin
`>=4bps`, prefer OKX. Runtime implementation is the equivalent narrow ordering:
`sol_okx_missing_fallback` before `sol_binance_fallback`.

SOL accepted rows: `231`.

Base:

- max `5.592404`
- p95 `2.761610`
- p99 `3.930794`
- mean `1.166137`
- side_errors `0`

Candidate:

- max `4.495420`
- p95 `2.674122`
- p99 `3.930794`
- mean `1.156841`
- side_errors `0`
- row_regression_gt_0.5bps `0`

Selected rows:

- `20260514_024406` SOL `1778737800`: `5.592404 -> 4.495420`
- `20260513_045906` SOL `1778659500`: `2.902984 -> 1.852701`

Full accepted replay with existing deterministic HYPE/DOGE unchanged:

- deterministic candidate before SOL fix: global max `5.592404`
- after SOL fallback preference: expected global max returns below `5bps`, with
  BNB historical max `4.984909` and DOGE current residual `4.979561` as limits.
- BTC unchanged because the trigger is SOL-only.

## Decision

Decision: `keep_runtime_dryrun_candidate`.

This is a narrow dry-run runtime change, not a gate and not live trading. It
does not relax thresholds, block samples, change global weights, or introduce
SOL/BNB/XRP frozen debias. It only changes SOL fallback priority when the
router is already in the single-source fallback path.
