# Observable Pre-Action Same-Window Label Handoff Diagnostic

日期：2026-05-26

结论：`RUNNING_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_NO_ORDER_DIAGNOSTIC`

用户明确批准后，已启动一个 exact bounded no-order diagnostic，用来生成同窗口 feature rows + offline label handoff。

## Run

- run name: `xuan_research_observable_pre_action_same_window_label_handoff_20260526T110803Z`
- host: `ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com`
- remote run dir: `/srv/pm_as_ofi/xuan_research_runs/xuan_research_observable_pre_action_same_window_label_handoff_20260526T110803Z`
- runner PID: `522389`
- started around: `2026-05-26T11:16:12Z`
- expected finish around: `2026-05-26T12:16:12Z`

## Config

- `PM_DRY_RUN=true`
- shared-ingress client root: `/srv/pm_as_ofi/shared-ingress-main`
- prefix: `btc-updown-5m`
- offsets: `0..25`
- duration: `3600s`
- edge: `0.075`
- target qty: `5`
- max open cost: `80`
- imbalance qty cap: `1.25`
- seed offset max: `300s`
- activation mode: `none`

Enabled flags:

- `--event-lite-summary`
- `--source-link-transition-event-lite-summary`
- `--observable-pre-action-rule-miner-feature-join-output`
- `--observable-pre-action-same-window-offline-label-handoff-output`

Disabled or not used:

- symmetric activation
- late-repair
- fill-to-balance
- micro-deficit repair guard
- portfolio-ledger trading rule
- source-opportunity marker families
- public-profile single-day filters
- live/canary/orders/cancels/redeems

## Safety

Preflight passed: no active `pmofi` xuan runner, shared-ingress broker/socket files present, broker manifest fresh, helper present, sudo as `pmofi` OK, root disk/inode sane.

Only the current `tools/xuan_dplus_passive_passive_shadow_runner.py` was copied into the isolated run dir. No repo sync/build, no service/systemd controls, no shared-ingress/broker/env/live writes, no events JSONL read/pull, no raw/replay/full-store scan, and no order/cancel/redeem.

The first start attempt failed before `cd` because the remote directory had not been created with sufficient permission. It did not start a runner. The directory creation was retried with sudo, the runner was copied once, and exactly one runner was started.

Initial live check:

- PID alive: `522389`
- exact run-dir python count: `1`
- all `pmofi` runner count: `1`
- `runner.pid`: `522389`
- summary JSON count: `26`
- per-slug feature-join manifests: `26`
- per-slug label CSV files: `26`
- per-slug label handoff manifests: `26`
- aggregate report not present yet
- aggregate same-window label handoff not present yet
- stdout/stderr size: `0`

## Status

This is a diagnostic no-order handoff-generation run only. It is not strategy evidence, private truth, deployable evidence, promotion evidence, or canary readiness.

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`
