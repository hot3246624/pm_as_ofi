# Observable Pre-Action Feature-Join Handoff Diagnostic

日期：2026-05-26

结论：`RUNNING_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_HANDOFF_NO_ORDER_DIAGNOSTIC`

用户明确批准了这个 exact bounded no-order diagnostic，用于生成 observable pre-action feature-join handoff 输入。本轮只启动一个 diagnostic-only no-order runner；未启动 G2 canary/live，未 repo sync/build，未使用 systemd/service controls，未修改 shared-ingress/broker/env/live，未读或拉取 events JSONL，未扫描 raw/replay/full store，未发送 order/cancel/redeem。

## Active Run

- remote host: `ubuntu@ec2-52-209-13-135.eu-west-1.compute.amazonaws.com`
- remote run dir: `/srv/pm_as_ofi/xuan_research_runs/xuan_research_observable_pre_action_feature_join_handoff_20260526T020736Z`
- runner PID: `505126`
- started around: `2026-05-26T03:05:57Z`
- expected finish around: `2026-05-26T04:05:57Z`
- startup artifact: `xuan_research_artifacts/xuan_observable_pre_action_feature_join_handoff_driver_20260526T020736Z/manifest.json`

## Preflight

Preflight passed:

- host `ip-172-31-37-43`
- root disk `55%`
- root inode `2%`
- active pmofi python runner count before start `0`
- active pmofi xuan runner count before start `0`
- shared-ingress broker manifest, market.sock, chainlink.sock, local_price.sock present
- broker manifest age `1s`
- helper `/srv/pm_as_ofi/repo/scripts/resolve_market_ids.py` present
- `sudo -n -u pmofi` OK

Only current `tools/xuan_dplus_passive_passive_shadow_runner.py` was copied into the isolated run dir.

## Run Config

- `PM_DRY_RUN=true`
- `PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main`
- `PM_SHARED_INGRESS_ROLE=client`
- prefix `btc-updown-5m`
- round offsets `0..25`
- duration `3600s`
- edge `0.075`
- target qty `5`
- max open cost `80`
- imbalance qty cap `1.25`
- seed offset max `300s`
- activation mode `none`
- enabled flags:
  - `--event-lite-summary`
  - `--source-link-transition-event-lite-summary`
  - `--observable-pre-action-rule-miner-feature-join-output`

Disabled or not used: symmetric activation, late-repair, fill-to-balance, micro-deficit repair guard, portfolio-ledger trading rule, source-opportunity marker families, public-profile single-day filters, live/canary/orders/cancels/redeems.

## Live Check

Initial retry live check:

- PID alive: yes
- exact run-dir python count `1`
- all pmofi runner count `1`
- summary JSON count `26`
- per-slug feature-join manifest count `26`
- output manifest present
- aggregate report not present yet
- aggregate feature-join manifest not present yet
- stdout/stderr size `0`

Aggregate outputs are expected only after completion.

## Next Step

Next heartbeat should poll PID `505126` and must not start a second runner. If the run has finished, pull back only allowlisted files:

- `output/manifest.json`
- `output/aggregate_report.json`
- `output/*.summary.json`
- `output/observable_pre_action_candidate_rows.jsonl`
- `output/observable_pre_action_source_link_summary.json`
- `output/observable_pre_action_feature_join_manifest.json`
- `stdout.log`
- `stderr.log`
- `runner.pid`
- small sanity files

Do not pull or read events JSONL.

After pullback, run feature-join output scorer and then the safe handoff/arrival validation chain. This diagnostic is not strategy evidence, private truth, deployable evidence, canary readiness, or promotion evidence.

`strategy_evidence=false`

`private_truth_ready=false`

`deployable=false`

`promotion_gate.passed=false`
