# Canonical Prompt: xuan-frontier remote verifier

Owner: `xuan frontier`

Active automation id: `xuan-frontier-remote-verifier-heartbeat`

Paused legacy automation id: `xuan-frontier-remote-verifier-loop`

Recommended model: `gpt-5.5`

Recommended reasoning effort: `xhigh`

Purpose: controlled remote-verifier loop for xuan frontier. It may run only whitelisted read-only / bounded verification on the research server, write only xuan-frontier artifacts, and never make production decisions.

Recommended schedule: heartbeat `FREQ=HOURLY;INTERVAL=1`

Important operational note:

- Do **not** run this as a standalone cron automation. The old cron `xuan-frontier-remote-verifier-loop` was paused because its own sandbox returned `Operation not permitted` for SSH, even with fixed IP and HostKeyAlias.
- Run this as a thread heartbeat attached to the main xuan-frontier thread. The heartbeat uses the current thread environment, where fixed-IP SSH has been verified.
- Do not use successful SSH from an interactive/main thread as proof that a standalone cron can SSH. If someone wants to re-enable cron later, first prove SSH and DuckDB import from the cron's own memory/artifact.

```text
Run the xuan frontier remote-verifier heartbeat for the D+/B27/RWO BTC 5m strategy line.

Namespace and ownership are strict: xuan frontier owns xuan-frontier-* only. Do not create/update/delete other automations. Do not modify xuan-research-* or local-agg-* automations, do not enter sibling worktrees, and do not report their state unless it directly blocks this loop.

Model policy: use gpt-5.5 with xhigh reasoning because this loop interprets verifier results and may produce patch/verifier proposals. It must still keep final output concise.

This must run as a thread heartbeat, not as a standalone cron. The standalone cron environment has previously blocked SSH with `Operation not permitted`; the heartbeat should use the main xuan-frontier thread environment.

Allowed remote access is limited to SSH preflight and whitelisted research-server verification:

Use the canonical fixed-IP SSH target for every remote command:

```bash
SSH_OPTS="-i ~/.ssh/polymarket-Ireland.pem -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8 -o HostKeyAlias=ec2-3-248-230-60.eu-west-1.compute.amazonaws.com"
SSH_TARGET="ubuntu@3.248.230.60"
ssh $SSH_OPTS "$SSH_TARGET" '<read/check/run bounded verifier command>'
```

Do not use `ubuntu@ec2-3-248-230-60.eu-west-1.compute.amazonaws.com` as the connection target inside this automation. The EC2 public DNS name has produced repeated automation-only DNS resolution failures even after SSH preflight passed. The fixed IP plus `HostKeyAlias` preserves host-key checking while avoiding DNS.

SSH preflight is mandatory before any remote work, and every later SSH command in the same run must reuse the exact same fixed-IP target and options. If fixed-IP preflight fails, archive quietly with UNKNOWN verdict; do not retry with rsync/scp/tunnel, do not use ssh-agent assumptions, and do not treat SSH failure as a strategy failure. If preflight succeeds but a later command fails with DNS resolution, treat that as a loop-command bug: correct the command to the canonical fixed-IP form and retry once before surfacing infrastructure UNKNOWN.

Before starting a remote verification, check for existing similar xuan-frontier verifier processes with pgrep/ps and avoid overlap. If one is already running, write memory/manifest if needed and archive with UNKNOWN.

Remote Python is explicit: use `/tmp/xuan_duckdb_venv/bin/python` for D+ verifier commands, falling back only to `/home/ubuntu/.venvs/xuan-duckdb/bin/python` if the first path is missing. Do not call bare `python3` or `/usr/bin/python3` for DuckDB verifiers. Before launching, run a one-line import preflight (`import duckdb`) with the selected interpreter and write UNKNOWN if neither interpreter exists or imports duckdb.

Do not rsync. Do not scp. Do not use remote NFS from the local machine. Do not run raw/replay SQLite scans from this automation. Do not modify collector/raw/replay/rebuild/publish/broker/shared ingress/env/systemd/live trading/oracle/local agg. Do not start/stop/restart services. Do not deploy. Do not git commit/push, gh, or open PRs.

Whitelisted remote inputs:
- /mnt/poly-verification-store/completion_unwind_event_store_v2/*
- /mnt/poly-cache/taker_buy_signal_core_v2_strict_l1/* for discovery only
- /mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb for B27/RWO public-account audit/proxy truth
- /home/ubuntu/xuan_frontier_runs/* existing xuan-frontier artifacts and scripts

Whitelisted remote outputs:
- /home/ubuntu/xuan_frontier_runs/xuan-frontier-remote-verifier-loop/<timestamp>/
- /home/ubuntu/xuan_frontier_runs/d_branch_minorder_* only when running the D+ min-order verifier family

Primary job:
1. Use the data-source rules in `docs/research/xuan/XUAN_BACKTEST_DATA_SOURCES_20260515_ZH.md`: routine research may use only 2026-05-02..2026-05-13. Do not include 20260514 or 20260515 unless the user explicitly says the data is corrected or asks for fault forensics.
2. Discover latest validated completion_unwind_event_store_v2 labels on the research server. A label is runnable only when the directory contains `EVENT_STORE_MANIFEST.json` and `event_store.duckdb`, and the label does not include 20260514 or 20260515. Current expected labels: `20260502_20260508`, `20260509`, `20260510`, `20260511`, `20260512`, `20260513`.
3. Discover strict V2 cache labels only for taker-buy/public trigger searches. A cache label is valid only when `CACHE_MANIFEST.json` exists and the label does not include 20260514 or 20260515. Current expected labels: `20260502_20260507`, `20260508`, `20260509`, `20260510`, `20260511`, `20260512`, `20260513`.
4. Use `/mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb` for B27/RWO public account audit/proxy truth. This table is useful for observed fill/merge/redeem/settlement behavior and strict L1/L2 context, but it is not private queue truth.
5. Current D+ reactive-after-public-SELL Rust mapping is not a candidate after shadow-realistic verifier failures. Do not repeat reactive-after-sell D+ grids. Next research axis is pre-positioned resting two-sided maker inventory calibrated against B27/RWO public account audit and tested against completion_unwind_event_store_v2.
6. If a verifier output is complete, compare against current frontier gates: stress100_worst > 0, actual PnL > 0, net_pair_cost < 0.94, cycles/market > 8, residual_qty_rate preferably < 4% and never accepted if risk conclusion is negative.

Every non-archive remote run must write a manifest JSON in its output directory. The manifest must include: automation id, UTC start/end, host, exact SSH command class, remote command, input paths, output path, script sha256, git/repo commit if available, process-overlap check, metric summary, and verdict. Verdict must be exactly KEEP, DISCARD, or UNKNOWN. Do not use vague language like “looks good” without a verdict.

Automation does not make final production decisions. If verdict is KEEP, produce a concise patch/verifier/shadow recommendation for the main thread. If DISCARD, record why and the next bounded search axis. UNKNOWN is normally non-material: write memory/manifest if appropriate, then archive. Surface UNKNOWN only when it is a repeated verifier infrastructure failure or requires main-thread action.

STRICT AUTO-ARCHIVE RULE: If there is no new KEEP/DISCARD verifier result, no repeated infrastructure failure, and no material decision request, final output exactly and only: ::archive{reason="routine xuan frontier remote verifier checkpoint"}. This includes no-new-day runs, successful preflight with no work, ordinary SSH preflight failure, overlap/no-op, and ordinary UNKNOWN. Do not output summaries, bullets, status text, "quiet progress", or "no new day" for routine runs. For material findings, keep final under 8 lines with exact remote artifact paths and the KEEP/DISCARD/UNKNOWN verdict.
```
