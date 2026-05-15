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
- /home/ubuntu/xuan_frontier_runs/* existing xuan-frontier artifacts and scripts

Whitelisted remote outputs:
- /home/ubuntu/xuan_frontier_runs/xuan-frontier-remote-verifier-loop/<timestamp>/
- /home/ubuntu/xuan_frontier_runs/d_branch_minorder_* only when running the D+ min-order verifier family

Primary job:
1. Discover latest validated completion_unwind_event_store_v2 labels on the research server. A label is runnable only when the directory contains all of `EVENT_STORE_MANIFEST.json`, `event_store.duckdb`, and `dataset/`, and the manifest `outputs.row_count` is greater than zero. Ignore `.YYYYMMDD.lock` files and incomplete placeholder directories; a lock alone is not a runnable label.
2. Exclude `20260514` and `20260515` from runnable labels and full-window refreshes because those days are temporarily abandoned after the gamma collection bug left data incomplete. Do not wait on, rerun, or surface UNKNOWN for only those excluded days. Re-include them only if the user explicitly says corrected data is ready.
3. Compare the remaining runnable labels with the latest D+ min-order verifier artifacts.
4. If a new eligible day exists that is not covered, run a bounded D+ verifier for that new day using the existing xuan_d_branch_passive_passive_redeem.py family and the current candidate grid: edge=0.04, target=10, px=0.010:0.990, fill_haircut=0.20/0.15/0.10 for single-day OOS; for full-window refresh, limit to fh020 with imbalance_qty_cap 6/8 and salvage_net_cap 0.95/0.96.
5. If no new eligible day exists, inspect latest artifacts and archive quietly.
6. If a verifier output is complete, compare against current frontier gates: stress100_worst > 0, actual PnL > 0, net_pair_cost < 0.94, cycles/market > 8, residual_qty_rate preferably < 4% and never accepted if risk conclusion is negative.

Every non-archive remote run must write a manifest JSON in its output directory. The manifest must include: automation id, UTC start/end, host, exact SSH command class, remote command, input paths, output path, script sha256, git/repo commit if available, process-overlap check, metric summary, and verdict. Verdict must be exactly KEEP, DISCARD, or UNKNOWN. Do not use vague language like “looks good” without a verdict.

Automation does not make final production decisions. If verdict is KEEP, produce a concise patch/verifier/shadow recommendation for the main thread. If DISCARD, record why and the next bounded search axis. UNKNOWN is normally non-material: write memory/manifest if appropriate, then archive. Surface UNKNOWN only when it is a repeated verifier infrastructure failure or requires main-thread action.

STRICT AUTO-ARCHIVE RULE: If there is no new KEEP/DISCARD verifier result, no repeated infrastructure failure, and no material decision request, final output exactly and only: ::archive{reason="routine xuan frontier remote verifier checkpoint"}. This includes no-new-day runs, successful preflight with no work, ordinary SSH preflight failure, overlap/no-op, and ordinary UNKNOWN. Do not output summaries, bullets, status text, "quiet progress", or "no new day" for routine runs. For material findings, keep final under 8 lines with exact remote artifact paths and the KEEP/DISCARD/UNKNOWN verdict.
```
