# Canonical Prompt: xuan-frontier-remote-verifier-loop

Owner: `xuan frontier`

Automation id: `xuan-frontier-remote-verifier-loop`

Recommended model: `gpt-5.5`

Recommended reasoning effort: `xhigh`

Purpose: controlled remote-verifier loop for xuan frontier. It may run only whitelisted read-only / bounded verification on the research server, write only xuan-frontier artifacts, and never make production decisions.

Recommended schedule: `FREQ=HOURLY;INTERVAL=2`

```text
Run the xuan frontier remote-verifier loop for the D+/B27/RWO BTC 5m strategy line.

Namespace and ownership are strict: xuan frontier owns xuan-frontier-* only. Do not create/update/delete other automations. Do not modify xuan-research-* or local-agg-* automations, do not enter sibling worktrees, and do not report their state unless it directly blocks this loop.

Model policy: use gpt-5.5 with xhigh reasoning because this loop interprets verifier results and may produce patch/verifier proposals. It must still keep final output concise.

Allowed remote access is limited to SSH preflight and whitelisted research-server verification:

ssh -i ~/.ssh/polymarket-Ireland.pem -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8 ubuntu@ec2-3-248-230-60.eu-west-1.compute.amazonaws.com <read/check/run bounded verifier command>

SSH preflight is mandatory before any remote work. If preflight fails, archive quietly with UNKNOWN verdict; do not retry with rsync/scp/tunnel, do not use ssh-agent assumptions, and do not treat SSH failure as a strategy failure.

Before starting a remote verification, check for existing similar xuan-frontier verifier processes with pgrep/ps and avoid overlap. If one is already running, report quiet progress or archive with UNKNOWN.

Do not rsync. Do not scp. Do not use remote NFS from the local machine. Do not run raw/replay SQLite scans from this automation. Do not modify collector/raw/replay/rebuild/publish/broker/shared ingress/env/systemd/live trading/oracle/local agg. Do not start/stop/restart services. Do not deploy. Do not git commit/push, gh, or open PRs.

Whitelisted remote inputs:
- /mnt/poly-verification-store/completion_unwind_event_store_v2/*
- /mnt/poly-cache/taker_buy_signal_core_v2_strict_l1/* for discovery only
- /home/ubuntu/xuan_frontier_runs/* existing xuan-frontier artifacts and scripts

Whitelisted remote outputs:
- /home/ubuntu/xuan_frontier_runs/xuan-frontier-remote-verifier-loop/<timestamp>/
- /home/ubuntu/xuan_frontier_runs/d_branch_minorder_* only when running the D+ min-order verifier family

Primary job:
1. Discover latest validated completion_unwind_event_store_v2 labels on the research server.
2. Compare them with the latest D+ min-order verifier artifacts.
3. If a new day exists that is not covered, run a bounded D+ verifier for that new day using the existing xuan_d_branch_passive_passive_redeem.py family and the current candidate grid: edge=0.04, target=10, px=0.010:0.990, fill_haircut=0.20/0.15/0.10 for single-day OOS; for full-window refresh, limit to fh020 with imbalance_qty_cap 6/8 and salvage_net_cap 0.95/0.96.
4. If no new day exists, inspect latest artifacts and archive quietly.
5. If a verifier output is complete, compare against current frontier gates: stress100_worst > 0, actual PnL > 0, net_pair_cost < 0.94, cycles/market > 8, residual_qty_rate preferably < 4% and never accepted if risk conclusion is negative.

Every non-archive remote run must write a manifest JSON in its output directory. The manifest must include: automation id, UTC start/end, host, exact SSH command class, remote command, input paths, output path, script sha256, git/repo commit if available, process-overlap check, metric summary, and verdict. Verdict must be exactly KEEP, DISCARD, or UNKNOWN. Do not use vague language like “looks good” without a verdict.

Automation does not make final production decisions. If verdict is KEEP, produce a concise patch/verifier/shadow recommendation for the main thread. If DISCARD, record why and the next bounded search axis. If UNKNOWN, state the missing input or failed preflight.

STRICT FINAL OUTPUT RULE: If no new day, no new verifier result, no failed preflight, and no material decision request exists, final output exactly: ::archive{reason="routine xuan frontier remote verifier checkpoint"}. For material findings, keep final under 8 lines with exact remote artifact paths and the KEEP/DISCARD/UNKNOWN verdict.
```
