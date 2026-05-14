# Canonical Prompt: xuan-frontier-research-loop

Owner: `xuan frontier`

Automation id: `xuan-frontier-research-loop`

Purpose: local-only xuan/B27/RWO frontier planning, artifact review, and quiet archival. It is not a remote executor.

```text
Run the xuan/B27/RWO BTC 5m frontier autoresearch loop in strict quiet-archive mode as a LOCAL-ONLY planning and repo/artifact loop. Work locally in /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier only.

Automation boundary is strict: do not SSH, do not rsync, do not use remote NFS, do not run remote EC2 jobs, do not use GitHub/gh/network push, do not commit/push, do not create/update/delete other automations, do not create sibling xuan heartbeats/crons, and do not spawn long-running subagents from this automation. The interactive Codex thread or an explicitly authorized server-side runner owns remote read/search/verifier execution.

Hard safety: do not modify collector/raw/replay/rebuild/publish/broker/shared ingress/env/systemd/live trading/oracle/local agg. Do not scan /mnt/poly-replay, replay_published, raw, or raw/replay SQLite. Do not start services or second shadows.

Allowed work: inspect local repo files, local artifacts/ledger, local automation memory, local scripts/docs, and prepare concise next-step plans or local patch proposals. Before any local-only analysis, auto-discover local inputs with /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier/scripts/discover_poly_backtest_inputs.py when available. If required data only exists remotely, record a local ledger note and archive quietly; do not report SSH denial as a blocker.

Strategic state: b27/D+ two-sided passive BUY from public SELL flow is the leading core; xuan provides completion/inventory lifecycle benchmark; rwo/d189 is tail/ITM control. Do not continue m0001 public-trade-triggered single-side maker unless new evidence overturns delayed-trigger failure. Metrics priority: PnL/stress first, supported by residual rate, cycles/market, and pair cost.

Current strongest implementable-size D+ result from interactive/remote work: /home/ubuntu/xuan_frontier_runs/d_branch_minorder_fillhaircut_full_0502_0512_20260514_0310. Over 2026-05-02..2026-05-12, t10/imb8/sv0950 at fill_haircut=0.20 has active=3154, pair_actions=35948, gross_cost=66105.18, net_pair_cost=0.922813, delay=26.71s, cycles=11.3976/market, qty_residual=3.7823%, cost_residual=2.5632%, PnL=+4604.17, ROI=6.9649%, stress100_worst=+2242.28. Conservative fill_haircut=0.10 remains positive: t10/imb8/sv0950 PnL=+3259.998, ROI=6.4251%, cycles=8.9537, qres=5.0688%, stress100_worst=+1194.08. Risk-balanced t10/imb6/sv0950 at fh020 has PnL=+4221.63, ROI=7.4415%, cycles=11.1062, qres=3.3624%, stress100_worst=+2152.83; at fh010 stress100_worst=+1104.76. Xuan public benchmark: pair_cost about 0.9783, cycles about 5.89/market, residual_market_rate about 11.08%, ROI about 2.22%.

Research queue state: commit 306953c4 added the local/remote research job protocol. Remote job /home/ubuntu/xuan_frontier_runs/job_runs/dplus_minorder_verifier_request_20260514_01 is waiting_external_verifier and only records the verifier request. Verifier queue was refreshed by interactive work, not run by automation: /home/ubuntu/xuan_frontier_runs/verifier_specs/verifier_spec_d_plus_minorder_fillhaircut_20260514_0310.json and .md.

Highest-value next steps for this local-only loop: prepare or refine a local dry-run-only Rust mapping plan for D+ two-sided passive inventory, identify exact files/tests needed, and archive unless a concrete local patch should be surfaced. Do not repeat discarded axes without new evidence: imbalance_cost_cap, salvage cap sweep alone, residual_cooldown_cost_cap, open-inventory pair-priority, internal pair_select, dynamic target_policy, hard L1 delta thresholds, and seed micro-gates.

STRICT FINAL OUTPUT RULE: If this hourly run has no new local patch proposal, no decision request, no data-source request, and no material strategy finding from already-local artifacts, the final response must be exactly this single line and nothing else: ::archive{reason="routine xuan frontier checkpoint"}. Do not include summaries, bullets, or status text in routine runs. Leave the thread unarchived only for material local strategy findings, decision requests, data-source blockers, failed local runs, or a candidate that should move to Rust patch; in those cases keep the visible final under 6 lines and include exact artifact paths. Do not ask whether to continue; keep/discard/refine and proceed unless blocked.
```

