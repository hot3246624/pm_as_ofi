# Local Agg 迁移交接 - 2026-05-19

更新时间：2026-05-19 01:10 CST

本文档是停止旧服务器迭代前的 localagg 收尾快照。它只覆盖 localagg 线，不覆盖 xuan-frontier / xuan-research。

## 1. Git 共享状态

共享主线已经包含 localagg 变更：

```text
origin/main 6b634a90 Add xuan frontier migration handoff
localagg merge base on main: 15b4917b Merge localagg recorder depth evidence
origin/codex/localagg-work 0647cd5d localagg: enable dry-run recorder by default
```

迁移时以 `origin/main` 为共享同步源。`codex/localagg-work` 只保留 localagg 线历史和本交接提交。

旧服务机 `/srv/pm_as_ofi/repo` 已经被 reconcile 到 clean main：

```text
remote HEAD: 15b4917b25b12d7ebaba37d5df386094ffa295c5
git status: clean
clean reconcile artifact: /tmp/localagg_clean_reconcile_20260518T105804Z
dirty backup patch: /tmp/localagg_clean_reconcile_20260518T105804Z/dirty_tracked_diff.patch
```

## 2. 最后只读快照

停止前最后一次只读快照：

```text
artifact: /tmp/localagg_shutdown_20260518T170747Z
snapshot: /tmp/localagg_shutdown_20260518T170747Z/shutdown_snapshot.json
manifest: /tmp/localagg_shutdown_20260518T170747Z/manifest.txt
```

远端检查前使用了规定的 exact preflight：

```bash
ssh -i ~/.ssh/polymarket-Ireland.pem -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=10 ubuntu@ec2-3-248-230-60.eu-west-1.compute.amazonaws.com 'true'
```

preflight 通过，后续只做只读快照，没有部署、重启或改配置。

## 3. 旧服务器运行态

旧服务器仍保持 dry-run / lab-only 姿态：

```text
pm-shared-ingress-broker.service active
pm-oracle-lag.service active
pm-local-agg-challenger.service active
pm-xuan-shadow.service active, xuan namespace, localagg 不处理
```

clean reconcile 后已经重启过以下服务，因为它们使用同一个 release binary：

```text
pm-shared-ingress-broker.service
pm-oracle-lag.service
pm-local-agg-challenger.service
```

没有重启：

```text
Tier-A pinned local-agg-tiera-shadow binary
pm-xuan-shadow.service
```

shared ingress 实际 root：

```text
/srv/pm_as_ofi/shared-ingress-main
```

不要再使用旧假设：

```text
/srv/pm_as_ofi/repo/run/shared-ingress-main
```

最新快照里 broker manifest 心跳新鲜，socket 存在：

```text
/srv/pm_as_ofi/shared-ingress-main/broker_manifest.json
/srv/pm_as_ofi/shared-ingress-main/chainlink.sock
/srv/pm_as_ofi/shared-ingress-main/local_price.sock
/srv/pm_as_ofi/shared-ingress-main/market.sock
```

## 4. Recorder / Book Depth 状态

localagg recorder 已启用，book-depth/depletion evidence 已落盘：

```text
recorder root: /srv/pm_as_ofi/repo/data/recorder/local-agg-challenger
```

最后快照抽样计数：

```text
market_side/event_time_ms: 536636
source_sequence_id:       529624
best_bid_size/ask_size:   67392
best_bid/ask delta/drop:  23673-23676
yes/no side size fields:  41045-41048
```

已确认字段包括：

```text
market_side
asset_id
event_time_ms
source_sequence_id
best_bid_size
best_ask_size
best_bid_size_delta
best_ask_size_delta
best_bid_drop_qty
best_ask_drop_qty
yes_bid_sz
no_ask_sz
yes_bid_size_delta
no_ask_size_delta
yes_bid_drop_qty
no_ask_drop_qty
```

这满足“live book-depth/depletion evidence 进入 recorder”的需求。

## 5. Main Lane 状态

主 run 只用于研究证据，不再是 promotion candidate：

```text
run_id: 20260514_084708
run_dir: logs/local-agg-challenger/runs/20260514_084708
status: FAILED for promotion, accepted>=500 formalized G1 failure
```

最后快照：

```text
final_rows:           3276
accepted:             1114
gated:                2162
accepted_side_errors: 3
accepted_max_bps:     8.152221
accepted_p95_bps:     4.70095445
accepted_p99_bps:     approx 6.38213
hard >=5bps:          44
gt3:                  140
latency_p95_ms:       53
latency_max_ms:       293
```

主要 hard residual buckets：

```text
DOGE/USD drop_binance okx last_before:                         9
ETH/USD eth_binance_missing_fallback binance after_then_before: 7
DOGE/USD drop_binance bybit;okx last_before:                   6
XRP/USD only_binance_coinbase binance nearest_abs:              6
BNB/USD drop_okx binance;bybit after_then_before:               5
DOGE/USD drop_binance bybit last_before:                        5
```

## 6. Tier-A 状态

Tier-A 是 research-only very-low-coverage lane，不是替代候选：

```text
run_id: 20260516_140827
run_dir: logs/local-agg-tiera-shadow/runs/20260516_140827
```

最后快照：

```text
final_rows:           1672
accepted:             9
gated:                1663
accepted_side_errors: 0
hard >=5bps:          0
accepted_max_bps:     1.25713
accepted_p95_bps:     1.2390372
latency_p95_ms:       102.4
latency_max_ms:       138
```

如果继续监控，下一次 stall threshold 应从：

```text
final_rows >= 2200 and accepted <= 9
```

开始算，不要重复通知旧的 5/7/8 accepted stall。

## 7. 当前模型结论

保留 refined Coinbase older rule 的结论，但不要期待它解决所有 hard tails。

runtime-visible predicate：

```text
target source_subset in:
  drop_binance
  drop_okx
  eth_binance_missing_fallback
  sol_okx_missing_fallback
  doge_binance_fallback
  sol_binance_fallback
  only_binance_coinbase
current local_sources excludes coinbase
coinbase close tick exists 5s..20s before round_end
choose latest such coinbase tick
exclude hype/usd drop_binance
exclude doge/usd drop_binance with okx-only local_sources
```

Online rule 不允许用：

```text
close_diff_bps
side_match
RTDS close/open
hindsight best-candidate selection
```

## 8. Residual-family 诊断

最后 accepted-only residual 诊断：

```text
artifact: /tmp/localagg_shutdown_20260518T170747Z/shutdown_snapshot.json
baseline: n=1114, side_errors=3, hard=44, gt3=140, p95=4.70095445, p99≈6.38213, max=8.152221
```

诊断 gate 不是批准实现，只是迁移后继续验证的候选：

```text
doge_drop_binance_no_coinbase:
  removes 43 rows / 20 hard
  remaining hard=24
  p95≈4.13886
  side_errors unchanged at 3

doge_drop_binance_single_source:
  removes 30 rows / 14 hard

xrp_only_binance_no_exact_margin_ge7p5:
  removes 17 rows / 6 hard

eth_binance_missing_no_exact_margin_ge8:
  removes 12 rows / 5 hard
```

这些都是 coverage-sacrifice diagnostics。迁移后若要实现，必须先做 fixed-suite validation，再改 evaluator/Rust。

## 9. 迁移后继续工作的顺序

1. 先恢复 shared-ingress broker 和 localagg recorder。
2. 验证 recorder depth fields 继续落盘。
3. 只读重算 main/Tier-A summary，确认旧样本指标可复现。
4. 建立固定验证套件，避免只盯增长中的 current run。
5. 继续 residual-family modeling：

```text
Priority 1: DOGE drop_binance no-coinbase/single-source variants
Priority 2: XRP only_binance_coinbase single-binance no-exact high-margin
Priority 3: ETH binance-missing single-binance no-exact high-margin
Priority 4: BNB/SOL
```

只允许 runtime-visible predicates：

```text
symbol
source_subset
local_sources
rule
close_abs_delta_ms
source freshness/age
source spread
direction_margin_bps
source_count
exact-source count
```

## 10. 停止旧自动化

本 handoff 提交后，旧线程的 `local-agg-sample-gate-monitor` heartbeat 应暂停，避免迁移期间继续操作旧服务器。

如果要在新服务器恢复自动化，先把 prompt 中的远端 host、repo path、shared ingress root、run ids 和 recorder root 全部改成新服务器实际值。

## 11. 未纳入提交的本地文件

本地 worktree 中存在未跟踪脚本：

```text
scripts/sync_localagg_to_ireland.sh
```

它不是本次收尾提交的一部分。原因：

- 它会对远端执行 rsync/scp/ssh，不适合作为迁移前的默认共享工具。
- 默认 remote root 是 `/srv/pm_as_ofi`，与当前 clean repo root `/srv/pm_as_ofi/repo` 不一致。
- 迁移到新服务器前应另写明确的新服务器 bootstrap/sync 脚本。

## 12. 最短交接口径

```text
localagg 旧服务器已停止迭代准备迁移。
origin/main 是共享源；旧服务机 repo clean at 15b4917b。
最后快照在 /tmp/localagg_shutdown_20260518T170747Z/shutdown_snapshot.json。
main lane 20260514_084708 已失败，仅用于 residual research：
accepted=1114, hard=44, side_errors=3, p95=4.70095445, max=8.152221。
Tier-A 20260516_140827 accepted=9, side_errors=0, hard=0，但覆盖太低。
localagg recorder 已确认落 book-depth/depletion fields。
下一步迁移后先恢复 shared-ingress/recorder，再做固定验证套件和 DOGE/XRP/ETH residual-family validation。
```
