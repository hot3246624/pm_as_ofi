# Local Agg 新线程交接与 Worktree 操作步骤

更新时间：2026-05-06 12:50 CST

## 1. 当前仓库状态

主线已统一：

- `main` 最新提交：`ccd96b4`
- `origin/main` 已推送
- GitHub 远端已合并分支已删除，现在只剩 `origin/main`

当前本机 worktree：

```text
/Users/hot/web3Scientist/pm_as_ofi_main_merge  main，推荐作为稳定 main 参考目录
/Users/hot/web3Scientist/pm_as_ofi             codex/self-built-price-aggregator，本地 dirty，不要直接动
/Users/hot/.codex/worktrees/91a3/pm_as_ofi     detached HEAD，需确认是否仍被其他 agent 使用
```

重要限制：

- 不要在多个 worktree 同时 checkout 同一个本地 `main` 分支。
- 如果看到 `fatal: 'main' is already used by worktree at ...`，这是 Git 的正常限制。
- 新 agent 不应该在 `/Users/hot/web3Scientist/pm_as_ofi` 里直接切 `main`，因为该目录有未提交/未跟踪改动。

## 2. 推荐的新 agent 启动方式

### 2.1 Local Agg 新线程

在终端执行：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-localagg -b codex/localagg-work origin/main
```

然后让 Codex 新线程打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-localagg
```

如果 `codex/localagg-work` 已经存在，则改用：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git worktree add ../pm_as_ofi-localagg codex/localagg-work
```

如果目录也已经存在，先检查：

```bash
git worktree list
git branch
```

不要直接 `rm -rf`，除非确认该 worktree 没有未提交改动。

### 2.2 网络/流量审计新线程

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-network -b codex/network-audit origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-network
```

### 2.3 PGT / xuan 策略新线程

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-pgt -b codex/pgt-work origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-pgt
```

### 2.4 只读查看 main

如果只是看代码，不改代码：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add --detach ../pm_as_ofi-readonly origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-readonly
```

## 3. 不要碰的目录

不要让新 agent 默认使用这个目录做开发：

```text
/Users/hot/web3Scientist/pm_as_ofi
```

原因：

- 当前分支是 `codex/self-built-price-aggregator`，但远端分支已删除。
- 该 worktree 有本地 dirty 文件和多个未跟踪研究脚本。
- 直接 `reset`、`clean`、`switch main` 都可能覆盖其他 agent 或用户的工作。

如必须处理，先执行：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi
git status --short --branch
```

然后让用户决定是 stash、迁移、提交还是删除。

## 4. 当前运行实例

截至 2026-05-06 12:43 CST，以下实例在跑：

- `shared_ingress_broker`
- `local_agg_challenger`
- `local_agg_light_monitor`
- `pgt_shadow_loop`

不要默认停止 `shared_ingress_broker`，它是跨进程共享数据平面，可能被多个消费者使用。

当前 local agg run：

```text
logs/local-agg-boundary-challenger-lab/runs/20260506_004039
```

关键日志：

```text
logs/local-agg-boundary-challenger-lab/runs/20260506_004039/local_agg_lab_20260506_004039.log
```

## 5. 12:43 Local Agg 验收结果

验收方式：

- 使用当前 run `20260506_004039`
- 重新构建 current-run boundary dataset
- 跑 `evaluate_local_agg_boundary_router.py`
- 使用最新已训练 gate model：

```text
logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json
```

输出：

```text
/tmp/local_agg_boundary_dataset_handoff_20260506_124343.csv
/tmp/local_agg_boundary_router_eval_handoff_20260506_124343.csv
/tmp/local_agg_gate_handoff_20260506_124343.json
```

结果：

```text
eval rows              956
accepted               591
filtered               127
missing                20
gated                  218
accepted side errors   0
accepted mean          0.813614 bps
accepted max           4.49144 bps
local p50              28 ms
local p95              140 ms
local max              304 ms
worker late starts     0
```

结论：

- 当前 dry-run gated path 是 clean。
- 这不是生产启用许可。
- raw router 未 gate 前仍有 side errors，这是预期；验收看 accepted-after-gate。
- `local max=304ms` 是边缘尾部，p95 很稳；如果生产前要求硬性 `max <= 300ms`，需要继续查这个尾部点。

## 6. 正式自动化报告位置

最新完整正式 hourly 报告：

```text
logs/local-agg-boundary-challenger-lab/monitor_reports/latest_recap.log
logs/local-agg-boundary-challenger-lab/monitor_reports/automation_state.json
logs/local-agg-boundary-challenger-lab/monitor_reports/automation_recap_20260506_102709.log
```

该报告结论：

```text
action                 clean_keep_monitoring
accepted side errors   0
accepted max           4.149405 bps
local p95              194 ms
pass                   true
```

## 7. 新 agent 第一轮应该做什么

新 agent 接手后不要马上改策略，先做这几步：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi-localagg

git status --short --branch

tmux ls 2>/dev/null || true
ps -axo pid,ppid,stat,etime,%cpu,%mem,command | rg 'polymarket_v2|local_agg|shared_ingress|pgt_shadow' | rg -v 'rg '

cat /Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/automation_state.json | head -120
cat /tmp/local_agg_gate_handoff_20260506_124343.json
```

然后汇报：

- 当前 run 是否仍在增长
- `local_agg_challenger` 是否仍在跑
- 最新 accepted-after-gate 是否仍为 0 side errors
- p95 latency 是否仍低于 300ms

## 8. 继续推进的准则

继续使用这些硬门槛：

- accepted side errors 必须为 `0`
- accepted max error 必须 `< 5bps`
- local final p95 必须 `< 300ms`
- 不允许启用 live trading
- `PM_DRY_RUN=true`
- `PM_ORACLE_LAG_LAB_ONLY=true`

如果出现 accepted side error：

1. 打开对应 eval CSV。
2. 只看 `gate_status=accepted` 且 `side_error=True` 的行。
3. 按以下字段聚类：

```text
symbol
source_subset
rule
sources
gate_key_level
direction_margin_bps
source_spread_bps
close_abs_delta_ms
```

4. 如果是 near-flat、stale-source、source-spread、single-source tail，允许做窄过滤。
5. 窄过滤必须同步到：

```text
scripts/evaluate_local_agg_boundary_router.py
src/bin/polymarket_v2.rs
```

6. 改完必须跑：

```bash
python3 scripts/evaluate_local_agg_boundary_router.py --boundary-csv <dataset.csv> --out-csv <eval.csv> --sample-mode latest --test-rounds 999999
cargo check --bin polymarket_v2
```

如果要做以下动作，必须先问用户：

- 新增或删除交易所/价格源
- 改全局权重或聚合家族
- 放宽 `5bps`、`0 side error`、`300ms` 门槛
- 大幅牺牲 coverage
- 启用 production/live trading
- 停止或重启 `shared_ingress_broker`

## 9. 常用命令

正式 hourly check：

```bash
/Users/hot/.codex/skills/local-agg-hourly/scripts/hourly_check.sh /Users/hot/web3Scientist/pm_as_ofi
```

只看当前 run 快速验收：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi
RUN=logs/local-agg-boundary-challenger-lab/runs/20260506_004039
STAMP=$(date +%Y%m%d_%H%M%S)
python3 scripts/build_local_agg_boundary_dataset.py \
  --logs-root logs \
  --instance-glob "local-agg-boundary-challenger-lab/runs/$(basename "$RUN")" \
  --mode full \
  --out-csv "/tmp/local_agg_boundary_dataset_${STAMP}.csv" \
  --cap-local-ready-lag-ms 300
python3 scripts/evaluate_local_agg_boundary_router.py \
  --boundary-csv "/tmp/local_agg_boundary_dataset_${STAMP}.csv" \
  --out-csv "/tmp/local_agg_boundary_router_eval_${STAMP}.csv" \
  --sample-mode latest \
  --test-rounds 999999
```

编译测试：

```bash
cargo check --bins
cargo test --lib
```

## 10. 关键文件

代码：

```text
src/bin/polymarket_v2.rs
scripts/evaluate_local_agg_boundary_router.py
scripts/evaluate_local_agg_uncertainty_gate.py
scripts/build_local_agg_boundary_dataset.py
scripts/run_local_agg_lab.sh
scripts/run_shared_ingress_broker.sh
```

模型：

```text
logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json
```

文档：

```text
docs/LOCAL_PRICE_AGG_CONVERGENCE_PLAN_ZH.md
docs/LOCAL_AGG_EXTERNAL_TAPE_BACKFILL_PLAN_ZH.md
docs/runbooks/AWS_RUNTIME_MIGRATION_RUNBOOK_ZH.md
docs/architecture/SHARED_INGRESS_MULTIPROCESS_ZH.md
docs/runbooks/LOCAL_AGG_THREAD_HANDOFF_2026-05-06_ZH.md
```
