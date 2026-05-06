# 多线程 Worktree 与策略交接 - 2026-05-06

更新时间：2026-05-06 13:00 CST

## 0. 目标

把后续工作拆成 3 个互不踩代码的方向：

1. Local Agg：本地价格聚合器收敛、验收、dry-run 复盘。
2. Xuan Research：xuanxuan008 研究版，只做数据、复盘、模型假设验证。
3. Xuan Frontier：xuanxuan008 先行版，把已验证假设做成 shadow/prototype，不直接 live。

每个方向使用独立 worktree + 独立分支。不要让多个 agent 在同一个 dirty 目录里并行改代码。

## 1. 当前 Git 状态

主线：

```text
main: 3c5aef26 docs: add local agg thread handoff runbook
remote: origin/main
```

远端分支已清理，目前 GitHub 只保留 `origin/main`。

本机已有 worktree：

```text
/Users/hot/web3Scientist/pm_as_ofi_main_merge  main，稳定主线参考目录
/Users/hot/web3Scientist/pm_as_ofi             codex/self-built-price-aggregator，本地 dirty，不要直接动
/Users/hot/.codex/worktrees/91a3/pm_as_ofi     detached HEAD，确认无人使用后再清理
```

Git worktree 限制：同一个本地分支不能被两个 worktree 同时 checkout。如果看到：

```text
fatal: 'main' is already used by worktree at '...'
```

这是正常限制。解决方式不是强行切分支，而是给每个方向创建自己的分支。

## 2. 三个新 worktree

以下命令从稳定 main 目录执行。

### 2.1 Local Agg 方向

用途：继续本地价格聚合器，验收 accepted-after-gate、时延、RDTS 对齐，做窄过滤和 Rust/evaluator 同步。

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-localagg -b codex/localagg-work origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-localagg
```

### 2.2 Xuan Research 研究版

用途：只研究，不抢实现。负责 xuanxuan008 / completion-first / PGT 的历史行为复盘、交易路径归因、maker/taker 推断、数据集质量验证、假设矩阵。

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-xuan-research -b codex/xuan-research origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-xuan-research
```

边界：

- 不改执行层 live 行为。
- 不改 local agg gate 门槛。
- 产出应优先是 report、CSV、analysis script、可验证假设。

### 2.3 Xuan Frontier 先行版

用途：把研究版已验证的假设快速做成 shadow/prototype。可以改 `pair_gated_tranche`、`completion_first`、PGT shadow runner，但必须保持 dry-run/shadow。

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git fetch origin --prune
git worktree add ../pm_as_ofi-xuan-frontier -b codex/xuan-frontier origin/main
```

Codex 打开：

```text
/Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
```

边界：

- 不启用 live trading。
- 不绕过 shared-ingress。
- 不和 Research 争同一批输出文件名。
- 任何生产启用、仓位扩大、门槛放宽都必须回到用户决策。

## 3. 如果 worktree 或分支已存在

先检查：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi_main_merge
git worktree list
git branch
```

如果分支已存在但目录不存在：

```bash
git worktree add ../pm_as_ofi-localagg codex/localagg-work
git worktree add ../pm_as_ofi-xuan-research codex/xuan-research
git worktree add ../pm_as_ofi-xuan-frontier codex/xuan-frontier
```

如果目录已存在，先进入该目录检查：

```bash
git status --short --branch
```

只有确认没有未提交改动，才可以移除：

```bash
git worktree remove <path>
```

不要直接 `rm -rf`。

## 4. 不要碰的旧目录

不要让新 agent 默认使用：

```text
/Users/hot/web3Scientist/pm_as_ofi
```

原因：

- 当前分支 `codex/self-built-price-aggregator` 的远端已删除。
- 目录里有 `.claude/settings.local.json` 修改、pycache 删除、多个未跟踪研究脚本。
- 直接 `reset`、`clean`、`switch main` 可能覆盖用户或其他 agent 工作。

如必须处理，先汇报 `git status --short --branch`，让用户决定 stash、迁移、提交还是删除。

## 5. 当前运行实例

截至 2026-05-06 12:43 CST，以下实例在跑：

```text
shared_ingress_broker
local_agg_challenger
local_agg_light_monitor
pgt_shadow_loop
```

原则：

- 不默认停止 `shared_ingress_broker`，它是跨进程共享数据平面。
- 不默认停止 `local_agg_challenger`，它在积累验收样本。
- 若需要节省流量，必须先确认用户是否允许停止。

## 6. Local Agg 当前状态

当前 run：

```text
logs/local-agg-boundary-challenger-lab/runs/20260506_004039
```

关键日志：

```text
logs/local-agg-boundary-challenger-lab/runs/20260506_004039/local_agg_lab_20260506_004039.log
```

最新 gate model：

```text
logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json
```

12:43 直接验收输出：

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

结论：dry-run gated path 当前 clean，但不是生产启用许可。

注意：raw router 未 gate 前仍有 side errors，这是预期；验收看 accepted-after-gate。

## 7. Local Agg 下一个 agent 怎么前进

第一步只做确认，不急着改代码：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi-localagg

git status --short --branch

tmux ls 2>/dev/null || true
ps -axo pid,ppid,stat,etime,%cpu,%mem,command | rg 'polymarket_v2|local_agg|shared_ingress|pgt_shadow' | rg -v 'rg '

cat /Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/automation_state.json | head -120
cat /tmp/local_agg_gate_handoff_20260506_124343.json
```

然后判断：

- run 是否仍增长。
- accepted-after-gate 是否仍为 0 side errors。
- accepted max 是否仍 < 5 bps。
- local p95 是否仍 < 300ms。
- local max 304ms 尾部是否需要专门压到 <=300ms。

硬门槛：

```text
accepted side errors = 0
accepted max error < 5 bps
local final p95 < 300ms
不 live
PM_DRY_RUN=true
PM_ORACLE_LAG_LAB_ONLY=true
```

如果出现 accepted side error：

1. 只看 `gate_status=accepted && side_error=True`。
2. 按以下字段聚类：

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

3. 如果是 near-flat、stale-source、source-spread、single-source tail，允许做窄过滤。
4. 窄过滤必须同步：

```text
scripts/evaluate_local_agg_boundary_router.py
src/bin/polymarket_v2.rs
```

5. 检查：

```bash
cargo check --bin polymarket_v2
cargo test --lib
```

需要用户决策的事项：

- 新增/删除价格源。
- 改全局权重或聚合家族。
- 放宽 5bps、0 side error、300ms 门槛。
- 明显牺牲 coverage。
- 启用 production/live trading。
- 停止或重启 `shared_ingress_broker`。

## 8. Xuan Research 下一个 agent 怎么前进

目标：把 xuanxuan008 的行为证据化，给 Frontier 提供可实现假设，而不是直接写交易逻辑。

优先任务：

1. 读取并复盘已有 xuan 文档：

```text
docs/research/xuan/XUANXUAN008_STRATEGY_DECONSTRUCTION_ZH.md
docs/research/xuan/XUANXUAN008_STRATEGY_DEEP_DIVE_ZH.md
docs/research/xuan/XUANXUAN008_STRATEGY_V2_ZH.md
docs/research/xuan/XUAN_OPEN_GATE_FAMILIES_ZH.md
docs/research/xuan/XUAN_FRONTIER_MICROSTRUCTURE_PROVISIONAL_ZH.md
```

2. 检查研究脚本和测试：

```text
scripts/analyze_xuan_long_window.py
scripts/analyze_xuan_positions.py
scripts/analyze_xuan_replay_truth.py
scripts/export_xuan_pgt_gap_report.py
scripts/infer_xuan_maker_taker.py
scripts/tests/test_infer_xuan_maker_taker.py
```

3. 把 xuan 假设拆成：已证实、弱证据、待证伪、不可用。
4. 产出 report，不要直接改 live execution。

验收产物：

- 一份 `docs/research/xuan/...` 更新或新增报告。
- 一份可复现 CSV/report 路径。
- 明确告诉 Frontier：哪些假设值得实现，哪些不能实现。

## 9. Xuan Frontier 下一个 agent 怎么前进

目标：把 Research 已确认的假设做成 shadow/prototype，优先不碰 live。

优先关注：

```text
src/polymarket/strategy/pair_gated_tranche.rs
src/polymarket/strategy/completion_first.rs
src/polymarket/coordinator_execution.rs
src/polymarket/pair_ledger.rs
scripts/run_pgt_fixed_shadow_next.sh
scripts/run_pgt_shadow_loop.sh
scripts/export_pgt_shadow_report.py
scripts/analyze_pgt_shadow_events.py
```

第一轮动作：

1. 先跑现有测试：

```bash
cargo test --lib polymarket::strategy::pair_gated_tranche
cargo test --lib polymarket::strategy::completion_first
cargo check --bin polymarket_v2
```

2. 复盘当前 PGT shadow 输出，不改参数：

```bash
bash scripts/rebuild_pgt_shadow_reports_latest.sh
```

3. 只在 Research 明确支持的假设上做小步实现。

禁止：

- 直接 live。
- 放宽风险门槛。
- 和 Local Agg agent 同时改同一段 shared-ingress 或 coordinator 核心逻辑，除非先沟通。

## 10. AWS 运行配置建议

按用途分两档。

### 10.1 只跑实盘/dry-run runtime

推荐：

```text
m7i-flex.large 或 m7i.large
2 vCPU / 8 GiB RAM
EBS gp3 200 GiB 起步
```

适合：

- shared-ingress broker
- oracle lag dry-run/live runtime
- completion-first / PGT shadow
- local agg challenger 轻量持续运行

不适合：

- 同机频繁跑全量 all-runs replay / 大 CSV 训练。

### 10.2 同机跑三方向测试 + hourly replay + 较多日志

推荐：

```text
m7i-flex.large 最低可用，但建议 m7i-flex.xlarge 或 m7i.xlarge
最低 8 GiB RAM，推荐 16 GiB RAM
EBS gp3 300 GiB
```

理由：

- 当前 all-runs boundary dataset 单次可到约 900MB+。
- Python replay/evaluator 会产生多个 `/tmp/*.csv`，内存与磁盘峰值明显高于 Rust runtime。
- `target/`、logs、reports、临时 CSV 会持续增长。

### 10.3 m7 还是 c7

建议优先 m7，不优先 c7 large。

原因：

- 这个项目不是纯 CPU 密集。它同时需要 WS、Rust runtime、日志、Python CSV replay、tmux、多策略 shadow。
- `c7i.large` 常见形态是 2 vCPU / 4 GiB，4 GiB 对 Python replay 和多进程太紧。
- `m7i/m7i-flex` 是 4:1 内存/vCPU，更适合混合负载。

如果你一定要 c7：

```text
不要选 4 GiB 的 c7i.large。
至少选 c7i.xlarge 级别，拿到 8 GiB+ RAM。
```

### 10.4 硬盘 200G 还是 300G

结论：

```text
runtime only: 200 GiB gp3 可以
三方向测试 + replay + 日志保留: 300 GiB gp3 更稳
```

如果数据采集大文件在同区另一台服务器，并通过 S3/EFS/rsync 调用，本机仍建议 300 GiB，因为本地 `/tmp`、`logs`、`target` 会吃空间。

最低运维要求：

- 开 log rotation。
- 定期清理 `/tmp/local_agg_*.csv`。
- `target/` 可按需清理但不要在编译中删除。
- EBS 用 gp3，必要时独立提高 IOPS/throughput。

## 11. 三个 agent 的协作规则

- Local Agg：价格聚合器与 gate，只改 local agg 相关路径。
- Xuan Research：只研究和产出证据，不碰 live 执行。
- Xuan Frontier：只实现 Research 已验证假设，默认 shadow。
- 三者都不要使用 `/Users/hot/web3Scientist/pm_as_ofi` 这个 dirty 旧目录。
- 每个方向提交到自己的 `codex/...` 分支。
- 合并回 `main` 前必须跑相关 tests/checks。
