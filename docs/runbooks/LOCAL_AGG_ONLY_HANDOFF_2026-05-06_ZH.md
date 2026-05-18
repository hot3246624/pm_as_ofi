# Local Agg 单线交接 - 2026-05-06

更新时间：2026-05-06 15:30 CST

## 1. 你应该打开哪个目录

只打开这个 worktree：

```text
/Users/hot/web3Scientist/pm_as_ofi-localagg
```

对应分支：

```text
codex/localagg-work
remote: origin/codex/localagg-work
```

不要默认使用：

```text
/Users/hot/web3Scientist/pm_as_ofi
```

原因：

- 这是旧 dirty worktree。
- 它不再是这条线的安全交接目录。
- 里面还有其他改动和未跟踪脚本，容易把 local agg 任务污染掉。

## 2. 当前运行态

当前实例仍在跑：

```text
tmux session: local_agg_challenger
tmux session: local_agg_light_monitor
```

原则：

- 不默认停 `local_agg_challenger`。
- 不默认停 `shared_ingress_broker`。
- 当前任务优先读现有产物，不要先重启。

## 3. 当前样本状态

当前 run：

```text
logs/local-agg-boundary-challenger-lab/runs/20260506_004039
```

关键文件：

```text
logs/local-agg-boundary-challenger-lab/monitor_reports/automation_state.json
logs/local-agg-boundary-challenger-lab/monitor_reports/latest_recap.log
logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json
```

最新自动化快照，生成于 `2026-05-06T15:26:10+08:00`：

```text
current_run_id         20260506_004039
current eval rows      1144
accepted               728
accepted side errors   0
accepted mean          0.874298 bps
accepted max           4.49144 bps
local p50              28 ms
local p95              103 ms
local max              304 ms
action                 clean_keep_monitoring
```

这说明：

- `current run` 已经持续 clean。
- accepted-after-gate 仍满足 `0 side error`。
- accepted max 仍低于 `5 bps`。
- local p95 已明显低于 `300 ms`。

但这不等于可以上生产。

## 4. 现在真正没收敛的地方

不要再把注意力放在 `current run` 本身。当前主问题是：

```text
all-runs / unseen 历史全样本还没收敛
```

上一轮自动化里，all-runs test 仍显示：

```text
all-runs ok            10098
all-runs side          28
all-runs max           12.922542 bps
```

所以当前局面是：

- online current run：clean
- historical all-runs：not clean

这意味着 gate 和局部规则已经足够保护当前样本，但历史失败簇还没有被系统性消灭。

## 5. 下一个 agent 的唯一主线

主线不是“继续观察 current run 有没有 side error”。

主线是：

```text
聚类 all-runs / unseen 的 accepted-side-error 历史失败簇，
做窄过滤和 evaluator/runtime 同步修正，
把历史失败样本继续压缩。
```

只允许做这几类动作：

1. 聚类历史 `gate_status=accepted && side_error=True` 样本。
2. 按以下字段分桶：

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

3. 对明显 near-flat / stale / wide-spread / single-source tail 做窄过滤。
4. 保持 Python evaluator 与 Rust runtime 同步：

```text
scripts/evaluate_local_agg_boundary_router.py
src/bin/polymarket_v2.rs
```

不要做这几类事：

1. 不要改聚合家族。
2. 不要新增一堆数据源。
3. 不要放宽 `5bps / 0 side error / 300ms` 门槛。
4. 不要因为 `current run` clean 就启用 production。

## 6. 进入工作前先执行的检查

进入目录：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi-localagg
```

先确认：

```bash
git status --short --branch
tmux ls 2>/dev/null | rg 'local_agg|shared_ingress' || true
sed -n '1,220p' /Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/automation_state.json
tail -n 80 /Users/hot/web3Scientist/pm_as_ofi/logs/local-agg-boundary-challenger-lab/monitor_reports/latest_recap.log
```

然后再决定是否重跑 evaluator。

## 7. 验收门槛

当前必须坚持：

```text
accepted side errors = 0
accepted max error < 5 bps
local final p95 < 300 ms
PM_DRY_RUN=true
PM_ORACLE_LAG_LAB_ONLY=true
```

真正接近生产前，还必须满足：

```text
all-runs / unseen 历史失败簇显著收敛
不是只靠当前 run clean
```

## 8. 最短口径

给下一个 local agg agent 只需要这段：

```text
只打开 /Users/hot/web3Scientist/pm_as_ofi-localagg，分支是 codex/localagg-work。
不要碰 /Users/hot/web3Scientist/pm_as_ofi 旧 dirty worktree。

当前 local agg run 是 20260506_004039。
最新自动化时间 2026-05-06 15:26 CST：
accepted=728
accepted_side_errors=0
accepted_max=4.49144bps
local_p95=103ms
current run 继续 clean。

但主线不是继续盯 current run。
主线是 all-runs / unseen 历史失败簇收敛：
上一轮 all-runs side=28，max=12.922542bps，仍未达标。

下一步只做历史 accepted-side-error 聚类、窄过滤、Python/Rust 同步，不要改聚合家族，不要放宽门槛，不要 live。
```
