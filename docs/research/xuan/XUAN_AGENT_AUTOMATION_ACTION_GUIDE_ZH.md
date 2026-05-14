# Xuan Frontier Agent Automation Action Guide

这份文档给三个 worktree 的 agent 使用。目标是让 `pm_as_ofi-xuan-frontier`、`pm_as_ofi-xuan-research`、`pm_as_ofi-localagg` 的 subagent/automation 分工稳定，避免权限环境混乱、automation 互相覆盖、routine loop 刷屏和误触远程。

完整 owner 规则见：

```text
docs/research/xuan/XUAN_AUTOMATION_OWNER_RULES_ZH.md
```

## 一句话原则

```text
需要身份的事：主线程做
需要并行脑力的事：subagent 做
需要未来定时的事：automation 做
需要最终判断的事：主线程做
```

不要把 subagent 或 automation 当成主线程副本。它们不应依赖 `ssh-agent`、`gh auth`、私有 token、后台服务、当前 shell env 或交互登录状态。

## 当前 Worktree / Namespace 分工

```text
pm_as_ofi-xuan-frontier
  cwd: /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
  owner: xuan frontier
  automation namespace: xuan-frontier-*
  current active: xuan-frontier-research-loop

pm_as_ofi-xuan-research
  cwd: /Users/hot/web3Scientist/pm_as_ofi-xuan-research
  owner: xuan research
  automation namespace: xuan-research-* 或该 agent 明确声明的 xuan-research 前缀

pm_as_ofi-localagg
  cwd: /Users/hot/web3Scientist/pm_as_ofi-localagg
  owner: localagg
  automation namespace: local-agg-*
```

每个 agent 可以在自己的 worktree/namespace 下开 automation 和 subagent。禁止的是跨 namespace 修改别人拥有的 automation。

如果发现其他 namespace 异常，只报告 automation id、状态、cwd、风险，不直接修复。

检查命令：

```bash
cd /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
python3 scripts/check_xuan_automation_guard.py
```

对 xuan frontier 的预期：

```text
ok = true
active_xuan_ids = ["xuan-frontier-research-loop"]
```

这个 guard 只确认 `xuan-frontier-*` 没被误改，不限制 `xuan-research-*` 或 `local-agg-*` 的 owner 自治。

## 主线程职责

这些事只应由主线程做，或者由用户显式授权的运行面做：

- `ssh`、`rsync`、远程 EC2 操作。
- `gh auth`、`gh pr`、`gh workflow`、私有 repo/registry。
- `git fetch`、`git pull`、`git push`、创建 PR、merge。
- 部署、重启服务、shadow/live 控制。
- 修改 broker、shared ingress、env/root/protocol/market universe。
- 最终架构判断、实验方向裁决、是否进入 shadow/live。
- 最终验收和 source-of-truth verifier 调度。

主线程如果要让 subagent 分析远程信息，应先把远程信息拉成本地文件，再把本地文件交给 subagent。

## Subagent 使用规则

Subagent 只做当前任务内的本地并行工作。

适合 explorer：

- 查代码入口、数据流、配置来源、测试覆盖。
- 分析本地日志、CSV、parquet、DuckDB 输出。
- 比较几个模块实现差异。

适合 worker：

- 在明确文件边界内新增脚本、测试、报告。
- 改一个局部函数或独立模块。
- 生成本地 artifact。

适合 verifier：

- 跑本地测试。
- 小样本复现实验结果。
- 检查输出一致性和明显回归。

Subagent 禁止：

- SSH、`gh`、`git fetch`、`git push`、部署、CI 操作。
- 写没有明确 owner 的文件。
- 和另一个 worker 同时改同一文件。
- 执行“优化整个系统”这类无边界任务。
- 执行主线程下一步必须等待的关键路径阻塞任务。

Subagent prompt 固定约束模板：

```text
你可能不在主线程的权限环境中。不要依赖 ssh、gh、git remote、私有 token、
当前 shell env、后台服务或交互登录状态。不要运行 git fetch、git push、gh、ssh。
只使用本地已有文件和依赖。如果遇到权限或环境问题，停止并说明缺少什么。
```

Explorer prompt 模板：

```text
Workspace: /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
只读分析，不修改文件。
目标：<明确问题>
输入：<本地文件/目录>
禁止：ssh、gh、git fetch、git push、远程 NFS、部署、automation 修改。
输出：关键文件路径、关键函数、最小改动点、剩余风险。
```

Worker prompt 模板：

```text
Workspace: /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
你负责实现：<明确任务>
写入范围只限：
- <file A>
- <file B>
不要修改其他文件。不要运行 ssh、gh、git fetch、git push。
完成后列出改动文件、运行过的本地测试、剩余风险。
```

Verifier prompt 模板：

```text
Workspace: /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
只做本地验证，不修改文件。
验证命令：
- <command 1>
- <command 2>
如果失败，报告失败命令、错误摘要、可能原因和最小复现线索。
不要运行 ssh、gh、git fetch、git push。
```

## Automation 使用规则

Automation 是时间维度工具，不是并行开发 worker。

Heartbeat 适合：

- 30 到 60 分钟后检查长实验是否跑完。
- 稍后回到当前 thread 汇总最新本地日志。
- 明天继续当前 thread 的上下文。

Cron 适合：

- 每小时读取本地 artifact 做 quiet archive。
- 每天生成本地误差/策略日报。
- 周期性检查本地数据质量。

Automation 禁止：

- SSH、rsync、远程 NFS、远程 EC2 job。
- `gh`、git network、commit/push/merge/PR。
- 启停服务、部署、重启 shadow、启动 broker。
- 扫 `/mnt/poly-replay`、`replay_published`、`raw`、大 SQLite replay。
- 创建 sibling `xuan-frontier-*` automation 或修改其他 owner 的 automation。
- 启动长时间 subagent。

Automation prompt 必须自包含，写明：

- workspace。
- 可读输入。
- 禁止读取/禁止执行项。
- 可运行命令边界。
- 输出格式。
- 何时 quiet archive。
- 失败时如何汇报。

如果调用旧脚本，必须显式禁远程 fallback：

```bash
REMOTE_INSPECT=0 REMOTE_DISCOVERY=0 <command>
```

Routine loop 没有 material 发现时，最终输出必须 quiet archive：

```text
::archive{reason="routine xuan frontier checkpoint"}
```

## xuan-frontier-research-loop 标准行为

这个 cron 只做本地 planning、artifact review、quiet archive。

它可以：

- 读本地 repo、docs、scripts、artifacts、ledger。
- 运行本地 lightweight discovery/guard/test。
- 生成 patch proposal、data-source request、verifier request spec。

它不可以：

- 运行远程命令。
- 修改 production 服务。
- commit/push。
- 自己创建新 automation。
- 在缺少远程数据时刷屏报错。

如果缺数据：

```text
写一条 local ledger/archive，说明缺少哪个本地输入；不要尝试 SSH/rsync。
```

如果发现 automation 冲突：

```text
报告冲突 automation id、状态、cwd、风险；不要直接暂停/恢复/删除。
```

## xuan 当前策略上下文

当前主线：

```text
b27/D+ two-sided passive BUY from public SELL flow
```

当前代码状态：

```text
origin/codex/xuan-frontier
45572c16 Add D+ minorder dry-run profile
5fdf8a41 Add xuan automation rules and per-side public trades
```

当前 D+ profile：

```text
PM_PGT_SHADOW_PROFILE=dplus_minorder_v1
dry-run-only
not deployed
not live-enabled
```

当前强候选参考指标来自 interactive/remote work：

```text
2026-05-02..2026-05-12
t10/imb8/sv0950 fill_haircut=0.20
net_pair_cost=0.922813
cycles=11.3976/market
qty_residual=3.7823%
PnL=+4604.17
ROI=6.9649%
stress100_worst=+2242.28
```

这些结果是研究候选，不是直接部署凭据。最终上线仍需要 verifier/shadow 证据。

## 其他 agent 的标准操作顺序

1. 先读本指南和 owner rules。
2. 运行 guard：

```bash
python3 scripts/check_xuan_automation_guard.py
```

3. 明确自己角色：

```text
主线程 / explorer / worker / verifier / automation
```

4. 如果不是主线程，不运行权限命令：

```text
ssh / gh / git fetch / git push / rsync / deploy
```

5. 如果要写文件，先声明写入范围。
6. 如果要创建或修改 automation，先确认 namespace owner；只改自己 namespace，不是 owner 就报告，不修改。
7. 输出必须可验收：文件路径、命令、结果、剩余风险。

## 交接给 xuan-research 的短版

可以直接复制这一段：

```text
你是 pm_as_ofi-xuan-research worktree 的 owner。请使用自己的 cwd：
/Users/hot/web3Scientist/pm_as_ofi-xuan-research

你的 automation namespace 应使用 xuan-research-* 或你明确声明的 xuan-research 前缀。
你可以维护自己的 automation/subagent，但不要创建/恢复/暂停/删除/修改 xuan-frontier-* 或 local-agg-*。

不要运行 ssh、gh、git fetch、git push、rsync、远程 NFS、部署或服务控制。
如果你的任务确实需要远程/权限/部署，把需求交给主线程，不要在 automation/subagent 里做。

如需检查 xuan-frontier namespace 是否被误改，只读运行：
cd /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
python3 scripts/check_xuan_automation_guard.py

若调用旧脚本，必须禁远程 fallback：
REMOTE_INSPECT=0 REMOTE_DISCOVERY=0 <command>

只使用本地已有文件和依赖。缺数据就说明缺少什么，不要尝试远程获取。
```

## 交接给 localagg 的短版

可以直接复制这一段：

```text
你是 pm_as_ofi-localagg worktree 的 owner。请使用自己的 cwd：
/Users/hot/web3Scientist/pm_as_ofi-localagg

你的 automation namespace 是 local-agg-*。
你可以维护自己的 automation/subagent，但不要创建/恢复/暂停/删除/修改 xuan-frontier-* 或 xuan-research-*。

不要运行 xuan strategy shadow/live 控制，不要修改 xuan-frontier 或 xuan-research worktree。
不要触碰 shared ingress broker，除非主线程/用户明确授权。

如果你的 automation 需要远程/权限/部署，必须在 prompt 里写清边界和安全 gate；涉及跨策略线的修改只报告，不直接执行。

如需检查 xuan-frontier namespace 是否被误改，只读运行：
cd /Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
python3 scripts/check_xuan_automation_guard.py

只使用自己 worktree 的本地文件和依赖。缺数据就说明缺少什么，不要跨 worktree 写文件。
```
