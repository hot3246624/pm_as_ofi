# Xuan Frontier Automation Owner Rules

目标：避免多个 agent 互相恢复、暂停、覆盖 automation，避免 routine loop 误触远程 SSH/NFS/runner。

## 核心分工

```text
主线程：权限、远程、副作用、最终判断
Subagent：当前任务内的并行本地工作
Automation：未来时间点或周期性的本地检查/汇报
```

不要把 subagent 或 automation 当成完整复制主线程环境的 worker。它们不应默认拥有 `ssh-agent`、`gh auth`、私有 token、后台进程、当前 shell env 或已登录状态。

主线程负责：

- `ssh`、`gh auth`、`gh pr`、`gh workflow`。
- `git fetch`、`git pull`、`git push`、创建 PR、merge。
- 访问私有 repo/registry、处理密钥/token/keychain。
- 部署、远程服务器操作、最终验收。
- 最终架构判断、实验方向裁决、是否进入 shadow/live。

Subagent 负责：

- explorer：只读调查代码入口、数据流、配置来源、测试覆盖、日志/CSV/parquet。
- worker：在明确文件边界内做本地代码修改或报告生成。
- verifier：跑本地测试、小样本复现、输出一致性检查。

Automation 负责：

- heartbeat：稍后回到当前 thread 检查长实验或日志。
- cron：周期性读取本地 workspace 输出，生成本地检查/汇报/quiet archive。

需要身份的事由主线程做；需要并行脑力的事交给 subagent；需要未来定时的事交给 automation；需要最终判断的事回到主线程。

## Owner 与命名

- `xuan frontier` 拥有 `xuan-frontier-*` automation namespace。
- 当前唯一应保持 active 的 xuan frontier automation 是 `xuan-frontier-research-loop`。
- 其他 agent 不应创建、恢复、暂停、删除或修改 `xuan-frontier-*` automation。
- 如果其他 agent 发现 `xuan-frontier-*` 状态异常，只报告给主线程或用户，不直接修。
- 其他策略线必须使用自己的前缀，例如 `local-agg-*`，不能复用 `xuan-frontier-*`。

## Automation 运行边界

`xuan-frontier-research-loop` 是本地 quiet archive loop，不是远程执行器。

禁止：

- SSH、rsync、远程 NFS、远程 EC2 job。
- GitHub/`gh` push、commit、merge、创建 PR。
- 启停服务、重启 shadow、启动 broker、修改 env/root/protocol/market universe。
- 读取或扫描 `/mnt/poly-replay`、`replay_published`、`raw`、大 SQLite replay。
- 创建 sibling xuan heartbeat/cron，或启动长时间 subagent。

允许：

- 读本地 repo、docs、scripts、artifacts、ledger。
- 运行本地 lightweight discovery/guard/test。
- 生成本地 patch proposal、data-source request、verifier request spec。
- 无 material 发现时 quiet archive。

Automation prompt 必须自包含，不能依赖当前聊天里的隐含上下文。prompt 里必须写明：

- 工作目录。
- 可读输入和禁止读取的数据源。
- 可运行命令边界。
- 输出格式和何时 quiet archive。
- 失败时只报告失败命令、错误摘要和缺少的本地输入。

所有 subagent/automation prompt 都应包含这类固定约束：

```text
你可能不在主线程的权限环境中。不要依赖 ssh、gh、git remote、私有 token、
当前 shell env、后台服务或交互登录状态。不要运行 git fetch、git push、gh、ssh。
只使用本地已有文件和依赖。如果遇到权限或环境问题，停止并说明缺少什么。
```

Subagent 任务必须满足：

- 目标明确。
- 输入在本地。
- 输出可检查。
- 文件边界清楚。
- 不依赖权限或交互登录。
- 不和其他 worker 写同一组文件。

禁止把 subagent 用作：

- “优化整个系统”这类无边界任务。
- 远程 SSH/GitHub/CI/部署执行器。
- 与另一个 worker 同时改同一文件的并发 writer。
- 主线程等待它完成后才开始工作的关键路径阻塞项。

## 远程 Artifact 与 Job

- 文档里的远程路径只作为 evidence reference。
- 需要远程 research/verifier 时，只能通过标准 job spec 交给运行面或显式授权的 runner。
- Automation 不直接执行远程脚本。
- job 输出目录必须唯一，不能多个 agent 共用。

## Remote Fallback

部分历史脚本带有 SSH/rsync fallback。automation 调用这些脚本时必须显式禁用远程路径：

```bash
REMOTE_INSPECT=0 REMOTE_DISCOVERY=0 <command>
```

如果本地缺数据，automation 只记录 local ledger/archive，不把 SSH denied 当作需要刷屏的 blocker。

## 修改前检查

任何 agent 在触碰 xuan automation 前必须先运行：

```bash
python3 scripts/check_xuan_automation_guard.py
```

预期只有：

```text
active_xuan_ids = ["xuan-frontier-research-loop"]
ok = true
```

如果 guard 失败，非 owner agent 只报告，不修复。
