# Xuan Frontier Automation Owner Rules

目标：避免多个 agent 互相恢复、暂停、覆盖 automation，避免 routine loop 误触远程 SSH/NFS/runner。

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
