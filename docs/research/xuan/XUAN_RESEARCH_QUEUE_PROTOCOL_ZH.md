# Xuan Frontier Research Queue Protocol

目标：把多 agent 并行推进变成 artifact 协议，而不是让多个 agent 直接 SSH、直接拼 shell、直接碰生产服务。

## 角色边界

- 主线程：策略 owner、结果集成、部署 gate。
- Codex automation：本地 quiet loop，只读本地 repo/artifacts，生成 plan 或 patch proposal。
- 远程 research runner：只在研究服务器执行白名单 job，写标准化结果。
- Subagent：只处理有边界的并行研究，输出到独立目录。

## 目录约定

远程研究服务器：

```text
/home/ubuntu/xuan_frontier_runs/
  job_specs/        # 输入 job JSON
  job_runs/         # 每个 job 的输出目录
  verifier_specs/   # verifier request/spec
  frontier_ledgers/ # 汇总 ledger
```

本地 repo：

```text
artifacts/xuan_research_jobs/ # 本地 job/spec 备份
docs/research/xuan/           # 协议和结论
scripts/xuan_research_job_runner.py
```

## Job JSON

最小结构：

```json
{
  "job_id": "dplus_minorder_verifier_request_20260514_01",
  "kind": "verifier_spec_request",
  "params": {
    "spec_path": "/home/ubuntu/xuan_frontier_runs/verifier_specs/verifier_spec_d_plus_minorder_fillhaircut_20260514_0310.json",
    "markdown_path": "/home/ubuntu/xuan_frontier_runs/verifier_specs/verifier_spec_d_plus_minorder_fillhaircut_20260514_0310.md"
  }
}
```

Runner 输出：

```text
/home/ubuntu/xuan_frontier_runs/job_runs/<job_id>/
  job.json
  status.json
  result.json
  stdout/stderr or error artifacts
```

## 白名单 kind

当前 runner 只接受：

- `inspect_artifacts`：只读汇总 `/home/ubuntu/xuan_frontier_runs` 下的 artifact。
- `verifier_spec_request`：登记 verifier 请求，复制 spec 到 job run 目录，不运行重型 verifier。
- `dplus_minorder_fillhaircut_full`：运行固定白名单脚本 `run_xuan_dplus_minorder_fillhaircut_dayshard_remote.sh`。

不接受任意 shell 命令。路径必须留在 `/home/ubuntu/xuan_frontier_runs` 下。

## 执行方式

在研究服务器上：

```bash
python3 /home/ubuntu/xuan_frontier_runs/xuan_research_job_runner.py \
  --job /home/ubuntu/xuan_frontier_runs/job_specs/<job_id>.json
```

重复执行同一个 job 会被拒绝，除非显式 `--force`。

## 推进原则

1. 多 agent 可以提交不同 job spec，但不能共用 output dir。
2. runner 只写 `job_runs/<job_id>`，不碰生产服务。
3. source-of-truth verifier 仍然单独排队；search/cache/store 结果不能直接部署。
4. 主线程只读取标准指标：PnL/stress、residual、cycles、pair cost、fill realism、verifier status。
5. D+ 当前优先级：`verifier_spec_d_plus_minorder_fillhaircut_20260514_0310` -> Rust dry-run-only profile -> local tests -> shadow。

## Automation owner 规则

自动化任务的 owner/namespace 规则见：

```text
docs/research/xuan/XUAN_AUTOMATION_OWNER_RULES_ZH.md
```

简要原则：`xuan frontier` 只拥有 `xuan-frontier-*`；其他 agent 不修改这个 namespace；`xuan-frontier-research-loop` 是本地 quiet archive loop，不执行 SSH/rsync/远程 NFS/远程 job。调用带远程 fallback 的旧脚本时必须显式设置 `REMOTE_INSPECT=0 REMOTE_DISCOVERY=0`。
