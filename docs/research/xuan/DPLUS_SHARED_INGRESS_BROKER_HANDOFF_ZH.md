# xuan_b27_dplus Shared-Ingress Broker Handoff

本文档只描述 `xuan-research-*` 进入 read-only User WS observer 前，对 shared-ingress broker 的外部依赖。它不是 broker 修复 runbook，也不是启动指令。

## 当前状态

`xuan_b27_dplus` 本地门禁已经到 `READY_FOR_APPROVAL`：

- 独立 read-only User WS observer 已实现，不构造 `Executor`、`OrderManager`、`InventoryManager`。
- auth-source preflight 通过：`.env` 软链可用，当前通过 `POLYMARKET_PRIVATE_KEY` 派生 User WS credentials。
- status bundle verdict 为 `READY_FOR_APPROVAL_WAITING_FOR_SHARED_INGRESS_BROKER`。

当前阻塞点是 shared-ingress broker 本地 manifest 缺失：

```text
/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main/broker_manifest.json
```

最新只读 preflight 记录：

```text
xuan_research_artifacts/xuan_b27_dplus_shared_ingress_preflight_20260516T074437Z/manifest.json
status=UNAVAILABLE
reason=broker_manifest_missing
```

## xuan-research 不会做的事

`xuan-research-*` 不会：

- 启动、停止、重启或修复 shared-ingress broker。
- 修改 `/Users/hot/web3Scientist/pm_as_ofi` 的 broker、shared-ingress、systemd、部署或 live trading 配置。
- 回退到 standalone public market WS 来绕过 broker。
- 在 broker 不健康时启动 read-only User WS observer。

## broker owner 需要提供的条件

read-only User WS observer 只要求已有 broker 满足这些本地条件：

- `broker_manifest.json` 存在。
- `protocol_version=1`。
- `schema_version=1`。
- `last_heartbeat_ms` 新鲜，默认 heartbeat age 不超过 `10000ms`。
- manifest 中的 `chainlink_socket`、`local_price_socket`、`market_socket` 都存在且是 socket。

这些条件只代表 xuan-research 可以作为 shared-ingress client 启动 observer；不代表任何订单路径被授权。

## xuan-research 验收命令

只读检查命令：

```bash
scripts/xuan_b27_dplus_shared_ingress_preflight.py \
  --root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main
```

若只需要生成诊断 artifact，不希望命令因 broker 不健康返回非零：

```bash
scripts/xuan_b27_dplus_shared_ingress_preflight.py \
  --root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
  --soft
```

通过条件：

```text
status=OK
reason=broker_manifest_and_sockets_ok
side_effects.started_broker=false
side_effects.stopped_broker=false
side_effects.connected_to_broker=false
side_effects.modified_shared_ingress=false
```

## 下一授权点

只有在 shared-ingress preflight 为 `OK` 后，才可以回到主线程请求下一次明确授权：

```text
启动 xuan_b27_dplus read-only User WS observer：
PM_DRY_RUN=true
PM_SHARED_INGRESS_ROLE=client
30 分钟
BTC 5m
不下单、不撤单、不 redeem
输出到 xuan_research_artifacts/...
```

即使 broker 已健康，heartbeat/automation 也不能自行启动该 observer；必须由主线程明确授权该 exact run。
