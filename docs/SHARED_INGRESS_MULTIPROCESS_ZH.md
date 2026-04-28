# 共享数据平面：多进程运行说明

本文档描述 `PM_SHARED_INGRESS_ROLE` 的运行方式，以及多个 agent / 多个独立进程如何安全共用同一套公共行情连接。

## 1. 目标

当前已经支持把以下**公共数据平面**跨进程共享：

- market WS
- Chainlink RTDS
- Local Price feeds

共享的目标是：

- 避免多个进程重复建立相同的公网连接
- 降低 WS 限流、带宽、CPU、日志噪音
- 让多个独立 agent / 多个 shadow / 多个不同钱包实例共用同一份公共行情数据

当前**不**在共享平面里处理：

- 钱包私钥
- 下单执行 authority
- 用户仓位/余额真相
- user WS

这些仍然留在各自进程本地。

## 2. 角色

通过 `PM_SHARED_INGRESS_ROLE` 明确指定角色：

- `standalone`
  - 默认模式
  - 当前进程自己建立全部公共连接
- `broker`
  - 只负责持有公共上游连接并向本机 client 广播标准化事件
- `client`
  - 不再自行建立公共行情连接
  - 通过 Unix socket 向 broker 订阅数据

### 重要约束

当前**没有自动选主**。

也就是说：

- 你必须显式决定谁是 `broker`
- 其他进程显式设成 `client`

不要期待“第一个启动的 agent 自动变 broker，后面的自动变 client”。  
这类自动选主在当前阶段会引入 split-brain 和误判，得不偿失。

## 3. 共享目录

broker 与所有 client 必须共享同一个：

- `PM_SHARED_INGRESS_ROOT`

该目录下当前会创建：

- `chainlink.sock`
- `local_price.sock`
- `market.sock`

建议路径示例：

```bash
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main
```

## 4. 启动顺序

### 方案 A：一个 broker + 多个 client

这是当前推荐方案。

1. 先启动 broker
2. 再启动任意数量的 client

broker 示例：

```bash
PM_INSTANCE_ID=shared-ingress-broker \
PM_SHARED_INGRESS_ROLE=broker \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
./scripts/run_shared_ingress_broker.sh
```

client 示例：

```bash
PM_INSTANCE_ID=local-agg-client-a \
PM_SHARED_INGRESS_ROLE=client \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
./scripts/run_local_agg_lab.sh
```

第二个 client 只需要换 `PM_INSTANCE_ID`：

```bash
PM_INSTANCE_ID=local-agg-client-b \
PM_SHARED_INGRESS_ROLE=client \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
./scripts/run_local_agg_lab.sh
```

### 方案 B：单独进程独跑

如果某个 agent 不想共用 broker，就保持：

```bash
PM_SHARED_INGRESS_ROLE=standalone
```

或直接不设该变量。

## 5. “我是第一个还是第 n 个 agent”有区别吗？

有区别，但区别不是自动判断，而是**角色不同**：

- 第一个应该由你**明确指定为 broker**
- 第 n 个应明确指定为 `client`

所以，其他 agent 不需要“猜自己是不是第一个”。

它只需要知道两件事：

1. 当前有没有现成 broker
2. 自己应该用哪组环境变量

推荐约定：

- 只保留一个固定 broker 名称，例如：
  - `PM_INSTANCE_ID=shared-ingress-broker`
- 其他所有 agent 一律：
  - 各自唯一 `PM_INSTANCE_ID`
  - `PM_SHARED_INGRESS_ROLE=client`
  - 同一个 `PM_SHARED_INGRESS_ROOT`

## 6. 钱包怎么处理

### 不同钱包

最简单，也最安全：

- 每个进程保留自己的钱包环境变量
- broker 不持有任何私钥
- 所有进程只共享公共数据

这是当前推荐用法。

### 相同钱包

当前**不推荐**。

原因：

- 共享数据平面只能避免重复行情连接
- 不能避免：
  - 双重下单
  - 重复撤单
  - headroom/balance 竞争
  - inventory 真相不一致

如果必须多个进程共用同一个钱包，必须再做：

- 单执行 authority

当前尚未实现。

## 7. 已解决的问题

当前版本已经解决：

- 多进程重复建立 RTDS / local price / market WS
- 同实例日志目录互相覆盖
- 同一实例重复启动导致的 compare 样本污染

## 8. 当前仍未解决的问题

以下问题不属于共享数据平面的范围：

- user WS 跨进程共享
- 同钱包多执行冲突
- 统一 executor / single execution authority

如果以后需要同钱包多策略并行，再进入下一阶段设计。

## 9. 推荐运行纪律

1. broker 始终单独跑
2. broker 不持有私钥
3. 不同 client 使用不同 `PM_INSTANCE_ID`
4. 不同 client 共用同一个 `PM_SHARED_INGRESS_ROOT`
5. 同钱包不要并行 live
6. client 若只是研究/回测/对照，继续 `PM_DRY_RUN=true`

## 10. 相关脚本

- broker：
  - [run_shared_ingress_broker.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_shared_ingress_broker.sh)
- 本地聚合器 lab：
  - [run_local_agg_lab.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_local_agg_lab.sh)
- 停止实例：
  - [stop_markets.sh](/Users/hot/web3Scientist/pm_as_ofi/stop_markets.sh)
