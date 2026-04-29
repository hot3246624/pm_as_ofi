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

- `auto`
  - 推荐模式
  - 启动时先检查共享 ingress broker 是否健康、是否兼容
  - 若不存在健康 broker，则当前实例自动拉起 sidecar broker
  - 若已存在健康 broker，则当前实例直接作为 client 接入
- `standalone`
  - 兼容模式
  - 当前进程自己建立全部公共连接
- `broker`
  - 只负责持有公共上游连接并向本机 client 广播标准化事件
- `client`
  - 不再自行建立公共行情连接
  - 通过 Unix socket 向 broker 订阅数据

### 当前推荐

日常多 agent / 多本地进程开发测试，直接用：

- `PM_SHARED_INGRESS_ROLE=auto`

不需要提前决定“谁是第一个、谁是第 n 个”。

`auto` 的实际语义是：

1. 先尝试连接现有 broker
2. 若 broker 不存在、失活、或版本不兼容
3. 通过单机锁选出一个 sidecar broker
4. 其他实例全部作为 client 接入

当前刻意**不做**运行中 client 自动晋升 broker，避免 split-brain。

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

### 方案 A：推荐，所有实例都用 `auto`

这是现在的首选。

所有 agent / 进程只要：

- 使用不同的 `PM_INSTANCE_ID`
- 共享同一个 `PM_SHARED_INGRESS_ROOT`
- 统一 `PM_SHARED_INGRESS_ROLE=auto`

即可。

示例（`oracle_lag_sniping` / 本地聚合器实验线）：

```bash
PM_INSTANCE_ID=local-agg-client-a \
PM_SHARED_INGRESS_ROLE=auto \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
./scripts/run_local_agg_lab.sh
```

第二个实例只改 `PM_INSTANCE_ID`：

```bash
PM_INSTANCE_ID=local-agg-client-b \
PM_SHARED_INGRESS_ROLE=auto \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
./scripts/run_local_agg_lab.sh
```

`auto` 运行特性：

- 首个实例会自动拉起 broker sidecar
- 后续实例直接复用，不再重复建公网连接
- 最后一个 client 退出后，broker 在 idle grace 后自动退出
- broker 兼容性只按 shared-ingress ABI 判定：
  - `protocol_version`
  - `schema_version`
- `build_id` 只保留为观测字段，不再作为默认硬阻断
- 因此，不同 PR / 不同 build 只要没有改 shared-ingress 协议或广播 schema，仍然可以复用同一个 broker
- 只有当你修改了：
  - control 握手
  - wire message
  - chainlink / local_price / market 广播结构
  才需要 bump `protocol_version` 或 `schema_version`，并滚动 broker

### 方案 B：一个手工 broker + 多个 client

这是高级/手工控制方案。

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

### 方案 C：单独进程独跑

如果某个 agent 不想共用 broker，就保持：

```bash
PM_SHARED_INGRESS_ROLE=standalone
```

或直接不设该变量。

## 5. “我是第一个还是第 n 个 agent”有区别吗？

如果你使用 `auto`：

- 没区别
- 不需要自己判断
- 只需要：
  - 唯一的 `PM_INSTANCE_ID`
  - 相同的 `PM_SHARED_INGRESS_ROOT`

如果你使用手工 `broker/client`：

- 才有区别
- 第一个需要你显式起成 `broker`
- 后续显式起成 `client`

所以，当前推荐路径下，其他 agent 不需要猜“自己是不是第一个”。

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
- `auto` 模式下的单机选主
- client 自动接入现有 broker
- broker 无 client 后自动退出
- shared-ingress ABI 不兼容时触发 broker 代际切换

## 8. 当前仍未解决的问题

以下问题不属于共享数据平面的范围：

- user WS 跨进程共享
- 同钱包多执行冲突
- 统一 executor / single execution authority

如果以后需要同钱包多策略并行，再进入下一阶段设计。

## 9. 推荐运行纪律

1. 默认优先用 `PM_SHARED_INGRESS_ROLE=auto`
2. 不同 client 使用不同 `PM_INSTANCE_ID`
3. 不同 client 共用同一个 `PM_SHARED_INGRESS_ROOT`
4. broker 不持有私钥
5. 同钱包不要并行 live
6. client 若只是研究/回测/对照，继续 `PM_DRY_RUN=true`
7. 不同 PR 只要**没有改 shared-ingress ABI**，可以共用同一个 `PM_SHARED_INGRESS_ROOT`

## 10. 策略入口

不要再把 [run_local_agg_lab.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_local_agg_lab.sh) 当成所有策略的统一入口。

当前脚本分工：

- [run_local_agg_lab.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_local_agg_lab.sh)
  - **只用于** `PM_STRATEGY=oracle_lag_sniping`
  - 内部硬编码：
    - `PM_STRATEGY=oracle_lag_sniping`
    - `PM_ORACLE_LAG_LAB_ONLY=true`
    - `PM_LOCAL_PRICE_AGG_ENABLED=true`
    - `PM_LOCAL_PRICE_AGG_DECISION_ENABLED=false`
- [run_pgt_fixed_shadow_next.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_pgt_fixed_shadow_next.sh)
  - **只用于** `PM_STRATEGY=pair_gated_tranche_arb`
  - 内部会解析下一轮固定 market，并以 PGT shadow 方式启动
- [run_strategy_instance.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_strategy_instance.sh)
  - 通用包装入口
  - 按 `PM_STRATEGY` 自动分流到对应脚本

推荐：

```bash
PM_INSTANCE_ID=agent-a \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
PM_STRATEGY=oracle_lag_sniping \
./scripts/run_strategy_instance.sh
```

PGT / xuan 研究线：

```bash
PM_INSTANCE_ID=xuanxuan008_research \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
PM_STRATEGY=pair_gated_tranche_arb \
./scripts/run_strategy_instance.sh btc-updown-5m
```

说明：

- `run_strategy_instance.sh` 只统一**入口分流**，不覆盖各自策略脚本内部的专属参数。
- 如果你已经明确知道自己要跑哪条策略，也可以直接调用对应的策略专用脚本。

## 11. 相关脚本

- broker：
  - [run_shared_ingress_broker.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_shared_ingress_broker.sh)
- 本地聚合器 lab：
  - [run_local_agg_lab.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_local_agg_lab.sh)
- 通用策略入口：
  - [run_strategy_instance.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_strategy_instance.sh)
- 停止实例：
  - [stop_markets.sh](/Users/hot/web3Scientist/pm_as_ofi/stop_markets.sh)
