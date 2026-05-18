# AWS 多策略启动说明书

更新时间：2026-05-06

适用前提：

- 服务器代码目录已切到 `main`
- 目标是在同一台 AWS 机器上同时常驻多个策略/测试进程
- 这些进程需要复用同一套公共上游连接，而不是各自重建 WS / RTDS / local price feeds

本文档给出一套最小、可执行、可运维的启动方式。

## 1. 先说结论

如果服务器上要同时运行以下任意两条或更多：

- `oracle_lag_sniping`
- `local agg challenger`
- `pair_gated_tranche_arb` / xuan shadow

那就不要用 `PM_SHARED_INGRESS_ROLE=standalone`。

正确拓扑是：

1. 一个 `shared-ingress broker`
2. 多个 `PM_SHARED_INGRESS_ROLE=client` 的策略进程

原因：

- 一个 `polymarket_v2` 进程只能有一个 `PM_STRATEGY`
- 多策略并行本质上是多进程
- 多进程要共享公共数据平面，就必须回到 broker/client

## 2. 目录与用户约定

假设服务器目录如下：

```text
/srv/pm_as_ofi/
  repo/                    # git clone，当前在 main
  shared-ingress-main/     # broker manifest / sock
```

假设运行用户：

```text
pmofi
```

假设环境文件统一放在：

```text
/etc/pm_as_ofi/
```

## 3. 启动顺序

固定顺序：

1. 启动 `shared-ingress broker`
2. 启动 `oracle lag`
3. 启动 `local agg challenger`
4. 启动 `xuan shadow`

不要反过来。

如果 broker 没先起来，client 虽然能重试，但运维体验会变差，也更难排错。

## 4. Broker

### 4.1 环境文件

文件：

```text
/etc/pm_as_ofi/shared-ingress.env
```

内容：

```bash
RUST_LOG=info
PM_INSTANCE_ID=shared-ingress-aws
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
PM_SHARED_INGRESS_ROLE=broker
PM_SHARED_INGRESS_IDLE_EXIT_ENABLED=false
PM_DRY_RUN=true
PM_RECORDER_ENABLED=false
PM_STRATEGY=oracle_lag_sniping
PM_LOCAL_PRICE_AGG_ENABLED=true

# broker 是共享数据面，必须覆盖当前全部 5m 市场
PM_MULTI_MARKET_PREFIXES=hype-updown-5m,btc-updown-5m,eth-updown-5m,sol-updown-5m,bnb-updown-5m,doge-updown-5m,xrp-updown-5m
PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp
```

### 4.2 systemd unit

文件：

```text
/etc/systemd/system/pm-shared-ingress-broker.service
```

内容：

```ini
[Unit]
Description=pm_as_ofi shared ingress broker
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=pmofi
WorkingDirectory=/srv/pm_as_ofi/repo
EnvironmentFile=-/etc/pm_as_ofi/shared-ingress.env
ExecStart=/usr/bin/env bash /srv/pm_as_ofi/repo/scripts/run_shared_ingress_broker.sh
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

### 4.3 启动

```bash
cd /srv/pm_as_ofi/repo
cargo build --release --bin polymarket_v2
sudo systemctl daemon-reload
sudo systemctl enable --now pm-shared-ingress-broker.service
```

### 4.4 验收

```bash
sudo systemctl status pm-shared-ingress-broker.service
jq . /srv/pm_as_ofi/shared-ingress-main/broker_manifest.json
ls -l /srv/pm_as_ofi/shared-ingress-main/*.sock
```

应看到：

- broker 进程常驻
- `broker_manifest.json` 心跳刷新
- `chainlink.sock`
- `local_price.sock`
- `market.sock`

## 5. Oracle Lag

### 5.1 关键提醒

不要用：

```bash
./scripts/run_strategy_instance.sh
```

也不要用：

```bash
./scripts/run_local_agg_lab.sh
```

原因：

- 它们会把 `oracle_lag_sniping` 分流/固定到 lab 模式
- `run_local_agg_lab.sh` 会强制：
  - `PM_DRY_RUN=true`
  - `PM_ORACLE_LAG_LAB_ONLY=true`

所以服务器上的 `oracle lag` 常驻实例，应该直接跑二进制。

### 5.2 环境文件

文件：

```text
/etc/pm_as_ofi/oracle-lag.env
```

最小 dry-run 版本：

```bash
RUST_LOG=info
PM_INSTANCE_ID=oracle-lag-main
PM_STRATEGY=oracle_lag_sniping
PM_SHARED_INGRESS_ROLE=client
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
PM_INPROC_SUPERVISOR=1
PM_MULTI_MARKET_PREFIXES=btc-updown-5m
PM_ORACLE_LAG_SYMBOL_UNIVERSE=btc
PM_LOCAL_PRICE_AGG_ENABLED=true
PM_DRY_RUN=true
PM_RECORDER_ENABLED=true
```

如果要扩到多币种，再逐步改为：

```bash
PM_MULTI_MARKET_PREFIXES=hype-updown-5m,btc-updown-5m,eth-updown-5m,sol-updown-5m,bnb-updown-5m,doge-updown-5m,xrp-updown-5m
PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp
```

如果未来要切 live，只改：

```bash
PM_DRY_RUN=false
```

但切 live 前必须单独过实盘检查，不在本文直接授权。

### 5.3 systemd unit

文件：

```text
/etc/systemd/system/pm-oracle-lag.service
```

内容：

```ini
[Unit]
Description=pm_as_ofi oracle lag runtime
Wants=network-online.target pm-shared-ingress-broker.service
After=network-online.target pm-shared-ingress-broker.service

[Service]
Type=simple
User=pmofi
WorkingDirectory=/srv/pm_as_ofi/repo
EnvironmentFile=-/etc/pm_as_ofi/oracle-lag.env
ExecStart=/srv/pm_as_ofi/repo/target/release/polymarket_v2
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

### 5.4 启动

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pm-oracle-lag.service
```

### 5.5 验收

```bash
sudo systemctl status pm-oracle-lag.service
sudo journalctl -u pm-oracle-lag.service -f
```

日志中应看到：

- `shared ingress client mode`
- `shared ingress chainlink_hub remote client`
- `shared ingress local_price_hub remote client`
- 多市场时应出现 `in-proc multi-market supervisor enabled`

## 6. Local Agg Challenger

### 6.1 环境文件

文件：

```text
/etc/pm_as_ofi/local-agg.env
```

内容：

```bash
RUST_LOG=info
PM_INSTANCE_ID=local-agg-challenger
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
PM_SHARED_INGRESS_ROLE=client
PM_DRY_RUN=true
PM_RECORDER_ENABLED=false
PM_STRATEGY=oracle_lag_sniping
PM_INPROC_SUPERVISOR=1
PM_LOCAL_PRICE_AGG_ENABLED=true
PM_LOCAL_PRICE_AGG_DECISION_ENABLED=false
PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS=50
PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED=true
PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH=/srv/pm_as_ofi/repo/logs/local-agg-challenger/monitor_reports/local_agg_uncertainty_gate_model.latest.json
PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS=2500
PM_MULTI_MARKET_PREFIXES=hype-updown-5m,btc-updown-5m,eth-updown-5m,sol-updown-5m,bnb-updown-5m,doge-updown-5m,xrp-updown-5m
PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp
```

### 6.2 systemd unit

文件：

```text
/etc/systemd/system/pm-local-agg-challenger.service
```

内容：

```ini
[Unit]
Description=pm_as_ofi local aggregator challenger
Wants=network-online.target pm-shared-ingress-broker.service
After=network-online.target pm-shared-ingress-broker.service

[Service]
Type=simple
User=pmofi
WorkingDirectory=/srv/pm_as_ofi/repo
EnvironmentFile=-/etc/pm_as_ofi/local-agg.env
ExecStart=/usr/bin/env bash /srv/pm_as_ofi/repo/scripts/run_local_agg_lab.sh
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

### 6.3 启动

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pm-local-agg-challenger.service
```

### 6.4 验收

```bash
sudo systemctl status pm-local-agg-challenger.service
sudo journalctl -u pm-local-agg-challenger.service -f
```

应看到：

- `shared ingress client attach`
- compare 持续写入
- accepted side error 维持为 0

## 7. Xuan Shadow

### 7.1 环境文件

文件：

```text
/etc/pm_as_ofi/xuan-shadow.env
```

内容：

```bash
RUST_LOG=info
PM_INSTANCE_ID=xuan-shadow
PM_SHARED_INGRESS_ROLE=client
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
PM_PGT_SHADOW_PROFILE=xuan_ladder_v1
PM_PGT_SHADOW_LOOP_OVERLAP=true
PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS=45
PM_DRY_RUN=true
```

如果使用独立钱包，再把该钱包对应环境变量放在另一个仅 root/pmofi 可读的 env 文件里，并在 unit 中额外引入。

### 7.2 systemd unit

文件：

```text
/etc/systemd/system/pm-xuan-shadow.service
```

内容：

```ini
[Unit]
Description=pm_as_ofi xuan shadow
Wants=network-online.target pm-shared-ingress-broker.service
After=network-online.target pm-shared-ingress-broker.service

[Service]
Type=simple
User=pmofi
WorkingDirectory=/srv/pm_as_ofi/repo
EnvironmentFile=-/etc/pm_as_ofi/xuan-shadow.env
ExecStart=/usr/bin/env bash /srv/pm_as_ofi/repo/scripts/run_pgt_shadow_loop.sh btc-updown-5m
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

### 7.3 启动

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pm-xuan-shadow.service
```

### 7.4 验收

```bash
sudo systemctl status pm-xuan-shadow.service
sudo journalctl -u pm-xuan-shadow.service -f
```

日志中应看到：

- `shared_ingress_role=client`
- `pgt shadow overlap child start`
- fixed round worker 正常 attach，且每轮 child 在目标 round start 前预启动
- 无 connect/shutdown 风暴

补充：

- `run_pgt_shadow_loop.sh` 默认使用 `target/debug/polymarket_v2`
- 如果 debug 二进制不存在或过期，它会在 loop 启动时执行一次 `cargo build --bin polymarket_v2`
- 因此第一次启动比 broker / oracle lag 更慢是正常现象
- `PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS=45` 保留上一轮盘后 45 秒追踪；overlap loop 会同时预启动下一轮，所以不会再牺牲下一轮开盘前 45 秒。
- 不要把 systemd 直接指回 `run_pgt_fixed_shadow_next.sh`；单轮脚本会等盘后追踪退出后才由 systemd 重启，无法做到“上一轮盘后继续、下一轮按时启动”。连续运行应由 overlap loop 调度每轮 fixed worker。

## 8. 推荐的首批上线组合

初期不要全开。

推荐按以下顺序推进：

### 阶段 A

只开：

1. `pm-shared-ingress-broker`
2. `pm-local-agg-challenger`

目的：

- 验证 broker 稳定
- 验证 local agg 继续 clean
- 测流量

### 阶段 B

再加：

3. `pm-oracle-lag.service`

仍保持：

```text
PM_DRY_RUN=true
```

### 阶段 C

最后再加：

4. `pm-xuan-shadow.service`

这样最容易定位是哪个 client 把数据平面打坏。

## 9. 钱包规则

### 同一个钱包

同一个钱包下：

- 只能有一个 live executor
- 其他必须 dry-run / shadow

shared-ingress 只共享公共数据平面，不解决：

- balance/headroom 冲突
- inventory 真相冲突
- cancel/place 互踩

### 不同钱包

如果 `oracle lag` 和 `xuan shadow` 用不同钱包：

- 公共数据面仍可共享
- 两个策略都可常驻
- 但仍建议至少一条先保持 dry-run，稳定后再讨论 live

## 10. 常用命令

### 启动全部

```bash
sudo systemctl enable --now pm-shared-ingress-broker.service
sudo systemctl enable --now pm-oracle-lag.service
sudo systemctl enable --now pm-local-agg-challenger.service
sudo systemctl enable --now pm-xuan-shadow.service
```

### 停止全部

```bash
sudo systemctl stop pm-xuan-shadow.service
sudo systemctl stop pm-local-agg-challenger.service
sudo systemctl stop pm-oracle-lag.service
sudo systemctl stop pm-shared-ingress-broker.service
```

### 看状态

```bash
sudo systemctl status pm-shared-ingress-broker.service
sudo systemctl status pm-oracle-lag.service
sudo systemctl status pm-local-agg-challenger.service
sudo systemctl status pm-xuan-shadow.service
```

### 持续看日志

```bash
sudo journalctl -u pm-shared-ingress-broker.service -f
sudo journalctl -u pm-oracle-lag.service -f
sudo journalctl -u pm-local-agg-challenger.service -f
sudo journalctl -u pm-xuan-shadow.service -f
```

## 11. 最短版口径

发给同事只需要这段：

```text
服务器 main 分支上如果要同时跑 oracle lag、local agg、xuan shadow，就统一按多进程共享数据平面来启动：

1. 先起 pm-shared-ingress-broker.service
2. oracle lag 直接跑 target/release/polymarket_v2，不要用 run_strategy_instance.sh
3. local agg 用 pm-local-agg-challenger.service
4. xuan shadow 用 pm-xuan-shadow.service

所有策略进程统一：
PM_SHARED_INGRESS_ROLE=client
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main

broker 独占：
PM_SHARED_INGRESS_ROLE=broker

同一个钱包只能有一个 live executor，其余保持 dry-run/shadow。
```
