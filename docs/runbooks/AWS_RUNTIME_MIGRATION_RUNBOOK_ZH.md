# AWS 运行迁移 Runbook

本文档用于把本地 local price aggregator / oracle-lag dry-run / shadow 运行迁移到 AWS Ireland (`eu-west-1`)。目标不是把所有开发、训练、回放都塞进一台小机器，而是把**长久在线运行的数据平面和轻量 challenger**搬到云上，避免本机流量、睡眠和网络波动影响样本。

## 1. 推荐硬件

### 长久在线运行

- 实例：`m7i-flex.large`
- CPU / 内存：2 vCPU / 8 GiB
- 磁盘：300 GiB gp3 起步
- 系统：Ubuntu 24.04 LTS
- 区域：Ireland `eu-west-1`

适合：

- 1 个 shared-ingress broker
- 1 个 local-agg challenger
- 少量 shadow worker
- systemd 常驻与日志轮转

不适合：

- 多分支同时 `cargo build --release`
- 大规模 replay / walk-forward / 模型搜索
- 多币种全量 market WS 长期展开

### 并行开发 / 训练 / 回放

- 最低：`m7i-flex.xlarge`，4 vCPU / 16 GiB
- 更稳：`m7i-flex.2xlarge`，8 vCPU / 32 GiB
- 磁盘：500 GiB gp3 起步

建议把生产候选长跑和重型训练分开。`m7i-flex.large` 可以做实盘候选运行环境，不应该承担所有研究工作。

## 2. 系统布局

建议统一使用：

```bash
/srv/pm_as_ofi/
  repo/                    # 主 repo
  worktrees/               # 其他分支 worktree
  shared-ingress-main/     # broker socket / manifest
  logs/                    # 可选外部日志根
  data/                    # 可选数据根
```

创建运行用户：

```bash
sudo useradd --system --create-home --shell /bin/bash pmofi
sudo mkdir -p /srv/pm_as_ofi
sudo chown -R pmofi:pmofi /srv/pm_as_ofi
```

基础依赖：

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev clang cmake git curl jq python3 python3-venv
```

Rust：

```bash
curl https://sh.rustup.rs -sSf | sh
```

## 3. 部署 shared-ingress broker

shared-ingress broker 是服务器上的公共行情数据平面。它只持有公共 WS / RTDS / local price feeds，不持有钱包私钥。

安装 systemd unit：

```bash
sudo mkdir -p /etc/pm_as_ofi
sudo cp deploy/systemd/shared-ingress.env.example /etc/pm_as_ofi/shared-ingress.env
sudo cp deploy/systemd/pm-shared-ingress-broker.service /etc/systemd/system/
sudo systemctl daemon-reload
```

编辑 `/etc/pm_as_ofi/shared-ingress.env`：

- `PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main`
- `PM_SHARED_INGRESS_ROLE=broker`
- `PM_SHARED_INGRESS_IDLE_EXIT_ENABLED=false`
- 初期只保留 `PM_MULTI_MARKET_PREFIXES=btc-updown-5m`
- 初期只保留 `PM_ORACLE_LAG_SYMBOL_UNIVERSE=btc`

编译并启动：

```bash
cargo build --release --bin polymarket_v2
sudo systemctl enable --now pm-shared-ingress-broker.service
sudo systemctl status pm-shared-ingress-broker.service
```

健康检查：

```bash
jq . /srv/pm_as_ofi/shared-ingress-main/broker_manifest.json
ls -l /srv/pm_as_ofi/shared-ingress-main/*.sock
```

验收：

- manifest 中 `last_heartbeat_ms` 持续刷新
- `chainlink.sock`、`local_price.sock`、`market.sock` 存在
- 没有重复 broker 进程

## 4. 部署 local-agg challenger

challenger 应当作为 shared-ingress client 运行。这样 broker 出问题时 challenger 重启，不会偷偷新建一套公网 WS。

安装 unit：

```bash
sudo cp deploy/systemd/local-agg.env.example /etc/pm_as_ofi/local-agg.env
sudo cp deploy/systemd/pm-local-agg-challenger.service /etc/systemd/system/
sudo systemctl daemon-reload
```

编辑 `/etc/pm_as_ofi/local-agg.env`：

- `PM_SHARED_INGRESS_ROLE=client`
- `PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main`
- `PM_LOCAL_PRICE_AGG_DECISION_ENABLED=false`
- `PM_DRY_RUN=true`
- `PM_RECORDER_ENABLED=false`

启动：

```bash
sudo systemctl enable --now pm-local-agg-challenger.service
sudo journalctl -u pm-local-agg-challenger.service -f
```

验收：

- 日志出现 shared-ingress client attach
- local-agg compare 持续写入
- accepted side error 仍为 0
- local final ready p95 维持在 500ms 内，目标 300ms 内

## 5. 多 worktree 开发

不同分支并行开发必须使用 git worktree，不要让多个 Codex 线程共用同一个 `.git` 工作区反复切分支。

```bash
cd /srv/pm_as_ofi/repo
git fetch --all --prune
git worktree add /srv/pm_as_ofi/worktrees/localagg codex/self-built-price-aggregator
git worktree add /srv/pm_as_ofi/worktrees/pgt codex/completion-first-v2-shadow
```

所有 worktree 如果要复用公共数据平面，都指向同一个：

```bash
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
```

研究 / shadow 进程默认使用：

```bash
PM_SHARED_INGRESS_ROLE=client
```

只有明确要自己开公共 WS 时才用：

```bash
PM_SHARED_INGRESS_ROLE=standalone
```

## 6. 钱包与密钥

shared-ingress broker 不需要任何钱包密钥。

不同钱包运行多个实例时：

- 公共数据可共享
- 钱包密钥留在各自策略进程
- user WS 和执行 authority 仍归属各自实例

同一个钱包不要多进程同时实盘。当前共享数据平面不能解决同钱包下的 balance/headroom/inventory 真相冲突。

密钥不要写进 repo。建议放在：

```bash
/etc/pm_as_ofi/live-wallet-a.env
/etc/pm_as_ofi/live-wallet-b.env
```

并设置：

```bash
sudo chmod 600 /etc/pm_as_ofi/*.env
```

## 7. 流量控制

迁移初期必须 BTC-only：

```bash
PM_MULTI_MARKET_PREFIXES=btc-updown-5m
PM_ORACLE_LAG_SYMBOL_UNIVERSE=btc
```

确认带宽后再逐步加币种。每次新增币种后至少观察：

- active WS 数
- `ifstat` / cloudwatch network in
- accepted coverage
- side error
- local ready latency

如果 broker 流量重新接近本地 40GB/day 级别，先回退到 BTC-only，而不是增加实例。

## 8. 生产启用门槛

local aggregator 进入生产集成前，至少需要：

- 连续 dry-run clean：accepted side error = 0
- accepted max error < 5bps
- local final ready p95 <= 500ms，目标 <= 300ms
- broker 无 connect/shutdown 风暴
- shared-ingress client 无长时间空流
- 同一钱包只有一个 live executor

未满足这些条件前，只能 shadow / dry-run，不进入实盘下单路径。
