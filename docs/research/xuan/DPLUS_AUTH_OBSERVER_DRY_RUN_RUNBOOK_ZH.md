# xuan_b27_dplus Auth Observer Dry-Run Runbook

更新时间：2026-05-15

## 目的

这个 runbook 只用于启动 authenticated observer dry-run。它验证我们自己的 user websocket / recorder / correlation 链路能否闭合，不验证盈利，也不允许下单、撤单、redeem 或任何 live 交易副作用。

当前状态：第一轮本地 30 分钟 no-order observer 已获主线程授权并完成，输出在
`/Users/hot/web3Scientist/pm_as_ofi-xuan-research/xuan_research_artifacts/xuan_b27_dplus_auth_observer_20260515T105826Z/`。
该轮验证了 market observer / recorder / no-order safety，但因为 `PM_DRY_RUN=true` 当前会禁用 User WS，不能视作 authenticated fill proof。
该轮还暴露出 market slug prefix 过滤问题，已在代码中修复；下一轮仍需主线程明确批准。

## 允许范围

- 策略：`xuan_b27_dplus`
- 模式：`auth_observer`
- 订单行为：禁止下单，禁止撤单，禁止 redeem
- 市场范围：单个 BTC 5m market slug
- 建议时长：30 分钟或 6 个完整 5m 市场，先到为准
- 输出目录：`/home/ubuntu/xuan_research_runs/xuan_b27_dplus_auth_observer_<timestamp>`

## 必须环境

```bash
PM_DRY_RUN=true
PM_STRATEGY=xuan_b27_dplus
PM_XUAN_B27_DPLUS_MODE=auth_observer
PM_XUAN_B27_DPLUS_MARKET_SLUG=btc-updown-5m
PM_XUAN_B27_DPLUS_EDGE=0.040
PM_XUAN_B27_DPLUS_TARGET_QTY=5
PM_XUAN_B27_DPLUS_SEED_PX_LO=0.010
PM_XUAN_B27_DPLUS_SEED_PX_HI=0.990
PM_XUAN_B27_DPLUS_IMBALANCE_QTY_CAP=2
PM_XUAN_B27_DPLUS_SALVAGE_NET_CAP=0.950
PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC=50
PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC=100
PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=0
PM_XUAN_B27_DPLUS_POST_ONLY=true
PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER=false
PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN=true
```

`MAX_LIVE_ORDERS=0` 是本 runbook 的关键约束：即使未来 controller 接好，auth observer dry-run 也必须保持 no-order。
`PM_XUAN_B27_DPLUS_MARKET_SLUG=btc-updown-5m` 代表 BTC 5m 系列前缀，代码必须匹配实际 `btc-updown-5m-<timestamp>` 市场 slug。

## 运行前门槛

全部满足才允许向用户请求授权：

1. 本地 safety smoke PASS：
   `/Users/hot/web3Scientist/pm_as_ofi-xuan-research/scripts/xuan_b27_dplus_observer_safety_smoke.sh`
2. observer payload 包含 `order_attempt_trace_preview`，并且：
   - `preview_only=true`
   - `submitted=false`
   - `venue_order_id=null`
3. recorder 已有可关联字段：
   - `order_accepted.data.correlation`
   - `order_accepted.data.order_id`
   - `order_accepted.data.venue_order_id`
   - `user_ws_fill_parsed.data.order_id`
   - `user_ws_fill_parsed.data.trade_id`
4. 本 runbook 已经存在并记录 stop conditions。

## 成功判据

auth observer dry-run 只接受以下结果：

- 产生 `xuan_b27_dplus_observer_tick` 或等价 observer event。
- 每个 candidate 有 deterministic `candidate_id` 和 `order_attempt_trace_preview.order_attempt_id`。
- 运行期间没有任何 order placement、cancel、redeem 事件。
- 若 user websocket 出现自有 fill，能写出 `user_ws_fill_parsed`，并可通过 `order_id/trade_id` 进入 UNKNOWN/PASS reconciler。
- 若没有 fill，也必须输出 no-fill / no-order 的明确 manifest，不得伪造 PASS。

第一轮本地 dry-run 结果：

- `run_manifest.json`: `TIMEOUT_TERMINATED`，持续 1800.026s，`orders_allowed=false`、`cancels_allowed=false`、`redeems_allowed=false`。
- `observer_summary.json`: 6 个 BTC 5m 市场、290917 个 observer tick、0 个 no-order violation、0 个 submitted preview、0 个 `would_place`。
- caveat: 第一轮候选全部被旧 `market_slug_filter` 阻塞；修复后需要第二轮 no-order observer 才能验证 candidate tracking。

## 第二轮验收命令

第二轮 no-order observer 仍需主线程明确授权后才能启动。运行结束后，必须在本地 artifact 目录执行以下检查，其中 `$RUN_DIR` 是该轮输出目录：

推荐使用 wrapper 启动，避免临时拼错环境变量：

```bash
scripts/xuan_b27_dplus_auth_observer_dry_run.py \
  --approved-no-order-observer \
  --duration-seconds 1800 \
  --market-slug btc-updown-5m
```

该 wrapper 不带 `--approved-no-order-observer` 会拒绝启动；启动前会加载当前 worktree 的 `.env`，再执行 `cargo build --bin polymarket_v2`，并把 build 输出、二进制 mtime/size 写入 artifact，避免源码已修但运行旧 `target/debug/polymarket_v2`。它强制写入 `PM_DRY_RUN=true`、`PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=0`、禁止 claim/redeem 相关环境，并在结束后自动运行下面的 summary gate。

公共 market WS 默认必须走已有 shared ingress broker：wrapper 设置 `PM_SHARED_INGRESS_ROLE=client`，默认 `PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main`。如果该 broker 不健康或不存在，wrapper 写出 `SHARED_INGRESS_BROKER_UNAVAILABLE` 并退出，不会自己拉起 broker，也不会回退到 standalone market WS。只有主线程额外明确授权 `--allow-standalone-market-ws` 时，才允许独立 public market WS。

wrapper 会把 `PM_MULTI_MARKET_PREFIXES` 强制设为 `--market-slug`，并用 `--require-market-prefix` 做 summary gate，防止 `.env` 中的多市场默认配置把 observer 扩到 BTC 5m 之外。

默认情况下，wrapper 仍关闭 dry-run User WS。若下一轮目标是只读验证 authenticated User WS / `user_ws_fill_parsed` 链路，必须由主线程另行明确授权并额外传入：

```bash
--approved-readonly-user-ws
```

该开关只允许在 `xuan_b27_dplus` + `auth_observer` + `PM_DRY_RUN=true` 下启用。身份来源与主程序一致：若 env 提供完整 `POLYMARKET_API_KEY` / `POLYMARKET_API_SECRET` / `POLYMARKET_API_PASSPHRASE`，则直接使用；若三项留空，则使用 `POLYMARKET_PRIVATE_KEY` 自动派生 CLOB/User WS credentials。派生出的 authenticated client 只用于读取 User WS credentials，不会传给 executor，不初始化 live order mode，不允许 order/cancel/redeem，且仍受 `MAX_LIVE_ORDERS=0` 和 summary no-order gate 约束。

如果三项 `POLYMARKET_API_*` 只填了一部分，wrapper 会在启动 binary 之前写出 `READONLY_USER_WS_CREDENTIALS_PARTIAL` manifest 并退出；如果三项全空且没有 `POLYMARKET_PRIVATE_KEY`，会写出 `READONLY_USER_WS_AUTH_SOURCE_MISSING`。若 binary 非 timeout/正常退出而提前返回非零码，wrapper 会标记 `FAIL_PROCESS_EXIT_NONZERO`，不能视为 observer pass。

### 更严格的独立 User WS observer 入口

为避免主策略二进制在 `auth_observer` 中仍构造 `Executor` / `OrderManager` / `InventoryManager`，已新增独立二进制：

```bash
cargo build --bin xuan_b27_dplus_user_ws_observer
```

这个入口只做三件事：

- 检查已有 shared-ingress broker 是否健康；不健康时写出 `SHARED_INGRESS_BROKER_UNAVAILABLE` 并退出。
- 使用完整 `POLYMARKET_API_*`，或从 `POLYMARKET_PRIVATE_KEY` 派生 read-only User WS credentials。
- 启动 `UserWsListener`，把 raw / parsed User WS 和本地 fill sink summary 写入 recorder artifact。

它不导入、不创建、不调用 `Executor`、`OrderManager`、`InventoryManager`、order/cancel/redeem/claim 路径，也没有 standalone public market WS fallback。启动仍必须有显式授权：

```bash
target/debug/xuan_b27_dplus_user_ws_observer \
  --approved-readonly-user-ws \
  --duration-secs 1800 \
  --market-slug btc-updown-5m \
  --shared-ingress-root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
  --output-dir /Users/hot/web3Scientist/pm_as_ofi-xuan-research/xuan_research_artifacts/xuan_b27_dplus_user_ws_observer_<timestamp>
```

该入口的本地无网络门禁：

```bash
scripts/xuan_b27_dplus_readonly_user_ws_static_smoke.sh
```

readiness checker 已要求该 static smoke 通过。下一次 read-only User WS 验收优先使用这个独立入口；旧 `polymarket_v2` wrapper 只保留为 no-order market observer / 兼容路径。

推荐使用独立入口 wrapper，避免手工运行旧二进制：

```bash
scripts/xuan_b27_dplus_readonly_user_ws_observer.py \
  --approved-readonly-user-ws \
  --duration-seconds 1800 \
  --market-slug btc-updown-5m \
  --shared-ingress-root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main
```

在请求 broker owner 处理前，可以先跑只读 preflight。它只读取
`broker_manifest.json` 和 manifest 中列出的 socket 路径，不启动、不停止、不连接、不修复 broker：

```bash
scripts/xuan_b27_dplus_shared_ingress_preflight.py \
  --root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main
```

若仅想写出诊断 artifact 而不让命令因 broker 缺失失败，可加：

```bash
--soft
```

preflight 的 `status=OK` 只说明 shared-ingress broker 具备启动只读 User WS observer 的本地前置条件；`status=UNAVAILABLE` 时仍不能由 xuan-research 启动或修复 broker，应由 broker owner 处理。

该 wrapper 会先 build 当前 `xuan_b27_dplus_user_ws_observer`，强制 shared-ingress client / no live orders，结束后自动运行：

```bash
scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py "$RUN_DIR" \
  --output "$RUN_DIR/readonly_user_ws_summary.json" \
  --check-readonly \
  --require-user-ws-records
```

独立 User WS summary gate 的 exit code：

- exit 0：只读约束通过，且有 User WS 记录。
- exit 2：出现 order/cancel/redeem/claim 等 forbidden event，必须停止。
- exit 3：没有任何 User WS 原始记录，不能作为 User WS 验收通过。

```bash
scripts/xuan_b27_dplus_summarize_observer_run.py "$RUN_DIR" \
  --output "$RUN_DIR/observer_summary.json" \
  --check-no-order \
  --require-tracked-candidates \
  --require-market-prefix btc-updown-5m
```

验收解释：

- exit 0：no-order 安全通过，且至少一个 observer candidate 进入 `would_track=true`。
- exit 2：出现 order/cancel/redeem、submitted preview、`would_place=true` 等安全违规，必须停止并标记 `BLOCKED_BEFORE_CANARY`。
- exit 3：no-order 可能仍安全，但没有任何 tracked candidate；不能用于候选有效性判断，需要先修配置或 market filter。
- exit 4：记录到了授权范围外的 market prefix；必须停止并修 wrapper/env scope。

当前 summarizer gate 已有本地 fixture smoke：

```bash
scripts/xuan_b27_dplus_observer_summary_gate_smoke.sh
```

该 smoke 验证 tracked/no-order、blocked/no-order、order-violation 三类 artifact 的 exit code 行为，并已被 readiness checker 纳入门槛。

## 立即停止条件

任一条件触发即停止并标记 `BLOCKED_BEFORE_CANARY`：

- 出现任何 `orders_sent_by_this_module=true`。
- 出现任何 `would_place=true`。
- 出现任何真实 order/cancel/redeem call。
- `PM_DRY_RUN` 不是 true。
- strategy 不是 `xuan_b27_dplus`。
- mode 不是 `auth_observer`。
- 任何 source-of-truth 字段 UNKNOWN 但代码尝试进入 canary/live。

## 不做的事

- 不运行 live canary。
- 不验证盈利。
- 不扫 replay/raw。
- 不修改 collector、broker、shared ingress、systemd 或 live trading。
- 不把 public-account proxy truth 当作私有 order/cancel/queue truth。
