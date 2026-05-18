# D+ Canary Design

更新时间：2026-05-15

本文只定义 D+ 从研究候选进入 observer / canary 的设计边界。它不是上线许可，不包含任何 live 启动命令。

## 1. 当前状态

D+ 已通过 clean-room 研究验证，但还没有变成独立 Rust 生产策略。

已完成：

- Python 离线模型：`tools/xuan_d_branch_passive_passive_redeem.py`
- Python read-only shadow：`tools/xuan_dplus_passive_passive_shadow_runner.py`
- clean-room full window：`/home/ubuntu/xuan_research_runs/xuan_research_cleanroom_dplus_full_20260514_0830`
- G1 verifier：`/home/ubuntu/xuan_research_runs/xuan_research_g1_verifier_20260514_0925`
- 最新 G1b/G1c：`/home/ubuntu/xuan_research_runs/xuan_research_g1b_g1c_reconcile_20260515_030405`

最新阻塞：

- authenticated order/fill/queue truth：UNKNOWN
- wallet/redeem/cashflow truth：UNKNOWN
- production readiness：NOT_ESTABLISHED

解释：现有 P2/cache/public 数据足够支撑强研究候选，不足以证明真实 maker queue / fill / cashflow。

## 2. 策略核心

D+ 是 B27/RWO 启发的双边 passive inventory 策略：

- 单 market：BTC 5m
- 常态动作：YES/NO 双边 passive BUY
- 订单语义：post-only maker-only，禁止意外 taker
- 内部账本：按 fill 构建 YES/NO pair ledger
- 盈亏来源：低 pair cost + winner-side redeem/settlement
- 修复动作：仅在残差恶化时允许小额 FAK salvage

不复刻 B27 的私有执行，也不依赖 B27/xuan/RWO 私有数据。生产验证只能来自我们自己的订单、fill、钱包和 redeem 记录。

## 3. Rust 化范围

建议新增独立策略，而不是塞进现有 PGT/C 分支：

- 新策略名：`xuan_b27_dplus`
- 新模块：`src/polymarket/strategy/xuan_b27_dplus.rs`
- 新 `StrategyKind`：`XuanB27Dplus`
- 执行模式：优先复用 `UnifiedBuys`
- 输出槽位：常态只输出 `YES_BUY` / `NO_BUY`
- salvage：单独走受限 FAK repair controller，不能混入常态 passive quote

策略层只表达意图，不直接访问 OMS / Executor / wallet / network。订单生命周期、post-only、reprice、cancel、recorder、cashflow 对账仍走共享层。

## 4. 必需 recorder 字段

canary 前必须确保每一笔事件可以审计：

- strategy candidate id
- market slug / condition id / token id
- side：YES / NO
- order direction：BUY / SELL
- intended price / size
- order type：GTC/GTD/FAK
- post_only flag
- client order id / venue order id
- submit timestamp / ack timestamp
- ack status：live / matched / delayed / rejected / cancelled
- reject reason
- cancel / replace lifecycle
- fill id / trade id
- fill price / size / fee / maker-taker role / timestamp
- pair ledger action id
- salvage action id
- residual lot id
- wallet balance snapshot
- open-order reserved balance
- redeem tx hash / status / amount
- realized USDC balance delta

没有这些字段时，不允许把 canary 结果解释成生产验证。

## 5. 阶段计划

### G1a: Rust observer

目标：不下单，只让 Rust runtime 生成 D+ candidate 和 recorder 事件。

约束：

- `PM_DRY_RUN=true`
- 不进入 Executor 下单路径
- 单 market
- 不开 salvage live path
- 输出 candidate、book snapshot、ledger projection、would_place/would_cancel/would_salvage

通过标准：

- 连续 30 个 BTC 5m round 无 panic / stale / duplicate loop
- candidate 数量、price bucket、side balance 与 Python shadow 同量级
- recorder 可以完整重建 would-be pair ledger
- 无 UNKNOWN recorder 字段

### G1b: authenticated dry-run observer

目标：接入账户只读数据，仍不下单。

约束：

- `PM_DRY_RUN=true`
- 可读 open orders / fills / balances 时只做 reconciliation
- 禁止 create / cancel / post order

通过标准：

- 能关联 account id / wallet / allowance / balance
- 能记录真实 account state snapshot
- 能证明 recorder 字段覆盖未来 canary 所需数据

### G2: $50-$100 smoke canary

目标：验证真实 post-only maker lifecycle，不追求收益。

初始硬限制：

- prefund cap：`100 USDC`
- max strategy exposure：`100 USDC`
- max open cost：`40-60 USDC`
- max active market：`1`
- max live orders：`2`
- max order size：`5 shares`
- max imbalance qty：`2`
- max daily realized loss：`5 USDC`
- max unresolved residual cost：`5 USDC`
- max runtime：`10-20 BTC 5m rounds`
- any UNKNOWN recorder/cashflow state：stop
- any unexpected taker fill from passive leg：stop
- any cancel/reject storm：stop
- any wallet/balance mismatch：stop

Order policy：

- passive leg：post-only GTC or GTD only
- post-only reject is acceptable and should be recorded, not retried aggressively
- if local TTL is used, cancel must be confirmed before opening replacement exposure
- salvage leg：disabled in the first smoke pass unless residual exceeds hard threshold and manual approval is granted

### G3: $200-$500 initial canary

目标：收集足够样本估计 fill rate、pair share、residual、cashflow。

进入条件：

- G2 全部通过
- no unexpected taker passive fills
- no unmanaged open orders
- no wallet/cashflow mismatch
- no material residual tail

通过标准：

- at least 50-100 candidate actions
- authenticated fill rate within conservative haircut band
- pair share remains high enough to support D+ thesis
- residual cost does not dominate PnL
- all redeem/cashflow events reconcile

### G4: production candidate

只有 G3 多日通过后才讨论。任何 G4 前都需要单独审批，不由 automation 自动进入。

## 6. 第一版参数建议

基准从保守 core 开始，不直接用 scaled 层：

```text
PM_STRATEGY=xuan_b27_dplus
PM_XUAN_B27_DPLUS_MODE=observer
PM_XUAN_B27_DPLUS_MARKET_SLUG=btc-updown-5m
PM_XUAN_B27_DPLUS_EDGE=0.040
PM_XUAN_B27_DPLUS_TARGET_QTY=5
PM_XUAN_B27_DPLUS_SEED_PX_LO=0.010
PM_XUAN_B27_DPLUS_SEED_PX_HI=0.990
PM_XUAN_B27_DPLUS_IMBALANCE_QTY_CAP=2
PM_XUAN_B27_DPLUS_SALVAGE_NET_CAP=0.950
PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC=50
PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC=100
PM_XUAN_B27_DPLUS_MAX_ACTIVE_MARKETS=1
PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=2
PM_XUAN_B27_DPLUS_POST_ONLY=true
PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER=false
PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN=true
```

G2 smoke canary 前，默认 `PM_XUAN_B27_DPLUS_MODE=observer`，不得直接设成 live/canary。

## 7. Kill Switch

任一条件触发立即停止策略并保留 artifact：

- unknown order state
- unknown fill state
- unknown wallet/balance state
- post-only passive leg unexpectedly executes as taker
- open orders cannot be cancelled
- stale market or wrong market slug
- duplicate instance id
- max open cost breached
- max strategy exposure breached
- max daily realized loss breached
- max residual cost breached
- account/wallet balance mismatch
- recorder write failure
- user websocket disconnected beyond grace

## 8. 预算判断

推荐资金分层：

- G2 smoke：`50-100 USDC`
- G3 initial canary：`200-500 USDC`
- production pilot：`1000-2000 USDC`，仅 G2/G3 通过后再讨论

`100 USDC` 足够验证机制，但不足以证明盈利稳定性。它的目标是确认真实 maker lifecycle、fill/reject/cancel、ledger、wallet、redeem/cashflow 是否闭环。

## 9. 当前下一步

下一步不是扩大回测参数，而是实现 Rust observer：

1. 新增 `xuan_b27_dplus` 策略骨架。
2. 先只输出 observer/candidate 事件，不下单。
3. 接入 recorder 字段，确保 canary 所需数据能完整重建。
4. 连续跑 BTC 5m observer 30 轮。
5. 通过后再讨论 G2 smoke canary。
