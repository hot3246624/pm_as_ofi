# pair_arb_v2 实施方案 — 基于 evidence 逆向工程

## 目标

将 pair_arb 策略改造为高频 buy-only maker，对标 0x8dxd/k9 的盈利模式：
- **每轮交易 50-100+ 笔**（当前 2-4 笔）
- **pair_cost 目标 < 0.95**（当前 0.937 但无法配对）
- **net_diff 容忍 100-200 units**（当前 5 units）
- **被动等待 taker 成交，不做方向预测**

---

## User Review Required

> [!IMPORTANT]
> **资金需求**：改造后每轮需要 $200-500 投入（当前 $5）。请确认账户余额是否充足。

> [!IMPORTANT]
> **执行模式切换**：pair_arb 当前使用 `DirectionalHedgeOverlay`（有 sell-side hedging），改造后切换到 `UnifiedBuys`（buy-only）。这意味着**不再有自动卖出**，策略完全依赖到期结算收益。

> [!WARNING]
> **风险变化**：max_net_diff 从 5 增加到 100 意味着单轮最大风险从 ~$5 增加到 ~$100。建议从 dry-run 开始验证。

---

## Proposed Changes

### 1. Strategy Layer — pair_arb.rs 重写

#### [MODIFY] [pair_arb.rs](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs)

**当前问题**：
- 仅 121 行，逻辑过于简单
- 没有价格分离意识
- 没有单侧连续累积能力
- 固定 `bid_size`，不根据机会质量调整

**改造要点**：

```rust
// 新增：3 阶段报价逻辑
enum PairArbPhase {
    // price_separation < 0.15 → 双边小量
    BalancedBuild,
    // price_separation >= 0.15 且一侧便宜 → 加大便宜侧
    OpportunisticBuild, 
    // pair_cost 接近 pair_target 或 net_diff 接近上限 → 只允许改善配对
    PairingOnly,
}
```

核心定价逻辑保留 pair_cost guard（`ceiling = pair_target - opposite_avg_cost`），但新增：

1. **价格分离度计算**：`separation = |yes_ask - no_ask|`
   - 当 `separation >= 0.15` 时，允许单侧大量累积
   - 辅以 `open_pair_band` 上限保护

2. **动态 sizing**：
   - 基础: `bid_size`（20 units）
   - 当对侧 ask 很便宜（< 0.15）时：加倍 `bid_size × 2`
   - 受 `max_net_diff` 硬约束

3. **pair_cost 阶梯控制**：
   - `pair_cost < 0.90` → 继续累积，宽松报价
   - `0.90 ≤ pair_cost < 0.95` → 只做改善配对的交易
   - `pair_cost ≥ 0.95` → 暂停，等更好价格（除非是配对修复）

---

### 2. Execution Mode — strategy.rs

#### [MODIFY] [strategy.rs](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy.rs)

将 `PairArb` 的执行模式从 `DirectionalHedgeOverlay` 改为 `UnifiedBuys`：

```diff
 pub(crate) fn execution_mode(self) -> StrategyExecutionMode {
     match self {
         Self::GabagoolGrid | Self::GabagoolCorridor => StrategyExecutionMode::UnifiedBuys,
         Self::GlftMm => StrategyExecutionMode::SlotMarketMaking,
-        Self::PairArb | Self::DipBuy | Self::PhaseBuilder => {
-            StrategyExecutionMode::DirectionalHedgeOverlay
+        Self::PairArb => StrategyExecutionMode::UnifiedBuys,
+        Self::DipBuy | Self::PhaseBuilder => {
+            StrategyExecutionMode::DirectionalHedgeOverlay
         }
     }
 }
```

**理由**：`DirectionalHedgeOverlay` 会在 `net_diff != 0` 时触发卖出对冲。这与 gabagool/0x8dxd 的 buy-only 策略完全矛盾——他们**故意维持大量未对冲仓位**等待价格分离。

---

### 3. Parameters — .env

#### [MODIFY] [.env](file:///Users/hot/web3Scientist/pm_as_ofi/.env)

Phase 1 (dry-run 验证)：

```diff
-PM_STRATEGY=glft_mm
+PM_STRATEGY=pair_arb

-PM_BID_SIZE=5.0
+PM_BID_SIZE=20.0

-PM_MAX_NET_DIFF=5.0
+PM_MAX_NET_DIFF=100.0

-PM_PAIR_TARGET=0.97
+PM_PAIR_TARGET=0.95

-PM_DEBOUNCE_MS=700
+PM_DEBOUNCE_MS=200

-PM_REPRICE_THRESHOLD=0.020
+PM_REPRICE_THRESHOLD=0.010

-PM_POST_ONLY_SAFETY_TICKS=2.0
+PM_POST_ONLY_SAFETY_TICKS=1.0

 # pair_arb 必需参数（取消注释）
-# PM_OPEN_PAIR_BAND=0.99
+PM_OPEN_PAIR_BAND=0.99

-# PM_AS_SKEW_FACTOR=0.15
+PM_AS_SKEW_FACTOR=0.10
```

---

### 4. Coordinator Config — coordinator.rs

需确认 `pair_arb` 在使用 `UnifiedBuys` 模式时，coordinator 的以下行为正确：
- ✅ `apply_flow_risk()` 不会阻止 buy-only 的报价（**已确认**：`UnifiedBuys` 模式只在 `projected_abs_net_diff <= current` 时才阻止，不影响正常 build）
- ✅ `finalize_provide_dispatch()` 在没有 hedge 时直接走 provide 路径（**已确认**）
- ⚠️ `should_execute_directional_hedges()` 在 `UnifiedBuys` 切换后，只在 HardClose 阶段才触发卖出（**需要确认 endgame 是否需要特殊处理**）

---

## Open Questions

> [!IMPORTANT]
> **Q1: 账户余额**
> 改造后每轮最大投入 ~$100-200（bid_size=20 × max_net_diff=100 × 价格平均 0.50 ÷ 配对 ≈ $200）。当前 USDC 余额是否充足？需要至少 $500 以覆盖多轮 + merge 操作。

> [!IMPORTANT]
> **Q2: 到期仓位处理**
> 在 buy-only 模式下，轮次结束时可能有 100+ units 的单侧仓位。当前的 endgame（soft=60s/hard=30s）是否对 pair_arb 生效？是否需要保留 endgame 的 taker derisk 能力？
> - 选项 A: 完全不做 endgame exit（信任 pair_cost < 0.95 的 edge 会在到期时获利）
> - 选项 B: 保留 endgame 但放宽阈值

> [!WARNING]
> **Q3: GLFT 信号是否保留**
> pair_arb 当前不依赖 GLFT 信号（glft=None in StrategyTickInput）。是否需要利用 Binance 数据作为辅助（如 OFI toxicity detection），还是完全独立运行？
> - 如果保留 OFI：可以在毒性信号出现时暂停报价，防止被信息化 taker 扫单
> - 如果不保留：更简单，不受 Binance WS 断连影响

---

## Verification Plan

### Phase 1: Dry-Run 验证（20 轮）

1. 切换 `PM_STRATEGY=pair_arb`，`PM_DRY_RUN=true`
2. 验证指标：
   - 每轮报价次数 >= 20
   - pair_cost at end-of-round < 0.95（至少 50% 的轮次）
   - 没有 sell-side 订单生成
   - 没有 recovery storm
3. 检查日志中 `DirectionalHedge` 是否完全消失
4. 检查 `execute_hedges` 是否只在 HardClose 阶段触发

### Phase 2: 小额实盘（10 轮）

1. `PM_DRY_RUN=false`，`PM_BID_SIZE=10`，`PM_MAX_NET_DIFF=30`
2. 监控实际 fill 率和 pair_cost
3. 验证余额变化和 claim 流程

### Phase 3: 全额实盘（20 轮）

1. `PM_BID_SIZE=20`，`PM_MAX_NET_DIFF=100`
2. 目标：总 PnL > $0（覆盖交易成本后）
