# 新增策略开发指南

本文档只描述一件事：如何在当前架构下新增一个策略插件，而不破坏统一执行层。

## 1. 先判断该不该新建策略

应该新建策略，当且仅当：
- 入场逻辑变了
- 常态报价方向变了
- 目标函数变了

不应该新建策略，而应改执行层/风控层，当问题属于：
- keep-if-safe / reprice / post-only
- 共享 OFI 引擎或其消费边界
- stale / endgame
- recycle / claim
- reconcile / OMS / Executor 生命周期

## 2. 当前插件边界

策略层输入：`StrategyTickInput`
- `inv`
- `book`
- `metrics`
- `ofi`（可选）
- `glft`（可选）

策略层输出：`StrategyQuotes`
- `yes_buy`
- `yes_sell`
- `no_buy`
- `no_sell`

每个 `StrategyIntent` 只描述：
- `side`
- `direction`
- `price`
- `size`
- `reason`

策略层不允许：
- 直接访问 OMS / Executor
- 直接改库存
- 直接发网络请求
- 自己实现 endgame / recycle / claim

## 3. 执行模式

当前有三种执行模式：
- `UnifiedBuys`
  - 典型：`pair_arb` / `gabagool_grid` / `gabagool_corridor`
  - 正常盘中主要输出 buy，由共享执行层治理保留/重报价/清槽
- `DirectionalHedgeOverlay`
  - 典型：`dip_buy` / `phase_builder`
  - 策略层输出 provide，统一层叠加 directional hedge
- `SlotMarketMaking`
  - 典型：`glft_mm`
  - 常态输出四槽位真双边 maker

新增策略前先明确自己属于哪一类。

## 4. 最小接入步骤

### 步骤 1：新增策略文件

放到：
- `src/polymarket/strategy/<your_strategy>.rs`

最小骨架：

```rust
use crate::polymarket::coordinator::StrategyCoordinator;
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{
    QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput,
};

pub(crate) struct MyStrategy;
pub(crate) static MY_STRATEGY: MyStrategy = MyStrategy;

impl QuoteStrategy for MyStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::MyStrategy
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        let px = coordinator.safe_price(input.book.yes_bid);
        if px > 0.0 {
            quotes.set(StrategyIntent {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                price: px,
                size: 5.0,
                reason: BidReason::Provide,
            });
        }
        quotes
    }
}
```

### 步骤 2：注册到 `strategy.rs`

需要改四处：
- `pub mod ...`
- `StrategyKind`
- `parse()/as_str()`
- `StrategyRegistry::entries()`

### 步骤 3：如有新参数，接入 `CoordinatorConfig`

不要在策略里直接 `env::var()`。
参数应进入：
- `CoordinatorConfig`
- `from_env()`
- `.env.example`
- `docs/reference/CONFIG_REFERENCE_ZH.md`

### 步骤 4：补测试

至少补：
- 解析/注册测试
- 最小行为测试
- 若策略消费 `ofi/glft`，补对应信号分支测试
- 若涉及 sell/四槽位，再补执行层兼容测试

### 步骤 5：同步文档

至少同步：
- `README.md`
- `docs/architecture/STRATEGY_V2_CORE_ZH.md`
- `docs/reference/CONFIG_REFERENCE_ZH.md`
- `.env.example`

如果该策略将成为 live 主线或长期维护策略，还应新增独立规格书：
- `docs/strategies/STRATEGY_<YOUR_STRATEGY>_ZH.md`

如果只是实验/回放策略：
- 不必强行新增完整规格书
- 但至少要在 `README.md` 里说明定位
- 并在 `docs/reference/CONFIG_REFERENCE_ZH.md` / `.env.example` 标清哪些参数只服务该策略

## 5. 开发原则

1. 策略只表达意图，不做执行。
2. 风控尽量走共享层，不要给局部路径特权。
3. 新策略优先复用现有 reason 和 slot 语义。
4. 如果问题属于执行稳定性，就不要伪装成“新策略”。
