# `pair_arb` 测试与上线清单

## 1. 目标

当前主线是 `pair_arb`，目标分两段：
- 机制正确性：`5m` dry-run
- 收益验证：`15m`（先 dry-run，再小额 live）

## 2. 固定基线（验证期冻结）

```env
PM_STRATEGY=pair_arb
POLYMARKET_MARKET_SLUG="btc-updown-15m"
PM_BID_SIZE=5.0
PM_MAX_NET_DIFF=5.0
PM_PAIR_TARGET=0.98
PM_AS_SKEW_FACTOR=0.15
PM_AS_TIME_DECAY_K=2.0
PM_DRY_RUN=true
```

验证期内不要中途改这些参数，避免样本不可解释。

## 3. 阶段 A：5m 机制冒烟（2-3 轮）

目标：
- 无生命周期异常（ghost order / unmanaged order）
- `pair_arb` 不发 `Hedge` / `OneShotTakerHedge`
- 无明显撤挂风暴

通过标准：
- 轮次完整结束
- 没有异常拒单连锁
- 库存和净仓轨迹与日志一致

## 4. 阶段 B：15m 收益验证（10 轮）

先 `PM_DRY_RUN=true` 跑 3 轮，再切小额 live 跑满 10 轮。

聚焦指标：
- `round_total_pnl`
- `residual_inventory_cost_end`
- `loss_attribution`
- `time_weighted_abs_net_diff`

建议门槛（10 轮）：
- 累计 `round_total_pnl >= 0`
- 中位数 `round_total_pnl >= 0`
- `residual_inventory_cost_end` 不是主亏损来源

## 5. 切 live 前检查

1. `cargo check`
2. `cargo test --lib`
3. `cargo test --bin polymarket_v2`
4. `.env` 已显式 `PM_STRATEGY=pair_arb`（不要依赖 fallback）
5. `PM_DRY_RUN=false` 前确认 Builder/交易凭证可用
