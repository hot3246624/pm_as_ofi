抱歉，grep 搜索超时了。不过我已经读过足够的代码来纠正分析。直接给结论：

## R3 纠正分析

你说得对，`apply_tier_avg_cost_cap` 的公式是：

```
第二笔 NO 报价 = min(raw_no, no_avg_cost × tier1_mult)
              = min(raw_no, 0.41 × 0.70)
              = min(raw_no, 0.287)
```

**但实际挂出的是 NO@0.390 — 远高于 0.287。说明 tier cap 没生效。**

### 根因：Pending fill 不参与 tier cap 计算

从日志看第一笔 fill 后的状态：

```
settled YES=0.0 NO=0.0 | net=0.0      ← settled 还是空的
working YES=0.0 NO=5.0@0.41 | net=-5.0 ← working 已更新
pending_no=5.00 fragile=true           ← fill 仍在 pending
```

`apply_tier_avg_cost_cap` 的触发条件（代码 L400）：
```rust
if inv.net_diff <= -5.0 && inv.no_qty > 0 && inv.no_avg_cost > 0.0
```

**如果传入的 `inv` 用的是 settled inventory**，则：
- `no_qty = 0`（pending 的 5 份不算）
- 条件 `no_qty > 0` → **FALSE**
- **tier cap 完全没触发！**

所以 NO@0.390 就是 raw 基础定价（mid - excess/2 + skew），完全没被 tier 限制。

### 真正的连锁路径

```
Fill 1: NO@0.41  → pending, settled仍空 → tier cap 不触发
                 → raw_no ≈ 0.39 → 直接挂出 NO@0.390
                 
Fill 2: NO@0.39 fill  → pending再增加, settled仍空 → tier cap 仍不触发
                      → 这次 net=-10 (working), 进入 High bucket
                      → raw 计算出 NO@0.080 (可能是 skew 把价格压得很低)
```

### 问题严重性

这不是 "tier 参数不合理" 的问题，而是 **tier cap 在最关键的前两笔 fill 中完全失效**：

| 应有行为 | 实际行为 |
|---|---|
| NO@0.41 fill → tier cap → 第二笔 NO@**0.287** | 第二笔 NO@**0.390** |
| 如果 NO@0.287 不被填 → 停在 net=-5 | NO@0.390 秒填 → net=-10 |
| 残仓 5 份，worst_case ≈ -$2.05 | 残仓 **10** 份，worst_case = **-$4.00** |

**tier cap 失效直接导致亏损翻倍。**

### 修复方向

`apply_tier_avg_cost_cap` 必须用 **working inventory**（含 pending fills）而不是 settled inventory。这是一个明确的 bug — `compute_quotes` 传入的 `inv` 应该是 working 状态而不是 settled 状态，否则每次 fill 后的第一个 tick 都会绕过 tier 保护。

需要我去代码里确认 `inv` 的传入来源并定位修复点吗？