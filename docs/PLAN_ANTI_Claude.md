I've now traced through the actual Round #3 fill sequence against the V5 rules quantitatively. Let me write the evaluation.

Edited plan_codex_evaluation.md

评估完成。核心发现：

## V5 评分：7.5/10 — 一个关键盲区

### ✅ V5 做得好的
- **状态签名 Republish** 设计优秀（`dominant_side × net_bucket × soft_close`）
- **无路径依赖**哲学比 V1 更优雅
- **Tier cap 参数化**方向正确，对正常价格区间有约束力
- SoftClose、OFI 边界明确

### ❌ 结构性盲区：Tier cap 是天花板，不是地板

我用 Round#3 的**实际填单数据**逐笔回放了 V5 规则：

```
YES@0.060 下单时: yes_avg=0.324, net_diff=5
  → tier 1 cap = 0.324 × 0.80 = 0.259
  → 0.060 < 0.259 → ✅ 通过（cap 是上限，0.06 远低于上限）

YES@0.010 下单时: yes_avg=0.280, net_diff=10
  → tier 2 cap = 0.280 × 0.60 = 0.168
  → 0.010 < 0.168 → ✅ 通过

高失衡准入: min_utility_delta = 0.10
  → 买 5@0.01 成本仅 $0.05，但 pair_cost 从 0.6925 降到 0.6539
  → locked_pnl 改善 +$0.77 → 远超 0.10 门槛 → ✅ 通过
```

**V5 的全部 6 层防线无一能拦截 YES@0.01。**

### 📌 唯一建议：加一个 `MIN_BID_PRICE`

这不违反 V5 的「无路径依赖、只看当前状态」哲学：
- 不看 last fill price
- 不冻结同侧买入
- 只是「价格低于 0.10 直接不报价」
- Tier cap = 天花板，MIN_BID = 地板——**天花板和地板组成完整防线**

详细分析（含完整定量回放表格）已写入 artifact。