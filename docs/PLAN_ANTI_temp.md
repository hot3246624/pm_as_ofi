## PLAN_Codex.md 总评：7/10 — 方向对，但有 4 个关键漏洞

### ✅ 计划做对了的
- 保持 `maker-only / buy-only / pair-cost-first` 纪律
- SoftClose 设计思路正确（让 PairArb 不再完全绕过 endgame）
- OFI 不动是明智的
- 可观测性补齐是必要的

### ❌ 4 个必须修正的缺陷

| 缺陷 | 问题 | 后果 |
|---|---|---|
| **A: 梯度规则不解决 Round#2** | Round#2 的 `0.33→0.28→0.25→0.17` **本身就是递减的** | 梯度规则全部 pass，Round#2 灾难会重现 |
| **B: 缺少价格底限** | Round#3 的 `YES@0.06, @0.01` 无人阻止 | VWAP 数学允许但策略上不合理 |
| **C: 梯度重置条件太松** | NO 成交后 residual_qty 下降触发重置 | 正是 Round#1 `0.23→0.25` 回升的根因，改了等于没改 |
| **D: 旧单退休用 Cancel 有空窗** | `1s grace → Clear(SoftReset)` 导致 1-2s 无单 | 快速市场会错过配对窗口 |

### 📌 建议补充的 4 个补丁
1. **pair_progress_brake**（Round#2 解药：`net_diff≥10` 且 pair 无进展 >30s 时暂停主仓侧）
2. **min_bid_price = 0.10**（绝对价格底限）
3. **梯度重置仅在主仓侧翻转**（收紧条件）
4. **fill 后 forced reprice**（缩小 reprice_band 到 0，而非 cancel+re-place）

详细分析已写入 artifact，请审阅。