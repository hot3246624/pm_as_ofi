这份 `2026-04-08` dry-run 的结论很明确：**V4 没有回退，但也没有被真正验证到。**

**核心结论**
1. **这份日志没有任何 fill，V4 的两条主修复几乎都没被触发。**  
- [logs/polymarket.log.2026-04-08:53](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:53)  
- [logs/polymarket.log.2026-04-08:350](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:350)  
- [logs/polymarket.log.2026-04-08:993](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:993)  
这些地方都写了 `No User WS — net_diff stays 0 (no fills)`。  
所以：
- `PairProgressRegime` 一直是 `Healthy`
- `StateImprovementReanchor` 没有真实 fill 可触发
- `merge-aware accounting` 也自然全是 `0`

2. **系统层面是稳定的，之前那类执行层 churn 已经明显不在主矛盾位置。**  
完成 round 的 shutdown 大致是：
- [logs/polymarket.log.2026-04-08:302](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:302)
- [logs/polymarket.log.2026-04-08:608](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:608)
- [logs/polymarket.log.2026-04-08:942](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:942)

共同特征：
- `recovery=0`
- `safety=0`
- `toxic_events=0`
- `kill_events=0`
- `source_blocked/divergence=0`

这说明：
- 没有 recovery storm
- 没有 source stale 把系统搞乱
- 没有 reference/publish 架构回退

3. **当前 dry-run 暴露的真实问题，不是 V4 失效，而是 `pair_arb` 在“无成交、库存永远 0”的环境里仍然会持续深挂。**  
最关键证据是 `PairArbGate(30s)`：
- [logs/polymarket.log.2026-04-08:100](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:100)
- [logs/polymarket.log.2026-04-08:272](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:272)
- [logs/polymarket.log.2026-04-08:922](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:922)
- [logs/polymarket.log.2026-04-08:1168](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:1168)

全部是：
- `keep_rate=100.0%`
- `skip(inv/sim/util/edge)=0/0/0/0`

这意味着：
- 当前 candidate admission 在 `net_diff=0` 的 dry-run 场景下几乎没有形成任何过滤
- 策略会一路把 bid 往下走，只要 book 和内部报价链允许

4. **因此你看到的深价继续存在，但它和上一轮要修的“状态改善后旧单不重锚”不是同一个问题。**  
例如：
- `No@0.400 -> 0.330 -> 0.280 -> 0.230 -> 0.180`  
  [logs/polymarket.log.2026-04-08:108](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:108)
  [logs/polymarket.log.2026-04-08:109](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:109)
  [logs/polymarket.log.2026-04-08:110](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:110)
  [logs/polymarket.log.2026-04-08:120](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:120)
  [logs/polymarket.log.2026-04-08:171](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:171)
- `Yes@0.140 -> 0.090 -> 0.040`  
  [logs/polymarket.log.2026-04-08:1059](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:1059)
  [logs/polymarket.log.2026-04-08:1096](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:1096)
  [logs/polymarket.log.2026-04-08:1111](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:1111)

但这里 `net_diff` 始终是 `0`，所以：
- 这不是“高失衡 stalled 失效”
- 也不是“状态改善后没 reanchor”
- 而是“flat/no-fill 环境下，策略没有单独的 exploration governor”

5. **OFI 还在起作用，但只是 subordinate shaping，不是主 gate。**  
shutdown 显示：
- `pair_arb_softened=16618 / 20982 / 24417`
- `pair_arb_suppressed=0`
对应：
- [logs/polymarket.log.2026-04-08:302](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:302)
- [logs/polymarket.log.2026-04-08:608](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:608)
- [logs/polymarket.log.2026-04-08:942](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-08:942)

所以不是 OFI 被抛弃了，而是：
- 它在这类场景只负责把报价压低一点
- 并没有强到会阻止整段 stair-step 深挂

**我对这份日志的判断**
- **V4 没有失败。**
- **但这份 dry-run 也没有证明 V4 成功。**
- 它证明的是另一件事：  
  `pair_arb` 在“无 fill、永远 flat”的 dry-run 模式下，仍然会持续深挂，而当前 `PairArbGate` 对这种场景几乎没有过滤。

**是否需要继续优化**
如果你问“下一步最有价值的动作是什么”，答案是：

1. **如果目标是验证 V4 是否解决实盘问题：不要继续靠这种 dry-run。**  
因为它根本不产生：
- `net_diff` 变化
- bucket 改善
- stalled
- reanchor

2. **如果目标是继续从 dry-run 里挖问题：那现在唯一值得修的是一个独立问题。**  
也就是：
- `flat + no-fill` 场景下的单侧深挂探索约束  
这和 V4 不是一回事，应该被当成单独策略问题处理。

**建议**
1. 先跑小额 live，验证 V4 真正关心的两件事：
- 高失衡且长期无配对时，same-side build 是否收敛
- `|net_diff|` 改善或回到 `0` 后，旧低价单是否被重锚
2. 如果你仍想先在 dry-run 上再收一道口，我建议下一轮只做一个非常小的策略改动：
- 给 `pair_arb` 增加 `flat/no-fill exploration guard`
- 只作用于 `net_diff=0` 且长期无成交的场景
- 不碰 V4 的 reanchor / stalled 逻辑

如果你要，我下一步可以直接给出这个 `flat/no-fill exploration guard` 的最小实现方案。