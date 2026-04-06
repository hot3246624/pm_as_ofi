这份日志我看完了，结论是：**系统稳定性还可以，但发布层仍未收敛到可实盘阈值**。

**范围确认**
- 文件有 `2102` 行，只包含：
1. `Round #1` 完整闭环（开盘到 `Shutdown`）  
2. `Round #2` 仅前约 2 分钟片段（没有 shutdown）  
- 证据：Round1 开始在 [polymarket.log.2026-04-06:24](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:24)，Round1 结束在 [polymarket.log.2026-04-06:1884](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1884)，Round2 开始在 [polymarket.log.2026-04-06:1904](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1904)。

**Round #1 核心量化**
- Shutdown 指标：`placed=824`，`cancel=801`，`replace=22`，`cancel_reprice=796`，`cancel_stale=5`，`reprice_ratio=0.97 (ALERT)`  
  见 [polymarket.log.2026-04-06:1883](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1883)、[polymarket.log.2026-04-06:1886](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1886)。
- `PairArbGate` 总体 keep 仍高，但尾段明显塌陷：`83.6% -> 70.2% -> 50.0%`  
  见 [polymarket.log.2026-04-06:1818](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1818)、[polymarket.log.2026-04-06:1867](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1867)、[polymarket.log.2026-04-06:1873](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1873)。
- OFI 是“热度提醒”状态，不是毒性阻断主因：`heat_events=10, toxic_events=0, kill_events=0`  
  见 [polymarket.log.2026-04-06:1883](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1883)。
- WS 健康总体可用，但有短时 stale 尖峰（触发 StaleData 撤单）：`no_full_book_for` 到 `5.2s`  
  见 [polymarket.log.2026-04-06:1864](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1864)。

**结构性问题（关键）**
1. **发布抖动主因仍是 Reprice 全量路径**，不是 replace。你现在是“高频 cancel+reprovide”，而不是“高频 replace”。  
2. **尾盘价格极端区间（如 YES 0.91~0.96 / NO 0.04~0.01）仍在持续挂撤**，说明“极端段保留策略”没有生效成稳定机制。  
   见 [polymarket.log.2026-04-06:1819](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1819) 到 [polymarket.log.2026-04-06:1860](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1860)。
3. **可观测性还有不一致**：`publish(events=802)` 但 `initial/policy/safety/recovery=0`，这会妨碍你定位发布原因分布。  
   见 [polymarket.log.2026-04-06:1883](/Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-04-06:1883)。

**是否收敛到可实盘**
- 这份日志下我的判断：**还没到**。  
- 主要卡点不是 OFI，也不是 source block，而是 **reprice 全量重挂链路 + 尾段 keep_rate 崩塌**。

如果你同意，我下一步就按这个顺序动手：  
1. 先修发布原因统计口径（把 policy/initial/safety/recovery 记准）。  
2. 再把 pair_arb 的 Reprice 从“Full reset”改成“可保留优先 + 软重发”，专门压 `reprice_ratio`。  
3. 最后针对尾盘 `keep_rate<80%` 段做单独保留策略，避免 0.9/0.1 区间持续抖动。