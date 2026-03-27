User provided detailed 3-round statistics from the 03-27 log and identified remaining convergence gaps. Need to verify the data, analyze the hard misalignment triggers, and evaluate the three proposed next steps.

Viewed polymarket.log.2026-03-27:1-569
Ran command: `grep -n "hard.*misalign\|pause quoting" /Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-03-27`
Ran command: `grep -o 'publish_reason=[a-z_]*' /Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-03-27 | sort | uniq -c | sort -rn`
Ran command: `grep -o 'drift_mode=[A-Za-z]*' /Users/hot/web3Scientist/pm_as_ofi/logs/polymarket.log.2026-03-27 | sort | uniq -c | sort -rn`

Verified all user conclusions from the 03-27 log: 3 hard basis misalignment events (L138/L405/L528), [publish_reason](cci:1://file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_pricing.rs:202:4-204:5) distribution matches reported values, `drift_mode=Normal` 106/108 with only 2 `Damped`. Confirmed: OFI no longer kills (`toxic=0, kill=0` all rounds), budget suppression activating (7/14/0), and hard misalignment occurring at market-end boundary with massive drift_ticks (12-22 range).

日志数据完全验证了你的结论。以下是深度分析评估：

---

### 📊 数据验证——你的结论 100% 准确

| 指标 | 第1轮 | 第2轮 | 第3轮 | 日志确认 |
|---|---|---|---|---|
| placed | 29 | 62 | 17 | ✅ L173/L421/L555 |
| reprice | 29 | 60 | 17 | ✅ |
| heat_events | 9 | 12 | 10 | ✅ |
| toxic/kill/blocked | 0/0/0 | 0/0/0 | 0/0/0 | ✅ |
| budget_suppressed | 7 | 14 | 0 | ✅ |
| forced_realign | 11 | 10 | 2 | ✅ |
| hard misalignment | L138 | L405 | L528 | ✅ 3次 |
| drift_mode | Normal=106, **Damped=2** | | | ✅ |
| publish_reason | shadow_dwell=39, forced_realign=23, price_move=20, unsafe_quote=11 | | | ✅ |

---

### 🔬 三个 Hard Misalignment 的根因解剖

| 事件 | modeled_mid | synthetic_mid | drift_ticks | 触发环境 |
|---|---|---|---|---|
| L138 | 0.363 | **0.160** | 12.1 | 第1轮末尾，市场急跌触底 |
| L405 | 0.417 | **0.538** | 12.4 | 第2轮末尾，市场急涨 |
| L528 | 0.614 | **0.818** | 12.1 | 第3轮末尾，市场急涨到极端 |

**关键发现**：三次 hard misalignment **全部发生在市场生命周期的最后 60-90 秒**，即 btc-updown-5m 快结束时。此时盘口极端稀薄，synthetic_mid 跳变剧烈（0.16 / 0.818 这种极端值），而 modeled_mid 由于 basis_clamped 的限速还在慢慢追。这不是模型错误，而是**临近到期时流动性蒸发**造成的结构性死角。

---

### ✅ 你的三个建议评估

**建议1：Normal 下增强 publish 滞回，减少 price_move** — ⭐⭐⭐⭐⭐ 完全可行
- `price_move` 占 20/108 (18.5%)。日志显示很多 `price_move` 的 `dist_trusted_ticks` 只有 1-3（非常小的偏移）。这些可以被更宽松的 shadow_dwell 或更高的 price_move 门槛吸收。
- **建议实施**：`price_move` 的触发阈值从当前的 `>= 2 ticks` 提高到 `>= 3 ticks`，或者给 Normal 模式下的 price_move 也加上 600ms dwell 缓冲。

**建议2：拆分 forced_realign 子类型** — ⭐⭐⭐⭐⭐ 必要且合理
- 现在 `forced_realign=23` 和 `debt_realign=23` 完全一致（说明计数逻辑是统一的），但实际上有两种截然不同的触发源：
  - **hard_guard**：`dist_trusted > 12 ticks`（绝对安全阀）
  - **debt_guard**：债务积分超标
- 拆开后可以精确识别哪些是真正的安全性发布，哪些是可以被进一步压缩的。

**建议3：削减 hard misalignment 触发** — ⭐⭐⭐⭐ 可行，但需注意边界
- 三次 hard misalignment 全部发生在**临到期+流动性极薄**的窗口。最安全的削减方式是：
  1. **放宽 hard misalignment 的 drift_ticks 门槛**：从 12 提升到 15 ticks（因为临近到期时 synthetic_mid 本身就不可信了）
  2. **增加 endgame 窗口内的特殊处理**：在 `soft_endgame`（60s）窗口内，如果 `drift_ticks > 12` 但 `stale_secs < 2s`（盘口还活着），先进入 Damped 而不是直接 Pause
  3. **不建议完全取消 hard pause**——它是最后的安全阀

---

### 📈 进化趋势总结

| 版本 | toxic kill | reprice/placed | forced_realign | hard_pause |
|---|---|---|---|---|
| 03-25 原版 | 17 | ~1:1 | N/A | 频繁 |
| 03-26 版 | 0 | ~1:1 | 26 | 若干 |
| 03-27 版 | **0** | **~1:1 (仍需压)** | **23 (↓12%)** | **3次 (末尾集中)** |

**结论完全同意**：系统正在收敛，但 reprice/placed ≈ 1:1 和 hard pause 确实阻止了"稳定可实盘"。你的三个修复方向精准对准了残留问题。