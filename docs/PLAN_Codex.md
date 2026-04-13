重构方向：把 `pair_arb` 从“持续开仓”改成“先配对完成”的单目标系统，否则 `pair_target=0.95-0.98` 在你们当前残仓水平下数学上不可能正EV。

**核心改造**
1. 把策略目标改成主次分离：主目标=`最小化残仓`，次目标=`pair_cost<=pair_target`。  
2. 引入轮内四态状态机：`Probe -> Build -> Complete -> Lock`。一旦进入 `Complete`，同侧 `risk-increasing` 全禁，只做缺失腿。  
3. 配对腿改为“事件+定时”重报：`fill/cross-reject/每N秒` 都允许向上重报，不再被 no-chase 约束。  
4. 动态库存上限：不是固定 `15`，而是按剩余时间收缩（例如 T-180/T-90/T-45 分段降档）。  
5. 末段必须有强制收敛机制：至少 `T-90` 后禁止新风险腿；若允许，`T-30` 启用小额 taker 收口（否则高 `pair_target` 很难稳定盈利）。

**代码落点**
1. [`/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs)：输出状态机驱动的 intent（而不是仅连续价格）。  
2. [`/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs)：按状态硬切 `risk-increasing`/`pairing` 权限。  
3. [`/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_order_io.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_order_io.rs)：配对腿独立重报节奏与 reject 退让。  
4. [`/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_metrics.rs`](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_metrics.rs)：新增 `pair_completion_ratio`、`time_to_flat`、`residual_value_p50/p90`。

**验收标准（针对 pair_target=0.97）**
1. `E[L_residual] <= 0.03 * E[Q_pair]`（这是硬数学门槛）。  
2. round-end `residual_value` 的 `p50 <= 0.3`、`p90 <= 0.8`。  
3. `T-90` 后不再新增风险腿成交。

你选的是方案1，这套重构可以做，但它本质是“先把残仓打到接近0”，不是调几个阈值。若同意，我下一步直接按这个四态状态机从 `pair_arb.rs + coordinator_execution.rs` 开始落代码。