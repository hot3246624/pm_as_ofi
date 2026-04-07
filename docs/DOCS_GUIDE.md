# 文档导航

当前仓库保留一套正式文档体系：
- `pair_arb` 是当前验证主线
- `glft_mm` 是 challenger / research
- `gabagool_grid` 是可直接运行的 buy-only 对照策略

## 首选阅读路径

1. `README.md`
2. `docs/STRATEGY_V2_CORE_ZH.md`
3. `docs/STRATEGY_PAIR_ARB_ZH.md`
4. `docs/STRATEGY_GLFT_MM_ZH.md`
5. `docs/STRATEGY_GABAGOOL_GRID_ZH.md`
6. `docs/CONFIG_REFERENCE_ZH.md`
7. `docs/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md`
8. `docs/TESTING.md`
9. `docs/ADDING_STRATEGY_ZH.md`

## 文档职责

- `README.md`
  - 仓库入口、策略定位、最小启动方式
- `docs/STRATEGY_V2_CORE_ZH.md`
  - 当前系统架构、共享执行链路与共享风控边界
- `docs/STRATEGY_GLFT_MM_ZH.md`
  - `glft_mm` 的独立策略规格书：信号、数学、适用边界、当前真实行为（非当前主线）
- `docs/STRATEGY_PAIR_ARB_ZH.md`
  - `pair_arb` 的独立策略说明：`pair_target`、三段 skew、tier avg-cost cap、VWAP ceiling、OFI 软塑形、Round Suitability Gate、关键日志口径
- `docs/STRATEGY_GABAGOOL_GRID_ZH.md`
  - `gabagool_grid` 的独立策略说明：open-pair-band、utility 过滤、离散 buy-only 逻辑
- `docs/CONFIG_REFERENCE_ZH.md`
  - `.env` / `.env.example` 参数语义与推荐模板值
- `docs/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md`
  - `pair_arb` 的分阶段测试与上线清单（5m 冒烟 + 15m 收益验证）
- `docs/TESTING.md`
  - dry-run / live 冒烟 / 回归测试方法
- `docs/ADDING_STRATEGY_ZH.md`
  - 如何新增一个可插拔策略，而不破坏统一执行层

## 维护原则

1. 只保留当前有效主线，不再同时维护多份历史路线图。
2. 参数含义以 `docs/CONFIG_REFERENCE_ZH.md` 和 `.env.example` 为准。
3. 共享行为边界以 `docs/STRATEGY_V2_CORE_ZH.md` 为准，具体策略行为以各自独立策略文档为准。
4. 若文档与代码冲突，以代码为准，并应立即修正文档。
