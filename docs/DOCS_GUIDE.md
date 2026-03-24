# 文档导航

当前仓库只保留一套正式文档体系，围绕 `glft_mm` 实盘主线展开。

## 首选阅读路径

1. `README.md`
2. `docs/STRATEGY_V2_CORE_ZH.md`
3. `docs/STRATEGY_GLFT_MM_ZH.md`
4. `docs/CONFIG_REFERENCE_ZH.md`
5. `docs/GO_LIVE_5M_CHECKLIST_ZH.md`
6. `docs/TESTING.md`
7. `docs/ADDING_STRATEGY_ZH.md`

## 文档职责

- `README.md`
  - 仓库入口、策略定位、最小启动方式
- `docs/STRATEGY_V2_CORE_ZH.md`
  - 当前系统架构、共享执行链路与共享风控边界
- `docs/STRATEGY_GLFT_MM_ZH.md`
  - `glft_mm` 的独立策略规格书：信号、数学、适用边界、当前真实行为
- `docs/CONFIG_REFERENCE_ZH.md`
  - `.env` / `.env.example` 参数语义与推荐模板值
- `docs/GO_LIVE_5M_CHECKLIST_ZH.md`
  - 5m 实盘前的最终验收清单和推荐参数快照
- `docs/TESTING.md`
  - dry-run / live 冒烟 / 回归测试方法
- `docs/ADDING_STRATEGY_ZH.md`
  - 如何新增一个可插拔策略，而不破坏统一执行层

## 维护原则

1. 只保留当前有效主线，不再同时维护多份历史路线图。
2. 参数含义以 `docs/CONFIG_REFERENCE_ZH.md` 和 `.env.example` 为准。
3. 共享行为边界以 `docs/STRATEGY_V2_CORE_ZH.md` 为准，`glft_mm` 专属行为以 `docs/STRATEGY_GLFT_MM_ZH.md` 为准。
4. 若文档与代码冲突，以代码为准，并应立即修正文档。
