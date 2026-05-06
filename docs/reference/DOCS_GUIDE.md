# 文档维护指南

主入口已经迁移到 [docs/README.md](../README.md)。本文只保留维护规则。

## 分类规则

| 目录 | 放什么 |
|---|---|
| `architecture/` | 系统架构、共享执行链路、数据采集、CLOB 升级 |
| `strategies/` | 策略行为规格、策略边界、策略参数语义 |
| `runbooks/` | 运行命令、上线 checklist、dry-run 验收、排障步骤 |
| `reference/` | 长期参考材料，例如配置、测试、新增策略、文档规则 |
| `plans/` | 计划、评审、历史路线图、阶段性设计稿 |
| `research/` | trader 研究、xuan 深度逆向、archetype 对照 |
| `learning/` | 面向本项目的学习材料、案例手册、训练材料 |

## 放置原则

1. 新文档不要直接放在 `docs/` 根目录，除非它是目录入口。
2. 研究文档和运行文档必须分开；研究结论不能混进上线 checklist。
3. 阶段性计划可以保留在 `plans/`，但被主线取代后要在文档开头标明取代关系。
4. `xuanxuan008` 主线研究统一放在 `research/xuan/`。
5. 横向 trader 对照放在 `research/traders/`，archetype 总结放在 `research/archetypes/`。
6. 参数含义以 [CONFIG_REFERENCE_ZH.md](CONFIG_REFERENCE_ZH.md) 和 `.env.example` 为准。
7. 若文档与代码冲突，以代码为准，并立即修正文档。

## 推荐阅读路径

当前系统与运行：

1. [README.md](../../README.md)
2. [docs/README.md](../README.md)
3. [STRATEGY_V2_CORE_ZH.md](../architecture/STRATEGY_V2_CORE_ZH.md)
4. [STRATEGY_PAIR_ARB_ZH.md](../strategies/STRATEGY_PAIR_ARB_ZH.md)
5. [CONFIG_REFERENCE_ZH.md](CONFIG_REFERENCE_ZH.md)
6. [GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md](../runbooks/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md)
7. [TESTING.md](TESTING.md)

`xuanxuan008` 研究：

1. [XUANXUAN008_STRATEGY_V2_ZH.md](../research/xuan/XUANXUAN008_STRATEGY_V2_ZH.md)
2. [XUAN_WINNER_BIAS_SOURCE_ASSESSMENT_ZH.md](../research/xuan/XUAN_WINNER_BIAS_SOURCE_ASSESSMENT_ZH.md)
3. [XUAN_OPEN_GATE_FAMILIES_ZH.md](../research/xuan/XUAN_OPEN_GATE_FAMILIES_ZH.md)
4. [XUAN_INCOMING_DATA_FALSIFICATION_MATRIX_ZH.md](../research/xuan/XUAN_INCOMING_DATA_FALSIFICATION_MATRIX_ZH.md)

学习材料：

1. [XUAN_QUANT_LEARNING_DOC_ZH.md](../learning/XUAN_QUANT_LEARNING_DOC_ZH.md)
2. [POLYMARKET_PAIR_ARB_CASEBOOK_ZH.md](../learning/POLYMARKET_PAIR_ARB_CASEBOOK_ZH.md)
