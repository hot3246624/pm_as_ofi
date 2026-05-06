# 文档入口

本目录按用途分层，避免研究报告、运行手册、系统说明和策略计划混在一起。

## 目录结构

| 目录 | 用途 |
|---|---|
| `architecture/` | 系统架构、共享执行链路、数据采集与 CLOB 升级说明 |
| `strategies/` | 当前或候选策略的行为规格 |
| `runbooks/` | dry-run、上线、排障、验收清单 |
| `reference/` | 参数、测试、新增策略、文档维护等长期参考 |
| `plans/` | 阶段性实施计划、评审稿、历史路线图 |
| `research/` | trader 研究、archetype 对照、`xuanxuan008` 深度逆向 |
| `learning/` | 面向当前项目的量化学习材料与案例手册 |

## 推荐入口

### 当前系统与运行

1. [STRATEGY_V2_CORE_ZH.md](architecture/STRATEGY_V2_CORE_ZH.md)
2. [STRATEGY_PAIR_ARB_ZH.md](strategies/STRATEGY_PAIR_ARB_ZH.md)
3. [CONFIG_REFERENCE_ZH.md](reference/CONFIG_REFERENCE_ZH.md)
4. [GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md](runbooks/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md)
5. [TESTING.md](reference/TESTING.md)

### `xuanxuan008` 研究主线

1. [XUANXUAN008_STRATEGY_V2_ZH.md](research/xuan/XUANXUAN008_STRATEGY_V2_ZH.md)
2. [XUANXUAN008_STRATEGY_V2_MAJOR_UPDATE_EVAL_ZH.md](research/xuan/XUANXUAN008_STRATEGY_V2_MAJOR_UPDATE_EVAL_ZH.md)
3. [XUAN_WINNER_BIAS_SOURCE_ASSESSMENT_ZH.md](research/xuan/XUAN_WINNER_BIAS_SOURCE_ASSESSMENT_ZH.md)
4. [XUAN_OPEN_GATE_FAMILIES_ZH.md](research/xuan/XUAN_OPEN_GATE_FAMILIES_ZH.md)
5. [XUAN_INCOMING_DATA_FALSIFICATION_MATRIX_ZH.md](research/xuan/XUAN_INCOMING_DATA_FALSIFICATION_MATRIX_ZH.md)

### 快速补齐策略研究能力

1. [XUAN_QUANT_LEARNING_DOC_ZH.md](learning/XUAN_QUANT_LEARNING_DOC_ZH.md)
2. [POLYMARKET_PAIR_ARB_CASEBOOK_ZH.md](learning/POLYMARKET_PAIR_ARB_CASEBOOK_ZH.md)

## 放置规则

- 新 trader 深度研究放到 `research/traders/`。
- `xuanxuan008` 主线研究放到 `research/xuan/`。
- 横向 archetype 和 leaderboard 对照放到 `research/archetypes/`。
- 可运行流程、检查清单、验收 SQL 放到 `runbooks/`。
- 稳定参数说明和开发参考放到 `reference/`。
- 阶段性路线图和讨论稿放到 `plans/`。
- 不再把新文档直接放在 `docs/` 根目录，根目录只保留入口文件。
