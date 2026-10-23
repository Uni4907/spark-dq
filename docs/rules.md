# SparkDQ 规则管理阶段开发日志

日期：2026-10-18（Asia/Shanghai）。真实仓库 /home/sparkdq/projects/spark-dq（SparkDQ-Ubuntu）。起点 0c90328，工作区干净。

## 已完成

1. 类型检测：64 位有符号整数、十进制数、YYYY-MM-DD 日期；真实 Spark try_cast/Column 谓词与异常文件输出。
2. 数值范围检测：包含上下边界，可单边限制；参数用规范十进制字符串冻结，API 对 JSON 小数使用 BigDecimal，避免二进制浮点舍入。
3. 正则检测：Java 语法、整字段匹配、最多 256 字符，提交前校验语法；运行仍受现有 Spark 子进程 300 秒超时约束。
4. 规则模板新增、读取、编辑、历史修订查询和归档。每次更新追加修订，不覆盖旧定义；expectedRevision 防止陈旧编辑覆盖。归档不删除模板、修订或任务数据。
5. 任务冻结完整类型/字段/参数/definitionVersion=1，并记录模板 ID、修订和当时名称。模板修改或归档后，历史任务继续按原快照复检和治理。
6. 报告按类型、字段和参数共同匹配规则；不同范围/格式不误配。新规则指标、配置、模板来源、异常预览均接入页面、图表和报告。
7. 页面支持附加规则、完整模板编辑、保存新模板/新修订、选择历史修订、刷新冲突后的模板、归档。未保存修改时阻止按模板提交。

## 规则口径

- 空值规则沿用原定义：缺失、空字符串与纯空格。TYPE/RANGE/REGEX 跳过这些值，以 checkedCount、skippedBlankCount、invalidCount 单独记录，避免把缺失值当类型错误。
- 不自动 trim 非空记录。INTEGER 采用有符号十进制整数文本并检查 BIGINT 溢出；DECIMAL/RANGE 最多 28 位整数、10 位小数，数据不接受科学计数法或多余前后空格。小数点后必须有数字，也允许 .5。
- 日期须严格满足四位年、两位月、两位日和有效日期；例如 2024-02-29 合格、2023-02-29 不合格。
- RANGE 中不能按约定十进制格式解析（含超精度）的值也记异常，nonNumericCount 是 invalidCount 的子集，不应再次相加。边界为包含关系。
- REGEX 以严格起止锚点包裹；尾随换行不会被宽松的美元结束锚点接受。支持的复杂正则未宣称无性能风险，未做病态表达式压力测试。
- 规则定义版本独立于 CSV 读取版本。新任务仍为 csv-v2，历史 csv-v1 的读取语义和复检兼容保留。
- 治理仍只有移除空值和按业务键去重；不会默默填充、转换或修复新增规则异常。选中数据中不存在的治理规则时，在入队前返回 400。

## 实际测试

| 检查 | 本次结果 |
| --- | --- |
| Maven package、后端和前端镜像构建 | 成功；11 项单元测试，0 失败、0 错误 |
| tests/rule_templates.py | 通过：已知异常统计/预览，模板修订 1→2、陈旧修订 409、历史读取、归档、参数快照、复检、治理、报告及非法请求拒绝 |
| 精度边界 tests/precise_bounds.py | 通过：28 位整数/10 位小数的 JSON 数字精确保留，Spark 区分相差 0.0000000001 的两个值 |
| tests/rules_persist.py | 最终容器重建后、安全重启前后通过：模板、归档、修订、参数、来源与八条规则报告保持 |
| tests/e2e.py | 原 MVP 上传、空值、重复、复合/空键和异常预览通过 |
| tests/rerun.py | 原规则、新数据、旧任务兼容、参数/结果独立及拒绝请求通过 |
| tests/governance.py | 原治理闭环、空版本、CSV 保真和版本连续治理通过 |
| tests/edgecases.py | 大小写、count 字段、空白行、BOM/换行通过 |
| tests/csv_compat.py | 真实历史 csv-v1 快照复检指标保持，旧口径治理明确拒绝 |
| tests/operational.py | 环境文件保留、超限上传 413、10000 行样本通过；最新耗时 5698 ms |
| 真实浏览器 | 保存新模板、将 age 上界 120 改为 30、保存修订 2、未保存修改禁用检测、按修订 2 提交并完成 |

单元测试使用 mock 检查失败分支和配置校验；所有检测计数验收均使用独立 Spark 3.5.8 JVM 与真实 MySQL。模板冲突测试验证陈旧 expectedRevision 拒绝，未进行多客户端并发压力测试。没有重新强杀任务或等待 300 秒制造超时。

## 可核对证据

- 八条规则来源任务：3fe7c196-7300-4a6c-95f3-e45c40a542fc，10 行，7128 ms。
- 模板：6f5c809e-4d22-4967-a082-f1457ee89f0e，来源使用修订 1。修订 2 将 age 上界变为 30，旧任务复检仍保持原来的 120。
- 治理任务：47dd4d0f-fbe4-4df8-b017-3fc52d380449，10→6 行；整数类型异常仍为 2，格式异常仍为 1，反映当前策略并未修复这些值。
- 页面任务：113f1990-0769-4ccf-b8b9-b0530668ee94，模板“页面验收-数据规则”修订 2，6681 ms；age 范围异常为 5。

| 来源修订 1 的规则 | 真实指标 |
| --- | --- |
| DUPLICATE · id | {'excessRows': 1, 'duplicateRows': 2, 'duplicateGroups': 1} |
| MISSING · age | {'nullCount': 1, 'blankCount': 3, 'emptyCount': 1, 'whitespaceCount': 1} |
| RANGE · amount | {'checkedCount': 7, 'invalidCount': 4, 'nonNumericCount': 2, 'skippedBlankCount': 3} |
| RANGE · age | {'checkedCount': 7, 'invalidCount': 4, 'nonNumericCount': 1, 'skippedBlankCount': 3} |
| REGEX · code | {'checkedCount': 7, 'invalidCount': 2, 'skippedBlankCount': 3} |
| TYPE · amount | {'checkedCount': 7, 'invalidCount': 2, 'skippedBlankCount': 3} |
| TYPE · joined | {'checkedCount': 7, 'invalidCount': 3, 'skippedBlankCount': 3} |
| TYPE · age | {'checkedCount': 7, 'invalidCount': 2, 'skippedBlankCount': 3} |

## 改动文件

- 新增 backend/src/main/java/cn/sparkdq/Rules.java、RuleTemplates.java、RuleTemplateApi.java。
- 修改 Tasks.java：完整规则参数、模板来源、快照复检和治理校验；Reports.java：按参数匹配新指标。
- schema.sql 追加 rule_parameters、rule_template_heads、rule_template_versions、task_template_refs；不修改旧表、不删除旧数据。
- application.properties：精确 JSON 小数解析。
- spark-engine/src/main/java/cn/sparkdq/engine/Engine.java：TYPE/RANGE/REGEX 的实际 Spark 检测。
- 新增 frontend/src/RuleManager.vue，更新 App.vue 的编辑、指标/参数展示。
- RulesTest.java、ReportsTest.java；新增 tests/rule_templates.py、rules_persist.py、precise_bounds.py。
- README、development、semantics、roadmap 与本日志。

## 接口和复现

模板：GET/POST /api/rule-templates；GET/PUT /api/rule-templates/{id}；GET /api/rule-templates/{id}/versions；DELETE /api/rule-templates/{id}?revision=N 仅归档。

模板创建/更新参数为 name、rules，更新另需 expectedRevision。每条规则含 type、columns、params。任务 POST /api/tasks 可传 datasetId+rules，或 datasetId+templateId+可选 templateRevision，二者不能混用；原 fields/keys 简写兼容。

在 WSL 项目已有 Docker 权限的会话执行 infrastructure/dev.sh build、up，健康后运行 timeout 360s python3 tests/rule_templates.py，timeout 90s python3 tests/precise_bounds.py；rules_persist.py 需前者生成 .runtime/rule_templates.json 后执行，重启只在无活动任务时进行。其余测试按 README 执行。

## 问题修复及限制

- UI 辅助脚本的一处引号错误、精度样本的一处换行转义错误已修正；后者在上传校验阶段被拒绝，未误报 Spark 成功。
- 旧规则参数默认空对象，新规则参数独立存表并进入快照；归档通过软标记，未提供取消归档页面。
- 模板按字段名复用，暂不支持字段映射；列表最多 200 项，历史修订暂无分页。单任务最多 101 条规则，最多一条重复规则。
- 当前仍为 Spark local[2]、单后端、串行任务、20 MiB 上限；未做集群、大规模或病态正则性能验证。Vite 大资源块提示仍存在。
- WSL 空闲退出可能停止服务，本次保留有时限的会话验证；未改系统自启动/权限。
- 未推送远程、发布、删除数据卷、重置仓库、输出凭证或修改 Windows 旧 Java/Spark/MySQL。

## 下一阶段

规则管理基础已完成。后续优先推进可配置填充/映射/标准化治理及保留策略，或任务取消、分页、迁移、文件对账和备份恢复，再依据真实数据规模评估集群扩展。
