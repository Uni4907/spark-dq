# SparkDQ 治理闭环开发日志

2026-10-18（Asia/Shanghai）。开发前 HEAD 为 df68478，工作区干净。真实仓库：SparkDQ-Ubuntu 的 /home/sparkdq/projects/spark-dq。

## 本阶段交付

现在可完成：配置规则 → Spark 检测 → 选择治理策略 → Spark 生成独立 CSV 版本 → 读取生成文件实际复检 → MySQL 登记版本/策略/结果 → 页面指标对比和 JSON 报告下载。

- 策略：移除命中任一空值规则的整条记录，以及按业务键去重。先移除空值，再去重；策略冻结在治理任务中。
- 去重按全部字段字典序选择一条，NULL 排在前面，不表示保留最新或业务最优记录。输出可能为空。
- 使用真实 Spark DataFrame filter、Window/row_number、CSV writer 和独立 JVM；原文件和以前版本不修改。
- 指标基于生成的真实 CSV 文件重新读取计算，独立复检可再次验证。
- 追加 governance_runs、dataset_versions 两张表。版本和成功结果在同一事务登记；SQL 失败回滚元数据，但可能保留未登记的任务输出/复制文件供排查，不自动删除。
- 治理任务的 dataset_id 和输入 SHA 指向本次输入；输出数据集、行数、指纹和父版本独立记录。再次治理或默认复检治理任务时，使用已生成版本。
- 数据集详情提供父版本和直接子版本，页面可导航、下载 CSV 与 JSON 报告。版本以独立 ID 和父关系追溯，尚无图形化版本树。
- 报告按同一规则定义比较，保留负的“减少量”表示恶化；不把重叠规则异常相加成总异常数，同时展示前后行数和策略。

## CSV 口径兼容

新检测任务使用 csv-v2，正确处理 RFC4180 双引号转义。导出保留纯空格，以带引号空字段区分空字符串和 NULL，保留逗号、引号与字段内换行。

旧任务和快照 csv-v1 复检保留原读取行为，不静默换口径。直接治理旧任务返回 409，提示先对原数据集新建检测。没有快照的旧任务可复检，新快照标为 csv-v1。报告只在兼容口径间比较。

## 实际验收

| 检查 | 结果 |
| --- | --- |
| Maven package、Docker 后端/前端构建 | 成功；4 项 Java 单元测试，0 失败、0 错误 |
| tests/governance.py | 通过：7→2 行、源字节/历史结果不变、输出指纹、版本关系、真实文件复检、报告 |
| 仅去重 | 7→6 行；name 空值仍为 3，email 空值为 1，重复涉及行 0 |
| 连续治理与默认复检 | 使用上次输出作为输入，父子版本正确 |
| 全部记录被移除 | 3→0 行，保留字段/表头，零指标，可独立复检 |
| CSV 保真 | 引号、逗号、换行、纯空格、空字符串、NULL 保持；独立复检指标一致 |
| tests/e2e.py | 既有上传、7 行已知异常、复合/空键、清洁数据、异常预览通过 |
| tests/rerun.py | 原规则/新数据复检、历史保留、400/404/409 与旧任务通过 |
| tests/edgecases.py | 大小写、count 业务键、空白物理行、BOM/字段内换行通过 |
| tests/csv_compat.py | 实际 csv-v1 快照复检指标保持；治理旧口径明确拒绝 |
| tests/operational.py | 环境文件生成/保留、超限上传 413、10000 行样本通过；最新任务 5071 ms |
| tests/governance_persist.py | 安全重启前后通过：策略、版本、输出字节/指纹、复检与报告保持 |

单元测试使用 mock 验证输入指纹失败落库、停止调度器拒绝、报告匹配与恶化指标；真实闭环使用 MySQL 和独立 Spark 3.5.8，未用 mock 计算。重启前确认没有 QUEUED/RUNNING 任务。未重新强杀任务或等待 300 秒制造超时。

## 发现并修复

- 一次方法签名替换错误导致编译失败，修正后构建和单测成功。
- 第一轮 CSV 保真验收发现空字符串导出成未加引号空字段，读取后成为 NULL。修正 emptyValue 引号写出后完整重跑成功。早期合成测试版本保留，没有改写用户文件。
- WSL 非交互会话结束后服务仍可能停止，本次以有时限的前台会话验收；未改系统、Docker 权限或自启动。
- Vite 仍有单块超过 500 kB 提示，未新增依赖，也未宣称完成生产部署。

## 改动文件

- backend/src/main/java/cn/sparkdq/Tasks.java：治理、口径、输出校验和版本事务。
- backend/src/main/java/cn/sparkdq/Reports.java：报告与逐规则比较。
- backend/src/main/java/cn/sparkdq/Datasets.java：共享登记、空派生版本和父子版本查询。
- backend/src/main/java/cn/sparkdq/TaskApi.java、DatasetApi.java：治理、报告与下载接口。
- backend/src/main/resources/schema.sql：追加两张表。
- spark-engine/src/main/java/cn/sparkdq/engine/Engine.java：Spark 治理、确定性去重、CSV 兼容与实际文件复检。
- frontend/src/App.vue：治理策略、版本导航、对比表与下载。
- backend/src/test/java/cn/sparkdq/ReportsTest.java：规则匹配与恶化测试。
- tests/governance.py、governance_persist.py、csv_compat.py：真实闭环、持久化和兼容验收。
- tests/rerun.py：新建任务口径验收更新。
- README.md、docs/development.md、docs/governance.md、docs/roadmap.md：说明与计划。

## 接口及复现

POST /api/tasks/{成功任务ID}/govern，JSON 参数：{"dropMissing":true,"deduplicate":true}。
GET /api/tasks/{任务ID}/report 下载 JSON。
GET /api/datasets/{数据集ID}/download 下载 CSV。

在有现有 Docker 权限的 WSL 项目会话中，执行原来的 infrastructure/dev.sh build、up。构建运行单元测试。健康接口可用后依次运行上述 Python 测试，每个测试可用 timeout 约束在 90～360 秒。完整命令见 README。

governance_persist.py 依赖 governance.py 创建的 .runtime/governance.json；重启前必须无活动任务，再运行同一只读脚本复验。测试新增的均为合成样本，不清除历史。

## 真实闭环证据

- 来源任务：5e44a2ef-63ca-4584-a3b3-2ae66332507d
- 治理任务：7515b266-5c34-42f3-bf3a-c0a939bdf61a，5429 ms。
- 新版本数据集：ac7c4c64-9760-4dda-9571-d3f0aed31fcf
- 输入 7 行 → 输出 2 行，移除 5 行。
- name 空值 3→0，email 空值 2→0，重复涉及行 2→0。
- 该合成样本的真实报告与 CSV 已导出到聊天输出目录。

## 长期计划与边界

MVP、快照/复检和治理/版本/报告基础闭环均完成。下一阶段优先规则模板与类型/范围/正则规则，再扩展填充/映射治理，随后完善迁移、分页、取消、文件对账、备份和长期启动。

当前仍是单后端、串行任务、Spark local[2]、20 MiB 上限。治理单分片 CSV 适用于当前上限，不代表大规模分布式导出。没有集群提交/故障协调、真实规模基准、PDF/Word 报告或业务专用保留策略。

未推送远程、对外发布、删除数据卷、重置仓库、读取凭证内容或改动 Windows 旧 Java/Spark/MySQL。

真实浏览器验收：页面选择两种策略并提交，任务 5fb9aea2-a272-47f4-88b1-60a9d6a94dac 成功，5472 ms，7→2 行；新版本 cde40bcb-2db9-427f-8242-47fda90514a8，指标对比和下载入口显示正确。截图：SparkDQ-治理闭环验收.png。
