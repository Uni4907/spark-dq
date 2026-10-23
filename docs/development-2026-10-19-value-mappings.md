# 精确值映射阶段开发记录

完成记录时间：2026-10-19T11:43:08+08:00。真实项目 /home/sparkdq/projects/spark-dq；本阶段起点 main/fbcca25。没有远程推送、对外发布、删卷、删除用户数据或覆盖原卷恢复。

## 改动与行为

- Tasks.java：严格字符串映射项反序列化，六字段治理请求与旧构造兼容；空 mappings 不进入旧 v2 JSON。
- GovernancePolicies.java：v3 校验、按字段/来源排序、嵌套不可变快照、每字段50/合计200项及40000字符预算，拒绝重复来源、NUL、保留标记、无效 Unicode。
- Engine.java：标准化后以 Spark DataFrame 同时执行精确映射，通过单次阶段聚合统计字段及逐项命中/改变行数；填充、移除、去重、实际输出文件复检继续执行。
- GovernancePanel.vue、App.vue：映射字段/来源/目标表单，重复/预算校验，逐项审计、空字符串/纯空格展示；报告沿用持久化策略和审计。
- ValueMappingsTest.java：新增6项；tests/value_mappings.py：新增真实 HTTP/Spark 综合验收及只读持久化核对。
- tests/governance_transforms.py：只将“未知版本”探针从现已支持的3改为仍未支持的4，保留全部 v2 快照和实际行为断言。
- README、roadmap、value-mappings.md、本日志与完整交付报告：使用方法、范围、证据及后续安排。

## 本轮真实验证

Java 48项测试通过（原42+新增6），Maven backend/spark-engine package 成功；Python 48项现有单元测试通过；前后端候选镜像构建和 Vue/Vite 构建成功，无新依赖或数据库迁移。

隔离新项目从上一阶段受控备份恢复，原业务行哈希不变。完整值映射验收通过：严格请求拒绝且不入队、非连锁精确匹配、标准化→映射→填充、NULL与引号空字符串、中文/换行/引号/反斜线、无匹配/no-op、组合移除和去重、零行输出、复检、JSON报告和原数据不变。10行映射样本匹配8行/改变7行，组合治理10→7行，空输出治理10→0行。

隔离映射任务 bfafb225-3afb-4102-b641-2b26491e0311，组合任务 27f5fd9a-1ced-4258-b11f-1bf6a3bd28f3；夹具 /home/sparkdq/projects/spark-dq/.runtime/value-mappings-9e7b7401456e.json。实际浏览器提交任务22715643-0c93-4e54-89d8-4cd11f3589d4成功，status字段2项映射均命中1行、改变1行，页面前后规则比较如实显示映射引入的新格式异常；截图已导出。

隔离重建后端前预置同 owner 的私有维护标识，确认新容器启动仍拒绝写入503；原业务行、所有文件及 .env 不变，解除保护后正常读取。主环境同样在维护保护下先停机一致备份，再更换代码镜像、验证并解除保护。部署回执：/home/sparkdq/projects/spark-dq/.runtime/value-mapping-deploy-e9bb92989a65/receipt.json；部署前备份：/home/sparkdq/projects/spark-dq/.runtime/value-mapping-deploy-e9bb92989a65/backup。

主环境本轮重新执行 MVP e2e（CSV上传、7行空值/重复、复合/NULL业务键、清洁数据、异常预览）、完整 v2 文本标准化/填充治理验收、完整 v3 值映射验收。主库新映射任务 f5e53489-88d5-498f-a248-5599df6db505；组合任务 40ff1ec0-84cd-4c80-9b58-5a8ccf4b5c6d；复检 b1ad272a-12d3-45bf-9a37-b7c31c1d535f。

之后再执行受控备份并停止/启动同一后端，回执完成且自动恢复可写。v2/v3在重启前后均核对；历史 governance/rules/cancellation 持久化检查全部通过。规则、策略、逐项审计、版本CSV摘要、结果、报告保留，无活动任务、无快照指纹不符、无成功规则缺结果。重启备份 /home/sparkdq/projects/spark-dq/.runtime/value-mapping-persist-5baa838da853/managed-backup/backup；主验收夹具 /home/sparkdq/projects/spark-dq/.runtime/value-mappings-a50820e6d81d.json。

当前 179 个数据集、228 个任务、12张业务表+迁移历史表（2行）；schemaVersion=2，maintenance=false，健康UP。测试记录包含预期失败/取消的历史样本，不能把所有记录说成成功任务。

## 失败运行和修正

首轮新验收的汇总对普通任务 governance=null 调用 get，修正为兼容None后完整重跑；真实计算已先通过，不将失败汇总算作完成。升级演练最初卷查询缺参数、Compose create不支持--no-deps，均只发生于隔离副本；保留现场、启动同副本按owner恢复、改为 up --no-start 后完整演练通过。主环境未受这些演练错误影响。

主环境首次 v2 回归因测试仍将 v3 当未知版本而失败，新接口按设计接受；探针更新为v4后完整重跑v2。首轮主回归日志保留为 .runtime/value-mapping-main-*-first-failure.log。未放宽 v2 精确策略形状/CSV/计数断言。

## 留存与后续

本阶段所有备份、合成数据与失败现场保留。隔离容器在验收后停止，卷不删除。导出仅源代码、合成样本、报告、日志和摘要，不导出 .env、维护密钥、业务数据库SQL或备份tar。

下一业务增量建议：可配置去重保留优先级（排序字段/方向/NULL与并列处理），继续快照、审计、独立版本与复检。随后补真实性能基准、集群执行/共享存储与状态协调、最终论文/答辩材料；不把本机 local[2] 宣称成集群能力。
