# 开发记录

2026-10-16：检查 SparkDQ-Ubuntu：项目目录为空；WSL 11 GiB RAM、4 GiB swap；Windows C 盘剩余约 117 GiB。未改 Windows Java/Spark/MySQL。用户授权项目专用 Docker 管理操作。宿主 Java/Node 缺失，Maven 指向 Windows，故使用容器隔离。目录、父 Maven 工程及 MySQL 命名卷已建立，待实际验证。

阶段 1 验收：Docker 镜像内 Maven 3.9.12 / Java 17.0.18；父工程及两模块 mvn package 成功。Compose MySQL healthy；GET /api/health 返回 UP / MySQL 8.4.8。首轮依赖下载构建 7 分 29 秒。

阶段 2 验收：容器内 mvn package 成功；python3 tests/e2e.py --uploads-only 通过。覆盖空文件、重复/保留表头、列数不匹配、无数据、引号未闭合、非法 UTF-8、非 CSV 扩展名拒绝；BOM 与带换行引号字段接收；7 行样本注册、SHA-256、字段预览查询正确。原文件只读保留。

阶段 3 边界回归发现并修复：业务键名 count 与 Spark 聚合 count 列冲突，改用保留内部别名；CSV 注册原先计入空白物理行而 Spark 跳过，现统一忽略空白物理行（带引号的空字符串仍保留）；Spark 开启字段名大小写敏感，与上传校验一致；失败原因增加异常摘要，完整日志保留在文件卷。

重建测试发现：优雅停止中断线程的 InterruptedException 没有 message，原先保存为 null 字符串；现明确记录“服务停止中断任务，请重新提交”。恢复验收另用 SIGKILL 模拟崩溃，验证启动时修复排队和运行任务。

阶段 3 验收：完整 e2e、edgecases 均通过。7 行样本 name=1 null+1 empty+1 whitespace，email=2 null；重复 1 组/2 行/1 多余。复合及 null 键、清洁数据、count 字段、大小写字段、空白物理行、BOM/多行均通过真实 Spark 3.5.8 检测。lifecycle 验收通过：缺失合成输入保存真实 Spark 原因及耗时且无半份 SQL 结果；SIGKILL 后 down/up，排队/运行任务变 FAILED；MySQL 统计、异常预览、原文件 SHA-256 均保留。样本成功任务 a5b2281f-91f2-4c3b-9fd4-46070635e8b2，耗时 7555 ms。

阶段 4 构建记录：npm ci + Vite build 成功；非 root preview 初次启动发现 .vite-temp 无写权限，修复该临时目录所有者；按需导入柱状图减少打包体积。Vue/Vite 同主版本修复补丁复审后锁为 3.5.44/7.3.7，ECharts 5 告警及规避路径见 versions.md。

阶段 4 验收完成：真实浏览器选择规则并提交成功；另从浏览器上传 known-issues.csv 后完成任务 18a721cf-c210-4648-bb0f-28ab3a46915e，7 行统计和 name 三条异常预览正确。页面截图已导出。operational：环境生成/权限/不覆盖、20 MiB 返回 413、一万条合成数据均通过（7921 ms）。graceful_stop：正常停止恢复并保存明确中断原因通过。最终前端锁定版本保持 Vue3/Vite7/ECharts5，残余 ECharts 告警详见版本文档。

2026-10-18：完成规则快照、输入指纹校验、来源关系与原规则复检，真实 Spark MVP/复检/边界/一万行测试和重启持久化均通过。详细文件、证据与限制见 [无人值守开发日志](development-2026-10-18.md)。

2026-10-18（后续阶段）：完成 Spark 治理→独立版本→真实文件复检→指标比较/报告闭环，CSV 保真、空版本、旧快照兼容、重启与现有回归均通过。详见 [治理闭环日志](governance.md)。

2026-10-18（规则阶段）：完成版本化模板、TYPE/RANGE/REGEX、完整参数快照、复检/治理/报告联动，11 项单元测试与真实 Spark、精确边界、重启及原有回归通过。详见 [规则阶段日志](rules.md)。

2026-10-19（治理扩展）：完成按字段常量填充、5 种文本标准化、固定顺序与逐操作审计，保留历史策略并阻止 CSV 空值标记碰撞。16 项单元测试、5 套真实 Spark 流程、重启持久化及浏览器提交均通过；7→5 行合成样本和报告已导出。详见 [填充与标准化日志](governance-transforms.md)。

2026-10-19（可靠性阶段）：完成排队/运行任务取消、持久化取消中/已取消、独立进程清理重试及取消后复检；增加任务/数据集分页、状态筛选和字面名称搜索。31 项 Java 测试、真实 Spark 进程/8 槽容量验收、5 套既有流程、优雅停止、实际取消中恢复及页面操作通过。详见 [取消与分页记录](reliability.md)。

2026-10-19（存储可靠性）：完成离线只读对账、数据库+文件+实际应用包一致备份、校验和新卷隔离恢复。14 项标准库测试通过；142 数据集/190 任务/12 表及全部归档文件核对，恢复后真实 Spark 五规则复检、原服务数据隔离和异常探针对账通过；现有 MVP/历史持久化检查通过。详见 [存储阶段记录](storage.md)。

2026-10-19（数据库版本管理）：完成 Flyway V1/V2、完整旧库结构保护、迁移失败先于任务恢复、12/13 表备份兼容与实际升级；34 项 Java、15 项存储测试、隔离失败场景、旧数据真实 Spark 和主服务回归通过。详见 [数据库迁移阶段日志](development-2026-10-19-migrations.md)。

2026-10-19（运行维护）：完成独立 Windows/WSL 限时后台会话、有任务停止保护/到期等待、备份完整性/新鲜度/容量和保留建议。32 项 Python 测试、真实 10 万行 Spark 生命周期、两份备份完整校验及既有回归通过；已开启 12 小时会话，未配置开机/定时任务。详见 [本阶段日志](development-2026-10-19-operations.md)。

2026-10-19（维护备份）：完成受保护维护准入、单次一致备份/完整校验/同容器恢复与私有回执续恢复。42 项 Java、48 项 Python、隔离真实中断/失败路径、主库受控备份/隔离恢复与 Spark 回归通过，原数据不变。详见 [维护备份阶段记录](development-2026-10-19-maintenance.md)。

2026-10-19（值映射治理）：完成策略v3精确映射、深度快照、Spark非连锁替换和逐项审计；48项Java/48项Python、隔离与主环境真实验收、v2兼容、浏览器提交及受控重启持久化通过。详见 [值映射阶段记录](development-2026-10-19-value-mappings.md)。

2026-10-19（业务保留顺序）：完成v4多级文本/精确数值/日期保留优先级、空值位置、稳定并列和审计；55项Java/48项Python、隔离与主环境真实验收、旧v2/v3兼容和跨重启持久化通过。详见 [业务保留顺序日志](development-2026-10-19-retention.md)。

2026-10-19（单机基准）：完成固定种子1万/5万/20万行、1/2/4输入分区及3种流程的27条件/54次正式测量，另3次预热；59项Java、56项Python、候选默认API/重启与真实超时探针通过，主数据不变。详见 [基准阶段记录](development-2026-10-19-benchmark.md)。

2026-10-19（输入格式基础）：离线清单固定的多文件CSV/全字符串Parquet准备与读取、双向保真/逐文件指纹和兼容旧CSV；66项Java/61项Python、24正式格式对照/6特殊文本/6空版本/6真实失败及隔离默认API/重启回归通过。主应用未替换，Web格式注册待接入。详见 [本阶段开发记录](development-2026-10-19-prepared-inputs.md)。

## 2026-10-19 执行副本任务接入

已完成网页选择、V3快照、共享预算/取消恢复、治理复检与备份对账。74项Java/66项Python及真实API、故障、恢复、主环境重启验证通过。见[开发日志](development-2026-10-19-execution-inputs.md)及[完整进度交付](progress-delivery-execution-inputs-2026-10-19.md)。

## 2026-10-19 多CSV分片注册与来源追溯

受控2..16份分片、V4来源清单/原分片下载、备份对账及真实治理复检已上线。79项Java、69项Python通过。记录事务代理启动修复与旧验收地址误路由修复。见[开发日志](development-2026-10-19-csv-parts.md)、[完整进度](progress-delivery-csv-parts-2026-10-19.md)、[接口说明](csv-parts.md)。

## 2026-10-19 原生Parquet离线导入基础

Java/Spark精确标量转换、15类拒绝、指纹/COMPLETE与全多重集回读完成。86项Java/75项工具测试通过，4项隔离真实Spark业务任务和重启核验通过。主环境V4/业务行/文件/镜像保持不变。见[完整进度](progress-delivery-native-parquet-2026-10-19.md)、[开发日志](development-2026-10-19-native-parquet.md)、[契约与复现](native-parquet.md)。

## 2026-10-20 原生Parquet在线导入

V5持久化导入、网页/幂等/取消/重试/恢复、来源下载和备份对账已上线。98项Java、83项运维测试及真实业务/300秒超时/新卷恢复验证通过。详见[开发日志](development-2026-10-20-native-imports.md)与[完整进度](progress-delivery-native-imports-2026-10-20.md)。

## 2026-10-20 备份巡检与业务恢复点

修复V5备份被巡检误拒绝，新增只读一致业务指纹、覆盖变化判据和恢复前实际指纹校验。103项运维测试、同数量改值识别、错误恢复点拒绝、主新备份/全新卷恢复通过；主业务/文件不变。详见[日志](development-2026-10-20-recovery-points.md)和[完整交付](progress-delivery-recovery-points-2026-10-20.md)。

## 2026-10-20 同机Spark Standalone

完成隔离1 Master/2 Worker/Client Driver真实检测和治理、取消资源释放、复检及错误清单拒绝；107项Java、114项运维测试、4项候选local API任务及重启通过。主数据/镜像不变，尚未接入线上集群API。见[完整进度](progress-delivery-standalone-2026-10-20.md)、[开发日志](development-2026-10-20-standalone.md)、[架构与复现](standalone.md)。

## 2026-10-20 作业身份与只读对账

新增请求绑定的持久化attempt/applicationId与Driver回执，任务详情/报告读取及Standalone只读对账；117项Java、124项运维测试、真实集群SIGKILL身份保留和候选API/重启验证通过。线上仍local，尚无自动集群协调。见[完整进度](progress-delivery-execution-receipts-2026-10-20.md)、[开发日志](development-2026-10-20-execution-receipts.md)、[契约与复现](execution-receipts.md)。

## 2026-10-20 受控Standalone HTTP执行

现有HTTP检测/治理/复检接入同机固定集群，新增全节点清理门禁、故障待核对和重启恢复协调；126项Java、128项运维测试及真实HTTP/通信故障验收通过。主环境仍local。见[完整进度](progress-delivery-cluster-api-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-api.md)、[架构与复现](cluster-api.md)。

## 2026-10-20 隔离集群演示与浏览器闭环

新增可维护六服务隔离部署、有限会话/断联工具、执行状态与清理门禁展示，修复任务切换图表生命周期及历史CLEAR覆盖当前观察。130项Java、136项运维、8项Node测试通过；真实浏览器检测→治理→复检、断联取消及同卷重启核验通过。主数据/镜像不变，仍local；跨物理机/HA/集群性能加速尚未验收。见[完整进度](progress-delivery-cluster-ui-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-ui.md)、[部署指南](cluster-lab.md)。

## 2026-10-20 local / 同机集群配对基准

新增相同输入/规则/聚合配额下的local与双Worker对照、cgroup资源采样及完整证据图表。1万/20万行检测与治理各两次，16正式+2预热全部正确性通过；所有远程试次两个Worker均完成任务。148项运维测试通过。本轮集群耗时更长，不宣称提速，主数据/文件/镜像不变。见[完整进度](progress-delivery-cluster-compare-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-compare.md)、[复现指南](cluster-compare.md)。

## 2026-10-20 三格式与转换成本对照

新增源读取分区/重分区观测，完成相同20万行的两模式三格式检测/治理配对、转换成本及严格摊平模型。130项Java、159项运维、28次真实Spark运行通过；全量结果一致，主数据/镜像不变。未接入跨任务缓存或改变生产格式。见[完整进度](progress-delivery-cluster-formats-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-formats.md)、[复现指南](cluster-formats.md)。

## 2026-10-20 分片CSV表头验证优化

新增逐文件有界标准表头核对及Spark兼容回退；135项Java、166项运维测试、22兼容/拒绝用例与20次A/B运行完成，主数据/镜像不变。候选JAR离线验收，尚未上线服务。见[完整进度](progress-delivery-cluster-headers-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-headers.md)、[复现指南](cluster-headers.md)。

## 2026-10-20 优化候选服务与恢复验收

135项Java、181项运维测试；新镜像11个HTTP任务（10成功/1预期中断失败）、整套重启、1万行双Worker及故障后复检通过。新增镜像选择/独立网段/私有启动日志，修复重启后地址陈旧。主数据/镜像保持，候选运行于隔离环境。见[完整进度](progress-delivery-header-service-2026-10-20.md)、[开发日志](development-2026-10-20-header-service.md)、[复现](header-service-acceptance.md)。

## 2026-10-20 受控连续运行

191项运维测试；三轮两格式闭环、排队/运行取消及重放共22HTTP任务（20成功/2预期取消），11次空闲资源核对、全量异常与治理预期/历史报告/版本字节验证通过。主数据/镜像保持。属于短时观察，不宣称多日稳定或无泄漏。见[完整进度](progress-delivery-cluster-stability-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-stability.md)、[复现](cluster-stability.md)。

## 2026-10-20 5万/20万行HTTP闭环

196项运维测试；两规模CSV/Parquet检测→治理→复检共12HTTP成功、12本地准备verified及12双Worker应用正常结束，6次空闲核对、全量异常/治理/历史报告/版本指纹通过。保持20MiB和300秒限制，主数据/镜像不变。单次串行观察不等同生产容量或超时故障证明。见[完整进度](progress-delivery-cluster-scale-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-scale.md)、[复现](cluster-scale.md)。

## 2026-10-20 受控超时与恢复

202项运维测试；独立60秒预算下5HTTP（3成功/2预期超时FAILED）、7次空闲检查、4次资源待核对准入/报告409通过。准备超时无Engine回执，检测超时保留原身份；Master恢复后才发布失败，两次复检及后端重启持久性通过。主300秒预算/数据/镜像不变。见[完整进度](progress-delivery-cluster-timeout-2026-10-20.md)、[开发日志](development-2026-10-20-cluster-timeout.md)、[复现](cluster-timeout.md)。

## 2026-10-20 可阅读质量报告

新增独立可阅读/离线HTML报告及任务详情入口，支持浏览器打印另存PDF；涵盖统计/前后对比/治理策略审计/版本追溯，失败取消不伪装成功。143项Java、8项Node、202项运维测试及真实检测→治理→复检/失败/取消/浏览器验证通过。主数据和部署保持。见[完整进度](progress-delivery-report-documents-2026-10-20.md)、[开发日志](development-2026-10-20-report-documents.md)、[使用说明](report-documents.md)。

## 2026-10-20 数据集历史与版本追踪

新增数据集关联任务分页/筛选、输入与输出角色区分、父子版本/生成与来源任务导航及逐规则质量变化。147项Java、14项Node、真实9/2/1数据集历史及浏览器分页/跳转/对比通过。主部署/数据保持。见[完整进度](progress-delivery-dataset-history-2026-10-20.md)、[开发日志](development-2026-10-20-dataset-history.md)、[使用说明](dataset-history.md)。

## 2026-10-20 历史治理策略复用

新增历史成功策略搜索/分页、目标兼容校验、带入/撤销/修改后执行；不复制来源检测规则，不改原快照。155 Java、17 Node及实际浏览器治理通过，新值REUSED_NAME写入500行版本，原策略UNKNOWN保持。主部署/数据保持。见[完整进度](progress-delivery-governance-reuse-2026-10-20.md)、[开发日志](development-2026-10-20-governance-reuse.md)、[使用说明](governance-reuse.md)。
