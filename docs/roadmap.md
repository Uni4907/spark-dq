# SparkDQ 长期路线

当前执行与证据口径以 [2026-10-21 T0 / T1-A 状态](current-status-2026-10-21.md) 为准。历史节点保留，不将计划视作通过。

按可运行、可验证的小阶段推进，每阶段记录真实测试，不以架构名称替代完成能力。

| 阶段 | 状态 | 交付/验收目标 |
| --- | --- | --- |
| CSV→Spark→持久化→前端 | 已完成 | 已知异常、异常预览、恢复 |
| 快照与复检 | 已完成 | 独立 ID、指纹、原规则与来源 |
| 治理→版本→复检→报告 | 已完成基础闭环 | 显式策略、新文件、前后指标、JSON 报告 |
| 规则管理 | 已完成基础阶段 | 模板 CRUD、类型/范围/正则、规则版本与真实 Spark 测试 |
| 治理策略扩展 | 已完成常量填充/标准化/映射/业务保留顺序 | 策略快照、逐操作审计、真实文件复检；v3 非连锁值映射与逐项计数已完成；v4可配置业务保留优先级已接入，单机系统性规模基准已完成，同机双Worker检测/治理已验证，已完成受控同机HTTP执行/清理协调；隔离部署模板/浏览器闭环已完成，跨主机待做 |
| 运维可靠性 | 已完成本地运行与备份基础 | 队列释放、真实进程停止、取消恢复、历史分页；只读对账/一致备份/同机隔离恢复已完成；数据库版本迁移与旧库接入已完成；有期限后台会话、备份只读巡检/容量提醒已完成；空闲维护准入/单次备份自动恢复已完成；V5巡检/业务恢复点对账与恢复指纹校验已补齐；开机自启/续期/定时备份待做 |
| 大规模运行 | 已建立单机基准与离线多格式输入基础，同机集群已最小验证 | 1万/5万/20万行及1/2/4分区基准已完成；任务可选多文件CSV/全字符串Parquet执行副本、快照/一次性SHA绑定、治理复检、取消恢复与备份对账已接入；受控多CSV注册/来源下载/备份对账已完成；原生Parquet精确标量转换/离线保真校验已完成；有状态导入/网页原生注册、幂等/取消/恢复/来源备份已接入V5；同机Standalone双Worker检测/治理与取消/复检已验证；受控同机HTTP提交/取消/恢复清理已验收；更大规模、同机隔离部署/浏览器故障闭环、local/集群1万与20万行配对资源对照已完成；本轮未提速，跨主机共享存储与更大规模待完成 |
| 毕设交付 | 持续积累 | 架构/ER/API、数据集证据、可复现实验、论文图表与演示 |

先完成规则管理和可靠性，再按实际规模扩展。按当前目标补齐 IQR/FD 及 CSV/MySQL/Hive；Hive业务源尚未实现，HDFS/Kafka等仅有明确需求时引入，不提前宣称分布式性能。

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

## 2026-10-21 按新审查契约校准

IQR/FD及CSV、MySQL、Hive为当前用户指定目标。B1 IQR已完成精确小样本贯通（docs/iqr.md）；B2 FD待做；B3效果与性能实验待做。MySQL仅为元数据库，Hive未接入；此前暂不引入Hive的安排不能代替当前目标。治理预览、命名方案库、云采购等后移。

B2 FD已完成配置式组合依赖、冲突参与行与全跳过边界及重启持久性验收，见docs/fd.md。B3尚待实际执行，不以正确性oracle吻合冒称业务准确率。

## 2026-10-21 当前目标检查点

B1 IQR、B2配置式FD已完成；B3独立正确性/三种子18效果场景/三规模每算法3正式+1预热已完成，最终证据和严格数值域修复见docs/algorithm-experiments.md。新算法本轮只验证local，不能借旧五规则的同机集群记录宣称集群已验收。B4只盘点MySQL业务源/Hive差距，二者仍未实现，下一阶段优先MySQL受控业务快照与CSV对照。治理预览/命名方案库继续后移。
