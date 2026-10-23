> 历史T10阶段记录。对应指纹见release-candidate-t10.json；T11最终分片补充、最新镜像与恢复点见release-candidate.json及final-handbook.md。

# T10 统一候选审查与发布准备（2026-10-21）

## 当前结论与版本

统一候选的受影响测试、真实业务、恢复、旧主库副本迁移/回滚及当前JAR同机双Worker兼容均已通过。已知解析/预览内存和报告长文本问题已修复，未发现阻断本机单人毕业设计演示的问题。**尚未切换主部署，正式升级仍需用户明确批准。** 这不是公网多租户或第三方渗透验收。

- 后端镜像 `spark-dq-backend:t10-controlled-223120` / `sha256:70869e4b7dfe84a9af0df2e9734381ca66b82fa7b09ef1555bdcb39bd468a090`；实际backend.jar SHA `174cb2079246754f2538cda13928b07d9293201cad31a06b52fea1a6e1589f63`。
- 前端镜像 `spark-dq-frontend:t10-controlled-223131` / `sha256:c859f51c6ad162dce1905a1f1c95cd5a41598024631f108013c31999cd9a8a51`。
- Engine SHA `f4006c52ef1689db7fde83deb9a9f3f673434b95b5803357f7ef636ad8819633`（沿用T6修复后版本；未修改IQR/FD算法）。候选源文件指纹、JAR/镜像/回归日志与最终源码提交见release-candidate.json和本阶段manifest。
- 本批最终常规Java后端170+Engine43=213项通过，另新建专用MySQL实例实际启用JDBC10项且全部通过，**本批不同用例223项**。Node21、Python224通过。早期候选构建不与最终数累加；T4/T5历史与T6/T8阶段结果也不当作本次回归。

## 主模块审查及修复

|模块|实际审查与验收|结果/边界|
|---|---|---|
|CSV/分片/受控登记|Datasets、CsvImports、CsvParts、ControlledFiles/Profile/HiveReceipt；正式20MiB拒绝/错误指纹不发布/幂等/1000001行拒绝|普通20MiB保持；受控64MiB/百万行明确档位|
|内存边界|新增CsvLimitsReader，Commons CSV分配完整记录前逐字符检查65536 UTF-16单元格/100列/原始字段最多131074字符；保留Commons CSV语法判定|最大65536个转义引号实际通过，超限和列数在流式读取时拒绝；原文与完整异常不裁剪|
|数据与异常预览|数据最多10行、异常最多50行；每格256字符含省略号，不切断代理对，保留NULL与重复组大小|真实长单元格API/页面通过；不把原始异常改成截断内容。历史预览经读取也裁剪显示|
|业务MySQL|只读白名单、固定表/字段、InnoDB一致快照、精确小数、无SQL/JDBC客户端输入、错误只保留代码|正式API与CSV完整IQR/FD一致；错误凭据/行限不发布；专项10项含真实锁超时/取消/并发提交一致性|
|原生Parquet与执行副本|NativeImports/ParquetImport/ExecutionInputs/PreparedInput，限定类型、不可变指纹、输入计划冻结|实际6行原生导入、CSV与Parquet执行副本结果全量一致，源Parquet字节保留|
|规则/模板/治理/版本|Rules/RuleTemplates/GovernancePolicies/Reuse，参数和列校验、冻结规则、事务发布与父版本关系|百万→50万正式闭环全部指标和完整多重集独立一致；治理后再登记与复检可继续，未泄漏登记信号量|
|任务/取消/恢复|Tasks每次执行独立Job、单Worker+8排队、300秒预算、行锁决定唯一发布结果、进程/远端确认清理|实际Spark启动后取消无结果；中断/重启为FAILED且原快照不变；远端不可观察时不提前发布终态/新任务|
|报告/分页/导航|Pages参数化查询、20默认/100最多、旧list最多200，前端请求序号防过期更新；ReportDocuments转义与CSP|IQR比较从长JSON改精确可读阈值摘要，浏览器viewport1265/scrollWidth1265无横向溢出；长预览提示清晰|
|安全/部署|只读源/配置、真实路径/指纹、随机私有凭据、API不含密码、维护Key常数时间校验、无任意路径/SQL、命令参数列表|绑定127.0.0.1；元库/业务库/Worker无宿主端口；非多租户鉴权系统，不作为公网发布许可|
|迁移/恢复|Flyway已知旧布局才baseline、校验checksum、不自动clean/repair；备份前维护锁/排空/停后端，恢复新卷|实际主库副本V1—V5→V6，225个原数据集完整下载SHA与表行数均保留；源主环境未启动|

流式读取和预览上限是实际资源改进，不代表任意64MiB形状均完成负载验收。源文件总保留预算1GiB/10000目录项、至少8GiB文件系统可用空间；受控输出每500ms观察1GiB/10000项并停止，可能短暂超调，非瞬时硬配额。容器无法单靠虚拟文件系统余量判断Windows后备盘，运行前另检查宿主余量。当前单机资源已满足实际实验；多并发、多租户、跨机、64MiB最宽表组合及500万正式产品流程未验收。

## 本批真实验收与当前Standalone

统一实验 `/home/sparkdq/projects/spark-dq/.runtime/t10-ui-20261021-223246-c22348`：million-acceptance-20261021-223450、sources-acceptance-20261021-223656、hive-acceptance-20261021-224011的summary均ok=true。百万行60,728,899字节→治理50万25,364,453字节，全部指标、精确IQR与完整异常多重集仍与独立流式参考一致；20MiB上传和非法源拒绝、运行中取消实际通过。MySQL及Hive分别和CSV全量对照、治理/复检、源指纹与快照通过。Native Parquet/格式副本进行正式完整结果等价比较。浏览器实际查看源、版本、长预览与报告；不把所有API步骤冒称UI点击。

`/home/sparkdq/projects/spark-dq/.runtime/t10-standalone-20261021-225037-3e1b1c`：同一当前Engine JAR/13行/15规则在local与Standalone完全一致，事件证明worker1与worker2均成功执行任务。原规则/模板冻结、完整异常、报告/复检、Master不可用期间取消仍待清理、新任务409、后端重启保留应用身份至清理确认、5秒合计预算超时清理均通过。5秒是准备/检测合计预算，不宣称它一定在远端算法内部到期。此为同一物理机功能兼容，**不是跨物理机或性能加速**；T4 d9aeac旧JAR的500万性能仍独立引用，没有重跑性能矩阵。

## 备份、隔离恢复与实际主库回滚演练

候选一致性恢复点 `7c8f0e0af9d6c1cf3f8f446ae32bd49dcf99b602fb133d3edae64e4529b6ef31`，私有完整备份 `/home/sparkdq/projects/spark-dq/.runtime/t10-ui-20261021-223246-c22348/recovery/maintenance-backup/backup`（SQL、files.tar、application.tar、校验与COMPLETE）。经全新三卷恢复，/app只读且JAR指纹匹配；原受控目录/MySQL业务源/Hive Catalog均不配置，仍能下载百万快照与50万版本，真实Spark复检50万完整异常一致；MySQL和Hive小样本真实复检全量一致，原生Parquet原文也保留。最终收据 recovery/final-summary.json 为ok=true。恢复使用基础镜像 `sha256:a627b16e8c44d48b5efc4a9db9c7442bd70daa1e9d65a3fdf037b94b84c28c85`，实际应用来自校验后的/app；**不能只用基础镜像ID冒充应用版本**。

实际旧主环境冷备 `/home/sparkdq/projects/spark-dq/.runtime/t10-main-cold-20261021-223614`：原3容器全部停止，只读挂载原元数据库/文件卷打包，前后ID/镜像/停止状态及完整内容树指纹相同。私有物理冷备绑定MySQL8.4.8固定镜像及原凭据，不作为通用跨版本逻辑备份；原始主数据和private.env仅留本地，不进公共交付包。cold receipt记录tar完整SHA。

`/home/sparkdq/projects/spark-dq/.runtime/t10-main-rehearsal-20261021-224304/final-summary.json`：将同一实际冷备分别还原到两套全新卷；旧镜像可启动并完整下载核对225个原数据集，实现回滚恢复演练；新候选启动执行V6增量迁移，原13个业务表计数及225个完整下载SHA保持，离线audit通过。实际旧库已有baseline1及V2—V5；V6增加dataset_imports.source_kind/source_request_json并放宽source_sha256/source_bytes可空，没有删除历史行。T7/T8/T10无额外结构迁移。

保留真实失败：早期百万验收脚本缺少sha辅助函数（产品登记已成功，脚本退出）；冷演练误假设旧库无Flyway历史；两份恢复脚本最后调用offline audit前未停后端。均已修正工具/顺序，失败summary不覆盖。finalize脚本在停止副本后审计，并重新独立核对已保存的实际Spark全量输出与HTTP来源/应用指纹后生成final-summary；不是伪造成功或重复混算测量。首轮候选/失败目录和卷均保留。

## 待批准的主环境升级方案

本阶段只是可审计发布候选准备，不执行切换。主部署当前仍旧镜像、本地local配置且停止。正式批准前最终交付会列出目标提交、镜像/JAR、实际主库冷备SHA、候选恢复点及操作方案。切换应先复核实际源卷仍等于冷备指纹；如变更则新建恢复点，绝不拿过期备份替代。停止旧服务后启动明确目标版本，迁移V6并检查数据/原文/健康，失败则停止新服务，在**全新卷**还原切换前完整元库+文件及旧镜像，确认后再连接稳定访问入口；原卷保留，不通过回退镜像强行操作V6/新来源数据。

切换过程服务暂不可用；现主环境本来停止。回滚到切换前恢复点可能丢失切换后新增记录，须先另存新数据/报告并明确取舍。冷备包含225数据集、292任务等实际历史，需要与文件卷一起保护；不能只备份源码或SQL。没有推送远程、删除卷/历史、全局清理、外部付费或开放公网。

## 复现和后续

源码包含release_lab.py（SPARK_DQ_SCALE_FIXTURE可指定T7 prepare产生的新参考目录，SPARK_DQ_HIVE_FIXTURE指定真实Hive夹具目录）、release_million_acceptance.py、release_sources_acceptance.py、release_hive_acceptance.py、release_restore_trial.py、release_cluster_trial.py及冷备/隔离演练工具。后者只接受已停止真实主环境；任何正式切换不由脚本自动执行。测试Parquet样本已放入tests/fixtures/t10-positive.parquet，SHA与原样本一致，脱离Windows临时目录依赖。新增工具已语法编译，原正式验收使用相同样本字节。

T11继续整合架构/ER/状态恢复/算法/API、实验与演示材料，并提供独立于主环境的固定本机演示入口。学校正式任务书/开题原文仍需用户之后逐条核对，现有总任务书不替代学校要求。
