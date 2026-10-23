# B4：MySQL 业务源与 Hive 目标接入差距（只读盘点）

2026-10-21。本轮仅盘点，不实现连接器、不启动 Hive、不采购云、不创建外部账号。用户指定目标包括 CSV/MySQL/Hive；此前路线将 Hive 暂列为不引入，不能据此判定用户目标已完成。

## 当前源码证据

| 范围 | 证据 | 判断 |
| --- | --- | --- |
| MySQL | backend/src/main/resources/application.properties 的 spring.datasource 指向系统 sparkdq 库，后端 JdbcTemplate 管理任务、模板、结果等 | 是元数据持久化；没有业务表数据源注册/字段选择/快照读取接口 |
| Spark 输入 | Engine.java 读取 readCsv 或 PreparedInput.read，治理后读取独立 CSV | 已有CSV/受控Parquet输入；没有JDBC/Hive输入分派 |
| 原生 Parquet | ImportApi、ParquetImport、NativeImports，成功登记规范CSV | 是Parquet原生导入，不是MySQL或Hive连接器 |
| Hive | spark-engine/pom.xml显式依赖spark-sql，无spark-hive依赖；Engine无enableHiveSupport/表读取，配置未提供metastore契约 | 没有Hive检测源的产品接入或端到端验收；未将Spark依赖里潜在库视作已实现 |

当前已有MySQL镜像可复用作独立业务fixture；仍需独立数据库/只读账号，不能直接以元数据库充当业务样本。Hive镜像、metastore、兼容版本和持久化空间尚未建立并验收，不能用CSV导出上传替代Hive原生接入证明。

## MySQL 下一批最小实现建议

1. 新建隔离业务fixture库和只读账号，与系统元数据库账号分离；凭证只在私有配置中使用，不进入接口日志/报告/源码包。
2. 首批只允许白名单库表与字段、有限条数/字节/超时，不接受任意SQL；明确读取权限、连接超时及取消路径。
3. 定义NULL、整数、精确DECIMAL、DATE与文本契约，拒绝不支持或有损类型。若转成受控规范CSV接入，要明确称为MySQL快照导入，而不是“任意JDBC分布式直接检测”。
4. 在一致快照内读取并保留数据副本、表/字段/快照元信息与内容SHA；多连接分区读取需解决跨连接快照一致性，不能只配置numPartitions便声称可复现。
5. 同一逻辑数据CSV与MySQL来源运行IQR/FD及旧规则，全量指标/异常集合一致；修改源表后，旧快照任务仍能复检，取消/失败不发布半成品数据集。
6. 来源纳入备份/恢复和维护准入；本批受20MiB边界，扩容量另行分批。

## Hive 下一批最小解除条件与验收

- 固定Spark3.5.8/Scala2.12所需Hive集成依赖与metastore兼容方案；先核对已有镜像与可信来源、磁盘/内存预算及端口。
- 在项目专用隔离网络部署最小metastore/表环境或明确受控Hive快照导入契约。外部metastore若需要账号/端点，由独立配置接入。
- 真实建立小型表（含精确数值/NULL/文本），从Hive表入口读取，冻结来源与输入内容；对同一逻辑CSV运行IQR/FD并全量对照，来源变更后历史仍可复检。
- 若资源不足，交付实际资源错误与可复现最小步骤，明确“未完成”，不将一般CSV导入称作Hive支持。

## 本轮状态

IQR、FD已在前两批实现和隔离验收。MySQL业务源、Hive源均未实现、未验证；本文件不构成接入成功证据。优先建议下一批补MySQL受控业务快照，再做Hive最小适配，并重新核对正式开题/任务书的必做范围。
