# 架构与当前数据源边界

2026-10-21。版本、工件及四层证据表见 [当前状态](current-status-2026-10-21.md)。

Vue 工作台 → Spring Boot 正式 API → 数据集/规则/任务/报告元数据（系统 MySQL）与受控文件卷 → 单后端有界调度队列 → 独立 JVM Spark 检测 → 异常文件与持久化结果 → 报告、治理版本和原规则复检。主配置维持 local；同机 Standalone 使用独立实验拓扑。

| 数据源/存储 | 入口与实际处理 | 当前边界 |
| --- | --- | --- |
| CSV / 多 CSV | Datasets / CsvParts，冻结输入及来源 | 产品已有入口；20MiB |
| Parquet | NativeImports → 隔离 Spark 转换 → 规范 CSV 原子登记 | 产品已有入口；不能称为 Hive 接入 |
| 系统 MySQL | JdbcTemplate / Flyway | 存储元数据，不等于读取 MySQL 业务表 |
| MySQL 业务表 | 本批 MysqlSnapshot → 一致性读取 → 独立规范 CSV/manifest | 仅读取核心与隔离测试；正式 API 与检测闭环待接线 |
| Hive 表 | 尚无 enableHiveSupport / 正式表入口 | T2 可行性审计及接入验证均未完成 |

新增 MySQL 核心不改变既有依赖或数据库结构；凭证只经调用方私有连接工厂传入 JDBC，不进入规范 CSV、manifest 或异常对象。读取结果首先是独立快照，后续产品集成必须在维护/取消状态机控制下登记后，才能成为正式数据集。

IQR 使用精确十进制连续插值，FD 按配置的 X→Y 验证并返回冲突组全部参与行。它们不是自动依赖发现或错误来源定位。现有最高20万行、低于20MiB、local历史实验不证明GB/TB、集群加速或跨物理机能力。
