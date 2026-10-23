# T2 Hive 接入可行性审计

审计日期：2026-10-21。结论：**有条件可执行；本批未启动 Hive，也未取得 Hive 接入实测通过证据。** 不阻塞 T3 的独立 Standalone 实验。

## 当前能力与差距

当前引擎使用 Spark SQL / DataFrame。`spark-engine/pom.xml` 直接依赖 `spark-sql_2.12`，没有声明 `spark-hive_2.12`。已有 Parquet 文件输入不能证明 Hive catalog 表读取、分区发现或 Metastore 兼容。MySQL 元数据表、MySQL 业务快照导入也不能代替 Hive 接入。

当前构建矩阵为 Spark 3.5.8、Scala 2.12、Java 17。官方文档列出的默认 Metastore 客户端是 Hive 2.3.9；另一个可审计的候选组合是外置 3.1.3 Metastore 与明确固定的 3.1.3 客户端依赖。`builtin` 要求启用 Hive 构建且 Metastore 版本为 2.3.9；不能仅把配置值改成 3.1.3 就认为完成兼容。Java 17 下的类加载、模块访问、JDBC 驱动和 SerDe 仍需按候选组合实测，不在本批给出兼容通过结论。

Spark 官方 3.5.8 文档要求 Hive 相关依赖进入 Spark 与 worker 的 classpath；通过 `enableHiveSupport()` 开启持久 Metastore、Hive SerDe 与 Hive UDF 支持。未提供 `hive-site.xml` 的模式会在工作目录生成 `metastore_db`，并使用 `spark.sql.warehouse.dir`。这意味着仅添加表名参数、复用 Parquet 文件路径，不足以交付 Hive 源。

依据：

- [Spark 3.5.8 Hive Tables](https://spark.apache.org/docs/3.5.8/sql-data-sources-hive-tables.html)
- 仓库 `spark-engine/pom.xml`、现有 CSV/Parquet 输入边界。

## 两条实施路径

| 路径 | 最小组成 | 适用与限制 |
|---|---|---|
| 嵌入式实验 | 与当前 Spark 同版本的 spark-hive；独立 Derby Metastore 目录；独立 warehouse；catalog 表夹具 | 用于先验证 catalog 读取。单个拥有者进程，避免多进程 Derby 锁冲突；不能据此声称远程 Metastore 或跨机能力 |
| 独立 Metastore | 独立 Hive Metastore 服务及其专用数据库；匹配的客户端 JAR；hive-site.xml；所有执行节点可访问的 warehouse | 更接近集群条件；需额外镜像、服务内存、数据库及版本矩阵验证，当前不直接部署 |

所有服务使用独立 Compose 项目、卷和显式分配的未占用子网。Metastore 的 9083 等内部端口只在实验网络内开放；如需管理入口，只绑定回环地址。主部署继续 local。配置与数据目录写入新实验目录，不复用主库或历史卷。结束时停止本实验容器并保留卷、网络、失败日志，不做全局清理。

资源预算是下一次实施的准入方案，而非实测消耗：嵌入式试验预留 2GiB 容器内存和 2 CPU；外置服务方案再为 Metastore 预留 1GiB、专用数据库 768MiB。启动前要求 WSL 可用内存至少覆盖拟启动容器配额并余留 2GiB，宿主与 WSL 数据盘各至少有 8GiB 可用空间。每个夹具最多 100 行、单次 120 秒；先停止本批自有实验容器，避免与规模实验并行。任一门槛不满足即保留审计结论并继续不需要 Hive 的工作。

## 实施前门槛与验收

先固定 Spark、Scala、Java、Hive client 和 Metastore 版本，验证所有 worker 实际加载的依赖清单，再运行 10–100 行 catalog 表夹具。检查类型、NULL/空串、DECIMAL、DATE、中文、换行、分区和表名/字段白名单；不开放任意 SQL。导入仍需有界、可取消，快照与来源清单进入既有数据集和备份恢复链路。

只有 catalog 表读取、来源冻结、CSV 同逻辑数据全指标与完整异常多重集一致、原规则复检及无源凭证恢复均取得实测证据，才标记 Hive 接入完成。当前缺少依赖、catalog 入口和上述实测，因此继续标为待实现，不把设计方案当验收结果。
