# 当前状态与证据入口（2026-10-21，T0 / T1-A）

本页替代 README 中指向 2026-10-20 的当前状态入口。历史阶段日志保留原文，只代表各阶段记录；“尚未开放/已完成”等描述须按日期和版本读取。正式学校任务书尚未取得，学校要求符合性全部待核对。

## 本批身份核验

- 待核验 B3 Git 基线 `355d649e779ab6859daeaca4a73c30013ec2bd7f`：开始工作时 HEAD 匹配且工作区干净。
- `SparkDQ-source-B3-final.zip` SHA-256：`1d0af153573308f9eaae5091146f9f96e52de779c36c971f2a69dbc9a7da83a2`。本批逐文件比较 300 个文件与上述 Git 基线，全部相同。
- `SparkDQ-evidence-B3-final.zip` SHA-256：`651c6ac6dd48e0a65de035394ea358422797e5f21193a03c1cf8d3f38a30c074`，本批与历史交付清单一致。哈希一致不表示本批重跑历史实验。
- 工作目录引擎 JAR 与 B3 候选容器内引擎 JAR：`ec2457f7e9c39f9b289929e7053c7ed5284141d6d190174c37c7bdc782461c65`。
- B3 隔离候选镜像 ID：`sha256:69adc4169938e98eb094227d7f79ddff90c71a9ce9043850a6c05d201b54a238`；本批只读观察候选后端处于 running。
- 历史 B3 实验记录 as-run 源提交为父提交 `7fde724bc2e18d6c30733f951229796b2d2ab8e6`。不能仅因交付提交较新便认为历史所有实验在该提交重新执行；以历史源文件哈希、JAR 和镜像记录对应。
- **主部署没有切换到 B3**：`spark-dq` 三个容器在本批审计时均 stopped/exited。主后端镜像 ID `sha256:a627b16e8c44d48b5efc4a9db9c7442bd70daa1e9d65a3fdf037b94b84c28c85`，内置引擎 JAR `faeb5f7da0d37819dacb07b0c1d1988e324290ed30b4008a2d32c75615d73276`，与 B3 不同。主配置仍为 local，未启动、重建或迁移主服务。

本批机器可见约 11.7GiB WSL 内存、约 10.2GiB available（采样），WSL 文件系统报告可用约864GiB；该虚拟磁盘值不代表 Windows 宿主可用物理空间，不能据此批准大实验。

## 能力与四层证据

| 能力 | 源码存在 | 历史验收 | 本批实测 | 主部署状态 |
| --- | --- | --- | --- | --- |
| CSV/多 CSV、Parquet 输入 | Datasets / CsvParts / NativeImports / ParquetImport | 历史 B3 及前阶段记录 | 本批后端回归；未重跑 HTTP/Spark 导入 | 停止，主镜像不是 B3 |
| MySQL 元数据持久化 | JdbcTemplate / Flyway V1–V5 | 历史任务、模板、来源、报告持久化验收 | 隔离 MySQL 业务夹具不等于元数据端到端回归 | 停止，保留原卷 |
| MySQL 业务快照导入 | 本批 MysqlSnapshot 读取核心；**无正式 API/注册流程** | B3 未实现 | T1-A 独立 InnoDB 夹具、只读账号、精确序列化与故障拒绝测试 | 未部署；T1 未完成 |
| Hive 表源 | 无 spark-hive 显式依赖及正式表入口 | 无接入验收 | 未执行 T2；不得标记 Hive 已接入 | 未部署 |
| IQR | IqrDetection：**精确十进制连续插值** | B1/B3 local 历史 | 未重跑本批 Spark 算法实验 | 停止；旧主 JAR 不得并入 B3 |
| FD | FdDetection：**配置式依赖验证，返回冲突组全部参与行** | B2/B3 local 历史 | 未重跑本批 Spark 算法实验 | 同上 |
| 同机 Standalone | 既有集群执行与清理机制 | 旧规则同机双 Worker 历史验收 | 新 IQR/FD 的 T3 未执行 | 主配置保持 local |
| 规模与识别效果 | algorithm_experiments / algorithm_oracle | B3 最高20万行、低于20MiB、local | T4 未开始 | 无 GB/TB/跨物理机能力结论 |

### 历史统计的适用边界

B3 的 **166 Java、207 Python、2 HTTP、25 Spark 运行**是历史报告记录，不是本批重跑。**17 Node 在 B2 执行，B3 未重跑**。本批后端测试数量及范围见 [MySQL 快照核心](mysql-snapshot-core.md)，不能与 B3 合并成同一轮全模块验收。

IQR 在偏态长尾中的误报、30% 污染和轻微偏移中的漏报及零召回按历史报告保留；FD 的冲突参与行检测不等于错误来源定位。未扩大统计结论。

## 当前最小修正与顺序

实际差异是“B3 源码/候选已交付，但主部署仍是不同 JAR 且停止”，影响任何声称主服务已获得 B3 能力的结论。最小修正是分开记录身份、保留主服务，后续如需升级，另行按隔离验收与备份门槛执行。

本批完成 T0 文档口径及身份冻结，开展 T1-A 读取核心；T1 后续须补正式 API 的白名单配置/字段选择/取消状态、原子数据集登记与来源下载、MySQL/CSV 的 IQR+FD 全指标/完整异常行多重集对照、报告/复检及隔离备份恢复。T1 完整验收后继续 T2→T3→T4→T5。当前尚无真实跨物理机条件。

证据根目录：`.runtime/t0-t1-20261021-a/`；`baseline-final/baseline-verification.json` 保存身份对应。导出交付只包含经过挑选的证据，不包含私有环境文件、容器数据或密码。
