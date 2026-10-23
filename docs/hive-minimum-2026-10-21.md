# T8 Hive 最小真实表接入（2026-10-21）

实际完成：固定 Spark 3.5.8/Scala2.12/Java17 镜像中的 spark-hive、Hive2.3.9、Derby10.14，建立 **Hive SerDe 管理表** sparkdq_lab.quality_fixture，Provider=hive、ParquetHiveSerDe；创建进程退出后，由独立 Spark 进程 enableHiveSupport 通过 Catalog 按表名读取。保留 DDL、DESCRIBE EXTENDED、真实 Location=file:/lab/warehouse/sparkdq_lab.db/quality_fixture、嵌入式Metastore文件、配置与日志。不是直接读取Parquet路径。

## 接入范围

管理员执行受限夹具导出程序（固定表、最多100行，单进程120秒/2CPU/2GiB/1GiB堆、无网络）得到规范CSV与来源凭据，再通过T7白名单正式登记。服务端校验凭据字段、Catalog/Provider/版本、行数、列名、CSV完整指纹和凭据指纹，将来源与原文同时持久化。浏览器仅传别名，不提交SQL、路径或凭据。原受控文件配置仍兼容，无结构迁移，Hive类库只在隔离导出工具中，产品Engine不增加Hive依赖。

这是**真实Hive表→管理员有界导出→规范快照接入**的最小闭环。当前没有独立远端Metastore、在线任意表查询、通用异步Hive导入服务、分区表或多客户端并发支持。Derby只允许单进程所有者，导出后检测/复检不访问原Catalog。不能把本批14行解释为大规模Hive接入已验证。配置凭据是受信任管理员的来源记录，不是第三方签名认证；执行真实性由随附Catalog、DDL和进程日志证明。

## 真实验收

14行涵盖中文、空字符串、NULL、引号、逗号、内嵌CRLF、文本精确小数、重复与FD冲突。Hive全量输出与独立CSV行多重集一致。正式API登记后执行IQR(n)和FD(x→y)，与CSV的全部指标、精确阈值和**完整异常行多重集**一致；独立复检、任务快照固定、原文/来源下载完整指纹、HTML/JSON报告、重复登记幂等通过。浏览器实际点击登记并自动选中，展示Hive表/嵌入式Metastore/凭据指纹，读取复检报告。

数据集 `37a3ae55-5707-4a68-8f9c-1be391c75efd`；Hive检测 `83a9d287-f4b0-44a9-ae5e-7d533a49594c`；CSV检测 `998e0be2-bbb9-47ae-8511-1f78aa8f5fe2`；独立复检 `9e09f571-0427-4019-8cab-3291a82c2d2f`。IQR Q1=2.25、Q3=5.75、下界=-3、上界=11、异常2行；FD全部14行参与2个冲突组。原文SHA `d2bcb01d60afba8ac5c3417d8ed6faff710b0fd6f2ac81e8fbc0164c7233863f`，凭据SHA `67f1f3211529bffff2cfb54fc0d0daab1f6c36cbb87996d9c98f3b7a77bd0d54`。

首轮无Python解释器/主机名解析失败，改用镜像自带Java；随后全量比较发现未引用空字段读为NULL。保留失败目录及日志，修正夹具显式引用空字符串后重建新目录，未忽略差异，也未修改检测算法。

本批Java后端162+引擎43=205项通过，Node21项通过。Python最终独立回归224项通过（晚于镜像构建，不合并历史未启用的真实JDBC专项10项）。引擎未修改，SHA `f4006c52ef1689db7fde83deb9a9f3f673434b95b5803357f7ef636ad8819633`。后端镜像 `sha256:47960a231416a90db9bf9a18ef6c1fbe4de6244651db0065bfd8df92f5d95039`；前端 `sha256:d59cec736da429ddc1e4f9cbc709c73dba8973c36558de7f9c6347ac5281d0ed`。候选构建标识见manifest。

## 安全、兼容与后续

配置默认关闭；凭据必须在只读受控目录内、无符号链接、最多16KiB，不包含账号密码/任意SQL。凭据与规范CSV不符则拒绝发布；相同别名/字节但凭据变化拒绝复用旧数据集。恢复工具会校验保存的Hive来源结构与快照，不要求原Metastore在线。此项已自动测试；T8未单独再执行整套备份恢复，统一候选恢复留到T10（T7实际恢复证据仍绑定T7）。治理版本通过父版本追溯Hive，不能将治理后的行数写回原表来源。

主部署未切换，元数据库无结构迁移；所有本批容器已停止，历史文件/卷/网络保留。新来源字段仍需匹配本批应用与storage工具；回滚沿用切换前恢复点。T9具备执行条件。

## 复现与证据

`python3 infrastructure/hive_fixture_lab.py` 在全新.runtime/t8-hive-*创建真实表并由第二进程导出，固定Java源码 infrastructure/hive/SparkDQHiveSnapshot.java。结果必须 fullFixtureMultisetEqual=true。将hive.csv及hive-receipt.json以只读文件挂至/run/controlled-inputs，在SPARK_DQ_CONTROLLED_FILES_FILE管理员配置项加入hiveReceipt路径（其他4项沿用T7）。再使用 infrastructure/t8_hive_lab.py start <已准备新目录> 和 infrastructure/hive_product_acceptance.py <目录> 验收，stop保留卷。原实验位于 `/home/sparkdq/projects/spark-dq/.runtime/t8-product-20261021-220444-521bb7`；Catalog证据 `/home/sparkdq/projects/spark-dq/.runtime/t8-hive-20261021-220303`；构建 `/home/sparkdq/projects/spark-dq/.runtime/t8-build-20261021-220330`。

官方依据：[Spark3.5.8 Hive表文档](https://spark.apache.org/docs/3.5.8/sql-data-sources-hive-tables.html)说明Hive支持使用持久化Metastore和SerDe，所需依赖须在运行类路径中；本批另有本地实际验证。
