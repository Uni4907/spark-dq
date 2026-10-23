# 固定版本与官方依据

初始锁定：Spring Boot 3.5.16、Spark 3.5.8 / Scala 2.12（实际传递补丁随 dependency tree 记录）、MySQL Server 8.4.8 LTS、Connector/J 8.4.0、Maven 3.9.12、Node 22.22.0、Vue 3.5.44、Vite 7.3.7、ECharts 5.6.0。镜像在首次拉取后锁定摘要，Java 17 实际补丁记录在验收记录。

- https://docs.spring.io/spring-boot/3.5/system-requirements.html ：Java >=17、Maven >=3.6.3。
- https://spark.apache.org/docs/3.5.8/ ：Java 8/11/17、Scala 2.12/2.13；本工程只使用 _2.12 制品。
- https://spark.apache.org/docs/3.5.8/sql-data-sources-csv.html ：CSV nullValue、emptyValue、多行、FAILFAST 选项。
- https://v7.vite.dev/guide/migration ：Node >=20.19 或 >=22.12。
- https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-8.html ：8.4.8 LTS 已发布。
- https://maven.apache.org/docs/3.9.12/release-notes.html
- https://nodejs.org/en/download/archive/v22.22.0
- https://github.com/vuejs/core/blob/main/CHANGELOG.md

Spring Boot 和 Spark 使用独立 JVM / 独立依赖树，避免 Jackson、日志框架等传递依赖冲突。Spark 不加入 backend 的 Maven 依赖。

2026-10-16 实际补丁：Temurin 17.0.18、Scala 2.12.18。镜像摘要见 Dockerfile 与 compose.yaml。Vue/Vite 候选旧补丁经 npm audit 被剔除，使用 https://github.com/vuejs/core/releases/tag/v3.5.44 及 https://github.com/vitejs/vite/releases/tag/v7.3.7 的已发布修复补丁，保持原主版本。

最终 npm 锁文件审计还保留 ECharts 5.6.0 的 1 个 moderate 告警（GHSA-fgmj-fm8m-jvvx / CVE-2026-45249，修复版本为 6.1.0）。按用户 ECharts 5 基线不跨主版本升级。本页面只导入 BarChart，不导入 Lines；tooltip 指定 richText，图表标签来自受限字段名，数值来自引擎统计，不传入原始用户 HTML。该用法规避已描述的 Lines + 默认 HTML tooltip 触发路径，但不宣称底层依赖告警已消除。后续允许改基线时升级至已修复版本并回归。

官方说明：https://lists.apache.org/thread/1g6xk7gd9vg1c6zyqqt2lnko10zomc3o ；依赖告警：https://github.com/advisories/GHSA-fgmj-fm8m-jvvx 。

2026-10-19 数据库迁移：Flyway core/mysql 使用 Spring Boot 3.5.16 BOM 管理的 11.7.2，本轮 Maven 实际解析、构建与 MySQL 8.4.8 空库/旧库验收通过。没有升级其他框架或 Spark 依赖。
