# SparkDQ 数据库迁移阶段开发记录

完成时间：2026-10-19T10:22:02+08:00。真实项目 /home/sparkdq/projects/spark-dq（SparkDQ-Ubuntu / Docker）。工作开始时 HEAD 为 f66a3d1，工作区干净；本次没有远程推送或发布。

## 实际完成

将每次启动重复建表改为 Flyway 11.7.2 版本管理，版本取自现有 Spring Boot 3.5.16 BOM，并加入 MySQL 支持模块。V1 固定原 12 张业务表；V2 增加任务状态/时间与数据集时间分页索引，不改变业务记录或文件。

旧库仅在全部参考元数据一致时接入 baseline 1。缺表、改列或额外对象拒绝自动接入；已有版本历史按 checksum 校验，未来版本和失败记录均阻止启动。Tasks 显式依赖数据库初始化，避免迁移未成功就恢复任务状态。健康接口返回 schemaVersion 字符串 2。

备份工具兼容旧 12 表及加迁移历史的 13 表。旧备份继续用归档旧应用恢复；新备份包含完整历史。本轮真实验证两种布局，以及新备份的再次隔离恢复。

## 改动文件

- backend/pom.xml、application.properties：依赖和启动配置。
- backend/src/main/java/cn/sparkdq/DatabaseMigrations.java、Tasks.java、App.java：旧库保护、恢复顺序和健康版本。
- backend/src/main/resources/db/migration/V1__initial_schema.sql、V2__history_paging_indexes.sql：不可变版本脚本。
- db/legacy-metadata.sql、db/legacy-metadata.txt：固定 MySQL 元数据查询及参考；原 schema.sql 移为测试夹具。
- DatabaseMigrationsTest.java、tests/migrations_acceptance.py、migration_fixture.py、fixtures/legacy-schema.sql、fixtures/migrations-compose.yaml：单元与隔离真实验收。
- infrastructure/storage.py、tests/test_storage.py：双布局备份兼容。
- README.md、docs/migrations.md、roadmap.md、storage.md、versions.md、development.md 和本日志：运行条件、证据和限制。

## 本轮真实测试

- Maven package：34 项 Java 测试，0 失败/错误/跳过；后端与 Spark 模块构建成功。没有更改前端，本轮未重复构建 Vue。
- 标准库存储测试：15 项，全部通过；新增旧/新布局接受、未知表拒绝。
- 隔离 MySQL/应用验收：8 组检查通过。空库执行 V1/V2；重复启动不增加历史；缺表/改列拒绝；checksum 错误和未来版本拒绝；故意 V3 前条 DDL 成功、后条失败，保留失败历史，再次启动拒绝。全部失败场景中的 QUEUED 探针保持原状，未执行任务恢复。
- 从受保护旧备份恢复 142 数据集、190 任务及其规则/结果/版本/审计；升级前后所有业务表逐行查询哈希和文件卷所有条目哈希一致。baseline 1 + SQL V2；重复启动仍两条历史。
- 升级副本真实 Spark 复检任务 7ec775e0-7c6a-4279-89bc-0b2dd31c3bfa 成功，5 条规则指标与原成功任务一致。副本继续备份为 13 表，再恢复至独立新项目 spark-dq-restore-de7b42dc6e09，迁移历史一致。
- 主服务升级前确认无活动任务并先离线备份。升级前 146 数据集、193 任务、3249 文件及全部业务行在升级后完全不变，当前 UP / MySQL 8.4.8 / schemaVersion=2。
- 主服务升级后 tests/e2e.py、governance_persist.py、rules_persist.py、governance_transforms_persist.py、cancellation_persist.py --require-recovery 均通过。e2e 真实 7 行 Spark 任务 5a26e90b-4771-42ee-bfa2-4db1b080f587，name 命中 3、email 命中 2、重复 1 组/2 行/1 多余；复合/NULL 键和清洁数据也通过。
- 回归只新增合成记录；最终库计数 150 数据集、196 任务，迁移历史 2 条，活动任务 0。页面首页 HTTP 200。

## 真实遇到的问题

首轮新增单元测试修改了不含 char(36) 的元数据行，因没有实际制造差异而失败；已修正夹具位置并重新跑完 34 项。首次隔离验收重启后复用旧随机端口导致 ConnectionRefused；已重新获取映射端口，并用全新夹具重跑完整验收通过。两个首轮失败日志均保留，没有把它们计作通过。

WSL/Docker 需要现有 root 工具权限，已在本轮自动审批下执行。部分历史日志/备份为 root 私有目录；读取和导出由 root 完成，没有放宽密码或备份权限。没有要求用户复制、登录或确认。

## 证据与恢复点

- 升级前一致备份：/home/sparkdq/projects/spark-dq/.runtime/migration-deploy-9a6578b3dcf9/backup（私有目录，含 SQL、文件和实际旧应用；不导出业务内容到报告）。
- 最终隔离验收：.runtime/migrations-4727627170cc/acceptance.json。
- 主库升级核对：.runtime/migration-deploy-9a6578b3dcf9/verified.json。
- 构建、单元、验收与回归：.runtime/migrations-maven.log、migrations-storage-unit.log、migrations-acceptance.log、migrations-regressions.json，以及 migrations-*.py.log。
- 首轮失败：migrations-maven-first-failure.log、migrations-acceptance-first-failure.log。故意失败的数据库及独立项目日志留在本轮隔离目录。
- 所有隔离验收容器已停止，所有新卷与备份保留；原文件、凭证及原先备份没有删除或覆盖。

## 遗留问题与下一阶段

MySQL DDL 不保证回滚；本阶段明确失败后停止，不自动 repair/clean/降级或切换原卷。当前自动接入仅支持已知完整 12 表旧库，早期四表库/人工改造库需专门评估。迁移管理后的人工 DDL 不一定会被 checksum 发现，必须用新版本脚本变更。

WSL 缺少长期前台会话时仍可能退出；本轮没有修改 Windows 启动项或创建自动任务。定期备份、容量/保留期维护、完整镜像导出与异机恢复尚待实现。仍为 20 MiB 上传、单后端、串行 Spark 本地模式，未宣称集群或大规模性能验证。既有 ECharts 依赖告警与其他限制见版本记录。

用户回来无需执行迁移或补命令，可打开 http://localhost:15173/ 查看现有页面。下一阶段优先处理 WSL 长期运行与备份维护，再推进治理映射/业务去重优先级或规模基准。
