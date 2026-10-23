# 数据库版本化迁移

本阶段使用 Spring Boot 3.5.16 管理的 Flyway 11.7.2（core/mysql）。迁移在启动时运行，成功后才允许任务恢复逻辑执行。MySQL 固定为 8.4.8；数据库账号、文件路径和原始数据保持原有配置。

## 版本与接入条件

- V1：固定现有 12 张业务表，空库执行完整建表。文件位于 backend/src/main/resources/db/migration/V1__initial_schema.sql。
- V2：增加 tasks(status,created_at,id) 与 datasets(created_at,id) 索引，支持状态/时间分页；不修改业务行或文件。名称子串查询仍不保证使用索引。
- 已有完整旧库：先比对固定 MySQL 参考元数据，再显式记为 baseline 1，仅执行 V2。参考覆盖表、列顺序/类型/空值/默认值/字符集、主键/唯一及普通索引、外键与更新/删除行为；额外视图、触发器、存储程序、事件或 CHECK 约束同样会导致拒绝。
- 旧库缺表、改列、额外对象或不匹配的排序规则：拒绝自动接入，不补表掩盖差异。更早的四表 MVP 必须先评估、备份并编写专门升级，不在当前自动接入范围。
- 已有 flyway_schema_history：由 Flyway 校验并执行后续版本。未知未来版本、校验值变更或失败记录会停止启动；不忽略 future，不自动 repair、clean 或降级。

spring.sql.init.mode 已改为 never，旧 schema.sql 不再在每次启动执行。旧版建表仅作为 tests/fixtures/legacy-schema.sql 测试夹具保存。Tasks 显式依赖数据库初始化，校验失败时不会把排队任务改为失败。健康接口增加 schemaVersion，当前为字符串 2。

参考元数据由固定 MySQL 镜像执行旧版建表夹具产生，db/legacy-metadata.sql 与 db/legacy-metadata.txt 同时纳入版本控制。测试会重新生成并比对，不能为让旧库通过而直接替换该参考。Flyway 校验的是迁移文件记录；接入版本管理后，人工直接修改数据库结构并不都会被 checksum 检出，因此结构修改必须通过新迁移完成。

## 升级流程

先确认无排队/运行/取消中的任务，在维护窗口停止后端，使用 infrastructure/storage.py backup 创建一致备份。备份完成前不要切换应用。构建、重启后的当前应用健康接口必须返回 schemaVersion=4，并核对历史数据与任务。失败时保留日志、数据库和备份，停止该应用；不要直接清空历史、修改 checksum 或重跑已部分执行的 SQL。

MySQL 的 DDL 不保证事务回滚：例如一个迁移的前一条建表成功、后一条失败时，前一条可能已提交。本项目会保留失败记录并拒绝后续启动，没有宣称自动回滚。当前恢复工具仅支持新卷隔离恢复，可在其中核对备份与旧应用，不自动替换原卷。

后续变更追加 V5__描述.sql 等新文件；已执行 V1/V2 不编辑、不改名、不重复使用版本号。先在隔离空库和备份副本中验证，再执行原库维护升级。没有自动计划任务、定期清理或远程发布。

## 备份兼容与复现

storage.py 接受原 12 张业务表，或业务表加 flyway_schema_history 的 13 表布局；其他未知表仍拒绝。新备份包括完整迁移历史，旧备份仍用归档旧应用恢复，不强行升级旧备份。迁移验收还会把已经升级的副本再次备份/恢复，核对历史不增长。

在项目 WSL 目录、具备 Docker 权限的用户下执行：

    timeout 60s python3 -m unittest discover -s tests -p test_storage.py -v
    timeout 1500s python3 tests/migrations_acceptance.py --legacy-backup /绝对路径/完整旧版备份

先构建候选后端镜像 spark-dq-backend:migrations-v2（backend.Dockerfile），迁移验收只使用新建项目与新卷，不修改源服务。旧版备份需含至少一个成功、带快照的普通检测任务以验证真实 Spark 复检。验收会占用额外磁盘；finally 停止新容器，所有卷、失败数据库及日志保留。可通过 --fixture-project 指向本轮已生成的专用夹具目录排查，但正式复现应不传此项以创建全新夹具。

测试覆盖：空库 V1/V2、重复启动、旧库全行/文件哈希保持、完整旧库 baseline、缺表/改列拒绝、checksum 错误、未来版本拒绝、故意中途 DDL 失败及再次启动拒绝、这些失败下排队任务不变，以及旧数据真实 Spark 复检、13 表新备份与隔离恢复。具体本轮结果见开发记录，测试失败日志也保留，不把预期拒绝当作业务功能成功。

设计依据：[Flyway baseline 安全边界](https://documentation.red-gate.com/flyway/reference/configuration/flyway-namespace/flyway-baseline-on-migrate-setting)、[迁移校验](https://documentation.red-gate.com/flyway/reference/commands/validate)。

## 当前V3/V4增量

上述V1/V2测试为迁移基础阶段历史记录。当前V3增加任务执行计划/清单SHA，V4增加datasets.source_manifest_json，均为可空追加列；当前schemaVersion=4。V3/V4真实备份恢复与旧应用拒绝证据分别见[执行副本](execution-inputs.md)及[分片注册](csv-parts.md)。不要编辑已经执行的V1..V4；旧应用拒绝新库时在新卷验证整组备份，不drop列、repair或clean迁移。
