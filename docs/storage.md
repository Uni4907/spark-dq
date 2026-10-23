# 存储对账、备份与隔离恢复（2026-10-19）

本阶段增加 infrastructure/storage.py（Python 3 标准库 CLI），将 MySQL 元数据、完整文件卷和实际运行应用的 JAR/依赖组成可校验的本地备份。读取数据卷时使用只读挂载；恢复创建新项目和新卷，主服务继续使用原卷。

## 操作约定

在项目 WSL 根目录、拥有项目 Docker 权限时运行。工具不自动停止业务：audit/backup 要求后端已停止、MySQL 正常、数据库没有 QUEUED/RUNNING/CANCELLING。维护前先确认任务为空闲，保留原服务状态并在结束后启动后端。CLI 使用非阻塞操作锁，避免同时执行两次存储操作。

输出路径必须全新，父目录须已存在；即使目标目录为空，也不复用已有目录。建议放在 Git 忽略的 .runtime 下。命令示例（备份名称应按本次操作取新值）：

    mkdir -p .runtime/backups
    docker compose --env-file .env -f infrastructure/compose.yaml stop backend
    timeout 240s python3 infrastructure/storage.py audit --output .runtime/audit-new.json
    timeout 600s python3 infrastructure/storage.py backup --output .runtime/backups/capture-new
    timeout 240s python3 infrastructure/storage.py verify .runtime/backups/capture-new
    docker compose --env-file .env -f infrastructure/compose.yaml up -d backend
    timeout 600s python3 infrastructure/storage.py restore .runtime/backups/capture-new --output .runtime/restore-new

restore 返回隔离 API 的随机回环端口、配置路径和卷名。隔离 API 只绑定 127.0.0.1；不占用主服务 18080/15173。恢复程序将 mysql/files/application 三个新卷分别初始化，核对后再启动后端。数据库和文件卷都没有指向主服务；应用卷只读挂载到 /app。

全局 --config/--env 参数放在子命令前，可对隔离项目进行离线对账。路径按调用者当前目录解析。工具没有“覆盖生产卷”或“删除孤立文件”命令。正常手工 restore 会保留隔离服务供查看；验收脚本在 finally 停止其容器，所有新卷继续保留，便于复查。

## 对账口径

逐数据集核对固定路径、文件存在、大小和 SHA-256；逐成功规则核对异常 JSON 分片存在，包括合法的零字节结果分片；检查非成功任务上的结果、成功规则漏结果和输入快照指纹不符。未登记的 raw 文件和未知任务目录列为 warning；缺失/变更/非法路径列为 issue。已登记失败/取消任务的日志、中间文件作为诊断资料保留。

对账返回 JSON：ok、counts、fileCount、bytes、issues、warnings。发现 issue 的 audit 退出码为 2，正常为 0；格式、前置条件或执行失败为 1。无论是否发现问题，都不自动修改元数据或删除文件。

## 备份格式与恢复保护

目录包括 database.sql、files.tar、application.tar、audit.json、manifest.json 和最后创建的 COMPLETE。manifest 记录受支持表的数量、UTC 创建时间、运行镜像来源、各产物字节数及 SHA-256；COMPLETE 绑定 manifest。失败时保留已有文件，没有 COMPLETE 的目录不能恢复。

数据库仅导出项目业务库，不导出账号表。原 .env 不放入备份；新环境生成独立密码并以 0600 保存。备份目录 0700、各文件 0600。原始数据与备份包含业务内容，应按本地私有数据保管；源代码提交只包含工具、测试和文档。

应用归档从原容器 /app 读取实际 backend.jar、Spark 引擎 JAR 和依赖，拒绝额外的非 JAR 文件。恢复使用清单中已存在的本地基础镜像，并以归档应用覆盖 /app，从而保留备份时真实执行的代码。清单分别记录原容器镜像引用和恢复基础镜像 ID；完整镜像本身尚未导出。异机恢复需要另行准备清单对应的基础镜像，这一阶段验证的是同机隔离恢复。

恢复前检查完整标记、全部校验值、归档条目。拒绝绝对路径、父目录越界、重复路径、软/硬链接和特殊设备；每个 tar 最大 4 GiB、100 万条目。校验失败在创建 Docker 资源前退出。恢复卷名由新 UUID 生成，并拒绝已有同名卷。恢复后核对所有受支持表数量，以及文件/应用的全部条目、大小和哈希，再启动 API。校验值用于完整性检查，备份应来自本项目可信来源。

## 本次真实验收

备份时间：2026-10-19 09:25:17（Asia/Shanghai）。位置：/home/sparkdq/projects/spark-dq/.runtime/storage-ebb319c34784/backup。

- 142 个数据集、190 个任务、670 条规则、592 份结果；共 12 张业务表。主文件卷 3208 个文件，对账 issue=0、warning=0。
- SQL 539442 字节，数据 tar 24872960 字节，应用 tar 251940864 字节；主要产物合计约 264.5 MiB。
- 隔离项目 spark-dq-restore-518cfe1ae7fd：所有表数量和归档文件完全核对，CSV、版本关系、快照/审计、报告、异常预览及两版模板保持；另抽查了成功/失败/取消三类既有历史。
- 恢复后运行真实 Spark 复检 898534e9-442c-457e-bc5a-3aa2615239f7，4 行、5 条规则指标及模板修订 1 与原治理任务一致；该新任务在原数据库不存在。
- 只在隔离副本中新建三个探针文件/记录，检测出缺失、校验值不符和未登记文件。没有改写任何原有文件，源库/源文件/源 .env 和备份不变。
- 14 项标准库单元测试通过；本轮 tests/e2e.py、governance_persist.py、rules_persist.py、governance_transforms_persist.py、cancellation_persist.py --require-recovery 均通过。没有重跑旧 Java/Vue 构建，应用代码未改动，不将上一阶段 31 项测试当作本次新结果。
- 首轮恢复因旧容器镜像引用无法通过镜像预检查失败，未开始恢复写入。修复为同时打包实际 JAR/依赖并使用可核对的本地基础镜像；保留首轮失败、最终成功和回归日志。还修复了从其他目录调用相对配置路径的解析问题。

可复现验收：

    timeout 60s python3 -m unittest discover -s tests -p test_storage.py -v
    timeout 1500s python3 tests/storage_acceptance.py

完整验收会新建合成数据/模板/任务，并短暂停止原后端进行一致捕获；只适用于无活动任务的维护窗口。脚本 finally 恢复原后端并停止本轮生成的隔离容器，所有新卷保留。本次隔离卷额外保留了有意新增的对账探针；它们不属于正常恢复数据。该隔离环境的验收 API 端口现已停止，主服务仍为 http://localhost:15173。

## 后续边界

当前支持本项目现有 12 张业务表，以及加 flyway_schema_history 的 13 表布局和单后端实例。跨资源快照依赖后端离线维护窗口，不提供在线热备份。未实现调度/保留期清理、加密异地备份、完整镜像导出、生产卷切换或跨实例协调。数据库写库故障、WSL 长期启动与分布式运行仍按长期路线另行推进。

数据库迁移与历史表备份兼容在下一阶段完成，说明和独立测试见 [数据库迁移](migrations.md)。上述 14 项测试及 12 表恢复数字是存储阶段当时的记录，不代表迁移后表数量。

已有离线工具的前后维护步骤已由下一阶段 maintenance.py 协调，含准入、恢复回执及中断恢复；详情见 [空闲维护备份](maintenance.md)。原 storage.py 的离线接口与格式保持兼容，子命令中断清理增加了限时保护。

## V4来源完整性

当前对账覆盖受控多CSV来源清单、合并SHA、字段/记录数及每份原文件的大小/SHA/有序路径；损坏阻止完整备份，失败目录保留并告警。兼容无V4列旧库，新卷恢复同时核验来源下载与真实复检。见[分片注册说明](csv-parts.md)。

## 业务恢复点扩展

新备份清单可选recoveryPoint，完整校验检查其格式/清单数量绑定，新卷恢复在应用启动前校验实际记录指纹。没有该字段的旧备份继续支持。备份新鲜度、业务覆盖、文件审计和实际恢复是不同判据，详见[恢复点与巡检](recovery-points.md)。
