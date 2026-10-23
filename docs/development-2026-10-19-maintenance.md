# SparkDQ 维护准入与单次备份开发记录

完成时间：2026-10-19T11:11:49+08:00。真实 WSL 项目 /home/sparkdq/projects/spark-dq；起点 main / b2c438e，初始工作区干净。本轮没有远程推送、发布、删除备份、删卷或恢复覆盖原数据。

## 完成的增量

增加受保护的维护准入，解决“检查无任务后又接收新提交”的窗口。公平读写锁覆盖所有 HTTP 写入，进入维护等待已有写入完成并确认 QUEUED/RUNNING/CANCELLING=0 后关闭准入；有任务返回 409，不强制取消。维护中写入返回 503/Retry-After 和明确提示，读取保留至实际停机。

维护控制通过容器内自动生成的私有随机密钥授权，工具内部读取使用，无需用户复制或认证。owner 原子持久化，同容器重启继续保护、密钥轮换；不同 owner 不能解除。临时控制文件不在数据/应用备份范围，新恢复项目正常开放。

新增 maintenance.py 单次协调：新目录/容量预检 → 关闭写入 → 停止原后端 → 原有一致备份与再次完整校验 → 启动同一容器 → 解除保护。正常失败/可处理中断执行恢复，强制中断保留回执供 resume；容器 ID/owner 不符拒绝自动解除。子命令中断/超时清理自己创建的进程并限时回收，没有无限等待或输出命令 stderr 中的凭证。

## 改动文件

- backend/src/main/java/cn/sparkdq/Maintenance.java：授权、持久标识、互斥进入/退出和活动任务检查。
- MaintenanceApi.java、MaintenanceFilter.java、App.java：内部控制、全写入准入和健康 maintenance 状态。
- backend/src/test/java/cn/sparkdq/MaintenanceTest.java：并发排空、权限、所有写入口、owner/重启测试。
- infrastructure/maintenance.py、storage.py：备份协调、恢复回执、现有存储子命令的中断清理。
- tests/test_maintenance.py、test_storage_runner.py、maintenance_acceptance.py：故障单元与真实隔离验收。
- README.md、docs/maintenance.md、roadmap.md、storage.md、development.md、本日志：使用方法、保证范围及证据。

## 测试与真实证据

- Maven package 成功：42 项 Java 测试，0 失败/错误/跳过，包括新增 8 项维护测试；真实并发测试证明旧写入未完成时不会提前检查/进入，后续写入不越过等待中的维护。
- Python 48 项标准库测试全部通过（此前 32 + 协调 11 + 子命令 5）。覆盖原数据/输出目录保护、容量不足、忙碌/丢响应、备份失败恢复、恢复失败保留、同容器/owner 防错、临时文件碰撞不删除、超时/中断回收与错误输出不泄漏。
- 隔离验收 8 组全部通过：启动旧记录/文件不变、未授权拒绝、忙碌 Spark 拒绝进入且正常成功、全部写入口拒绝而读取保留、同容器重启标识保留/密钥轮换/回执恢复、成功备份后同容器恢复、备份独立恢复后真实 Spark 复检一致、真实强制杀掉隔离备份协调进程后的恢复，以及缺失文件对账失败后恢复可写。详细每项见 .runtime/maintenance-1de6f8aef675/acceptance.json。
- 对强制终止，仅杀本轮隔离备份工具进程；没有杀原服务/用户计算或删除任何卷。离线回执已落盘、锁释放，resume 恢复后 backend 同一 ID，未完成备份没有 COMPLETE。
- 对账失败探针只插入隔离副本：保留缺失文件记录和不完整备份，finally 恢复服务，随后真实上传成功。源数据库没有该探针。
- 首次部署主库前确认无活动任务并留旧应用一致备份；更新后先核对，再实际执行新维护流程成功。156 数据集/203 任务、13 表（含 2 条迁移历史）、全部业务行/文件/原 .env 在部署和受控备份前后均不变。
- 本次新备份主要产物约 278.3 MiB，回执为 complete/serviceResumed=true/maintenanceReleased=true。对应主库快照含 156 数据集、203 任务，warnings=0；不把随后新增回归样本说成已纳入这一快照。
- 该主库备份又恢复为新项目 spark-dq-restore-31a32c5e87f4，所有表数量/归档文件核对，maintenance=false。真实 Spark 复检 86872f5f-36ef-4396-b3ba-50c48eb038ef 对原 7 行任务获得相同三规则指标，且复检 ID 在源库不存在。
- 新服务上 e2e.py 通过 CSV 格式/BOM、多行/空值/重复、复合和 NULL 键、清洁数据、异常预览。7 行成功任务 7f3788fb-ef6f-4a79-a988-06e116de2c59，name 合计 3、email 2、重复 1 组/2 行/1 多余。
- governance_persist.py、rules_persist.py、governance_transforms_persist.py、cancellation_persist.py --require-recovery 均通过。它们实际跨过本次主后端重启，核对既有版本/策略/模板/报告/文件和取消恢复证据，不重新伪造历史。
- 当前主库 160 数据集、206 任务，新增的仅为本轮回归样本；活动任务 0、迁移历史仍 2。通过前端代理 /api/health 返回 UP/schemaVersion=2/maintenance=false，页面可继续使用。Java 后端已重建，Vue 未改动，本轮未重跑前端构建。

## 实际遇到并修复

首轮隔离验收重启后沿用旧临时端口出现 ConnectionRefused；生产协调器原本每次查询当前端口，问题在验收脚本，现已修正并从新副本完整重跑，首轮日志保留。子命令中断测试最初期望 InterruptedError 直接抛出，现有工具合同会将 OSError 子类转换为 StorageError；修正测试期望后全部 48 项通过，进程清理断言仍保留。没有把失败运行计作通过。

## 恢复点与可查日志

- 旧版回退备份：/home/sparkdq/projects/spark-dq/.runtime/maintenance-deploy-12008e020c7d/pre-upgrade-backup。
- 受控新备份：/home/sparkdq/projects/spark-dq/.runtime/maintenance-deploy-12008e020c7d/managed-backup/backup，同级父目录 receipt.json 可用于维护恢复；不要用恢复工具覆盖原卷。
- 主库部署/备份核对：.runtime/maintenance-deploy-12008e020c7d/verified.json。
- 新备份恢复证明：/home/sparkdq/projects/spark-dq/.runtime/maintenance-primary-restore-5e08b0d0688f/spark-verified.json。
- Maven、Python、隔离验收、主库恢复与回归日志均为 .runtime/maintenance-*.log；首轮失败分别 maintenance-acceptance-first-failure.log、maintenance-python-tests-first-failure.log。
- 所有隔离测试容器已停止，卷、成功/失败/中断备份和探针保留；只有原三项业务容器运行。实际数据备份留在 WSL 私有目录，导出仅日志、摘要和清单，不导出控制密钥/.env/数据 SQL/tar。

## 范围与下一步

本阶段是单后端、同容器、单次维护；不协调直接 SQL/文件写入、其他实例或手工 Docker 操作，不保证整机断电事务。首次安装必须用既有空闲窗口，后续单次备份才由新准入保护；维护中不要手动重建容器。标识损坏、owner/容器变化、恢复失败时保留现场，不能通过清空标识或篡改回执掩盖。

尚未设置定时任务、自动保留删除、开机自启、长期续期或异机托管。原 12 小时后台会话 9a3e7fc1ef514db0be9c8257c10fac67 仍在运行，没有改其期限或 Windows 设置。用户无需补配置、复制密钥或执行恢复；可直接使用 http://localhost:15173/。下一阶段可在该基础上设计定时备份的空闲窗口策略，或推进治理映射/业务去重规则。
