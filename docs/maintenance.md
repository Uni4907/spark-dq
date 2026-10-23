# 单次空闲维护备份

本阶段在已有离线对账/备份之上增加“停止接收新写入 → 确认无活动任务 → 停止原后端 → 备份与完整校验 → 启动同一容器 → 解除保护”的单次流程。解决原先检查任务数之后又收到新任务的窗口。工具不取消任务、不重建容器、不覆盖恢复原卷、不删除旧备份，也不设置定时任务。

## 准入与内部控制

MaintenanceFilter 以公平读写锁覆盖所有非 GET/HEAD/OPTIONS 的 HTTP 请求。普通写入持有读锁直到请求处理结束；进入维护取得写锁，最多等 10 秒，再查询 QUEUED/RUNNING/CANCELLING 数量。确认零活动任务后原子写入本维护 owner，并阻止后续写入。进入中的新写入最多等 1 秒后返回 503；维护中写入返回 503、Retry-After:30 和明确中文提示。已有上传/模板写入也在同一保护内。

活跃 Spark 任务使进入请求返回 409，工具不会先停止它。读取在保护期间仍可用；实际备份需要短暂停止后端，所以停机期间 API 会暂时不可用。GET /api/health 的 status=UP 表示进程健康，新增 maintenance 布尔值表示写入是否受保护。

内部 GET /api/internal/maintenance、POST /enter、POST /leave 需要 X-SparkDQ-Maintenance-Key。应用每次启动自动生成随机密钥并存入容器 /tmp/sparkdq-maintenance.key（0600）；工具通过已有 Docker 权限在内部读取，不打印、不放在命令参数、回执或备份中。普通页面不能发起维护；缺失/错误密钥返回 403。没有让用户新配账号或复制密钥。

owner 是独立 UUID。同一 owner 进入/退出幂等，另一个 owner 不能解除保护。标识原子写入 /tmp/sparkdq-maintenance.owner（0600）；同一容器 stop/start 后继续关闭准入，密钥重新生成。标识损坏会阻止应用正常启动，避免悄悄开放。控制文件不属于 /data 或 /app，因此不进入备份；恢复到新项目会生成新密钥并正常接受写入。

## 使用与恢复

在 WSL 项目目录、现有 Docker 权限用户下：

    timeout 1500s python3 infrastructure/maintenance.py backup --output .runtime/maintenance-backup-新名称
    timeout 900s python3 infrastructure/maintenance.py resume .runtime/maintenance-backup-新名称/receipt.json

backup 要求后端和 MySQL 原本运行、健康且没有其他维护 owner。原本停机时拒绝，不擅自启动。输出目录必须全新；父目录必须存在。启动前按应用、文件、数据库大小和余量估算 Linux 空间；可用 --host-capacity 指向 host.ps1 capacity 产生的对应宿主磁盘报告，同时拒绝已知不足的空间。应传入当前发行版的新报告，估算仍不是未来增长保证。

支持 --config/--env 指向隔离 Compose 项目。工具与 storage.py 使用同一个非阻塞锁，防止两个存储维护操作重叠。已有后台运行会话不自动重启服务，备份期间继续持有 WSL 会话。

回执 receipt.json（0600，父目录 0700）在进入保护前就落盘，随后原子更新 prepared/guarded/offline/backup-verified/complete 或 failed 状态。它保存 owner、原容器 ID 和 Compose 配置路径，不保存密码或控制密钥。backup/ 子目录使用原 sparkdq-backup-v1 格式，包含 SQL、数据、实际应用、对账和 COMPLETE；再次完整校验通过才算备份成功。

正常失败和可处理的中断进入恢复分支：只启动原来的容器，等待健康，再读取当前密钥并核对 owner 后解除保护。若恢复失败，回执保留 needsResume=true；resume 无需重新填写配置，只处理记录的同一容器。强制杀掉工具时 finally 不会执行，仍可用已写回执恢复；本轮对此做了真实隔离故障测试。

子进程有有限超时；超时/中断时杀掉本工具创建的命令子进程并限时回收，避免异常路径无期限等待。不会为了恢复而取消 Spark 任务、改写数据库终态或强行解除其他 owner。

## 保证范围

当前是单后端 HTTP 准入保护，不防止另一个应用实例、直接 SQL/文件写入或手工 Docker 操作绕过流程。维护期间不要手动重建容器；控制标识只随同一容器保留，容器重建不是本流程的安全恢复方式。工具发现容器 ID 或 owner 改变会拒绝自动解除保护。

进程中断恢复不是整机断电事务保证；备份失败后的部分目录保留，缺少 COMPLETE 不能恢复。对账问题会导致备份失败，工具不会自动修补或删除原数据。工具恢复服务成功不等于该次备份成功，failed/recovered 与 complete 明确区分。

首次部署本保护需要一次既有的空闲维护窗口，先备份旧应用再替换；后续备份才由新准入流程协调。本阶段没有数据库结构迁移，schemaVersion 保持 2。尚未实现定时调度、自动清理、权限体系或多实例维护锁。

## 复现验证

    timeout 60s python3 -m unittest discover -s tests -p 'test_*.py' -v
    timeout 1200s python3 tests/maintenance_acceptance.py --backup /完整且可信的备份目录

验收前需构建候选 spark-dq-backend:maintenance；脚本只在新项目/新卷中测试，保留所有卷与故障探针并在 finally 停止隔离容器。覆盖忙碌任务拒绝、所有写入口、不同 owner、重启保护/密钥轮换、回执恢复、成功备份、独立恢复后的真实 Spark 复检、强制终止备份进程后的恢复，以及只在隔离副本插入缺失文件探针导致对账失败后的自动恢复。不要把这些故障探针添加到主库。
