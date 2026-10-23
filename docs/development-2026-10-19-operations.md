# SparkDQ 后台运行与备份巡检阶段

完成时间：2026-10-19T10:40:03+08:00。起点 main / 18076d9，初始工作区干净。项目 /home/sparkdq/projects/spark-dq。本轮没有远程推送、发布、删卷或删除旧备份。

## 交付

- infrastructure/runtime.py：有期限的前台 WSL 会话、单实例锁、原子私有状态、PID/启动时间身份检查、健康记录、有任务拒绝停止和到期排空等待。
- infrastructure/host.ps1：隐藏 Windows 后台启动、状态/停止入口，45 秒状态命令超时与 230 秒启动观察窗口；只读宿主卷容量检查。不会改全局设置或创建开机/定时任务。
- infrastructure/backup_catalog.py：只读目录、完整性/新鲜度、容量估算和保留建议，明确区分元数据检查与完整验证；损坏候选回退，绝不自动删除。
- tests/test_operations.py：新增 17 项边界测试；tests/operations_acceptance.py：真实会话与 Spark 生命周期。
- docs/runtime.md、README.md、roadmap.md、development.md：操作方式、证据和限制。

## 真实结果

32 项 Python 测试通过（新增运维 17 项 + 原存储 15 项），包括损坏/缺失/链接、同大小篡改、旧备份回退、无法读取目录显式报错、年龄/容量不足、保留不删除、旧 PID、不相关进程保护、重复启动、繁忙停止拒绝、启动失败保留和时钟变化。本轮没有修改 Java、Vue、镜像或数据库结构，因此没有将上一阶段 34 项 Java 测试当作本轮新结果，也没有重复构建它们。

Windows 入口实际启动并查询过短期限会话，父启动命令退出后会话继续；重复启动不改变期限或重建后端；手动停止和自然到期后容器仍保留、API UP。曾因 Windows PowerShell Start-Process 返回的 ExitCode 缓存为空使状态查询误报失败，已改为显式 Process 句柄与并行读取输出，并重测。另发现将启动命令直接接入 PowerShell 管道会等待后台后代，改为直接启动、另行 status 捕获；日志记录的是实际修正后的入口。

真实生命周期验收最终回执 /home/sparkdq/projects/spark-dq/.runtime/operations-acceptance-9ff3fdcaa9ad.json。同一后端容器全程未被重建；计算中停止请求被拒绝，任务正常成功；1 秒租约到期后观察到 draining，等待 10 万行真实 Spark 完成再退出。任务 9f88ee9c-3cb9-4773-a087-58282cc3e525、数据集 e42c0f8d-667a-446d-9e6e-14a095056462：空值命中 25000，重复 50000 组/100000 行/50000 多余。没有将这个正确性样本当作大规模/分布式性能基准。加入时钟边界保护后完整重跑通过，旧回执也保留。

主服务后续 e2e.py 通过真实 7 行空值/重复、复合/NULL 键、清洁文件和异常预览；任务 c5d1124b-1fc8-47e6-bb99-1f91e06ed558。governance_persist.py、rules_persist.py、governance_transforms_persist.py、cancellation_persist.py --require-recovery 全部通过。它们是本轮读取检查；本轮未为这些检查重新启动业务后端，未冒称额外完成崩溃恢复实验。

备份扫描识别 3 份完整元数据候选、1 份历史不完整候选；最新两份（旧 12 表主库迁移前备份、13 表迁移副本备份）完整校验通过，最老一份本轮未重跑完整校验。另以普通用户实测：5 个受限目录明确记录 PermissionError、scanComplete=false，退出码 2；root 完整扫描 scanComplete=true。所有备份原样保留；没有以不完整目录作为恢复点。容量估算需要 1421482549 字节；Linux 空闲约 928.4 GiB。实际 VHDX 位于 D:\WSL\SparkDQ，Windows 对应卷空闲约 59.8 GiB，也满足该估算。宿主注册信息仅只读查询；普通沙箱读取被限制后，经本轮自动审批的只读操作完成，没有修改注册表。

## 当前后台会话

已实际开启独立 Windows 后台 WSL 会话 9a3e7fc1ef514db0be9c8257c10fac67，Linux PID 88848；默认 12 小时，预计到期 2026-10-19 22:37:38（Asia/Shanghai）。状态 UP / schemaVersion 2。父启动命令已结束，另行 status 确认进程仍存活；没有等待 12 小时后声称耐久测试已完成。到期有任务最多再等待 50 分钟；这不是永久后台托管。

用户无需再执行启动或迁移，可继续使用 http://localhost:15173/。最终状态与容量回执已导出至 outputs；原始状态 .runtime/service-session.json 为 0600。新的运行会话不是 Windows 开机自动启动，关机/重启后不自动恢复。

## 证据与遗留

.runtime/operations-tests.log、operations-acceptance.log、operations-acceptance-initial.log、runtime-backup-catalog-final.json、operations-e2e.log 及 operations-*-persist.log 保留全部结果。Windows 入口日志位于导出脚本旁 .runtime，只有会话状态与错误，不含 .env 内容。

尚未实现定时备份、自动保留清理、登录/开机自启、长期续期、告警通知、异机恢复或多实例协调；备份新鲜度不保证本轮新增样本都已捕获。会话健康失败只记录不自动重启，以兼容维护停机和避免中断任务。仍为单后端、20 MiB、串行本地 Spark；全局系统/休眠设置保持原状。下一步可做维护窗口与备份自动化的任务准入保护，或回到治理策略扩展。
