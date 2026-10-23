# WSL 运行会话与备份巡检

当前系统是 WSL2 中的本地 Docker 应用。systemd=true、Docker enabled 已存在，但 systemd 服务本身不会保持 WSL 实例常驻，见 [Microsoft 官方说明](https://learn.microsoft.com/en-us/windows/wsl/systemd)。本阶段提供独立 Windows 后台 WSL 进程持有运行会话，没有改写系统启动项、全局 WSL 设置或注册定时任务。

## Windows 后台入口

infrastructure/host.ps1 可复制到 Windows 本地目录使用；本轮已导出 outputs/SparkDQ-runtime.ps1。默认发行版 SparkDQ-Ubuntu、项目 /home/sparkdq/projects/spark-dq，可用参数覆盖，但拒绝包含空格/引号或父目录跳转的路径。脚本使用现有 WSL root 权限，不读取/打印 .env。

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\SparkDQ-runtime.ps1 -Action start -Seconds 43200
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\SparkDQ-runtime.ps1 -Action status
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\SparkDQ-runtime.ps1 -Action stop
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\SparkDQ-runtime.ps1 -Action capacity

ExecutionPolicy 只作用于本次 PowerShell 进程，不更改系统策略。启动入口以隐藏窗口创建独立 wsl.exe，启动命令退出后会话仍运行；没有用户登录或复制步骤。本轮已实际开启 12 小时会话并核对状态。

会话时长 1..43200 秒，默认 12 小时，有明确 UTC startedAt/expiresAt。单实例文件锁阻止重复会话；重复 start 返回现有会话，不续期、不重建活跃后端。空闲时 stop 再 start 才开启新期限。启动后端已运行时保留其原容器，避免因镜像标签/配置差异重建并打断计算；只在后端未运行时使用现有镜像启动服务，不构建、不拉取。已有服务健康失败时记录问题，不自动重启。

状态持久化在 .runtime/service-session.json（0600、原子替换），包括 Linux PID、进程启动标识、会话 ID、期限和健康状态；status 会重新检查进程身份，旧状态不冒充存活。启动/状态命令及子操作都有超时。启动命令应直接运行；如需保存结果，另行调用 status，避免 PowerShell 的原生命令管道等待后台进程结束。

stop 只释放本会话，不停止 Docker 容器、不取消任务、不删除文件。若检测到 QUEUED/RUNNING/CANCELLING 或数据库不可查询，会拒绝释放；会话端收到信号也复核。到期有活动任务时最多延长 50 分钟等待结束；达到该上限仍会退出并记录 activeAtExit，不保证无限期等待。期限同时检查单调时钟与墙上时钟，避免回拨无限延长或休眠恢复后忽略原期限。

这不是 7×24 托管：Windows 关机/重启不自动恢复，会话到期或主动停止后 WSL 仍可能空闲退出。未修改电脑休眠设置。需要自动开机/登录启动、持续续期或跨主机服务时应另行设计，不把本轮短时验收称为经过 12 小时持续测试。

## 备份目录巡检

infrastructure/backup_catalog.py 只读扫描本项目 .runtime 或显式 --root。它不创建备份、不停止服务、不恢复、不删除。候选由 manifest.json、COMPLETE、database.sql 定位；跳过目录链接并拒绝链接/缺失/大小不符的产物，限制元数据文件大小，检查完整标记绑定、时间、12/13/14 表布局和文件大小。

    # 受保护备份目录需使用现有 WSL root 用户读取
    timeout 180s python3 infrastructure/backup_catalog.py --verify-latest 2 --keep 3 --output .runtime/catalog-new.json

默认 verify-latest=0 只核对元数据/大小，不能证明 SQL/tar 内容未损坏，报告明确标为 not-run。verify-latest=N 从最新候选开始调用已有完整校验，失败则记录并继续尝试较早候选，最多取得 N 份通过校验的备份。freshVerifiedBackup 必须来自本次完整校验，默认时间阈值 24 小时。只有旧备份、无有效完整校验或容量估算不足时可返回非零；报告文件拒绝覆盖。无法读取的目录会记录 scanErrors 并令 scanComplete=false，命令返回非零，避免把权限不足误报为空目录。

keep 默认 3 仅产生 keep/review-only/inspect 建议，不执行保留期删除；没有把损坏的新备份用于替代已知有效备份。完整校验也不等于再次恢复演练，旧阶段恢复证据另见 storage.md/migrations.md。

容量估算以已知最大备份产物大小 ×1.25，加默认 1 GiB 余量计算，只作为提醒，不保证未来数据增长或压缩比例。Linux 可见空间不能代表稀疏 VHDX 宿主盘的真实余量：host.ps1 capacity 会只读查询该发行版注册位置及对应 Windows 卷剩余空间，查询失败时标为 unknown。可用 -RequiredFreeBytes 传入巡检报告的 requiredFreeBytesEstimate 同时比较。

当前不自动调度新备份，不加密/上传，也不处理过期备份清理。新任务仍需按原离线备份流程捕获；“24 小时内有已验证备份”不意味着当前每个新任务都已备份。

## 验证方式

    timeout 60s python3 -m unittest discover -s tests -p 'test_*.py' -v
    timeout 480s python3 tests/operations_acceptance.py

第二项要求系统无活动任务、没有正在运行的后台会话，并已通过 e2e.py 生成测试夹具（不要为了测试强停用户会话），使用真实主服务创建少量合成任务和一个 10 万行合成 CSV，保留它们。验证单实例、有任务拒绝停止、成功后可停止、到期等待 Spark 完成、原容器和历史任务保持。finally 不停止容器或取消任务。现有 12 小时会话运行期间应使用状态/只读巡检，不直接再次运行该生命周期验收。

## 业务恢复点对账

V5巡检兼容及可选--compare-current已补齐；新备份记录一致业务指纹，恢复到新卷后在启动应用前核对。旧备份未记录指纹时保留unknown，不回填。详见[恢复点说明](recovery-points.md)。
