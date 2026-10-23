# 真实超时与恢复验收

在全新同机双Worker候选环境中，以60秒测试预算完成基线、准备超时、准备超时后复检、Engine超时及复检，共5个HTTP任务。后端业务仍Java/Spark；Python只用于HTTP验收、故障注入和证据核对。主环境300秒预算不变。

## 创建和执行

按[隔离部署指南](cluster-lab.md)确认容量、镜像和主任务空闲，使用新的实验目录和刚获取的Windows后备盘容量文件。预算在创建时冻结到独立Compose，并纳入原有配置摘要；不能通过serve修改旧项目配置。

```sh
timeout 90s python3 infrastructure/cluster_lab.py create --output "$PWD/.runtime/timeout-lab-NEW" --host-capacity /mnt/c/path/to/current-capacity.json --backend-image spark-dq-backend:header-api-20261020 --task-timeout-seconds 60
python3 infrastructure/cluster_lab.py serve --output "$PWD/.runtime/timeout-lab-NEW" --seconds 3600
```

在另一非交互进程运行：

```sh
timeout --signal=TERM --kill-after=30s 970s python3 infrastructure/cluster_timeout.py --lab "$PWD/.runtime/timeout-lab-NEW" --output "$PWD/.runtime/cluster-timeout-NEW" --engine-sha256 ACTUAL_SHA256
timeout 90s python3 -m unittest discover -s tests -p 'test_*.py'
```

ACTUAL_SHA256需核对容器Engine JAR实际摘要。lab只允许5..300秒，默认300；本验收工具固定要求60，拒绝旧的非空环境，不改变主配置。单个终态等待110秒，整段内部900秒，外层970秒后TERM、30秒后KILL；所有进程/容器操作均有限时。试验失败后保留原目录，重跑创建新实验。

## 故障矩阵

1. 基线使用1000行、七条规则及CSV执行副本，确认完整结果和两个Worker实际完成Task。
2. 对本任务PreparedInput子进程发送SIGSTOP，让现有后端预算真实到期并终止进程。要求FAILED、CLEAR、无结果指标、无Engine回执、未登记副本清单SHA。
3. 使用失败任务的冻结规则/输入/副本计划复检，验证全量异常和独立预期，保留原失败记录。
4. 等待另一个任务副本准备成功及Engine应用STARTED后，仅暂停该任务的Engine进程；短时暂停本实验自有Master，使超时后的资源核对暂时不可达。要求错误真实记录“准备与检测合计超过60秒”，但任务保持RUNNING待清理、指标为空、报告409、新任务409，资源门禁不接受任务。恢复Master后才允许FAILED/CLEAR。
5. 从Engine超时任务再次复检成功，随后重启同一个候选后端，确认全部终态任务、报告和原下载字节不变。副本/异常文件和数据库卷保留。

SIGSTOP目标必须同时匹配容器完整ID/cgroup、后端直接子进程、精确main class、精确任务请求路径和/proc启动时钟；不会按进程名全局暂停。恢复前再次核对身份，已退出或PID复用的进程不得发送SIGCONT。Master操作前核对自有标签且拒绝接管已暂停的Master。正常/异常/TERM退出均尝试恢复自有Master和仍存活的暂停子进程；SIGKILL/断电不能执行finally，需按私有回执核对本实验容器后恢复，不进行全局unpause/删除。

## 时间与证据口径

`shared-budget.json`分别保存首次持久化超时错误时的duration_ms、副本内部preparationMs以及清理后FAILED终态duration_ms。首次超时时长应小于“60秒＋已耗副本时间”，从而拒绝为Engine另起一份60秒的观察结果。终态时长还包含集群核对等待，不能用它冒充纯计算耗时或工作进程预算到期时刻。最终报告保留原应用身份；被强杀的Driver可能只有STARTED回执，不能补造ENDED。

结果目录包含逐任务真实日志/请求/副本/异常/回执、四次门禁观察、共享预算记录、历史报告、主数据只读指纹和汇总。COMPLETE绑定摘要/任务记录/门禁/预算文件；缺失COMPLETE不能声称整体通过。主数据文件和镜像前后不一致、错误状态或任何独立预期不符都阻止完整通过。

这是同机隔离的受控故障证明，未模拟CPU饱和、跨物理机网络、全天运行或生产HA；不据此扩大上传容量、改变主预算、宣布格式提速或上线候选部署。实际结果以本阶段开发日志为准。
