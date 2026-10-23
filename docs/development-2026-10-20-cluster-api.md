# 开发日志：受控Standalone HTTP执行与清理协调（2026-10-20）

起点9d341ad，工作区干净。上阶段只有持久作业回执和离线只读对账，网页任务仍local。本阶段实现服务级固定拓扑适配和任务状态协调，不对主环境部署。

## 改动

ClusterExecution.java新增受信配置、子进程环境隔离、create-only意图/清理回执、有界HTTP全节点核对和配置指纹。Tasks.java接入成功发布前清理、取消/失败/超时延后终态、后台重试、重启待核对和新任务门禁；本地模式保留原有生命周期。Engine.java支持Client jar分发、固定资源预算、注册等待和可选实际TaskEnd证据。cluster-worker.Dockerfile解决共享卷UID一致性；cluster_api_trial.py复用专用恢复副本验收，新增Java及运维测试。

## 真实问题与修复

1. 首轮Java回归发现新增门禁误限制本地旧任务队列，原有取消清理重试测试超时。已将门禁限定到持久登记的集群任务，保留本地语义；后续完整回归通过。
2. 新增证据路径推导遇到单测相对request路径无父目录，已先解析绝对路径；完整回归通过。
3. 实验a：4个真实HTTP/Spark业务任务通过，但一万行3规则任务只观测一个Worker参与，双Worker验收未通过。加入有界资源注册等待，规模测试扩为7规则并逐项核对独立预期。a目录保留，不记完整通过。
4. 实验b：重跑时create-only全局开发指针冲突，尚未启动节点即拒绝；改成每轮输出目录内独立project.json，保留原指针和目录，没有覆盖旧证据。
5. 实验c为最终完整验收；所有失败日志和部分目录保留，未虚构或合并成一次成功运行。

## 证据

- .runtime/cluster-api-maven-5.log：126项Java，零失败/错误/跳过。
- .runtime/cluster-api-python.log：128项运维测试通过，包含节点所有权和暂停恢复检查。
- .runtime/cluster-api-20261020-c/verified.json：4个真实HTTP集群任务、原生导入、治理/复检/报告、Master不可达取消待确认、Client强制中断恢复门禁及独立文件对账。
- 同目录large-api-metrics-verified.json、two-worker-proof.json：一万行七规则逐项独立核验与真实TaskEnd参与证据。
- unavailable-cancellation.json、unavailable-recovery.json：通信故障时不提前发布终态，拒绝新任务409；恢复后清理核对通过，再进入CANCELLED/FAILED。

## 数据保护和未完成事项

只复用了此前从备份恢复、已停止的spark-dq-restore-69aa13eb17b4专用副本；记录与卷保留，追加本阶段合成任务。主数据、文件和镜像前后指纹一致。无远程推送、无主镜像替换、无删除用户数据或重置仓库。业务仍Java/Spark、Spring Boot、Vue、MySQL，Python是编排/验收。

当前是受控同机API模式，未切换主环境，也未验证集群浏览器交互。尚无多后端、任意进程热重启、跨主机共享存储、生产鉴权或HA。固定拓扑的两次快照观察不等同于通用分布式故障证明。

最终一万行任务实际成功Task数：{"worker2": 1, "worker1": 239}。各Worker计数为观测值，不用于计算加速比。

## 最终补充回归

真实5秒集群超时通过：终止Client Driver并完成全节点清理核对后才FAILED，无业务结果，见timeout-verified.json。相同最终镜像切回默认local后，原生导入及4个真实API/Spark任务、报告、同容器重启和离线文件对账通过，见local-verified.json、final-audit.json。两份配置仅作用于专用恢复副本，未替换主镜像。

额外只读诊断曾在候选已停止后查询端口，被正确拒绝；最终一万行独立指标核验在实验c内完成，不将该诊断失败记作测试通过。最终主业务、文件和镜像再次核对不变，只有主环境3个容器运行；所有本轮节点停止，卷和日志保留。聚合证据为aggregate-verified.json。Java126/运维128及新增脚本语法、差异格式检查通过。
