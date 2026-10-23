# 开发日志：同机双Worker Spark Standalone（2026-10-20）

## 真实起点与实现

起点提交fda5b3b，工作区干净；已有CSV/多CSV/原生Parquet、规则快照、治理版本/复检/报告、V5备份与业务恢复点功能。Engine仍将Driver固定127.0.0.1，未有真实集群任务参与证据。因此选择离线最小集群增量，保留线上local运行方式。

变更：Engine.java接入DriverNetwork.java；新增ClusterEvidence.java可选SparkListener及对应Java测试；infrastructure/standalone_trial.py生成样本、冻结输入、启动隔离集群、验证完整内容和失败清理；tests/test_standalone_trial.py约束参与证据和资源所有权。README、roadmap、development和standalone文档同步。

## 真实失败与修正

1. standalone-20261020-a：本地和远程数据核验通过，但参与证据误以Docker IP对比Spark配置主机名。改为核对受控Worker主机名，另存IP映射；不是指标成功即视为完整验收通过。
2. b：Docker进程查询缺PID字段，被Docker拒绝；改用带PID字段的输出格式并补测试。
3. c：检测、治理与取消通过，复检结果正确但实际仅一个Worker有成功任务。原“每个作业都要求双Worker”过严；检测/治理继续要求双Worker，复检记录实际参与者且要求远程执行，不作均衡声明。
4. d：Docker默认网络地址池耗尽。仅核对并回收本阶段a/b/c三个已有空临时网络，保留容器和日志，不清理其他项目资源。新增结束时核对所属空网络并移除，拒绝有连接或所有权不符的网络；补2项测试。
5. e：完整流程全部通过，为最终验收；上述失败目录及证据保留，不标记为完整成功。

## 最终证据

- .runtime/standalone-20261020-e/verified.json：完整验收通过；10000行、双Worker检测/治理、治理5000行、取消/复用、错误指纹拒绝、主环境保护和资源收尾。
- 同目录distributed-verified.json、governance-verified.json、recheck-verified.json保存逐规则指标、完整异常指纹、执行参数及真实TaskEnd参与计数；output/worker*-runtime保留Executor日志。
- .runtime/standalone-api-fbffc1fd2cd2/verified.json：从备份新卷恢复的候选local API，原生导入、4任务、离线对账、重启验证通过；候选已停止，卷保留。
- .runtime/cluster-maven.log及surefire报告：107项Java、零失败/错误/跳过。
- .runtime/cluster-python-accepted.log：114项Python运维/验收测试通过；git diff --check通过。
- .runtime/standalone-network-reclaim.json：三个本阶段所属空网络的回收记录。

主环境业务SHA256为1c2caca9b8dff53db7627c96c051d4cd7aa665fc3f9fb71961daa2fded3bb0c6，实验前后一致。225数据集/292任务/1导入，V5；最终仅主环境3个容器运行。源文件、镜像、备份未被覆盖；无远程推送。独立实验尚无线上集群API、跨物理机共享存储和HA，不宣称加速比。

复现命令、镜像摘要、资源预算、超时和网络边界见standalone.md。当前无必须由用户处理的开发阻塞。
