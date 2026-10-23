# 开发日志：受控超时与恢复（2026-10-20）

从ce08935干净工作树继续；实际检查Java调度器预算、状态发布、清理门禁和报告逻辑。现有实现准备和Engine共用start计时，远程失败要等集群清理确认；本轮针对既有行为补齐真实实验，没有虚构新业务能力。

## 文件与实现

- infrastructure/cluster_lab.py：新增仅create可选--task-timeout-seconds，严格整数5..300，默认300；冻结独立Compose配置并保存taskTimeoutSeconds元数据，保留旧项目加载兼容。模板预算歧义拒绝，不改主.env。
- infrastructure/cluster_timeout.py：全新候选环境固定60秒实验；自有直接子进程身份/cgroup/PID启动时钟核对后SIGSTOP，真实后端超时，Master临时故障时四次任务/门禁/提交/报告观察；恢复、失败后复检、实际后端重启、历史持久性及主数据指纹保护。
- tests/test_cluster_timeout.py：新增6项测试，覆盖PID复用/退出/错误任务或父进程/容器拒绝、预算类型和范围、错误预算拒绝副作用、模板歧义。
- docs/cluster-timeout.md、docs/progress-delivery-cluster-timeout-2026-10-20.md、本文：可复现命令、全部证据/口径/边界与当前进度；README.md、docs/development.md、docs/roadmap.md和docs/cluster-lab.md更新入口/预算说明。

## 验证

202项运维测试全部通过（实验前和最终各一次）。5HTTP：3成功、2预期FAILED；7次空闲检查，4次准入/报告409。成功任务的全量异常/七规则及两个Worker真实任务通过；准备超时无清单绑定/无Engine回执，Engine超时保留STARTED原身份。主数据恢复点/文件/镜像与运行前一致。

| 场景 | 实际终态 | 持久化终态耗时秒 | Engine回执 |
| --- | --- | --- | --- |
| baseline | SUCCEEDED | 24.003 | DRIVER_ENDED |
| prepare-timeout | FAILED | 60.258 | ABSENT |
| prepare-replay | SUCCEEDED | 23.777 | DRIVER_ENDED |
| engine-timeout | FAILED | 64.303 | STARTED |
| engine-replay | SUCCEEDED | 23.231 | DRIVER_ENDED |

共享预算首次到期60.000秒，副本内部5.013秒，终态64.303秒；清理等待单独解释，未误认为超时预算失效。Master暂停49.960秒并已恢复。失败报告无outputDataset/comparison；后端重启后所有任务/报告和下载字节保持。Java/Node未变未重跑，先前135项Java仅作既有记录。

所有部分/失败文件保留在/home/sparkdq/projects/spark-dq/.runtime/cluster-timeout-20261020-a；原始包带精确实验源码、实际JAR及元数据，不打包lab.env或主业务行。主环境未替换、无远程推送/发布。SIGKILL/断电无法保证finally执行，恢复步骤和受控实验边界见复现指南；不能以本试验声称CPU过载/跨主机/HA完成。
