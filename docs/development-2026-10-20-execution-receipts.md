# 开发日志：持久化作业身份与只读对账（2026-10-20）

起点0594956，工作区干净。已有同机双Worker离线验证，但缺少可跨查询/重启保留的业务任务与Spark作业绑定。本阶段先补齐这一独立增量，未切换主服务执行模式。

## 实现

- spark-engine/ExecutionReceipt.java：提交/启动/结束的create-only持久化文件、请求SHA与随机attemptId，Engine使用唯一应用名并把绑定放入result.execution.receipt。
- backend/ExecutionReceipts.java及Tasks.java：有界、拒绝符号链接、严格字段和指纹校验；任务详情及现有报告增加execution，历史为ABSENT、损坏为INVALID。
- infrastructure/execution_receipts.py：显式受信Master快照或只读GET的作业观察；ID及唯一应用名双重匹配；未知/不可达/冲突不判成功，不写业务状态。
- infrastructure/receipt_trial.py：复用现有独立预期和双Worker验收，增加真实ACTIVE/历史观察、SIGKILL与持久身份核对。
- Java回执/读取测试、Python对账测试及tests/execution_receipts_e2e.py、契约与路线图文档。

## 验证证据

- .runtime/receipts-maven-final.log：117项Java，零失败/错误/跳过。
- .runtime/receipts-python.log：124项运维/工具测试通过。
- .runtime/receipts-cluster-20261020-a/verified.json：1万行、双Worker检测/治理、完整异常指纹及治理5000行独立核验；取消后复用复检、错误清单拒绝、主环境保护、所属空网络清理通过。各*-receipt.json、*-observation.json保存真实身份与观察。
- 同目录cancel-verified.json：在两个Executor均分配且Master实际ACTIVE后，SIGKILL Driver，无result/ended回执，applicationId保留；Executor进程退出。Master历史实际标FINISHED，工具仍明确不推断业务成功或远程清理，说明分离状态的必要性。
- .runtime/receipts-api-a1a6879ab005/verified.json：新卷隔离候选镜像，原生导入+4个真实API任务、规则/治理/复检/报告、历史ABSENT、损坏回执INVALID、精确恢复原文件、重启及对账通过。

## 实际问题

首次构建命令的容器工作目录未挂入实际项目，Maven报告No POM，未运行测试。改用参数数组和明确绝对路径后构建成功，保留receipts-maven.log失败日志和后续成功日志。准备脚本误用了不可执行的Windows Python别名，改为既有WSL Python，不安装/修改Windows运行时。两项均为工具调用问题，不记为测试成功。

## 边界与后续

回执是文件持久登记，不是新增的数据库集群作业表；任务可按ID查询，尚无独立按applicationId索引的管理API。没有在线集群提交、自动远程取消/恢复或对Master状态自动写回。主服务原镜像与225数据集/292任务/1导入保持不变；新实现仅在隔离候选服务验证。Python仍只用于运维/测试，业务仍Java。

下一阶段需要设计集群任务执行适配层和持久化状态机，先证明中断后的进程所有权及Executor清理，再接入网页提交。跨主机共享存储和公平规模实验仍待完成。

## 收尾补充验证

运行中取消及SIGKILL候选容器后，同容器恢复将中断任务标FAILED，启动回执与applicationId原样保留；取消状态保留，无规则成功结果或报告成功输出。证据：.runtime/receipts-api-a1a6879ab005/failure-verified.json。

四个业务任务与报告的只读独立脚本再次通过，4个不同attemptId和applicationId在重启后保持不变。随后将候选数据一致备份并恢复到全新卷，回执、完整文件清单、业务指纹、报告和故障状态全部核验通过；见同目录backup-restore-verified.json。首次额外备份因候选MySQL此前已停止被正确拒绝，未生成备份；按契约只启动隔离MySQL后重试通过，未放宽备份准入。

最终117项Java、124项运维工具测试通过，新增脚本语法检查及diff检查通过。聚合证据为.runtime/receipts-cluster-20261020-a/receipts-verified.json。主业务、文件和镜像再次核对不变；仅主环境3个容器运行，候选/恢复容器均停止，卷和日志保留。
