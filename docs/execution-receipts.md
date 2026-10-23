# 作业身份、Driver回执与Standalone只读对账

## 当前能力和边界

本阶段为后续在线集群协调建立基础。每个新Engine执行先生成独立attemptId，再保存请求SHA256、业务taskId、master和提交时间；SparkContext建立后保存applicationId；正常经过finally时保存Driver流程结果和spark.stop是否返回。应用名由taskId和attemptId共同组成，避免Master重启后applicationId重用造成误认。

这不是网页集群执行上线：现有网页/API任务仍走local，离线集群使用同机隔离实验入口。没有新增HTTP提交地址参数、自动远程取消、自动采纳孤儿作业或集群状态写回。PreparedInput和原生导入转换进程尚未纳入这套Engine回执。本阶段无需数据库迁移，V5结构保持兼容。

## 回执契约

Engine输出目录新增execution-submitted.json、execution-started.json、execution-ended.json。各阶段带共同的version=1、taskId、attemptId、requestSha256、sparkMaster。启动阶段附applicationId；结束阶段附driverOutcome、stopReturned，若已登记applicationId则必须相同。

文件内容先写临时文件并force，再以原子、不可覆盖的硬链接发布并同步目录，最后移除本次临时文件。要求运行文件系统支持本地硬链接和目录同步；本项目WSL ext4/Docker卷已经实际验证。拒绝覆盖已有submitted回执，不能在同一输出目录复用新尝试。回执仅含元数据，无数据行、凭证或任意异常文本。

Backend对每份回执限16KiB，任务请求限1MiB，拒绝符号链接、未知字段、缺失文件、请求SHA不符和身份冲突；读到损坏证据仅返回INVALID，不改写业务状态或暴露损坏内容。读操作不访问集群。只验证证据内部一致性及请求绑定，不提供防管理员篡改的数字签名。

GET /api/tasks/{id}新增execution字段，现有报告的task.execution随之保留：

| evidenceState | 含义 |
| --- | --- |
| ABSENT | 无回执；包括历史任务和尚未启动Engine的任务 |
| SUBMITTED | 最后保存了提交回执，尚无可用applicationId |
| STARTED | 最后保存了启动回执，不代表现在仍在运行 |
| DRIVER_ENDED | 已保存Driver结束回执，不代表业务数据库事务成功或远程Executor已清理 |
| INVALID | 证据缺失/损坏/不匹配，不从中推断运行状态 |

数据库task.status继续独立表示QUEUED/RUNNING/CANCELLING/CANCELLED/FAILED/SUCCEEDED。Driver完成后后端仍可能登记版本失败；Driver被杀后启动回执仍可存在；因此两套状态不能互相替代。已存在的历史任务不补造applicationId。

## 只读复现

在WSL项目目录，使用运维工具查看已有任务（不会启动/停止作业或修改任务状态）：

```bash
timeout 15s python3 infrastructure/execution_receipts.py --folder /data副本/tasks/任务ID --task-id 任务ID
timeout 15s python3 infrastructure/execution_receipts.py --folder /实验/output/distributed/result --request /实验/output/distributed/request.json --task-id distributed --master-json /受信快照/master.json
```

也可由操作者显式传--master-url http://受信Master地址:8080/json/。地址不从请求或回执读取；禁用代理、重定向、URL凭证、查询参数和片段，5秒网络超时、响应限2MiB。仅做GET，匹配Master url、ALIVE状态、applicationId及唯一应用名；不打开实验端口。它是针对固定Spark3.5.8的只读观察入口，非稳定通用调度协议。

ACTIVE表示观察到活动记录；HISTORY表示观察到历史记录；UNKNOWN表示该快照未找到，不能当作结束；UNAVAILABLE表示访问/结构/主节点身份不可信；CONFLICT表示ID重用或记录冲突；无启动回执则APPLICATION_ID_NOT_RECORDED，不按名称猜测采纳。HISTORY的FINISHED仍不证明业务成功；工具明确返回businessSuccessInferred=false、remoteCleanupConfirmed=false。快照可能很快过时，Executor释放仍需独立证据。

退出码：有效读取含ABSENT/UNKNOWN为0；INVALID、CONFLICT或UNAVAILABLE为2；参数错误由命令行解析器拒绝。调用方必须检查JSON，不能把退出0解释为业务成功。

## 实际验收入口

```bash
timeout 2100s python3 infrastructure/receipt_trial.py --output .runtime/receipts-new --host-capacity /绝对路径/host-capacity.json --rows 10000
timeout 90s python3 tests/execution_receipts_e2e.py --base http://127.0.0.1:隔离端口/api --fixture /本次native_imports_e2e生成的fixture.json
```

集群入口沿用standalone.md的固定镜像、资源和主环境保护：本地基线→双Worker检测/治理→捕捉真实ACTIVE身份→SIGKILL Driver→保留无end回执的applicationId→历史对账→独立确认Executor退出→复检→错误指纹拒绝。任务详情与报告脚本只读检查已有native_imports_e2e产生的四个真实任务；不要将隔离fixture误用于主环境。

固定Spark字段依据：[Spark3.5.8 JsonProtocol源码](https://github.com/apache/spark/blob/v3.5.8/core/src/main/scala/org/apache/spark/deploy/JsonProtocol.scala)。后续在线协调需要额外完成进程所有权、故障状态机与独立Executor清理确认，不能直接以本工具输出改写数据库。
