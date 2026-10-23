# 任务执行副本：CSV / 分片 CSV / Parquet

本阶段把离线引擎输入基础接入任务、页面、快照、治理复检及备份。注册数据集仍保存一份不可变 CSV，下载和治理版本也仍为 CSV；分片 CSV / Parquet 是服务端为特定任务生成的受控执行副本，不接受客户端文件路径，原生Parquet上传仍未开放；受控多CSV注册见 [分片注册](csv-parts.md)。

## 使用

在工作台选择已注册数据集和规则，在“检测输入方式”中选择原始 CSV（默认）、分片 CSV 执行副本或 Parquet 执行副本。页面副本请求固定4分片，实际文件数与Spark分区可能不同，空数据尤其如此。转换和校验需要额外时间；上一阶段小样本没有测到提速，不保证新格式更快。

~~~json
{
  "datasetId": "已注册的数据集UUID",
  "fields": ["name"],
  "keys": ["id"],
  "executionInput": {"format": "PARQUET", "partitions": 4}
}
~~~

POST /api/tasks 的 executionInput 也适用于完整 rules 或 templateId/templateRevision。对象严格只含 format、整数 partitions；format=CSV/PARQUET时分片1..16，CSV_SINGLE仅允许1。省略或null使用原始CSV。不能提交path、manifest、SHA或自定义执行类。

POST /api/tasks/{id}/rerun 沿用原任务执行方式，也可以显式覆盖：

~~~json
{"executionInput":{"format":"CSV_SINGLE","partitions":1}}
~~~

可同时指定 datasetId 对其他兼容数据集复检；新任务重新冻结该数据集的行数、字段和指纹。治理沿用输入方式；治理完成生成独立CSV版本并实际读取文件复检，后续复检针对该新版本生成自己的副本。历史csv-v1只能原CSV复检，显式选择新格式返回409；新建csv-v2检测后可使用副本。

## 生命周期和证据

1. 入队前校验配置，在同一事务内保存规则、原CSV SHA、统计口径和执行计划。V3新增task_snapshots.execution_plan_json、input_manifest_sha256两个可空列，旧业务字段不改。
2. 单任务仍占一个调度槽。Java先验证原CSV SHA，再启动独立Java/Spark准备进程；双向exceptAll核对完整行多重集后才发布清单。
3. 后端再核对源SHA、格式、冻结行数/字段、准备回执和逐文件路径/大小/SHA。持任务行锁，仅RUNNING且尚未绑定时一次性写入清单SHA；取消竞争不能发布成功结果。
4. 第二个Java/Spark进程只读清单列出的文件，再次验证清单和全部分片、实际行数与字符串字段。分片CSV逐个核对表头，Parquet逐个核对schema。治理输出和异常记录仍走原来流程。
5. 准备和检测共享300秒执行预算；预算耗尽不会再启动下一进程。排队/运行取消、子进程清理、服务停止及重启恢复覆盖两个阶段；失败不保存半份成功指标或治理版本。prepare.log和engine.log分别保留诊断。

已结束任务快照的executionInput记录version=1、format、partitions、rowCount、columns；input_manifest_sha256是准备完成后追加的真实工件指纹，不重写原规则、配置和原CSV指纹。复检生成新任务/新工件，清单SHA可以不同。旧任务省略新增字段，历史任务/报告JSON结构保持兼容。

~~~mermaid
flowchart LR
  A[注册只读 CSV] --> B[Java 调度与冻结快照]
  B --> C[直接读取原 CSV]
  B --> D[Java / Spark 准备执行副本]
  D --> E[保真校验与一次性绑定清单 SHA]
  C --> F[Java / Spark 检测或治理]
  E --> F
  F --> G[统计与持久化状态]
  F --> H[独立 CSV 版本与实际文件复检]
  H --> I[前后比较和 JSON 报告]
~~~

Python继续承担测试驱动、备份/恢复、运行会话和实验工具；在线检测、治理和本阶段副本准备均由Java/Spark执行。

## 备份、恢复与升级

当前storage.py会将数据库计划、绑定的清单SHA、源SHA、行数/字段和逐分片大小/SHA一起对账，流式归档额外捕获单清单最多1MiB、全组最多16MiB的清单JSON。失败或取消任务若已有绑定指纹，同样检查其工件；未绑定的局部目录仅作为诊断保留。发现损坏即拒绝完整备份。旧V1/V2库仍可只读巡检和备份；新备份包含V3 SQL与实际应用包，新卷恢复验证全部文件和快照，再验证真实复检。

V3只有增加列，不删除旧数据。旧V2应用会拒绝V3数据库，不能把应用镜像直接降级当作回滚；不得drop列、repair/clean迁移或覆盖用户数据。升级前保留数据库+文件+实际应用包的一致备份。如需恢复，先使用新卷隔离恢复并核对，再决定服务切换。

## 验收与限制

~~~bash
timeout 150s python3 -m unittest discover -s tests -p 'test_*.py' -v
timeout 900s python3 tests/execution_inputs.py --base http://127.0.0.1:18080/api
timeout 90s python3 tests/execution_inputs.py --base http://127.0.0.1:18080/api --check /absolute/fixture.json
~~~

首次接口验收仅新增合成数据/任务/治理版本并生成唯一夹具文件；--check只读。tests/execution_lifecycle.py只允许新建execution-*恢复克隆，明确拒绝主容器，包含进程暂停/分片损坏/容器崩溃，不能当作生产验收。所有诊断和旧副本保留，不自动清理数据。

仍受20MiB上传、100列、单后端串行调度限制。没有缓存复用/自动清理、原生Parquet上传、用户任意多文件注册或集群/TB级验证；执行副本会增加存储和准备成本。后续继续扩展原生Parquet注册与受控容量、更大样本和集群提交/共享存储。
