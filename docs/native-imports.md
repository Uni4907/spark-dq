# 原生 Parquet 在线导入 v1

在线导入由 Java/Spring Boot 接收单份 Parquet，Java/Spark 转换和完整多重集回读，最终在同一数据库事务内登记数据集、来源及成功状态。转换契约沿用 [原生类型契约](native-parquet.md)，失败保留原文件，原检测/治理/复检/报告继续使用规范 csv-v2。

## 页面与接口

页面上方“原生 Parquet 导入”选择文件，任务每3秒更新；成功后点击“选择此数据集开始检测”。展开数据集文件来源可下载校验后的原 Parquet，也可下载转换 CSV。失败或取消后可从保留文件创建独立重试任务。导入历史分页不会改变检测规则或当前选择。

| 请求 | 含义 |
| --- | --- |
| POST /api/imports | multipart `file`，必须有 `X-Import-Key: <小写标准UUID>`；受理返回202，后台执行 |
| GET /api/imports/page?page=1&pageSize=20 | 总数及稳定倒序分页，pageSize≤100 |
| GET /api/imports/{id} | 状态、源SHA/字节数、数据集ID、清单SHA、时间及失败原因 |
| POST /api/imports/{id}/cancel | QUEUED直接取消；RUNNING先进入CANCELLING，进程退出后CANCELLED |
| POST /api/imports/{id}/retry | FAILED/CANCELLED可用新X-Import-Key重试；保留原记录与parent_import_id |
| GET /api/datasets/{id}/sources/1/download | 校验源文件SHA/大小、无链接后下载原Parquet |

同一个key只有文件名、SHA、实际大小及重试来源完全一致时返回原任务；不论排队、成功或失败均不重新执行。不同内容返回409。重试必须使用新key，不需要重新上传源文件。页面在当前页面内保留待重发文件和key；网络响应丢失可重发同一次请求。刷新页面会丢失尚未确认的本地File/key，请先查历史列表，避免另行提交；不宣称跨浏览器持久化上传。

## 状态与数据一致性

V5新增dataset_imports表，不改历史V1..V4迁移。QUEUED→RUNNING→SUCCEEDED/FAILED；取消使用CANCELLING→CANCELLED。每次启动将中断的QUEUED/RUNNING标记FAILED、CANCELLING标记CANCELLED，不自动重放。服务停止/任务超时由任务所属进程句柄清理；失败终态在确认进程停止后发布。

导入与检测共用单线程+8排队槽，容量检查/排队发布/取消共用监视器。排满429且不创建任务；同key回放优先于容量检查。每次重试有独立UUID和只读原文件副本，绝不覆盖父任务。

`/data/imports/<id>/source.parquet`冻结源，prepared/canonical.csv和prepared/manifest.json为worker输出。后端独立验证源、契约、字段、行数、清单及规范CSV指纹，再由现有CSV注册器校验实际CSV；成功数据集ID等于导入ID，来源清单、dataset_id和manifest_sha256在一个事务内发布。没有SUCCEEDED和绑定指纹的部分文件不得视为导入成果。离线CLI的COMPLETE只适用于CLI；在线由数据库事务承担完成标记。

失败会保留日志和临时工件。数据库回滚发生在CSV原子移动之后时，可能留下未注册raw文件，审计会报告诊断警告；重试使用新ID，保留旧文件，不自动删除或覆盖。

## 限额、维护与备份

原文件12字节..20MiB，规范CSV≤20MiB，1..100万行、1..100列，单元格≤65536 UTF-16单元；支持整数、≤38位decimal、boolean、严格UTF8文本及1582-10-15..9999-12-31日期。浮点/时间戳/嵌套/二进制、NUL、CR/CRLF、非法UTF8和非空字面值\N明确拒绝。类型不猜测转换，也不静默规范化文本。

在线工作进程沿用现有backend内独立JVM、local[2]、1GiB堆及300秒预算；不是每个导入独立的无网络容器，后端容器暂无CPU/内存cgroup硬限制。离线CLI仍使用其独立容器资源限制，两者边界不要混同。保留目录准入预算1GiB/10000目录项、Linux余量至少1GiB，并预留活动任务空间；这不是整个数据卷硬配额，raw和历史日志也会占空间。需同时检查承载WSL的实际Windows盘，暂不自动清理。

维护准入、离线备份和有期限运行会话均统计活动导入。V5备份包含SQL、原Parquet、转换CSV/清单及实际应用；离线只读审计绑定每条导入的源SHA/大小，成功记录还绑定规范CSV、清单、来源及数据集。FAILED/CANCELLED仅核对原文件，部分输出作诊断保留。兼容V1..V4备份；旧V4应用拒绝V5，禁止直接降级或在已有卷覆盖恢复。

## 可复现验证

在隔离测试环境准备上一阶段有类型夹具后运行：

```bash
python3 tests/native_imports_e2e.py --base http://127.0.0.1:测试端口/api --source /绝对路径/source.parquet --expected /绝对路径/expected.json --failures /绝对路径/fixtures
python3 tests/native_imports_e2e.py --base http://127.0.0.1:测试端口/api --check .runtime/imports-api-fixture-实际ID.json
```

此测试写入合成导入和任务；--failures可省略以只验证成功闭环。包括真实上传/幂等与冲突、源下载、全部原始值、七规则统计、Parquet执行副本、显式治理后的独立CSV及复检/报告。不把单元测试等同于真实Spark测试。
